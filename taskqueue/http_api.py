"""HTTP API exposing the durable task queue.

Endpoints
---------
POST   /tasks                 submit a task (idempotent)
POST   /tasks/lease           claim one due task (long-poll optional)
POST   /tasks/{id}/complete   report success (requires lease_id)
POST   /tasks/{id}/fail       report retryable/permanent failure
POST   /tasks/{id}/heartbeat  extend a lease
POST   /tasks/{id}/reclaim    force a lease-expiry sweep (tests/ops)
GET    /tasks/{id}            full task state + delivery history
GET    /tasks/key/{key}       lookup by idempotency key
GET    /healthz               liveness
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .config import QueueConfig
from .models import FailureKind
from .queue import LeaseError, SubmissionRejected, TaskQueue

logger = logging.getLogger("taskqueue.api")


class SubmitRequest(BaseModel):
    payload: dict[str, Any]
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=200)


class LeaseRequest(BaseModel):
    consumer: str | None = Field(default=None, max_length=200)
    wait_timeout: float = Field(default=0.0, ge=0.0, le=30.0)


class CompleteRequest(BaseModel):
    lease_id: str
    result: str = Field(default="succeeded")


class FailRequest(BaseModel):
    lease_id: str
    kind: FailureKind
    error: str | None = Field(default=None, max_length=2000)


class LeaseTokenRequest(BaseModel):
    lease_id: str


def _task_dict(t: Any) -> dict[str, Any]:
    return {
        "id": t.id,
        "idempotency_key": t.idempotency_key,
        "payload": t.payload,
        "state": t.state.value,
        "attempt_count": t.attempt_count,
        "attempts_allowed": None,
        "created_at": t.created_at,
        "available_at": t.available_at,
        "lease": (
            None
            if t.lease_id is None
            else {
                "lease_id": t.lease_id,
                "consumer": t.leased_to,
                "leased_at": t.leased_at,
                "expires_at": t.lease_expires_at,
            }
        ),
        "last_error": t.last_error,
        "history": [
            {
                "attempt": h.attempt,
                "lease_id": h.lease_id,
                "consumer": h.consumer,
                "leased_at": h.leased_at,
                "expires_at": h.expires_at,
                "reported_at": h.reported_at,
                "result": h.result,
                "failure_kind": h.failure_kind,
                "error": h.error,
            }
            for h in t.history
        ],
    }


def create_app(
    config: QueueConfig | None = None,
    *,
    start_reaper: bool = True,
) -> FastAPI:
    config = config or QueueConfig()
    app = FastAPI(title="Durable Local Task Queue", version="0.1.0")
    queue = TaskQueue(config)
    app.state.queue = queue
    app.state.config = config

    # include attempt budget in every task view
    def view(t: Any) -> dict[str, Any]:
        d = _task_dict(t)
        d["attempts_allowed"] = config.max_attempts
        return d

    # -- background lease reaper ----------------------------------------

    stop_event = threading.Event()

    def _reaper_loop() -> None:
        while not stop_event.wait(config.reaper_interval):
            try:
                queue.reclaim_expired()
            except Exception:  # pragma: no cover - defensive
                logger.exception("reaper sweep failed")

    reaper_thread: threading.Thread | None = None
    if start_reaper:
        reaper_thread = threading.Thread(
            target=_reaper_loop, name="lease-reaper", daemon=True
        )
    stop_event = threading.Event()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if start_reaper:
            reaper_thread.start()
        try:
            yield
        finally:
            stop_event.set()
            queue.close()

    app.router.lifespan_context = lifespan

    # -- endpoints ------------------------------------------------------

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/tasks")
    def submit_task(req: SubmitRequest) -> JSONResponse:
        import json

        payload = json.dumps(req.payload, separators=(",", ":"), ensure_ascii=False)
        try:
            task, created = queue.submit(payload, req.idempotency_key)
        except SubmissionRejected as exc:
            return JSONResponse(
                status_code=409 if exc.reason.value == "idempotency_key_payload_mismatch" else 422,
                content={
                    "error": "submission_rejected",
                    "reason": exc.reason.value,
                    "detail": exc.message,
                },
            )
        body = view(task)
        body["created"] = created
        return JSONResponse(status_code=201 if created else 200, content=body)

    @app.post("/tasks/lease")
    def lease_task(req: LeaseRequest) -> JSONResponse:
        deadline = time.time() + req.wait_timeout
        task = None
        while True:
            task = queue.lease(req.consumer)
            if task is not None or time.time() >= deadline:
                break
            time.sleep(min(0.2, max(0.0, deadline - time.time())))
        if task is None:
            return JSONResponse(
                status_code=204,
                content={"message": "no task available"},
            )
        return JSONResponse(status_code=200, content=view(task))

    def _lease_problem(exc: LeaseError) -> JSONResponse:
        return JSONResponse(
            status_code=409,
            content={"error": "lease_conflict", "detail": str(exc)},
        )

    @app.post("/tasks/{task_id}/complete")
    def complete_task(task_id: str, req: CompleteRequest) -> JSONResponse:
        try:
            task = queue.complete(task_id, req.lease_id, req.result)
        except LeaseError as exc:
            return _lease_problem(exc)
        return JSONResponse(status_code=200, content=view(task))

    @app.post("/tasks/{task_id}/fail")
    def fail_task(task_id: str, req: FailRequest) -> JSONResponse:
        try:
            task = queue.fail(task_id, req.lease_id, req.kind, req.error)
        except LeaseError as exc:
            return _lease_problem(exc)
        return JSONResponse(status_code=200, content=view(task))

    @app.post("/tasks/{task_id}/heartbeat")
    def heartbeat_task(task_id: str, req: LeaseTokenRequest) -> JSONResponse:
        try:
            task = queue.heartbeat(task_id, req.lease_id)
        except LeaseError as exc:
            return _lease_problem(exc)
        return JSONResponse(status_code=200, content=view(task))

    @app.post("/tasks/reclaim")
    def force_reclaim() -> dict[str, int]:
        return {"reclaimed": queue.reclaim_expired()}

    @app.get("/tasks/{task_id}")
    def get_task(task_id: str) -> JSONResponse:
        task = queue.get_task(task_id)
        if task is None:
            return JSONResponse(status_code=404, content={"error": "not_found"})
        return JSONResponse(status_code=200, content=view(task))

    @app.get("/tasks/key/{idempotency_key}")
    def get_task_by_key(idempotency_key: str) -> JSONResponse:
        task = queue.get_task_by_key(idempotency_key)
        if task is None:
            return JSONResponse(status_code=404, content={"error": "not_found"})
        return JSONResponse(status_code=200, content=view(task))

    return app
