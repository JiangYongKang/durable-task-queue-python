"""HTTP API exposing the queue service via FastAPI."""

import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel

from taskqueue.config import QueueConfig
from taskqueue.models import Task, TaskEvent
from taskqueue.service import LeaseError, QueueService, SubmitRejected

_REASON_STATUS = {
    "invalid_idempotency_key": 422,
    "invalid_payload": 422,
    "payload_too_large": 413,
    "payload_conflict": 409,
}


class SubmitRequest(BaseModel):
    idempotency_key: Any = None
    payload: Any = None


class LeaseRequest(BaseModel):
    lease_id: str


class FailRequest(BaseModel):
    lease_id: str
    retryable: bool = True


def _task_view(task: Task) -> dict:
    return {
        "task_id": task.task_id,
        "idempotency_key": task.idempotency_key,
        "payload": task.payload,
        "state": task.state.value,
        "attempts": task.attempts,
        "max_attempts": task.max_attempts,
        "lease_id": task.lease_id,
        "lease_expires_at": task.lease_expires_at,
        "not_before": task.not_before,
        "created_at": task.created_at,
        "updated_at": task.updated_at,
    }


def _event_view(event: TaskEvent) -> dict:
    return {
        "event_id": event.event_id,
        "task_id": event.task_id,
        "event_type": event.event_type,
        "detail": event.detail,
        "created_at": event.created_at,
    }


def config_from_env() -> QueueConfig:
    return QueueConfig(
        db_path=os.environ.get("TQ_DB_PATH", "taskqueue.db"),
        lease_seconds=float(os.environ.get("TQ_LEASE_SECONDS", "30")),
        max_attempts=int(os.environ.get("TQ_MAX_ATTEMPTS", "3")),
        max_payload_bytes=int(os.environ.get("TQ_MAX_PAYLOAD_BYTES", str(64 * 1024))),
        retry_initial_delay=float(os.environ.get("TQ_RETRY_INITIAL_DELAY", "0")),
        retry_max_delay=float(os.environ.get("TQ_RETRY_MAX_DELAY", "60")),
        retry_backoff_factor=float(os.environ.get("TQ_RETRY_BACKOFF_FACTOR", "2")),
    )


def create_app(config: QueueConfig | None = None) -> FastAPI:
    cfg = config or config_from_env()
    service = QueueService(cfg)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        service.open()
        yield
        service.close()

    app = FastAPI(title="durable-task-queue", lifespan=lifespan)
    app.state.service = service

    @app.exception_handler(SubmitRejected)
    async def on_rejected(_req: Request, exc: SubmitRejected) -> JSONResponse:
        return JSONResponse(
            status_code=_REASON_STATUS.get(exc.reason, 400),
            content={"error": exc.reason, "message": str(exc)},
        )

    @app.exception_handler(LeaseError)
    async def on_lease_error(_req: Request, exc: LeaseError) -> JSONResponse:
        return JSONResponse(
            status_code=409, content={"error": "lease_invalid", "message": str(exc)}
        )

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok"}

    @app.post("/tasks", status_code=201)
    def submit(req: SubmitRequest) -> JSONResponse:
        task, created = service.submit(req.idempotency_key, req.payload)
        return JSONResponse(
            status_code=201 if created else 200,
            content={"task": _task_view(task), "created": created},
        )

    @app.post("/tasks/acquire")
    def acquire() -> Response:
        result = service.acquire()
        if result is None:
            return Response(status_code=204)
        task, lease_id = result
        return JSONResponse(content={"task": _task_view(task), "lease_id": lease_id})

    @app.post("/tasks/{task_id}/ack")
    def ack(task_id: str, req: LeaseRequest) -> dict:
        return {"task": _task_view(service.ack(task_id, req.lease_id))}

    @app.post("/tasks/{task_id}/fail")
    def fail(task_id: str, req: FailRequest) -> dict:
        return {"task": _task_view(service.fail(task_id, req.lease_id, req.retryable))}

    @app.get("/tasks/{task_id}")
    def get_task(task_id: str) -> JSONResponse:
        task = service.get_task(task_id)
        if task is None:
            return JSONResponse(status_code=404, content={"error": "not_found"})
        return JSONResponse(content={"task": _task_view(task)})

    @app.get("/tasks/{task_id}/history")
    def get_history(task_id: str) -> dict:
        return {"events": [_event_view(e) for e in service.get_history(task_id)]}

    return app
