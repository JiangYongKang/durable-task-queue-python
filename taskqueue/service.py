"""Queue service: submission, dedup, leasing, retry, and terminal states."""

import json
import logging
import sqlite3
import time
import uuid
from collections.abc import Callable

from taskqueue.config import QueueConfig
from taskqueue.models import Task, TaskEvent, TaskState
from taskqueue.storage import Storage

log = logging.getLogger("taskqueue.service")


class SubmitRejected(Exception):
    """Submission rejected with a machine-readable reason."""

    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason


class LeaseError(Exception):
    """Lease is missing, expired, or does not match the task."""


class QueueService:
    def __init__(self, config: QueueConfig, clock: Callable[[], float] = time.time) -> None:
        self._config = config
        self._clock = clock
        self._storage = Storage(config)

    def open(self) -> None:
        self._storage.open()

    def close(self) -> None:
        self._storage.close()

    def __enter__(self) -> "QueueService":
        self.open()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- submission ------------------------------------------------------

    def _canonical_payload(self, payload: object) -> str:
        try:
            return json.dumps(payload, sort_keys=True, separators=(",", ":"))
        except (TypeError, ValueError) as exc:
            raise SubmitRejected("invalid_payload", f"payload is not JSON-serializable: {exc}")

    def submit(self, idempotency_key: object, payload: object) -> tuple[Task, bool]:
        """Submit a task; returns (task, created). Raises SubmitRejected."""
        if not isinstance(idempotency_key, str) or not idempotency_key.strip():
            raise SubmitRejected(
                "invalid_idempotency_key", "idempotency_key must be a non-empty string"
            )
        canonical = self._canonical_payload(payload)
        if len(canonical.encode("utf-8")) > self._config.max_payload_bytes:
            raise SubmitRejected(
                "payload_too_large",
                f"payload exceeds {self._config.max_payload_bytes} bytes",
            )
        existing = self._storage.get_task_by_key(idempotency_key)
        if existing is not None:
            if existing.payload == canonical:
                return existing, False
            raise SubmitRejected(
                "payload_conflict",
                "idempotency_key already used with a different payload",
            )
        now = self._clock()
        task = Task(
            task_id=uuid.uuid4().hex,
            idempotency_key=idempotency_key,
            payload=canonical,
            state=TaskState.PENDING,
            attempts=0,
            max_attempts=self._config.max_attempts,
            created_at=now,
            updated_at=now,
        )
        try:
            self._storage.insert_task(task)
        except sqlite3.IntegrityError:
            # Lost a submit race; the stored row is authoritative.
            winner = self._storage.get_task_by_key(idempotency_key)
            assert winner is not None
            if winner.payload == canonical:
                return winner, False
            raise SubmitRejected(
                "payload_conflict",
                "idempotency_key already used with a different payload",
            )
        return task, True

    # -- consumption -----------------------------------------------------

    def acquire(self) -> tuple[Task, str] | None:
        """Lease the next deliverable task; returns (task, lease_id)."""
        task = self._storage.acquire_lease(self._clock(), self._config.lease_seconds)
        if task is None or task.lease_id is None:
            return None
        log.info(
            "acquire idempotency_key=%s task_id=%s lease_id=%s attempt=%d/%d"
            " redelivered=%s",
            task.idempotency_key, task.task_id, task.lease_id, task.attempts,
            task.max_attempts, task.attempts > 1,
        )
        return task, task.lease_id

    def ack(self, task_id: str, lease_id: str) -> Task:
        task = self._storage.complete_task(task_id, lease_id, self._clock())
        if task is None:
            raise LeaseError(f"no valid lease {lease_id} for task {task_id}")
        log.info(
            "ack idempotency_key=%s task_id=%s lease_id=%s attempts=%d",
            task.idempotency_key, task_id, lease_id, task.attempts,
        )
        return task

    def fail(self, task_id: str, lease_id: str, retryable: bool) -> Task:
        task = self._storage.fail_task(
            task_id, lease_id, retryable, self._clock(), self._config
        )
        if task is None:
            log.info(
                "fail rejected lease_id=%s task_id=%s (stale/unknown lease)",
                lease_id, task_id,
            )
            raise LeaseError(f"no valid lease {lease_id} for task {task_id}")
        wait_info = (
            f" not_before={task.not_before}" if task.not_before is not None else ""
        )
        log.info(
            "fail idempotency_key=%s task_id=%s lease_id=%s attempt=%d/%d"
            " retryable=%s state=%s%s",
            task.idempotency_key, task_id, lease_id, task.attempts,
            task.max_attempts, retryable, task.state.value, wait_info,
        )
        return task

    # -- queries ---------------------------------------------------------

    def get_task(self, task_id: str) -> Task | None:
        return self._storage.get_task(task_id)

    def get_history(self, task_id: str) -> list[TaskEvent]:
        return self._storage.list_events(task_id)
