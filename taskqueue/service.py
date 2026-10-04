"""Queue service: submission, dedup, leasing, retry, and terminal states."""

import json
import sqlite3
import time
import uuid
from collections.abc import Callable

from taskqueue.config import QueueConfig
from taskqueue.models import Task, TaskEvent, TaskState
from taskqueue.storage import Storage


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
        return task, task.lease_id

    def ack(self, task_id: str, lease_id: str) -> Task:
        task = self._storage.complete_task(task_id, lease_id, self._clock())
        if task is None:
            raise LeaseError(f"no valid lease {lease_id} for task {task_id}")
        return task

    def _retry_delay(self, attempts: int) -> float:
        """Backoff wait before the next delivery, given failures so far.

        Grows exponentially from retry_initial_delay_seconds and is capped at
        retry_max_delay_seconds: attempt 1 -> initial, 2 -> 2x, 3 -> 4x, ...
        """
        cfg = self._config
        delay = cfg.retry_initial_delay_seconds * (2 ** max(0, attempts - 1))
        return min(delay, cfg.retry_max_delay_seconds)

    def fail(self, task_id: str, lease_id: str, retryable: bool) -> Task:
        # Read attempts first to size the backoff; attempts only change on
        # acquire, so the value cannot race with this fail for the same lease.
        current = self._storage.get_task(task_id)
        attempts = current.attempts if current is not None else 0
        delay = self._retry_delay(attempts) if retryable else 0.0
        task = self._storage.fail_task(
            task_id, lease_id, retryable, self._clock(),
            self._config.max_attempts, delay,
        )
        if task is None:
            raise LeaseError(f"no valid lease {lease_id} for task {task_id}")
        return task

    # -- queries ---------------------------------------------------------

    def get_task(self, task_id: str) -> Task | None:
        return self._storage.get_task(task_id)

    def get_history(self, task_id: str) -> list[TaskEvent]:
        return self._storage.list_events(task_id)
