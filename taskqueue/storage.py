"""SQLite-backed persistence layer (local storage simulating a broker)."""

import sqlite3

from taskqueue.config import QueueConfig
from taskqueue.models import Task, TaskEvent


class Storage:
    """Durable storage for tasks, leases, and event history."""

    def __init__(self, config: QueueConfig) -> None:
        self._config = config
        self._conn: sqlite3.Connection | None = None

    def open(self) -> None:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError

    def insert_task(self, task: Task) -> None:
        raise NotImplementedError

    def get_task(self, task_id: str) -> Task | None:
        raise NotImplementedError

    def get_task_by_key(self, idempotency_key: str) -> Task | None:
        raise NotImplementedError

    def acquire_lease(self, now: float, lease_seconds: float) -> Task | None:
        raise NotImplementedError

    def complete_task(self, task_id: str, lease_id: str, now: float) -> Task | None:
        raise NotImplementedError

    def fail_task(
        self, task_id: str, lease_id: str, retryable: bool, now: float, max_attempts: int
    ) -> Task | None:
        raise NotImplementedError

    def list_events(self, task_id: str) -> list[TaskEvent]:
        raise NotImplementedError
