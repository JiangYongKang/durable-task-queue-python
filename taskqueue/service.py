"""Queue service: submission, dedup, leasing, retry, and terminal states."""

from taskqueue.config import QueueConfig
from taskqueue.models import Task, TaskEvent
from taskqueue.storage import Storage


class SubmitRejected(Exception):
    """Submission rejected with a machine-readable reason."""

    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason


class QueueService:
    def __init__(self, config: QueueConfig) -> None:
        self._config = config
        self._storage = Storage(config)

    def open(self) -> None:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError

    def submit(self, idempotency_key: str, payload: object) -> tuple[Task, bool]:
        """Submit a task; returns (task, created). Raises SubmitRejected."""
        raise NotImplementedError

    def acquire(self) -> tuple[Task, str] | None:
        """Lease the next deliverable task; returns (task, lease_id)."""
        raise NotImplementedError

    def ack(self, task_id: str, lease_id: str) -> Task:
        raise NotImplementedError

    def fail(self, task_id: str, lease_id: str, retryable: bool) -> Task:
        raise NotImplementedError

    def get_task(self, task_id: str) -> Task | None:
        raise NotImplementedError

    def get_history(self, task_id: str) -> list[TaskEvent]:
        raise NotImplementedError
