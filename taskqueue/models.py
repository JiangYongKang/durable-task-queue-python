"""Domain models: task states, failure classes, and records."""

from dataclasses import dataclass, field
from enum import Enum


class TaskState(str, Enum):
    PENDING = "pending"
    LEASED = "leased"
    SUCCEEDED = "succeeded"
    FAILED = "failed"  # non-retryable terminal failure
    DEAD = "dead"  # exhausted retries, moved to dead letter


class FailureClass(str, Enum):
    RETRYABLE = "retryable"
    NON_RETRYABLE = "non_retryable"


@dataclass
class Task:
    task_id: str
    idempotency_key: str
    payload: str
    state: TaskState
    attempts: int
    max_attempts: int
    created_at: float
    updated_at: float
    lease_id: str | None = None
    lease_expires_at: float | None = None
    # Earliest time the task may be delivered again; None means immediately.
    # Set when a retryable failure schedules a backoff wait.
    next_attempt_at: float | None = None


@dataclass
class TaskEvent:
    event_id: int
    task_id: str
    event_type: str
    detail: str
    created_at: float
