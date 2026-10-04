"""Domain models: task states, failure classes, and records."""

from dataclasses import dataclass, field
from enum import Enum


class TaskState(str, Enum):
    PENDING = "pending"
    LEASED = "leased"
    WAITING = "waiting"  # retryable failure; held until not_before elapses
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
    not_before: float | None = None  # WAITING tasks become deliverable at this time


@dataclass
class TaskEvent:
    event_id: int
    task_id: str
    event_type: str
    detail: str
    created_at: float
