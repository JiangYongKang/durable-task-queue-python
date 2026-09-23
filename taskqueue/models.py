"""Domain models and status enumerations."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum

from .config import QueueConfig


class TaskState(str, Enum):
    PENDING = "pending"
    INFLIGHT = "inflight"
    SUCCEEDED = "succeeded"
    RETRYING = "retrying"
    DEAD = "dead"


class RejectReason(str, Enum):
    PAYLOAD_MISMATCH = "idempotency_key_payload_mismatch"
    INVALID_PAYLOAD = "invalid_payload"
    PAYLOAD_TOO_LARGE = "payload_too_large"


class FailureKind(str, Enum):
    RETRYABLE = "retryable"
    PERMANENT = "permanent"


@dataclass
class DeliveryRecord:
    attempt: int
    lease_id: str
    consumer: str | None
    leased_at: float
    expires_at: float
    reported_at: float | None = None
    result: str = "leased"
    failure_kind: str | None = None
    error: str | None = None


@dataclass
class Task:
    id: str
    idempotency_key: str
    payload: str
    state: TaskState
    attempt_count: int
    created_at: float
    available_at: float
    lease_id: str | None
    leased_to: str | None
    leased_at: float | None
    lease_expires_at: float | None
    last_error: str | None
    history: list[DeliveryRecord] = field(default_factory=list)
