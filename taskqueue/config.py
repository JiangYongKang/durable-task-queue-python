"""Configuration for the durable task queue."""

from dataclasses import dataclass


@dataclass(frozen=True)
class QueueConfig:
    """Tunable settings for the queue service."""

    db_path: str = "taskqueue.db"
    lease_seconds: float = 30.0
    max_attempts: int = 3
    max_payload_bytes: int = 64 * 1024
    # Backoff schedule for retryable failures: the wait before redelivery is
    # min(retry_initial_delay_seconds * 2**(failures - 1), retry_max_delay_seconds).
    retry_initial_delay_seconds: float = 1.0
    retry_max_delay_seconds: float = 60.0
