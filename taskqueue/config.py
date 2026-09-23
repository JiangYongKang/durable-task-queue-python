"""Configuration for the durable task queue."""

from dataclasses import dataclass


@dataclass(frozen=True)
class QueueConfig:
    """Tunable settings for the queue service."""

    db_path: str = "taskqueue.db"
    lease_seconds: float = 30.0
    max_attempts: int = 3
    max_payload_bytes: int = 64 * 1024
