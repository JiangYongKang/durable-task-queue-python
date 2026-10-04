"""Configuration for the durable task queue."""

from dataclasses import dataclass


@dataclass(frozen=True)
class QueueConfig:
    """Tunable settings for the queue service."""

    db_path: str = "taskqueue.db"
    lease_seconds: float = 30.0
    max_attempts: int = 3
    max_payload_bytes: int = 64 * 1024
    # Delayed retry scheduling: after a retryable failure the task waits
    # min(retry_initial_delay * backoff_factor ** (failures - 1),
    #     retry_max_delay).
    retry_initial_delay: float = 0.0
    retry_max_delay: float = 60.0
    retry_backoff_factor: float = 2.0

    def __post_init__(self) -> None:
        if self.retry_initial_delay < 0:
            raise ValueError("retry_initial_delay must be >= 0")
        if self.retry_max_delay < self.retry_initial_delay:
            raise ValueError("retry_max_delay must be >= retry_initial_delay")
        if self.retry_backoff_factor < 1.0:
            raise ValueError("retry_backoff_factor must be >= 1")

    def retry_delay_for_attempt(self, failure_count: int) -> float:
        """Wait before redelivery after ``failure_count`` retryable failures."""
        raw = self.retry_initial_delay * (
            self.retry_backoff_factor ** (failure_count - 1)
        )
        return min(raw, self.retry_max_delay)
