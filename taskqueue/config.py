"""Queue configuration options.

All values can be overridden through environment variables (prefix
``TASKQUEUE_``) or CLI flags in ``main.py``.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class QueueConfig:
    #: SQLite database file used as the local message broker.
    db_path: str = "taskqueue.db"
    #: Seconds a lease stays valid without a heartbeat / result report.
    visibility_timeout: float = 30.0
    #: Maximum delivery attempts before a retryable task is dead-lettered.
    max_attempts: int = 3
    #: Base seconds for exponential backoff: base * 2**(attempt-1).
    retry_backoff_base: float = 1.0
    #: Maximum accepted payload size in bytes.
    max_payload_bytes: int = 64 * 1024
    #: Interval of the background expired-lease reaper, seconds.
    reaper_interval: float = 5.0


_ENV_MAP = {
    "db_path": ("TASKQUEUE_DB_PATH", str),
    "visibility_timeout": ("TASKQUEUE_VISIBILITY_TIMEOUT", float),
    "max_attempts": ("TASKQUEUE_MAX_ATTEMPTS", int),
    "retry_backoff_base": ("TASKQUEUE_RETRY_BACKOFF_BASE", float),
    "max_payload_bytes": ("TASKQUEUE_MAX_PAYLOAD_BYTES", int),
    "reaper_interval": ("TASKQUEUE_REAPER_INTERVAL", float),
}


def config_from_env(**overrides: object) -> QueueConfig:
    """Build a :class:`QueueConfig` using defaults + env vars + overrides."""
    values: dict[str, object] = {}
    for field, (env, cast) in _ENV_MAP.items():
        if env in os.environ:
            values[field] = cast(os.environ[env])
    values.update({k: v for k, v in overrides.items() if v is not None})
    return QueueConfig(**values)  # type: ignore[arg-type]


DEFAULT_CONFIG = config_from_env()
