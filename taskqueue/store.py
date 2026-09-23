"""SQLite-backed durable storage for the task queue.

The store exposes a small set of primitives; all queue semantics live in
``taskqueue.queue``.

Durability / consistency choices:

* a single SQLite connection with WAL journaling (``synchronous=FULL``) so
  every state transition survives process restart;
* one process-wide :class:`threading.RLock` serialises every transaction,
  which makes "one task has at most one valid lease" hold even when many
  consumer threads race inside one server process;
* each delivery attempt is appended to the ``deliveries`` table and never
  mutated beyond its final result, so the full history is explainable.
"""

from __future__ import annotations

import sqlite3
import threading
from typing import Any

_SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    id               TEXT PRIMARY KEY,
    idempotency_key  TEXT NOT NULL UNIQUE,
    payload          TEXT NOT NULL,
    payload_hash     TEXT NOT NULL,
    state            TEXT NOT NULL,
    attempt_count    INTEGER NOT NULL DEFAULT 0,
    created_at       REAL NOT NULL,
    available_at     REAL NOT NULL,
    lease_id         TEXT,
    leased_to        TEXT,
    leased_at        REAL,
    lease_expires_at REAL,
    last_error       TEXT
);

CREATE INDEX IF NOT EXISTS idx_tasks_dispatch
    ON tasks(state, available_at);

CREATE TABLE IF NOT EXISTS deliveries (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id     TEXT NOT NULL,
    attempt     INTEGER NOT NULL,
    lease_id    TEXT NOT NULL,
    consumer    TEXT,
    leased_at   REAL NOT NULL,
    expires_at  REAL NOT NULL,
    reported_at REAL,
    result      TEXT NOT NULL,
    failure_kind TEXT,
    error       TEXT,
    FOREIGN KEY (task_id) REFERENCES tasks(id)
);

CREATE INDEX IF NOT EXISTS idx_deliveries_task
    ON deliveries(task_id, id);
"""


class TaskStore:
    """Thin durable data layer. SQL transactions run under :attr:`lock`."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            db_path, check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.executescript(_SCHEMA)

    @property
    def lock(self) -> threading.RLock:
        return self._lock

    @property
    def conn(self) -> sqlite3.Connection:
        return self._conn

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # -- read helpers -----------------------------------------------------

    @staticmethod
    def _row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
        return dict(row) if row is not None else None

    def get_task_row(self, task_id: str) -> dict[str, Any] | None:
        cur = self._conn.execute(
            "SELECT * FROM tasks WHERE id = ?", (task_id,)
        )
        return self._row_to_dict(cur.fetchone())

    def get_task_by_idempotency_key(self, key: str) -> dict[str, Any] | None:
        cur = self._conn.execute(
            "SELECT * FROM tasks WHERE idempotency_key = ?", (key,)
        )
        return self._row_to_dict(cur.fetchone())

    def get_delivery_rows(self, task_id: str) -> list[dict[str, Any]]:
        cur = self._conn.execute(
            "SELECT * FROM deliveries WHERE task_id = ? ORDER BY id",
            (task_id,),
        )
        return [dict(r) for r in cur.fetchall()]
