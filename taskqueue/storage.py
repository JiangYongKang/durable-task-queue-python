"""SQLite-backed persistence layer (local storage simulating a broker).

All multi-step writes run inside BEGIN IMMEDIATE transactions so that
concurrent consumers observe atomic lease acquisition and state changes.
A single connection guarded by a lock keeps semantics deterministic.
"""

import sqlite3
import threading
import uuid

from taskqueue.config import QueueConfig
from taskqueue.models import Task, TaskEvent, TaskState

_SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    task_id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    payload TEXT NOT NULL,
    state TEXT NOT NULL,
    attempts INTEGER NOT NULL,
    max_attempts INTEGER NOT NULL,
    lease_id TEXT,
    lease_expires_at REAL,
    next_attempt_at REAL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    detail TEXT NOT NULL,
    created_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_task ON events(task_id);
"""


class Storage:
    """Durable storage for tasks, leases, and event history."""

    def __init__(self, config: QueueConfig) -> None:
        self._config = config
        self._conn: sqlite3.Connection | None = None
        self._lock = threading.Lock()

    def open(self) -> None:
        self._conn = sqlite3.connect(
            self._config.db_path, check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.executescript(_SCHEMA)
        self._migrate()

    def _migrate(self) -> None:
        """Add columns introduced after the initial schema (idempotent)."""
        conn = self._require_conn()
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(tasks)")}
        if "next_attempt_at" not in cols:
            conn.execute("ALTER TABLE tasks ADD COLUMN next_attempt_at REAL")

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    # -- helpers ---------------------------------------------------------

    def _require_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("storage is not open")
        return self._conn

    @staticmethod
    def _row_to_task(row: sqlite3.Row) -> Task:
        return Task(
            task_id=row["task_id"],
            idempotency_key=row["idempotency_key"],
            payload=row["payload"],
            state=TaskState(row["state"]),
            attempts=row["attempts"],
            max_attempts=row["max_attempts"],
            lease_id=row["lease_id"],
            lease_expires_at=row["lease_expires_at"],
            next_attempt_at=row["next_attempt_at"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def _add_event(
        self, conn: sqlite3.Connection, task_id: str, event_type: str, detail: str, now: float
    ) -> None:
        conn.execute(
            "INSERT INTO events (task_id, event_type, detail, created_at) VALUES (?,?,?,?)",
            (task_id, event_type, detail, now),
        )

    # -- commands --------------------------------------------------------

    def insert_task(self, task: Task) -> None:
        """Insert a new task; raises sqlite3.IntegrityError on duplicate key."""
        conn = self._require_conn()
        with self._lock:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "INSERT INTO tasks (task_id, idempotency_key, payload, state, attempts,"
                    " max_attempts, lease_id, lease_expires_at, next_attempt_at,"
                    " created_at, updated_at)"
                    " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        task.task_id,
                        task.idempotency_key,
                        task.payload,
                        task.state.value,
                        task.attempts,
                        task.max_attempts,
                        task.lease_id,
                        task.lease_expires_at,
                        task.next_attempt_at,
                        task.created_at,
                        task.updated_at,
                    ),
                )
                self._add_event(
                    conn, task.task_id, "submitted",
                    f"idempotency_key={task.idempotency_key}", task.created_at,
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def get_task(self, task_id: str) -> Task | None:
        conn = self._require_conn()
        row = conn.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
        return self._row_to_task(row) if row else None

    def get_task_by_key(self, idempotency_key: str) -> Task | None:
        conn = self._require_conn()
        row = conn.execute(
            "SELECT * FROM tasks WHERE idempotency_key=?", (idempotency_key,)
        ).fetchone()
        return self._row_to_task(row) if row else None

    def acquire_lease(self, now: float, lease_seconds: float) -> Task | None:
        """Atomically lease the next deliverable task.

        Deliverable means PENDING and past its backoff wait (next_attempt_at
        is NULL or <= now), or LEASED with an expired lease (the previous
        consumer timed out or crashed). Increments attempts per delivery.
        Tasks still in their backoff wait are invisible here, so they never
        block newer tasks that are already deliverable.
        """
        conn = self._require_conn()
        with self._lock:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    "SELECT * FROM tasks WHERE"
                    " (state=? AND (next_attempt_at IS NULL OR next_attempt_at<=?))"
                    " OR (state=? AND lease_expires_at<=?)"
                    " ORDER BY COALESCE(next_attempt_at, created_at) LIMIT 1",
                    (
                        TaskState.PENDING.value,
                        now,
                        TaskState.LEASED.value,
                        now,
                    ),
                ).fetchone()
                if row is None:
                    conn.execute("COMMIT")
                    return None
                task = self._row_to_task(row)
                waited_detail = ""
                if task.state == TaskState.LEASED:
                    self._add_event(
                        conn, task.task_id, "lease_expired",
                        f"stale lease_id={task.lease_id} expired_at={task.lease_expires_at}",
                        now,
                    )
                elif task.next_attempt_at is not None:
                    # Redelivery after a backoff wait: updated_at was the
                    # failure time, so this is how long it actually waited.
                    waited_detail = f" waited_seconds={now - task.updated_at:.6f}"
                lease_id = uuid.uuid4().hex
                attempts = task.attempts + 1
                expires = now + lease_seconds
                conn.execute(
                    "UPDATE tasks SET state=?, attempts=?, lease_id=?, lease_expires_at=?,"
                    " next_attempt_at=NULL, updated_at=? WHERE task_id=?",
                    (TaskState.LEASED.value, attempts, lease_id, expires, now, task.task_id),
                )
                self._add_event(
                    conn, task.task_id, "leased",
                    f"lease_id={lease_id} attempt={attempts}/{task.max_attempts}"
                    f" expires_at={expires}{waited_detail}",
                    now,
                )
                conn.execute("COMMIT")
                return self.get_task(task.task_id)
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def _finish_lease(
        self, conn: sqlite3.Connection, task_id: str, lease_id: str, now: float
    ) -> Task | None:
        """Validate an active lease; return the task or None if invalid."""
        row = conn.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
        if row is None:
            return None
        task = self._row_to_task(row)
        if (
            task.state != TaskState.LEASED
            or task.lease_id != lease_id
            or task.lease_expires_at is None
            or task.lease_expires_at <= now
        ):
            return None
        return task

    def complete_task(self, task_id: str, lease_id: str, now: float) -> Task | None:
        """Mark a leased task succeeded; None if the lease is not valid."""
        conn = self._require_conn()
        with self._lock:
            conn.execute("BEGIN IMMEDIATE")
            try:
                task = self._finish_lease(conn, task_id, lease_id, now)
                if task is None:
                    conn.execute("COMMIT")
                    return None
                conn.execute(
                    "UPDATE tasks SET state=?, lease_id=NULL, lease_expires_at=NULL,"
                    " updated_at=? WHERE task_id=?",
                    (TaskState.SUCCEEDED.value, now, task_id),
                )
                self._add_event(
                    conn, task_id, "succeeded",
                    f"lease_id={lease_id} attempt={task.attempts}", now,
                )
                conn.execute("COMMIT")
                return self.get_task(task_id)
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def fail_task(
        self,
        task_id: str,
        lease_id: str,
        retryable: bool,
        now: float,
        max_attempts: int,
        retry_delay_seconds: float = 0.0,
    ) -> Task | None:
        """Record a failure; route to PENDING (scheduled retry), DEAD, or FAILED.

        A retryable failure below the attempt cap goes back to PENDING with
        next_attempt_at = now + retry_delay_seconds, so it stays invisible to
        consumers until the backoff wait elapses.
        """
        conn = self._require_conn()
        with self._lock:
            conn.execute("BEGIN IMMEDIATE")
            try:
                task = self._finish_lease(conn, task_id, lease_id, now)
                if task is None:
                    conn.execute("COMMIT")
                    return None
                next_attempt_at: float | None = None
                schedule_detail = ""
                if not retryable:
                    new_state, event = TaskState.FAILED, "failed_non_retryable"
                elif task.attempts >= max_attempts:
                    new_state, event = TaskState.DEAD, "dead_lettered"
                else:
                    new_state, event = TaskState.PENDING, "failed_retryable"
                    next_attempt_at = now + retry_delay_seconds
                    schedule_detail = (
                        f" delay_seconds={retry_delay_seconds:.6f}"
                        f" next_attempt_at={next_attempt_at}"
                    )
                conn.execute(
                    "UPDATE tasks SET state=?, lease_id=NULL, lease_expires_at=NULL,"
                    " next_attempt_at=?, updated_at=? WHERE task_id=?",
                    (new_state.value, next_attempt_at, now, task_id),
                )
                self._add_event(
                    conn, task_id, event,
                    f"lease_id={lease_id} attempt={task.attempts}/{max_attempts}"
                    f" retryable={retryable}{schedule_detail}",
                    now,
                )
                conn.execute("COMMIT")
                return self.get_task(task_id)
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def list_events(self, task_id: str) -> list[TaskEvent]:
        conn = self._require_conn()
        rows = conn.execute(
            "SELECT * FROM events WHERE task_id=? ORDER BY event_id", (task_id,)
        ).fetchall()
        return [
            TaskEvent(
                event_id=r["event_id"],
                task_id=r["task_id"],
                event_type=r["event_type"],
                detail=r["detail"],
                created_at=r["created_at"],
            )
            for r in rows
        ]
