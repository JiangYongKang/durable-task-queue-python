"""Task queue core: submission, leasing, completion, retry and dead-lettering.

Delivery semantics (at-least-once with a single valid lease):

* a task that is *pending/retrying* and due is handed to a consumer together
  with a **lease**; at most one lease per task is valid at any instant,
  because every transition is serialised by the store lock;
* a consumer reports success/failure against its ``lease_id``; a stale or
  expired lease is rejected, so an ack from a zombie consumer can never
  double-complete a task;
* a lease that is neither renewed nor reported in time is reclaimed: the
  attempt still counts, it is recorded as ``timeout`` and the task is
  redelivered (or dead-lettered once ``max_attempts`` is reached);
* ``permanent`` failures move straight to the terminal ``dead`` state;
  ``retryable`` failures are rescheduled with exponential backoff until the
  attempt budget is exhausted, then dead-lettered.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
import uuid
from contextlib import contextmanager
from typing import Any, Iterator

from .config import QueueConfig
from .models import DeliveryRecord, FailureKind, RejectReason, Task, TaskState
from .store import TaskStore

logger = logging.getLogger("taskqueue")


class SubmissionRejected(Exception):
    def __init__(self, reason: RejectReason, message: str) -> None:
        super().__init__(message)
        self.reason = reason
        self.message = message


class LeaseError(Exception):
    """Raised when a lease token is unknown, stale or expired."""


@contextmanager
def _transaction(store: TaskStore) -> Iterator[Any]:
    with store.lock:
        conn = store.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise


class TaskQueue:
    def __init__(self, config: QueueConfig | None = None) -> None:
        self.config = config or QueueConfig()
        self.store = TaskStore(self.config.db_path)

    # ------------------------------------------------------------------ #
    # lifecycle
    # ------------------------------------------------------------------ #

    def close(self) -> None:
        self.store.close()

    def __enter__(self) -> TaskQueue:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ------------------------------------------------------------------ #
    # helpers
    # ------------------------------------------------------------------ #

    @staticmethod
    def _hash_payload(payload: str) -> str:
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def _validate_payload(self, payload: str) -> None:
        if not isinstance(payload, str) or not payload.strip():
            raise SubmissionRejected(
                RejectReason.INVALID_PAYLOAD,
                "payload must be a non-empty JSON object string",
            )
        if len(payload.encode("utf-8")) > self.config.max_payload_bytes:
            raise SubmissionRejected(
                RejectReason.PAYLOAD_TOO_LARGE,
                f"payload exceeds {self.config.max_payload_bytes} bytes",
            )
        try:
            decoded = json.loads(payload)
        except (json.JSONDecodeError, ValueError) as exc:
            raise SubmissionRejected(
                RejectReason.INVALID_PAYLOAD, f"payload is not valid JSON: {exc}"
            ) from exc
        if not isinstance(decoded, dict):
            raise SubmissionRejected(
                RejectReason.INVALID_PAYLOAD, "payload must be a JSON object"
            )

    def _to_task(self, row: dict[str, Any]) -> Task:
        deliveries = [
            DeliveryRecord(
                attempt=d["attempt"],
                lease_id=d["lease_id"],
                consumer=d["consumer"],
                leased_at=d["leased_at"],
                expires_at=d["expires_at"],
                reported_at=d["reported_at"],
                result=d["result"],
                failure_kind=d["failure_kind"],
                error=d["error"],
            )
            for d in self.store.get_delivery_rows(row["id"])
        ]
        return Task(
            id=row["id"],
            idempotency_key=row["idempotency_key"],
            payload=row["payload"],
            state=TaskState(row["state"]),
            attempt_count=row["attempt_count"],
            created_at=row["created_at"],
            available_at=row["available_at"],
            lease_id=row["lease_id"],
            leased_to=row["leased_to"],
            leased_at=row["leased_at"],
            lease_expires_at=row["lease_expires_at"],
            last_error=row["last_error"],
            history=deliveries,
        )

    def _backoff_delay(self, failed_attempt: int) -> float:
        return self.config.retry_backoff_base * (2 ** (failed_attempt - 1))

    # ------------------------------------------------------------------ #
    # submit + dedup
    # ------------------------------------------------------------------ #

    def submit(
        self, payload: str, idempotency_key: str | None = None
    ) -> tuple[Task, bool]:
        """Submit a task.

        Returns ``(task, created)``. ``created=False`` means an existing task
        with the same idempotency key *and* identical payload was returned.
        Raises :class:`SubmissionRejected` for invalid/oversized payloads or a
        reused key with a different payload.
        """
        self._validate_payload(payload)
        key = idempotency_key or uuid.uuid4().hex
        payload_hash = self._hash_payload(payload)
        now = time.time()

        with _transaction(self.store) as conn:
            existing = conn.execute(
                "SELECT * FROM tasks WHERE idempotency_key = ?", (key,)
            ).fetchone()
            if existing is not None:
                if existing["payload_hash"] != payload_hash:
                    logger.warning(
                        "submit rejected idempotency_key=%s reason=%s",
                        key,
                        RejectReason.PAYLOAD_MISMATCH.value,
                    )
                    raise SubmissionRejected(
                        RejectReason.PAYLOAD_MISMATCH,
                        "idempotency key already exists with a different payload",
                    )
                logger.info(
                    "submit deduplicated idempotency_key=%s task_id=%s state=%s",
                    key,
                    existing["id"],
                    existing["state"],
                )
                row, created = dict(existing), False
            else:
                task_id = uuid.uuid4().hex
                conn.execute(
                    """INSERT INTO tasks
                           (id, idempotency_key, payload, payload_hash, state,
                            attempt_count, created_at, available_at)
                       VALUES (?, ?, ?, ?, 'pending', 0, ?, ?)""",
                    (task_id, key, payload, payload_hash, now, now),
                )
                logger.info(
                    "submit created idempotency_key=%s task_id=%s", key, task_id
                )
                row, created = (
                    dict(
                        conn.execute(
                            "SELECT * FROM tasks WHERE id = ?", (task_id,)
                        ).fetchone()
                    ),
                    True,
                )
            # build the snapshot inside the lock/transaction so it cannot race
            # with a concurrent close() or another state transition
            task = self._to_task(row)
        return task, created

    # ------------------------------------------------------------------ #
    # lease / claim
    # ------------------------------------------------------------------ #

    def lease(self, consumer: str | None = None) -> Task | None:
        """Atomically claim one due task; ``None`` when nothing is available."""
        now = time.time()
        timeout = self.config.visibility_timeout
        with _transaction(self.store) as conn:
            row = conn.execute(
                """SELECT * FROM tasks
                   WHERE state IN ('pending', 'retrying')
                     AND available_at <= ?
                   ORDER BY available_at, created_at
                   LIMIT 1""",
                (now,),
            ).fetchone()
            if row is None:
                return None
            row = dict(row)
            attempt = row["attempt_count"] + 1
            lease_id = uuid.uuid4().hex
            expires_at = now + timeout
            conn.execute(
                """UPDATE tasks
                   SET state='inflight', attempt_count=?, lease_id=?, leased_to=?,
                       leased_at=?, lease_expires_at=?, available_at=?
                   WHERE id=?""",
                (attempt, lease_id, consumer, now, expires_at, expires_at, row["id"]),
            )
            conn.execute(
                """INSERT INTO deliveries
                       (task_id, attempt, lease_id, consumer, leased_at,
                        expires_at, reported_at, result)
                   VALUES (?, ?, ?, ?, ?, ?, NULL, 'leased')""",
                (row["id"], attempt, lease_id, consumer, now, expires_at),
            )
            row.update(
                state="inflight",
                attempt_count=attempt,
                lease_id=lease_id,
                leased_to=consumer,
                leased_at=now,
                lease_expires_at=expires_at,
                available_at=expires_at,
            )
            task = self._to_task(row)
        logger.info(
            "lease granted task_id=%s idempotency_key=%s lease_id=%s consumer=%s "
            "attempt=%d expires_at=%.3f",
            task.id, task.idempotency_key, lease_id, consumer, attempt, expires_at,
        )
        print(
            f"[lease] idempotency_key={task.idempotency_key} lease_id={lease_id} "
            f"consumer={consumer} attempt={attempt} expires_at={expires_at:.3f}",
            flush=True,
        )
        return task

    # ------------------------------------------------------------------ #
    # result reporting
    # ------------------------------------------------------------------ #

    def _load_active_lease(
        self, conn: Any, task_id: str, lease_id: str, now: float
    ) -> dict[str, Any]:
        row = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
        if row is None:
            raise LeaseError(f"unknown task {task_id}")
        row = dict(row)
        if row["lease_id"] != lease_id:
            raise LeaseError(
                f"lease {lease_id} is not the current lease for task {task_id}"
            )
        if row["state"] != "inflight" or (
            row["lease_expires_at"] is not None and row["lease_expires_at"] <= now
        ):
            raise LeaseError(f"lease {lease_id} is expired or no longer active")
        return row

    @staticmethod
    def _finish_delivery(
        conn: Any, task_id: str, lease_id: str, now: float,
        result: str, failure_kind: str | None, error: str | None,
    ) -> None:
        conn.execute(
            """UPDATE deliveries
               SET reported_at=?, result=?, failure_kind=?, error=?
               WHERE task_id=? AND lease_id=? AND result='leased'""",
            (now, result, failure_kind, error, task_id, lease_id),
        )

    def complete(self, task_id: str, lease_id: str, result: str = "succeeded") -> Task:
        now = time.time()
        with _transaction(self.store) as conn:
            row = self._load_active_lease(conn, task_id, lease_id, now)
            self._finish_delivery(conn, task_id, lease_id, now, "succeeded", None, None)
            conn.execute(
                """UPDATE tasks
                   SET state='succeeded', lease_id=NULL, leased_to=NULL,
                       leased_at=NULL, lease_expires_at=NULL, available_at=?,
                       last_error=NULL
                   WHERE id=?""",
                (now, task_id),
            )
            row.update(
                state="succeeded", lease_id=None, leased_to=None, leased_at=None,
                lease_expires_at=None, available_at=now, last_error=None,
            )
            task = self._to_task(row)
        logger.info(
            "task succeeded task_id=%s idempotency_key=%s lease_id=%s attempts=%d",
            task_id, task.idempotency_key, lease_id, task.attempt_count,
        )
        print(
            f"[complete] idempotency_key={task.idempotency_key} "
            f"lease_id={lease_id} attempts={task.attempt_count}",
            flush=True,
        )
        return task

    def fail(
        self,
        task_id: str,
        lease_id: str,
        kind: FailureKind,
        error: str | None = None,
    ) -> Task:
        now = time.time()
        with _transaction(self.store) as conn:
            row = self._load_active_lease(conn, task_id, lease_id, now)
            attempts = row["attempt_count"]

            if kind == FailureKind.PERMANENT or attempts >= self.config.max_attempts:
                new_state = TaskState.DEAD.value
                available_at = now
                delay = None
                delivery_result = "permanent_failure" if kind == FailureKind.PERMANENT else "retry_exhausted"
            else:
                new_state = TaskState.RETRYING.value
                delay = self._backoff_delay(attempts)
                available_at = now + delay
                delivery_result = "retryable_failure"

            self._finish_delivery(
                conn, task_id, lease_id, now,
                delivery_result, kind.value, error,
            )
            conn.execute(
                """UPDATE tasks
                   SET state=?, lease_id=NULL, leased_to=NULL, leased_at=NULL,
                       lease_expires_at=NULL, available_at=?, last_error=?
                   WHERE id=?""",
                (new_state, available_at, error, task_id),
            )
            row.update(
                state=new_state, lease_id=None, leased_to=None, leased_at=None,
                lease_expires_at=None, available_at=available_at, last_error=error,
            )
            task = self._to_task(row)
        logger.info(
            "task failed task_id=%s idempotency_key=%s lease_id=%s kind=%s "
            "attempt=%d/%d state=%s backoff=%s",
            task_id, task.idempotency_key, lease_id, kind.value, attempts,
            self.config.max_attempts, new_state, delay,
        )
        print(
            f"[fail] idempotency_key={task.idempotency_key} lease_id={lease_id} "
            f"kind={kind.value} attempt={attempts}/{self.config.max_attempts} "
            f"state={new_state} backoff={delay}",
            flush=True,
        )
        return task

    def heartbeat(self, task_id: str, lease_id: str) -> Task:
        now = time.time()
        with _transaction(self.store) as conn:
            row = self._load_active_lease(conn, task_id, lease_id, now)
            expires_at = now + self.config.visibility_timeout
            conn.execute(
                "UPDATE tasks SET lease_expires_at=?, available_at=? WHERE id=?",
                (expires_at, expires_at, task_id),
            )
            conn.execute(
                """UPDATE deliveries SET expires_at=?
                   WHERE task_id=? AND lease_id=? AND result='leased'""",
                (expires_at, task_id, lease_id),
            )
            row.update(lease_expires_at=expires_at, available_at=expires_at)
            task = self._to_task(row)
        logger.info(
            "heartbeat task_id=%s lease_id=%s expires_at=%.3f",
            task_id, lease_id, expires_at,
        )
        return task

    # ------------------------------------------------------------------ #
    # lease expiry / reaping
    # ------------------------------------------------------------------ #

    def reclaim_expired(self, now: float | None = None) -> int:
        """Re-drive inflight tasks whose leases expired; returns reclaimed count."""
        now = now if now is not None else time.time()
        reclaimed = 0
        with _transaction(self.store) as conn:
            rows = conn.execute(
                """SELECT * FROM tasks
                   WHERE state='inflight' AND lease_expires_at <= ?""",
                (now,),
            ).fetchall()
            for raw in rows:
                r = dict(raw)
                attempts = r["attempt_count"]
                if attempts >= self.config.max_attempts:
                    new_state = TaskState.DEAD.value
                    available_at = now
                    delivery_result = "timeout_exhausted"
                else:
                    new_state = TaskState.RETRYING.value
                    available_at = now
                    delivery_result = "timeout"
                conn.execute(
                    """UPDATE deliveries
                       SET reported_at=?, result=?, failure_kind=?, error=?
                       WHERE task_id=? AND lease_id=? AND result='leased'""",
                    (now, delivery_result, FailureKind.RETRYABLE.value,
                     "lease expired without report", r["id"], r["lease_id"]),
                )
                conn.execute(
                    """UPDATE tasks
                       SET state=?, lease_id=NULL, leased_to=NULL, leased_at=NULL,
                           lease_expires_at=NULL, available_at=?,
                           last_error=?
                       WHERE id=?""",
                    (new_state, available_at, "lease expired without report", r["id"]),
                )
                reclaimed += 1
                logger.info(
                    "lease reclaimed task_id=%s idempotency_key=%s lease_id=%s "
                    "attempt=%d/%d state=%s",
                    r["id"], r["idempotency_key"], r["lease_id"], attempts,
                    self.config.max_attempts, new_state,
                )
                print(
                    f"[reap] idempotency_key={r['idempotency_key']} "
                    f"lease_id={r['lease_id']} attempt={attempts}/"
                    f"{self.config.max_attempts} state={new_state}",
                    flush=True,
                )
        return reclaimed

    # ------------------------------------------------------------------ #
    # queries
    # ------------------------------------------------------------------ #

    def get_task(self, task_id: str) -> Task | None:
        with self.store.lock:
            row = self.store.get_task_row(task_id)
            return self._to_task(row) if row else None

    def get_task_by_key(self, idempotency_key: str) -> Task | None:
        with self.store.lock:
            row = self.store.get_task_by_idempotency_key(idempotency_key)
            return self._to_task(row) if row else None
