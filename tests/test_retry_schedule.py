"""Tests for delayed retry scheduling (waiting period + capped backoff).

Covers: waiting tasks are not deliverable before not_before, delivery after
the wait, waiting tasks not blocking fresh tasks, concurrent acquisition at
batch expiry, duplicate failure reports, failure reported after lease
expiry, and restart preserving the wait schedule. Logs print idempotency
keys, leases, attempt counts and wait durations.
"""

import logging
import os
import threading

import pytest

from taskqueue import QueueConfig, QueueService
from taskqueue.models import TaskState
from taskqueue.service import LeaseError

log = logging.getLogger("test.retry_schedule")


class FakeClock:
    """Deterministic monotonic clock shared across threads."""

    def __init__(self, start: float = 1000.0) -> None:
        self._now = start
        self._cond = threading.Condition()

    def time(self) -> float:
        with self._cond:
            return self._now

    def advance(self, seconds: float) -> None:
        with self._cond:
            self._now += seconds

    def set(self, value: float) -> None:
        with self._cond:
            self._now = value


@pytest.fixture()
def clock() -> FakeClock:
    return FakeClock()


def make_service(tmp_path, clock: FakeClock, **overrides) -> QueueService:
    settings = dict(
        db_path=os.path.join(tmp_path, "sched.db"),
        lease_seconds=0.05,
        max_attempts=3,
        retry_initial_delay=0.1,
        retry_max_delay=0.3,
        retry_backoff_factor=2.0,
    )
    settings.update(overrides)
    cfg = QueueConfig(**settings)
    service = QueueService(cfg, clock=clock.time)
    service.open()
    return service


@pytest.fixture()
def svc(tmp_path, clock):
    service = make_service(tmp_path, clock)
    yield service
    service.close()


def test_not_delivered_until_wait_elapses(svc, clock):
    task, _ = svc.submit("key-wait", {"job": "wait"})
    leased, lease_id = svc.acquire()
    assert leased.attempts == 1

    failed = svc.fail(task.task_id, lease_id, retryable=True)
    fail_at = clock.time()
    assert failed.state == TaskState.WAITING
    assert failed.attempts == 1
    assert failed.not_before == pytest.approx(fail_at + 0.1)
    log.info(
        "scheduled idempotency_key=%s lease_id=%s failure_count=1 not_before=%.3f delay=0.1s",
        failed.idempotency_key, lease_id, failed.not_before,
    )

    # Still inside the waiting window: nobody can take it.
    clock.advance(0.05)
    assert svc.acquire() is None
    blocked = svc.get_task(task.task_id)
    assert blocked.state == TaskState.WAITING
    log.info("blocked idempotency_key=%s remaining=%.3fs", task.idempotency_key,
             blocked.not_before - clock.time())

    # Due: redelivered exactly once, attempts incremented by one.
    clock.advance(0.06)
    redelivered, lease_id2 = svc.acquire()
    assert redelivered.task_id == task.task_id
    assert redelivered.attempts == 2
    assert lease_id2 != lease_id
    assert redelivered.not_before is None
    log.info(
        "due idempotency_key=%s old_lease=%s new_lease=%s attempt=%d waited=0.11s",
        redelivered.idempotency_key, lease_id, lease_id2, redelivered.attempts,
    )
    assert svc.acquire() is None  # leased, not due again


def test_backoff_grows_and_is_capped(tmp_path, clock):
    # 5 attempts allowed; delays 0.1, 0.2, 0.3 (cap), 0.3 (cap)
    svc = make_service(tmp_path, clock, max_attempts=5)
    task, _ = svc.submit("key-backoff", {"job": "backoff"})
    expected_delays = [0.1, 0.2, 0.3, 0.3]
    for failure_count, expected in enumerate(expected_delays, start=1):
        leased, lease_id = svc.acquire()
        assert leased.attempts == failure_count
        failed = svc.fail(task.task_id, lease_id, retryable=True)
        assert failed.state == TaskState.WAITING
        assert failed.not_before == pytest.approx(clock.time() + expected)
        log.info(
            "backoff idempotency_key=%s lease_id=%s failure_count=%d delay=%.1fs",
            failed.idempotency_key, lease_id, failure_count, expected,
        )
        clock.advance(expected + 0.01)
    svc.close()


def test_waiting_task_does_not_block_new_pending(svc, clock):
    waiting_task, _ = svc.submit("key-old", {"job": "old"})
    _, lease_id = svc.acquire()
    svc.fail(waiting_task.task_id, lease_id, retryable=True)
    clock.advance(0.01)  # old task still waiting

    fresh_task, _ = svc.submit("key-new", {"job": "new"})
    picked, fresh_lease = svc.acquire()
    assert picked.task_id == fresh_task.task_id  # due new task not head-of-line blocked
    assert svc.acquire() is None  # old task still not due
    log.info(
        "skip-waiting idempotency_key=%s leased while waiting_key=%s not_before=%.3f",
        picked.idempotency_key, waiting_task.idempotency_key,
        svc.get_task(waiting_task.task_id).not_before,
    )
    svc.ack(fresh_task.task_id, fresh_lease)

    clock.advance(0.1)
    old, _ = svc.acquire()
    assert old.task_id == waiting_task.task_id


def test_concurrent_acquire_at_batch_expiry(tmp_path, clock):
    svc = make_service(tmp_path, clock, lease_seconds=10.0)
    n_tasks, n_workers = 30, 10
    ids = set()
    for i in range(n_tasks):
        task, _ = svc.submit(f"key-batch-{i}", {"n": i})
        ids.add(task.task_id)
        _, lease_id = svc.acquire()
        svc.fail(task.task_id, lease_id, retryable=True)
    assert svc.acquire() is None  # all waiting

    delivered: list[str] = []
    errors: list[Exception] = []
    barrier = threading.Barrier(n_workers)
    lock = threading.Lock()

    def worker(name: str) -> None:
        try:
            barrier.wait()
            while True:
                result = svc.acquire()
                if result is None:
                    return
                task, lease_id = result
                with lock:
                    delivered.append(task.task_id)
                log.info(
                    "worker=%s took due idempotency_key=%s lease_id=%s attempt=%d",
                    name, task.idempotency_key, lease_id, task.attempts,
                )
        except Exception as exc:  # pragma: no cover - surfaced via assertion
            errors.append(exc)

    clock.advance(0.2)  # every waiting task becomes due simultaneously
    threads = [threading.Thread(target=worker, args=(f"w{i}",)) for i in range(n_workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    assert not errors
    assert sorted(delivered) == sorted(ids)  # each due task delivered exactly once
    for task_id in ids:
        task = svc.get_task(task_id)
        assert task.attempts == 2  # not double counted under contention
    svc.close()


def test_duplicate_failure_report_is_idempotent(svc, clock):
    task, _ = svc.submit("key-dup-fail", {"job": "dup"})
    _, lease_id = svc.acquire()
    failed = svc.fail(task.task_id, lease_id, retryable=True)
    not_before = failed.not_before
    assert failed.state == TaskState.WAITING

    # Same failure reported twice (retry/duplicate callback): lease is gone,
    # so it must not count another attempt nor schedule another wait.
    with pytest.raises(LeaseError):
        svc.fail(task.task_id, lease_id, retryable=True)
    again = svc.get_task(task.task_id)
    assert again.state == TaskState.WAITING
    assert again.attempts == 1
    assert again.not_before == not_before
    events = svc.get_history(task.task_id)
    assert [e.event_type for e in events].count("failed_retryable") == 1
    log.info(
        "duplicate-fail idempotency_key=%s lease_id=%s attempts=%d schedules=%d"
        " not_before=%.3f",
        task.idempotency_key, lease_id, again.attempts,
        [e.event_type for e in events].count("failed_retryable"), again.not_before,
    )

    clock.advance(0.11)
    redelivered, _ = svc.acquire()
    assert redelivered.attempts == 2  # single scheduled wait, single redelivery


def test_failure_reported_after_lease_expiry_rejected(svc, clock):
    task, _ = svc.submit("key-stale-fail", {"job": "stale"})
    _, lease_id = svc.acquire()

    clock.advance(0.06)  # lease (0.05s) expired, consumer reports failure late
    with pytest.raises(LeaseError):
        svc.fail(task.task_id, lease_id, retryable=True)
    stalled = svc.get_task(task.task_id)
    assert stalled.state == TaskState.LEASED
    assert stalled.not_before is None  # no wait scheduled from a stale report
    assert stalled.attempts == 1
    log.info(
        "stale-fail rejected idempotency_key=%s lease_id=%s attempts=%d",
        task.idempotency_key, lease_id, stalled.attempts,
    )

    redelivered, new_lease = svc.acquire()
    assert redelivered.task_id == task.task_id
    assert redelivered.attempts == 2
    svc.fail(task.task_id, new_lease, retryable=True)
    assert svc.get_task(task.task_id).state == TaskState.WAITING


def test_restart_preserves_waiting_schedule(tmp_path, clock):
    db = os.path.join(tmp_path, "restart-sched.db")

    def build() -> QueueService:
        cfg = QueueConfig(
            db_path=db, lease_seconds=0.05, max_attempts=3,
            retry_initial_delay=0.1, retry_max_delay=0.3, retry_backoff_factor=2.0,
        )
        service = QueueService(cfg, clock=clock.time)
        service.open()
        return service

    svc1 = build()
    task, _ = svc1.submit("key-restart-wait", {"job": "x"})
    _, lease_id = svc1.acquire()
    failed = svc1.fail(task.task_id, lease_id, retryable=True)
    due_at = failed.not_before
    svc1.close()
    log.info(
        "before restart idempotency_key=%s attempts=%d not_before=%.3f",
        task.idempotency_key, 1, due_at,
    )

    svc2 = build()
    restored = svc2.get_task(task.task_id)
    assert restored.state == TaskState.WAITING
    assert restored.attempts == 1
    assert restored.not_before == pytest.approx(due_at)  # neither early nor late

    clock.advance(0.05)
    assert svc2.acquire() is None  # still due at the persisted timestamp
    clock.advance(0.06)
    redelivered, _ = svc2.acquire()
    assert redelivered is not None and redelivered.attempts == 2
    history = svc2.get_history(task.task_id)
    kinds = [e.event_type for e in history]
    assert kinds == ["submitted", "leased", "failed_retryable", "leased"]
    log.info(
        "after restart idempotency_key=%s attempts=%d events=%s",
        redelivered.idempotency_key, redelivered.attempts, kinds,
    )
    svc2.close()


def test_history_records_failure_count_delay_and_waited(svc, clock):
    task, _ = svc.submit("key-history-wait", {"job": "h"})
    _, lease_id = svc.acquire()
    svc.fail(task.task_id, lease_id, retryable=True)
    clock.advance(0.12)  # slightly later than the scheduled 0.1s
    svc.acquire()

    events = svc.get_history(task.task_id)
    by_type = {e.event_type: e for e in events}
    fail_detail = by_type["failed_retryable"].detail
    assert "failure_count=1" in fail_detail
    assert "delay=0.100s" in fail_detail
    assert "not_before=" in fail_detail
    leased_details = [e.detail for e in events if e.event_type == "leased"]
    assert "scheduled_delay=0.100s" in leased_details[-1]
    assert "waited=0.120s" in leased_details[-1]
    assert "retry_after_failure=1" in leased_details[-1]
    log.info(
        "history idempotency_key=%s fail_detail=%s", task.idempotency_key, fail_detail,
    )
    log.info("history idempotency_key=%s redeliver_detail=%s",
             task.idempotency_key, leased_details[-1])


def test_terminal_rules_unchanged_with_backoff_configured(svc, clock):
    # Non-retryable: terminal failed, no waiting scheduled.
    fatal, _ = svc.submit("key-fatal-wait", {"job": "f"})
    _, lease_id = svc.acquire()
    failed = svc.fail(fatal.task_id, lease_id, retryable=False)
    assert failed.state == TaskState.FAILED
    assert failed.not_before is None
    clock.advance(10.0)
    assert svc.acquire() is None

    # Max attempts reached: dead letter even though delays are configured.
    dead, _ = svc.submit("key-dead-wait", {"job": "d"})
    delays = {1: 0.11, 2: 0.21}  # delays after failures 1 and 2 (0.1s, 0.2s)
    result = None
    for expected in (1, 2, 3):
        leased, lid = svc.acquire()
        assert leased.attempts == expected
        result = svc.fail(dead.task_id, lid, retryable=True)
        if expected < 3:
            assert result.state == TaskState.WAITING
            clock.advance(delays[expected])
    assert result.state == TaskState.DEAD
    assert result.not_before is None
    clock.advance(10.0)
    assert svc.acquire() is None
    log.info(
        "terminal-with-backoff idempotency_key=%s final=%s attempts=%d",
        dead.idempotency_key, result.state.value, result.attempts,
    )
