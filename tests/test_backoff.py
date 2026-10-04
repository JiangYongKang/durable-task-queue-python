"""Tests for backoff-based retry scheduling.

Covers: no delivery during the wait window, delivery after it elapses,
exponential growth with a cap, waiting tasks not blocking new ones,
concurrent consumers at mass expiry, duplicate/stale failure reports,
and wait progress surviving a restart. Logs print idempotency keys,
leases, attempt counts, and wait durations.
"""

import logging
import os
import threading

import pytest

from taskqueue import QueueConfig, QueueService
from taskqueue.models import TaskState
from taskqueue.service import LeaseError

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
log = logging.getLogger("test.backoff")


class FakeClock:
    def __init__(self, start: float = 1_000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture()
def clock():
    return FakeClock()


@pytest.fixture()
def svc(tmp_path, clock):
    service = QueueService(
        QueueConfig(
            db_path=os.path.join(tmp_path, "backoff.db"),
            lease_seconds=10.0,
            max_attempts=5,
            retry_initial_delay_seconds=1.0,
            retry_max_delay_seconds=3.0,
        ),
        clock=clock,
    )
    service.open()
    yield service
    service.close()


def test_no_delivery_during_wait_then_delivered(svc, clock):
    task, _ = svc.submit("key-wait", {"job": "wait"})
    leased, lease_id = svc.acquire()
    assert leased.attempts == 1

    failed = svc.fail(task.task_id, lease_id, retryable=True)
    assert failed.state == TaskState.PENDING
    assert failed.next_attempt_at == pytest.approx(clock.now + 1.0)
    log.info(
        "failed idempotency_key=%s lease_id=%s attempt=%d next_attempt_at=%.3f",
        failed.idempotency_key, lease_id, failed.attempts, failed.next_attempt_at,
    )

    # Still inside the wait window: no consumer can lease it.
    clock.advance(0.5)
    assert svc.acquire() is None
    view = svc.get_task(task.task_id)
    assert view.next_attempt_at == pytest.approx(failed.next_attempt_at)
    log.info(
        "waiting idempotency_key=%s attempts=%d next_attempt_at=%.3f now=%.3f",
        view.idempotency_key, view.attempts, view.next_attempt_at, clock.now,
    )

    # Past the due time: deliverable again, attempts move to 2.
    clock.advance(0.6)
    redelivered, lease_id2 = svc.acquire()
    assert redelivered.task_id == task.task_id
    assert redelivered.attempts == 2
    assert redelivered.next_attempt_at is None  # cleared while leased

    history = svc.get_history(task.task_id)
    fail_ev = next(e for e in history if e.event_type == "failed_retryable")
    assert "attempt=1/5" in fail_ev.detail
    assert "delay_seconds=1.0" in fail_ev.detail
    assert "next_attempt_at=" in fail_ev.detail
    lease_ev = [e for e in history if e.event_type == "leased"][-1]
    assert "waited_seconds=" in lease_ev.detail
    waited = float(lease_ev.detail.split("waited_seconds=")[1])
    assert waited == pytest.approx(1.1)
    log.info(
        "redelivered idempotency_key=%s lease_id=%s attempt=%d waited_seconds=%.3f",
        redelivered.idempotency_key, lease_id2, redelivered.attempts, waited,
    )


def test_backoff_grows_exponentially_and_caps(svc, clock):
    task, _ = svc.submit("key-backoff", {"job": "flaky"})
    expected_delays = [1.0, 2.0, 3.0, 3.0]  # 2**n growth capped at max=3.0
    for attempt, expected in enumerate(expected_delays, start=1):
        leased, lease_id = svc.acquire()
        assert leased.attempts == attempt
        failed = svc.fail(task.task_id, lease_id, retryable=True)
        assert failed.next_attempt_at == pytest.approx(clock.now + expected)
        log.info(
            "backoff idempotency_key=%s attempt=%d delay_seconds=%.1f",
            failed.idempotency_key, attempt, expected,
        )
        clock.advance(expected)


def test_waiting_task_does_not_block_new_task(svc, clock):
    old, _ = svc.submit("key-old", {"job": "old"})
    leased, lease_id = svc.acquire()
    svc.fail(old.task_id, lease_id, retryable=True)  # waits 1s from now

    new, _ = svc.submit("key-new", {"job": "new"})
    result = svc.acquire()
    assert result is not None
    leased_new, _ = result
    assert leased_new.task_id == new.task_id
    log.info(
        "new task delivered while retry waits: new=%s waiting=%s next_attempt_at=%.3f",
        leased_new.idempotency_key, old.idempotency_key,
        svc.get_task(old.task_id).next_attempt_at,
    )


def test_concurrent_acquire_on_mass_expiry(svc, clock):
    n_tasks, n_workers = 12, 4
    task_ids = []
    for i in range(n_tasks):
        task, _ = svc.submit(f"key-mass-{i}", {"n": i})
        _, lease_id = svc.acquire()
        svc.fail(task.task_id, lease_id, retryable=True)
        task_ids.append(task.task_id)
    log.info("mass expiry: %d tasks all due at %.3f", n_tasks, clock.now + 1.0)

    clock.advance(1.5)  # all waits elapse at once
    leased_ids: list[str] = []
    errors: list[Exception] = []
    lock = threading.Lock()

    def worker(name: str) -> None:
        try:
            while True:
                result = svc.acquire()
                if result is None:
                    return
                task, lease_id = result
                with lock:
                    leased_ids.append(task.task_id)
                log.info(
                    "worker=%s leased idempotency_key=%s lease_id=%s attempt=%d",
                    name, task.idempotency_key, lease_id, task.attempts,
                )
        except Exception as exc:  # pragma: no cover - surfaced via assertion
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(f"w{i}",)) for i in range(n_workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    assert not errors
    # Each expired task leased exactly once; no duplicates, no losses.
    assert sorted(leased_ids) == sorted(task_ids)
    for task_id in task_ids:
        task = svc.get_task(task_id)
        assert task.state == TaskState.LEASED
        assert task.attempts == 2
    log.info("mass expiry verified: %d unique leases across %d workers",
             len(set(leased_ids)), n_workers)


def test_duplicate_and_stale_failure_reports_rejected(svc, clock):
    task, _ = svc.submit("key-dup-fail", {"job": "dup"})
    _, lease_id = svc.acquire()
    failed = svc.fail(task.task_id, lease_id, retryable=True)
    first_due = failed.next_attempt_at

    # Same failure reported twice with the same lease: rejected, no extra
    # attempt counted, no re-queued wait.
    with pytest.raises(LeaseError):
        svc.fail(task.task_id, lease_id, retryable=True)
    view = svc.get_task(task.task_id)
    assert view.attempts == 1
    assert view.next_attempt_at == first_due
    kinds = [e.event_type for e in svc.get_history(task.task_id)]
    assert kinds.count("failed_retryable") == 1
    log.info(
        "duplicate fail rejected idempotency_key=%s lease_id=%s attempts=%d",
        view.idempotency_key, lease_id, view.attempts,
    )

    # Lease expires after redelivery; a late failure report is rejected too.
    clock.advance(1.5)
    _, lease_id2 = svc.acquire()
    clock.advance(11.0)  # lease_seconds=10 -> lease2 now stale
    with pytest.raises(LeaseError):
        svc.fail(task.task_id, lease_id2, retryable=True)
    view = svc.get_task(task.task_id)
    assert view.attempts == 2
    log.info(
        "stale fail rejected idempotency_key=%s lease_id=%s attempts=%d",
        view.idempotency_key, lease_id2, view.attempts,
    )

    # The expired lease still redelivers exactly once.
    redelivered, lease_id3 = svc.acquire()
    assert redelivered.task_id == task.task_id
    assert redelivered.attempts == 3
    done = svc.ack(task.task_id, lease_id3)
    assert done.state == TaskState.SUCCEEDED


def test_restart_preserves_backoff_progress(tmp_path):
    clock = FakeClock()
    db = os.path.join(tmp_path, "restart-backoff.db")
    cfg = QueueConfig(
        db_path=db,
        lease_seconds=10.0,
        max_attempts=5,
        retry_initial_delay_seconds=1.0,
        retry_max_delay_seconds=3.0,
    )

    svc1 = QueueService(cfg, clock=clock)
    svc1.open()
    task, _ = svc1.submit("key-restart-wait", {"job": "persist"})
    _, lease_id = svc1.acquire()
    svc1.fail(task.task_id, lease_id, retryable=True)
    due = svc1.get_task(task.task_id).next_attempt_at
    svc1.close()
    log.info(
        "before restart idempotency_key=%s attempts=%d next_attempt_at=%.3f now=%.3f",
        task.idempotency_key, 1, due, clock.now,
    )

    # Restart: the wait must survive — not delivered early...
    svc2 = QueueService(cfg, clock=clock)
    svc2.open()
    clock.advance(0.5)
    assert svc2.acquire() is None
    restored = svc2.get_task(task.task_id)
    assert restored.state == TaskState.PENDING
    assert restored.attempts == 1
    assert restored.next_attempt_at == pytest.approx(due)

    # ...and not delayed past the due time either.
    clock.advance(0.6)
    redelivered, lease_id2 = svc2.acquire()
    assert redelivered.task_id == task.task_id
    assert redelivered.attempts == 2
    history = svc2.get_history(task.task_id)
    kinds = [e.event_type for e in history]
    assert kinds == ["submitted", "leased", "failed_retryable", "leased"]
    log.info(
        "after restart idempotency_key=%s lease_id=%s attempts=%d events=%s",
        redelivered.idempotency_key, lease_id2, redelivered.attempts, kinds,
    )
    svc2.close()
