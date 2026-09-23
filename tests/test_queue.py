"""Tests for the durable task queue.

Covers duplicate submission (idempotent dedup), lease expiry redelivery,
retry/dead-letter routing, restart persistence, and multi-consumer
concurrency. Logs print idempotency keys, leases, and attempt counts.
"""

import logging
import os
import threading
import time

import pytest

from taskqueue import QueueConfig, QueueService
from taskqueue.models import TaskState
from taskqueue.service import LeaseError, SubmitRejected

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
log = logging.getLogger("test.queue")


@pytest.fixture()
def svc(tmp_path):
    service = QueueService(
        QueueConfig(
            db_path=os.path.join(tmp_path, "test.db"),
            lease_seconds=0.1,
            max_attempts=3,
        )
    )
    service.open()
    yield service
    service.close()


def test_duplicate_submission_dedup(svc):
    task, created = svc.submit("key-1", {"job": "a"})
    assert created
    log.info("submit idempotency_key=%s task_id=%s created=%s", "key-1", task.task_id, created)

    again, created_again = svc.submit("key-1", {"job": "a"})
    assert not created_again
    assert again.task_id == task.task_id
    log.info(
        "resubmit idempotency_key=%s deduplicated task_id=%s attempts=%d",
        "key-1", again.task_id, again.attempts,
    )

    with pytest.raises(SubmitRejected) as exc:
        svc.submit("key-1", {"job": "different"})
    assert exc.value.reason == "payload_conflict"
    log.info("conflict idempotency_key=%s reason=%s", "key-1", exc.value.reason)


def test_invalid_submissions_rejected(svc):
    with pytest.raises(SubmitRejected) as exc:
        svc.submit("", {"a": 1})
    assert exc.value.reason == "invalid_idempotency_key"

    with pytest.raises(SubmitRejected) as exc:
        svc.submit("key-bad-payload", object())
    assert exc.value.reason == "invalid_payload"

    with pytest.raises(SubmitRejected) as exc:
        svc.submit("key-too-large", "x" * (svc._config.max_payload_bytes + 1))
    assert exc.value.reason == "payload_too_large"
    log.info("rejections verified: invalid key, invalid payload, oversized payload")


def test_lease_expiry_redelivery(svc):
    task, _ = svc.submit("key-lease", {"job": "lease"})
    leased, lease_id = svc.acquire()
    assert leased.task_id == task.task_id and leased.attempts == 1
    log.info(
        "leased idempotency_key=%s lease_id=%s attempt=%d",
        leased.idempotency_key, lease_id, leased.attempts,
    )

    # Consumer "crashes": no ack/fail. Lease expires and task is redelivered.
    time.sleep(0.15)
    redelivered, lease_id2 = svc.acquire()
    assert redelivered.task_id == task.task_id
    assert redelivered.attempts == 2
    assert lease_id2 != lease_id
    log.info(
        "redelivered idempotency_key=%s old_lease=%s new_lease=%s attempt=%d",
        redelivered.idempotency_key, lease_id, lease_id2, redelivered.attempts,
    )

    # The stale lease can no longer ack; the new lease succeeds exactly once.
    with pytest.raises(LeaseError):
        svc.ack(task.task_id, lease_id)
    done = svc.ack(task.task_id, lease_id2)
    assert done.state == TaskState.SUCCEEDED
    assert svc.acquire() is None
    log.info(
        "acked idempotency_key=%s lease_id=%s final_state=%s attempts=%d",
        done.idempotency_key, lease_id2, done.state.value, done.attempts,
    )


def test_retry_then_dead_letter(svc):
    task, _ = svc.submit("key-retry", {"job": "flaky"})
    for expected_attempt in (1, 2, 3):
        leased, lease_id = svc.acquire()
        assert leased.task_id == task.task_id
        assert leased.attempts == expected_attempt
        failed = svc.fail(task.task_id, lease_id, retryable=True)
        log.info(
            "failed idempotency_key=%s lease_id=%s attempt=%d state=%s",
            failed.idempotency_key, lease_id, failed.attempts, failed.state.value,
        )
    assert failed.state == TaskState.DEAD
    assert svc.acquire() is None

    history = svc.get_history(task.task_id)
    kinds = [e.event_type for e in history]
    assert kinds.count("leased") == 3
    assert kinds.count("failed_retryable") == 2
    assert kinds.count("dead_lettered") == 1
    log.info("history idempotency_key=%s events=%s", task.idempotency_key, kinds)


def test_non_retryable_failure_is_terminal(svc):
    task, _ = svc.submit("key-fatal", {"job": "bad"})
    leased, lease_id = svc.acquire()
    failed = svc.fail(task.task_id, lease_id, retryable=False)
    assert failed.state == TaskState.FAILED
    assert failed.attempts == 1
    assert svc.acquire() is None
    log.info(
        "terminal idempotency_key=%s lease_id=%s state=%s attempts=%d",
        failed.idempotency_key, lease_id, failed.state.value, failed.attempts,
    )


def test_restart_preserves_state(tmp_path):
    db = os.path.join(tmp_path, "restart.db")
    cfg = QueueConfig(db_path=db, lease_seconds=0.1, max_attempts=3)

    svc1 = QueueService(cfg)
    svc1.open()
    task, _ = svc1.submit("key-restart", {"job": "persist"})
    leased, lease_id = svc1.acquire()
    svc1.fail(task.task_id, lease_id, retryable=True)
    svc1.close()
    log.info(
        "before restart idempotency_key=%s attempts=%d state=%s",
        task.idempotency_key, 1, "pending",
    )

    svc2 = QueueService(cfg)
    svc2.open()
    restored = svc2.get_task(task.task_id)
    assert restored is not None
    assert restored.state == TaskState.PENDING
    assert restored.attempts == 1  # not lost, not double-counted
    history = svc2.get_history(task.task_id)
    assert [e.event_type for e in history] == ["submitted", "leased", "failed_retryable"]
    log.info(
        "after restart idempotency_key=%s attempts=%d state=%s events=%d",
        restored.idempotency_key, restored.attempts, restored.state.value, len(history),
    )
    svc2.close()


def test_concurrent_consumers(svc):
    n_tasks, n_workers = 20, 8
    ids = {}
    for i in range(n_tasks):
        task, _ = svc.submit(f"key-conc-{i}", {"n": i})
        ids[task.task_id] = task.idempotency_key

    acked: list[str] = []
    ack_lock = threading.Lock()
    errors: list[Exception] = []

    def worker(name: str) -> None:
        try:
            while True:
                result = svc.acquire()
                if result is None:
                    return
                task, lease_id = result
                log.info(
                    "worker=%s leased idempotency_key=%s lease_id=%s attempt=%d",
                    name, task.idempotency_key, lease_id, task.attempts,
                )
                done = svc.ack(task.task_id, lease_id)
                with ack_lock:
                    acked.append(done.task_id)
                log.info(
                    "worker=%s acked idempotency_key=%s lease_id=%s attempts=%d",
                    name, done.idempotency_key, lease_id, done.attempts,
                )
        except Exception as exc:  # pragma: no cover - surfaced via assertion
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(f"w{i}",)) for i in range(n_workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    assert not errors
    # Every task succeeded exactly once: no duplicates, no losses.
    assert sorted(acked) == sorted(ids)
    for task_id, key in ids.items():
        task = svc.get_task(task_id)
        assert task.state == TaskState.SUCCEEDED
        assert task.attempts == 1
    log.info(
        "concurrency verified: %d tasks, %d workers, %d unique acks",
        n_tasks, n_workers, len(set(acked)),
    )
