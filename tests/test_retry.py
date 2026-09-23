"""Failure classification, retry policy, dead-lettering and durability."""

from __future__ import annotations

import time

from taskqueue.config import QueueConfig
from taskqueue.models import FailureKind, TaskState
from taskqueue.queue import TaskQueue


def _next_lease(queue):
    t = queue.lease(consumer="w")
    assert t is not None
    print(
        f"[test] leased idempotency_key={t.idempotency_key} lease_id={t.lease_id} "
        f"attempt={t.attempt_count}"
    )
    return t


def test_retryable_retries_until_max_then_dead(queue):
    task, _ = queue.submit('{"job": "flaky"}', "flaky-1")

    # attempt 1 -> retry
    l1 = _next_lease(queue)
    r1 = queue.fail(l1.id, l1.lease_id, FailureKind.RETRYABLE, "transient-1")
    assert r1.state is TaskState.RETRYING and r1.attempt_count == 1
    assert queue.lease() is None  # backoff window
    time.sleep(r1.available_at - time.time() + 0.01)

    # attempt 2 -> retry
    l2 = _next_lease(queue)
    assert l2.attempt_count == 2 and l2.lease_id != l1.lease_id
    r2 = queue.fail(l2.id, l2.lease_id, FailureKind.RETRYABLE, "transient-2")
    time.sleep(r2.available_at - time.time() + 0.01)

    # attempt 3 (== max_attempts) reports retryable -> dead immediately
    l3 = _next_lease(queue)
    r3 = queue.fail(l3.id, l3.lease_id, FailureKind.RETRYABLE, "transient-3")
    assert r3.state is TaskState.DEAD
    assert r3.attempt_count == 3
    print(f"[test] dead idempotency_key=flaky-1 attempts={r3.attempt_count}")

    final = queue.get_task(task.id)
    assert queue.lease() is None  # dead tasks are never redelivered
    results = [h.result for h in final.history]
    # first two attempts are plain retries; the last one explains the dead state
    assert results == ["retryable_failure", "retryable_failure", "retry_exhausted"]
    assert [h.attempt for h in final.history] == [1, 2, 3]
    assert {h.lease_id for h in final.history} == {l1.lease_id, l2.lease_id, l3.lease_id}
    assert final.last_error == "transient-3"


def test_permanent_failure_goes_straight_to_dead(queue):
    task, _ = queue.submit('{"job": "poison"}', "poison-1")
    lease = _next_lease(queue)
    dead = queue.fail(lease.id, lease.lease_id, FailureKind.PERMANENT, "invalid job")
    print(f"[test] permanent idempotency_key=poison-1 lease_id={lease.lease_id} "
          f"attempts={dead.attempt_count} state={dead.state.value}")
    assert dead.state is TaskState.DEAD
    assert dead.attempt_count == 1
    assert dead.history[-1].result == "permanent_failure"
    assert dead.history[-1].failure_kind == "permanent"
    assert queue.lease() is None


def test_retryable_on_last_attempt_dead_letters(queue):
    t, _ = queue.submit('{"j": 2}', "last-attempt")
    lease = _next_lease(queue)
    # succeed-on-never; force attempts up to budget
    r = queue.fail(lease.id, lease.lease_id, FailureKind.RETRYABLE, "e1")
    time.sleep(r.available_at - time.time() + 0.01)
    l2 = _next_lease(queue)
    r2 = queue.fail(l2.id, l2.lease_id, FailureKind.RETRYABLE, "e2")
    time.sleep(r2.available_at - time.time() + 0.01)
    l3 = _next_lease(queue)
    assert l3.attempt_count == 3
    final = queue.fail(l3.id, l3.lease_id, FailureKind.RETRYABLE, "e3")
    assert final.history[-1].result == "retry_exhausted"


def test_attempts_survive_restart_without_double_counting(config: QueueConfig):
    q1 = TaskQueue(config)
    q1.submit('{"job": "restart"}', "restart-1")
    l = q1.lease(consumer="w1")
    q1.fail(l.id, l.lease_id, FailureKind.RETRYABLE, "boom-before-restart")
    q1.close()

    # reopen: state and attempt count intact, no extra counting
    q2 = TaskQueue(config)
    t = q2.get_task_by_key("restart-1")
    print(f"[test] restart idempotency_key=restart-1 state={t.state.value} "
          f"attempts={t.attempt_count}")
    assert t.state is TaskState.RETRYING
    assert t.attempt_count == 1
    assert len(t.history) == 1
    assert t.history[0].error == "boom-before-restart"

    time.sleep(t.available_at - time.time() + 0.01)
    l2 = q2.lease(consumer="w2")
    assert l2.attempt_count == 2  # continues at 2, not restarted at 1
    done = q2.complete(l2.id, l2.lease_id)
    assert done.state is TaskState.SUCCEEDED
    assert done.attempt_count == 2
    q2.close()


def test_stale_lease_cannot_ack_after_redelivery(queue):
    queue.submit('{"job": "zombie"}', "zombie-1")
    old = queue.lease(consumer="slow")
    # old lease expires and gets reclaimed
    time.sleep(queue.config.visibility_timeout + 0.05)
    assert queue.reclaim_expired() == 1
    new = queue.lease(consumer="fast")
    assert new.attempt_count == 2
    # zombie consumer wakes up and tries to ack with its old lease
    from taskqueue.queue import LeaseError

    try:
        queue.complete(old.id, old.lease_id)
        assert False, "stale lease must be rejected"
    except LeaseError:
        print(f"[test] stale ack rejected idempotency_key=zombie-1 "
              f"old_lease={old.lease_id} new_lease={new.lease_id}")
    done = queue.complete(new.id, new.lease_id)
    assert done.state is TaskState.SUCCEEDED
