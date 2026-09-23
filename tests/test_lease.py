"""Lease exclusivity, expiry-based redelivery and heartbeat renewal."""

from __future__ import annotations

import time

from taskqueue.models import TaskState
from taskqueue.queue import LeaseError


def test_only_one_valid_lease_at_a_time(queue):
    t, _ = queue.submit('{"job": 1}', "lease-single")
    lease = queue.lease(consumer="c1")
    assert lease is not None and lease.state is TaskState.INFLIGHT
    print(f"[test] lease idempotency_key=lease-single lease_id={lease.lease_id} "
          f"consumer=c1 expires_at={lease.lease_expires_at:.3f}")

    # second consumer must not receive the same task while lease is valid
    assert queue.lease(consumer="c2") is None

    # unknown / wrong lease tokens are rejected
    try:
        queue.complete(t.id, "does-not-exist")
        assert False
    except LeaseError:
        pass
    done = queue.complete(t.id, lease.lease_id)
    assert done.state is TaskState.SUCCEEDED
    assert done.lease_id is None


def test_expired_lease_is_redelivered_and_attempt_counts(queue):
    t, _ = queue.submit('{"job": 2}', "lease-expiry")
    first = queue.lease(consumer="crash")
    print(f"[test] first lease idempotency_key=lease-expiry "
          f"lease_id={first.lease_id} attempt={first.attempt_count}")
    assert first.attempt_count == 1

    # not reclaimable before timeout
    time.sleep(queue.config.visibility_timeout - 0.1)
    assert queue.reclaim_expired() == 0
    assert queue.lease() is None  # still locked

    time.sleep(0.15)
    n = queue.reclaim_expired()
    assert n == 1
    print(f"[test] reclaimed idempotency_key=lease-expiry old_lease={first.lease_id}")

    second = queue.lease(consumer="recovery")
    assert second is not None
    assert second.attempt_count == 2
    assert second.lease_id != first.lease_id
    print(f"[test] redelivered idempotency_key=lease-expiry "
          f"new_lease={second.lease_id} attempt=2")

    # history explains: timeout then a fresh leased delivery
    hist = second.history
    assert hist[0].result == "timeout"
    assert hist[0].lease_id == first.lease_id
    assert hist[0].error == "lease expired without report"
    assert hist[1].result == "leased"

    done = queue.complete(t.id, second.lease_id)
    assert done.state is TaskState.SUCCEEDED
    assert done.attempt_count == 2


def test_heartbeat_extends_lease(queue):
    queue.submit('{"job": 3}', "lease-hb")
    lease = queue.lease(consumer="hb-worker")
    first_expiry = lease.lease_expires_at
    time.sleep(0.1)
    renewed = queue.heartbeat(lease.id, lease.lease_id)
    print(f"[test] heartbeat idempotency_key=lease-hb lease_id={lease.lease_id} "
          f"old_expiry={first_expiry:.3f} new_expiry={renewed.lease_expires_at:.3f}")
    assert renewed.lease_expires_at > first_expiry

    # a sweep at the original expiry must not reclaim the renewed lease
    time.sleep(queue.config.visibility_timeout - 0.15)
    assert queue.reclaim_expired() == 0
    assert queue.get_task(lease.id).state is TaskState.INFLIGHT


def test_heartbeat_with_wrong_lease_rejected(queue):
    queue.submit('{"job": 4}', "lease-hb-bad")
    lease = queue.lease(consumer="w")
    try:
        queue.heartbeat(lease.id, "stale")
        assert False
    except LeaseError:
        print("[test] heartbeat rejected for stale lease idempotency_key=lease-hb-bad")
