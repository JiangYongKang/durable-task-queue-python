"""Submission idempotency and distinguishable rejection reasons."""

from __future__ import annotations

import pytest

from taskqueue.models import RejectReason
from taskqueue.queue import SubmissionRejected

pytestmark = pytest.mark.idempotency


def test_same_key_same_payload_does_not_create_again(queue):
    t1, created = queue.submit('{"order": 1}', "order-1001")
    assert created is True
    print(f"[test] submit idempotency_key={t1.idempotency_key} task_id={t1.id} created=True")

    t2, created2 = queue.submit('{"order": 1}', "order-1001")
    print(
        f"[test] resubmit idempotency_key={t2.idempotency_key} "
        f"task_id={t2.id} created={created2} state={t2.state.value}"
    )
    assert created2 is False
    assert t2.id == t1.id
    assert t2.attempt_count == 0
    assert len(t2.history) == 0


def test_same_key_different_payload_is_rejected(queue):
    queue.submit('{"v": 1}', "dup-key")
    with pytest.raises(SubmissionRejected) as ei:
        queue.submit('{"v": 2}', "dup-key")
    print(f"[test] reject idempotency_key=dup-key reason={ei.value.reason.value}")
    assert ei.value.reason is RejectReason.PAYLOAD_MISMATCH
    # original task untouched
    t = queue.get_task_by_key("dup-key")
    assert t.payload == '{"v": 1}'


def test_invalid_payloads_are_rejected(queue):
    for bad in ["", "   ", "not-json", "[1,2,3]", "123", "true", '"str"']:
        with pytest.raises(SubmissionRejected) as ei:
            queue.submit(bad, f"bad-{bad!r}")
        print(f"[test] reject idempotency_key=bad payload={bad[:20]!r} "
              f"reason={ei.value.reason.value}")
        assert ei.value.reason is RejectReason.INVALID_PAYLOAD


def test_oversized_payload_is_rejected(queue):
    big = '{"blob": "' + ("a" * 2000) + '"}'
    with pytest.raises(SubmissionRejected) as ei:
        queue.submit(big, "too-big")
    print(f"[test] reject idempotency_key=too-big reason={ei.value.reason.value} "
          f"bytes={len(big.encode())}")
    assert ei.value.reason is RejectReason.PAYLOAD_TOO_LARGE


def test_http_rejection_status_codes_and_reasons(client):
    r1 = client.post("/tasks", json={"payload": {"a": 1}, "idempotency_key": "hk"})
    assert r1.status_code == 201
    r2 = client.post("/tasks", json={"payload": {"a": 1}, "idempotency_key": "hk"})
    assert r2.status_code == 200 and r2.json()["created"] is False
    print(f"[test] http dedup idempotency_key=hk created=False")

    r3 = client.post("/tasks", json={"payload": {"a": 2}, "idempotency_key": "hk"})
    print(f"[test] http mismatch idempotency_key=hk status={r3.status_code} "
          f"reason={r3.json()['reason']}")
    assert r3.status_code == 409
    assert r3.json()["reason"] == RejectReason.PAYLOAD_MISMATCH.value

    r4 = client.post("/tasks", json={"payload": {"z": "a" * 2000}})
    assert r4.status_code == 422
    assert r4.json()["reason"] == RejectReason.PAYLOAD_TOO_LARGE.value
