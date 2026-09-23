"""Multi-consumer concurrency: exactly-once delivery, consistent counters."""

from __future__ import annotations

import threading
import time

from taskqueue.models import FailureKind, TaskState


def test_concurrent_consumers_each_task_leased_once(queue):
    n_tasks = 50
    keys = [f"concurrent-{i}" for i in range(n_tasks)]
    for k in keys:
        queue.submit('{"item": "%s"}' % k, k)

    claimed: list[tuple[str, str, str]] = []
    claimed_lock = threading.Lock()
    barrier = threading.Barrier(6)

    def consumer(name: str) -> None:
        barrier.wait()
        while True:
            task = queue.lease(consumer=name)
            if task is None:
                return
            with claimed_lock:
                claimed.append((task.id, task.lease_id, name))
            print(
                f"[test] claimed idempotency_key={task.idempotency_key} "
                f"lease_id={task.lease_id} consumer={name} "
                f"attempt={task.attempt_count}"
            )
            queue.complete(task.id, task.lease_id)

    threads = [threading.Thread(target=consumer, args=(f"c{i}",)) for i in range(6)]
    for th in threads:
        th.start()
    for th in threads:
        th.join(timeout=10)

    task_ids = [c[0] for c in claimed]
    assert len(claimed) == n_tasks
    assert len(set(task_ids)) == n_tasks  # no task delivered twice
    assert len(set(l for _, l, _ in claimed)) == n_tasks  # distinct lease ids

    for k in keys:
        t = queue.get_task_by_key(k)
        assert t.state is TaskState.SUCCEEDED, (k, t.state)
        assert t.attempt_count == 1
        assert len(t.history) == 1 and t.history[0].result == "succeeded"
    print(f"[test] all {n_tasks} tasks delivered exactly once across 6 consumers")


def test_concurrent_resubmissions_deduplicate(queue):
    results = []
    results_lock = threading.Lock()
    barrier = threading.Barrier(8)

    def submitter(i: int) -> None:
        barrier.wait()
        task, created = queue.submit('{"amount": 100}', "money-key")
        with results_lock:
            results.append((i, task.id, created))
        print(f"[test] submitter={i} idempotency_key=money-key "
              f"task_id={task.id} created={created}")

    threads = [threading.Thread(target=submitter, args=(i,)) for i in range(8)]
    for th in threads:
        th.start()
    for th in threads:
        th.join(timeout=10)

    ids = {tid for _, tid, _ in results}
    assert len(ids) == 1  # exactly one task
    assert sum(1 for _, _, created in results if created) == 1
    t = queue.get_task_by_key("money-key")
    assert t.attempt_count == 0 and len(t.history) == 0


def test_concurrent_lease_retry_race_keeps_consistent_attempts(queue):
    # One flaky task: many consumers race for every redelivery.
    queue.submit('{"x": 1}', "race-retry")

    def round_consumer(round_no: int, expect_attempt: int) -> str | None:
        picked = []
        plock = threading.Lock()
        stop = threading.Event()
        barrier = threading.Barrier(5)

        def worker() -> None:
            barrier.wait()
            while not stop.is_set():
                t = queue.lease(consumer=f"r{round_no}-w")
                if t is None:
                    return
                with plock:
                    picked.append(t)
                print(f"[test] race round={round_no} idempotency_key=race-retry "
                      f"lease_id={t.lease_id} attempt={t.attempt_count}")
                if round_no < 3:
                    queue.fail(t.id, t.lease_id, FailureKind.RETRYABLE, f"e{round_no}")
                else:
                    queue.complete(t.id, t.lease_id)

        ths = [threading.Thread(target=worker) for _ in range(5)]
        for th in ths:
            th.start()
        deadline = time.time() + 2
        while time.time() < deadline and not picked:
            time.sleep(0.005)
        stop.set()
        for th in ths:
            th.join(timeout=5)
        assert len(picked) == 1, "only one consumer may win each delivery round"
        assert picked[0].attempt_count == expect_attempt
        return picked[0].id

    tid = round_consumer(1, 1)
    t = queue.get_task(tid)
    time.sleep(max(0.0, t.available_at - time.time()) + 0.02)
    tid2 = round_consumer(2, 2)
    assert tid2 == tid
    t = queue.get_task(tid)
    time.sleep(max(0.0, t.available_at - time.time()) + 0.02)
    tid3 = round_consumer(3, 3)
    assert tid3 == tid

    final = queue.get_task(tid)
    assert final.state is TaskState.SUCCEEDED
    assert final.attempt_count == 3
    assert [h.result for h in final.history] == [
        "retryable_failure", "retryable_failure", "succeeded"]
    print(f"[test] race-retry final attempts=3 distinct leases="
          f"{len({h.lease_id for h in final.history})}")
