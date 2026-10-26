"""MS0 出口条件：全局 seq 单调（B3 单写者）。

对应 milestones.md MS0：「并发 append 1000 条，seq 严格单调无重复」。
"""

from __future__ import annotations

import threading

from harness import EventLog, EventKind


def test_event_log_seq_monotonic_under_concurrency():
    log = EventLog()
    threads = [threading.Thread(target=lambda: [log.append(EventKind.RESOURCE_COMMITTED, "r", {"i": i})
                                                 for i in range(200)])
               for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    seqs = [e.seq for e in log.all()]
    assert len(seqs) == 1000
    assert len(set(seqs)) == 1000              # 无重复
    assert seqs == sorted(seqs)                # 严格单调
    assert log.tail() == 1000


def test_event_log_replay_is_ordered_and_filterable():
    log = EventLog()
    log.append(EventKind.RESOURCE_COMMITTED, "a", {"v": 1})
    log.append(EventKind.RESOURCE_COMMITTED, "b", {"v": 2})
    log.append(EventKind.RESOURCE_COMMITTED, "a", {"v": 3})

    a_events = list(log.replay(subject="a"))
    assert [e.payload["v"] for e in a_events] == [1, 3]
    assert [e.seq for e in a_events] == sorted(e.seq for e in a_events)

    assert log.has("a", EventKind.RESOURCE_COMMITTED) is True
    assert log.has("a", EventKind.TASK_COMPLETED) is False
