"""S24-P1：多进程 Raft 故障注入（真 TCP + SIGSTOP/SIGKILL）。

- 默认门禁：`topology_ready` + `stale_epoch_rejected`（约 5-10s）；
- `RSC_CHAOS=1`：完整矩阵（暂停 follower/leader、杀 leader、epoch 接管）。
- 缺 pysyncobj → 整模块 xfail（不假绿）。

诚实边界：单主机、共享内核时钟、localhost TCP、无丢包/重排；
SIGSTOP 是进程暂停，不是网络分区。
"""

from __future__ import annotations

import json
import os
import pathlib
import select
import signal
import subprocess
import sys
import time

import pytest

from harness.storage.raft import _HAS_PYSYNCOBJ, pick_free_base

pytestmark = [pytest.mark.raft]
if not _HAS_PYSYNCOBJ:                                # pragma: no cover
    pytestmark.append(pytest.mark.xfail(
        reason="requires-pysyncobj", run=False, strict=False))

CHILD = pathlib.Path(__file__).with_name("_raft_proc.py")


class Cluster:
    def __init__(self, tmp_path: pathlib.Path) -> None:
        base = pick_free_base(45_500, 3)
        self.addrs = [f"127.0.0.1:{base + i}" for i in range(3)]
        self.data_root = tmp_path / "raft"
        self.procs: list[subprocess.Popen] = []
        self.alive: set[int] = set()
        for index, addr in enumerate(self.addrs):
            partners = [x for x in self.addrs if x != addr]
            data_dir = self.data_root / f"node{index}"
            data_dir.mkdir(parents=True)
            self.procs.append(subprocess.Popen(
                [sys.executable, "-u", str(CHILD),
                 "--addr", addr, "--partners", json.dumps(partners),
                 "--data-dir", str(data_dir), "--index", str(index)],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL, text=True, start_new_session=True))
            self.alive.add(index)

    def send(self, index: int, cmd: dict, timeout: float = 3.0) -> dict:
        proc = self.procs[index]
        proc.stdin.write(json.dumps(cmd) + "\n")
        proc.stdin.flush()
        ready, _, _ = select.select([proc.stdout], [], [], timeout)
        if not ready:
            raise TimeoutError(f"node{index} 未在 {timeout}s 内回包")
        line = proc.stdout.readline()
        if not line:
            raise RuntimeError(f"node{index} 已退出")
        return json.loads(line)

    def status(self, index: int) -> dict:
        return self.send(index, {"op": "status"})

    def wait_leader(self, timeout: float = 8.0) -> tuple[int, dict[int, dict]]:
        deadline = time.monotonic() + timeout
        last: dict[int, dict] = {}
        while time.monotonic() < deadline:
            last = {i: self.status(i) for i in sorted(self.alive)}
            leaders = [i for i, st in last.items() if st["leader"]]
            if len(leaders) == 1 and all(st["ready"] for st in last.values()):
                return leaders[0], last
            time.sleep(0.1)
        raise AssertionError(f"{timeout}s 内未选出唯一 leader：{last}")

    def signal(self, index: int, sig: int) -> None:
        os.kill(self.procs[index].pid, sig)

    def close(self) -> None:
        for index in list(self.alive):
            try:
                self.send(index, {"op": "shutdown"}, timeout=2.0)
            except Exception:                             # noqa: BLE001
                pass
            try:
                os.killpg(os.getpgid(self.procs[index].pid), signal.SIGKILL)
            except Exception:                             # noqa: BLE001
                pass
        for proc in self.procs:
            try:
                proc.wait(timeout=3.0)
            except subprocess.TimeoutExpired:             # pragma: no cover
                pass


@pytest.fixture()
def cluster(tmp_path):
    c = Cluster(tmp_path)
    try:
        yield c
    finally:
        c.close()


def _wait_records(c: Cluster, index: int, target: int,
                  timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if c.status(index)["records"] >= target:
            return True
        time.sleep(0.05)
    return c.status(index)["records"] >= target


def test_topology_ready_with_isolated_journals(cluster):
    leader, statuses = cluster.wait_leader()
    assert set(statuses) == {0, 1, 2}
    assert all(st["ready"] for st in statuses.values())
    assert statuses[leader]["leader"] is True


def test_stale_epoch_append_is_rejected(cluster):
    leader, _ = cluster.wait_leader()
    g1 = cluster.send(leader, {"op": "acquire", "owner": "A", "instance": "i1",
                               "now_ms": 0, "ttl_ms": 10})
    assert g1["lease"]["outcome"] == "acquired" and g1["epoch"] == 1
    g2 = cluster.send(leader, {"op": "advance", "owner": "A", "instance": "i1",
                               "epoch": 1, "now_ms": 10, "ttl_ms": 100})
    assert g2["epoch"] == 2
    before = cluster.status(leader)
    stale = cluster.send(leader, {"op": "append", "rec": {"kind": "OLD"},
                                  "stamp": {"owner": "A", "instance": "i1",
                                            "epoch": 1, "now_ms": 10}})
    assert stale["accepted"] is False
    api = cluster.status(leader)
    assert api["records"] == before["records"]
    assert api["rejected"] == before["rejected"] + 1
    good = cluster.send(leader, {"op": "append", "rec": {"kind": "NEW"},
                                 "stamp": {"owner": "A", "instance": "i1",
                                           "epoch": 2, "now_ms": 10}})
    assert good["accepted"] is True


@pytest.mark.chaos
def test_sigkill_leader_reelects_and_keeps_epoch(cluster):
    leader, statuses = cluster.wait_leader()
    old_term = statuses[leader]["term"]
    cluster.signal(leader, signal.SIGKILL)
    cluster.alive.discard(leader)
    cluster.procs[leader].wait(timeout=3.0)
    new_leader, after = cluster.wait_leader()
    assert after[new_leader]["term"] > old_term
    assert after[new_leader]["epoch"] >= statuses[leader]["epoch"]


@pytest.mark.chaos
def test_sigstop_follower_catches_up_after_resume(cluster):
    leader, _ = cluster.wait_leader()
    follower = next(i for i in cluster.alive if i != leader)
    cluster.signal(follower, signal.SIGSTOP)
    cluster.alive.discard(follower)
    try:
        for n in range(3):
            res = cluster.send(leader, {"op": "append",
                                        "rec": {"kind": "X", "n": n}})
            assert res["accepted"] is True
        target = cluster.status(leader)["records"]
    finally:
        cluster.signal(follower, signal.SIGCONT)
        cluster.alive.add(follower)
    assert _wait_records(cluster, follower, target)


@pytest.mark.chaos
def test_sigstop_leader_steps_down_after_resume(cluster):
    leader, _ = cluster.wait_leader()
    cluster.signal(leader, signal.SIGSTOP)
    cluster.alive.discard(leader)
    try:
        new_leader, _ = cluster.wait_leader()
        assert new_leader != leader
        cluster.send(new_leader, {"op": "append", "rec": {"kind": "AFTER"}})
        target = cluster.status(new_leader)["records"]
    finally:
        cluster.signal(leader, signal.SIGCONT)
        cluster.alive.add(leader)
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        st = cluster.status(leader)
        if not st["leader"] and st["records"] >= target:
            return
        time.sleep(0.1)
    raise AssertionError(f"旧 leader 未降级/追平：{cluster.status(leader)}")


@pytest.mark.chaos
def test_lease_takeover_after_leader_failover_is_monotonic(cluster):
    leader, _ = cluster.wait_leader()
    cluster.send(leader, {"op": "acquire", "owner": "A", "instance": "i1",
                          "now_ms": 0, "ttl_ms": 10})
    advanced = cluster.send(leader, {"op": "advance", "owner": "A",
                                     "instance": "i1", "epoch": 1,
                                     "now_ms": 10, "ttl_ms": 10})
    old = advanced["epoch"]
    cluster.signal(leader, signal.SIGKILL)
    cluster.alive.discard(leader)
    cluster.procs[leader].wait(timeout=3.0)
    new_leader, _ = cluster.wait_leader()
    taken = cluster.send(new_leader, {"op": "acquire", "owner": "B",
                                      "instance": "i2", "now_ms": 20,
                                      "ttl_ms": 10, "expected_epoch": old})
    assert taken["epoch"] == old + 1


@pytest.mark.chaos
def test_shutdown_leaves_no_child(cluster):
    cluster.wait_leader()
    for index in list(cluster.alive):
        assert cluster.send(index, {"op": "shutdown"})["ok"] is True
    cluster.alive.clear()
    for proc in cluster.procs:
        assert proc.wait(timeout=3.0) == 0
