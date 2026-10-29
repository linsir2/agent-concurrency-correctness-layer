"""S24-P1：把 `_ReplicatedLeaseLog` 暴露成**独立进程**的 JSON-lines 服务。

用途：多进程 Raft 故障注入（真 TCP、SIGSTOP/SIGKILL）。协议是**同步回包**：
写命令等「已应用」后才回复，避免调用方读到旧 `lease_last`。

诚实边界：单主机、共享内核时钟、localhost TCP、无丢包/重排；
`SIGSTOP` 不是网络分区。
"""

from __future__ import annotations

import json
import os
import sys
import time
from collections.abc import Callable

from .raft import _HAS_PYSYNCOBJ, require_pysyncobj

if _HAS_PYSYNCOBJ:                                   # pragma: no cover
    from .raft import SyncObjConf, _ReplicatedLeaseLog
else:                                                # pragma: no cover
    SyncObjConf = None
    _ReplicatedLeaseLog = None

DEFAULT_TIMEOUT = 3.0


def node_conf(*, data_dir: str, index: int, password: str = "harness-raft",
              fast: bool = True) -> object:
    """独立进程节点配置：`autoTick=True` + 低延迟超时（满足 pysyncobj 断言）。"""
    require_pysyncobj()
    os.makedirs(data_dir, exist_ok=True)
    return SyncObjConf(
        autoTick=True,
        appendEntriesPeriod=0.02,
        raftMinTimeout=0.12, raftMaxTimeout=0.24,
        connectionTimeout=0.30, connectionRetryTime=0.05,
        leaderFallbackTimeout=0.60, bindRetryTime=0.05, maxBindRetries=3,
        journalFile=os.path.join(data_dir, f"node{index}.journal"),
        fullDumpFile=os.path.join(data_dir, f"node{index}.dump"),
        password=password,
        dynamicMembershipChange=False,
    )


class RaftNode:
    """单节点服务；`serve_stdio()` 读一行 JSON、回一行 JSON。"""

    def __init__(self, addr: str, partners: list[str], data_dir: str,
                 index: int, *, timeout: float = DEFAULT_TIMEOUT,
                 password: str = "harness-raft") -> None:
        require_pysyncobj()
        self.addr = addr
        self.partners = list(partners)
        self.timeout = float(timeout)
        self._obj = _ReplicatedLeaseLog(
            addr, self.partners,
            node_conf(data_dir=data_dir, index=index, password=password))

    # ------------------------------------------------------------ 观测
    def status(self) -> dict:
        obj = self._obj
        return {
            "leader": bool(obj._isLeader()),
            "ready": bool(obj.isReady()),
            "term": int(getattr(obj, "raftCurrentTerm", 0) or 0),
            "epoch": int(obj.lease.epoch),
            "lease": dict(obj.lease.snapshot()),
            "records": len(obj.records),
            "rejected": int(obj.rejected),
        }

    def _wait(self, predicate: Callable[[], bool]) -> bool:
        deadline = time.monotonic() + self.timeout
        while time.monotonic() < deadline:
            if predicate():
                return True
            time.sleep(0.01)
        return predicate()

    # ------------------------------------------------------------ 命令
    def acquire(self, **kw) -> dict:
        before = int(self._obj.lease_version)
        self._obj.lease_acquire(kw["owner"], kw["instance"], int(kw["now_ms"]),
                                int(kw["ttl_ms"]), kw.get("expected_epoch"))
        applied = self._wait(lambda: int(self._obj.lease_version) > before)
        return self._grant(applied)

    def renew(self, **kw) -> dict:
        before = int(self._obj.lease_version)
        self._obj.lease_renew(kw["owner"], kw["instance"], int(kw["epoch"]),
                              int(kw["now_ms"]), int(kw["ttl_ms"]))
        applied = self._wait(lambda: int(self._obj.lease_version) > before)
        return self._grant(applied)

    def release(self, **kw) -> dict:
        before = int(self._obj.lease_version)
        self._obj.lease_release(kw["owner"], kw["instance"], int(kw["epoch"]))
        applied = self._wait(lambda: int(self._obj.lease_version) > before)
        return self._grant(applied)

    def advance(self, **kw) -> dict:
        before = int(self._obj.lease_version)
        self._obj.lease_advance(kw["owner"], kw["instance"], int(kw["epoch"]),
                                int(kw["now_ms"]), int(kw["ttl_ms"]))
        applied = self._wait(lambda: int(self._obj.lease_version) > before)
        return self._grant(applied)

    def _grant(self, applied: bool) -> dict:
        if not applied:
            return {"error": "not_applied"}
        return {"lease": dict(self._obj.lease_last),
                "epoch": int(self._obj.lease.epoch)}

    def append(self, **kw) -> dict:
        before_records = len(self._obj.records)
        before_rejected = int(self._obj.rejected)
        self._obj.append_record(kw.get("rec") or {}, kw.get("stamp"))
        applied = self._wait(
            lambda: len(self._obj.records) > before_records
            or int(self._obj.rejected) > before_rejected)
        if not applied:
            return {"error": "not_applied"}
        return {"records": len(self._obj.records),
                "rejected": int(self._obj.rejected),
                "accepted": int(self._obj.rejected) == before_rejected}

    def dispatch(self, cmd: dict) -> dict:
        op = cmd.get("op")
        if op == "status":
            return self.status()
        if op in ("acquire", "renew", "release", "advance", "append"):
            return getattr(self, op)(**{k: v for k, v in cmd.items() if k != "op"})
        return {"error": f"unknown op: {op}"}

    def serve_stdio(self) -> int:
        for line in sys.stdin:
            try:
                cmd = json.loads(line)
            except json.JSONDecodeError:
                print(json.dumps({"error": "bad_json"}), flush=True)
                continue
            if cmd.get("op") == "shutdown":
                self._obj.destroy()
                print(json.dumps({"ok": True}), flush=True)
                return 0
            try:
                out = self.dispatch(cmd)
            except Exception as exc:                 # noqa: BLE001
                out = {"error": f"{type(exc).__name__}: {exc}"}
            print(json.dumps(out, sort_keys=True), flush=True)
        return 0
