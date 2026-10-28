"""S16 / MS9：用 Raft 替换「单写者 seq」与 fencing epoch。

设计要点（详见 `docs/s16-ms9-raft.md`）：

1. **只替换串行化点**：EventLog 的 seq 分配交给 Raft leader（日志索引 = seq），
   其余机制（RSC / 租约 / Saga / 收据 / 上下文）**完全不动**。
2. **不用 `replicated_sync`**：实测它在「单线程手动 `doTick()`」下**自我死锁**
   （阻塞等提交时无人 tick）。改用「**一个受控 ticker 线程** + 异步 `@replicated`
   + 轮询已应用状态」。
3. **查询复用 `_EventLogBase`**：`replay/has/tail/all/subscribe/notify_events`
   直接继承 → 与内存/WAL 后端**语义天然一致**（由 S15 后端一致性套件验证）。

`pysyncobj` 是**可选依赖**（`optional-dependencies.raft`）；未安装时构造
`RaftEventLog` 抛 `RaftUnavailable`（**fail-closed**，不静默降级为单机）。
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable

from ..event_log import _draft, _event, _EventLogBase
from ..types import Event

try:                                                    # pragma: no cover
    from pysyncobj import SyncObj, SyncObjConf, replicated
    _HAS_PYSYNCOBJ = True
except ImportError:                                     # pragma: no cover
    SyncObj = object                                    # type: ignore[assignment]
    SyncObjConf = None                                  # type: ignore[assignment]
    replicated = None                                   # type: ignore[assignment]
    _HAS_PYSYNCOBJ = False


class RaftUnavailable(RuntimeError):
    """未安装 `pysyncobj`（或集群不可用）→ 拒绝启动 Raft 后端。"""


def require_pysyncobj() -> None:
    if not _HAS_PYSYNCOBJ:
        raise RaftUnavailable(
            "需要 pysyncobj（`pip install pysyncobj>=0.3.14` 或 "
            "`pip install .[raft]`）；本后端**不会**静默降级为单机")


def _port_free(port: int) -> bool:
    """探测端口可用（`SO_REUSEADDR`：TIME_WAIT 不算占用）。"""
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind(("127.0.0.1", port))
            return True
        except OSError:
            return False


def pick_free_base(base: int, count: int, *, max_shift: int = 500) -> int:
    """从 `base` 起找**连续 count 个**可用端口；找不到则抛 `RaftUnavailable`。

    为什么必须探测：多组测试/多次运行会复用端口段，撞上 TIME_WAIT 或残留监听时
    pysyncobj 会静默重试绑定（`bindRetryTime`），表现为「选主超时」而非明确报错。
    """
    for shift in range(max_shift):
        cand = base + shift * (count + 1)
        if cand + count > 65000:
            break
        if all(_port_free(cand + i) for i in range(count)):
            return cand
    raise RaftUnavailable(f"从 {base} 起找不到 {count} 个连续可用端口")


def raft_conf(**overrides):
    """一套满足 pysyncobj 校验的默认配置（本地 3 节点、低延迟）。

    实测约束：`raftMinTimeout > appendEntriesPeriod * 3`、
    `connectionTimeout >= raftMaxTimeout`（否则 `SyncObjConf.validate()` 直接断言）。
    """
    if not _HAS_PYSYNCOBJ:
        require_pysyncobj()
    base = {
        "autoTick": False,           # ★ 由 RaftCluster 的 ticker 统一驱动
        "appendEntriesPeriod": 0.05,
        "raftMinTimeout": 0.3,
        "raftMaxTimeout": 0.6,
        "connectionTimeout": 1.2,
        "password": "harness-raft",  # 非空：pysyncobj 要求加密连接
    }
    base.update(overrides)
    return SyncObjConf(**base)


class RaftCluster:
    """3 节点进程内集群 + **受控** ticker 线程（测试/开发用）。

    为什么需要 ticker：`replicated_sync` 与手动单线程 tick 互斥（实测死锁）。
    但线程生命周期由本类掌控（`close()` 停止），不引入「后台线程改状态」的
    不确定性来源。
    """

    def __init__(self, node_factory: Callable[[str, list[str], object], SyncObj],
                 *, base_port: int, nodes: int = 3,
                 conf_factory: Callable[[], object] = raft_conf,
                 tick_interval: float = 0.002) -> None:
        require_pysyncobj()
        self._tick_interval = tick_interval
        self._stop = threading.Event()
        base_port = pick_free_base(base_port, nodes)
        addrs = [f"127.0.0.1:{base_port + i}" for i in range(nodes)]
        self.addrs = addrs
        self.nodes = []
        for addr in addrs:
            partners = [a for a in addrs if a != addr]
            self.nodes.append(node_factory(addr, partners, conf_factory()))
        self._thread = threading.Thread(target=self._run, daemon=True,
                                        name="raft-ticker")
        self._thread.start()

    # ---- 受控驱动 -------------------------------------------------
    def _run(self) -> None:
        while not self._stop.is_set():
            for nd in self.nodes:
                try:
                    nd.doTick()
                except Exception:  # noqa: BLE001,S112  # 单节点 tick 异常不得中断集群
                    continue
            time.sleep(self._tick_interval)

    def close(self) -> None:
        self._stop.set()
        self._thread.join(timeout=3.0)
        for nd in self.nodes:
            try:
                nd.destroy()
            except Exception:  # noqa: BLE001,S112  # destroy 失败不阻断关闭
                continue

    # ---- 观测 -----------------------------------------------------
    def wait_until(self, pred: Callable[[], bool],
                   timeout: float = 20.0) -> bool:
        """轮询直到 `pred()` 为真（**不用 sleep 猜时长**；超时返回 False）。"""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if pred():
                return True
            time.sleep(0.005)
        return pred()

    def wait_ready(self, timeout: float = 20.0, *, all_nodes: bool = True) -> bool:
        """等待就绪。默认**全部**节点就绪（集群真正可用，而非「有一个 leader」）。"""
        if all_nodes:
            return self.wait_until(lambda: all(n.isReady() for n in self.nodes),
                                   timeout)
        return self.wait_until(lambda: any(n.isReady() for n in self.nodes),
                               timeout)

    def leader(self):
        return next((n for n in self.nodes if n.isReady()), None)

    def leader_term(self) -> int:
        """当前 leader 的 Raft term（用于 fencing epoch）。"""
        lead = self.leader()
        return int(getattr(lead, "raftCurrentTerm", 0) or 0) if lead else 0

    def quorum(self) -> bool:
        lead = self.leader()
        return bool(getattr(lead, "hasQuorum", False)) if lead else False


if _HAS_PYSYNCOBJ:
    class _ReplicatedLog(SyncObj):                      # pragma: no cover
        """权威状态：已提交事件记录（`seq` 由 Raft 日志索引语义分配）。"""

        def __init__(self, addr, partners, conf) -> None:
            super().__init__(addr, partners, conf=conf)
            self.records: list[dict] = []

        @replicated
        def append_record(self, rec: dict) -> None:
            self.records.append(dict(rec, seq=len(self.records) + 1))

        @replicated
        def append_records(self, recs: list[dict]) -> None:
            for rec in recs:
                self.records.append(dict(rec, seq=len(self.records) + 1))
else:                                                   # pragma: no cover
    class _ReplicatedLog:                               # type: ignore[no-redef]
        def __init__(self, *a, **kw) -> None:
            require_pysyncobj()


class RaftEventLog(_EventLogBase):
    """`EventLog` 的 Raft 后端：**seq 分配走 leader + quorum 提交**。

    - 写入：异步 `@replicated` 提交 → 轮询本地副本「已应用」→ 索引成 `Event`
    - 读取：**继承 `_EventLogBase`**（`replay/has/tail/all/...`）→ 语义与
      内存/WAL 后端一致（S15 一致性套件对三后端跑同一组断言）
    - `set_fencing(...)`：**兼容空实现** —— Raft 用 term/leader 取代
      `wal.log.epoch`，调用点无需改（语义变化见文档）
    """

    def __init__(self, clock=None, *, base_port: int, nodes: int = 3,
                 cluster: RaftCluster | None = None,
                 timeout: float = 20.0) -> None:
        super().__init__(clock)
        require_pysyncobj()
        self._timeout = timeout
        self._lock = threading.RLock()
        self._applied = 0                 # 已索引到 `_events` 的 records 数
        self._cluster = cluster or RaftCluster(
            _ReplicatedLog, base_port=base_port, nodes=nodes)
        self._owns_cluster = cluster is None
        self._replica = self._cluster.nodes[0]
        if not self._cluster.wait_ready(timeout):
            raise RaftUnavailable(
                f"Raft 集群 {timeout}s 内未就绪（端口 {base_port}）")
        self._sync_applied()

    # ---- 内部：把已提交记录索引成本地事件 -------------------------
    def _sync_applied(self) -> None:
        recs = list(getattr(self._replica, "records", ()))
        while self._applied < len(recs):
            self._index(_event(recs[self._applied]))
            self._applied += 1

    def _wait_committed(self, target: int, what: str) -> None:
        if not self._cluster.wait_until(
                lambda: len(getattr(self._replica, "records", ())) >= target,
                self._timeout):
            raise RaftUnavailable(
                f"{what}: {self._timeout}s 内未提交（target={target}，"
                f"applied={len(getattr(self._replica, 'records', ()))}）")

    # ---- 写路径 ---------------------------------------------------
    def append(self, kind, subject: str, payload=None, producer: str = "",
               caused_by: str = "", idempotency_key: str = "", *,
               run_id: str = "", notify: bool = True) -> Event:
        with self._lock:
            rec = _draft(kind, subject, payload, producer, caused_by,
                         idempotency_key, run_id, self._clock)
            target = len(getattr(self._replica, "records", ())) + 1
            self._replica.append_record(rec)        # 异步：交 Raft
            self._wait_committed(target, "append")
            self._sync_applied()
            ev = self._events[-1]
        if notify:
            self._notify([ev])
        return ev

    def append_batch(self, drafts: list[dict], notify: bool = True) -> list[Event]:
        if not drafts:
            return []
        with self._lock:
            recs = [_draft(d.get("kind"), d.get("subject", ""), d.get("payload"),
                           d.get("producer", ""), d.get("caused_by", ""),
                           d.get("idempotency_key", ""), d.get("run_id", ""),
                           self._clock)
                    for d in drafts]
            before = len(self._events)
            target = len(getattr(self._replica, "records", ())) + len(recs)
            self._replica.append_records(recs)
            self._wait_committed(target, "append_batch")
            self._sync_applied()
            evs = self._events[before:]
        if notify:
            self._notify(evs)
        return evs

    def append_in_txn(self, kind, subject: str, payload=None, producer: str = "",
                      caused_by: str = "", idempotency_key: str = "", *,
                      run_id: str = "") -> Event:
        return self.append(kind, subject, payload, producer, caused_by,
                           idempotency_key, run_id=run_id, notify=False)

    # ---- 兼容面 ---------------------------------------------------
    def set_fencing(self, **_kwargs) -> None:
        """Raft 后端**不需要** `wal.log.epoch` fencing（leader/term 已保证唯一写者）。

        保留方法是为了让 `Harness._wire_wal_fencing` 等调用点无需分支；
        **语义变化**：不再有「应用层自增 epoch」，单调性由共识提供。
        """
        return

    def close(self) -> None:
        self._run_close_hooks()
        if self._owns_cluster:
            self._cluster.close()


class RaftEpochSource:
    """epoch = leader 的 Raft **term**（随选主单调增）。

    诚实说明：本地**不能** `bump_epoch` 自增 —— 那会破坏 Raft 的单调性
    （旧 leader 自己把 epoch 抬高就能越过 fence）。`bump_epoch()` 因此
    返回**当前 term**（可能不变），语义是「读共识推进后的 epoch」。
    """

    def __init__(self, cluster: RaftCluster) -> None:
        self._cluster = cluster

    def read_epoch(self) -> int:
        return self._cluster.leader_term()

    def bump_epoch(self) -> int:
        return self._cluster.leader_term()
