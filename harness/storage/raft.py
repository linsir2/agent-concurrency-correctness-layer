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


class NotLeader(RuntimeError):
    """`require_leader=True` 时在 follower 上发起线性一致读 → 拒绝（不返回陈旧值）。"""


class EpochNotBumpable(RuntimeError):
    """Raft 的 epoch 由共识 term 推进，**应用层不得自增**（S17 K-85）。"""


def require_pysyncobj() -> None:
    if not _HAS_PYSYNCOBJ:
        raise RaftUnavailable(
            "需要 pysyncobj（`pip install pysyncobj>=0.3.14` 或 "
            "`pip install .[raft]`）；本后端**不会**静默降级为单机")


def _port_free(port: int) -> bool:
    """探测端口可用。

    **不设 `SO_REUSEADDR`**：否则「已被监听」或「TIME_WAIT」的端口会被误判为空闲，
    导致 pysyncobj 静默重试绑定 → 表现为「选主超时」而非明确失败（S16 F-2 根因）。
    TIME_WAIT 也视为不可用 —— 反正我们会顺延到下一个端口。
    """
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
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


def raft_conf(*, journal_file=None, full_dump_file=None,
              compaction_min_entries: int = 2, **overrides):
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
    # S17 K-86：日志压缩/快照（实测：写入 → compact → 重启可完整恢复）
    if journal_file is not None:
        base["journalFile"] = journal_file
    if full_dump_file is not None:
        base["fullDumpFile"] = full_dump_file
        # pysyncobj 断言 `logCompactionMinEntries >= 2`；实测 2 + compact() 才会
        # 真正写 `.dump`（阈值过大 → compact 空操作 → 重启无快照可恢复）。
        base.setdefault("logCompactionMinEntries", max(2, compaction_min_entries))
        base.setdefault("logCompactionMinTime", 0.1)
    base.update(overrides)
    return SyncObjConf(**base)


def production_conf(*, node_id: str, addrs: list[str], data_dir: str) -> object:
    """**真实多机**配置（S17 K-83）：每节点独立 journal/dump + 生产级超时。

    `addrs` 为全体节点地址（`host:port`），`node_id` 必须是其中之一。
    超时按「跨机 RTT 远大于本机」放宽：`raftMinTimeout=0.6s / max=1.2s`，
    `connectionTimeout=2.4s`（仍满足 pysyncobj 的两条断言）。

    **诚实边界**：本沙箱无法起真实多机，故只验证配置正确性；
    「集群发现 / DNS 轮转 / 跨机网络分区演练」仍是 K-83 的剩余部分。
    """
    if not addrs or node_id not in addrs:
        raise ValueError(f"node_id={node_id!r} 必须在 addrs 中：{addrs}")
    if len(addrs) < 3:
        raise ValueError(f"Raft 至少 3 节点（当前 {len(addrs)}）")
    if not data_dir:
        raise ValueError("data_dir 不能为空（journal/快照需要持久目录）")
    idx = addrs.index(node_id)
    return raft_conf(
        journal_file=f"{data_dir}/node{idx}.journal",
        full_dump_file=f"{data_dir}/node{idx}.dump",
        raftMinTimeout=0.6, raftMaxTimeout=1.2,
        connectionTimeout=2.4, appendEntriesPeriod=0.1,
        compaction_min_entries=1000)


class RaftCluster:
    """3 节点进程内集群 + **受控** ticker 线程（测试/开发用）。

    为什么需要 ticker：`replicated_sync` 与手动单线程 tick 互斥（实测死锁）。
    但线程生命周期由本类掌控（`close()` 停止），不引入「后台线程改状态」的
    不确定性来源。
    """

    def __init__(self, node_factory: Callable[[str, list[str], object], SyncObj],
                 *, base_port: int, nodes: int = 3,
                 conf_factory: Callable[[], object] | None = None,
                 data_dir: str | None = None,
                 tick_interval: float = 0.002) -> None:
        require_pysyncobj()
        self._tick_interval = tick_interval
        self._stop = threading.Event()
        base_port = pick_free_base(base_port, nodes)
        addrs = [f"127.0.0.1:{base_port + i}" for i in range(nodes)]
        self.addrs = addrs
        self.nodes = []
        for idx, addr in enumerate(addrs):
            partners = [a for a in addrs if a != addr]
            if conf_factory is not None:
                conf = conf_factory()
            elif data_dir:
                # S17 K-83/K-86：每节点独立 journal/dump（实测可重启恢复）
                conf = raft_conf(
                    journal_file=f"{data_dir}/node{idx}.journal",
                    full_dump_file=f"{data_dir}/node{idx}.dump")
            else:
                conf = raft_conf()
            self.nodes.append(node_factory(addr, partners, conf))
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

    def compact(self) -> None:
        """对全部节点触发日志压缩（S17 K-86；配合 `fullDumpFile` 落快照）。"""
        for nd in self.nodes:
            try:
                nd.forceLogCompaction()
            except Exception:  # noqa: BLE001,S112  # 压缩失败不阻断（下轮再试）
                continue

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
                 timeout: float = 20.0,
                 require_leader: bool = False) -> None:
        super().__init__(clock)
        require_pysyncobj()
        self._timeout = timeout
        # S17 K-82：默认 follower 可读（最终一致）；显式开启后非 leader 抛错
        self._require_leader = require_leader
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

    # ---- 读一致性（S17 K-82）--------------------------------------
    def _is_leader(self) -> bool:
        lead = self._cluster.leader()
        return lead is self._replica and bool(
            getattr(self._replica, "hasQuorum", False))

    def _check_read(self) -> None:
        """`require_leader=True` 时拒绝在 follower 上读（避免静默返回陈旧值）。

        **不声称** follower 也能线性一致读：pysyncobj 不暴露 ReadIndex RPC，
        真正的线性一致读需要「leader 在当前 term 确认领导权」。这里只保证
        「要么在 leader 上读，要么明确失败」。
        """
        if self._require_leader and not self._is_leader():
            raise NotLeader(
                "require_leader=True 但当前节点不是 leader："
                "请路由到 leader，或改用最终一致读（require_leader=False）")

    def _wait_applied_commit(self) -> None:
        """leader 上等到 `applied >= commit`（读到自己已提交的最新状态）。"""
        self._cluster.wait_until(
            lambda: int(getattr(self._replica, "raftLastApplied", 0))
            >= int(getattr(self._replica, "raftCommitIndex", 0)),
            self._timeout)

    def all(self) -> list[Event]:
        self._check_read()
        if self._require_leader:
            self._wait_applied_commit()
        return super().all()

    def tail(self) -> int:
        self._check_read()
        if self._require_leader:
            self._wait_applied_commit()
        return super().tail()

    def replay(self, subject=None, from_seq: int = 0, **kw):
        self._check_read()
        if self._require_leader:
            self._wait_applied_commit()
        return super().replay(subject, from_seq, **kw)

    def has(self, subject: str, kind) -> bool:
        self._check_read()
        return super().has(subject, kind)

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

    supports_bump = False

    def bump_epoch(self) -> int:
        """**明确不可用**（S17 K-85）。

        原实现返回当前 term，调用方会误以为「已推进」。Raft 下 epoch 只能由
        共识推进（选主换 term）；应用层自增会让旧 leader 越过 fence。
        """
        raise EpochNotBumpable(
            "Raft 的 epoch 由共识 term 推进，应用层不得自增（见 S17 K-85）")


# =====================================================================
# S17 K-81：Raft 预算（确定性 id 来自**复制状态里的计数器**）
# =====================================================================
if _HAS_PYSYNCOBJ:
    class _ReplicatedBudget(SyncObj):                     # pragma: no cover
        """预算的复制状态机。

        **为什么 id 用复制计数器而不是 `uuid4`**：RSM 要求每个副本对同一命令
        产生**相同**结果。`uuid4()` 会各副本不同 → 状态发散 → `release(rid)`
        在 follower 上永远匹配不到 → 预算泄漏（S16 K-81 的根因）。

        计数器 `_seq` 本身是复制状态的一部分 → 所有副本按相同顺序应用相同命令
        → `f"R{n}"` 必然一致，且**重启后由 journal/快照恢复**继续单调。
        """

        def __init__(self, addr, partners, conf) -> None:
            super().__init__(addr, partners, conf=conf)
            self.version = 0
            self._seq = 0
            self._limits: dict[str, int] = {}
            self._reserved: dict[str, int] = {}
            self._resv: dict[str, list] = {}
            self.last_rid = ""

        @replicated
        def create_node(self, node_id: str, limits: int) -> None:
            self._limits.setdefault(str(node_id), int(limits))
            self.version += 1

        @replicated
        def reserve(self, node_id: str, amount: int) -> None:
            self._seq += 1
            rid = f"R{self._seq}"
            lim = int(self._limits.get(str(node_id), 0))
            used = int(self._reserved.get(str(node_id), 0))
            if int(amount) <= 0 or used + int(amount) > lim:
                self.last_rid = ""            # 拒绝：不分配 id（但仍占一条日志）
            else:
                self._reserved[str(node_id)] = used + int(amount)
                self._resv[rid] = [str(node_id), int(amount), "ACTIVE", 0]
                self.last_rid = rid
            self.version += 1

        @replicated
        def consume(self, rid: str, amount: int = 1) -> None:
            ent = self._resv.get(str(rid))
            if ent and ent[2] == "ACTIVE":
                ent[3] = int(ent[3]) + int(amount)
            self.version += 1

        @replicated
        def release(self, rid: str) -> None:
            ent = self._resv.get(str(rid))
            if ent and ent[2] in ("ACTIVE", "SETTLED"):
                node_id = ent[0]
                back = int(ent[1]) - int(ent[3])
                self._reserved[node_id] = max(
                    0, int(self._reserved.get(node_id, 0)) - back)
                ent[2] = "RELEASED"
            self.version += 1

        @replicated
        def settle(self, rid: str) -> None:
            ent = self._resv.get(str(rid))
            if ent and ent[2] == "ACTIVE":
                node_id = ent[0]
                back = int(ent[1]) - int(ent[3])
                self._reserved[node_id] = max(
                    0, int(self._reserved.get(node_id, 0)) - back)
                ent[2] = "SETTLED"
            self.version += 1
else:                                                     # pragma: no cover
    class _ReplicatedBudget:                              # type: ignore[no-redef]
        def __init__(self, *a, **kw) -> None:
            require_pysyncobj()


class _NodeView:
    """`RaftBudgetManager` 的节点只读视图（供 `root`/`_nodes` 兼容既有读法）。"""

    __slots__ = ("consumed", "limits", "node_id", "self_reserved")

    def __init__(self, node_id: str, limits: int, self_reserved: int,
                 consumed: int = 0) -> None:
        self.node_id = node_id
        self.limits = limits
        self.self_reserved = self_reserved
        self.consumed = consumed

    @property
    def subtree_reserved(self) -> int:
        return self.self_reserved

    @property
    def remaining(self) -> int:
        return max(0, self.limits - self.self_reserved)


class RaftBudgetManager:
    """`BudgetManager` 的 Raft 后端（**只把 reserve 串行化点交给共识**）。

    一致性范围（诚实声明）：
    - **集群一致**：`create_node` / `reserve` / `consume` / `release` / `settle`
      （走 Raft，quorum 提交后才返回）；查询读本地已应用副本。
    - **leader 本地（不保证集群一致）**：`archive_node` / `remove_node` / `gc` /
      `check_invariants` / `apply_budget_event` / `budget_events` /
      `suppressed` / `flush_pending` —— 委托给内嵌的 `BudgetManager` 镜像。
    - **明确不可用**：`set_epoch` / `bump_epoch`（epoch 由共识 term 推进，K-85）。
    """

    def __init__(self, *, base_port: int, nodes: int = 3,
                 cluster: RaftCluster | None = None,
                 timeout: float = 20.0,
                 data_dir: str | None = None) -> None:
        super().__init__()
        require_pysyncobj()
        from ..budget import BudgetManager  # 局部导入避免循环

        self._timeout = timeout
        self._cluster = cluster or RaftCluster(
            _ReplicatedBudget, base_port=base_port, nodes=nodes,
            data_dir=data_dir)
        self._owns_cluster = cluster is None
        self._replica = self._cluster.nodes[0]
        if not self._cluster.wait_ready(timeout):
            raise RaftUnavailable(
                f"Raft 集群 {timeout}s 内未就绪（端口 {base_port}）")
        # 非共识路径（归档/gc/不变量/事件）的本地镜像；**不参与**复制的判定
        self._local = BudgetManager(event_log=None)

    # ---- 内部：等待复制生效 ---------------------------------------
    def _bump_and_wait(self, fn, *args, **kw) -> None:
        before = int(getattr(self._replica, "version", 0))
        fn(*args, **kw)
        if not self._cluster.wait_until(
                lambda: int(getattr(self._replica, "version", 0)) > before,
                self._timeout):
            raise RaftUnavailable(
                f"预算操作 {getattr(fn, '__name__', fn)} {self._timeout}s 内未提交")

    # ---- 共识路径 ------------------------------------------------
    def create_node(self, node_id: str, limits: int,
                    parent_id: str = "run") -> _NodeView:
        """建节点（**走共识**，保证所有副本 limits 一致）。"""
        self._bump_and_wait(self._replica.create_node, node_id, limits)
        return self._view(node_id)

    def reserve(self, node_id: str, amount: int, *,
                reservation_id: str | None = None):
        """预留（**走共识**）。返回的 `reservation_id` 由**复制计数器**决定。

        `reservation_id` 入参被**忽略**并告警式记录：Raft 下 id 必须由共识
        分配，否则副本发散（这正是 K-81 的教训）。
        """
        from ..budget import Reservation

        self._bump_and_wait(self._replica.reserve, node_id, amount)
        rid = str(getattr(self._replica, "last_rid", "") or "")
        if not rid:
            return None
        return Reservation(rid, node_id, int(amount))

    def consume(self, reservation_id: str, amount: int = 1, *,
                acb=None) -> int:
        self._bump_and_wait(self._replica.consume, reservation_id, amount)
        return int(amount)

    def release(self, reservation_id: str) -> int:
        before = self._reserved_of(self._rid_owner(reservation_id))
        self._bump_and_wait(self._replica.release, reservation_id)
        return max(0, before - self._reserved_of(self._rid_owner(reservation_id)))

    def settle(self, reservation_id: str) -> int:
        before = self._reserved_of(self._rid_owner(reservation_id))
        self._bump_and_wait(self._replica.settle, reservation_id)
        return max(0, before - self._reserved_of(self._rid_owner(reservation_id)))

    # ---- 读面（本地已应用副本）-----------------------------------
    def _limits_of(self, node_id: str) -> int:
        return int((getattr(self._replica, "_limits", {}) or {}).get(node_id, 0))

    def _reserved_of(self, node_id: str) -> int:
        return int((getattr(self._replica, "_reserved", {}) or {}).get(node_id, 0))

    def _rid_owner(self, rid: str) -> str:
        ent = (getattr(self._replica, "_resv", {}) or {}).get(rid)
        return str(ent[0]) if ent else ""

    def _view(self, node_id: str) -> _NodeView:
        return _NodeView(node_id, self._limits_of(node_id),
                         self._reserved_of(node_id))

    @property
    def root(self) -> _NodeView:
        return self._view("run")

    @property
    def _nodes(self) -> dict:
        limits = dict(getattr(self._replica, "_limits", {}) or {})
        return {nid: self._view(nid) for nid in limits}

    @property
    def _reservations(self) -> dict:
        resv = dict(getattr(self._replica, "_resv", {}) or {})
        return {rid: (self._view(ent[0]), ent) for rid, ent in resv.items()}

    def check(self, node_id: str, amount: int = 0) -> bool:
        return self._reserved_of(node_id) + int(amount) <= self._limits_of(node_id)

    def check_reservations(self, reservation_ids, amount: int = 1) -> bool:
        return all(self._rid_owner(rid) for rid in reservation_ids)

    def _subtree_nodes(self, node: object) -> list:
        """本实现是**扁平**账本（无父子链）→ 子树即自身。"""
        return [node]

    # ---- 明确不可用（共识拥有）-----------------------------------
    def set_epoch(self, epoch: int, *, emit: bool = True) -> None:
        raise EpochNotBumpable("Raft 的预算 epoch 由共识 term 推进（S17 K-85）")

    def bump_epoch(self, run_id: str | None = None) -> int:
        raise EpochNotBumpable("Raft 的预算 epoch 由共识 term 推进（S17 K-85）")

    # ---- 非共识路径（**显式**声明；委托本地镜像，不保证集群一致）------
    # 注意：Python 3.12+ 的 `runtime_checkable` 用**静态**属性查找，
    # 动态 `__getattr__` **不满足** `isinstance(x, Protocol)`（实测 3.14）。
    # 因此这些成员必须显式定义，不能靠 `__getattr__` 兜底。
    def apply_budget_event(self, ev) -> None:
        """非共识路径：由本地镜像处理（仅用于重放/测试，不参与复制判定）。"""
        self._local.apply_budget_event(ev)

    def budget_events(self, run_id=None):
        return self._local.budget_events(run_id)

    def archive_node(self, node_id: str) -> bool:
        return self._local.archive_node(node_id)

    def remove_node(self, node_id: str) -> bool:
        return self._local.remove_node(node_id)

    def check_invariants(self) -> None:
        self._local.check_invariants()

    def gc(self, now_s: float) -> int:
        return self._local.gc(now_s)

    def suppressed(self):
        return self._local.suppressed()

    def discard_pending(self) -> None:
        self._local.discard_pending()

    def flush_pending(self) -> list:
        return self._local.flush_pending()

    def close(self) -> None:
        if self._owns_cluster:
            self._cluster.close()
