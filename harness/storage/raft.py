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
from contextlib import contextmanager

from ..budget import BudgetManager
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
              compaction_min_entries: int = 2, password: str = "harness-raft",
              **overrides):
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
        "password": password,        # 非空：pysyncobj 要求加密连接（S19 K-92 可配）
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

    @classmethod
    def from_addrs(cls, node_factory, *, addrs: list[str],
                   data_dir: str | None = None,
                   tick_interval: float = 0.002,
                   password: str = "harness-raft",
                   precheck: bool | str = "auto",
                   precheck_timeout: float = 1.0) -> RaftCluster:
        """**外部地址集群**（S18 K-83）：部署时传入真实 `host:port` 列表。

        校验：≥3 个地址、无重复、形如 `host:port`（端口为整数）、`data_dir`
        （若给）可写。**不发起任何连接** —— 连接由 pysyncobj 自己建立。

        诚实边界：跨机 / 网络分区 / 时钟漂移**未**在本仓库验证（沙箱不可测）。
        """
        if not isinstance(addrs, list) or len(addrs) < 3:
            raise ValueError(f"Raft 至少 3 个地址（当前 {len(addrs or [])}）")
        if len(set(addrs)) != len(addrs):
            raise ValueError(f"地址重复：{addrs}")
        for addr in addrs:
            host, _, port = str(addr).rpartition(":")
            if not host or not port.isdigit() or not (0 < int(port) < 65536):
                raise ValueError(f"地址格式应为 host:port：{addr!r}")

        if precheck:
            # S19 K-92：建簇前探测可达性。**为什么不是「全须可达」**：
            # 首次启动时三节点都还没监听，全不可达是**正常**的；真正的配置错误
            # 是「部分可达、部分不可达」（拼错某个地址/漏启动某台）——
            # 那种情况会让 `wait_ready` 干等 20s 且报错无法定位。
            #
            # `precheck="auto"`：全不可达 → 视为首次启动（放行）
            #                  部分可达 → 抛错并**列出不可达地址**
            # `precheck=True` ：严格（加入既有集群时用）
            # `precheck=False`：跳过
            import socket as _sock
            bad: list[str] = []
            reachable = 0
            for addr in addrs:
                host, _, port_s = addr.rpartition(":")
                try:
                    with _sock.create_connection((host, int(port_s)),
                                                 timeout=precheck_timeout):
                        reachable += 1
                except OSError as exc:
                    bad.append(f"{addr}({exc.__class__.__name__})")
            strict = precheck is True
            if bad and (strict or reachable > 0):
                raise RaftUnavailable(
                    f"地址可达性不一致/不足：不可达 {bad}，可达 {reachable}/"
                    f"{len(addrs)}；首次启动请传 precheck=False")

        cls_ = cls.__new__(cls)
        cls_._tick_interval = tick_interval
        cls_._stop = threading.Event()
        cls_.addrs = list(addrs)
        cls_.nodes = []
        for idx, addr in enumerate(addrs):
            partners = [a for a in addrs if a != addr]
            if data_dir:
                conf = raft_conf(
                    journal_file=f"{data_dir}/node{idx}.journal",
                    full_dump_file=f"{data_dir}/node{idx}.dump",
                    password=password)
            else:
                conf = raft_conf(password=password)
            cls_.nodes.append(node_factory(addr, partners, conf))
        cls_._thread = threading.Thread(target=cls_._run, daemon=True,
                                       name="raft-ticker")
        cls_._thread.start()
        return cls_

    def verify_connected(self) -> None:
        """S20 K-96：`wait_ready` 之后复核**真实连通性**。

        为什么需要：`precheck` 只证明 TCP 能建连（SYN 可达），**不等于**
        pysyncobj 能通信（密码 / 版本 / 集群不一致时仍会失败，但 `wait_ready`
        可能因为「本地已就绪」而通过）。这里用 `isNodeConnected` 逐 peer 复核，
        失败即列出断连地址。
        """
        lead = self.leader()
        if lead is None:
            raise RaftUnavailable("verify_connected：无 leader（集群未真正就绪）")
        probe = getattr(lead, "isNodeConnected", None)
        if not callable(probe):
            return                      # pysyncobj 版本不支持 → 跳过（不假报）
        # 注意：pysyncobj 的 `isNodeConnected(node)` 接收的是 **Node 对象**
        # （不是地址字符串），peer 列表在 `otherNodes` 里。
        peers = list(getattr(lead, "otherNodes", []) or [])
        bad = []
        for node in peers:
            try:
                ok = bool(probe(node))
            except Exception:  # noqa: BLE001  # 探测异常按「未连通」处理
                ok = False
            if not ok:
                bad.append(str(getattr(node, "address", node)))
        if bad:
            raise RaftUnavailable(
                f"以下 peer 未连通（TCP 可达但 Raft 不通，检查密码/版本）：{bad}")

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

        @replicated
        def barrier(self) -> None:
            """S18 K-82：**读穿日志**的一步 —— 写一条 no-op 日志项。

            它一旦提交，就证明「当前 leader 在提交这一刻仍是 leader（quorum 已确认）」，
            等价于 Raft ReadIndex 协议里的「为本次读做一轮心跳确认」。
            """
            self.barrier_count = int(getattr(self, "barrier_count", 0)) + 1
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
        if self._require_leader:
            self._wait_applied_commit()
        return super().has(subject, kind)

    # ---- S18 K-82：线性一致读（读穿日志）---------------------------
    def barrier(self) -> None:
        """等一条 no-op 日志提交 → 之后的本地读是**线性一致**的。

        为什么必须这样：pysyncobj 不暴露 ReadIndex（无「针对本次读的 quorum
        确认」API），`hasQuorum` 只是布尔、不能复用旧确认。写一条 no-op 日志是
        唯一能在**不改外部库**前提下正确实现的方式（Ongaro 论文 §6.4 列出的
        代价：每次读多一条日志写入）。
        """
        before = int(getattr(self._replica, "barrier_count", 0) or 0)
        self._replica.barrier()
        if not self._cluster.wait_until(
                lambda: int(getattr(self._replica, "barrier_count", 0)) > before,
                self._timeout):
            raise RaftUnavailable(f"barrier {self._timeout}s 内未提交")
        # 审查 P0-1：barrier 只等计数器不够 —— 必须把**已提交但尚未索引**的记录
        # 同步进本地 `_events`，否则随后的 `all()/replay()` 仍看到旧视图。
        with self._lock:
            self._sync_applied()

    @contextmanager
    def linearizable(self):
        """上下文：`with log.linearizable(): log.all()` → 线性一致读。

        显式开启（默认不自动 barrier），避免「每次读都写日志」的写放大。
        """
        self.barrier()
        yield self

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
# S18 K-87：Raft 预算 = **真 BudgetManager 的 RSM**（树 / 归档 / gc / 不变量）
# =====================================================================
if _HAS_PYSYNCOBJ:
    class _ReplicatedBudget(SyncObj):                     # pragma: no cover
        """预算的复制状态机 —— 内部**直接持有真实的 `BudgetManager`**。

        S17 的实现是「扁平账本」（只有 `limits/reserved/resv` 三个 dict），
        后果：`subtree_reserved` 语义丢失、`archive_node` 折叠永久账（I5c）
        与 `check_invariants` 只能在 leader 本地跑 → **各副本不一致**。

        S18 改为：把 `BudgetManager` 整个作为复制状态（它有 `__getstate__`
        去掉 `RLock`，可被 pysyncobj 序列化到 journal/快照）。
        于是「树 / 归档折叠 / gc / 不变量」全部成为共识路径的确定性结果。

        **确定性前提**：`now`（归档/gc 用的时间）必须由**命令参数**传入，
        不能各副本自己取 `time.time()`。`reservation_id` 由复制计数器分配。
        """

        def __init__(self, addr, partners, conf) -> None:
            super().__init__(addr, partners, conf=conf)
            self.bm = BudgetManager(event_log=None)   # 静默：不 emit 事件
            self.version = 0
            self._seq = 0
            self.last_rid = ""
            self.last_ok = False
            # S19 K-89：每个 ACB 的**最大** claim_epoch（单调）；旧 worker 的低
            # epoch 会被永久拒（fencing），无需额外 set_claim 调用。
            self.claims = {}
            # S20 K-94：已剪 ACB 的 claim 墓碑（**有界 LRU**）。裸删会让旧 worker
            # 复活（`claims.get` 回 0 → 任何 epoch 都通过）。
            self.claim_tombstones = {}
            self.claim_tombstone_max = 1024

        @replicated
        def create_node(self, node_id: str, limits: int,
                        parent_id: str, run_id: str) -> None:
            # 审查 P1-4：`create_node` 在「同名但 limits 不同」时返回 None。
            # 必须回传该结果，否则调用方以为改成功（实际仍是旧节点）。
            node = self.bm.create_node(node_id, limits, parent_id, run_id=run_id)
            self.last_ok = node is not None
            self.version += 1

        @replicated
        def reserve(self, node_id: str, amount: int) -> None:
            self._seq += 1
            rid = f"R{self._seq}"
            resv = self.bm.reserve(node_id, amount, reservation_id=rid)
            self.last_rid = resv.reservation_id if resv is not None else ""
            self.version += 1

        @replicated
        def consume(self, rid: str, amount: int, acb_id: str = "",
                    claim_epoch: int = 0) -> None:
            """消费 + **claim fencing**（S19 K-89）。

            `claim_epoch` 作为命令参数参与复制 → 一次日志写入内完成
            「校验 claim + 变更账本」，副本间确定。
            单调：`claim_epoch < 已记录` → 拒绝（旧 worker 被 fence）。
            """
            floor = max(int(self.claims.get(acb_id, 0)),
                        int(self.claim_tombstones.get(acb_id, 0)))
            if acb_id and int(claim_epoch) < floor:
                self.last_ok = False
                self.version += 1
                return
            if acb_id:
                self.claims[acb_id] = int(claim_epoch)
            self.bm.consume(rid, amount)
            self.last_ok = True
            self.version += 1

        @replicated
        def release(self, rid: str, now: float | None = None) -> None:
            self.bm.release(rid, now=now)
            self.version += 1

        @replicated
        def settle(self, rid: str, now: float | None = None) -> None:
            self.bm.settle(rid, now=now)
            self.version += 1

        @replicated
        def archive_node(self, node_id: str, now: float) -> None:
            # 审查 P0-3：必须把**实际结果**记下来回传 —— Harness 依据 True 清空
            # `acb.budget_node`；若归档被拒（仍有 ACTIVE 预留）却返回 True，
            # 会造成节点/预留泄漏。
            self.last_ok = bool(self.bm.archive_node(node_id, now=now))
            self.version += 1

        @replicated
        def remove_node(self, node_id: str, owner: str = "") -> None:
            # S19 K-93：owner 校验也走共识（原来只在本机判断）
            self.last_ok = bool(self.bm.remove_node(node_id, owner=owner))
            self.version += 1

        @replicated
        def gc(self, now: float) -> None:
            self.removed = int(self.bm.gc(now))
            self.version += 1

        @replicated
        def prune(self, now: float, live_rids: tuple | None,
                  retention_s: float, live_acb_ids: tuple | None = None) -> None:
            """S19 K-88：`now` / `live_rids` / **生效保留期**都随命令复制 → 确定。

            **为什么传 `retention_s` 而不是让副本自己读 `_tombstone_ttl_s`**：
            ttl 是本地配置，不是复制状态；若两副本配置不同，同一命令会得出
            不同结果 → 状态机发散。由调用方算好再复制，才是确定性 RSM。
            """
            self.removed = int(self.bm.prune(
                now, live_rids, retention_s=retention_s))
            # S20 K-94：只剪「不在活 ACB 集合」的 claim，且**先写墓碑**再删。
            if live_acb_ids is not None:
                live_ids = set(live_acb_ids)
                for aid in list(self.claims):
                    if aid in live_ids:
                        continue
                    self.claim_tombstones[aid] = int(self.claims.pop(aid))
                # 有界：按插入顺序挤出最旧（dict 保序）
                overflow = len(self.claim_tombstones) - int(
                    self.claim_tombstone_max)
                for old in list(self.claim_tombstones)[:max(0, overflow)]:
                    self.claim_tombstones.pop(old, None)
            self.version += 1
else:                                                     # pragma: no cover
    class _ReplicatedBudget:                              # type: ignore[no-redef]
        def __init__(self, *a, **kw) -> None:
            require_pysyncobj()


class RaftBudgetManager:
    """`BudgetManager` 的 Raft 后端（**真 RSM**：连树与归档都走共识）。

    一致性范围（S18 起）：
    - **集群一致**：`create_node` / `reserve` / `consume` / `release` / `settle` /
      `archive_node` / `remove_node` / `gc` —— 全部经 quorum 提交；
      读直接取本地已应用副本的 `BudgetManager`（树、账目、不变量都真实）。
    - **仍不可用**：`set_epoch` / `bump_epoch`（epoch 由共识 term 推进，K-85）。

    与 S17 的关键差别：S17 的 `archive/gc/check_invariants` 是「leader 本地」，
    现在它们是共识结果 → 可以在**任意副本**上跑 `check_invariants()` 并一致。
    """

    def __init__(self, *, base_port: int, nodes: int = 3,
                 cluster: RaftCluster | None = None,
                 timeout: float = 20.0,
                 data_dir: str | None = None) -> None:
        require_pysyncobj()
        self._timeout = timeout
        self._cluster = cluster or RaftCluster(
            _ReplicatedBudget, base_port=base_port, nodes=nodes,
            data_dir=data_dir)
        self._owns_cluster = cluster is None
        self._replica = self._cluster.nodes[0]
        if not self._cluster.wait_ready(timeout):
            raise RaftUnavailable(
                f"Raft 集群 {timeout}s 内未就绪（端口 {base_port}）")

    # ---- 内部 -----------------------------------------------------
    @property
    def _bm(self) -> BudgetManager:
        """本地已应用副本的**真实**预算管理器（树/账目/不变量）。"""
        return self._replica.bm

    def _bump_and_wait(self, fn, *args) -> None:
        before = int(getattr(self._replica, "version", 0))
        fn(*args)
        if not self._cluster.wait_until(
                lambda: int(getattr(self._replica, "version", 0)) > before,
                self._timeout):
            raise RaftUnavailable(
                f"预算操作 {getattr(fn, '__name__', fn)} {self._timeout}s 内未提交")

    def _now(self) -> float:
        """命令参数用的时间戳（**由调用方决定并复制**，各副本一致）。"""
        import time as _t
        return float(int(_t.time()))

    # ---- 共识路径（写）-------------------------------------------
    def create_node(self, node_id: str, limits: int,
                    parent_id: str = "run", run_id: str = ""):
        """建节点（共识）。**同名不同 limits → 返回 None**（不静默返回旧节点）。"""
        self._bump_and_wait(self._replica.create_node, node_id, int(limits),
                            parent_id, run_id)
        if not bool(getattr(self._replica, "last_ok", False)):
            return None
        return self._bm._nodes.get(node_id)

    def reserve(self, node_id: str, amount: int, *,
                reservation_id: str | None = None):
        """预留（**走共识**）。id 由复制计数器分配；入参 `reservation_id` 被忽略
        （Raft 下 id 必须由共识分配，否则副本发散 —— K-81 的教训）。"""
        self._bump_and_wait(self._replica.reserve, node_id, int(amount))
        rid = str(getattr(self._replica, "last_rid", "") or "")
        if not rid:
            return None
        from ..budget import Reservation

        return Reservation(rid, node_id, int(amount))

    def consume(self, reservation_id: str, amount: int = 1, *,
                acb=None) -> int:
        """消费（共识）。

        **审查 P0-2 的处置**：`claim_epoch` fencing 目前**没有**被复制进预算状态机，
        而 `BudgetManager.consume` 的 `claim_guard` 在副本里恒为空 → 若静默接受
        `acb`，等于**悄悄丢掉 fencing**。这里选择 **fail-closed**：带 claim 的
        调用直接拒绝，而不是假装成功（K-89）。
        """
        acb_id = str(getattr(acb, "id", "") or "") if acb is not None else ""
        epoch = int(getattr(acb, "claim_epoch", 0) or 0)
        self._bump_and_wait(self._replica.consume, reservation_id,
                            int(amount), acb_id, epoch)
        if not bool(getattr(self._replica, "last_ok", True)):
            # S19 K-89：claim epoch 过旧（旧 worker）→ 拒绝，不静默记账
            return 0
        return int(amount)

    def release(self, reservation_id: str, *,
                now: float | None = None) -> int:
        node = self._bm._reservations.get(reservation_id)
        before = int(getattr(node[1], "amount", 0)) if node else 0
        self._bump_and_wait(self._replica.release, reservation_id, now)
        return before

    def settle(self, reservation_id: str, *,
               now: float | None = None) -> int:
        node = self._bm._reservations.get(reservation_id)
        before = int(getattr(node[1], "amount", 0)) if node else 0
        self._bump_and_wait(self._replica.settle, reservation_id, now)
        return before

    def archive_node(self, node_id: str, *, now: float | None = None) -> bool:
        """归档（**走共识**）；`now` 由调用方决定并随命令复制（保证确定性）。"""
        stamp = self._now() if now is None else float(now)
        self._bump_and_wait(self._replica.archive_node, node_id, stamp)
        return bool(getattr(self._replica, "last_ok", False))

    def remove_node(self, node_id: str, *, owner: str = "") -> bool:
        self._bump_and_wait(self._replica.remove_node, node_id, owner)
        return bool(getattr(self._replica, "last_ok", False))

    def gc(self, now: float) -> int:
        self._bump_and_wait(self._replica.gc, float(now))
        return int(getattr(self._replica, "removed", 0) or 0)

    def prune(self, now: float, live_rids=None, *,
              max_age_s: float | None = None,
              live_acb_ids=None) -> int:
        """S19/S20：三栅栏剪枝 + claim 墓碑清理（参数全部随命令复制）。

        `live_rids=None` → **fail-safe 不剪**（K-95）；`live_acb_ids=None` →
        不动 claims（同样 fail-safe）。
        """
        if live_rids is None and live_acb_ids is None:
            return 0
        ttl = float(self._bm._tombstone_ttl_s)
        retention = ttl if max_age_s is None else min(ttl, float(max_age_s))
        rids = ("__none__",) if live_rids is None else tuple(
            sorted(str(r) for r in live_rids))
        self._bump_and_wait(
            self._replica.prune, float(now),
            None if live_rids is None else rids, retention,
            None if live_acb_ids is None else tuple(
                sorted(str(a) for a in live_acb_ids)))
        return int(getattr(self._replica, "removed", 0) or 0)

    def remaining(self, node_id: str) -> int:
        return self._bm.remaining(node_id)

    # ---- 读面（本地已应用副本的**真实**账本）-----------------------
    @property
    def root(self):
        return self._bm.root

    @property
    def _nodes(self) -> dict:
        return self._bm._nodes

    @property
    def _reservations(self) -> dict:
        return self._bm._reservations

    def check(self, node_id: str, amount: int = 0) -> bool:
        return self._bm.check(node_id, amount)

    def check_reservations(self, reservation_ids, amount: int = 1) -> bool:
        return self._bm.check_reservations(reservation_ids, amount)

    def _subtree_nodes(self, node) -> list:
        return self._bm._subtree_nodes(node)

    def check_invariants(self, *, cross_process: bool = False) -> None:
        """在**任意副本**上校验（S18 起是集群一致的，不再是 leader 本地）。"""
        self._bm.check_invariants(cross_process=cross_process)

    # ---- 明确不可用（共识拥有）-----------------------------------
    def set_epoch(self, epoch: int, *, emit: bool = True) -> None:
        raise EpochNotBumpable("Raft 的预算 epoch 由共识 term 推进（S17 K-85）")

    def bump_epoch(self, run_id: str | None = None) -> int:
        raise EpochNotBumpable("Raft 的预算 epoch 由共识 term 推进（S17 K-85）")

    # ---- 事件面：副本内 `event_log=None` → 均为 no-op（显式声明）----
    def apply_budget_event(self, ev) -> None:
        self._bm.apply_budget_event(ev)

    def budget_events(self, run_id=None):
        return self._bm.budget_events(run_id)

    def suppressed(self):
        return self._bm.suppressed()

    def discard_pending(self) -> None:
        self._bm.discard_pending()

    def flush_pending(self) -> list:
        return self._bm.flush_pending()

    # ---- 与 Harness/checkpoint 的装配面（审查 P0-5）-----------------
    # `checkpoint._build_payload` 会读 `budget._lock` / `budget.root`；
    # `Harness.recover` 会调 `budget._reset_baseline()`。缺这些会让「装到 Harness
    # 即 AttributeError」——契约测试只查 hasattr 发现不了。
    @property
    def _lock(self):
        return self._bm._lock

    @property
    def _last_budget_seq(self) -> int:
        return int(getattr(self._bm, "_last_budget_seq", 0) or 0)

    def _reset_baseline(self) -> None:
        """恢复基线（委托本地副本；**不**广播 —— 各副本各自恢复自己的镜像）。"""
        self._bm._reset_baseline()

    def close(self) -> None:
        if self._owns_cluster:
            self._cluster.close()
