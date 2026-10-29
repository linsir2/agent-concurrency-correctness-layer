"""Harness —— 组装所有组件的门面（对应文档 §20.2 端到端数据流）。

用法见 `examples/` 与 `tests/`。
"""

from __future__ import annotations

import itertools
import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor

from .auth import Signer
from .budget import BudgetManager
from .checkpoint import (
    CheckpointStore,
    EffectJournal,
    FileCheckpointStore,
)
from .clock import SystemClock
from .compensation import CompensationJournal
from .context import ContextAssembler
from .egress import EgressGuard
from .event_log import EventLog
from .ingress import Ingress
from .intent_registry import IntentRegistry
from .kernel.children import ChildDeps, ChildManager
from .kernel.commit import CommitDeps, CommitService
from .kernel.dispatch import (
    _MAX_INLINE_DEPTH,          # noqa: F401 —— 旧名再导出（tests/test_s14_boundaries 从这里取）
    DispatchDeps,
    DispatchService,
)
from .kernel.identity import IdentityDeps, SigningService
from .kernel.leases import EpochStore, LeaseDeps, WriterLeaseService
from .kernel.receipts import (
    ReceiptChain,
    ReceiptDeps,
    ReceiptScanFailed,
)
from .kernel.recovery import (
    RecoveryDeps,
    RecoveryService,
)
from .kernel.state import KernelState
from .kernel.tasks import TaskDeps, TaskService
from .llm import LLMClient
from .orchestration import (
    HITL,
    Dispatcher,
    Planner,
    ReconcileLoop,
    RunCoordinator,
    Scheduler,
    SlotTracker,
)
from .policy import (
    CapabilityToken,
    PolicyEngine,
)
from .resolver import ScriptedResolver
from .runtime import AgentRuntime
from .storage._lock import (
    RECEIPTS_ORDER,
    FencingRejected,
    _FileLock,
    receipts_lock_path,
)
from .storage.blob import InMemoryBlobStore
from .storage.lease import (
    LeaseStore,
)
from .subscription import SubscriptionRegistry
from .timer import DurableTimer
from .types import (
    ACB,
    TASK_TERMINAL,
    EventKind,
    Mode,
    Result,
    TaskState,
    Write,
)
from .workspace import WorkspaceManager, WorkspaceVersionStore

# S13d §2.5 P0-9：worker 内嵌套 `run_task` 一律内联；该上限只用于**防递归**
# （不是 spawn 的父子层数 `spawn_depth`，两者语义不同）。
# S3.3：常量随实现迁至 `kernel/dispatch.py`，此处只做旧名再导出（见文件顶部 import）。

# S22：外部幂等键命中「同键异内容」时的哨兵（与「无命中=None」区分开）。






class Harness:
    """组装所有组件的门面（对应文档 §20.2 端到端数据流）。

    所有可调常量均可注入（避免硬编码，见 milestones.md §5）。
    """

    def __init__(self, run_limits: int = 1000, retention: int = 10000,
                 lease_ms: int = 60_000, guard_every: int = 1,
                 aging_max: int = 5, max_steps: int = 10_000,
                 max_concurrent: int = 100,
                 policy_default_allow: bool = False, *,
                 hitl_timeout_ms: int = 30_000,
                 clock=None, event_log=None, intents=None, versions=None,
                 workspace=None, budget=None, checkpoints=None, effects=None,
                 policy=None, context=None, subscriptions=None,
                 decide=None, default_token=None, resolver=None,
                 blobs=None, path_of=None, lock_timeout_s: float = 5.0,
                 auth_secret: str | None = None,
                 tombstone_ttl_s: float = 86400.0,
                 auth_gc_every: int = 256,
                 gc_every: int = 1000,
                 per_action_delay: float = 0.0,
                 receipts_enabled: bool = True,
                 checkpoint_root=None,
                 writer_id: str | None = None,
                 lease_store: LeaseStore | None = None,
                 entropy=None,
                 definitions=None,
                 timers=None,
                 durable_timers: bool = False,
                 egress_required: bool | None = None,
                 epoch_scope: str = "global",
                 replan_budget: int = 2,
                 spawn_depth: int = 1,
                 max_fanout: int = 6,
                 min_budget_per_child: int = 1,
                 scheduler_mode: str = "sync",
                 pool_workers: int = 4,
                 pool_timeout_s: float = 120.0,
                 claim_lease_ms: int | None = None,
                 aging_ms: int = 0,
                 aging_max_attempts: int = 3,
                 max_redrives: int = 3,
                 poison_after_failures: int = 3,
                 drain_timeout_s: float = 5.0) -> None:
        if epoch_scope not in ("run", "global"):
            raise ValueError(f"未知 epoch_scope：{epoch_scope!r}（run|global）")
        # S24-10：确定性模式的安全/边界（fail-closed，绝不静默削弱）
        if entropy is not None:
            if clock is not None and clock is not entropy:
                raise ValueError("entropy 与 clock 不能同时提供（entropy 自带时钟）")
            if auth_secret or os.environ.get("HARNESS_AUTH_SECRET"):
                raise ValueError(
                    "确定性模式不支持 auth_secret：Signer 的 jti/exp 必须是真实随机/墙钟")
            if scheduler_mode != "sync":
                raise ValueError("确定性模式只支持 scheduler_mode='sync'（并发交错不可重放）")
        self.entropy = entropy
        # S1：跨服务共享状态容器（字段与旧 `self._xxx` 一一对应）
        self.state = KernelState()
        # S24-11：定义版本化（默认 None → 无 definition 键时零影响）
        self.definitions = definitions
        if timers is not None and durable_timers:
            raise ValueError("timers 与 durable_timers 不能同时提供")
        if isinstance(replan_budget, bool) or not isinstance(replan_budget, int) \
                or replan_budget < 0:
            raise ValueError(f"非法 replan_budget：{replan_budget!r}（>=0 的整数）")
        self.epoch_scope = epoch_scope
        self.state.replan_budget = replan_budget  # S13b §2.3：replan 预算（写前计数）
        # S13c §2.4 C-5：有界 spawn（Codex 默认 1 层 / fanout 6）
        if isinstance(spawn_depth, bool) or not isinstance(spawn_depth, int) \
                or spawn_depth < 0:
            raise ValueError(f"非法 spawn_depth：{spawn_depth!r}（>=0 的整数）")
        if isinstance(max_fanout, bool) or not isinstance(max_fanout, int) \
                or max_fanout < 0:
            raise ValueError(f"非法 max_fanout：{max_fanout!r}（>=0 的整数）")
        if (isinstance(min_budget_per_child, bool)
                or not isinstance(min_budget_per_child, int)
                or min_budget_per_child < 1):
            raise ValueError(
                f"非法 min_budget_per_child：{min_budget_per_child!r}（>=1 的整数）")
        self.state.spawn_depth = spawn_depth
        self.state.max_fanout = max_fanout
        self.state.min_budget_per_child = min_budget_per_child
        self._spawn_counter = itertools.count(1)
        # ---- S13d §2.5：Scheduler 模式 + claim 租约 + aging（墙钟）----
        if scheduler_mode not in ("sync", "pool"):
            raise ValueError(f"未知 scheduler_mode：{scheduler_mode!r}（sync|pool）")
        if isinstance(pool_workers, bool) or not isinstance(pool_workers, int) \
                or pool_workers < 1:
            raise ValueError(f"非法 pool_workers：{pool_workers!r}（>=1 的整数）")
        if isinstance(aging_ms, bool) or not isinstance(aging_ms, int) or aging_ms < 0:
            raise ValueError(f"非法 aging_ms：{aging_ms!r}（>=0 的整数，0=禁用）")
        if (isinstance(aging_max_attempts, bool)
                or not isinstance(aging_max_attempts, int)
                or aging_max_attempts < 1):
            raise ValueError(
                f"非法 aging_max_attempts：{aging_max_attempts!r}（>=1 的整数）")
        self.state.scheduler_mode = scheduler_mode
        self.state.aging_ms = int(aging_ms)
        self.state.aging_max_attempts = int(aging_max_attempts)
        # claim 租约 TTL 必须覆盖「单 action 上界」：至少 >= 资源租约 `lease_ms`，
        # 否则一个慢 action 期间 claim 就会过期 → 第二个 worker 合法重 claim。
        if int(lease_ms) <= 0:
            raise ValueError(
                f"非法 lease_ms：{lease_ms!r}（必须 > 0；ttl<=0 等于关闭租约过期 → fencing 失效）")
        claim_ttl = lease_ms if claim_lease_ms is None else int(claim_lease_ms)
        if claim_ttl < int(lease_ms):
            raise ValueError(
                f"claim_lease_ms({claim_ttl}) 必须 >= lease_ms({lease_ms})"
                "（否则单 action 未完成即可被重 claim）")
        self.state.claim_lease_ms = claim_ttl
        self.state.pool_workers = int(pool_workers)
        self.state.pool_timeout_s = float(pool_timeout_s)
        self._tls = threading.local()             # in_worker / depth / worker_id
        self._worker_ids = itertools.count(1)
        self._pool = (ThreadPoolExecutor(max_workers=self.pool_workers,
                                         thread_name_prefix="harness-worker")
                      if scheduler_mode == "pool" else None)
        self.state.lease_ms = int(lease_ms)
        self.guard_every = guard_every
        self.aging_max = aging_max
        self.max_steps = max_steps
        self.max_concurrent = max_concurrent
        self.state.hitl_timeout_ms = int(hitl_timeout_ms)  # S13a：审批超时（tick 判定）
        self.state.retention = retention
        self.per_action_delay = per_action_delay   # §2.5：基准配对用注入点
        # S12 K-33：per-run 高水位游标（每 run 独立推进，见 `floor_for`）
        self.state.run_cursors = {}

        # ---- S11c/d/e：egress 强制点（见 `harness/egress/__init__.py`）---------
        # `egress_required=True`  → 构造即校验，无强制点 → `EgressUnavailable`（拒绝启动）
        # `egress_required=None`  → 构造不校验，**任何出站** fail-closed（默认）
        # `egress_required=False` → 显式降级（仅测试/离线），原因记入 `degradations`
        self.egress = EgressGuard(required=egress_required)
        # S11-8 / 终审 8-1：真实出站路径（`llm._post`）必须过同一个 guard；
        # 只构造 guard 不接线等于「有锁不装门」——`ensure()` 永远不会被调用。
        self.llm = LLMClient(egress=self.egress)

        # S24-10：确定性熵源同时充当时钟（严格单调的模拟时间）
        self.clock = entropy if entropy is not None else (clock or SystemClock())
        self.event_log = (event_log if event_log is not None
                          else EventLog(clock=self.clock, entropy=entropy))
        if entropy is not None:
            # 覆盖用户传入的 EventLog：事件 id 与 ts 都必须可重放
            setter = getattr(self.event_log, "set_entropy", None)
            if callable(setter):
                setter(entropy)
            self.event_log._clock = entropy
        # S24-11：DurableTimer（默认关闭；开启时要求可持久事件日志）
        if timers is None and durable_timers:
            durable_log = (getattr(self.event_log, "_wal", None) is not None
                           or type(self.event_log).__name__ == "RaftEventLog")
            if not durable_log:
                raise ValueError(
                    "durable_timers=True 需要 WAL/Raft 事件日志（内存日志无法持久）")
            timers = DurableTimer(self.event_log, self.clock)
        self.timers = timers
        self._timers_fired: list = []
        # S12 K-43：run 层 epoch store。WAL 模式 → `wal.log.epoch.<run_id>`；
        # 文件 checkpoint 模式 → `store.epoch.<run_id>`（与 store.json 同目录）；
        # 均无 → 纯内存（旧用例）。
        _wal0 = getattr(self.event_log, "_wal", None)
        _epoch_prefix = None
        if _wal0 is not None and getattr(_wal0, "epoch_path", None) is not None:
            _epoch_prefix = str(_wal0.epoch_path)
        else:
            _root = (checkpoint_root if checkpoint_root is not None
                     else getattr(checkpoints, "root", None))
            if _root is not None:
                _epoch_prefix = os.path.join(str(_root), "store.epoch")
        self._epoch_store = EpochStore(_epoch_prefix)
        self.blobs = blobs if blobs is not None else InMemoryBlobStore()
        self.intents = (intents if intents is not None
                        else IntentRegistry(self.clock, run_scope=self.epoch_scope))
        self.intents._epoch_store = self._epoch_store    # S12 K-43：run 层 epoch
        self.intents._run_scope = self.epoch_scope       # S12 K-43：scope 注入
        self.versions = versions if versions is not None else WorkspaceVersionStore()
        self.state.lock_timeout_s = float(lock_timeout_s)
        self.workspace = (workspace if workspace is not None
                          else WorkspaceManager(self.event_log, self.intents, self.versions,
                                                blobs=self.blobs, path_of=path_of,
                                                lock_timeout_s=lock_timeout_s))
        self.budget = (budget if budget is not None
                       else BudgetManager(run_limits, tombstone_ttl_s=tombstone_ttl_s,
                                          run_scope=self.epoch_scope,
                                          clock=entropy,
                                          id_factory=(entropy.token
                                                      if entropy is not None else None)))
        if entropy is not None:
            # 用户传入的 BudgetManager 也必须可重放（id + 时间）
            self.budget._clock = entropy
            self.budget._id_factory = entropy.token
        self.budget._epoch_store = self._epoch_store    # S12 K-43：run 层 epoch
        self.budget._run_scope = self.epoch_scope        # S12 K-43：scope 注入
        # S10a §2.1：预算事件溯源 = 唯一真相源（预算变更必须落到全局 EventLog）
        self.budget.event_log = self.event_log
        self._run_limits = int(run_limits)
        self.checkpoints = (checkpoints if checkpoints is not None
                            else (FileCheckpointStore(
                                checkpoint_root, entropy=entropy, clock=self.clock)
                                  if checkpoint_root is not None
                                  else CheckpointStore()))
        self.checkpoints.event_log = self.event_log      # §2.4：无条件注入全局水位源
        self.checkpoints.budget = self.budget            # S10a：聚合值 + 游标同临界区
        self.checkpoints.run_limits = self._run_limits   # S10a：run_limits 漂移基准
        # S11b K-46：fencing epoch 绑 WAL（`wal.log.epoch`）；无 WAL 时退回 store.json
        _wal = getattr(self.event_log, "_wal", None)
        self._wal_epoch_backend = (
            _wal if callable(getattr(_wal, "read_epoch", None)) else None)
        self.budget._epoch_source = (
            _wal.read_epoch if self._wal_epoch_backend is not None
            else self.checkpoints.read_epoch)
        # S1：写者租约服务（组件均已就绪；此时尚未发生任何 WAL append）
        self.leases = WriterLeaseService(
            LeaseDeps(budget=self.budget, checkpoints=self.checkpoints,
                      clock=self.clock, event_log=self.event_log,
                      intents=self.intents, epoch_scope=self.epoch_scope,
                      wal_epoch_backend=self._wal_epoch_backend),
            self.state)
        # S10b §3.4：WAL append 的 fencing（同一 flock 内复验 run 级 epoch）
        self._wire_wal_fencing()
        # ---- S10b §3.2 / S11b §3.2：跨进程锁全序（写死）--------------------
        #   resource → budget → receipts.lock → wal.lock → epoch → store.lock
        #   → eventlog._lock
        # 各 flock 按上表偏序获取；反向获取（如「持 store.lock 取 epoch」）会成环
        # 死锁 → 由 `_FileLock(order=...)` 运行期断言拦截。
        # `receipts.lock`：durable 模式跨进程互斥；内存模式退回进程内 RLock。
        cp_root = getattr(self.checkpoints, "root", None)
        self.state.receipts_flock = (
            _FileLock(receipts_lock_path(cp_root), order=RECEIPTS_ORDER)
            if cp_root is not None else None)
        # S10b §3.4：WAL append fencing 延后到租约服务建立之后（首个 append 之前）
        self.effects = (effects if effects is not None
                        else EffectJournal(self.event_log))
        self.state.comp_journal = CompensationJournal(self.event_log)
        self._applied_event_ids: set[str] = set()
        self.checkpoints.applied_event_ids = self._applied_event_ids
        self.state.drain_skipped = []      # D11：调度跳过原因（不再静默）
        # S14 K-69：`tick` 的跳过原因（水合失败等），与调度跳过分开，避免混淆
        self.state.tick_skipped = []
        self.auth_gc_every = max(1, int(auth_gc_every))
        self.gc_every = max(1, int(gc_every))
        self.state.event_count = 0
        self.state.auth_secret = auth_secret
        self.state.auth = Signer(auth_secret) if auth_secret else None
        self.subscriptions = (subscriptions if subscriptions is not None
                              else SubscriptionRegistry(self.event_log))
        self.policy = (policy if policy is not None
                       else PolicyEngine(default_allow=policy_default_allow,
                                         default_token=default_token))
        # S1：身份服务（签名 / 有效 token / 幂等键 / 事件分发）
        self.identity = SigningService(
            IdentityDeps(clock=self.clock, policy=self.policy, budget=self.budget,
                         subscriptions=self.subscriptions,
                         auth_gc_every=self.auth_gc_every,
                         gc_every=self.gc_every),
            self.state)
        self.policy._signer_factory = self.identity.get_signer
        # S1：收据链服务（构造顺序：event_log → workspace → policy → receipts）
        self.state.receipts_enabled = bool(receipts_enabled)
        self.receipts = ReceiptChain(
            ReceiptDeps(event_log=self.event_log, workspace=self.workspace,
                        epoch_scope=self.epoch_scope,
                        ),
            self.state)
        self.identity.bind_receipts(self.receipts)
        # S2：提交服务（依赖 workspace/intents/blobs/event_log/receipts/leases）
        self.commit = CommitService(
            CommitDeps(workspace=self.workspace, intents=self.intents,
                       blobs=self.blobs, event_log=self.event_log,
                       receipts=self.receipts, leases=self.leases,
                       # late lookup：保留「构造后替换实例方法仍生效」的旧语义
                       claim_guard=lambda acb: self._claim_fenced(acb),
                       definition_check=lambda ref: self.check_definition(ref)),
            self.state)
        self._sync_run_cursors()          # S12 K-33：从既有事件初始化 per-run 游标
        self.context = (context if context is not None
                        else ContextAssembler(self.event_log, self.workspace, retention,
                                              floor_for=lambda *a, **k: self.floor_for(*a, **k),
                                              run_scoped=(self.epoch_scope == "run")))
        self.decide = decide
        self.resolver = (resolver if resolver is not None
                         else ScriptedResolver())       # §7 默认 scripted（不联网）

        self.slots = SlotTracker(max_concurrent, shared=self.checkpoints)
        self.checkpoints.slots = self.slots        # checkpoint 快照活跃槽位
        # F6：checkpoint 落盘的是**有效 token**（`acb.capability` → `policy._tokens`
        # → `default_token`），否则跨进程 recover 会拿到 `None` → 工具全 DENIED。
        self.checkpoints.token_provider = lambda *a, **k: self._effective_token(*a, **k)
        self.planner = Planner(self)
        self.dispatcher = Dispatcher(self)
        self.scheduler = Scheduler(self, mode=scheduler_mode)
        self.run_coordinator = RunCoordinator(self)
        # S3.1：子任务 / spawn 簇（依赖此刻已全部就绪：budget/checkpoints/
        # event_log/policy 在上，slots/run_coordinator 在紧邻上方）。构造不追加任何
        # 事件。与 S1/S2 一致：仍留在 Harness 上的方法走 **late lookup lambda**，
        # 保证「构造后替换实例方法」（tests 打桩）依旧生效。
        self.children = ChildManager(
            ChildDeps(budget=self.budget, checkpoints=self.checkpoints,
                      event_log=self.event_log, policy=self.policy,
                      slots=self.slots, run_coordinator=self.run_coordinator,
                      safe_save=lambda *a, **k: self._safe_save(*a, **k),
                      effective_token=lambda *a, **k: self._effective_token(*a, **k),
                      register_admission=lambda *a, **k: self._register_admission(*a, **k),
                      transition_lock=lambda *a, **k: self._transition_lock(*a, **k),
                      finalize=lambda *a, **k: self._finalize(*a, **k)),
            self.state)
        self.reconcile = ReconcileLoop(self)
        self.hitl = HITL()
        # S3.2：准入 / 执行 / 终态 / 重规划簇。依赖此刻已全部就绪（budget /
        # checkpoints / event_log / policy 在上；slots / dispatcher / planner /
        # scheduler / run_coordinator / children / reconcile / hitl 在紧邻上方）。
        # 构造不追加任何事件。可变配置/容器一律在调用时读 `KernelState`；仍留在
        # Harness 上的方法走 **late lookup lambda** —— 绑定方法会冻结原实现，
        # 让 tests 的类级/实例级打桩（`_hitl_gate` / `run_task` / `tick`）失效。
        self.tasks = TaskService(
            TaskDeps(budget=self.budget, checkpoints=self.checkpoints,
                     event_log=self.event_log, hitl=self.hitl,
                     intents=self.intents, policy=self.policy,
                     slots=self.slots, dispatcher=self.dispatcher,
                     planner=self.planner, reconcile=self.reconcile,
                     run_coordinator=self.run_coordinator,
                     scheduler=self.scheduler, children=self.children,
                     safe_save=lambda *a, **k: self._safe_save(*a, **k),
                     effective_token=lambda *a, **k: self._effective_token(*a, **k),
                     ensure_policy_token=lambda *a, **k:
                         self._ensure_policy_token(*a, **k),
                     run_task=lambda *a, **k: self.run_task(*a, **k),
                     purge_run=lambda *a, **k: self.purge_run(*a, **k),
                     token_cleanup=lambda: getattr(self, "_token_cleanup", None)),
            self.state)
        # ---- S15 D：生产化（Admin 暂停 / DLQ 重放 / Ingress 幂等）----
        # pause 只阻止**未开始**的调度，资源保留（可恢复）；与 cancel 不同。
        self.state.paused_runs = set()
        self.state.max_redrives = max(0, int(max_redrives))
        # S17 MS10：毒任务识别阈值（连续失败 >= N → 直接 DLQ，避免无限重试）
        self.state.poison_after_failures = max(0, int(poison_after_failures))
        self.state.drain_timeout_s = float(drain_timeout_s)
        self._draining = False
        # S21 K-100：本部署的最大租约时长（秒）——墓碑窗口的量纲依据。
        # 必须**向上取整**且覆盖 claim 租约：`lease_ms=1500` 的真实租约是 1.5s，
        # 若向下取整成 1s，墓碑会在旧持有者租约仍有效时被忘记 → fencing 被削弱。
        _max_ttl_ms = max(int(self.lease_ms), int(self.claim_lease_ms))
        self.state.max_lease_ttl_s = max(1, -(-_max_ttl_ms // 1000))
        self.state.claim_window_exceeded = 0
        # S22：门面 token 回收后的清理钩子（由 `RscSession` 注入，用于销毁 Z3）。
        self._token_cleanup = None
        self.ingress = Ingress(self)
        # S13d §2.5 P0-10：claim 的 fence epoch 接入**写路径**（commit_batch /
        # budget.consume / effects.begin / checkpoints.save）。sync 模式无 claim
        # → 守卫恒为 False（保持既有语义，36 个同步用例零破坏）。
        # S3.0：late lookup —— 注入 lambda 而非 bound method，保证「构造后替换
        # `h._claim_fenced` 实例方法仍生效」（tests 会打桩）。
        self.budget.claim_guard = lambda *a, **k: self._claim_fenced(*a, **k)
        self.effects.claim_guard = lambda *a, **k: self._claim_fenced(*a, **k)
        self.checkpoints.claim_guard = lambda *a, **k: self._claim_fenced(*a, **k)
        # X7：claim 三字段随快照落盘（无锁只读，仅作恢复后的观测/租约判定）。
        self.checkpoints.claim_provider = lambda *a, **k: self._claim_snapshot(*a, **k)

        self.runtime = AgentRuntime(self)
        # S3.3：执行 / 调度 / tick / claim fencing 簇。依赖此刻已全部就绪
        # （scheduler / run_coordinator / slots 在上；hitl / reconcile / tasks 紧随其上；
        # runtime 紧邻本行）。构造不追加任何事件。可变容器/配置一律在调用时读
        # `KernelState`；仍留在 Harness 上或会被测试打桩的方法一律走 **late lookup
        # lambda** —— 绑定方法会冻结原实现，让 tests 的实例级/类级打桩失效
        # （`test_s13_scheduler` 替换 `_run_one`/`_drain_scheduler`，
        # `test_s13_hitl` 替换 `tick` 与 `Harness._hitl_gate`）。
        self.dispatch = DispatchService(
            DispatchDeps(
                scheduler=self.scheduler, run_coordinator=self.run_coordinator,
                intents=self.intents, budget=self.budget,
                checkpoints=self.checkpoints, clock=self.clock,
                hitl=self.hitl, reconcile=self.reconcile, timers=self.timers,
                runtime=self.runtime, slots=self.slots, tasks=self.tasks,
                run_one=lambda *a, **k: self._run_one(*a, **k),
                tick=lambda *a, **k: self.tick(*a, **k),
                drain=lambda *a, **k: self._drain_scheduler(*a, **k),
                hitl_gate=lambda *a, **k: self._hitl_gate(*a, **k),
                terminal_status=lambda *a, **k: self._terminal_status(*a, **k),
                finalize=lambda *a, **k: self._finalize(*a, **k),
                safe_save=lambda *a, **k: self._safe_save(*a, **k),
                transition_lock=lambda *a, **k: self._transition_lock(*a, **k),
                clear_admission=lambda *a, **k: self._clear_admission(*a, **k),
                dead_letter_after_replan=lambda *a, **k:
                    self._dead_letter_after_replan(*a, **k),
                approval_slot=lambda *a, **k: self._approval_slot(*a, **k),
                record_approval=lambda *a, **k: self._record_approval(*a, **k),
                reap_expired_tokens=lambda *a, **k:
                    self._reap_expired_tokens(*a, **k),
                restore_acb=lambda *a, **k: self._restore_acb(*a, **k),
                is_paused=lambda *a, **k: self.is_paused(*a, **k),
                bump_epoch=lambda *a, **k: self.bump_epoch(*a, **k)),
            self.state)
        self.state.plans = {}
        self.state.acbs = {}
        self.state.lock = threading.RLock()
        # R6：准入唯一性登记 `(run_id, task_id, plan_version) → ACB`。
        # 同一 key 的活跃 ACB 不会被第二次 try_admit 复制出 `A-...-2`（双跑）。
        self.state.admission_index = {}
        # R7：每 ACB 一把 transition 锁（replan / _finalize / requeue_readmit 共用）
        self.state.transition_locks = {}
        # X6：`parent_id → {child_acb_id}` 反向索引（跨 run 子；父 `children_ids`
        # 缺失的老 checkpoint 也能被取消级联找回）。恢复时按 `spec.spawn.parent_id`
        # 反扫建立；spawn 成功与恢复子时增量更新。
        self.state.children_index = {}
        self.state.children_index_built = False
        # S9 §2.3：收据链（每 scope 一把 RLock；head/id 由 EventLog 重建）
        # 锁序写死：resource → scope → eventlog（docs s9 §2.3.5）。
        # **durable 模式为单写者**：`_scope_lock` 是**进程内** `threading.RLock`，
        # 跨进程**不互斥**；`WalEventLog.append_batch` 的 `_lock` 同理。跨进程
        # 多写者必须自行串行化（跨进程 scope 锁留 S10，本阶段不实现）。
        self.state.scope_locks = {}
        # S22：外部幂等键的进程内串行锁（同键并发 apply 不得双写）。
        self.state.ext_key_locks = {}
        self.state.receipt_heads = {}
        self.state.receipt_ids = set()
        self.state.tainted_scopes = set()
        self.state.malformed_receipts = []
        # 锁内增量刷新：水位（已扫到的最大 seq）+ 刷新互斥（内存模式 per-scope
        # 锁不互斥 → 需串行化，避免共享水位被并发推进而漏扫）。durable 模式
        # scope 锁即跨进程 flock，天然串行；此锁仍保证同进程多线程一致。
        self.state.receipt_scan_watermark = 0
        self.state.receipt_refresh_lock = threading.RLock()
        # S12 K-45：粘滞闩锁（回扫失败 → 后续提交一律被拒，直到全量重建）
        self.state.receipt_scan_failed = False
        self._rebuild_receipts()

        # ---- S10a §2.5：单写者租约（durable 模式在构造期即 acquire）----
        # 生产路径必须真正持有租约：此前 `_try_acquire_writer_lease` 只被测试调用，
        # 单写者形同虚设。acquire 失败 → `_writer_lease_ok=False`，写入口拒绝服务。
        # S11b / 终审 7-1：**取租约前先恢复 durable epoch**。否则干净重启
        # （lease 已释放但 WAL epoch 仍为 N）时本地 epoch=0 → 首写被 fence。
        # S24-8：`lease_store` 提供时，租约走**可复制后端**（Raft/内存），
        # 不再读写本地 `writer_lease.json`；未提供时既有路径字节不变。
        self.state.lease_store = lease_store
        if entropy is not None:
            self.state.lease_instance = entropy.token("writer_instance")[:16]
            self.state.writer_id = writer_id or f"writer-{entropy.uuid('writer')[:12]}"
        else:
            self.state.lease_instance = (f"{os.getpid()}-{uuid.uuid4().hex[:8]}"
                                         if lease_store is not None else "")
            self.state.writer_id = writer_id or f"harness-{os.getpid()}"
        self.state.writer_epoch = 0
        self.state.lease_outcome = ""
        self.state.lease_reason = ""
        self._restore_epoch()
        self.state.durable = (self._lease_path() is not None
                              or self.state.lease_store is not None)
        self.state.writer_lease_ok = True
        if self.state.durable:
            self._try_acquire_writer_lease()
            self._wire_lease_fence()
            on_close = getattr(self.event_log, "on_close", None)
            if callable(on_close):
                on_close(self._release_writer_lease)

        # S4：恢复 / 预算回放 / 能力恢复簇。依赖此刻已全部存在（budget /
        # checkpoints / clock / comp_journal / effects / event_log / policy /
        # slots / run_coordinator / workspace / receipts / children 均在上方）。
        # 构造**不追加任何事件**（`__init__` 只保存引用）。放在 `subscribe` 之前：
        # 本簇不需要订阅，且不移动任何既有语句（`subscribe` 仍是最后一条）。
        # 仍留在 Harness 上的方法一律走 **late lookup lambda** —— 绑定方法会冻结
        # 原实现，让 tests 的实例级/类级打桩失效（`test_s13_hitl` 打桩 `tick`）。
        self.recovery = RecoveryService(
            RecoveryDeps(
                budget=self.budget, checkpoints=self.checkpoints,
                clock=self.clock, comp_journal=self.comp_journal,
                effects=self.effects, event_log=self.event_log,
                policy=self.policy, run_coordinator=self.run_coordinator,
                slots=self.slots, workspace=self.workspace,
                receipts=self.receipts, children=self.children,
                ensure_writer_lease=lambda *a, **k:
                    self._ensure_writer_lease(*a, **k),
                restore_epoch=lambda *a, **k: self._restore_epoch(*a, **k),
                bump_epoch=lambda *a, **k: self.bump_epoch(*a, **k),
                tick=lambda *a, **k: self.tick(*a, **k),
                register_admission=lambda *a, **k:
                    self._register_admission(*a, **k),
                run_one_compensation=lambda *a, **k:
                    self._run_one_compensation(*a, **k),
                rebuild_pause_state=lambda: self._rebuild_pause_state(),
                token_from_payload=lambda *a, **k:
                    self._token_from_payload(*a, **k),
                register_token=lambda *a, **k: self._register_token(*a, **k),
                ensure_policy_token=lambda *a, **k:
                    self._ensure_policy_token(*a, **k),
                replay_budget_events=lambda *a, **k:
                    self._replay_budget_events(*a, **k),
                replay_events=lambda *a, **k: self._replay_events(*a, **k)),
            self.state)

        self.event_log.subscribe(self._on_event)

    # ---- 收据链（S9 §2.3 / K-24） --------------------------------
    # ---- 收据链（S1：实现已迁至 kernel/receipts.py）----------
    def _rebuild_receipts(self, *args, **kwargs):
        return self.receipts._rebuild_receipts(*args, **kwargs)
    @staticmethod
    def _scan_receipts(*args, **kwargs):
        return ReceiptChain._scan_receipts(*args, **kwargs)
    def _receipt_records(self, *args, **kwargs):
        return self.receipts._receipt_records(*args, **kwargs)
    @staticmethod
    def _rescan_wal_from_disk(*args, **kwargs):
        return ReceiptChain._rescan_wal_from_disk(*args, **kwargs)
    def _refresh_receipts_locked(self, *args, **kwargs):
        return self.receipts.refresh_locked(*args, **kwargs)
    @staticmethod
    def _payload_scope(*args, **kwargs):
        return ReceiptChain._payload_scope(*args, **kwargs)
    def _scope_lock(self, *args, **kwargs):
        return self.receipts.scope_lock(*args, **kwargs)
    def _sync_run_cursors(self, *args, **kwargs):
        return self.receipts._sync_run_cursors(*args, **kwargs)
    def _track_run_cursor(self, *args, **kwargs):
        return self.receipts.track_run_cursor(*args, **kwargs)
    def _run_tail(self, *args, **kwargs):
        return self.receipts._run_tail(*args, **kwargs)
    def floor_for(self, *args, **kwargs):
        return self.receipts.floor_for(*args, **kwargs)
    def _receipt_secret(self, *args, **kwargs):
        return self.receipts.receipt_secret(*args, **kwargs)
    @staticmethod
    def _commit_key(*args, **kwargs):
        return ReceiptChain.commit_key(*args, **kwargs)
    def _dedup_event(self, *args, **kwargs):
        return self.receipts.dedup_event(*args, **kwargs)
    def _ext_key_lock(self, *args, **kwargs):
        return self.receipts.ext_key_lock(*args, **kwargs)
    def _dedup_by_external_key(self, *args, **kwargs):
        return self.receipts.dedup_by_external_key(*args, **kwargs)
    def _replay_dedup_events(self, *args, **kwargs):
        return self.receipts.replay_dedup_events(*args, **kwargs)
    def _refresh_after_commit(self, *args, **kwargs):
        return self.receipts.refresh_after_commit(*args, **kwargs)
    def _receipt_event(self, *args, **kwargs):
        return self.receipts.receipt_event(*args, **kwargs)
    def _receipt_is_stale(self, *args, **kwargs):
        return self.receipts.receipt_is_stale(*args, **kwargs)
    def _derive_receipt_id(self, *args, **kwargs):
        return self.receipts.derive_receipt_id(*args, **kwargs)
    def receipts_of(self, *args, **kwargs):
        return self.receipts.receipts_of(*args, **kwargs)
    def verify_receipts(self, *args, **kwargs):
        return self.receipts.verify_receipts(*args, **kwargs)
    def _commit_event_for_receipt(self, *args, **kwargs):
        return self.receipts._commit_event_for_receipt(*args, **kwargs)
    def reconcile_scope(self, *args, **kwargs):
        return self.receipts.reconcile_scope(*args, **kwargs)







    # ---- K-33：per-run retention（每 run 独立高水位）----------------




    # ---- 写者租约 / epoch（S1：实现已迁至 kernel/leases.py）----
    def _wire_wal_fencing(self, *args, **kwargs):
        return self.leases._wire_wal_fencing(*args, **kwargs)
    def _wire_lease_fence(self, *args, **kwargs):
        return self.leases._wire_lease_fence(*args, **kwargs)
    def _apply_lease_grant(self, *args, **kwargs):
        return self.leases._apply_lease_grant(*args, **kwargs)
    def _lease_failure_reason(self, *args, **kwargs):
        return self.leases.failure_reason(*args, **kwargs)
    def _lease_path(self, *args, **kwargs):
        return self.leases._lease_path(*args, **kwargs)
    def _read_lease(self, *args, **kwargs):
        return self.leases._read_lease(*args, **kwargs)
    def _store_flock(self, *args, **kwargs):
        return self.leases._store_flock(*args, **kwargs)
    def _try_acquire_writer_lease(self, *args, **kwargs):
        return self.leases._try_acquire_writer_lease(*args, **kwargs)
    def _renew_writer_lease(self, *args, **kwargs):
        return self.leases._renew_writer_lease(*args, **kwargs)
    def _ensure_writer_lease(self, *args, **kwargs):
        return self.leases.ensure_writer_lease(*args, **kwargs)
    def _release_writer_lease(self, *args, **kwargs):
        return self.leases._release_writer_lease(*args, **kwargs)
    def _epoch_backend(self, *args, **kwargs):
        return self.leases._epoch_backend(*args, **kwargs)
    def _restore_epoch(self, *args, **kwargs):
        return self.leases._restore_epoch(*args, **kwargs)
    def bump_epoch(self, reason: str = "", run_id: str | None = None, *,
                   epoch_scope: str | None = None) -> int:
        return self.leases.bump_epoch(reason, run_id, epoch_scope=epoch_scope)














    # ---- 收据冲突对账（P2-C） --------------------------------------


    # ---- S1 兼容属性（S5 删除）：收据链状态已迁至 KernelState ----
    @property
    def _lock(self):
        return self.state.lock

    @_lock.setter
    def _lock(self, value) -> None:
        self.state.lock = value

    @property
    def _run_cursors(self):
        return self.state.run_cursors

    @_run_cursors.setter
    def _run_cursors(self, value) -> None:
        self.state.run_cursors = value

    @property
    def _receipts_flock(self):
        return self.state.receipts_flock

    @_receipts_flock.setter
    def _receipts_flock(self, value) -> None:
        self.state.receipts_flock = value

    @property
    def _scope_locks(self):
        return self.state.scope_locks

    @_scope_locks.setter
    def _scope_locks(self, value) -> None:
        self.state.scope_locks = value

    @property
    def _ext_key_locks(self):
        return self.state.ext_key_locks

    @_ext_key_locks.setter
    def _ext_key_locks(self, value) -> None:
        self.state.ext_key_locks = value

    @property
    def _receipt_heads(self):
        return self.state.receipt_heads

    @_receipt_heads.setter
    def _receipt_heads(self, value) -> None:
        self.state.receipt_heads = value

    @property
    def _receipt_ids(self):
        return self.state.receipt_ids

    @_receipt_ids.setter
    def _receipt_ids(self, value) -> None:
        self.state.receipt_ids = value

    @property
    def _tainted_scopes(self):
        return self.state.tainted_scopes

    @_tainted_scopes.setter
    def _tainted_scopes(self, value) -> None:
        self.state.tainted_scopes = value

    @property
    def _malformed_receipts(self):
        return self.state.malformed_receipts

    @_malformed_receipts.setter
    def _malformed_receipts(self, value) -> None:
        self.state.malformed_receipts = value

    @property
    def _receipt_scan_watermark(self) -> int:
        return self.state.receipt_scan_watermark

    @_receipt_scan_watermark.setter
    def _receipt_scan_watermark(self, value: int) -> None:
        self.state.receipt_scan_watermark = value

    @property
    def _receipt_refresh_lock(self):
        return self.state.receipt_refresh_lock

    @_receipt_refresh_lock.setter
    def _receipt_refresh_lock(self, value) -> None:
        self.state.receipt_refresh_lock = value

    @property
    def _receipt_scan_failed(self) -> bool:
        return self.state.receipt_scan_failed

    @_receipt_scan_failed.setter
    def _receipt_scan_failed(self, value: bool) -> None:
        self.state.receipt_scan_failed = value

    @property
    def receipts_enabled(self) -> bool:
        return self.state.receipts_enabled

    @receipts_enabled.setter
    def receipts_enabled(self, value: bool) -> None:
        self.state.receipts_enabled = bool(value)

    @property
    def lease_ms(self) -> int:
        return self.state.lease_ms

    @lease_ms.setter
    def lease_ms(self, value: int) -> None:
        self.state.lease_ms = int(value)

    @property
    def comp_journal(self):
        return self.state.comp_journal

    @comp_journal.setter
    def comp_journal(self, value) -> None:
        self.state.comp_journal = value

    @property
    def lock_timeout_s(self) -> float:
        return self.state.lock_timeout_s

    @lock_timeout_s.setter
    def lock_timeout_s(self, value: float) -> None:
        self.state.lock_timeout_s = float(value)

    @property
    def compensation_journal(self):
        """S2：补偿日志已随 CommitService 迁移（旧的可观测视图）。"""
        return self.commit.compensation_journal

    @compensation_journal.setter
    def compensation_journal(self, value) -> None:
        self.commit.compensation_journal = value

    # ---- S1 兼容属性（S5 删除）：写者租约状态已迁至 KernelState ----
    @property
    def _writer_lease_ok(self) -> bool:
        return self.state.writer_lease_ok

    @_writer_lease_ok.setter
    def _writer_lease_ok(self, value: bool) -> None:
        self.state.writer_lease_ok = value


    @property
    def _lease_store(self):
        return self.state.lease_store

    @_lease_store.setter
    def _lease_store(self, value) -> None:
        self.state.lease_store = value

    @property
    def _lease_instance(self):
        return self.state.lease_instance

    @_lease_instance.setter
    def _lease_instance(self, value) -> None:
        self.state.lease_instance = value

    @property
    def _writer_id(self):
        return self.state.writer_id

    @_writer_id.setter
    def _writer_id(self, value) -> None:
        self.state.writer_id = value

    @property
    def _writer_epoch(self) -> int:
        return self.state.writer_epoch

    @_writer_epoch.setter
    def _writer_epoch(self, value: int) -> None:
        self.state.writer_epoch = value

    @property
    def _lease_outcome(self):
        return self.state.lease_outcome

    @_lease_outcome.setter
    def _lease_outcome(self, value) -> None:
        self.state.lease_outcome = value

    @property
    def _lease_reason(self):
        return self.state.lease_reason

    @_lease_reason.setter
    def _lease_reason(self, value) -> None:
        self.state.lease_reason = value

    @property
    def _durable(self) -> bool:
        return self.state.durable

    @_durable.setter
    def _durable(self, value: bool) -> None:
        self.state.durable = value

    # ---- 事件 → 订阅失效（文档 §5.6） ----------------------------
    # S1：实现已迁至 `kernel/identity.py::SigningService`（此处仅委托）
    def _on_event(self, ev) -> None:
        self.identity.on_event(ev)

    # ---- 签名 / token ---------------------------------------------
    def _get_signer(self) -> Signer:
        return self.identity.get_signer()

    @property
    def auth(self) -> Signer:
        return self.identity.auth

    def _effective_token(self, acb: ACB):
        return self.identity.effective_token(acb)

    # ---- 幂等键（文档 §5.8 / I7） --------------------------------
    def idem_key(self, acb: ACB, action, action_index: int) -> str:
        return self.identity.idem_key(acb, action, action_index)

    def track_run_cursor(self, ev) -> None:
        """`SigningService` 的游标回调（过渡期直连 Harness；S1 完成后改指 ReceiptChain）。"""
        self._track_run_cursor(ev)

    # ---- 兼容属性（S5 删除）--------------------------------------
    @property
    def _auth(self):
        return self.state.auth

    @_auth.setter
    def _auth(self, value) -> None:
        self.state.auth = value

    @property
    def _auth_secret(self):
        return self.state.auth_secret

    @_auth_secret.setter
    def _auth_secret(self, value) -> None:
        self.state.auth_secret = value

    @property
    def _event_count(self) -> int:
        return self.state.event_count

    @_event_count.setter
    def _event_count(self, value: int) -> None:
        self.state.event_count = value

    # ---- S3.0 兼容属性（S5 删除）：调度/子任务状态已迁至 KernelState ----
    @property
    def plans(self):
        """`run_id → plan`（旧 `self.plans: dict[str, dict]`）。"""
        return self.state.plans

    @plans.setter
    def plans(self, value) -> None:
        self.state.plans = value

    @property
    def acbs(self):
        """`acb_id → ACB` 活跃任务表（旧 `self.acbs: dict[str, ACB]`）。"""
        return self.state.acbs

    @acbs.setter
    def acbs(self, value) -> None:
        self.state.acbs = value

    @property
    def _paused_runs(self):
        """S15 D：Admin 暂停的 run 集合（旧 `self._paused_runs: set[str]`）。"""
        return self.state.paused_runs

    @_paused_runs.setter
    def _paused_runs(self, value) -> None:
        self.state.paused_runs = value

    @property
    def _drain_skipped(self):
        """D11：调度跳过原因（旧 `self._drain_skipped: list[tuple[str, str]]`）。"""
        return self.state.drain_skipped

    @_drain_skipped.setter
    def _drain_skipped(self, value) -> None:
        self.state.drain_skipped = value

    @property
    def _tick_skipped(self):
        """S14 K-69：tick 跳过原因（旧 `self._tick_skipped: list[tuple[str, str]]`）。"""
        return self.state.tick_skipped

    @_tick_skipped.setter
    def _tick_skipped(self, value) -> None:
        self.state.tick_skipped = value

    @property
    def _applied_event_ids(self):
        """S4：恢复去重集合（旧 `self._applied_event_ids: set[str]`）。"""
        return self.state.applied_event_ids

    @_applied_event_ids.setter
    def _applied_event_ids(self, value) -> None:
        self.state.applied_event_ids = value

    @property
    def _admission_index(self):
        """R6：准入唯一性登记（旧 `self._admission_index: dict[tuple, ACB]`）。"""
        return self.state.admission_index

    @_admission_index.setter
    def _admission_index(self, value) -> None:
        self.state.admission_index = value

    @property
    def _transition_locks(self):
        """R7：每 ACB 一把 transition 锁（旧 `dict[str, threading.RLock]`）。"""
        return self.state.transition_locks

    @_transition_locks.setter
    def _transition_locks(self, value) -> None:
        self.state.transition_locks = value

    @property
    def _children_index(self):
        """X6：`parent_id → {child_acb_id}`（旧 `dict[str, set[str]]`）。"""
        return self.state.children_index

    @_children_index.setter
    def _children_index(self, value) -> None:
        self.state.children_index = value

    @property
    def _children_index_built(self) -> bool:
        return self.state.children_index_built

    @_children_index_built.setter
    def _children_index_built(self, value: bool) -> None:
        self.state.children_index_built = value

    @property
    def _spawn_counter(self):
        """S3.1：spawn 子 id 序号源（已迁至 KernelState，随 ChildManager 移交）。"""
        return self.state.spawn_counter

    @_spawn_counter.setter
    def _spawn_counter(self, value) -> None:
        self.state.spawn_counter = value

    # ---- S3.3 兼容属性（S5 删除）：执行/调度簇的可变状态已迁至 KernelState ----
    # `_tls` 是**线程本地**对象（`threading.local` 本体不会变，只有其上的字段变），
    # 迁入 state 后 DispatchService 与 Harness 读写的是同一个实例。
    @property
    def _tls(self):
        return self.state.tls

    @_tls.setter
    def _tls(self, value) -> None:
        self.state.tls = value

    @property
    def _pool(self):
        """S3.3：`ThreadPoolExecutor` 仍在 `__init__` 建一次，服务只读不重建。"""
        return self.state.pool

    @_pool.setter
    def _pool(self, value) -> None:
        self.state.pool = value

    @property
    def _worker_ids(self):
        return self.state.worker_ids

    @_worker_ids.setter
    def _worker_ids(self, value) -> None:
        self.state.worker_ids = value

    @property
    def _timers_fired(self) -> list:
        return self.state.timers_fired

    @_timers_fired.setter
    def _timers_fired(self, value) -> None:
        self.state.timers_fired = value

    @property
    def _draining(self) -> bool:
        return self.state.draining

    @_draining.setter
    def _draining(self, value) -> None:
        self.state.draining = value

    # ---- S3.0 兼容属性（S5 删除）：可变配置已迁至 KernelState ----
    # 一律透传赋值（不做二次强转）：`__init__` 已完成各自类型转换，行为不变。
    @property
    def scheduler_mode(self) -> str:
        return self.state.scheduler_mode

    @scheduler_mode.setter
    def scheduler_mode(self, value) -> None:
        self.state.scheduler_mode = value

    @property
    def aging_ms(self) -> int:
        return self.state.aging_ms

    @aging_ms.setter
    def aging_ms(self, value) -> None:
        self.state.aging_ms = value

    @property
    def aging_max_attempts(self) -> int:
        return self.state.aging_max_attempts

    @aging_max_attempts.setter
    def aging_max_attempts(self, value) -> None:
        self.state.aging_max_attempts = value

    @property
    def claim_lease_ms(self) -> int:
        return self.state.claim_lease_ms

    @claim_lease_ms.setter
    def claim_lease_ms(self, value) -> None:
        self.state.claim_lease_ms = value

    @property
    def pool_workers(self) -> int:
        return self.state.pool_workers

    @pool_workers.setter
    def pool_workers(self, value) -> None:
        self.state.pool_workers = value

    @property
    def pool_timeout_s(self) -> float:
        return self.state.pool_timeout_s

    @pool_timeout_s.setter
    def pool_timeout_s(self, value) -> None:
        self.state.pool_timeout_s = value

    @property
    def drain_timeout_s(self) -> float:
        return self.state.drain_timeout_s

    @drain_timeout_s.setter
    def drain_timeout_s(self, value) -> None:
        self.state.drain_timeout_s = value

    @property
    def hitl_timeout_ms(self) -> int:
        return self.state.hitl_timeout_ms

    @hitl_timeout_ms.setter
    def hitl_timeout_ms(self, value) -> None:
        self.state.hitl_timeout_ms = value

    @property
    def max_redrives(self) -> int:
        return self.state.max_redrives

    @max_redrives.setter
    def max_redrives(self, value) -> None:
        self.state.max_redrives = value

    @property
    def poison_after_failures(self) -> int:
        return self.state.poison_after_failures

    @poison_after_failures.setter
    def poison_after_failures(self, value) -> None:
        self.state.poison_after_failures = value

    @property
    def max_fanout(self) -> int:
        return self.state.max_fanout

    @max_fanout.setter
    def max_fanout(self, value) -> None:
        self.state.max_fanout = value

    @property
    def min_budget_per_child(self) -> int:
        return self.state.min_budget_per_child

    @min_budget_per_child.setter
    def min_budget_per_child(self, value) -> None:
        self.state.min_budget_per_child = value

    @property
    def spawn_depth(self) -> int:
        return self.state.spawn_depth

    @spawn_depth.setter
    def spawn_depth(self, value) -> None:
        self.state.spawn_depth = value

    @property
    def replan_budget(self) -> int:
        return self.state.replan_budget

    @replan_budget.setter
    def replan_budget(self, value) -> None:
        self.state.replan_budget = value

    @property
    def retention(self) -> int:
        return self.state.retention

    @retention.setter
    def retention(self, value) -> None:
        self.state.retention = value

    @property
    def max_lease_ttl_s(self) -> int:
        return self.state.max_lease_ttl_s

    @max_lease_ttl_s.setter
    def max_lease_ttl_s(self, value) -> None:
        self.state.max_lease_ttl_s = value

    @property
    def _claim_window_exceeded(self) -> int:
        return self.state.claim_window_exceeded

    @_claim_window_exceeded.setter
    def _claim_window_exceeded(self, value) -> None:
        self.state.claim_window_exceeded = value

    # ---- RSC 提交（文档 §5.5） ------------------------------------
    # ---- RSC 提交（S2：实现已迁至 kernel/commit.py）--------------------
    def commit_batch(self, acb: ACB, writes: list[Write],
                     mode: Mode = Mode.COMMIT, *,
                     idempotency_key: str | None = None) -> Result:
        return self.commit.commit_batch(
            acb, writes, mode, idempotency_key=idempotency_key)
    def _commit_record(self, *args, **kwargs):
        return self.commit._commit_record(*args, **kwargs)
    def _last_committed_seq(self, *args, **kwargs):
        return self.commit._last_committed_seq(*args, **kwargs)
    def _last_receipt_seq(self, *args, **kwargs):
        return self.commit._last_receipt_seq(*args, **kwargs)
    def compensate(self, *args, **kwargs):
        return self.commit.compensate(*args, **kwargs)
    def _run_one_compensation(self, *args, **kwargs):
        return self.commit._run_one_compensation(*args, **kwargs)
    def _mark_write(self, *args, **kwargs):
        return self.commit._mark_write(*args, **kwargs)
    def _compensate_one(self, *args, **kwargs):
        return self.commit._compensate_one(*args, **kwargs)








    # ---- 重规划（文档 §5.2④ / §8.1） ------------------------------
    def propagate_failure(self, *args, **kwargs):
        return self.tasks.propagate_failure(*args, **kwargs)

    # ---- Supervisor 辅助 ------------------------------------------
    def supervisor_note_crash(self, *args, **kwargs):
        return self.dispatch.supervisor_note_crash(*args, **kwargs)

    # ---- 单写者租约 + fencing epoch（S10a §2.5 / §3.4 / S24-8）----------

    # ---- S24-11：定义版本化 + DurableTimer 便捷入口 ----------------
    def check_definition(self, ref) -> str:
        """校验 ACB/plan 钉住的定义引用；空串 = 通过（fail-closed）。"""
        if not ref:
            return ""
        if self.definitions is None:
            return "DEFINITION_REGISTRY_MISSING"
        try:
            return str(self.definitions.check(ref))
        except Exception:  # noqa: BLE001  # 非法 ref → 拒绝
            return "DEFINITION_MALFORMED"

    def set_timer(self, timer_id: str, fire_at_ms: int, **kwargs):
        if self.timers is None:
            raise RuntimeError("未启用 timers（Harness(timers=...) / durable_timers=True）")
        return self.timers.schedule(timer_id, fire_at_ms, **kwargs)

    def cancel_timer(self, timer_id: str, *, reason: str = "") -> bool:
        if self.timers is None:
            raise RuntimeError("未启用 timers")
        return self.timers.cancel(timer_id, reason=reason)












    # ---- 端到端运行 -----------------------------------------------
    def _admit_and_register(self, *args, **kwargs):
        return self.tasks.admit_and_register(*args, **kwargs)

    def dispatch_and_run(self, *args, **kwargs):
        return self.tasks.dispatch_and_run(*args, **kwargs)

    def finish(self, *args, **kwargs):
        return self.tasks.finish(*args, **kwargs)

    def abort(self, *args, **kwargs):
        return self.tasks.abort(*args, **kwargs)

    def run_task(self, *args, **kwargs):
        return self.dispatch.run_task(*args, **kwargs)

    # ---- S13d §2.5：pool 调度（单一 pop 所有者 + claim/fencing）--------
    def _in_worker(self, *args, **kwargs):
        return self.dispatch._in_worker(*args, **kwargs)

    def _current_worker_id(self, *args, **kwargs):
        return self.dispatch._current_worker_id(*args, **kwargs)

    def _run_one_inline(self, *args, **kwargs):
        return self.dispatch._run_one_inline(*args, **kwargs)

    def _run_task_pooled(self, *args, **kwargs):
        return self.dispatch._run_task_pooled(*args, **kwargs)

    def _worker_main(self, *args, **kwargs):
        return self.dispatch._worker_main(*args, **kwargs)

    def _await_completion(self, *args, **kwargs):
        return self.dispatch._await_completion(*args, **kwargs)

    # ---- S13c §2.4：spawn_child（S3.1：实现已迁至 kernel/children.py）----
    # 旧名委托保留到 S5；`self.children` 是唯一实现（不再有第二份逻辑）。
    def _spawn_depth_of(self, *args, **kwargs):
        return self.children._spawn_depth_of(*args, **kwargs)

    def _spawn_reject(self, *args, **kwargs):
        return self.children._spawn_reject(*args, **kwargs)

    def _spawn_rollback(self, *args, **kwargs):
        return self.children._spawn_rollback(*args, **kwargs)

    def _save_strict(self, *args, **kwargs):
        return self.children._save_strict(*args, **kwargs)

    def _parent_child_alloc(self, *args, **kwargs):
        return self.children._parent_child_alloc(*args, **kwargs)

    def _child_ids_for_cancel(self, *args, **kwargs):
        return self.children._child_ids_for_cancel(*args, **kwargs)

    def _ref_for_child(self, *args, **kwargs):
        return self.children.ref_for_child(*args, **kwargs)

    def _settle_orphan_child(self, *args, **kwargs):
        return self.children.settle_orphan_child(*args, **kwargs)

    def _reconcile_children_refs(self, *args, **kwargs):
        return self.children._reconcile_children_refs(*args, **kwargs)

    def _reconcile_children_pending(self, *args, **kwargs):
        return self.children._reconcile_children_pending(*args, **kwargs)

    def spawn_child(self, *args, **kwargs):
        return self.children.spawn_child(*args, **kwargs)

    def _live_child_ids(self, *args, **kwargs):
        return self.children._live_child_ids(*args, **kwargs)

    def _load_persisted_child(self, *args, **kwargs):
        return self.children._load_persisted_child(*args, **kwargs)

    def result_envelope(self, *args, **kwargs):
        return self.children.result_envelope(*args, **kwargs)

    def apply_result_envelope(self, *args, **kwargs):
        return self.children.apply_result_envelope(*args, **kwargs)

    def _approval_slot(self, *args, **kwargs):
        return self.tasks._approval_slot(*args, **kwargs)

    def _record_approval(self, *args, **kwargs):
        return self.tasks.record_approval(*args, **kwargs)

    def _hitl_gate(self, *args, **kwargs):
        return self.tasks._hitl_gate(*args, **kwargs)

    def _terminal_status(self, *args, **kwargs):
        return self.tasks.terminal_status(*args, **kwargs)

    def tick(self, *args, **kwargs):
        return self.dispatch.tick(*args, **kwargs)

    # ---- S14 K-69：tick 的持久候选集 + 外部驱动器 ---------------------
    def _tick_candidates(self, *args, **kwargs):
        return self.dispatch._tick_candidates(*args, **kwargs)

    def _pending_deadlines(self, *args, **kwargs):
        return self.dispatch._pending_deadlines(*args, **kwargs)

    def next_deadline_ms(self, *args, **kwargs):
        return self.dispatch.next_deadline_ms(*args, **kwargs)

    def pump(self, *args, **kwargs):
        return self.dispatch.pump(*args, **kwargs)

    # ---- S15 D3：Admin pause / resume（持久化、不丢状态）-------------
    def is_paused(self, run_id: str) -> bool:
        """该 run 是否处于 Admin 暂停（只阻止**未开始**的任务被调度）。"""
        return bool(run_id) and run_id in self._paused_runs

    def pause_run(self, run_id: str, *, reason: str = "") -> bool:
        """暂停一个 run：保留槽位/预算/节点（可恢复），只停止新调度。

        与 `cancel_run` 的区别：cancel **释放资源且不可恢复**；pause 不释放。
        在途任务**不打断**（避免半提交），让它跑完。
        返回 `True` = 本次真的从「运行」变为「暂停」；已暂停返回 `False`。
        """
        run_id = str(run_id or "")
        if not run_id or run_id in self._paused_runs:
            return False
        self._paused_runs.add(run_id)
        self.event_log.append(EventKind.RUN_PAUSED, run_id,
                              {"reason": str(reason or "")}, run_id=run_id)
        return True

    def resume_run(self, run_id: str) -> bool:
        """恢复一个被暂停的 run（`True` = 本次真的从「暂停」变为「运行」）。"""
        run_id = str(run_id or "")
        if run_id not in self._paused_runs:
            return False
        self._paused_runs.discard(run_id)
        self.event_log.append(EventKind.RUN_RESUMED, run_id, {}, run_id=run_id)
        return True

    def paused_runs(self) -> list[str]:
        return sorted(self._paused_runs)

    def _rebuild_pause_state(self) -> None:
        """从事件流重建暂停集合（**持久真相**，重启后仍暂停）。

        按 seq 顺序应用 `RUN_PAUSED` / `RUN_RESUMED`，后者覆盖前者。
        """
        paused: set[str] = set()
        for ev in self.event_log.all():
            if ev.kind is EventKind.RUN_PAUSED:
                paused.add(str(ev.subject))
            elif ev.kind is EventKind.RUN_RESUMED:
                paused.discard(str(ev.subject))
        self._paused_runs = paused

    # ---- S15 D2：DLQ redrive（终态 + 显式重放 + 有界）----------------
    def redrive_dead_letter(self, *args, **kwargs):
        return self.tasks.redrive_dead_letter(*args, **kwargs)

    # ---- S15 D4：可观测快照（零依赖，OTel 友好结构）------------------
    def metrics(self) -> dict:
        """只读快照：`{"counters": {...}, "gauges": {...}}`。

        从 EventLog 与内存状态派生，**不引入 OTel/Prometheus 依赖**
        （`dependencies = []` 是硬约束）；使用者可自行映射到自己的指标后端。
        """
        counters: dict[str, int] = {}
        for ev in self.event_log.all():
            name = ev.kind.value if hasattr(ev.kind, "value") else str(ev.kind)
            counters[name] = counters.get(name, 0) + 1
        gauges = {
            "acbs_total": len(self.acbs),
            "acbs_nonterminal": sum(
                1 for a in self.acbs.values() if a.state not in TASK_TERMINAL),
            "paused_runs": len(self._paused_runs),
            "dead_letter": sum(
                1 for a in self.acbs.values()
                if a.state is TaskState.DEAD_LETTER),
            "waiting_approval": sum(
                1 for a in self.acbs.values()
                if a.state is TaskState.WAITING_APPROVAL),
            "budget_root_reserved": int(
                getattr(self.budget.root, "subtree_reserved", 0) or 0),
            "event_log_tail": self.event_log.tail(),
        }
        return {"counters": counters, "gauges": gauges}

    # ---- S17 MS10：优雅排空 / Admin kill·purge / 可观测 ----------------
    def drain(self, *args, **kwargs):
        return self.dispatch.drain(*args, **kwargs)

    def kill_run(self, *args, **kwargs):
        return self.tasks.kill_run(*args, **kwargs)

    def purge_run(self, run_id: str) -> bool:
        """把**已全终态**的 run 移出内存（`acbs`/`plans`）。

        **不删除历史事件**（保留审计与收据链），只写 `RUN_PURGED` 事件。
        存在未终态任务 → 返回 `False`（要求先 `kill_run`/等完成）。
        """
        acbs = [a for a in self.acbs.values() if a.run_id == run_id]
        if any(a.state not in TASK_TERMINAL for a in acbs):
            return False
        for acb in acbs:
            self.acbs.pop(acb.id, None)
            self.run_coordinator.unregister(acb)
        self.plans.pop(run_id, None)
        self._paused_runs.discard(run_id)
        self.event_log.append(EventKind.RUN_PURGED, run_id, {}, run_id=run_id)
        return True

    def _reap_expired_tokens(self, *args, **kwargs):
        return self.tasks._reap_expired_tokens(*args, **kwargs)

    def claim_window_s(self, *args, **kwargs):
        return self.dispatch.claim_window_s(*args, **kwargs)

    def prune_budget(self, *, max_age_s: float | None = None,
                     now_ms: int | None = None) -> int:
        """S19 K-88：剪掉「已终结 + 无人引用 + 超过保留期」的预算预留条目。

        `live_rids` 由**活 ACB**（非终态）的 `reservation_ids` 精确算出 ——
        这是比「按序号水位」更强的引用栅栏（单机 rid 是 uuid，没有单调 seq）。
        """
        now_s = (int(self.clock.now_ms()) if now_ms is None else int(now_ms)) / 1000.0
        live: set[str] = set()
        live_ids: set[str] = set()
        for acb in self.acbs.values():
            if acb.state in TASK_TERMINAL:
                continue
            live_ids.add(acb.id)
            live.update(acb.reservation_ids or ())
        # S21 K-100：墓碑的**年龄窗口**随命令下发（`max_lease_ttl_s`）——由
        # Raft 副本按「入墓已满该时长」判定能否忘记，避免复活仍活着的旧 worker。
        # 单机后端接收后忽略。拒绝淘汰的次数回读到 `_claim_window_exceeded`。
        removed = self.budget.prune(now_s, live, max_age_s=max_age_s,
                                    live_acb_ids=live_ids,
                                    max_lease_ttl_s=float(self.max_lease_ttl_s))
        self._claim_window_exceeded = int(
            getattr(self.budget, "claim_window_exceeded", 0) or 0)
        return removed

    # ---- S20 阶段三：多租户（**配额隔离**，非安全隔离）----------------
    def ensure_tenant(self, tenant: str, quota: int, *,
                      parent: str = "run", min_share: float = 0.0):
        """确保存在 `tenant:<t>` 预算节点（幂等）。挂在它下面的节点共享受配额约束。

        `min_share>0` 时再建 `tenant:<t>:reserved`（优先级份额池）。

        **失败零残留**：份额/配额先校验再建节点；配置冲突（同名节点已存在但
        quota/parent/share 不同）**显式抛错**，绝不静默返回旧节点。

        **诚实边界**：这只有**配额**隔离；同进程仍共享内存（安全隔离靠 egress/沙箱）。
        `plan["tenant"]` 已由 K-97 自动接到准入（见 `Dispatcher._resolve_parent_budget`）。
        """
        tid = str(tenant or "")
        if not tid:
            return None
        if ":" in tid:
            # `tenant:<t>` 的解析靠 `:` 分隔；租户名含 `:` 会让归属/计量错位。
            raise ValueError(f"非法 tenant 名：{tid!r}（不得含 ':'）")
        # 显式拒绝小数：`int()` 静默截断会让「6.9 变成 6」看起来成功。
        if int(quota) != quota:
            raise ValueError(f"quota={quota!r} 必须是整数")
        if min_share is not None and int(min_share) != min_share:
            raise ValueError(f"min_share={min_share!r} 必须是整数")
        quota = int(quota)
        share = int(min_share or 0)
        # ① 先校验（在建任何节点之前）—— 非法份额不得留下半成品。
        if quota <= 0:
            raise ValueError(f"quota={quota} 必须 > 0")
        if share < 0 or share > quota:
            raise ValueError(
                f"min_share={share} 必须满足 0 <= share <= quota={quota}")
        if parent != "run" and self.budget._nodes.get(parent) is None:
            raise ValueError(f"父预算节点不存在：{parent!r}")
        # ② 建租户节点；同名但配置不同 → create_node 返回 None → 显式拒绝。
        node = self.budget.create_node(f"tenant:{tid}", quota, parent, tenant=tid)
        if node is None:
            raise ValueError(
                f"tenant:{tid} 已存在但 quota/parent 不一致（不会静默沿用旧配置）")
        if share > 0:
            # 预留子节点：已知档位（premium/standard/developer）挂它，
            # 其他/未知档位回落共享池（超额借用）。
            rnode = self.budget.create_node(f"tenant:{tid}:reserved", share,
                                            f"tenant:{tid}", tenant=tid)
            if rnode is None:
                raise ValueError(
                    f"tenant:{tid}:reserved 已存在但 min_share 不一致（不会静默沿用旧配置）")
        return node

    # ---- S20 阶段三：计量（**从事件派生**，天然幂等）------------------
    def usage_report(self, *, tenant: str | None = None) -> dict:
        """按租户归集的用量（从 EventLog 重算，**幂等**）。

        为什么不用原子计数器：AWS Builders' Library 指出原子计数器**不幂等**
        （重试会重复计数）；EventLog 是唯一提交点（I1），从它重算天然幂等。

        归集口径（**明确写死，避免歧义**）：
        - `actions`：`RESOURCE_COMMITTED` 事件数（全局）
        - `runs`：出现过的 `run_id` 去重数
        - `by_tenant[t]`：**预算预留事件数**（`BUDGET_RESERVED`），按事件的
          `subject`（= 预算节点 id）映射到该节点上的 `tenant` 标签归集
        - `tenant=` 给定时只返回该租户的切片
        """
        node_tenant: dict[str, str] = {}
        for nid, node in (getattr(self.budget, "_nodes", {}) or {}).items():
            t = getattr(node, "tenant", "") or ""
            if t:
                node_tenant[nid] = t

        actions = 0
        runs: set[str] = set()
        by_tenant: dict[str, int] = {}
        for ev in self.event_log.all():
            kind = ev.kind.value if hasattr(ev.kind, "value") else str(ev.kind)
            if ev.run_id:
                runs.add(ev.run_id)
            if kind == "RESOURCE_COMMITTED":
                actions += 1
            elif kind == "BUDGET_RESERVED":
                t = node_tenant.get(str(ev.subject), "")
                if t:
                    by_tenant[t] = by_tenant.get(t, 0) + 1

        if tenant is not None:
            t = str(tenant)
            return {"tenant": t, "reservations": int(by_tenant.get(t, 0)),
                    "runs": len(runs), "actions": actions}
        return {"actions": actions, "runs": len(runs), "by_tenant": by_tenant}

    def export_observability(self, sink=None) -> int:
        """把事件流 + metrics 推给 `SpanSink`（默认 `MemorySpanSink`）。

        **零依赖**：不 import OTel；需要真实 OTel 时传入
        `observability.make_otel_sink()`。返回推送的 span 条数。
        """
        from .observability import MemorySpanSink, export_metrics, export_spans

        target = sink if sink is not None else MemorySpanSink()
        n = export_spans(target, self.event_log.all())
        export_metrics(target, self.metrics())
        return n


    # ---- S13d §2.5：aging（墙钟 deadline）+ claim fencing --------------
    def _arm_aging(self, *args, **kwargs):
        return self.dispatch.arm_aging(*args, **kwargs)

    def _aging_timeout(self, *args, **kwargs):
        return self.dispatch.aging_timeout(*args, **kwargs)

    def readmit_after_aging(self, *args, **kwargs):
        return self.dispatch.readmit_after_aging(*args, **kwargs)

    def _exec_claim_epoch(self, *args, **kwargs):
        return self.dispatch._exec_claim_epoch(*args, **kwargs)

    def _bind_claim_epoch(self, *args, **kwargs):
        return self.dispatch._bind_claim_epoch(*args, **kwargs)

    def _unbind_claim_epoch(self, *args, **kwargs):
        return self.dispatch._unbind_claim_epoch(*args, **kwargs)

    def claim_scope(self, *args, **kwargs):
        return self.dispatch.claim_scope(*args, **kwargs)

    def _claim_fenced(self, *args, **kwargs):
        return self.dispatch.claim_fenced(*args, **kwargs)

    def _claim_snapshot(self, *args, **kwargs):
        return self.dispatch._claim_snapshot(*args, **kwargs)

    # ---- X6：跨 run 子索引（S3.1：实现已迁至 kernel/children.py）----
    def _index_child(self, *args, **kwargs):
        return self.children._index_child(*args, **kwargs)

    def _ensure_children_index(self, *args, **kwargs):
        return self.children.ensure_children_index(*args, **kwargs)

    def _run_one(self, *args, **kwargs):
        return self.dispatch._run_one(*args, **kwargs)

    def _drain_scheduler(self, *args, **kwargs):
        return self.dispatch._drain_scheduler(*args, **kwargs)

    def _drain_scheduler_claimed(self, *args, **kwargs):
        return self.dispatch._drain_scheduler_claimed(*args, **kwargs)

    def _drain_skip_reason(self, *args, **kwargs):
        return self.dispatch._drain_skip_reason(*args, **kwargs)

    def _is_current_plan_version(self, *args, **kwargs):
        return self.dispatch._is_current_plan_version(*args, **kwargs)

    def _resolve_current_acb(self, *args, **kwargs):
        return self.tasks._resolve_current_acb(*args, **kwargs)

    def _admit_ready_successor(self, *args, **kwargs):
        return self.tasks._admit_ready_successor(*args, **kwargs)

    def _adopt_capability(self, *args, **kwargs):
        return self.tasks.adopt_capability(*args, **kwargs)

    def _register_token(self, acb: ACB, token) -> None:
        """C5：恢复/签发的 token 必须注册进 `policy._tokens`（否则 `_traceable` 全红）。

        以 **ACB id** 为键（`_traceable` 按 `parent_id` 查）；`agent_id="*"`
        是测试夹具通配，不占用 `"*"` 键（避免覆盖 default_token）。
        """
        if token is None:
            return
        self.policy._tokens[acb.id] = token
        agent_id = str(getattr(token, "agent_id", "") or "")
        if agent_id and agent_id != "*" and agent_id != acb.id:
            self.policy._tokens.setdefault(agent_id, token)

    def _token_from_payload(self, cap, acb_id: str = ""):
        """把 checkpoint 的 `capability` 重建为 `CapabilityToken`（不可解析 → None）。"""
        if isinstance(cap, dict):
            return CapabilityToken(
                agent_id=str(cap.get("agent_id", "") or acb_id),
                tools=set(cap.get("tools") or ()),
                paths=set(cap.get("paths") or ()),
                network=set(cap.get("network") or ()),
                data_class=str(cap.get("data_class", "public")),
                parent_id=str(cap.get("parent_id", "") or ""),
                parent_scope=cap.get("parent_scope")
                if isinstance(cap.get("parent_scope"), dict) else None,
                parent_scope_digest=str(
                    cap.get("parent_scope_digest", "") or ""),
            )
        if isinstance(cap, str):
            resolved = self.policy._resolve_raw(cap)
            return resolved
        return None

    def _ensure_policy_token(self, parent_id: str, depth: int = 0):
        """C5：按需递归加载父链 token 并注册（不依赖恢复顺序）。

        内存无该 ACB → 读 `latest_ref_for_acb(parent_id)` 快照重建 token；
        祖先继续递归。任一环缺失 → 返回 None（`_traceable` fail-closed）。
        """
        if not parent_id or depth >= 16:
            return None
        cached = self.policy._tokens.get(parent_id)
        if cached is not None:
            return cached
        token = None
        src = self.acbs.get(parent_id)
        if src is not None:
            raw = getattr(src, "capability", None)
            token = self._token_from_payload(raw, parent_id) \
                if raw is not None else self.policy.token(parent_id)
        if token is None:
            ref = self._ref_for_child(parent_id)
            if ref:
                try:
                    data = self.checkpoints.load_strict(ref)
                except Exception:  # noqa: BLE001
                    try:
                        data = self.checkpoints.load(ref)
                    except Exception:  # noqa: BLE001
                        data = None
                if isinstance(data, dict):
                    token = self._token_from_payload(data.get("capability"),
                                                     parent_id)
        if token is None or not isinstance(token, CapabilityToken):
            return None
        self.policy._tokens[parent_id] = token
        agent_id = str(getattr(token, "agent_id", "") or "")
        if agent_id and agent_id != "*" and agent_id != parent_id:
            self.policy._tokens.setdefault(agent_id, token)
        ancestor = str(getattr(token, "parent_id", "") or "")
        if ancestor:
            self._ensure_policy_token(ancestor, depth + 1)
        return token

    def _safe_save(self, acb: ACB) -> bool:
        """落盘（**保证不抛普通异常**）：P1-5 —— 非法 staged 内容不得阻断结算。

        X8：**不再吞 `FencingRejected`** —— 被 fence 的快照写不进盘（旧 worker
        不得覆盖新 claim），这里显式记录到 `_drain_skipped` 并返回 `False`，
        由 `_finalize` 中止结算（交给新持有者）。普通编码错误仍按 P1-5 放行
        （返回 `True`，不阻断结算/槽位释放）。`CrashInjected`（BaseException）穿透。
        """
        try:
            self.checkpoints.save(acb)
            return True
        except FencingRejected:
            self._drain_skipped.append((acb.id, "save_fenced"))
            return False
        except Exception:  # noqa: BLE001  # 契约：落盘失败不得跳过后续结算
            return True

    # ---- R6：准入唯一性登记 -----------------------------------------
    def _admission_key_for(self, *args, **kwargs):
        return self.tasks._admission_key_for(*args, **kwargs)

    def _lookup_admission(self, *args, **kwargs):
        return self.tasks.lookup_admission(*args, **kwargs)

    def _register_admission(self, *args, **kwargs):
        return self.tasks.register_admission(*args, **kwargs)

    def _clear_admission(self, *args, **kwargs):
        return self.tasks.clear_admission(*args, **kwargs)

    # ---- S3.1：已迁至 kernel/children.py（旧名委托，S5 删除）----
    def _replace_child_ref(self, *args, **kwargs):
        return self.children._replace_child_ref(*args, **kwargs)

    # ---- R7：每 ACB 一把 transition 锁 -------------------------------
    def _transition_lock(self, *args, **kwargs):
        return self.tasks.transition_lock(*args, **kwargs)

    def _finalize(self, *args, **kwargs):
        return self.tasks._finalize(*args, **kwargs)

    def _maybe_poison_locked(self, *args, **kwargs):
        return self.tasks._maybe_poison_locked(*args, **kwargs)

    def _finalize_locked(self, *args, **kwargs):
        return self.tasks._finalize_locked(*args, **kwargs)

    def escalate_quarantined(self, *args, **kwargs):
        return self.tasks.escalate_quarantined(*args, **kwargs)

    # ---- 恢复（S4：实现已迁至 kernel/recovery.py；旧名委托，S5 删除）------
    # 旧名（文档 §5.8 / §4.3）保留到 S5；`self.recovery` 是唯一实现
    # （不再有第二份逻辑）。签名保持不变（`_restore_acb` 的 `bump_epoch`
    # 由门面 token 懒恢复使用）。
    def _pointer_header_fallback(self, ref: str) -> dict:
        return self.recovery._pointer_header_fallback(ref)

    def recover(self, ref: str, *, resume_policy: str = "RESUME") -> ACB:
        return self.recovery.recover(ref, resume_policy=resume_policy)

    def replan(self, *args, **kwargs):
        return self.tasks.replan(*args, **kwargs)

    def _prune_id_aliases(self, *args, **kwargs):
        return self.tasks._prune_id_aliases(*args, **kwargs)

    def replan_from_quarantine(self, *args, **kwargs):
        return self.tasks.replan_from_quarantine(*args, **kwargs)

    def _dead_letter_after_replan(self, *args, **kwargs):
        return self.tasks._dead_letter_after_replan(*args, **kwargs)

    def _readmit_after_replan(self, *args, **kwargs):
        return self.tasks._readmit_after_replan(*args, **kwargs)

    def requeue_readmit(self, *args, **kwargs):
        return self.tasks.requeue_readmit(*args, **kwargs)

    def _restore_acb(self, data: dict, resume_policy: str = "RESUME", *,
                     bump_epoch: bool = True, for_sweep: bool = False) -> ACB:
        return self.recovery.restore_acb(
            data, resume_policy, bump_epoch=bump_epoch, for_sweep=for_sweep)

    def _restore_capability(self, acb: ACB, cap) -> None:
        return self.recovery._restore_capability(acb, cap)

    def _restore_budget(self, acb: ACB, data: dict) -> None:
        return self.recovery._restore_budget(acb, data)

    def _replay_budget_events(self, cutoff_epoch: int,
                              run_id: str | None = None) -> tuple[bool, list[str]]:
        return self.recovery._replay_budget_events(cutoff_epoch, run_id)

    def _declared_budget_nodes(self, run_id: str | None = None) -> set[str]:
        return self.recovery._declared_budget_nodes(run_id)

    def _restore_budget_by_replay(self, acb: ACB, data: dict, node_id: str) -> bool:
        return self.recovery._restore_budget_by_replay(acb, data, node_id)

    def _restore_budget_ledger(self, acb: ACB, data: dict, node_id: str) -> None:
        return self.recovery._restore_budget_ledger(acb, data, node_id)

    def _restore_path(self, path: list[str], limits=None) -> list:
        return self.recovery._restore_path(path, limits=limits)

    def _replay_events(self, from_seq: int, dedup: bool = True) -> int:
        return self.recovery._replay_events(from_seq, dedup)

    @staticmethod
    def _propagate_subtree_delta(node, delta: int) -> None:
        return RecoveryService._propagate_subtree_delta(node, delta)


def create_production_harness(*args, **kwargs) -> Harness:
    """S12 K-55：**生产唯一入口**（显式 fail-closed，不依赖运维记得设 env）。

    - 强制 `egress_required=True`（构造即校验，无强制点 → 拒绝启动）；
    - 再显式 `require_available()`（无强制点 → `EgressUnavailable`）。

    `Harness()` 默认 `egress_required=None`（本地可跑）——**生产必须走本入口**。
    """
    kwargs["egress_required"] = True
    harness = Harness(*args, **kwargs)
    harness.egress.require_available()
    return harness


__all__ = ["EpochStore", "Harness", "ReceiptScanFailed", "create_production_harness"]
