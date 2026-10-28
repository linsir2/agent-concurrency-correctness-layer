"""Harness —— 组装所有组件的门面（对应文档 §20.2 端到端数据流）。

用法见 `examples/` 与 `tests/`。
"""

from __future__ import annotations

import copy
import hashlib
import itertools
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext

from .auth import Signer
from .budget import BudgetManager, InvariantError, Reservation, _is_budget_kind
from .checkpoint import (
    CheckpointCorrupt,
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
from .llm import LLMClient
from .orchestration import (
    _UNSET,
    HITL,
    Dispatcher,
    Planner,
    ReconcileLoop,
    RunCoordinator,
    Scheduler,
    SlotTracker,
    _tenant_of_node_id,
    classify_verdict,
)
from .policy import (
    CapabilityAttenuationError,
    CapabilityToken,
    PolicyEngine,
    Scope,
    attenuate_token,
)
from .rebase import DELETE
from .resolver import ScriptedResolver
from .runtime import AgentRuntime, CrashInjected
from .spec.receipt import (
    GENESIS_HASH,
    Receipt,
    ReceiptMalformed,
    build_receipt,
    receipt_hash,
    verify_chain_detailed,
)
from .storage._lock import (
    RECEIPTS_ORDER,
    FencingRejected,
    _FileLock,
    receipts_lock_path,
)
from .storage.atomic import atomic_write
from .storage.blob import InMemoryBlobStore, canonical_bytes
from .storage.codec import CodecError, dumps, loads
from .subscription import SubscriptionRegistry
from .types import (
    ACB,
    TASK_TERMINAL,
    EventKind,
    Mode,
    ReceiptConflict,
    Result,
    ResultEnvelope,
    Status,
    TaskState,
    Write,
    advance,
)
from .workspace import WorkspaceManager, WorkspaceVersionStore

# S13d §2.5 P0-9：worker 内嵌套 `run_task` 一律内联；该上限只用于**防递归**
# （不是 spawn 的父子层数 `spawn_depth`，两者语义不同）。
_MAX_INLINE_DEPTH = 8

# S22：外部幂等键命中「同键异内容」时的哨兵（与「无命中=None」区分开）。
_EXT_KEY_CONFLICT = object()


class _NullLock:
    """空上下文管理器（无外部幂等键时占位，避免分支里写两套 `with`）。"""

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _SpawnAbort(Exception):
    """C1：spawn ② 事务的「明确拒绝」信号（非崩溃），由回滚分支转成 failure。"""


class ReceiptScanFailed(RuntimeError):
    """K-45：收据回扫失败（EventLog 不可用 / WAL 回扫失败）。

    **专用异常**（不复用 `EgressUnavailable`）：收据回扫与 egress 无关，
    语义混用会让上层无法区分「出站不可用」与「审计状态不可信」。
    抛出即**拒绝服务**（不 append、不改 head、不加 taint），由粘滞闩锁
    （`_receipt_scan_failed`）保证后续提交一律被拒，直到一次成功的全量重建。
    """


class EpochStore:
    """K-43 两层 epoch 中的 **run 层**：`read_epoch(run_id)` / `bump_epoch(run_id)`。

    - 全局 writer lease epoch 仍由 `wal.log.epoch` 承担（接管 fencing，保留）；
    - 本 store 按 run 分键，隔离**同一 run 内**的租约（监督者回收一个 run
      不得作废另一个 run 的租约）；
    - `run_id=None`（无参）→ 全局键 `""`，退回全局语义（向后兼容）；
    - 持久化：`path_prefix` 给定 → 每 run 一个文件 `"{prefix}.{run_id}"`
      （WAL 模式即 `wal.log.epoch.<run_id>`），`atomic_write` 保证不撕裂。
    """

    def __init__(self, path_prefix=None) -> None:
        self._lock = threading.RLock()
        self._epochs: dict[str, int] = {}
        self._prefix = str(path_prefix) if path_prefix else None

    @staticmethod
    def _key(run_id: str | None) -> str:
        return run_id or ""

    @staticmethod
    def _safe(key: str) -> str:
        if not key:
            return "global"
        clean = "".join(c if (c.isalnum() or c in "._-") else "_" for c in key)
        if clean != key:
            clean = f"{clean}-{hashlib.sha1(key.encode('utf-8')).hexdigest()[:8]}"
        return clean

    def _path(self, key: str):
        if self._prefix is None:
            return None
        return f"{self._prefix}.{self._safe(key)}"

    def _load(self, key: str) -> int:
        path = self._path(key)
        if path is None:
            return 0
        try:
            with open(path, "r", encoding="utf-8") as fh:
                return int((fh.read() or "0").strip())
        except (OSError, ValueError):
            return 0

    def _store(self, key: str, epoch: int) -> int:
        self._epochs[key] = epoch
        path = self._path(key)
        if path is not None:
            atomic_write(path, str(epoch).encode("utf-8"))
        return epoch

    def read_epoch(self, run_id: str | None = None) -> int:
        with self._lock:
            key = self._key(run_id)
            if key not in self._epochs:
                self._epochs[key] = self._load(key)
            return int(self._epochs.get(key, 0) or 0)

    def set_epoch(self, run_id: str | None, epoch: int) -> int:
        with self._lock:
            key = self._key(run_id)
            epoch = int(epoch or 0)
            cur = self.read_epoch(run_id)
            if epoch > cur:
                self._store(key, epoch)
            return int(self._epochs.get(key, 0) or 0)

    def bump_epoch(self, run_id: str | None = None) -> int:
        with self._lock:
            key = self._key(run_id)
            return self._store(key, self.read_epoch(run_id) + 1)


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
        if isinstance(replan_budget, bool) or not isinstance(replan_budget, int) \
                or replan_budget < 0:
            raise ValueError(f"非法 replan_budget：{replan_budget!r}（>=0 的整数）")
        self.epoch_scope = epoch_scope
        self.replan_budget = replan_budget       # S13b §2.3：replan 预算（写前计数）
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
        self.spawn_depth = spawn_depth
        self.max_fanout = max_fanout
        self.min_budget_per_child = min_budget_per_child
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
        self.scheduler_mode = scheduler_mode
        self.aging_ms = int(aging_ms)
        self.aging_max_attempts = int(aging_max_attempts)
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
        self.claim_lease_ms = claim_ttl
        self.pool_workers = int(pool_workers)
        self.pool_timeout_s = float(pool_timeout_s)
        self._tls = threading.local()             # in_worker / depth / worker_id
        self._worker_ids = itertools.count(1)
        self._pool = (ThreadPoolExecutor(max_workers=self.pool_workers,
                                         thread_name_prefix="harness-worker")
                      if scheduler_mode == "pool" else None)
        self.lease_ms = lease_ms
        self.guard_every = guard_every
        self.aging_max = aging_max
        self.max_steps = max_steps
        self.max_concurrent = max_concurrent
        self.hitl_timeout_ms = int(hitl_timeout_ms)   # S13a：审批超时（tick 判定）
        self.retention = retention
        self.per_action_delay = per_action_delay   # §2.5：基准配对用注入点
        # S12 K-33：per-run 高水位游标（每 run 独立推进，见 `floor_for`）
        self._run_cursors: dict[str, int] = {}

        # ---- S11c/d/e：egress 强制点（见 `harness/egress/__init__.py`）---------
        # `egress_required=True`  → 构造即校验，无强制点 → `EgressUnavailable`（拒绝启动）
        # `egress_required=None`  → 构造不校验，**任何出站** fail-closed（默认）
        # `egress_required=False` → 显式降级（仅测试/离线），原因记入 `degradations`
        self.egress = EgressGuard(required=egress_required)
        # S11-8 / 终审 8-1：真实出站路径（`llm._post`）必须过同一个 guard；
        # 只构造 guard 不接线等于「有锁不装门」——`ensure()` 永远不会被调用。
        self.llm = LLMClient(egress=self.egress)

        self.clock = clock or SystemClock()
        self.event_log = event_log if event_log is not None else EventLog(clock=self.clock)
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
        self.lock_timeout_s = lock_timeout_s
        self.workspace = (workspace if workspace is not None
                          else WorkspaceManager(self.event_log, self.intents, self.versions,
                                                blobs=self.blobs, path_of=path_of,
                                                lock_timeout_s=lock_timeout_s))
        self.budget = (budget if budget is not None
                       else BudgetManager(run_limits, tombstone_ttl_s=tombstone_ttl_s,
                                          run_scope=self.epoch_scope))
        self.budget._epoch_store = self._epoch_store    # S12 K-43：run 层 epoch
        self.budget._run_scope = self.epoch_scope        # S12 K-43：scope 注入
        # S10a §2.1：预算事件溯源 = 唯一真相源（预算变更必须落到全局 EventLog）
        self.budget.event_log = self.event_log
        self._run_limits = int(run_limits)
        self.checkpoints = (checkpoints if checkpoints is not None
                            else (FileCheckpointStore(checkpoint_root)
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
        # ---- S10b §3.2 / S11b §3.2：跨进程锁全序（写死）--------------------
        #   resource → budget → receipts.lock → wal.lock → epoch → store.lock
        #   → eventlog._lock
        # 各 flock 按上表偏序获取；反向获取（如「持 store.lock 取 epoch」）会成环
        # 死锁 → 由 `_FileLock(order=...)` 运行期断言拦截。
        # `receipts.lock`：durable 模式跨进程互斥；内存模式退回进程内 RLock。
        cp_root = getattr(self.checkpoints, "root", None)
        self._receipts_flock = (
            _FileLock(receipts_lock_path(cp_root), order=RECEIPTS_ORDER)
            if cp_root is not None else None)
        # S10b §3.4：WAL append 的 fencing（同一 flock 内复验 run 级 epoch）
        self._wire_wal_fencing()
        self.effects = (effects if effects is not None
                        else EffectJournal(self.event_log))
        self.comp_journal = CompensationJournal(self.event_log)
        self._applied_event_ids: set[str] = set()
        self.checkpoints.applied_event_ids = self._applied_event_ids
        self._drain_skipped: list[tuple[str, str]] = []   # D11：调度跳过原因（不再静默）
        # S14 K-69：`tick` 的跳过原因（水合失败等），与调度跳过分开，避免混淆
        self._tick_skipped: list[tuple[str, str]] = []
        self.auth_gc_every = max(1, int(auth_gc_every))
        self.gc_every = max(1, int(gc_every))
        self._event_count = 0
        self._auth_secret = auth_secret
        self._auth: Signer | None = Signer(auth_secret) if auth_secret else None
        self.subscriptions = (subscriptions if subscriptions is not None
                              else SubscriptionRegistry(self.event_log))
        self.policy = (policy if policy is not None
                       else PolicyEngine(default_allow=policy_default_allow,
                                         default_token=default_token))
        self.policy._signer_factory = self._get_signer
        self._sync_run_cursors()          # S12 K-33：从既有事件初始化 per-run 游标
        self.context = (context if context is not None
                        else ContextAssembler(self.event_log, self.workspace, retention,
                                              floor_for=self.floor_for,
                                              run_scoped=(self.epoch_scope == "run")))
        self.compensation_journal: list[dict] = []      # 文档 §5.4
        self.decide = decide
        self.resolver = (resolver if resolver is not None
                         else ScriptedResolver())       # §7 默认 scripted（不联网）

        self.slots = SlotTracker(max_concurrent, shared=self.checkpoints)
        self.checkpoints.slots = self.slots        # checkpoint 快照活跃槽位
        # F6：checkpoint 落盘的是**有效 token**（`acb.capability` → `policy._tokens`
        # → `default_token`），否则跨进程 recover 会拿到 `None` → 工具全 DENIED。
        self.checkpoints.token_provider = self._effective_token
        self.planner = Planner(self)
        self.dispatcher = Dispatcher(self)
        self.scheduler = Scheduler(self, mode=scheduler_mode)
        self.run_coordinator = RunCoordinator(self)
        self.reconcile = ReconcileLoop(self)
        self.hitl = HITL()
        # ---- S15 D：生产化（Admin 暂停 / DLQ 重放 / Ingress 幂等）----
        # pause 只阻止**未开始**的调度，资源保留（可恢复）；与 cancel 不同。
        self._paused_runs: set[str] = set()
        self.max_redrives = max(0, int(max_redrives))
        # S17 MS10：毒任务识别阈值（连续失败 >= N → 直接 DLQ，避免无限重试）
        self.poison_after_failures = max(0, int(poison_after_failures))
        self.drain_timeout_s = float(drain_timeout_s)
        self._draining = False
        # S21 K-100：本部署的最大租约时长（秒）——墓碑窗口的量纲依据。
        # 必须**向上取整**且覆盖 claim 租约：`lease_ms=1500` 的真实租约是 1.5s，
        # 若向下取整成 1s，墓碑会在旧持有者租约仍有效时被忘记 → fencing 被削弱。
        _max_ttl_ms = max(int(self.lease_ms), int(self.claim_lease_ms))
        self.max_lease_ttl_s = max(1, -(-_max_ttl_ms // 1000))
        self._claim_window_exceeded = 0
        # S22：门面 token 回收后的清理钩子（由 `RscSession` 注入，用于销毁 Z3）。
        self._token_cleanup = None
        self.ingress = Ingress(self)
        # S13d §2.5 P0-10：claim 的 fence epoch 接入**写路径**（commit_batch /
        # budget.consume / effects.begin / checkpoints.save）。sync 模式无 claim
        # → 守卫恒为 False（保持既有语义，36 个同步用例零破坏）。
        self.budget.claim_guard = self._claim_fenced
        self.effects.claim_guard = self._claim_fenced
        self.checkpoints.claim_guard = self._claim_fenced
        # X7：claim 三字段随快照落盘（无锁只读，仅作恢复后的观测/租约判定）。
        self.checkpoints.claim_provider = self._claim_snapshot

        self.runtime = AgentRuntime(self)
        self.plans: dict[str, dict] = {}
        self.acbs: dict[str, ACB] = {}
        self._lock = threading.RLock()
        # R6：准入唯一性登记 `(run_id, task_id, plan_version) → ACB`。
        # 同一 key 的活跃 ACB 不会被第二次 try_admit 复制出 `A-...-2`（双跑）。
        self._admission_index: dict[tuple[str, str, int], ACB] = {}
        # R7：每 ACB 一把 transition 锁（replan / _finalize / requeue_readmit 共用）
        self._transition_locks: dict[str, threading.RLock] = {}
        # X6：`parent_id → {child_acb_id}` 反向索引（跨 run 子；父 `children_ids`
        # 缺失的老 checkpoint 也能被取消级联找回）。恢复时按 `spec.spawn.parent_id`
        # 反扫建立；spawn 成功与恢复子时增量更新。
        self._children_index: dict[str, set[str]] = {}
        self._children_index_built = False
        # S9 §2.3：收据链（每 scope 一把 RLock；head/id 由 EventLog 重建）
        self.receipts_enabled = receipts_enabled
        # 锁序写死：resource → scope → eventlog（docs s9 §2.3.5）。
        # **durable 模式为单写者**：`_scope_lock` 是**进程内** `threading.RLock`，
        # 跨进程**不互斥**；`WalEventLog.append_batch` 的 `_lock` 同理。跨进程
        # 多写者必须自行串行化（跨进程 scope 锁留 S10，本阶段不实现）。
        self._scope_locks: dict[tuple[str, str], threading.RLock] = {}
        # S22：外部幂等键的进程内串行锁（同键并发 apply 不得双写）。
        self._ext_key_locks: dict[str, threading.RLock] = {}
        self._receipt_heads: dict[tuple[str, str], str] = {}
        self._receipt_ids: set[str] = set()
        self._tainted_scopes: set[tuple[str, str]] = set()
        self._malformed_receipts: list[dict] = []
        # 锁内增量刷新：水位（已扫到的最大 seq）+ 刷新互斥（内存模式 per-scope
        # 锁不互斥 → 需串行化，避免共享水位被并发推进而漏扫）。durable 模式
        # scope 锁即跨进程 flock，天然串行；此锁仍保证同进程多线程一致。
        self._receipt_scan_watermark = 0
        self._receipt_refresh_lock = threading.RLock()
        # S12 K-45：粘滞闩锁（回扫失败 → 后续提交一律被拒，直到全量重建）
        self._receipt_scan_failed = False
        self._rebuild_receipts()

        # ---- S10a §2.5：单写者租约（durable 模式在构造期即 acquire）----
        # 生产路径必须真正持有租约：此前 `_try_acquire_writer_lease` 只被测试调用，
        # 单写者形同虚设。acquire 失败 → `_writer_lease_ok=False`，写入口拒绝服务。
        # S11b / 终审 7-1：**取租约前先恢复 durable epoch**。否则干净重启
        # （lease 已释放但 WAL epoch 仍为 N）时本地 epoch=0 → 首写被 fence。
        self._restore_epoch()
        self._writer_id = writer_id or f"harness-{os.getpid()}"
        self._durable = self._lease_path() is not None
        self._writer_lease_ok = True
        if self._durable:
            self._try_acquire_writer_lease()
            on_close = getattr(self.event_log, "on_close", None)
            if callable(on_close):
                on_close(self._release_writer_lease)

        self.event_log.subscribe(self._on_event)

    # ---- 收据链（S9 §2.3 / K-24） --------------------------------
    def _rebuild_receipts(self) -> None:
        """无条件从 EventLog 重建收据 head / id（不读 checkpoint 内存值）。

        P2：按 scope 维护 running head，**校验链连续性** —— `prev_receipt_hash`
        必须等于 running head 且 `receipt_id` 不重复；失败 → 该 scope 标
        **tainted** 并拒绝继续扩展。否则已存在的重复/乱序收据会被静默折叠，
        新收据继续接在坏 head 上 → `verify_receipts` 永远 False。
        P2：畸形 payload（缺 `receipt_id`/`run_id`/`task_id`）→ 记入
        `_malformed_receipts`，其 scope 标 tainted（**不得抛** → 单条坏事件
        不得打挂 Harness 启动）。
        """
        heads: dict[tuple[str, str], str] = {}
        ids: set[str] = set()
        tainted: set[tuple[str, str]] = set()
        running: dict[tuple[str, str], str] = {}
        malformed: list[dict] = []
        for ev in self.event_log.all():
            if ev.kind != EventKind.RECEIPT_ISSUED:
                continue
            payload = ev.payload or {}
            try:
                receipt = Receipt.from_dict(payload)
            except ReceiptMalformed as exc:
                malformed.append({"seq": ev.seq, "error": str(exc),
                                  "payload": payload})
                scope = self._payload_scope(payload)
                if scope is not None:
                    tainted.add(scope)
                continue
            scope = (receipt.run_id, receipt.logical_subgoal_id)
            if scope in tainted:
                continue
            if receipt.receipt_id in ids:            # 重复 receipt_id → 断链
                tainted.add(scope)
                continue
            head = running.get(scope, GENESIS_HASH)
            if receipt.prev_receipt_hash != head:    # 乱序/断链
                tainted.add(scope)
                continue
            ids.add(receipt.receipt_id)
            running[scope] = receipt_hash(receipt)
            heads[scope] = running[scope]
        self._receipt_heads = heads
        self._receipt_ids = ids
        self._tainted_scopes = tainted
        self._malformed_receipts = malformed
        # K-45：**成功的全量重建**是粘滞闩锁的唯一解除条件（水位推到 tail）。
        self._receipt_scan_failed = False
        # 首次构建完成 → 水位推到当前尾：之后锁内刷新只扫「新增」收据，
        # 不重复扫旧事件（否则每次 commit 全量扫 EventLog → O(N²)）。
        try:
            self._receipt_scan_watermark = self.event_log.tail()
        except Exception:  # noqa: BLE001  # tail 不可用 → 0（刷新退回全扫，仍正确）
            self._receipt_scan_watermark = 0

    @staticmethod
    def _scan_receipts(records, heads: dict, ids: set, tainted: set,
                       malformed: list) -> int:
        """按 seq 顺序应用收据记录，维护 running head + 连续性校验。

        `_rebuild_receipts`（首次构建）与 `_refresh_receipts_locked`（锁内增量
        刷新）**共用**此逻辑：`prev_receipt_hash` 必须等于 running head 且
        `receipt_id` 不重复；失败 → 该 scope tainted 并拒绝继续扩展；畸形
        payload → 记入 `malformed`（**不得抛**）。返回处理到的最大 seq
        （推进水位用）。

        注意 `running`/`heads` 合一（原实现两者恒等）：每 scope 的 head 就是
        该 scope 最后一条被接受的收据的链接值。
        """
        max_seq = -1
        for seq, payload in records:
            max_seq = seq
            try:
                receipt = Receipt.from_dict(payload)
            except ReceiptMalformed as exc:
                malformed.append({"seq": seq, "error": str(exc),
                                  "payload": payload})
                scope = Harness._payload_scope(payload)
                if scope is not None:
                    tainted.add(scope)
                continue
            scope = (receipt.run_id, receipt.logical_subgoal_id)
            if scope in tainted:
                continue
            if receipt.receipt_id in ids:            # 重复 receipt_id → 断链
                tainted.add(scope)
                continue
            head = heads.get(scope, GENESIS_HASH)
            if receipt.prev_receipt_hash != head:    # 乱序/断链
                tainted.add(scope)
                continue
            ids.add(receipt.receipt_id)
            heads[scope] = receipt_hash(receipt)
        return max_seq

    def _receipt_records(self, watermark: int = 0) -> list[tuple[int, dict]]:
        """返回 `seq > watermark` 的 `RECEIPT_ISSUED` 记录 `[(seq, payload)]`。

        **K-44**：WAL 实现走 `wal.receipts_from(watermark)` —— 在 `wal.lock` 内
        O(1) fstat 比对 + **只解析新增帧**（增量索引），不再每次 `wal.records()`
        全量枚举（那会是 O(N²)）。内存 `EventLog` 直接读 `all()`。
        """
        log = self.event_log
        wal = getattr(log, "_wal", None)
        if wal is None:
            recs = [(ev.seq, ev.payload) for ev in log.all()
                    if ev.kind == EventKind.RECEIPT_ISSUED]
        else:
            receipts_from = getattr(wal, "receipts_from", None)
            if callable(receipts_from):
                recs = list(receipts_from(watermark))
            else:                                    # 兼容旧 WAL 实现
                lock = getattr(wal, "_lock", None)
                if lock is not None:                 # 锁序 receipts → wal
                    with lock():
                        self._rescan_wal_from_disk(wal)
                        raw = wal.records()
                else:
                    self._rescan_wal_from_disk(wal)
                    raw = wal.records()
                recs = [(r.get("seq", 0), r.get("payload"))
                        for r in raw
                        if r.get("kind") == EventKind.RECEIPT_ISSUED.value]
        recs.sort(key=lambda rp: rp[0])
        return recs

    @staticmethod
    def _rescan_wal_from_disk(wal) -> None:
        """flock 内 O(1) fstat 比对：仅当别的进程写过才回扫（复用 WAL 既有优化）。

        K-45：回扫失败**不得静默**（与 `_receipt_records` / `Wal.receipts_from`
        同口径）—— 异常向上抛，由 `_refresh_receipts_locked` 置粘滞闩锁并
        抛 `ReceiptScanFailed` → 提交 fail-closed。
        """
        fd = getattr(wal, "_fd", None)
        cached = getattr(wal, "_cached_end", None)
        if (fd is not None and cached is not None
                and os.fstat(fd).st_size == cached):
            return
        rescan = getattr(wal, "_rescan_from_disk", None)
        if callable(rescan):
            rescan()

    def _refresh_receipts_locked(self) -> None:
        """scope 临界区内：从 EventLog **增量**刷新 head/ids（跨进程安全）。

        根因（K-24 后续）：`_receipt_heads`/`_receipt_ids` 只在 `__init__`/
        `recover` 重建一次；跨进程下另一进程可能在我们构建之后才 append 收据
        → 锁内读到**过期 head** → 链分叉。故在 `commit_batch` 的 scope 临界区
        内、读 head 前调用本方法：取锁 → 刷新 → 读 head → 构造 drafts →
        append_batch → 提交内存态。

        增量：只处理 `seq > _receipt_scan_watermark` 的 RECEIPT_ISSUED（水位
        在首次构建时推到当前尾），避免每次 commit 全量扫。

        K-45（fail-closed）：回扫失败**不再静默回退** —— 置粘滞闩锁
        `_receipt_scan_failed` 并抛 `ReceiptScanFailed`（专用异常）。
        不 append、不改 head、不加 taint（链未污染，无需隔离）。
        """
        if not self.receipts_enabled:
            return
        with self._receipt_refresh_lock:
            watermark = self._receipt_scan_watermark
            try:
                records = self._receipt_records(watermark)
            except Exception as exc:  # K-45：回扫失败 → 拒绝服务
                self._receipt_scan_failed = True
                raise ReceiptScanFailed(str(exc)) from exc
            new_records = [rp for rp in records if rp[0] > watermark]
            if not new_records:
                return
            heads = dict(self._receipt_heads)
            ids = set(self._receipt_ids)
            tainted = set(self._tainted_scopes)
            malformed = list(self._malformed_receipts)
            max_seq = self._scan_receipts(new_records, heads, ids, tainted,
                                          malformed)
            self._receipt_heads = heads
            self._receipt_ids = ids
            self._tainted_scopes = tainted
            self._malformed_receipts = malformed
            if max_seq > watermark:
                self._receipt_scan_watermark = max_seq

    @staticmethod
    def _payload_scope(payload: dict) -> tuple[str, str] | None:
        """从（可能畸形的）payload 尽力推断 scope（用于 tainted 标记）。"""
        if not isinstance(payload, dict):
            return None
        run_id = payload.get("run_id")
        if not isinstance(run_id, str) or not run_id:
            return None
        return (run_id, payload.get("logical_subgoal_id") or "")

    def _scope_lock(self, scope: tuple[str, str]):
        """取（必要时创建）scope 锁；只护锁表，极短，不覆盖临界区。

        **durable 模式**：返回跨进程 `receipts.lock`（flock，全序第 1 位），
        跨进程互斥 → 同 scope 并发提交串行化（链不分叉）。
        **内存模式**：退回每 scope 一把进程内 `RLock`（跨进程不互斥，§K-34）。
        """
        if self._receipts_flock is not None:
            return self._receipts_flock()
        with self._lock:
            lock = self._scope_locks.get(scope)
            if lock is None:
                lock = threading.RLock()
                self._scope_locks[scope] = lock
            return lock

    # ---- K-33：per-run retention（每 run 独立高水位）----------------
    def _sync_run_cursors(self) -> None:
        """从既有事件初始化 per-run 游标（构造 / 恢复后调用一次）。"""
        for ev in self.event_log.all():
            self._track_run_cursor(ev)

    def _track_run_cursor(self, ev) -> None:
        """事件到达 → 推进其 run 的高水位（未打 run 标签的事件记入 `""`）。"""
        run_id = getattr(ev, "run_id", "") or ""
        seq = int(getattr(ev, "seq", 0) or 0)
        if seq > self._run_cursors.get(run_id, 0):
            self._run_cursors[run_id] = seq

    def _run_tail(self, run_id: str) -> int:
        """该 run 的**per-run** tail（含未打标签事件 `""`，向后兼容旧日志）。"""
        return max(int(self._run_cursors.get(run_id, 0) or 0),
                   int(self._run_cursors.get("", 0) or 0))

    def floor_for(self, run_id: str) -> int:
        """该 run 的保留窗口下界（`retention <= 0` → 不限制）。

        S12 K-33：`epoch_scope="run"` → 用 **per-run tail**（否则 run B 的活动会把
        run A 的保留窗口一起推进，A 的历史事件被截断后**找不回**）；
        `epoch_scope="global"` → **保持现状**（全局 tail，旧语义）。
        """
        if self.retention <= 0:
            return 0
        if self.epoch_scope != "run":
            return max(0, self.event_log.tail() - self.retention + 1)
        return max(0, self._run_tail(run_id) - self.retention + 1)

    def _wire_wal_fencing(self) -> None:
        """S11b K-46：把 fencing 三件套注入 `WalEventLog`（fencing **绑 WAL**）。

        - `lock`：`wal.epoch_lock`（独立 epoch 锁，全序第 4 位）；
        - `epoch_reader`：`wal.read_epoch`（读 `wal.log.epoch`，无锁原子读）；
        - `epoch_provider`：本写者当前 run 级 epoch（`intents.get_epoch()`）。

        **不再绑 store.json**：旧实现用 `checkpoints._flock` + `read_epoch`，
        导致「两个实例共享 WAL、不同 checkpoint root」时互不 fence（S11-3 负对照）。
        非 WAL 实现 / 无 epoch 接口 → 不注入（fencing 不生效）。
        """
        log = self.event_log
        setter = getattr(log, "set_fencing", None)
        if not callable(setter):
            return
        wal = getattr(log, "_wal", None)
        epoch_lock = getattr(wal, "epoch_lock", None)
        read_epoch = getattr(wal, "read_epoch", None)
        if epoch_lock is None or not callable(read_epoch):
            return
        setter(lock=epoch_lock, epoch_reader=read_epoch,
               epoch_provider=lambda: int(self.intents.get_epoch() or 0))

    def _receipt_secret(self) -> str:
        """收据签名密钥来源：构造参数优先，否则读 env（开关独立于 secret 是否存在）。"""
        if self._auth_secret:
            return self._auth_secret
        return os.environ.get("HARNESS_AUTH_SECRET", "")

    @staticmethod
    def _commit_key(acb: ACB, resource: str, content_hash: str, mode: Mode) -> str:
        """同一逻辑提交恒等键（P0-2）：**不含 base_version**。

        `base_version` 是「读到哪一版」的**状态**而非写的内容；刷新 ACB
        （提交后重读）后它会变，若入键则同一逻辑写的重试去重失效，
        导致重复提交 + 重复收据。
        """
        raw = (f"{acb.run_id}|{acb.logical_subgoal_id}|{resource}|"
               f"{content_hash}|{mode.value}")
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]

    def _dedup_event(self, acb: ACB, resource: str, blob_hash: str,
                     deleted: bool, mode: Mode):
        """查找既有 `RESOURCE_COMMITTED`：同 (run, subgoal, resource, content, mode)。

        P0-1②：必须校验已提交 `blob_hash` == 新内容 hash（含 DELETE 哨兵），
        内容不同 → 返回 `None`（视为不同写，绝不允许短路）。

        P1-B：dedup 只能命中该资源的**最新**提交 —— 命中事件的 `version` 必须等于
        `workspace._effective_version(resource)`。否则说明该内容已被后续写覆盖，
        命中它会把新写**静默丢弃**（返回 OK + dedup=True，但 version 与值都不变）。
        反例：`r=A(v1) → r=A(v2) → r=B(v3) → 再写 A` 必须产生 v4=A，而不是命中 v2。
        """
        effective = self.workspace._effective_version(resource)
        for ev in reversed(self.event_log.all()):
            if ev.kind != EventKind.RESOURCE_COMMITTED:
                continue
            payload = ev.payload or {}
            if payload.get("resource") != resource:
                continue
            if payload.get("mode", Mode.COMMIT.value) != mode.value:
                continue
            if ev.run_id != acb.run_id:
                continue
            if payload.get("logical_subgoal_id", "") != acb.logical_subgoal_id:
                continue
            if bool(payload.get("deleted", False)) != deleted:
                continue
            if payload.get("blob_hash", "") != blob_hash:
                continue                      # 内容不同 → 不得短路（P0-1②）
            try:
                matched_version = int(payload.get("version", 0) or 0)
            except (TypeError, ValueError):
                return None                   # 版本不可解析 → 视为新写（不猜）
            if matched_version != effective:
                return None                   # 命中的是陈旧版本 → 视为新写（P1-B）
            return ev
        return None

    def _ext_key_lock(self, key: str):
        """S22：外部幂等键的进程内串行锁（只护锁表，不覆盖临界区）。"""
        with self._lock:
            lock = self._ext_key_locks.get(key)
            if lock is None:
                lock = threading.RLock()
                self._ext_key_locks[key] = lock
            return lock

    def _dedup_by_external_key(self, key: str, expected: set):
        """S22：按**外部业务键**查既有提交（事件即真相，不另建存储）。

        `expected = {(resource, blob_hash, deleted)}`。
        返回 `(hits, receipt_ids, seq)` / `_EXT_KEY_CONFLICT` / `None`。

        收据 id 从既有 `RECEIPT_ISSUED` 事件读（**不能**用当前 ACB 重算：
        重启后 token 恢复出的 ACB 可能换了 run/subgoal，重算会得到不同 id）。
        """
        hits = []
        for ev in self.event_log.all():
            if ev.kind is not EventKind.RESOURCE_COMMITTED:
                continue
            if (ev.payload or {}).get("external_idem_key") != key:
                continue
            hits.append(ev)
        if not hits:
            return None
        actual = {(e.payload.get("resource"), e.payload.get("blob_hash"),
                   bool(e.payload.get("deleted", False))) for e in hits}
        if actual != set(expected):
            return _EXT_KEY_CONFLICT
        scopes = {(e.run_id, (e.payload or {}).get("logical_subgoal_id", ""))
                  for e in hits}
        resources = {r for r, _h, _d in expected}
        receipt_ids = []
        for ev in self.event_log.all():
            if ev.kind is not EventKind.RECEIPT_ISSUED:
                continue
            p = ev.payload or {}
            if ((ev.run_id, p.get("logical_subgoal_id", "")) in scopes
                    and p.get("resource") in resources):
                rid = p.get("receipt_id")
                if rid:
                    receipt_ids.append(rid)
        seq = max(int(getattr(e, "seq", 0) or 0) for e in hits)
        return tuple(hits), tuple(receipt_ids), seq

    def _replay_dedup_events(self, dedup_events) -> None:
        """P0-1①：把已提交但未投影的 `RESOURCE_COMMITTED` 补投影到 manifest。

        用版本守卫投影（`_project_event`），既补上缺失投影，又不会把 manifest
        回退到更旧的版本。
        """
        for ev in dedup_events:
            if ev is not None:
                self.workspace.apply_event(ev)

    def _refresh_after_commit(self, acb: ACB, records) -> None:
        """⑦′：自写后重读，刷新 ACB 的 read_set / base_version。"""
        for w, _draft, _vb, _va, _ck in records:
            new_version = self.workspace.version(w.resource)
            acb.read_set[w.resource] = new_version
            acb.base_version[w.resource] = new_version

    def _receipt_event(self, receipt_id: str):
        for ev in reversed(self.event_log.all()):
            if ev.kind == EventKind.RECEIPT_ISSUED and ev.subject == receipt_id:
                return ev
        return None

    def _receipt_is_stale(self, receipt_id: str) -> bool:
        """该 `receipt_id` 对应的收据是否为「陈旧内容」收据（P1-B 后续）。

        若资源当前版本已推进到比该收据 `version_after` 更新的位置，说明该内容
        已被后续写覆盖 → 本次是**合法新写**（dedup 正确地未命中陈旧版本），
        应换 receipt_id 而不是永久冲突。
        """
        ev = self._receipt_event(receipt_id)
        if ev is None:
            return False
        try:
            receipt = Receipt.from_dict(ev.payload or {})
        except ReceiptMalformed:
            return False
        return self.workspace.version(receipt.resource) != receipt.version_after

    def _derive_receipt_id(self, commit_key: str) -> str:
        """为合法重写派生一个不冲突的 `receipt_id`（换 receipt_id，P2-C）。"""
        n = 1
        candidate = f"{commit_key}#{n}"
        while candidate in self._receipt_ids:
            n += 1
            candidate = f"{commit_key}#{n}"
        return candidate

    def receipts_of(self, run_id: str, logical_subgoal_id: str | None = None
                    ) -> list[Receipt]:
        """按 seq 顺序返回某 run（可选 scope）的全部收据。

        P2：畸形事件（缺必填字段）**跳过并记录**（记入 `_malformed_receipts`），
        不再抛 `KeyError` —— 单条坏事件不得让验签/启动打挂。
        """
        out: list[Receipt] = []
        for ev in self.event_log.all():
            if ev.kind != EventKind.RECEIPT_ISSUED:
                continue
            payload = ev.payload or {}
            if payload.get("run_id") != run_id:
                continue
            if (logical_subgoal_id is not None
                    and payload.get("logical_subgoal_id") != logical_subgoal_id):
                continue
            try:
                out.append(Receipt.from_dict(payload))
            except ReceiptMalformed:
                continue
        return out

    def verify_receipts(self, run_id: str,
                        logical_subgoal_id: str | None = None) -> bool | None:
        """离线验证该 run 的收据链（三态：`True` / `False` / `None`）。

        链按 `(run_id, logical_subgoal_id)` 独立成链；未指定 scope 时逐链验证后
        合并：任一 `False` → `False`；全部 `True` → `True`；否则 `None`。

        P2：tainted scope（畸形/重复/断链）→ `False`（拒绝继续扩展坏链）。
        """
        receipts = self.receipts_of(run_id, logical_subgoal_id)
        secret = self._receipt_secret()
        if logical_subgoal_id is not None:
            if (run_id, logical_subgoal_id) in self._tainted_scopes:
                return False
            return verify_chain_detailed(receipts, secret)
        groups: dict[str, list[Receipt]] = {}
        for receipt in receipts:
            groups.setdefault(receipt.logical_subgoal_id, []).append(receipt)
        results = []
        for subgoal, chain in groups.items():
            if (run_id, subgoal) in self._tainted_scopes:
                results.append(False)
            else:
                results.append(verify_chain_detailed(chain, secret))
        if not results or any(r is False for r in results):
            return False
        if all(r is True for r in results):
            return True
        return None

    # ---- 收据冲突对账（P2-C） --------------------------------------
    def _commit_event_for_receipt(self, receipt, run_id: str,
                                  logical_subgoal_id: str):
        """判断某收据是否有对应的 `RESOURCE_COMMITTED` 事件（对账用）。

        匹配键：`run_id` + `logical_subgoal_id` + `resource` + `version_after`
        （版本在资源内唯一，故可定位到唯一提交事件）。
        """
        for ev in self.event_log.all():
            if ev.kind != EventKind.RESOURCE_COMMITTED:
                continue
            payload = ev.payload or {}
            if ev.run_id != run_id:
                continue
            if payload.get("logical_subgoal_id", "") != logical_subgoal_id:
                continue
            if payload.get("resource") != receipt.resource:
                continue
            try:
                version = int(payload.get("version", 0) or 0)
            except (TypeError, ValueError):
                continue
            if version == receipt.version_after:
                return ev
        return None

    def reconcile_scope(self, run_id: str, logical_subgoal_id: str) -> Result:
        """P2-C 显式对账入口：解除 `ReceiptConflict` 造成的 scope 隔离。

        `ReceiptConflict` 的根因是「收据已签发（`receipt_id` 在 `_receipt_ids`），
        但对应的 `RESOURCE_COMMITTED` 事件缺失」→ `_dedup_event` 失效 → 同逻辑写
        **永久**冲突。本入口执行**补写丢失的 RESOURCE_COMMITTED**：

        ① 对该 scope 的每条收据，若缺少对应的提交事件，且 workspace 的当前版本
           恰好等于 `version_after`（内容未被后续写覆盖）→ 用 workspace 当前内容
           补写一条提交事件（`prev_hash` 不可重建时留空，不影响去重/投影）；
        ② 补写后重跑 `_rebuild_receipts`；若 scope 不再 tainted → 解除隔离；
        ③ 无法补写（资源已被覆盖 / 内容不可重建）→ 保持隔离并返回
           `NEEDS_RECONCILIATION`（需人工：换 receipt_id 或修复事件日志）。
        """
        scope = (run_id, logical_subgoal_id)
        receipts = self.receipts_of(run_id, logical_subgoal_id)
        missing = [r for r in receipts
                   if self._commit_event_for_receipt(r, run_id,
                                                     logical_subgoal_id) is None]
        backfilled = 0
        for r in missing:
            current = self.workspace.version(r.resource)
            blob_hash = self.workspace.content_hash(r.resource)
            if current != r.version_after or not blob_hash:
                # 无法补写 → **保持隔离**（不得因重建而丢失 taint）
                self._tainted_scopes.add(scope)
                return Result(Status.NEEDS_RECONCILIATION, {
                    "scope": list(scope), "reason": "resource_overwritten",
                    "resource": r.resource, "expected_version": r.version_after,
                    "actual_version": current, "blob_hash": blob_hash,
                    "hint": "需人工：换 receipt_id 或修复事件日志",
                })
            payload = {
                "resource": r.resource, "version": r.version_after,
                "mode": Mode.COMMIT.value,
                "logical_subgoal_id": logical_subgoal_id,
                "acb_id": "", "blob_hash": blob_hash, "prev_hash": "",
                "diff": {"prev_hash": "", "current_hash": blob_hash},
            }
            ev = self.event_log.append(
                EventKind.RESOURCE_COMMITTED, r.resource, payload,
                producer="", run_id=run_id,
                idempotency_key=f"reconcile:{run_id}:{logical_subgoal_id}:"
                                f"{r.receipt_id}")
            self.workspace.apply_event(ev)
            backfilled += 1
        self._rebuild_receipts()
        if scope in self._tainted_scopes:
            return Result(Status.NEEDS_RECONCILIATION, {
                "scope": list(scope), "reason": "still_tainted",
                "hint": "需人工：换 receipt_id 或修复事件日志",
            })
        return Result(Status.OK, {"scope": list(scope), "reconciled": True,
                                  "backfilled": backfilled})

    # ---- 事件 → 订阅失效（文档 §5.6） ----------------------------
    def _on_event(self, ev) -> None:
        # S12 K-33：per-run 高水位游标独立推进（run 之间不串）
        self._track_run_cursor(ev)
        # 周期性清理已过期的 jti（按注入时钟，避免 `_seen` 无界增长）
        self._event_count += 1
        if self._event_count % self.gc_every == 0:
            # §4.4：接上 budget.gc（原死代码），now 用注入时钟的 epoch 秒
            self.budget.gc(self.clock.now_ms() / 1000.0)
        if (self._auth is not None
                and self._event_count % self.auth_gc_every == 0):
            self._auth.gc_seen(self.clock.now_ms() / 1000.0)
        if ev.kind == EventKind.RESOURCE_COMMITTED:
            for agent_id in self.subscriptions.subscribers(ev.subject):
                self.subscriptions.notify(ev.subject, ev)

    # ---- 签名 / token ---------------------------------------------
    def _get_signer(self) -> Signer:
        """惰性构造 `Signer`：优先注入 secret，否则读 `HARNESS_AUTH_SECRET`。"""
        if self._auth is None:
            self._auth = Signer.from_env()
        return self._auth

    @property
    def auth(self) -> Signer:
        return self._get_signer()

    def _effective_token(self, acb: ACB):
        """取该 ACB 的有效 token：`acb.capability` → `policy._tokens` → `default_token`。

        无 token → `None`（`_gateway_check` 据此 deny-by-default）。
        """
        token = getattr(acb, "capability", None)
        if token is not None:
            return token
        token = self.policy.token(acb.id)
        if token is not None:
            return token
        return self.policy.default_token


    # ---- 幂等键（文档 §5.8 / I7） --------------------------------
    def idem_key(self, acb: ACB, action, action_index: int) -> str:
        """`hash(run_id, logical_subgoal_id, hash(tool, hash(args)))` —— 不含 task_id。"""
        payload = f"{acb.run_id}|{acb.logical_subgoal_id}|{action.tool}|{action.args}"
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]

    # ---- RSC 提交（文档 §5.5） ------------------------------------
    def commit_batch(self, acb: ACB, writes: list[Write],
                     mode: Mode = Mode.COMMIT, *,
                     idempotency_key: str | None = None) -> Result:
        """公共写入口。提交点顺序不可调换：

        ① 输入校验 → ② 预留租约(L0) → ③ 全序一次性持锁(L3，锁集合含读集合)
        → ④ 校验 → ④′ 内容先落盘 → ⑤ 提交点前复验租约
        → ⑥ 提交点（一次 fdatasync）→ ⑦ 投影 → ⑧ 锁外通知（不掩盖返回值）。

        S22：`idempotency_key` 是**外部业务键**（宿主派生）。同键同内容 → 返回
        既有收据（`dedup=True`，不重复写）；同键异内容 → `CONFLICT`（绝不
        静默覆盖）。为空 → 完全走旧路径，事件字节不变。
        """
        if not writes:
            return Result(Status.OK, {"noop": True, "seq": None})
        # K-45：收据回扫失败的粘滞闩锁未解除 → 一律拒绝服务（fail-closed，
        # 不 append、不改 head、不加 taint；绝不把水位清水位到 0）。
        if self._receipt_scan_failed:
            return Result(Status.UPSTREAM_FAILED, {"reason": "receipt_scan_failed"})
        # S10a §2.5：提交前续租（心跳）；租约被其他写者持有时拒绝服务。
        if not self._ensure_writer_lease():
            return Result(Status.UPSTREAM_FAILED, {
                "reason": "writer_lease_held", "writer": self._writer_id,
            })
        # S13d §2.5 P0-10：claim fencing —— 旧 worker（fence_epoch 落后）的写一律拒绝
        # （sync 模式无 claim → 守卫恒 False，既有语义不变）。
        if self._claim_fenced(acb):
            return Result(Status.REJECT_STALE, {
                "reason": "claim_fenced", "acb_id": acb.id,
                "claim_epoch": int(getattr(acb, "claim_epoch", 0) or 0),
            })
        resources = {w.resource for w in writes}
        if len(resources) != len(writes):                          # ① 同批重复 resource
            return Result(Status.INVALID_INPUT,
                          {"reason": "duplicate_resource", "resources": sorted(resources)})
        contents = {w.resource: acb.staged.get(w.resource, w.content) for w in writes}
        for w in writes:                                           # ① None 内容
            if contents[w.resource] is None:
                return Result(Status.INVALID_INPUT,
                              {"reason": "none_content", "resource": w.resource})
        for w in writes:                                           # ①′ 内容 fail-fast
            if contents[w.resource] is DELETE:
                continue
            try:                                                   # P1-5：非法内容不得进临界区
                canonical_bytes(contents[w.resource])
            except (TypeError, ValueError) as exc:
                return Result(Status.INVALID_INPUT,
                              {"reason": "bad_content", "resource": w.resource,
                               "error": type(exc).__name__})

        resources |= set(acb.read_set)                             # ③ 读集合一并持锁 → 原子复验
        leases: dict = {}
        events = None
        held: list[str] = []
        try:
            for w in writes:                                       # ② 预留租约（L0）
                base = self.workspace._base_of(acb, w.resource)
                lease = self.intents.reserve(w.resource, acb.id, base, self.lease_ms,
                                             run_id=acb.run_id)
                if lease is None:
                    return Result(Status.DENIED, {"resource": w.resource})
                leases[w.resource] = lease

            held, ok = self.workspace._acquire_all(resources, timeout=self.lock_timeout_s)
            if not ok:
                return Result(Status.UPSTREAM_FAILED, {"reason": "lock_timeout", "held": held})

            for w in writes:                                       # ④ 全部校验（锁内）
                r = self.workspace._validate_locked(acb, w, leases[w.resource], mode)
                if r != Status.VALID:
                    return r

            blob_hashes = {}                              # ④′ 内容先落盘（DELETE 跳过）
            try:
                for w in writes:
                    if contents[w.resource] is DELETE:
                        blob_hashes[w.resource] = ""
                    else:
                        blob_hashes[w.resource] = self.blobs.put(
                            canonical_bytes(contents[w.resource]))
            except (CodecError, TypeError, ValueError) as exc:   # P1-5：编码失败 → INVALID_INPUT
                return Result(Status.INVALID_INPUT,
                              {"reason": "bad_content",
                               "error": type(exc).__name__})

            for w in writes:                                       # ⑤ 提交点前复验租约
                if not self.intents.is_valid(leases[w.resource]):
                    return Result(Status.REJECT_STALE,
                                  {"resource": w.resource,
                                   "reason": "lease_expired_before_commit"})

            records = []
            for w in writes:
                draft, version_before, version_after = self._commit_record(
                    acb, w, blob_hashes[w.resource],
                    deleted=contents[w.resource] is DELETE, mode=mode,
                    idempotency_key=idempotency_key)
                commit_key = self._commit_key(acb, w.resource, blob_hashes[w.resource], mode)
                records.append((w, draft, version_before, version_after, commit_key))

            scope = (acb.run_id, acb.logical_subgoal_id)
            ext_lock = (self._ext_key_lock(idempotency_key)
                        if idempotency_key else _NullLock())
            # scope 临界区**覆盖**「去重检查 → 读 head → 构造 drafts → append_batch」
            # P2：去重检查**必须**在临界区内 —— 否则同 scope 跨资源场景下，去重判断
            # 与 head/append 非原子（并发写各自判定「非去重」→ 重复提交 + 重复收据）。
            # S22：外部键再叠一把**按键**的进程内锁（durable 模式下 receipts flock
            # 已全局串行化；内存模式靠这把锁防同键并发双写）。
            with ext_lock, self._scope_lock(scope):                # 锁序 resource → ext → scope → eventlog
                try:
                    self._refresh_receipts_locked()
                except ReceiptScanFailed:
                    # K-45：拒绝服务（不 append、不改 head、不加 taint）
                    return Result(Status.UPSTREAM_FAILED,
                                  {"reason": "receipt_scan_failed"})
                # S22：外部幂等键 —— 同键同内容直接返回既有收据；同键异内容 CONFLICT
                if idempotency_key:
                    ext = self._dedup_by_external_key(
                        idempotency_key,
                        {(w.resource, blob_hashes[w.resource],
                          contents[w.resource] is DELETE) for w in writes})
                    if ext is _EXT_KEY_CONFLICT:
                        return Result(Status.CONFLICT, {
                            "reason": "idempotency_key_reused_with_different_content",
                            "external_key": idempotency_key})
                    if ext is not None:
                        hits, receipt_ids, seq = ext
                        self._replay_dedup_events(hits)
                        # S22 评审 P1：重放命中也要刷新 base_version/read_set，
                        # 否则门面 `gate` 会拿旧版本比对 → 误判 stale_view。
                        self._refresh_after_commit(acb, records)
                        return Result(Status.OK, {
                            "resources": sorted(blob_hashes),
                            "seq": seq,
                            "receipt_ids": list(receipt_ids),
                            "dedup": True,
                            "external_key": idempotency_key,
                        })
                # 幂等短路（S9 §2.3.3 / P0-1②③）：逐写匹配既有提交事件
                #   键 = (run_id, logical_subgoal_id, resource, content_hash, mode)，
                #   且已提交 blob_hash 必须 == 新内容 hash（内容不同 → 不得短路）。
                dedup_events = [self._dedup_event(acb, w.resource, blob_hashes[w.resource],
                                                  contents[w.resource] is DELETE, mode)
                                for w, _d, _vb, _va, _ck in records]
                # P0-3：is_dedup **只**依据事件匹配；receipt_id 冲突由 append 前断言兜底
                #（若这里也按 receipt_id 判 dedup，守卫将永不可达）。
                is_dedup = [ev is not None for ev in dedup_events]

                if all(is_dedup):
                    # 全命中：必须**补投影**（P0-1①），再刷新读版本，最后返回 OK
                    self._replay_dedup_events(dedup_events)
                    self._refresh_after_commit(acb, records)
                    return Result(Status.OK, {
                        "resources": sorted(blob_hashes),
                        "seq": self._last_committed_seq(dedup_events),
                        "receipt_seq": self._last_receipt_seq(records),
                        "receipt_ids": [ck for *_rest, ck in records],
                        "dedup": True,
                    })

                # P0-3：逐写过滤——已去重的写只补投影，不生成 commit/receipt draft
                to_append = [rec for rec, dedup in zip(records, is_dedup) if not dedup]

                # P2：tainted scope（畸形/重复/断链）拒绝继续扩展（不得接在坏 head 上）
                if scope in self._tainted_scopes:
                    raise ReceiptConflict(
                        f"scope 已隔离（tainted），拒绝扩展：{scope}")

                head = self._receipt_heads.get(scope, GENESIS_HASH)
                secret = self._receipt_secret()
                drafts: list[dict] = []
                issued: list[str] = []
                for w, draft, version_before, version_after, commit_key in to_append:
                    drafts.append(draft)
                    if not self.receipts_enabled:
                        continue
                    # P0-3：append 前断言 receipt_id 不冲突
                    if (commit_key in self._receipt_ids
                            and self._receipt_is_stale(commit_key)):
                        # P1-B 后续：同一内容被后续写覆盖后又写回 = **合法新写**
                        # （dedup 正确地未命中陈旧版本）。此时资源版本已推进到比该
                        # 收据 `version_after` 更新的位置 → 换 receipt_id（派生后缀），
                        # 否则会永久 ReceiptConflict（P2-C）。
                        commit_key = self._derive_receipt_id(commit_key)
                    if commit_key in self._receipt_ids:
                        # P2-C：冲突必须把 scope 加入 `_tainted_scopes`（否则重试同逻辑写
                        # 会**永久** ReceiptConflict，跨进程/重启一样）。隔离后
                        # `verify_receipts` 对该 scope 返回 False，且可由
                        # `reconcile_scope` 显式对账解除。
                        self._tainted_scopes.add(scope)
                        raise ReceiptConflict(
                            f"receipt_id 已签发：{commit_key}（scope={scope}）")
                    receipt = build_receipt(
                        receipt_id=commit_key, run_id=acb.run_id,
                        task_id=acb.task_id,
                        logical_subgoal_id=acb.logical_subgoal_id,
                        resource=w.resource,
                        version_before=version_before, version_after=version_after,
                        prev_receipt_hash=head, secret=secret)
                    drafts.append({
                        "kind": EventKind.RECEIPT_ISSUED,
                        "subject": receipt.receipt_id,     # subject 不能用 resource（§2.3.6）
                        "payload": receipt.to_dict(),      # resource 放 payload
                        "producer": acb.id,
                        "run_id": acb.run_id,
                        "idempotency_key": f"receipt:{receipt.receipt_id}",
                    })
                    head = receipt_hash(receipt)
                    issued.append(receipt.receipt_id)
                try:
                    events = self.event_log.append_batch(drafts, notify=False)   # ⑥ 提交点
                except FencingRejected as exc:
                    # S10b §3.4：旧 epoch 写者被接管 → 拒绝写入（不写入、不 ack、
                    # 不改 head）。未 append → 链未污染，故不 taint scope。
                    return Result(Status.REJECT_STALE, {
                        "reason": "fencing_epoch_rejected",
                        "scope": list(scope), "detail": str(exc)})
                # S12 K-33：`notify=False` 不触发订阅钩子 → 主动推进 per-run 游标
                for ev in events:
                    self._track_run_cursor(ev)
                # ③ head 后置提交：append 成功后才改内存态
                # 水位随 append 同步推进：否则下次锁内刷新会重扫本进程刚 append
                # 的收据，与「已更新的 head」比对 → 误判断链 → 误 taint scope。
                with self._receipt_refresh_lock:
                    if self.receipts_enabled:
                        self._receipt_heads[scope] = head
                        self._receipt_ids.update(issued)
                    for e in events:
                        self._receipt_scan_watermark = max(self._receipt_scan_watermark, e.seq)

            # ⑦ 投影（锁外，可重放）：去重写走版本守卫，新写走 _apply_locked
            for (w, _d, _vb, version_after, _ck), ev in zip(records, dedup_events):
                if ev is not None:
                    self.workspace.apply_event(ev)                 # P0-1①：补投影
                    continue
                prev = self.workspace._current_blob_hash(w.resource)
                self.workspace._apply_locked(
                    acb, w, blob_hashes[w.resource], prev,
                    deleted=contents[w.resource] is DELETE,
                    version_after=version_after)
            self._refresh_after_commit(acb, records)                  # ⑦′ 自写后重读
            commit_seqs = [e.seq for e in events
                           if e.kind == EventKind.RESOURCE_COMMITTED]
            receipt_seqs = [e.seq for e in events
                            if e.kind == EventKind.RECEIPT_ISSUED]
            return Result(Status.OK, {
                "resources": sorted(blob_hashes),
                "seq": commit_seqs[-1],                  # 保持最后一条 RESOURCE_COMMITTED 的 seq
                "receipt_seq": receipt_seqs[-1] if receipt_seqs else None,
                # S22：门面需要收据 id 才能返回 `Receipt`（新增字段，向后兼容）
                "receipt_ids": list(issued),
            })
        finally:
            self.workspace._release_all(held)                      # 先释放资源锁
            for lease in leases.values():
                self.intents.revoke(lease)                         # 再 revoke 租约
            if events is not None:                                 # ⑧ 锁外通知（不掩盖返回值）
                try:
                    self.event_log.notify_events(events)
                except CrashInjected:                 # S9 §2.1：注入崩溃必须穿透，不得被吞
                    raise
                except Exception:  # noqa: BLE001  # 通知失败不得掩盖提交返回值
                    pass

    def _commit_record(self, acb: ACB, w: Write, blob_hash: str,
                       deleted: bool = False, mode: Mode = Mode.COMMIT,
                       idempotency_key: str | None = None
                       ) -> tuple[dict, int, int]:
        """构造 `RESOURCE_COMMITTED` 草稿，并返回 `(draft, version_before, version_after)`。

        版本只算一次：`_apply_locked` 复用 `version_after` 并 assert 相等（S9 §2.3.9）。
        P0-1③：`version_before/version_after` 以**事件日志水位**为准
        （`manifest` 可能因「append 后、投影前崩溃」落后于日志）。
        """
        version_before = self.workspace._effective_version(w.resource)
        version = version_before + 1
        prev_hash = self.workspace._current_blob_hash(w.resource)
        payload = {
            "resource": w.resource,
            "version": version,
            "mode": mode.value,                 # 幂等短路键的一部分（§2.3.3）
            "logical_subgoal_id": acb.logical_subgoal_id,   # P0-2：去重键的 scope 分量
            "acb_id": acb.id,
            "blob_hash": blob_hash,
            "prev_hash": prev_hash,
            "diff": {"prev_hash": prev_hash, "current_hash": blob_hash},
        }
        if deleted:                                  # Delete 哨兵事件（可重放）
            payload["deleted"] = True
        # S22：外部幂等键只在实际使用时写入 —— 不用时事件字节与旧版完全一致。
        if idempotency_key:
            payload["external_idem_key"] = str(idempotency_key)
        draft = {
            "kind": EventKind.RESOURCE_COMMITTED,          # ← 必须与重建过滤一致
            "subject": w.resource,
            "payload": payload,
            "producer": acb.id,
            "run_id": acb.run_id,
            "idempotency_key": f"{acb.run_id}:{acb.id}:{w.resource}",
        }
        return draft, version_before, version

    def _last_committed_seq(self, dedup_events) -> int | None:
        """幂等短路时：已存在 `RESOURCE_COMMITTED` 的最大 seq。"""
        seq = None
        for ev in dedup_events:
            if ev is not None and (seq is None or ev.seq > seq):
                seq = ev.seq
        return seq

    def _last_receipt_seq(self, records) -> int | None:
        """幂等短路时：最后一条已存在 `RECEIPT_ISSUED` 的 seq。"""
        seq = None
        for _w, _draft, _vb, _va, commit_key in records:
            ev = self._receipt_event(commit_key)
            if ev is not None and (seq is None or ev.seq > seq):
                seq = ev.seq
        return seq

    def compensate(self, acb: ACB, applied: list[Write]) -> None:
        """Saga 补偿（§4.2）：批量原子计划 → 释放租约 → 逐条续做。

        - `comp_journal.plan`：**一次 `append_batch`** 写全部条目 + 批次标记（同帧）；
        - `revoke_all`：补偿前先释放租约；
        - 逆序 `_run_one_compensation`：先按 `state` 分支，`deleted` 用版本号判定。
        """
        entries = self.comp_journal.plan(acb, applied)
        self.intents.revoke_all(acb.id)
        by_id = {e.comp_id: w for e, w in zip(entries, applied)}
        for e in reversed(entries):
            self._run_one_compensation(acb, e, by_id.get(e.comp_id))

    def _run_one_compensation(self, acb: ACB, e, w: Write | None = None) -> None:
        """单条补偿（§4.2）：先按 journal 状态分支，再按 hash / 版本号判定。"""
        if w is not None and w.compensated:
            self.compensation_journal.append({"resource": e.resource, "state": "SKIPPED"})
            return
        if e.state == "COMPENSATED":
            self._mark_write(w, "COMPENSATED")
            self.compensation_journal.append({"resource": e.resource, "state": "COMPENSATED"})
            return
        if e.state == "PIVOT":
            self._mark_write(w, "PIVOT")
            self.compensation_journal.append({"resource": e.resource, "state": "PIVOT"})
            return
        exists = self.workspace.get(e.resource) is not None
        current = self.workspace.content_hash(e.resource)
        if e.deleted:
            # 删除类：必须用版本号判定（空 hash 无法区分「不存在」与「删除后」）
            if not exists and self.workspace.version(e.resource) == e.version_before + 1:
                self.comp_journal.mark(e.comp_id, "COMPENSATED")
                self._mark_write(w, "COMPENSATED")
                self.compensation_journal.append({"resource": e.resource, "state": "COMPENSATED"})
                return
            if exists and current != e.applied_hash:
                self.comp_journal.mark(e.comp_id, "PIVOT")
                self._mark_write(w, "PIVOT")
                self.compensation_journal.append({"resource": e.resource, "state": "PIVOT"})
                return
        else:
            if current == e.target_hash:
                self.comp_journal.mark(e.comp_id, "COMPENSATED")
                self._mark_write(w, "COMPENSATED")
                self.compensation_journal.append({"resource": e.resource, "state": "COMPENSATED"})
                return
            if current != e.applied_hash:
                self.comp_journal.mark(e.comp_id, "PIVOT")
                self._mark_write(w, "PIVOT")
                self.compensation_journal.append({"resource": e.resource, "state": "PIVOT"})
                return
        # 执行反写
        rw = Write(e.resource, content=None, version_before=e.version_before,
                   applied_hash=e.applied_hash)
        reverse = self.workspace.compute_reverse_patch(rw)
        if reverse is None:
            self.comp_journal.mark(e.comp_id, "PIVOT")
            self._mark_write(w, "PIVOT")
            self.compensation_journal.append({"resource": e.resource, "state": "PIVOT"})
            return
        acb.staged[e.resource] = DELETE if reverse is DELETE else reverse
        st = self._compensate_one(acb, rw)
        if st == Status.OK:
            self.comp_journal.mark(e.comp_id, "COMPENSATED")
            self._mark_write(w, "COMPENSATED")
            self.compensation_journal.append({"resource": e.resource, "state": "COMPENSATED"})
        elif st == Status.REJECT_COMPENSATE:
            self.comp_journal.mark(e.comp_id, "PIVOT")
            self._mark_write(w, "PIVOT")
            self.compensation_journal.append({"resource": e.resource, "state": "PIVOT"})
        else:
            self.comp_journal.mark(e.comp_id, "FAILED")
            self.compensation_journal.append({"resource": e.resource, "state": "FAILED"})

    @staticmethod
    def _mark_write(w: Write | None, state: str) -> None:
        """兼容旧 `compensation_journal` / `Write.compensated|pivot` 语义。"""
        if w is None:
            return
        if state == "COMPENSATED":
            w.compensated = True
        elif state == "PIVOT":
            w.pivot = True

    def _compensate_one(self, acb: ACB, w: Write) -> Result:
        """单写补偿，带**真正的**有界重试（v1.11 修复）。"""
        for _ in range(acb.retry_budget + 1):
            st = self.commit_batch(acb, [w], Mode.COMPENSATE)
            if st == Status.OK:
                return Result(Status.OK, {})
            if st == Status.REJECT_COMPENSATE:
                return st
        return Result(Status.COMMIT_UNRECOVERABLE, {})

    # ---- 重规划（文档 §5.2④ / §8.1） ------------------------------
    def propagate_failure(self, task_id: str, plan_version: int) -> None:
        """只传播到同一 Plan 版本的后继（v1.10 修复 P0-3；R3 统一 alias）。

        R3：边仍写改名前的旧 id（`T1`），失败/取消来自当前 task（`T1'`）→
        字面比较 `edge["from"] == task_id` 永不命中。这里先用 `task_for` 规范化
        源任务，再按「task_id 相同」或「alias 一跳解析到同一 subgoal」匹配边，
        并把后继也 `CANCELLED`（否则 CANCELLED 不再算前驱满足 → 后继永久阻塞）。
        """
        for plan in self.plans.values():
            task = self.planner.task_for(plan, task_id)
            if not isinstance(task, dict):
                task = plan.get("tasks", {}).get(task_id)
            if not isinstance(task, dict):
                continue
            src_id = task.get("task_id")
            if int(task.get("plan_version", 1) or 1) != int(plan_version):
                # 解析到的当前 task 属于别的版本 → 旧版本事件不得波及新版本分支
                continue
            src_subgoal = task.get("logical_subgoal_id")
            for edge in plan.get("edges", []) or []:
                src, dst = ((edge.get("from"), edge.get("to"))
                            if isinstance(edge, dict) else (edge[0], edge[1]))
                same = src == src_id or (
                    bool(src_subgoal)
                    and self.planner.resolve_alias(src, plan) == src_subgoal)
                if not same:
                    continue
                succ = self.planner.task_for(plan, dst)
                if isinstance(succ, dict) and succ.get("state") not in TASK_TERMINAL:
                    succ["state"] = TaskState.CANCELLED

    # ---- Supervisor 辅助 ------------------------------------------
    def supervisor_note_crash(self, acb: ACB) -> None:
        """Supervisor 心跳超时回收（文档 §5.8③）。

        X1：**先 revoke claim（记录 epoch 水位）再落盘**。否则 save 走
        `_claim_fenced` 时会因「claim 存在但主线程无执行上下文」被拒；而撤销
        水位保证旧 worker（线程本地 epoch 落后）此后所有写一律 `REJECT_STALE`。
        """
        self.intents.revoke_all(acb.id)
        self.bump_epoch()
        run_id = getattr(acb, "run_id", "") or ""
        mark = self.scheduler._epoch_now(run_id or None)
        self.scheduler.revoke_claim(acb.id, watermark=mark)
        # §2.4：落盘（duck-typed 探针无 budget_node 时跳过，保持向后兼容）。
        # 这次快照是**监督者**的权威写入：以撤销水位作为执行上下文 epoch，
        # 使得「本监督者」不被自己刚推进的水位 fence 掉（旧 worker 的后续写
        # 仍会因上下文 epoch 落后而被拒）。
        if hasattr(acb, "budget_node"):
            prev = self._bind_claim_epoch(acb.id, mark)
            try:
                self.checkpoints.save(acb)
            finally:
                self._unbind_claim_epoch(acb.id, prev)

    # ---- 单写者租约 + fencing epoch（S10a §2.5 / §3.4）------------------
    def _lease_path(self):
        root = getattr(self.checkpoints, "root", None)
        if root is None:
            return None
        return os.path.join(str(root), "writer_lease.json")

    def _read_lease(self) -> dict | None:
        path = self._lease_path()
        if not path or not os.path.exists(path):
            return None
        try:
            with open(path, "r", encoding="utf-8") as fh:
                data = loads(fh.read())
        except Exception:  # noqa: BLE001
            return None
        return data if isinstance(data, dict) else None

    def _store_flock(self):
        lock = getattr(self.checkpoints, "_flock", None)
        return lock() if lock is not None else nullcontext()

    def _try_acquire_writer_lease(self) -> bool:
        """获取 durable 单写者租约。

        接管 = 确认旧租约**过期** → `bump_epoch` → 落盘 → 才开始服务（§2.5）。
        旧租约仍有效且属于**其他写者** → 返回 False（拒绝服务，避免双写者）；
        同一写者（同 owner）重入 → 仅续期（同进程多 Harness 共享同一写者身份）。

        K-46：`bump_epoch` 只取独立 `epoch` 锁，**必须在 `store.lock` 之外**
        调用（否则 store(5) → epoch(4) 反向获取 → `LockOrderViolation`，接管崩）。
        """
        path = self._lease_path()
        if not path:
            self._writer_lease_ok = True
            return True
        takeover = False
        with self._store_flock():
            now = self.clock.now_ms()
            cur = self._read_lease()
            if cur is not None:
                exp = int(cur.get("expires_at", 0) or 0)
                same_owner = cur.get("owner") == self._writer_id
                if exp and now < exp:
                    if not same_owner:
                        self._writer_lease_ok = False
                        return False
                    doc = dict(cur)                  # 同写者重入：续期，不 bump epoch
                    doc["expires_at"] = now + int(self.lease_ms)
                    atomic_write(path, dumps(doc).encode("utf-8"))
                    self._writer_lease_ok = True
                    return True
                if not same_owner:
                    takeover = True                  # 旧租约已过期 → 接管
        # 终审 7-2：接管必须**先 bump_epoch（独立 epoch 锁）再写租约**。
        # 旧实现「store.lock 内先落租约 → 锁外 bump」存在窗口：W1 在途写会在
        # bump 之前读到未推进的 durable epoch → fence 通过 → W2 接管后仍写成功。
        epoch = self.bump_epoch() if takeover else self.intents.get_epoch()
        with self._store_flock():
            cur = self._read_lease()
            if cur is not None and cur.get("owner") != self._writer_id:
                exp = int(cur.get("expires_at", 0) or 0)
                if exp and self.clock.now_ms() < exp:
                    # 别的写者在 bump 与落盘之间抢到了租约 → 放弃（epoch 已推进，无害）
                    self._writer_lease_ok = False
                    return False
            doc = {"owner": self._writer_id,
                   "epoch": epoch,
                   "expires_at": self.clock.now_ms() + int(self.lease_ms)}
            parent = os.path.dirname(path)
            if parent:
                os.makedirs(parent, exist_ok=True)
            atomic_write(path, dumps(doc).encode("utf-8"))
            self._writer_lease_ok = True
        return True

    def _renew_writer_lease(self) -> bool:
        """续租（仅当本写者仍是持有者）；过期则走接管。"""
        path = self._lease_path()
        if not path:
            self._writer_lease_ok = True
            return True
        with self._store_flock():
            cur = self._read_lease()
            now = self.clock.now_ms()
            if cur is not None and cur.get("owner") == self._writer_id:
                doc = dict(cur)
                doc["expires_at"] = now + int(self.lease_ms)
                atomic_write(path, dumps(doc).encode("utf-8"))
                self._writer_lease_ok = True
                return True
        # 非本写者：**释放 store.lock 后**再走接管（接管内可能 bump epoch）
        return self._try_acquire_writer_lease()

    def _ensure_writer_lease(self) -> bool:
        """写入口守卫：durable 模式下必须持有/可续租；失败 → 拒绝服务。"""
        if not self._durable:
            return True
        return self._renew_writer_lease()

    def _release_writer_lease(self) -> None:
        """释放本写者的租约（EventLog 关闭 = 写者会话结束）。"""
        path = self._lease_path()
        if not path:
            return
        with self._store_flock():
            cur = self._read_lease()
            if cur is not None and cur.get("owner") == self._writer_id:
                try:
                    os.unlink(path)
                except OSError:
                    pass

    def _epoch_backend(self):
        """run 级 epoch 的持久化后端：WAL 优先（K-46），无 WAL → checkpoint store。"""
        backend = getattr(self, "_wal_epoch_backend", None)
        if backend is not None:
            return backend
        return self.checkpoints

    def _restore_epoch(self) -> int:
        """从持久化存储读 run 级 epoch（RMW 语义，§3.4）。"""
        reader = getattr(self._epoch_backend(), "read_epoch", None)
        epoch = 0
        if callable(reader):
            try:
                epoch = int(reader())
            except Exception:  # noqa: BLE001
                epoch = 0
        self.intents.set_epoch(epoch)
        self.budget.set_epoch(epoch, emit=False)
        return epoch

    def bump_epoch(self, reason: str = "", run_id: str | None = None, *,
                   epoch_scope: str | None = None) -> int:
        """epoch 单调递增（K-43 两层）。

        - `epoch_scope="run"`（构造参数，可用 `epoch_scope=` 覆盖）+ `run_id`
          → **run 层**：只推进该 run 的 epoch（`EpochStore`），只作废该 run 的
          租约 → 其他 run 不受影响；
        - `run_id is None` 或 `epoch_scope="global"` → **全局 writer lease 层**：
          推进 `wal.log.epoch`（接管 fencing），作废全部租约（向后兼容）。

        K-46：全局路径只取独立 epoch 锁，不得在 `store.lock` 内调用。
        """
        scope = self.epoch_scope if epoch_scope is None else epoch_scope
        if run_id is None or scope == "global":
            bumper = getattr(self._epoch_backend(), "bump_epoch", None)
            epoch = None
            if callable(bumper):
                try:
                    epoch = int(bumper())
                except Exception:  # noqa: BLE001
                    epoch = None
            if epoch is None:
                epoch = self.intents.get_epoch() + 1
            self.intents.set_epoch(epoch)
            self.budget.set_epoch(epoch, emit=True)
            return epoch
        # ---- run 层（K-43）：per-run epoch，隔离同 run 内的租约 ----
        epoch = self.budget.bump_epoch(run_id=run_id)     # store 唯一一次推进 + 事件
        self.intents.bump_epoch(run_id, epoch=epoch)      # 作废该 run 租约
        return epoch

    # ---- 端到端运行 -----------------------------------------------
    def _admit_and_register(self, plan: dict, task_id: str, *,
                            required_tools: set[str] | None = None,
                            token=None) -> ACB:
        """S22：准入 + 注册（`dispatch_and_run` 与门面 `begin` 共用）。

        **先登记 plan 再准入**：`_tenant_of`/`_tier_of` 会读 `h.plans[run_id]`，
        顺序反了会让 plan 级 tenant/tier 静默失效（S21 终审 P1）。
        """
        run_id = plan["run_id"]
        self.plans[run_id] = plan
        acb = self.dispatcher.try_admit(plan["tasks"][task_id], run_id,
                                        required_tools=required_tools,
                                        token=token, plan=plan)
        self.acbs[acb.id] = acb
        self.run_coordinator.register(acb)
        return acb

    def dispatch_and_run(self, plan: dict, task_id: str, decisions, *,
                         required_tools: set[str] | None = None,
                         crash_at=None, token=None) -> ACB:
        """唯一生产入口：准入+注册 → run_task → 返回 acb。"""
        acb = self._admit_and_register(plan, task_id,
                                       required_tools=required_tools, token=token)
        if acb.state is not TaskState.ADMITTED:
            return acb
        # F8：原「HITL 判态」分支在此处是死代码（新准入恒为 ADMITTED，永远不会
        # 走到 WAITING_APPROVAL）；恢复路径统一走 `run_task` 的入口判态，故删除。
        self.run_task(acb, decisions, crash_at=crash_at)
        return acb

    def finish(self, acb: ACB, *, outcome: str = "DONE") -> Result:
        """S22：门面 token 的**结束** —— ADMITTED → DONE/FAILED → 结算/归档/释放槽位。

        结算逻辑**全部复用** `_finalize`（不重写）：预留 settle、节点归档、
        槽位释放、后继推进。已终态 → 直接 OK（幂等，不重复结算）。
        """
        if acb.state in TASK_TERMINAL:
            return Result(Status.OK, {"state": acb.state.value, "already": True})
        event = "finish_ok" if str(outcome).upper() == "DONE" else "fail"
        moved = advance(acb, event)
        if moved != Status.OK:
            return Result(Status.INVALID_INPUT, {
                "reason": "illegal_finish", "state": acb.state.value,
                "outcome": outcome})
        self._finalize(acb)
        return Result(Status.OK, {"state": acb.state.value})

    def abort(self, acb: ACB) -> None:
        """S22：门面 token 的**中止** —— 逆序退款/释放（幂等）。

        与 `finish` 的区别：`abort` 是「这笔工作不算数」，预留**全额退还**、
        节点摘除；`finish` 是「算数」，已消费部分留账、节点归档。
        """
        self.dispatcher.abort(acb)

    def run_task(self, acb: ACB, decisions, crash_at=None) -> Status | Result:
        """外部直接调用入口：注册 + push 到调度器 + 执行 + 消费调度器。

        S13d §2.5：
        - `scheduler_mode="sync"`（默认）保持原路径：push → **内联 `_run_one`**
          → `finally drain`（不改动，36 个同步用例零破坏）；
        - `scheduler_mode="pool"`：push → 提交 worker（claim + `_run_one` + drain），
          本线程等待 `_await_completion`（`CrashInjected` 必须重抛）；
        - **worker 内嵌套 `run_task`** → 一律内联（P0-9：池满时父等子会永久死锁），
          并由 `_tls.depth` 防递归。
        """
        self.acbs[acb.id] = acb
        self.run_coordinator.register(acb)
        # S13a P0-3：唯一超时驱动面（每轮调用）；recover 结束也调一次。
        self.tick()
        # 修 B-1：`runtime.run` 会无条件置 RUNNING —— 必须在进入 `_run_one`
        # **之前**判态，否则恢复出的 WAITING_APPROVAL 审批被绕过。
        gate = self._hitl_gate(acb)
        if gate is not None:
            return gate                     # 不 push、不 finalize（槽位已在等待态释放）
        if acb.state in TASK_TERMINAL:
            return self._terminal_status(acb)   # tick 已在本次调用中结算过期审批
        if acb.state is TaskState.INADMISSIBLE and acb.failure == "AGING_TIMEOUT":
            # aging 已判「超时未调度」→ 不得再借 `runtime.run` 无条件置 RUNNING 执行。
            return Result(Status.DEADLINE_EXCEEDED, {"reason": "AGING_TIMEOUT"})
        if self._in_worker():
            # P0-9：worker 内嵌套 → 内联（不入队），否则池满时无 worker 可 pop。
            return self._run_one_inline(acb, decisions, crash_at=crash_at)
        self.scheduler.push(acb.id)            # D10：调度器存 acb.id（不再存 task_id）
        if self.scheduler_mode == "pool":
            return self._run_task_pooled(acb, decisions, crash_at=crash_at)
        try:
            return self._run_one(acb, decisions, crash_at=crash_at)
        finally:
            self._drain_scheduler()            # D12：异常路径（含 CrashInjected）也消费队列

    # ---- S13d §2.5：pool 调度（单一 pop 所有者 + claim/fencing）--------
    def _in_worker(self) -> bool:
        return bool(getattr(self._tls, "in_worker", False))

    def _current_worker_id(self) -> str:
        wid = getattr(self._tls, "worker_id", "")
        return wid or f"local-{threading.get_ident()}"

    def _run_one_inline(self, acb: ACB, decisions=None, crash_at=None):
        """worker 内的嵌套执行（**不入队**）；`_tls.depth` 防递归。

        X4：内联子**必须 claim**（以父 worker 身份）。否则内联执行期间若队列
        中有该子的 ready 键（`_finalize` 的后继 push），另一 worker 可重 claim
        并双跑。claim 不可得（已被他人持有）→ 不内联、记 `inline_claim_held`。
        """
        depth = int(getattr(self._tls, "depth", 0) or 0)
        if depth >= _MAX_INLINE_DEPTH:
            # S14 K-71(a)：这是**终局**，不是可重试的普通拒绝。走状态机显式边
            # （ADMITTED/RUNNING → FAILED）+ `_finalize` 传播，Run 才能 finalize
            # 且事件流能看到原因（旧写法返回 INVALID_INPUT 会被调用方当可重试）。
            self._drain_skipped.append((acb.id, "inline_depth_exceeded"))
            if advance(acb, "inline_depth_exceeded") == Status.OK:
                acb.failure = "INLINE_DEPTH_EXCEEDED"
                self.intents.revoke_all(acb.id)
                self._finalize(acb)
            return Result(Status.GUARD_VIOLATION,
                          {"reason": "INLINE_DEPTH_EXCEEDED",
                           "max_inline_depth": _MAX_INLINE_DEPTH})
        self._tls.depth = depth + 1
        parent = getattr(self._tls, "running_acb", None)
        claim = None
        if self.scheduler_mode == "pool":
            claim = self.scheduler.claim(acb.id, self._current_worker_id())
            if claim is None:
                self._drain_skipped.append((acb.id, "inline_claim_held"))
                self._tls.depth = depth
                return Result(Status.REJECT_STALE, {"reason": "inline_claim_held"})
            # 审计：内联子记录父 ACB（不参与 fence 判定）。
            if parent is not None and parent is not acb:
                acb.spec["inline_parent"] = parent.id
        try:
            if claim is not None:
                with self.claim_scope(acb, claim):
                    return self._run_one(acb, decisions, crash_at=crash_at)
            return self._run_one(acb, decisions, crash_at=crash_at)
        finally:
            self._tls.depth = depth
            if claim is not None:
                self.scheduler.release_claim(acb.id, self._current_worker_id())

    def _run_task_pooled(self, acb: ACB, decisions, crash_at):
        """提交一个 worker 并等待；**不在本线程 drain**（单一 pop 所有者 = worker）。"""
        if self._pool is None:                 # 防御：mode=pool 必有池
            return self._run_one(acb, decisions, crash_at=crash_at)
        worker_id = f"W{next(self._worker_ids)}-{acb.id}"
        future = self._pool.submit(self._worker_main, acb, decisions,
                                   crash_at, worker_id)
        return self._await_completion(future)

    def _worker_main(self, acb: ACB, decisions, crash_at, worker_id: str):
        """worker 入口：claim → `_run_one` → drain（异常时不 drain，防双跑）。"""
        self._tls.in_worker = True
        self._tls.worker_id = worker_id
        self._tls.depth = 0
        crashed = False
        try:
            claim = self.scheduler.claim(acb.id, worker_id)
            if claim is None:
                # 同一 ACB 的有效租约归其他 worker → 绝不双跑。
                self._drain_skipped.append((acb.id, "claim_held"))
                return Status.DEADLINE_EXCEEDED
            try:
                # X1：fence 的 epoch 绑在**本线程执行上下文**，不写共享 ACB。
                with self.claim_scope(acb, claim):
                    return self._run_one(acb, decisions, crash_at=crash_at)
            finally:
                self.scheduler.release_claim(acb.id, worker_id)
        except BaseException:
            # CrashInjected（BaseException）必须穿透到等待侧；且**不得再 drain**
            # （否则重复 ready 键会在崩溃后被再次消费 → 双跑）。
            crashed = True
            raise
        finally:
            try:
                if not crashed:
                    # 单一 pop 所有者：worker 自己消费队列（`in_worker` 仍为 True，
                    # 期间嵌套 run_task 继续内联 → 无池满死锁）。
                    self._drain_scheduler()
            finally:
                self._tls.in_worker = False
                self._tls.depth = 0
                self._tls.worker_id = ""

    def _await_completion(self, future, *, timeout_s: float | None = None):
        """等待 worker 完成：超时可取消、异常（含 `BaseException`）在等待侧重抛。

        `ThreadPoolExecutor` 会把 `BaseException` 收进 `Future`；不调 `result()`
        则既不抛也不结束。这里统一 `result(timeout)`，并保证 `CrashInjected`
        不被吞掉（S13d §2.5 / B-3）。
        """
        timeout = self.pool_timeout_s if timeout_s is None else timeout_s
        try:
            return future.result(timeout=timeout)
        except TimeoutError:
            future.cancel()                    # 可取消（已运行则为 best-effort）
            self._drain_skipped.append(("", "worker_timeout"))
            raise
        finally:
            if not future.done():
                future.cancel()

    # ---- S13c §2.4：spawn_child -------------------------------------
    def _spawn_depth_of(self, acb: ACB) -> int:
        """父 ACB 的嵌套深度（根 = 0）。

        C8：spawn 时把 `depth` 写进子 payload（`spec["spawn"]["depth"]`），
        恢复后**优先读持久化值**（不依赖内存父链）；内存链断掉（祖先缺失且
        无持久化 depth）→ **fail-closed** 返回超过上限的值（绝不返回 1 让
        great-grandchild 蒙混通过）。
        """
        info = acb.spec.get("spawn") if isinstance(acb.spec, dict) else None
        if isinstance(info, dict):
            depth = info.get("depth")
            if isinstance(depth, int) and not isinstance(depth, bool) and depth >= 0:
                return depth
        depth = 0
        cur = acb
        seen: set[str] = set()
        while getattr(cur, "parent_id", ""):
            if depth >= 64 or cur.id in seen:
                return self.spawn_depth + 1
            seen.add(cur.id)
            depth += 1
            parent = self.acbs.get(cur.parent_id)
            if parent is None:
                # 父链断链：无法证明未越界 → 按上限拒绝（fail-closed）。
                return self.spawn_depth + 1
            cur = parent
        return depth

    def _spawn_reject(self, parent: ACB | None, child_id: str, run_id: str,
                      task_id: str, reason: str) -> ACB:
        """拒绝 spawn：返回明确 failure 的 ACB（父不变、零残留、不抛）。"""
        acb = ACB(id=child_id, run_id=run_id, task_id=task_id)
        acb.state = TaskState.INADMISSIBLE
        acb.failure = reason
        acb.parent_id = parent.id if parent is not None else ""
        acb.spec["spawn_error"] = reason
        self._drain_skipped.append(
            (parent.id if parent is not None else run_id, f"spawn_{reason}"))
        return acb

    def _spawn_rollback(self, child_id: str, node, resv, slot: bool) -> None:
        """spawn 失败逆序回滚（预留 → 节点 → 槽位）。

        C1：回滚链路与 `Dispatcher._rollback` 一致 —— 用 **`release`**（不是
        `settle` + 手 pop），并放在 `budget.suppressed()` 内部（失败事务零事件
        残留，由调用方 `discard_pending`）。非 `CrashInjected` 的异常不得中断
        后续回滚（槽位/节点仍必须回收）。
        """
        if resv is not None:
            try:
                self.budget.release(resv.reservation_id)
                self.budget._reservations.pop(resv.reservation_id, None)
                if node is not None:
                    node.reservations.pop(resv.reservation_id, None)
            except Exception:  # noqa: BLE001
                pass
        if node is not None:
            try:
                if not self.budget.remove_node(node.node_id, owner=child_id):
                    self.budget.archive_node(node.node_id)
            except Exception:  # noqa: BLE001
                pass
        if slot:
            self.slots.release(child_id)

    def _save_strict(self, acb: ACB) -> str:
        """C1：关键落盘（spawn 两阶段）**不得吞异常**。

        `_safe_save` 的静默 `pass` 会让「崩溃可续做」失去保证；这里让普通
        存储错误也冒泡，调用方据此回滚 / 由恢复路径按 `children_pending` 收口。
        """
        return self.checkpoints.save(acb)

    def _parent_child_alloc(self, parent_acb: ACB) -> tuple[int, int]:
        """C7：父的任务子预算配额与已用额（`child_alloc` / `child_alloc_used`）。

        未显式声明（或缺省 0）→ 用 `max_fanout * min_budget_per_child` 推导；
        已用额随父 ACB 持久化，spawn 成功时按 `slice` 记账、失败/孤儿回收回滚。
        """
        raw = parent_acb.spec.get("child_alloc")
        if isinstance(raw, bool) or not isinstance(raw, int) or raw <= 0:
            raw = max(self.max_fanout, 1) * self.min_budget_per_child
        used = parent_acb.spec.get("child_alloc_used", 0)
        if isinstance(used, bool) or not isinstance(used, int) or used < 0:
            used = 0
        return int(raw), int(used)

    def _child_ids_for_cancel(self, acb: ACB) -> set[str]:
        """该 ACB 已知的子 id（内存 + 持久化 + pending）。"""
        ids: set[str] = set()
        for child in list(getattr(acb, "children", []) or []):
            if isinstance(child, ACB):
                ids.add(child.id)
        for entry in list(getattr(acb, "children_ids", []) or []):
            try:
                ids.add(str(entry[0]))
            except (TypeError, IndexError):
                continue
        for cid in (getattr(acb, "children_pending", None) or {}):
            ids.add(str(cid))
        return ids

    def _ref_for_child(self, child_id: str, info: dict | None = None):
        """C1：按 `child_id` 找回子快照（ref=None 的崩溃窗口）。

        先查 per-ACB 指针，再退到 `(run_id, task_id, logical_subgoal)` 指针。
        """
        getter = getattr(self.checkpoints, "latest_ref_for_acb", None)
        if callable(getter):
            try:
                ref = getter(child_id)
            except Exception:  # noqa: BLE001
                ref = None
            if ref:
                return ref
        info = info if isinstance(info, dict) else {}
        getter = getattr(self.checkpoints, "latest_ref_for_task", None)
        if callable(getter) and info.get("task_id"):
            try:
                return getter(info.get("run_id") or "",
                              info.get("task_id") or "",
                              info.get("logical_subgoal_id") or "")
            except Exception:  # noqa: BLE001
                return None
        return None

    def _settle_orphan_child(self, parent_acb: ACB, child_id: str,
                             info: dict | None = None) -> bool:
        """C1：spawn 崩溃窗口的孤儿结算（父预算节点上的 ACTIVE 预留）。

        - 找到 `B-{child_id}` 节点（或 owner == child_id 的节点）→ 结算其子树
          全部 ACTIVE 预留 → 归档 / 摘除；
        - **必留痕**：写 `SPAWN_ORPHAN_RECLAIMED` 事件 + `_drain_skipped`，
          绝不静默 `cancel_skip_missing_ref`；
        - 回滚父的 `child_alloc_used`（该子从未成功产生）。
        """
        info = info if isinstance(info, dict) else {}
        node = None
        parent_node = getattr(parent_acb, "budget_node", None)
        candidate_ids = [f"B-{child_id}"]
        if parent_node is not None:
            for child in list(getattr(parent_node, "children", []) or []):
                if (getattr(child, "owner", "") == child_id
                        or child.node_id in candidate_ids):
                    node = child
                    break
        for node_id in candidate_ids:
            if node is not None:
                break
            node = self.budget._nodes.get(node_id)
        if node is None:
            for candidate in list(self.budget._nodes.values()):
                if getattr(candidate, "owner", "") == child_id:
                    node = candidate
                    break
        reclaimed = False
        if node is not None:
            with self.budget.suppressed():
                for sub in list(self.budget._subtree_nodes(node)):
                    for rid, resv in list(sub.reservations.items()):
                        if resv.state == "ACTIVE":
                            self.budget.settle(rid)
                if not self.budget.archive_node(node.node_id):
                    self.budget.remove_node(node.node_id)
            self.budget.flush_pending()
            reclaimed = True
        self.slots.release(child_id)
        # 注：`child_alloc_used` 只在 phase ③ 成功后记账，孤儿从未计入 → 不回滚。
        self.event_log.append(
            EventKind.SPAWN_ORPHAN_RECLAIMED, parent_acb.id,
            {"child_id": child_id,
             "node_id": node.node_id if node is not None else None,
             "task_id": info.get("task_id"),
             "reason": "SPAWN_ORPHAN",
             "reclaimed": reclaimed},
            producer=parent_acb.id, run_id=parent_acb.run_id)
        self._drain_skipped.append((child_id, "spawn_orphan_reclaimed"))
        return reclaimed

    def _reconcile_children_refs(self, acb: ACB) -> None:
        """C1：`recover` 时补全 `children_ids` 里 ref=None 的持久化子。

        能按 `child_id` 找回 → 回填 ref；完全找不到 → 孤儿结算（留痕后移除条目）。
        """
        pending_ids = set(getattr(acb, "children_pending", None) or {})
        new_entries: list = []
        changed = False
        for entry in list(getattr(acb, "children_ids", []) or []):
            try:
                cid = str(entry[0])
                ref = entry[1] if len(entry) > 1 else None
            except (TypeError, IndexError):
                changed = True
                continue
            if cid in pending_ids:
                new_entries.append((cid, ref))       # 交给 pending 对账
                continue
            if ref:
                new_entries.append((cid, ref))
                continue
            found = self._ref_for_child(cid)
            if found:
                new_entries.append((cid, found))
            else:
                self._settle_orphan_child(acb, cid)
            changed = True
        if changed:
            acb.children_ids = new_entries
            self._safe_save(acb)

    def _reconcile_children_pending(self, acb: ACB) -> None:
        """C1：`recover` 时对账第一阶段登记（宁可留痕，绝不静默）。

        找到子快照 → 升级为 `children_ids=(id, ref)`；找不到 → 孤儿结算。
        """
        pending = acb.children_pending
        if not isinstance(pending, dict) or not pending:
            return
        changed = False
        promoted_slices = 0
        for child_id, info in list(pending.items()):
            info = info if isinstance(info, dict) else {}
            ref = self._ref_for_child(str(child_id), info)
            promoted = False
            if ref:
                try:
                    data = self.checkpoints.load_strict(ref)
                except Exception:  # noqa: BLE001
                    try:
                        data = self.checkpoints.load(ref)
                    except Exception:  # noqa: BLE001
                        data = None
                if isinstance(data, dict) and \
                        (data.get("parent_id") or "") == acb.id:
                    promoted = True
                    replaced = False
                    new_ids: list = []
                    for entry in list(acb.children_ids):
                        if isinstance(entry, (list, tuple)) and \
                                str(entry[0]) == str(child_id):
                            new_ids.append((str(child_id), ref))
                            replaced = True
                        else:
                            new_ids.append(entry)
                    if not replaced:
                        new_ids.append((str(child_id), ref))
                    acb.children_ids = new_ids
            if not promoted:
                self._settle_orphan_child(acb, str(child_id), info)
            else:
                slice_amt = info.get("slice")
                if isinstance(slice_amt, int) and not isinstance(slice_amt, bool):
                    promoted_slices += slice_amt
            acb.children_pending.pop(child_id, None)
            changed = True
        if promoted_slices:
            # phase ① 时未记账（只有 ③ 成功才记账）→ 恢复出的成功子补记。
            used = acb.spec.get("child_alloc_used", 0)
            if not isinstance(used, int) or isinstance(used, bool):
                used = 0
            acb.spec["child_alloc_used"] = used + promoted_slices
        if changed:
            self._safe_save(acb)

    def spawn_child(self, parent_acb: ACB, spec: dict,
                    plan: dict | None = None) -> ACB:
        """S13c §2.4：派生一个子 ACB（能力子集 + **无双重预留** + 两阶段落盘）。

        预算的唯一写法：`create_node(B-{child}, limits=slice, parent_id=父节点)`
        + `reserve(child_node, min)` → 链式传播只计一次（**禁止**再
        `reserve(parent_node)`，否则父 `subtree_reserved` 2×）。父准入时
        `footprint.child_alloc` 已把子份额计入父 limits。

        两阶段：① 先写父 `children_pending={child_id: {...}}` 并**严格**落盘
        （可重建；失败/崩溃后按 child_id 找回或孤儿结算）→ ② 建子（失败/异常
        逆序回滚）→ ③ 成功后清 pending、按 `child_id` 精确回填
        `children_ids=(id, ref)` 再落盘。
        """
        spec = dict(spec or {})
        run_id = spec.get("run_id") or parent_acb.run_id
        task_id = spec.get("task_id") or f"C{next(self._spawn_counter)}"
        child_id = spec.get("child_id") or \
            f"A-{run_id}-{task_id}-spawn-{next(self._spawn_counter)}"
        min_budget = spec.get("min_budget")
        if isinstance(min_budget, bool) or not isinstance(min_budget, int) \
                or min_budget < 0:
            return self._spawn_reject(parent_acb, child_id, run_id, task_id,
                                      "INVALID_INPUT")

        with self._transition_lock(parent_acb):
            if parent_acb.state in TASK_TERMINAL:
                return self._spawn_reject(parent_acb, child_id, run_id, task_id,
                                          "SPAWN_PARENT_TERMINAL")
            if child_id in self.acbs or child_id in {
                    str(e[0]) for e in parent_acb.children_ids if e}:
                return self._spawn_reject(parent_acb, child_id, run_id, task_id,
                                          "SPAWN_DUPLICATE_CHILD")
            if child_id in (parent_acb.children_pending or {}):
                return self._spawn_reject(parent_acb, child_id, run_id, task_id,
                                          "SPAWN_DUPLICATE_CHILD")
            parent_depth = self._spawn_depth_of(parent_acb)
            if parent_depth + 1 > self.spawn_depth:
                return self._spawn_reject(parent_acb, child_id, run_id, task_id,
                                          "SPAWN_DEPTH_EXCEEDED")
            # C8：fanout 只计**未终态**子（含 ref=None 的 pending），终态子不占名额。
            live_ids = self._live_child_ids(parent_acb)
            if len(live_ids) >= self.max_fanout:
                return self._spawn_reject(parent_acb, child_id, run_id, task_id,
                                          "SPAWN_FANOUT_EXCEEDED")
            if min_budget < self.min_budget_per_child:
                return self._spawn_reject(parent_acb, child_id, run_id, task_id,
                                          "CHILD_BUDGET_TOO_SMALL")
            parent_node = parent_acb.budget_node
            if parent_node is None:
                return self._spawn_reject(parent_acb, child_id, run_id, task_id,
                                          "SPAWN_NO_BUDGET_NODE")
            if min_budget > parent_node.remaining:
                return self._spawn_reject(parent_acb, child_id, run_id, task_id,
                                          "CHILD_BUDGET_EXHAUSTED")
            parent_token = self._effective_token(parent_acb)
            resolved_parent = self.policy.resolve_token(parent_token)
            if resolved_parent is None:
                return self._spawn_reject(parent_acb, child_id, run_id, task_id,
                                          "SPAWN_NO_CAPABILITY")
            requested = spec.get("scope")
            if requested is None:
                requested = spec.get("capability")
            if requested is None:                     # 未声明 → 继承父的全量 scope
                requested = Scope(tools=set(resolved_parent.tools),
                                  paths=set(resolved_parent.paths),
                                  network=set(resolved_parent.network),
                                  data_class=resolved_parent.data_class)
            try:
                child_token = attenuate_token(resolved_parent, requested, child_id)
            except CapabilityAttenuationError:
                return self._spawn_reject(parent_acb, child_id, run_id, task_id,
                                          "SPAWN_CAPABILITY_DENIED")
            # 追溯锚定到**父 ACB id**（生产 token 的 agent_id 即父 id；测试夹具的
            # 通配 token agent_id="*" 也统一到 id，否则网关 lineage 校验必红）。
            # C6：父能力快照已在 `attenuate_token` 冻结，lineage 不依赖全局 dict。
            child_token.parent_id = parent_acb.id
            # 父 token 必须可被追溯（`_traceable` 按 `_tokens[parent_id]` 查）。
            if self.policy.token(parent_acb.id) is None and parent_token is not None:
                self.policy._tokens[parent_acb.id] = parent_token

            logical_subgoal = spec.get("logical_subgoal_id") or f"S-{task_id}"
            try:
                retry_budget = int(spec.get("retry_budget", 3) or 3)
                requested_slice = spec.get("budget")
                if requested_slice is None:
                    requested_slice = min_budget + retry_budget + 1
                requested_slice = int(requested_slice)
            except (TypeError, ValueError):
                return self._spawn_reject(parent_acb, child_id, run_id, task_id,
                                          "INVALID_INPUT")
            # C7：slice 受父 `child_alloc_remaining` 约束（不写死、不超配）。
            alloc, used = self._parent_child_alloc(parent_acb)
            alloc_remaining = max(0, alloc - used)
            slice_limits = min(requested_slice, alloc_remaining)
            if slice_limits < min_budget:
                return self._spawn_reject(parent_acb, child_id, run_id, task_id,
                                          "CHILD_BUDGET_EXHAUSTED")
            # 子的 child_alloc 由父剩余额推导（不写死 0，孙辈才有预算）。
            child_alloc = max(0, slice_limits - min_budget)

            child = ACB(id=child_id, run_id=run_id, task_id=task_id,
                        logical_subgoal_id=logical_subgoal)
            child.parent_id = parent_acb.id
            child.capability = child_token
            child.retry_budget = retry_budget
            child.spec["plan_version"] = int((plan or {}).get("version", 1) or 1)
            child.spec["spawn"] = {"parent_id": parent_acb.id,
                                   "min_budget": min_budget,
                                   "slice": slice_limits,
                                   "depth": parent_depth + 1}
            child.spec["child_alloc"] = child_alloc
            child.spec["min_budget"] = min_budget
            child.spec["parent_budget"] = parent_node.node_id
            child.spec["child_alloc_used"] = 0

            # ① 两阶段落盘第一阶段：登记可重建的 `children_pending`（ref 尚未产生）。
            pending_info = {
                "task_id": task_id,
                "min_budget": min_budget,
                "slice": slice_limits,
                "run_id": run_id,
                "logical_subgoal_id": logical_subgoal,
                "depth": parent_depth + 1,
                "parent_id": parent_acb.id,
            }
            parent_acb.children_pending[child_id] = pending_info
            self._save_strict(parent_acb)

            node = None
            resv = None
            slot = False
            failure = None
            # ② 建子（槽位 → 子预算节点 → 预留）。**任何**异常（含注入崩溃）
            #    都必须回滚后重抛；普通拒绝则回滚并返回明确 failure。
            try:
                with self.budget.suppressed():
                    if not self.slots.try_acquire(child_id):
                        failure = "SPAWN_NO_SLOT"
                    else:
                        slot = True
                        node = self.budget.create_node(
                            f"B-{child_id}", slice_limits,
                            parent_id=parent_node.node_id, run_id=run_id,
                            # S21 K-97：子节点标签必须与**实际挂载点**一致，
                            # 否则子任务的预留不计入租户计量。
                            tenant=_tenant_of_node_id(parent_node.node_id))
                        if node is None:
                            failure = "SPAWN_BUDGET_UNAVAILABLE"
                        else:
                            node.owner = child_id
                            resv = self.budget.reserve(node.node_id, min_budget)
                            if resv is None:
                                failure = "CHILD_BUDGET_EXHAUSTED"
                    if failure is not None:
                        raise _SpawnAbort(failure)
                self.budget.flush_pending()
            except BaseException as exc:  # CrashInjected 必须回滚后重抛
                with self.budget.suppressed():
                    self._spawn_rollback(child_id, node, resv, slot)
                self.budget.discard_pending()
                parent_acb.children_pending.pop(child_id, None)
                self._safe_save(parent_acb)
                if isinstance(exc, _SpawnAbort):
                    return self._spawn_reject(parent_acb, child_id, run_id,
                                              task_id, str(exc))
                raise

            child.reservation_ids = [resv.reservation_id]
            child.budget_node = node
            child.state = TaskState.ADMITTED
            self.acbs[child.id] = child
            self.run_coordinator.register(child)
            self._register_admission(child)
            self._index_child(child.id, parent_acb.id)   # X6：跨 run 子反扫索引
            if plan is not None:
                plan.setdefault("tasks", {})[task_id] = {
                    "task_id": task_id,
                    "logical_subgoal_id": logical_subgoal,
                    "footprint": {"min_budget": min_budget,
                                  "child_alloc": child_alloc,
                                  "parent_budget": parent_node.node_id},
                    "merge_policy": {},
                    "state": TaskState.ADMITTED,
                    "plan_version": child.spec["plan_version"],
                }
            self._safe_save(child)
            # ③ 成功后转正：清 pending、按 child_id 精确回填 ref，再落盘。
            ref = self._ref_for_child(child.id, pending_info)
            parent_acb.children_pending.pop(child_id, None)
            # 精确移除同 id 的旧条目（绝不 `pop()` 末尾，避免并发/重复写错位）。
            parent_acb.children_ids = [
                e for e in parent_acb.children_ids if str(e[0]) != str(child.id)]
            parent_acb.children_ids.append((child.id, ref))
            parent_acb.children.append(child)
            parent_acb.spec["child_alloc_used"] = used + slice_limits
            self._save_strict(parent_acb)
            return child

    def _live_child_ids(self, parent_acb: ACB) -> set[str]:
        """C8：未终态子 id（内存 / 持久化 / pending）。

        终态子不占 `max_fanout` 名额；内存缺失的持久化子一律按未终态计
        （保守，宁可拒绝也不超界）。
        """
        live: set[str] = set()
        for child in list(getattr(parent_acb, "children", []) or []):
            if isinstance(child, ACB) and child.state not in TASK_TERMINAL:
                live.add(child.id)
        for entry in list(getattr(parent_acb, "children_ids", []) or []):
            try:
                cid = str(entry[0])
            except (TypeError, IndexError):
                continue
            child = self.acbs.get(cid)
            if child is None or child.state not in TASK_TERMINAL:
                live.add(cid)
        for cid in (getattr(parent_acb, "children_pending", None) or {}):
            live.add(str(cid))
        return live

    def _load_persisted_child(self, child_id: str, ref: str | None):
        """C3：按 ref 加载子快照为 ACB；不可读 → `None`。"""
        ref = ref or self._ref_for_child(child_id)
        if not ref:
            return None
        try:
            data = self.checkpoints.load_strict(ref)
        except Exception:  # noqa: BLE001
            try:
                data = self.checkpoints.load(ref)
            except Exception:  # noqa: BLE001
                return None
        if not isinstance(data, dict):
            return None
        acb = ACB(id=str(data.get("acb_id") or child_id),
                  run_id=data.get("run_id", ""),
                  task_id=data.get("task_id", ""),
                  logical_subgoal_id=data.get("logical_subgoal_id", ""))
        try:
            acb.state = TaskState(data.get("state", TaskState.PLANNED.value))
        except ValueError:
            acb.state = TaskState.PLANNED
        acb.parent_id = data.get("parent_id", "") or ""
        acb.failure = data.get("failure")
        acb.result_ref = ref
        acb.budget_node = self.budget._nodes.get(data.get("budget_node_id"))
        acb.reservation_ids = list(data.get("reservation_ids") or [])
        acb.spec = dict(data.get("spec") or {})
        return acb

    def result_envelope(self, parent_acb: ACB) -> ResultEnvelope:
        """S13c §2.4：汇总直接子的结构化回传（C3 修正状态归类）。

        - `DONE_PARTIAL` 只在「children 非空 ∧ **全部子终态** ∧ ≥1 DONE ∧
          ≥1 FAILED/CANCELLED」时产生；
        - 仍有 pending 子 → `RUNNING`（**不 finalize**）；
        - `CANCELLED` 单独归类（不再混进 pending；全取消 → CANCELLED）；
        - `children_ids` 有而内存无 → 按 ref 加载；加载不到 → `NEEDS_RECONCILIATION`
          （**绝不**当成 DONE）；
        - 未完成的两阶段 spawn（`children_pending` 且找不回子）→
          `NEEDS_RECONCILIATION`。
        """
        seen: set[str] = set()
        children: list[ACB] = []
        unresolved = False
        for child in list(getattr(parent_acb, "children", []) or []):
            if isinstance(child, ACB) and child.id not in seen:
                seen.add(child.id)
                children.append(child)
        for entry in list(getattr(parent_acb, "children_ids", []) or []):
            try:
                cid, ref = str(entry[0]), (entry[1] if len(entry) > 1 else None)
            except (TypeError, IndexError):
                unresolved = True
                continue
            if cid in seen:
                continue
            child = self.acbs.get(cid)
            if child is None:
                child = self._load_persisted_child(cid, ref)
            if child is None:
                unresolved = True
                continue
            seen.add(cid)
            children.append(child)
        for cid in (getattr(parent_acb, "children_pending", None) or {}):
            cid = str(cid)
            if cid in seen:
                continue
            child = self._load_persisted_child(cid, None)
            if child is None:
                unresolved = True
                continue
            seen.add(cid)
            children.append(child)

        coverage: dict = {}
        artifacts: list = []
        conflicts: list = []
        budget_usage = 0
        done = failed = cancelled = pending = 0
        partial_child = False
        for child in children:
            coverage[child.task_id] = child.state.value
            node = child.budget_node
            if node is not None:
                budget_usage += int(getattr(node, "consumed", 0) or 0)
            else:
                try:
                    data = (self.checkpoints.load_strict(child.result_ref)
                            if child.result_ref else {})
                except Exception:  # noqa: BLE001
                    data = {}
                budget_usage += int(
                    (data.get("budget_consumed") or 0)
                    + (data.get("budget_folded_consumed") or 0))
            artifacts.append({"acb_id": child.id, "task_id": child.task_id,
                              "result_ref": child.result_ref})
            if child.state is TaskState.DONE:
                done += 1
            elif child.state is TaskState.DONE_PARTIAL:
                done += 1
                partial_child = True
            elif child.state is TaskState.CANCELLED:
                cancelled += 1
            elif child.state in (TaskState.FAILED, TaskState.DEAD_LETTER,
                                 TaskState.QUARANTINED):
                failed += 1
                conflicts.append({"acb_id": child.id,
                                  "reason": child.failure or child.state.value})
            else:
                pending += 1

        total = len(children)
        terminal = done + failed + cancelled
        if unresolved:
            status = "NEEDS_RECONCILIATION"
        elif pending:
            status = "RUNNING"
        elif total == 0 or terminal == total and done == total and not partial_child:
            status = "DONE"
        elif terminal == total and done > 0 and (failed or cancelled or partial_child):
            status = "DONE_PARTIAL"
        elif terminal == total and failed:
            status = "FAILED"
        elif terminal == total and cancelled:
            status = "CANCELLED"
        else:                                # 防御：不可达（非终态已在 pending 分支）
            status = "RUNNING"
        return ResultEnvelope(status=status, coverage=coverage,
                              artifacts=artifacts, conflicts=conflicts,
                              budget_usage=budget_usage)

    def apply_result_envelope(self, parent_acb: ACB) -> ResultEnvelope:
        """父汇总落状态（C3：终态 guard + 幂等）。

        - 父已是终态 → 直接返回当前 envelope（绝不覆盖 DONE_PARTIAL / 二次结算）；
        - `DONE_PARTIAL` → 走状态机边（RUNNING→DONE_PARTIAL）或兼容赋值 + `_finalize`
          （归档预算节点、释放槽位）；
        - `RUNNING` / `NEEDS_RECONCILIATION` → **不 finalize**。
        """
        env = self.result_envelope(parent_acb)
        if parent_acb.state in TASK_TERMINAL:
            return env
        if env.status == "DONE_PARTIAL":
            if advance(parent_acb, "partial") != Status.OK:
                parent_acb.state = TaskState.DONE_PARTIAL
            self._finalize(parent_acb)
        return env

    @staticmethod
    def _approval_slot(acb: ACB) -> tuple[str | None, int | None]:
        """取 `(approval_id, deadline_ms)`；任一不可用 → `None`（fail-closed 依据）。"""
        pending = acb.hitl_pending if isinstance(acb.hitl_pending, dict) else None
        if pending is None:
            return (None, None)
        approval_id = pending.get("approval_id")
        deadline = pending.get("deadline_ms")
        if not isinstance(approval_id, str) or not approval_id:
            approval_id = None
        if isinstance(deadline, bool) or not isinstance(deadline, int):
            deadline = None
        return (approval_id, deadline)

    def _record_approval(self, acb: ACB, decision: str, reason: str) -> None:
        """F8/F11：结算审批 —— 落 `hitl_outcome`、清 `hitl_pending`、写审计事件。

        批准/拒绝/超时/非法/状态丢失**全部** append `APPROVAL_RESOLVED`
        （payload 带 `approval_id` + `decision` + `reason`），避免只剩 RAISED。
        """
        approval_id, _ = self._approval_slot(acb)
        acb.hitl_outcome = {
            "decision": decision,
            "reason": reason,
            "approval_id": approval_id,
        }
        acb.hitl_pending = None
        self.event_log.append(
            EventKind.APPROVAL_RESOLVED, acb.task_id,
            {"approval_id": approval_id, "decision": decision, "reason": reason},
            producer=acb.id, run_id=acb.run_id)

    def _hitl_gate(self, acb: ACB) -> Status | Result | None:
        """S13a §2.2：`WAITING_APPROVAL` 进入执行前的三态判态（fail-closed）。

        - `_UNSET` → `NEEDS_APPROVAL`，保持等待（不 push / 不 finalize）；
        - `{"approved": True}` → `advance("resume")` 继续（重占槽位）；
        - `None` / `{"approved": False}` → FAILED(APPROVAL_DENIED) + 结算；
        - 其余判词（str / 非 dict / approved 非 bool）→ FAILED(APPROVAL_INVALID)；
        - `hitl_pending` 不可用（无 `approval_id` / 无 `deadline_ms`）→
          FAILED(APPROVAL_STATE_LOST)（F2：绝不允许永久挂起）。

        `tick` 与本函数在 `HITL._lock` 内完成「读 verdict + 翻转状态」的 CAS。
        """
        if acb.state is not TaskState.WAITING_APPROVAL:
            return None
        with self.hitl._lock:
            if acb.state is not TaskState.WAITING_APPROVAL:
                return None
            approval_id, deadline = self._approval_slot(acb)
            if approval_id is None or deadline is None:
                advanced = advance(acb, "timeout")
                if advanced != Status.OK:
                    return self._terminal_status(acb)
                acb.failure = "APPROVAL_STATE_LOST"
                self._record_approval(acb, "lost", "APPROVAL_STATE_LOST")
                decision = "lost"
            else:
                kind = classify_verdict(self.hitl.resolve(acb))
                if kind == "unset":
                    return Status.NEEDS_APPROVAL
                if kind == "approved":
                    acb.hitl_result = {"approved": True}
                    advanced = advance(acb, "resume")
                    if advanced != Status.OK:
                        return self._terminal_status(acb)
                    self._record_approval(acb, "approved", "APPROVED")
                    decision = "approved"
                else:
                    advanced = advance(acb, "timeout")
                    if advanced != Status.OK:
                        return self._terminal_status(acb)
                    acb.failure = ("APPROVAL_DENIED" if kind == "denied"
                                   else "APPROVAL_INVALID")
                    self._record_approval(
                        acb, "denied" if kind == "denied" else "invalid",
                        acb.failure)
                    decision = "denied" if kind == "denied" else "invalid"
        if decision == "approved":
            # F5：等待态已释放槽位 —— 恢复执行前重新占用（release 幂等）。
            if not self.slots.try_acquire(acb.id):
                acb.state = TaskState.QUARANTINED
                acb.failure = "NO_SLOT"
                acb.needs_readmit = True
                self.intents.revoke_all(acb.id)
                self._finalize(acb)
                return Status.SEMANTIC_REJECTED
            self._safe_save(acb)
            return None
        self.intents.revoke_all(acb.id)     # 释放租约
        self._finalize(acb)                 # 结算预算 / 释放槽位
        if decision == "timeout":
            return Status.APPROVAL_TIMEOUT
        # F11：拒绝/非法/状态丢失带结构化原因（不复用裸 `Status.DENIED`，
        # 避免与租约失败等其它 DENIED 混淆）。
        return Result(Status.DENIED,
                      {"reason": acb.failure, "approval_id": approval_id})

    @staticmethod
    def _terminal_status(acb: ACB) -> Status:
        # F11：先读审批结论（`hitl_outcome`），避免 `failure` 被预算等其它码覆盖后
        # 把审批拒绝误报成 SEMANTIC_REJECTED。
        outcome = getattr(acb, "hitl_outcome", None)
        decision = outcome.get("decision") if isinstance(outcome, dict) else None
        if decision == "timeout":
            return Status.APPROVAL_TIMEOUT
        if decision in ("denied", "invalid", "lost"):
            return Status.DENIED
        if acb.failure == "APPROVAL_TIMEOUT":
            return Status.APPROVAL_TIMEOUT
        if acb.failure in ("APPROVAL_DENIED", "APPROVAL_INVALID",
                           "APPROVAL_STATE_LOST"):
            return Status.DENIED
        if acb.state in (TaskState.DONE, TaskState.DONE_PARTIAL):
            return Status.OK
        if acb.state is TaskState.CANCELLED:
            return Status.CANCELLED
        return Status.SEMANTIC_REJECTED

    def tick(self, now_ms: int | None = None, *,
             persist_scan: bool = True) -> None:
        """S13a P0-3：唯一超时驱动面（fail-closed）。

        supervisor / 调度器每轮调用；`recover()` 结束也调一次（否则恢复出的
        过期审批永不结算）。**不引入后台线程**（`ManualClock` 不可共享，会 flaky）。

        扫描与审批提交在 `HITL._lock` 内完成 CAS：若已有决策（批准/拒绝），
        跳过超时（决策优先，交给 `run_task` 的入口判态处理），避免双写。
        F2：`hitl_pending` 不可用 → `APPROVAL_STATE_LOST`（不再 `continue` 永久挂起）。
        F4：`advance` 返回值必须校验（非法迁移 → 不结算）。
        """
        now = int(self.clock.now_ms() if now_ms is None else now_ms)
        # K-69：候选集 = 内存 ∪ **持久真相**（未 `recover` 的等待态也要能结算）。
        # 锁序红线（S14 §2.2）：`_tick_candidates` 读完即返回，下面的 `_finalize`
        # **不持 store 锁** —— 否则 budget(1) 套 store(5) 反向获取 → LockOrderViolation。
        for acb in self._tick_candidates(persist_scan=persist_scan):
            if acb.state is not TaskState.WAITING_APPROVAL:
                continue
            with self.hitl._lock:
                if acb.state is not TaskState.WAITING_APPROVAL:
                    continue
                approval_id, deadline = self._approval_slot(acb)
                if approval_id is None or deadline is None:
                    # X1：判终态前先 revoke 租约 + bump epoch + 撤销 claim，
                    # 旧 worker 解锁后的写才会被 fence（否则终态被改回 DONE）。
                    self.supervisor_note_crash(acb)
                    if advance(acb, "timeout") != Status.OK:
                        continue
                    acb.failure = "APPROVAL_STATE_LOST"
                    self._record_approval(acb, "lost", "APPROVAL_STATE_LOST")
                else:
                    if now < deadline:
                        continue
                    if self.hitl.peek(acb) is not _UNSET:
                        continue            # 已有决策 → 不判超时
                    self.supervisor_note_crash(acb)
                    if advance(acb, "timeout") != Status.OK:
                        continue
                    acb.failure = "APPROVAL_TIMEOUT"
                    self._record_approval(acb, "timeout", "APPROVAL_TIMEOUT")
            self.intents.revoke_all(acb.id)  # 释放租约
            self._finalize(acb)              # 结算预算 / 释放槽位
        # S21 K-99：惰性回收过期租约（与 aging/HITL 共用同一驱动面）
        try:
            self.intents.reap_expired(now)
        except Exception:  # noqa: BLE001  # 回收失败不影响超时结算
            pass
        # S22：回收过期的门面 token（只作用于 `spec["rsc_token"]`，不动普通 ACB）
        self._reap_expired_tokens(now)
        # S13d §2.5 / B-4：aging（墙钟）与 HITL 超时共用**唯一**驱动面；
        # 扫描入口在 ReconcileLoop（`scan_deadlines`），便于观测/替换。
        self.reconcile.scan_deadlines(now)

    # ---- S14 K-69：tick 的持久候选集 + 外部驱动器 ---------------------
    def _tick_candidates(self, *, persist_scan: bool = True) -> list[ACB]:
        """`tick` 的候选集 = 内存 ∪ **持久真相**（`persist_scan=False` 时仅内存）。

        为什么必须含持久：S13a 的 `tick` 只遍历 `self.acbs`，而 `recover()` 一次
        只恢复 1 个 ACB → **未 `recover` 的等待态永远不在扫描范围**，反复调
        `tick` 也结算不到（K-69 的真 bug，不是「记得调 tick」能解决的）。

        `persist_scan=False` 供 `recover()` 使用：恢复尚未完成时，别的 ACB 可能
        只恢复了一半，**此时结算它们是错的**（S13 评审 R-5）。恢复结束后的
        首次 `pump()` 会补上。

        锁序红线（S14 §2.2）：本方法内 `refs_all()` / `load_strict()` **读完即
        返回**，调用方在**无 store 锁**下再调 `_finalize`；否则 `budget(1)` 会
        套在 `store(5)` 里 → 反向获取 → `LockOrderViolation`。

        失败不静默：枚举/读取/水合的任一步失败都记 `_tick_skipped`。
        """
        out: list[ACB] = []
        seen: set[str] = set()
        for acb in list(self.acbs.values()):
            seen.add(acb.id)
            out.append(acb)
        if not persist_scan:
            return out
        try:
            refs = self.checkpoints.refs_all()
        except Exception:                       # noqa: BLE001 —— 枚举失败降级为仅内存
            self._tick_skipped.append(("", "refs_all_failed"))
            return out
        for acb_id, ref in refs:
            if acb_id in seen or ref is None:
                continue
            try:
                payload = self.checkpoints.load_strict(ref)
            except Exception:                   # noqa: BLE001
                self._tick_skipped.append((acb_id, "tick_load_failed"))
                continue
            try:
                out.append(self._restore_acb(payload))
            except Exception:                   # noqa: BLE001
                self._tick_skipped.append((acb_id, "tick_hydrate_failed"))
        return out

    def _pending_deadlines(self) -> list[tuple[int, str]]:
        """当前**待结算**项 `[(deadline_ms, acb_id)]`（HITL 超时 ∪ aging）。"""
        out: list[tuple[int, str]] = []
        for acb in self._tick_candidates():
            if acb.state is TaskState.WAITING_APPROVAL:
                _, deadline = self._approval_slot(acb)
            elif acb.state is TaskState.ADMITTED and acb.aging_deadline is not None:
                deadline = acb.aging_deadline
            else:
                continue
            if deadline is None:
                continue
            out.append((int(deadline), acb.id))
        return out

    def next_deadline_ms(self) -> int | None:
        """最近的到期时刻；`None` = 当前无待结算项。

        供外部驱动器**精确定时**（睡到最近到期点），而不是固定间隔轮询
        （对齐 K8s 的 level-triggered resync 只作兜底）。
        """
        pending = self._pending_deadlines()
        return min((d for d, _ in pending), default=None)

    def pump(self, now_ms: int | None = None) -> int:
        """驱动器入口：`tick()` 后返回**仍待结算**项数（`0` = 可退出）。

        典型用法（supervisor，**不引后台线程**）::

            while h.pump() > 0:
                h.tick(h.next_deadline_ms())

        注：`now_ms` 透传给 `tick`，便于 `ManualClock` 精确构造（不用 sleep）。
        """
        self.tick(now_ms)
        return len(self._pending_deadlines())

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
    def redrive_dead_letter(self, acb: ACB) -> ACB | None:
        """把 `DEAD_LETTER` 重新变成可执行（**造新 ACB**，旧 ACB 仍为终态）。

        - 仅 `DEAD_LETTER` 可 redrive（其他状态返回 `None`，不误用）
        - `redrive_count >= max_redrives` → 拒绝（**有界**，对齐 SQS redrive 语义）
        - 走 `requeue_readmit` 同款清理序列：release slot → settle 旧预留 →
          remove/archive 旧节点
        - 写 `DLQ_REDRIVEN` 事件（审计：第几次）

        返回新 ACB（`ADMITTED`）；失败返回 `None`。
        """
        if acb is None or acb.state is not TaskState.DEAD_LETTER:
            return None
        count = int(getattr(acb, "redrive_count", 0) or 0)
        if count >= self.max_redrives:
            self._drain_skipped.append((acb.id, "redrive_limit_exceeded"))
            return None
        plan = self.plans.get(acb.run_id)
        task = (plan.get("tasks", {}) or {}).get(acb.task_id) if plan else None
        if task is None:
            self._drain_skipped.append((acb.id, "redrive_task_missing"))
            return None

        with self._transition_lock(acb):
            old_node = acb.budget_node
            old_rids = list(acb.reservation_ids or [])
            self.slots.release(acb.id)
            fresh = self.dispatcher.try_admit(
                task, acb.run_id, token=None, plan=plan, replace=True)
            if fresh.state is not TaskState.ADMITTED:
                self._drain_skipped.append((acb.id, "redrive_admit_failed"))
                return None
            for rid in old_rids:
                self.budget.settle(rid)
            acb.reservation_ids = []
            if old_node is not None:
                if not self.budget.remove_node(old_node.node_id):
                    self.budget.archive_node(old_node.node_id)
                acb.budget_node = None
            fresh.redrive_count = count + 1
            fresh.parent_id = acb.parent_id
            self._adopt_capability(acb, fresh)
            # 旧 ACB 保持终态（不 superseded 成 CANCELLED，避免改写历史结论）
            acb.needs_readmit = False
            self._safe_save(acb)
            self._safe_save(fresh)
            self.acbs[fresh.id] = fresh
            self.run_coordinator.register(fresh)
            self._register_admission(fresh)
            self.event_log.append(
                EventKind.DLQ_REDRIVEN, acb.run_id,
                {"from_acb_id": acb.id, "to_acb_id": fresh.id,
                 "attempt": fresh.redrive_count, "task_id": acb.task_id},
                run_id=acb.run_id)
            return fresh

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
    def drain(self, timeout_s: float | None = None) -> dict:
        """优雅排空：**不再接受新调度**，等在途任务自然结束。

        返回值：`{"drained": 已终态数, "remaining": 未完成数, "timed_out": bool}`

        与 `cancel` 的区别：drain **不打断**在途任务（避免半提交），只阻止新任务
        被取出执行；超时也不强杀（由调用方决定是否 `kill_run`）。
        """
        import time as _time

        timeout = self.drain_timeout_s if timeout_s is None else float(timeout_s)
        self._draining = True
        deadline = _time.monotonic() + max(0.0, timeout)
        before = sum(1 for a in self.acbs.values() if a.state in TASK_TERMINAL)
        while True:
            self.pump()
            active = [a for a in self.acbs.values() if a.state not in TASK_TERMINAL]
            if not active:
                return {"drained": before, "remaining": 0, "timed_out": False}
            if _time.monotonic() >= deadline:
                return {"drained": before, "remaining": len(active),
                        "timed_out": True}
            _time.sleep(0.005)

    def kill_run(self, run_id: str) -> int:
        """**立即**终止该 run 的全部未终态任务（不可恢复；资源全部结算）。

        与 `pause_run` 的区别：pause 保留资源可恢复；kill 释放一切且不可恢复。
        返回被终止的 ACB 数。
        """
        killed = 0
        for acb in list(self.acbs.values()):
            if acb.run_id != run_id or acb.state in TASK_TERMINAL:
                continue
            with self._transition_lock(acb):
                if acb.state in TASK_TERMINAL:
                    continue
                acb.state = TaskState.CANCELLED
                acb.failure = "KILLED_BY_ADMIN"
                self.intents.revoke_all(acb.id)
                self._finalize(acb)
                killed += 1
        self._paused_runs.discard(run_id)
        return killed

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

    def _reap_expired_tokens(self, now_ms: int) -> int:
        """S22：回收**过期的门面 token**（宿主崩溃/忘记 finish 的兜底）。

        只作用于 `spec["rsc_token"]` 的 ACB（普通 ACB 一律跳过 → 不影响既有
        aging 语义）。动作 = `cancel` → `dispatcher.abort`（退款/释放槽位）
        → 清理钩子（销毁 Z3）→ `purge_run`（回收内存）。返回回收数。
        """
        now = int(now_ms)
        reaped = 0
        for acb in list(self.acbs.values()):
            spec = acb.spec or {}
            if not spec.get("rsc_token"):
                continue
            if acb.state in TASK_TERMINAL:
                continue
            # 评审 P1-5：`INADMISSIBLE`/`QUARANTINED` 是**可重试/待人工**状态
            # （预算已结算），不能被 token 过期直接转成终态并 purge —— 那会
            # 永久夺走宿主的重试入口。
            if acb.state in (TaskState.INADMISSIBLE, TaskState.QUARANTINED):
                continue
            # 评审 P1-6：有活跃 claim 说明正在提交临界区 → 让本次 tick 跳过，
            # 避免「退款」与「在途 commit/consume」竞争。
            if self.scheduler.claim_of(acb.id) is not None:
                continue
            deadline = spec.get("token_deadline_ms")
            if deadline is None or now < int(deadline):
                continue
            if advance(acb, "cancel") != Status.OK:
                continue
            self.dispatcher.abort(acb)
            cleanup = getattr(self, "_token_cleanup", None)
            if callable(cleanup):
                try:
                    cleanup(acb)
                except Exception:  # noqa: BLE001  # 清理失败不得阻断回收
                    pass
            self._drain_skipped.append((acb.id, "token_expired"))
            self.purge_run(acb.run_id)
            reaped += 1
        return reaped

    def claim_window_s(self) -> int:
        """有界墓碑集合的**总覆盖窗口** = `tombstone_max × max_lease_ttl_s`（K-100）。

        **为什么必须用租约时长而不是固定窗口**：若部署把 `lease_ms` 调大
        （例如 600s），而窗口按固定 30s 记，则窗口会**小于**租约时长 →
        残留 fencing 风险从「可忽略」变成「真实」。公式随部署自动适配。

        **强制点在哪**：真正保证不复活旧 worker 的是 Raft 副本里的**逐墓碑
        年龄判定**（`now - 入墓时刻 >= max_lease_ttl_s` 才允许忘记，见
        `_ReplicatedBudget.prune`）；本函数给出的是「满员墓碑集合能覆盖多久」
        的诊断量，供运维核对部署参数是否落在设计假设内。
        """
        tombstone_max = int(getattr(self.budget, "claim_tombstone_max", 1024))
        return max(1, tombstone_max * int(self.max_lease_ttl_s))

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
    def _arm_aging(self, acb: ACB) -> None:
        """准入成功后起算 aging（`aging_ms=0` = 禁用，不改既有语义）。"""
        if self.aging_ms <= 0:
            return
        acb.aging_deadline = int(self.clock.now_ms()) + int(self.aging_ms)

    def _aging_timeout(self, acb: ACB) -> None:
        """到期未调度：单次 → INADMISSIBLE（可 readmit）+ 结算；累计 N → DEAD_LETTER。

        P1-1：`INADMISSIBLE` 是**非终态**，若不给出路 Run 永不 finalize。故
        ① 每次超时都结算租约/预算/槽位（`fail-closed`，不占资源）；
        ② 超过 `aging_max_attempts` → `aging_exhausted` 走真终态并传播 failure。
        """
        with self._transition_lock(acb):
            if acb.state is not TaskState.ADMITTED:
                return
            count = int(getattr(acb, "aging_count", 0) or 0) + 1
            acb.aging_count = count
            if count >= self.aging_max_attempts:
                # 先走 `aging_timeout`（ADMITTED → INADMISSIBLE）再 `aging_exhausted`
                # （INADMISSIBLE → DEAD_LETTER）：只用**已声明**的边，状态机不留暗门。
                if advance(acb, "aging_timeout") != Status.OK:
                    return
                if advance(acb, "aging_exhausted") != Status.OK:
                    return
                acb.failure = "AGING_EXHAUSTED"
                acb.aging_deadline = None
                self.intents.revoke_all(acb.id)
                self._dead_letter_after_replan(acb)      # settle+归档+传播+落盘
                plan = self.plans.get(acb.run_id)
                if plan and acb.task_id in plan.get("tasks", {}):
                    plan["tasks"][acb.task_id]["state"] = acb.state
                return
            if advance(acb, "aging_timeout") != Status.OK:
                return
            acb.failure = "AGING_TIMEOUT"
            acb.aging_deadline = None
            self.intents.revoke_all(acb.id)
            for rid in list(acb.reservation_ids):
                self.budget.settle(rid)
            acb.reservation_ids = []
            self.slots.release(acb.id)
            if acb.budget_node is not None:
                node_id = acb.budget_node.node_id
                if not self.budget.archive_node(node_id):
                    self.budget.remove_node(node_id)
                acb.budget_node = None
            self._clear_admission(acb=acb)
            self._safe_save(acb)

    def readmit_after_aging(self, acb: ACB) -> ACB:
        """S13d P1-1：单次 aging 超时后重新准入（`INADMISSIBLE --readmit--> PLANNED`）。

        复用 `requeue_readmit` 的清理序列；`aging_count` 随新 ACB 继承，
        累计到 `aging_max_attempts` 由 `tick` 判 `DEAD_LETTER`（真终态）。
        """
        with self._transition_lock(acb):
            if acb.state is not TaskState.INADMISSIBLE:
                return acb
            if advance(acb, "readmit") != Status.OK:
                return acb
            count = int(getattr(acb, "aging_count", 0) or 0)
            acb.needs_readmit = True
            fresh = self.requeue_readmit(acb)
            if fresh is not None and fresh is not acb:
                fresh.aging_count = count
                self._safe_save(fresh)
            return fresh if fresh is not None else acb

    def _exec_claim_epoch(self, acb_id: str) -> int | None:
        """当前线程**执行上下文**里的 claim epoch（X1：不读共享 ACB 字段）。"""
        ctx = getattr(self._tls, "claim_epochs", None)
        if not ctx:
            return None
        return ctx.get(acb_id)

    def _bind_claim_epoch(self, acb_id: str, epoch: int) -> int | None:
        ctx = getattr(self._tls, "claim_epochs", None)
        if ctx is None:
            ctx = {}
            self._tls.claim_epochs = ctx
        prev = ctx.get(acb_id)
        ctx[acb_id] = int(epoch)
        return prev

    def _unbind_claim_epoch(self, acb_id: str, prev: int | None) -> None:
        ctx = getattr(self._tls, "claim_epochs", None)
        if not ctx:
            return
        if prev is None:
            ctx.pop(acb_id, None)
        else:
            ctx[acb_id] = prev

    def claim_scope(self, acb: ACB, epoch):
        """把 claim epoch 绑定到**当前线程**的执行上下文（worker / 内联 / 测试）。

        `epoch` 可以是 `Claim`（取 `fence_epoch`）或整数。返回上下文管理器；
        退出时恢复原值（内联嵌套继承父 epoch 也用同一机制）。
        """
        value = int(getattr(epoch, "fence_epoch", epoch))
        h = self

        class _Scope:
            def __enter__(self_inner):
                self_inner._prev = h._bind_claim_epoch(acb.id, value)
                self_inner._acb_id = acb.id
                return acb

            def __exit__(self_inner, *_exc):
                h._unbind_claim_epoch(self_inner._acb_id, self_inner._prev)
                return False

        return _Scope()

    def _claim_fenced(self, acb: ACB) -> bool:
        """P0-10：写路径 fencing —— 比较**执行上下文 epoch**，不读共享 ACB。

        - 活跃 claim 存在：上下文没有 epoch（外部线程）或 epoch 不等 → fenced；
        - claim 已撤销（tick/supervisor 回收）：上下文 epoch **低于撤销水位** → fenced
          （否则审批超时被在途 worker 解锁后的写覆盖）；
        - sync 模式 / 从未 claim → 恒 `False`（36 个同步用例零破坏）。
        """
        if self.scheduler_mode != "pool":
            return False
        ctx = self._exec_claim_epoch(acb.id)
        claim = self.scheduler.claim_of(acb.id)
        if claim is not None:
            if ctx is None:
                return True
            return int(ctx) != int(claim.fence_epoch)
        watermark = self.scheduler.revoked_epoch(acb.id)
        if watermark is not None and ctx is not None:
            return int(ctx) < int(watermark)
        return False

    def _claim_snapshot(self, acb: ACB) -> dict:
        """X7：快照用的 claim 三字段（无锁只读，避免 store ↔ scheduler AB-BA）。"""
        claim = self.scheduler.peek_claim(acb.id)
        if claim is None:
            return {}
        return {"claim_epoch": int(claim.fence_epoch),
                "claim_worker": str(claim.worker_id),
                "claim_lease_until_ms": int(claim.lease_until_ms)}

    # ---- X6：跨 run 子索引 -----------------------------------------
    def _index_child(self, child_id: str, parent_id: str) -> None:
        if parent_id and str(child_id) != str(parent_id):
            self._children_index.setdefault(str(parent_id), set()).add(str(child_id))

    def _ensure_children_index(self) -> None:
        """按 `parent_id` / `spec.spawn.parent_id` 反扫全部快照，建父→子索引。

        只在第一次取消级联/恢复子时构建一次；`spawn_child` 与 `_restore_acb`
        会增量更新，所以构建后仍能覆盖本进程新产生的子。构建失败不抛（尽力）。
        """
        if self._children_index_built:
            return
        self._children_index_built = True
        getter = getattr(self.checkpoints, "refs_all", None)
        if not callable(getter):
            return
        try:
            entries = getter()
        except Exception:  # noqa: BLE001
            return
        for acb_id, ref in entries or []:
            if not ref:
                continue
            try:
                data = self.checkpoints.load(ref)
            except Exception:  # noqa: BLE001
                continue
            if not isinstance(data, dict):
                continue
            parent = data.get("parent_id") or ""
            if not parent:
                spec = data.get("spec")
                spawn = spec.get("spawn") if isinstance(spec, dict) else None
                if isinstance(spawn, dict):
                    parent = spawn.get("parent_id") or ""
            if parent:
                self._index_child(str(data.get("acb_id") or acb_id), str(parent))

    def _run_one(self, acb: ACB, decisions=None, crash_at=None) -> Status:
        """执行体（§2.6）：`runtime.run` + `_finalize`；**不 push、不 drain**（防递归）。"""
        prev_running = getattr(self._tls, "running_acb", None)
        self._tls.running_acb = acb          # X4：内联子记录 `inline_parent`
        try:
            return self.runtime.run(acb, decisions or [], crash_at=crash_at)
        finally:
            self._finalize(acb)
            self._tls.running_acb = prev_running

    def _drain_scheduler(self) -> None:
        """真消费 ready_queue（§2.6）：`pop → 找 ACB → 只启动 ADMITTED`。

        - `_run_one` 不 push、不递归 drain → 无无限递归、无双重 run；
        - `needs_readmit` 的 ACB 不进调度（§5.7）：需先 `requeue_readmit`；
        - **D10（S13b P1-5）**：队列键统一 `acb.id`，**删除**「按 task_id 扫第一个
          ADMITTED」兜底 —— 该兜底在 replan 后同 task_id 出现两个 ADMITTED 时会
          跑错代际 ACB / 双跑；
        - 另按 `(run_id, task_id, plan_version)` 过滤陈旧代际（旧 ACB 即使仍
          ADMITTED 也不跑）；
        - D11：被跳过的键记录原因到 `_drain_skipped`（不再静默 continue）；
        - **S13d §2.5**：`pool` 模式在同一把 scheduler 锁内「pop + claim」——
          只有拿到 claim 的 worker 执行（单一 pop 所有者）。
        """
        if self.scheduler_mode == "pool":
            self._drain_scheduler_claimed()
            return
        while True:
            key = self.scheduler.pop()
            if key is None:
                break
            reason = self._drain_skip_reason(key)
            if reason is not None:
                self._drain_skipped.append((key, reason))
                continue
            self._run_one(self.acbs[key])

    def _drain_scheduler_claimed(self) -> None:
        """pool 模式的 drain：pop+claim → 校验 → 执行 → 释放 claim。"""
        worker_id = self._current_worker_id()
        while True:
            got = self.scheduler.pop_claimed(worker_id)
            if got is None:
                break
            key, claim = got
            if claim is None:
                # 有效租约归其他 worker（同 id 的重复 ready 键）→ 绝不双跑。
                self._drain_skipped.append((key, "claim_held"))
                continue
            try:
                reason = self._drain_skip_reason(key)
                if reason is not None:
                    self._drain_skipped.append((key, reason))
                    continue
                with self.claim_scope(self.acbs[key], claim):
                    self._run_one(self.acbs[key])
            finally:
                self.scheduler.release_claim(key, worker_id)

    def _drain_skip_reason(self, key: str) -> str | None:
        """`None` = 可调度；否则为 `_drain_skipped` 的原因（D11，不静默）。"""
        acb = self.acbs.get(key)
        if acb is None:
            return "unknown_acb"
        if acb.needs_readmit:
            return "needs_readmit"
        if acb.state is not TaskState.ADMITTED:
            # PLANNED / 已终态等：按设计不调度，但记录原因（D11）
            return f"state={acb.state.value}"
        if not self._is_current_plan_version(acb):
            return "stale_plan_version"
        if self.is_paused(acb.run_id):
            # S15 D3：Admin 暂停 —— 不执行但**保留**槽位/预算/节点（可恢复）
            return "run_paused"
        return None

    def _is_current_plan_version(self, acb: ACB) -> bool:
        """D10：同 `(run_id, task_id)` 存在**更新代际** ADMITTED ACB 时，旧代际不调度。

        只比较同一 task 的兄弟 ACB，而非 plan 里该 task 的 `plan_version` ——
        未被 patch 改名的任务（如 successors）仍持有 replan 前准入的 ACB，
        它只有一个 ADMITTED 实例，必须继续可跑（S13-4 执行层）。
        """
        siblings = [a for a in self.acbs.values()
                    if a.run_id == acb.run_id and a.task_id == acb.task_id
                    and a.state is TaskState.ADMITTED]
        if len(siblings) <= 1:
            return True
        best = max(int(a.spec.get("plan_version", 0) or 0) for a in siblings)
        return int(acb.spec.get("plan_version", 0) or 0) == best

    def _resolve_current_acb(self, run_id: str, task_id: str,
                             plan_version: int | None = None,
                             *, plan: dict | None = None,
                             src: ACB | None = None) -> ACB | None:
        """按 `(run_id, dst, plan_version)` **解析或准入**后继 ACB（D10 / R5）。

        - 有唯一候选 → 返回；
        - 无候选且 `plan` 给出了 PLANNED 任务 → **就地 `try_admit`**（生产常态：
          `dispatch_and_run` 一次只准入一个 task，后继不会提前有 ACB）；准入失败：
          `INVALID_INPUT` / `DENIED_CAPABILITY` / `CONTRACT_CONFLICT` 等**硬失败**
          → `advance("give_up")` 后 `_finalize`（FAILED 并传播），绝不 silent skip；
          `NO_SLOT` / `BUDGET_EXHAUSTED` 等可恢复失败 → 记 `_drain_skipped` 留给
          supervisor 重试；
        - 多候选 → 按 `(plan_version, generation, acb.id)` 确定性选一个，落选者
          标 `needs_readmit`（不扫第一个、不双跑）。
        """
        cands = [a for a in self.acbs.values()
                 if a.run_id == run_id and a.task_id == task_id
                 and a.state is TaskState.ADMITTED and not a.needs_readmit
                 and not getattr(a, "superseded", False)]
        if plan_version is not None:
            exact = [a for a in cands
                     if int(a.spec.get("plan_version", 0) or 0) == int(plan_version)]
            if exact:
                cands = exact
        if not cands:
            return self._admit_ready_successor(run_id, task_id, plan, src)
        if len(cands) == 1:
            return cands[0]
        cands.sort(key=lambda a: (int(a.spec.get("plan_version", 0) or 0),
                                  int(a.generation or 0), a.id), reverse=True)
        best = cands[0]
        for loser in cands[1:]:
            loser.needs_readmit = True
            self._safe_save(loser)
        self._drain_skipped.append(
            (f"{run_id}:{task_id}", "ambiguous_pick_best"))
        return best

    def _admit_ready_successor(self, run_id: str, task_id: str,
                               plan: dict | None, src: ACB | None = None) -> ACB | None:
        """R5：把 ready 集中的 PLANNED 任务就地准入（无候选时的唯一出路）。"""
        if plan is None:
            return None
        task = self.planner.task_for(plan, task_id)
        if not isinstance(task, dict) or task.get("state") is not TaskState.PLANNED:
            return None
        task_id = task.get("task_id", task_id)
        try:
            fresh = self.dispatcher.try_admit(task, run_id, plan=plan)
        except Exception:  # noqa: BLE001  # 准入异常不得打断父任务的结算
            self._drain_skipped.append((f"{run_id}:{task_id}", "admit_raised"))
            return None
        if fresh.state is TaskState.ADMITTED:
            self.acbs[fresh.id] = fresh
            self.run_coordinator.register(fresh)
            self._adopt_capability(src, fresh)
            if getattr(fresh, "capability", None) is not None:
                self._safe_save(fresh)      # R1：新身份 token 随快照落盘
            return fresh
        reason = fresh.failure or fresh.state.value
        self._drain_skipped.append((f"{run_id}:{task_id}", f"admit_{reason}"))
        if fresh.state is TaskState.INADMISSIBLE and reason in (
                "INVALID_INPUT", "DENIED_CAPABILITY", "CONTRACT_CONFLICT"):
            # R5：无法处理 → 走状态机 give_up（FAILED），由 `_finalize` 传播；
            # 资源已在准入回滚中释放，不会留下残留。
            advance(fresh, "give_up")
            self._finalize(fresh)
        return None

    def _adopt_capability(self, src: ACB | None, fresh: ACB) -> None:
        """R1/R5：绝不复用旧 ACB 的 token（身份绑定到旧 id 必然 DENIED）。

        以旧 token 的**scope**为新 id 重新签发内存 token（不是复用原 token 对象）；
        已有可用 token（`acb.capability` / `policy._tokens[fresh.id]`）时不重复签发。

        C6：新 ACB **继承父身份与父能力快照**（`parent_id` 不可变）；
        `requeue`/`replan` 后的子仍是原子树的子，lineage 校验继续生效。
        """
        if src is None or fresh is None:
            return
        if getattr(fresh, "capability", None) is not None:
            return
        if self.policy.token(fresh.id) is not None:
            return
        token = self._effective_token(src)
        if token is None:
            return
        resolved = self.policy.resolve_token(token)
        if resolved is None:
            return
        fresh.capability = self.policy.issue(
            fresh.id, tools=set(resolved.tools), paths=set(resolved.paths),
            network=set(resolved.network), data_class=resolved.data_class)
        # C6：lineage 继承（父 id + 冻结的父能力快照）。
        if resolved.parent_id:
            fresh.capability.parent_id = resolved.parent_id
            if isinstance(getattr(resolved, "parent_scope", None), dict):
                fresh.capability.parent_scope = copy.deepcopy(
                    resolved.parent_scope)
                fresh.capability.parent_scope_digest = (
                    getattr(resolved, "parent_scope_digest", "") or "")
            self._ensure_policy_token(resolved.parent_id)
            if not fresh.parent_id:
                fresh.parent_id = resolved.parent_id

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
    @staticmethod
    def _admission_key_for(acb: ACB) -> tuple[str, str, int]:
        return (acb.run_id, acb.task_id,
                int(acb.spec.get("plan_version", 1) or 1))

    def _lookup_admission(self, key: tuple[str, str, int]) -> ACB | None:
        """取同一 `(run_id, task_id, plan_version)` 的**活跃** ACB（无则 None）。

        终态 / `INADMISSIBLE` / `QUARANTINED` / `superseded` → 视为已失效并清登记
        （允许显式重新准入，例如 T29 `requeue_readmit` / 失败后重试）。
        """
        with self._lock:
            acb = self._admission_index.get(key)
            if acb is None:
                return None
            dead = (acb.state in TASK_TERMINAL
                    or acb.state in (TaskState.INADMISSIBLE, TaskState.QUARANTINED)
                    or getattr(acb, "superseded", False))
            if dead:
                self._admission_index.pop(key, None)
                return None
            return acb

    def _register_admission(self, acb: ACB) -> None:
        with self._lock:
            self._admission_index[self._admission_key_for(acb)] = acb

    def _clear_admission(self, key: tuple[str, str, int] | None = None,
                         acb: ACB | None = None) -> None:
        with self._lock:
            if key is not None:
                self._admission_index.pop(key, None)
            if acb is not None:
                self._admission_index.pop(self._admission_key_for(acb), None)

    def _replace_child_ref(self, old: ACB, fresh: ACB) -> None:
        """C6：replan/requeue 后把父树中的旧子 id 原位替换为新 id。

        `parent_id` 不可变意味着新 ACB 仍是同一父的子；父的 `children` /
        `children_ids` / `children_pending` 必须同步，否则取消/汇总会漏掉新身份。

        X3：**ref 必须是 fresh 自己的**（调用方须先 `save(fresh)`）。复用旧子
        的 ref 会让重启后的 `result_envelope` 按该 ref 载回**旧子**（永远
        pending → 父不 finalize），`_walk_children` 也会漏掉新身份。
        """
        parent_id = getattr(old, "parent_id", "") or ""
        if not parent_id:
            return
        self._index_child(fresh.id, parent_id)   # X6：替换后的新身份也进反扫索引
        fresh_ref = fresh.result_ref or self._ref_for_child(fresh.id)

        def _patch(entries):
            out: list = []
            replaced = False
            for entry in list(entries or []):
                if isinstance(entry, (list, tuple)) and str(entry[0]) == str(old.id):
                    out.append((fresh.id, fresh_ref))
                    replaced = True
                else:
                    out.append(entry)
            if not replaced:
                out.append((fresh.id, fresh_ref))
            return out

        parent = self.acbs.get(parent_id)
        if parent is None:
            # 父不在内存 → 在**最新快照**上原地改 children_ids（不重建 ACB，
            # 否则会丢掉 staged / 预算 / 能力等字段）。
            updater = getattr(self.checkpoints, "update_children", None)
            if callable(updater):
                # S14 K-68：条件写 —— 用**本次读到的**父 ref 作为期望值，
                # 避免两个并发 replan 互相覆盖（后写者丢更新）。
                expect = self.checkpoints.latest_ref_for_acb(parent_id)
                try:
                    updater(parent_id, _patch, expect_ref=expect)
                except FencingRejected:
                    # CAS 冲突耗尽重试：显式记录（不静默丢更新），父的下次
                    # 汇总/取消会由 `_index_child` 的反扫索引兜底。
                    self._drain_skipped.append(
                        (parent_id, "child_ref_cas_conflict"))
            return
        parent.children = [fresh if child is old else child
                           for child in list(getattr(parent, "children", []) or [])]
        parent.children_ids = _patch(parent.children_ids)
        pending = getattr(parent, "children_pending", None) or {}
        if str(old.id) in pending:
            parent.children_pending[str(fresh.id)] = pending.pop(str(old.id))
        self._safe_save(parent)

    # ---- R7：每 ACB 一把 transition 锁 -------------------------------
    def _transition_lock(self, acb: ACB) -> threading.RLock:
        with self._lock:
            lock = self._transition_locks.get(acb.id)
            if lock is None:
                lock = threading.RLock()
                self._transition_locks[acb.id] = lock
            return lock

    def _finalize(self, acb: ACB) -> None:
        # R7：`superseded` 的旧 ACB 已由 replan 在同一临界区结算 + 摘除；
        # 这里**不得**再落盘（否则覆盖 replan 后的快照 → 恢复出僵尸）。
        if getattr(acb, "superseded", False):
            return
        with self._transition_lock(acb):
            self._finalize_locked(acb)
            self._maybe_poison_locked(acb)

    def _maybe_poison_locked(self, acb: ACB) -> None:
        """S17 MS10：**毒任务识别** —— 连续失败达阈值 → 直接 `DEAD_LETTER`。

        为什么复用 `escalate_quarantined`：`QUARANTINED →(replan_budget_exhausted)
        → DEAD_LETTER` 是**已声明**的合法边；不新造状态机暗门，也不改
        `DEAD_LETTER` 的终态语义。失败码记为 `POISON_TASK` 以便 DLQ 检索。
        """
        if acb.state in (TaskState.DONE, TaskState.DONE_PARTIAL):
            acb.consecutive_failures = 0
            return
        if acb.state is TaskState.FAILED:
            acb.consecutive_failures = int(acb.consecutive_failures or 0) + 1
            return
        if acb.state is not TaskState.QUARANTINED:
            return
        acb.consecutive_failures = int(acb.consecutive_failures or 0) + 1
        if self.poison_after_failures <= 0:
            return
        if acb.consecutive_failures < self.poison_after_failures:
            return
        # 达到阈值 → 走既有合法边进 DEAD_LETTER（真终态）
        self.escalate_quarantined(acb)
        acb.failure = "POISON_TASK"
        self._safe_save(acb)

    def _finalize_locked(self, acb: ACB) -> None:
        # X1：结算前先释放活跃 claim（幂等）。否则 finalize→release 的顺序会让
        # 「已终态但 claim 未释放」窗口里的另一 worker 误以为仍被持有。
        self.scheduler.release_claim(acb.id)
        plan = self.plans.get(acb.run_id)
        if plan and acb.task_id in plan["tasks"]:
            plan["tasks"][acb.task_id]["state"] = acb.state
        # §2.4：_finalize 落盘（K-3：在结算/归档**之前**，保留活跃预留供 recover 恢复）；
        # P1-5：保存失败**不得**阻断下面的结算/归档/槽位释放（否则槽位泄漏）。
        # X8：唯一例外是 claim fencing —— 旧 worker 的快照不得覆盖新 claim，
        # 且结算由新持有者完成，这里直接中止（留痕在 `_drain_skipped`）。
        if not self._safe_save(acb):
            return
        # S10a §2.1：结算 + 归档的预算事件**同帧 append_batch**（一次临界区，
        # 避免逐笔 append 放大事件日志锁竞争 → 基准时序抖动）
        with self.budget.suppressed():
            if acb.state in (TaskState.FAILED, TaskState.CANCELLED, TaskState.DEAD_LETTER,
                             TaskState.DONE, TaskState.DONE_PARTIAL, TaskState.QUARANTINED):
                # 终态 + QUARANTINED 结算全部预留（文档 §5.5：未消耗部分归还，已消耗留账）
                for rid in list(acb.reservation_ids):
                    self.budget.settle(rid)
            if (acb.state in TASK_TERMINAL and acb.budget_node is not None
                    and self.budget.archive_node(acb.budget_node.node_id)):
                # §5.3（K-13）：终态且无 ACTIVE 预留 → 归档（consumed 折入父级）
                acb.budget_node = None
        self.budget.flush_pending()
        # F5：等待审批**不是**占槽位的理由（否则 `max_concurrent` 下等待中的任务会
        # 饿死其他 run）；统一释放，`_hitl_gate` 批准后重新 `try_acquire`。
        # `SlotTracker.release` 幂等，重复释放不会泄漏。
        self.slots.release(acb.id)                 # P1-5：先释放槽位（不因落盘失败跳过）
        if acb.state in (TaskState.FAILED, TaskState.CANCELLED, TaskState.DEAD_LETTER):
            # D14：patch 后旧 ACB 的 task_id 可能已不在 plan["tasks"] 中 → 用 .get() 安全取值
            task = plan.get("tasks", {}).get(acb.task_id) if plan else None
            plan_version = task.get("plan_version", 1) if isinstance(task, dict) else 1
            self.propagate_failure(acb.task_id, plan_version)
        # level-triggered 对账（文档 §5.2.1）：事件即真相
        if plan:
            self.reconcile.reconcile_events(acb.run_id)
            # D9：重建 ready 集后真正 push 到调度器（原返回值被丢弃 → 后继永不调度）。
            # 只调度「因本任务完成而新就绪」的后继：早已就绪（无前驱）的任务仍由
            # run_task/dispatch_and_run 显式驱动，避免把非终态任务一并拉起（A2/A11/I12）。
            #
            # B-2 ③（S13b P0-4）：边仍写旧 id（T1），任务已改名（T1'）→ 直接
            # `src == acb.task_id` 永不匹配。改为「task_id 相同 **或** alias 一跳
            # 解析到本 ACB 的稳定 logical_subgoal_id」。
            #
            # R11：同 subgoal 可能对应多个后继（重复 subgoal）→ 额外要求 alias
            # 解析出的**当前 task 就是本 ACB 的 task**，避免「T1' 完成推起 T3 和 T4」。
            #
            # D10（S13b P1-5）：队列键统一 `acb.id`；`dst` 按当前 plan 版本解析到
            # **唯一** ACB，解析不到记 `_drain_skipped`（不静默、不扫第一个）。
            # R5：解析不到 ACB 时**就地准入**（生产常态：一次只准入一个 task），
            # 仍无法处理才记录 give_up（绝不 silent skip）。
            ready = set(self.reconcile.rebuild_ready_set(acb.run_id))
            for edge in plan.get("edges", []) or []:
                src, dst = ((edge.get("from"), edge.get("to"))
                            if isinstance(edge, dict) else (edge[0], edge[1]))
                src_task = self.planner.task_for(plan, src)
                same_task = src == acb.task_id or (
                    isinstance(src_task, dict)
                    and src_task.get("task_id") == acb.task_id)
                same_subgoal = bool(acb.logical_subgoal_id) and \
                    self.planner.resolve_alias(src, plan) == acb.logical_subgoal_id \
                    and isinstance(src_task, dict) \
                    and src_task.get("task_id") == acb.task_id
                if not (same_task or same_subgoal):
                    continue
                dst_task = self.planner.task_for(plan, dst)
                if not isinstance(dst_task, dict):
                    self._drain_skipped.append((f"{acb.run_id}:{dst}",
                                                "push_unresolved_task"))
                    continue
                dst_tid = dst_task.get("task_id")
                if dst_tid not in ready:
                    continue
                dst_pv = int(dst_task.get("plan_version",
                                          plan.get("version", 1)) or 1)
                target = self._resolve_current_acb(
                    acb.run_id, dst_tid, dst_pv, plan=plan, src=acb)
                if target is None:
                    self._drain_skipped.append((f"{acb.run_id}:{dst_tid}",
                                                "push_no_current_acb"))
                    continue
                self.scheduler.push(target.id)

    def escalate_quarantined(self, acb: ACB) -> Result:
        """人工 escalate：QUARANTINED → DEAD_LETTER，释放预算并传播 failure（文档 §5.5）。"""
        if acb.state is not TaskState.QUARANTINED:
            return Result(Status.INVALID_INPUT, {"state": acb.state.value})
        r = advance(acb, "replan_budget_exhausted")     # QUARANTINED → DEAD_LETTER（合法边）
        if r != Status.OK:
            return r
        for rid in list(acb.reservation_ids):
            self.budget.settle(rid)
        self.slots.release(acb.id)
        plan = self.plans.get(acb.run_id)
        if plan and acb.task_id in plan["tasks"]:
            self.propagate_failure(acb.task_id,
                                   plan["tasks"][acb.task_id].get("plan_version", 1))
        self.checkpoints.save(acb)              # §2.4：escalate 落盘
        return Result(Status.OK, {"state": acb.state.value})

    def _pointer_header_fallback(self, ref: str) -> dict:
        """P1-6：ref 头部为空时，从 checkpoint 的 latest 指针兜底取元数据。"""
        fallback = getattr(self.checkpoints, "_header_from_pointers", None)
        if callable(fallback):
            try:
                return dict(fallback(ref) or {})
            except Exception:  # noqa: BLE001
                return {}
        return {}

    # ---- 恢复（文档 §5.8 / §4.3） -------------------------------
    def recover(self, ref: str, *, resume_policy: str = "RESUME") -> ACB:
        """崩溃恢复（§4.3）：journal 重建 → ACB 重建 → 事件重放 → probe → 终态策略。

        - `resume_policy="RESUME"`：probe 三态对账（`True` → CONFIRMED；`None` → 隔离）；
        - `resume_policy="COMPENSATE"`：**终态策略**（清 staged，补偿后不恢复执行）。
        """
        # S10a §2.5：恢复即写者接管，必须持租约；被其他写者持有 → 拒绝服务。
        if not self._ensure_writer_lease():
            header = self._pointer_header_fallback(ref)
            acb = ACB(id=header.get("acb_id", ""),
                      run_id=header.get("run_id", ""),
                      task_id=header.get("task_id", ""))
            acb.generation = int(header.get("generation", 1) or 1) + 1
            acb.state = TaskState.QUARANTINED
            acb.failure = "WRITER_LEASE_HELD"
            acb.needs_readmit = True
            if acb.id:
                self.acbs[acb.id] = acb
                self.run_coordinator.register(acb)
            return acb
        try:
            data = self.checkpoints.load_strict(ref)  # 绕过缓存，校验双 checksum
        except CheckpointCorrupt as exc:
            # §2.4.8：payload 坏 → 用头部构造 QUARANTINED + needs_readmit
            header = dict(exc.header or {})
            if not header:
                # P1-6：头部为空时用指针文件里的 acb_id/run_id/task_id/generation 兜底
                header = self._pointer_header_fallback(ref)
            acb = ACB(id=header.get("acb_id", ""),
                      run_id=header.get("run_id", ""),
                      task_id=header.get("task_id", ""))
            acb.generation = int(header.get("generation", 1) or 1) + 1
            acb.state = TaskState.QUARANTINED
            acb.failure = "CHECKPOINT_CORRUPT"
            acb.needs_readmit = True
            self.acbs[acb.id] = acb
            self.run_coordinator.register(acb)
            return acb
        events = self.event_log.all()                  # 锁内快照（单一数据源）
        self._rebuild_receipts()                       # §2.3.4：head/id 从 EventLog 重建
        mine = [e for e in events if e.producer == data.get("acb_id")]
        self.effects.rebuild_from(mine)
        self.comp_journal.rebuild_from(mine)
        self._restore_epoch()                          # S10a §3.4：epoch 读持久化值
        acb = self._restore_acb(data, resume_policy)
        # 恢复已应用事件集合（去重键持久化，避免重放重复投影）
        persisted = data.get("applied_event_ids")
        if persisted:
            self._applied_event_ids.update(persisted)
        # ① 先重放事件（把外部状态对齐），再 probe
        self._replay_events(from_seq=int(data.get("event_cursor") or 0), dedup=True)
        for entry in self.effects.pending(acb.id):     # 含 INTENT
            probe = self.effects.probe_one(entry.idem_key)
            if probe == "True":
                self.effects.confirm(entry.idem_key, {"recovered": True})
            elif probe == "None":
                acb.state = TaskState.QUARANTINED
                acb.failure = "NEEDS_RECONCILIATION"
        # ② COMPENSATE 是终态策略：补偿前丢弃 staged，补偿后不恢复执行
        if resume_policy == "COMPENSATE":
            acb.staged = {}
            plan = self.comp_journal.resume(acb, self.workspace)
            for e in plan["retry"]:
                self._run_one_compensation(acb, e)
            acb.staged = {}                     # 终态策略：补偿后不恢复执行
            # 重试后重新读取条目终态：escalate（FAILED）/ pivot 必须被消费
            entries = [e for e in self.comp_journal._entries.values()
                       if e.acb_id == acb.id]
            escalate = bool(plan["escalate"]) or any(
                e.state == "FAILED" for e in entries)
            pivot = bool(plan["pivot"]) or any(
                e.state == "PIVOT" for e in entries)
            if escalate:
                acb.state = TaskState.QUARANTINED
                acb.failure = "COMPENSATE_FAILED"
            elif pivot:
                acb.state = TaskState.DEAD_LETTER
                acb.failure = "COMPENSATE_PIVOT"
            else:
                acb.state = TaskState.DONE_PARTIAL
                acb.failure = None
            # 终态策略：结算全部预留 → 归档预算节点 → 持久化终态
            for rid in list(acb.reservation_ids):
                self.budget.settle(rid)
            if (acb.budget_node is not None
                    and self.budget.archive_node(acb.budget_node.node_id)):
                acb.budget_node = None
            if acb.state in (TaskState.DONE_PARTIAL, TaskState.DEAD_LETTER):
                self.slots.release(acb.id)
            self.checkpoints.save(acb)
        # ③ 恢复后全树必查（K-14）；失败 → QUARANTINED + slots.release
        #    S10a §2.3：跨进程恢复额外做**树完整性**校验（锚定 EventLog 声明集）
        try:
            self.budget.check_invariants(cross_process=True)
        except InvariantError:
            acb.state = TaskState.QUARANTINED
            acb.failure = acb.failure or "INVARIANT_BROKEN"
            self.slots.release(acb.id)
            self.checkpoints.save(acb)
        self.acbs[acb.id] = acb
        self.run_coordinator.register(acb)
        # R6：恢复出的活跃 ACB 也纳入准入登记（避免同 key 再次 try_admit 复制一份）
        self._register_admission(acb)
        # C1：对账两阶段窗口留下的 `children_pending` / ref=None 子（找回 /
        # 孤儿结算留痕）。
        self._reconcile_children_refs(acb)
        self._reconcile_children_pending(acb)
        # S15 D3：从事件流重建 Admin 暂停态（重启后仍暂停）
        self._rebuild_pause_state()
        # S13a P0-3：恢复结束即驱动一次超时结算（否则恢复出的过期审批永不结算）。
        # S14 K-69：此处 **persist_scan=False** —— 恢复尚未完成，别的 ACB 可能只
        # 恢复了一半，此时结算它们是错的（S13 评审 R-5）。恢复后的首次 `pump()`
        # 会做持久扫描补上。
        self.tick(persist_scan=False)
        return acb

    def replan(self, acb: ACB, plan: dict | None = None, *,
               diagnosis: dict | None = None) -> dict | None | Result:
        """S13b §2.3 P0-5：有界 replan（write-ahead 计数 → patch → 再准入）。

        顺序固定：**先查预算/状态 → 计数 + 落盘 → patch（纯函数）→ 同帧提交事件 →
        清理旧节点并再准入**。先落盘计数才能防「崩溃后 replan_count 归零 →
        无限重规划」。

        Saga 语义（S13b §2.3 / S13-9）：replan **不重跑整棵树**，只重做**未提交**
        的分支；已提交副作用靠 `effect_journal` + 跨 replan 稳定的
        `logical_subgoal_id` 去重（`effect_idem_key` 不含 task_id / generation）。

        预算耗尽：`advance(acb, "replan_budget_exhausted")` → DEAD_LETTER，
        且**不 patch**（`PLAN_PATCHED` 事件数不变、plan.version 不变、
        `replan_count` 不递增）。

        R8：入口态只允许 `{QUARANTINED, REPLANNING}`（否则 `INVALID_INPUT`）。
        已完成/已归档的任务被再 replan 会整支重做，绝不允许。
        R4：计数是 write-ahead 的；patch/append 失败 → **回滚 `replan_count`**
        （瞬时错误不吃掉预算），但 `replan_attempts` 永不回滚（熔断风暴），
        并把 REPLANNING 拉回 QUARANTINED（不允许非终态卡死）。
        """
        plan = plan if plan is not None else self.plans.get(acb.run_id)
        if plan is None:
            return None
        with self._transition_lock(acb):
            if acb.state not in (TaskState.QUARANTINED, TaskState.REPLANNING):
                return Result(Status.INVALID_INPUT, {
                    "reason": "invalid_replan_state", "state": acb.state.value})
            exhausted = (acb.replan_count >= self.replan_budget
                         or acb.replan_attempts > self.replan_budget)
            if exhausted:                              # ① 先查预算 / 风暴熔断
                if advance(acb, "replan_budget_exhausted") == Status.OK:
                    self._dead_letter_after_replan(acb)     # → DEAD_LETTER
                else:
                    self._safe_save(acb)
                return None
            prev = acb.replan_count                    # ② 计数（成功预算）
            acb.replan_count = prev + 1
            acb.replan_attempts = acb.replan_attempts + 1   # 尝试数（永不回滚）
            try:
                # ③ write-ahead 落盘 —— X5：必须**在 try 内**。pool 模式下
                # `FencingRejected` 会在这里抛出；放在 try 外则内存已 +1 而
                # 磁盘仍是旧值且无法回滚（池化旧 worker 的写被 fence）。
                self.checkpoints.save(acb)
                new_plan, event_draft = self.planner.patch(   # ④ 纯函数 patch
                    plan, diagnosis or {})
                # ⑤ 同帧提交：新 plan 事件 + 计数（已落盘）原子可见
                self.event_log.append_batch([event_draft])
            except Exception:                          # 不捕获 BaseException
                acb.replan_count = prev                # 补偿：只回滚成功计数
                # `replan_attempts` 仍**永不回滚**（熔断 replan 风暴，R4）。
                if acb.state is TaskState.REPLANNING:
                    acb.state = TaskState.QUARANTINED  # 非终态不得卡死
                self._safe_save(acb)                   # 磁盘回滚（可能已被 fence）
                raise
            self.plans[acb.run_id] = new_plan
            # ⑥ 清理旧节点 + 再准入（P0-6，复用 requeue_readmit 序列）；
            #    R7：superseded 的标记与落盘在同一临界区内完成
            # 注意顺序：`_readmit_after_replan` 内部还要用**旧 task_id** 解析
            # （`task_for(new_plan, acb.task_id)`），所以剪枝必须放在它**之后**。
            self._readmit_after_replan(acb, new_plan)
            # S14 K-67(A)：别名剪枝（判据含「活跃 ACB 的 task_id / subgoal」）。
            self._prune_id_aliases(acb.run_id, new_plan)
            return new_plan

    def _prune_id_aliases(self, run_id: str, plan: dict) -> None:
        """S14 K-67(A)：剪掉**已不可能再被引用**的旧 id → logical_subgoal 映射。

        判据（修正 S13 §2.3 的漏洞）::

            keep = edges 端点 ∪ plan["tasks"] ∪ 本 run 活跃 ACB 的 task_id
                                    ∪ 本 run 活跃 ACB 的 logical_subgoal_id

        为什么必须含最后两项：`patch()` 已把 `edges` 端点**物化**为 canonical id，
        于是边不再引用旧 id；但 `_finalize` / `_readmit_after_replan` 仍按
        **活跃 ACB 的旧 `task_id`** 解析。只按 edges 剪枝会把它们剪掉 →
        `replan_no_task`。这正是 K-67 的判据修正。

        收益：`patch()` 每次 `deepcopy(plan)` 会连别名表一起拷，别名无界增长会让
        单 plan 内 n 次 replan 退化为 O(n²)；剪枝让它**单调不增**。
        """
        aliases = plan.get("_id_aliases")
        if not isinstance(aliases, dict) or not aliases:
            return
        keep: set[str] = set(plan.get("tasks") or {})
        for edge in plan.get("edges", []) or []:
            keep.add(str(edge.get("from", "")))
            keep.add(str(edge.get("to", "")))
        for acb in list(self.acbs.values()):
            if acb.run_id != run_id:
                continue
            keep.add(str(acb.task_id))
            if acb.logical_subgoal_id:
                keep.add(str(acb.logical_subgoal_id))
        plan["_id_aliases"] = {k: v for k, v in aliases.items() if str(k) in keep}

    def replan_from_quarantine(self, acb: ACB, plan: dict | None = None, *,
                               diagnosis: dict | None = None) -> dict | None | Result:
        """S13b §2.3：隔离态走合法边 `escalate` → REPLANNING → 再规划。

        与 `escalate_quarantined` 的旧行为（`replan_budget_exhausted` →
        DEAD_LETTER，T21 断言不变）并存；本方法只处理「还想再规划」的路径。
        """
        if acb.state is not TaskState.QUARANTINED:
            return None
        if advance(acb, "escalate") != Status.OK:
            return None
        return self.replan(acb, plan, diagnosis=diagnosis)

    def _dead_letter_after_replan(self, acb: ACB) -> None:
        """预算耗尽后的 DEAD_LETTER 结算（与 `escalate_quarantined` 同序）。

        R9：settle 之后必须**归档并摘除预算节点**，否则节点永久留在 `_nodes`、
        父 run 永不归档（原测试把残留固化成计数快照）。
        """
        for rid in list(acb.reservation_ids):
            self.budget.settle(rid)
        acb.reservation_ids = []
        self.slots.release(acb.id)
        if acb.budget_node is not None:
            node_id = acb.budget_node.node_id
            if not self.budget.archive_node(node_id):
                self.budget.remove_node(node_id)
            acb.budget_node = None
        plan = self.plans.get(acb.run_id)
        # B-2：旧 task_id 可能已被 patch 改名 → 用一跳 alias 解析到当前版本 task
        task = self.planner.task_for(plan, acb.task_id) if plan else None
        if not isinstance(task, dict) and plan:
            task = plan.get("tasks", {}).get(acb.task_id)
        plan_version = task.get("plan_version", 1) if isinstance(task, dict) else 1
        task_id = task.get("task_id", acb.task_id) if isinstance(task, dict) \
            else acb.task_id
        self.propagate_failure(task_id, plan_version)
        self._safe_save(acb)

    def _readmit_after_replan(self, acb: ACB, new_plan: dict) -> ACB:
        """S13b §2.3 P0-6：清理旧节点后再准入（顺序与 `requeue_readmit` 一致）。

        ```
        release old slot → settle old reservation_ids → remove_node/archive_node
        → try_admit 新 → 新 ACB；旧节点 consumed>0 时新 min_budget 扣掉已消耗
        ```

        若只 `try_admit` 而**不清理旧节点**：旧节点 `self_reserved` 不归零 →
        `root.subtree_reserved` 双份（违反 I5）；`remove_node` 又因 ACTIVE 预留
        返回 False → 旧节点永久残留、父节点永不归档。

        R1：**绝不复用旧 ACB 的 token** —— 旧 token 的 `agent_id` 绑定旧 id，
        新 ACB id 必然不同 → `DENIED_CAPABILITY`。此处 `token=None` 让 policy 按
        新 id 解析，随后以旧 token 的 **scope** 为新 id 重新签发（`_adopt_capability`）。
        R7：成功时在**同一临界区**内把旧 ACB 标 `superseded` 并落盘，再 pop。
        """
        task = self.planner.task_for(new_plan, acb.logical_subgoal_id or acb.task_id)
        if not isinstance(task, dict):
            acb.needs_readmit = True
            self._drain_skipped.append((acb.task_id, "replan_no_task"))
            self._safe_save(acb)
            return acb
        old_node = acb.budget_node
        old_rids = list(acb.reservation_ids)
        consumed = int(getattr(old_node, "consumed", 0) or 0) if old_node else 0
        # ① release old slot
        self.slots.release(acb.id)
        # ② settle old reservation_ids
        for rid in old_rids:
            self.budget.settle(rid)
        acb.reservation_ids = []
        # ③ remove_node / archive_node（ACTIVE 预留已结算 → 可摘除）
        if old_node is not None:
            if not self.budget.remove_node(old_node.node_id):
                self.budget.archive_node(old_node.node_id)
            acb.budget_node = None
        # ④ try_admit 新；已消耗部分从新 min_budget 扣除（不重复计账）
        fresh_task = copy.deepcopy(task)
        footprint = fresh_task.setdefault("footprint", {})
        if consumed > 0:
            footprint["min_budget"] = max(
                0, int(footprint.get("min_budget", 0) or 0) - consumed)
        fresh = self.dispatcher.try_admit(
            fresh_task, acb.run_id, token=None, plan=new_plan)
        if fresh.state is not TaskState.ADMITTED:
            acb.needs_readmit = True
            self._safe_save(acb)
            return acb
        fresh.replan_count = acb.replan_count
        fresh.replan_attempts = acb.replan_attempts
        # C6：replan 后的新 ACB 仍属于原子树 —— 继承父身份、子清单与父能力快照。
        if acb.parent_id:
            fresh.parent_id = acb.parent_id
        fresh.children_ids = list(acb.children_ids)
        fresh.children_pending = copy.deepcopy(acb.children_pending)
        self._adopt_capability(acb, fresh)
        # R7：同一临界区内标记 + 落盘 + 摘除（避免 `_finalize` 覆盖快照）
        acb.superseded = True
        acb.needs_readmit = False
        self._clear_admission(acb=acb)
        self._safe_save(acb)
        self.acbs.pop(acb.id, None)
        self.acbs[fresh.id] = fresh
        self.run_coordinator.register(fresh)
        self._register_admission(fresh)
        # X3：先 save 拿到 **fresh 自己的 ref**，再回填父的 children_ids。
        self.checkpoints.save(fresh)          # §2.4：再准入成功落盘
        self._replace_child_ref(acb, fresh)
        return fresh

    def requeue_readmit(self, acb: ACB) -> ACB:
        """§5.7（K-9）：`needs_readmit` 的 ACB 重新准入（清标记 → 走 dispatcher）。

        成功重新准入后必须清理旧 ACB 的槽位 / 预留 / 预算节点，否则旧账会
        与新准入叠加（槽位泄漏 + `subtree_reserved` 虚高）。

        R1/R6/R7：与 replan 共用 transition 锁；`replace=True` 允许替换同 key
        登记；旧 ACB 标 `superseded` 并落盘；token 按新身份重新签发。
        """
        # S10-2：`RUN_LIMITS_DRIFT` 是**上限被静默改写**的信号，必须由人工处置；
        # 直接 re-admit 会永久抹除漂移告警。拒绝重新准入（保留 needs_readmit）。
        if acb.failure == "RUN_LIMITS_DRIFT":
            acb.needs_readmit = True
            self.checkpoints.save(acb)
            return acb
        with self._transition_lock(acb):
            if getattr(acb, "superseded", False):
                return acb
            acb.needs_readmit = False
            plan = self.plans.get(acb.run_id)
            task = plan.get("tasks", {}).get(acb.task_id) if plan else None
            if task is None:
                self.checkpoints.save(acb)
                return acb
            old_node = acb.budget_node
            old_rids = list(acb.reservation_ids)
            # 先释放旧槽位，避免 max_concurrent 下自阻塞；失败分支再补回
            self.slots.release(acb.id)
            fresh = self.dispatcher.try_admit(
                task, acb.run_id, token=None, plan=plan, replace=True)
            if fresh.state is TaskState.ADMITTED:
                for rid in old_rids:
                    self.budget.settle(rid)
                acb.reservation_ids = []
                if old_node is not None:
                    if not self.budget.remove_node(old_node.node_id):
                        self.budget.archive_node(old_node.node_id)
                    acb.budget_node = None
                self._adopt_capability(acb, fresh)
                acb.superseded = True
                acb.needs_readmit = False
                self._safe_save(acb)
                self.acbs.pop(acb.id, None)
                self.acbs[fresh.id] = fresh
                self.run_coordinator.register(fresh)
                # X3：先 save 拿到 fresh 的 ref，再回填父的 children_ids。
                self.checkpoints.save(fresh)        # §2.4：requeue 成功落盘
                self._replace_child_ref(acb, fresh)
                return fresh
            self.slots.try_acquire(acb.id)
            acb.needs_readmit = True
            self.checkpoints.save(acb)              # §2.4：requeue 失败落盘
            return acb

    def _restore_acb(self, data: dict, resume_policy: str = "RESUME", *,
                     bump_epoch: bool = True) -> ACB:
        """从 checkpoint 重建 ACB；失败改 `needs_readmit`（**不抛**，§4.3）。

        S22：`bump_epoch=False` 供**门面 token 的懒恢复**使用 —— 全局 epoch
        推进（`epoch_scope="global"`）会作废**其他 token** 的租约；新进程里
        旧租约本就不存在，不需要 fence。默认 `True` 保持崩溃恢复的既有语义。
        """
        try:
            acb = ACB(id=data["acb_id"], run_id=data["run_id"],
                      task_id=data["task_id"],
                      logical_subgoal_id=data.get("logical_subgoal_id", ""))
            acb.generation = data["generation"] + 1
            acb.state = TaskState(data["state"])
            acb.turn_seq = data["turn_seq"]
            acb.staged = dict(data["staged"])
            acb.read_set = dict(data["read_set"])
            acb.base_version = dict(data["base_version"])
            # §2.4：优先用 per-ACB 物化游标；旧 checkpoint 回退到 `cursor`
            acb.cursor = data.get("materialized_cursor", data.get("cursor", 0))
            acb.pre_image = dict(data.get("pre_image") or {})
            # P1-8：恢复 subscriptions / materialized（及其水位），否则 context_view 为空
            acb.subscriptions = set(data.get("subscriptions") or [])
            acb.materialized = dict(data.get("materialized") or {})
            acb.materialized_watermark = int(data.get("materialized_watermark", 0) or 0)
            acb.last_committed_seq = int(data.get("last_committed_seq", 0) or 0)
            acb.aging_deadline = data.get("aging_deadline")
            acb.step = data.get("effect_journal_offset", 0)
            acb.hitl_result = data.get("hitl_result")
            acb.rebase_attempts = data.get("rebase_attempts", 0)
            acb.retry_budget = data.get("retry_budget", 3)
            acb.failure = data.get("failure")
            acb.spec = dict(data.get("spec") or {})
            acb.deadline = data.get("deadline")
            acb.no_progress_steps = data.get("no_progress_steps", 0)
            acb.needs_readmit = bool(data.get("needs_readmit", False))
            # ---- S13-0 §2.1：新字段全部 `.get()` 读（兼容旧 checkpoint）----
            hp = data.get("hitl_pending")
            acb.hitl_pending = dict(hp) if isinstance(hp, dict) else None
            ho = data.get("hitl_outcome")
            acb.hitl_outcome = dict(ho) if isinstance(ho, dict) else None
            acb.replan_count = int(data.get("replan_count", 0) or 0)
            acb.replan_attempts = int(data.get("replan_attempts", 0) or 0)
            # S13d §2.5：aging 计数跨恢复继承（否则重规划/重准入会无限超时）
            acb.aging_count = int(data.get("aging_count", 0) or 0)
            acb.redrive_count = int(data.get("redrive_count", 0) or 0)
            acb.consecutive_failures = int(
                data.get("consecutive_failures", 0) or 0)
            # X7：恢复 claim 三字段（观测用）；租约已过期 → 立即视为已释放，
            # 允许重新 claim（本进程的 scheduler 本就没有该 claim）。
            acb.claim_epoch = int(data.get("claim_epoch", 0) or 0)
            acb.claim_worker = str(data.get("claim_worker", "") or "")
            acb.claim_lease_until_ms = int(
                data.get("claim_lease_until_ms", 0) or 0)
            if (acb.claim_lease_until_ms
                    and int(self.clock.now_ms()) >= acb.claim_lease_until_ms):
                self._drain_skipped.append((acb.id, "claim_lease_expired_released"))
                acb.claim_epoch = 0
                acb.claim_worker = ""
                acb.claim_lease_until_ms = 0
            # R7：被 replan/requeue 取代的旧 ACB 即使被旧 ref 恢复，也不得变成
            # 「QUARANTINED + needs_readmit」的僵尸与新 ACB 并存 → 直接终态。
            acb.superseded = bool(data.get("superseded", False))
            if acb.superseded:
                acb.state = TaskState.CANCELLED
                acb.needs_readmit = False
            acb.parent_id = data.get("parent_id", "") or ""
            if not acb.parent_id:
                spawn = acb.spec.get("spawn") if isinstance(acb.spec, dict) else None
                if isinstance(spawn, dict):
                    acb.parent_id = str(spawn.get("parent_id") or "")
            self._index_child(acb.id, acb.parent_id)   # X6：恢复即登记反扫索引
            children: list = []
            for child in data.get("children_ids") or []:
                if isinstance(child, (list, tuple)) and child:
                    children.append((child[0],
                                     child[1] if len(child) > 1 else None))
            acb.children_ids = children
            # C1：恢复第一阶段登记（spawn 崩溃窗口），由 recover 做对账。
            pending_raw = data.get("children_pending")
            if isinstance(pending_raw, dict):
                acb.children_pending = {
                    str(cid): dict(info) if isinstance(info, dict) else {}
                    for cid, info in pending_raw.items()
                }
            # F6：能力来源不可知 → fail-closed，不回退 `default_token`（后者会
            # 静默抹掉 attenuation）。空串是**哨兵**：`_effective_token` 只判
            # `is not None`，空串会原样返回并被 gateway 判为不可解析 → 拒绝；
            # 不能留 None（会落到 `policy.default_token` 而 fail-open）。
            #
            # 判定依据（F10 的 schema_version 消费者）：
            #   ① 缺 `capability` 键（旧扁平 payload）→ 丢失；
            #   ② 有 `capability` 但为 None 且**无** `schema_version`（F10 前的
            #      旧 payload / 旧 bug 落盘的 None）→ 丢失；
            #   ③ 当前 schema（`schema_version` 存在）且 None → 本进程确实未签发
            #      token，保持 None（既有回退语义）。
            cap = data.get("capability")
            legacy_payload = "schema_version" not in data
            if cap is None and ("capability" not in data or legacy_payload):
                acb.capability = ""
                acb.needs_readmit = True
                acb.failure = acb.failure or "CAPABILITY_LOST"
            elif cap is not None:
                self._restore_capability(acb, cap)
            if not acb.superseded:
                self._restore_budget(acb, data)
            # §2.4：`slots_active` 交叉校验——非终态快照若不含本 ACB 槽位，
            # 说明快照摄于槽位释放之后（如 `_finalize` 末），标记 needs_readmit。
            # F5：`WAITING_APPROVAL` 本来就**不**占槽位，必须豁免。
            snap_active = data.get("slots_active")
            if (isinstance(snap_active, list) and snap_active
                    and acb.state not in TASK_TERMINAL
                    and acb.state is not TaskState.WAITING_APPROVAL
                    and acb.id not in snap_active):
                acb.needs_readmit = True
            # F5：等待态不占槽位（与 `_finalize` 一致）；其它状态恢复即重新占用。
            if (acb.state is not TaskState.WAITING_APPROVAL
                    and not self.slots.try_acquire(acb.id)):
                acb.state = TaskState.QUARANTINED
                acb.failure = "NO_SLOT"
            if bump_epoch:
                self.bump_epoch()
            return acb
        except Exception:  # noqa: BLE001  # 恢复失败 → needs_readmit，不抛
            acb = ACB(id=data.get("acb_id", ""), run_id=data.get("run_id", ""),
                      task_id=data.get("task_id", ""))
            acb.state = TaskState.QUARANTINED
            acb.failure = "restore_failed"
            acb.needs_readmit = True
            return acb

    def _restore_capability(self, acb: ACB, cap) -> None:
        """S13-0 §2.1：从 dict 重建 `CapabilityToken`（P1-4：**不得**回退 default_token）。

        payload 已把 token 转成内建类型；恢复后 `_effective_token` 直接命中
        `acb.capability`，不会落到 `policy.default_token`（后者会抹掉 attenuation）。
        F6：字符串 token 在新进程里无法验签（换 secret / 无 signer）→
        `CAPABILITY_LOST` + `needs_readmit`（留告警，而非静默全 DENIED）。

        C5：重建后**必须注册**进 `policy._tokens[acb.id]`，并按父链按需加载
        祖先（`_ensure_policy_token`），否则跨进程恢复后 `_traceable` 全拒。
        """
        if isinstance(cap, dict):
            token = self._token_from_payload(cap, acb.id)
            acb.capability = token
            self._register_token(acb, token)
            if getattr(token, "parent_id", ""):
                self._ensure_policy_token(token.parent_id)
        elif isinstance(cap, str):
            acb.capability = cap
            resolved = self.policy._resolve_raw(cap)
            if resolved is None:
                acb.needs_readmit = True
                acb.failure = acb.failure or "CAPABILITY_LOST"
            else:
                self._register_token(acb, resolved)
                if getattr(resolved, "parent_id", ""):
                    self._ensure_policy_token(resolved.parent_id)
        # None → 不覆盖（旧格式 / 未签发）

    def _restore_budget(self, acb: ACB, data: dict) -> None:
        """恢复预算节点与预留（S10a §2.2 重写）。

        **新格式**（payload 含 `run_limits` / `budget_cursor`）→ 从 EventLog 重放：
        ① `run_limits` 漂移检测（与配置不一致 → `needs_readmit` + `RUN_LIMITS_DRIFT`）；
        ② 重放前**重置基线为 0**（不叠加 checkpoint 聚合 → 修 P1-3 双份）；
        ③ 按 `rid` 幂等（set 语义）；
        ④ 摘要（含 `budget_siblings`）**仅交叉校验**，不等 → `needs_readmit`。

        **旧格式**（手构 dict，无新字段）→ 走旧分支（保 `test_ms2`/`test_s8`
        直接调用不破，§8.3 防护 2）。
        """
        node_id = data.get("budget_node_id")
        run_limits = data.get("run_limits")
        new_format = ("budget_cursor" in data) or ("run_limits" in data)

        if (new_format and run_limits is not None
                and int(run_limits) != int(self.budget.root.limits)):
            acb.reservation_ids = []
            acb.budget_node = None
            acb.state = TaskState.QUARANTINED
            acb.needs_readmit = True
            acb.failure = acb.failure or "RUN_LIMITS_DRIFT"
            return

        if not node_id:
            acb.reservation_ids = []
            acb.budget_node = None
            acb.needs_readmit = True
            return

        # S22：**同进程懒恢复**（token 被 purge 后又用同 id 恢复）时，节点还在
        # `_nodes` 里 —— 直接重绑，绝不触发 `_replay_budget_events`（那会
        # `_reset_baseline()` 重建整棵树，影响其他 token 的在途预留）。
        existing = self.budget._nodes.get(node_id)
        if existing is not None and (not existing.owner or existing.owner == acb.id):
            acb.budget_node = existing
            acb.reservation_ids = list(existing.reservations)
            return

        if (new_format and self.budget.budget_events()
                and self._restore_budget_by_replay(acb, data, node_id)):
            return
        self._restore_budget_ledger(acb, data, node_id)

    def _replay_budget_events(self, cutoff_epoch: int,
                              run_id: str | None = None) -> tuple[bool, list[str]]:
        """从 EventLog 重放 `BUDGET_*` 事件（**重放前重置基线为 0**）。

        epoch 过滤：`budget_epoch < cutoff_epoch` 的事件视为**旧 epoch 已 fence**，
        不重建（真实路径，**不删文件**，§2.3 / S10-4）。
        `run_id` 给定 → **分 run 重放**（K-40/K-43）；否则全局（向后兼容）。
        返回 `(ok, built_nodes)`；失败时已回滚已建节点。
        """
        budget = self.budget
        built: list[str] = []
        with budget.suppressed():
            budget._reset_baseline()
            try:
                for ev in budget.budget_events(run_id):
                    p = ev.payload or {}
                    if ev.kind == EventKind.BUDGET_EPOCH:
                        budget.apply_budget_event(ev)
                        continue
                    if int(p.get("budget_epoch", 0) or 0) < cutoff_epoch:
                        continue
                    if ev.kind == EventKind.BUDGET_NODE_CREATED:
                        nid = p.get("node_id")
                        node = budget.create_node(
                            nid, int(p.get("limits", 0) or 0),
                            parent_id=p.get("parent_id") or "run",
                            tenant=str(p.get("tenant", "") or ""))
                        if node is None:
                            raise ValueError(
                                f"budget replay: create_node({nid!r}) failed")
                        if node.node_id not in built:
                            built.append(node.node_id)
                        continue
                    if not budget.apply_budget_event(ev):
                        raise ValueError(f"budget replay failed at seq={ev.seq}")
            except Exception:  # noqa: BLE001
                # 失败回滚必须**强制摘除**已建节点：`remove_node` 对「有预留 /
                # 已消耗 / 有子」返回 False → 原实现不回滚 → 残留 + 二次传播双份。
                for nid in reversed(built):
                    node = budget._nodes.pop(nid, None)
                    if node is None:
                        continue
                    if node.parent is not None and node in node.parent.children:
                        node.parent.children.remove(node)
                    for rid in list(node.reservations):
                        budget._reservations.pop(rid, None)
                    node.reservations.clear()
                    node.children.clear()
                    node.self_reserved = 0
                    node.subtree_reserved = 0
                    node.consumed = 0
                    node.folded_consumed = 0
                budget.discard_pending()
                # 清 root 残留计数（reserve 已沿祖先链累加）+ 归零 epoch
                budget._reset_baseline()
                return False, []
            budget.discard_pending()
        return True, built

    def _declared_budget_nodes(self, run_id: str | None = None) -> set[str]:
        """EventLog 声明的节点集（`ARCHIVED` 视为已摘除）——树完整性的真相源。

        S12 K-40：`run_id` 给定 → 只统计该 run 的预算事件（分 run 重放）。
        """
        declared: set[str] = set()
        for ev in self.budget.budget_events(run_id):
            p = ev.payload or {}
            nid = p.get("node_id")
            if not nid or nid == "run":
                continue
            if ev.kind == EventKind.BUDGET_ARCHIVED:
                declared.discard(nid)
            else:
                declared.add(nid)
        return declared

    def _restore_budget_by_replay(self, acb: ACB, data: dict, node_id: str) -> bool:
        """重放重建整棵树；成功且含本 ACB 节点 → 返回 True。"""
        cutoff = int(data.get("budget_epoch", 0) or 0)
        ok, _built = self._replay_budget_events(cutoff)
        if not ok:
            acb.reservation_ids = []
            acb.budget_node = None
            acb.needs_readmit = True
            return False
        # 已应用事件身份入去重集合（避免 `_replay_events` 二次投影 → 双份）
        for ev in self.budget.budget_events():
            if ev.event_id:
                self._applied_event_ids.add(ev.event_id)
        node = self.budget._nodes.get(node_id)
        if node is None:
            return False
        # 摘要**仅交叉校验**（非权威）：不等 → needs_readmit
        consistent = True
        expected_self = int(data.get("budget_self", node.self_reserved))
        expected_consumed = int(data.get("budget_consumed", node.consumed))
        if node.self_reserved != expected_self or node.consumed != expected_consumed:
            consistent = False
        if not node.children:
            expected_subtree = int(data.get("budget_subtree", node.subtree_reserved))
            if expected_subtree != node.subtree_reserved:
                consistent = False
        siblings = data.get("budget_siblings")
        if isinstance(siblings, dict):
            for sid, snap in siblings.items():
                if not isinstance(snap, dict):
                    continue
                sib = self.budget._nodes.get(sid)
                if sib is None:
                    continue
                if (int(snap.get("self_reserved", sib.self_reserved)) != sib.self_reserved
                        or int(snap.get("consumed", sib.consumed)) != sib.consumed):
                    consistent = False
        # 树完整性（§2.3）：declared（EventLog） vs rebuilt（_nodes）
        if self._declared_budget_nodes() != (set(self.budget._nodes) - {"run"}):
            consistent = False
        acb.budget_node = node
        acb.reservation_ids = list(node.reservations)
        acb.needs_readmit = acb.needs_readmit or not consistent
        return True

    def _restore_budget_ledger(self, acb: ACB, data: dict, node_id: str) -> None:
        """旧分支：逐笔账本为准，聚合快照仅作交叉校验（§5.1 / K-6）。

        - **逐笔账本为准**：ACTIVE 按 `amount`、已结算按 `consumed` 重建
          `self_reserved`；`consumed = Σconsumed`；
        - 聚合快照仅作交叉校验，不一致 → 保持 `needs_readmit=True`；
        - 无 `reservations`（旧 checkpoint）→ 退回聚合快照兼容路径，
          但**不能**据此把 `needs_readmit` 清成 False（无逐笔依据）。
        """
        node = self.budget._nodes.get(node_id)
        if node is None:
            path = data.get("budget_path") or []
            if path:
                limits = data.get("budget_path_limits")
                if not isinstance(limits, dict):
                    limits = {node_id: data.get("budget_limits")}
                try:
                    self._restore_path(path, limits=limits)
                except (ValueError, KeyError, RuntimeError):
                    acb.reservation_ids = []
                    acb.budget_node = None
                    acb.needs_readmit = True
                    return
                node = self.budget._nodes.get(node_id)
            if node is None:
                limits = int(data.get("budget_limits") or 0)
                parent_id = data.get("budget_parent_id") or "run"
                if limits <= 0:
                    acb.reservation_ids = []
                    acb.budget_node = None
                    acb.needs_readmit = True
                    return
                node = self.budget.create_node(node_id, limits, parent_id=parent_id)
                if node is None:
                    acb.reservation_ids = []
                    acb.budget_node = None
                    acb.needs_readmit = True
                    return

        # `reservations` 键存在（含空列表）→ 新格式逐笔账本；缺失 → 旧格式
        has_ledger = isinstance(data.get("reservations"), list)
        consistent = True
        if has_ledger:
            restored: list[str] = []
            for item in data.get("reservations") or []:
                rid = item.get("rid")
                if not rid:
                    continue
                existing = self.budget._reservations.get(rid)
                # 仅当既有句柄属于**另一活节点**时跳过；已归档的 tombstone 可复用
                if (existing is not None and existing[0] is not node
                        and existing[0] in self.budget._nodes.values()):
                    continue
                resv = Reservation(rid, item.get("node_id") or node_id,
                                   int(item.get("amount", 0)),
                                   int(item.get("consumed", 0)),
                                   item.get("state", "ACTIVE"))
                node.reservations[rid] = resv
                self.budget._reservations[rid] = (node, resv)
                restored.append(rid)
            # 逐笔重建：ACTIVE 占 amount；SETTLED/RELEASED/ARCHIVED 只占 consumed
            node.folded_consumed = int(data.get("budget_folded_consumed", 0) or 0)
            node.self_reserved = sum(
                r.amount if r.state == "ACTIVE" else r.consumed
                for r in node.reservations.values()) + node.folded_consumed
            node.consumed = (sum(r.consumed for r in node.reservations.values())
                             + node.folded_consumed)
            # 聚合快照只作交叉校验；不一致 → 保持 needs_readmit（不覆盖）
            expected_self = int(data.get("budget_self", node.self_reserved))
            expected_consumed = int(data.get("budget_consumed", node.consumed))
            consistent = (node.self_reserved == expected_self
                          and node.consumed == expected_consumed)
            acb.reservation_ids = restored
        else:
            # 兼容路径：旧 checkpoint 无逐笔账本 → 用聚合快照（不可清 needs_readmit）
            node.folded_consumed = int(data.get("budget_folded_consumed", 0) or 0)
            if not node.children:
                node.self_reserved = int(data.get("budget_self", 0))
                node.consumed = int(data.get("budget_consumed", 0))
            restored = []
            consumed = node.consumed
            for index, rid in enumerate(list(data.get("reservation_ids") or [])):  # legacy 字段
                existing = self.budget._reservations.get(rid)
                if (existing is not None and existing[0] is not node
                        and existing[0] in self.budget._nodes.values()):
                    continue
                if index == 0:
                    resv = Reservation(rid, node_id, node.self_reserved, consumed)
                else:
                    resv = Reservation(rid, node_id, 0, 0, "SETTLED")
                node.reservations[rid] = resv
                self.budget._reservations[rid] = (node, resv)
                restored.append(rid)
            acb.reservation_ids = restored
            consistent = False

        old_subtree = node.subtree_reserved
        node.subtree_reserved = node.self_reserved + sum(
            c.subtree_reserved for c in node.children)
        # §2.4：`budget_subtree` 交叉校验（叶节点严格；有 children 时子树按子节点重算，
        # 可与快照合法偏离 → 不据此判不一致）
        if not node.children:
            expected_subtree = int(data.get("budget_subtree", node.subtree_reserved))
            if expected_subtree != node.subtree_reserved:
                consistent = False
        self._propagate_subtree_delta(node, node.subtree_reserved - old_subtree)
        acb.budget_node = node
        acb.needs_readmit = acb.needs_readmit or not consistent

    def _restore_path(self, path: list[str], limits=None) -> list:
        """§5.2（K-7）：自顶向下重建**任意深度**路径。

        - `parent` 恒为路径上的前一个节点（首节点挂到 `run`）；
        - checkpoint 只持久化目标节点的 limits 时，中间节点用同一上限兜底；
        - 环检测 + 失败时逆序回滚已建节点。
        """
        if len(path) != len(set(path)):
            raise ValueError("path has cycle")
        limits = limits or {}
        default_limit = 0
        for value in limits.values():
            try:
                default_limit = max(default_limit, int(value or 0))
            except (TypeError, ValueError):
                continue
        built: list = []
        try:
            for i, nid in enumerate(path):
                existing = self.budget._nodes.get(nid)
                if existing is not None:
                    built.append(existing)
                    continue
                parent_id = path[i - 1] if i > 0 else "run"
                lim = int(limits.get(nid) or 0)
                if lim <= 0:
                    lim = default_limit
                if lim <= 0:
                    raise RuntimeError(f"path restore failed at {nid}")
                node = self.budget.create_node(nid, lim, parent_id=parent_id)
                if node is None:
                    raise RuntimeError(f"path restore failed at {nid}")
                built.append(node)
            return built
        except Exception:
            for b in reversed(built):
                if b is not self.budget.root:
                    try:
                        self.budget.remove_node(b.node_id)
                    except Exception:  # noqa: BLE001
                        pass
            raise

    def _replay_events(self, from_seq: int, dedup: bool = True) -> int:
        """§4.3：从 `from_seq` 重放事件，真正调用 workspace 投影。

        投影按 `(resource, version)` 做版本守卫（幂等）；`event_id` 去重集合
        会写入 checkpoint（`applied_event_ids`），跨进程恢复后仍可去重。

        S10a §2.2：增 **budget 投影**（`BUDGET_*` 事件 → 重建树，按 `rid` 幂等）。
        已由 `_restore_budget` 重放过的事件身份在 `_applied_event_ids` 中，
        `dedup=True` 时会被跳过 → 不会双份（修 P1-3）。
        """
        applied = 0
        with self.budget.suppressed():
            for ev in self.event_log.all():
                if ev.seq < from_seq:
                    continue
                if (dedup and ev.event_id
                        and ev.event_id in self._applied_event_ids):
                    continue
                if _is_budget_kind(ev.kind):
                    self.budget.apply_budget_event(ev)
                else:
                    self.workspace.apply_event(ev)
                if dedup and ev.event_id:
                    self._applied_event_ids.add(ev.event_id)
                applied += 1
            self.budget.discard_pending()
        return applied

    @staticmethod
    def _propagate_subtree_delta(node, delta: int) -> None:
        parent = node.parent
        while parent is not None:
            parent.subtree_reserved += delta
            parent = parent.parent


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
