"""KernelState —— 跨服务共享的可变状态容器（纯数据，无业务逻辑）。

拆分前这些字段散落在 `Harness.__init__`（507 处 `self._`）里。搬出来的唯一目的
是让服务只依赖「它真正需要的那部分状态」，而不是整个 Harness。

约定：
- 只放**可变容器/标量**，不放组件（组件走各服务的 `deps`）。
- 字段名与旧 `self._xxx` 一一对应，便于机械迁移与回归比对。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any


class _NullLock:
    """空上下文管理器（无外部幂等键时占位，避免分支里写两套 `with`）。"""

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _SpawnAbort(Exception):
    """C1：spawn ② 事务的「明确拒绝」信号（非崩溃），由回滚分支转成 failure。

    TODO(S3)：这是控制流异常，不是状态；S3 抽取 ChildManager 时移到 kernel/children.py。
    """


@dataclass
class KernelState:
    # ---- 身份 / 签名（SigningService）----
    auth: Any = None                 # Signer | None（惰性构造）
    auth_secret: str | None = None
    event_count: int = 0

    # ---- 收据链（ReceiptChain）----
    receipt_heads: dict = field(default_factory=dict)
    receipt_ids: set = field(default_factory=set)
    tainted_scopes: set = field(default_factory=set)
    malformed_receipts: list = field(default_factory=list)
    receipt_scan_watermark: int = 0
    receipt_scan_failed: bool = False
    receipt_refresh_lock: threading.RLock = field(default_factory=threading.RLock)
    scope_locks: dict = field(default_factory=dict)
    ext_key_locks: dict = field(default_factory=dict)
    receipts_flock: Any = None
    run_cursors: dict = field(default_factory=dict)
    lock: threading.RLock = field(default_factory=threading.RLock)

    # ---- 写者租约 / epoch（WriterLeaseService）----
    writer_id: str = ""
    writer_epoch: int = 0
    lease_instance: str = ""
    lease_store: Any = None
    lease_outcome: str = ""
    lease_reason: str = ""
    durable: bool = False
    writer_lease_ok: bool = True

    # ---- 可变配置 / 可替换组件（调用时读取，不得冻结进 deps）----
    lock_timeout_s: float = 5.0
    comp_journal: Any = None
    receipts_enabled: bool = True
    lease_ms: int = 60_000

    # ---- S3.0：调度 / 子任务状态容器（dispatch/tasks/children 抽取前置）----
    # 命名与旧 `self.acbs` / `self._drain_skipped` 一一对应（去掉前导下划线）。
    acbs: dict = field(default_factory=dict)          # acb_id → ACB（活跃任务表）
    plans: dict = field(default_factory=dict)         # run_id → plan
    paused_runs: set = field(default_factory=set)     # S15 D：Admin 暂停的 run
    drain_skipped: list = field(default_factory=list)  # D11：调度跳过原因（不再静默）
    tick_skipped: list = field(default_factory=list)   # S14 K-69：tick 跳过原因
    admission_index: dict = field(default_factory=dict)   # R6：(run,task,ver) → ACB
    transition_locks: dict = field(default_factory=dict)  # R7：每 ACB 一把 transition 锁
    children_index: dict = field(default_factory=dict)    # X6：parent_id → {child_acb_id}
    children_index_built: bool = False

    # ---- S3.0：可变配置（服务/测试在构造后修改，必须调用时读取）----
    scheduler_mode: str = "sync"
    aging_ms: int = 0
    aging_max_attempts: int = 3
    claim_lease_ms: int = 60_000      # 构造函数默认 None → 取 lease_ms
    pool_workers: int = 4
    pool_timeout_s: float = 120.0
    drain_timeout_s: float = 5.0
    hitl_timeout_ms: int = 30_000
    max_redrives: int = 3
    poison_after_failures: int = 3
    max_fanout: int = 6
    min_budget_per_child: int = 1
    spawn_depth: int = 1
    replan_budget: int = 2
    retention: int = 10_000
    max_lease_ttl_s: int = 60
    claim_window_exceeded: int = 0
