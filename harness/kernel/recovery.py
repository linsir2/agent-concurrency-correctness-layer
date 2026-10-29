"""RecoveryService —— 崩溃恢复 / 预算回放 / 能力恢复簇（S4 从 Harness 迁出，纯位移）。

搬出的是「恢复」整簇（12 个方法）：

- 入口 `recover`（**13 步有序过程**：租约 → load_strict → 收据/日志重建 →
  epoch → ACB 重建 → 事件重放 → probe → COMPENSATE 终态策略 → 不变量 →
  注册/准入 → 子对账 → 暂停态 → tick）；
- ACB 重建（`restore_acb`）与其三分支：能力（`_restore_capability`）、
  预算（`restore_budget` → 重放 / 逐笔账本）、槽位交叉校验；
- 预算重放（`replay_budget_events` / `_restore_budget_by_replay` /
  `declared_budget_nodes` / `_restore_budget_ledger` / `restore_path` /
  `_propagate_subtree_delta`）；
- 事件重放（`replay_events`）与 checkpoint 头部兜底
  （`_pointer_header_fallback`）。

约定（与 `kernel/tasks.py` / `kernel/dispatch.py` 一致）：

- `RecoveryDeps` 只放**稳定对象引用**与 late-lookup callable；可变配置/容器一律在
  调用时读 `self.state`（`acbs` / `drain_skipped` / `applied_event_ids`）。冻结进
  deps 会读到旧值。
- 仍留在 Harness 上的方法（`_ensure_writer_lease` / `_restore_epoch` /
  `bump_epoch` / `tick` / `_register_admission` / `_run_one_compensation` /
  `_token_from_payload` / `_register_token` /
  `_ensure_policy_token`）以 **lambda** 注入，调用时才做属性查找 —— 绑定方法会
  冻结原实现，让测试的类级/实例级打桩失效（`tests/test_s13_hitl.py` 会打桩
  `tick`）。`rebuild_pause_state` 自 S5-e 起直连
  `kernel/admin.py::AdminService`（暂停族的唯一实现），不再经 Harness。
- 跨服务调用只走**公开接缝**：收据链 `receipts.rebuild()`（`_rebuild_receipts`
  的唯一实现）、子任务 `children.index_child` / `reconcile_children_refs` /
  `reconcile_children_pending`。绝不 `._private` 穿透。
- 本模块的构造**不产生任何事件**（`__init__` 只保存引用）。
- **`recover` 的 13 步顺序与每个条件分支逐行不变**（黄金哈希只覆盖提交路径，
  重排恢复步骤不会被任何用例发现 → 这里禁止任何「顺手清理」）。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from ..budget import InvariantError, Reservation, _is_budget_kind
from ..checkpoint import CheckpointCorrupt
from ..types import ACB, TASK_TERMINAL, EventKind, TaskState


@dataclass(frozen=True)
class RecoveryDeps:
    # ---- 稳定对象引用（构造期注入，不随配置变化）----
    budget: Any
    checkpoints: Any
    clock: Any
    comp_journal: Any
    effects: Any
    event_log: Any
    policy: Any
    run_coordinator: Any
    slots: Any
    workspace: Any
    receipts: Any                  # ReceiptChain：只用公开接缝（rebuild）
    children: Any                  # ChildManager：只用公开接缝（index/reconcile）
    # ---- 仍留在 Harness 上的方法：late lookup，绝不传绑定方法 ----
    ensure_writer_lease: Callable[..., bool]
    restore_epoch: Callable[..., int]
    bump_epoch: Callable[..., int]
    tick: Callable[..., Any]
    register_admission: Callable[[ACB], None]
    run_one_compensation: Callable[..., None]
    rebuild_pause_state: Callable[[], None]
    token_from_payload: Callable[..., Any]
    register_token: Callable[..., None]
    ensure_policy_token: Callable[..., Any]
    # 本簇内、但会被测试/基准在**实例级打桩**的两个（走 late lookup 回 Harness，
    # 否则 `monkeypatch.setattr(h, "replay_budget_events", ...)` 静默失效）：
    #   - `tests/test_s10_budget_reconcile.py` 用 `replay_budget_events` 做
    #     「禁用重放 → 必超发」的负对照；
    #   - `bench/run_bench.py` 给 `replay_events` 套计时包装。
    replay_budget_events: Callable[..., tuple[bool, list[str]]]
    replay_events: Callable[..., int]


class RecoveryService:
    """恢复/预算回放/能力恢复唯一实现（Harness 仅保留同名委托，S5 删除）。"""

    def __init__(self, deps: RecoveryDeps, state) -> None:
        self.deps = deps
        self.state = state

    def _pointer_header_fallback(self, ref: str) -> dict:
        """P1-6：ref 头部为空时，从 checkpoint 的 latest 指针兜底取元数据。"""
        fallback = getattr(self.deps.checkpoints, "_header_from_pointers", None)
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
        if not self.deps.ensure_writer_lease():
            header = self._pointer_header_fallback(ref)
            acb = ACB(id=header.get("acb_id", ""),
                      run_id=header.get("run_id", ""),
                      task_id=header.get("task_id", ""))
            acb.generation = int(header.get("generation", 1) or 1) + 1
            acb.state = TaskState.QUARANTINED
            acb.failure = "WRITER_LEASE_HELD"
            acb.needs_readmit = True
            if acb.id:
                self.state.acbs[acb.id] = acb
                self.deps.run_coordinator.register(acb)
            return acb
        try:
            data = self.deps.checkpoints.load_strict(ref)  # 绕过缓存，校验双 checksum
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
            self.state.acbs[acb.id] = acb
            self.deps.run_coordinator.register(acb)
            return acb
        events = self.deps.event_log.all()                  # 锁内快照（单一数据源）
        self.deps.receipts.rebuild()                       # §2.3.4：head/id 从 EventLog 重建
        mine = [e for e in events if e.producer == data.get("acb_id")]
        self.deps.effects.rebuild_from(mine)
        self.deps.comp_journal.rebuild_from(mine)
        self.deps.restore_epoch()                          # S10a §3.4：epoch 读持久化值
        acb = self.restore_acb(data, resume_policy)
        # 恢复已应用事件集合（去重键持久化，避免重放重复投影）
        persisted = data.get("applied_event_ids")
        if persisted:
            self.state.applied_event_ids.update(persisted)
        # ① 先重放事件（把外部状态对齐），再 probe
        self.deps.replay_events(
            from_seq=int(data.get("event_cursor") or 0), dedup=True)
        for entry in self.deps.effects.pending(acb.id):     # 含 INTENT
            probe = self.deps.effects.probe_one(entry.idem_key)
            if probe == "True":
                self.deps.effects.confirm(entry.idem_key, {"recovered": True})
            elif probe == "None":
                acb.state = TaskState.QUARANTINED
                acb.failure = "NEEDS_RECONCILIATION"
        # ② COMPENSATE 是终态策略：补偿前丢弃 staged，补偿后不恢复执行
        if resume_policy == "COMPENSATE":
            acb.staged = {}
            plan = self.deps.comp_journal.resume(acb, self.deps.workspace)
            for e in plan["retry"]:
                self.deps.run_one_compensation(acb, e)
            acb.staged = {}                     # 终态策略：补偿后不恢复执行
            # 重试后重新读取条目终态：escalate（FAILED）/ pivot 必须被消费
            entries = [e for e in self.deps.comp_journal._entries.values()
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
                self.deps.budget.settle(rid)
            if (acb.budget_node is not None
                    and self.deps.budget.archive_node(acb.budget_node.node_id)):
                acb.budget_node = None
            if acb.state in (TaskState.DONE_PARTIAL, TaskState.DEAD_LETTER):
                self.deps.slots.release(acb.id)
            self.deps.checkpoints.save(acb)
        # ③ 恢复后全树必查（K-14）；失败 → QUARANTINED + slots.release
        #    S10a §2.3：跨进程恢复额外做**树完整性**校验（锚定 EventLog 声明集）
        try:
            self.deps.budget.check_invariants(cross_process=True)
        except InvariantError:
            acb.state = TaskState.QUARANTINED
            acb.failure = acb.failure or "INVARIANT_BROKEN"
            self.deps.slots.release(acb.id)
            self.deps.checkpoints.save(acb)
        self.state.acbs[acb.id] = acb
        self.deps.run_coordinator.register(acb)
        # R6：恢复出的活跃 ACB 也纳入准入登记（避免同 key 再次 try_admit 复制一份）
        self.deps.register_admission(acb)
        # C1：对账两阶段窗口留下的 `children_pending` / ref=None 子（找回 /
        # 孤儿结算留痕）。
        self.deps.children.reconcile_children_refs(acb)
        self.deps.children.reconcile_children_pending(acb)
        # S15 D3：从事件流重建 Admin 暂停态（重启后仍暂停）
        self.deps.rebuild_pause_state()
        # S13a P0-3：恢复结束即驱动一次超时结算（否则恢复出的过期审批永不结算）。
        # S14 K-69：此处 **persist_scan=False** —— 恢复尚未完成，别的 ACB 可能只
        # 恢复了一半，此时结算它们是错的（S13 评审 R-5）。恢复后的首次 `pump()`
        # 会做持久扫描补上。
        self.deps.tick(persist_scan=False)
        return acb

    def restore_acb(self, data: dict, resume_policy: str = "RESUME", *,
                     bump_epoch: bool = True, for_sweep: bool = False) -> ACB:
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
            # S24-P0：`aging_deadline` 被 checkpoint 写入（checkpoint.py:388）
            # 却从未被读回 —— 重启后持久期限凭空消失，`pump()` 无法结算。
            aging_deadline = data.get("aging_deadline")
            acb.aging_deadline = (int(aging_deadline)
                                  if isinstance(aging_deadline, (int, float))
                                  else None)
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
                    and int(self.deps.clock.now_ms()) >= acb.claim_lease_until_ms):
                self.state.drain_skipped.append(
                    (acb.id, "claim_lease_expired_released"))
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
            self.deps.children.index_child(acb.id, acb.parent_id)   # X6：恢复即登记反扫索引
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
                self.restore_budget(acb, data)
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
            # S24-P0：`for_sweep=True` 只水合字段（只读扫描），不得改槽位/epoch。
            if (not for_sweep and acb.state is not TaskState.WAITING_APPROVAL
                    and not self.deps.slots.try_acquire(acb.id)):
                acb.state = TaskState.QUARANTINED
                acb.failure = "NO_SLOT"
            if bump_epoch and not for_sweep:
                self.deps.bump_epoch()
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
            token = self.deps.token_from_payload(cap, acb.id)
            acb.capability = token
            self.deps.register_token(acb, token)
            if getattr(token, "parent_id", ""):
                self.deps.ensure_policy_token(token.parent_id)
        elif isinstance(cap, str):
            acb.capability = cap
            resolved = self.deps.policy._resolve_raw(cap)
            if resolved is None:
                acb.needs_readmit = True
                acb.failure = acb.failure or "CAPABILITY_LOST"
            else:
                self.deps.register_token(acb, resolved)
                if getattr(resolved, "parent_id", ""):
                    self.deps.ensure_policy_token(resolved.parent_id)
        # None → 不覆盖（旧格式 / 未签发）

    def restore_budget(self, acb: ACB, data: dict) -> None:
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
                and int(run_limits) != int(self.deps.budget.root.limits)):
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
        # `_nodes` 里 —— 直接重绑，绝不触发 `replay_budget_events`（那会
        # `_reset_baseline()` 重建整棵树，影响其他 token 的在途预留）。
        existing = self.deps.budget._nodes.get(node_id)
        if existing is not None and (not existing.owner or existing.owner == acb.id):
            acb.budget_node = existing
            acb.reservation_ids = list(existing.reservations)
            return

        if (new_format and self.deps.budget.budget_events()
                and self._restore_budget_by_replay(acb, data, node_id)):
            return
        self._restore_budget_ledger(acb, data, node_id)

    def replay_budget_events(self, cutoff_epoch: int,
                              run_id: str | None = None) -> tuple[bool, list[str]]:
        """从 EventLog 重放 `BUDGET_*` 事件（**重放前重置基线为 0**）。

        epoch 过滤：`budget_epoch < cutoff_epoch` 的事件视为**旧 epoch 已 fence**，
        不重建（真实路径，**不删文件**，§2.3 / S10-4）。
        `run_id` 给定 → **分 run 重放**（K-40/K-43）；否则全局（向后兼容）。
        返回 `(ok, built_nodes)`；失败时已回滚已建节点。
        """
        budget = self.deps.budget
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

    def declared_budget_nodes(self, run_id: str | None = None) -> set[str]:
        """EventLog 声明的节点集（`ARCHIVED` 视为已摘除）——树完整性的真相源。

        S12 K-40：`run_id` 给定 → 只统计该 run 的预算事件（分 run 重放）。
        """
        declared: set[str] = set()
        for ev in self.deps.budget.budget_events(run_id):
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
        ok, _built = self.deps.replay_budget_events(cutoff)
        if not ok:
            acb.reservation_ids = []
            acb.budget_node = None
            acb.needs_readmit = True
            return False
        # 已应用事件身份入去重集合（避免 `replay_events` 二次投影 → 双份）
        for ev in self.deps.budget.budget_events():
            if ev.event_id:
                self.state.applied_event_ids.add(ev.event_id)
        node = self.deps.budget._nodes.get(node_id)
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
                sib = self.deps.budget._nodes.get(sid)
                if sib is None:
                    continue
                if (int(snap.get("self_reserved", sib.self_reserved)) != sib.self_reserved
                        or int(snap.get("consumed", sib.consumed)) != sib.consumed):
                    consistent = False
        # 树完整性（§2.3）：declared（EventLog） vs rebuilt（_nodes）
        if self.declared_budget_nodes() != (set(self.deps.budget._nodes) - {"run"}):
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
        node = self.deps.budget._nodes.get(node_id)
        if node is None:
            path = data.get("budget_path") or []
            if path:
                limits = data.get("budget_path_limits")
                if not isinstance(limits, dict):
                    limits = {node_id: data.get("budget_limits")}
                try:
                    self.restore_path(path, limits=limits)
                except (ValueError, KeyError, RuntimeError):
                    acb.reservation_ids = []
                    acb.budget_node = None
                    acb.needs_readmit = True
                    return
                node = self.deps.budget._nodes.get(node_id)
            if node is None:
                limits = int(data.get("budget_limits") or 0)
                parent_id = data.get("budget_parent_id") or "run"
                if limits <= 0:
                    acb.reservation_ids = []
                    acb.budget_node = None
                    acb.needs_readmit = True
                    return
                node = self.deps.budget.create_node(node_id, limits, parent_id=parent_id)
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
                existing = self.deps.budget._reservations.get(rid)
                # 仅当既有句柄属于**另一活节点**时跳过；已归档的 tombstone 可复用
                if (existing is not None and existing[0] is not node
                        and existing[0] in self.deps.budget._nodes.values()):
                    continue
                resv = Reservation(rid, item.get("node_id") or node_id,
                                   int(item.get("amount", 0)),
                                   int(item.get("consumed", 0)),
                                   item.get("state", "ACTIVE"))
                node.reservations[rid] = resv
                self.deps.budget._reservations[rid] = (node, resv)
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
                existing = self.deps.budget._reservations.get(rid)
                if (existing is not None and existing[0] is not node
                        and existing[0] in self.deps.budget._nodes.values()):
                    continue
                if index == 0:
                    resv = Reservation(rid, node_id, node.self_reserved, consumed)
                else:
                    resv = Reservation(rid, node_id, 0, 0, "SETTLED")
                node.reservations[rid] = resv
                self.deps.budget._reservations[rid] = (node, resv)
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

    def restore_path(self, path: list[str], limits=None) -> list:
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
                existing = self.deps.budget._nodes.get(nid)
                if existing is not None:
                    built.append(existing)
                    continue
                parent_id = path[i - 1] if i > 0 else "run"
                lim = int(limits.get(nid) or 0)
                if lim <= 0:
                    lim = default_limit
                if lim <= 0:
                    raise RuntimeError(f"path restore failed at {nid}")
                node = self.deps.budget.create_node(nid, lim, parent_id=parent_id)
                if node is None:
                    raise RuntimeError(f"path restore failed at {nid}")
                built.append(node)
            return built
        except Exception:
            for b in reversed(built):
                if b is not self.deps.budget.root:
                    try:
                        self.deps.budget.remove_node(b.node_id)
                    except Exception:  # noqa: BLE001
                        pass
            raise

    def replay_events(self, from_seq: int, dedup: bool = True) -> int:
        """§4.3：从 `from_seq` 重放事件，真正调用 workspace 投影。

        投影按 `(resource, version)` 做版本守卫（幂等）；`event_id` 去重集合
        会写入 checkpoint（`applied_event_ids`），跨进程恢复后仍可去重。

        S10a §2.2：增 **budget 投影**（`BUDGET_*` 事件 → 重建树，按 `rid` 幂等）。
        已由 `restore_budget` 重放过的事件身份在 `_applied_event_ids` 中，
        `dedup=True` 时会被跳过 → 不会双份（修 P1-3）。
        """
        applied = 0
        with self.deps.budget.suppressed():
            for ev in self.deps.event_log.all():
                if ev.seq < from_seq:
                    continue
                if (dedup and ev.event_id
                        and ev.event_id in self.state.applied_event_ids):
                    continue
                if _is_budget_kind(ev.kind):
                    self.deps.budget.apply_budget_event(ev)
                else:
                    self.deps.workspace.apply_event(ev)
                if dedup and ev.event_id:
                    self.state.applied_event_ids.add(ev.event_id)
                applied += 1
            self.deps.budget.discard_pending()
        return applied

    @staticmethod
    def _propagate_subtree_delta(node, delta: int) -> None:
        parent = node.parent
        while parent is not None:
            parent.subtree_reserved += delta
            parent = parent.parent
