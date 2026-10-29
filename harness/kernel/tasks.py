"""TaskService —— 准入 / 执行 / 终态 / 重规划簇（S3.2 从 Harness 迁出，纯位移）。

搬出的是「任务生命周期」整簇（30 个方法）：

- 准入与登记（`_admit_and_register` / `_admission_key_for` / `_lookup_admission`
  / `_register_admission` / `_clear_admission`）与执行入口（`dispatch_and_run`）；
- 终态结算（`finish` / `abort` / `_finalize` / `_finalize_locked` /
  `_maybe_poison_locked` / `escalate_quarantined`）；
- HITL 判态（`_approval_slot` / `_record_approval` / `_hitl_gate` /
  `_terminal_status`）与每 ACB 的 transition 锁；
- 重规划 / 再准入（`replan` / `_prune_id_aliases` / `replan_from_quarantine` /
  `_dead_letter_after_replan` / `_readmit_after_replan` / `requeue_readmit` /
  `redrive_dead_letter` / `propagate_failure` / `_resolve_current_acb` /
  `_admit_ready_successor` / `_adopt_capability`）；
- 管理面回收（`kill_run` / `_reap_expired_tokens`）。

约定（与 `kernel/children.py` / `kernel/commit.py` 一致）：

- `TaskDeps` 只放**稳定对象引用**与 late-lookup callable；可变配置/容器一律在
  调用时读 `self.state`（`plans` / `acbs` / `admission_index` /
  `transition_locks` / `drain_skipped` / `paused_runs` / `replan_budget` /
  `max_redrives` / `poison_after_failures` / `lock`）。冻结进 deps 会读到旧值。
- 仍留在 Harness 上的方法（`_safe_save` / `_effective_token` /
  `_ensure_policy_token` / `run_task` / `purge_run` / token 清理钩子）以
  **lambda** 注入，调用时才做属性查找 —— 绑定方法会冻结原实现，让测试的
  类级/实例级打桩失效（`tests/test_s13_hitl.py` 会打桩 `_hitl_gate` 与 `tick`）。
- 本模块的构造**不产生任何事件**（`__init__` 只保存引用）。
- 事件种类/payload/顺序、锁序、构造顺序全部保持不变（纯位移）。
"""

from __future__ import annotations

import copy
import threading
from dataclasses import dataclass
from typing import Any, Callable

from ..orchestration import classify_verdict
from ..types import (
    ACB,
    TASK_TERMINAL,
    EventKind,
    Result,
    Status,
    TaskState,
    advance,
)


@dataclass(frozen=True)
class TaskDeps:
    # ---- 稳定对象引用（构造期注入，不随配置变化）----
    budget: Any
    checkpoints: Any
    event_log: Any
    hitl: Any
    intents: Any
    policy: Any
    slots: Any
    dispatcher: Any
    planner: Any
    reconcile: Any
    run_coordinator: Any
    scheduler: Any
    children: Any                  # ChildManager：只用公开接缝（replace_child_ref）
    # ---- 仍留在 Harness 上的方法：late lookup，绝不传绑定方法 ----
    safe_save: Callable[..., bool]
    effective_token: Callable[..., Any]
    ensure_policy_token: Callable[..., None]
    run_task: Callable[..., Any]
    purge_run: Callable[..., bool]
    token_cleanup: Callable[..., Any]


class TaskService:
    """准入/执行/终态/重规划唯一实现（Harness 仅保留同名委托，S5 删除）。"""

    def __init__(self, deps: TaskDeps, state) -> None:
        self.deps = deps
        self.state = state

    def _admit_and_register(self, plan: dict, task_id: str, *,
                            required_tools: set[str] | None = None,
                            token=None) -> ACB:
        """S22：准入 + 注册（`dispatch_and_run` 与门面 `begin` 共用）。

        **先登记 plan 再准入**：`_tenant_of`/`_tier_of` 会读 `h.plans[run_id]`，
        顺序反了会让 plan 级 tenant/tier 静默失效（S21 终审 P1）。
        """
        run_id = plan["run_id"]
        self.state.plans[run_id] = plan
        acb = self.deps.dispatcher.try_admit(plan["tasks"][task_id], run_id,
                                             required_tools=required_tools,
                                             token=token, plan=plan)
        self.state.acbs[acb.id] = acb
        self.deps.run_coordinator.register(acb)
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
        self.deps.run_task(acb, decisions, crash_at=crash_at)
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
        self.deps.dispatcher.abort(acb)

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
        self.deps.event_log.append(
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
        with self.deps.hitl._lock:
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
                kind = classify_verdict(self.deps.hitl.resolve(acb))
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
            if not self.deps.slots.try_acquire(acb.id):
                acb.state = TaskState.QUARANTINED
                acb.failure = "NO_SLOT"
                acb.needs_readmit = True
                self.deps.intents.revoke_all(acb.id)
                self._finalize(acb)
                return Status.SEMANTIC_REJECTED
            self.deps.safe_save(acb)
            return None
        self.deps.intents.revoke_all(acb.id)     # 释放租约
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

    @staticmethod
    def _admission_key_for(acb: ACB) -> tuple[str, str, int]:
        return (acb.run_id, acb.task_id,
                int(acb.spec.get("plan_version", 1) or 1))

    def _lookup_admission(self, key: tuple[str, str, int]) -> ACB | None:
        """取同一 `(run_id, task_id, plan_version)` 的**活跃** ACB（无则 None）。

        终态 / `INADMISSIBLE` / `QUARANTINED` / `superseded` → 视为已失效并清登记
        （允许显式重新准入，例如 T29 `requeue_readmit` / 失败后重试）。
        """
        with self.state.lock:
            acb = self.state.admission_index.get(key)
            if acb is None:
                return None
            dead = (acb.state in TASK_TERMINAL
                    or acb.state in (TaskState.INADMISSIBLE, TaskState.QUARANTINED)
                    or getattr(acb, "superseded", False))
            if dead:
                self.state.admission_index.pop(key, None)
                return None
            return acb

    def _register_admission(self, acb: ACB) -> None:
        with self.state.lock:
            self.state.admission_index[self._admission_key_for(acb)] = acb

    def _clear_admission(self, key: tuple[str, str, int] | None = None,
                         acb: ACB | None = None) -> None:
        with self.state.lock:
            if key is not None:
                self.state.admission_index.pop(key, None)
            if acb is not None:
                self.state.admission_index.pop(self._admission_key_for(acb), None)

    def _transition_lock(self, acb: ACB) -> threading.RLock:
        with self.state.lock:
            lock = self.state.transition_locks.get(acb.id)
            if lock is None:
                lock = threading.RLock()
                self.state.transition_locks[acb.id] = lock
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
        if self.state.poison_after_failures <= 0:
            return
        if acb.consecutive_failures < self.state.poison_after_failures:
            return
        # 达到阈值 → 走既有合法边进 DEAD_LETTER（真终态）
        self.escalate_quarantined(acb)
        acb.failure = "POISON_TASK"
        self.deps.safe_save(acb)

    def _finalize_locked(self, acb: ACB) -> None:
        # X1：结算前先释放活跃 claim（幂等）。否则 finalize→release 的顺序会让
        # 「已终态但 claim 未释放」窗口里的另一 worker 误以为仍被持有。
        self.deps.scheduler.release_claim(acb.id)
        plan = self.state.plans.get(acb.run_id)
        if plan and acb.task_id in plan["tasks"]:
            plan["tasks"][acb.task_id]["state"] = acb.state
        # §2.4：_finalize 落盘（K-3：在结算/归档**之前**，保留活跃预留供 recover 恢复）；
        # P1-5：保存失败**不得**阻断下面的结算/归档/槽位释放（否则槽位泄漏）。
        # X8：唯一例外是 claim fencing —— 旧 worker 的快照不得覆盖新 claim，
        # 且结算由新持有者完成，这里直接中止（留痕在 `_drain_skipped`）。
        if not self.deps.safe_save(acb):
            return
        # S10a §2.1：结算 + 归档的预算事件**同帧 append_batch**（一次临界区，
        # 避免逐笔 append 放大事件日志锁竞争 → 基准时序抖动）
        with self.deps.budget.suppressed():
            if acb.state in (TaskState.FAILED, TaskState.CANCELLED, TaskState.DEAD_LETTER,
                             TaskState.DONE, TaskState.DONE_PARTIAL, TaskState.QUARANTINED):
                # 终态 + QUARANTINED 结算全部预留（文档 §5.5：未消耗部分归还，已消耗留账）
                for rid in list(acb.reservation_ids):
                    self.deps.budget.settle(rid)
            if (acb.state in TASK_TERMINAL and acb.budget_node is not None
                    and self.deps.budget.archive_node(acb.budget_node.node_id)):
                # §5.3（K-13）：终态且无 ACTIVE 预留 → 归档（consumed 折入父级）
                acb.budget_node = None
        self.deps.budget.flush_pending()
        # F5：等待审批**不是**占槽位的理由（否则 `max_concurrent` 下等待中的任务会
        # 饿死其他 run）；统一释放，`_hitl_gate` 批准后重新 `try_acquire`。
        # `SlotTracker.release` 幂等，重复释放不会泄漏。
        self.deps.slots.release(acb.id)                 # P1-5：先释放槽位（不因落盘失败跳过）
        if acb.state in (TaskState.FAILED, TaskState.CANCELLED, TaskState.DEAD_LETTER):
            # D14：patch 后旧 ACB 的 task_id 可能已不在 plan["tasks"] 中 → 用 .get() 安全取值
            task = plan.get("tasks", {}).get(acb.task_id) if plan else None
            plan_version = task.get("plan_version", 1) if isinstance(task, dict) else 1
            self.propagate_failure(acb.task_id, plan_version)
        # level-triggered 对账（文档 §5.2.1）：事件即真相
        if plan:
            self.deps.reconcile.reconcile_events(acb.run_id)
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
            ready = set(self.deps.reconcile.rebuild_ready_set(acb.run_id))
            for edge in plan.get("edges", []) or []:
                src, dst = ((edge.get("from"), edge.get("to"))
                            if isinstance(edge, dict) else (edge[0], edge[1]))
                src_task = self.deps.planner.task_for(plan, src)
                same_task = src == acb.task_id or (
                    isinstance(src_task, dict)
                    and src_task.get("task_id") == acb.task_id)
                same_subgoal = bool(acb.logical_subgoal_id) and \
                    self.deps.planner.resolve_alias(src, plan) == acb.logical_subgoal_id \
                    and isinstance(src_task, dict) \
                    and src_task.get("task_id") == acb.task_id
                if not (same_task or same_subgoal):
                    continue
                dst_task = self.deps.planner.task_for(plan, dst)
                if not isinstance(dst_task, dict):
                    self.state.drain_skipped.append((f"{acb.run_id}:{dst}",
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
                    self.state.drain_skipped.append((f"{acb.run_id}:{dst_tid}",
                                                     "push_no_current_acb"))
                    continue
                self.deps.scheduler.push(target.id)

    def escalate_quarantined(self, acb: ACB) -> Result:
        """人工 escalate：QUARANTINED → DEAD_LETTER，释放预算并传播 failure（文档 §5.5）。"""
        if acb.state is not TaskState.QUARANTINED:
            return Result(Status.INVALID_INPUT, {"state": acb.state.value})
        r = advance(acb, "replan_budget_exhausted")     # QUARANTINED → DEAD_LETTER（合法边）
        if r != Status.OK:
            return r
        for rid in list(acb.reservation_ids):
            self.deps.budget.settle(rid)
        self.deps.slots.release(acb.id)
        plan = self.state.plans.get(acb.run_id)
        if plan and acb.task_id in plan["tasks"]:
            self.propagate_failure(acb.task_id,
                                   plan["tasks"][acb.task_id].get("plan_version", 1))
        self.deps.checkpoints.save(acb)              # §2.4：escalate 落盘
        return Result(Status.OK, {"state": acb.state.value})

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
        plan = plan if plan is not None else self.state.plans.get(acb.run_id)
        if plan is None:
            return None
        with self._transition_lock(acb):
            if acb.state not in (TaskState.QUARANTINED, TaskState.REPLANNING):
                return Result(Status.INVALID_INPUT, {
                    "reason": "invalid_replan_state", "state": acb.state.value})
            exhausted = (acb.replan_count >= self.state.replan_budget
                         or acb.replan_attempts > self.state.replan_budget)
            if exhausted:                              # ① 先查预算 / 风暴熔断
                if advance(acb, "replan_budget_exhausted") == Status.OK:
                    self._dead_letter_after_replan(acb)     # → DEAD_LETTER
                else:
                    self.deps.safe_save(acb)
                return None
            prev = acb.replan_count                    # ② 计数（成功预算）
            acb.replan_count = prev + 1
            acb.replan_attempts = acb.replan_attempts + 1   # 尝试数（永不回滚）
            try:
                # ③ write-ahead 落盘 —— X5：必须**在 try 内**。pool 模式下
                # `FencingRejected` 会在这里抛出；放在 try 外则内存已 +1 而
                # 磁盘仍是旧值且无法回滚（池化旧 worker 的写被 fence）。
                self.deps.checkpoints.save(acb)
                new_plan, event_draft = self.deps.planner.patch(   # ④ 纯函数 patch
                    plan, diagnosis or {})
                # ⑤ 同帧提交：新 plan 事件 + 计数（已落盘）原子可见
                self.deps.event_log.append_batch([event_draft])
            except Exception:                          # 不捕获 BaseException
                acb.replan_count = prev                # 补偿：只回滚成功计数
                # `replan_attempts` 仍**永不回滚**（熔断 replan 风暴，R4）。
                if acb.state is TaskState.REPLANNING:
                    acb.state = TaskState.QUARANTINED  # 非终态不得卡死
                self.deps.safe_save(acb)                   # 磁盘回滚（可能已被 fence）
                raise
            self.state.plans[acb.run_id] = new_plan
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
        for acb in list(self.state.acbs.values()):
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
            self.deps.budget.settle(rid)
        acb.reservation_ids = []
        self.deps.slots.release(acb.id)
        if acb.budget_node is not None:
            node_id = acb.budget_node.node_id
            if not self.deps.budget.archive_node(node_id):
                self.deps.budget.remove_node(node_id)
            acb.budget_node = None
        plan = self.state.plans.get(acb.run_id)
        # B-2：旧 task_id 可能已被 patch 改名 → 用一跳 alias 解析到当前版本 task
        task = self.deps.planner.task_for(plan, acb.task_id) if plan else None
        if not isinstance(task, dict) and plan:
            task = plan.get("tasks", {}).get(acb.task_id)
        plan_version = task.get("plan_version", 1) if isinstance(task, dict) else 1
        task_id = task.get("task_id", acb.task_id) if isinstance(task, dict) \
            else acb.task_id
        self.propagate_failure(task_id, plan_version)
        self.deps.safe_save(acb)

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
        task = self.deps.planner.task_for(new_plan, acb.logical_subgoal_id or acb.task_id)
        if not isinstance(task, dict):
            acb.needs_readmit = True
            self.state.drain_skipped.append((acb.task_id, "replan_no_task"))
            self.deps.safe_save(acb)
            return acb
        old_node = acb.budget_node
        old_rids = list(acb.reservation_ids)
        consumed = int(getattr(old_node, "consumed", 0) or 0) if old_node else 0
        # ① release old slot
        self.deps.slots.release(acb.id)
        # ② settle old reservation_ids
        for rid in old_rids:
            self.deps.budget.settle(rid)
        acb.reservation_ids = []
        # ③ remove_node / archive_node（ACTIVE 预留已结算 → 可摘除）
        if old_node is not None:
            if not self.deps.budget.remove_node(old_node.node_id):
                self.deps.budget.archive_node(old_node.node_id)
            acb.budget_node = None
        # ④ try_admit 新；已消耗部分从新 min_budget 扣除（不重复计账）
        fresh_task = copy.deepcopy(task)
        footprint = fresh_task.setdefault("footprint", {})
        if consumed > 0:
            footprint["min_budget"] = max(
                0, int(footprint.get("min_budget", 0) or 0) - consumed)
        fresh = self.deps.dispatcher.try_admit(
            fresh_task, acb.run_id, token=None, plan=new_plan)
        if fresh.state is not TaskState.ADMITTED:
            acb.needs_readmit = True
            self.deps.safe_save(acb)
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
        self.deps.safe_save(acb)
        self.state.acbs.pop(acb.id, None)
        self.state.acbs[fresh.id] = fresh
        self.deps.run_coordinator.register(fresh)
        self._register_admission(fresh)
        # X3：先 save 拿到 **fresh 自己的 ref**，再回填父的 children_ids。
        self.deps.checkpoints.save(fresh)          # §2.4：再准入成功落盘
        self.deps.children.replace_child_ref(acb, fresh)
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
            self.deps.checkpoints.save(acb)
            return acb
        with self._transition_lock(acb):
            if getattr(acb, "superseded", False):
                return acb
            acb.needs_readmit = False
            plan = self.state.plans.get(acb.run_id)
            task = plan.get("tasks", {}).get(acb.task_id) if plan else None
            if task is None:
                self.deps.checkpoints.save(acb)
                return acb
            old_node = acb.budget_node
            old_rids = list(acb.reservation_ids)
            # 先释放旧槽位，避免 max_concurrent 下自阻塞；失败分支再补回
            self.deps.slots.release(acb.id)
            fresh = self.deps.dispatcher.try_admit(
                task, acb.run_id, token=None, plan=plan, replace=True)
            if fresh.state is TaskState.ADMITTED:
                for rid in old_rids:
                    self.deps.budget.settle(rid)
                acb.reservation_ids = []
                if old_node is not None:
                    if not self.deps.budget.remove_node(old_node.node_id):
                        self.deps.budget.archive_node(old_node.node_id)
                    acb.budget_node = None
                self._adopt_capability(acb, fresh)
                acb.superseded = True
                acb.needs_readmit = False
                self.deps.safe_save(acb)
                self.state.acbs.pop(acb.id, None)
                self.state.acbs[fresh.id] = fresh
                self.deps.run_coordinator.register(fresh)
                # X3：先 save 拿到 fresh 的 ref，再回填父的 children_ids。
                self.deps.checkpoints.save(fresh)        # §2.4：requeue 成功落盘
                self.deps.children.replace_child_ref(acb, fresh)
                return fresh
            self.deps.slots.try_acquire(acb.id)
            acb.needs_readmit = True
            self.deps.checkpoints.save(acb)              # §2.4：requeue 失败落盘
            return acb

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
        if count >= self.state.max_redrives:
            self.state.drain_skipped.append((acb.id, "redrive_limit_exceeded"))
            return None
        plan = self.state.plans.get(acb.run_id)
        task = (plan.get("tasks", {}) or {}).get(acb.task_id) if plan else None
        if task is None:
            self.state.drain_skipped.append((acb.id, "redrive_task_missing"))
            return None

        with self._transition_lock(acb):
            old_node = acb.budget_node
            old_rids = list(acb.reservation_ids or [])
            self.deps.slots.release(acb.id)
            fresh = self.deps.dispatcher.try_admit(
                task, acb.run_id, token=None, plan=plan, replace=True)
            if fresh.state is not TaskState.ADMITTED:
                self.state.drain_skipped.append((acb.id, "redrive_admit_failed"))
                return None
            for rid in old_rids:
                self.deps.budget.settle(rid)
            acb.reservation_ids = []
            if old_node is not None:
                if not self.deps.budget.remove_node(old_node.node_id):
                    self.deps.budget.archive_node(old_node.node_id)
                acb.budget_node = None
            fresh.redrive_count = count + 1
            fresh.parent_id = acb.parent_id
            self._adopt_capability(acb, fresh)
            # 旧 ACB 保持终态（不 superseded 成 CANCELLED，避免改写历史结论）
            acb.needs_readmit = False
            self.deps.safe_save(acb)
            self.deps.safe_save(fresh)
            self.state.acbs[fresh.id] = fresh
            self.deps.run_coordinator.register(fresh)
            self._register_admission(fresh)
            self.deps.event_log.append(
                EventKind.DLQ_REDRIVEN, acb.run_id,
                {"from_acb_id": acb.id, "to_acb_id": fresh.id,
                 "attempt": fresh.redrive_count, "task_id": acb.task_id},
                run_id=acb.run_id)
            return fresh

    def propagate_failure(self, task_id: str, plan_version: int) -> None:
        """只传播到同一 Plan 版本的后继（v1.10 修复 P0-3；R3 统一 alias）。

        R3：边仍写改名前的旧 id（`T1`），失败/取消来自当前 task（`T1'`）→
        字面比较 `edge["from"] == task_id` 永不命中。这里先用 `task_for` 规范化
        源任务，再按「task_id 相同」或「alias 一跳解析到同一 subgoal」匹配边，
        并把后继也 `CANCELLED`（否则 CANCELLED 不再算前驱满足 → 后继永久阻塞）。
        """
        for plan in self.state.plans.values():
            task = self.deps.planner.task_for(plan, task_id)
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
                    and self.deps.planner.resolve_alias(src, plan) == src_subgoal)
                if not same:
                    continue
                succ = self.deps.planner.task_for(plan, dst)
                if isinstance(succ, dict) and succ.get("state") not in TASK_TERMINAL:
                    succ["state"] = TaskState.CANCELLED

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
        cands = [a for a in self.state.acbs.values()
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
            self.deps.safe_save(loser)
        self.state.drain_skipped.append(
            (f"{run_id}:{task_id}", "ambiguous_pick_best"))
        return best

    def _admit_ready_successor(self, run_id: str, task_id: str,
                               plan: dict | None, src: ACB | None = None) -> ACB | None:
        """R5：把 ready 集中的 PLANNED 任务就地准入（无候选时的唯一出路）。"""
        if plan is None:
            return None
        task = self.deps.planner.task_for(plan, task_id)
        if not isinstance(task, dict) or task.get("state") is not TaskState.PLANNED:
            return None
        task_id = task.get("task_id", task_id)
        try:
            fresh = self.deps.dispatcher.try_admit(task, run_id, plan=plan)
        except Exception:  # noqa: BLE001  # 准入异常不得打断父任务的结算
            self.state.drain_skipped.append((f"{run_id}:{task_id}", "admit_raised"))
            return None
        if fresh.state is TaskState.ADMITTED:
            self.state.acbs[fresh.id] = fresh
            self.deps.run_coordinator.register(fresh)
            self._adopt_capability(src, fresh)
            if getattr(fresh, "capability", None) is not None:
                self.deps.safe_save(fresh)      # R1：新身份 token 随快照落盘
            return fresh
        reason = fresh.failure or fresh.state.value
        self.state.drain_skipped.append((f"{run_id}:{task_id}", f"admit_{reason}"))
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
        if self.deps.policy.token(fresh.id) is not None:
            return
        token = self.deps.effective_token(src)
        if token is None:
            return
        resolved = self.deps.policy.resolve_token(token)
        if resolved is None:
            return
        fresh.capability = self.deps.policy.issue(
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
            self.deps.ensure_policy_token(resolved.parent_id)
            if not fresh.parent_id:
                fresh.parent_id = resolved.parent_id

    def kill_run(self, run_id: str) -> int:
        """**立即**终止该 run 的全部未终态任务（不可恢复；资源全部结算）。

        与 `pause_run` 的区别：pause 保留资源可恢复；kill 释放一切且不可恢复。
        返回被终止的 ACB 数。
        """
        killed = 0
        for acb in list(self.state.acbs.values()):
            if acb.run_id != run_id or acb.state in TASK_TERMINAL:
                continue
            with self._transition_lock(acb):
                if acb.state in TASK_TERMINAL:
                    continue
                acb.state = TaskState.CANCELLED
                acb.failure = "KILLED_BY_ADMIN"
                self.deps.intents.revoke_all(acb.id)
                self._finalize(acb)
                killed += 1
        self.state.paused_runs.discard(run_id)
        return killed

    def _reap_expired_tokens(self, now_ms: int) -> int:
        """S22：回收**过期的门面 token**（宿主崩溃/忘记 finish 的兜底）。

        只作用于 `spec["rsc_token"]` 的 ACB（普通 ACB 一律跳过 → 不影响既有
        aging 语义）。动作 = `cancel` → `dispatcher.abort`（退款/释放槽位）
        → 清理钩子（销毁 Z3）→ `purge_run`（回收内存）。返回回收数。
        """
        now = int(now_ms)
        reaped = 0
        for acb in list(self.state.acbs.values()):
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
            if self.deps.scheduler.claim_of(acb.id) is not None:
                continue
            deadline = spec.get("token_deadline_ms")
            if deadline is None or now < int(deadline):
                continue
            if advance(acb, "cancel") != Status.OK:
                continue
            self.deps.dispatcher.abort(acb)
            cleanup = self.deps.token_cleanup()
            if callable(cleanup):
                try:
                    cleanup(acb)
                except Exception:  # noqa: BLE001  # 清理失败不得阻断回收
                    pass
            self.state.drain_skipped.append((acb.id, "token_expired"))
            self.deps.purge_run(acb.run_id)
            reaped += 1
        return reaped
