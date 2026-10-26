"""Harness —— 组装所有组件的门面（对应文档 §20.2 端到端数据流）。

用法见 `examples/` 与 `tests/`。
"""

from __future__ import annotations

import hashlib
import threading

from .budget import BudgetManager
from .checkpoint import CheckpointStore, EffectJournal
from .context import ContextAssembler
from .event_log import EventLog
from .intent_registry import IntentRegistry
from .orchestration import (Dispatcher, HITL, Planner, ReconcileLoop,
                            RunCoordinator, Scheduler)
from .policy import PolicyEngine
from .runtime import AgentRuntime
from .subscription import SubscriptionRegistry
from .types import ACB, EventKind, Mode, Status, TaskState, Write
from .workspace import WorkspaceManager, WorkspaceVersionStore


class Harness:
    """组装所有组件的门面（对应文档 §20.2 端到端数据流）。

    所有可调常量均可注入（避免硬编码，见 milestones.md §5）。
    """

    def __init__(self, run_limits: int = 1000, retention: int = 10000,
                 lease_ms: int = 60_000, guard_every: int = 1,
                 aging_max: int = 5, max_steps: int = 10_000,
                 max_concurrent: int = 100,
                 policy_default_allow: bool = True) -> None:
        self.lease_ms = lease_ms
        self.guard_every = guard_every
        self.aging_max = aging_max
        self.max_steps = max_steps
        self.max_concurrent = max_concurrent

        self.event_log = EventLog()
        self.intents = IntentRegistry()
        self.versions = WorkspaceVersionStore()
        self.workspace = WorkspaceManager(self.event_log, self.intents, self.versions)
        self.budget = BudgetManager(run_limits)
        self.checkpoints = CheckpointStore()
        self.effects = EffectJournal(self.event_log)
        self.subscriptions = SubscriptionRegistry(self.event_log)
        self.policy = PolicyEngine(default_allow=policy_default_allow)
        self.context = ContextAssembler(self.event_log, self.workspace, retention)
        self.compensation_journal: list[dict] = []      # 文档 §5.4

        self.planner = Planner(self)
        self.dispatcher = Dispatcher(self)
        self.scheduler = Scheduler(self)
        self.run_coordinator = RunCoordinator(self)
        self.reconcile = ReconcileLoop(self)
        self.hitl = HITL()

        self.runtime = AgentRuntime(self)
        self.plans: dict[str, dict] = {}
        self.acbs: dict[str, ACB] = {}
        self._lock = threading.RLock()

        self.event_log.subscribe(self._on_event)

    # ---- 事件 → 订阅失效（文档 §5.6） ----------------------------
    def _on_event(self, ev) -> None:
        if ev.kind == EventKind.RESOURCE_COMMITTED:
            for agent_id in self.subscriptions.subscribers(ev.subject):
                self.subscriptions.notify(ev.subject, ev)

    # ---- 幂等键（文档 §5.8 / I7） --------------------------------
    def idem_key(self, acb: ACB, action, action_index: int) -> str:
        """`hash(run_id, logical_subgoal_id, hash(tool, hash(args)))` —— 不含 task_id。"""
        payload = f"{acb.run_id}|{acb.logical_subgoal_id}|{action.tool}|{action.args}"
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]

    # ---- RSC 提交（文档 §5.4） ------------------------------------
    def commit_batch(self, acb: ACB, writes: list[Write],
                     mode: Mode = Mode.COMMIT) -> Status:
        leases: list = []
        applied: list[Write] = []
        try:
            for w in writes:
                base = acb.read_set.get(w.resource, self.workspace.version(w.resource))
                leases.append(self.intents.reserve(w.resource, acb.id, base, self.lease_ms))

            for w, lease in zip(writes, leases):
                st = self.workspace.prepare(acb, w, lease, mode)
                if st != Status.VALID:
                    return st

            for w, lease in zip(writes, leases):
                st = self.workspace.apply(acb, w, lease, mode)
                if st != Status.OK:
                    if mode == Mode.COMMIT:
                        self.compensate(acb, applied)
                    return st
                applied.append(w)
            return Status.OK
        finally:
            for lease in leases:
                self.intents.revoke(lease)

    def compensate(self, acb: ACB, applied: list[Write]) -> None:
        """Saga 补偿：逆序回滚；`REJECT_COMPENSATE` → PIVOT；带 compensation_journal。"""
        self.intents.revoke_all(acb.id)
        for w in reversed(applied):
            if w.compensated:
                self.compensation_journal.append({"resource": w.resource, "state": "SKIPPED"})
                continue
            reverse = self.workspace.compute_reverse_patch(w)
            if reverse is None:
                w.pivot = True
                self.compensation_journal.append({"resource": w.resource, "state": "PIVOT"})
                self.event_log.append(EventKind.PIVOT, w.resource, {}, producer=acb.id)
                continue
            acb.staged[w.resource] = reverse
            st = self._compensate_one(acb, w)
            if st == Status.REJECT_COMPENSATE:
                w.pivot = True
                self.compensation_journal.append({"resource": w.resource, "state": "PIVOT"})
                self.event_log.append(EventKind.PIVOT, w.resource, {}, producer=acb.id)
            elif st == Status.OK:
                w.compensated = True
                self.compensation_journal.append({"resource": w.resource, "state": "COMPENSATED"})
                self.event_log.append(EventKind.COMPENSATION_DONE, w.resource, {},
                                      producer=acb.id)
            else:
                self.compensation_journal.append({"resource": w.resource, "state": "FAILED"})
                self.event_log.append(EventKind.COMPENSATION_FAILED, w.resource, {},
                                      producer=acb.id)

    def _compensate_one(self, acb: ACB, w: Write) -> Status:
        """单写补偿，带**真正的**有界重试（v1.11 修复）。"""
        for _ in range(acb.retry_budget + 1):
            st = self.commit_batch(acb, [w], Mode.COMPENSATE)
            if st == Status.OK:
                return Status.OK
            if st == Status.REJECT_COMPENSATE:
                return st
        return Status.COMMIT_UNRECOVERABLE

    # ---- 重规划（文档 §5.2④ / §8.1） ------------------------------
    def propagate_failure(self, task_id: str, plan_version: int) -> None:
        """只传播到同一 Plan 版本的后继（v1.10 修复 P0-3）。"""
        for run_id, plan in self.plans.items():
            task = plan["tasks"].get(task_id)
            if not task or task.get("plan_version") != plan_version:
                continue
            for edge in plan["edges"]:
                if edge["from"] == task_id:
                    succ = plan["tasks"].get(edge["to"])
                    if succ and succ.get("plan_version") == plan_version:
                        succ["state"] = TaskState.CANCELLED

    # ---- Supervisor 辅助 ------------------------------------------
    def supervisor_note_crash(self, acb: ACB) -> None:
        """Supervisor 心跳超时回收（文档 §5.8③）。"""
        self.intents.revoke_all(acb.id)
        self.bump_epoch()

    def bump_epoch(self) -> None:
        self.intents.bump_epoch()

    # ---- 端到端运行 -----------------------------------------------
    def run_task(self, acb: ACB, decisions, crash_at=None) -> Status:
        self.acbs[acb.id] = acb
        self.run_coordinator.register(acb)
        self.scheduler.push(acb.task_id)
        try:
            return self.runtime.run(acb, decisions, crash_at=crash_at)
        finally:
            self._finalize(acb)
            self._drain_scheduler()

    def _drain_scheduler(self) -> None:
        """消费 ready_queue（避免只写不读的死代码）。"""
        while not self.scheduler.is_empty():
            self.scheduler.pop()

    def _finalize(self, acb: ACB) -> None:
        plan = self.plans.get(acb.run_id)
        if plan and acb.task_id in plan["tasks"]:
            plan["tasks"][acb.task_id]["state"] = acb.state
        if acb.state in (TaskState.FAILED, TaskState.CANCELLED, TaskState.DEAD_LETTER,
                         TaskState.DONE, TaskState.DONE_PARTIAL):
            # 终态释放预算（文档 §9.1：结束必 release）
            if acb.budget_node is not None:
                self.budget.release(acb.budget_node.node_id)
        if acb.state in (TaskState.FAILED, TaskState.CANCELLED, TaskState.DEAD_LETTER):
            self.propagate_failure(acb.task_id,
                                   plan["tasks"][acb.task_id].get("plan_version", 1)
                                   if plan else 1)
        # level-triggered 对账（文档 §5.2.1）：事件即真相
        if plan:
            self.reconcile.reconcile_events(acb.run_id)
            self.reconcile.rebuild_ready_set(acb.run_id)

    # ---- 恢复（文档 §5.8） ----------------------------------------
    def recover(self, ref: str) -> ACB:
        data = self.checkpoints.load(ref)
        acb = ACB(id=data["acb_id"], run_id=data["run_id"], task_id=data["task_id"],
                  logical_subgoal_id=data.get("logical_subgoal_id", ""))
        acb.generation = data["generation"] + 1
        acb.state = TaskState(data["state"])
        acb.turn_seq = data["turn_seq"]
        acb.staged = dict(data["staged"])
        acb.read_set = dict(data["read_set"])
        acb.base_version = dict(data["base_version"])
        acb.cursor = data["cursor"]
        acb.aging_deadline = data.get("aging_deadline")
        acb.step = data["effect_journal_offset"]
        acb.hitl_result = data.get("hitl_result")
        if acb.budget_node is None:
            node_id = f"B-{acb.task_id}"
            acb.budget_node = self.budget._nodes.get(node_id) or self.budget.create_node(
                node_id, 10)
        self.bump_epoch()
        self.acbs[acb.id] = acb
        self.run_coordinator.register(acb)
        return acb
