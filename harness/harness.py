"""Harness —— 组装所有组件的门面（对应文档 §20.2 端到端数据流）。

用法见 `examples/` 与 `tests/`。
"""

from __future__ import annotations

import hashlib
import threading

from .budget import BudgetManager
from .checkpoint import CheckpointStore, EffectJournal
from .clock import SystemClock
from .context import ContextAssembler
from .event_log import EventLog
from .intent_registry import IntentRegistry
from .orchestration import (Dispatcher, HITL, Planner, ReconcileLoop,
                            RunCoordinator, Scheduler)
from .policy import PolicyEngine
from .runtime import AgentRuntime
from .subscription import SubscriptionRegistry
from .types import ACB, EventKind, Mode, Result, Status, TaskState, Write
from .storage.blob import InMemoryBlobStore, canonical_bytes
from .workspace import WorkspaceManager, WorkspaceVersionStore


class Harness:
    """组装所有组件的门面（对应文档 §20.2 端到端数据流）。

    所有可调常量均可注入（避免硬编码，见 milestones.md §5）。
    """

    def __init__(self, run_limits: int = 1000, retention: int = 10000,
                 lease_ms: int = 60_000, guard_every: int = 1,
                 aging_max: int = 5, max_steps: int = 10_000,
                 max_concurrent: int = 100,
                 policy_default_allow: bool = False, *,
                 clock=None, event_log=None, intents=None, versions=None,
                 workspace=None, budget=None, checkpoints=None, effects=None,
                 policy=None, context=None, subscriptions=None,
                 decide=None, default_token=None,
                 blobs=None, path_of=None, lock_timeout_s: float = 5.0) -> None:
        self.lease_ms = lease_ms
        self.guard_every = guard_every
        self.aging_max = aging_max
        self.max_steps = max_steps
        self.max_concurrent = max_concurrent
        self.retention = retention

        self.clock = clock or SystemClock()
        self.event_log = event_log if event_log is not None else EventLog(clock=self.clock)
        self.blobs = blobs if blobs is not None else InMemoryBlobStore()
        self.intents = intents if intents is not None else IntentRegistry(self.clock)
        self.versions = versions if versions is not None else WorkspaceVersionStore()
        self.lock_timeout_s = lock_timeout_s
        self.workspace = (workspace if workspace is not None
                          else WorkspaceManager(self.event_log, self.intents, self.versions,
                                                blobs=self.blobs, path_of=path_of,
                                                lock_timeout_s=lock_timeout_s))
        self.budget = budget if budget is not None else BudgetManager(run_limits)
        self.checkpoints = checkpoints if checkpoints is not None else CheckpointStore()
        self.effects = (effects if effects is not None
                        else EffectJournal(self.event_log))
        self.subscriptions = (subscriptions if subscriptions is not None
                              else SubscriptionRegistry(self.event_log))
        self.policy = (policy if policy is not None
                       else PolicyEngine(default_allow=policy_default_allow,
                                         default_token=default_token))
        self.context = (context if context is not None
                        else ContextAssembler(self.event_log, self.workspace, retention))
        self.compensation_journal: list[dict] = []      # 文档 §5.4
        self.decide = decide

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

    # ---- RSC 提交（文档 §5.5） ------------------------------------
    def commit_batch(self, acb: ACB, writes: list[Write],
                     mode: Mode = Mode.COMMIT) -> Result:
        """公共写入口。提交点顺序不可调换：

        ① 输入校验 → ② 预留租约(L0) → ③ 全序一次性持锁(L3，锁集合含读集合)
        → ④ 校验 → ④′ 内容先落盘 → ⑤ 提交点前复验租约
        → ⑥ 提交点（一次 fdatasync）→ ⑦ 投影 → ⑧ 锁外通知（不掩盖返回值）。
        """
        if not writes:
            return Result(Status.OK, {"noop": True, "seq": None})
        resources = {w.resource for w in writes}
        if len(resources) != len(writes):                          # ① 同批重复 resource
            return Result(Status.INVALID_INPUT,
                          {"reason": "duplicate_resource", "resources": sorted(resources)})
        contents = {w.resource: acb.staged.get(w.resource, w.content) for w in writes}
        for w in writes:                                           # ① None 内容
            if contents[w.resource] is None:
                return Result(Status.INVALID_INPUT,
                              {"reason": "none_content", "resource": w.resource})

        resources |= set(acb.read_set)                             # ③ 读集合一并持锁 → 原子复验
        leases: dict = {}
        events = None
        held: list[str] = []
        try:
            for w in writes:                                       # ② 预留租约（L0）
                base = self.workspace._base_of(acb, w.resource)
                lease = self.intents.reserve(w.resource, acb.id, base, self.lease_ms)
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

            blob_hashes = {w.resource: self.blobs.put(canonical_bytes(contents[w.resource]))
                           for w in writes}                        # ④′ 内容先落盘

            for w in writes:                                       # ⑤ 提交点前复验租约
                if not self.intents.is_valid(leases[w.resource]):
                    return Result(Status.REJECT_STALE,
                                  {"resource": w.resource,
                                   "reason": "lease_expired_before_commit"})

            drafts = [self._commit_record(acb, w, blob_hashes[w.resource]) for w in writes]
            events = self.event_log.append_batch(drafts, notify=False)   # ⑥ 提交点

            for w in writes:                                       # ⑦ 投影（可重放）
                prev = (self.workspace.manifest[w.resource].blob_hash
                        if w.resource in self.workspace.manifest else "")
                self.workspace._apply_locked(acb, w, blob_hashes[w.resource], prev)
            for w in writes:                                       # ⑦′ 自写后重读：刷新 ACB 读版本
                new_version = self.workspace.version(w.resource)
                acb.read_set[w.resource] = new_version
                acb.base_version[w.resource] = new_version
            return Result(Status.OK, {"resources": sorted(blob_hashes), "seq": events[-1].seq})
        finally:
            self.workspace._release_all(held)                      # 先释放资源锁
            for lease in leases.values():
                self.intents.revoke(lease)                         # 再 revoke 租约
            if events is not None:                                 # ⑧ 锁外通知（不掩盖返回值）
                try:
                    self.event_log.notify_events(events)
                except Exception:
                    pass

    def _commit_record(self, acb: ACB, w: Write, blob_hash: str) -> dict:
        cur = self.workspace.manifest.get(w.resource)
        return {
            "kind": EventKind.RESOURCE_COMMITTED,          # ← 必须与重建过滤一致
            "subject": w.resource,
            "payload": {
                "resource": w.resource,
                "version": (cur.version if cur else 0) + 1,
                "blob_hash": blob_hash,
                "prev_hash": cur.blob_hash if cur else "",
                "diff": {"prev_hash": cur.blob_hash if cur else "", "current_hash": blob_hash},
            },
            "producer": acb.id,
            "run_id": acb.run_id,
            "idempotency_key": f"{acb.run_id}:{acb.id}:{w.resource}",
        }

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
