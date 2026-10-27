"""L4 编排（参考实现，可替换）—— 文档 §5.2。

- Planner：产出 Plan + footprint（committed/contingent/logical_deps/logical_subgoal_id）
- Dispatcher：try_admit（预算预留 + 能力 + intents.compatible）
- Scheduler：ready_queue + 升级路径（aging → INADMISSIBLE）
- RunCoordinator：唯一 Run 状态写入者（B1），取消广播
- ReconcileLoop：level-triggered 对账（文档 §5.2.1）
- HITL：审批处理器
"""

from __future__ import annotations

import copy
import itertools
import threading

from .types import ACB, EventKind, Mode, Status, TaskState, Write


def _normalize_required(t) -> dict:
    """入口规范化（§5.5）：set → `{tool: ""}`；dict → 原样（`{tool: path}`）。"""
    if t is None:
        return {}
    if isinstance(t, (set, frozenset, list, tuple)):
        return {x: "" for x in t}
    if isinstance(t, dict):
        return dict(t)
    return {}


class Planner:
    def __init__(self, harness) -> None:
        self.h = harness
        self._counter = itertools.count(1)

    def plan(self, run_id: str, tasks: list[dict]) -> dict:
        plan = {"run_id": run_id, "version": 1, "tasks": {}, "edges": []}
        for spec in tasks:
            task_id = spec.get("task_id", f"T{next(self._counter)}")
            subgoal = spec.get("logical_subgoal_id") or f"S-{task_id}"
            plan["tasks"][task_id] = {
                "task_id": task_id,
                "logical_subgoal_id": subgoal,
                "footprint": {
                    "committed": set(spec.get("committed", [])),
                    "contingent": set(spec.get("contingent", [])),
                    "min_budget": spec.get("min_budget", 10),
                    "logical_deps": spec.get("logical_deps", []),
                },
                "merge_policy": spec.get("merge_policy") or {},
                "state": TaskState.PLANNED,
                "plan_version": 1,
            }
        self.h.event_log.append(EventKind.PLAN_CREATED, run_id, {"tasks": list(plan["tasks"])})
        return plan

    def add_logical_edge(self, plan: dict, from_id: str, to_id: str) -> None:
        plan["edges"].append({"from": from_id, "to": to_id, "kind": "LOGICAL"})

    def merge_policy_of(self, resource: str) -> dict:
        """§3.2：从 task spec 的 `merge_policy` 读 per-key 策略（缺省空 dict）。

        按 resource 归属（committed/contingent）定位任务；未命中 → `{}`。
        """
        for plan in self.h.plans.values():
            for task in plan.get("tasks", {}).values():
                fp = task.get("footprint", {})
                if (resource in fp.get("committed", ())
                        or resource in fp.get("contingent", ())):
                    mp = task.get("merge_policy")
                    return dict(mp) if isinstance(mp, dict) else {}
        return {}

    def patch(self, plan: dict, diagnosis: dict) -> dict:
        """文档 §8.1：诊断 → PlanPatch（复用 logical_subgoal_id）。"""
        new_plan = copy.deepcopy(plan)
        new_plan["version"] = plan["version"] + 1
        remade: dict[str, dict] = {}
        for tid, task in new_plan["tasks"].items():
            if task["logical_subgoal_id"] in diagnosis.get("failing_subgoals", []):
                new_tid = f"{tid}'"
                task["task_id"] = new_tid
                task["state"] = TaskState.PLANNED
                task["plan_version"] = new_plan["version"]
                remade[new_tid] = task
            else:
                task["plan_version"] = new_plan["version"]
                remade[tid] = task
        new_plan["tasks"] = remade
        for dep in diagnosis.get("missing_deps", []):
            new_plan["edges"].append({"from": dep["from"], "to": dep["to"], "kind": "LOGICAL"})
        self.h.event_log.append(EventKind.PLAN_PATCHED, plan["run_id"],
                                {"version": new_plan["version"]})
        return new_plan


class SlotTracker:
    """活跃槽位计数（可回滚的资源，文档 §5.2）。"""

    def __init__(self, limit: int) -> None:
        self._limit = limit
        self._active: set[str] = set()
        self._lock = threading.Lock()

    def try_acquire(self, key: str) -> bool:
        with self._lock:
            if key in self._active:
                return True                     # 幂等
            if len(self._active) >= self._limit:
                return False
            self._active.add(key)
            return True

    def release(self, key: str) -> None:
        with self._lock:
            self._active.discard(key)

    @property
    def active(self) -> int:
        with self._lock:
            return len(self._active)

    def active_keys(self) -> list[str]:
        with self._lock:
            return list(self._active)


class Dispatcher:
    def __init__(self, harness) -> None:
        self.h = harness
        self._attempt_counter = itertools.count(1)

    def _next_attempt(self) -> int:
        return next(self._attempt_counter)

    def _deny(self, acb: ACB, failure: str) -> ACB:
        acb.state = TaskState.INADMISSIBLE
        acb.failure = failure
        return acb

    def _capability_ok(self, acb: ACB, required_tools,
                       *, network=None, data_class=None) -> bool:
        """与运行时同一判定入口（§5.9）：token 显式传入 + 四维。"""
        token = self.h._effective_token(acb)
        required = _normalize_required(required_tools)
        return all(self.h.policy.check_capability(
            acb.id, token, tool, path, network=network, data_class=data_class)
            for tool, path in required.items())

    def try_admit(self, task: dict, run_id: str,
                  required_tools: set[str] | None = None,
                  token=None) -> ACB:
        """准入谓词（文档 §5）：slots → 能力 → 预算 → 逻辑，失败零残留。"""
        acb = ACB(id=f"A-{run_id}-{task['task_id']}-{self._next_attempt()}",
                  run_id=run_id, task_id=task["task_id"],
                  logical_subgoal_id=task["logical_subgoal_id"])
        if token is not None:                 # 显式 token 绑定到 ACB（§5.9）
            acb.capability = token

        node = None
        resv = None
        declared: list[str] = []
        committed = False                           # 哨兵：副作用全部完成才置 True
        try:
            # ① slots
            if not self.h.slots.try_acquire(acb.id):
                return self._deny(acb, "NO_SLOT")

            # ② 能力
            if required_tools and not self._capability_ok(acb, required_tools):
                return self._deny(acb, "DENIED_CAPABILITY")

            # ③ 预算：limits 留出重试余量，但只预留 min_budget
            min_budget = task["footprint"]["min_budget"]
            limits = min_budget + acb.retry_budget + 1
            node = self.h.budget.create_node(
                f"B-{acb.id}", limits,
                parent_id=task["footprint"].get("parent_budget", "run"))
            if node is None:
                return self._deny(acb, "BUDGET_UNAVAILABLE")
            node.owner = acb.id
            resv = self.h.budget.reserve(node.node_id, min_budget)
            if resv is None:
                return self._deny(acb, "BUDGET_EXHAUSTED")
            acb.reservation_ids = [resv.reservation_id]
            acb.budget_node = node

            # ④ 逻辑声明（文档 §7.3：先声明，再判定他人是否已声明同一依赖）
            for dep in task["footprint"].get("logical_deps", []):
                self.h.intents.declare_logical(dep["id"], acb.id, dep["kind"])
                declared.append(dep["id"])
                if not self.h.intents.compatible(dep["id"], acb.id, dep["kind"]):
                    return self._deny(acb, "CONTRACT_CONFLICT")

            acb.state = TaskState.ADMITTED
            self.h.event_log.append(EventKind.BUDGET_RESERVED, node.node_id,
                                    {"amount": resv.amount})
            committed = True
            return acb
        finally:
            if not committed:
                self._rollback(acb, node, resv, declared)

    def _rollback(self, acb: ACB, node, resv, declared: list[str]) -> None:
        """逆序撤销（逻辑 → 预留 → 节点 → 槽位）；不吞原异常。"""
        for dep_id in declared:
            try:
                self.h.intents.undeclare_logical(dep_id, acb.id)
            except Exception:
                pass
        if resv is not None:
            try:
                self.h.budget.release(resv.reservation_id)
                # 准入回滚的预留从未消费：清除 tombstone，保证 T9 快照零残留
                self.h.budget._reservations.pop(resv.reservation_id, None)
                if node is not None:
                    node.reservations.pop(resv.reservation_id, None)
            except Exception:
                pass
        if node is not None:
            try:
                self.h.budget.remove_node(node.node_id, owner=acb.id)
            except Exception:
                pass
        acb.reservation_ids = []
        acb.budget_node = None
        self.h.slots.release(acb.id)


class Scheduler:
    def __init__(self, harness) -> None:
        self.h = harness
        self._ready: list[str] = []
        self._lock = threading.RLock()

    def push(self, task_id: str) -> None:
        with self._lock:
            if task_id not in self._ready:
                self._ready.append(task_id)

    def pop(self) -> str | None:
        with self._lock:
            return self._ready.pop(0) if self._ready else None

    def is_empty(self) -> bool:
        return not self._ready


class RunCoordinator:
    """B1：唯一 Run 状态写入者；取消直接广播（不依赖父子链转发）。"""

    def __init__(self, harness) -> None:
        self.h = harness
        self._cancelled: set[str] = set()
        self._active: dict[str, str] = {}
        self._lock = threading.RLock()

    def register(self, acb: ACB) -> None:
        with self._lock:
            self._active[acb.id] = acb.run_id

    def is_cancelled(self, acb: ACB) -> bool:
        with self._lock:
            return acb.run_id in self._cancelled

    def cancel_run(self, run_id: str) -> None:
        """取消广播给所有后代（文档 §5.2⑦）。"""
        with self._lock:
            self._cancelled.add(run_id)
            for acb_id, rid in list(self._active.items()):
                if rid == run_id:
                    self.h.intents.revoke_all(acb_id)
                    self.h.bump_epoch()

    def submit_command(self, cmd: dict) -> Status:
        """命令接口（B1）：Dispatcher/Scheduler/Supervisor 通过它请求状态变更。"""
        return Status.OK


class ReconcileLoop:
    """level-triggered 对账（文档 §5.2.1）。"""

    def __init__(self, harness) -> None:
        self.h = harness

    def rebuild_ready_set(self, run_id: str) -> list[str]:
        """从 PlanStore 重建 ready 集（不依赖内存）。"""
        plan = self.h.plans.get(run_id)
        if not plan:
            return []
        done = {tid for tid, t in plan["tasks"].items() if t["state"] == TaskState.DONE}
        ready = []
        for tid, task in plan["tasks"].items():
            deps = [e["from"] for e in plan["edges"] if e["to"] == tid]
            if all(d in done for d in deps) and task["state"] in (TaskState.PLANNED, TaskState.ADMITTED):
                ready.append(tid)
        return ready

    def reconcile_events(self, run_id: str) -> int:
        """事件即真相：EventLog 有 TASK_COMPLETED 则强制置 DONE。"""
        plan = self.h.plans.get(run_id)
        if not plan:
            return 0
        fixed = 0
        for tid, task in plan["tasks"].items():
            if self.h.event_log.has(tid, EventKind.TASK_COMPLETED) and task["state"] != TaskState.DONE:
                task["state"] = TaskState.DONE
                fixed += 1
        return fixed


class HITL:
    def __init__(self) -> None:
        self._pending: dict[str, object] = {}

    def resolve(self, acb: ACB):
        """返回审批结果；None 表示超时。测试中可注入。"""
        return self._pending.get(acb.task_id, {"approved": True})

    def set_result(self, task_id: str, result) -> None:
        self._pending[task_id] = result

    def clear(self, task_id: str) -> None:
        self._pending.pop(task_id, None)
