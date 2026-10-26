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


class Dispatcher:
    def __init__(self, harness) -> None:
        self.h = harness

    def try_admit(self, task: dict, run_id: str,
                  required_tools: set[str] | None = None) -> ACB:
        """准入谓词（文档 §5.2⑤）：预算 + 能力 + intents.compatible + slots。

        - 预算：`reserve(min_budget)`
        - 能力：`required_tools` 必须是已签发 token 的子集
        - 契约/读-写冲突：`intents.compatible`（**不拦同资源写-写**）
        - slots：`max_concurrent` 限制
        """
        acb = ACB(id=f"A-{task['task_id']}", run_id=run_id, task_id=task["task_id"],
                  logical_subgoal_id=task["logical_subgoal_id"])

        # ① slots
        if len(self.h.acbs) >= self.h.max_concurrent:
            acb.state = TaskState.INADMISSIBLE
            acb.failure = "NO_SLOT"
            return acb

        # ② 能力
        if required_tools:
            token = self.h.policy.token(acb.id)
            if token is None or not required_tools.issubset(token.tools):
                acb.state = TaskState.INADMISSIBLE
                acb.failure = "DENIED_CAPABILITY"
                return acb

        # ③ 契约/读-写冲突（文档 §7.3：先声明，再判定他人是否已声明同一依赖）
        declared: list[str] = []
        try:
            for dep in task["footprint"].get("logical_deps", []):
                self.h.intents.declare_logical(dep["id"], acb.id, dep["kind"])
                declared.append(dep["id"])
                if not self.h.intents.compatible(dep["id"], acb.id, dep["kind"]):
                    acb.state = TaskState.INADMISSIBLE
                    acb.failure = "CONTRACT_CONFLICT"
                    return acb

            # ④ 预算
            node_id = f"B-{task['task_id']}"
            if not self.h.budget.create_node(node_id, task["footprint"]["min_budget"]):
                acb.state = TaskState.INADMISSIBLE
                return acb
            if not self.h.budget.reserve(node_id, task["footprint"]["min_budget"]):
                acb.state = TaskState.INADMISSIBLE
                acb.failure = "BUDGET_EXHAUSTED"
                return acb
            acb.budget_node = self.h.budget._nodes[node_id]
            acb.state = TaskState.ADMITTED
        finally:
            if acb.state != TaskState.ADMITTED:        # 失败回滚已声明的逻辑依赖
                for dep_id in declared:
                    self.h.intents.undeclare_logical(dep_id, acb.id)
        self.h.event_log.append(EventKind.BUDGET_RESERVED, node_id,
                                {"amount": task["footprint"]["min_budget"]})
        return acb


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
