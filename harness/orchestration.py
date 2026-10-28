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

from .types import ACB, EventKind, Status, TaskState
from .storage.blob import canonical_bytes


def _normalize_required(t) -> dict:
    """入口规范化（§5.5）：set → `{tool: ""}`；dict → 原样（`{tool: path}`）。"""
    if t is None:
        return {}
    if isinstance(t, (set, frozenset, list, tuple)):
        return {x: "" for x in t}
    if isinstance(t, dict):
        return dict(t)
    return {}


def _edge_endpoints(edge) -> tuple:
    """统一取边的 `(src, dst)`：兼容 dict（`from`/`to`）与二元组两种表示。"""
    if isinstance(edge, dict):
        return edge.get("from"), edge.get("to")
    return edge[0], edge[1]


# S13b §2.3：前驱闸门与 ready 重建必须共用**同一套终态定义**（此前一处只认
# DONE、另一处认 DONE+CANCELLED → alias 修复后两边判定分裂）。
PREDECESSOR_SATISFIED = frozenset({TaskState.DONE, TaskState.CANCELLED})


def _has_cycle(edges) -> bool:
    """DFS 三色环检测（§2.5）：有向图存在后向边 → `True`。"""
    adj: dict = {}
    for e in edges or []:
        src, dst = _edge_endpoints(e)
        if src is None or dst is None:
            continue
        adj.setdefault(src, []).append(dst)
    color: dict = {}                       # 0=未访 1=在栈 2=已完成

    def visit(n) -> bool:
        color[n] = 1
        for m in adj.get(n, []):
            c = color.get(m, 0)
            if c == 1:
                return True
            if c == 0 and visit(m):
                return True
        color[n] = 2
        return False

    return any(color.get(n, 0) == 0 and visit(n) for n in list(adj))


def _admission_input_error(task) -> str | None:
    """准入输入 fail-fast 校验（P1-5）：返回 `None` 表示通过，否则为原因。

    用户内容（`merge_policy` / `logical_deps`）必须在**准入边界**通过严格 codec
    校验；否则非法内容会一路带到 commit（UPSTREAM_FAILED）与 `_finalize` 落盘
    （codec 再抛 → 穿透 run → 槽位/预算结算被跳过）。
    """
    if not isinstance(task, dict):
        return "task_not_dict"
    footprint = task.get("footprint")
    if not isinstance(footprint, dict):
        return "footprint_not_dict"
    for key in ("task_id", "logical_subgoal_id"):
        if not isinstance(task.get(key), str):
            return f"bad_{key}"
    min_budget = footprint.get("min_budget")
    if isinstance(min_budget, bool) or not isinstance(min_budget, int) or min_budget < 0:
        return "bad_min_budget"
    try:
        canonical_bytes(task.get("merge_policy") or {})
        deps = footprint.get("logical_deps") or []
        if not isinstance(deps, list):
            return "bad_logical_deps"
        canonical_bytes(deps)
    except (TypeError, ValueError):
        return "bad_task_content"
    return None


class Planner:
    def __init__(self, harness) -> None:
        self.h = harness
        self._counter = itertools.count(1)
        # 供 `resolve_alias(old_id)`（无 plan 参数调用）兜底；有 plan 时以
        # plan["_id_aliases"] 为准，避免跨 run 串味。
        self._id_aliases: dict[str, str] = {}

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

    def resolve_alias(self, old_id: str, plan: dict | None = None) -> str:
        """`old_id` → 稳定 `logical_subgoal_id`（**一跳解析**，不做链式递归）。

        值**永远**是 `logical_subgoal_id`（不是下一个 task id）：这样
        `missing_deps` 直接复用 `add_logical_edge`（含环检测）也不会因 id 链
        造出环（S13b K-65）。有 `plan` 时读该 plan 的 `_id_aliases`，否则退回
        Planner 级累积表（兼容单参数调用）。
        """
        aliases = None
        if plan is not None:
            aliases = plan.get("_id_aliases")
        if not isinstance(aliases, dict):
            aliases = self._id_aliases
        return aliases.get(old_id, old_id)

    def task_for(self, plan: dict | None, task_id: str) -> dict | None:
        """旧 id / 稳定 subgoal id → **当前 plan 版本**下的唯一 task。

        S13b §2.3：同一 `logical_subgoal_id` 在多次 patch 后可能对应多个
        历史 task（T1 / T1' / T1''），`setdefault` 只保留首个会解析到陈旧分支。
        这里按「当前版本优先、否则取 plan_version 最大者」挑选。
        """
        tasks = (plan or {}).get("tasks", {}) or {}
        stable = self.resolve_alias(task_id, plan)
        direct = tasks.get(task_id)
        candidates: list[dict] = []
        if isinstance(direct, dict):
            candidates.append(direct)
        if stable:
            for task in tasks.values():
                if (isinstance(task, dict) and task is not direct
                        and task.get("logical_subgoal_id") == stable):
                    candidates.append(task)
        if not candidates:
            return None
        current = (plan or {}).get("version")
        current_c = [t for t in candidates
                     if t.get("plan_version") == current]
        pool = current_c or candidates
        return max(pool, key=lambda t: int(t.get("plan_version", 0) or 0))

    def add_logical_edge(self, plan: dict, from_id: str, to_id: str) -> None:
        """加逻辑边（§2.5）：先落边再做 DFS 环检测；**含环的边不落盘**。"""
        edges = plan.setdefault("edges", [])
        edges.append({"from": from_id, "to": to_id, "kind": "LOGICAL"})
        if _has_cycle(edges):
            edges.pop()
            raise ValueError(f"logical edge {from_id}->{to_id} would form a cycle")

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

    def patch(self, plan: dict, diagnosis: dict) -> tuple[dict, dict]:
        """文档 §8.1：诊断 → PlanPatch（复用 logical_subgoal_id）。

        **纯函数**（S13b §2.3 P0-5）：只产出 `(new_plan, event_draft)`，不 append
        事件；由 `Harness.replan()` 用 `append_batch` 与「计数落盘」同帧提交。

        D6：任务改名（T1→T1'）后，边仍写旧 id；此处记录 `旧 id → logical_subgoal_id`
        别名，供 `_predecessors_done` / `rebuild_ready_set` / `_finalize` 三处
        统一解析，避免后继永久 `PREDECESSOR_NOT_DONE`。
        """
        new_plan = copy.deepcopy(plan)
        new_plan["version"] = plan["version"] + 1
        remade: dict[str, dict] = {}
        id_aliases = new_plan.setdefault("_id_aliases", {})
        for tid, task in new_plan["tasks"].items():
            if task["logical_subgoal_id"] in diagnosis.get("failing_subgoals", []):
                new_tid = f"{tid}'"
                # 值保持稳定 logical id（不做 id 链）：一跳解析天然无环
                id_aliases[tid] = task["logical_subgoal_id"]
                self._id_aliases[tid] = task["logical_subgoal_id"]
                task["task_id"] = new_tid
                task["state"] = TaskState.PLANNED
                task["plan_version"] = new_plan["version"]
                remade[new_tid] = task
            else:
                task["plan_version"] = new_plan["version"]
                remade[tid] = task
        new_plan["tasks"] = remade
        # S13b §2.3：走 add_logical_edge（含 `_has_cycle`）而非裸 append
        for dep in diagnosis.get("missing_deps", []):
            self.add_logical_edge(new_plan, dep["from"], dep["to"])
        event_draft = {
            "kind": EventKind.PLAN_PATCHED,
            "subject": plan["run_id"],
            "payload": {"version": new_plan["version"]},
            "run_id": plan["run_id"],
        }
        return new_plan, event_draft


class SlotTracker:
    """活跃槽位计数（可回滚的资源，文档 §5.2）。

    S10a §2.5：可选 `shared` 后端（durable checkpoint store）—— 槽位与预算同属
    一个配额服务，避免「槽位与预算各超各的」。`shared=None` 时退化为纯进程内计数
    （保持既有语义）。
    """

    def __init__(self, limit: int, shared=None) -> None:
        self._limit = limit
        self._active: set[str] = set()
        self._lock = threading.Lock()
        self._shared = shared

    def _shared_read(self) -> set[str]:
        if self._shared is None:
            return set()
        reader = getattr(self._shared, "read_slots", None)
        if not callable(reader):
            return set()
        try:
            return set(reader())
        except Exception:  # noqa: BLE001
            return set()

    def _shared_write(self, keys) -> None:
        if self._shared is None:
            return
        writer = getattr(self._shared, "write_slots", None)
        if not callable(writer):
            return
        try:
            writer(sorted(keys))
        except Exception:  # noqa: BLE001
            pass

    def try_acquire(self, key: str) -> bool:
        """占用槽位。

        共享后端支持 `update_slots`（同一 flock 内 CAS）时，判定与写入**原子**
        完成；否则退回旧的 `read + write`（仅内存后端）。
        """
        cas = getattr(self._shared, "update_slots", None)
        if callable(cas):
            def mutate(shared: set[str]):
                if key in shared:
                    return shared, True          # 幂等
                if len(shared) >= self._limit:
                    return shared, False
                return shared | {key}, True

            try:
                outcome = cas(mutate)
            except Exception:  # noqa: BLE001
                outcome = None
            if outcome is not None:
                ok = bool(outcome)
                with self._lock:
                    if ok:
                        self._active.add(key)
                return ok
        with self._lock:
            active = self._active | self._shared_read()
            if key in active:
                self._active.add(key)
                return True                     # 幂等
            if len(active) >= self._limit:
                return False
            self._active.add(key)
            self._shared_write(active | {key})
            return True

    def release(self, key: str) -> None:
        cas = getattr(self._shared, "update_slots", None)
        if callable(cas):
            try:
                cas(lambda shared: (shared - {key}, None))
                with self._lock:
                    self._active.discard(key)
                return
            except Exception:  # noqa: BLE001
                pass
        with self._lock:
            self._active.discard(key)
            shared = self._shared_read()
            shared.discard(key)
            self._shared_write(shared)

    @property
    def active(self) -> int:
        reader = getattr(self._shared, "read_slots", None)
        if callable(reader):
            try:
                return len(set(reader()))
            except Exception:  # noqa: BLE001
                pass
        with self._lock:
            return len(self._active | self._shared_read())

    def active_keys(self) -> list[str]:
        reader = getattr(self._shared, "read_slots", None)
        if callable(reader):
            try:
                return sorted(set(reader()))
            except Exception:  # noqa: BLE001
                pass
        with self._lock:
            return sorted(self._active | self._shared_read())


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
        """与运行时同一判定入口（§5.9）：token 显式传入 + 四维。

        终审 1-1：**无条件**校验 token 可解析且 `agent_id` 绑定到本 ACB
        （不再只在 `required_tools` 非空时才看能力）——否则
        `try_admit(..., token="garbage-token")` 会直接 ADMITTED。
        """
        token = self.h._effective_token(acb)
        if token is not None:
            resolved = self.h.policy.resolve_token(token)
            if resolved is None or resolved.agent_id not in (acb.id, "*"):
                return False
        required = _normalize_required(required_tools)
        if not required:
            return True
        return all(self.h.policy.check_capability(
            acb.id, token, tool, path, network=network, data_class=data_class)
            for tool, path in required.items())

    def _predecessors_done(self, plan: dict, task_id: str) -> bool:
        """§2.5 前驱闸门：所有指向 `task_id` 的边，其 `src` 必须已终态。

        S13b §2.3 P0-4：`src` 先按当前 plan 版本直接匹配；不命中时走
        `Planner.task_for`（alias 一跳解析到稳定 `logical_subgoal_id`，再取
        **当前版本**的唯一 task）——不是 `setdefault` 保留首个历史 task。
        终态定义与 `RebuildReadySet` 共用 `PREDECESSOR_SATISFIED`。
        """
        for edge in plan.get("edges", []) or []:
            src, dst = _edge_endpoints(edge)
            if dst != task_id:
                continue
            src_task = self.h.planner.task_for(plan, src)
            if src_task is None:
                return False                    # 无法解析 → 保守拒绝
            state = src_task.get("state")
            if state not in PREDECESSOR_SATISFIED:
                return False
        return True

    def try_admit(self, task: dict, run_id: str,
                  required_tools: set[str] | None = None,
                  token=None, plan: dict | None = None) -> ACB:
        """准入谓词（文档 §5）：前驱 → slots → 能力 → 预算 → 逻辑，失败零残留。

        `plan` 可选：显式传入（生产入口）或从 `h.plans[run_id]` 取；
        **拿不到 plan/edges 时安全跳过**前驱闸门（向后兼容）。
        """
        acb = ACB(id=f"A-{run_id}-{task['task_id']}-{self._next_attempt()}",
                  run_id=run_id, task_id=task["task_id"],
                  logical_subgoal_id=task["logical_subgoal_id"])
        # S13b D10：记录准入时的 plan 版本 —— `_finalize` / `_drain_scheduler`
        # 按 `(run_id, task_id, plan_version)` 解析「当前版本唯一 ACB」，
        # 避免 replan 后旧代际 ACB 被误跑（双跑）。
        acb.spec["plan_version"] = int(task.get("plan_version", 1) or 1)
        if token is not None:                 # 显式 token 绑定到 ACB（§5.9）
            acb.capability = token

        # P1-5：准入输入 fail-fast（不占槽位、不留残留）
        input_error = _admission_input_error(task)
        if input_error is not None:
            acb.spec["admission_error"] = input_error
            return self._deny(acb, "INVALID_INPUT")

        eff_plan = plan if plan is not None else self.h.plans.get(run_id)
        if eff_plan is not None and not self._predecessors_done(eff_plan, task["task_id"]):
            return self._deny(acb, "PREDECESSOR_NOT_DONE")

        node = None
        resv = None
        declared: list[str] = []
        committed = False                           # 哨兵：副作用全部完成才置 True
        with self.h.budget.suppressed():            # 准入事务：失败零事件残留
            committed = self._try_admit_commit(
                acb, task, required_tools, eff_plan)
        if committed:
            self.h.budget.flush_pending()           # 成功 → 同帧 append_batch
        else:
            self.h.budget.discard_pending()         # 失败 → 丢弃草稿
        return acb

    def _try_admit_commit(self, acb: ACB, task: dict, required_tools,
                          eff_plan: dict | None) -> bool:
        """准入临界区（槽位 → 能力 → 预算 → 逻辑）；失败逆序回滚。"""
        node = None
        resv = None
        declared: list[str] = []
        committed = False
        try:
            # ① slots
            if not self.h.slots.try_acquire(acb.id):
                self._deny(acb, "NO_SLOT")
                return False

            # ② 能力（无条件：token 存在即校验可解析 + 身份绑定）
            if not self._capability_ok(acb, required_tools):
                self._deny(acb, "DENIED_CAPABILITY")
                return False

            # ③ 预算：limits 留出重试余量，但只预留 min_budget
            min_budget = task["footprint"]["min_budget"]
            limits = min_budget + acb.retry_budget + 1
            node = self.h.budget.create_node(
                f"B-{acb.id}", limits,
                parent_id=task["footprint"].get("parent_budget", "run"),
                run_id=acb.run_id)
            if node is None:
                self._deny(acb, "BUDGET_UNAVAILABLE")
                return False
            node.owner = acb.id
            resv = self.h.budget.reserve(node.node_id, min_budget)
            if resv is None:
                self._deny(acb, "BUDGET_EXHAUSTED")
                return False
            acb.reservation_ids = [resv.reservation_id]
            acb.budget_node = node

            # ④ 逻辑声明（文档 §7.3：先声明，再判定他人是否已声明同一依赖）
            for dep in task["footprint"].get("logical_deps", []):
                self.h.intents.declare_logical(dep["id"], acb.id, dep["kind"])
                declared.append(dep["id"])
                if not self.h.intents.compatible(dep["id"], acb.id, dep["kind"]):
                    self._deny(acb, "CONTRACT_CONFLICT")
                    return False

            acb.state = TaskState.ADMITTED
            committed = True
            return True
        finally:
            if not committed:
                self._rollback(acb, node, resv, declared)

    def _rollback(self, acb: ACB, node, resv, declared: list[str]) -> None:
        """逆序撤销（逻辑 → 预留 → 节点 → 槽位）；不吞原异常。

        S9 §2.1 边界（K-25）：这里**保持** `except Exception` 而非
        `CrashInjected`/`BaseException` —— 回滚链必须跑完，注入的崩溃
        不得中断「逻辑撤销 → 预留释放 → 节点移除 → 槽位释放」。真正的
        进程崩溃（SIGKILL）由子进程用例覆盖，不靠本函数的异常语义。
        """
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
        """从 PlanStore 重建 ready 集（不依赖内存）。

        S13b §2.3 P0-4：依赖判定必须读 alias —— 边仍写旧 id（T1），任务已改名
        （T1'）；`resolve_alias(T1) == S1`，故 `done` 集**同时收录**已终态任务的
        task_id 与 `logical_subgoal_id`（否则一跳解析到的是 subgoal 而非 task id，
        无法命中）。终态定义与 `_predecessors_done` 共用 `PREDECESSOR_SATISFIED`。
        """
        plan = self.h.plans.get(run_id)
        if not plan:
            return []
        satisfied: set[str] = set()
        for tid, task in plan["tasks"].items():
            if task["state"] in PREDECESSOR_SATISFIED:
                satisfied.add(tid)
                subgoal = task.get("logical_subgoal_id")
                if subgoal:
                    satisfied.add(subgoal)
        ready = []
        for tid, task in plan["tasks"].items():
            deps = [src for src, dst in
                    (_edge_endpoints(e) for e in plan.get("edges", []) or [])
                    if dst == tid]
            if all(d in satisfied or self.h.planner.resolve_alias(d, plan) in satisfied
                   for d in deps) and task["state"] in (TaskState.PLANNED, TaskState.ADMITTED):
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


_UNSET = object()          # S13a P0-1：三态哨兵（absent ≠ None）


def classify_verdict(verdict) -> str:
    """审批判词的**严格**分类：`unset` / `approved` / `denied` / `invalid`。

    F1：只认 `{"approved": True}` 为批准、`None` 或 `{"approved": False}` 为拒绝；
    其余（字符串 / 非 dict / `approved` 非 bool / 多余键）一律 `invalid`
    （fail-closed，绝不落到「批准」分支）。
    """
    if verdict is _UNSET:
        return "unset"
    if verdict is None:
        return "denied"
    if isinstance(verdict, dict) and set(verdict) == {"approved"}:
        value = verdict["approved"]
        if value is True:
            return "approved"
        if value is False:
            return "denied"
    return "invalid"


class HITL:
    """审批处理器（S13a §2.2）。

    **三态**：`_UNSET`（还没人审批）/ `None` 或 `{"approved": False}`（显式拒绝）/
    `{"approved": True}`（批准）。超时不在本类判定，由 `Harness.tick()` 驱动。

    审批键是 `(run_id, approval_id)`（P0-2）——`recover` 会把 `generation` +1，
    用 generation 做键会让崩溃前已批准的审批在恢复后失配。`approval_id` 随
    `acb.hitl_pending` 落盘，天然跨进程稳定。

    F3：**没有 legacy 回退键**（旧签名 `set_result(task_id, verdict)` 会让同
    `task_id` 的跨 run 审批互相串键）；`set_result` 拒绝空 `approval_id`。
    """

    def __init__(self) -> None:
        self._pending: dict[tuple[str, str], object] = {}
        # P0-3：扫描（tick）与审批提交必须同一把锁内 CAS，否则「批准/超时」双写
        self._lock = threading.RLock()

    @staticmethod
    def _key(acb: ACB) -> tuple[str, str] | None:
        pending = getattr(acb, "hitl_pending", None)
        if not isinstance(pending, dict):
            return None
        approval_id = pending.get("approval_id")
        run_id = getattr(acb, "run_id", "")
        if not isinstance(approval_id, str) or not approval_id:
            return None
        if not isinstance(run_id, str) or not run_id:
            return None
        return (run_id, approval_id)

    def peek(self, acb: ACB):
        """只查不消费（供 `tick` 在锁内做「是否已有决策」CAS）。"""
        key = self._key(acb)
        if key is None:
            return _UNSET
        with self._lock:
            return self._pending.get(key, _UNSET)

    def resolve(self, acb: ACB):
        """返回并**消费**审批结果：`_UNSET` | `None` | `{"approved": bool}`。

        命中即 `pop`（F3：防 `_pending` 无界增长；键不存在时返回 `_UNSET`）。
        """
        key = self._key(acb)
        if key is None:
            return _UNSET
        with self._lock:
            return self._pending.pop(key, _UNSET)

    def set_result(self, run_id: str, approval_id: str, verdict) -> None:
        """提交审批结果（**唯一**签名：`set_result(run_id, approval_id, verdict)`）。

        F1：入口 schema 校验 —— 只接受 `None` 或 `{"approved": bool}`，
        其余抛 `ValueError`（不再静默接受任意对象）；F3：`run_id` /
        `approval_id` 必须是非空字符串（空键会退化为 `(run_id, "")`，
        一次提交可批准该 run 的所有空 pending）。
        """
        if not isinstance(run_id, str) or not run_id:
            raise ValueError("set_result: run_id 必须是非空字符串")
        if not isinstance(approval_id, str) or not approval_id:
            raise ValueError("set_result: approval_id 必须是非空字符串")
        valid = (verdict is None
                 or (isinstance(verdict, dict)
                     and set(verdict) == {"approved"}
                     and isinstance(verdict["approved"], bool)))
        if not valid:
            raise ValueError(
                "set_result: verdict 只接受 None | {'approved': bool}")
        with self._lock:
            self._pending[(run_id, approval_id)] = verdict
