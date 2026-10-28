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
from dataclasses import dataclass

from .types import ACB, TASK_TERMINAL, EventKind, Status, TaskState, advance
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


# S13b §2.3：前驱闸门与 ready 重建必须共用**同一套终态定义**。
# R3：**CANCELLED 不算前驱满足**。取消/失败代表依赖分支已死；若把 CANCELLED
# 当「满足」，`_finalize` 会把后继推起来真跑（拿到了从未产出的输入）。
# 取消必须由 `propagate_failure` 级联到后继，而不是放行。
#
# X2（S13 跨波次终审）：`DONE_PARTIAL` 是「有产出、未全覆盖」的**任务级终态**，
# 必须算前驱满足（方案 a）。否则父汇总为 DONE_PARTIAL 后下游永久 PLANNED、
# ready 集为空、Run 永不 finalize。更严的「输出不完整」约束应由 plan 的
# `required_coverage` 表达，而不是把整条下游分支静默吊死。
PREDECESSOR_SATISFIED = frozenset({TaskState.DONE, TaskState.DONE_PARTIAL})


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
    # S13c：子份额是父 limits 的组成部分，非法值必须在准入边界 fail-fast。
    child_alloc = footprint.get("child_alloc", 0)
    if (isinstance(child_alloc, bool) or not isinstance(child_alloc, int)
            or child_alloc < 0):
        return "bad_child_alloc"
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

    @staticmethod
    def _validate_subgoals(tasks: dict) -> None:
        """R11：`logical_subgoal_id` 在**同一 plan 内必须唯一**（fail-fast）。

        idem_key / 收据 scope / effect 去重都以 `(run_id, logical_subgoal_id)`
        为键；两个任务共享同一 subgoal 会让「一个完成」误当成「两个都完成」
        （`_finalize` 的同 subgoal 后继命中），也会让副作用去重串味。
        """
        seen: set[str] = set()
        for task in tasks.values():
            subgoal = task.get("logical_subgoal_id")
            if not subgoal:
                continue
            if subgoal in seen:
                raise ValueError(f"duplicate logical_subgoal_id: {subgoal!r}")
            seen.add(subgoal)

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
                    # S13c §2.4 P0-8：父预算挂点与子份额**不得丢弃**（否则
                    # 恒为 "run" → 多级预算树不可达；`child_alloc` 缺失会让父
                    # limits 只剩 retry_budget+1，任何正常 child 都放不下）。
                    "parent_budget": spec.get("parent_budget", "run"),
                    "child_alloc": spec.get("child_alloc", 0),
                },
                "merge_policy": spec.get("merge_policy") or {},
                "state": TaskState.PLANNED,
                "plan_version": 1,
            }
        self._validate_subgoals(plan["tasks"])
        self.h.event_log.append(EventKind.PLAN_CREATED, run_id, {"tasks": list(plan["tasks"])})
        return plan

    def resolve_alias(self, old_id: str, plan: dict | None) -> str:
        """`old_id` → 稳定 `logical_subgoal_id`（**一跳解析**，不做链式递归）。

        值**永远**是 `logical_subgoal_id`（不是下一个 task id）：这样
        `missing_deps` 直接复用 `add_logical_edge`（含环检测）也不会因 id 链
        造出环（S13b K-65）。

        R2：别名表**只属于单个 plan**（`plan["_id_aliases"]`）。没有该键 → 恒等
        返回；**不存在 Planner 级全局回退**（全局表会让 Run A 的 patch 串到
        Run B，把从未 patch 过的前驱误判为已满足）。
        """
        aliases = (plan or {}).get("_id_aliases")
        if not isinstance(aliases, dict):
            return old_id
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

    def _canonical_task_id(self, plan: dict, task_id: str) -> str | None:
        """R10：把边端点规范化到**当前版本存在的 task id**（alias 一跳）。"""
        task = self.task_for(plan, task_id)
        return task.get("task_id") if isinstance(task, dict) else None

    def add_logical_edge(self, plan: dict, from_id: str, to_id: str) -> None:
        """加逻辑边（§2.5）：先规范化端点并**按 `(from,to,kind)` 去重**，
        再落边并做 DFS 环检测；**含环的边不落盘**。

        R10：① 重复加同一条边是 replan 风暴下的常态（每次 patch 都读
        `missing_deps`）→ 不去重会让 `edges` 无界增长；② `from`/`to` 仍写
        改名前的旧 id 时，边会落在已不存在的 task 上 → 依赖形同虚设。
        端点在当前 plan 中无法解析 → **fail-fast**（不静默落一条死边）。
        """
        edges = plan.setdefault("edges", [])
        src = self._canonical_task_id(plan, from_id)
        dst = self._canonical_task_id(plan, to_id)
        if src is None or dst is None:
            raise ValueError(f"logical edge unknown task: {from_id}->{to_id}")
        for e in edges:
            ef, et = _edge_endpoints(e)
            kind = e.get("kind", "LOGICAL") if isinstance(e, dict) else "LOGICAL"
            ef_c = self._canonical_task_id(plan, ef) or ef
            et_c = self._canonical_task_id(plan, et) or et
            if ef_c == src and et_c == dst and kind == "LOGICAL":
                return                      # 同一依赖 → 幂等
        edges.append({"from": src, "to": dst, "kind": "LOGICAL"})
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

        R2：别名只写**本 plan 副本**的 `_id_aliases`，不再写 Planner 级全局表。
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
                task["task_id"] = new_tid
                task["state"] = TaskState.PLANNED
                task["plan_version"] = new_plan["version"]
                remade[new_tid] = task
            else:
                task["plan_version"] = new_plan["version"]
                remade[tid] = task
        new_plan["tasks"] = remade
        # R11：remake 后同一 subgoal 不得对应两个 task（否则一个完成会推出全部）
        self._validate_subgoals(remade)
        # S13b §2.3：走 add_logical_edge（含 `_has_cycle` + 去重 + 端点规范化）
        for dep in diagnosis.get("missing_deps", []):
            self.add_logical_edge(new_plan, dep["from"], dep["to"])
        # S14 K-67(B)：**物化 edges 端点**为当前 canonical id。
        # 与 Camunda「迁移时把 A→C 物化成实例挂在 C 上」同构：patch 之后 edges
        # 不再引用旧 id，别名只剩「**活跃 ACB 的旧 task_id** → 当前 task」这一种
        # 用途 —— 这样剪枝才有安全判据（否则会剪掉在途 ACB 的映射）。
        _subgoal_to_tid: dict[str, str] = {}
        for _tid, _task in remade.items():
            _subgoal_to_tid.setdefault(_task["logical_subgoal_id"], _tid)

        def _canon(x: str) -> str:
            if x in remade:
                return x
            sg = id_aliases.get(x)
            if sg is not None:
                return _subgoal_to_tid.get(sg, x)
            return x

        materialized: list[dict] = []
        for edge in new_plan.get("edges", []) or []:
            edge = dict(edge)
            edge["from"] = _canon(str(edge.get("from", "")))
            edge["to"] = _canon(str(edge.get("to", "")))
            materialized.append(edge)
        new_plan["edges"] = materialized
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
            parent_id = getattr(acb, "parent_id", "") or ""
            if parent_id and str(getattr(resolved, "parent_id", "") or "") != str(parent_id):
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
                  token=None, plan: dict | None = None, *,
                  replace: bool = False) -> ACB:
        """准入谓词（文档 §5）：前驱 → slots → 能力 → 预算 → 逻辑，失败零残留。

        `plan` 可选：显式传入（生产入口）或从 `h.plans[run_id]` 取；
        **拿不到 plan/edges 时安全跳过**前驱闸门（向后兼容）。

        R6：准入层按 `(run_id, task_id, plan_version)` 做唯一性登记。同一 key
        已有**活跃** ACB（非终态、非隔离）→ 直接返回该 ACB，绝不再造一个
        `A-...-{attempt}` 让 drain 双跑。`replace=True` 仅供显式重新准入
        （`requeue_readmit`）使用。
        """
        plan_version = int(task.get("plan_version", 1) or 1)
        key = (run_id, task["task_id"], plan_version)
        prev = None
        if not replace:
            existing = self.h._lookup_admission(key)
            if existing is not None:
                return existing
        else:
            # C6：replace 也必须继承**旧 ACB 的 lineage**（parent_id 不可变）。
            prev = self.h._lookup_admission(key)
        acb = ACB(id=f"A-{run_id}-{task['task_id']}-{self._next_attempt()}",
                  run_id=run_id, task_id=task["task_id"],
                  logical_subgoal_id=task["logical_subgoal_id"])
        if prev is not None:
            acb.parent_id = prev.parent_id
            acb.children_ids = list(prev.children_ids)
            acb.children_pending = copy.deepcopy(prev.children_pending)
            if prev.parent_id and token is None:
                # 子被重新准入仍必须是原子树之子：预先按旧 token 签发继承快照，
                # 否则 `_capability_ok` 会因 parent_id 非空而 fail-closed。
                self.h._adopt_capability(prev, acb)
        # S13b D10：记录准入时的 plan 版本 —— `_finalize` / `_drain_scheduler`
        # 按 `(run_id, task_id, plan_version)` 解析「当前版本唯一 ACB」，
        # 避免 replan 后旧代际 ACB 被误跑（双跑）。
        acb.spec["plan_version"] = plan_version
        if token is not None:                 # 显式 token 绑定到 ACB（§5.9）
            acb.capability = token

        # P1-5：准入输入 fail-fast（不占槽位、不留残留）
        input_error = _admission_input_error(task)
        if input_error is not None:
            acb.spec["admission_error"] = input_error
            return self._deny(acb, "INVALID_INPUT")

        eff_plan = plan if plan is not None else self.h.plans.get(run_id)
        # R11：同一 plan 内 subgoal 唯一（共享 subgoal 会串 idem_key / 误判完成）
        if eff_plan is not None:
            dup = [t for t in eff_plan.get("tasks", {}).values()
                   if isinstance(t, dict)
                   and t.get("logical_subgoal_id") == task["logical_subgoal_id"]]
            if len(dup) > 1:
                acb.spec["admission_error"] = "DUPLICATE_SUBGOAL"
                return self._deny(acb, "INVALID_INPUT")
            if not self._predecessors_done(eff_plan, task["task_id"]):
                return self._deny(acb, "PREDECESSOR_NOT_DONE")

        node = None
        resv = None
        declared: list[str] = []
        committed = False                           # 哨兵：副作用全部完成才置 True
        with self.h.budget.suppressed():            # 准入事务：失败零事件残留
            committed = self._try_admit_commit(
                acb, task, required_tools, eff_plan)
        if committed:
            if replace:
                self.h._clear_admission(key)
            self.h._register_admission(acb)
            self.h.budget.flush_pending()           # 成功 → 同帧 append_batch
            # S13d §2.5 / B-4：aging 是**墙钟**语义 —— 准入成功即起算，
            # `tick()` 扫描到期项（`aging_ms=0` 表示禁用，不改既有语义）。
            self.h._arm_aging(acb)
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

            # ③ 预算：limits 留出重试余量与**子任务份额**，但只预留 min_budget
            min_budget = task["footprint"]["min_budget"]
            # C7：`child_alloc` 缺省/0 → 可推导默认（max_fanout × 每个子最小预算）；
            # 显式正值过小 → 准入 fail-fast（否则父永远 spawn 不出一个合格子）。
            child_alloc = task["footprint"].get("child_alloc", 0) or 0
            if child_alloc <= 0:
                child_alloc = max(self.h.max_fanout, 1) * self.h.min_budget_per_child
            if child_alloc < self.h.min_budget_per_child:
                acb.spec["admission_error"] = "child_alloc_too_small"
                self._deny(acb, "INVALID_INPUT")
                return False
            limits = min_budget + child_alloc + acb.retry_budget + 1
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
            # C7：父 spawn 时按 `child_alloc_used` 记账（初值 0，随快照持久化）。
            acb.spec["child_alloc"] = int(child_alloc)
            acb.spec["min_budget"] = int(min_budget)
            acb.spec["child_alloc_used"] = 0

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


@dataclass
class Claim:
    """S13d §2.5 P0-10：ready 队列键的**所有权租约**（防双跑 = claim + lease + fencing）。

    - `worker_id`：持有者身份（同一写者重入不视为他人）；
    - `lease_until_ms`：租约到期（墙钟，来自注入时钟）；到期后另一 worker 可重 claim；
    - `attempt`：重 claim 次数（首 claim = 1）；
    - `fence_epoch`：重 claim 时**原子**推进的 epoch；写路径比对「执行上下文
      epoch」（X1：绝不写回共享 ACB）—— 旧 worker 的写会被判 `REJECT_STALE`。
    """
    acb_id: str
    worker_id: str
    lease_until_ms: int
    attempt: int = 1
    fence_epoch: int = 0


class Scheduler:
    """ready_queue + **单一 pop 所有者**（S13d §2.5）。

    `mode="sync"`（默认）保持既有语义：`push` → 调用方内联 `_run_one` →
    `finally` drain；`pop()` 只做队列出队，**不 claim**（36 个同步用例零破坏）。

    `mode="pool"`：`pop_claimed()` 在**同一把锁内**完成「出队 + claim」；
    只有拿到 claim 的 worker 才执行，重 claim 时原子 `supervisor_note_crash(old)`
    （revoke + bump_epoch）后再写新 claim。
    """

    def __init__(self, harness, mode: str = "sync") -> None:
        self.h = harness
        self.mode = mode if mode in ("sync", "pool") else "sync"
        self._ready: list[str] = []
        self._claims: dict[str, Claim] = {}
        self._claim_history: list[Claim] = []
        # X1：被**撤销**的 claim 的 epoch 水位（acb_id → 最小有效 epoch）。
        # claim 释放后仍要 fence 旧 worker，故不能只看 `_claims`（否则一释放
        # `_claim_fenced` 就恒 False，审批超时被在途 worker 抹掉）。
        self._revoked: dict[str, int] = {}
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

    # ---- claim / lease / fencing（P0-10）----------------------------
    @property
    def claims(self) -> dict[str, Claim]:
        with self._lock:
            return dict(self._claims)

    @property
    def claim_history(self) -> list[Claim]:
        with self._lock:
            return list(self._claim_history)

    def claim_of(self, acb_id: str) -> Claim | None:
        with self._lock:
            return self._claims.get(acb_id)

    def peek_claim(self, acb_id: str) -> Claim | None:
        """**无锁**只读（供 `checkpoint._build_payload` 构造 claim 快照）。

        走锁会形成 `scheduler._lock → store.lock`（claim_kernel → save）与
        `store.lock → scheduler._lock`（payload）之间的 AB-BA 环；dict 单键读
        在 CPython 下原子，这里只用于观测量，故可无锁。
        """
        return self._claims.get(acb_id)

    def revoked_epoch(self, acb_id: str) -> int | None:
        """被撤销 claim 的 epoch 水位（无锁只读；旧上下文 `<` 水位 → fenced）。"""
        return self._revoked.get(acb_id)

    def _epoch_now(self, run_id: str | None) -> int:
        """当前 epoch（run 层优先，无 run 隔离则全局）；claim 的 fence 基准。"""
        try:
            return int(self.h.intents.get_epoch(run_id or None))
        except Exception:  # noqa: BLE001  # 读失败不得阻断 claim
            return 0

    def _claim_locked(self, acb_id: str, worker_id: str | None,
                      acb: ACB | None) -> Claim | None:
        """临界区内的 claim（调用方已持 `_lock`）。

        有效租约归**他人** → `None`（不得双持）；租约到期 → 原子
        `supervisor_note_crash(old)` 后重 claim（epoch 单调递增）。
        """
        now = int(self.h.clock.now_ms())
        cur = self._claims.get(acb_id)
        if (cur is not None and cur.lease_until_ms > now
                and cur.worker_id != (worker_id or "")):
            return None
        attempt = 1 if cur is None else cur.attempt + 1
        if cur is not None and acb is not None:
            try:
                self.h.supervisor_note_crash(acb)   # revoke + bump_epoch + 落盘
            except Exception:  # noqa: BLE001  # 回收失败不得阻断重新 claim
                pass
        run_id = getattr(acb, "run_id", "") if acb is not None else ""
        fence = self._epoch_now(run_id)
        if cur is not None and fence <= cur.fence_epoch:
            # 兜底：epoch 必须严格单调（否则旧 worker 的写会通过 fence）
            fence = cur.fence_epoch + 1
            try:
                fence = max(fence, int(self.h.bump_epoch(run_id=run_id or None)))
            except Exception:  # noqa: BLE001
                pass
        claim = Claim(acb_id=acb_id, worker_id=worker_id or "",
                      lease_until_ms=now + int(self.h.claim_lease_ms),
                      attempt=attempt, fence_epoch=fence)
        self._claims[acb_id] = claim
        self._claim_history.append(claim)
        # X1：**绝不**把 fence 写回共享 ACB —— 旧 worker 与主线程持同一对象，
        # 写回会让双方 epoch 恒等 → `_claim_fenced` 永远 False（审批门被绕过）。
        # 执行上下文（线程本地）由 worker 入口绑定，见 `Harness.claim_scope`。
        return claim

    def claim(self, acb_id: str, worker_id: str | None = None) -> Claim | None:
        with self._lock:
            return self._claim_locked(acb_id, worker_id, self.h.acbs.get(acb_id))

    def pop_claimed(self, worker_id: str | None = None
                    ) -> tuple[str, Claim | None] | None:
        """同一锁内「出队 + claim」（pool 模式唯一 pop 所有者）。"""
        with self._lock:
            if not self._ready:
                return None
            key = self._ready.pop(0)
            acb = self.h.acbs.get(key)
            return key, self._claim_locked(key, worker_id, acb)

    def release_claim(self, acb_id: str, worker_id: str | None = None) -> bool:
        """正常释放 claim（仅持有者可释放）；幂等。

        **不**记录撤销水位 —— 正常完成/交接后同一 epoch 的后续 claim 合法
        （`test_s13_11` 的重复 ready 键依赖此语义）。
        """
        with self._lock:
            cur = self._claims.get(acb_id)
            if cur is None:
                return False
            if worker_id is not None and cur.worker_id != worker_id:
                return False
            self._claims.pop(acb_id, None)
            return True

    def revoke_claim(self, acb_id: str, worker_id: str | None = None,
                     watermark: int | None = None) -> Claim | None:
        """**撤销** claim（X1）：释放并记录 epoch 水位；旧上下文低于水位 → fenced。

        与 `release_claim` 分离：撤销（tick 超时 / supervisor 崩溃回收）后，
        旧 worker 即使已经没有 claim 可比对，也必须被判 stale，否则解锁后继续
        执行会把审批超时覆盖成 DONE。水位取 `max(原 fence, watermark)`，调用方
        传入 bump 之后的 epoch。
        """
        with self._lock:
            cur = self._claims.pop(acb_id, None)
            if cur is None:
                # 没有活跃 claim 时也要推进水位（如 tick 在 claim 已释放后判超时）
                if watermark is not None:
                    self._revoked[acb_id] = max(
                        int(self._revoked.get(acb_id, 0)), int(watermark))
                return None
            if worker_id is not None and cur.worker_id != worker_id:
                self._claims[acb_id] = cur          # 归还（非持有者不得撤销）
                return None
            mark = cur.fence_epoch
            if watermark is not None:
                mark = max(mark, int(watermark))
            self._revoked[acb_id] = max(int(self._revoked.get(acb_id, 0)), mark)
            return cur


class RunCoordinator:
    """B1：唯一 Run 状态写入者；取消直接广播（不依赖父子链转发）。

    S13c §2.4 P1-2：取消必须**级联** —— 既能走内存 `parent/children` 快路径
    （A5 手工建树、不写 checkpoint），也能读持久化 `children_ids`（含崩溃前
    已 spawn 的子）。结算全部走幂等 API（`settle`/`slots.release` 返回 0 不报错，
    `advance("cancel")` 对终态返回 `INVALID_INPUT` 不抛）。
    """

    def __init__(self, harness) -> None:
        self.h = harness
        self._cancelled: set[str] = set()
        self._cancelled_acbs: set[str] = set()   # 子用独立 run_id 时的取消依据
        self._active: dict[str, str] = {}
        self._lock = threading.RLock()

    def register(self, acb: ACB) -> None:
        with self._lock:
            self._active[acb.id] = acb.run_id

    def is_cancelled(self, acb: ACB) -> bool:
        """先查 **acb_id**（子可能用独立 run_id），再查 run_id。"""
        with self._lock:
            return (acb.id in self._cancelled_acbs
                    or acb.run_id in self._cancelled)

    def cancel_run(self, run_id: str, *, cascade: bool = True) -> None:
        """取消广播给所有后代（文档 §5.2⑦ / S13c §2.4 P1-2）。

        `cascade=False` 只取消该 run 的**顶层** ACB（负对照用；子孙保持非终态）。
        """
        with self._lock:
            self._cancelled.add(run_id)
            candidates = list(self._active.items())
        roots: list[ACB] = []
        seen_ids: set[str] = set()
        for acb in list(self.h.acbs.values()):
            if acb.run_id != run_id or acb.id in seen_ids:
                continue
            if not cascade and getattr(acb, "parent_id", ""):
                continue
            seen_ids.add(acb.id)
            roots.append(acb)
        seen_ids.clear()
        for acb in roots:
            self._cancel_one(acb, seen_ids, cascade=cascade)
        if not roots:
            # 纯 run_id 广播（旧语义，A5/ms4 兼容）：没有可定位的 ACB 时只标记 run。
            for acb_id, rid in candidates:
                if rid == run_id:
                    self.h.intents.revoke_all(acb_id)
        if cascade:
            self._cascade_from_persisted(run_id, seen_ids)

    # ---- 级联实现 --------------------------------------------------
    def _cancel_one(self, acb: ACB, seen: set[str], *, cascade: bool) -> None:
        """取消单个 ACB（幂等结算）并按 `cascade` 递归其子。

        C3/C10：父即使已终态，也必须继续遍历其子（A2 语义：父 DONE 而子仍
        active 时，Run 取消仍要级联）；只跳过父自身的结算。
        """
        if acb.id in seen:
            return
        seen.add(acb.id)
        with self._lock:
            self._cancelled_acbs.add(acb.id)
        if acb.state in TASK_TERMINAL:
            self.h._drain_skipped.append((acb.id, "cancel_skip_terminal"))
            if cascade:
                self._walk_children(acb, seen)
            return
        # 先撤销子树，再结算自身：父预算节点含活跃子预留时无法归档，反序会
        # 留下永不归档的父节点（预算泄漏）。
        if cascade:
            self._walk_children(acb, seen)
        self._settle_cancel(acb)

    def _settle_cancel(self, acb: ACB) -> None:
        """状态 → CANCELLED + 租约/预算/槽位**幂等**结算 + 落盘。"""
        try:
            with self.h._transition_lock(acb):
                advance(acb, "cancel")          # 终态 → INVALID_INPUT（不抛）
                self.h.intents.revoke_all(acb.id)
                for rid in list(acb.reservation_ids):
                    self.h.budget.settle(rid)    # 已结算/未知 → 0
                acb.reservation_ids = []
                self.h.slots.release(acb.id)     # 幂等
                node = acb.budget_node
                if node is not None:
                    node_id = node.node_id
                    if not self.h.budget.archive_node(node_id):
                        self.h.budget.remove_node(node_id)
                    acb.budget_node = None
                self.h._safe_save(acb)
        except Exception as exc:  # noqa: BLE001  # 单个后代异常绝不中断级联
            self.h._drain_skipped.append((acb.id, f"cancel_error:{type(exc).__name__}"))
        self.h.bump_epoch()

    def _walk_children(self, acb: ACB, seen: set[str]) -> None:
        """内存快路径（`children`）+ 持久化链（`children_ids`）+ pending 三源遍历。

        C1：`children_ids` 的 ref 为 `None`（两阶段窗口）→ 按 `child_id`
        反查最新快照；仍找不到 → 孤儿结算（不静默 skip）。`children_pending`
        （phase ① 已落盘、② 未完成）同样按此处理。
        """
        entries: list[tuple[ACB | None, str | None, str]] = []
        for child in list(getattr(acb, "children", []) or []):
            if isinstance(child, ACB):
                entries.append((child, None, child.id))
        for entry in list(getattr(acb, "children_ids", []) or []):
            if isinstance(entry, ACB):
                entries.append((entry, None, entry.id))
                continue
            try:
                cid, ref = str(entry[0]), (entry[1] if len(entry) > 1 else None)
            except (TypeError, IndexError):
                self.h._drain_skipped.append((acb.id, "cancel_bad_child_entry"))
                continue
            entries.append((self.h.acbs.get(cid), ref, cid))
        pending = getattr(acb, "children_pending", None) or {}
        for cid, info in pending.items():
            cid = str(cid)
            entries.append((self.h.acbs.get(cid), None, cid))
            # 保留 info 供孤儿结算使用
            if not isinstance(info, dict):
                pending[cid] = {}
        # X6：跨 run 子（`spec.spawn.parent_id`）反向索引 —— 老 checkpoint 的
        # `children_ids` 缺失时，子可能既不在父 run 枚举、也不在上面的三源里。
        # 不补这一步 → 取消**静默漏掉**跨 run 子。
        self.h._ensure_children_index()
        known = {str(cid) for _child, _ref, cid in entries}
        for cid in sorted(self.h._children_index.get(acb.id, ())):
            if cid in seen or cid in known:
                continue
            entries.append((self.h.acbs.get(cid), None, cid))
        for child, ref, cid in entries:
            if child is not None:
                self._cancel_one(child, seen, cascade=True)
                continue
            if not ref:
                ref = self.h._ref_for_child(cid, pending.get(cid)
                                            if isinstance(pending.get(cid), dict)
                                            else None)
            if ref:
                self._cancel_from_ref(ref, seen, child_id=cid)
                continue
            # C1：无 ref 且无快照 → 孤儿结算（事件留痕 + _drain_skipped）。
            self.h._settle_orphan_child(acb, cid, pending.get(cid))

    def _cancel_from_ref(self, ref: str | None, seen: set[str],
                         *, child_id: str | None = None) -> None:
        """持久化子（进程内已无 ACB）：按 ref 加载快照后取消；缺失/终态 skip。

        C1：`ref=None` 但给了 `child_id` → 按 `latest_ref_for_acb` 找回。
        C3：终态子也要继续遍历其后代（父终态不代表子树终态）。
        """
        if not ref and child_id:
            ref = self.h._ref_for_child(str(child_id))
        if not ref:
            self.h._drain_skipped.append(
                (str(child_id if child_id is not None else ref),
                 "cancel_skip_missing_ref"))
            return
        try:
            data = self.h.checkpoints.load_strict(ref)
        except Exception:  # noqa: BLE001
            try:
                data = self.h.checkpoints.load(ref)
            except Exception:  # noqa: BLE001
                self.h._drain_skipped.append((str(ref), "cancel_skip_unreadable_ref"))
                return
        acb = self._stub_from_payload(data)
        if acb.id in seen:
            return
        seen.add(acb.id)
        if acb.state in TASK_TERMINAL:
            self.h._drain_skipped.append((acb.id, "cancel_skip_terminal"))
            self._walk_children(acb, seen)
            return
        node_id = data.get("budget_node_id")
        node = self.h.budget._nodes.get(node_id) if node_id else None
        if node_id and node is None:
            self.h._drain_skipped.append((acb.id, "cancel_skip_node_archived"))
        acb.budget_node = node
        acb.reservation_ids = list(data.get("reservation_ids") or [])
        with self._lock:
            self._cancelled_acbs.add(acb.id)
        # 子树先撤（与 `_cancel_one` 同序，保证父节点可归档）。
        self._walk_children(acb, seen)
        self._settle_cancel(acb)

    def _cascade_from_persisted(self, run_id: str, seen: set[str]) -> None:
        """C2：枚举该 run 的**全部** ACB 快照并重建父子图后撤销。

        root 不能再取 `latest_ref_for_run` —— 该指针会被任意同 run 的后代
        `save` 覆盖成后代，导致父/兄弟漏取消。这里按 `refs_for_run` 逐项加载，
        以 `parent_id ∉ 本 run 集合` 者为根（图断开时也兜底全撤）。
        """
        getter = getattr(self.h.checkpoints, "refs_for_run", None)
        if not callable(getter):
            return
        try:
            entries = getter(run_id)
        except Exception:  # noqa: BLE001
            return
        loaded: dict[str, tuple[str, dict]] = {}
        for acb_id, ref in entries or []:
            acb_id = str(acb_id)
            if acb_id in seen:
                continue
            ref = ref or self.h._ref_for_child(acb_id)
            if not ref:
                self.h._drain_skipped.append(
                    (acb_id, "cancel_missing_persisted_ref"))
                continue
            try:
                data = self.h.checkpoints.load_strict(ref)
            except Exception:  # noqa: BLE001
                try:
                    data = self.h.checkpoints.load(ref)
                except Exception:  # noqa: BLE001
                    self.h._drain_skipped.append(
                        (acb_id, "cancel_unreadable_persisted_ref"))
                    continue
            if isinstance(data, dict):
                loaded[acb_id] = (ref, data)
        if not loaded:
            return
        parent_of = {aid: str((data.get("parent_id") or ""))
                     for aid, (_ref, data) in loaded.items()}
        roots = [aid for aid in loaded if parent_of.get(aid) not in loaded]
        for aid in roots:
            if aid in seen:
                continue
            ref, _data = loaded[aid]
            self._cancel_from_ref(ref, seen, child_id=aid)
        # 环/断链兜底：仍未被撤销的节点逐个处理（绝不静默漏掉）。
        for aid, (ref, _data) in loaded.items():
            if aid not in seen:
                self._cancel_from_ref(ref, seen, child_id=aid)

    @staticmethod
    def _stub_from_payload(data: dict) -> ACB:
        acb = ACB(id=data.get("acb_id", ""), run_id=data.get("run_id", ""),
                  task_id=data.get("task_id", ""),
                  logical_subgoal_id=data.get("logical_subgoal_id", ""))
        try:
            acb.state = TaskState(data.get("state", TaskState.PLANNED.value))
        except ValueError:
            acb.state = TaskState.PLANNED
        acb.parent_id = data.get("parent_id", "") or ""
        children: list = []
        for child in data.get("children_ids") or []:
            if isinstance(child, (list, tuple)) and child:
                children.append((child[0], child[1] if len(child) > 1 else None))
        acb.children_ids = children
        pending = data.get("children_pending")
        if isinstance(pending, dict):
            acb.children_pending = {
                str(cid): dict(info) if isinstance(info, dict) else {}
                for cid, info in pending.items()
            }
        return acb

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

    def scan_deadlines(self, now_ms: int) -> int:
        """S13d §2.5：aging（**墙钟**）扫描 —— 唯一超时驱动面的调度侧入口。

        `aging_ms` 与步数语义的 `aging_max` 是**两套**东西（B-4）：这里只看
        `aging_deadline`（`try_admit` 成功后设 `now + aging_ms`），到点 →
        `Harness._aging_timeout`（`ADMITTED → INADMISSIBLE` + 结算）。

        返回本次处理的 ACB 数（供调用方观测；0 = 无到期项）。
        """
        now = int(now_ms)
        acted = 0
        for acb in list(self.h.acbs.values()):
            if acb.state is not TaskState.ADMITTED:
                continue
            deadline = getattr(acb, "aging_deadline", None)
            if deadline is None or now < int(deadline):
                continue
            self.h._aging_timeout(acb)
            acted += 1
        return acted


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
