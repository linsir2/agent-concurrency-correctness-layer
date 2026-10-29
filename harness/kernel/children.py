"""ChildManager —— 子任务 / spawn 簇（S3.1 从 Harness 迁出，纯位移）。

搬出的是「父子生命周期」整簇：

- spawn 三阶段（登记 pending → 建子 → 转正回填 ref）与逆序回滚；
- 崩溃窗口的孤儿结算 / `ref=None` 对账（留痕，绝不静默）；
- 结果信封汇总（`result_envelope`）与父状态落地（`apply_result_envelope`）；
- 取消级联用的父→子反扫索引（X6）。

约定（与 `kernel/commit.py` 一致）：

- `ChildDeps` 只放**稳定对象引用**与 late-lookup callable；可变配置一律在调用时读
  `self.state`（`spawn_depth` / `max_fanout` / `min_budget_per_child` 等都是构造后
  可改的，冻结进 deps 会读到旧值）。
- 仍留在 Harness 上的方法（`_safe_save` / `_effective_token` /
  `_register_admission` / `_transition_lock` / `_finalize`）以 **lambda** 注入，
  调用时才做属性查找 —— 绑定方法会冻结原实现，让测试的实例级打桩失效。
- 本模块的构造**不产生任何事件**（`__init__` 只保存引用）。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from ..orchestration import _tenant_of_node_id
from ..policy import CapabilityAttenuationError, Scope, attenuate_token
from ..storage._lock import FencingRejected
from ..types import (
    ACB,
    TASK_TERMINAL,
    EventKind,
    ResultEnvelope,
    Status,
    TaskState,
    advance,
)


class _SpawnAbort(Exception):
    """C1：spawn ② 事务的「明确拒绝」信号（非崩溃），由回滚分支转成 failure。"""


@dataclass(frozen=True)
class ChildDeps:
    # ---- 稳定对象引用（构造期注入，不随配置变化）----
    budget: Any
    checkpoints: Any
    event_log: Any
    policy: Any
    slots: Any
    run_coordinator: Any
    # ---- 仍留在 Harness 上的方法：late lookup，绝不传绑定方法 ----
    safe_save: Callable[[ACB], bool]
    effective_token: Callable[[ACB], Any]
    register_admission: Callable[[ACB], None]
    transition_lock: Callable[[ACB], Any]
    finalize: Callable[[ACB], None]


class ChildManager:
    """父子生命周期唯一实现（Harness 仅保留同名委托，S5 删除）。"""

    def __init__(self, deps: ChildDeps, state) -> None:
        self.deps = deps
        self.state = state

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
                return self.state.spawn_depth + 1
            seen.add(cur.id)
            depth += 1
            parent = self.state.acbs.get(cur.parent_id)
            if parent is None:
                # 父链断链：无法证明未越界 → 按上限拒绝（fail-closed）。
                return self.state.spawn_depth + 1
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
        self.state.drain_skipped.append(
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
                self.deps.budget.release(resv.reservation_id)
                self.deps.budget._reservations.pop(resv.reservation_id, None)
                if node is not None:
                    node.reservations.pop(resv.reservation_id, None)
            except Exception:  # noqa: BLE001
                pass
        if node is not None:
            try:
                if not self.deps.budget.remove_node(node.node_id, owner=child_id):
                    self.deps.budget.archive_node(node.node_id)
            except Exception:  # noqa: BLE001
                pass
        if slot:
            self.deps.slots.release(child_id)

    def _save_strict(self, acb: ACB) -> str:
        """C1：关键落盘（spawn 两阶段）**不得吞异常**。

        `_safe_save` 的静默 `pass` 会让「崩溃可续做」失去保证；这里让普通
        存储错误也冒泡，调用方据此回滚 / 由恢复路径按 `children_pending` 收口。
        """
        return self.deps.checkpoints.save(acb)

    def _parent_child_alloc(self, parent_acb: ACB) -> tuple[int, int]:
        """C7：父的任务子预算配额与已用额（`child_alloc` / `child_alloc_used`）。

        未显式声明（或缺省 0）→ 用 `max_fanout * min_budget_per_child` 推导；
        已用额随父 ACB 持久化，spawn 成功时按 `slice` 记账、失败/孤儿回收回滚。
        """
        raw = parent_acb.spec.get("child_alloc")
        if isinstance(raw, bool) or not isinstance(raw, int) or raw <= 0:
            raw = max(self.state.max_fanout, 1) * self.state.min_budget_per_child
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
        getter = getattr(self.deps.checkpoints, "latest_ref_for_acb", None)
        if callable(getter):
            try:
                ref = getter(child_id)
            except Exception:  # noqa: BLE001
                ref = None
            if ref:
                return ref
        info = info if isinstance(info, dict) else {}
        getter = getattr(self.deps.checkpoints, "latest_ref_for_task", None)
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
            node = self.deps.budget._nodes.get(node_id)
        if node is None:
            for candidate in list(self.deps.budget._nodes.values()):
                if getattr(candidate, "owner", "") == child_id:
                    node = candidate
                    break
        reclaimed = False
        if node is not None:
            with self.deps.budget.suppressed():
                for sub in list(self.deps.budget._subtree_nodes(node)):
                    for rid, resv in list(sub.reservations.items()):
                        if resv.state == "ACTIVE":
                            self.deps.budget.settle(rid)
                if not self.deps.budget.archive_node(node.node_id):
                    self.deps.budget.remove_node(node.node_id)
            self.deps.budget.flush_pending()
            reclaimed = True
        self.deps.slots.release(child_id)
        # 注：`child_alloc_used` 只在 phase ③ 成功后记账，孤儿从未计入 → 不回滚。
        self.deps.event_log.append(
            EventKind.SPAWN_ORPHAN_RECLAIMED, parent_acb.id,
            {"child_id": child_id,
             "node_id": node.node_id if node is not None else None,
             "task_id": info.get("task_id"),
             "reason": "SPAWN_ORPHAN",
             "reclaimed": reclaimed},
            producer=parent_acb.id, run_id=parent_acb.run_id)
        self.state.drain_skipped.append((child_id, "spawn_orphan_reclaimed"))
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
            self.deps.safe_save(acb)

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
                    data = self.deps.checkpoints.load_strict(ref)
                except Exception:  # noqa: BLE001
                    try:
                        data = self.deps.checkpoints.load(ref)
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
            self.deps.safe_save(acb)

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
        task_id = spec.get("task_id") or f"C{next(self.state.spawn_counter)}"
        child_id = spec.get("child_id") or \
            f"A-{run_id}-{task_id}-spawn-{next(self.state.spawn_counter)}"
        min_budget = spec.get("min_budget")
        if isinstance(min_budget, bool) or not isinstance(min_budget, int) \
                or min_budget < 0:
            return self._spawn_reject(parent_acb, child_id, run_id, task_id,
                                      "INVALID_INPUT")

        with self.deps.transition_lock(parent_acb):
            if parent_acb.state in TASK_TERMINAL:
                return self._spawn_reject(parent_acb, child_id, run_id, task_id,
                                          "SPAWN_PARENT_TERMINAL")
            if child_id in self.state.acbs or child_id in {
                    str(e[0]) for e in parent_acb.children_ids if e}:
                return self._spawn_reject(parent_acb, child_id, run_id, task_id,
                                          "SPAWN_DUPLICATE_CHILD")
            if child_id in (parent_acb.children_pending or {}):
                return self._spawn_reject(parent_acb, child_id, run_id, task_id,
                                          "SPAWN_DUPLICATE_CHILD")
            parent_depth = self._spawn_depth_of(parent_acb)
            if parent_depth + 1 > self.state.spawn_depth:
                return self._spawn_reject(parent_acb, child_id, run_id, task_id,
                                          "SPAWN_DEPTH_EXCEEDED")
            # C8：fanout 只计**未终态**子（含 ref=None 的 pending），终态子不占名额。
            live_ids = self._live_child_ids(parent_acb)
            if len(live_ids) >= self.state.max_fanout:
                return self._spawn_reject(parent_acb, child_id, run_id, task_id,
                                          "SPAWN_FANOUT_EXCEEDED")
            if min_budget < self.state.min_budget_per_child:
                return self._spawn_reject(parent_acb, child_id, run_id, task_id,
                                          "CHILD_BUDGET_TOO_SMALL")
            parent_node = parent_acb.budget_node
            if parent_node is None:
                return self._spawn_reject(parent_acb, child_id, run_id, task_id,
                                          "SPAWN_NO_BUDGET_NODE")
            if min_budget > parent_node.remaining:
                return self._spawn_reject(parent_acb, child_id, run_id, task_id,
                                          "CHILD_BUDGET_EXHAUSTED")
            parent_token = self.deps.effective_token(parent_acb)
            resolved_parent = self.deps.policy.resolve_token(parent_token)
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
            if self.deps.policy.token(parent_acb.id) is None and parent_token is not None:
                self.deps.policy._tokens[parent_acb.id] = parent_token

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
                with self.deps.budget.suppressed():
                    if not self.deps.slots.try_acquire(child_id):
                        failure = "SPAWN_NO_SLOT"
                    else:
                        slot = True
                        node = self.deps.budget.create_node(
                            f"B-{child_id}", slice_limits,
                            parent_id=parent_node.node_id, run_id=run_id,
                            # S21 K-97：子节点标签必须与**实际挂载点**一致，
                            # 否则子任务的预留不计入租户计量。
                            tenant=_tenant_of_node_id(parent_node.node_id))
                        if node is None:
                            failure = "SPAWN_BUDGET_UNAVAILABLE"
                        else:
                            node.owner = child_id
                            resv = self.deps.budget.reserve(node.node_id, min_budget)
                            if resv is None:
                                failure = "CHILD_BUDGET_EXHAUSTED"
                    if failure is not None:
                        raise _SpawnAbort(failure)
                self.deps.budget.flush_pending()
            except BaseException as exc:  # CrashInjected 必须回滚后重抛
                with self.deps.budget.suppressed():
                    self._spawn_rollback(child_id, node, resv, slot)
                self.deps.budget.discard_pending()
                parent_acb.children_pending.pop(child_id, None)
                self.deps.safe_save(parent_acb)
                if isinstance(exc, _SpawnAbort):
                    return self._spawn_reject(parent_acb, child_id, run_id,
                                              task_id, str(exc))
                raise

            child.reservation_ids = [resv.reservation_id]
            child.budget_node = node
            child.state = TaskState.ADMITTED
            self.state.acbs[child.id] = child
            self.deps.run_coordinator.register(child)
            self.deps.register_admission(child)
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
            self.deps.safe_save(child)
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
            child = self.state.acbs.get(cid)
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
            data = self.deps.checkpoints.load_strict(ref)
        except Exception:  # noqa: BLE001
            try:
                data = self.deps.checkpoints.load(ref)
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
        acb.budget_node = self.deps.budget._nodes.get(data.get("budget_node_id"))
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
            child = self.state.acbs.get(cid)
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
                    data = (self.deps.checkpoints.load_strict(child.result_ref)
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
            self.deps.finalize(parent_acb)
        return env

    # ---- S3.2：跨服务公开接缝 ---------------------------------------
    # `TaskService`（replan / requeue_readmit）需要在父树中替换子身份。跨服务
    # 只走**公开**名，不去碰本类的 `_` 私有（实现仍是同一份，无第二套逻辑）。
    def replace_child_ref(self, old: ACB, fresh: ACB) -> None:
        """`_replace_child_ref` 的公开别名（S3.2：TaskService 的唯一入口）。"""
        self._replace_child_ref(old, fresh)

    # ---- X6：跨 run 子索引 -----------------------------------------
    def _index_child(self, child_id: str, parent_id: str) -> None:
        if parent_id and str(child_id) != str(parent_id):
            self.state.children_index.setdefault(str(parent_id), set()).add(str(child_id))

    def _ensure_children_index(self) -> None:
        """按 `parent_id` / `spec.spawn.parent_id` 反扫全部快照，建父→子索引。

        只在第一次取消级联/恢复子时构建一次；`spawn_child` 与 `_restore_acb`
        会增量更新，所以构建后仍能覆盖本进程新产生的子。构建失败不抛（尽力）。
        """
        if self.state.children_index_built:
            return
        self.state.children_index_built = True
        getter = getattr(self.deps.checkpoints, "refs_all", None)
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
                data = self.deps.checkpoints.load(ref)
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

        parent = self.state.acbs.get(parent_id)
        if parent is None:
            # 父不在内存 → 在**最新快照**上原地改 children_ids（不重建 ACB，
            # 否则会丢掉 staged / 预算 / 能力等字段）。
            updater = getattr(self.deps.checkpoints, "update_children", None)
            if callable(updater):
                # S14 K-68：条件写 —— 用**本次读到的**父 ref 作为期望值，
                # 避免两个并发 replan 互相覆盖（后写者丢更新）。
                expect = self.deps.checkpoints.latest_ref_for_acb(parent_id)
                try:
                    updater(parent_id, _patch, expect_ref=expect)
                except FencingRejected:
                    # CAS 冲突耗尽重试：显式记录（不静默丢更新），父的下次
                    # 汇总/取消会由 `_index_child` 的反扫索引兜底。
                    self.state.drain_skipped.append(
                        (parent_id, "child_ref_cas_conflict"))
            return
        parent.children = [fresh if child is old else child
                           for child in list(getattr(parent, "children", []) or [])]
        parent.children_ids = _patch(parent.children_ids)
        pending = getattr(parent, "children_pending", None) or {}
        if str(old.id) in pending:
            parent.children_pending[str(fresh.id)] = pending.pop(str(old.id))
        self.deps.safe_save(parent)
