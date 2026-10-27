"""AgentRuntime —— 文档 §5.3（运行数据流）。

- 每 turn 一次 `guard`，每个 action 前也检查（per-action guard，v1.5）
- `REQUEST_HITL` 挂起前 `discard_staged` + `release_all_locks`
- `resume` 值三条去向（v1.10 修复 P0-4）
- `commit_batch` 失败 → 语义 rebase（3-way merge → 失败回喂 LLM）
"""

from __future__ import annotations

import copy
from collections.abc import Callable
from typing import Any

from .rebase import (MISSING, MergeResult, _kind, _strict_eq, merge3)
from .types import (
    ACB,
    Action,
    Decision,
    EventKind,
    Mode,
    Result,
    Status,
    TaskState,
    Write,
    advance,
    ok,
)


_HANDLERS: dict[str, Callable] = {}


def register_action(kind: str):
    """动作注册表（§2.1）：精确 key 命中，重复注册抛 `ValueError`。"""
    def deco(fn):
        if kind in _HANDLERS:
            raise ValueError(f"duplicate action handler: {kind}")
        _HANDLERS[kind] = fn
        return fn
    return deco


class AgentRuntime:
    def __init__(self, harness) -> None:
        self.h = harness

    # ---- 主循环 ---------------------------------------------------
    def run(self, acb: ACB, decisions: list[Decision],
            crash_at: tuple[int, int] | None = None) -> Status:
        """执行一个 agent 的计划。

        `decisions` 由 Planner/模型提供；`crash_at=(turn, action)` 用于测试崩溃恢复。
        """
        if acb.state is TaskState.QUARANTINED:
            # 已隔离：不得覆写为 FAILED（文档 §5.5）
            return Status.SEMANTIC_REJECTED
        acb.state = TaskState.RUNNING
        for decision in decisions:
            acb.turn_seq = decision.turn_seq
            acb.cursor = self.h.event_log.tail()          # §2.3：save 前对齐全局水位
            acb.heartbeat_at = self.h.clock.now_ms()      # D13b：心跳推进（不再只恢复）
            self.h.checkpoints.save(acb)

            guard_ok, reason = self.guard(acb)
            if not guard_ok:
                acb.state = TaskState.FAILED
                acb.failure = reason or "GUARD_VIOLATION"
                self.h.checkpoints.save(acb)
                return Status.GUARD_VIOLATION

            progressed = False
            for i, action in enumerate(decision.actions):
                if crash_at and crash_at == (acb.turn_seq, i):
                    # 模拟「工具已调用但副作用未确认」→ effect 停在 UNKNOWN（文档 §5.8⑤）
                    if action.kind == "tool":
                        key = self.h.idem_key(acb, action, i)
                        self.h.effects.begin(acb, action)
                        self.h.effects.mark_unknown(key, acb.id)
                    self.h.supervisor_note_crash(acb)
                    raise CrashInjected(acb.turn_seq, i)

                if i % self.h.guard_every == 0:
                    guard_ok, reason = self.guard(acb)
                    if not guard_ok:
                        acb.state = TaskState.FAILED
                        acb.failure = reason or "GUARD_VIOLATION"
                        self.h.checkpoints.save(acb)
                        return Status.GUARD_VIOLATION

                st = self._do_action(acb, action, i)
                if st in (Status.OK, Status.VALID):      # D13b：本 turn 有进展
                    progressed = True
                if st in (Status.CANCELLED, Status.APPROVAL_TIMEOUT):
                    return st
                if st == Status.NEEDS_APPROVAL:
                    continue
                if st == Status.NEEDS_RECONCILIATION:
                    # §4.3：probe 三态中 `None` → 升级人工（显式分支，不走通用 FAILED）
                    self._quarantine(acb, "NEEDS_RECONCILIATION")
                    return st
                if st not in (Status.OK, Status.VALID):
                    # 冲突类失败走语义 rebase 回路（§5.1）；其余失败交上层分类
                    if isinstance(st, Result) and st.status in (
                            Status.CONFLICT, Status.REJECT_STALE, Status.REJECT_STALE_READ):
                        st = self.handle_commit_result(acb, st)
                    if acb.state is TaskState.QUARANTINED:
                        # 已隔离：直接返回，不改状态、不覆盖 failure（§5.5）
                        return st
                    if st not in (Status.OK, Status.VALID):
                        acb.state = TaskState.FAILED
                        acb.failure = st.value
                        self.h.checkpoints.save(acb)
                        return st

            st = self._commit_staged(acb)             # 返回值必须被使用（B11）
            if st != Status.OK:
                st = self.handle_commit_result(acb, st)
            if acb.state is TaskState.QUARANTINED:
                return st
            if st != Status.OK:
                acb.state = TaskState.FAILED
                acb.failure = st.value
                self.h.checkpoints.save(acb)
                return st
            self.h.checkpoints.save(acb)
            acb.step += 1
            if progressed:
                acb.no_progress_steps = 0
            else:
                acb.no_progress_steps += 1            # D13b：本 turn 无进展 → 自增
                if acb.no_progress_steps >= self.h.aging_max:
                    acb.aging_deadline = self.h.clock.now_ms()   # D13b：到达 aging_max 设定期限

        acb.state = TaskState.DONE
        self.h.event_log.append(EventKind.TASK_COMPLETED, acb.task_id, {}, producer=acb.id)
        self.h.checkpoints.save(acb)
        return Status.OK

    # ---- 单动作 ---------------------------------------------------
    def _effect_id(self, idem_key: str) -> str:
        entry = self.h.effects.get(idem_key)
        return entry.effect_id if entry is not None else idem_key

    def _gateway_check(self, acb: ACB, action: Action) -> bool:
        """§5.6（K-11）：gateway 强制点，读 token 的 network / data_class。

        - 无 token → 拒绝（deny-by-default）；
        - `action` 无 `network` / `data_class` 属性 → 放行（不引入额外约束）。
        """
        token = self.h._effective_token(acb)
        if token is None:
            return False
        network = getattr(action, "network", None)
        if network and not self.h.policy.allows_network(token, network):
            return False
        data_class = getattr(action, "data_class", None)
        if data_class and not self.h.policy.allows_data_class(token, data_class):
            return False
        return True

    def _do_action(self, acb: ACB, action: Action, i: int) -> Result:
        """§2.1：查表派发 + 归一化（裸 `Status` → `Result`）+ 异常映射。

        D1：handler 返回值三分支归一化 —— `Result` 原样、`Status` 包装、
        其他（含 `None`）→ `UPSTREAM_FAILED(bad_return)`，避免 `Result(None)`
        在 `run()` 里 `st.value` 崩溃。
        """
        fn = _HANDLERS.get(action.kind)
        if fn is None:
            return Result(Status.INVALID_INPUT, {"reason": "unknown_action"})
        try:
            out = fn(self, acb, action, i)
        except Exception as e:  # noqa: BLE001  # §2.1：异常映射到 Result，不穿透
            return Result(Status.UPSTREAM_FAILED, {"reason": type(e).__name__})
        if isinstance(out, Result):
            return out
        if isinstance(out, Status):
            return Result(out)
        return Result(Status.UPSTREAM_FAILED, {"reason": "bad_return"})

    # ---- 动作处理器（§2.1，逐个对应原 if/elif 分支）-----------------
    @register_action("stage")
    def _action_stage(self, acb: ACB, action: Action, i: int) -> Result:
        self.h.workspace.stage(acb, action.resource, action.content)
        acb.subscriptions.add(action.resource)
        self.h.subscriptions.add(acb.id, action.resource)   # 读即订阅（文档 §5.6）
        return ok()

    @register_action("commit")
    def _action_commit(self, acb: ACB, action: Action, i: int) -> Result:
        for w in action.writes:               # 未暂存的资源不得提交
            if w.resource not in acb.staged:
                return Result(Status.INVALID_INPUT, {})
        writes = [Write(resource=w.resource, content=acb.staged.get(w.resource))
                  for w in action.writes]
        st = self.h.commit_batch(acb, writes, Mode.COMMIT)
        if st == Status.OK:
            for w in action.writes:           # 已提交的从 staged 移除（避免重复提交）
                acb.staged.pop(w.resource, None)
                new_ver = self.h.workspace.version(w.resource)   # 刷新为写入后的新版本
                acb.read_set[w.resource] = new_ver
                acb.base_version[w.resource] = new_ver
            acb.last_committed_seq = self.h.event_log.tail()   # D13：显式提交也推进水位
        return st

    @register_action("tool")
    def _action_tool(self, acb: ACB, action: Action, i: int) -> Result:
        # B4：能力强制点在 gateway 强制执行，模型不可绕过（§4.1 / §5.9）
        token = self.h._effective_token(acb)
        if not self.h.policy.check_capability(
                acb.id, token, action.tool, action.resource or ""):
            return Result(Status.DENIED_CAPABILITY, {"tool": action.tool})
        if not self._gateway_check(acb, action):
            return Result(Status.DENIED_CAPABILITY, {"reason": "gateway"})
        idem_key = self.h.idem_key(acb, action, i)
        st = self.h.effects.state(idem_key)
        if st == "CONFIRMED":
            # I7：副作用恰好一次；只有 CONFIRMED 可短路
            return Result(Status.OK, {"effect_id": self._effect_id(idem_key)})
        if st in ("INTENT", "UNKNOWN"):
            # §4.1：INTENT 与 UNKNOWN 同路径 —— probe 三态
            probe = self.h.effects.probe_one(idem_key)
            if probe == "True":
                self.h.effects.confirm(idem_key, {"recovered": True})
                return Result(Status.OK, {"effect_id": self._effect_id(idem_key)})
            if probe == "None":
                return Result(Status.NEEDS_RECONCILIATION, {"idem_key": idem_key})
            # probe == "False" → 落到下面重执行（同一 idem）
        # 预算消费先于副作用：消费失败则不执行工具、不写事件
        st = self._consume_budget(acb, action.cost)
        if st != Status.OK:
            if st == Status.BUDGET_EXHAUSTED:
                acb.state = TaskState.FAILED
                acb.failure = "BUDGET_EXHAUSTED"
            return Result(st)
        self.h.effects.begin(acb, action)
        self.h.effects.confirm(idem_key, acb.id)
        self.h.event_log.append(EventKind.TOOL_COMPLETED, idem_key,
                                {"tool": action.tool}, producer=acb.id,
                                idempotency_key=idem_key)
        return Result(Status.OK, {"effect_id": idem_key})

    @register_action("request_hitl")
    def _action_request_hitl(self, acb: ACB, action: Action, i: int) -> Result:
        acb.staged.clear()
        self.h.intents.revoke_all(acb.id)
        acb.state = TaskState.WAITING_APPROVAL
        self.h.checkpoints.save(acb)
        self.h.event_log.append(EventKind.APPROVAL_RAISED, acb.task_id, {},
                                producer=acb.id)
        resolved = self.h.hitl.resolve(acb)
        if resolved is None:
            acb.state = TaskState.FAILED
            acb.failure = "APPROVAL_TIMEOUT"
            return Result(Status.APPROVAL_TIMEOUT, {})
        acb.hitl_result = resolved
        acb.state = TaskState.RUNNING
        self.h.event_log.append(EventKind.APPROVAL_RESOLVED, acb.task_id, {},
                                producer=acb.id)
        return Result(Status.NEEDS_APPROVAL, {})

    @register_action("finish")
    def _action_finish(self, acb: ACB, action: Action, i: int) -> Result:
        return ok()

    # ---- guard（文档 §5.3 / §4.4） -------------------------------
    def _guard_checks(self, acb: ACB) -> list[Callable[[], str | None]]:
        """§2.2：谓词列表（顺序即优先级）。每个谓词返回 `None` 或失败 reason。"""
        return [
            lambda: "CANCELLED" if self.h.run_coordinator.is_cancelled(acb) else None,
            lambda: self._renew_or_fail(acb),
            lambda: ("BUDGET_EXHAUSTED"
                     if not self.h.budget.check_reservations(acb.reservation_ids, 1)
                     else None),
            lambda: ("DEADLINE_EXCEEDED"
                     if acb.deadline is not None and acb.step >= acb.deadline
                     else None),
            lambda: "MAX_STEPS" if acb.step > self.h.max_steps else None,
            lambda: "NO_PROGRESS" if acb.no_progress_steps > self.h.aging_max else None,
        ]

    def _renew_or_fail(self, acb: ACB) -> str | None:
        """§2.2 续租三态：

        - 无有效租约（如已提交 revoke）→ `None`（**不判失败**，避免提交后误判）；
        - 有有效租约但 `renew_for` 失败 → `"LEASE_EXPIRED"`。
        """
        live = [ls for leases in self.h.intents._active.values()
                for ls in leases if ls.agent_id == acb.id and ls.valid]
        if not live:
            return None
        return None if self.h.intents.renew_for(acb) else "LEASE_EXPIRED"

    def guard(self, acb: ACB) -> tuple[bool, str | None]:
        """§2.2：谓词列表求值；reason 非 `CANCELLED` 时落 `acb.failure`。"""
        for chk in self._guard_checks(acb):
            reason = chk()
            if reason:
                if reason != "CANCELLED":
                    acb.failure = reason
                return False, reason
        return True, None

    def _consume_budget(self, acb: ACB, amount: int) -> Status:
        """按预留消费：找到**第一个**可支付 `amount` 的预留。

        全部失败时：任一为 `BUDGET_EXHAUSTED` → 按耗尽处理；
        否则（unknown / settled 的 `INVALID_INPUT`）→ 返回 `INVALID_INPUT`。
        """
        exhausted = False
        for rid in list(acb.reservation_ids):
            result = self.h.budget.consume(rid, amount)
            if result.status == Status.OK:
                return Status.OK
            if result.status == Status.BUDGET_EXHAUSTED:
                exhausted = True
        return Status.BUDGET_EXHAUSTED if exhausted else Status.INVALID_INPUT

    # ---- 提交 + 补偿（文档 §5.4） --------------------------------
    def _commit_staged(self, acb: ACB) -> Result:
        if not acb.staged:
            return ok()
        writes = [Write(resource=r, content=c) for r, c in acb.staged.items()]
        st = self.h.commit_batch(acb, writes, Mode.COMMIT)
        if st == Status.OK:
            acb.staged.clear()
            acb.last_committed_seq = self.h.event_log.tail()   # D13：提交成功后真实写入
        return st

    # ---- 冲突处理回路（文档 §5） ----------------------------------
    def handle_commit_result(self, acb: ACB, result: Result) -> Result:
        """提交失败 → **有界**语义 rebase（§5.1）。

        - `REJECT_STALE`：租约失效 → 重新 reserve 重试一次（§5.4）；
        - `REJECT_STALE_READ`：读依赖被推进 → 重读刷新 read_set（不做 merge3）；
        - `CONFLICT`：取 pre_image → merge3 → 成功则 _accept 重试，否则 resolver；
        - 内部有界循环（≤ `retry_budget + 1` 次）直到 OK 或隔离，
          避免「第二次冲突直接 FAILED」丢写；
        - 每次尝试 `budget.consume(1)`（失败即隔离），超限 → `QUARANTINED`；
          成功 → `RUNNING`。
        """
        st = result.status
        if st == Status.REJECT_STALE:
            return self._retry_with_fresh_lease(acb, result)
        if st not in (Status.CONFLICT, Status.REJECT_STALE_READ):
            return result

        if not self._enter_rebasing(acb):
            acb.failure = "not_rebasable"
            return Result(Status.SEMANTIC_REJECTED, {"reason": "not_rebasable"})

        if not self._ensure_retry_budget(acb):
            self._quarantine(acb, "budget_exhausted")
            return Result(Status.SEMANTIC_REJECTED, {"reason": "budget_exhausted"})

        for _ in range(acb.retry_budget + 1):
            if st == Status.REJECT_STALE_READ:
                self._refresh_read_dep(acb, result.detail.get("resource"))
            else:
                outcome = self._rebase_write(acb, result)
                if outcome is not None:
                    return outcome

            acb.rebase_attempts += 1
            if self._consume_budget(acb, 1) != Status.OK:
                self._quarantine(acb, "budget_exhausted")
                return Result(Status.SEMANTIC_REJECTED, {"reason": "budget_exhausted"})
            if acb.rebase_attempts > acb.retry_budget:
                self._quarantine(acb, "rebase_budget_exhausted")
                return Result(Status.SEMANTIC_REJECTED, {"reason": "rebase_budget_exhausted"})

            retry = self._commit_staged(acb)          # 返回值必须被使用（B11）
            if retry == Status.OK:
                advance(acb, "patch_applied")        # REBASING → RUNNING
                return Result(Status.OK, {"rebased": True, "attempts": acb.rebase_attempts})
            if retry.status not in (Status.CONFLICT, Status.REJECT_STALE_READ):
                return retry
            # 第二次（及以后）冲突：不丢写，继续有界重试
            result = retry
            st = retry.status
            self._enter_rebasing(acb)
        return Result(Status.SEMANTIC_REJECTED, {"reason": "rebase_budget_exhausted"})

    def _enter_rebasing(self, acb: ACB) -> bool:
        """§5.2 状态归一化：仅允许 `RUNNING→REBASING` 或已在 `REBASING`。

        `QUARANTINED` 与终态一律拒绝（返回 `False`），不再强行置 REBASING
        （避免对已隔离/已结束任务做无意义 rebase 而丢写）。
        """
        if acb.state is TaskState.REBASING:
            return True
        if acb.state is TaskState.RUNNING:
            advance(acb, "verifier_failed")           # 表 A 的合法边
            return True
        return False

    def _ensure_retry_budget(self, acb: ACB) -> bool:
        """为重试补足预算：已有活跃预留余额不足时追加一笔并登记到 ACB。

        预留失败 → 调用方隔离。
        """
        node = acb.budget_node
        if node is None:
            return False
        needed = acb.retry_budget + 1
        available = 0
        for rid in acb.reservation_ids:
            resv = node.reservations.get(rid)
            if resv is not None and resv.state == "ACTIVE":
                available += resv.balance
        if available >= needed:
            return True
        resv = self.h.budget.reserve(node.node_id, needed - available)
        if resv is None:
            return False
        acb.reservation_ids.append(resv.reservation_id)
        return True

    def _rebase_write(self, acb: ACB, result: Result):
        """§5.3：以 pre_image 为 base 做 3-way merge；成功则 _accept，冲突交 resolver。"""
        resource = result.detail["resource"]
        base = self.h.workspace.pre_image(resource, acb.base_version.get(resource, 0))
        local = acb.staged.get(resource)
        committed = self.h.workspace.get(resource)
        if base is MISSING:                           # 前像缺失 → 不能自动合并
            self._quarantine(acb, "no_pre_image")
            return Result(Status.SEMANTIC_REJECTED, {"reason": "no_pre_image"})

        mr = merge3(base, local, committed, self.h.planner.merge_policy_of(resource))
        if mr.ok:
            self._accept(acb, resource, mr.merged, merge=False)
            return None

        ctx = {"attempt": acb.rebase_attempts + 1,
               "task": acb.task_id, "subgoal": acb.logical_subgoal_id,
               "script": acb.spec.get("script")}
        resolved = None
        if self.h.resolver is not None:
            resolved = self.h.resolver(acb, resource, mr.conflicts, ctx)
        if resolved is None or not _validate_resolution(resolved, mr, base, committed):
            # resolver 放弃 / 输出不合 schema → 计入一次尝试，由 retry_budget 隔离
            acb.failure = "resolver_failed"
            return None
        self._accept(acb, resource, resolved, merge=True)
        return None

    def _accept(self, acb: ACB, resource: str, content: Any, *, merge: bool = True) -> None:
        """接受合并结果：写 staged 并刷新 base_version 与 read_set（P0-9）。

        `merge=True`（resolver 输出）：深合并到 committed（保留非冲突内容）；
        `merge=False`（merge3 已成功）：整体替换（mr.merged 已权威）。
        """
        if merge:
            committed = self.h.workspace.get(resource)
            if isinstance(committed, dict) and isinstance(content, dict):
                content = _deep_merge(committed, content)
        acb.staged[resource] = content
        v = self.h.workspace.version(resource)
        acb.base_version[resource] = v
        acb.read_set[resource] = v

    def _refresh_read_dep(self, acb: ACB, resource: str | None = None) -> None:
        """§5.4：REJECT_STALE_READ → 一次刷新**所有**陈旧读依赖（不做 merge3）。

        同时刷新 `read_set` 与 `base_version`（否则 base 仍是旧值 → 再冲突）。
        """
        targets = list(acb.read_set)
        if resource is not None and resource not in targets:
            targets.append(resource)
        for r in targets:
            actual = self.h.workspace.version(r)
            if acb.read_set.get(r) != actual:
                acb.read_set[r] = actual
                acb.base_version[r] = actual

    def _retry_with_fresh_lease(self, acb: ACB, result: Result) -> Result:
        """§5.4：REJECT_STALE → 重新 reserve（不是 renew）后重试一次。

        D4：此处直接 reserve 的租约**不在** `commit_batch` 的 finally 撤销集合里，
        重试后必须显式 revoke，否则遗留 valid 租约。
        """
        resource = result.detail.get("resource")
        if resource is None:
            return result
        base = self.h.workspace._base_of(acb, resource)
        lease = self.h.intents.reserve(resource, acb.id, base, self.h.lease_ms)
        if lease is None:
            acb.state = TaskState.FAILED
            acb.failure = "RESERVE_FAILED"
            return result
        acb.leases.append(lease)
        try:
            retry = self._commit_staged(acb)
        finally:
            self.h.intents.revoke(lease)          # D4：重试提交后 revoke 直接预留的租约
        if retry == Status.OK:
            return Result(Status.OK, {"rebased": True, "attempts": acb.rebase_attempts})
        if retry == Status.REJECT_STALE:              # 再失败 → FAILED
            acb.state = TaskState.FAILED
            acb.failure = retry.status.value
        return retry

    def _quarantine(self, acb: ACB, reason: str) -> None:
        """隔离：REBASING --replan_budget_exhausted--> QUARANTINED（合法边，§5.5）。"""
        if acb.state is not TaskState.QUARANTINED:
            if acb.state is TaskState.REBASING:
                advance(acb, "replan_budget_exhausted")
            else:
                acb.state = TaskState.QUARANTINED
        acb.failure = reason
        self.h.checkpoints.save(acb)


def _deep_merge(base: Any, override: Any) -> Any:
    """深合并：`override` 覆盖 `base`（同为 dict 时递归），返回新对象。"""
    if isinstance(base, dict) and isinstance(override, dict):
        out = copy.deepcopy(base)
        for k, v in override.items():
            out[k] = _deep_merge(out[k], v) if k in out else copy.deepcopy(v)
        return out
    return copy.deepcopy(override)


def _validate_resolution(resolved: Any, mr: MergeResult, base: Any, committed: Any) -> bool:
    """§7.1 输出注入防护 + 语义完整性：resolver 必须给出**完整内容**。

    - 是 dict；
    - 键集合 == `committed` 顶层键 ∪ 冲突路径顶层键（不多不少）；
    - 非冲突键：值必须**严格等于** `committed`；
    - 冲突键：值类型必须与 `committed`（缺失时退到 `base`）同类。
    任一不满足 → `False`（→ 隔离）。
    """
    if not isinstance(resolved, dict):
        return False
    conflict_tops = {c.path.strip("/").split("/")[0] if c.path else ""
                     for c in mr.conflicts}
    if not isinstance(committed, dict):
        return False
    if set(resolved) != (set(committed) | conflict_tops):
        return False
    for key, value in resolved.items():
        if key in conflict_tops:
            ref = committed.get(key, MISSING)
            if ref is MISSING and isinstance(base, dict):
                ref = base.get(key, MISSING)
            if ref is not MISSING and _kind(value) != _kind(ref):
                return False
        else:
            if not _strict_eq(value, committed[key]):
                return False
    return True


class CrashInjected(Exception):
    def __init__(self, turn_seq: int, action_index: int) -> None:
        super().__init__(f"crash at turn={turn_seq} action={action_index}")
        self.turn_seq = turn_seq
        self.action_index = action_index
