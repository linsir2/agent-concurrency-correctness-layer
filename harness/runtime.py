"""AgentRuntime —— 文档 §5.3（运行数据流）。

- 每 turn 一次 `guard`，每个 action 前也检查（per-action guard，v1.5）
- `REQUEST_HITL` 挂起前 `discard_staged` + `release_all_locks`
- `resume` 值三条去向（v1.10 修复 P0-4）
- `commit_batch` 失败 → 语义 rebase（3-way merge → 失败回喂 LLM）
"""

from __future__ import annotations

import copy

from .types import ACB, Action, Decision, EventKind, Mode, Status, TaskState, Write


class AgentRuntime:
    def __init__(self, harness) -> None:
        self.h = harness

    # ---- 主循环 ---------------------------------------------------
    def run(self, acb: ACB, decisions: list[Decision],
            crash_at: tuple[int, int] | None = None) -> Status:
        """执行一个 agent 的计划。

        `decisions` 由 Planner/模型提供；`crash_at=(turn, action)` 用于测试崩溃恢复。
        """
        acb.state = TaskState.RUNNING
        for decision in decisions:
            acb.turn_seq = decision.turn_seq
            self.h.checkpoints.save(acb)

            if not self.guard(acb):
                acb.state = TaskState.FAILED
                acb.failure = "GUARD_VIOLATION"
                self.h.checkpoints.save(acb)
                return Status.GUARD_VIOLATION

            for i, action in enumerate(decision.actions):
                if crash_at and crash_at == (acb.turn_seq, i):
                    # 模拟「工具已调用但副作用未确认」→ effect 停在 UNKNOWN（文档 §5.8⑤）
                    if action.kind == "tool":
                        key = self.h.idem_key(acb, action, i)
                        self.h.effects.begin(acb, action.tool, action.args, key)
                        self.h.effects.mark_unknown(key, acb.id)
                    self.h.supervisor_note_crash(acb)
                    raise CrashInjected(acb.turn_seq, i)

                if i % self.h.guard_every == 0 and not self.guard(acb):
                    acb.state = TaskState.FAILED
                    acb.failure = "GUARD_VIOLATION"
                    self.h.checkpoints.save(acb)
                    return Status.GUARD_VIOLATION

                st = self._do_action(acb, action, i)
                if st in (Status.CANCELLED, Status.APPROVAL_TIMEOUT):
                    return st
                if st == Status.NEEDS_APPROVAL:
                    continue
                if st not in (Status.OK, Status.VALID):
                    acb.state = TaskState.FAILED
                    acb.failure = st.value
                    self.h.checkpoints.save(acb)
                    return st

            self._commit_staged(acb)
            self.h.checkpoints.save(acb)
            acb.step += 1

        acb.state = TaskState.DONE
        self.h.event_log.append(EventKind.TASK_COMPLETED, acb.task_id, {}, producer=acb.id)
        self.h.checkpoints.save(acb)
        return Status.OK

    # ---- 单动作 ---------------------------------------------------
    def _do_action(self, acb: ACB, action: Action, i: int) -> Status:
        if action.kind == "stage":
            self.h.workspace.stage(acb, action.resource, action.content)
            acb.subscriptions.add(action.resource)
            self.h.subscriptions.add(acb.id, action.resource)   # 读即订阅（文档 §5.6）
            return Status.OK

        if action.kind == "commit":
            for w in action.writes:               # 未暂存的资源不得提交
                if w.resource not in acb.staged:
                    return Status.INVALID_INPUT
            writes = [Write(resource=w.resource, content=acb.staged.get(w.resource))
                      for w in action.writes]
            st = self.h.commit_batch(acb, writes, Mode.COMMIT)
            if st == Status.OK:
                for w in action.writes:           # 已提交的从 staged 移除（避免重复提交）
                    acb.staged.pop(w.resource, None)
                    new_ver = self.h.workspace.version(w.resource)   # 刷新为写入后的新版本
                    acb.read_set[w.resource] = new_ver
                    acb.base_version[w.resource] = new_ver
            return st

        if action.kind == "tool":
            # B4：能力强制点在 gateway 强制执行，模型不可绕过
            if not self.h.policy.check_capability(acb.id, action.tool, action.resource or ""):
                return Status.DENIED_CAPABILITY
            idem_key = self.h.idem_key(acb, action, i)
            self.h.effects.begin(acb, action.tool, action.args, idem_key)
            prev = self.h.effects.state(idem_key)
            if prev == "CONFIRMED":
                return Status.OK
            if prev == "UNKNOWN":
                self.h.effects.mark_unknown(idem_key, acb.id)
            self.h.effects.confirm(idem_key, acb.id)
            self.h.event_log.append(EventKind.TOOL_COMPLETED, idem_key,
                                    {"tool": action.tool}, producer=acb.id,
                                    idempotency_key=idem_key)
            if not self.h.budget.consume(acb.budget_node.node_id, action.cost):
                acb.state = TaskState.FAILED
                acb.failure = "BUDGET_EXHAUSTED"
                return Status.BUDGET_EXHAUSTED
            return Status.OK

        if action.kind == "request_hitl":
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
                return Status.APPROVAL_TIMEOUT
            acb.hitl_result = resolved
            acb.state = TaskState.RUNNING
            self.h.event_log.append(EventKind.APPROVAL_RESOLVED, acb.task_id, {},
                                    producer=acb.id)
            return Status.NEEDS_APPROVAL

        if action.kind == "finish":
            return Status.OK

        return Status.INVALID_INPUT

    # ---- guard（文档 §5.3 / §4.4） -------------------------------
    def guard(self, acb: ACB) -> bool:
        """五查：取消 / 期限 / 预算 / 步数 / 无进展；并续租。"""
        if self.h.run_coordinator.is_cancelled(acb):
            acb.state = TaskState.CANCELLED
            return False
        self.h.intents.renew_for(acb)
        # 预算 guard：要求「还能再消费至少 1 单位」（否则恒真，形同空操作）
        if not self.h.budget.check(acb.budget_node.node_id, 1):
            acb.failure = "BUDGET_EXHAUSTED"
            return False
        if acb.deadline is not None and acb.step >= acb.deadline:
            acb.failure = "DEADLINE_EXCEEDED"
            return False
        if acb.step > self.h.max_steps:
            return False
        if acb.no_progress_steps > self.h.aging_max:
            acb.failure = "NO_PROGRESS"
            return False
        return True

    # ---- 提交 + 补偿（文档 §5.4） --------------------------------
    def _commit_staged(self, acb: ACB) -> Status:
        if not acb.staged:
            return Status.OK
        writes = [Write(resource=r, content=c) for r, c in acb.staged.items()]
        st = self.h.commit_batch(acb, writes, Mode.COMMIT)
        if st == Status.OK:
            acb.staged.clear()
        return st


class CrashInjected(Exception):
    def __init__(self, turn_seq: int, action_index: int) -> None:
        super().__init__(f"crash at turn={turn_seq} action={action_index}")
        self.turn_seq = turn_seq
        self.action_index = action_index
