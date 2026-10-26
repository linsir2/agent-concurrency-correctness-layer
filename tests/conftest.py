"""测试公共工具：构建场景的辅助函数。

对应 `docs/use-cases-and-adversarial-tests.md` 的 17 个用例。
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from harness import Action, CapabilityToken, Decision, Harness, TaskState, Write  # noqa: E402


def make_harness(run_limits: int = 1000) -> Harness:
    """默认 Harness + 通配能力 token（D13：deny-by-default 下的测试夹具）。

    安全用例必须用**不装 default_token** 的 `Harness()` 自行断言拒绝。
    """
    h = Harness(run_limits=run_limits)
    h.policy.default_token = CapabilityToken(
        "*", tools={"*"}, paths={"*"}, network={"*"}, data_class="*")
    return h


def stage(resource: str, content) -> Action:
    return Action(kind="stage", resource=resource, content=content)


def commit(*resources: str) -> Action:
    return Action(kind="commit", writes=[Write(resource=r) for r in resources])


def tool(name: str, args=None, cost: int = 1) -> Action:
    return Action(kind="tool", tool=name, args=args or {}, cost=cost)


def hitl() -> Action:
    return Action(kind="request_hitl")


def finish() -> Action:
    return Action(kind="finish")


def decision(turn_seq: int, *actions: Action) -> Decision:
    return Decision(turn_seq=turn_seq, actions=list(actions))


def admit(h: Harness, run_id: str, plan: dict, task_id: str):
    task = plan["tasks"][task_id]
    acb = h.dispatcher.try_admit(task, run_id)
    h.acbs[acb.id] = acb
    h.run_coordinator.register(acb)
    return acb


def seed(h: Harness, resource: str, content) -> None:
    h.workspace.put(resource, content)


def state_of(h: Harness, run_id: str, task_id: str) -> TaskState:
    return h.plans[run_id]["tasks"][task_id]["state"]
