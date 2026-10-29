"""测试公共工具：构建场景的辅助函数。

对应 `docs/use-cases-and-adversarial-tests.md` 的 17 个用例。
"""

from __future__ import annotations

import itertools
import os
import sys
import uuid

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from harness import ACB, Action, Decision, Harness, TaskState, Write
from harness.runtime import CrashInjected


def pytest_collection_modifyitems(config, items):
    """`chaos` 用例默认跳过；显式 `RSC_CHAOS=1` 才跑完整故障矩阵。"""
    if os.environ.get("RSC_CHAOS") == "1":
        return
    import pytest

    skip = pytest.mark.skip(reason="完整混沌矩阵需 RSC_CHAOS=1")
    for item in items:
        if "chaos" in item.keywords:
            item.add_marker(skip)


def make_harness(run_limits: int = 1000, *, clock=None, blobs=None, probe=None,
                 toolgw=None, compensate_crash_at=None,
                 auth_secret: str | None = None,
                 auth_gc_every: int = 256, gc_every: int = 1000,
                 tombstone_ttl_s: float = 86400.0,
                 hitl_timeout_ms: int = 30_000,
                 max_concurrent: int = 100,
                 checkpoints=None, checkpoint_root=None,
                 poison_after_failures: int = 3,
                 drain_timeout_s: float = 5.0,
                 lease_ms: int = 60_000,
                 lease_store=None, definitions=None, timers=None,
                 durable_timers: bool = False) -> Harness:
    """默认 Harness + 通配能力 token（D13：deny-by-default 下的测试夹具）。

    安全用例必须用**不装 default_token** 的 `Harness()` 自行断言拒绝。
    `clock`/`blobs` 透传（MS1：注入时钟构造租约过期、共享 blob 目录）。

    MS3 注入参数（§7）：
    - `probe`：注入 `EffectJournal._probe`（三态反查）；
    - `toolgw`：挂到 `h.toolgw`（工具网关占位；当前 runtime 内联执行）；
    - `compensate_crash_at`：补偿第 N 条后抛 `CrashInjected`（真实崩溃注入）；
    - `auth_secret`：注入 HMAC 签名密钥（缺失 → `MissingSecret`）。

    S8 覆盖率参数（§4.4）：
    - `auth_gc_every` / `gc_every`：周期清理钩子触发频率；
    - `tombstone_ttl_s`：预算归档 tombstone 的 TTL（构造过期回收）。
    """
    h = Harness(run_limits=run_limits, clock=clock, blobs=blobs,
                auth_secret=auth_secret, auth_gc_every=auth_gc_every,
                gc_every=gc_every, tombstone_ttl_s=tombstone_ttl_s,
                hitl_timeout_ms=hitl_timeout_ms,
                max_concurrent=max_concurrent,
                checkpoints=checkpoints, checkpoint_root=checkpoint_root,
                poison_after_failures=poison_after_failures,
                drain_timeout_s=drain_timeout_s,
                lease_ms=lease_ms, lease_store=lease_store,
                definitions=definitions, timers=timers,
                durable_timers=durable_timers)
    if probe is not None:
        h.effects._probe = probe
    if toolgw is not None:
        h.toolgw = toolgw
    if compensate_crash_at is not None:
        _orig_run_one = h.commit._run_one_compensation
        _counter = itertools.count(1)

        def _crashy(acb, e, w=None, _orig=_orig_run_one, _n=compensate_crash_at):
            if next(_counter) > _n:
                raise CrashInjected(0, 0)
            return _orig(acb, e, w)

        # S2：补偿已迁到 CommitService，打桩必须打在服务的可替换接缝上
        h.commit.run_one_hook = _crashy
    h.policy.default_token = h.policy.issue(
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


def seed(h: Harness, resource: str, content) -> int:
    """写入种子并返回 put 后的版本（D-4：put 推进版本，首次 put 返回 1）。"""
    return h.workspace.put(resource, content).version


def _acb(h: Harness, resource: str) -> ACB:
    """构造一个绑定预算节点的 ACB（MS1 测试用）。

    节点 id 与 ACB id 都必须唯一（`B-{task_id}` 会与同 task 重试撞旧节点）；
    建节点后立刻预留下全额，使 ACB 的 `reservation_ids` 与 `budget_node`
    同生产路径一致（`_finalize`/`escalate` 都按预留结算）。
    """
    acb = ACB(id=f"A-{resource}-{uuid.uuid4().hex[:8]}", run_id="R", task_id="T")
    node = h.budget.create_node(f"B-{acb.id}", 100)
    node.owner = acb.id
    acb.budget_node = node
    resv = h.budget.reserve(node.node_id, node.limits)
    if resv is not None:
        acb.reservation_ids = [resv.reservation_id]
    return acb


def state_of(h: Harness, run_id: str, task_id: str) -> TaskState:
    return h.plans[run_id]["tasks"][task_id]["state"]


def pytest_terminal_summary(terminalreporter, exitstatus, config) -> None:
    """S11 egress 环境 invalid **显式登记**（禁止静默 skip，见 `_egress_env.py`）。"""
    try:
        from _egress_env import report_lines
    except ImportError:                      # pragma: no cover - 收集期异常兜底
        return
    for line in report_lines():
        terminalreporter.write_line(line)
