"""S24-11：定义版本化（content hash 钉住，未知/异 hash 一律拒绝）。

RIPR：
- Reachability：plan → task → `acb.spec["definition"]` 真的落下引用；
- Infection：版本不匹配在**占预算/槽位之前**拒绝；热更新后提交被拒；
- Propagation：`commit_batch` 返回 DENIED 且共享态版本不变；
- Revealability：错误码可执行（UNKNOWN / HASH_MISMATCH / REGISTRY_MISSING）。
"""

from __future__ import annotations

import pytest
from conftest import make_harness

from harness.definitions import (
    DefinitionConflict,
    DefinitionRef,
    DefinitionRegistry,
)
from harness.types import Status, TaskState, Write

DEF = {"prompt": "write config", "tools": ["write"], "model": "m1"}


def _registry() -> tuple[DefinitionRegistry, DefinitionRef]:
    reg = DefinitionRegistry()
    return reg, reg.register(DEF, name="coder", version="1")


def _plan(h, ref: dict | None, *, run_id: str = "run-1"):
    task = {"task_id": "t", "logical_subgoal_id": "g", "min_budget": 1}
    if ref is not None:
        task["definition"] = ref
    return h.planner.plan(run_id, [task])


# ---------------------------------------------------------------- registry
def test_register_is_content_addressed():
    reg, ref = _registry()
    assert len(ref.content_hash) == 64
    assert reg.check(ref) == ""
    assert reg.check(ref.as_dict()) == ""
    assert reg.latest("coder") == ref
    assert reg.resolve(ref) == DEF


def test_same_version_different_content_conflicts():
    reg, _ref = _registry()
    with pytest.raises(DefinitionConflict):
        reg.register({"prompt": "different"}, name="coder", version="1")


def test_check_reports_unknown_and_mismatch():
    reg, ref = _registry()
    assert reg.check(DefinitionRef("coder", "9", ref.content_hash)) \
        == "DEFINITION_UNKNOWN"
    assert reg.check(DefinitionRef("coder", "1", "0" * 64)) \
        == "DEFINITION_HASH_MISMATCH"
    assert reg.check({"name": "coder"}) == "DEFINITION_MALFORMED"
    assert reg.check(None) == "DEFINITION_MALFORMED"


# ---------------------------------------------------------------- 准入
def test_plan_and_admit_pin_definition():
    reg, ref = _registry()
    h = make_harness(definitions=reg)
    plan = _plan(h, ref.as_dict())
    task = plan["tasks"]["t"]
    assert task["definition"] == ref.as_dict()
    acb = h.tasks.admit_and_register(plan, "t")
    assert acb.state is TaskState.ADMITTED
    assert acb.spec["definition"] == ref.as_dict()


def test_unknown_definition_rejected_before_budget():
    reg, ref = _registry()
    h = make_harness(definitions=reg)
    bad = DefinitionRef("coder", "9", ref.content_hash).as_dict()
    task = {"task_id": "t", "logical_subgoal_id": "g", "min_budget": 1,
            "definition": bad}
    plan = {"run_id": "run-1", "tasks": {"t": task}}
    acb = h.dispatcher.try_admit(task, "run-1", plan=plan)
    assert acb.state is TaskState.INADMISSIBLE
    assert acb.failure == "DEFINITION_UNKNOWN"
    assert h.budget._reservations == {}          # 零残留
    assert acb.budget_node is None


def test_plan_with_unknown_definition_raises():
    reg, ref = _registry()
    h = make_harness(definitions=reg)
    bad = DefinitionRef("coder", "9", ref.content_hash).as_dict()
    with pytest.raises(ValueError):
        _plan(h, bad)


def test_missing_registry_is_fail_closed():
    h = make_harness()                            # 无 definitions
    ref = DefinitionRef("coder", "1", "0" * 64).as_dict()
    task = {"task_id": "t", "logical_subgoal_id": "g", "min_budget": 1,
            "definition": ref}
    plan = {"run_id": "run-1", "tasks": {"t": task}}
    acb = h.dispatcher.try_admit(task, "run-1", plan=plan)
    assert acb.state is TaskState.INADMISSIBLE
    assert acb.failure == "DEFINITION_REGISTRY_MISSING"


# ---------------------------------------------------------------- 提交
def test_commit_succeeds_with_pinned_definition():
    reg, ref = _registry()
    h = make_harness(definitions=reg)
    plan = _plan(h, ref.as_dict())
    acb = h.tasks.admit_and_register(plan, "t")
    res = h.commit_batch(acb, [Write("a.md", "v1")])
    assert res.status is Status.OK
    assert h.workspace.version("a.md") == 1


def test_hot_definition_change_blocks_old_acb_commit():
    reg, ref = _registry()
    h = make_harness(definitions=reg)
    plan = _plan(h, ref.as_dict())
    acb = h.tasks.admit_and_register(plan, "t")

    # 模拟热部署：同名同版本换了内容（新注册表里 hash 不同）
    replacement = DefinitionRegistry()
    replacement.register({"prompt": "changed"}, name="coder", version="1")
    h.definitions = replacement

    res = h.commit_batch(acb, [Write("a.md", "v1")])
    assert res.status is Status.DENIED
    assert res.detail["reason"] == "DEFINITION_HASH_MISMATCH"
    assert h.workspace.version("a.md") == 0


def test_no_definition_leaves_paths_unchanged():
    reg, _ref = _registry()
    h = make_harness(definitions=reg)
    plan = _plan(h, None)
    assert "definition" not in plan["tasks"]["t"]
    acb = h.tasks.admit_and_register(plan, "t")
    assert "definition" not in acb.spec
    res = h.commit_batch(acb, [Write("a.md", "v1")])
    assert res.status is Status.OK
