"""MS8 出口条件（续）：I9 / I10 / I13 + 规范四件套的可执行验证。"""

from __future__ import annotations

from conftest import admit, make_harness, seed, stage

from harness.spec import (
    INVARIANT_IDS,
    ResourceKind,
    ResourceSpec,
    build_receipt,
    decide,
    receipt_hash,
    verify_chain,
)


def test_spec_resource_model():
    """资源模型：版本语义分类正确。"""
    assert ResourceSpec(ResourceKind.FILE, "a.ts").version_semantics() == "monotonic"
    assert ResourceSpec(ResourceKind.CONTRACT, "c").version_semantics() == "content_hash"
    assert ResourceSpec(ResourceKind.EFFECT, "e").version_semantics() == "idempotency_key"
    assert set(INVARIANT_IDS) == {f"I{i}" for i in range(1, 15)}


def test_spec_protocol_grant_wait_die():
    """协调协议：契约冲突 → WAIT；写-写重叠 → DIE（放行至提交层）。"""
    assert decide(contract_conflict=True).verdict.value == "WAIT"
    assert decide(read_write_conflict=True).verdict.value == "WAIT"
    assert decide(write_write_overlap=True).verdict.value == "DIE"
    assert decide().verdict.value == "GRANT"


def test_i9_plan_valid_acyclic():
    """I9：Plan 无环、footprint 完整。"""
    h = make_harness()
    plan = h.planner.plan("I9", [
        {"task_id": "T1", "committed": ["a"], "min_budget": 5},
        {"task_id": "T2", "committed": ["b"], "min_budget": 5},
    ])
    h.planner.add_logical_edge(plan, "T1", "T2")
    h.plans["I9"] = plan

    # 无环
    visiting, visited = set(), set()

    def visit(n):
        if n in visiting:
            raise AssertionError("cycle")
        if n in visited:
            return
        visiting.add(n)
        for e in plan["edges"]:
            if e["from"] == n:
                visit(e["to"])
        visiting.discard(n)
        visited.add(n)

    for tid in plan["tasks"]:
        visit(tid)

    # footprint 完整
    for task in plan["tasks"].values():
        assert "committed" in task["footprint"]
        assert "min_budget" in task["footprint"]


def test_i10_isolation_private_worktree_invisible():
    """I10：Z3 私有 worktree 对共享态不可见（stage 不改变共享态）。"""
    h = make_harness()
    v0 = seed(h, "r", "shared")
    plan = h.planner.plan("I10", [{"task_id": "T1", "min_budget": 10}])
    h.plans["I10"] = plan
    acb = admit(h, "I10", plan, "T1")

    h.runtime._do_action(acb, stage("r", "private-edit"), 0)
    assert acb.staged["r"] == "private-edit"      # 写在 worktree
    assert h.workspace.get("r") == "shared"       # 共享态未变
    assert h.workspace.version("r") == v0


def test_i13_receipt_chain():
    """I13：收据 hash-chain + 签名可离线验证。"""
    secret = "s3cr3t"
    r1 = build_receipt(receipt_id="R1", run_id="run", task_id="T1",
                       logical_subgoal_id="S1", resource="a.ts",
                       version_before=0, version_after=1, secret=secret)
    r2 = build_receipt(receipt_id="R2", run_id="run", task_id="T1",
                       logical_subgoal_id="S1", resource="a.ts",
                       version_before=1, version_after=2,
                       prev_receipt_hash=receipt_hash(r1), secret=secret)

    assert verify_chain([r1, r2], secret) is True
    assert verify_chain([r1, r2], "wrong-secret") is False

    tampered = build_receipt(receipt_id="R1", run_id="run", task_id="T1",
                             logical_subgoal_id="S1", resource="a.ts",
                             version_before=0, version_after=99, secret=secret)
    assert verify_chain([tampered, r2], secret) is False
