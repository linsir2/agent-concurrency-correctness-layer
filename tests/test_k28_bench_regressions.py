"""K-28 与基准工具链的回归测试（对抗评审清单 1–3、5–8）。

覆盖：
- C1 静默丢写（`REJECT_STALE_READ` 必须对陈旧 staged 重跑 merge3）；
- `_clean_merge_new_tops` 按 merge3 对象规则精确判定（删除键不得复活）；
- `_py_line_merge` 零长插入块冲突（并与 `git merge-file` 差分）；
- `oracle_merge` 驱动 `expected_by_resource` 的自检；
- `_diff_keys` 的 missing/extra/changed 三路互斥；
- `assert_deterministic`（B1/B2 逐值相等）与 `assert_b3_bounded`（B3 有界带）与 `judge_cell(degraded=True)`。
"""

from __future__ import annotations

import shutil
import subprocess
import tempfile

import pytest
from conftest import admit, commit, decision, make_harness, seed

from bench import conditions as C
from bench import metrics as M
from bench import run_bench as RB
from bench.workloads import WORKLOADS, oracle_for, oracle_self_check
from harness import Status, merge3
from harness.runtime import _validate_resolution


# ================================================================ 1. K-28
def _k28_setup():
    h = make_harness()
    seed(h, "r1", {"a": 0})
    seed(h, "r2", {"x": 0, "y": 0})
    plan = h.planner.plan("R", [
        {"task_id": "T1", "committed": ["r1", "r2"]},
        {"task_id": "T2", "committed": ["r1", "r2"]},
    ])
    h.plans["R"] = plan
    a1 = admit(h, "R", plan, "T1")
    a2 = admit(h, "R", plan, "T2")
    # a1 先基于 v1 提交 r2 的 x=1（只 stage r2，避免 turn 末自动提交 r1）
    h.workspace.stage(a1, "r2", {"x": 1, "y": 0})
    # a2 基于 v1 同时 stage r1/r2；提交 r1 时复验 read_set 会先撞上陈旧的 r2
    h.workspace.stage(a2, "r1", {"a": 2})
    h.workspace.stage(a2, "r2", {"x": 0, "y": 2})
    h.run_task(a1, [decision(1, commit("r2"))])
    assert h.workspace.get("r2") == {"x": 1, "y": 0}
    return h, a2


def test_k28_stale_staged_merged_not_overwritten():
    """`REJECT_STALE_READ` 后必须合并陈旧 staged，不得直接覆盖他人写。"""
    h, a2 = _k28_setup()
    st = h.run_task(a2, [decision(1, commit("r1", "r2"))])

    assert st is Status.OK
    assert a2.state.name == "DONE"
    assert h.workspace.get("r1") == {"a": 2}
    # a1 的 x=1 与 a2 的 y=2 都在（旧实现会丢 x=1）
    assert h.workspace.get("r2") == {"x": 1, "y": 2}


def test_k28_old_behavior_counterproof(monkeypatch):
    """反证：把修复回退成「只刷版本」→ 陈旧 staged 覆盖他人写（x=1 丢失）。"""
    h, a2 = _k28_setup()

    def old_refresh(acb, result):
        h.runtime._refresh_read_dep(acb, result.detail.get("resource"))

    monkeypatch.setattr(h.runtime, "_rebase_stale_staged", old_refresh)
    h.run_task(a2, [decision(1, commit("r1", "r2"))])

    assert h.workspace.get("r2") == {"x": 0, "y": 2}      # x=1 被静默覆盖


# ================================================================ 2. clean_new_tops
def test_clean_merge_new_tops_deleted_key_not_resurrected():
    """base={x:0,a:0} local={x:0,a:1} committed={a:2}：x 被 merge3 删除。"""
    base = {"x": 0, "a": 0}
    local = {"x": 0, "a": 1}
    committed = {"a": 2}
    mr = merge3(base, local, committed)
    assert not mr.ok

    # 与 merge3 一致（x 删除）→ True
    assert _validate_resolution({"a": 1}, mr, base, committed, local) is True
    # 复活已删除键 / 多余非法键 → False
    assert _validate_resolution({"a": 1, "x": 0}, mr, base, committed, local) is False
    # 缺键 → False
    assert _validate_resolution({}, mr, base, committed, local) is False


def test_validate_resolution_allows_clean_delete_of_committed_key():
    """committed 中某键被 merge3 干净删除、另一键冲突 → 合法输出不得含被删键。"""
    base = {"a": 1, "b": 1}
    local = {"b": 2}
    committed = {"a": 1, "b": 3}
    mr = merge3(base, local, committed)
    assert not mr.ok and {c.path for c in mr.conflicts} == {"/b"}

    assert _validate_resolution({"b": 5}, mr, base, committed, local) is True
    assert _validate_resolution({"a": 1, "b": 5}, mr, base, committed, local) is False


# ================================================================ 3. 行级降级合并
def test_py_line_merge_insert_conflict():
    """零长插入块：双方在同一点插入不同行 → 冲突（旧实现两行全丢且不报冲突）。"""
    assert C._py_line_merge("A\nB\n", "A\nX\nB\n", "A\nY\nB\n") == (None, True)


_GIT_CASES = [
    ("A\nB\n", "A\nX\nB\n", "A\nY\nB\n"),                    # 同点插入不同内容
    ("A\nB\n", "A\nX\nB\n", "A\nX\nB\n"),                    # 同点插入相同内容
    ("A\nB\nC\n", "A\nX\nB\nC\n", "A\nB\nC\nD\n"),           # 远离的插入
    ("A\nB\n", "A\nX\n", "A\nY\n"),                          # 同区间不同替换
    ("A\nB\nC\n", "A\nX\nC\n", "A\nB\nY\n"),                 # 相邻替换
    ("A\nB\nC\n", "A\nB\nX\nC\n", "A\nY\nB\nC\n"),           # 插入 vs 插入（不同点）
    ("A\nB\nC\n", "A\nB\nX\nC\n", "A\nY\nC\n"),              # 插入紧贴替换边界
    ("A\nB\nC\n", "A\nX\nB\nC\n", "A\nB\nY\n"),              # 插入 vs 非相邻替换
]


@pytest.mark.skipif(shutil.which("git") is None, reason="git 不可用")
def test_py_line_merge_matches_git():
    """与 `git merge-file` 差分：冲突判定一致；干净时输出一致。"""
    git = shutil.which("git")
    for base, ours, theirs in _GIT_CASES:
        with tempfile.TemporaryDirectory() as d:
            paths = []
            for name, text in (("b", base), ("o", ours), ("t", theirs)):
                p = f"{d}/{name}"
                with open(p, "w", encoding="utf-8") as f:
                    f.write(text)
                paths.append(p)
            proc = subprocess.run([git, "merge-file", "-p", paths[1], paths[0], paths[2]],
                                  capture_output=True, text=True, check=False)
        git_conflict = proc.returncode != 0
        merged, py_conflict = C._py_line_merge(base, ours, theirs)
        assert py_conflict == git_conflict, (base, ours, theirs)
        if not git_conflict:
            assert merged == proc.stdout, (base, ours, theirs)


# ================================================================ 5. oracle
def test_oracle_self_check_all_workloads():
    for name, w in WORKLOADS.items():
        assert oracle_self_check(w) is True, name
        assert oracle_for(w.decisions, w.initial_state) == w.expected_by_resource


# ================================================================ 6. _diff_keys
def test_diff_keys_missing_extra_changed_disjoint():
    exp = {"a": 1, "b": {"c": 2}, "lst": [1, 2, 3]}
    fin = {"a": 9, "b": {}, "lst": [1, 2], "z": 0}
    d = M._diff_keys(exp, fin)

    assert d["missing"] == {"/b/c", "/lst/2"}
    assert d["extra"] == {"/z"}
    assert d["changed"] == {"/a", "/lst/__len__"}
    # 三路互斥且并集 == 旧语义的「不一致路径」
    assert not (d["missing"] & d["extra"])
    assert M._key_diff(exp, fin) == d["missing"] | d["extra"] | d["changed"]


def test_diff_keys_absent_resource():
    d = M._diff_keys({"a": 1}, M._ABSENT)
    assert d == {"missing": {"/"}, "extra": set(), "changed": set()}


# ================================================================ 7. 确定性判定
# B1/B2（确定性算法）逐值相等；B3（并发）有界带（见 run_bench.py 模块 docstring）。
def _cell(values_by_metric: dict) -> dict:
    return {
        k: {"values": list(v), "mean": sum(v) / len(v), "variance": 0.0,
            "min": min(v), "max": max(v), "unique": sorted(set(v))}
        for k, v in values_by_metric.items()
    }


def _c1_cell(conflict, wasted, lost=0.0, silent=0.0, n=3) -> dict:
    """C1 一格的四指标 cell；conflict/wasted 可为标量或序列。"""
    def rep(x):
        return list(x) if isinstance(x, (list, tuple)) else [x] * n
    return _cell({
        "lost_update_rate": rep(lost),
        "conflict_rate_silent": rep(silent),
        "conflict_rate_detected": rep(conflict),
        "wasted_work": rep(wasted),
    })


def _summary(b1=None, b2=None, b3=None) -> dict:
    b1 = b1 if b1 is not None else _c1_cell(4 / 9, 4 / 3)      # C1/B1 典型值
    b2 = b2 if b2 is not None else _c1_cell(0.0, 0.0)           # C1/B2 典型值
    b3 = b3 if b3 is not None else _c1_cell(5 / 9, 5 / 3)       # C1/B3 期望值
    return {"C1": {"B1": b1, "B2": b2, "B3": b3}}


def test_assert_deterministic_passes_when_samples_equal():
    RB.assert_deterministic(_summary())


def test_assert_deterministic_fails_when_b1_differs():
    """反证：B1 某指标随机 → 逐值相等断言必须失败（B1/B2 的牙还在）。"""
    b1 = _c1_cell([4 / 9, 4 / 9, 0.9], 4 / 3)
    with pytest.raises(AssertionError):
        RB.assert_deterministic(_summary(b1=b1))


def test_assert_deterministic_fails_when_b2_differs():
    b2 = _c1_cell(0.0, [0.0, 0.0, 0.4])
    with pytest.raises(AssertionError):
        RB.assert_deterministic(_summary(b2=b2))


def test_assert_deterministic_ignores_b3_jitter():
    """B3 的并发抖动（5/9↔6/10）不得触发逐值相等断言（那是 B1/B2 的职责）。"""
    b3 = _c1_cell([5 / 9, 5 / 9, 6 / 10], [5 / 3, 5 / 3, 2.0])
    RB.assert_deterministic(_summary(b3=b3))


def test_assert_b3_bounded_passes_within_band():
    b3 = _c1_cell([5 / 9, 5 / 9, 6 / 10], [5 / 3, 5 / 3, 2.0])
    RB.assert_b3_bounded(_summary(b3=b3))


def test_assert_b3_bounded_fails_outside_band():
    b3 = _c1_cell(5 / 9, [5 / 3, 5 / 3, 3.0])
    with pytest.raises(AssertionError):
        RB.assert_b3_bounded(_summary(b3=b3))


def test_assert_b3_bounded_fails_when_correctness_metric_nonzero():
    """期望为 0 的 B3 正确性指标（lost/silent）必须严格 0（模拟 K-28 丢写）。"""
    b3 = _c1_cell(5 / 9, 5 / 3, lost=0.333, silent=0.333)
    with pytest.raises(AssertionError):
        RB.assert_b3_bounded(_summary(b3=b3))


# ================================================================ 8. degraded 判定
def _fake_means(lost: float, degraded: bool) -> dict:
    return {"lost_update_rate": {"mean": lost}, "degraded": degraded}


def test_judge_cell_degraded_invalid():
    j = RB.judge_cell("C2", _fake_means(1.0, True), _fake_means(0.0, False))
    assert j["kind"] == "degraded_invalid"
    assert j["degraded"] is True
    assert j["verdict"] is None
    assert j["role"] == "环境降级，判定无效"


def test_judge_cell_normal_c2_verdict():
    j = RB.judge_cell("C2", _fake_means(1.0, False), _fake_means(0.0, False))
    assert j["verdict"] is True
