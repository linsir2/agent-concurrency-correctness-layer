"""MS1 出口条件：JSON-aware rebase 的数组集合语义（S3 / T8）。

对应 docs/s3-rebase-and-compensation.md §4.2 / §9 T8。
7 条断言覆盖：双方 append 并集、单方删除自动删、双方同删、增删互不干扰、
对象数组 PIVOT、GROW_ONLY 显式 PIVOT、确定性。
"""

from __future__ import annotations

import pytest

from harness import MISSING, MergePolicy, merge3


# ---------------------------------------------------------------- T8
@pytest.mark.ms1
def test_ms1_rebase_array_set_semantics():
    # ① 双方各 append 一项 → 并集（保序去重），无冲突
    r = merge3({"deps": ["a"]}, {"deps": ["a", "b"]}, {"deps": ["a", "c"]})
    assert r.ok and r.merged == {"deps": ["a", "b", "c"]}

    # ② 单方删除 → 自动合并为「删掉」（不是 PIVOT）
    r2 = merge3({"deps": ["a", "b"]}, {"deps": ["a"]}, {"deps": ["a", "b"]})
    assert r2.ok and r2.merged == {"deps": ["a"]}

    # ③ 双方删同一元素 → 删掉（一致）
    r3 = merge3({"deps": ["a", "b"]}, {"deps": ["b"]}, {"deps": ["b"]})
    assert r3.ok and r3.merged == {"deps": ["b"]}

    # ④ 一方删 b、另一方加 c → 结果既删 b 又加 c（增删互不干扰）
    r4 = merge3({"deps": ["a", "b"]}, {"deps": ["a"]}, {"deps": ["a", "b", "c"]})
    assert r4.ok and r4.merged == {"deps": ["a", "c"]}

    # ⑤ 对象数组无身份 → PIVOT
    r5 = merge3({"x": [{"id": 1}]},
                {"x": [{"id": 1}, {"id": 2}]},
                {"x": [{"id": 1}, {"id": 3}]})
    assert not r5.ok and r5.conflicts[0].kind == "pivot"
    assert r5.merged is MISSING

    # ⑥ GROW_ONLY 显式声明 + 删除 → PIVOT（用于不可缩短的列表）
    r6 = merge3({"log": ["a", "b"]}, {"log": ["a"]}, {"log": ["a", "b"]},
                policy={"": MergePolicy.GROW_ONLY})
    assert not r6.ok and r6.conflicts[0].kind == "pivot"

    # ⑦ 确定性：同输入 → 同输出
    assert merge3({"deps": ["a"]}, {"deps": ["a", "b"]}, {"deps": ["a", "c"]}) == r
    # ⑧ 嵌套 bool/int 不得混淆（容器层也要类型严格）
    r7 = merge3({"a": 1}, {"a": True}, {"a": 1})
    assert not r7.ok and r7.conflicts[0].kind == "type_change"
    assert r7.conflicts[0].path == "/a"
