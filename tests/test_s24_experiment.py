"""S24 实验冒烟：语义冲突实验必须跑通，且四段结论都成立。

防腐烂：一旦有人改坏 verifier 门禁或 git 行为变化，这里会先红。
"""

from __future__ import annotations

import pathlib
import subprocess
import sys

_ROOT = pathlib.Path(__file__).resolve().parents[1]


def test_semantic_conflict_experiment_runs():
    proc = subprocess.run(
        [sys.executable, str(_ROOT / "examples/semantic_conflict/run.py")],
        cwd=str(_ROOT), capture_output=True, text=True, timeout=180,
        check=False)
    assert proc.returncode == 0, proc.stderr[-2000:]
    out = proc.stdout
    # [1] 文本合并干净，检查红
    assert "合并干净=True" in out and "合并后绿=False" in out
    # [2] 没有 verify_cmd 时两张收据都签发（缺口）
    assert "A=Receipt  B=Receipt" in out
    # [3] verify_cmd 拦住
    assert "B=Deny" in out and "code=verify_failed" in out
    assert "code=semantic_conflict" in out
    # [4] 修正后放行且 verified
    assert "verified=True" in out
    assert "[结论]" in out
