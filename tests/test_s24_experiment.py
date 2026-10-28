"""S24 调研实验冒烟：语义冲突实验必须跑通且结论成立。

防腐烂：一旦有人给门面加了 verifier 而没更新实验，或 git 行为变化，
这里会先红。
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
    # 前提：文本合并干净、测试红
    assert "合并干净=True" in out
    assert "合并后测试绿=False" in out
    # 关键结论：我们的层不报冲突但也没拦住
    assert "出现冲突=False" in out and "都拿到收据=True" in out
    assert "允许提交=False" in out
    assert "[结论]" in out
