"""S23 示例冒烟：`examples/three_agents/run.py` 必须可跑且三条保证成立。

示例本身包含 naive vs RSC 的对照断言；这里只保证它没有随时间腐烂
（文件缺失、导入错误、退出码非 0）。
"""

from __future__ import annotations

import pathlib
import subprocess
import sys

_ROOT = pathlib.Path(__file__).resolve().parents[1]


def test_three_agents_example_runs():
    proc = subprocess.run(
        [sys.executable, str(_ROOT / "examples/three_agents/run.py")],
        cwd=str(_ROOT), capture_output=True, text=True, timeout=120,
        check=False)
    assert proc.returncode == 0, proc.stderr[-2000:]
    assert "[PASS]" in proc.stdout
    # 关键证据：naive 丢写、RSC 后两份都在、HTTP 状态码
    assert "lost update" in proc.stdout
    assert "- arch" in proc.stdout and "- api" in proc.stdout
    assert "401" in proc.stdout and "409" in proc.stdout and "403" in proc.stdout
