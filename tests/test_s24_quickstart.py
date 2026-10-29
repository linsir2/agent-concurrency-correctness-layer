"""S24-P1：quickstart 四场景样例的防腐守卫。

样例是「对外说明书」，必须在主门禁里真跑，否则文案会慢慢变成假的。
"""

from __future__ import annotations

import pathlib
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "examples" / "quickstart" / "run.py"


def test_quickstart_runs_all_four_scenarios():
    proc = subprocess.run([sys.executable, str(SCRIPT)], capture_output=True,
                          text=True, timeout=180, check=False)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    for name in ("S1 并行 coding agents", "S2 同一逻辑实体并发改",
                 "S3 长任务崩溃续跑", "S4 多租户配额"):
        assert name in proc.stdout, proc.stdout
    assert "semantic_conflict" in proc.stdout
    assert "verify_failed" in proc.stdout
    assert "dedup=True" in proc.stdout
    assert "BUDGET_EXHAUSTED" in proc.stdout
