"""S3：源文本一致性脚本必须跟随内核拆分（防止「搬代码 → 检查静默失效」）。

背景：`scripts/check_s24_conformance.py` 用**源码文本断言**校验内核行为
（例如「定义版本钉住」这条注释、`state.lease_store is not None` 这个路径）。
S1–S3 把实现搬进 `harness/kernel/*.py` 后，单文件 grep 会假红/假绿。

本用例把该脚本本身当作门禁：
- S1 搬到 commit.py 时它已经红过一次（"定义版本钉住"）；
- S3 又红一次（`_lease_store` → `state.lease_store`）。

RIPR：Reachability=真跑脚本；Infection=拆分导致 token 位置变化；
Propagation=退出码非 0；Revealability=失败时回显脚本输出。
"""

from __future__ import annotations

import pathlib
import subprocess
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent

# 这 5 个脚本在原始提交 999b8c3 上就是全绿；s17–s22 在原始提交上已失败
# （AttributeError / ❌），不属于本次重构回归，故不纳入门禁。
PASSING_BASELINE = [
    "check_s14_conformance.py",
    "check_s15_conformance.py",
    "check_s16_conformance.py",
    "check_s23_conformance.py",
    "check_s24_conformance.py",
]


@pytest.mark.parametrize("script", PASSING_BASELINE)
def test_conformance_script_passes(script):
    proc = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / script)],
        capture_output=True, text=True, cwd=str(ROOT), timeout=180)
    assert proc.returncode == 0, (
        f"{script} 失败（通常是源码被搬动后断言未跟随）:\n"
        + proc.stdout[-3000:] + proc.stderr[-2000:])
