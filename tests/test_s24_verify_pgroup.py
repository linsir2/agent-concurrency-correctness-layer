"""S24-P0：Verifier 超时必须杀掉**整个进程组**（孙进程不得存活）。

旧实现 `subprocess.run(timeout=...)` 只杀直接子进程：验证命令再起测试运行器/
构建工具时，孙进程会继续写文件、占端口。本用例用「父睡 30s、孙 1.5s 后写
marker」构造：修复后 marker 不得出现。
"""

from __future__ import annotations

import pathlib
import subprocess
import sys
import time

from conftest import make_harness

from harness import RscSession
from harness.worktree import WorktreeManager


def _session(tmp_path: pathlib.Path):
    repo = pathlib.Path(tmp_path) / "repo"
    (repo / "app").mkdir(parents=True)
    (repo / "app" / "__init__.py").write_text("", encoding="utf-8")
    (repo / "app" / "a.py").write_text("X = 1\n", encoding="utf-8")
    for args in (["init", "-q"],
                 ["config", "user.email", "c@e.com"],
                 ["config", "user.name", "c"],
                 ["add", "."],
                 ["commit", "-qm", "init"]):
        subprocess.run(["git", "-C", str(repo), *args], check=True,
                       capture_output=True, text=True)
    h = make_harness(run_limits=10)
    session = RscSession(
        h, tenant="t", min_budget=1,
        worktrees=WorktreeManager(str(repo), str(tmp_path / "wt")),
        state_dir=str(tmp_path / "state"))
    return h, session


def test_verify_timeout_kills_grandchildren(tmp_path):
    _h, session = _session(tmp_path)
    tok = session.begin("a", "g", writes=["app/a.py"])
    marker = tmp_path / "marker"
    grandchild = (
        "import time,pathlib;"
        "time.sleep(1.5);"
        f"pathlib.Path({str(marker)!r}).write_text('x')"
    )
    code = (
        "import subprocess, sys, time\n"
        f"subprocess.Popen([sys.executable, '-c', {grandchild!r}])\n"
        "time.sleep(30)\n"
    )
    result = session.verify(tok.token, [sys.executable, "-c", code],
                            timeout_s=1)
    assert result.status.value == "DENIED"
    assert result.detail["exit_code"] == -1
    assert "verify_timeout" in result.detail["tail"]
    time.sleep(2.0)
    assert not marker.exists()          # 孙进程已被进程组 kill 掉


def test_verify_success_still_works(tmp_path):
    _h, session = _session(tmp_path)
    tok = session.begin("a", "g", writes=["app/a.py"])
    result = session.verify(tok.token, [sys.executable, "-c", "pass"],
                            timeout_s=10)
    assert result.status.value == "OK"
    assert result.detail["exit_code"] == 0
