"""#4：ToolRunner —— 库内唯一外部命令执行抽象。

重点验证「默认路径与旧内联 Popen 逐行为等价」，以及沙箱能力矩阵的
fail-closed 语义（cwd 不支持 → 抛，不静默降级）。
"""

from __future__ import annotations

import sys

import pytest
from conftest import make_harness

from harness import RscSession
from harness.tool_runner import (
    EgressUnsupported,
    LocalRunner,
    SandboxedRunner,
    ToolResult,
    make_runner,
)


def test_local_runner_basic():
    r = LocalRunner().run([sys.executable, "-c", "print('hi')"])
    assert r.rc == 0 and "hi" in r.output and not r.timed_out


def test_local_runner_spawn_failure_is_stable():
    r = LocalRunner().run(["/nonexistent/binary-xyz"])
    assert r.rc == -1 and r.output.startswith("verify_spawn_failed")


def test_local_runner_timeout_kills_process_group():
    code = ("import subprocess,sys,time;"
            "subprocess.Popen([sys.executable,'-c','import time;time.sleep(30)']);"
            "time.sleep(30)")
    r = LocalRunner().run([sys.executable, "-c", code], timeout_s=0.3)
    assert r.timed_out and r.rc == -1
    assert "verify_timeout" in r.output


def test_make_runner_off_is_local():
    assert isinstance(make_runner("off", None), LocalRunner)


def test_make_runner_require_fails_closed(monkeypatch):
    import harness.egress as egress

    class S:
        available = False

    monkeypatch.setattr(egress, "probe", lambda: S())
    with pytest.raises(egress.EgressUnavailable):
        make_runner("require", None)


def test_make_runner_auto_degrades_to_local(monkeypatch):
    import harness.egress as egress

    class S:
        available = False

    monkeypatch.setattr(egress, "probe", lambda: S())
    assert isinstance(make_runner("auto", None), LocalRunner)


def test_sandbox_unsupported_cwd_is_explicit():
    with pytest.raises(EgressUnsupported) as exc:
        SandboxedRunner("lsm").run(["true"], cwd="/tmp")
    assert "lsm:cwd" in str(exc.value)


def test_session_verify_uses_injected_runner():
    h = make_harness()
    calls = []

    class Fake:
        backend = "fake"

        def run(self, argv, *, cwd=None, timeout_s=30.0, env=None):
            calls.append((list(argv), cwd, timeout_s))
            return ToolResult(0, "ok", "", False, "fake")

    s = RscSession(h, worktrees=None, tool_runner=Fake())
    code, out = s._run_verify_step(["true"], "/tmp", 2.0)
    assert (code, out) == (0, "ok")
    assert calls and calls[0][0] == ["true"]


def test_session_verify_timeout_mapping_is_unchanged():
    """超时仍映射为 `(-1, "verify_timeout after Ns")`（下游按此字符串分类）。"""
    h = make_harness()

    class Slow:
        backend = "slow"

        def run(self, argv, *, cwd=None, timeout_s=30.0, env=None):
            return ToolResult(-1, "", "whatever", True, "slow")

    s = RscSession(h, worktrees=None, tool_runner=Slow())
    assert s._run_verify_step(["x"], "/tmp", 3.0) == (-1, "verify_timeout after 3.0s")
