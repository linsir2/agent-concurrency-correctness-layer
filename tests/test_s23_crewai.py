"""S23-1：CrewAI 适配器（包装 `BaseTool.run`，硬阻断 + 账本提交）。

需要 `pip install 'harness[crewai]'`（CrewAI 1.15.x，`Requires-Python <3.14`）；
未安装则整文件 skip（CI 的 3.14 任务会跳过，3.12 任务会真跑）。

RIPR：
- Reachability：工具 `_run` 确实被调用 / 未调用（spy 计数）；
- Infection：注入 stale/预算耗尽后 apply 真的被拒；
- Propagation：返回给模型的文本含 `RSC_CONFLICT` / `RSC_WAIT` / `RSC_DENY`；
- Revealability：断言账本内容、版本、收据 id 与 dedup，而不只看「没抛异常」。
"""

from __future__ import annotations

import os
import subprocess
from typing import Any

import pytest

crewai = pytest.importorskip("crewai", reason="需要 CrewAI 1.15.x")

from conftest import make_harness
from crewai.tools import BaseTool

from harness import RscSession
from harness.facade import Receipt
from harness.integrations.crewai import rsc_tools
from harness.worktree import WorktreeManager


# ---------------------------------------------------------------- fixtures
def _git(repo, *args):
    subprocess.run(["git", "-C", str(repo), *args],
                   check=True, capture_output=True, text=True)


@pytest.fixture()
def repo(tmp_path):
    base = tmp_path / "repo"
    base.mkdir()
    _git(base, "init", "-q")
    _git(base, "config", "user.email", "t@example.com")
    _git(base, "config", "user.name", "t")
    (base / "seed.txt").write_text("init", encoding="utf-8")
    _git(base, "add", ".")
    _git(base, "commit", "-qm", "init")
    return base


@pytest.fixture()
def session(tmp_path, repo):
    h = make_harness(run_limits=1000)
    return RscSession(h, tenant="t", min_budget=1,
                      worktrees=WorktreeManager(str(repo), str(tmp_path / "wt")),
                      state_dir=str(tmp_path / "state"))


class WriteTool(BaseTool):
    """声明 root 形参的工具：写入 `root/a.txt`。"""

    name: str = "write_file"
    description: str = "write a.txt under root"
    calls: int = 0

    def _run(self, root: str, value: str) -> str:
        self.calls += 1
        with open(os.path.join(root, "a.txt"), "w", encoding="utf-8") as fh:
            fh.write(value)
        return "ok"


class NoRootTool(BaseTool):
    """没有 root 形参：strict 模式下必须被拒绝，且不执行。"""

    name: str = "no_root"
    description: str = "no root param"
    calls: int = 0

    def _run(self, value: str) -> str:
        self.calls += 1
        return "ran"


class RacingTool(BaseTool):
    """执行期间由另一会话先提交同一资源 → 适配器 apply 必须拿到 Conflict。"""

    name: str = "racing"
    description: str = "commit from another session mid-run"
    other: Any = None

    def _run(self, root: str, value: str) -> str:
        with open(os.path.join(root, "a.txt"), "w", encoding="utf-8") as fh:
            fh.write(value)
        tok = self.other.begin("other", "race", writes=("a.txt",))
        self.other.apply(tok.token, [("a.txt", b"other")],
                         idempotency_key="other-1", agent_id="other")
        return "ok"


class CwdTool(BaseTool):
    """无 root 形参、按 cwd 相对路径写：验证 shared_cwd 模式真的读回工具产物。"""

    name: str = "cwd_tool"
    description: str = "write a.txt in cwd"

    def _run(self, value: str) -> str:
        with open("a.txt", "w", encoding="utf-8") as fh:
            fh.write(value)
        return "ran"


def _wrap(session, tool, paths=("a.txt",), **kw):
    wrapped = rsc_tools(session, [tool], wraps={tool.name: paths},
                        agent_id="a1", **kw)
    assert wrapped and wrapped[0] is not tool
    return wrapped[0]


# ---------------------------------------------------------------- 用例
def test_wrapped_tool_commits_into_ledger(session):
    h = session.h
    tool = WriteTool()
    out = _wrap(session, tool).run(root="/tmp/ignored", value="v1")
    assert out == "ok" and tool.calls == 1
    assert h.workspace.get("a.txt") == b"v1"
    assert h.workspace.version("a.txt") == 1


def test_wrapped_tool_keeps_args_schema(session):
    tool = WriteTool()
    wrapped = _wrap(session, tool)
    fields = getattr(wrapped.args_schema, "model_fields", {})
    assert "root" in fields and "value" in fields


def test_same_args_retry_returns_same_receipt(session):
    seen: list = []
    orig = session.apply

    def spy(*a, **kw):
        r = orig(*a, **kw)
        seen.append(r)
        return r

    session.apply = spy                                   # type: ignore[method-assign]
    tool = _wrap(session, WriteTool())
    assert tool.run(root="/x", value="v1") == "ok"
    assert tool.run(root="/x", value="v1") == "ok"
    assert all(isinstance(r, Receipt) for r in seen), seen
    assert seen[0].receipt_id == seen[1].receipt_id
    assert seen[1].dedup is True
    assert session.h.workspace.version("a.txt") == 1


def test_root_is_excluded_from_idempotency_key(session):
    """评审 P1：root 会被适配器覆盖，不能参与业务键（换 root 同内容要命中同键）。"""
    seen: list = []
    orig = session.apply

    def spy(*a, **kw):
        r = orig(*a, **kw)
        seen.append(r)
        return r

    session.apply = spy                                   # type: ignore[method-assign]
    tool = _wrap(session, WriteTool())
    assert tool.run(root="/x", value="v1") == "ok"
    assert tool.run(root="/y", value="v1") == "ok"
    assert isinstance(seen[0], Receipt) and isinstance(seen[1], Receipt)
    assert seen[0].receipt_id == seen[1].receipt_id and seen[1].dedup is True
    assert session.h.workspace.version("a.txt") == 1


def test_stale_write_maps_to_rsc_conflict(session, tmp_path, repo):
    other = RscSession(session.h, tenant="t", min_budget=1,
                       worktrees=WorktreeManager(str(repo), str(tmp_path / "wt2")),
                       state_dir=str(tmp_path / "state2"))
    tool = RacingTool(other=other)
    out = _wrap(session, tool).run(root="/x", value="mine")
    assert out.startswith("RSC_CONFLICT:"), out
    assert session.h.workspace.get("a.txt") == b"other"


def test_missing_root_is_fail_closed(session):
    tool = NoRootTool()
    out = _wrap(session, tool).run(value="v")
    assert out == "RSC_DENY: tool_must_accept_root(root)"
    assert tool.calls == 0
    assert session.h.workspace.version("a.txt") == 0


def test_shared_cwd_mode_commits(tmp_path, repo, monkeypatch):
    h = make_harness(run_limits=1000)
    session = RscSession(h, tenant="t", min_budget=1,
                         worktrees=WorktreeManager(str(repo), str(tmp_path / "wt")),
                         state_dir=str(tmp_path / "state"))
    monkeypatch.chdir(tmp_path)
    tool = CwdTool()
    wrapped = rsc_tools(session, [tool], wraps={"cwd_tool": ("a.txt",)},
                        agent_id="a1", shared_cwd=True)[0]
    assert wrapped.run(value="v") == "ran"
    assert wrapped.run(value="v") == "ran"      # 同键重放由 facade 去重
    assert h.workspace.get("a.txt") == b"v"


def test_budget_exhausted_is_soft_wait(tmp_path, repo):
    h = make_harness(run_limits=1)
    session = RscSession(h, tenant="t", min_budget=1,
                         worktrees=WorktreeManager(str(repo), str(tmp_path / "wt")),
                         state_dir=str(tmp_path / "state"))
    tool = _wrap(session, WriteTool())
    assert tool.run(root="/x", value="v1") == "ok"      # 用掉唯一预算
    out = tool.run(root="/x", value="v2")
    assert out.startswith("RSC_WAIT:"), out
    assert h.workspace.version("a.txt") == 1


def test_coordinator_unavailable_is_deny(session, monkeypatch):
    class RaftUnavailable(RuntimeError):
        pass

    def boom(*a, **kw):
        raise RaftUnavailable("no quorum")

    monkeypatch.setattr(session.h, "_admit_and_register", boom)
    tool = _wrap(session, WriteTool())
    out = tool.run(root="/x", value="v")
    assert out.startswith("RSC_DENY: coordinator_unavailable"), out


def test_undeclared_tool_passes_through(session):
    tool = NoRootTool()
    wrapped = rsc_tools(session, [tool], wraps={}, agent_id="a1")
    assert wrapped[0] is tool
    assert tool.run(value="v") == "ran"
