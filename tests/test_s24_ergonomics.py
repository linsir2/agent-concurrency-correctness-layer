"""S24-P1：`@rsc_tool` / `rsc_task` 接入壳。

RIPR：
- Reachability：装饰器真的走了 begin → 私有 worktree → apply → finish；
- Infection：Conflict 自动重试、未声明写、语义拦截、验证红各造 1 例；
- Propagation：`Deny → RscToolDenied`、`Wait → RscDeferred`、`Conflict → RscConflict`；
- Revealability：`ctx.receipt` / `last_receipt()` 可读，ledger 版本与收据一致。
"""

from __future__ import annotations

import subprocess
import sys

import pytest
from conftest import make_harness

from harness import RscSession
from harness.ergonomics import (
    RscConflict,
    RscDeferred,
    RscToolDenied,
    last_receipt,
    rsc_session,
    rsc_task,
    rsc_tool,
)
from harness.worktree import WorktreeManager

CORE = "def compute_rate(x):\n    return x * 2\n"
API = "from app.core import compute_rate\n\n\ndef handler():\n    return compute_rate(3)\n"
FEATURE = "from app.core import compute_rate\n\n\ndef feature():\n    return compute_rate(5)\n"
CHECK = [sys.executable, "check.py"]
CHECK_PY = ("import importlib, pkgutil\nimport app\n"
            "for m in pkgutil.iter_modules(app.__path__):\n"
            "    importlib.import_module(f'app.{m.name}')\n")


def _git(repo, *args):
    subprocess.run(["git", "-C", str(repo), *args], check=True,
                   capture_output=True, text=True)


@pytest.fixture()
def repo(tmp_path):
    base = tmp_path / "repo"
    (base / "app").mkdir(parents=True)
    _git(base, "init", "-q")
    _git(base, "config", "user.email", "t@example.com")
    _git(base, "config", "user.name", "t")
    (base / "app/__init__.py").write_text("", encoding="utf-8")
    (base / "app/core.py").write_text(CORE, encoding="utf-8")
    (base / "app/api.py").write_text(API, encoding="utf-8")
    (base / "check.py").write_text(CHECK_PY, encoding="utf-8")
    (base / "pyproject.toml").write_text("[tool]\n", encoding="utf-8")
    _git(base, "add", ".")
    _git(base, "commit", "-qm", "init")
    return base


def _session(tmp_path, repo=None, **kw):
    h = make_harness(run_limits=100)
    worktrees = WorktreeManager(str(repo), str(tmp_path / "wt")) if repo else None
    return RscSession(h, tenant="t", min_budget=1, worktrees=worktrees,
                      state_dir=str(tmp_path / "state"), **kw), h


# ---------------------------------------------------------------- 单次调用
def test_tool_commits_and_returns_value(tmp_path, repo):
    session, h = _session(tmp_path, repo)

    @rsc_tool(writes=["app/feature.py"])
    def add_feature(ctx, text):
        ctx.write("app/feature.py", text)
        return "ok"

    with rsc_session(session, "agent-a"):
        assert add_feature(FEATURE) == "ok"
    assert h.workspace.version("app/feature.py") == 1
    receipt = last_receipt()
    assert receipt is not None and receipt.seq >= 1


def test_decorator_writes_into_private_worktree(tmp_path, repo):
    session, h = _session(tmp_path, repo)
    seen = {}

    @rsc_tool(writes=["app/feature.py"])
    def add_feature(ctx):
        seen["worktree"] = ctx.worktree
        path = ctx.path("app/feature.py")
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(FEATURE)

    with rsc_session(session, "agent-a"):
        add_feature()
    assert seen["worktree"] and str(repo) not in seen["worktree"]
    assert not (repo / "app/feature.py").exists()      # 主树未被污染
    assert h.workspace.version("app/feature.py") == 1


def test_conflict_is_retried_with_fresh_base(tmp_path, repo):
    session, h = _session(tmp_path, repo)
    state = {"calls": 0}

    @rsc_tool(writes=["app/feature.py"], retry=1)
    def add_feature(ctx):
        if state["calls"] == 0:                 # 在「读完之后、提交之前」别人提交
            other = session.begin("agent-b", "other", writes=["app/feature.py"])
            session.apply(other.token, [("app/feature.py", "X = 1\n")],
                          idempotency_key="b:1", agent_id="agent-b")
            session.finish(other.token)
        state["calls"] += 1
        ctx.write("app/feature.py", FEATURE)

    with rsc_session(session, "agent-a"):
        add_feature()
    assert state["calls"] == 2                     # 第一次 Conflict，重试一次
    assert h.workspace.version("app/feature.py") == 2


def test_conflict_without_retry_raises(tmp_path, repo):
    session, h = _session(tmp_path, repo)

    @rsc_tool(writes=["app/feature.py"])
    def add_feature(ctx):
        other = session.begin("agent-b", "other", writes=["app/feature.py"])
        session.apply(other.token, [("app/feature.py", "X = 1\n")],
                      idempotency_key="b:1", agent_id="agent-b")
        session.finish(other.token)
        ctx.write("app/feature.py", FEATURE)

    with rsc_session(session, "agent-a"), pytest.raises(RscConflict):
        add_feature()
    assert h.workspace.version("app/feature.py") == 1   # 只有别人的写入


def test_undeclared_write_is_rejected(tmp_path, repo):
    session, _ = _session(tmp_path, repo)

    @rsc_tool(writes=["app/feature.py"])
    def bad(ctx):
        ctx.write("app/api.py", "oops\n")

    with rsc_session(session, "agent-a"), pytest.raises(RscToolDenied) as exc:
        bad()
    assert exc.value.code == "undeclared_write"


def test_delete_is_declared_write(tmp_path, repo):
    session, h = _session(tmp_path, repo)

    @rsc_tool(writes=["app/core.py"])
    def drop(ctx):
        ctx.delete("app/core.py")

    with rsc_session(session, "agent-a"):
        drop()
    assert h.workspace.version("app/core.py") == 1
    assert h.workspace.deleted_resources() == ["app/core.py"]


def test_semantic_block_raises_denied(tmp_path, repo):
    session, h = _session(tmp_path, repo, semantic="block")

    @rsc_tool(writes=["app/core.py"])
    def rename(ctx):
        ctx.write("app/core.py", CORE.replace("compute_rate", "compute_rate_v2"))

    with rsc_session(session, "agent-a"):
        rename()                                # 单文件改名：无外部引用 → 放行
    assert h.workspace.version("app/core.py") == 1

    @rsc_tool(writes=["app/feature.py"])
    def add_old_name(ctx):
        ctx.write("app/feature.py", FEATURE)

    with rsc_session(session, "agent-b"), pytest.raises(RscToolDenied) as exc:
        add_old_name()
    assert exc.value.code == "semantic_conflict"
    assert h.workspace.version("app/feature.py") == 0


def test_verify_red_blocks_commit(tmp_path, repo):
    session, h = _session(tmp_path, repo)

    @rsc_tool(writes=["app/feature.py"], verify=[CHECK])
    def add_broken(ctx):
        ctx.write("app/feature.py", "from app.core import nope\n")

    with rsc_session(session, "agent-a"), pytest.raises(RscToolDenied) as exc:
        add_broken()
    assert exc.value.code == "verify_failed"
    assert h.workspace.version("app/feature.py") == 0


def test_same_args_are_idempotent(tmp_path, repo):
    session, h = _session(tmp_path, repo)

    @rsc_tool(writes=["app/feature.py"])
    def add_feature(ctx, text):
        ctx.write("app/feature.py", text)
        return text

    with rsc_session(session, "agent-a"):
        add_feature("A")
        first = last_receipt()
        add_feature("A")
        second = last_receipt()
    assert first.receipt_id == second.receipt_id and second.dedup is True
    assert h.workspace.version("app/feature.py") == 1


def test_read_returns_pinned_version_not_latest(tmp_path):
    """`ctx.read` 必须读**本次 begin 钉住的那一版**，否则等于 read-committed。"""
    session, _h = _session(tmp_path)

    @rsc_tool(writes=["cfg"])
    def write_cfg(ctx, value):
        ctx.write("cfg", value)

    with rsc_session(session, "agent-a"):
        write_cfg("v1")
    with rsc_session(session, "agent-a"):
        write_cfg("v2")

    seen = {}

    @rsc_tool(writes=["other"], reads=["cfg"])
    def probe(ctx):
        seen["pinned"] = ctx.read("cfg")
        with rsc_session(session, "agent-c"):   # 事务中间另一个 agent 提交 v3
            write_cfg("v3")
        seen["after"] = ctx.read("cfg")
        ctx.write("other", "x")

    with rsc_session(session, "agent-b"):
        probe()
    assert seen["pinned"] == b"v2"             # 钉住版本
    assert seen["after"] == b"v2"              # 不会偷偷变成最新值


def test_without_binding_raises():
    @rsc_tool(writes=["a.md"])
    def noop(ctx):
        return None

    with pytest.raises(RuntimeError):
        noop()


# ---------------------------------------------------------------- 长任务
def test_task_reuses_one_token_for_many_applies(tmp_path, repo):
    session, h = _session(tmp_path, repo)
    with rsc_task(session, "agent-a", writes=["app/feature.py", "app/api.py"]) as task:
        task.write("app/feature.py", FEATURE)
        task.write("app/api.py", API)
        assert task.apply() is not None
        task.write("app/feature.py", FEATURE + "# v2\n")
        assert task.apply() is not None
    assert h.workspace.version("app/feature.py") == 2

    h2 = make_harness(run_limits=100)
    session2 = RscSession(h2, tenant="t", min_budget=1,
                          worktrees=WorktreeManager(str(repo), str(tmp_path / "wt2")),
                          state_dir=str(tmp_path / "state2"))
    with rsc_task(session2, "agent-b", writes=["app/feature.py"]) as task2:
        task2.write("app/feature.py", "X = 1\n")
        task2.apply()
    assert h2.workspace.version("app/feature.py") == 1


def test_tool_inside_task_reuses_token(tmp_path, repo):
    session, h = _session(tmp_path, repo)

    @rsc_tool(writes=["app/feature.py"])
    def add_feature(ctx):
        ctx.write("app/feature.py", FEATURE)

    with rsc_task(session, "agent-a", writes=["app/feature.py"]):
        add_feature()
        assert last_receipt().seq >= 1
    assert h.workspace.version("app/feature.py") == 1


def test_task_aborts_on_exception(tmp_path, repo):
    session, h = _session(tmp_path, repo)
    with pytest.raises(RuntimeError), \
            rsc_task(session, "agent-a", writes=["app/feature.py"]) as task:
        task.write("app/feature.py", FEATURE)
        raise RuntimeError("boom")
    assert h.workspace.version("app/feature.py") == 0


# ---------------------------------------------------------------- 逻辑资源
def test_logical_resource_without_worktree(tmp_path):
    """非文件资源（配置/工单）：没有 worktree 也能用，靠 ctx.write 声明内容。"""
    session, h = _session(tmp_path)

    @rsc_tool(writes=["svc/payments/config"])
    def patch_config(ctx, value):
        ctx.write("svc/payments/config", value)

    with rsc_session(session, "agent-a"):
        patch_config(b'{"replicas": 2}')
    assert h.workspace.version("svc/payments/config") == 1


def test_deferred_on_budget_exhausted(tmp_path):
    h = make_harness(run_limits=1)
    session = RscSession(h, tenant="t", min_budget=1,
                         state_dir=str(tmp_path / "state"))

    @rsc_tool(writes=["a.md"])
    def write_it(ctx):
        ctx.write("a.md", "v1")

    with rsc_session(session, "agent-a"):
        write_it()
        with pytest.raises(RscDeferred):
            write_it()
