"""S23-3：AG2（ag2ai 1.2.x）适配器（工具执行中间件）。

直接调用 `on_tool_execution`，与 AG2 的 `call_next` 语义一致：
不调 `call_next` 即短路；`event.serialized_arguments` 可注入 root。
"""

from __future__ import annotations

import asyncio
import os
import subprocess

import pytest

ag2 = pytest.importorskip("ag2", reason="需要 ag2 1.2.x")

from ag2.events.tool_events import ToolCallEvent, ToolResultEvent
from conftest import make_harness

from harness import RscSession
from harness.facade import Receipt
from harness.integrations.ag2 import rsc_middleware
from harness.worktree import WorktreeManager


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


def _event(name="write_file", args='{"value":"v1"}', call_id=None) -> ToolCallEvent:
    ev = ToolCallEvent(name=name, arguments=args)
    if call_id is not None:
        ev.id = call_id
    return ev


def _middleware(factory, event):
    return factory(event, None)


def _writer(hook=None):
    """返回 `call_next`：把 args["root"]/a.txt 写成 args["value"]。"""
    async def call_next(event, context):
        args = event.serialized_arguments
        if hook is not None:
            hook()
        with open(os.path.join(args["root"], "a.txt"), "w", encoding="utf-8") as fh:  # noqa: ASYNC230  # 测试内同步写
            fh.write(args["value"])
        return ToolResultEvent.from_call(event, "ok")
    return call_next


# ---------------------------------------------------------------- 用例
def test_commits_into_ledger(session):
    h = session.h
    factory = rsc_middleware(session, writes={"write_file": ("a.txt",)},
                             agent_id="a1")
    ev = _event()
    out = asyncio.run(_middleware(factory, ev).on_tool_execution(
        _writer(), ev, None))
    assert isinstance(out, ToolResultEvent)
    assert h.workspace.get("a.txt") == b"v1"
    assert h.workspace.version("a.txt") == 1


def test_same_args_dedups_across_call_ids(session):
    seen: list = []
    orig = session.apply

    def spy(*a, **kw):
        r = orig(*a, **kw)
        seen.append(r)
        return r

    session.apply = spy                                   # type: ignore[method-assign]
    factory = rsc_middleware(session, writes={"write_file": ("a.txt",)},
                             agent_id="a1")
    for call_id in ("c1", "c2"):
        ev = _event(call_id=call_id)
        assert isinstance(asyncio.run(
            _middleware(factory, ev).on_tool_execution(_writer(), ev, None)),
            ToolResultEvent)
    assert all(isinstance(r, Receipt) for r in seen), seen
    assert seen[0].receipt_id == seen[1].receipt_id and seen[1].dedup is True
    assert session.h.workspace.version("a.txt") == 1


def test_root_is_excluded_from_idempotency_key(session):
    seen: list = []
    orig = session.apply

    def spy(*a, **kw):
        r = orig(*a, **kw)
        seen.append(r)
        return r

    session.apply = spy                                   # type: ignore[method-assign]
    factory = rsc_middleware(session, writes={"write_file": ("a.txt",)},
                             agent_id="a1")
    for call_id, args in (("c1", '{"root":"/x","value":"v1"}'),
                          ("c2", '{"root":"/y","value":"v1"}')):
        ev = _event(args=args, call_id=call_id)
        assert isinstance(asyncio.run(
            _middleware(factory, ev).on_tool_execution(_writer(), ev, None)),
            ToolResultEvent)
    assert seen[0].receipt_id == seen[1].receipt_id and seen[1].dedup is True
    assert session.h.workspace.version("a.txt") == 1


def test_stale_write_returns_conflict_event(session, tmp_path, repo):
    other = RscSession(session.h, tenant="t", min_budget=1,
                       worktrees=WorktreeManager(str(repo), str(tmp_path / "wt2")),
                       state_dir=str(tmp_path / "state2"))

    def interleave():
        tok = other.begin("other", "race", writes=("a.txt",))
        other.apply(tok.token, [("a.txt", b"other")],
                    idempotency_key="other-1", agent_id="other")

    factory = rsc_middleware(session, writes={"write_file": ("a.txt",)},
                             agent_id="a1")
    ev = _event()
    out = asyncio.run(
        _middleware(factory, ev).on_tool_execution(_writer(interleave), ev, None))
    assert "RSC_CONFLICT" in str(out.result)
    assert session.h.workspace.get("a.txt") == b"other"


def test_budget_exhausted_short_circuits(tmp_path, repo):
    h = make_harness(run_limits=1)
    session = RscSession(h, tenant="t", min_budget=1,
                         worktrees=WorktreeManager(str(repo), str(tmp_path / "wt")),
                         state_dir=str(tmp_path / "state"))
    factory = rsc_middleware(session, writes={"write_file": ("a.txt",)},
                             agent_id="a1")
    first = _event()
    assert isinstance(asyncio.run(
        _middleware(factory, first).on_tool_execution(_writer(), first, None)),
        ToolResultEvent)
    second = _event(args='{"value":"v2"}')
    out = asyncio.run(
        _middleware(factory, second).on_tool_execution(_writer(), second, None))
    assert "RSC_WAIT" in str(out.result)
    assert h.workspace.version("a.txt") == 1


def test_coordinator_unavailable_short_circuits(session):
    class RaftUnavailable(RuntimeError):
        pass

    def boom(*a, **kw):
        raise RaftUnavailable("no quorum")

    session.h.tasks.admit_and_register = boom                  # type: ignore[method-assign]
    factory = rsc_middleware(session, writes={"write_file": ("a.txt",)},
                             agent_id="a1")
    ev = _event()
    out = asyncio.run(
        _middleware(factory, ev).on_tool_execution(_writer(), ev, None))
    assert "RSC_DENY" in str(out.result)


def test_undeclared_tool_passthrough(session):
    factory = rsc_middleware(session, writes={}, agent_id="a1")
    ev = _event(name="search")

    async def call_next(event, context):
        return ToolResultEvent.from_call(event, "ok")

    out = asyncio.run(
        _middleware(factory, ev).on_tool_execution(call_next, ev, None))
    assert isinstance(out, ToolResultEvent)
    assert "ok" in str(out.result)
    assert session.h.workspace.version("a.txt") == 0
