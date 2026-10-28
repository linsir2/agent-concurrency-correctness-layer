"""S24-4：进程内 merge queue（单写者 FIFO，验证与提交串行）。

RIPR：
- Reachability：票真的入队并被 worker 取走（depth/status 变化）；
- Infection：容量满/超时/取消真的改变返回值；队首重验能拦住语义冲突；
- Propagation：`Wait(QUEUE_FULL/QUEUE_TIMEOUT/QUEUE_CANCELLED)` 宿主可见；
- Revealability：账本版本、收据、队列状态可断言。
"""

from __future__ import annotations

import os
import subprocess
import sys

import pytest
from conftest import make_harness

from harness import RscSession
from harness.facade import Deny, Receipt, Wait
from harness.merge_queue import QUEUE_CANCELLED, QUEUE_FULL, QUEUE_TIMEOUT
from harness.worktree import WorktreeManager

CORE = "def compute_rate(x):\n    return x * 2\n"
API = "from app.core import compute_rate\n\n\ndef handler():\n    return compute_rate(3)\n"
FEATURE = "from app.core import compute_rate\n\n\ndef feature():\n    return compute_rate(5)\n"
CHECK = ("import importlib, pkgutil\nimport app\n"
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
    (base / "check_all.py").write_text(CHECK, encoding="utf-8")
    _git(base, "add", ".")
    _git(base, "commit", "-qm", "init")
    return base


_OPEN: list[RscSession] = []


@pytest.fixture(autouse=True)
def _close_sessions():
    yield
    while _OPEN:
        _OPEN.pop().close()


def _session(tmp_path, repo, **kw):
    h = make_harness(run_limits=100)
    session = RscSession(h, tenant="t", min_budget=1,
                         worktrees=WorktreeManager(str(repo), str(tmp_path / "wt")),
                         state_dir=str(tmp_path / "state"), **kw)
    _OPEN.append(session)
    return session, h


def _write(root, rel, text):
    path = os.path.join(root, rel)
    os.makedirs(os.path.dirname(path) or root, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(text)


def test_apply_queued_commits(tmp_path, repo):
    session, h = _session(tmp_path, repo, merge_queue={})
    tok = session.begin("a1", "i", writes=["app/feature.py"])
    _write(tok.worktree, "app/feature.py", "X = 1\n")
    r = session.apply_queued(tok.token, [("app/feature.py", "X = 1\n")],
                             idempotency_key="k1", agent_id="a1")
    assert isinstance(r, Receipt)
    assert h.workspace.version("app/feature.py") == 1
    assert session.queue_depth() == 0


def test_default_off_falls_back_to_sync_apply(tmp_path, repo):
    session, h = _session(tmp_path, repo)
    tok = session.begin("a1", "i", writes=["app/feature.py"])
    _write(tok.worktree, "app/feature.py", "X = 1\n")
    r = session.apply_queued(tok.token, [("app/feature.py", "X = 1\n")],
                             idempotency_key="k1", agent_id="a1")
    assert isinstance(r, Receipt) and h.workspace.version("app/feature.py") == 1
    assert session.queue_depth() == 0


def test_bad_params_rejected_before_enqueue(tmp_path, repo):
    session, _ = _session(tmp_path, repo, merge_queue={})
    tok = session.begin("a1", "i", writes=["app/feature.py"])
    assert isinstance(session.enqueue_queued(tok.token, [], idempotency_key="k"),
                      Deny)
    assert isinstance(session.enqueue_queued(tok.token, [("app/feature.py", "x")],
                                             idempotency_key="k", cost=0), Deny)
    assert session.queue_depth() == 0


def test_queue_capacity_returns_wait(tmp_path, repo):
    session, _ = _session(tmp_path, repo, merge_queue={"capacity": 1})
    import threading
    started = threading.Event()
    release = threading.Event()

    def blocker(ticket):
        started.set()
        release.wait(5)
        return Wait("TEST")

    session._queue._runner = blocker                  # 模拟一个占住 worker 的长任务
    tok1 = session.begin("a1", "i1", writes=["app/feature.py"])
    tok2 = session.begin("a2", "i2", writes=["app/feature.py"])
    t1 = session.enqueue_queued(tok1.token, [("app/feature.py", "a")],
                                idempotency_key="k1", agent_id="a1")
    assert started.wait(2)
    t2 = session.enqueue_queued(tok2.token, [("app/feature.py", "b")],
                                idempotency_key="k2", agent_id="a2")
    assert isinstance(t2, Wait) and t2.reason == QUEUE_FULL
    release.set()
    assert isinstance(session.wait_queued(t1), Wait)


def test_timeout_returns_wait(tmp_path, repo):
    session, _ = _session(tmp_path, repo, merge_queue={})
    import threading
    started = threading.Event()
    release = threading.Event()

    def blocker(ticket):
        started.set()
        release.wait(5)
        return Receipt("x", 0)

    session._queue._runner = blocker
    tok = session.begin("a1", "i", writes=["app/feature.py"])
    t = session.enqueue_queued(tok.token, [("app/feature.py", "x")],
                               idempotency_key="k", agent_id="a1")
    assert started.wait(2)
    out = session.wait_queued(t, timeout_ms=30)
    assert isinstance(out, Wait) and out.reason == QUEUE_TIMEOUT
    release.set()


def test_cancel_pending_ticket(tmp_path, repo):
    session, _ = _session(tmp_path, repo, merge_queue={})
    import threading
    started = threading.Event()
    release = threading.Event()

    def blocker(ticket):
        started.set()
        release.wait(5)
        return Wait("TEST")

    session._queue._runner = blocker
    tok1 = session.begin("a1", "i1", writes=["app/feature.py"])
    tok2 = session.begin("a2", "i2", writes=["app/feature.py"])
    t1 = session.enqueue_queued(tok1.token, [("app/feature.py", "a")],
                                idempotency_key="k1", agent_id="a1")
    assert started.wait(2)
    t2 = session.enqueue_queued(tok2.token, [("app/feature.py", "b")],
                                idempotency_key="k2", agent_id="a2")
    assert session.queue_cancel(t2.ticket_id) is True
    assert session.wait_queued(t2).reason == QUEUE_CANCELLED
    assert session.queue_status(t2.ticket_id)["state"] == "CANCELLED"
    release.set()
    session.wait_queued(t1)


def test_head_reverify_blocks_semantic_conflict(tmp_path, repo):
    """核心价值：B 排在 A 后面出队 → 在 A 已提交的账本上重验 → 被拦。"""
    session, h = _session(tmp_path, repo, merge_queue={},
              verify_cmd=[sys.executable, "check_all.py"])
    renamed = CORE.replace("compute_rate", "compute_rate_v2")
    fixed_api = API.replace("compute_rate", "compute_rate_v2")

    ta = session.begin("agent-a", "rename",
                       writes=["app/core.py", "app/api.py"])
    _write(ta.worktree, "app/core.py", renamed)
    _write(ta.worktree, "app/api.py", fixed_api)
    ticket_a = session.enqueue_queued(
        ta.token, [("app/core.py", renamed), ("app/api.py", fixed_api)],
        idempotency_key="a:1", agent_id="agent-a")

    tb = session.begin("agent-b", "feature", writes=["app/feature.py"])
    _write(tb.worktree, "app/feature.py", FEATURE)
    ticket_b = session.enqueue_queued(tb.token, [("app/feature.py", FEATURE)],
                                      idempotency_key="b:1", agent_id="agent-b")

    ra = session.wait_queued(ticket_a)
    rb = session.wait_queued(ticket_b)
    assert isinstance(ra, Receipt)
    assert isinstance(rb, Deny) and rb.code == "verify_failed"
    assert h.workspace.version("app/feature.py") == 0
    assert h.workspace.version("app/core.py") == 1
    assert session.queue_depth() == 0


def test_close_cancels_pending(tmp_path, repo):
    session, _ = _session(tmp_path, repo, merge_queue={})
    import threading
    started = threading.Event()
    release = threading.Event()

    def blocker(ticket):
        started.set()
        release.wait(5)
        return Wait("TEST")

    session._queue._runner = blocker
    tok1 = session.begin("a1", "i1", writes=["app/feature.py"])
    tok2 = session.begin("a2", "i2", writes=["app/feature.py"])
    t1 = session.enqueue_queued(tok1.token, [("app/feature.py", "a")],
                                idempotency_key="k1", agent_id="a1")
    assert started.wait(2)
    t2 = session.enqueue_queued(tok2.token, [("app/feature.py", "b")],
                                idempotency_key="k2", agent_id="a2")
    session.close()
    assert session.wait_queued(t2).reason == QUEUE_CANCELLED
    release.set()
    session.wait_queued(t1)


def test_fifo_order(tmp_path, repo):
    session, h = _session(tmp_path, repo, merge_queue={})
    order: list[str] = []
    orig = session._run_queued_ticket

    def spy(ticket):
        order.append(ticket.agent_id)
        return orig(ticket)

    session._queue._runner = spy
    t1 = session.begin("a1", "i1", writes=["app/feature.py"])
    t2 = session.begin("a2", "i2", writes=["app/api.py"])
    _write(t1.worktree, "app/feature.py", "A = 1\n")
    _write(t2.worktree, "app/api.py", "B = 1\n")
    k1 = session.enqueue_queued(t1.token, [("app/feature.py", "A = 1\n")],
                                idempotency_key="k1", agent_id="a1")
    k2 = session.enqueue_queued(t2.token, [("app/api.py", "B = 1\n")],
                                idempotency_key="k2", agent_id="a2")
    assert isinstance(session.wait_queued(k1), Receipt)
    assert isinstance(session.wait_queued(k2), Receipt)
    assert order == ["a1", "a2"]
    assert h.workspace.version("app/feature.py") == 1
    assert h.workspace.version("app/api.py") == 1
