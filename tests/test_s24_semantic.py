"""S24-3：符号级语义冲突检测（静态，不执行代码）。

单元：`detect(base, post)` 的判定、抑制与误报对照；
集成：`semantic=block|warn|off` 在 `apply` 门禁里的行为。

RIPR：
- Reachability：确实扫到了目标文件（`scanned`/issue 行号与源一致）；
- Infection：改名/删模块真的产生 issue；兼容别名/纯新增不产生；
- Propagation：`block` 下 `apply` 返回 `Deny(semantic_conflict)`，版本不推进；
- Revealability：`spec["rsc_semantic"]` 落盘、detail 含 path/line/name。
"""

from __future__ import annotations

import os
import subprocess
import time

import pytest
from conftest import make_harness

from harness import RscSession
from harness.facade import Deny, Receipt
from harness.rebase import DELETE
from harness.semantic import detect
from harness.worktree import WorktreeManager

CORE = "def compute_rate(x):\n    return x * 2\n"
API = "from app.core import compute_rate\n\n\ndef handler():\n    return compute_rate(3)\n"
FEATURE = "from app.core import compute_rate\n\n\ndef feature():\n    return compute_rate(5)\n"


def _b(text: str) -> bytes:
    return text.encode("utf-8")


# ---------------------------------------------------------------- 单元
def test_rename_with_reference_is_detected():
    base = {"app/core.py": _b(CORE), "app/feature.py": _b(FEATURE)}
    post = {"app/core.py": _b(CORE.replace("compute_rate", "compute_rate_v2")),
            "app/feature.py": _b(FEATURE)}
    report = detect(base, post)
    assert not report.ok
    issue = report.issues[0]
    assert (issue.kind, issue.module, issue.name) == (
        "removed_symbol", "app.core", "compute_rate")
    assert issue.refs[0].path == "app/feature.py"
    assert issue.refs[0].line == 1


def test_compatible_alias_is_not_reported():
    base = {"app/core.py": _b(CORE), "app/feature.py": _b(FEATURE)}
    post = {"app/core.py": _b(CORE + "\ncompute_rate_v2 = compute_rate\n"),
            "app/feature.py": _b(FEATURE)}
    assert detect(base, post).ok


def test_pure_addition_is_not_reported():
    base = {"app/core.py": _b(CORE)}
    post = {"app/core.py": _b(CORE),
            "app/feature.py": _b(FEATURE)}
    assert detect(base, post).ok


def test_pre_existing_bad_reference_is_not_reported():
    """base 里就没有这个符号 → 历史问题，不归我们报。"""
    base = {"app/core.py": _b("X = 1\n"),
            "app/feature.py": _b("from app.core import nope\n")}
    post = {"app/core.py": _b("Y = 1\n"),
            "app/feature.py": _b("from app.core import nope\n")}
    assert detect(base, post).ok


def test_star_import_is_skipped():
    base = {"app/core.py": _b(CORE),
            "app/feature.py": _b("from app.core import *\n\nvalue = compute_rate(1)\n")}
    post = {"app/core.py": _b(CORE.replace("compute_rate", "compute_rate_v2")),
            "app/feature.py": _b("from app.core import *\n\nvalue = compute_rate(1)\n")}
    assert detect(base, post).ok


def test_dynamic_module_is_skipped():
    dyn = CORE + "\n\ndef __getattr__(name):\n    return None\n"
    base = {"app/core.py": _b(dyn), "app/feature.py": _b(FEATURE)}
    post = {"app/core.py": _b(dyn.replace("compute_rate", "compute_rate_v2")),
            "app/feature.py": _b(FEATURE)}
    assert detect(base, post).ok


def test_removed_module_is_detected():
    base = {"app/core.py": _b(CORE), "app/feature.py": _b(FEATURE)}
    post = {"app/feature.py": _b(FEATURE)}
    report = detect(base, post)
    assert not report.ok
    assert report.issues[0].kind == "removed_module"
    assert report.issues[0].module == "app.core"


def test_stdlib_import_is_not_reported():
    base = {"app/feature.py": _b("import os\n\nos.path.join('a', 'b')\n")}
    post = {"app/feature.py": _b("import os\n\nos.path.join('a', 'b')\n"),
            "app/core.py": _b("Z = 1\n")}
    assert detect(base, post).ok


def test_relative_import_is_resolved():
    base = {"app/core.py": _b(CORE),
            "app/feature.py": _b("from .core import compute_rate\n")}
    post = {"app/core.py": _b(CORE.replace("compute_rate", "compute_rate_v2")),
            "app/feature.py": _b("from .core import compute_rate\n")}
    report = detect(base, post)
    assert not report.ok
    assert report.issues[0].module == "app.core"


def test_attribute_reference_is_detected():
    base = {"app/core.py": _b(CORE),
            "app/feature.py": _b("import app.core as c\n\nc.compute_rate(1)\n")}
    post = {"app/core.py": _b(CORE.replace("compute_rate", "compute_rate_v2")),
            "app/feature.py": _b("import app.core as c\n\nc.compute_rate(1)\n")}
    report = detect(base, post)
    assert not report.ok
    assert report.issues[0].refs[0].line == 3


def test_annotation_reference_is_skipped():
    base = {"app/core.py": _b(CORE),
            "app/feature.py": _b("from app.core import compute_rate\n"
                                  "def f(x: compute_rate) -> None:\n    pass\n")}
    post = {"app/core.py": _b(CORE.replace("compute_rate", "compute_rate_v2")),
            "app/feature.py": _b("from app.core import compute_rate\n"
                                  "def f(x: compute_rate) -> None:\n    pass\n")}
    # 导入行仍算引用；但注解本身不额外增加
    report = detect(base, post)
    assert len(report.issues[0].refs) == 1


def test_guarded_and_type_checking_imports_are_skipped():
    guarded = ("try:\n    from app.core import compute_rate\n"
               "except ImportError:\n    compute_rate = None\n")
    base = {"app/core.py": _b(CORE), "app/feature.py": _b(guarded)}
    post = {"app/core.py": _b(CORE.replace("compute_rate", "compute_rate_v2")),
            "app/feature.py": _b(guarded)}
    assert detect(base, post).ok


def test_conditional_definition_is_uncertain_and_not_reported():
    conditional = "import sys\nif sys.version_info > (3,):\n    def compute_rate(x):\n        return x\n"
    base = {"app/core.py": _b(conditional), "app/feature.py": _b(FEATURE)}
    post = {"app/core.py": _b("X = 1\n"), "app/feature.py": _b(FEATURE)}
    assert detect(base, post).ok


def test_ignore_patterns_and_syntax_errors_are_reported_as_skipped():
    base = {"app/core.py": _b(CORE), "app/feature.py": _b(FEATURE)}
    post = {"app/core.py": _b(CORE.replace("compute_rate", "compute_rate_v2")),
            "app/feature.py": _b(FEATURE)}
    assert detect(base, post, ignore=("app/feature.py",)).ok
    broken = dict(post)
    broken["app/broken.py"] = b"def (:\n"
    report = detect(base, broken)
    assert "app/broken.py" in report.skipped


SIG_BASE = "def f(a, b=1):\n    return a + b\n"
SIG_POST = "def f(a):\n    return a\n"
CALLER = "from app.core import f\n\n\ndef run():\n    return f(1, b=2)\n"
CLASS_BASE = "class Cfg:\n    debug = 1\n\n    def use(self):\n        return self.debug\n"
CLASS_POST = "class Cfg:\n\n    def use(self):\n        return 1\n"
CLASS_USER = "from app.core import Cfg\n\n\ndef run():\n    return Cfg().debug\n"


def _pair(base_core: str, post_core: str, user: str):
    return ({"app/core.py": _b(base_core), "app/user.py": _b(user)},
            {"app/core.py": _b(post_core), "app/user.py": _b(user)})


def _kinds(report):
    return {(i.kind, i.module, i.name) for i in report.issues}


def test_signature_removed_keyword_is_detected():
    report = detect(*_pair(SIG_BASE, SIG_POST, CALLER))
    assert ("signature_incompatible", "app.core", "f") in _kinds(report)


def test_signature_added_required_positional_is_detected():
    base = {"app/core.py": _b("def f(a):\n    return a\n"),
            "app/user.py": _b("from app.core import f\n\nf(1)\n")}
    post = {"app/core.py": _b("def f(a, b):\n    return a\n"),
            "app/user.py": _b("from app.core import f\n\nf(1)\n")}
    assert ("signature_incompatible", "app.core", "f") in _kinds(detect(base, post))


def test_signature_added_required_kwonly_is_detected():
    base = {"app/core.py": _b("def f(a):\n    return a\n"),
            "app/user.py": _b("from app.core import f\n\nf(1)\n")}
    post = {"app/core.py": _b("def f(a, *, c):\n    return a\n"),
            "app/user.py": _b("from app.core import f\n\nf(1)\n")}
    assert ("signature_incompatible", "app.core", "f") in _kinds(detect(base, post))


def test_signature_optional_addition_is_not_reported():
    base = {"app/core.py": _b("def f(a):\n    return a\n"),
            "app/user.py": _b("from app.core import f\n\nf(1)\n")}
    post = {"app/core.py": _b("def f(a, b=2):\n    return a\n"),
            "app/user.py": _b("from app.core import f\n\nf(1)\n")}
    assert detect(base, post).ok


def test_signature_star_and_decorated_are_skipped():
    star = ("from app.core import f\n\nargs = (1,)\n"
            "f(*args)\nf(1, **{'b': 2})\n")
    assert detect(*_pair(SIG_BASE, SIG_POST, star)).ok
    decorated = ("from app.core import f\n\nf(1, b=2)\n")
    base = {"app/core.py": _b("@decorator\ndef f(a, b=1):\n    return a\n"),
            "app/user.py": _b(decorated)}
    post = {"app/core.py": _b("@decorator\ndef f(a):\n    return a\n"),
            "app/user.py": _b(decorated)}
    assert detect(base, post).ok


def test_removed_member_call_and_class_attr_are_detected():
    report = detect(*_pair(CLASS_BASE, CLASS_POST, CLASS_USER))
    assert ("removed_member", "app.core", "Cfg.debug") in _kinds(report)
    attr_user = "from app.core import Cfg\n\n\ndef run():\n    return Cfg.debug\n"
    report = detect(*_pair(CLASS_BASE, CLASS_POST, attr_user))
    assert ("removed_member", "app.core", "Cfg.debug") in _kinds(report)


def test_removed_member_local_instance_is_detected():
    user = ("from app.core import Cfg\n\ncfg = Cfg()\n\n\n"
            "def run():\n    return cfg.debug\n")
    report = detect(*_pair(CLASS_BASE, CLASS_POST, user))
    assert ("removed_member", "app.core", "Cfg.debug") in _kinds(report)


def test_removed_member_inherited_or_dynamic_is_skipped():
    user = "from app.core import Cfg\n\n\ndef run():\n    return Cfg().debug\n"
    inherited = "class Base:\n    pass\n\n\nclass Cfg(Base):\n    debug = 1\n"
    assert detect(*_pair(inherited, "class Base:\n    pass\n\n\n"
                         "class Cfg(Base):\n    pass\n", user)).ok
    dynamic = "class Cfg:\n    debug = 1\n\n    def __getattr__(self, n):\n        return 1\n"
    assert detect(*_pair(dynamic, "class Cfg:\n\n    def __getattr__(self, n):\n"
                         "        return 1\n", user)).ok


def test_detection_is_fast_enough():
    """200 文件 / 约 2 万行的上界（本机阈值放宽到 2s，避免 CI 抖动）。"""
    files = {}
    for i in range(200):
        body = "".join(f"def f_{i}_{j}(x):\n    return x + {j}\n\n" for j in range(12))
        files[f"pkg/m{i}.py"] = body.encode()
    started = time.monotonic()
    report = detect(files, dict(files))
    assert report.ok and report.scanned == 200
    assert time.monotonic() - started < 2.0


# ---------------------------------------------------------------- 集成
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
    _git(base, "add", ".")
    _git(base, "commit", "-qm", "init")
    return base


def _session(tmp_path, repo, **kw):
    h = make_harness(run_limits=100)
    return RscSession(h, tenant="t", min_budget=1,
                      worktrees=WorktreeManager(str(repo), str(tmp_path / "wt")),
                      state_dir=str(tmp_path / "state"), **kw), h


def _write(root, rel, text):
    path = os.path.join(root, rel)
    os.makedirs(os.path.dirname(path) or root, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(text)


def _commit_rename(session):
    ta = session.begin("agent-a", "rename",
                       writes=["app/core.py", "app/api.py"])
    renamed = CORE.replace("compute_rate", "compute_rate_v2")
    fixed = API.replace("compute_rate", "compute_rate_v2")
    _write(ta.worktree, "app/core.py", renamed)
    _write(ta.worktree, "app/api.py", fixed)
    r = session.apply(ta.token,
                      [("app/core.py", renamed), ("app/api.py", fixed)],
                      idempotency_key="a:1", agent_id="agent-a")
    assert isinstance(r, Receipt), r
    session.finish(ta.token)


@pytest.mark.parametrize("mode,expect_deny", [("block", True), ("warn", False),
                                              ("off", False)])
def test_gate_modes(tmp_path, repo, mode, expect_deny):
    session, h = _session(tmp_path, repo, semantic=mode)
    _commit_rename(session)

    tb = session.begin("agent-b", "feature", writes=["app/feature.py"])
    _write(tb.worktree, "app/feature.py", FEATURE)
    rb = session.apply(tb.token, [("app/feature.py", FEATURE)],
                       idempotency_key="b:1", agent_id="agent-b")
    if expect_deny:
        assert isinstance(rb, Deny) and rb.code == "semantic_conflict"
        assert "compute_rate" in rb.reason
        assert h.workspace.version("app/feature.py") == 0
        record = h.acbs[tb.token].spec.get("rsc_semantic")
        assert record and record["issues"][0]["module"] == "app.core"
    else:
        assert isinstance(rb, Receipt)
        if mode == "warn":
            record = h.acbs[tb.token].spec.get("rsc_semantic")
            assert record and record["issues"]
        else:
            assert h.acbs[tb.token].spec.get("rsc_semantic") is None


def test_compatible_change_passes_block_mode(tmp_path, repo):
    session, h = _session(tmp_path, repo, semantic="block")
    _commit_rename(session)
    tb = session.begin("agent-b", "feature", writes=["app/feature.py"])
    fixed = FEATURE.replace("compute_rate", "compute_rate_v2")
    _write(tb.worktree, "app/feature.py", fixed)
    rb = session.apply(tb.token, [("app/feature.py", fixed)],
                       idempotency_key="b:1", agent_id="agent-b")
    assert isinstance(rb, Receipt)
    assert h.workspace.version("app/feature.py") == 1


def test_invalid_mode_rejected():
    h = make_harness(run_limits=10)
    with pytest.raises(ValueError):
        RscSession(h, semantic="loud")


def test_check_semantics_is_explicit_and_non_mutating(tmp_path, repo):
    session, h = _session(tmp_path, repo, semantic="off")
    _commit_rename(session)
    tb = session.begin("agent-b", "feature", writes=["app/feature.py"])
    _write(tb.worktree, "app/feature.py", FEATURE)
    res = session.check_semantics(tb.token,
                                  pending=[("app/feature.py", _b(FEATURE))])
    assert res.status.value == "SEMANTIC_REJECTED"
    assert res.detail["issues"][0]["name"] == "compute_rate"
    assert h.workspace.version("app/feature.py") == 0


def test_no_worktree_is_fail_closed(tmp_path):
    h = make_harness(run_limits=10)
    session = RscSession(h, semantic="block", worktrees=None)
    tok = session.begin("a1", "i", writes=["a.py"])
    res = session.check_semantics(tok.token)
    assert res.status.value == "DENIED"
    assert res.detail["reason"] == "semantic_requires_worktree"


def test_deleted_module_is_detected_through_facade(tmp_path, repo):
    """A 删除 app/core.py，B 仍引用 → 门面必须报 removed_module（当前会漏）。"""
    session, h = _session(tmp_path, repo, semantic="block")
    ta = session.begin("agent-a", "delete",
                       writes=["app/core.py", "app/api.py"])
    ra = session.apply(ta.token, [("app/core.py", DELETE),
                                  ("app/api.py", "def handler():\n    return 3\n")],
                       idempotency_key="a:delete", agent_id="agent-a")
    assert isinstance(ra, Receipt), ra
    session.finish(ta.token)
    assert h.workspace.deleted_resources() == ["app/core.py"]

    tb = session.begin("agent-b", "feature", writes=["app/feature.py"])
    _write(tb.worktree, "app/feature.py", FEATURE)
    rb = session.apply(tb.token, [("app/feature.py", FEATURE)],
                       idempotency_key="b:1", agent_id="agent-b")
    assert isinstance(rb, Deny) and rb.code == "semantic_conflict", rb
    issues = h.acbs[tb.token].spec["rsc_semantic"]["issues"]
    assert issues[0]["kind"] == "removed_module"
    assert issues[0]["module"] == "app.core"
