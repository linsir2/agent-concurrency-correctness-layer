#!/usr/bin/env python3
"""S24 实验：文本不冲突 ≠ 语义不冲突 —— 并演示 `verify_cmd` 门禁能拦住。

同一仓库，两个 agent 各改「互不重叠」的文件：

- agent-A：把 `compute_rate` 改名为 `compute_rate_v2`，**同时**改掉自己的调用方
  `app/api.py`（所以 A 单独提交是绿的）；
- agent-B：新增 `app/feature.py`，里面**调用旧名字**（与 A 完全不重叠）。

四段对照：
1. `git`：两分支合并干净（无冲突标记），但 `check_all.py` 直接 ImportError；
2. `rsc`：两个 agent 并发提交 → **都是 Receipt、没有 Conflict**（资源不重叠）；
3. `gate`：`verify_cmd` 在「合并视图」上跑 → B 被 `Deny(verify_failed)` 拦住；
4. `green`：B 改用新名字 → 通过，收据 `verified=True`。

用法（只需 git，无需 API key）：
    python3 examples/semantic_conflict/run.py
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))))

from harness import Harness, RscSession
from harness.facade import Receipt, Token
from harness.worktree import WorktreeManager

CORE = "def compute_rate(x):\n    return x * 2\n"
API = "from app.core import compute_rate\n\n\ndef handler():\n    return compute_rate(3)\n"
FEATURE = "from app.core import compute_rate\n\n\ndef feature():\n    return compute_rate(5)\n"
CHECK = ("import importlib, pkgutil\nimport app\n"
         "for m in pkgutil.iter_modules(app.__path__):\n"
         "    importlib.import_module(f'app.{m.name}')\n")

RENAMED_CORE = CORE.replace("compute_rate", "compute_rate_v2")
RENAMED_API = API.replace("compute_rate", "compute_rate_v2")


def _git(repo: str, *args: str, check: bool = True):
    return subprocess.run(["git", "-C", repo, *args], capture_output=True,
                          text=True, check=check)


def _write(root: str, rel: str, text: str) -> None:
    path = os.path.join(root, rel)
    os.makedirs(os.path.dirname(path) or root, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(text)


def _make_repo(root: str) -> str:
    repo = os.path.join(root, "repo")
    os.makedirs(repo)
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "demo@example.com")
    _git(repo, "config", "user.name", "demo")
    _write(repo, "app/__init__.py", "")
    _write(repo, "app/core.py", CORE)
    _write(repo, "app/api.py", API)
    _write(repo, "check_all.py", CHECK)
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "init")
    return repo


def _check(repo: str) -> tuple[bool, str]:
    proc = subprocess.run([sys.executable, "check_all.py"], cwd=repo,
                          capture_output=True, text=True, timeout=120,
                          check=False)
    tail = ((proc.stdout or "") + (proc.stderr or "")).strip().splitlines()
    return proc.returncode == 0, " / ".join(tail[-2:])[:150]


# ---------------------------------------------------------------- 1) 纯 git
def git_scenario(root: str) -> dict:
    repo = _make_repo(root)
    ok0, _ = _check(repo)

    wt_a = os.path.join(root, "wt-a")
    _git(repo, "worktree", "add", "-q", "-b", "a", wt_a)
    _write(wt_a, "app/core.py", RENAMED_CORE)
    _write(wt_a, "app/api.py", RENAMED_API)
    _git(wt_a, "commit", "-qam", "rename")

    wt_b = os.path.join(root, "wt-b")
    _git(repo, "worktree", "add", "-q", "-b", "b", wt_b)
    _write(wt_b, "app/feature.py", FEATURE)
    _git(wt_b, "add", ".")
    _git(wt_b, "commit", "-qm", "feature")

    _git(repo, "merge", "-q", "--no-edit", "a")
    merge_b = _git(repo, "merge", "--no-edit", "b", check=False)
    ok1, tail = _check(repo)
    return {
        "baseline_green": ok0,
        "merge_clean": merge_b.returncode == 0,
        "merge_output": (merge_b.stdout or merge_b.stderr).strip().splitlines()[:2],
        "green": ok1,
        "tail": tail,
    }


# ---------------------------------------------------------------- 2) 我们的层
def _session(root: str, **kw) -> tuple[RscSession, Harness]:
    repo = _make_repo(root)
    h = Harness(run_limits=100, egress_required=False,
                checkpoint_root=os.path.join(root, "ckpt"))
    session = RscSession(h, tenant="demo", min_budget=1,
                         worktrees=WorktreeManager(repo, os.path.join(root, "wt")),
                         state_dir=os.path.join(root, "state"), **kw)
    return session, h


def _commit_a(session) -> Receipt:
    ta = session.begin("agent-a", "rename",
                       writes=["app/core.py", "app/api.py"])
    assert isinstance(ta, Token)
    _write(ta.worktree, "app/core.py", RENAMED_CORE)
    _write(ta.worktree, "app/api.py", RENAMED_API)
    ra = session.apply(ta.token,
                       [("app/core.py", RENAMED_CORE),
                        ("app/api.py", RENAMED_API)],
                       idempotency_key="a:1", agent_id="agent-a")
    assert isinstance(ra, Receipt), ra
    session.finish(ta.token)
    return ra


def rsc_scenario(root: str) -> dict:
    session, _h = _session(root)
    ra = _commit_a(session)
    tb = session.begin("agent-b", "feature", writes=["app/feature.py"])
    rb = session.apply(tb.token, [("app/feature.py", FEATURE)],
                       idempotency_key="b:1", agent_id="agent-b")
    session.finish(tb.token)
    return {"a": type(ra).__name__, "b": type(rb).__name__,
            "b_verified": getattr(rb, "verified", None)}


def gate_scenario(root: str) -> dict:
    """配置 verify_cmd：B 的提交会在合并视图上跑检查 → 红 → Deny。"""
    session, _h = _session(root, verify_cmd=[sys.executable, "check_all.py"])
    ra = _commit_a(session)
    tb = session.begin("agent-b", "feature", writes=["app/feature.py"])
    rb = session.apply(tb.token, [("app/feature.py", FEATURE)],
                       idempotency_key="b:1", agent_id="agent-b")
    session.abort(tb.token, "verify_failed")
    return {"a": type(ra).__name__, "b": type(rb).__name__,
            "b_reason": getattr(rb, "reason", ""),
            "b_code": getattr(rb, "code", "")}


def static_scenario(root: str) -> dict:
    """不跑任何代码：`semantic="block"` 静态查出「符号消失仍被引用」。"""
    session, _h = _session(root, semantic="block")
    _commit_a(session)
    tb = session.begin("agent-b", "feature", writes=["app/feature.py"])
    _write(tb.worktree, "app/feature.py", FEATURE)
    rb = session.apply(tb.token, [("app/feature.py", FEATURE)],
                       idempotency_key="b:1", agent_id="agent-b")
    session.abort(tb.token, "semantic_conflict")
    return {"b": type(rb).__name__, "code": getattr(rb, "code", ""),
            "reason": getattr(rb, "reason", "")}


def green_scenario(root: str) -> dict:
    session, _h = _session(root, verify_cmd=[sys.executable, "check_all.py"])
    _commit_a(session)
    fixed = FEATURE.replace("compute_rate", "compute_rate_v2")
    tb = session.begin("agent-b", "feature", writes=["app/feature.py"])
    _write(tb.worktree, "app/feature.py", fixed)
    rb = session.apply(tb.token, [("app/feature.py", fixed)],
                       idempotency_key="b:1", agent_id="agent-b")
    session.finish(tb.token)
    return {"b": type(rb).__name__, "b_verified": getattr(rb, "verified", None)}


def main() -> int:
    root = tempfile.mkdtemp(prefix="rsc-sem-")
    print("=" * 76)
    print("场景：A 改名+改自己调用方（可独立通过）｜B 新增文件仍调用旧名字（与 A 不重叠）")
    print("=" * 76)

    g = git_scenario(os.path.join(root, "git"))
    print("\n[1] 纯 git：worktree 隔离 + merge")
    print(f"    基线绿={g['baseline_green']}  合并干净={g['merge_clean']}  "
          f"合并后绿={g['green']}")
    print(f"    合并输出：{g['merge_output']}")
    print(f"    检查：{g['tail']}")

    r = rsc_scenario(os.path.join(root, "rsc"))
    print("\n[2] 没有 verify_cmd：两个 agent 并发提交")
    print(f"    A={r['a']}  B={r['b']}   B.verified={r['b_verified']}")
    print("    → 资源不重叠，两张收据都签发；合起来是坏的（这就是缺口）")

    q = gate_scenario(os.path.join(root, "gate"))
    print("\n[3] 配置 verify_cmd：B 的提交在「合并视图」上先跑检查")
    print(f"    A={q['a']}  B={q['b']}  code={q['b_code']}  reason={q['b_reason']}")
    print("    → 红 → 拒绝提交（账本不被污染）")

    st = static_scenario(os.path.join(root, "static"))
    print("\n[3b] 静态语义检查（semantic=\"block\"，不执行任何代码）")
    print(f"    B={st['b']}  code={st['code']}  reason={st['reason']}")
    print("    → 改名导致符号消失 → 新引用被拒（比跑测试更早、更便宜）")

    gr = green_scenario(os.path.join(root, "green"))
    print("\n[4] B 改用新名字")
    print(f"    B={gr['b']}  verified={gr['b_verified']}  → 放行")

    print("\n" + "=" * 76)
    blocked = (g["merge_clean"] and not g["green"]
               and r["a"] == "Receipt" and r["b"] == "Receipt"
               and q["b"] == "Deny" and q["b_code"] == "verify_failed"
               and st["b"] == "Deny" and st["code"] == "semantic_conflict"
               and gr["b"] == "Receipt" and gr["b_verified"] is True)
    print("[结论] " + ("git 与我方 CAS 都不报冲突；静态语义检查（不执行代码）"
                       "先拦住「符号消失仍被引用」，`verify_cmd` 再兜底「跑不通」。"
                       if blocked else "[FAIL] 实验前提不成立，请检查环境"))
    return 0 if blocked else 1


if __name__ == "__main__":
    sys.exit(main())
