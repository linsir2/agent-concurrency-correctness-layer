#!/usr/bin/env python3
"""S24 调研实验：文本不冲突 ≠ 语义不冲突，而我们的层目前**拦不住**后者。

实验设计（同一仓库，两个 agent 各改一处「互不重叠」的代码）：

- agent-A：把 `app/core.py` 的 `compute_rate` 改名为 `compute_rate_v2`；
- agent-B：新增 `app/feature.py`，里面**调用旧名字** `compute_rate`。

两者改的是不同文件 → git 合并干净（无冲突标记）；我们的 RSC 层也会给**两张收据**
（不同资源、各自 CAS 成功）。但合起来 `import` 就炸：**测试红了**。

三段对照：
1. `git`：worktree 隔离 + merge → 干净合并，但 `pytest` 失败；
2. `rsc`：两个 agent 走六动作门面并发提交 → 都是 `Receipt`，**没有 Conflict**；
3. `gate`：在「提交前」于合并结果上跑测试（约 20 行的 verifier 雏形）→ 红 → 应拒绝。

用法（只需 git + pytest）：
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
from harness.facade import Conflict, Receipt, Token
from harness.worktree import WorktreeManager

CORE = "def compute_rate(x):\n    return x * 2\n"
API = ("from app.core import compute_rate\n\n\n"
       "def handler():\n    return compute_rate(3)\n")
TEST = ("from app.api import handler\n\n\n"
        "def test_handler():\n    assert handler() == 6\n")
FEATURE = ("from app.core import compute_rate\n\n\n"
           "def feature():\n    return compute_rate(5)\n")


def _git(repo: str, *args: str, check: bool = True):
    return subprocess.run(["git", "-C", repo, *args], capture_output=True,
                          text=True, check=check)


def _write(root: str, rel: str, text: str) -> None:
    path = os.path.join(root, rel)
    os.makedirs(os.path.dirname(path), exist_ok=True)
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
    _write(repo, "tests/test_api.py", TEST)
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "init")
    return repo


def _pytest(repo: str) -> tuple[bool, str]:
    proc = subprocess.run([sys.executable, "-m", "pytest", "-q"], cwd=repo,
                          capture_output=True, text=True, timeout=120,
                          check=False)
    tail = (proc.stdout or proc.stderr).strip().splitlines()
    return proc.returncode == 0, " / ".join(tail[-2:])[:180]


# ---------------------------------------------------------------- 1) 纯 git
def git_scenario(root: str) -> dict:
    """两个 worktree 各改一处 → merge 干净；测试却失败。"""
    repo = _make_repo(root)
    ok0, _ = _pytest(repo)

    # agent-A：改名（只动 app/core.py）
    wt_a = os.path.join(root, "wt-a")
    _git(repo, "worktree", "add", "-q", "-b", "a", wt_a)
    _write(wt_a, "app/core.py", CORE.replace("compute_rate", "compute_rate_v2"))
    _git(wt_a, "commit", "-qam", "rename")

    # agent-B：新增文件（只动 app/feature.py），仍调用旧名字
    wt_b = os.path.join(root, "wt-b")
    _git(repo, "worktree", "add", "-q", "-b", "b", wt_b)
    _write(wt_b, "app/feature.py", FEATURE)
    _git(wt_b, "add", ".")
    _git(wt_b, "commit", "-qm", "feature")

    _git(repo, "merge", "-q", "--no-edit", "a")
    merge_b = _git(repo, "merge", "--no-edit", "b", check=False)
    ok1, tail = _pytest(repo)
    return {
        "baseline_green": ok0,
        "merge_clean": merge_b.returncode == 0,
        "merge_output": (merge_b.stdout or merge_b.stderr).strip().splitlines()[:2],
        "tests_green": ok1,
        "pytest_tail": tail,
    }


# ---------------------------------------------------------------- 2) 我们的层
def rsc_scenario(root: str) -> dict:
    """两个 agent 改**不同资源** → 各自 Receipt，没有 Conflict。"""
    repo = _make_repo(root)
    h = Harness(run_limits=100, egress_required=False,
                checkpoint_root=os.path.join(root, "ckpt"))
    session = RscSession(h, tenant="demo", min_budget=1,
                         worktrees=WorktreeManager(repo, os.path.join(root, "wt")),
                         state_dir=os.path.join(root, "state"))

    t1 = session.begin("agent-a", "rename", writes=["app/core.py"])
    t2 = session.begin("agent-b", "feature", writes=["app/feature.py"])
    assert isinstance(t1, Token) and isinstance(t2, Token)

    renamed = CORE.replace("compute_rate", "compute_rate_v2").encode()
    r1 = session.apply(t1.token, [("app/core.py", renamed)],
                       idempotency_key="a:1", agent_id="agent-a")
    r2 = session.apply(t2.token, [("app/feature.py", FEATURE.encode())],
                       idempotency_key="b:1", agent_id="agent-b")
    for tok in (t1.token, t2.token):
        session.finish(tok)

    # 模拟「两边都合进主干」：把账本里的两份内容落到仓库
    _write(repo, "app/core.py", renamed.decode())
    _write(repo, "app/feature.py", FEATURE)
    ok, tail = _pytest(repo)
    return {
        "r1": type(r1).__name__, "r2": type(r2).__name__,
        "conflict": isinstance(r1, Conflict) or isinstance(r2, Conflict),
        "receipts": (isinstance(r1, Receipt) and isinstance(r2, Receipt)),
        "tests_green": ok, "pytest_tail": tail,
    }


# ---------------------------------------------------------------- 3) 门禁雏形
def verify_gate(repo: str, rel: str, content: str,
                cmd: tuple[str, ...] = (sys.executable, "-m", "pytest", "-q"),
                timeout_s: int = 120) -> tuple[bool, str]:
    """提交前验证：把候选内容落进**隔离副本**跑命令，红则拒绝（20 行雏形）。

    这是 `VerifierRunner` 的最小可信形状：只读候选结果、超时、不污染主树。
    """
    import shutil
    sandbox = tempfile.mkdtemp(prefix="verify-", dir=os.path.dirname(repo))
    try:
        target = os.path.join(sandbox, "tree")
        shutil.copytree(repo, target, ignore=shutil.ignore_patterns(".git"))
        _write(target, rel, content)
        proc = subprocess.run(list(cmd), cwd=target, capture_output=True,
                              text=True, timeout=timeout_s, check=False)
        tail = (proc.stdout or proc.stderr).strip().splitlines()[-2:]
        return proc.returncode == 0, " / ".join(tail)[:180]
    finally:
        shutil.rmtree(sandbox, ignore_errors=True)


def gate_scenario(root: str) -> dict:
    """agent-A 已合入；agent-B 提交前跑测试 → 红 → 应拒绝。"""
    repo = _make_repo(root)
    _write(repo, "app/core.py", CORE.replace("compute_rate", "compute_rate_v2"))
    ok, tail = verify_gate(repo, "app/feature.py", FEATURE)
    return {"allow": ok, "pytest_tail": tail}


def main() -> int:
    root = tempfile.mkdtemp(prefix="rsc-sem-")
    print("=" * 74)
    print("场景：agent-A 改名（app/core.py）｜agent-B 新增调用旧名字的文件（app/feature.py）")
    print("=" * 74)

    g = git_scenario(os.path.join(root, "git"))
    print("\n[1] 纯 git：worktree 隔离 + merge")
    print(f"    基线测试绿={g['baseline_green']}  合并干净={g['merge_clean']}  "
          f"合并后测试绿={g['tests_green']}")
    print(f"    合并输出：{g['merge_output']}")
    print(f"    pytest：{g['pytest_tail']}")

    r = rsc_scenario(os.path.join(root, "rsc"))
    print("\n[2] 我们的 RSC 层：两个 agent 并发提交（不同资源）")
    print(f"    r1={r['r1']}  r2={r['r2']}  出现冲突={r['conflict']}  "
          f"都拿到收据={r['receipts']}")
    print(f"    两份内容都合入后，测试绿={r['tests_green']}：{r['pytest_tail']}")

    v = gate_scenario(os.path.join(root, "gate"))
    print("\n[3] 门禁雏形：提交前在隔离副本跑测试")
    print(f"    允许提交={v['allow']}（应为 False）  pytest：{v['pytest_tail']}")

    print("\n" + "=" * 74)
    blocked = (g["merge_clean"] and not g["tests_green"]
               and not r["conflict"] and r["receipts"] and not r["tests_green"]
               and not v["allow"])
    print("[结论] " + ("文本合并不报冲突、我们的 CAS 也不报冲突；"
                       "只有「提交前跑测试」能拦住——这正是当前缺的 Verifier 门禁。"
                       if blocked else "[FAIL] 实验前提不成立，请检查环境"))
    return 0 if blocked else 1


if __name__ == "__main__":
    sys.exit(main())
