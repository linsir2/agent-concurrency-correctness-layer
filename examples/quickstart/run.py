#!/usr/bin/env python3
"""四个真实场景的端到端样例（**无需 API key、无需网络**，只要 git）。

- S1 并行 coding agents：git 合并干净，但合并后 import 失败 → 语义/验证门禁拦住
- S2 同一逻辑实体并发改：后写者拿到 Conflict（不静默覆盖），重读后再提交
- S3 长任务崩溃续跑：同幂等键重放 → `dedup=True`，版本不重复推进
- S4 多租户配额：准入失败是 `Wait`（排队），不是继续写

运行：

    python3 examples/quickstart/run.py

每个场景都打印可核对的证据，并断言结论；任何一条不成立即以非 0 退出。
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))))

from harness import Harness, RscSession
from harness.ergonomics import (
    RscDeferred,
    RscToolDenied,
    last_receipt,
    rsc_session,
    rsc_task,
    rsc_tool,
)
from harness.facade import Receipt
from harness.worktree import WorktreeManager

CORE = "def compute_rate(x):\n    return x * 2\n"
API = "from app.core import compute_rate\n\n\ndef handler():\n    return compute_rate(3)\n"
FEATURE_OLD = "from app.core import compute_rate\n\n\ndef feature():\n    return compute_rate(5)\n"
FEATURE_FIXED = FEATURE_OLD.replace("compute_rate", "compute_rate_v2")
CHECK = [sys.executable, "check.py"]
CHECK_PY = ("import importlib, pkgutil\nimport app\n"
            "for m in pkgutil.iter_modules(app.__path__):\n"
            "    importlib.import_module(f'app.{m.name}')\n")


def _git(repo: str, *args: str) -> None:
    subprocess.run(["git", "-C", repo, *args], check=True,
                   capture_output=True, text=True)


def _make_repo(root: str) -> str:
    repo = os.path.join(root, "repo")
    os.makedirs(os.path.join(repo, "app"))
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "demo@example.com")
    _git(repo, "config", "user.name", "demo")
    for rel, text in (("app/__init__.py", ""), ("app/core.py", CORE),
                      ("app/api.py", API), ("check.py", CHECK_PY),
                      ("pyproject.toml", "[tool]\n")):
        with open(os.path.join(repo, rel), "w", encoding="utf-8") as fh:
            fh.write(text)
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "init")
    return repo


def _session(root: str, *, repo: str | None = None, **kw):
    ckpt = os.path.join(root, "ckpt")
    h = Harness(run_limits=kw.pop("run_limits", 100), checkpoint_root=ckpt,
                egress_required=False)
    worktrees = (WorktreeManager(repo, os.path.join(root, "wt"))
                 if repo else None)
    session = RscSession(h, tenant="demo", min_budget=1, worktrees=worktrees,
                         state_dir=os.path.join(root, "state"), **kw)
    return session, h


# ---------------------------------------------------------------- S1
def scenario_s1(root: str) -> dict:
    """并行 coding agents：改不同文件，git 干净，但合并后跑不通。"""
    repo = _make_repo(root)
    session, h = _session(root, repo=repo, semantic="block",
                          verify_cmd=CHECK)

    @rsc_tool(writes=["app/core.py", "app/api.py"], verify=[CHECK])
    def rename_api(ctx):
        ctx.write("app/core.py", CORE.replace("compute_rate", "compute_rate_v2"))
        ctx.write("app/api.py",
                  API.replace("compute_rate", "compute_rate_v2"))

    @rsc_tool(writes=["app/feature.py"])
    def add_feature_static(ctx, text):
        ctx.write("app/feature.py", text)

    @rsc_tool(writes=["app/feature.py"], verify=[CHECK])
    def add_feature_dynamic(ctx, text):
        ctx.write("app/feature.py", text)

    with rsc_session(session, "agent-a"):
        rename_api()                     # A 单独提交是绿的（自己的调用方也改了）
    assert h.workspace.version("app/core.py") == 1

    with rsc_session(session, "agent-b"):
        # ① 静态：不执行代码，只看符号表就能拦
        try:
            add_feature_static(FEATURE_OLD)
            static_code = "NOT_BLOCKED"
        except RscToolDenied as exc:
            static_code = exc.code
        assert static_code == "semantic_conflict", static_code
        assert h.workspace.version("app/feature.py") == 0

        # ② 动态：真去合并视图里跑 import，也能拦（双保险）
        try:
            add_feature_dynamic(FEATURE_OLD)
            dynamic_code = "NOT_BLOCKED"
        except RscToolDenied as exc:
            dynamic_code = exc.code
        assert dynamic_code == "verify_failed", dynamic_code
        assert h.workspace.version("app/feature.py") == 0

        add_feature_dynamic(FEATURE_FIXED)       # 改成新名字 → 通过
    assert h.workspace.version("app/feature.py") == 1

    print("  A 改名 → 账本 core.py v1（A 自己的调用方也改了，所以 A 是绿的）")
    print(f"  B 用旧名字 → 静态拒绝 {static_code}；动态拒绝 {dynamic_code}")
    print("  B 改用新名字 → 通过，收据 verified="
          f"{bool(last_receipt() and last_receipt().verified)}")
    print("  两个 agent 改的文件不重叠，git 合并不会报冲突；"
          "但只有门禁能发现「合并后 import 失败」")
    return {"static": static_code, "dynamic": dynamic_code,
            "feature_version": 1}


# ---------------------------------------------------------------- S2
def scenario_s2(root: str) -> dict:
    """同一逻辑实体（不是文件）并发改：后写者 Conflict，重读后合并。"""
    session, h = _session(root, repo=None)
    resource = "svc/payments/config"

    @rsc_tool(writes=[resource], retry=0)
    def patch(ctx, patch_dict):
        current = json.loads(ctx.read(resource) or b"{}")
        current.update(patch_dict)
        ctx.write(resource, json.dumps(current, sort_keys=True))

    @rsc_tool(writes=[resource], retry=1)
    def patch_with_retry(ctx, patch_dict):
        current = json.loads(ctx.read(resource) or b"{}")
        current.update(patch_dict)
        ctx.write(resource, json.dumps(current, sort_keys=True))

    with rsc_session(session, "arch"):
        patch({"replicas": 2})                       # v1
    assert h.workspace.version(resource) == 1

    with rsc_session(session, "api"):
        patch({"cpu": "500m"})                       # v2
    assert h.workspace.version(resource) == 2

    # 并发：B 在 A 提交前就钉住了旧版本 → CAS 拒绝（不静默覆盖）
    state = {"committed": False}

    @rsc_tool(writes=[resource], retry=1)
    def racing_patch(ctx, patch_dict):
        if not state["committed"]:
            # 模拟「B 读完配置之后，A 抢先提交」
            state["committed"] = True
            with rsc_session(session, "arch"):
                patch({"replicas": 3})
        current = json.loads(ctx.read(resource) or b"{}")
        current.update(patch_dict)
        ctx.write(resource, json.dumps(current, sort_keys=True))

    with rsc_session(session, "api"):
        racing_patch({"timeout": "30s"})

    final = json.loads(h.workspace.get(resource))
    assert final["replicas"] == 3 and final["timeout"] == "30s", final
    print(f"  资源 {resource} 最终 {json.dumps(final, sort_keys=True)}")
    print("  B 第一次 CAS 被拒 → 自动重读最新版本 → 合并提交（replicas 没被回退）")
    return {"final": final, "version": h.workspace.version(resource)}


# ---------------------------------------------------------------- S3
def scenario_s3(root: str) -> dict:
    """长任务崩溃续跑：同幂等键重放，版本不重复推进。"""
    session, h = _session(root, repo=None)
    resource = "orders/42"

    def stage(key: str, content: str):
        """模拟一个「写入后进程被杀、来不及 finish」的步骤。"""
        with rsc_task(session, "worker", writes=[resource]) as task:
            task.write(resource, content)
            receipt = task.apply(key=key)
        return receipt

    first = stage("import:step-1", '{"status":"paid"}')
    assert isinstance(first, Receipt) and first.dedup is False
    after_first = h.workspace.version(resource)

    # 进程重启：重新执行同一步（同键 + 同内容）
    replay = stage("import:step-1", '{"status":"paid"}')
    assert isinstance(replay, Receipt) and replay.dedup is True
    assert replay.receipt_id == first.receipt_id
    assert h.workspace.version(resource) == after_first

    print(f"  首次提交 → version={after_first}，receipt={first.receipt_id[:12]}")
    print(f"  重启后重放同键 → dedup=True，receipt 相同，"
          f"version 仍为 {h.workspace.version(resource)}")
    print("  checkpoint/WAL 在 "
          f"{os.path.join(root, 'ckpt')}（可用 rsc-diagnose 体检）")
    return {"version": h.workspace.version(resource),
            "dedup": replay.dedup, "receipt": first.receipt_id}


# ---------------------------------------------------------------- S4
def scenario_s4(root: str) -> dict:
    """多租户配额：准入失败返回 Wait（排队），不会继续写。"""
    session, h = _session(root, repo=None, run_limits=1)
    resource = "tickets/7"

    @rsc_tool(writes=[resource])
    def touch(ctx, value):
        ctx.write(resource, value)

    with rsc_session(session, "agent-a"):
        touch("v1")

    deferred = None
    with rsc_session(session, "agent-b"):
        try:
            touch("v2")
        except RscDeferred as exc:
            deferred = exc.reason
    assert deferred == "BUDGET_EXHAUSTED", deferred
    assert h.workspace.version(resource) == 1

    print(f"  第二个 agent 被准入拦下：Wait({deferred})")
    print(f"  账本未被写入：version 仍为 {h.workspace.version(resource)}")
    return {"deferred": deferred, "version": 1}


# ---------------------------------------------------------------- main
def main() -> int:
    with tempfile.TemporaryDirectory(prefix="rsc-quickstart-") as tmp:
        results = {}
        for name, fn in (("S1 并行 coding agents", scenario_s1),
                         ("S2 同一逻辑实体并发改", scenario_s2),
                         ("S3 长任务崩溃续跑", scenario_s3),
                         ("S4 多租户配额", scenario_s4)):
            print(f"\n[{name}]")
            results[name] = fn(os.path.join(tmp, name.split()[0]))
        print("\n全部场景通过：", ", ".join(results))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
