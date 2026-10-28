#!/usr/bin/env python3
"""S24 方案 ↔ 实现 一致性核对（可执行）。

用法：`python3 scripts/check_s24_conformance.py`（exit 0 = 全部一致）。
对应 `docs/s24-gap-analysis-and-repositioning.md` §5（S24-1 Verifier 门禁）。
"""

from __future__ import annotations

import inspect
import pathlib
import sys
import tempfile

_ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "tests"))

ok: list[str] = []
bad: list[tuple[str, str]] = []


def chk(name: str, cond: bool, detail: str = "") -> None:
    (ok if cond else bad).append((name, detail))


def _src(rel: str) -> str:
    return (_ROOT / rel).read_text(encoding="utf-8")


from conftest import make_harness

from harness import RscSession
from harness.facade import Deny, Receipt

# ---------------------------------------------------------------- 交付物
for rel in ("examples/semantic_conflict/run.py",
            "tests/test_s24_verify.py",
            "tests/test_s24_experiment.py",
            "docs/s24-gap-analysis-and-repositioning.md"):
    chk(f"交付存在 {rel}", (_ROOT / rel).exists())

# ---------------------------------------------------------------- 门面契约
_facade = _src("harness/facade.py")
chk("verify 方法存在", callable(getattr(RscSession, "verify", None)))
chk("构造参数 verify_cmd", "verify_cmd=None" in _facade)
chk("构造参数 verify_required", "verify_required: bool = False" in _facade)
chk("verify 需要工作区（fail-closed）", "verify_requires_worktree" in _facade)
chk("verify 不经 shell（argv）", "shlex.split" in _facade and "shell=True" not in _facade)
chk("verify 有超时", "subprocess.TimeoutExpired" in _facade)
chk("verify 在隔离副本跑", "copytree" in _facade and "rsc-verify-" in _facade)
chk("合并视图叠加账本", "workspace.manifest" in _facade)
chk("合并视图叠加 pending", "pending" in _facade and "_overlay" in _facade)
chk("apply 前过门禁", "_verify_gate(" in _facade)
chk("失败记录也拦（fail-closed）", "verify_failed_recorded" in _facade)
chk("收据带 verified", "verified: bool = False" in _facade)
_apply = inspect.getsource(RscSession._apply_locked)
chk("apply 返回 verified", "verified=self._verified(acb)" in _apply)
_ctor = inspect.getsource(RscSession.__init__)
chk("required 无 cmd 即拒绝构造", "verify_required=True 时必须提供 verify_cmd" in _ctor)

# ---------------------------------------------------------------- 适配器暴露
chk("sidecar 返回 verified", '"verified": r.verified' in _src(
    "harness/integrations/http_sidecar.py"))
chk("MCP 返回 verified", '"verified": r.verified' in _src(
    "harness/integrations/mcp_server.py"))

# ---------------------------------------------------------------- 文档/示例
_doc = _src("docs/s24-gap-analysis-and-repositioning.md")
chk("文档记录 Verifier 门禁", "Verifier" in _doc)
chk("文档记录 8 项差距", "差距排序" in _doc)
_ex = _src("examples/semantic_conflict/run.py")
for token in ("git_scenario", "rsc_scenario", "gate_scenario", "green_scenario"):
    chk(f"实验含 {token}", f"def {token}(" in _ex)
chk("README 链到 S24", "s24-gap-analysis-and-repositioning.md" in _src("README.md"))

# ---------------------------------------------------------------- S24-3 静态语义
chk("静态语义模块存在", (_ROOT / "harness/semantic.py").exists())
from harness.semantic import detect

chk("detect 可调用", callable(detect))
_sem = _src("harness/semantic.py")
chk("抑制星号导入", 'alias.name == "*"' in _sem)
chk("抑制动态模块（__getattr__）", "__getattr__" in _sem)
chk("条件定义为不确定", "uncertain" in _sem)
chk("跳过 TYPE_CHECKING", "TYPE_CHECKING" in _sem)
chk("跳过注解", "visit_AnnAssign" in _sem)
_facade2 = _src("harness/facade.py")
chk("门面导入 detect", "from .semantic import detect" in _facade2)
chk("semantic 三态校验", '"off", "warn", "block"' in _facade2)
chk("显式 check_semantics", "def check_semantics" in _facade2)
chk("静态门禁存在", "def _semantic_gate" in _facade2)
_apply2 = inspect.getsource(RscSession._apply_locked)
chk("静态门禁在动态验证之前",
    _apply2.index("_semantic_gate(") < _apply2.index("_verify_gate("))
chk("语义冲突用 Deny 而非 Conflict", 'code="semantic_conflict"' in _facade2)
chk("语义报告落盘", 'spec["rsc_semantic"]' in _facade2)
chk("测试覆盖三态", "semantic=mode" in _src("tests/test_s24_semantic.py"))
_chk_sem = detect(
    {"app/core.py": b"def f(x):\n    return x\n",
     "app/use.py": b"from app.core import f\n"},
    {"app/core.py": b"def g(x):\n    return x\n",
     "app/use.py": b"from app.core import f\n"})
chk("运行期检出改名引用", (not _chk_sem.ok)
    and _chk_sem.issues[0].name == "f")
chk("运行期兼容改动不报", detect(
    {"app/core.py": b"def f(x):\n    return x\n"},
    {"app/core.py": b"def f(x):\n    return x\ndef g():\n    return 1\n"}).ok)

# ---------------------------------------------------------------- S24-4 merge queue
chk("merge_queue 模块存在", (_ROOT / "harness/merge_queue.py").exists())
_mq = _src("harness/merge_queue.py")
chk("单写者 worker", "def _loop" in _mq and "popleft" in _mq)
chk("FIFO 队列", "deque(" in _mq)
chk("容量满信号", "QUEUE_FULL" in _mq)
chk("超时信号", "QUEUE_TIMEOUT" in _mq)
chk("取消未开始票", "def cancel" in _mq)
_facade3 = _src("harness/facade.py")
chk("默认关闭", "merge_queue: dict | None = None" in _facade3)
chk("apply_queued 存在", "def apply_queued" in _facade3)
chk("出队自持 token 锁", "with self._op_lock(ticket.token)" in _facade3)
_qt = _src("tests/test_s24_queue.py")
chk("用例覆盖队首重验", "test_head_reverify_blocks_semantic_conflict" in _qt)
chk("用例覆盖容量/超时/取消",
    "test_queue_capacity_returns_wait" in _qt
    and "test_timeout_returns_wait" in _qt
    and "test_cancel_pending_ticket" in _qt)

# ---------------------------------------------------------------- 运行期
with tempfile.TemporaryDirectory() as tmp:
    h = make_harness(run_limits=10)
    session = RscSession(h, tenant="t", min_budget=1, worktrees=None,
                         state_dir=str(pathlib.Path(tmp) / "s"))
    tok = session.begin("a1", "i", writes=["a.md"])
    res = session.verify(tok.token, [sys.executable, "-c", "pass"])
    chk("无工作区 → DENIED", res.status.value == "DENIED")
    chk("无工作区原因正确",
        res.detail.get("reason") == "verify_requires_worktree")

    try:
        RscSession(h, verify_required=True)
        chk("required 无 cmd 拒绝构造", False)
    except ValueError:
        chk("required 无 cmd 拒绝构造", True)

    try:
        RscSession(h, verify_cmd=123)
        chk("非法 cmd 类型拒绝", False)
    except TypeError:
        chk("非法 cmd 类型拒绝", True)


    # merge queue 运行期：入队→出队→提交
    qs = RscSession(h, tenant="t", min_budget=1, worktrees=None,
                    state_dir=str(pathlib.Path(tmp) / "q"),
                    merge_queue={"capacity": 4})
    qtok = qs.begin("q1", "qi", writes=["q.md"])
    qr = qs.apply_queued(qtok.token, [("q.md", "qv")],
                         idempotency_key="qk", agent_id="q1")
    chk("队列提交成功", isinstance(qr, Receipt))
    chk("队列深度归零", qs.queue_depth() == 0)
    qs.close()

    # 显式失败记录 → apply 必须拒绝（即使没配 verify_cmd）
    import subprocess
    repo = pathlib.Path(tmp) / "repo"
    (repo / "app").mkdir(parents=True)
    subprocess.run(["git", "-C", str(repo), "init", "-q"], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.email", "c@e.com"],
                   check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.name", "c"],
                   check=True)
    (repo / "app/__init__.py").write_text("", encoding="utf-8")
    (repo / "app/b.py").write_text("Y = 1\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(repo), "add", "."], check=True)
    subprocess.run(["git", "-C", str(repo), "commit", "-qm", "init"], check=True)

    from harness.worktree import WorktreeManager
    session2 = RscSession(h, tenant="t", min_budget=1,
                          worktrees=WorktreeManager(str(repo),
                                                    str(pathlib.Path(tmp) / "wt")),
                          state_dir=str(pathlib.Path(tmp) / "s2"))
    tok2 = session2.begin("a2", "i2", writes=["app/b.py"])
    failing = [sys.executable, "-c", "raise SystemExit(3)"]
    assert session2.verify(tok2.token, failing).status.value == "DENIED"
    blocked = session2.apply(tok2.token, [("app/b.py", "Y = 2\n")],
                             idempotency_key="k2", agent_id="a2")
    chk("已记录失败 → apply 拒绝", isinstance(blocked, Deny))
    chk("拒绝码为 verify_failed", getattr(blocked, "code", "") == "verify_failed")
    chk("账本未被写入", h.workspace.version("app/b.py") == 0)

    # 通过记录 → 允许提交且收据 verified
    good = [sys.executable, "-c", "pass"]
    assert session2.verify(tok2.token, good).status.value == "OK"
    r = session2.apply(tok2.token, [("app/b.py", "Y = 2\n")],
                       idempotency_key="k3", agent_id="a2")
    chk("通过后可提交", isinstance(r, Receipt))
    chk("收据带 verified", getattr(r, "verified", False) is True)

print(f"通过 {len(ok)} / 失败 {len(bad)}")
for name, detail in bad:
    print("  ❌", name, detail)
sys.exit(1 if bad else 0)
