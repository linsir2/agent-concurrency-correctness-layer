#!/usr/bin/env python3
"""S22 真机 DAG 演示：3 个 agent 通过 RSC 门面并发写共享仓库。

DAG（两条边）：
    arch ─┐
          ├─▶ summary
    api  ─┘

每个节点 = 一个 agent：
  1. `RscSession.begin(writes=[目标文件], reads=[上游文件])` —— 准入 + 钉 base_version + 建 Z3
  2. 在**自己的 worktree** 里写文件（内容由 DashScope 生成）
  3. `apply(...)` —— CAS 提交 + 幂等键 + 收据
  4. `finish(...)` —— 结算预算 + 释放槽位 + 销毁 Z3

额外验证三条保证：
  A. 并发写同一资源 → 后写者拿到 `Conflict`（不丢写）
  B. 同幂等键重试 → 同一张收据（不重复副作用）
  C. 忘记 finish → `tick()` 自动退回预算并销毁 worktree

用法：
    DASHSCOPE_API_KEY=xxx python3 scripts/demo_dag.py
    # 或写进 /workspace/.env（已在 .gitignore 中）
    python3 scripts/demo_dag.py --via-proxy   # 受限沙箱：只能经代理出站
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import time
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from harness import Harness, ManualClock, RscSession
from harness.egress.resolver import NoRedirect
from harness.facade import Conflict, Receipt, Token
from harness.llm import LLMClient, LLMConfig, load_env_file
from harness.worktree import WorktreeManager

# ---------------------------------------------------------------- DAG 定义
DAG = {
    "arch": {"writes": ["docs/arch.md"], "reads": [],
             "prompt": "用不超过 120 字的中文写一段「并发正确性层」的架构说明，"
                       "只需正文，不要标题。"},
    "api": {"writes": ["docs/api.md"], "reads": [],
            "prompt": "用不超过 120 字的中文写一段 RSC 六动作 API 的说明"
                      "（begin/apply/gate/finish/abort/snapshot），只需正文。"},
    "summary": {"writes": ["README.md"], "reads": ["docs/arch.md", "docs/api.md"],
                "prompt": "下面是两份文档，请用不超过 150 字的中文写一段总览，"
                          "只需正文。\n\n=== 架构 ===\n{arch}\n\n=== API ===\n{api}"},
}
EDGES = [("arch", "summary"), ("api", "summary")]


def _git(repo, *args):
    subprocess.run(["git", "-C", str(repo), *args],
                   check=True, capture_output=True, text=True)


def _make_repo(root):
    repo = os.path.join(root, "repo")
    os.makedirs(repo, exist_ok=True)
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "demo@example.com")
    _git(repo, "config", "user.name", "demo")
    with open(os.path.join(repo, "PLACEHOLDER"), "w", encoding="utf-8") as fh:
        fh.write("init\n")
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "init")
    return repo


def _write_into(root, rel, content):
    path = os.path.join(root, rel)
    os.makedirs(os.path.dirname(path), exist_ok=True) if os.path.dirname(rel) else None
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(content)


def _topo(nodes):
    order, done = [], set()
    while len(order) < len(nodes):
        progressed = False
        for name in nodes:
            if name in done:
                continue
            deps = {s for s, d in EDGES if d == name}
            if deps <= done:
                order.append(name)
                done.add(name)
                progressed = True
        if not progressed:
            raise RuntimeError("DAG 有环")
    return order


class _OfflineLLM:
    """`--fake` 模式：不联网，确定性生成（用于本地验证 DAG 流程）。"""

    def chat(self, messages, **_kw):
        text = messages[-1]["content"]
        return f"[offline] {text[:60]}"


def _proxy_opener() -> urllib.request.OpenerDirector:
    """`--via-proxy` 专用：沿用环境变量里的代理出站（仍禁重定向）。

    **诚实说明**：代理会把请求转给第三方，等于绕过 `build_opener()` 的 DNS
    钉住与凭证隔离（`harness/egress/resolver.py` 的默认语义）。本函数只用于
    「本沙箱无直连出口」的演示，**不得**用于生产。
    """
    proxies = urllib.request.getproxies()
    opener = urllib.request.OpenerDirector()
    for handler in (urllib.request.ProxyHandler(proxies),
                    urllib.request.HTTPHandler(),
                    urllib.request.HTTPSHandler(),
                    urllib.request.HTTPErrorProcessor(),
                    NoRedirect()):
        opener.add_handler(handler)
    return opener


def main() -> int:
    fake = "--fake" in sys.argv
    via_proxy = "--via-proxy" in sys.argv
    if fake:
        cfg = None
        print("mode=offline（--fake，不调用模型）")
    else:
        load_env_file()
        try:
            cfg = LLMConfig.from_env()
        except Exception as exc:  # noqa: BLE001  # 配置错误统一给可读提示
            print(f"[FAIL] 未配置模型：{exc}")
            return 2
        print(f"model={cfg.model} base_url={cfg.base_url} api_key=***")
        if via_proxy:
            print("mode=via-proxy（代理出站；绕过 DNS 钉住，仅演示用）")

    workdir = tempfile.mkdtemp(prefix="rsc-demo-")
    repo = _make_repo(workdir)
    # 诚实说明：本沙箱无 Landlock/netns 强制点，故显式 `egress_required=False`
    # （显式降级，不是静默放行）；生产请用 `create_production_harness`。
    h = Harness(run_limits=10_000, egress_required=False,
                checkpoint_root=os.path.join(workdir, "ckpt"))
    session = RscSession(
        h, tenant="demo", min_budget=20, token_ttl_ms=60_000,
        worktrees=WorktreeManager(repo, os.path.join(workdir, "wt")),
        state_dir=os.path.join(workdir, "state"))
    llm = _OfflineLLM() if fake else LLMClient(
        cfg, egress=h.egress, opener=_proxy_opener() if via_proxy else None)

    print(f"repo={repo}")
    contents: dict[str, str] = {}
    rows = []

    for name in _topo(list(DAG)):
        spec = DAG[name]
        prompt = spec["prompt"].format(**{k.split("/")[-1].split(".")[0]: v
                                          for k, v in contents.items()})
        token = session.begin(f"agent-{name}", f"task-{name}",
                              writes=spec["writes"], reads=spec["reads"])
        if not isinstance(token, Token):
            print(f"[{name}] 准入失败：{token}")
            return 1
        try:
            text = llm.chat([{"role": "user", "content": prompt}], max_tokens=400)
        except Exception as exc:  # noqa: BLE001  # 任一上游失败都必须先 abort
            session.abort(token.token, "llm_failed")
            print(f"[{name}] LLM 失败：{exc}")
            return 1
        for rel in spec["writes"]:
            _write_into(token.worktree, rel, text)
        r = session.apply(token.token, [(rel, text) for rel in spec["writes"]],
                          idempotency_key=f"task-{name}:v1",
                          agent_id=f"agent-{name}")
        if not isinstance(r, Receipt):
            session.abort(token.token, "apply_failed")
            print(f"[{name}] 提交失败：{r}")
            return 1
        session.finish(token.token)
        contents[spec["writes"][0]] = text
        rows.append((name, r.receipt_id, h.workspace.version(spec["writes"][0]),
                     len(text), r.dedup))

    # ---- A. 并发写同一资源 → 后写者 Conflict
    t1 = session.begin("racer-1", "race-1", writes=["docs/race.md"])
    t2 = session.begin("racer-2", "race-2", writes=["docs/race.md"])
    _write_into(t1.worktree, "docs/race.md", "racer-1 wins")
    a1 = session.apply(t1.token, [("docs/race.md", "racer-1 wins")],
                       idempotency_key="race-1", agent_id="racer-1")
    _write_into(t2.worktree, "docs/race.md", "racer-2 loses")
    a2 = session.apply(t2.token, [("docs/race.md", "racer-2 loses")],
                       idempotency_key="race-2", agent_id="racer-2")
    session.finish(t1.token)
    session.abort(t2.token, "conflict")

    # ---- B. 同键重试 → 同一张收据
    t3 = session.begin("retry", "retry-1", writes=["docs/retry.md"])
    _write_into(t3.worktree, "docs/retry.md", "idempotent")
    b1 = session.apply(t3.token, [("docs/retry.md", "idempotent")],
                       idempotency_key="retry:K", agent_id="retry")
    b2 = session.apply(t3.token, [("docs/retry.md", "idempotent")],
                       idempotency_key="retry:K", agent_id="retry")
    session.finish(t3.token)

    # ---- C. 忘记 finish → tick 回收
    t4 = session.begin("forgetful", "forget-1", writes=["docs/forget.md"])
    before = h.budget.remaining("run")
    time.sleep(0.05)
    h.clock.advance(70_000) if isinstance(h.clock, ManualClock) else None
    # 用真实时钟时直接改 deadline 触发回收（等价于超时）
    h.acbs[t4.token].spec["token_deadline_ms"] = 0
    h.tick()
    after = h.budget.remaining("run")
    reaped = t4.token not in h.acbs

    print("\n=== DAG 执行结果 ===")
    print(f"{'task':<10}{'receipt_id':<26}{'version':<9}{'bytes':<7}dedup")
    for name, rid, ver, n, dd in rows:
        print(f"{name:<10}{rid:<26}{ver:<9}{n:<7}{dd}")

    print("\n=== 模型产出（前 60 字） ===")
    for rel, text in contents.items():
        print(f"{rel:<16} {text[:60]}")

    ok = True
    print("\n=== 三条保证 ===")
    print(f"A 并发写: {type(a1).__name__} / {type(a2).__name__} "
          f"→ 账本={h.workspace.get('docs/race.md')!r}")
    ok &= isinstance(a1, Receipt) and isinstance(a2, Conflict)
    print(f"B 同键重试: {b1.receipt_id} / {b2.receipt_id} dedup={b2.dedup}")
    ok &= isinstance(b1, Receipt) and isinstance(b2, Receipt) \
        and b1.receipt_id == b2.receipt_id and b2.dedup
    print(f"C 忘记 finish: 预算 {before} → {after}（释放 {after - before}），"
          f"token 已回收={reaped}")
    ok &= after > before and reaped

    print(f"\n预算：remaining={h.budget.remaining('run')} / 10000")
    print(f"事件数={len(h.event_log.all())}  收据数="
          f"{len([e for e in h.event_log.all() if e.kind.value == 'RECEIPT_ISSUED'])}")
    print(f"工作目录（可检查）：{workdir}")
    print("\n[PASS] DAG 真机跑通" if ok else "\n[FAIL] 断言未通过")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
