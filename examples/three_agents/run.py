#!/usr/bin/env python3
"""S23 示例：三个 agent 并发改同一份计划 —— 先看「没有 RSC 会怎样」，再看「有 RSC」。

为什么需要这个示例：多 agent 框架（LangGraph / CrewAI / OpenAI Agents / AG2）
各自负责编排与 LLM，但它们**都不保证工具调用的并发正确性**。当两个 agent 同时
「读文件 → 思考 → 写回」时，后写者会静默覆盖先写者（lost update）。

本脚本用同一份仓库、同样的交错顺序跑两遍：

1. `naive`：直接 `read → write`，最后只剩一个 agent 的内容（丢写）；
2. `rsc`  ：走六动作门面（begin 钉版本 → 在 Z3 写 → apply CAS 提交），
   后写者拿到 `Conflict`，重试合并后**两份内容都在**，且每个提交都有收据。

另外两段演示：

3. `sidecar`：把同一门面暴露成 HTTP（纯 stdlib），外部 agent 只带密钥就能用；
   没密钥 401、跨 agent 操作 403、同键重试 dedup、并发冲突 409；
4. `budget`：预算耗尽时 `begin` 返回 `Wait`（排队），而不是继续写。

用法（无需任何 API key；仅标准库 + git）：
    python3 examples/three_agents/run.py
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))))

from harness import Harness, RscSession
from harness.facade import Conflict, Receipt, Token, Wait
from harness.integrations.http_sidecar import build_sidecar, serve_in_thread
from harness.worktree import WorktreeManager


# ---------------------------------------------------------------- 脚手架
def _git(repo: str, *args: str) -> None:
    subprocess.run(["git", "-C", repo, *args], check=True,
                   capture_output=True, text=True)


def _make_repo(root: str) -> str:
    repo = os.path.join(root, "repo")
    os.makedirs(repo)
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "demo@example.com")
    _git(repo, "config", "user.name", "demo")
    with open(os.path.join(repo, "plan.md"), "w", encoding="utf-8") as fh:
        fh.write("# 计划\n")
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "init")
    return repo


def _write(root: str, rel: str, content: str) -> None:
    path = os.path.join(root, rel)
    os.makedirs(os.path.dirname(path) or root, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(content)


def _read_ledger(h: Harness, rel: str) -> str:
    raw = h.workspace.get(rel)
    return raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)


# ---------------------------------------------------------------- 1. 无 RSC
def naive_lost_update(root: str) -> str:
    """两个 agent 同时 read→write：后写者覆盖先写者（真实丢写）。"""
    repo = _make_repo(root)
    path = os.path.join(repo, "plan.md")
    barrier = threading.Barrier(2)

    def agent(name: str) -> None:
        with open(path, encoding="utf-8") as fh:
            text = fh.read()               # 都读到同一版本
        barrier.wait()                     # 同时开始「思考」
        time.sleep(0.02)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(text + f"- {name}\n")  # 后写者覆盖

    ts = [threading.Thread(target=agent, args=(n,)) for n in ("arch", "api")]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    with open(path, encoding="utf-8") as fh:
        return fh.read()


# ---------------------------------------------------------------- 2. 有 RSC
def rsc_scenario(root: str) -> dict:
    """begin 钉版本 → Z3 内写 → apply CAS；冲突者重试合并。"""
    repo = _make_repo(root)
    h = Harness(run_limits=100, egress_required=False,
                checkpoint_root=os.path.join(root, "ckpt"))
    session = RscSession(h, tenant="demo", min_budget=1,
                         worktrees=WorktreeManager(repo, os.path.join(root, "wt")),
                         state_dir=os.path.join(root, "state"))

    t1 = session.begin("arch", "plan", writes=["plan.md"])
    t2 = session.begin("api", "plan", writes=["plan.md"])
    assert isinstance(t1, Token) and isinstance(t2, Token)

    # arch 先提交
    _write(t1.worktree, "plan.md", "# 计划\n- arch\n")
    r1 = session.apply(t1.token, [("plan.md", "# 计划\n- arch\n".encode())],
                       idempotency_key="arch:1", agent_id="arch")
    assert isinstance(r1, Receipt)

    # api 在旧版本上提交 → 必须被拒绝（不静默覆盖）
    _write(t2.worktree, "plan.md", "# 计划\n- api\n")
    r2 = session.apply(t2.token, [("plan.md", "# 计划\n- api\n".encode())],
                       idempotency_key="api:1", agent_id="api")
    assert isinstance(r2, Conflict), r2
    session.abort(t2.token, "stale")

    # 重试：重新 begin（钉住新版本），合并后提交
    t3 = session.begin("api", "plan-retry", writes=["plan.md"])
    merged = _read_ledger(h, "plan.md") + "- api\n"
    _write(t3.worktree, "plan.md", merged)
    r3 = session.apply(t3.token, [("plan.md", merged.encode())],
                       idempotency_key="api:2", agent_id="api")
    assert isinstance(r3, Receipt), r3

    # 同键重试 → 同一张收据（幂等，不重复副作用）
    t4 = session.begin("api", "plan-dedup", writes=["plan.md"])
    r4 = session.apply(t4.token, [("plan.md", merged.encode())],
                       idempotency_key="api:2", agent_id="api")
    assert isinstance(r4, Receipt) and r4.dedup

    for tok in (t1.token, t3.token, t4.token):
        session.finish(tok)

    return {
        "ledger": _read_ledger(h, "plan.md"),
        "version": h.workspace.version("plan.md"),
        "receipts": [r1.receipt_id, r3.receipt_id, r4.receipt_id],
        "conflict": r2.reason,
        "dedup": r4.dedup,
        "remaining": h.budget.remaining("run"),
    }


# ---------------------------------------------------------------- 3. sidecar
def sidecar_scenario(root: str) -> dict:
    """门面 → HTTP：外部 agent 只带自己的密钥，服务端负责鉴权/归属/幂等。"""
    h = Harness(run_limits=100, egress_required=False,
                checkpoint_root=os.path.join(root, "ckpt"))
    session = RscSession(h, tenant="demo", min_budget=1,
                         state_dir=os.path.join(root, "state"))
    server = build_sidecar(session, keys={"K-arch": "arch", "K-api": "api"})
    thread = serve_in_thread(server)
    host, port = server.server_address[:2]

    def call(method: str, path: str, body=None, key: str | None = None,
             idem: str | None = None) -> tuple[int, dict]:
        headers = {}
        if key:
            headers["X-RSC-Auth"] = key
        if idem:
            headers["Idempotency-Key"] = idem
        data = None if body is None else json.dumps(body).encode()
        if data is not None:
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(f"http://{host}:{port}{path}",
                                     data=data, headers=headers,
                                     method=method)
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status, json.loads(resp.read() or b"{}")
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read() or b"{}")

    try:
        out = {"no_key": call("POST", "/begin",
                              {"intent_key": "x", "writes": ["plan.md"]})[0]}
        _, a = call("POST", "/begin", {"intent_key": "a", "writes": ["plan.md"]},
                    key="K-arch")
        _, b = call("POST", "/begin", {"intent_key": "b", "writes": ["plan.md"]},
                    key="K-api")
        out["apply_a"] = call("POST", "/apply",
                              {"token": a["token"],
                               "writes": [{"resource": "plan.md", "content": "A"}]},
                              key="K-arch", idem="a1")[0]
        out["apply_b_stale"] = call("POST", "/apply",
                                    {"token": b["token"],
                                     "writes": [{"resource": "plan.md", "content": "B"}]},
                                    key="K-api", idem="b1")[0]
        out["dedup"] = call("POST", "/apply",
                            {"token": a["token"],
                             "writes": [{"resource": "plan.md", "content": "A"}]},
                            key="K-arch", idem="a1")[1].get("dedup")
        out["cross_agent"] = call("POST", "/finish", {"token": a["token"]},
                                  key="K-api")[0]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
    return out


# ---------------------------------------------------------------- 4. 预算
def budget_scenario(root: str) -> str:
    h = Harness(run_limits=1, egress_required=False,
                checkpoint_root=os.path.join(root, "ckpt"))
    session = RscSession(h, tenant="demo", min_budget=1,
                         state_dir=os.path.join(root, "state"))
    t1 = session.begin("arch", "b1", writes=["plan.md"])
    assert isinstance(t1, Token)
    assert isinstance(session.apply(t1.token, [("plan.md", b"one")],
                                    idempotency_key="b1", agent_id="arch"),
                      Receipt)
    session.finish(t1.token)
    t2 = session.begin("api", "b2", writes=["plan.md"])
    return t2.reason if isinstance(t2, Wait) else f"unexpected:{t2}"


def main() -> int:
    root = tempfile.mkdtemp(prefix="rsc-example-")
    ok = True

    print("=" * 72)
    print("1) 没有 RSC：两个 agent 同时 read→write")
    naive = naive_lost_update(os.path.join(root, "naive"))
    print(naive)
    lost = ("- arch" in naive) != ("- api" in naive)
    print(f"→ 结果：{'只剩一份写入（lost update）' if lost else '未丢写'}")
    ok &= lost

    print("=" * 72)
    print("2) 有 RSC：begin 钉版本 → Z3 写 → apply CAS → 冲突重试")
    rsc = rsc_scenario(os.path.join(root, "rsc"))
    print(rsc["ledger"])
    print(f"→ version={rsc['version']}  conflict={rsc['conflict']}")
    print(f"→ 收据：{rsc['receipts']}  dedup={rsc['dedup']}")
    print(f"→ 预算剩余={rsc['remaining']}")
    both = "- arch" in rsc["ledger"] and "- api" in rsc["ledger"]
    print(f"→ 结果：{'两份写入都保留，且有收据可审计' if both else '仍有丢写'}")
    ok &= both and rsc["version"] == 2 and rsc["dedup"]

    print("=" * 72)
    print("3) HTTP sidecar：外部 agent 只带密钥")
    side = sidecar_scenario(os.path.join(root, "side"))
    print(f"→ 无密钥={side['no_key']}  写A={side['apply_a']}  "
          f"B 冲突={side['apply_b_stale']}  同键 dedup={side['dedup']}  "
          f"跨 agent 操作={side['cross_agent']}")
    ok &= (side["no_key"] == 401 and side["apply_a"] == 200
           and side["apply_b_stale"] == 409 and side["dedup"] is True
           and side["cross_agent"] == 403)
    print("→ 结果：401/200/409/403 全部由服务端强制（宿主无需实现）")

    print("=" * 72)
    print("4) 预算耗尽：begin 返回 Wait（排队），不是继续写")
    reason = budget_scenario(os.path.join(root, "budget"))
    print(f"→ {reason}")
    ok &= "BUDGET_EXHAUSTED" in reason

    print("=" * 72)
    print("[PASS] 示例跑通：有 RSC 不丢写；HTTP 适配器自带鉴权/归属/幂等"
          if ok else "[FAIL] 断言未通过")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
