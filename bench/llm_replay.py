"""S24-9：真实模型 token 基准（录制 + 离线回放）。

**要回答什么**：「RSC 让 agent 返工，token 到底多花多少？」—— 在**同一份
真实模型调用记录**上，比较两个条件：

- `naive`：两个 agent 读同一版本、各写各的 → 后写覆盖前写（丢写），检查失败；
- `rsc`：同一次读取被版本钉住，后写者 `Conflict` → **多一次真实模型调用**
  按最新内容重写 → 检查通过。

指标：`total_tokens` / `token_multiplier`（rsc ÷ naive）/ `rework_rate`
（重试 turn ÷ 总 turn）/ `success` / `lost_update`。**token 全部来自上游
`usage`**，缺失即标 `usage_missing`，绝不按字符估算。

两种运行方式：

```bash
python3 -m bench.llm_replay --live --via-proxy --out bench/fixtures/llm_replay.json
python3 -m bench.llm_replay --fixture bench/fixtures/llm_replay.json --json
```

`--live` 需要 `DASHSCOPE_API_KEY`（读 `.env`）；没有 key → `status=skipped`
且**不输出任何数字**（`--require-live` 时退出码 3）。回放完全离线、确定性。

**诚实边界**：n=1 场景、每个条件 2–3 个 turn；只报配对数值与极差，不做
显著性声明。回放 ≠ live：`provenance=live` 表示 usage 录制自真实请求，
但并不是对模型能力的评测。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import re
import subprocess
import sys
import tempfile
import time
import urllib.request

from harness.facade import Conflict, RscSession
from harness.harness import Harness
from harness.llm import LLMClient, LLMConfig, LLMError, LLMReply, MissingAPIKey, load_env_file
from harness.worktree import WorktreeManager

SCHEMA_VERSION = 2
SCENARIO = "config_conflict"
FIXTURE = pathlib.Path(__file__).resolve().parent / "fixtures" / "llm_replay.json"

PROMPT_A = ('You are editing a JSON config. Current content: {"a": 0, "b": 0}. '
            'Choose a positive integer between 1 and 5 for key "a". '
            'Reply with ONLY that integer.')
PROMPT_B = ('You are editing a JSON config. Current content: {"a": 0, "b": 0}. '
            'Choose a positive integer between 1 and 5 for key "b". '
            'Reply with ONLY that integer.')
PROMPT_B_RETRY = ('The config was changed by another agent to: {current}. '
                  'Choose a positive integer between 1 and 5 for key "b", '
                  'keeping the other keys unchanged. Reply with ONLY that integer.')


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _parse_int(text: str) -> tuple[int, bool]:
    """只接受 1–5；解析失败 → (1, True)（**标注**，不静默假装成功）。"""
    match = re.search(r"\b([1-5])\b", text or "")
    return (int(match.group(1)), False) if match else (1, True)


def _proxy_opener() -> urllib.request.OpenerDirector:
    """受限沙箱专用：经环境代理出站（仅演示/基准，生产禁用）。"""
    from harness.egress.resolver import NoRedirect
    opener = urllib.request.OpenerDirector()
    for handler in (urllib.request.ProxyHandler(urllib.request.getproxies()),
                    urllib.request.HTTPHandler(),
                    urllib.request.HTTPSHandler(),
                    urllib.request.HTTPErrorProcessor(),
                    NoRedirect()):
        opener.add_handler(handler)
    return opener


def _git(repo: pathlib.Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(repo), *args], check=True,
                   capture_output=True, text=True)


def _make_repo(root: pathlib.Path) -> pathlib.Path:
    repo = root / "repo"
    (repo / "app").mkdir(parents=True)
    (repo / "config.json").write_text('{"a": 0, "b": 0}\n', encoding="utf-8")
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "bench@example.com")
    _git(repo, "config", "user.name", "bench")
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "init")
    return repo


def _usage(rep: LLMReply) -> dict:
    return rep.as_dict()


def _turn(agent: str, reason: str, rep: LLMReply, parse_failed: bool) -> dict:
    item = {"agent": agent, "reason": reason, "parse_failed": parse_failed}
    item.update(_usage(rep))
    return item


# ---------------------------------------------------------------- live 采集
def _capture_one(*, via_proxy: bool = False, enable_thinking: bool = False,
                 max_tokens: int = 64) -> dict:
    """一次真实采集：两个条件共享同一次 A/B 调用（配对）。"""
    load_env_file()
    cfg = LLMConfig.from_env()                 # 无 key → MissingAPIKey（调用方处理）
    h = Harness(run_limits=50, egress_required=False)
    llm = LLMClient(cfg, egress=h.egress,
                    opener=_proxy_opener() if via_proxy else None)
    extra = None if enable_thinking else {"enable_thinking": False}

    with tempfile.TemporaryDirectory(prefix="rsc-bench-") as tmp:
        root = pathlib.Path(tmp)
        repo = _make_repo(root)

        # ---- 真实模型调用（两个条件共享同一 turn 的 usage）--------
        rep_a = llm.chat_with_usage([{"role": "user", "content": PROMPT_A}],
                                    temperature=0.0, max_tokens=max_tokens,
                                    extra=extra)
        a_value, a_bad = _parse_int(rep_a.text)
        rep_b = llm.chat_with_usage([{"role": "user", "content": PROMPT_B}],
                                    temperature=0.0, max_tokens=max_tokens,
                                    extra=extra)
        b_value, b_bad = _parse_int(rep_b.text)

        # ---- naive 臂：真实写文件（后写覆盖前写）-----------------
        naive_dir = root / "naive"
        naive_dir.mkdir()
        config = naive_dir / "config.json"
        config.write_text('{"a": 0, "b": 0}', encoding="utf-8")
        config.write_text(json.dumps({"a": a_value, "b": 0}), encoding="utf-8")
        config.write_text(json.dumps({"a": 0, "b": b_value}), encoding="utf-8")
        final_naive = json.loads(config.read_text(encoding="utf-8"))
        naive_success = bool(final_naive.get("a", 0) > 0
                             and final_naive.get("b", 0) > 0)

        # ---- rsc 臂：同一版本钉住 → Conflict → 真实重试 -----------
        session = RscSession(
            h, tenant="bench", min_budget=5,
            worktrees=WorktreeManager(str(repo), str(root / "wt")),
            state_dir=str(root / "state"))
        tok_a = session.begin("agent-a", "key-a", writes=["config.json"])
        tok_b = session.begin("agent-b", "key-b", writes=["config.json"])
        assert hasattr(tok_a, "token") and hasattr(tok_b, "token")
        applied_a = session.apply(tok_a.token,
                                  [("config.json", json.dumps({"a": a_value, "b": 0}))],
                                  idempotency_key="bench-a", agent_id="agent-a")
        session.finish(tok_a.token)
        conflict = session.apply(tok_b.token,
                                 [("config.json", json.dumps({"a": 0, "b": b_value}))],
                                 idempotency_key="bench-b1", agent_id="agent-b")
        assert applied_a is not None
        # 真实返工：把「当前内容」反馈给模型，再写一次
        retry_prompt = PROMPT_B_RETRY.format(
            current=json.dumps({"a": a_value, "b": 0}, sort_keys=True))
        rep_retry = llm.chat_with_usage(
            [{"role": "user", "content": retry_prompt}],
            temperature=0.0, max_tokens=max_tokens, extra=extra)
        b2_value, b2_bad = _parse_int(rep_retry.text)
        merged = json.dumps({"a": a_value, "b": b2_value})
        if isinstance(conflict, Conflict):
            # 旧 token 的 base 已过期：按真实 agent 行为 abort → 重新 begin
            session.abort(tok_b.token, "stale_retry")
            tok_b2 = session.begin("agent-b", "key-b-retry",
                                   writes=["config.json"])
            applied_b = session.apply(tok_b2.token, [("config.json", merged)],
                                      idempotency_key="bench-b2",
                                      agent_id="agent-b")
            session.finish(tok_b2.token)
        else:                                  # 未冲突（不应发生）→ 直接写
            applied_b = session.apply(tok_b.token, [("config.json", merged)],
                                      idempotency_key="bench-b2",
                                      agent_id="agent-b")
            session.finish(tok_b.token)
        session.close()

    # 结果判定（来自真实 CAS 结果 + naive 真实文件状态，不是声明）
    rsc_success = not isinstance(applied_b, Conflict) and hasattr(applied_b, "seq")
    return {
        "schema_version": SCHEMA_VERSION,
        "scenario": SCENARIO,
        "provenance": "live",
        "captured_at": "2026-10-28",
        "model": rep_a.model,
        "temperature": 0.0,
        "claim_scope": "paired_token_cost_of_workflow_turns",
        "sample_size": 1,
        "prompts": {"a": _sha(PROMPT_A), "b": _sha(PROMPT_B),
                    "b_retry": _sha(PROMPT_B_RETRY)},
        "turns": {
            "a": _turn("agent-a", "initial", rep_a, a_bad),
            "b": _turn("agent-b", "initial", rep_b, b_bad),
            "b_retry": _turn("agent-b", "retry_conflict", rep_retry, b2_bad),
        },
        "arms": {
            "naive": {"turn_ids": ["a", "b"],
                      "outcome": {"success": naive_success,
                                  "lost_update": not naive_success}},
            "rsc": {"turn_ids": ["a", "b", "b_retry"],
                    "outcome": {"success": rsc_success,
                                "lost_update": False,
                                "first_conflict": isinstance(conflict, Conflict)}},
        },
    }


def capture_live(*, via_proxy: bool = False, n: int = 5,
                 enable_thinking: bool = False, max_tokens: int = 64) -> dict:
    """真实调用 n 次并录成 v2（每个 sample 独立 repo/session）。"""
    count = max(1, int(n))
    samples: list[dict] = []
    first: dict | None = None
    for index in range(count):
        one = _capture_one(via_proxy=via_proxy, enable_thinking=enable_thinking,
                           max_tokens=max_tokens)
        first = first or one
        samples.append({"sample_id": f"s{index}", "turns": one["turns"],
                        "arms": one["arms"]})
    assert first is not None
    if not enable_thinking:
        reasoning = sum(int(t.get("reasoning_tokens", 0))
                        for s in samples for t in s["turns"].values())
        if reasoning:
            raise LLMError(
                f"enable_thinking=False 但上游仍有 {reasoning} reasoning_tokens"
                "（该模型不可关闭 thinking，拒绝出数）")
    return {
        "schema_version": 2,
        "scenario": SCENARIO,
        "provenance": "live",
        "captured_at": time.strftime("%Y-%m-%d"),
        "model": first["model"],
        "claim_scope": "paired_token_cost_of_workflow_turns",
        "request": {"temperature": 0.0, "max_tokens": int(max_tokens),
                    "enable_thinking": bool(enable_thinking)},
        "prompts": first["prompts"],
        "samples": samples,
        "sample_size": count,
    }


# ---------------------------------------------------------------- 回放/指标
def _normalize(doc: dict) -> list[dict]:
    """v1 → 单样本；v2 → samples（下游只认样本列表）。"""
    version = doc.get("schema_version")
    if version == 2:
        samples = doc.get("samples")
        if not isinstance(samples, list) or not samples:
            raise ValueError("v2 fixture 缺少非空 samples")
        return samples
    if version == 1:
        turns, arms = doc.get("turns"), doc.get("arms")
        if not isinstance(turns, dict) or not isinstance(arms, dict):
            raise TypeError("fixture 缺少 turns/arms")
        return [{"sample_id": "s0", "turns": turns, "arms": arms}]
    raise ValueError("schema_version 必须是 1 或 2")


def _sample_problem(sample: dict) -> str | None:
    """返回样本无效原因；`None` = 有效。结构问题抛异常。"""
    if not isinstance(sample, dict):
        raise TypeError("sample 必须是对象")
    turns, arms = sample.get("turns"), sample.get("arms")
    if not isinstance(turns, dict) or not isinstance(arms, dict):
        raise TypeError(f"sample {sample.get('sample_id', '')} 缺少 turns/arms")
    for tid, turn in turns.items():
        if turn.get("usage_missing"):
            return f"turn {tid} 缺 usage"
        total = int(turn.get("total_tokens", 0))
        add = int(turn.get("prompt_tokens", 0)) + int(turn.get("completion_tokens", 0))
        if total != add:
            return f"turn {tid}: total_tokens({total}) != prompt+completion({add})"
        if turn.get("finish_reason") == "length":
            return f"turn {tid} 被 max_tokens 截断"
    for name, arm in arms.items():
        ids = arm.get("turn_ids") or []
        if not ids or any(t not in turns for t in ids):
            return f"arm {name} 引用了不存在的 turn"
        if not isinstance(arm.get("outcome"), dict):
            return f"arm {name} 缺少 outcome"
    return None


def validate_fixture(doc: dict) -> list[dict]:
    """结构 + 算术自检；返回被剔除的样本（全部无效 → ValueError）。"""
    if doc.get("schema_version") not in (1, 2):
        raise ValueError("schema_version 必须是 1 或 2")
    if doc.get("provenance") not in ("live", "authored"):
        raise ValueError("provenance 必须是 live|authored")
    samples = _normalize(doc)
    rejected = []
    for sample in samples:
        problem = _sample_problem(sample)
        if problem:
            rejected.append({"sample_id": str(sample.get("sample_id", "")),
                             "reason": problem})
    if len(rejected) == len(samples):
        raise ValueError(f"没有有效样本：{rejected}")
    return rejected


def _arm_metrics(sample: dict, name: str) -> dict:
    arm = sample["arms"][name]
    turns = [sample["turns"][t] for t in arm["turn_ids"]]
    total = sum(int(t.get("total_tokens", 0)) for t in turns)
    prompt = sum(int(t.get("prompt_tokens", 0)) for t in turns)
    completion = sum(int(t.get("completion_tokens", 0)) for t in turns)
    cached = sum(int(t.get("cached_tokens", 0)) for t in turns)
    reasoning = sum(int(t.get("reasoning_tokens", 0)) for t in turns)
    retries = sum(1 for t in turns if t.get("reason", "").startswith("retry_"))
    parse_failed = sum(1 for t in turns if t.get("parse_failed"))
    return {"turns": len(turns), "retry_turns": retries,
            "total_tokens": total, "prompt_tokens": prompt,
            "completion_tokens": completion, "cached_tokens": cached,
            "reasoning_tokens": reasoning,
            "rework_rate": round(retries / len(turns), 4) if turns else 0.0,
            "parse_failed": parse_failed,
            "outcome": arm["outcome"]}


def _median(values: list[float]) -> float:
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return float(ordered[mid])
    return (float(ordered[mid - 1]) + float(ordered[mid])) / 2


def _stats(values: list[float]) -> dict:
    med = round(_median(values), 4)
    lo, hi = min(values), max(values)
    return {"median": med, "min": round(lo, 4), "max": round(hi, 4),
            "range": round(hi - lo, 4), "values": [round(v, 4) for v in values]}


def _aggregate_arm(per: list[dict], name: str) -> dict:
    arms = [item[name] for item in per]
    out = {key: round(_median([a[key] for a in arms]), 4)
           for key in ("turns", "retry_turns", "total_tokens", "prompt_tokens",
                       "completion_tokens", "cached_tokens", "reasoning_tokens",
                       "rework_rate", "parse_failed")}
    successes = [bool(a["outcome"].get("success")) for a in arms]
    out["outcome"] = {"success": all(successes),
                      "success_rate": round(sum(successes) / len(successes), 4)}
    return out


def compute_metrics(doc: dict) -> dict:
    """纯函数：从 turn 的 usage 精确重算，不读任何缓存数字。"""
    rejected = validate_fixture(doc)
    per: list[dict] = []
    for sample in _normalize(doc):
        if _sample_problem(sample):
            continue
        naive = _arm_metrics(sample, "naive")
        rsc = _arm_metrics(sample, "rsc")
        base = naive["total_tokens"]
        per.append({
            "sample_id": str(sample.get("sample_id", "")),
            "naive": naive, "rsc": rsc,
            "token_multiplier": (round(rsc["total_tokens"] / base, 4)
                                 if base else "N/A"),
            "rework_rate_delta": round(rsc["rework_rate"] - naive["rework_rate"], 4),
        })
    if not per:
        raise ValueError("没有可聚合的有效样本")
    common = {
        "scenario": doc.get("scenario"),
        "provenance": doc.get("provenance"),
        "claim_scope": doc.get("claim_scope", ""),
        "sample_size": int(doc.get("sample_size", len(per))),
        "model": doc.get("model", ""),
    }
    if len(per) == 1:
        one = per[0]
        return {**common,
                "conditions": {"naive": one["naive"], "rsc": one["rsc"]},
                "token_multiplier": one["token_multiplier"],
                "rework_rate_delta": one["rework_rate_delta"]}
    multipliers = [p["token_multiplier"] for p in per
                   if isinstance(p["token_multiplier"], (int, float))]
    deltas = [p["rework_rate_delta"] for p in per]
    return {
        **common,
        "n_valid": len(per),
        "n_rejected": len(rejected),
        "rejected": rejected,
        "conditions": {"naive": _aggregate_arm(per, "naive"),
                       "rsc": _aggregate_arm(per, "rsc")},
        "token_multiplier": round(_median(multipliers), 4) if multipliers else "N/A",
        "token_multiplier_stats": _stats(multipliers) if multipliers else {},
        "rework_rate_delta": round(_median(deltas), 4),
        "rework_rate_delta_stats": _stats(deltas),
        "samples": per,
    }


def load_fixture(path: pathlib.Path | None = None) -> dict:
    path = pathlib.Path(path or FIXTURE)
    doc = json.loads(path.read_text(encoding="utf-8"))
    validate_fixture(doc)
    return doc


def write_report(report: dict) -> str:
    naive = report["conditions"]["naive"]
    rsc = report["conditions"]["rsc"]
    n_valid = int(report.get("n_valid", report.get("sample_size", 1)))
    success_naive = (naive["outcome"].get("success") if n_valid == 1
                     else naive["outcome"].get("success_rate"))
    success_rsc = (rsc["outcome"].get("success") if n_valid == 1
                   else rsc["outcome"].get("success_rate"))
    lines = [
        f"# LLM token 基准（{report['scenario']}）",
        "",
        (f"- provenance: **{report['provenance']}**（model={report['model']}，"
         f"n={report['sample_size']}，valid={n_valid}）"),
        f"- claim_scope: `{report['claim_scope']}`",
        "",
        "| 条件 | turns | total tokens | 重试 turns | rework_rate | success |",
        "|---|---|---|---|---|---|",
        (f"| naive | {naive['turns']} | {naive['total_tokens']} | "
         f"{naive['retry_turns']} | {naive['rework_rate']} | {success_naive} |"),
        (f"| rsc | {rsc['turns']} | {rsc['total_tokens']} | "
         f"{rsc['retry_turns']} | {rsc['rework_rate']} | {success_rsc} |"),
        "",
        f"- **token 倍数**（rsc ÷ naive）：**{report['token_multiplier']}**",
        f"- rework_rate 增量：{report['rework_rate_delta']}",
    ]
    if n_valid > 1:
        tm = report.get("token_multiplier_stats", {})
        rd = report.get("rework_rate_delta_stats", {})
        lines += [
            (f"- token 倍数中位数 {tm.get('median')}（min {tm.get('min')} / "
             f"max {tm.get('max')} / 极差 {tm.get('range')}）"),
            f"- rework_rate 增量中位数 {rd.get('median')}（极差 {rd.get('range')}）",
            f"- 被剔除样本：{report.get('n_rejected', 0)}",
            "",
            ("> 描述性统计（n>1），不作显著性结论；"
             "token 来自上游 `usage`，`reasoning_tokens` 已含在 completion 内。"),
        ]
    else:
        lines += [
            "",
            "> 小样本（n=1 场景、每条件 2–3 turn）。只报配对数值，不做显著性声明；",
            "> token 来自上游 `usage`，`reasoning_tokens` 已包含在 completion 内。",
        ]
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------- CLI
def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python3 -m bench.llm_replay",
                                 description="真实模型 token 基准（录制/回放）")
    ap.add_argument("--fixture", default=str(FIXTURE), help="回放 fixture")
    ap.add_argument("--live", action="store_true", help="真实调用模型并录制 fixture")
    ap.add_argument("--via-proxy", action="store_true", help="经环境代理出站（仅沙箱）")
    ap.add_argument("--out", default=None, help="--live 的 fixture 输出路径")
    ap.add_argument("--require-live", action="store_true",
                    help="没有 key 时退出码 3（默认跳过）")
    ap.add_argument("--n", type=int, default=5, help="--live 样本数（默认 5）")
    ap.add_argument("--enable-thinking", action="store_true",
                    help="--live 打开 thinking（默认关闭）")
    ap.add_argument("--max-tokens", type=int, default=64,
                    help="--live 每次调用的 max_tokens（默认 64）")
    ap.add_argument("--json", action="store_true", help="输出 JSON")
    ap.add_argument("--report", default=None, help="同时写 Markdown 报告")
    args = ap.parse_args(argv)

    if args.live:
        try:
            doc = capture_live(via_proxy=args.via_proxy, n=args.n,
                               enable_thinking=args.enable_thinking,
                               max_tokens=args.max_tokens)
        except MissingAPIKey as exc:
            payload = {"status": "skipped", "reason": "missing_api_key",
                       "detail": str(exc)}
            print(json.dumps(payload, ensure_ascii=False) if args.json
                  else f"SKIP: {exc}")
            return 3 if args.require_live else 0
        if args.out:
            path = pathlib.Path(args.out)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(doc, ensure_ascii=False, indent=2,
                                       sort_keys=True) + "\n", encoding="utf-8")
        report = compute_metrics(doc)
    else:
        report = compute_metrics(load_fixture(pathlib.Path(args.fixture)))

    if args.report:
        path = pathlib.Path(args.report)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(write_report(report), encoding="utf-8")
        report["report_path"] = str(path)
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
          if args.json else write_report(report))
    return 0


if __name__ == "__main__":                    # pragma: no cover
    sys.exit(main())
