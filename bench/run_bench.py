#!/usr/bin/env python3
"""S7 基准主入口（`docs/s7-benchmark-and-coverage.md` v1.1 §2.6/§2.7）。

用法::

    python bench/run_bench.py --quick            # n=3，冒烟
    python bench/run_bench.py                     # 完整模式 n>=30
    python bench/run_bench.py --n 8 --delay 0.002 --out bench/result.json

固定随机种子 + `PYTHONHASHSEED=0`；warmup；输出 JSON。
预注册判定**按格**：C0 只做 sanity；主终点在 C2；判定用非劣 + 等价（不用 p 值）。

**确定性判定按条件区分（诚实修正，非放宽）**：

- **B1（串行）/ B2（worktree + FIFO 合并）**是确定性算法 → 离散指标
  （`lost_update_rate` / `conflict_rate_detected` / `conflict_rate_silent` /
  `wasted_work`）重复 n 次必须**逐值相等**（方差为 0）。这两条件若出现非确定，
  那是**实现 bug**，必须修实现而不是放宽判定。
- **B3（`ThreadPoolExecutor` + `Barrier` 并发）**：`conflict_rate` / `wasted_work`
  是**线程交错的函数**（rebase 重试次数、冲突检测顺序），本质非确定（实测
  C1/B3 的 `conflict_rate_detected ∈ {5/9, 6/10}`、`wasted_work ∈ {5/3, 6/3}`，
  偶发多 1 次重试）→ 用**有界带**判定（落在预注册期望值 ±`B3_BAND_REL` 内），
  并把分布（min/max/unique）写入结果与报告。对期望值为 0 的正确性指标
  （`lost_update_rate` / `conflict_rate_silent`），带退化为**严格 0**（任何非零即 bug）。
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import random
import shutil
import statistics
import subprocess
import sys
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from harness import EventKind, Harness  # noqa: E402

from bench import conditions as C  # noqa: E402
from bench import metrics as M  # noqa: E402
from bench.workloads import WORKLOADS, freeze  # noqa: E402

CONDITIONS = {"B1": C.run_b1, "B2": C.run_b2, "B3": C.run_b3}
DEFAULT_SEED = 20261027
MARGIN = 0.0          # 预注册 margin（严格 < → 平局不成立）


def _reexec_hashseed() -> None:
    if os.environ.get("PYTHONHASHSEED") != "0":
        os.environ["PYTHONHASHSEED"] = "0"
        os.execv(sys.executable, [sys.executable] + sys.argv)


# ---------------------------------------------------------------- 环境信息
def collect_env() -> dict:
    return {
        "os": platform.system(),
        "kernel": platform.release(),
        "machine": platform.machine(),
        "python": sys.version.split()[0],
        "python_full": sys.version.replace("\n", " "),
        "cpu_count": os.cpu_count(),
        "fsync_available": hasattr(os, "fdatasync"),
        "wal_fsync_default": True,
        "git_available": shutil.which("git") is not None,
        "git_path": shutil.which("git"),
        "cwd": os.getcwd(),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
    }


# ---------------------------------------------------------------- warmup
def warmup() -> None:
    """预热：JIT/导入/文件缓存，避免首轮抖动。"""
    for name in ("C0", "C1"):
        w = WORKLOADS[name]
        for fn in (C.run_b1, C.run_b2, C.run_b3):
            fn(w, per_action_delay=0.0)


# ---------------------------------------------------------------- 跑格
def run_cell(workload_name: str, condition: str, n: int, delay: float) -> list:
    w = WORKLOADS[workload_name]
    fn = CONDITIONS[condition]
    out = []
    for i in range(n):
        out.append(fn(w, per_action_delay=delay))
    return out


def summarize(results: list, workload) -> dict:
    cells = [M.cell_metrics(r, workload) for r in results]
    keys = ("lost_update_rate", "conflict_rate_detected", "conflict_rate_silent",
            "serialization_rate", "wasted_work", "throughput",
            "commit_latency_p50", "commit_latency_p99")
    summary = {"n": len(results)}
    for k in keys:
        vals = [c[k] for c in cells]
        summary[k] = {
            "values": vals,
            "mean": sum(vals) / len(vals),
            "variance": M.variance(vals),
            # 分布（min/max/unique）：B3 并发指标的抖动如实记录，供报告与断言使用
            "min": min(vals),
            "max": max(vals),
            "unique": sorted(set(vals)),
        }
    walls = [r.wall_time for r in results]
    summary["wall_time"] = {"values": walls, "mean": sum(walls) / len(walls),
                            "variance": M.variance(walls)}
    summary["freeze"] = results[0].freeze if results else None
    summary["degraded"] = bool(results and results[0].degraded)
    summary["subprocess_s"] = sum(r.subprocess_s for r in results)
    summary["subprocess_calls"] = sum(r.subprocess_calls for r in results)
    summary["lost_resources"] = cells[0]["lost_resources"]
    summary["missing_keys"] = cells[0]["missing_keys"]
    summary["extra_keys"] = cells[0]["extra_keys"]
    summary["changed_keys"] = cells[0]["changed_keys"]
    # per-agent 重放次数分布（时序相关 → 观测非断言：B3 线程调度下可能
    # 出现 (3,2,3)/(3,3,2)/(2,3,3) 等排列；wasted_work 对其取和，仍稳定）
    dist = Counter(tuple(c["turn_replay_counts"]) for c in cells)
    summary["turn_replay_counts_distribution"] = {
        "|".join(str(x) for x in k): v for k, v in sorted(dist.items())
    }
    return summary


DISCRETE_METRICS = ("lost_update_rate", "conflict_rate_detected",
                    "conflict_rate_silent", "wasted_work")

# ---------------------------------------------------------------- B3 有界带
# B3（并发）离散指标的**预注册期望值**（确定性算法在「典型线程交错」下的众数，
# 见 report.md §3/§3.6）+ 有界带半宽。
#
# 理由（诚实修正，非放宽）：B3 是 `ThreadPoolExecutor` + `Barrier(N)` 的多线程
# 并发负载，`conflict_rate` / `wasted_work` 取决于**线程交错**（rebase 重试次数、
# 冲突检测顺序），本质非确定——实测 C1/B3 的 `conflict_rate_detected ∈ {5/9, 6/10}`、
# `wasted_work ∈ {5/3, 6/3}`（偶发多 1 次重试）。这不是实现 bug，是并发负载的固有
# 非确定性，故对 B3 用**有界带**而非逐值相等。
#
# 带宽 ±25%：覆盖实测最大并发抖动（C1/B3 `wasted_work` 1.667→2.0，+20%），
# 同时对正确性指标（期望 0）退化为严格 0，仍足以捕获真 bug（如 K-28 静默丢写）。
# B1（串行）/ B2（worktree + FIFO 合并）是确定性算法，**不**适用本带（见下）。
B3_EXPECTED: dict[str, dict[str, float]] = {
    "C0": {"lost_update_rate": 0.0, "conflict_rate_detected": 0.0,
           "conflict_rate_silent": 0.0, "wasted_work": 0.0},
    "C1": {"lost_update_rate": 0.0, "conflict_rate_detected": 5 / 9,
           "conflict_rate_silent": 0.0, "wasted_work": 5 / 3},
    "C2": {"lost_update_rate": 0.0, "conflict_rate_detected": 1 / 3,
           "conflict_rate_silent": 0.0, "wasted_work": 1 / 2},
    "C3": {"lost_update_rate": 0.0, "conflict_rate_detected": 2 / 5,
           "conflict_rate_silent": 0.0, "wasted_work": 1.0},
}
B3_BAND_REL = 0.25


def assert_deterministic(summary: dict) -> None:
    """B1/B2（确定性算法）：离散指标重复样本必须**逐项相等**。

    用「样本值全等」而非 `variance == 0.0`：后者受浮点求和舍入影响会出现
    `4.6e-33` 这类假阳性（值其实完全一致）。
    **B1/B2 若出现非确定 → 实现 bug**，必须修实现而不是放宽判定。
    """
    bad = []
    for wl, cells in summary.items():
        for cond in ("B1", "B2"):
            s = cells[cond]
            for key in DISCRETE_METRICS:
                values = s[key]["values"]
                if any(v != values[0] for v in values[1:]):
                    bad.append(f"{wl}/{cond}/{key}: values={values!r}")
    if bad:
        raise AssertionError(
            "B1/B2 离散指标非确定（确定性算法被破坏，必须修实现而非放宽）：\n"
            + "\n".join(bad))


def assert_b3_bounded(summary: dict) -> None:
    """B3（并发）：离散指标落在预注册期望值 ±`B3_BAND_REL` 的**有界带**内。

    - 期望值 > 0（`conflict_rate_detected` / `wasted_work`）：所有重复样本
      ∈ [E·(1−band), E·(1+band)]；
    - 期望值 == 0（`lost_update_rate` / `conflict_rate_silent`）：带退化为
      **严格 0** —— 任何非零即 bug（如 K-28 静默丢写）。
    """
    bad = []
    for wl, cells in summary.items():
        s = cells["B3"]
        for key in DISCRETE_METRICS:
            values = s[key]["values"]
            expected = B3_EXPECTED[wl][key]
            if expected == 0.0:
                if any(v != 0.0 for v in values):
                    bad.append(f"{wl}/B3/{key}: 期望严格 0，实测 {values!r}")
            else:
                lo = expected * (1.0 - B3_BAND_REL)
                hi = expected * (1.0 + B3_BAND_REL)
                if any(v < lo or v > hi for v in values):
                    bad.append(f"{wl}/B3/{key}: 期望 [{lo:.4f}, {hi:.4f}]，"
                               f"实测 {values!r}")
    if bad:
        raise AssertionError(
            "B3 离散指标超出有界带（并发抖动过大或实现 bug）：\n"
            + "\n".join(bad))


# ---------------------------------------------------------------- 预注册判定
def judge_cell(name: str, b2: dict, b3: dict) -> dict:
    """按格预注册判定；`degraded=True`（git 不可用）时**判定无效**。"""
    degraded = bool(b2.get("degraded") or b3.get("degraded"))
    if degraded:
        return {
            "kind": "degraded_invalid",
            "role": "环境降级，判定无效",
            "degraded": True,
            "reason": "git_unavailable_environment_degraded",
            "b2_lost": b2["lost_update_rate"]["mean"],
            "b3_lost": b3["lost_update_rate"]["mean"],
            "verdict": None,
            "pass": False,
        }
    if name == "C0":
        ok = (b2["lost_update_rate"]["mean"] == 0.0
              and b3["lost_update_rate"]["mean"] == 0.0)
        return {"kind": "sanity", "role": "不参与优越性判定", "degraded": False,
                "b2_lost": b2["lost_update_rate"]["mean"],
                "b3_lost": b3["lost_update_rate"]["mean"],
                "pass": ok}
    # 主终点（预注册在 C2）：非劣 + 等价（严格 <，平局不成立）
    b2_lost = b2["lost_update_rate"]["mean"]
    b3_lost = b3["lost_update_rate"]["mean"]
    non_inferior = b3_lost <= b2_lost
    equivalent = (b3_lost - b2_lost) < MARGIN
    return {
        "kind": "non_inferiority+equivalence",
        "role": "主终点" if name == "C2" else "次要（不参与优越性判定）",
        "degraded": False,
        "b2_lost": b2_lost, "b3_lost": b3_lost,
        "diff_b3_minus_b2": b3_lost - b2_lost,
        "margin": MARGIN,
        "non_inferior": non_inferior,
        "equivalent": equivalent,
        "verdict": non_inferior and equivalent,
    }


# ---------------------------------------------------------------- 二级指标
def measure_event_replay_cost(sizes) -> dict:
    """`event_replay_cost`：扫 `_rebuild_from_events`。"""
    out = {}
    for n in sizes:
        h = C._make_harness(0.0)
        for i in range(n):
            blob = h.blobs.put(M.canonical_bytes({"v": i}))
            h.event_log.append(EventKind.RESOURCE_COMMITTED, f"r{i}",
                              {"resource": f"r{i}", "version": 1,
                               "blob_hash": blob, "prev_hash": ""},
                              producer="A-x")
        t0 = time.perf_counter()
        h.workspace._rebuild_from_events()
        out[str(n)] = time.perf_counter() - t0
    return out


def measure_recovery_replay_cost(n_events: int) -> dict:
    """`recovery_replay_cost (in-memory store)`：分相位 rebuild / replay / invariant。

    **不等于**端到端持久恢复（内存态 `CheckpointStore`，见 report.md 测量局限）。
    """
    h = C._make_harness(0.0)
    plan = h.planner.plan("R", [{"task_id": "T1", "min_budget": 10}])
    h.plans["R"] = plan
    acb = h.dispatcher.try_admit(plan["tasks"]["T1"], "R", plan=plan)
    h.acbs[acb.id] = acb
    for i in range(n_events):
        blob = h.blobs.put(M.canonical_bytes({"v": i}))
        h.event_log.append(EventKind.RESOURCE_COMMITTED, f"r{i}",
                          {"resource": f"r{i}", "version": 1,
                           "blob_hash": blob, "prev_hash": ""},
                          producer=acb.id)
    ref = h.checkpoints.save(acb)

    phases = {"rebuild": 0.0, "replay": 0.0, "invariant": 0.0}
    orig = (h.effects.rebuild_from, h.comp_journal.rebuild_from,
            h._replay_events, h.budget.check_invariants)

    def timed(fn, key):
        def wrap(*a, **k):
            t0 = time.perf_counter()
            r = fn(*a, **k)
            phases[key] += time.perf_counter() - t0
            return r
        return wrap

    h.effects.rebuild_from = timed(orig[0], "rebuild")
    h.comp_journal.rebuild_from = timed(orig[1], "rebuild")
    h._replay_events = timed(orig[2], "replay")
    h.budget.check_invariants = timed(orig[3], "invariant")
    try:
        t0 = time.perf_counter()
        h.recover(ref)
        total = time.perf_counter() - t0
    finally:
        h.effects.rebuild_from, h.comp_journal.rebuild_from, h._replay_events, \
            h.budget.check_invariants = orig
    phases["total"] = total
    return phases


def _scalability_once(n: int, delay: float) -> float:
    """单次 N-agent 并发 wall_time（barrier 对齐后计时）。"""
    from harness import Action, Decision, Write
    h = C._make_harness(delay)
    C._seed(h, {f"r{i}.json": {"v": 0} for i in range(n)})
    plan = h.planner.plan("S", [{"task_id": f"T{i}", "committed": [f"r{i}.json"],
                                 "min_budget": 10} for i in range(n)])
    h.plans["S"] = plan
    acbs = C._admit_all(h, plan)
    h.scheduler = C._NoopScheduler()
    decisions = [Decision(turn_seq=1, actions=[
        Action(kind="stage", resource=f"r{i}.json", content={"v": 1}),
        Action(kind="commit", writes=[Write(resource=f"r{i}.json")])])
        for i in range(n)]
    C._pre_stage(h, acbs, decisions)
    barrier = threading.Barrier(n)

    def worker(acb, d):
        barrier.wait()
        return h.run_task(acb, [C._commit_actions(d)])

    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=n) as ex:
        futs = [ex.submit(worker, acb, d)
                for acb, d in zip(acbs, decisions)]
        for f in futs:
            f.result()
    return time.perf_counter() - t0


def measure_scalability(ns, delay: float, *, repeats: int = 5) -> dict:
    """`scalability_curve`：N ∈ {1,2,4,8} 的并发 wall_time / speedup。

    §2.6 修：基线**先 warmup，再多轮重采样取中位数**（旧实现用单次最冷
    N=1 基线，导致 speedup 被冷启动噪声压到 <1，与 result.json 自相矛盾）。
    """
    for n in ns:                                   # warmup（不入统计）
        _scalability_once(n, delay)
    out = {}
    base = None
    for n in ns:
        samples = [_scalability_once(n, delay) for _ in range(repeats)]
        wall = statistics.median(samples)
        if base is None:
            base = wall
        out[str(n)] = {
            "wall_time": wall,
            "speedup": base / wall if wall else 0.0,
            "samples": samples,
            "min": min(samples),
            "median": wall,
        }
    return out


# ---------------------------------------------------------------- main
def main() -> int:
    ap = argparse.ArgumentParser(description="S7 基准（B1/B2/B3 × C0–C3）")
    ap.add_argument("--quick", action="store_true", help="n=3 冒烟")
    ap.add_argument("--n", type=int, default=None, help="每格重复次数（>=3）")
    ap.add_argument("--delay", type=float, default=0.002, help="per_action_delay")
    ap.add_argument("--seed", type=int, default=DEFAULT_SEED)
    ap.add_argument("--out", type=str, default=None, help="JSON 输出路径")
    args = ap.parse_args()

    _reexec_hashseed()
    random.seed(args.seed)

    n = args.n if args.n is not None else (3 if args.quick else 30)
    n = max(3, n)

    env = collect_env()
    warmup()

    raw = {}
    for wl in WORKLOADS:
        raw[wl] = {}
        for cond in CONDITIONS:
            raw[wl][cond] = run_cell(wl, cond, n, args.delay)

    # ---- 三条件 freeze 对齐（G3） ----
    freeze_ok = all(
        raw[wl]["B1"][0].freeze == raw[wl]["B2"][0].freeze == raw[wl]["B3"][0].freeze
        for wl in WORKLOADS)

    # ---- oracle 自检（独立 oracle 驱动 expected；串行 B1 必须与之一致）----
    from bench.workloads import oracle_self_check
    oracle_ok = {}
    for wl in WORKLOADS:
        w = WORKLOADS[wl]
        oracle = w.expected_by_resource
        b1_final = raw[wl]["B1"][0].final_state
        matches_b1 = all(
            M.canon_hash(b1_final.get(k)) == M.canon_hash(v)
            for k, v in oracle.items())
        oracle_ok[wl] = bool(oracle_self_check(w) and matches_b1)
    assert all(oracle_ok.values()), f"oracle 自检失败：{oracle_ok}"

    # ---- 汇总 + 预注册判定 ----
    summary = {}
    judgments = {}
    for wl in WORKLOADS:
        s = {cond: summarize(raw[wl][cond], WORKLOADS[wl]) for cond in CONDITIONS}
        summary[wl] = s
        judgments[wl] = judge_cell(wl, s["B2"], s["B3"])

    # 确定性判定：B1/B2 逐值相等；B3 有界带（并发交错 → 见模块 docstring）
    assert_deterministic(summary)
    assert_b3_bounded(summary)

    # ---- 速度：B1 vs B3（时序，配对 bootstrap CI） ----
    speed = {}
    for wl in WORKLOADS:
        b1w = [r.wall_time for r in raw[wl]["B1"]]
        b3w = [r.wall_time for r in raw[wl]["B3"]]
        ci = M.bootstrap_ci([b1w, b3w], seed=args.seed)
        speed[wl] = {
            "b1_wall_mean": sum(b1w) / len(b1w),
            "b3_wall_mean": sum(b3w) / len(b3w),
            "speedup_b1_over_b3": (sum(b1w) / sum(b3w)) if sum(b3w) else 0.0,
            "paired_bootstrap_ci": ci,
        }

    secondary = {}
    if not args.quick:
        secondary["recovery_replay_cost"] = {
            "phases_s": measure_recovery_replay_cost(2000),
            "note": "in-memory CheckpointStore；≠ 端到端持久恢复",
        }
        secondary["event_replay_cost"] = {
            "rebuild_s": measure_event_replay_cost([1000, 10000, 100000]),
        }
        secondary["scalability_curve"] = {
            "by_n": measure_scalability([1, 2, 4, 8], args.delay),
        }

    report = {
        "schema_version": "1.1",
        "quick": args.quick,
        "n_per_cell": n,
        "seed": args.seed,
        "per_action_delay": args.delay,
        "env": env,
        "freeze_3_conditions_equal": freeze_ok,
        "oracle_self_check": oracle_ok,
        "summary": summary,
        "judgments": judgments,
        "speed": speed,
        "secondary": secondary,
    }

    text = json.dumps(report, ensure_ascii=True, indent=2, sort_keys=True)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(text + "\n")
    else:
        print(text)

    # ---- 控制台摘要 ----
    print("=" * 72, file=sys.stderr)
    print(f"S7 基准  n={n}  delay={args.delay}s  seed={args.seed}", file=sys.stderr)
    print(f"freeze 三条件相等: {freeze_ok}", file=sys.stderr)
    print(f"C0 oracle 自检: {oracle_ok.get('C0')}", file=sys.stderr)
    print(f"全部负载 oracle 自检: {oracle_ok}", file=sys.stderr)
    for wl in WORKLOADS:
        j = judgments[wl]
        print(f"  {wl}: B2_lost={j.get('b2_lost')} B3_lost={j.get('b3_lost')} "
              f"-> {j.get('kind')} verdict={j.get('verdict')} "
              f"({j.get('role')})", file=sys.stderr)
        b3 = summary[wl]["B3"]
        cr, ww = b3["conflict_rate_detected"], b3["wasted_work"]
        print(f"        B3 带内分布: conflict=[{cr['min']:.3f}, {cr['max']:.3f}] "
              f"wasted=[{ww['min']:.3f}, {ww['max']:.3f}]", file=sys.stderr)
    print("=" * 72, file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
