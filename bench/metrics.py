"""外部差分度量 + 统计（对应 `docs/s7-benchmark-and-coverage.md` v1.1 §2.3/§2.6）。

- **资源级外部差分**：`expected[resource] = H(oracle_merge(...))`；
  `lost = [w for w in all_writes if final[w.resource] != expected[w.resource]]`。
- **键级归因**：`missing_keys = set(expected) - set(final)`；`extra_keys` 同理；
  `silent = 提交成功但 missing_keys 非空`。
- 列表按**规范序**（排序后）比较，避免 GROW_ONLY 并集的顺序差异造成假丢写。
- 离散指标重复 n 次验确定性（方差应为 0）；时序指标配对 bootstrap CI。
"""

from __future__ import annotations

import hashlib
import json
import math

from harness import Status
from harness.storage.blob import canonical_bytes

from .workloads import Workload, collect_writes

_ABSENT = object()


# ---------------------------------------------------------------- 规范形
def canonical(v):
    """规范形：dict 键排序；list 排序（GROW_ONLY/SET 并集与顺序无关）。"""
    if isinstance(v, dict):
        return {k: canonical(v[k]) for k in sorted(v)}
    if isinstance(v, list):
        return sorted((canonical(x) for x in v),
                      key=lambda x: json.dumps(x, sort_keys=True, ensure_ascii=True))
    return v


def canon_hash(v) -> str:
    if v is None:
        return "MISSING"
    return hashlib.sha256(canonical_bytes(canonical(v))).hexdigest()


# ---------------------------------------------------------------- 差分
def _diff_keys(exp, fin, path: str = "") -> dict[str, set]:
    """递归键级差分：**分三路**返回 JSON Pointer 路径集合。

    - `missing`：`exp` 有而 `fin` 无（含 list 尾部缺失元素）；
    - `extra`：`fin` 有而 `exp` 无（含 list 尾部多余元素）；
    - `changed`：两侧都有但值不同（含 list 长度不一致的 `__len__` 标记）。

    旧实现把三路混在一个集合里，`missing_keys` 同时含 extra 键、且与
    `extra_keys` 重复计数（语义错误）。现在三路互斥。
    """
    missing: set = set()
    extra: set = set()
    changed: set = set()
    if exp is _ABSENT:
        extra.add(path or "/")
        return {"missing": missing, "extra": extra, "changed": changed}
    if fin is _ABSENT:
        missing.add(path or "/")
        return {"missing": missing, "extra": extra, "changed": changed}
    if isinstance(exp, dict) and isinstance(fin, dict):
        for k in set(exp) - set(fin):
            missing.add(f"{path}/{k}")
        for k in set(fin) - set(exp):
            extra.add(f"{path}/{k}")
        for k in set(exp) & set(fin):
            sub = _diff_keys(exp[k], fin[k], f"{path}/{k}")
            missing |= sub["missing"]
            extra |= sub["extra"]
            changed |= sub["changed"]
    elif isinstance(exp, list) and isinstance(fin, list):
        ce, cf = canonical(exp), canonical(fin)
        for i, (x, y) in enumerate(zip(ce, cf)):
            if x != y:
                changed.add(f"{path}/{i}")
        for i in range(len(cf), len(ce)):
            missing.add(f"{path}/{i}")
        for i in range(len(ce), len(cf)):
            extra.add(f"{path}/{i}")
        if len(ce) != len(cf):
            changed.add(f"{path}/__len__")
    elif exp != fin:
        changed.add(path or "/")
    return {"missing": missing, "extra": extra, "changed": changed}


def _key_diff(exp, fin, path: str = "") -> set:
    """兼容包装：三路差分的并集（保留旧名字，语义改为「不一致路径」）。"""
    d = _diff_keys(exp, fin, path)
    return d["missing"] | d["extra"] | d["changed"]


def resource_diff(workload: Workload, final_state: dict) -> dict:
    """资源级外部差分 + 键级归因（递归 JSON Pointer，missing/extra/changed 分路）。"""
    expected = workload.expected_by_resource
    writes = collect_writes(workload.decisions)
    all_writes = [(r, c) for r, cs in writes.items() for c in cs]

    lost_resources = []
    missing_keys: dict[str, set] = {}
    extra_keys: dict[str, set] = {}
    changed_keys: dict[str, set] = {}
    for r, exp in expected.items():
        fin = final_state.get(r, _ABSENT)
        if fin is _ABSENT or canon_hash(fin) != canon_hash(exp):
            lost_resources.append(r)
        diff = _diff_keys(exp, fin)
        if diff["missing"]:
            missing_keys[r] = diff["missing"]
        if diff["extra"]:
            extra_keys[r] = diff["extra"]
        if diff["changed"]:
            changed_keys[r] = diff["changed"]

    lost_writes = [(r, c) for r, c in all_writes if r in set(lost_resources)]
    return {
        "lost_resources": lost_resources,
        "lost_writes": lost_writes,
        "lost_update_rate": (len(lost_writes) / len(all_writes)) if all_writes else 0.0,
        "missing_keys": missing_keys,
        "extra_keys": extra_keys,
        "changed_keys": changed_keys,
        "all_writes": all_writes,
    }


def conflict_metrics(result, lost_writes) -> dict:
    """conflict_rate 分 detected / silent；serialization_rate；wasted_work。

    - `detected`：B1/B3 的 `Result != OK`；B2 的行级合并冲突。
    - `silent`：提交成功但资源最终缺键（即丢失写且该资源无 detected 冲突）。
    """
    ok_resources: set = set()
    if result.condition == "B2":
        detected = result.merge_conflicts
        total = len(result.turn_replay_counts)
        ok_resources = set()          # B2 无逐笔提交记录
    else:
        detected = 0
        for resources, r in result.commit_results:
            if r.status != Status.OK:
                detected += 1
            else:
                ok_resources.update(resources)
        total = len(result.commit_results)

    silent = sum(1 for r, _ in lost_writes if r in ok_resources)
    total_turns = max(len(result.turn_replay_counts), 1)
    rebase_retries = sum(max(0, c - 1) for c in result.turn_replay_counts)

    return {
        "detected_conflicts": detected,
        "total_commits": total,
        "conflict_rate_detected": detected / total if total else 0.0,
        "silent_lost_writes": silent,
        "conflict_rate_silent": silent / len(lost_writes) if lost_writes else (
            silent / total if total else 0.0),
        "serialization_rate": (result.lock_wait_s / result.wall_time
                               if result.wall_time else 0.0),
        "wasted_work": rebase_retries / total_turns,
    }


def cell_metrics(result, workload: Workload) -> dict:
    diff = resource_diff(workload, result.final_state)
    conf = conflict_metrics(result, diff["lost_writes"])
    lat = sorted(result.commit_latencies)
    n = len(lat)

    def pct(p: float) -> float:
        if not lat:
            return 0.0
        idx = min(n - 1, max(0, math.ceil(p / 100.0 * n) - 1))
        return lat[idx]

    total_commits = conf["total_commits"]
    return {
        "lost_update_rate": diff["lost_update_rate"],
        "lost_resources": diff["lost_resources"],
        "missing_keys": {k: sorted(v) for k, v in diff["missing_keys"].items() if v},
        "extra_keys": {k: sorted(v) for k, v in diff["extra_keys"].items() if v},
        "changed_keys": {k: sorted(v) for k, v in diff["changed_keys"].items() if v},
        "conflict_rate_detected": conf["conflict_rate_detected"],
        "conflict_rate_silent": conf["conflict_rate_silent"],
        "silent_lost_writes": conf["silent_lost_writes"],
        "serialization_rate": conf["serialization_rate"],
        "wasted_work": conf["wasted_work"],
        "throughput": total_commits / result.wall_time if result.wall_time else 0.0,
        "commit_latency_p50": pct(50),
        "commit_latency_p99": pct(99),
        "wall_time": result.wall_time,
        "detected_conflicts": conf["detected_conflicts"],
        "total_commits": total_commits,
        "turn_replay_counts": list(result.turn_replay_counts),
    }


# ---------------------------------------------------------------- 统计
def variance(values) -> float:
    n = len(values)
    if n < 2:
        return 0.0
    mean = sum(values) / n
    return sum((x - mean) ** 2 for x in values) / (n - 1)


def bootstrap_ci(pairs, *, iterations: int = 2000, alpha: float = 0.05,
                 seed: int = 0):
    """配对 bootstrap CI：配对单位 = 同 seed/同 workload/同迭代序号。

    `pairs` 为长度 2 的序列（如 [B1, B3] 的时序样本），返回每对的差值 CI。
    """
    import random
    rng = random.Random(seed)
    if len(pairs) != 2 or any(len(p) < 2 for p in pairs):
        return None
    n = min(len(pairs[0]), len(pairs[1]))
    diffs = [pairs[0][i] - pairs[1][i] for i in range(n)]
    boot = []
    for _ in range(iterations):
        sample = [diffs[rng.randrange(n)] for _ in range(n)]
        boot.append(sum(sample) / n)
    boot.sort()
    lo = boot[int(iterations * alpha / 2)]
    hi = boot[min(iterations - 1, int(iterations * (1 - alpha / 2)))]
    return {"n": n, "mean_diff": sum(diffs) / n, "ci_low": lo, "ci_high": hi}


def paired_time_samples(runs: dict) -> dict:
    """把 {condition: [wall_time,...]} 配对成 bootstrap 输入。"""
    return runs
