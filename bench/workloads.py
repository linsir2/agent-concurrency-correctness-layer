"""C0–C3 负载 + 决策序列固化（对应 `docs/s7-benchmark-and-coverage.md` v1.1 §2.2/§2.3）。

要点：

- `freeze` **先 `dataclasses.asdict`**（`Decision` 是 dataclass，直接 `json.dumps`
  会 `TypeError`），再 sha256；schema 区分 `logical_resource`（B3 用）与
  `worktree_path`（B2 用），**freeze 只覆盖逻辑名**。
- 独立 oracle：对 `GROW_ONLY`/`SET` 直接做「并集 additions − removals」，
  **不复用 `merge3`**；`expected_by_resource` **由 `oracle_for` 驱动**
  （不再是手写常量，避免「手写 expected == B1」的同义反复）。
- 每个负载返回 `(decisions, expected_by_resource, initial_state)`。
"""

from __future__ import annotations

import copy
import dataclasses
import hashlib
import json
from dataclasses import dataclass, field

from harness import Action, Decision, Write


# ---------------------------------------------------------------- 2.2 固化
def freeze(decisions) -> str:
    """决策序列固化：先 `asdict`（否则 TypeError）→ 紧凑 JSON → sha256。"""
    payload = [dataclasses.asdict(d) for d in decisions]
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=True)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------- schema
@dataclass(frozen=True)
class ResourceSpec:
    """资源 schema：`logical_resource`（B3/Harness 用）与 `worktree_path`（B2 用）。"""

    logical_resource: str
    worktree_path: str


def _spec(logical: str, path: str | None = None) -> ResourceSpec:
    return ResourceSpec(logical, path or logical)


# ---------------------------------------------------------------- 决策构造
def _stage(resource: str, content) -> Action:
    return Action(kind="stage", resource=resource, content=content)


def _commit(resources) -> Action:
    return Action(kind="commit", writes=[Write(resource=r) for r in resources])


def _decision(*actions: Action) -> Decision:
    return Decision(turn_seq=1, actions=list(actions))


# ---------------------------------------------------------------- 负载容器
@dataclass(frozen=True)
class Workload:
    """一个负载格（§2.3）：`decisions` / `expected` / `initial_state` + schema/策略。"""

    name: str
    decisions: list
    expected_by_resource: dict
    initial_state: dict
    schema: dict = field(default_factory=dict)          # logical -> ResourceSpec
    merge_policies: dict = field(default_factory=dict)   # logical -> {pointer: policy}
    file_format: str = "pretty"                          # B2 序列化：compact | pretty
    plan_spec: list = field(default_factory=list)


# ---------------------------------------------------------------- 独立 oracle
_MISSING = object()


def _eq(a, b) -> bool:
    """类型严格的深比较（与 `rebase._strict_eq` 语义一致，但独立实现）。"""
    if isinstance(a, bool) or isinstance(b, bool):
        return type(a) is type(b) and a == b
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return type(a) is type(b) and a == b
    if isinstance(a, dict) and isinstance(b, dict):
        if set(a) != set(b):
            return False
        return all(_eq(a[k], b[k]) for k in a)
    if isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b):
            return False
        return all(_eq(x, y) for x, y in zip(a, b))
    return type(a) is type(b) and a == b


def _contains(x, seq) -> bool:
    return any(_eq(x, y) for y in seq)


def oracle_merge(base, writes):
    """独立 oracle：`GROW_ONLY`/`SET` 直接做「并集 additions − removals」。

    对 dict 取键并集递归合并；对 list 取「base 保留 + 所有写新增 − 被任一写删除」；
    标量取一致值（冲突时取最后一笔 —— 本基准负载不触发标量冲突）。
    """
    return _oracle_field(base, writes)


def _oracle_field(base, writes):
    if isinstance(base, dict):
        keys = set(base)
        for w in writes:
            if isinstance(w, dict):
                keys |= set(w)
        out = {}
        for k in keys:
            b = base.get(k, _MISSING)
            ws = [w.get(k, _MISSING) for w in writes if isinstance(w, dict)]
            out[k] = _oracle_field(b, ws)
        return out
    if isinstance(base, list):
        added = []
        for w in writes:
            if isinstance(w, list):
                for x in w:
                    if not _contains(x, base) and not _contains(x, added):
                        added.append(x)
        removed = [x for x in base
                   if not any(_contains(x, w) for w in writes if isinstance(w, list))]
        out = [x for x in base if not _contains(x, removed)]
        for x in added:
            if not _contains(x, out):
                out.append(x)
        return out
    vals = [w for w in writes if w is not _MISSING]
    if not vals:
        return copy.deepcopy(base)
    changed = [v for v in vals if not _eq(v, base)]
    if not changed:
        return copy.deepcopy(base)
    if all(_eq(v, changed[0]) for v in changed):
        return copy.deepcopy(changed[0])
    return copy.deepcopy(changed[-1])   # 冲突：取最后一笔（本基准负载不触发）


def collect_writes(decisions) -> dict:
    """从决策里抽出「资源 → [内容]」的写集合（含重复，按 agent 顺序）。"""
    by_resource: dict[str, list] = {}
    for d in decisions:
        for a in d.actions:
            if a.kind == "stage":
                by_resource.setdefault(a.resource, []).append(a.content)
    return by_resource


def oracle_for(decisions, initial_state) -> dict:
    """由决策 + 初态算出资源级期望终态。"""
    writes = collect_writes(decisions)
    return {r: oracle_merge(initial_state[r], writes.get(r, []))
            for r in initial_state}


def oracle_self_check(workload: Workload) -> bool:
    """自检：无冲突负载下 oracle 结果必须 == 串行 B1 的结果（在 run_bench 里对 C0 断言）。"""
    return oracle_for(workload.decisions, workload.initial_state) == workload.expected_by_resource


# ---------------------------------------------------------------- C0 零冲突
def _c0() -> Workload:
    initial = {
        "a.json": {"v": 0},
        "b.json": {"v": 0},
        "c.json": {"v": 0},
    }
    decisions = [
        _decision(_stage("a.json", {"v": 1}), _commit(["a.json"])),
        _decision(_stage("b.json", {"v": 1}), _commit(["b.json"])),
        _decision(_stage("c.json", {"v": 1}), _commit(["c.json"])),
    ]
    expected = oracle_for(decisions, initial)
    return Workload(
        name="C0", decisions=decisions, expected_by_resource=expected,
        initial_state=initial,
        schema={r: _spec(r) for r in initial},
        plan_spec=[{"task_id": "T1", "committed": ["a.json"], "min_budget": 10},
                   {"task_id": "T2", "committed": ["b.json"], "min_budget": 10},
                   {"task_id": "T3", "committed": ["c.json"], "min_budget": 10}],
        file_format="pretty",
    )


# ---------------------------------------------------------------- C1 弱重叠
def _flat(keys, **vals) -> dict:
    return {k: vals.get(k, 0) for k in keys}


_C1_KEYS = [f"k{i:02d}" for i in range(20)]
_C1_C1_HITS = ("k03", "k10", "k17")
_C1_C2_HITS = ("k05", "k12", "k19")


def _c1() -> Workload:
    """3 agent 共享 `types.ts` + `config.json`，各自改**相距较远**的字段。

    「弱重叠」：改动行不相邻 → B2 行级合并可干净成功；结构级 `merge3` 亦干净。
    """
    initial = {
        "types.ts": _flat(_C1_KEYS),
        "config.json": _flat(_C1_KEYS),
        "own1.json": {"w": 0},
        "own2.json": {"w": 0},
        "own3.json": {"w": 0},
    }
    agents = []
    for i, hit in enumerate(_C1_C1_HITS):
        t = _flat(_C1_KEYS, **{hit: 1})
        c = _flat(_C1_KEYS, **{_C1_C2_HITS[i]: 1})
        agents.append(_decision(
            _stage("types.ts", t), _commit(["types.ts"]),
            _stage("config.json", c), _commit(["config.json"]),
            _stage(f"own{i + 1}.json", {"w": 1}), _commit([f"own{i + 1}.json"])))
    expected = oracle_for(agents, initial)
    return Workload(
        name="C1", decisions=agents, expected_by_resource=expected,
        initial_state=initial,
        schema={r: _spec(r) for r in initial},
        plan_spec=[{"task_id": "T1", "committed": ["types.ts", "config.json", "own1.json"],
                    "min_budget": 10},
                   {"task_id": "T2", "committed": ["types.ts", "config.json", "own2.json"],
                    "min_budget": 10},
                   {"task_id": "T3", "committed": ["types.ts", "config.json", "own3.json"],
                    "min_budget": 10}],
        file_format="pretty",
    )


# ---------------------------------------------------------------- C2 强重叠
def _c2() -> Workload:
    """同文件不同字段 + `GROW_ONLY` 数组。

    主终点：B2 行级合并（紧凑单行 JSON → 两侧改同一行）会真丢写；
    B3 结构级 `merge3` 干净合并 → 不丢写。
    """
    initial = {"shared.json": {"a": 0, "b": 0, "items": ["base"]}}
    decisions = [
        _decision(_stage("shared.json", {"a": 1, "b": 0, "items": ["base", "x"]}),
                  _commit(["shared.json"])),
        _decision(_stage("shared.json", {"a": 0, "b": 2, "items": ["base", "y"]}),
                  _commit(["shared.json"])),
    ]
    expected = oracle_for(decisions, initial)
    policy = {"/items": "GROW_ONLY"}
    return Workload(
        name="C2", decisions=decisions, expected_by_resource=expected,
        initial_state=initial,
        schema={"shared.json": _spec("shared.json")},
        merge_policies={"shared.json": policy},
        plan_spec=[{"task_id": "T1", "committed": ["shared.json"], "min_budget": 10,
                    "merge_policy": policy},
                   {"task_id": "T2", "committed": ["shared.json"], "min_budget": 10,
                    "merge_policy": policy}],
        file_format="compact",
    )


# ---------------------------------------------------------------- C3 write skew
_C3_KEYS = [f"c{i:02d}" for i in range(20)]
_C3_HITS = ("c05", "c15")


def _c3() -> Workload:
    """共享契约 + 各改不同文件；`logical_deps` **未声明**（🟡 已知未覆盖）。"""
    initial = {
        "contract.json": _flat(_C3_KEYS),
        "impl1.ts": {"x": 0},
        "impl2.ts": {"y": 0},
    }
    decisions = [
        _decision(_stage("contract.json", _flat(_C3_KEYS, **{_C3_HITS[0]: 1})),
                  _commit(["contract.json"]),
                  _stage("impl1.ts", {"x": 1}), _commit(["impl1.ts"])),
        _decision(_stage("contract.json", _flat(_C3_KEYS, **{_C3_HITS[1]: 1})),
                  _commit(["contract.json"]),
                  _stage("impl2.ts", {"y": 1}), _commit(["impl2.ts"])),
    ]
    expected = oracle_for(decisions, initial)
    return Workload(
        name="C3", decisions=decisions, expected_by_resource=expected,
        initial_state=initial,
        schema={r: _spec(r) for r in initial},
        plan_spec=[{"task_id": "T1", "committed": ["contract.json", "impl1.ts"],
                    "min_budget": 10},
                   {"task_id": "T2", "committed": ["contract.json", "impl2.ts"],
                    "min_budget": 10}],
        file_format="pretty",
    )


WORKLOADS: dict[str, Workload] = {
    "C0": _c0(),
    "C1": _c1(),
    "C2": _c2(),
    "C3": _c3(),
}


def get_workload(name: str) -> Workload:
    return WORKLOADS[name]
