"""B1/B2/B3 执行器（对应 `docs/s7-benchmark-and-coverage.md` v1.1 §2.4/§2.5）。

- **B1 串行**：同一 decisions 串行跑（先统一 pre-stage 到 v0，再串行提交，
  经 Harness `run_task` 走真实 `commit_batch` + `merge3` 回路）。
- **B2 worktree**：每 agent 一个临时目录；行级 3-way 合并用
  `shutil.which("git")` 探测的 `git merge-file`；不可用退纯 Python 行级合并并标
  `degraded=True`；合并前把文件解析回 JSON 再算 canonical hash；subprocess 耗时单独打点。
- **B3 共享层**：`ThreadPoolExecutor` + `Barrier(N)` 并发 `run_task`（经 Harness）。

三条件启动前各断言 `freeze(decisions)` 相等（G3）。
"""

from __future__ import annotations

import copy
import difflib
import json
import os
import shutil
import subprocess
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

from harness import Decision, Harness, Mode, Status

from .workloads import Workload, _spec, freeze


# ---------------------------------------------------------------- 结果容器
@dataclass
class ConditionResult:
    condition: str
    workload: str
    freeze: str
    final_state: dict
    results: list = field(default_factory=list)          # 每 agent 终态 Status
    wall_time: float = 0.0
    turn_replay_counts: list = field(default_factory=list)
    commit_latencies: list = field(default_factory=list)  # 每次 commit_batch 耗时
    commit_results: list = field(default_factory=list)    # [(resources, Result)]
    lock_wait_s: float = 0.0
    merge_conflicts: int = 0                              # B2：行级合并冲突数
    degraded: bool = False                                # B2：git 不可用
    subprocess_s: float = 0.0                             # B2：subprocess 耗时
    subprocess_calls: int = 0


# ---------------------------------------------------------------- 基础设施
def _make_harness(per_action_delay: float, run_limits: int = 1000) -> Harness:
    h = Harness(run_limits=run_limits, per_action_delay=per_action_delay)
    h.policy.default_token = h.policy.issue(
        "*", tools={"*"}, paths={"*"}, network={"*"}, data_class="*")
    return h


def _seed(h: Harness, initial_state: dict) -> None:
    for r, c in initial_state.items():
        h.workspace.put(r, copy.deepcopy(c))


def _build_plan(h: Harness, workload: Workload, run_id: str) -> dict:
    plan = h.planner.plan(run_id, workload.plan_spec)
    h.plans[run_id] = plan
    return plan


def _admit_all(h: Harness, plan: dict) -> list:
    acbs = []
    for tid in plan["tasks"]:
        acb = h.dispatcher.try_admit(plan["tasks"][tid], plan["run_id"], plan=plan)
        h.acbs[acb.id] = acb
        h.run_coordinator.register(acb)
        acbs.append(acb)
    return acbs


def _pre_stage(h: Harness, acbs, decisions) -> None:
    """统一 pre-stage 到 v0：所有 agent 基于同一 base 读版本。"""
    for acb, d in zip(acbs, decisions):
        for a in d.actions:
            if a.kind == "stage":
                h.workspace.stage(acb, a.resource, copy.deepcopy(a.content))


class _NoopScheduler:
    """B3 用：避免跨线程调度器把他人 ADMITTED 的 ACB 二次拉起。"""

    def push(self, task_id):  # noqa: D401
        return None

    def pop(self):
        return None

    def is_empty(self) -> bool:
        return True


class _Instrument:
    """打点：commit_batch 耗时/结果、锁等待耗时。"""

    def __init__(self, h: Harness) -> None:
        self.h = h
        self.commit_latencies: list = []
        self.commit_results: list = []
        self.lock_wait_s = 0.0
        self._orig_commit = h.commit_batch
        self._orig_acquire = h.workspace._acquire_all

    def __enter__(self):
        h = self.h
        orig_commit = self._orig_commit
        orig_acquire = self._orig_acquire
        lat = self.commit_latencies
        res = self.commit_results
        wait = self

        def commit_spy(acb, writes, mode=Mode.COMMIT):
            t0 = time.perf_counter()
            r = orig_commit(acb, writes, mode)
            lat.append(time.perf_counter() - t0)
            res.append((tuple(w.resource for w in writes), r))
            return r

        def acquire_spy(resources, timeout=None):
            t0 = time.perf_counter()
            out = orig_acquire(resources, timeout)
            wait.lock_wait_s += time.perf_counter() - t0
            return out

        h.commit_batch = commit_spy
        h.workspace._acquire_all = acquire_spy
        return self

    def __exit__(self, *exc):
        self.h.commit_batch = self._orig_commit
        self.h.workspace._acquire_all = self._orig_acquire
        return False


def _commit_actions(d: Decision) -> Decision:
    return Decision(turn_seq=1, actions=[a for a in d.actions if a.kind == "commit"])


def _final_state(h: Harness, initial_state: dict) -> dict:
    return {r: h.workspace.get(r) for r in initial_state}


# ---------------------------------------------------------------- B1 串行
def run_b1(workload: Workload, *, per_action_delay: float = 0.0,
           run_id: str | None = None) -> ConditionResult:
    h = _make_harness(per_action_delay)
    _seed(h, workload.initial_state)
    plan = _build_plan(h, workload, run_id or f"{workload.name}-B1")
    acbs = _admit_all(h, plan)
    _pre_stage(h, acbs, workload.decisions)

    results, replay = [], []
    with _Instrument(h) as inst:
        t0 = time.perf_counter()
        for acb, d in zip(acbs, workload.decisions):
            st = h.run_task(acb, [_commit_actions(d)])
            results.append(st)
            replay.append(1 + acb.rebase_attempts)
        wall = time.perf_counter() - t0

    return ConditionResult(
        condition="B1", workload=workload.name, freeze=freeze(workload.decisions),
        final_state=_final_state(h, workload.initial_state),
        results=results, wall_time=wall, turn_replay_counts=replay,
        commit_latencies=inst.commit_latencies, commit_results=inst.commit_results,
        lock_wait_s=inst.lock_wait_s,
    )


# ---------------------------------------------------------------- B3 共享层
def run_b3(workload: Workload, *, per_action_delay: float = 0.0,
           run_id: str | None = None) -> ConditionResult:
    h = _make_harness(per_action_delay)
    _seed(h, workload.initial_state)
    plan = _build_plan(h, workload, run_id or f"{workload.name}-B3")
    acbs = _admit_all(h, plan)
    h.scheduler = _NoopScheduler()
    _pre_stage(h, acbs, workload.decisions)

    barrier = threading.Barrier(len(acbs))

    def worker(acb, d):
        barrier.wait()
        return h.run_task(acb, [_commit_actions(d)])

    with _Instrument(h) as inst:
        t0 = time.perf_counter()
        with ThreadPoolExecutor(max_workers=len(acbs)) as ex:
            futs = [ex.submit(worker, acb, d)
                    for acb, d in zip(acbs, workload.decisions)]
            results = [f.result() for f in futs]
        wall = time.perf_counter() - t0

    return ConditionResult(
        condition="B3", workload=workload.name, freeze=freeze(workload.decisions),
        final_state=_final_state(h, workload.initial_state),
        results=results, wall_time=wall,
        turn_replay_counts=[1 + acb.rebase_attempts for acb in acbs],
        commit_latencies=inst.commit_latencies, commit_results=inst.commit_results,
        lock_wait_s=inst.lock_wait_s,
    )


# ---------------------------------------------------------------- B2 worktree
def _dumps(content, fmt: str) -> str:
    if fmt == "compact":
        return json.dumps(content, sort_keys=True, separators=(",", ":"),
                          ensure_ascii=True)
    return json.dumps(content, sort_keys=True, indent=2, ensure_ascii=True)


def _loads(text: str):
    try:
        return json.loads(text)
    except (ValueError, TypeError):
        return None


def _write(path: str, text: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


def _wname(workload: Workload, resource: str) -> str:
    return workload.schema.get(resource, _spec(resource)).worktree_path


def _git_merge_file(base: str, ours: str, theirs: str, git: str):
    with tempfile.TemporaryDirectory() as d:
        bp, op, tp = (os.path.join(d, "base"), os.path.join(d, "ours"),
                      os.path.join(d, "theirs"))
        _write(bp, base); _write(op, ours); _write(tp, theirs)
        t0 = time.perf_counter()
        proc = subprocess.run([git, "merge-file", "-p", op, bp, tp],
                              capture_output=True, text=True)
        dt = time.perf_counter() - t0
    return proc.stdout, proc.returncode != 0, dt


def _line_blocks(base_lines, other_lines):
    """返回相对 base 的变更块：`(i1, i2, 替换行)`；插入块满足 `i1 == i2`。"""
    sm = difflib.SequenceMatcher(None, base_lines, other_lines, autojunk=False)
    return [(i1, i2, other_lines[j1:j2])
            for tag, i1, i2, j1, j2 in sm.get_opcodes() if tag != "equal"]


def _blocks_conflict(a, b) -> bool:
    """两个变更块是否冲突（与 `git merge-file` 的冲突判定对齐，经差分实测）。

    - 同为插入且插入点相同：内容不同 → 冲突（内容相同 → 可去重合并）；
    - 插入 vs 替换：插入点落在替换闭区间 `[j1, j2]` 内 → 冲突
      （git 把「紧贴替换块边界」的插入也视为冲突）；
    - 替换 vs 替换：区间**相交或相邻**（`max(i1,j1) <= min(i2,j2)`）→ 冲突，
      除非区间完全相同且替换内容一致。
    """
    i1, i2, ra = a
    j1, j2, rb = b
    a_empty = i1 == i2
    b_empty = j1 == j2
    if a_empty and b_empty:
        return i1 == j1 and ra != rb
    if a_empty:
        return j1 <= i1 <= j2
    if b_empty:
        return i1 <= j1 <= i2
    if max(i1, j1) <= min(i2, j2):
        return not (i1 == j1 and i2 == j2 and ra == rb)
    return False


def _py_line_merge(base: str, ours: str, theirs: str):
    """纯 Python 行级 3-way 合并（git 不可用时的降级路径）。

    显式处理零长插入块 `(i1, i1)`：旧实现只按 `bounds` 段集合取值，插入块
    不在任何 `[s, e)` 段里而被静默丢弃（`base="A\\nB\\n"`、双方各在 A/B
    之间插入一行 → 两行双双消失且不报冲突）。现在插入块参与冲突判定与输出。
    """
    base_lines = base.splitlines(keepends=True)
    ours_lines = ours.splitlines(keepends=True)
    theirs_lines = theirs.splitlines(keepends=True)

    o_blocks = _line_blocks(base_lines, ours_lines)
    t_blocks = _line_blocks(base_lines, theirs_lines)

    for a in o_blocks:
        for b in t_blocks:
            if _blocks_conflict(a, b):
                return None, True
    if not o_blocks and not t_blocks:
        return base, False

    inserts: dict[int, list] = {}
    repl: dict[int, tuple[int, list]] = {}
    for blocks_ in (o_blocks, t_blocks):
        for i1, i2, lines in blocks_:
            if i1 == i2:
                existing = inserts.get(i1)
                if existing is not None and existing == lines:
                    continue                       # 同点同内容 → 去重
                inserts.setdefault(i1, []).extend(lines)
                continue
            prev = repl.get(i1)
            if prev is not None and prev[0] == i2 and prev[1] == lines:
                continue                           # 两侧同一替换 → 取一份
            repl[i1] = (i2, lines)

    out: list = []
    n = len(base_lines)
    p = 0
    while p <= n:
        if p in inserts:
            out.extend(inserts[p])
        if p == n:
            break
        if p in repl:
            end, lines = repl[p]
            out.extend(lines)
            p = end
            continue
        out.append(base_lines[p])
        p += 1
    return "".join(out), False


def run_b2(workload: Workload, *, per_action_delay: float = 0.0) -> ConditionResult:
    git = shutil.which("git")
    degraded = git is None
    tmp = tempfile.mkdtemp(prefix="bench-b2-")
    fmt = workload.file_format
    t_start = time.perf_counter()
    try:
        initial_text = {r: _dumps(c, fmt)
                        for r, c in workload.initial_state.items()}
        # 每 agent 一个 worktree：初态 + 该 agent 的 stage 覆盖
        agent_texts = []
        for i, d in enumerate(workload.decisions):
            ad = os.path.join(tmp, f"agent{i}")
            os.makedirs(ad, exist_ok=True)
            contents = dict(initial_text)
            if per_action_delay:
                time.sleep(per_action_delay)
            for a in d.actions:
                if a.kind == "stage":
                    contents[a.resource] = _dumps(a.content, fmt)
                    if per_action_delay:
                        time.sleep(per_action_delay)
            for r, t in contents.items():
                _write(os.path.join(ad, _wname(workload, r)), t)
            agent_texts.append(contents)

        final: dict = {}
        merge_conflicts = 0
        sub_s = 0.0
        sub_n = 0
        for r in workload.initial_state:
            writers = [i for i in range(len(workload.decisions))
                       if any(a.kind == "stage" and a.resource == r
                              for a in workload.decisions[i].actions)]
            if len(writers) <= 1:
                if not writers:
                    final[r] = copy.deepcopy(workload.initial_state[r])
                else:
                    final[r] = _loads(agent_texts[writers[0]][r])
                continue
            acc = agent_texts[writers[0]][r]
            for wi in writers[1:]:
                theirs = agent_texts[wi][r]
                if git:
                    merged, conflicted, dt = _git_merge_file(
                        initial_text[r], acc, theirs, git)
                    sub_s += dt
                    sub_n += 1
                else:
                    merged, conflicted = _py_line_merge(initial_text[r], acc, theirs)
                if conflicted:
                    merge_conflicts += 1
                    acc = theirs          # REPLACE（后写覆盖）
                else:
                    acc = merged
            final[r] = _loads(acc)       # 合并前解析回 JSON
        return ConditionResult(
            condition="B2", workload=workload.name, freeze=freeze(workload.decisions),
            final_state=final, results=[Status.OK] * len(workload.decisions),
            wall_time=time.perf_counter() - t_start,
            turn_replay_counts=[1] * len(workload.decisions),
            merge_conflicts=merge_conflicts, degraded=degraded,
            subprocess_s=sub_s, subprocess_calls=sub_n,
        )
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
