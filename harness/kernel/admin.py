"""AdminService —— 管理面（暂停/恢复、purge、排空、可观测）从 Harness 迁出（S5-e，纯位移）。

搬出的是 S15 D3/D4 + S17 MS10 的 10 个方法：

- 暂停族：`is_paused` / `pause_run` / `resume_run` / `paused_runs` /
  `rebuild_pause_state`（旧 `_rebuild_pause_state`，从事件流重建**持久真相**）；
- 内存回收：`purge_run`（只清内存，**不删历史事件**）；
- 可观测：`metrics`（零依赖快照）/ `export_observability`（推 SpanSink）；
- 排空与诊断：`drain` / `claim_window_s`（转发到 `DispatchService` 的公开接缝）。

约定（与 `kernel/tasks.py` / `kernel/dispatch.py` 一致）：

- `AdminDeps` 只放**稳定对象引用**（`event_log` / `budget` / `run_coordinator`）
  与 late-lookup callable；可变容器/配置一律在调用时读 `self.state`
  （`paused_runs` / `acbs` / `plans` / `drain_timeout_s` 等）—— 冻结进 deps 会
  读到旧值；
- `drain` / `claim_window_s` 的真实现留在 `DispatchService`，经 **lambda**
  二次转发（不传绑定方法：`tests/test_s13_scheduler` 会实例级打桩
  `_run_one` / `_drain_scheduler`，绑定方法会让打桩失效）；
- Harness 上只保留**公共**委托（`is_paused` / `pause_run` / `resume_run` /
  `paused_runs` / `purge_run` / `metrics` / `drain` / `export_observability` /
  `claim_window_s`），无私有委托 —— `rebuild_pause_state` 由 Harness 在
  `RecoveryDeps` 里以 lambda 直连本服务，Harness 不再持有私有同名方法；
- 本模块构造**不产生任何事件**（`__init__` 只保存引用）；
- 事件种类/payload/顺序、锁序、构造顺序全部保持不变（纯位移）。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from ..types import TASK_TERMINAL, EventKind, TaskState


@dataclass(frozen=True)
class AdminDeps:
    # ---- 稳定对象引用（构造期注入，不随配置变化）----
    event_log: Any
    budget: Any
    run_coordinator: Any
    # ---- 真实现在 DispatchService：late lookup，绝不传绑定方法 ----
    dispatch_drain: Callable[..., dict]
    dispatch_claim_window_s: Callable[..., int]


class AdminService:
    """暂停/恢复、purge、排空、可观测唯一实现（Harness 仅保留同名公共委托）。"""

    def __init__(self, deps: AdminDeps, state) -> None:
        self.deps = deps
        self.state = state

    # ---------------------------------------------------------- 暂停 / 恢复

    def is_paused(self, run_id: str) -> bool:
        """该 run 是否处于 Admin 暂停（只阻止**未开始**的任务被调度）。"""
        return bool(run_id) and run_id in self.state.paused_runs

    def pause_run(self, run_id: str, *, reason: str = "") -> bool:
        """暂停一个 run：保留槽位/预算/节点（可恢复），只停止新调度。

        与 `cancel_run` 的区别：cancel **释放资源且不可恢复**；pause 不释放。
        在途任务**不打断**（避免半提交），让它跑完。
        返回 `True` = 本次真的从「运行」变为「暂停」；已暂停返回 `False`。
        """
        run_id = str(run_id or "")
        if not run_id or run_id in self.state.paused_runs:
            return False
        self.state.paused_runs.add(run_id)
        self.deps.event_log.append(EventKind.RUN_PAUSED, run_id,
                                   {"reason": str(reason or "")}, run_id=run_id)
        return True

    def resume_run(self, run_id: str) -> bool:
        """恢复一个被暂停的 run（`True` = 本次真的从「暂停」变为「运行」）。"""
        run_id = str(run_id or "")
        if run_id not in self.state.paused_runs:
            return False
        self.state.paused_runs.discard(run_id)
        self.deps.event_log.append(EventKind.RUN_RESUMED, run_id, {}, run_id=run_id)
        return True

    def paused_runs(self) -> list[str]:
        return sorted(self.state.paused_runs)

    def rebuild_pause_state(self) -> None:
        """从事件流重建暂停集合（**持久真相**，重启后仍暂停）。

        按 seq 顺序应用 `RUN_PAUSED` / `RUN_RESUMED`，后者覆盖前者。
        """
        paused: set[str] = set()
        for ev in self.deps.event_log.all():
            if ev.kind is EventKind.RUN_PAUSED:
                paused.add(str(ev.subject))
            elif ev.kind is EventKind.RUN_RESUMED:
                paused.discard(str(ev.subject))
        self.state.paused_runs = paused

    # ---------------------------------------------------------- 内存回收

    def purge_run(self, run_id: str) -> bool:
        """把**已全终态**的 run 移出内存（`acbs`/`plans`）。

        **不删除历史事件**（保留审计与收据链），只写 `RUN_PURGED` 事件。
        存在未终态任务 → 返回 `False`（要求先 `kill_run`/等完成）。
        """
        acbs = [a for a in self.state.acbs.values() if a.run_id == run_id]
        if any(a.state not in TASK_TERMINAL for a in acbs):
            return False
        for acb in acbs:
            self.state.acbs.pop(acb.id, None)
            self.deps.run_coordinator.unregister(acb)
        self.state.plans.pop(run_id, None)
        self.state.paused_runs.discard(run_id)
        self.deps.event_log.append(EventKind.RUN_PURGED, run_id, {}, run_id=run_id)
        return True

    # ---------------------------------------------------------- 可观测

    def metrics(self) -> dict:
        """只读快照：`{"counters": {...}, "gauges": {...}}`。

        从 EventLog 与内存状态派生，**不引入 OTel/Prometheus 依赖**
        （`dependencies = []` 是硬约束）；使用者可自行映射到自己的指标后端。
        """
        acbs = self.state.acbs
        counters: dict[str, int] = {}
        for ev in self.deps.event_log.all():
            name = ev.kind.value if hasattr(ev.kind, "value") else str(ev.kind)
            counters[name] = counters.get(name, 0) + 1
        gauges = {
            "acbs_total": len(acbs),
            "acbs_nonterminal": sum(
                1 for a in acbs.values() if a.state not in TASK_TERMINAL),
            "paused_runs": len(self.state.paused_runs),
            "dead_letter": sum(
                1 for a in acbs.values()
                if a.state is TaskState.DEAD_LETTER),
            "waiting_approval": sum(
                1 for a in acbs.values()
                if a.state is TaskState.WAITING_APPROVAL),
            "budget_root_reserved": int(
                getattr(self.deps.budget.root, "subtree_reserved", 0) or 0),
            "event_log_tail": self.deps.event_log.tail(),
        }
        return {"counters": counters, "gauges": gauges}

    def export_observability(self, sink=None) -> int:
        """把事件流 + metrics 推给 `SpanSink`（默认 `MemorySpanSink`）。

        **零依赖**：不 import OTel；需要真实 OTel 时传入
        `observability.make_otel_sink()`。返回推送的 span 条数。
        """
        from ..observability import MemorySpanSink, export_metrics, export_spans

        target = sink if sink is not None else MemorySpanSink()
        n = export_spans(target, self.deps.event_log.all())
        export_metrics(target, self.metrics())
        return n

    # ---------------------------------------------------------- 排空 / 诊断

    def drain(self, *args, **kwargs):
        """优雅排空（真实现留在 `DispatchService.drain`，此处只转发）。"""
        return self.deps.dispatch_drain(*args, **kwargs)

    def claim_window_s(self, *args, **kwargs):
        """有界墓碑集合的总覆盖窗口（真实现留在 `DispatchService.claim_window_s`）。"""
        return self.deps.dispatch_claim_window_s(*args, **kwargs)
