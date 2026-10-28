"""ContextAssembler —— 文档 §5.5（物化视图 + cursor + HITL resume 值）。

- 从 EventLog 按订阅过滤重放（**非直读 Store**）
- `retention`：保留窗口（S9 §2.2 / K-26）—— 只有在 `acb.materialized` **非空**
  时才把 cursor 抬到 `floor`；无快照**绝不跳**（宁可全量重放，不丢上下文）
- `hitl_result` 作为独立字段进入 ctx（v1.10 修复）
"""

from __future__ import annotations

import copy

from .types import Event


class ContextAssembler:
    def __init__(self, event_log, workspace, retention: int = 10000,
                 floor_for=None, run_scoped: bool = False) -> None:
        if retention < 0:
            raise ValueError(f"retention must be >= 0, got {retention}")
        self._event_log = event_log
        self._workspace = workspace
        self.retention = retention
        # S12 K-33：per-run 保留窗口下界（由 Harness 注入；None → 全局 tail，
        # 向后兼容直接构造 `ContextAssembler` 的旧用例）。
        self._floor_for = floor_for
        # S12 K-33：`run_scoped=True` → 重放按 `run_id` 过滤（同名 subject 不串 run）
        self._run_scoped = bool(run_scoped)
        # S9 §2.2：两个计数器必须在构造时初始化（v1.0 漏了 → AttributeError）
        self.snapshots_forced = 0      # 总：有快照 → 强制抬 cursor 的次数
        self.retention_bypassed = 0    # 总：无快照/水位不足 → 放弃窗口限制（不跳）
        # P2：按 (run_id, logical_subgoal_id) 分桶，并区分「无快照」与「快照水位不足」
        self.snapshots_forced_by_scope: dict[tuple[str, str], int] = {}
        self.retention_bypassed_by_scope: dict[tuple[str, str], int] = {}
        self.no_snapshot_bypassed: dict[tuple[str, str], int] = {}
        self.watermark_short_bypassed: dict[tuple[str, str], int] = {}

    def context_view(self, acb) -> dict:
        """物化视图：从 EventLog 按订阅过滤重放。

        事件按 seq 有序应用；视图值取自「已确认的资源内容」（一致前缀）。

        `retention` 安全语义（S9 §2.2 / P1-7）：
        - `retention == 0` → **不限制**（特判，不是 `floor = tail`）；
        - `retention > 0` → `floor = max(0, tail - retention + 1)`；
          `cursor < floor` 时只有**物化快照非空且其水位 >= floor** 才允许跳
          （快照已含前缀），否则计入 `retention_bypassed` 并**保持完整重放**。
          「快照非空」不等于「覆盖到 floor」——快照只是 cursor 处的视图，
          不含 `[cursor, floor)` 的新事件，直接跳会丢订阅状态。
        """
        tail = self._event_log.tail()
        if self.retention > 0:
            if self._floor_for is not None:
                floor = self._floor_for(acb.run_id)      # K-33：per-run tail
            else:
                floor = max(0, tail - self.retention + 1)
            if acb.cursor < floor:
                watermark = int(getattr(acb, "materialized_watermark", 0) or 0)
                scope = self._scope_of(acb)
                if acb.materialized and watermark >= floor:   # 快照水位覆盖到 floor
                    acb.cursor = floor
                    self.snapshots_forced += 1
                    self._bump(self.snapshots_forced_by_scope, scope)
                else:                          # 快照缺失/水位不足 → 绝不跳（否则丢状态）
                    self.retention_bypassed += 1
                    self._bump(self.retention_bypassed_by_scope, scope)
                    # P2：区分「无快照」与「快照水位不足」
                    bucket = (self.watermark_short_bypassed if acb.materialized
                              else self.no_snapshot_bypassed)
                    self._bump(bucket, scope)
        view = copy.deepcopy(acb.materialized)
        run_id = getattr(acb, "run_id", "") or ""
        if self._run_scoped:
            # K-33：视图不串 run（重放在 EventLog 侧过滤，同名 subject 不串味）
            events = self._event_log.replay(from_seq=acb.cursor, run_id=run_id)
        else:
            events = self._event_log.replay(from_seq=acb.cursor)
        for ev in events:
            if ev.subject in acb.subscriptions:
                if isinstance(ev.payload, dict) and "content" in ev.payload:
                    view[ev.subject] = ev.payload["content"]
                else:
                    view[ev.subject] = self._workspace.get(ev.subject)
        acb.cursor = max(acb.cursor, self._event_log.tail())
        view["_private"] = dict(acb.staged)
        if acb.hitl_result is not None:
            view["_hitl"] = acb.hitl_result
        return view

    def replay(self, acb) -> list[Event]:
        if self._run_scoped:
            return list(self._event_log.replay(
                from_seq=acb.cursor, run_id=getattr(acb, "run_id", "") or ""))
        return list(self._event_log.replay(from_seq=acb.cursor))

    @staticmethod
    def _scope_of(acb) -> tuple[str, str]:
        """指标分桶键：`(run_id, logical_subgoal_id)`（P2）。"""
        return (getattr(acb, "run_id", "") or "",
                getattr(acb, "logical_subgoal_id", "") or "")

    @staticmethod
    def _bump(counter: dict, scope: tuple[str, str]) -> None:
        counter[scope] = counter.get(scope, 0) + 1
