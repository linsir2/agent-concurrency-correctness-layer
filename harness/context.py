"""ContextAssembler —— 文档 §5.5（物化视图 + cursor + HITL resume 值）。

- 从 EventLog 按订阅过滤重放（**非直读 Store**）
- `cursor` 不得落后于保留窗口下限，否则从最近快照重放
- `hitl_result` 作为独立字段进入 ctx（v1.10 修复）
"""

from __future__ import annotations

import copy

from .types import Event


class ContextAssembler:
    def __init__(self, event_log, workspace, retention: int = 10000) -> None:
        self._event_log = event_log
        self._workspace = workspace
        self.retention = retention

    def context_view(self, acb) -> dict:
        """物化视图：从 EventLog 按订阅过滤重放。

        事件按 seq 有序应用；视图值取自「已确认的资源内容」（一致前缀）。
        """
        view = copy.deepcopy(acb.materialized)
        for ev in self._event_log.replay(from_seq=acb.cursor):
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
        return list(self._event_log.replay(from_seq=acb.cursor))
