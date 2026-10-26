"""SubscriptionRegistry —— 文档 §5.6（读即订阅）。"""

from __future__ import annotations

from typing import Callable

from .event_log import EventLog
from .types import Event


class SubscriptionRegistry:
    """读即订阅 + 背压（文档 §5.6）。

    背压语义：per-subscriber 在途事件数超过 `max_inflight` 时，
    下游应**降级为快照重放**（不丢弃事件）。
    """

    def __init__(self, event_log: EventLog, max_inflight: int = 100) -> None:
        self._event_log = event_log
        self._subs: dict[str, set[str]] = {}
        self._inflight: dict[str, int] = {}
        self._max_inflight = max_inflight
        self._handlers: list[Callable[[str, Event], None]] = []

    def add(self, agent_id: str, resource: str) -> None:
        self._subs.setdefault(resource, set()).add(agent_id)

    def subscribers(self, resource: str) -> set[str]:
        return set(self._subs.get(resource, set()))

    def on_change(self, fn: Callable[[str, Event], None]) -> None:
        self._handlers.append(fn)

    def notify(self, resource: str, event: Event) -> None:
        for agent_id in self.subscribers(resource):
            self._inflight[agent_id] = self._inflight.get(agent_id, 0) + 1
        for fn in list(self._handlers):
            fn(resource, event)

    def ack(self, agent_id: str) -> None:
        self._inflight[agent_id] = max(0, self._inflight.get(agent_id, 0) - 1)

    def inflight(self, agent_id: str) -> int:
        return self._inflight.get(agent_id, 0)

    def is_backpressured(self, agent_id: str) -> bool:
        return self._inflight.get(agent_id, 0) >= self._max_inflight

    @property
    def max_inflight(self) -> int:
        return self._max_inflight
