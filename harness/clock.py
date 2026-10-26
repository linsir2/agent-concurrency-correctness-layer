"""可注入时钟（方案 §3.2 / D7）。

所有涉及时间判定的组件（租约过期、期限、老化、WAL 时间戳）都必须走这里，
禁止直接调用 `time.time()` —— 否则测试只能 sleep，无法精确构造过期竞态。
"""

from __future__ import annotations

import time
from typing import Protocol


class Clock(Protocol):
    def now_ms(self) -> int:
        ...


class SystemClock:
    def now_ms(self) -> int:
        return int(time.time() * 1000)


class ManualClock:
    """测试用：时间只在 `advance` 时前进。"""

    def __init__(self, start_ms: int = 1_000_000) -> None:
        self._now = start_ms

    def now_ms(self) -> int:
        return self._now

    def advance(self, ms: int) -> int:
        if ms < 0:
            raise ValueError("clock 不能倒退")
        self._now += ms
        return self._now
