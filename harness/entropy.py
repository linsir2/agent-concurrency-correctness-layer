"""S24-10：确定性原语（seeded uuid / now），让单线程场景可逐字节重放。

**为什么**：审计与故障复盘的信任基础是「同一份输入 + 同一个 seed → 同一份
事件日志」。当前 `event_id`/`run_id`/预算预留 id 用 `uuid4`、时间戳用墙钟，
同一场景跑两遍必然不同，无法证明「这次修好了」。

**规则**：`Deterministic(seed)` 按**域**派生 id，并给出严格单调的模拟时钟。
- 域隔离：`blake2b(seed | domain | counter)` → 不同域即使 counter 相同也不碰撞；
- 同一 seed + **同一调用序列** → 逐字节相同；
- 时间只随 `now_ms()` 前进（默认 +1ms），调用方不得另取墙钟。

**安全边界（必须遵守）**：
- 确定性 id/token **不是**安全随机；不得用作能力令牌/密钥（`Signer` 仍用真实
  `uuid4` + 墙钟）。确定性模式与 `auth_secret` 同时启用 → **构造即拒绝**。
- 只承诺**单线程、固定调度**下的重放；`scheduler_mode="pool"` / merge queue /
  真实网络 / 真实 LLM 不在承诺内（构造时拒绝前两者）。

默认（`entropy=None`）所有既有路径仍走 `uuid4` / `SystemClock`，字节不变。
"""

from __future__ import annotations

import hashlib
import threading
from typing import Protocol


class Entropy(Protocol):
    """注入面：三个方法覆盖「随机 id」与「时间」两类不确定性。"""

    def uuid(self, domain: str) -> str:
        ...

    def token(self, domain: str) -> str:
        ...

    def now_ms(self) -> int:
        ...


class Deterministic:
    """种子化熵源（测试/重放专用，**不是**安全随机）。"""

    __slots__ = (
        "_counters", "_lock", "_owner", "_owner_name", "_seed",
        "_start_ms", "_tick_ms", "_ticks",
    )

    def __init__(self, seed: str | bytes, *, start_ms: int = 1_000_000,
                 tick_ms: int = 1) -> None:
        self._seed = seed.encode("utf-8") if isinstance(seed, str) else bytes(seed)
        if not self._seed:
            raise ValueError("seed 不能为空")
        self._start_ms = int(start_ms)
        self._tick_ms = max(1, int(tick_ms))
        self._lock = threading.RLock()
        self._counters: dict[str, int] = {}
        self._ticks = 0
        self._owner = threading.get_ident()
        self._owner_name = threading.current_thread().name

    def _assert_owner(self) -> None:
        ident = threading.get_ident()
        if ident != self._owner:
            raise ValueError(
                "Deterministic 只能在构造线程使用："
                f"owner={self._owner_name}({self._owner}) "
                f"current={threading.current_thread().name}({ident})；"
                "并发场景请使用 scheduler_mode='sync' 或不要注入 entropy")

    # ---- 内部 -----------------------------------------------------
    def _next(self, domain: str) -> int:
        with self._lock:
            counter = self._counters.get(domain, 0)
            self._counters[domain] = counter + 1
            return counter

    def _digest(self, domain: str, counter: int, size: int = 16) -> str:
        h = hashlib.blake2b(digest_size=size, person=b"rsc-ent-v1")
        h.update(self._seed)
        h.update(b"\x00")
        h.update(str(domain).encode("utf-8"))
        h.update(b"\x00")
        h.update(int(counter).to_bytes(8, "big", signed=False))
        return h.hexdigest()

    # ---- Entropy 协议 ---------------------------------------------
    def uuid(self, domain: str) -> str:
        """16 字节（32 hex）的确定性 id；同域递增、跨域隔离。"""
        self._assert_owner()
        return self._digest(domain, self._next(domain), 16)

    def token(self, domain: str) -> str:
        """32 字节（64 hex）的确定性 token（**非安全**，勿作能力令牌）。"""
        self._assert_owner()
        return self._digest(domain, self._next(domain), 32)

    def now_ms(self) -> int:
        """严格单调的模拟时钟：每次调用 +`tick_ms`。"""
        self._assert_owner()
        with self._lock:
            self._ticks += 1
            return self._start_ms + self._ticks * self._tick_ms

    # ---- 观测 -----------------------------------------------------
    def calls(self, domain: str | None = None) -> int:
        with self._lock:
            if domain is None:
                return sum(self._counters.values())
            return int(self._counters.get(domain, 0))

    def __repr__(self) -> str:              # 不回显 seed（避免进日志）
        return (f"Deterministic(seed=<{len(self._seed)}B>, "
                f"start_ms={self._start_ms}, tick_ms={self._tick_ms})")


def entropy_of(obj) -> Entropy | None:
    """从 Harness/EventLog 等对象上取注入的熵源（没有 → None）。"""
    return getattr(obj, "entropy", None) or getattr(obj, "_entropy", None)
