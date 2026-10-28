"""S24-4：进程内 merge queue（单写者，FIFO）—— 把「验证 + 提交」绑成一次出队。

**为什么需要**：`apply` 里 verify 与 commit 之间有一个 TOCTOU 窗口 —— 两个 agent
各自在「提交前视图」验证通过，然后先后提交；后者的验证没看见前者的提交，
于是「验证通过但合入后坏了」。单写者队列把 **出队 → 重验（最新账本视图）→ 提交**
串成一步，队列里永远只有队首在动。

**v1 边界（有意）**：
- 单进程、单写者、FIFO；不做 batch / speculation / 持久化。
- 队列只做**串行化**，不额外占预算；token 仍在 `begin` 时占槽位/预留。
- 排队超时不撤销「已开始执行」的票（命令不可中断）；返回 `Wait("QUEUE_TIMEOUT")`，
  若之后仍提交成功，宿主用**同一幂等键**重试会命中 `dedup`。

用法（由 `RscSession` 构造，宿主不直接 new）：

```python
session = RscSession(..., merge_queue={"capacity": 64, "max_wait_ms": 30_000})
r = session.apply_queued(token, writes, idempotency_key="k")
```
"""

from __future__ import annotations

import threading
import time
import uuid
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

QUEUE_FULL = "QUEUE_FULL"
QUEUE_TIMEOUT = "QUEUE_TIMEOUT"
QUEUE_CANCELLED = "QUEUE_CANCELLED"


@dataclass
class Ticket:
    """一张待提交票；`event` 由出队线程置位，`result` 是门面返回值。"""

    ticket_id: str
    token: str
    writes: list
    idempotency_key: str
    cost: int
    agent_id: str
    enqueued_ms: float
    state: str = "QUEUED"            # QUEUED → RUNNING → DONE / CANCELLED
    result: Any = None
    event: threading.Event = field(default_factory=threading.Event)


class MergeQueue:
    """单写者 FIFO 队列；`runner(ticket)` 由门面提供（持 token 锁执行）。"""

    def __init__(self, runner: Callable[[Ticket], Any], *,
                 capacity: int = 64, max_wait_ms: int = 30_000) -> None:
        self._runner = runner
        self._capacity = max(1, int(capacity))
        self._max_wait_ms = max(1, int(max_wait_ms))
        self._cv = threading.Condition(threading.Lock())
        self._pending: deque[Ticket] = deque()
        self._active: Ticket | None = None
        self._closed = False
        self._tickets: dict[str, Ticket] = {}
        self._worker = threading.Thread(target=self._loop, daemon=True,
                                        name="rsc-merge-queue")
        self._worker.start()

    # ------------------------------------------------------------ 对外
    def enqueue(self, token: str, writes: list, *, idempotency_key: str,
                cost: int, agent_id: str = "") -> Ticket | None:
        """入队；满则返回 None（调用方映射成 `Wait(QUEUE_FULL)`）。"""
        with self._cv:
            if self._closed or self._inflight() >= self._capacity:
                return None
            ticket = Ticket(
                ticket_id=uuid.uuid4().hex[:12], token=str(token),
                writes=list(writes), idempotency_key=str(idempotency_key),
                cost=int(cost), agent_id=str(agent_id),
                enqueued_ms=time.monotonic() * 1000.0)
            self._pending.append(ticket)
            self._tickets[ticket.ticket_id] = ticket
            self._cv.notify()
            return ticket

    def wait(self, ticket: Ticket, timeout_ms: int | None = None) -> Any:
        """等待结果；超时返回 `Wait(QUEUE_TIMEOUT)`（见模块头边界说明）。"""
        timeout = (self._max_wait_ms if timeout_ms is None
                   else max(1, int(timeout_ms))) / 1000.0
        if ticket.event.wait(timeout):
            return ticket.result
        with self._cv:
            if ticket.state == "QUEUED":
                self._remove_locked(ticket)
                ticket.state = "CANCELLED"
        from .facade import Wait
        return Wait(QUEUE_TIMEOUT)

    def cancel(self, ticket_id: str, reason: str = "") -> bool:
        """取消**尚未开始**的票；已开始执行则不可取消（返回 False）。"""
        from .facade import Wait
        with self._cv:
            ticket = self._tickets.get(str(ticket_id))
            if ticket is None or ticket.state != "QUEUED":
                return False
            self._remove_locked(ticket)
            ticket.state = "CANCELLED"
            ticket.result = Wait(reason or QUEUE_CANCELLED)
            ticket.event.set()
            return True

    def status(self, ticket_id: str) -> dict | None:
        ticket = self._tickets.get(str(ticket_id))
        if ticket is None:
            return None
        return {"ticket_id": ticket.ticket_id, "state": ticket.state,
                "token": ticket.token, "agent_id": ticket.agent_id,
                "enqueued_ms": ticket.enqueued_ms}

    def depth(self) -> int:
        with self._cv:
            return self._inflight()

    def close(self) -> None:
        """停止 worker（测试/进程退出用）；未开始的票标记取消。"""
        from .facade import Wait
        with self._cv:
            self._closed = True
            for ticket in self._pending:
                ticket.state = "CANCELLED"
                ticket.result = Wait(QUEUE_CANCELLED)
                ticket.event.set()
            self._pending.clear()
            self._cv.notify_all()

    # ------------------------------------------------------------ 内部
    def _inflight(self) -> int:
        return len(self._pending) + (1 if self._active is not None else 0)

    def _remove_locked(self, ticket: Ticket) -> None:
        try:
            self._pending.remove(ticket)
        except ValueError:
            pass

    def _loop(self) -> None:
        while True:
            with self._cv:
                while not self._pending and not self._closed:
                    self._cv.wait()
                if self._closed:
                    return
                ticket = self._pending.popleft()
                ticket.state = "RUNNING"
                self._active = ticket
            try:
                ticket.result = self._runner(ticket)
            except BaseException as exc:                 # noqa: BLE001
                ticket.result = exc
            finally:
                with self._cv:
                    ticket.state = "DONE"
                    self._active = None
                ticket.event.set()
