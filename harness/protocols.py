"""组件协议（D2：可替换点）。

这些 `Protocol` 描述**当前阶段**的契约；`S1+` 会在实现落地时逐步收紧
（例如 `EventLog.append` 的 `run_id` 变为必填、`BudgetManager.reserve`
返回 `Reservation`）。收紧必须同时改调用点与测试，不允许静默放宽。

约定：Protocol 方法体一律 `...`，不写实现；实现类**不需要显式继承**。
"""

from __future__ import annotations

from typing import Any, Callable, Iterable, Optional, Protocol

from .budget import Reservation
from .resolver import ConflictResolver                        # noqa: F401  (re-export)
from .types import ACB, Event, EventKind, Lease, Mode, Result, Write

__all__ = [
    "Clock", "EventLog", "IntentRegistry", "BudgetManager", "Workspace",
    "CheckpointStore", "EffectJournal", "PolicyEngine", "Decider",
    "ConflictResolver",
]


class Clock(Protocol):
    def now_ms(self) -> int:
        ...


class EventLog(Protocol):
    def append(self, kind, subject, payload=None, producer: str = "",
               caused_by: str = "", idempotency_key: str = "",
               *, run_id: str = "") -> Event:
        ...

    def append_batch(self, drafts: list[dict]) -> list[Event]:
        ...

    def append_in_txn(self, kind, subject, payload=None, producer: str = "",
                      caused_by: str = "", idempotency_key: str = "",
                      *, run_id: str = "") -> Event:
        ...

    def replay(self, subject: Optional[str] = None, from_seq: int = 0) -> Iterable[Event]:
        ...

    def has(self, subject: str, kind: EventKind) -> bool:
        ...

    def tail(self) -> int:
        ...

    def subscribe(self, fn: Callable[[Event], None]) -> None:
        ...


class IntentRegistry(Protocol):
    def reserve(self, resource: str, agent_id: str, base_version: int,
                ttl_ms: int = 5000, mode: str = "WRITE") -> Lease:
        ...

    def is_valid(self, lease: Lease) -> bool:
        ...

    def revoke(self, lease: Lease) -> None:
        ...

    def revoke_all(self, agent_id: str) -> None:
        ...

    def renew(self, lease: Lease) -> bool:
        ...

    def compatible(self, dep_id: str, agent_id: str, kind: str = "CONTRACT") -> bool:
        ...


class BudgetManager(Protocol):
    def create_node(self, node_id: str, limits: int, parent_id: str = "run") -> Any:
        ...

    def reserve(self, node_id: str, amount: int) -> Optional[Reservation]:
        ...

    def consume(self, reservation_id: str, amount: int = 1, *,
                acb: Optional[ACB] = None) -> Result:
        ...

    def release(self, reservation_id: str) -> int:
        ...

    def settle(self, reservation_id: str) -> int:
        ...

    def check(self, node_id: str, amount: int = 0) -> bool:
        ...


class Workspace(Protocol):
    def put(self, resource: str, content) -> Any:
        ...

    def stage(self, acb: ACB, resource: str, content) -> None:
        ...

    def prepare(self, acb: ACB, w: Write, lease: Lease, mode: Mode) -> Result:
        ...

    def apply(self, acb: ACB, w: Write, lease: Lease, mode: Mode) -> Result:
        ...

    def get(self, name: str):
        ...

    def version(self, name: str) -> int:
        ...

    def content_hash(self, name: str) -> str:
        ...

    def lock_for(self, resource: str):
        ...

    def compute_reverse_patch(self, w: Write):
        ...


class CheckpointStore(Protocol):
    def save(self, acb: ACB) -> str:
        ...

    def load(self, ref: str) -> Any:
        ...


class EffectJournal(Protocol):
    def begin(self, acb: ACB, tool: str, args, idem_key: str) -> None:
        ...

    def confirm(self, idem_key: str, agent_id: str) -> None:
        ...

    def mark_unknown(self, idem_key: str, agent_id: str) -> None:
        ...

    def state(self, idem_key: str) -> Optional[str]:
        ...


class PolicyEngine(Protocol):
    def issue(self, arg, *, tools=None, paths=None, network=None,
              data_class: str = "*", allow_legacy: bool = False) -> Any:
        ...

    def check_capability(self, agent_id: str, tool: str, path: str = "") -> bool:
        ...


class Decider(Protocol):
    """阶段一由 scripted fake 实现；阶段二接真实模型。"""

    def __call__(self, acb: ACB, view: dict) -> Any:
        ...
