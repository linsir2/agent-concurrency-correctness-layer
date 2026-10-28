"""组件协议（D2：可替换点）。

这些 `Protocol` 描述**当前阶段**的契约；`S1+` 会在实现落地时逐步收紧
（例如 `EventLog.append` 的 `run_id` 变为必填、`BudgetManager.reserve`
返回 `Reservation`）。收紧必须同时改调用点与测试，不允许静默放宽。

约定：Protocol 方法体一律 `...`，不写实现；实现类**不需要显式继承**。

**S15 C1/C2（本文件 v2）**：
    原契约长期无人校验，已与实现严重漂移（`CheckpointStore` 只声明 2 个方法，
    而 Harness 实际调用 11 个）。现在以「**Harness 实际访问的成员**」为准补全，
    并由 `tests/test_s15_contract_drift.py` 用 `ast` 静态扫描**反向锁定**：
    任何人新增 `self.<组件>.<成员>`（含 `self.h.<组件>.<成员>`）而没在契约里
    声明，测试立刻红。

    标 `_` 前缀的成员是**内部接缝**（恢复期账本重建 / 不变量自检 / 策略缓存），
    它们同样是「替换后端必须提供」的一部分，因此**照样声明**——不设白名单，
    否则白名单会变成漂移的藏身处。

    所有契约标 `@runtime_checkable`：只能查「方法名/属性名存在」，**不查签名**
    （CPython 已知限制），签名与语义一致性由 C3 后端一致性套件兜底。
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any, Protocol, runtime_checkable

from .budget import Reservation
from .resolver import ConflictResolver
from .types import ACB, Event, EventKind, Lease, Mode, Result, Write

__all__ = [
    "BudgetManager",
    "CheckpointStore",
    "Clock",
    "ConflictResolver",
    "Decider",
    "EffectJournal",
    "EventLog",
    "IntentRegistry",
    "PolicyEngine",
    "Workspace",
]


@runtime_checkable
class Clock(Protocol):
    def now_ms(self) -> int:
        ...


@runtime_checkable
class EventLog(Protocol):
    def append(self, kind, subject, payload=None, producer: str = "",
               run_id: str = "") -> Event:
        ...

    def append_batch(self, drafts: list[dict]) -> list[Event]:
        ...

    def append_in_txn(self, kind, subject, payload=None, producer: str = "",
                      run_id: str = "") -> Event:
        ...

    def replay(self, subject: str | None = None, from_seq: int = 0) -> Iterable[Event]:
        ...

    def has(self, subject: str, kind: EventKind) -> bool:
        ...

    def tail(self) -> int:
        ...

    def subscribe(self, fn: Callable[[Event], None]) -> None:
        ...

    # ---- S15 C1：Harness 实际调用、旧契约漏声明 ----
    def all(self) -> list[Event]:
        """全量事件（重放/收据/不变量自检的读取面）。"""
        ...

    def notify_events(self, events: list[Event]) -> None:
        """批量提交后统一通知订阅者（`append_batch(notify=False)` 的配对调用）。"""
        ...

    # 注：`records()` 是 `WalEventLog` **专有**（内存实现没有），故**不入**契约；
    # 契约只描述「两套后端都提供」的公共面（由 C3 一致性套件验证）。


@runtime_checkable
class IntentRegistry(Protocol):
    def reserve(self, resource: str, agent_id: str, base_version: int,
                lease_ms: int = ..., run_id: str = ...) -> Lease | None:
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

    # ---- S15 C1：epoch 接缝（fencing 必需）----
    def get_epoch(self) -> int:
        ...

    def set_epoch(self, epoch: int) -> None:
        ...

    def bump_epoch(self, run_id: str | None = None) -> int:
        ...

    # ---- S15 C1：逻辑依赖声明 + 活租约视图 ----
    _active: dict
    """`resource → [Lease]` 活租约表（`runtime` 守卫的读取面）。"""

    def declare_logical(self, dep_id: str, agent_id: str, kind: str) -> None:
        ...

    def undeclare_logical(self, dep_id: str, agent_id: str) -> None:
        ...

    def renew_for(self, acb: ACB) -> bool:
        """按 ACB 的 `resource_ids` 批量续租。"""
        ...


@runtime_checkable
class BudgetManager(Protocol):
    # ---- S15 C1：属性型接缝（恢复/不变量/自检）----
    root: Any
    """根节点（`subtree_reserved` / `remaining` 等聚合读）。"""
    _nodes: dict
    """节点表（恢复期按 `budget_node_id` 重建 ACB 引用）。"""
    _reservations: dict
    """预留表（恢复期逐笔重建 `(node, Reservation)`）。"""

    def create_node(self, node_id: str, limits: int, parent_id: str = "run") -> Any:
        ...

    def reserve(self, node_id: str, amount: int, *,
                reservation_id: Optional[str] = None) -> Reservation | None:
        ...

    def consume(self, reservation_id: str, amount: int = 1, *,
                acb: Any = None) -> Any:
        ...

    def release(self, reservation_id: str) -> int:
        ...

    def settle(self, reservation_id: str) -> int:
        ...

    def check(self, node_id: str, amount: int = 0) -> bool:
        """**ANY 语义**：该节点**任一** ACTIVE 预留的余额是否够付 `amount`。

        注意：这不等于「剩余额度」；要问剩余额度请用 `remaining()`（S19 K-91）。
        """
        ...

    def remaining(self, node_id: str) -> int:
        """剩余可用额度 = `limits - subtree_reserved`。"""
        ...

    def prune(self, now: float, live_rids=(), *,
              max_age_s: float | None = None) -> int:
        """三栅栏剪枝（状态/引用/时间）；剪前折账（S19 K-88）。"""
        ...

    # ---- S15 C1：事件溯源 / 归档 / 不变量 ----
    def apply_budget_event(self, ev: Event) -> None:
        ...

    def budget_events(self, run_id: str | None = None) -> list[Event]:
        ...

    def archive_node(self, node_id: str, *, now: float | None = None) -> bool:
        ...

    def remove_node(self, node_id: str) -> bool:
        ...

    def set_epoch(self, epoch: int, *, emit: bool = True) -> None:
        ...

    def bump_epoch(self, run_id: str | None = None) -> int:
        ...

    def check_invariants(self) -> None:
        ...

    def _subtree_nodes(self, node: Any) -> list[Any]:
        """子树枚举（归档/取消/树完整性都在用）。"""
        ...

    def check_reservations(self, reservation_ids, amount: int = 1) -> bool:
        """批量检查多笔预留是否都还有余量（`commit_batch` 前置）。"""
        ...

    def gc(self, now_s: float) -> int:
        ...

    # ---- S15 C1：事件草案抑制（两阶段落盘用）----
    def suppressed(self) -> Any:
        """上下文管理器：期间的预算事件只进草稿，由 `flush_pending` 统一提交。"""
        ...

    def discard_pending(self) -> None:
        ...

    def flush_pending(self) -> list[Event]:
        ...


@runtime_checkable
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

    # ---- S15 C1：提交协议内部接缝（`commit_batch` 在同一临界区内使用）----
    def apply_event(self, ev: Event) -> None:
        ...

    def _effective_version(self, resource: str) -> int:
        ...

    def _base_of(self, acb: ACB, resource: str) -> int:
        ...

    def _current_blob_hash(self, resource: str) -> str | None:
        ...

    def _acquire_all(self, resources, timeout: float = ...):
        ...

    def _validate_locked(self, acb: ACB, w: Write, lease: Lease, mode: Mode) -> Result:
        ...

    def _apply_locked(self, acb: ACB, w: Write, lease: Lease, mode: Mode) -> Any:
        ...

    def _release_all(self, held) -> None:
        ...

    def pre_image(self, resource: str, version: int):
        """取指定版本的内容（逆补丁/补偿用）。"""
        ...


@runtime_checkable
class CheckpointStore(Protocol):
    def save(self, acb: ACB) -> str:
        ...

    def load(self, ref: str) -> Any:
        ...

    # ---- S15 C1：Harness 实际调用、旧契约漏声明（MS9 拦路石）----
    def load_strict(self, ref: str) -> dict:
        """校验校验和后返回 payload（损坏 → 抛，静默降级走 `load`）。"""
        ...

    def latest_ref_for_acb(self, acb_id: str) -> str | None:
        ...

    def refs_for_run(self, run_id: str) -> list[tuple[str, str | None]]:
        ...

    def refs_all(self) -> list[tuple[str, str | None]]:
        ...

    def update_children(self, acb_id: str, mutate, *,
                        expect_ref: str | None = None,
                        retries: int = 3) -> str | None:
        ...

    def read_epoch(self) -> int:
        ...

    def bump_epoch(self) -> int:
        ...


@runtime_checkable
class EffectJournal(Protocol):
    def begin(self, acb: ACB, tool: str, args, idem_key: str) -> None:
        ...

    def confirm(self, idem_key: str, agent_id: str) -> None:
        ...

    def mark_unknown(self, idem_key: str, agent_id: str) -> None:
        ...

    def state(self, idem_key: str) -> str | None:
        ...

    # ---- S15 C1：UNKNOWN 对账（崩溃恢复必需）----
    pending: dict
    """`idem_key → 状态` 的内存视图（对账扫描的读取面）。"""

    def probe_one(self, idem_key: str) -> Any:
        ...

    def rebuild_from(self, events: Iterable[Event]) -> None:
        ...

    def get(self, idem_key: str) -> Any:
        """按幂等键取账本条目（`runtime` 判定 UNKNOWN 用）。"""
        ...


@runtime_checkable
class PolicyEngine(Protocol):
    # ---- S15 C1：token 面 ----
    default_token: Any
    _tokens: dict
    """`agent_id → CapabilityToken` 缓存（恢复期按需注册父链）。"""

    def issue(self, arg, *, tools=None, paths=None, network=None,
              data_class: str = "*", allow_legacy: bool = False) -> Any:
        ...

    def check_capability(self, agent_id: str, tool: str, path: str = "") -> bool:
        ...

    def token(self, agent_id: str) -> Any:
        ...

    def resolve_token(self, token: Any) -> Any:
        ...

    def _resolve_raw(self, token: Any) -> Any:
        ...

    def allows_network(self, token: Any, host: str, port: int | None = None) -> bool:
        ...

    def allows_data_class(self, token: Any, data_class: str) -> bool:
        ...


@runtime_checkable
class Decider(Protocol):
    """阶段一由 scripted fake 实现；阶段二接真实模型。"""

    def __call__(self, acb: ACB, view: dict) -> Any:
        ...
