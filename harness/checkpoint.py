"""CheckpointStore + EffectJournal —— 文档 §4.2 / §4.4 / §5.8。

v1.1（S5 第一波，§3/§4）：
- `EffectJournal` 事件**落盘到同一个 `EventLog`**（单一数据源，§3.1）：
  `begin` 写 `EFFECT_INTENT`、`confirm` 写 `EFFECT_CONFIRMED`、
  `mark_unknown` 写 `EFFECT_UNKNOWN`、`probe_one` 写 `EFFECT_PROBED`（三态，只写一次）；
- 事件 `subject` 统一为 `acb.id`，`idem_key` 入 payload（修 P0-4）；
- `pending(acb_id)` 返回 `INTENT + UNKNOWN`（修 P0-1）；
- `rebuild_from(events)` 从传入快照重建（调用方按 `producer` 过滤，§4.3）；
- `CheckpointStore.save` 增 `reservations` / `budget_path` / `event_cursor` /
  `failure` / `spec` / `deadline` / `no_progress_steps`。
"""

from __future__ import annotations

import copy
import hashlib
import itertools
import threading
import time
from dataclasses import dataclass
from typing import Any

from .event_log import EventLog
from .types import Action, Event, EventKind


@dataclass
class EffectEntry:
    """外部副作用条目（三态必处理，§3.3）。"""

    idem_key: str
    effect_id: str
    tool: str
    args: dict
    state: str = "INTENT"        # INTENT | CONFIRMED | UNKNOWN
    probe: str = ""              # "" | "True" | "False" | "None"
    updated_at: float = 0.0


def effect_idem_key(acb, action) -> str:
    """稳定幂等键：`sha256(run_id|logical_subgoal_id|tool|args)[:16]`（不含 task_id/generation）。"""
    payload = f"{acb.run_id}|{acb.logical_subgoal_id}|{action.tool}|{action.args}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


class CheckpointStore:
    def __init__(self) -> None:
        self._store: dict[str, dict] = {}
        self._counter = itertools.count(1)
        self.slots = None            # 由 Harness 注入，用于快照活跃槽位
        self.event_log = None        # 可选注入，用于 `event_cursor`
        self.applied_event_ids = None  # 由 Harness 注入，用于恢复去重持久化

    def save(self, acb) -> str:
        ref = f"CP{next(self._counter)}"
        node = acb.budget_node
        self._store[ref] = {
            "acb_id": acb.id,
            "run_id": acb.run_id,
            "task_id": acb.task_id,
            "logical_subgoal_id": acb.logical_subgoal_id,
            "generation": acb.generation,
            "state": acb.state.value,
            "turn_seq": acb.turn_seq,
            "staged": copy.deepcopy(acb.staged),
            "read_set": dict(acb.read_set),
            "base_version": dict(acb.base_version),
            "cursor": acb.cursor,
            "aging_deadline": acb.aging_deadline,
            "effect_journal_offset": acb.step,
            "hitl_result": copy.deepcopy(acb.hitl_result),
            "rebase_attempts": acb.rebase_attempts,
            "retry_budget": acb.retry_budget,
            "reservation_ids": list(acb.reservation_ids),
            "needs_readmit": bool(getattr(acb, "needs_readmit", False)),
            "slots_active": (sorted(self.slots.active_keys())
                             if self.slots is not None else []),
            "budget_node_id": node.node_id if node is not None else None,
            "budget_parent_id": (node.parent.node_id
                                 if node is not None and node.parent is not None
                                 else None),
            "budget_limits": node.limits if node is not None else None,
            "budget_self": node.self_reserved if node is not None else 0,
            "budget_subtree": node.subtree_reserved if node is not None else 0,
            "budget_consumed": node.consumed if node is not None else 0,
            "budget_folded_consumed": (node.folded_consumed
                                       if node is not None else 0),
            # ---- S5 第一波新增（§5.1/§5.2/§4.3）----
            "reservations": self._reservations_snapshot(node),
            "budget_path": self._budget_path(node),
            "budget_path_limits": self._budget_path_limits(node),
            "event_cursor": self._event_cursor(acb),
            "applied_event_ids": (sorted(self.applied_event_ids)
                                  if self.applied_event_ids is not None else []),
            "failure": acb.failure,
            "spec": copy.deepcopy(acb.spec),
            "deadline": acb.deadline,
            "no_progress_steps": acb.no_progress_steps,
        }
        acb.result_ref = ref
        return ref

    def load(self, ref: str) -> dict:
        return copy.deepcopy(self._store[ref])

    # ---- S5 快照辅助 ------------------------------------------------
    @staticmethod
    def _reservations_snapshot(node) -> list[dict]:
        """逐笔账本（§5.1）：只以逐笔为准，聚合快照仅作交叉校验。"""
        if node is None:
            return []
        return [
            {"rid": rid, "node_id": r.node_id, "amount": r.amount,
             "consumed": r.consumed, "state": r.state}
            for rid, r in node.reservations.items()
        ]

    @staticmethod
    def _budget_path(node) -> list[str]:
        """`[root ... node]`（§5.2）：供 `_restore_path` 自顶向下重建。"""
        if node is None:
            return []
        path: list[str] = []
        cur = node
        while cur is not None:
            path.append(cur.node_id)
            cur = cur.parent
        path.reverse()
        return path

    @staticmethod
    def _budget_path_limits(node) -> dict[str, int]:
        """路径上每个节点的 limits（供任意深度恢复）。"""
        if node is None:
            return {}
        limits: dict[str, int] = {}
        cur = node
        while cur is not None:
            limits[cur.node_id] = cur.limits
            cur = cur.parent
        return limits

    def _event_cursor(self, acb) -> int:
        """已重放到的事件序号（§4.3）。未注入 `event_log` 时退回 `acb.cursor`。"""
        if self.event_log is not None:
            return self.event_log.tail()
        return int(getattr(acb, "cursor", 0) or 0)


class EffectJournal:
    """外部副作用账本。`idem_key` 是幂等键（I7）。

    事件写**同一个** `EventLog`（§3.1 单一数据源）；恢复走 `rebuild_from(events)`。
    """

    def __init__(self, event_log: EventLog, probe=None) -> None:
        self._entries: dict[str, EffectEntry] = {}
        self._acb_of: dict[str, str] = {}
        self._run_of: dict[str, str] = {}
        self._probes: dict[str, str] = {}
        self._event_log = event_log
        self._probe = probe
        self._counter = itertools.count(1)
        # 单写者临界区：begin/confirm/mark_unknown/probe_one 必须原子
        self._lock = threading.RLock()

    # ---- 写入（§3.2：subject = acb.id，idem_key 入 payload）----------
    def begin(self, acb, action, *legacy) -> str:
        """幂等：已存在（含 CONFIRMED/UNKNOWN）则不覆盖（I7）。

        **兼容垫片**：旧签名 `begin(acb, tool, args, key)` 仍可用
        （新签名 `begin(acb, action)` 是生产路径）。
        """
        if isinstance(action, str):
            action = Action(kind="tool", tool=action,
                            args=(legacy[0] if legacy else {}) or {})
        with self._lock:
            idem_key = effect_idem_key(acb, action)
            existing = self._entries.get(idem_key)
            if existing:
                return existing.effect_id
            eid = f"E{next(self._counter)}"
            args = dict(action.args) if action.args is not None else {}
            self._entries[idem_key] = EffectEntry(
                idem_key=idem_key, effect_id=eid, tool=action.tool,
                args=args, state="INTENT")
            self._acb_of[idem_key] = acb.id
            self._run_of[idem_key] = acb.run_id
            self._event_log.append(
                EventKind.EFFECT_INTENT, acb.id,
                {"idem_key": idem_key, "tool": action.tool, "args": action.args,
                 "effect_id": eid},
                producer=acb.id, idempotency_key=idem_key, run_id=acb.run_id)
            return eid

    def confirm(self, idem_key: str, result: Any = None) -> bool:
        with self._lock:
            entry = self._entries.get(idem_key)
            if not entry:
                return False
            entry.state = "CONFIRMED"
            entry.updated_at = time.time()
            acb_id = self._acb_of.get(idem_key, "")
            self._event_log.append(
                EventKind.EFFECT_CONFIRMED, acb_id,
                {"idem_key": idem_key, "effect_id": entry.effect_id,
                 "tool": entry.tool, "result": result},
                producer=acb_id, idempotency_key=idem_key,
                run_id=self._run_of.get(idem_key, ""))
            return True

    def mark_unknown(self, idem_key: str, reason: str = "") -> bool:
        with self._lock:
            entry = self._entries.get(idem_key)
            if not entry:
                return False
            entry.state = "UNKNOWN"
            entry.updated_at = time.time()
            acb_id = self._acb_of.get(idem_key, "")
            self._event_log.append(
                EventKind.EFFECT_UNKNOWN, acb_id,
                {"idem_key": idem_key, "tool": entry.tool, "reason": reason},
                producer=acb_id, idempotency_key=idem_key,
                run_id=self._run_of.get(idem_key, ""))
            return True

    # ---- 读 --------------------------------------------------------
    def pending(self, acb_id: str) -> list[EffectEntry]:
        """返回 `state ∈ {INTENT, UNKNOWN}` 的条目（含 INTENT，修 P0-1）。"""
        with self._lock:
            return [e for k, e in self._entries.items()
                    if self._acb_of.get(k) == acb_id
                    and e.state in ("INTENT", "UNKNOWN")]

    def state(self, idem_key: str) -> str | None:
        with self._lock:
            entry = self._entries.get(idem_key)
            return entry.state if entry else None

    def get(self, idem_key: str) -> EffectEntry | None:
        with self._lock:
            return self._entries.get(idem_key)

    # ---- probe（§3.5 / §4.1）--------------------------------------
    def probe_one(self, idem_key: str) -> str:
        """三态 probe：`"True"` / `"False"` / `"None"`。

        - double-checked locking：先无锁查缓存，加锁后复查，再调外部 probe
          （保证同一 idem_key 的外部探针只调用一次、`EFFECT_PROBED` 只写一次）；
        - 内部 `try/except BaseException → "None"`（异常不得冒泡终止 run，修 P1-2）；
        - 未配置 probe → `"None"`（fail-closed，Q2）。
        """
        cached = self._probes.get(idem_key)
        if cached is not None:
            return cached
        with self._lock:
            cached = self._probes.get(idem_key)
            if cached is not None:
                return cached
            if self._probe is None:
                probe = "None"
            else:
                try:
                    result = self._probe(idem_key)
                except BaseException:  # noqa: BLE001  # 契约：probe 异常一律降级为 "None"
                    result = None
                probe = "None" if result is None else ("True" if result else "False")
            self._probes[idem_key] = probe
            entry = self._entries.get(idem_key)
            if entry is not None:
                entry.probe = probe
            acb_id = self._acb_of.get(idem_key, "")
            self._event_log.append(
                EventKind.EFFECT_PROBED, acb_id,
                {"idem_key": idem_key, "probe": probe},
                producer=acb_id, idempotency_key=idem_key,
                run_id=self._run_of.get(idem_key, ""))
            return probe

    # ---- 重建（§4.3）----------------------------------------------
    def rebuild_from(self, events: list[Event]) -> int:
        """从传入快照重建（调用方已按 `producer` 过滤）。返回条目数。"""
        count = 0
        with self._lock:
            for ev in events:
                payload = ev.payload or {}
                if ev.kind == EventKind.EFFECT_INTENT:
                    idem_key = payload.get("idem_key", "")
                    if not idem_key:
                        continue
                    self._entries[idem_key] = EffectEntry(
                        idem_key=idem_key,
                        effect_id=payload.get("effect_id", ""),
                        tool=payload.get("tool", ""),
                        args=payload.get("args") or {},
                        state="INTENT",
                        updated_at=ev.ts / 1000.0)
                    self._acb_of[idem_key] = ev.subject
                    self._run_of[idem_key] = ev.run_id
                    count += 1
                elif ev.kind == EventKind.EFFECT_CONFIRMED:
                    idem_key = payload.get("idem_key", "")
                    entry = self._entries.get(idem_key)
                    if entry is not None:
                        entry.state = "CONFIRMED"
                        if payload.get("effect_id"):
                            entry.effect_id = payload["effect_id"]
                        entry.updated_at = ev.ts / 1000.0
                        self._acb_of.setdefault(idem_key, ev.subject)
                elif ev.kind == EventKind.EFFECT_UNKNOWN:
                    idem_key = payload.get("idem_key", "")
                    entry = self._entries.get(idem_key)
                    if entry is not None:
                        entry.state = "UNKNOWN"
                        entry.updated_at = ev.ts / 1000.0
                        self._acb_of.setdefault(idem_key, ev.subject)
                elif ev.kind == EventKind.EFFECT_PROBED:
                    idem_key = payload.get("idem_key", "")
                    probe = payload.get("probe", "")
                    self._probes[idem_key] = probe
                    entry = self._entries.get(idem_key)
                    if entry is not None:
                        entry.probe = probe
        return count
