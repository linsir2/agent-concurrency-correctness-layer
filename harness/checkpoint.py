"""CheckpointStore + EffectJournal —— 文档 §4.2 / §4.4 / §5.8。

- `CheckpointStore.save` 含 `turn_seq` / `staged` / `workspace_commit` /
  `effect_journal_offset` / `cursor` / `aging_deadline`（**唯一恢复依据**）
- `EffectJournal` 有 `begin` / `confirm` / `mark_unknown` 接口（v1.10 修复）
"""

from __future__ import annotations

import copy
import itertools

from .types import EventKind
from .event_log import EventLog


class CheckpointStore:
    def __init__(self) -> None:
        self._store: dict[str, dict] = {}
        self._counter = itertools.count(1)
        self.slots = None            # 由 Harness 注入，用于快照活跃槽位

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
        }
        acb.result_ref = ref
        return ref

    def load(self, ref: str) -> dict:
        return copy.deepcopy(self._store[ref])


class EffectJournal:
    """外部副作用账本。`idem_key` 是幂等键（I7）。"""

    def __init__(self, event_log: EventLog) -> None:
        self._entries: dict[str, dict] = {}
        self._event_log = event_log
        self._counter = itertools.count(1)

    def begin(self, acb, tool: str, args, idem_key: str) -> str:
        """幂等：已存在（含 CONFIRMED/UNKNOWN）则不覆盖（I7）。"""
        existing = self._entries.get(idem_key)
        if existing:
            return existing["effect_id"]
        eid = f"E{next(self._counter)}"
        self._entries[idem_key] = {
            "effect_id": eid, "state": "INTENT", "tool": tool,
            "args": args, "idem_key": idem_key,
        }
        return eid

    def confirm(self, idem_key: str, acb_id: str = "") -> bool:
        entry = self._entries.get(idem_key)
        if not entry:
            return False
        entry["state"] = "CONFIRMED"
        self._event_log.append(
            EventKind.EFFECT_CONFIRMED, idem_key,
            {"tool": entry["tool"]}, producer=acb_id, idempotency_key=idem_key,
        )
        return True

    def mark_unknown(self, idem_key: str, acb_id: str = "") -> None:
        entry = self._entries.get(idem_key)
        if entry:
            entry["state"] = "UNKNOWN"
        self._event_log.append(
            EventKind.EFFECT_UNKNOWN, idem_key, {}, producer=acb_id,
            idempotency_key=idem_key,
        )

    def state(self, idem_key: str) -> str | None:
        entry = self._entries.get(idem_key)
        return entry["state"] if entry else None

    def get(self, idem_key: str) -> dict | None:
        return self._entries.get(idem_key)
