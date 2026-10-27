"""CompensationJournal + CompEntry —— 文档 `s5-recovery-and-journals.md` v1.1 §3.4 / §4.2（S5 第一波）。

- `CompEntry`：补偿条目（含 `comp_id` 幂等键、`deleted` 哨兵）。
- `CompensationJournal.plan`：**一次 `append_batch`** 写全部 `COMPENSATION_ENTRY`
  + `COMPENSATING_BATCH`（同帧原子、一次 fdatasync，修 P1-3）；
- `mark(comp_id, state)`：写 `COMPENSATION_STATE`；
- `resume(acb)`：**先按 `e.state` 分支**（`COMPENSATED→done`、`FAILED→escalate`、
  `PIVOT→pivot`），仅 `COMPENSATING` 才看 hash/版本（修 P0-3）；
- `rebuild_from(events)`：从传入快照重建（调用方按 `producer` 过滤，§4.3）。

事件写**同一个** `EventLog`（单一数据源，§3.1）。
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from .event_log import EventLog
from .rebase import DELETE
from .types import Event, EventKind


@dataclass
class CompEntry:
    """补偿条目（§3.4）。`comp_id` 幂等键；`deleted` 用于 DELETE 哨兵补偿。"""

    comp_id: str     # sha256(run_id|acb_id|logical_subgoal_id|resource|version_before|applied_hash|target_hash)[:16]
    acb_id: str
    resource: str
    version_before: int
    applied_hash: str
    target_hash: str
    state: str = "PENDING"       # PENDING | COMPENSATING | COMPENSATED | FAILED | PIVOT
    deleted: bool = False        # DELETE 哨兵补偿必须用版本号，不能用空 hash


def comp_id_of(run_id: str, acb_id: str, logical_subgoal_id: str, resource: str,
               version_before: int, applied_hash: str, target_hash: str) -> str:
    """`sha256(run_id|acb_id|logical_subgoal_id|resource|version_before|applied_hash|target_hash)[:16]`。"""
    payload = (f"{run_id}|{acb_id}|{logical_subgoal_id}|{resource}|"
               f"{version_before}|{applied_hash}|{target_hash}")
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


class CompensationJournal:
    def __init__(self, event_log: EventLog) -> None:
        self._entries: dict[str, CompEntry] = {}
        self._run_of: dict[str, str] = {}
        self._event_log = event_log

    # ---- 计划（一批一帧，§4.2）------------------------------------
    def plan(self, acb, applied) -> list[CompEntry]:
        """把 `applied` 的全部补偿条目 + 批次标记放进**同一帧**（一次 `append_batch`）。"""
        entries: list[CompEntry] = []
        drafts: list[dict] = []
        comp_ids: list[str] = []
        for w in applied:
            target_hash = self._target_hash(acb, w)
            cid = comp_id_of(acb.run_id, acb.id, acb.logical_subgoal_id, w.resource,
                             w.version_before, w.applied_hash, target_hash)
            comp_ids.append(cid)
            existing = self._entries.get(cid)
            if existing is not None:
                entries.append(existing)                    # 幂等：已计划则不重复落盘
                continue
            entry = CompEntry(
                comp_id=cid, acb_id=acb.id, resource=w.resource,
                version_before=w.version_before, applied_hash=w.applied_hash,
                target_hash=target_hash, state="COMPENSATING",
                deleted=(w.content is DELETE))
            self._entries[cid] = entry
            self._run_of[cid] = acb.run_id
            entries.append(entry)
            drafts.append({
                "kind": EventKind.COMPENSATION_ENTRY,
                "subject": acb.id,
                "payload": {
                    "comp_id": cid, "resource": w.resource,
                    "version_before": w.version_before,
                    "applied_hash": w.applied_hash, "target_hash": target_hash,
                    "deleted": entry.deleted,
                },
                "producer": acb.id,
                "run_id": acb.run_id,
                "idempotency_key": cid,
            })
        if drafts:                                          # 批次标记与全部条目同一帧
            drafts.append({
                "kind": EventKind.COMPENSATING_BATCH,
                "subject": acb.id,
                "payload": {"acb_id": acb.id, "comp_ids": comp_ids},
                "producer": acb.id,
                "run_id": acb.run_id,
                "idempotency_key": f"batch:{acb.id}:{comp_ids[0]}",
            })
            self._event_log.append_batch(drafts)
        return entries

    # ---- 状态标记（§4.2）------------------------------------------
    def mark(self, comp_id: str, state: str, **kw) -> bool:
        entry = self._entries.get(comp_id)
        acb_id = entry.acb_id if entry is not None else kw.get("acb_id", "")
        if entry is not None:
            entry.state = state
        self._event_log.append(
            EventKind.COMPENSATION_STATE, acb_id,
            {"comp_id": comp_id, "state": state},
            producer=acb_id, idempotency_key=comp_id,
            run_id=self._run_of.get(comp_id, kw.get("run_id", "")))
        return entry is not None

    # ---- 续做分类（§4.2 / §4.3）-----------------------------------
    def resume(self, acb, workspace=None) -> dict:
        """先按 `e.state` 分支；仅 `COMPENSATING` 才看 hash/版本（修 P0-3）。"""
        plan = {"done": [], "retry": [], "escalate": [], "pivot": []}
        for entry in self._entries.values():
            if entry.acb_id != acb.id:
                continue
            if entry.state == "COMPENSATED":
                plan["done"].append(entry)
            elif entry.state == "FAILED":
                plan["escalate"].append(entry)
            elif entry.state == "PIVOT":
                plan["pivot"].append(entry)
            elif entry.state == "COMPENSATING":
                plan[self._classify_compensating(entry, workspace)].append(entry)
            else:                                            # PENDING → 视作待执行
                plan["retry"].append(entry)
        return plan

    @staticmethod
    def _classify_compensating(entry: CompEntry, workspace) -> str:
        """`COMPENSATING` 的 hash/版本判定；未给 workspace 时一律 `retry`。"""
        if workspace is None:
            return "retry"
        current = workspace.content_hash(entry.resource)
        if entry.deleted:
            exists = workspace.get(entry.resource) is not None
            # 删除类：必须用版本号判定（空 hash 无法区分「不存在」与「删除后」）
            if not exists and workspace.version(entry.resource) == entry.version_before + 1:
                return "done"
            if exists and current != entry.applied_hash:
                return "pivot"
            return "retry"
        if current == entry.target_hash:
            return "done"
        if current != entry.applied_hash:
            return "pivot"
        return "retry"

    # ---- 重建（§4.3）----------------------------------------------
    def rebuild_from(self, events: list[Event]) -> int:
        """从传入快照重建（调用方已按 `producer` 过滤）。返回条目数。"""
        count = 0
        for ev in events:
            payload = ev.payload or {}
            if ev.kind == EventKind.COMPENSATION_ENTRY:
                cid = payload.get("comp_id", "")
                if not cid:
                    continue
                self._entries[cid] = CompEntry(
                    comp_id=cid, acb_id=ev.subject,
                    resource=payload.get("resource", ""),
                    version_before=int(payload.get("version_before", 0)),
                    applied_hash=payload.get("applied_hash", ""),
                    target_hash=payload.get("target_hash", ""),
                    state="COMPENSATING",
                    deleted=bool(payload.get("deleted", False)))
                self._run_of[cid] = ev.run_id
                count += 1
            elif ev.kind == EventKind.COMPENSATION_STATE:
                cid = payload.get("comp_id", "")
                entry = self._entries.get(cid)
                if entry is not None:
                    entry.state = payload.get("state", entry.state)
            elif ev.kind == EventKind.COMPENSATING_BATCH:
                for cid in payload.get("comp_ids", []):
                    entry = self._entries.get(cid)
                    if entry is not None and entry.state == "PENDING":
                        entry.state = "COMPENSATING"
        return count

    # ---- 内部 ------------------------------------------------------
    @staticmethod
    def _target_hash(acb, w) -> str:
        """补偿目标内容 hash（前像）；无显式前像时为空串。"""
        pre_image = getattr(acb, "pre_image", None) or {}
        return pre_image.get(w.resource, "")
