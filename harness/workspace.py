"""WorkspaceManager + WorkspaceVersionStore —— 文档 §5.4 / §3.3。

**隔离模型（v1.8 修复 P0-3）**：
- `stage(resource, bytes)` = 写 agent 的 **Z3 worktree**（私有暂存）
- `prepare/apply` = 把 worktree 内容**并入 Z2 共享态**的 **merge 点**

RSC = OCC（read_set/write_set 验证）+ 按租约 fencing + 短临界区 + Saga 补偿。
"""

from __future__ import annotations

import copy
import hashlib
import threading

from .types import Mode, Resource, Status, Write
from .event_log import EventLog
from .intent_registry import IntentRegistry


class WorkspaceVersionStore:
    """保存每个资源的「提交前快照」，供 3-way merge 取 base 版本内容。"""

    def __init__(self) -> None:
        self._snapshots: dict[str, list[tuple[int, object]]] = {}

    def record(self, resource: str, version: int, content) -> None:
        self._snapshots.setdefault(resource, []).append((version, copy.deepcopy(content)))

    def get(self, resource: str, version: int):
        for v, content in self._snapshots.get(resource, []):
            if v == version:
                return content
        return None


def _hash(content) -> str:
    return hashlib.sha256(repr(content).encode("utf-8")).hexdigest()[:16]


class WorkspaceManager:
    def __init__(self, event_log: EventLog, intents: IntentRegistry,
                 versions: WorkspaceVersionStore | None = None) -> None:
        self._resources: dict[str, Resource] = {}
        self._event_log = event_log
        self._intents = intents
        self._versions = versions or WorkspaceVersionStore()
        self._lock = threading.RLock()

    # ---- 资源管理 -------------------------------------------------
    def put(self, name: str, content) -> Resource:
        with self._lock:
            if name not in self._resources:
                self._resources[name] = Resource(name=name, version=0, content=content)
            else:
                self._resources[name].content = content
            self._versions.record(name, self._resources[name].version,
                                  self._resources[name].content)
            return self._resources[name]

    def version(self, name: str) -> int:
        r = self._resources.get(name)
        return r.version if r else 0

    def content_hash(self, name: str) -> str:
        r = self._resources.get(name)
        return _hash(r.content) if r else _hash(None)

    def get(self, name: str):
        r = self._resources.get(name)
        return copy.deepcopy(r.content) if r else None

    def content(self, name: str):
        return self.get(name)

    # ---- 暂存（Z3 worktree） --------------------------------------
    def stage(self, acb, resource: str, content) -> None:
        """写 agent 的 Z3 worktree；同时记录前像（供补偿）。"""
        acb.staged[resource] = copy.deepcopy(content)
        cur = self._resources.get(resource)
        if cur is not None:
            self._versions.record(resource, cur.version, cur.content)

    # ---- RSC 提交（文档 §5.4） ------------------------------------
    def prepare(self, acb, w: Write, lease, mode: Mode) -> Status:
        with self._lock:
            if not self._intents.is_valid(lease):
                return Status.REJECT_STALE
            if mode == Mode.COMMIT:
                if acb.base_version.get(w.resource, self.version(w.resource)) != self.version(w.resource):
                    return Status.CONFLICT
                for r, v in acb.read_set.items():
                    if v != self.version(r):
                        return Status.REJECT_STALE_READ
            if mode == Mode.COMPENSATE:
                if self.content_hash(w.resource) != w.applied_hash:
                    return Status.REJECT_COMPENSATE
            return Status.VALID

    def apply(self, acb, w: Write, lease, mode: Mode) -> Status:
        """锁内复验 + 原子替换 + 版本推进 + 事件同事务（I8）。"""
        with self._lock:
            if not self._intents.is_valid(lease):
                return Status.REJECT_STALE
            if mode == Mode.COMMIT:
                if acb.base_version.get(w.resource, self.version(w.resource)) != self.version(w.resource):
                    return Status.CONFLICT
                for r, v in acb.read_set.items():
                    if v != self.version(r):
                        return Status.REJECT_STALE_READ
            if mode == Mode.COMPENSATE:
                if self.content_hash(w.resource) != w.applied_hash:
                    return Status.REJECT_COMPENSATE

            content = acb.staged.get(w.resource, w.content)
            r = self._resources.get(w.resource)
            if r is None:
                r = Resource(name=w.resource, version=0, content=None)
                self._resources[w.resource] = r
            r.content = copy.deepcopy(content)
            r.version += 1
            w.version_before = r.version - 1
            w.applied_hash = self.content_hash(w.resource)

            self._event_log.append_in_txn(
                "RESOURCE_COMMITTED", w.resource, {"version": r.version},
                producer=acb.id, idempotency_key=f"{acb.run_id}:{acb.id}",
            )
            acb.base_version[w.resource] = r.version
            acb.read_set[w.resource] = r.version
            return Status.OK

    # ---- 合并算法（文档 §7.2.1） ----------------------------------
    def rebase_json(self, base, ours, theirs):
        """JSON-aware merge：键级合并；数组默认 grow-only（删除需 pivot）。"""
        if not isinstance(base, dict) or not isinstance(ours, dict) or not isinstance(theirs, dict):
            return self._rebase_text(base, ours, theirs)
        result = {}
        conflicts = []
        for key in set(base) | set(ours) | set(theirs):
            b, o, t = base.get(key), ours.get(key), theirs.get(key)
            if o == t:
                result[key] = copy.deepcopy(o)
            elif b == o:
                result[key] = copy.deepcopy(t)
            elif b == t:
                result[key] = copy.deepcopy(o)
            else:
                conflicts.append(key)
        return {"merged": result, "conflicts": conflicts}

    def _rebase_text(self, base, ours, theirs):
        """文本 3-way merge（简化版：非重叠则合并，重叠则标记冲突）。"""
        if base == ours:
            return {"merged": copy.deepcopy(theirs), "conflicts": []}
        if base == theirs:
            return {"merged": copy.deepcopy(ours), "conflicts": []}
        if ours == theirs:
            return {"merged": copy.deepcopy(ours), "conflicts": []}
        return {"merged": None, "conflicts": ["<text-overlap>"]}

    def compute_reverse_patch(self, w: Write):
        """基于 WorkspaceVersionStore 的「提交前快照」计算反向 patch。"""
        return self._versions.get(w.resource, w.version_before)
