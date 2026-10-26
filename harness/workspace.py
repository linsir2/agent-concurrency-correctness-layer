"""WorkspaceManager + WorkspaceVersionStore —— 文档 §3.3 / §4 / §5.1–§5.7 / §6。

**读模型（S2）**：`manifest: {resource: ManifestEntry}` 是**唯一读路径**；
磁盘文件降级为可选物化视图（`path_of`，默认 `None` 不物化）。
与 git / OCI / etcd 同构：**内容不可变（blob）+ 指针可变（manifest）**。

**锁**：每资源一把 `RLock`，批量写按 `LOCK_KEY`（字典序）全序 all-or-nothing 获取。
"""

from __future__ import annotations

import copy
import json
import threading
from dataclasses import dataclass

from .types import EventKind, Mode, Result, Status, Write
from .event_log import EventLog
from .intent_registry import IntentRegistry
from .storage.blob import InMemoryBlobStore, _BYTES_PREFIX, canonical_bytes


class WorkspaceVersionStore:
    """保存每个资源的「提交前快照」，供 3-way merge 取 base 版本内容。

    按 `(resource, version)` 去重：WAL 重放会多次遇到同一版本（D-6）。
    """

    def __init__(self) -> None:
        self._snapshots: dict[str, list[tuple[int, object]]] = {}

    def record(self, resource: str, version: int, content) -> None:
        lst = self._snapshots.setdefault(resource, [])
        for v, _ in lst:
            if v == version:
                return
        lst.append((version, copy.deepcopy(content)))

    def get(self, resource: str, version: int):
        for v, content in self._snapshots.get(resource, []):
            if v == version:
                return content
        return None


@dataclass(frozen=True)
class ManifestEntry:
    """manifest 条目（文档 §3.1）。"""
    version: int
    blob_hash: str
    prev_hash: str = ""


class WorkspaceManager:
    def __init__(self, event_log: EventLog, intents: IntentRegistry,
                 versions: WorkspaceVersionStore | None = None, *,
                 blobs=None, path_of=None, lock_timeout_s: float = 5.0) -> None:
        self.manifest: dict[str, ManifestEntry] = {}
        self._event_log = event_log
        self._intents = intents
        self._versions = versions or WorkspaceVersionStore()
        self._blobs = blobs or InMemoryBlobStore()
        self._path_of = path_of
        self._lock_timeout_s = lock_timeout_s

        self._registry_lock = threading.Lock()          # 只护锁表，极短，不回调
        self._locks: dict[str, threading.RLock] = {}

        self._rebuild_from_events()

    # ---- 锁（文档 §4） --------------------------------------------
    def lock_for(self, resource: str) -> threading.RLock:
        with self._registry_lock:
            lk = self._locks.get(resource)
            if lk is None:
                lk = threading.RLock()
                self._locks[resource] = lk
            return lk

    def _acquire_all(self, resources, timeout: float | None = None) -> tuple[list[str], bool]:
        """按 `LOCK_KEY`（字典序）全序 all-or-nothing 获取。

        `acquire` 抛异常时释放已持有并 re-raise；返回**真实** held
        （超时也返回已持有列表，由调用方 finally 释放，避免重复 release）。
        """
        to = self._lock_timeout_s if timeout is None else timeout
        ordered = sorted(set(resources))                  # 先去重再排序
        held: list[str] = []
        try:
            for r in ordered:
                if self.lock_for(r).acquire(timeout=to):
                    held.append(r)
                else:
                    return held, False
            return held, True
        except BaseException:
            self._release_all(held)
            raise

    def _release_all(self, held: list[str]) -> None:
        for r in reversed(held):
            self.lock_for(r).release()

    # ---- 读（唯一路径，文档 §5.1） --------------------------------
    def get(self, resource: str):
        entry = self.manifest.get(resource)
        if entry is None:
            return None
        return self._decode(entry.blob_hash)

    def _decode(self, blob_hash: str):
        raw = self._blobs.get(blob_hash)
        if raw.startswith(_BYTES_PREFIX):          # 剥魔数前缀还原 bytes
            return raw[len(_BYTES_PREFIX):]
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return raw

    def content(self, resource: str):
        return self.get(resource)

    def version(self, resource: str) -> int:
        entry = self.manifest.get(resource)
        return entry.version if entry else 0

    def content_hash(self, resource: str) -> str:
        entry = self.manifest.get(resource)
        return entry.blob_hash if entry else ""

    # ---- 写（文档 §5.2） ------------------------------------------
    def put(self, resource: str, content) -> ManifestEntry:
        """upsert：每次 put 都是一个新版本（与 commit_batch 同一套版本语义）。

        仅用于**种子**；运行期写入口固定为 `Harness.commit_batch`。
        """
        if content is None:
            raise ValueError("资源内容不得为 None（与「不存在」歧义）")
        with self.lock_for(resource):
            blob_hash = self._blobs.put(canonical_bytes(content))
            cur = self.manifest.get(resource)
            version = (cur.version if cur else 0) + 1
            entry = ManifestEntry(version=version, blob_hash=blob_hash,
                                  prev_hash=cur.blob_hash if cur else "")
            self.manifest[resource] = entry
            ev = self._event_log.append(
                EventKind.WORKSPACE_COMMITTED, resource,
                {"resource": resource, "version": version, "blob_hash": blob_hash,
                 "prev_hash": entry.prev_hash},
                notify=False,
            )
        self._event_log.notify_events([ev])       # 出锁后通知（避免锁内回调）
        return entry

    # ---- 暂存（Z3 worktree，文档 §5.3） ----------------------------
    def stage(self, acb, resource: str, content) -> None:
        """覆盖式暂存；同时把「读版本」写入 read_set/base_version（每次 stage = 重新读）。"""
        with self.lock_for(resource):
            ver = self.version(resource)
            acb.read_set[resource] = ver
            acb.base_version[resource] = ver
            acb.pre_image[resource] = self.content_hash(resource)
            acb.staged[resource] = copy.deepcopy(content)

    # ---- 校验（文档 §5.4） ----------------------------------------
    def prepare(self, acb, w: Write, lease, mode: Mode) -> Result:
        """公共入口：自己取锁（测试 / 单写直接用）。"""
        with self.lock_for(w.resource):
            return self._validate_locked(acb, w, lease, mode)

    def _validate_locked(self, acb, w: Write, lease, mode: Mode) -> Result:
        """内部：调用方已持锁。两条入口语义相同。"""
        if not self._intents.is_valid(lease):
            return Result(Status.REJECT_STALE,
                          {"lease_id": lease.lease_id, "reason": "lease_invalid"})
        if mode == Mode.COMMIT:
            expected = self._base_of(acb, w.resource)
            actual = self.version(w.resource)
            if expected != actual:
                return Result(Status.CONFLICT,
                              {"resource": w.resource, "version": actual,
                               "expected": expected, "diff": self._diff(w.resource)})
            for r, v in acb.read_set.items():                 # 读集合复验
                if v != self.version(r):
                    return Result(Status.REJECT_STALE_READ,
                                  {"resource": r, "expected": v,
                                   "actual": self.version(r)})
        if mode == Mode.COMPENSATE:
            if self.content_hash(w.resource) != w.applied_hash:
                return Result(Status.REJECT_COMPENSATE,
                              {"resource": w.resource, "expected_hash": w.applied_hash,
                               "actual_hash": self.content_hash(w.resource)})
        return Result(Status.VALID,
                      {"resource": w.resource, "version": self.version(w.resource)})

    def _base_of(self, acb, resource: str) -> int:
        """对「已读资源」恒等；未读资源 = 无读依赖（文档 §6.1）。"""
        if resource in acb.base_version:
            return acb.base_version[resource]
        if resource in acb.read_set:
            return acb.read_set[resource]
        return self.version(resource)

    def _diff(self, resource: str) -> dict:
        entry = self.manifest.get(resource)
        return {"prev_hash": entry.prev_hash if entry else "",
                "current_hash": entry.blob_hash if entry else ""}

    # ---- 投影（文档 §5.6） ----------------------------------------
    def _apply_locked(self, acb, w: Write, blob_hash: str, prev_hash: str) -> None:
        """只做投影（manifest + 可选 link）+ 前像记录；不取锁（调用方已持）。"""
        cur = self.manifest.get(w.resource)
        base_version = cur.version if cur else 0
        self._versions.record(w.resource, base_version, self.get(w.resource))
        new_version = base_version + 1
        self.manifest[w.resource] = ManifestEntry(
            version=new_version, blob_hash=blob_hash, prev_hash=prev_hash)
        w.version_before = base_version
        w.applied_hash = blob_hash
        if self._path_of is not None:                 # 默认 None → 不写磁盘
            self._blobs.link(blob_hash, self._path_of(w.resource))

    # ---- 重建（文档 §5.7） ----------------------------------------
    def _rebuild_from_events(self) -> None:
        """从同一份 WAL 重放，重建 manifest 与前像（manifest 是纯逻辑投影）。"""
        for ev in self._event_log.replay():
            p = ev.payload or {}
            if ev.kind not in (EventKind.RESOURCE_COMMITTED,
                               EventKind.WORKSPACE_COMMITTED):
                continue
            resource = p.get("resource") or ev.subject
            blob_hash = p.get("blob_hash")
            prev_hash = p.get("prev_hash", "")
            if not resource or not blob_hash:
                continue                              # 旧格式事件：跳过
            version = int(p.get("version", 0))
            cur = self.manifest.get(resource)
            if cur is not None and version <= cur.version:
                continue                              # 版本未推进：忽略（不静默覆盖）
            if not self._blobs.exists(blob_hash):
                continue                              # 当前 blob 不存在：跳过
            # D-6：前像重建 —— 上一版本的 blob 就是 version-1 的内容
            if prev_hash and self._blobs.exists(prev_hash):
                self._versions.record(resource, version - 1, self._decode(prev_hash))
            self.manifest[resource] = ManifestEntry(
                version=version, blob_hash=blob_hash, prev_hash=prev_hash)

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
