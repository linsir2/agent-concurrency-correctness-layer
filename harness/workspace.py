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
from .rebase import DELETE, MISSING, merge3
from .storage.atomic import atomic_delete
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

    def has(self, resource: str, version: int) -> bool:
        """是否存在该版本的前像（区分「无前像」与「前像就是 None」）。"""
        return any(v == version for v, _ in self._snapshots.get(resource, []))

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
        self._tombstones: dict[str, int] = {}
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

    def pre_image(self, resource: str, version: int):
        """§5.3：取某版本的提交前快照（3-way merge 的 base）；不存在返回 `MISSING`。

        「无前像」与「前像就是 None」必须可区分（用 `has()` 而非 `get()` 的返回值）。
        """
        if self._versions.has(resource, version):
            return self._versions.get(resource, version)
        return MISSING

    def version(self, resource: str) -> int:
        entry = self.manifest.get(resource)
        if entry is not None:
            return entry.version
        return self._tombstones.get(resource, 0)   # manifest → tombstone → 0

    def _next_version(self, resource: str) -> int:
        """下一个单调版本号：`max(manifest.version, tombstone, 0) + 1`。

        统一 put / _apply_locked / _commit_record 的版本来源，
        避免「删除后重建」用 manifest 缺失去回绕到 1（文档 §3.3 / §19）。
        """
        entry = self.manifest.get(resource)
        cur = entry.version if entry is not None else 0
        return max(cur, self._tombstones.get(resource, 0), 0) + 1

    def _guard_version(self, resource: str) -> int:
        """当前最高已见版本（manifest 或 tombstone），供重放做版本守卫。"""
        return self._next_version(resource) - 1

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
            version = self._next_version(resource)
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
            actual_hash = self.content_hash(w.resource)
            if actual_hash != w.applied_hash:
                return Result(Status.REJECT_COMPENSATE,
                              {"resource": w.resource, "expected_hash": w.applied_hash,
                               "actual_hash": actual_hash})
            actual_version = self.version(w.resource)
            if actual_version != w.version_before + 1:
                return Result(Status.REJECT_COMPENSATE,
                              {"resource": w.resource,
                               "expected_version": w.version_before + 1,
                               "actual_version": actual_version})
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
    def _apply_locked(self, acb, w: Write, blob_hash: str, prev_hash: str,
                      deleted: bool = False) -> None:
        """只做投影（manifest + 可选 link）+ 前像记录；不取锁（调用方已持）。"""
        cur = self.manifest.get(w.resource)
        base_version = max(cur.version if cur is not None else 0,
                           self._tombstones.get(w.resource, 0))
        new_version = self._next_version(w.resource)
        if deleted:                                   # Delete 哨兵：删除并写 tombstone
            self.manifest.pop(w.resource, None)
            self._tombstones[w.resource] = new_version
            w.version_before = base_version
            w.applied_hash = ""
            if self._path_of is not None:             # 可选物化：原子删除
                atomic_delete(self._path_of(w.resource))
            return
        if cur is not None:                           # create 无前像 → 不记录（否则 has() 误判）
            self._versions.record(w.resource, base_version, self.get(w.resource))
        self.manifest[w.resource] = ManifestEntry(
            version=new_version, blob_hash=blob_hash, prev_hash=prev_hash)
        self._tombstones.pop(w.resource, None)       # 重建：清墓碑
        w.version_before = base_version
        w.applied_hash = blob_hash
        if self._path_of is not None:                 # 默认 None → 不写磁盘
            self._blobs.link(blob_hash, self._path_of(w.resource))

    # ---- 重建（文档 §5.7） ----------------------------------------
    def _rebuild_from_events(self) -> None:
        """从同一份 WAL 重放，重建 manifest、tombstone 与前像（manifest 是纯逻辑投影）。"""
        for ev in self._event_log.replay():
            p = ev.payload or {}
            if ev.kind not in (EventKind.RESOURCE_COMMITTED,
                               EventKind.WORKSPACE_COMMITTED):
                continue
            resource = p.get("resource") or ev.subject
            if not resource:
                continue
            version = int(p.get("version", 0))
            guard = self._guard_version(resource)     # max(manifest, tombstone)
            if bool(p.get("deleted")):                # 删除事件：pop + tombstone
                if version <= guard:
                    continue                          # 版本未推进（含已删资源）：忽略，防复活
                self.manifest.pop(resource, None)
                self._tombstones[resource] = version
                continue
            blob_hash = p.get("blob_hash")
            prev_hash = p.get("prev_hash", "")
            if not blob_hash:
                continue                              # 旧格式事件：跳过
            if version <= guard:
                continue                              # 版本未推进（含已删资源）：忽略，防复活
            if not self._blobs.exists(blob_hash):
                continue                              # 当前 blob 不存在：跳过
            # D-6：前像重建 —— 上一版本的 blob 就是 version-1 的内容
            if prev_hash and self._blobs.exists(prev_hash):
                self._versions.record(resource, version - 1, self._decode(prev_hash))
            self.manifest[resource] = ManifestEntry(
                version=version, blob_hash=blob_hash, prev_hash=prev_hash)
            self._tombstones.pop(resource, None)     # 重建：清墓碑

    # ---- 合并算法（文档 §7.2.1） ----------------------------------
    def rebase_json(self, base, ours, theirs):
        """`merge3` 的兼容薄封装（K-1）：返回旧 dict 结构。

        `{"merged": <完整结果或 None>, "conflicts": [path, ...]}`；
        有冲突时 `merged=None`、`conflicts` 为冲突路径列表（按 path 排序）。
        """
        mr = merge3(base, ours, theirs)
        if mr.ok:
            return {"merged": mr.merged, "conflicts": []}
        return {"merged": None, "conflicts": [c.path for c in mr.conflicts]}

    def compute_reverse_patch(self, w: Write):
        """基于 WorkspaceVersionStore 的「提交前快照」计算反向 patch。

        - 有前像 → 返回前像内容；
        - 无前像且 `version_before == 0`（create 型写）→ `DELETE` 哨兵；
        - 其他（前像被淘汰 / 版本不连续）→ `None`（不可补偿 → PIVOT）。
        """
        if self._versions.has(w.resource, w.version_before):
            return self._versions.get(w.resource, w.version_before)
        if w.version_before == 0:
            return DELETE
        return None
