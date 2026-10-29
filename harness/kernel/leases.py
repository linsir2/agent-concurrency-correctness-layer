"""WriterLeaseService —— 单写者租约、run 级 epoch、WAL/提交点 fencing（S1 迁出）。

设计要点（`storage/lease.py` / S10a / S24-8）：
- **互斥靠单调 epoch**，不靠时钟；租约过期只决定"何时允许接管"（liveness）。
- 取租约前必须先恢复 durable epoch，否则干净重启首写会被 fence。
- `LeaseStore` 提供时走可复制后端（Raft/内存），否则读写本地 `writer_lease.json`。
"""

from __future__ import annotations

import hashlib
import os
import threading
from contextlib import nullcontext

from dataclasses import dataclass

from ..storage._lock import (
    FencingRejected,
)
from ..storage.atomic import atomic_write
from ..storage.codec import dumps, loads
from ..storage.lease import (
    UNAVAILABLE,
    LeaseGrant,
    LeaseUnavailable,
)


class EpochStore:
    """K-43 两层 epoch 中的 **run 层**：`read_epoch(run_id)` / `bump_epoch(run_id)`。

    - 全局 writer lease epoch 仍由 `wal.log.epoch` 承担（接管 fencing，保留）；
    - 本 store 按 run 分键，隔离**同一 run 内**的租约（监督者回收一个 run
      不得作废另一个 run 的租约）；
    - `run_id=None`（无参）→ 全局键 `""`，退回全局语义（向后兼容）；
    - 持久化：`path_prefix` 给定 → 每 run 一个文件 `"{prefix}.{run_id}"`
      （WAL 模式即 `wal.log.epoch.<run_id>`），`atomic_write` 保证不撕裂。
    """

    def __init__(self, path_prefix=None) -> None:
        self._lock = threading.RLock()
        self._epochs: dict[str, int] = {}
        self._prefix = str(path_prefix) if path_prefix else None

    @staticmethod
    def _key(run_id: str | None) -> str:
        return run_id or ""

    @staticmethod
    def _safe(key: str) -> str:
        if not key:
            return "global"
        clean = "".join(c if (c.isalnum() or c in "._-") else "_" for c in key)
        if clean != key:
            clean = f"{clean}-{hashlib.sha1(key.encode('utf-8')).hexdigest()[:8]}"
        return clean

    def _path(self, key: str):
        if self._prefix is None:
            return None
        return f"{self._prefix}.{self._safe(key)}"

    def _load(self, key: str) -> int:
        path = self._path(key)
        if path is None:
            return 0
        try:
            with open(path, "r", encoding="utf-8") as fh:
                return int((fh.read() or "0").strip())
        except (OSError, ValueError):
            return 0

    def _store(self, key: str, epoch: int) -> int:
        self._epochs[key] = epoch
        path = self._path(key)
        if path is not None:
            atomic_write(path, str(epoch).encode("utf-8"))
        return epoch

    def read_epoch(self, run_id: str | None = None) -> int:
        with self._lock:
            key = self._key(run_id)
            if key not in self._epochs:
                self._epochs[key] = self._load(key)
            return int(self._epochs.get(key, 0) or 0)

    def set_epoch(self, run_id: str | None, epoch: int) -> int:
        with self._lock:
            key = self._key(run_id)
            epoch = int(epoch or 0)
            cur = self.read_epoch(run_id)
            if epoch > cur:
                self._store(key, epoch)
            return int(self._epochs.get(key, 0) or 0)

    def bump_epoch(self, run_id: str | None = None) -> int:
        with self._lock:
            key = self._key(run_id)
            return self._store(key, self.read_epoch(run_id) + 1)


@dataclass(frozen=True)
class LeaseDeps:
    budget: object
    checkpoints: object
    clock: object
    event_log: object
    intents: object
    epoch_scope: str
    wal_epoch_backend: object | None = None


class WriterLeaseService:
    def __init__(self, deps, state) -> None:
        self.deps = deps
        self.state = state

    def _wire_wal_fencing(self) -> None:
        """S11b K-46：把 fencing 三件套注入 `WalEventLog`（fencing **绑 WAL**）。

        - `lock`：`wal.epoch_lock`（独立 epoch 锁，全序第 4 位）；
        - `epoch_reader`：`wal.read_epoch`（读 `wal.log.epoch`，无锁原子读）；
        - `epoch_provider`：本写者当前 run 级 epoch（`intents.get_epoch()`）。

        **不再绑 store.json**：旧实现用 `checkpoints._flock` + `read_epoch`，
        导致「两个实例共享 WAL、不同 checkpoint root」时互不 fence（S11-3 负对照）。
        非 WAL 实现 / 无 epoch 接口 → 不注入（fencing 不生效）。
        """
        log = self.deps.event_log
        setter = getattr(log, "set_fencing", None)
        if not callable(setter):
            return
        wal = getattr(log, "_wal", None)
        epoch_lock = getattr(wal, "epoch_lock", None)
        read_epoch = getattr(wal, "read_epoch", None)
        if epoch_lock is None or not callable(read_epoch):
            return
        setter(lock=epoch_lock, epoch_reader=read_epoch,
               epoch_provider=lambda: int(self.deps.intents.get_epoch() or 0))
    def wire_lease_fence(self) -> None:
        """S24-8：租约与 Raft 事件日志**共享同一 RSM** 时，在提交点复核旧 epoch。

        条件不满足（事件日志不是 Raft，或 cluster 未启用租约状态）→ **不注入**，
        由 `ensure_writer_lease` 在写入口拒绝；这一边界在文档里如实标注
        （写入口检查 + 租约 TTL，不是零窗口）。绝不假装「提交点已 fence」。
        """
        if self.state.lease_store is None:
            return
        setter = getattr(self.deps.event_log, "set_lease_fence", None)
        replica = getattr(self.deps.event_log, "_replica", None)
        if not callable(setter) or not hasattr(replica, "lease"):
            return
        # S24-P0：租约与事件必须落在**同一个** Raft 集群，否则「提交点复核」
        # 是对着另一个状态机做的假检查（写入口挡得住，但 TOCTOU 窗口仍在）。
        store_cluster = getattr(self.state.lease_store, "cluster", None)
        log_cluster = getattr(self.deps.event_log, "_cluster", None)
        if store_cluster is not None and log_cluster is not None \
                and store_cluster is not log_cluster:
            raise LeaseUnavailable(
                "租约与事件日志必须共享同一 Raft 集群（否则提交点没有 fence）")
        setter(lambda: {
            "owner": self.state.writer_id,
            "instance": self.state.lease_instance,
            "epoch": int(self.state.writer_epoch),
            "now_ms": int(self.deps.clock.now_ms()),
        })
    def _apply_lease_grant(self, grant: LeaseGrant) -> None:
        """把可复制后端的 grant 落到 Harness 内部状态（epoch 同步到 intents/budget）。"""
        self.state.lease_outcome = grant.outcome
        self.state.lease_reason = grant.reason
        if not grant.ok:
            self.state.writer_lease_ok = False
            return
        self.state.writer_lease_ok = True
        epoch = int(grant.epoch)
        self.state.writer_epoch = epoch
        # 只在推进时写 intents/budget，避免每次续租都触发预算事件。
        if epoch != int(self.deps.intents.get_epoch() or 0):
            self.deps.intents.set_epoch(epoch)
            self.deps.budget.set_epoch(epoch, emit=False)
    def failure_reason(self) -> str:
        """写入口被拒时的可观测原因（区分「别人持有」与「后端不可用」）。"""
        if self.state.lease_outcome == UNAVAILABLE:
            return "writer_lease_unavailable"
        return "writer_lease_held"
    def _lease_path(self):
        root = getattr(self.deps.checkpoints, "root", None)
        if root is None:
            return None
        return os.path.join(str(root), "writer_lease.json")
    def _read_lease(self) -> dict | None:
        path = self._lease_path()
        if not path or not os.path.exists(path):
            return None
        try:
            with open(path, "r", encoding="utf-8") as fh:
                data = loads(fh.read())
        except Exception:  # noqa: BLE001
            return None
        return data if isinstance(data, dict) else None
    def _store_flock(self):
        lock = getattr(self.deps.checkpoints, "_flock", None)
        return lock() if lock is not None else nullcontext()
    def _try_acquire_writer_lease(self) -> bool:
        """获取 durable 单写者租约。

        接管 = 确认旧租约**过期** → `bump_epoch` → 落盘 → 才开始服务（§2.5）。
        旧租约仍有效且属于**其他写者** → 返回 False（拒绝服务，避免双写者）；
        同一写者（同 owner）重入 → 仅续期（同进程多 Harness 共享同一写者身份）。

        K-46：`bump_epoch` 只取独立 `epoch` 锁，**必须在 `store.lock` 之外**
        调用（否则 store(5) → epoch(4) 反向获取 → `LockOrderViolation`，接管崩）。
        """
        if self.state.lease_store is not None:
            try:
                grant = self.state.lease_store.acquire(
                    owner=self.state.writer_id, instance=self.state.lease_instance,
                    now_ms=int(self.deps.clock.now_ms()), ttl_ms=self.state.lease_ms)
            except LeaseUnavailable:
                self.state.lease_outcome = UNAVAILABLE
                self.state.lease_reason = "lease_store_unavailable"
                self.state.writer_lease_ok = False
                return False
            self._apply_lease_grant(grant)
            return grant.ok
        path = self._lease_path()
        if not path:
            self.state.writer_lease_ok = True
            return True
        takeover = False
        with self._store_flock():
            now = self.deps.clock.now_ms()
            cur = self._read_lease()
            if cur is not None:
                exp = int(cur.get("expires_at", 0) or 0)
                same_owner = cur.get("owner") == self.state.writer_id
                if exp and now < exp:
                    if not same_owner:
                        self.state.writer_lease_ok = False
                        return False
                    doc = dict(cur)                  # 同写者重入：续期，不 bump epoch
                    doc["expires_at"] = now + int(self.state.lease_ms)
                    atomic_write(path, dumps(doc).encode("utf-8"))
                    self.state.writer_lease_ok = True
                    return True
                if not same_owner:
                    takeover = True                  # 旧租约已过期 → 接管
        # 终审 7-2：接管必须**先 bump_epoch（独立 epoch 锁）再写租约**。
        # 旧实现「store.lock 内先落租约 → 锁外 bump」存在窗口：W1 在途写会在
        # bump 之前读到未推进的 durable epoch → fence 通过 → W2 接管后仍写成功。
        epoch = self.bump_epoch() if takeover else self.deps.intents.get_epoch()
        with self._store_flock():
            cur = self._read_lease()
            if cur is not None and cur.get("owner") != self.state.writer_id:
                exp = int(cur.get("expires_at", 0) or 0)
                if exp and self.deps.clock.now_ms() < exp:
                    # 别的写者在 bump 与落盘之间抢到了租约 → 放弃（epoch 已推进，无害）
                    self.state.writer_lease_ok = False
                    return False
            doc = {"owner": self.state.writer_id,
                   "epoch": epoch,
                   "expires_at": self.deps.clock.now_ms() + int(self.state.lease_ms)}
            parent = os.path.dirname(path)
            if parent:
                os.makedirs(parent, exist_ok=True)
            atomic_write(path, dumps(doc).encode("utf-8"))
            self.state.writer_lease_ok = True
        return True
    def _renew_writer_lease(self) -> bool:
        """续租（仅当本写者仍是持有者）。

        S24-8 的可复制后端**不做自动重取**：已丢失租约的旧写者必须被拒绝
        （否则它可以用新 epoch 抢回锁，违背「接管后旧写者退出」的预期）。
        """
        if self.state.lease_store is not None:
            try:
                grant = self.state.lease_store.renew(
                    owner=self.state.writer_id, instance=self.state.lease_instance,
                    epoch=self.state.writer_epoch, now_ms=int(self.deps.clock.now_ms()),
                    ttl_ms=self.state.lease_ms)
            except LeaseUnavailable:
                self.state.lease_outcome = UNAVAILABLE
                self.state.lease_reason = "lease_store_unavailable"
                self.state.writer_lease_ok = False
                return False
            self._apply_lease_grant(grant)
            return grant.ok
        path = self._lease_path()
        if not path:
            self.state.writer_lease_ok = True
            return True
        with self._store_flock():
            cur = self._read_lease()
            now = self.deps.clock.now_ms()
            if cur is not None and cur.get("owner") == self.state.writer_id:
                doc = dict(cur)
                doc["expires_at"] = now + int(self.state.lease_ms)
                atomic_write(path, dumps(doc).encode("utf-8"))
                self.state.writer_lease_ok = True
                return True
        # 非本写者：**释放 store.lock 后**再走接管（接管内可能 bump epoch）
        return self._try_acquire_writer_lease()
    def ensure_writer_lease(self) -> bool:
        """写入口守卫：durable 模式下必须持有/可续租；失败 → 拒绝服务。"""
        if not self.state.durable:
            return True
        return self._renew_writer_lease()
    def _release_writer_lease(self) -> None:
        """释放本写者的租约（EventLog 关闭 = 写者会话结束）。"""
        if self.state.lease_store is not None:
            try:
                self.state.lease_store.release(
                    owner=self.state.writer_id, instance=self.state.lease_instance,
                    epoch=self.state.writer_epoch)
            except LeaseUnavailable:
                pass          # 关闭阶段后端不可用：不阻断关闭（租约自然过期）
            return
        path = self._lease_path()
        if not path:
            return
        with self._store_flock():
            cur = self._read_lease()
            if cur is not None and cur.get("owner") == self.state.writer_id:
                try:
                    os.unlink(path)
                except OSError:
                    pass
    def _epoch_backend(self):
        """run 级 epoch 的持久化后端：WAL 优先（K-46），无 WAL → checkpoint store。"""
        backend = self.deps.wal_epoch_backend
        if backend is not None:
            return backend
        return self.deps.checkpoints
    def _restore_epoch(self) -> int:
        """从持久化存储读 run 级 epoch（RMW 语义，§3.4）。"""
        if self.state.lease_store is not None:
            epoch = int(self.state.lease_store.read_epoch())
            self.state.writer_epoch = epoch
            self.deps.intents.set_epoch(epoch)
            self.deps.budget.set_epoch(epoch, emit=False)
            return epoch
        reader = getattr(self._epoch_backend(), "read_epoch", None)
        epoch = 0
        if callable(reader):
            try:
                epoch = int(reader())
            except Exception:  # noqa: BLE001
                epoch = 0
        self.state.writer_epoch = epoch
        self.deps.intents.set_epoch(epoch)
        self.deps.budget.set_epoch(epoch, emit=False)
        return epoch
    def bump_epoch(self, reason: str = "", run_id: str | None = None, *,
                   epoch_scope: str | None = None) -> int:
        """epoch 单调递增（K-43 两层）。

        - `epoch_scope="run"`（构造参数，可用 `epoch_scope=` 覆盖）+ `run_id`
          → **run 层**：只推进该 run 的 epoch（`EpochStore`），只作废该 run 的
          租约 → 其他 run 不受影响；
        - `run_id is None` 或 `epoch_scope="global"` → **全局 writer lease 层**：
          推进 `wal.log.epoch`（接管 fencing），作废全部租约（向后兼容）。

        K-46：全局路径只取独立 epoch 锁，不得在 `store.lock` 内调用。
        """
        scope = self.deps.epoch_scope if epoch_scope is None else epoch_scope
        if run_id is None or scope == "global":
            if self.state.lease_store is not None:
                # S24-8：epoch 只能由共识推进；调用者必须是当前持有者，
                # 否则说明自己已被接管 —— 拒绝而不是静默自增。
                grant = self.state.lease_store.advance(
                    owner=self.state.writer_id, instance=self.state.lease_instance,
                    epoch=self.state.writer_epoch, now_ms=int(self.deps.clock.now_ms()),
                    ttl_ms=self.state.lease_ms)
                if not grant.ok:
                    raise FencingRejected(
                        f"lease advance 被拒：{grant.outcome}:{grant.reason}")
                self._apply_lease_grant(grant)
                self.deps.budget.set_epoch(self.state.writer_epoch, emit=True)
                return self.state.writer_epoch
            bumper = getattr(self._epoch_backend(), "bump_epoch", None)
            epoch = None
            if callable(bumper):
                try:
                    epoch = int(bumper())
                except Exception:  # noqa: BLE001
                    epoch = None
            if epoch is None:
                epoch = self.deps.intents.get_epoch() + 1
            self.state.writer_epoch = epoch
            self.deps.intents.set_epoch(epoch)
            self.deps.budget.set_epoch(epoch, emit=True)
            return epoch
        # ---- run 层（K-43）：per-run epoch，隔离同 run 内的租约 ----
        epoch = self.deps.budget.bump_epoch(run_id=run_id)     # store 唯一一次推进 + 事件
        self.deps.intents.bump_epoch(run_id, epoch=epoch)      # 作废该 run 租约
        return epoch
