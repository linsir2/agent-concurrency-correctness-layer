"""IntentRegistry —— 文档 §4.4 / §5.4。

- `reserve`：非阻塞预留（对应 TCC 的 Try）
- `is_valid`：无锁只读，按 `valid`/`epoch`/`expires_at` 判定（§6.4）
- `revoke`：只作废该租约（不误拒他人）
- `renew`：检查过期并续期；只对当前 epoch 生效（revoke 后不可复活）
- `compatible`：准入判定，只拦契约/不变量与读-写方向冲突（**不拦同资源写-写**）
"""

from __future__ import annotations

import itertools
import threading

from .clock import SystemClock
from .types import Lease


class IntentRegistry:
    def __init__(self, clock=None, epoch_store=None,
                 run_scope: str = "global") -> None:
        self._clock = clock or SystemClock()
        self._leases: dict[str, Lease] = {}
        self._active: dict[str, list[Lease]] = {}
        self._logical: dict[str, dict[str, str]] = {}   # dep_id -> {agent_id: kind}
        self._counter = itertools.count(1)
        self._lock = threading.RLock()
        self._epoch = 0
        # S12 K-43：run 层 epoch store（Harness 注入；None → 全局语义）
        self._epoch_store = epoch_store
        # S12 K-43：`run_scope="run"` 才启用 per-run 隔离；默认 "global" 保持旧语义
        self._run_scope = run_scope if run_scope in ("run", "global") else "global"

    def _run_scoped(self, run_id: str | None) -> bool:
        return (self._run_scope == "run" and bool(run_id)
                and self._epoch_store is not None)

    def _epoch_for(self, run_id: str | None) -> int:
        """本地 epoch：per-run（`run_scope="run"` 且给了 run_id）→
        `max(全局, run 层)`；否则全局（旧语义）。

        全局 writer lease epoch 是跨 run 的下界（接管 fencing 对所有 run 生效），
        run 层只在其上叠加隔离 → 其他 run 的租约不被本 run 的 bump 误伤。
        """
        epoch = self._epoch
        if self._run_scoped(run_id):
            epoch = max(epoch, int(self._epoch_store.read_epoch(run_id)))
        return epoch

    def declare_logical(self, dep_id: str, agent_id: str, kind: str) -> None:
        """声明逻辑依赖（文档 §7.3）。供准入判定使用。"""
        with self._lock:
            self._logical.setdefault(dep_id, {})[agent_id] = kind

    def undeclare_logical(self, dep_id: str, agent_id: str) -> None:
        """撤销逻辑依赖声明（准入失败回滚用）。"""
        with self._lock:
            holders = self._logical.get(dep_id)
            if not holders or agent_id not in holders:
                return
            del holders[agent_id]
            if not holders:
                self._logical.pop(dep_id, None)

    def reserve(self, resource: str, agent_id: str, base_version: int,
                ttl_ms: int = 5000, mode: str = "WRITE",
                run_id: str = "") -> Lease:
        with self._lock:
            mode = "W" if mode == "WRITE" else mode
            expires_at = 0 if ttl_ms <= 0 else self._clock.now_ms() + ttl_ms
            lease = Lease(
                lease_id=f"L{next(self._counter)}",
                resource=resource,
                agent_id=agent_id,
                epoch=self._epoch_for(run_id or None),
                ttl_ms=ttl_ms,
                valid=True,
                mode=mode,
                expires_at=expires_at,
                run_id=run_id or "",
            )
            self._leases[lease.lease_id] = lease
            self._active.setdefault(resource, []).append(lease)
            return lease

    def is_valid(self, lease: Lease) -> bool:
        """无锁只读：按 `lease_id` 查注册表当前租约，再判 valid/epoch/expires_at。

        注册表无该 `lease_id`（如调用方持有外部租约）时回退到传入对象。
        K-43：epoch 按**租约所属 run** 比较（per-run bump 不误伤其他 run）。
        """
        cur = self._leases.get(lease.lease_id, lease)
        if not cur.valid:
            return False
        if cur.epoch != self._epoch_for(cur.run_id or None):
            return False
        if cur.expires_at == 0:
            return True
        return self._clock.now_ms() < cur.expires_at

    def revoke(self, lease: Lease) -> None:
        """只作废该租约（文档 §5.4 P0-3）。"""
        with self._lock:
            cur = self._leases.get(lease.lease_id)
            if cur:
                cur.valid = False
            lst = self._active.get(lease.resource, [])
            self._active[lease.resource] = [l for l in lst if l.lease_id != lease.lease_id]

    def revoke_all(self, agent_id: str) -> None:
        for lease in list(self._leases.values()):
            if lease.agent_id == agent_id:
                self.revoke(lease)

    def renew(self, lease: Lease) -> bool:
        """检查过期并续期；只对当前 epoch 生效（revoke 后不可复活，文档 §3.3/§4.4）。"""
        with self._lock:
            if lease.epoch != self._epoch_for(lease.run_id or None):
                return False
            cur = self._leases.get(lease.lease_id)
            if cur is None or not cur.valid:
                return False
            now = self._clock.now_ms()
            if cur.expires_at != 0 and now >= cur.expires_at:
                return False
            cur.expires_at = now + cur.ttl_ms
            lease.expires_at = cur.expires_at
            return True

    def reap_expired(self, now_ms: int | None = None) -> int:
        """回收**已失效**的租约（K-99）。

        删除条件（二者任一）：
          ① `not lease.valid` —— `revoke`/`IntentRegistry.bump_epoch` 置的失效终态；
          ② `lease.epoch != 当前 epoch` —— **epoch 失效**。全局 `Harness.bump_epoch`
             走 `set_epoch`，只推进 epoch、不逐个置 `valid=False`；若只看 ①，
             这些 `is_valid` 已为 False 的租约将**永不回收** → 无界增长。

        **不要求「已过期」**：`expires_at==0`（永不过期）的失效租约同样要回收。
        `valid=True` **且 epoch 匹配** 的租约一律不删（哪怕已过期）：那是
        「持有者只是没续期」的诊断证据，`is_valid` 仍会拒它。

        注意：这只解决**孤儿回收**（不再有活持有者）。跨进程唯一写已由
        Raft 单写者 + `flock` 保证（S10/S16），**不需要复制租约状态**（K-103）。
        `now_ms` 仅为兼容签名/可注入时间；失效判定不依赖它。
        """
        removed = 0
        with self._lock:
            for lid in list(self._leases):
                lease = self._leases[lid]
                # epoch 匹配且 valid → 仍有效，绝不删
                if lease.valid and lease.epoch == self._epoch_for(lease.run_id or None):
                    continue
                self._leases.pop(lid, None)
                lst = self._active.get(lease.resource)
                if lst:
                    self._active[lease.resource] = [l for l in lst if l.lease_id != lid]
                removed += 1
        return removed

    def renew_for(self, acb) -> bool:
        """guard 中调用：续期该 agent 的所有未过期有效租约（v1.10 修复）。"""
        with self._lock:
            now = self._clock.now_ms()
            ok = False
            for lease in self._leases.values():
                if lease.agent_id != acb.id:
                    continue
                if lease.epoch != self._epoch_for(lease.run_id or None):
                    continue
                if not lease.valid:
                    continue
                if lease.expires_at != 0 and now >= lease.expires_at:
                    continue
                lease.expires_at = now + lease.ttl_ms
                ok = True
            return ok

    def bump_epoch(self, run_id: str | None = None,
                   epoch: int | None = None) -> int | None:
        """Supervisor 回收时调用：旧租约全部失效。

        K-43：`run_id` 给定 → 只推进该 run 的 epoch、只作废该 run 的租约
        （其他 run 不受影响）；无参 → 全局（作废全部租约，向后兼容）。
        `epoch` 给定时**不重复 bump** store（由调用方统一推进，避免双计数）。
        """
        with self._lock:
            if not self._run_scoped(run_id):
                # 全局 writer lease 层（`run_scope="global"` 或未给 run_id）
                self._epoch += 1
                for lease in self._leases.values():
                    lease.valid = False
                return self._epoch
            if epoch is None:
                epoch = int(self._epoch_store.bump_epoch(run_id))
            else:
                self._epoch_store.set_epoch(run_id, epoch)
            for lease in self._leases.values():
                if lease.run_id == run_id:
                    lease.valid = False
            return epoch

    def get_epoch(self, run_id: str | None = None) -> int:
        """当前 epoch（`run_id` 给定 → run 层；否则全局，§4.4）。"""
        with self._lock:
            return self._epoch_for(run_id or None)

    def set_epoch(self, epoch: int, run_id: str | None = None) -> int:
        """从**持久化存储**恢复 epoch（单调，禁止回绕）。

        S10a §3.4：跨进程恢复时 epoch 必须读持久化值，否则新进程从 0 开始
        → 旧租约被误判为有效（fencing 失效）。
        K-43：`run_id` 给定 → 写 run 层 store；否则全局（向后兼容）。
        """
        with self._lock:
            epoch = int(epoch or 0)
            if self._run_scoped(run_id):
                return self._epoch_store.set_epoch(run_id, epoch)
            if epoch > self._epoch:
                self._epoch = epoch
            return self._epoch

    def compatible(self, dep_id: str, agent_id: str, kind: str = "CONTRACT") -> bool:
        """准入判定（文档 §5.2⑤ / §7.3）——**真实 fail-fast**。

        - `CONTRACT` / `INVARIANT` / `TYPE_GRAPH`：若**其他 agent** 已声明同一逻辑依赖
          → 冲突 → `False`（准入 WAIT，避免 write skew）。
        - **同资源写-写不拦**（放行至提交层 CAS 裁决）。
        """
        with self._lock:
            holders = self._logical.get(dep_id, {})
            for other in holders:
                if other != agent_id:
                    return False
            return True

    def active_intents(self, resource: str) -> list[Lease]:
        return list(self._active.get(resource, []))
