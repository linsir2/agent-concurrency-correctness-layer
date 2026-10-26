"""IntentRegistry —— 文档 §4.4 / §5.4。

- `reserve`：非阻塞预留（对应 TCC 的 Try）
- `is_valid`：按租约有效性判定（v1.2 P0-3，替代全局单调 token）
- `revoke`：只作废该租约（不误拒他人）
- `renew`：只对当前 epoch 生效（revoke 后不可复活）
- `compatible`：准入判定，只拦契约/不变量与读-写方向冲突（**不拦同资源写-写**）
"""

from __future__ import annotations

import itertools
import threading

from .types import Lease


class IntentRegistry:
    def __init__(self) -> None:
        self._leases: dict[str, Lease] = {}
        self._active: dict[str, list[Lease]] = {}
        self._logical: dict[str, dict[str, str]] = {}   # dep_id -> {agent_id: kind}
        self._counter = itertools.count(1)
        self._lock = threading.RLock()
        self._epoch = 0

    def declare_logical(self, dep_id: str, agent_id: str, kind: str) -> None:
        """声明逻辑依赖（文档 §7.3）。供准入判定使用。"""
        with self._lock:
            self._logical.setdefault(dep_id, {})[agent_id] = kind

    def reserve(self, resource: str, agent_id: str, base_version: int,
                ttl_ms: int = 5000, mode: str = "WRITE") -> Lease:
        with self._lock:
            lease = Lease(
                lease_id=f"L{next(self._counter)}",
                resource=resource,
                agent_id=agent_id,
                epoch=self._epoch,
                ttl_ms=ttl_ms,
                valid=True,
            )
            self._leases[lease.lease_id] = lease
            self._active.setdefault(resource, []).append(lease)
            return lease

    def is_valid(self, lease: Lease) -> bool:
        cur = self._leases.get(lease.lease_id)
        return bool(cur and cur.valid and cur.epoch == lease.epoch)

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
        """只对当前 epoch 生效；revoke 后不可复活（文档 §4.4）。"""
        with self._lock:
            if self._epoch != lease.epoch:
                return False
            cur = self._leases.get(lease.lease_id)
            if cur and cur.valid:
                return True
            return False

    def renew_for(self, acb) -> bool:
        """guard 中调用：续期该 agent 的所有有效租约（v1.10 修复）。"""
        with self._lock:
            if self._epoch == 0:
                return True
            ok = False
            for lease in self._leases.values():
                if lease.agent_id == acb.id and lease.valid and lease.epoch == self._epoch:
                    ok = True
            return ok

    def bump_epoch(self) -> None:
        """Supervisor 回收时调用：旧租约全部失效。"""
        with self._lock:
            self._epoch += 1
            for lease in self._leases.values():
                lease.valid = False

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
