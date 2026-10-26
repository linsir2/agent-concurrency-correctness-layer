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
    def __init__(self, clock=None) -> None:
        self._clock = clock or SystemClock()
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
                ttl_ms: int = 5000, mode: str = "WRITE") -> Lease:
        with self._lock:
            mode = "W" if mode == "WRITE" else mode
            expires_at = 0 if ttl_ms <= 0 else self._clock.now_ms() + ttl_ms
            lease = Lease(
                lease_id=f"L{next(self._counter)}",
                resource=resource,
                agent_id=agent_id,
                epoch=self._epoch,
                ttl_ms=ttl_ms,
                valid=True,
                mode=mode,
                expires_at=expires_at,
            )
            self._leases[lease.lease_id] = lease
            self._active.setdefault(resource, []).append(lease)
            return lease

    def is_valid(self, lease: Lease) -> bool:
        """无锁只读：按 `lease_id` 查注册表当前租约，再判 valid/epoch/expires_at。

        注册表无该 `lease_id`（如调用方持有外部租约）时回退到传入对象。
        """
        cur = self._leases.get(lease.lease_id, lease)
        if not cur.valid:
            return False
        if cur.epoch != self._epoch:
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
            if lease.epoch != self._epoch:
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

    def renew_for(self, acb) -> bool:
        """guard 中调用：续期该 agent 的所有未过期有效租约（v1.10 修复）。"""
        with self._lock:
            now = self._clock.now_ms()
            ok = False
            for lease in self._leases.values():
                if lease.agent_id != acb.id or lease.epoch != self._epoch:
                    continue
                if not lease.valid:
                    continue
                if lease.expires_at != 0 and now >= lease.expires_at:
                    continue
                lease.expires_at = now + lease.ttl_ms
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
