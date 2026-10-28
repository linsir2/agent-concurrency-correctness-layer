"""S24-8：可复制的写者租约（跨主机 single-writer 的基础）。

**为什么需要它**：现有 `writer_lease.json` + `wal.log.epoch` 只在「同一份共享
目录」上有效；两台主机各自本地租约时会出现双写者。本模块把租约状态抽成
**纯状态机**（`LeaseState`），由 `MemoryLeaseStore`（同进程/测试）或
`RaftLeaseStore`（共识复制，见 `storage/raft.py`）执行。规则只写一份，
避免两个后端语义漂移。

**安全模型（与 Kleppmann 的 fencing 论证一致）**：

- 租约过期/接管**只决定「何时允许换人」**（活性），不决定互斥本身；
- 互斥由**单调 epoch** 保证：接管一定 `epoch + 1`，被保护资源（WAL / Raft
  日志）必须拒绝 `epoch` 落后的写入；
- 因此「进程 pause 后醒来」「时钟略快」都不会造成双写 —— 旧 epoch 必然被拒。

**边界**：租约本身的到期判断依赖调用方传入的 `now_ms`（各副本必须用**命令
参数**，不得各自取墙钟，否则状态机发散）。跨主机 Raft 的时钟漂移只影响
「谁在什么时刻能接管」，不影响互斥安全。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field

# ---- 结果枚举（字符串常量，避免调用方依赖 Enum 序列化） -------------------
ACQUIRED = "acquired"
RENEWED = "renewed"
HELD = "held"
FENCED = "fenced"
EXPIRED = "expired"
RELEASED = "released"
ADVANCED = "advanced"
UNAVAILABLE = "unavailable"

_OK = (ACQUIRED, RENEWED, ADVANCED)


class LeaseUnavailable(RuntimeError):
    """租约后端不可用（共识未就绪/复制失败）。

    **失败必须向上传播**：调用方要拒绝写，绝不允许静默退回本地文件租约。
    """


@dataclass(frozen=True)
class LeaseGrant:
    """一次租约操作的结果（不可变，便于跨进程/事件回报）。"""

    outcome: str
    owner: str = ""
    instance: str = ""
    epoch: int = 0
    expires_at_ms: int = 0
    reason: str = ""

    @property
    def ok(self) -> bool:
        return self.outcome in _OK

    def as_dict(self) -> dict:
        return {"outcome": self.outcome, "owner": self.owner,
                "instance": self.instance, "epoch": self.epoch,
                "expires_at_ms": self.expires_at_ms, "reason": self.reason}


@dataclass
class LeaseState:
    """租约的**纯状态机**（无锁、无 I/O）：两个后端共用同一套规则。"""

    owner: str = ""
    instance: str = ""
    epoch: int = 0
    expires_at_ms: int = 0
    released: bool = True

    # ------------------------------------------------------------------ 查询
    def active(self, now_ms: int) -> bool:
        """当前是否有一个**未过期且未释放**的持有者。"""
        return (not self.released) and int(self.expires_at_ms) > int(now_ms)

    def holder(self) -> tuple[str, str]:
        return (self.owner, self.instance)

    def snapshot(self) -> dict:
        return {"owner": self.owner, "instance": self.instance,
                "epoch": self.epoch, "expires_at_ms": self.expires_at_ms,
                "released": self.released}

    def _grant(self, outcome: str, *, reason: str = "",
               expires_at_ms: int | None = None) -> LeaseGrant:
        return LeaseGrant(outcome=outcome, owner=self.owner,
                          instance=self.instance, epoch=self.epoch,
                          expires_at_ms=(self.expires_at_ms if expires_at_ms is None
                                         else expires_at_ms),
                          reason=reason)

    # ------------------------------------------------------------------ 写
    def acquire(self, *, owner: str, instance: str, now_ms: int,
                ttl_ms: int, expected_epoch: int | None = None) -> LeaseGrant:
        """申请/续租/接管。

        - 同一 `(owner, instance)` 且未过期 → 续期（epoch 不变）；
        - 其他持有者未过期 → `held`（**不等待、不排队**，由宿主决定）；
        - 给了 `expected_epoch` 但与当前 epoch 不符 → `fenced`
          （说明期间已有别人接管；旧写者不得凭旧视图复活）；
        - 已释放或已过期 → `epoch + 1` 授予（接管）。
        """
        owner, instance = str(owner), str(instance)
        ttl = max(1, int(ttl_ms))
        now = int(now_ms)
        if not owner or not instance:
            return self._grant(FENCED, reason="bad_owner_or_instance")
        if expected_epoch is not None and int(expected_epoch) != self.epoch:
            return self._grant(FENCED,
                               reason=f"expected_epoch {expected_epoch} != {self.epoch}")
        if self.active(now):
            if self.holder() != (owner, instance):
                return self._grant(HELD, reason="lease_held")
            self.expires_at_ms = now + ttl
            return self._grant(RENEWED, expires_at_ms=self.expires_at_ms)
        # 首次 / 已释放 / 已过期 → 授予新 epoch（单调 +1）
        self.epoch += 1
        self.owner, self.instance = owner, instance
        self.expires_at_ms = now + ttl
        self.released = False
        return self._grant(ACQUIRED, expires_at_ms=self.expires_at_ms)

    def renew(self, *, owner: str, instance: str, epoch: int,
              now_ms: int, ttl_ms: int) -> LeaseGrant:
        """续租：身份/epoch 不符 → `fenced`；已释放 → `released`；已过期 → `expired`。"""
        if self.holder() != (str(owner), str(instance)) or int(epoch) != self.epoch:
            return self._grant(FENCED, reason="stale_epoch_or_owner")
        if self.released:
            return self._grant(RELEASED, reason="lease_released")
        if not self.active(now_ms):
            return self._grant(EXPIRED, reason="lease_expired")
        self.expires_at_ms = int(now_ms) + max(1, int(ttl_ms))
        return self._grant(RENEWED, expires_at_ms=self.expires_at_ms)

    def release(self, *, owner: str, instance: str, epoch: int) -> LeaseGrant:
        """主动释放（epoch 保留、不清零；旧 epoch 仍然无效）。"""
        if self.holder() != (str(owner), str(instance)) or int(epoch) != self.epoch:
            return self._grant(FENCED, reason="stale_epoch_or_owner")
        self.released = True
        self.expires_at_ms = 0
        return self._grant(RELEASED, expires_at_ms=0)

    def advance(self, *, owner: str, instance: str, epoch: int,
                now_ms: int, ttl_ms: int) -> LeaseGrant:
        """监督者推进 epoch 作废旧 worker：**保持自己是持有者**（不释放）。

        语义对齐单机 `wal.log.epoch`：监督者推进水位后仍继续服务，而其他实例
        手里的旧 epoch 立即失效。若在这里标记 `released`，监督者自己的下一次
        提交也会被拒 —— 与既有 `bump_epoch` 语义不符。
        """
        if self.holder() != (str(owner), str(instance)) or int(epoch) != self.epoch:
            return self._grant(FENCED, reason="stale_epoch_or_owner")
        self.epoch += 1
        self.released = False
        self.expires_at_ms = int(now_ms) + max(1, int(ttl_ms))
        return self._grant(ADVANCED, expires_at_ms=self.expires_at_ms)

    def authorizes(self, *, owner: str, instance: str, epoch: int,
                   now_ms: int) -> bool:
        """被保护资源（WAL/Raft 日志）在提交点的复核：当前租约是否授权这次写。"""
        return (self.holder() == (str(owner), str(instance))
                and int(epoch) == self.epoch
                and self.active(now_ms))


@dataclass
class LeaseStore:
    """租约后端接口（`MemoryLeaseStore` / `RaftLeaseStore` 实现）。

    `replicated=False` 的后端只能用于**同进程**多 Harness 测试，不能当成跨主机
    方案 —— 这一点必须如实标注，不能靠名字暗示。
    """

    replicated: bool = field(default=False, init=False)

    def acquire(self, *, owner: str, instance: str, now_ms: int, ttl_ms: int,
                expected_epoch: int | None = None) -> LeaseGrant:
        raise NotImplementedError

    def renew(self, *, owner: str, instance: str, epoch: int,
              now_ms: int, ttl_ms: int) -> LeaseGrant:
        raise NotImplementedError

    def release(self, *, owner: str, instance: str, epoch: int) -> LeaseGrant:
        raise NotImplementedError

    def advance(self, *, owner: str, instance: str, epoch: int,
                now_ms: int, ttl_ms: int) -> LeaseGrant:
        raise NotImplementedError

    def authorizes(self, *, owner: str, instance: str, epoch: int,
                   now_ms: int) -> bool:
        raise NotImplementedError

    def current(self) -> dict:
        raise NotImplementedError

    def read_epoch(self) -> int:
        raise NotImplementedError


class MemoryLeaseStore(LeaseStore):
    """进程内租约后端（确定性；**不是**跨主机复制）。

    用途：主门禁里模拟「两个 Harness 争同一把锁」，以及给 `RaftLeaseStore`
    做语义对照。`fail=True` 模拟后端故障（断言调用方 fail-closed、不回退）。
    """

    replicated = False

    def __init__(self, *, fail: bool = False) -> None:
        self._lock = threading.RLock()
        self._state = LeaseState()
        self._fail = bool(fail)
        self.calls = 0

    # ---- 内部 -----------------------------------------------------
    def _run(self, fn, **kw):
        self.calls += 1
        if self._fail:
            raise LeaseUnavailable("MemoryLeaseStore 注入故障")
        with self._lock:
            return fn(**kw)

    # ---- 接口 -----------------------------------------------------
    def acquire(self, **kw) -> LeaseGrant:
        return self._run(self._state.acquire, **kw)

    def renew(self, **kw) -> LeaseGrant:
        return self._run(self._state.renew, **kw)

    def release(self, **kw) -> LeaseGrant:
        return self._run(self._state.release, **kw)

    def advance(self, **kw) -> LeaseGrant:
        return self._run(self._state.advance, **kw)

    def authorizes(self, **kw) -> bool:
        with self._lock:
            if self._fail:
                raise LeaseUnavailable("MemoryLeaseStore 注入故障")
            return self._state.authorizes(**kw)

    def current(self) -> dict:
        with self._lock:
            return self._state.snapshot()

    def read_epoch(self) -> int:
        with self._lock:
            return int(self._state.epoch)

    # ---- 测试辅助 -------------------------------------------------
    def set_fail(self, fail: bool) -> None:
        with self._lock:
            self._fail = bool(fail)
