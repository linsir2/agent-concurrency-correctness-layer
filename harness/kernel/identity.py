"""SigningService —— 身份、签名、幂等键、事件分发（原 `Harness` 555-1178 段的一支）。

职责边界（只有四件事，不再多）：
1. 惰性构造 `Signer`（`HARNESS_AUTH_SECRET` 或注入 secret）；
2. 取 ACB 的有效 token（`capability` → `policy._tokens` → `default_token`）；
3. 计算幂等键 `idem_key`；
4. 事件回调：推进 per-run 游标 + 周期性 gc（budget / jti）+ 订阅通知。

依赖方向：identity → receipts（仅 `track_run_cursor`，晚绑定），见 `bind_receipts`。
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from ..auth import Signer
from ..types import ACB, EventKind


@dataclass(frozen=True)
class IdentityDeps:
    clock: object
    policy: object
    budget: object
    subscriptions: object
    auth_gc_every: int
    gc_every: int


class SigningService:
    def __init__(self, deps: IdentityDeps, state) -> None:
        self.deps = deps
        self.state = state
        self._receipts = None            # 晚绑定（ReceiptChain 构造晚于本服务）

    # ---- 接线 -----------------------------------------------------
    def bind_receipts(self, receipts) -> None:
        """`ReceiptChain` 构造完成后注入（唯一的跨服务依赖，方向 identity → receipts）。"""
        self._receipts = receipts

    # ---- 事件分发（文档 §5.6）--------------------------------------
    def on_event(self, ev) -> None:
        # S12 K-33：per-run 高水位游标独立推进（run 之间不串）
        if self._receipts is not None:
            self._receipts.track_run_cursor(ev)
        # 周期性清理已过期的 jti（按注入时钟，避免 `_seen` 无界增长）
        self.state.event_count += 1
        if self.state.event_count % self.deps.gc_every == 0:
            # §4.4：接上 budget.gc（原死代码），now 用注入时钟的 epoch 秒
            self.deps.budget.gc(self.deps.clock.now_ms() / 1000.0)
        if (self.state.auth is not None
                and self.state.event_count % self.deps.auth_gc_every == 0):
            self.state.auth.gc_seen(self.deps.clock.now_ms() / 1000.0)
        if ev.kind == EventKind.RESOURCE_COMMITTED:
            for agent_id in self.deps.subscriptions.subscribers(ev.subject):
                self.deps.subscriptions.notify(ev.subject, ev)

    # ---- 签名 / token ---------------------------------------------
    def get_signer(self) -> Signer:
        """惰性构造 `Signer`：优先注入 secret，否则读 `HARNESS_AUTH_SECRET`。"""
        if self.state.auth is None:
            self.state.auth = Signer.from_env()
        return self.state.auth

    @property
    def auth(self) -> Signer:
        return self.get_signer()

    def effective_token(self, acb: ACB):
        """取该 ACB 的有效 token：`acb.capability` → `policy._tokens` → `default_token`。

        无 token → `None`（`_gateway_check` 据此 deny-by-default）。
        """
        token = getattr(acb, "capability", None)
        if token is not None:
            return token
        token = self.deps.policy.token(acb.id)
        if token is not None:
            return token
        return self.deps.policy.default_token

    # ---- 幂等键（文档 §5.8 / I7）-----------------------------------
    def idem_key(self, acb: ACB, action, action_index: int) -> str:
        """`hash(run_id, logical_subgoal_id, hash(tool, hash(args)))` —— 不含 task_id。"""
        payload = (f"{acb.run_id}|{acb.logical_subgoal_id}|{action.tool}|{action.args}")
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
