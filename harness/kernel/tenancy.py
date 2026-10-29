"""TenancyService —— 多租户配额 + 计量 + 预算墓碑剪枝（S5-e 从 Harness 迁出，纯位移）。

搬出的是 S20 阶段三 / S19 K-88 的 3 个方法：

- `ensure_tenant`：幂等建 `tenant:<t>`（可选 `tenant:<t>:reserved` 份额池）；
  **先校验后建节点**（失败零残留），配置冲突显式抛错；
- `usage_report`：按租户归集用量 —— **从 EventLog 重算**（幂等，不用原子计数器）；
- `prune_budget`：剪掉「已终结 + 无人引用 + 超过保留期」的预算预留条目，
  `live_rids` 由活 ACB 精确算出（比「按序号水位」更强的引用栅栏）。

约定（与 `kernel/tasks.py` / `kernel/dispatch.py` 一致）：

- `TenancyDeps` 只放**稳定对象引用**（`budget` / `clock` / `event_log`）；
  可热改配置一律在调用时读 `self.state`（`acbs` / `max_lease_ttl_s` /
  `claim_window_exceeded`）—— 冻结进 deps 会读到旧值；
- `budget._nodes` 是**组件私有缝**（`harness/protocols.py::BudgetManager` 已声明），
  不属于跨服务私有访问；
- Harness 上只保留**公共**委托（`ensure_tenant` / `usage_report` /
  `prune_budget`），无私有委托；
- 本模块构造**不产生任何事件**（`__init__` 只保存引用）；
- 事件种类/payload/顺序、锁序、构造顺序全部保持不变（纯位移）。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..types import TASK_TERMINAL


@dataclass(frozen=True)
class TenancyDeps:
    # ---- 稳定对象引用（构造期注入）----
    budget: Any
    clock: Any
    event_log: Any


class TenancyService:
    """租户配额 / 计量 / 预算剪枝唯一实现（Harness 仅保留同名公共委托）。"""

    def __init__(self, deps: TenancyDeps, state) -> None:
        self.deps = deps
        self.state = state

    def ensure_tenant(self, tenant: str, quota: int, *,
                      parent: str = "run", min_share: float = 0.0):
        """确保存在 `tenant:<t>` 预算节点（幂等）。挂在它下面的节点共享受配额约束。

        `min_share>0` 时再建 `tenant:<t>:reserved`（优先级份额池）。

        **失败零残留**：份额/配额先校验再建节点；配置冲突（同名节点已存在但
        quota/parent/share 不同）**显式抛错**，绝不静默返回旧节点。

        **诚实边界**：这只有**配额**隔离；同进程仍共享内存（安全隔离靠 egress/沙箱）。
        `plan["tenant"]` 已由 K-97 自动接到准入（见 `Dispatcher._resolve_parent_budget`）。
        """
        tid = str(tenant or "")
        if not tid:
            return None
        if ":" in tid:
            # `tenant:<t>` 的解析靠 `:` 分隔；租户名含 `:` 会让归属/计量错位。
            raise ValueError(f"非法 tenant 名：{tid!r}（不得含 ':'）")
        # 显式拒绝小数：`int()` 静默截断会让「6.9 变成 6」看起来成功。
        if int(quota) != quota:
            raise ValueError(f"quota={quota!r} 必须是整数")
        if min_share is not None and int(min_share) != min_share:
            raise ValueError(f"min_share={min_share!r} 必须是整数")
        quota = int(quota)
        share = int(min_share or 0)
        # ① 先校验（在建任何节点之前）—— 非法份额不得留下半成品。
        if quota <= 0:
            raise ValueError(f"quota={quota} 必须 > 0")
        if share < 0 or share > quota:
            raise ValueError(
                f"min_share={share} 必须满足 0 <= share <= quota={quota}")
        if parent != "run" and self.deps.budget._nodes.get(parent) is None:
            raise ValueError(f"父预算节点不存在：{parent!r}")
        # ② 建租户节点；同名但配置不同 → create_node 返回 None → 显式拒绝。
        node = self.deps.budget.create_node(f"tenant:{tid}", quota, parent, tenant=tid)
        if node is None:
            raise ValueError(
                f"tenant:{tid} 已存在但 quota/parent 不一致（不会静默沿用旧配置）")
        if share > 0:
            # 预留子节点：已知档位（premium/standard/developer）挂它，
            # 其他/未知档位回落共享池（超额借用）。
            rnode = self.deps.budget.create_node(f"tenant:{tid}:reserved", share,
                                                 f"tenant:{tid}", tenant=tid)
            if rnode is None:
                raise ValueError(
                    f"tenant:{tid}:reserved 已存在但 min_share 不一致（不会静默沿用旧配置）")
        return node

    def usage_report(self, *, tenant: str | None = None) -> dict:
        """按租户归集的用量（从 EventLog 重算，**幂等**）。

        为什么不用原子计数器：AWS Builders' Library 指出原子计数器**不幂等**
        （重试会重复计数）；EventLog 是唯一提交点（I1），从它重算天然幂等。

        归集口径（**明确写死，避免歧义**）：
        - `actions`：`RESOURCE_COMMITTED` 事件数（全局）
        - `runs`：出现过的 `run_id` 去重数
        - `by_tenant[t]`：**预算预留事件数**（`BUDGET_RESERVED`），按事件的
          `subject`（= 预算节点 id）映射到该节点上的 `tenant` 标签归集
        - `tenant=` 给定时只返回该租户的切片
        """
        node_tenant: dict[str, str] = {}
        for nid, node in (getattr(self.deps.budget, "_nodes", {}) or {}).items():
            t = getattr(node, "tenant", "") or ""
            if t:
                node_tenant[nid] = t

        actions = 0
        runs: set[str] = set()
        by_tenant: dict[str, int] = {}
        for ev in self.deps.event_log.all():
            kind = ev.kind.value if hasattr(ev.kind, "value") else str(ev.kind)
            if ev.run_id:
                runs.add(ev.run_id)
            if kind == "RESOURCE_COMMITTED":
                actions += 1
            elif kind == "BUDGET_RESERVED":
                t = node_tenant.get(str(ev.subject), "")
                if t:
                    by_tenant[t] = by_tenant.get(t, 0) + 1

        if tenant is not None:
            t = str(tenant)
            return {"tenant": t, "reservations": int(by_tenant.get(t, 0)),
                    "runs": len(runs), "actions": actions}
        return {"actions": actions, "runs": len(runs), "by_tenant": by_tenant}

    def prune_budget(self, *, max_age_s: float | None = None,
                     now_ms: int | None = None) -> int:
        """S19 K-88：剪掉「已终结 + 无人引用 + 超过保留期」的预算预留条目。

        `live_rids` 由**活 ACB**（非终态）的 `reservation_ids` 精确算出 ——
        这是比「按序号水位」更强的引用栅栏（单机 rid 是 uuid，没有单调 seq）。
        """
        now_s = (int(self.deps.clock.now_ms()) if now_ms is None
                 else int(now_ms)) / 1000.0
        live: set[str] = set()
        live_ids: set[str] = set()
        for acb in self.state.acbs.values():
            if acb.state in TASK_TERMINAL:
                continue
            live_ids.add(acb.id)
            live.update(acb.reservation_ids or ())
        # S21 K-100：墓碑的**年龄窗口**随命令下发（`max_lease_ttl_s`）——由
        # Raft 副本按「入墓已满该时长」判定能否忘记，避免复活仍活着的旧 worker。
        # 单机后端接收后忽略。拒绝淘汰的次数回读到 `claim_window_exceeded`。
        removed = self.deps.budget.prune(
            now_s, live, max_age_s=max_age_s, live_acb_ids=live_ids,
            max_lease_ttl_s=float(self.state.max_lease_ttl_s))
        self.state.claim_window_exceeded = int(
            getattr(self.deps.budget, "claim_window_exceeded", 0) or 0)
        return removed
