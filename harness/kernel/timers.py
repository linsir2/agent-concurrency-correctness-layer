"""TimerService —— 定义版本钉住 + DurableTimer 便捷入口（S5-e 从 Harness 迁出，纯位移）。

搬出的是 S24-11 的 3 个方法：

- `check_definition`：ACB/plan 钉住的定义引用校验（空串 = 通过，fail-closed）；
  非法 ref 一律 `DEFINITION_MALFORMED`，注册表缺失一律 `DEFINITION_REGISTRY_MISSING`
  —— 绝不「无注册表就当通过」；
- `set_timer` / `cancel_timer`：`DurableTimer` 的门面入口（未启用 → 显式
  `RuntimeError`，不静默 no-op）。

约定（与 `kernel/tasks.py` / `kernel/recovery.py` 一致）：

- **刻意没有 `*Deps`**：本服务只碰两个**可在构造后整体替换**的组件引用
  （`definitions` / `timers`），按纪律一律在调用时读 `self.state` —— 
  `tests/test_s24_definitions.py` 会 `h.definitions = replacement`，把这两个
  引用冻进 `@dataclass(frozen=True)` 的 deps 会读到旧值（静默失效比报错更糟）；
- Harness 上只保留**公共**委托（`check_definition` / `set_timer` /
  `cancel_timer`），无私有委托（架构守卫生效）；
- 本模块构造**不产生任何事件**（`__init__` 只保存 state 引用）；
- 事件种类/payload/顺序、锁序、构造顺序全部保持不变（纯位移）。
"""

from __future__ import annotations


class TimerService:
    """定义校验 / timer 门面唯一实现（Harness 仅保留同名公共委托）。"""

    def __init__(self, state) -> None:
        self.state = state

    def check_definition(self, ref) -> str:
        """校验 ACB/plan 钉住的定义引用；空串 = 通过（fail-closed）。"""
        if not ref:
            return ""
        definitions = self.state.definitions
        if definitions is None:
            return "DEFINITION_REGISTRY_MISSING"
        try:
            return str(definitions.check(ref))
        except Exception:  # noqa: BLE001  # 非法 ref → 拒绝
            return "DEFINITION_MALFORMED"

    def set_timer(self, timer_id: str, fire_at_ms: int, **kwargs):
        timers = self.state.timers
        if timers is None:
            raise RuntimeError("未启用 timers（Harness(timers=...) / durable_timers=True）")
        return timers.schedule(timer_id, fire_at_ms, **kwargs)

    def cancel_timer(self, timer_id: str, *, reason: str = "") -> bool:
        timers = self.state.timers
        if timers is None:
            raise RuntimeError("未启用 timers")
        return timers.cancel(timer_id, reason=reason)
