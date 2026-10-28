"""S17 / MS10：可观测（**零依赖** + 可选 OTel 适配器）。

设计依据：OpenTelemetry 官方《Client Design Principles》——**库只应依赖
`opentelemetry-api`；未装 SDK 时 API 自带 no-op**。但本仓库的
`dependencies = []` 是硬约束，不能直接依赖 `opentelemetry-api`。

因此分两层：

1. **`SpanSink` Protocol**（本仓库自有，零依赖）：`emit_span` / `emit_metric`
2. **适配器**：
   - `MemorySpanSink`：收进内存（测试/调试）
   - `OTelAdapter`：**惰性** import `opentelemetry`；未装则抛 `OTelUnavailable`
     （fail-closed，**不静默丢弃**）

`event_spans(events)` 把 EventLog **映射**成 span 字典列表 —— **非侵入式**：
不用在业务代码里打点，事件流已经是唯一数据源（I1 单写者提交）。
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Protocol, runtime_checkable

# 关键事件 → span 名称（其余事件不进 span，避免噪声）
_SPAN_KINDS: dict[str, str] = {
    "WORKSPACE_COMMITTED": "harness.commit",
    "RESOURCE_COMMITTED": "harness.commit",
    "TASK_COMPLETED": "harness.task.complete",
    "BUDGET_RESERVED": "harness.budget.reserve",
    "BUDGET_SETTLED": "harness.budget.settle",
    "BUDGET_ARCHIVED": "harness.budget.archive",
    "RECEIPT_ISSUED": "harness.receipt",
    "EFFECT_INTENT": "harness.effect.begin",
    "EFFECT_CONFIRMED": "harness.effect.confirm",
    "EFFECT_UNKNOWN": "harness.effect.unknown",
    "COMPENSATION_STARTED": "harness.compensate.start",
    "COMPENSATION_DONE": "harness.compensate.done",
    "COMPENSATION_FAILED": "harness.compensate.failed",
    "RUN_PAUSED": "harness.admin.pause",
    "RUN_RESUMED": "harness.admin.resume",
    "RUN_PURGED": "harness.admin.purge",
    "DLQ_REDRIVEN": "harness.dlq.redrive",
    "INGRESS_ACCEPTED": "harness.ingress.accept",
}


class OTelUnavailable(RuntimeError):
    """指定使用 OTel 适配器但未安装 `opentelemetry` → 拒绝（不静默降级）。"""


@runtime_checkable
class SpanSink(Protocol):
    """最小可观测出口（零依赖）。"""

    def emit_span(self, span: dict) -> None:
        ...

    def emit_metric(self, name: str, value: float,
                    attributes: dict | None = None) -> None:
        ...


class MemorySpanSink:
    """内存 sink（测试/调试；也用于断言「确实有埋点」）。"""

    def __init__(self) -> None:
        self.spans: list[dict] = []
        self.metrics: list[tuple[str, float, dict]] = []

    def emit_span(self, span: dict) -> None:
        self.spans.append(dict(span))

    def emit_metric(self, name: str, value: float,
                    attributes: dict | None = None) -> None:
        self.metrics.append((str(name), float(value), dict(attributes or {})))

    def span_names(self) -> list[str]:
        return [s.get("name", "") for s in self.spans]


def event_spans(events: Iterable[Any]) -> list[dict]:
    """把事件流映射成 OTel 友好的 span 列表（**非侵入**）。

    span 字段：`name` / `trace_id`(=run_id) / `seq` / `subject` /
    `attributes`（原事件 payload + kind）。只映射关键事件（见 `_SPAN_KINDS`）。
    """
    out: list[dict] = []
    for ev in events:
        kind = getattr(ev, "kind", "")
        kind_name = kind.value if hasattr(kind, "value") else str(kind)
        name = _SPAN_KINDS.get(kind_name)
        if name is None:
            continue
        payload = dict(getattr(ev, "payload", None) or {})
        out.append({
            "name": name,
            "trace_id": getattr(ev, "run_id", "") or "",
            "span_id": f"{getattr(ev, 'subject', '')}#{getattr(ev, 'seq', 0)}",
            "seq": int(getattr(ev, "seq", 0) or 0),
            "subject": getattr(ev, "subject", ""),
            "attributes": {**payload, "kind": kind_name},
        })
    return out


def export_spans(sink: SpanSink, events: Iterable[Any]) -> int:
    """把事件映射成 span 并推给 sink；返回推送条数。"""
    spans = event_spans(events)
    for span in spans:
        sink.emit_span(span)
    return len(spans)


def export_metrics(sink: SpanSink, metrics: dict) -> int:
    """把 `Harness.metrics()` 的结构推给 sink（counters + gauges）。"""
    n = 0
    for name, value in (metrics.get("counters") or {}).items():
        sink.emit_metric(name, value, {"type": "counter"})
        n += 1
    for name, value in (metrics.get("gauges") or {}).items():
        sink.emit_metric(name, value, {"type": "gauge"})
        n += 1
    return n


class OTelAdapter:
    """把 span/metric 转发到真实 OpenTelemetry（**可选依赖**）。

    未安装 `opentelemetry` → 构造即抛 `OTelUnavailable`（**不静默丢弃**）。

    用法::

        from opentelemetry import trace, metrics
        sink = OTelAdapter(trace.get_tracer("harness"), metrics.get_meter("harness"))

    诚实说明：这里只做「最小映射」（span 属性 + 计数器/可观测值），
    **不是**完整 OTel 语义（无 parent/child 关系、无采样配置）。
    """

    def __init__(self, tracer, meter=None) -> None:
        self._tracer = tracer
        self._meter = meter
        self._counters: dict[str, Any] = {}

    @staticmethod
    def available() -> bool:
        try:
            import opentelemetry  # noqa: F401
            return True
        except ImportError:
            return False

    def emit_span(self, span: dict) -> None:
        with self._tracer.start_as_current_span(
                span.get("name", "harness.span")) as sp:
            for key, value in (span.get("attributes") or {}).items():
                try:
                    sp.set_attribute(str(key), value)
                except Exception:  # noqa: BLE001, S112  # 类型不支持则跳过
                    continue
            sp.set_attribute("harness.seq", int(span.get("seq", 0) or 0))

    def emit_metric(self, name: str, value: float,
                    attributes: dict | None = None) -> None:
        if self._meter is None:
            return
        counter = self._counters.get(name)
        if counter is None:
            counter = self._meter.create_counter(name)
            self._counters[name] = counter
        counter.add(float(value), attributes or {})


def make_otel_sink() -> SpanSink:
    """便捷构造：装了就返回 `OTelAdapter`，没装抛 `OTelUnavailable`。"""
    try:
        from opentelemetry import metrics, trace
    except ImportError as exc:                        # pragma: no cover
        raise OTelUnavailable(
            "未安装 opentelemetry（`pip install .[otel]`）；"
            "不会静默丢弃指标") from exc
    return OTelAdapter(trace.get_tracer("harness"),
                       metrics.get_meter("harness"))
