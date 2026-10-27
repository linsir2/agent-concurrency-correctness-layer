"""核心类型定义（对应文档 §4.5 数据结构定义）。

所有类型都是**纯数据**（dataclass），不绑定实现，便于替换存储后端。

v1.1（S0）：补齐 `Event` 的 4 个缺失字段（event_id/run_id/ts/schema_version）、
新增 `Result`（带载状态，供 CONFLICT 携带 diff）、`Reservation`、
`RunState`（表 B，与 TaskState 表 A 分离）、`TRANSITIONS`（状态机表驱动）。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


class Status(str, Enum):
    """文档 §4.4 错误码统一枚举（**封闭集合**，不得随意新增）。"""
    OK = "OK"
    VALID = "VALID"
    CONFLICT = "CONFLICT"
    REJECT_STALE = "REJECT_STALE"
    REJECT_STALE_READ = "REJECT_STALE_READ"
    REJECT_COMPENSATE = "REJECT_COMPENSATE"
    DENIED = "DENIED"
    DENIED_CAPABILITY = "DENIED_CAPABILITY"
    INVALID_INPUT = "INVALID_INPUT"
    NEEDS_APPROVAL = "NEEDS_APPROVAL"
    BUDGET_EXHAUSTED = "BUDGET_EXHAUSTED"
    DEADLINE_EXCEEDED = "DEADLINE_EXCEEDED"
    CANCELLED = "CANCELLED"
    UPSTREAM_FAILED = "UPSTREAM_FAILED"
    SEMANTIC_REJECTED = "SEMANTIC_REJECTED"
    COMMIT_UNRECOVERABLE = "COMMIT_UNRECOVERABLE"
    GUARD_VIOLATION = "GUARD_VIOLATION"
    APPROVAL_TIMEOUT = "APPROVAL_TIMEOUT"
    NEEDS_RECONCILIATION = "NEEDS_RECONCILIATION"


@dataclass(frozen=True, eq=False)
class Result:
    """带载状态：`CONFLICT` 必须带 diff、`REJECT_STALE_READ` 必须带 resource。

    兼容垫片：`result == Status.OK` 与 `result.value` 仍然成立，
    使签名迁移可以分批进行（S8 后移除垫片）。
    """

    status: Status
    detail: dict = field(default_factory=dict)

    @property
    def value(self) -> str:
        return self.status.value

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Status):
            return self.status == other
        if isinstance(other, Result):
            return self.status == other.status and self.detail == other.detail
        return NotImplemented

    def __ne__(self, other: object) -> bool:
        eq = self.__eq__(other)
        return NotImplemented if eq is NotImplemented else not eq

    def __hash__(self) -> int:
        return hash(self.status)


def ok(detail: Optional[dict] = None) -> Result:
    return Result(Status.OK, detail or {})


class Mode(str, Enum):
    """提交模式（文档 §5.4）。"""
    COMMIT = "COMMIT"
    COMPENSATE = "COMPENSATE"


class EventKind(str, Enum):
    """文档 §4.5 Event.type 枚举。"""
    WORKSPACE_COMMITTED = "WORKSPACE_COMMITTED"
    TOOL_COMPLETED = "TOOL_COMPLETED"
    EFFECT_INTENT = "EFFECT_INTENT"
    EFFECT_CONFIRMED = "EFFECT_CONFIRMED"
    EFFECT_UNKNOWN = "EFFECT_UNKNOWN"
    EFFECT_PROBED = "EFFECT_PROBED"
    COMPENSATING_BATCH = "COMPENSATING_BATCH"
    COMPENSATION_ENTRY = "COMPENSATION_ENTRY"
    COMPENSATION_STATE = "COMPENSATION_STATE"
    RESOURCE_COMMITTED = "RESOURCE_COMMITTED"
    COMPENSATION_STARTED = "COMPENSATION_STARTED"
    COMPENSATION_DONE = "COMPENSATION_DONE"
    COMPENSATION_FAILED = "COMPENSATION_FAILED"
    PIVOT = "PIVOT"
    PLAN_CREATED = "PLAN_CREATED"
    PLAN_PATCHED = "PLAN_PATCHED"
    BUDGET_RESERVED = "BUDGET_RESERVED"
    BUDGET_CONSUMED = "BUDGET_CONSUMED"
    APPROVAL_RAISED = "APPROVAL_RAISED"
    APPROVAL_RESOLVED = "APPROVAL_RESOLVED"
    TASK_COMPLETED = "TASK_COMPLETED"


class TaskState(str, Enum):
    """文档 §5.9 **表 A**：任务 / Agent 状态机。"""
    PLANNED = "PLANNED"
    ADMITTED = "ADMITTED"
    INSTANTIATING = "INSTANTIATING"
    READY = "READY"
    RUNNING = "RUNNING"
    WAITING_APPROVAL = "WAITING_APPROVAL"
    REBASING = "REBASING"
    RETRYING = "RETRYING"
    QUARANTINED = "QUARANTINED"
    INADMISSIBLE = "INADMISSIBLE"
    REPLANNING = "REPLANNING"
    DONE = "DONE"
    DONE_PARTIAL = "DONE_PARTIAL"   # 兼容保留：归属 RunState，S4 从 TaskState 移除
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    DEAD_LETTER = "DEAD_LETTER"


class RunState(str, Enum):
    """文档 §5.9 **表 B**：Run 状态机（阶段一只定义，MS4 启用）。"""
    NEW = "NEW"
    PLANNING = "PLANNING"
    RUNNING = "RUNNING"
    DONE = "DONE"
    DONE_PARTIAL = "DONE_PARTIAL"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


TASK_TERMINAL = frozenset({
    TaskState.DONE, TaskState.FAILED, TaskState.CANCELLED, TaskState.DEAD_LETTER,
})
RUN_TERMINAL = frozenset({
    RunState.DONE, RunState.DONE_PARTIAL, RunState.FAILED, RunState.CANCELLED,
})

# 兼容旧名（历史代码引用，S4 清理）
TERMINAL_STATES = TASK_TERMINAL


TRANSITIONS: dict[tuple[TaskState, str], TaskState] = {
    (TaskState.PLANNED, "try_admit"): TaskState.ADMITTED,
    (TaskState.PLANNED, "try_admit_rejected"): TaskState.INADMISSIBLE,
    (TaskState.ADMITTED, "instantiate"): TaskState.INSTANTIATING,
    (TaskState.ADMITTED, "instantiate_failed"): TaskState.FAILED,
    (TaskState.INSTANTIATING, "spawn"): TaskState.READY,
    (TaskState.INSTANTIATING, "spawn_failed"): TaskState.FAILED,
    (TaskState.READY, "schedule"): TaskState.RUNNING,
    (TaskState.RUNNING, "finish_ok"): TaskState.DONE,
    (TaskState.RUNNING, "request_approval"): TaskState.WAITING_APPROVAL,
    (TaskState.RUNNING, "verifier_failed"): TaskState.REBASING,
    (TaskState.RUNNING, "transient_error"): TaskState.RETRYING,
    (TaskState.RUNNING, "no_progress"): TaskState.FAILED,
    (TaskState.WAITING_APPROVAL, "resume"): TaskState.RUNNING,
    (TaskState.WAITING_APPROVAL, "timeout"): TaskState.FAILED,
    (TaskState.REBASING, "patch_applied"): TaskState.RUNNING,
    (TaskState.REBASING, "replan_required"): TaskState.REPLANNING,
    (TaskState.REBASING, "replan_budget_exhausted"): TaskState.QUARANTINED,
    (TaskState.RETRYING, "retry"): TaskState.RUNNING,
    (TaskState.RETRYING, "restart_window_exceeded"): TaskState.QUARANTINED,
    (TaskState.RETRYING, "schema_failed"): TaskState.FAILED,
    (TaskState.QUARANTINED, "escalate"): TaskState.REPLANNING,
    (TaskState.QUARANTINED, "replan_budget_exhausted"): TaskState.DEAD_LETTER,
    (TaskState.REPLANNING, "new_plan"): TaskState.PLANNED,
    (TaskState.INADMISSIBLE, "escalate"): TaskState.REPLANNING,
    (TaskState.INADMISSIBLE, "give_up"): TaskState.FAILED,
}

# 表 A 末行：除终态外，任何状态遇用户取消 → CANCELLED
CANCELLABLE = frozenset(set(TaskState) - set(TASK_TERMINAL))


def advance(acb: "ACB", event: str) -> Result:
    """状态机**唯一**写入口（D10）。非法转移返回 INVALID_INPUT（不新增错误码）。"""
    src = acb.state
    if event == "cancel":
        if src in CANCELLABLE:
            acb.state = TaskState.CANCELLED
            return Result(Status.OK, {"from": src.value, "to": TaskState.CANCELLED.value})
        return Result(Status.INVALID_INPUT, {"from": src.value, "event": event})
    dst = TRANSITIONS.get((src, event))
    if dst is None:
        return Result(Status.INVALID_INPUT, {"from": src.value, "event": event})
    acb.state = dst
    return Result(Status.OK, {"from": src.value, "to": dst.value})


@dataclass
class Resource:
    """共享态资源（Z2）。版本单调递增，禁止回绕（文档 §19 / §3.3）。"""
    name: str
    version: int = 0
    content: Any = None


@dataclass
class Lease:
    """租约（文档 §5.4 P0-3）：fencing 依据，按租约有效性判定。"""
    lease_id: str
    resource: str
    agent_id: str
    epoch: int
    ttl_ms: int = 5000
    valid: bool = True
    mode: str = "W"            # "R" | "W"（S2 起由 reserve 显式传入）
    expires_at: int = 0        # 注入时钟的毫秒时间戳（0 = 不按时间过期）


@dataclass
class Event:
    """文档 §4.5 Event。全局单调 seq（B3：只有 EventLog 能分配）。"""
    seq: int
    kind: EventKind
    subject: str
    payload: Any = None
    producer: str = ""
    caused_by: str = ""
    idempotency_key: str = ""
    event_id: str = ""          # ulid/uuid：恢复去重键
    run_id: str = ""
    ts: int = 0                 # epoch ms（来自注入时钟）
    schema_version: int = 1


@dataclass
class Write:
    """一次写操作。`applied_hash` 供补偿复验（文档 §5.4）。"""
    resource: str
    content: Any = None
    version_before: int = 0
    applied_hash: Optional[str] = None
    compensated: bool = False
    pivot: bool = False


@dataclass
class Action:
    """Agent 决策的一个动作（文档 §5.3）。"""
    kind: str                       # stage | commit | tool | request_hitl | finish
    resource: Optional[str] = None
    content: Any = None
    writes: list = field(default_factory=list)
    tool: Optional[str] = None
    args: Any = None
    cost: int = 1


@dataclass
class Decision:
    """模型决策（文档 §5.3）。`logical_action_id = (turn_seq, i)`。"""
    turn_seq: int
    actions: list = field(default_factory=list)

    def logical_action_id(self, i: int) -> tuple:
        return (self.turn_seq, i)


@dataclass
class ACB:
    """Agent Context Block（文档 §4.5）。"""
    id: str
    run_id: str
    task_id: str
    logical_subgoal_id: str = ""          # I7 幂等键基准（v1.10 修复 P0-2）
    generation: int = 1
    state: TaskState = TaskState.PLANNED
    spec: dict = field(default_factory=dict)
    capability: Any = None
    read_set: dict = field(default_factory=dict)
    base_version: dict = field(default_factory=dict)
    staged: dict = field(default_factory=dict)
    pre_image: dict = field(default_factory=dict)   # resource -> 前像 blob hash（供补偿）
    materialized: dict = field(default_factory=dict)
    cursor: int = 0
    subscriptions: set = field(default_factory=set)
    event_log: list = field(default_factory=list)
    budget_node: Any = None
    reservation_ids: list[str] = field(default_factory=list)
    needs_readmit: bool = False       # 恢复时无预算信息 → 由调用方重新 try_admit
    leases: list = field(default_factory=list)
    deadline: Optional[int] = None
    step: int = 0
    heartbeat_at: int = 0
    turn_seq: int = 0
    aging_deadline: Optional[int] = None
    failure: Optional[str] = None
    result_ref: Optional[str] = None
    hitl_result: Any = None
    retried: int = 0
    no_progress_steps: int = 0
    last_action_sig: Optional[str] = None
    retry_budget: int = 3
    rebase_attempts: int = 0          # 冲突 rebase 已尝试次数（跨调用累加，§5.5）
    trace_id: str = ""
    span_id: str = ""
    parent: "ACB | None" = None
    children: list = field(default_factory=list)
