"""核心类型定义（对应文档 §4.5 数据结构定义）。

所有类型都是**纯数据**（dataclass），不绑定实现，便于替换存储后端。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


class Status(str, Enum):
    """文档 §4.4 错误码统一枚举。"""
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


class Mode(str, Enum):
    """提交模式（文档 §5.4）。"""
    COMMIT = "COMMIT"
    COMPENSATE = "COMPENSATE"


class EventKind(str, Enum):
    """文档 §4.5 Event.type 枚举。"""
    WORKSPACE_COMMITTED = "WORKSPACE_COMMITTED"
    TOOL_COMPLETED = "TOOL_COMPLETED"
    EFFECT_CONFIRMED = "EFFECT_CONFIRMED"
    EFFECT_UNKNOWN = "EFFECT_UNKNOWN"
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
    """文档 §5.9 状态机。"""
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
    DONE_PARTIAL = "DONE_PARTIAL"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    DEAD_LETTER = "DEAD_LETTER"


TERMINAL_STATES = {TaskState.DONE, TaskState.FAILED, TaskState.CANCELLED, TaskState.DEAD_LETTER}


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
    ttl_ms: int
    valid: bool = True


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
    read_set: dict = field(default_factory=dict)
    base_version: dict = field(default_factory=dict)
    staged: dict = field(default_factory=dict)
    materialized: dict = field(default_factory=dict)
    cursor: int = 0
    subscriptions: set = field(default_factory=set)
    event_log: list = field(default_factory=list)
    budget_node: Any = None
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
    retry_budget: int = 3
    parent: "ACB | None" = None
    children: list = field(default_factory=list)
