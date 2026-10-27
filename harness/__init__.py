"""多智能体并发正确性层（Agent Concurrency Correctness Layer）可执行实现。

对应设计文档 `/workspace/docs/multi-agent-harness-design-final.md` v1.11。

设计原则（见文档 §0.5 / §4.1）：
- L1 核心（本包实现）：IntentRegistry / WorkspaceManager / EventLog /
  SubscriptionRegistry / BudgetManager / PolicyEngine / WorkspaceVersionStore /
  ArtifactStore / CheckpointStore / EffectJournal
- L3 执行：AgentRuntime / ContextAssembler / ModelGateway / ToolGateway
- L4 编排（参考实现，可替换）：Planner / Dispatcher / Scheduler /
  RunCoordinator / ReconcileLoop / HITL

本包**只实现 L1 + L3 + 最小 L4**，用于验证设计文档的 17 个用例。
所有组件均为**接口 + 可替换实现**，无可替换的硬编码路径
（替换点见 `docs/milestones.md` §1.3「可扩展点」）。
"""

from .types import (
    Resource, Lease, Event, ACB, Write, Decision, Action, Result,
    ReceiptConflict,
    Status, Mode, EventKind, TaskState, RunState, TASK_TERMINAL, RUN_TERMINAL,
    TRANSITIONS, advance,
)
from .clock import Clock, SystemClock, ManualClock
from .event_log import EventLog, WalEventLog
from .intent_registry import IntentRegistry
from .workspace import WorkspaceManager, WorkspaceVersionStore
from .budget import BudgetManager, InvariantError, Reservation
from .checkpoint import (CheckpointCorrupt, CheckpointStore, EffectEntry,
                         EffectJournal, FileCheckpointStore)
from .compensation import CompensationJournal, CompEntry
from .subscription import SubscriptionRegistry
from .policy import CapabilityToken, PolicyEngine
from .auth import Signer, MissingSecret
from .context import ContextAssembler
from .resolver import ConflictResolver, LLMResolver, ScriptedResolver
from .runtime import AgentRuntime
from .orchestration import (Planner, Dispatcher, Scheduler, SlotTracker,
                            RunCoordinator, ReconcileLoop, HITL)
from .harness import Harness
from .rebase import (MISSING, DELETE, Conflict, MergePolicy, MergeResult, merge3)

__all__ = [
    "Resource", "Lease", "Event", "ACB", "Write", "Decision", "Action",
    "Result", "Reservation", "ReceiptConflict",
    "Status", "Mode", "EventKind", "TaskState", "RunState",
    "TASK_TERMINAL", "RUN_TERMINAL", "TRANSITIONS", "advance",
    "Clock", "SystemClock", "ManualClock",
    "EventLog", "WalEventLog", "IntentRegistry", "WorkspaceManager", "WorkspaceVersionStore",
    "BudgetManager", "CheckpointStore", "CheckpointCorrupt",
    "FileCheckpointStore", "EffectJournal", "EffectEntry",
    "CompensationJournal", "CompEntry",
    "SubscriptionRegistry",
    "PolicyEngine", "CapabilityToken", "Signer", "MissingSecret",
    "ContextAssembler", "AgentRuntime",
    "InvariantError",
    "ConflictResolver", "ScriptedResolver", "LLMResolver",
    "Planner", "Dispatcher", "Scheduler", "SlotTracker", "RunCoordinator",
    "ReconcileLoop", "HITL",
    "Harness",
    "MISSING", "DELETE", "Conflict", "MergePolicy", "MergeResult", "merge3",
]
