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

from .auth import MissingSecret, Signer
from .budget import BudgetManager, InvariantError, Reservation
from .checkpoint import (
    CheckpointCorrupt,
    CheckpointStore,
    EffectEntry,
    EffectJournal,
    FileCheckpointStore,
)
from .clock import Clock, ManualClock, SystemClock
from .compensation import CompensationJournal, CompEntry
from .context import ContextAssembler
from .event_log import EventLog, WalEventLog
from .facade import Allow, Deny, Receipt, RscSession, Token, Wait
from .harness import EpochStore, Harness, ReceiptScanFailed, create_production_harness
from .intent_registry import IntentRegistry
from .orchestration import (
    HITL,
    Dispatcher,
    Planner,
    ReconcileLoop,
    RunCoordinator,
    Scheduler,
    SlotTracker,
)
from .policy import (
    CapabilityAttenuationError,
    CapabilityToken,
    PolicyEngine,
    Scope,
    attenuate_token,
)
from .rebase import DELETE, MISSING, Conflict, MergePolicy, MergeResult, merge3
from .resolver import ConflictResolver, LLMResolver, ScriptedResolver
from .runtime import AgentRuntime
from .subscription import SubscriptionRegistry
from .types import (
    ACB,
    RUN_TERMINAL,
    TASK_TERMINAL,
    TRANSITIONS,
    Action,
    Decision,
    Event,
    EventKind,
    Lease,
    Mode,
    ReceiptConflict,
    Resource,
    Result,
    ResultEnvelope,
    RunState,
    Status,
    TaskState,
    Write,
    advance,
)
from .workspace import WorkspaceManager, WorkspaceVersionStore

__all__ = [
    "ACB",
    "DELETE",
    "HITL",
    "MISSING",
    "RUN_TERMINAL",
    "TASK_TERMINAL",
    "TRANSITIONS",
    "Action",
    "AgentRuntime",
    "Allow",
    "BudgetManager",
    "CapabilityAttenuationError",
    "CapabilityToken",
    "CheckpointCorrupt",
    "CheckpointStore",
    "Clock",
    "CompEntry",
    "CompensationJournal",
    "Conflict",
    "ConflictResolver",
    "ContextAssembler",
    "Decision",
    "Deny",
    "Dispatcher",
    "EffectEntry",
    "EffectJournal",
    "EpochStore",
    "Event",
    "EventKind",
    "EventLog",
    "FileCheckpointStore",
    "Harness",
    "IntentRegistry",
    "InvariantError",
    "LLMResolver",
    "Lease",
    "ManualClock",
    "MergePolicy",
    "MergeResult",
    "MissingSecret",
    "Mode",
    "Planner",
    "PolicyEngine",
    "Receipt",
    "ReceiptConflict",
    "ReceiptScanFailed",
    "ReconcileLoop",
    "Reservation",
    "Resource",
    "Result",
    "ResultEnvelope",
    "RscSession",
    "RunCoordinator",
    "RunState",
    "Scheduler",
    "Scope",
    "ScriptedResolver",
    "Signer",
    "SlotTracker",
    "Status",
    "SubscriptionRegistry",
    "SystemClock",
    "TaskState",
    "Token",
    "Wait",
    "WalEventLog",
    "WorkspaceManager",
    "WorkspaceVersionStore",
    "Write",
    "advance",
    "attenuate_token",
    "create_production_harness",
    "merge3",
]
