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
    Resource, Lease, Event, ACB, Write, Decision, Action,
    Status, Mode, EventKind, TaskState,
)
from .event_log import EventLog
from .intent_registry import IntentRegistry
from .workspace import WorkspaceManager, WorkspaceVersionStore
from .budget import BudgetManager
from .checkpoint import CheckpointStore, EffectJournal
from .subscription import SubscriptionRegistry
from .policy import CapabilityToken, PolicyEngine
from .context import ContextAssembler
from .runtime import AgentRuntime
from .orchestration import Planner, Dispatcher, Scheduler, RunCoordinator, ReconcileLoop, HITL
from .harness import Harness

__all__ = [
    "Resource", "Lease", "Event", "ACB", "Write", "Decision", "Action",
    "Status", "Mode", "EventKind", "TaskState",
    "EventLog", "IntentRegistry", "WorkspaceManager", "WorkspaceVersionStore",
    "BudgetManager", "CheckpointStore", "EffectJournal", "SubscriptionRegistry",
    "PolicyEngine", "CapabilityToken", "ContextAssembler", "AgentRuntime",
    "Planner", "Dispatcher", "Scheduler", "RunCoordinator", "ReconcileLoop", "HITL",
    "Harness",
]
