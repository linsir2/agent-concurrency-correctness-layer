# 多智能体 Harness：完整数据流与架构规范

**副题：一个能跑通、说得通的多智能体运行时框架（v0.3）**

| 项目 | 内容 |
|---|---|
| 版本 | **v0.3（harness 定稿版）** |
| 日期 | 2026-10-26 |
| 前置文档 | [四层读写分区与并发上下文设计](multi-agent-rw-partition-design.md) · [OS ↔ 多智能体双向映射](os-to-multiagent-mapping.md) · [派发与运行架构方案 v0.2](multi-agent-dispatch-architecture.md) |
| 本轮输入 | 3 个子智能体：harness 能力清单调研 / OS·工作流引擎·Actor 运行时对比 / 对抗式数据流审计 |
| 修订说明 | v0.3 修复了对抗式审计发现的 20 个问题（含 3 个致命），补全状态机，给出**可跑通的完整数据流**与 **5 个样例逐步验证** |
| 状态 | **历史专题稿（已被 supersede）**：机制以 [设计规范 v1.12](multi-agent-harness-design-final.md) 为准；本文的 `L0–L4` 分层与 final 的 `P0–P5` 平面 / `L1–L5` 交付归属**不是同一套编号**，勿混用 |

---

## 0. 定位与核心结论

### 0.1 这是什么

**这是一个多智能体 harness** —— 一个承载多子智能体「派发、运行、状态流转、并发控制、失败恢复」的运行时框架。它不是「agent 之间怎么连」的编排图库，而是「**agent 怎么写共享状态而不出错、出错后怎么不重复**」的运行时。

### 0.2 核心结论

1. **对标定位**：以 **Actor 运行时**对标「实体语义」，以 **持久化执行引擎**对标「持久执行语义」，以 **OS** 对标「机制词汇表」。真正的基准是前两者的混合体 —— **「以 durable execution 为底座的 agent runtime」**。

2. **我们的差异化不在「编排」**（LangGraph/CrewAI 很强），**而在三件业届集体缺位的事**：
   - 共享工作区的**并发正确性**（RSC + fencing + CAS + 语义 rebase）
   - **独立上下文的语义验证**（对抗 Byzantine 静默错误）
   - **成本的硬约束**（层级预算树 + 准入预留 + 部分结果）

3. **v0.3 的最大价值**：把「非确定性 + 策略强制 + Byzantine 故障」这三个前提差异，从「隐藏假设」变成「**显式契约**」。

4. **数据流可跑通性**：v0.2 只有正常路径能跑通（6.5/10）；v0.3 修复后，**5 个样例场景全部可跑通**（见 §4）。

---

## 1. Harness 能力清单对比

### 1.1 多智能体框架对比（15 维度）

> ✅ 完整 ｜🟡 部分 ｜❌ 缺失 ｜➕ 我们优于框架

| 维度 | 关键能力 | 我们 | 最强参照 |
|---|---|---|---|
| 调度编排 | DAG、动态 fan-out、map-reduce、分层拓扑、重规划 | ✅➕ | LangGraph / CrewAI / Temporal |
| 状态持久化 | checkpoint、WAL、重放、续跑/重跑、历史压缩 | ✅➕（有 checkpoint+WAL；缺 continue-as-new） | Temporal / Restate / DBOS |
| 取消背压 | 协作式取消、级联、背压、drain、外部控制 | 🟡（缺 drain / Admin 控制面） | LangGraph / Restate |
| 重试幂等 | 分类重试、退避、幂等键、UNKNOWN 对账 | ✅➕ | Temporal / Restate |
| 并发一致性 | 版本 CAS、fencing、租约、锁排序、advisory | ✅➕ **业界独有** | 数据库 MVCC（非 agent 框架） |
| 配额限流 | 层级预算树、多维上限、下游限流、准入、老化 | ✅➕ | cgroup / DBOS |
| 可观测 | span、事件日志、成本归因、timeline、metrics | 🟡（缺 timeline/metrics） | OTel GenAI |
| HITL | 中断、挂起保存、审批前释放锁、工具级审批 | ✅➕ | LangGraph / Temporal |
| 上下文记忆 | 会话持久化、分层记忆、巩固、物化视图、订阅 | ✅➕（物化视图/订阅独有；分层记忆缺失） | CrewAI / MemGPT |
| 工具能力边界 | 白名单、schema、能力令牌、沙箱、密钥代理、MCP | ✅➕（缺 MCP 对接） | MCP / OpenAI |
| 结果契约验证 | 输出 schema、覆盖度、独立验证、矛盾检测 | ✅➕ **业界独有** | MAST 论文 |
| 失败语义 | 瞬态/永久三分、毒任务熔断、无进展检测、部分结果 | ✅➕ | Temporal / DBOS |
| 嵌套递归 | 子图、深度/扇出/总数上限、fork 子 agent | ✅（缺 fork） | LangGraph / Claude Code |
| 事件订阅 | 发布订阅、全局 seq、per-resource 投递、幂等投递 | ✅➕ | Temporal / AutoGen |
| 机制策略分离 | 机制/策略分离、策略可插拔、定义版本化、多档位 | ✅➕（缺定义版本化） | OS / Temporal |

### 1.2 OS / 工作流引擎 / Actor 运行时对比（14 项通用能力）

| 能力 | OS 内核 | 工作流引擎 | Actor 运行时 | 我们 |
|---|---|---|---|---|
| ① 身份与句柄 | PID/`task_struct` | Run ID | ActorRef | ✅ |
| ② 生命周期状态机 | 进程状态+wait | Workflow 状态 | preStart/postStop | ✅ |
| ③ 派生/复制 | fork/clone | child workflow | actorOf | ✅ |
| ④ 调度与准入 | CFS/EEVDF | task queue+poll | dispatcher/turn | ✅ |
| ⑤ 资源配额记账 | cgroup v2+OOM | rate limit/retry | mailbox 背压 | 🟡 |
| ⑥ 隔离与权限 | namespace+ring+seccomp | tenant+沙箱 | 进程隔离 | 🟡 |
| ⑦ 通信与共享状态 | IPC/VFS | Signal/Update/Query | mailbox | 🟡（未分离三类） |
| ⑧ 并发控制与裁决 | mutex/RCU/CAS | 单写者 workflow+saga | 单线程 turn | ✅ |
| ⑨ 持久化与重放 | journal/WAL/fsync | event history | persistence/lineage | 🟡（非确定，只能近似） |
| ⑩ 时间/定时器 | clock/timer | durable timer | grain timer | 🟡（无一等 timer） |
| ⑪ 故障检测恢复 | signal/core dump | 重放+补偿 | supervisor 树 | 🟡（缺重启强度窗口） |
| ⑫ 取消与中断 | signal+EINTR | workflow cancellation | CancellationToken | ✅ |
| ⑬ 版本化 | ABI | workflow versioning | serialization ver | 🟡 |
| ⑭ 可观测内省 | /proc/ptrace/strace | visibility+history | lifecycle+dashboard | 🟡（无 procfs 式内省） |

### 1.3 差距与差异化

**我们缺的（按优先级）**：
- **P0 结构性**：分布式/跨机（单调度器是硬上限）；确定性原语（`ctx.uuid/random/time`）；分层记忆（短期/长期/实体）
- **P1 工程化外壳**：队列级并发/限速、优雅排空 drain、Admin 控制面（pause/resume/kill/purge）、定义版本化、通用 signal/query/update 通道、timeline/metrics、MCP 对接
- **P2 增强**：重放 jitter、fork 子 agent、eval harness、注入检测

**我们做得比框架好的（独有）**：RSC 提交协议、IntentRegistry+fencing、EffectJournal 的 UNKNOWN 对账、独立上下文 VerifierRunner、BudgetNode 树、ContextAssembler 物化视图、footprint 声明、parallel 谓词、advisory rebase、CapabilityToken、SecretBroker、ApplicabilityGate、NO_PROGRESS、DONE_PARTIAL。

**「看起来该有但其实不该有」**：确定性重放（LLM 随机，应改「近似重放+幂等键」）、per-agent actor+mailbox（IDE 子代理共享 FS，消息传递会丢共享语义）、硬件式抢占（无微秒时钟）、完整 2PL（临界区分钟级）、CRDT/OT 自动合并（token 成本+语义不可靠）、全量上下文广播（token 爆炸）。

---

## 2. 完整数据流规范 v0.3

### 2.1 分层与组件（P0–P5）

```
P5 接入面    Ingress（规范化/鉴权/用户幂等/流式输出/HITL 审批台）
P4 治理面    PolicyEngine · BudgetManager · Trace/Audit · SecretBroker · VerifierRunner
P3 控制面    ApplicabilityGate · Planner · PlanValidator · Dispatcher · Scheduler
             Supervisor · Replanner · Aggregator · RunCoordinator
P2 执行面    AgentRuntime · ContextAssembler · ModelGateway · ToolGateway · WorkspaceManager
P1 状态面    EventLog · PlanStore · ArtifactStore · WorkspaceVersionStore · CheckpointStore
             IntentRegistry · EffectJournal · MemoryStore · SubscriptionRegistry
P0 底座      沙箱/容器 · FS overlay · 网络出口 · 资源限制
```

**五条边界规则**：
- B1：只有 P3 能改 Plan/Task 状态。
- B2：只有 P2 的 ToolGateway / WorkspaceManager 能产生外部副作用。
- B3：只有 P1 的 EventLog 能分配全局 `seq`。
- B4：P4 的检查点在 gateway 强制执行。
- B5：P0 只提供隔离与资源。

### 2.2 端到端数据流（闭环）

```
[用户输入]
   │ ① Ingress：规范化/鉴权/用户幂等键
   ▼
[ApplicabilityGate] ──不适合──▶ [单 Agent 路径]
   │ ② 适合
   ▼
[Planner] ─Plan v1→ [PlanValidator] ─非法→ 修复/失败
   │ ③ 合法
   ▼
[Dispatcher] try_admit + 预算预留 + intent 注册 + 实例化
   │ ④ dataflow ready 队列
   ├──▶ [AgentRuntime A] ┐
   ├──▶ [AgentRuntime B] ├─ 并行（依赖计数归零即就绪）
   └──▶ [AgentRuntime C] ┘
   │ ⑤ 执行循环
   │   ├─ ContextAssembler 装配上下文（物化视图 + 增量）
   │   ├─ ModelGateway 决策（产出带 logical_action_id 的动作）
   │   ├─ ToolGateway 工具调用（EffectJournal + 幂等键）
   │   └─ WorkspaceManager stage→commit（RSC v2 两阶段）
   ▼
[EventLog] ◀─提交事务内追加事件─ [WorkspaceManager]
   │ ⑥ 按 seq 单调投递（Signal/Update/Query 三分离）
   ▼
[ContextAssembler] 标记 stale / 增量重放
   ▼
[Aggregator] ⑦ 按契约聚合/去重/消解冲突
   ▼
[VerifierRunner] ⑧ 独立上下文校验
   │ 失败
   ├──▶ [Replanner] → [PlanValidator] → [Dispatcher]（Plan vN+1）
   │ 通过
   ▼
[Renderer] ⑨ 流式输出
   │
   ▼
[最终输出]
```

### 2.3 派发数据流

**适用性门控**（是否值得多智能体）：独立性 → 上下文是否溢出 → 价值/成本 → 失败可否独立归因。

**footprint 声明**：

```
footprint = {
  committed  : Set<ResourceSpec>,   // 确定会写，参与准入
  contingent : Set<ResourceSpec>,   // 可能写，不预留；首次写触发 scope promotion
  min_budget : BudgetRequest        // v0.3 新增：准入用预算下界
}
```

**并行判定**：

```
parallel(a, b) :=
      not reaches(a, b) ∧ not reaches(b, a)
   && committed_writes(a) ∩ committed_writes(b) = ∅
   && logical_writes(a)  ∩ logical_writes(b)  = ∅
   && (writes(a) ∩ reads(b) = ∅ ∨ reads(b) 固定版本且允许陈旧)
   && (writes(b) ∩ reads(a) = ∅ ∨ reads(a) 固定版本且允许陈旧)
   && fits_budget(a) ∧ fits_budget(b)
   && no_exclusive_external(a, b) ∧ no_ordering_only(a, b)
```

> **澄清（修 F14）**：`parallel(a,b)` 判定的是「**可准入**」，不是「提交必然成功」。两个 agent 可能被同时准入写同一资源（reservation 不阻塞），但**提交层用 fencing + CAS 裁决**，只有一个成功，另一个走 rebase。这两层是分离的。

**调度模型（dataflow ready queue + 升级路径）**：

```
on_task_complete(t):
    for s in successors(t):
        s.remaining_deps -= 1
        if s.remaining_deps == 0: ready_queue.push(s)

loop:
    progress = false
    for t in ready_queue by (priority, aging):
        if try_admit(t): launch(t); progress = true
    if ready_queue 非空 and not progress:
        for t in ready_queue:
            t.aging += 1
            if t.aging > AGING_MAX:                 // v0.3 新增
                t.state = INADMISSIBLE
                escalate(t, reason = BUDGET_OR_CONFLICT)
    ready_queue = ready_queue.filter(state != INADMISSIBLE)
    if no running and no ready:
        finalize_or_replan()                        // 现在一定可达
```

**准入**：

```
try_admit(task) -> Admitted | Rejected(reason)

Admit :=
     budget.reserve(task.min_budget) 成功
  ∧  capability(task) ⊆ policy(run)
  ∧  ∀r ∈ committed_writes: intents.compatible(r, active_intents)
  ∧  slots.available() > 0
  ∧  depth ≤ D ∧ fanout ≤ F
  ∧  now + task.min_wall < task.deadline
```

### 2.4 运行数据流

```
async function agent_loop(acb):
    acb.state = RUNNING
    turn_seq = 0
    while not acb.done:
        guard(acb)                                  // 取消/期限/预算/步数/无进展
        ctx = context_view(acb)
        decision = modelgw.infer(acb, ctx)
        trace.span(acb, decision)
        turn_seq += 1

        if decision.kind == FINISH:
            result = build_result_envelope(acb, decision)
            artifact_store.put(result.artifacts)    // v0.3：落库
            if contract.validate(result) and verifier.pass(result):
                finalize(acb, DONE); return
            acb.state = REBASING
            replanner.submit(acb, verifier.diagnose(result))
            return

        staged_writes = []
        for i, action in enumerate(decision.actions):
            action.logical_id = (turn_seq, i)       // v0.3：稳定逻辑序号
            match action.kind:
                READ:
                    ctxmgr.read(acb, action.resource)        // 读即订阅 + 注册
                STAGE_WRITE:
                    staged_writes.add(action)
                    ws.stage(acb, action.resource, action.payload)
                TOOL_CALL:
                    if tool_manifest[action.tool].effectful:
                        staged_writes.add_if_resource(action)
                    effect = toolgw.invoke(acb, action)      // EffectJournal
                    ctxmgr.append_local(acb, effect)
                REQUEST_HITL:
                    release_all_locks(acb)                    // 挂起前释放锁
                    acb.state = WAITING_APPROVAL
                    checkpoint_store.save(acb)
                    hitl.raise(acb, action)
                    ok = wait_resume(acb, timeout = HITL_TIMEOUT)   // v0.3：超时
                    if not ok:
                        acb.state = FAILED; acb.failure = APPROVAL_TIMEOUT; return
                    acb.state = RUNNING

        if staged_writes not empty:
            commit_batch(acb, staged_writes)                 // RSC v2

        advance_baseline(acb)                                // v0.3：每 turn 推进
        checkpoint_store.save(acb)
```

### 2.5 提交协议：RSC v2（两阶段，修复 F2/F3）

```
function commit_batch(acb, writes):
    sort(writes, by = LOCK_ORDER)
    leases = []
    try:
        for w in writes:
            leases.add(intents.reserve(w.resource, WRITE,
                                       base_version = acb.read_set[w.resource],
                                       ttl = LEASE_MS))
        // 阶段 1：全部校验
        for w, lease in zip(writes, leases):
            st = ws.validate(acb, w, lease.token)
            if st == REJECT_STALE:       return REJECT_STALE(w)
            if st == CONFLICT:           return CONFLICT(w, current, diff)
            if st == REJECT_STALE_READ:  return REJECT_STALE_READ(w)
        // 阶段 2：全部提交
        for w, lease in zip(writes, leases):
            ws.apply(acb, w, lease.token)
        return OK
    finally:
        release_leases(leases)           // v0.3：无论成败都释放

function ws.validate(acb, w, token):
    if token < w.resource.last_fencing_token:        return REJECT_STALE
    if acb.base_version[w.resource] != w.resource.version:
        return CONFLICT(w.resource.version, compute_diff(...))
    // v0.3 新增：读集合校验（修 F2）
    for r in acb.subscriptions ∩ acb.read_set:
        if acb.read_set[r] != ws.version(r):         return REJECT_STALE_READ(r)
    return VALID

function ws.apply(acb, w, token):
    lock(w.resource)                                  // 短临界区，O(write)
    atomic_replace(w.resource, acb.staged[w.resource])
    w.resource.version += 1
    w.resource.last_fencing_token = token
    eventlog.append_in_txn(RESOURCE_COMMITTED, w.resource, w.resource.version, acb)
    unlock(w.resource)
```

**冲突处理**：
- `CONFLICT` / `REJECT_STALE_READ` → 返回给 agent 做**语义 rebase**（只修复依赖旧版本的步骤），有界重试。
- `REJECT_STALE` → 重读 + 重放事件 + 用新 token 重试。
- 超限 → `QUARANTINED` → `REPLANNING`。

### 2.6 上下文数据流

```
function context_view(acb):
    view = clone(acb.materialized)
    for event in replay(acb.event_log, from = acb.cursor):
        if event.resource in acb.subscriptions:
            apply_diff(view, event.diff)
    acb.cursor = acb.event_log.tail
    view.private = load_private(acb)
    return view

function advance_baseline(acb):          // 每 turn 后调用（修 F12）
    acb.materialized = context_view(acb)
    acb.baseline_version = acb.materialized.version
    acb.cursor = acb.event_log.tail
    if acb.materialized.size > MATERIALIZED_CAP:
        continue_as_new(acb)              // 开新 segment，旧 segment 归档
```

**规则**：
- Z1 只读区永不失效；Z2 可写区通过事件失效；Z3 私有区不参与共享。
- **读即订阅 + 同步注册**（修 F8）：`ctxmgr.read()` 内部把资源加入 `acb.subscriptions`，并调用 `subscription_registry.add(acb, resource)`。
- **近似重放契约**（修「确定性重放」误导）：重放**不保证字节等价**，只保证「已确认 effect 不重复 + 事件按 seq 有序 + 冲突可检测」。

### 2.7 事件与订阅（Signal / Update / Query 三分离，修 F9）

| 通道 | 语义 | 用途 | 是否产生事件 |
|---|---|---|---|
| **Signal** | 异步、无返回值、绝不失败 | HITL 通知、外部触发、取消 | 否 |
| **Update** | 同步、原子、有返回值 | 资源提交、状态变更 | 是（WorkspaceEvent） |
| **Query** | 只读、不产生事件、不触库 | 读状态、procfs 式内省 | 否 |

**事件分两类**（修 I8 表述）：
- `WorkspaceEvent`：对应一个已提交资源版本（`RESOURCE_COMMITTED`）。
- `EffectEvent`：对应一个已提交 effect（`TOOL_COMPLETED`）。

### 2.8 失败与恢复

**失败分类**：

| 故障 | 处理 | 终态 |
|---|---|---|
| 瞬态错误 | 指数退避 + jitter（v0.3 明确 jitter） | RETRYING |
| schema/契约错误 | 结构化错误回喂模型 | RETRYING/FAILED |
| 语义错误 | Verifier 最小修复 | REBASING |
| 写冲突 / 陈旧读 | 合并 / 语义 rebase | REBASING |
| 陈旧写 | 重读 + 新 token | REBASING |
| 预算/期限耗尽 | 优雅停止 + 部分结果 | DONE_PARTIAL |
| agent 崩溃 | 租约回收、generation+1、checkpoint 恢复 | RETRYING |
| 毒任务（速率窗口内超限） | 熔断隔离（v0.3：重启强度窗口） | QUARANTINED |
| 孤儿 | 级联取消、回收租约 | CANCELLED |
| 无进展 | 触发修复或失败 | FAILED |

**重启强度 + 重启预算**（v0.3 新增，借鉴 Erlang/OTP）：每个 task 定义 `{max_restarts, period}`；窗口内超限 → 不再重试，直接升级 `QUARANTINED`。

**恢复流程**：

```
① 定位最近成功检查点（含 workspace_commit + effect_journal_offset + staged）
② 按 resume_policy 决定：RERUN | RESUME | COMPENSATE
③ generation += 1（旧 fencing token 失效）
④ 重放事件日志（幂等，按 event.id 去重）
⑤ 扫描 EffectJournal：
     - CONFIRMED → 跳过
     - UNKNOWN   → 先按 idempotency_key 反查外部系统；查不到再补偿或升级人工
     - INTENT/EXECUTING（v0.3：未确认）→ 视为 UNKNOWN 处理
⑥ 已确认副作用用 saga 补偿（非回滚）
⑦ 恢复 ACB，重新入队
```

### 2.9 状态机（完整版，修 F5/F15）

```
PLANNED ─submit─▶ ADMITTED ─try_admit─▶ INSTANTIATING ─spawn─▶ READY ─schedule─▶ RUNNING
                                                                                   │
   ┌──────────────┬──────────────┬──────────────┬──────────────┬──────────────┐    │
   ▼              ▼              ▼              ▼              ▼              ▼    │
WAITING_TOOL  WAITING_APPROVAL  REBASING     DONE          RETRYING    CANCELLED  │
   │              │              │             │              │                    │
   └──────────────┴──────┬───────┘             │              ▼                    │
                         │                     │          FAILED ──▶ QUARANTINED    │
                         ▼                     ▼              │              │
                    RUNNING ◀──patch applied──┘         REPLANNING ◀──────┘
                         │                                     │
                         ▼                                     ▼
                    DONE_PARTIAL ◀──预算耗尽───────────── (新 Plan vN+1)
```

**关键补充**：
- `QUARANTINED → REPLANNING → Dispatcher`（不再是死状态）。
- `REBASING → RUNNING`（patch 应用后回归）。
- `DONE_PARTIAL`、`FAILED` 是正式状态节点。
- `WAITING_APPROVAL` 有超时 → `FAILED(APPROVAL_TIMEOUT)`；等待中可响应 Signal 取消。

---

## 3. 关键协议修复（v0.2 → v0.3）

| 编号 | 问题 | 修复 |
|---|---|---|
| **F1** 🔴 | `idem_key` 含 `generation`，恢复后重复副作用 | 移除 `generation`，改用稳定 `logical_action_id = (turn_seq, action_index)`；`generation` 仅用于 fencing |
| **F2** 🔴 | 读-写冲突无硬校验，陈旧读静默提交 | `ws.validate` 增加读集合校验 → `REJECT_STALE_READ` → rebase |
| **F3** 🔴 | `commit_batch` 非 all-or-nothing + 租约泄漏 + `continue` 丢写 | 两阶段提交 + `finally release_leases` + 移除 `continue` |
| **F4** 🔴 | 全部不可准入时调度 livelock | `aging > AGING_MAX` → `INADMISSIBLE` → 升级 |
| **F5** 🔴 | QUARANTINED 无出边、REBASING 无回归 | 补状态机出边与回归路径 |
| **F6** 🔴 | 嵌套派生无接口/无校验/无继承 | 新增 `spawn_child` 契约（见 §3.1） |
| F7 | Replanner 绕过 PlanValidator | `Replanner → PlanValidator → Dispatcher` |
| F8 | 读即订阅未注册 EventBus | 同步调用 `subscription_registry.add` |
| F9 | I8 把文件写事件当 effect | 拆分 `WorkspaceEvent` / `EffectEvent`，重写 I8 |
| F10 | `INTENT/EXECUTING` 恢复语义未定义 | 视为 UNKNOWN：先反查外部系统，再补偿 |
| F11 | 预算无 release、`remaining` 未定义 | 补 `release` 调用点 + `remaining = limits - reserved` |
| F12 | 数据膨胀（EventLog/Checkpoint/materialized） | `advance_baseline` 每 turn 调用 + `MATERIALIZED_CAP` + `continue_as_new` + 事件保留窗口 |
| F13 | `ArtifactStore.put` 无调用点 | `build_result_envelope` 后立即 `put` |
| F14 | §4.3 与 §10.2 矛盾 | 澄清「准入判定」与「提交裁决」分离 |
| F15 | `DONE_PARTIAL`/`FAILED` 不在状态机 | 补为正式节点 |
| F16 | 审计字段缺失 | `Event`/`ACB` 增加 `model_version`/`policy_version` |
| F17 | HITL 无超时/不可取消 | `HITL_TIMEOUT` + Signal 取消 |
| F18 | `min_budget` 无来源 | footprint 增加 `min_budget` |
| F19 | 验收「恰好一个提交成功」矛盾 | 改为「恰好一个提交**生效**，另一个被拒并 rebase」 |
| F20 | Checkpoint 无 staged | Checkpoint 增加 `staged` 字段 |

### 3.1 嵌套派生契约（修 F6）

```
AgentRuntime → Dispatcher : spawn_child(spec, footprint, budget_slice, capability_subset) -> ChildHandle

约束：
  - 子任务必须声明 footprint（committed/contingent + min_budget）
  - 子计划必须经 PlanValidator 校验
  - 子 agent 的 budget 从父节点切分（I5 守恒）
  - 子 agent 的 capability 是父的子集（I4 封闭）
  - depth ≤ D ∧ fanout ≤ F ∧ total_agents < A
  - 父可选择 await 或继续；取消沿父子树级联
```

### 3.2 确定性原语（修「确定性重放」误导）

```
ModelGateway 提供重放安全原语：
  ctx.uuid()      -> 由 (run_id, turn_seq, action_index) 派生，非随机
  ctx.random()    -> 由 (run_id, turn_seq, action_index) 派生的稳定伪随机
  ctx.time()      -> 返回逻辑时钟（来自 EventLog 的 seq），非 wall clock
```

---

## 4. 样例跑通（5 个场景，逐步状态追踪）

**记号**：`ACB.state`；资源 `fileX@vN`；fencing `tok`；`idem` 为幂等键。

### 场景 A：三 agent 并行改三个不同文件（正常路径）✅

| # | Actor | 状态（前） | 动作 | 产生数据 | 状态（后） | 检查 |
|---|---|---|---|---|---|---|
| A1 | Ingress | — | `submit(req, idem_key)` | RunHandle | Run: NEW | ✅ 幂等去重 |
| A2 | Planner | PLANNED | `plan()` | Plan v1（含 footprint+min_budget） | — | ✅ |
| A3 | PlanValidator | Plan v1 | `validate()` | Valid | Plan: VALID | ✅ I9 |
| A4 | Dispatcher | T1 PLANNED | `reserve(min_budget)` | BudgetNode.reserved+ | T1: ADMITTED | ✅ min_budget 有来源（F18） |
| A5 | Dispatcher | T1 ADMITTED | `reserve(fileA, W, v0)` | lease{fileA,T1,tok11,v0} | — | ✅ |
| A6 | Scheduler | 三就绪 | `launch` ×3 | AgentHandle A1/A2/A3 | READY→RUNNING | ✅ |
| A7 | A1 | RUNNING | `stage(fileA)` | staged{fileA} | — | ✅ |
| A8 | A1 | RUNNING | `commit(fileA, tok11)` | `fileA@v1`，tok11→last_tok，事件 | — | ✅ I1/I2/I3 |
| A9 | A2/A3 | RUNNING | `commit(fileB/fileC)` | `fileB@v1`,`fileC@v1`，两事件 | — | ✅ 无冲突 |
| A10 | A1/A2/A3 | RUNNING | `contract.validate`+`verifier.pass`+`put` | ResultEnvelope + artifact 落库 | →DONE | ✅ artifact 已落库（F13） |
| A11 | Scheduler | 无 running/ready | `finalize_or_replan` | 触发聚合 | — | ✅ |
| A12 | Aggregator→Verifier | — | `reduce`+`verify` | 覆盖度 OK | — | ✅ |

**结论**：✅ 完全跑通。

### 场景 B：两 agent 改同一文件（写-写冲突）✅

| # | Actor | 状态 | 动作 | 产生数据 | 检查 |
|---|---|---|---|---|---|
| B1 | Dispatcher | T1/T2 PLANNED | `launch` ×2（均 base=v1） | A、B 启动 | ✅ 准入不互斥（澄清 F14） |
| B2 | A | RUNNING | `reserve(fileX,W,v1)` | tok7 | ✅ |
| B3 | B | RUNNING | `reserve(fileX,W,v1)` | tok8 | ✅ reservation 允许双写 |
| B4 | A | RUNNING | `commit(fileX,tok7)` | validate 通过 → apply → `fileX@v2`，last_tok=7 | ✅ I1/I2/I3 |
| B5 | B | RUNNING | `commit(fileX,tok8)` | validate：base v1≠v2 → `CONFLICT(v2,diff)` | ✅ 拦住丢失更新 |
| B6 | B | RUNNING | 语义 rebase：refresh→重读 v2→重做受影响步骤 | 重新 stage | ✅ |
| B7 | B | RUNNING | `commit(rebased,tok8,base=v2)` | `fileX@v3`，last_tok=8 | ✅ fencing 单调 |
| B8 | A/B | RUNNING | finalize | ResultEnvelope ×2 | ✅ |

**不变量核对**：I2 ✅（B 被拒）、I3 ✅（7→8）、I1 ✅（单提交临界区）、I8 ✅（`WorkspaceEvent` 对应已提交版本）。

**结论**：✅ 跑通（提交层裁决，非准入层）。

### 场景 C：改接口 vs 读接口（读-写冲突 + 上下文失效）✅

**设定**：T1 写 `api.ts`；T2 读 `api.ts` 写 `impl/b.ts`。

**分支 C-1（允许陈旧读 → v0.3 拦截）**：

| # | Actor | 状态 | 动作 | 产生数据 | 检查 |
|---|---|---|---|---|---|
| C1 | Dispatcher | T1/T2 | `launch`（T2 声明固定读 v1） | A、B 启动 | ✅ |
| C2 | B | RUNNING | `read(api.ts)` | subscriptions+={api.ts}；`read_set{api.ts:v1}`；`subscription_registry.add` | ✅ 已注册（F8） |
| C3 | A | RUNNING | `commit(api.ts,tok21)` | `api.ts@v2`，事件 | ✅ |
| C4 | EventBus | — | `route(event)` | B 视图标 STALE | ✅ |
| C5 | B | RUNNING | `commit(b.ts,tok22)` | validate：`read_set[api.ts]=v1 ≠ v2` → **`REJECT_STALE_READ(api.ts)`** | ✅ **拦住陈旧读（F2 修复）** |
| C6 | B | RUNNING | refresh + replay + rebase | 基于 v2 重做 | ✅ |
| C7 | B | RUNNING | `commit(rebased,tok23,base=v2)` | `b.ts@v1` 提交 | ✅ 最终一致 |

**分支 C-2（串行）**：T2 依赖 T1 → 天然一致 ✅。

**结论**：✅ 跑通（v0.3 的读集合校验堵住了 v0.2 的敞开通道）。

### 场景 D：写入中崩溃（恢复路径）✅

| # | Actor | 状态 | 动作 | 产生数据 | 检查 |
|---|---|---|---|---|---|
| D1 | A | RUNNING | `reserve(fileX,W,v1)` | tok7 | ✅ |
| D2 | A | RUNNING | `stage(fileX)` | staged{fileX}（存入 Checkpoint） | ✅ staged 持久化（F20） |
| D3 | A | RUNNING | 崩溃 | — | — |
| D4 | Supervisor | 心跳超时 | `expire(leaseA)`；`bump fencing→8` | leaseA=EXPIRED | ✅ I3 |
| D5 | B | RUNNING | `reserve(fileX,W,v1)` | tok8 | ✅ |
| D6 | B | RUNNING | `commit(fileX,tok8)` | `fileX@v2`，last_tok=8 | ✅ |
| D7 | Supervisor→A | — | `resume(ref)`：generation=2 | ACB 恢复（含 staged） | ✅ |
| D8 | A | RUNNING | `commit(oldStaged,tok7)` | tok7 < 8 → `REJECT_STALE` | ✅ I3 |
| D9 | A | RUNNING | 重读 v2 → rebase → `commit(tok9)` | `fileX@v3` | ✅ |

**副作用恢复（修 F1/F10）**：
- 设 A 在崩溃前调用过非幂等工具 T，`logical_action_id = (1, 3)`。
- `idem_key = hash(run_id, task_id, (1,3))` —— **不含 generation**，恢复后不变。
- 若 T 停在 `INTENT`（未确认）→ 按 UNKNOWN 处理 → **先反查外部系统** → 未发生则重执行（幂等键保证不重复）；已发生则跳过。
- ✅ **I7 成立**：副作用有效一次。

**结论**：✅ 跑通（工作区 + 副作用两条恢复路径都可靠）。

### 场景 E：预算耗尽 / 全失败 / 重规划 ✅

**设定**：根预算 `cost=100`；T1..T5 各 `min_budget=30`。

| # | Actor | 状态 | 动作 | 产生数据 | 检查 |
|---|---|---|---|---|---|
| E1 | Dispatcher | T1..T5 PLANNED | `reserve` T1/T2/T3 成功，T4/T5 失败 | reserved=90，remaining=10 | ✅ remaining 有定义（F11） |
| E2 | T1/T2/T3 | RUNNING | 执行、`consume` | consumed 上升 | ✅ |
| E3 | T4/T5 | deferred | 每轮 `try_admit` 失败 | aging++ | ✅ |
| E4 | T4/T5 | deferred | `aging > AGING_MAX` | → `INADMISSIBLE` → `escalate` | ✅ **不再 livelock（F4 修复）** |
| E5 | T1（毒任务） | RUNNING | 连续失败 | RETRYING→FAILED(poison) | ✅ |
| E6 | T1 | FAILED | 重启强度窗口内超限 | → QUARANTINED | ✅ 有窗口（借鉴 OTP） |
| E7 | T1 | QUARANTINED | 触发重规划 | → REPLANNING → Plan v2 | ✅ **有出边（F5 修复）** |
| E8 | PlanValidator | Plan v2 | `validate` | Valid | ✅ 不再绕过校验（F7 修复） |
| E9 | T1 后继 | — | `remaining_deps` 随新 Plan 重置 | 入队 | ✅ 不再死锁 |
| E10 | 全部失败 | — | 无 DONE | → `DONE_PARTIAL` + 覆盖度标注 | ✅ 有终态（F15 修复） |
| E11 | 正常完成 | — | 部分任务结束 | `budget.release` 归还剩余 | ✅ 预算守恒（F11） |

**结论**：✅ 跑通（livelock、死锁、无界重规划三处全部修复）。

### 4.1 跑通汇总

| 场景 | v0.2 | v0.3 |
|---|---|---|
| A 三 agent 并行改不同文件 | ✅ | ✅ |
| B 两 agent 改同一文件 | ⚠️ | ✅ |
| C 改接口 vs 读接口 | ❌（陈旧读） | ✅ |
| D 写入中崩溃 | ⚠️（副作用重复） | ✅ |
| E 预算耗尽/全失败/重规划 | ❌ | ✅ |

---

## 5. 不变量（v0.3）

| 编号 | 不变量 |
|---|---|
| I1 | 单写者提交：同一 Z2 资源任意时刻至多一个提交在临界区内 |
| I2 | 无丢失更新：提交必须校验 `base_version` 与**读集合版本**，否则拒绝/合并 |
| I3 | fencing 单调：旧 generation/token 的写必须被拒绝 |
| I4 | 能力封闭：任何工具/资源/网络访问必须在 capability 内 |
| I5 | 预算守恒：`sum(children.reserved) ≤ parent.remaining`；结束必 `release` |
| I6 | 上下文因果一致：`materialized + ordered events` 是一个一致前缀 |
| I7 | effect 有效一次：**稳定幂等键** + effect journal 保证不重复副作用 |
| I8 | 事件原子：每个 `WorkspaceEvent` 对应一个已提交版本；每个 `EffectEvent` 对应一个已提交 effect |
| I9 | 计划有效：无环、每 task 有契约与 footprint、依赖满足才启动 |
| I10 | 隔离：私有工作区默认不可见 |
| I11 | 有界：深度/扇出/步数/token/时间/重规划/事件保留均有上限 |
| I12 | 终止/公平：每 task 在期限内到达终态或升级；ready task 不被无限饿死 |
| I13 | 可审计：输出可追溯到 artifact、effect、模型版本、策略版本 |
| I14 | 无进展终止：连续无进展必须触发修复/失败 |

---

## 6. 验收标准（可测试）

| 验收项 | 测试方法 | 通过标准 |
|---|---|---|
| 写-写冲突 | 100 次并发两 agent 写同一文件 | 恰好一个提交**生效**，另一个被拒并 rebase；无交错 |
| 丢失更新 | A 读 v1、暂停；B 写 v2；A 写 | A 被 `CONFLICT` 拒绝 |
| 陈旧读 | B 声明固定读 v1，A 写 v2，B 提交 | B 被 `REJECT_STALE_READ` 拒绝 |
| fencing | 暂停写者→租约过期→他人提交→恢复写者 | 恢复写者收到 `REJECT_STALE` |
| 副作用幂等 | 工具执行后、CONFIRMED 前崩溃 | 恢复后按稳定幂等键去重，无重复副作用 |
| 崩溃恢复 | 随机步 `kill -9` | 从 checkpoint 恢复；无重复副作用 |
| 预算 | 子任务请求超过父剩余 | 拒绝；父预算不被突破；结束归还 |
| 并行度 | N 个独立只读任务 | 墙钟 ≤ 1.5 × 最慢任务 |
| 能力隔离 | agent 尝试越界 | `DENIED_CAPABILITY`，无副作用 |
| 上下文可重建 | 重放事件流 | 物化视图与重放结果一致 |
| 语义验证 | 注入已知语义 bug | Verifier 拦截 |
| 取消传播 | 取消根任务 | 步边界内子任务全 CANCELLED；无孤儿 |
| 无进展 | 构造重复动作循环 | K 步内触发 `NO_PROGRESS` |
| livelock | 全部任务不可准入 | aging 超限后升级，不死循环 |
| 覆盖度 | 故意漏掉子目标 | Aggregator 报告 missing |

---

## 7. 缺口与路线图

| 优先级 | 缺口 | 建议 |
|---|---|---|
| P0 | 分布式/跨机 | 引入共识 + RPC 层（切换分布式系统框架） |
| P0 | 确定性原语 | 已补 `ctx.uuid/random/time`（§3.2），需在 ModelGateway 落地 |
| P0 | 分层记忆 | 补短期/长期/实体三层 + 巩固/消解（借鉴 CrewAI Cognitive Memory / MemGPT） |
| P1 | 队列级并发/限速 | 借鉴 DBOS/Temporal 队列配置 |
| P1 | 优雅排空 drain | 借鉴 LangGraph graceful shutdown |
| P1 | Admin 控制面 | pause/resume/kill/purge（借鉴 Restate Admin） |
| P1 | 定义版本化 | agent/workflow spec 版本路由（借鉴 Temporal Worker Versioning） |
| P1 | procfs 式内省 | 标准化 introspection 接口 |
| P2 | MCP 对接 | 作为 ToolGateway 的一种后端 |
| P2 | eval harness | 回归评测（借鉴 LangSmith） |
| P2 | fork 子 agent | 从当前对话分叉（借鉴 Claude Code） |

---

## 8. 参考资料

**多智能体框架**：LangGraph [Checkpointers](https://docs.langchain.com/oss/javascript/langgraph/checkpointers) · [Interrupts](https://docs.langchain.com/oss/javascript/langgraph/interrupts) · [Fault tolerance](https://docs.langchain.com/oss/javascript/langgraph/fault-tolerance)；CrewAI [Crews](https://docs.crewai.com/edge/en/concepts/crews) · [Cognitive Memory](https://blog.crewai.com/how-we-built-cognitive-memory-for-agentic-systems/)；OpenAI [Agents SDK](https://openai.github.io/openai-agents-python/)；AutoGen [Core Concepts](https://mintlify.wiki/microsoft/autogen/concepts)；Anthropic [Multiagent orchestration](https://platform.claude.com/docs/en/managed-agents/multiagent-orchestration)

**持久化执行**：Temporal [Workflow](https://docs.temporal.io/workflows) · [Versioning](https://docs.temporal.io/develop/typescript/workflows/versioning) · [Saga](https://docs.temporal.io/guides/saga-pattern)；Restate [Durable Execution](https://docs.restate.dev/concepts/durable_execution) · [Durable Agents](https://docs.restate.dev/ai/patterns/durable-agents)；DBOS [Workflows](https://docs.dbos.dev/python/tutorials/workflow-tutorial) · [Queues](https://docs.dbos.dev/typescript/tutorials/queue-tutorial)

**Actor 运行时**：Erlang/OTP [supervisor](https://www.erlang.org/docs/29/apps/stdlib/supervisor)（intensity/period）；Akka [Supervision](https://doc.akka.io/libraries/akka-core/current/general/supervision.html)；Orleans [Overview](https://learn.microsoft.com/en-sg/dotnet/orleans/overview)（virtual actor）；Ray [Actor Fault Tolerance](https://docs.ray.io/en/latest/ray-core/fault_tolerance/actors.html)

**OS**：kernel.org [cgroup v2](https://docs.kernel.org/admin-guide/cgroup-v2.html) · [RCU](https://docs.kernel.org/RCU/whatisRCU.html) · [EEVDF](https://docs.kernel.org/scheduler/sched-eevdf.html) · [/proc](https://docs.kernel.org/filesystems/proc.html)；man7 [signal(7)](https://man7.org/linux/man-pages/man7/signal.7.html)

**并发研究**：CoAgent/MTPO [arXiv:2606.15376](https://arxiv.org/abs/2606.15376)；MAST [arXiv:2503.13657](https://arxiv.org/abs/2503.13657)；Tracking State Footprints [arXiv:2610.03140](https://arxiv.org/pdf/2610.03140)；Agent–tool boundary [arXiv:2609.15397](https://arxiv.org/html/2609.15397)

---

*本规范为语言无关架构设计。v0.3 已通过 5 个样例场景的逐步状态追踪验证；所有标注为「修复」的条目对应 v0.2 评审发现。落地时需结合具体运行时补充实现细节。*
