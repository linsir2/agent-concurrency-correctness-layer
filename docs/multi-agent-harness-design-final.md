# 多智能体 Harness 设计规范（最终版）

**一个承载多子智能体「派发、运行、状态流转、并发控制、失败恢复」的运行时框架**

| 项目 | 内容 |
|---|---|
| 版本 | **v1.11（契约同步收口）** |
| 日期 | 2026-10-26 |
| 形态 | 语言无关的架构规范 + 伪代码 + 时序图 + 场景实测 + 无死角图集 |
| 范围 | IDE 内派生的 sub-agent，共享同一工作区文件系统；单调度器 |
| 说明 | v1.2–v1.10 累计；**v1.11 契约同步收口**：统一 `commit_batch`/`apply` 的 `mode`；补偿反写接入 `staged`；HITL resume 值入 ACB + 进入 ctx；`BUDGET_EXHAUSTED` 统一；§20 图集与 §4.1/§4.2 对齐；分布式文档删「去协调者 2PC」 |
| 定位配套 | [定位与路线建议 v2.0](positioning-and-roadmap.md)（能力归属见其附录 B） |
| 对照文档 | [计网 ↔ 多智能体](networking-to-multiagent-mapping.md) · [OS/DB ↔ 多智能体](os-to-multiagent-mapping.md) · [分布式 ↔ 多智能体](distributed-to-multiagent-mapping.md) |
| 用例文档 | [用例集（4 真实 + 13 对抗）](use-cases-and-adversarial-tests.md) |

---

## 目录

- [0. 定位与核心结论](#0-定位与核心结论)
- [1. 背景：多智能体系统 = 操作系统问题的跨域重现](#1-背景)
- [2. 设计目标、约束与原则](#2-设计目标约束与原则)
- [3. 资源分区模型（Z0–Z3）](#3-资源分区模型)
- [4. 架构分层与组件（P0–P5）](#4-架构分层与组件)
- [5. 完整数据流（核心）](#5-完整数据流)
- [6. 上下文与记忆](#6-上下文与记忆)
- [7. 并发与一致性](#7-并发与一致性)
- [8. 结果聚合与验证](#8-结果聚合与验证)
- [9. 治理（预算 / 可观测 / HITL）](#9-治理)
- [10. 样例跑通（8 场景逐步实测）](#10-样例跑通)
- [11. 不变量](#11-不变量)
- [12. 验收标准](#12-验收标准)
- [13. Harness 能力清单与 OS 对比](#13-harness-能力清单与-os-对比)
- [14. 缺口与路线图](#14-缺口与路线图)
- [15. 术语表](#15-术语表)
- [16. 参考资料](#16-参考资料)
- [17. 跨域对照：设计决策的外部依据](#17-跨域对照设计决策的外部依据)
- [18. L2 规范四件套（阶段二交付物）](#18-l2-规范四件套阶段二交付物)
- [19. 分布式对照：设计决策的外部依据](#19-分布式对照设计决策的外部依据v18-新增)
- [20. 无死角图集](#20-无死角图集v18-新增)
- [附录：修订记录](#附录修订记录)

---

## 0. 定位与核心结论

### 0.1 这是什么

**这是一个「并发正确性层」**（Agent Concurrency Correctness Layer）—— 不是「agent 之间怎么连」的编排图库，而是「**多个 agent 写共享状态时怎么不出错、出错后怎么不重复**」的运行时。

> **承诺边界（v1.7 明确，对齐 §9.4）**：我们能承诺的是 **机械一致性**（无丢失更新、无重复副作用、可检测的语义冲突）；**不能承诺语义正确** —— 后者依赖 VerifierRunner，而「边界强度 = 校验器强度」。「像数据库事务一样可靠」特指**机械一致性**，不含语义正确性。

> **隔离模型（v1.7 明确，P0-6）**：**Z3 = per-agent overlay/worktree**（每个 agent 在独立 worktree 中工作，写本地文件）；**Z2 = 提交层仲裁的共享资源**（跨 agent 共享的接口契约、配置、类型）。RSC 的冲突模型基于此定义。

> 定位与路线见 [定位与路线建议 v2.0](positioning-and-roadmap.md)。本文描述**目标态**（含编排层 L4 参考实现）；阶段一 MVP 见定位文档 §6.4。

### 0.2 核心结论

1. **对标定位**：以 **Actor 运行时**对标实体语义，以**持久化执行引擎**（Temporal/Restate/DBOS）对标持久执行语义，以 **OS** 对标机制词汇表。真正的基准是前两者的混合体 —— **「以 durable execution 为底座的 agent runtime」**。

2. **差异化不在「编排」**（LangGraph/CrewAI 很强），**而在三件业界集体缺位的事**：
   - 共享工作区的**并发正确性**（RSC + fencing + CAS + 语义 rebase）
   - **独立上下文的语义验证**（对抗 Byzantine 静默错误）
   - **成本的硬约束**（层级预算树 + 准入预留 + 部分结果）

3. **最大价值**：把「非确定性 + 策略强制 + Byzantine 故障」这三个前提差异，从隐藏假设变成**显式契约**。

4. **数据流可跑通性**：本文 §10 用 8 个样例场景逐步追踪状态流转，证明数据流**闭环、说得通**。

### 0.3 一句话总览数据流

```
用户输入 → 接入 → 适用性门控 → 规划 → 校验 → 准入派发 → 并行执行
        → 提交(RSC) → 事件 → 上下文失效/重放 → 聚合 → 验证 → 输出
```

### 0.4 目标用户与场景（v1.2 新增）

**目标用户**（详见定位文档 §1.3）：

| 优先级 | 用户 |
|---|---|
| P0 | agent-native VCS（Oak / GitButler / Atlas）—— 互补品 |
| P0 | MCP 生态（Claude / Cursor / VS Code）—— 分发 |
| P1 | 编排框架（LangGraph / CrewAI / OpenAI SDK / AG2）—— 被集成 |
| P1 | IDE 厂商（Cursor / Windsurf / JetBrains）—— 平台位 |

**切入场景**（详见定位文档 §1.4）：批量重构 / 大规模迁移（⭐⭐⭐⭐⭐）> 并行代码审查（⭐⭐⭐⭐）> 多 agent 测试生成（⭐⭐⭐）。

### 0.5 组件分层归属（v1.2 新增）

本文的组件按新定位分为五层（详见定位文档附录 B.1）：

| 层 | 含义 | 组件 |
|---|---|---|
| **L1 核心** | 我们交付 | WorkspaceManager / IntentRegistry / EffectJournal / EventLog / CheckpointStore / WorkspaceVersionStore / ArtifactStore / BudgetManager / VerifierRunner / ContextAssembler(基线) / ToolGateway(幂等) / ModelGateway(replay-safe) / SubscriptionRegistry(读集) / PolicyEngine(能力强制) / Dispatcher(准入) / Supervisor(租约) / RSC 协议 |
| **L2 规范** | 阶段二交付 | 资源模型 / 协调协议 / 收据格式 / conformance suite |
| **L3 托管运行时** | 阶段三交付 | 跨主机 fencing / 分布式租约 / 多租户 / Admin |
| **L4 编排适配** | **参考实现，可替换** | Planner / PlanValidator(DAG) / Dispatcher(实例化) / Scheduler / Replanner / Aggregator / RunCoordinator / Ingress / Renderer / AgentRuntime / ApplicabilityGate / spawn_child / HITL |
| **L5 删除/不做** | 红线 | Trace/Audit(OTel) / SecretBroker / MemoryStore |

> **重要**：L4 组件**不是产品**，是「让我们能独立跑通端到端」的参考实现；实际集成时由 LangGraph/CrewAI 等宿主框架提供。
>
> **编号说明**：本文的 **L1–L5 是「交付归属层」**（L1 核心 / L2 规范 / L3 托管运行时 / L4 编排适配 / L5 删除），与定位文档附录 A 的 **Agent OS 栈层 L0–L5**（硬件/内核/运行时/用户态/治理/互联）是**两套正交的编号**。P0–P5 平面（§4.1）与 L1–L5 归属也正交（例如 Dispatcher 是 P3 平面、L1 交付）。

---

## 1. 背景

### 1.1 核心判断：同态而非同构

**多智能体系统与操作系统不是「同构（isomorphism）」，而是「同态（homomorphism）」——保留了问题结构，丢掉了机制与语义。**

三个认知陷阱（务必警惕）：
1. 把「有对应词」当成「有对应机制」——有「锁」这个词，不代表有原子原语。
2. 把「OS 解决了」当成「照搬就能解决」——OS 的解法建立在硬件前提（MMU、特权级、时钟中断）上。
3. 把「资源同名」当成「资源同质」——CPU 时间可分割可抢占，一次 LLM 调用不可分割不可抢占。

### 1.2 双类比

**类比一：虚拟地址空间分区** → 资源读写分区（Z0–Z3）。
**类比二：进程 vs 线程** → agent 的独享与共享边界。

### 1.3 六个根本问题（分析主轴）

| 代号 | 根本问题 | 一句话 |
|---|---|---|
| **I1** | 边界与信任 | 不可信的外部输入必须穿过保护边界进入可信内核 |
| **I2** | 抽象与虚拟化 | 用统一接口遮蔽混乱的底层基质 |
| **I3** | 并发与稀缺 | 可运行单元多于可用容量，必须多路复用 |
| **I4** | 隔离与故障域 | 一个单元的失败不能拖垮整体 |
| **I5** | 权限与最小特权 | 危险操作必须被中介、被审计、可拒绝 |
| **I6** | 可观测与可恢复 | 必须能看见发生了什么，并能从中断点继续 |

### 1.4 类比映射表（OS ↔ 多智能体）

| OS 概念 | 多智能体对应物 | 同构核心 | 可迁移性 |
|---|---|---|---|
| 进程 / PCB | 子智能体 + ACB | 有状态可调度单元 + 元数据结构 | ✅ 直接借鉴 |
| 线程 / TCB | 并行子任务 / parallel tool calls | 容器内并发、共享父资源 | ⚠️ 需改造 |
| 进程调度器 | Orchestrator / Scheduler | 中央仲裁 + 队列 + 分派 | ✅ 直接借鉴 |
| RR/MLFQ/CFS | 并发/token 预算 + 优先级 | 有限资源下的公平与优先级 | ⚠️ 需改造 |
| 上下文切换 | 子 agent 切换 / 上下文重载 | 切换的状态保存/恢复成本 | ✅ 直接借鉴 |
| 虚拟内存 | context window + 记忆分层 + 按需加载 | 小快窗口 + 大慢后端 | ✅ 直接借鉴 |
| 页缓存 | prompt cache / 记忆缓存 | 本地副本避免重复后端访问 | ✅ 直接借鉴 |
| LRU/Clock | 记忆淘汰 / compaction | 容量有限时按局部性丢弃 | ✅ 直接借鉴 |
| inode/目录/VFS/fd | 资源元数据/目录/MCP/句柄 | 命名+元数据+抽象+句柄四层分离 | ✅ 直接借鉴 |
| 系统调用 | 工具调用 | 受控越界入口 + 校验 + 权限 | ✅ 直接借鉴 |
| 陷入内核/权限检查 | 沙箱 + HITL + guardrails | 越权边界的权限判定与隔离 | ✅ 直接借鉴 |
| 共享内存/管道/信号量 | 消息传递 / 共享 state / handoff | 单元间数据交换与协调 | ⚠️ 需改造 |
| 信号 | 事件 / 用户打断 | 异步注入、打断执行流 | ⚠️ 需改造 |
| 死锁/饥饿 | 编排死锁 / 任务饥饿 | 循环等待 + 调度不公 | ✅ 直接借鉴 |
| RCU/seqlock/无锁 | 免锁读 / 乐观并发 / 版本号重试 | 按读写比选同步策略 | ⚠️ 需改造 |
| 中断 | HITL / 用户中途干预 | 关键边界打断 + 现场保存恢复 | ✅ 直接借鉴 |
| cgroup / namespace | 并发/成本配额 + 工作区隔离 | 隔离 + 配额双维度 | ✅ 直接借鉴 |
| syscall 返回 | 子 agent 结果回传 | 完成→回传→恢复调用方 | ✅ 直接借鉴 |
| journaling / fsync | checkpoint / 持久化 / 幂等重放 | 预写日志 + 检查点 | ⚠️ 需改造 |
| 容器 / namespace | 多 agent 工作区隔离 | namespace+cgroup 组合的轻量隔离单元 | ✅ 直接借鉴 |

### 1.5 伪映射清单（诚实边界）

| 伪映射 | 本质差异 | 更贴切的类比 |
|---|---|---|
| 上下文窗口 = 虚拟内存 | 无 MMU/页表/缺页；换出是有损摘要且不可逆 | 软件管理的分级缓存 + 有界工作缓冲区 |
| 子智能体 = 进程 | 无独立地址空间、无硬件特权级 | 同进程内的协作式任务/协程 |
| `fork` = 复制智能体 | fork 是 O(页表) 惰性 COW；派生是 O(context) 急切复制 | `posix_spawn` + 手写 argv |
| 锁/信号量 = agent 互斥 | 临界区长达秒级；无原子 CAS | 乐观并发 + 版本号 + 语义 rebase |
| 子智能体崩溃 = 进程崩溃 | OS 是 fail-stop 可检测；agent 是 Byzantine 静默语义错误 | 输出校验/评测当一等公民 |
| 用户打断 = 硬件中断 | 只能协作点生效；恢复不透明 | 协作式取消 |
| 规划 = `exec` | exec 加载固定镜像；规划是运行时生成 | 编译器/解释器（IR + 动态调度） |
| 编排 = CPU 调度器 | CPU 调度是强制抢占；编排是语义决策 | DAG 调度器 / 工作流引擎 |
| 最终输出 = `write` | write 复制确定字节；输出是生成的非确定内容 | 编译器产物 + 渲染管线 |

### 1.6 没有良好对应物的 OS 概念

根本原因三类：**① 无硬件强制层；② 无确定性时钟；③ 状态不可无损序列化/回滚**。

硬件 TLB、特权环、精确时钟/实时调度、信号的可靠投递与屏蔽、字节级崩溃一致性、硬件 CAS、页表隔离、固定页大小 —— 均无良好对应物。

---

## 2. 设计目标、约束与原则

### 2.1 目标

- 给出**端到端闭环**的数据流：每一步都有明确的产生者、传递路径、消费者。
- 给出**派发**完整流程：适用性门控 → 规划 → 校验 → 准入 → 实例化 → 启动。
- 给出**运行**完整流程：执行循环、工具调用、**提交协议**、上下文更新、并发控制、失败恢复。
- 给出**协议级**保证：写何时生效、冲突谁裁决、崩溃后如何不重复副作用、权限如何不被绕过。
- 明确**组件边界**与**接口契约**。

### 2.2 硬约束

| 编号 | 约束 | 影响 |
|---|---|---|
| C1 | LLM 调用高延迟、按 token 计费、上下文有限且随长度退化；不能在 token 生成中途被抢占 | 锁不能跨 turn 持有；并发控制要短临界区 |
| C2 | agent 行为随机；失败以「静默语义错误」为主（Byzantine 式） | 输出校验必须是一等公民 |
| C3 | 工具副作用可能不可逆、非幂等 | 需要幂等键 + effect journal + 补偿 |
| C4 | 共享工作区写入立即可见，无原生事务与回滚 | 需要 overlay/stage/commit + Run 级基线 |
| C5 | 派生 subagent 是 O(context) 急切复制，按 token 计费 | 要适用性门控，避免无谓并行 |

### 2.3 设计原则

1. 分层抽象 + 统一接口（窄腰）
2. 机制与策略分离（**内核强制，模型提议**）
3. 快路径极便宜，慢路径才进「内核」
4. 乐观 + 兜底（预留不阻塞，提交时裁决）
5. 隔离与共享可调
6. 可观测性内建
7. 失败模式显式化
8. **临界区必须短**：LLM 推理期间绝不持有共享写锁

---

## 3. 资源分区模型

> 编号约定：资源分区用 **Z0–Z3**，架构分层用 **P0–P5**，避免冲突。

### 3.1 四层分区

| 代号 | 名称 | 类比 | 读写 | 并发机制 | 版本语义 | 失效方式 |
|---|---|---|---|---|---|---|
| **Z0** | 内核区 | 内核区 | 只读（运行期只追加） | 无锁读 / 单写者串行 | 只追加 | 不变 |
| **Z1** | 只读共享区 | 共享库映射 + text | 只读 | 无锁读 + 版本校验 | 单调递增 | **读不阻塞、不产生失效事件；但提交期仍做版本校验** |
| **Z2** | 可读写共享区 | data + heap | 读写 | 预留-暂存-提交 + fencing + CAS | 线性化 | 事件失效 |
| **Z3** | 私有工作区 | stack | 读写 | 无锁（overlay） | 本地单调 | agent 内失效 |

### 3.2 分区判定流程

```
资源是否属于调度规则/锁表/事件日志/元数据？
  ├─ 是 → Z0
  └─ 否 → 该资源在本次任务中是否会被写入？
        ├─ 否（只读）→ 它是否被多个 agent 共享？
        │     ├─ 是 → Z1
        │     └─ 否 → Z3
        └─ 是（会写）→ 它是否被多个 agent 共享（或被他人读取）？
              ├─ 是 → Z2（必须走 RSC）
              └─ 否 → Z3（单写者，无锁，不走 RSC）
```

> **关键澄清**：分区判定决定**并发机制**。只有 **Z2** 走 RSC（预留-暂存-提交 + fencing + CAS）；Z3 单写者无锁。判定依据是「**是否被多个 agent 共享或被他人读取**」，而非「是否会被写」。

### 3.3 各层内容

- **Z0 内核区**：调度规则、锁表、事件日志、任务依赖图、ACB 表、审计日志。
- **Z1 只读共享区**：编码规范、架构约定、公共知识、被依赖的稳定代码、第三方库、接口契约（未变动部分）。
- **Z2 可读写共享区**：**正在被修改的代码文件**（**共享态**）、共享数据结构、任务产出物、接口契约（变动部分）。
- **Z3 私有工作区**：**per-agent worktree/overlay**（每个 agent 在独立 worktree 中工作）、agent 轨迹、推理草稿、临时文件、本地缓存、中间结果。

> **v1.8 修复（P0-2/P0-3）：隔离模型与 RSC 的桥接**
>
> | 层 | 内容 | 写路径 |
> |---|---|---|
> | **Z3** | per-agent **worktree**（代码文件的本地副本） | agent 直接写本地 worktree（**无锁**） |
> | **Z2** | **共享态**（worktree 并入后的结果） | **`commit_batch` 走 RSC** |
>
> **关键桥接**：`ws.stage(resource, bytes)` = 写 agent 的 **Z3 worktree**；`ws.prepare/apply` = 把 worktree 内容**并入 Z2 共享态**的 **merge 点**。
> - 冲突检测：`prepare` 内 `base_version != version` → `CONFLICT`（即「worktree 基于旧共享态」）。
> - 冲突解决：`CONFLICT` 后先做 **3-way merge（§7.2.1）** 生成新的 worktree 内容，再重新 `commit`。
> - **「提交层 CAS」与「worktree 冲突在合并期」并不矛盾**：RSC 的 `apply` 就是 merge 动作，CAS 校验的是「worktree 的 base 是否仍等于共享态版本」。
> - **线性化点**：worktree 期间 agent 本地写对共享态不可见；**线性化只对 merge 后的共享态成立**（Z2 线性化，Z3 不保证）。

---

## 4. 架构分层与组件

### 4.1 分层（P0–P5）

```
P5 接入面    Ingress（规范化/鉴权/用户幂等/流式输出/HITL 审批台）· Renderer（输出渲染）
P4 治理面    PolicyEngine · BudgetManager · Trace/Audit · SecretBroker
P3 控制面    ApplicabilityGate · Planner · PlanValidator · Dispatcher · Scheduler
             Supervisor · Replanner · Aggregator · RunCoordinator
P2 执行面    AgentRuntime · ContextAssembler · ModelGateway · ToolGateway
             WorkspaceManager · VerifierRunner
P1 状态面    EventLog · PlanStore · ArtifactStore · WorkspaceVersionStore · CheckpointStore
             IntentRegistry · EffectJournal · MemoryStore · SubscriptionRegistry
P0 底座      沙箱/容器 · FS overlay · 网络出口 · 资源限制
```

### 4.2 组件与职责

| 组件 | 平面 | 职责 | 强制点 |
|---|---|---|---|
| **Ingress** | P5 | 输入规范化、鉴权、用户请求幂等、创建 Run、HITL 审批台 | 重复请求不重复建 Run |
| **Renderer** | P5 | 输出格式化、渠道渲染、流式输出 | — |
| **ApplicabilityGate** | P3 | 判断是否值得多智能体 | 不满足则走单 agent |
| **Planner** | P3 | 任务分解、产出 Plan（TaskGraph + 契约 + footprint） | 只提议，不派发 |
| **PlanValidator** | P3 | schema/DAG/footprint/预算/深度扇出校验 | 非法计划 fail-closed |
| **Dispatcher** | P3 | 准入、预算预留、intent 注册、实例化 | 非阻塞 `try_admit` |
| **Scheduler** | P3 | dataflow ready 队列、优先级、老化、并发槽 | 不允许无限 defer 饥饿 |
| **Supervisor** | P3 | 心跳、租约、超时、重试、取消、孤儿回收 | 终态与 fencing 推进 |
| **Replanner** | P3 | 基于诊断生成 Plan patch（vN+1） | 有界重规划预算 |
| **Aggregator** | P3 | 按契约聚合、覆盖度检查、矛盾检测 | 不做自由发挥式总结 |
| **RunCoordinator** | P3 | Run 状态机、阶段推进、终态收敛 | 唯一 Run 状态写入者 |
| **HITL** | P3 | 审批处理器：`raise`/`wait_resume`/`get_result`（挂起前 checkpoint + 释放写锁） | 关键操作前中断，可跨月恢复  ← v1.9 修复 |
| **ReconcileLoop** | P3 | level-triggered 对账循环（§5.2.1）：重建 ready 集、纠正 Task 状态 | 事件即真相（对账时以事件为准）  ← v1.9 修复 |
| **Command** | P3 | RunCoordinator 的命令接口（B1）：Dispatcher/Scheduler/Supervisor/Replanner 请求状态变更的唯一通道 | 单写者串行应用  ← v1.9 修复 |
| **AgentRuntime** | P2 | agent 执行循环与状态机 | 每步检查取消/预算/期限 |
| **ContextAssembler** | P2 | 稳定前缀 + 契约 + 基线 + 工作集 + 轨迹 | 前缀稳定命中缓存 |
| **ModelGateway** | P2 | 路由、重试、限流、结构化输出、缓存、计费、重放安全原语 | 所有模型调用唯一入口 |
| **ToolGateway** | P2 | schema、能力、幂等、沙箱、结果压缩 | 工具副作用唯一入口 |
| **WorkspaceManager** | P2 | overlay/stage/commit、版本、原子替换、WAL | 短提交锁 + fencing + CAS |
| **VerifierRunner** | P2 | 独立上下文执行契约/测试/语义校验 | 不共享生成器轨迹 |
| **IntentRegistry** | P1 | 写意图预留、租约、fencing token | 单调 token，拒绝旧持有者 |
| **EffectJournal** | P1 | effect 意图/确认/未知/补偿 | 非幂等工具不盲重试 |
| **EventLog** | P1 | 追加式全序事件、WAL、重放 | 单序列器 |
| **SubscriptionRegistry** | P1 | 订阅注册与路由 | 读即订阅 |
| **CheckpointStore** | P1 | 每步 ACB + workspace commit + effect offset + staged | 恢复不重复副作用 |
| **PlanStore / ArtifactStore / WorkspaceVersionStore / MemoryStore** | P1 | 计划 / 内容寻址产物 / 资源版本 / 记忆分层 的持久化 | — |
| **BudgetManager** | P4 | 配额树、reserve/consume/release | 父约束不可突破 |
| **PolicyEngine** | P4 | 能力定义、工具/路径/网络/数据分级；签发 `CapabilityToken`（工具/路径/网络三维 scope） | gateway 调用 |
| **Trace/Audit** | P4 | OTel span、成本归因、审计 | 每条工具调用一个 span |
| **SecretBroker** | P4 | 按能力短期发放密钥 | 密钥不进上下文 |

### 4.3 边界规则

- B1：**只有 `RunCoordinator` 能写 Task/Run 状态**（单一控制写入者）；Dispatcher/Scheduler/Supervisor/Replanner 通过**命令接口**（`Command`）请求，由 RunCoordinator 串行应用。
  - **Command 接口定义（v1.9 修复）**：`RunCoordinator.submit_command(cmd) -> Status`；`cmd = { kind: ADMIT|LAUNCH|ESCALATE|CANCEL|FINALIZE|..., subject, payload, idempotency_key }`。所有请求方**只提交命令**，不直接改状态；RunCoordinator 串行应用（保持单写者）。
- B2：只有 P2 的 ToolGateway / WorkspaceManager 能产生外部副作用。
- B3：只有 P1 的 EventLog 能分配全局 `seq`。
- B4：P4 的检查点在 gateway 强制执行，模型不可绕过。
- B5：P0 只提供隔离与资源，不提供语义。

### 4.4 接口契约（v1.6 补齐）

> 错误码统一枚举：`OK | VALID | CONFLICT | REJECT_STALE | REJECT_STALE_READ | REJECT_COMPENSATE | DENIED | DENIED_CAPABILITY | INVALID_INPUT | NEEDS_APPROVAL | BUDGET_EXHAUSTED | DEADLINE_EXCEEDED | CANCELLED | UPSTREAM_FAILED | SEMANTIC_REJECTED | COMMIT_UNRECOVERABLE | GUARD_VIOLATION | APPROVAL_TIMEOUT`

| 组件 | 接口 | 语义 |
|---|---|---|
| **IntentRegistry** | `reserve(resource, mode, base_version, ttl) -> Lease` | 非阻塞；返回租约（含 fencing 依据） |
| | `is_valid(lease) -> bool` | 租约是否仍有效（未过期/未撤销） |
| | `revoke(lease)` | Supervisor 回收；只作废该租约 |
| | `renew(lease, epoch)` | 续期；只对当前 epoch 生效（revoke 后不可复活） |
| | `compatible(resource, active_intents) -> bool` | 准入判定：只拦契约/不变量与读-写方向冲突 |
| **WorkspaceManager** | `stage(acb, resource, bytes)` | 私有暂存；同时把前像写入 `WorkspaceVersionStore` |
| | `prepare(acb, w, lease, mode) -> Status` | 校验（`mode=COMMIT` 校验 base_version + 读集合；`mode=COMPENSATE` 只校验租约） |
| | `apply(acb, w, lease, mode) -> Status` | 锁内复验 + 原子替换 + 版本推进 + 事件同事务（`mode=COMPENSATE` 跳过 base_version/读集合校验，改做内容复验） |
| **EventLog** | `append_in_txn(type, subject, payload) -> seq` | 与资源替换同事务；全局单调 |
| **BudgetManager** | `reserve(node, amount) -> Reservation\|Denied` | **单写者**，原子 |
| | `consume(node, amount) -> OK\|BUDGET_EXHAUSTED` | `consumed ≤ reserved`；**调用点（v1.9 修复）：§5.3 的 `TOOL_CALL` 分支在 `toolgw.invoke` 之后调用**，返回 `BUDGET_EXHAUSTED` 则该 task → `FAILED(BUDGET_EXHAUSTED)`（Run 级 finalize 后 → `DONE_PARTIAL`） |
| | `release(node)` | 只归还 `remaining − sum(children.reserved)` |
| **CheckpointStore** | `save(acb) -> CheckpointRef` | 含 `turn_seq` / `staged` / `workspace_commit` / `effect_journal_offset` / `cursor` / `aging_deadline` |
| | `load(ref) -> ACB` | 恢复 |
| **DurableTimer** | `schedule(key, fire_at, payload) -> TimerId` | 持久化（存 PlanStore）；重启后仍有效 |
| | `cancel(TimerId)` | 取消 |
| | `expired(key) -> bool` | 供 Supervisor / reconcile 判定（`aging` / `HITL_TIMEOUT` / 退避） |
| | `fire_due()` | 由 Supervisor 轮询触发到期回调（**不依赖进程存活**） |
| **ToolGateway** | `invoke(acb, call) -> ToolResult` | 幂等键 + EffectJournal；`effect.state` 见 §5.8 |
| **VerifierRunner** | `validate(result) -> bool` | 轻量契约校验（循环内） |
| | `verify(result) -> Verdict` | 完整 test/lint/语义 rubric（聚合后） |
| | `diagnose(result) -> Diagnosis` | 结构化诊断（供 Replanner） |
| **HITL** | `raise(acb, action) -> ApprovalId` | 挂起前 checkpoint + 释放写锁，创建审批单  ← v1.9 修复 |
| | `wait_resume(acb, timeout) -> bool` | 等待审批；可被 Signal 取消（可中断），超时返回 `false` |
| | `get_result(acb) -> ApprovalResult` | 取审批结果（写入 ACB） |
| **AgentRuntime** | `guard(acb) -> bool` | 返回 `false` 表示应终止；**组成（v1.9 修复）**：查取消（`RunCoordinator.is_cancelled`）/ 期限（`DurableTimer`）/ 预算（`BudgetManager`）/ 步数（`step` 上限）/ 无进展（I14）+ **续租 `intents.renew(acb.leases, acb.generation)`** |
| **WorkspaceManager** | `commit_batch(acb, writes, mode) -> Status` | **自由函数**（非方法），由 AgentRuntime 调用；`mode = COMMIT\|COMPENSATE`；RSC 两阶段 + 补偿（见 §5.4） |
| **EffectJournal** | `begin(acb, call) -> EffectId` | 登记 effect 意图（`INTENT`） |
| | `confirm(effect_id)` | 标记 `CONFIRMED`（与 EffectEvent 同事务） |
| | `mark_unknown(effect_id)` | 标记 `UNKNOWN`（供恢复对账） |
| **EventLog** | `replay(filter, from_seq) -> Stream<Event>` | 从 `from_seq` 起按 filter 重放（供 ContextAssembler 增量重放 / ReconcileLoop 对账） |
| **RunCoordinator** | `is_cancelled(acb) -> bool` | guard 查取消 |
| | `finalize_or_replan(run)` | 无 active 时推进终态或触发重规划 |
| | `submit_command(cmd) -> Status` | B1 命令接口（见 §4.3）  ← v1.9 修复 |

### 4.5 数据结构定义（v1.6 补齐）

```
Event = {
    seq, event_id, run_id, ts,
    type: WORKSPACE_COMMITTED | TOOL_COMPLETED | EFFECT_CONFIRMED | EFFECT_UNKNOWN
        | RESOURCE_COMMITTED | COMPENSATION_* | PLAN_* | BUDGET_* | APPROVAL_* | TASK_COMPLETED,
    subject,                       // 资源 / effect / task / run
    payload,                       // diff（非全文）
    producer, caused_by, idempotency_key,
    schema_version
}

ACB = {
    id, run_id, task_id, generation,
    state, spec, capability: CapabilityToken,
    read_set: Map<Resource, Version>,
    base_version: Map<Resource, Version>,   # == read_set 快照
    staged: Map<Resource, Bytes>,
    materialized, cursor, subscriptions,
    event_log: Vec<Event>,                  # 本地已消费
    private_ws, budget: BudgetNodeRef,
    deadline, step, heartbeat_at, checkpoint_ref,
    turn_seq, logical_subgoal_id,
    parent, children, trace_id, span_id,
    result_ref, failure,
    hitl_result: ApprovalResult | null   # v1.10 修复：HITL resume 值；进入 ctx 的方式见 §5.5
}

Plan = { plan_id, run_id, version, tasks: Map<TaskId, Task>,
         edges: [{from,to,kind: DATA|CONTROL|ORDER|LOGICAL}], invariants: [Invariant],
         budget, parent_version }

Task = { task_id, plan_id, type: READ|WRITE|RMW|EFFECT|VERIFY|SYNTH,
         goal, footprint, deps: [TaskId], priority, deadline,
         budget: BudgetRequest, resume_policy: RERUN|RESUME|COMPENSATE,
         logical_subgoal_id,          # v1.9 修复（P0-2）：I7 幂等键基准；Planner 分配，跨 Plan 版本复用
         state, result_ref }

TaskGraph = Plan.tasks + Plan.edges（DAG，无环）

ResultEnvelope = { task_id, agent_id, status, artifacts, summary, coverage, evidence,
                   confidence, open_questions, cost, verification }

Diagnosis = { failing_subgoals: [logical_subgoal_id], missing_deps: [TaskId],
             evidence: [EvidenceRef], suggested_patch: PlanPatch }
```

---

## 5. 完整数据流

### 5.1 端到端全景

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
   │   （任务失败 → propagate_failure 到同 Plan 版本后继；重规划 remake 任务不受波及）  ← v1.9 修复
   │ ④ dataflow ready 队列
   ├──▶ [AgentRuntime A] ┐
   ├──▶ [AgentRuntime B] ├─ 并行（依赖计数归零即就绪）
   └──▶ [AgentRuntime C] ┘
   │ ⑤ 执行循环
   │   ├─ ContextAssembler 装配上下文（物化视图 + 增量）
   │   ├─ ModelGateway 决策（产出带 logical_action_id 的动作）
   │   ├─ ToolGateway 工具调用（EffectJournal + 稳定幂等键）
   │   ├─ spawn_child 嵌套派生（子 footprint 经 PlanValidator 校验，预算切分 I5 守恒）  ← v1.9 修复
   │   └─ WorkspaceManager stage→commit（RSC 两阶段；提交前校验 logical_deps hash）      ← v1.9 修复
   ▼
[EventLog] ◀─提交事务内追加事件（WAL）─ [WorkspaceManager]
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

**横切（异步）**：BudgetManager 全程拦截（预算耗尽 → **OOM 式驱逐**，杀价值最低的子 agent，返回部分结果）；CheckpointStore 每步落盘；Trace 每步采集；HITL 在关键操作前中断；**ReconcileLoop** 周期对账（level-triggered）；**DurableTimer** 驱动 aging / HITL 超时 / 退避。  ← v1.9 修复

### 5.2 派发数据流

**① 适用性门控**（是否值得多智能体）：

```
① 子任务能否独立并行（无共享中间状态）？ 否 → 单 agent
② 单 agent 上下文是否真的溢出（>200K 仍不够）？ 否 → 长上下文/RAG/compaction
③ 任务价值是否 > 多 agent 的 token 成本？ 否 → 单 agent
④ 每个子任务失败能否独立归因/验证？ 否 → 单 agent + 反思
全部是 → 多 agent
```

**② 任务分解与 footprint 声明**：

```
footprint = {
  committed  : Set<ResourceSpec>,   // 确定会写，参与准入
  contingent : Set<ResourceSpec>,   // 可能写，不预留；首次写触发 scope promotion
  min_budget : BudgetRequest        // 准入用预算下界
}
ResourceSpec = { kind: FILE|DIR|DB_ROW|API|CONTRACT|INVARIANT, id, mode, base_version? }
```

- **可验证单元**：只分解到「能独立验收」的粒度。
- **有界递归**：根 Planner 只分解第一层；agent 可在 `depth < D`、`fanout < F`、`total_agents < A` 且预算允许时请求派生（见 §5.2⑦ `spawn_child`）。
- **关键规则**：未知的**读**集合允许；未知的**写**集合不允许（fail-closed）。首次写未声明资源 → 拒绝 + `request_scope` 重新准入。

**③ 并行判定**：

```
parallel(a, b) :=
      not reaches(a, b) ∧ not reaches(b, a)          // 传递可达性
   && committed_writes(a) ∩ committed_writes(b) = ∅   // 物理写不重叠
   && logical_writes(a)  ∩ logical_writes(b)  = ∅     // 逻辑契约不冲突
   && (writes(a) ∩ reads(b) = ∅ ∨ reads(b) 固定版本且允许陈旧)
   && (writes(b) ∩ reads(a) = ∅ ∨ reads(a) 固定版本且允许陈旧)
   && fits_budget(a) ∧ fits_budget(b)
   && no_exclusive_external(a, b) ∧ no_ordering_only(a, b)
```

> **关键澄清（v1.5）**：
> - `parallel(a,b)` 判定的是「**DAG 中可否无依赖地并行调度**」，用于建依赖边。
> - **准入**是另一道门：`try_admit` + `intents.compatible`（见 ⑤）。它只拦**契约/不变量冲突**与**读-写方向冲突**，**不拦同资源写-写重叠**。
> - 因此两个 agent 可能被同时准入写同一 Z2 资源（reservation 不阻塞），但**提交层用 fencing + CAS 裁决**，只有一个生效，另一个走 rebase。**DAG 层、准入层、提交层三者分离**。

**④ 调度模型（dataflow ready queue + 升级路径）**：

```
on_task_complete(t):
    for s in successors(t):
        s.remaining_deps -= 1
        if s.remaining_deps == 0: ready_queue.push(s)
    if t.state in {FAILED, QUARANTINED, CANCELLED, DEAD_LETTER}:
        // v1.9 修复（P0-3）：只传播到「同一 Plan 版本」的后继；
        // 重规划产生的 remake 任务属新 Plan 版本，不被旧任务的失败波及
        propagate_failure(t, plan_version = t.plan_version)

loop:
    progress = false
    for t in ready_queue by (priority, aging):
        if try_admit(t): launch(t); progress = true
    if ready_queue 非空 and not progress:
        for t in ready_queue:
            // v1.8 修复（P1-3）：aging 由 DurableTimer 到期驱动，非轮次计数
            if durable_timer.expired(t.aging_deadline):
                t.state = INADMISSIBLE
                escalate(t, reason = BUDGET_OR_CONFLICT) // → REPLANNING 或 FAILED
    ready_queue = ready_queue.filter(state != INADMISSIBLE)
    if no active and no ready:
        finalize_or_replan()
```

> **v1.9 修复（命令式改造）**：上述状态变更（`t.state = INADMISSIBLE` / `launch` / `escalate` / `finalize_or_replan`）**均为命令式** —— Scheduler 只提交 `Command`（`ADMIT`/`LAUNCH`/`ESCALATE`/`FINALIZE`），由 `RunCoordinator` 串行应用（B1，见 §4.3）。伪代码内联仅为可读性，**不代表 Scheduler 直接写状态**。

**Barrier 只用于显式阶段语义**（如「API 冻结后才能并行实现」）。

> **v1.5 修复（P0-2）**：`finalize_or_replan` 的判据从「no running」改为「**no active**」。
> `active` 定义为**所有非终态任务**（含 `RUNNING / WAITING_APPROVAL / RETRYING / REBASING / REPLANNING / QUARANTINED / INADMISSIBLE`）。
> 否则处于 HITL 等待、退避重试中的任务会被误判为「无 active」，导致 Run 被提前 finalize。

**⑤ 准入（非阻塞、可老化）**：

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

> **v1.5 修复（P0-2）：`intents.compatible` 的精确定义**
>
> | 冲突类型 | 准入层（`intents.compatible`） | 提交层（fencing + CAS） |
> |---|---|---|
> | **契约/不变量冲突** | **fail-fast 拒绝** | — |
> | **读-写方向冲突** | **fail-fast 拒绝** | — |
> | **同资源写-写重叠** | **放行**（reservation 不阻塞） | **CAS 裁决** |
>
> 即：**准入层只拦「语义级」冲突，不拦「同资源写-写」**（后者留待提交层）。
> 这消除了「`parallel` 谓词要求写集不重叠」与「两 agent 可同时准入写同一资源」的矛盾。
> `active_intents` = 当前 Run 内所有有效租约集合。

**⑥ 派发状态机**：

```
PLANNED ─try_admit─▶ ADMITTED ─instantiate─▶ INSTANTIATING ─spawn─▶ READY ─schedule─▶ RUNNING
                            │                        │
                            ▼                        ▼
                         FAILED                  FAILED
```

**⑦ 嵌套派生契约（spawn_child）**：

```
AgentRuntime → Dispatcher : spawn_child(parent_acb, spec, footprint, budget_slice, capability_subset) -> ChildHandle

约束：
  - 子任务必须声明 footprint（committed/contingent + min_budget）
  - 子计划必须经 PlanValidator 校验
  - 子 agent 的 budget 从父节点切分（I5 守恒：child.reserved ≤ parent.remaining）
  - 子 agent 的 capability 是父的子集（I4 封闭）
  - depth ≤ D ∧ fanout ≤ F ∧ total_agents < A
  - 父可选择 await 或继续
  - 子结果通过 ResultEnvelope 回传父
```

> **v1.5 修复（P1-取消传播）**：取消**不依赖父子链转发**（父进入终态后无法转发，深层嵌套会断裂）。
> - **RunCoordinator 直接广播**：取消信号发给**所有后代**（RunCoordinator 维护全局活跃 agent 表）。
> - **Supervisor 维护活跃表**：心跳超时即回收，不依赖父 ACB。
> - **父自然完成（非取消）**：父 `DONE` 时把子的预算账户「过户」给 Run 级节点；子继续跑，由 RunCoordinator 直接管理（父不再参与）。

### 5.2.1 ReconcileLoop：level-triggered 对账（v1.7 补齐，P0）

> **问题**：`remaining_deps` / `ready_queue` 原在**内存**中（§5.2④）。若调度器在「A 完成 → 投递后继 B」之间崩溃，重启后 B 的 `remaining_deps` 仍是 1 → **B 永远不被调度**（EventLog 里有 A 的完成事件，但没人去对账）。这是**架构级缺陷**。

**解法（对标 K8s level-triggered reconcile / Airflow 周期扫描）**：

```
function reconcile_loop():                    // 独立线程/进程，周期性运行
    # ① 从持久化存储重建 ready 集（不依赖内存状态）
    all_tasks = plan_store.load(run_id)        # 持久化的 Plan + Task 状态
    for t in all_tasks:
        t.effective_deps = count(
            d in t.deps where plan_store.get(d).state != DONE
        )
        if t.effective_deps == 0 and t.state in {PLANNED, ADMITTED}:
            # v1.8 修复（P1-1）：放宽为 PLANNED/ADMITTED（原仅 READY，会漏掉崩溃时未 launch 的任务）
            ready_queue.push(t)
    # ② 扫描非终态但「看似卡住」的任务
    for t in all_tasks where t.state not in TERMINAL:
        if heartbeat_expired(t) and t.state not in {WAITING_APPROVAL}:
            mark_stale(t)                      # 触发重试/升级
    # ③ 对账 EventLog 与 Task 状态
    for t in all_tasks:
        if event_log.has(t.id, TASK_COMPLETED) and t.state != DONE:
            t.state = DONE                     # 事件即真相
```

**关键**：
- `remaining_deps` 从 `PlanStore` **重建**，不依赖内存 → 崩溃后可恢复。
- **事件即真相**：EventLog 里有 `TASK_COMPLETED` 则强制置 DONE（幂等）。
- 周期运行（如每 5s）+ 启动时必跑一次。

> **与 edge-triggered 的关系**：edge-triggered（`on_task_complete`）是**快路径**（正常情况低延迟）；level-triggered reconcile 是**兜底**（崩溃/漏事件时收敛）。两者并存。

### 5.3 运行数据流

```
async function agent_loop(acb):
    acb.state = RUNNING
    turn_seq = acb.turn_seq                 // v1.2：从 checkpoint 恢复，崩溃后不归零
    while not acb.done:
        guard(acb)                          // 取消/期限/预算/步数/无进展 + intents.renew(acb.leases, acb.generation)（v1.9 修复）
        ctx = context_view(acb)
        decision = modelgw.infer(acb, ctx)
        trace.span(acb, decision)
        turn_seq += 1
        acb.turn_seq = turn_seq              // 每 turn 持久化（幂等键基准）

        staged_writes = []
        for i, action in enumerate(decision.actions):
            action.logical_action_id = (turn_seq, i) // 稳定逻辑序号（用于幂等键）
            if i % GUARD_EVERY == 0:                 // v1.5：per-action guard（原仅 per-turn）
                if not guard(acb):                   // 取消/预算/期限/无进展 + renew（v1.9 修复）
                    acb.state = FAILED; acb.failure = GUARD_VIOLATION
                    checkpoint_store.save(acb); return
            match action.kind:
                READ:
                    ctxmgr.read(acb, action.resource)        // 读即订阅 + 注册 + 填充 read_set
                STAGE_WRITE:
                    staged_writes.add(action)
                    ws.stage(acb, action.resource, action.payload)
                TOOL_CALL:
                    if tool_manifest[action.tool].effectful:
                        staged_writes.add_if_resource(action)
                    effect = toolgw.invoke(acb, action)      // EffectJournal
                    // v1.9 修复：toolgw.invoke 后立即扣预算，调用点补齐
                    st_b = budget.consume(acb.budget, toolgw.cost(action))
                    if st_b == BUDGET_EXHAUSTED:
                        acb.state = FAILED; acb.failure = BUDGET_EXHAUSTED
                        checkpoint_store.save(acb); return
                    if effect.state == CONFIRMED:
                        eventlog.append_in_txn(EffectEvent, acb, effect)  // v1.5：补齐 EffectEvent 产生路径
                    ctxmgr.append_local(acb, effect)
                SPAWN_CHILD:
                    dispatcher.spawn_child(acb, action.spec, action.footprint,
                                          action.budget_slice, action.capability_subset)
                REQUEST_HITL:
                    // v1.5：挂起前把本 turn 已 staged 的写「显式处置」：
                    //   - 已 stage 但未 commit → 一律丢弃（记录 stale_staged），不自动提交
                    discard_staged(acb)                          // 避免挂起后写悬空
                    release_all_locks(acb)                       // 挂起前释放锁
                    acb.state = WAITING_APPROVAL
                    checkpoint_store.save(acb)
                    hitl.raise(acb, action)
                    ok = await hitl.wait_resume(acb, timeout = HITL_TIMEOUT)  // 可中断
                    if not ok:
                        if cancellation_requested(acb):      // v1.2：Signal 取消优先于超时
                            acb.state = CANCELLED; return
                        acb.state = FAILED; acb.failure = APPROVAL_TIMEOUT; return
                    // v1.9 修复（P0-4）：resume 值的三条去向
                    acb.hitl_result = hitl.get_result(acb)    // ① 审批结果写入 ACB
                    acb.state = RUNNING
                    continue                                  // ② 回到 while 顶部：重新 infer（结果进入 ctx）
                    // ③ staged 已在挂起前 discard，无需重做；未消费的 action 由重新 infer 生成

        if staged_writes not empty:
            st = commit_batch(acb, staged_writes)             // RSC（返回值必须处理）
            if st != OK:
                if not handle_commit_result(acb, st):         // v1.2：失败即短路，不继续循环
                    acb.state = FAILED; acb.failure = COMMIT_UNRECOVERABLE   // v1.5：设置终态
                    checkpoint_store.save(acb); return        // 避免悬空于 RUNNING
                continue                                      // rebase 后重试本 turn

        if decision.kind == FINISH:
            result = build_result_envelope(acb, decision)
            artifact_store.put(result.artifacts)
            if contract.validate(result):                // v1.5：循环内只做轻量契约校验
                advance_baseline(acb)                    // v1.2：return 前推进 + 保存
                checkpoint_store.save(acb)
                finalize(acb, DONE); return
            // 语义验证失败（由 §8 聚合后的 VerifierRunner 完整校验）
            acb.state = REBASING
            checkpoint_store.save(acb)
            replanner.submit(acb, verifier.diagnose(result))
            return

        advance_baseline(acb)                                // 每 turn 推进
        checkpoint_store.save(acb)
```

> **v1.2 修复（P0/P1）**：
> - **P0-4**：`turn_seq` 从 checkpoint 恢复并每 turn 持久化 —— `logical_action_id`/`idem_key` 崩溃后稳定，**I7 成立**。
> - **P1-4**：HITL 等待支持 Signal 取消（`cancellation_requested`），不再阻塞到超时。
> - **P1-5**：`handle_commit_result` 返回 `false` 时短路返回，避免失败提交被 `finalize` 成 `DONE`。

> **v1.1 修复**：`commit_batch` 返回值必须处理（不再静默丢弃）；`advance_baseline` + `checkpoint.save` 在 FINISH/REBASING 分支**之前**不跳过（把 FINISH 分支移到循环体末尾，或在其 return 前调用）。

### 5.4 提交协议：RSC（两阶段 + 补偿）

> 核心：**预留（不阻塞）→ 暂存（私有）→ 短临界区提交（fencing + CAS + 读集合校验）**。LLM 推理期间绝不持锁。
> **v1.1 修复**：validate 移入临界区（消除 TOCTOU）；apply 内复验；多资源用 prepare + 补偿实现 all-or-nothing。

```
function commit_batch(acb, writes, mode = COMMIT) -> CommitResult:   // v1.10 修复：统一 mode
    sort(writes, by = LOCK_ORDER)
    leases = []
    applied = []                          // v1.2：只记录「已 apply」的写，供补偿
    try:
        // 阶段 0：预留意图（非阻塞）
        for w in writes:
            leases.add(intents.reserve(w.resource, WRITE,
                                       base_version = acb.read_set[w.resource],
                                       ttl = LEASE_MS))
        // 阶段 1：prepare —— 逐资源加锁校验（临界区内）
        for w, lease in zip(writes, leases):
            st = ws.prepare(acb, w, lease, mode)
            if st != VALID:
                return st                 // 全部未提交（此时尚无 apply）
        // 阶段 2：apply —— 逐资源加锁提交（临界区内复验）
        for w, lease in zip(writes, leases):
            st = ws.apply(acb, w, lease, mode)
            if st != OK:                  // 中途失败
                compensate(acb, applied)  // 只回滚「已 apply」的（v1.2 修复过度回滚）
                return st
            applied.add(w)
        return OK
    finally:
        release_leases(leases)           // 无论成败都释放
```

```
function ws.apply(acb, w, lease, mode) -> Status:
    lock(w.resource)                                  // 短临界区，O(write)
    if not intents.is_valid(lease):                   // 锁内复验（租约是否仍有效）
        unlock(w.resource); return REJECT_STALE
    if mode == COMMIT:                                // v1.9 修复（P0-1）：补偿模式跳过
        if acb.base_version[w.resource] != w.resource.version:
            unlock(w.resource); return CONFLICT(w.resource.version, null)
        for r in acb.subscriptions ∩ acb.read_set:   // v1.2：apply 内复验读集合（消除陈旧读 TOCTOU）
            if acb.read_set[r] != ws.version(r):
                unlock(w.resource); return REJECT_STALE_READ(r)
    if mode == COMPENSATE:
        # v1.9 修复（A1）：补偿反写前复验「资源内容仍是本 agent apply 的内容」
        if ws.content_hash(w.resource) != w.applied_hash:
            unlock(w.resource); return REJECT_COMPENSATE
    atomic_replace(w.resource, acb.staged[w.resource])
    w.resource.version += 1
    w.applied_hash = ws.content_hash(w.resource)          // v1.8 修复（P0-1）：供补偿复验
    eventlog.append_in_txn(RESOURCE_COMMITTED, w.resource, w.resource.version, acb)  // 与替换同事务
    unlock(w.resource); return OK
```

> **v1.2 修复（P0）**：
> - **P0-1**：`prepared` 改为 `applied`，`compensate` 只回滚已 apply 的写（消除过度回滚）。
> - **P0-3**：fencing 判定从「资源级全局单调 token」改为「**按租约**有效性」（`intents.is_valid(lease)`）。Supervisor 回收租约时只 `intents.revoke(lease)`，**不误拒其他合法持有者**。
> - **P1-1**：`apply` 内复验读集合，消除陈旧读 TOCTOU。
>
> **跨域对照（v1.3 新增，详见 [计网对照文档](networking-to-multiagent-mapping.md)）**：
> RSC 的「预留→暂存→短临界区提交 + 冲突回滚 + 指数退避」与**计网数据链路层的 CSMA/CD**（先听后发 + 边发边听 + 冲突即停 + 二进制指数退避）同构；「提交层是唯一串行化点」对应**计网分组交换的存储转发节点**；「at-least-once + 幂等键」对应 **TCP 可靠性语义**（重传可能重复，靠接收端去重）。
> **反向警示**：计网用「最小帧长」保证「发送时长 ≥ 争用期」才能检测冲突；我们**不用**这个机制 —— 冲突检测靠**锁内原子复验**（CAS），与事务时长无关，且设计原则 8 明确要求「临界区必须短」。

**冲突处理**（`handle_commit_result`）：
- `CONFLICT` / `REJECT_STALE_READ` → 返回给 agent 做**语义 rebase**（只修复依赖旧版本的步骤），有界重试。
- `REJECT_STALE` → 重读 + 重放事件 + 用新租约重试。
- 超限 → `QUARANTINED` → `REPLANNING`。

**补偿原语（v1.2 新增，v1.5 重写为可执行）**：

> **v1.5 修复（P0-1）**：原实现的补偿提交会被自身 RSC 校验拦住（`apply` 已 `version += 1` 但 `base_version` 未更新 → `CONFLICT`；`read_set` 未刷新 → `REJECT_STALE_READ`），且嵌套提交时外层租约仍持有（自冲突）。现改为：

```
function ws.prepare(acb, w, lease, mode) -> Status:   // v1.10 修复：与上文的 commit_batch 配套
    lock(w.resource)
    if not intents.is_valid(lease):
        unlock(w.resource); return REJECT_STALE
    if mode == COMMIT:                       // 补偿模式跳过这两项
        if acb.base_version[w.resource] != w.resource.version:
            unlock(w.resource); return CONFLICT(w.resource.version, compute_diff(...))
        for r in acb.subscriptions ∩ acb.read_set:
            if acb.read_set[r] != ws.version(r):
                unlock(w.resource); return REJECT_STALE_READ(r)
    if mode == COMPENSATE:
        # v1.7 修复（A1 补偿交叉）：反写前复验「资源内容仍是本 agent apply 的内容」
        if ws.content_hash(w.resource) != w.applied_hash:
            unlock(w.resource); return REJECT_COMPENSATE     # 期间被他人改动 → 升级 PIVOT/人工
    unlock(w.resource); return VALID

function compensate(acb, applied):
    // ① 补偿前先释放/续期外层租约，避免自冲突
    release_leases(acb.leases)
    for w in reversed(applied):              // 逆序
        if w.compensated: continue
        reverse = compute_reverse_patch(w)   // 基于 WorkspaceVersionStore 中的「提交前快照」
        if reverse is None:
            mark_pivot(w)                    // 已不可逆 → 记录 pivot，交人工
            compensation_journal.write(PIVOT, w); continue
        acb.staged[w.resource] = reverse     // v1.10 修复（P0）：反写内容接入 staged（否则会重复前写）
        st = commit_batch(acb, [w], mode = COMPENSATE)   // 带补偿幂等键
        if st == REJECT_COMPENSATE:
            mark_pivot(w); compensation_journal.write(PIVOT, w); continue  // v1.7：期间被改，不得覆写
        if st != OK:
            compensation_journal.write(FAILED, w)
            if retry_budget(acb) > 0: continue       // 有界重试
            audit.append(COMPENSATION_FAILED, w)     // 升级人工
            continue
        w.compensated = true
        compensation_journal.write(COMPENSATED, w)
```

- **补偿提交走 `mode = COMPENSATE`**，跳过 `base_version`/读集合校验，但**仍做内容复验**（`REJECT_COMPENSATE`：期间资源被他人改动则拒写，消除 A1 补偿交叉）；补偿前先释放外层租约（消除 H3 自冲突）。
- **补偿前先释放外层租约**（消除 H3 自冲突）。
- **提交前快照**：`ws.stage()` 时把前像写入 `WorkspaceVersionStore`（消除 H4 无输入）。
- **补偿崩溃安全**：`compensation_journal`（`COMPENSATING/COMPENSATED/FAILED/PIVOT`）持久化，恢复时对账（对齐 §5.8 的 EffectJournal）。
- **补偿失败处理**：重试 N 次 → 写审计事件 → 升级人工（不再静默半回滚）。
- **pivot 语义**：一旦某写已产生外部副作用（如已发邮件、已部署），标记为 pivot，**不可自动补偿**，升级人工。
- **承诺边界**：all-or-nothing 的语义是「**要么全部 apply，要么全部回退到 apply 前的版本**」；对含 pivot 的写，降级为「尽力补偿 + pivot 上报」，不承诺物理回滚。

**为什么不用长持锁**：agent 的一次写可能包含长耗时工具；锁的持有时间会跨秒级甚至分钟级。CoAgent 实验显示 2PL 在争用负载上只有 1.04× 加速且 0.81 次死锁，OCC 因中止反而 0.93×，而 advisory 的 MTPO 达 1.40× —— 「锁-中止」范式在 LLM agent 上系统性失效。

### 5.5 上下文数据流

```
function context_view(acb):
    view = clone(acb.materialized)
    for event in replay(acb.event_log, from = acb.cursor):
        if event.resource in acb.subscriptions:
            apply_diff(view, event.diff)
    acb.cursor = acb.event_log.tail
    view.private = load_private(acb)
    if acb.hitl_result is not null:      // v1.10 修复：HITL resume 值进入 ctx
        view.hitl = acb.hitl_result      // 作为独立字段（不混入订阅 diff）
    return view

function advance_baseline(acb):          // 每 turn 后调用
    acb.materialized = context_view(acb)
    acb.materialized_version = acb.event_log.tail        // = 已应用事件的最大 seq
    acb.cursor = acb.event_log.tail
    if acb.materialized.size > MATERIALIZED_CAP:
        continue_as_new(acb)              // 开新 segment，旧 segment 归档
```

**规则**：
- **Z1 只读区**：读不阻塞、不产生失效事件、不参与**主动**重放；**但提交期仍做版本校验**（`REJECT_STALE_READ` 对 Z1 资源同样生效 —— 见 §5.4 prepare 的读集合校验）。**v1.5 修复（P1）**：原「永不失效、不参与重放」与「提交期校验读集合」矛盾，现澄清为「不主动失效，但提交期校验」。
- **Z2 可写区通过事件失效**，按需重放。
- **Z3 私有区不参与共享**，直接读写。
- **读即订阅 + 同步注册**：`ctxmgr.read()` 内部把资源加入 `acb.subscriptions`，调用 `subscription_registry.add(acb, resource)`，并把 `(resource, version)` 记入 `acb.read_set`。
- **近似重放契约**：重放**不保证字节等价**，只保证「已确认 effect 不重复 + 事件按 seq 有序 + 冲突可检测」。
- **事件保留与 cursor**：EventLog 保留最近 `RETENTION` 窗口的事件 + 每 K 个事件一个快照；**强制 `K ≤ RETENTION`**（保证快照落在保留窗口内）；`cursor` 不得落后于保留窗口下限，否则从最近快照重放（见 §11 I11）。
- **长挂起（v1.5）**：进入 `WAITING_APPROVAL` 时强制落一个快照，且该快照在 Run 存活期内不受 `RETENTION` 淘汰（对齐 §9.3 的「可跨月恢复」）。

### 5.6 事件与订阅（Signal / Update / Query 三分离）

| 通道 | 语义 | 用途 | 是否产生事件 |
|---|---|---|---|
| **Signal** | 异步、无返回值、绝不失败 | HITL 通知、外部触发、**取消** | 否 |
| **Update** | 同步、原子、有返回值 | 资源提交、状态变更 | 是（WorkspaceEvent） |
| **Query** | 只读、不产生事件、不触库 | 读状态、procfs 式内省 | 否 |

**事件分两类**：
- `WorkspaceEvent`：对应一个已提交资源版本（`RESOURCE_COMMITTED`）。
- `EffectEvent`：对应一个已提交 effect（`TOOL_COMPLETED`）。

**投递**：EventLog 分配全局 `seq`；SubscriptionRegistry 按 `(resource, version)` 单调投递给订阅者。

> **v1.7 修复（P0-2）：EventLog 的定位（v1.8 统一措辞）**
>
> **明确选择：EventLog 是「侧通道 + 对账输入」，不是「唯一真相」。**
> - **唯一恢复依据 = `CheckpointStore`**（含 `workspace_commit` + `effect_journal_offset` + `staged` + `turn_seq` + `cursor`）。
> - EventLog 承担：**失效传播**（订阅者按版本失效）、**审计**、**对账输入**（ReconcileLoop 用事件纠正 Task 状态）。
> - **措辞统一**：§5.2.1 的「事件即真相」应理解为「**对账时**以事件为准（幂等纠正 Task 状态）」，与「恢复依据是 CheckpointStore」**不矛盾** —— 前者是控制面对账，后者是执行面恢复。
> - **补齐产生者**：`PLAN_*`/`BUDGET_*`/`APPROVAL_*`/`COMPENSATION_*`/`TASK_COMPLETED` 事件在对应组件动作时追加（见 §4.5 Event.type 枚举）。
>
> **v1.7 修复（P0-3）：EffectEvent 原子性对齐 I8**
>
> - `WorkspaceEvent` 与资源替换**同事务**（I8 前半句）。
> - `EffectEvent` 与 EffectJournal 的 `CONFIRMED` 转态**同事务**（§5.3 已补产生路径）。
> - **I8 明确为**：「**资源提交**与 WorkspaceEvent 同事务；**effect 确认**与 EffectEvent 同事务」。
>
> **v1.7 修复（P1-背压）**：
> - `ready_queue` 设**深度上限** `MAX_READY`，满则暂停调度（背压到准入层）。
> - EventLog 投递设 **per-subscriber 游标 + 最大在途**，订阅者落后过多则**降级为快照重放**（保留事件，不丢弃 —— 避免与 I6「事件不丢」冲突）。  ← v1.9 修复
>
> **v1.7 修复（P1-DLQ）**：`QUARANTINED` 加 aging + 强制预算释放路径 → 超限转 **`DEAD_LETTER` 终态**（不再无限占用预算）。

### 5.7 并发控制：按层选机制

| 资源 | 读 | 写 | 机制 |
|---|---|---|---|
| Z3 私有 | 无锁 | 无锁 | overlay/scratch |
| Z1 只读 | 无锁 | 产生新版本 | 内容寻址快照 + 引用计数 GC |
| Z2 共享可写 | 版本快照 | intent + 短提交 + fencing + CAS | RSC |
| 外部副作用 | 查询 | 租约 + 审批 + 幂等 + 补偿 | EffectJournal |
| 逻辑契约/不变量 | 版本化契约 | 契约变更串行/公告 | Contract Resource |

**死锁预防**：提交锁短、按资源 ID 排序、超时即放弃；reservation 不阻塞，**契约级/读-写方向冲突在准入时 fail-fast**，**同资源写-写冲突留待提交层 CAS 裁决**，均不形成等待环。

### 5.8 失败与恢复

**失败分类**（「结果态」列：仅 `DONE/DONE_PARTIAL/FAILED/CANCELLED` 为**终态**，其余为中间态，见 §5.9）：

| 故障 | 处理 | 结果态 |
|---|---|---|
| 瞬态错误（限流/超时/5xx） | 指数退避 + jitter | RETRYING |
| schema/契约格式错误 | 结构化错误回喂模型，有限重试 | RETRYING → FAILED |
| 语义错误 | Verifier 最小修复 | REBASING |
| 写冲突 / 陈旧读 | 合并 / 语义 rebase | REBASING |
| 陈旧写 | 重读 + 新租约 | REBASING |
| 预算/期限耗尽 | 优雅停止 + 部分结果 | DONE_PARTIAL |
| agent 崩溃 | 租约回收、generation+1、checkpoint 恢复 | RETRYING |
| 毒任务（速率窗口内超限） | 熔断隔离 | QUARANTINED → REPLANNING |
| 孤儿 | 级联取消、回收租约 | CANCELLED |
| 无进展 | 触发修复或失败 | FAILED |
| 全部失败 | 聚合覆盖度 + 部分结果 | DONE_PARTIAL |

**重启强度 + 重启预算**（借鉴 Erlang/OTP）：每个 task 定义 `{max_restarts, period}`；窗口内超限 → 不再重试，直接升级 `QUARANTINED`。

**恢复流程**：

```
① 定位最近成功检查点（含 workspace_commit + effect_journal_offset + staged + turn_seq）
② 按 resume_policy 决定：RERUN | RESUME | COMPENSATE
   （resume_policy 存于 Task spec，**默认 RESUME**（v1.9 修复）：从 checkpoint 的 `turn_seq` 续跑；
     若显式 `RERUN` 则 `turn_seq` 归零；COMPENSATE 仅用于含 pivot 的 task）
③ generation += 1（旧租约失效；logical_action_id 不变）
④ 重放事件日志（幂等，按 event.id 去重）
⑤ 扫描 EffectJournal：
     - CONFIRMED → 跳过
     - UNKNOWN / INTENT / EXECUTING（未确认）→ 先按 idempotency_key 反查外部系统；
       查不到再补偿或升级人工（若外部系统不支持反查，则必须人工确认）
⑤′ 扫描 compensation_journal（v1.9 修复）：
     - COMPENSATING → 续做（补偿未完成，按幂等键重试）
     - FAILED        → 升级人工（补偿失败）
     - PIVOT         → 人工对账（不可自动合并）
⑥ 已确认副作用用 saga 补偿（非回滚）
⑦ 恢复 ACB，重新入队；Supervisor 回收租约并 budget.release
```

> **v1.5 修复（P0-3）：幂等键作用域**
>
> `idem_key` **不能含 `task_id`** —— 重规划会生成新 task，导致同一外部副作用被重复执行（I7 跨 replan 失效）。
>
> 改为基于**逻辑子目标 id**（跨 replan 稳定）：
>
> ```
> idem_key = hash(run_id, logical_subgoal_id, hash(call.tool, hash(call.args)))
> ```
>
> - `logical_subgoal_id`：Planner 为「一个可独立验收的子目标」分配的稳定 id，重规划时若子目标语义不变则复用。
> - 绑定 `tool + args` 哈希：避免「同一位置、不同动作」被误判为重复。
> - 恢复时若 `logical_subgoal_id` 无法对齐（计划结构大变），则该 effect 进入人工对账队列。

### 5.9 状态机（v1.3：拆成两张表，消除与场景表的矛盾）

> **v1.3 修复（P0-1）**：原表把「任务/agent 状态」与「Run 状态」混在一张表里，导致 `FAILED` 被声明为终态却出现 `FAILED→QUARANTINED`、缺 `REBASING→REPLANNING`/`RETRYING→FAILED`/`RUNNING→FAILED` 等矛盾。现拆成两张表。

**表 A：任务 / Agent 状态机**（单 agent 的生命周期）

| 当前状态 | 事件 | 下一状态 |
|---|---|---|
| PLANNED | try_admit 通过 | ADMITTED |
| PLANNED | try_admit 拒绝（aging 超限） | INADMISSIBLE |
| ADMITTED | instantiate 完成 / 失败 | INSTANTIATING / FAILED |
| ADMITTED | 用户取消 | CANCELLED | ← v1.9 修复
| INSTANTIATING | spawn 成功 / 失败 | READY / FAILED |
| INSTANTIATING | 用户取消 | CANCELLED | ← v1.9 修复
| READY | schedule | RUNNING |
| READY | 用户取消 | CANCELLED | ← v1.9 修复
| RUNNING | FINISH + 验证通过 | DONE |
| RUNNING | 请求审批 | WAITING_APPROVAL |
| RUNNING | verifier 失败（需重规划） | REBASING |
| RUNNING | 瞬态错误 | RETRYING |
| RUNNING | 无进展（I14） | FAILED |
| RUNNING | 用户取消 | CANCELLED |
| WAITING_APPROVAL | resume / 超时 / Signal 取消 | RUNNING / FAILED(APPROVAL_TIMEOUT) / CANCELLED |
| REBASING | 局部自愈 patch 应用完成（同 turn 内） | RUNNING |
| REBASING | 需完整重规划 | REPLANNING |
| REBASING | 重规划预算耗尽 | QUARANTINED |
| RETRYING | retry / 超重启窗口 | RUNNING / QUARANTINED |
| RETRYING | schema 失败（不可重试） | FAILED |
| QUARANTINED | escalate | REPLANNING |
| REPLANNING | 新 Plan vN+1 生成 | PLANNED（新任务，旧任务终态为 FAILED） |
| INADMISSIBLE | escalate | REPLANNING 或 FAILED |
| RETRYING/REBASING/QUARANTINED/REPLANNING/INADMISSIBLE | 用户取消（RunCoordinator 广播） | CANCELLED |

**表 B：Run 状态机**（一次多智能体任务的全局生命周期）

| 当前状态 | 事件 | 下一状态 |
|---|---|---|
| NEW | 规划完成 | PLANNING |
| PLANNING | 计划校验通过 / 失败 | RUNNING / FAILED |
| RUNNING | 全部任务 DONE | DONE |
| RUNNING | 部分任务失败但有产出 | DONE_PARTIAL |
| RUNNING | 全部任务失败 | DONE_PARTIAL（覆盖度标注） |
| RUNNING | 预算耗尽 | DONE_PARTIAL |
| RUNNING | 用户取消 | CANCELLED |

**终态**：
- 表 A：`DONE`、`FAILED`、`CANCELLED`、`DEAD_LETTER`（**无出边**）。
- 表 B：`DONE`、`DONE_PARTIAL`、`FAILED`、`CANCELLED`（**无出边**）。
- `QUARANTINED` 不是终态（经 REPLANNING 离开）；`REBASING`/`RETRYING`/`INADMISSIBLE` 同理。

> **v1.8 修复（P1-2）**：新增 `DEAD_LETTER` 终态（毒任务重规划预算耗尽后转入）。转移：
> - `QUARANTINED | 重规划预算耗尽 | DEAD_LETTER`
> - `DEAD_LETTER` 必释放租约与预算（避免资源饥饿）。

> **与场景表的对应**：场景 E6 的 `RETRYING → QUARANTINED`（非 `FAILED→QUARANTINED`）；场景 H5 的 `REBASING → REPLANNING`；场景 H10 的 `DONE_PARTIAL` 属**表 B**。

---

## 6. 上下文与记忆

### 6.1 上下文构成

```
C_agent = ( B, E, P, S )
B = 基线快照（物化视图 materialized）
E = 增量事件流（event_log，按 cursor 消费）
P = 私有状态（Z3）
S = 订阅集合（= read_set ∪ 运行时新读入的 Z2 资源）
```

**关键字段定义（v1.5 补齐）**：

| 字段 | 定义 |
|---|---|
| `read_set` | `Map<Resource, Version>`：agent 读过并依赖的资源及其**读取时版本** |
| `base_version` | `Map<Resource, Version>`：提交时的**期望基线版本**。**恒等于 `read_set[r]`**（提交基于读到的版本）；`advance_baseline` 后统一推进 |
| `staged` | 本 turn 已 stage 但未提交的写 |
| `cursor` | 已消费事件的水位线（单调推进） |
| `materialized` | 物化视图（已应用历史 diff） |
| `subscriptions` | 订阅的资源集合（用于失效传播） |

> **v1.5 修复（P1）**：`base_version` 全文未定义（旧版是 `Lease` 字段）。现明确它 = `read_set` 的快照，在 `advance_baseline` 时统一推进。

### 6.2 记忆分层

| 层 | 类比 | 内容 | 淘汰策略 |
|---|---|---|---|
| working（上下文窗口） | 寄存器/内存 | 当前 turn 活跃内容 | 随 turn 更替 |
| scratchpad | 页缓存 | 近期中间结果 | 近似 LRU |
| episodic（向量库） | 日志 | 历史事件/轨迹 | 保留窗口 |
| semantic（索引） | 数据库 | 事实/知识 | 相关性+时间 |
| procedural（技能库） | 可执行代码 | 可复用技能 | 使用频率 |

**记忆巩固**：episodic → semantic 的定期巩固（去重、消解矛盾、遗忘低价值）；**冲突消解**：同一事实多版本时按时间/来源可信度取舍。

### 6.3 上下文大小控制

- **分层加载**：先摘要/索引，按需展开详情。
- **懒加载**：只在实际访问时拉取。
- **裁剪**：丢弃已被覆盖的旧事件、已完成的中间结果。
- **前缀稳定**：静态内容前置、动态内容后置，命中 prompt cache。

---

## 7. 并发与一致性

### 7.1 一致性目标

| 层 | 目标 | 语义 |
|---|---|---|
| Z2 | **线性化**（单资源提交点） | 写原子生效；任何后续读立即看到最新值 |
| Z1 | **单调读** | 同一 agent 不会读到比之前更旧的版本 |
| Z3 | 本地单调 | 仅 agent 自己可见 |

> 注意：只承诺**单资源提交线性化**；agent 的整个 turn 不承诺线性化（跨分钟，期间他人可多次提交）。

### 7.2 混合策略

- **Z1 只读区**：无锁读 + 版本校验（内容寻址快照 + 引用计数 GC，**不照搬 RCU** —— agent 读临界区横跨数分钟推理，RCU 宽限期前提不成立）。
- **Z2 可写区**：预留-暂存-提交 + fencing + CAS（短临界区）。
- **Z3 私有区**：无锁（单写者）。
- **低冲突资源**：乐观并发 + 三路合并（见 §7.2.1）。
- **高冲突资源**：可选**单写者串行档**（借鉴 Restate Virtual Object / Orleans grain），把「LLM 语义 rebase」降为「确定性队列排序」。

### 7.2.1 语义 rebase 与合并算法（v1.6 补齐，此前是「空心机制」）

`CONFLICT` 后 agent 需把「基于旧版本的工作」迁移到新版本。分三档，按资源类型选择：

| 档 | 适用 | 算法 |
|---|---|---|
| **A. 文本文件（代码）** | `.ts/.js/.py/.go/...` | **3-way merge**（见下） |
| **B. 结构化配置** | `.json/.yaml/.toml` | **JSON-aware merge**（见下） |
| **C. 不可合并** | 二进制、锁文件、生成物 | **pivot**：标记不可自动合并，回喂 LLM 重做 |

**A. 文本 3-way merge**：

```
function rebase_text(base, ours, theirs) -> MergeResult:
    # base = 提交时的 base_version 内容；ours = 冲突方已提交的新内容；theirs = 本 agent 的 staged
    hunks = diff3(base, ours, theirs)
    if all hunks non-overlapping:
        return AUTO_MERGED(apply(hunks))
    else:
        return CONFLICT_MARKERS(mark(ours, theirs))   # 生成 <<<<<<< ======= >>>>>>>
```

- **非重叠**：自动合并（如 A 改函数签名、B 改函数体）。
- **重叠**：生成冲突标记，**回喂 LLM 重编辑**（带冲突上下文 + 任务目标），而非让 agent 盲目重做整个任务。
- **收敛保证**：3-way merge 是确定性的；LLM 重编辑是**有界重试**（超限 → `QUARANTINED`）。

**B. JSON-aware merge**：

```
function rebase_json(base, ours, theirs) -> MergeResult:
    # 键级合并：仅当同一 key 的三方值不完全相同时冲突
    for key in union(keys(base), keys(ours), keys(theirs)):
        if ours[key] == theirs[key]: keep
        elif base[key] == ours[key]: take theirs[key]      # ours 未改 → 取 theirs
        elif base[key] == theirs[key]: take ours[key]      # theirs 未改 → 取 ours
        else: mark_conflict(key)                           # 双方都改 → 冲突
    # 数组：默认 grow-only（只增不删；删除需 pivot）  ← v1.9 修复
```

- 解决用例 3 中「5 个 agent 都加 `"strict": true`」的**假冲突**（additive 改动仍整文件冲突）。
- 数组默认 **grow-only**（v1.9 修复）：**只增不删**（适合 `compilerOptions.paths`、`dependencies`）；
  **不支持删除**，删除需走 pivot（回喂 LLM 重做）；若要支持删除，需引入 **tombstone**（墓碑标记）。
- **不再声称「幂等律」（v1.9 修复）**：3-way merge **不是幂等的** —— 重叠冲突时**不收敛**（需 LLM 重编辑 + 有界重试，超限 → `QUARANTINED`）。

**per-key 合并策略配置来源（v1.9 修复）**：`strict` 等键的合并策略由 **Planner 在 footprint 声明 `merge_policy`** 决定：

| 键类型 | 默认策略 | 说明 |
|---|---|---|
| `strict` 等布尔开关 | `GROW_ONLY` | 多方置 `true` → 取 `true` |
| `compilerOptions.paths` / `dependencies` | `GROW_ONLY` | 只增不删（并集） |
| `scripts` | `REPLACE` | 整键替换（单写者） |
| 不可合并键（二进制 / 生成物） | `PIVOT` | 标记 pivot，回喂 LLM |

```
footprint.merge_policy: Map<Key, MERGE_STRATEGY>   # MERGE_STRATEGY = GROW_ONLY | REPLACE | PIVOT
```

**C. pivot**：不可合并 → 标记 pivot，回喂 LLM 重做受影响步骤。

> **为什么这是 P0**：原规范只说「返回给 agent 做语义 rebase」，**没有定义 rebase 做什么**。若实现为「取对方版本 + 重放自己的 diff」，代码文件没有 diff 概念（agent 产出整文件 staged 内容）；若实现为「LLM 重新编辑」，则非确定且无收敛保证。现补齐为**确定性 3-way merge + 有界 LLM 重编辑**。

### 7.3 写偏斜（write skew）与逻辑依赖（v1.6 补齐）

两个 agent 读同一不变量、写不同文件，文件锁抓不到。**根因：跨文件不变量未纳入 footprint**。

**解法**：footprint 增加 `logical_deps` 声明：

```
footprint = {
  committed  : Set<ResourceSpec>,
  contingent : Set<ResourceSpec>,
  min_budget : BudgetRequest,
  logical_deps: Set<LogicalResource],      # v1.6 新增：类型引用图、接口契约、配置不变量
  logical_subgoal_id: str                  # v1.9 新增（P0-2）：I7 幂等键基准
}
LogicalResource = { kind: CONTRACT|INVARIANT|TYPE_GRAPH, id, hash }
```

- **`logical_deps` 纳入准入**：`intents.compatible` 检查逻辑依赖是否被他人声明（冲突则准入 fail-fast）。
- **`logical_deps` 纳入提交校验**：提交前校验依赖的逻辑资源 hash 是否变化（类比读集合校验）。
- **Run 级不变量校验**：聚合后跑一次不变量检查（如 `tsc --noEmit`、lint 规则）；失败则生成修复任务。

> **用例 3 的 import-graph 问题由此解决**：`f1` import `f2` → `f1` 的 footprint 声明 `logical_deps={TYPE_GRAPH: f2}`，`f2` 迁移改类型时 `f1` 被准入拦截或提交期校验拦截，**不再依赖 Verifier 事后发现**。

### 7.4 死锁预防

锁排序（按路径字典序）+ all-or-nothing（提交层 prepare + 补偿）+ 租约超时回收。reservation 不阻塞，**契约级/读-写方向冲突在准入时 fail-fast**，**同资源写-写冲突留待提交层 CAS 裁决**，均不形成等待环。

---

## 8. 结果聚合与验证

```
并行组完成 ──▶ [Aggregator]
                 │ ① 按输出契约检查覆盖度
                 │ ② 去重（相同发现合并）
                 │ ③ 矛盾检测（互补/矛盾 → 综合或上报）
                 ▼
            [VerifierRunner]（独立上下文）
                 │ ① 先跑确定性检查（schema/test/lint/typecheck）
                 │ ② 再跑语义 rubric
                 ▼
            [Replanner] → [PlanValidator] → [Dispatcher]（失败时）
                 │ 通过
                 ▼
            [Renderer] ──▶ 最终输出（流式）
```

**ResultEnvelope 契约**：

```
ResultEnvelope = {
    task_id, agent_id, status,
    artifacts: [ArtifactRef],        // 内容寻址引用，不内联全文
    summary,
    coverage: { subgoals, missing },
    evidence: [EvidenceRef],
    confidence,
    open_questions,
    cost, verification
}
```

**为什么 Verifier 是一等公民**：agent 的主要失败是**静默语义错误**（退出码 0 但内容错），OS 式错误处理（errno/try-catch）会系统性漏检。必须把输出校验当一等公民的「健康检查」。

> **v1.1 澄清（v1.5 修订）**：VerifierRunner 在**两处**被调用，语义不同 —— ① agent_loop 内的**轻量契约校验**（`contract.validate`，仅校验 schema/契约格式，决定是否可以 finalize）；② 聚合后的**独立上下文验证**（完整 test/lint/typecheck + 语义 rubric，决定整体是否通过）。循环内**不跑**完整 verifier（太重），只做契约校验。

### 8.1 Replanner：从诊断到计划补丁（v1.6 补齐）

Verifier 失败 → `diagnose()` 产出**结构化 `Diagnosis`**（见 §4.5）→ Replanner 映射为 `PlanPatch`：

| Diagnosis 字段 | PlanPatch 动作 |
|---|---|
| `failing_subgoals` | 为每个失败子目标生成**新的 `logical_subgoal_id` 相同的任务**（复用幂等键基准） |
| `missing_deps` | 在 `edges` 中补 `LOGICAL` 依赖边（如 `f1 → f2`） |
| `evidence` | 附到任务的 `acceptance_criteria`（供 Verifier 复验） |
| `suggested_patch` | 直接作为 `PlanPatch.tasks` 的增删改 |

```
function replan(diagnosis, current_plan) -> PlanPatch:
    patch = { added: [], modified: [], removed: [] }
    for sg in diagnosis.failing_subgoals:
        # 复用 logical_subgoal_id → 幂等键稳定
        patch.modified.add(remake_task(sg, new_task_id(), keep_logical_subgoal_id=true))
    for dep in diagnosis.missing_deps:
        patch.modified.add(add_edge(dep.from, dep.to, kind=LOGICAL))
    return patch
```

> **v1.6 修复（P1）**：原规范只说「`replanner.submit(diagnose)`」，**未定义**如何把 `tsc`/Verifier 报错映射成任务级补丁。现明确：诊断结构化 + 补丁可生成 + 复用逻辑子目标 id（保证幂等键跨 replan 稳定）。

---

## 9. 治理

### 9.1 预算（BudgetNode 树）

```
BudgetNode = {
    node_id, parent_id, run_id, agent_id,
    limits: { input_tokens, output_tokens, cost, wall_ms, steps, tool_calls, children },
    reserved, consumed, children: [node_id]
}
parent.remaining = parent.limits − parent.own_reserved − sum(children.reserved)
不变量：
  I5a: consumed ≤ reserved ≤ limits
  I5b: sum(children.reserved) ≤ parent.remaining
release 只归还「parent.remaining − sum(children.reserved)」（不抽走子节点已占用的部分）
reserve 为单写者（或 CAS），保证并发准入不超支
```

- **父约束不可被下层突破**（cgroup 语义）。
- **`remaining` 口径（v1.5 修复 P0-4）**：必须扣除子节点预留，否则父可双花；`release` 只归还未被子占用的部分，避免「父抽走子的预算」。
- **并发安全**：`reserve` 为单写者（类比 EventLog 单序列器），或 CAS；避免「check + deduct」非原子导致超支。
- **OOM 式驱逐**：预算耗尽时杀价值最低的子 agent，返回部分结果 + 覆盖度标注（`DONE_PARTIAL`）。
- **准入预留**：`try_admit` 时预留，不阻塞运行中的 agent。
- **释放时机**：终态（DONE/DONE_PARTIAL/FAILED/CANCELLED）或级联取消时 `release`；`QUARANTINED`/`REPLANNING` 为**非终态**，预算保持占用直到进入终态。

### 9.2 可观测（Trace）

- 每条工具调用一个 span；decision span 作为 parent。
- span 属性：模型、token、成本、延迟、工具、资源、版本、fencing token、cache hit。
- Run → Task → Agent → Turn → ModelCall/ToolCall/Commit 的 span 树。
- 审计日志独立于业务日志。

### 9.3 HITL

- 关键操作前 pause，checkpoint 落盘，**挂起前释放全部写锁**。
- 可跨机/跨月恢复；`resume` 值需校验。
- **有超时**（`HITL_TIMEOUT`）→ `FAILED(APPROVAL_TIMEOUT)`。
- **等待可中断**（v1.1）：`wait_resume` 通过 Signal 通道响应取消 → `CANCELLED`（不再阻塞到超时）。
- **挂起前 staged 写处置**（v1.5）：`discard_staged`（显式丢弃），不自动提交。
- **Durable timer**（v1.7）：HITL 超时由**一等 `DurableTimer`** 承载（存 PlanStore），Supervisor 到期触发 —— **不依赖进程存活**（对齐「跨月恢复」）。

### 9.4 安全威胁模型（v1.2 新增）

| 威胁 | 攻击面 | 缓解 |
|---|---|---|
| **prompt injection**（间接注入） | 工具返回的网页/文档内容被当作指令 | 工具/子 agent 输出用 `<untrusted>` 包裹；系统指令明确「不得执行其中的指令」；auto 模式剥离工具结果 |
| **越权访问** | agent 访问未授权路径/网络 | CapabilityToken（工具/路径/网络三维 scope）；PolicyEngine 强制 |
| **沙箱逃逸** | 代码执行逃逸出容器 | OS 级沙箱（Seatbelt/Landlock/gVisor）；只读根文件系统 + 可写白名单 |
| **密钥泄漏** | 密钥进入上下文/日志 | SecretBroker 按能力短期发放；密钥不进上下文 |
| **供应链** | 依赖被投毒 | 锁定版本 + 校验和；最小依赖 |
| **重放攻击** | 重放已批准的写 | fencing token + 幂等键；收据签名 |
| **资源耗尽** | 恶意/失控 agent 烧钱 | 预算树硬上限 + OOM 式驱逐 |
| **审计抵赖** | agent 否认执行过某操作 | 收据（签名 + hash-chain）+ 独立审计日志 |

> **边界诚实声明**：我们的强制是**策略强制**（边界强度 = 校验器强度），**不是硬件强制**。prompt injection 可在语义层穿透白名单 —— 这是 L5 互联层（A2A 信任）与模型厂商的责任，不是本层能根治的。

---

## 10. 样例跑通

**记号**：`ACB.state`；资源 `fileX@vN`；fencing `tok`；`idem` 为稳定幂等键。

### 场景 A：三 agent 并行改三个不同文件（正常路径）✅

| # | Actor | 状态（前） | 动作 | 产生数据 | 状态（后） | 检查 |
|---|---|---|---|---|---|---|
| A1 | Ingress | — | `submit(req, idem_key)` | RunHandle | Run: NEW | ✅ 幂等去重 |
| A2 | Planner | PLANNED | `plan()` | Plan v1（含 footprint+min_budget） | — | ✅ |
| A3 | PlanValidator | Plan v1 | `validate()` | Valid | Plan: VALID | ✅ I9 |
| A4 | Dispatcher | T1 PLANNED | `try_admit` + `reserve(min_budget)` | BudgetNode.reserved+ | T1: ADMITTED | ✅ |
| A5 | Dispatcher | T1 ADMITTED | `reserve(fileA, W, v0)` | lease{fileA,T1,v0} | — | ✅ |
| A6 | Scheduler | 三就绪 | `launch` ×3 | AgentHandle A1/A2/A3 | READY→RUNNING | ✅ |
| A7 | A1 | RUNNING | `stage(fileA)` | staged{fileA} | — | ✅ |
| A8 | A1 | RUNNING | `commit(fileA, leaseA1)` | `fileA@v1`，事件 | — | ✅ I1/I2/I3 |
| A9 | A2/A3 | RUNNING | `commit(fileB/fileC)` | `fileB@v1`,`fileC@v1`，两事件 | — | ✅ 无冲突 |
| A10a | A1/A2/A3 | RUNNING | `contract.validate` + `put`（循环内轻量校验） | ResultEnvelope + artifact 落库 | →DONE | ✅  ← v1.9 修复 |
| A10b | Aggregator→Verifier | — | `verify`（聚合后完整 test/lint/语义校验） | Verdict | — | ✅  ← v1.9 修复 |
| A11 | Scheduler | 无 running/ready | `finalize_or_replan` | 触发聚合 | — | ✅ |
| A12 | Aggregator→Verifier | — | `reduce`+`verify` | 覆盖度 OK | — | ✅ |

**结论**：✅ 完全跑通。

### 场景 B：两 agent 改同一文件（写-写冲突）✅

| # | Actor | 状态 | 动作 | 产生数据 | 检查 |
|---|---|---|---|---|---|
| B1 | Scheduler | T1/T2 PLANNED | `launch` ×2（均 base=v1） | A、B 启动 | ✅ 准入不互斥（v1.9 修复：launch 归属 Scheduler） |
| B2 | A | RUNNING | `reserve(fileX,W,v1)` | leaseA | ✅ |
| B3 | B | RUNNING | `reserve(fileX,W,v1)` | leaseB | ✅ reservation 允许双写 |
| B4 | A | RUNNING | `commit(fileX, leaseA)` | prepare 通过 → apply → `fileX@v2` | ✅ I1/I2/I3 |
| B5 | B | RUNNING | `commit(fileX, leaseB)` | apply 内复验：base v1≠v2 → `CONFLICT(v2,diff)` | ✅ 拦住丢失更新 |
| B6 | B | RUNNING | 语义 rebase：refresh→重读 v2→重做受影响步骤 | 重新 stage | ✅ |
| B7 | B | RUNNING | `commit(rebased, leaseB, base=v2)` | `fileX@v3` | ✅ 租约仍有效 |
| B8 | A/B | RUNNING | finalize | ResultEnvelope ×2 | ✅ |

**真并发验证（v1.2 修正）**：若 A 在 `prepare` 后、`apply` 前，B 插入 —— A 的 `prepare` 已解锁，资源版本仍是 `v1`，故 B 的 `prepare` 会通过；随后 A `apply` 成功（`fileX@v2`），B 再 `apply` 时**锁内复验 `base_version`（v1≠v2）+ 读集合** → `CONFLICT`。✅ **单资源 base_version 与读集合的 TOCTOU 均在 apply 内复验，已消除**。

**结论**：✅ 跑通（提交层裁决，非准入层）。

### 场景 C：改接口 vs 读接口（读-写冲突 + 上下文失效）✅

**设定**：T1 写 `api.ts`；T2 读 `api.ts` 写 `impl/b.ts`。

**分支 C-1（允许陈旧读 → 提交期拦截）**：

| # | Actor | 状态 | 动作 | 产生数据 | 检查 |
|---|---|---|---|---|---|
| C1 | Scheduler | T1/T2 | `launch`（T2 声明固定读 v1） | A、B 启动 | ✅（v1.9 修复：launch 归属 Scheduler） |
| C2 | B | RUNNING | `read(api.ts)` | subscriptions+={api.ts}；`read_set{api.ts:v1}`；`subscription_registry.add` | ✅ 已注册 |
| C3 | A | RUNNING | `commit(api.ts, leaseA)` | `api.ts@v2`，事件 | ✅ |
| C4 | SubscriptionRegistry | — | `route(event)` | B 视图标 STALE | ✅ |
| C5 | B | RUNNING | `commit(b.ts, leaseB)` | prepare：`read_set[api.ts]=v1 ≠ v2` → **`REJECT_STALE_READ(api.ts)`** | ✅ **拦住陈旧读** |
| C6 | B | RUNNING | refresh + replay + rebase | 基于 v2 重做 | ✅ |
| C7 | B | RUNNING | `commit(rebased, leaseB, base=v2)` | `b.ts@v1` 提交 | ✅ 最终一致 |

**分支 C-2（串行）**：T2 依赖 T1 → 天然一致 ✅。

**结论**：✅ 跑通。（注：陈旧读检测粒度是「整 agent 订阅集」，高订阅数下可能过度拒绝 —— 见 §14 P2 优化项。）

### 场景 D：写入中崩溃（恢复路径）✅

| # | Actor | 状态 | 动作 | 产生数据 | 检查 |
|---|---|---|---|---|---|
| D1 | A | RUNNING | `reserve(fileX,W,v1)` | leaseA | ✅ |
| D2 | A | RUNNING | `stage(fileX)` | staged{fileX}（存入 Checkpoint） | ✅ staged 持久化 |
| D3 | A | RUNNING | 崩溃 | — | — |
| D4 | Supervisor | 心跳超时 | `revoke(leaseA)`（仅作废 A 的租约） | leaseA=REVOKED | ✅ I3（不误拒他人） |
| D5 | B | RUNNING | `reserve(fileX,W,v1)` | leaseB | ✅ |
| D6 | B | RUNNING | `commit(fileX, leaseB)` | `fileX@v2` | ✅ |
| D7 | Supervisor→A | — | `resume(ref)`：generation=2 | ACB 恢复（含 staged） | ✅ |
| D8 | A | RUNNING | `commit(oldStaged, leaseA)` | leaseA 已 revoked → `REJECT_STALE` | ✅ I3 |
| D9 | A | RUNNING | 重读 v2 → rebase → 新 `reserve` → `commit` | `fileX@v3` | ✅ |

**副作用恢复**：
- A 崩溃前调用过非幂等工具 T，`logical_action_id = (1, 3)`。
- `idem_key = hash(run_id, logical_subgoal_id, hash(tool, hash(args)))` —— **不含 task_id、不含 generation**；`turn_seq` 从 checkpoint 恢复（v1.2 修复）。**v1.7 修复**：原写 `hash(run_id, task_id, (1,3))` 含 task_id，会导致跨 replan 重复副作用；现改为逻辑子目标 id + 工具/参数哈希（对齐 §5.8/§15）。
- 若 T 停在 `INTENT`（未确认）→ 按 UNKNOWN 处理 → **先反查外部系统** → 未发生则重执行（幂等键保证不重复）；已发生则跳过；外部系统不支持反查则升级人工。
- ✅ **I7 成立**：副作用有效一次。

**结论**：✅ 跑通（工作区 + 副作用两条恢复路径都可靠）。

### 场景 E：预算耗尽 / 全失败 / 重规划 ✅

**设定**：根预算 `cost=100`；T1..T5 各 `min_budget=30`。

| # | Actor | 状态 | 动作 | 产生数据 | 检查 |
|---|---|---|---|---|---|
| E1 | Dispatcher | T1..T5 PLANNED | `reserve` T1/T2/T3 成功，T4/T5 失败 | reserved=90，remaining=10 | ✅ |
| E2 | T1/T2/T3 | RUNNING | 执行、`consume` | consumed 上升 | ✅ |
| E3 | T4/T5 | deferred | 每轮 `try_admit` 失败 | `durable_timer.expired(t.aging_deadline)`（v1.9 修复） | ✅ |
| E4 | T4/T5 | deferred | `aging > AGING_MAX` | → `INADMISSIBLE` → `escalate` | ✅ **不再 livelock** |
| E5 | T1（毒任务） | RUNNING | 瞬态错误 | → RETRYING | ✅ |
| E6 | T1 | RETRYING | 重启强度窗口内超限 | → QUARANTINED | ✅ 有窗口（不经 FAILED） |
| E7 | T1 | QUARANTINED | 触发重规划 | → REPLANNING → Plan v2 | ✅ **有出边** |
| E8 | PlanValidator | Plan v2 | `validate` | Valid | ✅ 不再绕过校验 |
| E9 | T1 后继 | — | `remaining_deps` 随新 Plan 重置 | 入队 | ✅ 不再死锁 |
| E10 | 全部失败 | — | 无 DONE | → `DONE_PARTIAL` + 覆盖度标注 | ✅ 有终态 |
| E11 | 正常完成 | — | 部分任务结束 | `budget.release` 归还剩余 | ✅ 预算守恒 |

**结论**：✅ 跑通（livelock、死锁、无界重规划三处全部修复）。

### 场景 F：嵌套派生（A 派生 B，B 写资源，A 被取消）✅

| # | Actor | 状态（前） | 动作 | 产生数据 | 状态（后） | 检查 |
|---|---|---|---|---|---|---|
| F1 | A | RUNNING | `spawn_child(spec, footprint, budget_slice, cap_subset)` | ChildHandle B | — | ✅ 契约存在（§5.2⑦） |
| F2 | BudgetManager | — | 切分预算 | `BudgetNode(B)`，`B.reserved ≤ A.remaining` | — | ✅ I5 守恒 |
| F3 | Dispatcher | — | 实例化 B（capability ⊆ A 的） | `ACB_B` | READY | ✅ I4 封闭 |
| F4 | B | RUNNING | `commit(R)`（R 为 Z2） | `R@v1` | — | ✅ RSC |
| F5 | 用户 | — | 取消（Signal） | Signal 入队 | — | ✅ |
| F6 | A | RUNNING | 步边界检查 | — | CANCELLED | ✅ |
| F7 | B | RUNNING | 级联取消（父→子树） | — | CANCELLED | ✅ 无孤儿 |
| F8 | Supervisor | — | 回收 B 租约 + `budget.release` | released | — | ✅ 无泄漏 |

**结论**：✅ 跑通（v1.1 补齐 `spawn_child` 契约）。

### 场景 G：HITL 审批 + 取消 ✅

| # | Actor | 状态（前） | 动作 | 产生数据 | 状态（后） | 检查 |
|---|---|---|---|---|---|---|
| G1 | A | RUNNING | `REQUEST_HITL` | — | RUNNING | ✅ |
| G2 | A | RUNNING | `release_all_locks` | 锁已释放 | RUNNING | ✅ §9.3 |
| G3 | A | RUNNING | `checkpoint.save` + `hitl.raise` + `await wait_resume(timeout)` | checkpoint | WAITING_APPROVAL | ✅ 可中断 |
| G4 | 用户 | — | 取消（Signal） | Signal 入队 | — | ✅ |
| G5 | A | WAITING_APPROVAL | `wait_resume` 收到 Signal | — | CANCELLED | ✅ **立即响应，不阻塞到超时** |
| G6 | Supervisor | — | 回收租约 + `budget.release` | released | — | ✅ |
| G7 | 超时分支 | WAITING_APPROVAL | `HITL_TIMEOUT` 到期（无取消时） | — | FAILED(APPROVAL_TIMEOUT) | ✅ 有超时兜底 |

**结论**：✅ 跑通（v1.1 让 `wait_resume` 可中断）。

### 场景 H：部分成功 + 覆盖度缺失 ✅

**设定**：5 并行任务，3 成功、1 失败、1 被 Verifier 打回。

| # | Actor | 状态（前） | 动作 | 产生数据 | 状态（后） | 检查 |
|---|---|---|---|---|---|---|
| H1 | Scheduler | T1..T5 PLANNED | `launch` ×5 | A1..A5 | READY→RUNNING | ✅（v1.9 修复：launch 归属 Scheduler） |
| H2 | A1/A2/A3 | RUNNING | commit + finalize | ResultEnvelope ×3 | DONE | ✅ |
| H3 | A4 | RUNNING | 连续失败 | — | FAILED | ✅ |
| H4 | A5 | RUNNING | `verifier.pass=false` | — | REBASING | ✅ |
| H5 | Replanner | REBASING | `submit(diagnose)`（受重规划预算约束） | Plan patch v2 | REPLANNING | ✅ **有界** |
| H6 | PlanValidator | Plan v2 | `validate` | Valid | — | ✅ |
| H7 | Dispatcher | — | 派发 T5'（原 T5 的 staged 已清理） | 新 ACB | ADMITTED | ✅ |
| H8 | Scheduler | 无 running/ready | `finalize_or_replan` | — | — | ✅ |
| H9 | Aggregator | — | `reduce` | `coverage.missing={subgoal_x}` | — | ✅ 覆盖度检查 |
| H10 | Run | — | 部分成功 | — | DONE_PARTIAL | ✅ **有终态** |

**结论**：✅ 跑通。

### 10.1 跑通汇总

| 场景 | 结果 |
|---|---|
| A 三 agent 并行改不同文件 | ✅ |
| B 两 agent 改同一文件（写-写冲突） | ✅ |
| C 改接口 vs 读接口（读-写冲突） | ✅ |
| D 写入中崩溃（恢复） | ✅ |
| E 预算耗尽/全失败/重规划 | ✅ |
| F 嵌套派生 + 取消级联 | ✅ |
| G HITL 审批 + 取消 | ✅ |
| H 部分成功 + 覆盖度缺失 | ✅ |

---

## 11. 不变量

| 编号 | 不变量 |
|---|---|
| I1 | 单写者提交：同一 Z2 资源任意时刻至多一个提交在临界区内 |
| I2 | 无丢失更新：提交必须校验 `base_version` 与**读集合版本**，否则拒绝/合并；validate 与 apply 均在锁内 |
| I3 | fencing 单调：旧 generation/token 的写必须被拒绝 |
| I4 | 能力封闭：任何工具/资源/网络访问必须在 capability 内 |
| I5 | 预算守恒：`sum(children.reserved) ≤ parent.remaining`；结束必 `release` |
| I6 | 上下文因果一致：`materialized + ordered events` 是一个一致前缀 |
| I7 | effect 有效一次：**稳定幂等键** + effect journal 保证不重复副作用 |
| I8 | 事件原子：每个 `WorkspaceEvent` 对应一个已提交版本；每个 `EffectEvent` 对应一个已提交 effect；替换与事件追加同事务（WAL） |
| I9 | 计划有效：无环、每 task 有契约与 footprint、依赖满足才启动 |
| I10 | 隔离：私有工作区默认不可见 |
| I11 | 有界：深度/扇出/步数/token/时间/重规划/事件保留均有上限；cursor 不落后于保留窗口 |
| I12 | 终止/公平：每 task 在期限内到达终态或升级；ready task 不被无限饿死 |
| I13 | 可审计：输出可追溯到 artifact、effect、模型版本、策略版本 |
| I14 | 无进展终止：连续无进展必须触发修复/失败 |

---

## 12. 验收标准

| 验收项 | 测试方法 | 通过标准 |
|---|---|---|
| 写-写冲突 | 100 次并发两 agent 写同一文件 | 恰好一个提交**生效**，另一个被拒并 rebase；无交错 |
| 丢失更新 | A 读 v1、暂停；B 写 v2；A 写 | A 被 `CONFLICT` 拒绝 |
| TOCTOU | A prepare 后、apply 前插入 B | B 在锁内复验，被拒 |
| 陈旧读 | B 声明固定读 v1，A 写 v2，B 提交 | B 被 `REJECT_STALE_READ` 拒绝 |
| fencing | 暂停写者→租约过期→他人提交→恢复写者 | 恢复写者收到 `REJECT_STALE` |
| 副作用幂等 | 工具执行后、CONFIRMED 前崩溃 | 恢复后按稳定幂等键去重，无重复副作用 |
| 崩溃恢复 | 随机步 `kill -9` | 从 checkpoint 恢复；无重复副作用；预算已归还 |
| 预算 | 子任务请求超过父剩余 | 拒绝；父预算不被突破；结束归还 |
| 并行度 | N 个独立只读任务 | 墙钟 ≤ 1.5 × 最慢任务 |
| 能力隔离 | agent 尝试越界 | `DENIED_CAPABILITY`，无副作用 |
| 上下文可重建 | 重放事件流 | 物化视图与重放结果一致 |
| 语义验证 | 注入已知语义 bug | Verifier 拦截 |
| 取消传播 | 取消根任务 | 步边界内子任务全 CANCELLED；无孤儿 |
| HITL 取消 | 审批等待期取消 | 立即 CANCELLED，不阻塞到超时 |
| 无进展 | 构造重复动作循环 | K 步内触发 `NO_PROGRESS` |
| livelock | 全部任务不可准入 | aging 超限后升级，不死循环 |
| 嵌套派生 | 父取消级联子 | 子全部 CANCELLED，预算归还 |
| 覆盖度 | 故意漏掉子目标 | Aggregator 报告 missing，终态 DONE_PARTIAL |

---

## 13. Harness 能力清单与 OS 对比

### 13.1 多智能体框架对比（15 维度）

> **口径说明**：本表「我们」列只评估 **L1 核心**（见 §0.5）；L4 编排适配组件**不计入差异化**（它们是参考实现，由宿主框架提供）。

| 维度 | 我们（L1 核心） | 最强参照 |
|---|---|---|
| 调度编排（DAG/动态 fan-out/map-reduce/分层/重规划） | **L4 参考实现，非差异化** | LangGraph / CrewAI / Temporal |
| 状态持久化（checkpoint/WAL/重放/续跑重跑/历史压缩） | ✅➕ | Temporal / Restate / DBOS |
| 取消背压（协作取消/级联/背压/HITL 可中断；缺 drain/Admin） | ✅ | LangGraph / Restate |
| 重试幂等（分类重试/退避/幂等键/UNKNOWN 对账） | ✅➕ | Temporal / Restate |
| 并发一致性（CAS/fencing/租约/锁排序/advisory） | ✅➕ **业界独有** | 数据库 MVCC |
| 配额限流（层级预算树/多维上限/下游限流/准入/老化） | ✅➕ | cgroup / DBOS |
| 可观测（span/事件日志/成本归因；缺 timeline/metrics） | 🟡 非差异化 | OTel GenAI |
| HITL（中断/挂起保存/审批前释放锁/工具级审批/可中断） | ✅（L4 适配） | LangGraph / Temporal |
| 工具能力边界（白名单/schema/能力令牌/沙箱/MCP；缺密钥代理与 MCP 对接） | ✅➕（缺 MCP 对接） | MCP / OpenAI |
| 结果契约验证（schema/覆盖度/独立验证/矛盾检测） | ✅➕ **业界独有** | MAST |
| 失败语义（三分/毒任务熔断/无进展/部分结果） | ✅➕ | Temporal / DBOS |
| 嵌套递归（深度扇出上限；spawn_child 属 L4） | 🟡（L4 参考） | LangGraph / Claude Code |
| 事件订阅（发布订阅/全局 seq/per-resource/幂等投递/三分离） | ✅➕ | Temporal / AutoGen |
| 机制策略分离（分离/可插拔/多档位；缺定义版本化） | ✅➕（缺定义版本化） | OS / Temporal |

### 13.2 OS / 工作流引擎 / Actor 运行时对比（14 项通用能力）

| 能力 | OS 内核 | 工作流引擎 | Actor 运行时 | 我们 |
|---|---|---|---|---|
| ① 身份与句柄 | PID/`task_struct` | Run ID | ActorRef | ✅ |
| ② 生命周期状态机 | 进程状态+wait | Workflow 状态 | preStart/postStop | ✅ |
| ③ 派生/复制 | fork/clone | child workflow | actorOf | ✅（spawn_child） |
| ④ 调度与准入 | CFS/EEVDF | task queue+poll | dispatcher/turn | ✅ |
| ⑤ 资源配额记账 | cgroup v2+OOM | rate limit/retry | mailbox 背压 | ✅（BudgetNode 树 + release） |
| ⑥ 隔离与权限 | namespace+ring+seccomp | tenant+沙箱 | 进程隔离 | 🟡（策略强制非硬件） |
| ⑦ 通信与共享状态 | IPC/VFS | Signal/Update/Query | mailbox | ✅（三分离） |
| ⑧ 并发控制与裁决 | mutex/RCU/CAS | 单写者 workflow+saga | 单线程 turn | ✅（RSC） |
| ⑨ 持久化与重放 | journal/WAL/fsync | event history | persistence/lineage | 🟡（非确定，近似重放） |
| ⑩ 时间/定时器 | clock/timer | durable timer | grain timer | 🟡（deadline+lease，无一等 timer） |
| ⑪ 故障检测恢复 | signal/core dump | 重放+补偿 | supervisor 树 | ✅（重启强度窗口） |
| ⑫ 取消与中断 | signal+EINTR | workflow cancellation | CancellationToken | ✅ |
| ⑬ 版本化 | ABI | workflow versioning | serialization ver | 🟡（Plan 版本化，缺定义版本化） |
| ⑭ 可观测内省 | /proc/ptrace/strace | visibility+history | lifecycle+dashboard | 🟡（OTel span，无 procfs 式内省） |

### 13.3 我们做得比框架好的（独有，仅 L1）

RSC 提交协议、IntentRegistry+fencing、EffectJournal 的 UNKNOWN 对账、独立上下文 VerifierRunner、BudgetNode 树、ContextAssembler 物化视图、footprint 声明、parallel 谓词、advisory rebase、CapabilityToken、NO_PROGRESS、DONE_PARTIAL。

> **注**：`ApplicabilityGate`（L4 编排）与 `SecretBroker`（L5）**不计入独有**（见 §0.5）。

### 13.4 「看起来该有但其实不该有」

确定性重放（改「近似重放+幂等键」）、per-agent mailbox（IDE 子代理共享 FS）、硬件式抢占（无微秒时钟）、完整 2PL（临界区分钟级）、CRDT 自动合并（token 成本+语义不可靠）、全量上下文广播（token 爆炸）。

---

## 14. 缺口与路线图

| 优先级 | 缺口 | 建议 |
|---|---|---|
| P0 | 分布式/跨机 | 引入共识 + RPC 层（切换分布式系统框架） |
| P0 | 确定性原语 | ~~`ctx.uuid/random/time`~~ **v1.7 修订**：与「近似重放」不矛盾 —— 重放的是**同一 decision**（持久化），但 `ctx.time/uuid/random` 仍需 replay-safe 注入（用于日志、缓存键），**降为 P1**（幂等键已覆盖重试场景） |
| **P0** | **Durable timer**（v1.7 补） | 一等 `DurableTimer`（存 PlanStore/独立表），由 Supervisor 到期触发；`aging` 改为基于 timer 而非轮询计数（对齐「跨月 HITL」） |
| **P0** | **隔离模型**（v1.7 补） | 明确选型：**Z3 = per-agent overlay/worktree（代码文件）**；Z2 = 提交层仲裁的共享资源。否则 RSC 冲突模型无法定义 |
| **P1** | **L2 规范四件套**（v1.6 补） | 资源模型补全（符号/键/外部副作用）、协调协议（GRANT/WAIT/DIE）、收据格式、conformance suite（见 §18） |
| P1 | 队列级并发/限速 | 借鉴 DBOS/Temporal 队列配置（**v1.7 已补 ready_queue 背压，见 §5.6**） |
| P1 | 优雅排空 drain | 借鉴 LangGraph graceful shutdown |
| P1 | Admin 控制面 | pause/resume/kill/purge（借鉴 Restate Admin） |
| P1 | 定义版本化 | agent/workflow spec 版本路由（借鉴 Temporal Worker Versioning） |
| P1 | procfs 式内省 | 标准化 introspection 接口 |
| **已完成** | 语义 rebase 合并算法 | ✅ §7.2.1（3-way merge / JSON-aware merge / pivot） |
| **已完成** | 接口契约章节 | ✅ §4.4 |
| P2 | MCP 对接 | 作为 ToolGateway 的一种后端 |
| P2 | eval harness | 回归评测（借鉴 LangSmith） |
| P2 | fork 子 agent | 从当前对话分叉（借鉴 Claude Code） |
| P2 | 陈旧读粒度优化 | 从「整订阅集拒绝」改为「按依赖图精确拒绝」 |
| P2 | I1–I14 形式化 | TLA+ 模型（§18.4 已提及，需落地） |

---

## 15. 术语表

| 术语 | 定义 |
|---|---|
| harness | 承载多智能体派发/运行/状态流转/并发控制/失败恢复的运行时框架 |
| 同态（homomorphism） | 保留问题结构、丢掉机制与语义的映射关系 |
| ACB | AgentControlBlock，agent 控制块（类比 PCB） |
| RSC | 预留-暂存-提交（Reserve-Stage-Commit）提交协议 |
| lease / fencing | 租约；`intents.is_valid(lease)` 拒绝已回收租约的写（v1.2 起 fencing 完全由租约生命周期承担） |
| effect | 工具的外部副作用；EffectJournal 记录其生命周期 |
| `idem_key` | 幂等键 = `hash(run_id, logical_subgoal_id, hash(tool, hash(args)))`（**不含 task_id**，跨 replan 稳定） |
| `logical_action_id` | `(turn_seq, action_index)`，动作逻辑序号（用于决策追踪，非幂等键） |
| `turn_seq` | turn 序号，从 checkpoint 恢复（崩溃后不归零） |
| `generation` | 恢复代次（每次恢复 +1，旧租约失效） |
| `base_version` | 提交时的期望基线版本，恒等于 `read_set[r]` |
| Byzantine 故障 | 静默的语义错误（退出码 0 但结果错） |
| 协作式取消 | 只能在协作点生效的取消 |
| 物化视图 | 已应用历史 diff 的上下文快照 |
| 读即订阅 | 读资源时自动加入订阅集合并注册 |
| 重启强度 | 时间窗口内允许的最大重启次数（Erlang/OTP 术语） |
| DONE_PARTIAL | 预算耗尽或全部失败时的部分完成终态 |
| `pivot` | saga 中不可回退的「不归点」 |
| `logical_subgoal_id` | 跨 replan 稳定的逻辑子目标 id（幂等键基准） |
| Z0–Z3 | 资源分区（内核区/只读共享/可读写共享/私有工作区） |
| L1–L5 | 交付归属层（核心/规范/托管运行时/编排适配/删除） |
| P0–P5 | 架构平面（底座/状态/执行/控制/治理/接入） |
| `CapabilityToken` | 能力令牌（工具/路径/网络三维 scope）；由 PolicyEngine 签发 |
| `NO_PROGRESS` | 无进展终止（阈值 K 由 PolicyEngine 配置） |
| `intents.compatible` | 准入层冲突判定：只拦契约/不变量与读-写方向冲突，**不拦同资源写-写** |

---

## 16. 参考资料

**多智能体框架**：LangGraph [Checkpointers](https://docs.langchain.com/oss/javascript/langgraph/checkpointers) · [Interrupts](https://docs.langchain.com/oss/javascript/langgraph/interrupts) · [Fault tolerance](https://docs.langchain.com/oss/javascript/langgraph/fault-tolerance)；CrewAI [Crews](https://docs.crewai.com/edge/en/concepts/crews) · [Cognitive Memory](https://blog.crewai.com/how-we-built-cognitive-memory-for-agentic-systems/)；OpenAI [Agents SDK](https://openai.github.io/openai-agents-python/)；AutoGen [Core Concepts](https://mintlify.wiki/microsoft/autogen/concepts)；Anthropic [Multiagent orchestration](https://platform.claude.com/docs/en/managed-agents/multiagent-orchestration)

**持久化执行**：Temporal [Workflow](https://docs.temporal.io/workflows) · [Versioning](https://docs.temporal.io/develop/typescript/workflows/versioning) · [Saga](https://docs.temporal.io/guides/saga-pattern)；Restate [Durable Execution](https://docs.restate.dev/concepts/durable_execution) · [Durable Agents](https://docs.restate.dev/ai/patterns/durable-agents)；DBOS [Workflows](https://docs.dbos.dev/python/tutorials/workflow-tutorial) · [Queues](https://docs.dbos.dev/typescript/tutorials/queue-tutorial)

**Actor 运行时**：Erlang/OTP [supervisor](https://www.erlang.org/docs/29/apps/stdlib/supervisor)（intensity/period）；Akka [Supervision](https://doc.akka.io/libraries/akka-core/current/general/supervision.html)；Orleans [Overview](https://learn.microsoft.com/en-sg/dotnet/orleans/overview)；Ray [Actor Fault Tolerance](https://docs.ray.io/en/latest/ray-core/fault_tolerance/actors.html)

**OS**：kernel.org [cgroup v2](https://docs.kernel.org/admin-guide/cgroup-v2.html) · [RCU](https://docs.kernel.org/RCU/whatisRCU.html) · [EEVDF](https://docs.kernel.org/scheduler/sched-eevdf.html) · [/proc](https://docs.kernel.org/filesystems/proc.html)；man7 [signal(7)](https://man7.org/linux/man-pages/man7/signal.7.html)

**并发研究**：CoAgent/MTPO [arXiv:2606.15376](https://arxiv.org/abs/2606.15376)；MAST [arXiv:2503.13657](https://arxiv.org/abs/2503.13657)；Tracking State Footprints [arXiv:2610.03140](https://arxiv.org/pdf/2610.03140)；Agent–tool boundary [arXiv:2609.15397](https://arxiv.org/html/2609.15397)

**跨域对照**：[OS ↔ 多智能体映射](os-to-multiagent-mapping.md) · [计网 ↔ 多智能体映射](networking-to-multiagent-mapping.md)

---

## 17. 跨域对照：设计决策的外部依据（v1.3 新增）

我们的每条核心设计都能在成熟系统里找到依据。这一节把**已采纳的对照**与**明确不采纳的对照**集中列出，作为设计决策的可追溯依据。

### 17.1 已采纳的对照

| 我们的设计 | 外部依据 | 采纳要点 |
|---|---|---|
| RSC 提交（预留-暂存-提交 + fencing + CAS） | 计网 CSMA/CD + TCP 可靠性 + 数据库 **OCC**（借 prepare 形态，**去掉协调者**，改逐资源 + 补偿） | 先尝试、短临界区提交、冲突回滚 + 指数退避；at-least-once + 幂等键 |
| 提交层是唯一串行化点 | 计网分组交换的存储转发节点；数据库单写者 | 上层语义无关，下层最小 |
| Z2 资源分区 | 计网 CIDR 聚合 + 交换机独立冲突域 | 缩小争用粒度（非消除） |
| 预算树 + OOM 式驱逐 | 计网拥塞控制（AIMD）+ OS cgroup + OOM killer | 加性增（老化）+ 乘性减（驱逐） |
| 上下文物化视图 + cursor | 计网累积确认 + cursor；数据库 MVCC | 水位线推进，重放增量 |
| VerifierRunner 独立上下文 | 端到端原则（正确性在端点） | 机械一致性在共享层，语义正确性在验证器 |
| 快照 + 重放（长任务） | 计网 FEC（先做冗余工作）；数据库 WAL | 长延迟场景用快照兜底 |
| 状态机（任务 + Run 两张表） | OS 进程状态 + 工作流状态 | 分层建模，避免混用 |

### 17.2 明确不采纳的对照

| 外部机制 | 不采纳原因 |
|---|---|
| 计网「最小帧长」（时长 ≥ 争用期） | 我们靠锁内 CAS 检测冲突，与时长无关；且设计原则 8 要求临界区短 |
| OS 抢占式调度 | 一次 LLM 调用不可分割；只能协作式取消 |
| 数据库 2PC/3PC | 依赖单一协调者 + 短事务；agent 事务跨分钟 |
| 硬件 RCU / 内存屏障 | 跨机不可用 |
| Mandatory file locking | Linux 内核明确不可靠 |
| 严格 serializable | read set 无法精确；代码可修补 |
| 字符级 CRDT | 收敛 ≠ 语义正确 |

> **依据来源**：[OS ↔ 多智能体映射](os-to-multiagent-mapping.md)（OS/数据库机制迁移）、[计网 ↔ 多智能体映射](networking-to-multiagent-mapping.md)（网络机制迁移）。

---

## 18. L2 规范四件套（阶段二交付物，v1.6 补齐）

定位文档 §6.3 承诺阶段二交付「规范 + 参考实现 + conformance suite」。本节给出**规范草案**。

### 18.1 资源模型

| 资源类型 | 说明 | 版本语义 |
|---|---|---|
| `FILE` | 文件（文本/二进制） | 单调版本号（≥64 位） |
| `DIR` | 目录 | 单调版本号 |
| `SYMBOL` | 代码符号（函数/类/接口） | hash |
| `DB_ROW` | 数据库行 | 版本号 |
| `API` | 外部 API 端点 | 契约 hash |
| `CONTRACT` | 接口契约 | hash |
| `INVARIANT` | 逻辑不变量 | hash |
| `TYPE_GRAPH` | 类型引用图 | hash |
| `BUDGET_NODE` | 预算节点 | 额度 |
| `EFFECT` | 外部副作用 | 幂等键 |

### 18.2 协调协议（GRANT / WAIT / DIE）

| 状态 | 含义 |
|---|---|
| `GRANT` | 准入成功，持有租约 |
| `WAIT` | 契约/读-写冲突，准入拒绝（fail-fast） |
| `DIE` | 同资源写-写，准入放行，提交层 CAS 裁决 |

### 18.3 收据格式

```
Receipt = {
    receipt_id, run_id, task_id, logical_subgoal_id,
    resource, version_before, version_after,
    effect_id?, idem_key?, compensation_ref?,
    prev_receipt_hash,            // hash-chain
    signature,                    // 由 P4 私钥签名
    schema_version
}
```

- 可离线验证（hash-chain + 签名）。
- 对齐 IETF PoB 草案（Proof-of-Behavior）。

### 18.4 conformance suite（不变量测试语料）

| 不变量 | 测试 |
|---|---|
| I1 单写者提交 | 并发写同一资源，临界区互斥 |
| I2 无丢失更新 | 覆盖写被拒并 rebase |
| I3 fencing 单调 | revoke 后旧租约被拒 |
| I4 能力封闭 | 越权访问被拒 |
| I5 预算守恒 | `consumed ≤ reserved ≤ limits`；父约束不被子突破 |
| I7 effect 有效一次 | 重复投递去重 |
| I8 事件原子 | 提交与事件同事务 |

> 用 TLA+ 建模核心不变量（I1–I4、I7）；I7 只建模**账本状态机**，不建模外部世界（LIMBO 教训：无 in-flight 上界时纯验证无法 exactly-once）。

---

## 19. 分布式对照：设计决策的外部依据（v1.9 新增）

> 详见 [分布式 ↔ 多智能体映射](distributed-to-multiagent-mapping.md)。本节只列**已采纳的结论**。

| 我们的机制 | 分布式依据 | 采纳 |
|---|---|---|
| RSC 提交 | **OCC**（read_set/write_set 验证） | ✅ |
| 补偿（逆序回滚 + pivot） | **Saga**（语义补偿） | ✅ |
| intent reservation | **TCC**（预留） | ✅ |
| base_version + 多版本 | **MVCC** | ✅ |
| logical_deps + Run 级校验 | **SSI 的 write skew 问题来源**（静态声明式依赖，非 SSI 动态检测） | 🟡（软准入 + 事后兜底） |
| EventLog（追加日志） | **LSM-Tree + WAL** | ✅ |
| CheckpointStore（快照） | **LSM flush + checkpoint** | ✅ |
| 订阅过滤 | **Bloom Filter** | ✅ |
| EventLog 作为变更流 | **CDC（binlog）** | ✅（建议强化） |
| ContextAssembler | **物化视图增量维护** | ✅ |
| cursor | **Watermark** | ✅ |
| idem_key | **Kafka 幂等生产者（PID+seq）** | ✅（语义级更强） |
| I7 | **Exactly-once = at-least-once + 幂等** | ✅ |
| ready_queue 上限 | **credit-based 背压** | ✅ |
| DEAD_LETTER | **DLQ** | ✅ |
| 单写者 seq / reserve | **Raft leader / Quorum** | ✅ |
| Z0–Z3 分区 | **一致性哈希 + vnode** | ✅ |
| 心跳 + 租约 | **Phi Accrual** | 🟡（阶段三引入连续怀疑度） |
| OOM 驱逐 | **熔断** | ✅ |
| per-agent 沙箱 | **gVisor / Firecracker** | ✅ |
| ReconcileLoop | **K8s level-triggered reconcile** | ✅ |
| DurableTimer | **Flink durable timer** | ✅ |
| JSON-aware merge | **3-way merge**（base/ours/theirs；非 CRDT，冲突时不收敛需 LLM 重编辑） | ✅ |

**明确不采纳**：2PC/3PC（协调者单点 + 长事务）、Calvin（要求确定性，agent 非确定）、字符级 CRDT（收敛 ≠ 语义正确，§13.4）、Percolator 的 TSO（中心授时，我们用单写者 seq）。

> **v1.9 澄清（P0-2）**：RSC 的 prepare/apply 两阶段**借自 OCC 的「验证-写入」结构**（Kung-Robinson 1981），**不是 2PC**。RSC = **OCC（read_set/write_set 验证）+ 按租约 fencing + 逐资源短临界区 + Saga 补偿**。去掉协调者后剩下的「prepare 校验 + apply 提交」就是 OCC 的标准形态。

---

## 20. 无死角图集（v1.9 新增）

> 目标：**尽可能消除模糊点**。以下 17 张图覆盖架构、数据流、调用、扇入扇出、状态机、事件流、恢复流、错误传播、并发竞争、安全边界、预算树、HITL。
>
> **诚实声明**：仍可能有个别视角未覆盖；发现遗漏请补图。

### 20.1 架构总图（分层 + 组件）

> **编号说明（v1.10 修复）**：图中用 **P0–P5 平面**（与 §4.1 一致）+ **L1–L5 交付归属**（与 §0.5 一致）双标签。
> - P0 隔离基质 = L0（基质）
> - P1 状态面 = **L1 核心**（我们交付）：WorkspaceManager / VerifierRunner / PolicyEngine / BudgetManager / CheckpointStore / EffectJournal 等
> - P2 执行面 = **L3 托管运行时**
> - P3 控制面 = **L4 编排适配**（参考实现）
> - P5 接入面 = **L4**（Ingress/Renderer）
> - **L2 规范**（阶段二交付，非运行时组件）、**L5 删除**（不做）不在图中。

```
┌──────────────────────────────────────────────────────────────────────────────┐
│ P5 接入面（L4，可替换）                                                        │
│   Ingress ──▶ Renderer                                                          │
├──────────────────────────────────────────────────────────────────────────────┤
│ P3 控制面（L4 编排，参考实现，非差异化）                                         │
│   ApplicabilityGate ─▶ Planner ─▶ PlanValidator ─▶ Dispatcher ─▶ Scheduler      │
│        │                  ▲                            │              │          │
│        │                  └──────── Replanner ◀────────┘              ▼          │
│        │                                                       RunCoordinator    │
│        ▼                                                                │        │
│   Aggregator                                                    ReconcileLoop      │
│                                                                    │             │
│   Supervisor（心跳/租约/取消/孤儿回收）◀──────────────────────────────┘             │
│   HITL（审批挂起/恢复）                                                          │
├──────────────────────────────────────────────────────────────────────────────┤
│ P2 执行面（L3 托管运行时）                                                     │
│   AgentRuntime ─▶ ContextAssembler ─▶ ModelGateway ─▶ ToolGateway               │
│        │                │                   │                │                 │
│        └────────────────┴───────────────────┴────────────────┴─▶ P1 状态面       │
├──────────────────────────────────────────────────────────────────────────────┤
│ P1 状态面（L1 核心，我们交付）                                                  │
│   WorkspaceManager   IntentRegistry   EventLog   SubscriptionRegistry          │
│   WorkspaceVersionStore   ArtifactStore   PlanStore   BudgetManager             │
│   VerifierRunner     PolicyEngine     CheckpointStore   EffectJournal           │
├──────────────────────────────────────────────────────────────────────────────┤
│ P0 隔离基质（L0）                                                             │
│   Sandbox / Worktree(overlay) / 网络出口 / 资源限制(cgroups)                      │
└──────────────────────────────────────────────────────────────────────────────┘
```

### 20.2 端到端数据流图（含分布式新增流）

```
[用户输入]
   │ ① Ingress（规范化/鉴权/idem_key）
   ▼
[ApplicabilityGate] ──不适合──▶ [单 Agent 路径]
   │ ②
   ▼
[Planner] ─Plan v1→ [PlanValidator] ─非法→ 修复/失败
   │ ③                              ▲
   ▼                                │ ③' PlanStore（持久化）
[Dispatcher] ─try_admit─────────────┘        ▲
   │ ④ BudgetManager.reserve                  │
   ▼                                          │
[Scheduler] ─launch─▶ [AgentRuntime]          │
   │ ⑤                      │                 │
   │                        ▼                 │
   │              [ContextAssembler] ◀─── EventLog（物化流 ★）
   │                        │                 ▲
   │                        ▼                 │
   │                 [ModelGateway]           │
   │                        │                 │
   │                        ▼                 │
   │                 [ToolGateway] ─▶ EffectJournal ─▶ EffectEvent ★
   │                        │
   │                        ▼
   │              [WorkspaceManager.stage]（写 Z3 worktree）
   │                        │
   │                        ▼
   │              [WorkspaceManager.prepare/apply]（merge 到 Z2 ★）
   │                        │
   │                        ├── WorkspaceEvent（同事务 ★）
   │                        ▼
   │                 [SubscriptionRegistry] ─▶ 失效通知
   │
   ▼ ⑥（每步）
[CheckpointStore.save]（turn_seq/staged/cursor）
   │
   ▼ ⑦（无 active）
[RunCoordinator.finalize_or_replan] ─▶ [Aggregator] ─▶ [VerifierRunner]
   │                                              │
   │                                              ▼ ⑧ 失败
   │                                        [Replanner] ─PlanPatch→ [PlanValidator] ─校验→ [Planner]  ← v1.9 修复
   ▼
[Renderer] ─▶ [输出]

★ = 分布式借鉴的流（**状态标注**）：
  · EventLog 物化流（CDC/物化视图）—— **建议**（未采纳；§5.6 已定 EventLog 为侧通道，唯一恢复依据 = CheckpointStore）
  · EffectEvent 同事务（Kafka 事务语义）—— **已采纳**（§5.6）
  · Z2 merge 点（OCC + Saga）—— **已采纳**（v1.8 P0-3 既定，非 v1.9 新增）

旁路（周期）：
[ReconcileLoop] ◀── PlanStore + EventLog（对账）
[DurableTimer.fire_due] ─▶ Supervisor（超时触发）
```

### 20.3 RSC 提交时序图（OCC + Saga）

```
Agent                WorkspaceManager         IntentRegistry        EventLog
 │                          │                       │                  │
 │ commit_batch(writes)     │                       │                  │
 ├─────────────────────────▶│                       │                  │
 │                          │ reserve(resource,W)   │                  │
 │                          ├──────────────────────▶│                  │
 │                          │◀──────────────────────┤ lease            │
 │                          │                       │                  │
 │        【阶段 1：prepare】│                       │                  │
 │                          │ lock(r)               │                  │
 │                          │ is_valid(lease)?      │                  │
 │                          │ base_version==ver?    │                  │
 │                          │ read_set 校验          │                  │
 │                          │ unlock(r)             │                  │
 │                          │                       │                  │
 │        【阶段 2：apply】  │                       │                  │
 │                          │ lock(r)               │                  │
 │                          │ 锁内复验（base/read_set）│                 │
 │                          │ atomic_replace(worktree→共享态)           │
 │                          │ version += 1          │                  │
 │                          │ applied_hash = hash   │                  │
 │                          │ append_in_txn(EVENT) ─┼─────────────────▶│
 │                          │ unlock(r)             │                  │
 │                          │                       │                  │
 │        【失败 → Saga 补偿】                      │                  │
 │                          │ release_leases()      │                  │
 │                          │ for w in reversed(applied):              │
 │                          │   reverse = compute_reverse_patch(w)    │
 │                          │   commit_batch(mode=COMPENSATE)          │
 │                          │   └─ content_hash != applied_hash?       │
 │                          │       └─ REJECT_COMPENSATE → PIVOT       │
 │                          │   compensation_journal.write(...)         │
 │                          │                       │                  │
 │◀─────────────────────────┤ OK / CONFLICT / ...   │                  │
```

### 20.4 调用图（Call Graph）

```
Ingress.submit
 └─ ApplicabilityGate.check
     └─ (适合) Planner.plan
         ├─ PlanValidator.validate
         └─ Dispatcher.try_admit
             ├─ IntentRegistry.compatible
             ├─ BudgetManager.reserve
             └─ Scheduler.launch
                 └─ AgentRuntime.run
                     ├─ ContextAssembler.read
                     │   ├─ EventLog.replay
                     │   └─ SubscriptionRegistry.add
                     ├─ ModelGateway.infer
                     ├─ (per action) AgentRuntime.guard
                     │   ├─ BudgetManager.check
                     │   ├─ DurableTimer.expired
                     │   ├─ IntentRegistry.renew（intents.renew）  ← v1.10 修复
                     │   └─ RunCoordinator.is_cancelled
                     ├─ ToolGateway.invoke
                     │   ├─ PolicyEngine.check_capability（CapabilityToken 校验）  ← v1.10 修复
                     │   ├─ EffectJournal.begin
                     │   ├─ EffectJournal.confirm
                     │   └─ EventLog.append_in_txn(EffectEvent)
                     ├─ WorkspaceManager.stage
                     │   └─ WorkspaceVersionStore（写前像）  ← v1.10 修复
                     ├─ commit_batch（自由函数，非 WorkspaceManager 方法）  ← v1.10 修复
                     │   ├─ IntentRegistry.reserve
                     │   ├─ WorkspaceManager.prepare
                     │   ├─ WorkspaceManager.apply
                     │   └─ (fail) compensate
                     │       └─ commit_batch(mode=COMPENSATE)
                     ├─ ArtifactStore.put（FINISH 时落产物）  ← v1.10 修复
                     ├─ VerifierRunner.validate
                     └─ CheckpointStore.save
                         └─ (return) RunCoordinator.finalize_or_replan
                             ├─ Aggregator.reduce
                             ├─ VerifierRunner.verify
                             │   └─ (fail) Replanner.submit
                             │       └─ PlanValidator.validate（PlanPatch）  ← v1.9 修复
                             │           └─ Planner.plan（PlanPatch）
                             └─ ReconcileLoop（周期）
```

### 20.5 扇入扇出图（Fan-in / Fan-out）

```
【扇出：一个组件调用多个下游】
  AgentRuntime ─┬─▶ ContextAssembler
                ├─▶ ModelGateway
                ├─▶ ToolGateway
                ├─▶ WorkspaceManager
                ├─▶ IntentRegistry（guard: intents.renew）
                ├─▶ EventLog
                ├─▶ CheckpointStore
                ├─▶ VerifierRunner
                └─▶ RunCoordinator        扇出 = 9

  commit_batch（自由函数，非 WorkspaceManager 方法） ─┬─▶ IntentRegistry.reserve
                                 ├─▶ WorkspaceManager.prepare
                                 ├─▶ WorkspaceManager.apply
                                 ├─▶ EventLog.append_in_txn（WorkspaceEvent）
                                 └─▶ compensate（失败时）  扇出 = 5

  Dispatcher.try_admit ─┬─▶ IntentRegistry.compatible
                        ├─▶ BudgetManager.reserve
                        ├─▶ Scheduler.launch
                        └─▶ (事件) EventLog（ADMIT_*）  扇出 = 4

  ApplicabilityGate.check ─▶ Planner.plan                    扇出 = 1

  ContextAssembler.read ─┬─▶ EventLog.replay
                         └─▶ SubscriptionRegistry.add        扇出 = 2

  Replanner.submit ─┬─▶ PlanValidator.validate
                    └─▶ Planner.plan（PlanPatch）            扇出 = 2

  RunCoordinator.finalize_or_replan ─┬─▶ Aggregator.reduce
                                    ├─▶ VerifierRunner.verify
                                    └─▶ CheckpointStore.save   扇出 = 3

  Aggregator.reduce ─▶ VerifierRunner.verify                扇出 = 1

  ToolGateway.invoke ─┬─▶ PolicyEngine.check_capability（CapabilityToken 校验）
                      ├─▶ EffectJournal.begin
                      ├─▶ EffectJournal.confirm
                      └─▶ EventLog.append_in_txn（EffectEvent）  扇出 = 4

  EffectJournal / SubscriptionRegistry（无扇出，纯被调）       扇出 = 0

【扇入：多个上游调用一个组件】
  EventLog ◀── WorkspaceManager.apply（WorkspaceEvent）
           ◀── ToolGateway.invoke（EffectEvent）
           ◀── RunCoordinator.finalize（TASK_COMPLETED）
           ◀── BudgetManager（BUDGET_* 事件）
           ◀── Dispatcher.try_admit（ADMIT_* 准入事件）
           ◀── Planner（PLAN_*）
           ◀── Replanner（PLAN_PATCH）
           ◀── HITL（APPROVAL_*）
           ◀── compensate（COMPENSATION_*）    扇入 = 9

  CheckpointStore.save ◀── AgentRuntime（每步 + HITL 挂起）
                       ◀── RunCoordinator（finalize）     扇入 = 2  ← v1.9 修复（去掉 Supervisor）

  BudgetManager.reserve ◀── Dispatcher.try_admit
                         ◀── AgentRuntime.guard    扇入 = 2

  IntentRegistry ◀── AgentRuntime.guard（renew）
                 ◀── commit_batch（reserve）
                 ◀── Dispatcher.try_admit（compatible）    扇入 = 3

  EffectJournal ◀── ToolGateway.invoke（begin/confirm）    扇入 = 1

  SubscriptionRegistry ◀── ContextAssembler.read（add）    扇入 = 1

  VerifierRunner ◀── AgentRuntime（contract.validate）
                 ◀── Aggregator.reduce（verify）
                 ◀── RunCoordinator.finalize_or_replan（verify）    扇入 = 3

【扇入扇出矩阵（组件 × 组件）】
> **v1.10 修复**：矩阵扩到 §20.4 调用图的全部 21 个组件（补齐 ApplicabilityGate / ContextAssembler / SubscriptionRegistry / EffectJournal / Aggregator / Replanner）。`1` = 存在调用边，`-` = 自身。

```
              Ingress ApplicabilityGate Planner PlanValidator Dispatcher Scheduler AgentRuntime ContextAssembler ModelGateway ToolGateway WorkspaceManager IntentRegistry SubscriptionRegistry BudgetManager EventLog CheckpointStore VerifierRunner RunCoordinator Aggregator Replanner EffectJournal
Ingress         -      1                0        0            0          0           0            0                0             0            0               0              0                 0             0        0             0              0           0        0         0
ApplicabilityGate 0     -                1        0            0          0           0            0                0             0            0               0              0                 0             0        0             0              0           0        0         0
Planner         0      0                -        1            1          0           0            0                0             0            0               0              0                 0             0        0             0              0           0        0         0
PlanValidator   0      0                0        -            0          0           0            0                0             0            0               0              0                 0             0        0             0              0           0        0         0
Dispatcher      0      0                0        0            -          1           0            0                0             0            0               1              0                 1             1        0             0              0           0        0         0
Scheduler       0      0                0        0            0          -           1            0                0             0            0               0              0                 0             0        0             0              0           0        0         0
AgentRuntime    0      0                0        0            0          0           -            1                1             1            1               1              0                 0             1        1             1              1           0        0         0
ContextAssembler 0     0                0        0            0          0           0            -                0             0            0               0              1                 0             1        0             0              0           0        0         0
ModelGateway    0      0                0        0            0          0           0            0                -             0            0               0              0                 0             0        0             0              0           0        0         0
ToolGateway     0      0                0        0            0          0           0            0                0             -            0               0              0                 0             1        0             0              0           0        0         1
WorkspaceMgr    0      0                0        0            0          0           0            0                0             0            -               1              0                 0             1        0             0              0           0        0         0
IntentRegistry  0      0                0        0            0          0           0            0                0             0            0               -              0                 0             0        0             0              0           0        0         0
SubscriptionRegistry 0 0                0        0            0          0           0            0                0             0            0               0              -                 0             0        0             0              0           0        0         0
BudgetManager   0      0                0        0            0          0           0            0                0             0            0               0              0                 -             1        0             0              0           0        0         0
EventLog        0      0                0        0            0          0           0            0                0             0            0               0              0                 0             -        0             0              0           0        0         0
CheckpointStore 0      0                0        0            0          0           0            0                0             0            0               0              0                 0             0        -             0              1           0        0         0
VerifierRunner  0      0                0        0            0          0           0            0                0             0            0               0              0                 0             0        0             -              0           0        1         0
RunCoordinator  0      0                0        0            0          0           0            0                0             0            0               0              0                 0             0        1             1              -           1        0         0
Aggregator      0      0                0        0            0          0           0            0                0             0            0               0              0                 0             0        0             1              0           -        0         0
Replanner       0      0                1        1            0          0           0            0                0             0            0               0              0                 0             0        0             0              0           0        -         0
EffectJournal   0      0                0        0            0          0           0            0                0             0            0               0              0                 0             0        0             0              0           0        0         -
```
> **注（v1.10 修复）**：`AgentRuntime → IntentRegistry = 1`（§5.3 guard 调用 `intents.renew`）。事件归属：`Dispatcher → EventLog` = `ADMIT_*` 准入事件；`BudgetManager → EventLog` = `BUDGET_*` 事件；`WorkspaceManager → EventLog` = `WorkspaceEvent`；`ToolGateway → EventLog` = `EffectEvent`。
```

### 20.6 事件流图（EventLog 作为变更流）

```
[WorkspaceManager.apply] ─┐
[ToolGateway.invoke]     ─┤
[RunCoordinator]         ─┼─▶ EventLog.append_in_txn ─▶ 全局 seq
[BudgetManager]          ─┤         │
[Planner/Replanner]      ─┤         ├─▶ SubscriptionRegistry ─▶ 订阅者失效
[HITL]                   ─┘         ├─▶ ContextAssembler ─▶ 物化视图（增量）
[compensate]             ──────────▶│
                                    ├─▶ ReconcileLoop ─▶ Task 状态对账
                                    └─▶ (保留窗口外) 快照
```

### 20.7 崩溃恢复流图

```
[崩溃]
  │
  ▼
Supervisor：心跳超时 → revoke(lease)
  │
  ▼
CheckpointStore.load(ref)
  │  （含 turn_seq / staged / cursor / effect_journal_offset / aging_deadline）
  ▼
EffectJournal.scan
  ├─ CONFIRMED ─▶ 跳过
  ├─ UNKNOWN  ─▶ 反查外部系统
  │              ├─ 未发生 ─▶ 用 idem_key 重执行
  │              ├─ 已发生 ─▶ 跳过
  │              └─ 不支持反查 ─▶ 升级人工
  └─ EXECUTING ─▶ 同上（UNKNOWN）
  │
  ▼
ReconcileLoop.rebuild_ready_set（从 PlanStore 重建）
  │
  ▼
AgentRuntime.resume（turn_seq 续跑）
```

### 20.8 隔离与数据流（Z3 worktree → Z2 共享态）

```
[Z3 per-agent worktree]                    [Z2 共享态]
  ┌─────────────────┐                      ┌─────────────────┐
  │ Agent A 本地写   │                      │  resources      │
  │ Agent B 本地写   │   stage()（写 Z3 worktree，v1.9 修复）│ (version) │
  │ Agent C 本地写   │ ───────────────▶     │                 │
  └─────────────────┘                      └────────▲────────┘
                                                  │
                                    prepare/apply（merge 点）
                                                  │
                                    ┌─────────────┴─────────────┐
                                    │ base_version == version?  │
                                    │  ├─ 是 ─▶ atomic_replace  │
                                    │  └─ 否 ─▶ CONFLICT        │
                                    │            └─ 3-way merge │
                                    └───────────────────────────┘
  [Z1 只读]  ── 无锁读 + 版本校验（不阻塞）
  [Z0 内核]  ── 调度/锁表/事件日志/ACB 表
```

### 20.9 扇出与背压（credit-based）

```
[Scheduler] ──▶ ready_queue（深度上限 MAX_READY）
                    │ 满 ─▶ 背压到准入层（暂停 launch）
                    ▼
              [AgentRuntime × N]

[EventLog] ──▶ SubscriptionRegistry ──▶ per-subscriber cursor
                    │ 落后过多 ─▶ 降级为快照重放（不丢弃事件，v1.9 修复）
                    ▼
              [ContextAssembler]
```

### 20.10 状态机总图（任务级 + Run 级）

```
任务级（表 A）：
PLANNED ─try_admit─▶ ADMITTED ─instantiate─▶ INSTANTIATING ─spawn─▶ READY ─schedule─▶ RUNNING
   │                    │                          │                     │
   │                    ▼                          ▼                     ▼
   │                 (失败)                     (失败)              WAITING_APPROVAL / REBASING / RETRYING / QUARANTINED
   │                                                                          │
   ▼                                                                          ▼
INADMISSIBLE ◀──aging──                                                     DONE / FAILED / CANCELLED / DEAD_LETTER
   │
   └──escalate──▶ REPLANNING ─新 Plan─▶ PLANNED（新任务）

Run 级（表 B）：
NEW ─规划─▶ PLANNING ─完成─▶ RUNNING ─全部 DONE─▶ DONE
                                  ├─部分失败─▶ DONE_PARTIAL
                                  ├─全部失败─▶ DONE_PARTIAL
                                  ├─预算耗尽─▶ DONE_PARTIAL
                                  └─取消─▶ CANCELLED
```

### 20.11 组件依赖图（DAG）

```
Ingress → ApplicabilityGate → Planner → PlanValidator → Dispatcher → Scheduler
                                                                      ↓
                                            RunCoordinator ←──────────┘
                                                  ↓
                                    ReconcileLoop ←─┘（周期）
                                                  ↓
                              Aggregator → VerifierRunner → (fail) Replanner → Planner

AgentRuntime → ContextAssembler → ModelGateway
AgentRuntime → ToolGateway → EffectJournal
AgentRuntime → WorkspaceManager → IntentRegistry
AgentRuntime → CheckpointStore
AgentRuntime → VerifierRunner

WorkspaceManager → EventLog
ToolGateway → EventLog
EventLog → SubscriptionRegistry → ContextAssembler（物化）
```

### 20.12 数据一致性视图（一致性级别映射）

```
强 ────────────────────────────────────────────────────────────▶ 弱

线性一致(单资源)   顺序一致(分区内)   因果一致(逻辑依赖)   会话保证(per-agent)   最终一致
   │                  │                  │                  │                 │
   ▼                  ▼                  ▼                  ▼                 ▼
RSC apply          Z2 per-resource    logical_deps      cursor+快照重放     EventLog 保留窗口
(base_version CAS) 串行化             + Run 级校验       (读己之写/单调读)    (无新事件则收敛)
```

### 20.13 错误传播图（18 错误码 → 处理 → 结果态）

```
CONFLICT ─────────────▶ handle_commit_result ─▶ rebase（3-way merge → 失败回喂 LLM）─▶ 重试
REJECT_STALE ─────────▶ 重读 + 新租约重试
REJECT_STALE_READ ────▶ refresh + replay + rebase
REJECT_COMPENSATE ────▶ mark_pivot + journal（升级人工）
BUDGET_EXHAUSTED ─────▶ 准入拒绝 / 执行期 guard → FAILED
DEADLINE_EXCEEDED ────▶ FAILED
GUARD_VIOLATION ──────▶ FAILED + checkpoint
COMMIT_UNRECOVERABLE ▶ FAILED
APPROVAL_TIMEOUT ─────▶ FAILED(APPROVAL_TIMEOUT)
CANCELLED ────────────▶ RunCoordinator 广播 → 各状态 → CANCELLED
SEMANTIC_REJECTED ────▶ REBASING → Replanner
UPSTREAM_FAILED ──────▶ RETRYING（瞬态）→ 超限 QUARANTINED
DENIED_CAPABILITY ────▶ gateway 拒绝
NEEDS_APPROVAL ───────▶ WAITING_APPROVAL → resume → RUNNING（P0-4 已修）
INVALID_INPUT ────────▶ Ingress/PlanValidator 拒绝
OK/VALID ─────────────▶ 继续
```

### 20.14 并发竞争图（两 agent 同时写同一资源）

```
Agent A                WorkspaceManager           Agent B
  │                          │                       │
  │ prepare(R)               │                       │
  ├─────────────────────────▶│ lock(R) 校验 base=v1  │
  │◀─────────────────────────┤ VALID                 │
  │                          │                       │
  │ apply(R)                 │                       │ prepare(R)
  ├─────────────────────────▶│ lock(R) 原子替换      ├─────────────────────────▶ lock(R)
  │                          │ version: v1→v2        │   base_version=v1 ≠ v2
  │                          │ applied_hash=h(v2)    │◀── CONFLICT(v2, diff)
  │◀─────────────────────────┤ OK                    │
  │                          │                       │
  │                          │                       │ rebase（3-way merge）
  │                          │                       │ base=v1, ours=v2, theirs=staged
  │                          │                       │ → 新 staged → commit(base=v2)
  │                          │                       ├─────────────────────────▶ OK → v3
```

### 20.15 安全边界图

```
[用户] ──▶ [Ingress] ──信任边界──▶ [Planner/Dispatcher] ──▶ [AgentRuntime]
                                                              │
                                                    ┌─────────┴─────────┐
                                                    │   PolicyEngine    │ CapabilityToken
                                                    │  (能力强制)        │ 工具/路径/网络/数据分级
                                                    └─────────┬─────────┘
                                                              ▼
                                              [ToolGateway] ──▶ [外部工具/API]
                                              [Sandbox/gVisor] ◀─ 不可信代码执行
                                                    │
                                              [工作区 worktree] ◀─ 文件系统边界
```

### 20.16 预算树图（BudgetNode 嵌套约束）

```
                    [Run Root: limits=100]
                    reserved=0, consumed=0
                            │
        ┌───────────────────┼───────────────────┐
        ▼                   ▼                   ▼
   [T1: limits=40]     [T2: limits=40]     [T3: limits=20]
   reserved=15         reserved=15         reserved=10
   consumed=12         consumed=8          consumed=5
        │
   ┌────┴────┐
   ▼         ▼
[T1a:20]  [T1b:20]        I5b: sum(children.reserved) ≤ parent.remaining
   │
   └── remaining(parent) = limits − own_reserved − sum(children.reserved)
```

### 20.17 HITL 挂起/恢复时序图

```
Agent                       HITL                     Supervisor          DurableTimer
 │                            │                          │                    │
 │ REQUEST_HITL               │                          │                    │
 ├───────────────────────────▶│ raise(acb, action)       │                    │
 │ discard_staged             │                          │                    │
 │ release_all_locks          │                          │                    │
 │ state=WAITING_APPROVAL     │                          │                    │
 │ checkpoint.save            │                          │                    │
 │                            │                          │                    │ schedule(HITL_TIMEOUT)
 │ await wait_resume          │                          │                    │
 │                            │                          │                    │
 │◀─────── 用户批准 ──────────┤                          │                    │
 │ hitl_result = get_result   │                          │                    │
 │ state = RUNNING            │                          │                    │
 │ continue（重新 infer）      │                          │                    │
 │                            │                          │                    │
 │ (若超时)                    │                          │◀───── fire ────────┤
 │                            │                          │ FAILED(APPROVAL_TIMEOUT)
 │ (若取消 Signal)             │                          │ CANCELLED
```

### 20.18 rebase 算法图（3-way merge）

```
        base (v1)
       ╱          ╲
   ours (v2)    theirs (staged)
      │              │
      └──── diff3 ───┘
             │
     ┌───────┴────────┐
     ▼                ▼
 无重叠            有重叠
     │                │
     ▼                ▼
AUTO_MERGED     CONFLICT_MARKERS
(自动合并)            │
                     ▼
              回喂 LLM 重编辑（有界重试）
                     │
                     ▼ 超限
                 QUARANTINED → REPLANNING
```

### 20.19 事件保留窗口 / cursor / checkpoint 生命周期图

```
时间 ──────────────────────────────────────────────────────────▶
  │  事件流（全局 seq）
  ▼
  [100] [101] ... [RETENTION 下限] ... [tail]
    ▲                          ▲          ▲
    │                          │          │
  cursor(订阅者)            快照边界    tail
    │                          │
    │ 落后 ────────────────────┤
    ▼                          ▼
  从最近快照重放          快照不受 RETENTION 淘汰（挂起任务）
  （K ≤ RETENTION）
```

### 20.20 Z0–Z3 判定树图

```
资源 R
 ├─ 属于调度规则/元数据？ ──是──▶ [Z0 内核区]
 ├─ 会被多个 agent 写/共享？
 │    ├─ 是 ──▶ 正在被修改？ ──是──▶ [Z2 共享态]（RSC）
 │    │                     └─ 否（只读共享）──▶ [Z1 只读共享区]（无锁读 + 版本校验）
 │    └─ 否 ──▶ [Z3 私有 worktree]（无锁，单写者，不走 RSC）
```
> **v1.10 修复**：与 §3.2 对齐——判定依据是「是否被多个 agent 写/共享」：**写 + 共享 → Z2（RSC）**；**只读共享 → Z1**；**非共享 → Z3**。原图把「共享但非代码文件」误判为 Z3，已修正。

---

## 附录：修订记录

### v1.11 契约同步收口（相对 v1.10）

| 问题 | 修复 |
|---|---|
| **P0**：§5.4 存在两份冲突的 `commit_batch`（第一份无 `mode`，引用未定义变量） | 合并为一份，统一 `mode = COMMIT`（§5.4） |
| **P0**：补偿反写内容未接入 `staged`（`reverse` 算出后未写入，`apply` 仍写原前写内容 → 补偿失效） | `acb.staged[w.resource] = reverse`（§5.4） |
| **P0 残留**：§4.4 `apply` 缺 `mode` | 补 `apply(acb, w, lease, mode)`（§4.4） |
| P1：HITL resume 值无 ACB 字段、无进入 ctx 的路径 | §4.5 加 `hitl_result`；§5.5 `context_view` 加 `view.hitl` |
| P1：`BUDGET_EXHAUSTED` 结果态不一致（§4.4 `DONE_PARTIAL` vs §5.3 `FAILED`） | 统一为「task `FAILED(BUDGET_EXHAUSTED)`；Run 级 finalize 后 `DONE_PARTIAL`」 |
| P1：§20.1 组件归属错位 | 重画（P1 状态面 12 组件 / P2 执行面 4 组件 / P3 控制面 11 组件 / P5 接入 2 组件） |
| P1：§20.5 矩阵缺 6 组件 | 扩到 21×21；`AgentRuntime → IntentRegistry = 1`；扇出计数 6 → 9 |
| P1：§20.20 判定树与 §3.2 冲突 | 重画（写+共享→Z2；只读共享→Z1；非共享→Z3） |
| P1：§20.4 调用图缺边 | 补 `PolicyEngine` / `ArtifactStore` / `WorkspaceVersionStore` / `IntentRegistry.renew` |
| P1：分布式文档残留「去协调者 2PC」 | 改为「RSC = OCC + 租约 fencing + 短临界区 + Saga；**非 2PC**」 |

### v1.10 修复记录（相对 v1.9，来自「评审 + 推演」两路子智能体）

**评审：6.5/10（未达最终版）；推演：4 真实 0✅/4⚠️，13 对抗 4✅/7⚠️/2❌。**

| 类别 | 问题 | 修复 |
|---|---|---|
| **P0-1** | `ws.apply` 无 `mode`，补偿在 apply 半边仍被自身 RSC 拒绝（动摇原子性） | `ws.apply(acb, w, lease, mode)`；`COMPENSATE` 跳过 base_version/读集合校验 + 内容复验（§5.4） |
| **P0-2** | `logical_subgoal_id`（I7 幂等键基准）无数据结构落点 | §4.5 `Task` 增加字段；§7.3 `footprint` 增加；§8.1 定义复用规则 |
| **P0-3** | `propagate_failure` 会取消 remake 任务（重规划不收敛） | 增加 `plan_version` 隔离（只传播同版本后继）（§5.2④） |
| **P0-4** | HITL `resume` 值去向未定义 | ① 写 `acb.hitl_result` ② `continue` 重新 infer ③ staged 已 discard（§5.3） |
| **P0-5** | CRDT 采纳 vs 明确不采纳矛盾 | 改为「3-way merge（非 CRDT，冲突不收敛）」；删 §19 的 CRDT 行 |
| **P0-6** | 「去协调者 2PC」vs 不采纳 2PC 矛盾 | 澄清：RSC = OCC + 租约 fencing + 短临界区 + Saga；**非 2PC**（§19） |
| **P1** | §20.1 层级编号冲突 | 改用 P0–P5 + L1–L5 双标签 + 编号说明 |
| **P1** | §20「无死角」过度声明 | 改为「尽可能消除」+ 诚实声明 |
| **P1** | 缺 6 类图 | 补 §20.13–§20.20（错误传播/并发竞争/安全边界/预算树/HITL/rebase/保留窗口/Z0–Z3 判定树） |
| **P1** | 扇入扇出矩阵不完整 | 扩到 15 组件，与 §20.4 对齐 |
| **P1** | 图里组件正文未定义 | §4.2 补 HITL/ReconcileLoop/Command；§4.4 补接口 |
| **P1** | §20.2 把未决建议画成已采纳 | 标注 ★ 状态（建议/已采纳） |
| **P1** | 映射文档过度类比 | 逐条改述（LSM 来源/一致性级别/idem_key/Raft/一致性哈希/SSI/CRDT/PA-EL/令牌桶/熔断/SWIM/容器/credit） |
| **P1** | 11/13 对抗用例无推演 | 补齐 13 个用例的逐步推演表 + 跑通判定标准 + 不变量编号 |

### v1.9 新增（相对 v1.8，来自分布式调研 + 用例落盘）

| 新增 | 内容 |
|---|---|
| **§19 分布式对照** | RSC = OCC + Saga + （去协调者）2PC；EventLog = LSM/WAL；idem_key = Kafka 幂等生产者；Raft = 单写者；一致性哈希 = Z0–Z3；credit 背压；DLQ；Phi Accrual；gVisor 沙箱；ReconcileLoop = K8s level-triggered |
| **§20 无死角图集** | 12 张图：架构总图 / 端到端数据流 / RSC 时序 / 调用图 / 扇入扇出 / 事件流 / 崩溃恢复 / 隔离数据流 / 背压 / 状态机总图 / 组件依赖 DAG / 一致性视图 |
| **用例落盘** | [用例集](use-cases-and-adversarial-tests.md)：4 真实 + 13 对抗（可审计） |
| **映射文档** | [分布式 ↔ 多智能体映射](distributed-to-multiagent-mapping.md) |

### v1.8 修复记录（相对 v1.7，来自终验子智能体）

**终验结果：真实 4/4 ✅；对抗 10/13 ✅（3 个不完整：A1、DEAD_LETTER、level-triggered）→ 已全部收口。**

| 类别 | 问题 | 修复 |
|---|---|---|
| **P0-1** | `REJECT_COMPENSATE` 依赖 `w.applied_hash`，但 `ws.apply` 从未赋值 | `ws.apply` 中 `w.applied_hash = ws.content_hash(...)`（§5.4） |
| **P0-2** | Z2 定义自相矛盾（§0.1 契约/配置 vs §3.3 含代码文件） | 统一为 **Z2 = 共享态（worktree 并入后）**；Z3 = per-agent worktree（§0.1、§3.3） |
| **P0-3** | worktree 与 RSC 未桥接（冲突在合并期 vs 提交期 CAS） | 明确 **`ws.apply` = merge 点**；CAS 校验 worktree base；CONFLICT 后先 3-way merge 再提交；Z2 线性化（§3.3） |
| **P1-1** | reconcile 入队条件 `state==READY` 过窄，漏掉崩溃时未 launch 的任务 | 放宽为 `state ∈ {PLANNED, ADMITTED}`（§5.2.1） |
| **P1-2** | `DEAD_LETTER` 未入状态机；`propagate_failure` 集合不含它 | 入表 A 终态 + 补 propagate_failure + 必释放租约/预算（§5.9、§5.2④） |
| **P1-3** | §14 称 aging 改 timer，§5.2④ 仍轮次计数 | 改为 `durable_timer.expired(t.aging_deadline)`（§5.2④） |
| **P1-4** | `DurableTimer` 无接口契约、无重启触发者 | §4.4 补 `schedule/cancel/expired/fire_due`；Supervisor 轮询触发（§4.4、§9.3） |
| **P1-5** | §5.6「EventLog=侧通道」 vs §5.2.1「事件即真相」 | 统一措辞：对账时以事件为准（幂等纠正），恢复依据是 CheckpointStore（§5.6） |
| **P1-8** | `TASK_COMPLETED` 未在 Event.type 枚举 | 补入（§4.5） |

### v1.7 修复记录（相对 v1.6，来自文档推演 + 数据流复审）

**文档推演（4 真实 + 13 对抗）结果：4/4 真实跑通、12/13 对抗跑通；仅剩 A1 补偿交叉 + 1 处文档矛盾。**

| 类别 | 问题 | 修复 |
|---|---|---|
| **P0（A1）** | 补偿交叉：`mode=COMPENSATE` 跳过基线校验 → 可静默覆盖并发提交 | 补偿反写前**内容复验**（`REJECT_COMPENSATE`）；期间被改则升级 `PIVOT`/人工（§5.4、§4.4） |
| **P0（数据流）** | 无 level-triggered 对账 → 崩溃后后继任务永久卡死 | 新增 §5.2.1 `ReconcileLoop`（从 PlanStore 重建 ready 集；事件即真相） |
| **P0（数据流）** | EventLog 定位不清（12 类事件仅 2 类有产生者） | 明确 **EventLog = 侧通道**；唯一恢复依据 = `CheckpointStore`；补齐其余事件产生者（§5.6） |
| **P0（数据流）** | EffectEvent 原子性弱于 WorkspaceEvent，I8 并列声明 | I8 明确为「资源提交↔WorkspaceEvent 同事务；effect 确认↔EffectEvent 同事务」（§5.6） |
| **P0** | Durable timer 缺失，与「跨月 HITL」冲突 | 一等 `DurableTimer`（存 PlanStore）；`aging` 改为基于 timer（§9.3、§14） |
| **P0** | 隔离模型未定义（共享 FS vs worktree） | 明确 **Z3 = per-agent overlay/worktree；Z2 = 提交层仲裁的共享资源**（§0.1、§14） |
| **P0** | 目标承诺过载（「像数据库事务一样可靠」） | 明确承诺边界：**机械一致性**，不含语义正确（§0.1） |
| **P0** | 「确定性原语」与「近似重放」矛盾 | 降为 P1；说明重放同一 decision，`ctx.time/uuid/random` 仅用于日志/缓存键（§14） |
| P1 | 文档内部矛盾：§10 D 的 `idem_key` 含 task_id | 改为 `hash(run_id, logical_subgoal_id, hash(tool, hash(args)))`（§10） |
| P1 | 无背压 | `ready_queue` 深度上限；per-subscriber 游标 + 最大在途（§5.6） |
| P1 | 无 DLQ | `QUARANTINED` aging + 强制预算释放 → `DEAD_LETTER` 终态（§5.6） |
| P1 | Task 状态 5 写者 vs B1 单一写入者 | 明确 `RunCoordinator` 唯一写者；其余走命令接口（§4.3 B1） |

### v1.6 补齐记录（相对 v1.5，来自用例推演与对抗测试）

| 缺口 | 补齐 |
|---|---|
| 语义 rebase 是「空心机制」 | 新增 §7.2.1：文本 3-way merge + JSON-aware merge + pivot 三档 |
| 共享配置 additive 改动整文件冲突 | §7.2.1 B：JSON-aware merge（数组并集去重） |
| import-graph 写偏斜未纳入 footprint | §7.3：footprint 增加 `logical_deps`（CONTRACT/INVARIANT/TYPE_GRAPH） |
| Replanner 输入不足 | §8.1：`diagnose()` 结构化 → `PlanPatch` 映射规则 |
| 缺接口契约 | §4.4：全部组件签名 + 错误码枚举 |
| 缺数据结构定义 | §4.5：Event/ACB/Plan/Task/ResultEnvelope/Diagnosis |
| 缺 L2 规范四件套 | §18：资源模型 / 协调协议（GRANT/WAIT/DIE）/ 收据格式 / conformance suite |

### v1.5 修复记录（相对 v1.4，来自三路子智能体：评审 + 真实用例推演 + 对抗式测试）

| 类别 | 问题 | 修复 |
|---|---|---|
| **P0-2** | **串行化点自相矛盾**：「提交层裁决」4 处 vs「准入层 fail-fast」5 处；`intents.compatible` 语义未定义 | 精确定义 `intents.compatible`（只拦契约/不变量与读-写方向，**不拦同资源写-写**）；§5.7/§7.4 改为「契约级准入 fail-fast，写-写留待提交层 CAS」（§5.2③⑤、§5.7、§7.4） |
| **P0-1** | **补偿原语不可执行**：嵌套 RSC 被自身校验拦住；外层租约未释放；提交前快照无存储 | 新增 `mode = COMPENSATE`（跳过 base_version/读集合校验）；补偿前释放外层租约；`ws.stage` 时存前像到 `WorkspaceVersionStore`；`compensation_journal` 崩溃安全（§5.4） |
| **P0-3** | **幂等键含 `task_id`** → 跨 replan 重复副作用 | 改为 `hash(run_id, logical_subgoal_id, hash(tool, hash(args)))`（§5.8） |
| **P0-4** | **预算公式自相矛盾** → 可双花 | `parent.remaining = limits − own_reserved − sum(children.reserved)`；`reserve` 单写者；`consumed ≤ reserved`（§9.1） |
| P1 | `REBASING→RUNNING` 无出处 | 改为「局部自愈 patch（同 turn 内）」；补齐非 RUNNING 状态的取消边（§5.9） |
| P1 | §5.3 短路 `return` 未设终态 | 补 `FAILED + COMMIT_UNRECOVERABLE`（§5.3） |
| P1 | §5.8「终态」列与 §5.9 冲突 | 表头改「结果态」+ 注明真正终态（§5.8） |
| P1 | 补偿提交失败无处理 | 重试 N → 审计事件 → 升级人工（§5.4） |
| P1 | 租约续期协议缺失 | 明确 `renew`/`revoke` 的原子顺序与 epoch 语义（§5.4） |
| P1 | HITL 挂起前 staged 写处置未定义 | 挂起前 `discard_staged`（显式丢弃）（§5.3） |
| P1 | 深层嵌套取消传播断裂 | 取消由 RunCoordinator **直接广播**到所有后代（§5.2⑦） |
| P1 | 缺 `RUNNING→FAILED`/`RETRYING→FAILED` | 补齐（§5.9） |
| P1 | `base_version` 未定义 | 定义 = `read_set[r]` 快照（§6.1） |
| P1 | `EffectEvent` 无产生路径 | `TOOL_CALL` 成功后追加（§5.3） |
| P1 | `finalize_or_replan` 误触发 | 判据改「no active」（§5.2④） |
| P1 | per-action guard 缺失 | action 循环内每 `GUARD_EVERY` 检查（§5.3） |
| P1 | 循环内 `verifier.pass` 与 §8 澄清冲突 | 循环内只做 `contract.validate`（§5.3、§8） |
| P1 | 逻辑不变量 write skew 无落点 | 不变量声明为 Contract Resource，纳入提交校验（§7.3） |
| P2 | Z1「永不失效」与提交期校验矛盾 | 澄清为「不主动失效，但提交期校验」（§3.1、§5.5） |
| P2 | 事件快照 cadence 与保留窗口冲突 | 强制 `K ≤ RETENTION`；挂起时强制落快照（§5.5） |
| P2 | 术语表漏收 | 扩充 §15（idem_key/lease/turn_seq/base_version/CapabilityToken 等） |
| P2 | 目录缺 §17 与附录 | 补齐（目录） |
| P2 | 缺 v1.3/v1.4 修订记录 | 补齐（本附录） |

### v1.4 修复记录（相对 v1.3）

| 类别 | 问题 | 修复 |
|---|---|---|
| — | 回写计网对照结论 | 新增 §5.4 跨域对照动机 + §17 跨域对照（§5.4、§17） |

### v1.3 修复记录（相对 v1.2）

| 类别 | 问题 | 修复 |
|---|---|---|
| **P0-1** | 状态机把任务状态与 Run 状态混在一张表，导致 `FAILED` 终态却有出边、缺多条边 | 拆成表 A（任务/agent）+ 表 B（Run）；补齐 `REBASING→REPLANNING`、`RETRYING→FAILED`、`RUNNING→FAILED`（§5.9） |
| P1 | L1–L5 与 Agent OS 栈层编号碰撞 | 加正交说明（§0.5） |
| P1 | §13.1 编排仍标 ✅➕、§13.3 含 L4/L5 组件 | 编排降级为「L4 参考实现」；§13.3 删除 L4/L5 项（§13.1、§13.3） |
| P2 | 版本引用陈旧（定位仍称 v1.1） | 统一为当前版本 |

### v1.2 修复记录（相对 v1.1，来自终审）

| 类别 | 问题 | 修复 |
|---|---|---|
| **P0-1** | `prepared` 集合混入「未 apply」的写，`compensate(prepared)` 会**过度回滚** | 改为累积 `applied`；`compensate(acb, applied)`（§5.4） |
| **P0-2** | 工作区资源**无补偿原语定义**，「all-or-nothing」未兑现 | 新增补偿原语（反向 patch + pivot 记录 + 补偿提交也走 RSC）（§5.4） |
| **P0-3** | fencing 过期 bump 会**误拒合法持有者** | 改为「按租约有效性」判定（`intents.is_valid(lease)`）；Supervisor 只 `revoke(lease)`（§5.4、场景 D） |
| **P0-4** | `turn_seq` 崩溃后重置 → `logical_action_id`/`idem_key` 不稳 → **I7 失效** | `turn_seq` 从 checkpoint 恢复并每 turn 持久化（§5.3） |
| **P1-1** | `apply` 未复验读集合 → 陈旧读 TOCTOU 残留 | `apply` 内复验读集合（§5.4） |
| **P1-2** | 场景 B「真并发验证」对 TOCTOU 机理描述与 §5.4 不符 | 修正描述（冲突在 apply 阶段发现）（§10 B） |
| **P1-4** | §5.3 `REQUEST_HITL` 伪代码缺 Signal 取消分支 | 补 `cancellation_requested` 分支（§5.3） |
| **P1-5** | `handle_commit_result` 后循环继续，可能把失败提交的任务 `finalize` 成 DONE | 失败即短路返回（§5.3） |
| 其他 | `advance_baseline` + `checkpoint.save` 在 FINISH/REBASING 分支被跳过 | return 前补调用（§5.3） |

### v1.1 修复记录（相对 v1.0）

| 类别 | 问题 | 修复 |
|---|---|---|
| P0 | `commit_batch` TOCTOU → 丢失更新 | validate 移入临界区；apply 锁内复验（§5.4） |
| P0 | `agent_loop` 忽略 `commit_batch` 返回值 | 必须处理返回码（§5.3） |
| P0 | `spawn_child` 契约缺失 | 补契约 + 预算切分 + 能力继承 + 取消级联（§5.2⑦） |
| P0 | HITL 等待不可中断 | `wait_resume` 支持 Signal 中断（§5.3、§9.3） |
| P0 | all-or-nothing 未兑现 | prepare + apply + 补偿（§5.4） |
| P1 | 状态机缺 `INADMISSIBLE`/`RETRYING→RUNNING`/`INSTANTIATING` 失败边 | 补全（§5.9） |
| P1 | `DONE_PARTIAL` 可达路径错误 | 由「预算耗尽」或「全部失败」到达（§5.9） |
| P1 | `on_task_complete` 不传播失败 | 补 `propagate_failure`（§5.2④） |
| P1 | 重规划无界 | 受重规划预算约束（§5.3、§10 H5） |
| P1 | `advance_baseline` 在 FINISH/REBASING 被跳过 | 移到循环体末尾（§5.3） |
| P1 | §13.2 与 §5.6/§5.8 自相矛盾 | 统一为「三分离 ✅」「重启强度 ✅」（§13.2） |
| P1 | §6.2 记忆分层 vs §13.1/§14 | 统一为「✅➕ 已定义」（§13.1、§14） |
| P1 | VerifierRunner 平面冲突 | 统一为 P2（§4.1、§4.2） |
| P1 | §4.1 组件在 §4.2 无定义 | 补齐 Renderer/RunCoordinator/各 Store（§4.2） |
| P1 | 工作区 replace 与事件 append 非原子 | 同事务（WAL）（§5.4） |
| P1 | 事件保留与 cursor 冲突 | 保留窗口 + 快照 + cursor 下限（§5.5、§11 I11） |
| P1 | `idem_key` 脆弱 + UNKNOWN 反查假设 | 明确反查失败则升级人工（§5.8） |
| P2 | 术语不统一 | `logical_action_id`、`EventLog`、`cursor`、`RSC` 统一（§5、§15） |
| P2 | `aging` 语义未定义 | 明确按轮次计数（§5.2④） |

---

*本规范为语言无关架构设计，是唯一权威文档，已通过 8 个样例场景实测验证。落地时需结合具体运行时补充实现细节。*
