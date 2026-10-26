# 定位与路线建议：多智能体并发正确性层

**从操作系统与 harness 的设计中学习，明确我们要做什么、不做什么、怎么做**

| 项目 | 内容 |
|---|---|
| 版本 | **v2.0（含 agent OS 判定 + 交叉对齐 + 终审修订计划）** |
| 日期 | 2026-10-26 |
| 输入 | 4 个调研子智能体 + 3 个审查子智能体（交叉对齐 / agent OS 判定 / 终审） |
| 前置文档 | [多智能体 Harness 设计规范 v1.12](multi-agent-harness-design-final.md) |
| 结论 | **不是 Agent OS**，是「Agent OS 的并发/事务子系统」——即 **Agent Concurrency Correctness Layer** |

---

## 目录

- [0. 结论摘要](#0-结论摘要)
- [1. 定位声明](#1-定位声明)
- [2. 现有相关素材盘点](#2-现有相关素材盘点)
- [3. 从 OS / 数据库学习](#3-从-os--数据库学习)
- [4. 从 harness 学习](#4-从-harness-学习)
- [5. 竞品与相邻产品](#5-竞品与相邻产品)
- [6. 路线建议](#6-路线建议)
- [7. 风险与红线](#7-风险与红线)
- [8. 术语表](#8-术语表)
- [9. 参考资料](#9-参考资料)
- [附录 A：Agent OS 判定](#附录-aagent-os-判定)
- [附录 B：v1.1 能力 ↔ 新定位 交叉对齐矩阵](#附录-bv11-能力--新定位-交叉对齐矩阵)
- [附录 C：终审修订计划](#附录-c终审修订计划)

---

## 0. 结论摘要

**一句话定位**：面向并行 coding agent 的**并发正确性层** —— 把数据库事务的成熟原语（预留/暂存/提交、fencing token、CAS、幂等对账、saga 补偿）适配到 LLM agent 的长事务与非确定性上，让多个 agent 并行修改同一代码库时保持可串行化正确性。

**四条硬结论**：

1. **赛道真实且无人占领**：agent 生成 PR 的合并冲突率实测 **27.67%**（142K+ PR / 59K+ 仓库）；跨 agent 冲突率 **41.7%**，是同一 agent 并行任务（19.8%）的两倍多。主流框架全部不做共享工作区并发控制。
2. **最接近我们的三个**：`agent-coherence`（开源，MESI+CAS+TLA+）、`Claim Plane`（学术，准入+fencing）、`CoAgent/MTPO`（学术，advisory 修复）。**「准入 + 幂等 + 预算 + 语义验证」四者合一 = 完全空白**。
3. **我们不是 Agent OS**（见[附录 A](#附录-aagent-os-判定)）：我们缺 L5 互联、缺跨机、缺完整用户态。我们是**Agent OS 的并发/事务子系统** —— 类比「Agent OS 的 WAL/TCP」。
4. **v1.1 需要修订**（见[附录 C](#附录-c终审修订计划)）：v1.1 的 P3/P5 平面（编排+接入）在新定位下是**孤儿**；有 6 个 P0 缺陷（含 3 个真实正确性 bug）、10 处直接冲突、~15 处缺口。

---

## 1. 定位声明

### 1.1 我们是什么 / 不是什么

| 是 | 不是 |
|---|---|
| 并发正确性层（中间件 / 库 / 规范 / 参考实现） | 通用多智能体编排框架 |
| 「agent 之间共享可变状态」的正确性保证 | agent 之间怎么连（图/角色/handoff） |
| 事务原语的 agent 化适配 | 又一个 LLM 应用开发框架 |
| **Agent OS 的一个子系统**（并发/事务内核） | **Agent OS 本身** |
| 可插在 LangGraph / CrewAI 之下的依赖 | 与它们正面竞争的产品 |

### 1.2 一句话定位

> **让多个 agent 并行修改同一份代码库时，像数据库事务一样可靠。**

**命名**：Agent Concurrency Correctness Layer（Agent 并发正确性层）；备选 Agent Transaction Layer。

### 1.3 目标用户（按优先级）

| 优先级 | 用户 | 为什么 |
|---|---|---|
| P0 | **agent-native VCS**（Oak / GitButler / Atlas / Diversion） | 已拥有历史层，冲突率是他们的核心卖点；我们是**互补品** |
| P0 | **MCP 生态**（Claude / Cursor / VS Code / Continue） | MCP 已是事实标准，分发最快 |
| P1 | **编排框架**（LangGraph / CrewAI / OpenAI SDK / AG2） | 分发渠道（被动接受，非主动集成） |
| P1 | **IDE 厂商**（Cursor / Windsurf / JetBrains） | 平台位：成为 IDE 默认并发层（对标 LSP） |
| P2 | **企业平台团队** | 内部 agent 舰队，merge gate 是瓶颈 |

### 1.4 切入场景（按推荐度）

| 场景 | 并发写强度 | 语义耦合 | 付费意愿 | 推荐度 |
|---|---|---|---|---|
| 批量重构 / 大规模迁移 | 高 | 高 | 高 | ⭐⭐⭐⭐⭐ |
| 并行代码审查（多 reviewer 改同一 PR） | 中 | 中 | 中高 | ⭐⭐⭐⭐ |
| 多 agent 测试生成 | 中 | 中 | 中 | ⭐⭐⭐ |
| 多 agent 文档/配置同步 | 低 | 低 | 低 | ⭐⭐ |

---

## 2. 现有相关素材盘点

### 2.1 学术论文（按与我们关系排序）

| 论文 | arXiv | 核心思想 | 关键指标 | 与我们的关系 |
|---|---|---|---|---|
| **CoAgent / MTPO** | 2606.15376 | 乐观并发 + advisory 修复 + saga 逆操作 | 10 负载：串行正确性 5% 内、**1.4× 加速**；2PL/OCC 几乎无收益 | **最直接竞争者**（不解决幂等/预算） |
| **Claim Plane** | 2607.21909 / 2608.00947 | 写前准入 + ChangeIntent + fencing token + fail-closed | 通过率 23.3%→50.0%；集成 65.6%→96.7%；**但 96.7% 被串行化** | **准入+fencing 最强参照** |
| **STORM** | 2605.20563 | 写时冲突检测 + 快照读 + 拒绝 payload | Commit0-Lite +18.7；优于 git-worktree | 互补（与我们 RSC 高度重合） |
| **AgentRoom** | 2608.23740 | CRDT 共享工作区 + 显式 claim 协议 | 2 agent 放弃任务比 solo 低 **13.7×**；「协调 > CRDT」 | 对照系 |
| **LIMBO** | 2609.29095 | 副作用 exactly-once 归属 | 25,930 episode；幂等键把重复率 **28%→4%**；**90% 重复 episode 中 agent 仍报成功** | **幂等对账权威依据 + 负面清单** |
| **Tracking State Footprints** | 2610.03140 | MAS 协调 = 数据管理问题 | vision 论文 | **问题陈述最佳引用** |
| **MAST** | 2503.13657 | 失败分类学（14 模式/3 类） | 失败率 41%–86.7%；inter-agent misalignment ≈37% | 可引用 |
| **MemTX** | 2607.23929 | 记忆写事务 + 不可逆动作门控 + 级联修复 | 550 万状态穷举机器验证 | 最接近的「记忆态并发正确性」论文 |
| **Governed Shared Memory** | 2606.24535 | fleet-memory 治理 | provenance 100% 重建；诚实负结果 | 治理维度模板 |
| **Semantic Consensus** | 2604.16339 | 语义意图分歧（SID） | 600 runs；**检测精度仅 27.9%** | 引其框架、避其精度陷阱 |
| **Agent Contracts** | 2601.08815 | 契约 + **预算守恒律** Σc ≤ B | token −90%、方差 −525× | 预算守恒律唯一形式化参照 |
| **Alignment Contracts** | 2605.00081 | 资源预算作为一等契约 + 可判定准入 | Lean 4 形式化 | 预算契约形式化蓝本 |
| **SagaLLM** | 2503.11951 | 全局验证 agent + 事务完整性 + 补偿 | 约束执行显著改善 | 独立验证 + 补偿的原型 |
| **AgenticFlict** | 2604.03551 | 多 agent 合并冲突数据集 | 142K+ PR；**冲突率 27.67%** | 评测集与基线事实基础 |
| **CAID** | 2603.21489 | 集中异步隔离委派（git 原语做协调） | PaperBench +26.7% | **worktree 集成最佳实践** |

**其他值得引用的**：Position: MAS Should Prioritize Concurrency Control（2608.18092）、TLA+ 并发异常目录（2606.17182）、CodeCRDT（2510.18893）、OptiMA（2511.03761）、S-Bus（2605.17076）。

### 2.2 开源项目

| 项目 | 语言/协议 | 解决什么 | 成熟度 | 集成方式 | 与我们的关系 |
|---|---|---|---|---|---|
| **agent-coherence** | Python；MESI + 乐观 CAS + read-generation fence | 静默 last-write-wins | 0.15.0（alpha）；**单主机**；**9 条 TLA+ 机器检验不变量** | LangGraph store drop-in（`CCSStore`）、CrewAI/AutoGen/OpenAI adapter、MCP server | **最强竞争者**；差异化须在幂等对账+预算+跨主机+语义验证 |
| **Klock** | Python + Rust 核心；Wait-Die + 租约 | MARC（多 agent 竞态条件） | 0.1.3；单一维护者 | LangChain `@klock_protected` 装饰器 | 竞争（工具层协调） |
| **agenthold** | Python；SQLite WAL；MCP | read-modify-write 冲突（OCC） | 0.10.0 | **纯 MCP**，多框架零胶水 | 竞争但小众（可作对照基线） |
| **HashPilot** | TypeScript/Bun；AST + SHA-256 锚定 | 单次编辑的正确性 | npm 4.8.4；424 tests | CLI + MCP | **互补**（编辑原语层） |
| **agent-ledger** | Python；幂等 + 审计账本 | 工具调用幂等 + 意图绑定审批 | 0.2.1 | 包装任意 async 工具 | 互补（幂等账本） |
| **AGLedger** | Node + Postgres；Ed25519 + hash-chain + in-toto | agent 行为公证（审计层） | Developer Edition 免费 | MCP + OpenAPI | 互补（审计收据） |

### 2.3 创业公司 / 产品

| 产品 | 定位 | 层级 | 规模证据 | 与我们的关系 |
|---|---|---|---|---|
| **Oak**（oak.space） | agentic substrate：mount 而非 clone、branch-per-task、拒绝直推 main | **历史/分支层** | 公开 beta；Apache-2.0 CLI；HN 首页 216 点 | **互补**（理念同源） |
| **Atlas**（pacifio） | source control for agents：checkpoint 绑定 session | 历史/分支层 + 轻量上下文 | GitHub ~8,100 star | 互补 |
| **GitButler** | virtual branches + worktree 替代 | 历史/分支层 | **$17M Series A**（a16z，Scott Chacon） | 互补 |
| **Diversion** | 云原生 VCS，独占文件锁 | 历史/分支层 | **累计 $12M**；15,000 开发者 | 竞争（目标域是二进制资产） |
| **GitMono / BrewFS** | agent 规模基础设施；多 agent 共享同一工作区的 Rust 文件系统 | 基础设施层 | 早期 | **最接近** |
| **8d / EightD** | 无锁 CRDT + Merkle-DAG 追踪 agent 推理 | 早期创业 | 4 star | CRDT 路线早期尝试 |

### 2.4 能力对比矩阵（关键空白）

横轴：RSC 提交 / 幂等对账 / 上下文一致性 / 预算树 / 语义验证 / worktree 集成 / CRDT / 准入控制

| 项目 | RSC | 幂等对账 | 上下文一致 | 预算树 | 语义验证 | worktree | CRDT | 准入控制 |
|---|---|---|---|---|---|---|---|---|
| CoAgent/MTPO | ✅ | ❌ | ✅ | ❌ | 🟡 | ❌ | ❌ | 🟡 |
| Claim Plane | 🟡 | ❌ | 🟡 | ❌ | 🟡 | ✅ | ❌ | ✅ |
| STORM | ✅ | ❌ | ✅ | ❌ | 🟡 | ❌ | ❌ | 🟡 |
| AgentRoom | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ | ✅ | 🟡 |
| LIMBO | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| MemTX | ✅ | ❌ | ✅ | ❌ | ✅ | ❌ | 🟡 | ✅ |
| Agent Contracts | ❌ | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ | ✅ |
| agent-coherence | 🟡 | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ | ✅ |
| Klock | ❌ | ❌ | 🟡 | ❌ | ❌ | ❌ | ❌ | ✅ |
| agenthold | ✅ | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ | 🟡 |
| **我们** | **✅** | **✅** | **✅** | **✅** | **✅** | **🟡** | **❌** | **✅** |

**三个空白带**：①「幂等对账」几乎只有 LIMBO + agent-ledger；②「预算树」只有 Agent Contracts / Alignment Contracts；③**「准入+幂等+预算+语义验证」四者合一 = 完全空白**。

---

## 3. 从 OS / 数据库学习

### 3.1 机制映射表（OS/DB ↔ 我们）

| OS/DB 机制 | 原系统解决什么 | 我们的对应问题 | 迁移形态 | 迁移难点 |
|---|---|---|---|---|
| **CAS on ref + 原子发布**（git update-ref / rename(2)） | 多写者更新共享状态，要么全生效要么不生效 | 多 agent 提交代码改动，需完整合入 | 提交层 CAS + 单次 ref 更新发布整棵 tree | 代码改动跨多文件，单文件原子 ≠ 多文件事务 |
| **fencing token**（Kleppmann/DDIA ch.9） | 锁过期后旧持有者「复活」双写 | agent 卡住 → 预留被回收给 B → A 恢复后覆盖 B | 预留颁发单调 token，提交层拒绝过期 token | agent「卡住」比 GC 暂停更极端 |
| **lease / 租约**（Chubby） | 粗粒度锁需知道持有者是否存活，过期无需通信 | 多 agent 抢同一文件写权 | 预留带 ttl + 心跳续期 + 过期自动释放 | ttl 太短误回收、太长拖慢恢复 |
| **MVCC 快照**（PostgreSQL） | 读不阻塞写，读到自洽快照 | agent 需自洽代码库快照作上下文 | 预留时物化不可变快照，钉住 baseRev | 物化有成本，需快照缓存 + 版本钉住 |
| **OCC / SSI read set 校验** | 无锁执行后检测 read set 是否被并发修改 | agent 基于快照改代码，提交时检测依赖是否被改 | 提交时校验 readSet 是否仍等于 base | **read set 难精确**；phantom 在代码里更严重 |
| **幂等 intentId + 去重表** | at-least-once 投递导致重复副作用 | agent 重试提交导致同一改动应用两次 | intentId 跨重试稳定，与副作用同事务 | agent 的「意图」是高层的 |
| **saga + 补偿**（Garcia-Molina & Salem） | 跨服务长事务无法原子提交 | 多 agent 协作长任务中途失败 | 逆序、幂等的语义回滚（revert commit） | 代码改动无法物理回滚；pivot 概念关键 |
| **write skew 防护**（SSI） | 读重叠集、写不重叠 → 非串行 | 两 agent 基于同快照改不同但语义相关的文件 | 依赖声明 + 提交后验证 + 补偿 | **最难**：不能靠锁根除 |
| **原子替换**（rename(2)） | 单文件更新要么全生效 | agent 重写文件时他人不能读到半截 | 强制工具走 tmpfile + fsync + rename | agent 常 in-place 写暴露半写 |
| **git 并发模型**（index.lock / CAS / rerere / ORIG_HEAD） | git 自身并发写 index/refs 的正确性 | 多 agent 共享 git 仓库 | 直接复用；rerere → 冲突解决记忆库 | — |
| **RCU / seqlock** | 读多写少，读者无锁 | 大量 agent 频繁读代码库 | 用**版本校验读**替代 RCU；或读缓存的不可变快照 | RCU 依赖单机 CPU 内存模型，**跨机不可用** |
| **CRDT** | 无中心、离线并发编辑保证收敛 | 多 agent 真正同时编辑同一文件 | **不作主机制**；仅窄场景 | 字符级 CRDT 收敛但**语义乱码** |

### 3.2 可迁移机制清单（按价值排序）

| # | 机制 | 来源 | 迁移形态 | 预期收益 | 难度 |
|---|---|---|---|---|---|
| 1 | CAS on ref + 原子发布 | git update-ref / rename | 提交层 CAS + 单次 ref 更新 | 极高 | 中 |
| 2 | fencing token | Kleppmann | 预留颁发单调 token，提交层拒绝过期 | 极高 | 中 |
| 3 | MVCC 快照 + 上下文物化 | PostgreSQL MVCC | 预留时物化不可变快照 | 高 | 低 |
| 4 | 幂等 intentId + 去重表 | idempotency key 实践 | intentId 跨重试稳定 | 高 | 低 |
| 5 | 暂存区（staging） | git index / WAL / tmpfile | agent 私有 worktree 累积 | 高 | 低 |
| 6 | OCC/SSI read set 校验 | OCC / PostgreSQL SSI | 提交时校验 readSet | 高 | 高 |
| 7 | lease + 心跳预留 | Chubby | 预留带 ttl，过期自动释放 | 高 | 中 |
| 8 | saga + 补偿 | Garcia-Molina & Salem | 逆序、幂等语义回滚 | 高 | 中高 |
| 9 | rerere / 冲突解决记忆 | git rerere | 记录冲突→方案，自动套用 | 中高 | 中 |
| 10 | 版本校验读（seqlock 式） | kernel seqlock | 读版本→读数据→再读版本 | 中 | 中 |

### 3.3 明确不可迁移的机制

| 机制 | 为何不可迁移 | 替代方案 |
|---|---|---|
| **2PC / 3PC** | 依赖「单一协调者 + 短事务」；agent 事务跨分钟、非确定、协调者崩溃即全局卡死 | **saga + 补偿** + **OCC/CAS 提交** |
| **RCU（硬件内存屏障）** | 依赖单机 CPU 内存模型 | **物化不可变快照** + **版本校验读** |
| **Mandatory file locking** | Linux 内核明确**不可靠** | **RSC 层 lease + CAS + fencing** |
| **严格 serializable（DB 语义）** | 需全局锁或谓词锁；agent read set 无法精确，且代码**可修补** | **路径级 CAS + 依赖声明 + 提交后验证 + 补偿** |
| **TrueTime / 时钟排序** | 依赖专用硬件 | **逻辑时钟 / 中心序列号 / git DAG** |
| **WAL 物理重放恢复** | agent 改动无法物理重放 | **补偿事务（语义回滚）** + **rerere 记忆** |
| **字符级 CRDT 用于代码** | 收敛 ≠ 语义正确 | **预留 + merge + 验证** |

### 3.4 关键洞察：必须人为制造「单一串行化点」

> **DB 的机制几乎都建立在「单一串行化点」上。迁移的关键不是把 DB 搬过来，而是人为制造一个串行化点 —— 也就是 RSC 的「提交层」。一旦有了这个点，2PC/OCC/CAS/fencing 都能落地；没有它，一切免谈。**

---

## 4. 从 harness 学习

### 4.1 四类 harness 的设计范式对照

| 范式 | 测试 harness | 训练 harness | workflow harness | agent harness | → 我们的形态 |
|---|---|---|---|---|---|
| **组织** | fixture / 测试函数 | LightningModule / worker | workflow / step | agent / tool / turn | **Run 实例 + Agent 定义** |
| **注入** | fixture 参数注入 | Strategy/Plugin | proxyActivities | 工具集 / prompt / MCP | **声明式依赖注入** |
| **隔离** | tmp_path / monkeypatch | worker 进程隔离 | namespace / 沙箱 | Docker 沙箱 / worktree | **每 agent 独立沙箱 + worktree** |
| **断言/验证** | assert + 断言内省 | metric 阈值 | step 结果校验 | finish action / 测试通过 | **可插拔 Verifier** |
| **恢复** | `--lf` / `--reruns` | checkpoint + resume | event history 重放 | session 持久化 | **durable step + checkpoint**（最大缺口） |
| **可观测** | junitxml | TensorBoard / logger | Web UI / Event History / SQL | event stream / tracing | **统一 trace + 可查询状态** |
| **配额** | `--test-threads` | `devices` / `ScalingConfig` | 并发限制 | turns / token / cost | **多级预算树** |
| **生命周期** | setup/teardown | fit/validate/test | start/pause/resume/cancel | session 创建/中断/恢复 | **完整状态机** |
| **扩展点** | pytest plugin / hook | callback / strategy | design pattern / Nexus | hooks / skills / MCP | **middleware + MCP** |
| **确定性** | 测试可重复 | `seed_everything` | workflow 必须确定 | 模型非确定，需显式管理 | **replay-safe 原语** |
| **幂等** | 测试可重复 | checkpoint 续跑不重复计步 | 逻辑日期幂等 | 会话可重连 | **副作用步必须带 idempotency key** |

### 4.2 两条「元范式」

- **元范式 A：harness 的价值 = 把「支撑机制」从被承载对象里剥离出来。** 我们的使命：把「多 agent 的协调、隔离、幂等、观测、限额」从 agent 业务逻辑里剥离出来。
- **元范式 B：被承载对象越「非确定 + 长生命周期」，harness 越需要「重放 + 补偿 + 可查询状态」。**

### 4.3 我们可能遗漏的 8 项（按风险排序）

1. **Durable step + 幂等键（最高风险）** —— 没有它，崩溃后从头重跑会**重复调用模型（烧钱）或重复执行副作用（重复扣款）**
2. **Compensation / Saga** —— 跨多个外部系统时必须逆序补偿
3. **确定性约束 + replay-safe 原语** —— `uuid` / `random` / `now` 必须有 seeded 版本
4. **Pause-and-fix / HITL** —— 区分「可自动重试」与「需人工修正」
5. **可查询的状态（Search Attributes / SQL）**
6. **Worker/Agent Versioning** —— 长跑 agent 会活过代码部署
7. **生命周期状态机** —— `running / waiting / paused / resumed / completed / failed`
8. **事件流即审计 + git 原生**

### 4.4 心智模型

> **多智能体 harness = agent harness 的「循环 + 工具 + 上下文 + 权限」，加上 workflow harness 的「持久化 + 重放 + 补偿 + 可查询」，再加上测试 harness 的「fixture + 断言 + 报告」，最后用训练 harness 的「checkpoint + 分层容错 + 资源配额」兜底。**

---

## 5. 竞品与相邻产品

### 5.1 三层玩家与空白

| 层 | 玩家 | 共识解法 | 留下的空白 |
|---|---|---|---|
| **历史/分支层** | Oak / Atlas / GitButler / Diversion / SproutGit / Cursor Origin | **isolation-first** | 运行时正确性（merge 之前） |
| **并发原语层** | agent-coherence / Klock / agenthold / HashPilot / agent-ledger | **单原语** + **单主机** | 跨主机 + 幂等 + 预算 + 语义验证 |
| **框架层** | LangGraph / CrewAI / OpenAI SDK / AG2 | **只做图内状态** | 图外资源（文件/DB/预算/副作用） |

**关键证据**：LangChain 官方论坛（2026-03-17），LangChain 团队成员 wfh 回复：「If you're trying to mutate an untracked object, modify an external db, etc., then you'll have to rely on that external untracked object's concurrency mechanism」；并承认「We've debated adding some more advanced channel types」。**CrewAI v1.14.7 把锁后端做成可插拔** —— 明确的「留缝」信号。

### 5.2 主流框架的集成点

| 框架 | 最佳插入点 | 插入形态 | 侵入度 |
|---|---|---|---|
| **LangGraph** | ①`wrap_tool_call` 中间件 ②自定义 store（对标 `CCSStore`）③自定义 checkpointer | 库依赖 + 自定义 state backend + MCP | 中 |
| **CrewAI** | ①`PRE_TOOL_CALL` 钩子（原生支持 `HookAborted`）②Memory scope 隔离 ③Flow 事件 | 库依赖 + MCP | 低-中 |
| **MS Agent Framework / SK** | ①Function calling middleware ②Agent Hooks（唯一跨三层）③Context provider ④流式门 | 库依赖（.NET/Python/Java）+ MCP | 中 |
| **OpenAI Agents SDK** | ①Sandbox Agents 的 `Manifest` + `cwd`（**最优雅的隔离原语**）②Tool guardrails | 库依赖 + MCP | 低-中 |
| **AutoGen / AG2** | ①`on_tool_execution` 中间件 ②`AssemblyPolicy`（**预算树完美落点**）③MemoryStream | 库依赖 + MCP | 中 |

**结论**：**集成形态已收敛为「MCP server + 框架 drop-in 适配器」双轨**。

### 5.3 框架作者视角

- **为什么不做**：① 抽象边界（图外资源属资源层）；② 原语不可通约；③ 非确定性是本质困难；④ 优先级。
- **会集成吗**：**被动接受而非主动集成**。**现实是「我们去适配他们」** —— `agent-coherence` 已为四个框架写好适配器。
- **策略**：做 **drop-in 适配器**，让集成成本低到框架无需决策。

### 5.4 反方观点（必须正视）

1. **「冲突率的锅在 prompt 不在 VCS」** —— 回应：任务切分能降频，但不能消除「共享工具文件/配置/预算」上的竞争。
2. **「96.1% 的冲突 merge commit 由人类解决」**（SBES 2026）—— **这恰恰强化我们的价值**：目标不是「更好的冲突解决」，而是**「在冲突发生前阻止，从而免除人类介入」**。

---

## 6. 路线建议

### 6.1 核心规律

> **「被集成」是采纳引擎，「被直接使用」是收入引擎。**

### 6.2 三段式路线

| 阶段 | 时间 | 形态 | 交付物 | 成功标志 |
|---|---|---|---|---|
| **一** | 0–12 月 | **被集成的专用库** | 零依赖原语库 + LangGraph/CrewAI/OpenAI SDK adapter + MCP server + 稳定 API + 基准 harness | 被 1–2 个框架/IDE 采用；有可复现基准 |
| **二** | 12–24 月 | **规范 + 参考实现** | 规范四件套（资源模型/协调协议/收据格式/conformance suite）+ 参考实现 | 规范被引用；conformance suite 成为准入门槛 |
| **三** | 24 月+ | **托管运行时** | 跨主机 fencing + 分布式租约 + 多租户 + Admin 控制面 | 有付费企业客户 |

### 6.3 规范应包含的四件套

1. **资源模型**：文件/目录/符号/键/预算节点/外部副作用的统一抽象
2. **协调协议**：声明意图 → 获取租约 → 执行 → 提交/释放（含 GRANT/WAIT/DIE 或 CAS 语义）
3. **收据格式**：签名 + hash-chain，可离线验证（对齐 IETF PoB 草案）
4. **一致性测试语料**：TLA+ 不变式 + 可执行 conformance suite（对标 LangGraph checkpointer conformance suite）

### 6.4 MVP 定义（阶段一）

**最小可行产品 = 一个能跑通「3 agent 并行改同一模块」的库 + 一个基准测试。**

| 组件 | MVP 范围 | 目标态（非 MVP） |
|---|---|---|
| 提交协议 | RSC（预留-暂存-提交 + fencing + CAS + 读集合校验） | 同左 |
| 幂等 | intentId 去重表 + UNKNOWN 对账 | 同左 |
| 上下文 | 物化快照 + 版本钉住 | 完整事件流 + 重放 + cursor |
| 预算 | 单层预算树 | 嵌套守恒（`sum(children.reserved) ≤ parent.remaining`） |
| 验证 | 调用测试/lint | 语义 rubric（LLM judge） |
| 集成 | LangGraph adapter + MCP server | 全框架 adapter |
| 基准 | 对标 git-worktree baseline，测冲突率/丢失更新率/并行加速比/token 成本 | 同左 |

> **注**：MVP 是目标态的**子集**，不是替代。v1.1 描述的是目标态；本表明确阶段一交付哪些。

### 6.5 阶段二 / 阶段三 MVP（v2.0 新增）

**阶段二 MVP（规范 + 参考实现）**：

| 交付物 | MVP 范围 |
|---|---|
| 资源模型 | 文件/目录/符号/键/外部副作用的统一抽象 + 形式化定义 |
| 协调协议 | 声明意图 → 获取租约 → 执行 → 提交/释放（含 GRANT/WAIT/DIE 状态） |
| 收据格式 | 签名 + hash-chain，可离线验证（对齐 IETF PoB） |
| conformance suite | 核心不变量（I1–I4、I7）的 TLA+ 模型 + 可执行测试 |
| 参考实现 | 单语言（Python）+ 与阶段一库兼容 |

**阶段三 MVP（托管运行时）**：

| 交付物 | MVP 范围 |
|---|---|
| 跨主机 fencing | 中心序列器（Raft/单写者）颁发单调 token |
| 分布式租约 | 跨主机租约 + 心跳 + 过期回收 |
| 多租户 | tenant 隔离 + 配额 |
| Admin 控制面 | pause / resume / kill / purge |
| 计量 | 按「动作/步骤」计费 |

**成功标志**：阶段二 = 规范被 ≥1 个第三方实现引用；阶段三 = ≥1 个付费企业客户。

---

## 7. 风险与红线

### 7.1 必须正视的风险

| 风险 | 说明 | 缓解 |
|---|---|---|
| **分解质量是天花板** | 若用户分解得好，worktree 就够了 | **必须在「分解不完美」时证明价值** |
| **语义冲突无解** | 锁/CAS 只能防文本级 lost update | 把语义验证做成独立层，明确**不承诺**解决语义冲突 |
| **单主机假设** | 现有并发原语均单机 | 规范预留分布式 fencing，实现分阶段 |
| **Claim Plane 教训** | 静态准入可靠但 96.7% 串行化 | 必须同时拿到可靠性与并行度（CoAgent 1.4×） |
| **UNKNOWN 对账是硬骨头** | LIMBO 证明「无 in-flight 时间上界时，纯验证策略无法 exactly-once」 | 明确对账边界 |
| **被框架吸收** | OpenAI Sandbox Agents 已占隔离位；AG2 已有 TokenBudgetPolicy | 差异化锁定「跨 agent 协调」与「幂等/审计」 |
| **赛道碎片化** | 5+ 项目各做一小块，无标准 | **这恰是机会：成为标准** |
| **成为 OS 的致命风险** | 需硬件级强制边界（无 MMU/特权级）+ 与 Agno/微软正面消耗战 | **永远不要走「成为 OS」那一段** |

### 7.2 红线（明确不做）

1. ❌ 不做通用编排图 / 状态机（LangGraph 的地盘）
2. ❌ 不做可观测 / trace / eval（已成入场券；审计收据归规范层）
3. ❌ 不做低代码画布（Flowise 已 EOL）
4. ❌ 不做单 agent harness（模型厂商锁死）
5. ❌ 不做角色/团队 DSL（CrewAI 已占位）
6. ❌ 不做「历史/分支层」（Oak/GitButler 已占位且融资充裕）
7. ❌ 不做纯隔离层（会被 OpenAI Sandbox Agents 吸收）
8. ❌ **不做 Agent OS 本体**（见附录 A）

---

## 8. 术语表

| 术语 | 定义 |
|---|---|
| 并发正确性层 | 保证多 agent 并发写共享状态正确的中间件/库/规范 |
| RSC | 预留-暂存-提交（Reserve-Stage-Commit） |
| fencing token | 单调令牌，拒绝旧持有者的写 |
| MARC | Multi-Agent Race Condition（多 agent 竞态条件） |
| state footprint | agent 读写了什么（跨上下文/编排器/外部系统） |
| conformance suite | 一致性测试语料，第三方实现必须通过 |
| isolation-first | 先隔离后合并的解法（worktree 派） |
| pivot | saga 中不可回退的「不归点」 |
| EffectJournal | 副作用幂等账本（对标 LIMBO） |
| BudgetNode | 预算树的节点（含 reserved/consumed/limits） |
| VerifierRunner | 独立上下文的验证器 |
| idem_key | 幂等键（稳定逻辑序号派生） |
| ACB | AgentControlBlock，agent 控制块 |

---

## 9. 参考资料

**学术论文**：CoAgent/MTPO [2606.15376](https://arxiv.org/pdf/2606.15376) · Claim Plane [2607.21909](https://arxiv.org/abs/2607.21909) · STORM [2605.20563](https://arxiv.org/html/2605.20563v1) · AgentRoom [2608.23740](https://arxiv.org/pdf/2608.23740) · LIMBO [2609.29095](https://arxiv.org/pdf/2609.29095) · State Footprints [2610.03140](https://arxiv.org/html/2610.03140v1) · MAST [2503.13657](https://arxiv.org/pdf/2503.13657v3.pdf) · MemTX [2607.23929](https://arxiv.org/html/2607.23929) · Governed Shared Memory [2606.24535](https://arxiv.org/html/2606.24535v1) · Semantic Consensus [2604.16339](https://arxiv.org/html/2604.16339) · Agent Contracts [2601.08815](https://arxiv.org/html/2601.08815v2) · Alignment Contracts [2605.00081](https://arxiv.org/pdf/2605.00081) · SagaLLM [2503.11951](https://arxiv.org/html/2503.11951v3) · AgenticFlict [2604.03551](https://arxiv.org/pdf/2604.03551v1) · CAID [2603.21489](https://arxiv.org/html/2603.21489v1) · TLA+ 异常目录 [2606.17182](https://arxiv.org/abs/2606.17182)

**开源项目**：[agent-coherence](https://pypi.org/project/agent-coherence/) · [Klock](https://klockcore.com/) · [agenthold](https://pypi.org/project/agenthold/) · [HashPilot](https://github.com/bigknoxy/HashPilot) · [agent-ledger](https://pypi.org/project/agent-ledger/) · [AGLedger](https://www.agledger.ai/)

**产品**：[Oak](https://oak.space/) · [GitButler](https://gitbutler.com/) · [Diversion](https://www.diversion.dev/) · [SproutGit](https://sproutgit.dev/)

**框架集成点**：[LangGraph 中间件](https://docs.langchain.com/oss/python/langchain/middleware/custom) · [CrewAI Tool Hooks](https://docs.crewai.com/v1.15.5/en/learn/tool-hooks.md) · [MS Agent Framework Middleware](https://learn.microsoft.com/en-us/agent-framework/concepts/agents/middleware/) · [OpenAI Agents SDK Guardrails](https://openai.github.io/openai-agents-python/guardrails/) · [AG2 Assembly](https://docs.ag2.ai/latest/docs/beta/advanced/assembly/)

**OS/DB 机制**：Kleppmann DDIA ch.7/9 · [How to do distributed locking](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html) · [PostgreSQL MVCC](https://www.postgresql.org/docs/14/mvcc-intro.html) · [PostgreSQL SSI](https://wiki.postgresql.org/wiki/SSI) · [Chubby OSDI'06](https://usenix.org/legacy/event/osdi06/tech/full_papers/burrows/burrows.pdf) · [git-update-ref](https://git-scm.com/docs/git-update-ref/2.50.0) · [git-rerere](https://git-scm.com/docs/git-rerere)

**harness**：[pytest fixtures](https://docs.pytest.org/en/6.2.x/fixture.html) · [JUnit 5](https://docs.junit.org/6.0.1/_exports/junit-user-guide-6.0.1.html) · [Lightning Trainer](https://lightning.ai/docs/pytorch/stable/common/trainer.html) · [Ray Train Fault Tolerance](https://docs.ray.io/en/latest/train/user-guides/fault-tolerance.html) · [Temporal Workflows](https://docs.temporal.io/workflows) · [Restate Durable Steps](https://docs.restate.dev/develop/python/durable-steps) · [DBOS Architecture](https://docs.dbos.dev/architecture) · [Claude Code Agent Loop](https://code.claude.com/docs/en/agent-sdk/agent-loop) · [OpenHands](https://arxiv.org/html/2407.16741v3/) · [SWE-agent](https://arxiv.org/pdf/2405.15793v3.pdf) · [Aider repo map](https://aider.chat/2023/10/22/repomap.html)

---

## 附录 A：Agent OS 判定

### A.1 什么是 Agent OS

**【事实】**「LLM 即操作系统」由学术界先于工业界提出：MemGPT（2023-10，[arXiv:2310.08560](https://arxiv.org/abs/2310.08560)，虚拟上下文管理）→ AIOS（2024-03，[arXiv:2403.16971](https://arxiv.org/pdf/2403.16971)，LLM 实例封装成 CPU core）→ AgentOS（2026-03，[arXiv:2603.08938](https://arxiv.org/html/2603.08938v1)，Agent Kernel）→ AOS（2026-06，[arXiv:2606.01508](https://arxiv.org/pdf/2606.01508v1.pdf)，最严谨定义）→ AgentKernel（2026-08，[arXiv:2609.29647](https://arxiv.org/pdf/2609.29647.pdf)，trust-native）。工业界：Agno AgentOS（生产运行时）、Letta（记忆 OS）、微软 `agent_os_kernel` + UFO2、MemOS（[arXiv:2507.03724](https://arxiv.org/abs/2507.03724)）、Agent-OSI 六层（[arXiv:2602.13795](https://arxiv.org/html/2602.13795v1)）。

**五层共识栈**：L0 硬件/隔离基质 · L1 内核（调度+强制边界+生命周期）· L2 运行时+记忆层次 · L3 用户态（工具/技能）· L4 治理与可观测 · L5 互联/分布式。

### A.2 我们是什么

| Agent OS 层 | 我们的对应 | 覆盖度 |
|---|---|---|
| L0 隔离基质 | P0 沙箱/overlay（v1.1） | 🟡 部分 |
| L1 内核 | IntentRegistry/WorkspaceManager/EventLog（**仅单机**） | ✅ 强（单调度器） |
| L2 运行时+记忆 | ContextAssembler/CheckpointStore/EventLog | ✅ 强 |
| L3 用户态 | ToolGateway（缺 MCP 对接） | 🟡 部分 |
| L4 治理 | BudgetManager/PolicyEngine/Trace | ✅ 强 |
| L5 互联 | **无** | ❌ 缺失 |

### A.3 判定

> ### **我们不是 Agent OS。我们是「Agent OS 的并发/事务子系统」——即 Agent Kernel 里的一个可插拔组件。**

三条理由：① 差异化全部锁定在 L1 的一个子问题（并发写共享状态的正确性）；② 缺 L5、缺跨机、L1 的「不可绕过」只在单机成立；③ 主动放弃 5 个 OS 必备层（编排/可观测/单 agent harness/历史层/纯隔离）。

### A.4 最贴切的类比：Agent OS 的 WAL / TCP

| 子系统 | 所属大系统 | 独立正确性判据 | 可被任何上层替换 |
|---|---|---|---|
| WAL | 数据库 | 崩溃后可恢复 | ✅ |
| TCP | 互联网 | 可靠有序 | ✅ |
| RCU | Linux 内核 | 读多写少无锁读 | ✅ |
| **我们** | **Agent OS** | **单资源线性化 + 无丢失更新 + 副作用有效一次** | **✅（可插在 LangGraph/CrewAI 之下）** |

### A.5 若要做 OS，缺什么

| 层 | 缺口 | 优先级 |
|---|---|---|
| L1 | 分布式 fencing、共识层、RPC、**真正的不可绕过边界** | 🔴 P0 |
| L2 | 确定性原语、durable timer、procfs 式内省、历史压缩 | 🟡 P0–P1 |
| L3 | MCP 对接、技能/工具注册表 | 🟡 P2 |
| L4 | 定义版本化、可查询状态、timeline/metrics | 🟡 P1 |
| L5 | A2A 端点、跨组织联邦、结算、agent 迁移 | 🔴 P0 |

**建议：永远不要走「成为 OS」那一段**（需硬件级强制边界 + 与 Agno/微软正面消耗战 + 无限维护面）。

---

## 附录 B：v1.3 能力 ↔ 新定位 交叉对齐矩阵

### B.1 分层归属表

> **编号说明**：本表的 **L1–L5 是「交付归属层」**（L1 核心 / L2 规范 / L3 托管运行时 / L4 编排适配 / L5 不交付），与附录 A 的 **Agent OS 栈层 L0–L5** 是**两套正交的编号**。

| v1.1 组件/能力 | v1.1 章节 | 归属 | 理由 |
|---|---|---|---|
| **Z0 内核区**（锁表/事件日志/审计） | §3.1 | **L1** | 「单一串行化点」的实现 |
| **Z1 只读共享区** | §3.1 | **L1** | 无锁读 + 版本校验 |
| **Z2 可读写共享区** | §3.1 | **L1** | RSC 唯一作用域（核心） |
| **Z3 私有工作区** | §3.1 | **L1**（≠隔离产品） | 是 staging，不是隔离卖点 |
| **P1 状态面**（EventLog/各 Store/IntentRegistry/EffectJournal/CheckpointStore） | §4.1 | **L1** | 正确性地基 |
| **WorkspaceManager** | §4.2 | **L1** | RSC 执行点 |
| **IntentRegistry** | §4.2 | **L1** | 预留/租约/fencing |
| **EffectJournal** | §4.2 | **L1** | 幂等对账 |
| **VerifierRunner** | §4.2 | **L1** | 独立验证（差异化之一） |
| **BudgetManager** | §4.2 | **L1** | 预算守恒 |
| **ArtifactStore / WorkspaceVersionStore** | §4.2 | **L1** | 版本引用 |
| **RSC 提交协议 / fencing / 读集合校验 / saga** | §5.4 | **L1** | 核心协议 |
| **资源提交状态机 / effect 状态机** | §5.4/§5.8 | **L1** | 正确性核心 |
| **ContextAssembler（基线/版本钉住）** | §4.2 | **L1** | 物化快照 |
| **ToolGateway（幂等/effect 钩子）** | §4.2 | **L1** | 副作用幂等 |
| **ModelGateway（replay-safe 原语）** | §14 | **L1** | 确定性原语 |
| **SubscriptionRegistry（读集追踪）** | §4.2 | **L1** | read-set 校验 |
| **PolicyEngine（能力强制）** | §4.2 | **L1** | I4 能力封闭 |
| **Dispatcher（intent 注册/准入）** | §4.2 | **L1** | 对标 Claim Plane |
| **Supervisor（租约推进）** | §4.2 | **L1** | fencing 推进 |
| **ApplicabilityGate** | §4.2 | **L4**（或删） | 编排决策 |
| **Planner / PlanValidator（DAG 部分）** | §4.2 | **L4**（footprint 留 L1） | 任务分解是编排 |
| **Dispatcher（实例化）/ Scheduler / Replanner / Aggregator / RunCoordinator** | §4.2 | **L4** | 编排 |
| **Ingress / Renderer** | §4.2 | **L4** | 接入面/渲染 |
| **AgentRuntime（单 agent 循环）** | §4.2 | **L4**（adapter 钩子） | 红线 4 |
| **spawn_child 嵌套派生** | §5.2⑦ | **L4** | 编排 |
| **HITL 审批台** | §9.3 | **L4** | 产品/编排 |
| **Trace/Audit（OTel span）** | §4.2/§9.2 | **L5**（不交付，只留接口契约） | 红线 2 |
| **SecretBroker** | §4.2 | **L5**（不交付，只留接口契约） | 平台能力 |
| **MemoryStore / §6.2 记忆分层** | §4.2/§6.2 | **L5**（不交付，只留接口契约） | 非并发正确性 |
| **收据格式 / conformance suite** | — | **L2**（新增） | 规范四件套 |
| **资源模型（符号/键/外部副作用）** | — | **L2**（补全） | ResourceSpec 仅 FILE/DIR/DB_ROW/API/CONTRACT/INVARIANT |
| **协调协议（GRANT/WAIT/DIE）** | — | **L2**（补全） | v1.1 只有 RSC |
| **跨主机 fencing / 分布式租约 / 多租户** | — | **L3**（新增） | §14 P0 缺口 |
| **TLA+ 不变量 / LangGraph store drop-in** | — | **L2**（新增） | 对标 agent-coherence |
| **exactly-once 对账边界** | — | **L2**（补全） | 对标 LIMBO |

### B.2 不变量覆盖表（I1–I14 → 阶段）

| 不变量 | 阶段一（库） | 阶段二（规范） | 阶段三（运行时） |
|---|---|---|---|
| I1 单写者提交 | ✅ | ✅ | ✅ |
| I2 无丢失更新 | ✅ | ✅ | ✅ |
| I3 fencing 单调 | ✅ | ✅ | ✅ |
| I4 能力封闭 | ✅ | ✅ | ✅ |
| I5 预算守恒 | 🟡 单层 | ✅ 嵌套 | ✅ |
| I6 上下文因果一致 | 🟡 物化快照 | ✅ 事件流 | ✅ |
| I7 effect 有效一次 | ✅ | ✅ 对账边界 | ✅ |
| I8 事件原子 | 🟡 | ✅ | ✅ |
| I9 计划有效 | — | — | — （编排层，L4） |
| I10 隔离（Z3 不可见） | ✅ | ✅ | ✅ |
| I11 有界 | ✅ | ✅ | ✅ |
| I12 终止/公平 | 🟡 | ✅ | ✅ |
| I13 可审计 | 🟡 收据 | ✅ | ✅ |
| I14 无进展终止 | 🟡 | ✅ | ✅ |

> **I9（计划有效）属编排层（L4），不在我们的交付范围**，但 footprint 声明部分在 L1。

---

## 附录 C：终审修订计划

终审发现 **6 个 P0**（含 3 个真实正确性缺陷）、10 个 P1、若干 P2。以下为修订清单。

### C.1 P0（必改）

| # | 问题 | 位置 | 修法 |
|---|---|---|---|
| **P0-1** | `prepared` 集合混入「未 apply」的写，`compensate(prepared)` 会**过度回滚** | v1.1 §5.4 | 改为累积 `applied` 列表；`compensate(acb, applied)` |
| **P0-2** | 工作区资源**无补偿原语定义**，「all-or-nothing」未兑现 | v1.1 §5.4 | 定义文件级补偿（反向 patch + 冲突处理），或降级承诺为「原子 + 尽力补偿」 |
| **P0-3** | fencing 过期 bump 会**误拒合法持有者** | v1.1 §5.4、场景 D | token 改为「按持有者发放」；bump 只作废被回收持有者 |
| **P0-4** | `turn_seq` 崩溃后重置 → `logical_action_id`/`idem_key` 不稳 → **I7 失效** | v1.1 §5.3/§5.8③ | checkpoint 持久化并恢复 `turn_seq`；或改用事件 seq 作幂等键基准 |
| **P0-5** | 定位路线未覆盖 I1–I14，且有 2 条冲突（I5/I8） | 定位 §6.2/§6.4 | 新增不变量覆盖表（见附录 B.2）；§6.4 改为与 §9.1 一致 |
| **P0-6** | 定位「不做通用编排」与设计 §4.1/§13.1「调度编排 ✅➕」**正面冲突** | 定位 §1.1/§7.2 vs v1.1 §4.1/§13.1 | 新增组件归属表（见附录 B.1）；设计 §13.1 编排项降级 |

### C.2 P1（应改）

| # | 问题 | 位置 |
|---|---|---|
| P1-1 | `apply` 未复验读集合 → 陈旧读 TOCTOU 残留 | v1.1 §5.4 |
| P1-2 | 场景 B「真并发验证」对 TOCTOU 机理描述与 §5.4 不符 | v1.1 §10 B |
| P1-3 | 状态机：`WAITING_TOOL` 不可达、`READY→WAITING_TOOL` 错边、缺 `RUNNING→DONE/DONE_PARTIAL`、`FAILED`/`QUARANTINED` 自称终态却有出边 | v1.1 §5.9 |
| P1-4 | §5.3 `REQUEST_HITL` 伪代码缺 Signal 取消分支 | v1.1 §5.3 |
| P1-5 | `handle_commit_result` 后循环继续，可能把失败提交的任务 `finalize` 为 DONE | v1.1 §5.3 |
| P1-6 | 设计缺「目标用户/场景/竞品/风险」专节 | v1.1 §0–§16 |
| P1-7 | 定位术语与设计术语几乎无交集（intentId/idem_key、conformance suite、EffectJournal） | 定位 §8 vs v1.1 §15 |
| P1-8 | 定位 §6.4「不做 LLM judge」与 §8「语义 rubric」张力 | 定位 §6.4；v1.1 §8 |
| P1-9 | 缺安全威胁模型 | 两份均缺 |
| P1-10 | 定位阶段二、三无交付物清单与 MVP | 定位 §6.2 |

### C.3 P2（打磨）

- 设计目录补「附录」条目；§4.2 补 P0 组件定义
- I5 补 `consumed ≤ reserved`；I11 措辞与 §5.5 快照重放对齐
- 快照重基机制未定义
- `QUARANTINED`/`REPLANNING` 的预算释放时机未定义
- 定位 §8 术语表扩充；加「目标读者」声明
- 两份文档去重（状态机只留 §5.9；失败语义只留 §5.8）
- 「治理」一词在两份文档含义不同，需消歧

### C.4 本次 v2.0 已处理

- ✅ 新增附录 A（Agent OS 判定）
- ✅ 新增附录 B（交叉对齐矩阵 + 不变量覆盖表）
- ✅ 修正定位内部冲突：预算 MVP（§6.4 明确为「目标态子集」）、事件流、trace、eval
- ✅ §7.2 红线新增第 8 条「不做 Agent OS 本体」
- ✅ §0 结论摘要纳入 agent OS 判定与终审结论

### C.5 待办（需同步修 v1.1）

附录 C.1 的 P0-1～P0-4 是 v1.1 的真实缺陷，需同步修 v1.1（见本次会话后续修订）。

---

*本文是方向性文档，所有「迁移形态」「我们形态」均为设计主张，需在实现中验证。落地时请回到一手来源复核竞品数据。*
