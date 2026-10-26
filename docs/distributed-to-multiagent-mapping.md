# 分布式系统 ↔ 多智能体并发正确性层：对照与借鉴

| 项目 | 内容 |
|---|---|
| 版本 | v1.0 |
| 日期 | 2026-10-26 |
| 输入 | 4 个调研子智能体（一致性理论 / 存储系统 / 事务与流处理 / 集群调度与协调） |
| 前置 | [并发正确性层设计规范 v1.12](multi-agent-harness-design-final.md) · [定位与路线 v2.0](positioning-and-roadmap.md) |
| 核心结论 | 分布式系统是我们**最深层的设计祖先**：RSC ≈ OCC + Saga；EventLog ≈ WAL/LSM；idem_key ≈ Kafka 幂等生产者（内容指纹 vs 去重计数器，机制不同）；单写者 = **无复制的串行化点**（Raft = 带 quorum 复制的串行化点）；Z0–Z3 是**语义分区**，非一致性哈希分区 |

> **标记约定**：【事实】= 论文/官方文档明确；【推断】= 映射建议（需验证）。

---

## 目录

1. [映射总表](#映射总表)
2. [一致性模型 → 我们的冲突检测](#第二部分一致性模型)
3. [时钟 → 我们的版本/seq/租约](#第三部分时钟与顺序)
4. [共识 → 我们的单写者仲裁](#第四部分共识与复制)
5. [CRDT → 我们的合并](#第五部分crdt)
6. [分布式事务 → RSC/Saga/幂等](#第六部分分布式事务)
7. [隔离级别 → write skew 检测](#第七部分隔离级别)
8. [存储引擎（LSM/WAL）→ EventLog/Checkpoint](#第八部分存储引擎)
9. [流处理（CDC/背压/幂等）→ 订阅/背压/幂等键](#第九部分流处理)
10. [集群调度 → Scheduler/ReconcileLoop](#第十部分集群调度)
11. [负载均衡与限流 → 分区/预算](#第十一部分负载均衡与限流)
12. [故障检测 → 心跳/租约](#第十二部分故障检测)
13. [资源隔离 → 沙箱/worktree](#第十三部分资源隔离)
14. [可观测性 → 我们的可观测](#第十四部分可观测性)
15. [合并后的数据流（含分布式借鉴）](#第十五部分合并后的数据流)

---

## 映射总表

| 分布式概念 | 来源 | 我们的对应 | 可迁移性 | 机制差异 |
|---|---|---|---|---|
| **OCC（乐观并发控制）** | Kung & Robinson 1981 | **RSC 提交**（prepare 校验 read_set/write_set） | ★★★★★ | 我们按资源逐条加锁校验，无全局事务快照 |
| **Saga + 补偿** | Garcia-Molina & Salem 1987 | **compensate**（逆序回滚 + pivot） | ★★★★★ | 补偿是语义回滚（留痕），pivot 降级人工 |
| **TCC 预留** | Oracle MicroTx | **intent reservation**（预留不阻塞） | ★★★★☆ | 无 Confirm 协调者，预留不阻塞 |
| **MVCC** | Bernstein & Goodman 1981 | **base_version + 多版本** | ★★★★★ | 版本号 per-resource，非全局事务快照 |
| **Snapshot Isolation + write skew** | Cahill 2008 | **logical_deps + Run 级不变量校验** | ★★★★☆ | `logical_deps` 是 Planner 静态依赖声明 + 准入期 rw-conflict 检查，非 SI 行级 FCW |
| **SSI 危险结构检测** | Cahill 2008 | **logical_deps 准入拦截** | ★★★☆☆ | 仅借「危险结构」检测思路，不做运行时动态 SSI |
| **Percolator（SI + 2PC + TSO）** | Peng & Dabek 2010 | **RSC（借 SI 读集/写集校验；去 TSO，改单写者 seq）** | ★★★★☆ | **非 2PC**：去 TSO（用单写者 seq），无协调者；校验借 OCC 形态 |
| **Calvin（确定性排序）** | Thomson 2012 | **单写者串行档** | ★★★☆☆ | 仅取单写者串行点，不做确定性预排序执行 |
| **Paxos/Raft 多数派** | Lamport/Ongaro | **EventLog/BudgetManager 单写者** | ★★★★☆ | 单写者 = **无复制的串行化点**（SPOF）；Raft = 带 quorum 复制的串行化点 |
| **向量时钟 / 版本向量** | Fidge/Mattern/Parker | **base_version + read_set** | ★★★★☆ | `read_set` 是资源级版本集，非副本级版本向量 |
| **HLC** | Kulkarni 2014 | **事件 `seq` + 租约 TTL** | ★★★☆☆ | seq 全局单调 + ts 物理，无 HLC 逻辑偏移 |
| **TrueTime** | Corbett 2012 | **DurableTimer**（有界超时） | ★★★☆☆ | 有界超时，非有界时钟不确定性 |
| **3-way merge（base/ours/theirs）** | diff3 | **JSON-aware merge（数组并集去重）** | ★★★☆☆ | **非 CRDT**：冲突时不收敛，需 LLM 重编辑 |
| **LSM-Tree + WAL** | O'Neil et al. 1996（LSM-Tree）/ Chang et al. 2006（Bigtable） | **EventLog（追加日志）+ CheckpointStore（快照）** | ★★★★★ | EventLog 是事件流，非 KV 存储的 LSM 层级 |
| **Compaction** | Bigtable | **快照压缩 + 事件保留窗口** | ★★★★☆ | 按时间窗口删除，非 SSTable 合并 |
| **Bloom Filter** | Bigtable | **订阅过滤（快速排除）** | ★★★☆☆ | 概率过滤，可能假阳性 |
| **CDC（binlog 变更流）** | Debezium | **EventLog 作为变更流** | ★★★★★ | 事件由应用显式产生，非 binlog 解析 |
| **物化视图增量维护** | Flink/Kafka Streams | **ContextAssembler（订阅 → 物化）** | ★★★★★ | 按 agent cursor 增量组装 |
| **Watermark** | Flink | **cursor 水位线** | ★★★★☆ | 单订阅者游标，非全局事件时间水位 |
| **Kafka 幂等生产者（PID+seq）** | Kafka 0.11 | **idem_key（幂等键）** | ★★★★☆ | 内容指纹（语义级）vs 去重计数器 |
| **Kafka 事务（read_committed）** | Kafka 0.11 | **RSC 提交可见性** | ★★★☆☆ | 无事务协调者，靠提交层 CAS |
| **credit-based 背压** | Flink 1.5 | **ready_queue 深度上限 + per-subscriber 背压** | ★★★☆☆ | 粗粒度背压；credit-based 为阶段三细化方向 |
| **DLQ（死信队列）** | 通用实践 | **DEAD_LETTER 终态** | ★★★★★ | 由 aging + 预算释放触发，非消费失败计数 |
| **Quorum（R+W>N）** | Gifford 1979/Dynamo | **准入仲裁 + 单写者** | ★★★☆☆ | 准入只拦语义冲突，非 R+W>N 投票 |
| **一致性哈希 + vnode** | Dynamo/Cassandra | **Z0–Z3 分区** | ★☆☆☆☆ | **机制不同：Z0–Z3 是语义分区，不是哈希分区** |
| **Phi Accrual 故障检测** | Hayashibara 2004 | **心跳 + 租约 TTL** | ★★★★☆ | 固定超时二元判决，非连续怀疑度 |
| **熔断/舱壁/限流** | Microsoft/Azure | **OOM 驱逐 + 预算上限 + 单资源锁** | ★★★☆☆ | OOM 是资源耗尽杀进程（非失败率开路）；预算为预留式配额 |
| **Borg/Omega/K8s 调度** | Burns 2016 | **Scheduler + ReconcileLoop** | ★★★★☆ | MVP 单体调度器，Omega 式共享状态为阶段三 |
| **cgroups/namespaces** | Linux | **沙箱 + worktree** | ★★★☆☆ | worktree 是 FS overlay，非内核 cgroup 隔离 |
| **gVisor/Firecracker** | Google/AWS | **per-agent 沙箱** | ★★★☆☆ | MVP 进程级隔离，未达 gVisor 内核态拦截 |
| **SLI/SLO/错误预算** | SRE Book | **conformance suite + 基准** | ★★★☆☆ | 不变量测试，非持续 SLO 监控 |

---

## 第二部分：一致性模型

### 2.1 强度谱系 → 我们的一致性级别

分布式的一致性模型从强到弱：

```
严格串行化 > 线性一致性 > 顺序一致性 > 因果一致性 > PRAM > 会话保证 > 最终一致
```

**我们的对应**：

| 我们的机制 | 等价一致性级别 | 说明 |
|---|---|---|
| **RSC 提交（base_version CAS + 读集合校验）** | **单资源线性化** | Z2 单资源线性化点 = `ws.apply`；读集合校验强于 SI 的行级写冲突检测，但仍**不构成可串行化**（write skew 需 `logical_deps`） |
| **Z2 分区（per-resource 串行化）** | **顺序一致性（分区内）** | 每资源一个串行化点 |
| **logical_deps + Run 级不变量校验** | **因果一致性 + 显式依赖** | 依赖图 = 因果图 |
| **ContextAssembler（cursor + 快照重放）** | **读己之写 / 单调读** | 会话保证（per-agent cursor） |
| **EventLog 全局 seq** | **PRAM/FIFO** | 按 `(resource, version)` 单调投递 |

### 2.2 CAP/PACELC → 我们的取舍

【推断】我们采用**分层取舍**，不适用单一 PACELC 标签：
- **Z2 单资源**：CP（线性化）——提交层是唯一串行化点。
- **跨资源不变量**：AP（最终一致 + 事后校验）——不阻塞，靠 `logical_deps` 与 Run 级校验收敛。
- **理由**：跨资源强一致需要 2PC，而 agent 事务跨分钟 → 采纳 Saga 式最终一致（见 §6）。

---

## 第三部分：时钟与顺序

### 3.1 时钟方案 → 我们的版本/seq/租约

| 分布式方案 | 我们 | 说明 |
|---|---|---|
| **Lamport Clock**（偏序，不能检测并发） | `generation`（**恢复代次计数器**） | 仅用于恢复时作废旧租约，**不是 Lamport clock**（不承担事件排序） |
| **向量时钟**（检测并发） | `read_set`（**资源级版本集**） | 记录「读到的各资源版本」，**不是版本向量**（无副本维度） |
| **版本向量**（副本级冲突检测） | `base_version` | 检测「我的 base 是否已被超越」 |
| **HLC**（物理 + 逻辑） | `seq`（全局单调）+ `ts`（物理） | 事件同时带 seq 与 ts（不实现 HLC 逻辑偏移） |
| **TrueTime**（有界区间） | `DurableTimer`（有界超时） | 用于 deadline/超时，**非有界时钟不确定性** |

### 3.2 关键洞察

【推断】**我们不需要向量时钟的 O(N) 维度** —— 因为我们的「冲突检测」是**资源级**（每资源一个版本号），不是「事件级因果图」。资源级版本号（`base_version`）= 向量时钟在「单资源」上的退化形式，**O(1)**。

> 这是把「事件级因果」降为「资源级版本」的关键简化：agent 的决策因果由**逻辑子目标 id**（`logical_subgoal_id`）承担，资源冲突由 `base_version` 承担。

---

## 第四部分：共识与复制

### 4.1 Raft/Paxos → 我们的单写者仲裁

【推断】我们的「单写者」设计（B3：只有 EventLog 能分配全局 `seq`；§9.1：`reserve` 单写者）是一个**串行化点**，但**不等价于 Raft 组**：

- **单写者 = 无复制的串行化点**（单点故障 SPOF）。
- **Raft = 带 quorum 复制的串行化点**（leader 是串行化点，日志经多数派复制后持久）。
- **MVP → 阶段三的差距**是 quorum/复制/选主，而非「把单写者当成 Raft 的退化形态」。

| Raft 概念 | 我们（MVP） | 阶段三 |
|---|---|---|
| Leader | EventLog 序列器 / BudgetManager（单写者） | 经选主产生的 leader |
| Log 条目 | Event / BudgetReservation | 复制日志 |
| 多数派 | **无**（无复制） | quorum 复制 |
| 选举 | **无**（单点，SPOF） | 引入选主 |
| 快照 | CheckpointStore | CheckpointStore + 日志截断 |

**结论**：MVP 的「单写者」是**无复制的串行化点**；阶段三用**带 quorum 复制的 Raft 组**替换（见定位 §6.5）。

### 4.2 Quorum → 准入仲裁

【推断】`intents.compatible` 的准入判定可视为**轻量仲裁**：
- 单资源写-写：**不仲裁**（放行至提交层 CAS）
- 契约/不变量冲突：**准入 fail-fast**（类比 quorum 拒绝）

---

## 第五部分：CRDT

### 5.1 CRDT 收敛条件 → 我们的合并（对照失败）

【事实】CRDT 的收敛依赖交换律 + 结合律 + 幂等律，且合并结果必须与顺序无关。
【推断】我们的 **JSON-aware merge**（§7.2.1 B）是 **3-way merge（base/ours/theirs）**，**不是 CRDT**：
- 键级合并借 diff3 的「三方比较」，不是 join-semilattice 的 join
- 数组并集去重只是启发式，冲突时**不保证收敛**
- 冲突无法自动消解时，需 **LLM 重编辑**（可能产生不同结果）

| 我们借鉴的合并语义 | 用途 | 是否 CRDT |
|---|---|---|
| **三方键级合并** | 配置项（同 key 冲突 → pivot 或 LLM 重编辑） | 否 |
| **数组并集去重** | `dependencies` / `paths` 数组 | 否（启发式，非 OR-Set） |
| **预算增/减** | 预算（`reserve`/`consume`/`release`） | 否（预留式配额，非 PN-Counter） |
| **序列合并** | 不适用（代码不是可交换序列） | 否 |

### 5.2 局限

【推断】3-way merge **不能表达**「类型一致性」这类跨文件不变量，且冲突时**不收敛** → 这正是我们需要 `logical_deps` + Run 级校验 + LLM 重编辑的原因（合并收敛 ≠ 语义正确）。

---

## 第六部分：分布式事务

### 6.1 协议 → 我们的机制

| 协议 | 核心 | 我们的对应 |
|---|---|---|
| **2PC** | 协调者 + 两阶段投票 | **不采纳**（RSC 借 OCC 的「验证-写入」结构，**非两阶段投票**；无协调者） |
| **3PC** | 非阻塞（但受分区限制） | 不采纳 |
| **Saga** | 局部事务 + 逆序补偿 | **compensate**（逆序回滚 + pivot） |
| **TCC** | Try 预留 / Confirm / Cancel | **intent reservation**（预留不阻塞） |
| **OCC** | 读-验证-写 | **RSC 提交**（read_set/write_set 验证） |
| **Percolator** | SI + 2PC + TSO | **RSC（借 SI 读集/写集校验；去 TSO，改单写者 seq）** |
| **Calvin** | 确定性排序后执行 | **单写者串行档** |

### 6.2 关键洞察

【推断】**RSC = OCC（read_set/write_set 验证）+ 按租约 fencing + 逐资源短临界区 + Saga 补偿**：
- OCC 提供「prepare 校验 read_set/write_set」
- 按租约 fencing 保证旧持有者被拒（单调 fencing token）
- 逐资源短临界区（LLM 推理期间绝不持锁）
- Saga 提供「失败时逆序补偿」
- **非 2PC**：无协调者、无两阶段投票；各资源独立 prepare/apply，去掉协调者后剩下的「prepare 校验 + apply 提交」就是 OCC 的标准形态

### 6.3 补偿 = 语义回滚（非数据库回滚）

【事实】Saga 的补偿是**语义回滚**（留下痕迹），不是数据库回滚。
【推断】对应我们的 **pivot** 语义：不可逆动作（已发邮件）→ 标记 pivot，交人工。

---

## 第七部分：隔离级别

### 7.1 SI 的 write skew → 我们的 logical_deps

【事实】Snapshot Isolation 只检测「同一行」的写冲突（行级），无法捕获「跨行但共享谓词」的冲突（write skew）。
【推断】这正是我们 §7.3 的问题：**Z2 单资源锁抓不到跨文件不变量**。
`logical_deps` = **Planner 静态依赖声明** + **准入期 rw-conflict 检查**；SSI 仅提供 write skew 的**问题来源**与「危险结构」的**检测思路**，我们**不做**运行时动态 SSI。

| 隔离级别 | 我们的对应 |
|---|---|
| **Read Committed** | 无（我们不承诺） |
| **Snapshot Isolation** | 仅部分借鉴：RSC 的 `base_version` 快照 + 读集合校验**强于** SI 行级写冲突检测 |
| **Serializable** | **不承诺**：`logical_deps` + Run 级校验只能覆盖已声明的逻辑依赖，write skew 仍可能漏网 |
| **SSI** | 仅借「危险结构」检测思路；当前实现为 Planner 静态声明 + 软准入 |

---

## 第八部分：存储引擎

### 8.1 LSM-Tree → EventLog + CheckpointStore

【事实】LSM 结构：写入 → WAL → memtable → immutable → L0 → L1..Ln；compaction 合并。
【推断】我们的对应：

| LSM | 我们 |
|---|---|
| **WAL** | **EventLog**（追加日志，全局 seq） |
| **memtable** | 内存事件缓冲 |
| **SSTable flush** | **checkpoint 落盘** |
| **L0（可重叠）** | 未 checkpoint 的事件 |
| **Lk（不重叠）** | 已 checkpoint 的事件 |
| **Compaction** | **快照压缩 + 事件保留窗口**（`RETENTION`） |
| **Bloom Filter** | **订阅过滤**（快速排除） |
| **MVCC** | **base_version 多版本** |

### 8.2 「日志即真相」

【推断】EventLog 应成为**变更流的唯一真相**（类比 CDC/binlog），下游（ContextAssembler、Verifier）从日志派生，而非直接读状态。

> **当前差距**：我们的 EventLog 是「侧通道」（§5.6），下游直接读 WorkspaceVersionStore。**建议**：把 ContextAssembler 改为「从 EventLog 派生的物化视图」（见 §15）。

---

## 第九部分：流处理

### 9.1 CDC → EventLog 作为变更流

【事实】CDC（binlog/logical replication）把数据库变更暴露为事件流。
【推断】我们的 EventLog 就是「agent 动作的 CDC」——每个资源提交/effect 确认都是一个变更事件。

### 9.2 背压 → ready_queue + per-subscriber

【事实】Flink 1.5 的 **credit-based flow control**：接收方按可用 buffer 授予 credit，发送方无 credit 即阻塞。
【推断】我们的 `ready_queue` 深度上限 = **粗粒度背压**（无 credit 反馈回路）；per-subscriber 最大在途仅作上限约束。**credit-based 为阶段三细化方向**。

### 9.3 幂等生产者 → idem_key

【事实】Kafka 幂等生产者用 `(PID, partition, seq)` 去重，其中 seq 是**单调计数器**（只保证同一分区内不重复，不识别内容）。
【推断】我们的 `idem_key = hash(run_id, logical_subgoal_id, hash(tool, hash(args)))` 是**内容指纹**，作用更强（同一副作用跨重试/跨 replan 都可识别），但**机制不同**（非计数器）。

| Kafka | 我们 | 作用域/生命周期 | 机制差异 |
|---|---|---|---|
| PID | `run_id` | 会话级 | Kafka 用 PID 区分生产者会话 |
| partition | `logical_subgoal_id`（逻辑子目标） | 逻辑子目标，跨 replan 稳定 | Kafka 为分区内去重 |
| sequence number | **Kafka seq（去重计数器）↔ 无对应**（我们的 `idem_key` 是内容指纹，作用更强但机制不同） | Kafka：producer session；我们：EffectJournal 持久化，跨崩溃/重规划 | 计数器 vs 内容指纹 |

### 9.4 Exactly-once → I7

【事实】Kafka 端到端 exactly-once = 事务 + read_committed。
【推断】我们的 I7 = **at-least-once + 幂等键 + UNKNOWN 对账**（不依赖事务，更通用）。

---

## 第十部分：集群调度

### 10.1 Borg/Omega/K8s → Scheduler + ReconcileLoop

【事实】K8s 是 **level-triggered reconcile**（响应当前状态，而非事件流）。
【推断】我们的 §5.2.1 `ReconcileLoop` 正是 K8s 式对账（从 PlanStore 重建 ready 集）。

| 调度架构 | 我们的选择 |
|---|---|
| **单体（Borg/K8s 默认）** | ✅ MVP（单 Scheduler） |
| **共享状态（Omega）** | 阶段三（多调度器 + OCC） |
| **两级（Mesos）** | 不采纳 |

### 10.2 优先级与抢占 → 预算树

【推断】K8s 的 PriorityClass + Preemption 对应我们的 `BudgetNode` 优先级 + OOM 驱逐。

---

## 第十一部分：负载均衡与限流

### 11.1 一致性哈希 + vnode → Z0–Z3 分区（不成立）

【事实】一致性哈希：key 顺时针落到节点；vnode 均衡负载，目标是**把同一 key 稳定映射到节点**并支持节点增减时最小迁移。
【推断】我们的 **Z0–Z3 分区**与之**机制不同**：Z0–Z3 是**语义分区**（按资源类型与冲突性质决定并发机制），不是哈希分区，也不做 key→节点映射或 vnode 均衡。
- 资源 → 语义分区（Z0–Z3）
- 分区 → 并发机制（无锁/短临界区/单写者）

> 可迁移性：★☆☆☆☆（仅「分区」一词同形，机制不同）。

### 11.2 限流 → 预算

【事实】令牌桶（允许突发）vs 漏桶（匀速）。
【推断】我们的 `BudgetNode` 是**预留式配额**（`reserve` 预扣、`consume` 消耗、`release` 归还），**不是令牌桶**（无令牌生成速率，只有硬上限与守恒约束）；`ready_queue` 是**并发限制**。

### 11.3 熔断/舱壁 → OOM 驱逐/隔离

【事实】熔断（失败率阈值 → 开路）、舱壁（资源池隔离）。
【推断】我们的 **OOM 式驱逐**（预算耗尽时杀价值最低的子 agent）= **资源耗尽杀进程**，**不是熔断**（无失败率阈值与开路状态）；**per-agent 沙箱** ≈ 舱壁（资源池隔离）。

---

## 第十二部分：故障检测

### 12.1 Phi Accrual → 心跳 + 租约

【事实】Phi Accrual 输出连续怀疑度 φ，由应用设阈值（检测与判决解耦）。
【推断】我们的 **心跳 + 租约 TTL** 是**固定超时**（二元判决）；**建议**阶段三引入 Phi Accrual 式连续怀疑度（用于 Supervisor 的软降级）。

### 12.2 SWIM/gossip → Supervisor 广播（不成立）

【推断】RunCoordinator 的取消广播是**集中式直接广播**（维护全局活跃 agent 表，一次发给所有后代），**不是 SWIM**（无随机探测、无 gossip 传播、无成员视图收敛）。

---

## 第十三部分：资源隔离

### 13.1 隔离档位 → 沙箱选择

【事实】隔离档位：进程 < 容器 < seccomp < gVisor < Firecracker < VM。
【推断】我们的 **Z3 worktree** 是 **FS overlay**（文件系统视图隔离），**不是容器级隔离**（不提供 namespaces/cgroups 的进程与资源隔离）；**per-agent 沙箱**目标为 gVisor 级（不可信代码），MVP 为进程级。

| 档位 | 我们的用途 | 实际机制 |
|---|---|---|
| **FS overlay** | Z3 worktree（代码文件） | 文件系统视图隔离，非容器 |
| **进程级** | per-agent 沙箱（MVP） | 进程隔离，未达 gVisor 内核态拦截 |
| **gVisor** | per-agent 沙箱（目标） | 不可信代码执行 |
| **Firecracker** | 阶段三（跨机强隔离） | — |

---

## 第十四部分：可观测性

【推断】SLI/SLO/错误预算 → 我们的 **conformance suite**（I1–I14 测试）+ **基准**（3-agent 并行加速比）。

---

## 第十五部分：合并后的数据流

> 见设计规范 §20「无死角图集」。本节列出**分布式借鉴带来的新增数据流**：

| 新增流 | 来源 | 内容 |
|---|---|---|
| **EventLog → ContextAssembler 物化流** | CDC/物化视图 | ContextAssembler 从 EventLog 派生（而非直读 Store） |
| **ReconcileLoop 对账流** | K8s level-triggered | 周期扫描 + 事件纠正 Task 状态 |
| **DurableTimer 触发流** | Flink durable timer | 到期回调 → Supervisor |
| **背压流（粗粒度）** | Flink 1.5（阶段三细化） | ready_queue 深度上限；credit 反馈为阶段三方向 |
| **logical_deps 校验流** | SSI「危险结构」检测思路 | Planner 静态声明 + 准入期 rw-conflict 检查 |
| **3-way merge 流** | diff3（非 CRDT） | JSON-aware merge；冲突不收敛时 LLM 重编辑 |

---

## 附：来源清单

- **一致性**：CAP（Brewer/Gilbert-Lynch）、PACELC（Abadi）、线性一致（Herlihy-Wing）、顺序一致（Lamport）、因果一致（Ahamad）、CRDT（Shapiro）、OCC（Kung-Robinson）、MVCC（Bernstein-Goodman）、2PL（Eswaran）、SI（Berenson）、SSI（Cahill）
- **存储**：GFS、Bigtable、Spanner、Dynamo、Cassandra、HBase、RocksDB TOS、DDIA
- **事务**：2PC（Gray）、3PC（Skeen）、Saga（Garcia-Molina）、Percolator（Peng-Dabek）、Calvin（Thomson）
- **流处理**：Kafka、Pulsar、Flink 1.5 背压、Debezium
- **集群**：Raft（Ongaro）、Paxos（Lamport）、ZAB（Junqueira）、Chubby（Burrows）、Borg/Omega/K8s（Burns）、Phi Accrual（Hayashibara）、gVisor、K8s 文档

> **事实/推断边界**：映射部分（【推断】）为设计建议，非论文结论；落地前需回一手来源复核。
