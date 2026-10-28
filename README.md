# Agent Concurrency Correctness Layer

让多个 agent 并行修改同一份代码库时，像数据库事务一样可靠的运行时。

> 核心机制 **RSC（Reserve-Stage-Commit）** = OCC（乐观并发控制）+ 租约 fencing + 短临界区 + Saga 补偿。
> 明确**不是** 2PC、**不是** CRDT、**不是** Agent OS —— 只解决「并发正确性」这一件事。

---

## 文档（项目方案）

| 文档 | 内容 |
|---|---|
| [第一阶段实现方案 v1.1](docs/phase1-implementation-plan.md) | 阶段一 MVP 的落地设计：提交点（WAL+blob）、补偿幂等键、EffectJournal 去重语义、全局锁序、S0–S8 实施步骤、19 个可判定测试、基准防作弊 |
| [S2 详细方案 v1.3](docs/s2-workspace-and-locks.md) | manifest + blob 读模型、每资源锁与全序批量获取、租约时钟与过期语义、提交点顺序、4 组可判定测试（T4/T5/T5b/T7） |
| [S3 详细方案 v1.3](docs/s3-rebase-and-compensation.md) | 结构化冲突的 JSON 3-way merge（`SET` 增并删减 / GROW_ONLY / REPLACE / PIVOT）、有界 rebase 回路、Delete 哨兵与 tombstone、resolver 三层安全防护 |
| [S4 详细方案 v1.2](docs/s4-budget-and-admission.md) | 预算树（`Reservation` 句柄 + 子树守恒 + 幂等 release/settle）、准入四级回滚、`SlotTracker`、能力签发收口、并发阴性对照 |
| [S5 详细方案 v1.2](docs/s5-recovery-and-journals.md) | effect/compensation journal 落盘 + probe 三态反查 + 补偿批量原子与续做 + `recover` 的 `resume_policy` + K-6..K-14 边界收口 |
| [S6 详细方案 v1.2](docs/s6-runtime-and-e2e.md) | 动作注册表 + guard 谓词列表与续租三态 + 事件游标真水位 + snapshot 补字段 + `edges` 前驱闸门与环检测 + `_drain_scheduler` 真消费 + U 用例去假绿 + 10 条不变量 E2E |
| [S7 方案 v1.2](docs/s7-benchmark-and-coverage.md) | 三条件（串行/worktree/共享层）× 四负载基准 + 外部差分度量（资源级 expected + 键级归因）+ 独立 oracle + 二级指标 + 覆盖率分模块阈值 |
| [S9 方案 v1.2](docs/s9-persistence-and-receipts.md) | 持久化 checkpoint（信封/codec/双层锁/唯一 ref）+ 收据链（同帧 + `commit_key` 去重 + scope 锁 + head 锚点）+ `CrashInjected` 契约 + `retention` 安全语义 + 跨进程恢复与 SIGKILL |
| [S10 方案 v1.2](docs/s10-cross-process-reconciliation.md) | 跨进程预算对账（事件溯源 + 全量重放 + 树完整性 + 单写者 lease）+ 跨进程 flock（`_FileLock` 下沉 + 三锁全序 + WAL seq fstat + fencing）+ `FinalitySink` 接口草案 |
| [S22 方案 v1.1](docs/s22-implementation-plan.md) | **被集成层**：内核收口（abort/finish/外部幂等键/token 回收/懒恢复）+ Z3 私有工作区（git worktree + copy）+ `RscSession` 六动作门面 + LangGraph 中间件（sync/async）+ MCP server；`dependencies = []` 不变（适配器走 extras） |
| [S23 方案 v1.0](docs/s23-integration-plan.md) | **集成面第二轮**：CrewAI（包装 `BaseTool.run`）、OpenAI Agents（guardrail + root 注入）、AG2（`on_tool_execution` 短路）、纯 stdlib HTTP sidecar（每 agent 密钥、1 MiB/并发上限）；含 [三 agent 示例](examples/three_agents/README.md)（naive 丢写 vs RSC 保全）、79 条一致性核对与对抗式评审记录（1 P0 + 5 P1 + 3 P2 已修） |
| [S24 差距分析与 Verifier](docs/s24-gap-analysis-and-repositioning.md) | **里程碑对账 + 定位复核 + S24-1 Verifier 门禁**：最初承诺 vs 现状逐条对照、8 项差距排序、文档矛盾清单；`RscSession(verify_cmd=...)` 在「候选副本 + 账本合并视图 + pending」上跑命令，红则拒绝提交（收据带 `verified`）；`semantic=block` 静态拦「改名/删名仍被引用」（不执行代码）；[语义冲突实验](examples/semantic_conflict/run.py) 从「拦不住」变成「拦住」 |
| [S21 方案 v1.1](docs/s21-tenant-admission-and-leases.md) | K-97 tenant 自动接准入 + K-99 失效租约回收器（状态复制 → K-103）+ K-100 墓碑窗口量化（逐墓碑年龄 + 上限随命令复制）+ 多租户优先级份额（reserved 池 / 超额借用） |
| [S20 方案 v1.1](docs/s20-tenant-metering-and-k94.md) | K-94 claims 有界墓碑 + K-95 prune fail-safe + K-96 就绪复核 + 阶段三：多租户配额 / 事件派生计量 / 统一 Admin 命令面 |
| [S19 方案 v1.1](docs/s19-budget-gc-and-hardening.md) | K-88 账本三栅栏精密修剪 + K-89 claim fencing 复制 + K-90 重放时间戳 + K-91 `remaining` + K-92 预检/可配密钥 + K-93 owner 共识 |
| [S18 方案 v1.1](docs/s18-budget-rsm-and-linearizable-read.md) | K-87 预算树纳入共识（真 RSM）+ K-82 读穿日志线性一致读 + K-83 外部地址集群 |
| [S17 方案 v1.1](docs/s17-boundaries-and-ms10.md) | K-81~K-86 收口（确定性 id / require_leader / 生产配置 / 快照恢复 / epoch 语义）+ MS10（可观测 / 优雅排空 / 毒任务 DLQ / kill·purge） |
| [S16 方案 v1.1](docs/s16-ms9-raft.md) | MS9 分布式化：`RaftEventLog`（seq 由 leader 分配）+ `RaftCluster` + `RaftEpochSource`；三后端一致性（memory/wal/raft）；`pysyncobj` 可选依赖 |
| [S15 方案 v1.1](docs/s15-protocol-and-production.md) | 大类 C：Protocol 契约补全 + 后端一致性套件 + 反漂移静态检查（MS9 前置）；大类 D：Ingress 幂等 + DLQ redrive + Admin pause/resume + metrics 快照 |
| [S14 方案 v1.3](docs/s14-boundaries-and-ms8-closure.md) | 边界缺口收口（K-67..K-71）：`tick` 持久候选集 + 驱动器、`update_children` 条件写、深度超限终局化、edges 物化 + 别名剪枝、MS8 I12 不饿死 |
| [S13 方案 v1.2](docs/s13-orchestration.md) | orchestration 四件套：HITL 三态 + tick 超时、replan（三处 alias 统一）、`spawn_child`（能力子集 + `child_alloc` 预分配 + 取消级联）、Scheduler（默认 sync + claim/fencing） |
| [S12 方案 v1.2](docs/s12-kernel-enforcement-and-core.md) | egress 后端收口（`unshare -Urn`/`runsc`）+ K-45 收据回扫 fail-closed + per-run epoch/retention + Landlock errno 探测 + 生产入口 fail-closed |
| [S11 方案 v1.2](docs/s11-egress-enforcement.md) | egress 强制点（外部库 `sandlock`/`ip netns`，不自研）+ DNS 钉住与 SSRF 阻断 + 统一 gateway 前置 + K-44 收据增量索引 + K-46 fencing 绑 WAL |
| [设计规范 v1.12](docs/multi-agent-harness-design-final.md) | 主设计规范：定位与承诺边界、Z0–Z3 分区、组件契约、RSC 提交与补偿、不变量 I1–I14、20 张数据流/架构/调用/扇入扇出图 |
| [里程碑与模块划分](docs/milestones.md) | MS0–MS10 共 11 个里程碑，每个含目标 / 新增模块 / 依赖 / 出入口条件 / 扩展点 / 承上启下；含实现前决策 D1–D16 |
| [用例与对抗测试](docs/use-cases-and-adversarial-tests.md) | 4 个真实用例 + 13 个对抗用例，逐步推演表 + 跑通判定标准 |
| [定位与路线图](docs/positioning-and-roadmap.md) | 为什么不是 Agent OS、能力边界、演进路线 |
| [分布式 → 多智能体映射](docs/distributed-to-multiagent-mapping.md) | Paxos/Raft/OCC/MVCC/Saga/WAL/LSM/背压 等机制的投射与「机制差异」 |
| [计算机网络 → 多智能体映射](docs/networking-to-multiagent-mapping.md) | 信息有效性、高信息密度、流控与可靠传递 |
| [OS/DB → 多智能体映射](docs/os-to-multiagent-mapping.md) | 隔离、调度、地址空间、事务语义 |
| [调度架构](docs/multi-agent-dispatch-architecture.md) · [数据流](docs/multi-agent-harness-dataflow.md) · [读写分区](docs/multi-agent-rw-partition-design.md) | 专题设计 |

## 可执行实现

`harness/` 是设计规范的最小可执行基线（纯标准库 + pytest，无第三方依赖）：

- L1 核心：`types` / `event_log` / `intent_registry` / `workspace` / `budget` / `checkpoint` / `subscription`
- L3 执行：`context` / `policy` / `runtime`
- L4 编排：`planner` / `dispatcher` / `scheduler` / `run_coordinator` / `reconcile` / `hitl`
- L5 门面：`harness`（组装 + `commit_batch` + `compensate` + `recover`）
- L2 规范：`harness/spec/`（资源模型 / 协调协议 GRANT-WAIT-DIE / 收据 hash-chain / 不变量语料）

详见 [harness/README.md](harness/README.md)。

## 运行测试

```bash
python3 -m pytest tests/ -q     # 733 passed / 3 skipped / 4 xfailed（沙箱缺能力项显式 xfail）
```

测试覆盖：U1–U4 真实用例、A1–A13 对抗用例（含背压降级）、MS0 出口（seq 单调 + schema 定稿 + 状态机表驱动）、MS7 出口（能力强制）、MS8 出口（I1–I13 一致性校验）。
