# Agent Concurrency Correctness Layer

让多个 agent 并行修改同一份代码库时，像数据库事务一样可靠的运行时。

> 核心机制 **RSC（Reserve-Stage-Commit）** = OCC（乐观并发控制）+ 租约 fencing + 短临界区 + Saga 补偿。
> 明确**不是** 2PC、**不是** CRDT、**不是** Agent OS —— 只解决「并发正确性」这一件事。

---

## 文档（项目方案）

| 文档 | 内容 |
|---|---|
| [第一阶段实现方案 v1.1](docs/phase1-implementation-plan.md) | 阶段一 MVP 的落地设计：提交点（WAL+blob）、补偿幂等键、EffectJournal 去重语义、全局锁序、S0–S8 实施步骤、19 个可判定测试、基准防作弊 |
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
python3 -m pytest tests/ -q     # 44 passed
```

测试覆盖：U1–U4 真实用例、A1–A13 对抗用例（含背压降级）、MS0 出口（seq 单调 + schema 定稿 + 状态机表驱动）、MS7 出口（能力强制）、MS8 出口（I1–I13 一致性校验）。
