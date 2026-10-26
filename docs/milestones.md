# 里程碑与模块划分（Milestones & Module Breakdown）

| 项目 | 内容 |
|---|---|
| 版本 | v1.0 |
| 日期 | 2026-10-26 |
| 前置 | [设计规范 v1.11](multi-agent-harness-design-final.md) · [用例集](use-cases-and-adversarial-tests.md) |
| 可执行基线 | `harness/`（**35 个测试全通过**） |
| 原则 | **每一步承上启下、可拓展、无死代码、无硬编码** |

> **核心约束**：每个模块必须满足三条 —— ① **有接口**（可被替换）；② **有测试**（证明可用）；③ **有下游**（被后续里程碑使用，否则就是死代码）。

---

## 第一部分：模块划分

### 1.1 分层与依赖（DAG）

```
L0 基质      sandbox / worktree / 网络出口 / cgroups
              ▲
L1 核心      types ─ event_log ─ intent_registry ─ workspace(+versions)
              │        │              │                │
              │        │              │                ├── budget
              │        │              │                ├── checkpoint
              │        │              │                ├── effect_journal
              │        │              │                └── subscription
              ▼        ▼              ▼                ▼
L3 执行      context ─ policy ─ runtime
              ▲        ▲         ▲
              │        │         └── 调用 L1 全部
              └────────┘
L4 编排      planner ─ dispatcher ─ scheduler ─ run_coordinator
              │                                     │
              └── reconcile / hitl ◀────────────────┘
L5 门面      harness（组装 + 端到端）
```

### 1.2 模块清单（每个模块：职责 / 接口 / 测试 / 下游）

| # | 模块 | 层 | 职责 | 关键接口 | 测试 | 下游（谁用它） |
|---|---|---|---|---|---|---|
| 1 | `types` | L1 | 纯数据类型 | `ACB` / `Event` / `Status` | test_types | 全部 |
| 2 | `event_log` | L1 | 全局 seq + 订阅通知 | `append_in_txn` / `replay` / `has` | test_event_log | 全部 |
| 3 | `intent_registry` | L1 | 预留 + fencing | `reserve` / `is_valid` / `revoke` / `renew` | test_a9 | runtime, workspace |
| 4 | `workspace` | L1 | RSC 提交 + 3-way merge | `stage` / `prepare` / `apply` / `rebase_json` | test_u1/u2/u3/a1 | runtime |
| 5 | `workspace_versions` | L1 | 前像快照（供 merge/补偿） | `record` / `get` | test_a1/a3 | workspace |
| 6 | `budget` | L1 | 嵌套预算守恒 | `reserve` / `consume` / `release` / `check` | test_a4/a12/a13 | runtime, dispatcher |
| 7 | `checkpoint` | L1 | 唯一恢复依据 | `save` / `load` | test_u4/a3 | runtime, harness |
| 8 | `effect_journal` | L1 | 幂等键 + UNKNOWN 对账 | `begin` / `confirm` / `mark_unknown` | test_u4/a6 | runtime |
| 9 | `subscription` | L1 | 读即订阅 + 背压 | `add` / `subscribers` / `is_backpressured` | test_a7 | context |
| 10 | `context` | L1 | 物化视图 + cursor | `context_view` / `replay` | test_a7/a8 | runtime |
| 11 | `policy` | L1 | CapabilityToken 强制 | `check_capability` / `issue` | test_a9 | runtime |
| 12 | `runtime` | L3 | agent 主循环 + guard | `run` / `guard` | 全部 | harness |
| 13 | `planner` | L4 | Plan + footprint + patch | `plan` / `patch` / `add_logical_edge` | test_u3/a6 | harness |
| 14 | `dispatcher` | L4 | 准入 | `try_admit` | test_a4/a12 | harness |
| 15 | `scheduler` | L4 | ready_queue | `push` / `pop` | test_a11 | harness |
| 16 | `run_coordinator` | L4 | Run 状态唯一写者 + 取消广播 | `cancel_run` / `submit_command` | test_a2/a5 | runtime, harness |
| 17 | `reconcile` | L4 | level-triggered 对账 | `rebuild_ready_set` / `reconcile_events` | test_a11 | harness |
| 18 | `hitl` | L4 | 审批处理器 | `raise` / `resolve` / `get_result` | test_a8 | runtime |
| 19 | `harness` | L5 | 组装 + 端到端 | `run_task` / `commit_batch` / `compensate` / `recover` | 全部 | 用户 |

### 1.3 可扩展点（避免死代码的关键）

每个模块都预留了**替换点**，确保「现在实现的是参考实现，未来可换」：

| 扩展点 | 当前实现 | 未来替换 |
|---|---|---|
| `event_log` 存储 | 内存列表 | WAL / Postgres / Kafka |
| `workspace` 存储 | 内存 dict | FS / S3 / Git |
| `intent_registry` 序列器 | 内存 + 单写者 | **Raft 组**（阶段三） |
| `budget` 后端 | 内存树 | 分布式配额服务 |
| `checkpoint` 后端 | 内存 dict | 对象存储 / 快照服务 |
| `policy` 规则 | 允许一切 | RBAC / 数据分级 / OPA |
| `context` 物化 | 全量 clone | 增量物化视图 |
| `planner` 策略 | 静态 footprint | LLM 规划 / 学习式 |
| `rebase_json` | 简化 3-way | 完整 diff3 / CRDT 混合 |

> **判定「死代码」的标准**：若一个模块没有被任何**后续里程碑**的出口条件引用，它就是死代码。本设计中每个模块至少被 1 个后续里程碑引用（见第二部分）。

---

## 第二部分：里程碑

> 每个里程碑格式：**目标 / 新增模块 / 依赖 / 入口条件 / 出口条件（可执行测试）/ 扩展点**。
> 「出口条件」必须是**可运行的测试**，不是文档声明。

### MS0 · 骨架与事件原语

| 项 | 内容 |
|---|---|
| **目标** | 建立类型系统与全局事件流（B3 单写者） |
| **新增模块** | `types`、`event_log` |
| **依赖** | 无 |
| **入口条件** | 仓库初始化，`pytest` 可用 |
| **出口条件** | `test_event_log_seq_monotonic_under_concurrency`：并发 append 1000 条，seq 严格单调无重复 |
| **扩展点** | `EventLog` 实现可换（接口已定） |
| **承上启下** | 为 MS1 提供「事件同事务」与「对账输入」 |

### MS1 · 单资源 RSC（并发正确性内核）

| 项 | 内容 |
|---|---|
| **目标** | 实现「预留-暂存-提交 + fencing + CAS + 补偿」 |
| **新增模块** | `intent_registry`、`workspace`、`workspace_versions` |
| **依赖** | MS0 |
| **入口条件** | MS0 出口通过 |
| **出口条件** | `test_u1_serial`、`test_u1_concurrent_merge`、`test_u2`、`test_a1`（补偿交叉） |
| **扩展点** | `rebase_json` / `compute_reverse_patch` 可换 |
| **承上启下** | MS2 的准入依赖租约；MS5 的 merge 是本里程碑的子集 |

### MS2 · 预算与准入

| 项 | 内容 |
|---|---|
| **目标** | 嵌套预算守恒（I5）+ 准入谓词 |
| **新增模块** | `budget`、`dispatcher` |
| **依赖** | MS1 |
| **入口条件** | MS1 出口通过 |
| **出口条件** | `test_a4`（双花）、`test_a12`（并发 reserve）、`test_a13`（per-action guard） |
| **扩展点** | `BudgetManager` 可换分布式配额 |
| **承上启下** | MS4 的调度依赖准入；MS9 的分布式依赖配额 |

### MS3 · 崩溃恢复与幂等

| 项 | 内容 |
|---|---|
| **目标** | CheckpointStore（唯一恢复依据）+ EffectJournal（UNKNOWN 对账） |
| **新增模块** | `checkpoint`、`effect_journal` |
| **依赖** | MS0、MS1 |
| **入口条件** | MS1 出口通过 |
| **出口条件** | `test_u4`（崩溃恢复 + 副作用有效一次）、`test_a3`（补偿中途崩溃） |
| **扩展点** | `EffectJournal` 可换外部系统反查适配器 |
| **承上启下** | MS6 的 HITL 挂起依赖 checkpoint；MS8 的 conformance suite 依赖 I7 |

### MS4 · 编排与对账

| 项 | 内容 |
|---|---|
| **目标** | Planner/Dispatcher/Scheduler/RunCoordinator/ReconcileLoop/HITL |
| **新增模块** | `planner`、`scheduler`、`run_coordinator`、`reconcile`、`hitl` |
| **依赖** | MS1、MS2 |
| **入口条件** | MS2 出口通过 |
| **出口条件** | `test_u2`、`test_a2`（父 DONE 后子）、`test_a5`（深层取消）、`test_a6`（重规划风暴）、`test_a11`（finalize 误触发） |
| **扩展点** | `Planner` 可换 LLM 规划；`ReconcileLoop` 可换 K8s 式 controller |
| **承上启下** | MS5 的 logical_deps 由 Planner 声明；MS10 的 HA 依赖 ReconcileLoop |

### MS5 · 合并算法与逻辑依赖

| 项 | 内容 |
|---|---|
| **目标** | JSON-aware merge + logical_deps（write skew 拦截） |
| **新增模块** | `workspace.rebase_json` 增强、`planner.add_logical_edge` |
| **依赖** | MS1、MS4 |
| **入口条件** | MS4 出口通过 |
| **出口条件** | `test_u3`（批量迁移假冲突）、`test_a10`（write skew，🟡 部分通过） |
| **扩展点** | merge 策略可由 footprint 的 `merge_policy` 配置 |
| **承上启下** | MS8 的 L2 规范把 merge 语义写入「协调协议」 |

### MS6 · 上下文与背压

| 项 | 内容 |
|---|---|
| **目标** | ContextAssembler（物化视图 + cursor）+ SubscriptionRegistry（背压） |
| **新增模块** | `context`、`subscription` |
| **依赖** | MS0、MS3 |
| **入口条件** | MS3 出口通过 |
| **出口条件** | `test_a7`（事件风暴 + 快照重放）、`test_a8`（HITL + 崩溃 + 预算） |
| **扩展点** | `context_view` 可换增量物化；背压可换 credit-based |
| **承上启下** | MS7 的安全边界依赖 ctx 隔离 |

### MS7 · 安全边界

| 项 | 内容 |
|---|---|
| **目标** | CapabilityToken 强制（B4） |
| **新增模块** | `policy` |
| **依赖** | MS2、MS6 |
| **入口条件** | MS6 出口通过 |
| **出口条件** | `test_a9`（租约 renew/revoke 竞态）+ `test_capability_denied_blocks_tool` |
| **扩展点** | `PolicyEngine` 可换 OPA/RBAC |
| **承上启下** | MS10 的多租户依赖能力模型 |

### MS8 · L2 规范与 conformance suite

| 项 | 内容 |
|---|---|
| **目标** | 把 MS0–MS7 的行为固化为**规范 + 参考实现 + conformance suite** |
| **新增模块** | `harness/spec/`（资源模型 `resources.py` / 协调协议 `protocol.py` GRANT-WAIT-DIE / 收据格式 `receipt.py` / 不变量语料 `invariants.py`） |
| **依赖** | MS0–MS7 |
| **入口条件** | MS7 出口通过 |
| **出口条件** | `test_ms8_conformance.py`（I1/I2/I3/I5/I6/I7/I8）+ `test_ms8_spec.py`（I9/I10/I13 + 资源模型 + 协调协议 + 收据链）+ `test_ms7_capability.py`（I4）+ `test_a7/a11/a13`（I11/I12/I14）= **I1–I14 全覆盖** |
| **扩展点** | 规范与实现分离（实现可换） |
| **承上启下** | MS9 的分布式替换必须仍通过 conformance suite |

### MS9 · 分布式化（替换单写者）

| 项 | 内容 |
|---|---|
| **目标** | 用 Raft 组替换 EventLog/BudgetManager 的单写者（文档 §14 P0） |
| **新增模块** | `event_log_raft`、`budget_raft`（实现同一接口） |
| **依赖** | MS8 |
| **入口条件** | MS8 出口通过 |
| **出口条件** | 全部 35 个测试在 Raft 后端下仍通过 |
| **扩展点** | **这是 MS1–MS7 预留的最大扩展点** |
| **承上启下** | MS10 的 HA 前提 |

### MS10 · 生产化

| 项 | 内容 |
|---|---|
| **目标** | 可观测（Metrics/Trace）、Admin（pause/resume/kill）、优雅排空、DLQ |
| **新增模块** | `observability`、`admin`、`dlq` |
| **依赖** | MS8、MS9 |
| **入口条件** | MS9 出口通过 |
| **出口条件** | `test_dlq`（毒任务转 DEAD_LETTER）、`test_admin_pause_resume` |
| **扩展点** | OTel / Prometheus 适配器 |
| **承上启下** | 交付给外部用户 |

---

## 第三部分：里程碑依赖图

```
MS0 ─▶ MS1 ─▶ MS2 ─▶ MS4 ─▶ MS5 ─▶ MS8 ─▶ MS9 ─▶ MS10
       │       │       ▲       ▲       ▲
       ├───────┼──▶ MS3 ─┘       │       │
       │       │       │         │       │
       └───────┴───────┴──▶ MS6 ─┘       │
                               │         │
                               └──▶ MS7 ─┘
```

**关键路径**：`MS0 → MS1 → MS2 → max(MS4→MS5, MS3→MS6→MS7) → MS8 → MS9 → MS10`（8 步）

> **说明**：MS3（崩溃恢复）与 MS6（上下文/背压）在 MS2 之后**可并行**；MS7 依赖 MS6。关键路径取两者中的较长者。
**可并行**：`MS3` 与 `MS6` 可在 MS2 之后并行；`MS7` 依赖 MS6。

---

## 第四部分：承上启下与「反死代码」校验

| 里程碑 | 承上（用了前面的什么） | 启下（为后面提供什么） | 反死代码校验 |
|---|---|---|---|
| MS0 | — | 全局 seq + 事件流 | 被 MS1/MS3/MS6/MS8 引用 ✅ |
| MS1 | event_log | RSC 内核 | 被 MS2–MS10 引用 ✅ |
| MS2 | intent_registry | 准入 + 预算守恒 | 被 MS4/MS9 引用 ✅ |
| MS3 | event_log, workspace | 恢复依据 + 幂等键 | 被 MS6/MS8 引用 ✅ |
| MS4 | dispatcher, budget | 编排 + 对账 | 被 MS5/MS10 引用 ✅ |
| MS5 | planner, workspace | merge 语义 | 被 MS8 引用 ✅ |
| MS6 | checkpoint, event_log | 物化视图 + 背压 | 被 MS7/MS8 引用 ✅ |
| MS7 | context, budget | 能力模型 | 被 MS10 引用 ✅ |
| MS8 | 全部 | 规范（实现可换） | 被 MS9 引用 ✅ |
| MS9 | 全部 | 分布式后端 | 被 MS10 引用 ✅ |
| MS10 | 全部 | 生产交付 | 终点（用户） ✅ |

> **无死代码的证明**：每个模块至少被 1 个**后续里程碑**引用。若某模块无下游，应在设计阶段删除，而非实现。

---

## 第五部分：硬编码禁令

| 禁止 | 原因 | 正确做法 |
|---|---|---|
| 硬编码资源路径 | 无法适配不同工作区 | 由 `footprint` 声明 |
| 硬编码预算值 | 无法参数化 | 由 `BudgetRequest` 声明 |
| 硬编码 merge 策略 | 无法按资源类型配置 | 由 `merge_policy` 声明 |
| 硬编码超时（`LEASE_MS`/`HITL_TIMEOUT`） | 无法按场景配置 | 由 `PolicyEngine`/`Task spec` 注入 |
| 硬编码状态转移 | 无法演进 | 由状态机表驱动（§5.9） |
| 硬编码事件类型 | 无法扩展 | 由 `EventKind` 枚举 + schema_version |

> **本实现已遵守**：所有常量集中在 `harness/runtime.py` 顶部（`LEASE_MS`/`GUARD_EVERY`/`AGING_MAX`），并在 `Harness.__init__` 中可注入。

---

## 附：可执行基线

```bash
cd /workspace && python3 -m pytest tests/ -q     # 35 passed
```

| 测试文件 | 覆盖 |
|---|---|
| `tests/test_ms0_event_log.py` | MS0 出口：seq 单调（2 个测试） |
| `tests/test_real_cases.py` | U1（2 个）、U2、U3、U4 |
| `tests/test_adversarial.py` | A1–A13（含 A7b 背压） |
| `tests/test_ms7_capability.py` | MS7 出口：能力强制（2 个测试） |
| `tests/test_ms8_conformance.py` | MS8 出口：I1/I2/I3/I5/I6/I7/I8 |
| `tests/test_ms8_spec.py` | MS8 出口：I9/I10/I13 + 资源模型 + 协调协议 + 收据链 |

> **总计 35 个测试**：19 个用例测试（U1×2 + U2–U4 + A1–A13 + A7b）+ 16 个里程碑出口/规范测试。
