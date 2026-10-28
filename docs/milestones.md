# 里程碑与模块划分（Milestones & Module Breakdown）

| 项目 | 内容 |
|---|---|
| 版本 | v1.1（交付前终审） |
| 日期 | 2026-10-26 |
| 前置 | [设计规范 v1.12](multi-agent-harness-design-final.md) · [用例集](use-cases-and-adversarial-tests.md) |
| 可执行基线 | `harness/`（**35 个测试全通过**，见 §附：基线边界） |
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
| 1 | `types` | L1 | 纯数据类型 | `ACB` / `Event` / `Status` | 全量（被所有测试间接覆盖） | 全部 |
| 2 | `event_log` | L1 | 全局 seq + 订阅通知 | `append_in_txn` / `replay` / `has` | test_ms0_event_log | 全部 |
| 3 | `intent_registry` | L1 | 预留 + fencing | `reserve` / `is_valid` / `revoke` / `renew` | test_a9 | runtime, workspace |
| 4 | `workspace` | L1 | RSC 提交 + 3-way merge | `stage` / `prepare` / `apply` / `rebase_json` | test_u1×2/u2/u3/a1 | runtime |
| 5 | `workspace_versions`（类） | L1 | 前像快照（供 merge/补偿） | `record` / `get` | test_a1/a3 | workspace |
| 6 | `budget` | L1 | 嵌套预算守恒 | `reserve` / `consume` / `release` / `check` | test_a4/a12/a13 | runtime, dispatcher |
| 7 | `checkpoint` | L1 | 唯一恢复依据 | `save` / `load` | test_u4/a3 | runtime, harness |
| 8 | `effect_journal`（类） | L1 | 幂等键 + UNKNOWN 对账 | `begin` / `confirm` / `mark_unknown` | test_u4/a6 | runtime |
| 9 | `subscription` | L1 | 读即订阅 + 背压 | `add` / `subscribers` / `is_backpressured` | test_a7×2 | harness（`_on_event`）, runtime（读集） |
| 10 | `context` | L1 | 物化视图 + cursor | `context_view` / `replay` | test_a7/a8 | harness（`context_view` 调用点；MS6 接线到 runtime） |
| 11 | `policy` | L1（P4 平面） | CapabilityToken 强制 | `check_capability` / `issue` | test_ms7_capability×2 | runtime |
| 12 | `runtime` | L4（P2 平面） | agent 主循环 + guard | `run` / `guard` | 全部 | harness |
| 13 | `planner` | L4 | Plan + footprint + patch | `plan` / `patch` / `add_logical_edge` | test_u3/a6 | harness |
| 14 | `dispatcher` | L4（P3 平面） | 准入 | `try_admit` | test_a4/a12 | harness（**MS4 接线进 `run_task`**） |
| 15 | `scheduler` | L4 | ready_queue | `push` / `pop` | test_a11 | harness（**MS4 接线：pop 后真正启动**） |
| 16 | `run_coordinator` | L4 | Run 状态唯一写者 + 取消广播 | `cancel_run` / `submit_command` | test_a2/a5 | runtime, harness |
| 17 | `reconcile` | L4 | level-triggered 对账 | `rebuild_ready_set` / `reconcile_events` | test_a11 | harness |
| 18 | `hitl` | L4 | 审批处理器 | `resolve` / `set_result` / `clear`（**目标接口** `raise`/`wait_resume`/`get_result` 见设计 §4.2，MS4 补齐） | test_a8 | runtime |
| 19 | `harness` | L5 | 组装 + 端到端 | `run_task` / `commit_batch` / `compensate` / `recover` | 全部 | 用户 |

> **命名说明（v1.1）**：`workspace_versions` 与 `effect_journal` 是**类**（`WorkspaceVersionStore`、`EffectJournal`），分别位于 `workspace.py` 与 `checkpoint.py`，不是独立文件 —— 保留为独立行是因为它们是**独立的可替换点**。
> **层号说明**：`层` 列是**交付归属 L1–L5**（设计 §0.5），与 `P0–P5 平面`（设计 §4.1）正交。`policy` 属 L1 交付 + P4 治理面；`dispatcher` 属 L1 交付 + P3 控制面；`runtime` 属 L4 参考实现 + P2 执行面。

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

> **当前落点（v1.1 诚实标注）**：`Harness.__init__` 已把可调常量参数化（`run_limits` / `retention` / `lease_ms` / `guard_every` / `aging_max` / `max_steps` / `max_concurrent` / `policy_default_allow`）；但**存储类组件的接口抽象（`Protocol`/ABC + 构造注入）尚未落地** —— 现在全部是具体类直接实例化。这是 **MS9 的前置条件**（见 §六 D2），也是上表「可换」声明的唯一未兑现项。

### 1.4 设计中已有、基线尚未实现的组件（实现阶段补齐）

设计规范 §4.2 定义了 30+ 组件，`harness/` 基线只落地了并发正确性内核相关的 19 个。其余组件**不是死代码**，而是后续里程碑的交付物：

| 组件 | 平面 | 归属里程碑 | 说明 |
|---|---|---|---|
| `PlanValidator`（DAG/schema/footprint/预算校验） | P3 | MS4 | 当前 `Planner.plan` 内联做了环检测，未独立成组件 |
| `Replanner` | P3 | MS4 | 当前由 `Planner.patch` 承担 |
| `Supervisor`（心跳/租约/超时/孤儿回收） | P3 | MS4 | 当前租约回收散在 `IntentRegistry.revoke` |
| `Ingress`（鉴权/用户幂等/建 Run） | P5 | MS10 | 当前 `run_task` 直接接收 ACB |
| `Renderer`（流式输出） | P5 | MS10 | — |
| `ApplicabilityGate`（是否值得多智能体） | P3 | MS4 | 当前无 |
| `Aggregator`（覆盖度/矛盾检测） | P3 | MS4 | 当前无（`DONE_PARTIAL` 无产生路径） |
| `VerifierRunner`（独立上下文校验） | P2 | MS5 | 当前无 |
| `ModelGateway` / `ToolGateway` | P2 | MS3/MS7 | 当前 `runtime._do_action` 内联工具执行与幂等 |
| `PlanStore` / `ArtifactStore` | P1 | MS3 | 当前 `Harness.plans` 是内存 dict |
| `DurableTimer`（跨月 HITL 超时） | P3 | MS4 | 当前 `HITL.resolve` 无超时 |
| `Trace/Audit`† / `SecretBroker`† / `MemoryStore`† | P4/P1 | **不交付（L5）** | 只留接口契约（设计 §0.5） |

> 这张表的存在是为了**避免「方案里写了、里程碑里没有、实现时忘掉」**。若某组件最终不做，必须在本表标注「不做」并说明理由，而不是让它悄悄消失。

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
| **目标** | 实现「预留-暂存-提交 + fencing + CAS + 补偿」，以及驱动它的最小执行循环 |
| **新增模块** | `intent_registry`、`workspace`、`workspace_versions`、`runtime`（最小执行循环：stage→commit） |
| **依赖** | MS0 |
| **入口条件** | MS0 出口通过 |
| **出口条件** | `test_u1_parallel_refactor_serial_branch`、`test_u1_parallel_refactor_concurrent_conflict_and_merge`、`test_a1_compensation_cross_no_overwrite`、`test_a9_lease_renew_revoke_race`（fencing 归属本里程碑） |
| **扩展点** | `rebase_json` / `compute_reverse_patch` 可换 |
| **承上启下** | MS2 的准入依赖租约；MS2 的 per-action guard 依赖本里程碑的 `runtime.guard`；MS5 的 merge 是本里程碑的子集 |

### MS2 · 预算与准入

| 项 | 内容 |
|---|---|
| **目标** | 嵌套预算守恒（I5）+ 准入谓词 |
| **新增模块** | `budget`、`dispatcher` |
| **依赖** | MS1 |
| **入口条件** | MS1 出口通过 |
| **出口条件** | `test_a4_budget_double_spend`、`test_a12_concurrent_reserve_no_overcommit`、`test_a13_per_action_guard_budget` |
| **扩展点** | `BudgetManager` 可换分布式配额 |
| **承上启下** | MS4 的调度依赖准入；MS9 的分布式依赖配额 |

### MS3 · 崩溃恢复与幂等

| 项 | 内容 |
|---|---|
| **目标** | CheckpointStore（唯一恢复依据）+ EffectJournal（UNKNOWN 对账） |
| **新增模块** | `checkpoint`、`effect_journal` |
| **依赖** | MS0、MS1 |
| **入口条件** | MS1 出口通过 |
| **出口条件** | `test_u4_long_task_crash_recovery_idempotent`（崩溃恢复 + 副作用有效一次）、`test_a3_compensation_crash_midway`（补偿中途崩溃） |
| **扩展点** | `EffectJournal` 可换外部系统反查适配器 |
| **承上启下** | MS6 的 HITL 挂起依赖 checkpoint；MS8 的 conformance suite 依赖 I7 |
| **已知缺口** | `test_a3` 当前是**测试自造状态**（手工置 `w2.compensated=True`），未真正走 `recover` 扫描 `compensation_journal`。本里程碑内必须补齐「`COMPENSATING` 意图落盘 → `recover` 续做」路径（设计 §5.4 v1.12 修复），否则本里程碑不算通过 |

### MS4 · 编排与对账

| 项 | 内容 |
|---|---|
| **目标** | Planner/Dispatcher/Scheduler/RunCoordinator/ReconcileLoop/HITL |
| **新增模块** | `planner`、`scheduler`、`run_coordinator`、`reconcile`、`hitl` |
| **依赖** | MS1、MS2 |
| **入口条件** | MS2 出口通过 |
| **出口条件** | `test_a2_parent_done_child_budget_transfer`、`test_a5_deep_nested_cancellation_broadcast`、`test_a6_replan_storm_idempotent`、`test_a11_finalize_not_triggered_with_nonterminal` |
| **扩展点** | `Planner` 可换 LLM 规划；`ReconcileLoop` 可换 K8s 式 controller |
| **承上启下** | MS5 的 logical_deps 由 Planner 声明；MS10 的 HA 依赖 ReconcileLoop |
| **接线要求（v1.1）** | 本里程碑必须把 `dispatcher` 接进 `run_task`（当前只在测试里被直接调用）、`scheduler.pop` 必须真正启动任务（当前弹出即丢弃）、`context_view` 接进 `runtime` 的推理步。否则这三个模块是**执行路径上的死代码** |

### MS5 · 合并算法与逻辑依赖

| 项 | 内容 |
|---|---|
| **目标** | JSON-aware merge + logical_deps（write skew 拦截） |
| **新增模块** | `workspace.rebase_json` 增强、`planner.add_logical_edge` |
| **依赖** | MS1、MS4 |
| **入口条件** | MS4 出口通过 |
| **出口条件** | `test_u3_batch_migration_json_aware_merge`、`test_u2_parallel_review_conflict_and_rebase`、`test_a10_write_skew_logical_deps`（**必须从 🟡 提升为 ✅**：write skew 被 `logical_deps` 拦截；仍是 🟡 则本里程碑不通过） |
| **扩展点** | merge 策略可由 footprint 的 `merge_policy` 配置 |
| **承上启下** | MS8 的 L2 规范把 merge 语义写入「协调协议」 |
| **接线要求（v1.1）** | `CONFLICT`/`REJECT_STALE_READ` 必须接进执行循环（`handle_commit_result`，当前缺失，`runtime` 对非 OK 一律 `FAILED`），否则「冲突 → 语义 rebase」只是文档承诺 |

### MS6 · 上下文与背压

| 项 | 内容 |
|---|---|
| **目标** | ContextAssembler（物化视图 + cursor）+ SubscriptionRegistry（背压） |
| **新增模块** | `context`、`subscription` |
| **依赖** | MS0、MS3、MS4（`test_a8` 用到 `HITL`） |
| **入口条件** | MS3 出口通过 |
| **出口条件** | `test_a7_event_storm_cursor_snapshot_replay`、`test_a7_backpressure_degrades_to_snapshot`、`test_a8_hitl_crash_budget_compensation` |
| **扩展点** | `context_view` 可换增量物化；背压可换 credit-based |
| **承上启下** | MS7 的安全边界依赖 ctx 隔离 |
| **已知缺口** | `retention` 只赋值不读取（cursor 无 floor）→ I11 未被真正验证；`ack` 无生产调用 → `inflight` 单调增长。本里程碑内必须补齐 |

### MS7 · 安全边界

| 项 | 内容 |
|---|---|
| **目标** | CapabilityToken 强制（B4） |
| **新增模块** | `policy` |
| **依赖** | MS2、MS6 |
| **入口条件** | MS6 出口通过 |
| **出口条件** | `test_capability_denied_blocks_tool`（未签发 token → **拒绝**）、`test_toolgateway_enforces_capability`（模型发出的越权动作被拦，无副作用） |
| **扩展点** | `PolicyEngine` 可换 OPA/RBAC |
| **承上启下** | MS10 的多租户依赖能力模型 |
| **已知缺口** | `Harness(policy_default_allow=True)` 与 `PolicyEngine(default_allow=False)` 默认值相反，且前者使「未签发 token 即放行」→ **I4 能力封闭在默认配置下不成立**。本里程碑必须改为 deny-by-default，并由测试夹具显式签发通配 token（见 §六 D13） |

### MS8 · L2 规范与 conformance suite

| 项 | 内容 |
|---|---|
| **目标** | 把 MS0–MS7 的行为固化为**规范 + 参考实现 + conformance suite** |
| **新增模块** | `harness/spec/`（资源模型 `resources.py` / 协调协议 `protocol.py` GRANT-WAIT-DIE / 收据格式 `receipt.py` / 不变量语料 `invariants.py`） |
| **依赖** | MS0–MS7 |
| **入口条件** | MS7 出口通过 |
| **出口条件** | `test_ms8_conformance.py`（I1/I2/I3/I5/I6/I7/I8）+ `test_ms8_spec.py`（I9/I10/I13 + 资源模型 + 协调协议 + 收据链）+ `test_ms7_capability.py`（I4）+ `test_a7/a11/a13`（I11/I12/I14） |
| **覆盖强度（v1.2 已按代码复核修正）** | **强**（断言机制本身）：I1/I2/I3/I5/I7；**强（S9–S14 已补齐，原 v1.1 的自评已过期）**：I11（`retention` 在 `context.py` 真读取、`floor_for` 在 `harness.py` per-run 实现，S12 K-33）、I13（`RECEIPT_ISSUED` 由 `commit_batch` 在 `harness.py` 真产生）、I14（`no_progress_steps` 在 `runtime.py` 写入、`NO_PROGRESS` 谓词可达）；**中**：I4/I6/I8/I9/I10；**S14 补强**：I12（新增 `test_s14_i12.py`：aging 有界离开 ADMITTED，不再只断言「存在非终态」） |
| **扩展点** | 规范与实现分离（实现可换） |
| **承上启下** | MS9 的分布式替换必须仍通过 conformance suite |

### MS9 · 分布式化（替换单写者）

| 项 | 内容 |
|---|---|
| **目标** | 用 Raft 组替换 EventLog/BudgetManager 的单写者（文档 §14 P0） |
| **新增模块** | `event_log_raft`、`budget_raft`（实现同一接口） |
| **依赖** | MS8 |
| **入口条件** | MS8 出口通过 |
| **出口条件（v1.3 重述，S16）** | 原「全部 35 个测试在 Raft 后端下仍通过」写于测试只有 35 个时，现 563 个且 500+ 与共识无关 → 重述为可判定四条：① **S15 后端一致性套件**（EventLog 5 组断言）在 `memory`/`wal`/**`raft`** 三后端全过；② `RaftEventLog` 通过 `protocols.EventLog` 契约；③ `Harness(event_log=raft_log)` 跑通 E2E 到 `DONE`；④ 三副本收敛一致（同 seq/同内容）。**未完成**：`budget_raft` 见下方「已知边界」 |
| **扩展点** | **这是 MS1–MS7 预留的最大扩展点** |
| **前置条件（v1.2 已按代码复核修正）** | 「实现同一接口」要求先有接口。**实测：`Harness.__init__` 已支持全组件 keyword 注入**（`event_log`/`intents`/`workspace`/`budget`/`checkpoints`/`effects`/`policy`/`context`/`subscriptions`/`clock`/`blobs`），故 v1.1 的「无构造注入、MS9 不得开工」**已过期**。真正缺的只有两件：① 无 `Protocol`/`ABC` 契约声明（`grep` 零命中）→ 无法类型校验替代后端；② 无后端一致性测试套件。二者均为纯 Python，见 S15 大类 C |
| **承上启下** | MS10 的 HA 前提 |

### MS10 · 生产化

| 项 | 内容 |
|---|---|
| **目标** | 可观测（Metrics/Trace）、Admin（pause/resume/kill）、优雅排空、DLQ |
| **新增模块** | `observability`、`admin`、`dlq` |
| **依赖** | MS8、MS9 |
| **入口条件** | MS9 出口通过 |
| **出口条件** | `test_dlq`（毒任务转 `DEAD_LETTER` —— **产生路径已有 4 条**：aging `AGING_EXHAUSTED`、`escalate_quarantined`、replan 预算耗尽、aging 上限；缺的是「毒任务自动识别 + redrive」的测试）、`test_admin_pause_resume`（**需新增**：当前无 pause/resume 语义）、`test_ingress_idempotent`（**需新增**：`RunCoordinator.submit_command` 仍是空壳） |
| **扩展点** | OTel / Prometheus 适配器 |
| **承上启下** | 交付给外部用户 |

---

## 第三部分：里程碑依赖图

```
MS0 ─▶ MS1 ─▶ MS2 ─┬─▶ MS4 ─▶ MS5 ─┐
        │           │                │
        ├──▶ MS3 ───┴──▶ MS6 ─▶ MS7 ─┤
        │                            │
        └────────────────────────────┴─▶ MS8 ─▶ MS9 ─▶ MS10
```

**依赖边（与各里程碑「依赖」行严格一致）**：

| 里程碑 | 入边 |
|---|---|
| MS1 | MS0 |
| MS2 | MS1 |
| MS3 | MS0, MS1 |
| MS4 | MS1, MS2 |
| MS5 | MS1, MS4 |
| MS6 | MS0, MS3, MS4 |
| MS7 | MS2, MS6 |
| MS8 | MS0–MS7 |
| MS9 | MS8 |
| MS10 | MS8, MS9 |

**关键路径**：`MS0 → MS1 → MS2 → MS4 → MS5 → MS8 → MS9 → MS10`（8 个里程碑；另一支 `MS0 → MS1 → MS3 → MS6 → MS7 → MS8` 为 6 个，更短）

> **说明**：MS3（崩溃恢复）与 MS4（编排）在 MS1/MS2 之后**可并行**；MS6 依赖 MS4（`test_a8` 需要 `HITL`）；MS7 依赖 MS6。
> **v1.1 修复**：原图多画了 `MS3→MS4`、`MS1/MS2→MS6` 两条边，与依赖表冲突，已按表重画。

---

## 第四部分：承上启下与「反死代码」校验

| 里程碑 | 承上（用了前面的什么） | 启下（为后面提供什么） | 反死代码校验（里程碑引用） | 代码级接线状态（v1.1 复核） |
|---|---|---|---|---|
| MS0 | — | 全局 seq + 事件流 | 被 MS1/MS3/MS6/MS8 引用 ✅ | ✅ 生产路径（所有提交都写事件） |
| MS1 | event_log | RSC 内核 | 被 MS2–MS10 引用 ✅ | ✅ 生产路径 |
| MS2 | intent_registry | 准入 + 预算守恒 | 被 MS4/MS9 引用 ✅ | ⚠️ `budget` 在生产路径；`dispatcher.try_admit` 仅测试调用（MS4 接线） |
| MS3 | event_log, workspace | 恢复依据 + 幂等键 | 被 MS6/MS8 引用 ✅ | ⚠️ `effects` 在生产路径；`checkpoint.save/load` 仅 `recover` 用；`compensation_journal` 未被 `recover` 扫描（MS3 补） |
| MS4 | dispatcher, budget | 编排 + 对账 | 被 MS5/MS10 引用 ✅ | ⚠️ `planner`/`run_coordinator` 生产路径；`scheduler.pop` 丢弃返回值、`reconcile.rebuild_ready_set` 返回值被丢弃、`submit_command` 空实现（MS4 接线） |
| MS5 | planner, workspace | merge 语义 | 被 MS8 引用 ✅ | ⚠️ `rebase_json` 仅测试调用（`handle_commit_result` 缺失，MS5 接线） |
| MS6 | checkpoint, event_log | 物化视图 + 背压 | 被 MS7/MS8 引用 ✅ | ⚠️ `context_view`/`is_backpressured` 仅测试调用；`on_change`/`ack` 无生产调用（MS6 接线） |
| MS7 | context, budget | 能力模型 | 被 MS10 引用 ✅ | ⚠️ `policy` 在 `runtime` 生产路径，但默认放行（MS7 改 deny-by-default） |
| MS8 | 全部 | 规范（实现可换） | 被 MS9 引用 ✅ | ⚠️ `spec/receipt` 运行时从不产生收据（MS8 补 I13） |
| MS9 | 全部 | 分布式后端 | 被 MS10 引用 ✅ | ⏳ 未开工（依赖 D2 接口抽象） |
| MS10 | 全部 | 生产交付 | 终点（用户） ✅ | ⏳ 未开工 |

> **无死代码的证明**：每个模块至少被 1 个**后续里程碑**的出口条件引用。
> **但「被里程碑引用」≠「已接线」**：上表右列是代码级复核结果。标 ⚠️ 的模块目前只在测试里被驱动，**必须在对应里程碑内接进执行路径**，否则它们在生产路径上就是死代码。这是 v1.1 新增的**可验收判据**：对应里程碑的「接线要求」行未满足，不得标记完成。

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

> **基线合规状态（v1.1 逐条复核，不再笼统声称「已遵守」）**：
>
> | 禁令 | 状态 | 证据 / 待办 |
> |---|---|---|
> | 硬编码资源路径 | ✅ 合规 | 由 `footprint` / `stage(resource=...)` 声明 |
> | 硬编码预算值 | ⚠️ 部分 | `BudgetRequest` 已用；但 `Planner` 的 `spec.get("min_budget", 10)` 与 `BudgetManager(run_limits=1000)` 有默认值（可注入，但默认值散落） |
> | 硬编码 merge 策略 | ⚠️ 部分 | `spec/resources.py` 有 `merge_policy` 枚举，但 `Planner` footprint **未携带该字段**，merge 策略实际不可按资源配置（MS5 补） |
> | 硬编码超时 | ⚠️ 部分 | 常量在 **`Harness.__init__`**（`lease_ms`/`guard_every`/`aging_max`/`max_steps`/`max_concurrent`/`retention`/`run_limits`），**不在 `runtime.py` 顶部**；`IntentRegistry` 的 `ttl_ms=5000` 与 `Task.retry_budget=3` 仍是类内默认值；`HITL_TIMEOUT` 尚未存在（MS4 补） |
> | 硬编码状态转移 | ❌ 未合规 | 状态赋值散落在 `runtime.py` 各处，**非表驱动**；设计 §5.9 是表，代码不是（MS4 补） |
> | 硬编码事件类型 | ⚠️ 部分 | `EventKind` 枚举 ✅；但 `Event` 无 `schema_version` 字段（仅 `spec/receipt.py` 有） |
>
> 结论：**禁令方向正确，基线只兑现了一部分**。上表 ⚠️/❌ 项是 MS3–MS7 的验收内容，不能算「已遵守」。

---

## 第六部分：实现前必须落定的决策（D1–D16）

> 这些是**写第一行代码前就要定**的问题。每项给「推荐默认值」——若你同意，可以直接按推荐值开工；不同意就改这一行，不要留到实现中途才发现。

| # | 决策 | 为什么必须先定 | 推荐默认值 | 影响 |
|---|---|---|---|---|
| **D1** | **权威文档版本** | `rw-partition v0.1` 主张悲观读写锁 + 租约；`dispatch v0.2` 用单调 fencing token；`final v1.12` 用按租约有效性 + RSC。三份并存会被误读 | **以 `multi-agent-harness-design-final.md` v1.12 为唯一权威**；`rw-partition`/`dispatch`/`dataflow` 标注为「历史专题稿，机制以 final 为准」 | 全部 |
| **D2** | **扩展点形态** | 当前全是具体类直接实例化，「可替换」无落点；MS9 的「Raft 实现同一接口」无法开工 | 对 `EventLog` / `BudgetManager` / `IntentRegistry` / `PolicyEngine` / `CheckpointStore` / `EffectJournal` 定义 `Protocol`，`Harness.__init__` 接受注入（默认给内存实现） | MS9、全部 |
| **D3** | **持久化边界** | 「EventLog = WAL」「Checkpoint = 唯一恢复依据」若只落内存，崩溃恢复与跨月 HITL 都是假的 | 阶段一：`EventLog`/`CheckpointStore`/`EffectJournal`/`compensation_journal` **必须落盘**（append-only 文件即可）；`workspace` 内容可先内存 + 快照 | MS3、MS9、MS10 |
| **D4** | **锁粒度与 LOCK_ORDER** | 现在是单把全局 `RLock`；设计要求按资源短临界区。改成每资源锁后，**锁排序规则未定义就是死锁源头** | 每资源锁 + **按 `resource_id` 字典序**获取；跨资源批量提交时先全排序再逐个加锁；禁止嵌套持锁 | MS1、MS9 |
| **D5** | **冲突处理接入点** | `handle_commit_result` 不存在，`runtime` 对非 OK 一律 `FAILED`，「冲突 → rebase」是空承诺 | 在 `runtime.run` 的动作循环里加 `handle_commit_result`：`CONFLICT`/`REJECT_STALE_READ` → 有界 rebase 重试；`REJECT_STALE` → 重取租约；超限 → `QUARANTINED` | MS5 |
| **D6** | **事件 schema 定稿** | `Event` 缺 `event_id`/`run_id`/`ts`/`schema_version`，`payload` 不带 `content`，导致 I6/I8 无法验证 | `Event = (seq, event_id, run_id, ts, kind, subject, payload, producer, caused_by, idempotency_key, schema_version=1)`；`RESOURCE_COMMITTED.payload` 带 `content_hash` + `diff`（不带全量内容） | MS0、MS6、MS8 |
| **D7** | **retention / 快照间隔 / cursor floor** | I11「有界」依赖具体数值；`retention` 现在只赋值不读取 | `retention` = 事件条数上限（默认 10000）；每 K=100 条事件或每 30s 打快照；`cursor < tail - retention` → 强制从最近快照重建 | MS6 |
| **D8** | **`idem_key` 最终算法** | 规范是嵌套 hash，实现是字符串拼接；且默认 `logical_subgoal_id = "S-{task_id}"` 在 replan 后变化 → I7 失效 | `idem_key = sha256(run_id ‖ logical_subgoal_id ‖ sha256(tool ‖ canonical_json(args)))`；`logical_subgoal_id` **必须由 Planner 显式声明且跨 replan 稳定**，缺省时报错而非派生 | MS3、MS4 |
| **D9** | **`resume_policy` 与 epoch 语义** | `Task.resume_policy` 字段不存在；恢复时「重跑 / 续跑 / 补偿」未定义 | `resume_policy ∈ {RERUN, RESUME, COMPENSATE}`，默认 `RESUME`；每次 replan 递增 `epoch`，旧 epoch 的写一律 `REJECT_STALE` | MS3 |
| **D10** | **状态机终态与唯一写者** | `TERMINAL_STATES` 零引用；`DONE_PARTIAL` 不在其中却被当终态；`DEAD_LETTER`/`QUARANTINED`/`REPLANNING` 无产生路径 | `TERMINAL_STATES = {DONE, DONE_PARTIAL, FAILED, CANCELLED, DEAD_LETTER}`；**只有 `RunCoordinator` 能写状态**（B1），其他组件提交 `Command` | MS4、MS10 |
| **D11** | **HITL 契约** | 现在是 stub（`resolve` 恒 `approved=True`），`APPROVAL_TIMEOUT` 不可达；`DurableTimer` 不存在 | `HITL.raise(acb, reason) → checkpoint + 释放写锁`；`wait_resume(acb) → Signal/DurableTimer`；`get_result()`；超时 → `FAILED(APPROVAL_TIMEOUT)`；取消可中断 | MS4、MS6 |
| **D12** | **预算释放时机** | `cancel_run` 不释放预算；`compensate`/`propagate_failure` 的释放口径未定 → 预算泄漏 | 释放点唯一：**进入终态时**（含 `CANCELLED`/`DEAD_LETTER`/`DONE_PARTIAL`）由 `RunCoordinator` 统一 `release`；补偿不重复释放（`release` 幂等） | MS2、MS4 |
| **D13** | **能力默认值与 scope 维度** | `Harness` 默认放行、`PolicyEngine` 默认拒绝，两处相反；scope 设计四维、实现两维 | **deny-by-default**（`policy_default_allow=False`）；scope 四维 `tool/path/network/data_class`；测试夹具显式签发 `{"*"}` 通配 token | MS7 |
| **D14** | **里程碑「通过」定义** | MS5 出口含 🟡 用例，门不可判 | **出口条件必须全 ✅**；🟡 视为未通过，必须在本里程碑内修到 ✅ 或降级为「明确不承诺」并写进设计 | MS5、MS8 |
| **D15** | **里程碑独立可运行性** | 当前所有测试依赖完整 `Harness`，无法证明「按序独立交付」 | 每个里程碑提供 `pytest -m msN` 子集，且**只依赖 ≤N 的模块**；用 `pytest.ini` 的 markers 固化 | 全部 |
| **D16** | **测试是否允许绕过 `run_task`** | `conftest.admit` 直接调 `dispatcher.try_admit`，绕过了真实入口 → MS2/MS4 出口可能「测了假路径」 | 允许**单元级**直调，但每个里程碑**至少 1 个端到端用例走 `run_task`**，否则出口不算通过 | MS2、MS4 |

> **开工建议顺序**：D1 → D2 → D6 → D10（这四项决定数据结构和接口，改动成本最高）；其余可在对应里程碑开工前定。

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

### 基线边界（必须知道它「不是」什么）

`harness/` 是**设计验证夹具**，不是生产实现。35 passed 证明的是「RSC 内核的关键路径自洽」，**不等于**下列能力已具备：

| 测试 | 真实强度 | 差距 |
|---|---|---|
| `test_a3_compensation_crash_midway` | 🔴 自造状态 | 手工置 `w2.compensated=True` 后直调 `compensate`，**未走 `recover`**；`compensation_journal` 未被恢复流程扫描 → 崩溃中途补偿未验证（MS3 补） |
| `test_a7_event_storm_cursor_snapshot_replay`（I11） | 🔴 未验证 | 只断言 cursor 追平 tail；`retention` 未读取、无 cursor floor |
| `test_a11_finalize_not_triggered_with_nonterminal`（I12） | 🟡 弱 | 只断言「存在非终态任务」，未验证不饿死 |
| `test_a13_per_action_guard_budget`（I14） | 🟡 错位 | 断言 `BUDGET_EXHAUSTED`，与 `NO_PROGRESS` 无关；`no_progress_steps` 从无写入 |
| `test_i13_receipt_chain` | 🔴 未落地 | 只测 spec 层 hash-chain，运行时从不产生收据 |
| `test_i8_event_atomic_with_replace` | 🟡 弱 | 只断言「提交后事件存在」，未注入失败验证原子性（`append_in_txn` ≡ `append`） |
| `test_capability_denied_blocks_tool` | 🔴 反向 | 断言「未签发 token → **放行**」，与 I4「能力封闭」相反（MS7 改 deny-by-default） |

> 上表 🔴/🟡 项**不影响「设计可行」的判断**（机制在文档里是完整的），但**影响「实现已完成」的判断**——它们是 MS3/MS6/MS7/MS8 的验收内容，不是已完成项。这份清单的作用是：**开工时不要以为这些已经做完了**。
