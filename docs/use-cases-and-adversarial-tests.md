# 用例集：4 个真实用例 + 13 个对抗用例（落盘）

| 项目 | 内容 |
|---|---|
| 版本 | v1.0 |
| 日期 | 2026-10-26 |
| 输入 | 4 个真实用例（人工构造）+ 13 个对抗用例（子智能体构造） |
| 用途 | 让「4/4 真实跑通、12/13 对抗跑通」的质量声明**可审计、可复现** |
| 前置 | [设计规范 v1.12](multi-agent-harness-design-final.md) |

> **为什么需要本文**：v1.7 的修订记录声称「4/4 真实跑通、12/13 对抗跑通」，但全文未枚举这 17 个用例，导致**不可审计**。本文把全部用例落盘。

---

## 第一部分：4 个真实用例

### U1 并行重构同一模块（3 agent 改 `src/auth/`，共享 `types.ts`）

**初始状态**：Run R1=`NEW`；`src/auth/types.ts`=v0；T1=`login.ts`、T2=`session.ts`、T3=`oauth.ts`。三者 footprint 均把 `types.ts` 声明为 **`contingent`**（可能写、不预留），不进入 `committed_writes`。

**分支 A（DAG 串行）**：`parallel(T1,T2)=false`（保守策略：`contingent` 重叠也建依赖边）→ 三 agent 串行。

| # | Actor | 状态前 | 动作 | 产生数据 | 状态后 | 不变量 |
|---|---|---|---|---|---|---|
| 1 | Ingress | — | `submit(req, idem_key)` | RunHandle | Run: NEW | ✅ |
| 2 | Planner | — | `plan()` | Plan v1（footprint+min_budget） | — | ✅ I9 |
| 3 | PlanValidator | Plan v1 | `validate()` | Valid | VALID | ✅ |
| 4 | Dispatcher | T1..T3 PLANNED | `try_admit` | T1 `ADMITTED`；T2/T3 等待 | — | ✅ |
| 5 | Scheduler | ready={T1} | `launch(T1)` | ACB_A1 | RUNNING | ✅ |
| 6 | A1 | RUNNING | `stage(login.ts, types.ts)` | staged | — | ✅ |
| 7 | A1 | RUNNING | `commit_batch` | `login.ts@v1`、`types.ts@v1` + 事件 | — | ✅ I1/I2/I8 |
| 8 | A1 | RUNNING | FINISH + `contract.validate` | RE | DONE | ✅ |
| 9 | Scheduler | T1 DONE | `launch(T2)` | ACB_A2 | RUNNING | ✅ |
| 10 | A2 | RUNNING | `commit(session.ts, types.ts, base v1)` | `session.ts@v1`、`types.ts@v2` | — | ✅ I2 |
| 11 | A3 | RUNNING | `commit(oauth.ts, types.ts, base v2)` | `oauth.ts@v1`、`types.ts@v3` | DONE | ✅ |
| 12 | Aggregator→Verifier | — | `reduce`+`verify` | 覆盖度 OK | Run: DONE | ✅ |

**分支 B（contingent 并发）**：T1/T2/T3 同时准入（T2/T3 的 `types.ts` 为 `contingent`，不预留）；A1 先提交 `types.ts@v1`；A2/A3 首次写 `types.ts` 时触发 **scope promotion**，**显式重声明 footprint**（`contingent → committed`）并重新准入；随后 `CONFLICT` → 3-way merge → 重新提交。

**最终状态**：`types.ts@v3`、`login/session/oauth.ts@v1`、Run `DONE`。
**验证的不变量**：I1/I2/I8/I9。
**结论**：✅ **跑通**（两分支均成立；分支 B 依赖 §7.2.1 确定性 3-way merge）。

---

### U2 并行代码审查（3 reviewer 改同一批文件）

**初始状态**：Run R2；`src/a.ts/b.ts/c.ts`；3 个 `VERIFY` 任务，committed 写集重叠。

| # | Actor | 状态前 | 动作 | 产生数据 | 状态后 |
|---|---|---|---|---|---|
| 1 | Planner | — | `plan()` | Plan v1 | — |
| 2 | Dispatcher | T1..T3 | `try_admit` ×3 | ADMITTED ×3 | — |
| 3 | R1 | RUNNING | `commit(a.ts)` | `a.ts@v1` | — |
| 4 | R2 | RUNNING | `commit(b.ts)` | `b.ts@v1` | — |
| 5 | R3 | RUNNING | `commit(a.ts, base v0)` | `CONFLICT(v1)` | — |
| 6 | R3 | REBASING | `rebase_text` | merge → `a.ts@v2` | — |
| 7 | Aggregator | — | `reduce` | 去重 findings + 矛盾检测 | — |
| 8 | VerifierRunner | — | `verify` | 覆盖度 OK | Run: DONE |

**验证的不变量**：I1/I2/I8/I9。
**结论**：✅ **跑通**（findings 聚合路径完整；写冲突走 RSC + 3-way merge）。

---

### U3 批量迁移 JS→TS（5 agent + 共享配置 + import-graph）

**初始状态**：`f1..f5`；`f1` imports `f2`；`tsconfig.json`/`package.json` 为 JSON。

| # | Actor | 状态前 | 动作 | 产生数据 | 状态后 |
|---|---|---|---|---|---|
| 1 | Planner | — | `plan()`（**在 `plan()` 内为 T1 声明 `logical_deps={TYPE_GRAPH:f2}`**） | Plan v1（含 `logical_deps`） | — |
| 2 | Dispatcher | T1..T5 | `try_admit`（`intents.compatible` 查 step 1 声明的 logical_deps） | ADMITTED | — |
| 3 | A1 | RUNNING | `commit(f1.ts)` + `commit(tsconfig.json)` | `f1.ts@v1`、`tsconfig@v1` | — |
| 4 | A2 | RUNNING | `commit(tsconfig.json, base v0)` | `CONFLICT(v1)` | — |
| 5 | A2 | REBASING | `rebase_json`（strict: keep；paths: take theirs） | `tsconfig@v2` | — |
| 6 | A3..A5 | 同 | `rebase_json`（数组并集去重） | 收敛 | — |
| 7 | A1/A2 | RUNNING | 提交期校验 step 1 声明的 `logical_deps`：`TYPE_GRAPH:f2` hash 变化 → 拦截 | `WAIT`/`REJECT_STALE_READ` | — |
| 8 | Verifier | — | `tsc --noEmit` | 不变量通过 | Run: DONE |

**验证的不变量**：I2/I5/I9。
**结论**：✅ **跑通**（JSON-aware merge 解决假冲突；`logical_deps` 拦截写偏斜）。

---

### U4 长任务 + 崩溃恢复（20 步，第 12 步崩溃，非幂等工具）

**初始状态**：1 个任务，20 步；含非幂等工具（deploy/email）。

| # | Actor | 状态前 | 动作 | 产生数据 | 状态后 |
|---|---|---|---|---|---|
| 1–11 | A | RUNNING | 每步 guard→infer→tool→commit→`checkpoint.save` | EffectJournal `CONFIRMED` | — |
| 12 | A | RUNNING | `tool_call(deploy)` 后崩溃 | effect=`UNKNOWN` | — |
| 13 | Supervisor | 心跳超时 | `revoke(leaseA)` | `leaseA=REVOKED` | — |
| 14 | CheckpointStore | — | `load(ref)` | ACB（`turn_seq=11`） | — |
| 15 | 恢复流程 | — | 扫 EffectJournal：步12 `UNKNOWN` → 反查外部系统 | 反查结果 | — |
| 16 | 恢复流程 | — | 反查=已发生→跳过；未发生→用稳定 `idem_key` 重执行 | `CONFIRMED` | — |
| 17 | A | RUNNING | 重放事件（按 `event.id` 去重）；`turn_seq` 从 11 续到 12 | — | — |
| 18 | A | RUNNING | 执行步 12..20 | 逐步 commit | DONE |

**验证的不变量**：I7/I8/I11/I13。
**结论**：✅ **跑通**（`idem_key` 跨 replan 稳定；副作用有效一次）。

---

## 第二部分：13 个对抗用例

### 跑通判定标准

本文所有「✅ 跑通」均按以下三条**同时成立**判定：

1. **到达预期终态**：用例声明的最终状态（Run `DONE`/`DONE_PARTIAL`/`CANCELLED`、资源版本号等）与实际一致。
2. **相关不变量成立**：该用例「验证的不变量编号」所列的 I1–I14 全部成立（定义见设计规范 §11）。
3. **无重复副作用**：effectful 动作（deploy/email 等）在重试、崩溃恢复、重规划后仍**恰好生效一次**（I7）。

任一不满足即记为「🟡 部分通过（有残留）」或「❌ 未通过」，并在用例中写明根因与残留项。

| # | 角度 | 攻击名 | 目标组件 | 验证的不变量编号 | 结果 |
|---|---|---|---|---|---|
| A1 | 资源竞争 | 补偿交叉 | `compensate`/`ws.prepare` | I2/I3 | ✅（v1.9 修复 `REJECT_COMPENSATE`） |
| A2 | 状态机 | 父 DONE 后子 RUNNING | `BudgetManager`/`§5.2⑦` | I5/I12 | ✅ |
| A3 | 崩溃时机 | 补偿中途崩溃 | `compensation_journal` | I7/I8/I13 | ✅ |
| A4 | 预算边界 | `remaining` 口径不一致 → 双花 | `§9.1`/I5 | I5 | ✅ |
| A5 | 嵌套派生 | 深层嵌套取消传播断裂 | `§5.2⑦` 广播 | I5/I12 | ✅ |
| A6 | 重规划风暴 | verifier 抖动 + `task_id` 变更 | I7/`§8.1` | I7/I11 | ✅ |
| A7 | 事件风暴 | cursor 落后 + 快照失效 | `§5.5`/I11 | I6/I11 | ✅ |
| A8 | 混合攻击 | HITL 挂起 + 崩溃 + 预算 + 补偿 | `§5.3`/`§5.8` | I5/I7/I13 | ✅ |
| A9 | 崩溃时机 | 租约 renew/revoke 竞态 | `§4.4`/`§5.4` | I1/I3 | ✅ |
| A10 | 资源竞争 | 逻辑不变量 write skew | `§7.3` | I2/I9（部分） | 🟡 部分通过（有残留：仍依赖事后 `tsc` 兜底） |
| A11 | 状态机 | `finalize_or_replan` 误触发 | `§5.2④` `no active` | I12 | ✅ |
| A12 | 预算边界 | 并发 `reserve` 超支 | `§5.2⑤`/§5.4 | I5 | ✅ |
| A13 | 崩溃时机 | 单 turn 击穿预算/期限 | `§5.3` per-action guard | I5/I11/I14 | ✅ |

### A1 补偿交叉（详细推演）

**场景**：A 的 `commit_batch` 在 apply 中途失败 → `compensate(A, applied=[fileX])`；补偿反写前 B 提交了 `fileX@v3`。

| 步 | Actor | 动作 | 系统状态 |
|---|---|---|---|
| 1 | A | `ws.apply(fileX)` → `fileX@v2` | `w.applied_hash = content_hash(v2)` |
| 2 | A | 同批后续写失败 → `compensate` | — |
| 3 | compensate | `release_leases(acb.leases)` | 外层租约释放 |
| 4 | compensate | `compute_reverse_patch(w)` | 基于前像 |
| 5 | B | `commit(fileX)` → `fileX@v3` | — |
| 6 | A | `commit_batch(mode=COMPENSATE)` → `prepare` | `content_hash(fileX) != w.applied_hash` |
| 7 | A | 命中 → `REJECT_COMPENSATE` → `mark_pivot(w)` | **不覆写 B 的值** |

**预期**：补偿不得覆盖 B 的新值；A 的写被拒并升级人工。
**实际**：✅ 一致（v1.9 修复）。
**能否跑通**：是。
**根因**：`mode=COMPENSATE` 反写前复验 `w.applied_hash`，命中则 `REJECT_COMPENSATE` → `mark_pivot`。
**验证的不变量**：I2/I3。

### A2 父 DONE 后子 RUNNING（详细推演）

**场景**：A 派生 B 后，A 的主任务先完成；B 仍在写资源。

| 步 | Actor | 动作 | 系统状态 |
|---|---|---|---|
| 1 | A | `spawn_child(B, footprint, budget_slice)` | B 的 `reserved=slice`；`A.reserved -= slice`（I5） |
| 2 | A | 主任务完成 → `DONE` | A 进入终态，但 B 仍 `RUNNING` |
| 3 | BudgetManager | 父 DONE → 预算账户「过户」给 Run 级节点 | B 的预算不随父终态丢失 |
| 4 | Scheduler | 检查活跃任务 | B 属 `active` → Run **不** `finalize` |
| 5 | B | `commit(...)` | 资源版本推进，B → `DONE` |
| 6 | Aggregator→Verifier | `reduce` + `verify` | Run → `DONE` |

**预期**：父 DONE 不连带取消子；预算不丢；Run 不提前 finalize。
**实际**：✅ 一致。
**能否跑通**：是。
**根因**：父完成只做预算过户、不转发取消；`finalize_or_replan` 用 `no active`（含 RUNNING 子）。
**验证的不变量**：I5/I12。

### A3 补偿中途崩溃（详细推演）

**场景**：`compensate` 已补偿 `fileY`、尚未补偿 `fileX` 时进程崩溃。

| 步 | Actor | 动作 | 系统状态 |
|---|---|---|---|
| 1 | A | `commit_batch` 先 apply `fileX`，再 apply `fileY` 失败 | `applied=[fileX]` |
| 2 | compensate | 逆序补偿 `fileY` | `compensation_journal: fileY=COMPENSATED` |
| 3 | A | 补偿 `fileX` 前崩溃 | `fileX` 仍 `COMPENSATING` |
| 4 | Supervisor | 心跳超时 → `revoke(leaseA)` | `generation+1`，旧租约失效 |
| 5 | 恢复流程 | `checkpoint.load` + 扫描 `compensation_journal` | 发现 `fileX=COMPENSATING`、`fileY=COMPENSATED` |
| 6 | 恢复流程 | 只重试 `fileX` 补偿（补偿幂等键） | `fileX=COMPENSATED` |
| 7 | 恢复流程 | 跳过 `fileY`（已完成） | 无重复补偿 |

**预期**：恢复后只补未完成的补偿，已补偿的不重复。
**实际**：✅ 一致。
**能否跑通**：是。
**根因**：`compensation_journal` 持久化 + 补偿幂等键 + 恢复对账。
**验证的不变量**：I7/I8/I13。

### A4 remaining 口径不一致 → 双花（详细推演）

**场景**：父节点 `limits=100`、`own_reserved=20`、子预留 60；并发准入按错误口径计算 `remaining`。

| 步 | Actor | 动作 | 系统状态 |
|---|---|---|---|
| 1 | BudgetManager | 初始化 | `remaining = 100 − 20 − 60 = 20`（I5b） |
| 2 | Dispatcher | 旧口径（不扣子预留）误算 | 误以为 `remaining=80` |
| 3 | Dispatcher | `reserve(50)` | 按正确口径 **Denied**（`BUDGET_EXHAUSTED`） |
| 4 | 子节点 | 结束 `release(30)` | `children.reserved=30`，`remaining=50` |
| 5 | Dispatcher | `reserve(50)` | OK，`consumed ≤ reserved ≤ limits`（I5a） |
| 6 | 父节点 | 结束 `release` | 只归还 `remaining − sum(children.reserved)` |

**预期**：任何时刻不双花；I5a/I5b 成立。
**实际**：✅ 一致（v1.5 修复 `remaining` 口径）。
**能否跑通**：是。
**根因**：`remaining` 必须扣除子预留；`release` 只归还非子占用部分。
**验证的不变量**：I5。

### A5 深层嵌套取消传播断裂（详细推演）

**场景**：A→B→C→D 四层派生；A 先进入终态，若靠父子链转发取消会断裂。

| 步 | Actor | 动作 | 系统状态 |
|---|---|---|---|
| 1 | A | `spawn_child(B)` | 活跃表 {A,B} |
| 2 | B | `spawn_child(C)` | 活跃表 {A,B,C} |
| 3 | C | `spawn_child(D)` | 活跃表 {A,B,C,D} |
| 4 | A | 进入终态（DONE） | 父链转发已不可用 |
| 5 | RunCoordinator | `cancel(run)` → 直接广播所有后代 | B/C/D 均收到取消 |
| 6 | Supervisor | 回收 B/C/D 租约 + `budget.release` | 无孤儿、无预算泄漏 |
| 7 | 状态机 | B/C/D → `CANCELLED` | Run → `CANCELLED` |

**预期**：深层后代全部收到取消，无孤儿。
**实际**：✅ 一致。
**能否跑通**：是。
**根因**：RunCoordinator 维护全局活跃表并直接广播，不依赖父 ACB 转发。
**验证的不变量**：I5/I12。

### A6 重规划风暴（详细推演）

**场景**：verifier 抖动反复判失败，Replanner 生成新 `task_id`，但逻辑子目标不变。

| 步 | Actor | 动作 | 系统状态 |
|---|---|---|---|
| 1 | A | 执行 `deploy`，`idem_key=hash(run,S1,tool,args)` | EffectJournal `CONFIRMED` |
| 2 | Verifier | 抖动 → `SEMANTIC_REJECTED` | T1 → `FAILED` → `REPLANNING` |
| 3 | Replanner | 生成 T1'（新 `task_id`，`logical_subgoal_id=S1`），Plan v2 | `propagate_failure` 仅限 Plan v1 |
| 4 | Dispatcher | T1' 准入 | 旧 Plan 失败不波及新 Plan 的 remake |
| 5 | T1' | 重试 `deploy` | `idem_key` 相同 → 命中 EffectJournal，**跳过重复副作用** |
| 6 | Replanner | 重规划次数达上限（I11） | 停止重规划，转 `DONE_PARTIAL`/`FAILED` |
| 7 | Aggregator | 汇总覆盖度 | Run 收敛 |

**预期**：不重复 `deploy`；重规划风暴有界。
**实际**：✅ 一致。
**能否跑通**：是。
**根因**：`idem_key` 不含 `task_id`（用 `logical_subgoal_id`）；失败传播限定同 Plan 版本；重规划次数上限。
**验证的不变量**：I7/I11。

### A7 事件风暴 + cursor 落后（详细推演）

**场景**：事件写入远快于消费，cursor 落到保留窗口之外，快照失效。

| 步 | Actor | 动作 | 系统状态 |
|---|---|---|---|
| 1 | ContextAssembler | 订阅，`cursor=100` | 物化到 seq 100 |
| 2 | EventLog | 事件风暴 → seq 到 5000 | 保留窗口 floor=4000 |
| 3 | ContextAssembler | 尝试增量重放 101.. | 发现 `cursor < floor`，快照失效 |
| 4 | ContextAssembler | 从 CheckpointStore 最近快照（seq 4200）重建 | 物化到 4200 |
| 5 | ContextAssembler | 重放 4201..5000 | 物化到 5000，`cursor=5000` |
| 6 | ContextAssembler | 若快照也过期 → 全量重建物化视图 + 告警 | 有界退化 |
| 7 | 监控 | 校验 `cursor ≥ floor`（I11） | 不再落后 |

**预期**：不丢事件、不无限落后。
**实际**：✅ 一致（有界：落后即快照重建）。
**能否跑通**：是。
**根因**：cursor 保留窗口校验 + checkpoint 快照回退 + 全量重建兜底。
**验证的不变量**：I6/I11。

### A8 HITL + 崩溃 + 预算 + 补偿（详细推演）

**场景**：A 在 effectful 动作上等待审批时崩溃，同时涉及预算占用与已 apply 写的补偿。

| 步 | Actor | 动作 | 系统状态 |
|---|---|---|---|
| 1 | A | `tool_call(deploy)` → `NEEDS_APPROVAL` | `WAITING_APPROVAL`，`DurableTimer(HITL_TIMEOUT)` |
| 2 | A | 崩溃 | 心跳停止 |
| 3 | Supervisor | 心跳超时 → `revoke(lease)` | `generation+1`；`WAITING_APPROVAL` 不被判 stale |
| 4 | 恢复流程 | `checkpoint.load` + 扫 EffectJournal | `effect=UNKNOWN` → 反查外部系统 |
| 5 | BudgetManager | `WAITING_APPROVAL` 属非终态 | 预算**保持占用**，不 `release` |
| 6 | DurableTimer | `HITL_TIMEOUT` 到期 | `APPROVAL_TIMEOUT` → 补偿/取消 |
| 7 | compensate | 补偿已 apply 的写 + `budget.release` | 无半回滚、无预算泄漏 |

**预期**：HITL 挂起不被心跳误杀；预算不泄漏；effect 对账。
**实际**：✅ 一致。
**能否跑通**：是。
**根因**：DurableTimer 持久化审批超时；reconcile 排除 `WAITING_APPROVAL`；非终态保持预算；EffectJournal 反查。
**验证的不变量**：I5/I7/I13。

### A9 租约 renew/revoke 竞态（详细推演）

**场景**：Supervisor 判定超时并 revoke，与此同时 A 恢复并尝试续期/提交。

| 步 | Actor | 动作 | 系统状态 |
|---|---|---|---|
| 1 | A | 持有 `lease(epoch=e)` | 租约有效 |
| 2 | Supervisor | 心跳超时 → `revoke(lease)` | `epoch=e+1`，旧租约作废 |
| 3 | A | `renew(lease, epoch=e)` | **拒绝**（revoke 后不可复活） |
| 4 | A | 用旧租约 `commit` → `ws.apply` 锁内 `is_valid=false` | `REJECT_STALE` |
| 5 | A | 重读 + 申请新租约 `epoch=e+1` | 新租约有效 |
| 6 | A | 用新租约 `commit` | 成功，版本推进一次 |
| 7 | 监控 | 校验无双重提交 | I1/I3 成立 |

**预期**：revoke 后旧租约不能续期/提交；新租约正常。
**实际**：✅ 一致。
**能否跑通**：是。
**根因**：`renew` 仅当前 epoch 生效；`apply` 锁内复验租约有效性；fencing 按租约。
**验证的不变量**：I1/I3。

### A10 write skew（🟡 部分通过，有残留）

**场景**：A 读 `CONTRACT:api-spec@v1` 写 `file1`；B 读同一契约写 `file2`。

| 步 | Actor | 动作 | 系统状态 |
|---|---|---|---|
| 1 | Planner | `plan()` 声明 `logical_deps={CONTRACT:api-spec}` | Plan v1 |
| 2 | Dispatcher | A/B `try_admit` → `intents.compatible` 检出共享契约 | 一方 `ADMITTED`，另一方 `WAIT` |
| 3 | A | 读 `api-spec@v1` 写 `file1` | `file1@v1` |
| 4 | B | rebase 后读 `api-spec@v1` 写 `file2` | `file2@v1` |
| 5 | Verifier | Run 级 `tsc --noEmit` | 若两者都改契约语义 → 可能失败 |
| 6 | Verifier | 事后发现类型不一致 → 生成修复任务 | Run 收敛 |

**残留**：并发提交顺序下，先提交方通过校验；若 `logical_deps` 声明不全（漏声明契约），write skew 仍依赖事后 `tsc` 发现（与 §7.3「不再依赖事后发现」有张力）。
**预期**：共享契约的 write skew 在准入/提交期被拦截，无需事后发现。
**实际**：🟡 部分一致——声明完整时拦截；漏声明时仍靠事后 `tsc`。
**能否跑通**：部分通过（有残留）。
**根因**：`logical_deps` 是 Planner 静态声明 + 软准入，非 SSI 动态危险结构检测；无法覆盖未声明的逻辑依赖。
**验证的不变量**：I2/I9（部分）。
**结论**：🟡 **部分通过（有残留）**——终态一致，但需明确 `logical_deps` 是「硬依赖边」还是「软准入检查」。

### A11 finalize_or_replan 误触发（详细推演）

**场景**：T1 DONE，T2 等待审批、T3 退避重试；若按「no running」判据会误判 Run 可结束。

| 步 | Actor | 动作 | 系统状态 |
|---|---|---|---|
| 1 | 状态机 | T1 → `DONE` | T2 `WAITING_APPROVAL`、T3 `RETRYING` |
| 2 | Scheduler | 旧判据「no running」 | T2/T3 非 RUNNING → 误判可 finalize |
| 3 | Scheduler | 新判据「no active」 | T2/T3 属 active → **不** finalize |
| 4 | HITL | T2 审批通过 | T2 → `RUNNING` |
| 5 | T3 | 退避结束 | T3 → `RUNNING` |
| 6 | T2/T3 | 执行完成 | 均 → `DONE` |
| 7 | Scheduler | `no active` 且 `no ready` → `finalize_or_replan` | Run → `DONE` |

**预期**：存在任何非终态任务时不 finalize。
**实际**：✅ 一致（v1.5 修复 `no active`）。
**能否跑通**：是。
**根因**：`active` = 所有非终态（含 `WAITING_APPROVAL/RETRYING/REBASING/REPLANNING/QUARANTINED/INADMISSIBLE`）。
**验证的不变量**：I12。

### A12 并发 reserve 超支（详细推演）

**场景**：两个 agent 同时向同一父节点 `reserve(60)`，父 `remaining=100`。

| 步 | Actor | 动作 | 系统状态 |
|---|---|---|---|
| 1 | BudgetManager | 父 `remaining=100` | — |
| 2 | A/B | 非原子 `check+deduct` 并发读到 100 | 都通过 → 预留 120（超支） |
| 3 | BudgetManager | 改为单写者串行（或 CAS） | A `reserve(60)` → `remaining=40` |
| 4 | BudgetManager | B `reserve(60)` | **Denied**（`BUDGET_EXHAUSTED`）或部分预留 |
| 5 | 监控 | 校验 `consumed ≤ reserved ≤ limits` | I5a 成立 |
| 6 | 子节点 | 结束 `release` | `remaining` 恢复 |
| 7 | 监控 | 校验 `sum(children.reserved) ≤ parent.remaining` | I5b 成立，无超支 |

**预期**：并发预留不超支。
**实际**：✅ 一致。
**能否跑通**：是。
**根因**：`reserve` 为单写者/CAS 原子，消除 `check+deduct` 竞态。
**验证的不变量**：I5。

### A13 单 turn 击穿预算/期限（详细推演）

**场景**：一个 turn 的 decision 含 10 个 action，执行中途预算/期限逼近上限。

| 步 | Actor | 动作 | 系统状态 |
|---|---|---|---|
| 1 | A | `turn_seq += 1`，decision.actions=[a1..a10] | per-turn guard 通过 |
| 2 | A | 执行 a1..a4 | token/时间消耗上升 |
| 3 | A | 执行 a5 后触发 per-action guard（`GUARD_EVERY`） | 预算/期限接近上限 |
| 4 | A | guard 判定超限 → `FAILED(GUARD_VIOLATION)` | 停止后续 action |
| 5 | A | 已执行 action 的 effect 记入 EffectJournal；未提交 staged 写丢弃 | 无半提交 |
| 6 | CheckpointStore | `save(acb)`（含 `turn_seq`） | 恢复基准一致 |
| 7 | 状态机 | Run 收敛 | `DONE_PARTIAL`/`FAILED` |

**预期**：单 turn 内不会因 per-turn guard 间隔而击穿预算/期限。
**实际**：✅ 一致（v1.5 per-action guard）。
**能否跑通**：是。
**根因**：每 `GUARD_EVERY` 个 action 执行一次 guard，限制单 turn 超支。
**验证的不变量**：I5/I11/I14。

---

## 第三部分：跑通汇总

| 类别 | 通过 | 总数 | 说明 |
|---|---|---|---|
| 真实用例 | 4 | 4 | ✅ 全部通过 |
| 对抗用例（完全通过） | 12 | 13 | ✅ 按「跑通判定标准」三条同时成立 |
| 对抗用例（部分通过） | 1（A10） | 13 | 🟡 有残留：漏声明 `logical_deps` 时仍靠事后 `tsc` 兜底 |

> **结论**：真实 4/4 通过；对抗 **12/13 完全通过**，A10 为「🟡 部分通过（有残留）」。

> **可审计性**：本文档即为审计依据。任何对「跑通」的质疑，应指向本文件的具体步骤与「跑通判定标准」。
