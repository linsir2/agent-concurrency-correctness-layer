# S6：runtime 表驱动 + guard/续租 + snapshot + E2E 收口

| 项目 | 内容 |
|---|---|
| 版本 | **v1.2（实现完成 + 实现后评审修订）** |
| 日期 | 2026-10-27 |
| 上游 | [第一阶段方案 v1.1](phase1-implementation-plan.md) §4.10/§5.5/§7、[S5 方案 v1.2](s5-recovery-and-journals.md) §12 |
| 覆盖测试 | T19 + U1×2/U2/U3 走真实路径 + 不变量 E2E |
| 交付判据 | 全部绿 + 连跑 10 轮稳定 |
| 交付判据 | **123 passed**（连跑 10 轮稳定）；`-m ms4`→16、`-m e2e`→10 |
| 修订 | v1.1 → v1.2：实现后评审修 1 高 + 4 中 + 6 低（§12） |

---

## 0. 调研结论（12 条差距）

| # | 差距 | 严重级 |
|---|---|---|
| 1 | **U3 完全不调用 `run_task`** —— merge 与依赖拦截全被绕过 | P0 |
| 2 | **U1-B/U2 手工陈旧 base + 手工 `rebase_json`** —— 生产 rebase 回路零覆盖 | P0 |
| 3 | U1-A 未建依赖边，串行靠手工顺序 | P1 |
| 4 | U4 手工 `effects.confirm` 短路 probe | P1 |
| 5 | `_do_action` 是 if/elif，非注册表 | P2 |
| 6 | `guard` 五查硬编码；`renew_for` 返回值被丢弃 | P1 |
| 7 | `checkpoints.event_log` 未注入 → `event_cursor` 语义错误 | P1 |
| 8 | snapshot 缺 `pre_image`/`last_committed_seq` 等 | P1 |
| 9 | 4 处终态路径不 save | P2 |
| 10 | 死字段 `slots_active`/`budget_subtree` | P3 |
| 11 | `applied_event_ids` 无界 | P3 |
| 12 | `_drain_scheduler` 空壳 | P1 |

---

## 1. 目标

| # | 目标 | 判据 |
|---|---|---|
| G1 | `_do_action` 注册表；`guard` 谓词列表 + 续租消费 | 单测 |
| G2 | `event_cursor` 真水位（per-ACB，非全局 tail） | 新增用例 |
| G3 | snapshot 补字段 + 4 处补 save | 新增用例 |
| G4 | **U1×2/U2/U4 去假绿**（U3 拆正/负对照） | U 用例重写 |
| G5 | **新增 `edges` 准入闸门 + 环检测**（否则 I9/U1-A 不可达） | 新增用例 |
| G6 | 不变量 E2E（**排除 I9/I13 中无实现支撑的部分**） | 新增套件 |
| G7 | `_drain_scheduler` 真消费（**防递归**） | 新增用例 |

---

## 2. 设计（v1.1：按真实接口重写）

### 2.1 动作注册表（修 P1-1a/1b/1c/1d）

```python
_HANDLERS: dict[str, Callable] = {}

def register_action(kind: str):
    def deco(fn):
        if kind in _HANDLERS:
            raise ValueError(f"duplicate action handler: {kind}")
        _HANDLERS[kind] = fn
        return fn
    return deco

class TaskRuntime:
    def _do_action(self, acb, action, i) -> Result:
        fn = _HANDLERS.get(action.kind)
        if fn is None:
            return Result(Status.INVALID_INPUT, {"reason": "unknown_action"})
        try:
            out = fn(self, acb, action, i)
        except Exception as e:                       # ← 修 1d：异常映射到 Result
            return Result(Status.UPSTREAM_FAILED, {"reason": type(e).__name__})
        return out if isinstance(out, Result) else Result(out)   # ← 修 1a：归一化
```

**契约**：handler **必须返回 `Result`**；`_do_action` 做归一化兜底；异常不穿透。
**不引入优先级**（精确 key 命中）；重复注册抛错。

### 2.2 guard 谓词列表（修 P0-2a/2b/2c + P1-2d/2e）

```python
def _guard_checks(self, acb) -> list[Callable[[], str | None]]:
    return [
        lambda: "CANCELLED" if self.coordinator.is_cancelled(acb.task_id) else None,
        lambda: self._renew_or_fail(acb),
        lambda: "BUDGET_EXHAUSTED" if not self.budget.check_reservations(acb.reservation_ids, 1) else None,   # ← 修 2c：传 reservation_ids
        lambda: "DEADLINE_EXCEEDED" if acb.step >= acb.deadline else None,
        lambda: "MAX_STEPS" if acb.step > self.max_steps else None,
        lambda: "NO_PROGRESS" if acb.no_progress_steps > self.aging_max else None,
    ]

def guard(self, acb) -> tuple[bool, str | None]:
    for chk in self._guard_checks(acb):
        reason = chk()
        if reason:
            if reason != "CANCELLED":
                acb.failure = reason          # ← 修 2e：reason 落 failure（不再恒 GUARD_VIOLATION）
            return False, reason
    return True, None
```

**`_renew_or_fail` 三态**（修 P0-2b：`renew_for` 无法区分「无租约」与「失效」）：

```python
def _renew_or_fail(self, acb) -> str | None:
    live = [ls for ls in self.intents.active_intents(acb.id).values() if ls.valid]
    if not live:
        return None                            # 无有效租约（如已提交 revoke）→ 不判失败
    return None if self.intents.renew_for(acb) else "LEASE_EXPIRED"
```

> `commit_batch` 的 `finally` 会 revoke → 提交后 `active_intents` 为空 → **不误判**。

### 2.3 event_cursor：per-ACB 水位（修 P1-3a/3b/3c）

```python
# checkpoint.save
"event_cursor": self.event_log.tail(),          # 全局水位（重放起点）
"materialized_cursor": acb.cursor,              # per-ACB 物化游标
```

**保存不变量**：save 前 `acb.cursor = self.event_log.tail()`，保证两字段**不分叉**（修 3b）。
**恢复兼容**（修 3c）：`acb.cursor = data.get("materialized_cursor", data.get("cursor", 0))`。

### 2.4 snapshot 字段（修 P1-4b/4c）

**只加「有消费点」的字段**：

| 字段 | 消费点 |
|---|---|
| `pre_image` | `_restore_acb` 恢复（补偿 PIVOT 判定依赖） |
| `last_committed_seq` | 新增 `ACB.last_committed_seq`；recover 后用于去重水位校验 |

**不加** `parent_id`/`children_ids`（无消费点 → 不做新死字段）。
**死字段处理**：`slots_active` 在 recover 消费（断言恢复后槽位一致）；`budget_subtree` 在 recover 做交叉校验；`reservation_ids` 标 `legacy`。

**save 时机**：`_finalize` 末尾、`escalate_quarantined`、`requeue_readmit`、`supervisor_note_crash`。
**注意**：save 放在 `_commit_staged` 层，**不放进 `commit_batch`**（避免 REBASING 瞬态快照 + 租约未 revoke 的瞬时态）。

---

## 3. U 用例去假绿（v1.1：修正目标）

| 用例 | v1.0 的错误目标 | v1.1 正确目标 |
|---|---|---|
| **U3** | 「提交期 `logical_deps` 拦截 → `WAIT`/`REJECT_STALE_READ`」**不可达** | **拆两用例**：①正例 = 两 task 同 `dep_id` → 第二个 `INADMISSIBLE` + `failure="CONTRACT_CONFLICT"` + **零残留**；②负对照 = `dep_id` 不同 → **两者都 `ADMITTED`**。原「5 agent 全 OK + merge」保留为**独立用例**（不经准入） |
| **U1-B** | 「第二个提交必然 CONFLICT」（Barrier 依赖，flaky） | **stage-before-commit 串行构造**：B 先 `stage`（base=v0）→ A 经 `run_task` 提交（v1）→ B 提交 → **必然 CONFLICT**。断言 `rebase_attempts == 1` + 合并内容含两侧改动 |
| **U2** | 同上 | 同上；补 `REJECT_STALE_READ`：stage 后**他人推进 read_set 资源** → 读依赖陈旧 |
| **U1-A** | 「串行准入」**无机制支撑** | **先补 `edges` 准入闸门**（§2.5），再断言：T1 未完成时 T2 `INADMISSIBLE`；T1 `DONE` 后 T2 `ADMITTED`。**负对照**：删边 → T2 立即可 admit |
| **U4** | 只断言 `CONFIRMED` 可达 | **probe 三态矩阵** + `EFFECT_PROBED` 事件断言（`seq` < `EFFECT_CONFIRMED`）+ 注入计数器断言 `calls == [key12]` |

### 2.5 `edges` 准入闸门（新增，G5）

```python
# orchestration.py: Dispatcher.try_admit 前置检查
def _predecessors_done(self, plan, task_id) -> bool:
    for src, dst in plan["edges"]:
        if dst == task_id:
            src_state = plan["tasks"][src]["state"]
            if src_state not in ("DONE", "CANCELLED"):
                return False
    return True
# try_admit 里：未完成 → _deny(acb, "PREDECESSOR_NOT_DONE") → INADMISSIBLE
```

**环检测**（新增，G5）：`add_logical_edge` 增加 DFS 环检测，含环 → 抛 `ValueError`（**准入前拒绝**）。

### 2.6 `_drain_scheduler` 真消费（修 P0-6a）

```python
def _drain_scheduler(self):
    while True:
        task_id = self.scheduler.pop()
        if task_id is None:
            break
        acb = next((a for a in self.acbs.values() if a.task_id == task_id), None)
        if acb is None or acb.state != TaskState.ADMITTED:   # ← 防重入：只启动 ADMITTED
            continue
        self._run_one(acb)          # 抽出 run_task 的执行体，不 push、不递归 drain
```

**关键**：`_run_one` **不 push、不调用 `_drain_scheduler`** → 无递归。`run_task` 保留 push（供外部直接调用），但 `_drain_scheduler` 走 `_run_one`。

---

## 4. 不变量 E2E（v1.1：只测有实现支撑的）

| 不变量 | 构造 | 确定性 |
|---|---|---|
| I1 单写者 | stage-before-commit（串行）→ 恰好一个 `OK` | ✅ 无 Barrier |
| I2 无丢失更新 | 同上 + 经 `run_task` rebase → 合并值含两侧 | ✅ |
| I3 fencing | `reserve` → `supervisor_note_crash` → 提交 `REJECT_STALE` | ✅ |
| I5 预算守恒 | 串行 admit T1/T2 → T2 `INADMISSIBLE` + `check_invariants` | ✅ |
| I6 上下文因果 | 多次 commit + `context_view` → seq 有序 | ✅ |
| I8 事件原子 | 陈旧 base 的失败 commit → `has()` 全假 | ✅ |
| I10 隔离 | 一 agent 失败不影响另一 ACB | ✅ |
| **I9 计划有效** | **新增环检测后**：含环 → `add_logical_edge` 抛错 | ✅（依赖 §2.5） |
| I12 非终态不 finalize | 注入不结束的任务 → `Run` 未被 finalize | ✅ |
| I4/I7/I11 | 补强（`required_tools` 全链路 / probe 三态 / retention 窗口） | ✅ |

**I13 收据链**：`run_task` 路径**不产生** `Receipt`（生产代码无 `build_receipt` 调用）→ **本阶段不纳入 E2E**，记为 K-24（需先补收据生成）。

**I11 真并发**：仅 A12 保留 Barrier 真并发（已有），E2E 用串行构造。

---

## 5. 迁移清单

| 文件 | 变更 |
|---|---|
| `harness/runtime.py` | `_do_action` 注册表 + 归一化 + 异常映射；`guard` 谓词列表 + `_renew_or_fail`；每 turn 推进 `acb.cursor` |
| `harness/orchestration.py` | `add_logical_edge` 环检测；`try_admit` 前驱闸门；`check_reservations` 签名对齐 |
| `harness/harness.py` | 注入 `checkpoints.event_log`；`_run_one` 抽取；`_drain_scheduler` 真消费；4 处补 save |
| `harness/checkpoint.py` | save 增 `pre_image`/`last_committed_seq`/`materialized_cursor`；死字段消费 |
| `harness/types.py` | `ACB.last_committed_seq` |
| `tests/test_real_cases.py` | U1×2/U2/U4 重写；U3 拆正/负对照 |
| `tests/test_e2e_invariants.py`（新） | 10 条 E2E |
| `tests/test_ms4_runtime.py`（新） | T19 + guard/续租/游标/调度/环检测 |

---

## 6. 风险与对策

| 风险 | 对策 |
|---|---|
| U 重写暴露真实缺陷 | 逐条修实现，不放宽断言 |
| 注册表破坏语义 | handler 逐条搬迁 + 行为对照测试 |
| `_drain_scheduler` 递归 | `_run_one` 不 push、不 drain；`ADMITTED` 守卫 |
| `edges` 闸门破坏现有 U1-A | 负对照（删边即可 admit） |
| 旧 checkpoint 不兼容 | `data.get(...)` 双回退 |

---

## 7. 决策（v1.1 定案）

| # | 问题 | 结论 |
|---|---|---|
| Q1 | `LEASE_EXPIRED` 新增 `Status`？ | **不新增** → 复用 `GUARD_VIOLATION` + `failure="LEASE_EXPIRED"` |
| Q2 | `_drain_scheduler` 真消费冲突？ | **撤回 v1.0 结论**：会双重 run + 无限递归 → 用 `_run_one` + `ADMITTED` 守卫 |
| Q3 | I9/I13 是否 E2E？ | I9 **补环检测后**纳入；I13 **暂不纳入**（记 K-24） |
| Q4 | U1-B 真并发还是串行？ | **串行 stage-before-commit**（确定性，无 flaky） |

---

## 11. 评审修订记录（v1.0 → v1.1）

### 11.1 P0（6 项）

| # | v1.0 问题 | 修法 |
|---|---|---|
| **P0-1** | `_renew_or_fail` 调 `intents.leases()` —— **该方法不存在** → 每 turn `AttributeError` | 改用 `active_intents(acb.id)` + `ls.valid` 过滤 |
| **P0-2** | `check_reservations(acb.budget_node, 1)` —— 首参应为 **reservation_ids 列表** → `TypeError` | 改 `check_reservations(acb.reservation_ids, 1)` |
| **P0-3** | `_drain_scheduler` 真消费 → **无限递归**（`run_task` push + finally drain）+ 双重 run | 抽 `_run_one`（不 push、不 drain）+ `ADMITTED` 守卫 |
| **P0-4** | **I9 环检测不存在** → 「含环准入拒绝」E2E 必失败 | `add_logical_edge` 加 DFS 环检测 |
| **P0-5** | **I13 收据不在 `run_task` 路径产生** → E2E 无从断言 | 移出 E2E，记 **K-24** |
| **P0-6** | **U3 目标不可达**：`logical_deps` 只在**准入期**生效（非提交期）；且 5 个同 `dep_id` 会全被拒 | 拆正例（`INADMISSIBLE`/`CONTRACT_CONFLICT` + 零残留）/负对照（不同 dep → 都 ADMITTED）；原 merge 用例独立 |

### 11.2 P1（12 项）

| # | 问题 | 修法 |
|---|---|---|
| 1 | handler 返回裸 `Status` 时 `isinstance` 判定失败 → **rebase 回路被静默绕过** | `_do_action` 归一化 `Result` |
| 2 | handler 异常穿透 `run` | `try/except → UPSTREAM_FAILED` |
| 3 | `renew_for` 无法区分「无租约」与「失效」→ **提交后 guard 恒假失败** | 先查 `active_intents`，无有效租约则不判失败 |
| 4 | 谓词顺序改变 `failure` 字段 | reason 统一落 `acb.failure`；补 `MAX_STEPS` |
| 5 | `run()` 硬编码 `GUARD_VIOLATION` 丢弃 reason | `guard` 返回 `(bool, reason)` |
| 6 | `event_log.tail()` 是**全局**水位，多 run 互相耦合 | 明确为全局重放起点；`materialized_cursor` 为 per-ACB |
| 7 | save 在两字段间分叉 | save 前对齐 `acb.cursor = tail()` |
| 8 | 旧 checkpoint 无 `materialized_cursor` → 静默降级 | `data.get("materialized_cursor", data.get("cursor", 0))` |
| 9 | `parent_id`/`children_ids` 会成为**新死字段** | 不加（无消费点） |
| 10 | U1-A「串行准入」**无机制** | 新增前驱闸门（§2.5）+ 负对照 |
| 11 | U1-B Barrier 依赖 → flaky | stage-before-commit 串行构造 |
| 12 | U4 只断言 `CONFIRMED` 可达 | probe 三态矩阵 + `EFFECT_PROBED` 事件 + 计数器 |

### 11.3 P2/P3

| 问题 | 修法 |
|---|---|
| 未知 kind 兜底返回层级不一致 | 统一 `Result` |
| 模块级全局注册表无 teardown | 明确「无优先级、精确命中」；测试不替换 handler |
| `pre_image` 被误判为「无界快照」 | 实为 hash（按资源去重）；无界风险归 `applied_event_ids`/`staged` |
| `last_committed_seq` 与 `event_cursor` 冗余 | 明确分工：per-ACB 已提交 vs 全局重放水位 |
| save 放进 `commit_batch` 会拍到 REBASING 瞬态 | 放在 `_commit_staged` 层 |
| `pop()` 返回 `None` 未判空 | `if task_id is None: break` |
| I3 需手工持有旧 lease | 注入 `ttl_ms=0` + `ManualClock` |
| **A11 是同义反复**（自测谓词） | 改为断言 `Run` 未被 finalize |

---

## 12. 实现后对抗评审（v1.1 → v1.2）

实现分三波（runtime 表驱动 → orchestration/harness → 测试重写 + E2E），随后对抗评审。**1 高 + 4 中 + 6 低已修**，并补注册了 `ms4`/`e2e` marker。

### 12.1 高（已修）

| # | 问题（反例） | 修法 |
|---|---|---|
| **D6** | **`patch` 后边端点陈旧**：task_id 改名 `T1→T1'`，但 edges 仍写 `T1` → `_predecessors_done` 用 `tasks.get("T1")` 得 `None` → 后继**永久** `PREDECESSOR_NOT_DONE`（实测） | `patch` 记 `_id_aliases`；`_predecessors_done` 按 `logical_subgoal_id` 兜底匹配 remake 任务 |

### 12.2 中（已修）

| # | 问题 | 修法 |
|---|---|---|
| **D4** | `_retry_with_fresh_lease` 直接 `reserve` 的租约**不在 `commit_batch` 的 finally 撤销集合**里 → 遗留 1 条 valid 租约（实测） | `try/finally` 显式 `revoke` |
| **D9** | `_finalize` 调 `rebuild_ready_set` 但**丢弃返回值** → T2 永不调度 | 消费返回值；**只调度因本任务完成而新就绪的后继**（level-triggered，避免误拉无前驱任务破坏 A2/A11/I12） |
| **D10** | `_drain_scheduler` 按 `task_id` 匹配 → 同 task_id 多 ACB 时命中旧的（实测新 ACB 永不运行） | push/match 改用 `acb.id`，回退 `task_id` |
| **D14** | `_finalize` 直接 `plan["tasks"][acb.task_id]` → patch 后旧 ACB 失败时 `KeyError` | 改 `.get()` + None 安全处理 |

### 12.3 低（已修）

| # | 问题 | 修法 |
|---|---|---|
| **D1** | handler 返回非 `Status`/`Result`（如 `None`）→ `Result(None)` → `run()` 里 `st.value` 崩（实测） | 三分支归一化（其他 → `UPSTREAM_FAILED(bad_return)`） |
| **D11** | PLANNED 任务被静默 `continue` | 记录到 `_drain_skipped`（原因可见） |
| **D12** | `run_task` 抛 `CrashInjected` 时**跳过** `_drain_scheduler` → 队列不消费 | 放进 `finally` |
| **D13** | `last_committed_seq` 是死字段（从不写入） | 两条提交路径成功后真实写入 `= event_log.tail()` |
| **D13b** | `no_progress_steps`/`aging_deadline`/`heartbeat_at` 只恢复从不推进 → `NO_PROGRESS` 谓词**不可达** | 每 turn 推进 `heartbeat_at`；无进展时 `no_progress_steps += 1`；达 `aging_max` 设 `aging_deadline` |
| **marker** | `-m ms4`/`-m e2e` 全部 deselected | 注册 markers |

### 12.4 未发现缺陷的维度（已实测）

- **环检测**：自环/重边/无环/菱形/元组边均正确；失败时 `edges.pop()` 只移除刚落地的边，不破坏已有边
- **`CrashInjected` 捕获**：当前安全（抛在 `_do_action` 的 `try` 之外）；但**潜在隐患**：若将来 handler 内部抛 `CrashInjected` 会被误映射成 `UPSTREAM_FAILED` → 建议让 `CrashInjected(BaseException)`（记 **K-25**）
- **`_renew_or_fail`**：结构与 `reserve`/`revoke` 一致；`bump_epoch` 使 lease 全 invalid → 不会误判 `LEASE_EXPIRED`
- **游标对齐不变量**：与「save 前对齐」一致；全局 `tail()` 的耦合是文档明确接受的设计

### 12.5 测试证伪性复核

| 用例 | 结论 |
|---|---|
| 重复注册抛错 | **可证伪**（模拟「无重复检查」→ 不抛 → 失败） |
| 环检测 | 真构造 `T1→T2→T3→T1` + 断言 `len(edges)==2` |
| `_drain_scheduler` | `ran == ["T1","T2"]` 是真断言 |
| I1/I2 rebase | 已插桩验证 I2 确走 `handle_commit_result`（`calls=[CONFLICT]`、`rebase_attempts==1`） |
| U4 probe 注入 | `calls == [key12]` 证明真注入（默认 `None` 时 `calls` 为空 → 失败） |

### 12.6 新增已知边界（v1.2）

| # | 边界 |
|---|---|
| **K-24** | **I13 收据链未纳入 E2E** —— `run_task` 路径不产生 `Receipt`（生产代码无 `build_receipt` 调用），需先补收据生成 |
| **K-25** | `CrashInjected` 若改为在 handler 内部抛出，会被 `_do_action` 的 `except Exception` 误映射为 `UPSTREAM_FAILED` → 建议改继承 `BaseException` |
| **K-26** | I11 retention 窗口：`context.retention` 只赋值不读取，无「越过窗口强制从快照重建」实现 → E2E 用「有界重放 + seq 有序」替代 |
| **K-27** | `materialized_cursor` 与 `event_cursor` 在「save 前对齐」下恒等 —— 全局 `tail()` 是多 run 耦合的设计选择 |
