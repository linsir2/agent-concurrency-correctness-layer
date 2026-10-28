# S14：边界缺口收口 + MS8 conformance 封顶

| 项目 | 内容 |
|---|---|
| 版本 | **v1.3（实现完成 + 自评审）** |
| 日期 | 2026-10-27 |
| 上游 | [S13 v1.2](s13-orchestration.md) §8.5（K-67..K-71）、[milestones.md](milestones.md) MS8/MS9/MS10 |
| 覆盖 | **K-67..K-71 收口** + **MS8 补强（I12 / `required_coverage` 语义预留）** |
| 不覆盖 | MS9 真 Raft（推迟 S16）、MS10 全套（拆 S15） |
| 硬约束 | 475 passed + 4 xfailed 全绿；不引后台线程；零第三方运行时依赖 |

---

## 0. 项目当前位置（核对 milestones.md）

| MS | 状态 | 证据 |
|---|---|---|
| MS0–MS7 | ✅ 完成 | 28 个测试文件 + 475 passed |
| **MS8** | **≈90%（文档自评已过期）** | §0.1 |
| MS9 | ❌ 未开工（但门槛比文档说的窄，见 §0.2） | — |
| MS10 | ❌ 未开工（模块不存在，但 `DEAD_LETTER` 已有 4 条产生路径） | §0.3 |

### 0.1 MS8 自评的 4 个弱项 —— **3 个已被 S9–S13 修掉**

| I | MS8 文档自评 | 代码实况 | 结论 |
|---|---|---|---|
| **I11** | 「`retention` 未读取、无 cursor floor」 | `retention` 在 [context.py:17-47](harness/context.py) 真读取；`floor_for` 在 [harness.py:639-649](harness/harness.py)（S12 K-33 per-run） | **已修**，文档过期 |
| **I13** | 「运行时**从不产生收据**」 | `RECEIPT_ISSUED` 在 [harness.py:1126](harness/harness.py) 由 `commit_batch` 真产生 | **已修**，文档过期 |
| **I14** | 「`no_progress_steps` 从无写入」 | 写入在 [runtime.py:166-169](harness/runtime.py)；谓词在 [runtime.py:397](harness/runtime.py) | **已修**，文档过期 |
| **I12** | 「只断言存在非终态，未验证不饿死」 | 只有 `test_i12_nonterminal_task_not_finalized`（[test_e2e_invariants.py:187](tests/test_e2e_invariants.py)） | ⚠ **仍弱**，本波补 |

### 0.2 MS9 门槛比文档说的**窄得多**

文档（milestones.md:238）：「组件均为具体类直接实例化，**无构造注入**，MS9 不得开工」。

**实测：注入已存在。** [`Harness.__init__`](harness/harness.py#L146-L176) 接受 `event_log`/`intents`/`workspace`/`budget`/`checkpoints`/`effects`/`policy`/`context`/`subscriptions`/`clock`/`blobs` 全部 keyword 注入。

**真正缺的只有两件**：① 无 `Protocol`/`ABC` 契约（`grep` **零命中**）；② 无后端一致性测试套件。
→ 准备工作纯 Python、可独立交付。Raft 本身仍是工程（`pysyncobj` 0.3.14 / 2025-02 / MIT 可用）。

### 0.3 MS10 实况

| 项 | 实况 |
|---|---|
| `DEAD_LETTER` 产生路径 | ✅ **已有 4 条**（aging [2515](harness/harness.py)、escalate [3161](harness/harness.py)、replan 耗尽 [3164](harness/harness.py)、[3264](harness/harness.py)）→ 文档「无产生路径」**已过期** |
| `test_dlq` / `test_admin_pause_resume` / `test_ingress_idempotent` | ❌ 均不存在 |
| `observability`/`admin`/`dlq`/`Ingress`/`Renderer` | ❌ 均不存在；`RunCoordinator.submit_command` 空壳 |

---

## 1. 大类划分

| 大类 | 内容 | 结论 |
|---|---|---|
| **A. 边界缺口收口** | K-67..K-71 | ✅ **S14 做** |
| **B. MS8 conformance 封顶** | I12 补强 + `required_coverage` 语义预留 + 文档对齐 | ✅ **S14 做** |
| **C. 可插拔契约** | `Protocol` 声明 + 后端一致性套件（MS9 前置） | ⏭ S15 |
| **D. 生产化最小集** | Ingress 幂等 + DLQ redrive + Admin pause/resume + 可观测 | ⏭ S15 |
| **E. MS9 真 Raft** | `pysyncobj` 后端 | ⏸ 推迟（单写者已够） |

---

## 2. S14A：五个边界缺口（按优先级，非按编号）

### 2.1 K-69（**P1**）：`tick` 只看内存 → 重启后等待态永不结算

**真 bug，不是使用姿势**（[harness.py:2476](harness/harness.py)）：
```python
for acb in list(self.acbs.values()):      # 只看内存
```
`recover()` 一次只恢复**一个** ACB（[3296](harness/harness.py) 调 `tick`）。若 supervisor 只恢复它关心的 ACB，**其它等待态永远不结算** —— 反复调 `tick` 也没用。

**修法（三件套，不新建索引、不引后台线程）**

S13c 已有 `refs_all`（内存版 [checkpoint.py:231](harness/checkpoint.py)、文件版 [1114](harness/checkpoint.py)）→ 直接枚举即可。

```python
def tick(self, now_ms=None) -> None:
    now = self.clock.now_ms() if now_ms is None else int(now_ms)
    seen = set()
    for acb in list(self.acbs.values()):                  # 内存（热路径）
        seen.add(acb.id)
        self._tick_one(acb, now)
    for acb_id, ref in self.checkpoints.refs_all():       # ← 持久真相
        if acb_id in seen:
            continue
        try:
            payload = self.checkpoints.load_strict(ref)   # 先读完，**释放 store 锁**
        except Exception:
            self._tick_skipped.append((acb_id, "load_failed"))   # 不静默
            continue
        self._tick_one(self._hydrate(payload), now)       # 再结算（见 §2.2 锁序）

def next_deadline_ms(self) -> int | None:
    """min(WAITING_APPROVAL.deadline, ADMITTED.aging_deadline)；无则 None"""

def pump(self, timeout_ms: int | None = None) -> int:
    """tick() + 返回仍等待项数（0 = 可退出）"""
```
- **edge 唤醒**：`HITL.set_result` 成功 / `recover` 结束 / `_finalize` 推送后置 `threading.Event`
- **内存版 `refs_all` 返回空** → 自然退化为原行为，**零破坏**
- `tick` 内部仍调 `reconcile.scan_deadlines`（aging），新增的只是 HITL 侧的持久枚举

### 2.2 **锁序红线（v1.1 新增，实现必须遵守）**

`tick` 触发 `_finalize` → 后者取 `budget`（order=1）+ `store`（order=5）。
若 `tick` **持 store 锁**去调 `_finalize` → `budget(1)` 在 `store(5)` 内 = **反向获取 → `LockOrderViolation`**（[storage/_lock.py](harness/storage/_lock.py) 的 `LOCK_ORDER` 会直接抛）。

**硬规则**：`load_strict` / `refs_all` **读完即释放锁**，`_tick_one` 在**无 store 锁**时执行。等价于「先水合、后结算」，与 `commit_batch` 的既有做法一致。

### 2.3 K-68（**P2**）：`update_children` 改**条件写**，不硬接 claim

**原理修正**：`update_children` 只拿 `parent_id`、**无执行上下文** → **原理上无法做身份 fencing**（无 epoch 可比）。把 `claim_epoch` 塞进去是**语义错位**（调用者是 replan/requeue，非 claim 持有者）→ 造假拒绝。

```python
def update_children(self, acb_id, mutate, *, expect_ref=None, retries=3):
    for _ in range(retries):
        with self._flock():
            cur = self.latest_ref_for_acb(acb_id)     # flock 内重读
            if cur is None:
                return None
            if expect_ref is not None and cur != expect_ref:
                continue                               # 有人先写 → 重读重试
            return self._save_payload_locked(mutate(self._load_payload(cur)))
```
**技术坑**：`_save_payload` 会再取 flock → 若 `_FileLock` 不可重入，须拆 `_save_payload_locked`。

### 2.4 K-71(a)（**P2**）：深度超限 → 显式失败并传播

现状（[1624-1626](harness/harness.py)）：`return Result(Status.INVALID_INPUT, ...)` → 调用方当**可重试**，实际是终局。

**修法**：`FAILED` + `failure="INLINE_DEPTH_EXCEEDED"` + `propagate_failure`。
**已核实**：`grep inline_depth_exceeded tests/` **零命中** → 改状态不破坏现有断言。

### 2.5 K-71(b)：推迟 S15

依据本仓对 HITL 的既有裁决（[3101-3104](harness/harness.py)「等待审批**不是**占槽位的理由」），把同一规则推广到 spawn 是正确方向，但默认 `spawn_depth=1` → 内联链最多 1 层 → **收益为 0**。

### 2.6 K-67（**P3**）：先**物化 edges**，再剪枝

**两处修正**：
1. 真实代价不是内存，而是 `patch()` 每次 `copy.deepcopy(plan)`（[orchestration.py:258](harness/orchestration.py)）连别名表深拷贝 → 单 plan n 次 replan = **O(n²)**
2. **文档旧判据有漏洞**：只说「是否仍出现在 `edges.from`」，但别名还被**活跃 ACB 的 `task_id`** 引用（[3391](harness/harness.py)、[3417](harness/harness.py)）→ 剪掉在途映射 → `replan_no_task`

- **B（结构性）**：`patch()` 末尾把 `edges` 端点重写为 canonical id（Camunda「迁移时物化」同构）
- **A（配套 10 行）**：在 `Harness.replan()`（**不是** `patch()`，保纯函数）剪枝：
```python
keep = {e["from"] for e in plan["edges"]} | {e["to"] for e in plan["edges"]} \
     | set(plan["tasks"]) \
     | {a.task_id for a in self.acbs.values() if a.run_id == run_id} \
     | {a.logical_subgoal_id for a in self.acbs.values() if a.run_id == run_id}
```

### 2.7 K-70：`required_coverage` —— **只预留语义**

**业界**：主流是默认严格（Airflow `all_success`）+ 逐任务声明放宽；「部分完成」不是终态而是被 `trigger_rule` 消费的状态集合；不满足时 **`upstream_failed`（响亮失败）**，非永久挂起。K8s Job `completions` 是计数形态。

**本仓**：X2 选宽松（`DONE_PARTIAL ∈ PREDECESSOR_SATISFIED`）以避免悬挂 —— 合理但与业界默认相反。

**本波只做**：① `ResultEnvelope.coverage`（[2276](harness/harness.py)）注释预留 `{"min_done_ratio"}` / `{"require": {...}}`；② 文档写明「严格化若实现，不满足**必须** `UPSTREAM_INSUFFICIENT` 终态失败，**不得**永久 PLANNED」。

**不实现的理由**：`coverage` 只在父 `result_envelope` 算，准入时刻拿不到 → 需写回 plan 成唯一判定源（60–100 行 + 一致性用例）。没有真实用例前做了，就是把 X2 刚修好的「不悬挂」重新弄坏。

---

## 3. S14B：MS8 封顶

**I12 补强**：构造饥饿（`max_concurrent=1` + 长任务 + 低优先级），断言「低优先级在 N 次 `pump()` 内被调度」或「无进展 → aging → INADMISSIBLE → readmit」。

**文档对齐**：I11/I13/I14 → **强**（附行号）；MS10「无产生路径」→「4 条」；MS9「无注入」→「注入已存在，缺 Protocol 契约」。

---

## 4. 测试

| # | 用例 | 判据 |
|---|---|---|
| S14-1 | **K-69 重启后结算** | 进程 A 写等待态 → 进程 B **不 recover**、`tick(now>deadline)` → **结算**（当前必红） |
| S14-2 | K-69 不动点 | `tick(now)` 后 `next_deadline_ms() > now` 或 `None` |
| S14-3 | K-69 pump | `pump()`==0 时无剩余；>0 时 `next_deadline_ms` 非空 |
| S14-4 | K-69 零破坏 | 内存 store（`refs_all` 空）→ 行为同 S13 |
| S14-5 | **K-69 锁序** | `tick` 触发 `_finalize` **无 `LockOrderViolation`**（durable 模式） |
| S14-6 | K-68 条件写 | 双线程同时替换 child ref → 不丢更新；**负对照**：无 `expect_ref` → 后写覆盖 |
| S14-7 | K-71(a) | 超限 → `FAILED(INLINE_DEPTH_EXCEEDED)` + 传播 + Run 能 finalize |
| S14-8 | K-67(B) | replan 后 `edges` 端点全 canonical |
| S14-9 | K-67(A) | replan 3 次别名表**单调不增**；在途 ACB 映射**不被剪**（负对照：旧判据会剪） |
| S14-10 | I12 不饿死 | `max_concurrent=1` + 低优先级 → N 次 pump 内被调度 |

---

## 5. 迁移清单

| 文件 | 变更 |
|---|---|
| `harness/harness.py` | `tick` 改「内存 ∪ `refs_all`」+ 抽 `_tick_one`（**无 store 锁下执行**）；`next_deadline_ms`/`pump`/edge Event；`_replace_child_ref` 传 `expect_ref`；深度超限改显式失败；`replan()` 加剪枝 |
| `harness/checkpoint.py` | `update_children` 条件写 + `_save_payload_locked`（若 `_FileLock` 不可重入） |
| `harness/orchestration.py` | `patch()` 末尾物化 `edges` 端点 |
| `harness/types.py` | `ResultEnvelope.coverage` 注释预留语义 |
| `tests/` | `test_s14_boundaries.py`、`test_s14_i12.py` |
| `docs/milestones.md` | MS8 分级 + MS10 `DEAD_LETTER` + MS9 门槛 |

---

## 6. 风险与对策

| 风险 | 对策 |
|---|---|
| **`tick` 持 store 锁调 `_finalize` → `LockOrderViolation`** | §2.2 硬规则：先水合（释放锁）→ 再结算 |
| `refs_all` 文件版是 O(N) 目录扫描 | `tick` 非热路径；内存命中先跳过；N 大再加索引（不在本波） |
| `_hydrate` 误结算不在内存的 ACB | 只结算**已到期且状态匹配**；`_finalize` 幂等；`load_strict` 失败记 `tick_skip` |
| K-67(B) 物化 edges 改 `test_s13_replan.py` 断言 | 断言改「端点 canonical」是**加强** |
| K-68 条件写活锁 | `retries=3` 有界；超限显式 `FencingRejected` |
| K-71(a) 改状态打挂断言 | **已核实** `grep` 零命中 |
| I12 饥饿场景依赖时序 | `ManualClock` + 显式 `pump`，不用 sleep |

---

## 7. 决策（定案）

| # | 结论 |
|---|---|
| Q1 | S14 = A（K-67..K-71）+ B（MS8 封顶） |
| Q2 | K-71(b) **推 S15**（`spawn_depth=1` 收益 0） |
| Q3 | K-70 **只预留语义**（避免弄坏 X2） |
| Q4 | S15 = C（Protocol 契约）+ D（生产化最小集） |
| Q5 | MS9 真 Raft **推迟** |

---

## 8. 自评审记录（v1.0 → v1.2）

| # | 问题 | 修法 |
|---|---|---|
| **R-1** | K-69 方案过重：新建 `deadlines` 索引（~50 行 × 两套 store） | 复用 `refs_all`（~15 行） |
| **R-2** | 未说明内存版 store 怎么办 | `refs_all` 空 → 自然退化，零破坏 |
| **R-3** | 未考虑水合失败 | `load_strict` 失败 → `tick_skip`，不静默 |
| **R-4** | K-67 剪枝判据用 v1.0 旧判据 | 补「活跃 ACB 的 task_id/subgoal」 |
| **R-5** | K-71(a) 未提可能打挂断言 | 补 grep 前置（**已核实零命中**） |
| **R-6** | I12 判据含糊（「N tick 内」） | 改「N 次 `pump()` 内」+ aging 联动 |
| **R-7** | 未说明 K-69 与 `scan_deadlines` 关系 | 明确：`tick` 仍调 `scan_deadlines`（aging） |
| **R-8** | **锁序红线未写**：`tick` 持 store 锁调 `_finalize` → `budget(1)` 套 `store(5)` → `LockOrderViolation` | 新增 §2.2 硬规则 + S14-5 用例 |

---

## 9. 实现结果（v1.2 → v1.3）

### 9.1 逐项交付（附代码位置）

| 项 | 实现 | 用例 |
|---|---|---|
| **K-69** | `Harness.tick(now_ms, *, persist_scan=True)` → `_tick_candidates()` = 内存 ∪ `checkpoints.refs_all()`；新增 `next_deadline_ms()` / `pump(now_ms)`；`_tick_skipped` 记录枚举/读取/水合失败（不静默）；**`recover()` 内 `persist_scan=False`**（恢复未完成时结算别的 ACB 是错的，S13 评审 R-5） | S14-1/2/3/4/5 |
| **K-68** | 内存版与文件版 `update_children(acb_id, mutate, *, expect_ref=None, retries=3)`：**冲突即「置为最新 ref 后重读重应用」**（`mutate` 每次收到最新 children 列表，故不丢更新）；重试耗尽 → `FencingRejected`；文件版新增 `_save_payload_locked`（CAS 必须在**同一 flock 内**完成） | S14-6（含负对照 + 耗尽） |
| **K-71(a)** | 深度超限：新增状态机边 `(ADMITTED/RUNNING, "inline_depth_exceeded") → FAILED`；返回 `GUARD_VIOLATION` + `failure="INLINE_DEPTH_EXCEEDED"` + `_finalize` 传播（旧写法 `INVALID_INPUT` 是可重试语义） | S14-7 |
| **K-67(B)** | `Planner.patch()` 末尾**物化 edges 端点**为 canonical id（Camunda「迁移时物化」同构） | S14-8 |
| **K-67(A)** | `Harness.replan()` 在 `_readmit_after_replan` **之后**调 `_prune_id_aliases`；判据 = `edges 端点 ∪ plan["tasks"] ∪ 本 run 活跃 ACB 的 task_id ∪ logical_subgoal_id` | S14-9 |
| **K-70** | 仅 `ResultEnvelope.coverage` docstring **语义预留**（`min_done_ratio` / `require`），并写明「严格化若不满足**必须** `UPSTREAM_INSUFFICIENT` 终态失败」 | — |
| **I12** | 新增 `tests/test_s14_i12.py`：饿死任务经 aging **有界离开 ADMITTED** → `INADMISSIBLE` → readmit → 第 N 次 `DEAD_LETTER`；负对照 `aging_ms=0` 永停 ADMITTED | S14-10 |
| 文档对齐 | `milestones.md` MS8 覆盖分级（I11/I13/I14 → 强，附代码位置）、MS9 前置（注入已存在）、MS10（`DEAD_LETTER` 已有 4 条产生路径） | — |

### 9.2 门禁

| 门禁 | 结果 |
|---|---|
| pytest 全量 | **494 passed + 4 xfailed**（连跑 5 轮全绿；基线 475 → 494，+19） |
| 覆盖率 | **通过**（`check_coverage.py` exit 0） |
| bench | `--n 30` exit 0；`report_check` exit 0 |

### 9.3 实现期发现并修正的两处偏差（记录在案）

| # | 偏差 | 处理 |
|---|---|---|
| **D-1** | `tick` 首次实现时 `recover()` 也做持久扫描 → **打挂 2 个 S10 用例**（`_applied_event_ids` 被别的 ACB 结算干扰） | 加 `persist_scan` 开关，`recover` 传 `False`；与本文件 §2.1 的「恢复未完成不结算他人」一致 |
| **D-2** | K-67(A) 剪枝初版放在 `_readmit_after_replan` **之前** → 再准入拿不到旧 id 的 task（`replan_no_task`） | 剪枝移到 `_readmit_after_replan` **之后**；S14-9 与 `test_s13_4_two_replans` 共同锁定该顺序 |

### 9.4 因「前提改变」而改写（非放宽）的两个既有断言

| 用例 | 原断言 | 现断言 | 理由 |
|---|---|---|---|
| `test_s13_4_negative_control_without_alias_ready_empty` | 「去掉别名 → `ready` 为空」 | 「edges 已物化（`ready` 不依赖别名）；但**旧 task_id → 当前 task** 仍只能靠别名」 | K-67(B) 后旧前提失效；新断言**更强**（锁定剪枝判据必须含活跃 ACB 的 task_id） |
| `test_s13_4_two_replans_still_resolve_old_ids` | 「两次 replan 后 `T1`/`T1'` 永久可解析」 | 「edges 全 canonical；别名表 `≤1`（有界）；当前代完成 → 后继仍就绪」 | 「永久可解析」正是 K-67 要移除的无界增长；新断言量化了「有界」 |

### 9.5 新增已知边界（v1.3）

| # | 边界 |
|---|---|
| K-72 | `tick` 的持久扫描是 **O(N) 目录枚举**（文件 store）；低频调用可接受，N 大时需索引（本波未做） |
| K-73 | `pump()` 只**被动**结算到期项；长期空闲的等待态仍需 supervisor 反复调 `pump()`（无后台线程，仍受 K-69 原约束的「驱动器」部分） |
| K-74 | K-70 `required_coverage` **仅语义预留**，未实现严格化（无真实用例前不实现，避免弄坏 X2 的「不悬挂」） |
| K-75 | K-71(b)（父等子释放槽位）**未做**：默认 `spawn_depth=1` 时收益为 0，推 S15 |
