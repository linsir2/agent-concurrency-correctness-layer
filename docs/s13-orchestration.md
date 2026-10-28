# S13：orchestration 四件套（HITL / replan / spawn_child / Scheduler）

| 项目 | 内容 |
|---|---|
| 版本 | **v1.1（已按两路对抗评审修订）** |
| 日期 | 2026-10-27 |
| 上游 | [S12 v1.2](s12-kernel-enforcement-and-core.md) §6（K-57）、[milestones.md](milestones.md) MS4/MS10、[设计规范](multi-agent-harness-design-final.md) §5.2⑦ |
| 分波 | **S13-0** schema+契约 → **S13a** HITL → **S13b** replan → **S13c** spawn_child → **S13d** Scheduler |
| 修订 | v1.0 → v1.1：修 7 P0 + 9 P1（§7） |

---

## 0. 调研结论

### 0.1 本地代码：4 个真 bug（已定位到行）

| # | bug | 证据 |
|---|---|---|
| **B-1** | **HITL 审批绕过**：`_action_request_hitl` 落盘后（runtime.py:287）、写 `APPROVAL_RESOLVED` 前（297）崩溃 → recover 读回 `WAITING_APPROVAL`（harness.py:1708），但 `runtime.run` **无条件置 RUNNING**（runtime.py:62）→ 直接执行。`_pending` 键仅 `task_id`（orchestration.py:544）→ 跨 run 串键 | 审批不是门，是可丢失的内存默认值 |
| **B-2** | **alias 断链有 3 处消费点**：`Planner.patch` 记 `_id_aliases[T1]=S1`（149-153），但 ① `_predecessors_done`（312-338）② `rebuild_ready_set`（521-527）③ `_finalize` 的 push（1526，`src == acb.task_id`）**均不读 aliases** → `T1→T1'` 后**后继永不就绪/永不 push** | 两次 replan 后必现 |
| **B-3** | **Scheduler 双跑**：`run_task` push 后**直接内联 `_run_one`**（1435-1437），finally 才 drain（1439）；线程池会在窗口内 pop 同一 ACB。`CrashInjected(BaseException)` 被 `Future` 吞 | 池化必现 |
| **B-4** | **`aging_deadline` 死字段**：`runtime.py:140` 写、`checkpoint.py:225` 存、`harness.py:1721` 恢复，**全仓无读取** | aging 升级路径不存在 |

### 0.2 契约缺口

| 项 | 状态 |
|---|---|
| `TRANSITIONS` | 约 20 条边无触发，状态多为**直接赋值** |
| **已存在的边（v1.0 误报缺失）** | `(WAITING_APPROVAL,"timeout")→FAILED`（types.py:176）、`ACB.generation`（types.py:290） |
| **真缺失的边** | `AGING_TIMEOUT`、`ADMITTED→INADMISSIBLE`、`REPLANNING→DEAD_LETTER`、`INADMISSIBLE→PLANNED` |
| `escalate_quarantined` | 名实不符：走 `replan_budget_exhausted`→DEAD_LETTER，`escalate` 边永不触达 |
| ACB 未持久化 | `capability`、`parent`/`children`、`hitl_pending`（无字段）、`replan_count`（无字段） |
| `Planner.plan` | **丢弃 `footprint.parent_budget`**（105-110）→ 恒为 `"run"` → 多级预算树不可达 |
| **D10 旧 bug** | `_finalize` push **task_id**（1521-1527）但 `run_task` push **acb.id**（1435）；`_drain_scheduler` 兜底**按 task_id 扫第一个 ADMITTED**（1463-1465）→ replan/spawn 后**跑错代际 ACB / 双跑** |

### 0.3 业界共识（6 条）

| # | 结论 | 代表 |
|---|---|---|
| **C-1** | **权限只减不增**；后台子未预批准工具**自动拒绝且不能弹窗** | Claude / Codex / Devin |
| **C-2** | **agent 的消息不能算审批**；审批必须**绑 agent 身份**（否则泄漏到兄弟/后代） | Claude 明确；OpenAI `always_approve` 不按 agent 隔离 = **反例** |
| **C-3** | **fail-closed 是共识**：参数无法解析 → 直接人工；非交互无法弹窗 → **失败** | OpenAI / Devin / Codex / Cursor |
| **C-4** | **超时判失败**（GitHub Actions 30 天 → expired **failed**）；**绝不无限等**（LangGraph 无限等 = 生产反模式） | GitHub / Temporal |
| **C-5** | **深度默认 1–3 层**：Codex `1/6`、Cursor `2`、Claude `3/20`、Devin `3`、CodeBuddy `5/20/200`；Claude 已移除总数上限 → **总数与预算联动，不是裸计数** | 全行业 |
| **C-6** | **防双跑 = claim + lease + fencing**：SQS visibility timeout / Temporal activity idem key / Celery `acks_late` 要求幂等 | 消息队列 |

### 0.4 两路调研冲突与裁决

| | 建议顺序 |
|---|---|
| 业界（agent 视角） | HITL → **Scheduler** → spawn → replan（spawn 需槽位基础设施） |
| 本地（代码视角） | HITL → replan → spawn → **Scheduler**（池化横切 **36 个函数/37 个用例**） |

**裁决：采纳本地顺序**。理由：`spawn_child` 在 sync 模式也能工作（父同步等子），「spawn 需池」在本仓库不成立（`SlotTracker` 已与池解耦）；池化横切面是全仓最大。

### 0.5 **同步执行路径的真实规模（修正 v1.0 的「24」）**

v1.0 说「24 个」只统计 5 个文件（ms4:3 + ms3:4 + adv:6 + e2e:6 + real:5）。实际**另有 12 个函数**同样走 `run_task`/`dispatch_and_run`：

| 文件 | 数量 |
|---|---|
| `test_ms2_budget.py` | 4 |
| `test_s8_coverage.py` | 3 |
| `test_k28_bench_regressions.py` | 2 |
| `test_review_counterexamples.py` | 1 |
| `test_ms7_capability.py` | 1 |
| `test_s9_cross_process.py` | 1 |

**真实规模 = 36 个函数 / 37 个 test item**（U4 parametrize 算 2）。sync 回归矩阵必须覆盖全部 36，不只是 24。

---

## 1. 目标

| 波 | 目标 | 判据 |
|---|---|---|
| **S13-0** | schema 一次迁移 + `RunCoordinator` 发现契约 | 旧 payload 可 load（`.get` 默认） |
| **S13a** | HITL **三态 + fail-closed + tick 驱动超时 + 跨进程恢复**（修 B-1） | S13-1/2/3 |
| **S13b** | replan（修 **B-2 三处** + 预算落盘 + 再准入复用 `requeue_readmit`） | S13-4/5/6 |
| **S13c** | `spawn_child`（能力子集 + **无双重预留** + 取消级联 + 有界） | S13-7/8/9/10 |
| **S13d** | Scheduler（默认 sync + claim/fencing + aging 接线，修 B-3/B-4/D10） | S13-11/12 |

---

## 2. 设计

### 2.1 S13-0：一次 schema 迁移（避免两次）

**在 S13a 就把所有新字段一次性加进 payload**（全部 `.get` 读，兼容旧格式）：

```python
# checkpoint payload 新增（均为 .get 读）
hitl_pending   = {"approval_id": str, "deadline_ms": int, "plan_version": int}
replan_count   = 0
parent_id      = ""
children_ids   = []          # 存 (acb_id, checkpoint_ref) —— 内存版 latest 恒 None
capability     = {"agent_id","tools","paths","network","data_class"}   # 转 dict，不能放对象
```

**关键**：
- `CapabilityToken` **不能直接序列化**（codec 只支持内建类型/DELETE/bytes）→ 转 dict，恢复时重建
- `children_ids` **必须存 ref**（`latest_ref_for_acb` 内存版恒 `None`，checkpoint.py:123-124）
- `schema_version` 递增但 **load 不严格比对**（否则 S9 legacy 用例红）
- **不做原地重写**（`_write_new` 用 `os.link` 拒绝覆盖，checkpoint.py:608-633）→ 只做读时默认

### 2.2 S13a：HITL（修 B-1）

**三态（P0-1，修 v1.0 的 absent≠timeout 混淆）**：

```python
_UNSET = object()

class HITL:
    def resolve(self, acb) -> dict | None | object:
        key = (acb.run_id, acb.hitl_pending["approval_id"])   # ← 修：不用 generation
        return self._pending.get(key, _UNSET)                  # absent ≠ None
```

| 返回值 | 语义 | 动作 |
|---|---|---|
| `_UNSET` | **还没人审批** | `NEEDS_APPROVAL` + **保持 `WAITING_APPROVAL`**（不执行、不失败） |
| `None` / `{"approved": False}` | **显式拒绝** | `FAILED(APPROVAL_DENIED)` |
| deadline 到期 | **超时** | `FAILED(APPROVAL_TIMEOUT)` |
| `{"approved": True}` | 批准 | `advance(acb, "resume")` 继续 |

**为什么不用 `generation` 做键（P0-2）**：`recover` 会把 generation **+1**（harness.py:1707）→ 崩溃前已批准的审批在恢复后**键失配** → 反而丢失审批。用 `approval_id`（随 `hitl_pending` 落盘）。

**恢复不置 RUNNING（修 B-1 根因）**：
```python
# 注意：必须在 run_task / dispatch_and_run 进入 _run_one **之前**判态，
# 不能只改 runtime.run —— 否则 _finalize 会无条件 slots.release（harness.py:1508）
# → 每次重试泄漏并发额度
if acb.state is TaskState.WAITING_APPROVAL and not resolved:
    return Status.NEEDS_APPROVAL          # 不 push、不 finalize、不 release 槽位
```

**tick 显式驱动（P0-3：不实现则超时是空承诺）**：
```python
class Harness:
    def tick(self, now_ms=None) -> None:
        """唯一超时驱动面。supervisor/调度器每轮调用；recover 结束也调一次。"""
        self.reconcile.scan_deadlines(now_ms or self.clock.now_ms())
```
- **不引入后台线程**（`ManualClock` 不可共享，会 flaky）
- 测试用 `ManualClock.advance()` + `h.tick()` 精确构造
- 扫描与审批提交**同一把锁内 CAS**，否则「批准/超时」双写

### 2.3 S13b：replan（修 B-2 三处）

**alias 三处消费点统一（P0-4）**：
```python
def resolve_alias(self, old_id: str) -> str:
    """一跳解析：old_id → 稳定 logical_subgoal_id（**不做链式递归，避免环**）"""
    return self._id_aliases.get(old_id, old_id)

# ① _predecessors_done ② rebuild_ready_set ③ _finalize 的 push —— 三处都用它
```

**值保持稳定 `logical_subgoal_id`（不做 id 链）** → 天然无环、无需深度上限。v1.0 若改成「old_id → new_id」链式解析，`missing_deps` 可直接 append 无环检测的边（orchestration.py:162-163）→ **可造环**。

**剪枝不能用「最近 K 版本」（P1）**：`patch` **从不改写 edges**，`T1→T2` 从 v1 活到 vN。剪枝判据必须是「该 old_id 是否仍出现在**任何** plan 版本的 `edges.from` 中」。

**replan 顺序（P0-5：预算落盘先于副作用）**：
```python
def replan(self, acb, plan) -> Plan | None:
    if acb.replan_count >= self.replan_budget:        # 先查
        advance(acb, "replan_budget_exhausted"); return None
    acb.replan_count += 1
    self.checkpoints.save(acb)                        # ← write-ahead 落盘（防崩溃后无限重规划）
    new_plan = self.planner.patch(plan, ...)          # patch 为**纯函数**（不 append 事件）
    self._readmit(acb, new_plan)                      # ← 复用 requeue_readmit 清理序列
    return new_plan
```

**再准入必须清理旧节点（P0-6）**：直接复用 `requeue_readmit`（harness.py:1660-1699）的顺序：
```
release old slot → settle old reservation_ids → remove_node/archive_node →
try_admit 新 → 新 ACB；若旧节点 consumed>0，新 min_budget 扣掉已消耗
```
否则：旧节点 `self_reserved` 不归零 → `root.subtree_reserved` 双份 → 违反 I5；`remove_node` 因 ACTIVE 预留返回 False → **旧节点永久残留**，父节点永不归档。

**`escalate_quarantined` 保留旧行为（P1 定死，不留「或」）**：继续走 `replan_budget_exhausted`→`DEAD_LETTER`（T21 断言不变）；新增 `replan_from_quarantine(acb)` 走 `escalate`→`REPLANNING`→`new_plan`。

### 2.4 S13c：spawn_child

**能力子集（P0-7：v1.0 的 `min()` 是字面量比较 → 越权）**：
```python
# ❌ v1.0：min(parent.data_class, requested.data_class)
#    min("public","confidential") == "confidential" → 子拿到比父更高的等级！
# ✅ 用仓库已有的分级序 _DATA_CLASS_LEVELS（policy.py:23-29）
def _lower_class(a: str, b: str) -> str:
    if a == "*": return b
    if b == "*": return a
    la, lb = _DATA_CLASS_LEVELS.get(a), _DATA_CLASS_LEVELS.get(b)
    if la is None or lb is None:
        raise EgressDenied(f"未知 data_class: {a!r}/{b!r}")   # 未知 → 拒绝（fail-closed）
    return a if la <= lb else b

def attenuate(parent, requested, child_id) -> CapabilityToken:
    """tools/paths/network 用通配感知的交集；data_class 用分级序取低。"""
    # 父 "*" → 保留 requested；子 "*" 且父非 "*" → 取父
```

**禁止子自签（P1）**：`policy.issue` 的合同**不能改**（`test_i4_capability_production_no_self_issue` 会红）。改为：
- 新增 `attenuate_token(parent_token, requested, child_id)`——**唯一**给子签发的路径
- token 内嵌 `parent_id` + `max_data_class`；`resolve_token`/`check_capability` 校验「有 parent_id 的 token 必须能追溯到父 token」
- **恢复后不得回退到 `default_token`**（现状 `_restore_acb` 不恢复 capability → 退到通配 token，抹掉 attenuation）

**预算：禁止双重预留（P0-8）**：
```python
# ❌ v1.0：reserve(parent_node, min_budget) + try_admit 里又 reserve(child_node)
#    → 父 subtree_reserved 增加 2×min（静默超支）；slice_resv 未登记 → 永不结算 → 永久泄漏
# ✅ 二选一（定死推荐方案）：
#    父准入时 footprint.child_alloc 已把子份额计入父 limits
#    spawn 时只 create_node(child, limits=slice, parent_id=父节点) + reserve(child_node, min)
#    → 链式传播只算一次
```
且父入准 `limits = min_budget + retry_budget + 1` 时，`remaining` 只剩 `retry_budget+1`（默认 4）→ 任何 `min_budget > 4` 的 child 都失败。必须引入 `child_alloc` 概念。

**取消级联两阶段落盘（P1）**：
```python
# ① 先写父的 children_pending 并 save（崩溃可续做）
# ② try_admit 建子
# ③ 成功后改成 children_ids 再 save
```
否则 `try_admit` 与 `save` 之间崩溃 → 子已占预算但父 `children_ids` 无它 → **孤儿**。
`cancel_run` 必须同时维护 `_cancelled_acbs: set[acb_id]`（`is_cancelled` 先查 acb_id 再查 run_id，否则子用独立 run_id 时看不到取消）。
`_descendants` 对「ref 缺失/已归档/已终态」分别 skip，**绝不中断级联**；结算全部走幂等 API。

**有界（C-5）**：`depth=1`（对齐 Codex）、`fanout=6`、总数与预算联动（`min_budget_per_child`）。

**ResultEnvelope**（结构化回传）：`status/coverage/artifacts/conflicts/budget_usage` → 支撑 `DONE_PARTIAL` 产生路径。

### 2.5 S13d：Scheduler（修 B-3/B-4/D10）

**默认 sync（36 个用例零破坏）**：`Harness(scheduler_mode="sync"|"pool")`，默认 `"sync"`。

**pool 嵌套死锁防护（P0-9）**：
```python
# worker 线程用 thread-local 标记；worker 内嵌套 run_task → **内联 _run_one**（不入队）
# 否则池满时父等子 → 无 worker 可 pop → 永久死锁
if getattr(self._tls, "in_worker", False):
    return self._run_one_inline(acb, ...)      # 内联 + spawn_depth 防递归
```

**claim + fencing（P0-10）**：
```python
@dataclass
class Claim:
    acb_id: str; worker_id: str; lease_until_ms: int; attempt: int; fence_epoch: int
# re-claim 时**原子地** supervisor_note_crash(old)（revoke + bump_epoch(run)）再写新 claim
# 写路径（commit_batch/budget.consume/effects.begin/checkpoints.save）校验 acb.claim_epoch
```

**`CrashInjected` 必须重抛**：`future.result()` + `except CrashInjected: raise`。

**aging 接线（修 B-4 + P1）**：
```python
# 用 aging_ms（墙钟）而非复用 aging_max（步数）；在 try_admit 成功后设 deadline
# 新增边 (ADMITTED,"aging_timeout")→INADMISSIBLE + settle/archive/release slot
# ⚠️ INADMISSIBLE 非终态 → Run 永不 finalize（P1）→ 必须：
#    新增 (INADMISSIBLE,"readmit")→PLANNED，或 aging 超过 N 次 → DEAD_LETTER（真终态）
```

**修 D10（P1）**：scheduler 队列键统一 `acb.id`；`_finalize` 按 `(run_id, dst, plan_version)` 解析到**当前版本唯一 ACB** 再 push（解析不到记 `_drain_skipped`，不静默）；**删除「按 task_id 扫第一个 ADMITTED」兜底**。

---

## 3. 测试（12 组，含负对照）

| # | 用例 | 关键判据 + 负对照 |
|---|---|---|
| S13-1 | HITL 三态 | `_UNSET` → 仍 `WAITING_APPROVAL`（不执行）；`None` → `APPROVAL_DENIED`；deadline → `APPROVAL_TIMEOUT` + 释放。**负对照**：旧实现（进入即 RUNNING）下必红 |
| S13-2 | HITL 跨进程 | 注入 `resolve` 抛 `CrashInjected`（正好在 287/297 之间）→ 共享 store+event_log 恢复 → **仍 `WAITING_APPROVAL`**；批准后 `advance("resume")` 继续。**负对照**：`hitl_pending` 置空仍返回 `NEEDS_APPROVAL`（无 `TOOL_COMPLETED`、版本不变） |
| S13-3 | HITL 键不串 | 两 run 同 `task_id`：只批准 R1 → R1 DONE、R2 仍 WAITING。**负对照**：旧 `set_result(task_id="T1")` 会让两个都恢复 |
| S13-4 | replan alias | ①`rebuild_ready_set` 返回含 T2 ②**执行层**：`_finalize` 后 `_drain_scheduler` 真跑 T2（`ran==["T1'","T2"]`）③两次 replan（T1''）仍可解析。**负对照**：去掉 alias 解析 → ready 为空 |
| S13-5 | replan 预算 | `PLAN_PATCHED` **事件数**不变（主断言）+ plan.version 不变 + `replan_count==0` + `state is DEAD_LETTER`。**落盘往返**：save→recover→再 replan 仍 DLQ |
| S13-6 | replan 再准入 | 新 ACB 有预算节点 + **旧节点已 settle 且移除**（无残留）+ 旧 epoch 写 `REJECT_STALE` + **另一 run 租约仍有效**（per-run 负对照） |
| S13-7 | spawn 能力子集 | ①交集不变量 ②**正控制**（交集内仍允许）③越界运行时 `DENIED_CAPABILITY` ④`resolve_token(child).agent_id == child.id`。**夹具陷阱**：必须裸 `Harness+mint`，不能用 `make_harness`（`default_token="*"` 会全放行） |
| S13-8 | spawn 预算守恒 | Barrier+计数：成功数==1、其余 `CHILD_BUDGET_EXHAUSTED`、`parent.self_reserved == 成功切片量`（**不是只看「没超」**）、零残留快照（nodes/reservations/slots/事件计数/children 长度） |
| S13-9 | 取消级联（含崩溃前） | ①可发现性（`children_ids` 含 child）②状态 CANCELLED ③**持久化**（load_strict 见 CANCELLED）④租约全 invalid ⑤预算全 SETTLED/RELEASED + `check_invariants` ⑥槽位/事件。**负对照**：`cascade=False` 时 child 仍非终态 |
| S13-10 | spawn 有界 | 超 depth/fanout/预算 → 拒绝且父不崩；零残留 |
| S13-11 | Scheduler 单所有者 | ①`_run_one` 次数==1 ②idem_key 的 `TOOL_COMPLETED`==1 ③`receipts_of` 数量==1 ④`claims` 只 1 owner。**`CrashInjected` 被 `pytest.raises` 捕获** 且 drain 未再跑。**负对照**：`pop` 返回两次且不 claim → `TOOL_COMPLETED==2` |
| S13-12 | aging | `tick()` 显式驱动：`deadline-1` 未超时、`deadline` 超时 → `INADMISSIBLE` + 结算 + **Run 能 finalize**。**触发器有效性**：spy `scan_deadlines` 在 `run_task` 期间被调 ≥1 次 |

---

## 4. 迁移清单

| 文件 | 变更 |
|---|---|
| `harness/types.py` | ACB 增 `hitl_pending`/`replan_count`；补边 `(ADMITTED,"aging_timeout")→INADMISSIBLE`、`(INADMISSIBLE,"readmit")→PLANNED`、`(REPLANNING,"replan_budget_exhausted")→DEAD_LETTER`。**`ACB.generation` 与 `(WAITING_APPROVAL,"timeout")` 已存在，勿重复加** |
| `harness/orchestration.py` | `HITL` 三态 + `approval_id` 键；`resolve_alias` **三处**；`replan`；`replan_from_quarantine`；`spawn_child`；`cancel_run` 级联 + `_cancelled_acbs`；`Scheduler` claim/lease/`acb.id` 键；`Planner.plan` 保留 `parent_budget`/`child_alloc`；`patch` **纯函数化** + `missing_deps` 走环检测 |
| `harness/runtime.py` | `WAITING_APPROVAL` 判态移到 `run_task` 入口；`aging_deadline` 接线 |
| `harness/harness.py` | `Harness.tick()`；`scheduler_mode`；`_run_one_inline` + thread-local；`_finalize` 按 `(run_id,dst,plan_version)` push + 删 task_id 兜底；recover 恢复新字段；**`escalate_quarantined` 保留旧行为** |
| `harness/policy.py` | `attenuate_token(parent, requested, child_id)`（**不改 `issue` 合同**）；`_lower_class` 用分级序；token 内嵌 `parent_id` |
| `harness/checkpoint.py` | payload 增 5 个字段（`capability` 转 dict、`children_ids` 存 `(id, ref)`）；`schema_version` 递增但 load 不严格比对 |
| `tests/` | 新增 4 文件；**迁移 T19 + A8 到「显式审批」语义**；A5 改为也写 checkpoint |

---

## 5. 风险与对策

| 风险 | 对策 |
|---|---|
| **HITL 默认 deny 与「36 个全绿」互斥** | T19 + A8 列为**契约迁移**（显式 `set_result`），不计入「零破坏」；文档同步写明 |
| spawn 双重预留 | `child_alloc` 预分配，子只 `create+reserve`（不双预留） |
| `data_class` 字面量比较致越权 | 用 `_DATA_CLASS_LEVELS`，未知等级 → 拒绝 |
| pool 嵌套死锁 | thread-local 内联 + `spawn_depth` |
| claim 双持 | `fence_epoch` 接入写路径 |
| **INADMISSIBLE 非终态 → Run 吊死** | 加 `readmit` 边 + 超 N 次 → `DEAD_LETTER` |
| replan 双份占用 | 复用 `requeue_readmit` 清理序列 |
| capability 序列化失败 | 转 dict；恢复不回退 `default_token` |
| **D10 兜底双跑** | 队列键统一 `acb.id` + 删 task_id 兜底 |
| `aging` 谓词顺序打挂 guard 测试 | 以 `aging_deadline is not None` 为门，放在现有谓词**之后** |

---

## 6. 决策（v1.1 定案）

| # | 问题 | 结论 |
|---|---|---|
| Q1 | 分波 | **S13-0 schema → a HITL → b replan → c spawn → d Scheduler** |
| Q2 | HITL 语义 | **三态**（`_UNSET`≠`None`≠timeout）；键 `(run_id, approval_id)`，**不用 generation** |
| Q3 | 超时驱动 | **`Harness.tick()` 显式驱动**（不引后台线程） |
| Q4 | `escalate_quarantined` | **保留旧行为**（DEAD_LETTER），新增 `replan_from_quarantine` |
| Q5 | spawn 预算 | **`child_alloc` 预分配**，禁止父+子双重 reserve |
| Q6 | spawn 深度 | **默认 1**（对齐 Codex） |
| Q7 | Scheduler | **默认 sync**，pool 走 flag；worker 内嵌套内联 |
| Q8 | 同步回归规模 | **36 个函数 / 37 个用例**（不是 24） |

---

## 7. 评审修订记录（v1.0 → v1.1）

### 7.1 P0（7 项）

| # | v1.0 问题 | 修法 |
|---|---|---|
| **P0-1** | HITL `resolve` 缺失返回 `None`，而 handler 又把 `None` 当超时 → **「等待」与「拒绝」混为一谈**，进程内永远不能真等待 | 三态 + `_UNSET` 哨兵 |
| **P0-2** | 审批键用 `generation`，但 `recover` 会 +1 → **崩溃前已批准的审批键失配** | 改用 `(run_id, approval_id)` |
| **P0-3** | **`DurableTimer` 用「deadline+扫描」但全仓无扫描入口** → 超时是空承诺 | 新增 `Harness.tick()`，`recover` 后也调 |
| **P0-4** | 只修 `_predecessors_done` 一处，`rebuild_ready_set` + `_finalize` push **两处仍断链** | 三处统一 `resolve_alias` |
| **P0-5** | replan 先 patch 后计数 → **崩溃后 `replan_count` 归零 → 无限重规划** | write-ahead 落盘 |
| **P0-6** | 「必须再准入」但**旧节点未 finalize** → 双份占用 + 永久残留（`remove_node` 因 ACTIVE 返回 False） | 复用 `requeue_readmit` 清理序列 |
| **P0-7** | `min(parent.data_class, requested.data_class)` **字面量比较** → `min("public","confidential")=="confidential"` → **子越权** | 用 `_DATA_CLASS_LEVELS`；未知 → 拒绝 |

### 7.2 P0/P1（新增识别）

| # | 问题 | 修法 |
|---|---|---|
| **P0-8** | spawn **双重预留**（父 reserve + 子 reserve）→ 父 subtree 加 2×min（静默超支）+ `slice_resv` 未登记 → **永久泄漏** | `child_alloc` 预分配，子只 create+reserve |
| **P0-9** | pool 模式父在 worker 里 `_await_completion` 等子 → **池满死锁** | thread-local 内联 |
| **P0-10** | claim 无 fencing → 重 claim 后**两个 worker 同时持有** | `fence_epoch` 接写路径 |
| **P1-1** | `INADMISSIBLE` 非终态 → aging 把 **Run 永远吊死** | 加 `readmit` 边 + 超 N 次 DLQ |
| **P1-2** | spawn 崩溃窗口 → **孤儿**（`try_admit` 后、`save` 前） | 两阶段落盘 |
| **P1-3** | alias 剪枝「最近 K 版本」→ **必误删仍被引用的旧 alias** | 判据改「是否仍出现在任何 edges.from」 |
| **P1-4** | `capability` 是自定义对象 → codec `TypeError`；恢复退到 `default_token` → **抹掉 attenuation** | 转 dict + 恢复不回退 |
| **P1-5** | **D10 兜底按 task_id 扫第一个 ADMITTED** → replan/spawn 后跑错代际/双跑 | 队列键统一 `acb.id`，删兜底 |
| **P1-6** | `escalate_quarantined` 方案写「保留旧行为**或**同步改测试」= **含糊** | 定死保留旧行为 |

### 7.3 事实修正（v1.0 写错）

| 项 | 修正 |
|---|---|
| 「`(WAITING_APPROVAL,"timeout")` 缺失」 | **已存在**（types.py:176） |
| 「`ACB.generation` 需新增」 | **已存在**（types.py:290） |
| 「24 个同步测试」 | **36 个函数 / 37 个用例** |
| §3 表格标 12 组但只有 11 行 | 补 S13-10 |
| `policy.issue` 可改合同 | **不可改**（`test_i4_capability_production_no_self_issue` 会红）→ 新增 `attenuate_token` |

### 7.4 新增已知边界（v1.1）

| # | 边界 |
|---|---|
| K-63 | HITL 默认 deny 是**契约迁移**：T19 + A8 改为显式审批（不计入「零破坏」） |
| K-64 | `INADMISSIBLE` 需 `readmit` 边或 DLQ 兜底，否则 Run 不 finalize |
| K-65 | alias 只做**一跳解析**（值 = 稳定 logical id），不做 id 链（避免环） |
| K-66 | pool 模式仅承接**非 worker** 调用方；worker 内嵌套一律内联 |
