# S15：大类 C（可插拔契约）+ 大类 D（生产化最小集）

| 项目 | 内容 |
|---|---|
| 版本 | **v1.1（实现完成 + 自评审）** |
| 日期 | 2026-10-27 |
| 上游 | [S14 v1.3](s14-boundaries-and-ms8-closure.md) §1（大类 C/D）、[milestones.md](milestones.md) MS9/MS10 |
| 覆盖 | **C**：Protocol 契约补全 + 后端一致性套件 + **反漂移静态检查**；**D**：Ingress 幂等 + DLQ redrive + Admin pause/resume + 可观测快照 |
| 硬约束 | 494 passed + 4 xfailed 全绿；零第三方运行时依赖（`dependencies = []`） |
| 不覆盖 | MS9 真 Raft（需 `pysyncobj`，破坏零依赖，且单写者已够） |

---

## 0. 调研结论（含一处**对自己上一轮结论的更正**）

### 0.1 ⚠️ 更正：Protocol **早已存在**，我上一轮说「grep 零命中」是错的

S14 文档里我写「无 `Protocol`/`ABC` 契约声明（`grep` 零命中）」。**这是我 grep 正则写错**：我用了 `class \w+(Protocol|ABC)`，漏掉了 `class Clock(Protocol)` 这种带括号的形式。

**实测** [protocols.py](harness/protocols.py) 已有 **9 个 Protocol**：

| Protocol | 行 |
|---|---|
| `Clock` / `EventLog` / `IntentRegistry` / `BudgetManager` | 25 / 30 / 57 / 78 |
| `Workspace` / `CheckpointStore` / `EffectJournal` | 99 / 128 / 136 |
| `PolicyEngine` / `Decider` | 150 / 159 |

### 0.2 真正的缺口：契约**无人校验** → 已严重漂移

| 事实 | 证据 |
|---|---|
| **没有任何测试 import `protocols`** | `grep "protocols import" tests/` → **零命中** |
| **`CheckpointStore` 契约只声明 2 个方法** | `save` / `load`（[protocols.py:128-133](harness/protocols.py)） |
| **实现有 9 个，且 Harness 真的在调** | `load_strict` / `latest_ref_for_acb` / `refs_for_run` / `refs_all` / `update_children` / `read_epoch` / `bump_epoch`（[checkpoint.py:183-290](harness/checkpoint.py)） |
| **未标 `@runtime_checkable`** | `grep runtime_checkable` → 零命中 |

**后果**：若有人照 `CheckpointStore` Protocol 实现一个 Raft 后端（MS9 的目标），**Harness 会在运行时崩**（缺 7 个方法）。这就是 MS9 的真实拦路石——不是「没有接口」，而是「接口是错的且没人验」。

### 0.3 业界做法（决定设计）

| # | 结论 | 来源 |
|---|---|---|
| **P-1** | `Protocol` 做**结构子类型**，适合「零依赖 + 可替换后端」；`ABC` 需要显式继承，会强耦合 | Python typing 规范 |
| **P-2** | `@runtime_checkable` **只查方法名存在性，不查签名** → 不能单独作为一致性保证 | typing 文档已知限制 |
| **P-3** | 契约与实现的一致性必须靠**可执行测试**（参数化同一套用例跑多后端），不能靠注释 | Temporal/Redis 客户端做法 |
| **P-4** | Ingress 幂等：`Idempotency-Key` + **payload 指纹**；同 key 同指纹 → 重放原结果；同 key **不同**指纹 → **409 Conflict** | [IETF draft-ietf-httpapi-idempotency-key-header-02](https://datatracker.ietf.org/doc/html/draft-ietf-httpapi-idempotency-key-header-02)（该 draft 明确要求 409 + 指纹校验） |
| **P-5** | Admin `pause` ≠ `cancel`：pause 保留状态可恢复，cancel 终止；pause 必须**持久化**（重启后仍暂停） | Temporal `Cancel` vs `Terminate` 语义 |
| **P-6** | DLQ 必须**终态 + 可显式重放（redrive）**，且重放要有界（`max_redrives`），不能无限重投 | SQS DLQ + redrive |

---

## 1. 目标

| # | 目标 | 判据 |
|---|---|---|
| **C1** | 补全 Protocol 到「Harness 实际调用的全部方法」 | C-1 |
| **C2** | `@runtime_checkable` + `isinstance` 结构校验（双后端） | C-2 |
| **C3** | **后端一致性套件**：同一套断言跑 内存/文件（CheckpointStore）、内存/WAL（EventLog） | C-3 |
| **C4** | **反漂移静态检查**：`self.<comp>.<attr>` 必须都在对应 Protocol 里声明 | C-4（新增，长期防漂移） |
| **D1** | Ingress 幂等（`Idempotency-Key` + 指纹 + 409） | D-1 |
| **D2** | DLQ redrive（有界、显式、可审计） | D-2 |
| **D3** | Admin pause/resume（持久化、不丢状态） | D-3 |
| **D4** | 可观测快照（从 EventLog 派生，零依赖） | D-4 |

---

## 2. 大类 C 设计

### 2.1 C1：补全 Protocol（以「Harness 实际调用」为准，不以「想让契约长什么样」为准）

```python
@runtime_checkable
class CheckpointStore(Protocol):
    # 原有两个
    def save(self, acb: ACB) -> str: ...
    def load(self, ref: str) -> Any: ...
    # 补：Harness 真在调、但契约漏掉的 7 个
    def load_strict(self, ref: str) -> dict: ...
    def latest_ref_for_acb(self, acb_id: str) -> str | None: ...
    def refs_for_run(self, run_id: str) -> list[tuple[str, str | None]]: ...
    def refs_all(self) -> list[tuple[str, str | None]]: ...
    def update_children(self, acb_id: str, mutate, *,
                        expect_ref: str | None = None, retries: int = 3) -> str | None: ...
    def read_epoch(self) -> int: ...
    def bump_epoch(self) -> int: ...
```

同样处理 `EventLog`（补 `records` / `receipts_from` / `all`，如果 Harness 调用）、`BudgetManager`（补 `archive_node` / `gc` / `check_invariants` / `subtree_reserved` 等实际调用的）。

### 2.2 C2：`@runtime_checkable` 的真实价值与边界

- **能**：`isinstance(store, CheckpointStore)` 检「方法名都在」（快速冒烟）
- **不能**：检签名/参数默认值（P-2）→ **必须**由 C3 的实跑测试兜底
- 因此 C2 **不单独作为保证**，只作第一道筛

### 2.3 C3：后端一致性套件（`tests/test_s15_backend_conformance.py`）

```python
@pytest.fixture(params=["memory", "file"])
def checkpoint_store(request, tmp_path):
    return CheckpointStore() if request.param == "memory" \
        else FileCheckpointStore(str(tmp_path))

def test_ckpt_roundtrip(checkpoint_store): ...
def test_ckpt_latest_pointer_advances(checkpoint_store): ...
def test_ckpt_update_children_cas(checkpoint_store): ...     # K-68 语义必须两后端一致
def test_ckpt_refs_all_reflects_saves(checkpoint_store): ...
def test_ckpt_epoch_monotonic(checkpoint_store): ...
```

EventLog 同理（`EventLog` vs `WalEventLog`）：`append`/`append_batch`/`tail`/`records`/`receipts_from` 语义一致。

**关键**：这些用例在**当前实现下应当全部通过**（内存/文件已对齐）；若某条不过，说明**真有不一致 bug**（这正是套件的价值）。

### 2.4 C4：反漂移静态检查（**本次最有价值的一项**）

用 `ast` 扫 `harness/*.py`，收集 `self.<组件>.<attr>` 形式的所有属性访问，与 Protocol 成员比对：

```python
_COMPONENTS = {"checkpoints": CheckpointStore, "event_log": EventLog,
               "budget": BudgetManager, "intents": IntentRegistry,
               "workspace": Workspace, "policy": PolicyEngine,
               "effects": EffectJournal}

def test_no_contract_drift():
    accessed = _collect_component_attrs()      # ast 静态扫描
    for comp, proto in _COMPONENTS.items():
        declared = {n for n in dir(proto) if not n.startswith("_")}
        missing = accessed[comp] - declared
        assert not missing, f"{comp} 契约漏声明: {sorted(missing)}"
```

**为什么它比 C3 强**：C3 只验「我想到的用例」；C4 验「**Harness 用到的每一个属性**」，且**新增代码会自动被拦**（未来漂移立刻红）。

**排除项**（避免误报，需显式白名单 + 理由）：属性型访问（`self.budget.root`）、测试专用 setter、`_` 前缀内部方法。

---

## 3. 大类 D 设计

### 3.1 D1：Ingress 幂等（`harness/ingress.py`）

```python
@dataclass(frozen=True)
class IngressResult:
    status: Status             # OK / CONFLICT / INVALID_INPUT
    run_id: str = ""
    replayed: bool = False     # True = 命中幂等记录，未真的新建

class Ingress:
    """用户入口：幂等建 Run（IETF Idempotency-Key 语义）。"""
    def submit(self, *, idempotency_key: str, plan: dict,
               payload: dict | None = None) -> IngressResult:
        # ① 空 key → INVALID_INPUT（IETF：空值必须拒绝）
        # ② 同 key + 同指纹 → 重放原 run_id（replayed=True）
        # ③ 同 key + 不同指纹 → CONFLICT（对齐 IETF 的 409）
        # ④ 新 key → 建 Run，写 INGRESS_ACCEPTED 事件（指纹随事件持久化）
```

- **持久化**：指纹与 key 写 EventLog（`INGRESS_ACCEPTED`），跨进程可用 → 不依赖内存 dict
- **指纹**：`sha256(canonical_json(plan + payload))`，稳定序列化
- **与 `commit_key` 的关系**：`commit_key` 去重**提交**（同一变更不重复落库）；`idempotency_key` 去重**请求**（同一用户请求不重复建 Run）—— 两者层次不同，不合并

### 3.2 D2：DLQ redrive（有界、显式、可审计）

```python
def redrive_dead_letter(self, acb: ACB) -> ACB:
    """DEAD_LETTER → 重新可执行（**有界**：超过 max_redrives → 拒绝）。"""
    # ① 仅 DEAD_LETTER 可 redrive（其他状态 INVALID_INPUT）
    # ② redrive_count >= max_redrives → 拒绝（防无限重投，对齐 SQS）
    # ③ 走 requeue_readmit 的清理序列（release slot / settle / archive 旧节点）
    # ④ 写 DLQ_REDRIVEN 事件（审计：谁、第几次）
```

- **不自动重投**：必须显式调用（对齐 SQS redrive 是**手动动作**）
- `DEAD_LETTER` 仍是终态（`TASK_TERMINAL` 不变），redrive 是**换新 ACB**，不改旧 ACB 的终态

### 3.3 D3：Admin pause/resume（持久化、不丢状态）

```python
def pause_run(self, run_id: str, *, reason: str = "") -> bool
def resume_run(self, run_id: str) -> bool
def is_paused(self, run_id: str) -> bool
```

- **pause 语义**：`_drain_scheduler` 跳过该 run 的 ADMITTED（**不执行**），但**不释放**其槽位/预算/节点（状态完整保留，可恢复）
- **持久化**：`RUN_PAUSED` / `RUN_RESUMED` 事件 → 重启后 `recover` 重放得回暂停态
- **与 cancel 的区别**（P-5）：pause 可恢复且保留资源；cancel 释放资源且不可恢复
- **不阻塞已在执行的 ACB**：pause 只影响**未开始**的任务（在途的让它跑完，避免半提交）

### 3.4 D4：可观测快照（零依赖）

```python
def metrics(self) -> dict:
    """从 EventLog 派生（单一只读快照，不引入 OTel 依赖）。

    返回 OTel 友好结构：{"counters": {name: int}, "gauges": {name: int}}
    """
    # counters: 各 EventKind 计数；gauges: 非终态 ACB 数 / 暂停 run 数 / DLQ 数
```

- **不装 OTel**：`dependencies = []` 是硬约束；导出 OTel 友好结构，让使用者自己接
- **只读**：不改任何状态，可随时调用

---

## 4. 测试

| # | 用例 | 判据 |
|---|---|---|
| C-1 | Protocol 完整性 | `CheckpointStore` 声明 ⊇ Harness 调用集（由 C-4 保证） |
| C-2 | `isinstance` 结构校验 | 内存/文件 store 都是 `CheckpointStore` 实例 |
| C-3 | 后端一致性（CheckpointStore） | 5 组断言在 memory/file **结果完全相同** |
| C-3b | 后端一致性（EventLog） | 4 组断言在 memory/WAL 相同 |
| **C-4** | **反漂移** | `ast` 扫出的 `self.<comp>.<attr>` 全部在 Protocol 中；**负对照**：故意去掉一个声明 → 必须红 |
| D-1 | Ingress 同 key 同指纹 | 第二次 `replayed=True`、**不新建 Run**（ACB 数不变） |
| D-1b | Ingress 同 key 异指纹 | `CONFLICT`，不新建 |
| D-1c | Ingress 空 key | `INVALID_INPUT`（IETF 要求） |
| D-1d | Ingress 跨进程幂等 | 事件重放后仍能识别（不依赖内存 dict） |
| D-2 | DLQ redrive | `DEAD_LETTER` → 新 ACB 可执行；**负对照**：超 `max_redrives` 被拒 |
| D-2b | redrive 非 DLQ 状态 | `INVALID_INPUT`（不误用） |
| D-3 | pause 阻止调度 | pause 后 `_drain_scheduler` 不跑该 run；**资源保留**（槽位/预算/节点不变） |
| D-3b | resume 恢复 | resume 后正常调度 |
| D-3c | pause 持久化 | 重启（recover）后仍暂停 |
| D-4 | metrics | 计数与实际事件数一致（**负对照**：多写一条事件 → 计数 +1） |

---

## 5. 迁移清单

| 文件 | 变更 |
|---|---|
| `harness/protocols.py` | 补全 `CheckpointStore`/`EventLog`/`BudgetManager` 等契约；加 `@runtime_checkable` |
| `harness/ingress.py`（新） | `Ingress` + `IngressResult` |
| `harness/admin.py`（新） | `pause_run`/`resume_run`/`is_paused`/`redrive_dead_letter` |
| `harness/harness.py` | 接 `Ingress`（或 `submit_command` 转发）；`_drain_scheduler` 尊重 pause；`metrics()`；`recover` 重放 pause 事件 |
| `harness/types.py` | 新增 `EventKind.INGRESS_ACCEPTED` / `RUN_PAUSED` / `RUN_RESUMED` / `DLQ_REDRIVEN`；`ACB.redrive_count` |
| `harness/checkpoint.py` | payload 增 `redrive_count` |
| `tests/` | `test_s15_backend_conformance.py`、`test_s15_contract_drift.py`、`test_s15_ingress.py`、`test_s15_admin.py` |

---

## 6. 风险与对策

| 风险 | 对策 |
|---|---|
| C4 反漂移误报（属性访问/测试 setter） | 白名单 + 每条注明理由；只检 `self.<comp>.<attr>` 形式（非调用也算访问） |
| C3 套件暴露真不一致 → 需修实现 | **这正是价值**；若发现，修实现而非放宽断言 |
| pause 破坏 `max_concurrent`（槽位不释放但也不执行） | 明确语义：pause 保留槽位（可恢复优先）；文档写明「大量 pause 会占用并发额度」 |
| Ingress 指纹不稳定（dict 顺序） | `sort_keys=True` 规范化；测试覆盖字段顺序不同但内容相同 |
| redrive 造成状态机暗门 | 不改 `DEAD_LETTER` 终态；redrive 走 `requeue_readmit` 造**新** ACB |
| 新增 4 个 EventKind 影响现有测试 | 只增不改；`metrics` 计数测试独立 |

---

## 7. 决策

| # | 结论 |
|---|---|
| Q1 | Protocol 补全以「Harness 实际调用」为准（C4 静态检查反向驱动） |
| Q2 | `@runtime_checkable` 只作第一道筛，一致性靠 C3 实跑 + C4 静态 |
| Q3 | Ingress 幂等用 IETF 语义（同 key 异指纹 → `CONFLICT`），持久化在 EventLog |
| Q4 | pause 保留资源、只阻止未开始任务；`DEAD_LETTER` 仍为终态，redrive 造新 ACB |
| Q5 | 可观测只做 `metrics()` 快照，不引 OTel（保零依赖） |
| Q6 | MS9 真 Raft **不在本波**（`pysyncobj` 破坏零依赖；C 完成后另议） |

---

## 8. 实现结果（v1.0 → v1.1）

### 8.1 大类 C（可插拔契约，MS9 前置）

| 项 | 实现 | 用例 |
|---|---|---|
| **C1** | 契约以「Harness 实际访问」为准补全：`CheckpointStore` 从 **2 个方法 → 9 个**；`EventLog` 补 `all`/`notify_events`；`BudgetManager` 补 12 个（含 `_subtree_nodes`/`check_reservations`/`apply_budget_event`/`suppressed` 等）；`IntentRegistry` 补 `declare_logical`/`undeclare_logical`/`renew_for`/`_active`/epoch 三件套；`Workspace` 补 8 个提交内部接缝；`PolicyEngine` 补 `token`/`resolve_token`/`allows_network`/`allows_data_class`；`EffectJournal` 补 `get`/`probe_one`/`rebuild_from`/`pending` | 由 C4 锁定 |
| **C2** | 9 个 Protocol 全部标 `@runtime_checkable`；内存/文件 store、内存/WAL log 均结构校验通过 | C-2 |
| **C3** | 后端一致性套件：`CheckpointStore` 7 条 × 2 后端、`EventLog` 5 条 × 2 后端，**同一断言两套后端** | **27 passed** |
| **C4** | **反漂移静态检查**（`ast` 扫 `self.<comp>` 与 `self.h.<comp>` 的读取/调用，排除赋值注入端口） | 5 passed（含自检 + 负对照） |

**C4 的收敛过程（真实数据）**：首次运行扫出 **43 个未声明成员**（7 个组件全漂移）→ 第一轮补全后 **8 个** → 扩展扫描到 `self.h.<comp>` 后暴露 `declare_logical`/`renew_for`/`check_reservations` 等 → 归零。

> 这证明「契约存在 ≠ 契约正确」：9 个 Protocol 在仓库里躺了很久，但**没有任何测试引用**，所以漂移无人发现。

### 8.2 大类 D（生产化最小集）

| 项 | 实现 | 用例 |
|---|---|---|
| **D1** | `harness/ingress.py`：`Ingress.submit/lookup` + `fingerprint`（`sort_keys` 稳定）。空 key → `INVALID_INPUT`；同 key 同指纹 → `replayed=True` **不新建**；同 key 异指纹 → `CONFLICT`（IETF 409）；指纹写 `INGRESS_ACCEPTED` 事件 → **跨进程仍生效** | 7 passed（含「内存态去重会重复受理」负对照） |
| **D2** | `Harness.redrive_dead_letter`：仅 `DEAD_LETTER` 可重放；`redrive_count >= max_redrives` → 拒绝；走 `settle`/`remove_node`/`archive_node` 清理序列；写 `DLQ_REDRIVEN` 审计；**旧 ACB 保持终态**（不改写历史结论） | 5 passed（含**真执行**验证） |
| **D3** | `pause_run`/`resume_run`/`is_paused`/`paused_runs`；`_drain_skip_reason` 加 `run_paused`；`RUN_PAUSED`/`RUN_RESUMED` 事件 → `recover` 重放；`submit_command` 路由且**幂等** | 7 passed（含重启后仍暂停） |
| **D4** | `Harness.metrics()`：`{"counters": 各 EventKind 计数, "gauges": 非终态/暂停/死信/等待审批/预算占用/日志尾}`，只读、零依赖 | 4 passed |

### 8.3 门禁

| 门禁 | 结果 |
|---|---|
| pytest 全量 | **546 passed + 4 xfailed**（连跑 5 轮全绿；基线 494 → 546，+52） |
| 覆盖率 | **通过**（`check_coverage.py` exit 0） |
| S14 一致性核对 | 45/45（未回归） |
| **S15 一致性核对** | **73/73**（`scripts/check_s15_conformance.py` exit 0） |
| bench | `--n 30` exit 0；`report_check` exit 0 |

### 8.4 自评审发现并修复的 2 个真问题

| # | 问题 | 修法 |
|---|---|---|
| **R1** | **Admin 命令不幂等**：`submit_command` 用 `pause_run()` 的「是否发生状态变化」决定 `Status`，导致**重复 pause 报 `INVALID_INPUT`**（运维重试会看到错误） | 改为「目标态已达成 → `OK`」，只有缺参才 `INVALID_INPUT` |
| **R2** | **redrive 出的 ACB 只断言 `ADMITTED`，没验证真能执行** —— 这正是 S13b 那类「token 身份错配 → 100% `DENIED_CAPABILITY`」的藏身处 | 新增 `test_d2_redrive_acb_can_actually_run`：redrive 后真跑一个 decision 到 `DONE` |

### 8.5 与方案的偏差（如实记录）

| # | 方案 | 实际 | 理由 |
|---|---|---|---|
| 1 | 新建 `harness/admin.py` | **未创建**，方法直接在 `Harness` 上 | 额外一层 facade 无行为增益；`Ingress` 有独立结果类型才值得单独成模块 |
| 2 | S14 §0.3 称「无 Protocol 契约（grep 零命中）」 | **是错的**：9 个 Protocol 早已存在 | 我上一轮的 grep 正则 `class \w+(Protocol\|ABC)` 漏了 `class X(Protocol)` 形式 → 已在 §0.1 更正 |
| 3 | `DLQ_REDRIVED` | `DLQ_REDRIVEN` | 命名统一（核对脚本曾因拼写不一致误报，已修正） |

### 8.6 新增已知边界（v1.1）

| # | 边界 |
|---|---|
| K-76 | `metrics()` / `Ingress.lookup()` 每次**全量扫事件**（O(N)）；低频调用可接受，事件量大时需增量索引 |
| K-77 | `pause` **保留**槽位（可恢复优先）→ 大量 paused run 会占用 `max_concurrent` 额度 |
| K-78 | `@runtime_checkable` 只查**成员存在性**，不查签名/默认值（CPython 限制）→ 签名一致性靠 C3 实跑兜底 |
| K-79 | MS9 真 Raft **未做**：`pysyncobj` 破坏 `dependencies = []`；本体已具备「可注入 + 契约完整 + 双后端一致」，接 Raft 只是新增一个后端实现 |
| K-80 | `Ingress` 只建 Run（写 `plans` + 事件），**不负责**准入/调度；准入仍需 `Dispatcher.try_admit` |
