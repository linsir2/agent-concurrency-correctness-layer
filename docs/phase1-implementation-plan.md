# 第一阶段实现方案（Phase 1 Implementation Plan）

| 项目 | 内容 |
|---|---|
| 版本 | **v1.1（已按三路审查修订）** |
| 日期 | 2026-10-26 |
| 上游 | [设计规范 v1.12](multi-agent-harness-design-final.md) · [里程碑 v1.1](milestones.md) · [用例集](use-cases-and-adversarial-tests.md) |
| 定位 | 阶段一 MVP：**一个能跑通「3 agent 并行改同一模块」的库 + 一个基准** |
| 决策前提 | [milestones.md §六 D1–D16](milestones.md) 全部按**推荐值**执行 |
| 运行时依赖 | **零第三方依赖**（stdlib only） |
| 测试期依赖 | `pytest`、`pytest-cov`、`hypothesis`（仅 dev） |
| Python | ≥ 3.11 |
| 修订 | v1.0 → v1.1：修 3 个 P0 + 14 个 P1（逐条见 §12） |

---

## 1. 范围与裁决

### 1.1 第一阶段 = 哪几个里程碑

**In scope**：

| 里程碑 | 内容 | 为什么必须在阶段一 |
|---|---|---|
| **MS0** | `types`（schema 定稿）+ `event_log`（WAL）+ `storage/`（blob + 原子写） | 提交点定错后面全返工 |
| **MS1** | `intent_registry` + `workspace` + `rebase` + `runtime`（最小循环）+ 补偿 | 并发正确性内核 |
| **MS2** | `budget`（嵌套守恒）+ `dispatcher`（准入） | 「3 agent 同时改同一模块」的第一道闸 |
| **MS3-lite** | `effect_journal`（幂等 + UNKNOWN 对账）+ `checkpoint`（最小快照）+ `recover` | MVP 明确要求「幂等」 |
| **E2E** | 3 agent 并行改同一模块的 demo（**必须经 `run_task`**） | MVP 的定义就是它 |
| **Bench** | B1/B2/B3 + C0–C3 冲突负载 | 没有基准无法证明「比 worktree 好」 |

**In scope（但归为「补强」而非新里程碑）**：I8（事件原子）、I13（收据产生）、I14（无进展终止）、I11（cursor floor）。这四项是**内核正确性**，不补则阶段一的承诺不成立；它们不是 MS8 的 conformance suite（那属阶段二）。

**Out of scope（明确不做 + 理由）**：

| 不做 | 理由 | 归属 |
|---|---|---|
| `scheduler` / `reconcile` / `hitl` | 阶段一 demo 用**固定 DAG**，无重规划、无审批 | MS4 |
| `RunCoordinator` / `Command` | 阶段一没有并发写 Run 状态的第二个写者 | MS4 |
| 完整 `context_view`（重放） | MVP 只要求「物化快照 + 版本钉住」 | MS6 |
| `policy` 的 network/data_class **强制** | 字段建好，阶段一只校验 tool/path | MS7 |
| WAL 段轮转与删除 | 删除需要「订阅者 cursor 持久化」，属 MS6 | MS6 |
| LangGraph adapter / MCP server | **范围修订 R1**：适配器是分发渠道，不改变正确性论证，且引入宿主依赖拖慢内核验证 | 阶段二 |
| 分布式（Raft） | 阶段一是单机单调度器 | MS9 |

> **R1** 是唯一需要你确认的范围变更（定位文档 §6.4 的 MVP 表把 adapter 列为阶段一）。

### 1.2 与上游文档的 3 处口径修正

| 上游原话 | 本方案 | 依据 |
|---|---|---|
| 设计 §5.6 / D1：**CheckpointStore 是唯一恢复依据**，EventLog 是侧通道 | 采用上游口径；**v1.0 的「事件是唯一真相源」是错的，已删** | 审查 9.1 |
| 里程碑 MS2 = 嵌套预算；定位 MVP 表写「单层」 | **实现嵌套**（MVP 表是下限，代码已实现嵌套且 A4/A12 依赖它） | 降级是倒退 |
| 里程碑 MS3 = 完整崩溃恢复 | **只做 EffectJournal + 最小 checkpoint + recover**；HITL/编排相关恢复归 MS4/MS6 | 阶段一无处挂载 |

### 1.3 任务级 vs Run 级状态机（v1.1 修正）

设计 §5.9 有**两张表**：表 A（Task 级）与表 B（Run 级）。v1.0 把表 B 的 `DONE_PARTIAL` 塞进 `TERMINAL_STATES` 是错的。修正：

```python
class TaskState(Enum):   # 表 A，阶段一唯一使用
    PLANNED, ADMITTED, INSTANTIATING, READY, RUNNING, WAITING_APPROVAL
    REBASING, RETRYING, QUARANTINED, INADMISSIBLE, REPLANNING
    DONE, FAILED, CANCELLED, DEAD_LETTER

TASK_TERMINAL = {DONE, FAILED, CANCELLED, DEAD_LETTER}          # 不含 DONE_PARTIAL

class RunState(Enum):    # 表 B，阶段一只定义不使用（MS4 启用）
    NEW, PLANNING, RUNNING, DONE, DONE_PARTIAL, FAILED, CANCELLED

RUN_TERMINAL = {DONE, DONE_PARTIAL, FAILED, CANCELLED}
```

---

## 2. 现状、破坏性变更与迁移清单

### 2.1 必须重构的 8 处

| 现状 | 问题 | 目标 |
|---|---|---|
| 具体类直接实例化 | MS9 无法替换；测试无法注入 fake | `Protocol` + 构造注入 + 默认实现 |
| `Event` 缺 `event_id/run_id/ts/schema_version` **4 个**字段 | I6/I8 不可验证；跨进程恢复无法对齐 | 按 §4.5 定稿（§4.1） |
| `EventLog` 纯内存 | 「WAL」是假的 | blob + WAL 两段式提交（§5.1） |
| 单把全局 `RLock` | 与「按资源短临界区」不符 | 每资源锁 + 全局锁序（§6） |
| `base_version` 在 apply 时才写 | OCC 退化为「与当前版本比」 | `stage` 时同时写 `read_set` 与 `base_version` |
| 新建资源无前像 → 不可补偿 | 创建型写必进 PIVOT | `Delete` 哨兵（§4.6） |
| `compensation_journal` 是内存 list | 崩溃后补偿丢失 | 持久化 + 补偿幂等键（§4.8） |
| `Harness(policy_default_allow=True)` | I4 默认不成立 | deny-by-default + 夹具签发通配 token |

### 2.2 破坏性接口变更（**必须先做，否则 S0 无法开工**）

| 接口 | 变更 | 受影响调用点（全部要改） |
|---|---|---|
| `EventLog.append/append_in_txn` | 关键字必填 `run_id`；返回 `Event`（不再是 `seq`） | `runtime.py`、`harness.py`、`orchestration.py`、`checkpoint.py`、`test_ms0_event_log.py` |
| `Status` → `Result` | 提交类接口返回 `Result(status, detail)`，因为 `CONFLICT` 必须带 `diff`、`REJECT_STALE_READ` 必须带 `resource` | 全部 `commit_batch`/`apply`/`prepare` 调用点 |
| `IntentRegistry.reserve` | `reserve(resource, mode: "R"\|"W", base_version, ttl_ms, agent_id, epoch)`；`Lease` 增加 `mode/expires_at`，删 `valid/ttl_ms` | `harness.py`、`workspace.py`、`test_adversarial.py` |
| `IntentRegistry.compatible` | `compatible(resource, intents: list[Lease], deps: set[str]) -> bool` | `dispatcher.try_admit` |
| `CapabilityToken` | 四维 `(tools, paths, network, data_class)`，支持 `"*"`；`agent_id` 移到 `issue()` | `policy.py`、`test_ms7_capability.py` |
| `BudgetManager.reserve/release` | `reserve(node, amount) -> Reservation\|None`；`release(reservation_id)` 幂等 | `budget.py`、`dispatcher`、`harness.py` |
| `CheckpointStore.load` | 返回 `ACB`（不是 dict）；字段名 `effect_journal_offset` | `harness.recover` |
| `AgentRuntime.run` | 签名 `run(acb) -> Result`；决策由注入的 `decide` 提供 | `harness.run_task`、全部用例测试 |
| `Planner.plan` | `logical_subgoal_id` **必填**，缺失 → `INVALID_INPUT`（删 `"S-"+task_id` 回退） | `conftest`、`test_real_cases.py`、`test_adversarial.py` |

**兼容垫片**：`Result.__eq__` 接受 `Status`（`result == Status.OK` 仍成立），S8 后移除。这样迁移可以**分批**，不会一次性红一片。

### 2.3 迁移策略

- **S0 一次性完成** §2.2 全部签名变更 + 调用点迁移，判据 = **现有 35 测试全绿**。
- 迁移期间禁止改行为（只改签名/结构），行为改动一律留到 S1–S8。这样出问题能立刻定位是「迁移错」还是「行为错」。

---

## 3. 目标架构

### 3.1 目录

```
harness/
  types.py            # 纯数据 + TaskState/RunState + TRANSITIONS
  protocols.py        # 全部 Protocol（D2）
  clock.py            # SystemClock / ManualClock（可注入）
  storage/
    blob.py           # 内容寻址存储（sha256 → bytes），原子写 + fsync
    wal.py            # 帧格式 + 短写循环 + 恢复校验 + 段管理
    atomic.py         # atomic_write / atomic_delete / fsync_dir
  event_log.py        # WalEventLog（默认）/ InMemoryEventLog
  intent_registry.py  # LeaseRegistry
  workspace.py        # WorkspaceManager（manifest + blob + 每资源锁）
  rebase.py           # JSON-aware 3-way merge
  budget.py           # BudgetManager
  checkpoint.py       # CheckpointStore / EffectJournal / CompensationJournal
  policy.py           # PolicyEngine + CapabilityToken
  context.py          # SnapshotAssembler（cursor + retention floor）
  subscription.py     # SubscriptionRegistry（阶段一：注册 + 有序分发，无背压）
  runtime.py          # AgentRuntime + handle_commit_result
  orchestration.py    # FixedDagPlanner + Dispatcher
  harness.py          # 门面
  spec/               # L2 规范四件套（沿用）
```

### 3.2 注入图（D2）

```python
Harness(
    clock: Clock,                      # 默认 SystemClock
    event_log: EventLog,               # 默认 WalEventLog(path)
    blobs: BlobStore,                  # 默认 FileBlobStore(root)
    intents: IntentRegistry,           # 默认 LeaseRegistry(clock)
    workspace: WorkspaceManager,       # 默认 WorkspaceManager(event_log, blobs, intents)
    budget: BudgetManager,
    checkpoints: CheckpointStore,
    effects: EffectJournal,            # 依赖 event_log + probe
    compensation: CompensationJournal, # 依赖 wal
    policy: PolicyEngine,
    context: ContextAssembler,
    subscriptions: SubscriptionRegistry,
    decide: Callable[[ACB, dict], Decision],   # 阶段一 = scripted fake
    *constants,
)
```

**规则**：
1. 每个组件一个 Protocol（`protocols.py`），方法体 `...`。
2. **`Harness()` 无参可用**：默认 `InMemory*` 实现 + `SystemClock` + `policy_default_allow=False` + **`default_token` 通配**（否则现有测试全被拒）。
3. 只注入要观测/替换的那一个。
4. 装配顺序：`clock → storage → event_log → intents → workspace → budget → checkpoint/effects/compensation → policy → context/subscription → planner/dispatcher → runtime`。

> **v1.0 缺陷修正**：v1.0 的注入图漏了 `context/subscription/rebase/planner/dispatcher/decide`，且「WAL 默认 + deny-by-default」与「保持 35 测试兼容」互相矛盾。现规定：**默认实现是内存版**，`WalEventLog` 只在显式传 `wal_path` 时启用；deny-by-default 由 `default_token` 兜底兼容。

### 3.3 并发模型：threading

| 理由 | 说明 |
|---|---|
| 临界区必须真互斥 | `commit_batch` 是同步短临界区，`threading.Lock` 语义直给；asyncio 会掩盖真实竞争 |
| 测试要能造真并发 | `Barrier` + `ThreadPoolExecutor` 可制造 8 路同时提交；asyncio 下退化为顺序执行 |
| 本层不做长 I/O | LLM 调用在 `decide` 里（阶段一 scripted），本层只做协调 |
| 可演进 | 未来 `AgentRuntime` 接真模型时可在其外层叠 asyncio，本层不变 |

**代价**：GIL 下 CPU 不并行。所以基准必须注入受控 I/O 等待（§9.2），否则加速比恒 ≈1（审查 7.1）。

---

## 4. 逐模块设计

### 4.1 `types.py` —— schema 定稿（D6）

```python
@dataclass(frozen=True, slots=True)
class Event:
    seq: int; event_id: str; run_id: str; ts: int
    kind: EventKind; subject: str; payload: dict
    producer: str = ""; caused_by: str = ""; idempotency_key: str = ""
    schema_version: int = 1

@dataclass(frozen=True)
class Result:
    status: Status
    detail: dict = field(default_factory=dict)   # {resource, version, diff, reason}
    def __eq__(self, other): ...                 # 兼容 `== Status.OK`
```

**payload 契约**（决定 I6/I8 能否验证）：

| kind | payload |
|---|---|
| `RESOURCE_COMMITTED` | `{resource, version, blob_hash, prev_hash, diff}` |
| `COMPENSATION_*` | `{comp_id, resource, target_hash, mode}` |
| `EFFECT_*` | `{effect_id, tool, args_hash, state}` |
| `BUDGET_*` | `{node, amount, consumed, reserved}` |
| `TASK_COMPLETED` | `{task_id, status, result_ref}` |

**不放全文**：事件只带 `blob_hash + diff`，全文在 blob store。这样 `retention` 才有意义（I11）。

**状态机表驱动**（D10）：`TRANSITIONS: dict[tuple[TaskState, str], TaskState]`，逐行抄设计 §5.9 表 A；`advance(acb, event)` 是**唯一**改状态的入口；非法转移返回 `Result(INVALID_INPUT, {"from":..., "event":...})`（不新增错误码，复用 §4.4 封闭枚举）。

### 4.2 `storage/atomic.py` + `storage/blob.py`

```python
def atomic_write(path, data: bytes) -> None
    # 同目录 mkstemp → write_all → flush → fsync(fd) → os.replace → fsync(dir)
def atomic_delete(path) -> None      # unlink → fsync(dir)
def write_all(fd, data) -> None      # 循环写，处理短写与 EINTR
```

`BlobStore`：`put(bytes) -> sha256`（内容寻址，写 `blobs/aa/bb/<hash>`，已存在则跳过）、`get(hash) -> bytes`、`link(hash, target_path)`。

**序列化**：`canonical_bytes(obj)` = `json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()`；非 JSON 类型（bytes）直接存原始字节。**`content_hash` 一律是 blob hash**（不再是 `repr(content)`，审查 48）。

### 4.3 `storage/wal.py`

```
帧 = [magic:u32 "MWAL"][len:u32][crc32:u32][payload json]
payload 必含：{seq, event_id, ...}      ← 必须入帧（审查 41）
```

| 规则 | 原因 |
|---|---|
| 写：`O_APPEND` + `write_all` 循环写 + `fdatasync` | 短写会在文件中段产生坏帧（审查 2.2） |
| 首次创建 / 轮转新建段后 `fsync` 父目录 | 否则掉电后整文件消失（审查 2.5） |
| 恢复：逐帧校验 `magic`/`len`/`crc32`/`seq == prev+1`/`event_id` 唯一 | 只验 CRC 会把损坏 payload 当有效事件（审查 5.1） |
| 恢复：坏帧 → 记录 `truncated_at`，**只允许最后一段的尾部**截断；中段坏帧 → `COMMIT_UNRECOVERABLE` | 「遇坏帧即停」会丢后续有效事件（审查 5.2） |
| **阶段一不轮转**（单段无上限） | 段删除需要订阅者 cursor 持久化（MS6）；不删就不会丢（审查 2.4） |
| `tail` = 最后一个**完整**帧的 seq | 否则 `replay` 越界（审查 5.4） |

### 4.4 `event_log.py`

```python
class EventLog(Protocol):
    def append(self, *, run_id, kind, subject, payload, producer="", caused_by="", idem="") -> Event: ...
    def append_batch(self, events: list[EventDraft]) -> list[Event]: ...   # 一次 fsync，多资源原子提交
    def replay(self, from_seq: int = 0, subject: str | None = None) -> list[Event]: ...
    def has(self, subject, kind) -> bool: ...
    @property
    def tail(self) -> int: ...
    def subscribe(self, fn) -> None: ...
```

- `append` 与 `append_batch` **共用同一把 WAL 锁与同一个序列器**（审查 2.3）。
- **通知乱序修复（P2-7）**：锁内分配 seq 并入**无界队列**，锁外单分发线程按 seq 调用订阅者。队列无界 → 不会在持锁时阻塞（审查 6.3）。
- **禁止**在订阅者回调里调用 `event_log.append/replay`（会造成分发线程等 WAL 锁）；违反时 `subscribe` 包装器检测到重入直接抛错。

### 4.5 `intent_registry.py`

```python
class IntentRegistry(Protocol):
    def reserve(self, resource, mode: Literal["R","W"], base_version, ttl_ms,
                agent_id, epoch) -> Lease | None: ...
    def is_valid(self, lease: Lease) -> bool: ...        # 必须按 expires_at + 注入时钟判定
    def revoke(self, lease) -> None: ...
    def renew(self, lease, epoch) -> bool: ...
    def compatible(self, resource, intents: list[Lease], deps: set[str]) -> bool: ...
```

- `Lease = {lease_id, resource, mode, agent_id, epoch, expires_at}`；`expires_at` 用**注入时钟**（`ManualClock` 精确推进，A9 不用 sleep）。
- `renew` 只对当前 `epoch` 生效；`revoke` 后不可复活（D9）。
- `compatible` 判据严格对齐设计 §7.4：① 契约/不变量冲突（`deps` 相交）→ 拒绝；② 读-写方向冲突 → 拒绝；③ 写-写同资源 → **放行**（留提交层 CAS）。
- `reserve` 返回 `None` 时，调用方必须处理（不得直接 `is_valid(None)`，审查 26）。

### 4.6 `workspace.py` + `rebase.py`

**锁**（D4，v1.1 修正）：

```python
LOCK_KEY = lambda w: w.resource           # 动态全序，不是预存列表
# 批量提交：一次性按 LOCK_KEY 排序后 acquire 全部资源锁，期间完成全部 prepare+apply，最后统一释放
```

> v1.0 写 `LOCK_ORDER = sorted()`（把内建函数当常量）且同时说「禁止嵌套持锁」，自相矛盾。现统一为：**一次性按序持有全部资源锁**（all-or-nothing），**允许**同时持多把，但**必须**按 `LOCK_KEY` 升序获取。

**读写模型**（v1.1 新增，解决 I8）：

- `manifest: {resource: (version, blob_hash)}` —— **读路径只看 manifest**，不看磁盘文件。
- 磁盘文件是「物化视图」，由 `link(blob_hash, path)` 生成，**可随时从 manifest + blob 重建**。
- 因此「提交点 = WAL 追加成功」，文件是否已 link 不影响读一致性。

**`stage`（修 H8）**：

```python
def stage(self, acb, resource, content):
    ver = self.version(resource)
    acb.read_set[resource] = ver
    acb.base_version[resource] = ver          # == read_set 快照（§6.1 恒等）
    acb.staged[resource] = content
    if ver > 0: acb.pre_image[resource] = self.manifest[resource][1]   # 前像 = blob hash
```

**`compute_reverse_patch`**：

- 有前像 → 返回前像 blob hash；
- **无前像（create）→ 返回 `Delete` 哨兵** → 补偿时 `atomic_delete` + manifest 移除。
- **v1.0 矛盾修正**：§2.1 原写「创建型写必进 PIVOT」，与 Delete 哨兵冲突。现明确：**create 型写可补偿（删除）**；只有「外部副作用」与「前像被淘汰」才 PIVOT。

**`rebase.py`（对齐设计 §7.2.1）**：

```python
def merge3(base, local, committed, policy) -> MergeResult
# MergeResult = {merged: dict | None, conflicts: [{path, kind, local, committed}]}
# kind ∈ add_add | modify_modify | delete_modify | type_change
```

| 类型 | 默认策略 |
|---|---|
| object | 递归 |
| scalar | 两方都改且不同 → `modify_modify` 冲突 |
| array | **`GROW_ONLY`（默认，设计 §7.2.1）**：并集保序；`REPLACE`：取 `committed`；`PIVOT`：升级 |

> **v1.0 错误修正**：v1.0 把数组默认设为 `fail_closed`、策略词表用 `fail_closed|by_id|union`，与设计 `GROW_ONLY|REPLACE|PIVOT` 不符，会导致 U3（tsconfig 场景）误报冲突（审查 D3）。
> **命名**：代码里用 `local`（本 agent staged）与 `committed`（已提交当前值），**不用** `ours/theirs`（与 git 相反，易错位）。

### 4.7 `budget.py`

- 节点 `{limits, own_reserved, consumed, children}`；`reserve` 返回 `Reservation{id, node, amount}`。
- 判据：`consumed + Σchildren.reserved + own_reserved + amount ≤ limits`。
- `release(reservation_id)` **幂等**（重复 no-op）；`cancel_run`/补偿/终态统一走它（D12）。
- 单写者：内部 `_lock`；**`budget` 锁是叶子锁**（持有期间不得再取任何锁）。

### 4.8 `checkpoint.py`

**`EffectJournal`**（D8 + 审查 4.1/4.2）：

```python
idem_key = sha256(run_id ‖ logical_subgoal_id ‖ sha256(tool ‖ canonical_json(args)))
```

- `canonical_json` **必须**是 `json.dumps(sort_keys=True, separators=(",",":"), ensure_ascii=False)`（审查 4.1：`repr(dict)` 键序敏感会让同一动作产生不同 key）。
- 状态 `INTENT → EXECUTING → CONFIRMED | UNKNOWN`，每次转移写 WAL。
- **去重语义（审查 4.2，P0）**：

| 已存在状态 | 处理 |
|---|---|
| `CONFIRMED` | 直接返回既有结果（唯一可短路的状态） |
| `INTENT` / `EXECUTING` | 必须 `probe(idem_key)`：`True`→`CONFIRMED`；`False`→用同一 key 重执行；`None`→人工队列 |
| `UNKNOWN` | 同上，**禁止**不 probe 就 `CONFIRMED` |

**`CompensationJournal`**（审查 3.1/3.2，P0）：

```python
entry = {comp_id, acb_id, resource, applied_hash, reverse_hash, state}
comp_id = sha256(run_id ‖ acb_id ‖ resource ‖ version_before ‖ applied_hash)
```

- 持久化复用 `storage/wal.py`（append-only + CRC + fsync）。
- **恢复判定（幂等，解决误判 PIVOT）**：

| 当前内容 hash | 结论 |
|---|---|
| `== reverse_hash` | 已补偿完成 → 置 `COMPENSATED`（不重跑） |
| `== applied_hash` | 未补偿 → 重跑 reverse |
| 其他 | 期间被他人改 → `PIVOT` |

- **批量意图先落盘（审查 3.2）**：`compensate` 在**释放租约之前**先写一条 `COMPENSATING_BATCH{entries:[...]}`，再逐条 `COMPENSATING → COMPENSATED/FAILED/PIVOT`。否则「释放租约后、写第一条日志前崩溃」会整体丢补偿意图。
- `applied` 清单必须可从 `COMPENSATING_BATCH` 重建（不依赖内存）。

**`CheckpointStore`**：`save(acb) -> ref`、`load(ref) -> ACB`。字段 = 设计 §4.4 全集：`turn_seq / staged / manifest_versions / effect_journal_offset / cursor / aging_deadline / hitl_result`（v1.0 漏了 `cursor` 与 `aging_deadline`，审查 A6）。

### 4.9 `policy.py`

- `CapabilityToken(tools, paths, network, data_class)`，四维全给默认值 `{"*"}`，`allows_*` 支持 `"*"`。
- `PolicyEngine(default_allow=False)`；`Harness` 默认 `policy_default_allow=False` + `default_token`（通配）保证现有测试兼容。
- 阶段一只**强制** tool/path；network/data_class 存字段、不校验（MS7 启用）。
- `test_capability_denied_blocks_tool` 重写：用**不装 `default_token`** 的 Harness，断言未签发 → `False`。

### 4.10 `runtime.py`

```python
def run(self, acb) -> Result:
    while True:
        g = self.guard(acb)                 # 取消/期限/预算/步数/无进展/续租
        if g != Status.OK: return self.finish(acb, g)
        d = self.decide(acb, self.h.context.snapshot(acb))
        for i, act in enumerate(d.actions):
            st = self.execute(acb, act, d.logical_action_id(i))
            if st.status != Status.OK:
                st = self.handle_commit_result(acb, st)
                if st.status != Status.OK: return st
```

`guard` 组成（对齐设计 §4.4 v1.9）：查取消 → 期限 → 预算 → 步数 → **无进展（I14）** → `intents.renew(acb.leases, acb.generation)`。

**I14 无进展判定（v1.0 缺失，审查 57）**：`no_progress_steps` 的写入点是 `execute` 末尾——若本步所有动作的 `(kind, subject, hash(args))` 与上一步完全相同，则 `+1`，否则归零；`> aging_max` → `FAILED(NO_PROGRESS)`。

`handle_commit_result`：

| 入参 | 处理 |
|---|---|
| `CONFLICT(v, diff)` | `rebase.merge3` → 更新 `staged`/`base_version` → 重试（有界 3） |
| `REJECT_STALE_READ(r)` | **先 rebase 再重读**（设计 §5.4 把两者同归语义 rebase，v1.0 少了 rebase，审查 D1） |
| `REJECT_STALE` | `renew` → 失败则重新 `reserve` |
| 超限 | `QUARANTINED`（有 replan 才 `REPLANNING`；阶段一无 replan，直接 `DEAD_LETTER`） |
| 其余 | 按设计 §5.8 失败分类表映射（表驱动） |

### 4.11 `orchestration.py`

- `FixedDagPlanner.plan(goal, tasks)`：校验无环 + footprint 非空 + 预算合法 + **`logical_subgoal_id` 必填**（缺失 `INVALID_INPUT`）。阶段一**不做 LLM 规划**。
- `Dispatcher.try_admit(task, run_id) -> ACB | Result(INADMISSIBLE)`，顺序：`validate → budget.reserve → intents.compatible → policy.check → 建 ACB`。
- **失败回滚（审查 52）**：任一步失败必须撤销已做的预留——`budget.release(reservation_id)` + `intents.revoke(lease)` + **撤销 `declare_logical` 声明**，否则残留声明会让后续准入被误判冲突。
- `Harness.run_task(acb_or_task)` **必须走 dispatcher**（D16）；`conftest.admit` 改为调用 `run_task`（D16 端到端要求）。

### 4.12 `harness.py`

`run_task` / `commit_batch` / `compensate` / `recover` 四入口。

**明确偏差（审查 F8）**：阶段一没有 `RunCoordinator`，`runtime` 直接 `advance(acb, event)` 写 `acb.state`。这偏离设计 B1「只有 RunCoordinator 能写状态」，**但 `advance` 是唯一入口**，MS4 接入 `RunCoordinator` 时只需把 `advance` 的调用方换成 Command 通道。

---

## 5. 关键时序与崩溃窗口

### 5.1 提交（v1.1 重写，解决 I8）

```
commit_batch(acb, writes, mode):
  resources = sorted({w.resource for w in writes}, key=LOCK_KEY)
  acquire_all(resources)                       # 按序，一次性
  try:
    # ① 校验（全部写；任一失败 → 未写任何东西，无需补偿）
    for w in writes:
        st = validate(acb, w, mode)            # lease / base_version / read_set / (COMPENSATE) hash
        if st != OK: return st
    # ② 内容先落盘（blob，原子写 + fsync）
    for w in writes:
        blob_hash[w] = blobs.put(canonical_bytes(acb.staged[w.resource]))
    # ③ 提交点：一次 append_batch（单帧多写，一次 fdatasync）
    events = [commit_record(w, blob_hash[w], version+1) for w in writes]
    eventlog.append_batch(events)              # ← 返回即已提交、已持久
    # ④ 投影（可重放，失败不回滚决定）
    for w in writes:
        manifest[w.resource] = (version+1, blob_hash[w])
        blobs.link(blob_hash[w], path_of(w.resource))
    return OK
  finally:
    release_all(reversed(resources))
```

**崩溃窗口分析**：

| 崩溃点 | 结果 | 恢复动作 |
|---|---|---|
| ② 之前 | 什么都没写 | 无 |
| ② 中（blob 写一半） | 临时文件残留，manifest 未变 | 清理 `.tmp` |
| ② 之后、③ 之前 | blob 已存但未提交 | 孤儿 blob（GC 清理），语义上未提交 |
| **③ 中（WAL 半帧）** | 尾部截断 → 视为未提交 | 截断到上一完整帧 |
| **③ 之后、④ 之前** | **WAL 已提交，manifest 未更新** | **重放 WAL 重建 manifest（redo）** |
| ④ 中 | manifest 部分更新 | 同上，重放 WAL 全量覆盖 |

> 因为**读路径只看 manifest + blob**，且 manifest 由 WAL 重放重建，所以「资源已替换但事件未写」或反之**都不可能发生**。这是 I8 的真正落地方式（v1.0 的「atomic_write 后 append」做不到）。

### 5.2 冲突 → rebase

```
commit_batch → CONFLICT(v2, diff)   （此时未写任何东西，无需补偿）
  → handle_commit_result → merge3(base=前像, local=staged, committed=当前)
  → 无冲突: 更新 staged + base_version=v2 → 重试（有界 3）
  → 有冲突: 交 decide() 重编辑（阶段一 scripted 直接返回冲突报告 → QUARANTINED）
```

### 5.3 补偿（含崩溃恢复）

```
compensate(acb, applied):
  entries = [{comp_id, resource, applied_hash, reverse_hash} for w in reversed(applied)]
  compensation.append_batch(COMPENSATING_BATCH, entries)     # ① 先落盘全部意图
  release_leases(acb.leases)                                 # ② 再释放租约
  for e in entries:
      compensation.write(COMPENSATING, e)                    # ③ 逐条标记
      st = commit_batch(acb, [e], mode=COMPENSATE)
      compensation.write(COMPENSATED | FAILED | PIVOT, e)

recover():
  for e in compensation.pending():          # COMPENSATING / FAILED / PIVOT
      cur = workspace.blob_hash(e.resource)
      if cur == e.reverse_hash:  compensation.write(COMPENSATED, e)   # 幂等完成
      elif cur == e.applied_hash: retry compensate(e)                  # 续做
      else: compensation.write(PIVOT, e)                               # 他人改过
```

> ① 在 ② 之前是关键：否则「释放租约后、写日志前崩溃」会整体丢补偿意图（审查 3.2）。

### 5.4 恢复总流程（对齐设计 §5.8）

```
recover():
  ① wal.replay() → 重建 manifest + seq/tail（坏帧只允许尾部截断）
  ② 定位最近 checkpoint（CheckpointStore 是唯一恢复依据）
  ③ 按 resume_policy: RERUN（turn_seq=0）| RESUME（从 turn_seq 续）| COMPENSATE
  ④ generation += 1
  ⑤ 扫 EffectJournal: CONFIRMED→跳过；INTENT/EXECUTING/UNKNOWN→probe（§4.8）
  ⑤′ 扫 CompensationJournal（§5.3）
  ⑥ budget.release 未归还的 reservation
  ⑦ 重建 ACB 并重新入队
```

**D9 落地**：`Task.resume_policy` 字段必须有，`recover` 按它分支（v1.0 完全漏了，审查 F3）。

---

## 6. 全局锁序

**阶段一只用 threading 锁，必须遵守下表**（违反时 debug 模式抛错）：

```
L0  budget 锁 / intents 锁        ← 叶子锁：持有期间不得再取任何锁
L1  compensation_journal 锁
L2  checkpoint 锁
L3  资源锁（按 LOCK_KEY 升序，可同时持多把）
L4  WAL 锁                        ← 最内层
```

规则：
1. **只能从低层向高层取**（`L0 → L1 → L2 → L3 → L4`），禁止反向。
2. `L0` 是叶子锁：`budget`/`intents` 的方法内部只做本地操作，**不回调**其他组件。
3. `L3` 可持多把，但必须按 `LOCK_KEY` 升序；批量提交一次性取全部。
4. `L4` 最内层：`append_batch` 只在持资源锁后调用，且订阅者回调在锁外执行。
5. `Harness` 的 `plans/acbs` 注册表用独立 `RLock`（`L2.5`），只做字典读写、不跨组件调用。

> v1.0 只写了「资源锁 → WAL 锁」一条，未定义 budget/intents/compensation/checkpoint 的位置，存在 AB-BA 反例（审查 1.3/6.1/6.2）。

---

## 7. 测试计划

### 7.1 配置（新增 `pyproject.toml`）

```toml
[tool.pytest.ini_options]
addopts = "--strict-markers"
markers = ["ms0","ms1","ms2","ms3","conformance","adversarial","slow","serial","fault"]

[tool.coverage.run]
source = ["harness"]; branch = true; omit = ["tests/*"]

[tool.coverage.report]
exclude_lines = ["pragma: no cover", "if TYPE_CHECKING:", "\\.\\.\\.", "raise NotImplementedError"]
```

**覆盖率分模块设阈值**（v1.0 一刀切 80% 对 `wal.py`/`rebase.py` 不可达，审查 8.4）：

| 模块 | 阈值 |
|---|---|
| `workspace` / `budget` / `intent_registry` / `event_log` | ≥ 85% |
| `rebase` / `wal` / `checkpoint` | ≥ 75% |
| `runtime` / `orchestration` | ≥ 70% |
| 全局 | ≥ 80% |

### 7.2 测试层次

| 层 | 手段 | 覆盖 |
|---|---|---|
| 单元 | 纯函数直测 | `merge3`、`canonical_json`、`idem_key`、`TRANSITIONS`、`write_all` |
| 并发 | `ThreadPoolExecutor` + `Barrier(N)`，`@pytest.mark.serial` | I1、A4、A12 |
| 属性 | `hypothesis` `@given` / `RuleBasedStateMachine` | I1、I5、I7 |
| 故障注入 | 子进程 + `SIGKILL`（**固定种子 + 枚举关键窗口**，审查 72） | WAL 截断、补偿中途崩溃 |
| 端到端 | 3 agent 并行改同一模块（**经 `run_task`**） | MVP 主场景 |
| 基准 | §9 | 五指标 |

### 7.3 必须新增/重写的测试（精确清单）

| # | 测试 | 断言（可判定判据） | 步骤 |
|---|---|---|---|
| T1 | `test_ms0_wal_torn_tail_truncated` | 写 10 帧 + 半帧 → 重开 → `tail==10`、`truncated_at` 非空 | S1 |
| T2 | `test_ms0_wal_fsync_durable` | `append` 返回后，**独立进程**读到该帧 | S1 |
| T3 | `test_ms0_wal_seq_continuity` | 人为改坏中段帧 → 恢复抛 `COMMIT_UNRECOVERABLE`（不是静默截断） | S1 |
| T4 | `test_ms1_commit_point_is_wal` | 在 ③ 与 ④ 之间注入崩溃 → 恢复后 manifest 与 WAL 一致 | S1 |
| T5 | `test_ms1_occ_readset_rejects_stale` | 读集合被改 → `REJECT_STALE_READ`，且 `detail.resource` 正确 | S2 |
| T6 | `test_ms1_create_is_compensable` | 新建 → 补偿 → manifest 无该资源、文件被删 | S3 |
| T7 | `test_ms1_lock_order_no_deadlock` | 两 agent 反向顺序提交同一对资源 → 5s 内全部返回（超时即失败） | S2 |
| T8 | `test_ms1_rebase_array_grow_only` | 两方各 append 一项 → 合并后两项都在、无冲突 | S3 |
| T9 | `test_ms2_admit_rollback_on_deny` | 准入被拒后 `budget` 与 `intents` **无残留**（数量断言） | S4 |
| T10 | `test_ms2_run_task_goes_through_dispatcher` | 计数器证明 `try_admit` 被调用且只调一次 | S4 |
| T11 | `test_ms3_effect_unknown_reconcile` | probe=True→CONFIRMED；False→重执行一次；None→人工队列 | S5 |
| T12 | `test_ms3_compensation_crash_recovery` | 反写后、`COMPENSATED` 前崩溃 → 恢复后内容==前像、**无 PIVOT** | S5 |
| T13 | `test_ms3_idem_key_stable_when_task_id_changes` | 固定 `logical_subgoal_id`，改 `task_id` → `idem_key` 不变 | S5 |
| T14 | `test_i4_capability_deny_by_default` | 不装 default_token → `False` | S4 |
| T15 | `test_i8_event_atomic_on_failure` | 注入 `blobs.put` 失败 → 未提交、无事件 | S8 |
| T16 | `test_i11_cursor_floor_forces_snapshot` | cursor < tail-retention → 强制从快照重建 | S8 |
| T17 | `test_i13_receipt_emitted_on_commit` | 每次提交产生收据，hash-chain 可验 | S8 |
| T18 | `test_i14_no_progress_terminates` | 连续 K 步相同动作 → `FAILED(NO_PROGRESS)` | S8 |
| T19 | `test_e2e_three_agents_same_module` | 经 `run_task` 三路并发 → 最终内容包含三者全部意图（按 blob hash 断言），无丢失更新 | S6 |

> **T13 的诚实边界**：阶段一无真实 replan 路径，该测试验证的是**公式稳定性**（固定 `logical_subgoal_id` 时 `task_id` 不影响 key），不是真实 replan 端到端。真实 replan 验证归 MS4（审查 8.3）。

---

## 8. 实施顺序（每步判据可判定）

| Step | 内容 | 完成判据（精确） |
|---|---|---|
| **S0** | `pyproject.toml` + markers + `protocols.py` + `types.py`（Event/Result/TaskState/RunState/TRANSITIONS）+ `clock.py` + **§2.2 全部签名迁移** | ① `pytest --markers` 输出含 `ms0..ms3`；② **现有 35 测试全绿**（迁移不改变行为） |
| **S1** | `storage/atomic.py` + `blob.py` + `wal.py` + `WalEventLog` + `InMemoryEventLog` | T1–T4 绿 |
| **S2** | `intent_registry`（注入时钟）+ `workspace`（manifest/blob/锁序/base_version 恒等） | T5、T7 + `test_a9_lease_renew_revoke_race` 绿 |
| **S3** | `rebase.py` + `handle_commit_result` + `Delete` 哨兵补偿 | T6、T8 + `test_a1_compensation_cross_no_overwrite` 绿 |
| **S4** | `budget`（Reservation + 幂等 release）+ `dispatcher`（回滚）+ `policy`（deny-by-default + default_token） | T9、T10、T14 + `test_a4_budget_double_spend`、`test_a12_concurrent_reserve_no_overcommit`、`test_a13_per_action_guard_budget` 绿 |
| **S5** | `EffectJournal`（probe）+ `CompensationJournal`（落盘 + 幂等键）+ `recover`（resume_policy） | T11、T12、T13 + `test_u4_long_task_crash_recovery_idempotent` 绿 |
| **S6** | `runtime`（表驱动 + guard + 续租）+ `run_task` 走 dispatcher + `snapshot` | T19 + U1×2、U2、U3 绿 |
| **S7** | 基准（B1/B2/B3 + C0–C3）+ 报告 | `bench/report.md` 含 §9.3 **全部 5 项**指标 |
| **S8** | 补强 T15–T18 + 覆盖率达标 | 全部测试绿 + 分模块覆盖率达标（§7.1） |

> 每步结束打 tag `phase1-s0`…`phase1-s8`；任一步不达标回退到上一个 tag，不带着红灯前进。
> **Step ↔ 里程碑映射**：S1→MS0，S2/S3→MS1，S4→MS2，S5→MS3-lite，S6→E2E，S7→Bench。每步判据里列的测试名就是该里程碑的出口条件（满足 D14「出口必须全 ✅」）。

---

## 9. 基准方案（修正 4 处作弊点）

### 9.1 三条件（必须配对到 action 级）

| 条件 | 实现 |
|---|---|
| **B1 串行单 agent** | 同一决策序列顺序执行 |
| **B2 worktree + merge** | 每 agent 一个目录，FIFO 合并；**合并用独立实现**（`git merge-file` 或独立代码路径，**不复用 `rebase.py`**） |
| **B3 共享 + 本层** | `harness/` RSC |

**配对纪律**：三条件必须使用**同一决策序列**（用 `sha256(decisions_json)` 固化并断言相等）、同一初始状态、同一资源集合。否则 B3 可被喂「天然不重叠」的决策而虚假胜出（审查 7.4）。

### 9.2 必须注入受控 I/O（否则加速比恒 ≈1）

`decide` 的 scripted fake 在每个 action 前 `sleep(AGENT_IO_DELAY_MS)`（默认 20ms，模拟模型/工具延迟）。B1/B2/B3 用**相同**延迟。

> 不注入 I/O 时，GIL 下三条件墙钟几乎相同，`speedup ≈ 1`，指标无信息量（审查 7.1）。

### 9.3 指标（5 项，全部外部观测）

| 指标 | 定义 | 观测方式（**不由被测系统自报**） |
|---|---|---|
| `lost_update_rate` | 被静默覆盖的写 / 并发写总数 | 基准脚本对比「各 agent 预期写集合（blob hash）」与「最终 manifest」做差分 |
| `conflict_rate` | 有冲突提交 / 提交总数 | 分 `detected`（本层返回非 OK）与 `silent`（提交成功但最终内容缺失该 agent 意图） |
| `speedup` | `T_B1 / T_B3` | 墙钟，只计执行期（不含 setup） |
| `serialization_rate` | 等待/被拒的 task / task 总数 | 基准脚本统计 `Result` 分布 |
| `wasted_work` | rebase/重试的 agent-turn / 总 agent-turn | 基准脚本统计 `turn_seq` 重放次数 |

> **`token_multiplier` 阶段一不适用**（scripted fake 无 LLM），明确标注 N/A 并给出阶段二启用条件（审查 7.3）。v1.0 说「四指标」与列 6 项矛盾，现统一为 **5 项**。

### 9.4 冲突负载

| 负载 | 构造 | 期望（诚实） |
|---|---|---|
| `C0` 文件集互斥 | 3 agent 改不同文件 | 0 冲突（sanity） |
| `C1` 弱重叠 | 共享 `types.ts`/配置 | 冲突集中在共享文件 |
| `C2` 强重叠 | 同一文件不同函数 | 需 merge，`GROW_ONLY` 生效 |
| `C3` write skew | 共享契约、各自改不同文件 | **本层在声明了 `logical_deps` 时拦截；未声明时与 A10 一致为 🟡（事后校验）** |

> v1.0 期望 C3「本层应拦截、B2 应漏检」高于设计承诺（A10 是 🟡），已改为诚实口径（审查 7.5/E2）。C3 的 `logical_deps` 由 `FixedDagPlanner` 从 task spec 读取（阶段一支持该字段）。

### 9.5 统计

- 每格 **n ≥ 30**（v1.0 的 n=3 无法支撑 95% CI，审查 7.6）。
- 报告：均值 ± 95% CI、配对符号检验、固定随机种子与硬件参数。
- 预注册判定阈值：`lost_update_rate(B3) < lost_update_rate(B2)` 且 `p < 0.05` 才算「本层优于 worktree」。

---

## 10. 风险与对策

| 风险 | 影响 | 对策 |
|---|---|---|
| 自研 JSON 3-way 语义不全 | 冲突处理错 | 默认 `GROW_ONLY` + 标量冲突即报；不追求「自动解决一切」 |
| WAL `fsync` 拖慢测试 | 测试慢 | 测试默认 `InMemoryEventLog`；WAL 只在 `-m fault` 与基准用；`fsync=False` 档位**禁止**进入 `-m fault`/基准 |
| 并发测试 flaky | 假失败 | `Barrier` + 固定轮次 + `@pytest.mark.serial`；断言用不变量 |
| scripted fake 掩盖真实 LLM 行为 | demo 不真实 | demo 双轨：scripted（CI 必跑）+ 真模型（手动） |
| 范围蔓延到 L4 | 阶段一失控 | §1.1 Out of scope 是硬边界；新需求进 §11 |
| 签名迁移一次性红一片 | 无法定位 | §2.3：迁移期只改签名不改行为；S0 判据是 35 测试全绿 |

---

## 11. 需要你确认的 4 件事

| # | 问题 | 建议 |
|---|---|---|
| **Q1** | 是否同意 R1：LangGraph/MCP 适配器移出阶段一 | **同意** |
| **Q2** | 测试期是否允许 `hypothesis`（运行时仍零依赖） | **允许** |
| **Q3** | demo 用 scripted fake，还是接真实 LLM | **scripted 为主 + 真模型可选** |
| **Q4** | Python 最低版本 | **3.11** |

---

## 12. 审查修订记录（v1.0 → v1.1）

### 12.1 P0（3 项）

| # | v1.0 问题 | v1.1 修法 |
|---|---|---|
| **P0-1** | `apply` 先 `atomic_write` 再 `append_in_txn`，崩溃窗口内「资源已改、事件未写」→ I8 不成立、丢事件 | 改为 **blob 先落盘 → WAL `append_batch` 为提交点 → manifest/文件为可重放投影**；读路径只看 manifest（§5.1） |
| **P0-2** | 补偿日志无**补偿幂等键**，崩溃重跑会误判 PIVOT 或二次回滚 | 加 `comp_id`；恢复按「当前 hash == reverse_hash / applied_hash / 其他」三分支判定（§4.8、§5.3） |
| **P0-3** | `EffectJournal` 对 `INTENT/EXECUTING/UNKNOWN` 的去重语义未定义 → 可能丢副作用或重复副作用 | 明确「只有 CONFIRMED 可短路，其余必须 probe」（§4.8） |

### 12.2 P1（14 项）

| # | 问题 | 修法 |
|---|---|---|
| 1 | `LOCK_ORDER = sorted()` 是内建函数，且「禁止嵌套持锁」与「排序后逐资源加锁」自相矛盾 | 改 `LOCK_KEY = lambda w: w.resource`；统一为「一次性按序持有全部资源锁」（§4.6、§6） |
| 2 | prepare/apply 两次加锁，未定义释放时机 → 自死锁或 TOCTOU | 明确「一次 acquire 全部，期间完成全部 prepare+apply，最后释放」（§5.1） |
| 3 | 全局锁序缺失（budget/intents/compensation/checkpoint 位置未定） | 新增 L0–L4 全局锁序表（§6） |
| 4 | WAL 帧不含 `seq/event_id` → 恢复后 seq 重排 | 帧内必含；恢复校验 seq 连续 + event_id 唯一（§4.3） |
| 5 | 未处理短写；`atomic_write` 同样未处理 | `write_all` 循环写（§4.2） |
| 6 | WAL 新建/轮转未 `fsync` 父目录 → 掉电丢整个文件 | 明确要求（§4.3） |
| 7 | `compensation_journal` 落盘后「先释放租约再写日志」丢意图 | 先写 `COMPENSATING_BATCH` 再释放租约（§5.3） |
| 8 | `canonical_json` 未定义，`repr(dict)` 键序敏感 → idem_key 失效 | 明确 `sort_keys/separators/ensure_ascii`（§4.8） |
| 9 | `logical_subgoal_id` 无分配者，Planner 回退到 task_id | Planner 必填、缺失报错（§4.11） |
| 10 | 接口签名与现状冲突（append/reserve/compatible/Status 带载/CheckpointStore 等） | 新增 §2.2 破坏性变更与调用点迁移清单 + `Result` 兼容垫片 |
| 11 | 注入图漏 10 个组件；「WAL 默认 + deny-by-default」与「35 测试兼容」矛盾 | 补全注入图；默认内存实现 + `default_token`（§3.2） |
| 12 | 基准：加速比无 I/O 恒 ≈1；丢失更新率自报（循环论证）；token 指标不适用 | 注入 I/O、外部差分度量、5 指标 + token N/A（§9） |
| 13 | 验收门不可判（S0 空跑、S2/S4/S5 测试数量不符、四 vs 六指标） | 每步判据列精确测试名（§8），指标统一 5 项（§9.3） |
| 14 | `§1.1` 称「事件是唯一真相源」，与设计 §5.6「CheckpointStore 是唯一恢复依据」冲突 | 改为上游口径（§1.2） |

### 12.3 中/低（已一并修）

| 问题 | 修法 |
|---|---|
| `DONE_PARTIAL` 混进任务级终态 | 拆 `TaskState` / `RunState`（§1.3） |
| 数组默认 `fail_closed` 与设计 `GROW_ONLY` 冲突 | 改 `GROW_ONLY` 默认 + 策略词表对齐（§4.6） |
| 「创建型写必进 PIVOT」与 Delete 哨兵矛盾 | 明确 create 可补偿（§4.6） |
| `REJECT_STALE_READ` 少了 rebase | 补 rebase 步骤（§4.10） |
| `guard` 漏 `intents.renew` | 补（§4.10） |
| `no_progress_steps` 无写入点 → I14 不可达 | 定义判定规则（§4.10） |
| `CheckpointStore` 漏 `cursor/aging_deadline` | 补齐（§4.8） |
| `resume_policy`（D9）完全缺失 | 补字段 + recover 分支（§5.4） |
| `try_admit` 失败不撤销 logical 声明 | 补回滚（§4.11） |
| `Event` 缺字段计数写错（6 vs 4） | 改为 4（§2.1） |
| `INVALID_TRANSITION` 破坏错误码封闭性 | 复用 `INVALID_INPUT`（§4.1） |
| `CapabilityToken` 四维 vs 夹具三维 | 四维全默认（§4.9） |
| `subscription` 无归属 | 纳入 §3.1，阶段一只做注册 + 有序分发（§4.4） |
| 覆盖率一刀切 80% 不可达 | 分模块阈值（§7.1） |
| C3 期望高于 A10 承诺 | 改为诚实口径（§9.4） |
