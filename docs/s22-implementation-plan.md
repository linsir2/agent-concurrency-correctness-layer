# S22 实施方案 v0.2：内核收口 + 私有工作区 + 被集成门面（待拍板）

| 项目 | 内容 |
|---|---|
| 版本 | **v0.2（含 worktree 决策）** |
| 日期 | 2026-10-28 |
| 上游 | [S22 调研与被集成方案 v0.2](s22-integration-surface.md)；[设计规范 v1.12](multi-agent-harness-design-final.md) §3（Z3=per-agent worktree，Z2=RSC 提交层） |
| 已拍板 | ① 内容来源=**读私有工作区** ② worktree **由我们实现**（最小侵入） ③ 砍掉 `run_plan` ④ token **持久化** ⑤ 允许新增两条状态机边 |
| 原则 | 最小化、第一性原理、只改根源；先复用本地已有设计；TDD + RIPR |

---

## 0. 为什么做这些（第一性原理）

宿主只需要我们回答三件事：**动手前允许吗 / 动手后算数吗 / 出事时资源还回来吗**。

现有内核已实现前两件的内部逻辑，但只通过 `dispatch_and_run`（自己跑完 agent 循环）暴露。宿主自己跑循环时，第三件（结算/回收）没有入口 → 必然泄漏预算与槽位。**本轮补第三件 + 把三件包成门面 + 提供 Z3 私有工作区（否则"读回真实内容"无从谈起）。**

**Z3 的必要性**：如果不给每个 agent 独立工作区，工具会直接改共享文件——我们只能在提交时发现"你基于旧版本"，但文件已被覆盖，阻止不了。Z3 是"先隔离、后合并"的物理前提。

---

## 1. 现有设计（必须复用，不另起炉灶）

| 机制 | 位置 | 本轮怎么用 |
|---|---|---|
| `Planner.plan` | [orchestration.py:149](harness/orchestration.py#L149) | `begin` 把 intent 变合法 task |
| `try_admit` | [orchestration.py:503](harness/orchestration.py#L503) | 唯一准入入口 |
| `_rollback` | [orchestration.py:714](harness/orchestration.py#L714) | **泛化为公共 `abort`** |
| `_finalize` | [harness.py:3579](harness/harness.py#L3579) | **暴露为公共 `finish`** |
| `commit_batch` | [harness.py:1022](harness/harness.py#L1022) | `apply` 的唯一提交入口 |
| `_dedup_event` / `_commit_key` | [harness.py:754](harness/harness.py#L754) / [:743](harness/harness.py#L743) | 扩展出外部幂等键分支 |
| `checkpoints.latest_ref_for_acb` | [checkpoint.py:207](harness/checkpoint.py#L207) | **token 持久化的索引**（不新建存储） |
| `_restore_acb` / `_restore_budget` | [harness.py:4103](harness/harness.py#L4103) | token 恢复（含预算节点/槽位重绑） |
| `purge_run` | [harness.py:2847](harness/harness.py#L2847) | reaper 回收内存 |
| `path_of` | [workspace.py:64](harness/workspace.py#L64) | 提交后把内容物化到真实路径（可选） |
| `IntentRegistry._logical` | [intent_registry.py:25](harness/intent_registry.py#L25) | `undeclare_all` 反查逻辑声明 |

**不动的既有设计**：`dispatch_and_run` / `run_task` / `_drain_scheduler` / `scan_deadlines`(aging) / `Ingress` / `WorkspaceManager.stage` / `EffectJournal` 全部保留原语义。

---

## 2. 变更清单

| 编号 | 变更 | 类型 | 文件 | 规模 |
|---|---|---|---|---|
| **C1** | 公共 `abort(acb)`；`_rollback` 收敛到它 | 内核（泛化） | orchestration.py, intent_registry.py | ~40 行 |
| **C2** | 公共 `finish(acb)` + 2 条状态机边 | 内核（暴露） | harness.py, types.py | ~25 行 |
| **C3** | `commit_batch(..., idempotency_key=)` | 内核（扩展） | harness.py | ~55 行 |
| **C4** | token 过期回收（`tick()` 内） | 内核（新增） | harness.py | ~25 行 |
| **C5** | `_restore_acb(..., bump_epoch=False)` | 内核（参数化） | harness.py | 2 行 |
| **C6** | `harness/worktree.py`：Z3 生命周期 | 新能力 | 新文件 | ~140 行 |
| **C7** | `harness/facade.py`：`RscSession` 六动作 + token 索引 | 外壳 | 新文件 | ~260 行 |
| **C8** | LangGraph 中间件（root 注入 + 读回） | 外壳 | 新目录 | ~180 行 |
| **C9** | MCP server（4 工具） | 外壳 | 新目录 | ~120 行 |
| **D1** | ~~`run_plan`~~ | 不做 | — | 宿主驱动自己的循环 |
| **D2** | ~~独立 `stage` 动作~~ | 不做 | — | `apply` 直接带内容 |
| **D3** | ~~`gate` 接 EffectJournal~~ | 不做 | — | 外部副作用幂等归宿主 outbox |
| **D4** | ~~回写用户仓库~~ | 不做 | — | 我们只写 Z2 账本；合并回仓库由宿主/历史层决定 |

---

## 3. 逐项设计

### C1 · 公共 `abort(acb)`

**根因**：`_rollback` 需要局部变量 `declared`，外部拿不到。

**修法（不新增 ACB 字段）**：`IntentRegistry` 已有 `_logical: {dep_id: {agent_id: kind}}`，补一个与 `revoke_all` 对称的 `undeclare_all(agent_id)`；`_rollback` 直接委托 `abort`。

```python
# intent_registry.py
def undeclare_all(self, agent_id: str) -> int      # 幂等，返回撤销条数
# orchestration.py · Dispatcher
def abort(self, acb: ACB) -> None                  # 幂等；不抛普通异常
```

**步骤**：
1. `undeclare_all`：遍历 `_logical`，删该 agent 条目，空 dict 回收。
2. `Dispatcher.abort`：`undeclare_all` → 逐个 `budget.release(rid)`（退款）+ 摘 `_reservations`/`node.reservations` → `remove_node(owner=acb.id)` → 清 `reservation_ids`/`budget_node` → `slots.release`。
3. `_rollback(acb, node, resv, declared)` 改为 `self.abort(acb)`（保留签名，20+ 调用点不动）。
   **等价性**：`reserve` 成功后紧接写 `acb.reservation_ids`/`budget_node`（[orchestration.py:688-693](harness/orchestration.py#L688)）；`declared` 即该 ACB 在 `_logical` 的全部条目。
4. `Harness.abort(acb)` 薄委托。

**RIPR**：`test_c1_abort_releases_everything`
- R：`begin` 成功后 `abort`；I：若只 release 不摘节点/槽位；P：`budget._nodes`/`slots`/`_reservations` 公开可读；Reveal：断言节点消失、预留消失、`subtree_reserved` 归零、槽位可再取满额、`_logical` 为空。

---

### C2 · 公共 `finish(acb)`

**根因**：`_finalize` 只对**终态**结算；门面 token 停在 `ADMITTED`，没有到 DONE/FAILED 的合法边。

```python
# types.py
(TaskState.ADMITTED, "finish_ok"): TaskState.DONE
(TaskState.ADMITTED, "fail"):      TaskState.FAILED
# harness.py
def finish(self, acb: ACB, *, outcome: str = "DONE") -> Result
```

**步骤**：`advance` → 成功则 `self._finalize(acb)`；已是终态 → 直接 OK（幂等）。

**RIPR**：
1. `test_c2_finish_settles_and_releases`：`begin→apply→finish`；断言预留 `SETTLED`、`consumed` 保留、槽位释放、节点归档。变异：只改状态不 `_finalize` → 预留仍 ACTIVE。
2. `test_c2_finish_without_apply_refunds`：`begin→finish`；断言 `remaining()` 回初值。变异：用 `consume` 代替 `settle` → 余额不还。
3. `test_c2_double_finish_is_noop`：两次 finish 不重复结算。

---

### C3 · 外部幂等键

**根因**：`_dedup_event` 只按内容哈希；重试同内容能去重，但"同键异内容"无法识别为冲突。

```python
def commit_batch(self, acb, writes, mode=Mode.COMMIT, *, idempotency_key=None) -> Result
```

**步骤**：
1. `_commit_record(..., idempotency_key=None)`：非空才写 `payload["external_idem_key"]`（空则旧事件字节不变）。
2. `commit_batch` 在 `_scope_lock` 内、`_dedup_event` 之前：`_dedup_by_external_key(key, expected)`：
   - 集合完全相同 → 补投影 + 返回 `OK{dedup:True, receipt_ids, seq}`；
   - 任一不同 → `CONFLICT`；
   - 无 → 继续原流程。
3. `apply` 在解析 token **之前**先查外部键 → finish/purge 之后的重试仍幂等。
   代价：O(事件数) 线性扫描（与既有 `_dedup_event` 同阶）；日后可加二级索引。

**RIPR**：
1. `test_c3_same_key_same_content`：事件只 +1、receipt 相同。变异：不查键 → 事件 +2。
2. `test_c3_same_key_different_content`：`CONFLICT`、`version` 不变。
3. `test_c3_replay_after_finish`：`apply→finish→purge→apply(同键)` 仍返回同一收据。变异：先解析 token → unknown → 失败。

---

### C4 · token 过期回收

**根因**：宿主崩溃/忘 finish → 预算+槽位+worktree 永久占用。

**不复用 aging**：aging 语义是"准入后未被调度 → INADMISSIBLE（可重试）"；token 过期是"会话被放弃 → 退款取消"。混用会把可重试状态和预算语义搅在一起。

```python
# begin 写入
acb.spec["rsc_token"] = True
acb.spec["token_deadline_ms"] = now + token_ttl_ms
# harness.py
def _reap_expired_tokens(self, now_ms: int) -> int
```

**步骤**：`tick()` 在 `reap_expired` 后调用；条件 = `spec["rsc_token"]` + 非终态 + 超时 → `advance(cancel)` → `dispatcher.abort` → 销毁 worktree（回调）→ 全终态则 `purge_run`。只碰带 `rsc_token` 的 ACB。

**RIPR**：
1. `test_c4_expired_token_reclaimed`：`ManualClock` 超时 → tick → 状态 CANCELLED、预算恢复、worktree 目录消失。
2. `test_c4_finished_token_not_reaped`：finish 后 tick 不二次结算。
3. `test_c4_non_token_untouched`：普通 ACB 不受影响。

---

### C5 · token 持久化

**根因**：`acb.id` 只在 `h.acbs` 内存；重启后 token 失效。

**复用**：`checkpoints.latest_ref_for_acb(acb_id)` 已能按 id 找快照；`_restore_acb` 已能重绑预算节点与槽位。

**唯一内核改动**：`_restore_acb(data, resume_policy="RESUME", *, bump_epoch=True)`，把 [:4217](harness/harness.py#L4217) 的 `self.bump_epoch()` 改为条件执行。
**为什么必须关掉**：`bump_epoch()` 在 `epoch_scope="global"` 下会推进全局 epoch，**误伤其他 token 的租约**；懒恢复单个 token 不该有全局副作用。新进程里旧租约本就不存在，不需要 fence。

**token 索引**（facade 维护，不新增内核存储）：`{token: {ref, worktree, agent_id, deadline_ms}}`，用 `storage/atomic.atomic_write` 落盘到 `state_dir`。
`resolve(token)`：内存 → 索引 → `load_strict(ref)` → `_restore_acb(..., bump_epoch=False)` → 终态则 `Deny(terminal)`。

**RIPR**：
1. `test_c5_token_survives_restart`：新建 Harness（同 `checkpoint_root`）→ `apply(token)` 成功，`version` 推进。
2. `test_c5_unknown_token_denied`：随机 token → `Deny(unknown_token)`（不静默）。
3. `test_c5_no_global_epoch_bump`：恢复 token 后，另一 token 的租约仍有效（`is_valid=True`）。变异：不关 `bump_epoch` → 另一租约失效 → 失败。

---

### C6 · `harness/worktree.py`：Z3 生命周期

**目标**：每个 agent 一个真实目录，工具在里面改文件，我们从里面读回内容。

**契约**：

```python
class WorktreeManager:
    def __init__(self, base: str, root: str, *, ref: str = "HEAD", mode: str = "auto")
    def create(self, agent_id: str) -> str        # 幂等；返回绝对路径
    def destroy(self, path: str) -> None          # 幂等
    def changed(self, path: str) -> list[str]     # 相对路径
    def read(self, path: str, rel: str) -> bytes | None
    def destroy_all(self) -> None
```

**实现**（已实测）：

- `mode="auto"`：`base/.git` 存在且 `git` 可用 → `git worktree add --detach <root>/<safe(agent)> <ref>`；否则 `shutil.copytree` 兜底。
- `changed`：git 模式 `git -C <path> status --porcelain`；copy 模式对比 create 时记录的 mtime+size 清单。
- `read`：`open(join(path, rel), "rb")`；不存在 → `None`（用于 DELETE 判定）。
- `destroy`：`git worktree remove --force` / `shutil.rmtree`。
- **安全**：`rel` 必须经 `realpath` 校验在 `path` 之内（防 `../` 逃逸）；目录名对 agent_id 做安全化。

**关键边界（写进文档）**：
- 我们**不 chdir、不 monkeypatch `open`、不碰宿主的 cwd**（并发下 chdir 会串）。
- 我们**不回写用户仓库**；只把内容提交进 Z2 账本。合并回仓库由宿主/历史层决定。
- 共用同一 root 的场景不保证不覆盖（Z3 的前提是独立 root）。

**RIPR**：
1. `test_c6_isolation`：两个 agent 各自 worktree 改同一文件；断言两个目录内容互不影响、base 仓库不变。
2. `test_c6_changed_and_read`：改一个、删一个、加一个；`changed` 精确返回三个相对路径；`read` 对已删文件返回 None。
3. `test_c6_path_escape_rejected`：`read(path, "../../etc/passwd")` 抛 `ValueError`。
4. `test_c6_cleanup`：`destroy` 后目录消失；重复 `destroy` 不抛。
5. `test_c6_copy_fallback`：非 git 目录 → copytree 模式仍可 create/changed/read/destroy。

---

### C7 · `harness/facade.py`：`RscSession`

**语义修正**：**token = agent 会话**（不是单次工具调用）。一个 agent 多次 `apply`，最后 `finish`；worktree 全程复用。

```python
class RscSession:
    def __init__(self, harness, *, tenant=None, token_ttl_ms=120_000,
                 min_budget=1, worktrees: WorktreeManager | None = None,
                 state_dir: str | None = None)

    def begin(self, agent_id, intent_key, *, writes=(), reads=()) -> Token | Wait | Deny
    def apply(self, token, writes, *, idempotency_key, cost=1) -> Receipt | Conflict | Deny
    def gate(self, token) -> Allow | Deny
    def finish(self, token, *, outcome="DONE") -> None
    def abort(self, token, reason="") -> None
    def snapshot(self, token) -> dict[str, int]
```

| 动作 | 步骤（全部复用） |
|---|---|
| `begin` | 建 run_id/task spec → `planner.plan` → **先登记 `h.plans`** → `_admit_and_register`（从 `dispatch_and_run` 抽出的三行）→ 非 ADMITTED 映射 `Wait`/`Deny` → 钉住 `base_version` → 建 worktree（若配置）→ 写 token 索引 → `Token` |
| `apply` | 先按外部键查（C3）→ 命中即返回；否则解析 token → 校验 `cost<=min_budget` → `commit_batch(..., idempotency_key)` → OK 且非 dedup → `budget.consume` → `Receipt`；冲突/拒绝按 §4 映射 |
| `gate` | 终态 → `Deny(terminal)`；逐项比较 `workspace.version(r)` 与 `base_version` → `Deny(stale_view)` / `Allow` |
| `finish` | `h.finish(acb, outcome)` → 销毁 worktree → 删索引 |
| `abort` | `advance(cancel)` → `h.abort(acb)` → 销毁 worktree → 删索引 |
| `snapshot` | `dict(acb.base_version)` |

**异常 → Deny（G5）**：外层按**异常类名**映射 `RaftUnavailable/NotLeader/FencingRejected/ReceiptConflict/InvariantError` → `Deny`；其他异常原样抛。

**`dispatch_and_run` 去重**：抽出 `Harness._admit_and_register(plan, task_id, ...)`，两处共用。

---

### C8 · LangGraph 中间件

```python
def rsc_writes(*paths: str, root_arg: str | None = None)   # 声明写哪些相对路径
class RscMiddleware(AgentMiddleware):
    def before_agent(self, state, runtime) -> dict | None   # 建会话（begin + worktree）
    def after_agent(self, state, runtime) -> dict | None    # finish + 销毁 worktree
    def wrap_tool_call(self, request, handler)               # gate → 注入 root → handler → 读回 → apply
    async def awrap_tool_call(self, request, handler)        # 同上（异步必须实现）
```

**关键点（评审 P0 的修复）**：
1. **sync + async 双实现**（基类异步默认抛 `NotImplementedError`）。
2. **root 注入是显式 opt-in**：`@rsc_writes("src/auth.py", root_arg="root")` 且工具函数接受 `root`；中间件用 `dataclasses.replace(request, tool_call={...,"args":{**args,"root":path}})`（已实测 `ToolCallRequest` 是 dataclass）。
3. **未声明 writes → 直接放行**并记 `unsupported`（不假装覆盖）。
4. **读回**：对每个声明路径 `worktree.read(path, rel)`；存在 → `Write(rel, content)`；不存在且账本里有 → `Write(rel, DELETE)`；不存在且账本里没有 → 跳过。
5. 失败映射：`Wait` → `ToolMessage(status="error", content="RSC_WAIT:...")`（不抛，可排队）；`Deny` → 抛 typed error；`Conflict` → `Command(update={"messages":[ToolMessage(..., tool_call_id=同一 id)]})`（必须绑定同一 id，否则 `ToolNode` 校验失败）。
6. 懒加载：模块可导入；未装 langchain 时 `RscMiddleware.__init__` 抛 `ImportError`；测试 `pytest.importorskip`。

---

### C9 · MCP server

| 工具 | 语义 |
|---|---|
| `rsc_apply(resource, content, idempotency_key)` | 无 token 的**单次会话**：begin→apply→finish（内部建/销毁临时 worktree 或直接提交 content） |
| `rsc_begin(agent_id)` / `rsc_finish(token)` | 多步会话（供会多次写的 agent） |
| `rsc_gate(token)` | 副作用前校验 |
| `rsc_snapshot(token)` | 版本快照（+ `rsc://snapshot/{token}` 资源模板） |

服务端入口先 `h.tick()`；`agent_id` 由 server 配置，不接受调用方声明（防冒名）。

---

## 4. 数据流

```
宿主 agent（LangGraph node / MCP client）
  │  before_agent / 首次工具调用
  ├─▶ RscSession.begin ──▶ Planner.plan + try_admit ──▶ Token
  │                         └─ WorktreeManager.create(agent) ──▶ Z3 目录
  │
  ├─▶ 工具在 Z3 目录里改文件（root 由中间件注入；不碰共享目录）
  │
  ├─▶ wrap_tool_call
  │     ├─ gate：base_version 比对 → Deny(stale_view)
  │     ├─ handler(request with root)
  │     ├─ 读回 Z3 的声明路径（存在/删除）
  │     └─ apply → 外部键查重 → commit_batch(CAS+租约+收据) → budget.consume
  │
  └─▶ after_agent / rsc_finish
        └─ finish → _finalize（settle + archive + slots.release）→ destroy worktree
                                  ▲
        崩溃/忘 finish ──▶ tick → _reap_expired_tokens → abort（退款+释放）+ 销毁 worktree + purge_run
```

**架构图**

```
┌────────────────────────────────────────────────────────────┐
│ 宿主：LangGraph / MCP client（自己的编排与 LLM）            │
└──────────┬───────────────────────────────┬─────────────────┘
           │ ★RscMiddleware（root 注入/读回）│ ★MCP server
           ▼                               ▼
   ┌──────────────────────────────────────────────┐
   │ ★ RscSession（六动作 + token 索引）           │
   └───┬───────────────┬──────────────────┬───────┘
       │               │                  │
 ┌─────▼──────┐ ┌──────▼───────┐  ┌───────▼────────┐
 │ Dispatcher │ │ commit_batch │  │ ★WorktreeMgr   │
 │ admit/abort│ │ +idem key    │  │ Z3 生命周期     │
 └─────┬──────┘ └──────┬───────┘  └────────────────┘
       │               │
 ┌─────▼──────┐ ┌──────▼───────┐ ┌────────────────┐
 │ IntentReg  │ │ WorkspaceMgr │ │ BudgetManager  │
 │+undeclare  │ │ RSC/CAS      │ │ consume/settle │
 └────────────┘ └──────────────┘ └────────────────┘
       │
 ┌─────▼──────────────────────────────────────────────┐
 │ EventLog（唯一提交点）+ CheckpointStore（token 恢复）│
 └────────────────────────────────────────────────────┘
```

---

## 5. TDD / RIPR 用例矩阵

| # | 场景 | 形态 | 判据 |
|---|---|---|---|
| 1 | 两 agent 并发写同一文件 | facade / LG / MCP | 一成功一 `Conflict`；各自 Z3 内容独立；base 仓库不变 |
| 2 | 同键同内容重试 | 三形态 | 同一 receipt；事件只 +1 |
| 3 | 同键异内容 | 三形态 | `CONFLICT`；`version` 不变 |
| 4 | 过期 token 提交 | 三形态 | `Deny`；不写入 |
| 5 | 预算耗尽 → 回收 | 三形态 | `Wait`；tick 后余额恢复、Z3 目录消失 |
| 6 | 协调器不可达 | 三形态 | `Deny`（不抛、不放行） |
| 7 | 重启后旧 token 继续用 | facade | `apply` 成功（C5） |
| 8 | 路径逃逸 | worktree | `ValueError` |

**RIPR 统一写法**：R 走到目标分支；I 变异后状态错；P 通过 `workspace.version`/`budget.remaining`/事件计数/`receipt_id`/目录存在性观测；Reveal 有显式断言。每个用例在 §3 标了"必杀变异"。

---

## 6. 影响面与兼容性

| 变更 | 对既有 652 用例 | 论证 |
|---|---|---|
| C1/C2 | 应全绿 | 等价性见 §3；新增边无既有路径 |
| C3 | 无 | 默认 `None` → 事件字节不变 |
| C4 | 无 | 只匹配 `spec["rsc_token"]` |
| C5 | 无 | 新增关键字参数默认 `True`（原行为） |
| C6–C9 | 无 | 新模块；`harness/__init__.py` 只导出 `RscSession`；适配器不导出 |

**回归**：全量 3 轮 + S14–S21 八个 conformance + 覆盖率门禁 + 新增 S22 conformance。

**打包**：`dependencies = []` 不变；extras 加 `langgraph = ["langchain>=1.2","langgraph>=1.0"]`、`mcp = ["mcp>=1.9"]`。

---

## 7. 最终形态（先想终点）

```python
from harness import create_production_harness
from harness.facade import RscSession
from harness.worktree import WorktreeManager
from harness.integrations.langgraph import RscMiddleware, rsc_writes

h = create_production_harness(checkpoint_root="./ckpt")
s = RscSession(h, tenant="acme",
               worktrees=WorktreeManager("/path/to/repo", "/tmp/rsc-wt"),
               state_dir="./ckpt")

@rsc_writes("src/auth.py", root_arg="root")
def edit_auth(root: str, spec: str) -> str:
    ...  # 在 root 下改文件
    return "done"

agent = create_agent(model="...", tools=[edit_auth],
                     middleware=[RscMiddleware(s)])
agent.invoke({"messages": [...]})
# 并发第二个 agent → 一个成功、一个 Conflict 并重读
# 进程崩溃 → 下次 tick 自动退预算/槽位/删除 worktree
```

**验收**：8 个场景 × 对应形态全绿；全量 0 回归；`dependencies=[]`；未装 langgraph/mcp 时 `import harness` 正常。

---

## 8. 明确不做

| 项 | 理由 |
|---|---|
| `run_plan` | 宿主驱动自己的循环；L4 不是产品 |
| 独立 `stage` | `apply` 直接带内容 |
| `gate` 接 EffectJournal | 外部副作用幂等归宿主 outbox |
| 回写用户仓库 | 我们只写 Z2 账本；合并回仓库由宿主/历史层决定 |
| 全局 `chdir` / monkeypatch `open` | 并发下会串；root 通过显式参数注入 |
| overlayfs / FUSE | 需要特权，沙箱不可用；git worktree + copy 兜底已够 |
| 字符级 CRDT | 收敛 ≠ 语义正确 |

---

## 9. 已拍板记录

| # | 决策 | 结论 |
|---|---|---|
| Q1 | 内容来源 | **读私有工作区（Z3）** |
| Q2 | worktree 谁做 | **我们做**（git worktree + copy 兜底，不碰宿主 cwd/代码） |
| Q3 | `run_plan` | **砍掉** |
| Q4 | token 持久化 | **做**（`latest_ref_for_acb` + `_restore_acb(bump_epoch=False)`） |
| Q5 | 状态机边 | **允许新增两条** |

---

## 10. 自检

| 检查项 | 结论 |
|---|---|
| 每个新增函数有唯一消费者？ | `undeclare_all`←`abort`；`abort`←`_rollback`+facade+reaper；`finish`←facade；`_dedup_by_external_key`←`apply`；`_reap_expired_tokens`←`tick`；`_admit_and_register`←`dispatch_and_run`+`begin`；`WorktreeManager`←facade |
| 和已有设计重复？ | 无：结算复用 `_finalize`、回滚复用 `_rollback`、去重复用事件扫描、回收复用 `abort`+`purge_run`、恢复复用 `_restore_acb` |
| 删了仍在数据流上的东西？ | 无：`stage`/`EffectJournal`/`dispatch_and_run`/`run_task`/`scan_deadlines` 全保留 |
| 断言能被变异杀死？ | §3 每条都标了"必杀变异" |
| 新增依赖？ | 否；只有 extras |
| 最小侵入？ | 宿主侧只需 `@rsc_writes(..., root_arg=...)` + 工具接受 `root`；不改 cwd、不改宿主代码 |

---

## 11. 实现结果（C1–C6，2026-10-28）

### 11.1 交付

| 编号 | 状态 | 落点 |
|---|---|---|
| C1 `abort` / `undeclare_all` / `_rollback` 收敛 | ✅ | [orchestration.py](harness/orchestration.py)、[intent_registry.py](harness/intent_registry.py) |
| C2 `finish` + 两条状态机边 | ✅ | [harness.py](harness/harness.py)、[types.py](harness/types.py) |
| C3 外部幂等键 + `receipt_ids` | ✅ | [harness.py](harness/harness.py) |
| C4 token 过期回收 | ✅ | [harness.py](harness/harness.py) |
| C5 `_restore_acb(bump_epoch=False)` + 预算懒重绑 | ✅ | [harness.py](harness/harness.py) |
| C6 `WorktreeManager`（git + copy） | ✅ | [worktree.py](harness/worktree.py) |
| C7 facade / C8 LangGraph / C9 MCP | ⏳ 未开始 | 等本轮内核验收后再做 |

### 11.2 验证

| 门禁 | 结果 |
|---|---|
| 全量测试 | **679 passed / 4 xfailed**（新增 27 个 S22 用例） |
| S22 一致性核对 | `scripts/check_s22_conformance.py` **69 / 0** |
| 历史核对 | S14–S21 全 0 失败（45/73/32/56/37/40/34/77） |
| 覆盖率 | 整体 **84.6%**；`worktree.py` 85%+ |
| 回归 | 既有 652 个用例零破坏（含 S15 契约漂移检查） |

### 11.3 评审驱动的修复（对抗式评审，1 P0 + 6 P1）

| 级别 | 发现 | 修复 |
|---|---|---|
| P0 | copy 模式只用 `(mtime,size)` 检测 → **同尺寸+恢复 mtime 的改写漏检 = 静默丢写** | 快照增加 `ctime_ns` + 小文件（≤256KiB）内容哈希；补回归用例 |
| P1 | 外部键重放命中后不刷新 `base_version` → 门面 `gate` 必然误判 stale | 命中分支补 `_refresh_after_commit` |
| P1 | `create` 同 agent 并发互删目录 / `FileExistsError` | `WorktreeManager` 加进程内 RLock 覆盖 create/destroy |
| P1 | git→copy 降级后 `changed()` 抛 `AttributeError` | 无基线时按「全部可疑改动」保守返回 |
| P1 | base 缺失/路径已存在抛裸 `OSError` | 统一包装为 `WorktreeError` |
| P1 | reaper 把 `INADMISSIBLE`/`QUARANTINED`（可重试态）转终态并 purge | 显式跳过这两个状态 |
| P1 | reaper 与在途提交竞争（退款 vs consume） | 有活跃 claim 时跳过本次回收 |
| P2 | 懒恢复可能触发 `_replay_budget_events` 重置整棵树 | 节点已存在则直接重绑（且校验 owner） |
| P2 | `destroy` 失败留下 git prunable 登记 | 追加 `git worktree prune` |

### 11.4 明确未做（本轮）

- C7 `RscSession`、C8 LangGraph 中间件、C9 MCP server —— 依赖本轮内核验收。
- token 索引文件（C7 随 facade 一起落地）。
- `run_plan`（已拍板砍掉）。

---

## 12. C7–C9 实现调研（2026-10-28）

> 目标：确定 facade / LangGraph 中间件 / MCP server 的**真实可用形态**，并给出测试策略。
> 核实方式：直接读 `langchain 1.2.15` / `langgraph 1.1.10` / `mcp 1.30.0` 的源码签名 + 本地实测。

### 12.1 API 实况（已核实）

| 项 | 事实 | 结论 |
|---|---|---|
| `AgentMiddleware.wrap_tool_call` | `(self, request, handler) -> ToolMessage \| Command` | 只拿到 `tool_call{name,args,id}` 与工具返回值 |
| `AgentMiddleware.awrap_tool_call` | 同上，`handler` 是 async；**基类默认抛 `NotImplementedError`** | 必须 sync + async 双实现 |
| `ToolCallRequest` | dataclass，字段 `tool_call/tool/state/runtime`；直接赋值**已废弃** | 用 `request.override(tool_call=...)` 做不可变替换 |
| `request.runtime` | `ToolRuntime`：`state`、`config`（含 `run_id`/`thread_id`）、`context`、`tool_call_id` | 从这里取 agent_id / token / 身份 |
| `before_agent` / `after_agent` | `(self, state, runtime) -> dict \| None`（返回即**合并进 state**） | 会话生命周期的正确挂点 |
| `Runtime`（中间件里） | 只有 `context` / `store` / `stream_writer`，**没有** `config` | agent_id 从 `runtime.context` 取 |
| `ToolMessage` | `content` / `tool_call_id` / `artifact` / `status` | 失败消息要绑定 `tool_call_id` |
| `Command` | `Command(update={"messages": [ToolMessage(...)]})` | `ToolNode` 校验要求消息绑定同一 `tool_call_id` |
| MCP `FastMCP` | `.tool(name=..., description=...)`、`.resource(uri, ...)`；stdio / Streamable HTTP（SSE 已废弃） | 装饰器必须关键字参数 |
| 本地 codec | `storage/codec.dumps/loads`（str）、`storage/atomic.atomic_write` | token 索引文件可直接复用 |

### 12.2 C7 · `RscSession`（facade.py）

**语义**：token = agent 会话。`begin` 一次、多次 `apply`、`finish`/`abort` 一次；worktree 全程复用。

```python
@dataclass(frozen=True) class Token: token, run_id, agent_id, tenant, base_versions, worktree, deadline_ms
@dataclass(frozen=True) class Wait: reason
@dataclass(frozen=True) class Deny: reason
@dataclass(frozen=True) class Receipt: receipt_id, seq, dedup
@dataclass(frozen=True) class Conflict: reason
@dataclass(frozen=True) class Allow
```

| 动作 | 步骤（全部复用内核） |
|---|---|
| `begin(agent_id, intent_key, *, writes=(), reads=())` | `run_id=f"rsc-{uuid4().hex[:12]}"` → `planner.plan` → `_admit_and_register` → 非 ADMITTED 映射 `Wait`(BUDGET/SLOT/PREDECESSOR) 或 `Deny` → 钉住 `base_version` → `worktrees.create(agent_id)` → 写 `rsc_token`/`token_deadline_ms` → `checkpoints.save` + 索引落盘 → `Token` |
| `apply(token, writes, *, idempotency_key, cost=1)` | **先按外部键查**（`h._dedup_by_external_key`，算 `sha256(canonical_bytes(content))`）→ 命中即 `Receipt`、异内容 `Conflict`；否则解析 token → 校验 `cost<=min_budget` → `commit_batch(idempotency_key=...)` → OK 且非 dedup → `budget.consume` → `Receipt` |
| `gate(token)` | 终态 → `Deny(terminal)`；逐项比对 `workspace.version` 与 `base_version` → `Deny(stale_view)` / `Allow` |
| `finish(token)` | `h.finish` → 销毁 worktree → 删索引 |
| `abort(token)` | `advance(cancel)` → `h.abort` → 销毁 worktree → 删索引 |
| `snapshot(token)` | `dict(acb.base_version)` |

**token 持久化（C5 的消费者）**：索引文件 `tokens.json`（`codec.dumps` + `atomic_write`），结构 `{token: {ref, worktree, agent_id, deadline_ms}}`。`resolve(token)`：内存 → 索引 → `checkpoints.load_strict(ref)` → `_restore_acb(..., bump_epoch=False)` → 终态或 `needs_readmit` → `Deny`。

**异常 → Deny**：外层按**类名**映射 `RaftUnavailable/NotLeader/FencingRejected/ReceiptConflict/InvariantError` → `Deny`，其余原样抛。
**注册清理钩子**：`h._token_cleanup = self._on_reaped`（C4 的消费者）。

### 12.3 C8 · LangGraph 中间件

```python
def rsc_writes(*paths, root_arg=None): ...     # 装饰器 → func.__rsc_writes__/__rsc_root_arg__

class RscMiddleware:
    def __init__(self, session, *, agent_id_from=None)
    def before_agent(self, state, runtime) -> dict | None   # begin → 返回 {"rsc_token": ...}
    def after_agent(self, state, runtime) -> dict | None    # finish
    def wrap_tool_call(self, request, handler) -> ToolMessage | Command
    async def awrap_tool_call(self, request, handler) -> ToolMessage | Command
```

流程（每次工具调用）：
1. 从 `tool` 读 `__rsc_writes__`；**未声明 → `handler(request)` 直接放行**（记 `unsupported`）。
2. `token = request.state.get("rsc_token")`；无 token（宿主没用 `before_agent`）→ 懒建。
3. `gate` → `Deny` 抛 typed error；`Allow` 继续。
4. `root_arg` 存在 → `request.override(tool_call={**tc, "args": {**tc["args"], root_arg: worktree}})`；否则原样。
5. `result = handler(req2)`（异步版 `await`）。
6. 读回声明路径：`worktree.read(path, rel)` 存在 → `Write(rel, content)`；不存在且账本有 → `Write(rel, DELETE)`；不存在且账本没有 → 跳过。
7. `apply(token, writes, idempotency_key=key)`：
   - `Receipt` → 返回 `result`；
   - `Conflict` → `Command(update={"messages": [ToolMessage(content="RSC_CONFLICT:...", tool_call_id=tc["id"], status="error")]})`；
   - `Deny` → 抛 typed error；
   - `Wait` → `ToolMessage(status="error", content="RSC_WAIT:...")`（不抛，可排队）。

**幂等键**：`sha256(f"{tenant}|{agent_id}|{resource}|{tool_call['id']}")`。

**agent_id 约定（重要）**：`agent_id_from` 默认 `runtime.context["agent_id"]` → `runtime.config["thread_id"]` → `"default"`。**多 agent 节点必须各自设置 `context.agent_id`**，否则多个节点共享一个 token/worktree（失去隔离）。

### 12.4 C9 · MCP server

| 工具 | 语义 |
|---|---|
| `rsc_apply(resource, content, idempotency_key)` | 无 token 的**单次会话**：begin→apply→finish（内部临时 worktree） |
| `rsc_begin(agent_id)` / `rsc_finish(token)` | 多步会话 |
| `rsc_gate(token)` / `rsc_snapshot(token)` | 副作用前校验 / 版本快照 |

+ resource `rsc://snapshot/{token}`。
实现上：`make_tools(session) -> dict[str, callable]`（模块级可测函数）+ `build_mcp_server(session)`（用 `FastMCP` 装饰器包装）。服务端入口先 `h.tick()`。`agent_id` 由 server 配置，**不接受调用方声明**（防冒名）。

### 12.5 测试策略（不跑真模型）

| 形态 | 怎么测 |
|---|---|
| facade | 直接调 `RscSession`，配合 `tmp_path` git 仓库的 `WorktreeManager` |
| LangGraph | 构造 duck-typed `SimpleNamespace` 的 `request`（`tool_call/tool/state/runtime`），直接调 `wrap_tool_call` 与 `await awrap_tool_call`；工具用真实函数写文件 |
| MCP | 直接调 `make_tools(session)[...]` 返回的函数（不走 stdio） |

8 个场景 × 三形态：并发写、同键重试、同键异内容、过期 token、预算耗尽+回收、协调器不可达、重启后 token 复用、路径逃逸。

### 12.6 风险与边界

| 风险 | 对策 |
|---|---|
| 宿主不用 `before_agent` | `wrap_tool_call` 里**懒建 token**（按 agent_id 复用），并在文档强调推荐用生命周期钩子 |
| 宿主不设置 `agent_id`（多节点图） | 默认退化为 `thread_id`；文档写明"多 agent 节点必须各自设置 context.agent_id" |
| 第三方工具没有 `root` 参数 | 无法重定向 → passthrough + 记 `unsupported`（不假装覆盖） |
| `after_agent` 在异常路径不触发 | TTL reaper（C4）兜底 |
| 中间件每次工具调用都建 worktree 的开销 | 用会话级 worktree（token 携带路径），不重复建 |
| MCP 单次 `rsc_apply` 的 worktree | 内部临时创建/销毁（不复用），适合单写工具 |

### 12.7 实施顺序与出口

1. C7 facade（+ token 索引 + 8 场景 facade 形态）
2. C6 的 `WorktreeManager` 作为 facade 依赖（已完成）
3. C8 LangGraph（sync/async + root 注入 + 读回 + 8 场景）
4. C9 MCP（4 工具 + resource + 8 场景）
5. 更新 `pyproject` extras、`harness/__init__.py` 导出 `RscSession`、S22 conformance 扩到 C7–C9

**出口**：8 场景 × 3 形态全绿；全量 679 → 679+N 零回归；`dependencies=[]` 不变。

---

## 13. C7–C9 实现结果（2026-10-28）

### 13.1 交付

| 编号 | 状态 | 落点 |
|---|---|---|
| C7 `RscSession` 六动作 + token 索引 | ✅ | [facade.py](harness/facade.py) |
| C8 LangGraph 中间件（sync/async、root 注入、读回） | ✅ | [integrations/langgraph.py](harness/integrations/langgraph.py) |
| C9 MCP server（6 工具 + resource） | ✅ | [integrations/mcp_server.py](harness/integrations/mcp_server.py) |
| extras / 导出 / conformance | ✅ | `pyproject.toml`、`harness/__init__.py`、`scripts/check_s22_conformance.py` |

### 13.2 验证

| 门禁 | 结果 |
|---|---|
| 全量测试（连跑 3 轮） | **719 passed / 4 xfailed** |
| S22 用例 | 67 个（core 20 + worktree 10 + facade 14 + langgraph 14 + mcp 11） |
| S22 一致性核对 | **156 / 0** |
| 历史核对 | S14–S21 全 0 失败 |
| 覆盖率 | 整体 **84.27%** ≥ 80，门禁通过 |

### 13.3 评审驱动的修复（第二、三轮对抗式评审）

| 级别 | 发现 | 修复 |
|---|---|---|
| P0 | token 索引存 begin 时的 ref，`finish` 后仍可用旧快照复活 token，并导致预算双计 | `resolve` 改用 `latest_ref_for_acb`；`apply` 后 `_resave`；TTL 校验 |
| P0 | 外部幂等键无命名空间 → 跨会话同键静默丢写 | 键改为 `{tenant}|{agent_id}|{key}` |
| P0 | MCP `rsc_begin(agent_id_)` 允许冒名 → 可销毁他人 worktree | 删除该形参，`agent_id` 只由 server 配置 |
| P0 | worktree 目录缺失时读回 `None` → 误判 DELETE 删账本 | 执行前 `_worktree_ok` 检查，缺失即 `RscDenied` |
| P1 | `cost=0/-1/True/1.5` → 免费写；多预留重复扣费 | `apply` 前置校验；`_consume` 只消费一笔 |
| P1 | `UPSTREAM_FAILED`（可重试）被硬 Deny；真 OCC 冲突被标成 key 复用 | 分别映射为 `Wait` / `Conflict("stale_write")` |
| P1 | `writes` 漏传 → 静默失去 OCC（懒建路径 100%） | 取消懒建（无会话即 `no_session` 拒绝）；未钉住资源 `unpinned` 拒绝 |
| P1 | `resolve` 恢复终态/`needs_readmit` 时不释放槽位 | 显式 `slots.release` |
| P1 | `begin` 被拒（Wait/Deny）留下僵尸 ACB | `_discard`：`cancel` + `abort` + `purge_run` |
| P1 | worktree 创建失败不回滚准入 | `begin` 内 try → `_discard` 后重抛 |
| P1 | `gate` 靠字符串前缀判 `stale_view` | `Deny.code` 类型化判定 |
| P1 | `root_arg` 与工具签名不匹配 → LangChain 静默吞参数 | `_accepts_root` 用 `inspect.signature` 校验，不接则 passthrough |
| P1 | 路径逃逸/IO 异常裸抛给宿主 | `_read_back` 包 `(ValueError, OSError)` → 拒绝 |
| P2 | `rsc_apply` 返回已失效 token | 移除该字段 |
| P2 | 幂等键用逗号拼接路径可碰撞 | 改 `\x1f` 分隔 |
| P2 | `session.h` 被适配器穿透 | 新增 `tick()/ledger_get()/ledger_version()` 公共方法 |

### 13.4 已知边界（如实记录）

| 边界 | 说明 |
|---|---|
| 必须用 `before_agent` | 中间件不再懒建 token；无会话 → `RscDenied(no_session)`（fail-closed） |
| 必须声明 `writes` | 未钉住的资源一律 `unpinned` 拒绝；这是有意的 fail-closed |
| 多 agent 需各自设 `context.agent_id` | 否则共享一个 token/worktree（失去隔离） |
| 第三方工具无 `root` 参数 | passthrough（不假装覆盖） |
| `after_agent` 异常路径 | 靠 TTL reaper（C4）兜底 |
| 外部副作用 exactly-once | 归宿主 outbox；`gate` 只校验读集是否过期 |

### 13.5 下一轮（S23）

- CrewAI（`scoped_hooks`）+ OpenAI Agents SDK（guardrails + MCP）+ AG2 + HTTP sidecar
- extras 打包与 3-agent 示例

### 13.6 真机 DAG（2026-10-28，DashScope）

`scripts/demo_dag.py` 3 节点（arch + api → summary）跑通真模型：

| 项 | 结果 |
|---|---|
| 模型 | `qwen3.7-flash`（OpenAI 兼容端点） |
| 每节点 | `begin` → 模型生成 → 写 Z3 → `apply` → `finish`，各得 1 张收据，`version=1` |
| A 并发写 | 先写者 `Receipt`、后写者 `Conflict`，账本保留 `racer-1 wins` |
| B 同键重试 | `receipt_id` 相同、`dedup=True` |
| C 忘记 finish | `tick()` 退回 20 预算并回收 token |
| 离线对照 | `--fake` 同三条保证全过 |

沙箱只有代理出口，而 `build_opener()` **按设计**禁环境代理（防钉住/凭证隔离失效）。
为此 `LLMClient`/`_post` 增加 `opener` 注入点（默认仍走钉住 opener），演示脚本用
`--via-proxy` 显式选择代理出口，并打印「绕过 DNS 钉住，仅演示用」提示。
**生产不得使用 `--via-proxy`**（见 §13.4 与 `harness/egress/resolver.py` 头注释）。
