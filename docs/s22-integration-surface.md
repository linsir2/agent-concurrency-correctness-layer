# S22：被集成方案（Integration Surface）

| 项目 | 内容 |
|---|---|
| 版本 | **v0.2（评审后重写）** |
| 日期 | 2026-10-28 |
| 上游 | [定位与路线图 v2.0](positioning-and-roadmap.md) §5.2 / §6.2；[设计规范 v1.12](multi-agent-harness-design-final.md) §4.1（L4 不是产品）、§14 MCP |
| 覆盖 | 我们如何被 LangGraph / CrewAI / OpenAI Agents SDK / AG2 / 任意 MCP 客户端 / HTTP 宿主集成 |
| 硬约束 | 不改内核语义；`dependencies = []` 不变（适配器全放 extras）；协调器不可达 **fail-closed** |
| 评审 | 两路对抗式评审（外部可行性 + 内部可落地性），修复 9 个 P0 后重写；记录见 §9 |

---

## 0. 调研结论

### 0.1 内部接缝（代码实况）与**五个内核缺口**

| 接缝 | 位置 | 用途 |
|---|---|---|
| 全组件构造注入 | [harness.py:185](harness/harness.py#L185) | 宿主可把后端换成我们的 |
| 唯一写入口 | [harness.py:1022](harness/harness.py#L1022) `commit_batch(acb, writes, mode)` | 提交点顺序固定 |
| 暂存 | [workspace.py:246](harness/workspace.py#L246) `stage(acb, resource, content) -> Result` | 候选改动先暂存 |
| 准入 | [orchestration.py:503](harness/orchestration.py#L503) `try_admit(task, run_id, ...)` | 需完整 `task`（非 `agent_id+intent`） |
| 入口幂等 | [ingress.py:83](harness/ingress.py#L83) `submit(idempotency_key=..., plan=...)` | **只作用于建 Run，不作用于 commit** |
| 可观测 | [harness.py:2993](harness/harness.py#L2993) `export_observability(sink)` | span 导出 |
| 生产入口 | [harness.py:4601](harness/harness.py#L4601) `create_production_harness` | egress fail-closed |

**评审确认的五个内核缺口（必须先补，否则门面落不了地）**：

| # | 缺口 | 证据 | 后果 |
|---|---|---|---|
| G1 | `commit_batch` **不消费预算**；结算只在 `_finalize`（[harness.py:3632](harness/harness.py#L3632) `budget.settle` + `:3642` `slots.release`） | 读代码 | 只做 `begin/commit` → 预算与槽位**永久泄漏** |
| G2 | 无公共 `abort(acb)`：`_rollback(acb, node, resv, declared)` 需要 `declared`，而它只是 `_try_admit_commit` 的局部变量（[orchestration.py:700](harness/orchestration.py#L700)） | 读代码 | 外部无法安全撤销 |
| G3 | 外部 `idempotency_key` 未贯通：`_commit_key` 是内容哈希（[harness.py:750](harness/harness.py#L750)），`ingress` 的 IETF 语义与 commit 无关 | 读代码 | 「同 key 异内容 → CONFLICT」**不存在** |
| G4 | `token = acb.id` 只在内存（`h.acbs` 是 dict，[harness.py:422](harness/harness.py#L422)）；重启后不可解析 | 读代码 | 跨进程 / MCP 场景 token 失效 |
| G5 | 无异常 → 拒绝映射：Raft 不可用时 `try_admit` 在 `flush_pending` **抛异常**（[harness.py:571](harness/harness.py#L571)） | 读代码 | §4 的 fail-closed 不成立 |

### 0.2 外部口子（**按实装版本核实**，非文档转述）

核实版本：`langgraph 1.1.10` / `langgraph-checkpoint 4.3.1` / `langchain 1.2.15` / `crewai 1.15.32` / `openai-agents 0.13.1` / `mcp 1.30.0`。

| 宿主 | 真实可插入点 | 关键限制（评审纠正） |
|---|---|---|
| **LangGraph** | `create_agent(middleware=[...])`；`AgentMiddleware.wrap_tool_call` **与** `awrap_tool_call`；`BaseCheckpointSaver`；`BaseStore` | ① `BaseStore` 的抽象方法只有 **`batch` / `abatch`**（`get/put/search` 是基类基于 `batch` 的具体实现）→ 只写三个方法**实例化即 TypeError**；② 只写同步钩子，在 `ainvoke/astream` 下**直接 NotImplementedError**；③ `wrap_tool_call` 只能看到 `tool_call{name,args,id}` 与 `ToolMessage`，**看不到工具内部写了哪个 resource** |
| **CrewAI** | `@on(PRE_TOOL_CALL)` / `POST_TOOL_CALL`；`ToolCallHookContext`；`HookAborted`；per-run 用 `scoped_hooks(...)` | ① **没有 `tool_call_id`**（字段只有 `tool_name/tool_input/tool/agent/task/crew/tool_result`）；② `Crew(...)` **没有 `hooks=` 参数**；③ 阻断是**软拒绝**：返回固定文案 `"Tool execution blocked by hook..."`，reason 不透传，agent 继续跑 |
| **OpenAI Agents SDK** | `tool_input_guardrails`（FunctionTool 执行前）；`tool_output_guardrails`；`RunHooks`（只观测）；MCP 工具 | ① `Session` 是**会话历史**（4 个 async 方法 + `session_settings`），**不能**承载资源版本快照；② guardrail **只对 FunctionTool 生效**，MCP 工具走不到 → 必须由我们 server 端 gate 兜底 |
| **AG2** | `on_tool_execution`（beta，async，可短路/改写入参） | `AssemblyPolicy` 是**上下文组装策略**（compact/aggregate），**不是预算树**；beta 通道需锁版本，且只承诺 AG2 不是 MS AutoGen |
| **MCP** | `FastMCP.tool(name=..., description=...)`、`resource(uri, ...)`、`Context`；stdio / Streamable HTTP（SSE 已 deprecated） | ① `resource("rsc://snapshot/{agent_id}")` 会变成 **resource template**，且很多客户端不主动读 resources → 需同时给工具；② 模型**不会**可靠地按 5 步顺序调用 |
| **HTTP 宿主（遗漏项）** | 任意语言：`POST /begin /apply /gate /snapshot /abort` | 成本最低、覆盖面最广；`ingress.py` 的 IETF 幂等语义可直接复用 |
| **CI / merge-gate（遗漏项）** | PR 检查：跑一致性语料 | 最便宜的分发，与 S24 corpus 天然复用 |
| **MS Agent Framework / SK（遗漏项）** | Function calling middleware + Agent Hooks + Context provider | 自家 roadmap §5.2 已列为最佳插入点，本方案 v0.1 整段漏掉 |

### 0.3 竞品作业：agent-coherence（事实核实 + 差异化修正）

核实（PyPI v0.15.0）：
- extras：`[langgraph] [crewai] [openai-agents(experimental)] [diagnose] [mcp] [conformance] [all]`。
- `CCSStore` **只做读侧**（peer 提交后失效本地缓存），官方原话「`put` is not version-CAS」；写侧走 `write_cas` / `CoherentVolume`。
- 已有：`gate()` effect fence（MCP `swg_gate` + HTTP `/hooks/effect-fence`）、`atomic_publish`、多 artifact snapshot session、9 条 TLA+ 不变量、conformance corpus、`ccs-diagnose`。
- 单主机；跨主机在 roadmap。

> **v0.1 的差异化表述是错的**：写侧 CAS / effect gate 竞品已有。**真正的差异化只剩五项**：
> **① 准入 GRANT/WAIT/DIE ② 预算树 ③ 幂等对账（EffectJournal 三态） ④ 跨主机 Raft ⑤ 签名收据链**。

### 0.4 形态取舍（修正后）

| 形态 | 覆盖 | 前提 | 结论 |
|---|---|---|---|
| **A 注入**（Store/Checkpointer/Session） | 只覆盖状态读写 | 必须做**写侧 CAS** 才有意义 | **S22 不做**（Store 需 `batch/abatch`；Session 语义错配） |
| **B 拦截**（middleware / tool hook） | 工具调用边界 | **工具必须声明 writes**，否则 commit 空转 | **主力** |
| **C 出进程**（MCP / HTTP sidecar） | 任何宿主 | 服务端必须自带幂等 + reaper | **分发** |
| **D 事后校验**（CI merge-gate） | 合并前 | 只读语料 | **S24** |

**结论：B + C 双轨；A 暂缓；D 后置。**

---

## 1. 集成契约（修正版：可落地）

**v0.1 的错误**：`commit(token, writes)` 假设中间件知道"写了什么"，而 `wrap_tool_call` 根本看不到。修正为「**工具声明 writes，门面做 apply**」。

```python
# ① 准入：意图 → 计划（服务端生成 run_id/task_id/subgoal，不暴露给模型）
begin(intent, *, tenant=None, min_budget=1, writes=()) \
    -> Token(token, base_versions) | Wait(reason) | Deny(reason)

# ② 应用：stage + commit + consume + 收据，**一个幂等动作**
apply(token, writes, *, idempotency_key, cost=1) \
    -> Receipt(receipt_id, seq) | Conflict(stale, retryable) | Deny(reason)

# ③ 副作用闸门：外部副作用（发邮件/开 PR/调 API）前再验一次
gate(token, effect) -> Allow | Deny(stale_view | lease_expired)

# ④ 结束：结算预算 + 释放槽位 + 归档节点（**必须有，否则泄漏**）
finish(token, *, outcome="DONE") -> None

# ⑤ 中止：逆序释放（逻辑 → 预留 → 节点 → 槽位）
abort(token, reason) -> None

# ⑥ 读侧快照：版本而非内容（内容由宿主自己取）
snapshot(token) -> {resource: version}
```

**与现有实现/新增项的映射**：

| 动作 | 复用 | 必须新增 |
|---|---|---|
| `begin` | `Planner.plan` + `try_admit` | **intent→spec 映射**（`run_id=uuid`、`task_id=agent_id`、`logical_subgoal_id=intent_key`、`min_budget`、`writes→committed`）；异常→Deny（G5） |
| `apply` | `workspace.stage` + `commit_batch` | 外部 `idempotency_key` 贯通到 `_commit_key`/`_dedup_event`/收据 + CONFLICT 注册表（G3）；`budget.consume`（G1） |
| `gate` | `intents.is_valid` + `workspace.version` | **新写**（v0.1 说"已有"是错的；sync 模式 claim fencing 恒 False） |
| `finish` | `_finalize` 的结算分支 | 公共 `finish(acb)` 包装（G1） |
| `abort` | `_rollback` | 公共 `abort(acb)`；把 `declared` 记到 ACB 上（G2） |
| `snapshot` | `acb.base_version` | **不能用 `context_view`**（它返回内容且含 `_private` 未提交内容） |

**token 生命周期（G4）**：门面维护 `token -> (run_id, ref)` 持久映射（复用 checkpoint store）；`recover()` 后重绑定；token 过期时间 = `max_lease_ttl_s`，过期由 reaper 回收（释放预留/槽位并写审计事件）。

**幂等键（修正）**：**不用** `tool_call_id`（LangGraph 的 id 由模型生成、CrewAI 没有），改用业务键：

```
idempotency_key = sha256(f"{tenant}|{agent_id}|{resource}|{intent_key}")
```

`intent_key` 由工具显式声明（`@rsc_intent("...")` 或工具名→键映射）。`tool_call_id` 只用于同一次调用内的 replay 提示。

---

## 2. 适配器映射（修正版）

### 2.1 LangGraph（S22 首个）

```python
# 工具必须声明它写哪些资源（否则中间件无法 commit）
@rsc_writes("src/auth.py")
def edit_auth(...): ...

agent = create_agent(model="...", tools=[...], middleware=[RscMiddleware(harness)])
```

`RscMiddleware` 必须**同时**实现 `wrap_tool_call` 与 `awrap_tool_call`：

```
request -> writes = tool.rsc_writes            # 无声明 → 直接放行（记为 unsupported）
        -> token = begin(intent, writes=writes)
        -> gate(token, effect)
        -> handler(request)
        -> apply(token, [Write(r, result) for r in writes], idempotency_key)
        -> finish(token)
```

失败映射：
- `Wait`（预算不足）→ 返回 `ToolMessage(status="error", content="RSC_WAIT: ...")`，**不抛**（可排队）。
- `Deny`（能力/租约/协调器不可达）→ 抛 typed error（宿主可见、终止）。
- `Conflict` → 返回 `Command(update={"messages": [ToolMessage(...)]})`，**必须绑定同一 `tool_call_id`**，否则 `ToolNode` 校验失败。

**不做**：`RscStore`（需 `batch/abatch` 且只读侧无价值）、接管 `BaseCheckpointSaver`（语义不等价：我们存 ACB，不存 graph channel）。

### 2.2 CrewAI（S23）

```python
from harness.integrations.crewai import rsc_hooks
with rsc_hooks(harness, writes_map={...}):     # scoped_hooks 上下文管理器
    crew.kickoff()
```
- `PRE_TOOL_CALL` → `begin + gate`；`POST_TOOL_CALL` → `apply + finish`。
- 无 `tool_call_id` → 幂等键用业务键。
- 阻断是**软拒绝**：把 typed reason 编进文案（`RSC_WAIT: budget` / `RSC_DENY: capability`），并承认"模型可能重试"。
- 注意 `crew.cache` 命中时工具不执行但 PRE hook 已跑 → 按"是否真的执行"判定。

### 2.3 OpenAI Agents SDK（S23）

- **不用** `Session` 做快照（语义错配）。
- `tool_input_guardrails` → `gate`（仅 FunctionTool）。
- MCP 工具走我们自己的 MCP server 兜底（客户端 guardrail 覆盖不到）。
- token/base_versions 放 `RunContext`。

### 2.4 AG2（S23，beta）

`on_tool_execution(call_next, event, context)` 包住工具；**不用** `AssemblyPolicy` 承载预算；锁版本。

### 2.5 MCP server（S22 第二个）

**收敛为 3 个工具**（避免"模型按 5 步顺序调用"的可靠性陷阱）：

| 工具 | 语义 |
|---|---|
| `rsc_apply(intent, resource, content, idempotency_key)` | 服务端内部完成 `begin→gate→apply→finish`，一次调用一个幂等单元 |
| `rsc_gate(token, effect)` | 副作用前校验（高级用法） |
| `rsc_snapshot(token)` | 版本快照（同时保留 `rsc://snapshot/{token}` 资源模板） |

服务端**强制**兜底（不能寄托模型行为）：
- `begin` 幂等：同 `idempotency_key` → 同 token；异 intent → `CONFLICT`（复用 `Ingress.lookup` 语义）。
- token TTL + reaper：过期释放预留/租约/槽位 + 审计事件。
- `agent_id` **由服务端从连接主体推导**，不接受调用方声明（防冒名，I4）。

### 2.6 HTTP sidecar（S23，低成本高覆盖）

`POST /begin /apply /gate /snapshot /abort /finish`，`Idempotency-Key` 头直接复用 `ingress` 语义；任何语言宿主可用。

---

## 3. S22 范围（收缩后）

| # | 交付物 | 说明 |
|---|---|---|
| 1 | **G1–G5 内核补齐** | `finish(acb)`、`abort(acb)`、`idempotency_key` 贯通、token 持久映射、异常→Deny |
| 2 | `harness/facade.py`：`RscSession` | 六动作门面（薄封装） |
| 3 | `Harness.run_plan(plan, decisions_provider)` | **自写驱动循环**（不能只复用 `_finalize`：`_drain_scheduler` 用 `decisions=None`，后继会空跑成 DONE） |
| 4 | `harness/integrations/langgraph.py` | `RscMiddleware`（sync + async）+ `@rsc_writes` |
| 5 | `harness/integrations/mcp_server.py` | 3 tools + 1 resource + reaper |
| 6 | `tests/test_s22_*.py` | **同一套 6 场景**跑 facade / LangGraph / MCP |

**6 个适配器一致性场景**：

1. 两 agent 并发写同一资源 → 一成功一 `Conflict`（不丢写）。
2. 同 `idempotency_key` 重试 → 同一收据（不重复副作用）。
3. 同 key 异内容 → `CONFLICT`（IETF 语义）。
4. 租约过期后旧 token 提交 → `Deny(stale)`（fencing）。
5. 预算耗尽 → `Wait`；**未 `finish` 的 token 被 reaper 回收后预算恢复**。
6. 协调器不可达 → 全部 `Deny`（fail-closed）。

---

## 4. 失败语义（写死）

| 情形 | 行为 | 实现点 |
|---|---|---|
| 协调器不可达 / 无 quorum | `Deny` | 门面包 `except (RaftUnavailable, NotLeader)`（G5） |
| 同资源并发写 | `Conflict(stale, retryable)` | `commit_batch` 现有 CAS |
| 同 key 异内容 | `CONFLICT` | 新增 key 注册表（G3） |
| 能力未签发 | `Deny` | `_capability_ok` |
| 预算不足 | `Wait` | `budget.reserve` 返回 None |
| token 过期未 finish | reaper 回收 + 审计 | 新增 |
| 宿主绕过我们写 | **不保证** | 诚实边界 |

---

## 5. 明确不做

| 项 | 理由 |
|---|---|
| 编排图 / 角色 DSL / handoff | 宿主地盘（红线 1/5） |
| LLM 调用 / prompt 管理 | 宿主地盘（红线 4） |
| Trace UI / eval 平台 | 红线 2 |
| `RscStore`（Store drop-in） | 需 `batch/abatch`，且只读侧防不住 lost update |
| 接管宿主 `BaseCheckpointSaver` | 语义不等价（ACB ≠ graph channel） |
| 用 `Session` 承载版本快照 | 语义错配 |
| 字符级 CRDT | 收敛 ≠ 语义正确 |
| A2A 消息总线 | 非并发正确性 |

---

## 6. 里程碑

| 批次 | 范围 | 出口条件 |
|---|---|---|
| **S22** | G1–G5 + facade + `run_plan` + LangGraph（sync/async）+ MCP（3 工具 + reaper） | 6 场景 × 3 形态全绿；全量测试不回归 |
| **S23** | CrewAI（scoped_hooks）+ OpenAI（guardrails + MCP）+ AG2 + HTTP sidecar + extras 打包 | 6 场景 × 5 形态全绿；一个 3-agent 示例 |
| **S24** | conformance corpus 对外发布 + `rsc-diagnose` CLI + CI merge-gate + TLA+ 对齐 | 第三方可只用 corpus 自测 |

---

## 7. 风险与对策

| 风险 | 对策 |
|---|---|
| 宿主 API 高频漂移 | 适配器薄 + 锁版本 + 每个适配器一组契约测试 |
| 工具不声明 writes → commit 空转 | `@rsc_writes` 强制；未声明则**明确放行并记 `unsupported`**（不假装覆盖） |
| 长驻预留泄漏 | `finish` 必调 + token TTL reaper + 预算/槽位审计事件 |
| 双重持久化 | 宿主管图状态，我们管共享资源 + 副作用 |
| 与 agent-coherence 同质化 | 差异化锁定：准入 + 预算 + 幂等对账 + 跨主机 Raft + 收据（**不是**写侧 CAS） |
| 被误认为编排框架 | README/适配器首行写明定位 |

---

## 8. 决策

| # | 结论 |
|---|---|
| Q1 | 形态：**B 拦截 + C 出进程**；A 暂缓；D 后置 |
| Q2 | 契约：6 动作（`begin/apply/gate/finish/abort/snapshot`），`apply` 合并 stage+commit |
| Q3 | 幂等键用**业务键**，不用 `tool_call_id` |
| Q4 | 首个适配器 LangGraph（sync + async 双实现）+ MCP（3 工具） |
| Q5 | 必须先补 G1–G5，否则门面会泄漏预算/槽位 |
| Q6 | MCP 不暴露 5 步流程，收敛为幂等 `rsc_apply` + 服务端 reaper |

---

## 9. 评审修复记录（v0.1 → v0.2）

| 级别 | v0.1 的问题 | v0.2 修复 |
|---|---|---|
| P0 | `BaseStore` 只写 `get/put/search` 会实例化失败（抽象方法是 `batch/abatch`） | 删掉 Store drop-in，列入「不做」 |
| P0 | CrewAI 无 `tool_call_id`、`Crew(hooks=)` 不存在 | 改业务键 + `scoped_hooks` 上下文管理器 |
| P0 | 只写同步 `wrap_tool_call`，异步下抛 NotImplementedError | 契约强制 sync + async 双实现 |
| P0 | `commit` 不知道"写了什么"（中间件看不到 resource） | 引入 `@rsc_writes` 声明；未声明明确 `unsupported` |
| P0 | `commit` 不消费预算、不释放槽位 → 泄漏 | 新增 `finish`；`apply` 内 `budget.consume` |
| P0 | `run_plan` 复用 `_finalize` 会让后继空跑成 DONE | 自写驱动循环 + `decisions_provider` |
| P0 | 外部 `idempotency_key` 未参与 commit 去重 | 贯通 `_commit_key`/`_dedup_event`/收据 + CONFLICT 注册表 |
| P0 | `abort` 拿不到 `declared` | 新增公共 `abort(acb)`，`declared` 记到 ACB |
| P0 | MCP 5 工具依赖模型顺序 | 收敛为 `rsc_apply` + 服务端 reaper + begin 幂等 |
| P1 | `Session` 语义错配 | 改用 guardrails + RunContext + MCP |
| P1 | `gate` 被当成"已有语义" | 标记为新写（sync 模式 claim fencing 恒 False） |
| P1 | 差异化写成"写侧 CAS" | 改为「准入 + 预算 + 幂等对账 + Raft + 收据」 |
| P1 | 遗漏 HTTP sidecar / CI merge-gate / MS Agent Framework | 分别纳入 S23 / S24 / S23 |
| P1 | AG2 `AssemblyPolicy` 被当成预算树 | 更正为上下文组装策略 |
| P1 | `snapshot`/`base_versions` 用 `context_view` | 改用 `acb.base_version` |
