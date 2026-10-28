# S23：集成面第二轮（CrewAI / OpenAI Agents / AG2 / HTTP sidecar）

| 项目 | 内容 |
|---|---|
| 版本 | **v0.1（调研完成，未实现）** |
| 日期 | 2026-10-28 |
| 上游 | [s22 集成面](s22-integration-surface.md) §2.2–2.6 / §6；[s22 实现计划](s22-implementation-plan.md) §13.5 |
| 目标 | 在**不改内核语义、`dependencies = []` 不变**的前提下，把 S22 的六动作门面接到另外 3 个宿主 + 1 个进程外协议 |
| 证据口径 | **优先运行实测**（Python 3.12 venv + 官方 PyPI）；沙箱主环境为 Python 3.14，见 §0 |

---

## 0. 环境与版本实测（先于一切结论）

| 事实 | 证据 |
|---|---|
| 沙箱主解释器是 **Python 3.14.8**；`crewai` 元数据 `Requires-Python <3.14,>=3.10` → **主环境装不上** | `importlib.metadata.metadata("crewai")` |
| 另建 `/tmp/s23venv`（Python 3.12.15）安装成功：`crewai 1.15.32`、`openai-agents 0.13.6`、`ag2 1.2.2` | `python -c "import crewai, agents, ag2"` |
| 内网镜像 `crewai` 最高 1.15.17 且缺 `cel-python>=0.5` → 必须 `--index-url https://pypi.org/simple`（经沙箱代理） | `pip install` 输出 |
| 主环境已有 `langgraph / langchain / mcp`，**没有** `crewai / agents / ag2` | `importlib.util.find_spec` |
| `openai-agents` / `ag2` 元数据 `Requires-Python >=3.10`，但**未在 3.14 实测** | `metadata("openai-agents")` |

**直接后果**：S23 的三个框架适配器测试在 CI 里必须 `pytest.importorskip`；若要真的跑通，
CI 需要 **Python 3.12 任务**（或容器镜像），否则只能做「已安装即跑、未装即 skip」。
这是发布层面的诚实边界，不能靠 mock 假装通过。

---

## 1. 能力矩阵（运行实测 + 源码定位）

安装路径下的相对引用：`crewai/`、`agents/`、`ag2/`（venv site-packages）。

| 宿主 / 拦截点 | 能否真正阻止执行 | tool_call_id | 能否读写上下文 | 证据 |
|---|---|---|---|---|
| **CrewAI `before_tool_call`** | **能，但只到「软阻断」**：`HookAborted`/返回 `False` → 工具不执行，但 agent 收到固定文案 `"Tool execution blocked by hook. Tool: X"`，**reason 被丢弃**，run 继续 | **无** | `tool_input` 可就地改；`agent/task/crew` 可读 | [实测] `run_before_tool_call_hooks` 结果：`raise→False`、`HookAborted→True`、`False→True`、`True→False`；`crewai/hooks/tool_hooks.py:173`、`crewai/agents/agent_executor.py:2218,2237` |
| **CrewAI 普通异常** | **fail-open**：hook 抛非 `HookAborted` 异常被吞掉，工具照常执行 | — | — | `crewai/hooks/dispatch.py:265` `_invoke_hook` 显式 swallow |
| **CrewAI 包装 `BaseTool.run`** | **能硬阻断**（工具根本不执行，异常转 `"Error executing tool: ..."` 回模型） | 无（只有入参） | 可挂类属性/外部映射 | `crewai/utilities/agent_utils.py:303` `available_functions[name] = tool.run` |
| **CrewAI `on(point, agents=, tools=)`** | 注册是**全局表 + 名字过滤**；`@CrewBase` 类内方法延迟到 crew 初始化注册 | — | — | `crewai/hooks/dispatch.py:400`；**1.15.32 无 `scoped_hooks`**（`crewai/hooks/__init__.py` 导出清单无此项） |
| **CrewAI 缓存命中** | 工具不执行，但 `before_tool_call` 已执行 | — | — | `agent_executor.py:2222-2236`（`from_cache` 分支在 hook 之后） |
| **OpenAI `tool_input_guardrail`** | **能硬拒绝**：`raise_exception` → `ToolInputGuardrailTripwireTriggered`；`reject_content` → 文本回模型、工具不执行 | **有**（`ToolContext.tool_call_id` 必填） | `ToolContext.context`（`RunContextWrapper.context`） | `agents/run_internal/tool_execution.py:2109-2140`；`agents/tool_context.py:42` |
| **OpenAI `tool_output_guardrail`** | 执行后拒绝：可改最终返回文本；**副作用已发生** | 有 | 同上 | `tool_execution.py:2143-2178` |
| **OpenAI `FunctionTool` 注入** | `dataclass`，`dataclasses.replace(tool, tool_input_guardrails=[...])` 实测可用，原对象不变 | — | — | [实测] `replace` 后 `on_invoke_tool` 同一 callable |
| **OpenAI `RunHooks.on_tool_start`** | 只观测，**不能阻断**（异常=崩溃，不是 Deny） | 有 | 有 | `agents/lifecycle.py`；签名实测 |
| **OpenAI MCP 工具** | 0.13.6：MCP→`FunctionTool` 转换点 `MCPUtil.to_function_tool` 可包 guardrail（私有面）；**HostedMCPTool 不走 guardrail** | 有 | 有 | `agents/mcp/util.py`；`agents/tool.py` |
| **AG2 `BaseMiddleware.on_tool_execution`** | **能短路**：不调 `call_next` 直接返回 `ToolResultEvent` 即不执行 | **有**（`ToolCallEvent.id` = uuid4） | `Context` | `ag2/middleware/base.py:105`；`ag2/events/tool_events.py:69`；[实测] `ToolResultEvent.from_call` 构造成功 |
| **AG2 装配策略 `AssemblyPolicy`** | 与预算无关（上下文组装），**不要**误用 | — | — | s22 结论保留 |

---

## 2. 对 S22 文档的三处修正（必须改口）

| # | S22 原说法 | 实测结论 |
|---|---|---|
| 1 | CrewAI 用 `scoped_hooks` 上下文管理器做 per-run 隔离 | **1.15.32 无此 API**。只有全局 `register_*` + `on(agents=/tools=)` 名字过滤 + `clear_*`。per-run 隔离只能靠**独立进程**或**包装工具**（推荐后者） |
| 2 | CrewAI 阻断是「软拒绝，reason 不透传」 | 前半对；但 **`HookAborted` 确实能让工具不执行**（`run_before_tool_call_hooks` 捕获后返回 True）。软的是**原因**与**是否终止 run**，不是「工具仍执行」 |
| 3 | OpenAI `Session` 不能承载版本快照 | 不变；补充：**`tool_call_id` 在 `ToolContext` 里是必填字段**，可用作幂等提示键（但业务键仍优先，见 §4） |

---

## 3. 接入设计（逐框架）

### 3.1 CrewAI（`harness/integrations/crewai.py`）

**主路径：包装工具，而不是依赖全局 hook。**

```python
from harness.integrations.crewai import rsc_tools

tools = rsc_tools(session, wraps={"write_file": ("docs/a.md",)},
                  tools=[write_file, search], agent_id="crew-arch")
agent = Agent(role=..., tools=tools)          # 传入包装后的工具
```

- 包装类 `RscTool(BaseTool)` 覆写 `run()`：`begin`（若无 token）→ `gate` → 调原工具 → 读回
  declared 路径 → `apply(业务键)`；`finish` 在 crew 结束（`after_kickoff` 或 contextmanager）。
- 为什么不用 hook：hook 拿不到 `tool_call_id`、异常 fail-open、reason 丢失、缓存命中时
  无法区分「真执行」。包装 `run()` 是**唯一同时满足**「硬阻断 + 能读回产物」的点
  （`available_functions = tool.run` 已实测）。
- `before_tool_call` 只用于**观测/输入净化**（可选），不承担正确性。
- **fail-closed**：已声明 writes 但无 token / worktree 缺失 / 路径逃逸 → 不执行，返回
  `"RSC_DENY:..."` 并记审计；未声明 writes 的工具**透传**并在结果里标 `unsupported`（不假装覆盖）。
- `HookAborted` 仅在需要终止整个 step 时使用（reason 仍会丢，属宿主限制）。

### 3.2 OpenAI Agents SDK（`harness/integrations/openai_agents.py`）

```python
from harness.integrations.openai_agents import rsc_guardrails, rsc_run

agent = Agent(name="arch", tools=rsc_guardrails(session, agent, writes={"write_file": ("docs/a.md",)}))
result = await rsc_run(session, agent, input="...", agent_id="arch")   # 外层 begin/finish
```

- `rsc_run` 在最外层 `begin`，结束时 `finish`（`RunHooks` 不能阻断，不能当 gate）。
- 每个 `FunctionTool` 用 `dataclasses.replace` 注入 input/output guardrail（实测可用）。
- input guardrail：`tool_call_id` 缺失 → **直接 `raise_exception`（fail-closed）**；有则
  `gate`，`Deny` → `raise_exception`，`Wait` → `reject_content`（让模型排队/换策略）。
- output guardrail：读回 + `apply(idempotency_key=业务键)`；`Conflict` → `reject_content`
  并把 stale 版本写进文本；`Deny` → `raise_exception`。
- MCP：**优先用我们自己的 MCP server**（[mcp_server.py](harness/integrations/mcp_server.py)）；
  0.13.6 的客户端 guardrail 覆盖不到 HostedMCPTool，文档写明。

### 3.3 AG2（`harness/integrations/ag2.py`，beta）

```python
from harness.integrations.ag2 import RscToolMiddleware

agent = Agent(name="arch", tools=[...], middleware=[RscToolMiddleware(session, writes={...})])
```

- `on_tool_execution(call_next, event, context)`：`event.id` 可作 replay 提示；
  不调 `call_next`、直接返回 `ToolResultEvent.from_call(event, "RSC_DENY:...")` 即短路
  （实测可构造；是否被 agent 记为成功需 S23-0 验证）。
- `Deny` → 返回 `ToolResultEvent`（内容为 typed reason）；`Wait` 同理。
- 版本锁定 `ag2>=1.2,<2`：1.0→1.2 导入路径已变（`autogen.beta` → `ag2`），**不加 Classic**。

### 3.4 HTTP sidecar（`harness/integrations/http_sidecar.py`，纯 stdlib）

**协议最小集**（与门面一一对应）：

| 动作 | 请求 | 成功 | 失败映射 |
|---|---|---|---|
| `begin` | `POST /begin` `{intent_key, writes, reads}` | `200 Token{token, base_versions}` | `Wait→429`（带 `Retry-After`）；`Deny→403` |
| `apply` | `POST /apply` + `Idempotency-Key` 头 | `200 Receipt{receipt_id, dedup}` | `Conflict→409`；`Wait→429`；`Deny→403` |
| `gate` | `POST /gate` `{token}` | `200 {allow:true}` | `409 {code:"stale_view"}` |
| `finish` | `POST /finish` `{token, outcome}` | `200` | `404/410`（已终态） |
| `abort` | `POST /abort` `{token, reason}` | `200` | `404/410` |
| `snapshot` | `GET /snapshot/{token}` | `200 {resource:version}` | `404/410` |

**硬约束（实测 `http.server` 默认值）**：
- 默认 `protocol_version = HTTP/1.0` → 必须显式设 `HTTP/1.1` + 每响应 `Content-Length`，
  否则 keep-alive 不成立；
- **无请求体上限、不支持 chunked**：必须自己限 `Content-Length ≤ 1 MiB`（超出 `413`）；
  无 `Content-Length` 且非 chunked → `411`；
- `ThreadingHTTPServer.daemon_threads=True`，但**线程/请求无上限** → 自加信号量（≤32）
  并把 `request_queue_size` 调到 64；
- **不做长轮询 / SSE / TLS**：`gate` 是即时版本比较，长轮询只会挂死线程；用短轮询 +
  `Retry-After`。
- 鉴权：**每个 agent 一把密钥**（`{key: agent_id}`），逐个 `hmac.compare_digest`
  比较；缺失密钥 `401`、未知密钥 `403`。**`agent_id` 绝不作为请求参数**（它参与
  幂等命名空间与 worktree 归属，冒名会命中他人收据/删他人目录），只从密钥映射推导。
- 每个请求调 `session.tick()`（与 MCP 一致），保证 reaper 在无后台线程时也推进。
- 不交付 FastAPI 双实现（避免协议漂移）；确需 ASGI 时再开 `sidecar-fastapi` extra。

---

## 4. 幂等键：统一业务键，`tool_call_id` 只作提示

```
idempotency_key = sha256(f"{tenant}|{agent_id}|{resource}|{intent_key}")
```

| 宿主 | 可取信息 | 结论 |
|---|---|---|
| LangGraph（S22） | `tool_call.id` | 仍用业务键（已实现） |
| CrewAI | **无** call id；有 `tool_name + tool_input + task.id + agent.role` | 业务键 + `task.id` + `sha256(canonical args)`；同任务同参重复调用**不可区分**（诚实边界） |
| OpenAI | `ToolContext.tool_call_id` | 业务键优先；`tool_call_id` 写进审计事件用于 replay 定位 |
| AG2 | `ToolCallEvent.id` | 同上 |
| HTTP | `Idempotency-Key` 头 | 直接透传到 `apply`（服务端还会再加 `{tenant}|{agent}` 命名空间） |

---

## 5. 打包与示例

| 新 extra | 依赖 | 说明 |
|---|---|---|
| `crewai` | `crewai>=1.15,<2` | 主环境（3.14）装不上 → 测试 `importorskip` |
| `openai-agents` | `openai-agents>=0.13,<1` | 0.13.6 实测；0.24.x 的差异见 §8 |
| `ag2` | `ag2>=1.2,<2` | 1.2.2 实测；不加 Classic |
| `integrations` | 以上三者聚合 | 对齐现有 `egress` 聚合写法 |

- **不建空 `sidecar` extra**：纯 stdlib 无依赖。
- 示例放 `examples/three_agents/`（`run.py` + `README.md`，fake-LLM、无密钥可跑）；
  `scripts/` 保持 conformance/repro 定位。`testpaths=["tests"]` 不会收集 `examples/`。
- 新增 marker（如 `crewai` / `agents` / `ag2` / `sidecar`）必须登记到
  `pyproject.toml` 的 `markers`（`--strict-markers` 会拦住未登记项）。

---

## 6. 分轮实现计划（每轮独立可验收）

| 轮 | 交付 | 状态（2026-10-28） |
|---|---|---|
| **S23-0** 验证基线 | 3.12 venv 内运行期验证 §1 的阻断语义（`HookAborted→blocked`、普通异常 `→fail-open`、openai guardrail、AG2 短路） | ✅ 结论并入 §1；venv 保留在 `/tmp/s23venv` |
| **S23-1** CrewAI 适配器 | `rsc_tools` 包装 `BaseTool.run` + `writes_map` + `root` 注入 | ✅ [crewai.py](harness/integrations/crewai.py)，9 用例 |
| **S23-2** OpenAI 适配器 | `rsc_agent`（guardrail + root 注入 + schema 去 root） | ✅ [openai_agents.py](harness/integrations/openai_agents.py)，9 用例 |
| **S23-3** AG2 适配器 | `rsc_middleware`（`on_tool_execution` 短路） | ✅ [ag2.py](harness/integrations/ag2.py)，7 用例 |
| **S23-4** HTTP sidecar | stdlib server + 6 端点 + 每 agent 密钥 + 体积/并发上限 | ✅ [http_sidecar.py](harness/integrations/http_sidecar.py)，14 用例 |
| **S23-5** 打包/示例/文档 | extras（`crewai`/`openai-agents`/`ag2`/`integrations`）+ `examples/three_agents/`（naive 丢写 vs RSC 保全 + HTTP + 预算）+ `check_s23_conformance` | ✅ 示例可跑；一致性核对 **80/80** |

**门禁**：主环境（3.14）**733 passed / 3 skipped / 4 xfailed**（3 个框架文件 `importorskip`）；
3.12 venv **39 passed**（4 个 S23 文件全跑）；`ruff check .` 0 errors；S23 核对 **80/80**。

**统一 6 场景**（沿用 S22）：并发写冲突、同键重试同收据、同键异内容 CONFLICT、
租约过期旧 token 被拒、预算耗尽 WAIT + reaper 回收、协调器不可达 fail-closed。

### RIPR（每个适配器至少这 4 条）

| 维度 | 用例 |
|---|---|
| Reachability | 工具调用确实走到我们的拦截点（计数/审计事件） |
| Infection | 注入 stale/deny 条件后状态真的改变 |
| Propagation | 拒绝原因以**宿主可见**的形式出现（文案/异常/ToolMessage） |
| Revealability | 断言收据 ID / 版本 / 预算余额，而不是只看「没抛异常」 |

### 必杀变异（写进 conformance 脚本）

1. 删除 HTTP 鉴权 → `test_http_requires_auth` 必须失败；
2. 允许 `agent_id` 作请求参数 → 冒名用例必须失败；
3. CrewAI 包装器跳过 `apply`（只跑工具）→ 幂等/版本用例必须失败；
4. OpenAI input guardrail 在缺 `tool_call_id` 时放行 → fail-closed 用例必须失败；
5. AG2 调用了 `call_next` 再判 Deny → 「副作用未发生」用例必须失败；
6. sidecar 去掉 1 MiB 上限 → 413 用例必须失败。

---

## 7. 风险与不做

| 风险 | 对策 |
|---|---|
| 3.14 主环境装不上 crewai | 适配器 `importorskip` + 文档写明；CI 增 3.12 任务（否则只能「装了才测」） |
| CrewAI hook 异常 fail-open | **不用 hook 承载正确性**；正确性在 `RscTool.run` |
| CrewAI 同任务同参不可区分 | 业务键 + 明说「无法区分同参重复」 |
| OpenAI 0.24.x API 漂移 | 锁 `>=0.13,<1`；适配器只依赖 `FunctionTool` dataclass 字段与 guardrail 协议（0.13 已验证） |
| AG2 beta 漂移 | 锁 `>=1.2,<2`；只依赖 `BaseMiddleware` + `ToolCallEvent.id` |
| sidecar 线程耗尽 | 信号量 + 队列 + 超时；明说非生产级 |
| 宿主绕过我们写文件 | **不保证**（诚实边界不变） |

**不做**：CrewAI Classic（`autogen 0.14`）、`scoped_hooks`（不存在）、长轮询/SSE/TLS、
FastAPI 双实现、把 `agent_id` 当请求参数、接管宿主编排/LLM。

---

## 8. 待确认（实现前需要你拍板）

| # | 问题 | 建议 |
|---|---|---|
| Q1 | CrewAI 阻断策略 | **包装 `BaseTool.run`** 为主（硬阻断 + 可读回），`HookAborted` 仅作整步终止 |
| Q2 | OpenAI 版本基线 | 以 **0.13.6** 为验证基线，锁 `>=0.13,<1`；不追 0.24 的 `ToolExecutionConfig` |
| Q3 | AG2 支持级别 | 以 **1.2.x middleware** 为准，标 beta；不承诺 Classic |
| Q4 | sidecar 依赖 | 纯 stdlib；不建 FastAPI 双实现 |
| Q5 | CI | 增 **Python 3.12** 任务跑三个框架适配器；3.14 任务只跑 skip |

---

## 9. 对抗式评审与修复（2026-10-28）

评审在 3.12 venv 实跑 22 条适配器用例并做针对性实证，结论落地如下：

| 级别 | 发现 | 修复 |
|---|---|---|
| P0 | sidecar 只校验密钥→agent，**不校验 token 归属** → K2 可 apply/finish K1 的 token，写他人账本并销毁其 worktree | 门面新增 `owner_of`；五个端点（apply/gate/finish/abort/snapshot）不符即 `403 token_owner_mismatch`；用例 `test_token_owner_cannot_be_taken_over` |
| P1 | `/apply` 可写 `begin` 未声明的资源（无 base_version → 静默 last-write-wins） | 门面 `begin` 记 `spec.rsc_writes`，`apply` 校验子集 → `Deny(undeclared_write)`；sidecar 映射 `400`；用例 `test_apply_rejects_undeclared_resource` |
| P1 | OpenAI 工具抛错时 SDK **不跑 output guardrail** → token/预算/槽位泄漏到 TTL | `invoke` 捕获后 `_abandon`（pop + abort）再重抛；用例 `test_tool_exception_aborts_token` |
| P1 | OpenAI 幂等键直接哈希原始 JSON 字符串（空白差异 → 不同键） | 先 `json.loads` 再 `canonical_args`；解析失败 `trip`；用例 `test_arguments_are_canonicalized_for_idempotency` |
| P1 | CrewAI/AG2 键把将被覆盖的 `root` 算进去（换 root 同内容不去重） | 算键前剔除 `root_arg`；两框架各加用例 |
| P1 | sidecar 信号量在 `tick()` 抛异常时泄漏；连接无读超时 | `_guard` 异常路径释放；handler `timeout=10`；用例 `test_tick_failure_does_not_leak_inflight` |
| P2 | sidecar `reads` 未逐项校验 → facade `set()` TypeError（断连而非 400） | 逐项 `isinstance(str)` → `400 bad_reads`；用例 `test_bad_reads_is_400` |
| P2 | `request_queue_size` 在 `listen()` 后赋值无效 | 用 `_SidecarServer` 子类在 bind 前设 64 |
| P2 | CrewAI 缓存在 `tool.run` 之上命中 → wrapper 不执行、账本无提交 | `cache_function=_never_cache`（模块级函数，避免序列化告警） |
| P2 | 测试覆盖名不副实（`shared_cwd` 未证明工具写入） | 共享 cwd 用例改为断言「工具实际产物被读回」；框架适配器测试统一 `importorskip`，CI 需 3.12 任务（Q5） |
