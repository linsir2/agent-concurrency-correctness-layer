# 三 agent 并发示例：没有 RSC vs 有 RSC

这个目录用**一份代码**回答两个问题：

1. **适配器到底是什么？**
2. **这个项目到底起了什么作用？**

运行（无需 API key，仅标准库 + git）：

```bash
python3 examples/three_agents/run.py
```

---

## 一、适配器是什么

宿主框架（LangGraph / CrewAI / OpenAI Agents / AG2 / 任意语言）负责**编排、LLM、记忆**，
但它们**不保证工具调用之间的并发正确性**。适配器是夹在「宿主的工具调用」与
「我们的六动作门面」之间的一层薄翻译器：

```text
宿主框架（负责编排 / 调模型）
   │  工具调用（写文件、发 PR、扣款…）
   ▼
[适配器]  ── 翻译 1：宿主事件 → begin / gate / apply / finish
   │      ── 翻译 2：Receipt / Conflict / Wait / Deny → 宿主能懂的错误或消息
   ▼
RSC 门面（RscSession）── 账本、版本、预算、收据、Z3 私有工作区
```

它**不重写宿主的循环，也不碰 prompt**；它只接管「工具执行」这一个边界。具体到每个宿主：

| 宿主 | 适配器插在哪 | 宿主看到的结果 |
|---|---|---|
| LangGraph | `AgentMiddleware.wrap_tool_call` | `ToolMessage` / `Command` / 异常 |
| CrewAI | 包装 `BaseTool.run`（hook 普通异常会被吞，不能放正确性） | 工具返回文本（`RSC_WAIT:` / `RSC_CONFLICT:`） |
| OpenAI Agents | `FunctionTool` 的 input/output guardrail | guardrail behavior（allow / reject_content / raise） |
| AG2 | `BaseMiddleware.on_tool_execution` | `ToolResultEvent` |
| 任意语言 | HTTP sidecar（本示例第 3 段） | 401 / 200 / 409 / 403 等状态码 |
| MCP 客户端 | 我们的 MCP server（S22） | 工具调用结果 |

一句话：**适配器 = 把宿主的「工具调用」翻译成 RSC 的六动作，再把六动作的结果翻译回宿主。**

---

## 二、这个项目到底起了什么作用

`run.py` 用同样的交错顺序跑四段，每段都打印可核对的证据：

### 1) 没有 RSC：真实丢写

两个 agent 同时 `read → 思考 → write`：

```text
# 计划
- api
```

`arch` 的写入被静默覆盖，没有任何报错——这就是多 agent 最常见的 lost update。

### 2) 有 RSC：冲突被拦住，重试后两份都在

```text
begin(arch) 钉住版本 0
begin(api)  钉住版本 0
arch apply  → Receipt（v1）
api  apply  → Conflict(stale_write)     ← 不静默覆盖
api  重新 begin（钉 v1）→ 合并 → apply → Receipt（v2）
同键重试    → 同一张收据（dedup=True）
```

最终账本是 `- arch` 和 `- api` **都在**，版本 2，三张收据可审计。

### 3) HTTP sidecar：任意语言宿主，服务端负责正确性

外部 agent 只用 `X-RSC-Auth` 密钥：

```text
无密钥            → 401
arch 写           → 200
api 在旧版本写    → 409
arch 同键重试     → 200 + dedup=True
api 想 finish arch 的 token → 403（token 归属校验）
```

宿主不需要实现 CAS、幂等、鉴权；它只需要调 HTTP。

### 4) 预算耗尽：是排队，不是继续写

```text
第二次 begin → Wait(BUDGET_EXHAUSTED)
```

---

## 三、和「普通锁」/「数据库事务」的区别

| 能力 | 文件锁 | 数据库事务 | 本项目 |
|---|---|---|---|
| 防止 lost update | 单机可以 | 需要自己写 SQL | ✅ CAS + 版本钉住 |
| 跨进程/跨语言 | ❌ | 需要共享 DB | ✅ HTTP / MCP / Raft |
| 幂等重试（同键同收据） | ❌ | 需要自己建表 | ✅ 外部幂等键 |
| 预算/配额准入 | ❌ | ❌ | ✅ 预算树 + `Wait` |
| 可审计收据链 | ❌ | 部分 | ✅ 签名收据 |
| 私有工作区（不污染主仓） | ❌ | ❌ | ✅ Z3 worktree/overlay |

它**不是**编排框架，也不是 LLM 网关：编排和模型仍是宿主的，我们只补「工具调用
并发正确性」这一层。

---

## 四、诚实边界

- 示例里的 `naive` 用线程模拟 agent；真实框架的调度方式不同，但「读-改-写」竞态一致。
- 幂等只覆盖**账本提交**；工具内部的外部副作用（发邮件/开 PR）重试会真的再执行一次，
  exactly-once 归宿主的 outbox。
- CrewAI 适配器实测无 `tool_call_id`，同任务同参重复调用无法区分（文档已写明）。
- 适配器测试在 Python 3.12 下真跑；3.14 主环境因 CrewAI `Requires-Python <3.14` 会 skip。
