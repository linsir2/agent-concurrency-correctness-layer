# 快速开始：把「工具调用」接进并发正确性层

本页只讲**怎么用**。机制与保证见 [S24 实施方案](s24-p1p2-implementation-plan.md) 与
[定位与路线图](positioning-and-roadmap.md)。

## 0. 一句话模型

你不是「加一个观测钩子」，而是把**写共享状态的那一步**交给 `apply`：

```text
begin（钉住版本） → 在私有 worktree / ctx 里干活 → apply（CAS + 幂等键） → finish
                                      ↘ 冲突 → 重读重试；拒绝 → 抛错；排队 → Wait
```

`@rsc_tool` 只是把这段样板代码收进装饰器，调用点仍然只写业务逻辑。

## 1. 安装与自检

```bash
pip install -e .                     # 零运行时依赖，Python >= 3.11
python3 examples/quickstart/run.py   # 四个场景，无需 API key / 网络
```

四个场景（每个都打印可核对的证据）：

| 场景 | 现象 | 谁拦住 |
|---|---|---|
| S1 并行 coding agents | git 合并干净，但合并后 `import` 失败 | `semantic=block`（静态）+ `verify_cmd`（动态） |
| S2 同一逻辑实体并发改 | 后写覆盖前写 | CAS → `Conflict` → 重读重试 |
| S3 长任务崩溃续跑 | 重启后重复写 / 重复扣款 | 同幂等键 → `dedup=True`，版本不重复推进 |
| S4 多租户配额 | 并发任务超卖额度 | 预算准入 → `Wait(BUDGET_EXHAUSTED)` |

## 2. 最小代码

```python
from harness import Harness, RscSession
from harness.ergonomics import rsc_session, rsc_tool
from harness.worktree import WorktreeManager

h = Harness(run_limits=1000, checkpoint_root="./state")
session = RscSession(
    h, tenant="team-a", min_budget=1,
    worktrees=WorktreeManager("./repo", "./state/wt"),
    state_dir="./state",
    verify_cmd=["python3", "-c", "import app"],   # 可选：提交前验证
    semantic="block",                             # 可选：静态拦语义冲突
)

@rsc_tool(writes=["app/feature.py"], reads=["app/core.py"], verify=None, retry=1)
def add_feature(ctx, text: str) -> str:
    ctx.write("app/feature.py", text)             # 写私有 worktree + 登记提交
    return "ok"

with rsc_session(session, "agent-a"):
    add_feature("...")                            # begin/apply/finish 全自动
```

要点：

- `writes` **必须声明**：这是钉住版本的唯一时机，漏了就没有并发检测；
- `reads` 也会被钉住；`ctx.read(rel)` 读的是**本次 begin 的那一版**，不是最新版；
- `retry` 只处理 `Conflict`（版本过期）；`Deny` / `Wait` 不会被自动重试。

## 3. 长任务：一个 token 多次提交

```python
with rsc_task(session, "agent-b", writes=["app/a.py", "app/b.py"]) as task:
    task.write("app/a.py", "v1")
    task.apply()                                  # 版本 1
    task.write("app/b.py", "v2")
    task.apply()                                  # 版本 1（各自资源）
# 正常退出 → finish；异常退出 → abort（不留半成品）
```

## 4. 结果映射

| 门面结果 | 装饰器行为 | 宿主应该做什么 |
|---|---|---|
| `Receipt` | 正常返回函数值 | 记录 `receipt_id` / `verified` |
| `Conflict` | `retry>0` 自动重来；否则抛 `RscConflict` | 重读最新值、合并后重试 |
| `Deny` | 抛 `RscToolDenied(code)` | **不要重试**（改代码 / 改内容 / 换资源） |
| `Wait` | 抛 `RscDeferred(reason)` | 排队，稍后重试 |

## 5. 不用 Python 的情况

- **任意语言 / 独立进程**：HTTP sidecar，只带 `X-RSC-Auth`；
  `401` 无密钥、`409` 版本过期、`403` token 不属于你、`200` 成功（含 `dedup`）。
- **LangGraph / CrewAI / OpenAI Agents / AG2**：`harness/integrations/` 里的中间件；
- **MCP 客户端**：`rsc_begin / rsc_apply / rsc_gate / rsc_finish` 六个工具。

## 6. 投产前

```bash
rsc-diagnose --state-dir ./state            # 只读体检
rsc-diagnose --state-dir ./state --strict   # warn 也算失败（CI）
```

## 7. 别用它的场景（诚实边界）

- 单 agent，或写入集合完全不重叠、文件级隔离够用 → 用 git worktree 就够；
- 只要 trace / 评估 / 成本看板 → 用 Langfuse / OTel；
- 要跨机强一致又不想引入 `pysyncobj` → 我们只能给单写者 + 租约，给不了共识；
- 外部副作用（发邮件、扣款）的 exactly-once → 账本层保证 at-most-once，outbox 要自己做。
