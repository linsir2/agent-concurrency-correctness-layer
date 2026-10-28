# S24：里程碑差距、定位复核与「是否走偏」

| 项目 | 内容 |
|---|---|
| 版本 | v1.0（调研 + 真跑，未改内核） |
| 日期 | 2026-10-28 |
| 触发 | 「对照最初的总体方案还差啥」「我们是让别人设计 agent 系统时调用吗」「这样也实现不了编程代码跑起来发现问题、减少冲突吧」「感觉走偏了」 |
| 结论摘要 | 里程碑大致达成，但**价值主张偏了**：我们卖的是「不丢写」，而并行编程 agent 的第一痛点是「合起来跑不通」；`VerifierRunner` 在最初方案里被列为差异化能力，至今**零实现**。 |

---

## 0. 真跑证据（可复现）

```bash
python3 -m pytest -q                                  # 734 passed / 3 skipped / 4 xfailed
python3 scripts/check_s23_conformance.py              # 80 / 0
python3 examples/three_agents/run.py                  # [PASS] naive 丢写 vs RSC 保全
python3 examples/semantic_conflict/run.py             # [结论] 我们拦不住语义冲突
```

`examples/semantic_conflict/run.py` 的实际输出（同一仓库、两处互不重叠的改动）：

```text
[1] 纯 git      基线绿=True  合并干净=True  合并后绿=False
                检查：ImportError: cannot import name 'compute_rate'
[2] 没有 verify A=Receipt  B=Receipt  B.verified=False      ← 缺口
[3] 配置 verify A=Receipt  B=Deny  code=verify_failed       ← 拦住
[4] B 改对       B=Receipt  verified=True                   ← 放行
```

**这就是问题的核心**：agent-A 把 `compute_rate` 改名，agent-B 新增文件调用旧名字。
两者改的是**不同文件**，所以 git 合并干净、我们的 CAS 也认为「资源不重叠」，
两张收据都签发——但合起来 `import` 直接失败。

---

## 1. 最初方案承诺了什么 vs 现在有什么

以 `docs/positioning-and-roadmap.md`、`docs/milestones.md`、
`docs/multi-agent-harness-design-final.md` 为准（证据均为文档章节或代码位置）。

| 能力 | 出处 | 状态 |
|---|---|---|
| RSC（版本钉住 + CAS + fencing） | design §5.4 | ✅ `harness/workspace.py`、`intent_registry.py` |
| EffectJournal 三态对账 | design §5.8 | ✅ S5 |
| 嵌套预算树 | design §9.1 | ✅ `harness/budget.py` |
| 准入 GRANT/WAIT/DIE | design §18.2 | ✅ `harness/spec/protocol.py` |
| Saga 补偿 | design §5.4 | ✅ `harness/compensation.py` |
| 物化快照 + 版本钉住 | design §5.5 | ✅ `harness/context.py` |
| Z3 私有 worktree | design §0.1 | ✅ `harness/worktree.py` |
| 收据签名链 | design §18.3 | ✅ `harness/spec/receipt.py` |
| **VerifierRunner / 语义验证** | design §4.2、milestones §「VerifierRunner（独立上下文校验）」标 P2/MS5「当前无」 | **❌ 零实现** |
| 四件套对外（TLA+ / corpus / CLI） | 定位 §6.3、design §18.4 | 🟡 规范在 `harness/spec/`；**无 .tla**、无对外 corpus、无 `rsc-diagnose` |
| 跨主机租约复制 | 定位 §6.5、S21 明确「不做 → K-103」 | 🟡 Raft 事件/预算已跨主机，租约未复制 |
| 多租户 / 计量 / Admin | 定位 §6.5 | ✅ S20/S15/S17 |
| 宿主适配（LangGraph/CrewAI/OpenAI/AG2/MCP/HTTP） | 定位 §6.2 阶段一交付物、§6.4 MVP | ✅ S22/S23 |
| 基准五指标 | phase1 §9.3 | 🟡 token 指标 N/A、LLM 用 scripted |
| 确定性原语（seeded uuid/now） | 定位 §4.3 第 3 条 | ❌ `uuid4` 仍散落 |
| DurableTimer / 定义版本化 | design §14 | ❌ 仅 `tick` 轮询 |

### 里程碑状态（按 phase1 实录，非 milestones 旧表）

MS0–MS10 均已收口（S0–S18），阶段三（S19–S21）、被集成层（S22–S23）在其后。
**但 `docs/milestones.md:300-301` 仍写「MS9/MS10 未开工」**（v1.1 遗留），与
`docs/phase1-implementation-plan.md` 的完成记录冲突——这是文档腐化，不是实现缺失。

---

## 2. 差距排序（只列「当初承诺、现在没有、且对用户有价值」）

| # | 缺口 | 为什么重要 | 最小可验证补法 |
|---|---|---|---|
| 1 | **Verifier（提交前验证）** | 用户要的「跑起来发现问题」就是它；也是最初方案里唯一被标为「业界独有」的能力 | `finish` 前在 Z3 隔离副本跑用户命令，红则 `Deny`；实验见 §0 第 3 段 |
| 2 | 语义冲突检测 | 文本合并不冲突但符号不兼容（改名/签名变更）是我们**唯一能超过 git** 的地方 | 对候选改动做符号级 diff：删除/改名的符号是否仍被引用 |
| 3 | merge queue 集成 | 并行度与串行安全兼得；CAS 提交点天然是 enqueue 点 | 提交成功后 rebase 到队首 + 回写验证结果 |
| 4 | 规范对外化（corpus / `rsc-diagnose` / CI merge-gate） | 别人能自测，才谈得上「层」 | 导出 `harness/spec` 语料 + 一个只读 CLI |
| 5 | TLA+ 模型 | 并发正确性缺形式化背书，说服力弱 | I1/I2/I4/I7 四个模型 + TLC |
| 6 | 跨主机租约复制 | 阶段三卖点（多机）目前只有事件/预算跨主机 | `RaftIntentRegistry` + 旧 fence 被拒用例 |
| 7 | 真实基准（LLM/token） | 差异化缺少数据 | 小样本真模型回放，报 token 倍数与返工率 |
| 8 | 确定性原语 | 重放不可复现，影响审计可信度 | 注入 `clock`/`uuid` 的 seeded 版本 |

---

## 3. 定位：适配器是什么、我们是「让别人调用」吗

**是。** 我们不提供 agent 编排、不做 LLM 网关、不做 UI。宿主（LangGraph / CrewAI /
OpenAI Agents / AG2 / 任意语言）继续管编排与模型；我们只接管**「工具执行」这一个边界**：

```text
宿主框架（编排 + 模型）
   │ 工具调用（写文件 / 发 PR / 扣款）
   ▼
[适配器] 宿主事件 → begin / gate / apply / finish；结果 → 宿主能懂的错误
   ▼
RscSession（版本、预算、收据、Z3 私有工作区）
```

所以两种用法：

1. **库内调用**：`from harness import RscSession`，在你自己 agent 的工具函数里调六动作；
2. **进程外调用**：`POST /apply`（HTTP sidecar）或 MCP 工具，任何语言都能用。

---

## 4. 「走偏了吗」

### 成立的部分（必须承认）

1. **「跑起来发现问题」是 0 实现**。门面六动作里没有 verify；`verifier_failed` 只是状态机标签
   （`harness/types.py`），唯一调用点是 `advance(acb, "verifier_failed")`（`harness/runtime.py`），
   没有任何地方执行测试或命令。`WorktreeManager` 只有 `create/destroy/changed/read`。
2. **我们防的「丢写」不是并行编程 agent 的第一因**。每个 agent 一个 git worktree 后，
   文本级覆盖基本被隔离吸收；真正让 agent 返工的是「合起来跑不通」与「沟通/期望不一致」。
3. **CAS 的增量被 git 挤压**。分支隔离下，提交本身就受 `git update-ref` 的 old-value 校验保护；
   我们如果只讲「防丢写」，说服力不足。

### 不成立的部分

1. **幂等与预算确实 git 给不了**：重试不重复副作用（同键同收据）、15× token 成本的可控准入，
   这些是独立价值，不是重复造轮子。
2. **merge queue 只买串行安全，不买并行度**，串行队列本身会成为瓶颈；CAS 提交点可以做成
   非阻塞 enqueue，这是有增量的。
3. **跨语言/跨进程的一致提交点**，git 也没有现成答案（HTTP/MCP 是真实需求）。

### 结论

**整体没走偏，价值主张偏了。** 我们一直在讲「并发正确性（不丢写）」，但用户的真实诉求是
「并行改代码时，别让坏改动进主干，并减少返工」。前者是手段，后者是结果。
**必须在门面里补上 Verifier 门禁**，并把它作为对外的主卖点；否则我们只是一个更严格的
文件锁，无法回答「能不能跑起来发现问题」。

---

## 5. 下一步（S24 提案）

| 轮 | 交付 | 状态（2026-10-28） |
|---|---|---|
| S24-1 | `session.verify(token, cmd, timeout)`：在「候选副本 + 账本合并视图 + 本次 pending」上跑命令；红/超时/无工作区 → 拒绝提交；结果写进 `spec["rsc_verify"]`，收据带 `verified` | ✅ 已实现（`harness/facade.py`），11 个用例 |
| S24-2 | 适配器接入：`RscSession(verify_cmd=...)` 后 `apply` 自动先跑验证（无需改适配器代码） | ✅ 已生效；sidecar/MCP 响应新增 `verified` |
| S24-3 | 符号级语义冲突检测：删除/改名符号仍被引用 → `Deny(semantic_conflict)`（不执行代码、零依赖、仅 Python） | ✅ 已实现（`harness/semantic.py` + `semantic=off|warn|block`），22 个用例 |
| S24-4 | 进程内 merge queue：单写者 FIFO，「出队 → 最新账本重验 → 提交」串成一步 | ✅ 已实现（`harness/merge_queue.py` + `apply_queued`），9 个用例 |

### 5.1 S24-1 实现要点

```python
session = RscSession(h, worktrees=WorktreeManager(repo, wt_dir),
                     verify_cmd=[sys.executable, "check_all.py"],   # 或 "pytest -q"
                     verify_required=True, verify_timeout_s=120)
```

- **合并视图**：复制候选工作区（不含 `.git`）→ 叠加账本里已存在资源的最新内容
  → 叠加本次 `apply` 的 pending 内容。「提交后会是什么样」就验证什么。
- **不经 shell**：`shlex.split` 后直接 argv，杜绝命令注入。
- **fail-closed**：无 Z3 工作区 → 拒绝；超时 → 拒绝；已记录失败且未重新验证 → 拒绝；
  `verify_required=True` 但没给命令 → **构造即 ValueError**（不静默放行）。
- **不污染**：验证在临时副本里跑，工作区与账本都不改动（有用例）。

### 5.2 实测效果（`examples/semantic_conflict/run.py`）

```text
[1] 纯 git    合并干净=True  合并后绿=False      ← 无冲突标记，import 炸
[2] 无 verify A=Receipt  B=Receipt              ← 我们都放行（缺口）
[3] 有 verify A=Receipt  B=Deny code=verify_failed   ← 拦住
[4] B 改对     B=Receipt  verified=True         ← 放行
```

### 5.5 S24-4 merge queue 要点

- **关闭的窗口**：原来 `verify` 与 `commit` 之间存在 TOCTOU —— 两个 agent 各自在
  「提交前视图」验证通过，然后先后提交，后者的验证没看见前者。队列让
  「出队 → 在最新账本视图重验 → 提交」串成一步。
- **API**：`RscSession(merge_queue={"capacity": 64, "max_wait_ms": 30_000})`；
  `apply_queued()`（阻塞）/ `enqueue_queued()` + `wait_queued()` / `queue_depth()` /
  `queue_status()` / `queue_cancel()`；默认 `merge_queue=None` 时原 `apply` 路径不变。
- **失败语义**：容量满 → `Wait(QUEUE_FULL)`；超时 → `Wait(QUEUE_TIMEOUT)`；取消未开始票
  → `Wait(QUEUE_CANCELLED)`；队首验证失败 → 该票 `Deny` 并被剔除，不连带他人。
- **锁**：出队线程自己拿 `_op_lock(token)` 再走原提交路径 —— 调用方等待时不持锁，
  所以不会与 `finish/abort` 互锁。
- **v1 不做**：batch / speculation / 持久化队列 / 多节点；超时后已在执行的票不撤销，
  若仍提交成功，宿主用同一幂等键重试会命中 `dedup`（诚实边界）。
- **不是吞吐优化**：串行队列的吞吐 = 1/(verify+commit)；收益是「验证与提交同视图」。

### 5.4 S24-3 静态语义检测要点

- **只报「base 有、post 没有」的模块级显式引用**：`from m import x`、`import m as a` 后的
  `a.x`、相对导入；模块整体消失也报（`removed_module`）。
- **用召回换精度**：`from m import *`、`__getattr__`、`getattr/globals/exec/importlib`、
  条件定义（`if`/`try` 内）、注解、`try/except ImportError`、`TYPE_CHECKING` 一律跳过；
  语法错误只跳过该文件，不拒绝提交。
- **三态**：`off`（默认，不跑）/ `warn`（只写 `spec["rsc_semantic"]`）/ `block`（`Deny`）。
  非法值构造即 `ValueError`。
- **顺序**：静态检查在 `verify` **之前**（便宜、给出 `path:line:符号`，可行动）；
  动态验证仍是权威判据。
- **性能**：上限 2000 文件 / 5 MiB 单文件；200 文件约 2 万行 < 2s（有用例）。
- **实测**：`examples/semantic_conflict` 增加 `[3b]`，不跑任何代码即返回
  `code=semantic_conflict reason=removed_symbol:app.core:compute_rate@app/feature.py:1`。

### 5.3 已知边界

| 边界 | 说明 |
|---|---|
| 验证的是**文件视图** | 只叠加「路径已存在于候选副本」的账本资源；纯 JSON/非文件资源不参与合并视图 |
| 不替代 CI | 只跑你给的一条命令；耗时 = 一次全量检查，接受不了就配更快的子集 |
| 静态检测只覆盖「名字消失」 | 签名/行为变化、类成员、动态引用查不到（S24-3 明确不做，交给 `verify`） |
| merge queue 没做 | S24-4；当前没有 rebase 到队首再验的串行化 |
| 副本复制成本 | 大仓库每次 `apply` 复制一棵树；可用 `verify_cmd=None` + 显式 `verify()` 控制频率 |

**不做**：语义 diff 的全量程序分析、替代 CI、替代 git 合并。

---

## 6. 文档待修（本次只记录，避免顺手大改）

| 位置 | 问题 |
|---|---|
| `docs/milestones.md:300-301` | MS9/MS10 仍写「未开工」，与 phase1 完成记录冲突 |
| `docs/milestones.md` MS5 出口 | 未包含 `VerifierRunner`，却判通过（它是 P2 但被列为差异化能力） |
| `docs/positioning-and-roadmap.md` §2.4 | 标「语义验证 ✅、worktree 🟡」，与代码相反 |
| `docs/positioning-and-roadmap.md` 附录 A.2 | 把 Trace 计入 L4 交付，与 design §0.5「L5 不交付」冲突 |
| `harness/spec/invariants.py` | I12/I14 的测试指向旧用例名 |
| `README.md`（已修） | 测试数写 91，实际 733 |
