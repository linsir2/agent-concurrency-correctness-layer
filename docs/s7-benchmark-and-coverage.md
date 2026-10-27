# S7 基准 + S8 覆盖率收口

| 项目 | 内容 |
|---|---|
| 版本 | **v1.1（已按两路对抗评审修订）** |
| 日期 | 2026-10-27 |
| 上游 | [第一阶段方案 v1.1](phase1-implementation-plan.md) §7/§9、[S6 方案 v1.2](s6-runtime-and-e2e.md) §12 |
| S7 交付 | `bench/` + `bench/report.md`（§9.3 五项 + 二级指标 + 环境信息 + 测量局限） |
| S8 交付 | 覆盖率分模块阈值落地 + 补强用例 |
| 修订 | v1.0 → v1.1：修 1 严重 + 4 高 + 5 中（§9） |

---

## 0. 调研结论

### 0.1 §9.3 的 5 项指标 vs 直觉维度

| 直觉维度 | §9.3 | 缺口 |
|---|---|---|
| 吞吐 | ⚠️ 间接 | 无 commits/sec |
| 延迟 | ⚠️ 间接 | 无 p50/p99 |
| **恢复时间** | ❌ | MS3 `recover` 无度量 |
| 并发扩展性 | ⚠️ 部分 | 无 N 路曲线 |
| **事件量增长** | ❌ | 与 retention/cursor floor 相关 |

### 0.2 三条件 × 四负载（§9.1/§9.4）

B1 串行 / B2 worktree + merge / B3 共享层；C0 零冲突 / C1 弱重叠 / C2 强重叠 / C3 write skew（未声明 `logical_deps` 为 🟡）。

### 0.3 五项指标

`lost_update_rate`、`conflict_rate`（分 `detected`/`silent`）、`speedup`、`serialization_rate`、`wasted_work`；`token_multiplier` **阶段一 N/A**。

### 0.4 S8 现状

- **`pyproject.toml` 缺 `fail_under`** → §7.1 分模块阈值形同虚设
- **`hypothesis` / `pytest-cov` 未安装**（dev extras 未装）→ 属性测试与覆盖率门禁**当前跑不起来**
- **`fault` marker 已定义但零使用**
- 最高价值缺口：`recover(COMPENSATE)`、resolver 成功路径、`_restore_path`、`budget.gc`（死代码）、`auth.gc_seen`

---

## 1. 目标

| # | 目标 | 判据 |
|---|---|---|
| G1 | `bench/` 三条件 × 四负载 | `run_bench.py` 可运行 |
| G2 | `report.md` 含 5 项 + 二级指标 + **测量局限** | 报告完整 |
| G3 | 决策序列固化（**先 `asdict`**） | 三条件 `freeze` 相等 |
| G4 | 覆盖率分模块阈值落地 | `check_coverage.py` 通过 |
| G5 | 补 3 个高价值缺口 | 新增用例 |
| G6 | 死代码处理 | `gc` 接上或删除 |

---

## 2. S7 设计（v1.1：按真实接口重写）

### 2.1 目录

```
bench/
  workloads.py    # C0–C3 负载 + 决策序列固化（asdict）
  conditions.py   # B1/B2/B3 执行器
  metrics.py      # 外部差分（资源级 + 键级归因）
  run_bench.py    # 主入口
  report.md
```

### 2.2 决策序列固化（修「严重」）

```python
def freeze(decisions) -> str:
    payload = [asdict(d) for d in decisions]        # ← 修：Decision 是 dataclass
    return hashlib.sha256(json.dumps(payload, sort_keys=True,
              separators=(",", ":"), ensure_ascii=True).encode()).hexdigest()
```

**三条件对齐**：schema 里区分 `logical_resource`（B3 用）与 `worktree_path`（B2 用）；`freeze` **只覆盖逻辑名**。B2 比 hash 前**先解析回 JSON 再算 canonical hash**（不比文件字节）。

### 2.3 外部差分度量（修「严重」—— 原设计方向会反）

**v1.0 的错误**：`final[w.resource] != w.expected_hash`（expected = 该 agent 自己那一笔的 hash）。C2 两 agent 改同文件不同字段，**正确合并**后 final = 两侧并集，既不等于 A 的 hash 也不等于 B 的 → **两个写都被判 lost** → `lost_update_rate(B3)=1.0`，而「REPLACE 覆盖」实现只有 0.5 → **判定反向**，得出「本层更差」的荒谬结论。

**v1.1 定死**：

```python
# ① expected 用资源级：该资源所有应存活写的合并结果（独立 oracle，禁止复用 merge3）
expected[resource] = H(oracle_merge(base, all_surviving_writes))
lost = [w for w in all_writes if final[w.resource] != expected[w.resource]]

# ② 归因用键级差分（才能算 silent）
missing_keys = set(expected_keys) - set(final_keys)
extra_keys   = set(final_keys) - set(expected_keys)
# silent = 提交成功但 missing_keys 非空
```

**独立 oracle 自检**：对无冲突负载，oracle 结果必须 == 串行 B1 的结果。

### 2.4 B2 合并（修「高」—— 语义不可比）

| 项 | 处理 |
|---|---|
| git 可用性 | `shutil.which("git")` 探测；不可用走**纯 Python 行级**并在报告标「环境降级」 |
| **行级 vs 结构级** | 明确写成**显式混杂变量**：额外输出「语义冲突率」（两侧最终内容解析回 JSON 后离线用 merge3 判定） |
| 不可直接比 | 报告明确 **B2 合并 ≡ REPLACE（后写覆盖）**，与 B3 的 SET/GROW_ONLY 不同 → 比较同时混杂「并发模型」与「合并算法」两个变量 |

### 2.5 受控 I/O 注入（修「中」—— 无挂载点）

`Harness.decide` **从不被调用** → 无挂载点。v1.1：给 `TaskRuntime` 加 `per_action_delay` 注入点，B1/B3 复用同一 delay；**B2 的 `git merge-file` 子进程耗时单独打点并从 `speedup` 剔除**（或显式声明 B2 含 subprocess 常数项）。

### 2.6 统计纪律（修「中」—— 离散指标套 p 值会假显著）

| 指标类型 | 判定 |
|---|---|
| **离散确定性**（`lost_update_rate`/`conflict_rate`） | 固定种子下**不是随机变量** → 用**精确判定 + 重复 n 次验证确定性（方差应为 0，非 0 即 bug）**；**不用 p 值** |
| **时序**（`speedup`/延迟） | 配对 bootstrap CI（配对单位 = 同 seed/同 workload/同迭代序号）+ 报告机器负载 |

**预注册判定按格（per-cell）**：
- **C0 只做 sanity**：断言 `B2 == B3 == 0`，**不参与**优越性判定
- **主终点预注册在 C2**（只有那里 B2 行级合并才可能真丢写）
- 判定改**非劣 + 等价**：`B3 ≤ B2` 且差值 CI 上界 < 预注册 margin（严格 `<` 对平局零容忍 → C1 可能恒不成立）

### 2.7 二级指标

`throughput`、`commit_latency_p50/p99`、**`recovery_replay_cost`**（改名，见 §3）、`scalability_curve`、`event_replay_cost`。

---

## 3. `recovery_time` 口径修正（修「低」—— 表述过强）

v1.0 说「无意义」**过强**。`recover()` 有真实 CPU 成本（`rebuild_from` ×2、`_replay_events` 逐条取锁投影、`_restore_budget`/`_restore_path`、`check_invariants` O(N)）—— 都是**事件数的函数**，正是 `event_replay_cost` 想刻画的。

**v1.1**：口径改为 **`recovery_replay_cost (in-memory store)`**，分相位打点（rebuild / replay / invariant），明确 **≠ 端到端持久恢复**；用 WAL 版 + fsync 另测「durable recovery」作对照。

---

## 4. S8 设计

### 4.1 覆盖率配置

```toml
[tool.coverage.report]
fail_under = 80        # 兜底
```

**模块定义**：`harness/` **顶层 .py 文件**。三档阈值（§7.1）：

| 档 | 模块 |
|---|---|
| ≥85% | `workspace.py`、`budget.py`、`intent_registry.py`、`event_log.py` |
| ≥75% | `rebase.py`、`storage/wal.py`、`checkpoint.py` |
| ≥70% | `runtime.py`、`orchestration.py` |
| 其余（`auth`/`resolver`/`policy`/`context`/`subscription`/`compensation`/`clock`/`types`/`llm`/`storage/*`/`spec/*`） | 归全局，**软提示** |

**`LLMResolver` 网络分支**：标 `# pragma: no cover` 并在报告注明（阶段一无真实 LLM，`token_multiplier` N/A）；成功路径只覆盖 `ScriptedResolver` + `_validate_resolution`。

新增 `scripts/check_coverage.py`：硬门禁（上表）未达即 `exit 1`；软提示只告警。**显式列出被排除的模块与理由**，避免阈值形同虚设。断言 `bench/` 在 coverage source 外。

### 4.2 补 3 个高价值缺口（v1.1：构造走真实路径）

| 缺口 | 构造 |
|---|---|
| **`recover(COMPENSATE)`** | **两段式**（v1.0 单次断言自相矛盾：`CrashInjected` 会从 `recover` 直接抛出）：①共享 `event_log`+`checkpoints`+`blobs`，monkeypatch `comp_journal.mark` 在 `COMPENSATED` 处抛 → 第一次 `recover` 抛 `CrashInjected`，断言 journal 状态；②第二次 `recover(resume_policy="COMPENSATE")` → 断言 `DONE_PARTIAL` + **幂等（无重复反写）** + 预留已结算 + 节点已归档 + `check_invariants()` |
| **resolver 成功路径** | 注入返回**非法类型**（list）的 resolver → 断言 `_validate_resolution(list,...) is False`（**直接单测**）+ spy 计数 ≥1 + 非法值从未落盘 + **在第一次 `_rebase_write` 返回后立刻捕获 `failure == "resolver_failed"`**（终态会被 `_quarantine` 覆盖）+ **反证**（monkeypatch 校验器恒 `True` → 非法值被接受） |
| **`_restore_path`** | **祖先缺失走真实路径**：新进程 + 嵌套 `parent_budget` → `_budget_path` = `["run","B-parent","B-child"]` → spy 记录 `create_node` 的 `(node_id, parent_id)` 序列；**回滚用例**：`budget_path_limits` 让中间节点 `create_node` 返回 `None` → 断言逆序 `remove_node` + `needs_readmit=True`；**环**只能合成（真实路径无环）→ 断言 `needs_readmit=True` + 无残留（**不是**抛 `ValueError`，会被 `_restore_budget` 吞） |

### 4.3 **`_validate_resolution` 语义缺陷（修「高」）**

评审指出：校验器要求 `set(resolved) == set(committed) | conflict_tops`，但 merge3 的**干净合并也会产出新键**（B 字段是 local 新增且 committed 缺失）→ **合法输出被拒 → 误隔离**。

**修法**：键集合改为 `committed ∪ conflict_tops ∪ merge3 干净合并新增的顶层键`（非 dict committed 直接 PIVOT），并加一条「干净合并新增键」回归用例。

### 4.4 死代码处理

| 死代码 | 处理 |
|---|---|
| `budget.gc` | **接上**：在 `_on_event` 的 `_event_count % gc_every == 0` 触发（与 `auth.gc_seen` 共用钩子），`now` 用注入时钟的 epoch 秒 |
| **`check_invariants` 无锁** | **加 `_lock`**（否则周期 gc 与 recover 后的 `check_invariants` 并发会读到中间态 → 偶发 `InvariantError` → 新 flaky） |
| `auth.gc_seen` | 补用例：`make_harness(auth_secret=..., auth_gc_every=2)` + 过 exp 的 token |
| 其他保留扩展点 | 文档标「保留扩展点」+ `pragma: no cover`，计入「已知未覆盖清单」 |

### 4.5 T15–T18 补强（**消歧**）

v1.0 的 T15–T18 与 §7.3 的 T15–T19 **编号冲突** → 改名 **`S8-1..S8-4`**：

| 编号 | 内容 | 备注 |
|---|---|---|
| **S8-1** | 子进程 SIGKILL（`@pytest.mark.fault`） | **分层**：层 A 只测 WAL/事件层（现在可做）；层 B 需文件版 checkpoint（**未实现** → 报告显式写「checkpoint 内存态，SIGKILL 不覆盖 checkpoint 恢复」） |
| **S8-2** | 属性测试（I1/I5/I7） | 需先 `pip install -e .[dev]`（装 `hypothesis`/`pytest-cov`） |
| **S8-3** | 端到端 3 agent | 经 `dispatch_and_run` |
| **S8-4** | 故障窗口枚举 | 固定种子枚举 WAL 截断/补偿中途崩溃 |

---

## 5. 交付判据

| 项 | 判据 |
|---|---|
| S7 | `python bench/run_bench.py` 成功；`report.md` 含 5 项 + 二级指标 + 环境信息 + **测量局限** + 混杂变量声明 |
| S8 | `pytest --cov` + `scripts/check_coverage.py` 通过；连跑 10 轮稳定 |

---

## 6. 风险与对策

| 风险 | 对策 |
|---|---|
| 基准本身 flaky | 离散指标验确定性（方差 0）；时序指标配对 bootstrap CI |
| 基准在沙箱太慢 | `--quick`（n=3）与完整模式（n≥30） |
| GIL 致 `speedup` 无意义 | 明确结论写入报告，不粉饰 |
| 补缺口暴露生产缺陷 | 只加用例；若暴露则单独修（如 §4.3） |
| `hypothesis`/`pytest-cov` 未装 | 先 `pip install -e .[dev]` |

---

## 7. 决策（v1.1 定案）

| # | 问题 | 结论 |
|---|---|---|
| Q1 | 基准用内存 checkpoint？ | **内存**；`recovery_replay_cost` 口径 + 显式局限 |
| Q2 | B2 用 git merge-file？ | **优先 git**；不可用退纯 Python 行级 + 报告标「环境降级」 |
| Q3 | `budget.gc` 接上/删除？ | **接上**（`_on_event` 周期钩子）+ `check_invariants` 加锁 |
| Q4 | 离散指标用 p 值？ | **不用** → 精确判定 + 重复 n 次验确定性 |

---

## 9. 评审修订记录（v1.0 → v1.1）

### 9.1 严重（1 项）

| # | 问题 | 修法 |
|---|---|---|
| **P0-1** | **外部差分方向会反**：`final[w.resource] != w.expected_hash`（expected = 该 agent 单笔 hash）→ C2 正确合并被判全丢 → `B3=1.0 > B2=0.5` → **判定反向** | expected 改**资源级合并结果**（独立 oracle）；归因用**键级差分**；oracle 自检（无冲突负载 == B1） |

### 9.2 高（4 项）

| # | 问题 | 修法 |
|---|---|---|
| **P0-2** | **`freeze` 对 dataclass 直接抛错**（实测 `TypeError`）→ G3 防作弊失效 | 先 `dataclasses.asdict`；schema 区分 `logical_resource`/`worktree_path`；B2 比 hash 前解析回 JSON |
| **P0-3** | **B2 行级 vs B3 结构级不可比**（git 实测产出 `<<<<<<<` 与非法 JSON）；git 可用性无硬保证 | `shutil.which("git")` 探测 + 降级；显式声明混杂变量；额外输出「语义冲突率」 |
| **P0-4** | **COMPENSATE 单次断言自相矛盾**（`CrashInjected` 从 `recover` 直接抛出） | **两段式**（第一次抛 + 断言 journal；第二次续做 + 断言 `DONE_PARTIAL` + 幂等） |
| **P0-5** | **`_validate_resolution` 语义过严**：要求 `set(resolved) == committed ∪ conflict_tops`，但 merge3 干净合并会产出新键 → **合法输出被拒 → 误隔离** | 键集合加「merge3 干净合并新增顶层键」；补回归用例 |

### 9.3 中（5 项）

| # | 问题 | 修法 |
|---|---|---|
| 1 | `sleep` **无挂载点**（`decide` 从不被调用） | 给 runtime 加 `per_action_delay`；B2 subprocess 耗时单独打点并从 `speedup` 剔除 |
| 2 | **离散指标套 p 值 → 假显著**；固定 seed 无法固定线程调度 | 离散用精确判定 + 验确定性；时序用配对 bootstrap CI |
| 3 | `fail_under` 缺失；「模块」未定义；`LLMResolver` 网络路径 | 补 `fail_under=80`；模块 = 顶层 .py 三档；网络分支 `pragma: no cover` |
| 4 | **`budget.gc` 触发点未定义**；`check_invariants` **无锁** → 并发读到中间态 → 新 flaky | `_on_event` 周期钩子 + 注入时钟；`check_invariants` 加 `_lock` |
| 5 | **T15–T18 编号与 §7.3 冲突** | 改名 `S8-1..S8-4` + 对照表 |

### 9.4 低

| 问题 | 修法 |
|---|---|
| `recovery_time` 表述过强 | 改 `recovery_replay_cost (in-memory store)` + 分相位打点 |
| 预注册判定未按格 | C0 只 sanity；主终点 C2；改非劣 + 等价（严格 `<` 对平局零容忍） |

---

## 10. 实现后对抗评审（v1.1 → v1.2）

实现分三波（生产修复 + 覆盖率配置 → 基准套件 → 覆盖率补强），随后对抗评审。**4 高 + 5 中 + 2 低已修**，其中 **1 个是真生产缺陷**。

### 10.1 高（已修）

| # | 问题（反例） | 修法 |
|---|---|---|
| **P0-7** | **C1 静默丢写（K-28，真缺陷）**：`_refresh_read_dep` 只刷版本不重合并 → 重试用**陈旧 staged** 提交 → 覆盖他人写（实测 `config.json` 丢 `k05/k12`；`conflict_rate_silent=1.000`） | 新增 `_rebase_stale_staged`：以 `pre_image(base_version)` 为 base 重跑 `merge3`；仍不可解则**拒绝重试**。**残值归零**（0.333/1.000 → 0.000） |
| **P0-8** | **`_clean_merge_new_tops` 推断不成立**：`base={x:0,a:0} local={x:0,a:1} committed={a:2}` → 推断 `{x}`，但 merge3 对 x 走「有/base 缺→删除」→ `_validate_resolution({a:1,x:0})=True`（**放行复活已删键**）、`_validate_resolution({a:1})=False`（**拒掉与 merge3 一致的结果**） | 改 `_clean_merge_present_tops`：直接复用 `_merge_object` 按 merge3 对象规则精确求「干净合并后仍存在的键」 |
| **P0-9** | **B2 降级 `_py_line_merge` 静默丢写**：`base="A\nB\n" ours="A\nX\nB\n" theirs="A\nY\nB\n"` → git 判冲突，`_py_line_merge` 返回 `('A\nB\n', False)` —— **X/Y 双双丢失且不报冲突**（零长插入块被丢弃） | 显式处理 `i1==i2` 插入块；补与 `git merge-file` 的 8 组差分测试 |
| **P0-10** | **report.md 与 result.json 互相矛盾**：§4.5 用单次 N=1 最冷基线 → `speedup<1`，被自家 `result.json` 证伪（0.50 vs 1.103）→ 「GIL 结论」不成立 | 基线改 warmup 后重采样 5 次取中位数；新增 `report_check.py` 校验一致性 |

### 10.2 中（已修）

| # | 问题 | 修法 |
|---|---|---|
| M-1 | **`oracle_merge` 是死代码**：`expected_by_resource` 手写，`oracle_for`/`oracle_self_check` 从未调用 → 「独立 oracle 自检」实为「手写 expected == B1」**同义反复** | `expected_by_resource` 全由 `oracle_for` 驱动；对**每个负载**断言 `oracle_self_check` 且 **B1 终态 == oracle** |
| M-2 | **`missing_keys` 语义错误**：同时含 extra 键且与 `extra_keys` 重复计数 | 拆为 `_diff_keys` 三路互斥 `missing/extra/changed` |
| M-3 | **方差是观测非断言**；「固定种子」不能固定线程调度（实测 `turn_replay_counts` ∈ 3 种） | 加 `assert_deterministic`（逐项相等）；报告同时给 per-agent 分布 |
| M-4 | **`degraded` 不参与判定**：git 缺失时仍按 git 语义出判定 | `judge_cell` 在 `degraded=True` 时返回「环境降级，判定无效」 |
| M-5 | **两段式是进程内模拟**（journal/checkpoint 皆内存态） | 测试 docstring + 报告显式声明「非持久恢复」 |

### 10.3 低（已修）

| 问题 | 修法 |
|---|---|
| `check_coverage._module_key` 绝对路径会假阴性 | 统一按路径分量取 `harness/` 之后 |
| `_restore_budget` 的 `except Exception` 过宽 | 收窄为 `(ValueError, KeyError, RuntimeError)` |
| 报告一致性检查对**时序指标**过严（重跑必然 exit 1） | 区分**严格**（`lost`/`conflict`/`wasted`/oracle/verdict）与**容差**（`wall`/`speedup` rel 25%；`serialization_rate` abs 0.05 + 量级 `[0,1]`）；超容差仍 `exit 1` |

### 10.4 新增已知边界（v1.2）

| # | 边界 |
|---|---|
| **K-28** | ~~C1 静默丢写~~ → **已修**（残值 0）。但根因提示：**任何「刷新版本后重试」都必须重算 staged**，否则就是 OCC 陈旧读覆盖 |
| **K-29** | `scalability_curve` 仍 `speedup<1`（N=1/2/4/8 → 1.00/0.88/0.73/0.53）—— **GIL 下的真实观测**，非缺陷；报告与 result.json 已同源 |
| **K-30** | `recover(COMPENSATE)` 两段式是**进程内模拟**（journal/checkpoint 内存态），非持久恢复 |
| **K-31** | `degraded`（无 git）路径本机未触发，判定逻辑有单测但**无端到端验证** |
| **K-32** | 时序指标本质非确定 → 报告一致性用容差；**确定性指标**（`lost`/`conflict`）必须逐值稳定 |
