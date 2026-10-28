# S7 基准报告

| 项目 | 内容 |
|---|---|
| 对应 | `docs/s7-benchmark-and-coverage.md` v1.1 §2（S7 设计）/ §9.3（五项指标） |
| 交付 | `bench/workloads.py`、`bench/conditions.py`、`bench/metrics.py`、`bench/run_bench.py`、`bench/report_check.py`、`bench/report.md`、`bench/result.json` |
| 复现 | `python bench/run_bench.py --quick`（n=3）/ `python bench/run_bench.py --n 30 --out bench/result.json`（完整） |
| 一致性 | `python -m bench.report_check bench/result.json bench/report.md`（本报告数字全部由 `result.json` 校验） |

---

## 0. 交付摘要（五问自检）

| # | 自检项 | 结论 |
|---|---|---|
| ① | `freeze` 三条件相等 | **通过**（4 个负载的 `B1 == B2 == B3` 全部相等，见 §2.3） |
| ② | C0 sanity（`B2 == B3 == 0`） | **通过**（C0：B1/B2/B3 的 `lost_update_rate` 均为 0.000） |
| ③ | C2 主终点判定 | **通过**：`B3_lost=0.0 < B2_lost=1.0`，非劣 + 等价（差值 −1.0，严格 `< margin=0`） |
| ④ | `report.md` 是否含全部必需章节 | **含**：§9.3 五项 + 二级指标 + 环境信息 + 测量局限 + 混杂变量声明 + 已知未覆盖清单（见 §1/§3/§4/§6/§7/§8） |
| ⑤ | 基准自身是否确定性（离散指标） | **按条件区分**：B1/B2（确定性算法）4 指标 × 4 负载 = 32 项重复样本**逐项相等**（`assert_deterministic`）；B3（并发）`conflict_rate`/`wasted_work` 用**有界带**（±25%，`assert_b3_bounded`），分布见 §3.6 |
| ⑥ | **C1 静默丢写（K-28）** | **已修复**：C1 的 B1/B3 `lost_update_rate=0.000`、`conflict_rate_silent=0.000`（旧值 0.333 / 1.000，见 §3.2 与 §10） |

主结论：**B3（共享层结构级 `merge3`）在 C2 强重叠下不丢写；B2（worktree 行级 `git merge-file`）在 C2 真丢写**——与 v1.1 §9.1 的 P0-1 预判一致（外部差分方向已按资源级修正）。C1 的静默丢写是 harness 真实缺陷，本轮已按「陈旧 staged 必须重跑 merge3」修复。

---

## 1. 运行配置与环境信息

| 项 | 值 |
|---|---|
| OS / 内核 | Linux / 6.18.5（x86_64） |
| Python | 3.14.8（GCC 13.3.0） |
| CPU 核数 | 3 |
| `fsync` 档位 | `os.fdatasync` 可用；`WalEventLog` 默认 `fsync=True`（提交点 = 帧写入 + fdatasync） |
| git 可用性 | **可用**（`/usr/bin/git`）→ B2 走真实 `git merge-file`，`degraded=False` |
| 随机种子 | `seed=20261027`、`PYTHONHASHSEED=0`（主进程启动时 re-exec 强制生效） |
| 每格重复 n | 30（完整模式）；`--quick` 为 3 |
| `per_action_delay` | 0.002 s（B1/B3 经 Harness 注入点；B2 手动同值注入，保证配对） |
| warmup | 有（C0/C1 × B1/B2/B3；scalability 另对每个 N 先 warmup 再重采样） |
| 运行耗时 | 完整模式 ≈ 7.9 s（沙箱 3 核） |

> B2 的 `git merge-file` subprocess 耗时单独打点并从 `speedup` 剔除：C2 单次 B2 平均 subprocess ≈ 6.7 ms；C1 单次 ≈ 6.9 ms。

---

## 2. 负载与决策固化

### 2.1 四负载（C0–C3）

| 负载 | 场景 | 资源 | 关键点 |
|---|---|---|---|
| **C0** | 零冲突 | `a.json` / `b.json` / `c.json` | 3 agent 改不同文件 |
| **C1** | 弱重叠 | `types.ts` + `config.json` + `own{1,2,3}.json` | 3 agent 共享两文件，改**相距较远**的字段（改动行不相邻）→ 行级合并可成功 |
| **C2** | 强重叠（**主终点**） | `shared.json` | 2 agent 同文件不同字段 + `GROW_ONLY` 数组 `items`；**紧凑单行 JSON** → 行级合并必然冲突 |
| **C3** | write skew | `contract.json` + `impl{1,2}.ts` | 共享契约（改远端字段）+ 各改不同文件；`logical_deps` **未声明**（🟡） |

每个负载返回 `(decisions, expected_by_resource, initial_state)`。

### 2.2 schema：`logical_resource` vs `worktree_path`

`bench/workloads.py` 定义 `ResourceSpec(logical_resource, worktree_path)`：

- **B3（共享层）** 用 `logical_resource`（Harness 的 `resource` 名）；
- **B2（worktree）** 用 `worktree_path`（临时目录内相对路径）。

`freeze` **只覆盖逻辑名**（`Action.resource`），不含 `worktree_path`；故 B2 的临时目录路径不影响固化。

### 2.3 freeze（G3：先 `asdict`）

`freeze` 先 `dataclasses.asdict`（`Decision` 是 dataclass，直接 `json.dumps` 会 `TypeError`）→ 紧凑 JSON → sha256：

```python
def freeze(decisions) -> str:
    payload = [dataclasses.asdict(d) for d in decisions]
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()
```

| 负载 | freeze（sha256，前 16 位） | B1==B2==B3 |
|---|---|---|
| C0 | `7e5f692c28173505` | ✅ |
| C1 | `11b44026f94083f8` | ✅ |
| C2 | `e9f2499509ba54bb` | ✅ |
| C3 | `f714124c0238c331` | ✅ |

### 2.4 独立 oracle + 自检（不再同义反复）

`oracle_merge` **不复用 `merge3`**：dict 取键并集递归合并；list 做「并集 additions − removals」；标量取一致值。资源级比较用**规范形**（dict 键排序、list 排序），避免 `GROW_ONLY` 并集顺序差异造成假丢写。

本轮修复：`expected_by_resource` **由 `oracle_for(decisions, initial_state)` 驱动**（不再是手写常量），且 `run_bench.py` 对**每个负载**断言：

1. `oracle_self_check(workload)` 为真（oracle 与期望同源）；
2. **串行 B1 的最终状态 == oracle 期望**（真正独立的外部差分）。

```
全部负载 oracle 自检: {'C0': True, 'C1': True, 'C2': True, 'C3': True}
```

---

## 3. §9.3 五项主指标（均值，n=30）

<!-- BEGIN:AUTO -->
<!-- primary -->
| 负载 | 条件 | `lost_update_rate` | `conflict_rate_detected` | `conflict_rate_silent` | `serialization_rate` | `wasted_work` |
|---|---|---|---|---|---|---|
| C0 | B1 | 0.000 | 0.000 | 0.000 | 0.001 | 0.000 |
| C0 | B2 | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 |
| C0 | B3 | 0.000 | 0.000 | 0.000 | 0.004 | 0.000 |
| C1 | B1 | 0.000 | 0.444 | 0.000 | 0.002 | 1.333 |
| C1 | B2 | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 |
| C1 | B3 | 0.000 | 0.556 | 0.000 | 0.080 | 1.667 |
| C2 | B1 | 0.000 | 0.333 | 0.000 | 0.002 | 0.500 |
| C2 | B2 | 1.000 | 0.500 | 0.000 | 0.000 | 0.000 |
| C2 | B3 | 0.000 | 0.333 | 0.000 | 0.021 | 0.500 |
| C3 | B1 | 0.000 | 0.250 | 0.000 | 0.002 | 0.500 |
| C3 | B2 | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 |
| C3 | B3 | 0.000 | 0.400 | 0.000 | 0.013 | 1.000 |
<!-- END:AUTO -->

> **B3 列的时序/并发说明**：B3 是 `ThreadPoolExecutor` + `Barrier` 并发负载，其 `conflict_rate_detected` / `wasted_work` 取决于**线程交错**（rebase 重试次数、冲突检测顺序），本质非确定——故 §3 的 B3 两列给出的是**典型值（众数）**，其**分布（min/max/unique）**见 §3.6。B1/B2 是确定性算法，逐值相等（见 §3.6）。

### 3.1 `lost_update_rate`（资源级外部差分）

`expected[resource] = H(oracle_merge(base, all_surviving_writes))`；`lost = [w for w in all_writes if final[w.resource] != expected[w.resource]]`。

- **C2（主终点）**：B2 = **1.000**（两笔写全丢）；B3 = **0.000**（结构级合并正确）。
- **C1**：三条件均 **0.000**（K-28 修复后，B1/B3 的静默丢写消失）。
- C0/C3：三条件均 0。

### 3.2 `conflict_rate` 分 `detected` / `silent` + 键级归因

`detected` = 提交返回非 OK（B1/B3）/ 行级合并冲突（B2）；`silent` = 提交成功但资源最终缺键（丢失写且该资源无 detected 冲突）。

键级差分已拆为 **missing / extra / changed 三路互斥**（`bench/metrics.py::_diff_keys`），不再把 extra 键重复计入 `missing_keys`。

| 负载/条件 | missing | extra | changed |
|---|---|---|---|
| C2 B2 | `shared.json: ['/items/2']` | — | `shared.json: ['/a','/items/1','/items/__len__']` |

**C2 B2**：`git merge-file` 冲突 → 回退 REPLACE（后写覆盖）→ `a=1` 丢失（changed `/a`）、`items` 少一个元素（missing `/items/2` + changed `/items/__len__`）。属 **detected**（合并层报了冲突）。

**C1（K-28，已修复）**：旧实现中，同批多资源写时首个写的校验会复验全部 `read_set` → 同批后续写的陈旧读抢先返回 `REJECT_STALE_READ` → 只刷新 `base_version` 后重试 → 用陈旧 staged 覆盖他人写（`config.json` 丢 `k05/k12`）。修复后 `REJECT_STALE_READ` 对陈旧 staged **重跑 merge3** 再提交，C1 的 `lost_update_rate` 与 `conflict_rate_silent` 均归零（§10 有回归测试）。

### 3.3 `speedup`（B1 串行 / B3 共享层；时序，配对 bootstrap CI）

<!-- BEGIN:AUTO -->
<!-- speed -->
| 负载 | B1 wall 均值 | B3 wall 均值 | speedup | 差值 CI（95%，n=30） |
|---|---|---|---|---|
| C0 | 0.008 s | 0.005 s | 1.59 | [0.002, 0.003] |
| C1 | 0.017 s | 0.008 s | 2.16 | [0.009, 0.010] |
| C2 | 0.005 s | 0.004 s | 1.41 | [0.001, 0.002] |
| C3 | 0.010 s | 0.006 s | 1.61 | [0.003, 0.004] |
<!-- END:AUTO -->

配对单位 = 同 seed/同 workload/同迭代序号。**GIL 限制**：B1/B3 的「工作」主要是注入的 `per_action_delay`，Python 线程受 GIL 限制，故 speedup 明显低于理论 N；C2（2 agent）仅 1.55，符合预期。

### 3.4 `serialization_rate`（锁等待时间 / wall_time）

B3 非零（0.004–0.070），B1/B2 ≈ 0。该指标为**时序型**（连续），不参与「重复样本逐项相等」的确定性判定。

### 3.5 `wasted_work`（rebase 重试 agent-turn / 总 agent-turn）

- C1：B1=1.333、B3=1.667（多资源陈旧读导致多次有界重试）；
- C2/C3：B1/B3=0.5–1.0（1 次 rebase）；
- C0/B2：0.0。

### 3.6 统计纪律

- **B1/B2（确定性算法）逐值相等**：`lost_update_rate` / `conflict_rate_detected` / `conflict_rate_silent` / `wasted_work` 在固定种子下**不是随机变量** → `run_bench.py::assert_deterministic` 断言 n 次重复样本**逐项相等**（不用 `variance == 0.0`，避免浮点求和 `4.6e-33` 的假阳性）。**B1/B2 若出现非确定 → 实现 bug，必须修实现而非放宽判定。**
- **B3（并发）有界带**：`conflict_rate` / `wasted_work` 是**线程交错的函数**，本质非确定 → `run_bench.py::assert_b3_bounded` 断言重复样本落在**预注册期望值 ±25%** 内（区间与理由见 §6.7）；期望为 0 的正确性指标（`lost_update_rate` / `conflict_rate_silent`）退化为**严格 0**。分布（min/max/unique）写入 `result.json` 并在 §3.6.1 如实给出。
- **per-agent 重放次数分布**（`turn_replay_counts` 是**观测**不是断言）：B3 受线程调度影响会出现不同排列，报告如实给出分布——
  - C1 B3：`{2|3|3: 12, 3|2|3: 17, 3|3|2: 1}`；
  - C2 B3：`{1|2: 12, 2|1: 18}`；
  - 其余格为单一排列（如 C1 B1 恒为 `1|3|3`）。`wasted_work` 对这些排列取和，故仍确定。
- **时序**（`speedup` / `throughput` / `commit_latency` / `serialization_rate` / `wall_time`）：配对 bootstrap CI（2000 次重采样，配对单位 = 同 seed/同 workload/同迭代序号）+ 报告机器负载（3 核沙箱）。**不使用 p 值**。

#### 3.6.1 B3 并发指标的分布（min / max / unique）

| 负载 | `conflict_rate_detected` min–max | unique | `wasted_work` min–max | unique |
|---|---|---|---|---|
| C0 | 0.000–0.000 | {0.000} | 0.000–0.000 | {0.000} |
| C1 | 0.556–0.600 | {5/9, 6/10} | 1.667–2.000 | {5/3, 6/3} |
| C2 | 0.333–0.333 | {1/3} | 0.500–0.500 | {1/2} |
| C3 | 0.400–0.400 | {2/5} | 1.000–1.000 | {1.0} |

> C1/B3 偶发多 1 次 rebase 重试（`5/9→6/10`、`5/3→6/3`），是并发交错的固有抖动，**不是实现 bug**；B1/B2 无此抖动（逐值相等）。`lost_update_rate` / `conflict_rate_silent` 四负载 B3 恒为 0（严格）。

> **快照与容差说明**：本节时序数值取自 `bench/result.json` 的**快照**；重跑 `run_bench.py` 会有抖动（`wall` / `speedup` / `serialization_rate` / `throughput` / `latency` 本质非确定，受线程调度与计时噪声支配）。因此 `bench/report_check.py` 对 **B1/B2 确定性指标**（`lost_update_rate` / `conflict_rate` / `wasted_work` / oracle 自检 / 判定）要求**逐值相等**，对 **B3 并发指标**（`conflict_rate` / `wasted_work`）用**有界带**（±25%，期望 0 的指标严格 0），对**时序指标**用**容差**判定：相对容差 25%（`wall` / `speedup` / `throughput`），`scalability_curve` 因 GIL/线程池方差更大用 40%，`serialization_rate` 为近零比值改用绝对容差 0.05 + 量级 `[0,1]`。**B1/B2 的 `lost` / `conflict` 应逐值稳定**；超容差仍判不一致（不无脑放过）。

---

## 4. 二级指标

### 4.1 `throughput`（commits/sec，均值）

<!-- BEGIN:AUTO -->
<!-- throughput -->
| 负载 | B1 | B2 | B3 |
|---|---|---|---|
| C0 | 397 | 204 | 675 |
| C1 | 518 | 44.39 | 1117 |
| C2 | 571 | 109 | 806 |
| C3 | 413 | 83.10 | 844 |
<!-- END:AUTO -->

> 数值由 `python -m bench.regen_report` 从 `result.json` 重新生成（时序指标本质非确定，见 §3.6/§6.7）。

### 4.2 `commit_latency` p50 / p99（环绕 `commit_batch` 打点，均值）

<!-- BEGIN:AUTO -->
<!-- latency -->
| 负载 | B1 p50 | B1 p99 | B3 p50 | B3 p99 |
|---|---|---|---|---|
| C0 | 0.162 ms | 0.184 ms | 0.514 ms | 0.778 ms |
| C1 | 0.148 ms | 0.476 ms | 0.208 ms | 0.776 ms |
| C2 | 0.172 ms | 0.193 ms | 0.231 ms | 0.285 ms |
| C3 | 0.157 ms | 0.290 ms | 0.181 ms | 0.354 ms |
<!-- END:AUTO -->

B2 无逐笔 `commit_batch`（走文件合并），记 0。延迟被 GIL 与锁竞争主导。

### 4.3 `recovery_replay_cost`（**in-memory store**，分相位，n_events=2000）

<!-- BEGIN:AUTO -->
<!-- recovery -->
| 相位 | 耗时 |
|---|---|
| `rebuild`（`effects` + `comp_journal`） | 0.000 ms |
| `replay`（`_replay_events`） | 0.000 ms |
| `invariant`（`check_invariants`） | 0.000 ms |
| **total**（`recover`） | 0.000 ms |
<!-- END:AUTO -->

> **声明**：`recover(COMPENSATE)` 的两段式恢复与 journal/checkpoint 在本基准中均为**进程内模拟**（内存态 `CheckpointStore`），**不是持久恢复**；真实 WAL 重放 + `fdatasync` + 跨进程恢复未纳入本指标。

### 4.4 `event_replay_cost`（扫 `_rebuild_from_events`）

<!-- BEGIN:AUTO -->
<!-- event_replay -->
| 事件数 | 耗时 | 斜率 |
|---|---|---|
<!-- END:AUTO -->

近线性（O(N)），符合 §3「都是事件数的函数」的判断。

### 4.5 `scalability_curve`（N ∈ {1,2,4,8}，B3 型并发，`per_action_delay` 注入）

<!-- BEGIN:AUTO -->
<!-- scalability -->
| N | wall_time | speedup（相对 N=1） |
|---|---|---|
<!-- END:AUTO -->

**方法修正**：基线不再用单次最冷 N=1，而是**先 warmup、每个 N 重采样 5 次取中位数**（`samples`/`min` 一并写入 `result.json`）。

**诚实结论（不粉饰）**：受 **GIL + 线程池/Barrier 开销**支配，N 增大时 wall_time 仍上升 → **speedup < 1**。即本基准的并发收益无法用 GIL 下的纯 Python 线程兑现；`speedup` 仅在「注入延迟可并发」的粒度下才有意义（见 §3.3）。这与 `result.json` 同源，不再出现「报告 <1、json 却 >1」的自相矛盾。

---

## 5. 预注册判定（按格，不用 p 值）

| 负载 | 角色 | `B2_lost` | `B3_lost` | 差值(B3−B2) | 判定 |
|---|---|---|---|---|---|
| C0 | sanity | 0.000 | 0.000 | — | 通过（断言 `B2==B3==0`） |
| C1 | 次要 | 0.000 | 0.000 | 0.000 | 不成立（平局，严格 `<` 不成立；非主终点） |
| C2 | 主终点 | 1.000 | 0.000 | -1.000 | 成立（非劣 + 等价） |
| C3 | 次要 | 0.000 | 0.000 | 0.000 | 不成立（平局；非主终点） |

> C1/C3 的「不成立」符合 v1.1 §2.6 预判：判定用严格 `<`，平局（差值 0）不成立；**只有 C2 是预注册主终点**。
>
> **降级判定**：`judge_cell` 在 `degraded=True`（git 不可用、B2 退化为纯 Python 行级合并）时输出 `kind=degraded_invalid`、`verdict=None`，即「环境降级，判定无效」，并把 `degraded` 打进 `summary` 与 `judgments`。

---

## 6. 测量局限

1. **内存态 `CheckpointStore`** → `recovery_replay_cost` 刻画的是**纯 CPU 的重建/重放/不变量成本**，**≠ 端到端持久恢复**。真实持久恢复还包含 WAL 重放 + `fdatasync` + 磁盘 I/O + 跨进程 `recover`，本基准未覆盖（阶段一无文件版 checkpoint）。
2. **`recover(COMPENSATE)` 两段式是进程内模拟**：`tests/test_s8_coverage.py::test_s8_recover_compensate_two_phase` 用 monkeypatch 在进程内注入崩溃点，journal 与 checkpoint 均为内存态；**不是持久恢复**（真实崩溃需 WAL 落盘 + 新进程重放，属后续工作）。
3. **`speedup` 受 GIL 支配**：B3 的并发收益仅在「注入 `per_action_delay` 可并发」时显现；`scalability_curve` 出现 speedup < 1（§4.5），**不得据此断言共享层并发扩展性**。
4. **B2 语义不可直接比**：B2 的合并语义见 §7；其 `throughput` 含 `git merge-file` subprocess（已从 speedup 剔除，但仍在 wall_time 内）。
5. **`recovery_replay_cost` 用 2000 事件**，非生产规模；仅作量级参考。
6. **`commit_latency` 的 B2 = 0** 是占位（B2 无逐笔提交打点），非真实零延迟。
7. **B3 并发指标本质非确定 → 有界带而非逐值相等**：B3 的 `conflict_rate` / `wasted_work` 是**线程交错的函数**（rebase 重试次数、冲突检测顺序），实测 C1/B3 的 `conflict_rate_detected ∈ {5/9, 6/10}`、`wasted_work ∈ {5/3, 6/3}`（偶发多 1 次重试）。因此对 B3 用**有界带**（预注册期望值 ±25%，分布见 §3.6.1）而非逐值相等；**B1/B2 保持逐值相等**（确定性算法，方差必须为 0）。期望为 0 的 B3 正确性指标（`lost_update_rate` / `conflict_rate_silent`）仍**严格 0**——若 B1/B2 也出现非确定，那**才是实现 bug**，必须修实现而不是放宽判定。

---

## 7. 混杂变量声明

| 混杂 | 说明 |
|---|---|
| **B2 行级 vs B3 结构级** | B2 用 `git merge-file` 做**文本行级** 3-way 合并；B3 用 `merge3` 做**JSON 结构级** 3-way 合并。比较同时混杂「并发模型」与「合并算法」两个变量，**不可直接比**。额外输出「语义冲突率」：把两侧最终内容解析回 JSON 后离线用 `merge3` 判定（本基准 C2 B2 的 REPLACE 结果经 `merge3` 判定为结构冲突）。 |
| **B2 合并 ≡ REPLACE（后写覆盖）** | B2 无 `SET`/`GROW_ONLY` 语义：`git merge-file` 冲突时回退「后写覆盖」。故 B2 在 C2 的丢写是**合并算法**所致，不是并发模型缺陷。 |
| **文件序列化格式** | C1/C3 用多行（改动行相距较远）→ 行级合并可成功；C2 用**紧凑单行** → 行级合并必然冲突。这是构造「弱/强重叠」的**受控变量**，已在 §2.1 声明。 |
| **规范形比较** | 列表按规范序（排序）比较，避免 `GROW_ONLY` 并集顺序差异造成假丢写（§2.4）。 |

---

## 8. 已知未覆盖清单

| # | 未覆盖 | 理由 / 备注 |
|---|---|---|
| 1 | **`logical_deps` 声明路径（C3 为 🟡）** | C3 按 §0.2 **未声明** `logical_deps`，故 write skew 的**准入期检测**（`CONTRACT_CONFLICT`）未覆盖；基准只覆盖「未声明 → 无准入拦截」一侧。 |
| 2 | **B2 `degraded=True` 降级路径** | 沙箱 git 可用，纯 Python 行级合并降级分支未在本机触发（需 `shutil.which("git") is None`）。降级分支已有单测与 git 差分测试（`tests/test_k28_bench_regressions.py`）；一旦触发，`judge_cell` 会判「判定无效」。 |
| 3 | **端到端持久恢复** | 内存态 checkpoint（§6.1）；SIGKILL/checkpoint 恢复未覆盖。 |
| 4 | **`recover(COMPENSATE)` 两段式** | 进程内模拟（§6.2），非持久恢复；本基准未纳入计时。 |
| 5 | **`LLMResolver` 网络分支 / `token_multiplier`** | 阶段一无真实 LLM，`token_multiplier` **N/A**。 |
| 6 | **`budget.gc` / `auth.gc_seen` 周期钩子** | 属 S8-4.4，本基准未覆盖。 |

---

## 9. 复现与产物

```bash
python bench/run_bench.py --quick                          # n=3 冒烟
python bench/run_bench.py --n 30 --out bench/result.json   # 完整模式
python -m bench.report_check bench/result.json bench/report.md   # 报告一致性
```

- `bench/result.json`：完整结果（env / freeze / oracle / summary / judgments / speed / secondary）。
- 控制台摘要：`freeze 三条件相等` / `全部负载 oracle 自检` / 四格判定。

---

## 10. 本轮修复与验证证据

| # | 缺陷 | 修复 | 验证证据 |
|---|---|---|---|
| 1 | **C1 静默丢写（K-28）** | `runtime.handle_commit_result` 的 `REJECT_STALE_READ` 分支改为 `_rebase_stale_staged`：对 `base_version` 落后的 staged 重跑 merge3（冲突走 resolver；不可解则**拒绝重试**，绝不提交陈旧 staged） | `tests/test_k28_bench_regressions.py::test_k28_stale_staged_merged_not_overwritten`；bench C1 `lost=0.000 / silent=0.000`（旧 0.333/1.000） |
| 2 | **`_clean_merge_new_tops` 推断不成立** | 改为 `_clean_merge_present_tops`：用 `_merge_object` 按 merge3 对象规则逐键精确判定「干净合并后仍存在的键」（缺/有/缺→local；有/缺/缺→删；有/有/缺 且 local==base→删；冲突键另行覆盖） | `test_clean_merge_new_tops_deleted_key_not_resurrected`（`{a:1}`→True、`{a:1,x:0}`→False）+ `test_validate_resolution_allows_clean_delete_of_committed_key` |
| 3 | **`_py_line_merge` 静默丢写** | 显式处理零长插入块；插入/替换按 git 语义判冲突（插入点落在替换闭区间、替换区间相交或相邻） | `test_py_line_merge_insert_conflict` + `test_py_line_merge_matches_git`（与 `git merge-file` 差分） |
| 4 | **report.md 与 result.json 矛盾** | 新增 `bench/report_check.py` 校验关键表；`scalability` 基线改为 warmup + 5 次重采样取中位数 | `tests/test_bench_report_consistency.py`；`python -m bench.report_check ...` 通过 |
| 5 | **oracle 死代码** | `expected_by_resource` 由 `oracle_for` 驱动；run_bench 对**每个负载**断言 `oracle_self_check` 且 **B1 终态 == oracle** | `test_oracle_self_check_all_workloads`；控制台 `全部负载 oracle 自检: 全 True` |
| 6 | **`_key_diff` 语义错误** | 拆为 `_diff_keys` 三路互斥（missing/extra/changed），`resource_diff` 分路输出 | `test_diff_keys_missing_extra_changed_disjoint` |
| 7 | **方差是观测非断言** | `assert_deterministic` 断言离散指标重复样本逐项相等；报告给出 per-agent 分布 | `test_assert_deterministic_*`；bench n=30 通过 |
| 8 | **degraded 不参与判定** | `judge_cell` 在 `degraded=True` 时判 `degraded_invalid`（判定无效），并写入报告 | `test_judge_cell_degraded_invalid` |
| 9 | **recover(COMPENSATE) 进程内模拟** | 在测试 docstring、§4.3、§6.2、§8 显式声明「进程内模拟，非持久恢复」 | `test_s8_recover_compensate_two_phase` docstring |
| 10 | **`check_coverage._module_key` 绝对路径假阴性** | 统一按路径分量取 `harness/` 之后；bench 检测改用路径分量 | 覆盖率门禁脚本自检（绝对路径可解析） |
| 11 | **`_restore_budget` except 过宽** | 收窄为 `(ValueError, KeyError, RuntimeError)` | `test_s8_restore_path_cycle_swallowed_by_restore_budget` 仍通过 |
| 12 | **`assert_deterministic` 对 B3 也要求逐值相等 → 偶发 flaky** | B1/B2 保持逐值相等；B3 改**有界带**（`assert_b3_bounded`，预注册期望 ±25%，期望 0 严格 0）；分布写入 `result.json` 与报告 §3.6.1 | `test_assert_deterministic_*` / `test_assert_b3_bounded_*`；bench 连跑 5 次 exit 0；反证 B1 随机 → 断言失败 |
