# Agent Concurrency Correctness Layer —— 可执行实现

设计文档：[`/workspace/docs/multi-agent-harness-design-final.md`](../docs/multi-agent-harness-design-final.md) v1.11
里程碑：[`/workspace/docs/milestones.md`](../docs/milestones.md)
用例集：[`/workspace/docs/use-cases-and-adversarial-tests.md`](../docs/use-cases-and-adversarial-tests.md)

## 运行

```bash
cd /workspace && python3 -m pytest tests/ -q     # 152 passed
```

无第三方依赖（仅标准库 + pytest）。

## 模块

| 模块 | 层 | 对应里程碑 |
|---|---|---|
| `harness/types.py` | L1 | MS0 |
| `harness/event_log.py` | L1 | MS0 |
| `harness/intent_registry.py` | L1 | MS1 |
| `harness/workspace.py` | L1 | MS1 / MS5 |
| `harness/budget.py` | L1 | MS2 |
| `harness/checkpoint.py` | L1 | MS3 |
| `harness/subscription.py` | L1 | MS6 |
| `harness/context.py` | L1 | MS6 |
| `harness/policy.py` | L1 | MS7 |
| `harness/runtime.py` | L3 | MS1–MS4 |
| `harness/orchestration.py` | L4 | MS4 |
| `harness/harness.py` | L5 | 全部 |
| `harness/spec/` | L2 | MS8（资源模型 / 协调协议 / 收据 / 不变量语料） |

## 测试

| 文件 | 覆盖 |
|---|---|
| `tests/test_ms0_event_log.py` | MS0 出口：seq 单调（2 个测试） |
| `tests/test_ms0_skeleton.py` | MS0 出口：Event 全字段 / `append_batch` 连续 seq / `Result` 带载 / 状态机表驱动 / Task·Run 终态分离 / `ManualClock`（9 个测试） |
| `tests/test_ms0_wal.py` | MS0 出口：WAL 尾部半帧截断 / 跨进程 fsync 持久 / 中段坏帧 `WalCorrupt` / 重放重建 seq / 原子写无残留（5 个测试） |
| `tests/test_ms1_workspace_locks.py` | MS1 出口：提交点=WAL + 前像重建 / 读集合陈旧拒绝 / 租约时钟过期 / 锁序不死锁 + 6 个回归（10 个测试） |
| `tests/test_ms1_rebase.py` | MS1 出口：`merge3` 的 `SET` 集合语义（增并删减 / 单方删除 / 对象数组 PIVOT / GROW_ONLY / 嵌套类型）（1 个测试，8 条断言） |
| `tests/test_ms1_rebase_loop.py` | MS1 出口：create 可补偿 + 幂等 / rebase 回路与状态回边 / 重试有界与预算消耗 / Delete 哨兵补偿交叉（4 个测试） |
| `tests/test_ms2_budget.py` | MS2 出口：预算守恒 + 幂等释放 / 并发 reserve + 阴性对照 + yield 注入 / 准入四级零残留 / dispatch_and_run 唯一入口 / 能力收口（14 个测试） |
| `tests/test_ms3_recovery.py` | MS3 出口：effect UNKNOWN 反查三分支 / 补偿中途崩溃续做（批次原子）/ idem_key 稳定性 / 逐笔账本恢复 / 补偿幂等键 / 生产不变量 / network+data_class+签名 / requeue_readmit（8 个测试） |
| `tests/test_ms4_runtime.py` | MS4 出口：动作注册表（5 kind + 未知 kind + 异常映射 + 重复注册）/ guard 谓词与续租三态 / 事件游标 / `_drain_scheduler` 防递归 / 前驱闸门 / 环检测（16 个测试） |
| `tests/test_e2e_invariants.py` | 端到端不变量：I1/I2/I3/I5/I6/I8/I10/I9/I12 + I4/I7/I11 补强（10 个测试，全部串行确定性构造） |
| `tests/test_s8_coverage.py` | S8 覆盖率补强：`recover(COMPENSATE)` 两段式 / resolver 成功路径与反证 / `_restore_path` 逐级与回滚 / `auth.gc_seen` / `budget.gc` / S8-1..S8-4（15 个测试） |
| `tests/test_k28_bench_regressions.py` | K-28 基准回归：陈旧 staged 必须重合并（含反证）/ `_validate_resolution` 精确语义 / B2 降级合并与 git 差分 / 报告一致性 |
| `tests/test_real_cases.py` | U1（2 个）、U2、U3、U4 |
| `tests/test_adversarial.py` | A1–A13（含 A7b 背压） |
| `tests/test_ms7_capability.py` | MS7 出口：能力强制（2 个测试） |
| `tests/test_ms8_conformance.py` | MS8 出口：I1/I2/I3/I5/I6/I7/I8 |
| `tests/test_ms8_spec.py` | MS8 出口：I9/I10/I13 + 资源模型 + 协调协议 + 收据链 |

> **总计 152 个测试**：19 个用例测试 + 133 个里程碑出口/规范/E2E/覆盖率/基准回归测试。
> 按里程碑切子集：`pytest -m ms0`（14）、`-m ms1`（15）、`-m ms2`（9）、`-m ms3`（8）、`-m ms4`（16）、`-m e2e`（10）、`-m fault`（S8-1）；markers 见 `pyproject.toml`。
> 覆盖率：`python3 -m pytest --cov=harness --cov-report=json && python3 scripts/check_coverage.py`（整体 83.89%，分模块硬门禁通过）。
> 基准：`python3 bench/run_bench.py --n 30 --out bench/result.json`（三条件×四负载，约 8s）。

## 设计要点

- **RSC = OCC + 租约 fencing + 短临界区 + Saga 补偿**（非 2PC）
- **EventLog = WAL/LSM**（全局单调 seq，B3 单写者）
- **CheckpointStore = 唯一恢复依据**；EventLog 是侧通道
- **idem_key = hash(run_id, logical_subgoal_id, hash(tool, hash(args)))**（不含 task_id）
- **Z3 = per-agent worktree**；`prepare/apply` = merge 到 Z2 的 merge 点
- **ReconcileLoop = K8s level-triggered reconcile**

## 可扩展点

所有组件均为接口 + 可替换实现，见 `docs/milestones.md` §1.3。
生产化时替换：`EventLog`→WAL/Raft、`WorkspaceManager`→FS/S3、`PolicyEngine`→OPA。
