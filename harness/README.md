# Agent Concurrency Correctness Layer —— 可执行实现

设计文档：[`/workspace/docs/multi-agent-harness-design-final.md`](../docs/multi-agent-harness-design-final.md) v1.11
里程碑：[`/workspace/docs/milestones.md`](../docs/milestones.md)
用例集：[`/workspace/docs/use-cases-and-adversarial-tests.md`](../docs/use-cases-and-adversarial-tests.md)

## 运行

```bash
cd /workspace && python3 -m pytest tests/ -q     # 546 passed
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
| `harness/spec/corpus.py` + `harness/spec/conformance/` | L2 | S24-5（对外合规语料：语言无关 JSON 向量 + manifest + runner，`--check` 防漂移） |
| `harness/diagnose.py` | 运维 | S24-6（只读诊断：WAL / 收据链 / 版本流 / effect / 租约 / token，`python3 -m harness.diagnose`） |
| `harness/storage/lease.py` | L1 | S24-8（可复制写者租约：`LeaseState` 纯状态机 + `MemoryLeaseStore`；Raft 后端见 `storage/raft.py` 的 `RaftLeaseStore`） |
| `bench/llm_replay.py` | 基准 | S24-9（真实模型 usage 录制 + 离线回放：token 倍数 / rework_rate；无 key 显式 skip） |
| `harness/entropy.py` | L1 | S24-10（确定性原语：域隔离 seeded uuid/token + 严格单调模拟时钟） |
| `harness/timer.py` | L1 | S24-11（DurableTimer：SET/FIRED/CANCELLED 事件溯源，重启重建、幂等触发） |
| `harness/definitions.py` | L2 | S24-11（定义版本化：content-hash 钉住，未知/异 hash 拒绝） |
| `harness/worktree.py` | L1 | S22（Z3 私有工作区） |
| `harness/facade.py` | L5 | S22/S24（`RscSession` 门面；S24 增加 `verify` 提交前验证 + `semantic` 静态门禁） |
| `harness/semantic.py` | L2 | S24-3（符号级语义冲突：零依赖 `ast`，仅 Python） |
| `harness/merge_queue.py` | 适配 | S24-4（单写者 FIFO merge queue，验证与提交串行） |
| `harness/integrations/` | 适配 | S22/S23（LangGraph / MCP / CrewAI / OpenAI Agents / AG2 / HTTP sidecar，可选依赖） |

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
| `tests/test_s9_contract.py` | S9a 契约：`CrashInjected` 继承与穿透（handler/probe）/ `retention` 安全语义（无快照不跳、水位不足不跳、retention=0 不限）+ 负对照 |
| `tests/test_s9_receipts.py` | S9b 收据链：同帧（`WalEventLog`）/ 投影前崩溃去重 / 并发正负对照 / 未签名三态 / 篡改 / 空链与重复 id / head 重建 |
| `tests/test_s9_persistence.py` | S9c 文件 store：单进程重开 / ref 唯一 / 损坏隔离（`CheckpointCorrupt` + 头部兜底）/ 旧格式兼容 / codec 单例往返 |
| `tests/test_s9_cross_process.py` | S9d 跨进程：durable 三件套 + 子进程从 seq 0 重建 + `latest_ref_for_acb` + 收据链校验 + `context_view` 非空 |
| `tests/test_s9_sigkill.py` | S9d SIGKILL：四阶段 barrier（半写 tmp / link 前 / replace 后未写指针 / 指针写入后）+ 原子性与隔离断言（`@pytest.mark.fault`） |
| `tests/test_review_counterexamples.py` | S9 评审 8 条反例回归（P0 dedup 丢写 / 幂等键 / 部分命中 / 嵌套 bytes / 槽位泄漏 / 幽灵 ACB / retention 丢状态 / 恢复丢订阅） |
| `tests/test_p2_review.py` | S9 终审 P2 回归（blob 隔离 / 陈旧内容去重 / ReceiptConflict 隔离与对账 / 指针单调 CAS / seq 不回退 / 畸形收据） |
| `tests/test_s10_budget_reconcile.py` | S10a 预算对账：跨进程不超发（含前提断言 + 负对照）/ run_limits 漂移 / 重放锚定事件身份 / 树完整性（epoch 过期真实路径）/ 全局对账等式 / 预算事件不触发 gc |
| `tests/test_s10_cross_process_locks.py` | S10b 跨进程锁：scope flock（文件信号 barrier 正负对照）/ WAL seq 不冲突 / fencing / 无循环导入 / 锁序 |
| `tests/test_s11_gate.py` | S11a 统一 gate：穷举 5 kind 前置（正/负对照）/ `_commit_staged` 边界 / policy 通配·CIDR·端口 + 分级序 / 垃圾 token 拒绝 |
| `tests/test_s11_receipt_index.py` | S11b：收据增量索引（枚举线性 + 索引==全量等价）/ 回扫重置 / fencing 绑 WAL / 干净重启 / WAL generation |
| `tests/test_s11_egress.py` | S11c/d/e：DNS 钉住 + SSRF 完整 IP 列表（16 参数化）/ 重定向 302 真拒绝 / netns 正负对照 / 模式互斥 / 失败清理 / 降级 fail-closed |
| `tests/_egress_child.py` | S11 子进程脚本（sandbox/netns 探针，pytest 不收集） |
| `tests/test_s13_hitl.py` | S13a：HITL 三态（`_UNSET`/deny/timeout）/ 跨进程恢复仍 WAITING / 键不串 / tick 驱动 |
| `tests/test_s13_replan.py` | S13b：replan alias 三处统一 / 预算事件计数 / 再准入清理 / 同版本双准入 / 覆盖缺口 16 条 |
| `tests/test_s13_spawn.py` | S13c：能力子集分级序 / 并发 spawn 守恒 / 取消级联（含崩溃后）/ 有界 / 孤儿回收 / `ResultEnvelope` |
| `tests/test_s13_scheduler.py` | S13d：pool 单所有者 / `CrashInjected` 重抛 / 负对照双跑 / aging 边界 / 嵌套不死锁 / claim 过期 fencing |
| `tests/test_s14_boundaries.py` | S14：K-69（未 recover 的等待态经 tick 结算 + 负对照）/ 不动点 / pump / 锁序 / K-68 条件写（含负对照 + 耗尽）/ K-71(a) 终局 / K-67 edges 物化 + 剪枝判据 |
| `tests/test_s14_i12.py` | S14：MS8 I12 不饿死（aging 有界离开 ADMITTED；负对照 aging_ms=0 永停） |
| `tests/test_s15_contract_drift.py` | S15 C4：契约反漂移（`ast` 扫 `self[.h].<comp>.<member>`，排除赋值）+ 自检 + 负对照 + `@runtime_checkable` |
| `tests/test_s15_backend_conformance.py` | S15 C3：后端一致性（CheckpointStore 内存/文件 ×7、EventLog 内存/WAL ×5） |
| `tests/test_s15_ingress.py` | S15 D1：Ingress 幂等（空 key / 重放 / CONFLICT / 跨进程 / 内存态去重负对照） |
| `tests/test_s15_admin.py` | S15 D2/D3/D4：DLQ redrive（有界 + 真执行）、pause/resume（保留资源 + 持久化 + 幂等）、metrics |
| `tests/test_s16_raft.py` | S16：Raft 后端（选主 / seq 无空洞 / 并发串行化 / 三副本收敛 / 契约 / replay 过滤 / E2E 装配 / epoch=term / 无库 fail-closed） |
| `tests/test_s17_raft_boundaries.py` | S17：K-81 确定性 rid（三副本一致 + 重启续号）/ K-82 require_leader / K-83 生产配置 / K-85 bump_epoch 抛错 / K-86 快照恢复 |
| `tests/test_s17_ms10.py` | S17 MS10：`test_dlq` 毒任务 / `test_admin_pause_resume` / `test_ingress_idempotent` / drain / kill / purge / 可观测导出 |
| `tests/test_s18_raft_rsm.py` | S18：K-87 预算树/归档折叠/不变量跨副本一致 + K-82 barrier 线性一致读 + K-83 from_addrs + 终审 4 P0 回归 |
| `tests/test_s19_budget_gc.py` | S19：K-88 三栅栏修剪（三个负对照 + 有界性 + 剪后不变量）/ K-89 claim 复制 / K-90 归档时间戳 / K-91 remaining / K-92 预检 / K-93 owner |
| `tests/test_s20_tenant_metering.py` | S20：K-94 有界墓碑（剪后旧 worker 仍被拒 + 有界）/ K-95 fail-safe / K-96 连通性复核 / 多租户配额隔离 / 计量幂等 / 统一 Admin |
| `tests/test_s21_tenant_and_leases.py` | S21：K-97 tenant 自动接准入（含负对照/配额隔离/计量一致性）/ K-99 失效租约回收（含 `tick` 驱动）/ K-100 窗口量化 + 逐墓碑年龄淘汰 + 三副本上限一致性 / 优先级份额（reserved / 超额借用 / 上限与冲突拒绝） |
| `tests/test_s22_core.py` | S22 内核：公共 `abort`/`finish`、外部幂等键、token 回收、懒恢复（无全局 epoch 推进） |
| `tests/test_s22_worktree.py` | S22 Z3：git/copy worktree 隔离、改动检测（含同尺寸同 mtime）、路径逃逸、并发 create |
| `tests/test_s22_facade.py` | S22 门面：并发冲突 / 同键重放 / 跨会话命名空间 / 过期 token / 预算回收 / 重启恢复 |
| `tests/test_s22_langgraph.py` | S22 LangGraph：root 注入、读回、sync+async、Conflict→Command、worktree 缺失 fail-closed |
| `tests/test_s22_mcp.py` | S22 MCP：单次 apply / 多步会话 / 幂等 / 删除 / agent_id 不可冒名 |
| `tests/test_s23_crewai.py` | S23 CrewAI：包装 `BaseTool.run`、root 注入、读回提交、同参幂等、stale→CONFLICT、预算 WAIT、协调器 DENY（`importorskip`） |
| `tests/test_s23_openai_agents.py` | S23 OpenAI Agents：guardrail 顺序、schema 去 root、`tool_call_id` 必填 fail-closed、dispositions、共享 cwd 模式（`importorskip`） |
| `tests/test_s23_ag2.py` | S23 AG2：`on_tool_execution` 短路、`serialized_arguments` 注入、stale→CONFLICT、预算 WAIT、未声明透传（`importorskip`） |
| `tests/test_s23_sidecar.py` | S23 HTTP sidecar：鉴权（401/403）、幂等重放、stale 409、411/413/501、404/410、agent 不可冒名、tick 回收 |
| `tests/test_s23_example.py` | S23 示例冒烟：`examples/three_agents/run.py` 的 naive 丢写 vs RSC 保全、HTTP 401/409/403 |
| `tests/test_s24_verify.py` | S24 Verifier：合并视图拦住语义冲突、红/超时/无工作区拒绝、收据 `verified`、副本不污染 |
| `tests/test_s24_experiment.py` | S24 实验冒烟：git 合并干净但检查红、无 verify 放行、有 verify 拒绝、修正后放行 |
| `tests/test_s24_semantic.py` | S24-3 静态语义：改名/删模块检出、别名/星号/动态/注解/条件定义抑制、三态门禁、性能上界 |
| `tests/test_s24_queue.py` | S24-4 队列：FIFO、容量满、超时、取消、队首重验拦住语义冲突、关闭清理 |
| `tests/test_s24_corpus.py` | S24-5 语料：磁盘无漂移、向量可执行、manifest 计数/sha、冻结 HMAC、变异检测、CLI 0/1、不变量测试指针存在 |
| `tests/test_s24_diagnose.py` | S24-6 诊断：干净状态、只读（目录哈希不变）、中段 CRC、尾部截断、seq 断档、断链/重复 id、版本跳跃、重复幂等键、租约 epoch 落后、CLI 0/1/2 |
| `tests/test_s24_tla.py` | S24-7 TLA+：四模型资产 + 正确/反例 TLC 实测（缺 java/jar 时显式 skip） |
| `tests/test_s24_lease.py` | S24-8 租约（内存后端）：单持有者/接管 epoch+1/旧 epoch 续租被拒/Harness 第二写者被拒/存储故障不回退 |
| `tests/test_s24_raft_lease.py` | S24-8 租约（真实 3 节点 Raft）：竞争唯一 winner、旧 append 在提交点被拒且 tail 不变、共享集群要求、缺 pysyncobj fail-closed |
| `tests/test_s24_llm_usage.py` | S24-9 LLM usage：OpenAI/Anthropic 字段、缺 usage 标 `usage_missing`（不估算）、`chat()` 兼容、重试 attempts |
| `tests/test_s24_replay.py` | S24-9 token 回放：fixture 算术自检、指标精确重算、删重试 turn 的必杀变异、CLI JSON/报告、无 key skip 且无数字 |
| `tests/test_s24_determinism.py` | S24-10 确定性：同 seed WAL 逐字节相等、换 seed/默认模式必不同、域隔离/单调时钟、拒绝 auth/pool/merge_queue |
| `tests/test_s24_timer.py` | S24-11 timer：到期才触发、取消、重复调度拒绝、WAL 重启重建、已触发不双触发、tick/pump 收敛、默认关闭无事件 |
| `tests/test_s24_definitions.py` | S24-11 定义：content hash、同名异内容冲突、未知/异 hash 准入拒绝（零残留）、热更新后旧 ACB 提交被拒 |
| `tests/test_s12_audit_and_isolation.py` | S12：K-45 fail-closed（三路径注入 + 粘滞 + 静默分叉负对照）/ 预算事件 run_id / per-run epoch·retention 参数化 / `unshare -Urn` 与 `runsc` / Landlock errno 38·95 / 生产 gate |
| `tests/_s10_child.py` | S10 子进程脚本（scope-lock / wal-append 两模式，pytest 不收集） |
| `tests/test_real_cases.py` | U1（2 个）、U2、U3、U4 |
| `tests/test_adversarial.py` | A1–A13（含 A7b 背压） |
| `tests/test_ms7_capability.py` | MS7 出口：能力强制（2 个测试） |
| `tests/test_ms8_conformance.py` | MS8 出口：I1/I2/I3/I5/I6/I7/I8 |
| `tests/test_ms8_spec.py` | MS8 出口：I9/I10/I13 + 资源模型 + 协调协议 + 收据链 |

> **总计 546 个测试**（另 4 个 xfail 为环境能力缺失显式登记）：19 个用例测试 + 527 个里程碑/规范/E2E/覆盖率/基准/持久化/收据/跨进程对账/egress 测试。
> 按里程碑切子集：`pytest -m ms0`（14）、`-m ms1`（15）、`-m ms2`（9）、`-m ms3`（8）、`-m ms4`（16）、`-m e2e`（10）、`-m fault`（5）、`-m sandbox`/`-m netns`/`-m egress`；markers 见 `pyproject.toml`。
> 覆盖率：`python3 -m pytest --cov=harness --cov-report=json && python3 scripts/check_coverage.py`（整体 83.06%，分模块硬门禁通过）。
> 基准：`python3 bench/run_bench.py --n 30 --out bench/result.json`（三条件×四负载，约 8s）；报告数值表用 `python3 -m bench.regen_report` 从 `result.json` 重新生成。
> egress：外部库（`sandlock`/`py-landlock`/`pyroute2`/`mitmproxy`）放 `optional-dependencies`；默认**出站 fail-closed**，生产用 `HARNESS_EGRESS_REQUIRED=1` 或 `egress_required=True` 强制启动即校验。netns/代理为**部署级**（需 root）。

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
