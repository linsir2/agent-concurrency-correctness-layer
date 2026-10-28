# S24 边界复核与优化方案（评审版）

| 项目 | 内容 |
|---|---|
| 日期 | 2026-10-29 |
| 基线 | `355b29c`（S24-1..11 全部完成并推送） |
| 方法 | 三个子智能体分别调研「确定性/诊断」「基准/验证/语义」「租约/TLA/语料/文档」，我逐条核对代码与实测后裁决 |
| 结论 | 11 条边界可消除/收窄，4 条明确不做；其中发现 **1 个 P0 真 bug**（只读 API 有写副作用并 fence 活跃写者） |

---

## 0. 评审修正（不采信子智能体原话的部分）

1. **pysyncobj 当前未安装**（实测 `import pysyncobj` 失败；`tests/test_s24_raft_lease.py`
   现为 1 passed / 6 xfailed）。文档里「7 条真实 Raft 用例」是上一环境的事实，
   本环境不可复现 → 需要「可复现的 Raft 环境脚本 + 可选 CI」，而不是继续声称默认门禁覆盖。
2. 不建议引入 `docs/status.json` 作为第二真值源：只做**针对现有文档的断言检查**，
   避免又一份会腐化的状态文件。
3. 语料扩展拆两步：先做 I3/I9 **纯函数向量**；I6/I8/I11/I12/I14 的 trace checker
   单独立项（成本高、易变成「伪运行期」）。
4. 基准「n=5」可做，但**不产出显著性结论**；只报逐样本值 + 中位数/极差 + 自查区间。
5. `verify` 的「只验证文件视图」不改成逻辑资源视图：没有稳定路径映射时只会假绿。

---

## 1. 边界裁决

| # | 边界 | 裁决 | 关键证据 |
|---|---|---|---|
| 1 | **`next_deadline_ms()` 只读却有副作用**（本轮新发现） | **P0 必修（真 bug）** | 实测：新进程调用后 `wal.log.epoch` 0→1、`slots.active_keys()` 多出 `A-rsc-…`。根因：`_pending_deadlines` → `_tick_candidates(persist_scan=True)` → `_restore_acb(payload)` 默认 `bump_epoch=True`（`harness/harness.py:4608`）且 `slots.try_acquire`（`:4604`）。后果：旁观者会 fence 活跃写者（其下一次 append `fencing_epoch` 落后 → `FencingRejected`）。 |
| 2 | `scan_deadlines` 只扫内存 | **P0 同修** | `harness/orchestration.py:1350` 只遍历 `self.h.acbs`，而 `_pending_deadlines` 扫持久集（`harness/harness.py:2974`）→ 未 `recover` 的 aging 到期项让 `pump()` 恒 >0。 |
| 3 | HITL `approval_id` 用 `uuid4` | **P0 小修** | `harness/runtime.py:339`；进入 `APPROVAL_RAISED` payload（`:353-356`）与 checkpoint → 同 seed 的 HITL 场景不可逐字节重放。 |
| 4 | Verifier 只杀直接子进程 | **P0 小修** | `harness/facade.py:546` 用 `subprocess.run(timeout=...)`；超时不杀进程组 → 孙进程存活、继续占端口/写文件。 |
| 5 | 事件层 fence 不校验「同一 RSM」 | **P0 小修** | `harness/harness.py:830` 只判断 `hasattr(replica,"lease")`；配置成两个不同 cluster 时会「装上假 fence」（写入口有检查、提交点没有）。 |
| 6 | 文档与代码不一致 | **P0 必修** | `docs/milestones.md:300-301`（MS9/MS10「未开工」）、MS5 出口未含 VerifierRunner、`docs/positioning-and-roadmap.md` §2.4/附录 A.2 与代码相反。 |
| 7 | 语义检测只覆盖「名字消失」 | **P1 可收窄** | `harness/semantic.py`；实测漏报：函数签名变更、类方法删除、`app.core.compute_rate` 点号链。纯 `ast` 可做保守版本。 |
| 8 | token 基准 n=1、thinking 未关 | **P1 可消除/收窄** | `bench/llm_replay.py:192`；`harness/llm.py` 未透传 `enable_thinking`、未记 `finish_reason`。实测该模型 `max_tokens=8` 仍耗 ~100+ completion（thinking 计入）。 |
| 9 | Verifier 整树复制、单命令、无诊断 | **P1 可收窄** | `harness/facade.py:544` 每次 `copytree`；成功/失败无耗时/文件数细节；多步命令不支持。 |
| 10 | 诊断不校验 checkpoint 语义 | **P1 可收窄** | `harness/diagnose.py` 只做结构检查；空转现场 `ok=True`、无 finding。 |
| 11 | 语料只有 I1/I2/I13 可执行 | **P1 可收窄（先纯函数）** | `harness/spec/conformance/manifest.json` 的 `executable_invariants`。 |
| 12 | TLA 只有 4 个安全模型、需 jar | **P2 可收窄** | `spec/tla/`；缺 I3（fencing 单调）——正是租约簇的核心；无活性/公平性。 |
| 13 | 跨主机只验本地 3 节点 | **P2 可收窄到多进程** | 沙箱无 `CAP_NET_ADMIN`（`unshare --net` EPERM、无 iptables）；可退到 3 个 OS 进程 + 真 TCP + SIGSTOP/SIGKILL；真多机/分区**不可做**。 |
| 14 | checkpoint `store_uuid`/ref 名不确定 | **P2 可消除** | `harness/checkpoint.py:560/919/1204`；ref 唯一性已由 seq 保证，后缀可换熵源。 |
| 15 | 确定性只承诺单线程 | **P2 收窄（守卫，不消除）** | `pool`/`merge_queue` 是真实线程；若要消除得做全事件循环重写，收益/风险不划算；改为「跨线程使用熵源即拒绝」。 |
| 16 | per-resource 租约分片 | **不做** | 与全局 `wal.log.epoch`、单写者收据链假设冲突，应单独立项。 |
| 17 | Verifier 替代 CI / 逻辑资源视图 | **不做** | 无路径映射时只会假绿；保持「一条命令 + 文件视图」的定位。 |
| 18 | 动态引用/多语言语义分析 | **不做** | 需要类型推断/运行时，超出零依赖边界。 |
| 19 | TLA refinement 证明 | **不做** | 给 Python 实现建 refinement 易变「伪形式化」；用 trace 一致性替代。 |
| 20 | 诊断自动修复/在线 daemon | **不做** | 只读红线是信任基础；最多做 `--dry-run` 建议。 |

---

## 2. 实施清单（按优先级，每步可独立提交）

### P0（正确性与信任，建议一次做完）

1. **读取面纯化 + aging 扫描统一**
   - `_restore_acb(..., for_sweep: bool=False)`：`for_sweep=True` 时**跳过**
     `bump_epoch()` 与 `slots.try_acquire()`（只水合字段）。
   - `_tick_candidates(persist_scan=True, for_sweep=False)`；`_pending_deadlines` 用
     `for_sweep=True`。
   - `tick()` 只算一次 candidates，并把同一列表传给
     `reconcile.scan_deadlines(now, candidates=...)`（签名兼容旧调用）。
   - 测试：`test_next_deadline_ms_is_read_only`（epoch 文件字节与 slots 不变）、
     `test_pump_settles_persisted_aging_without_full_recover`（pump 收敛到 0）；
     必杀变异：恢复副作用 → 两条必红。
2. **HITL 可重放**：`runtime.py:339` 用 `h.entropy.uuid("approval_id")`（无熵源仍 uuid4）。
   测试：同 seed 的 HITL 场景 WAL 逐字节相等；回退 uuid4 必红。
3. **verify 进程组**：`Popen(start_new_session=True)` + `communicate(timeout)` +
   `os.killpg(SIGKILL)`；测试孙进程在 2×timeout 内消失；单命令/退出码语义不变。
4. **fence 身份**：`_wire_lease_fence` 要求 `event_log._cluster is lease_store.cluster`，
   否则构造期 `LeaseUnavailable`；`set_lease_fence` 同步校验。测试（缺 pysyncobj 时 xfail）：
   错配 → 构造失败；共享 → 旧 epoch 提交被拒。
5. **文档真值 + 守卫**：修 3 处；新增 `scripts/check_docs_consistency.py` +
   `tests/test_s24_docs.py`（向量数、测试指针、MS5 出口、§2.4 状态、禁用「未开工」）。
   验收：3 处腐化在临时副本中各产生 ≥1 finding；HEAD 0 finding。

### P1（差异化）

6. **语义扩展**：`signature_incompatible`（post 调用点用被删关键字/少传必选位参）+
   `removed_member`（`Cfg().m` / `Cfg.m`）；跳过装饰器、`*args/**kwargs`、`@overload`、
   有 bases 的类。正负用例各 6；200 文件 <2s。
7. **基准 n=5 + thinking 开关**：`chat_with_usage(extra=None)` 透传
   `{"enable_thinking": False}`；`LLMReply.finish_reason`；fixture v2 多样本；
   `compute_metrics` 增中位数/极差（n>1），固定文案「描述性，不作显著性」。
   无 key → skip 且不落数字；任一样本 `usage_missing` → 拒绝整样本。
8. **Verifier 诊断/多步**：`verify_steps` fail-fast；`verify_include/exclude` 缩小复制集合；
   detail 增 `copy_ms/files/run_ms`（仅开启诊断时）。隔离仍是新副本，不用硬链接。
9. **诊断补检查**：checkpoint envelope/payload checksum、`AGING_OVERDUE`（带 `hint`）。
   空转现场必须报 finding。
10. **语料纯函数扩展**：I3（fencing 单调）、I9（plan 无环）向量；
    `corpus_version` minor 升级；`--check` 仍防漂移。
11. **TLA I3 + 活性**：`I3FencingMonotonic`（broken 去掉 epoch 校验）、I1 活性 cfg
    （`PROPERTY` + `WF_vars`）；`check_tla.py` 支持 property 失败匹配；可选 CI job
    （不阻塞主门禁）。

### P2（需要环境/收益延后）

12. **多进程 Raft 故障**：`harness/storage/raft_node.py` + `tests/_raft_proc.py`；
   3 OS 进程 + 真 TCP，SIGSTOP/SIGKILL；先 `pip install pysyncobj` 到专用 venv 才可跑，
   缺依赖显式 xfail。诚实口径：单主机、共享时钟、无丢包/重排。
13. **确定性 checkpoint**：`FileCheckpointStore(entropy=..., clock=...)`；
   同 seed 全目录文件名+字节一致；默认不变。
14. **跨进程重放门禁**：`PYTHONHASHSEED` 矩阵 + 子进程用例（复用 `tests/_s10_child.py` 模式）。
15. **确定性线程守卫**：`Deterministic` 记录首个线程，跨线程调用即 `ValueError`。

---

## 3. 明确不做

- per-resource/分片租约；跨主机 worktree/文件同步。
- Verifier 替代 CI、逻辑资源视图。
- 动态引用/多语言语义分析、TLA refinement。
- 诊断自动修复/在线 daemon。
- 真多机/网络分区/时钟调整（沙箱无 `CAP_NET_ADMIN`；只能进程级故障）。

---

## 4. 验收基线

- 每步独立提交；主门禁保持 `pytest -q` 全绿、`ruff` 0、S24 conformance 不退化。
- 新增测试必须能在**无网络、无 pysyncobj、无 Java** 的默认环境运行；
  依赖外部能力的用例显式 xfail/skip（不假绿）。
- 默认路径（未开启新参数）事件字节不变；由确定性用例与 conformance 双重守卫。

---

## 5. 实施状态（2026-10-29）

| 步骤 | 状态 | 提交/证据 |
|---|---|---|
| P0-1 读取面纯化 + aging 期限恢复 | ✅ | `859378c`；4 用例；实测 `next_deadline_ms()` 后 epoch 0→0、WAL 字节不变 |
| P0-2 HITL `approval_id` 可重放 | ✅ | `e265356`；同 seed HITL WAL 逐字节相等 |
| P0-3 Verifier 杀进程组 | ✅ | `eebc986`；孙进程 marker 不再出现；成功路径不变 |
| P0-4 lease fence 同集群 | ✅ | `f405ba4`；错配构造期 `LeaseUnavailable` |
| P0-5 文档真值 + 守卫 | ✅ | `a4bd2e1`；`scripts/check_docs_consistency.py` + 7 用例；HEAD 0 findings |

> 环境说明：pysyncobj 在本环境重新安装后 raft 用例恢复真跑；TLA jar 需按 README 下载。
> 主门禁当前快照：**851 passed / 14 skipped / 4 xfailed**；带 TLA jar：**859 / 6 / 4**。
