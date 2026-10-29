"""S14 方案 ↔ 实现 一致性核对（可执行）。

防止「文档说做了但代码没做」/「代码做了但语义与文档不符」。
用法：`python3 scripts/check_s14_conformance.py`（exit 0 = 全部一致）。
对应 `docs/s14-boundaries-and-ms8-closure.md` §2–§4 与 §9.1。
"""
import inspect
import os
import pathlib
import sys

_ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT)); sys.path.insert(0, str(_ROOT / 'tests'))
from harness.kernel.dispatch import DispatchService
from harness.kernel.tasks import TaskService
from harness.kernel.children import ChildManager
from harness.kernel.commit import CommitService
from harness.kernel.receipts import ReceiptChain
from harness.kernel.leases import WriterLeaseService
from harness import Harness, TaskState
from harness.checkpoint import CheckpointStore, FileCheckpointStore
from harness.kernel.tasks import TaskService
from harness.orchestration import PREDECESSOR_SATISFIED, Planner
from harness.types import TRANSITIONS, ResultEnvelope

ok, bad = [], []


_OWNERS = (DispatchService, TaskService, ChildManager, CommitService,
           ReceiptChain, WriterLeaseService, Harness)


def _impl(name: str) -> str:
    """`name` 的**实现**源码（S3：拆到 kernel/*.py 后 Harness 只剩委托）。

    按「服务优先、Harness 兜底」解析：同一名字在服务上有实现、在 Harness 上
    只有一行转发时，必须取实现，否则源码文本断言变成空断言。
    """
    for owner in _OWNERS:
        if hasattr(owner, name):
            return inspect.getsource(getattr(owner, name))
    raise AttributeError(name)


def chk(name, cond, detail=""):
    (ok if cond else bad).append((name, detail))

# --- K-69
src = _impl("tick")
chk("K-69 tick 用 _tick_candidates(persist_scan=)", "_tick_candidates(persist_scan=persist_scan)" in src)
chk("K-69 tick 签名含 persist_scan", "persist_scan" in inspect.signature(DispatchService.tick).parameters)
c = _impl("_tick_candidates")
chk("K-69 候选集含 refs_all", "refs_all()" in c)
chk("K-69 候选集含内存", "self.state.acbs.values()" in c)
chk("K-69 水合失败记录 _tick_skipped", "tick_load_failed" in c and "tick_hydrate_failed" in c)
chk("K-69 recover 传 persist_scan=False", "tick(persist_scan=False)" in inspect.getsource(Harness.recover))
chk("K-69 next_deadline_ms 存在", hasattr(Harness, "next_deadline_ms"))
chk("K-69 pump 存在", hasattr(Harness, "pump"))
pt = inspect.getsource(Harness.tick)
chk("K-69 锁序：tick 内无 with store 锁包 _finalize", "_flock()" not in pt and "_thread_lock" not in pt)

# --- K-68
sig_m = inspect.signature(CheckpointStore.update_children).parameters
sig_f = inspect.signature(FileCheckpointStore.update_children).parameters
chk("K-68 内存版有 expect_ref/retries", "expect_ref" in sig_m and "retries" in sig_m)
chk("K-68 文件版有 expect_ref/retries", "expect_ref" in sig_f and "retries" in sig_f)
chk("K-68 文件版 _save_payload_locked 存在", hasattr(FileCheckpointStore, "_save_payload_locked"))
chk("K-68 冲突用最新值重读重应用", "expect_ref = ref" in inspect.getsource(FileCheckpointStore.update_children))
chk("K-68 耗尽抛 FencingRejected", "FencingRejected" in inspect.getsource(FileCheckpointStore.update_children))

# --- K-71(a)
chk("K-71a 新边 ADMITTED->FAILED", TRANSITIONS.get((TaskState.ADMITTED,"inline_depth_exceeded")) is TaskState.FAILED)
chk("K-71a 新边 RUNNING->FAILED", TRANSITIONS.get((TaskState.RUNNING,"inline_depth_exceeded")) is TaskState.FAILED)
d = _impl("_run_one_inline")
chk("K-71a 返回 GUARD_VIOLATION", "GUARD_VIOLATION" in d)
chk("K-71a 不再返回 INVALID_INPUT", 'Status.INVALID_INPUT, {"reason": "inline_depth_exceeded"}' not in d)
chk("K-71a 有 _finalize 传播", "self.deps.finalize(acb)" in d)

# --- K-67(B) 物化 edges
p = inspect.getsource(Planner.patch)
chk("K-67B patch 物化 edges", '_canon(' in p and 'new_plan["edges"] = materialized' in p)
# --- K-67(A) 剪枝
chk("K-67A _prune_id_aliases 存在", hasattr(Harness, "_prune_id_aliases"))
rp = _impl("replan")
i_readmit = rp.find("_readmit_after_replan(acb, new_plan)")
i_prune = rp.find("_prune_id_aliases(acb.run_id, new_plan)")
chk("K-67A 剪枝在 readmit 之后", 0 < i_readmit < i_prune, f"readmit@{i_readmit} prune@{i_prune}")
pa = _impl("_prune_id_aliases")
chk("K-67A 判据含活跃 ACB task_id", "acb.task_id" in pa)
chk("K-67A 判据含 logical_subgoal_id", "acb.logical_subgoal_id" in pa)

# --- K-70 预留
chk("K-70 coverage 注释预留 min_done_ratio", "min_done_ratio" in (ResultEnvelope.__doc__ or ""))
chk("K-70 写明 UPSTREAM_INSUFFICIENT 终态", "UPSTREAM_INSUFFICIENT" in (ResultEnvelope.__doc__ or ""))
chk("K-70 未实现严格化（宽松默认仍在）", TaskState.DONE_PARTIAL in PREDECESSOR_SATISFIED)

# --- 文档对齐
ms = (_ROOT / 'docs/milestones.md').read_text(encoding='utf-8')
chk("MS8 分级已更新（v1.2 复核）", "覆盖强度（v1.2 已按代码复核修正）" in ms)
chk("MS9 前置已更新（注入已存在）", "已支持全组件 keyword 注入" in ms)
chk("MS10 已按实况更新（S15+S17 已达成出口）",
    "已达成，S15+S17" in ms or "产生路径已有 4 条" in ms)

# --- 测试文件与用例名
for f in ("tests/test_s14_boundaries.py", "tests/test_s14_i12.py"):
    chk(f"测试文件存在 {f}", os.path.exists(str(_ROOT / f)))
tb = (_ROOT / 'tests/test_s14_boundaries.py').read_text(encoding='utf-8')
for n in ["test_s14_1_unrecovered_waiting_is_settled_by_tick",
          "test_s14_1_negative_control_memory_only_tick_misses_persisted",
          "test_s14_2_tick_is_a_fixed_point","test_s14_3_pump_reports_remaining_until_zero",
          "test_s14_4_without_checkpoint_saves_tick_is_memory_only",
          "test_s14_5_tick_finalize_does_not_violate_lock_order",
          "test_s14_6_update_children_cas_does_not_lose_update",
          "test_s14_6_update_children_negative_control_stale_snapshot_loses",
          "test_s14_7_inline_depth_exceeded_is_terminal_and_propagates",
          "test_s14_8_patch_materializes_edges_to_canonical_ids",
          "test_s14_9_prune_keeps_alias_referenced_by_active_acb"]:
    chk(f"用例存在 {n}", f"def {n}(" in tb)
ti = (_ROOT / 'tests/test_s14_i12.py').read_text(encoding='utf-8')
for n in ["test_s14_10_starved_task_leaves_admitted_within_bounded_pumps",
          "test_s14_10_negative_control_without_aging_stays_admitted"]:
    chk(f"用例存在 {n}", f"def {n}(" in ti)

print(f"通过 {len(ok)} / 失败 {len(bad)}")
for n,d in bad: print("  ❌", n, d)
sys.exit(1 if bad else 0)
