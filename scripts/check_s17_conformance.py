"""S17 方案 ↔ 实现 一致性核对（可执行）。

用法：`python3 scripts/check_s17_conformance.py`（exit 0 = 全部一致）。
对应 `docs/s17-boundaries-and-ms10.md` §1（K-81~K-86）与 §1.7（MS10）。
"""

from __future__ import annotations

import inspect
import pathlib
import sys

_ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "tests"))

ok: list[str] = []
bad: list[tuple[str, str]] = []


def chk(name: str, cond: bool, detail: str = "") -> None:
    (ok if cond else bad).append((name, detail))


import harness.observability as obs                       # noqa: E402
import harness.orchestration as orchestration              # noqa: E402
from harness import Harness, protocols                    # noqa: E402
from harness.budget import BudgetManager                   # noqa: E402
from harness.storage import raft as raft_mod               # noqa: E402
from harness.types import ACB, EventKind                   # noqa: E402

# ===================================================== K-81
sig = inspect.signature(BudgetManager.reserve).parameters
chk("K-81 BudgetManager.reserve 支持注入 reservation_id",
    "reservation_id" in sig)
chk("K-81 缺省仍为 None（向后兼容）",
    sig["reservation_id"].default is None)
chk("K-81 protocols.BudgetManager.reserve 同步签名",
    "reservation_id" in inspect.signature(
        protocols.BudgetManager.reserve).parameters)
chk("K-81 RaftBudgetManager 存在", hasattr(raft_mod, "RaftBudgetManager"))
chk("K-81 复制状态机存在", hasattr(raft_mod, "_ReplicatedBudget"))
_src_rb = inspect.getsource(raft_mod._ReplicatedBudget)
chk("K-81 id 来自复制计数器（非 uuid4 调用）",
    'f"R{self._seq}"' in _src_rb and "uuid.uuid4(" not in _src_rb)

# ===================================================== K-82
_log_src = inspect.getsource(raft_mod.RaftEventLog)
chk("K-82 require_leader 开关存在", "require_leader" in _log_src)
chk("K-82 NotLeader 异常存在", hasattr(raft_mod, "NotLeader"))
chk("K-82 读前校验 _check_read", "_check_read" in _log_src)
chk("K-82 不声称 follower 线性一致（有明确注释）",
    "不声称" in _log_src or "最终一致" in _log_src)

# ===================================================== K-83
chk("K-83 production_conf 存在", hasattr(raft_mod, "production_conf"))
_prod = inspect.getsource(raft_mod.production_conf)
chk("K-83 校验 node_id 在 addrs 内", "必须在 addrs 中" in _prod)
chk("K-83 校验 ≥3 节点", "至少 3 节点" in _prod)

# ===================================================== K-84
_t16 = (_ROOT / "tests/test_s16_raft.py").read_text(encoding="utf-8")
chk("K-84 模块级共享集群 fixture", 'scope="module"' in _t16
    and "shared_cluster" in _t16)

# ===================================================== K-85
_epoch = inspect.getsource(raft_mod.RaftEpochSource)
chk("K-85 bump_epoch 显式抛错", "EpochNotBumpable" in _epoch)
chk("K-85 supports_bump = False", "supports_bump = False" in _epoch)

# ===================================================== K-86
_conf_src = inspect.getsource(raft_mod.raft_conf)
chk("K-86 raft_conf 支持 journal_file", "journal_file" in _conf_src)
chk("K-86 raft_conf 支持 full_dump_file", "full_dump_file" in _conf_src)
chk("K-86 压缩阈值 >=2（pysyncobj 断言）",
    "max(2, compaction_min_entries)" in _conf_src)
_cluster_src = inspect.getsource(raft_mod.RaftCluster)
chk("K-86 RaftCluster 支持 data_dir", "data_dir" in _cluster_src)
chk("K-86 compact() 存在", "def compact" in _cluster_src)

# ===================================================== MS10
for name in ("drain", "kill_run", "purge_run", "metrics",
             "export_observability"):
    chk(f"MS10 Harness.{name} 存在", callable(getattr(Harness, name, None)))
chk("MS10 毒任务阈值参数", "poison_after_failures"
    in inspect.signature(Harness.__init__).parameters)
chk("MS10 毒任务识别方法", hasattr(Harness, "_maybe_poison_locked"))
chk("MS10 RUN_PURGED 事件", hasattr(EventKind, "RUN_PURGED"))
chk("MS10 ACB.consecutive_failures",
    "consecutive_failures" in inspect.getsource(ACB))
chk("MS10 RunCoordinator.unregister",
    hasattr(orchestration.RunCoordinator, "unregister"))

# 可观测（零依赖 + 可选 OTel）
chk("MS10 SpanSink 契约", hasattr(obs, "SpanSink"))
chk("MS10 MemorySpanSink", hasattr(obs, "MemorySpanSink"))
chk("MS10 event_spans 映射", callable(getattr(obs, "event_spans", None)))
chk("MS10 OTelAdapter 存在", hasattr(obs, "OTelAdapter"))
chk("MS10 无 SDK 时 fail-closed（OTelUnavailable）",
    "OTelUnavailable" in inspect.getsource(obs.make_otel_sink))
chk("MS10 observability 不 import otel（零依赖）",
    "import opentelemetry" not in inspect.getsource(obs)[:400])

# 依赖约束
_pyproject = (_ROOT / "pyproject.toml").read_text(encoding="utf-8")
chk("dependencies = [] 未被破坏", "dependencies = []" in _pyproject)
chk("otel 在 optional-dependencies", "otel = [" in _pyproject)

# 测试
for f, names in (
    ("tests/test_s17_raft_boundaries.py",
     ["test_s17_1_deterministic_rid_across_replicas",
      "test_s17_1_rid_continues_after_restart",
      "test_s17_2_raft_budget_does_not_overspend",
      "test_s17_3_raft_budget_satisfies_protocol",
      "test_s17_4_require_leader_rejects_follower_read",
      "test_s17_5_production_conf_validates",
      "test_s17_6_snapshot_and_restart_recovers",
      "test_s17_7_bump_epoch_is_explicitly_unavailable"]),
    ("tests/test_s17_ms10.py",
     ["test_dlq", "test_admin_pause_resume", "test_ingress_idempotent",
      "test_drain_completes_when_all_terminal",
      "test_kill_run_cancels_and_settles",
      "test_purge_run_requires_terminal_and_keeps_events"]),
):
    p = _ROOT / f
    chk(f"测试文件存在 {f}", p.exists())
    txt = p.read_text(encoding="utf-8") if p.exists() else ""
    for n in names:
        chk(f"用例存在 {n}", f"def {n}(" in txt)

# 文档
chk("方案文档存在", (_ROOT / "docs/s17-boundaries-and-ms10.md").exists())

print(f"通过 {len(ok)} / 失败 {len(bad)}")
for n, d in bad:
    print("  ❌", n, d)
sys.exit(1 if bad else 0)
