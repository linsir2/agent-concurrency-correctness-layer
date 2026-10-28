"""S20 方案 ↔ 实现 一致性核对（可执行）。

用法：`python3 scripts/check_s20_conformance.py`（exit 0 = 全部一致）。
对应 `docs/s20-tenant-metering-and-k94.md` §1 / §6。
"""

from __future__ import annotations

import inspect
import pathlib
import sys

_ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

ok: list[str] = []
bad: list[tuple[str, str]] = []


def chk(name: str, cond: bool, detail: str = "") -> None:
    (ok if cond else bad).append((name, detail))


from harness import Harness
from harness.budget import BudgetManager
from harness.storage import raft as raft_mod

# ---------------------------------------------------- K-94
_rb = inspect.getsource(raft_mod._ReplicatedBudget)
chk("K-94 有界墓碑 claim_tombstones", "claim_tombstones" in _rb)
chk("K-94 墓碑上限 claim_tombstone_max", "claim_tombstone_max" in _rb)
chk("K-94 consume 判据同时看 claims+tombstones",
    "claim_tombstones.get(acb_id, 0)" in _rb)
chk("K-94 prune 接收 live_acb_ids", "live_acb_ids" in _rb)
chk("K-94 prune 先写墓碑再删 claims",
    "self.claim_tombstones[aid] = int(self.claims.pop(aid))"
    in _rb.replace("\n", " ").replace("  ", " "))
_rbm = inspect.getsource(raft_mod.RaftBudgetManager.prune)
chk("K-94 RaftBudgetManager.prune 透传 live_acb_ids",
    "live_acb_ids" in _rbm)

# ---------------------------------------------------- K-95
_sig = inspect.signature(BudgetManager.prune).parameters
chk("K-95 prune 默认 live_rids=None（fail-safe）",
    _sig["live_rids"].default is None)
_pr = inspect.getsource(BudgetManager.prune)
chk("K-95 缺 live_rids 直接返回 0",
    "live_rids is None" in _pr and "return 0" in _pr)
chk("K-95 拒绝计数 _prune_refused", "_prune_refused" in _pr)

# ---------------------------------------------------- K-96
chk("K-96 verify_connected 存在",
    callable(getattr(raft_mod.RaftCluster, "verify_connected", None)))
_vc = inspect.getsource(raft_mod.RaftCluster.verify_connected)
chk("K-96 用 otherNodes 而非地址字符串", "otherNodes" in _vc)
chk("K-96 列出断连 peer", "未连通" in _vc)

# ---------------------------------------------------- 多租户
# BudgetNode 定义在 budget.py（不在 storage/raft.py）
from harness.budget import BudgetNode

chk("多租户 BudgetNode.tenant 字段",
    "self.tenant = tenant" in inspect.getsource(BudgetNode))
chk("多租户 ensure_tenant 存在",
    callable(getattr(Harness, "ensure_tenant", None)))
chk("多租户 create_node 接受 tenant",
    "tenant" in inspect.signature(BudgetManager.create_node).parameters)

# ---------------------------------------------------- 计量
chk("计量 usage_report 存在",
    callable(getattr(Harness, "usage_report", None)))
_ur = inspect.getsource(Harness.usage_report)
chk("计量从事件派生（非内存计数器）",
    "event_log.all()" in _ur and "tenant" in _ur)

# ---------------------------------------------------- 统一 Admin
_sc = inspect.getsource(
    sys.modules["harness.orchestration"].RunCoordinator.submit_command)
for kind in ("KILL_RUN", "PURGE_RUN", "DLQ_REDRIVE", "PRUNE"):
    chk(f"Admin 路由 {kind}", f'"{kind}"' in _sc)

# ---------------------------------------------------- 依赖与测试
_p = (_ROOT / "pyproject.toml").read_text(encoding="utf-8")
chk("dependencies = [] 未被破坏", "dependencies = []" in _p)
_t = _ROOT / "tests/test_s20_tenant_metering.py"
chk("S20 测试文件存在", _t.exists())
_txt = _t.read_text(encoding="utf-8") if _t.exists() else ""
for n in ("test_s20_1_prune_failsafe_without_live_rids",
          "test_s20_2_claims_pruned_for_dead_acbs",
          "test_s20_3_dead_worker_still_fenced_after_prune",
          "test_s20_3_tombstones_are_bounded",
          "test_s20_4_verify_connected_after_ready",
          "test_s20_5_tenant_quota_isolation",
          "test_s20_6_tenant_quota_negative_control",
          "test_s20_7_usage_report_is_idempotent",
          "test_s20_8_usage_report_by_tenant",
          "test_s20_9_submit_command_routes_all_admin_ops"):
    chk(f"用例存在 {n}", f"def {n}(" in _txt)
chk("方案文档存在", (_ROOT / "docs/s20-tenant-metering-and-k94.md").exists())

print(f"通过 {len(ok)} / 失败 {len(bad)}")
for n, d in bad:
    print("  ❌", n, d)
sys.exit(1 if bad else 0)
