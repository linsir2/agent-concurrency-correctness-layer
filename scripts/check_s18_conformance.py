"""S18 方案 ↔ 实现 一致性核对（可执行）。

用法：`python3 scripts/check_s18_conformance.py`（exit 0 = 全部一致）。
对应 `docs/s18-budget-rsm-and-linearizable-read.md` §1 / §6。
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


from harness.budget import BudgetManager
from harness.storage import raft as raft_mod

# ---------------------------------------------------- K-87
chk("K-87 BudgetManager 可被 pickle（去掉 RLock）",
    hasattr(BudgetManager, "__getstate__") and hasattr(BudgetManager, "__setstate__"))
_gs = inspect.getsource(BudgetManager.__getstate__)
chk("K-87 __getstate__ 移除 _lock", 'state.pop("_lock"' in _gs)
chk("K-87 __getstate__ 移除 event_log（副本不 emit）",
    'state.pop("event_log"' in _gs)
chk("K-87 archive_node 支持注入 now",
    "now" in inspect.signature(BudgetManager.archive_node).parameters)

_rb = inspect.getsource(raft_mod._ReplicatedBudget)
chk("K-87 复制状态持有真 BudgetManager", "self.bm = BudgetManager(" in _rb)
chk("K-87 树/归档/gc 全走共识",
    all(x in _rb for x in ("def create_node", "def archive_node", "def gc",
                           "def remove_node")))
chk("K-87 归档回传真实结果（不恒 True）", "last_ok" in _rb)
_rbm = inspect.getsource(raft_mod.RaftBudgetManager)
chk("K-87 读委托真账本（_bm 属性）", "def _bm" in _rbm)
chk("K-87 archive 返回 last_ok", 'getattr(self._replica, "last_ok"' in _rbm)
chk("K-87 create_node 冲突返回 None", 'if not bool(getattr(self._replica, "last_ok"' in _rbm)
chk("K-87 claim fencing 未复制 → fail-closed（K-89）",
    "K-89" in _rbm and "raise RaftUnavailable" in _rbm)
chk("K-87 装配面 _reset_baseline", "def _reset_baseline" in _rbm)
chk("K-87 装配面 _lock", "def _lock" in _rbm)

# ---------------------------------------------------- K-82
chk("K-82 barrier() 存在", "def barrier" in inspect.getsource(raft_mod.RaftEventLog))
chk("K-82 linearizable() 存在",
    "def linearizable" in inspect.getsource(raft_mod.RaftEventLog))
_log_src = inspect.getsource(raft_mod.RaftEventLog)
chk("K-82 barrier 后刷新本地视图（审查 P0-1）",
    "_sync_applied()" in inspect.getsource(raft_mod.RaftEventLog.barrier))
chk("K-82 has() 在 require_leader 下等 applied",
    "_wait_applied_commit()" in inspect.getsource(raft_mod.RaftEventLog.has))
chk("K-82 说明「读穿日志」的代价",
    "读穿日志" in _log_src or "no-op" in _log_src)

# ---------------------------------------------------- K-83
chk("K-83 from_addrs 存在", hasattr(raft_mod.RaftCluster, "from_addrs"))
_fa = inspect.getsource(raft_mod.RaftCluster.from_addrs)
chk("K-83 校验 ≥3 地址", "至少 3 个地址" in _fa)
chk("K-83 校验重复", "地址重复" in _fa)
chk("K-83 校验 host:port 格式", "host:port" in _fa)

# ---------------------------------------------------- 依赖与测试
_p = (_ROOT / "pyproject.toml").read_text(encoding="utf-8")
chk("dependencies = [] 未被破坏", "dependencies = []" in _p)
_t = (_ROOT / "tests/test_s18_raft_rsm.py")
chk("S18 测试文件存在", _t.exists())
_txt = _t.read_text(encoding="utf-8") if _t.exists() else ""
for n in ("test_s18_1_parent_child_tree_reservation",
          "test_s18_2_archive_folds_consumed_and_replicates",
          "test_s18_3_invariants_checkable_on_any_replica",
          "test_s18_4_chain_limit_rejects_overspend",
          "test_s18_5_tree_survives_restart",
          "test_s18_6_barrier_gives_linearizable_read",
          "test_s18_7_require_leader_vs_barrier",
          "test_s18_8_from_addrs_validates_and_builds",
          "test_review_archive_returns_real_result",
          "test_review_create_node_conflicting_limits_returns_none",
          "test_review_consume_with_claim_fails_closed",
          "test_review_assembly_surface_exists"):
    chk(f"用例存在 {n}", f"def {n}(" in _txt)
chk("方案文档存在",
    (_ROOT / "docs/s18-budget-rsm-and-linearizable-read.md").exists())

print(f"通过 {len(ok)} / 失败 {len(bad)}")
for n, d in bad:
    print("  ❌", n, d)
sys.exit(1 if bad else 0)
