"""S19 方案 ↔ 实现 一致性核对（可执行）。

用法：`python3 scripts/check_s19_conformance.py`（exit 0 = 全部一致）。
对应 `docs/s19-budget-gc-and-hardening.md` §1 / §6。
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


from harness import Harness, protocols                    # noqa: E402
from harness.budget import BudgetManager, Reservation      # noqa: E402
from harness.storage import raft as raft_mod               # noqa: E402

# ---------------------------------------------------- K-88
_rs = inspect.getsource(Reservation)
chk("K-88 Reservation 有 settled_at（终态时刻）", "settled_at" in _rs)
chk("K-88 prune 存在", hasattr(BudgetManager, "prune"))
_pr = inspect.getsource(BudgetManager.prune)
chk("K-88 三栅栏：状态", 'not in ("RELEASED", "SETTLED")' in _pr)
chk("K-88 三栅栏：引用 live_rids", "rid in live" in _pr)
chk("K-88 三栅栏：时间 settled_at", "settled_at" in _pr)
chk("K-88 剪前折账 folded_consumed", "folded_consumed += int(resv.consumed)" in _pr)
chk("K-88 生效保留期可由参数传入（RSM 确定性）", "retention_s" in _pr)
_release = inspect.getsource(BudgetManager.release)
chk("K-88 release 设 settled_at + expires_at",
    "resv.settled_at = stamp" in _release and "resv.expires_at = stamp" in _release)
_settle = inspect.getsource(BudgetManager.settle)
chk("K-88 settle 设 settled_at", "resv.settled_at = stamp" in _settle)
chk("K-88 Harness.prune_budget 存在",
    callable(getattr(Harness, "prune_budget", None)))
_hp = inspect.getsource(Harness.prune_budget)
chk("K-88 从活 ACB 精确算 live_rids",
    "acb.reservation_ids" in _hp and "TASK_TERMINAL" in _hp)

# ---------------------------------------------------- K-89
_rb = inspect.getsource(raft_mod._ReplicatedBudget)
chk("K-89 副本有 claims 表", "self.claims = {}" in _rb)
chk("K-89 consume 带 acb_id/claim_epoch",
    "claim_epoch" in inspect.getsource(raft_mod._ReplicatedBudget.consume))
chk("K-89 单调拒绝旧 epoch", "int(self.claims.get(acb_id, 0))" in _rb)
_rbm = inspect.getsource(raft_mod.RaftBudgetManager.consume)
chk("K-89 不再 fail-closed 拒绝（改为传 claim）",
    "raise RaftUnavailable" not in _rbm and "acb_id" in _rbm)

# ---------------------------------------------------- K-90
chk("K-90 归档事件带 expires_at",
    '"expires_at": stamp + int(self._tombstone_ttl_s)' in inspect.getsource(
        BudgetManager.archive_node))
chk("K-90 重放优先用 payload expires_at",
    'p.get("expires_at")' in inspect.getsource(BudgetManager._apply_archived))

# ---------------------------------------------------- K-91
chk("K-91 remaining() 存在", hasattr(BudgetManager, "remaining"))
chk("K-91 remaining = limits - subtree_reserved",
    "node.limits - node.subtree_reserved" in inspect.getsource(
        BudgetManager.remaining))
chk("K-91 契约写明 ANY 语义", "**ANY 语义**" in inspect.getsource(
    protocols.BudgetManager.check))

# ---------------------------------------------------- K-92
_fa = inspect.getsource(raft_mod.RaftCluster.from_addrs)
chk("K-92 预检存在", "precheck" in _fa)
chk("K-92 预检默认 auto（不误伤首次启动）",
    'precheck: bool | str = "auto"' in _fa)
chk("K-92 密码可配", "password" in _fa)
_conf = inspect.getsource(raft_mod.raft_conf)
chk("K-92 raft_conf 密码可配（去硬编码）",
    "password: str = " in _conf)

# ---------------------------------------------------- K-93
chk("K-93 remove_node 复制 owner",
    "owner" in inspect.getsource(raft_mod._ReplicatedBudget.remove_node))
chk("K-93 RaftBudgetManager.remove_node 透传 owner",
    "self._replica.remove_node, node_id, owner" in _rbm.replace("\n", " ")
    or "node_id, owner" in inspect.getsource(raft_mod.RaftBudgetManager.remove_node))

# ---------------------------------------------------- 依赖与测试
_p = (_ROOT / "pyproject.toml").read_text(encoding="utf-8")
chk("dependencies = [] 未被破坏", "dependencies = []" in _p)
_t = _ROOT / "tests/test_s19_budget_gc.py"
chk("S19 测试文件存在", _t.exists())
_txt = _t.read_text(encoding="utf-8") if _t.exists() else ""
for n in ("test_s19_1_prune_removes_settled_after_ttl",
          "test_s19_2_invariants_hold_before_and_after_prune",
          "test_s19_3_fence_state_active_is_never_pruned",
          "test_s19_3_fence_live_reference_blocks_prune",
          "test_s19_3_fence_time_blocks_prune",
          "test_s19_4_reservation_table_is_bounded",
          "test_s19_5_claim_fencing_replicated",
          "test_s19_6_archive_event_carries_expires_at",
          "test_s19_7_remaining_vs_check_semantics",
          "test_s19_8_from_addrs_precheck_fails_fast",
          "test_s19_9_remove_node_owner_replicated"):
    chk(f"用例存在 {n}", f"def {n}(" in _txt)
chk("方案文档存在", (_ROOT / "docs/s19-budget-gc-and-hardening.md").exists())

print(f"通过 {len(ok)} / 失败 {len(bad)}")
for n, d in bad:
    print("  ❌", n, d)
sys.exit(1 if bad else 0)
