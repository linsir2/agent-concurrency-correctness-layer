"""S21 方案 ↔ 实现 一致性核对（可执行）。

用法：`python3 scripts/check_s21_conformance.py`（exit 0 = 全部一致）。
对应 `docs/s21-tenant-admission-and-leases.md` §1 / §6。
"""

from __future__ import annotations

import inspect
import pathlib
import sys

_ROOT = pathlib.Path(__file__).resolve().parent.parent


def _impl() -> str:
    """内核实现全文：harness/harness.py + harness/kernel/*.py（拆分后不再固定单文件）。"""
    parts = [(_ROOT / "harness/harness.py").read_text(encoding="utf-8")]
    parts += [p.read_text(encoding="utf-8")
              for p in sorted((_ROOT / "harness/kernel").glob("*.py"))]
    return "\n".join(parts)
sys.path.insert(0, str(_ROOT))

ok: list[str] = []
bad: list[tuple[str, str]] = []


def chk(name: str, cond: bool, detail: str = "") -> None:
    (ok if cond else bad).append((name, detail))


from harness import Harness
from harness.intent_registry import IntentRegistry
from harness.orchestration import _RESERVED_TIERS, Dispatcher, Planner
from harness.storage import raft as raft_mod

# ---------------------------------------------------- K-97 租户自动接准入
_dp = inspect.getsource(Dispatcher)
chk("K-97 _resolve_parent_budget 存在", "_resolve_parent_budget" in _dp)
chk("K-97 _tenant_of 统一解析", "def _tenant_of" in _dp)
chk("K-97 _tier_parent 存在", "def _tier_parent" in _dp)
_rp = inspect.getsource(Dispatcher._resolve_parent_budget)
chk("K-97 显式 parent_budget 优先", "parent_budget" in _rp)
chk("K-97 无 tenant 回落 run", 'return "run"' in _rp)
chk("K-97 不静默建 tenant 节点",
    "_nodes.get(tid)" in _rp)
_ac = inspect.getsource(Dispatcher._try_admit_commit)
chk("K-97 准入用 _tier_parent 作父节点",
    "parent_id = self._tier_parent(acb, task)" in _ac)
chk("K-97 节点 tenant 与父节点同源",
    "tenant=_tenant_of_node_id(parent_id)" in _ac)
_pl = inspect.getsource(Planner.plan)
chk("K-97 Planner 透传 tenant 到 footprint",
    'footprint["tenant"]' in _pl)
chk("K-97 Planner 透传 tier 到 footprint",
    'footprint["tier"]' in _pl)
_dar = inspect.getsource(Harness._admit_and_register)
chk("K-97 生产入口先登记 plan 再准入",
    _dar.index("self.plans[run_id] = plan")
    < _dar.index("self.dispatcher.try_admit"))
_rcn = inspect.getsource(raft_mod._ReplicatedBudget.create_node)
chk("K-97 Raft 复制 create_node 携带 tenant", "tenant" in _rcn)
_rcn2 = inspect.getsource(raft_mod.RaftBudgetManager.create_node)
chk("K-97 Raft 后端 create_node 接收 tenant", "tenant" in _rcn2)
_bpy = (_ROOT / "harness/budget.py").read_text(encoding="utf-8")
chk("K-97 节点创建事件带 tenant", '"tenant": node.tenant' in _bpy)
_hpy = _impl()
chk("K-97 重放恢复 tenant", 'p.get("tenant"' in _hpy)
chk("K-97 spawn_child 子节点带 tenant",
    "_tenant_of_node_id(parent_node.node_id)" in _hpy)

# ---------------------------------------------------- K-99 过期租约回收
chk("K-99 IntentRegistry.reap_expired 存在",
    callable(getattr(IntentRegistry, "reap_expired", None)))
_re = inspect.getsource(IntentRegistry.reap_expired)
chk("K-99 只删 valid=False", "lease.valid" in _re)
chk("K-99 同时回收 epoch 失效租约",
    "lease.epoch == self._epoch_for" in _re)
chk("K-99 失效即回收（不等过期，避免无界增长）",
    "lease.expires_at" not in _re)
chk("K-99 回收后清 _active", "_active" in _re)
_tk = inspect.getsource(Harness.tick)
chk("K-99 tick 触发回收", "reap_expired" in _tk)
chk("K-99 tick 传入同一 now", "reap_expired(now)" in _tk)
_proto = (_ROOT / "harness/protocols.py").read_text(encoding="utf-8")
chk("K-99 协议声明 reap_expired", "def reap_expired" in _proto)

# ---------------------------------------------------- K-100 墓碑窗口量化
chk("K-100 max_lease_ttl_s 存在", hasattr(Harness, "max_lease_ttl_s")
    or "max_lease_ttl_s" in inspect.getsource(Harness.__init__))
chk("K-100 claim_window_s 存在", callable(getattr(Harness, "claim_window_s", None)))
_cw = inspect.getsource(Harness.claim_window_s)
chk("K-100 窗口用租约时长而非常量", "max_lease_ttl_s" in _cw)
chk("K-100 窗口 = 上限 × 租约时长", "claim_tombstone_max" in _cw)
_pb = inspect.getsource(Harness.prune_budget)
chk("K-100 prune 下发 max_lease_ttl_s", "max_lease_ttl_s" in _pb)
chk("K-100 回读 claim_window_exceeded", "claim_window_exceeded" in _pb)
_rb = inspect.getsource(raft_mod._ReplicatedBudget)
chk("K-100 墓碑记录入墓时刻", "claim_tombstone_at" in _rb)
chk("K-100 淘汰按年龄判定", "float(now) - born < ttl" in _rb)
chk("K-100 无足够老墓碑则拒绝淘汰", "claim_window_exceeded += 1" in _rb)
chk("K-100 旧墓碑缺时间戳时补记（可淘汰）",
    "claim_tombstone_at.setdefault" in _rb)
_rpr = inspect.getsource(raft_mod._ReplicatedBudget.prune)
chk("K-100 RSM prune 接收 max_lease_ttl_s", "max_lease_ttl_s" in _rpr)
chk("K-100 墓碑上限随命令复制（不读副本本地配置）",
    "tombstone_max" in _rpr
    and "cap = int(self.claim_tombstone_max)" not in _rpr)
_rbm = inspect.getsource(raft_mod.RaftBudgetManager.prune)
chk("K-100 Raft 后端透传 max_lease_ttl_s", "max_lease_ttl_s" in _rbm)
chk("K-100 Raft 后端下发 tombstone_max",
    "claim_tombstone_max" in _rbm)
chk("K-100 Raft 后端暴露 claim_tombstone_max 诊断",
    isinstance(getattr(raft_mod.RaftBudgetManager, "claim_tombstone_max", None),
               property))

# ---------------------------------------------------- 多租户优先级份额
_et = inspect.getsource(Harness.ensure_tenant)
chk("份额 ensure_tenant 支持 min_share", "min_share" in _et)
chk("份额 建 tenant:<t>:reserved", ":reserved" in _et)
chk("份额 超 quota 显式拒绝", "ValueError" in _et)
chk("份额 先校验后建节点（失败零残留）",
    _et.index("raise ValueError") < _et.index("create_node"))
chk("份额 配置冲突显式拒绝（不静默沿用旧配置）",
    "已存在但" in _et)
chk("份额 拒绝含冒号的租户名", '":" in tid' in _et)
chk("份额 拒绝小数份额", "必须是整数" in _et)
chk("份额 父节点缺失显式拒绝", "父预算节点不存在" in _et)
_tp = inspect.getsource(Dispatcher._tier_parent)
chk("份额 只认已登记档位", "_RESERVED_TIERS" in _tp)
chk("份额 只在租户节点下探测预留池", 'base.startswith("tenant:")' in _tp)
chk("份额 未知档位回落共享池", "return base" in _tp)
chk("份额 档位集合含 premium/standard/developer",
    {"premium", "standard", "developer"} <= set(_RESERVED_TIERS))
chk("份额 tier 规范化（大小写/空白）", ".strip().lower()" in
    inspect.getsource(Dispatcher._tier_of))
_nt = (_ROOT / "harness/orchestration.py").read_text(encoding="utf-8")
chk("K-97 node.tenant 由实际父节点反推",
    "_tenant_of_node_id(parent_id)" in _nt)

# ---------------------------------------------------- 依赖与测试
_p = (_ROOT / "pyproject.toml").read_text(encoding="utf-8")
chk("dependencies = [] 未被破坏", "dependencies = []" in _p)
_t = _ROOT / "tests/test_s21_tenant_and_leases.py"
chk("S21 测试文件存在", _t.exists())
_txt = _t.read_text(encoding="utf-8") if _t.exists() else ""
for n in ("test_s21_1_tenant_auto_attached_to_parent_budget",
          "test_s21_2_no_tenant_keeps_legacy_parent",
          "test_s21_2_explicit_parent_budget_wins",
          "test_s21_3_tenant_quota_blocks_without_affecting_others",
          "test_s21_4_reap_expired_leases",
          "test_s21_5_reap_never_deletes_valid_leases",
          "test_s21_5_tick_triggers_reap",
          "test_s21_6_claim_window_scales_with_lease",
          "test_s21_7_tombstone_eviction_respects_lease_window",
          "test_s21_8_tier_reserved_share",
          "test_s21_9_oversubscribed_tier_borrows_shared_pool",
          "test_s21_10_share_exceeding_quota_is_rejected",
          "test_s21_10_quota_conflict_is_rejected",
          "test_s21_10_share_zero_is_noop",
          "test_s21_11_tombstone_cap_is_replicated",
          "test_s21_12_node_tenant_matches_actual_parent",
          "test_s21_13_reap_epoch_stale_leases",
          "test_s21_14_plan_tenant_via_dispatch_and_run",
          "test_s21_15_tenant_label_survives_replay",
          "test_s21_16_ensure_tenant_rejects_bad_input",
          "test_s21_17_raft_create_node_carries_tenant"):
    chk(f"用例存在 {n}", f"def {n}(" in _txt)
chk("方案文档存在", (_ROOT / "docs/s21-tenant-admission-and-leases.md").exists())

print(f"通过 {len(ok)} / 失败 {len(bad)}")
for n, d in bad:
    print("  ❌", n, d)
sys.exit(1 if bad else 0)
