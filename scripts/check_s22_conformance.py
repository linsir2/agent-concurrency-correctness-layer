"""S22 方案 ↔ 实现 一致性核对（C1–C6，可执行）。

用法：`python3 scripts/check_s22_conformance.py`（exit 0 = 全部一致）。
对应 `docs/s22-implementation-plan.md` §3。
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


from harness import Harness, TaskState
from harness.intent_registry import IntentRegistry as _IR
from harness.orchestration import Dispatcher
from harness.types import TRANSITIONS
from harness.worktree import WorktreeManager

# ---------------------------------------------------- C1 abort
chk("C1 Dispatcher.abort 存在", callable(getattr(Dispatcher, "abort", None)))
_rb = inspect.getsource(Dispatcher._rollback)
chk("C1 _rollback 收敛到 abort", "self.abort(acb)" in _rb)
chk("C1 IntentRegistry.undeclare_all 存在",
    callable(getattr(_IR, "undeclare_all", None)))
chk("C1 Harness.abort 存在", callable(getattr(Harness, "abort", None)))
_proto = (_ROOT / "harness/protocols.py").read_text(encoding="utf-8")
chk("C1 协议声明 undeclare_all", "def undeclare_all" in _proto)

# ---------------------------------------------------- C2 finish
chk("C2 状态机边 ADMITTED→DONE",
    TRANSITIONS.get((TaskState.ADMITTED, "finish_ok")) is TaskState.DONE)
chk("C2 状态机边 ADMITTED→FAILED",
    TRANSITIONS.get((TaskState.ADMITTED, "fail")) is TaskState.FAILED)
_fin = inspect.getsource(Harness.finish)
chk("C2 finish 复用 _finalize", "self._finalize(acb)" in _fin)
chk("C2 finish 幂等（已终态直接返回）", "already" in _fin)

# ---------------------------------------------------- C3 外部幂等键
_sig = inspect.signature(Harness.commit_batch).parameters
chk("C3 commit_batch 接受 idempotency_key", "idempotency_key" in _sig)
_cr = inspect.getsource(Harness._commit_record)
chk("C3 事件 payload 写 external_idem_key",
    '"external_idem_key"' in _cr)
chk("C3 _dedup_by_external_key 存在",
    callable(getattr(Harness, "_dedup_by_external_key", None)))
_dd = inspect.getsource(Harness._dedup_by_external_key)
chk("C3 同键异内容 → CONFLICT 哨兵", "_EXT_KEY_CONFLICT" in _dd)
chk("C3 收据 id 从事件读（不重算）",
    "RECEIPT_ISSUED" in _dd and "receipt_id" in _dd)
chk("C3 同键并发有进程内锁", callable(getattr(Harness, "_ext_key_lock", None)))
_cb = inspect.getsource(Harness.commit_batch)
chk("C3 外部键命中后刷新 base_version",
    "self._refresh_after_commit(acb, records)" in _cb)

# ---------------------------------------------------- C4 token reaper
chk("C4 _reap_expired_tokens 存在",
    callable(getattr(Harness, "_reap_expired_tokens", None)))
_rp = inspect.getsource(Harness._reap_expired_tokens)
chk("C4 只处理 rsc_token", '"rsc_token"' in _rp)
chk("C4 回收走 abort + purge_run",
    "dispatcher.abort" in _rp and "purge_run" in _rp)
chk("C4 跳过可重试态（INADMISSIBLE/QUARANTINED）",
    "INADMISSIBLE" in _rp and "QUARANTINED" in _rp)
chk("C4 跳过在途 claim", "claim_of(acb.id)" in _rp)
_tick = inspect.getsource(Harness.tick)
chk("C4 tick 触发 token 回收", "_reap_expired_tokens(now)" in _tick)
chk("C4 有清理钩子（销毁 Z3）", "_token_cleanup" in _rp)

# ---------------------------------------------------- C5 token 恢复
_rs = inspect.signature(Harness._restore_acb).parameters
chk("C5 _restore_acb 接受 bump_epoch", "bump_epoch" in _rs)
chk("C5 bump_epoch 默认 True（保持旧语义）", _rs["bump_epoch"].default is True)
_rb_src = inspect.getsource(Harness._restore_acb)
chk("C5 bump_epoch 可控", "if bump_epoch:" in _rb_src)

# ---------------------------------------------------- C6 worktree
for name in ("create", "destroy", "changed", "read", "destroy_all"):
    chk(f"C6 WorktreeManager.{name} 存在",
        callable(getattr(WorktreeManager, name, None)))
_wt = (_ROOT / "harness/worktree.py").read_text(encoding="utf-8")
chk("C6 git worktree 后端", "worktree\", \"add\"" in _wt or '"worktree", "add"' in _wt)
chk("C6 copy 兜底", "copytree" in _wt)
chk("C6 路径逃逸校验", "路径逃逸" in _wt)
chk("C6 不 chdir / 不 monkeypatch open",
    "chdir" not in _wt and "builtins" not in _wt)
chk("C6 copy 改动检测含 ctime/内容哈希（防同尺寸同 mtime 漏检）",
    "st_ctime_ns" in _wt and "_HASH_LIMIT" in _wt)
chk("C6 create/destroy 有并发锁", "_lock" in _wt and "threading.RLock" in _wt)
chk("C6 OSError 包装为 WorktreeError", "raise WorktreeError" in _wt)

# ---------------------------------------------------- 依赖与测试
_p = (_ROOT / "pyproject.toml").read_text(encoding="utf-8")
chk("dependencies = [] 未被破坏", "dependencies = []" in _p)
for f, names in (
    ("tests/test_s22_core.py",
     ("test_c1_abort_releases_everything",
      "test_c1_abort_after_partial_apply_keeps_consumed",
      "test_c2_finish_settles_and_releases",
      "test_c2_finish_without_apply_refunds",
      "test_c2_double_finish_is_noop",
      "test_c3_same_key_same_content_returns_same_receipt",
      "test_c3_same_key_different_content_conflicts",
      "test_c3_replay_after_other_token",
      "test_c3_replay_refreshes_base_version",
      "test_c4_expired_token_reclaimed",
      "test_c4_finished_token_not_reaped",
      "test_c4_inadmissible_token_not_reaped",
      "test_c4_token_with_active_claim_not_reaped",
      "test_c4_non_token_acb_untouched",
      "test_c5_restore_acb_without_global_epoch_bump",
      "test_c5_lazy_restore_does_not_reset_budget_tree",
      "test_c5_bump_epoch_true_keeps_legacy_semantics")),
    ("tests/test_s22_worktree.py",
     ("test_c6_isolation", "test_c6_changed_and_read",
      "test_c6_path_escape_rejected", "test_c6_cleanup",
      "test_c6_copy_fallback", "test_c6_create_is_idempotent",
      "test_c6_copy_detects_same_size_same_mtime",
      "test_c6_symlink_escape_rejected",
      "test_c6_concurrent_create_same_agent",
      "test_c6_missing_base_raises_worktree_error")),
):
    path = _ROOT / f
    chk(f"测试文件存在 {f}", path.exists())
    txt = path.read_text(encoding="utf-8") if path.exists() else ""
    for n in names:
        chk(f"用例存在 {n}", f"def {n}(" in txt)
chk("方案文档存在", (_ROOT / "docs/s22-implementation-plan.md").exists())

print(f"通过 {len(ok)} / 失败 {len(bad)}")
for n, d in bad:
    print("  ❌", n, d)
sys.exit(1 if bad else 0)
