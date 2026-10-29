"""S2：`commit_batch` 抽取的**字节级**黄金守卫。

为什么需要它：
    普通测试只能证明「结果对」，不能证明「提交路径逐字节没变」。`commit_batch`
    是线性化点（单帧 fdatasync + 收据链），任何阶段重排、去重分支漂移、事件字段
    增删都会改变 WAL。重构要求**行为零变化**，那就用 WAL 原始字节比对。

RIPR：
- Reachability：本场景覆盖 commit_batch 的 5 条路径 ——
  多资源同帧提交 / 单资源新版本 / 同键重放（dedup 短路） / Delete 哨兵 / finish 收据锚点；
- Infection：`Deterministic(seed)` 固定 event_id / ts / run_id / 收据 id，
  同 seed 两次运行字节相同（本文件自检 stable）；
- Propagation：WAL 字节 → sha256 → 与冻结常量比对，任何字段漂移即失败；
- Revealability：失败时打印实际哈希与字节数，便于定位「哪个阶段变了」。

**边界**：只锁「提交产生的事件流」，不锁实现结构。提取阶段/换文件不影响它。
"""

from __future__ import annotations

import hashlib
import pathlib

import pytest

from harness import RscSession
from harness.entropy import Deterministic
from harness.event_log import WalEventLog
from harness.harness import Harness
from harness.rebase import DELETE

SEED = "s2-golden"
GOLDEN_SHA256 = "942fdeb23fdbfb37bc9dde008ed2b439e02cecc1f81417e3cd4cd08ed3832330"


def _run_commit_scenario(root: pathlib.Path, seed: str = SEED) -> bytes:
    ent = Deterministic(seed)
    wal = WalEventLog(str(root / "wal.log"), fsync=False, clock=ent, entropy=ent)
    h = Harness(event_log=wal, checkpoint_root=str(root), entropy=ent, run_limits=100)
    s = RscSession(h, tenant="t", min_budget=1, state_dir=str(root / "idx"))
    tok = s.begin("agent-a", "goal", writes=["a.md", "b.md"])
    # ① 多资源同帧提交
    s.apply(tok.token, [("a.md", "v1"), ("b.md", "v1")],
            idempotency_key="k1", agent_id="agent-a")
    # ② 单资源新版本（收据链续接）
    s.apply(tok.token, [("a.md", "v2")], idempotency_key="k2", agent_id="agent-a")
    # ③ 同键重放（dedup 短路，不得重复推进版本）
    s.apply(tok.token, [("a.md", "v2")], idempotency_key="k2", agent_id="agent-a")
    # ④ Delete 哨兵
    s.apply(tok.token, [("b.md", DELETE)], idempotency_key="k3", agent_id="agent-a")
    s.finish(tok.token)
    wal.close()
    return (root / "wal.log").read_bytes()


def test_commit_scenario_is_reproducible(tmp_path):
    """自检：同 seed 两次运行必须逐字节相同（否则黄金常量没有意义）。"""
    a = _run_commit_scenario(tmp_path / "a")
    b = _run_commit_scenario(tmp_path / "b")
    assert a == b, "确定性原语失效：同 seed 的 WAL 不一致，黄金守卫不可信"
    assert len(a) > 1000, "场景过小，可能没覆盖到提交路径"


def test_commit_wal_bytes_match_golden(tmp_path):
    """主断言：提交路径的事件字节不得因重构而变化。"""
    raw = _run_commit_scenario(tmp_path)
    actual = hashlib.sha256(raw).hexdigest()
    assert actual == GOLDEN_SHA256, (
        f"WAL 字节漂移：actual={actual} len={len(raw)}\n"
        "提交路径（阶段顺序 / 去重分支 / 事件字段）发生了变化。"
        "若这是**有意**的契约变更，必须同时更新本常量并在 PR 说明原因。"
    )


@pytest.mark.parametrize("seed", ["other-seed"])
def test_different_seed_changes_bytes(tmp_path, seed):
    """负对照：换 seed 必须不同（证明常量绑定的是事件流，不是空文件）。"""
    raw = _run_commit_scenario(tmp_path, seed=seed)
    assert hashlib.sha256(raw).hexdigest() != GOLDEN_SHA256
