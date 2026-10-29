"""S2：commit 抽取的结构守卫（阶段顺序 / 清理不变量 / 部分租约回收）。

与 `test_s2_commit_golden.py` 分工：
- golden 锁**事件字节**（结果层）；
- 本文件锁**控制流**（阶段顺序、finally 覆盖范围），补上「结果对但清理漏了」这类
  只在故障路径暴露的缺陷。

RIPR：
- Reachability：真正走进每个阶段与 `finally`；
- Infection：注入 reserve 失败 / tainted scope（故障注入到状态）；
- Propagation：阶段序列错误、锁未释放、租约未 revoke → 断言失败；
- Revealability：失败信息打印实际序列 / 资源名。
"""

from __future__ import annotations

import pytest
from conftest import make_harness

from harness.kernel.commit import CommitService
from harness.types import ACB, ReceiptConflict, Status, Write

ORDER = ["_phase_preflight", "_phase_acquire", "_phase_records",
         "_phase_append", "_phase_project", "_cleanup"]


def _acb(run_id: str = "R-S2", subgoal: str = "S1") -> ACB:
    return ACB(id="A-S2", run_id=run_id, task_id="T1", logical_subgoal_id=subgoal)


def _canonical_order(seen: list[str]) -> bool:
    return seen == ORDER


def test_commit_phase_order(monkeypatch):
    """主断言：阶段必须按 ①→⑧ 顺序执行，最后一定是 cleanup。"""
    h = make_harness()
    acb = _acb()
    acb.staged["r"] = "v1"
    acb.base_version["r"] = 0

    seen: list[str] = []
    for name in ORDER:
        orig = getattr(CommitService, name)

        def wrapper(self, ctx, _n=name, _f=orig):
            seen.append(_n)
            return _f(self, ctx)

        monkeypatch.setattr(CommitService, name, wrapper)

    assert h.commit_batch(acb, [Write("r")]) == Status.OK
    assert _canonical_order(seen), f"阶段顺序漂移：{seen}"


def test_order_checker_negative_control():
    """负对照：交换两个阶段必须被判错（否则上面的断言是空的）。"""
    swapped = list(ORDER)
    swapped[1], swapped[2] = swapped[2], swapped[1]
    assert not _canonical_order(swapped)
    assert not _canonical_order(ORDER[:-1])
    missing_cleanup = ORDER[:-1]
    assert not _canonical_order(missing_cleanup), "缺 cleanup 必须判错"


def test_partial_lease_revoked_when_second_reserve_fails(monkeypatch):
    """② 阶段中途失败：已预留的租约必须被 finally revoke，且账本零写入。"""
    h = make_harness()
    acb = _acb()
    for res in ("r1", "r2"):
        acb.base_version[res] = 0
        acb.staged[res] = "v"

    calls = {"n": 0}
    orig = h.intents.reserve

    def flaky(*a, **k):
        calls["n"] += 1
        return None if calls["n"] >= 2 else orig(*a, **k)

    monkeypatch.setattr(h.intents, "reserve", flaky)
    st = h.commit_batch(acb, [Write("r1"), Write("r2")])

    assert st == Status.DENIED, st
    assert h.intents.active_intents("r1") == [], "第一个租约泄漏（finally 未覆盖）"
    assert h.workspace.version("r1") == 0 and h.workspace.version("r2") == 0


def test_cleanup_on_receipt_conflict(monkeypatch):
    """append 阶段抛 ReceiptConflict：锁必须释放、租约必须 revoke、事件不得提交。"""
    h = make_harness(auth_secret="s3cr3t")
    acb = _acb()
    acb.base_version["r"] = 0
    acb.staged["r"] = "v1"
    assert h.commit_batch(acb, [Write("r")]) == Status.OK

    # 让 scope 处于 tainted → 第二次提交在 append 阶段抛 ReceiptConflict
    scope = (acb.run_id, acb.logical_subgoal_id)
    h.state.tainted_scopes.add(scope)
    acb.base_version["r"] = h.workspace.version("r")
    acb.staged["r"] = "v2"

    notified: list = []
    orig_notify = h.event_log.notify_events
    monkeypatch.setattr(h.event_log, "notify_events",
                        lambda evs: (notified.append(len(evs)), orig_notify(evs))[1])

    with pytest.raises(ReceiptConflict):
        h.commit_batch(acb, [Write("r")])

    # 资源锁已释放：主线程可零等待获取
    lock = h.workspace.lock_for("r")
    assert lock.acquire(timeout=0), "资源锁泄漏（finally 未执行）"
    lock.release()
    assert h.intents.active_intents("r") == [], "租约泄漏（finally 未执行）"
    assert h.workspace.get("r") == "v1", "失败提交不得写入"
    assert notified == [], "events 为 None 时不得 notify"
