"""S12：egress 后端收口 + 审计正确性 + 多 run 隔离（`docs/s12-kernel-enforcement-and-core.md` v1.1 §2）。

| 用例 | 判据 |
|---|---|
| S12-1 | K-45：三条回扫路径注入失败 → 提交被拒、tail/head 不变、既有链 True、**粘滞**；负对照（静默回退）→ 断链 |
| S12-2 | K-40：预算事件 `e.run_id == run_id`；`budget_events(run_id)` 非空 |
| S12-3 | K-43：两 run 各自 bump → 互不 fence；负对照 `epoch_scope="global"` → 互相 fence |
| S12-4 | K-33：run B 活动不推进 run A 保留窗口；视图不串 run（同名 subject） |
| S12-5 | `unshare -Urn` 子进程 connect → `ENETUNREACH`；负对照无 `--net` → 不同；启动前探针 + 专用异常 |
| S12-6 | Landlock errno：fake syscall errno 38/95 → `(0,38)`/`(0,95)`；`≥1` → `(abi, None)` |
| S12-7 | 生产 gate：`create_production_harness()` 无强制点 → 拒绝启动 |
| S12-8 | gVisor 适配（外部部署；无 `runsc` → xfail + 登记） |

环境 invalid 一律 `pytest.xfail` + `_egress_env.register()` **显式登记**（禁止静默 skip）。
"""

from __future__ import annotations

import contextlib
import ctypes
import errno
import socket
import subprocess
import sys
import threading

import pytest
from _egress_env import FEATURE_UNSHARE, probe_environment, register, unshare_available

from harness.kernel.receipts import ReceiptChain
from harness import (
    ACB,
    EpochStore,
    EventKind,
    Harness,
    Lease,
    ReceiptScanFailed,
    Result,
    Status,
    TaskState,
    Write,
    create_production_harness,
)
from harness.egress import (
    EgressStatus,
    EgressUnavailable,
    SandboxUnavailable,
    container,
    lsm,
    netns,
    probe,
)
from harness.event_log import WalEventLog
from harness.storage.wal import Wal

SECRET = "s12-receipt-secret"
FEATURE_GVISOR = "gvisor-runsc"

pytestmark = pytest.mark.egress


# ================================================================ 夹具/工具
def _tokenized(h: Harness) -> None:
    h.policy.default_token = h.policy.issue(
        "*", tools={"*"}, paths={"*"}, network={"*"}, data_class="*")


def _wal_harness(tmp_path, name: str = "cp", **kw) -> tuple[Harness, WalEventLog]:
    log = WalEventLog(str(tmp_path / "wal.log"))
    h = Harness(run_limits=1000, event_log=log,
                checkpoint_root=str(tmp_path / name),
                auth_secret=SECRET, **kw)
    _tokenized(h)
    return h, log


def _acb(run_id: str = "R-S12-1", subgoal: str = "S1", task_id: str = "T1") -> ACB:
    return ACB(id=f"A-{run_id}-{task_id}", run_id=run_id, task_id=task_id,
               logical_subgoal_id=subgoal)


def _commit(h: Harness, acb: ACB, resource: str = "r", content=None) -> Result:
    h.workspace.stage(acb, resource, {"v": 1} if content is None else content)
    return h.commit_batch(acb, [Write(resource)])


def _connect_snippet(host: str, port: int) -> str:
    return (
        "import socket,sys,errno\n"
        f"s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)\n"
        "s.settimeout(2)\n"
        "try:\n"
        f"    s.connect(('{host}',{port}))\n"
        "except OSError as e:\n"
        "    sys.stderr.write('ERR:%s\\n'%errno.errorcode.get(e.errno,'UNKNOWN'))\n"
        "    sys.exit(1)\n"
        "sys.exit(0)\n"
    )


@contextlib.contextmanager
def _listener():
    """本地监听器（正/负对照的真实目标）。"""
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(8)
    srv.settimeout(0.1)
    port = srv.getsockname()[1]
    stop = threading.Event()
    conns: list[socket.socket] = []

    def _loop():
        while not stop.is_set():
            try:
                conn, _ = srv.accept()
                conns.append(conn)
            except (TimeoutError, OSError):
                continue

    thread = threading.Thread(target=_loop, daemon=True)
    thread.start()
    try:
        yield port, conns
    finally:
        stop.set()
        thread.join(2)
        for conn in conns:
            conn.close()
        srv.close()


# ================================================================ S12-1（K-45）
@pytest.mark.parametrize("path", [
    "_receipt_records",          # ① 内存/WAL 统一入口
    "receipts_from",             # ② WAL 增量索引
    "_rescan_wal_from_disk",     # ③ 兼容旧 WAL 的回扫路径
])
def test_s12_1_receipt_scan_failure_is_fail_closed(tmp_path, monkeypatch, path):
    """回扫失败 → 提交被拒（UPSTREAM_FAILED/receipt_scan_failed），
    tail/head 不变、既有链仍 True、**不 taint**、粘滞直到全量重建。"""
    h, log = _wal_harness(tmp_path)
    acb = _acb()
    assert _commit(h, acb) == Status.OK
    scope = (acb.run_id, acb.logical_subgoal_id)
    head_before = h._receipt_heads[scope]
    tail_before = log.tail()
    assert h.verify_receipts(acb.run_id, acb.logical_subgoal_id) is True

    if path == "_receipt_records":
        def boom(self, watermark: int = 0):
            raise RuntimeError("fake: receipt scan failed")
        monkeypatch.setattr(ReceiptChain, "_receipt_records", boom)
    elif path == "receipts_from":
        def boom(self, watermark: int = 0):
            raise RuntimeError("fake: wal receipts_from failed")
        monkeypatch.setattr(Wal, "receipts_from", boom)
    else:
        # 让 `_receipt_records` 走「无 receipts_from」的兼容分支 → 真回扫路径；
        # 故障注入到 **Wal._rescan_from_disk**（而不是 Harness 的包装函数），
        # 这样能验证 `_rescan_wal_from_disk` 本身不再吞异常（fail-closed）。
        monkeypatch.setattr(Wal, "receipts_from", None)

        def boom() -> None:
            raise RuntimeError("fake: rescan failed")

        monkeypatch.setattr(Wal, "_rescan_from_disk", boom)
        monkeypatch.setattr(h.event_log._wal, "_cached_end", -1)   # 迫使回扫

    acb2 = _acb(task_id="T2")
    st = _commit(h, acb2, resource="r2", content={"v": 2})
    assert st.status is Status.UPSTREAM_FAILED
    assert st.detail == {"reason": "receipt_scan_failed"}
    assert h._receipt_scan_failed is True, "回扫失败必须置粘滞闩锁"
    assert log.tail() == tail_before, "被拒的写不得落盘（tail 不变）"
    assert h._receipt_heads[scope] == head_before, "被拒的写不得改 head"
    assert h._tainted_scopes == set(), "未 append → 链未污染，不得 taint"
    assert h.verify_receipts(acb.run_id, acb.logical_subgoal_id) is True
    assert h._receipt_scan_watermark != 0 or tail_before == 0, \
        "绝不把水位清水位到 0（会 taint 所有 scope）"

    # 粘滞：闩锁未解除前，后续每次提交都被拒
    acb3 = _acb(task_id="T3")
    st3 = _commit(h, acb3, resource="r3", content={"v": 3})
    assert st3.status is Status.UPSTREAM_FAILED
    assert st3.detail == {"reason": "receipt_scan_failed"}
    assert log.tail() == tail_before

    # 一次**成功的全量重建** → 解除闩锁，写入口恢复服务
    monkeypatch.undo()
    h._rebuild_receipts()
    assert h._receipt_scan_failed is False
    acb4 = _acb(task_id="T4")
    assert _commit(h, acb4, resource="r4", content={"v": 4}) == Status.OK
    assert log.tail() > tail_before
    assert h.verify_receipts(acb.run_id, acb.logical_subgoal_id) is True, \
        "全量重建后恢复服务，链必须仍然有效"


def test_s12_1_shared_wal_refresh_keeps_chain_valid(tmp_path):
    """正对照：共享 WAL 时锁内刷新看到他人收据 → 接着它的 head 续链（不断链）。"""
    wal_path = str(tmp_path / "wal.log")
    h1 = Harness(event_log=WalEventLog(wal_path),
                 checkpoint_root=str(tmp_path / "cp1"),
                 writer_id="w1", auth_secret=SECRET)
    _tokenized(h1)
    h2 = Harness(event_log=WalEventLog(wal_path), auth_secret=SECRET)   # 非 durable
    _tokenized(h2)

    run_id, sub = "R-S12-1P", "S1"
    assert _commit(h1, ACB(id="A1", run_id=run_id, task_id="T1",
                           logical_subgoal_id=sub),
                   resource="r", content={"v": 1}) == Status.OK
    assert _commit(h2, ACB(id="A2", run_id=run_id, task_id="T2",
                           logical_subgoal_id=sub),
                   resource="r", content={"v": 2}) == Status.OK
    assert _commit(h1, ACB(id="A3", run_id=run_id, task_id="T3",
                           logical_subgoal_id=sub),
                   resource="r", content={"v": 3}) == Status.OK

    reader = Harness(event_log=WalEventLog(wal_path), auth_secret=SECRET)
    assert reader.verify_receipts(run_id, sub) is True, \
        "h1 未刷新到 h2 的收据 → 链分叉（正对照无牙）"


def test_s12_1_negative_control_silent_fallback_forks_chain(tmp_path, monkeypatch):
    """负对照：**静默实现**（刷新失败直接返回、不更新 head）→ h1 用陈旧 head
    续链 → 真分叉（断链）。证明「静默回退」不是可接受实现。"""
    wal_path = str(tmp_path / "wal.log")
    h1 = Harness(event_log=WalEventLog(wal_path),
                 checkpoint_root=str(tmp_path / "cp1"),
                 writer_id="w1", auth_secret=SECRET)
    _tokenized(h1)
    h2 = Harness(event_log=WalEventLog(wal_path), auth_secret=SECRET)
    _tokenized(h2)

    run_id, sub = "R-S12-1N", "S1"
    assert _commit(h1, ACB(id="A1", run_id=run_id, task_id="T1",
                           logical_subgoal_id=sub),
                   resource="r", content={"v": 1}) == Status.OK
    assert _commit(h2, ACB(id="A2", run_id=run_id, task_id="T2",
                           logical_subgoal_id=sub),
                   resource="r", content={"v": 2}) == Status.OK

    # 静默实现：回扫「成功」但返回空（等价于旧 `except Exception: return`
    # 与 v1.0 的静默回退）→ h1 用陈旧 head 续链
    monkeypatch.setattr(ReceiptChain, "_receipt_records", lambda self, watermark=0: [])
    assert _commit(h1, ACB(id="A3", run_id=run_id, task_id="T3",
                           logical_subgoal_id=sub),
                   resource="r", content={"v": 3}) == Status.OK

    reader = Harness(event_log=WalEventLog(wal_path), auth_secret=SECRET)
    assert reader.verify_receipts(run_id, sub) is False, \
        "静默回退必须断链（否则负对照恒真、判据无效）"


def test_s12_1_receipt_scan_failed_is_dedicated_exception():
    """K-45：`ReceiptScanFailed` 是**专用**异常（不复用 `EgressUnavailable`）。"""
    assert issubclass(ReceiptScanFailed, RuntimeError)
    assert not issubclass(ReceiptScanFailed, EgressUnavailable)
    assert ReceiptScanFailed is not EgressUnavailable


# ================================================================ S12-2（K-40）
def test_s12_2_budget_events_carry_run_id():
    """预算事件 `e.run_id == run_id`；`budget_events(run_id)` 非空。"""
    h = Harness(run_limits=1000)
    h.budget.set_run_id("R-S12-2")
    node = h.budget.create_node("B-S12-2", 100)
    assert node.run_id == "R-S12-2"
    resv = h.budget.reserve("B-S12-2", 10)
    assert resv is not None
    assert h.budget.consume(resv.reservation_id, 1) == Status.OK
    h.budget.release(resv.reservation_id)

    evs = h.budget.budget_events("R-S12-2")
    assert evs, "budget_events(run_id) 不得为空（否则 K-40 未落地）"
    assert all(e.run_id == "R-S12-2" for e in evs), \
        f"存在未带 run_id 的预算事件：{[e.run_id for e in evs]}"
    assert h.budget.budget_events("R-OTHER") == [], "不得串 run"
    # 向后兼容：无参 → 全部
    assert len(h.budget.budget_events()) >= len(evs)
    # K-40：树完整的种子集/校验同步支持 run 过滤
    assert "B-S12-2" in h._declared_budget_nodes("R-S12-2")
    assert h._declared_budget_nodes("R-OTHER") == set()
    h.budget._tree_integrity("R-S12-2")


def test_s12_2_budget_events_backward_compatible_without_run_context():
    """无 run 上下文 → 事件 `run_id=""`（全局键，向后兼容；不串 run 过滤）。"""
    h = Harness(run_limits=1000)
    assert h.budget.create_node("B-NORUN", 100) is not None
    resv = h.budget.reserve("B-NORUN", 5)
    assert resv is not None
    assert h.budget.settle(resv.reservation_id) == 5
    evs = h.budget.budget_events("")
    assert evs, "无 run 标签的事件归全局键（旧语义）"
    assert all(e.run_id == "" for e in evs)
    assert h.budget.budget_events() == evs


def test_s12_2_budget_events_carry_run_id_production_path():
    """生产路径（`dispatcher.try_admit`）的预算事件同样带 run_id。"""
    h = Harness(run_limits=1000)
    plan = h.planner.plan("R-S12-2P", [{"task_id": "T1", "min_budget": 10}])
    h.plans["R-S12-2P"] = plan
    acb = h.dispatcher.try_admit(plan["tasks"]["T1"], "R-S12-2P", plan=plan)
    assert acb.state is TaskState.ADMITTED
    assert acb.budget_node.run_id == "R-S12-2P"

    evs = h.budget.budget_events("R-S12-2P")
    assert evs, "生产路径必须带 run_id（否则 budget_events(run_id) 恒为空）"
    assert all(e.run_id == "R-S12-2P" for e in evs)


# ================================================================ S12-3（K-43）
@pytest.mark.parametrize("scope", ["run", "global"])
def test_s12_3_two_layer_epoch_parameterized(scope):
    """同一段代码按 `epoch_scope` 参数化：

    - `"run"`：A bump 后 B 的租约/预算仍有效（run 层隔离）；
    - `"global"`（负对照）：A bump 走全局 writer lease 层 → B 一并被 fence。
    """
    h = Harness(run_limits=1000, epoch_scope=scope)
    la = h.intents.reserve("ra", "A1", 0, run_id="RUN-A")
    lb = h.intents.reserve("rb", "B1", 0, run_id="RUN-B")
    assert h.intents.is_valid(la) and h.intents.is_valid(lb)

    epoch = h.bump_epoch("supervisor_reclaim", run_id="RUN-A")
    assert isinstance(epoch, int) and epoch >= 1
    assert h.intents.is_valid(la) is False, "本 run 的租约必须失效"

    if scope == "run":
        assert h.intents.is_valid(lb) is True, "其他 run 的租约不得被 fence"
        assert h.intents.get_epoch("RUN-A") == epoch
        assert h.intents.get_epoch("RUN-B") == 0
        assert h.intents.get_epoch() == 0, "run 层 bump 不得推进全局 writer lease"
        # 预算层同样隔离
        assert h.budget.create_node("B-A", 100, run_id="RUN-A") is not None
        assert h.budget.create_node("B-B", 100, run_id="RUN-B") is not None
        assert h.budget.reserve("B-B", 5) is not None, "RUN-B 不得被 RUN-A 的 bump fence"
        assert h.budget.reserve("B-A", 5) is None, "RUN-A 自身必须被 fence"
    else:
        assert h.intents.is_valid(lb) is False, \
            "全局 epoch 必须作废**全部**租约（负对照无牙）"
        assert h.intents.get_epoch() >= 1, "全局 writer lease 必须推进"


def test_s12_3_bump_epoch_without_run_id_stays_global():
    """向后兼容：`bump_epoch()` 无参 → 全局（现有调用不破）。"""
    h = Harness(run_limits=1000)
    la = h.intents.reserve("ra", "A1", 0, run_id="RUN-A")
    lb = h.intents.reserve("rb", "B1", 0, run_id="RUN-B")
    h.bump_epoch("lease_expired")
    assert h.intents.is_valid(la) is False
    assert h.intents.is_valid(lb) is False
    assert h.intents.get_epoch() >= 1


def test_s12_3_epoch_store_is_per_run(tmp_path):
    """`EpochStore` 按 run 分键；无参 → 全局键；`path_prefix` → 持久化。"""
    store = EpochStore()
    assert store.read_epoch("A") == 0 and store.read_epoch("B") == 0
    assert store.bump_epoch("A") == 1
    assert store.bump_epoch("A") == 2
    assert store.read_epoch("B") == 0, "run 之间不得串"
    assert store.bump_epoch() == 1, "无参 → 全局键"
    assert store.read_epoch("A") == 2, "全局键不得影响 run 键"

    # 持久化：`wal.log.epoch.<run_id>` 同款每 run 文件（新实例可读回）
    prefix = str(tmp_path / "wal.log.epoch")
    disk = EpochStore(prefix)
    assert disk.bump_epoch("R-1") == 1
    assert disk.bump_epoch("R-1") == 2
    reopened = EpochStore(prefix)
    assert reopened.read_epoch("R-1") == 2, "run 层 epoch 必须持久化"
    assert reopened.read_epoch("R-2") == 0
    assert (tmp_path / "wal.log.epoch.R-1").read_text(encoding="utf-8") == "2"


def test_s12_3_budget_run_scope_edges():
    """`BudgetManager` 的 run 层分支：非法 scope 回退 / 单调 set_epoch /
    durable 读取失败不误杀写路径。"""
    from harness.budget import BudgetManager

    assert BudgetManager(run_scope="bogus")._run_scope == "global"

    # 无 event_log / 无 epoch_source（纯内存预算管理器）→ 旧分支仍可写
    bare = BudgetManager(run_scope="run")
    assert bare.run_id == ""
    assert bare.create_node("B-NOEV", 10) is not None      # event_log=None → 不落事件
    assert bare.reserve("B-NOEV", 5) is not None           # 无 durable 源 → 不 fence
    assert bare.reserve("B-MISSING", 1) is None            # 节点不存在

    h = Harness(run_limits=1000, epoch_scope="run")
    assert h.budget.create_node("B-E", 100, run_id="R-E") is not None
    assert h.budget.set_epoch(3, run_id="R-E") == 3
    assert h.budget.set_epoch(2, run_id="R-E") == 3, "run 层 epoch 单调（不回绕）"
    assert h.budget.set_epoch(4, emit=False, run_id="R-E") == 4

    def boom():
        raise RuntimeError("fake: durable read failed")

    h.budget._epoch_source = boom
    assert h.budget._durable_epoch("R-E") == 4, \
        "durable 读失败退回本地/store 值（不得误杀写路径）"


def test_s12_3_intent_registry_epoch_branches():
    """`IntentRegistry` 的 per-run epoch 三个分支（store bump / 显式 epoch / 全局）。"""
    h = Harness(run_limits=1000, epoch_scope="run")
    intents = h.intents

    # run 层（store 推进）：只影响本 run
    lb = intents.reserve("rb", "B1", 0, run_id="RUN-B")
    la = intents.reserve("ra", "A1", 0, run_id="RUN-A")
    assert intents.bump_epoch("RUN-B") == 1
    assert intents.get_epoch("RUN-B") == 1
    assert intents.get_epoch("RUN-A") == 0, "其他 run 的 epoch 不得被推进"
    assert intents.is_valid(lb) is False
    assert intents.is_valid(la) is True

    # 显式 epoch（调用方统一推进，避免双计数）
    lc = intents.reserve("rc", "C1", 0, run_id="RUN-C")
    assert intents.bump_epoch("RUN-C", epoch=7) == 7
    assert intents.get_epoch("RUN-C") == 7
    assert intents.is_valid(lc) is False

    # 恢复：run 层写 store；全局写 self._epoch
    assert intents.set_epoch(9, run_id="RUN-D") == 9
    assert intents.get_epoch("RUN-D") == 9
    assert intents.set_epoch(4) == 4
    assert intents.get_epoch() == 4

    # 全局（无参）→ 作废全部租约
    ld = intents.reserve("rd", "D1", 0, run_id="RUN-D")
    le = intents.reserve("re", "E1", 0, run_id="RUN-E")
    assert intents.bump_epoch() == intents.get_epoch()
    assert intents.is_valid(ld) is False and intents.is_valid(le) is False

    # 覆盖几个旧分支（revoke/undeclare/active_intents）
    lf = intents.reserve("rf", "F1", 0)
    assert intents.active_intents("rf")[0].lease_id == lf.lease_id
    intents.revoke(Lease("L-UNKNOWN", "rf", "F1", 0))     # 注册表无该 id → 不抛
    assert intents.undeclare_logical("dep-unknown", "F1") is None


# ================================================================ S12-4（K-33）
@pytest.mark.parametrize("scope,advanced", [("run", False), ("global", True)])
def test_s12_4_per_run_retention_parameterized(scope, advanced):
    """精确事件数：retention=5、run A 3 条、run B 100 条。

    - `epoch_scope="run"`：A 的 floor 不因 B 推进（per-run tail）；
    - `epoch_scope="global"`（负对照）：保持现状（全局 tail）→ A 的 floor 被推进。
    视图：run 层按 run_id 过滤（同名 subject 不串味）。
    """
    h = Harness(retention=5, epoch_scope=scope)
    run_a, run_b = "R-A", "R-B"
    for i in range(3):
        h.event_log.append(EventKind.RESOURCE_COMMITTED, "r",
                           {"content": {"run": "A", "i": i}, "version": i + 1},
                           run_id=run_a)
    for i in range(100):
        h.event_log.append(EventKind.RESOURCE_COMMITTED, "r",
                           {"content": {"run": "B", "i": i}, "version": i + 1},
                           run_id=run_b)

    if advanced:
        assert h.floor_for(run_a) > 0, "全局模式应保持旧语义（全局 tail）"
        assert h.floor_for(run_a) == max(0, h.event_log.tail() - 5 + 1)
    else:
        assert h.floor_for(run_a) == 0, "run B 的活动不得推进 run A 的保留窗口"
        assert h.floor_for(run_b) > 0, "run B 自己的窗口必须推进"
        assert h._run_cursors[run_a] == 3
        assert h._run_cursors[run_b] == 103

    acb_a = ACB(id="A-S12-4", run_id=run_a, task_id="T", logical_subgoal_id="S")
    acb_a.subscriptions.add("r")
    view = h.context.context_view(acb_a)
    if advanced:
        # 旧语义：不做 run 过滤 → 同名 subject 被 B 覆盖（保持现状）
        assert view["r"] == {"run": "B", "i": 99}
    else:
        assert view["r"] == {"run": "A", "i": 2}, f"视图串 run：{view['r']!r}"


def test_s12_4_floor_for_uses_per_run_tail_not_global():
    """`floor_for` 在 `epoch_scope="run"` 时用 per-run tail（不是全局 tail）。"""
    h = Harness(retention=3, epoch_scope="run")
    # S3.0：注入改为 late-lookup（lambda），不再是 bound method —— 断言改为
    # 行为断言：实例上替换 `floor_for` 后必须被 context 侧看见。
    assert callable(h.context._floor_for)
    h.floor_for = lambda run_id: 4242
    assert h.context._floor_for("R-ANY") == 4242, "floor_for 必须是调用时查找"
    del h.floor_for          # 还原为类方法（仅实例属性）
    h.event_log.append(EventKind.RESOURCE_COMMITTED, "r", {"v": 1}, run_id="R-X")
    assert h.floor_for("R-X") == 0
    for i in range(10):
        h.event_log.append(EventKind.RESOURCE_COMMITTED, "r", {"v": i}, run_id="R-Y")
    # R-X 的 tail 仍是 1（R-Y 的 11 个事件与它无关）
    assert h.floor_for("R-X") == 0
    assert h.floor_for("R-Y") == 9


# ================================================================ S12-5（unshare -Urn）
def test_s12_5_userns_child_gets_enetunreach():
    """正对照：`unshare -Urn` 子进程 connect → `ENETUNREACH`（**非** ECONNREFUSED）。"""
    ok, reason = unshare_available()
    if not ok:
        pytest.xfail(register(FEATURE_UNSHARE, reason))

    proc = subprocess.run(["unshare", "-Urn", sys.executable,
                           "-c", _connect_snippet("192.0.2.1", 443)],
                          capture_output=True, text=True, timeout=30, check=False)
    out = (proc.stderr or "").strip()
    assert out == "ERR:ENETUNREACH", f"期望 ENETUNREACH，实际 {out!r}"
    assert "ECONNREFUSED" not in out


def test_s12_5_negative_control_without_net_differs():
    """负对照：去掉 `--net`（只留 user ns）→ 能连上宿主监听器（结果不同）。"""
    ok, reason = unshare_available()
    if not ok:
        pytest.xfail(register(FEATURE_UNSHARE, reason))

    with _listener() as (port, _conns):
        inside = subprocess.run(["unshare", "-Urn", sys.executable,
                                 "-c", _connect_snippet("127.0.0.1", port)],
                                capture_output=True, text=True, timeout=30,
                                check=False)
        outside = subprocess.run(["unshare", "-Ur", sys.executable,
                                  "-c", _connect_snippet("127.0.0.1", port)],
                                 capture_output=True, text=True, timeout=30,
                                 check=False)

    assert (inside.stderr or "").strip() == "ERR:ENETUNREACH"
    assert outside.returncode == 0, \
        f"去掉 --net 后应能连上宿主监听器：{outside.stderr}"
    assert inside.stderr != outside.stderr


def test_s12_5_spawn_userns_uses_spec_argv(monkeypatch):
    """`spawn_userns` 的规范 argv 必须与规范一致（`--map-root-user` 而非 `--map-root`）。"""
    assert netns.USERNS_ARGV == ["unshare", "--user", "--map-root-user", "--net",
                                 "--fork", "--pid", "--mount-proc"]
    assert netns.USERNS_SHORT == ["unshare", "-Urn"]
    monkeypatch.setattr(netns, "_probe_userns", lambda python=None, prefix=None: True)
    captured: dict = {}

    class _Popen:
        def __init__(self, argv, **kwargs):
            captured["argv"] = list(argv)

    monkeypatch.setattr(netns.subprocess, "Popen", _Popen)
    netns.spawn_userns(["echo", "hi"])
    assert captured["argv"] == [*netns.USERNS_ARGV, "echo", "hi"]


def test_s12_5_spawn_userns_probe_failure_raises(monkeypatch):
    """探针未过 → `SandboxUnavailable`（**专用**异常）+ 显式降级登记。"""
    monkeypatch.setattr(netns, "_probe_userns", lambda python=None, prefix=None: False)
    before = len(netns.degradations())
    with pytest.raises(SandboxUnavailable, match="探针未通过"):
        netns.spawn_userns(["true"])
    assert not issubclass(SandboxUnavailable, EgressUnavailable)
    assert len(netns.degradations()) == before + 1, "降级必须显式登记（不静默）"


def test_s12_5_spawn_userns_missing_binary_raises(monkeypatch):
    """无 `unshare` → `SandboxUnavailable` + 登记。"""
    monkeypatch.setattr("shutil.which", lambda name: None)
    with pytest.raises(SandboxUnavailable, match="unshare 不可用"):
        netns.spawn_userns(["true"])
    assert netns.degradations()


def test_s12_5_spawn_userns_real_child(monkeypatch):
    """真跑一次 `spawn_userns`：**默认拒绝**语义 —— 子进程不得连上外部地址。

    规范 argv 含 `--mount-proc/--fork/--pid`；本沙箱 `--mount-proc` 返回 EPERM
    → `SandboxUnavailable`（**不静默降级**为残缺 argv）→ `xfail` + 显式登记。
    """
    ok, reason = netns.userns_available()
    if not ok:
        pytest.xfail(register(FEATURE_UNSHARE, reason))

    try:
        proc = netns.spawn_userns([sys.executable,
                                   "-c", _connect_snippet("192.0.2.1", 443)],
                                  stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE, text=True)
    except SandboxUnavailable as exc:
        pytest.xfail(register(FEATURE_UNSHARE, f"完整 argv 不可用：{exc}"))
        return
    _out, err = proc.communicate(timeout=30)
    err = (err or "").strip()
    assert proc.returncode != 0, "默认拒绝：子进程不得连上外部地址"
    if err.startswith("ERR:"):
        assert err == "ERR:ENETUNREACH", f"期望 ENETUNREACH，实际 {err!r}"
    else:
        register(FEATURE_UNSHARE, f"spawn_userns 子进程未执行（{err}）")


def test_s12_5_userns_eperm_env_is_xfail_registered():
    """`EPERM`（无 user/net ns 权限）→ `xfail` + 显式登记（**不 skip**）。"""
    ok, reason = unshare_available()
    if ok:
        proc = subprocess.run(["unshare", "-Urn", sys.executable,
                               "-c", _connect_snippet("192.0.2.1", 443)],
                              capture_output=True, text=True, timeout=30, check=False)
        err = (proc.stderr or "").strip()
        assert err == "ERR:ENETUNREACH", f"可用时应为 ENETUNREACH，实际 {err!r}"
        return
    pytest.xfail(register(FEATURE_UNSHARE, f"EPERM/能力缺失：{reason}"))


def test_s12_5_probe_is_cached_per_session(monkeypatch):
    """探针结果 session 级缓存（避免每用例重复起子进程）。"""
    calls: list[list[str]] = []

    class _Proc:
        returncode = 1
        stderr = "ERR:ENETUNREACH"

    def fake_run(argv, **kwargs):
        calls.append(list(argv))
        return _Proc()

    monkeypatch.setattr(netns, "_PROBE_CACHE", {})
    monkeypatch.setattr(netns.subprocess, "run", fake_run)
    prefix = list(netns.USERNS_SHORT)
    assert netns._probe_userns(python="/py", prefix=prefix) is True
    assert netns._probe_userns(python="/py", prefix=prefix) is True
    assert len(calls) == 1, "同一 (python, argv) 只探测一次"


def test_s12_5_probe_and_degradation_are_registered_consistently():
    """探针结果与降级登记口径一致（环境 invalid 不得静默）。"""
    snap = probe_environment()
    ok, _reason = netns.userns_available()
    if not ok:
        assert netns.degradations(), "netns 后端不可用必须登记降级"
    else:
        assert netns._probe_userns(), "userns_available 与探针结果必须一致"
    assert snap is not None


# ================================================================ S12-6（Landlock errno）
def test_s12_6_landlock_probe_reports_errno(monkeypatch):
    """fake syscall 返回 -1 + errno 38/95 → `(0,38)`/`(0,95)`；`≥1` → `(abi, None)`。"""
    monkeypatch.setattr(lsm.platform, "machine", lambda: "x86_64")

    def fake(nr: int, err: int) -> int:
        ctypes.set_errno(err)
        return -1

    assert lsm.landlock_probe(lambda nr: fake(nr, errno.ENOSYS)) == (0, errno.ENOSYS)
    assert lsm.landlock_probe(lambda nr: fake(nr, errno.EOPNOTSUPP)) == \
        (0, errno.EOPNOTSUPP)
    assert lsm.landlock_probe(lambda nr: fake(nr, errno.EPERM)) == (0, errno.EPERM)
    assert lsm.landlock_probe(lambda nr: 6) == (6, None)
    assert lsm.landlock_probe(lambda nr: 10) == (10, None)
    # int 兼容：`landlock_abi_version` 仍返回 int
    assert isinstance(lsm.landlock_abi_version(), int)


def test_s12_6_landlock_probe_non_generic_arch(monkeypatch):
    """非通用架构（i686/riscv）→ `(0, None)` + **显式登记**。"""
    monkeypatch.setattr(lsm.platform, "machine", lambda: "i686")
    assert lsm.landlock_probe(lambda nr: 6) == (0, None)
    assert lsm.arch_degradations(), "非通用架构必须显式登记（不静默）"


def test_s12_6_landlock_errno_constants_distinguishable():
    """四成因判别的前提：38 / 95 / 1 必须两两不同。"""
    assert errno.ENOSYS not in (errno.EOPNOTSUPP, errno.EPERM)
    assert errno.EOPNOTSUPP not in (errno.ENOSYS, errno.EPERM)


# ================================================================ S12-7（生产 gate）
def _unavailable() -> EgressStatus:
    return EgressStatus(lsm=False, netns=False, landlock_abi=0,
                        dns_enforceable=False, reasons=("fake: no backend",))


def _available() -> EgressStatus:
    return EgressStatus(lsm=True, netns=False, landlock_abi=10,
                        dns_enforceable=True)


def test_s12_7_production_harness_refuses_without_enforcement(monkeypatch):
    """无强制点 → 拒绝启动（fail-closed）。"""
    import harness.egress as egress_pkg

    monkeypatch.setattr(egress_pkg, "probe", _unavailable)
    with pytest.raises(EgressUnavailable, match="无可用 egress 强制点"):
        create_production_harness()

    # `Harness()` 默认仍可构造（本地可跑；出站仍 fail-closed）
    h = Harness()
    assert h.egress.required is None


def test_s12_7_production_harness_constructs_with_enforcement(monkeypatch):
    """有强制点 → 正常构造，且 `egress_required=True`。"""
    import harness.egress as egress_pkg

    monkeypatch.setattr(egress_pkg, "probe", _available)
    h = create_production_harness()
    assert h.egress.required is True
    assert h.egress.ensure().available is True
    assert h.egress.degradations == ()


def test_s12_7_production_harness_real_probe():
    """真实探测口径：无强制点 → 拒绝启动；有 → 构造成功。"""
    status = probe()
    if status.available:
        h = create_production_harness()
        assert h.egress.required is True
    else:
        with pytest.raises(EgressUnavailable):
            create_production_harness()
        h = Harness()
        with pytest.raises(EgressUnavailable):
            h.egress.ensure()
        assert h.egress.degradations == status.reasons


# ================================================================ S12-8（gVisor）
def test_s12_8_spawn_in_gvisor_uses_runsc_direct(monkeypatch, tmp_path):
    """直接调 `runsc run --bundle <dir>`（不依赖 Docker daemon）。"""
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    (bundle / "config.json").write_text("{}")
    captured: dict = {}

    class _Popen:
        def __init__(self, argv, **kwargs):
            captured["argv"] = list(argv)

    monkeypatch.setattr(container, "runsc_path", lambda: "/usr/bin/runsc")
    monkeypatch.setattr(container.subprocess, "Popen", _Popen)
    container.spawn_in_gvisor(["echo", "hi"], str(bundle))

    assert captured["argv"] == ["/usr/bin/runsc", "run", "--bundle", str(bundle),
                               "harness-egress", "echo", "hi"]


def test_s12_8_spawn_in_gvisor_missing_runsc_raises(monkeypatch, tmp_path):
    """无 `runsc` → `SandboxUnavailable`（**专用**异常）+ 登记。"""
    monkeypatch.setattr(container, "runsc_path", lambda: None)
    with pytest.raises(SandboxUnavailable, match="runsc"):
        container.spawn_in_gvisor(["true"], str(tmp_path))
    assert container.degradations()


def test_s12_8_spawn_in_gvisor_invalid_bundle_raises(monkeypatch, tmp_path):
    """bundle 无 `config.json` → 拒绝裸跑（fail-closed）+ 登记。"""
    monkeypatch.setattr(container, "runsc_path", lambda: "/usr/bin/runsc")
    with pytest.raises(SandboxUnavailable, match="bundle"):
        container.spawn_in_gvisor(["true"], str(tmp_path))
    assert container.degradations()


def test_s12_8_gvisor_backend_environment_registration():
    """外部部署：无 `runsc` → xfail + **显式登记**（不静默）。"""
    ok, reason = container.gvisor_available()
    if not ok:
        pytest.xfail(register(FEATURE_GVISOR, reason))
    assert container.runsc_path()


def test_s12_8_enforce_userns_and_gvisor_backends(monkeypatch, tmp_path):
    """`enforce()` 增加 `userns` / `gvisor` 后端选项（显式才启用）。"""
    import harness.egress as egress_pkg

    class _Proc:
        def wait(self):
            return 0

    monkeypatch.setattr(netns, "spawn_userns", lambda argv, **kw: _Proc())
    assert egress_pkg.enforce(["true"], backend="userns") == 0

    monkeypatch.setattr(container, "spawn_in_gvisor",
                        lambda argv, bundle, **kw: _Proc())
    bundle = str(tmp_path / "bundle")
    (tmp_path / "bundle").mkdir()
    assert egress_pkg.enforce(["true"], backend="gvisor", bundle=bundle) == 0

    with pytest.raises(ValueError, match="bundle"):
        egress_pkg.enforce(["true"], backend="gvisor")
