"""S9a 契约测试：K-25（`CrashInjected` 穿透）+ K-26（`retention` 安全语义）。

对应 `docs/s9-persistence-and-receipts.md` v1.1 §2.1 / §2.2，判据 **S9-1** / **S9-2**。
"""

from __future__ import annotations

import copy

import pytest

from harness import ACB, Action, ContextAssembler, EventKind, EventLog, Harness
from harness import runtime as runtime_mod
from harness.runtime import CrashInjected


# ================================================================ S9-1 (K-25)
def test_s9_1_crash_injected_is_baseexception_not_exception():
    """S9-1 断言①：`CrashInjected` 是 `BaseException` 且**不是** `Exception`。"""
    exc = CrashInjected()
    assert isinstance(exc, BaseException) is True
    assert isinstance(exc, Exception) is False
    # 无参构造可用（测试夹具直接 `raise CrashInjected()`），且保留定位属性
    assert (exc.turn_seq, exc.action_index) == (0, 0)
    assert isinstance(CrashInjected(3, 7), BaseException)
    assert not isinstance(CrashInjected(3, 7), Exception)


def test_s9_1_handler_crash_passes_through_do_action(monkeypatch):
    """S9-1 断言②：handler 内崩溃**穿透** `_do_action`，不被映射成 `UPSTREAM_FAILED`。"""
    h = Harness()
    # S11a §2.2：`_do_action` 统一前置 gateway（token 必须存在）。本用例测的是
    # 「handler 崩溃穿透」，故给该路径**显式装 token**（不放宽断言、不改被测语义）。
    h.policy.default_token = h.policy.issue(
        "*", tools={"*"}, paths={"*"}, network={"*"}, data_class="*")

    def boom(rt, acb, action, i):
        raise CrashInjected(7, 3)

    def plain_error(rt, acb, action, i):
        raise ValueError("ordinary")

    # 唯一名 + monkeypatch：测试结束自动还原，不污染全局注册表
    monkeypatch.setitem(runtime_mod._HANDLERS, "boom_s9", boom)
    monkeypatch.setitem(runtime_mod._HANDLERS, "plain_s9", plain_error)
    acb = ACB(id="A-S9-1", run_id="R-S9-1", task_id="T-S9-1")

    with pytest.raises(CrashInjected):
        h.runtime._do_action(acb, Action(kind="boom_s9"), 0)

    # 对照：普通异常仍被映射（证明没有把 `except Exception` 改成 `BaseException`）
    mapped = h.runtime._do_action(acb, Action(kind="plain_s9"), 0)
    assert mapped.status is not None and mapped.status.value == "UPSTREAM_FAILED"
    assert mapped.detail == {"reason": "ValueError"}


def test_s9_1_probe_crash_not_swallowed(monkeypatch):
    """S9-1 断言③：probe 内崩溃**不返回 `"None"`**，而是抛出。"""
    h = Harness()

    def probe(idem_key):
        raise CrashInjected(0, 0)

    monkeypatch.setattr(h.effects, "_probe", probe)
    with pytest.raises(CrashInjected):
        h.effects.probe_one("idem-s9-1")

    # 未降级为 "None"：既没缓存三态结果，也没写 EFFECT_PROBED 事件
    assert "idem-s9-1" not in h.effects._probes
    assert not any(e.kind == EventKind.EFFECT_PROBED for e in h.event_log.all())


# ================================================================ S9-2 (K-26)
class _ReplayRecorder(EventLog):
    """记录每次 `replay(from_seq)` 的起点，以及调用瞬间 `acb.cursor` 的值。"""

    def __init__(self) -> None:
        super().__init__()
        self.calls: list[tuple[int, int | None]] = []
        self.acb = None

    def replay(self, subject=None, from_seq=0):
        self.calls.append((from_seq, getattr(self.acb, "cursor", None)))
        return super().replay(subject, from_seq)


class _FakeWorkspace:
    """最小 workspace 桩：仅供 `context_view` 的 `get(subject)` 回退分支。"""

    def __init__(self, values: dict | None = None) -> None:
        self.values = dict(values or {})

    def get(self, resource):
        return self.values.get(resource)


def _seed(log: EventLog, n: int, subject: str = "r") -> None:
    for i in range(n):
        log.append(EventKind.RESOURCE_COMMITTED, subject,
                   {"content": {"i": i}, "version": i})


def _observe(*, retention: int, cursor: int, materialized: dict,
             n_events: int, subject: str = "r",
             materialized_watermark: int = 0) -> dict:
    """跑一次 `context_view` 并返回可断言的可观测量（含插桩的 replay 起点）。

    `materialized_watermark`：物化快照覆盖到的事件水位（P1-7）。只有
    `watermark >= floor` 才允许跳；缺省 0 表示「快照不含被跳窗口」。
    """
    log = _ReplayRecorder()
    _seed(log, n_events, subject)
    acb = ACB(id="A-S9-2", run_id="R-S9-2", task_id="T-S9-2")
    acb.subscriptions.add("r")
    acb.cursor = cursor
    acb.materialized = copy.deepcopy(materialized)
    acb.materialized_watermark = materialized_watermark
    asm = ContextAssembler(log, _FakeWorkspace({"r": "LIVE"}), retention)
    log.acb = acb
    view = asm.context_view(acb)
    tail = log.tail()
    return {
        "tail": tail,
        "floor": max(0, tail - retention + 1) if retention > 0 else 0,
        "replay_from": log.calls[-1][0],
        "cursor_at_replay": log.calls[-1][1],
        "cursor_after": acb.cursor,
        "snapshots_forced": asm.snapshots_forced,
        "retention_bypassed": asm.retention_bypassed,
        "view": view,
    }


def test_s9_2_retention_zero_means_no_limit():
    """S9-2 断言①：`retention=0` → **不限制**（特判，不是 `floor=tail`）。"""
    obs = _observe(retention=0, cursor=0, materialized={}, n_events=6)
    assert obs["tail"] >= 4
    # 不限制的直接证据：重放起点就是原 cursor（没有被 floor 抬走）
    assert obs["replay_from"] == 0
    assert obs["cursor_at_replay"] == 0
    assert obs["retention_bypassed"] == 0
    assert obs["snapshots_forced"] == 0
    # 原逻辑不变：重放到 tail 后追平水位
    assert obs["cursor_after"] == obs["tail"]


def test_s9_2_no_snapshot_never_jumps():
    """S9-2 断言②：`retention=3, cursor=0, tail>=4, materialized={}` → **不跳**。"""
    obs = _observe(retention=3, cursor=0, materialized={}, n_events=6)
    assert obs["tail"] >= 4
    assert obs["floor"] == obs["tail"] - 3 + 1
    # 不跳：cursor 保持不变、从 0 全量重放（宁可慢，不丢状态）
    assert obs["cursor_at_replay"] == 0
    assert obs["replay_from"] == 0
    assert obs["retention_bypassed"] >= 1
    assert obs["snapshots_forced"] == 0
    # 全量重放 → 视图拿到完整历史的最新值
    assert obs["view"]["r"] == {"i": 5}


def test_s9_2_jump_needs_snapshot_watermark_and_negative_control(monkeypatch):
    """S9-2 断言③④⑤：快照水位 >= floor 才跳 + 负对照（旧实现必红）。"""
    # 事件主题故意不是订阅的 "r"：跳转后 "r" 只能来自快照 → 证明快照含前缀、未丢状态
    obs = _observe(retention=3, cursor=0, materialized={"r": "SNAP"},
                   n_events=6, subject="other",
                   materialized_watermark=6)      # 快照水位覆盖到 tail（>= floor=4）
    assert obs["floor"] == 4
    # 跳：cursor 被抬到 floor（replay 调用瞬间 cursor 即 floor），且计数 +1
    assert obs["cursor_at_replay"] == obs["floor"]
    assert obs["replay_from"] == obs["floor"]
    assert obs["snapshots_forced"] == 1
    assert obs["retention_bypassed"] == 0
    assert obs["view"]["r"] == "SNAP"

    # 反证（P1-7）：同样有快照，但水位=0（不含 [cursor, floor) 的订阅事件）→ 不得跳
    short = _observe(retention=3, cursor=0, materialized={"r": "SNAP"},
                     n_events=6, subject="other", materialized_watermark=0)
    assert short["snapshots_forced"] == 0
    assert short["retention_bypassed"] == 1

    # 负对照：monkeypatch 回 v1.0 旧实现（忽略 retention）→ 同一「跳」判据必须为 False
    monkeypatch.setattr(ContextAssembler, "context_view", _legacy_context_view)
    legacy = _observe(retention=3, cursor=0, materialized={"r": "SNAP"},
                      n_events=6, subject="other")
    assert _jump_criteria(legacy) is False
    assert legacy["cursor_at_replay"] == 0        # 旧实现不会抬 cursor
    assert legacy["snapshots_forced"] == 0

    # 判据本身有效（正控 True / 负控 False）
    assert _jump_criteria(obs) is True


def test_s9_2_negative_retention_rejected():
    """S9-2 断言④：`retention < 0` → 构造时 `ValueError`。"""
    with pytest.raises(ValueError):
        ContextAssembler(EventLog(), _FakeWorkspace(), -1)


# ================================================================ S9-2 P2：指标分桶
def _observe_scoped(*, run_id: str, subgoal: str, retention: int,
                    cursor: int, materialized: dict, n_events: int,
                    subject: str = "r",
                    materialized_watermark: int = 0) -> dict:
    """跑一次 `context_view` 并按 scope 分桶断言（P2）。"""
    log = _ReplayRecorder()
    _seed(log, n_events, subject)
    acb = ACB(id="A-S9-2B", run_id=run_id, task_id="T-S9-2B",
              logical_subgoal_id=subgoal)
    acb.subscriptions.add(subject)
    acb.cursor = cursor
    acb.materialized = copy.deepcopy(materialized)
    acb.materialized_watermark = materialized_watermark
    asm = ContextAssembler(log, _FakeWorkspace({subject: "LIVE"}), retention)
    asm.context_view(acb)
    scope = (run_id, subgoal)
    return {
        "snapshots_forced_by_scope": asm.snapshots_forced_by_scope.get(scope, 0),
        "retention_bypassed_by_scope": asm.retention_bypassed_by_scope.get(scope, 0),
        "no_snapshot_bypassed": asm.no_snapshot_bypassed.get(scope, 0),
        "watermark_short_bypassed": asm.watermark_short_bypassed.get(scope, 0),
    }


def test_s9_2_p2_retention_metrics_bucketed_by_scope():
    """S9-2 P2：`snapshots_forced`/`retention_bypassed` 按 run/subgoal 分桶，
    并区分「无快照」与「快照水位不足」。"""
    # 无快照 → no_snapshot_bypassed 分桶
    no_snap = _observe_scoped(run_id="R-1", subgoal="S-1", retention=3,
                              cursor=0, materialized={}, n_events=6)
    assert no_snap["retention_bypassed_by_scope"] == 1
    assert no_snap["no_snapshot_bypassed"] == 1
    assert no_snap["watermark_short_bypassed"] == 0
    assert no_snap["snapshots_forced_by_scope"] == 0

    # 快照水位不足 → watermark_short_bypassed 分桶
    short = _observe_scoped(run_id="R-1", subgoal="S-2", retention=3,
                            cursor=0, materialized={"r": "SNAP"},
                            n_events=6, subject="other",
                            materialized_watermark=0)
    assert short["retention_bypassed_by_scope"] == 1
    assert short["watermark_short_bypassed"] == 1
    assert short["no_snapshot_bypassed"] == 0

    # 快照水位覆盖 → snapshots_forced_by_scope 分桶
    forced = _observe_scoped(run_id="R-1", subgoal="S-3", retention=3,
                             cursor=0, materialized={"r": "SNAP"},
                             n_events=6, subject="other",
                             materialized_watermark=6)
    assert forced["snapshots_forced_by_scope"] == 1
    assert forced["retention_bypassed_by_scope"] == 0

    # 分桶彼此独立：不同 scope 不串味
    assert set(ContextAssembler(EventLog(), _FakeWorkspace(), 3).snapshots_forced_by_scope) == set()


def _jump_criteria(obs: dict) -> bool:
    """「跳」判据：正控（新实现）必须 True，负控（旧实现）必须 False。"""
    return (obs["cursor_at_replay"] == obs["floor"]
            and obs["replay_from"] == obs["floor"]
            and obs["snapshots_forced"] == 1
            and obs["view"].get("r") == "SNAP")


def _legacy_context_view(self, acb) -> dict:
    """v1.0 旧实现（负对照）：`retention` 只配置不读取，永不抬 cursor。"""
    view = copy.deepcopy(acb.materialized)
    for ev in self._event_log.replay(from_seq=acb.cursor):
        if ev.subject in acb.subscriptions:
            if isinstance(ev.payload, dict) and "content" in ev.payload:
                view[ev.subject] = ev.payload["content"]
            else:
                view[ev.subject] = self._workspace.get(ev.subject)
    acb.cursor = max(acb.cursor, self._event_log.tail())
    view["_private"] = dict(acb.staged)
    if acb.hitl_result is not None:
        view["_hitl"] = acb.hitl_result
    return view
