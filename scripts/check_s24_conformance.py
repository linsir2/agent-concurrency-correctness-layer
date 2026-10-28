#!/usr/bin/env python3
"""S24 方案 ↔ 实现 一致性核对（可执行）。

用法：`python3 scripts/check_s24_conformance.py`（exit 0 = 全部一致）。
对应 `docs/s24-gap-analysis-and-repositioning.md` §5（S24-1 Verifier 门禁）。
"""

from __future__ import annotations

import contextlib
import inspect
import io
import itertools
import json
import os
import pathlib
import subprocess
import sys
import tempfile

_ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "tests"))

ok: list[str] = []
bad: list[tuple[str, str]] = []


def chk(name: str, cond: bool, detail: str = "") -> None:
    (ok if cond else bad).append((name, detail))


def _src(rel: str) -> str:
    return (_ROOT / rel).read_text(encoding="utf-8")


from conftest import make_harness

from harness import RscSession
from harness.facade import Deny, Receipt

# ---------------------------------------------------------------- 交付物
for rel in ("examples/semantic_conflict/run.py",
            "tests/test_s24_verify.py",
            "tests/test_s24_experiment.py",
            "docs/s24-gap-analysis-and-repositioning.md"):
    chk(f"交付存在 {rel}", (_ROOT / rel).exists())

# ---------------------------------------------------------------- 门面契约
_facade = _src("harness/facade.py")
chk("verify 方法存在", callable(getattr(RscSession, "verify", None)))
chk("构造参数 verify_cmd", "verify_cmd=None" in _facade)
chk("构造参数 verify_required", "verify_required: bool = False" in _facade)
chk("verify 需要工作区（fail-closed）", "verify_requires_worktree" in _facade)
chk("verify 不经 shell（argv）", "shlex.split" in _facade and "shell=True" not in _facade)
chk("verify 有超时", "subprocess.TimeoutExpired" in _facade)
chk("verify 在隔离副本跑", "copytree" in _facade and "rsc-verify-" in _facade)
chk("合并视图叠加账本", "workspace.manifest" in _facade)
chk("合并视图叠加 pending", "pending" in _facade and "_overlay" in _facade)
chk("apply 前过门禁", "_verify_gate(" in _facade)
chk("失败记录也拦（fail-closed）", "verify_failed_recorded" in _facade)
chk("收据带 verified", "verified: bool = False" in _facade)
_apply = inspect.getsource(RscSession._apply_locked)
chk("apply 返回 verified", "verified=self._verified(acb)" in _apply)
_ctor = inspect.getsource(RscSession.__init__)
chk("required 无 cmd 即拒绝构造", "verify_required=True 时必须提供 verify_cmd" in _ctor)

# ---------------------------------------------------------------- 适配器暴露
chk("sidecar 返回 verified", '"verified": r.verified' in _src(
    "harness/integrations/http_sidecar.py"))
chk("MCP 返回 verified", '"verified": r.verified' in _src(
    "harness/integrations/mcp_server.py"))

# ---------------------------------------------------------------- 文档/示例
_doc = _src("docs/s24-gap-analysis-and-repositioning.md")
chk("文档记录 Verifier 门禁", "Verifier" in _doc)
chk("文档记录 8 项差距", "差距排序" in _doc)
_ex = _src("examples/semantic_conflict/run.py")
for token in ("git_scenario", "rsc_scenario", "gate_scenario", "green_scenario"):
    chk(f"实验含 {token}", f"def {token}(" in _ex)
chk("README 链到 S24", "s24-gap-analysis-and-repositioning.md" in _src("README.md"))

# ---------------------------------------------------------------- S24-3 静态语义
chk("静态语义模块存在", (_ROOT / "harness/semantic.py").exists())
from harness.semantic import detect

chk("detect 可调用", callable(detect))
_sem = _src("harness/semantic.py")
chk("抑制星号导入", 'alias.name == "*"' in _sem)
chk("抑制动态模块（__getattr__）", "__getattr__" in _sem)
chk("条件定义为不确定", "uncertain" in _sem)
chk("跳过 TYPE_CHECKING", "TYPE_CHECKING" in _sem)
chk("跳过注解", "visit_AnnAssign" in _sem)
_facade2 = _src("harness/facade.py")
chk("门面导入 detect", "from .semantic import detect" in _facade2)
chk("semantic 三态校验", '"off", "warn", "block"' in _facade2)
chk("显式 check_semantics", "def check_semantics" in _facade2)
chk("静态门禁存在", "def _semantic_gate" in _facade2)
_apply2 = inspect.getsource(RscSession._apply_locked)
chk("静态门禁在动态验证之前",
    _apply2.index("_semantic_gate(") < _apply2.index("_verify_gate("))
chk("语义冲突用 Deny 而非 Conflict", 'code="semantic_conflict"' in _facade2)
chk("语义报告落盘", 'spec["rsc_semantic"]' in _facade2)
chk("测试覆盖三态", "semantic=mode" in _src("tests/test_s24_semantic.py"))
_chk_sem = detect(
    {"app/core.py": b"def f(x):\n    return x\n",
     "app/use.py": b"from app.core import f\n"},
    {"app/core.py": b"def g(x):\n    return x\n",
     "app/use.py": b"from app.core import f\n"})
chk("运行期检出改名引用", (not _chk_sem.ok)
    and _chk_sem.issues[0].name == "f")
chk("运行期兼容改动不报", detect(
    {"app/core.py": b"def f(x):\n    return x\n"},
    {"app/core.py": b"def f(x):\n    return x\ndef g():\n    return 1\n"}).ok)

# ---------------------------------------------------------------- S24-4 merge queue
chk("merge_queue 模块存在", (_ROOT / "harness/merge_queue.py").exists())
_mq = _src("harness/merge_queue.py")
chk("单写者 worker", "def _loop" in _mq and "popleft" in _mq)
chk("FIFO 队列", "deque(" in _mq)
chk("容量满信号", "QUEUE_FULL" in _mq)
chk("超时信号", "QUEUE_TIMEOUT" in _mq)
chk("取消未开始票", "def cancel" in _mq)
_facade3 = _src("harness/facade.py")
chk("默认关闭", "merge_queue: dict | None = None" in _facade3)
chk("apply_queued 存在", "def apply_queued" in _facade3)
chk("出队自持 token 锁", "with self._op_lock(ticket.token)" in _facade3)
_qt = _src("tests/test_s24_queue.py")
chk("用例覆盖队首重验", "test_head_reverify_blocks_semantic_conflict" in _qt)
chk("用例覆盖容量/超时/取消",
    "test_queue_capacity_returns_wait" in _qt
    and "test_timeout_returns_wait" in _qt
    and "test_cancel_pending_ticket" in _qt)

# ---------------------------------------------------------------- S24-5 合规语料
chk("语料模块存在", (_ROOT / "harness/spec/corpus.py").exists())
chk("语料 manifest 存在", (_ROOT / "harness/spec/conformance/manifest.json").exists())
chk("语料 runner 协议说明", (_ROOT / "harness/spec/conformance/README.md").exists())
from harness.spec.corpus import build_corpus, check_corpus

_manifest = json.loads(build_corpus()["manifest.json"].decode("utf-8"))
_corpus = check_corpus()
chk("磁盘语料无漂移", _corpus["ok"], str(_corpus["drift"]))
chk("语料向量可执行", _corpus["run"]["failed"] == 0,
    str(_corpus["run"]["errors"][:1]))
chk("语料覆盖 I1-I14", _manifest["totals"]["invariants"] == 14)
chk("语料向量 >= 30", _manifest["totals"]["vectors"] >= 30)
chk("I13 有可执行向量", "I13" in _manifest["totals"]["executable_invariants"])
chk("I9/I10 测试指针已修",
    "test_i9_plan_valid_acyclic" in _src("harness/spec/invariants.py")
    and "test_i10_isolation_private_worktree_invisible" in _src(
        "harness/spec/invariants.py"))
chk("语料版本化", "CORPUS_VERSION" in _src("harness/spec/corpus.py")
    and "corpus_version" in _src("harness/spec/corpus.py"))
chk("冻结签名不现场生成", '"payload_sha256"' in _src("harness/spec/corpus.py"))
_ct_corpus = _src("tests/test_s24_corpus.py")
for token in ("test_disk_corpus_matches_reference", "test_changed_expect_is_detected",
              "test_tampered_file_breaks_manifest_sha",
              "test_main_check_exit_one_on_drift",
              "test_invariant_test_pointers_exist"):
    chk(f"语料用例 {token}", f"def {token}(" in _ct_corpus)
chk("package-data 含语料", "conformance/vectors/*.json" in _src("pyproject.toml"))

# ---------------------------------------------------------------- S24-6 诊断 CLI
chk("诊断模块存在", (_ROOT / "harness/diagnose.py").exists())
chk("诊断用例存在", (_ROOT / "tests/test_s24_diagnose.py").exists())
_diag = _src("harness/diagnose.py")
chk("诊断不构造 Wal", "Wal(" not in _diag)
chk("诊断不构造 FileCheckpointStore", "FileCheckpointStore(" not in _diag)
for _bad in ("write_bytes(", "write_text(", "mkdir(", "unlink(", "truncate(",
             "os.replace(", "os.remove("):
    chk(f"诊断只读（无 {_bad}）", _bad not in _diag)
chk("诊断只读读 WAL（read_bytes）", "read_bytes" in _diag)
chk("退出码 0/1/2", 'return 0 if report["ok"] else 1' in _diag
    and "return 2" in _diag)
chk("rsc-diagnose 入口", 'rsc-diagnose = "harness.diagnose:main"' in _src(
    "pyproject.toml"))
chk("诊断 JSON schema", '"schema_version": SCHEMA_VERSION' in _diag)
_diag_test = _src("tests/test_s24_diagnose.py")
for token in ("test_mid_frame_crc_is_error", "test_broken_receipt_chain",
              "test_resource_version_gap", "test_diagnose_is_read_only",
              "test_lease_epoch_behind_wal", "test_cli_exit_one_on_error"):
    chk(f"诊断用例 {token}", f"def {token}(" in _diag_test)

# ---------------------------------------------------------------- S24-7 TLA+
_TLA = _ROOT / "spec" / "tla"
chk("TLA 目录存在", _TLA.is_dir())
chk("TLA 校验脚本存在", (_ROOT / "scripts/check_tla.py").exists())
chk("TLA 说明存在", (_TLA / "README.md").exists())
_TLA_CASES = (("I1", "I1SingleWriter", "AtMostOneWriter"),
              ("I2", "I2NoLostUpdate", "NoLostUpdate"),
              ("I4", "I4Capability", "CapabilityClosed"),
              ("I7", "I7EffectExactlyOnce", "EffectAtMostOnce"))
for _key, _mod, _inv in _TLA_CASES:
    chk(f"TLA {_key} 三件套",
        (_TLA / f"{_mod}.tla").is_file()
        and (_TLA / f"{_mod}.cfg").is_file()
        and (_TLA / f"{_mod}.broken.cfg").is_file())
    _tla_src = (_TLA / f"{_mod}.tla").read_text(encoding="utf-8") \
        if (_TLA / f"{_mod}.tla").is_file() else ""
    chk(f"TLA {_key} 模块名", f"---- MODULE {_mod} ----" in _tla_src)
    chk(f"TLA {_key} 不变量", f"{_inv} ==" in _tla_src and "TypeOK" in _tla_src)
    _cfg = (_TLA / f"{_mod}.cfg").read_text(encoding="utf-8") \
        if (_TLA / f"{_mod}.cfg").is_file() else ""
    chk(f"TLA {_key} cfg 声明不变量", f"INVARIANT {_inv}" in _cfg)
chk("TLA 主门禁不依赖 Java（--require 才硬失败）",
    "--require" in _src("scripts/check_tla.py")
    and "SKIP" in _src("scripts/check_tla.py"))
chk("TLA 用例缺 jar 显式 skip", "pytest.skip" in _src("tests/test_s24_tla.py")
    and "pytest.fail" in _src("tests/test_s24_tla.py"))
_tla_jar = os.environ.get("TLA2TOOLS_JAR")
if _tla_jar and pathlib.Path(_tla_jar).is_file():
    _tla_run = subprocess.run(
        [sys.executable, str(_ROOT / "scripts/check_tla.py"), "--jar", _tla_jar],
        capture_output=True, text=True, check=False)
    chk("TLA 实跑 8/0", _tla_run.returncode == 0, _tla_run.stdout[-200:])
else:
    chk("TLA 缺 jar 时 SKIP exit 0",
        subprocess.run([sys.executable, str(_ROOT / "scripts/check_tla.py")],
                       capture_output=True, text=True, check=False).returncode == 0)

# ---------------------------------------------------------------- S24-8 跨主机租约
chk("租约模块存在", (_ROOT / "harness/storage/lease.py").exists())
_lease_src = _src("harness/storage/lease.py")
chk("纯状态机（规则只写一份）", "class LeaseState" in _lease_src)
chk("单调 epoch + 接管", "self.epoch += 1" in _lease_src)
chk("旧视图 expected_epoch 被拒", "expected_epoch" in _lease_src
    and "fenced" in _lease_src)
from harness.storage.lease import LeaseState, MemoryLeaseStore

_st = LeaseState()
_g1 = _st.acquire(owner="a", instance="i1", now_ms=0, ttl_ms=10)
_g2 = _st.acquire(owner="b", instance="i2", now_ms=1, ttl_ms=10)
_g3 = _st.acquire(owner="b", instance="i2", now_ms=11, ttl_ms=10,
                  expected_epoch=1)
_g4 = _st.renew(owner="a", instance="i1", epoch=1, now_ms=12, ttl_ms=10)
chk("同一资源单持有者", _g1.epoch == 1 and _g2.outcome == "held")
chk("接管 epoch+1", _g3.epoch == 2)
chk("旧 epoch 续租被拒", _g4.outcome == "fenced")
_facade_h = _src("harness/harness.py")
chk("Harness 接受 lease_store", "lease_store: LeaseStore | None = None" in _facade_h)
chk("存储模式不写本地租约文件", "self._lease_store is not None" in _facade_h)
chk("写入口原因可观测", "writer_lease_unavailable" in _facade_h)
_raft_src = _src("harness/storage/raft.py")
chk("租约与事件同一 RSM", "class _ReplicatedLeaseLog" in _raft_src)
chk("提交点复核 stamp", "def append_record(self, rec: dict, stamp" in _raft_src)
chk("提交点拒绝旧 epoch", "旧 epoch 写入被 RSM 拒绝" in _raft_src)
chk("共享集群要求（不共享即拒绝注入）",
    "set_lease_fence" in _raft_src
    and "租约与日志必须同一 RSM" in _raft_src)
chk("RaftLeaseStore 存在", "class RaftLeaseStore" in _raft_src)
chk("缺 pysyncobj fail-closed", "require_pysyncobj()" in _raft_src)
_lease_tests = _src("tests/test_s24_lease.py")
_raft_tests = _src("tests/test_s24_raft_lease.py")
for token in ("test_second_harness_is_held_and_cannot_write",
              "test_takeover_fences_old_writer_commit",
              "test_store_failure_is_fail_closed_and_no_file_fallback",
              "test_bump_epoch_advances_store_and_self_stays_valid"):
    chk(f"租约用例 {token}", f"def {token}(" in _lease_tests)
for token in ("test_only_one_host_can_hold_lease",
              "test_shared_cluster_rejects_stale_append",
              "test_missing_pysyncobj_refuses_to_start"):
    chk(f"Raft 租约用例 {token}", f"def {token}(" in _raft_tests)

# ---------------------------------------------------------------- S24-9 token 基准
chk("bench 回放模块存在", (_ROOT / "bench/llm_replay.py").exists())
chk("live fixture 存在", (_ROOT / "bench/fixtures/llm_replay.json").exists())
from harness.llm import LLMReply, usage_from_response

chk("LLMReply 保留 usage", hasattr(LLMReply, "prompt_tokens")
    and hasattr(LLMReply, "total_tokens"))
_u = usage_from_response({"usage": {"prompt_tokens": 3,
                                    "completion_tokens": 4,
                                    "total_tokens": 7}})
chk("usage 解析正确", _u["total_tokens"] == 7 and _u["usage_missing"] is False)
chk("缺 usage 标 missing（不估算）",
    usage_from_response({})["usage_missing"] is True)
chk("chat 兼容旧签名", "return self.chat_with_usage(" in _src("harness/llm.py"))

from bench.llm_replay import compute_metrics, load_fixture

_fixture = load_fixture()
_metrics = compute_metrics(_fixture)
chk("fixture provenance=live", _fixture["provenance"] == "live")
chk("fixture 有真实 token", _metrics["conditions"]["naive"]["total_tokens"] > 0)
chk("token 倍数 > 1", _metrics["token_multiplier"] > 1)
chk("rsc 有返工", _metrics["conditions"]["rsc"]["rework_rate"] > 0)
chk("naive 丢写 / rsc 成功",
    _metrics["conditions"]["naive"]["outcome"]["success"] is False
    and _metrics["conditions"]["rsc"]["outcome"]["success"] is True)
_replay_src = _src("bench/llm_replay.py")
chk("回放离线可重算", "sum(int(t.get(\"total_tokens\", 0)) for t in turns)" in _replay_src)
chk("无 key 显式 skip", '"status": "skipped"' in _replay_src
    and "missing_api_key" in _replay_src)
chk("缺 usage 不估算（显式标志）", "usage_missing" in _src("harness/llm.py"))
_replay_tests = _src("tests/test_s24_replay.py")
for token in ("test_metrics_are_recomputed_from_turns",
              "test_killer_mutation_removing_retry_changes_metrics",
              "test_missing_usage_is_rejected",
              "test_live_without_key_skips_without_numbers"):
    chk(f"回放用例 {token}", f"def {token}(" in _replay_tests)
chk("usage 用例存在", (_ROOT / "tests/test_s24_llm_usage.py").exists())

# ---------------------------------------------------------------- S24-10 确定性
chk("entropy 模块存在", (_ROOT / "harness/entropy.py").exists())
from harness.entropy import Deterministic

_e1 = Deterministic("conformance-seed")
_ids_a = [_e1.uuid("event_id") for _ in range(20)]
_ids_b = [_e1.uuid("budget_reservation") for _ in range(20)]
chk("域隔离（不同域不碰撞）", not (set(_ids_a) & set(_ids_b)))
_e3 = Deterministic("conformance-seed")
chk("同 seed 同域可重建", _ids_a[:3] == [_e3.uuid("event_id") for _ in range(3)])
_e2 = Deterministic("s", start_ms=1_000, tick_ms=5)
_ticks = [_e2.now_ms() for _ in range(20)]
chk("模拟时钟严格单调", all(b > a for a, b in itertools.pairwise(_ticks)))
chk("event_id 走熵源", 'entropy.uuid("event_id")' in _src("harness/event_log.py"))
chk("run_id 可重放", "entropy.uuid('run_id')" in _src("harness/facade.py"))
chk("预算 id/时间可注入",
    "self._id_factory" in _src("harness/budget.py")
    and "def _now_s" in _src("harness/budget.py"))
chk("WAL generation 可注入",
    'entropy.uuid("wal_generation")' in _src("harness/storage/wal.py"))
chk("确定性拒绝 auth_secret",
    "确定性模式不支持 auth_secret" in _src("harness/harness.py"))
chk("确定性拒绝 merge_queue",
    "确定性模式不支持 merge_queue" in _src("harness/facade.py"))
_det_tests = _src("tests/test_s24_determinism.py")
for token in ("test_same_seed_replays_identical_wal",
              "test_different_seed_changes_bytes",
              "test_default_mode_is_still_random",
              "test_entropy_rejects_auth_secret"):
    chk(f"确定性用例 {token}", f"def {token}(" in _det_tests)


def _det_wal(root, seed):
    """同一固定场景跑一遍（用于门禁里的逐字节重放断言）。"""
    from harness.entropy import Deterministic as _D
    from harness.event_log import WalEventLog as _WEL
    from harness.harness import Harness as _H

    root.mkdir(parents=True, exist_ok=True)
    ent = _D(seed)
    wal = _WEL(str(root / "wal.log"), fsync=False, clock=ent, entropy=ent)
    h = _H(event_log=wal, checkpoint_root=str(root), entropy=ent, run_limits=10)
    s = RscSession(h, tenant="t", min_budget=1, state_dir=str(root / "idx"))
    tok = s.begin("a", "g", writes=["a.md"])
    s.apply(tok.token, [("a.md", "v1")], idempotency_key="k", agent_id="a")
    s.finish(tok.token)
    wal.close()
    return (root / "wal.log").read_bytes()


# ---------------------------------------------------------------- S24-11 timer + 定义
chk("timer 模块存在", (_ROOT / "harness/timer.py").exists())
chk("definitions 模块存在", (_ROOT / "harness/definitions.py").exists())
chk("TIMER 事件已登记", "TIMER_SET" in _src("harness/types.py")
    and "TIMER_FIRED" in _src("harness/types.py")
    and "TIMER_CANCELLED" in _src("harness/types.py"))
chk("tick 驱动 timer", "self.timers.fire_due(now)" in _src("harness/harness.py"))
chk("内存日志拒绝 durable_timers",
    "需要 WAL/Raft 事件日志" in _src("harness/harness.py"))
chk("定义在准入前校验",
    "占预算/槽位之前" in _src("harness/orchestration.py"))
chk("定义在提交前校验",
    "定义版本钉住" in _src("harness/harness.py"))
chk("同名异内容冲突", "class DefinitionConflict" in _src("harness/definitions.py"))
_det_t = _src("tests/test_s24_timer.py")
_det_d = _src("tests/test_s24_definitions.py")
for token in ("test_fires_only_when_due", "test_cancel_prevents_fire",
              "test_rebuild_from_wal_survives_restart",
              "test_harness_tick_drives_and_restarts",
              "test_default_off_has_no_timer_state"):
    chk(f"timer 用例 {token}", f"def {token}(" in _det_t)
for token in ("test_unknown_definition_rejected_before_budget",
              "test_hot_definition_change_blocks_old_acb_commit",
              "test_missing_registry_is_fail_closed",
              "test_plan_and_admit_pin_definition"):
    chk(f"定义用例 {token}", f"def {token}(" in _det_d)

# ---------------------------------------------------------------- 运行期
with tempfile.TemporaryDirectory() as tmp:
    h = make_harness(run_limits=10)
    session = RscSession(h, tenant="t", min_budget=1, worktrees=None,
                         state_dir=str(pathlib.Path(tmp) / "s"))
    tok = session.begin("a1", "i", writes=["a.md"])
    res = session.verify(tok.token, [sys.executable, "-c", "pass"])
    chk("无工作区 → DENIED", res.status.value == "DENIED")
    chk("无工作区原因正确",
        res.detail.get("reason") == "verify_requires_worktree")

    try:
        RscSession(h, verify_required=True)
        chk("required 无 cmd 拒绝构造", False)
    except ValueError:
        chk("required 无 cmd 拒绝构造", True)

    try:
        RscSession(h, verify_cmd=123)
        chk("非法 cmd 类型拒绝", False)
    except TypeError:
        chk("非法 cmd 类型拒绝", True)


    # merge queue 运行期：入队→出队→提交
    qs = RscSession(h, tenant="t", min_budget=1, worktrees=None,
                    state_dir=str(pathlib.Path(tmp) / "q"),
                    merge_queue={"capacity": 4})
    qtok = qs.begin("q1", "qi", writes=["q.md"])
    qr = qs.apply_queued(qtok.token, [("q.md", "qv")],
                         idempotency_key="qk", agent_id="q1")
    chk("队列提交成功", isinstance(qr, Receipt))
    chk("队列深度归零", qs.queue_depth() == 0)
    qs.close()

    # 显式失败记录 → apply 必须拒绝（即使没配 verify_cmd）
    import subprocess
    repo = pathlib.Path(tmp) / "repo"
    (repo / "app").mkdir(parents=True)
    subprocess.run(["git", "-C", str(repo), "init", "-q"], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.email", "c@e.com"],
                   check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.name", "c"],
                   check=True)
    (repo / "app/__init__.py").write_text("", encoding="utf-8")
    (repo / "app/b.py").write_text("Y = 1\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(repo), "add", "."], check=True)
    subprocess.run(["git", "-C", str(repo), "commit", "-qm", "init"], check=True)

    from harness.worktree import WorktreeManager
    session2 = RscSession(h, tenant="t", min_budget=1,
                          worktrees=WorktreeManager(str(repo),
                                                    str(pathlib.Path(tmp) / "wt")),
                          state_dir=str(pathlib.Path(tmp) / "s2"))
    tok2 = session2.begin("a2", "i2", writes=["app/b.py"])
    failing = [sys.executable, "-c", "raise SystemExit(3)"]
    assert session2.verify(tok2.token, failing).status.value == "DENIED"
    blocked = session2.apply(tok2.token, [("app/b.py", "Y = 2\n")],
                             idempotency_key="k2", agent_id="a2")
    chk("已记录失败 → apply 拒绝", isinstance(blocked, Deny))
    chk("拒绝码为 verify_failed", getattr(blocked, "code", "") == "verify_failed")
    chk("账本未被写入", h.workspace.version("app/b.py") == 0)

    # 通过记录 → 允许提交且收据 verified
    good = [sys.executable, "-c", "pass"]
    assert session2.verify(tok2.token, good).status.value == "OK"
    r = session2.apply(tok2.token, [("app/b.py", "Y = 2\n")],
                       idempotency_key="k3", agent_id="a2")
    chk("通过后可提交", isinstance(r, Receipt))
    chk("收据带 verified", getattr(r, "verified", False) is True)

    # S24-8 运行期：两个 Harness 共享内存租约，只有一个写者
    _store = MemoryLeaseStore()
    _h1 = make_harness(lease_store=_store)
    _h2 = make_harness(lease_store=_store)
    chk("第二个 Harness 被租约拒绝", _h2._writer_lease_ok is False)
    _store.set_fail(True)
    chk("租约后端故障 fail-closed",
        _h1._renew_writer_lease() is False and _h1._lease_outcome == "unavailable")

    # S24-11 运行期：timer 只触发一次；定义未知在准入前拒绝
    from harness.clock import ManualClock as _MC
    from harness.event_log import EventLog as _EL
    from harness.timer import DurableTimer as _DT
    _clock = _MC(start_ms=1_000)
    _tlog = _EL(clock=_clock)
    _timer = _DT(_tlog, _clock)
    _timer.schedule("t1", 1_500)
    chk("timer 到期前不触发", _timer.fire_due(1_400) == [])
    chk("timer 到期触发一次", len(_timer.fire_due(1_500)) == 1
        and _timer.fire_due(9_999) == [])
    chk("timer 取消后不触发", _timer.cancel("t1") is False)

    from harness.definitions import DefinitionRegistry as _DR
    _reg = _DR()
    _ref = _reg.register({"p": "v1"}, name="coder", version="1")
    chk("定义 content hash", len(_ref.content_hash) == 64 and _reg.check(_ref) == "")
    chk("未知定义拒绝", _reg.check({"name": "coder", "version": "2",
                                    "content_hash": _ref.content_hash})
        == "DEFINITION_UNKNOWN")
    _bad_task = {"task_id": "t", "logical_subgoal_id": "g",
                 "min_budget": 1, "definition": {"name": "coder", "version": "9",
                                                 "content_hash": _ref.content_hash}}
    _bad_plan = {"run_id": "r", "tasks": {"t": _bad_task}}
    _hdef = make_harness(definitions=_reg, run_limits=10)
    _bad_acb = _hdef.dispatcher.try_admit(_bad_task, "r", plan=_bad_plan)
    chk("定义未知 → 准入拒绝（零残留）",
        _bad_acb.state.value == "INADMISSIBLE"
        and _bad_acb.failure == "DEFINITION_UNKNOWN"
        and _hdef.budget._reservations == {})

    # S24-10 运行期：同 seed 的 WAL 逐字节相等；换 seed 必不同
    _det_root = pathlib.Path(tmp)
    chk("同 seed WAL 逐字节相等",
        _det_wal(_det_root / "d1", "gate-seed")
        == _det_wal(_det_root / "d2", "gate-seed"))
    chk("不同 seed WAL 不同",
        _det_wal(_det_root / "d3", "seed-a")
        != _det_wal(_det_root / "d4", "seed-b"))

    # S24-9 运行期：离线回放 JSON 可解析
    from bench.llm_replay import main as replay_main
    with contextlib.redirect_stdout(io.StringIO()):
        replay_rc = replay_main(["--fixture", str(_ROOT / "bench/fixtures/llm_replay.json"),
                                 "--json"])
    chk("token 回放 exit 0", replay_rc == 0)

    # S24-6 运行期：空状态 → 0；无参数 → 2（用法错误）
    from harness.diagnose import main as diag_main
    diag_dir = pathlib.Path(tmp) / "diag"
    diag_dir.mkdir()
    with contextlib.redirect_stdout(io.StringIO()), \
            contextlib.redirect_stderr(io.StringIO()):
        chk("诊断空状态 exit 0", diag_main(["--state-dir", str(diag_dir)]) == 0)
        chk("诊断无参数 exit 2", diag_main([]) == 2)

print(f"通过 {len(ok)} / 失败 {len(bad)}")
for name, detail in bad:
    print("  ❌", name, detail)
sys.exit(1 if bad else 0)
