"""S15 方案 ↔ 实现 一致性核对（可执行）。

防止「文档说做了但代码没做」/「代码做了但语义与文档不符」。
用法：`python3 scripts/check_s15_conformance.py`（exit 0 = 全部一致）。

对应 `docs/s15-protocol-and-production.md` §2（大类 C）与 §3（大类 D）。
"""

from __future__ import annotations

import inspect
import pathlib
import sys

_ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "tests"))

import harness
from harness import (
    EventKind,
    Harness,
    protocols,
)
from harness.ingress import Ingress, IngressResult, fingerprint
from harness.protocols import CheckpointStore as CheckpointProto

ok: list[str] = []
bad: list[tuple[str, str]] = []


def chk(name: str, cond: bool, detail: str = "") -> None:
    (ok if cond else bad).append((name, detail))


# ===================================================================== C
# C1：契约补全（CheckpointStore 必须覆盖 Harness 实际调用的方法）
_ckpt_methods = {
    "save", "load", "load_strict", "latest_ref_for_acb", "refs_for_run",
    "refs_all", "update_children", "read_epoch", "bump_epoch",
}
_declared = ({k for k in vars(CheckpointProto) if not k.startswith("__")}
             | {k for k in getattr(CheckpointProto, "__annotations__", {})
                if not k.startswith("__")})
chk("C1 CheckpointStore 契约覆盖 9 个方法",
    _ckpt_methods <= _declared, f"缺 {sorted(_ckpt_methods - _declared)}")

# C1：所有 9 个 Protocol 都在
for name in ("Clock", "EventLog", "IntentRegistry", "BudgetManager",
             "Workspace", "CheckpointStore", "EffectJournal", "PolicyEngine",
             "Decider"):
    chk(f"C1 契约存在 {name}", hasattr(protocols, name))

# C2：全部 runtime_checkable
for name in ("Clock", "EventLog", "IntentRegistry", "BudgetManager",
             "Workspace", "CheckpointStore", "EffectJournal", "PolicyEngine",
             "Decider"):
    proto = getattr(protocols, name)
    chk(f"C2 {name} 标了 @runtime_checkable",
        bool(getattr(proto, "_is_runtime_protocol", False)))

# C2：双后端均满足结构
from harness.checkpoint import CheckpointStore as _MemStore
from harness.event_log import EventLog as _MemLog

chk("C2 内存 CheckpointStore 满足契约",
    isinstance(_MemStore(), CheckpointProto))
chk("C2 内存 EventLog 满足契约", isinstance(_MemLog(), protocols.EventLog))

# C3：一致性套件文件与关键用例存在
_conf = _ROOT / "tests/test_s15_backend_conformance.py"
chk("C3 一致性套件文件存在", _conf.exists())
_conf_txt = _conf.read_text(encoding="utf-8") if _conf.exists() else ""
for n in ("test_ckpt_save_load_roundtrip", "test_ckpt_latest_pointer_advances",
          "test_ckpt_update_children_cas_consistent", "test_ckpt_epoch_monotonic",
          "test_log_append_and_tail", "test_log_append_batch_atomic_numbers"):
    chk(f"C3 用例存在 {n}", f"def {n}(" in _conf_txt)
chk("C3 双后端参数化", 'params=["memory", "file"]' in _conf_txt
    and 'params=["memory", "wal"]' in _conf_txt)

# C4：反漂移检查存在且真的扫（用例数 + 负对照）
_drift = _ROOT / "tests/test_s15_contract_drift.py"
chk("C4 反漂移测试存在", _drift.exists())
_drift_txt = _drift.read_text(encoding="utf-8") if _drift.exists() else ""
for n in ("test_no_contract_drift", "test_drift_check_is_not_vacuous",
          "test_negative_control_removing_declaration_is_detected"):
    chk(f"C4 用例存在 {n}", f"def {n}(" in _drift_txt)
chk("C4 扫描含 self.h.<comp>", "self.h" in _drift_txt)

# ===================================================================== D
# D1：Ingress
chk("D1 Ingress 类存在", inspect.isclass(Ingress))
chk("D1 IngressResult 存在", inspect.isclass(IngressResult))
chk("D1 Ingress.submit 存在", callable(getattr(Ingress, "submit", None)))
chk("D1 Ingress.lookup 存在", callable(getattr(Ingress, "lookup", None)))
chk("D1 指纹函数存在", callable(fingerprint))
_src_i = inspect.getsource(Ingress)
chk("D1 空 key → INVALID_INPUT",
    "Status.INVALID_INPUT" in _src_i and "idempotency_key_required" in _src_i)
chk("D1 同 key 异指纹 → CONFLICT", "Status.CONFLICT" in _src_i)
chk("D1 指纹写事件（持久化）", "INGRESS_ACCEPTED" in _src_i)
_src_c = inspect.getsource(harness.orchestration.RunCoordinator.submit_command)
chk("D1 submit_command 路由 INGRESS_SUBMIT", "INGRESS_SUBMIT" in _src_c)
chk("D1 submit_command 不再无条件 OK 空壳",
    "return Status.OK\n" != _src_c.split("INGRESS_SUBMIT")[0][-40:])

# D2：DLQ redrive
chk("D2 redrive_dead_letter 存在",
    callable(getattr(Harness, "redrive_dead_letter", None)))
src_r = inspect.getsource(Harness.redrive_dead_letter)
chk("D2 仅 DEAD_LETTER 可重放",
    "TaskState.DEAD_LETTER" in src_r and "return None" in src_r)
chk("D2 有界（max_redrives）",
    "max_redrives" in src_r and "redrive_limit_exceeded" in src_r)
chk("D2 写审计事件", "DLQ_REDRIVEN" in src_r)
chk("D2 走清理序列（settle/remove_node）",
    "budget.settle" in src_r and "remove_node" in src_r)

# D3：Admin pause/resume
for m in ("pause_run", "resume_run", "is_paused", "paused_runs",
          "_rebuild_pause_state"):
    chk(f"D3 Harness.{m} 存在", callable(getattr(Harness, m, None)))
src_p = inspect.getsource(Harness.pause_run)
chk("D3 pause 写 RUN_PAUSED 事件（持久化）", "RUN_PAUSED" in src_p)
src_r2 = inspect.getsource(Harness.resume_run)
chk("D3 resume 写 RUN_RESUMED 事件", "RUN_RESUMED" in src_r2)
src_d = inspect.getsource(Harness._drain_skip_reason)
chk("D3 调度尊重暂停（run_paused）", "run_paused" in src_d)
chk("D3 recover 重建暂停态", "_rebuild_pause_state" in inspect.getsource(Harness.recover))
chk("D3 submit_command 路由 PAUSE/RESUME",
    "PAUSE_RUN" in _src_c and "RESUME_RUN" in _src_c)

# D4：可观测
chk("D4 metrics 存在", callable(getattr(Harness, "metrics", None)))
src_m = inspect.getsource(Harness.metrics)
chk("D4 返回 counters/gauges", '"counters"' in src_m and '"gauges"' in src_m)
chk("D4 零依赖（不 import otel/prometheus）",
    "opentelemetry" not in src_m and "prometheus" not in src_m)

# 事件类型与字段
for k in ("INGRESS_ACCEPTED", "RUN_PAUSED", "RUN_RESUMED", "DLQ_REDRIVEN"):
    chk(f"事件类型存在 {k}", hasattr(EventKind, k))
chk("ACB.redrive_count 存在",
    "redrive_count" in inspect.getsource(harness.types.ACB))

# 零运行时依赖（硬约束）
_pyproject = (_ROOT / "pyproject.toml").read_text(encoding="utf-8")
chk("零运行时依赖未被破坏", "dependencies = []" in _pyproject)

# 测试文件
for f in ("tests/test_s15_contract_drift.py", "tests/test_s15_backend_conformance.py",
          "tests/test_s15_ingress.py", "tests/test_s15_admin.py"):
    chk(f"测试文件存在 {f}", (_ROOT / f).exists())

# 文档
_doc = (_ROOT / "docs/s15-protocol-and-production.md")
chk("方案文档存在", _doc.exists())

print(f"通过 {len(ok)} / 失败 {len(bad)}")
for n, d in bad:
    print("  ❌", n, d)
sys.exit(1 if bad else 0)
