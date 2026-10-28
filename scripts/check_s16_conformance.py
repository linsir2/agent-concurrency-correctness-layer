"""S16 方案 ↔ 实现 一致性核对（可执行）。

防止「文档说做了但代码没做」/「代码做了但语义与文档不符」。
用法：`python3 scripts/check_s16_conformance.py`（exit 0 = 全部一致）。

对应 `docs/s16-ms9-raft.md` §0–§3。
"""

from __future__ import annotations

import inspect
import pathlib
import sys

_ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "tests"))

ok: list[str] = []
bad: list[tuple[str, str]] = []


def chk(name: str, cond: bool, detail: str = "") -> None:
    (ok if cond else bad).append((name, detail))


from harness import protocols
from harness.storage import raft as raft_mod

# ===================================================== 模块与契约
chk("raft 模块存在", hasattr(raft_mod, "RaftEventLog"))
chk("RaftCluster 存在", hasattr(raft_mod, "RaftCluster"))
chk("RaftEpochSource 存在", hasattr(raft_mod, "RaftEpochSource"))
chk("RaftUnavailable 存在", hasattr(raft_mod, "RaftUnavailable"))
chk("raft_conf 存在", callable(getattr(raft_mod, "raft_conf", None)))
chk("pick_free_base 存在（端口探测）",
    callable(getattr(raft_mod, "pick_free_base", None)))

# MS9：EventLog 走 raft 后端
chk("RaftEventLog 继承 _EventLogBase（查询语义复用）", any(
    " _EventLogBase" in f" {b.__name__}" or b.__name__ == "_EventLogBase"
    for b in raft_mod.RaftEventLog.__mro__),
    str([b.__name__ for b in raft_mod.RaftEventLog.__mro__]))

# 契约一致性（EventLog Protocol 全覆盖）
_declared = {k for k in vars(protocols.EventLog)
             if not k.startswith("__") and not k.startswith("_abc")
             and k not in ("_is_protocol", "_is_runtime_protocol")}
_missing = {m for m in _declared if not hasattr(raft_mod.RaftEventLog, m)}
chk("RaftEventLog 覆盖 EventLog 契约全部成员", not _missing, str(sorted(_missing)))

# 若 pysyncobj 可用，做结构校验
if raft_mod._HAS_PYSYNCOBJ:
    chk("RaftEventLog 通过 isinstance(EventLog)",
        issubclass(raft_mod.RaftEventLog, protocols.EventLog) or
        all(hasattr(raft_mod.RaftEventLog, m) for m in _declared))

# ===================================================== 关键设计约束
_src = inspect.getsource(raft_mod)
_import_lines = [ln.strip() for ln in _src.splitlines()
                 if ln.strip().startswith(("from pysyncobj", "import pysyncobj"))]
chk("**不使用** replicated_sync（实测会死锁）",
    not any("replicated_sync" in ln for ln in _import_lines)
    and "replicated_sync(" not in _src,
    f"imports={_import_lines}")
chk("使用受控 ticker（autoTick 关闭）",
    '"autoTick": False' in _src or "autoTick=False" in _src)
chk("有 wait_until 轮询助手（不用 sleep 猜时长）",
    "def wait_until" in _src)
chk("set_fencing 兼容空实现", "def set_fencing" in _src)
chk("无库时 fail-closed", "def require_pysyncobj" in _src
    and "RaftUnavailable" in _src)

# epoch 语义
_epoch_src = inspect.getsource(raft_mod.RaftEpochSource)
chk("bump_epoch 不自增（读共识 term）",
    "leader_term()" in _epoch_src and "self._epoch + 1" not in _epoch_src)

# ===================================================== 依赖约束
_pyproject = (_ROOT / "pyproject.toml").read_text(encoding="utf-8")
chk("dependencies = [] 未被破坏", "dependencies = []" in _pyproject)
chk("pysyncobj 在 optional-dependencies.raft",
    'raft = ["pysyncobj' in _pyproject)
chk("raft marker 已注册（strict-markers）", '"raft: ' in _pyproject)

# ===================================================== 测试
_t = _ROOT / "tests/test_s16_raft.py"
chk("测试文件存在", _t.exists())
_txt = _t.read_text(encoding="utf-8") if _t.exists() else ""
for n in ("test_s16_1_cluster_elects_leader",
          "test_s16_2_seq_assigned_by_leader_no_gaps",
          "test_s16_2_concurrent_append_serialized",
          "test_s16_3_replica_convergence",
          "test_s16_5_raft_log_satisfies_protocol",
          "test_s16_6_replay_and_has_filter",
          "test_s16_7_harness_runs_e2e_with_raft",
          "test_s16_8_epoch_comes_from_consensus",
          "test_s16_10_missing_pysyncobj_fails_closed"):
    chk(f"用例存在 {n}", f"def {n}(" in _txt)

# 三后端一致性套件真的加了 raft
_conf = (_ROOT / "tests/test_s15_backend_conformance.py").read_text(encoding="utf-8")
chk("后端一致性套件含 raft 参数", '"memory", "wal", "raft"' in _conf)
chk("一致性套件对 raft 有 finally close", "raft_log.close()" in _conf)

# ===================================================== 文档
_doc = (_ROOT / "docs/s16-ms9-raft.md")
chk("方案文档存在", _doc.exists())
_ms = (_ROOT / "docs/milestones.md").read_text(encoding="utf-8")
chk("MS9 出口条件已重述（不再沿用「35 个测试」）",
    "35 个测试" not in _ms.split("### MS9")[1].split("### MS10")[0]
    or "重述" in _ms.split("### MS9")[1].split("### MS10")[0])

print(f"通过 {len(ok)} / 失败 {len(bad)}")
for n, d in bad:
    print("  ❌", n, d)
sys.exit(1 if bad else 0)
