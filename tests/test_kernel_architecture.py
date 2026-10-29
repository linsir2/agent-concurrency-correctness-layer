"""S3.4：**内核架构守卫**（体量 / 边界 / 冻结配置 / 公共面 / 无环 / 构造期零事件）。

为什么需要它：
    god-module 拆分（`harness/harness.py` 2473 行、160 个方法）把实现搬进
    `harness/kernel/{state,identity,receipts,leases,commit,children,tasks,dispatch}.py`，
    而 `Harness` 保留同名委托直到 S5。**搬迁期是最容易被侵蚀的时期**：
    - 新服务可以悄悄长出 300 行方法（下一个 god module 的胚胎）；
    - 服务之间可以绕开公开接缝直接摸 `self.deps.tasks._finalize`（隐性耦合）；
    - 可热改的配置可以被冻进 `*Deps`（frozen dataclass），之后 `h.lease_ms = ...`
      静默失效；
    - 160 个方法的搬迁可能**丢**一两个（没有测试引用它们时无人发现）；
    - kernel 反过来 `from ..harness import Harness` → 循环依赖，拆分变假象。

本文件把这些「架构不变量」变成**可执行判据**。每条断言都带负对照或扫描面自检：
一个永远绿的检查等于没有检查（见 `docs/…` 里「判据必须能红」的纪律）。

S3.4 是**纯测试**步骤：本文件不引入任何生产代码改动。
"""

from __future__ import annotations

import ast
import pathlib

from harness import Harness

_ROOT = pathlib.Path(__file__).resolve().parent.parent
_KERNEL_DIR = _ROOT / "harness" / "kernel"

# ---------------------------------------------------------------------------
# 1) 体量：单个方法 > 150 行 = 需要再拆
# ---------------------------------------------------------------------------
_METHOD_SIZE_LIMIT = 150

# **唯一**豁免：`spawn_child`（198 行）是 S5 的显式欠账（父子树 + 上限 + 令牌衰减
# + 槽位 + 准入的回滚链本来就是一体）。allowlist 必须**恰好**是这一条，
# 并由 `test_service_method_size_limit` 断言其不能增长、也不能「烂在名单里」。
_METHOD_SIZE_ALLOWLIST: set[tuple[str, str]] = {("children.py", "spawn_child")}

# ---------------------------------------------------------------------------
# 2) 边界：服务只能经**公开**接缝互相调用
# ---------------------------------------------------------------------------
# kernel 里的**服务**（拆出来的 8 个模块的服务类名）
_SERVICES: frozenset[str] = frozenset({
    "commit", "receipts", "leases", "identity", "children", "tasks", "dispatch",
})

# **组件私有缝**是合法的：组件（workspace/budget/...）本来就不是「服务」，
# 它们由 Harness 注入 kernel，其私有方法在 S1~S3 期间被服务直接使用（如
# `self.deps.workspace._root`、`self.deps.checkpoints._store`）。
# 这里显式列出，防止有人为了「让检查变绿」而把组件名混进 `_SERVICES`。
_COMPONENT_SEAMS: frozenset[str] = frozenset({
    "workspace", "budget", "checkpoints", "event_log", "intents", "policy",
    "effects", "slots", "hitl", "blobs", "comp_journal", "timers",
    "run_coordinator", "planner", "dispatcher", "scheduler", "reconcile",
})

# ---------------------------------------------------------------------------
# 3) 配置：可热改配置**不能**冻进 *Deps
# ---------------------------------------------------------------------------
# 这些名字全部是 Harness 上「构造后可改」的配置（`self.state.<name>` + property
# setter）。一旦被 `@dataclass(frozen=True)` 的 `*Deps` 抓成快照，
# `h.lease_ms = 10` 之后 service 仍读旧值 → **静默失效**（比报错更糟）。
_MUTABLE_CONFIG: frozenset[str] = frozenset({
    "lease_ms", "receipts_enabled", "lock_timeout_s", "aging_ms",
    "aging_max_attempts", "max_redrives", "poison_after_failures",
    "pool_timeout_s", "drain_timeout_s", "scheduler_mode", "replan_budget",
    "hitl_timeout_ms", "claim_lease_ms", "retention", "max_lease_ttl_s",
})

# 既有欠账（S5 一起收）：`ReceiptChain` 在构造时把 `retention` 快照进
# `ReceiptDeps`（`floor_for` 读 `self.deps.retention`）。S3.4 是纯测试步骤，
# **不许**改生产代码，所以这里显式登记，并且：
#   - 断言 allowlist **恰好**是这一条（不能悄悄长）；
#   - 断言它**仍然**是真违规（S5 修掉后本测试会红，逼着把名单删掉）。
_KNOWN_CONFIG_SNAPSHOTS: set[tuple[str, str, str]] = set()

# ---------------------------------------------------------------------------
# 4) 公共面：S3 之前 `Harness` 的公开方法/属性（refactor 前基线）
# ---------------------------------------------------------------------------
# 由 `git show 999b8c3:harness/harness.py` + `ast` 计算得出（跳过 `_` 开头与
# `__init__`）：160 个成员中 45 个公开。**这是 pre-refactor 基线快照**，
# 只增不减 —— 搬迁不得「丢」任何一个名字。
_BASELINE_PUBLIC_SURFACE: frozenset[str] = frozenset({
    "abort", "apply_result_envelope", "auth", "bump_epoch", "cancel_timer",
    "check_definition", "claim_scope", "claim_window_s", "commit_batch",
    "compensate", "dispatch_and_run", "drain", "ensure_tenant",
    "escalate_quarantined", "export_observability", "finish", "floor_for",
    "idem_key", "is_paused", "kill_run", "metrics", "next_deadline_ms",
    "pause_run", "paused_runs", "propagate_failure", "prune_budget", "pump",
    "purge_run", "readmit_after_aging", "receipts_of", "reconcile_scope",
    "recover", "redrive_dead_letter", "replan", "replan_from_quarantine",
    "requeue_readmit", "result_envelope", "resume_run", "run_task", "set_timer",
    "spawn_child", "supervisor_note_crash", "tick", "usage_report",
    "verify_receipts",
})


# ===========================================================================
# 共用扫描器（全部接受「任意 AST」，以便负对照可喂合成源码）
# ===========================================================================
def _kernel_files() -> list[pathlib.Path]:
    files = sorted(_KERNEL_DIR.glob("*.py"))
    assert files, f"找不到 kernel 模块（{_KERNEL_DIR}）"
    return files


def _parse(path: pathlib.Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _methods(tree: ast.AST) -> list[ast.FunctionDef | ast.AsyncFunctionDef]:
    return [n for n in ast.walk(tree)
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]


def _span(node: ast.AST) -> int:
    """方法体量（含 `def` 行到末行；`spawn_child` 记为 198 行）。"""
    return int(node.end_lineno or node.lineno) - int(node.lineno) + 1


def _private_service_accesses(tree: ast.AST, filename: str = "<ast>") -> list[str]:
    """`self.deps.<svc>._<m>` / `self.<svc>._<m>`（svc ∈ `_SERVICES`，非 dunder）。

    组件私有缝（`_COMPONENT_SEAMS`）不在 `_SERVICES` 里，天然不会被命中。
    """
    hits: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Attribute):
            continue
        member = node.attr
        if not member.startswith("_") or member.startswith("__"):
            continue
        holder = node.value
        # self.deps.<svc>._<m>
        if (isinstance(holder, ast.Attribute)
                and isinstance(holder.value, ast.Attribute)
                and isinstance(holder.value.value, ast.Name)
                and holder.value.value.id == "self"
                and holder.value.attr == "deps"
                and holder.attr in _SERVICES):
            hits.append(f"{filename}:{node.lineno} self.deps.{holder.attr}.{member}")
            continue
        # self.<svc>._<m>
        if (isinstance(holder, ast.Attribute)
                and isinstance(holder.value, ast.Name)
                and holder.value.id == "self"
                and holder.attr in _SERVICES):
            hits.append(f"{filename}:{node.lineno} self.{holder.attr}.{member}")
    return hits


def _cross_service_accesses(tree: ast.AST) -> list[str]:
    """所有 `self.deps.<svc>.<m>`（公开 + 私有），仅用于自检扫描面非空。"""
    out: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Attribute):
            continue
        holder = node.value
        if (isinstance(holder, ast.Attribute)
                and isinstance(holder.value, ast.Attribute)
                and isinstance(holder.value.value, ast.Name)
                and holder.value.value.id == "self"
                and holder.value.attr == "deps"
                and holder.attr in _SERVICES):
            out.append(f"self.deps.{holder.attr}.{node.attr}")
    return out


def _is_dataclass(cls: ast.ClassDef) -> bool:
    for dec in cls.decorator_list:
        target = dec.func if isinstance(dec, ast.Call) else dec
        name = getattr(target, "attr", None) or getattr(target, "id", None)
        if name == "dataclass":
            return True
    return False


def _deps_dataclasses(tree: ast.AST) -> list[ast.ClassDef]:
    """`*Deps` 的 dataclass 定义（`CommitDeps` / `ReceiptDeps` / …）。"""
    return [n for n in ast.walk(tree)
            if isinstance(n, ast.ClassDef)
            and n.name.endswith("Deps")
            and _is_dataclass(n)]


def _deps_fields(cls: ast.ClassDef) -> list[str]:
    return [stmt.target.id for stmt in cls.body
            if isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name)]


def _frozen_config_violations(
        tree: ast.AST, filename: str = "<ast>") -> list[tuple[str, str, str]]:
    """`(file, class, field)`：被冻进 `*Deps` 的可热改配置字段。"""
    out: list[tuple[str, str, str]] = []
    for cls in _deps_dataclasses(tree):
        for name in _deps_fields(cls):
            if name in _MUTABLE_CONFIG:
                out.append((filename, cls.name, name))
    return out


def _harness_import_hits(tree: ast.AST, filename: str = "<ast>") -> list[str]:
    """`from ..harness import …` / `import harness.harness` 的命中（循环依赖）。"""
    hits: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            module = node.module or ""
            # `from ..harness import X` / `from .harness import X`
            if module.split(".")[-1:] == ["harness"]:
                hits.append(f"{filename}:{node.lineno} from ...{module} import ...")
                continue
            # `from .. import harness`
            if any(alias.name.split(".")[0] == "harness" for alias in node.names):
                hits.append(f"{filename}:{node.lineno} from ... import harness")
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "harness" or alias.name.startswith("harness."):
                    hits.append(f"{filename}:{node.lineno} import {alias.name}")
    return hits


def _constructor_event_appends(tree: ast.AST, filename: str = "<ast>") -> list[str]:
    """构造器里的 `xxx.append(...)` —— 构造期**只接线，不产生事件**。

    构造期 `append` 会带来两个坑：
      ① 事件序号依赖构造顺序 → 恢复/对账的 tail 语义被污染；
      ② 「先造 Harness A、再造 B」时事件串台（A 的 log 多一条 B 的引导事件）。
    """
    hits: list[str] = []
    for cls in [n for n in ast.walk(tree) if isinstance(n, ast.ClassDef)]:
        for func in cls.body:
            if not isinstance(func, ast.FunctionDef) or func.name != "__init__":
                continue
            for node in ast.walk(func):
                if not isinstance(node, ast.Call):
                    continue
                fname = getattr(node.func, "attr", None) or getattr(node.func, "id", None)
                if fname == "append":
                    hits.append(f"{filename}:{node.lineno} {cls.name}.__init__ append(...)")
    return hits


# ===========================================================================
# 1) 体量守卫
# ===========================================================================
def test_service_method_size_limit():
    """任何 kernel 方法体量 > 150 行 → 红（`spawn_child` 是唯一豁免）。"""
    total = 0
    offenders: list[str] = []
    allow_hits: set[tuple[str, str]] = set()

    for path in _kernel_files():
        for func in _methods(_parse(path)):
            total += 1
            size = _span(func)
            if size <= _METHOD_SIZE_LIMIT:
                continue
            key = (path.name, func.name)
            if key in _METHOD_SIZE_ALLOWLIST:
                allow_hits.add(key)
                continue
            offenders.append(f"{path.name}::{func.name} ({size} lines)")

    assert not offenders, (
        f"以下方法体量超过 {_METHOD_SIZE_LIMIT} 行（继续拆，别让 kernel 变成新的 "
        f"god module）：\n" + "\n".join(offenders))

    # 豁免名单：① 不能增长（恰好一条）；② 不能「烂在名单里」（被豁免的方法
    # 若已拆分到 ≤150 行，本断言会红，逼着删掉 allowlist 条目）。
    assert _METHOD_SIZE_ALLOWLIST == {("children.py", "spawn_child")}, (
        f"体量 allowlist 只能有 `spawn_child`（S5 欠账），实际：{_METHOD_SIZE_ALLOWLIST}")
    assert allow_hits == _METHOD_SIZE_ALLOWLIST, (
        f"allowlist 与实际命中不一致（{allow_hits} vs {_METHOD_SIZE_ALLOWLIST}）"
        " —— 方法已缩小就该把条目删掉")

    # 自检：扫描面不能是空的
    assert total > 150, f"只扫到 {total} 个方法 —— 扫描器可能失效"


# ===========================================================================
# 2) 边界守卫：服务只能经公开接缝互调
# ===========================================================================
def test_no_private_cross_service_access():
    """服务之间不许 `self.deps.<svc>._x` / `self.<svc>._x`（组件私有缝除外）。"""
    hits: list[str] = []
    public_refs = 0
    for path in _kernel_files():
        tree = _parse(path)
        hits.extend(_private_service_accesses(tree, path.name))
        public_refs += len(_cross_service_accesses(tree))

    assert not hits, (
        "服务越界访问另一服务的私有成员（请改成公开接缝，或把方法提到对方公开面上）："
        "\n" + "\n".join(hits))
    assert public_refs > 0, "kernel 里一次服务间调用都没扫到 —— 扫描器失效"

    # ---- 负对照：合成 AST 必须被检出 ----
    probe = ast.parse("def f(self):\n    self.deps.tasks._finalize(x)\n")
    flagged = _private_service_accesses(probe, "<neg>")
    assert flagged, "`self.deps.tasks._finalize(x)` 未被检出 —— 边界检查不可信"

    probe2 = ast.parse("def f(self):\n    self.tasks._finalize(x)\n")
    assert _private_service_accesses(probe2, "<neg>"), "`self.tasks._finalize` 未被检出"

    # ---- 合法形不得误报：组件私有缝 + 公开接缝 + dunder ----
    probe3 = ast.parse(
        "def f(self):\n"
        "    self.deps.workspace._root\n"
        "    self.deps.tasks.requeue_readmit(x)\n"
        "    self.deps.receipts.__class__\n")
    assert not _private_service_accesses(probe3, "<neg>"), "把合法访问误判成越界"


# ===========================================================================
# 3) 冻结配置守卫
# ===========================================================================
def test_mutable_config_not_frozen_in_deps():
    """可热改配置不得出现在 `*Deps`（frozen dataclass）字段里。"""
    seen: list[tuple[str, str, str]] = []
    deps_classes = 0
    for path in _kernel_files():
        tree = _parse(path)
        deps_classes += len(_deps_dataclasses(tree))
        seen.extend(_frozen_config_violations(tree, path.name))

    assert deps_classes >= 6, f"只找到 {deps_classes} 个 *Deps dataclass —— 扫描器失效"

    bad = [v for v in seen if v not in _KNOWN_CONFIG_SNAPSHOTS]
    assert not bad, (
        "可热改配置被冻进 *Deps（frozen=True 后 `h.<cfg> = x` 静默失效）：\n"
        + "\n".join(f"{f}::{cls}.{name}" for f, cls, name in bad))

    # S3.5：ReceiptDeps.retention 已改为读 KernelState，欠账清零。
    # 名单必须保持为空 —— 任何新增都会让本断言与上面的 bad 列表同时失败。
    assert _KNOWN_CONFIG_SNAPSHOTS == set(), (
        f"冻结配置 allowlist 必须为空，实际：{_KNOWN_CONFIG_SNAPSHOTS}")

    # ---- 负对照：合成 dataclass 带 `lease_ms` 必须被检出 ----
    probe = ast.parse(
        "from dataclasses import dataclass\n"
        "@dataclass(frozen=True)\n"
        "class FakeDeps:\n"
        "    clock: object\n"
        "    lease_ms: int = 60_000\n")
    neg = _frozen_config_violations(probe, "<neg>")
    assert ("<neg>", "FakeDeps", "lease_ms") in neg, f"`lease_ms` 未被检出：{neg}"

    # 合法：稳定对象引用不报
    probe2 = ast.parse(
        "from dataclasses import dataclass\n"
        "@dataclass(frozen=True)\n"
        "class FakeDeps:\n"
        "    clock: object\n"
        "    retention_s: int = 10\n")
    assert not _frozen_config_violations(probe2, "<neg>"), "稳定引用被误判成配置快照"


# ===========================================================================
# 4) 公共面守卫：搬 160 个方法，一个都不能丢
# ===========================================================================
def test_harness_public_surface_preserved():
    """`Harness` 公开面（999b8c3 基线，45 个名字）必须仍然可达。"""
    assert len(_BASELINE_PUBLIC_SURFACE) == 45, (
        f"基线公开面应为 45 个名字，实际 {len(_BASELINE_PUBLIC_SURFACE)} —— "
        "基线快照被改动过（它是历史事实，不是可调参数）")

    h = Harness()
    missing = sorted(
        name for name in _BASELINE_PUBLIC_SURFACE
        if not _reachable(h, name))
    assert not missing, (
        "重构把 Harness 的公开方法/属性弄丢了（调用方会 AttributeError）："
        f"{missing}")


def _reachable(obj, name: str) -> bool:
    """`name` 在实例或类上都算可达；getter 依赖运行时状态也不判丢。"""
    if hasattr(type(obj), name):
        return True
    try:
        getattr(obj, name)
    except AttributeError:
        return False
    except Exception:            # 属性存在，只是 getter 需要更多运行时状态
        return True
    return True


# ===========================================================================
# 5) 无环：kernel 不得反向 import harness 模块
# ===========================================================================
def test_kernel_modules_do_not_import_harness_module():
    """kernel 服务必须能被独立构造 —— 不许 `from ..harness import …`。"""
    hits: list[str] = []
    scanned = 0
    for path in _kernel_files():
        scanned += 1
        hits.extend(_harness_import_hits(_parse(path), path.name))

    assert scanned >= 8, f"只扫了 {scanned} 个 kernel 模块"
    assert not hits, (
        "kernel 反向依赖 harness 模块（循环依赖，拆分变假象）：\n" + "\n".join(hits))

    # ---- 负对照：合成源码必须被检出 ----
    neg = ast.parse("from ..harness import Harness\n")
    assert _harness_import_hits(neg, "<neg>"), "`from ..harness import …` 未被检出"
    neg2 = ast.parse("import harness.harness\n")
    assert _harness_import_hits(neg2, "<neg>"), "`import harness.harness` 未被检出"
    neg3 = ast.parse("from .. import harness\n")
    assert _harness_import_hits(neg3, "<neg>"), "`from .. import harness` 未被检出"

    # 合法的相对 import（`from ..types import ACB`）不得误报
    ok = ast.parse("from ..types import ACB\nfrom .state import _NullLock\n")
    assert not _harness_import_hits(ok, "<neg>"), "把合法相对 import 误判成循环依赖"


# ===========================================================================
# 6) 构造期零事件
# ===========================================================================
def test_service_constructors_emit_no_events():
    """服务构造器只接线，不 append 事件；两个 Harness 的事件流互不串台。"""
    hits: list[str] = []
    inits = 0
    for path in _kernel_files():
        tree = _parse(path)
        inits += sum(1 for cls in ast.walk(tree) if isinstance(cls, ast.ClassDef)
                     for f in cls.body
                     if isinstance(f, ast.FunctionDef) and f.name == "__init__")
        hits.extend(_constructor_event_appends(tree, path.name))

    assert inits >= 6, f"只检查了 {inits} 个构造器 —— 扫描器失效"
    assert not hits, (
        "构造期就在写事件（事件序号绑定构造顺序，恢复/对账 tail 会漂）：\n"
        + "\n".join(hits))

    # 运行时佐证：构造 Harness 本身产生 0 条事件，且两个实例不共享 log
    h1 = Harness()
    before = len(h1.event_log.all())
    h2 = Harness()
    assert h2.event_log is not h1.event_log, "两个 Harness 共享了 event_log"
    assert len(h2.event_log.all()) == 0, (
        f"构造第二个 Harness 时写入了 {len(h2.event_log.all())} 条事件")
    assert len(h1.event_log.all()) == before, "构造第二个 Harness 污染了第一个的事件流"

    # ---- 负对照：合成构造器里的 append 必须被检出 ----
    neg = ast.parse(
        "class S:\n"
        "    def __init__(self, log):\n"
        "        self.log = log\n"
        "        self.log.append('bootstrap')\n")
    assert _constructor_event_appends(neg, "<neg>"), "构造器里的 append 未被检出"
