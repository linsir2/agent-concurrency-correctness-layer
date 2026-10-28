"""S15 C4：**契约反漂移**静态检查（`docs/s15-protocol-and-production.md` §2.4）。

为什么需要它：
    `harness/protocols.py` 的 9 个 `Protocol` 从未被任何测试引用 → 已严重漂移
    （`CheckpointStore` 契约只声明 `save`/`load`，而 Harness 实际调用 11 个）。
    照旧契约实现一个 Raft 后端（MS9 目标）会在**运行时**崩。

本检查**反向驱动**契约补全：
    用 `ast` 扫 `harness/**.py` 里所有 `self.<组件>.<成员>` 与
    `self.h.<组件>.<成员>` 的**读取/调用**（排除赋值 —— 那是 Harness 注入端口，
    不是被调用面），要求它们全部在对应 `Protocol` 中声明。

比「参数化跑用例」更强：新增代码若访问了未声明的成员，**立刻红**。
"""

from __future__ import annotations

import ast
import pathlib

from harness import protocols

_ROOT = pathlib.Path(__file__).resolve().parent.parent
_HARNESS_DIR = _ROOT / "harness"

# 组件属性名 → 对应契约
_COMPONENTS: dict[str, str] = {
    "checkpoints": "CheckpointStore",
    "event_log": "EventLog",
    "budget": "BudgetManager",
    "intents": "IntentRegistry",
    "workspace": "Workspace",
    "policy": "PolicyEngine",
    "effects": "EffectJournal",
    "clock": "Clock",
}


def _declared(proto_name: str) -> set[str]:
    """该 Protocol **自身**声明的成员（方法 + 属性注解，不含 dunder）。"""
    proto = getattr(protocols, proto_name)
    names = {k for k in vars(proto) if not k.startswith("__")}
    # 属性型成员（`root: Any`）只出现在 __annotations__，不在 __dict__
    names |= {k for k in getattr(proto, "__annotations__", {}) if not k.startswith("__")}
    return names


def _holder_and_comp(node: ast.AST) -> str | None:
    """若 `node` 形如 `self.<comp>` 或 `self.h.<comp>`，返回 comp。"""
    if not isinstance(node, ast.Attribute):
        return None
    # self.<comp>
    if isinstance(node.value, ast.Name) and node.value.id == "self":
        return node.attr
    # self.h.<comp>
    if (isinstance(node.value, ast.Attribute)
            and isinstance(node.value.value, ast.Name)
            and node.value.value.id == "self"
            and node.value.attr == "h"):
        return node.attr
    return None


def _collect_accessed() -> dict[str, dict[str, list[tuple[str, int]]]]:
    """扫出 `self[.h].<comp>.<member>` 的读取/调用（排除赋值/删除）。

    返回 `{comp: {member: [(file, lineno), ...]}}`。
    """
    found: dict[str, dict[str, list[tuple[str, int]]]] = {
        comp: {} for comp in _COMPONENTS
    }
    for path in sorted(_HARNESS_DIR.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        rel = str(path.relative_to(_ROOT))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Attribute):
                continue
            # 只算「读取/调用」：赋值目标(Store)/删除(Del) 是 Harness 注入端口
            if not isinstance(node.ctx, ast.Load):
                continue
            comp = _holder_and_comp(node.value)
            if comp is None or comp not in _COMPONENTS:
                continue
            found[comp].setdefault(node.attr, []).append((rel, node.lineno))
    return found


def test_no_contract_drift():
    """**C-4 主断言**：Harness 访问的每个组件成员都必须在契约里声明。"""
    accessed = _collect_accessed()
    problems: list[str] = []
    for comp, proto_name in _COMPONENTS.items():
        declared = _declared(proto_name)
        missing = sorted(set(accessed[comp]) - declared)
        if missing:
            detail = "; ".join(
                f"{m} @ {accessed[comp][m][0][0]}:{accessed[comp][m][0][1]}"
                for m in missing)
            problems.append(f"{comp} 契约漏声明 {missing}（{detail}）")
    assert not problems, "契约漂移：\n" + "\n".join(problems)


def test_drift_check_is_not_vacuous():
    """**自检**：确认扫描真的扫到了东西（否则上面的绿是空的）。"""
    accessed = _collect_accessed()
    total = sum(len(v) for v in accessed.values())
    assert total > 30, f"扫描面过小（{total}），检查可能失效"
    for comp in _COMPONENTS:
        assert accessed[comp], f"{comp} 一次都没被访问 → 组件名可能写错"


def test_negative_control_removing_declaration_is_detected():
    """**负对照**：人为删掉一个已声明成员 → 检查必须能发现。"""
    accessed = _collect_accessed()
    declared = _declared("CheckpointStore")
    sample = next(iter(set(accessed["checkpoints"]) & declared), None)
    assert sample is not None, "找不到可用的样本成员"

    reduced = declared - {sample}
    missing = sorted(set(accessed["checkpoints"]) - reduced)
    assert sample in missing, (
        f"删掉 {sample} 后未被检出 —— 负对照失效（检查不可信）")


def test_protocols_are_runtime_checkable():
    """C-2：契约标了 `@runtime_checkable`，可做结构冒烟。"""
    from harness.checkpoint import CheckpointStore as MemStore
    from harness.event_log import EventLog as MemLog

    assert getattr(protocols.CheckpointStore, "_is_runtime_protocol", False), (
        "CheckpointStore 未标 @runtime_checkable")
    assert isinstance(MemStore(), protocols.CheckpointStore)
    assert isinstance(MemLog(), protocols.EventLog)


def test_real_backends_satisfy_checkpoint_protocol(tmp_path):
    """C-2：**两套后端**都满足 `CheckpointStore` 契约（结构层）。"""
    from harness.checkpoint import CheckpointStore as MemStore
    from harness.checkpoint import FileCheckpointStore

    proto = protocols.CheckpointStore
    assert isinstance(MemStore(), proto)
    assert isinstance(FileCheckpointStore(str(tmp_path)), proto)
