"""S4：`recover` 的**阶段顺序**守卫。

为什么需要：
    黄金 WAL 哈希只覆盖提交路径；恢复步骤被重排不会被任何现有用例发现
    （13 步里有多步都会写事件/预算，顺序错了结果仍可能"看起来对"）。
    恢复正确性 = 顺序 + 每步的失败分支，因此把顺序本身钉死。

RIPR：
- Reachability：真解析 `RecoveryService.recover` 的 AST 并取源码顺序；
- Infection：重排任意两个阶段 / 删除一个阶段；
- Propagation：顺序断言失败；
- Revealability：打印实际序列与期望序列的差集。
"""

from __future__ import annotations

import ast
import pathlib

ROOT = pathlib.Path(__file__).resolve().parent.parent
SRC = ROOT / "harness/kernel/recovery.py"

# 恢复的 13 步（归一化调用名，见 _ops）。顺序即契约。
PHASES = [
    "ensure_writer_lease",          # ① 写者租约守卫
    "checkpoints.load_strict",      # ② 严格加载（双 checksum）
    "event_log.all",                # ③ 事件快照
    "receipts.rebuild",             # ④ 收据链重建
    "effects.rebuild_from",         # ⑤ effect journal 重建
    "comp_journal.rebuild_from",    # ⑤ 补偿 journal 重建
    "restore_epoch",                # ⑥ durable epoch 恢复
    "restore_acb",                  # ⑦ ACB 重建（generation+1 / 能力 / 预算 / 槽位）
    "replay_events",                # ⑧ 事件重放（对齐外部状态）
    "effects.probe_one",            # ⑨ effect 三态对账
    "comp_journal.resume",          # ⑩ 可选 COMPENSATE 终态策略
    "budget.check_invariants",      # ⑪ 全树不变量
    "register_admission",           # ⑫ 准入登记
    "children.reconcile_children_refs",     # ⑫ 子对账
    "rebuild_pause_state",          # ⑫ Admin 暂停态重建
    "tick",                         # ⑬ 恢复后驱动一次超时结算
]


def _ops(fn: ast.FunctionDef) -> list[str]:
    """按源码行号取归一化调用名：`self.deps.svc.m` → `svc.m`，`self.m` → `m`。"""
    out: list[tuple[int, str]] = []
    for node in ast.walk(fn):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        chain, cur = [], node.func
        while isinstance(cur, ast.Attribute):
            chain.append(cur.attr)
            cur = cur.value
        if not (isinstance(cur, ast.Name) and cur.id == "self"):
            continue
        chain.reverse()
        if chain and chain[0] == "deps":
            chain = chain[1:]
        out.append((node.lineno, ".".join(chain)))
    return [name for _, name in sorted(out)]


def _order_ok(ops: list[str]) -> bool:
    idx = []
    for phase in PHASES:
        if phase not in ops:
            return False
        idx.append(ops.index(phase))
    return idx == sorted(idx) and len(set(idx)) == len(idx)


def _recover_fn() -> ast.FunctionDef:
    tree = ast.parse(SRC.read_text(encoding="utf-8"))
    cls = next(n for n in tree.body
               if isinstance(n, ast.ClassDef) and n.name == "RecoveryService")
    return next(n for n in cls.body
                if isinstance(n, ast.FunctionDef) and n.name == "recover")


def test_recover_phase_order():
    ops = _ops(_recover_fn())
    assert _order_ok(ops), (
        "恢复阶段顺序漂移。\n期望: " + " → ".join(PHASES)
        + "\n实际: " + " → ".join(x for x in ops if x in PHASES))


def test_order_checker_negative_control():
    """负对照：交换/删除任意阶段必须被判错（否则上面的断言是空的）。"""
    ops = _ops(_recover_fn())
    assert _order_ok(ops)
    swapped = list(PHASES)
    swapped[3], swapped[4] = swapped[4], swapped[3]      # 相邻交换（完整序列）
    assert not _order_ok(swapped), "相邻阶段交换未被发现"
    assert not _order_ok(PHASES[:-1]), "缺阶段未被发现"
    assert not _order_ok([]), "空序列未被发现"


def test_recover_paths_are_unchanged():
    """错误分支的产物不得改名（恢复失败时下游依赖这些字符串）。"""
    src = SRC.read_text(encoding="utf-8")
    for token in ("WRITER_LEASE_HELD", "CHECKPOINT_CORRUPT",
                  "NEEDS_RECONCILIATION", "COMPENSATE_FAILED",
                  "COMPENSATE_PIVOT", "INVARIANT_BROKEN"):
        assert token in src, f"恢复失败语义缺失：{token}"
