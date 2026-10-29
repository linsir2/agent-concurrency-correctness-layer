#!/usr/bin/env python3
"""S24-P0：文档真值守卫（只检查可机读断言，不引入第二份真值源）。

用法：`python3 scripts/check_docs_consistency.py [--root DIR]`（exit 0/1）。

检查项：
1. `docs/milestones.md` 的 MS9/MS10 不得仍写「未开工」；
2. MS5 出口必须登记 `VerifierRunner`（S24-1 交付）；
3. `docs/positioning-and-roadmap.md` 的「我们」行：语义验证 = 🟡、worktree = ✅；
4. A.2 的 L4 行不得把 Trace 算作已交付；
5. 文档里引用的语料条数与 `manifest.json` 的 `totals.vectors` 一致。

**边界**：只覆盖上述可判定断言；历史章节/叙述性文字不检查（避免误伤）。
"""

from __future__ import annotations

import argparse
import json
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent


def _read(root: pathlib.Path, rel: str) -> str:
    return (root / rel).read_text(encoding="utf-8")


def check_docs(root: pathlib.Path | None = None) -> list[dict]:
    """返回 findings `[{file, message}]`；空列表 = 一致。"""
    root = pathlib.Path(root or ROOT)
    out: list[dict] = []

    milestones_rel = "docs/milestones.md"
    positioning_rel = "docs/positioning-and-roadmap.md"
    milestones = _read(root, milestones_rel)
    positioning = _read(root, positioning_rel)

    # 1) MS9/MS10 收口
    lines = milestones.splitlines()
    for label in ("MS9", "MS10"):
        rows = [ln for ln in lines if ln.startswith(f"| {label} |")]
        if not rows:
            out.append({"file": milestones_rel, "message": f"缺少 {label} 行"})
        elif any("未开工" in row for row in rows):
            out.append({"file": milestones_rel,
                        "message": f"{label} 仍标「未开工」，与 S16/S17 收口冲突"})

    # 2) MS5 出口登记 VerifierRunner
    section = re.search(r"### MS5[\s\S]*?(?=\n### |\Z)", milestones)
    if section is None or "VerifierRunner" not in section.group(0):
        out.append({"file": milestones_rel,
                    "message": "MS5 出口未登记 VerifierRunner（S24-1）"})

    # 3) 竞品矩阵「我们」行
    our_row = next((ln for ln in positioning.splitlines()
                    if ln.startswith("| **我们** |")), "")
    cells = [c.strip() for c in our_row.strip("|").split("|")] if our_row else []
    if len(cells) != 9:
        out.append({"file": positioning_rel,
                    "message": "竞品矩阵「我们」行格式变化（应为 9 列）"})
    else:
        if cells[5] != "**🟡**":
            out.append({"file": positioning_rel,
                        "message": "语义验证应标 🟡（静态改名检测 + Verifier，非全量等价）"})
        if cells[6] != "**✅**":
            out.append({"file": positioning_rel,
                        "message": "worktree 应标 ✅（WorktreeManager 已接线）"})

    # 4) L4 不得计入 Trace
    for ln in positioning.splitlines():
        if (ln.startswith("| L4 治理 |") and "Trace" in ln
                and "Trace 不交付" not in ln):
            out.append({"file": positioning_rel,
                        "message": "L4 不应把 Trace 计入交付（design §0.5：L5 不交付）"})

    # 5) 语料条数一致
    manifest = json.loads(_read(root, "harness/spec/conformance/manifest.json"))
    total = int(manifest["totals"]["vectors"])
    for rel in ("README.md", "docs/s24-gap-analysis-and-repositioning.md"):
        text = _read(root, rel)
        if f"{total} 条" not in text:
            out.append({"file": rel,
                        "message": f"未引用当前语料条数 {total}（文档可能已过期）"})
    # 6) README 不得写**易变**的测试计数（passed/failed/xfailed/skipped）
    #    计数随依赖（pysyncobj/sandlock/mcp）与平台变化，写进正文必然腐化；
    #    CI 报告是唯一来源。
    volatile = re.compile(r"\b\d+\s+(?:passed|failed|xfailed|skipped)\b")
    for ln in _read(root, "README.md").splitlines():
        if volatile.search(ln):
            out.append({"file": "README.md",
                        "message": f"易变测试计数应移除（CI 报告为唯一来源）：{ln.strip()}"})
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="check-docs", description="文档真值守卫")
    ap.add_argument("--root", default=None, help="仓库根目录（默认本仓库）")
    args = ap.parse_args(argv)
    findings = check_docs(pathlib.Path(args.root) if args.root else None)
    if not findings:
        print("docs ok: 0 findings")
        return 0
    print(f"docs: {len(findings)} findings")
    for item in findings:
        print(f"  {item['file']}: {item['message']}")
    return 1


if __name__ == "__main__":                    # pragma: no cover
    sys.exit(main())
