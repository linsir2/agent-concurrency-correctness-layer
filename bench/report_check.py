"""`report.md` ↔ `result.json` 数值一致性校验（S7 §2.6/§9.3）。

目的：报告里的关键数字必须与 `result.json` **同源**，禁止手抄漂移
（旧报告 §4.5 用单次 N=1 最冷基线得出 speedup<1，被自家 result.json 证伪）。

**指标分两类**（时序与线程调度本质非确定，不能用逐值相等判定）：

- **确定性（严格逐值相等）**：`lost_update_rate`、`conflict_rate_detected`、
  `conflict_rate_silent`、`wasted_work`、oracle 自检、C0 sanity、主终点 verdict；
- **容差**：`wall_time` / `speedup` / `throughput` / `commit_latency`（相对 25%）、
  `serialization_rate`（近零比值，用绝对容差 + 量级合理性）。

容差判定打印「容差内」；**超出容差仍判不一致**（不无脑放过）。

用法::

    python -m bench.report_check bench/result.json bench/report.md

测试 `tests/test_bench_report_consistency.py` 直接调用 `check_report`。
"""

from __future__ import annotations

import ast
import json
import math
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

# ---------------------------------------------------------------- 指标分类
# 确定性（严格逐值相等）：lost_update_rate / conflict_rate_detected /
#   conflict_rate_silent / wasted_work / oracle 自检 / C0 sanity / 主终点 verdict
# 容差（非确定）：wall/speedup/throughput/latency（相对）、serialization_rate（绝对+量级）
REL_TOL = 0.25          # 时序指标相对容差（wall/speedup/throughput/latency）
SER_ABS_TOL = 0.05      # serialization_rate 绝对容差（近零比值，相对误差无意义）
SER_MAX = 1.0           # serialization_rate 量级上界（= lock_wait / wall ≤ 1）


# ---------------------------------------------------------------- 解析工具
def _table_after(text: str, heading_prefix: str) -> list[list[str]]:
    """取某标题之后的第一张 Markdown 表（跳过表头分隔行）。"""
    lines = text.splitlines()
    start = None
    for i, line in enumerate(lines):
        if line.startswith(heading_prefix):
            start = i
            break
    if start is None:
        raise ValueError(f"报告缺少章节：{heading_prefix}")
    rows: list[list[str]] = []
    in_table = False
    for line in lines[start + 1:]:
        s = line.strip()
        if s.startswith("|"):
            in_table = True
            cells = [c.strip() for c in s.strip("|").split("|")]
            if all(set(c) <= {"-", ":", " "} for c in cells):
                continue
            rows.append(cells)
            continue
        if in_table:
            break
        if s.startswith("#"):
            break
    return rows


def _plain(cell: str) -> str:
    return cell.replace("**", "").replace("`", "").strip()


_NUM_RE = re.compile(r"[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?")


def _num(cell: str) -> float | None:
    """从表格单元格解析数值（容忍单位后缀与 markdown 标记）。"""
    m = _NUM_RE.search(_plain(cell))
    if not m:
        return None
    try:
        v = float(m.group(0))
    except ValueError:
        return None
    return v if math.isfinite(v) else None


# ---------------------------------------------------------------- 校验结果
@dataclass
class CheckOutcome:
    """校验结果：`errors` 为空 = 一致。"""

    errors: list[str] = field(default_factory=list)      # 不一致（含超容差）
    strict_ok: int = 0                                   # 确定性一致项数
    tol_ok: list[str] = field(default_factory=list)      # 「容差内」条目
    tol_fail: list[str] = field(default_factory=list)    # 超容差条目（⊆ errors）

    @property
    def ok(self) -> bool:
        return not self.errors


# ---------------------------------------------------------------- 判定原语
def _expect_strict(out: CheckOutcome, where: str, got: str, want: str) -> None:
    """确定性指标：逐值相等。"""
    if _plain(got) != want:
        out.errors.append(f"{where}: report={_plain(got)!r} result.json={want!r}")
    else:
        out.strict_ok += 1


def _expect_rel(out: CheckOutcome, where: str, got_cell: str, want: float,
                rel: float = REL_TOL) -> None:
    """时序指标：相对容差。"""
    got = _num(got_cell)
    if got is None:
        out.errors.append(f"{where}: report={_plain(got_cell)!r} 无法解析为数值 "
                          f"（期望 {want:.4g}）")
        out.tol_fail.append(where)
        return
    denom = abs(want) if want != 0.0 else 1.0
    rel_err = abs(got - want) / denom
    if rel_err <= rel:
        out.tol_ok.append(f"{where}: report={got:.4g} result={want:.4g} "
                          f"(rel {rel_err:.1%} ≤ {rel:.0%}) 容差内")
    else:
        out.errors.append(f"{where}: report={got:.4g} result={want:.4g} "
                          f"(rel {rel_err:.1%} > {rel:.0%}) 超出容差")
        out.tol_fail.append(where)


def _expect_ratio(out: CheckOutcome, where: str, got_cell: str, want: float,
                  abs_tol: float = SER_ABS_TOL, hi: float = SER_MAX) -> None:
    """近零比值（serialization_rate）：量级合理性 + 绝对容差。"""
    got = _num(got_cell)
    if got is None:
        out.errors.append(f"{where}: report={_plain(got_cell)!r} 无法解析为数值 "
                          f"（期望 {want:.4g}）")
        out.tol_fail.append(where)
        return
    if not (0.0 <= got <= hi and 0.0 <= want <= hi):
        out.errors.append(f"{where}: 量级不合理 report={got:.4g} result={want:.4g} "
                          f"（应在 [0, {hi:g}]）")
        out.tol_fail.append(where)
        return
    if abs(got - want) <= abs_tol:
        out.tol_ok.append(f"{where}: report={got:.4g} result={want:.4g} "
                          f"(abs {abs(got - want):.4g} ≤ {abs_tol:.2f}, 量级合理) 容差内")
    else:
        out.errors.append(f"{where}: report={got:.4g} result={want:.4g} "
                          f"(abs {abs(got - want):.4g} > {abs_tol:.2f}) 超出容差")
        out.tol_fail.append(where)


def _check_oracle(out: CheckOutcome, result: dict, report_text: str) -> None:
    """oracle 自检（确定性）：报告里的自检结果必须与 result.json 一致。"""
    line = next((ln.strip() for ln in report_text.splitlines()
                 if ln.strip().startswith("全部负载 oracle 自检")), None)
    if line is None:
        out.errors.append("oracle 自检: 报告缺少「全部负载 oracle 自检」行")
        return
    rhs = line.partition(":")[2].strip()
    try:
        got = ast.literal_eval(rhs)
    except (ValueError, SyntaxError):
        out.errors.append(f"oracle 自检: 报告值无法解析：{rhs!r}")
        return
    want = result.get("oracle_self_check", {})
    if dict(got) != dict(want):
        out.errors.append(f"oracle 自检: report={got} result.json={want}")
    else:
        out.strict_ok += 1


# ---------------------------------------------------------------- 主校验
def check_report(result: dict, report_text: str) -> CheckOutcome:
    """返回校验结果（`errors` 为空 = 一致）。"""
    out = CheckOutcome()
    summary = result["summary"]

    # ---- §3 五项主指标（确定性逐值 + serialization_rate 容差）----
    rows = _table_after(report_text, "## 3. ")
    body = rows[1:]
    if len(body) != len(summary) * 3:
        out.errors.append(f"§3 表行数={len(body)}，期望 {len(summary) * 3}")
    for cells in body:
        wl, cond = _plain(cells[0]), _plain(cells[1])
        if wl not in summary or cond not in summary[wl]:
            out.errors.append(f"§3 未知行：{cells[:2]}")
            continue
        s = summary[wl][cond]
        _expect_strict(out, f"§3 {wl}/{cond}/lost_update_rate", cells[2],
                       f"{s['lost_update_rate']['mean']:.3f}")
        _expect_strict(out, f"§3 {wl}/{cond}/conflict_rate_detected", cells[3],
                       f"{s['conflict_rate_detected']['mean']:.3f}")
        _expect_strict(out, f"§3 {wl}/{cond}/conflict_rate_silent", cells[4],
                       f"{s['conflict_rate_silent']['mean']:.3f}")
        _expect_ratio(out, f"§3 {wl}/{cond}/serialization_rate", cells[5],
                      s["serialization_rate"]["mean"])
        _expect_strict(out, f"§3 {wl}/{cond}/wasted_work", cells[6],
                       f"{s['wasted_work']['mean']:.3f}")

    # ---- §3.3 speedup（时序容差）----
    rows = _table_after(report_text, "### 3.3")
    for cells in rows[1:]:
        wl = _plain(cells[0])
        sp = result["speed"][wl]
        _expect_rel(out, f"§3.3 {wl}/b1", cells[1], sp["b1_wall_mean"])
        _expect_rel(out, f"§3.3 {wl}/b3", cells[2], sp["b3_wall_mean"])
        _expect_rel(out, f"§3.3 {wl}/speedup", cells[3],
                    sp["speedup_b1_over_b3"])

    # ---- §4.1 throughput（时序容差）----
    rows = _table_after(report_text, "### 4.1")
    for cells in rows[1:]:
        wl = _plain(cells[0])
        for idx, cond in ((1, "B1"), (2, "B2"), (3, "B3")):
            _expect_rel(out, f"§4.1 {wl}/{cond}", cells[idx],
                        summary[wl][cond]["throughput"]["mean"])

    # ---- §4.5 scalability（时序容差）----
    rows = _table_after(report_text, "### 4.5")
    by_n = result["secondary"]["scalability_curve"]["by_n"]
    for cells in rows[1:]:
        n = _plain(cells[0])
        _expect_rel(out, f"§4.5 N={n}/wall", cells[1], by_n[n]["wall_time"] * 1000)
        _expect_rel(out, f"§4.5 N={n}/speedup", cells[2], by_n[n]["speedup"])

    # ---- §5 预注册判定（确定性）----
    rows = _table_after(report_text, "## 5. ")
    for cells in rows[1:]:
        wl = _plain(cells[0])
        j = result["judgments"][wl]
        _expect_strict(out, f"§5 {wl}/b2", cells[2], f"{j['b2_lost']:.3f}")
        _expect_strict(out, f"§5 {wl}/b3", cells[3], f"{j['b3_lost']:.3f}")
        want_diff = "—" if wl == "C0" else f"{j['diff_b3_minus_b2']:.3f}"
        _expect_strict(out, f"§5 {wl}/diff", cells[4], want_diff)
        if wl == "C0":
            want = "通过" if j["pass"] else "未通过"
        else:
            want = "成立" if j["verdict"] else "不成立"
        if want not in _plain(cells[5]):
            out.errors.append(f"§5 {wl}/verdict: report={_plain(cells[5])!r} 期望含 {want!r}")
        else:
            out.strict_ok += 1

    # ---- oracle 自检（确定性）----
    _check_oracle(out, result, report_text)

    return out


def main(argv: list[str]) -> int:
    result_path = Path(argv[1]) if len(argv) > 1 else Path("bench/result.json")
    report_path = Path(argv[2]) if len(argv) > 2 else Path("bench/report.md")
    result = json.loads(result_path.read_text(encoding="utf-8"))
    text = report_path.read_text(encoding="utf-8")
    out = check_report(result, text)

    print(f"确定性指标一致 ✅（{out.strict_ok} 项）")
    if out.tol_ok:
        print(f"时序指标容差内 ✅（{len(out.tol_ok)} 项）")
        for item in out.tol_ok:
            print(f"  - {item}")
    if out.errors:
        print("report.md 与 result.json 不一致：")
        for e in out.errors:
            print("  -", e)
        if out.tol_fail:
            print(f"时序指标超容差 ❌（{len(out.tol_fail)} 项）")
        return 1
    print("report.md 与 result.json 数值一致（确定性逐值相等，时序容差内）")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
