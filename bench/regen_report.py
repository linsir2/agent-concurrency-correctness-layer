"""从 `result.json` 重新生成 `report.md` 的**数值表**（S10 时序位移后保持同源）。

用法：
    python -m bench.regen_report [result.json] [report.md]

只替换 report.md 中标注为「机器生成」的表格块（`<!-- BEGIN:AUTO -->` …
`<!-- END:AUTO -->`），**保留全部叙述文字与测量局限**。这样：

* 时序指标（`wall` / `speedup` / `throughput` / `latency` / `serialization_rate`）
  每次重跑后刷新，不再与 `result.json` 脱节；
* 确定性指标（`lost` / `conflict` / `wasted` / 判定）**不在此列** —— 它们由
  `report_check.py` 逐值校验，必须稳定；
* 叙述章节（§1–§3、§5–§9）由人维护，脚本不碰。
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

BEGIN = "<!-- BEGIN:AUTO -->\n"
END = "<!-- END:AUTO -->\n"


def _mean(cell: dict) -> float:
    values = cell.get("values") or []
    return sum(values) / len(values) if values else 0.0


def _fmt(value: float, digits: int = 3) -> str:
    if abs(value) >= 100:
        return f"{value:.0f}"
    if abs(value) >= 1:
        return f"{value:.2f}"
    return f"{value:.{digits}f}"


def _speed_table(speed: dict) -> str:
    lines = ["| 负载 | B1 wall 均值 | B3 wall 均值 | speedup | 差值 CI（95%，n=30） |",
             "|---|---|---|---|---|"]
    for wl in ("C0", "C1", "C2", "C3"):
        s = speed[wl]
        ci = s.get("paired_bootstrap_ci") or {}
        lines.append("| {} | {} s | {} s | {} | [{}, {}] |".format(
            wl, _fmt(s["b1_wall_mean"]), _fmt(s["b3_wall_mean"]),
            _fmt(s["speedup_b1_over_b3"]),
            _fmt(ci.get("ci_low", 0.0)), _fmt(ci.get("ci_high", 0.0))))
    return "\n".join(lines)


def _throughput_table(summary: dict) -> str:
    lines = ["| 负载 | B1 | B2 | B3 |", "|---|---|---|---|"]
    for wl in ("C0", "C1", "C2", "C3"):
        row = summary[wl]
        lines.append("| {} | {} | {} | {} |".format(
            wl,
            _fmt(_mean(row["B1"]["throughput"])),
            _fmt(_mean(row["B2"]["throughput"])),
            _fmt(_mean(row["B3"]["throughput"])),
        ))
    return "\n".join(lines)


def _latency_table(summary: dict) -> str:
    lines = ["| 负载 | B1 p50 | B1 p99 | B3 p50 | B3 p99 |", "|---|---|---|---|---|"]
    for wl in ("C0", "C1", "C2", "C3"):
        row = summary[wl]
        lines.append("| {} | {} ms | {} ms | {} ms | {} ms |".format(
            wl,
            _fmt(_mean(row["B1"]["commit_latency_p50"]) * 1000),
            _fmt(_mean(row["B1"]["commit_latency_p99"]) * 1000),
            _fmt(_mean(row["B3"]["commit_latency_p50"]) * 1000),
            _fmt(_mean(row["B3"]["commit_latency_p99"]) * 1000),
        ))
    return "\n".join(lines)


def _scalability_table(secondary: dict) -> str:
    curve = secondary.get("scalability_curve") or {}
    points = curve.get("points") or curve.get("values") or []
    lines = ["| N | wall_time | speedup（相对 N=1） |", "|---|---|---|"]
    base = None
    for pt in points:
        n = pt.get("n")
        wall = pt.get("wall_ms") or pt.get("wall") or 0.0
        if base is None:
            base = wall if wall else 1.0
        lines.append("| {} | {} ms | {:.2f} |".format(n, _fmt(wall), (wall / base) if base else 0.0))
    return "\n".join(lines)


def _recovery_table(secondary: dict) -> str:
    rec = secondary.get("recovery_replay_cost") or {}
    phases = rec.get("phases") or {}
    rows = [
        ("`rebuild`（`effects` + `comp_journal`）", phases.get("rebuild")),
        ("`replay`（`_replay_events`）", phases.get("replay")),
        ("`invariant`（`check_invariants`）", phases.get("invariant")),
        ("**total**（`recover`）", rec.get("total_ms") or rec.get("total")),
    ]
    lines = ["| 相位 | 耗时 |", "|---|---|"]
    for name, value in rows:
        lines.append("| {} | {} ms |".format(name, _fmt(value or 0.0)))
    return "\n".join(lines)


def _event_replay_table(secondary: dict) -> str:
    ev = secondary.get("event_replay_cost") or {}
    points = ev.get("points") or []
    lines = ["| 事件数 | 耗时 | 斜率 |", "|---|---|---|"]
    prev = None
    for pt in points:
        n = pt.get("n_events") or pt.get("n")
        ms = pt.get("ms") or pt.get("wall_ms") or 0.0
        slope = "—"
        if prev:
            dn = (n or 0) - prev[0]
            slope = f"≈ {(ms - prev[1]) / dn * 1000:.1f} µs/event" if dn else "—"
        lines.append("| {}k | {} ms | {} |".format(
            (n or 0) // 1000 if n else "?", _fmt(ms), slope))
        prev = (n, ms)
    return "\n".join(lines)


def regen(result_path: Path, report_path: Path) -> None:
    data = json.loads(result_path.read_text(encoding="utf-8"))
    summary = data["summary"]
    secondary = data.get("secondary") or {}
    text = report_path.read_text(encoding="utf-8")

    blocks = {
        "speed": _speed_table(data["speed"]),
        "throughput": _throughput_table(summary),
        "latency": _latency_table(summary),
        "scalability": _scalability_table(secondary),
        "recovery": _recovery_table(secondary),
        "event_replay": _event_replay_table(secondary),
    }

    def _replace(match: re.Match) -> str:
        key = match.group(1)
        return f"{BEGIN}<!-- {key} -->\n{blocks.get(key, match.group(0))}\n{END}"

    text = re.sub(
        re.escape(BEGIN) + r"<!-- (\w+) -->\n.*?" + re.escape(END),
        _replace, text, flags=re.S,
    )
    report_path.write_text(text, encoding="utf-8")


def main(argv: list[str]) -> int:
    result = Path(argv[1]) if len(argv) > 1 else Path("bench/result.json")
    report = Path(argv[2]) if len(argv) > 2 else Path("bench/report.md")
    regen(result, report)
    print(f"已按 {result} 重新生成 {report} 的数值表")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
