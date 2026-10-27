"""`bench/report.md` 与 `bench/result.json` 数值一致性（对抗评审清单 4）。

- **确定性指标**（`lost_update_rate` / `conflict_rate` / `wasted_work` /
  oracle 自检 / 判定）：必须与 `result.json` **逐值相等**；
- **时序指标**（`wall` / `speedup` / `serialization_rate` / `throughput`）：
  本质非确定（线程调度 + 计时抖动），用**容差**判定（相对 25%；
  `serialization_rate` 近零比值用绝对 0.05 + 量级 [0,1]）。

防止手抄漂移，同时不再对非确定指标做「逐值相等」的误判。
"""

from __future__ import annotations

import json
from pathlib import Path

from bench.report_check import check_report

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_report_matches_result_json():
    result = json.loads((REPO_ROOT / "bench" / "result.json").read_text(encoding="utf-8"))
    report = (REPO_ROOT / "bench" / "report.md").read_text(encoding="utf-8")
    outcome = check_report(result, report)
    assert outcome.errors == [], "\n".join(outcome.errors)
