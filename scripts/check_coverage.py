#!/usr/bin/env python3
"""覆盖率分模块阈值门禁（文档 `s7-benchmark-and-coverage.md` v1.1 §4.1）。

用法：

    coverage run -m pytest
    coverage json -o coverage.json
    python scripts/check_coverage.py coverage.json

硬门禁（未达即 `exit 1`）：

    档  阈值   模块
    --- ----- ------------------------------------------------
    一  ≥85%   workspace.py / budget.py / intent_registry.py / event_log.py
    二  ≥75%   rebase.py / storage/wal.py / checkpoint.py
    三  ≥70%   runtime.py / orchestration.py

软提示（只告警，不影响退出码）：其余 `harness/` 顶层模块。
兜底：整体覆盖率须达 `fail_under = 80`（`pyproject.toml [tool.coverage.report]`）。

本脚本会**显式列出被排除的模块与理由**（避免阈值形同虚设），并断言
`bench/` 不在 coverage source 内（基准代码不计入产品覆盖率）。
"""

from __future__ import annotations

import json
import sys
import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_JSON = REPO_ROOT / "coverage.json"

TIERS: list[tuple[int, list[str]]] = [
    (85, ["workspace.py", "budget.py", "intent_registry.py", "event_log.py"]),
    (75, ["rebase.py", "storage/wal.py", "checkpoint.py"]),
    (70, ["runtime.py", "orchestration.py"]),
]
HARD_MODULES = {m for _, mods in TIERS for m in mods}

# 被排除（软提示）模块与理由 —— §4.1「其余」清单，显式打印避免形同虚设。
# 注意：storage/wal.py 已**提升**为硬门禁（档二），故 storage/* 仅指其余文件。
EXCLUDED_REASON: dict[str, str] = {
    "auth.py": "Signer 的 HMAC/jti 语义已随 spec 降级，扩展点",
    "resolver.py": "LLMResolver 网络分支无真实 LLM（阶段一 token_multiplier N/A）",
    "policy.py": "策略引擎以配置校验为主，归全局软提示",
    "context.py": "上下文装配为读路径，归全局软提示",
    "subscription.py": "订阅失效为读路径，归全局软提示",
    "compensation.py": "Saga 补偿入口，归全局软提示",
    "clock.py": "时钟为薄封装，归全局软提示",
    "types.py": "类型/枚举/状态机，归全局软提示",
    "llm.py": "真实网络分支已标 pragma: no cover（阶段一无真实 LLM）",
    "storage/atomic.py": "原子写底层，归全局软提示",
    "storage/blob.py": "blob 存储底层，归全局软提示",
    "storage/__init__.py": "包初始化，归全局软提示",
    "spec": "不变量/协议/回执规范，归全局软提示",
}


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _module_key(path: str) -> str:
    """`harness/storage/wal.py` / `/abs/.../harness/storage/wal.py` → `storage/wal.py`。

    统一按**路径分量**取 `harness/` 之后的部分（旧实现只匹配相对前缀，
    绝对路径会被原样返回 → 硬门禁模块「缺失」的假阴性）。
    """
    norm = path.replace("\\", "/")
    parts = [p for p in norm.split("/") if p not in ("", ".")]
    if "harness" in parts:
        idx = len(parts) - 1 - parts[::-1].index("harness")
        return "/".join(parts[idx + 1:])
    return norm


def _has_part(path: str, name: str) -> bool:
    """路径分量中是否含 `name`（绝对/相对路径皆可）。"""
    return name in [p for p in path.replace("\\", "/").split("/") if p not in ("", ".")]


def _percent(summary: dict) -> float:
    return float(summary.get("percent_covered", 0.0))


def _assert_bench_outside_source(data: dict) -> None:
    """断言 `bench/` 不在 coverage source 内。"""
    files = data.get("files", {})
    bench_files = [p for p in files if _has_part(p, "bench")]
    if bench_files:
        raise SystemExit(f"[ERROR] bench/ 不应计入 coverage source：{bench_files}")

    pyproject = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    source = (pyproject.get("tool", {}).get("coverage", {})
              .get("run", {}).get("source", []))
    bad = [s for s in source if "bench" in str(s).replace("\\", "/")]
    if bad:
        raise SystemExit(f"[ERROR] pyproject coverage source 含 bench/：{bad}")


def main(argv: list[str]) -> int:
    json_path = Path(argv[1]) if len(argv) > 1 else DEFAULT_JSON
    if not json_path.exists():
        print(f"[ERROR] 找不到 coverage json 报告：{json_path}")
        print("请先运行：coverage run -m pytest && coverage json -o coverage.json")
        return 2

    data = _load_json(json_path)
    _assert_bench_outside_source(data)

    files = data.get("files", {})
    by_module: dict[str, float] = {}
    for path, info in files.items():
        by_module[_module_key(path)] = _percent(info.get("summary", {}))
    totals = data.get("totals", {})
    overall = _percent(totals)

    print("=" * 72)
    print("S8 覆盖率门禁（s7-benchmark-and-coverage.md §4.1）")
    print("=" * 72)

    ok = True

    # ---- 硬门禁：三档阈值 ----
    for threshold, mods in TIERS:
        print(f"\n[硬门禁] ≥{threshold}%")
        for mod in mods:
            cov = by_module.get(mod)
            if cov is None:
                print(f"  FAIL  {mod:<24} 缺失（未出现在报告中）")
                ok = False
            elif cov < threshold:
                print(f"  FAIL  {mod:<24} {cov:6.2f}%  <  {threshold}%")
                ok = False
            else:
                print(f"  ok    {mod:<24} {cov:6.2f}%")

    # ---- 软提示：其余模块 ----
    print("\n[软提示] 其余模块（不阻塞门禁）")
    for mod, cov in sorted(by_module.items()):
        if mod in HARD_MODULES or mod.startswith("spec/"):
            continue
        reason = EXCLUDED_REASON.get(mod, "未纳入分档硬门禁")
        flag = "" if cov >= 85 else "  <- 低于 85%"
        print(f"  warn  {mod:<24} {cov:6.2f}%  （{reason}）{flag}")

    # ---- 排除清单（显式列出，避免阈值形同虚设） ----
    print("\n[排除清单] 模块与理由")
    for mod, reason in EXCLUDED_REASON.items():
        print(f"  - {mod:<22} {reason}")
    print("  - storage/wal.py 已提升为硬门禁（档二 ≥75%），不在此排除清单")

    # ---- 兜底：整体 fail_under ----
    pyproject = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    fail_under = (pyproject.get("tool", {}).get("coverage", {})
                  .get("report", {}).get("fail_under", 80))
    print(f"\n[兜底] 整体覆盖率 {overall:.2f}%  vs  fail_under={fail_under}")
    if overall < fail_under:
        print(f"  FAIL  整体未达 fail_under={fail_under}")
        ok = False
    else:
        print("  ok    整体达标")

    print("\n" + "=" * 72)
    if ok:
        print("覆盖率门禁：通过")
        return 0
    print("覆盖率门禁：未通过（硬门禁或 fail_under 未达）")
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
