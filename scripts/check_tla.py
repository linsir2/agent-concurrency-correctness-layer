#!/usr/bin/env python3
"""S24 差距清单第 5 项：TLA+ 模型实测（I1 / I2 / I4 / I7）。

每个模型跑两遍 TLC：
- 正确 cfg：期望 exit 0（不变量成立）；
- broken cfg：期望 exit 非 0，且输出含 `Invariant <名字> is violated.`。

用法：
    python3 scripts/check_tla.py                  # 从 TLA2TOOLS_JAR 取 jar
    python3 scripts/check_tla.py --jar /tmp/tla2tools.jar
    python3 scripts/check_tla.py --require        # 缺 Java/jar → exit 2（CI 门禁）
    python3 scripts/check_tla.py --only I2        # 只跑一个模型

缺 Java/jar 且未加 --require 时打印 SKIP 并 exit 0（不假装通过）。
纯标准库；TLC 由外部 tla2tools.jar 提供（仓库不引入运行时依赖）。
"""

from __future__ import annotations

import argparse
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile

ROOT = pathlib.Path(__file__).resolve().parent.parent
TLA_DIR = ROOT / "spec" / "tla"

# (编号, 模块名, 被验证的不变量)
CASES: tuple[tuple[str, str, str], ...] = (
    ("I1", "I1SingleWriter", "AtMostOneWriter"),
    ("I2", "I2NoLostUpdate", "NoLostUpdate"),
    ("I4", "I4Capability", "CapabilityClosed"),
    ("I7", "I7EffectExactlyOnce", "EffectAtMostOnce"),
)


def run_tlc(
    java: str,
    jar: str,
    tla_file: pathlib.Path,
    cfg_file: pathlib.Path,
    timeout: float = 60.0,
) -> tuple[int, str]:
    """跑一次 TLC，返回 (exit_code, stdout+stderr)。超时抛 TimeoutExpired。"""
    tla_file = pathlib.Path(tla_file)
    cfg_file = pathlib.Path(cfg_file)
    with tempfile.TemporaryDirectory(prefix="tlc-") as tmp:
        cmd = [
            java,
            "-XX:+UseParallelGC",
            "-cp",
            str(jar),
            "tlc2.TLC",
            "-deadlock",
            "-workers",
            "1",
            "-metadir",
            str(pathlib.Path(tmp) / "states"),
            "-config",
            str(cfg_file),
            str(tla_file),
        ]
        proc = subprocess.run(
            cmd, cwd=tmp, capture_output=True, text=True, timeout=timeout, check=False
        )
    return proc.returncode, f"{proc.stdout}\n{proc.stderr}"


def resolve_toolchain(jar_arg: str | None) -> tuple[str | None, str | None, list[str]]:
    """解析 Java 与 jar；返回 (java, jar, 缺失项说明)。"""
    java = shutil.which("java")
    jar = jar_arg or os.environ.get("TLA2TOOLS_JAR")
    problems: list[str] = []
    if java is None:
        problems.append("未找到 java")
    if not jar:
        problems.append("未提供 --jar / 未设置 TLA2TOOLS_JAR")
    elif not pathlib.Path(jar).is_file():
        problems.append(f"jar 不存在: {jar}")
    return java, jar, problems


def tail(text: str, n: int = 6) -> str:
    """取输出的最后 n 行非空内容（便于报错）。"""
    lines = [ln for ln in text.splitlines() if ln.strip()]
    return "\n".join(lines[-n:])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="对 spec/tla 的 4 个模型跑正确/ broken 两套 TLC 配置"
    )
    parser.add_argument("--jar", default=None, help="tla2tools.jar 路径")
    parser.add_argument(
        "--require",
        action="store_true",
        help="缺少 Java/jar 时退化为 exit 2（CI 门禁）；默认 SKIP + exit 0",
    )
    parser.add_argument("--timeout", type=float, default=60.0, help="单次 TLC 超时（秒）")
    parser.add_argument(
        "--only",
        action="append",
        choices=[case[0] for case in CASES],
        help="只跑指定编号（可重复）",
    )
    args = parser.parse_args(argv)

    java, jar, problems = resolve_toolchain(args.jar)
    if problems:
        detail = "；".join(problems)
        if args.require:
            print(f"FAIL: {detail}", file=sys.stderr)
            return 2
        print(f"SKIP: {detail}（未执行 TLC 实测）")
        return 0

    assert java is not None and jar is not None  # 由 problems 为空保证

    selected = [c for c in CASES if args.only is None or c[0] in args.only]
    failures = 0

    for key, module, invariant in selected:
        for broken in (False, True):
            cfg_name = f"{module}.broken.cfg" if broken else f"{module}.cfg"
            label = f"[{key}] {'broken' if broken else 'correct'} {cfg_name}"
            try:
                rc, out = run_tlc(
                    java,
                    jar,
                    TLA_DIR / f"{module}.tla",
                    TLA_DIR / cfg_name,
                    timeout=args.timeout,
                )
            except subprocess.TimeoutExpired:
                failures += 1
                print(f"{label}: FAIL 超时（>{args.timeout}s）")
                continue

            if broken:
                violated = invariant in out and "is violated" in out
                if rc != 0 and violated:
                    print(f"{label}: PASS exit={rc}（{invariant} 被违反，符合预期）")
                else:
                    failures += 1
                    print(f"{label}: FAIL exit={rc}，未检出 `{invariant}` 违反")
                    print(tail(out))
            elif rc == 0:
                print(f"{label}: PASS exit=0")
            else:
                failures += 1
                print(f"{label}: FAIL exit={rc}（正确模型不应失败）")
                print(tail(out))

    total = len(selected) * 2
    print(f"通过 {total - failures} / 失败 {failures}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
