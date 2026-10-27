"""S11 egress 测试的**环境 invalid 显式登记**（禁止静默 skip）。

约定（`docs/s11-egress-enforcement.md` v1.1 §7 / §9.3 P2）：

- 环境能力缺失时，用例走 `pytest.xfail(...)`（**不计 pass**），reason 由本模块
  `register()` 生成 → 报告里能看到「哪一项、为什么」；
- `pytest_terminal_summary`（`tests/conftest.py`）在会话结束时统一打印登记表；
- 任何地方**不得**使用 `pytest.skip` / `@pytest.mark.skip`（`test_s11_egress.py`
  有自检用例扫描源码）。
"""

from __future__ import annotations

import subprocess

INVALID: dict[str, str] = {}

FEATURE_LSM = "landlock-lsm"
FEATURE_NETNS = "netns"
FEATURE_UNSHARE = "unshare-netns"


def register(feature: str, reason: str) -> str:
    """登记一项环境 invalid，返回可直接喂给 `pytest.xfail` 的 reason 串。"""
    reason = (reason or "").strip() or "未给出原因"
    INVALID.setdefault(feature, reason)
    return f"环境 invalid[{feature}]：{reason}"


def snapshot() -> dict[str, str]:
    return dict(INVALID)


def reset() -> None:
    INVALID.clear()


def unshare_available() -> tuple[bool, str]:
    """`unshare -Urn` 是否可用（不可用 = 无 user/net namespace 权限）。"""
    try:
        proc = subprocess.run(["unshare", "-Urn", "true"],
                              capture_output=True, text=True, timeout=20, check=False)
    except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
        return False, f"unshare 不可执行：{exc}"
    if proc.returncode != 0:
        detail = (proc.stderr or "").strip() or f"exit={proc.returncode}"
        return False, detail
    return True, ""


def probe_environment() -> dict[str, str]:
    """探测三项环境能力并登记不可用原因；返回当前登记表快照。"""
    from harness.egress import lsm, netns, probe

    status = probe()
    if not status.lsm:
        _ok, reason = lsm.sandbox_available()
        register(FEATURE_LSM, reason)
    if not status.netns:
        _ok, reason = netns.netns_available()
        register(FEATURE_NETNS, reason)
    ok, reason = unshare_available()
    if not ok:
        register(FEATURE_UNSHARE, reason)
    return snapshot()


def report_lines() -> list[str]:
    """会话结束时的登记表（给 `pytest_terminal_summary` 用）。"""
    snap = snapshot()
    if not snap:
        return ["[S11 egress 环境登记] 无降级项（强制点可用）"]
    lines = ["[S11 egress 环境登记] 以下能力缺失已显式登记（对应用例 xfail，不计 pass）："]
    lines.extend(f"  - {feature:<16} {reason}" for feature, reason in sorted(snap.items()))
    return lines
