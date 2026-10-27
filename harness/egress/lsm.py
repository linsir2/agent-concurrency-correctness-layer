"""S11d：Landlock 强制（真实外部库 `sandlock`，**子进程**）。

硬约束（`docs/s11-egress-enforcement.md` v1.1 §5 / P0-2 / K-50）：

1. **不自研 seccomp/Landlock**：只调用外部库 `sandlock`（Apache-2.0）；
2. **不用进程内 `confine()`**：Landlock 一旦 `apply` **不可逆**，写错即锁死主进程
   → 只用**子进程** `sandbox.run(argv)`（子进程可重启）；
3. **fail-closed**：库缺失 / 内核 ABI < 6 → `EgressUnavailable`（拒绝服务，
   **不静默放行**）；
4. **UDP 需 ABI v10（内核 6.15+）**：`net_allow` 里的 `udp://:53` 在低 ABI 上
   不被强制 → DNS 口径不可控（K-50），由 `dns_enforceable()` 显式暴露。

真实 API（不是编造的）：`Sandbox(fs_readable=..., net_allow=..., http_allow=...)`
+ `sandbox.run([...])`。
"""

from __future__ import annotations

import ctypes
import os
import platform
import subprocess  # noqa: F401  （保留：沙箱子进程语义的文档锚点）
from collections.abc import Iterable, Sequence

try:                                    # pragma: no cover - 取决于环境是否装了库
    from sandlock import Sandbox  # type: ignore[import-not-found]
    _HAS_SANDLOCK = True
except ImportError:                     # pragma: no cover - 取决于环境是否装了库
    Sandbox = None                      # type: ignore[assignment]
    _HAS_SANDLOCK = False

LANDLOCK_ABI_MIN = 6                    # sandlock 要求 6.12+ / ABI v6
LANDLOCK_ABI_UDP = 10                   # UDP 强制需 ABI v10（内核 6.15+）
KERNEL_MIN = (6, 12)

_LANDLOCK_CREATE_RULESET_VERSION = 1
# landlock_create_ruleset 在 x86_64 / arm64 的通用编号均为 444
_SYSCALL_NR = {"x86_64": 444, "amd64": 444, "aarch64": 444, "arm64": 444}

DEFAULT_FS_READABLE: tuple[str, ...] = ("/usr", "/etc", "/workspace")
DEFAULT_DNS_ALLOW = "udp://:53"


def landlock_abi_version() -> int:
    """探测内核 Landlock ABI 版本；不可用（ENOSYS/EPERM/非 Linux）→ 0（不抛）。"""
    if not hasattr(os, "uname") or os.uname().sysname != "Linux":
        return 0
    nr = _SYSCALL_NR.get(platform.machine().lower())
    if nr is None:                      # pragma: no cover - 非 x86_64/arm64 架构
        return 0
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        rc = libc.syscall(ctypes.c_long(nr), None,
                          ctypes.c_size_t(0),
                          ctypes.c_uint32(_LANDLOCK_CREATE_RULESET_VERSION))
    except (OSError, AttributeError):   # pragma: no cover - 平台无 libc.syscall
        return 0
    return max(0, int(rc))


def kernel_release() -> str:
    try:
        return os.uname().release
    except AttributeError:              # pragma: no cover - 非 POSIX
        return "unknown"


def kernel_version() -> tuple[int, int]:
    """`/proc/version` 之外的第二口径：`uname -r` 的主/次版本（解析失败 → (0, 0)）。"""
    parts = kernel_release().split("-")[0].split(".")
    try:
        return (int(parts[0]), int(parts[1]))
    except (IndexError, ValueError):     # pragma: no cover - 非常规 release 串
        return (0, 0)


def sandbox_available(abi_probe=None) -> tuple[bool, str]:
    """返回 `(可用, 不可用原因)`；原因串用于**显式登记**（不允许静默降级）。"""
    if not _HAS_SANDLOCK:
        return False, "sandlock 未安装（pip install sandlock 不可用 → fail-closed）"
    probe = abi_probe or landlock_abi_version
    abi = probe()
    if abi < LANDLOCK_ABI_MIN:
        return False, (f"Landlock ABI={abi} < {LANDLOCK_ABI_MIN}"
                       f"（内核 {kernel_release()}）→ fail-closed")
    return True, ""


def dns_enforceable(abi_probe=None) -> bool:
    """DNS（UDP:53）是否可被内核强制：需 ABI >= v10（内核 6.15+，K-50）。"""
    probe = abi_probe or landlock_abi_version
    return probe() >= LANDLOCK_ABI_UDP


def confine(*_args, **_kwargs):        # pragma: no cover - 永远拒绝，仅作反模式闸门
    """**禁止**进程内 `confine()`：Landlock 不可逆，写错即锁死主进程（P0-2）。"""
    from . import EgressUnavailable

    raise EgressUnavailable(
        "进程内 confine() 被禁止（Landlock 不可逆，会锁死主进程）→ 用 run_sandboxed 子进程")


def build_sandbox(allow_hosts: Iterable[str], *,
                  fs_readable: Sequence[str] | None = None,
                  http_allow: Sequence[str] | None = None,
                  dns_allow: bool = True, abi: int | None = None):
    """按真实 API 构造 `Sandbox`：`net_allow = [f"{h}:443", ...] (+ ["udp://:53"])`。

    终审 4-3：`udp://:53` **只在 ABI ≥ v10（DNS 可真强制）时才加入**。
    ABI 6–9 上该规则不被内核强制 → 加了也只是假象（DNS 可指向任意 resolver）；
    此时不加入，DNS 被完全阻断（fail-closed，而不是 fail-open）。
    """
    hosts = sorted({str(h).strip() for h in allow_hosts if str(h).strip()})
    net_allow = [f"{h}:443" for h in hosts]
    if dns_allow and (abi is None or abi >= LANDLOCK_ABI_UDP):
        net_allow.append(DEFAULT_DNS_ALLOW)
    return Sandbox(                              # type: ignore[misc]
        fs_readable=list(fs_readable or DEFAULT_FS_READABLE),
        net_allow=net_allow,
        http_allow=list(http_allow or []),
    )


def run_sandboxed(argv: Sequence[str], allow_hosts: Iterable[str] = (), *,
                  fs_readable: Sequence[str] | None = None,
                  http_allow: Sequence[str] | None = None,
                  dns_allow: bool = True,
                  abi_probe=None) -> int:
    """在 **Landlock 子进程**里跑 `argv`，返回子进程退出码。

    - 无 `sandlock` 或 ABI < 6 → `EgressUnavailable`（**拒绝服务，不静默放行**）；
    - 网络白名单：`{host}:443`（逐 host）+ `udp://:53`（仅 ABI ≥ v10 时）；
    - 只走 `sandbox.run(argv)`：子进程，可重启，主进程不被锁死。
    """
    from . import EgressUnavailable

    ok, reason = sandbox_available(abi_probe)
    if not ok:
        raise EgressUnavailable(reason)
    abi = (abi_probe or landlock_abi_version)()
    sandbox = build_sandbox(allow_hosts, fs_readable=fs_readable,
                            http_allow=http_allow, dns_allow=dns_allow, abi=abi)
    return int(sandbox.run(list(argv)))


__all__ = [
    "DEFAULT_DNS_ALLOW",
    "DEFAULT_FS_READABLE",
    "KERNEL_MIN",
    "LANDLOCK_ABI_MIN",
    "LANDLOCK_ABI_UDP",
    "build_sandbox",
    "confine",
    "dns_enforceable",
    "kernel_release",
    "kernel_version",
    "landlock_abi_version",
    "run_sandboxed",
    "sandbox_available",
]
