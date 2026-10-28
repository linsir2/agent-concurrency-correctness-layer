"""S12：容器运行时适配（gVisor `runsc` + OCI bundle）。

**为什么单独一个模块（且叫 `container.py` 而非 `runtime.py`）**：
`harness/runtime.py` 是**进程内 agent 运行时**（表驱动 action 循环），与
「把子进程放进隔离容器」是两件事；同名会让读者以为是一回事（P2 修订）。

**设计**：

- 直接调 `runsc`（**不依赖 Docker daemon**）—— 外部部署里 Docker 守护进程是
  额外的故障面与权限面，`runsc run --bundle <dir>` 已足够；
- 无 `runsc` → `SandboxUnavailable`（**专用异常**，非 `EgressUnavailable`）
  + **显式登记降级**（不允许静默）；
- 缺 `bundle` / `bundle` 无 `config.json` → `SandboxUnavailable`（fail-closed）。

**诚实边界（K-60）**：gVisor 是**用户态内核**，**不支持 Landlock**（官方测试
`SKIP_IF(IsRunningOnGvisor())`，`landlock_create_ruleset` 返回 ENOSYS）。
在 gVisor 内不能依赖 LSM 强制出站，须用 netns / 防火墙兜底。
"""

from __future__ import annotations

import os
import shutil
import subprocess
from collections.abc import Sequence

_DEGRADATIONS: list[tuple[str, str]] = []


def register_degradation(backend: str, reason: str) -> None:
    """显式登记一项后端降级（不静默）。"""
    _DEGRADATIONS.append((backend, (reason or "").strip() or "未给出原因"))


def degradations() -> list[tuple[str, str]]:
    """已登记的后端降级（空列表 = 无降级）。"""
    return list(_DEGRADATIONS)


def runsc_path() -> str | None:
    return shutil.which("runsc")


def gvisor_available() -> tuple[bool, str]:
    """`(runsc 可用, 不可用原因)`；原因串用于**显式登记**。"""
    if runsc_path() is None:
        return False, "runsc 不可用（gVisor 未安装）→ 无法用容器后端隔离"
    return True, ""


def spawn_in_gvisor(argv: Sequence[str], bundle: str,
                    *, runsc: str | None = None) -> subprocess.Popen:
    """在 gVisor（`runsc`）+ OCI bundle 里启动 `argv`，返回 `Popen`。

    - 无 `runsc` → `SandboxUnavailable`（**专用异常**，非 `EgressUnavailable`）
      + 显式登记降级；
    - `bundle` 缺失或无 `config.json` → `SandboxUnavailable`（fail-closed，
      绝不裸跑）。
    """
    from . import SandboxUnavailable

    exe = runsc or runsc_path()
    if exe is None:
        reason = "runsc 不可用（gVisor 未安装）→ 无法用容器后端隔离"
        register_degradation("gvisor", reason)
        raise SandboxUnavailable(reason)
    if not bundle or not os.path.isfile(os.path.join(str(bundle), "config.json")):
        reason = (f"OCI bundle 无效（{bundle!r} 缺少 config.json）"
                  f" → 拒绝裸跑（fail-closed）")
        register_degradation("gvisor", reason)
        raise SandboxUnavailable(reason)
    return subprocess.Popen([exe, "run", "--bundle", str(bundle),
                             "harness-egress", *argv])


__all__ = [
    "degradations",
    "gvisor_available",
    "register_degradation",
    "runsc_path",
    "spawn_in_gvisor",
]
