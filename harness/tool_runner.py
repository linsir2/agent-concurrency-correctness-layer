"""ToolRunner —— 库内唯一「执行外部命令」的抽象（#4）。

背景：`egress.enforce()` 此前没有生产调用点；库内唯一真实出站执行是
`RscSession.verify()` 的验证子进程（facade.py 的内联 Popen）。本模块把它
收敛成一个可替换接口，并给出两种实现：

- `LocalRunner`：现状（Popen + start_new_session + 超时杀进程组），**行为逐条等价**；
- `SandboxedRunner`：把 argv 放进 netns/userns/LSM/gVisor。

能力矩阵（实测签名，不猜）：
| backend | cwd | timeout | 说明 |
|---|---|---|---|
| userns | ✅ | ✅ | `spawn_userns(**popen_kwargs)` |
| netns  | ✅（S5 加透传） | ✅ | `spawn_in_netns(**popen_kwargs)` |
| lsm    | ❌ | ❌ | `sandbox.run(argv)` 只有 rc，无 Popen |
| gvisor | ❌ | ✅ | `runsc` 自带 cwd 语义 |

`cwd` 不受支持时**显式抛 `EgressUnsupported`**，绝不静默改用 Local。
"""

from __future__ import annotations

import os
import signal
import subprocess
from dataclasses import dataclass
from typing import Callable, Protocol, Sequence, runtime_checkable


@dataclass(frozen=True)
class ToolResult:
    rc: int
    stdout: str = ""
    stderr: str = ""
    timed_out: bool = False
    backend: str = "local"

    @property
    def output(self) -> str:
        return (self.stdout + self.stderr).strip()


class EgressUnsupported(RuntimeError):
    """所选沙箱后端无法满足请求的约束（如 `cwd`）——fail-closed，不降级。"""


@runtime_checkable
class ToolRunner(Protocol):
    backend: str

    def run(self, argv: Sequence[str], *, cwd: str | None = None,
            timeout_s: float = 30.0, env: dict | None = None) -> ToolResult:
        ...


def kill_process_group(proc: subprocess.Popen) -> None:
    """超时后杀**整个进程组**（S24-P0：孙进程不得存活）。"""
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except (ProcessLookupError, PermissionError, OSError):
        try:
            proc.kill()
        except OSError:
            pass


def _wait(proc: subprocess.Popen, timeout_s: float, backend: str) -> ToolResult:
    try:
        stdout, stderr = proc.communicate(timeout=timeout_s)
        return ToolResult(proc.returncode, stdout or "", stderr or "",
                          False, backend)
    except subprocess.TimeoutExpired:
        kill_process_group(proc)
        try:
            proc.communicate(timeout=5.0)
        except subprocess.TimeoutExpired:      # 极端：组内仍有顽固进程
            pass
        return ToolResult(-1, "", f"verify_timeout after {timeout_s}s", True, backend)


class LocalRunner:
    """现状实现：本机直接执行（无隔离）。"""

    backend = "local"

    def run(self, argv, *, cwd=None, timeout_s=30.0, env=None) -> ToolResult:
        try:
            proc = subprocess.Popen(
                list(argv), cwd=cwd, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, text=True, start_new_session=True,
                env=env)
            return _wait(proc, timeout_s, self.backend)
        except OSError as exc:
            return ToolResult(-1, "", f"verify_spawn_failed: {exc}", False,
                              self.backend)


class SandboxedRunner:
    """把 argv 放进指定沙箱后端；不可用 → 抛（不静默放行）。"""

    def __init__(self, backend: str = "userns", *, netns_name: str = "harness-egress",
                 allow_hosts: Sequence[str] = (), bundle: str | None = None,
                 probe: Callable[[], object] | None = None) -> None:
        self.backend = backend
        self.netns_name = netns_name
        self.allow_hosts = tuple(allow_hosts)
        self.bundle = bundle
        self._probe = probe

    @property
    def supports_cwd(self) -> bool:
        return self.backend in ("userns", "netns")

    def run(self, argv, *, cwd=None, timeout_s=30.0, env=None) -> ToolResult:
        if cwd is not None and not self.supports_cwd:
            raise EgressUnsupported(f"{self.backend}:cwd")
        from . import egress
        from .egress import netns

        if self._probe is not None:
            status = self._probe()
            if not getattr(status, "available", False):
                raise egress.EgressUnavailable("无可用强制点")

        if self.backend == "userns":
            proc = netns.spawn_userns(list(argv), cwd=cwd, env=env)
            return _wait(proc, timeout_s, self.backend)
        if self.backend == "netns":
            proc = netns.spawn_in_netns(list(argv), self.netns_name,
                                        cwd=cwd, env=env)
            return _wait(proc, timeout_s, self.backend)
        if self.backend == "lsm":
            rc = egress.lsm.run_sandboxed(list(argv), self.allow_hosts)
            return ToolResult(int(rc), "", "", False, self.backend)
        if self.backend == "gvisor":
            from .egress import container
            if not self.bundle:
                raise EgressUnsupported("gvisor:bundle")
            proc = container.spawn_in_gvisor(list(argv), self.bundle)
            return _wait(proc, timeout_s, self.backend)
        raise EgressUnsupported(f"unknown_backend:{self.backend}")


def make_runner(mode: str, egress_guard, *, backend: str = "userns",
                netns_name: str = "harness-egress",
                allow_hosts: Sequence[str] = (),
                bundle: str | None = None) -> ToolRunner:
    """按模式选 runner。

    - `off`：LocalRunner（现状）；
    - `auto`：有可用强制点 → Sandboxed；否则 Local（egress.probe 已登记降级）；
    - `require`：无可用强制点 → 抛 `EgressUnavailable`（fail-closed）。
    """
    if mode not in ("off", "auto", "require"):
        raise ValueError(f"未知 verify_egress：{mode!r}（off|auto|require）")
    from . import egress as egress_pkg

    if mode == "off":
        return LocalRunner()
    status = egress_pkg.probe()
    if not getattr(status, "available", False):
        if mode == "require":
            raise egress_pkg.EgressUnavailable("verify_egress=require 但无可用强制点")
        return LocalRunner()
    chosen = backend
    if chosen == "userns" and not getattr(status, "userns", True):
        chosen = "netns"
    return SandboxedRunner(chosen, netns_name=netns_name,
                           allow_hosts=allow_hosts, bundle=bundle)
