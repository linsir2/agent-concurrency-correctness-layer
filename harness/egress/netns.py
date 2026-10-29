"""S11e：netns 沙箱（**部署级**，外部工具 `ip`，需 `CAP_NET_ADMIN`）。

诚实定位（`docs/s11-egress-enforcement.md` v1.1 §6 / P0-1 / P0-3 / K-49）：

- 单进程 harness **无法**进程内 netns：agent 与 harness 同进程，把整个 harness 放进
  netns 会**同时切断 harness 自身出站**（`llm.py` 的 urllib）→ netns **只能用于子进程**；
- 无 root / 无 `ip` → `EgressUnavailable`（**显式登记**，不静默 skip）。

**二选一（不可叠加）**：

- **A：无路由**（`MODE_A_NO_ROUTE`）—— 默认拒绝，无代理；子进程连任何外部地址
  都得 `ENETUNREACH`。语义最强，但第 1 层 L7 代理**收不到流量**；
- **B：veth + 路由到代理**（`MODE_B_PROXY`）—— 有 L7 ACL（mitmproxy 等），
  但**放弃**「无路由默认拒绝」语义（流量确实能出去，只是先到代理）。

物理互斥原因：无路由 → 沙箱连不到宿主代理 → 代理永远收不到流量。二者是
**替代关系**，叠加等于自相矛盾。故 `mode` 是单值枚举，`MODE_B_PROXY` 必须给
`proxy`，且不允许同时要求「无路由」。
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
from collections.abc import Sequence

MODE_A_NO_ROUTE = "a"
MODE_B_PROXY = "b"
MODES = (MODE_A_NO_ROUTE, MODE_B_PROXY)

# 兼容旧常量（不再用于实际接口名；接口名按 netns 名派生，见 `_veth_names`）
VETH_HOST = "egress-h"
VETH_NS = "egress-ns"
SUBNET_PREFIX = "10.200.0"
HOST_ADDR = f"{SUBNET_PREFIX}.1/24"
NS_ADDR = f"{SUBNET_PREFIX}.2/24"

MARKER_SUFFIX = ".mode"


def _veth_names(name: str) -> tuple[str, str]:
    """按 netns 名派生**每实例唯一**的 veth 名（终审 5-3）。

    Linux 接口名上限 15 字节（IFNAMSIZ）→ 基名截断到 12，后缀 `-h` / `-ns`。
    超长名截断时附 3 位 sha1 后缀，避免两个长名截断后撞名。
    """
    base = re.sub(r"[^A-Za-z0-9_.-]", "-", name).strip("-")
    if not base:
        base = "egress"
    if len(base) > 12:
        digest = hashlib.sha1(name.encode("utf-8")).hexdigest()[:3]
        base = f"{base[:8]}-{digest}"
    return f"{base}-h", f"{base}-ns"


def _marker_dir() -> str:
    """模式标记目录：优先与 netns 同目录（`/run/netns`），不可写则退回临时目录。"""
    if os.path.isdir("/run/netns") and os.access("/run/netns", os.W_OK):
        return "/run/netns"
    return os.path.join(tempfile.gettempdir(), "harness-netns")


def _marker_path(name: str) -> str:
    return os.path.join(_marker_dir(), f"{name}{MARKER_SUFFIX}")


def _write_mode_marker(name: str, mode: str, proxy: str | None) -> None:
    path = _marker_path(name)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump({"mode": mode, "proxy": proxy}, fh, sort_keys=True)
        fh.flush()
        os.fsync(fh.fileno())


def _read_mode_marker(name: str) -> dict | None:
    try:
        with open(_marker_path(name), "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def _remove_mode_marker(name: str) -> None:
    try:
        os.unlink(_marker_path(name))
    except OSError:
        pass


def ip_path() -> str | None:
    return shutil.which("ip")


def netns_available() -> tuple[bool, str]:
    """返回 `(可用, 不可用原因)`；原因串用于**显式登记**。"""
    if ip_path() is None:
        return False, "`ip` 不可用（iproute2 未安装）→ 部署级 egress 不可用"
    if os.geteuid() != 0:
        return False, "非 root（需 CAP_NET_ADMIN）→ 部署级 egress 不可用"
    return True, ""


def _run(argv: Sequence[str], *, check: bool = True) -> subprocess.CompletedProcess:
    from . import EgressUnavailable

    proc = subprocess.run(list(argv), capture_output=True, text=True, check=False)
    if check and proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip()
        raise EgressUnavailable(f"`{' '.join(argv)}` 失败：{detail or proc.returncode}")
    return proc


def _require_available() -> str:
    from . import EgressUnavailable

    ok, reason = netns_available()
    if not ok:
        raise EgressUnavailable(reason)
    ip = ip_path()
    assert ip is not None                      # netns_available 已保证
    return ip


def create_netns(name: str, *, mode: str = MODE_A_NO_ROUTE,
                 proxy: str | None = None, uplink: str = "eth0") -> None:
    """建 netns 并按其模式完成**二选一**配置。

    - `MODE_A_NO_ROUTE`：只把 `lo` 拉起，**不建 veth、不加路由**（默认拒绝）；
    - `MODE_B_PROXY`：建 veth 对 + 在 ns 内加默认路由指向宿主 veth（→ 代理）。
      `proxy` 仅用于语义校验（`host:port`），实际 ACL 由代理进程负责。
    """
    if mode not in MODES:
        raise ValueError(f"未知 netns 模式：{mode!r}（只能是 {MODES}）")
    if mode == MODE_B_PROXY and not proxy:
        raise ValueError("MODE_B_PROXY 必须提供 proxy（host:port），否则等于无 L7 ACL")
    if mode == MODE_A_NO_ROUTE and proxy:
        raise ValueError("MODE_A_NO_ROUTE 与代理互斥（无路由 → 代理收不到流量，P0-3）")

    ip = _require_available()
    host_veth, ns_veth = _veth_names(name)
    _run([ip, "netns", "add", name])
    try:
        _run([ip, "netns", "exec", name, "ip", "link", "set", "lo", "up"])
        if mode == MODE_A_NO_ROUTE:
            _write_mode_marker(name, mode, None)
            return
        # ---- MODE_B_PROXY：veth + 路由（放弃「无路由」语义，换取 L7 ACL）----
        _run([ip, "link", "add", host_veth, "type", "veth", "peer", "name", ns_veth])
        _run([ip, "link", "set", ns_veth, "netns", name])
        _run([ip, "addr", "add", HOST_ADDR, "dev", host_veth])
        _run([ip, "link", "set", host_veth, "up"])
        _run([ip, "netns", "exec", name, "ip", "addr", "add", NS_ADDR, "dev", ns_veth])
        _run([ip, "netns", "exec", name, "ip", "link", "set", ns_veth, "up"])
        _run([ip, "netns", "exec", name, "ip", "route", "add", "default",
              "via", f"{SUBNET_PREFIX}.1", "dev", ns_veth])
        # 代理在宿主侧监听（`proxy` 的端口），ns 内流量经默认路由到宿主 veth。
        # 上游转发（NAT/代理 → 真实网络）由部署方配置，本模块不接管。
        _write_mode_marker(name, mode, proxy)
        _ = uplink
    except BaseException:
        # 终审 5-1：`ip netns add` 成功后任何一步失败都必须清理，避免残留
        # netns / veth 让下一次复用拿到半配置环境（fail-open）。
        _run([ip, "link", "del", host_veth], check=False)
        _run([ip, "netns", "del", name], check=False)
        _remove_mode_marker(name)
        raise


def spawn_in_netns(argv: Sequence[str], name: str, *,
                   mode: str = MODE_A_NO_ROUTE,
                   proxy: str | None = None,
                   **popen_kwargs) -> subprocess.Popen:
    """在 `name` netns 里启动子进程（**部署级**；进程内 netns 不支持，P0-1）。

    终审 5-2：复用**已存在**的 netns 前必须校验模式标记 —— 否则已有 MODE_B
    （有路由）时请求 MODE_A（无路由）会直接复用 → 实际可出站（fail-open）。
    标记缺失视为未知 → 拒绝复用（fail-closed）。
    """
    from . import EgressUnavailable

    ip = _require_available()
    if mode not in MODES:
        raise ValueError(f"未知 netns 模式：{mode!r}（只能是 {MODES}）")
    if mode == MODE_B_PROXY and not proxy:
        raise ValueError("MODE_B_PROXY 必须提供 proxy（host:port），否则等于无 L7 ACL")
    if mode == MODE_A_NO_ROUTE and proxy:
        raise ValueError("MODE_A_NO_ROUTE 与代理互斥（无路由 → 代理收不到流量，P0-3）")
    proc = subprocess.run([ip, "netns", "list"], capture_output=True, text=True, check=False)
    names = {line.split()[0] for line in proc.stdout.splitlines() if line.strip()}
    if name not in names:
        create_netns(name, mode=mode, proxy=proxy)
    else:
        marker = _read_mode_marker(name)
        if marker is None:
            raise EgressUnavailable(
                f"netns {name!r} 已存在但模式标记缺失 → 拒绝复用（fail-closed）")
        if marker.get("mode") != mode:
            raise EgressUnavailable(
                f"netns {name!r} 既有模式 {marker.get('mode')!r} ≠ 请求模式 "
                f"{mode!r} → 拒绝复用（MODE_A/B 互斥）")
        if mode == MODE_B_PROXY and marker.get("proxy") != proxy:
            raise EgressUnavailable(
                f"netns {name!r} 既有代理 {marker.get('proxy')!r} ≠ 请求代理 "
                f"{proxy!r} → 拒绝复用")
    return subprocess.Popen([ip, "netns", "exec", name, *argv],
                            **popen_kwargs)


def delete_netns(name: str) -> None:
    """删除 netns（幂等：不存在视为成功；本实例的宿主 veth 一并清理）。"""
    ip = _require_available()
    host_veth, _ns_veth = _veth_names(name)
    _run([ip, "link", "del", host_veth], check=False)
    _run([ip, "netns", "del", name], check=False)
    _remove_mode_marker(name)


# ================================================================ #
# S12：`unshare -Urn` backend（用户态 + 网络命名空间，无需 root）
# ================================================================ #
#
# 与上面的 `ip netns`（部署级、需 CAP_NET_ADMIN）互补：`unshare --map-root-user
# --net` 用 **user namespace** 映射 root，普通用户即可创建**独立网络命名空间**
# → 新 netns 内**无路由**（只有未起的 loopback）→ 任何外部 connect 得
# `ENETUNREACH`（默认拒绝，语义同 MODE_A）。
#
# **不叠加** `ip netns`：两者是**替代关系**（一个用 user ns、一个用
# CAP_NET_ADMIN），同时启用只会互相干扰。

# 显式降级登记（不允许静默）：`(backend, reason)` 列表，`degradations()` 可读。
_DEGRADATIONS: list[tuple[str, str]] = []

# 正确参数（**修 P1**：`--map-root` 不存在，应为 `--map-root-user`）
USERNS_ARGV = ["unshare", "--user", "--map-root-user", "--net",
               "--fork", "--pid", "--mount-proc"]
# 规范探针用的简写（与 `USERNS_ARGV` 的隔离语义等价；见 `_probe_userns`）
USERNS_SHORT = ["unshare", "-Urn"]

# 探针目标：TEST-NET-1（RFC 5737，保证不可路由）
_PROBE_SNIPPET = (
    "import socket,sys,errno\n"
    "s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)\n"
    "s.settimeout(2)\n"
    "try:\n"
    "    s.connect(('192.0.2.1',443))\n"
    "except OSError as e:\n"
    "    sys.stderr.write('ERR:%s\\n'%errno.errorcode.get(e.errno,'UNKNOWN'))\n"
    "    sys.exit(1)\n"
    "sys.exit(0)\n"
)

# S12：探针缓存（session 级；key = (python, argv 前缀)）
_PROBE_CACHE: dict[tuple[str, tuple[str, ...]], bool] = {}


def register_degradation(backend: str, reason: str) -> None:
    """显式登记一项后端降级（不静默）。"""
    _DEGRADATIONS.append((backend, (reason or "").strip() or "未给出原因"))


def degradations() -> list[tuple[str, str]]:
    """已登记的后端降级（空列表 = 无降级）。"""
    return list(_DEGRADATIONS)


def userns_available() -> tuple[bool, str]:
    """`unshare -Urn` 是否可用（不可用 = 无 user/net namespace 权限）。"""
    import shutil as _shutil
    import sys as _sys
    if _shutil.which("unshare") is None:
        return False, "unshare 不可用（util-linux 未安装）"
    if _probe_userns(_sys.executable):
        return True, ""
    return False, ("unshare -Urn 探针未通过（netns 内 connect 未得 ENETUNREACH"
                   " → 隔离语义不成立，可能 EPERM/被 seccomp 拦）")


def _probe_userns(python: str | None = None,
                  prefix: Sequence[str] | None = None) -> bool:
    """**启动前探针**（真跑一次）：netns 内 connect 必须 `ENETUNREACH`。

    不依赖「已强制」的声明 —— 直接验证隔离语义本身。`prefix` 默认用规范简写
    `unshare -Urn`；传 `USERNS_ARGV` 可验证**完整 argv**（含 `--mount-proc`）。

    S12：结果按 `(python, prefix)` **session 级缓存**（避免每用例重复起子进程）。
    """
    import shutil as _shutil
    import sys as _sys
    python = python or _sys.executable
    argv = list(prefix) if prefix is not None else list(USERNS_SHORT)
    key = (python, tuple(argv))
    if key in _PROBE_CACHE:
        return _PROBE_CACHE[key]
    if _shutil.which("unshare") is None:
        _PROBE_CACHE[key] = False
        return False
    try:
        proc = subprocess.run([*argv, python, "-c", _PROBE_SNIPPET],
                              capture_output=True, text=True, timeout=20,
                              check=False)
    except (OSError, subprocess.TimeoutExpired):
        _PROBE_CACHE[key] = False
        return False
    ok = (proc.returncode != 0
          and "ENETUNREACH" in (proc.stderr or ""))
    _PROBE_CACHE[key] = ok
    return ok


def spawn_userns(argv: Sequence[str], **popen_kwargs) -> subprocess.Popen:
    """在 `unshare -Urn` 子进程里跑 `argv`（**默认拒绝出站**）。

    - 无 `unshare` / 探针未过 → `SandboxUnavailable`（**专用异常**，非
      `EgressUnavailable`），并显式登记降级；
    - 规范 argv = `USERNS_ARGV`（`--map-root-user` 而非 `--map-root`），
      **必须**含 `--net`；完整 argv 探针不过 → 同样拒绝（**不静默降级**为
      无 `--net` 或残缺 argv）。
    """
    import shutil as _shutil
    import sys as _sys

    from . import SandboxUnavailable
    if _shutil.which("unshare") is None:
        reason = "unshare 不可用（util-linux 未安装）→ 无法建立 netns 隔离"
        register_degradation("unshare", reason)
        raise SandboxUnavailable(reason)
    if not _probe_userns(_sys.executable):
        reason = ("unshare -Urn 探针未通过（netns 内 connect 未得 ENETUNREACH"
                  " → 隔离语义不成立，可能 EPERM/被 seccomp 拦）")
        register_degradation("unshare", reason)
        raise SandboxUnavailable(reason)
    if not _probe_userns(_sys.executable, USERNS_ARGV):
        reason = ("unshare 完整 argv 探针未通过（--mount-proc/--fork 可能 EPERM）"
                  " → 拒绝启动（不静默降级为残缺隔离）")
        register_degradation("unshare", reason)
        raise SandboxUnavailable(reason)
    return subprocess.Popen([*USERNS_ARGV, *argv], **popen_kwargs)


__all__ = [
    "MODES",
    "MODE_A_NO_ROUTE",
    "MODE_B_PROXY",
    "USERNS_ARGV",
    "USERNS_SHORT",
    "create_netns",
    "degradations",
    "delete_netns",
    "ip_path",
    "netns_available",
    "register_degradation",
    "spawn_in_netns",
    "spawn_userns",
    "userns_available",
]
