"""S11 egress 强制点：编排入口 + `EgressUnavailable`（fail-closed）。

分波（`docs/s11-egress-enforcement.md` v1.1）：

| 模块 | 层次 | 强制力 |
|---|---|---|
| `resolver.py`（S11c） | 应用层 DNS 钉住 + SSRF + peer 复核 | **可绕过**（agent 直连 IP，K-52） |
| `lsm.py`（S11d） | 内核 LSM（Landlock，外部库 `sandlock`，**子进程**） | 不可绕过（TCP 端口级） |
| `netns.py`（S11e） | 内核路由层（`ip netns`，**部署级**，需 root） | 不可绕过（无路由默认拒绝） |

**统一口径**：没有强制点 → `EgressUnavailable`（**拒绝服务，不静默放行**）。
唯一允许「降级继续跑」的入口是 `EgressGuard(required=False)` —— 显式声明 +
`degradations` 留痕（`Harness(egress_required=False)` 或环境变量
`HARNESS_EGRESS_REQUIRED=0`），生产部署应传 `egress_required=True`
（构造即校验，缺强制点则拒绝启动）。
"""

from __future__ import annotations

import os
from collections.abc import Iterable, Sequence
from dataclasses import dataclass

from . import container, lsm, netns
from .resolver import (
    CLOUD_METADATA,
    EgressDenied,
    NoRedirect,
    build_opener,
    connect_and_verify,
    connect_pinned,
    is_forbidden,
    pinned_connection,
    resolve_and_pin,
    verify_peer,
)

ENV_REQUIRED = "HARNESS_EGRESS_REQUIRED"

_TRUE = {"1", "true", "yes", "on"}
_FALSE = {"0", "false", "no", "off"}


class EgressUnavailable(RuntimeError):
    """无可用 egress 强制点（库缺失 / 内核不支持 / 无 root）→ **拒绝服务**。"""


class SandboxUnavailable(RuntimeError):
    """S12：**沙箱后端**不可用（`unshare` 缺失/EPERM、`runsc` 缺失）。

    **专用异常**（不复用 `EgressUnavailable`）：语义不同 —— 前者是「无强制点」，
    后者是「选定的隔离后端跑不起来」。混用会让「后端选型失败」被误报成
    「没有强制点」，掩盖真实降级原因。
    """


@dataclass(frozen=True)
class EgressStatus:
    """强制点探测结果（`reasons` 为**显式登记**的降级原因，不允许静默）。"""

    lsm: bool
    netns: bool
    landlock_abi: int
    dns_enforceable: bool
    reasons: tuple[str, ...] = ()

    @property
    def available(self) -> bool:
        return self.lsm or self.netns

    def as_dict(self) -> dict:
        return {
            "lsm": self.lsm,
            "netns": self.netns,
            "landlock_abi": self.landlock_abi,
            "dns_enforceable": self.dns_enforceable,
            "available": self.available,
            "reasons": list(self.reasons),
        }


def probe() -> EgressStatus:
    """探测两条强制点（LSM / netns），**不抛**；不可用原因全部收集。"""
    lsm_ok, lsm_reason = lsm.sandbox_available()
    netns_ok, netns_reason = netns.netns_available()
    abi = lsm.landlock_abi_version()
    reasons = tuple(r for r in ("" if lsm_ok else lsm_reason,
                                "" if netns_ok else netns_reason) if r)
    return EgressStatus(lsm=lsm_ok, netns=netns_ok, landlock_abi=abi,
                        dns_enforceable=abi >= lsm.LANDLOCK_ABI_UDP,
                        reasons=reasons)


def require_available() -> EgressStatus:
    """必须有强制点，否则 `EgressUnavailable`（fail-closed）。"""
    status = probe()
    if not status.available:
        raise EgressUnavailable("无可用 egress 强制点：" + "；".join(status.reasons))
    return status


def _env_required() -> bool | None:
    raw = (os.environ.get(ENV_REQUIRED) or "").strip().lower()
    if raw in _TRUE:
        return True
    if raw in _FALSE:
        return False
    return None


class EgressGuard:
    """`Harness` 的 egress 开关（三态语义，避免「不装库就绕过」）。

    | `required` | 语义 |
    |---|---|
    | `True` | **构造即校验**：无强制点 → `EgressUnavailable`（生产部署，拒绝启动） |
    | `None`（默认，可被 `HARNESS_EGRESS_REQUIRED` 覆盖） | 构造不校验，**任何出站**经 `ensure()` 时 fail-closed |
    | `False` | 显式降级：允许构造并继续跑，但降级原因写入 `degradations`（留痕） |

    默认 `None` 而非 `True` 的原因：单进程 harness 的**本地动作**（stage/commit）
    不经过 egress（K-51），把「无库」等同于「不能构造 Harness」会让纯本地用例
    全部无法运行；但**出站**仍然 fail-closed —— 即「不装库 = 不能出站」，
    与原意（不能靠不装库绕过）一致。生产入口请显式传 `egress_required=True`。
    """

    def __init__(self, required: bool | None = None, *, probe_fn=None) -> None:
        self._probe = probe_fn or probe
        self.required = _env_required() if required is None else bool(required)
        self._status: EgressStatus | None = None
        if self.required is True:
            self.ensure()

    @property
    def status(self) -> EgressStatus:
        if self._status is None:
            self._status = self._probe()
        return self._status

    @property
    def degradations(self) -> tuple[str, ...]:
        """显式降级登记（无强制点时的原因；空元组 = 有强制点）。"""
        return () if self.status.available else self.status.reasons

    def ensure(self) -> EgressStatus:
        """出站前置：无强制点 → `EgressUnavailable`（除显式 `required=False`）。

        终审 4-3：`required=True` 时**还要求 DNS 可强制**（LSM ABI ≥ v10）。
        否则 ABI 6–9 上 `udp://:53` 不被内核强制 → DNS 可指向任意 resolver，
        「有强制点」是假象（fail-open）。
        """
        status = self.status
        if self.required is True and status.lsm and not status.dns_enforceable:
            raise EgressUnavailable(
                f"egress_required=True 但 DNS 不可强制（Landlock ABI="
                f"{status.landlock_abi} < {lsm.LANDLOCK_ABI_UDP}）→ 拒绝出站")
        if status.available or self.required is False:
            return status
        raise EgressUnavailable("无可用 egress 强制点：" + "；".join(status.reasons))

    def require_available(self) -> EgressStatus:
        """**生产 gate**（S12 K-55）：无强制点 → `EgressUnavailable`（拒绝启动）。

        与 `ensure()` 的差别：`ensure()` 尊重 `required=False` 的显式降级，
        本方法**不尊重** —— 生产入口不允许「降级继续跑」。
        """
        return require_available()

    def __repr__(self) -> str:  # 避免把探测细节泄漏进日志（只给结论）
        return f"EgressGuard(required={self.required!r}, available={self.status.available})"


def enforce(argv: Sequence[str], allow_hosts: Iterable[str] = (), *,
            backend: str = "auto", netns_name: str = "harness-egress",
            mode: str = netns.MODE_A_NO_ROUTE, proxy: str | None = None,
            bundle: str | None = None) -> int:
    """把 `argv` 放进**最强可用**的强制点里跑，返回退出码。

    `backend`：`auto`（LSM 优先，退 netns）/ `lsm` / `netns` /
    **`userns`**（`unshare -Urn`，无需 root）/ **`gvisor`**（`runsc` + OCI
    bundle，需 `bundle`）；都不可用 → `EgressUnavailable`
    （**拒绝服务**，绝不静默直接跑）。

    `auto` **不**降级到 `userns`/`gvisor`：显式后端才启用，避免改变
    「无强制点即拒绝」的既有语义（与 S11 用例一致）。
    """
    hosts = tuple(sorted({str(h) for h in allow_hosts if str(h).strip()}))
    if backend not in ("auto", "lsm", "netns", "userns", "gvisor"):
        raise ValueError(f"未知 backend：{backend!r}")
    if backend == "gvisor":
        if not bundle:
            raise ValueError("backend='gvisor' 必须提供 OCI bundle")
        proc = container.spawn_in_gvisor(argv, bundle)
        return int(proc.wait())
    status = probe()
    if backend in ("auto", "lsm") and status.lsm:
        return lsm.run_sandboxed(argv, hosts)
    if backend in ("auto", "netns") and status.netns:
        proc = netns.spawn_in_netns(argv, netns_name, mode=mode, proxy=proxy)
        return int(proc.wait())
    if backend == "userns":
        proc = netns.spawn_userns(argv)
        return int(proc.wait())
    raise EgressUnavailable("无可用 egress 强制点：" + "；".join(status.reasons))


__all__ = [
    "CLOUD_METADATA",
    "ENV_REQUIRED",
    "EgressDenied",
    "EgressGuard",
    "EgressStatus",
    "EgressUnavailable",
    "NoRedirect",
    "SandboxUnavailable",
    "build_opener",
    "connect_and_verify",
    "connect_pinned",
    "container",
    "enforce",
    "is_forbidden",
    "lsm",
    "netns",
    "pinned_connection",
    "probe",
    "require_available",
    "resolve_and_pin",
    "verify_peer",
]
