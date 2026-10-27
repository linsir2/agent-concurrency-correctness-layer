"""S11c：DNS 钉住 + SSRF 阻断（纯 Python，标准库）。

设计约束（`docs/s11-egress-enforcement.md` v1.1 §4 / K-52）：

1. **钉住点必须是 `getaddrinfo`**（真实出站 `urllib → socket.create_connection`
   走的是 `getaddrinfo`，不是 `gethostbyname`）；
2. **取全 A/AAAA 再判**：只判「第一个返回的 IP」会被多记录 DNS 绕过
   （一个公网 IP + 一个 `169.254.169.254` 即命中）；
3. **`getpeername()` 复核**：`connect` 之后比对实际 peer，防 DNS rebinding
   （解析时给公网 IP、连接时给内网 IP）；
4. **重定向禁止二次解析**：`3xx` 直接拒绝，不跟随（跟随 = 第二次解析 = 钉住失效）。

诚实边界（K-52）：应用层钉住**不能替代**内核层强制 —— agent 直接 `socket.connect`
到 IP 即可绕过；本模块只保证「走本模块的路径」不可绕过，内核层由 `lsm.py`
（Landlock）/ `netns.py`（netns）负责。
"""

from __future__ import annotations

import functools
import http.client
import ipaddress
import socket
import urllib.request
from collections.abc import Callable, Iterable, Sequence
from typing import Any

Resolver = Callable[..., Sequence[Any]]

# 云 metadata 端点（IPv4 + IPv6 + 主机名）。`is_private`/`is_link_local` 覆盖不到
# 全部：`100.100.100.200`（阿里云）在 CGNAT 段、`fd00:ec2::254`（AWS IPv6）在 ULA 段、
# `169.254.170.2`（ECS task role）虽属 link-local 但显式列出以免依赖库行为变化。
CLOUD_METADATA: frozenset[str] = frozenset({
    "169.254.169.254",   # AWS / GCP / Azure / OpenStack IMDS
    "169.254.169.255",   # IMDS 保留
    "100.100.100.200",   # 阿里云 metadata
    "fd00:ec2::254",     # AWS IPv6 IMDS
    "169.254.170.2",     # AWS ECS task role
    "metadata.google.internal",   # GCP 主机名（DNS 指向 169.254.169.254）
})

AF_INET_LIKE = (socket.AF_INET, socket.AF_INET6)

# `ipaddress` 的 `is_private` 在不同 Python 版本对 CGNAT 判定不一致
# （3.14 上 `100.64.0.1` 的 `is_private` 为 False）→ 显式列出，不依赖库行为。
_EXTRA_FORBIDDEN_NETS = (
    ipaddress.ip_network("100.64.0.0/10"),      # CGNAT（阿里云 metadata 所在段）
    ipaddress.ip_network("192.0.0.0/24"),       # IETF protocol assignments
    ipaddress.ip_network("198.18.0.0/15"),      # benchmark
)


class EgressDenied(PermissionError):
    """出站被策略拒绝（SSRF / peer 复核失败 / 重定向）。"""


def _normalize(ip: str) -> str:
    text = str(ip).strip().lower()
    if text.startswith("[") and text.endswith("]"):
        text = text[1:-1]
    if "%" in text:                       # IPv6 zone id（fe80::1%eth0）
        text = text.split("%", 1)[0]
    return text


def _embedded_ipv4(addr: ipaddress._BaseAddress) -> Iterable[str]:
    """拆出 IPv6 里内嵌的 IPv4（v4-mapped / 6to4 / Teredo）。"""
    if not isinstance(addr, ipaddress.IPv6Address):
        return ()
    out: list[str] = []
    if addr.ipv4_mapped is not None:
        out.append(str(addr.ipv4_mapped))
    if addr.sixtofour is not None:
        out.append(str(addr.sixtofour))
    if addr.teredo is not None:
        out.extend(str(x) for x in addr.teredo)
    return out


def is_forbidden(ip: str) -> bool:
    """该 IP 是否禁止出站（fail-closed：不可解析也判禁止）。

    覆盖：private / loopback / link-local / multicast / reserved / unspecified
    + **完整云 metadata 列表**（见 `CLOUD_METADATA`）+ IPv6 内嵌 IPv4。
    """
    text = _normalize(ip)
    if text in CLOUD_METADATA:
        return True
    try:
        addr = ipaddress.ip_address(text)
    except ValueError:
        return True                        # 非 IP → 调用方有 bug → 拒绝（不静默放行）
    if (addr.is_private or addr.is_loopback or addr.is_link_local
            or addr.is_multicast or addr.is_reserved or addr.is_unspecified):
        return True
    if not addr.is_global:                 # CGNAT / TEST-NET / benchmark 等非全局段
        return True
    if any(addr in net for net in _EXTRA_FORBIDDEN_NETS
           if net.version == addr.version):
        return True
    return any(is_forbidden(x) for x in _embedded_ipv4(addr))


def _pin_key(ip: str) -> tuple[int, bytes]:
    addr = ipaddress.ip_address(ip)
    return (addr.version, addr.packed)


def resolve_and_pin(host: str, resolver: Resolver = socket.getaddrinfo) -> str:
    """解析 `host` 的**全 A/AAAA**，逐个校验，返回钉住的 IP（确定性排序取第一）。

    - 任一 IP 命中 `is_forbidden` → 抛 `EgressDenied`（不「取第一个合法的」：
      多记录 DNS 只要有一条指向内网即视为攻击）；
    - 结果为空 → `EgressDenied`（无地址不视为可用）；
    - 排序用 `(version, packed)` 而非字符串，保证跨版本/跨记录顺序一致。
    """
    infos = resolver(host, 0, proto=socket.IPPROTO_TCP)
    ips = {str(info[4][0]) for info in infos if info[0] in AF_INET_LIKE}
    if not ips:
        raise EgressDenied(f"DNS 无 A/AAAA 记录：{host!r}")
    for ip in ips:
        if is_forbidden(ip):
            raise EgressDenied(f"SSRF: {host} → {ip}")
    return min(ips, key=_pin_key)


def verify_peer(sock: Any, pinned: str) -> str:
    """`getpeername()` 复核：peer 必须等于钉住 IP 且本身合法，否则关 socket 拒绝。"""
    try:
        peer = sock.getpeername()
    except OSError as exc:                 # pragma: no cover - 依赖具体 socket 实现
        sock.close()
        raise EgressDenied(f"peer 复核失败（钉住 {pinned}）：{exc}") from exc
    peer_ip = str(peer[0])
    if _normalize(peer_ip) != _normalize(pinned) or is_forbidden(peer_ip):
        sock.close()
        raise EgressDenied(
            f"peer 复核失败：钉住 {pinned} 实际连到 {peer_ip}（DNS rebinding？）")
    return peer_ip


def connect_pinned(sock: Any, pinned: str, port: int, *,
                   timeout: float | None = None) -> str:
    """连**已钉住**的 IP 并复核 peer（不解析；供重试/复用路径）。"""
    if timeout is not None:
        sock.settimeout(timeout)
    sock.connect((pinned, port))
    return verify_peer(sock, pinned)


def connect_and_verify(sock: Any, host: str, port: int, *,
                       resolver: Resolver = socket.getaddrinfo,
                       timeout: float | None = None) -> str:
    """解析**一次** → 连钉住 IP → `getpeername()` 复核；返回实际 peer IP。

    peer 与钉住 IP 不一致或 peer 本身被禁止 → 关 socket 并抛 `EgressDenied`
    （防 DNS rebinding：解析时给公网 IP、连接时给内网 IP）。
    """
    pinned = resolve_and_pin(host, resolver)
    return connect_pinned(sock, pinned, port, timeout=timeout)


def pinned_connection(host: str, port: int, *,
                      resolver: Resolver = socket.getaddrinfo,
                      timeout: float | None = 10.0) -> socket.socket:
    """建立一条**钉住 + 复核**的 TCP 连接（只解析一次；调用方负责 `close`）。"""
    pinned = resolve_and_pin(host, resolver)
    family = socket.AF_INET6 if ":" in pinned else socket.AF_INET
    sock = socket.socket(family, socket.SOCK_STREAM)
    try:
        connect_pinned(sock, pinned, port, timeout=timeout)
    except BaseException:
        sock.close()
        raise
    return sock


class NoRedirect(urllib.request.HTTPRedirectHandler):
    """禁止跟随重定向：`3xx` 即拒绝。

    跟随重定向 = 对新 URL **二次解析** → 钉住失效（且可被 302 到 metadata）。
    """

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise EgressDenied(f"重定向禁止二次解析：{code} → {newurl}")


class _PinnedHTTPConnection(http.client.HTTPConnection):
    """连接**已钉住**的 IP（每次 `connect` 只解析一次；不二次解析）。"""

    def __init__(self, host: str, *, resolver: Resolver = socket.getaddrinfo,
                 timeout: Any = socket._GLOBAL_DEFAULT_TIMEOUT, **kwargs) -> None:
        super().__init__(host, timeout=timeout, **kwargs)
        self._resolver = resolver

    def connect(self) -> None:
        self.sock = pinned_connection(self.host, self.port,
                                      resolver=self._resolver,
                                      timeout=self.timeout)


class _PinnedHTTPSConnection(http.client.HTTPSConnection):
    """TLS 版本：钉住 IP 建连后按**主机名**做 SNI/证书校验。"""

    def __init__(self, host: str, *, resolver: Resolver = socket.getaddrinfo,
                 timeout: Any = socket._GLOBAL_DEFAULT_TIMEOUT,
                 context: Any = None, **kwargs) -> None:
        super().__init__(host, timeout=timeout, context=context, **kwargs)
        self._resolver = resolver

    def connect(self) -> None:
        sock = pinned_connection(self.host, self.port,
                                 resolver=self._resolver,
                                 timeout=self.timeout)
        self.sock = self._context.wrap_socket(sock, server_hostname=self.host)


class _PinnedHTTPHandler(urllib.request.HTTPHandler):
    """`http://` 走钉住连接（`do_open` 由 `_PinnedHTTPConnection.connect` 解析一次）。"""

    def __init__(self, *, resolver: Resolver = socket.getaddrinfo) -> None:
        super().__init__()
        self._resolver = resolver

    def http_open(self, req):
        cls = functools.partial(_PinnedHTTPConnection, resolver=self._resolver)
        return self.do_open(cls, req)


class _PinnedHTTPSHandler(urllib.request.HTTPSHandler):
    """`https://` 走钉住连接 + SNI（证书仍按原主机名校验）。"""

    def __init__(self, *, resolver: Resolver = socket.getaddrinfo) -> None:
        super().__init__()
        self._resolver = resolver

    def https_open(self, req):
        cls = functools.partial(_PinnedHTTPSConnection, resolver=self._resolver)
        return self.do_open(cls, req, context=self._context)


def build_opener(resolver: Resolver = socket.getaddrinfo
                 ) -> urllib.request.OpenerDirector:
    """返回**钉住 + 不跟随重定向**的 opener（供真实出站路径使用）。

    - `HTTPErrorProcessor` 必须显式加入：否则 `3xx` 被当成普通响应返回，
      `NoRedirect.redirect_request` 永远收不到 → 重定向测试会恒真（终审 9-3）；
    - 禁用环境代理：代理会把请求转给第三方，等于绕过钉住与凭证隔离；
    - 只允许显式 `http://` / `https://`（不注册 `ftp` 等）。
    """
    opener = urllib.request.OpenerDirector()
    for handler in (urllib.request.ProxyHandler({}),
                    _PinnedHTTPHandler(resolver=resolver),
                    _PinnedHTTPSHandler(resolver=resolver),
                    urllib.request.HTTPErrorProcessor(),
                    NoRedirect()):
        opener.add_handler(handler)
    return opener


__all__ = [
    "CLOUD_METADATA",
    "EgressDenied",
    "NoRedirect",
    "build_opener",
    "connect_and_verify",
    "connect_pinned",
    "is_forbidden",
    "pinned_connection",
    "resolve_and_pin",
    "verify_peer",
]
