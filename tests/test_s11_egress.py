"""S11c/d/e：DNS 钉住 + SSRF / Landlock / netns / 降级（`docs/s11-egress-enforcement.md` v1.1 §4-§7）。

| 用例 | 正对照 | 负对照 |
|---|---|---|
| S11-4 | 注入两次不同 IP 的 `getaddrinfo` → **只解析一次**（钉住）；私网/metadata → `EgressDenied` | 直连 `getaddrinfo` 绕过 → 被 `getpeername` 复核拦（K-52） |
| S11-5 | `sandbox.run` 子进程连未授权端口 → 失败 | 未 sandbox 的子进程 → 成功 |
| S11-6 | `unshare -Urn` 子进程 connect → `ENETUNREACH`（**非** `ECONNREFUSED`） | 去掉 `--net` → 结果不同 |
| S11-7 | 无 `sandlock`/`ip` → `EgressUnavailable`（拒服务） | 库在（fake）→ 返回码来自**真跑**的子进程 |

环境 invalid（sandlock 缺失 / Landlock ABI=0 / `ip netns` 无 CAP_NET_ADMIN /
`unshare` EPERM）一律 `pytest.xfail` + `_egress_env.register()` **显式登记**，
**不计 pass**；本文件自检禁止 `pytest.skip`。
"""

from __future__ import annotations

import contextlib
import errno
import http.server
import os
import pathlib
import socket
import subprocess
import sys
import threading
from typing import ClassVar

import pytest
from _egress_env import (
    FEATURE_LSM,
    FEATURE_NETNS,
    FEATURE_UNSHARE,
    probe_environment,
    register,
    snapshot,
    unshare_available,
)

import harness.egress as egress_pkg
from harness import Harness
from harness.egress import (
    EgressDenied,
    EgressGuard,
    EgressStatus,
    EgressUnavailable,
    NoRedirect,
    build_opener,
    connect_and_verify,
    enforce,
    is_forbidden,
    lsm,
    netns,
    pinned_connection,
    probe,
    resolve_and_pin,
    verify_peer,
)

pytestmark = pytest.mark.egress

CHILD = str(pathlib.Path(__file__).with_name("_egress_child.py"))
PUBLIC = "93.184.216.34"


# ================================================================ 夹具/工具
class CountingResolver:
    """按调用次数返回不同答案的 `getaddrinfo` 替身（第 2 次起故意给内网 IP）。"""

    def __init__(self, answers: list[list[str]]) -> None:
        self.answers = answers
        self.calls = 0

    def __call__(self, host, port, **kwargs):
        self.calls += 1
        ips = self.answers[min(self.calls - 1, len(self.answers) - 1)]
        return [(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", (ip, 0))
                for ip in ips]


class FakeSock:
    """可控 peer 的 socket 替身（模拟 DNS rebinding / peer 不一致）。"""

    def __init__(self, peer: str) -> None:
        self.peer = peer
        self.connected = None
        self.closed = False
        self.timeouts: list[float] = []

    def settimeout(self, value):
        self.timeouts.append(value)

    def connect(self, addr):
        self.connected = addr

    def getpeername(self):
        return (self.peer, 443)

    def close(self):
        self.closed = True


@contextlib.contextmanager
def _listener():
    """本地监听器（正/负对照的真实目标）。"""
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(8)
    srv.settimeout(0.1)
    port = srv.getsockname()[1]
    stop = threading.Event()
    conns: list[socket.socket] = []

    def _loop():
        while not stop.is_set():
            try:
                conn, _ = srv.accept()
                conns.append(conn)
            except (TimeoutError, OSError):
                continue

    thread = threading.Thread(target=_loop, daemon=True)
    thread.start()
    try:
        yield port, conns
    finally:
        stop.set()
        thread.join(2)
        for conn in conns:
            conn.close()
        srv.close()


def _run_child(prefix: list[str], *args: str, timeout: float = 20.0):
    return subprocess.run([*prefix, sys.executable, CHILD, *args],
                          capture_output=True, text=True, timeout=timeout, check=False)


# ================================================================ S11-4 DNS/SSRF
def test_s11_4_resolve_and_pin_resolves_exactly_once():
    """正对照：两次答案不同，但只解析一次 → 钉住首个解析结果。"""
    resolver = CountingResolver([["93.184.216.34", "93.184.216.35"], ["10.0.0.9"]])

    pinned = resolve_and_pin("api.example.com", resolver)

    assert pinned == "93.184.216.34"
    assert resolver.calls == 1, "钉住必须在一次解析内完成（第二次解析 = 可被 rebinding 利用）"


def test_s11_4_resolve_and_pin_deterministic_order():
    """多记录 → 数值序取第一（不受 DNS 返回顺序影响）。"""
    forward = CountingResolver([["93.184.216.35", "93.184.216.34"]])
    backward = CountingResolver([["93.184.216.34", "93.184.216.35"]])
    assert resolve_and_pin("api.example.com", forward) == PUBLIC
    assert resolve_and_pin("api.example.com", backward) == PUBLIC


@pytest.mark.parametrize("ip", [
    "127.0.0.1", "10.1.2.3", "172.16.0.1", "192.168.0.1", "100.64.0.1",
    "169.254.169.254", "169.254.169.255", "100.100.100.200", "169.254.170.2",
    "fd00:ec2::254", "::1", "fe80::1", "224.0.0.1", "0.0.0.0", "255.255.255.255",
    "::ffff:169.254.169.254",
])
def test_s11_4_resolve_and_pin_rejects_forbidden(ip):
    """私网 / link-local / CGNAT / 云 metadata → `EgressDenied`（任一记录命中即拒）。"""
    resolver = CountingResolver([[PUBLIC, ip]])
    with pytest.raises(EgressDenied, match="SSRF"):
        resolve_and_pin("mixed.example.com", resolver)


def test_s11_4_resolve_and_pin_rejects_empty_answer():
    resolver = CountingResolver([[]])
    with pytest.raises(EgressDenied, match="无 A/AAAA"):
        resolve_and_pin("nx.example.com", resolver)


def test_s11_4_is_forbidden_matrix():
    for ip in ("1.1.1.1", "8.8.8.8", PUBLIC, "2606:4700:4700::1111"):
        assert is_forbidden(ip) is False, f"{ip} 是公网地址，不应被禁止"
    for ip in ("169.254.169.254", "100.100.100.200", "fd00:ec2::254",
               "metadata.google.internal", "127.0.0.1", "100.64.0.1"):
        assert is_forbidden(ip) is True
    assert is_forbidden("not-an-ip") is True, "不可解析必须 fail-closed"


def test_s11_4_connect_and_verify_uses_pinned_ip():
    resolver = CountingResolver([[PUBLIC], ["10.0.0.9"]])
    sock = FakeSock(peer=PUBLIC)

    peer = connect_and_verify(sock, "api.example.com", 443, resolver=resolver, timeout=3.0)

    assert peer == PUBLIC
    assert sock.connected == (PUBLIC, 443), "连接必须用钉住 IP，不得二次解析"
    assert resolver.calls == 1
    assert sock.timeouts == [3.0]
    assert sock.closed is False


def test_s11_4_negative_control_peer_mismatch_denied():
    """负对照：解析给公网 IP、实际 peer 是 metadata（rebinding）→ 复核拦下。"""
    resolver = CountingResolver([[PUBLIC]])
    sock = FakeSock(peer="169.254.169.254")

    with pytest.raises(EgressDenied, match="peer 复核失败"):
        connect_and_verify(sock, "evil.example.com", 443, resolver=resolver)

    assert sock.closed is True


def test_s11_4_verify_peer_rejects_forbidden_even_if_equal():
    """peer 与钉住一致但本身被禁止（钉住值来自绕过路径）→ 仍拒。"""
    sock = FakeSock(peer="169.254.169.254")
    with pytest.raises(EgressDenied):
        verify_peer(sock, "169.254.169.254")


def test_s11_4_negative_control_direct_bypass_reaches_listener():
    """K-52 诚实边界：绕过钉住直连 `getaddrinfo` 是成功的；钉住路径在解析期即拒。"""
    with _listener() as (port, _conns):
        infos = socket.getaddrinfo("127.0.0.1", port, proto=socket.IPPROTO_TCP)
        sock = socket.create_connection(infos[0][4], timeout=2.0)   # 直连绕过
        try:
            assert sock.fileno() != -1
        finally:
            sock.close()

        with pytest.raises(EgressDenied, match="SSRF"):
            connect_and_verify(socket.socket(), "127.0.0.1", port)


def test_s11_4_pinned_connection_resolves_once(monkeypatch):
    """`pinned_connection` 只解析一次；成功路径 + peer 复核（模拟公网 IP）。"""
    monkeypatch.setattr("harness.egress.resolver.is_forbidden", lambda ip: False)
    resolver = CountingResolver([["127.0.0.1"], ["10.0.0.9"]])

    with _listener() as (port, _conns):
        sock = pinned_connection("api.example.com", port, resolver=resolver, timeout=2.0)
        try:
            assert sock.getpeername()[0] == "127.0.0.1"
        finally:
            sock.close()

    assert resolver.calls == 1


def test_s11_4_redirect_is_refused():
    """重定向禁止二次解析：`3xx` 直接拒绝，不跟随。"""
    handler = NoRedirect()
    with pytest.raises(EgressDenied, match="重定向禁止二次解析"):
        handler.redirect_request(None, None, 302, "Found", {},
                                 "http://169.254.169.254/latest/meta-data/")
    opener = build_opener()
    assert any(isinstance(h, NoRedirect) for h in opener.handlers)
    # 终审 9-3：`HTTPErrorProcessor` 必须在链上，否则 3xx 不会进 error 链，
    # 上面的 handler 单测恒真、真实 opener 却会直接返回 302。
    import urllib.request
    assert any(isinstance(h, urllib.request.HTTPErrorProcessor)
               for h in opener.handlers)


def test_s11_4_redirect_302_raises_egress_denied(monkeypatch):
    """终审 9-3：真实 opener 遇到本地 302 **必须抛** `EgressDenied`（不跟随）。

    只断言 `any(isinstance(h, NoRedirect))` 是恒真：缺 `HTTPErrorProcessor` 时
    `opener.open` 会正常返回 302 响应。本用例用本地 server 证伪。
    """
    monkeypatch.setattr("harness.egress.resolver.is_forbidden", lambda ip: False)

    class _Redirect(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(302)
            self.send_header("Location",
                             "http://169.254.169.254/latest/meta-data/")
            self.end_headers()

        def log_message(self, *args):            # 静音测试日志
            pass

    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Redirect)
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    try:
        url = f"http://127.0.0.1:{srv.server_address[1]}/x"
        with pytest.raises(EgressDenied, match="重定向"):
            build_opener().open(url, timeout=3)
    finally:
        srv.shutdown()
        srv.server_close()
        thread.join(2)


# ================================================================ S11-8 出站接线
def test_s11_8_harness_wires_egress_guard_into_llm():
    """终审 8-1：`Harness` 必须把**同一个** guard 注入 llm（不是各建一个）。"""
    h = Harness()
    assert h.llm.egress is h.egress


def test_s11_8_llm_post_calls_ensure_before_network(monkeypatch):
    """`llm._post` 入口先 `ensure()`；不可用 → 拒绝出站（不发起连接）。"""
    from harness import llm

    class _Guard:
        def __init__(self) -> None:
            self.called = False

        def ensure(self):
            self.called = True
            raise EgressUnavailable("fake: no egress")

    guard = _Guard()
    called = {"opener": 0}

    def _boom(*args, **kwargs):
        called["opener"] += 1
        raise AssertionError("ensure 拒绝后不得发起出站")

    monkeypatch.setattr(llm, "build_opener", _boom)
    with pytest.raises(EgressUnavailable, match="no egress"):
        llm._post("https://dashscope.aliyuncs.com/x", {}, {}, 1.0, egress=guard)
    assert guard.called is True
    assert called["opener"] == 0


def test_s11_8_llm_post_fail_closed_by_default(monkeypatch):
    """未注入 guard → 模块默认 guard（required=None）出站 fail-closed。"""
    from harness import llm

    monkeypatch.setattr(llm, "_default_guard",
                        EgressGuard(probe_fn=_unavailable_status))
    with pytest.raises(EgressUnavailable, match="无可用 egress 强制点"):
        llm._post("https://dashscope.aliyuncs.com/x", {}, {}, 1.0)


# ================================================================ S11-5 Landlock
def test_s11_5_negative_control_unsandboxed_child_reaches_port():
    """负对照：**未** sandbox 的子进程连本地端口 → 成功（证明目标可达）。"""
    with _listener() as (port, _conns):
        proc = _run_child([], "connect", "127.0.0.1", str(port), "2")
    assert proc.returncode == 0, f"未沙箱子进程应连上：{proc.stdout} {proc.stderr}"
    assert proc.stdout.strip() == "OK"


@pytest.mark.sandbox
def test_s11_5_sandboxed_child_blocked_from_unauthorized_port():
    """正对照：Landlock 子进程连**未授权**端口 → 失败。"""
    ok, reason = lsm.sandbox_available()
    if not ok:
        pytest.xfail(register(FEATURE_LSM, reason))

    with _listener() as (port, _conns):
        rc = lsm.run_sandboxed(
            [sys.executable, CHILD, "connect", "127.0.0.1", str(port), "2"],
            {"api.example.com"})
    assert rc != 0, "Landlock 未拦住未授权端口"


# ================================================================ S11-6 netns
@pytest.mark.netns
def test_s11_6_netns_child_gets_enetunreach():
    """正对照：`unshare -Urn` 子进程 connect → `ENETUNREACH`（**非** ECONNREFUSED）。"""
    ok, reason = unshare_available()
    if not ok:
        pytest.xfail(register(FEATURE_UNSHARE, reason))

    proc = _run_child(["unshare", "-Urn"], "connect", "192.0.2.1", "443", "0.5")
    out = proc.stdout.strip()
    assert out == "ERR:ENETUNREACH", f"期望 ENETUNREACH，实际 {out!r} / {proc.stderr}"
    assert "ECONNREFUSED" not in out


@pytest.mark.netns
def test_s11_6_negative_control_without_net_differs():
    """负对照：去掉 `--net`（只留 `-Ur`）→ 结果不同（本地端口可达 / 不再是 ENETUNREACH）。"""
    ok, reason = unshare_available()
    if not ok:
        pytest.xfail(register(FEATURE_UNSHARE, reason))

    with _listener() as (port, _conns):
        inside = _run_child(["unshare", "-Urn"], "connect", "127.0.0.1", str(port), "0.5")
        outside = _run_child(["unshare", "-Ur"], "connect", "127.0.0.1", str(port), "2")

    assert inside.stdout.strip() == "ERR:ENETUNREACH"
    assert outside.stdout.strip() == "OK", "去掉 --net 后应能连上宿主监听器"
    assert inside.stdout != outside.stdout


@pytest.mark.netns
def test_s11_6_ip_netns_unavailable_is_registered():
    """`ip netns` 需 CAP_NET_ADMIN：不可用 → xfail 并登记（不静默 skip）。"""
    ok, reason = netns.netns_available()
    if not ok:
        pytest.xfail(register(FEATURE_NETNS, reason))
    if os.geteuid() != 0:                      # pragma: no cover - 与上面重复，防御
        pytest.xfail(register(FEATURE_NETNS, "非 root"))
    proc = subprocess.run([netns.ip_path(), "netns", "add", "s11-probe"],
                          capture_output=True, text=True, timeout=20, check=False)
    try:
        if proc.returncode != 0:
            pytest.xfail(register(FEATURE_NETNS,
                                  (proc.stderr or "").strip() or "ip netns add 失败"))
    finally:
        subprocess.run([netns.ip_path(), "netns", "del", "s11-probe"],
                       capture_output=True, text=True, timeout=20, check=False)


# ================================================================ S11-7 降级
def test_s11_7_lsm_missing_raises(monkeypatch):
    """无 sandlock → `EgressUnavailable`（拒绝服务，不静默放行）。"""
    monkeypatch.setattr(lsm, "_HAS_SANDLOCK", False)
    monkeypatch.setattr(lsm, "Sandbox", None)

    with pytest.raises(EgressUnavailable, match="sandlock"):
        lsm.run_sandboxed(["true"], set())


def test_s11_7_lsm_abi_too_low_raises(monkeypatch):
    """ABI < 6 → fail-closed（即使库在）。"""
    monkeypatch.setattr(lsm, "_HAS_SANDLOCK", True)
    monkeypatch.setattr(lsm, "Sandbox", object)

    with pytest.raises(EgressUnavailable, match="ABI=3"):
        lsm.run_sandboxed(["true"], set(), abi_probe=lambda: 3)


def test_s11_7_confine_in_process_forbidden():
    """P0-2：进程内 `confine()` 不可逆 → 永久拒绝。"""
    with pytest.raises(EgressUnavailable, match="confine"):
        lsm.confine(object())


def test_s11_7_netns_missing_ip_raises(monkeypatch):
    monkeypatch.setattr(netns, "ip_path", lambda: None)
    with pytest.raises(EgressUnavailable, match="ip"):
        netns.spawn_in_netns(["true"], "s11-no-ip")


def test_s11_7_netns_non_root_raises(monkeypatch):
    monkeypatch.setattr(netns, "ip_path", lambda: "/usr/sbin/ip")
    monkeypatch.setattr(netns.os, "geteuid", lambda: 1000)
    with pytest.raises(EgressUnavailable, match="root"):
        netns.spawn_in_netns(["true"], "s11-non-root")


class FakeSandbox:
    """sandlock 替身：记录构造参数，`run` **真跑**子进程。"""

    last_kwargs: dict | None = None
    instances: ClassVar[list[FakeSandbox]] = []

    def __init__(self, **kwargs) -> None:
        FakeSandbox.last_kwargs = dict(kwargs)
        self.kwargs = dict(kwargs)
        FakeSandbox.instances.append(self)

    def run(self, argv):
        return subprocess.run(list(argv), check=False).returncode


def test_s11_7_negative_control_fake_backend_really_runs(monkeypatch):
    """负对照：库在（fake）→ 白名单形状正确，且返回值来自**真跑**的子进程。"""
    monkeypatch.setattr(lsm, "_HAS_SANDLOCK", True)
    monkeypatch.setattr(lsm, "Sandbox", FakeSandbox)

    rc = lsm.run_sandboxed(
        [sys.executable, CHILD, "exit", "7"],
        {"b.example.com", "api.example.com"},
        abi_probe=lambda: 6)

    assert rc == 7, "返回码必须来自真实子进程（不是硬编码成功）"
    kwargs = FakeSandbox.last_kwargs or {}
    # ABI=6 < v10：DNS 不可真强制 → **不得**加入 udp://:53（fail-closed）
    assert kwargs["net_allow"] == [
        "api.example.com:443", "b.example.com:443"]
    assert kwargs["fs_readable"] == list(lsm.DEFAULT_FS_READABLE)
    assert kwargs["http_allow"] == []


def test_s11_7_dns_rule_only_when_enforceable(monkeypatch):
    """终审 4-3：ABI 6–9 不加 `udp://:53`；ABI ≥ 10 才加（DNS 可真强制）。"""
    monkeypatch.setattr(lsm, "Sandbox", FakeSandbox)
    lsm.build_sandbox(["api.example.com"], abi=6)
    assert FakeSandbox.last_kwargs["net_allow"] == ["api.example.com:443"]
    lsm.build_sandbox(["api.example.com"], abi=9)
    assert FakeSandbox.last_kwargs["net_allow"] == ["api.example.com:443"]
    lsm.build_sandbox(["api.example.com"], abi=10)
    assert FakeSandbox.last_kwargs["net_allow"] == [
        "api.example.com:443", "udp://:53"]


def test_s11_7_build_sandbox_can_drop_dns(monkeypatch):
    """`dns_allow=False` 时不写 `udp://:53`（UDP 需 ABI v10，K-50 可显式放弃）。"""
    monkeypatch.setattr(lsm, "Sandbox", FakeSandbox)
    lsm.build_sandbox(["api.example.com"], dns_allow=False)
    assert FakeSandbox.last_kwargs["net_allow"] == ["api.example.com:443"]


def test_s11_7_dns_enforceable_reflects_abi():
    assert lsm.dns_enforceable(abi_probe=lambda: 9) is False
    assert lsm.dns_enforceable(abi_probe=lambda: 10) is True


def _unavailable_status(reasons=("fake: no backend",)) -> EgressStatus:
    return EgressStatus(lsm=False, netns=False, landlock_abi=0,
                        dns_enforceable=False, reasons=reasons)


def test_s11_7_enforce_fail_closed_without_backend(monkeypatch):
    """无强制点 → `enforce` 拒绝服务（绝不静默直接跑）。"""
    monkeypatch.setattr(egress_pkg, "probe", lambda: _unavailable_status())
    with pytest.raises(EgressUnavailable, match="无可用 egress 强制点"):
        enforce(["true"], {"api.example.com"})


def test_s11_7_enforce_dispatch_lsm_then_netns(monkeypatch):
    """编排：LSM 优先；无 LSM 退 netns；两者都无 → 拒。"""
    monkeypatch.setattr(egress_pkg, "probe", lambda: EgressStatus(
        lsm=True, netns=True, landlock_abi=6, dns_enforceable=False))
    monkeypatch.setattr(lsm, "run_sandboxed", lambda argv, hosts, **kw: 0)
    assert enforce(["true"], {"api.example.com"}) == 0

    monkeypatch.setattr(egress_pkg, "probe", lambda: EgressStatus(
        lsm=False, netns=True, landlock_abi=0, dns_enforceable=False))

    class _Proc:
        def wait(self):
            return 5

    monkeypatch.setattr(netns, "spawn_in_netns",
                        lambda argv, name, **kw: _Proc())
    assert enforce(["true"], backend="netns") == 5

    with pytest.raises(ValueError, match="未知 backend"):
        enforce(["true"], backend="bpf")


def test_s11_7_guard_three_states(monkeypatch):
    """三态：True=构造即拒 / None=出站时拒 / False=显式降级并留痕。"""
    with pytest.raises(EgressUnavailable):
        EgressGuard(required=True, probe_fn=_unavailable_status)

    guard = EgressGuard(probe_fn=_unavailable_status)          # None（默认）
    with pytest.raises(EgressUnavailable):
        guard.ensure()
    assert guard.degradations == ("fake: no backend",)

    degraded = EgressGuard(required=False, probe_fn=_unavailable_status)
    assert degraded.ensure().available is False
    assert degraded.degradations, "显式降级必须留痕"
    assert "available=False" in repr(degraded)


def test_s11_7_guard_env_switch(monkeypatch):
    monkeypatch.setenv("HARNESS_EGRESS_REQUIRED", "0")
    assert EgressGuard(probe_fn=_unavailable_status).required is False
    monkeypatch.setenv("HARNESS_EGRESS_REQUIRED", "1")
    with pytest.raises(EgressUnavailable):
        EgressGuard(probe_fn=_unavailable_status)


def test_s11_7_required_guard_requires_dns_enforceable():
    """终审 4-3：有 LSM 但 ABI 6–9（DNS 不可强制）→ `required=True` 必须拒绝。"""
    abi6 = EgressStatus(lsm=True, netns=False, landlock_abi=6,
                        dns_enforceable=False)
    with pytest.raises(EgressUnavailable, match="DNS 不可强制"):
        EgressGuard(required=True, probe_fn=lambda: abi6)

    abi10 = EgressStatus(lsm=True, netns=False, landlock_abi=10,
                         dns_enforceable=True)
    guard = EgressGuard(required=True, probe_fn=lambda: abi10)
    assert guard.ensure().dns_enforceable is True


def test_s11_7_harness_egress_required_true_refuses_start(monkeypatch):
    """`Harness(egress_required=True)`：无强制点 → 拒绝启动（生产 fail-closed）。"""
    import harness.kernel.kernel as harness_mod

    def _boom(*args, **kwargs):
        raise EgressUnavailable("fake: 启动校验失败")

    monkeypatch.setattr(harness_mod, "EgressGuard", _boom)
    with pytest.raises(EgressUnavailable, match="启动校验失败"):
        Harness(egress_required=True)


def test_s11_7_harness_real_backend_semantics():
    """真实探测下三态语义自洽（有强制点且 DNS 可强制 → 通过；否则 fail-closed）。"""
    status = probe()
    usable = status.available and not (status.lsm and not status.dns_enforceable)
    if usable:
        h = Harness(egress_required=True)
        assert h.egress.required is True
        assert h.egress.ensure().available is True
        assert h.egress.degradations == ()
    else:
        with pytest.raises(EgressUnavailable):
            Harness(egress_required=True)
        h = Harness()
        with pytest.raises(EgressUnavailable):
            h.egress.ensure()
        assert h.egress.degradations == status.reasons


def test_s11_7_harness_explicit_degradation_constructs():
    h = Harness(egress_required=False)
    assert h.egress.required is False
    st = h.egress.ensure()
    if st.available:
        assert h.egress.degradations == ()
    else:
        assert h.egress.degradations, "降级必须有原因登记"


def test_s11_7_netns_modes_are_exclusive():
    """A（无路由）/ B（veth+代理）**二选一**：不可叠加、B 必须给 proxy。"""
    with pytest.raises(ValueError, match="未知 netns 模式"):
        netns.create_netns("x", mode="c")
    with pytest.raises(ValueError, match="必须提供 proxy"):
        netns.create_netns("x", mode=netns.MODE_B_PROXY)
    with pytest.raises(ValueError, match="互斥"):
        netns.create_netns("x", mode=netns.MODE_A_NO_ROUTE, proxy="127.0.0.1:8080")


def _fake_run(calls: list[list[str]]):
    def _run(argv, *, check=True):
        calls.append(list(argv))
        return subprocess.CompletedProcess(list(argv), 0, "", "")
    return _run


def test_s11_7_netns_mode_a_has_no_route(monkeypatch):
    calls: list[list[str]] = []
    monkeypatch.setattr(netns, "netns_available", lambda: (True, ""))
    monkeypatch.setattr(netns, "ip_path", lambda: "/sbin/ip")
    monkeypatch.setattr(netns, "_run", _fake_run(calls))

    netns.create_netns("egress-a", mode=netns.MODE_A_NO_ROUTE)

    assert ["/sbin/ip", "netns", "add", "egress-a"] in calls
    assert ["/sbin/ip", "netns", "exec", "egress-a", "ip", "link", "set", "lo", "up"] in calls
    assert not any("veth" in " ".join(c) for c in calls), "MODE_A 不得建 veth/路由"
    assert not any("route" in c for c in calls)


def test_s11_7_netns_mode_b_adds_veth_and_route(monkeypatch):
    calls: list[list[str]] = []
    monkeypatch.setattr(netns, "netns_available", lambda: (True, ""))
    monkeypatch.setattr(netns, "ip_path", lambda: "/sbin/ip")
    monkeypatch.setattr(netns, "_run", _fake_run(calls))

    netns.create_netns("egress-b", mode=netns.MODE_B_PROXY, proxy="127.0.0.1:8080")

    joined = [" ".join(c) for c in calls]
    assert any("veth" in line for line in joined)
    assert any("route add default" in line for line in joined)


class _FakePopen:
    def __init__(self, argv):
        self.argv = list(argv)

    def wait(self):
        return 0


class _FakeSubprocess:
    def __init__(self, listing: str) -> None:
        self.listing = listing
        self.popen: list[_FakePopen] = []

    def run(self, argv, **kwargs):
        return subprocess.CompletedProcess(list(argv), 0, self.listing, "")

    def Popen(self, argv):
        proc = _FakePopen(argv)
        self.popen.append(proc)
        return proc


def test_s11_7_spawn_in_netns_uses_ip_exec(monkeypatch):
    fake = _FakeSubprocess("egress-a\n")
    monkeypatch.setattr(netns, "netns_available", lambda: (True, ""))
    monkeypatch.setattr(netns, "ip_path", lambda: "/sbin/ip")
    monkeypatch.setattr(netns, "_run", _fake_run([]))
    monkeypatch.setattr(netns, "subprocess", fake)
    monkeypatch.setattr(netns, "_read_mode_marker",
                        lambda name: {"mode": netns.MODE_A_NO_ROUTE, "proxy": None})

    proc = netns.spawn_in_netns(["echo", "hi"], "egress-a")

    assert proc.argv == ["/sbin/ip", "netns", "exec", "egress-a", "echo", "hi"]
    assert len(fake.popen) == 1


def test_s11_7_spawn_refuses_mode_mismatch(monkeypatch):
    """终审 5-2：已存在 MODE_B（有路由）时请求 MODE_A（无路由）→ 拒绝复用。"""
    fake = _FakeSubprocess("egress-a\n")
    monkeypatch.setattr(netns, "netns_available", lambda: (True, ""))
    monkeypatch.setattr(netns, "ip_path", lambda: "/sbin/ip")
    monkeypatch.setattr(netns, "subprocess", fake)
    monkeypatch.setattr(netns, "_read_mode_marker",
                        lambda name: {"mode": netns.MODE_B_PROXY,
                                      "proxy": "127.0.0.1:8080"})

    with pytest.raises(EgressUnavailable, match="拒绝复用"):
        netns.spawn_in_netns(["true"], "egress-a", mode=netns.MODE_A_NO_ROUTE)
    assert fake.popen == [], "模式不匹配不得启动子进程（fail-open）"


def test_s11_7_spawn_refuses_missing_mode_marker(monkeypatch):
    """标记缺失 = 未知模式 → 拒绝复用（不猜）。"""
    fake = _FakeSubprocess("egress-a\n")
    monkeypatch.setattr(netns, "netns_available", lambda: (True, ""))
    monkeypatch.setattr(netns, "ip_path", lambda: "/sbin/ip")
    monkeypatch.setattr(netns, "subprocess", fake)
    monkeypatch.setattr(netns, "_read_mode_marker", lambda name: None)

    with pytest.raises(EgressUnavailable, match="标记缺失"):
        netns.spawn_in_netns(["true"], "egress-a")
    assert fake.popen == []


def test_s11_7_veth_names_are_per_instance():
    """终审 5-3：veth 名按 netns 派生（两实例不冲突），且 ≤ IFNAMSIZ(15)。"""
    h1, n1 = netns._veth_names("egress-a")
    h2, n2 = netns._veth_names("egress-b")
    assert h1 != h2 and n1 != n2
    assert h1 != netns.VETH_HOST and n1 != netns.VETH_NS
    assert max(len(h1), len(n1), len(h2), len(n2)) <= 15


def test_s11_7_create_netns_cleans_up_on_failure(monkeypatch):
    """终审 5-1：`netns add` 成功后配置失败 → 清理 netns + veth（不残留）。"""
    calls: list[list[str]] = []
    removed: list[str] = []

    def failing(argv, *, check=True):
        calls.append(list(argv))
        if argv[:3] == ["/sbin/ip", "link", "add"]:
            raise EgressUnavailable("fake: veth add failed")
        return subprocess.CompletedProcess(list(argv), 0, "", "")

    monkeypatch.setattr(netns, "netns_available", lambda: (True, ""))
    monkeypatch.setattr(netns, "ip_path", lambda: "/sbin/ip")
    monkeypatch.setattr(netns, "_run", failing)
    monkeypatch.setattr(netns, "_remove_mode_marker", lambda name: removed.append(name))

    with pytest.raises(EgressUnavailable, match="veth add failed"):
        netns.create_netns("egress-x", mode=netns.MODE_B_PROXY,
                           proxy="127.0.0.1:8080")

    joined = [" ".join(c) for c in calls]
    assert "/sbin/ip netns add egress-x" in joined
    assert any(line.startswith("/sbin/ip link del") for line in joined), "veth 未清理"
    assert "/sbin/ip netns del egress-x" in joined, "netns 未清理"
    assert removed == ["egress-x"], "模式标记未清理"


def test_s11_7_delete_netns_is_idempotent(monkeypatch):
    calls: list[list[str]] = []
    monkeypatch.setattr(netns, "netns_available", lambda: (True, ""))
    monkeypatch.setattr(netns, "ip_path", lambda: "/sbin/ip")
    monkeypatch.setattr(netns, "_run", _fake_run(calls))

    netns.delete_netns("egress-a")

    assert ["/sbin/ip", "netns", "del", "egress-a"] in calls
    host_veth, _ = netns._veth_names("egress-a")
    assert ["/sbin/ip", "link", "del", host_veth] in calls, \
        "必须清理本实例的 veth（不是全局固定名）"


# ================================================================ 登记自检
def test_s11_7_environment_invalid_registered():
    """环境 invalid 必须显式登记（与真实探测结果一致，不允许静默 skip）。"""
    snap = probe_environment()
    status = probe()

    if not status.lsm:
        assert snap.get(FEATURE_LSM), "Landlock 缺失未登记"
    if not status.netns:
        assert snap.get(FEATURE_NETNS), "netns 缺失未登记"
    ok, _reason = unshare_available()
    if not ok:
        assert FEATURE_UNSHARE in snap, "unshare 缺失未登记"


def test_s11_7_no_silent_skip():
    """自检：本文件不得使用 skip 语义（只允许 xfail + register 登记）。"""
    source = pathlib.Path(__file__).read_text(encoding="utf-8")
    skip_needle = "pytest" + ".skip("                 # 拆开写，避免自检命中自身字面量
    mark_needle = "pytest.mark" + ".skip"
    xfail_needle = "pytest" + ".xfail("
    assert skip_needle not in source
    assert mark_needle not in source
    for line in source.splitlines():
        if xfail_needle in line and "assert" not in line:
            assert "register(" in line, f"xfail 未登记原因：{line.strip()}"


def test_s11_7_errno_constants_used_by_child():
    """子进程探针的判据常量存在（`ENETUNREACH` 与 `ECONNREFUSED` 必须可区分）。"""
    assert errno.ENETUNREACH != errno.ECONNREFUSED
    assert snapshot() is not None
