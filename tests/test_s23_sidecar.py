"""S23-4：HTTP sidecar（协议、鉴权、上限、并发写冲突、tick 回收）。

对应 `docs/s23-integration-plan.md` §3.4 / §6：
- 401/403/404/409/410/411/413/429/501 全链路；
- `agent_id` **不接受**请求参数（防冒名，密钥→agent 映射）；
- 每请求 `tick()`：过期 token 在下一次请求即被回收。
"""

from __future__ import annotations

import http.client
import json

import pytest
from conftest import make_harness

from harness import ManualClock, RscSession
from harness.integrations.http_sidecar import build_sidecar, serve_in_thread

AUTH = {"a1": "K1", "a2": "K2"}


@pytest.fixture()
def sidecar(tmp_path):
    h = make_harness(run_limits=1000)
    session = RscSession(h, tenant="t", min_budget=1,
                         state_dir=str(tmp_path / "state"))
    server = build_sidecar(session, keys={"K1": "a1", "K2": "a2"})
    thread = serve_in_thread(server)
    yield server, session, h
    server.shutdown()
    server.server_close()
    thread.join(timeout=5)


def _conn(server, timeout=5):
    host, port = server.server_address[:2]
    return http.client.HTTPConnection(host, port, timeout=timeout)


def _call(server, method, path, body=None, *, key="K1", headers=None):
    conn = _conn(server)
    hdr = dict(headers or {})
    if key is not None:
        hdr.setdefault("X-RSC-Auth", key)
    payload = None if body is None else json.dumps(body).encode("utf-8")
    if payload is not None:
        hdr.setdefault("Content-Type", "application/json")
    conn.request(method, path, body=payload, headers=hdr)
    resp = conn.getresponse()
    raw = resp.read()
    conn.close()
    return resp.status, (json.loads(raw) if raw else {})


# ---------------------------------------------------------------- 鉴权
def test_sidecar_requires_auth_and_known_key(sidecar):
    server, _, _ = sidecar
    assert _call(server, "GET", "/healthz", key=None)[0] == 401   # 未带密钥
    assert _call(server, "GET", "/healthz", key="wrong")[0] == 403  # 未知密钥
    # 未登记的密钥 → 403（不可能冒名成任何 agent）
    assert _call(server, "GET", "/healthz", key="K9")[0] == 403
    assert _call(server, "GET", "/healthz", key="K1")[0] == 200


# ---------------------------------------------------------------- 主链路
def test_begin_apply_dedup_snapshot_finish(sidecar):
    server, _, _ = sidecar
    code, out = _call(server, "POST", "/begin",
                      {"intent_key": "k", "writes": ["docs/a.md"]})
    assert code == 200, out
    token = out["token"]
    assert out["base_versions"] == {"docs/a.md": 0}

    body = {"token": token,
            "writes": [{"resource": "docs/a.md", "content": "v1"}]}
    code, first = _call(server, "POST", "/apply", body,
                        headers={"Idempotency-Key": "idem-1"})
    assert code == 200, first
    assert first["dedup"] is False

    code, snap = _call(server, "GET", f"/snapshot/{token}")
    assert code == 200 and snap == {"docs/a.md": 1}

    # 同键同内容重试 → 同收据 + dedup
    code, again = _call(server, "POST", "/apply", body,
                        headers={"Idempotency-Key": "idem-1"})
    assert code == 200 and again["receipt_id"] == first["receipt_id"]
    assert again["dedup"] is True

    # 同键异内容 → 409（IETF 语义）
    other = {"token": token,
             "writes": [{"resource": "docs/a.md", "content": "v2"}]}
    code, conflict = _call(server, "POST", "/apply", other,
                           headers={"Idempotency-Key": "idem-1"})
    assert code == 409, conflict

    assert _call(server, "POST", "/finish", {"token": token})[0] == 200
    # 已终态 → 410，且不会复活
    assert _call(server, "POST", "/apply", body,
                 headers={"Idempotency-Key": "idem-1"})[0] == 410


def test_stale_write_between_agents_is_409(sidecar):
    server, _, _ = sidecar
    _, t1 = _call(server, "POST", "/begin",
                  {"intent_key": "x", "writes": ["docs/race.md"]}, key="K1")
    _, t2 = _call(server, "POST", "/begin",
                  {"intent_key": "y", "writes": ["docs/race.md"]}, key="K2")
    first = {"token": t1["token"],
             "writes": [{"resource": "docs/race.md", "content": "a1"}]}
    second = {"token": t2["token"],
              "writes": [{"resource": "docs/race.md", "content": "a2"}]}
    assert _call(server, "POST", "/apply", first,
                 headers={"Idempotency-Key": "r1"})[0] == 200
    code, out = _call(server, "POST", "/apply", second, key="K2",
                      headers={"Idempotency-Key": "r2"})
    assert code == 409, out


def test_gate_detects_stale_view(sidecar):
    server, _session, h = sidecar
    _, t1 = _call(server, "POST", "/begin",
                  {"intent_key": "g", "writes": ["docs/g.md"]})
    # 他人提交 → base_version 过期
    h.workspace.put("docs/g.md", "other")
    code, out = _call(server, "POST", "/gate", {"token": t1["token"]})
    assert code == 409 and out["code"] == "stale_view", out


# ---------------------------------------------------------------- 协议边界
def test_body_limits_and_unsupported_chunked(sidecar):
    server, _, _ = sidecar

    conn = _conn(server)
    conn.putrequest("POST", "/begin", skip_accept_encoding=True)
    conn.putheader("X-RSC-Auth", "K1")
    conn.endheaders()
    assert conn.getresponse().status == 411          # 无 Content-Length
    conn.close()

    conn = _conn(server)
    conn.putrequest("POST", "/begin", skip_accept_encoding=True)
    conn.putheader("X-RSC-Auth", "K1")
    conn.putheader("Content-Length", str(2 << 20))
    conn.endheaders()
    assert conn.getresponse().status == 413          # 超 1 MiB
    conn.close()

    conn = _conn(server)
    conn.putrequest("POST", "/begin", skip_accept_encoding=True)
    conn.putheader("X-RSC-Auth", "K1")
    conn.putheader("Transfer-Encoding", "chunked")
    conn.endheaders()
    assert conn.getresponse().status == 501          # 不支持 chunked
    conn.close()


def test_unknown_token_is_404(sidecar):
    server, _, _ = sidecar
    code, _ = _call(server, "POST", "/apply",
                    {"token": "nope",
                     "writes": [{"resource": "a", "content": "x"}]},
                    headers={"Idempotency-Key": "k"})
    assert code == 404
    assert _call(server, "GET", "/snapshot/nope")[0] == 404
    assert _call(server, "POST", "/gate", {"token": "nope"})[0] == 404


def test_agent_id_is_not_a_request_parameter(sidecar):
    server, session, _ = sidecar
    code, out = _call(server, "POST", "/begin",
                      {"intent_key": "i", "writes": ["a.md"],
                       "agent_id": "victim"})
    assert code == 200
    # 服务端只用「密钥 → agent」映射，body 里的 agent_id 被忽略
    assert session._index[out["token"]]["agent_id"] == "a1"


def test_token_owner_cannot_be_taken_over(sidecar):
    """评审 P0：只能用自己的密钥操作自己的 token（跨 agent 401/403）。"""
    server, _, h = sidecar
    _, t1 = _call(server, "POST", "/begin",
                  {"intent_key": "o", "writes": ["a.md"]}, key="K1")
    token = t1["token"]
    body = {"token": token, "writes": [{"resource": "a.md", "content": "x"}]}
    assert _call(server, "POST", "/apply", body,
                 key="K2", headers={"Idempotency-Key": "k"})[0] == 403
    assert _call(server, "POST", "/finish", {"token": token}, key="K2")[0] == 403
    assert _call(server, "GET", f"/snapshot/{token}", key="K2")[0] == 403
    assert h.workspace.version("a.md") == 0
    assert len(h.acbs) == 1                      # 未被他人 finish/abort 掉


def test_apply_rejects_undeclared_resource(sidecar):
    """评审 P1：apply 只能写 begin 声明过的资源（否则静默 last-write-wins）。"""
    server, _, h = sidecar
    _, t = _call(server, "POST", "/begin",
                 {"intent_key": "u", "writes": ["a.md"]})
    code, out = _call(server, "POST", "/apply",
                      {"token": t["token"],
                       "writes": [{"resource": "b.md", "content": "x"}]},
                      headers={"Idempotency-Key": "u1"})
    assert code == 400 and "undeclared_write" in out.get("error", ""), out
    assert h.workspace.version("b.md") == 0


def test_bad_reads_is_400(sidecar):
    server, _, _ = sidecar
    code, _ = _call(server, "POST", "/begin",
                    {"intent_key": "r", "writes": ["a.md"],
                     "reads": [{"x": 1}]})
    assert code == 400


def test_tick_failure_does_not_leak_inflight(tmp_path):
    """评审 P1：tick 抛异常也必须释放信号量，否则 max_inflight=1 时永久 503。"""
    h = make_harness(run_limits=100)
    session = RscSession(h, tenant="t", min_budget=1,
                         state_dir=str(tmp_path / "s"))
    server = build_sidecar(session, keys={"K": "a"}, max_inflight=1)
    thread = serve_in_thread(server)
    orig = session.tick
    state = {"fail": True}

    def flaky():
        if state["fail"]:
            state["fail"] = False
            raise RuntimeError("boom")
        orig()

    session.tick = flaky                          # type: ignore[method-assign]
    try:
        try:
            _call(server, "GET", "/healthz", key="K")
        except (ConnectionError, OSError):
            pass                                  # 首个请求异常断连属预期
        assert _call(server, "GET", "/healthz", key="K")[0] == 200
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_budget_exhausted_is_429(tmp_path):
    h = make_harness(run_limits=1)
    session = RscSession(h, tenant="t", min_budget=1, state_dir=str(tmp_path / "s"))
    server = build_sidecar(session, keys={"K": "a"})
    thread = serve_in_thread(server)
    try:
        assert _call(server, "POST", "/begin",
                     {"intent_key": "1", "writes": ["a"]}, key="K")[0] == 200
        code, out = _call(server, "POST", "/begin",
                          {"intent_key": "2", "writes": ["b"]}, key="K")
        assert code == 429 and "BUDGET_EXHAUSTED" in out.get("error", ""), out
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_tick_reaps_expired_token_on_next_request(tmp_path):
    clock = ManualClock()
    h = make_harness(run_limits=100, clock=clock)
    session = RscSession(h, tenant="t", min_budget=1, token_ttl_ms=10,
                         state_dir=str(tmp_path / "s"))
    server = build_sidecar(session, keys={"K": "a"})
    thread = serve_in_thread(server)
    try:
        _, out = _call(server, "POST", "/begin",
                       {"intent_key": "t", "writes": ["a.md"]}, key="K")
        token = out["token"]
        clock.advance(20)                       # 超过 token_ttl_ms
        code, _ = _call(server, "GET", f"/snapshot/{token}", key="K")
        assert code in (404, 410)
        assert token not in h.acbs
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
