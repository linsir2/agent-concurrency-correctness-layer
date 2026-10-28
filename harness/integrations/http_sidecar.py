"""S23：HTTP sidecar（纯标准库）—— 任何语言宿主零 Python 集成。

协议与门面一一对应（见 `docs/s23-integration-plan.md` §3.4）：

```
POST /begin        {intent_key, writes, reads}   -> 200 Token | 429 | 403 | 400
POST /apply        {token, writes} + Idempotency-Key 头 -> 200 Receipt | 409 | 429 | 403
POST /gate         {token}                       -> 200 | 409 stale_view | 403
POST /finish       {token, outcome}              -> 200 | 410
POST /abort        {token, reason}               -> 200 | 410
GET  /snapshot/{token}                           -> 200 | 410
```

硬约束（stdlib 实测，不依赖框架）：

- 显式 `HTTP/1.1` + 每个响应带 `Content-Length`（`BaseHTTPRequestHandler` 默认
  `HTTP/1.0`，不设就不是 keep-alive）；
- **无 chunked、无内置体积上限** → 本模块强制 `Content-Length` 且 ≤ 1 MiB
  （超出 413；缺失 411；chunked 501）；
- 线程/请求无上限 → `BoundedSemaphore`（默认 32，满则 503）；
- 鉴权：`X-RSC-Auth` 共享密钥 + `hmac.compare_digest`；
- **`agent_id` 绝不作为请求参数**（它参与幂等命名空间与 worktree 归属），由
  「密钥 → agent」映射推导（对齐 `mcp_server.py` 的反冒名设计）；
- 每请求调 `session.tick()`（无后台线程时也让 reaper 推进）；
- **不做**长轮询 / SSE / TLS：`gate` 是即时版本比较，长轮询只会挂死线程。
"""

from __future__ import annotations

import hmac
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from ..facade import Allow, Conflict, Deny, Receipt, RscSession, Token, Wait
from ..rebase import DELETE

MAX_BODY = 1 << 20                     # 1 MiB


def _writes_of(raw: Any) -> list[tuple[str, Any]] | None:
    """解析 `[{"resource","content"}]` 或 `[[resource, content]]`；非法 → None。"""
    if not isinstance(raw, list) or not raw:
        return None
    out: list[tuple[str, Any]] = []
    for item in raw:
        if isinstance(item, dict):
            res, content = item.get("resource"), item.get("content")
        elif isinstance(item, (list, tuple)) and len(item) == 2:
            res, content = item[0], item[1]
        else:
            return None
        if not isinstance(res, str) or not res:
            return None
        out.append((res, DELETE if content is None else content))
    return out


class _SidecarServer(ThreadingHTTPServer):
    """必须在 `__init__`（bind/listen）前设 `request_queue_size`，事后赋值无效。"""

    daemon_threads = True
    request_queue_size = 64


def build_sidecar(session: RscSession, *, keys: dict[str, str] | None = None,
                  auth_token: str = "", default_agent: str = "http-agent",
                  max_body: int = MAX_BODY,
                  max_inflight: int = 32,
                  host: str = "127.0.0.1", port: int = 0) -> ThreadingHTTPServer:
    """构造（未启动）sidecar；调用方 `serve_forever()` 或 `serve_in_thread`。

    - `keys`: **每个 agent 一把密钥**（`{key: agent_id}`）；不允许「共享密钥 +
      请求里声明 agent」——那等于允许冒名。
    - `auth_token`: 单 agent 便捷写法（映射到 `default_agent`）。
    - 返回的 server 上挂了 `sidecar_session`，便于测试/运维观测。
    """
    table = dict(keys or {})
    if not table and auth_token:
        table = {str(auth_token): str(default_agent)}
    if not table:
        raise ValueError("keys 不能为空（否则等于无鉴权）")
    inflight = threading.BoundedSemaphore(max(1, int(max_inflight)))
    entries = tuple((str(k), str(v)) for k, v in table.items())

    def _agent_of(candidate: str) -> str | None:
        """常量时间比较：逐个 key 用 `compare_digest`，不做普通字符串等值。"""
        found = None
        raw = candidate.encode("utf-8")
        for key, agent in entries:
            if hmac.compare_digest(raw, key.encode("utf-8")):
                found = agent
        return found

    class _Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        timeout = 10                                        # 半截 body 不得长期占槽

        # ------------------------------------------------------------ 输出
        def log_message(self, fmt, *args):                  # 静默（不污染 stdout）
            return

        def _send(self, code: int, payload: dict, *, close: bool = False,
                  headers: dict | None = None) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            for k, v in (headers or {}).items():
                self.send_header(k, v)
            if close:
                self.send_header("Connection", "close")
                self.close_connection = True
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                # 对端已关闭（如 411/413 后客户端先断）→ 不再抛，保持日志干净
                self.close_connection = True

        # ------------------------------------------------------------ 鉴权
        def _auth(self) -> tuple[int, str]:
            """返回 `(0, agent)` = 通过；`(401, "")` = 未带密钥；`(403, "")` = 未知密钥。"""
            got = self.headers.get("X-RSC-Auth", "")
            if not got:
                return 401, ""
            agent = _agent_of(got)
            if agent is None:
                return 403, ""
            return 0, agent

        # ------------------------------------------------------------ 入口
        def _read_json(self) -> dict | None:
            transfer = (self.headers.get("Transfer-Encoding") or "").lower()
            if "chunked" in transfer:
                self._send(501, {"error": "chunked_not_supported"}, close=True)
                return None
            raw_len = self.headers.get("Content-Length")
            if raw_len is None:
                self._send(411, {"error": "content_length_required"}, close=True)
                return None
            try:
                length = int(raw_len)
            except ValueError:
                self._send(400, {"error": "bad_content_length"}, close=True)
                return None
            if length < 0 or length > max_body:
                self._send(413, {"error": "payload_too_large"}, close=True)
                return None
            raw = self.rfile.read(length)
            try:
                data = json.loads(raw.decode("utf-8")) if raw else {}
            except (ValueError, UnicodeDecodeError):
                self._send(400, {"error": "bad_json"})
                return None
            if not isinstance(data, dict):
                self._send(400, {"error": "json_object_required"})
                return None
            return data

        def _guard(self) -> str | None:
            if not inflight.acquire(blocking=False):
                self._send(503, {"error": "too_many_inflight"},
                           headers={"Retry-After": "1"})
                return None
            try:
                code, agent = self._auth()
                if code:
                    self._send(code, {"error": "unauthorized"})
                    inflight.release()
                    return None
                session.tick()
                return agent
            except BaseException:
                # 评审 P1：tick/鉴权异常也必须释放槽位，否则永久 503
                inflight.release()
                raise

        def _owned(self, token: str, agent: str) -> bool:
            """评审 P0：token 只能由**自己的 agent** 操作（跨密钥不得接管）。"""
            owner = session.owner_of(token)
            if owner is None:
                return True                     # 未知/终态 → 端点自己给 404/410
            if owner != agent:
                self._send(403, {"error": "token_owner_mismatch"})
                return False
            return True

        def do_GET(self) -> None:
            agent = self._guard()
            if agent is None:
                return
            try:
                if self.path == "/healthz":
                    self._send(200, {"ok": True})
                elif self.path.startswith("/snapshot/"):
                    token = self.path[len("/snapshot/"):]
                    if not self._owned(token, agent):
                        return
                    out = session.snapshot(token)
                    if isinstance(out, Deny):
                        self._send(_deny_status(out), {"error": out.reason,
                                                       "code": out.code})
                    else:
                        self._send(200, dict(out))
                else:
                    self._send(404, {"error": "not_found"})
            finally:
                inflight.release()

        def do_POST(self) -> None:
            agent = self._guard()
            if agent is None:
                return
            try:
                self._handle_post(agent)
            finally:
                inflight.release()

        def _handle_post(self, agent: str) -> None:
            data = self._read_json()
            if data is None:
                return
            path = self.path
            if path == "/begin":
                writes = data.get("writes")
                if not isinstance(writes, list) or not all(
                        isinstance(x, str) and x for x in writes):
                    self._send(400, {"error": "writes_required"})
                    return
                reads = data.get("reads")
                if reads is None:
                    reads = []
                elif not isinstance(reads, list) or not all(
                        isinstance(x, str) and x for x in reads):
                    self._send(400, {"error": "bad_reads"})
                    return
                tok = session.begin(agent, str(data.get("intent_key") or "http"),
                                    writes=tuple(writes), reads=tuple(reads))
                if isinstance(tok, Token):
                    self._send(200, {"token": tok.token,
                                     "base_versions": dict(tok.base_versions),
                                     "deadline_ms": tok.deadline_ms})
                elif isinstance(tok, Wait):
                    self._send(429, {"error": tok.reason}, headers={"Retry-After": "1"})
                else:
                    self._send(403, {"error": getattr(tok, "reason", "denied")})
                return

            if path == "/apply":
                token = str(data.get("token") or "")
                writes = _writes_of(data.get("writes"))
                key = (self.headers.get("Idempotency-Key")
                       or str(data.get("idempotency_key") or ""))
                if not token or writes is None:
                    self._send(400, {"error": "token_and_writes_required"})
                    return
                if not key:
                    self._send(400, {"error": "idempotency_key_required"})
                    return
                if not self._owned(token, agent):
                    return
                r = session.apply(token, writes, idempotency_key=key,
                                  agent_id=agent)
                if isinstance(r, Receipt):
                    self._send(200, {"receipt_id": r.receipt_id, "seq": r.seq,
                                     "dedup": r.dedup,
                                     "verified": r.verified})
                elif isinstance(r, Conflict):
                    self._send(409, {"error": r.reason})
                elif isinstance(r, Wait):
                    self._send(429, {"error": r.reason}, headers={"Retry-After": "1"})
                else:
                    self._send(_deny_status(r), {"error": getattr(r, "reason", "denied"),
                                                 "code": getattr(r, "code", "")})
                return

            if path == "/gate":
                token = str(data.get("token") or "")
                if not self._owned(token, agent):
                    return
                g = session.gate(token)
                if isinstance(g, Allow):
                    self._send(200, {"verdict": "allow"})
                else:
                    self._send(_deny_status(g), {"verdict": "deny",
                                                 "error": g.reason, "code": g.code})
                return

            if path in ("/finish", "/abort"):
                token = str(data.get("token") or "")
                if not token:
                    self._send(400, {"error": "token_required"})
                    return
                if not self._owned(token, agent):
                    return
                if path == "/finish":
                    res = session.finish(token, outcome=str(data.get("outcome") or "DONE"))
                else:
                    res = session.abort(token, str(data.get("reason") or ""))
                payload = {"status": res.status.value, "detail": res.detail}
                if res.status.value in ("OK", "VALID"):
                    code = 200
                elif res.detail.get("reason") == "unknown_token":
                    code = 404
                else:
                    code = 410
                self._send(code, payload)
                return

            self._send(404, {"error": "not_found"})

    server = _SidecarServer((host, int(port)), _Handler)
    server.sidecar_session = session                        # type: ignore[attr-defined]
    return server


def _deny_status(deny: Deny) -> int:
    if deny.code == "already":
        return 410
    if deny.reason == "unknown_token":
        return 404
    if deny.reason.startswith("undeclared_write"):
        return 400
    if deny.code == "stale_view":
        return 409
    return 403


def serve_in_thread(server: ThreadingHTTPServer) -> threading.Thread:
    """后台线程启动；测试用完 `server.shutdown()` + `server.server_close()`。"""
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return thread


__all__ = ["MAX_BODY", "build_sidecar", "serve_in_thread"]
