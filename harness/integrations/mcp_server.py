"""S22：MCP server —— 任何 MCP 客户端（Claude / Cursor / VS Code）零 Python 集成。

设计见 `docs/s22-implementation-plan.md` §12.4（评审修订版）：

- **收敛为少量幂等工具**，不暴露 5 步流程（模型不会可靠地按顺序调用）。
- `agent_id` **只由 server 配置**，任何工具都不接受调用方声明（防冒名、
  防误删他人 Z3，评审 P0）。
- `rsc_begin(writes=...)` 必填 → OCC 才有钉住点（否则 gate 恒 allow）。
- 实现分两层：`make_tools(session)` 返回**模块级可测函数**（不走 stdio 也能测），
  `build_mcp_server(session)` 用 `FastMCP` 装饰器包装。
"""

from __future__ import annotations

import json

try:                                                        # pragma: no cover
    from mcp.server.fastmcp import FastMCP
    _HAS_MCP = True
except ImportError:                                         # pragma: no cover
    _HAS_MCP = False
    FastMCP = None                                          # type: ignore[assignment]

from ..facade import Conflict, Deny, Receipt, RscSession, Token
from ..rebase import DELETE


def make_tools(session: RscSession, *, agent_id: str = "mcp-agent") -> dict:
    """返回可测的 MCP 工具函数（不依赖 FastMCP，便于单元测试）。"""
    aid = str(agent_id)

    def _ownership_ok(token: str) -> bool:
        """token 归属校验（S7）。

        - `owner is None`（token 不存在）→ **True**，保持原有错误路径
          （`unknown_token`），不把它伪装成归属错误；
        - `owner != aid` → False，由调用方返回 `token_owner_mismatch`
          （与 HTTP sidecar 的 403 同语义，只是传输层不同）。
        """
        owner = session.owner_of(str(token))
        return owner is None or owner == aid

    _MISMATCH = {"error": "token_owner_mismatch"}

    def rsc_apply(resource: str, content=None,
                  idempotency_key: str = "") -> dict:
        """一次性提交一个资源写入（无 token 会话）。content=None 表示删除。"""
        session.tick()
        if not str(resource or ""):
            return {"error": "resource_required"}
        if not str(idempotency_key or ""):
            return {"error": "idempotency_key_required"}
        tok = session.begin(aid, f"{aid}:{resource}", writes=(resource,))
        if not isinstance(tok, Token):
            return {"error": getattr(tok, "reason", "not_granted")}
        writes = [(resource, DELETE if content is None else content)]
        r = session.apply(tok.token, writes, idempotency_key=idempotency_key,
                          agent_id=aid)
        if isinstance(r, Receipt):
            session.finish(tok.token)
            return {"receipt_id": r.receipt_id, "seq": r.seq,
                    "dedup": r.dedup, "verified": r.verified}
        session.abort(tok.token)
        if isinstance(r, Conflict):
            return {"conflict": r.reason}
        return {"error": getattr(r, "reason", "apply_failed")}

    def rsc_write(token: str, resource: str, content=None,
                  idempotency_key: str = "") -> dict:
        """在已有会话（rsc_begin）内提交一次写。"""
        session.tick()
        if not _ownership_ok(token):
            return dict(_MISMATCH)
        writes = [(resource, DELETE if content is None else content)]
        r = session.apply(str(token), writes, idempotency_key=idempotency_key,
                          agent_id=aid)
        if isinstance(r, Receipt):
            return {"receipt_id": r.receipt_id, "seq": r.seq,
                    "dedup": r.dedup, "verified": r.verified}
        if isinstance(r, Conflict):
            return {"conflict": r.reason}
        return {"error": getattr(r, "reason", "apply_failed")}

    def rsc_begin(writes: list) -> dict:
        """开一个会话（多步写之前）。`writes` 必填 —— 它是 OCC 的钉住点。"""
        session.tick()
        tok = session.begin(aid, f"{aid}:session", writes=tuple(writes or ()))
        if not isinstance(tok, Token):
            return {"error": getattr(tok, "reason", "not_granted")}
        return {"token": tok.token}

    def rsc_finish(token: str, outcome: str = "DONE") -> dict:
        """结束会话（结算 + 释放 + 销毁 Z3）。"""
        session.tick()
        if not _ownership_ok(token):
            return dict(_MISMATCH)
        res = session.finish(str(token), outcome=outcome)
        return {"status": res.status.value, "detail": res.detail}

    def rsc_gate(token: str) -> dict:
        """外部副作用（发邮件/开 PR）前的读集校验。"""
        session.tick()
        if not _ownership_ok(token):
            return dict(_MISMATCH)
        g = session.gate(str(token))
        return ({"verdict": "deny", "reason": g.reason, "code": g.code}
                if isinstance(g, Deny) else {"verdict": "allow"})

    def rsc_snapshot(token: str) -> dict:
        """当前 base_versions（供宿主做上下文）。"""
        session.tick()
        if not _ownership_ok(token):
            return dict(_MISMATCH)
        s = session.snapshot(str(token))
        return {"error": s.reason} if isinstance(s, Deny) else dict(s)

    return {"rsc_apply": rsc_apply, "rsc_write": rsc_write,
            "rsc_begin": rsc_begin, "rsc_finish": rsc_finish,
            "rsc_gate": rsc_gate, "rsc_snapshot": rsc_snapshot}


def build_mcp_server(session: RscSession, *, name: str = "rsc",
                     agent_id: str = "mcp-agent"):
    """用 FastMCP 包装工具 + resource（需要 `pip install 'harness[mcp]'`）。"""
    if not _HAS_MCP:                                        # pragma: no cover
        raise ImportError("需要 MCP SDK：pip install 'harness[mcp]'")
    tools = make_tools(session, agent_id=agent_id)
    mcp = FastMCP(name)

    for tool_name, fn in tools.items():
        mcp.tool(name=tool_name, description=(fn.__doc__ or "").strip())(fn)

    @mcp.resource("rsc://snapshot/{token}")
    def _snapshot(token: str) -> str:
        """token 的 base_versions（JSON）。"""
        session.tick()
        out = session.snapshot(str(token))
        return json.dumps(out) if isinstance(out, dict) else json.dumps(
            {"error": out.reason})

    return mcp
