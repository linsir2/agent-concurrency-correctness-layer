"""S23：AG2（ag2ai 1.2.x）适配器 —— 工具执行中间件。

1.2.2 实测（`docs/s23-integration-plan.md` §1）：

- `BaseMiddleware.on_tool_execution(call_next, event, context)` 不调 `call_next`
  即**短路**（工具不执行）；
- `ToolCallEvent.id`（uuid4）存在，但幂等仍用业务键（参数哈希）；
- 函数工具从 `event.serialized_arguments` 取参（`tools/final/function_tool.py`），
  所以 root 注入要改这个 dict 的副本，不能只改 `arguments` 字符串。

注册方式：`Agent(..., middleware=[Middleware(RscToolMiddleware, session=..., ...)])`
（`Middleware` 是 per-turn 工厂，`BaseMiddleware.__init__(event, context)`）。
"""

from __future__ import annotations

import copy
from typing import Any

from ..facade import Conflict, Deny, Receipt, RscSession, Token, Wait
from ._common import read_writes, tool_key

try:                                                        # pragma: no cover
    from ag2.events.tool_events import ToolResultEvent
    from ag2.middleware.base import BaseMiddleware
    _HAS_AG2 = True
except ImportError:                                         # pragma: no cover
    _HAS_AG2 = False
    BaseMiddleware = object                                 # type: ignore[assignment,misc]
    ToolResultEvent = None                                  # type: ignore[assignment]


def _read_cwd(paths) -> list[tuple[str, bytes]] | None:
    import os

    from ._common import safe_rel
    out: list[tuple[str, bytes]] = []
    for rel in paths:
        target = safe_rel(os.getcwd(), rel)
        if target is None:
            return None
        try:
            with open(target, "rb") as fh:
                out.append((str(rel), fh.read()))
        except (OSError, ValueError):
            return None
    return out


class RscToolMiddleware(BaseMiddleware):                    # type: ignore[misc]
    """按工具名声明 writes；执行前准入/闸门，执行后读回并 CAS 提交。"""

    def __init__(self, event: Any, context: Any, *, session: RscSession,
                 writes: dict | None = None, agent_id: str = "ag2-agent",
                 root_arg: str = "root", shared_cwd: bool = False) -> None:
        if not _HAS_AG2:                                    # pragma: no cover
            raise ImportError("需要 AG2：pip install 'harness[ag2]'")
        super().__init__(event, context)
        self.session = session
        self.writes = {str(k): tuple(v) for k, v in dict(writes or {}).items()}
        self.agent_id = str(agent_id)
        self.root_arg = str(root_arg)
        self.shared_cwd = bool(shared_cwd)

    async def on_tool_execution(self, call_next, event, context):
        paths = self.writes.get(str(event.name))
        if not paths:                                       # 未声明 → 透传
            return await call_next(event, context)

        # 评审 P1：`root` 由适配器注入并覆盖，不能参与幂等键
        key_args = dict(event.serialized_arguments)
        key_args.pop(self.root_arg, None)
        key = tool_key(str(event.name), key_args)
        tok = self.session.begin(self.agent_id, f"{event.name}:{key}",
                                 writes=paths)
        if isinstance(tok, Wait):
            return ToolResultEvent.from_call(event, f"RSC_WAIT: {tok.reason}")
        if not isinstance(tok, Token):
            return ToolResultEvent.from_call(
                event, f"RSC_DENY: {getattr(tok, 'reason', 'not_granted')}")

        gate = self.session.gate(tok.token)
        if isinstance(gate, Deny):
            self.session.abort(tok.token, "gate_denied")
            return ToolResultEvent.from_call(event, f"RSC_DENY: {gate.reason}")

        worktree = None if self.shared_cwd else self.session.worktree_for(tok.token)
        if not self.shared_cwd:
            if not worktree:
                self.session.abort(tok.token, "worktree_missing")
                return ToolResultEvent.from_call(event, "RSC_DENY: worktree_missing")
            call_event = copy.copy(event)
            args = dict(event.serialized_arguments)
            args[self.root_arg] = worktree
            call_event.serialized_arguments = args
        else:
            call_event = event

        try:
            result = await call_next(call_event, context)
        except BaseException:
            self.session.abort(tok.token, "tool_raised")
            raise

        writes = (_read_cwd(paths) if self.shared_cwd
                  else read_writes(worktree, paths))
        if writes is None:
            self.session.abort(tok.token, "worktree_missing")
            return ToolResultEvent.from_call(event, "RSC_DENY: worktree_missing")

        r = self.session.apply(tok.token, writes, idempotency_key=key,
                               agent_id=self.agent_id)
        if isinstance(r, Receipt):
            self.session.finish(tok.token)
            return result
        self.session.abort(tok.token, "apply_failed")
        if isinstance(r, Conflict):
            return ToolResultEvent.from_call(event, f"RSC_CONFLICT: {r.reason}")
        if isinstance(r, Wait):
            return ToolResultEvent.from_call(event, f"RSC_WAIT: {r.reason}")
        return ToolResultEvent.from_call(
            event, f"RSC_DENY: {getattr(r, 'reason', 'apply_failed')}")


def rsc_middleware(session: RscSession, *, writes: dict,
                   agent_id: str = "ag2-agent", root_arg: str = "root",
                   shared_cwd: bool = False):
    """返回可传给 `Agent(middleware=[...])` 的工厂（`Middleware`）。"""
    if not _HAS_AG2:                                        # pragma: no cover
        raise ImportError("需要 AG2：pip install 'harness[ag2]'")
    from ag2.middleware import Middleware
    return Middleware(RscToolMiddleware, session=session, writes=writes,
                      agent_id=agent_id, root_arg=root_arg,
                      shared_cwd=shared_cwd)


__all__ = ["RscToolMiddleware", "rsc_middleware"]
