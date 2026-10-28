"""S22：LangGraph 适配器（`AgentMiddleware`）——评审修订版。

设计见 `docs/s22-implementation-plan.md` §12.3：

- **token = agent 会话**：`before_agent` 建 token + Z3；`after_agent` 结算；
  `wrap_tool_call` 只做 gate / root 注入 / 读回 / apply。
- **`writes` 必须声明**（构造参数）：它是 OCC 的唯一钉住时机。漏传 →
  `_check_gate` 对未钉住的资源**硬拒绝**（fail-closed），绝不静默覆盖。
- **root 注入是显式 opt-in**：`@rsc_writes("src/a.py", root_arg="root")`
  且工具签名**确实接受** `root`；否则 passthrough（LangChain 会静默吞掉
  未声明的参数 → 必须显式校验）。
- **sync + async 双实现**（基类 `awrap_tool_call` 默认抛 NotImplementedError）。
- 失败映射：`Deny` → 抛 `RscDenied`；`Wait` → `ToolMessage(error)`；
  `Conflict` → `Command`（带同一 `tool_call_id`）。

`agent_id` 约定：默认 `runtime.context["agent_id"]` → `config["thread_id"]`
→ `"default"`。**多 agent 节点必须各自设置 `context.agent_id`**，否则共享一个
token/worktree（失去隔离）。
"""

from __future__ import annotations

import hashlib
import inspect
import os
import uuid
from collections.abc import Callable
from typing import Any

try:                                                        # pragma: no cover
    from langchain.agents.middleware import AgentMiddleware
    from langchain_core.messages import ToolMessage
    from langgraph.types import Command
    _HAS_LANGCHAIN = True
except ImportError:                                         # pragma: no cover
    _HAS_LANGCHAIN = False
    AgentMiddleware = object                                # type: ignore[assignment]

from ..facade import Conflict, Deny, RscSession, Token, Wait
from ..rebase import DELETE
from ._common import RscDenied


def rsc_writes(*paths: str, root_arg: str | None = None):
    """声明工具会写哪些**相对 worktree 的路径**（可选 `root_arg` 注入根目录）。"""

    def deco(fn):
        fn.__rsc_writes__ = tuple(paths)
        fn.__rsc_root_arg__ = root_arg
        return fn

    return deco


def _attr(obj: Any, name: str, default=None):
    for target in (getattr(obj, "func", None), obj):
        if target is not None and hasattr(target, name):
            value = getattr(target, name)
            if value is not None:
                return value
    return default


class RscMiddleware(AgentMiddleware):
    """把工具调用包进 RSC（gate → 注入 root → 执行 → 读回 → apply）。"""

    def __init__(self, session: RscSession, *,
                 agent_id_from: Callable | None = None,
                 writes: tuple[str, ...] = ()) -> None:
        if not _HAS_LANGCHAIN:                               # pragma: no cover
            raise ImportError("需要 LangGraph：pip install 'harness[langgraph]'")
        if session.worktrees is None:
            raise ValueError("RscMiddleware 需要 worktrees（Z3 私有工作区）")
        super().__init__()
        self.s = session
        self._agent_id_from = agent_id_from or self._default_agent_id
        # 本 agent 会写哪些资源（各工具 @rsc_writes 的并集）。
        self._writes = tuple(writes)

    # ---- 生命周期 -------------------------------------------------
    @staticmethod
    def _default_agent_id(state, runtime) -> str:
        # 注意：`before_agent` 的 runtime 是中间件 Runtime，**没有 config**；
        # `thread_id` 只在 `wrap_tool_call` 的 ToolRuntime 上有。
        ctx = getattr(runtime, "context", None)
        if isinstance(ctx, dict) and ctx.get("agent_id"):
            return str(ctx["agent_id"])
        if isinstance(state, dict) and state.get("agent_id"):
            return str(state["agent_id"])
        return "default"

    def before_agent(self, state, runtime) -> dict | None:
        aid = self._agent_id_from(state, runtime)
        tok = self.s.begin(aid, f"{aid}:{uuid.uuid4().hex[:8]}",
                           writes=self._writes)
        if isinstance(tok, Token):
            return {"rsc_token": tok.token, "rsc_agent": aid}
        return {"rsc_token": ""}

    def after_agent(self, state, runtime) -> dict | None:
        tok = state.get("rsc_token") if isinstance(state, dict) else None
        if tok:
            self.s.finish(tok)
        return None

    # ---- 工具调用 -------------------------------------------------
    def _token_of(self, request) -> str:
        """token 只来自 `before_agent` 写入的 state —— **不懒建**。

        懒建会在「首次工具调用」时才钉 base_version：若先写者已提交，后写者
        钉到新版本 → OCC 归零（评审 P1-1）。因此没有会话就 fail-closed。
        """
        state = request.state
        if isinstance(state, dict) and state.get("rsc_token"):
            return str(state["rsc_token"])
        return ""

    def _agent_of(self, request) -> str:
        try:
            return self._agent_id_from(request.state, request.runtime)
        except Exception:  # noqa: BLE001
            return "default"

    def _paths_of(self, request) -> tuple[str, ...]:
        paths = _attr(request.tool, "__rsc_writes__", ())
        return tuple(paths) if paths else ()

    def _accepts_root(self, request) -> str | None:
        """声明了 `root_arg` 且工具签名确实接受 → 返回该参数名，否则 None。"""
        arg = _attr(request.tool, "__rsc_root_arg__", None)
        if not arg:
            return None
        fn = getattr(request.tool, "func", None) or request.tool
        try:
            params = inspect.signature(fn).parameters
        except (TypeError, ValueError):
            return None
        return arg if arg in params else None

    def _worktree_ok(self, token: str) -> bool:
        root = self.s.worktree_for(token)
        return bool(root) and os.path.isdir(root)

    def _read_back(self, token: str, paths) -> list | None:
        """读回声明路径。`None` = **拒绝**（worktree 缺失/路径逃逸），绝不发 DELETE。"""
        if not self._worktree_ok(token):
            return None
        root = self.s.worktree_for(token)
        out = []
        for rel in paths:
            try:
                content = self.s.worktrees.read(root, rel)
            except (ValueError, OSError):
                return None                  # 评审 P1-6：逃逸/IO 错误 → fail-closed
            if content is None:
                if self.s.ledger_get(rel) is not None:
                    out.append((rel, DELETE))
            else:
                out.append((rel, content))
        return out

    def _check_gate(self, token: str, paths) -> Conflict | Deny | None:
        """返回 None=放行 / Conflict=可重试冲突 / Deny=硬拒绝。"""
        acb = self.s.resolve(token)
        if isinstance(acb, Deny):
            return acb
        for rel in paths:
            if rel not in acb.base_version:
                # 评审 P1-8：未声明 writes → 硬拒绝（不补钉、不静默放行）。
                return Deny(f"unpinned:{rel}")
        gate = self.s.gate(token)
        if isinstance(gate, Deny):
            if gate.code == "stale_view":
                return Conflict(gate.reason)
            return gate
        return None

    @staticmethod
    def _map_result(tool_call_id: str, r) -> Any:
        """把 apply 结果映射成中间件返回值；Receipt → None（返回工具结果）。"""
        if isinstance(r, Deny):
            raise RscDenied(f"RSC_DENY: {r.reason}")
        if isinstance(r, Wait):
            return ToolMessage(content=f"RSC_WAIT: {r.reason}",
                               tool_call_id=tool_call_id, status="error")
        if isinstance(r, Conflict):
            return Command(update={"messages": [ToolMessage(
                content=f"RSC_CONFLICT: {r.reason}",
                tool_call_id=tool_call_id, status="error")]})
        return None                            # Receipt

    def _run(self, request, handler) -> Any:
        paths = self._paths_of(request)
        if not paths:                            # 未声明 → 直接放行
            return handler(request)
        root_arg = self._accepts_root(request)
        if _attr(request.tool, "__rsc_root_arg__", None) and not root_arg:
            return handler(request)              # 工具不接 root → passthrough
        token = self._token_of(request)
        if not token:
            raise RscDenied("RSC_DENY: no_session（请用 before_agent 建会话）")
        if not self._worktree_ok(token):
            raise RscDenied("RSC_DENY: worktree_missing")
        gate = self._check_gate(token, paths)
        if isinstance(gate, Conflict):
            return Command(update={"messages": [ToolMessage(
                content=f"RSC_CONFLICT: {gate.reason}",
                tool_call_id=request.tool_call.get("id"), status="error")]})
        if isinstance(gate, Deny):
            raise RscDenied(f"RSC_DENY: {gate.reason}")

        tc = dict(request.tool_call)
        if root_arg:
            args = dict(tc.get("args") or {})
            args[root_arg] = self.s.worktree_for(token)
            request = self._override_args(request, tc, args)

        result = handler(request)
        writes = self._read_back(token, paths)
        if writes is None:
            raise RscDenied("RSC_DENY: worktree_missing_or_bad_path")
        if not writes:
            return result
        key = self._idempotency_key(request, paths, tc)
        mapped = self._map_result(tc.get("id"),
                                  self.s.apply(token, writes,
                                               idempotency_key=key,
                                               agent_id=self._agent_of(request)))
        return result if mapped is None else mapped

    def wrap_tool_call(self, request, handler):
        return self._run(request, handler)

    async def awrap_tool_call(self, request, handler):
        paths = self._paths_of(request)
        if not paths:
            return await handler(request)
        root_arg = self._accepts_root(request)
        if _attr(request.tool, "__rsc_root_arg__", None) and not root_arg:
            return await handler(request)
        token = self._token_of(request)
        if not token:
            raise RscDenied("RSC_DENY: no_session（请用 before_agent 建会话）")
        if not self._worktree_ok(token):
            raise RscDenied("RSC_DENY: worktree_missing")
        gate = self._check_gate(token, paths)
        if isinstance(gate, Conflict):
            return Command(update={"messages": [ToolMessage(
                content=f"RSC_CONFLICT: {gate.reason}",
                tool_call_id=request.tool_call.get("id"), status="error")]})
        if isinstance(gate, Deny):
            raise RscDenied(f"RSC_DENY: {gate.reason}")
        tc = dict(request.tool_call)
        if root_arg:
            args = dict(tc.get("args") or {})
            args[root_arg] = self.s.worktree_for(token)
            request = self._override_args(request, tc, args)
        result = await handler(request)
        writes = self._read_back(token, paths)
        if writes is None:
            raise RscDenied("RSC_DENY: worktree_missing_or_bad_path")
        if not writes:
            return result
        mapped = self._map_result(
            tc.get("id"),
            self.s.apply(token, writes,
                         idempotency_key=self._idempotency_key(request, paths, tc),
                         agent_id=self._agent_of(request)))
        return result if mapped is None else mapped

    def _idempotency_key(self, request, paths, tc) -> str:
        # 用 \x1f 分隔，避免路径里的逗号造成碰撞（评审 P2）。
        raw = (f"{self.s.tenant}|{self._agent_of(request)}|"
               + "\x1f".join(paths) + f"|{tc.get('id', '')}")
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    @staticmethod
    def _override_args(request, tool_call: dict, args: dict):
        """不可变替换 tool_call.args（`ToolCallRequest.override` 优先）。"""
        new_call = {**tool_call, "args": args}
        if hasattr(request, "override"):
            return request.override(tool_call=new_call)
        import dataclasses
        if dataclasses.is_dataclass(request):
            return dataclasses.replace(request, tool_call=new_call)
        return type(request)(**{**request.__dict__, "tool_call": new_call})
