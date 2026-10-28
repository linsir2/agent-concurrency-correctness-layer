"""S23：OpenAI Agents SDK 适配器（guardrails + root 注入）。

0.13.6 实测结论（`docs/s23-integration-plan.md` §1）：

- `ToolContext.tool_call_id` / `tool_arguments` 必填 → 幂等键可用业务键 + 审计定位；
- `FunctionTool` 是 dataclass → `dataclasses.replace` 注入 guardrail / 改 schema / 包
  `on_invoke_tool` 都不改原对象；
- `raise_exception` → `ToolInputGuardrailTripwireTriggered`（硬拒，工具不执行）；
  `reject_content` → 文本回模型、工具不执行（软拒，可排队/换策略）；
- output guardrail 在工具执行**之后**：适合做「读回 + apply」，拒绝只能改返回文本。

用法：

```python
from harness.integrations.openai_agents import rsc_agent
agent = rsc_agent(session, agent, writes={"write_file": ("docs/a.md",)})
```

`root` 参数会从工具 schema 中**移除**（模型不必知道），由适配器在执行前注入
worktree；若工具既没有 `root` 也未显式 `shared_cwd=True` → 构造即 `ValueError`
（fail-closed，不静默放行）。
"""

from __future__ import annotations

import dataclasses
import json
import threading
from typing import Any

from ..facade import Conflict, Deny, Receipt, RscSession, Token, Wait
from ._common import read_writes, tool_key

try:                                                        # pragma: no cover
    from agents import Agent, FunctionTool
    from agents.tool_context import ToolContext
    from agents.tool_guardrails import (
        ToolGuardrailFunctionOutput,
        ToolInputGuardrail,
        ToolInputGuardrailData,
        ToolOutputGuardrail,
        ToolOutputGuardrailData,
    )
    _HAS_AGENTS = True
except ImportError:                                         # pragma: no cover
    _HAS_AGENTS = False
    Agent = FunctionTool = ToolContext = object             # type: ignore[assignment,misc]
    ToolGuardrailFunctionOutput = ToolInputGuardrail = None  # type: ignore[assignment]
    ToolInputGuardrailData = ToolOutputGuardrail = None      # type: ignore[assignment]
    ToolOutputGuardrailData = None                           # type: ignore[assignment]


def _allow():
    return ToolGuardrailFunctionOutput(output_info=None,
                                       behavior={"type": "allow"})


def _reject(message: str):
    return ToolGuardrailFunctionOutput(
        output_info={"reason": message},
        behavior={"type": "reject_content", "message": message})


def _trip(reason: str):
    return ToolGuardrailFunctionOutput(output_info={"reason": reason},
                                       behavior={"type": "raise_exception"})


def _parsed_args(raw: str) -> dict | None:
    """规范化前的 JSON 解析；非 object / 非法 JSON → None（调用方 fail-closed）。"""
    try:
        obj = json.loads(raw or "{}")
    except (TypeError, ValueError):
        return None
    return obj if isinstance(obj, dict) else None


def _strip_param(schema: dict, name: str) -> dict:
    """从 JSON schema 移除 `name` 参数（模型不必提供，由适配器注入）。"""
    out = json.loads(json.dumps(schema or {}))
    props = out.get("properties")
    if isinstance(props, dict):
        props.pop(name, None)
    required = out.get("required")
    if isinstance(required, list) and name in required:
        out["required"] = [x for x in required if x != name]
    return out


def _wrap_function_tool(session: RscSession, tool: Any, paths: tuple[str, ...],
                        *, agent_id: str, root_arg: str, shared_cwd: bool):
    tokens: dict[str, tuple[str, str]] = {}
    lock = threading.Lock()
    original_invoke = tool.on_invoke_tool
    schema_props = (tool.params_json_schema or {}).get("properties") or {}
    inject = root_arg in schema_props
    if not inject and not shared_cwd:
        raise ValueError(
            f"工具 {tool.name!r} 必须声明 {root_arg!r} 参数，或显式 shared_cwd=True")

    async def input_guard(data):
        ctx = data.context
        call_id = str(getattr(ctx, "tool_call_id", "") or "")
        if not call_id:                                     # fail-closed
            return _trip("missing_tool_call_id")
        parsed = _parsed_args(getattr(ctx, "tool_arguments", "") or "")
        if parsed is None:
            return _trip("bad_arguments")
        # 评审 P1：`root` 会被适配器覆盖，不能参与幂等键（否则换 root 不去重）
        key_args = dict(parsed)
        key_args.pop(root_arg, None)
        key = tool_key(tool.name, key_args)
        tok = session.begin(agent_id, f"{tool.name}:{key}", writes=paths)
        if isinstance(tok, Wait):
            return _reject(f"RSC_WAIT: {tok.reason}")
        if not isinstance(tok, Token):
            return _trip(f"RSC_DENY: {getattr(tok, 'reason', 'not_granted')}")
        gate = session.gate(tok.token)
        if isinstance(gate, Deny):
            session.abort(tok.token, "gate_denied")
            return _trip(f"RSC_DENY: {gate.reason}")
        with lock:
            tokens[call_id] = (tok.token, key)
        return _allow()

    async def output_guard(data):
        ctx = data.context
        call_id = str(getattr(ctx, "tool_call_id", "") or "")
        with lock:
            entry = tokens.pop(call_id, None)
        if entry is None:                                   # 无 token → 不得静默放行
            return _trip("missing_session")
        token, key = entry
        writes = (_read_cwd(paths) if shared_cwd
                  else read_writes(session.worktree_for(token), paths))
        if writes is None:
            session.abort(token, "worktree_missing")
            return _trip("RSC_DENY: worktree_missing")
        r = session.apply(token, writes, idempotency_key=key, agent_id=agent_id)
        if isinstance(r, Receipt):
            session.finish(token)
            return _allow()
        session.abort(token, "apply_failed")
        if isinstance(r, Conflict):
            return _reject(f"RSC_CONFLICT: {r.reason}")
        if isinstance(r, Wait):
            return _reject(f"RSC_WAIT: {r.reason}")
        return _trip(f"RSC_DENY: {getattr(r, 'reason', 'apply_failed')}")

    async def invoke(ctx: Any, input_json: str) -> Any:
        call_id = str(getattr(ctx, "tool_call_id", "") or "")
        if not shared_cwd:
            with lock:
                entry = tokens.get(call_id)
            worktree = session.worktree_for(entry[0]) if entry else None
            if not worktree:
                raise RuntimeError("RSC_DENY: worktree_missing")
            parsed = _parsed_args(input_json)
            if parsed is None:
                _abandon(call_id)
                raise ValueError("RSC_DENY: bad_arguments")
            parsed[root_arg] = worktree
            input_json = json.dumps(parsed)
        try:
            return await original_invoke(ctx, input_json)
        except BaseException:
            # 评审 P1：SDK 在工具抛错时**不会**跑 output guardrail → 这里必须撤销，
            # 否则 token/预算/槽位要等 TTL reaper 才回收。
            _abandon(call_id)
            raise

    def _abandon(call_id: str) -> None:
        with lock:
            entry = tokens.pop(call_id, None)
        if entry is not None:
            session.abort(entry[0], "tool_raised")

    return dataclasses.replace(
        tool,
        on_invoke_tool=invoke,
        params_json_schema=(_strip_param(tool.params_json_schema, root_arg)
                            if inject else tool.params_json_schema),
        tool_input_guardrails=[
            ToolInputGuardrail(guardrail_function=input_guard, name="rsc_input")],
        tool_output_guardrails=[
            ToolOutputGuardrail(guardrail_function=output_guard, name="rsc_output")])


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


def rsc_agent(session: RscSession, agent, *, writes: dict,
              agent_id: str | None = None, root_arg: str = "root",
              shared_cwd: bool = False):
    """返回注入了 RSC guardrail 的**新 Agent**（原 agent 不变）。"""
    if not _HAS_AGENTS:                                     # pragma: no cover
        raise ImportError("需要 OpenAI Agents SDK：pip install 'harness[openai-agents]'")
    aid = str(agent_id or getattr(agent, "name", "") or "openai-agent")
    wraps = {str(k): tuple(v) for k, v in dict(writes or {}).items()}
    tools = []
    for tool in list(getattr(agent, "tools", []) or []):
        paths = wraps.get(str(getattr(tool, "name", "")))
        if not paths or not isinstance(tool, FunctionTool):
            tools.append(tool)                              # 未声明 / 非函数工具 → 透传
            continue
        tools.append(_wrap_function_tool(session, tool, paths, agent_id=aid,
                                         root_arg=root_arg, shared_cwd=shared_cwd))
    return dataclasses.replace(agent, tools=tools)


__all__ = ["rsc_agent"]
