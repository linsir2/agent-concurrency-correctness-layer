"""S23：CrewAI 适配器 —— 包装 `BaseTool.run`（硬阻断 + 账本提交）。

为什么不是 hook：1.15.32 实测（`crewai/hooks/dispatch.py:265`）hook 里的普通异常
会被**吞掉（fail-open）**；`HookAborted`/返回 `False` 只让工具不执行，且拒绝原因
被丢弃（`agent_executor.py:2237` 固定文案），agent 继续跑。因此正确性必须放在
**工具执行入口**：CrewAI 原生工具调用实际走 `available_functions[name] = tool.run`
（`utilities/agent_utils.py:303`），包装 `run()` 是唯一同时满足「硬阻断 + 能读回
产物 + 可注入 Z3 根目录」的点。

用法：

```python
from harness.integrations.crewai import rsc_tools
tools = rsc_tools(session, tools=[write_file, search],
                  wraps={"write_file": ("docs/a.md",)}, agent_id="crew-arch")
agent = Agent(role=..., tools=tools)
```

失败语义（写死）：

| 情形 | 工具执行 | 返回给模型 |
|---|---|---|
| `Wait`（预算/租约） | 否 | `"RSC_WAIT: ..."` |
| `Deny`（能力/协调器不可达/token 失效） | 否 | `"RSC_DENY: ..."` |
| `Conflict`（stale write） | 已执行 | `"RSC_CONFLICT: ..."`（账本未写） |
| 工具自身异常 | 已尝试 | 原样抛出（CrewAI 转成 `"Error executing tool"`） |

**诚实边界**：幂等只覆盖账本提交（同键同内容 → 同收据），**不覆盖工具内部
外部副作用**（重试会真的再执行一次工具）。
"""

from __future__ import annotations

import inspect
from typing import Any

from ..facade import Conflict, Deny, Receipt, RscSession, Token, Wait
from ._common import RscDenied, read_writes, safe_rel, tool_key

try:                                                        # pragma: no cover
    from crewai.tools import BaseTool
    _HAS_CREWAI = True
except ImportError:                                         # pragma: no cover
    _HAS_CREWAI = False
    BaseTool = object                                       # type: ignore[assignment,misc]


def _accepts_root(tool: Any, root_arg: str) -> bool:
    """工具是否声明了 `root_arg` 形参（`args_schema` 优先，其次 `_run` 签名）。"""
    schema = getattr(tool, "args_schema", None)
    fields = getattr(schema, "model_fields", None)
    if fields is not None:
        return root_arg in fields
    fn = getattr(type(tool), "_run", None)
    try:
        params = inspect.signature(fn).parameters
    except (TypeError, ValueError):
        return False
    return root_arg in params


def _never_cache(*args: Any, **kwargs: Any) -> bool:
    """CrewAI 缓存开关：恒 False（模块级函数，避免 pydantic 序列化告警）。"""
    return False


class RscTool(BaseTool):                                    # type: ignore[misc]
    """`BaseTool` 代理：执行前准入/闸门，执行后读回 Z3 并 CAS 提交。"""

    name: str = ""
    description: str = ""
    orig: Any = None
    session: Any = None
    writes: tuple[str, ...] = ()
    root_arg: str = "root"
    agent_id: str = ""
    shared_cwd: bool = False

    def _run(self, *args: Any, **kwargs: Any) -> Any:       # pragma: no cover
        raise NotImplementedError("RscTool 覆写 run，不直接走 _run")

    def run(self, *args: Any, **kwargs: Any) -> Any:
        if not self.writes:                                  # 未声明 → 原样透传
            return self.orig.run(*args, **kwargs)
        inject = _accepts_root(self.orig, self.root_arg)
        if not inject and not self.shared_cwd:
            return f"RSC_DENY: tool_must_accept_root({self.root_arg})"

        # 评审 P1：`root` 会被适配器覆盖，不参与幂等键（否则换 root 同内容不去重）
        key_kwargs = dict(kwargs)
        key_kwargs.pop(self.root_arg, None)
        key = tool_key(self.name, {"args": list(args), "kwargs": key_kwargs})
        tok = self.session.begin(self.agent_id, f"{self.name}:{key}",
                                 writes=self.writes)
        if isinstance(tok, Wait):
            return f"RSC_WAIT: {tok.reason}"
        if not isinstance(tok, Token):
            return f"RSC_DENY: {getattr(tok, 'reason', 'not_granted')}"

        committed = False
        try:
            gate = self.session.gate(tok.token)
            if isinstance(gate, Deny):
                return f"RSC_DENY: {gate.reason}"

            call_kwargs = dict(kwargs)
            base_dir = tok.worktree
            if inject:
                call_kwargs[self.root_arg] = tok.worktree
            else:
                base_dir = None                              # 共享 cwd 模式
            result = self.orig.run(*args, **call_kwargs)

            if base_dir is None:
                writes = _read_from_cwd(self.writes)
            else:
                writes = read_writes(base_dir, self.writes)
            if writes is None:
                return "RSC_DENY: worktree_missing"

            r = self.session.apply(tok.token, writes,
                                   idempotency_key=key, agent_id=self.agent_id)
            if isinstance(r, Receipt):
                committed = True
                return result
            if isinstance(r, Conflict):
                return f"RSC_CONFLICT: {r.reason}"
            if isinstance(r, Wait):
                return f"RSC_WAIT: {r.reason}"
            return f"RSC_DENY: {getattr(r, 'reason', 'apply_failed')}"
        finally:
            # 成功 → 结算；其余（拒绝/冲突/工具异常）→ 逆序撤销（幂等）。
            if committed:
                self.session.finish(tok.token)
            else:
                self.session.abort(tok.token, "tool_not_committed")


def _read_from_cwd(paths) -> list[tuple[str, bytes]] | None:
    """共享 cwd 模式：从进程当前目录读产物（仅显式 `shared_cwd=True` 时使用）。"""
    import os
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


def rsc_tools(session: RscSession, tools, *, wraps: dict | None = None,
              agent_id: str = "crew-agent", root_arg: str = "root",
              shared_cwd: bool = False) -> list:
    """包装声明了 writes 的工具；未声明的原样返回（`unsupported`，不假装覆盖）。

    `wraps`: `{工具名: (相对路径, ...)}`；也可在工具类上设 `rsc_writes` 类属性。
    """
    if not _HAS_CREWAI:                                     # pragma: no cover
        raise ImportError("需要 CrewAI：pip install 'harness[crewai]'")
    wraps = dict(wraps or {})
    out = []
    for tool in tools:
        name = str(getattr(tool, "name", ""))
        paths = tuple(wraps.get(name) or getattr(tool, "rsc_writes", ()) or ())
        if not paths:
            out.append(tool)
            continue
        out.append(RscTool(
            name=name,
            description=str(getattr(tool, "description", "")),
            args_schema=getattr(tool, "args_schema", None),
            # 评审 P2：CrewAI 缓存在 `tool.run` **之上**命中（命中则 wrapper 不被调用），
            # 会让「账本无提交但模型看到 ok」→ 显式禁用。
            cache_function=_never_cache,
            orig=tool, session=session, writes=paths, root_arg=root_arg,
            agent_id=str(agent_id), shared_cwd=bool(shared_cwd)))
    return out


__all__ = ["RscDenied", "RscTool", "rsc_tools"]
