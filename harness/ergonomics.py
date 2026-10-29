"""S24-P1：框架无关的「一行接入」壳（`@rsc_tool` / `rsc_task`）。

**定位**：强制内核（`RscSession`）不变；本模块只解决接入体验。工具函数不需要
知道 token / 版本 / 幂等键，装饰器把它翻译成 RSC 的动作序列：

    begin（钉版本）→ 注入私有 worktree → 执行函数 → 读回声明产物
    → 可选 verify → apply（CAS + 幂等键）→ finish

两种用法：

```python
with rsc_session(session, agent_id="agent-a"):     # 单次工具调用：自动 begin/apply/finish
    edit_config({"a": 1})

with rsc_task(session, "agent-b", writes=["a.md"]) as task:   # 长任务：一个 token 多次 apply
    task.write("a.md", "v1")
```

**失败语义**（与门面一一对应，不吞、不降级）：

| 门面结果 | 本模块 |
|---|---|
| `Receipt` | 正常返回函数值；`ctx.receipt` / `last_receipt()` 可取 |
| `Conflict` | `retry>0` 自动重新 begin 重跑（有界）；否则抛 `RscConflict` |
| `Deny` | 抛 `RscToolDenied`（硬拒绝，调用方不应重试） |
| `Wait` | 抛 `RscDeferred`（排队，调用方稍后重试） |
"""

from __future__ import annotations

import contextlib
import contextvars
import functools
import hashlib
import inspect
import json
import os
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .facade import Conflict, Deny, Receipt, RscSession, Token, Wait
from .integrations._common import safe_rel, tool_key
from .rebase import DELETE, MISSING
from .types import Status

__all__ = [
    "RscConflict",
    "RscContext",
    "RscDeferred",
    "RscToolDenied",
    "current_context",
    "last_receipt",
    "rsc_session",
    "rsc_task",
    "rsc_tool",
]


# ---------------------------------------------------------------- 异常
class RscToolDenied(RuntimeError):
    """硬拒绝（语义冲突 / 未声明写 / 验证红）：重试无意义，必须改代码或改内容。"""

    def __init__(self, code: str, reason: str = "") -> None:
        self.code = str(code)
        self.reason = str(reason)
        super().__init__(f"{self.code}: {self.reason}" if self.reason else self.code)


class RscConflict(RuntimeError):
    """版本过期（CAS 拒绝）：重读最新值后重试。`retry>0` 时由装饰器自动重试。"""

    def __init__(self, reason: str = "") -> None:
        self.reason = str(reason)
        super().__init__(f"conflict: {self.reason}" if self.reason else "conflict")


class RscDeferred(RuntimeError):
    """准入失败（预算/并发/前驱）：这是**排队**信号，不是错误。"""

    def __init__(self, reason: str = "") -> None:
        self.reason = str(reason)
        super().__init__(f"deferred: {self.reason}" if self.reason else "deferred")


# ---------------------------------------------------------------- 上下文
@dataclass
class RscContext:
    """一次 begin 对应的上下文：私有工作区 + 已声明写集 + 收据。"""

    session: RscSession
    agent_id: str
    token: str
    worktree: str | None
    writes: tuple[str, ...] = ()
    reads: tuple[str, ...] = ()
    intent: str = ""
    tool: str = ""                 # #2：工具名（能力 tokens 的 tool 维度）
    receipt: Receipt | None = None
    attempts: int = 1
    _pending: dict[str, Any] = field(default_factory=dict)

    # ---- 路径 / 写入 -------------------------------------------------
    def path(self, rel: str) -> str:
        """`rel` 在私有 worktree 内的绝对路径（越界 → ValueError）。"""
        if not self.worktree:
            raise RuntimeError(
                "当前没有私有工作区（session.worktrees=None）；"
                "请用 ctx.write(rel, content) 直接声明内容")
        target = safe_rel(self.worktree, rel)
        if target is None:
            raise ValueError(f"路径逃逸：{rel!r}")
        Path(target).parent.mkdir(parents=True, exist_ok=True)
        return target

    def write(self, rel: str, content) -> None:
        """写私有工作区并登记为本次提交内容（`rel` 必须在 `writes` 里）。"""
        rel = self._declared(rel)
        data = content.encode("utf-8") if isinstance(content, str) else bytes(content)
        if self.worktree:
            target = self.path(rel)
            with open(target, "wb") as fh:
                fh.write(data)
        self._pending[rel] = data

    def delete(self, rel: str) -> None:
        """登记删除（墓碑）；文件存在则同时从私有工作区移除。"""
        rel = self._declared(rel)
        if self.worktree:
            target = safe_rel(self.worktree, rel)
            if target and os.path.isfile(target):
                os.unlink(target)
        self._pending[rel] = DELETE

    def _declared(self, rel: str) -> str:
        """未声明的写 → 立即失败（静默丢弃会让「并发检测」形同虚设）。"""
        rel = str(rel)
        if rel not in self.writes:
            raise RscToolDenied(
                "undeclared_write",
                f"{rel!r} 不在 writes={list(self.writes)} 里；"
                "请在 begin / @rsc_tool 时声明")
        return rel

    def read(self, rel: str) -> bytes | None:
        """读**本次 begin 钉住的那一版**（不是最新版）。

        顺序：私有 worktree 文件 → 该版本的 pre_image → 账本最新值。
        钉住版本读不到（版本 0 / 已删除）→ None。
        """
        if self.worktree:
            target = safe_rel(self.worktree, rel)
            if target and os.path.isfile(target):
                return Path(target).read_bytes()
        workspace = self.session.h.workspace
        base = self.session.snapshot(self.token)
        pinned = int((base or {}).get(rel, 0)) if isinstance(base, dict) else 0
        if pinned <= 0:
            return None                     # 本次 begin 时该资源还不存在
        value = workspace.pre_image(rel, pinned)      # 该版本的快照（实测语义）
        if value is MISSING:                # 历史被裁剪/非提交路径 → 退回最新
            value = self.session.ledger_get(rel)
        if value is None or value is DELETE:
            return None
        return _to_bytes(value)

    def apply(self, *, key: str = "") -> Any:
        """把已登记内容提交（长任务里多次调用 → 多条版本）。返回门面结果。"""
        pending = self.collect()
        if not pending:
            raise RscToolDenied("no_output", "没有待提交内容（先 ctx.write/delete）")
        res = self.session.apply(self.token, pending,
                                 idempotency_key=key or _content_key(pending),
                                 agent_id=self.agent_id, tool=self.tool)
        if isinstance(res, Receipt):
            self.receipt = res
            self._pending.clear()
            _LAST.set(self)
        return res

    # ---- 内部 -------------------------------------------------------
    def collect(self) -> list[tuple[str, Any]]:
        """汇总待提交内容：`write/delete` 显式登记优先，其次读回声明路径。"""
        out: list[tuple[str, Any]] = []
        for rel in self.writes:
            if rel in self._pending:
                out.append((rel, self._pending[rel]))
                continue
            if self.worktree:
                target = safe_rel(self.worktree, rel)
                if target and os.path.isfile(target):
                    out.append((rel, Path(target).read_bytes()))
        return out


@dataclass
class _Binding:
    session: RscSession
    agent_id: str
    task: RscContext | None = None


_BINDING: contextvars.ContextVar[_Binding | None] = contextvars.ContextVar(
    "rsc_binding", default=None)
_LAST: contextvars.ContextVar[RscContext | None] = contextvars.ContextVar(
    "rsc_last", default=None)


def current_context() -> RscContext | None:
    """当前绑定的任务上下文（无 → None）；工具函数可用它拿 token/worktree。"""
    binding = _BINDING.get()
    return binding.task if binding is not None else None


def last_receipt() -> Receipt | None:
    """最近一次成功提交的收据（单次工具调用模式下取结果用）。"""
    ctx = _LAST.get()
    return ctx.receipt if ctx is not None else None


def _require_binding() -> _Binding:
    binding = _BINDING.get()
    if binding is None:
        raise RuntimeError(
            "没有绑定 RscSession：请用 `with rsc_session(session, agent_id=...)`"
            " 或 `with rsc_task(session, agent_id, writes=[...])` 包住调用")
    return binding


# ---------------------------------------------------------------- 会话绑定
@contextlib.contextmanager
def rsc_session(session: RscSession, agent_id: str):
    """绑定会话（不 begin）：装饰器在每次工具调用时自动 begin/apply/finish。"""
    token = _BINDING.set(_Binding(session=session, agent_id=str(agent_id)))
    try:
        yield session
    finally:
        _BINDING.reset(token)


@contextlib.contextmanager
def rsc_task(session: RscSession, agent_id: str, *, writes=(), reads=(),
             intent: str = "", cost: int = 1):
    """长任务：**一次 begin**，多次 `ctx.write` + `apply`，退出时 finish。

    异常退出 → `abort`（不留半成品）。与 `rsc_tool` 组合时，声明写集被任务覆盖
    的工具会复用同一个 token（不重复预留预算）。
    """
    agent_id = str(agent_id)
    ctx = _begin(session, agent_id, tuple(str(w) for w in writes), cost,
                 intent or f"{agent_id}:{uuid.uuid4().hex[:8]}",
                 tuple(str(r) for r in reads))
    token = _BINDING.set(_Binding(session=session, agent_id=agent_id, task=ctx))
    failed = True
    try:
        yield ctx
        failed = False
    finally:
        try:
            if failed:
                session.abort(ctx.token, "tool_error")
            else:
                res = session.finish(ctx.token)
                if getattr(res, "status", None) is Status.DENIED:
                    raise RscToolDenied(
                        "finish_failed", str(getattr(res, "detail", "")))
        finally:
            _BINDING.reset(token)      # 自动恢复外层绑定


def _begin(session: RscSession, agent_id: str, writes: tuple[str, ...],
           cost: int, intent: str, reads: tuple[str, ...] = (),
           tool: str = "") -> RscContext:
    tok = session.begin(agent_id, intent, writes=list(writes),
                        reads=list(reads))
    if isinstance(tok, Wait):
        raise RscDeferred(tok.reason)
    if isinstance(tok, Deny):
        raise RscToolDenied(tok.code or "begin_denied", tok.reason)
    assert isinstance(tok, Token)
    return RscContext(session=session, agent_id=agent_id, token=tok.token,
                      worktree=tok.worktree, writes=writes, reads=reads,
                      intent=intent, tool=tool)


# ---------------------------------------------------------------- 装饰器
def rsc_tool(writes=(), reads=(), *, retry: int = 0, verify=None,
             cost: int = 1):
    """把「改文件的工具函数」接进 RSC。

    - `writes`：该工具会写的资源（**必须**声明，否则并发检测失效）；
    - `retry`：`Conflict` 时自动重新 begin 重跑的次数；
    - `verify`：多步验证命令（`[["ruff","check","."],["pytest","-q"]]`），
      在私有副本上先跑，红则拒绝提交。
    """
    paths = tuple(str(p) for p in (writes or ()))
    read_paths = tuple(str(p) for p in (reads or ()))
    max_retry = max(0, int(retry))
    steps = [list(s) if isinstance(s, (list, tuple)) else s
             for s in (verify or ())] or None

    def deco(fn: Callable) -> Callable:
        sig = inspect.signature(fn)
        if not sig.parameters:
            raise TypeError(f"{fn.__name__} 必须至少接收一个 ctx 参数")

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            binding = _require_binding()
            if binding.task is not None and set(paths) <= set(binding.task.writes):
                return _run_in_task(binding.task, fn, args, kwargs, steps)
            return _run_ephemeral(binding, fn, args, kwargs, paths, read_paths,
                                  steps, max_retry, cost)

        wrapper.__rsc_writes__ = paths
        wrapper.__rsc_reads__ = read_paths
        wrapper.__rsc_verify__ = steps
        return wrapper

    return deco


def _to_bytes(value) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    return json.dumps(value, sort_keys=True, ensure_ascii=False,
                      default=str).encode("utf-8")


def _content_key(pending) -> str:
    """内容寻址的幂等键：同内容的重复提交自动去重。"""
    digest = hashlib.sha256()
    for rel, content in pending:
        digest.update(str(rel).encode("utf-8"))
        digest.update(b"\x00")
        if content is DELETE:
            digest.update(b"<DELETE>")
        elif isinstance(content, bytes):
            digest.update(content)
        else:
            digest.update(str(content).encode("utf-8"))
        digest.update(b"\x00")
    return f"content:{digest.hexdigest()[:16]}"


def _idem_key(fn: Callable, args, kwargs) -> str:
    return f"{fn.__name__}:{tool_key(fn.__name__, {'args': list(args), **kwargs})}"


def _apply(ctx: RscContext, fn: Callable, args, kwargs, steps) -> Any:
    pending = ctx.collect()
    if not pending:
        ctx.session.abort(ctx.token, "no_output")
        raise RscToolDenied(
            "no_output",
            f"{fn.__name__} 没有产生任何声明写集内的内容（writes={list(ctx.writes)}）")
    if steps:
        res = ctx.session.verify(ctx.token, steps=steps, pending=pending)
        if getattr(res, "status", None) is not Status.OK:
            detail = getattr(res, "detail", {}) or {}
            ctx.session.abort(ctx.token, "verify_failed")
            raise RscToolDenied(
                "verify_failed",
                str(detail.get("tail") or detail.get("reason") or detail))
    res = ctx.session.apply(ctx.token, pending,
                            idempotency_key=_idem_key(fn, args, kwargs),
                            agent_id=ctx.agent_id, tool=ctx.tool)
    return res


def _accept_or_raise(ctx: RscContext, res, *, allow_retry: bool) -> None:
    if isinstance(res, Receipt):
        ctx.receipt = res
        _LAST.set(ctx)
        return
    ctx.session.abort(ctx.token, type(res).__name__.lower())
    if isinstance(res, Wait):
        raise RscDeferred(res.reason)
    if isinstance(res, Deny):
        raise RscToolDenied(res.code or "denied", res.reason)
    if isinstance(res, Conflict):
        if allow_retry:
            raise RscConflict(res.reason)          # 由 _run_ephemeral 捕获重试
        raise RscConflict(res.reason)
    raise RscToolDenied("unexpected_result", repr(res))


def _run_in_task(ctx: RscContext, fn, args, kwargs, steps):
    value = fn(ctx, *args, **kwargs)
    _accept_or_raise(ctx, _apply(ctx, fn, args, kwargs, steps), allow_retry=False)
    return value


def _run_ephemeral(binding: _Binding, fn, args, kwargs, paths, reads, steps,
                   retry, cost):
    last_conflict: RscConflict | None = None
    for attempt in range(retry + 1):
        ctx = _begin(binding.session, binding.agent_id, paths, cost,
                     f"{fn.__name__}:{binding.agent_id}:{uuid.uuid4().hex[:8]}",
                     reads, tool=fn.__name__)
        ctx.attempts = attempt + 1
        try:
            value = fn(ctx, *args, **kwargs)
            res = _apply(ctx, fn, args, kwargs, steps)   # 失败时内部已 abort
        except (RscToolDenied, RscConflict, RscDeferred):
            binding.session.abort(ctx.token, "tool_error")
            raise
        except BaseException:
            binding.session.abort(ctx.token, "tool_error")
            raise
        try:
            _accept_or_raise(ctx, res, allow_retry=attempt < retry)
        except RscConflict as exc:
            last_conflict = exc
            continue
        binding.session.finish(ctx.token)
        return value
    raise last_conflict or RscConflict("retry_exhausted")
