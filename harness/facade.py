"""S22/S24：集成门面 `RscSession` —— 宿主只依赖这几个动作。

设计见 `docs/s22-implementation-plan.md` §12.2（评审修订版）与
`docs/s24-gap-analysis-and-repositioning.md` §5（S24-1 Verifier 门禁）：

- **token = agent 会话**：`begin` 一次、多次 `apply`、`finish`/`abort` 一次；
  Z3 worktree 全程复用（`Token.worktree`）。
- 结算/回滚/提交**全部复用内核**（`_finalize` / `abort` / `commit_batch`），
  本模块只做编排与错误映射。
- **外部幂等键带命名空间**：`{tenant}|{agent_id}|{key}` —— 否则跨会话
  同键会静默丢写（评审 P0）。
- **base_version 必须显式声明**：`begin(writes=...)` 是唯一钉住时机；
  未钉住的资源在 `gate` 处**硬拒绝**（fail-closed），绝不静默 last-write-wins。
- token 索引落盘（`state_dir`），`resolve` 用 `latest_ref_for_acb` +
  `_restore_acb(bump_epoch=False)`（C5 的消费者）+ TTL 校验。
- **S24 Verifier**：`verify(token, cmd)` 在「候选工作区副本 + 账本合并视图」上
  执行命令；红则不得提交。这是「文本不冲突但合起来跑不通」的唯一拦截点。

异常映射：只把已知的基础设施异常转成 `Deny`（fail-closed），其余原样抛。
"""

from __future__ import annotations

import fnmatch
import hashlib
import os
import shlex
import shutil
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING

from .kernel.receipts import _EXT_KEY_CONFLICT
from .merge_queue import QUEUE_FULL, MergeQueue
from .rebase import DELETE
from .semantic import detect as _detect_semantics
from .storage.atomic import atomic_write
from .storage.blob import canonical_bytes
from .storage.codec import dumps as _dumps
from .storage.codec import loads as _loads
from .types import ACB, TASK_TERMINAL, Result, Status, TaskState, Write, advance

if TYPE_CHECKING:                            # pragma: no cover
    from .harness import Harness
from .tool_runner import ToolResult, make_runner
from .worktree import WorktreeManager

# ---------------------------------------------------------------- 结果类型
@dataclass(frozen=True)
class Token:
    token: str
    run_id: str
    agent_id: str
    tenant: str
    base_versions: dict
    worktree: str | None
    deadline_ms: int


@dataclass(frozen=True)
class Wait:
    reason: str = ""


@dataclass(frozen=True)
class Deny:
    reason: str = ""
    code: str = ""                 # 例如 "stale_view"（可重试）/ 其他（硬拒绝）


@dataclass(frozen=True)
class Receipt:
    receipt_id: str
    seq: int
    dedup: bool = False
    verified: bool = False          # S24：提交前 Verifier 是否跑过且通过


@dataclass(frozen=True)
class Conflict:
    reason: str = ""


@dataclass(frozen=True)
class Allow:
    pass


# 基础设施异常（按**类名**匹配，避免门面依赖 raft/可选依赖链）
_KNOWN_EXC = ("RaftUnavailable", "NotLeader", "FencingRejected",
              "ReceiptConflict", "InvariantError")
_WAIT_REASONS = ("BUDGET_EXHAUSTED", "NO_SLOT", "PREDECESSOR_NOT_DONE",
                 "UPSTREAM_FAILED")
_VERIFY_TAIL_CHARS = 1200        # 回传给宿主的输出上限（防日志爆炸）


def _norm_cmd(cmd) -> list[str] | None:
    """把 `"pytest -q"` / `["pytest", "-q"]` 归一为 argv（**不经 shell**）。"""
    if cmd is None:
        return None
    if isinstance(cmd, str):
        argv = shlex.split(cmd)
    elif isinstance(cmd, (list, tuple)):
        argv = [str(x) for x in cmd]
    else:
        raise TypeError(f"verify_cmd 必须是 str 或 list，得到 {type(cmd).__name__}")
    if not argv:
        raise ValueError("verify_cmd 不能为空")
    return argv


def _safe_rel(root: str, rel: str) -> str | None:
    """把 `rel` 解析到 `root` 内；逃逸（`..`/绝对路径/符号链接）→ None。"""
    try:
        base = os.path.realpath(root)
        target = os.path.realpath(os.path.join(root, rel))
    except (OSError, ValueError):
        return None
    if target == base or not target.startswith(base + os.sep):
        return None
    return target


_ROOT_CONFIGS = ("pyproject.toml", "setup.cfg", "setup.py", "pytest.ini",
                 "tox.ini", "conftest.py", "requirements.txt", "poetry.lock",
                 "uv.lock", "Makefile")
_VERIFY_PROTECTED = frozenset(_ROOT_CONFIGS + ("__init__.py",))


def _norm_rel(value: str, *, wildcard: bool) -> str:
    """规范化 include/exclude 路径；越界/通配（按需）→ ValueError。"""
    text = str(value).replace("\\", "/").strip()
    if not text:
        raise ValueError("verify 路径不能为空")
    if os.path.isabs(text) or text.startswith("/") or ".." in text.split("/"):
        raise ValueError(f"verify 路径必须是仓库内相对路径：{value!r}")
    if not wildcard and any(ch in text for ch in "*?[]"):
        raise ValueError(f"verify_include 不支持通配：{value!r}")
    while text.startswith("./"):
        text = text[2:]
    return text.rstrip("/")


def _normalize_capability(harness, capability):
    """把 capability 归一为 `CapabilityToken`（None → None）。

    接受三种形态（`policy.issue` 的实际签名，见 policy.py）：
    - CapabilityToken：直接用；
    - str：已签名 token，必须可解析（否则 fail-closed）；
    - dict：必须含 `agent_id`，其余按 issue 的关键字参数展开。
    """
    if capability is None:
        return None
    if isinstance(capability, dict):
        spec = dict(capability)
        agent = str(spec.pop("agent_id", "") or "")
        if not agent:
            raise ValueError("capability 字典必须包含 agent_id")
        return harness.policy.issue(
            agent, tools=set(spec.get("tools") or ()),
            paths=set(spec.get("paths") or ()),
            network=set(spec.get("network") or ()),
            data_class=str(spec.get("data_class", "public")))
    if isinstance(capability, str):
        resolved = harness.policy.resolve_token(capability)
        if resolved is None:
            raise ValueError("capability 字符串无法解析（未签名/过期/缺 secret）")
        return resolved
    return capability


def _norm_scope(include, exclude) -> tuple[tuple[str, ...], tuple[str, ...]]:
    inc = tuple(_norm_rel(x, wildcard=False) for x in (include or ()))
    exc = tuple(_norm_rel(x, wildcard=True) for x in (exclude or ()))
    for pattern in exc:
        if any(fnmatch.fnmatch(name, pattern) for name in _VERIFY_PROTECTED):
            raise ValueError(f"verify_exclude 不得排除保护路径：{pattern!r}")
    return inc, exc


def _scope_allows(rel: str, include: tuple[str, ...],
                  exclude: tuple[str, ...]) -> bool:
    rel = rel.replace("\\", "/")
    if any(fnmatch.fnmatch(rel, p) for p in exclude):
        return False
    if not include:
        return True
    base = rel.rsplit("/", 1)[-1]
    if rel in _ROOT_CONFIGS or base in _ROOT_CONFIGS:
        return True
    if base == "__init__.py":
        parent = rel.rsplit("/", 1)[0] if "/" in rel else ""
        if any(inc == parent or inc.startswith(parent + "/") for inc in include):
            return True
    return any(rel == inc or rel.startswith(inc + "/") for inc in include)


def _content_bytes(content) -> bytes | None:
    """账本/pending 内容 → bytes；DELETE/None/结构化对象 → None（不参与）。"""
    if content is DELETE or content is None:
        return None
    if isinstance(content, bytes):
        return content
    if isinstance(content, str):
        return content.encode("utf-8")
    return None


def _issue_summary(issues: list, limit: int = 3) -> str:
    """把前几条 issue 压成一行（不刷屏），完整报告在 `spec["rsc_semantic"]`。"""
    parts: list[str] = []
    for item in issues[:limit]:
        ref = (item.get("refs") or [{}])[0]
        parts.append(f"{item.get('kind')}:{item.get('module')}:"
                     f"{item.get('name')}@{ref.get('path')}:{ref.get('line')}")
    if len(issues) > limit:
        parts.append(f"+{len(issues) - limit} more")
    return "; ".join(parts)



class RscSession:
    """门面：`begin / apply / gate / verify / finish / abort / snapshot`。"""

    def __init__(self, harness: Harness, *, tenant: str | None = None,
                 token_ttl_ms: int = 120_000, min_budget: int = 1,
                 worktrees: WorktreeManager | None = None,
                 state_dir: str | None = None,
                 verify_cmd=None, verify_required: bool = False,
                 verify_timeout_s: float = 120.0,
                 verify_include: tuple[str, ...] = (),
                 verify_exclude: tuple[str, ...] = (),
                 verify_diagnostics: bool = False,
                 semantic: str = "off",
                 semantic_ignore: tuple[str, ...] = (),
                 merge_queue: dict | None = None,
                 capability=None, capability_required: bool = False,
                 verify_egress: str = "off", tool_runner=None) -> None:
        self.h = harness
        self.tenant = str(tenant or "")
        self.token_ttl_ms = max(1, int(token_ttl_ms))
        self.min_budget = max(1, int(min_budget))
        self.worktrees = worktrees
        self.verify_cmd = _norm_cmd(verify_cmd)
        self.verify_required = bool(verify_required)
        self.verify_timeout_s = max(1.0, float(verify_timeout_s))
        self.verify_include, self.verify_exclude = _norm_scope(
            verify_include, verify_exclude)
        self.verify_diagnostics = bool(verify_diagnostics)
        if self.verify_required and self.verify_cmd is None:
            # fail-closed：要求验证却没给命令，等于「静默放行」
            raise ValueError("verify_required=True 时必须提供 verify_cmd")
        self.semantic = str(semantic)
        if self.semantic not in ("off", "warn", "block"):
            raise ValueError(f"semantic 必须是 off|warn|block，得到 {semantic!r}")
        self.semantic_ignore = tuple(str(x) for x in semantic_ignore)
        # #2：能力令牌（opt-in）。不传 = 完全走旧路径；required 时 fail-closed。
        if capability_required and capability is None:
            raise ValueError("capability_required=True 时必须提供 capability")
        self.capability = _normalize_capability(harness, capability)
        self.capability_required = bool(capability_required)
        # #4：外部命令执行器。默认 LocalRunner（与内联 Popen 逐行为等价）；
        # verify_egress != off 时按能力矩阵选择沙箱后端。
        self.verify_egress = str(verify_egress)
        self.tool_runner = (tool_runner if tool_runner is not None
                            else make_runner(self.verify_egress, harness.egress))
        # S24-4：可选单写者队列（默认关闭 → `apply` 路径字节不变）。
        self._queue: MergeQueue | None = None
        if merge_queue is not None:
            if getattr(harness, "entropy", None) is not None:
                raise ValueError(
                    "确定性模式不支持 merge_queue：队列线程的调度交错不可重放")
            cfg = dict(merge_queue)
            self._queue = MergeQueue(
                self._run_queued_ticket,
                capacity=int(cfg.get("capacity", 64)),
                max_wait_ms=int(cfg.get("max_wait_ms", 30_000)))
        self._lock = threading.RLock()
        self._op_locks: dict[str, threading.RLock] = {}
        self._index: dict[str, dict] = {}
        self._index_path = (os.path.join(str(state_dir), "tokens.json")
                            if state_dir else None)
        self._load_index()
        # C4 的消费者：token 被 reaper 回收时销毁 Z3 + 清索引。
        harness._token_cleanup = self._on_reaped

    # ------------------------------------------------------------------ 锁
    def _op_lock(self, token: str) -> threading.RLock:
        with self._lock:
            lock = self._op_locks.get(token)
            if lock is None:
                lock = threading.RLock()
                self._op_locks[token] = lock
            return lock

    # ------------------------------------------------------------------ 索引
    def _load_index(self) -> None:
        if not self._index_path or not os.path.isfile(self._index_path):
            return
        try:
            with open(self._index_path, "r", encoding="utf-8") as fh:
                raw = _loads(fh.read())
        except Exception:  # noqa: BLE001
            return
        if isinstance(raw, dict):
            self._index = {str(k): dict(v) for k, v in raw.items()}

    def _save_index(self) -> None:
        if not self._index_path:
            return
        try:
            atomic_write(self._index_path, _dumps(self._index).encode("utf-8"))
        except Exception:  # noqa: BLE001   # 索引失败不得阻断主路径
            pass

    def _drop_index(self, token: str) -> None:
        with self._lock:
            if self._index.pop(token, None) is not None:
                self._save_index()

    def _on_reaped(self, acb: ACB) -> None:
        path = (acb.spec or {}).get("rsc_worktree")
        if path and self.worktrees is not None:
            try:
                self.worktrees.destroy(path)
            except Exception:  # noqa: BLE001
                pass
        self._drop_index(acb.id)

    # ------------------------------------------------------------------ 动作
    def begin(self, agent_id: str, intent_key: str, *,
              writes=(), reads=()) -> Token | Wait | Deny:
        """准入：建 token + 钉住 base_versions + （可选）建 Z3 worktree。

        **必须声明 `writes`** —— 它是 OCC 的唯一钉住时机；漏传会失去并发写
        检测（适配器层用 fail-closed 校验，见 `RscMiddleware`）。
        """
        agent_id, intent_key = str(agent_id), str(intent_key)
        if not agent_id or not intent_key:
            return Deny("bad_agent_or_intent")
        # #2：能力令牌（opt-in）—— 身份绑定 + 声明写集 fail-fast。
        cap = self.capability
        if cap is not None:
            if str(getattr(cap, "agent_id", "")) != agent_id:
                return Deny("capability_identity_mismatch",
                            code="capability_identity_mismatch")
            for r in writes:
                if not cap.allows_path(str(r)):
                    return Deny(f"capability_path_denied:{r}",
                                code=f"capability_path_denied:{r}")
        # S24-10：确定性模式下 run_id 可重放（它进入 commit_key / 收据 id）
        entropy = getattr(self.h, "entropy", None)
        run_id = (f"rsc-{entropy.uuid('run_id')[:12]}" if entropy is not None
                  else f"rsc-{uuid.uuid4().hex[:12]}")
        acb = None
        try:
            plan = self.h.planner.plan(run_id, [{
                "task_id": agent_id,
                "logical_subgoal_id": intent_key,
                "min_budget": self.min_budget,
            }])
            acb = self.h.tasks.admit_and_register(plan, agent_id)
        except Exception as exc:
            if type(exc).__name__ in _KNOWN_EXC:
                return Deny(f"coordinator_unavailable:{type(exc).__name__}")
            raise
        if acb.state is TaskState.ADMITTED and cap is not None:
            # 门面路径的 token 按业务 agent 签发，不能交给准入期（按 acb.id 校验）；
            # 准入通过后再绑定，供 apply/gate 的能力门禁使用。
            acb.capability = cap
        if acb.state is not TaskState.ADMITTED:
            reason = str(acb.failure or "not_admitted")
            self._discard(acb, run_id)           # 评审 P1-8：不留僵尸 ACB
            return Wait(reason) if reason in _WAIT_REASONS else Deny(reason)

        # 钉住 base_versions：工具执行期间他人提交 → gate/apply 立刻发现。
        for resource in set(writes) | set(reads):
            acb.base_version[str(resource)] = self.h.workspace.version(resource)

        worktree = None
        try:
            if self.worktrees is not None:
                worktree = self.worktrees.create(agent_id)
        except Exception:
            # 评审 P1-7：worktree 失败必须回滚准入（零残留）。
            self._discard(acb, run_id)
            raise
        acb.spec["rsc_worktree"] = worktree
        acb.spec["rsc_agent"] = agent_id
        acb.spec["rsc_token"] = True
        acb.spec["rsc_writes"] = [str(x) for x in writes]
        deadline = int(self.h.clock.now_ms()) + self.token_ttl_ms
        acb.spec["token_deadline_ms"] = deadline
        ref = self.h.checkpoints.save(acb)
        with self._lock:
            self._index[acb.id] = {"ref": ref, "worktree": worktree,
                                   "agent_id": agent_id, "deadline_ms": deadline}
            self._save_index()
        return Token(token=acb.id, run_id=run_id, agent_id=agent_id,
                     tenant=self.tenant, base_versions=dict(acb.base_version),
                     worktree=worktree, deadline_ms=deadline)

    def apply(self, token: str, writes: list, *, idempotency_key: str,
              cost: int = 1, agent_id: str = "",
              tool: str = "") -> Receipt | Conflict | Deny | Wait:
        """提交：按外部键重放 → CAS 提交 → 消费预算 → 落盘快照。"""
        writes = list(writes)
        if not writes:
            return Deny("empty_writes")
        if isinstance(cost, bool) or not isinstance(cost, int) or cost <= 0:
            return Deny("bad_cost")
        if cost > self.min_budget:
            return Deny("cost_exceeds_reservation")
        with self._op_lock(token):
            return self._apply_locked(token, writes, idempotency_key,
                                      int(cost), str(agent_id), str(tool))

    def _apply_locked(self, token, writes, key, cost, agent_id, tool=""):
        acb = self.resolve(token)
        if isinstance(acb, Deny):
            return acb
        # 评审 P1：只允许写 `begin` 时声明过的资源（否则无 base_version，
        # `_base_of` 回落当前版本 → 静默 last-write-wins）。旧快照无此键 → 跳过。
        declared = acb.spec.get("rsc_writes")
        if declared is not None:
            allowed = {str(x) for x in declared}
            for resource, _content in writes:
                if str(resource) not in allowed:
                    return Deny(f"undeclared_write:{resource}")
        # 外部键加命名空间（评审 P0：跨会话同键不得互相命中）。
        ns_key = f"{self.tenant}|{agent_id or acb.spec.get('rsc_agent', '')}|{key}" \
            if key else ""
        if ns_key:
            replay = self._replay_by_key(ns_key, writes)
            if isinstance(replay, (Receipt, Conflict)):
                return replay
        batch = []
        for resource, content in writes:
            if content is not DELETE:
                try:
                    canonical_bytes(content)   # 提前 fail-fast，不逃逸
                except (TypeError, ValueError) as exc:
                    return Deny(f"bad_content:{type(exc).__name__}")
            batch.append(Write(str(resource), content))
        pending = [(w.resource, w.content) for w in batch]
        # #2：能力门禁必须早于任何副作用（与 semantic/verify 门禁互不依赖）。
        blocked = self._capability_gate(acb, pending, tool)
        if blocked is not None:
            return blocked
        # S24-3：静态语义冲突先挡（便宜、可行动）；S24-1 动态验证后置（权威）。
        blocked = self._semantic_gate(token, acb, pending)
        if blocked is not None:
            return blocked
        # S24-1：Verifier 门禁 —— 提交前必须过验证（自动或显式记录）。
        blocked = self._verify_gate(token, acb, pending)
        if blocked is not None:
            return blocked
        try:
            res = self.h.commit_batch(acb, batch, idempotency_key=ns_key or None)
        except Exception as exc:
            if type(exc).__name__ in _KNOWN_EXC:
                return Deny(f"coordinator_unavailable:{type(exc).__name__}")
            raise
        if res == Status.OK:
            if not res.detail.get("dedup"):
                self._consume(acb, cost)
            # 评审 P0-1：提交后必须刷新快照，否则 resolve 用旧 ref。
            self._resave(acb, token)
            return Receipt(receipt_id=(res.detail.get("receipt_ids") or [""])[0],
                           seq=int(res.detail.get("seq", 0) or 0),
                           dedup=bool(res.detail.get("dedup")),
                           verified=self._verified(acb))
        if res == Status.CONFLICT:
            detail = res.detail or {}
            if detail.get("reason") == "idempotency_key_reused_with_different_content":
                return Conflict(detail["reason"])
            return Conflict(f"stale_write:{detail.get('reason', res.value)}")
        if res == Status.BUDGET_EXHAUSTED:
            return Wait(str(res.detail.get("reason") or res.value))
        if res == Status.UPSTREAM_FAILED:            # 可重试的基础设施态
            return Wait(str(res.detail.get("reason") or res.value))
        if res in (Status.REJECT_STALE, Status.REJECT_STALE_READ):
            return Conflict(f"stale_write:{res.detail.get('reason', res.value)}")
        if res in (Status.DENIED, Status.DENIED_CAPABILITY, Status.GUARD_VIOLATION):
            return Deny(str(res.detail.get("reason") or res.value))
        return Deny(res.value)

    def gate(self, token: str) -> Allow | Deny:
        """副作用前置校验：读集是否仍与 begin 时一致（OCC）。"""
        acb = self.resolve(token)
        if isinstance(acb, Deny):
            return acb
        for resource, base in acb.base_version.items():
            if self.h.workspace.version(resource) != base:
                return Deny(f"stale_view:{resource}", code="stale_view")
        return Allow()



    # ------------------------------------------------------------ merge queue
    def _run_queued_ticket(self, ticket):
        """出队线程入口：**自己拿 token 锁**再走原提交路径（避免与调用方互锁）。"""
        with self._op_lock(ticket.token):
            return self._apply_locked(ticket.token, ticket.writes,
                                      ticket.idempotency_key, ticket.cost,
                                      ticket.agent_id)

    def enqueue_queued(self, token: str, writes: list, *,
                       idempotency_key: str, cost: int = 1,
                       agent_id: str = ""):
        """入队（不阻塞）；队列满 → `Wait(QUEUE_FULL)`，未启用 → 直接 `apply` 结果。"""
        if self._queue is None:
            return self.apply(token, writes, idempotency_key=idempotency_key,
                              cost=cost, agent_id=agent_id)
        writes = list(writes)
        if not writes:
            return Deny("empty_writes")
        if isinstance(cost, bool) or not isinstance(cost, int) or cost <= 0:
            return Deny("bad_cost")
        if cost > self.min_budget:
            return Deny("cost_exceeds_reservation")
        ticket = self._queue.enqueue(token, writes,
                                     idempotency_key=idempotency_key,
                                     cost=int(cost), agent_id=str(agent_id))
        if ticket is None:
            return Wait(QUEUE_FULL)
        return ticket

    def wait_queued(self, ticket, timeout_ms: int | None = None):
        """等待出队结果；`ticket` 不是票（如已被拒）→ 原样返回。"""
        if self._queue is None or not hasattr(ticket, "event"):
            return ticket
        return self._queue.wait(ticket, timeout_ms)

    def apply_queued(self, token: str, writes: list, *,
                     idempotency_key: str, cost: int = 1,
                     agent_id: str = "", timeout_ms: int | None = None):
        """入队并等待：串行「重验 + 提交」，关闭 verify→commit 的 TOCTOU 窗口。"""
        ticket = self.enqueue_queued(token, writes,
                                     idempotency_key=idempotency_key,
                                     cost=cost, agent_id=agent_id)
        return self.wait_queued(ticket, timeout_ms)

    def queue_depth(self) -> int:
        return 0 if self._queue is None else self._queue.depth()

    def queue_status(self, ticket_id: str) -> dict | None:
        return None if self._queue is None else self._queue.status(ticket_id)

    def queue_cancel(self, ticket_id: str, reason: str = "") -> bool:
        return False if self._queue is None else self._queue.cancel(ticket_id, reason)

    def close(self) -> None:
        """停止队列 worker（测试/退出）；未开始的票置为 `Wait(QUEUE_CANCELLED)`。"""
        if self._queue is not None:
            self._queue.close()

    # ------------------------------------------------------------ 静态语义
    def _semantic_view(self, worktree: str, pending=None):
        """base = 工作区原样（agent 起始视角）；post = 账本合并视图 + 本次 pending。"""
        base: dict[str, bytes] = {}
        for root, dirs, files in os.walk(worktree):
            dirs[:] = [d for d in dirs if d not in (".git", "__pycache__")]
            for name in files:
                if not name.endswith(".py"):
                    continue
                full = os.path.join(root, name)
                rel = os.path.relpath(full, worktree).replace(os.sep, "/")
                try:
                    with open(full, "rb") as fh:
                        base[rel] = fh.read()
                except OSError:
                    continue
        post = dict(base)
        for resource in list(self.h.workspace.manifest):
            # 新增资源也必须进入 post（否则新模块里的调用点不被扫描）
            data = _content_bytes(self.h.workspace.get(resource))
            if data is not None:
                post[str(resource)] = data
        for resource in self.h.workspace.deleted_resources():
            post.pop(str(resource), None)
        for resource, content in (pending or []):
            if content is DELETE:
                post.pop(str(resource), None)
                continue
            data = _content_bytes(content)
            if data is not None:
                post[str(resource)] = data
        return base, post

    def check_semantics(self, token: str, *, pending=None) -> Result:
        """显式运行静态语义检查（宿主可先查详情再决定是否提交）。"""
        with self._op_lock(token):
            return self._semantic_locked(token, pending)

    def _semantic_locked(self, token, pending=None) -> Result:
        acb = self.resolve(token)
        if isinstance(acb, Deny):
            return Result(Status.DENIED, {"reason": acb.reason,
                                          "code": acb.code})
        worktree = acb.spec.get("rsc_worktree")
        if not worktree or not os.path.isdir(str(worktree)):
            return Result(Status.DENIED, {"reason": "semantic_requires_worktree"})
        base, post = self._semantic_view(str(worktree), pending)
        report = _detect_semantics(base, post, ignore=self.semantic_ignore)
        detail = {"issues": [i.as_dict() for i in report.issues],
                  "scanned": report.scanned,
                  "skipped": dict(report.skipped),
                  "duration_ms": report.duration_ms}
        acb.spec["rsc_semantic"] = detail
        self._resave(acb, token)
        return Result(Status.OK if report.ok else Status.SEMANTIC_REJECTED,
                      detail)

    def _capability_gate(self, acb, writes, tool: str) -> Deny | None:
        """四维能力门禁（path / tool）。opt-in：无 capability → 直接放行。

        principal 用**业务 agent_id**（`acb.spec["rsc_agent"]`），不是 `acb.id`：
        `policy.check_capability` 做 `resolved.agent_id in (principal, "*")`，
        而门面的 token 是按业务 agent 签发的（runtime 路径按 acb.id 签发）。
        """
        cap = self.capability
        if cap is None:
            return None
        principal = str(acb.spec.get("rsc_agent") or "")
        for resource, _content in writes:
            if tool and not cap.allows_tool(tool):
                return Deny(f"capability_tool_denied:{tool}",
                            code=f"capability_tool_denied:{tool}")
            if not cap.allows_path(str(resource)):
                return Deny(f"capability_path_denied:{resource}",
                            code=f"capability_path_denied:{resource}")
            if not self.h.policy.check_capability(
                    principal, cap, tool or "", str(resource)):
                return Deny("capability_denied",
                            code=f"capability_denied:{resource}")
        return None

    def _semantic_gate(self, token: str, acb: ACB, pending=None) -> Deny | None:
        """`off` 不跑；`warn` 只记录；`block` 返回 Deny（不可重试，需改代码）。"""
        if self.semantic == "off":
            return None
        res = self._semantic_locked(token, pending)
        if res == Status.OK or self.semantic == "warn":
            return None
        detail = res.detail or {}
        issues = detail.get("issues") or []
        return Deny(_issue_summary(issues) or "semantic_conflict",
                    code="semantic_conflict")

    # ------------------------------------------------------------ Verifier
    def verify(self, token: str, cmd=None, *,
               timeout_s: float | None = None, steps=None,
               include=None, exclude=None, pending=None) -> Result:
        """S24：在**候选工作区副本 + 账本合并视图**上执行命令（不经 shell）。

        为什么要「合并视图」：agent 的工作区是 `begin` 时钉住的旧快照，
        另一个 agent 之后提交的改动不在里面。只有把账本里**已存在**的资源
        叠加进来，才能暴露「文本不冲突、合起来跑不通」。

        `steps`：多步命令，同一副本串行、首步失败即停；与 `cmd` 互斥。
        `include/exclude`：缩小复制集合（默认关闭；见 `_scope_allows`）。

        结果写入 `spec["rsc_verify"]`（随 checkpoint 落盘，供审计）；
        `verify_cmd` 配置了时，`apply` 会**自动**先跑一遍，红则拒绝提交。
        """
        if cmd is not None and steps is not None:
            raise ValueError("verify 的 cmd 与 steps 不能同时提供")
        with self._op_lock(token):
            return self._verify_locked(token, cmd, timeout_s, pending=pending,
                                       steps=steps, include=include,
                                       exclude=exclude)

    def _verify_locked(self, token, cmd=None, timeout_s=None, pending=None, *,
                       steps=None, include=None, exclude=None):
        acb = self.resolve(token)
        if isinstance(acb, Deny):
            return Result(Status.DENIED, {"reason": acb.reason,
                                          "code": acb.code})
        argv_list = self._argv_list(cmd, steps)
        if argv_list is None:
            return Result(Status.DENIED, {"reason": "verify_cmd_required"})
        worktree = acb.spec.get("rsc_worktree")
        if not worktree or not os.path.isdir(str(worktree)):
            # 没有 Z3 就没有隔离的候选副本 → fail-closed（不跑在主树上）
            return Result(Status.DENIED, {"reason": "verify_requires_worktree"})
        inc, exc = _norm_scope(
            self.verify_include if include is None else include,
            self.verify_exclude if exclude is None else exclude)
        timeout = (self.verify_timeout_s if timeout_s is None
                   else max(1.0, float(timeout_s)))
        started = time.monotonic()
        copy_started = started
        try:
            tree = self._verification_tree(str(worktree), pending=pending,
                                           include=inc, exclude=exc)
        except ValueError as exc:
            return Result(Status.DENIED, {"reason": str(exc),
                                          "code": "verify_config_invalid"})
        except OSError as exc:
            return Result(Status.DENIED, {"reason": f"verify_copy_failed: {exc}",
                                          "code": "verify_copy_failed"})
        copy_ms = int((time.monotonic() - copy_started) * 1000)
        files = self._count_files(tree)
        step_details: list[dict] = []
        failed_step: int | None = None
        run_ms = 0
        try:
            for index, argv in enumerate(argv_list):
                step_started = time.monotonic()
                code, out = self._run_verify_step(argv, tree, timeout)
                step_ms = int((time.monotonic() - step_started) * 1000)
                run_ms += step_ms
                step_details.append({
                    "cmd": argv, "exit_code": code,
                    "duration_ms": step_ms,
                    "tail": out[-_VERIFY_TAIL_CHARS:]})
                if code != 0:
                    failed_step = index
                    for rest in argv_list[index + 1:]:
                        step_details.append({"cmd": rest, "skipped": True})
                    break
        finally:
            shutil.rmtree(os.path.dirname(tree), ignore_errors=True)
        exit_code = 0 if failed_step is None else step_details[failed_step]["exit_code"]
        tail = (step_details[failed_step]["tail"] if failed_step is not None
                else (step_details[-1]["tail"] if step_details else ""))
        ok = failed_step is None
        detail = {"ok": ok, "exit_code": exit_code, "cmd": argv_list[0],
                  "tail": tail,
                  "duration_ms": int((time.monotonic() - started) * 1000)}
        if steps is not None:
            detail["steps"] = step_details
            if failed_step is not None:
                detail["failed_step"] = failed_step
        if self.verify_diagnostics:
            detail.update({"copy_ms": copy_ms, "files": files, "run_ms": run_ms})
        acb.spec["rsc_verify"] = dict(detail)
        self._resave(acb, token)
        return Result(Status.OK if ok else Status.DENIED, detail)

    def _argv_list(self, cmd, steps) -> list[list[str]] | None:
        if steps is not None:
            if isinstance(steps, (str, bytes)) or not isinstance(steps, (list, tuple)):
                raise TypeError("verify steps 必须是列表")
            out: list[list[str]] = []
            for step in steps:
                argv = _norm_cmd(step)
                if argv is None:
                    raise ValueError("verify steps 不能包含空命令")
                out.append(argv)
            if not out:
                raise ValueError("verify steps 不能为空")
            return out
        argv = _norm_cmd(cmd) if cmd is not None else self.verify_cmd
        return None if argv is None else [argv]

    def _run_verify_step(self, argv: list[str], tree: str,
                         timeout: float) -> tuple[int, str]:
        """执行一步验证命令。

        行为契约（与内联 Popen 版本逐条等价）：
        - 正常：`(rc, stdout+stderr stripped)`；
        - 超时：`(-1, "verify_timeout after {timeout}s")`，**杀整个进程组**；
        - spawn 失败：`(-1, "verify_spawn_failed: ...")`。
        """
        res: ToolResult = self.tool_runner.run(
            argv, cwd=tree, timeout_s=float(timeout))
        if res.timed_out:
            return -1, f"verify_timeout after {timeout}s"
        if res.rc == -1 and res.output.startswith("verify_spawn_failed"):
            return -1, res.output
        return res.rc, res.output

    @staticmethod
    def _count_files(tree: str) -> int:
        return sum(len(files) for _root, _dirs, files in os.walk(tree))

    def _ignore_fn(self, worktree: str, include: tuple[str, ...],
                   exclude: tuple[str, ...]):
        """`copytree` 的 ignore：按 include/exclude 裁剪，但保留祖先目录。"""
        root = os.path.realpath(worktree)

        def ignore(src: str, names: list[str]) -> set[str]:
            rel_dir = os.path.relpath(src, root).replace(os.sep, "/")
            rel_dir = "" if rel_dir == "." else rel_dir
            ignored: set[str] = set()
            for name in names:
                rel = f"{rel_dir}/{name}" if rel_dir else name
                if name == ".git":
                    ignored.add(name)
                    continue
                if any(fnmatch.fnmatch(rel, p) for p in exclude):
                    ignored.add(name)
                    continue
                if not include:
                    continue
                full = os.path.join(src, name)
                if os.path.isdir(full):
                    # 目录：是 include 的祖先，或位于 include 目录内 → 继续遍历
                    if any(inc == rel or inc.startswith(rel + "/")
                           or rel.startswith(inc + "/") for inc in include):
                        continue
                    ignored.add(name)
                elif not _scope_allows(rel, include, exclude):
                    ignored.add(name)
            return ignored

        return ignore

    def _verification_tree(self, worktree: str, pending=None, *,
                           include: tuple[str, ...] = (),
                           exclude: tuple[str, ...] = ()) -> str:
        """复制候选工作区（不含 `.git`），叠加「账本合并视图 + 本次待提交内容」。

        顺序很重要：工作区副本 → 账本（其它 agent 已提交）→ 墓碑删除 →
        本次 pending（后者覆盖前者），得到「提交后会是什么样」的真实视图。
        """
        holder = tempfile.mkdtemp(prefix="rsc-verify-")
        tree = os.path.join(holder, "tree")
        shutil.copytree(worktree, tree, symlinks=True,
                        ignore=self._ignore_fn(worktree, include, exclude))
        for resource in list(self.h.workspace.manifest):
            rel = str(resource)
            if not _scope_allows(rel, include, exclude):
                continue
            self._overlay(tree, rel, self.h.workspace.get(resource), create=True)
        for resource in self.h.workspace.deleted_resources():
            if _scope_allows(str(resource), include, exclude):
                self._overlay(tree, str(resource), DELETE, delete=True)
        for resource, content in (pending or []):
            rel = str(resource)
            if not _scope_allows(rel, include, exclude):
                continue
            # pending 可能是新文件（本次新增）→ 允许创建
            self._overlay(tree, rel, content, create=True)
        if include and not self._include_matched(tree, include):
            shutil.rmtree(holder, ignore_errors=True)
            raise ValueError("verify_include_empty：include 未命中任何文件")
        return tree

    @staticmethod
    def _include_matched(tree: str, include: tuple[str, ...]) -> bool:
        for root, _dirs, files in os.walk(tree):
            for name in files:
                rel = os.path.relpath(os.path.join(root, name), tree)
                rel = rel.replace(os.sep, "/")
                if any(rel == inc or rel.startswith(inc + "/") for inc in include):
                    return True
        return False

    @staticmethod
    def _overlay(tree: str, resource: str, content, *,
                 create: bool = False, delete: bool = False) -> None:
        """把 `content` 写进副本；`create=False` 时只覆盖**已存在**的路径。"""
        path = _safe_rel(tree, resource)
        if path is None:
            return
        if delete or content is DELETE:
            if os.path.isfile(path):
                try:
                    os.unlink(path)
                except OSError:
                    pass
            return
        if not create and not os.path.isfile(path):
            return
        if content is None:
            return
        if isinstance(content, bytes):
            data = content
        elif isinstance(content, str):
            data = content.encode("utf-8")
        else:
            return
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as fh:
                fh.write(data)
        except OSError:
            return

    def _verified(self, acb: ACB) -> bool:
        return bool((acb.spec.get("rsc_verify") or {}).get("ok"))

    def _verify_gate(self, token: str, acb: ACB, pending=None) -> Deny | None:
        """提交前门禁：配置了命令就自动跑；否则已有的失败记录也一律拒绝。"""
        if self.verify_cmd is not None:
            res = self._verify_locked(token, pending=pending)
            if res != Status.OK:
                detail = res.detail or {}
                if detail.get("reason") in ("verify_cmd_required",
                                            "verify_requires_worktree"):
                    return Deny(str(detail["reason"]), code="verify_failed")
                return Deny(f"verify_failed:{detail.get('exit_code')}",
                            code="verify_failed")
            return None
        record = acb.spec.get("rsc_verify")
        if record is not None and not record.get("ok"):
            # 显式 verify 失败过 → 未重新验证通过前不得提交（fail-closed）
            return Deny("verify_failed_recorded", code="verify_failed")
        if self.verify_required and not self._verified(acb):
            return Deny("verify_required", code="verify_failed")
        return None

    def finish(self, token: str, *, outcome: str = "DONE") -> Result:
        with self._op_lock(token):
            acb = self.resolve(token)
            if isinstance(acb, Deny):
                if acb.code == "already":
                    return Result(Status.OK, {"already": True})
                return Result(Status.DENIED, {"reason": acb.reason})
            res = self.h.finish(acb, outcome=outcome)
            if res == Status.OK:
                self._destroy_worktree(acb)
                self._drop_index(token)
                self._op_locks.pop(token, None)
            return res

    def abort(self, token: str, reason: str = "") -> Result:
        with self._op_lock(token):
            acb = self.resolve(token)
            if isinstance(acb, Deny):
                if acb.code == "already":
                    return Result(Status.OK, {"reason": reason, "already": True})
                return Result(Status.DENIED, {"reason": acb.reason})
            if acb.state not in TASK_TERMINAL:
                advance(acb, "cancel")
            self.h.abort(acb)
            self._destroy_worktree(acb)
            self._drop_index(token)
            self._op_locks.pop(token, None)
            return Result(Status.OK, {"reason": reason})

    def snapshot(self, token: str) -> dict | Deny:
        acb = self.resolve(token)
        if isinstance(acb, Deny):
            return acb
        return dict(acb.base_version)

    def owner_of(self, token: str) -> str | None:
        """token 的归属 agent；未知/终态 → None（由调用方映射 404/410）。"""
        acb = self.resolve(token)
        if isinstance(acb, Deny):
            return None
        return str(acb.spec.get("rsc_agent") or "")

    def declared_writes(self, token: str) -> tuple[str, ...] | None:
        """`begin` 时声明的写集；旧快照无此键 → None（不改变既有语义）。"""
        acb = self.resolve(token)
        if isinstance(acb, Deny):
            return None
        raw = acb.spec.get("rsc_writes")
        return None if raw is None else tuple(str(x) for x in raw)

    # ------------------------------------------------------------------ 内部
    def resolve(self, token: str) -> ACB | Deny:
        """token → ACB：内存 → 索引 → checkpoint 恢复（C5）+ TTL 校验。"""
        acb = self.h.acbs.get(str(token))
        if acb is not None:
            if acb.state in TASK_TERMINAL:
                return Deny("terminal_token", code="already")
            return acb
        entry = self._index.get(str(token))
        if entry is None:
            return Deny("unknown_token")
        deadline = entry.get("deadline_ms")
        if deadline is not None and int(self.h.clock.now_ms()) >= int(deadline):
            # 评审 P1-5：磁盘上的过期 token 必须拒绝（reaper 只扫内存）。
            self._drop_index(str(token))
            if entry.get("worktree") and self.worktrees is not None:
                try:
                    self.worktrees.destroy(entry["worktree"])
                except Exception:  # noqa: BLE001
                    pass
            return Deny("token_expired", code="already")
        ref = self.h.checkpoints.latest_ref_for_acb(str(token)) or entry["ref"]
        try:
            data = self.h.checkpoints.load_strict(ref)
            restored = self.h.recovery.restore_acb(data, bump_epoch=False)
        except Exception:  # noqa: BLE001
            return Deny("token_restore_failed")
        if restored.state in TASK_TERMINAL or restored.needs_readmit:
            # 评审 P1-6：恢复失败必须释放槽位（`_restore_acb` 已 try_acquire）。
            self.h.slots.release(restored.id)
            return Deny("token_unusable", code="already")
        self.h.acbs[restored.id] = restored
        self.h.run_coordinator.register(restored)
        return restored

    def _consume(self, acb: ACB, cost: int) -> None:
        """只消费**一笔**预留（评审 P1-2：多预留不得重复扣费）。"""
        for rid in list(acb.reservation_ids)[:1]:
            try:
                self.h.budget.consume(rid, cost, acb=acb)
            except Exception:  # noqa: BLE001
                pass

    def _resave(self, acb: ACB, token: str) -> None:
        try:
            ref = self.h.checkpoints.save(acb)
            with self._lock:
                entry = self._index.get(token)
                if entry is not None:
                    entry["ref"] = ref
                    self._save_index()
        except Exception:  # noqa: BLE001
            pass

    def _discard(self, acb: ACB, run_id: str) -> None:
        """准入失败/回滚：撤销 + 置终态 + 移出内存（零残留）。"""
        if acb.state not in TASK_TERMINAL:
            advance(acb, "cancel")           # PLANNED/INADMISSIBLE → CANCELLED
        try:
            self.h.abort(acb)
        except Exception:  # noqa: BLE001
            pass
        try:
            self.h.purge_run(run_id)
        except Exception:  # noqa: BLE001
            pass

    def _replay_by_key(self, key: str, writes: list):
        """按外部键查既有提交（不需要 token）。命中 → Receipt/Conflict/None。"""
        if not key:
            return None
        expected = set()
        for resource, content in writes:
            if content is DELETE:
                expected.add((str(resource), "", True))
            else:
                try:
                    digest = hashlib.sha256(canonical_bytes(content)).hexdigest()
                except (TypeError, ValueError):
                    return None            # 非法内容交给 commit_batch 处理
                expected.add((str(resource), digest, False))
        try:
            hit = self.h.receipts.dedup_by_external_key(key, expected)
        except Exception as exc:
            if type(exc).__name__ in _KNOWN_EXC:
                return Deny(f"coordinator_unavailable:{type(exc).__name__}")
            raise
        if hit is _EXT_KEY_CONFLICT:
            return Conflict("idempotency_key_reused_with_different_content")
        if hit is None:
            return None
        _hits, receipt_ids, seq = hit
        return Receipt(receipt_id=(receipt_ids[0] if receipt_ids else ""),
                       seq=int(seq), dedup=True)

    def worktree_for(self, token: str) -> str | None:
        acb = self.h.acbs.get(str(token))
        if acb is not None:
            return (acb.spec or {}).get("rsc_worktree")
        entry = self._index.get(str(token))
        return entry.get("worktree") if entry else None

    def ledger_version(self, resource: str) -> int:
        return self.h.workspace.version(resource)

    def ledger_get(self, resource: str):
        return self.h.workspace.get(resource)

    def tick(self) -> None:
        self.h.tick()

    def _destroy_worktree(self, acb: ACB) -> None:
        path = (acb.spec or {}).pop("rsc_worktree", None)
        if path and self.worktrees is not None:
            try:
                self.worktrees.destroy(path)
            except Exception:  # noqa: BLE001
                pass
