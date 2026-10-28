"""S22：集成门面 `RscSession` —— 宿主只依赖这 6 个动作。

设计见 `docs/s22-implementation-plan.md` §12.2（评审修订版）：

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

异常映射：只把已知的基础设施异常转成 `Deny`（fail-closed），其余原样抛。
"""

from __future__ import annotations

import hashlib
import os
import threading
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING

from .harness import _EXT_KEY_CONFLICT
from .rebase import DELETE
from .storage.atomic import atomic_write
from .storage.blob import canonical_bytes
from .storage.codec import dumps as _dumps
from .storage.codec import loads as _loads
from .types import ACB, TASK_TERMINAL, Result, Status, TaskState, Write, advance

if TYPE_CHECKING:                            # pragma: no cover
    from .harness import Harness
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


class RscSession:
    """六动作门面：`begin / apply / gate / finish / abort / snapshot`。"""

    def __init__(self, harness: Harness, *, tenant: str | None = None,
                 token_ttl_ms: int = 120_000, min_budget: int = 1,
                 worktrees: WorktreeManager | None = None,
                 state_dir: str | None = None) -> None:
        self.h = harness
        self.tenant = str(tenant or "")
        self.token_ttl_ms = max(1, int(token_ttl_ms))
        self.min_budget = max(1, int(min_budget))
        self.worktrees = worktrees
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
            raw = _loads(open(self._index_path, "r", encoding="utf-8").read())
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
        run_id = f"rsc-{uuid.uuid4().hex[:12]}"
        acb = None
        try:
            plan = self.h.planner.plan(run_id, [{
                "task_id": agent_id,
                "logical_subgoal_id": intent_key,
                "min_budget": self.min_budget,
            }])
            acb = self.h._admit_and_register(plan, agent_id)
        except Exception as exc:
            if type(exc).__name__ in _KNOWN_EXC:
                return Deny(f"coordinator_unavailable:{type(exc).__name__}")
            raise
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
              cost: int = 1, agent_id: str = "") -> Receipt | Conflict | Deny | Wait:
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
                                      int(cost), str(agent_id))

    def _apply_locked(self, token, writes, key, cost, agent_id):
        acb = self.resolve(token)
        if isinstance(acb, Deny):
            return acb
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
                           dedup=bool(res.detail.get("dedup")))
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
            restored = self.h._restore_acb(data, bump_epoch=False)
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
            hit = self.h._dedup_by_external_key(key, expected)
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
