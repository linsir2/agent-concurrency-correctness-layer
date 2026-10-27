"""Harness —— 组装所有组件的门面（对应文档 §20.2 端到端数据流）。

用法见 `examples/` 与 `tests/`。
"""

from __future__ import annotations

import hashlib
import threading

from .auth import Signer
from .budget import BudgetManager, InvariantError, Reservation
from .checkpoint import CheckpointStore, EffectJournal
from .clock import SystemClock
from .compensation import CompensationJournal
from .context import ContextAssembler
from .event_log import EventLog
from .intent_registry import IntentRegistry
from .orchestration import (Dispatcher, HITL, Planner, ReconcileLoop,
                            RunCoordinator, Scheduler, SlotTracker)
from .policy import PolicyEngine
from .resolver import ScriptedResolver
from .runtime import AgentRuntime
from .subscription import SubscriptionRegistry
from .types import (ACB, TASK_TERMINAL, EventKind, Mode, Result, Status,
                    TaskState, Write, advance)
from .storage.blob import InMemoryBlobStore, canonical_bytes
from .workspace import WorkspaceManager, WorkspaceVersionStore
from .rebase import DELETE


class Harness:
    """组装所有组件的门面（对应文档 §20.2 端到端数据流）。

    所有可调常量均可注入（避免硬编码，见 milestones.md §5）。
    """

    def __init__(self, run_limits: int = 1000, retention: int = 10000,
                 lease_ms: int = 60_000, guard_every: int = 1,
                 aging_max: int = 5, max_steps: int = 10_000,
                 max_concurrent: int = 100,
                 policy_default_allow: bool = False, *,
                 clock=None, event_log=None, intents=None, versions=None,
                 workspace=None, budget=None, checkpoints=None, effects=None,
                 policy=None, context=None, subscriptions=None,
                 decide=None, default_token=None, resolver=None,
                 blobs=None, path_of=None, lock_timeout_s: float = 5.0,
                 auth_secret: str | None = None,
                 tombstone_ttl_s: float = 86400.0,
                 auth_gc_every: int = 256,
                 gc_every: int = 1000,
                 per_action_delay: float = 0.0) -> None:
        self.lease_ms = lease_ms
        self.guard_every = guard_every
        self.aging_max = aging_max
        self.max_steps = max_steps
        self.max_concurrent = max_concurrent
        self.retention = retention
        self.per_action_delay = per_action_delay   # §2.5：基准配对用注入点

        self.clock = clock or SystemClock()
        self.event_log = event_log if event_log is not None else EventLog(clock=self.clock)
        self.blobs = blobs if blobs is not None else InMemoryBlobStore()
        self.intents = intents if intents is not None else IntentRegistry(self.clock)
        self.versions = versions if versions is not None else WorkspaceVersionStore()
        self.lock_timeout_s = lock_timeout_s
        self.workspace = (workspace if workspace is not None
                          else WorkspaceManager(self.event_log, self.intents, self.versions,
                                                blobs=self.blobs, path_of=path_of,
                                                lock_timeout_s=lock_timeout_s))
        self.budget = (budget if budget is not None
                       else BudgetManager(run_limits, tombstone_ttl_s=tombstone_ttl_s))
        self.checkpoints = checkpoints if checkpoints is not None else CheckpointStore()
        if hasattr(self.checkpoints, "event_log"):      # §2.4：注入全局水位源
            self.checkpoints.event_log = self.event_log
        self.effects = (effects if effects is not None
                        else EffectJournal(self.event_log))
        self.comp_journal = CompensationJournal(self.event_log)
        self._applied_event_ids: set[str] = set()
        self.checkpoints.applied_event_ids = self._applied_event_ids
        self._drain_skipped: list[tuple[str, str]] = []   # D11：调度跳过原因（不再静默）
        self.auth_gc_every = max(1, int(auth_gc_every))
        self.gc_every = max(1, int(gc_every))
        self._event_count = 0
        self._auth_secret = auth_secret
        self._auth: Signer | None = Signer(auth_secret) if auth_secret else None
        self.subscriptions = (subscriptions if subscriptions is not None
                              else SubscriptionRegistry(self.event_log))
        self.policy = (policy if policy is not None
                       else PolicyEngine(default_allow=policy_default_allow,
                                         default_token=default_token))
        self.policy._signer_factory = self._get_signer
        self.context = (context if context is not None
                        else ContextAssembler(self.event_log, self.workspace, retention))
        self.compensation_journal: list[dict] = []      # 文档 §5.4
        self.decide = decide
        self.resolver = (resolver if resolver is not None
                         else ScriptedResolver())       # §7 默认 scripted（不联网）

        self.slots = SlotTracker(max_concurrent)
        self.checkpoints.slots = self.slots        # checkpoint 快照活跃槽位
        self.planner = Planner(self)
        self.dispatcher = Dispatcher(self)
        self.scheduler = Scheduler(self)
        self.run_coordinator = RunCoordinator(self)
        self.reconcile = ReconcileLoop(self)
        self.hitl = HITL()

        self.runtime = AgentRuntime(self)
        self.plans: dict[str, dict] = {}
        self.acbs: dict[str, ACB] = {}
        self._lock = threading.RLock()

        self.event_log.subscribe(self._on_event)

    # ---- 事件 → 订阅失效（文档 §5.6） ----------------------------
    def _on_event(self, ev) -> None:
        # 周期性清理已过期的 jti（按注入时钟，避免 `_seen` 无界增长）
        self._event_count += 1
        if self._event_count % self.gc_every == 0:
            # §4.4：接上 budget.gc（原死代码），now 用注入时钟的 epoch 秒
            self.budget.gc(self.clock.now_ms() / 1000.0)
        if (self._auth is not None
                and self._event_count % self.auth_gc_every == 0):
            self._auth.gc_seen(self.clock.now_ms() / 1000.0)
        if ev.kind == EventKind.RESOURCE_COMMITTED:
            for agent_id in self.subscriptions.subscribers(ev.subject):
                self.subscriptions.notify(ev.subject, ev)

    # ---- 签名 / token ---------------------------------------------
    def _get_signer(self) -> Signer:
        """惰性构造 `Signer`：优先注入 secret，否则读 `HARNESS_AUTH_SECRET`。"""
        if self._auth is None:
            self._auth = Signer.from_env()
        return self._auth

    @property
    def auth(self) -> Signer:
        return self._get_signer()

    def _effective_token(self, acb: ACB):
        """取该 ACB 的有效 token：`acb.capability` → `policy._tokens` → `default_token`。

        无 token → `None`（`_gateway_check` 据此 deny-by-default）。
        """
        token = getattr(acb, "capability", None)
        if token is not None:
            return token
        token = self.policy.token(acb.id)
        if token is not None:
            return token
        return self.policy.default_token


    # ---- 幂等键（文档 §5.8 / I7） --------------------------------
    def idem_key(self, acb: ACB, action, action_index: int) -> str:
        """`hash(run_id, logical_subgoal_id, hash(tool, hash(args)))` —— 不含 task_id。"""
        payload = f"{acb.run_id}|{acb.logical_subgoal_id}|{action.tool}|{action.args}"
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]

    # ---- RSC 提交（文档 §5.5） ------------------------------------
    def commit_batch(self, acb: ACB, writes: list[Write],
                     mode: Mode = Mode.COMMIT) -> Result:
        """公共写入口。提交点顺序不可调换：

        ① 输入校验 → ② 预留租约(L0) → ③ 全序一次性持锁(L3，锁集合含读集合)
        → ④ 校验 → ④′ 内容先落盘 → ⑤ 提交点前复验租约
        → ⑥ 提交点（一次 fdatasync）→ ⑦ 投影 → ⑧ 锁外通知（不掩盖返回值）。
        """
        if not writes:
            return Result(Status.OK, {"noop": True, "seq": None})
        resources = {w.resource for w in writes}
        if len(resources) != len(writes):                          # ① 同批重复 resource
            return Result(Status.INVALID_INPUT,
                          {"reason": "duplicate_resource", "resources": sorted(resources)})
        contents = {w.resource: acb.staged.get(w.resource, w.content) for w in writes}
        for w in writes:                                           # ① None 内容
            if contents[w.resource] is None:
                return Result(Status.INVALID_INPUT,
                              {"reason": "none_content", "resource": w.resource})

        resources |= set(acb.read_set)                             # ③ 读集合一并持锁 → 原子复验
        leases: dict = {}
        events = None
        held: list[str] = []
        try:
            for w in writes:                                       # ② 预留租约（L0）
                base = self.workspace._base_of(acb, w.resource)
                lease = self.intents.reserve(w.resource, acb.id, base, self.lease_ms)
                if lease is None:
                    return Result(Status.DENIED, {"resource": w.resource})
                leases[w.resource] = lease

            held, ok = self.workspace._acquire_all(resources, timeout=self.lock_timeout_s)
            if not ok:
                return Result(Status.UPSTREAM_FAILED, {"reason": "lock_timeout", "held": held})

            for w in writes:                                       # ④ 全部校验（锁内）
                r = self.workspace._validate_locked(acb, w, leases[w.resource], mode)
                if r != Status.VALID:
                    return r

            blob_hashes = {}                              # ④′ 内容先落盘（DELETE 跳过）
            for w in writes:
                if contents[w.resource] is DELETE:
                    blob_hashes[w.resource] = ""
                else:
                    blob_hashes[w.resource] = self.blobs.put(
                        canonical_bytes(contents[w.resource]))

            for w in writes:                                       # ⑤ 提交点前复验租约
                if not self.intents.is_valid(leases[w.resource]):
                    return Result(Status.REJECT_STALE,
                                  {"resource": w.resource,
                                   "reason": "lease_expired_before_commit"})

            drafts = [self._commit_record(acb, w, blob_hashes[w.resource],
                                          contents[w.resource] is DELETE)
                      for w in writes]
            events = self.event_log.append_batch(drafts, notify=False)   # ⑥ 提交点

            for w in writes:                                       # ⑦ 投影（可重放）
                prev = (self.workspace.manifest[w.resource].blob_hash
                        if w.resource in self.workspace.manifest else "")
                self.workspace._apply_locked(acb, w, blob_hashes[w.resource], prev,
                                             deleted=contents[w.resource] is DELETE)
            for w in writes:                                       # ⑦′ 自写后重读：刷新 ACB 读版本
                new_version = self.workspace.version(w.resource)
                acb.read_set[w.resource] = new_version
                acb.base_version[w.resource] = new_version
            return Result(Status.OK, {"resources": sorted(blob_hashes), "seq": events[-1].seq})
        finally:
            self.workspace._release_all(held)                      # 先释放资源锁
            for lease in leases.values():
                self.intents.revoke(lease)                         # 再 revoke 租约
            if events is not None:                                 # ⑧ 锁外通知（不掩盖返回值）
                try:
                    self.event_log.notify_events(events)
                except Exception:
                    pass

    def _commit_record(self, acb: ACB, w: Write, blob_hash: str,
                       deleted: bool = False) -> dict:
        cur = self.workspace.manifest.get(w.resource)
        version = self.workspace._next_version(w.resource)
        payload = {
            "resource": w.resource,
            "version": version,
            "blob_hash": blob_hash,
            "prev_hash": cur.blob_hash if cur else "",
            "diff": {"prev_hash": cur.blob_hash if cur else "", "current_hash": blob_hash},
        }
        if deleted:                                  # Delete 哨兵事件（可重放）
            payload["deleted"] = True
        return {
            "kind": EventKind.RESOURCE_COMMITTED,          # ← 必须与重建过滤一致
            "subject": w.resource,
            "payload": payload,
            "producer": acb.id,
            "run_id": acb.run_id,
            "idempotency_key": f"{acb.run_id}:{acb.id}:{w.resource}",
        }

    def compensate(self, acb: ACB, applied: list[Write]) -> None:
        """Saga 补偿（§4.2）：批量原子计划 → 释放租约 → 逐条续做。

        - `comp_journal.plan`：**一次 `append_batch`** 写全部条目 + 批次标记（同帧）；
        - `revoke_all`：补偿前先释放租约；
        - 逆序 `_run_one_compensation`：先按 `state` 分支，`deleted` 用版本号判定。
        """
        entries = self.comp_journal.plan(acb, applied)
        self.intents.revoke_all(acb.id)
        by_id = {e.comp_id: w for e, w in zip(entries, applied)}
        for e in reversed(entries):
            self._run_one_compensation(acb, e, by_id.get(e.comp_id))

    def _run_one_compensation(self, acb: ACB, e, w: Write | None = None) -> None:
        """单条补偿（§4.2）：先按 journal 状态分支，再按 hash / 版本号判定。"""
        if w is not None and w.compensated:
            self.compensation_journal.append({"resource": e.resource, "state": "SKIPPED"})
            return
        if e.state == "COMPENSATED":
            self._mark_write(w, "COMPENSATED")
            self.compensation_journal.append({"resource": e.resource, "state": "COMPENSATED"})
            return
        if e.state == "PIVOT":
            self._mark_write(w, "PIVOT")
            self.compensation_journal.append({"resource": e.resource, "state": "PIVOT"})
            return
        exists = self.workspace.get(e.resource) is not None
        current = self.workspace.content_hash(e.resource)
        if e.deleted:
            # 删除类：必须用版本号判定（空 hash 无法区分「不存在」与「删除后」）
            if not exists and self.workspace.version(e.resource) == e.version_before + 1:
                self.comp_journal.mark(e.comp_id, "COMPENSATED")
                self._mark_write(w, "COMPENSATED")
                self.compensation_journal.append({"resource": e.resource, "state": "COMPENSATED"})
                return
            if exists and current != e.applied_hash:
                self.comp_journal.mark(e.comp_id, "PIVOT")
                self._mark_write(w, "PIVOT")
                self.compensation_journal.append({"resource": e.resource, "state": "PIVOT"})
                return
        else:
            if current == e.target_hash:
                self.comp_journal.mark(e.comp_id, "COMPENSATED")
                self._mark_write(w, "COMPENSATED")
                self.compensation_journal.append({"resource": e.resource, "state": "COMPENSATED"})
                return
            if current != e.applied_hash:
                self.comp_journal.mark(e.comp_id, "PIVOT")
                self._mark_write(w, "PIVOT")
                self.compensation_journal.append({"resource": e.resource, "state": "PIVOT"})
                return
        # 执行反写
        rw = Write(e.resource, content=None, version_before=e.version_before,
                   applied_hash=e.applied_hash)
        reverse = self.workspace.compute_reverse_patch(rw)
        if reverse is None:
            self.comp_journal.mark(e.comp_id, "PIVOT")
            self._mark_write(w, "PIVOT")
            self.compensation_journal.append({"resource": e.resource, "state": "PIVOT"})
            return
        acb.staged[e.resource] = DELETE if reverse is DELETE else reverse
        st = self._compensate_one(acb, rw)
        if st == Status.OK:
            self.comp_journal.mark(e.comp_id, "COMPENSATED")
            self._mark_write(w, "COMPENSATED")
            self.compensation_journal.append({"resource": e.resource, "state": "COMPENSATED"})
        elif st == Status.REJECT_COMPENSATE:
            self.comp_journal.mark(e.comp_id, "PIVOT")
            self._mark_write(w, "PIVOT")
            self.compensation_journal.append({"resource": e.resource, "state": "PIVOT"})
        else:
            self.comp_journal.mark(e.comp_id, "FAILED")
            self.compensation_journal.append({"resource": e.resource, "state": "FAILED"})

    @staticmethod
    def _mark_write(w: Write | None, state: str) -> None:
        """兼容旧 `compensation_journal` / `Write.compensated|pivot` 语义。"""
        if w is None:
            return
        if state == "COMPENSATED":
            w.compensated = True
        elif state == "PIVOT":
            w.pivot = True

    def _compensate_one(self, acb: ACB, w: Write) -> Result:
        """单写补偿，带**真正的**有界重试（v1.11 修复）。"""
        for _ in range(acb.retry_budget + 1):
            st = self.commit_batch(acb, [w], Mode.COMPENSATE)
            if st == Status.OK:
                return Result(Status.OK, {})
            if st == Status.REJECT_COMPENSATE:
                return st
        return Result(Status.COMMIT_UNRECOVERABLE, {})

    # ---- 重规划（文档 §5.2④ / §8.1） ------------------------------
    def propagate_failure(self, task_id: str, plan_version: int) -> None:
        """只传播到同一 Plan 版本的后继（v1.10 修复 P0-3）。"""
        for run_id, plan in self.plans.items():
            task = plan["tasks"].get(task_id)
            if not task or task.get("plan_version") != plan_version:
                continue
            for edge in plan["edges"]:
                if edge["from"] == task_id:
                    succ = plan["tasks"].get(edge["to"])
                    if succ and succ.get("plan_version") == plan_version:
                        succ["state"] = TaskState.CANCELLED

    # ---- Supervisor 辅助 ------------------------------------------
    def supervisor_note_crash(self, acb: ACB) -> None:
        """Supervisor 心跳超时回收（文档 §5.8③）。"""
        self.intents.revoke_all(acb.id)
        self.bump_epoch()
        # §2.4：落盘（duck-typed 探针无 budget_node 时跳过，保持向后兼容）
        if hasattr(acb, "budget_node"):
            self.checkpoints.save(acb)

    def bump_epoch(self) -> None:
        self.intents.bump_epoch()

    # ---- 端到端运行 -----------------------------------------------
    def dispatch_and_run(self, plan: dict, task_id: str, decisions, *,
                         required_tools: set[str] | None = None,
                         crash_at=None, token=None) -> ACB:
        """唯一生产入口：try_admit → 注册 plan/acb → run_task → 返回 acb。"""
        run_id = plan["run_id"]
        acb = self.dispatcher.try_admit(plan["tasks"][task_id], run_id,
                                        required_tools=required_tools,
                                        token=token, plan=plan)
        self.plans[run_id] = plan
        self.acbs[acb.id] = acb
        self.run_coordinator.register(acb)
        if acb.state is not TaskState.ADMITTED:
            return acb
        self.run_task(acb, decisions, crash_at=crash_at)
        return acb

    def run_task(self, acb: ACB, decisions, crash_at=None) -> Status:
        """外部直接调用入口：注册 + push 到调度器 + 执行 + 消费调度器。"""
        self.acbs[acb.id] = acb
        self.run_coordinator.register(acb)
        self.scheduler.push(acb.id)            # D10：调度器存 acb.id（不再存 task_id）
        try:
            return self._run_one(acb, decisions, crash_at=crash_at)
        finally:
            self._drain_scheduler()            # D12：异常路径（含 CrashInjected）也消费队列

    def _run_one(self, acb: ACB, decisions=None, crash_at=None) -> Status:
        """执行体（§2.6）：`runtime.run` + `_finalize`；**不 push、不 drain**（防递归）。"""
        try:
            return self.runtime.run(acb, decisions or [], crash_at=crash_at)
        finally:
            self._finalize(acb)

    def _drain_scheduler(self) -> None:
        """真消费 ready_queue（§2.6）：`pop → 找 ACB → 只启动 ADMITTED`。

        - `_run_one` 不 push、不递归 drain → 无无限递归、无双重 run；
        - `needs_readmit` 的 ACB 不进调度（§5.7）：需先 `requeue_readmit`；
        - D10：优先按 `acb.id` 精确命中；`acb.id` 未命中时回退按 `task_id` 找
          ADMITTED 的 ACB（兼容 `_finalize` 推入的 `task_id` 键）；
        - D11：被跳过的键记录原因到 `_drain_skipped`（不再静默 continue）。
        """
        while True:
            key = self.scheduler.pop()
            if key is None:
                break
            acb = self.acbs.get(key)
            if acb is None:
                acb = next((a for a in self.acbs.values()
                            if a.task_id == key and a.state is TaskState.ADMITTED
                            and not a.needs_readmit), None)
            if acb is None:
                self._drain_skipped.append((key, "no_admitted_acb"))
                continue
            if acb.needs_readmit:
                self._drain_skipped.append((key, "needs_readmit"))
                continue
            if acb.state is not TaskState.ADMITTED:
                # PLANNED / 已终态等：按设计不调度，但记录原因（D11）
                self._drain_skipped.append((key, f"state={acb.state.value}"))
                continue
            self._run_one(acb)

    def _finalize(self, acb: ACB) -> None:
        plan = self.plans.get(acb.run_id)
        if plan and acb.task_id in plan["tasks"]:
            plan["tasks"][acb.task_id]["state"] = acb.state
        # §2.4：_finalize 落盘（K-3：在结算/归档**之前**，保留活跃预留供 recover 恢复）
        self.checkpoints.save(acb)
        if acb.state in (TaskState.FAILED, TaskState.CANCELLED, TaskState.DEAD_LETTER,
                         TaskState.DONE, TaskState.DONE_PARTIAL, TaskState.QUARANTINED):
            # 终态 + QUARANTINED 结算全部预留（文档 §5.5：未消耗部分归还，已消耗留账）
            for rid in list(acb.reservation_ids):
                self.budget.settle(rid)
        if acb.state in TASK_TERMINAL and acb.budget_node is not None:
            # §5.3（K-13）：终态且无 ACTIVE 预留 → 归档（consumed 折入父级）
            if self.budget.archive_node(acb.budget_node.node_id):
                acb.budget_node = None
        self.slots.release(acb.id)
        if acb.state in (TaskState.FAILED, TaskState.CANCELLED, TaskState.DEAD_LETTER):
            # D14：patch 后旧 ACB 的 task_id 可能已不在 plan["tasks"] 中 → 用 .get() 安全取值
            task = plan.get("tasks", {}).get(acb.task_id) if plan else None
            plan_version = task.get("plan_version", 1) if isinstance(task, dict) else 1
            self.propagate_failure(acb.task_id, plan_version)
        # level-triggered 对账（文档 §5.2.1）：事件即真相
        if plan:
            self.reconcile.reconcile_events(acb.run_id)
            # D9：重建 ready 集后真正 push 到调度器（原返回值被丢弃 → 后继永不调度）。
            # 只调度「因本任务完成而新就绪」的后继：早已就绪（无前驱）的任务仍由
            # run_task/dispatch_and_run 显式驱动，避免把非终态任务一并拉起（A2/A11/I12）。
            ready = set(self.reconcile.rebuild_ready_set(acb.run_id))
            for edge in plan.get("edges", []) or []:
                if isinstance(edge, dict):
                    src, dst = edge.get("from"), edge.get("to")
                else:
                    src, dst = edge[0], edge[1]
                if src == acb.task_id and dst in ready:
                    self.scheduler.push(dst)

    def escalate_quarantined(self, acb: ACB) -> Result:
        """人工 escalate：QUARANTINED → DEAD_LETTER，释放预算并传播 failure（文档 §5.5）。"""
        if acb.state is not TaskState.QUARANTINED:
            return Result(Status.INVALID_INPUT, {"state": acb.state.value})
        r = advance(acb, "replan_budget_exhausted")     # QUARANTINED → DEAD_LETTER（合法边）
        if r != Status.OK:
            return r
        for rid in list(acb.reservation_ids):
            self.budget.settle(rid)
        self.slots.release(acb.id)
        plan = self.plans.get(acb.run_id)
        if plan and acb.task_id in plan["tasks"]:
            self.propagate_failure(acb.task_id,
                                   plan["tasks"][acb.task_id].get("plan_version", 1))
        self.checkpoints.save(acb)              # §2.4：escalate 落盘
        return Result(Status.OK, {"state": acb.state.value})

    # ---- 恢复（文档 §5.8 / §4.3） -------------------------------
    def recover(self, ref: str, *, resume_policy: str = "RESUME") -> ACB:
        """崩溃恢复（§4.3）：journal 重建 → ACB 重建 → 事件重放 → probe → 终态策略。

        - `resume_policy="RESUME"`：probe 三态对账（`True` → CONFIRMED；`None` → 隔离）；
        - `resume_policy="COMPENSATE"`：**终态策略**（清 staged，补偿后不恢复执行）。
        """
        data = self.checkpoints.load(ref)
        events = self.event_log.all()                  # 锁内快照（单一数据源）
        mine = [e for e in events if e.producer == data.get("acb_id")]
        self.effects.rebuild_from(mine)
        self.comp_journal.rebuild_from(mine)
        acb = self._restore_acb(data, resume_policy)
        # 恢复已应用事件集合（去重键持久化，避免重放重复投影）
        persisted = data.get("applied_event_ids")
        if persisted:
            self._applied_event_ids.update(persisted)
        # ① 先重放事件（把外部状态对齐），再 probe
        self._replay_events(from_seq=int(data.get("event_cursor") or 0), dedup=True)
        for entry in self.effects.pending(acb.id):     # 含 INTENT
            probe = self.effects.probe_one(entry.idem_key)
            if probe == "True":
                self.effects.confirm(entry.idem_key, {"recovered": True})
            elif probe == "None":
                acb.state = TaskState.QUARANTINED
                acb.failure = "NEEDS_RECONCILIATION"
        # ② COMPENSATE 是终态策略：补偿前丢弃 staged，补偿后不恢复执行
        if resume_policy == "COMPENSATE":
            acb.staged = {}
            plan = self.comp_journal.resume(acb, self.workspace)
            for e in plan["retry"]:
                self._run_one_compensation(acb, e)
            acb.staged = {}                     # 终态策略：补偿后不恢复执行
            # 重试后重新读取条目终态：escalate（FAILED）/ pivot 必须被消费
            entries = [e for e in self.comp_journal._entries.values()
                       if e.acb_id == acb.id]
            escalate = bool(plan["escalate"]) or any(
                e.state == "FAILED" for e in entries)
            pivot = bool(plan["pivot"]) or any(
                e.state == "PIVOT" for e in entries)
            if escalate:
                acb.state = TaskState.QUARANTINED
                acb.failure = "COMPENSATE_FAILED"
            elif pivot:
                acb.state = TaskState.DEAD_LETTER
                acb.failure = "COMPENSATE_PIVOT"
            else:
                acb.state = TaskState.DONE_PARTIAL
                acb.failure = None
            # 终态策略：结算全部预留 → 归档预算节点 → 持久化终态
            for rid in list(acb.reservation_ids):
                self.budget.settle(rid)
            if (acb.budget_node is not None
                    and self.budget.archive_node(acb.budget_node.node_id)):
                acb.budget_node = None
            if acb.state in (TaskState.DONE_PARTIAL, TaskState.DEAD_LETTER):
                self.slots.release(acb.id)
            self.checkpoints.save(acb)
        # ③ 恢复后全树必查（K-14）；失败 → QUARANTINED + slots.release
        try:
            self.budget.check_invariants()
        except InvariantError:
            acb.state = TaskState.QUARANTINED
            acb.failure = acb.failure or "INVARIANT_BROKEN"
            self.slots.release(acb.id)
            self.checkpoints.save(acb)
        self.acbs[acb.id] = acb
        self.run_coordinator.register(acb)
        return acb

    def requeue_readmit(self, acb: ACB) -> ACB:
        """§5.7（K-9）：`needs_readmit` 的 ACB 重新准入（清标记 → 走 dispatcher）。

        成功重新准入后必须清理旧 ACB 的槽位 / 预留 / 预算节点，否则旧账会
        与新准入叠加（槽位泄漏 + `subtree_reserved` 虚高）。
        """
        acb.needs_readmit = False
        plan = self.plans.get(acb.run_id)
        task = plan.get("tasks", {}).get(acb.task_id) if plan else None
        if task is None:
            self.checkpoints.save(acb)
            return acb
        token = self._effective_token(acb)
        old_node = acb.budget_node
        old_rids = list(acb.reservation_ids)
        # 先释放旧槽位，避免 max_concurrent 下自阻塞；失败分支再补回
        self.slots.release(acb.id)
        fresh = self.dispatcher.try_admit(task, acb.run_id, token=token)
        if fresh.state is TaskState.ADMITTED:
            for rid in old_rids:
                self.budget.settle(rid)
            acb.reservation_ids = []
            if old_node is not None:
                if not self.budget.remove_node(old_node.node_id):
                    self.budget.archive_node(old_node.node_id)
                acb.budget_node = None
            self.acbs.pop(acb.id, None)
            self.acbs[fresh.id] = fresh
            self.checkpoints.save(fresh)        # §2.4：requeue 成功落盘
            return fresh
        self.slots.try_acquire(acb.id)
        acb.needs_readmit = True
        self.checkpoints.save(acb)              # §2.4：requeue 失败落盘
        return acb

    def _restore_acb(self, data: dict, resume_policy: str = "RESUME") -> ACB:
        """从 checkpoint 重建 ACB；失败改 `needs_readmit`（**不抛**，§4.3）。"""
        try:
            acb = ACB(id=data["acb_id"], run_id=data["run_id"],
                      task_id=data["task_id"],
                      logical_subgoal_id=data.get("logical_subgoal_id", ""))
            acb.generation = data["generation"] + 1
            acb.state = TaskState(data["state"])
            acb.turn_seq = data["turn_seq"]
            acb.staged = dict(data["staged"])
            acb.read_set = dict(data["read_set"])
            acb.base_version = dict(data["base_version"])
            # §2.4：优先用 per-ACB 物化游标；旧 checkpoint 回退到 `cursor`
            acb.cursor = data.get("materialized_cursor", data.get("cursor", 0))
            acb.pre_image = dict(data.get("pre_image") or {})
            acb.last_committed_seq = int(data.get("last_committed_seq", 0) or 0)
            acb.aging_deadline = data.get("aging_deadline")
            acb.step = data.get("effect_journal_offset", 0)
            acb.hitl_result = data.get("hitl_result")
            acb.rebase_attempts = data.get("rebase_attempts", 0)
            acb.retry_budget = data.get("retry_budget", 3)
            acb.failure = data.get("failure")
            acb.spec = dict(data.get("spec") or {})
            acb.deadline = data.get("deadline")
            acb.no_progress_steps = data.get("no_progress_steps", 0)
            acb.needs_readmit = bool(data.get("needs_readmit", False))
            self._restore_budget(acb, data)
            # §2.4：`slots_active` 交叉校验——非终态快照若不含本 ACB 槽位，
            # 说明快照摄于槽位释放之后（如 `_finalize` 末），标记 needs_readmit。
            snap_active = data.get("slots_active")
            if (isinstance(snap_active, list) and snap_active
                    and acb.state not in TASK_TERMINAL
                    and acb.id not in snap_active):
                acb.needs_readmit = True
            if not self.slots.try_acquire(acb.id):
                acb.state = TaskState.QUARANTINED
                acb.failure = "NO_SLOT"
            self.bump_epoch()
            return acb
        except Exception:  # noqa: BLE001  # 恢复失败 → needs_readmit，不抛
            acb = ACB(id=data.get("acb_id", ""), run_id=data.get("run_id", ""),
                      task_id=data.get("task_id", ""))
            acb.state = TaskState.QUARANTINED
            acb.failure = "restore_failed"
            acb.needs_readmit = True
            return acb

    def _restore_budget(self, acb: ACB, data: dict) -> None:
        """恢复预算节点与预留（§5.1 / K-6）。

        - **逐笔账本为准**：ACTIVE 按 `amount`、已结算按 `consumed` 重建
          `self_reserved`；`consumed = Σconsumed`；
        - 聚合快照仅作交叉校验，不一致 → 保持 `needs_readmit=True`；
        - 无 `reservations`（旧 checkpoint）→ 退回聚合快照兼容路径，
          但**不能**据此把 `needs_readmit` 清成 False（无逐笔依据）。
        """
        node_id = data.get("budget_node_id")
        if not node_id:
            acb.reservation_ids = []
            acb.budget_node = None
            acb.needs_readmit = True
            return

        node = self.budget._nodes.get(node_id)
        if node is None:
            path = data.get("budget_path") or []
            if path:
                limits = data.get("budget_path_limits")
                if not isinstance(limits, dict):
                    limits = {node_id: data.get("budget_limits")}
                try:
                    self._restore_path(path, limits=limits)
                except (ValueError, KeyError, RuntimeError):
                    acb.reservation_ids = []
                    acb.budget_node = None
                    acb.needs_readmit = True
                    return
                node = self.budget._nodes.get(node_id)
            if node is None:
                limits = int(data.get("budget_limits") or 0)
                parent_id = data.get("budget_parent_id") or "run"
                if limits <= 0:
                    acb.reservation_ids = []
                    acb.budget_node = None
                    acb.needs_readmit = True
                    return
                node = self.budget.create_node(node_id, limits, parent_id=parent_id)
                if node is None:
                    acb.reservation_ids = []
                    acb.budget_node = None
                    acb.needs_readmit = True
                    return

        # `reservations` 键存在（含空列表）→ 新格式逐笔账本；缺失 → 旧格式
        has_ledger = isinstance(data.get("reservations"), list)
        consistent = True
        if has_ledger:
            restored: list[str] = []
            for item in data.get("reservations") or []:
                rid = item.get("rid")
                if not rid:
                    continue
                existing = self.budget._reservations.get(rid)
                # 仅当既有句柄属于**另一活节点**时跳过；已归档的 tombstone 可复用
                if (existing is not None and existing[0] is not node
                        and existing[0] in self.budget._nodes.values()):
                    continue
                resv = Reservation(rid, item.get("node_id") or node_id,
                                   int(item.get("amount", 0)),
                                   int(item.get("consumed", 0)),
                                   item.get("state", "ACTIVE"))
                node.reservations[rid] = resv
                self.budget._reservations[rid] = (node, resv)
                restored.append(rid)
            # 逐笔重建：ACTIVE 占 amount；SETTLED/RELEASED/ARCHIVED 只占 consumed
            node.folded_consumed = int(data.get("budget_folded_consumed", 0) or 0)
            node.self_reserved = sum(
                r.amount if r.state == "ACTIVE" else r.consumed
                for r in node.reservations.values()) + node.folded_consumed
            node.consumed = (sum(r.consumed for r in node.reservations.values())
                             + node.folded_consumed)
            # 聚合快照只作交叉校验；不一致 → 保持 needs_readmit（不覆盖）
            expected_self = int(data.get("budget_self", node.self_reserved))
            expected_consumed = int(data.get("budget_consumed", node.consumed))
            consistent = (node.self_reserved == expected_self
                          and node.consumed == expected_consumed)
            acb.reservation_ids = restored
        else:
            # 兼容路径：旧 checkpoint 无逐笔账本 → 用聚合快照（不可清 needs_readmit）
            node.folded_consumed = int(data.get("budget_folded_consumed", 0) or 0)
            if not node.children:
                node.self_reserved = int(data.get("budget_self", 0))
                node.consumed = int(data.get("budget_consumed", 0))
            restored = []
            consumed = node.consumed
            for index, rid in enumerate(list(data.get("reservation_ids") or [])):  # legacy 字段
                existing = self.budget._reservations.get(rid)
                if (existing is not None and existing[0] is not node
                        and existing[0] in self.budget._nodes.values()):
                    continue
                if index == 0:
                    resv = Reservation(rid, node_id, node.self_reserved, consumed)
                else:
                    resv = Reservation(rid, node_id, 0, 0, "SETTLED")
                node.reservations[rid] = resv
                self.budget._reservations[rid] = (node, resv)
                restored.append(rid)
            acb.reservation_ids = restored
            consistent = False

        old_subtree = node.subtree_reserved
        node.subtree_reserved = node.self_reserved + sum(
            c.subtree_reserved for c in node.children)
        # §2.4：`budget_subtree` 交叉校验（叶节点严格；有 children 时子树按子节点重算，
        # 可与快照合法偏离 → 不据此判不一致）
        if not node.children:
            expected_subtree = int(data.get("budget_subtree", node.subtree_reserved))
            if expected_subtree != node.subtree_reserved:
                consistent = False
        self._propagate_subtree_delta(node, node.subtree_reserved - old_subtree)
        acb.budget_node = node
        acb.needs_readmit = acb.needs_readmit or not consistent

    def _restore_path(self, path: list[str], limits=None) -> list:
        """§5.2（K-7）：自顶向下重建**任意深度**路径。

        - `parent` 恒为路径上的前一个节点（首节点挂到 `run`）；
        - checkpoint 只持久化目标节点的 limits 时，中间节点用同一上限兜底；
        - 环检测 + 失败时逆序回滚已建节点。
        """
        if len(path) != len(set(path)):
            raise ValueError("path has cycle")
        limits = limits or {}
        default_limit = 0
        for value in limits.values():
            try:
                default_limit = max(default_limit, int(value or 0))
            except (TypeError, ValueError):
                continue
        built: list = []
        try:
            for i, nid in enumerate(path):
                existing = self.budget._nodes.get(nid)
                if existing is not None:
                    built.append(existing)
                    continue
                parent_id = path[i - 1] if i > 0 else "run"
                lim = int(limits.get(nid) or 0)
                if lim <= 0:
                    lim = default_limit
                if lim <= 0:
                    raise RuntimeError(f"path restore failed at {nid}")
                node = self.budget.create_node(nid, lim, parent_id=parent_id)
                if node is None:
                    raise RuntimeError(f"path restore failed at {nid}")
                built.append(node)
            return built
        except Exception:
            for b in reversed(built):
                if b is not self.budget.root:
                    try:
                        self.budget.remove_node(b.node_id)
                    except Exception:  # noqa: BLE001
                        pass
            raise

    def _replay_events(self, from_seq: int, dedup: bool = True) -> int:
        """§4.3：从 `from_seq` 重放事件，真正调用 workspace 投影。

        投影按 `(resource, version)` 做版本守卫（幂等）；`event_id` 去重集合
        会写入 checkpoint（`applied_event_ids`），跨进程恢复后仍可去重。
        """
        applied = 0
        for ev in self.event_log.all():
            if ev.seq < from_seq:
                continue
            if (dedup and ev.event_id
                    and ev.event_id in self._applied_event_ids):
                continue
            self.workspace.apply_event(ev)
            if dedup and ev.event_id:
                self._applied_event_ids.add(ev.event_id)
            applied += 1
        return applied

    @staticmethod
    def _propagate_subtree_delta(node, delta: int) -> None:
        parent = node.parent
        while parent is not None:
            parent.subtree_reserved += delta
            parent = parent.parent
