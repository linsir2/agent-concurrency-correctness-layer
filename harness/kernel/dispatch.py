"""DispatchService —— 任务执行 / 调度队列 / tick 驱动 / claim fencing（S3.3 迁出）。

搬出的是「执行与调度」整簇（29 个方法）：

- 执行入口与 pool 调度：`run_task` / `_in_worker` / `_current_worker_id` /
  `_run_one_inline` / `_run_task_pooled` / `_worker_main` / `_await_completion`；
- 单步执行体与队列消费：`run_one` / `drain_scheduler` /
  `_drain_scheduler_claimed` / `_drain_skip_reason` / `_is_current_plan_version`；
- 超时驱动面与排空：`tick` / `_tick_candidates` / `_pending_deadlines` /
  `next_deadline_ms` / `pump` / `drain`；
- aging（墙钟）：`arm_aging` / `aging_timeout` / `readmit_after_aging`；
- claim fencing：`_exec_claim_epoch` / `_bind_claim_epoch` / `_unbind_claim_epoch` /
  `claim_scope` / `claim_fenced` / `_claim_snapshot` / `claim_window_s`，以及
  监督者回收 `supervisor_note_crash`。

约定（与 `kernel/tasks.py` / `kernel/children.py` 一致）：

- `DispatchDeps` 只放**稳定对象引用**与 late-lookup callable；可变配置/容器一律在
  调用时读 `self.state`（`tls` / `pool` / `worker_ids` / `timers_fired` / `draining` /
  `acbs` / `plans` / `drain_skipped` / `tick_skipped` / `scheduler_mode` / `aging_ms` /
  `aging_max_attempts` / `pool_timeout_s` / `drain_timeout_s` / `max_lease_ttl_s`）。
  冻结进 deps 会读到旧值。
- **被测试打桩的调用一律 late lookup**：`tests/test_s13_scheduler.py` 在实例上替换
  `h.run_one` / `h.drain_scheduler`，`tests/test_s13_hitl.py` 在实例与**类**上替换
  `h.tick` / `Harness._hitl_gate`。因此哪怕调用方与被调用方都在本服务内，
  `drain_scheduler` / `_drain_scheduler_claimed` / `_worker_main` / `run_task` /
  `pump` 也一律走 `self.deps.*`。
- 仍留在 Harness / TaskService 上的方法（`_finalize` / `_safe_save` /
  `_transition_lock` / `_restore_acb` / `_reap_expired_tokens` / `is_paused` /
  `bump_epoch` …）以 **lambda** 注入，调用时才做属性查找；跨服务只用 TaskService 的
  **公开**方法（`requeue_readmit`）。
- 本模块的构造**不产生任何事件**；`run_task` / `_worker_main` 的 `try/finally`
  所有权逐字保留（不得拆分）。
- 事件种类/payload/顺序、锁序、构造顺序全部保持不变（纯位移）。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any, Callable

from ..orchestration import _UNSET
from ..types import ACB, TASK_TERMINAL, Result, Status, TaskState, advance

# S13d §2.5 P0-9：worker 内嵌套 `run_task` 一律内联；该上限只用于**防递归**
# （不是 spawn 的父子层数 `spawn_depth`，两者语义不同）。
_MAX_INLINE_DEPTH = 8


@dataclass(frozen=True)
class DispatchDeps:
    # ---- 稳定对象引用（构造期注入，不随配置变化）----
    scheduler: Any
    run_coordinator: Any
    intents: Any
    budget: Any
    checkpoints: Any
    clock: Any
    hitl: Any
    reconcile: Any
    timers: Any
    runtime: Any
    slots: Any
    tasks: Any                     # TaskService：只用**公开**方法（requeue_readmit）
    # ---- 仍在 Harness 上 / 会被测试打桩：late lookup，绝不传绑定方法 ----
    run_one: Callable[..., Any]            # h.run_one        —— 实例打桩
    tick: Callable[..., Any]               # h.tick            —— 实例打桩
    drain: Callable[..., Any]              # h.drain_scheduler —— 实例打桩
    hitl_gate: Callable[..., Any]          # h._hitl_gate      —— 实例 + 类打桩
    terminal_status: Callable[..., Any]    # TaskService 静态方法
    finalize: Callable[..., Any]           # h._finalize
    safe_save: Callable[..., bool]         # h._safe_save
    transition_lock: Callable[..., Any]    # h._transition_lock
    clear_admission: Callable[..., None]   # h._clear_admission
    dead_letter_after_replan: Callable[..., None]   # h._dead_letter_after_replan
    approval_slot: Callable[..., Any]      # h._approval_slot
    record_approval: Callable[..., None]   # h._record_approval
    reap_expired_tokens: Callable[..., int]         # h._reap_expired_tokens
    restore_acb: Callable[..., Any]        # h._restore_acb
    is_paused: Callable[..., bool]         # h.is_paused（Admin 暂停，留在 Harness）
    bump_epoch: Callable[..., int]         # h.bump_epoch（写者 epoch，留在 Harness）


class DispatchService:
    """执行 / 调度 / tick / claim fencing 的唯一实现（Harness 仅留同名委托，S5 删除）。"""

    def __init__(self, deps: DispatchDeps, state) -> None:
        self.deps = deps
        self.state = state

    # ================================================== 执行入口 / pool 调度

    def run_task(self, acb: ACB, decisions, crash_at=None) -> Status | Result:
        """外部直接调用入口：注册 + push 到调度器 + 执行 + 消费调度器。

        S13d §2.5：
        - `scheduler_mode="sync"`（默认）保持原路径：push → **内联 `run_one`**
          → `finally drain`（不改动，36 个同步用例零破坏）；
        - `scheduler_mode="pool"`：push → 提交 worker（claim + `run_one` + drain），
          本线程等待 `_await_completion`（`CrashInjected` 必须重抛）；
        - **worker 内嵌套 `run_task`** → 一律内联（P0-9：池满时父等子会永久死锁），
          并由 `_tls.depth` 防递归。
        """
        self.state.acbs[acb.id] = acb
        self.deps.run_coordinator.register(acb)
        # S13a P0-3：唯一超时驱动面（每轮调用）；recover 结束也调一次。
        self.deps.tick()
        # 修 B-1：`runtime.run` 会无条件置 RUNNING —— 必须在进入 `run_one`
        # **之前**判态，否则恢复出的 WAITING_APPROVAL 审批被绕过。
        gate = self.deps.hitl_gate(acb)
        if gate is not None:
            return gate                     # 不 push、不 finalize（槽位已在等待态释放）
        if acb.state in TASK_TERMINAL:
            return self.deps.terminal_status(acb)   # tick 已在本次调用中结算过期审批
        if acb.state is TaskState.INADMISSIBLE and acb.failure == "AGING_TIMEOUT":
            # aging 已判「超时未调度」→ 不得再借 `runtime.run` 无条件置 RUNNING 执行。
            return Result(Status.DEADLINE_EXCEEDED, {"reason": "AGING_TIMEOUT"})
        if self._in_worker():
            # P0-9：worker 内嵌套 → 内联（不入队），否则池满时无 worker 可 pop。
            return self._run_one_inline(acb, decisions, crash_at=crash_at)
        self.deps.scheduler.push(acb.id)            # D10：调度器存 acb.id（不再存 task_id）
        if self.state.scheduler_mode == "pool":
            return self._run_task_pooled(acb, decisions, crash_at=crash_at)
        try:
            return self.deps.run_one(acb, decisions, crash_at=crash_at)
        finally:
            self.deps.drain()            # D12：异常路径（含 CrashInjected）也消费队列

    def _in_worker(self) -> bool:
        return bool(getattr(self.state.tls, "in_worker", False))

    def _current_worker_id(self) -> str:
        wid = getattr(self.state.tls, "worker_id", "")
        return wid or f"local-{threading.get_ident()}"

    def _run_one_inline(self, acb: ACB, decisions=None, crash_at=None):
        """worker 内的嵌套执行（**不入队**）；`_tls.depth` 防递归。

        X4：内联子**必须 claim**（以父 worker 身份）。否则内联执行期间若队列
        中有该子的 ready 键（`_finalize` 的后继 push），另一 worker 可重 claim
        并双跑。claim 不可得（已被他人持有）→ 不内联、记 `inline_claim_held`。
        """
        depth = int(getattr(self.state.tls, "depth", 0) or 0)
        if depth >= _MAX_INLINE_DEPTH:
            # S14 K-71(a)：这是**终局**，不是可重试的普通拒绝。走状态机显式边
            # （ADMITTED/RUNNING → FAILED）+ `_finalize` 传播，Run 才能 finalize
            # 且事件流能看到原因（旧写法返回 INVALID_INPUT 会被调用方当可重试）。
            self.state.drain_skipped.append((acb.id, "inline_depth_exceeded"))
            if advance(acb, "inline_depth_exceeded") == Status.OK:
                acb.failure = "INLINE_DEPTH_EXCEEDED"
                self.deps.intents.revoke_all(acb.id)
                self.deps.finalize(acb)
            return Result(Status.GUARD_VIOLATION,
                          {"reason": "INLINE_DEPTH_EXCEEDED",
                           "max_inline_depth": _MAX_INLINE_DEPTH})
        self.state.tls.depth = depth + 1
        parent = getattr(self.state.tls, "running_acb", None)
        claim = None
        if self.state.scheduler_mode == "pool":
            claim = self.deps.scheduler.claim(acb.id, self._current_worker_id())
            if claim is None:
                self.state.drain_skipped.append((acb.id, "inline_claim_held"))
                self.state.tls.depth = depth
                return Result(Status.REJECT_STALE, {"reason": "inline_claim_held"})
            # 审计：内联子记录父 ACB（不参与 fence 判定）。
            if parent is not None and parent is not acb:
                acb.spec["inline_parent"] = parent.id
        try:
            if claim is not None:
                with self.claim_scope(acb, claim):
                    return self.deps.run_one(acb, decisions, crash_at=crash_at)
            return self.deps.run_one(acb, decisions, crash_at=crash_at)
        finally:
            self.state.tls.depth = depth
            if claim is not None:
                self.deps.scheduler.release_claim(acb.id, self._current_worker_id())

    def _run_task_pooled(self, acb: ACB, decisions, crash_at):
        """提交一个 worker 并等待；**不在本线程 drain**（单一 pop 所有者 = worker）。"""
        if self.state.pool is None:                 # 防御：mode=pool 必有池
            return self.deps.run_one(acb, decisions, crash_at=crash_at)
        worker_id = f"W{next(self.state.worker_ids)}-{acb.id}"
        future = self.state.pool.submit(self._worker_main, acb, decisions,
                                        crash_at, worker_id)
        return self._await_completion(future)

    def _worker_main(self, acb: ACB, decisions, crash_at, worker_id: str):
        """worker 入口：claim → `run_one` → drain（异常时不 drain，防双跑）。"""
        self.state.tls.in_worker = True
        self.state.tls.worker_id = worker_id
        self.state.tls.depth = 0
        crashed = False
        try:
            claim = self.deps.scheduler.claim(acb.id, worker_id)
            if claim is None:
                # 同一 ACB 的有效租约归其他 worker → 绝不双跑。
                self.state.drain_skipped.append((acb.id, "claim_held"))
                return Status.DEADLINE_EXCEEDED
            try:
                # X1：fence 的 epoch 绑在**本线程执行上下文**，不写共享 ACB。
                with self.claim_scope(acb, claim):
                    return self.deps.run_one(acb, decisions, crash_at=crash_at)
            finally:
                self.deps.scheduler.release_claim(acb.id, worker_id)
        except BaseException:
            # CrashInjected（BaseException）必须穿透到等待侧；且**不得再 drain**
            # （否则重复 ready 键会在崩溃后被再次消费 → 双跑）。
            crashed = True
            raise
        finally:
            try:
                if not crashed:
                    # 单一 pop 所有者：worker 自己消费队列（`in_worker` 仍为 True，
                    # 期间嵌套 run_task 继续内联 → 无池满死锁）。
                    self.deps.drain()
            finally:
                self.state.tls.in_worker = False
                self.state.tls.depth = 0
                self.state.tls.worker_id = ""

    # ------------------------------------------------------- worker 等待

    def _await_completion(self, future, *, timeout_s: float | None = None):
        """等待 worker 完成：超时可取消、异常（含 `BaseException`）在等待侧重抛。

        `ThreadPoolExecutor` 会把 `BaseException` 收进 `Future`；不调 `result()`
        则既不抛也不结束。这里统一 `result(timeout)`，并保证 `CrashInjected`
        不被吞掉（S13d §2.5 / B-3）。
        """
        timeout = self.state.pool_timeout_s if timeout_s is None else timeout_s
        try:
            return future.result(timeout=timeout)
        except TimeoutError:
            future.cancel()                    # 可取消（已运行则为 best-effort）
            self.state.drain_skipped.append(("", "worker_timeout"))
            raise
        finally:
            if not future.done():
                future.cancel()

    # ======================================= tick：超时驱动面 / 外部驱动器

    def tick(self, now_ms: int | None = None, *,
             persist_scan: bool = True) -> None:
        """S13a P0-3：唯一超时驱动面（fail-closed）。

        supervisor / 调度器每轮调用；`recover()` 结束也调一次（否则恢复出的
        过期审批永不结算）。**不引入后台线程**（`ManualClock` 不可共享，会 flaky）。

        扫描与审批提交在 `HITL._lock` 内完成 CAS：若已有决策（批准/拒绝），
        跳过超时（决策优先，交给 `run_task` 的入口判态处理），避免双写。
        F2：`hitl_pending` 不可用 → `APPROVAL_STATE_LOST`（不再 `continue` 永久挂起）。
        F4：`advance` 返回值必须校验（非法迁移 → 不结算）。
        """
        now = int(self.deps.clock.now_ms() if now_ms is None else now_ms)
        # S24-11：先结算到期定时器（只标记 + 返回记录，handler 由宿主幂等执行）
        if self.deps.timers is not None:
            self.state.timers_fired = self.deps.timers.fire_due(now)
        # K-69：候选集 = 内存 ∪ **持久真相**（未 `recover` 的等待态也要能结算）。
        # 锁序红线（S14 §2.2）：`_tick_candidates` 读完即返回，下面的 `_finalize`
        # **不持 store 锁** —— 否则 budget(1) 套 store(5) 反向获取 → LockOrderViolation。
        candidates = self._tick_candidates(persist_scan=persist_scan)
        for acb in candidates:
            if acb.state is not TaskState.WAITING_APPROVAL:
                continue
            with self.deps.hitl._lock:
                if acb.state is not TaskState.WAITING_APPROVAL:
                    continue
                approval_id, deadline = self.deps.approval_slot(acb)
                if approval_id is None or deadline is None:
                    # X1：判终态前先 revoke 租约 + bump epoch + 撤销 claim，
                    # 旧 worker 解锁后的写才会被 fence（否则终态被改回 DONE）。
                    self.supervisor_note_crash(acb)
                    if advance(acb, "timeout") != Status.OK:
                        continue
                    acb.failure = "APPROVAL_STATE_LOST"
                    self.deps.record_approval(acb, "lost", "APPROVAL_STATE_LOST")
                else:
                    if now < deadline:
                        continue
                    if self.deps.hitl.peek(acb) is not _UNSET:
                        continue            # 已有决策 → 不判超时
                    self.supervisor_note_crash(acb)
                    if advance(acb, "timeout") != Status.OK:
                        continue
                    acb.failure = "APPROVAL_TIMEOUT"
                    self.deps.record_approval(acb, "timeout", "APPROVAL_TIMEOUT")
            self.deps.intents.revoke_all(acb.id)  # 释放租约
            self.deps.finalize(acb)              # 结算预算 / 释放槽位
        # S21 K-99：惰性回收过期租约（与 aging/HITL 共用同一驱动面）
        try:
            self.deps.intents.reap_expired(now)
        except Exception:  # noqa: BLE001  # 回收失败不影响超时结算
            pass
        # S22：回收过期的门面 token（只作用于 `spec["rsc_token"]`，不动普通 ACB）
        self.deps.reap_expired_tokens(now)
        # S13d §2.5 / B-4：aging（墙钟）与 HITL 超时共用**唯一**驱动面；
        # 扫描入口在 ReconcileLoop（`scan_deadlines`）。S24-P0：复用本轮的
        # 候选集（含持久水合项），否则未 `recover` 的 aging 期限永远扫不到。
        self.deps.reconcile.scan_deadlines(now, candidates=candidates)

    def _tick_candidates(self, *, persist_scan: bool = True,
                         for_sweep: bool = False) -> list[ACB]:
        """`tick` 的候选集 = 内存 ∪ **持久真相**（`persist_scan=False` 时仅内存）。

        为什么必须含持久：S13a 的 `tick` 只遍历 `self.acbs`，而 `recover()` 一次
        只恢复 1 个 ACB → **未 `recover` 的等待态永远不在扫描范围**，反复调
        `tick` 也结算不到（K-69 的真 bug，不是「记得调 tick」能解决的）。

        `persist_scan=False` 供 `recover()` 使用：恢复尚未完成时，别的 ACB 可能
        只恢复了一半，**此时结算它们是错的**（S13 评审 R-5）。恢复结束后的
        首次 `pump()` 会补上。

        锁序红线（S14 §2.2）：本方法内 `refs_all()` / `load_strict()` **读完即
        返回**，调用方在**无 store 锁**下再调 `_finalize`；否则 `budget(1)` 会
        套在 `store(5)` 里 → 反向获取 → `LockOrderViolation`。

        失败不静默：枚举/读取/水合的任一步失败都记 `_tick_skipped`。
        """
        out: list[ACB] = []
        seen: set[str] = set()
        for acb in list(self.state.acbs.values()):
            seen.add(acb.id)
            out.append(acb)
        if not persist_scan:
            return out
        try:
            refs = self.deps.checkpoints.refs_all()
        except Exception:                       # noqa: BLE001 —— 枚举失败降级为仅内存
            self.state.tick_skipped.append(("", "refs_all_failed"))
            return out
        for acb_id, ref in refs:
            if acb_id in seen or ref is None:
                continue
            try:
                payload = self.deps.checkpoints.load_strict(ref)
            except Exception:                   # noqa: BLE001
                self.state.tick_skipped.append((acb_id, "tick_load_failed"))
                continue
            try:
                out.append(self.deps.restore_acb(payload, for_sweep=for_sweep))
            except Exception:                   # noqa: BLE001
                self.state.tick_skipped.append((acb_id, "tick_hydrate_failed"))
        return out

    def _pending_deadlines(self) -> list[tuple[int, str]]:
        """当前**待结算**项 `[(deadline_ms, acb_id)]`（HITL 超时 ∪ aging）。"""
        out: list[tuple[int, str]] = []
        # S24-P0：**只读**扫描 —— 不得 bump epoch / 占槽位（否则旁观者会 fence 写者）
        for acb in self._tick_candidates(for_sweep=True):
            if acb.state is TaskState.WAITING_APPROVAL:
                _, deadline = self.deps.approval_slot(acb)
            elif acb.state is TaskState.ADMITTED and acb.aging_deadline is not None:
                deadline = acb.aging_deadline
            else:
                continue
            if deadline is None:
                continue
            out.append((int(deadline), acb.id))
        return out

    @property
    def skipped(self):
        """调度跳过原因（旧 `Harness._drain_skipped`；唯一对象在 KernelState）。"""
        return self.state.drain_skipped

    def next_deadline_ms(self) -> int | None:
        """最近的到期时刻；`None` = 当前无待结算项。

        供外部驱动器**精确定时**（睡到最近到期点），而不是固定间隔轮询
        （对齐 K8s 的 level-triggered resync 只作兜底）。
        """
        candidates = [d for d, _ in self._pending_deadlines()]
        if self.deps.timers is not None:
            timer_deadline = self.deps.timers.next_deadline_ms()
            if timer_deadline is not None:
                candidates.append(int(timer_deadline))
        return min(candidates, default=None)

    def pump(self, now_ms: int | None = None) -> int:
        """驱动器入口：`tick()` 后返回**仍待结算**项数（`0` = 可退出）。

        典型用法（supervisor，**不引后台线程**）::

            while h.pump() > 0:
                h.tick(h.next_deadline_ms())

        注：`now_ms` 透传给 `tick`，便于 `ManualClock` 精确构造（不用 sleep）。
        """
        self.deps.tick(now_ms)
        pending = len(self._pending_deadlines())
        if self.deps.timers is not None:
            pending += len(self.deps.timers.pending())
        return pending

    # --------------------------------------------- 优雅排空

    def drain(self, timeout_s: float | None = None) -> dict:
        """优雅排空：**不再接受新调度**，等在途任务自然结束。

        返回值：`{"drained": 已终态数, "remaining": 未完成数, "timed_out": bool}`

        与 `cancel` 的区别：drain **不打断**在途任务（避免半提交），只阻止新任务
        被取出执行；超时也不强杀（由调用方决定是否 `kill_run`）。
        """
        import time as _time

        timeout = self.state.drain_timeout_s if timeout_s is None else float(timeout_s)
        self.state.draining = True
        deadline = _time.monotonic() + max(0.0, timeout)
        before = sum(1 for a in self.state.acbs.values() if a.state in TASK_TERMINAL)
        while True:
            self.pump()
            active = [a for a in self.state.acbs.values() if a.state not in TASK_TERMINAL]
            if not active:
                return {"drained": before, "remaining": 0, "timed_out": False}
            if _time.monotonic() >= deadline:
                return {"drained": before, "remaining": len(active),
                        "timed_out": True}
            _time.sleep(0.005)

    # ================================ 单步执行体 + 调度队列消费（drain）

    def run_one(self, acb: ACB, decisions=None, crash_at=None) -> Status:
        """执行体（§2.6）：`runtime.run` + `_finalize`；**不 push、不 drain**（防递归）。"""
        prev_running = getattr(self.state.tls, "running_acb", None)
        self.state.tls.running_acb = acb          # X4：内联子记录 `inline_parent`
        try:
            return self.deps.runtime.run(acb, decisions or [], crash_at=crash_at)
        finally:
            self.deps.finalize(acb)
            self.state.tls.running_acb = prev_running

    def drain_scheduler(self) -> None:
        """真消费 ready_queue（§2.6）：`pop → 找 ACB → 只启动 ADMITTED`。

        - `run_one` 不 push、不递归 drain → 无无限递归、无双重 run；
        - `needs_readmit` 的 ACB 不进调度（§5.7）：需先 `requeue_readmit`；
        - **D10（S13b P1-5）**：队列键统一 `acb.id`，**删除**「按 task_id 扫第一个
          ADMITTED」兜底 —— 该兜底在 replan 后同 task_id 出现两个 ADMITTED 时会
          跑错代际 ACB / 双跑；
        - 另按 `(run_id, task_id, plan_version)` 过滤陈旧代际（旧 ACB 即使仍
          ADMITTED 也不跑）；
        - D11：被跳过的键记录原因到 `_drain_skipped`（不再静默 continue）；
        - **S13d §2.5**：`pool` 模式在同一把 scheduler 锁内「pop + claim」——
          只有拿到 claim 的 worker 执行（单一 pop 所有者）。
        """
        if self.state.scheduler_mode == "pool":
            self._drain_scheduler_claimed()
            return
        while True:
            key = self.deps.scheduler.pop()
            if key is None:
                break
            reason = self._drain_skip_reason(key)
            if reason is not None:
                self.state.drain_skipped.append((key, reason))
                continue
            self.deps.run_one(self.state.acbs[key])

    def _drain_scheduler_claimed(self) -> None:
        """pool 模式的 drain：pop+claim → 校验 → 执行 → 释放 claim。"""
        worker_id = self._current_worker_id()
        while True:
            got = self.deps.scheduler.pop_claimed(worker_id)
            if got is None:
                break
            key, claim = got
            if claim is None:
                # 有效租约归其他 worker（同 id 的重复 ready 键）→ 绝不双跑。
                self.state.drain_skipped.append((key, "claim_held"))
                continue
            try:
                reason = self._drain_skip_reason(key)
                if reason is not None:
                    self.state.drain_skipped.append((key, reason))
                    continue
                with self.claim_scope(self.state.acbs[key], claim):
                    self.deps.run_one(self.state.acbs[key])
            finally:
                self.deps.scheduler.release_claim(key, worker_id)

    def _drain_skip_reason(self, key: str) -> str | None:
        """`None` = 可调度；否则为 `_drain_skipped` 的原因（D11，不静默）。"""
        acb = self.state.acbs.get(key)
        if acb is None:
            return "unknown_acb"
        if acb.needs_readmit:
            return "needs_readmit"
        if acb.state is not TaskState.ADMITTED:
            # PLANNED / 已终态等：按设计不调度，但记录原因（D11）
            return f"state={acb.state.value}"
        if not self._is_current_plan_version(acb):
            return "stale_plan_version"
        if self.deps.is_paused(acb.run_id):
            # S15 D3：Admin 暂停 —— 不执行但**保留**槽位/预算/节点（可恢复）
            return "run_paused"
        return None

    def _is_current_plan_version(self, acb: ACB) -> bool:
        """D10：同 `(run_id, task_id)` 存在**更新代际** ADMITTED ACB 时，旧代际不调度。

        只比较同一 task 的兄弟 ACB，而非 plan 里该 task 的 `plan_version` ——
        未被 patch 改名的任务（如 successors）仍持有 replan 前准入的 ACB，
        它只有一个 ADMITTED 实例，必须继续可跑（S13-4 执行层）。
        """
        siblings = [a for a in self.state.acbs.values()
                    if a.run_id == acb.run_id and a.task_id == acb.task_id
                    and a.state is TaskState.ADMITTED]
        if len(siblings) <= 1:
            return True
        best = max(int(a.spec.get("plan_version", 0) or 0) for a in siblings)
        return int(acb.spec.get("plan_version", 0) or 0) == best

    # ============================== aging（墙钟 deadline）+ claim fencing

    def arm_aging(self, acb: ACB) -> None:
        """准入成功后起算 aging（`aging_ms=0` = 禁用，不改既有语义）。"""
        if self.state.aging_ms <= 0:
            return
        acb.aging_deadline = int(self.deps.clock.now_ms()) + int(self.state.aging_ms)

    def aging_timeout(self, acb: ACB) -> None:
        """到期未调度：单次 → INADMISSIBLE（可 readmit）+ 结算；累计 N → DEAD_LETTER。

        P1-1：`INADMISSIBLE` 是**非终态**，若不给出路 Run 永不 finalize。故
        ① 每次超时都结算租约/预算/槽位（`fail-closed`，不占资源）；
        ② 超过 `aging_max_attempts` → `aging_exhausted` 走真终态并传播 failure。
        """
        with self.deps.transition_lock(acb):
            if acb.state is not TaskState.ADMITTED:
                return
            count = int(getattr(acb, "aging_count", 0) or 0) + 1
            acb.aging_count = count
            if count >= self.state.aging_max_attempts:
                # 先走 `aging_timeout`（ADMITTED → INADMISSIBLE）再 `aging_exhausted`
                # （INADMISSIBLE → DEAD_LETTER）：只用**已声明**的边，状态机不留暗门。
                if advance(acb, "aging_timeout") != Status.OK:
                    return
                if advance(acb, "aging_exhausted") != Status.OK:
                    return
                acb.failure = "AGING_EXHAUSTED"
                acb.aging_deadline = None
                self.deps.intents.revoke_all(acb.id)
                self.deps.dead_letter_after_replan(acb)      # settle+归档+传播+落盘
                plan = self.state.plans.get(acb.run_id)
                if plan and acb.task_id in plan.get("tasks", {}):
                    plan["tasks"][acb.task_id]["state"] = acb.state
                return
            if advance(acb, "aging_timeout") != Status.OK:
                return
            acb.failure = "AGING_TIMEOUT"
            acb.aging_deadline = None
            self.deps.intents.revoke_all(acb.id)
            for rid in list(acb.reservation_ids):
                self.deps.budget.settle(rid)
            acb.reservation_ids = []
            self.deps.slots.release(acb.id)
            if acb.budget_node is not None:
                node_id = acb.budget_node.node_id
                if not self.deps.budget.archive_node(node_id):
                    self.deps.budget.remove_node(node_id)
                acb.budget_node = None
            self.deps.clear_admission(acb=acb)
            self.deps.safe_save(acb)

    def readmit_after_aging(self, acb: ACB) -> ACB:
        """S13d P1-1：单次 aging 超时后重新准入（`INADMISSIBLE --readmit--> PLANNED`）。

        复用 `requeue_readmit` 的清理序列；`aging_count` 随新 ACB 继承，
        累计到 `aging_max_attempts` 由 `tick` 判 `DEAD_LETTER`（真终态）。
        """
        with self.deps.transition_lock(acb):
            if acb.state is not TaskState.INADMISSIBLE:
                return acb
            if advance(acb, "readmit") != Status.OK:
                return acb
            count = int(getattr(acb, "aging_count", 0) or 0)
            acb.needs_readmit = True
            fresh = self.deps.tasks.requeue_readmit(acb)
            if fresh is not None and fresh is not acb:
                fresh.aging_count = count
                self.deps.safe_save(fresh)
            return fresh if fresh is not None else acb

    def _exec_claim_epoch(self, acb_id: str) -> int | None:
        """当前线程**执行上下文**里的 claim epoch（X1：不读共享 ACB 字段）。"""
        ctx = getattr(self.state.tls, "claim_epochs", None)
        if not ctx:
            return None
        return ctx.get(acb_id)

    def _bind_claim_epoch(self, acb_id: str, epoch: int) -> int | None:
        ctx = getattr(self.state.tls, "claim_epochs", None)
        if ctx is None:
            ctx = {}
            self.state.tls.claim_epochs = ctx
        prev = ctx.get(acb_id)
        ctx[acb_id] = int(epoch)
        return prev

    def _unbind_claim_epoch(self, acb_id: str, prev: int | None) -> None:
        ctx = getattr(self.state.tls, "claim_epochs", None)
        if not ctx:
            return
        if prev is None:
            ctx.pop(acb_id, None)
        else:
            ctx[acb_id] = prev

    def claim_scope(self, acb: ACB, epoch):
        """把 claim epoch 绑定到**当前线程**的执行上下文（worker / 内联 / 测试）。

        `epoch` 可以是 `Claim`（取 `fence_epoch`）或整数。返回上下文管理器；
        退出时恢复原值（内联嵌套继承父 epoch 也用同一机制）。
        """
        value = int(getattr(epoch, "fence_epoch", epoch))
        svc = self

        class _Scope:
            def __enter__(self_inner):
                self_inner._prev = svc._bind_claim_epoch(acb.id, value)
                self_inner._acb_id = acb.id
                return acb

            def __exit__(self_inner, *_exc):
                svc._unbind_claim_epoch(self_inner._acb_id, self_inner._prev)
                return False

        return _Scope()

    def claim_fenced(self, acb: ACB) -> bool:
        """P0-10：写路径 fencing —— 比较**执行上下文 epoch**，不读共享 ACB。

        - 活跃 claim 存在：上下文没有 epoch（外部线程）或 epoch 不等 → fenced；
        - claim 已撤销（tick/supervisor 回收）：上下文 epoch **低于撤销水位** → fenced
          （否则审批超时被在途 worker 解锁后的写覆盖）；
        - sync 模式 / 从未 claim → 恒 `False`（36 个同步用例零破坏）。
        """
        if self.state.scheduler_mode != "pool":
            return False
        ctx = self._exec_claim_epoch(acb.id)
        claim = self.deps.scheduler.claim_of(acb.id)
        if claim is not None:
            if ctx is None:
                return True
            return int(ctx) != int(claim.fence_epoch)
        watermark = self.deps.scheduler.revoked_epoch(acb.id)
        if watermark is not None and ctx is not None:
            return int(ctx) < int(watermark)
        return False

    def _claim_snapshot(self, acb: ACB) -> dict:
        """X7：快照用的 claim 三字段（无锁只读，避免 store ↔ scheduler AB-BA）。"""
        claim = self.deps.scheduler.peek_claim(acb.id)
        if claim is None:
            return {}
        return {"claim_epoch": int(claim.fence_epoch),
                "claim_worker": str(claim.worker_id),
                "claim_lease_until_ms": int(claim.lease_until_ms)}

    def claim_window_s(self) -> int:
        """有界墓碑集合的**总覆盖窗口** = `tombstone_max × max_lease_ttl_s`（K-100）。

        **为什么必须用租约时长而不是固定窗口**：若部署把 `lease_ms` 调大
        （例如 600s），而窗口按固定 30s 记，则窗口会**小于**租约时长 →
        残留 fencing 风险从「可忽略」变成「真实」。公式随部署自动适配。

        **强制点在哪**：真正保证不复活旧 worker 的是 Raft 副本里的**逐墓碑
        年龄判定**（`now - 入墓时刻 >= max_lease_ttl_s` 才允许忘记，见
        `_ReplicatedBudget.prune`）；本函数给出的是「满员墓碑集合能覆盖多久」
        的诊断量，供运维核对部署参数是否落在设计假设内。
        """
        tombstone_max = int(getattr(self.deps.budget, "claim_tombstone_max", 1024))
        return max(1, tombstone_max * int(self.state.max_lease_ttl_s))

    def supervisor_note_crash(self, acb: ACB) -> None:
        """Supervisor 心跳超时回收（文档 §5.8③）。

        X1：**先 revoke claim（记录 epoch 水位）再落盘**。否则 save 走
        `claim_fenced` 时会因「claim 存在但主线程无执行上下文」被拒；而撤销
        水位保证旧 worker（线程本地 epoch 落后）此后所有写一律 `REJECT_STALE`。
        """
        self.deps.intents.revoke_all(acb.id)
        self.deps.bump_epoch()
        run_id = getattr(acb, "run_id", "") or ""
        mark = self.deps.scheduler._epoch_now(run_id or None)
        self.deps.scheduler.revoke_claim(acb.id, watermark=mark)
        # §2.4：落盘（duck-typed 探针无 budget_node 时跳过，保持向后兼容）。
        # 这次快照是**监督者**的权威写入：以撤销水位作为执行上下文 epoch，
        # 使得「本监督者」不被自己刚推进的水位 fence 掉（旧 worker 的后续写
        # 仍会因上下文 epoch 落后而被拒）。
        if hasattr(acb, "budget_node"):
            prev = self._bind_claim_epoch(acb.id, mark)
            try:
                self.deps.checkpoints.save(acb)
            finally:
                self._unbind_claim_epoch(acb.id, prev)
