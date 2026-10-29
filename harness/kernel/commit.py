"""CommitService —— RSC 提交协议（S2 从 Harness 迁出）。

提交点顺序**不可调换**（原 `commit_batch` docstring 逐字保留）：

    ① 输入校验 → ② 预留租约(L0) → ③ 全序一次性持锁(L3，锁集合含读集合)
    → ④ 校验 → ④′ 内容先落盘 → ⑤ 提交点前复验租约
    → ⑥ 提交点（一次 fdatasync）→ ⑦ 投影 → ⑧ 锁外通知

**编排方法独占 `try/finally`**：`finally` 必须覆盖 10 处提前 return 与所有异常，
并保证「释放资源锁 → revoke 租约 → 锁外 notify」的固定顺序。阶段方法只做步骤，
返回 `Result | None`（None = 继续）。

**字节级守卫**：`tests/test_s2_commit_golden.py` 锁定提交产生的事件流；
本文件任何改动都不得让该哈希变化。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from .receipts import _EXT_KEY_CONFLICT, ReceiptScanFailed
from .state import _NullLock
from ..rebase import DELETE
from ..runtime import CrashInjected
from ..spec.receipt import build_receipt, receipt_hash
from ..storage._lock import FencingRejected
from ..storage.blob import canonical_bytes
from ..storage.codec import CodecError
from ..types import ACB, EventKind, Mode, ReceiptConflict, Result, Status, Write


@dataclass(frozen=True)
class CommitDeps:
    workspace: Any
    intents: Any
    blobs: Any
    event_log: Any
    receipts: Any
    leases: Any
    # S3/S5 之前 `_claim_fenced` 与 `check_definition` 仍在 Kernel：用 callable 注入，
    # 不做 kernel 反向依赖。
    claim_guard: Callable[[ACB], bool]
    definition_check: Callable[[Any], str]


@dataclass
class _CommitCtx:
    """一次 commit 调用的全部中间量（并发调用之间互不共享）。"""

    acb: ACB
    writes: list
    mode: Mode
    idempotency_key: str | None
    contents: dict = field(default_factory=dict)
    resources: set = field(default_factory=set)
    leases: dict = field(default_factory=dict)
    held: list = field(default_factory=list)
    blob_hashes: dict = field(default_factory=dict)
    records: list = field(default_factory=list)
    dedup_events: list = field(default_factory=list)
    events: list | None = None
    issued: list = field(default_factory=list)
    scope: tuple = ("", "")


class CommitService:
    def __init__(self, deps: CommitDeps, state) -> None:
        self.deps = deps
        self.state = state
        self.compensation_journal: list[dict] = []   # 文档 §5.4（兼容视图）
        # 可替换的补偿单条执行器（崩溃注入 / 测试打桩的稳定接缝）
        self.run_one_hook = self._run_one_compensation

    # ------------------------------------------------------------------ 编排
    def commit_batch(self, acb: ACB, writes: list[Write],
                     mode: Mode = Mode.COMMIT, *,
                     idempotency_key: str | None = None) -> Result:
        ctx = _CommitCtx(acb=acb, writes=list(writes), mode=mode,
                         idempotency_key=idempotency_key)
        early = self._phase_preflight(ctx)
        if early is not None:
            return early
        try:
            early = self._phase_acquire(ctx)
            if early is not None:
                return early
            self._phase_records(ctx)
            early = self._phase_append(ctx)
            if early is not None:
                return early
            return self._phase_project(ctx)
        finally:
            self._cleanup(ctx)

    # ------------------------------------------------------- ① 前哨 + 输入校验
    def _phase_preflight(self, ctx: _CommitCtx) -> Result | None:
        """① 前哨 + 输入校验。

        **必须在 try/finally 之外调用**：本阶段不得获取锁/租约/写 blob，
        因此它的早退不需要 cleanup。若将来在此处加副作用，必须移进 try。
        """
        acb = ctx.acb
        if not ctx.writes:
            return Result(Status.OK, {"noop": True, "seq": None})
        # K-45：收据回扫失败的粘滞闩锁未解除 → 一律拒绝服务（fail-closed）。
        if self.state.receipt_scan_failed:
            return Result(Status.UPSTREAM_FAILED, {"reason": "receipt_scan_failed"})
        # S10a §2.5：提交前续租（心跳）；租约被其他写者持有时拒绝服务。
        if not self.deps.leases.ensure_writer_lease():
            return Result(Status.UPSTREAM_FAILED, {
                "reason": self.deps.leases.failure_reason(),
                "writer": self.state.writer_id,
                "lease_epoch": int(self.state.writer_epoch),
            })
        # S24-11：定义版本钉住 —— 写共享态前复验（热更新后旧 ACB 必须被拒）。
        definition_error = self.deps.definition_check(
            (acb.spec or {}).get("definition"))
        if definition_error:
            return Result(Status.DENIED, {"reason": definition_error})
        # S13d §2.5 P0-10：claim fencing —— 旧 worker 的写一律拒绝。
        if self.deps.claim_guard(acb):
            return Result(Status.REJECT_STALE, {
                "reason": "claim_fenced", "acb_id": acb.id,
                "claim_epoch": int(getattr(acb, "claim_epoch", 0) or 0),
            })
        ctx.resources = {w.resource for w in ctx.writes}
        if len(ctx.resources) != len(ctx.writes):                  # ① 同批重复 resource
            return Result(Status.INVALID_INPUT,
                          {"reason": "duplicate_resource",
                           "resources": sorted(ctx.resources)})
        ctx.contents = {w.resource: acb.staged.get(w.resource, w.content)
                        for w in ctx.writes}
        for w in ctx.writes:                                       # ① None 内容
            if ctx.contents[w.resource] is None:
                return Result(Status.INVALID_INPUT,
                              {"reason": "none_content", "resource": w.resource})
        for w in ctx.writes:                                       # ①′ 内容 fail-fast
            if ctx.contents[w.resource] is DELETE:
                continue
            try:                                                   # P1-5：非法内容不进临界区
                canonical_bytes(ctx.contents[w.resource])
            except (TypeError, ValueError) as exc:
                return Result(Status.INVALID_INPUT,
                              {"reason": "bad_content", "resource": w.resource,
                               "error": type(exc).__name__})
        return None

    # --------------------------------- ②③④④′⑤ 预留 / 持锁 / 校验 / 落盘 / 复验
    def _phase_acquire(self, ctx: _CommitCtx) -> Result | None:
        ctx.resources |= set(ctx.acb.read_set)                     # ③ 读集合一并持锁
        for w in ctx.writes:                                       # ② 预留租约（L0）
            base = self.deps.workspace._base_of(ctx.acb, w.resource)
            lease = self.deps.intents.reserve(
                w.resource, ctx.acb.id, base, self.state.lease_ms,
                run_id=ctx.acb.run_id)
            if lease is None:
                return Result(Status.DENIED, {"resource": w.resource})
            ctx.leases[w.resource] = lease

        ctx.held, ok = self.deps.workspace._acquire_all(
            ctx.resources, timeout=self.state.lock_timeout_s)
        if not ok:
            return Result(Status.UPSTREAM_FAILED,
                          {"reason": "lock_timeout", "held": ctx.held})

        for w in ctx.writes:                                       # ④ 全部校验（锁内）
            r = self.deps.workspace._validate_locked(
                ctx.acb, w, ctx.leases[w.resource], ctx.mode)
            if r != Status.VALID:
                return r

        try:                                                       # ④′ 内容先落盘
            for w in ctx.writes:
                if ctx.contents[w.resource] is DELETE:
                    ctx.blob_hashes[w.resource] = ""
                else:
                    ctx.blob_hashes[w.resource] = self.deps.blobs.put(
                        canonical_bytes(ctx.contents[w.resource]))
        except (CodecError, TypeError, ValueError) as exc:         # P1-5
            return Result(Status.INVALID_INPUT,
                          {"reason": "bad_content", "error": type(exc).__name__})

        for w in ctx.writes:                                       # ⑤ 提交点前复验租约
            if not self.deps.intents.is_valid(ctx.leases[w.resource]):
                return Result(Status.REJECT_STALE,
                              {"resource": w.resource,
                               "reason": "lease_expired_before_commit"})
        return None

    # --------------------------------------------------- ⑥ 前记录：提交事件草稿
    def _phase_records(self, ctx: _CommitCtx) -> None:
        for w in ctx.writes:
            draft, version_before, version_after = self._commit_record(
                ctx.acb, w, ctx.blob_hashes[w.resource],
                deleted=ctx.contents[w.resource] is DELETE, mode=ctx.mode,
                idempotency_key=ctx.idempotency_key)
            commit_key = self.deps.receipts.commit_key(
                ctx.acb, w.resource, ctx.blob_hashes[w.resource], ctx.mode)
            ctx.records.append(
                (w, draft, version_before, version_after, commit_key))

    # ------------------------------------------- ⑥ 去重 + 收据链 + 单帧提交点
    def _phase_append(self, ctx: _CommitCtx) -> Result | None:
        acb = ctx.acb
        ctx.scope = (acb.run_id, acb.logical_subgoal_id)
        ext_lock = (self.deps.receipts.ext_key_lock(ctx.idempotency_key)
                    if ctx.idempotency_key else _NullLock())
        # scope 临界区**覆盖**「去重检查 → 读 head → 构造 drafts → append_batch」
        # 锁序：resource → ext → scope → eventlog
        with ext_lock, self.deps.receipts.scope_lock(ctx.scope):
            try:
                self.deps.receipts.refresh_locked()
            except ReceiptScanFailed:
                return Result(Status.UPSTREAM_FAILED,
                              {"reason": "receipt_scan_failed"})
            # S22：外部幂等键 —— 同键同内容返回既有收据；同键异内容 CONFLICT
            if ctx.idempotency_key:
                ext = self.deps.receipts.dedup_by_external_key(
                    ctx.idempotency_key,
                    {(w.resource, ctx.blob_hashes[w.resource],
                      ctx.contents[w.resource] is DELETE) for w in ctx.writes})
                if ext is _EXT_KEY_CONFLICT:
                    return Result(Status.CONFLICT, {
                        "reason": "idempotency_key_reused_with_different_content",
                        "external_key": ctx.idempotency_key})
                if ext is not None:
                    hits, receipt_ids, seq = ext
                    self.deps.receipts.replay_dedup_events(hits)
                    # S22 评审 P1：重放命中也要刷新 base_version/read_set。
                    self.deps.receipts.refresh_after_commit(acb, ctx.records)
                    return Result(Status.OK, {
                        "resources": sorted(ctx.blob_hashes),
                        "seq": seq,
                        "receipt_ids": list(receipt_ids),
                        "dedup": True,
                        "external_key": ctx.idempotency_key,
                    })
            # 幂等短路（S9 §2.3.3 / P0-1②③）：逐写匹配既有提交事件
            ctx.dedup_events = [
                self.deps.receipts.dedup_event(
                    acb, w.resource, ctx.blob_hashes[w.resource],
                    ctx.contents[w.resource] is DELETE, ctx.mode)
                for w, _d, _vb, _va, _ck in ctx.records]
            is_dedup = [ev is not None for ev in ctx.dedup_events]

            if all(is_dedup):
                # 全命中：必须补投影（P0-1①），再刷新读版本，最后返回 OK
                self.deps.receipts.replay_dedup_events(ctx.dedup_events)
                self.deps.receipts.refresh_after_commit(acb, ctx.records)
                return Result(Status.OK, {
                    "resources": sorted(ctx.blob_hashes),
                    "seq": self._last_committed_seq(ctx.dedup_events),
                    "receipt_seq": self._last_receipt_seq(ctx.records),
                    "receipt_ids": [ck for *_rest, ck in ctx.records],
                    "dedup": True,
                })

            # P0-3：逐写过滤——已去重的写只补投影，不生成 commit/receipt draft
            to_append = [rec for rec, dedup in zip(ctx.records, is_dedup)
                         if not dedup]

            # P2：tainted scope 拒绝继续扩展（不得接在坏 head 上）
            if self.deps.receipts.is_tainted(ctx.scope):
                raise ReceiptConflict(
                    f"scope 已隔离（tainted），拒绝扩展：{ctx.scope}")

            head = self.deps.receipts.head_of(ctx.scope)
            secret = self.deps.receipts.receipt_secret()
            drafts: list[dict] = []
            for w, draft, version_before, version_after, commit_key in to_append:
                drafts.append(draft)
                if not self.state.receipts_enabled:
                    continue
                # P0-3：append 前断言 receipt_id 不冲突
                if (self.deps.receipts.has_receipt(commit_key)
                        and self.deps.receipts.receipt_is_stale(commit_key)):
                    commit_key = self.deps.receipts.derive_receipt_id(commit_key)
                if self.deps.receipts.has_receipt(commit_key):
                    # P2-C：冲突必须把 scope 加入 tainted（否则永久 ReceiptConflict）
                    self.deps.receipts.taint(ctx.scope)
                    raise ReceiptConflict(
                        f"receipt_id 已签发：{commit_key}（scope={ctx.scope}）")
                receipt = build_receipt(
                    receipt_id=commit_key, run_id=acb.run_id,
                    task_id=acb.task_id,
                    logical_subgoal_id=acb.logical_subgoal_id,
                    resource=w.resource,
                    version_before=version_before, version_after=version_after,
                    prev_receipt_hash=head, secret=secret)
                drafts.append({
                    "kind": EventKind.RECEIPT_ISSUED,
                    "subject": receipt.receipt_id,     # subject 不能用 resource（§2.3.6）
                    "payload": receipt.to_dict(),      # resource 放 payload
                    "producer": acb.id,
                    "run_id": acb.run_id,
                    "idempotency_key": f"receipt:{receipt.receipt_id}",
                })
                head = receipt_hash(receipt)
                ctx.issued.append(receipt.receipt_id)
            try:
                ctx.events = self.deps.event_log.append_batch(          # ⑥ 提交点
                    drafts, notify=False)
            except FencingRejected as exc:
                # S10b §3.4：旧 epoch 写者被接管 → 拒绝写入（不 append、不改 head）
                return Result(Status.REJECT_STALE, {
                    "reason": "fencing_epoch_rejected",
                    "scope": list(ctx.scope), "detail": str(exc)})
            # S12 K-33：notify=False → 主动推进 per-run 游标
            for ev in ctx.events:
                self.deps.receipts.track_run_cursor(ev)
            # ③ head 后置提交：append 成功后才改内存态（唯一写入者 = ReceiptChain）
            self.deps.receipts.record_commit(
                ctx.scope, head, ctx.issued, ctx.events)
        return None

    # ------------------------------------------------------------ ⑦ 投影 + 成功
    def _phase_project(self, ctx: _CommitCtx) -> Result:
        acb = ctx.acb
        for (w, _d, _vb, version_after, _ck), ev in zip(ctx.records,
                                                        ctx.dedup_events):
            if ev is not None:
                self.deps.workspace.apply_event(ev)                # P0-1①：补投影
                continue
            prev = self.deps.workspace._current_blob_hash(w.resource)
            self.deps.workspace._apply_locked(
                acb, w, ctx.blob_hashes[w.resource], prev,
                deleted=ctx.contents[w.resource] is DELETE,
                version_after=version_after)
        self.deps.receipts.refresh_after_commit(acb, ctx.records)  # ⑦′ 自写后重读
        commit_seqs = [e.seq for e in ctx.events
                       if e.kind == EventKind.RESOURCE_COMMITTED]
        receipt_seqs = [e.seq for e in ctx.events
                        if e.kind == EventKind.RECEIPT_ISSUED]
        return Result(Status.OK, {
            "resources": sorted(ctx.blob_hashes),
            "seq": commit_seqs[-1],          # 最后一条 RESOURCE_COMMITTED 的 seq
            "receipt_seq": receipt_seqs[-1] if receipt_seqs else None,
            "receipt_ids": list(ctx.issued),
        })

    # ------------------------------------------------------------ ⑧ 清理（必备）
    def _cleanup(self, ctx: _CommitCtx) -> None:
        self.deps.workspace._release_all(ctx.held)                 # 先释放资源锁
        for lease in ctx.leases.values():
            self.deps.intents.revoke(lease)                        # 再 revoke 租约
        if ctx.events is not None:                                 # 锁外通知
            try:
                self.deps.event_log.notify_events(ctx.events)
            except CrashInjected:                  # S9 §2.1：注入崩溃必须穿透
                raise
            except Exception:  # noqa: BLE001      # 通知失败不得掩盖提交返回值
                pass

    # ---------------------------------------------------------------- 记录构造
    def _commit_record(self, acb: ACB, w: Write, blob_hash: str,
                       deleted: bool = False, mode: Mode = Mode.COMMIT,
                       idempotency_key: str | None = None
                       ) -> tuple[dict, int, int]:
        """构造 `RESOURCE_COMMITTED` 草稿，并返回 `(draft, version_before, version_after)`。

        版本只算一次：`_apply_locked` 复用 `version_after` 并 assert 相等（S9 §2.3.9）。
        P0-1③：`version_before/version_after` 以**事件日志水位**为准。
        """
        version_before = self.deps.workspace._effective_version(w.resource)
        version = version_before + 1
        prev_hash = self.deps.workspace._current_blob_hash(w.resource)
        payload = {
            "resource": w.resource,
            "version": version,
            "mode": mode.value,                 # 幂等短路键的一部分（§2.3.3）
            "logical_subgoal_id": acb.logical_subgoal_id,   # P0-2：去重键的 scope 分量
            "acb_id": acb.id,
            "blob_hash": blob_hash,
            "prev_hash": prev_hash,
            "diff": {"prev_hash": prev_hash, "current_hash": blob_hash},
        }
        if deleted:                                  # Delete 哨兵事件（可重放）
            payload["deleted"] = True
        # S22：外部幂等键只在实际使用时写入 —— 不用时事件字节与旧版完全一致。
        if idempotency_key:
            payload["external_idem_key"] = str(idempotency_key)
        draft = {
            "kind": EventKind.RESOURCE_COMMITTED,          # ← 必须与重建过滤一致
            "subject": w.resource,
            "payload": payload,
            "producer": acb.id,
            "run_id": acb.run_id,
            "idempotency_key": f"{acb.run_id}:{acb.id}:{w.resource}",
        }
        return draft, version_before, version

    def _last_committed_seq(self, dedup_events) -> int | None:
        """幂等短路时：已存在 `RESOURCE_COMMITTED` 的最大 seq。"""
        seq = None
        for ev in dedup_events:
            if ev is not None and (seq is None or ev.seq > seq):
                seq = ev.seq
        return seq

    def _last_receipt_seq(self, records) -> int | None:
        """幂等短路时：最后一条已存在 `RECEIPT_ISSUED` 的 seq。"""
        seq = None
        for _w, _draft, _vb, _va, commit_key in records:
            ev = self.deps.receipts.receipt_event(commit_key)
            if ev is not None and (seq is None or ev.seq > seq):
                seq = ev.seq
        return seq

    # ------------------------------------------------------------------ 补偿
    def compensate(self, acb: ACB, applied: list[Write]) -> None:
        """Saga 补偿（§4.2）：批量原子计划 → 释放租约 → 逐条续做。"""
        entries = self.state.comp_journal.plan(acb, applied)
        self.deps.intents.revoke_all(acb.id)
        by_id = {e.comp_id: w for e, w in zip(entries, applied)}
        for e in reversed(entries):
            self.run_one_hook(acb, e, by_id.get(e.comp_id))

    def _run_one_compensation(self, acb: ACB, e, w: Write | None = None) -> None:
        """单条补偿（§4.2）：先按 journal 状态分支，再按 hash / 版本号判定。"""
        journal = self.compensation_journal
        if w is not None and w.compensated:
            journal.append({"resource": e.resource, "state": "SKIPPED"})
            return
        if e.state == "COMPENSATED":
            self._mark_write(w, "COMPENSATED")
            journal.append({"resource": e.resource, "state": "COMPENSATED"})
            return
        if e.state == "PIVOT":
            self._mark_write(w, "PIVOT")
            journal.append({"resource": e.resource, "state": "PIVOT"})
            return
        exists = self.deps.workspace.get(e.resource) is not None
        current = self.deps.workspace.content_hash(e.resource)
        if e.deleted:
            # 删除类：必须用版本号判定（空 hash 无法区分「不存在」与「删除后」）
            if not exists and self.deps.workspace.version(e.resource) == e.version_before + 1:
                self.state.comp_journal.mark(e.comp_id, "COMPENSATED")
                self._mark_write(w, "COMPENSATED")
                journal.append({"resource": e.resource, "state": "COMPENSATED"})
                return
            if exists and current != e.applied_hash:
                self.state.comp_journal.mark(e.comp_id, "PIVOT")
                self._mark_write(w, "PIVOT")
                journal.append({"resource": e.resource, "state": "PIVOT"})
                return
        else:
            if current == e.target_hash:
                self.state.comp_journal.mark(e.comp_id, "COMPENSATED")
                self._mark_write(w, "COMPENSATED")
                journal.append({"resource": e.resource, "state": "COMPENSATED"})
                return
            if current != e.applied_hash:
                self.state.comp_journal.mark(e.comp_id, "PIVOT")
                self._mark_write(w, "PIVOT")
                journal.append({"resource": e.resource, "state": "PIVOT"})
                return
        # 执行反写
        rw = Write(e.resource, content=None, version_before=e.version_before,
                   applied_hash=e.applied_hash)
        reverse = self.deps.workspace.compute_reverse_patch(rw)
        if reverse is None:
            self.state.comp_journal.mark(e.comp_id, "PIVOT")
            self._mark_write(w, "PIVOT")
            journal.append({"resource": e.resource, "state": "PIVOT"})
            return
        acb.staged[e.resource] = DELETE if reverse is DELETE else reverse
        st = self._compensate_one(acb, rw)
        if st == Status.OK:
            self.state.comp_journal.mark(e.comp_id, "COMPENSATED")
            self._mark_write(w, "COMPENSATED")
            journal.append({"resource": e.resource, "state": "COMPENSATED"})
        elif st == Status.REJECT_COMPENSATE:
            self.state.comp_journal.mark(e.comp_id, "PIVOT")
            self._mark_write(w, "PIVOT")
            journal.append({"resource": e.resource, "state": "PIVOT"})
        else:
            self.state.comp_journal.mark(e.comp_id, "FAILED")
            journal.append({"resource": e.resource, "state": "FAILED"})

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
