"""ReceiptChain —— 收据链、幂等去重、scope 锁、per-run 游标（S1 从 Harness 迁出）。

迁移方式：**纯代码位移 + 机械重命名**，判定逻辑与执行顺序逐行不变。

依赖：
- 组件  `deps.event_log` / `deps.workspace`
- 配置  `deps.epoch_scope` / `deps.receipts_enabled` / `deps.retention`
- 状态  `state.*`（收据 head/id/tainted、scope 锁、外部键锁、游标、粘滞闩锁）
"""

from __future__ import annotations

import hashlib
import os
import threading
from dataclasses import dataclass

from .state import _NullLock
from ..spec.receipt import (
    GENESIS_HASH,
    Receipt,
    ReceiptMalformed,
    build_receipt,
    receipt_hash,
    verify_chain_detailed,
)
from ..types import ACB, EventKind, Mode, ReceiptConflict, Result, Status, Write


@dataclass(frozen=True)
class ReceiptDeps:
    event_log: object
    workspace: object
    epoch_scope: str
    receipts_enabled: bool
    retention: int


_EXT_KEY_CONFLICT = object()


class ReceiptScanFailed(RuntimeError):
    """收据回扫失败（K-45）：调用方据此拒绝服务（fail-closed，不 append）。"""


class ReceiptChain:
    def __init__(self, deps, state) -> None:
        self.deps = deps
        self.state = state

    def _rebuild_receipts(self) -> None:
        """无条件从 EventLog 重建收据 head / id（不读 checkpoint 内存值）。

        P2：按 scope 维护 running head，**校验链连续性** —— `prev_receipt_hash`
        必须等于 running head 且 `receipt_id` 不重复；失败 → 该 scope 标
        **tainted** 并拒绝继续扩展。否则已存在的重复/乱序收据会被静默折叠，
        新收据继续接在坏 head 上 → `verify_receipts` 永远 False。
        P2：畸形 payload（缺 `receipt_id`/`run_id`/`task_id`）→ 记入
        `_malformed_receipts`，其 scope 标 tainted（**不得抛** → 单条坏事件
        不得打挂 Harness 启动）。
        """
        heads: dict[tuple[str, str], str] = {}
        ids: set[str] = set()
        tainted: set[tuple[str, str]] = set()
        running: dict[tuple[str, str], str] = {}
        malformed: list[dict] = []
        for ev in self.deps.event_log.all():
            if ev.kind != EventKind.RECEIPT_ISSUED:
                continue
            payload = ev.payload or {}
            try:
                receipt = Receipt.from_dict(payload)
            except ReceiptMalformed as exc:
                malformed.append({"seq": ev.seq, "error": str(exc),
                                  "payload": payload})
                scope = self._payload_scope(payload)
                if scope is not None:
                    tainted.add(scope)
                continue
            scope = (receipt.run_id, receipt.logical_subgoal_id)
            if scope in tainted:
                continue
            if receipt.receipt_id in ids:            # 重复 receipt_id → 断链
                tainted.add(scope)
                continue
            head = running.get(scope, GENESIS_HASH)
            if receipt.prev_receipt_hash != head:    # 乱序/断链
                tainted.add(scope)
                continue
            ids.add(receipt.receipt_id)
            running[scope] = receipt_hash(receipt)
            heads[scope] = running[scope]
        self.state.receipt_heads = heads
        self.state.receipt_ids = ids
        self.state.tainted_scopes = tainted
        self.state.malformed_receipts = malformed
        # K-45：**成功的全量重建**是粘滞闩锁的唯一解除条件（水位推到 tail）。
        self.state.receipt_scan_failed = False
        # 首次构建完成 → 水位推到当前尾：之后锁内刷新只扫「新增」收据，
        # 不重复扫旧事件（否则每次 commit 全量扫 EventLog → O(N²)）。
        try:
            self.state.receipt_scan_watermark = self.deps.event_log.tail()
        except Exception:  # noqa: BLE001  # tail 不可用 → 0（刷新退回全扫，仍正确）
            self.state.receipt_scan_watermark = 0
    @staticmethod
    def _scan_receipts(records, heads: dict, ids: set, tainted: set,
                       malformed: list) -> int:
        """按 seq 顺序应用收据记录，维护 running head + 连续性校验。

        `_rebuild_receipts`（首次构建）与 `refresh_locked`（锁内增量
        刷新）**共用**此逻辑：`prev_receipt_hash` 必须等于 running head 且
        `receipt_id` 不重复；失败 → 该 scope tainted 并拒绝继续扩展；畸形
        payload → 记入 `malformed`（**不得抛**）。返回处理到的最大 seq
        （推进水位用）。

        注意 `running`/`heads` 合一（原实现两者恒等）：每 scope 的 head 就是
        该 scope 最后一条被接受的收据的链接值。
        """
        max_seq = -1
        for seq, payload in records:
            max_seq = seq
            try:
                receipt = Receipt.from_dict(payload)
            except ReceiptMalformed as exc:
                malformed.append({"seq": seq, "error": str(exc),
                                  "payload": payload})
                scope = ReceiptChain._payload_scope(payload)
                if scope is not None:
                    tainted.add(scope)
                continue
            scope = (receipt.run_id, receipt.logical_subgoal_id)
            if scope in tainted:
                continue
            if receipt.receipt_id in ids:            # 重复 receipt_id → 断链
                tainted.add(scope)
                continue
            head = heads.get(scope, GENESIS_HASH)
            if receipt.prev_receipt_hash != head:    # 乱序/断链
                tainted.add(scope)
                continue
            ids.add(receipt.receipt_id)
            heads[scope] = receipt_hash(receipt)
        return max_seq
    def _receipt_records(self, watermark: int = 0) -> list[tuple[int, dict]]:
        """返回 `seq > watermark` 的 `RECEIPT_ISSUED` 记录 `[(seq, payload)]`。

        **K-44**：WAL 实现走 `wal.receipts_from(watermark)` —— 在 `wal.lock` 内
        O(1) fstat 比对 + **只解析新增帧**（增量索引），不再每次 `wal.records()`
        全量枚举（那会是 O(N²)）。内存 `EventLog` 直接读 `all()`。
        """
        log = self.deps.event_log
        wal = getattr(log, "_wal", None)
        if wal is None:
            recs = [(ev.seq, ev.payload) for ev in log.all()
                    if ev.kind == EventKind.RECEIPT_ISSUED]
        else:
            receipts_from = getattr(wal, "receipts_from", None)
            if callable(receipts_from):
                recs = list(receipts_from(watermark))
            else:                                    # 兼容旧 WAL 实现
                lock = getattr(wal, "_lock", None)
                if lock is not None:                 # 锁序 receipts → wal
                    with lock():
                        self._rescan_wal_from_disk(wal)
                        raw = wal.records()
                else:
                    self._rescan_wal_from_disk(wal)
                    raw = wal.records()
                recs = [(r.get("seq", 0), r.get("payload"))
                        for r in raw
                        if r.get("kind") == EventKind.RECEIPT_ISSUED.value]
        recs.sort(key=lambda rp: rp[0])
        return recs
    @staticmethod
    def _rescan_wal_from_disk(wal) -> None:
        """flock 内 O(1) fstat 比对：仅当别的进程写过才回扫（复用 WAL 既有优化）。

        K-45：回扫失败**不得静默**（与 `_receipt_records` / `Wal.receipts_from`
        同口径）—— 异常向上抛，由 `refresh_locked` 置粘滞闩锁并
        抛 `ReceiptScanFailed` → 提交 fail-closed。
        """
        fd = getattr(wal, "_fd", None)
        cached = getattr(wal, "_cached_end", None)
        if (fd is not None and cached is not None
                and os.fstat(fd).st_size == cached):
            return
        rescan = getattr(wal, "_rescan_from_disk", None)
        if callable(rescan):
            rescan()
    def refresh_locked(self) -> None:
        """scope 临界区内：从 EventLog **增量**刷新 head/ids（跨进程安全）。

        根因（K-24 后续）：`_receipt_heads`/`_receipt_ids` 只在 `__init__`/
        `recover` 重建一次；跨进程下另一进程可能在我们构建之后才 append 收据
        → 锁内读到**过期 head** → 链分叉。故在 `commit_batch` 的 scope 临界区
        内、读 head 前调用本方法：取锁 → 刷新 → 读 head → 构造 drafts →
        append_batch → 提交内存态。

        增量：只处理 `seq > _receipt_scan_watermark` 的 RECEIPT_ISSUED（水位
        在首次构建时推到当前尾），避免每次 commit 全量扫。

        K-45（fail-closed）：回扫失败**不再静默回退** —— 置粘滞闩锁
        `_receipt_scan_failed` 并抛 `ReceiptScanFailed`（专用异常）。
        不 append、不改 head、不加 taint（链未污染，无需隔离）。
        """
        if not self.deps.receipts_enabled:
            return
        with self.state.receipt_refresh_lock:
            watermark = self.state.receipt_scan_watermark
            try:
                records = self._receipt_records(watermark)
            except Exception as exc:  # K-45：回扫失败 → 拒绝服务
                self.state.receipt_scan_failed = True
                raise ReceiptScanFailed(str(exc)) from exc
            new_records = [rp for rp in records if rp[0] > watermark]
            if not new_records:
                return
            heads = dict(self.state.receipt_heads)
            ids = set(self.state.receipt_ids)
            tainted = set(self.state.tainted_scopes)
            malformed = list(self.state.malformed_receipts)
            max_seq = self._scan_receipts(new_records, heads, ids, tainted,
                                          malformed)
            self.state.receipt_heads = heads
            self.state.receipt_ids = ids
            self.state.tainted_scopes = tainted
            self.state.malformed_receipts = malformed
            if max_seq > watermark:
                self.state.receipt_scan_watermark = max_seq
    @staticmethod
    def _payload_scope(payload: dict) -> tuple[str, str] | None:
        """从（可能畸形的）payload 尽力推断 scope（用于 tainted 标记）。"""
        if not isinstance(payload, dict):
            return None
        run_id = payload.get("run_id")
        if not isinstance(run_id, str) or not run_id:
            return None
        return (run_id, payload.get("logical_subgoal_id") or "")
    def scope_lock(self, scope: tuple[str, str]):
        """取（必要时创建）scope 锁；只护锁表，极短，不覆盖临界区。

        **durable 模式**：返回跨进程 `receipts.lock`（flock，全序第 1 位），
        跨进程互斥 → 同 scope 并发提交串行化（链不分叉）。
        **内存模式**：退回每 scope 一把进程内 `RLock`（跨进程不互斥，§K-34）。
        """
        if self.state.receipts_flock is not None:
            return self.state.receipts_flock()
        with self.state.lock:
            lock = self.state.scope_locks.get(scope)
            if lock is None:
                lock = threading.RLock()
                self.state.scope_locks[scope] = lock
            return lock
    def _sync_run_cursors(self) -> None:
        """从既有事件初始化 per-run 游标（构造 / 恢复后调用一次）。"""
        for ev in self.deps.event_log.all():
            self.track_run_cursor(ev)
    def track_run_cursor(self, ev) -> None:
        """事件到达 → 推进其 run 的高水位（未打 run 标签的事件记入 `""`）。"""
        run_id = getattr(ev, "run_id", "") or ""
        seq = int(getattr(ev, "seq", 0) or 0)
        if seq > self.state.run_cursors.get(run_id, 0):
            self.state.run_cursors[run_id] = seq
    def _run_tail(self, run_id: str) -> int:
        """该 run 的**per-run** tail（含未打标签事件 `""`，向后兼容旧日志）。"""
        return max(int(self.state.run_cursors.get(run_id, 0) or 0),
                   int(self.state.run_cursors.get("", 0) or 0))
    def floor_for(self, run_id: str) -> int:
        """该 run 的保留窗口下界（`retention <= 0` → 不限制）。

        S12 K-33：`epoch_scope="run"` → 用 **per-run tail**（否则 run B 的活动会把
        run A 的保留窗口一起推进，A 的历史事件被截断后**找不回**）；
        `epoch_scope="global"` → **保持现状**（全局 tail，旧语义）。
        """
        if self.deps.retention <= 0:
            return 0
        if self.deps.epoch_scope != "run":
            return max(0, self.deps.event_log.tail() - self.deps.retention + 1)
        return max(0, self._run_tail(run_id) - self.deps.retention + 1)
    def receipt_secret(self) -> str:
        """收据签名密钥来源：构造参数优先，否则读 env（开关独立于 secret 是否存在）。"""
        if self.state.auth_secret:
            return self.state.auth_secret
        return os.environ.get("HARNESS_AUTH_SECRET", "")
    @staticmethod
    def commit_key(acb: ACB, resource: str, content_hash: str, mode: Mode) -> str:
        """同一逻辑提交恒等键（P0-2）：**不含 base_version**。

        `base_version` 是「读到哪一版」的**状态**而非写的内容；刷新 ACB
        （提交后重读）后它会变，若入键则同一逻辑写的重试去重失效，
        导致重复提交 + 重复收据。
        """
        raw = (f"{acb.run_id}|{acb.logical_subgoal_id}|{resource}|"
               f"{content_hash}|{mode.value}")
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]
    def dedup_event(self, acb: ACB, resource: str, blob_hash: str,
                     deleted: bool, mode: Mode):
        """查找既有 `RESOURCE_COMMITTED`：同 (run, subgoal, resource, content, mode)。

        P0-1②：必须校验已提交 `blob_hash` == 新内容 hash（含 DELETE 哨兵），
        内容不同 → 返回 `None`（视为不同写，绝不允许短路）。

        P1-B：dedup 只能命中该资源的**最新**提交 —— 命中事件的 `version` 必须等于
        `workspace._effective_version(resource)`。否则说明该内容已被后续写覆盖，
        命中它会把新写**静默丢弃**（返回 OK + dedup=True，但 version 与值都不变）。
        反例：`r=A(v1) → r=A(v2) → r=B(v3) → 再写 A` 必须产生 v4=A，而不是命中 v2。
        """
        effective = self.deps.workspace._effective_version(resource)
        for ev in reversed(self.deps.event_log.all()):
            if ev.kind != EventKind.RESOURCE_COMMITTED:
                continue
            payload = ev.payload or {}
            if payload.get("resource") != resource:
                continue
            if payload.get("mode", Mode.COMMIT.value) != mode.value:
                continue
            if ev.run_id != acb.run_id:
                continue
            if payload.get("logical_subgoal_id", "") != acb.logical_subgoal_id:
                continue
            if bool(payload.get("deleted", False)) != deleted:
                continue
            if payload.get("blob_hash", "") != blob_hash:
                continue                      # 内容不同 → 不得短路（P0-1②）
            try:
                matched_version = int(payload.get("version", 0) or 0)
            except (TypeError, ValueError):
                return None                   # 版本不可解析 → 视为新写（不猜）
            if matched_version != effective:
                return None                   # 命中的是陈旧版本 → 视为新写（P1-B）
            return ev
        return None
    def ext_key_lock(self, key: str):
        """S22：外部幂等键的进程内串行锁（只护锁表，不覆盖临界区）。"""
        with self.state.lock:
            lock = self.state.ext_key_locks.get(key)
            if lock is None:
                lock = threading.RLock()
                self.state.ext_key_locks[key] = lock
            return lock
    def dedup_by_external_key(self, key: str, expected: set):
        """S22：按**外部业务键**查既有提交（事件即真相，不另建存储）。

        `expected = {(resource, blob_hash, deleted)}`。
        返回 `(hits, receipt_ids, seq)` / `_EXT_KEY_CONFLICT` / `None`。

        收据 id 从既有 `RECEIPT_ISSUED` 事件读（**不能**用当前 ACB 重算：
        重启后 token 恢复出的 ACB 可能换了 run/subgoal，重算会得到不同 id）。
        """
        hits = []
        for ev in self.deps.event_log.all():
            if ev.kind is not EventKind.RESOURCE_COMMITTED:
                continue
            if (ev.payload or {}).get("external_idem_key") != key:
                continue
            hits.append(ev)
        if not hits:
            return None
        actual = {(e.payload.get("resource"), e.payload.get("blob_hash"),
                   bool(e.payload.get("deleted", False))) for e in hits}
        if actual != set(expected):
            return _EXT_KEY_CONFLICT
        scopes = {(e.run_id, (e.payload or {}).get("logical_subgoal_id", ""))
                  for e in hits}
        resources = {r for r, _h, _d in expected}
        receipt_ids = []
        for ev in self.deps.event_log.all():
            if ev.kind is not EventKind.RECEIPT_ISSUED:
                continue
            p = ev.payload or {}
            if ((ev.run_id, p.get("logical_subgoal_id", "")) in scopes
                    and p.get("resource") in resources):
                rid = p.get("receipt_id")
                if rid:
                    receipt_ids.append(rid)
        seq = max(int(getattr(e, "seq", 0) or 0) for e in hits)
        return tuple(hits), tuple(receipt_ids), seq
    def replay_dedup_events(self, dedup_events) -> None:
        """P0-1①：把已提交但未投影的 `RESOURCE_COMMITTED` 补投影到 manifest。

        用版本守卫投影（`_project_event`），既补上缺失投影，又不会把 manifest
        回退到更旧的版本。
        """
        for ev in dedup_events:
            if ev is not None:
                self.deps.workspace.apply_event(ev)
    def refresh_after_commit(self, acb: ACB, records) -> None:
        """⑦′：自写后重读，刷新 ACB 的 read_set / base_version。"""
        for w, _draft, _vb, _va, _ck in records:
            new_version = self.deps.workspace.version(w.resource)
            acb.read_set[w.resource] = new_version
            acb.base_version[w.resource] = new_version
    def receipt_event(self, receipt_id: str):
        for ev in reversed(self.deps.event_log.all()):
            if ev.kind == EventKind.RECEIPT_ISSUED and ev.subject == receipt_id:
                return ev
        return None
    def receipt_is_stale(self, receipt_id: str) -> bool:
        """该 `receipt_id` 对应的收据是否为「陈旧内容」收据（P1-B 后续）。

        若资源当前版本已推进到比该收据 `version_after` 更新的位置，说明该内容
        已被后续写覆盖 → 本次是**合法新写**（dedup 正确地未命中陈旧版本），
        应换 receipt_id 而不是永久冲突。
        """
        ev = self.receipt_event(receipt_id)
        if ev is None:
            return False
        try:
            receipt = Receipt.from_dict(ev.payload or {})
        except ReceiptMalformed:
            return False
        return self.deps.workspace.version(receipt.resource) != receipt.version_after
    def derive_receipt_id(self, commit_key: str) -> str:
        """为合法重写派生一个不冲突的 `receipt_id`（换 receipt_id，P2-C）。"""
        n = 1
        candidate = f"{commit_key}#{n}"
        while candidate in self.state.receipt_ids:
            n += 1
            candidate = f"{commit_key}#{n}"
        return candidate
    def receipts_of(self, run_id: str, logical_subgoal_id: str | None = None
                    ) -> list[Receipt]:
        """按 seq 顺序返回某 run（可选 scope）的全部收据。

        P2：畸形事件（缺必填字段）**跳过并记录**（记入 `_malformed_receipts`），
        不再抛 `KeyError` —— 单条坏事件不得让验签/启动打挂。
        """
        out: list[Receipt] = []
        for ev in self.deps.event_log.all():
            if ev.kind != EventKind.RECEIPT_ISSUED:
                continue
            payload = ev.payload or {}
            if payload.get("run_id") != run_id:
                continue
            if (logical_subgoal_id is not None
                    and payload.get("logical_subgoal_id") != logical_subgoal_id):
                continue
            try:
                out.append(Receipt.from_dict(payload))
            except ReceiptMalformed:
                continue
        return out
    def verify_receipts(self, run_id: str,
                        logical_subgoal_id: str | None = None) -> bool | None:
        """离线验证该 run 的收据链（三态：`True` / `False` / `None`）。

        链按 `(run_id, logical_subgoal_id)` 独立成链；未指定 scope 时逐链验证后
        合并：任一 `False` → `False`；全部 `True` → `True`；否则 `None`。

        P2：tainted scope（畸形/重复/断链）→ `False`（拒绝继续扩展坏链）。
        """
        receipts = self.receipts_of(run_id, logical_subgoal_id)
        secret = self.receipt_secret()
        if logical_subgoal_id is not None:
            if (run_id, logical_subgoal_id) in self.state.tainted_scopes:
                return False
            return verify_chain_detailed(receipts, secret)
        groups: dict[str, list[Receipt]] = {}
        for receipt in receipts:
            groups.setdefault(receipt.logical_subgoal_id, []).append(receipt)
        results = []
        for subgoal, chain in groups.items():
            if (run_id, subgoal) in self.state.tainted_scopes:
                results.append(False)
            else:
                results.append(verify_chain_detailed(chain, secret))
        if not results or any(r is False for r in results):
            return False
        if all(r is True for r in results):
            return True
        return None
    def _commit_event_for_receipt(self, receipt, run_id: str,
                                  logical_subgoal_id: str):
        """判断某收据是否有对应的 `RESOURCE_COMMITTED` 事件（对账用）。

        匹配键：`run_id` + `logical_subgoal_id` + `resource` + `version_after`
        （版本在资源内唯一，故可定位到唯一提交事件）。
        """
        for ev in self.deps.event_log.all():
            if ev.kind != EventKind.RESOURCE_COMMITTED:
                continue
            payload = ev.payload or {}
            if ev.run_id != run_id:
                continue
            if payload.get("logical_subgoal_id", "") != logical_subgoal_id:
                continue
            if payload.get("resource") != receipt.resource:
                continue
            try:
                version = int(payload.get("version", 0) or 0)
            except (TypeError, ValueError):
                continue
            if version == receipt.version_after:
                return ev
        return None
    def reconcile_scope(self, run_id: str, logical_subgoal_id: str) -> Result:
        """P2-C 显式对账入口：解除 `ReceiptConflict` 造成的 scope 隔离。

        `ReceiptConflict` 的根因是「收据已签发（`receipt_id` 在 `_receipt_ids`），
        但对应的 `RESOURCE_COMMITTED` 事件缺失」→ `dedup_event` 失效 → 同逻辑写
        **永久**冲突。本入口执行**补写丢失的 RESOURCE_COMMITTED**：

        ① 对该 scope 的每条收据，若缺少对应的提交事件，且 workspace 的当前版本
           恰好等于 `version_after`（内容未被后续写覆盖）→ 用 workspace 当前内容
           补写一条提交事件（`prev_hash` 不可重建时留空，不影响去重/投影）；
        ② 补写后重跑 `_rebuild_receipts`；若 scope 不再 tainted → 解除隔离；
        ③ 无法补写（资源已被覆盖 / 内容不可重建）→ 保持隔离并返回
           `NEEDS_RECONCILIATION`（需人工：换 receipt_id 或修复事件日志）。
        """
        scope = (run_id, logical_subgoal_id)
        receipts = self.receipts_of(run_id, logical_subgoal_id)
        missing = [r for r in receipts
                   if self._commit_event_for_receipt(r, run_id,
                                                     logical_subgoal_id) is None]
        backfilled = 0
        for r in missing:
            current = self.deps.workspace.version(r.resource)
            blob_hash = self.deps.workspace.content_hash(r.resource)
            if current != r.version_after or not blob_hash:
                # 无法补写 → **保持隔离**（不得因重建而丢失 taint）
                self.state.tainted_scopes.add(scope)
                return Result(Status.NEEDS_RECONCILIATION, {
                    "scope": list(scope), "reason": "resource_overwritten",
                    "resource": r.resource, "expected_version": r.version_after,
                    "actual_version": current, "blob_hash": blob_hash,
                    "hint": "需人工：换 receipt_id 或修复事件日志",
                })
            payload = {
                "resource": r.resource, "version": r.version_after,
                "mode": Mode.COMMIT.value,
                "logical_subgoal_id": logical_subgoal_id,
                "acb_id": "", "blob_hash": blob_hash, "prev_hash": "",
                "diff": {"prev_hash": "", "current_hash": blob_hash},
            }
            ev = self.deps.event_log.append(
                EventKind.RESOURCE_COMMITTED, r.resource, payload,
                producer="", run_id=run_id,
                idempotency_key=f"reconcile:{run_id}:{logical_subgoal_id}:"
                                f"{r.receipt_id}")
            self.deps.workspace.apply_event(ev)
            backfilled += 1
        self._rebuild_receipts()
        if scope in self.state.tainted_scopes:
            return Result(Status.NEEDS_RECONCILIATION, {
                "scope": list(scope), "reason": "still_tainted",
                "hint": "需人工：换 receipt_id 或修复事件日志",
            })
        return Result(Status.OK, {"scope": list(scope), "reconciled": True,
                                  "backfilled": backfilled})
