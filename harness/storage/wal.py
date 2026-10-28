"""Append-only WAL（方案 §4.3）。

帧格式：`[magic:4s][len:u32][crc32:u32][payload: json]`
- payload 是**一个记录列表**：多资源提交写在**同一帧**里 → 一帧要么全生效要么全不生效。
- `append_frame` 返回前 `fdatasync` → **返回成功 == 已落盘**（提交点）。
- 恢复：逐帧校验 magic/len/crc/seq 连续性；只允许**最后一段的尾部**截断，
  中段坏帧抛 `WalCorrupt`（对应 `COMMIT_UNRECOVERABLE`，不静默丢数据）。
- 阶段一**不轮转**（单段）：删段需要订阅者 cursor 持久化，属 MS6。

S10b §3.3：**跨进程 seq 不冲突**
- `append_frame` 在 `wal.lock`（跨进程 `flock`）内完成「读-分配-写-fdatasync」全程；
- 用 `os.fstat(fd).st_size` 与 `self._cached_end` **O(1) 比对**：不一致（别的
  进程/实例写过）→ `_rescan_from_disk()` 回扫重建 `_seq`（极少发生，非每次全量）；
- 不再「每次全量扫描」（O(N²)），也不「只信内存 `_seq`」（跨进程仍冲突）。

S10b §3.4：**fencing epoch**
- 每条 record 记录 run 级 `fencing_epoch`；
- append 前在**同一 flock 内**复验 epoch，`writer < durable` → 拒绝。

S11b §3.1/§3.2：
- **K-44 增量收据索引**：`_receipt_index` / `_receipt_index_seq` / `_indexed_frame`，
  `receipts_from(watermark)` 在 `wal.lock` 内**只解析新增帧**（O(新增)，非 O(N)）；
  `_rescan_from_disk` 重建 `_frames` 时同步重置索引游标（否则错位）。
- **K-46 fencing 绑 WAL**：`wal.log.epoch`（与 WAL 同目录）+ 独立 `epoch` 锁；
  `read_epoch` 无锁原子读，`bump_epoch` 只取 epoch 锁（不与其它锁嵌套）。
"""

from __future__ import annotations

import bisect
import json
import os
import struct
import uuid
import zlib
from pathlib import Path
from typing import Self

from ..types import EventKind
from ._lock import (
    EPOCH_ORDER,
    WAL_ORDER,
    FencingRejected,
    _FileLock,
    wal_epoch_lock_path,
    wal_epoch_path,
    wal_generation_path,
    wal_lock_path,
)
from .atomic import atomic_write, fsync_dir, write_all

MAGIC = b"MWAL"
HEADER = struct.Struct(">4sII")


class WalCorrupt(Exception):
    """中段损坏：不可恢复（不可静默截断，否则会丢后续有效事件）。"""


class Wal:
    def __init__(self, path: str | Path, fsync: bool = True, lock=None,
                 entropy=None) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fsync = fsync
        # S24-10：确定性 generation（默认 None → uuid4，字节不变）
        self._entropy = entropy
        # wal.lock：三锁全序的第 2 位（resource → receipts → wal → store → eventlog）
        self._lock = (lock if lock is not None
                      else _FileLock(wal_lock_path(self.path), order=WAL_ORDER))
        self.truncated_at: int | None = None
        self._frames: list[list[dict]] = []
        self._seq = 0
        # K-44：增量收据索引（只解析新增帧；`_indexed_frame` = 已索引帧数）
        self._receipt_index: list[tuple[int, dict]] = []
        # 终审 6-2：与 `_receipt_index` 平行的 seq 键，供 `bisect` 从水位切片
        self._receipt_index_keys: list[int] = []
        self._receipt_index_seq = 0
        self._indexed_frame = 0
        # 缓存尾偏移：append 后立即记录；跨进程写入后 fstat 比对可 O(1) 发现
        self._cached_end: int = 0
        self.generation_path = Path(wal_generation_path(self.path))
        # 终审 6-1：`_load` 必须在 `wal.lock` 内 —— 否则另一写者刚写 frame header
        # （body 未写完）时本实例会看到 `body_end > len` 并 truncate 在途帧。
        with self._lock():
            existed = self.path.exists()
            self._load()
            self._generation = self._ensure_generation(existed)
            self._fd = os.open(self.path,
                               os.O_CREAT | os.O_RDWR | os.O_APPEND, 0o644)
            fsync_dir(self.path)
        # K-46：WAL 级 fencing epoch（`wal.log.epoch`），独立 epoch 锁
        self.epoch_path = Path(wal_epoch_path(self.path))
        self.epoch_lock = _FileLock(wal_epoch_lock_path(self.path),
                                    order=EPOCH_ORDER)

    # ---- generation（终审 7-3） -----------------------------------
    def _ensure_generation(self, existed: bool) -> str:
        """读取/创建 WAL generation（调用方须持 `wal.lock`）。

        - WAL 文件不存在（被删除/重建）→ **轮换**新 UUID（旧写者一律拒绝）；
        - generation 文件缺失（首次使用/升级）→ 新建 UUID；
        - WAL 与 generation 都在 → 保持不变（正常重开/并发实例共享）。
        """
        try:
            gen = self.generation_path.read_text(encoding="utf-8").strip()
        except OSError:
            gen = ""
        if not existed or not gen:
            gen = (self._entropy.uuid("wal_generation")
                   if self._entropy is not None else uuid.uuid4().hex)
            atomic_write(self.generation_path, gen.encode("utf-8"))
        return gen

    @property
    def generation(self) -> str:
        return self._generation

    def _current_generation(self) -> str:
        """读盘上的当前 generation（调用方须持 `wal.lock`；缺失 → 空串）。"""
        try:
            return self.generation_path.read_text(encoding="utf-8").strip()
        except OSError:
            return ""

    # ---- 读 -------------------------------------------------------
    def _load(self) -> None:
        if not self.path.exists():
            self._cached_end = 0
            return
        raw = self.path.read_bytes()
        offset = 0
        prev_seq = 0
        while offset < len(raw):
            remain = len(raw) - offset
            if remain < HEADER.size:
                self.truncated_at = offset
                break
            magic, length, crc = HEADER.unpack_from(raw, offset)
            if magic != MAGIC:
                raise WalCorrupt(f"magic 不匹配 @ {offset}")
            body_start = offset + HEADER.size
            body_end = body_start + length
            if body_end > len(raw):
                self.truncated_at = offset
                break
            body = raw[body_start:body_end]
            if zlib.crc32(body) & 0xFFFFFFFF != crc:
                if body_end == len(raw):
                    self.truncated_at = offset
                    break
                raise WalCorrupt(f"CRC 校验失败（中段）@ {offset}")
            records = json.loads(body.decode("utf-8"))
            for rec in records:
                seq = rec.get("seq")
                if not isinstance(seq, int) or seq != prev_seq + 1:
                    raise WalCorrupt(f"seq 不连续 @ {offset}: {seq} != {prev_seq + 1}")
                prev_seq = seq
            self._frames.append(records)
            offset = body_end
        self._seq = prev_seq
        if self.truncated_at is not None:
            os.truncate(self.path, self.truncated_at)
            self._cached_end = self.truncated_at
        else:
            self._cached_end = len(raw)

    def _rescan_from_disk(self) -> None:
        """flock 内回扫：别的进程/实例写过 → 重建 `_frames` / `_seq` / `_cached_end`。

        只在 `fstat` 大小与缓存尾偏移不一致时调用（极少发生），不是每次 append。
        K-44：`_frames` 重建 → **同步重置**索引游标（否则旧游标指向新列表错位）。
        """
        self.truncated_at = None
        self._frames = []
        self._receipt_index = []
        self._receipt_index_keys = []
        self._receipt_index_seq = 0
        self._indexed_frame = 0
        self._load()

    # ---- K-44：增量收据索引 ---------------------------------------
    def _index_new_frames_locked(self) -> None:
        """只解析 `_indexed_frame` 之后的新增帧（调用方须持 `wal.lock`）。"""
        while self._indexed_frame < len(self._frames):
            frame = self._frames[self._indexed_frame]
            for rec in frame:
                if rec.get("kind") != EventKind.RECEIPT_ISSUED.value:
                    continue
                seq = rec.get("seq")
                if isinstance(seq, int):
                    self._receipt_index.append((seq, rec.get("payload")))
                    self._receipt_index_keys.append(seq)
                    self._receipt_index_seq = max(self._receipt_index_seq, seq)
            self._indexed_frame += 1

    def receipts_from(self, watermark: int = 0) -> list[tuple[int, dict]]:
        """返回 `seq > watermark` 的收据记录 `[(seq, payload)]`（增量，不重扫全量）。

        **RAW API**：只返回索引里的原始记录，**不做链连续性校验**（`prev_receipt_hash`
        / 重复 id / 畸形 payload 的判定由 `Harness._scan_receipts` 负责）。这样索引
        层保持 O(log N + K)，校验逻辑只有一份（避免两处实现漂移）。

        - 索引读写与 fstat 比对都在 `wal.lock` 内（跨进程安全）；
        - 只解析**新增帧**；`_rescan_from_disk` 后索引游标已重置 → 从头重建；
        - 终审 6-2：用 `bisect` 从水位位置切片（O(log N + K)），不再每次全量
          过滤整个索引（O(N) → 累计 O(N²)）。
        """
        with self._lock():
            size = os.fstat(self._fd).st_size
            if size != self._cached_end:
                self._rescan_from_disk()
            self._index_new_frames_locked()
            pos = bisect.bisect_right(self._receipt_index_keys, watermark)
            return list(self._receipt_index[pos:])

    # ---- K-46：WAL 级 fencing epoch -------------------------------
    def read_epoch(self) -> int:
        """无锁原子读 `wal.log.epoch`（`atomic_write` 保证不撕裂；缺失 → 0）。"""
        try:
            raw = self.epoch_path.read_text(encoding="utf-8").strip()
        except OSError:
            return 0
        try:
            return int(raw or 0)
        except ValueError:
            return 0

    def bump_epoch(self) -> int:
        """同一 `epoch` 锁临界区内 read-modify-write（只取该锁，不与其它锁嵌套）。"""
        with self.epoch_lock():
            epoch = self.read_epoch() + 1
            atomic_write(self.epoch_path, str(epoch).encode("utf-8"))
            return epoch

    # ---- 写 -------------------------------------------------------
    def append_frame(self, records: list[dict], *, fencing_epoch: int | None = None,
                     fencing_lock=None, fencing_reader=None) -> list[dict]:
        """给每个记录分配 seq，整帧一次写入并 fdatasync（提交点）。

        `fencing_lock` / `fencing_reader` 提供时启用 fencing：在**同一 wal.lock
        临界区**内按 wal(3) → epoch(4) 正序取独立 epoch 锁，读 `wal.log.epoch`
        复验，`fencing_epoch < durable_epoch` → 抛 `FencingRejected`（不写入、不 ack）。
        """
        if not records:
            return []
        with self._lock():
            # 终审 7-3：WAL 被删除/重建 → generation 轮换；旧写者一律拒绝。
            # 若只比较 epoch，重建后 `.epoch` 归零会让旧写者（本地 epoch=N>0）
            # 的 `N < 0` 为假 → 复活写入。
            current_gen = self._current_generation()
            if current_gen != self._generation:
                raise FencingRejected(
                    f"WAL generation 已轮换（本地 {self._generation!r} ≠ "
                    f"磁盘 {current_gen!r}）→ 旧写者拒绝写入")
            if fencing_lock is not None and fencing_reader is not None:
                with fencing_lock():
                    store_epoch = int(fencing_reader() or 0)
                    if fencing_epoch is not None and fencing_epoch < store_epoch:
                        raise FencingRejected(
                            f"fencing epoch {fencing_epoch} < store epoch {store_epoch}")
            size = os.fstat(self._fd).st_size
            if size != self._cached_end:            # O(1) 比对：跨进程写入 → 回扫
                self._rescan_from_disk()
            base = self._seq
            assigned = []
            for i, rec in enumerate(records, start=1):
                item = dict(rec)
                item["seq"] = base + i
                item["wal_generation"] = self._generation
                if fencing_epoch is not None:
                    item["fencing_epoch"] = fencing_epoch
                assigned.append(item)
            body = json.dumps(assigned, sort_keys=True, separators=(",", ":"),
                              ensure_ascii=False, default=str).encode("utf-8")
            header = HEADER.pack(MAGIC, len(body), zlib.crc32(body) & 0xFFFFFFFF)
            write_all(self._fd, header + body)
            if self._fsync:
                self._fdatasync()
            self._frames.append(assigned)
            self._seq = base + len(assigned)
            self._cached_end = os.fstat(self._fd).st_size
            return assigned

    def _fdatasync(self) -> None:
        if hasattr(os, "fdatasync"):
            os.fdatasync(self._fd)
        else:
            os.fsync(self._fd)

    # ---- 查询 -----------------------------------------------------
    def frames(self) -> list[list[dict]]:
        return list(self._frames)

    def records(self) -> list[dict]:
        return [rec for frame in self._frames for rec in frame]

    @property
    def tail(self) -> int:
        return self._seq

    def close(self) -> None:
        try:
            os.close(self._fd)
        except OSError:
            pass

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc) -> None:
        self.close()
