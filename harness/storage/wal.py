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
- append 前在**同一 flock 内**读 store.json 的 epoch 复验，`writer < store` → 拒绝。
"""

from __future__ import annotations

import json
import os
import struct
import zlib
from pathlib import Path

from ._lock import FencingRejected, WAL_ORDER, _FileLock, wal_lock_path
from .atomic import fsync_dir, write_all

MAGIC = b"MWAL"
HEADER = struct.Struct(">4sII")


class WalCorrupt(Exception):
    """中段损坏：不可恢复（不可静默截断，否则会丢后续有效事件）。"""


class Wal:
    def __init__(self, path: str | Path, fsync: bool = True, lock=None) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fsync = fsync
        # wal.lock：三锁全序的第 2 位（resource → receipts → wal → store → eventlog）
        self._lock = (lock if lock is not None
                      else _FileLock(wal_lock_path(self.path), order=WAL_ORDER))
        self.truncated_at: int | None = None
        self._frames: list[list[dict]] = []
        self._seq = 0
        # 缓存尾偏移：append 后立即记录；跨进程写入后 fstat 比对可 O(1) 发现
        self._cached_end: int = 0
        self._load()
        self._fd = os.open(self.path, os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0o644)
        fsync_dir(self.path)

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
        """
        self.truncated_at = None
        self._frames = []
        self._load()

    # ---- 写 -------------------------------------------------------
    def append_frame(self, records: list[dict], *, fencing_epoch: int | None = None,
                     fencing_lock=None, fencing_reader=None) -> list[dict]:
        """给每个记录分配 seq，整帧一次写入并 fdatasync（提交点）。

        `fencing_lock` / `fencing_reader` 提供时启用 fencing：在**同一 wal.lock
        临界区**内先 `with fencing_lock():` 读 store.json 的 epoch 复验，
        `fencing_epoch < store_epoch` → 抛 `FencingRejected`（不写入、不 ack）。
        """
        if not records:
            return []
        with self._lock():
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

    def __enter__(self) -> "Wal":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
