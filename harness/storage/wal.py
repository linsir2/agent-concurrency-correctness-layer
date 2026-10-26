"""Append-only WAL（方案 §4.3）。

帧格式：`[magic:4s][len:u32][crc32:u32][payload: json]`
- payload 是**一个记录列表**：多资源提交写在**同一帧**里 → 一帧要么全生效要么全不生效。
- `append_frame` 返回前 `fdatasync` → **返回成功 == 已落盘**（提交点）。
- 恢复：逐帧校验 magic/len/crc/seq 连续性；只允许**最后一段的尾部**截断，
  中段坏帧抛 `WalCorrupt`（对应 `COMMIT_UNRECOVERABLE`，不静默丢数据）。
- 阶段一**不轮转**（单段）：删段需要订阅者 cursor 持久化，属 MS6。
"""

from __future__ import annotations

import json
import os
import struct
import zlib
from pathlib import Path

from .atomic import fsync_dir, write_all

MAGIC = b"MWAL"
HEADER = struct.Struct(">4sII")


class WalCorrupt(Exception):
    """中段损坏：不可恢复（不可静默截断，否则会丢后续有效事件）。"""


class Wal:
    def __init__(self, path: str | Path, fsync: bool = True) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fsync = fsync
        self.truncated_at: int | None = None
        self._frames: list[list[dict]] = []
        self._seq = 0
        self._load()
        self._fd = os.open(self.path, os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0o644)
        fsync_dir(self.path)

    # ---- 读 -------------------------------------------------------
    def _load(self) -> None:
        if not self.path.exists():
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

    # ---- 写 -------------------------------------------------------
    def append_frame(self, records: list[dict]) -> list[dict]:
        """给每个记录分配 seq，整帧一次写入并 fdatasync（提交点）。"""
        if not records:
            return []
        base = self._seq
        assigned = []
        for i, rec in enumerate(records, start=1):
            item = dict(rec)
            item["seq"] = base + i
            assigned.append(item)
        body = json.dumps(assigned, sort_keys=True, separators=(",", ":"),
                          ensure_ascii=False, default=str).encode("utf-8")
        header = HEADER.pack(MAGIC, len(body), zlib.crc32(body) & 0xFFFFFFFF)
        write_all(self._fd, header + body)
        if self._fsync:
            self._fdatasync()
        self._frames.append(assigned)
        self._seq = base + len(assigned)
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
