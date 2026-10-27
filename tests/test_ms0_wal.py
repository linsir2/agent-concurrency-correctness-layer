"""S1 出口：WAL 持久化（方案 §8 S1 / T1–T4）。

判据：
- T1 尾部半帧 → 重开时截断，只保留完整帧
- T2 `append` 返回后，**独立进程**能读到（fsync 生效）
- T3 中段坏帧 → `WalCorrupt`（对应 COMMIT_UNRECOVERABLE，不静默丢后续事件）
- T4 重放重建 seq / by_subject / event_id
"""

from __future__ import annotations

import json
import os
import struct
import subprocess
import sys
import threading
import time
import zlib
from pathlib import Path

import pytest

from harness import EventKind, WalEventLog
from harness.storage import Wal, WalCorrupt, atomic_write, canonical_bytes
from harness.storage._lock import _FileLock, wal_lock_path
from harness.storage.wal import HEADER, MAGIC

pytestmark = pytest.mark.ms0
ROOT = Path(__file__).resolve().parents[1]


def _wal_path(tmp_path: Path) -> Path:
    return tmp_path / "wal.log"


def test_t1_torn_tail_truncated(tmp_path):
    path = _wal_path(tmp_path)
    log = WalEventLog(path, fsync=False)
    for i in range(3):
        log.append(EventKind.RESOURCE_COMMITTED, f"r{i}", {"i": i})
    log.close()
    complete = path.stat().st_size

    # 手工追加一个「半帧」：头写全了，body 只写一部分
    with open(path, "ab") as f:
        f.write(HEADER.pack(MAGIC, 100, 0))
        f.write(b"partial")

    reopened = WalEventLog(path, fsync=False)
    assert [e.subject for e in reopened.all()] == ["r0", "r1", "r2"]
    assert reopened.tail() == 3
    assert reopened.truncated_at == complete
    assert path.stat().st_size == complete
    reopened.close()


def test_t2_fsync_durable_across_process(tmp_path):
    path = _wal_path(tmp_path)
    log = WalEventLog(path, fsync=True)          # 真实 fdatasync
    log.append(EventKind.RESOURCE_COMMITTED, "a", {"v": 1}, run_id="R")
    log.append(EventKind.RESOURCE_COMMITTED, "b", {"v": 2}, run_id="R")
    log.append(EventKind.RESOURCE_COMMITTED, "c", {"v": 3}, run_id="R")

    code = ("import sys; from harness.storage.wal import Wal; "
            "w = Wal(sys.argv[1]); print(len(w.records()))")
    out = subprocess.check_output(
        [sys.executable, "-c", code, str(path)],
        cwd=str(ROOT), env={**os.environ, "PYTHONPATH": str(ROOT)},
    )
    assert out.strip() == b"3"
    log.close()


def test_t3_mid_frame_corruption_is_unrecoverable(tmp_path):
    path = _wal_path(tmp_path)
    log = WalEventLog(path, fsync=False)
    for i in range(3):
        log.append(EventKind.RESOURCE_COMMITTED, f"r{i}", {"i": i})
    log.close()

    raw = bytearray(path.read_bytes())
    first_len = HEADER.unpack_from(raw, 0)[1]
    second = HEADER.size + first_len
    raw[second + 8] ^= 0xFF                       # 破坏第二帧的 CRC
    path.write_bytes(bytes(raw))

    with pytest.raises(WalCorrupt):
        Wal(path)


def test_t4_replay_rebuilds_seq_and_index(tmp_path):
    path = _wal_path(tmp_path)
    log = WalEventLog(path, fsync=False)
    batch = log.append_batch([
        {"kind": EventKind.RESOURCE_COMMITTED, "subject": "a", "payload": {"v": 1}},
        {"kind": EventKind.RESOURCE_COMMITTED, "subject": "b", "payload": {"v": 2}},
    ])
    single = log.append(EventKind.TASK_COMPLETED, "t1", {}, run_id="R")
    assert [e.seq for e in batch] == [1, 2]
    assert single.seq == 3
    log.close()

    reopened = WalEventLog(path, fsync=False)
    assert reopened.tail() == 3
    assert [e.subject for e in reopened.replay("a")] == ["a"]
    assert reopened.has("t1", EventKind.TASK_COMPLETED)
    assert reopened.all()[2].event_id == single.event_id
    assert reopened.all()[0].run_id == ""
    assert reopened.all()[2].run_id == "R"
    reopened.close()


def test_atomic_write_is_all_or_nothing(tmp_path):
    target = tmp_path / "sub" / "f.json"
    atomic_write(target, canonical_bytes({"k": [1, 2, 3]}))
    assert target.read_bytes() == b'{"k":[1,2,3]}'
    atomic_write(target, canonical_bytes({"k": [9]}))
    assert target.read_bytes() == b'{"k":[9]}'
    # 目录里不应残留临时文件
    leftovers = [p.name for p in target.parent.iterdir() if p.name.startswith(".tmp-")]
    assert leftovers == []


def test_final_6_1_concurrent_construction_does_not_truncate_inflight(tmp_path):
    """终审 6-1：另一写者只写了 frame header（body 在途）时构造 Wal 不得截断。

    旧实现 `_load` 不在 `wal.lock` 内 → 看到 `body_end > len` 立即 truncate；
    写者补完 body 后文件变成「无 header 的 body」→ 重开 `WalCorrupt`。
    """
    path = _wal_path(tmp_path)
    body1 = json.dumps([{"kind": "x", "subject": "s", "payload": {}, "seq": 1}],
                       sort_keys=True, separators=(",", ":")).encode()
    body2 = json.dumps([{"kind": "x", "subject": "s2", "payload": {}, "seq": 2}],
                       sort_keys=True, separators=(",", ":")).encode()
    half = len(body2) // 2
    with open(path, "wb") as fh:
        fh.write(HEADER.pack(MAGIC, len(body1), zlib.crc32(body1) & 0xFFFFFFFF))
        fh.write(body1)
        fh.write(HEADER.pack(MAGIC, len(body2), zlib.crc32(body2) & 0xFFFFFFFF))
        fh.write(body2[:half])                    # 在途帧：body 未写完
        fh.flush()
        os.fsync(fh.fileno())

    lock = _FileLock(wal_lock_path(path))
    started = threading.Event()

    def writer():
        with lock():                              # 模拟持锁写者
            started.set()
            time.sleep(0.35)
            with open(path, "ab") as fh:
                fh.write(body2[half:])            # 补完 body
                fh.flush()
                os.fsync(fh.fileno())

    thread = threading.Thread(target=writer)
    thread.start()
    assert started.wait(5)
    wal = Wal(path)                               # 必须等锁 → 看到完整帧
    thread.join(5)

    assert wal.truncated_at is None, "构造期截断了在途帧"
    assert wal.tail == 2
    assert Wal(path).tail == 2, "重开必须不抛 WalCorrupt"
