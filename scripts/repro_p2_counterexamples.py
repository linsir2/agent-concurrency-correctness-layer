#!/usr/bin/env python3
"""对抗评审 **P2 清单**（1-7）逐条复现 + 修复前后对比。

用法：
    python3 scripts/repro_p2_counterexamples.py            # 全部
    python3 scripts/repro_p2_counterexamples.py 1 3 7      # 指定条目

每条给出 `[BEFORE]`（旧实现/旧行为，用内联旧代码模拟）与 `[AFTER]`（当前实现）
的对照输出。退出码恒为 0（演示脚本，不以失败为判据）。
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile
import traceback
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import harness.checkpoint as cp  # noqa: E402
from harness import ACB, EventKind, EventLog, Harness, Write  # noqa: E402
from harness.checkpoint import FileCheckpointStore, atomic_write  # noqa: E402
from harness.spec.receipt import (GENESIS_HASH, Receipt,  # noqa: E402
                                  ReceiptMalformed, build_receipt,
                                  receipt_hash)
from harness.storage.atomic import fsync_dir, write_all  # noqa: E402
from harness.storage.blob import BlobCorrupt, FileBlobStore  # noqa: E402
from harness.storage.codec import dumps, loads  # noqa: E402


def _acb(acb_id="A", gen=1):
    acb = ACB(id=acb_id, run_id="R", task_id="T")
    acb.generation = gen
    acb.staged = {"r": {"v": 1}}
    return acb


# ------------------------------------------------------------------ P2-1
def _old_write_new(path: Path, data: bytes) -> None:
    """旧实现：直接写最终路径（SIGKILL/短写 → 半截最终文件）。"""
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    try:
        write_all(fd, data)
        os.fsync(fd)
    finally:
        os.close(fd)
    fsync_dir(path)


def p2_1():
    print("=" * 72)
    print("P2-1 ref 写入非原子（旧：直接写最终路径；新：tmp+fsync+os.link）")
    ref = "CP-" + "0" * 8 + "-00000000000000000000-" + "a" * 12
    data = b'{"full": "checkpoint-envelope"}'

    # BEFORE
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        store = FileCheckpointStore(root)
        path = store._ref_path(ref)
        store._ensure_dir(path.parent)
        orig = cp.write_all

        def half_then_raise(fd, buf):
            orig(fd, buf[: len(buf) // 2])
            raise OSError("模拟 SIGKILL/短写")

        cp.write_all = half_then_raise
        try:
            _old_write_new(path, data)
        except OSError:
            pass
        finally:
            cp.write_all = orig
        print(f"[BEFORE] 崩溃后最终文件存在：{path.exists()}"
              f"（半截最终文件 {path.name}）")
        s2 = FileCheckpointStore(root)
        print(f"[BEFORE] _scan_max_seq 计入半截文件 → {s2._scan_max_seq()}"
              f"（seq 槽位被坏文件占住）")

    # AFTER
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        store = FileCheckpointStore(root)
        path = store._ref_path(ref)
        store._ensure_dir(path.parent)
        orig = cp.write_all

        def half_then_raise(fd, buf):
            orig(fd, buf[: len(buf) // 2])
            raise OSError("模拟 SIGKILL/短写")

        cp.write_all = half_then_raise
        try:
            store._write_new(path, data)
        except OSError:
            pass
        finally:
            cp.write_all = orig
        tmp = path.with_name(path.name + ".tmp")
        print(f"[AFTER]  崩溃后最终文件存在：{path.exists()}（绝不半截）")
        print(f"[AFTER]  残留 tmp：{tmp.exists()}（{tmp.name}）")
        s2 = FileCheckpointStore(root)
        print(f"[AFTER]  重开后 tmp 被隔离为 .corrupt："
              f"{tmp.with_name(tmp.name + '.corrupt').exists()}")
        print(f"[AFTER]  _scan_max_seq 不计入 tmp → {s2._scan_max_seq()}")
        print(f"[AFTER]  latest_ref_for_acb('A') → {s2.latest_ref_for_acb('A')}")


# ------------------------------------------------------------------ P2-2
def _tamper_next_seq(store, value=0):
    meta_path = store._meta_path
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    meta["next_seq"] = value
    meta_path.write_text(json.dumps(meta), encoding="utf-8")


def p2_2():
    print("=" * 72)
    print("P2-2 seq 复用（旧：_init_meta 修正只改内存不落盘；新：立即回写）")

    # BEFORE
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        store = FileCheckpointStore(root)
        store.save(_acb("A"))
        _tamper_next_seq(store, 0)
        orig = cp.atomic_write
        cp.atomic_write = lambda *a, **k: None          # 禁用落盘（模拟旧行为）
        try:
            reopened = FileCheckpointStore(root)
            mem = reopened._meta["next_seq"]
        finally:
            cp.atomic_write = orig
        disk = json.loads(store._meta_path.read_text(encoding="utf-8"))["next_seq"]
        print(f"[BEFORE] 内存 next_seq={mem}，磁盘 next_seq={disk}（修正未落盘）")
        try:
            ref1 = reopened.save(_acb("B"))
            print(f"[BEFORE] save 返回 seq={int(ref1.split('-')[2])}（复用了旧 seq）")
        except FileExistsError as exc:
            print(f"[BEFORE] save 抛 FileExistsError（seq 复用撞已用 ref）：{exc}")

    # AFTER
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        store = FileCheckpointStore(root)
        store.save(_acb("A"))
        _tamper_next_seq(store, 0)
        reopened = FileCheckpointStore(root)
        ref1 = reopened.save(_acb("B"))
        disk = json.loads(store._meta_path.read_text(encoding="utf-8"))["next_seq"]
        print(f"[AFTER]  内存 next_seq={reopened._meta['next_seq']}，磁盘 next_seq={disk}")
        print(f"[AFTER]  save 返回 seq={int(ref1.split('-')[2])}"
              f"（严格递增，不复用；load_strict ok="
              f"{reopened.load_strict(ref1)['acb_id'] == 'B'}）")


# ------------------------------------------------------------------ P2-3
def _old_write_pointer(path, ref, acb, seq, now):
    pointer = {"ref": ref, "acb_id": acb.id, "run_id": acb.run_id,
               "task_id": acb.task_id, "logical_subgoal_id": acb.logical_subgoal_id,
               "generation": int(acb.generation), "save_seq": seq,
               "updated_at_ms": now}
    atomic_write(path, dumps(pointer).encode("utf-8"))


def _old_read_pointer(store, path):
    if not path.exists():
        return None
    data = loads(path.read_bytes().decode("utf-8"))
    ref = data.get("ref")
    try:
        return ref if store._ref_path(ref).exists() else None
    except ValueError:
        return None


def p2_3():
    print("=" * 72)
    print("P2-3 latest 指针无单调 CAS + 32bit hash + 不校验 acb_id")

    print(f"[BEFORE] 旧指针 hash sha256[:8]  = "
          f"{hashlib.sha256('X'.encode()).hexdigest()[:8]}（32bit，~7 万 ACB 50% 碰撞）")
    print(f"[AFTER]  新指针 hash sha256[:16] = "
          f"{hashlib.sha256('X'.encode()).hexdigest()[:16]}（64bit）")

    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        store = FileCheckpointStore(root)
        ref1 = store.save(_acb("A", gen=1))
        ref2 = store.save(_acb("A", gen=2))
        path = store._latest_path("latest", "A")
        # BEFORE：无条件覆盖 → 滞后写者回退
        _old_write_pointer(path, ref1, _acb("A", gen=1), 0, 1)
        print(f"[BEFORE] 滞后写者回写后 latest = {store.latest_ref_for_acb('A')}"
              f"（回退到旧 ref {ref1}）")
        # AFTER：CAS → 不回退
        store._write_pointer(path, ref2, _acb("A", gen=2), 1, 2)
        store._write_pointer(path, ref1, _acb("A", gen=1), 0, 3)
        print(f"[AFTER]  滞后写者回写后 latest = {store.latest_ref_for_acb('A')}"
              f"（不回退，仍为 {ref2}）")

    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        store = FileCheckpointStore(root)
        store.save(_acb("A"))
        path = store._latest_path("latest", "A")
        doc = json.loads(path.read_text(encoding="utf-8"))
        doc["acb_id"] = "B"                      # 模拟 hash 碰撞串号
        path.write_text(json.dumps(doc), encoding="utf-8")
        print(f"[BEFORE] 旧 _read_pointer（不校验 acb_id）返回："
              f"{_old_read_pointer(store, path)}（把 B 的 ref 当成 A 的 latest）")
        print(f"[AFTER]  新 latest_ref_for_acb('A') 返回："
              f"{store.latest_ref_for_acb('A')}（键不匹配 → None）")


# ------------------------------------------------------------------ P2-4
def p2_4():
    print("=" * 72)
    print("P2-4 _receipt_ids 重建不校验链连续性（旧：折叠重复；新：标 tainted）")
    h = Harness(auth_secret="s3cr3t")
    r1 = build_receipt(receipt_id="R1", run_id="R", task_id="T",
                       logical_subgoal_id="S", resource="r",
                       version_before=0, version_after=1, secret="s3cr3t")
    r2 = build_receipt(receipt_id="R1", run_id="R", task_id="T",   # 重复 id
                       logical_subgoal_id="S", resource="r",
                       version_before=1, version_after=2,
                       prev_receipt_hash="deadbeef" + "0" * 8, secret="s3cr3t")
    for r in (r1, r2):
        h.event_log.append(EventKind.RECEIPT_ISSUED, r.receipt_id, r.to_dict(),
                           producer="A", run_id=r.run_id)

    # BEFORE：旧实现（最后一条生效、ids.add 折叠重复）
    heads, ids = {}, set()
    for ev in h.event_log.all():
        if ev.kind == EventKind.RECEIPT_ISSUED:
            receipt = Receipt.from_dict(ev.payload or {})
            heads[(receipt.run_id, receipt.logical_subgoal_id)] = receipt_hash(receipt)
            ids.add(receipt.receipt_id)
    print(f"[BEFORE] 旧实现 ids={sorted(ids)}（重复 R1 被折叠），"
          f"heads={ {k: v[:8] for k, v in heads.items()} }")

    # AFTER
    h._rebuild_receipts()
    print(f"[AFTER]  新实现 _tainted_scopes={h._tainted_scopes}")
    print(f"[AFTER]  verify_receipts('R','S') → {h.verify_receipts('R', 'S')}")


# ------------------------------------------------------------------ P2-5
def p2_5():
    print("=" * 72)
    print("P2-5 dedup 检查在 scope 锁外 → 非原子（旧：链分叉；新：临界区内）")
    from contextlib import nullcontext
    import threading
    import harness.harness as hm

    def run(use_nullcontext: bool) -> bool:
        h = Harness(auth_secret="s3cr3t")
        h.workspace.put("r1", {"v": 0})
        h.workspace.put("r2", {"v": 0})
        a = ACB(id="A", run_id="R", task_id="T1")
        a.base_version["r1"] = 1
        a.staged["r1"] = {"v": 1}
        b = ACB(id="B", run_id="R", task_id="T2")
        b.base_version["r2"] = 1
        b.staged["r2"] = {"v": 1}
        orig_build = hm.build_receipt
        if use_nullcontext:
            h._scope_lock = lambda scope: nullcontext()   # type: ignore[method-assign]
            barrier = threading.Barrier(2, timeout=5)

            def build_with_barrier(**kw):
                barrier.wait()                            # 读完 head、append 前
                return orig_build(**kw)

            hm.build_receipt = build_with_barrier
        results = []

        def worker(acb, res):
            try:
                results.append(h.commit_batch(acb, [Write(res)]))
            except BaseException as exc:  # noqa: BLE001
                results.append(exc)

        ts = [threading.Thread(target=worker, args=(a, "r1")),
              threading.Thread(target=worker, args=(b, "r2"))]
        for t in ts:
            t.start()
        for t in ts:
            t.join(timeout=10)
        if use_nullcontext:
            hm.build_receipt = orig_build
        return h.verify_receipts("R", "") is True

    ok = run(use_nullcontext=True)
    print(f"[BEFORE] scope 锁=nullcontext + 去重/head 在锁外 → verify={ok}（链分叉）")
    ok = run(use_nullcontext=False)
    print(f"[AFTER]  scope 临界区覆盖去重/head → verify={ok}（不分叉）")


# ------------------------------------------------------------------ P2-6
def _old_from_dict(data):
    """旧实现：直接 data["run_id"] → 缺字段时裸 KeyError。"""
    return Receipt(
        receipt_id=data["receipt_id"], run_id=data["run_id"],
        task_id=data["task_id"],
        logical_subgoal_id=data.get("logical_subgoal_id", ""),
        resource=data.get("resource", ""),
        version_before=int(data.get("version_before", 0)),
        version_after=int(data.get("version_after", 0)),
        effect_id=data.get("effect_id"), idem_key=data.get("idem_key"),
        compensation_ref=data.get("compensation_ref"),
        prev_receipt_hash=data.get("prev_receipt_hash", GENESIS_HASH),
        signature=data.get("signature", ""),
        schema_version=int(data.get("schema_version", 1)),
    )


def p2_6():
    print("=" * 72)
    print("P2-6 畸形收据事件让启动/验签抛 KeyError（旧：KeyError；新：ReceiptMalformed）")
    payload = {"receipt_id": "R", "task_id": "T"}        # 缺 run_id

    # BEFORE
    try:
        _old_from_dict(payload)
        print("[BEFORE] from_dict 未抛（?）")
    except KeyError as exc:
        print(f"[BEFORE] 旧 from_dict 抛 KeyError：{exc}")
    log = EventLog()
    log.append(EventKind.RECEIPT_ISSUED, "R", payload, run_id="")
    orig_fd = Receipt.from_dict
    Receipt.from_dict = classmethod(lambda cls, data: _old_from_dict(data))
    try:
        Harness(event_log=log)
        print("[BEFORE] Harness 启动未抛（?）")
    except KeyError as exc:
        print(f"[BEFORE] Harness 启动抛 KeyError（单条坏事件打挂启动）：{exc}")
    finally:
        Receipt.from_dict = orig_fd

    # AFTER
    try:
        Receipt.from_dict(payload)
        print("[AFTER]  from_dict 未抛（?）")
    except ReceiptMalformed as exc:
        print(f"[AFTER]  新 from_dict 抛 ReceiptMalformed：{exc}")
    h = Harness(event_log=log)
    print(f"[AFTER]  Harness 启动 OK；_malformed_receipts="
          f"{[m['error'] for m in h._malformed_receipts]}")
    print(f"[AFTER]  verify_receipts('','') → {h.verify_receipts('', '')}")


# ------------------------------------------------------------------ P2-7
def p2_7():
    print("=" * 72)
    print("P2-7 blob 读不校验内容哈希（旧：静默返回篡改内容；新：BlobCorrupt）")
    with tempfile.TemporaryDirectory() as d:
        store = FileBlobStore(Path(d) / "blobs")
        blob_hash = store.put(b"hello-p2")
        path = store._path(blob_hash)
        path.write_bytes(b"hello-p2!")                   # 位翻转/坏盘
        # BEFORE：旧 get 直接 read_bytes
        raw = path.read_bytes()
        print(f"[BEFORE] 旧 get 直接 read_bytes → {raw!r}（与 blob_hash 失配但静默）")
        # AFTER
        try:
            store.get(blob_hash)
            print("[AFTER]  get 未抛（?）")
        except BlobCorrupt as exc:
            print(f"[AFTER]  新 get 抛 BlobCorrupt：{exc}")


# ------------------------------------------------------------------ runner
CASES = [
    ("1", p2_1), ("2", p2_2), ("3", p2_3), ("4", p2_4),
    ("5", p2_5), ("6", p2_6), ("7", p2_7),
]


def main(argv):
    wanted = set(argv[1:]) if len(argv) > 1 else None
    for num, fn in CASES:
        if wanted and num not in wanted:
            continue
        try:
            fn()
        except BaseException:  # noqa: BLE001
            print(f"[ERROR] 条目{num} 复现脚本异常：")
            traceback.print_exc()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
