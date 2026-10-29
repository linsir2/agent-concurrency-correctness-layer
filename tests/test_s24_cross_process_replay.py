"""S24-P1：跨进程重放门禁（PYTHONHASHSEED 矩阵）。

RIPR：
- Reachability：子进程真的执行固定场景并回报三类摘要；
- Infection：set 入 WAL 若按 hash 序编码 → 不同 seed 摘要不同；
- Propagation：父进程断言所有 seed 摘要相等，否则 fail 并打印差异；
- Revealability：探针证明 `PYTHONHASHSEED` 真的生效（0 与 1 的 hash 不同）。
"""

from __future__ import annotations

import json
import os
import pathlib
import subprocess
import sys

import pytest

CHILD = pathlib.Path(__file__).with_name("_s24_hash_child.py")
SEEDS = ("0", "1", "2", "random")


def _run(seed: str, root: pathlib.Path) -> dict:
    env = dict(os.environ, PYTHONHASHSEED=seed, RSC_CHILD_ROOT=str(root))
    proc = subprocess.run([sys.executable, str(CHILD)], env=env,
                          capture_output=True, text=True, timeout=120,
                          check=False)
    assert proc.returncode == 0, proc.stderr
    return json.loads(proc.stdout)


def _probe(seed: str) -> str:
    env = dict(os.environ, PYTHONHASHSEED=seed)
    proc = subprocess.run([sys.executable, "-c", "print(hash('probe'))"],
                          env=env, capture_output=True, text=True, timeout=30,
                          check=False)
    assert proc.returncode == 0, proc.stderr
    return proc.stdout.strip()


def test_hashseed_probe_really_changes_hash():
    assert _probe("0") != _probe("1")


def test_same_seed_is_repeatable(tmp_path):
    a = _run("0", tmp_path / "a")
    b = _run("0", tmp_path / "b")
    assert a == b


def test_all_hashseeds_produce_identical_digests(tmp_path):
    baseline = None
    for seed in SEEDS:
        got = _run(seed, tmp_path / seed)
        if baseline is None:
            baseline = got
        assert got == baseline, f"seed={seed} 摘要漂移：{got} != {baseline}"


def test_unserializable_payload_fails_closed(tmp_path):
    from harness.storage.wal import Wal, _canon_default

    with pytest.raises(TypeError):
        _canon_default(object())
    path = tmp_path / "bad.log"
    wal = Wal(str(path), fsync=False)
    with pytest.raises(TypeError):
        wal.append_frame([{"kind": "BAD", "payload": {"x": object()}}])
    wal.close()
