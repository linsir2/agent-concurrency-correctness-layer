"""S24-6：只读诊断 CLI。

RIPR：
- Reachability：真实 durable 状态的 WAL / 收据 / 版本 / token / store 都被读到；
- Infection：中段 CRC、断链、版本跳跃、重复幂等键、租约 epoch 落后各造 1 例；
- Propagation：问题 → exit 1 且 finding 的 `code` 正确；干净状态 → exit 0；
- Revealability：JSON 报告字段完整；**诊断前后状态目录逐字节不变**（只读）。

诊断必须只读：本文件用「全目录哈希前后一致」兜底，防止哪天顺手调用了
`Wal()` / `FileCheckpointStore()` 的构造器（它们会写盘）。
"""

from __future__ import annotations

import hashlib
import json
import pathlib
import zlib

from harness import RscSession
from harness.diagnose import diagnose, main, read_wal
from harness.event_log import WalEventLog
from harness.harness import Harness
from harness.spec.receipt import build_receipt, receipt_hash
from harness.storage.wal import HEADER, Wal

SECRET = "diag-secret"


# ---------------------------------------------------------------- 工具
def _draft(kind: str, subject: str, payload: dict, seq_hint: int = 0) -> dict:
    return {"kind": kind, "subject": subject, "payload": payload,
            "producer": "diag", "caused_by": "", "idempotency_key": "",
            "run_id": payload.get("run_id", ""), "event_id": f"e{seq_hint}",
            "ts": 1, "schema_version": 1}


def _wal(path: pathlib.Path, frames: list[list[dict]]) -> None:
    w = Wal(str(path), fsync=False)
    for frame in frames:
        w.append_frame(frame)
    w.close()


def _receipt_payload(*, rid: str, prev: str, run: str = "run-1",
                     sub: str = "g", version: int = 1) -> dict:
    r = build_receipt(receipt_id=rid, run_id=run, task_id="t",
                      logical_subgoal_id=sub, resource="a.md",
                      version_before=version - 1, version_after=version,
                      prev_receipt_hash=prev, secret=SECRET)
    return r.to_dict()


def _tree_digest(root: pathlib.Path) -> str:
    h = hashlib.sha256()
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        h.update(str(path.relative_to(root)).encode("utf-8"))
        h.update(path.read_bytes())
    return h.hexdigest()


def _durable_session(tmp: pathlib.Path, *, run_limits: int = 10):
    """建一个真实 durable 状态：WAL + checkpoint + RscSession(token 索引)。"""
    root = tmp / "state"
    root.mkdir(parents=True)
    wal = WalEventLog(str(root / "wal.log"), fsync=False)
    h = Harness(event_log=wal, checkpoint_root=str(root),
                auth_secret=SECRET, run_limits=run_limits)
    session = RscSession(h, tenant="t", min_budget=1, state_dir=str(root))
    return root, wal, h, session


# ---------------------------------------------------------------- 干净状态
def test_clean_durable_state_is_ok(tmp_path):
    root, wal, _h, session = _durable_session(tmp_path)
    tok = session.begin("agent-a", "goal", writes=["a.md"])
    assert session.apply(tok.token, [("a.md", "v1")],
                         idempotency_key="k1", agent_id="agent-a")
    assert session.apply(tok.token, [("a.md", "v2")],
                         idempotency_key="k2", agent_id="agent-a")
    session.finish(tok.token)

    report = diagnose(state_dir=root, secret=SECRET)
    assert report["ok"], report["findings"]
    codes = {c["id"]: c["status"] for c in report["checks"]}
    assert codes["wal.integrity"] == "pass"
    assert codes["resource.versions"] == "pass"
    assert codes["receipt.chain"] == "pass"
    assert report["stats"]["records"] > 0
    assert report["stats"]["receipts"] == 2
    assert report["stats"]["resources"] == 1
    wal.close()


def test_diagnose_is_read_only(tmp_path):
    root, wal, _h, session = _durable_session(tmp_path)
    tok = session.begin("a", "g", writes=["a.md"])
    session.apply(tok.token, [("a.md", "v1")], idempotency_key="k", agent_id="a")
    before = _tree_digest(root)
    names_before = {p.name for p in root.iterdir()}
    report = diagnose(state_dir=root, secret=SECRET)
    assert report["ok"]
    assert _tree_digest(root) == before
    assert {p.name for p in root.iterdir()} == names_before
    wal.close()


def test_missing_wal_is_skip_not_fail(tmp_path):
    root = tmp_path / "state"
    root.mkdir()
    (root / "tokens.json").write_text("{}", encoding="utf-8")
    report = diagnose(state_dir=root)
    assert report["ok"]
    assert any(c["id"] == "wal.integrity" and c["status"] == "skip"
               for c in report["checks"])


# ---------------------------------------------------------------- WAL 损坏
def test_mid_frame_crc_is_error(tmp_path):
    path = tmp_path / "wal.log"
    _wal(path, [[_draft("RESOURCE_COMMITTED", "a.md",
                        {"resource": "a.md", "version": 1})],
                [_draft("RESOURCE_COMMITTED", "b.md",
                        {"resource": "b.md", "version": 1})],
                [_draft("RESOURCE_COMMITTED", "c.md",
                        {"resource": "c.md", "version": 1})]])
    raw = bytearray(path.read_bytes())
    _, first_len, _ = HEADER.unpack_from(raw, 0)
    # 第 2 帧（中段，不是末帧）的 body 改 1 字节
    second_body = HEADER.size + first_len + HEADER.size
    raw[second_body + 3] ^= 0xFF
    path.write_bytes(bytes(raw))
    records, integrity, _, _ = read_wal(path)
    assert integrity.status == "fail"
    assert integrity.findings[0].code == "WAL_CRC_MISMATCH"
    assert len(records) == 1               # 第 1 帧完整，之后不再解析


def test_tail_truncation_is_warning_and_strict_fails(tmp_path):
    path = tmp_path / "wal.log"
    _wal(path, [[_draft("RESOURCE_COMMITTED", "a.md",
                        {"resource": "a.md", "version": 1})],
                [_draft("RESOURCE_COMMITTED", "a.md",
                        {"resource": "a.md", "version": 2})]])
    raw = path.read_bytes()
    path.write_bytes(raw[:-7])             # 末帧 body 截断
    records, integrity, _, _ = read_wal(path)
    assert integrity.status == "warn"
    assert integrity.findings[0].code == "WAL_TAIL_TRUNCATED"
    assert len(records) == 1               # 只保留完整帧
    report = diagnose(wal=path)
    assert report["ok"] is True            # 非 strict：尾部半写不算 error
    report = diagnose(wal=path, strict=True)
    assert report["ok"] is False


def test_seq_gap_is_error(tmp_path):
    path = tmp_path / "wal.log"
    w = Wal(str(path), fsync=False)
    w.append_frame([_draft("TOOL_COMPLETED", "x", {})])
    w.close()
    # 手工追加一帧 seq 跳号（模拟另一写者写坏）
    draft = _draft("TOOL_COMPLETED", "y", {})
    draft["seq"] = 99
    body = json.dumps([draft], sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False).encode("utf-8")
    header = HEADER.pack(b"MWAL", len(body), zlib.crc32(body) & 0xFFFFFFFF)
    with open(path, "ab") as fh:
        fh.write(header + body)
    records, _, ordering, _ = read_wal(path)
    assert len(records) == 2
    assert ordering.findings[0].code == "WAL_SEQ_GAP"


# ---------------------------------------------------------------- 语义检查
def test_broken_receipt_chain(tmp_path):
    path = tmp_path / "wal.log"
    good = _receipt_payload(rid="r1", prev="0" * 16, version=1)
    bad = _receipt_payload(rid="r2", prev="f" * 16, version=2)
    _wal(path, [[_draft("RECEIPT_ISSUED", "a.md", good)],
                [_draft("RECEIPT_ISSUED", "a.md", bad)]])
    report = diagnose(wal=path, secret=SECRET)
    assert report["ok"] is False
    assert any(f["code"] == "RECEIPT_CHAIN_BROKEN" for f in report["findings"])


def test_duplicate_receipt_id(tmp_path):
    path = tmp_path / "wal.log"
    r1 = build_receipt(receipt_id="dup", run_id="run-1", task_id="t",
                       logical_subgoal_id="g", resource="a.md",
                       version_before=0, version_after=1,
                       prev_receipt_hash="0" * 16, secret=SECRET)
    r2 = build_receipt(receipt_id="dup", run_id="run-1", task_id="t",
                       logical_subgoal_id="g", resource="a.md",
                       version_before=1, version_after=2,
                       prev_receipt_hash=receipt_hash(r1), secret=SECRET)
    _wal(path, [[_draft("RECEIPT_ISSUED", "a.md", r1.to_dict())],
                [_draft("RECEIPT_ISSUED", "a.md", r2.to_dict())]])
    report = diagnose(wal=path, secret=SECRET)
    assert any(f["code"] == "RECEIPT_DUPLICATE_ID" for f in report["findings"])


def test_resource_version_gap(tmp_path):
    path = tmp_path / "wal.log"
    _wal(path, [[_draft("RESOURCE_COMMITTED", "a.md",
                        {"resource": "a.md", "version": 1})],
                [_draft("RESOURCE_COMMITTED", "a.md",
                        {"resource": "a.md", "version": 3})]])
    report = diagnose(wal=path)
    assert report["ok"] is False
    assert any(f["code"] == "RESOURCE_VERSION_REGRESSION"
               for f in report["findings"])


def test_duplicate_effect_idem_key(tmp_path):
    path = tmp_path / "wal.log"
    _wal(path, [[_draft("EFFECT_INTENT", "acb",
                        {"idem_key": "same", "tool": "t"})],
                [_draft("EFFECT_INTENT", "acb",
                        {"idem_key": "same", "tool": "t"})]])
    report = diagnose(wal=path)
    assert report["ok"] is False
    assert any(f["code"] == "EFFECT_DUPLICATE_INTENT" for f in report["findings"])


def test_lease_epoch_behind_wal(tmp_path):
    root = tmp_path / "state"
    root.mkdir()
    _wal(root / "wal.log", [[_draft("TOOL_COMPLETED", "x", {})]])
    (root / "wal.log.epoch").write_text("5", encoding="utf-8")
    (root / "writer_lease.json").write_text(json.dumps(
        {"owner": "w1", "epoch": 4, "expires_at": 10 ** 12}), encoding="utf-8")
    report = diagnose(state_dir=root)
    assert report["ok"] is False
    assert any(f["code"] == "LEASE_EPOCH_BEHIND" for f in report["findings"])


def test_malformed_tokens_index(tmp_path):
    root = tmp_path / "state"
    root.mkdir()
    (root / "tokens.json").write_text("[]", encoding="utf-8")
    report = diagnose(state_dir=root)
    assert report["ok"] is False
    assert any(f["code"] == "TOKENS_MALFORMED" for f in report["findings"])


# ---------------------------------------------------------------- CLI
def test_cli_usage_error(tmp_path):
    assert main([]) == 2
    assert main(["--state-dir", str(tmp_path / "nope")]) == 2
    assert main(["--wal", str(tmp_path / "nope.log")]) == 2


def test_cli_json_schema_and_exit_zero(tmp_path):
    root, wal, _h, session = _durable_session(tmp_path)
    tok = session.begin("a", "g", writes=["a.md"])
    session.apply(tok.token, [("a.md", "v")], idempotency_key="k", agent_id="a")
    assert main(["--state-dir", str(root), "--secret", SECRET, "--json"]) == 0
    wal.close()


def test_cli_exit_one_on_error(tmp_path, capsys):
    root = tmp_path / "state"
    root.mkdir()
    _wal(root / "wal.log", [[_draft("RESOURCE_COMMITTED", "a.md",
                                    {"resource": "a.md", "version": 2})]])
    assert main(["--state-dir", str(root), "--json"]) == 1
    report = json.loads(capsys.readouterr().out)
    assert report["ok"] is False
    assert report["tool"] == "rsc-diagnose"
    assert {"schema_version", "checks", "findings", "stats"} <= set(report)


def test_cli_text_output_mentions_findings(tmp_path, capsys):
    root = tmp_path / "state"
    root.mkdir()
    _wal(root / "wal.log", [[_draft("RESOURCE_COMMITTED", "a.md",
                                    {"resource": "a.md", "version": 2})]])
    assert main(["--state-dir", str(root)]) == 1
    out = capsys.readouterr().out
    assert "RESOURCE_VERSION_START" in out
    assert "stats:" in out
