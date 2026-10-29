"""S24-6：只读诊断 CLI `rsc-diagnose`。

**定位**：对一份持久化状态（WAL / checkpoint store / token 索引 / 写者租约）
做体检，输出「人能看懂 + 机器可解析」的报告。用于**事故发生后的取证**，
以及第三方接入时的自检。

**只读红线（最重要）**：不得构造 `Wal` / `FileCheckpointStore` ——
它们的构造器会 `mkdir`、`O_CREAT`、甚至截断 WAL 尾部（`harness/storage/wal.py:63`、
`:158-160`；`harness/checkpoint.py:552-565`）。本模块自己按帧格式解析 WAL，
只 `open(..., "rb")` 读文件，**绝不创建/修改/删除任何文件**（有测试兜底）。

用法：

```bash
python3 -m harness.diagnose --state-dir DIR [--wal P] [--checkpoint-root P] \
    [--tokens P] [--secret S] [--json] [--strict]
```

退出码：**0** 无 error；**1** 有 error（`--strict` 时 warn 也算）；**2** 用法/输入不可读。
"""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import os
import sys
import zlib
from dataclasses import dataclass, field
from pathlib import Path

from .checkpoint import envelope_problems, ref_path
from .spec.receipt import Receipt, ReceiptMalformed, verify_chain_detailed
from .storage.codec import decode
from .storage.wal import HEADER, MAGIC

TOOL = "rsc-diagnose"
SCHEMA_VERSION = 1


@dataclass(frozen=True)
class Finding:
    """一条诊断发现；`severity` ∈ error|warn|info。"""

    code: str
    severity: str
    message: str
    invariant: str = ""
    locator: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        return {"code": self.code, "severity": self.severity,
                "message": self.message, "invariant": self.invariant,
                "locator": self.locator}


@dataclass
class Check:
    """一个检查项的结果。"""

    id: str
    status: str = "pass"           # pass|warn|fail|skip
    summary: str = ""
    findings: list[Finding] = field(default_factory=list)

    def add(self, finding: Finding) -> None:
        self.findings.append(finding)
        self.status = "fail" if finding.severity == "error" else "warn"

    def as_dict(self) -> dict:
        return {"id": self.id, "status": self.status, "summary": self.summary,
                "findings": [f.as_dict() for f in self.findings]}


# ---------------------------------------------------------------- WAL 只读解析
def read_wal(path: Path) -> tuple[list[dict], Check, Check, dict]:
    """按帧格式只读解析 WAL；返回 `(records, 完整性检查, seq 检查, stats)`。

    只依赖 `MAGIC/HEADER` 常量（格式定义），不实例化 `Wal`。
    """
    integrity = Check("wal.integrity")
    ordering = Check("wal.seq")
    stats = {"frames": 0, "records": 0, "bytes": 0, "tail_truncated": False}
    records: list[dict] = []
    try:
        raw = path.read_bytes()
    except OSError as exc:
        integrity.status = "fail"
        integrity.add(Finding("WAL_UNREADABLE", "error", f"无法读取 WAL：{exc}"))
        return [], integrity, Check("wal.seq", "skip", "WAL 不可读"), stats

    stats["bytes"] = len(raw)
    offset = 0
    prev_seq = 0
    while offset < len(raw):
        remain = len(raw) - offset
        if remain < HEADER.size:
            stats["tail_truncated"] = True
            integrity.add(Finding("WAL_TAIL_TRUNCATED", "warn",
                                  f"尾部 {remain} 字节不足以构成帧头 @ {offset}",
                                  locator={"offset": offset}))
            break
        magic, length, crc = HEADER.unpack_from(raw, offset)
        if magic != MAGIC:
            integrity.add(Finding("WAL_MAGIC_MISMATCH", "error",
                                  f"帧头 magic 不是 {MAGIC!r} @ {offset}",
                                  locator={"offset": offset}))
            break
        body_start = offset + HEADER.size
        body_end = body_start + length
        if body_end > len(raw):
            stats["tail_truncated"] = True
            integrity.add(Finding("WAL_TAIL_TRUNCATED", "warn",
                                  f"末帧 body 越界（len={length}）@ {offset}",
                                  locator={"offset": offset}))
            break
        body = raw[body_start:body_end]
        if zlib.crc32(body) & 0xFFFFFFFF != crc:
            if body_end == len(raw):
                # 末帧 CRC 错：等价于尾部半写（`Wal._load` 也是截断处理）
                stats["tail_truncated"] = True
                integrity.add(Finding("WAL_TAIL_TRUNCATED", "warn",
                                      f"末帧 CRC 不匹配 @ {offset}",
                                      locator={"offset": offset}))
            else:
                integrity.add(Finding("WAL_CRC_MISMATCH", "error",
                                      "中段帧 CRC 不匹配（不可恢复）"
                                      f" @ {offset}",
                                      locator={"offset": offset}))
            break
        try:
            frame = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            integrity.add(Finding("WAL_BAD_JSON", "error",
                                  f"帧 payload 非法 JSON：{exc}",
                                  locator={"offset": offset}))
            break
        if not isinstance(frame, list):
            integrity.add(Finding("WAL_BAD_FRAME", "error",
                                  f"帧 payload 不是记录列表 @ {offset}",
                                  locator={"offset": offset}))
            break
        for rec in frame:
            if not isinstance(rec, dict):
                integrity.add(Finding("WAL_BAD_RECORD", "error",
                                      f"记录不是对象 @ {offset}",
                                      locator={"offset": offset}))
                continue
            seq = rec.get("seq")
            if not isinstance(seq, int) or seq != prev_seq + 1:
                ordering.add(Finding("WAL_SEQ_GAP", "error",
                                     f"seq 不连续：{seq!r} != {prev_seq + 1}",
                                     locator={"offset": offset, "seq": seq}))
                prev_seq = seq if isinstance(seq, int) else prev_seq
            else:
                prev_seq = seq
            records.append(rec)
        stats["frames"] += 1
        offset = body_end

    stats["records"] = len(records)
    integrity.summary = (f"frames={stats['frames']} records={stats['records']} "
                         f"bytes={stats['bytes']}")
    ordering.summary = f"tail_seq={prev_seq}"
    if integrity.status == "pass":
        integrity.summary += "（无损坏）"
    return records, integrity, ordering, stats


# ---------------------------------------------------------------- 租约
def _read_int(path: Path) -> int | None:
    try:
        return int(path.read_text(encoding="utf-8").strip() or "0")
    except (OSError, ValueError):
        return None


def check_lease(root: Path, wal: Path | None) -> Check:
    """写者租约 vs WAL fencing epoch（I3：epoch 不得回退）。"""
    chk = Check("writer.lease")
    path = root / "writer_lease.json"
    if not path.is_file():
        chk.status = "skip"
        chk.summary = "无 writer_lease.json（纯内存模式或已释放）"
        return chk
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        chk.add(Finding("LEASE_MALFORMED", "error", f"租约文件不可解析：{exc}"))
        return chk
    if not isinstance(doc, dict):
        chk.add(Finding("LEASE_MALFORMED", "error", "租约文件顶层不是对象"))
        return chk
    owner = doc.get("owner")
    epoch = doc.get("epoch")
    expires = doc.get("expires_at")
    if not isinstance(owner, str) or not owner:
        chk.add(Finding("LEASE_MISSING_OWNER", "error", "租约缺少 owner"))
    if not isinstance(epoch, int):
        chk.add(Finding("LEASE_MISSING_EPOCH", "error", "租约缺少整数 epoch"))
    if not isinstance(expires, int):
        chk.add(Finding("LEASE_MISSING_EXPIRY", "error", "租约缺少整数 expires_at"))
    if isinstance(expires, int) and expires <= 0:
        chk.add(Finding("LEASE_EXPIRED", "warn", "租约已过期（等待接管）",
                        invariant="I3", locator={"expires_at": expires}))
    if wal is not None and isinstance(epoch, int):
        durable = _read_int(Path(str(wal) + ".epoch"))
        if durable is not None and durable > epoch:
            chk.add(Finding("LEASE_EPOCH_BEHIND", "error",
                            f"租约 epoch={epoch} 落后于 WAL epoch={durable}"
                            "（旧写者未被 fence）",
                            invariant="I3", locator={"lease_epoch": epoch,
                                                     "wal_epoch": durable}))
    chk.summary = f"owner={owner!r} epoch={epoch!r} expires_at={expires!r}"
    return chk


# ---------------------------------------------------------------- 收据链
def _scope(payload: dict) -> str:
    run_id = payload.get("run_id") if isinstance(payload, dict) else ""
    sub = payload.get("logical_subgoal_id", "") if isinstance(payload, dict) else ""
    return f"{run_id}|{sub}"


def check_receipts(records: list[dict], secret: str) -> Check:
    """按 scope 重建收据链（I13）：断链 / 重复 id / 畸形 payload / 签名。"""
    chk = Check("receipt.chain")
    by_scope: dict[str, list] = {}
    seen_ids: dict[str, int] = {}
    count = 0
    for rec in records:
        if rec.get("kind") != "RECEIPT_ISSUED":
            continue
        payload = rec.get("payload") or {}
        try:
            receipt = Receipt.from_dict(payload)
        except ReceiptMalformed as exc:
            chk.add(Finding("RECEIPT_MALFORMED", "error", f"{exc}",
                            invariant="I13", locator={"seq": rec.get("seq")}))
            continue
        count += 1
        if receipt.receipt_id in seen_ids:
            chk.add(Finding("RECEIPT_DUPLICATE_ID", "error",
                            f"receipt_id={receipt.receipt_id!r} 重复",
                            invariant="I13",
                            locator={"seq": rec.get("seq"),
                                     "first_seq": seen_ids[receipt.receipt_id]}))
        else:
            seen_ids[receipt.receipt_id] = rec.get("seq", 0)
        by_scope.setdefault(_scope(payload), []).append(receipt)
    for scope, receipts in sorted(by_scope.items()):
        result = verify_chain_detailed(receipts, secret)
        if result is False:
            chk.add(Finding("RECEIPT_CHAIN_BROKEN", "error",
                            f"scope={scope} 链断裂或签名不符（{len(receipts)} 条）",
                            invariant="I13", locator={"scope": scope}))
        elif result is None:
            chk.add(Finding("RECEIPT_UNVERIFIED", "warn",
                            f"scope={scope} 链连续但未签名/未提供 secret",
                            invariant="I13", locator={"scope": scope}))
    chk.summary = f"receipts={count} scopes={len(by_scope)}"
    if count == 0 and chk.status == "pass":
        chk.status = "skip"
        chk.summary = "无 RECEIPT_ISSUED 事件"
    return chk


# ---------------------------------------------------------------- 资源版本
def check_versions(records: list[dict]) -> Check:
    """资源版本流（I2）：首版 = 1，之后严格 +1；出现回退/跳跃即 fail。"""
    chk = Check("resource.versions")
    pairs: dict[str, list[tuple[int, int]]] = {}   # resource -> [(seq, version)]
    for rec in records:
        if rec.get("kind") not in ("RESOURCE_COMMITTED", "WORKSPACE_COMMITTED"):
            continue
        payload = rec.get("payload") or {}
        resource = payload.get("resource") or rec.get("subject")
        version = payload.get("version")
        if not isinstance(resource, str) or not isinstance(version, int):
            chk.add(Finding("RESOURCE_EVENT_MALFORMED", "error",
                            "资源提交事件缺少 resource/version",
                            invariant="I2", locator={"seq": rec.get("seq")}))
            continue
        pairs.setdefault(resource, []).append((rec.get("seq", 0), version))
    for resource, items in sorted(pairs.items()):
        versions = []
        for seq, version in sorted(items):
            if not versions or versions[-1] != version:
                versions.append(version)
        if not versions:
            continue
        if versions[0] != 1:
            chk.add(Finding("RESOURCE_VERSION_START", "error",
                            f"资源 {resource!r} 首版是 {versions[0]}，应为 1",
                            invariant="I2", locator={"resource": resource}))
        for before, after in itertools.pairwise(versions):
            if after != before + 1:
                chk.add(Finding("RESOURCE_VERSION_REGRESSION", "error",
                                f"资源 {resource!r} 版本 {before} → {after}"
                                "（应 +1）",
                                invariant="I2", locator={"resource": resource,
                                                         "before": before,
                                                         "after": after}))
    chk.summary = f"resources={len(pairs)}"
    if not pairs and chk.status == "pass":
        chk.status = "skip"
        chk.summary = "无资源提交事件"
    return chk


# ---------------------------------------------------------------- effect
def check_effects(records: list[dict]) -> Check:
    """幂等键唯一（I7）：同一 idem_key 只允许一个 EFFECT_INTENT。"""
    chk = Check("effect.idem")
    seen: dict[str, int] = {}
    count = 0
    for rec in records:
        if rec.get("kind") != "EFFECT_INTENT":
            continue
        count += 1
        payload = rec.get("payload") or {}
        key = payload.get("idem_key") or rec.get("idempotency_key") or ""
        if not key:
            chk.add(Finding("EFFECT_MISSING_IDEM_KEY", "warn",
                            "EFFECT_INTENT 缺少 idem_key",
                            invariant="I7", locator={"seq": rec.get("seq")}))
            continue
        if key in seen:
            chk.add(Finding("EFFECT_DUPLICATE_INTENT", "error",
                            f"idem_key={key!r} 出现多次（首次 seq={seen[key]}）",
                            invariant="I7",
                            locator={"seq": rec.get("seq"), "first_seq": seen[key]}))
        else:
            seen[key] = rec.get("seq", 0)
    chk.summary = f"intents={count} idem_keys={len(seen)}"
    if count == 0 and chk.status == "pass":
        chk.status = "skip"
        chk.summary = "无 EFFECT_INTENT 事件"
    return chk


# ---------------------------------------------------------------- checkpoint
def check_checkpoint(root: Path) -> Check:
    """checkpoint store 结构：store.json 可解析、refs 不被静默重建、坏 ref 计数。"""
    chk = Check("checkpoint.store")
    meta = root / "store.json"
    refs = root / "refs"
    ref_files = [p for p in refs.rglob("*") if p.is_file()] if refs.is_dir() else []
    corrupt = [p for p in refs.rglob("*.corrupt")] if refs.is_dir() else []
    if not meta.is_file():
        if ref_files:
            chk.add(Finding("STORE_MISSING_WITH_REFS", "error",
                            f"refs 下有 {len(ref_files)} 个文件但 store.json 缺失"
                            "（不得静默重建）"))
        else:
            chk.status = "skip"
            chk.summary = "无 checkpoint store"
            return chk
    else:
        try:
            doc = json.loads(meta.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            chk.add(Finding("STORE_MALFORMED", "error", f"store.json 不可解析：{exc}"))
            doc = None
        if isinstance(doc, dict):
            if not isinstance(doc.get("store_uuid"), str) or not doc["store_uuid"]:
                chk.add(Finding("STORE_MISSING_UUID", "error",
                                "store.json 缺少 store_uuid"))
            if not isinstance(doc.get("next_seq"), int):
                chk.add(Finding("STORE_MISSING_NEXT_SEQ", "warn",
                                "store.json 缺少整数 next_seq"))
    if corrupt:
        chk.add(Finding("STORE_QUARANTINED_REFS", "warn",
                        f"存在 {len(corrupt)} 个 .corrupt 隔离 ref"))
    chk.summary = f"refs={len(ref_files)} corrupt={len(corrupt)}"
    return chk


def _read_json(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return None


def check_checkpoint_envelopes(root: Path) -> Check:
    """逐 ref 校验 header/payload 双 checksum（只读，不构造 store）。"""
    chk = Check("checkpoint.envelopes")
    refs = root / "refs"
    if not refs.is_dir():
        chk.status = "skip"
        chk.summary = "无 checkpoint refs"
        return chk
    count = 0
    for path in sorted(refs.rglob("*.json")):
        if path.name.endswith((".part", ".corrupt")):
            continue
        count += 1
        doc = _read_json(path)
        if not isinstance(doc, dict):
            chk.add(Finding("CHECKPOINT_UNREADABLE", "error",
                            f"{path.name} 不可解析或顶层不是对象",
                            locator={"ref": path.name[:-5]}))
            continue
        for problem in envelope_problems(doc):
            code = ("CHECKPOINT_HEADER_CHECKSUM" if problem == "header_checksum"
                    else "CHECKPOINT_PAYLOAD_CHECKSUM")
            chk.add(Finding(code, "error", f"{path.name}: {problem} 不匹配",
                            locator={"ref": path.name[:-5]}))
    chk.summary = f"refs={count}"
    return chk


def check_aging(root: Path, now_ms: int) -> Check:
    """latest 指针指向的 ADMITTED ACB 是否超过 aging_deadline（只读）。"""
    chk = Check("checkpoint.aging")
    latest = root / "latest"
    if not latest.is_dir():
        chk.status = "skip"
        chk.summary = "无 latest 指针"
        return chk
    seen: set[str] = set()
    for pointer in sorted(latest.glob("*.json")):
        meta = _read_json(pointer)
        ref = meta.get("ref") if isinstance(meta, dict) else None
        if not isinstance(ref, str) or not ref or ref in seen:
            continue
        seen.add(ref)
        try:
            doc = _read_json(ref_path(root, ref))
        except ValueError:
            continue
        payload = doc.get("payload", doc) if isinstance(doc, dict) else None
        try:
            payload = decode(payload)
        except (ValueError, TypeError):
            continue
        if not isinstance(payload, dict) or payload.get("state") != "ADMITTED":
            continue
        if payload.get("needs_readmit") or payload.get("superseded"):
            continue
        deadline = payload.get("aging_deadline")
        if not isinstance(deadline, int) or not (0 < deadline < now_ms):
            continue
        chk.add(Finding(
            "AGING_OVERDUE", "warn",
            f"ACB {payload.get('acb_id', '')} 超过 aging_deadline",
            invariant="I12",
            locator={"ref": ref,
                     "hint": f"检查 tick()/pump()；aging_count="
                             f"{payload.get('aging_count', 0)}"}))
    chk.summary = f"refs={len(seen)}"
    return chk


# ---------------------------------------------------------------- tokens
def check_tokens(path: Path, now_ms: int) -> Check:
    """token 索引：可解析、必填键齐全、过期 token 提示。"""
    chk = Check("tokens.index")
    if not path.is_file():
        chk.status = "skip"
        chk.summary = "无 tokens.json"
        return chk
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        chk.add(Finding("TOKENS_MALFORMED", "error", f"tokens.json 不可解析：{exc}"))
        return chk
    if not isinstance(doc, dict):
        chk.add(Finding("TOKENS_MALFORMED", "error", "tokens.json 顶层不是对象"))
        return chk
    expired = 0
    for token, entry in sorted(doc.items()):
        if not isinstance(entry, dict):
            chk.add(Finding("TOKEN_ENTRY_MALFORMED", "error",
                            f"token={token!r} 的条目不是对象"))
            continue
        if "ref" not in entry:
            chk.add(Finding("TOKEN_MISSING_REF", "error",
                            f"token={token!r} 缺少 ref",
                            locator={"token": token[:12]}))
        deadline = entry.get("deadline_ms")
        if isinstance(deadline, int) and deadline <= now_ms:
            expired += 1
    if expired:
        chk.add(Finding("TOKENS_EXPIRED", "warn",
                        f"{expired} 个 token 已过期（reaper 可能未清理）"))
    chk.summary = f"tokens={len(doc)} expired={expired}"
    return chk


# ---------------------------------------------------------------- 汇总
def diagnose(*, state_dir: Path | None = None, wal: Path | None = None,
             checkpoint_root: Path | None = None, tokens: Path | None = None,
             secret: str = "", strict: bool = False,
             now_ms: int | None = None) -> dict:
    """执行全部检查；返回可 JSON 序列化的报告（纯函数，无副作用）。"""
    root = Path(state_dir) if state_dir else None
    wal_path = Path(wal) if wal else (root / "wal.log" if root else None)
    cp_root = Path(checkpoint_root) if checkpoint_root else root
    tokens_path = Path(tokens) if tokens else (root / "tokens.json" if root else None)
    now = int(now_ms if now_ms is not None else _now_ms())

    checks: list[Check] = []
    stats = {"frames": 0, "records": 0, "receipts": 0, "resources": 0,
             "effects": 0, "refs": 0}
    if wal_path is not None and wal_path.is_file():
        records, integrity, ordering, wal_stats = read_wal(wal_path)
        checks.extend([integrity, ordering])
        stats.update({"frames": wal_stats["frames"],
                      "records": wal_stats["records"]})
        checks.append(check_receipts(records, secret))
        checks.append(check_versions(records))
        checks.append(check_effects(records))
        stats["receipts"] = sum(1 for r in records if r.get("kind") == "RECEIPT_ISSUED")
        stats["effects"] = sum(1 for r in records if r.get("kind") == "EFFECT_INTENT")
        stats["resources"] = len({
            res for r in records
            if r.get("kind") in ("RESOURCE_COMMITTED", "WORKSPACE_COMMITTED")
            for res in [(r.get("payload") or {}).get("resource")]
            if isinstance(res, str) and res})
    else:
        checks.append(Check("wal.integrity", "skip",
                            "未提供/未找到 WAL（纯内存模式）"))
    if root is not None:
        checks.append(check_lease(root, wal_path))
    if cp_root is not None:
        cp = check_checkpoint(cp_root)
        checks.append(cp)
        checks.append(check_checkpoint_envelopes(cp_root))
        checks.append(check_aging(cp_root, now))
        if cp_root.is_dir():
            stats["refs"] = sum(1 for p in (cp_root / "refs").rglob("*")
                                if p.is_file()) if (cp_root / "refs").is_dir() else 0
    if tokens_path is not None:
        checks.append(check_tokens(tokens_path, now))

    findings = [f for chk in checks for f in chk.findings]
    errors = [f for f in findings if f.severity == "error"]
    warns = [f for f in findings if f.severity == "warn"]
    ok = not errors and not (strict and warns)
    return {
        "tool": TOOL,
        "schema_version": SCHEMA_VERSION,
        "ok": ok,
        "strict": strict,
        "root": str(root) if root else None,
        "wal": str(wal_path) if wal_path else None,
        "checks": [chk.as_dict() for chk in checks],
        "findings": [f.as_dict() for f in findings],
        "stats": dict(stats, finger_sha256=_fingerprint(
            wal_path, tokens_path, cp_root)),
    }


def _fingerprint(wal: Path | None, tokens: Path | None,
                 root: Path | None) -> str:
    """状态指纹（只读取、阻塞 0 改）：用于「报告前后状态一致」的自证。"""
    h = hashlib.sha256()
    for path in (wal, tokens, (root / "store.json") if root else None,
                 (root / "writer_lease.json") if root else None):
        if path is not None and Path(path).is_file():
            h.update(Path(path).read_bytes())
    return h.hexdigest()


def _now_ms() -> int:
    import time

    return int(time.time() * 1000)


# ---------------------------------------------------------------- CLI
def _render_text(report: dict) -> str:
    lines = [(f"{report['tool']} schema={report['schema_version']} "
              f"ok={report['ok']}")]
    errors = sum(1 for f in report["findings"] if f["severity"] == "error")
    warns = sum(1 for f in report["findings"] if f["severity"] == "warn")
    lines.append(f"checks={len(report['checks'])} findings={len(report['findings'])} "
                 f"(error={errors} warn={warns})")
    for chk in report["checks"]:
        lines.append(f"  [{chk['status']}] {chk['id']}  {chk['summary']}")
        for f in chk["findings"]:
            inv = f" ({f['invariant']})" if f["invariant"] else ""
            lines.append(f"      {f['severity'].upper()} {f['code']}{inv}: "
                         f"{f['message']}")
    stats = report["stats"]
    lines.append("stats: " + ", ".join(f"{k}={v}" for k, v in stats.items()))
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog=TOOL, description="RSC 只读诊断：WAL / 收据链 / 版本 / effect / 租约")
    ap.add_argument("--state-dir", default=None,
                    help="状态目录（自动发现 wal.log / tokens.json / store.json）")
    ap.add_argument("--wal", default=None, help="WAL 文件路径")
    ap.add_argument("--checkpoint-root", default=None, help="checkpoint store 目录")
    ap.add_argument("--tokens", default=None, help="token 索引路径")
    ap.add_argument("--secret", default=None,
                    help="收据验签 secret（默认读 HARNESS_AUTH_SECRET）")
    ap.add_argument("--json", action="store_true", help="输出 JSON")
    ap.add_argument("--strict", action="store_true", help="warn 也按失败退出")
    args = ap.parse_args(argv)

    if not any((args.state_dir, args.wal, args.checkpoint_root, args.tokens)):
        ap.print_usage(sys.stderr)
        print(f"{TOOL}: 至少需要 --state-dir / --wal / --checkpoint-root / "
              "--tokens 之一", file=sys.stderr)
        return 2
    if args.state_dir and not Path(args.state_dir).is_dir():
        print(f"{TOOL}: --state-dir 不存在或不是目录：{args.state_dir}",
              file=sys.stderr)
        return 2
    if args.wal and not Path(args.wal).is_file():
        print(f"{TOOL}: --wal 不存在：{args.wal}", file=sys.stderr)
        return 2
    if args.tokens and not Path(args.tokens).is_file():
        print(f"{TOOL}: --tokens 不存在：{args.tokens}", file=sys.stderr)
        return 2

    secret = args.secret if args.secret is not None \
        else os.environ.get("HARNESS_AUTH_SECRET", "")
    report = diagnose(state_dir=Path(args.state_dir) if args.state_dir else None,
                      wal=Path(args.wal) if args.wal else None,
                      checkpoint_root=(Path(args.checkpoint_root)
                                       if args.checkpoint_root else None),
                      tokens=Path(args.tokens) if args.tokens else None,
                      secret=secret, strict=args.strict)
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
          if args.json else _render_text(report))
    return 0 if report["ok"] else 1


if __name__ == "__main__":                    # pragma: no cover
    sys.exit(main())
