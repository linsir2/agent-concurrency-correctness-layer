"""收据格式（文档 §18.3）：hash-chain + signature，可离线验证。"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Optional

GENESIS_HASH = "0" * 16


@dataclass
class Receipt:
    receipt_id: str
    run_id: str
    task_id: str
    logical_subgoal_id: str
    resource: str
    version_before: int
    version_after: int
    effect_id: Optional[str]
    idem_key: Optional[str]
    compensation_ref: Optional[str]
    prev_receipt_hash: str
    signature: str = ""
    schema_version: int = 1

    def payload(self) -> str:
        return json.dumps({
            "receipt_id": self.receipt_id, "run_id": self.run_id,
            "task_id": self.task_id, "logical_subgoal_id": self.logical_subgoal_id,
            "resource": self.resource, "version_before": self.version_before,
            "version_after": self.version_after, "effect_id": self.effect_id,
            "idem_key": self.idem_key, "compensation_ref": self.compensation_ref,
            "prev_receipt_hash": self.prev_receipt_hash,
            "schema_version": self.schema_version,
        }, sort_keys=True)


def _hash(payload: str) -> str:
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def receipt_hash(receipt: Receipt) -> str:
    """收据在 hash-chain 中的链接值。"""
    return _hash(receipt.payload())


def build_receipt(*, receipt_id: str, run_id: str, task_id: str,
                  logical_subgoal_id: str, resource: str,
                  version_before: int, version_after: int,
                  effect_id: str | None = None, idem_key: str | None = None,
                  compensation_ref: str | None = None,
                  prev_receipt_hash: str = GENESIS_HASH,
                  secret: str = "") -> Receipt:
    """构建收据并签名（签名 = hash(payload + secret)）。"""
    r = Receipt(
        receipt_id=receipt_id, run_id=run_id, task_id=task_id,
        logical_subgoal_id=logical_subgoal_id, resource=resource,
        version_before=version_before, version_after=version_after,
        effect_id=effect_id, idem_key=idem_key,
        compensation_ref=compensation_ref, prev_receipt_hash=prev_receipt_hash,
    )
    r.signature = _hash(r.payload() + secret)
    return r


def verify_chain(receipts: list[Receipt], secret: str = "") -> bool:
    """离线验证 hash-chain + 签名。"""
    prev = GENESIS_HASH
    for r in receipts:
        if r.prev_receipt_hash != prev:
            return False
        if r.signature != _hash(r.payload() + secret):
            return False
        prev = _hash(r.payload())
    return True
