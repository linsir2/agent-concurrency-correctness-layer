"""收据格式（文档 §18.3 / S9 §2.3）：hash-chain + HMAC 签名，可离线验证。

v1.1（S9b）变更：
- 签名改 HMAC-SHA256 **全量 64 hex**（原 `sha256(payload+secret)[:16]` 只有 64bit），
  secret 与 token 域分离（`HMAC(auth_secret, "receipt-v1")`）；
- `secret` 为空 → `signature == ""` 且 `signed is False`（**未签名**，不是篡改）；
- `verify_chain` 保持 `bool` 语义；新增 `verify_chain_detailed` 三态：
  `True`（链连续 + 签名全对）/ `None`（链连续但存在未签名）/ `False`（断链 / 签名不符）；
- 空列表返回 `False`（v1.0 返回 `True` 是 fail-open）；重复 `receipt_id` 返回 `False`；
- 新增 `to_dict` / `from_dict`（事件 payload 序列化）。

`hash-chain` 链接值仍为 `sha256(payload)[:16]`（v1.0 兼容，不改变既有链值）。
"""

from __future__ import annotations

import hashlib
import hmac
import json
from dataclasses import dataclass
from typing import Optional

GENESIS_HASH = "0" * 16

# 域分离标签：收据签名密钥 = HMAC(auth_secret, RECEIPT_SECRET_LABEL)
RECEIPT_SECRET_LABEL = b"receipt-v1"

# 必填字段：缺失即视为畸形收据（P2：不得让单条坏事件打挂 Harness 启动/验签）
_REQUIRED_FIELDS = ("receipt_id", "run_id", "task_id")


class ReceiptMalformed(ValueError):
    """收据 payload 缺必填字段或字段非法（P2）。

    单条 `RECEIPT_ISSUED` 事件若缺 `run_id`/`task_id`/`receipt_id`，
    旧实现 `from_dict` 直接 `KeyError` → `Harness.__init__` 的
    `_rebuild_receipts` / `receipts_of` / `verify_receipts` 全部抛异常，
    一条坏事件即可让 Harness 无法启动。改为显式异常 + 调用方容错。
    """

    def __init__(self, missing: tuple[str, ...] = ()) -> None:
        self.missing = tuple(missing)
        super().__init__(f"畸形收据 payload，缺失字段：{self.missing}")


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

    @property
    def signed(self) -> bool:
        """是否已签名（由 `signature` 是否为空推导，**不进入 payload**）。"""
        return bool(self.signature)

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

    def to_dict(self) -> dict:
        """序列化为事件 payload（含 signature；`signed` 为派生字段）。"""
        return {
            "receipt_id": self.receipt_id, "run_id": self.run_id,
            "task_id": self.task_id, "logical_subgoal_id": self.logical_subgoal_id,
            "resource": self.resource, "version_before": self.version_before,
            "version_after": self.version_after, "effect_id": self.effect_id,
            "idem_key": self.idem_key, "compensation_ref": self.compensation_ref,
            "prev_receipt_hash": self.prev_receipt_hash,
            "signature": self.signature,
            "signed": self.signed,
            "schema_version": self.schema_version,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Receipt":
        """从事件 payload 反序列化（`signed` 由 signature 推导，忽略该键）。

        P2：容错校验必填字段（`receipt_id`/`run_id`/`task_id`）；缺失 →
        `ReceiptMalformed`（不再裸 `KeyError`），由调用方决定跳过/隔离。
        """
        if not isinstance(data, dict):
            raise ReceiptMalformed(("payload 不是 dict",))
        missing = tuple(f for f in _REQUIRED_FIELDS
                        if not isinstance(data.get(f), str) or not data.get(f))
        if missing:
            raise ReceiptMalformed(missing)
        return cls(
            receipt_id=data["receipt_id"],
            run_id=data["run_id"],
            task_id=data["task_id"],
            logical_subgoal_id=data.get("logical_subgoal_id", ""),
            resource=data.get("resource", ""),
            version_before=int(data.get("version_before", 0)),
            version_after=int(data.get("version_after", 0)),
            effect_id=data.get("effect_id"),
            idem_key=data.get("idem_key"),
            compensation_ref=data.get("compensation_ref"),
            prev_receipt_hash=data.get("prev_receipt_hash", GENESIS_HASH),
            signature=data.get("signature", ""),
            schema_version=int(data.get("schema_version", 1)),
        )


def _hash(payload: str) -> str:
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def receipt_hash(receipt: Receipt) -> str:
    """收据在 hash-chain 中的链接值（v1.0 兼容：截断 16 hex）。"""
    return _hash(receipt.payload())


def _receipt_secret(secret: str) -> bytes:
    """收据签名密钥 = `HMAC(auth_secret, "receipt-v1")`（与 token 域分离）。"""
    return hmac.new(secret.encode("utf-8"), RECEIPT_SECRET_LABEL,
                    hashlib.sha256).digest()


def _sign(payload: str, secret: str) -> str:
    """HMAC-SHA256 **全量 64 hex**；secret 为空 → `""`（未签名）。"""
    if not secret:
        return ""
    return hmac.new(_receipt_secret(secret), payload.encode("utf-8"),
                    hashlib.sha256).hexdigest()


def build_receipt(*, receipt_id: str, run_id: str, task_id: str,
                  logical_subgoal_id: str, resource: str,
                  version_before: int, version_after: int,
                  effect_id: str | None = None, idem_key: str | None = None,
                  compensation_ref: str | None = None,
                  prev_receipt_hash: str = GENESIS_HASH,
                  secret: str = "") -> Receipt:
    """构建收据并签名（`secret` 为空 → 未签名，`signature == ""`）。"""
    r = Receipt(
        receipt_id=receipt_id, run_id=run_id, task_id=task_id,
        logical_subgoal_id=logical_subgoal_id, resource=resource,
        version_before=version_before, version_after=version_after,
        effect_id=effect_id, idem_key=idem_key,
        compensation_ref=compensation_ref, prev_receipt_hash=prev_receipt_hash,
    )
    r.signature = _sign(r.payload(), secret)
    return r


def verify_chain_detailed(receipts: list[Receipt], secret: str = "") -> bool | None:
    """离线验证 hash-chain + 签名，三态返回。

    - `True`  = 链连续 **且** 签名全对；
    - `None`  = 链连续但存在**未签名**条目（或未提供 secret，无法判定签名）；
    - `False` = 空列表 / 重复 `receipt_id` / 断链 / 签名不符。
    """
    if not receipts:
        return False
    seen: set[str] = set()
    prev = GENESIS_HASH
    unsigned = False
    for r in receipts:
        if r.receipt_id in seen:
            return False
        seen.add(r.receipt_id)
        if r.prev_receipt_hash != prev:
            return False
        if not r.signature:
            unsigned = True
        elif secret and not hmac.compare_digest(r.signature, _sign(r.payload(), secret)):
            return False
        prev = receipt_hash(r)
    if unsigned or not secret:
        return None
    return True


def verify_chain(receipts: list[Receipt], secret: str = "") -> bool:
    """离线验证 hash-chain + 签名（`bool` 语义）。

    - `secret` 为空 → 只校验链连续（`True`），调用方可从 `signature` 判断是否未签名；
    - `secret` 非空 → `True` 仅当链连续 **且** 签名全对（未签名 / 不匹配 → `False`）。
    """
    result = verify_chain_detailed(receipts, secret)
    if not secret:
        return result is not False
    return result is True
