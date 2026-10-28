"""S15 D1：**Ingress 幂等**（用户入口，`docs/s15-protocol-and-production.md` §3.1）。

语义对齐 IETF `Idempotency-Key`（draft-ietf-httpapi-idempotency-key-header-02）：

| 情形 | 结果 |
|---|---|
| key 为空 | `INVALID_INPUT`（IETF 要求拒绝空值） |
| 同 key + **同** payload 指纹 | `OK` 且 `replayed=True`（**不新建 Run**） |
| 同 key + **不同** 指纹 | `CONFLICT`（对齐 IETF 的 409） |
| 新 key | `OK`，建 Run，写 `INGRESS_ACCEPTED`（指纹随事件持久化） |

**与 `commit_key` 的区别**（不合并）：
- `commit_key` 去重**提交**（同一变更不重复落库，保护 I1/I5）
- `idempotency_key` 去重**请求**（同一用户请求不重复建 Run，保护成本与审计）

**持久化**：指纹写进 `INGRESS_ACCEPTED` 事件 → 跨进程/重启仍可识别，
不依赖进程内 dict（对比：内存态去重会在重启后重复受理）。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from .types import EventKind, Status


def fingerprint(payload: Any) -> str:
    """稳定指纹：键排序 + 紧凑分隔符 + UTF-8。

    **必须**稳定：同一内容但字段顺序不同 → 同一指纹（否则幂等会误判为冲突）。
    """
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class IngressResult:
    """入口结果（`replayed=True` 表示命中幂等记录、未真的新建）。"""
    status: Status
    run_id: str = ""
    replayed: bool = False
    reason: str = ""


class Ingress:
    """用户入口：幂等建 Run。"""

    def __init__(self, harness) -> None:
        self.h = harness

    # ---- 读取面：从事件流重建（持久真相，不用内存 dict）----
    def _records(self) -> list[dict]:
        """`[(idempotency_key, fingerprint, run_id)]`，按事件顺序。"""
        out: list[dict] = []
        for ev in self.h.event_log.all():
            if ev.kind is not EventKind.INGRESS_ACCEPTED:
                continue
            payload = ev.payload or {}
            out.append({
                "key": str(payload.get("idempotency_key", "")),
                "fingerprint": str(payload.get("fingerprint", "")),
                "run_id": str(payload.get("run_id", "")),
            })
        return out

    def lookup(self, idempotency_key: str, payload: Any) -> IngressResult:
        """只查不写：判断该 key 已知的受理结果。"""
        fp = fingerprint(payload)
        for rec in self._records():
            if rec["key"] != idempotency_key:
                continue
            if rec["fingerprint"] != fp:
                return IngressResult(
                    Status.CONFLICT, run_id=rec["run_id"],
                    reason="idempotency_key_reused_with_different_payload")
            return IngressResult(Status.OK, run_id=rec["run_id"], replayed=True)
        return IngressResult(Status.OK, reason="new")

    def submit(self, *, idempotency_key: str, plan: dict,
               payload: dict | None = None) -> IngressResult:
        """幂等受理一个 Run 创建请求。

        `payload` 是参与指纹的内容（默认取 `plan`）；调用方可显式传入以便
        只对「业务实质」做指纹（例如忽略无关的展示字段）。
        """
        key = str(idempotency_key or "")
        if not key:
            # IETF：空 key 必须拒绝（否则等于没有幂等保护）
            return IngressResult(Status.INVALID_INPUT,
                                 reason="idempotency_key_required")

        material = payload if payload is not None else plan
        prior = self.lookup(key, material)
        if prior.reason == "idempotency_key_reused_with_different_payload":
            return prior                      # CONFLICT：同 key 异指纹
        if prior.replayed:
            return prior                      # 重放：不新建

        run_id = str(plan.get("run_id", "") or "")
        if not run_id:
            return IngressResult(Status.INVALID_INPUT, reason="plan_missing_run_id")

        # 新 key：登记（写入事件 → 持久真相）+ 建 Run
        self.h.event_log.append(
            EventKind.INGRESS_ACCEPTED, run_id,
            {"idempotency_key": key, "fingerprint": fingerprint(material),
             "run_id": run_id},
            run_id=run_id)
        self.h.plans[run_id] = plan
        return IngressResult(Status.OK, run_id=run_id, replayed=False,
                             reason="accepted")
