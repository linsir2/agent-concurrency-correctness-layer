"""协调协议（文档 §18.2）：GRANT / WAIT / DIE。

- `GRANT`：准入成功，持有租约
- `WAIT`：契约/读-写冲突，准入拒绝（fail-fast）
- `DIE`：同资源写-写，准入放行，提交层 CAS 裁决

**关键**：`DIE` 不是「终止」，而是「放行至提交层裁决」。
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Verdict(str, Enum):
    GRANT = "GRANT"
    WAIT = "WAIT"
    DIE = "DIE"


@dataclass(frozen=True)
class Grant:
    verdict: Verdict = Verdict.GRANT
    reason: str = ""


@dataclass(frozen=True)
class Wait:
    verdict: Verdict = Verdict.WAIT
    reason: str = ""


@dataclass(frozen=True)
class Die:
    verdict: Verdict = Verdict.DIE
    reason: str = ""


def decide(*, contract_conflict: bool = False,
           read_write_conflict: bool = False,
           write_write_overlap: bool = False):
    """准入判定（文档 §5.2⑤）。

    只拦「契约/不变量」与「读-写方向」冲突；**同资源写-写放行**（DIE）。
    """
    if contract_conflict:
        return Wait(reason="CONTRACT_CONFLICT")
    if read_write_conflict:
        return Wait(reason="READ_WRITE_CONFLICT")
    if write_write_overlap:
        return Die(reason="WRITE_WRITE_OVERLAP")
    return Grant()
