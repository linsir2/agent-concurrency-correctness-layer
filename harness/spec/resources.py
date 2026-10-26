"""资源模型（文档 §18.1）。

规范层只定义「资源类型 + 版本语义」，具体存储由实现决定。
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class ResourceKind(str, Enum):
    FILE = "FILE"
    DIR = "DIR"
    SYMBOL = "SYMBOL"
    DB_ROW = "DB_ROW"
    API = "API"
    CONTRACT = "CONTRACT"
    INVARIANT = "INVARIANT"
    TYPE_GRAPH = "TYPE_GRAPH"
    BUDGET_NODE = "BUDGET_NODE"
    EFFECT = "EFFECT"


# 版本语义：单调版本号 vs 内容 hash
MONOTONIC = {ResourceKind.FILE, ResourceKind.DIR, ResourceKind.DB_ROW, ResourceKind.BUDGET_NODE}
CONTENT_ADDRESSED = {ResourceKind.SYMBOL, ResourceKind.API, ResourceKind.CONTRACT,
                     ResourceKind.INVARIANT, ResourceKind.TYPE_GRAPH}
IDEMPOTENCY_KEYED = {ResourceKind.EFFECT}

RESOURCE_KINDS = set(ResourceKind)


@dataclass(frozen=True)
class ResourceSpec:
    """footprint 中的一个资源声明（文档 §5.2② / §7.3）。"""
    kind: ResourceKind
    path: str
    base_version: int | None = None
    merge_policy: str = "replace"        # replace | json-merge | grow-only
    logical_deps: tuple = ()

    def version_semantics(self) -> str:
        if self.kind in MONOTONIC:
            return "monotonic"
        if self.kind in CONTENT_ADDRESSED:
            return "content_hash"
        if self.kind in IDEMPOTENCY_KEYED:
            return "idempotency_key"
        return "monotonic"


def resource_kind(path: str) -> ResourceKind:
    """按路径推断资源类型（参考实现；生产实现应由 Planner 显式声明）。"""
    if path.endswith((".ts", ".js", ".py", ".go", ".java")):
        return ResourceKind.FILE
    if path.endswith((".json", ".yaml", ".toml")):
        return ResourceKind.FILE
    if path.endswith(".sql"):
        return ResourceKind.DB_ROW
    return ResourceKind.FILE
