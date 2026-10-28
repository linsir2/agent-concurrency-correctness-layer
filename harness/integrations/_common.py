"""S23：适配器共享件（业务键、拒绝类型、Z3 读回）。

- **业务键**统一为 `tool:<sha256(canonical args)[:16]>`；门面 `apply` 会再叠加
  `{tenant}|{agent_id}` 命名空间（见 `facade.py::_apply_locked`）。
- **不依赖任何宿主框架**：本模块只涉及 stdlib，供 crewai / openai_agents / ag2 /
  http_sidecar 复用。
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any


class RscDenied(RuntimeError):
    """适配器层硬拒绝（宿主可见、应终止当前动作）。"""


def canonical_args(args: Any) -> str:
    """参数规范化：键排序 + 稳定分隔，保证同参重试命中同一业务键。"""
    return json.dumps(args, sort_keys=True, ensure_ascii=False, default=str,
                      separators=(",", ":"))


def tool_key(tool_name: str, args: Any) -> str:
    """`工具名 + 规范化参数` 的短哈希（16 位十六进制）。"""
    raw = f"{tool_name}\x1f{canonical_args(args)}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def safe_rel(worktree: str, rel: str) -> str | None:
    """把 `rel` 解析到 worktree 内；逃逸（`..`/绝对路径/符号链接）→ None。"""
    try:
        root = os.path.realpath(worktree)
        target = os.path.realpath(os.path.join(worktree, rel))
    except (OSError, ValueError):
        return None
    if target != root and not target.startswith(root + os.sep):
        return None
    return target


def read_writes(worktree: str | None, paths) -> list[tuple[str, bytes]] | None:
    """从 Z3 读回 declare 的产物；缺失/逃逸/IO 失败 → None（调用方 fail-closed）。"""
    if not worktree:
        return None
    out: list[tuple[str, bytes]] = []
    for rel in paths:
        target = safe_rel(worktree, rel)
        if target is None:
            return None
        try:
            out.append((str(rel), Path(target).read_bytes()))
        except (OSError, ValueError):
            return None
    return out
