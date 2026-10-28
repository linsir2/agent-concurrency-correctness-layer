"""S24-11：Agent/工具**定义版本化**（content hash 钉住，未知版本拒绝）。

**要解决什么**：`plan`/ACB 只记录 footprint 与 plan_version，不记录「这个任务
用的是哪一版 agent 定义（prompt / 工具 schema / 模型参数）」。定义热更新后，
在途 run 继续跑、重放也说不清当时用的是哪版 —— 审计与复现都不可信。

本模块给出最小闭环：

- `DefinitionRegistry.register(definition, name=..., version=...)` → `DefinitionRef`
  （`content_hash = sha256(canonical_bytes(definition))`，与 blob 同约定）；
- 同名同版本**异内容** → `DefinitionConflict`（不静默覆盖）；
- `check(ref)` 三态错误码：`DEFINITION_MALFORMED` / `DEFINITION_UNKNOWN` /
  `DEFINITION_HASH_MISMATCH`，通过返回空串；
- 校验点（fail-closed）：`Planner.plan`、`Dispatcher.try_admit`（占预算/槽位前）、
  `Harness.commit_batch`（写共享态前）。**不跟随 latest、不自动迁移。**

**默认关闭**：没有 `definition` 键时所有路径字节不变；注册表缺失但 ref 存在
→ `DEFINITION_REGISTRY_MISSING`（拒绝，绝不当作「没关系」）。
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from .storage.blob import canonical_bytes


class DefinitionConflict(ValueError):
    """同名同版本被注册了不同内容（必须显式升版本）。"""


@dataclass(frozen=True)
class DefinitionRef:
    name: str
    version: str
    content_hash: str

    def as_dict(self) -> dict:
        return {"name": self.name, "version": self.version,
                "content_hash": self.content_hash}

    @classmethod
    def from_dict(cls, data) -> DefinitionRef:
        if isinstance(data, DefinitionRef):
            return data
        if not isinstance(data, dict):
            raise TypeError("definition ref 必须是对象")
        name = data.get("name")
        version = data.get("version")
        content_hash = data.get("content_hash")
        if not all(isinstance(x, str) and x for x in (name, version, content_hash)):
            raise ValueError("definition ref 缺 name/version/content_hash")
        return cls(name=name, version=version, content_hash=content_hash)


class DefinitionRegistry:
    """内存定义注册表（可注入；进程重启需重新注册 —— v1 边界）。"""

    def __init__(self) -> None:
        self._refs: dict[tuple[str, str], DefinitionRef] = {}
        self._defs: dict[tuple[str, str], object] = {}
        self._order: list[tuple[str, str]] = []

    def register(self, definition, *, name: str, version: str) -> DefinitionRef:
        name, version = str(name or ""), str(version or "")
        if not name or not version:
            raise ValueError("name/version 不能为空")
        digest = hashlib.sha256(canonical_bytes(definition)).hexdigest()
        key = (name, version)
        existing = self._refs.get(key)
        if existing is not None:
            if existing.content_hash != digest:
                raise DefinitionConflict(
                    f"{name}@{version} 已注册且内容不同（"
                    f"{existing.content_hash[:12]} != {digest[:12]}）；请升版本")
            return existing
        ref = DefinitionRef(name=name, version=version, content_hash=digest)
        self._refs[key] = ref
        self._defs[key] = definition
        self._order.append(key)
        return ref

    # ---- 查询 -----------------------------------------------------
    def check(self, ref) -> str:
        """返回空串 = 通过；否则为可执行的错误码（fail-closed）。"""
        try:
            parsed = DefinitionRef.from_dict(ref)
        except (ValueError, TypeError):
            return "DEFINITION_MALFORMED"
        entry = self._refs.get((parsed.name, parsed.version))
        if entry is None:
            return "DEFINITION_UNKNOWN"
        if entry.content_hash != parsed.content_hash:
            return "DEFINITION_HASH_MISMATCH"
        return ""

    def resolve(self, ref):
        parsed = DefinitionRef.from_dict(ref)
        return self._defs.get((parsed.name, parsed.version))

    def latest(self, name: str) -> DefinitionRef | None:
        for key in reversed(self._order):
            if key[0] == name:
                return self._refs[key]
        return None

    def has(self, name: str, version: str) -> bool:
        return (str(name), str(version)) in self._refs
