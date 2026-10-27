"""内容寻址存储（方案 §4.2 / §5.1）。

`content_hash` 一律是 **blob hash**（`sha256(canonical_bytes)`），
不再用 `repr(content)` —— 后者对 dict 键序敏感，会让同一内容得到不同 hash。
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Protocol

from .atomic import atomic_write
from .codec import to_bytes


_BYTES_PREFIX = b"\x00BYTES\x00"   # 魔数前缀：JSON 输出不会以 \x00 开头


class BlobCorrupt(Exception):
    """blob 内容哈希失配 / 缺失（P2：坏盘或位翻转）。

    `FileBlobStore.get` 必须校验 `sha256(data) == blob_hash`；旧实现直接
    `read_bytes`、`_project_event` 只 `exists()` → 静默返回被篡改内容。
    不继承 `ValueError`：避免被 `_decode` 的宽 `except` 吞掉降级成裸 bytes。
    """

    def __init__(self, blob_hash: str = "", reason: str = "") -> None:
        self.blob_hash = blob_hash
        self.reason = reason
        super().__init__(f"blob 损坏：{blob_hash} ({reason})")


def canonical_bytes(obj: Any) -> bytes:
    """确定性序列化：键排序 + 紧凑分隔符 + 不转义非 ASCII。

    `bytes` 加魔数前缀，避免 `b'{"a":1}'` 被下游 `json.loads` 误解码成 dict。

    非 bytes 内容走**严格 codec**（S9 §2.4.6）：`set` / `tuple` / 自定义对象 /
    `NaN` / `Inf` 一律 `TypeError`，不再用 `default=str` 静默降级。
    """
    if isinstance(obj, bytes):
        return _BYTES_PREFIX + obj
    if isinstance(obj, bytearray):
        return _BYTES_PREFIX + bytes(obj)
    return to_bytes(obj)


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class BlobStore(Protocol):
    def put(self, data: bytes) -> str:
        ...

    def get(self, blob_hash: str) -> bytes:
        ...

    def exists(self, blob_hash: str) -> bool:
        ...

    def link(self, blob_hash: str, target_path: str | Path) -> None:
        ...


class InMemoryBlobStore:
    def __init__(self) -> None:
        self._blobs: dict[str, bytes] = {}

    def put(self, data: bytes) -> str:
        h = sha256_hex(data)
        self._blobs.setdefault(h, data)
        return h

    def get(self, blob_hash: str) -> bytes:
        return self._blobs[blob_hash]

    def exists(self, blob_hash: str) -> bool:
        return blob_hash in self._blobs

    def link(self, blob_hash: str, target_path: str | Path) -> None:
        atomic_write(target_path, self._blobs[blob_hash])


class FileBlobStore:
    """`root/aa/bb/<hash>`，已存在则跳过（内容寻址天然幂等）。"""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def _path(self, blob_hash: str) -> Path:
        return self.root / blob_hash[:2] / blob_hash[2:4] / blob_hash

    def put(self, data: bytes) -> str:
        h = sha256_hex(data)
        path = self._path(h)
        if not path.exists():
            atomic_write(path, data)
        return h

    def get(self, blob_hash: str) -> bytes:
        data = self._path(blob_hash).read_bytes()
        if sha256_hex(data) != blob_hash:        # P2：坏盘/位翻转必须报错
            raise BlobCorrupt(blob_hash, "content_hash mismatch")
        return data

    def exists(self, blob_hash: str) -> bool:
        return self._path(blob_hash).exists()

    def link(self, blob_hash: str, target_path: str | Path) -> None:
        atomic_write(target_path, self.get(blob_hash))
