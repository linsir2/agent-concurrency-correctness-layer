"""内容寻址存储（方案 §4.2 / §5.1）。

`content_hash` 一律是 **blob hash**（`sha256(canonical_bytes)`），
不再用 `repr(content)` —— 后者对 dict 键序敏感，会让同一内容得到不同 hash。
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Protocol

from .atomic import atomic_write


_BYTES_PREFIX = b"\x00BYTES\x00"   # 魔数前缀：JSON 输出不会以 \x00 开头


def canonical_bytes(obj: Any) -> bytes:
    """确定性序列化：键排序 + 紧凑分隔符 + 不转义非 ASCII。

    `bytes` 加魔数前缀，避免 `b'{"a":1}'` 被下游 `json.loads` 误解码成 dict。
    """
    if isinstance(obj, bytes):
        return _BYTES_PREFIX + obj
    if isinstance(obj, bytearray):
        return _BYTES_PREFIX + bytes(obj)
    return json.dumps(obj, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, default=str).encode("utf-8")


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
        return self._path(blob_hash).read_bytes()

    def exists(self, blob_hash: str) -> bool:
        return self._path(blob_hash).exists()

    def link(self, blob_hash: str, target_path: str | Path) -> None:
        atomic_write(target_path, self.get(blob_hash))
