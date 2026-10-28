"""严格 checkpoint codec（S9 §2.4.5 / §2.4.6）。

规则（fail-fast，禁止静默降级）：

- 支持 `None` / `bool` / `int` / 有限 `float` / `str` / `bytes` / `dict` / `list`；
- `DELETE` 哨兵编码为 `{"__type__": "DELETE"}`，解码**必须**返回 `rebase.DELETE`
  模块级单例（`contents[r] is DELETE` 依赖身份比较）；
- `bytes` 以 base64 tag 编码；
- `set` / `tuple` / 自定义对象 / `NaN` / `Inf` → `TypeError`；
- 用户数据中递归出现保留键 `__type__` → `ValueError`（避免被误解码为哨兵）。
"""

from __future__ import annotations

import base64
import json
import math
from typing import Any

from ..rebase import DELETE

_DELETE_TAG = {"__type__": "DELETE"}
_BYTES_TYPE = "bytes"


class CodecError(ValueError):
    """codec 解码期错误（畸形 tag / 非有限浮点）。"""


def _encode(v: Any) -> Any:
    if v is DELETE:
        return dict(_DELETE_TAG)
    if v is None or isinstance(v, (bool, str)):
        return v
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        if not math.isfinite(v):
            raise TypeError("NaN/Inf 不可序列化")
        return v
    if isinstance(v, bytes):
        return {"__type__": _BYTES_TYPE,
                "data": base64.b64encode(v).decode("ascii")}
    if isinstance(v, list):
        return [_encode(x) for x in v]
    if isinstance(v, dict):
        if "__type__" in v:
            raise ValueError("用户数据不得包含保留键 __type__")
        out: dict = {}
        for key, value in v.items():
            if not isinstance(key, str):
                raise TypeError(f"dict 键必须是 str：{type(key).__name__}")
            out[key] = _encode(value)
        return out
    raise TypeError(f"不支持的类型：{type(v).__name__}")


def encode(v: Any) -> Any:
    """把 Python 值编码为可 JSON 序列化的严格结构。"""
    return _encode(v)


def _decode(v: Any) -> Any:
    if isinstance(v, dict):
        if v == _DELETE_TAG:
            return DELETE
        tag = v.get("__type__")
        if tag is not None:
            if (tag == _BYTES_TYPE and set(v) == {"__type__", "data"}
                    and isinstance(v["data"], str)):
                try:
                    return base64.b64decode(v["data"].encode("ascii"), validate=True)
                except Exception as exc:
                    raise CodecError("bytes tag 非法") from exc
            raise CodecError(f"未知 __type__ tag：{tag!r}")
        return {key: _decode(value) for key, value in v.items()}
    if isinstance(v, list):
        return [_decode(x) for x in v]
    if isinstance(v, float) and not math.isfinite(v):
        raise CodecError("非有限浮点不可解码")
    return v


def decode(v: Any) -> Any:
    """把编码结构还原为 Python 值（`DELETE` 返模块级单例）。"""
    return _decode(v)


def dumps(encoded: Any) -> str:
    """严格 JSON 序列化（已编码结构；禁止 NaN/Inf）。"""
    return json.dumps(encoded, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False)


def loads(text: str) -> Any:
    """严格 JSON 反序列化（拒绝 NaN/Infinity 常量）。"""
    def _reject(value: str):
        raise CodecError(f"非有限浮点常量不可解码：{value}")

    return json.loads(text, parse_constant=_reject)


def to_bytes(v: Any) -> bytes:
    """`encode` + 严格 JSON → UTF-8 bytes（确定性）。"""
    return dumps(encode(v)).encode("utf-8")
