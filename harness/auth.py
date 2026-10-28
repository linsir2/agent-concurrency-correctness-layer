"""Signer —— 文档 `s5-recovery-and-journals.md` v1.1 §5.9（K-10）。

HMAC-SHA256 签名 token（`mint`/`verify`/`tamper`）+ `jti` 去重。

**目标已明确降级（§5.9）**：HMAC 只能防「没有 secret 的第三方」篡改/重放。
威胁模型是 agent 与 harness **同进程** —— agent 可读 `os.environ` 或直接
`Signer(secret).mint(...)`，验签必然通过。**同进程内无法靠算法解决自证**，
真隔离归阶段二（独立信任域 / enclave）。

secret 从环境变量 `HARNESS_AUTH_SECRET` 注入；缺失抛 `MissingSecret`（fail-closed）。
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import threading
import time
import uuid
from typing import Any


class MissingSecret(Exception):
    """未注入签名密钥 → fail-closed。"""


def _b64encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64decode(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


class Signer:
    """HMAC-SHA256 签名器。

    `verify` 返回值约定（对齐 T28）：
    - 验签通过且未重放 → `payload`（含 `agent_id` 与四维 scope）；
    - 篡改 / 解析失败 / 过期 → `False`；
    - `jti` 重放 → `None`。
    """

    def __init__(self, secret: str | bytes) -> None:
        if secret is None or secret == "":
            raise MissingSecret("缺少签名密钥（HARNESS_AUTH_SECRET）")
        self._secret = secret.encode("utf-8") if isinstance(secret, str) else bytes(secret)
        self._seen: dict[str, float] = {}
        self._lock = threading.Lock()

    @classmethod
    def from_env(cls, env: str = "HARNESS_AUTH_SECRET") -> Signer:
        """从环境变量读取 secret；缺失 → `MissingSecret`（fail-closed）。"""
        return cls(os.environ.get(env, ""))

    # ---- 签发 ------------------------------------------------------
    def mint(self, agent_id: str, *, tools=None, paths=None,
             network=None, data_class: str = "public", exp=None) -> str:
        now = int(time.time())
        payload = {
            "agent_id": agent_id,
            "tools": sorted(tools or []),
            "paths": sorted(paths or []),
            "network": sorted(network or []),
            "data_class": data_class or "public",
            "exp": int(exp) if exp else now + 3600,
            "jti": uuid.uuid4().hex,
        }
        body = _b64encode(json.dumps(payload, sort_keys=True,
                                     separators=(",", ":")).encode("utf-8"))
        sig = _b64encode(hmac.new(self._secret, body.encode("ascii"),
                                  hashlib.sha256).digest())
        return f"{body}.{sig}"

    # ---- 验签 ------------------------------------------------------
    def verify(self, token: Any) -> Any:
        if not isinstance(token, str):
            return False
        try:
            body, sig = token.split(".")
            payload = json.loads(_b64decode(body))
            expected = hmac.new(self._secret, body.encode("ascii"),
                                hashlib.sha256).digest()
            if not hmac.compare_digest(_b64decode(sig), expected):
                return False
        except Exception:  # noqa: BLE001  # 解析失败按验签失败处理
            return False
        # fail-closed：`exp` 缺失 / 非法 / 已过期一律拒绝
        exp = payload.get("exp")
        if exp is None:
            return False
        try:
            exp_value = float(exp)
        except (TypeError, ValueError):
            return False
        if time.time() >= exp_value:
            return False
        jti = payload.get("jti")
        if not jti:
            return False
        with self._lock:
            if jti in self._seen:
                return None                       # 重放
            self._seen[jti] = exp_value
        return payload

    # ---- 测试辅助 --------------------------------------------------
    def tamper(self, token: str) -> str:
        """翻转签名首字节 → 解码后的签名**必然**改变（验签必然失败）。

        不能只改末位 base64 字符：末位含未使用填充位，可能解码出相同字节
        （这正是 T28 偶发假通过的根因）。
        """
        try:
            body, sig = token.split(".")
        except ValueError:
            return token + "x"
        try:
            raw = _b64decode(sig)
        except Exception:  # noqa: BLE001
            return token + "x"
        if not raw:
            return token + "x"
        mutated = bytes([raw[0] ^ 0xFF]) + raw[1:]
        return f"{body}.{_b64encode(mutated)}"

    def gc_seen(self, now: float | None = None) -> int:
        """清理已过期的 `jti`（TTL 至 `exp`）。"""
        now = time.time() if now is None else now
        with self._lock:
            expired = [k for k, v in self._seen.items() if v <= now]
            for k in expired:
                del self._seen[k]
        return len(expired)
