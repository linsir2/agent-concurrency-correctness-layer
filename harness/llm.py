"""LLM 客户端（DashScope OpenAI 兼容模式）。

**密钥只从环境变量读取**，绝不写入代码、文档或仓库：
- `DASHSCOPE_API_KEY`（必填）
- `DASHSCOPE_BASE_URL`（默认 OpenAI 兼容端点）
- `DASHSCOPE_MODEL`（默认 qwen3.7-flash）

零第三方依赖（stdlib `urllib`）。导入本模块不需要密钥；只有真正调用时才校验。
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping, Optional
from urllib.parse import urlsplit

from .egress import EgressDenied, EgressGuard, build_opener

DEFAULT_BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
DEFAULT_MODEL = "qwen3.7-flash"
ENV_KEY = "DASHSCOPE_API_KEY"
ENV_BASE_URL = "DASHSCOPE_BASE_URL"
ENV_MODEL = "DASHSCOPE_MODEL"

# 端点白名单（§7.1：防把 Authorization 与资源内容 POST 到攻击者端点）
ALLOWED_BASE_URL_HOSTS = frozenset({"dashscope.aliyuncs.com"})
REPO_ROOT = Path(__file__).resolve().parents[1]


class MissingAPIKey(RuntimeError):
    """未配置 DASHSCOPE_API_KEY。"""


class LLMError(RuntimeError):
    """上游返回错误或网络失败（已重试）。"""


class DisallowedBaseURL(ValueError):
    """base_url 主机不在白名单内（防密钥外泄）。"""


def validate_base_url(base_url: str) -> None:
    """校验 base_url 主机 ∈ 白名单（只允许 `dashscope.aliyuncs.com`）。"""
    host = urlsplit(base_url).hostname or ""
    if host not in ALLOWED_BASE_URL_HOSTS:
        raise DisallowedBaseURL(
            f"base_url 主机 {host!r} 不在白名单 {sorted(ALLOWED_BASE_URL_HOSTS)} 中"
            f"（防密钥/内容外泄到非官方端点）")


def load_env_file(path: Optional[str] = None) -> dict:
    """只读加载 `.env`（默认仓库根 `.env`）到 `os.environ`，**不覆盖**已有变量。

    返回实际解析出的键值（无论是否已存在）。文件不存在 → 返回空 dict。
    """
    env_path = Path(path) if path is not None else REPO_ROOT / ".env"
    loaded: dict = {}
    if not env_path.exists():
        return loaded
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].lstrip()
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        if not key:
            continue
        loaded[key] = value
        os.environ.setdefault(key, value)
    return loaded


@dataclass(frozen=True)
class LLMConfig:
    api_key: str
    base_url: str = DEFAULT_BASE_URL
    model: str = DEFAULT_MODEL
    timeout_s: float = 60.0
    max_retries: int = 2
    backoff_s: float = 0.5

    @classmethod
    def from_env(cls, env: Optional[Mapping[str, str]] = None) -> "LLMConfig":
        source = os.environ if env is None else env
        key = (source.get(ENV_KEY) or "").strip()
        if not key:
            raise MissingAPIKey(
                f"未设置 {ENV_KEY}：请在 .env 或环境变量中配置（不要写进代码）")
        base = (source.get(ENV_BASE_URL) or DEFAULT_BASE_URL).rstrip("/")
        validate_base_url(base)
        return cls(
            api_key=key,
            base_url=base,
            model=source.get(ENV_MODEL) or DEFAULT_MODEL,
        )

    def __repr__(self) -> str:  # 避免密钥进日志
        return (f"LLMConfig(model={self.model!r}, base_url={self.base_url!r}, "
                f"api_key='***', timeout_s={self.timeout_s})")


def _post(url: str, payload: dict, headers: dict, timeout_s: float, *,
          egress: Optional[EgressGuard] = None,
          opener: Optional[urllib.request.OpenerDirector] = None) -> dict:
    """真实网络分支：**必须**先过 egress 强制点，再走钉住 + 禁重定向的 opener。

    - `ensure()` 在入口调用：无强制点 → `EgressUnavailable`（拒绝出站，fail-closed）；
    - `build_opener()` 用 `pinned_connection` 只解析一次（防 DNS rebinding / SSRF）；
    - `3xx` 显式转成 `EgressDenied`（`NoRedirect` 已在 opener 内拒绝，这里是兜底）。
    """
    guard = egress if egress is not None else _default_egress()
    guard.ensure()
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    # `opener` 仅供受限环境（必须经代理才能出站）显式注入：默认 None →
    # 钉住 + 禁代理的 `build_opener()`（生产语义，不被本参数改变）。
    active = opener if opener is not None else build_opener()
    try:
        with active.open(req, timeout=timeout_s) as resp:
            return json.loads(resp.read().decode("utf-8"))  # pragma: no cover
    except urllib.error.HTTPError as exc:
        if 300 <= int(exc.code) < 400:
            raise EgressDenied(
                f"重定向禁止二次解析：{exc.code} → {exc.headers.get('Location')!r}") from exc
        raise


_default_guard: Optional[EgressGuard] = None


def _default_egress() -> EgressGuard:
    """未显式注入 guard 时的默认值：`required=None` → 出站时 fail-closed。"""
    global _default_guard
    if _default_guard is None:
        _default_guard = EgressGuard()
    return _default_guard


def configure_egress(guard: Optional[EgressGuard]) -> None:
    """注入/覆盖模块级默认 guard（`Harness` 会注入自己的 guard）。"""
    global _default_guard
    _default_guard = guard


class LLMClient:
    """把 egress guard 与配置绑在一起的客户端（`Harness.llm` 使用）。"""

    def __init__(self, config: Optional[LLMConfig] = None, *,
                 egress: Optional[EgressGuard] = None,
                 opener: Optional[urllib.request.OpenerDirector] = None) -> None:
        self.config = config
        self.egress = egress if egress is not None else _default_egress()
        # 见 `_post`：默认 None = 钉住 + 禁代理；仅受限环境显式覆盖。
        self.opener = opener

    def chat(self, messages: Iterable[Mapping[str, str]], *,
             temperature: float = 0.0,
             max_tokens: Optional[int] = None) -> str:
        cfg = self.config or LLMConfig.from_env()
        validate_base_url(cfg.base_url)
        url = f"{cfg.base_url}/chat/completions"
        payload = {
            "model": cfg.model,
            "messages": list(messages),
            "temperature": temperature,
        }
        if max_tokens is not None:
            payload["max_tokens"] = max_tokens
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {cfg.api_key}",
        }

        last_error: Optional[Exception] = None
        for attempt in range(cfg.max_retries + 1):
            try:
                data = _post(url, payload, headers, cfg.timeout_s,
                             egress=self.egress, opener=self.opener)
                return data["choices"][0]["message"]["content"]
            except EgressDenied:
                raise                       # 出站被策略拒绝：不得重试、不得降级
            except urllib.error.HTTPError as exc:          # 4xx 不重试（除 429）
                last_error = exc
                if exc.code != 429 and 400 <= exc.code < 500:
                    break
            except (urllib.error.URLError, TimeoutError, KeyError) as exc:
                last_error = exc
            if attempt < cfg.max_retries:
                time.sleep(cfg.backoff_s * (2 ** attempt))
        raise LLMError(f"调用失败（model={cfg.model}）：{last_error}")


def chat(messages: Iterable[Mapping[str, str]], *,
         config: Optional[LLMConfig] = None,
         temperature: float = 0.0,
         max_tokens: Optional[int] = None,
         egress: Optional[EgressGuard] = None,
         opener: Optional[urllib.request.OpenerDirector] = None) -> str:
    """调用 chat/completions，返回首条回复文本。

    仅用于阶段二的真实模型接入；阶段一 demo 默认走 scripted fake。
    """
    return LLMClient(config, egress=egress, opener=opener).chat(
        messages, temperature=temperature, max_tokens=max_tokens)
