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
from typing import Iterable, Mapping, Optional

DEFAULT_BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
DEFAULT_MODEL = "qwen3.7-flash"
ENV_KEY = "DASHSCOPE_API_KEY"
ENV_BASE_URL = "DASHSCOPE_BASE_URL"
ENV_MODEL = "DASHSCOPE_MODEL"


class MissingAPIKey(RuntimeError):
    """未配置 DASHSCOPE_API_KEY。"""


class LLMError(RuntimeError):
    """上游返回错误或网络失败（已重试）。"""


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
        return cls(
            api_key=key,
            base_url=(source.get(ENV_BASE_URL) or DEFAULT_BASE_URL).rstrip("/"),
            model=source.get(ENV_MODEL) or DEFAULT_MODEL,
        )

    def __repr__(self) -> str:  # 避免密钥进日志
        return (f"LLMConfig(model={self.model!r}, base_url={self.base_url!r}, "
                f"api_key='***', timeout_s={self.timeout_s})")


def _post(url: str, payload: dict, headers: dict, timeout_s: float) -> dict:
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        return json.loads(resp.read().decode("utf-8"))


def chat(messages: Iterable[Mapping[str, str]], *,
         config: Optional[LLMConfig] = None,
         temperature: float = 0.0,
         max_tokens: Optional[int] = None) -> str:
    """调用 chat/completions，返回首条回复文本。

    仅用于阶段二的真实模型接入；阶段一 demo 默认走 scripted fake。
    """
    cfg = config or LLMConfig.from_env()
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
            data = _post(url, payload, headers, cfg.timeout_s)
            return data["choices"][0]["message"]["content"]
        except urllib.error.HTTPError as exc:          # 4xx 不重试（除 429）
            last_error = exc
            if exc.code != 429 and 400 <= exc.code < 500:
                break
        except (urllib.error.URLError, TimeoutError, KeyError) as exc:
            last_error = exc
        if attempt < cfg.max_retries:
            time.sleep(cfg.backoff_s * (2 ** attempt))
    raise LLMError(f"调用失败（model={cfg.model}）：{last_error}")
