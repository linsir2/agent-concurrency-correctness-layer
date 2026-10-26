"""LLM 配置：密钥只从环境变量读，且绝不入库。

对应「密钥只进 .env、不入仓库」的约束：
- 配置层只认 `DASHSCOPE_API_KEY`；
- 默认模型 `qwen3.7-flash`；
- `repr` 不泄露密钥；
- 仓库内（除 .env 外）不得出现形如真实密钥的字符串。
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from harness.llm import DEFAULT_BASE_URL, DEFAULT_MODEL, LLMConfig, MissingAPIKey

ROOT = Path(__file__).resolve().parents[1]
# 真实密钥形态：sk- 后跟 >=20 位十六进制（本文件里的字面量不满足，不会自匹配）
SECRET_PATTERN = re.compile("sk" + "-" + "[0-9a-f]{20,}")


def test_config_from_env_uses_qwen_flash_default():
    cfg = LLMConfig.from_env({"DASHSCOPE_API_KEY": "dummy"})
    assert cfg.model == DEFAULT_MODEL == "qwen3.7-flash"
    assert cfg.base_url == DEFAULT_BASE_URL
    assert cfg.api_key == "dummy"


def test_env_overrides_base_url_and_model():
    cfg = LLMConfig.from_env({
        "DASHSCOPE_API_KEY": "dummy",
        "DASHSCOPE_BASE_URL": "https://example.invalid/v1/",
        "DASHSCOPE_MODEL": "other-model",
    })
    assert cfg.base_url == "https://example.invalid/v1"
    assert cfg.model == "other-model"


def test_missing_key_raises():
    with pytest.raises(MissingAPIKey):
        LLMConfig.from_env({})


def test_repr_hides_key():
    cfg = LLMConfig.from_env({"DASHSCOPE_API_KEY": "dummy-secret-value"})
    assert "dummy-secret-value" not in repr(cfg)
    assert "***" in repr(cfg)


def test_no_api_key_in_repository():
    """仓库里除 .env 外不得出现真实密钥（防止误提交）。"""
    offenders = []
    skip_dirs = {".git", "__pycache__", ".pytest_cache", ".hypothesis", ".uploads", "node_modules"}
    for path in ROOT.rglob("*"):
        if not path.is_file() or path.name == ".env":
            continue
        if skip_dirs & set(path.parts):
            continue
        if path.suffix not in {".py", ".md", ".toml", ".txt", ".yaml", ".yml", ".json", ".example", ""}:
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        if SECRET_PATTERN.search(text):
            offenders.append(str(path.relative_to(ROOT)))
    assert offenders == [], f"疑似密钥出现在仓库文件中: {offenders}"
