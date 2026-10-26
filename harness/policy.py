"""PolicyEngine —— 文档 §4.2 / §9.4（CapabilityToken 能力强制）。

D13：**deny-by-default**。未签发 token 且无 `default_token` → 拒绝。
测试夹具可显式签发通配 token（`{"*"}`）以覆盖非安全用例；
安全用例必须用**不装 `default_token`** 的 Harness 断言拒绝。
"""

from __future__ import annotations

from typing import Optional


class CapabilityToken:
    """四维 scope（tool / path / network / data_class），支持 `"*"` 通配。"""

    def __init__(self, agent_id: str = "", tools: Optional[set[str]] = None,
                 paths: Optional[set[str]] = None,
                 network: Optional[set[str]] = None,
                 data_class: str = "*") -> None:
        self.agent_id = agent_id
        self.tools = set(tools or ())
        self.paths = set(paths or ())
        self.network = set(network or ())
        self.data_class = data_class

    def allows_tool(self, tool: str) -> bool:
        return "*" in self.tools or tool in self.tools

    def allows_path(self, path: str) -> bool:
        if "*" in self.paths:
            return True
        return any(path.startswith(p) for p in self.paths)

    def allows_network(self, host: str) -> bool:
        return "*" in self.network or host in self.network

    def allows_data_class(self, data_class: str) -> bool:
        return self.data_class == "*" or self.data_class == data_class


class PolicyEngine:
    """能力强制点（B4：检查点在 gateway 强制执行，模型不可绕过）。

    `default_allow=False` → **deny-by-default**（默认，生产口径）。
    `default_token` → 未单独签发的 agent 走这个兜底 token（测试夹具用）。
    """

    def __init__(self, default_allow: bool = False,
                 default_token: Optional[CapabilityToken] = None) -> None:
        self._tokens: dict[str, CapabilityToken] = {}
        self.default_allow = default_allow
        self.default_token = default_token

    def issue(self, token: CapabilityToken) -> None:
        if not token.agent_id:
            raise ValueError("issue() 需要 agent_id")
        self._tokens[token.agent_id] = token

    def check_capability(self, agent_id: str, tool: str, path: str = "") -> bool:
        token = self._tokens.get(agent_id)
        if token is None:
            token = self.default_token
        if token is None:
            return self.default_allow
        return token.allows_tool(tool) and token.allows_path(path)

    def token(self, agent_id: str) -> Optional[CapabilityToken]:
        return self._tokens.get(agent_id)
