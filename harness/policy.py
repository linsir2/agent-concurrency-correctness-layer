"""PolicyEngine —— 文档 §4.2 / §9.4（CapabilityToken 能力强制）。

本实现是可插拔的：默认策略允许一切；生产实现可接 RBAC/数据分级。
"""

from __future__ import annotations


class CapabilityToken:
    def __init__(self, agent_id: str, tools: set[str], paths: set[str],
                 network: set[str] | None = None, data_class: str = "public") -> None:
        self.agent_id = agent_id
        self.tools = tools
        self.paths = paths
        self.network = network or set()
        self.data_class = data_class

    def allows_tool(self, tool: str) -> bool:
        return tool in self.tools

    def allows_path(self, path: str) -> bool:
        return any(path.startswith(p) for p in self.paths)


class PolicyEngine:
    """能力强制点（B4：检查点在 gateway 强制执行，模型不可绕过）。

    `default_allow=False` → **deny-by-default**（生产推荐）。
    `default_allow=True`  → 参考实现模式（未签发 token 时放行，便于测试）。
    """

    def __init__(self, default_allow: bool = False) -> None:
        self._tokens: dict[str, CapabilityToken] = {}
        self.default_allow = default_allow

    def issue(self, token: CapabilityToken) -> None:
        self._tokens[token.agent_id] = token

    def check_capability(self, agent_id: str, tool: str, path: str = "") -> bool:
        token = self._tokens.get(agent_id)
        if token is None:
            return self.default_allow
        return token.allows_tool(tool) and token.allows_path(path)

    def token(self, agent_id: str) -> CapabilityToken | None:
        return self._tokens.get(agent_id)
