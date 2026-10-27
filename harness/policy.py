"""PolicyEngine —— 文档 §4.2 / §9.4（CapabilityToken 能力强制）。

D13：**deny-by-default**。未签发 token 且无 `default_token` → 拒绝。
测试夹具可显式签发通配 token（`{"*"}`）以覆盖非安全用例；
安全用例必须用**不装 `default_token`** 的 Harness 断言拒绝。

v1.1（S5 第二波，§5.9）：
- `mint(...)`：委托 `auth.Signer` 签发 HMAC token（**token 显式传入**）；
- `check_capability(principal, token, tool, path, *, network=None, data_class=None)`：
  五参签名 + 四维（tool / path / network / data_class）；
- 保留 `issue`（旧内存 token，测试/兼容）。
"""

from __future__ import annotations

from typing import Optional

from .auth import MissingSecret


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

    `_signer_factory`：返回 `auth.Signer`（可抛 `MissingSecret`）的零参可调用对象。
    """

    def __init__(self, default_allow: bool = False,
                 default_token: Optional[CapabilityToken] = None,
                 *, signer_factory=None) -> None:
        self._tokens: dict[str, object] = {}
        self.default_allow = default_allow
        self.default_token = default_token
        self._signer_factory = signer_factory

    # ---- 签发 ------------------------------------------------------
    def mint(self, agent_id: str, *, tools: Optional[set[str]] = None,
             paths: Optional[set[str]] = None,
             network: Optional[set[str]] = None,
             data_class: str = "*", exp=None) -> str:
        """签发 HMAC token（§5.9：委托 `auth`）。缺失 signer → `MissingSecret`。"""
        signer = self._signer_factory() if self._signer_factory else None
        if signer is None:
            raise MissingSecret("PolicyEngine 未配置 signer（无法 mint）")
        token = signer.mint(agent_id, tools=tools, paths=paths,
                            network=network, data_class=data_class, exp=exp)
        self._tokens[agent_id] = token
        return token

    def issue(self, arg, *, tools: Optional[set[str]] = None,
              paths: Optional[set[str]] = None,
              network: Optional[set[str]] = None,
              data_class: str = "*",
              allow_legacy: bool = False) -> CapabilityToken:
        """两种用法：

        - `issue(agent_id, tools=...)`        ← 新签名（生产推荐）
        - `issue(token, allow_legacy=True)`   ← 旧签名，仅测试/兼容
        """
        if isinstance(arg, CapabilityToken):
            if not allow_legacy:
                raise ValueError(
                    "issue(token) 仅允许 allow_legacy=True（防自证身份）")
            token = arg
        else:
            token = CapabilityToken(arg, tools=tools, paths=paths,
                                    network=network, data_class=data_class)
        self._tokens[token.agent_id] = token
        return token

    # ---- 能力判定（五参 + 四维）------------------------------------
    def _resolve(self, token):
        """把 token 归一为 `CapabilityToken`；无法解析 → `None`。"""
        if token is None:
            return None
        if isinstance(token, CapabilityToken):
            return token
        if isinstance(token, str):
            signer = self._signer_factory() if self._signer_factory else None
            if signer is None:
                return None
            payload = signer.verify(token)
            if not payload or payload is False:
                return None
            return CapabilityToken(
                payload.get("agent_id", ""),
                tools=set(payload.get("tools") or []),
                paths=set(payload.get("paths") or []),
                network=set(payload.get("network") or []),
                data_class=payload.get("data_class") or "*")
        return None

    def check_capability(self, principal: str, token, tool: str = "",
                         path: str = "", *, network=None, data_class=None) -> bool:
        """五参签名 + 四维（§5.9）。

        **兼容垫片**：旧三参调用 `check_capability(agent_id, tool, path)`
        （第 2 个位置参数为不含 `.` 的字符串）仍按旧语义判定。
        """
        if isinstance(token, str) and "." not in token:
            legacy_tool, legacy_path = token, tool
            tok = self._tokens.get(principal)
            if tok is None:
                tok = self.default_token
            if tok is None:
                return self.default_allow
            return (tok.allows_tool(legacy_tool) and tok.allows_path(legacy_path))
        resolved = self._resolve(token)
        if resolved is None:
            return False
        # 身份绑定：token 的 agent_id 必须与 principal 一致（"*" 仅测试夹具通配）
        if resolved.agent_id not in (principal, "*"):
            return False
        if not resolved.allows_tool(tool):
            return False
        if path and not resolved.allows_path(path):
            return False
        if network and not resolved.allows_network(network):
            return False
        if data_class and not resolved.allows_data_class(data_class):
            return False
        return True

    def allows_network(self, token, host: str) -> bool:
        resolved = self._resolve(token)
        return resolved is not None and resolved.allows_network(host)

    def allows_data_class(self, token, data_class: str) -> bool:
        resolved = self._resolve(token)
        return resolved is not None and resolved.allows_data_class(data_class)

    def token(self, agent_id: str):
        return self._tokens.get(agent_id)
