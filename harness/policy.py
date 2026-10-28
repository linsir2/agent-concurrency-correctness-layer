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

import ipaddress
import time
from dataclasses import dataclass, field
from typing import Optional

from .auth import MissingSecret


# S11a §2.1：data_class 分级序（低 → 高）；token 的等级即「可访问的最高等级」。
_DATA_CLASS_LEVELS = {
    "public": 0,
    "internal": 1,
    "confidential": 2,
    "restricted": 3,
}


class CapabilityAttenuationError(ValueError):
    """S13c §2.4 P0-7：能力衰减遇到**未知 data_class** → fail-closed。

    专用异常（不复用 `ValueError`）：调用方需要区分「参数非法」与
    「等级序不可判 → 拒绝派生」，后者绝不能退化为字面量比较。
    """


@dataclass
class Scope:
    """子任务请求的能力范围（S13c §2.4）。`data_class=None` → 继承父等级。"""
    tools: set[str] = field(default_factory=set)
    paths: set[str] = field(default_factory=set)
    network: set[str] = field(default_factory=set)
    data_class: Optional[str] = None


def _lower_class(a: str, b: Optional[str]) -> str:
    """按分级序取**更低**的 data_class（S13c P0-7）。

    ❌ `min("public", "confidential") == "confidential"`（字面量比较 → 越权）；
    ✅ 用 `_DATA_CLASS_LEVELS` 比较。`"*"`：父 `*` → 取 requested；requested
    `*`（或 None）→ 取父。未知等级 → `CapabilityAttenuationError`（fail-closed）。
    """
    if a == "*":
        return b if b else "*"
    if b is None or b == "*":
        return a
    la = _DATA_CLASS_LEVELS.get(a)
    lb = _DATA_CLASS_LEVELS.get(b)
    if la is None or lb is None:
        raise CapabilityAttenuationError(
            f"未知 data_class: {a!r}/{b!r}（无法按分级序比较 → 拒绝派生）")
    return a if la <= lb else b


def _intersect_scope(parent_set, requested_set) -> set[str]:
    """通配感知交集（S13c §2.4）：父 `{"*"}` → requested；子 `{"*"}` → 父；否则 `&`。"""
    parent = set(parent_set or ())
    requested = set(requested_set or ())
    if "*" in parent:
        return requested
    if "*" in requested:
        return parent
    return parent & requested


def _token_contains(child: "CapabilityToken", parent: "CapabilityToken") -> bool:
    """`child` 每一维是否 ⊆ `parent`（父 `"*"` 视作全集）。"""
    for dim in ("tools", "paths", "network"):
        p = set(getattr(parent, dim, ()) or ())
        c = set(getattr(child, dim, ()) or ())
        if "*" in p:
            continue
        if "*" in c or not c <= p:
            return False
    pd = getattr(parent, "data_class", "public")
    cd = getattr(child, "data_class", "public")
    if pd == "*":
        return True
    pc = _DATA_CLASS_LEVELS.get(pd)
    cc = _DATA_CLASS_LEVELS.get(cd)
    if pc is None or cc is None:
        return cd == pd
    return cc <= pc


def attenuate_token(parent: "CapabilityToken", requested: Scope,
                    child_id: str) -> "CapabilityToken":
    """S13c §2.4 P0-7：子能力 = parent ∩ requested；`data_class` 按分级序取低。

    这是**唯一**给子签发能力（派生）的路径；`policy.issue` 的合同保持不变
    （`test_i4_capability_production_no_self_issue` 依赖旧签名/自证拒绝）。
    """
    if not isinstance(child_id, str) or not child_id:
        raise ValueError("attenuate_token: child_id 必须是非空字符串")
    if isinstance(requested, dict):
        requested = Scope(
            tools=set(requested.get("tools") or ()),
            paths=set(requested.get("paths") or ()),
            network=set(requested.get("network") or ()),
            data_class=requested.get("data_class"))
    token = CapabilityToken(
        child_id,
        tools=_intersect_scope(parent.tools, requested.tools),
        paths=_intersect_scope(parent.paths, requested.paths),
        network=_intersect_scope(parent.network, requested.network),
        data_class=_lower_class(parent.data_class, requested.data_class),
        parent_id=parent.agent_id,
    )
    return token


def _split_host_port(entry: str) -> tuple[str, Optional[int]]:
    """拆分 `host[:port]`（兼容 IPv6 方括号写法 `[::1]:443`）。"""
    text = (entry or "").strip()
    if not text:
        return "", None
    if text.startswith("["):
        end = text.find("]")
        if end != -1:
            host = text[1:end]
            rest = text[end + 1:]
            if rest.startswith(":") and rest[1:].isdigit():
                return host, int(rest[1:])
            return host, None
        return text, None
    if text.count(":") == 1:
        host, _, port = text.partition(":")
        if port.isdigit():
            return host, int(port)
    return text, None


def _ip_in_cidr(host: str, cidr: str) -> bool:
    """`host` 是否为 `cidr` 内地址（非 IP / 非法 CIDR → False）。"""
    try:
        return ipaddress.ip_address(host) in ipaddress.ip_network(cidr, strict=False)
    except ValueError:
        return False


class CapabilityToken:
    """四维 scope（tool / path / network / data_class），支持 `"*"` 通配。"""

    def __init__(self, agent_id: str = "", tools: Optional[set[str]] = None,
                 paths: Optional[set[str]] = None,
                 network: Optional[set[str]] = None,
                 data_class: str = "public",
                 parent_id: str = "") -> None:
        self.agent_id = agent_id
        self.tools = set(tools or ())
        self.paths = set(paths or ())
        self.network = set(network or ())
        self.data_class = data_class
        # S13c §2.4：派生 token 的父身份（`attenuate_token` 写入；根 token 为空）。
        self.parent_id = parent_id

    def allows_tool(self, tool: str) -> bool:
        return "*" in self.tools or tool in self.tools

    def allows_path(self, path: str) -> bool:
        if "*" in self.paths:
            return True
        return any(path.startswith(p) for p in self.paths)

    def allows_network(self, host: str) -> bool:
        """S11a §2.1：精确 host / 通配 `*.example.com` / CIDR / 端口。"""
        if "*" in self.network:
            return True
        target_host, target_port = _split_host_port(host)
        target_host = target_host.lower()
        for pattern in self.network:
            if pattern == "*":
                return True
            p_host, p_port = _split_host_port(pattern)
            if p_port is not None and p_port != target_port:
                continue
            p_host = p_host.lower()
            if "/" in p_host:
                if target_host and _ip_in_cidr(target_host, p_host):
                    return True
                continue
            if p_host.startswith("*."):
                if target_host.endswith(p_host[1:]):
                    return True
                continue
            if p_host == target_host:
                return True
        return False

    def allows_data_class(self, data_class: str) -> bool:
        """S11a §2.1：分级序（public < internal < confidential < restricted）。

        未知等级（如旧测试的 `"secret"`）→ 退回字符串相等（向后兼容）。
        """
        if self.data_class == "*":
            return True
        token_level = _DATA_CLASS_LEVELS.get(self.data_class)
        requested = _DATA_CLASS_LEVELS.get(data_class)
        if token_level is None or requested is None:
            return self.data_class == data_class
        return requested <= token_level


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
        # 终审 1-2：同一 token 值只验签一次。`Signer.verify` 的 jti 去重是**消息
        # 重放**语义（第二次返回 None），若每次都调用，多动作任务第二个动作就被拒。
        # 缓存的是「已验签 payload → CapabilityToken」，仍按 token 自带 exp 过期。
        self._verified: dict[str, tuple[float, CapabilityToken]] = {}
        self.default_allow = default_allow
        self.default_token = default_token
        self._signer_factory = signer_factory

    # ---- 签发 ------------------------------------------------------
    def mint(self, agent_id: str, *, tools: Optional[set[str]] = None,
             paths: Optional[set[str]] = None,
             network: Optional[set[str]] = None,
             data_class: str = "public", exp=None) -> str:
        """签发 HMAC token（§5.9：委托 `auth`）。缺失 signer → `MissingSecret`。

        终审 2-1：`data_class` 默认**最低等级**（`public`），不再默认 `"*"`
        （通配会让未显式声明的 token 直接可读 `restricted`，属 fail-open）。
        """
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
              data_class: str = "public",
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
    def resolve_token(self, token) -> Optional[CapabilityToken]:
        """把 token 归一为 `CapabilityToken`；无法解析 / 过期 → `None`。

        gateway / 准入必须走本入口（终审 1-1：只判 `token is not None` 会让
        任意垃圾字符串通过）。
        """
        return self._resolve(token)

    def _resolve(self, token):
        """把 token 归一为 `CapabilityToken`；无法解析 → `None`。

        S13c §2.4：**派生 token 必须可追溯到父 token**（`parent_id` 非空时，
        父 token 必须已签发且子 scope ⊆ 父 scope）。否则子把 `acb.capability`
        换成更宽的 token 会绕过 attenuation。

        终审 1-2：字符串 token 按**值**缓存已验签结果（同一 token 复用），
        避免 `Signer.verify` 的 jti 去重把第二次判定当成消息重放。
        """
        resolved = self._resolve_raw(token)
        if resolved is None:
            return None
        if not self._traceable(resolved):
            return None
        return resolved

    def _resolve_raw(self, token):
        """验签/归一（不做 parent 追溯；供 `_traceable` 内部避免递归）。"""
        if token is None:
            return None
        if isinstance(token, CapabilityToken):
            return token
        if isinstance(token, str):
            cached = self._verified.get(token)
            if cached is not None:
                exp, resolved = cached
                if exp > time.time():
                    return resolved
                self._verified.pop(token, None)
                return None
            signer = None
            if self._signer_factory is not None:
                try:
                    signer = self._signer_factory()
                except MissingSecret:
                    return None          # 无 secret → 无法验签 → 拒绝（不抛给 gate）
            if signer is None:
                return None
            payload = signer.verify(token)
            if not payload or payload is False:
                return None
            try:
                exp_value = float(payload.get("exp"))
            except (TypeError, ValueError):
                return None
            resolved = CapabilityToken(
                payload.get("agent_id", ""),
                tools=set(payload.get("tools") or []),
                paths=set(payload.get("paths") or []),
                network=set(payload.get("network") or []),
                data_class=payload.get("data_class") or "public",
                parent_id=str(payload.get("parent_id") or ""))
            if exp_value > time.time():
                self._cache_verified(token, exp_value, resolved)
            return resolved
        return None

    def _traceable(self, token: CapabilityToken, depth: int = 0) -> bool:
        """`parent_id` 非空 → 父 token 必须已签发且 `token ⊆ parent`（可递归）。"""
        parent_id = getattr(token, "parent_id", "") or ""
        if not parent_id:
            return True
        if depth >= 16:                       # 防御性上限（避免伪造环）
            return False
        parent_raw = self._tokens.get(parent_id)
        if parent_raw is None:
            return False
        parent = self._resolve_raw(parent_raw)
        if parent is None:
            return False
        if not _token_contains(token, parent):
            return False
        return self._traceable(parent, depth + 1)


    def _cache_verified(self, token: str, exp: float,
                        resolved: CapabilityToken) -> None:
        """缓存已验签 token；过期项优先清理，仍过多时丢弃最早一半（有界）。"""
        if len(self._verified) >= 4096:
            now = time.time()
            self._verified = {k: v for k, v in self._verified.items() if v[0] > now}
            if len(self._verified) >= 4096:
                for key in list(self._verified)[:2048]:
                    self._verified.pop(key, None)
        self._verified[token] = (exp, resolved)

    def check_capability(self, principal: str, token, tool: str = "",
                         path: str = "", *, network=None, data_class=None) -> bool:
        """五参签名 + 四维（§5.9）。

        **兼容垫片**：旧三参调用 `check_capability(agent_id, tool, path)`
        （第 2 个位置参数为不含 `.` 的字符串，且第 3 个位置参数为空）仍按旧语义
        判定。**仅当未配置 signer**（纯内存 token 时代）才启用；配置了 signer
        后，无 `.` 的字符串一律当 token 验签（失败即拒绝），防止垃圾 token 走
        兼容分支拿 `default_token` 放行（终审 1-1）。
        """
        if (isinstance(token, str) and "." not in token and not path
                and self._signer_factory is None):
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
