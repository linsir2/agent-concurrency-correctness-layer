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

import hashlib
import ipaddress
import time
from dataclasses import dataclass, field

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
    data_class: str | None = None


def _lower_class(a: str, b: str | None) -> str:
    """按分级序取**更低**的 data_class（S13c P0-7）。

    ❌ `min("public", "confidential") == "confidential"`（字面量比较 → 越权）；
    ✅ 用 `_DATA_CLASS_LEVELS` 比较。`"*"`：父 `*` → 取 requested；requested
    `*`（或 None）→ 取父。未知等级 → `CapabilityAttenuationError`（fail-closed）。

    C4：**任一分支**都必须校验等级可达 —— `a == "*"` 不得成为「未知 b 绕过
    fail-closed」的后门；`b is None/*` 也同样不得让未知 `a` 原样通过。
    """
    if a != "*" and a not in _DATA_CLASS_LEVELS:
        raise CapabilityAttenuationError(
            f"未知 data_class: {a!r}（无法按分级序比较 → 拒绝派生）")
    if b is not None and b != "*" and b not in _DATA_CLASS_LEVELS:
        raise CapabilityAttenuationError(
            f"未知 data_class: {b!r}（无法按分级序比较 → 拒绝派生）")
    if a == "*":
        return b if b else "*"
    if b is None or b == "*":
        return a
    la = _DATA_CLASS_LEVELS[a]
    lb = _DATA_CLASS_LEVELS[b]
    return a if la <= lb else b


def _path_prefix_match(prefix: str, path: str) -> bool:
    """路径前缀匹配（**按段边界**）。

    `docs/secret.md` 不得覆盖 `docs/secret.md.bak`（旧的裸 startswith 会）。
    目录前缀以 `/` 结尾时按目录匹配；否则要求 `path == prefix` 或以
    `prefix + "/"` 开头。
    """
    if path == prefix:
        return True
    if prefix.endswith("/"):
        return path.startswith(prefix)
    return path.startswith(prefix + "/")


def _path_contains_one(wide: str, narrow: str) -> bool:
    """路径前缀包含：`narrow` 是否被 `wide` 覆盖（`allows_path` 同谓词）。"""
    if wide == "*":
        return True
    if narrow == "*":
        return False
    return _path_prefix_match(wide, narrow)


def _paths_contains(parent_paths: set[str], child_paths: set[str]) -> bool:
    """`child` 的路径语言是否 ⊆ `parent`（每项至少被一个父前缀覆盖）。"""
    parent = set(parent_paths or ())
    if "*" in parent:
        return True
    child = set(child_paths or ())
    if "*" in child:
        return False
    return all(any(_path_contains_one(p, c) for p in parent) for c in child)


def _intersect_paths(parent_set, requested_set) -> set[str]:
    """C4：paths 按**语义包含**求交（不是集合精确交集）。

    父 `{"/src"}` 与请求 `{"/src/lib"}` → 收窄保留 `{"/src/lib"}`（子只能访问
    更窄子树）；请求更宽 `{"/"}` 而父 `{"/src"}` → 保留父项 `{"/src"}`。
    """
    parent = set(parent_set or ())
    requested = set(requested_set or ())
    if "*" in parent:
        return requested
    if "*" in requested:
        return parent
    out: set[str] = set()
    for r in requested:
        for p in parent:
            if _path_contains_one(p, r):
                out.add(r)                 # 请求更窄（父覆盖请求项）
            elif _path_contains_one(r, p):
                out.add(p)                 # 父更窄（父项覆盖请求项）
    return out


def _network_contains_one(wide: str, narrow: str) -> bool:
    """`narrow` 允许的主机集合是否 ⊆ `wide`（host/port/CIDR/通配）。"""
    if wide == "*":
        return True
    if narrow == "*":
        return False
    w_host, w_port = _split_host_port(wide)
    n_host, n_port = _split_host_port(narrow)
    if w_port is not None and w_port != n_port:
        return False
    w_host = w_host.lower()
    n_host = n_host.lower()
    if not w_host or not n_host:
        return False
    if w_host == n_host:
        return True
    if "/" in w_host:
        if "/" in n_host:
            try:
                return ipaddress.ip_network(n_host, strict=False).subnet_of(
                    ipaddress.ip_network(w_host, strict=False))
            except ValueError:
                return False
        return _ip_in_cidr(n_host, w_host)
    if w_host.startswith("*."):
        return n_host.endswith(w_host[1:])
    return False


def _networks_contains(parent_net: set[str], child_net: set[str]) -> bool:
    parent = set(parent_net or ())
    if "*" in parent:
        return True
    child = set(child_net or ())
    if "*" in child:
        return False
    return all(any(_network_contains_one(p, c) for p in parent) for c in child)


def _intersect_network(parent_set, requested_set) -> set[str]:
    """C4：network 用语义包含求交（父网段/通配覆盖请求项则保留请求项）。"""
    parent = set(parent_set or ())
    requested = set(requested_set or ())
    if "*" in parent:
        return requested
    if "*" in requested:
        return parent
    out: set[str] = set()
    for r in requested:
        for p in parent:
            if _network_contains_one(p, r):
                out.add(r)
            elif _network_contains_one(r, p):
                out.add(p)
    return out


def _intersect_scope(parent_set, requested_set) -> set[str]:
    """通配感知交集（S13c §2.4）：父 `{"*"}` → requested；子 `{"*"}` → 父；否则 `&`。"""
    parent = set(parent_set or ())
    requested = set(requested_set or ())
    if "*" in parent:
        return requested
    if "*" in requested:
        return parent
    return parent & requested


def _scope_digest(scope: dict) -> str:
    """能力快照摘要（C6：lineage 锚定不可变快照，不读全局 `_tokens`）。"""
    payload = {
        "tools": sorted(scope.get("tools") or ()),
        "paths": sorted(scope.get("paths") or ()),
        "network": sorted(scope.get("network") or ()),
        "data_class": scope.get("data_class") or "public",
    }
    return hashlib.sha256(repr(payload).encode("utf-8")).hexdigest()[:32]


def _scope_of_token(token) -> dict:
    return {
        "tools": sorted(getattr(token, "tools", ()) or ()),
        "paths": sorted(getattr(token, "paths", ()) or ()),
        "network": sorted(getattr(token, "network", ()) or ()),
        "data_class": getattr(token, "data_class", "public") or "public",
    }


def _token_contains_scope(child: CapabilityToken, scope: dict) -> bool:
    """`child` 是否 ⊆ 给定父能力快照（语义包含，C4）。"""
    if not _paths_contains(set(scope.get("paths") or ()), child.paths):
        return False
    if not _networks_contains(set(scope.get("network") or ()),
                              child.network):
        return False
    p_tools = set(scope.get("tools") or ())
    if "*" not in p_tools and ("*" in child.tools or not (child.tools <= p_tools)):
        return False
    pd = scope.get("data_class", "public") or "public"
    cd = child.data_class
    if pd == "*":
        return True
    pc = _DATA_CLASS_LEVELS.get(pd)
    cc = _DATA_CLASS_LEVELS.get(cd)
    if pc is None or cc is None:
        return cd == pd
    return cc <= pc


def _token_contains(child: CapabilityToken, parent: CapabilityToken) -> bool:
    """`child` 每一维是否 ⊆ `parent`（父 `"*"` 视作全集；C4 语义包含）。"""
    if not _paths_contains(set(getattr(parent, "paths", ()) or ()),
                           set(getattr(child, "paths", ()) or ())):
        return False
    if not _networks_contains(set(getattr(parent, "network", ()) or ()),
                              set(getattr(child, "network", ()) or ())):
        return False
    p_tools = set(getattr(parent, "tools", ()) or ())
    c_tools = set(getattr(child, "tools", ()) or ())
    if "*" not in p_tools and ("*" in c_tools or not (c_tools <= p_tools)):
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


def attenuate_token(parent: CapabilityToken, requested: Scope,
                    child_id: str) -> CapabilityToken:
    """S13c §2.4 P0-7：子能力 = parent ∩ requested；`data_class` 按分级序取低。

    这是**唯一**给子签发能力（派生）的路径；`policy.issue` 的合同保持不变
    （`test_i4_capability_production_no_self_issue` 依赖旧签名/自证拒绝）。

    C4：tools 仍为集合精确交集；paths/network 按**语义包含**求交。
    C6：把父能力快照写进 token（`parent_scope` + 摘要），lineage 校验不再
    依赖可被任意 `issue` 覆盖的全局 `_tokens`。
    """
    if not isinstance(child_id, str) or not child_id:
        raise ValueError("attenuate_token: child_id 必须是非空字符串")
    if isinstance(requested, dict):
        requested = Scope(
            tools=set(requested.get("tools") or ()),
            paths=set(requested.get("paths") or ()),
            network=set(requested.get("network") or ()),
            data_class=requested.get("data_class"))
    parent_scope = _scope_of_token(parent)
    token = CapabilityToken(
        child_id,
        tools=_intersect_scope(parent.tools, requested.tools),
        paths=_intersect_paths(parent.paths, requested.paths),
        network=_intersect_network(parent.network, requested.network),
        data_class=_lower_class(parent.data_class, requested.data_class),
        parent_id=parent.agent_id,
        parent_scope=parent_scope,
        parent_scope_digest=_scope_digest(parent_scope),
    )
    return token


def _split_host_port(entry: str) -> tuple[str, int | None]:
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

    def __init__(self, agent_id: str = "", tools: set[str] | None = None,
                 paths: set[str] | None = None,
                 network: set[str] | None = None,
                 data_class: str = "public",
                 parent_id: str = "",
                 parent_scope: dict | None = None,
                 parent_scope_digest: str = "") -> None:
        self.agent_id = agent_id
        self.tools = set(tools or ())
        self.paths = set(paths or ())
        self.network = set(network or ())
        self.data_class = data_class
        # S13c §2.4：派生 token 的父身份（`attenuate_token` 写入；根 token 为空）。
        self.parent_id = parent_id
        # C6：父能力**快照**（签发时冻结），lineage 校验只认它，不认全局 dict。
        self.parent_scope = (None if parent_scope is None
                             else {
                                 "tools": sorted(parent_scope.get("tools") or ()),
                                 "paths": sorted(parent_scope.get("paths") or ()),
                                 "network": sorted(parent_scope.get("network") or ()),
                                 "data_class": parent_scope.get("data_class") or "public",
                             })
        self.parent_scope_digest = parent_scope_digest or (
            _scope_digest(self.parent_scope) if self.parent_scope is not None else "")

    def allows_tool(self, tool: str) -> bool:
        return "*" in self.tools or tool in self.tools

    def allows_path(self, path: str) -> bool:
        if "*" in self.paths:
            return True
        return any(_path_prefix_match(p, path) for p in self.paths)

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
                 default_token: CapabilityToken | None = None,
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
    def mint(self, agent_id: str, *, tools: set[str] | None = None,
             paths: set[str] | None = None,
             network: set[str] | None = None,
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

    def issue(self, arg, *, tools: set[str] | None = None,
              paths: set[str] | None = None,
              network: set[str] | None = None,
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
    def resolve_token(self, token) -> CapabilityToken | None:
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
        """`parent_id` 非空 → 父 token 必须已签发且 `token ⊆ parent`（可递归）。

        C6：lineage 锚定到签发时冻结的**父能力快照**（`parent_scope`）。全局
        `_tokens[parent_id]` 可被任意 `issue` 覆盖成宽 token，绝不能作为
        attenuation 的唯一依据；快照缺失（伪造 parent_id 的自签 token）→
        fail-closed。

        C5：跨进程恢复后父 token 由 `_restore_capability` 按需加载并注册，
        因此仍要求 `_tokens[parent_id]` 存在来递归校验父链。
        """
        parent_id = getattr(token, "parent_id", "") or ""
        if not parent_id:
            return True
        if depth >= 16:                       # 防御性上限（避免伪造环）
            return False
        snapshot = getattr(token, "parent_scope", None)
        if not isinstance(snapshot, dict):
            return False                      # 有父却无快照 → 不可追溯（fail-closed）
        digest = getattr(token, "parent_scope_digest", "") or ""
        if digest and digest != _scope_digest(snapshot):
            return False                      # 快照被篡改
        if not _token_contains_scope(token, snapshot):
            return False
        parent_raw = self._tokens.get(parent_id)
        if parent_raw is None:
            return False                      # 父链未注册（恢复缺失 → 拒绝）
        parent = self._resolve_raw(parent_raw)
        if parent is None:
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
        return not data_class or resolved.allows_data_class(data_class)

    def allows_network(self, token, host: str) -> bool:
        resolved = self._resolve(token)
        return resolved is not None and resolved.allows_network(host)

    def allows_data_class(self, token, data_class: str) -> bool:
        resolved = self._resolve(token)
        return resolved is not None and resolved.allows_data_class(data_class)

    def token(self, agent_id: str):
        return self._tokens.get(agent_id)
