"""JSON-aware 3-way rebase（S3，文档 §3.1/§3.2/§3.3/§4）。

「冲突 → 语义 rebase」的算法内核。语义要点（**顺序即语义，不可调换**）：

- 求值顺序：① 类型 → ② 双方一致 → ③ 数组 → ④ 对象 → ⑤ 标量。
- 数组默认 `SET`（增并删减、remove-wins）；`GROW_ONLY` 只增不删（删除 → PIVOT）。
- `merge_policy` 最长前缀优先；同长度歧义 → `PIVOT`。
- 冲突无 partial result（全有或全无），冲突按 `path` 排序；同输入同输出。

**定位**：3-way merge，非 CRDT —— 不做 tombstone 存储，不承诺任意交错下的收敛，
只保证「同一份 (base, local, committed) 快照 → 同一结果」。
"""

from __future__ import annotations

import copy
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional


# ---------------------------------------------------------------- §3.1 数据结构
class _Missing:
    """「无前像」哨兵。必须与合法 JSON 值可区分（区别于 `None`）。"""

    __slots__ = ()

    def __repr__(self) -> str:  # pragma: no cover - 仅用于调试展示
        return "MISSING"


MISSING = _Missing()


@dataclass(frozen=True)
class Conflict:
    """一条结构化冲突：typed + path + 三方 presence。"""

    path: str                 # JSON Pointer；顶层用 ""
    kind: str                 # add_add | modify_modify | delete_modify | type_change | pivot
    local: Any                # 本 agent staged（文档禁用 ours/theirs）
    committed: Any            # 已提交当前值
    base: Any = MISSING


@dataclass(frozen=True)
class MergeResult:
    """3-way 合并结果。`merged` 可为标量；有冲突时为 MISSING。"""

    merged: Any
    conflicts: tuple[Conflict, ...]

    @property
    def ok(self) -> bool:
        # 不变量：ok ⇔ conflicts == ()（禁止 partial result）
        return self.merged is not MISSING and not self.conflicts


# ---------------------------------------------------------------- §3.2 策略枚举
class MergePolicy(str, Enum):
    """数组合并策略（统一枚举）。"""

    SET = "SET"             # 默认（数组）：增并删减（3-way 集合语义），删除自动合并
    GROW_ONLY = "GROW_ONLY"  # 可选：只增不删；出现删除 → PIVOT（审计日志等不可缩短列表）
    REPLACE = "REPLACE"     # 整值取 committed（单写者键）
    PIVOT = "PIVOT"         # 不自动合并，直接升级


# 旧词表兼容（spec 早期用 `replace` / `json-merge` / `grow-only`）
_POLICY_ALIASES: dict = {
    "set": MergePolicy.SET,
    "grow_only": MergePolicy.GROW_ONLY,
    "replace": MergePolicy.REPLACE,
    "pivot": MergePolicy.PIVOT,
    "json-merge": MergePolicy.SET,
    "grow-only": MergePolicy.GROW_ONLY,
}


# ---------------------------------------------------------------- §3.3 Delete 哨兵
class DeleteResource:
    """create 型写的反向操作：删除该资源（不是写入某个值）。

    `DELETE` 与 `None` 必须可区分：`None` = 不可补偿；`DELETE` = 可补偿（删）。
    """

    __slots__ = ()

    def __repr__(self) -> str:  # pragma: no cover - 仅用于调试展示
        return "DELETE"

    def __copy__(self):
        return self

    def __deepcopy__(self, memo=None):
        return self

    def __reduce__(self):
        # pickle 往返必须回到模块级单例（不能新建实例）
        return (_delete_singleton, ())


def _delete_singleton() -> "DeleteResource":
    return DELETE


DELETE = DeleteResource()


# ---------------------------------------------------------------- 内部工具
def _kind(v: Any) -> str:
    """§4.1 类型分组：bool 独立；int/float 同为 number；str/list/dict/null 各自独立。"""
    if v is MISSING:
        return "missing"
    if v is None:
        return "null"
    if isinstance(v, bool):          # bool 是 int 子类，必须单独判断
        return "bool"
    if isinstance(v, (int, float)):
        return "number"
    if isinstance(v, str):
        return "str"
    if isinstance(v, list):
        return "list"
    if isinstance(v, dict):
        return "dict"
    return type(v).__name__


def _is_scalar(v: Any) -> bool:
    """数组元素是否为标量（对象数组无身份，无法安全合并）。"""
    return v is None or isinstance(v, (bool, int, float, str))


def _deepcopy(v: Any) -> Any:
    """深拷贝合法 JSON 值；`MISSING` 保持单例以维持身份语义。"""
    return MISSING if v is MISSING else copy.deepcopy(v)


def _escape(key: str) -> str:
    """JSON Pointer 转义：`~` → `~0`，`/` → `~1`。"""
    return str(key).replace("~", "~0").replace("/", "~1")


def _union(*lists) -> list:
    """保序去重并集（保持首次出现顺序；成员判定用类型严格比较）。"""
    out: list = []
    for lst in lists:
        for x in lst:
            if not _strict_in(x, out):
                out.append(x)
    return out


def _conflict(conflicts: list, path: str, kind: str,
              local: Any, committed: Any, base: Any = MISSING) -> Any:
    """记录一条冲突并返回 MISSING（通知调用方放弃 partial result）。"""
    conflicts.append(Conflict(path=path, kind=kind, local=local,
                              committed=committed, base=base))
    return MISSING


def _coerce_policy(v: Any) -> MergePolicy:
    """兼容旧词表：`replace`→REPLACE、`json-merge`→SET、`grow-only`→GROW_ONLY；
    未知词 → PIVOT（不抛 ValueError，避免脏配置击穿合并）。"""
    if isinstance(v, MergePolicy):
        return v
    key = str(v).strip().lower()
    return _POLICY_ALIASES.get(key, MergePolicy.PIVOT)


def _resolve_policy(path: str, policy: Optional[dict]) -> Optional[MergePolicy]:
    """§3.2 解析规则：最长前缀优先；同长度歧义 → PIVOT；无命中 → None（按类型缺省）。"""
    if not policy:
        return None
    best_len = -1
    best_policy: Optional[MergePolicy] = None
    for k, v in policy.items():
        key = "" if k is None else str(k)
        if key == "":
            matched = True                      # 根前缀：匹配一切路径（§3.2 规则 4）
        else:
            matched = (path == key) or path.startswith(key + "/")
        if not matched:
            continue
        if len(key) > best_len:
            best_len = len(key)
            best_policy = _coerce_policy(v)
        elif len(key) == best_len and _coerce_policy(v) != best_policy:
            return MergePolicy.PIVOT             # 歧义 → PIVOT（不静默取第一条）
    return best_policy


# ---------------------------------------------------------------- §4.1 求值顺序
def _strict_eq(a: Any, b: Any) -> bool:
    """类型严格的深比较（避免 `{"a": True} == {"a": 1}` 这类嵌套混淆）。"""
    if a is b:
        return True
    if _kind(a) != _kind(b):
        return False
    if isinstance(a, dict):
        if set(a) != set(b):
            return False
        return all(_strict_eq(a[k], b[k]) for k in a)
    if isinstance(a, list):
        if len(a) != len(b):
            return False
        return all(_strict_eq(x, y) for x, y in zip(a, b))
    return a == b


def _strict_in(x: Any, seq) -> bool:
    """类型严格的成员判定（`True` 与 `1` 不得互相命中）。"""
    return any(_strict_eq(x, y) for y in seq)


def _merge(base: Any, local: Any, committed: Any, path: str,
           policy: Optional[dict], conflicts: list) -> Any:
    """递归合并；返回合并值，冲突时返回 MISSING。"""
    # ① 类型检查（先于相等性比较，避免 1==True / 1==1.0 静默合并）
    if not (_kind(base) == _kind(local) == _kind(committed)):
        return _conflict(conflicts, path, "type_change", local, committed, base)

    # ② 双方一致 → 收敛（用类型严格比较，防嵌套 bool/int 混淆）
    if _strict_eq(local, committed):
        return _deepcopy(local)

    # ③ 数组
    if isinstance(local, list):
        return _merge_array(base, local, committed, path, policy, conflicts)

    # ④ 对象
    if isinstance(local, dict):
        return _merge_object(base, local, committed, path, policy, conflicts)

    # ⑤ 标量
    if _strict_eq(base, local):
        return _deepcopy(committed)
    if _strict_eq(base, committed):
        return _deepcopy(local)
    return _conflict(conflicts, path, "modify_modify", local, committed, base)


# ---------------------------------------------------------------- §4.3 对象
def _merge_object(base: Any, local: Any, committed: Any, path: str,
                  policy: Optional[dict], conflicts: list) -> Any:
    b = base if isinstance(base, dict) else {}
    l = local if isinstance(local, dict) else {}
    c = committed if isinstance(committed, dict) else {}
    out: dict = {}
    for key in sorted(set(b) | set(l) | set(c)):
        bv = b.get(key, MISSING)
        lv = l.get(key, MISSING)
        cv = c.get(key, MISSING)
        child = f"{path}/{_escape(key)}"

        if bv is MISSING:                       # base 缺
            if lv is MISSING:
                out[key] = _deepcopy(cv)        # 缺 缺 有 → committed
            elif cv is MISSING:
                out[key] = _deepcopy(lv)        # 缺 有 缺 → local
            elif _strict_eq(lv, cv):
                out[key] = _deepcopy(lv)        # 缺 有 有 相等 → 取
            else:
                _conflict(conflicts, child, "add_add", lv, cv, MISSING)
            continue

        if lv is MISSING and cv is MISSING:     # 有 缺 缺 → 删除
            continue
        if lv is MISSING:                       # 有 缺 有
            if _strict_eq(cv, bv):              # 有 缺 =base → 删除
                continue
            _conflict(conflicts, child, "delete_modify", MISSING, cv, bv)
            continue
        if cv is MISSING:                       # 有 有 缺
            if _strict_eq(lv, bv):              # 有 =base 缺 → 删除
                continue
            _conflict(conflicts, child, "delete_modify", lv, MISSING, bv)
            continue

        res = _merge(bv, lv, cv, child, policy, conflicts)   # 有 有 有 → 递归
        if res is not MISSING:
            out[key] = res
    return out


# ---------------------------------------------------------------- §4.2 数组
def _merge_array(base: Any, local: Any, committed: Any, path: str,
                 policy: Optional[dict], conflicts: list) -> Any:
    p = _resolve_policy(path, policy)           # §3.2
    if p is MergePolicy.REPLACE:
        return _deepcopy(committed)
    if p is MergePolicy.PIVOT:
        return _conflict(conflicts, path, "pivot", local, committed, base)

    # 对象数组无身份：按值合并会把「同元素被改」变成「新增两项」→ 直接升级
    if not all(_is_scalar(x) for x in list(base or []) + list(local) + list(committed)):
        return _conflict(conflicts, path, "pivot", local, committed, base)

    b, l, c = list(base or []), list(local), list(committed)

    if p is MergePolicy.GROW_ONLY:              # 可选：只增不删
        if any(not _strict_in(x, l) or not _strict_in(x, c) for x in b):
            return _conflict(conflicts, path, "pivot", local, committed, base)
        return _union(l, c)

    # SET（默认）：增并删减 —— remove-wins
    removals = [x for x in b if not _strict_in(x, l) or not _strict_in(x, c)]  # 任一方删了 → 删
    additions = [x for x in l if not _strict_in(x, b)]
    additions += [x for x in c if not _strict_in(x, b) and not _strict_in(x, additions)]
    out = [x for x in b if not _strict_in(x, removals)]          # 保 base 序
    for x in additions:
        if not _strict_in(x, out) and not _strict_in(x, removals):
            out.append(x)
    return out


# ---------------------------------------------------------------- 公开入口
def merge3(base: Any, local: Any, committed: Any,
           policy: Optional[dict] = None) -> MergeResult:
    """JSON-aware 3-way merge（§4）。

    :param base:    合并基线（无 pre-image 时为 MISSING）
    :param local:   本 agent staged 值
    :param committed: 已提交当前值
    :param policy:  `{JSON Pointer 前缀: MergePolicy}`，最长前缀优先
    """
    conflicts: list = []
    merged = _merge(base, local, committed, "", policy, conflicts)
    if conflicts:
        conflicts.sort(key=lambda cf: cf.path)     # §4.4-2 按 path 排序
        return MergeResult(MISSING, tuple(conflicts))
    return MergeResult(merged, ())
