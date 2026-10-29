"""S24-3：符号级语义冲突检测（静态 / 零运行时依赖 / 仅 Python）。

**要解决什么**：两个 agent 改不同文件，文本合并不冲突，但一方删/改名了模块级
符号、另一方的**新代码**仍显式引用它 —— 这类问题当前只能靠 `verify` 跑起来
才发现。本模块在不执行代码的前提下，把「名字消失」这一类先静态挡住。

**精度边界（用召回换精度，宁放过不误杀）**：

| 能查 | 查不到（故意跳过） |
|---|---|
| 模块级 def/class/赋值/import 绑定 | `getattr` / `importlib` / `exec` / `globals()` 构造的引用 |
| `from m import x`、`import m as a` 后的 `a.x` | `from m import *`、模块级 `__getattr__` |
| 相对导入（按文件所在包解析） | 类/实例成员、签名/行为变化 |
| 模块被整体删除 | 条件定义（`if`/`try` 内）一律按「不确定」放过 |
| 注解里的引用不算（运行期不求值） | 命名空间包 / 多根布局的歧义模块 → 跳过 |

**判定规则**：`name ∈ symbols(base_module) ∧ name ∉ symbols(post_module) ∧ 模块可解析
∧ 双方都不 dynamic` → `removed_symbol`；base 有模块、post 无 → `removed_module`。
**只报「base 有、post 没有」**：base 就已经坏掉的引用永不报（那是历史问题，
交给 `verify` 或 CI）。
"""

from __future__ import annotations

import ast
import fnmatch
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field

MAX_FILES = 2000
MAX_FILE_BYTES = 5 << 20
_DYNAMIC_ATTRS = ("__getattr__", "__getattribute__")
_DYNAMIC_CALLS = ("globals", "exec", "eval", "__import__", "setattr", "getattr")


@dataclass(frozen=True)
class RefSite:
    """一处引用（文件 + 行 + 列）。"""

    path: str
    line: int
    col: int


@dataclass(frozen=True)
class SemanticIssue:
    """一个语义冲突：`kind` ∈ {removed_symbol, removed_module}。"""

    kind: str
    module: str
    name: str
    refs: tuple[RefSite, ...] = ()
    hint: str = ""

    def as_dict(self) -> dict:
        return {"kind": self.kind, "module": self.module, "name": self.name,
                "hint": self.hint,
                "refs": [{"path": r.path, "line": r.line, "col": r.col}
                         for r in self.refs]}


@dataclass(frozen=True)
class SemanticReport:
    """检测结果；`skipped` 记录因何未检查（可观测，不静默）。"""

    issues: tuple[SemanticIssue, ...] = ()
    scanned: int = 0
    skipped: dict[str, str] = field(default_factory=dict)
    duration_ms: int = 0

    @property
    def ok(self) -> bool:
        return not self.issues


# ---------------------------------------------------------------- 模块映射
def _module_name(rel: str) -> str | None:
    """`app/core.py` → `app.core`；`app/__init__.py` → `app`；非 .py → None。"""
    path = rel.replace("\\", "/").lstrip("./")
    if not path.endswith(".py"):
        return None
    parts = path[:-3].split("/")
    if parts and parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join(parts) if parts else None


def _package_of(module: str, is_init: bool) -> tuple[str, ...]:
    parts = module.split(".") if module else []
    if not is_init:
        parts = parts[:-1]
    return tuple(parts)


def _resolve_import(node: ast.ImportFrom, module: str, is_init: bool) -> str | None:
    """相对导入 → 绝对模块名；无法解析 → None（跳过）。"""
    if not node.level:
        return node.module
    pkg = _package_of(module, is_init)
    drop = node.level - 1
    if drop >= len(pkg):
        return None
    base = pkg[: len(pkg) - drop]
    if node.module:
        base = base + tuple(node.module.split("."))
    return ".".join(base) or None


# ---------------------------------------------------------------- 符号表
def _target_names(target: ast.AST) -> list[str]:
    if isinstance(target, ast.Name):
        return [target.id]
    if isinstance(target, (ast.Tuple, ast.List)):
        out: list[str] = []
        for elt in target.elts:
            out.extend(_target_names(elt))
        return out
    return []


def _conditional_bodies(stmt: ast.stmt) -> Iterable[list[ast.stmt]]:
    if isinstance(stmt, ast.If):
        yield stmt.body
        yield stmt.orelse
    elif isinstance(stmt, (ast.Try, getattr(ast, "TryStar", ast.Try))):
        yield stmt.body
        for handler in stmt.handlers:
            yield handler.body
        yield stmt.orelse
        yield stmt.finalbody
    elif isinstance(stmt, (ast.For, ast.AsyncFor, ast.While)):
        yield stmt.body
        yield stmt.orelse
    elif isinstance(stmt, (ast.With, getattr(ast, "AsyncWith", ast.With))):
        yield stmt.body


@dataclass(frozen=True)
class FuncSig:
    """模块级函数签名（只用于「post 调用是否仍合法」的保守判定）。"""

    name: str
    posonly_required: tuple[str, ...] = ()
    posonly_optional: tuple[str, ...] = ()
    poskw_required: tuple[str, ...] = ()
    poskw_optional: tuple[str, ...] = ()
    kwonly_required: tuple[str, ...] = ()
    kwonly_optional: tuple[str, ...] = ()
    varargs: bool = False
    varkw: bool = False
    opaque: bool = False


@dataclass(frozen=True)
class ClassFacts:
    """模块级类成员；有基类/装饰器/动态属性时 `opaque=True`（不判删除）。"""

    name: str
    members: frozenset[str] = frozenset()
    uncertain: frozenset[str] = frozenset()
    has_bases: bool = False
    opaque: bool = False


@dataclass(frozen=True)
class ModuleFacts:
    symbols: frozenset[str]
    uncertain: frozenset[str]
    dynamic: bool
    funcs: Mapping[str, FuncSig]
    classes: Mapping[str, ClassFacts]


@dataclass(frozen=True)
class CallSite:
    module: str
    name: str
    pos_count: int
    kw_names: tuple[str, ...]
    has_star: bool
    has_kwstar: bool
    site: RefSite


@dataclass(frozen=True)
class MemberRef:
    module: str
    cls: str
    attr: str
    site: RefSite


def _func_sig(node) -> FuncSig:
    args = node.args
    defaults = len(args.defaults)
    positional = list(args.posonlyargs) + list(args.args)
    required = positional[: len(positional) - defaults]
    optional = positional[len(positional) - defaults:] if defaults else []
    posonly_n = len(args.posonlyargs)
    kw_defaults = list(args.kw_defaults)
    kw_required = [a.arg for a, d in zip(args.kwonlyargs, kw_defaults) if d is None]
    kw_optional = [a.arg for a, d in zip(args.kwonlyargs, kw_defaults) if d is not None]
    return FuncSig(
        name=node.name,
        posonly_required=tuple(a.arg for a in required[:posonly_n]),
        posonly_optional=tuple(a.arg for a in optional[:max(0, posonly_n - len(required))]),
        poskw_required=tuple(a.arg for a in required[posonly_n:]),
        poskw_optional=tuple(a.arg for a in optional[max(0, posonly_n - len(required)):]),
        kwonly_required=tuple(kw_required),
        kwonly_optional=tuple(kw_optional),
        varargs=args.vararg is not None,
        varkw=args.kwarg is not None,
        opaque=bool(node.decorator_list),
    )


def _class_facts(node: ast.ClassDef) -> ClassFacts:
    members: set[str] = set()
    uncertain: set[str] = set()
    opaque = bool(node.decorator_list) or bool(node.bases) or bool(node.keywords)

    def collect(stmts: Iterable[ast.stmt], maybe: bool) -> None:
        nonlocal opaque
        for stmt in stmts:
            target = uncertain if maybe else members
            if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
                target.add(stmt.name)
                if stmt.name in _DYNAMIC_ATTRS:
                    opaque = True
            elif isinstance(stmt, ast.ClassDef):
                target.add(stmt.name)
            elif isinstance(stmt, ast.Assign):
                for tgt in stmt.targets:
                    target.update(_target_names(tgt))
            elif isinstance(stmt, ast.AnnAssign):
                target.update(_target_names(stmt.target))
            elif isinstance(stmt, (ast.If, ast.Try, ast.For, ast.AsyncFor,
                                   ast.While, ast.With,
                                   getattr(ast, "TryStar", ast.Try),
                                   getattr(ast, "AsyncWith", ast.With))):
                for sub in _conditional_bodies(stmt):
                    collect(sub, True)

    collect(node.body, False)
    return ClassFacts(name=node.name, members=frozenset(members),
                      uncertain=frozenset(uncertain),
                      has_bases=bool(node.bases), opaque=opaque)


def _facts(tree: ast.Module) -> ModuleFacts:
    """模块级符号 + 函数签名 + 类成员（条件定义只进 uncertain）。"""
    certain: set[str] = set()
    uncertain: set[str] = set()
    dynamic = False
    funcs: dict[str, FuncSig] = {}
    classes: dict[str, ClassFacts] = {}

    def body(stmts: Iterable[ast.stmt], maybe: bool) -> None:
        nonlocal dynamic
        for stmt in stmts:
            target = uncertain if maybe else certain
            if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
                target.add(stmt.name)
                if stmt.name in _DYNAMIC_ATTRS:
                    dynamic = True
                if not maybe:
                    funcs[stmt.name] = _func_sig(stmt)
            elif isinstance(stmt, ast.ClassDef):
                target.add(stmt.name)
                if not maybe:
                    classes[stmt.name] = _class_facts(stmt)
            elif isinstance(stmt, ast.Assign):
                for tgt in stmt.targets:
                    target.update(_target_names(tgt))
            elif isinstance(stmt, ast.AnnAssign):
                target.update(_target_names(stmt.target))
            elif isinstance(stmt, (ast.Import, ast.ImportFrom)):
                for alias in stmt.names:
                    if alias.name == "*":
                        dynamic = True
                        continue
                    target.add(alias.asname or alias.name.split(".")[0])
            elif isinstance(stmt, (ast.If, ast.Try, ast.For, ast.AsyncFor,
                                   ast.While, ast.With,
                                   getattr(stmt, "TryStar", ast.Try),
                                   getattr(stmt, "AsyncWith", ast.With))):
                for sub in _conditional_bodies(stmt):
                    body(sub, True)

    body(tree.body, False)
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        is_dynamic_name = (isinstance(node.func, ast.Name)
                           and node.func.id in _DYNAMIC_CALLS)
        is_dynamic_attr = (isinstance(node.func, ast.Attribute)
                           and node.func.attr in ("import_module",
                                                  "module_from_spec"))
        dynamic = dynamic or is_dynamic_name or is_dynamic_attr
    return ModuleFacts(frozenset(certain), frozenset(uncertain), dynamic,
                       funcs, classes)


def _symbols(tree: ast.Module) -> tuple[set[str], set[str], bool]:
    """兼容包装：返回 `(确定符号, 不确定符号, 是否动态)`。"""
    facts = _facts(tree)
    return set(facts.symbols), set(facts.uncertain), facts.dynamic


# ---------------------------------------------------------------- 引用
class _Refs(ast.NodeVisitor):
    """收集「显式模块级引用」；注解与条件导入/受保护块不计。"""

    def __init__(self, module: str, is_init: bool, path: str, *,
                 facts: ModuleFacts | None = None,
                 facts_by_module: Mapping[str, ModuleFacts] | None = None) -> None:
        self.module = module
        self.is_init = is_init
        self.path = path
        self.facts = facts or ModuleFacts(frozenset(), frozenset(), False, {}, {})
        self.facts_by_module = facts_by_module or {}
        self.aliases: dict[str, str] = {}
        self.imported: dict[str, tuple[str, str]] = {}
        self.refs: list[tuple[str, str | None, RefSite]] = []
        self.calls: list[CallSite] = []
        self.member_refs: list[MemberRef] = []
        self.var_class: dict[str, tuple[str, str]] = {}
        self._guard = 0

    def _resolve_name(self, name: str) -> tuple[str, str, str] | None:
        """本地名 → `(module, symbol, kind)`；kind ∈ {func, class}。"""
        if name in self.imported:
            module, orig = self.imported[name]
            facts = self.facts_by_module.get(module)
            if facts is not None:
                if orig in facts.classes:
                    return module, orig, "class"
                if orig in facts.funcs:
                    return module, orig, "func"
            return None
        if name in self.facts.funcs:
            return self.module, name, "func"
        if name in self.facts.classes:
            return self.module, name, "class"
        return None

    def bind_vars(self, tree: ast.Module) -> None:
        """收集唯一 `cfg = Cfg(...)` 绑定（同名再赋值/导入/定义 → 放弃）。"""
        # 先登记顶层导入，否则 `Cfg` 尚未进入 self.imported，绑定无法解析。
        for stmt in tree.body:
            if isinstance(stmt, ast.ImportFrom):
                module = _resolve_import(stmt, self.module, self.is_init)
                if module is None:
                    continue
                for alias in stmt.names:
                    if alias.name != "*":
                        self.imported[alias.asname or alias.name] = (module, alias.name)
            elif isinstance(stmt, ast.Import):
                for alias in stmt.names:
                    local = alias.asname or alias.name.split(".")[0]
                    self.aliases[local] = (alias.name if alias.asname
                                           else alias.name.split(".")[0])
        counts: dict[str, int] = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store):
                counts[node.id] = counts.get(node.id, 0) + 1
        for stmt in tree.body:
            value = getattr(stmt, "value", None)
            if not isinstance(value, ast.Call) or not isinstance(value.func, ast.Name):
                continue
            owner = self._resolve_name(value.func.id)
            if owner is None or owner[2] != "class":
                continue
            targets: list[str] = []
            if isinstance(stmt, ast.Assign):
                for tgt in stmt.targets:
                    targets.extend(_target_names(tgt))
            elif isinstance(stmt, ast.AnnAssign):
                targets.extend(_target_names(stmt.target))
            for name in targets:
                if (counts.get(name, 0) == 1 and name not in self.imported
                        and name not in self.facts.funcs
                        and name not in self.facts.classes):
                    self.var_class[name] = (owner[0], owner[1])

    def _site(self, node: ast.AST) -> RefSite:
        return RefSite(self.path, int(getattr(node, "lineno", 0)),
                       int(getattr(node, "col_offset", 0)))

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            local = alias.asname or alias.name.split(".")[0]
            self.aliases[local] = alias.name if alias.asname else alias.name.split(".")[0]
            if not self._guard:
                self.refs.append((alias.name, None, self._site(alias)))
        for alias in node.names:
            if alias.name not in self.aliases:
                self.aliases[alias.name.split(".")[0]] = alias.name.split(".")[0]

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        module = _resolve_import(node, self.module, self.is_init)
        if module is None:
            return
        for alias in node.names:
            if alias.name == "*":
                continue
            if not self._guard:
                self.imported[alias.asname or alias.name] = (module, alias.name)
                self.refs.append((module, alias.name, self._site(alias)))

    def visit_Attribute(self, node: ast.Attribute) -> None:
        if isinstance(node.value, ast.Name) and node.value.id in self.aliases:
            self.refs.append((self.aliases[node.value.id], node.attr, self._site(node)))
        if not self._guard and not node.attr.startswith("__"):
            owner = None
            if isinstance(node.value, ast.Name):
                if node.value.id in self.var_class:
                    owner = (*self.var_class[node.value.id], "class")
                else:
                    owner = self._resolve_name(node.value.id)
            elif isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Name):
                owner = self._resolve_name(node.value.func.id)
            if owner is not None and owner[2] == "class":
                self.member_refs.append(
                    MemberRef(owner[0], owner[1], node.attr, self._site(node)))
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        owner = None
        if isinstance(node.func, ast.Name):
            owner = self._resolve_name(node.func.id)
        elif (isinstance(node.func, ast.Attribute)
              and isinstance(node.func.value, ast.Name)
              and node.func.value.id in self.aliases):
            module = self.aliases[node.func.value.id]
            facts = self.facts_by_module.get(module)
            if facts is not None and node.func.attr in facts.funcs:
                owner = (module, node.func.attr, "func")
        if owner is not None and owner[2] == "func" and not self._guard:
            pos_count = sum(1 for a in node.args if not isinstance(a, ast.Starred))
            has_star = any(isinstance(a, ast.Starred) for a in node.args)
            kw_names = tuple(k.arg for k in node.keywords if k.arg is not None)
            has_kwstar = any(k.arg is None for k in node.keywords)
            self.calls.append(CallSite(owner[0], owner[1], pos_count, kw_names,
                                       has_star, has_kwstar, self._site(node)))
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if node.target is not None:
            self.visit(node.target)
        if node.value is not None:
            self.visit(node.value)

    def _visit_function(self, node) -> None:
        for deco in node.decorator_list:
            self.visit(deco)
        args = node.args
        for default in list(args.defaults) + [d for d in args.kw_defaults if d]:
            self.visit(default)
        for stmt in node.body:
            self.visit(stmt)

    visit_FunctionDef = _visit_function
    visit_AsyncFunctionDef = _visit_function

    def visit_Try(self, node: ast.Try) -> None:
        guarded = any(self._catches_import_error(h) for h in node.handlers)
        if guarded:
            self._guard += 1
            for stmt in node.body:
                self.visit(stmt)
            self._guard -= 1
        else:
            for stmt in node.body:
                self.visit(stmt)
        for handler in node.handlers:
            for stmt in handler.body:
                self.visit(stmt)
        for stmt in node.orelse + node.finalbody:
            self.visit(stmt)

    def visit_If(self, node: ast.If) -> None:
        guarded = _mentions_type_checking(node.test) or _is_false(node.test)
        self.visit(node.test)
        if guarded:
            for stmt in node.orelse:
                self.visit(stmt)
        else:
            for stmt in node.body + node.orelse:
                self.visit(stmt)

    @staticmethod
    def _catches_import_error(handler: ast.ExceptHandler) -> bool:
        types = handler.type
        if types is None:
            return True
        names = types.elts if isinstance(types, ast.Tuple) else [types]
        return any(isinstance(n, ast.Name) and n.id in ("ImportError",
                                                        "ModuleNotFoundError")
                   for n in names)


def _mentions_type_checking(node: ast.AST) -> bool:
    return any(isinstance(n, ast.Name) and n.id == "TYPE_CHECKING"
               for n in ast.walk(node))


def _is_false(node: ast.AST) -> bool:
    return isinstance(node, ast.Constant) and node.value is False


# ---------------------------------------------------------------- 主流程
def detect(base: Mapping[str, bytes], post: Mapping[str, bytes], *,
           ignore: tuple[str, ...] = (), max_files: int = MAX_FILES
           ) -> SemanticReport:
    """比较 base/post 两个「相对路径 → 源码字节」视图，返回语义冲突报告。

    - 只解析 `.py`，超过 `max_files` 的仓库直接标 `skipped`（不拒绝）；
    - 文件语法错误只跳过该文件，不影响其它文件；
    - `ignore` 是 fnmatch 模式（按相对路径匹配）。
    """
    started = time.monotonic()
    skipped: dict[str, str] = {}

    def view(src: Mapping[str, bytes], tag: str) -> dict[str, tuple[str, ast.Module]]:
        out: dict[str, tuple[str, ast.Module]] = {}
        items = [(p, b) for p, b in src.items()
                 if _module_name(p) and not any(fnmatch.fnmatch(p, g) for g in ignore)]
        if len(items) > max_files:
            skipped[tag] = f"too_many_files:{len(items)}"
            items = items[:max_files]
        for path, raw in items:
            if len(raw) > MAX_FILE_BYTES:
                skipped[path] = "too_large"
                continue
            module = _module_name(path)
            try:
                tree = ast.parse(raw.decode("utf-8"))
            except (SyntaxError, UnicodeDecodeError, ValueError) as exc:
                skipped[path] = f"parse_error:{type(exc).__name__}"
                continue
            if module in out:
                skipped[path] = "duplicate_module"
                continue
            out[module] = (path, tree)
        return out

    b = view(base, "base")
    p = view(post, "post")
    if "base" in skipped or "post" in skipped:
        return SemanticReport((), len(b), skipped,
                              int((time.monotonic() - started) * 1000))

    base_facts = {m: _facts(tree) for m, (_path, tree) in b.items()}
    post_facts = {m: _facts(tree) for m, (_path, tree) in p.items()}

    removed: dict[str, set[str]] = {}
    removed_modules: set[str] = set()
    for module in b:
        if module not in p:
            removed_modules.add(module)
            continue
        bf, pf = base_facts[module], post_facts[module]
        if pf.dynamic or bf.dynamic:
            continue
        gone = (bf.symbols - pf.symbols) - pf.uncertain - bf.uncertain
        if gone:
            removed[module] = gone

    found: dict[tuple[str, str], list[RefSite]] = {}
    for module, (path, tree) in p.items():
        is_init = path.replace("\\", "/").endswith("__init__.py")
        visitor = _Refs(module, is_init, path, facts=post_facts[module],
                        facts_by_module=post_facts)
        visitor.bind_vars(tree)
        visitor.visit(tree)
        for target, name, site in visitor.refs:
            if target in removed_modules:
                found.setdefault(("removed_module", target), []).append(site)
            elif name and name in removed.get(target, ()):
                found.setdefault(("removed_symbol", f"{target}:{name}"), []).append(site)
        for call in visitor.calls:
            bf = base_facts.get(call.module)
            pf = post_facts.get(call.module)
            if bf is None or pf is None:
                continue
            bs, ps = bf.funcs.get(call.name), pf.funcs.get(call.name)
            if (bs is None or ps is None or bs.opaque or ps.opaque
                    or call.has_star or call.has_kwstar):
                continue
            if _call_ok(bs, call) and not _call_ok(ps, call):
                key = f"{call.module}:{call.name}"
                found.setdefault(("signature_incompatible", key), []).append(call.site)
        for ref in visitor.member_refs:
            bf = base_facts.get(ref.module)
            pf = post_facts.get(ref.module)
            if bf is None or pf is None:
                continue
            bc, pc = bf.classes.get(ref.cls), pf.classes.get(ref.cls)
            if (bc is None or pc is None or bc.opaque or pc.opaque
                    or bc.has_bases or pc.has_bases):
                continue
            if (ref.attr in bc.members and ref.attr not in pc.members
                    and ref.attr not in pc.uncertain
                    and ref.attr not in bc.uncertain):
                key = f"{ref.module}:{ref.cls}.{ref.attr}"
                found.setdefault(("removed_member", key), []).append(ref.site)

    issues: list[SemanticIssue] = []
    for (kind, key), sites in sorted(found.items()):
        module, _, name = key.partition(":")
        if kind == "removed_module":
            module, name = key, ""
            hint = "模块被删除/改名；请改为新的模块路径"
        elif kind == "removed_symbol":
            hint = "符号被删除/改名；请改用新名字，或保留兼容别名"
        elif kind == "signature_incompatible":
            bf, pf = base_facts.get(module), post_facts.get(module)
            bs = bf.funcs.get(name) if bf else None
            ps = pf.funcs.get(name) if pf else None
            hint = (f"签名不兼容：{_sig_text(bs)} -> {_sig_text(ps)}"
                    if bs and ps else "函数签名不兼容")
        else:
            hint = "类成员被删除/改名；请改用新名字，或保留兼容别名"
        uniq = tuple(sorted(set(sites), key=lambda s: (s.path, s.line, s.col)))
        issues.append(SemanticIssue(kind, module, name, uniq, hint))
    return SemanticReport(tuple(issues), len(p), skipped,
                          int((time.monotonic() - started) * 1000))


def _call_ok(sig: FuncSig, site: CallSite) -> bool:
    """调用点是否满足该签名（保守：无法确信 → True，宁可漏报）。"""
    pos_slots = (len(sig.posonly_required) + len(sig.posonly_optional)
                 + len(sig.poskw_required) + len(sig.poskw_optional))
    required_pos = len(sig.posonly_required) + len(sig.poskw_required)
    if not sig.varargs and (site.pos_count > pos_slots
                            or site.pos_count < required_pos):
        return False
    allowed_kw = set(sig.poskw_required + sig.poskw_optional
                     + sig.kwonly_required + sig.kwonly_optional)
    if not sig.varkw and not set(site.kw_names) <= allowed_kw:
        return False
    return set(sig.kwonly_required) <= set(site.kw_names)


def _sig_text(sig: FuncSig) -> str:
    parts: list[str] = []
    parts.extend(sig.posonly_required)
    parts.extend(sig.posonly_optional)
    if sig.posonly_required or sig.posonly_optional:
        parts.append("/")
    parts.extend(sig.poskw_required)
    parts.extend(f"{n}=..." for n in sig.poskw_optional)
    if sig.varargs:
        parts.append("*args")
    elif sig.kwonly_required or sig.kwonly_optional:
        parts.append("*")
    parts.extend(sig.kwonly_required)
    parts.extend(f"{n}=..." for n in sig.kwonly_optional)
    if sig.varkw:
        parts.append("**kwargs")
    return f"{sig.name}({', '.join(parts)})"
