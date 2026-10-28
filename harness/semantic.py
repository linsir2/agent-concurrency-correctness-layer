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


def _symbols(tree: ast.Module) -> tuple[set[str], set[str], bool]:
    """返回 `(确定符号, 不确定符号, 是否动态)`。

    - 不确定 = 只出现在 `if`/`try`/循环/`with` 体里的定义（不据此判「已删除」）；
    - 动态 = 星号导入 / 模块级 `__getattr__` / 调用 `globals/exec/eval/__import__/
      getattr/setattr`（无法静态确定属性集合）。
    """
    certain: set[str] = set()
    uncertain: set[str] = set()
    dynamic = False

    def body(stmts: Iterable[ast.stmt], maybe: bool) -> None:
        nonlocal dynamic
        for stmt in stmts:
            target = uncertain if maybe else certain
            if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                target.add(stmt.name)
                if stmt.name in _DYNAMIC_ATTRS:
                    dynamic = True
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
                                   getattr(ast, "TryStar", ast.Try),
                                   getattr(ast, "AsyncWith", ast.With))):
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
    return certain, uncertain, dynamic


# ---------------------------------------------------------------- 引用
class _Refs(ast.NodeVisitor):
    """收集「显式模块级引用」；注解与条件导入/受保护块不计。"""

    def __init__(self, module: str, is_init: bool, path: str) -> None:
        self.module = module
        self.is_init = is_init
        self.path = path
        self.aliases: dict[str, str] = {}
        self.refs: list[tuple[str, str | None, RefSite]] = []
        self._guard = 0

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
            if alias.name == "*" or self._guard:
                continue
            self.refs.append((module, alias.name, self._site(alias)))

    def visit_Attribute(self, node: ast.Attribute) -> None:
        if isinstance(node.value, ast.Name) and node.value.id in self.aliases:
            self.refs.append((self.aliases[node.value.id], node.attr, self._site(node)))
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

    base_syms: dict[str, set[str]] = {}
    base_unc: dict[str, set[str]] = {}
    base_dyn: set[str] = set()
    for module, (_path, tree) in b.items():
        syms, unc, dyn = _symbols(tree)
        base_syms[module], base_unc[module] = syms, unc
        if dyn:
            base_dyn.add(module)

    removed: dict[str, set[str]] = {}
    removed_modules: set[str] = set()
    for module in b:
        if module not in p:
            removed_modules.add(module)
            continue
        syms, unc, dyn = _symbols(p[module][1])
        if dyn or module in base_dyn:
            continue
        gone = (base_syms[module] - syms) - unc - base_unc[module]
        if gone:
            removed[module] = gone

    if not removed and not removed_modules:
        return SemanticReport((), len(p), skipped,
                              int((time.monotonic() - started) * 1000))

    found: dict[tuple[str, str], list[RefSite]] = {}
    for module, (path, tree) in p.items():
        is_init = path.replace("\\", "/").endswith("__init__.py")
        visitor = _Refs(module, is_init, path)
        visitor.visit(tree)
        for target, name, site in visitor.refs:
            if target in removed_modules:
                found.setdefault(("removed_module", target), []).append(site)
            elif name and name in removed.get(target, ()):
                found.setdefault(("removed_symbol", f"{target}:{name}"), []).append(site)

    issues: list[SemanticIssue] = []
    for (kind, key), sites in sorted(found.items()):
        if kind == "removed_module":
            module, name = key, ""
            hint = "模块被删除/改名；请改为新的模块路径"
        else:
            module, name = key.split(":", 1)
            hint = "符号被删除/改名；请改用新名字，或保留兼容别名"
        uniq = tuple(sorted(set(sites), key=lambda s: (s.path, s.line, s.col)))
        issues.append(SemanticIssue(kind, module, name, uniq, hint))
    return SemanticReport(tuple(issues), len(p), skipped,
                          int((time.monotonic() - started) * 1000))
