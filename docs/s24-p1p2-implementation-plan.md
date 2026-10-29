# S24 P1/P2 实施方案（评审版，未实现）

| 项目 | 内容 |
|---|---|
| 日期 | 2026-10-29 |
| 基线 | `6065f42`（S24-P0 全部完成并推送） |
| 范围 | P1 六项 + P2 四项，全部落地；不做项见 §7 |
| 方法 | 三个子智能体分别调研「语义/Verifier/诊断/语料」「基准/TLA」「Raft 混沌/确定性/跨进程」，我逐条对回代码与沙箱实测后重写 |
| 交付方式 | 每项独立提交；先 TDD 用例，再实现；最后统一评审 |
| 状态 | **已评审通过并实施（2026-10-29）**；实施状态与门禁快照见 §9 |

> 本文件只描述**要改什么、怎么改、数据流、失败语义、测试**，不包含实现。
> 所有行号以 `6065f42` 为准；实现后允许行号漂移，但函数名与行为契约不变。

---

## 0. 结论摘要

- **P1（6 项）**：语义检测扩签名/类成员、Verifier 多步与诊断、诊断补 checkpoint/aging、语料补 I3/I9、token 基准 n=5、TLA 补 I3 与活性。
- **P2（4 项）**：3 进程真 TCP Raft 故障注入、确定性 checkpoint、跨进程重放门禁、`Deterministic` 线程守卫。
- 每项都给出：**问题 → 数据流 → 主代码改动（函数/行/伪代码）→ 失败语义 → TDD 用例（含必杀变异）→ 验收**。
- 混沌测试单独设计：Verifier 进程组/多步故障、Raft 的 `SIGSTOP/SIGCONT/SIGKILL`；默认门禁跑「混沌-lite」，完整矩阵由 `RSC_CHAOS=1` 显式开启。
- 默认路径（新参数不开启）**事件字节、checkpoint 字节、CLI 输出保持不变**，由确定性用例和 conformance 双重守卫。
- 本方案已于 2026-10-29 评审通过；实施结果见 §9。

编号映射（本方案 → `docs/s24-boundary-optimization-plan.md` §2）：

| 本方案 | 边界清单 | 内容 |
|---|---|---|
| P1-1 | P1-6 | 语义签名/类成员 |
| P1-2 | P1-8 | Verifier 多步/诊断/include-exclude |
| P1-3 | P1-9 | 诊断 checkpoint/aging |
| P1-4 | P1-10 | 语料 I3/I9 |
| P1-5 | P1-7 | token 基准 n=5 |
| P1-6 | P1-11 | TLA I3/活性 |
| P2-1 | P2-12 | 多进程 Raft 故障 |
| P2-2 | P2-13 | 确定性 checkpoint |
| P2-3 | P2-14 | 跨进程重放门禁 |
| P2-4 | P2-15 | Deterministic 线程守卫 |

---

## 1. 评审修正（不采信子智能体原话的部分）

| # | 调研结论 | 我的核对与修正 |
|---|---|---|
| 1 | Verifier 的 delete 会假绿 | 成立，但根因更深：`self.h.workspace.manifest` **只含存活资源**（删除时 `manifest.pop`，见 [workspace.py](file:///workspace/harness/workspace.py#L335-L368)），所以 `_verification_tree` 根本走不到 `DELETE` 分支；`_overlay` 的 `content is DELETE`（[facade.py](file:///workspace/harness/facade.py#L613-L614)）对 manifest 路径不可达，对 pending 路径也直接 return。必须新增墓碑读取，而不是只改 `_overlay`。 |
| 2 | 语义检测只漏签名/成员 | 还漏了**删除可见性**：`_semantic_view` 的 `post` 以 `base` 为底（[facade.py](file:///workspace/harness/facade.py#L482-L492)），删除的文件永远留在 `post` 里，`removed_module` 在门面路径上几乎不可能触发。P1-1 必须同时修这个视图。 |
| 3 | 诊断要重复实现双 checksum | 不必。`FileCheckpointStore.load_strict` 已有 header+payload 双校验（[checkpoint.py](file:///workspace/harness/checkpoint.py#L970-L989)）；应抽成**模块级纯函数** `envelope_problems(doc)`，`load_strict` 与 `diagnose` 共用，避免两份逻辑漂移。 |
| 4 | `AGING_OVERDUE` 加 `hint` 字段 | `Finding` 没有 `hint` 字段（[diagnose.py](file:///workspace/harness/diagnose.py#L41-L54)）；为不改报告 schema，hint 放进 `locator={"hint": ...}`。 |
| 5 | 基准直接升 v2 覆盖旧 fixture | 会破坏现有文档的 `1.39×` 证据与回放契约。改为：**v1 保持默认**，新增 `llm_replay.n5.json` + `llm_replay.n5.md`；v2 只作为新证据，文档明确「v1 与 v2 不可直接比」。 |
| 6 | 语料加向量即可 | `scripts/check_docs_consistency.py` 强制 README 与 gap-analysis 含 `"{total} 条"`（[check_docs_consistency.py](file:///workspace/scripts/check_docs_consistency.py#L79-L86)）；当前是 `35 条`，加向量后必须同步文档，否则门禁红。 |
| 7 | TLA 活性直接塞进现有循环 | TLC 不能指名被违反的 temporal property，且 `broken` 判定只认 `Invariant <name> is violated`（[check_tla.py](file:///workspace/scripts/check_tla.py#L142-L155)）。活性必须单独分类：rc≠0 且输出含 `Temporal properties were violated`。 |
| 8 | WAL 的 `default=str` 已经是漏洞 | 目前只是**风险**：`json.dumps(..., default=str)`（[wal.py](file:///workspace/harness/storage/wal.py#L274-L275)）会把 set 变成 hash 序 repr。现有路径未发现 set 入事件，但不能靠「当前没发生」承诺跨进程重放；P2-3 改为显式规范编码。 |
| 9 | `Deterministic` 线程守卫会破坏现有用例 | 现有 `Harness` 已在构造期拒绝 `pool` 与 `merge_queue`（[harness.py](file:///workspace/harness/harness.py#L254-L255)、[facade.py](file:///workspace/harness/facade.py#L196-L199)），测试也没有跨线程使用熵源；守卫只在构造线程校验即可。 |
| 10 | I3/I9 需要新建两个 spec 模块 | 不需要。I3 可直接驱动真实的 `LeaseState`（[lease.py](file:///workspace/harness/storage/lease.py#L68)）；I9 可直接用生产用的 `_has_cycle`（[orchestration.py](file:///workspace/harness/orchestration.py#L70)）。语料执行真实实现，比再写一份参考实现更有价值。 |
| 11 | Raft 多进程可用 `isReady()` 判 leader | 不行。`RaftCluster.leader()` 用的就是 `isReady()`（[raft.py](file:///workspace/harness/storage/raft.py#L220-L226)），它只表示「初次同步完成」。多进程节点必须用 `SyncObj._isLeader()` / `_getLeader()`（本地 pysyncobj 0.3.14 已确认存在）。 |

---

## 2. 全局数据流

```text
                     ┌──────────────────────────────┐
 agent worktree ────>│ base 视图（旧快照，只读）      │
 (Z3, begin 时钉住)  └──────────────┬───────────────┘
                                    │
 ledger manifest ───┐               ▼
 (存活资源)          ├──> 合并视图：base → ledger 覆盖 → pending 覆盖 → 墓碑删除
 pending writes ─────┤               │
 workspace tombstones┘               ├──> Verifier 副本（include/exclude）→ 多步命令 → DENY/OK
                                     └──> 语义 post 视图 → ast facts → issues → block/warn

 ACB ──save──> FileCheckpointStore ──> refs/（header+payload 双 checksum）
                                     ├──> latest/ 指针
                                     └──> diagnose（只读）→ envelope/aging findings

 3 个 OS 进程 ──真 TCP──> pysyncobj RSM（lease + records）
                                     └──> append_record(stamp) 在 RSM 内复核 → 接受/拒绝
```

三条原则：

1. **读面不写**：`diagnose`、`verify`、`semantic` 不得构造 `Wal`/`FileCheckpointStore`，不得 `bump_epoch`。
2. **删除是一等公民**：manifest 看不到删除，必须读 `_tombstones`；否则 Verify/语义都会假绿。
3. **默认路径字节不变**：所有新能力默认关闭或保持旧分支；新字段只在显式开启时出现。

---

## 3. P1 实施项

### P1-1 语义检测：签名不兼容 + 类成员删除

#### 问题

当前 `detect` 只报 `removed_symbol` / `removed_module`（[semantic.py](file:///workspace/harness/semantic.py#L344-L370)）：

- 两个 agent 改不同文件，A 把 `f(a, b=1)` 改成 `f(a)`，B 的新代码仍调用 `f(a, b=2)` → 文本不冲突，`verify` 才炸；
- A 删掉 `class Cfg: debug`，B 的新代码写 `Cfg().debug` / `Cfg.debug` → 同样漏报；
- 门面的 `post` 视图保留被删除文件（见 §1-#2），`removed_module` 在真实门面路径上失效。

#### 数据流（改后）

```text
base bytes ──ast──> ModuleFacts{symbols, uncertain, dynamic, funcs, classes}
post bytes ──ast──> ModuleFacts{...}
                       │
                       ├─ removed_symbol / removed_module（旧逻辑）
                       ├─ signature_incompatible：post 调用点 × base/post FuncSig
                       └─ removed_member：post 成员引用 × base/post ClassFacts
```

#### 主代码改动

1. **`harness/semantic.py:139` 附近新增 facts 结构，保留 `_symbols` 薄包装**

```python
@dataclass(frozen=True)
class FuncSig:
    name: str
    posonly_required: tuple[str, ...]   # / 前的无默认值参数（不能按关键字传）
    posonly_optional: tuple[str, ...]   # / 前的有默认值参数
    poskw_required: tuple[str, ...]     # / 后、* 前的无默认值参数
    poskw_optional: tuple[str, ...]     # / 后、* 前的有默认值参数
    kwonly_required: tuple[str, ...]    # 无默认值的 kw-only
    kwonly_optional: tuple[str, ...]    # 有默认值的 kw-only
    varargs: bool
    varkw: bool
    opaque: bool                        # 有装饰器 / 含 *args/**kwargs 嵌套? 一律跳过

@dataclass(frozen=True)
class ClassFacts:
    name: str
    members: frozenset[str]           # 直接定义的方法/赋值
    uncertain: frozenset[str]         # if/try/循环体内定义
    has_bases: bool
    opaque: bool                      # 装饰器 / __getattr__ / __setattr__ / 动态

@dataclass(frozen=True)
class ModuleFacts:
    symbols: frozenset[str]
    uncertain: frozenset[str]
    dynamic: bool
    funcs: Mapping[str, FuncSig]
    classes: Mapping[str, ClassFacts]
```

- `_symbols(tree)` 保留为 `return facts.symbols, facts.uncertain, facts.dynamic`，已有调用点零改动。
- `FuncSig.from_ast`：用 `ast.arguments.posonlyargs/args/defaults/kwonlyargs/kw_defaults/vararg/kwarg`；`defaults` 对齐最后 N 个位置参数；`kw_defaults[i] is None` 表示必选。
- `ClassFacts.from_ast`：只扫类体**直接语句**；条件/异常/循环体内的成员进 `uncertain`；`has_bases = bool(node.bases)`；任意装饰器 → `opaque=True`。

2. **`harness/semantic.py:190` 的 `_Refs` 增三类采集**

```python
self.imported: dict[str, tuple[str, str]] = {}   # 本地名 -> (module, 原名)
self.calls: list[CallSite] = []
self.member_refs: list[MemberRef] = []
self.var_class: dict[str, tuple[str, str]] = {}  # cfg -> (module, Class)
```

- `visit_Import/ImportFrom`：记录 `from m import f as g` / `import m as a`。
- `visit_Call`：
  - `f(...)` 且 `f` 来自 `from m import f` 或同模块 `def f` → `CallSite(module, f, pos_count, kw_names, has_star, has_kwstar)`；
  - `m.f(...)` 且 `m` 是模块别名 → 同上；
  - `C().m` 的 `C` 若是导入类 → 记 `MemberRef(module, C, m)`。
- `visit_Attribute`：
  - `C.m`、`C().m`、唯一绑定 `cfg = Cfg()` 的 `cfg.m` → `MemberRef`；
  - 与现有模块别名 `a.x` 不冲突（按 `imported` 区分模块 vs 类）。
- `var_class` 预扫描：模块顶层每个名字只允许一次 `name = ClassName(...)`；二次赋值/参数同名 → 删除该绑定（保守）。

3. **`harness/semantic.py:294` `detect` 重排**

```python
b = view(base, "base"); p = view(post, "post")
base_facts = {m: _facts(tree) for m, (_, tree) in b.items()}
post_facts = {m: _facts(tree) for m, (_, tree) in p.items()}

# 旧：removed_symbol / removed_module（改为用 facts.symbols / uncertain / dynamic）
# 新 1：签名
for site in post_refs.calls:
    bs, ps = base_facts[site.module].funcs.get(site.name), post_facts[site.module].funcs.get(site.name)
    if not bs or not ps or bs.opaque or ps.opaque or site.has_star or site.has_kwstar:
        continue
    if _call_ok(bs, site) and not _call_ok(ps, site):
        add("signature_incompatible", site.module, site.name, site.site, hint=render(bs, ps))

# 新 2：类成员
for ref in post_refs.member_refs:
    bc = base_facts[ref.module].classes.get(ref.cls)
    pc = post_facts.get(ref.module, EMPTY).classes.get(ref.cls)
    if not bc or not pc or bc.opaque or pc.opaque or bc.has_bases or pc.has_bases:
        continue
    if ref.attr in bc.members and ref.attr not in pc.members \
            and ref.attr not in pc.uncertain and ref.attr not in bc.uncertain:
        add("removed_member", ref.module, f"{ref.cls}.{ref.attr}", ref.site)
```

`_call_ok(sig, site)` 的判定（保守）：

```python
pos_slots = (sig.posonly_required + sig.posonly_optional
             + sig.poskw_required + sig.poskw_optional)
required_pos = len(sig.posonly_required) + len(sig.poskw_required)
if not sig.varargs and site.pos_count > len(pos_slots):
    return False
if site.pos_count < required_pos and not sig.varargs:
    return False
# posonly 不能按关键字传；只有 pos-or-kw / kw-only 接受关键字
allowed_kw = set(sig.poskw_required + sig.poskw_optional
                 + sig.kwonly_required + sig.kwonly_optional)
if not sig.varkw and not set(site.kw_names) <= allowed_kw:
    return False
if not set(sig.kwonly_required) <= set(site.kw_names):
    return False
return True
```

**只报「base 调用合法、post 调用不合法」**：base 已经坏的调用永不报（历史问题交给 `verify`/CI）。

4. **门面删除视图修正（同属 P1-1，必须一起做）**

- `WorkspaceManager` 新增只读方法：

```python
def deleted_resources(self) -> list[str]:
    """当前被删除且未重建的资源（manifest 不含，tombstone 含）。"""
    return sorted(r for r in self._tombstones if r not in self.manifest)
```

- `RscSession._semantic_view`（[facade.py](file:///workspace/harness/facade.py#L467-L493)）：
  - 账本资源**无论 base 是否有**都写入 `post[str(resource)] = data`（新增文件也要参与调用点分析）；
  - 对每个 `deleted_resources()`：`post.pop(resource, None)`；
  - `pending` 中 `content is DELETE`：`post.pop(resource, None)`；
  - 这样 A 删模块、B 仍引用时，`removed_module` 才能真正触发；A 新增模块、B 引用时也不会漏扫。

#### 失败语义

- 只新增 issue kind，不抛异常；单文件解析失败仍进 `skipped`。
- `semantic="warn"`：写 `acb.spec["rsc_semantic"]`，不拒绝；`block`：`Deny(code="semantic_conflict")`（[facade.py](file:///workspace/harness/facade.py#L519-L529) 不变）。
- 装饰器、`*args/**kwargs`、有基类、`__getattr__`、星号导入、条件定义、同名重绑定、注解引用**一律跳过**（宁可漏报）。

#### TDD 用例（`tests/test_s24_semantic.py`）

| # | 用例 | 构造 | 期望 | 必杀变异 |
|---|---|---|---|---|
| 1 | `sig_keyword_removed` | base `f(a, b=1)` → post `f(a)`；post 调 `f(1, b=2)` | `signature_incompatible`，refs 行号精确 | 删掉「被删 kw」分支 → 红 |
| 2 | `sig_required_added` | post `f(a, b)`；post 调 `f(1)` | issue | 不检查必选位参 → 红 |
| 3 | `sig_kwonly_added` | post `f(a, *, c)`；调 `f(1)` | issue | 不检查必选 kw-only → 红 |
| 4 | `sig_optional_added_ok` | post `f(a, b=2)`；调 `f(1)` | 无 issue | 把可选当必选 → 红 |
| 5 | `sig_star_splat_ok` | 调 `f(*xs)` / `f(**kw)` | 无 issue | 不跳过星号 → 红 |
| 6 | `sig_decorated_ok` | `@overload` / 自定义装饰器 | 无 issue | 不跳过装饰器 → 红 |
| 7 | `member_call` | base `Cfg.debug` 删除；post `Cfg().debug` | `removed_member` | 只认 `C.m` 不认 `C().m` → 红 |
| 8 | `member_class_attr` | post `Cfg.debug` | issue | 漏 `C.m` → 红 |
| 9 | `member_local_instance` | 唯一 `cfg = Cfg()`；post `cfg.debug` | issue | 不做唯一绑定 → 红 |
| 10 | `member_inherited_ok` | `class Cfg(Base)` 删成员 | 无 issue | 不检查 bases → 红 |
| 11 | `member_dynamic_ok` | 类有 `__getattr__` | 无 issue | 不跳过动态 → 红 |
| 12 | `deleted_module_via_facade` | A 删 `app/b.py`，B 的 worktree 仍引用 `from app.b import x` | 门面报 `removed_module` | 不修 `_semantic_view` → 红（当前就是漏） |
| 13 | `perf_200_files` | 生成 200 个 py | `duration_ms < 2000` | 无（防回归） |

#### 验收

- 新用例全绿，现有 `test_s24_semantic.py` 不退化；
- 200 文件实测 < 2s（当前基线 266ms；2s 是宽松回归线，约为基线的 7.5 倍，避免 CI 抖动误杀）；
- `semantic` 默认仍是 `off`，门禁路径字节不变。

---

### P1-2 Verifier：多步、诊断、include/exclude（含删除修正）

#### 问题

- `verify` 每次 `copytree` 整棵树（[facade.py](file:///workspace/harness/facade.py#L589-L598)），大仓库慢；
- 只支持一条命令，无法「先 lint 再 test」；
- 失败只回 tail，没有复制耗时/文件数/单步耗时；
- **删除的账本资源不会从副本删除**：`manifest` 不含删除项（[workspace.py](file:///workspace/harness/workspace.py#L335-L368)），pending 的 `DELETE` 也被 `_overlay` 忽略（[facade.py](file:///workspace/harness/facade.py#L613-L614)）→ 脚本断言「文件不存在」会假绿。

#### 数据流（改后）

```text
worktree ──copytree(include/exclude)──> tree
tree ← ledger 存活资源 overlay(create=False)
tree ← workspace.deleted_resources() 逐个 unlink
tree ← pending overlay(create=True, DELETE→unlink)
tree ──step1──> step2──> step3（fail-fast；同一副本）
```

#### 主代码改动

1. **`WorkspaceManager` 新增 `deleted_resources()`**（同 P1-1，复用）。

2. **`RscSession.__init__` 新增构造参数**

```python
verify_include: tuple[str, ...] = (),
verify_exclude: tuple[str, ...] = (),
verify_diagnostics: bool = False,
```

校验：include 只允许字面相对路径/目录前缀；含 `*?[]`、`..`、绝对路径 → `ValueError`。

3. **`verify` 签名扩展（向后兼容）**

```python
def verify(self, token: str, cmd=None, *, timeout_s=None,
           steps=None, include=None, exclude=None) -> Result:
    ...
    return self._verify_locked(token, cmd, timeout_s,
                               steps=steps, include=include, exclude=exclude)
```

- `cmd` 与 `steps` 同时给 → `ValueError`；
- `steps` 每项经 `_norm_cmd`，空列表 → `ValueError`；
- 未给时回落构造参数 `verify_cmd`。

4. **`_verify_locked` 改为多步执行**

```python
argv_list = normalize(cmd, steps)          # list[list[str]]
tree = self._verification_tree(worktree, pending, include, exclude)
copy_ms = ...
for i, argv in enumerate(argv_list):
    started = monotonic()
    proc = Popen(argv, cwd=tree, start_new_session=True, ...)
    try:
        out, err = proc.communicate(timeout=timeout)
        code = proc.returncode
    except TimeoutExpired:
        _kill_process_group(proc); code = -1
    steps_detail.append({"cmd": argv, "exit_code": code,
                         "duration_ms": ..., "tail": ...})
    if code != 0:
        failed_step = i
        for rest in argv_list[i+1:]: steps_detail.append({"cmd": rest, "skipped": True})
        break
ok = all(s.get("exit_code") == 0 for s in steps_detail)
```

- `detail["exit_code"]`：全部成功为 0，否则为**首个失败步**的退出码；
- 单命令路径保留 `cmd`、`tail`、`duration_ms` 三个旧键，新增 `steps` 仅在 `steps` 非空时出现；
- `verify_diagnostics=True` 时额外加 `copy_ms`、`files`、`run_ms`；默认不出现（旧字节不变）。

5. **`_verification_tree` 支持 include/exclude 与删除**

```python
def _verification_tree(self, worktree, pending=None, include=None, exclude=None):
    holder = mkdtemp(...)
    tree = join(holder, "tree")
    shutil.copytree(worktree, tree, symlinks=True,
                    ignore=self._ignore_fn(include or self.verify_include,
                                           exclude or self.verify_exclude))
    for resource in list(self.h.workspace.manifest):
        # create=True：其他 agent 新增的已提交文件也必须进入合并视图，
        # 否则 B 的测试 import 不到新模块（假红）；_safe_rel 已限制在 tree 内。
        self._overlay(tree, resource, self.h.workspace.get(resource), create=True)
    for resource in self.h.workspace.deleted_resources():
        self._overlay(tree, resource, DELETE, delete=True)
    for resource, content in (pending or []):
        self._overlay(tree, str(resource), content, create=True)
    if include and not any(files): raise VerifyConfigError("include_empty")
    return tree
```

`_ignore_fn(include, exclude)` 规则（`copytree` 的 ignore 对**目录**也要正确返回，否则祖先目录不会被遍历）：

- include 非空时：对目录，若它是某个 include 路径的祖先（`include.startswith(dir + "/")`）→ **保留并继续遍历**；对文件，保留命中「文件 == 路径」或「文件以 `dir/` 开头」的路径；**自动保留**根配置 `pyproject.toml/setup.cfg/setup.py/pytest.ini/tox.ini/conftest.py/requirements*.txt/poetry.lock/uv.lock/Makefile`，以及命中文件所有祖先目录的 `__init__.py`；其余忽略。
- exclude：`fnmatch` 命中即忽略；若命中 `__init__.py` 或根配置 → 配置错误（fail-closed）。
- include 非空且复制后 0 文件 → `Deny("verify_include_empty", code="verify_config_invalid")`。
- **账本/pending/墓碑必须走同一过滤谓词**：否则 include 只缩小了 copytree，overlay 又把集合外的文件写回来，include 形同虚设。

6. **`_overlay` 增删除能力**

```python
def _overlay(tree, resource, content, *, create=False, delete=False):
    path = _safe_rel(tree, resource)
    if path is None: return
    if delete or content is DELETE:
        if os.path.isfile(path): os.unlink(path)
        return
    if not create and not os.path.isfile(path): return
    ...写文件...
```

#### 失败语义

| 场景 | 结果 |
|---|---|
| 第 i 步非零/超时 | `DENIED`，`failed_step=i`，后续步 `skipped`，`exit_code` 为该步退出码 |
| 起进程失败 | 该步 `exit_code=-1`，`tail=verify_spawn_failed`，fail-fast |
| include 非法/为空/exclude 命中保护路径 | `DENIED`，`code=verify_config_invalid` |
| 复制失败 | `DENIED`，`code=verify_copy_failed` |
| 所有步 0 | `OK`，写 `acb.spec["rsc_verify"]` |

#### TDD 用例（`tests/test_s24_verify.py` 扩展）

| # | 用例 | 构造 | 期望 | 必杀变异 |
|---|---|---|---|---|
| 1 | `steps_pass` | 两步都 0 | OK，`steps` 长度 2 | — |
| 2 | `steps_failfast` | 步 2 返回 3 | `failed_step=1`，步 3 `skipped` | 改成 continue → 步 3 执行，红 |
| 3 | `steps_shared_tree` | 步 1 写文件，步 2 读 | 步 2 读到 | 每步新副本 → 红 |
| 4 | `steps_timeout_group` | 步 1 起孙进程 sleep | 超时后孙进程消失 | 用 `run(timeout)` → 孙进程存活，红 |
| 5 | `ledger_delete` | A 已删 `app/b.py`，脚本断言不存在 | 红 | 不读 tombstone → 假绿 |
| 6 | `pending_delete` | pending 为 DELETE，同断言 | 红 | 忽略 DELETE → 假绿 |
| 6b | `ledger_create` | A 已提交新文件 `app/new.py`，B 的脚本 `import app.new` | 通过 | 账本 overlay `create=False` → 假红 |
| 7 | `include_init` | include `["app"]`，脚本 `import app` | 通过 | 不补 `app/__init__.py` → 红 |
| 8 | `include_root_config` | include `["app"]`，脚本读 `pyproject.toml` | 通过 | 不保留根配置 → 红 |
| 9 | `include_empty` | include `["nope"]` | `verify_config_invalid` | 放行 → 红 |
| 10 | `exclude_protected` | exclude `__init__.py` | 构造/verify 拒绝 | 放行 → 红 |
| 11 | `diag_fields` | 开关诊断 | 开：有 `copy_ms/files/run_ms`；关：detail 键集不变 | 默认加字段 → 红 |

#### 混沌/故障注入

- 进程组：步内起「子进程再起孙进程写 marker」，超时后断言 marker 不存在（覆盖现有 `test_s24_verify_pgroup.py`）。
- 多步：步 1 `touch`、步 2 断言、步 3 故意失败；断言步 3 不执行且副本被清理（临时目录不存在）。
- 大目录：生成 2000 文件，`include=["app"]` 后 `files` 显著下降且 `copy_ms` 可观测。

#### 验收

- 默认 `verify_cmd` 单命令路径的 detail 键集与字节输出不变；
- 删除场景从「假绿」变为「红」；
- `include` 默认关闭；文档标注「对数据文件/动态发现测试不安全」。

---

### P1-3 诊断：checkpoint 信封 + AGING_OVERDUE

#### 问题

`check_checkpoint` 只看 `store.json`/refs 是否存在（[diagnose.py](file:///workspace/harness/diagnose.py#L343-L376)）；坏 payload、checksum 篡改、aging 到期都不报，空转现场 `ok=True`。

#### 数据流

```text
refs/**/*.json ──> envelope_problems(doc) ──> CHECKPOINT_*_CHECKSUM finding
latest/*.json ──> ref ──> ref 文件 payload ──> state/aging_deadline/needs_readmit ──> AGING_OVERDUE
```

#### 主代码改动

1. **`harness/checkpoint.py` 抽出纯函数**（放在 `_HEADER_FIELDS` 附近，[checkpoint.py](file:///workspace/harness/checkpoint.py#L43-L52)）

```python
def envelope_problems(doc) -> tuple[str, ...]:
    """返回 ("header_checksum",) / ("payload_checksum",) / ()；旧扁平格式 → ()。"""
    if not FileCheckpointStore._is_envelope(doc):
        return ()
    header = {k: doc.get(k) for k in _HEADER_FIELDS}
    try:
        if _sha256(dumps(header).encode()) != doc.get("header_checksum"):
            return ("header_checksum",)
    except Exception:
        return ("header_checksum",)
    try:
        if _sha256(dumps(doc.get("payload")).encode()) != doc.get("payload_checksum"):
            return ("payload_checksum",)
    except Exception:
        return ("payload_checksum",)
    return ()
```

- `load_strict` 改为先 `problems = envelope_problems(doc)`，非空即 `raise CheckpointCorrupt(ref, header, problems[0])`；正常路径返回结果不变（现有 5 个用例守护）。

2. **`harness/checkpoint.py` 抽模块级 `ref_path(root, ref)`**（复用 `_REF_RE` 与 [checkpoint.py](file:///workspace/harness/checkpoint.py#L777-L780) 的切片规则），`FileCheckpointStore._ref_path` 改为委托它。

3. **`harness/diagnose.py` 新增两个 check**

```python
def check_checkpoint_envelopes(root: Path) -> Check:
    chk = Check("checkpoint.envelopes")
    for path in sorted((root / "refs").rglob("*.json")):
        if path.name.endswith((".part", ".corrupt")): continue
        doc = loads(path.read_text("utf-8"))        # 失败 → CHECKPOINT_UNREADABLE(error)
        problems = envelope_problems(doc)
        for p in problems:
            chk.add(Finding(f"CHECKPOINT_{p.upper()}", "error",
                            f"{path.name}: {p} 不匹配",
                            locator={"ref": path.stem}))
    return chk

def check_aging(root: Path, now_ms: int) -> Check:
    chk = Check("checkpoint.aging")
    for ptr in sorted((root / "latest").glob("*.json")):
        ref = pointer_ref(ptr)
        doc = read_ref(root, ref)                   # 复用 ref_path，只读
        payload = doc.get("payload", doc)
        if payload.get("state") != "ADMITTED": continue
        if payload.get("needs_readmit") or payload.get("superseded"): continue
        deadline = payload.get("aging_deadline")
        if isinstance(deadline, int) and 0 < deadline < now_ms:
            chk.add(Finding("AGING_OVERDUE", "warn",
                            f"ACB {payload['acb_id']} 超过 aging_deadline",
                            invariant="I12",
                            locator={"hint": "检查 tick()/pump()；aging_count=%d"
                                             % payload.get("aging_count", 0)}))
    return chk
```

- 在 `diagnose()` 中 `cp_root` 存在时把两个 check 都加入；`stats["refs"]` 不变。
- `Finding`/schema 不改；`--strict` 已支持 warn 退 1（[diagnose.py](file:///workspace/harness/diagnose.py#L459-L462)）。

#### 失败语义

| 情况 | 级别 |
|---|---|
| header/payload checksum 不匹配 | **error**（不可降级） |
| ref JSON 不可读/顶层非 dict | error `CHECKPOINT_UNREADABLE` |
| 旧扁平 payload（无 envelope） | 跳过，不报（兼容 S9-10） |
| `AGING_OVERDUE` | **warn**（活性停滞，不是损坏）；`--strict` 才退 1 |
| 非 ADMITTED / needs_readmit / superseded | 不报 |

#### TDD 用例（`tests/test_s24_diagnose.py` 扩展）

| # | 用例 | 构造 | 期望 | 必杀变异 |
|---|---|---|---|---|
| 1 | `payload_tamper` | 改 payload 一个字节 | `CHECKPOINT_PAYLOAD_CHECKSUM` error | 不校验 payload → 绿 |
| 2 | `header_tamper` | 改 header 字段 | `CHECKPOINT_HEADER_CHECKSUM` error | 不校验 header → 绿 |
| 3 | `legacy_flat_ref` | 手写旧扁平 ref | 无 checksum finding | 把旧格式当信封 → 红 |
| 4 | `ref_unreadable` | 写坏 JSON | `CHECKPOINT_UNREADABLE` error，其余 ref 继续 | 遇错即停 → 红 |
| 5 | `aging_idle` | `Harness(aging_ms=50)` begin 后不 tick；`diagnose(now_ms=deadline+1)` | `AGING_OVERDUE`+hint | 删 aging 检查 → 无 finding |
| 6 | `aging_terminal` | 已终态 ACB | 无 finding | 不筛 state → 红 |
| 7 | `aging_readmit` | `needs_readmit=True` | 无 finding | 不筛 → 红 |
| 8 | `read_only` | 诊断前后目录指纹 | 完全相同 | 构造 store → 可能写文件 |

#### 验收

- 空转现场必报 `AGING_OVERDUE`；
- 诊断前后 `_fingerprint`（[diagnose.py](file:///workspace/harness/diagnose.py#L477-L485)）不变；
- 现有 `test_s24_diagnose.py` 全绿。

---

### P1-4 语料：I3 fencing 单调 + I9 plan 无环

#### 问题

`manifest.totals.executable_invariants = ["I1","I13","I2"]`（当前 35 条）；I3/I9 只有 pytest 用例，没有可移植的纯函数向量。

#### 数据流

```text
vectors/fencing.json ──> _run_vector(kind="fencing.trace") ──> LeaseState.acquire/advance/authorizes
vectors/plan.json    ──> _run_vector(kind="plan.acyclic")  ──> orchestration._has_cycle / add_logical_edge
manifest.json        ──> totals + executable_invariants + sha256
```

#### 主代码改动

1. **`harness/spec/corpus.py`**
   - `_VECTOR_FILES` 增 `vectors/fencing.json`、`vectors/plan.json`；
   - 新增 `_fencing_vectors()` 与 `_plan_vectors()`；
   - `build_corpus()` 组装两个新文件；
   - `_run_vector` 增两个分支：

```python
if kind == "fencing.trace":
    state = LeaseState()
    epochs, outcomes, authorized = [], [], []
    for op in data["ops"]:
        if op["op"] == "acquire":
            g = state.acquire(owner=..., instance=..., now_ms=..., ttl_ms=...,
                              expected_epoch=op.get("expected_epoch"))
        elif op["op"] == "advance": ...
        epochs.append(g.epoch); outcomes.append(g.outcome)
        authorized.append(state.authorizes(owner=..., instance=..., epoch=..., now_ms=...))
    assert (epochs, outcomes, authorized) == tuple(expect[k] for k in (...))

if kind == "plan.acyclic":
    assert _has_cycle(data["edges"]) is expect["acyclic"]
```

2. **向量内容（最小但覆盖语义）**

`fencing.json`（I3）：

| id | ops | expect 要点 |
|---|---|---|
| `fencing.takeover` | acquire A@1（ttl 10）→ advance A→2 → acquire B expected=1 | `fenced`，epoch 仍 2；A@1 `authorizes=False` |
| `fencing.expire` | acquire A@0 ttl 10 → acquire B now=20 | B `acquired`，epoch 2，A@1 无效 |
| `fencing.renew` | acquire A@0 → renew A@1 | `renewed`，epoch 1 |
| `fencing.release` | acquire A@0 → release A@1 → acquire B@1 | B 新 epoch 2，A@1 `authorizes=False` |
| `fencing.stale_acquire` | acquire A@0 → acquire B expected=999 | `fenced`，epoch 仍 1 |
| `fencing.monotonic` | 连续 advance 三次 | epochs 严格 +1 |

`plan.json`（I9）：

| id | edges | acyclic |
|---|---|---|
| `plan.dag` | `a→b, a→c, b→d, c→d` | true |
| `plan.two_cycle` | `a→b, b→a` | false |
| `plan.self_loop` | `a→a` | false |
| `plan.unknown_endpoint` | `a→zz` | true（`_has_cycle` 忽略未知端点，由 `add_logical_edge` 单独 fail-fast） |
| `plan.duplicate_edge` | `a→b, a→b` | true |
| `plan.diamond` | `a→b, a→c, b→d, c→d`（重复端点） | true |

3. **`CORPUS_VERSION` `1.0.0 → 1.1.0`**（加向量 = minor）；`SPEC_VERSION` 不动。
4. **文档同步**：`README.md`、`docs/s24-gap-analysis-and-repositioning.md` 的「35 条」改为新总数（预计 47，以 `manifest.totals.vectors` 为准）；`scripts/check_docs_consistency.py` 会强制。
5. **`scripts/check_s24_conformance.py`** 无需改，`--check` 自动比对 sha/计数。

#### 失败语义

- 向量不符 → `run_corpus` 的 `errors` 非空、`--check` 退 1；
- 未知 kind 仍 fail-closed（现有分支最后 `raise`）；
- 向量执行不得引入 I/O/线程/时钟：`LeaseState` 纯状态机、`_has_cycle` 纯函数。

#### TDD 用例（`tests/test_s24_corpus.py` 扩展）

| # | 用例 | 期望 | 必杀变异 |
|---|---|---|---|
| 1 | `all_vectors_execute` | `passed > 0, failed == 0` | 新增 kind 未注册 → 红 |
| 2 | `i3_takeover_fenced` | `fenced` + epoch 不减 | 删 expected_epoch 校验 → 红 |
| 3 | `i3_stale_authorizes_false` | A@1 `authorizes=False` | 不校验 epoch → 红 |
| 4 | `i9_self_loop` | `acyclic=false` | 去掉环检查 → 红 |
| 5 | `i9_two_cycle` | `acyclic=false` | 只查自环 → 红 |
| 6 | `manifest_version` | `corpus_version=1.1.0`，totals 与文件数一致 | 只加向量不改 manifest → 红 |
| 7 | `docs_count` | `check_docs_consistency.py` 0 findings | 不同步条数 → 红 |
| 8 | `deterministic_build` | 同一代码两次 `build_corpus()` 字节相等 | 引入非确定顺序 → 红 |

---

### P1-5 token 基准：n=5、thinking 开关、描述性统计

#### 问题

`capture_live` 每个条件 n=1（[bench/llm_replay.py](file:///workspace/bench/llm_replay.py#L112-L210)），请求体没有 thinking 开关（[llm.py](file:///workspace/harness/llm.py#L251-L257)），不记 `finish_reason`；实测同一模型 `max_tokens=8` 仍耗 100+ completion（thinking 计入）。

#### 数据流

```text
live: capture_live(n=5) ──> chat_with_usage(extra={"enable_thinking": false})
                         ──> LLMReply{usage, finish_reason}
                         ──> fixture v2 {samples:[...]}
replay: load_fixture → _normalize(v1|v2) → validate_sample → aggregate → median/range → report
```

#### 主代码改动

1. **`harness/llm.py`**
   - `LLMReply` 增 `finish_reason: str = ""`（[llm.py](file:///workspace/harness/llm.py#L158-L189)），`as_dict()` 输出；
   - `chat` / `chat_with_usage` 增 `extra: Mapping[str, Any] | None = None`；
   - payload 组装：

```python
payload = {"model": cfg.model, "messages": list(messages), "temperature": temperature}
if max_tokens is not None: payload["max_tokens"] = max_tokens
if extra:
    forbidden = {"model", "messages"}
    if forbidden & set(extra): raise ValueError("extra 不得覆盖 model/messages")
    payload.update(extra)
```

   - 响应：`finish_reason = data["choices"][0].get("finish_reason", "")`，落 `LLMReply`。

2. **`bench/llm_replay.py`**
   - `SCHEMA_VERSION = 2`；
   - `capture_live(*, via_proxy=False, n=5, enable_thinking=False, max_tokens=64)`：循环 n 次，每次独立 temp repo/session；三处 `chat_with_usage` 传 `extra={"enable_thinking": False}`（若 `enable_thinking=True` 则不传）；
   - 返回 v2：

```json
{"schema_version": 2, "scenario": "...", "provenance": "live",
 "model": "...", "request": {"temperature": 0.0, "max_tokens": 64,
                              "enable_thinking": false},
 "samples": [{"sample_id": "s0", "turns": {...}, "arms": {...}}, ...]}
```

   - `_normalize(doc)`：v1 → 包成单样本（`request` 取顶层字段），下游统一按 samples 处理；
   - `validate_fixture` 接受 `schema_version ∈ {1, 2}`（v1 走旧校验分支）；逐样本校验；以下样本判无效并剔除：
     - 任一 turn `usage_missing`；
     - `total_tokens != prompt+completion`；
     - `finish_reason == "length"`；
     - 全部样本无效 → `ValueError`；
   - `compute_metrics`：
     - n=1 时输出与现有完全相同的键（`sample_size`、`conditions`、`token_multiplier`、`rework_rate_delta`）；
     - n>1 时新增 `n_valid/n_rejected/rejected[]`，以及 `token_multiplier`、两臂 `total_tokens`、`rework_rate` 的 `median/min/max/range`；固定文案「描述性统计，不作显著性结论」；
   - CLI：`--n`（默认 5）、`--enable-thinking`、`--max-tokens`（默认 64）、`--out` 仍写 v2。

3. **fixture 与文档策略**
   - `bench/fixtures/llm_replay.json`（v1）**保持默认**，现有 `pytest` 与文档的 `1.39×` 不动；
   - 新增 `bench/fixtures/llm_replay.n5.json`（v2）与 `bench/llm_replay.n5.md`，用真实调用录制；
   - 文档新增一段：v2 是「thinking 关闭 + n=5」的新证据，**与 v1（thinking 默认 + n=1）不可直接比**；同时更新 `docs/s7-benchmark-and-coverage.md` 与 `docs/s24-gap-analysis-and-repositioning.md` §5.10 的引用路径。
   - 若录制时 `reasoning_tokens > 0`（模型不可关 thinking）→ 拒绝出数、不提交 fixture，报告写明原因。

#### 失败语义

| 场景 | 行为 |
|---|---|
| 无 key | `skipped`，退 0（`--require-live` 退 3），不落文件/数字（现有路径） |
| `usage_missing` | 该样本无效；全部无效 → 非 0，无数字 |
| `finish_reason="length"` | 该样本无效（截断不代表真实成本） |
| `extra` 撞 `model/messages` | `ValueError` |
| `enable_thinking=False` 但 `reasoning_tokens>0` | capture 抛错，不产出数值 |
| n=1 | 不输出 median/range，旧键不变 |

#### TDD 用例（`tests/test_s24_llm_usage.py` / `tests/test_s24_replay.py` 扩展）

| # | 用例 | 期望 | 必杀变异 |
|---|---|---|---|
| 1 | `extra_thinking_false` | monkeypatch `_post` 捕获 payload | 顶层 `enable_thinking=false` | 删 extra 透传 → 红 |
| 2 | `extra_reserved_key` | `extra={"model": "x"}` | `ValueError` | 不设白名单 → 红 |
| 3 | `finish_reason_length` | 响应 `length` | `LLMReply.finish_reason == "length"`，`as_dict` 含 | 不解析 → 红 |
| 4 | `length_sample_rejected` | 5 样本中 1 个 length | `n_valid=4`，该样本不进聚合 | 计入 → 红 |
| 5 | `usage_missing_sample` | 样本 2 缺 usage | 只剔样本 2 | 整批拒绝 → 红 |
| 6 | `v1_backward_compat` | 现有 v1 fixture | 归一 1 样本，数值与旧断言一致 | 破坏 v1 → 红 |
| 7 | `v2_median_range` | 5 样本固定值 | median/min/max 精确；文案含「描述性」 | 换文案/漏 keys → 红 |
| 8 | `live_no_key` | 无 key `--n 5` | skipped、无数字、退 0/3 | 落假数字 → 红 |
| 9 | `cli_n1` | `--n 1` | 无区间键 | 无条件输出区间 → 红 |

#### 验收

- 离线测试全绿，不需要网络/key；
- v2 录制成功且所有样本 `reasoning_tokens==0`、无 `length`；
- 报告同时给逐样本值与中位数/极差，不出现「显著」字样。

---

### P1-6 TLA：I3 fencing 单调 + I1 活性

#### 问题

现有 4 个模型只查安全（`spec/tla/README.md` 第 108-116 行），缺 I3（租约簇核心），也没有活性/公平性。

#### 主代码改动

1. **新增 `spec/tla/I3Fencing.tla` + 两个 cfg**

```tla
EXTENDS Naturals, TLC
CONSTANTS Writers, MaxEpoch, EnforceFence
VARIABLES epoch, prevEpoch, leaseEpoch, staleCommits

Init == epoch = 0 /\ prevEpoch = 0 /\ leaseEpoch = [w \in Writers |-> 0] /\ staleCommits = 0

Grant(w) ==
    /\ leaseEpoch' = [leaseEpoch EXCEPT ![w] = epoch]
    /\ UNCHANGED <<epoch, prevEpoch, staleCommits>>

Fence ==
    /\ epoch < MaxEpoch
    /\ prevEpoch' = epoch
    /\ epoch' = epoch + 1
    /\ UNCHANGED <<leaseEpoch, staleCommits>>

Commit(w) ==
    /\ prevEpoch' = epoch
    (* EnforceFence=TRUE：旧 epoch 被拒，不计数；FALSE：旧 epoch 被接受，计数 *)
    /\ staleCommits' = staleCommits +
         (IF ~EnforceFence /\ leaseEpoch[w] # epoch THEN 1 ELSE 0)
    /\ UNCHANGED <<epoch, leaseEpoch>>

Next == \E w \in Writers : Grant(w) \/ Commit(w) \/ Fence

I3FencingMonotonic == epoch >= prevEpoch /\ staleCommits = 0
```

- `I3Fencing.cfg`：`EnforceFence = TRUE`；
- `I3Fencing.broken.cfg`：只改 `EnforceFence = FALSE`，走 `Grant(epoch0) → Fence → Commit`，`staleCommits=1` 必被找到。

2. **`spec/tla/I1SingleWriter.tla` 追加活性**

```tla
FairSpec == Spec /\ \A w \in Writers :
    WF_vars(Reserve(w)) /\ WF_vars(EnterCrit(w)) /\ SF_vars(Commit(w))

EventuallyCommitted == <>(\E w \in Writers : pc[w] = "committed")
```

- `I1SingleWriter.liveness.cfg`：`SPECIFICATION FairSpec` + `PROPERTY EventuallyCommitted`；
- `I1SingleWriter.unfair.cfg`：`SPECIFICATION Spec` + 同 `PROPERTY`，必须被 TLC 找到反例（所有写者永远 `outside`）。
- 为什么用无条件 `<>`：`~>` 带前提在非公平执行下会真空成立，测不出问题。
- **为什么 `Commit` 必须用 `SF_vars` 而不是 `WF_vars`**：模型允许 `Abort` 无限抢占，`Commit` 不是连续使能；弱公平无法强制提交。已用 TLC 实测：`WF_vars(Commit)` 的「正确」配置也会产生反例（crit→Abort 循环），改成 `SF_vars(Commit)` 后 fair cfg `exit=0`，unfair cfg `exit=13` 且输出 `Temporal properties were violated`。

3. **`scripts/check_tla.py`**

- `CASES` 增 `("I3", "I3Fencing", "I3FencingMonotonic")`（[check_tla.py](file:///workspace/scripts/check_tla.py#L32-L37)）；
- 新增 `PROPERTY_CASES`（I1 liveness correct / unfair）；
- 抽出 `_classify(rc, out, kind, name)`：
  - safety broken：`rc != 0 and f"Invariant {name} is violated" in out`；
  - property correct：`rc == 0`；
  - property broken：`rc != 0 and "Temporal properties were violated" in out` 且不含 `Invariant ... is violated`；
- `--only` 的 choices 合并两组；
- 默认 timeout 120s（活性比安全慢）；若实测 > 30s，改为 `--liveness` 显式开关并将默认门禁切到「安全 + 无活性」，**不静默跳过**。

4. **文档**：`spec/tla/README.md` 标题/表格/运行命令更新为 5 个安全模型 + 1 组活性；说明 TLC 不能指名 property。

#### 失败语义

| 场景 | 行为 |
|---|---|
| 无 jar/Java | SKIP + 退 0；`--require` 退 2（现有语义） |
| 正确模型非 0 | FAIL 退 1 |
| broken 未检出指定 invariant | FAIL 退 1 |
| unfair 未检出 temporal 违反 | FAIL 退 1 |
| TLC 超时 | FAIL（不是 SKIP） |

#### TDD 用例（`tests/test_s24_tla.py` 扩展）

| # | 用例 | 期望 | 必杀变异 |
|---|---|---|---|
| 1 | `i3_assets` | 三个文件存在，broken 仅一行不同 | 资产缺失 → 红 |
| 2 | `i3_correct` | `I3Fencing.cfg` 退 0 | — |
| 3 | `i3_broken` | 退非 0 且含指定 invariant | 去掉 epoch 守卫 → 红 |
| 4 | `i1_liveness_correct` | `FairSpec` 退 0 | 把 `SF_vars(Commit)` 改回 `WF_vars` → 红（已实测） |
| 5 | `i1_unfair_red` | `Spec` 退非 0 且含 temporal 标记 | 把 `<>` 改成带前提 `~>` → 红 |
| 6 | `classify_no_cross` | 两段真实输出 | safety/property 不互判 | 正则过宽 → 红 |
| 7 | `no_jar_skip` | 无 jar | SKIP 0 | 报错退 1 → 红 |
| 8 | `timeout_fail` | `--timeout 0.01` | FAIL，不是 SKIP | 把超时当 SKIP → 红 |

---

## 4. P2 实施项

### P2-1 多进程 Raft 故障注入（3 OS 进程 + 真 TCP）

#### 问题

现有 `RaftCluster` 是**单进程 3 个 SyncObj**（[raft.py](file:///workspace/harness/storage/raft.py#L146-L200)），没有真正验证「进程被暂停/杀死后旧 epoch 被拒」。沙箱无 `CAP_NET_ADMIN`，不能做网络分区；但可以做进程级故障。

#### 拓扑

```text
pytest 父进程
  ├── Popen(node0) 127.0.0.1:base+0  data_dir/node0  (stdin/stdout JSON-lines)
  ├── Popen(node1) 127.0.0.1:base+1  data_dir/node1
  └── Popen(node2) 127.0.0.1:base+2  data_dir/node2
      每个子进程：SyncObj(autoTick=True, fast timeouts) + _ReplicatedLeaseLog
```

- 地址必须在启动时固定（`dynamicMembershipChange=False`），不支持动态发现；
- 每节点独立 `data_dir/node{i}`；复用目录会带入旧日志；
- 端口用 `pick_free_base(45_100, 3)` 在父进程探测后传给子进程（[raft.py](file:///workspace/harness/storage/raft.py#L75-L87)）。

#### 主代码改动

1. **新增 `harness/storage/raft_node.py`**

```python
def node_conf(addr, partners, data_dir, index, *, fast=True, password="harness-raft"):
    return SyncObjConf(
        autoTick=True,
        appendEntriesPeriod=0.02,
        raftMinTimeout=0.12, raftMaxTimeout=0.24,
        connectionTimeout=0.30, connectionRetryTime=0.05,
        leaderFallbackTimeout=0.60, bindRetryTime=0.05, maxBindRetries=3,
        journalFile=f"{data_dir}/node{index}.journal",
        fullDumpFile=f"{data_dir}/node{index}.dump",
        password=password,
        dynamicMembershipChange=False,
    )

class RaftNode:
    """把一个 _ReplicatedLeaseLog 暴露成 JSON-lines 服务。"""
    def serve_stdio(self): ...
```

2. **JSON-lines 命令协议**（stdin 一行一个 JSON，stdout 一行一个回包）

| 命令 | 参数 | 回包 |
|---|---|---|
| `status` | — | `{leader, term, epoch, lease, records, rejected, ready}` |
| `acquire` | owner/instance/now_ms/ttl_ms/expected_epoch | `{lease: LeaseGrant, epoch}`（**应用后**才回包） |
| `renew` | owner/instance/epoch/now_ms/ttl_ms | 同上 |
| `release` | owner/instance/epoch | 同上 |
| `advance` | owner/instance/epoch/now_ms/ttl_ms | 同上 |
| `append` | rec, stamp（可选） | `{records, rejected}`（应用后） |
| `shutdown` | — | `{ok: true}` 后退出 |

- `status.leader` 用 `self._isLeader()`，**不用** `isReady()`；
- **写命令必须等「已应用」再回包**：子进程内 `before = lease_version`（或 `len(records)`），提交后 `wait_until(> before, timeout)`；超时回 `{error: "not_applied"}`。否则父进程会读到旧 `lease_last`（原型已实测：立刻回包时 `lease={}`、`epoch=0`）。
- 父进程维护 `alive` 集合：**绝不向 `SIGSTOP`/`SIGKILL` 的节点发命令**，否则 `readline` 永久阻塞（原型踩到）；`status` 用 `select` + 超时，或每节点一个读线程。
- 原型实测：3 进程 + 真 TCP 在约 **0.4s** 内选出唯一 leader；本沙箱可行。

3. **新增 `tests/_raft_proc.py`**：只做参数解析 + `RaftNode(...).serve_stdio()`，便于子进程启动。
4. **新增 `tests/test_s24_raft_chaos.py`**：父进程编排；默认门禁跑「混沌-lite」，完整矩阵由 `RSC_CHAOS=1` 开启。
5. **`harness/storage/raft.py` 的小修**：新增只读 `leader_is()` 或文档注明 `RaftCluster.leader()` 语义；多进程路径不复用它。若不想改公共 API，测试直接读 `status`。

#### 混沌测试设计

| 用例 | 信号序列 | 断言 | 预算 |
|---|---|---|---|
| `topology_ready` | 启动 3 进程 | 3s 内全部 ready 且恰有 1 leader | 3s |
| `sigstop_follower_catchup` | SIGSTOP f1 → leader append → SIGCONT | f1 `records` 追平；旧 stamp 不被接受 | 停 1s + 追平 2s |
| `sigkill_leader_reelect` | SIGKILL leader → 等新 leader | `term` 增大；新 leader `epoch` 不倒退 | 3s |
| `stale_epoch_rejected` | 接管后用旧 stamp `append` | `rejected` +1，`records` 不变 | 1s |
| `lease_epoch_monotonic_failover` | 旧租约过期后 `acquire expected=old` | 返回 `old+1`，旧 epoch `authorizes=False` | 1s |
| `sigstop_leader_steps_down` | SIGSTOP leader → 新 leader → SIGCONT | 旧节点不再 `leader`，且追平 | 3s |
| `shutdown_clean` | 全量 shutdown | 无存活/僵尸子进程 | 5s |

- 全部用 `time.monotonic()` 与 `wait_until` 轮询，不 sleep 猜时长；
- 失败时父进程 `killpg` 清场，打印每个节点最后 20 行 stdout/stderr；
- 诚实口径写进测试 docstring：**单主机、共享内核时钟、localhost TCP、无丢包/重排**；信号暂停不是网络分区。

#### 失败语义

- 缺 `pysyncobj` → 整个模块 `xfail`（不是 skip 假绿）；
- bind 失败/子进程提前退出 → 父进程读 stderr 后 fail；
- 3s 内无 leader → fail；
- `SIGSTOP` 期间内核保持 TCP 连接，判死由 `connectionTimeout=0.30s` 与选举 `0.12–0.24s` 决定。

#### 验收

- 默认门禁：`topology_ready` + `stale_epoch_rejected`（约 5-10s）；
- `RSC_CHAOS=1 pytest tests/test_s24_raft_chaos.py`：7 条全绿；
- 现有 `tests/test_s16_raft.py` / `test_s18_raft_rsm.py` / `test_s24_raft_lease.py` 不退化。

---

### P2-2 确定性 checkpoint（entropy/clock 注入）

#### 问题

`store_uuid`、ref 后缀、`created_at_ms` 用 `uuid4`/`time.time()`（[checkpoint.py](file:///workspace/harness/checkpoint.py#L560)、[checkpoint.py](file:///workspace/harness/checkpoint.py#L919-L920)、[checkpoint.py](file:///workspace/harness/checkpoint.py#L1204-L1206)），同一 seed 场景无法逐字节重放。

#### 主代码改动

```python
class FileCheckpointStore(CheckpointStore):
    def __init__(self, root, *, entropy=None, clock=None):
        self._entropy = entropy          # 必须先于 _init_meta()
        self._clock = clock
        ...                              # 之后才 self._init_meta()

    def _new_store_uuid(self) -> str:
        if self._entropy is not None:
            return self._entropy.uuid("checkpoint_store_uuid")
        return uuid.uuid4().hex

    def _new_ref_suffix(self) -> str:
        if self._entropy is not None:
            return self._entropy.uuid("checkpoint_ref")[:12]
        return uuid.uuid4().hex[:12]

    def _now_ms(self) -> int:
        if self._clock is not None:
            return int(self._clock.now_ms())
        return int(time.time() * 1000)
```

- 替换 `:560`、`:919`、`:920`、`:1204`、`:1206`；
- `Harness` 构造处（[harness.py](file:///workspace/harness/harness.py#L395-L398)）改为：
  ```python
  FileCheckpointStore(checkpoint_root, entropy=entropy, clock=self.clock)
  ```
- `store.json` 已存在时**不重算** uuid（沿用现有 `_init_meta` 分支）；
- 默认 `entropy=None, clock=None` 时表达式与旧版逐字相同（`SystemClock.now_ms()` 就是 `int(time.time()*1000)`，[clock.py](file:///workspace/harness/clock.py#L18-L20)）。

#### 验收

- fresh root + 同 seed + 同调用序列 → 两次目录的 relpath 集合与每个文件 sha256 完全相等；
- 默认构造 → 现有测试字节不变；
- 生成 id/time 在写 meta 之前完成，异常时不留空 seq。

#### TDD 用例（`tests/test_s24_determinism.py` 扩展）

| # | 用例 | 期望 | 必杀变异 |
|---|---|---|---|
| 1 | `same_seed_tree_bytes` | 两 fresh root 同 seed，目录/字节全等 | 换回 uuid4 → 红 |
| 2 | `default_unchanged` | monkeypatch uuid4/time，默认构造走旧表达式 | 强制熵源 → 红 |
| 3 | `store_uuid_domain` | 等于 `ent.uuid("checkpoint_store_uuid")` | 不接熵源 → 红 |
| 4 | `ref_suffix_domain` | 后缀等于 `ent.uuid("checkpoint_ref")[:12]` | 用 uuid4 → 红 |
| 5 | `created_at_clock` | 固定 tick 时钟 → header 时间精确 | 用 time.time → 红 |
| 6 | `harness_wiring` | `Harness(entropy, checkpoint_root)` 跑两遍字节相等 | 漏传 entropy → 红 |
| 7 | `entropy_error_fail_closed` | 熵源抛错 | 无 ref 文件，不回落 | catch 后 uuid4 → 红 |
| 8 | `different_seed_differs` | 不同 seed | 目录不同（控制组） | — |

---

### P2-3 跨进程重放门禁（PYTHONHASHSEED 矩阵 + WAL 规范编码）

#### 问题

当前确定性只承诺单进程（`entropy.py` 文档第 15-16 行）；`wal.py` 用 `default=str`，set/frozenset 会按 hash 序落成 repr，跨进程不稳定。

#### 主代码改动

1. **`harness/storage/wal.py` 用规范编码替换 `default=str`**

```python
def _canon_default(obj):
    if isinstance(obj, (set, frozenset)):
        return sorted(obj, key=lambda x: json.dumps(x, sort_keys=True, default=_canon_default))
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, bytes):
        return obj.hex()
    raise TypeError(f"不可规范序列化：{type(obj).__name__}")
```

- `json.dumps(..., default=_canon_default)`；
- 先跑全量测试确认没有依赖 `str(obj)` 的 payload；若有，补显式转换（不扩大 `default` 白名单）。
- `harness/storage/blob.py` 已有严格 codec，不重复改。

2. **新增 `tests/_s24_hash_child.py`**
   - 固定场景：`Deterministic(seed)` + `WalEventLog` + `Harness(checkpoint_root)`，执行 begin/apply/finish；
   - 输出 `{"wal_sha256": ..., "checkpoint_tree_sha256": ...}`；
   - 树摘要：排序后的 `relpath + sha256` 再哈希。

3. **新增 `tests/test_s24_cross_process_replay.py`**
   - 父进程用 `subprocess.run(..., env={**os.environ, "PYTHONHASHSEED": seed})` 跑 child，seed ∈ `{"0","1","2","random"}`，每个 seed 跑 2 次；
   - 断言：同一 seed 两次摘要相同；**所有 seed 摘要也相同**（规范编码后不应依赖 hash seed）；
   - 先跑一个探针：子进程输出 `hash("probe")`，确认 seed 0/1 不同（证明环境变量真的生效）。

#### 失败语义

- 摘要不等 → fail，并打印首个差异 relpath 与两侧 sha；
- 子进程超时/非零 → fail，禁止 skip；
- 不可序列化对象 → `TypeError`（fail-closed），不再静默 `str()`。

#### TDD 用例

| # | 用例 | 期望 | 必杀变异 |
|---|---|---|---|
| 1 | `hashseed_probe` | seed 0≠1 的 `hash("probe")` | 不传 env → 红 |
| 2 | `same_seed_wal` | 同 seed 两次 WAL 摘要相等 | 非确定 id → 红 |
| 3 | `all_seeds_equal` | 0/1/2/random 摘要全等 | set 入 WAL → 红 |
| 4 | `set_payload_canonical` | payload 含 set，跨 seed 字节稳定 | 恢复 `default=str` → 红 |
| 5 | `unserializable_fail_closed` | 自定义对象 | `TypeError` | `str(obj)` → 红 |
| 6 | `checkpoint_cross_seed` | 同 seed 的 checkpoint 树摘要相等 | 未排序集合 → 红 |

---

### P2-4 `Deterministic` 线程守卫

#### 问题

`Deterministic` 只有 `RLock`，不记录所有者（[entropy.py](file:///workspace/harness/entropy.py#L41-L99)）；跨线程调用会破坏「同一调用序列」承诺，但当前静默通过。

#### 主代码改动

```python
class Deterministic:
    __slots__ = (..., "_owner", "_owner_name")

    def __init__(...):
        ...
        self._owner = threading.get_ident()
        self._owner_name = threading.current_thread().name

    def _assert_owner(self) -> None:
        ident = threading.get_ident()
        if ident != self._owner:
            raise ValueError(
                f"Deterministic 只能在构造线程使用："
                f"owner={self._owner_name}({self._owner}) current="
                f"{threading.current_thread().name}({ident})；"
                "并发场景请使用 scheduler_mode='sync' 或不要注入 entropy")

    def uuid(self, domain): self._assert_owner(); ...
    def token(self, domain): self._assert_owner(); ...
    def now_ms(self): self._assert_owner(); ...
```

- `_next` 只由 `uuid/token` 调用，不重复断言；
- 构造期记录、调用期校验（避免「首个调用线程漂移」的歧义）；
- 抛错后计数器/时钟都不前进。

#### TDD 用例（`tests/test_s24_determinism.py` 扩展）

| # | 用例 | 期望 | 必杀变异 |
|---|---|---|---|
| 1 | `owner_recorded` | 构造线程正确 | 不记录 owner → 红 |
| 2 | `cross_thread_uuid` | 子线程调用 | `ValueError`，counter 不变 | 删守卫 → 红 |
| 3 | `cross_thread_now_ms` | 子线程调用 | `ValueError`，ticks 不变 | 删守卫 → 红 |
| 4 | `construct_in_worker_ok` | 在 worker 构造并使用 | 正常 | 误用主线程 id → 红 |
| 5 | `error_message` | 两条线程名都在消息里 | 只报 id → 红 |
| 6 | `pool_rejected` | `Harness(entropy, scheduler_mode="pool")` | 配置期 `ValueError` | 删配置检查 → 红 |
| 7 | `merge_queue_rejected` | 带熵源 + merge_queue | 配置期 `ValueError` | 删检查 → 红 |

---

## 5. 测试与门禁总表

| 层级 | 命令 | 何时跑 | 依赖 |
|---|---|---|---|
| 主门禁 | `pytest -q` | 每次提交 | 无网络/key/Java/pysyncobj（Raft 用例缺依赖 xfail） |
| TLA | `TLA2TOOLS_JAR=/tmp/tla2tools.jar pytest tests/test_s24_tla.py -q` | P1-6 及之后 | Java + jar |
| 语料 | `python3 scripts/check_s24_conformance.py` | 每次提交 | 无 |
| 文档 | `python3 scripts/check_docs_consistency.py` | P1-4/P1-5 及之后 | 无 |
| 混沌-lite | 默认门禁内的 Raft 2 条 | 每次提交 | pysyncobj |
| 混沌-full | `RSC_CHAOS=1 pytest tests/test_s24_raft_chaos.py -q` | 手动/CI 专用 job | pysyncobj |
| 跨进程重放 | `pytest tests/test_s24_cross_process_replay.py -q` | 每次提交 | 无（子进程） |
| 真实基准 | `python3 -m bench.llm_replay --live --via-proxy --n 5 ...` | 手动，产物提交 | key + 代理 |

**测试设计方法**：每条新用例都写「构造 → 期望 → 必杀变异」；必杀变异必须在实现中能被一条用例杀死，否则该用例只是覆盖率装饰。

**禁止假绿**：

- 缺 pysyncobj → `xfail` 并注明 `requires-pysyncobj`；
- 缺 Java/jar → `skip`/`--require` 退 2；
- 无 key → `skipped`，不落数字；
- 混沌-full 默认不跑，但 CI 有独立 job，且文档写明「未跑 = 未验证」，不宣称已覆盖。

---

## 6. 实施顺序与提交拆分

| 顺序 | 提交 | 内容 | 前置 |
|---|---|---|---|
| 1 | `feat(s24-p1): 语义签名/类成员 + 删除视图` | P1-1 | 无 |
| 2 | `feat(s24-p1): Verifier 多步/诊断/include-exclude + 墓碑删除` | P1-2 | 复用 P1-1 的 `deleted_resources()` |
| 3 | `feat(s24-p1): diagnose 校验 checkpoint 信封与 aging` | P1-3 | 无 |
| 4 | `feat(s24-p1): 语料 I3/I9 向量 + 文档计数` | P1-4 | 无 |
| 5 | `feat(s24-p1): LLM 基准 n=5/thinking/finish_reason` | P1-5 | 离线用例先行；live 录制最后 |
| 6 | `feat(s24-p1): TLA I3 + I1 活性` | P1-6 | jar 可用 |
| 7 | `feat(s24-p2): 多进程 Raft 故障注入` | P2-1 | pysyncobj |
| 8 | `feat(s24-p2): 确定性 checkpoint` | P2-2 | 无 |
| 9 | `feat(s24-p2): 跨进程重放门禁 + WAL 规范编码` | P2-3 | 无 |
| 10 | `feat(s24-p2): Deterministic 线程守卫` | P2-4 | 无 |

每个提交都必须独立可回滚、独立全绿；不允许「先红后绿」的中间提交留在 main。

---

## 7. 风险与明确不做

| 风险/边界 | 处理 |
|---|---|
| 语义检测误报（duck typing、死代码、re-export） | 只报显式导入/同模块定义；装饰器/基类/动态一律跳过；默认 `off`，建议先 `warn` |
| include 缩小复制集合导致假绿 | include 默认关闭；自动补 `__init__.py` 与根配置；空集合直接 Deny；文档标注不安全场景 |
| v2 基准依赖真实调用/额度 | 离线用例先绿；录制失败不提交假数字；v1 保留为历史证据 |
| thinking 无法关闭的模型 | 录制后校验 `reasoning_tokens==0`，否则拒绝出数 |
| TLA 活性慢/噪声 | 固定 2 写者/1 资源/MaxVer=3；120s 超时；超 30s 改 `--liveness` |
| Raft 多进程时序抖动 | 预算 3s/3s/2s；`wait_until` 轮询；失败清场并打印子进程日志 |
| 跨进程 WAL 规范编码破坏旧 payload | 先跑全量；只对 set/Enum/bytes 显式处理，其余 `TypeError` |
| 线程守卫误伤 | 先审计现有用例；构造期已拒绝 pool/merge_queue；错误消息给出线程名 |
| per-resource 租约分片 | **不做**（与全局 epoch/单写者收据链冲突） |
| Verifier 替代 CI / 逻辑资源视图 | **不做**（无稳定路径映射会假绿） |
| 动态引用/多语言语义 | **不做**（需类型推断/运行时） |
| TLA refinement 证明 | **不做**（用 trace 一致性替代） |
| 诊断自动修复/在线 daemon | **不做**（只读红线） |
| 真多机/网络分区/时钟调整 | **不做**（沙箱无 `CAP_NET_ADMIN`；只做进程级故障） |

---

## 8. 验收清单（已实施）

- [x] P1-1：门面删除场景由漏报变报；签名/成员正负用例绿。
- [x] P1-2：删除/新增文件都进入合并视图；默认 detail 键集不变。
- [x] P1-3：空转现场报 `AGING_OVERDUE`；诊断前后指纹不变。
- [x] P1-4：`corpus_version=1.1.0`；47 条向量；文档条数与 manifest 一致。
- [x] P1-5：v2 录制成功且无 thinking/截断样本；报告含中位数/极差。
- [x] P1-6：I3 broken 与 I1 unfair 均被 TLC 找到反例；fair 配置 exit 0。
- [x] P2-1：默认混沌-lite 绿；`RSC_CHAOS=1` 7 条绿；老 Raft 用例不退化。
- [x] P2-2：同 seed 目录字节一致；默认路径不变。
- [x] P2-3：所有 hash seed 摘要一致；不可序列化 fail-closed。
- [x] P2-4：跨线程调用明确 `ValueError`，计数/时钟不前进。
- [x] 全量：`pytest -q`、`ruff check`、`check_s24_conformance.py`、`check_docs_consistency.py` 全绿；带 TLA jar 的测试全绿。

---

## 9. 实施状态（2026-10-29）

| 项 | 关键产物 | 验证 |
|---|---|---|
| P1-1 | `harness/semantic.py`（`ModuleFacts`/`FuncSig`/`ClassFacts`/`CallSite`/`MemberRef`）+ `WorkspaceManager.deleted_resources()` | `tests/test_s24_semantic.py` 31 passed |
| P1-2 | `RscSession.verify(steps/include/exclude/diagnostics)`、墓碑/新增文件合并视图 | `tests/test_s24_verify.py` + pgroup 23 passed |
| P1-3 | `checkpoint.envelope_problems`、`diagnose.check_checkpoint_envelopes/check_aging` | `tests/test_s24_diagnose.py` 23 passed |
| P1-4 | `vectors/fencing.json`、`vectors/plan.json`、`CORPUS_VERSION=1.1.0` | `tests/test_s24_corpus.py` 23 passed；conformance 217/217 |
| P1-5 | `LLMReply.finish_reason`、`extra` 透传、v2 样本聚合、`bench/fixtures/llm_replay.n5.json` | `tests/test_s24_replay.py` + `test_s24_llm_usage.py` 25 passed |
| P1-6 | `spec/tla/I3Fencing.tla` + I1 `FairSpec`/`EventuallyCommitted` + `check_tla.py` | `TLA2TOOLS_JAR=/tmp/tla2tools.jar pytest tests/test_s24_tla.py` 19 passed |
| P2-1 | `harness/storage/raft_node.py`、`tests/_raft_proc.py`、`tests/test_s24_raft_chaos.py` | lite 2 passed；`RSC_CHAOS=1` 7 passed |
| P2-2 | `FileCheckpointStore(entropy, clock)`、`Harness` 接线 | `tests/test_s24_determinism.py` 23 passed |
| P2-3 | WAL `_canon_default`、`tests/_s24_hash_child.py`、`test_s24_cross_process_replay.py` | 4 passed（4 个 seed 摘要全等） |
| P2-4 | `Deterministic._assert_owner()` | 跨线程/时钟/构造线程用例绿 |
| 补壳 | `harness/ergonomics.py`（`@rsc_tool` / `rsc_task` / `rsc_session`）、`docs/quickstart.md`、`examples/quickstart/run.py` | 17 条接入壳用例 + 四场景样例（S1–S4）真跑 |

门禁快照：

- 主门禁：**924 passed / 23 skipped / 4 xfailed**（88s）；
- 带 TLA jar：**936 passed / 11 skipped / 4 xfailed**（95s）；
- 完整混沌：`RSC_CHAOS=1 pytest tests/test_s24_raft_chaos.py` = **7 passed**；
- `ruff check .` 0；`scripts/check_s24_conformance.py` **217/217**；
  `scripts/check_docs_consistency.py` **0 findings**。
