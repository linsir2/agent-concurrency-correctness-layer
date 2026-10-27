# S9：持久化 checkpoint + 收据链 + 契约收紧

| 项目 | 内容 |
|---|---|
| 版本 | **v1.2（实现完成 + 三轮对抗评审修订）** |
| 日期 | 2026-10-27 |
| 上游 | [S5 v1.2](s5-recovery-and-journals.md) §12.6、[S6 v1.2](s6-runtime-and-e2e.md) §12.6、[S7 v1.2](s7-benchmark-and-coverage.md) §10.4 |
| 覆盖边界 | **K-24**、**K-25**、**K-26**、**K-30** |
| 分波 | **S9a**（K-25 + K-26，小）→ **S9b**（K-24 收据链，中）→ **S9c**（FileCheckpointStore，大）→ **S9d**（跨进程 + SIGKILL） |
| 延后 | MS9 跨进程预算对账、egress gateway → **S10** |
| 交付判据 | **214 passed**（连跑 5 轮稳定）；覆盖率 **84.69%**；`pytest -m fault` → 5 |
| 修订 | v1.0 → v1.1 修 8 P0 + 9 P1（§7）；v1.1 → v1.2 实现后三轮评审修 2 P0 + 6 P1 + 11 P2/P3（§8） |

---

## 0. 调研结论（决定 6 个选择）

| 结论 | 影响 |
|---|---|
| `ref = CP{next(counter)}` 进程重启与磁盘旧 ref **冲突** | 需**持久 `next_seq` + `O_EXCL` 预占** |
| `atomic.py` 已有正确原子写（temp 同目录 → fsync → `os.replace` → `fsync_dir`） | **复用，不重写** |
| `Receipt` 是线性 hash-chain；`verify_chain` 只查 prev 连续 + 签名 | 裸链**证不了尾部截断** → 需 head 锚点 |
| 单文件 map 是 **O(N²)**（`save` 每 turn 一次） | 每 ref 一文件 + **小文件 latest 指针**（index 不做全量重写） |
| `CRITICAL`：`materialized` **全仓无写入点**、checkpoint **不持久化** | **K-26 必须先定语义**（见 §2.2） |
| `CRITICAL`：`DELETE` 的比较是 `content is DELETE` | codec **decode 必须返模块级单例**，否则字符串 `"DELETE"` 被当内容提交 |

---

## 1. 目标

| # | 目标 | 判据 |
|---|---|---|
| G1 | `CrashInjected(BaseException)`，且 probe 不吞它 | S9-1 |
| G2 | `retention` 语义定死且**安全**（无快照不跳） | S9-2 |
| G3 | 收据在 `commit_batch` 同帧生成、确定性去重、head 后置提交 | S9-3/4/5 |
| G4 | `FileCheckpointStore`（信封 + codec + 锁 + 唯一 ref + 损坏隔离） | S9-6~11 |
| G5 | 跨进程 + SIGKILL（阶段 barrier，非竞速） | S9-12/13 |
| G6 | 现有 152 用例全绿 | 回归 |

---

## 2. 设计

### 2.1 K-25：`CrashInjected(BaseException)`

```python
class CrashInjected(BaseException): ...
```

**三处必改（否则注入仍失效）**：

| 位置 | 问题 | 修法 |
|---|---|---|
| `checkpoint.py` `probe_one` | `except BaseException → "None"` **会吞掉注入崩溃** | 前面加 `except CrashInjected: raise` |
| `harness.py` notify 的 `except Exception: pass` | 改继承后**挡不住** `BaseException` → 订阅者抛崩溃会覆盖 `commit_batch` 返回值 | 保留 `except Exception`，前加 `except CrashInjected: raise`（崩溃应穿透） |
| `orchestration._rollback` 的 `except Exception: pass` | 崩溃会中断回滚链 | 保持 `except Exception`（回滚必须跑完）；文档记边界 |

> **不要把 `except Exception` 改成 `except BaseException`**（v1.0 的错误 —— 会吞 `KeyboardInterrupt`/`SystemExit`，且 `bench/conditions.py` 根本没有该 except）。
>
> **`finally` 仍执行** → `CrashInjected` **不是进程崩溃**：`_run_one` 的 `finally` 会跑 `_finalize`。因此 SIGKILL 用例**必须走子进程**；`crash_at` 只能测「业务异常」。

### 2.2 K-26：`retention` 语义（v1.1 重定）

**v1.0 的错误**：`materialized` **从未被写入**、checkpoint **不持久化** → 切 floor 会跳进**空视图**（丢状态），且 `retention=0` 时 `floor=tail` 会跳过全部。

**v1.1 定死（安全优先）**：

```python
def context_view(self, acb) -> dict:
    tail = self.event_log.tail()
    if self.retention > 0:
        floor = max(0, tail - self.retention + 1)
        if acb.cursor < floor:
            if acb.materialized:          # 有快照 → 快照已含前缀，可跳
                acb.cursor = floor
                self.snapshots_forced += 1
            else:                          # 无快照 → 绝不跳（否则丢状态）
                self.retention_bypassed += 1
    ...
```

**要点**：
- `retention=0` → **不限制**（特判，不是 `floor=tail`）
- `retention<0` → 构造时拒绝
- `snapshots_forced` / `retention_bypassed` 在 `__init__` **初始化**（v1.0 漏了 → `AttributeError`）
- **无快照时不跳** —— 这是诚实的选择：宁可全量重放（慢），不丢上下文
- 边界：`tail` 是**全局水位**，跨 run 会互相淘汰（记 K-33）

### 2.3 K-24：收据链（v1.1 重写）

#### 2.3.1 默认开启 + 未签名显式化（修 P0：默认关闭 = 名义修复）

```python
# 总是签发；无 secret 时 signature="" 且 signed=False
receipts_enabled: bool = True        # 可显式关闭（kill-switch）
```

`VerifyResult = True | False | None`：
- `True` = 链连续 **且** 签名全对
- `None` = **链连续但未签名**（不等于「通过」，也不等于「篡改」）
- `False` = 断链 / 签名不符

**`verify_chain` 必须区分**：v1.0 说「secret 为空返回 False」会把「未签名」和「被篡改」混为一谈。

#### 2.3.2 `commit_key`（修 P0：`receipt_id` 只用 `version_after` 证明不了去重）

```python
commit_key = sha256(f"{run_id}|{logical_subgoal_id}|{resource}|{content_hash}|{base_version}|{mode}")[:24]
receipt_id = commit_key          # 同一逻辑提交恒等
```

> 评审实测：`recover` 重放使 `manifest` 前进 → 重试拿到**新 `version_after`** → 若 id 只含 version 就会重复签发。`commit_key` 基于**内容 + base**，重试恒等。

#### 2.3.3 幂等短路（修 P0：一次提交两条事件）

`commit_batch` 在 `(resource, version_after, mode)` 已有 `RESOURCE_COMMITTED` 时**直接返回 OK**，不再 append。

#### 2.3.4 head 后置提交（修 P0：链污染）

```python
# ① 读 head（scope 锁内）
# ② 构造 drafts
try:
    self.event_log.append_batch(drafts)      # ← 提交点（一次 fdatasync）
except BaseException:
    raise                                     # head 未动 → 链不断
# ③ 只在 append 成功后提交内存态
self._receipt_heads[scope] = receipt_hash(last)
self._receipt_ids.add(rid)
```

**`_receipt_heads` / `_receipt_ids` 在 `Harness.__init__` / `recover` 无条件从 `EventLog` 重建**（不读 checkpoint 内存值）。

#### 2.3.5 作用域锁与锁序（修 P1：recover 逆序死锁）

```python
_scope_locks[(run_id, logical_subgoal_id)]      # 每 scope 一把 RLock
```

**全局锁序写死：`resource → scope → eventlog`**。scope 临界区**只覆盖**「读 head → 构造 drafts → append_batch」；**投影/通知/重放都在锁外**（否则 `recover` 持 scope 要 resource、`commit_batch` 持 resource 要 scope → 死锁）。

#### 2.3.6 `subject` 不能是 resource（修 P1：污染 I6）

`RECEIPT_ISSUED.subject = receipt_id`（资源放 payload）。否则 `context_view` 会对该 subject 回退 `workspace.get()` → 拿到**当前值**而非该 seq 历史值，破坏「一致前缀」。

#### 2.3.7 head 锚点

- `TASK_COMPLETED.payload` 增 `run_id` + `receipt_head` + `receipt_count`（当前**没有 run_id** → 跨 run 冲突）
- `plan["receipt_heads"][logical_subgoal_id]` 只作**非权威镜像**（plan 不持久化、patch 会 deepcopy）
- 恢复以 **EventLog 最后一条 `RECEIPT_ISSUED`** 为准

#### 2.3.8 签名与 secret 域分离

```python
receipt_secret = HMAC(auth_secret, "receipt-v1")   # 与 token 域分离
```
- 签名改 **HMAC-SHA256 全量 hex**（当前 `sha256(payload+secret)[:16]` = 64bit + 长度扩展）
- `compare_digest` 比较
- 开关**独立于 secret 是否存在**（修：`HARNESS_AUTH_SECRET` 走 env 而构造参数为 `None` 时 v1.0 会关掉收据）

#### 2.3.9 `version_after` 只算一次（修 P1）

`_commit_record` 返回 `(draft, version_before, version_after)`；`_apply_locked` 接受传入版本并 `assert` 相等；收据复用同一值。

### 2.4 K-30：`FileCheckpointStore`

#### 2.4.1 目录

```text
<root>/
  store.json          # {format_version, store_uuid, next_seq}
  store.lock          # flock 目标（稳定文件）
  refs/<xx>/<yy>/<ref>.json
  latest/<sha8(acb_id)>.json       # ← 小指针，不做全量 index（修 O(N²)）
  latest_run/<sha8>.json
  latest_task/<sha8>.json
```

信封（含 `header_checksum` 与 `payload_checksum` **双校验**，以便 payload 坏时仍能用头部构造隔离 ACB）：

```json
{
  "format_version": 1, "schema_version": 2, "ref": "CP-...",
  "acb_id": "...", "run_id": "...", "task_id": "...",
  "generation": 2, "save_seq": 42, "created_at_ms": 0, "event_cursor": 123,
  "header_checksum": "...", "payload_checksum": "...",
  "payload": { ...现 38 字段... }
}
```

#### 2.4.2 ref 分配：持锁内「分配并落盘」（修 P0 竞态）

```python
with self._thread_lock, self._flock():
    seq = self._meta["next_seq"]
    self._meta["next_seq"] = seq + 1
    atomic_write(meta_path, ...)          # ← 持锁内落盘（CAS 式）
    ref = f"CP-{uuid8}-{seq:020d}-{uuid4().hex[:12]}"
    # O_EXCL 预占，绝不 os.replace 覆盖
```

- **不用「扫描后放锁」**（v1.0 竞态：两进程都扫到 42，各分配 43）
- **随机段换 `uuid4().hex[:12]`（48bit）**，不用 `rand8`（32bit，8 万条量级生日碰撞）
- `seq` 从**文件名**取（内容坏不影响计数）；坏文件改名 `.corrupt`，**绝不复用槽位**

#### 2.4.3 双层锁（修 P0：flock 不是线程互斥）

```python
self._thread_lock = threading.RLock()      # 同进程线程
self._flock = FileLock(store_lock_path)    # 跨进程（本地 FS）
```

> flock 属于 open file description：**同一 fd 的 `LOCK_EX` 直接成功** → 8 个线程并发 `save` 全部通过。必须 RLock + flock 双层。

#### 2.4.4 先盘后缓存（修 P0：SIGKILL 假绿）

```python
def save(...):
    atomic_write(ref_path, envelope)       # ← 成功之后
    self._store[ref] = payload             # ← 才填缓存
```

`recover` 用 `load_strict()`（校验 checksum，绕过缓存）。`_store` **仅作不可变 ref 的只读镜像**。
> `test_ms4_runtime.py:163` 直接改 `_store[ref]` → 该用例显式迁到**内存版 store**（测试双重身份显式化）。

#### 2.4.5 codec：decode 必返单例（修 P0）

```python
_DELETE_TAG = {"__type__": "DELETE"}

def decode(v):
    if isinstance(v, dict) and v == _DELETE_TAG:
        return DELETE                      # ← 必须返模块级单例
    ...
```

> **为什么 P0**：若 decode 返新实例，`commit_batch` 的 `contents[r] is DELETE` 为 False → 走 `blobs.put(canonical_bytes(新实例))` → `default=str` 得字符串 `"DELETE"` → **把字符串当内容提交**（不是删除）。

**`__type__` 冲突**：`stage`/`commit` 边界**递归拒绝**含保留键的用户数据（fail-fast），否则 `{"__type__":"DELETE"}` 的用户内容会被误解成删除。

**`DeleteResource.__deepcopy__`/`__copy__`/`__reduce__` 返自身**（既有缺陷：`deepcopy(DELETE)` 已破坏身份）。

#### 2.4.6 codec 严格性必须与 blob 对齐（修 P0：已提交任务被 save 打死）

现状：`blob.canonical_bytes` 用 `default=str` 且不禁止 NaN → `stage({1,2})`/NaN **提交成功**；随后 `save` 的严格 codec 拒绝 → **在 `finally` 里抛，一个已提交任务直接崩**。

**修法**：blob 与 checkpoint **共用同一套严格 codec**，在 `stage`/`try_admit` 边界 fail-fast。

#### 2.4.7 目录 fsync（修 P1）

`atomic_write` 只 fsync 目标目录。首次写 `refs/xx/yy/` 时 `xx`/`yy` 的目录项**未 fsync** → 掉电整个分片消失。**新建祖先目录自底向上 fsync**。
> SIGKILL 测不出这个（page cache 不丢）→ 文档必须区分「SIGKILL 原子性」与「掉电持久性」；后者**本阶段不承诺**。

#### 2.4.8 `load` 失败纳入 recover（修 P1）

```python
try:
    data = self.checkpoints.load_strict(ref)
except CheckpointCorrupt as e:
    # 用**头部**字段（header_checksum 单独校验）构造 QUARANTINED + needs_readmit
    acb = ACB(e.header["acb_id"], e.header["run_id"], e.header["task_id"], ...)
    acb.state, acb.failure = QUARANTINED, "CHECKPOINT_CORRUPT"
```

#### 2.4.9 durable 前置条件（不做就是假持久化）

必须同时注入 `WalEventLog(fsync=True)` + `FileBlobStore`，且子进程必须**从 seq 0 全量重建 workspace**（`recover` 只从 per-ACB `event_cursor` 重放，新进程 manifest 是空的）。

---

## 3. 测试

| # | 用例 | 关键判据 |
|---|---|---|
| S9-1 | `CrashInjected` 是 `BaseException` 非 `Exception`；handler 内崩溃**不被**映射 `UPSTREAM_FAILED`；**probe 内崩溃不被吞成 `"None"`** | 3 断言 |
| S9-2 | `retention=0` → 不限；`retention=3, cursor=0, materialized={}` → **不跳**（`retention_bypassed==1`）；`materialized={"r":"SNAP"}` → 跳且 `view["r"]=="SNAP"`；插桩 `replay(from_seq)` 断言 | 4 断言 + **负对照**（monkeypatch 回旧实现 → 全红） |
| S9-3 | **同帧**：用 `WalEventLog`，断言 `wal.frames()[i]` **同一帧**含 `RESOURCE_COMMITTED` + `RECEIPT_ISSUED`；负对照（拆两次 append_batch → 红） | 结构证明，不需崩溃 |
| S9-4 | 去重：`commit_key` 重试恒等；**注入点在 `append_batch` 成功后、投影前**（v1.0 的 `crash_at` 在动作前，测不到）→ 断言不新增 `RESOURCE_COMMITTED` **且**不新增 `RECEIPT_ISSUED` | 幂等短路 |
| S9-5 | 并发：**同 scope 不同资源** + barrier 在「读完 head、append 前」；正对照（S2 必须能并行完成，证明不是全局锁）；负对照（`nullcontext()` 替锁 → barrier 到达 → 链分叉 → `verify` False） | 2 对照 |
| S9-6 | 跨进程：父 `WalEventLog(close)` → 子新实例 `load_strict` + **从 seq 0 重建** + `latest_ref_for_acb` | 子进程 |
| S9-7 | SIGKILL：**阶段 barrier**（子进程在 stage 写 marker 后 `Event.wait()`，父读到再 kill）4 个阶段 P0/P1/P2/P3 | 非竞速 |
| S9-8 | ref 唯一：持锁内分配；两进程并发启动断言 seq 不重用 | 并发 |
| S9-9 | 损坏：`load_strict` 抛 `CheckpointCorrupt`；`recover` 用头部构造 `QUARANTINED` | 不静默 |
| S9-10 | 旧格式 fixture（真扁平 payload、无 envelope）可加载 | 兼容 |
| S9-11 | codec：`decode(encode(DELETE)) is DELETE`；`deepcopy`/`copy`/pickle 往返仍是单例；`{"__type__":"DELETE"}` 用户数据**被拒绝** | 3 断言 |
| S9-12 | `retention>0` + `recover`（`materialized` 为空）→ **不跳**，不丢上下文 | 安全 |

---

## 4. 迁移清单

| 文件 | 变更 |
|---|---|
| `harness/runtime.py` | `CrashInjected(BaseException)`；`_commit_record` 返回版本三元组 |
| `harness/checkpoint.py` | `probe_one` 加 `except CrashInjected: raise`；`FileCheckpointStore` + codec + 双层锁 + 唯一 ref |
| `harness/context.py` | `retention` 安全语义 + 两个计数器 |
| `harness/types.py` | `EventKind.RECEIPT_ISSUED` |
| `harness/spec/receipt.py` | `to_dict/from_dict`；HMAC 全量；`verify_chain` 三态 + 非空/重复 id 检查 |
| `harness/harness.py` | `commit_batch` 收据（同帧 + `commit_key` + 幂等短路 + head 后置）；scope 锁；`receipts_of`/`verify_receipts`；head 重建；`checkpoint_root` |
| `harness/rebase.py` | `DeleteResource.__deepcopy__/__copy__/__reduce__` 返自身 |
| `harness/storage/blob.py` | 与 checkpoint 共用严格 codec |
| `tests/` | `test_s9_persistence.py`、`test_s9_receipts.py`；I13 纳入 E2E |

---

## 5. 风险与对策

| 风险 | 对策 |
|---|---|
| 收据污染现有事件流 | 默认开但测试显式可控；受影响断言改按 `kind` 过滤 |
| scope 锁 ↔ 资源锁死锁 | 锁序写死 `resource → scope → eventlog`；临界区只含 head→drafts→append |
| 缓存掩盖磁盘损坏 | 先盘后缓存；`recover` 用 `load_strict` |
| 链分叉 | scope 锁 + barrier 负对照验证 |
| 掉电持久性 | **不承诺**；只承诺 SIGKILL 原子性（明确写进报告） |

---

## 6. 决策（v1.1 定案）

| # | 问题 | 结论 |
|---|---|---|
| Q1 | 收据默认开关 | **默认开**；无 secret 时 `signed=False`，`verify` 返 `None`（三态） |
| Q2 | 链作用域 | `(run_id, logical_subgoal_id)` |
| Q3 | 文件 store 默认 | **否**，内存仍默认；durable 显式 |
| Q4 | `sequence` | **不加**（v1 不变）；用 `commit_key` + `TASK_COMPLETED.receipt_head` 锚定 |
| Q5 | K-26 无快照时 | **不跳**（宁可慢，不丢上下文） |

---

## 7. 评审修订记录（v1.0 → v1.1）

### 7.1 P0（8 项）

| # | v1.0 问题 | 修法 |
|---|---|---|
| **P0-1** | **K-26 核心不成立**：`materialized` **全仓无写入点**、checkpoint **不持久化** → 切 floor = 跳进空视图（丢状态）；且 `retention=0` → `floor=tail` **跳过全部** | 无快照**不跳**（`retention_bypassed`）；`retention=0` 特判不限；计数器在 `__init__` 初始化 |
| **P0-2** | **收据默认关闭 = K-24 名义修了实际没修**；空 secret `verify` 返 False 混淆「未签名」与「被篡改」 | 默认开 + `signed=False` + `verify` **三态** |
| **P0-3** | **head/dedup 在 append 之前更新** → append 抛异常则链永久断；重试重复签发 | append **成功后**才提交内存态；失败不改 head；从 EventLog 无条件重建 |
| **P0-4** | **`receipt_id` 只用 `version_after`** → `recover` 重放使 manifest 前进，重试拿新版本 → **重复签发** | 改 `commit_key`（内容 hash + base_version + mode） |
| **P0-5** | **一次提交两条 `RESOURCE_COMMITTED`**（append 成功、投影抛、重试） | 按 `(resource, version, mode)` **幂等短路** |
| **P0-6** | **`DELETE` decode 返新实例** → `contents[r] is DELETE` False → `default=str` 得 `"DELETE"` → **把字符串当内容提交** | decode 必返**模块级单例**；`stage/commit` 递归拒绝 `__type__` 保留键 |
| **P0-7** | **`_store` 缓存优先** → SIGKILL 用例假绿、生产拿到幽灵 checkpoint | **先盘后缓存**；`recover` 用 `load_strict` |
| **P0-8** | **ref 分配竞态**：持锁扫描后放锁 → 两进程都分配 43；`rand8` 32bit 碰撞 | 持锁内**分配并落盘** `next_seq`；`O_EXCL` 预占；`uuid4` 48bit |

### 7.2 P1（9 项）

| # | 问题 | 修法 |
|---|---|---|
| 1 | `probe_one` 的 `except BaseException` **吞掉注入崩溃** | 前加 `except CrashInjected: raise` |
| 2 | v1.0 说「把 `except Exception` 改 `BaseException`」是**事实错误且有害** | 不改；只在必要处前置 `except CrashInjected: raise` |
| 3 | notify 的 `except Exception: pass` 挡不住 `BaseException` → 覆盖 `commit_batch` 返回值 | 前置 `except CrashInjected: raise` |
| 4 | **scope 锁 + recover 逆序死锁** | 锁序写死；临界区只含 head→drafts→append |
| 5 | **flock 不是线程互斥**（同一 fd `LOCK_EX` 直接成功） | `RLock` + `flock` 双层 |
| 6 | **`index.json` 全量重写 = O(N²)**（单文件 map 被否决的理由复活） | 改**小文件 latest 指针** |
| 7 | **codec 严格 vs blob 宽松** → 已提交任务在 `save` 里被打死 | 共用严格 codec，`stage` 边界 fail-fast |
| 8 | **`RECEIPT_ISSUED.subject=resource`** 污染 `context_view`（拿到当前值而非历史值） | subject 改 `receipt_id` |
| 9 | **`TASK_COMPLETED` 无 `run_id`** → 跨 run head 冲突；`plan` 不持久化 | payload 加 `run_id`；plan 只作非权威镜像 |

### 7.3 其他（已修）

| 问题 | 修法 |
|---|---|
| `version_after` 被算三遍（脆弱） | `_commit_record` 返回三元组，`_apply_locked` assert 相等 |
| 新建祖先目录未 fsync（掉电丢分片） | 逐级 fsync；明确「掉电持久性本阶段不承诺」 |
| `load` 失败在 `_restore_acb` try 之外 | 双 checksum（header/payload）；catch → `QUARANTINED` |
| `hasattr(self.checkpoints, "event_log")` 静默跳过注入 | 存储类预声明属性，无条件注入 |
| `._store` 被测试直接改（`test_ms4:163`） | 该用例显式迁到内存版 store |
| S9-3「同帧」在内存 EventLog 下假绿 | 必须用 `WalEventLog` 断言同一 frame |
| S9-7 靠 sleep 猜时机 | **阶段 barrier**（写 marker → `Event.wait()` → 父 kill） |
| 范围过大（4 边界一批） | 拆 **S9a/b/c/d** 四波 |

### 7.4 P2（第二波对抗评审，已修）

| # | 问题 | 修法 |
|---|---|---|
| **P2-1** | `checkpoint._write_new` 直接写最终路径（SIGKILL/短写 → 半截最终文件）；测试注入 `.part` **没走真实 `_write_new`** | 写同目录 `ref.json.tmp` + fsync → `os.link(tmp, ref)`（EEXIST 即失败，原子且不覆盖）→ unlink tmp → fsync 目录；测试改为对真实 `_write_new` 注入半写，断言隔离（`.tmp.corrupt` 改名、seq 不复用、latest 悬空可重建） |
| **P2-2** | `_init_meta` 修正 `next_seq` 只改内存不落盘 → `save` 重读磁盘 stale 值 → seq 复用 | `_init_meta` 发现修正时**立即 `atomic_write` 回写** |
| **P2-3** | latest 指针无单调 CAS（滞后写者可回退）+ `_sha8` 32bit（~7 万 ACB 50% 碰撞）+ `_read_pointer` 不校验 `acb_id` | flock 内读旧指针，仅 `(generation, save_seq)` 更大时替换；指针 hash 加长到 `sha256[:16]`（64bit）；`_read_pointer` 校验 `acb_id`（task 指针校验三元组） |
| **P2-4** | `_receipt_ids` 重建 `ids.add()` 折叠重复、heads 取最后一条、不校验链连续性 → verify 永远 False | 重建按 scope 维护 running head，校验 `prev_receipt_hash == head` 且 `receipt_id` 不重复；失败 → 标 **tainted** 并拒绝继续扩展 |
| **P2-5** | `_scope_lock` 是进程内 RLock（跨进程不互斥）+ dedup 检查在取 scope 锁**之前**（同 scope 跨资源非原子） | 去重检查**移入 scope 临界区**；**明确声明 durable 模式为单写者**（`WalEventLog` 亦然），跨进程 scope 锁留 S10 |
| **P2-6** | `Receipt.from_dict` 强制 `data["run_id"]` → 一条缺 `run_id` 的 `RECEIPT_ISSUED` 让启动/验签抛 `KeyError` | `from_dict` 容错校验必填字段（`ReceiptMalformed`）；`receipts_of` 跳过畸形并记录；`_rebuild_receipts` 包校验 + 标 tainted；`verify_receipts` 返回 False |
| **P2-7** | `FileBlobStore.get` 直接 `read_bytes`、`_project_event` 只 `exists()` → 坏盘/位翻转静默改变内容 | `get` 校验 `sha256(data) == blob_hash`（失配 → `BlobCorrupt`）；重建遇缺失 → 隔离（`isolated_resources`），不再静默 `return False` |
| **P2-8** | 测试判据偏弱 | SIGKILL P2 新增「扫描能发现最大 seq ref 且双 checksum 有效」+ 三指针一致性；跨进程断言 `generation == parent + 1` + `context_view` 非空；retention 指标按 run/subgoal 分桶并区分「无快照」与「水位不足」 |
| **P2-9** | S8-2 断言由 `version == v0 + ok_count` 改为 `− dedup_count` 是否削弱 | **合理**（原断言在内容去重语义下不可能成立）；额外加 `_assert_genuine_dedup` 堵住「随便返回 OK」 |

> **单写者声明**：`_scope_lock` 与 `WalEventLog.append_batch` 的 `_lock` 均为**进程内**锁，
> 跨进程**不互斥**。durable 模式（`WalEventLog(fsync=True)` + `FileBlobStore` +
> `FileCheckpointStore`）必须按**单写者**使用；跨进程多写者需自行串行化（S10 再上跨进程 scope 锁）。

---

## 8. 实现后对抗评审（v1.1 → v1.2）

S9 分四波（S9a 契约 → S9b 收据 → S9c 文件 store → S9d 跨进程/SIGKILL），每波后各跑一次对抗评审，共 3 轮。**2 个 P0 + 6 个 P1 + 11 个 P2/P3 已修**。

### 8.1 P0（2 项，均实测静默丢写）

| # | 问题（反例） | 修法 |
|---|---|---|
| **P0-A** | **dedup 静默丢写**：命中 `(resource, version, mode)` 已提交事件后直接 return OK，**既不补投影也不比 content_hash**。实测：commit `{'v':1}` 在投影前抛 → 重试得 OK 但 manifest 仍 v0；再 stage `{'v':2}` 基于过期 v0 算 version=1 → **被误短路 → 新内容丢失且返回 OK** | ①dedup 命中走 `_replay_dedup_events` 补投影；②按 `(run,subgoal,resource,blob_hash,mode)` 匹配并**校验 blob_hash**；③版本以**事件日志水位**为准（`_watermark_version`/`_effective_version`） |
| **P0-B** | **`commit_key` 含可变 `base_version`** → `_accept`/`_refresh_read_dep` 刷新 base 后 key 变化 → **重复提交 + 重复收据** | key 去掉 base，只留 `(run_id, subgoal, resource, content_hash, mode)` |

### 8.2 P1（6 项）

| # | 问题（反例） | 修法 |
|---|---|---|
| **P1-A** | **all-or-nothing dedup** → 部分命中时给已签发的写**重复 append 收据**（重复 `receipt_id`）→ `verify` **永久 False**，链不可自愈 | **逐写过滤**；append 前 `assert` 不在 `_receipt_ids`，冲突抛 `ReceiptConflict` → 隔离 |
| **P1-B** | **嵌套 bytes 静默损坏**：`canonical_bytes` 编成 `{"__type__":"bytes"}`，但 `_decode` 只用裸 `json.loads` → 读回变 dict | `_decode` 改走 `codec.decode(codec.loads(...))` |
| **P1-C** | **stage 边界未 fail-fast**：`stage({1,2})` → commit 时 `canonical_bytes` 抛 `TypeError` → 被映射 `UPSTREAM_FAILED` → 随后 `checkpoints.save` **再抛** → 穿透 `run` → `_finalize` 里 save 第三次抛 → **slots.release / 预算结算 / 归档全跳过（槽位泄漏）** | `stage`/`try_admit` 先 `canonical_bytes` 校验 → `INVALID_INPUT` + 零残留；`commit_batch` 捕获转 `Result`；`_finalize` 用 `_safe_save` **保证不抛** |
| **P1-D** | **quarantine 后 latest 悬空**：`.corrupt` 改名后 latest 仍指向不存在的 ref；`_read_document` 对 missing 给 `header={}` → recover 构造 `acb_id=""` 的**幽灵 ACB** 注册到 `acbs[""]` | quarantine 后 `_rebuild_latest_pointers`（只更新已存在指针）；`_read_pointer` 校验 `_REF_RE` + 存在性；`_read_document` 用 `_header_from_pointers` 兜底；非法 ref → `CheckpointCorrupt` |
| **P1-E** | **retention 跳 floor 丢状态**：`materialized` 非空 ≠ 覆盖到 floor（只是 cursor 处视图，不含 `[cursor,floor)` 新事件）。实测：订阅事件在 seq1..3，retention=3/cursor=0/materialized={"r":"SNAP"} → 跳后 `r='SNAP'`，但**真实最新是 `{'i':2}`** | 只有**快照水位 >= floor** 才跳，否则老实重放 |
| **P1-F** | **recover 不恢复 subscriptions/materialized** → 恢复后 `context_view` 为空（事件全被 `ev.subject in acb.subscriptions` 过滤） | payload 增 `subscriptions`(sorted) / `materialized`(+水位)；`_restore_acb` 恢复 |

### 8.3 P2（终审第二轮，6 项）

| # | 问题（反例） | 修法 |
|---|---|---|
| **P2-A** | **`_project_event` 不隔离 `BlobCorrupt`**：位翻转单 blob → `FileBlobStore.get` 抛 `BlobCorrupt`（继承 Exception，不是 KeyError/FileNotFoundError）→ 冒泡出 `WorkspaceManager.__init__` → **Harness 启动直接失败** | 两处 except 都加 `BlobCorrupt` → `isolated_resources.add` + `return False` |
| **P2-B** | **陈旧内容去重（stale-content dedup）**：`seed r=A; commit r=A; commit r=B` 后重提交 A → 仍命中旧事件 dedup → **值仍 B、version 停 3、写丢失** | `_dedup_event` 要求 `matched.version == workspace._effective_version(resource)`；配套：版本推进后换 `receipt_id`（否则退化成永久 `ReceiptConflict`） |
| **P2-C** | **`ReceiptConflict` 不隔离 scope、无恢复**：只隔离 ACB → 重试同逻辑写**永久冲突**，跨进程/重启一样 | 冲突即 `_tainted_scopes.add(scope)`；新增 `reconcile_scope()` 显式对账入口 |
| **P2-D** | **`_assert_genuine_dedup` 漏报跨 subgoal 去重**：`committed_hashes` 是**全局集合** → a/b 不同 subgoal 共用 r 时，若实现漏 subgoal 分量 → 误判 dedup 仍满足两条断言 | 断言改校验命中事件的 `(run_id, logical_subgoal_id, resource, mode, deleted)` + `blob_hash` + 最新性 |
| **P2-E** | **ref 写入非原子**：`_write_new` 直接写最终路径 → 半截最终文件；且 SIGKILL 测试注入 `.part` **没走真实路径**（测假路径） | tmp + `os.link`(EEXIST 即失败) + unlink + fsync 目录；测试对真实 `_write_new` 注入 |
| **P2-F** | **seq 复用**：`_init_meta` 修正 `next_seq` 只改内存不落盘，`save` 重读磁盘 stale 值 | `_init_meta` 修正时**立即 atomic_write 回写** |

### 8.4 P3（5 项）

| 问题 | 修法 |
|---|---|
| latest 指针无单调 CAS | flock 内读旧指针，按 `kind` 分权威键（`latest` 用 gen 权威；`latest_run/task` 用 **seq 权威**，因 gen 是 ACB 级跨 ACB 非单调） |
| `_sha8` 只有 32bit（~7 万 ACB 50% 碰撞）+ 不校验 `acb_id` | 改 `_sha16`（64bit）+ 校验 `acb_id`/三元组 |
| `_receipt_ids` 重建不校验链连续性 | 重建时维护 running head 校验 prev + 重复 id → tainted |
| 畸形收据事件让**启动**抛 KeyError | `ReceiptMalformed`；`receipts_of` 跳过并记录；tainted → `verify False` |
| `_write_new` 非 EEXIST 时 tmp 泄漏；`_has_ref_files` 误算 `.tmp` | `try/finally` 兜底 unlink；排除 `*.tmp` |

### 8.5 测试强度修正

| 问题 | 修正 |
|---|---|
| `_assert_genuine_dedup` 全局 hash 集合（见 P2-D） | 改 scope 分量校验 |
| `ok_count >= dedup_count` **冗余**（由等式蕴含） | 删除 |
| SIGKILL P2 只断言 `latest is None`（证明「数据没坏」而非「可发现」） | 新增「扫描定位最大 seq ref 且双 checksum 有效」+ 三指针一致性 |
| 跨进程 `generation >= 2` 偏弱 | 改 `== parent_generation + 1` + `context_view` 非空 |
| retention 指标未分桶 | 按 `(run_id, subgoal)` 分桶，区分 `no_snapshot` / `watermark_short` |
| 3 条**恒真断言**（指针 64bit 结构、无指针文件的 `is None`、只证「会跳」不证「该跳时才跳」） | 收紧或标注 |

### 8.6 新增已知边界（v1.2）

| # | 边界 |
|---|---|
| **K-33** | `tail` 是**全局水位** → 跨 run 会互相淘汰（快 run 把慢 run 的 cursor 推出窗口） |
| **K-34** | **durable 模式为单写者**：`WalEventLog` 与 scope 锁均**跨进程不互斥**；跨进程 scope 锁留 S10 |
| **K-35** | **SIGKILL 只证明原子性，不证明掉电持久性**（新建祖先目录的 fsync 需另测） |
| **K-36** | `ReceiptConflict` 的 taint 为**内存态**；跨进程恢复走 `reconcile_scope()`（需人工或上层编排） |
| **K-37** | `storage.codec` 反向依赖领域层 `harness.rebase`（当前单向无环，但 `rebase` 一旦 import `storage.blob` 立即成环）→ 哨兵宜移中性模块 |
| **K-38** | `load_strict` 对**旧扁平 payload**（S9-10 兼容路径）不做 checksum 校验 → 「strict 对旧格式不 strict」 |
| **K-39** | `verify_receipts` 在**完全无收据**时返回 `False`（fail-closed），语义上易被误读为「篡改」而非「未启用」 |
