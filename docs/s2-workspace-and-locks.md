# S2 详细方案：workspace manifest + 每资源锁 + 租约时钟

| 项目 | 内容 |
|---|---|
| 版本 | **v1.1（已按三路评审修订）** |
| 日期 | 2026-10-26 |
| 上游 | [第一阶段方案 v1.1](phase1-implementation-plan.md) §4.5/§4.6/§5.1/§6、[设计规范 v1.12](multi-agent-harness-design-final.md) §5.4/§6.1/§7.4 |
| 覆盖测试 | T4（提交点 vs manifest）、T5（读集合陈旧 + `detail.resource`）、T5b（租约时钟过期）、T7（锁序不死锁） |
| 交付判据 | 上述 4 组测试绿 + **原有 49 测试全绿** |
| 修订 | v1.0 → v1.1：修 5 个 P0 + 12 个 P1（逐条见 §11） |

---

## 0. 评审结论摘要（v1.0 为什么不能直接编码）

| 级别 | 问题 | 处置 |
|---|---|---|
| **P0** | `put` 若推进版本，会打挂 11 个以 `version==0` 为基线的测试 | `put` **不推进版本**（新资源 version=0），只写种子事件（§5.2） |
| **P0** | `put` 写 `WORKSPACE_COMMITTED`，但重建只认 `RESOURCE_COMMITTED` → 重启丢种子 | 重建**同时识别两种 kind**（§5.7） |
| **P0** | `commit_batch` 归属二义（Harness vs WorkspaceManager） | **公共入口固定在 `Harness.commit_batch`**；workspace 只提供「已持锁」内部步骤（§5.5） |
| **P0** | `path_of` 未定义 | 改为**可选注入**，默认 `None` = 不做磁盘物化（§5.5） |
| **P0** | `renew` 不检查过期 → T5b 必红 | `renew` 检查 `expires_at` 并**续期**（§3.3） |
| **P1** | 订阅者回调在资源锁内执行（真死锁入口） | `append_batch(notify=False)` + 释放锁后 `notify_events()`（§5.5） |
| **P1** | 租约只在步骤②校验，提交点前不复验 | 提交点前**再复验一次**（§5.5 ③′） |
| **P1** | 锁超时后 `finally` 释放未持有的锁 → RuntimeError | `_acquire_all` 返回**实际已获取列表**（§4.4） |
| **P1** | `prune` 会造成「同资源两把锁」→ 互斥失效 | **S2 不提供 `prune`**，推迟到 S3（§4.5） |
| **P1** | T7 用「锁超时返回」冒充「不死锁」→ 假通过 | 断言两次提交都 `OK` + `version==1` + `len(_locks)>=2`（§8 T7） |

---

## 1. 目标与非目标

### 1.1 目标

| # | 目标 | 为什么是 S2 |
|---|---|---|
| G1 | `workspace` 改为 **manifest + blob 读模型** | I8 的另一半：S1 做了「提交点 = WAL」，S2 补「读路径不看磁盘文件」 |
| G2 | **每资源锁 + 按 `LOCK_KEY` 全序批量获取** | D4；且 T7 只有在多锁下才可证伪 |
| G3 | `prepare/apply` 返回 **`Result`（带 `detail`）** | T5 要断言 `detail.resource`；`CONFLICT` 要带 `diff` |
| G4 | `content_hash` 改为 **blob hash** | `repr(dict)` 键序敏感；补偿复验必须与落盘内容一致 |
| G5 | `intent_registry` **注入时钟**，`is_valid`/`renew` 按 `expires_at` 判定 | 租约过期竞态当前不可测 |
| G6 | `WorkspaceManager` **从事件重放重建 manifest** | 崩溃后 manifest 是纯投影，必须能 redo |

### 1.2 非目标

- Delete 哨兵补偿、`rebase.merge3` 独立模块（S3）
- `WorkspaceVersionStore` 前像的崩溃重建（S3，见 §9 D-6）
- 补偿日志落盘 + `COMPENSATING_BATCH`（S5）
- WAL 段轮转、锁表回收（MS6 / S3）

---

## 2. 调研结论（决定 5 个设计选择）

| 结论 | 影响 |
|---|---|
| git / OCI / etcd 的共同模式：**内容不可变（blob）+ 指针可变（manifest）分离**，读只解引用指针 | G1 用 `manifest: {resource: ManifestEntry}`；磁盘文件降级为可选物化视图 |
| ARIES 的 redo 是「重复历史」，但本仓库 manifest 是**纯逻辑投影** → redo 可直接全量覆盖，天然幂等 | G6 不需要 page-LSN |
| 锁排序是**预防**（破坏循环等待），超时是**逃生舱**；`join(timeout)` 返回 ≠ 线程结束 | G2 主用全序 all-or-nothing；T7 必须 `join(timeout)` + `is_alive()` |
| **当前单把全局 `RLock` 下，反向顺序测试恒绿（假通过）** | T7 必须先换多锁；并加「锁数 ≥ 2」的结构性断言 |
| `dict.setdefault` 原子，可做锁注册表；但锁表只增不减 | 阶段一接受（资源数有界）；**不做 prune**（会造成锁身份漂移） |

---

## 3. 数据结构

### 3.1 manifest 条目

```python
@dataclass(frozen=True)
class ManifestEntry:
    version: int
    blob_hash: str
    prev_hash: str = ""
```

`WorkspaceManager.manifest: dict[str, ManifestEntry]` —— **唯一读路径**。

### 3.2 `Result.detail` 契约（T5 依赖）

| status | detail 必含 |
|---|---|
| `CONFLICT` | `{resource, version, expected, diff}` |
| `REJECT_STALE_READ` | `{resource, expected, actual}` |
| `REJECT_STALE` | `{lease_id, reason}` |
| `REJECT_COMPENSATE` | `{resource, expected_hash, actual_hash}` |
| `UPSTREAM_FAILED`（锁超时） | `{reason: "lock_timeout", held: [...]}` |
| `OK` / `VALID` | `{resource, version}` |

> 锁超时用 `UPSTREAM_FAILED`（封闭枚举内、语义为瞬态上游失败），**不用** `COMMIT_UNRECOVERABLE` —— 后者只留给「WAL 中段坏帧」这类真不可恢复场景。

### 3.3 租约时间语义（v1.1 修正）

```python
# reserve: expires_at = 0 if ttl_ms <= 0 else clock.now_ms() + ttl_ms
# is_valid: valid and epoch == cur_epoch and (expires_at == 0 or clock.now_ms() < expires_at)
# renew:    若 expired 或 epoch 不符 → False；否则 expires_at = clock.now_ms() + ttl_ms → True
```

**三条必须写死的约定**：
1. **`expires_at == 0` 表示「不按时间过期」**（兼容现有 49 测试：它们不传 ttl 语义、也不推进时钟）。
2. **半开区间** `[start, start+ttl)`：`now == expires_at` 即失效。
3. `renew` **必须**检查过期并**刷新** `expires_at`；`renew_for(acb)` 同理（否则 T5b 的 `renew is False` 不成立）。

---

## 4. 锁设计

### 4.1 结构

```python
self._registry_lock = threading.Lock()          # 只护锁表，极短，不回调
self._locks: dict[str, threading.RLock] = {}

def lock_for(self, resource: str) -> threading.RLock:
    with self._registry_lock:
        lk = self._locks.get(resource)
        if lk is None:
            lk = threading.RLock()
            self._locks[resource] = lk
        return lk
```

`RLock` 允许同线程重入（`_apply_locked` 里再调 `version()`）。

### 4.2 批量获取（all-or-nothing）

```python
LOCK_KEY = lambda resource: resource            # 动态全序（字典序）

def _acquire_all(self, resources: set[str]) -> tuple[list[str], bool]:
    """返回 (已获取列表, 是否全部成功)。失败时已回滚。"""
    ordered = sorted(resources, key=LOCK_KEY)   # set 已去重
    held: list[str] = []
    for r in ordered:
        if self.lock_for(r).acquire(timeout=self._lock_timeout_s):
            held.append(r)
        else:
            for h in reversed(held):
                self.lock_for(h).release()
            return held, False
    return ordered, True
```

**四条硬规则**：
1. **先去重再排序**（`set` + `sorted`），否则同一把 `RLock` 被 acquire 两次。
2. **排序键是动态全序**，不预存列表。
3. **持资源锁期间不得回调**（不得触发订阅者、不得取 `budget`/`intents` 锁）。
4. **失败时在函数内回滚**，`finally` 只释放返回的 `held`（修复 v1.0 的过度释放）。

### 4.3 释放

`finally: for r in reversed(held): lock_for(r).release()` —— 与 `acquire` 严格配对。

### 4.4 超时

`lock_timeout_s` 由 `Harness.__init__` 注入（默认 `5.0`），**T7 显式设大（30s）**，使真死锁表现为「线程未结束」而不是「锁超时返回」。

### 4.5 不做 prune（v1.1 修正）

v1.0 提供 `prune(resource)`：若线程 A 正持锁、资源被删、`prune` 摘除锁表项，线程 B 会拿到**全新的锁** → 同资源并发写，互斥失效。

**裁决**：S2 **不提供** `prune`。阶段一资源数有界（单 run），锁表增长可接受；回收策略（引用计数 + 延迟回收）留到 S3 与 Delete 哨兵一起做。

### 4.6 与全局锁序的关系

资源锁属 **L3**，可同时持多把但必须升序；`budget`/`intents` 属 **L0 叶子锁**。因此：
- `_validate_locked` 里调 `intents.is_valid` 时**不得再取 intents 的锁**（`is_valid` 必须是无锁只读，见 §6.4）；
- 提交路径禁止在持资源锁时调 `budget.*`。

---

## 5. 读写路径

### 5.1 读（唯一路径）

```python
def get(self, resource: str):
    entry = self.manifest.get(resource)
    if entry is None:
        return None
    raw = self._blobs.get(entry.blob_hash)
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return raw                       # bytes 内容原样返回
```

- `version(resource)` = `manifest[r].version`（不存在 0）
- `content_hash(resource)` = `manifest[r].blob_hash`（不存在 `sha256(b"null")`）
- **类型约定**：只保证 `dict/list/str/int/float/bool/None/bytes` 往返；tuple/set 会退化为 list/str（`canonical_bytes` 的 `default=str`）。
- **None 歧义**：资源不存在与「内容为 None」都返回 `None`；约定**内容不得为 None**（`put`/`commit_batch` 对 None 内容返回 `INVALID_INPUT`）。

### 5.2 `put`（种子，**不推进版本**）

```python
def put(self, resource: str, content) -> ManifestEntry:
    """种初始内容：新资源 version=0；已存在则原地替换内容（仅供夹具/初始化）。"""
    if content is None:
        raise ValueError("资源内容不得为 None（与「不存在」歧义）")
    blob_hash = self._blobs.put(canonical_bytes(content))
    cur = self.manifest.get(resource)
    version = cur.version if cur else 0
    entry = ManifestEntry(version=version, blob_hash=blob_hash,
                          prev_hash=cur.blob_hash if cur else "")
    self.manifest[resource] = entry
    self._event_log.append(
        EventKind.WORKSPACE_COMMITTED, resource,
        {"resource": resource, "version": version, "blob_hash": blob_hash,
         "prev_hash": entry.prev_hash},
    )
    return entry
```

**为什么不推进版本**：11 个现有测试以 `version==0` 为基线（I10 断言 `version("r")==0`，I1/I2/I6/I8/U1/U2/U3/A1/A3 用 `base_version=0`）。`put` 是**种子**，不是运行期更新；运行期更新必须走 `commit_batch`。

### 5.3 `stage`（覆盖，不 setdefault）

```python
def stage(self, acb, resource, content):
    with self.lock_for(resource):               # 快照与提交之间不漂移（修 T2）
        ver = self.version(resource)
        acb.read_set[resource] = ver            # 每次 stage 视为「重新读」
        acb.base_version[resource] = ver        # == read_set 快照
        acb.pre_image[resource] = self.content_hash(resource)
        acb.staged[resource] = copy.deepcopy(content)
```

> v1.0 用 `setdefault`：二次 stage 会保留旧版本 → 误报 `CONFLICT`。改为**覆盖**，语义是「重新读一次」。

### 5.4 校验（两种入口，语义相同）

```python
def prepare(self, acb, w, lease, mode) -> Result:      # 公共：自己取锁（测试/单写直接用）
    with self.lock_for(w.resource):
        return self._validate_locked(acb, w, lease, mode)

def _validate_locked(self, acb, w, lease, mode) -> Result:   # 内部：调用方已持锁
    if not self._intents.is_valid(lease):
        return Result(Status.REJECT_STALE, {"lease_id": lease.lease_id, "reason": "lease_invalid"})
    if mode == Mode.COMMIT:
        expected = self._base_of(acb, w.resource)
        actual = self.version(w.resource)
        if expected != actual:
            return Result(Status.CONFLICT, {"resource": w.resource, "version": actual,
                                            "expected": expected, "diff": self._diff(w.resource)})
        for r, v in acb.read_set.items():                 # 读集合复验
            if v != self.version(r):
                return Result(Status.REJECT_STALE_READ, {"resource": r, "expected": v,
                                                         "actual": self.version(r)})
    if mode == Mode.COMPENSATE:
        if self.content_hash(w.resource) != w.applied_hash:
            return Result(Status.REJECT_COMPENSATE,
                          {"resource": w.resource, "expected_hash": w.applied_hash,
                           "actual_hash": self.content_hash(w.resource)})
    return Result(Status.VALID, {"resource": w.resource, "version": self.version(w.resource)})
```

**这解决了 v1.0 的 C5/C6 矛盾**：公共 `prepare` 自己取锁；`commit_batch` 持全部锁后调 `_validate_locked`。两条路径**不再重复加锁**，也不存在「禁止嵌套」与「一次性持有」的对立。

### 5.5 `Harness.commit_batch`（公共入口，提交点顺序不可调换）

```python
def commit_batch(self, acb, writes, mode=Mode.COMMIT) -> Result:
    resources = {w.resource for w in writes}
    held, ok = self.workspace._acquire_all(resources)          # ① 全序一次性
    if not ok:
        return Result(Status.UPSTREAM_FAILED, {"reason": "lock_timeout", "held": held})
    leases: dict[str, Lease] = {}
    try:
        for w in writes:                                       # ② 预留租约
            base = self.workspace._base_of(acb, w.resource)
            lease = self.intents.reserve(w.resource, acb.id, base, self.lease_ms)
            if lease is None:
                return Result(Status.DENIED, {"resource": w.resource})
            leases[w.resource] = lease

        for w in writes:                                       # ③ 全部校验
            r = self.workspace._validate_locked(acb, w, leases[w.resource], mode)
            if r != Status.VALID:
                return r

        for w in writes:                                       # ③′ 提交点前复验租约
            if not self.intents.is_valid(leases[w.resource]):
                return Result(Status.REJECT_STALE, {"resource": w.resource,
                                                    "reason": "lease_expired_before_commit"})

        blobs = {w.resource: self.blobs.put(canonical_bytes(acb.staged[w.resource]))
                 for w in writes}                              # ④ 内容先落盘

        drafts = [self._commit_record(acb, w, blobs[w.resource]) for w in writes]
        events = self.event_log.append_batch(drafts, notify=False)   # ⑤ 提交点（一次 fdatasync）

        for w in writes:                                       # ⑥ 投影（可重放）
            prev = self.workspace.manifest[w.resource].blob_hash \
                if w.resource in self.workspace.manifest else ""
            self.workspace._apply_locked(acb, w, blobs[w.resource], prev)
        return Result(Status.OK, {"resources": sorted(blobs), "seq": events[-1].seq})
    finally:
        for lease in leases.values():
            self.intents.revoke(lease)
        for r in reversed(held):
            self.workspace.lock_for(r).release()
        if 'events' in locals():                               # ⑦ 锁外通知（修 D1）
            self.event_log.notify_events(events)
```

`_commit_record`（v1.1 新增，消除未定义符号）：

```python
def _commit_record(self, acb, w, blob_hash) -> dict:
    cur = self.workspace.manifest.get(w.resource)
    return {
        "kind": EventKind.RESOURCE_COMMITTED,          # ← 必须与重建过滤一致
        "subject": w.resource,
        "payload": {
            "resource": w.resource,
            "version": (cur.version if cur else 0) + 1,
            "blob_hash": blob_hash,
            "prev_hash": cur.blob_hash if cur else "",
            "diff": {"prev_hash": cur.blob_hash if cur else "", "current_hash": blob_hash},
        },
        "producer": acb.id,
        "run_id": acb.run_id,
        "idempotency_key": f"{acb.run_id}:{acb.id}:{w.resource}",
    }
```

**关键点**：
- **通知在锁外**：`append_batch(notify=False)` → 释放资源锁后 `notify_events(events)`。否则订阅者回调在 L3 资源锁内执行，是 AB-BA 入口。
- **提交点前复验租约**（③′）：否则 ②→⑤ 之间 `advance` 时钟可绕过 fencing。
- **`_apply_locked` 只做投影**（manifest + 可选 link），不取锁（已持）。

### 5.6 `_apply_locked` 与可选物化

```python
def _apply_locked(self, acb, w, blob_hash, prev_hash):
    self._versions.record(w.resource, w.version_before, self.get(w.resource))  # 前像（供补偿）
    self.manifest[w.resource] = ManifestEntry(version=..., blob_hash=blob_hash, prev_hash=prev_hash)
    w.version_before = ...
    w.applied_hash = blob_hash
    if self._path_of is not None:                 # 默认 None → 不写磁盘
        self._blobs.link(blob_hash, self._path_of(w.resource))
```

- `path_of` **可选注入**（`WorkspaceManager(..., path_of=None)`）：默认不物化，读路径只看 manifest+blob。demo/bench 传 `lambda r: f"/tmp/ws/{r}"`。
- `_versions.record` 保留：A1/A3 依赖它取前像内容做 reverse patch。

### 5.7 `recover` / manifest 重建

```python
def _rebuild_from_events(self) -> None:
    for ev in self._event_log.replay():
        p = ev.payload or {}
        if ev.kind not in (EventKind.RESOURCE_COMMITTED, EventKind.WORKSPACE_COMMITTED):
            continue
        resource = p.get("resource") or ev.subject
        blob_hash = p.get("blob_hash")
        if not resource or not blob_hash:
            continue                              # 旧格式事件：跳过
        version = int(p.get("version", 0))
        cur = self.manifest.get(resource)
        if cur is not None and version < cur.version:
            continue                              # 版本回退：忽略（不静默覆盖）
        self.manifest[resource] = ManifestEntry(
            version=version, blob_hash=blob_hash, prev_hash=p.get("prev_hash", ""))
```

`WorkspaceManager.__init__` 末尾调用 → 同一份 WAL 重开，manifest 自动一致（T4 判据）。

---

## 6. 语义裁决（4 条，必须在方案里定死）

### 6.1 `base_version`：对「已读资源」恒等；未读资源 = 无读依赖

```python
def _base_of(self, acb, resource) -> int:
    if resource in acb.base_version:
        return acb.base_version[resource]
    if resource in acb.read_set:
        return acb.read_set[resource]
    return self.version(resource)        # 从未读 → 无读依赖，以提交时版本为基线
```

- **对已读资源**：`base_version == read_set[r]`（`stage` 同时写两者）→ 满足设计 §6.1 的恒等。
- **对未读资源**：OCC 语义下「没读就不需要校验读版本」，回退是**正确的**，不是放宽。
- **代价（写进文档）**：write-only 提交检测不到「中间被别人改过」。需要保护的写必须先 `stage`（`stage` 会写 `read_set`）。
- **对 phase1 S2 判据的澄清**：判据「base_version 恒等」应理解为「**已读资源**恒等」，与本节一致（已回写 phase1 §8 备注）。

### 6.2 `content_hash` 换 blob hash

`sha256(canonical_bytes(content))`，与 `blobs.put` 返回值**完全一致**。`w.applied_hash` 即 blob hash → 补偿复验与落盘内容天然一致。A1 的 `hash_of_A = content_hash("fileX")` 仍自洽。

### 6.3 `CONFLICT` 带 diff

阶段一 `_diff(resource)` 返回 `{prev_hash, current_hash}`（结构化 3-way 在 S3 的 `rebase.py`）。

### 6.4 `is_valid` 必须无锁只读

`is_valid` 在 `_validate_locked`（L3 资源锁内）被调用。若它内部再取 `intents._lock`（L0），就是 **L3→L0 反向取锁**，违反 §4.6。

**裁决**：`is_valid` 只读 `lease.valid` / `lease.epoch` / `lease.expires_at` 与时钟，**不取任何锁**；`reserve`/`revoke`/`bump_epoch` 才取锁。租约字段的可见性靠「`revoke` 写布尔 + GIL 下的原子读」保证（阶段一单进程可接受；跨进程需 MS9 的 fencing token）。

---

## 7. 迁移清单（完整，v1.1 补全）

| 文件 | 变更 |
|---|---|
| `harness/workspace.py` | manifest+blob 读模型；`ManifestEntry`；`lock_for`/`_acquire_all`；`put` 不推版本；`stage` 覆盖；`prepare` 公共取锁 + `_validate_locked`；`_apply_locked`；`_rebuild_from_events`；`get`/`version`/`content_hash` 改走 manifest；`path_of` 可选 |
| `harness/harness.py` | `commit_batch` 改为 §5.5（全序持锁 + 预留 + 复验 + 提交点 + 锁外通知）；`__init__` 增加 `blobs`/`clock`/`path_of`/`lock_timeout_s` 注入；`compensate`/`_compensate_one` 适配 `Result` |
| `harness/intent_registry.py` | `__init__(clock=None)`；`reserve` 写 `expires_at` + 归一化 `mode`（`"WRITE"→"W"`）；`is_valid` 查时间（`expires_at==0` 视为不过期）且**无锁**；`renew`/`renew_for` 检查过期并**续期** |
| `harness/event_log.py` | `append_batch(drafts, notify=True)`；新增 `notify_events(events)` |
| `harness/protocols.py` | `Workspace.prepare/apply -> Result`；补 `lock_for`/`put`/`get`/`version`/`content_hash`；`IntentRegistry.reserve` 的 `mode` 默认与实现统一 |
| `harness/runtime.py` | 删除 `stage` 后重复的 `read_set.setdefault`（`stage` 已负责）；`st.value` 继续可用（`Result.value`） |
| `harness/context.py` | `workspace.get()` 返回值从 deepcopy 变 JSON 解码（语义等价）；无代码改动，仅确认 |
| `tests/conftest.py` | `make_harness(run_limits=1000, *, clock=None, blobs=None)` |
| `tests/test_adversarial.py` | A12 的 `t.join()` → `join(timeout=5)` + `is_alive()` |
| `pyproject.toml` | 增加 `faulthandler_timeout = 10` |

**明确不改**：11 个以 `version==0` 为基线的测试（靠 §5.2 的「put 不推版本」保住）；A1/A3（靠 `_versions.record` 保住前像）。

---

## 8. 测试（4 组，判据可判定）

### T4 `test_ms1_commit_point_is_wal`（`@pytest.mark.ms1`）

```python
def test_ms1_commit_point_is_wal(tmp_path):
    wal_path = tmp_path / "wal.log"
    blobs = FileBlobStore(tmp_path / "blobs")
    log = WalEventLog(wal_path)
    ws = WorkspaceManager(log, IntentRegistry(), blobs=blobs)
    ws.put("r", "v0")
    h = Harness(event_log=log, blobs=blobs, workspace=ws)
    acb = _acb(h, "r")                       # 见下方 helper
    assert h.commit_batch(acb, [Write("r", content={"v": 1})]) == Status.OK
    hash_before = ws.content_hash("r")
    assert ws.version("r") == 1
    log.close()

    # 丢弃实例，用同一 WAL + 同一 blob 目录重建
    log2 = WalEventLog(wal_path)
    ws2 = WorkspaceManager(log2, IntentRegistry(), blobs=blobs)
    assert ws2.version("r") == 1             # 种子(0) + 提交(1) 都被重放
    assert ws2.get("r") == {"v": 1}
    assert ws2.content_hash("r") == hash_before
```

> 注意：`put` 的种子事件也必须被重放，否则 `version("r")` 只有 1 但 `get` 依赖的 blob 仍在——T4 用 `version==1` + `get` 双断言覆盖这一点。

### T5 `test_ms1_occ_readset_rejects_stale`

```python
def test_ms1_occ_readset_rejects_stale():
    h = make_harness()
    seed(h, "r", "v0"); seed(h, "dep", "d0")
    plan = h.planner.plan("T", [{"task_id": "T1"}, {"task_id": "T2"}])
    h.plans["T"] = plan
    a = admit(h, "T", plan, "T1"); b = admit(h, "T", plan, "T2")

    a.base_version["r"] = 0
    a.read_set["dep"] = 0                    # 读过 dep@0，但没读 r
    b.base_version["dep"] = 0
    assert h.commit_batch(b, [Write("dep", content="B")]) == Status.OK
    assert h.workspace.version("dep") == 1

    r = h.commit_batch(a, [Write("r", content="A")])
    assert r == Status.REJECT_STALE_READ     # base(r) 命中 → 隔离到读集合检查
    assert r.detail["resource"] == "dep"
    assert h.workspace.version("r") == 0
    assert h.workspace.get("r") == "v0"
```

### T5b `test_ms1_lease_expiry_uses_injected_clock`

```python
def test_ms1_lease_expiry_uses_injected_clock():
    clock = ManualClock(1_000_000)
    h = make_harness(clock=clock)
    seed(h, "r", "v0")
    acb = _acb(h, "r")
    lease = h.intents.reserve("r", acb.id, 0, 1000)
    assert lease.expires_at == 1_001_000

    clock.advance(999);  assert h.intents.is_valid(lease) is True
    clock.advance(1);    assert h.intents.is_valid(lease) is False   # 半开区间
    assert h.intents.renew(lease) is False
    assert h.workspace.prepare(acb, Write("r"), lease, Mode.COMMIT) == Status.REJECT_STALE

    # 对照：未过期时 renew 成功并续期
    lease2 = h.intents.reserve("r", acb.id, 0, 1000)
    clock.advance(500)
    assert h.intents.renew(lease2) is True
    assert lease2.expires_at == clock.now_ms() + 1000
```

### T7 `test_ms1_lock_order_no_deadlock`（`@pytest.mark.serial`）

```python
def test_ms1_lock_order_no_deadlock():
    h = make_harness()
    h.lock_timeout_s = 30.0                  # 让真死锁表现为「线程未结束」而非超时返回
    seed(h, "a", 0); seed(h, "b", 0)
    errors, results = [], []
    barrier = threading.Barrier(2)

    def worker(order):
        try:
            acb = _acb(h, order[0])
            acb.base_version.update({"a": 0, "b": 0})
            barrier.wait(timeout=5)
            results.append(h.commit_batch(
                acb, [Write(order[0], content=1), Write(order[1], content=1)]))
        except BaseException as e:
            errors.append(e)

    ts = [threading.Thread(target=worker, args=(("a", "b"),)),
          threading.Thread(target=worker, args=(("b", "a"),))]
    for t in ts: t.start()
    for t in ts: t.join(timeout=5)

    assert all(not t.is_alive() for t in ts), "线程未在期限内结束 → 疑似死锁"
    assert not errors
    assert all(r == Status.OK for r in results), f"提交失败（非 OK）: {results}"
    assert h.workspace.version("a") == 1 and h.workspace.version("b") == 1
    assert len(h.workspace._locks) >= 2, "仍是单锁 → 本测试无意义（假通过）"
```

**三条反假通过措施**：① 断言两次提交都 `OK`（锁超时会返回 `UPSTREAM_FAILED`，被这条挡住）；② 断言版本真的推进；③ 断言 `_locks >= 2`（证明是每资源锁）。

### helper

```python
def _acb(h, resource):
    acb = ACB(id=f"A-{resource}", run_id="R", task_id="T")
    node = h.budget.create_node(f"B-{acb.id}", 100)
    acb.budget_node = node
    return acb
```

---

## 9. 与上游的差异（需评审确认）

| # | 差异 | 理由 | 风险 |
|---|---|---|---|
| D-1 | `base_version` 对**未读资源**回退到提交时版本 | OCC「没读不校验」；`stage` 已保证已读资源恒等 | write-only 提交无保护；需显式 `stage` |
| D-2 | 公共 `prepare` 自己取锁；`commit_batch` 持全锁调 `_validate_locked` | 设计 §5.4 是逐阶段加锁，phase1 §5.1 是批量全序；用「两入口」同时满足 | 两个入口的语义必须由同一个 `_validate_locked` 保证 |
| D-3 | 锁超时不重试，返回 `UPSTREAM_FAILED` | 重试需随机抖动，属 MS4 Supervisor | 高争用下多一次失败 |
| D-4 | `put` 不推进版本（种子语义） | 11 个现有测试以 version=0 为基线 | 运行期误用 `put` 会绕过版本推进；靠文档 + 测试约束 |
| D-5 | 不兼容旧格式 WAL（无 `blob_hash` 的事件跳过） | 无线上数据 | 复用旧 WAL 会丢 manifest |
| D-6 | `WorkspaceVersionStore` 前像**不随 WAL 重建** | 需要 `prev_hash → blob` 反查，属 S3 的 Delete/补偿范围 | 崩溃后补偿退化为 PIVOT（S3 修，T6 在 S3） |
| D-7 | `is_valid` 无锁只读 | 避免 L3→L0 反向取锁 | 单进程可接受；跨进程需 MS9 fencing |
| D-8 | S2 不提供 `prune` | prune 会造成锁身份漂移、互斥失效 | 长跑进程锁表增长（阶段一有界） |

---

## 10. 风险与对策

| 风险 | 对策 |
|---|---|
| 换多锁后原有 49 测试出现竞态 | 所有批量路径统一走 `_acquire_all`；单资源走 `lock_for`；连跑 20 轮确认稳定 |
| manifest 重建依赖 payload，写错则重建错 | T4 用「丢弃实例再重建」；`_commit_record` 契约写死在 §5.5 |
| `get()` 类型漂移（tuple→list、bytes→raw） | 明确只保证 JSON 可序列化类型；bytes 原样返回；T4 只断言 dict/str |
| T7 flaky | `Barrier` + `join(timeout)` + `is_alive()` + `serial` + `faulthandler_timeout` |
| `put` 被误用于运行期更新 | docstring 写明「仅种子」；`commit_batch` 是唯一运行期写入口 |
| 锁表增长 | 阶段一有界；S3 引入引用计数回收 |

---

## 11. 评审修订记录（v1.0 → v1.1）

| # | v1.0 问题 | v1.1 修法 |
|---|---|---|
| **P0-1** | `put` 推进版本 → 打挂 11 个以 version=0 为基线的测试 | `put` 不推版本（§5.2） |
| **P0-2** | `put` 写 `WORKSPACE_COMMITTED` 但重建只认 `RESOURCE_COMMITTED` | 重建同时识别两种 kind（§5.7） |
| **P0-3** | `commit_batch` 归属二义；`detail` 无法透传 | 公共入口固定 `Harness.commit_batch`，workspace 只提供 `_validate_locked`/`_apply_locked`（§5.5） |
| **P0-4** | `path_of` 未定义 | 改为可选注入，默认 `None` = 不物化（§5.6） |
| **P0-5** | `renew` 不检查过期 → T5b 必红 | `renew`/`renew_for` 检查过期并续期（§3.3） |
| P1-1 | `make_harness` 未支持 `clock` | 迁移清单补 `conftest.py`（§7） |
| P1-2 | `Harness` 未注入 `clock`/`blobs` | 迁移清单补（§7） |
| P1-3 | `protocols.Workspace` 未同步 | 迁移清单补（§7） |
| P1-4 | T7 依赖的全序持锁未落到 `Harness` | §5.5 重写为全序持锁 |
| P1-5 | `validate`/`commit_record`/`_read_deps`/`lease[w]` 未定义 | 统一为 `_validate_locked`/`_commit_record`/`acb.read_set`/`leases[resource]`（§5.4/§5.5） |
| P1-6 | `_versions` 与新提交路径脱节 → A1/A3 会红 | `_apply_locked` 内 `_versions.record` 前像（§5.6） |
| P1-7 | `runtime.py`/`context.py` 未列入迁移 | 补（§7） |
| P1-8 | `Lease.mode` 取值不一致（`"W"` vs `"WRITE"`） | `reserve` 归一化（§7） |
| 高 | 订阅者回调在资源锁内（真死锁入口） | `append_batch(notify=False)` + 锁外 `notify_events`（§5.5） |
| 高 | 提交点前不复验租约 → 时钟可绕过 fencing | 增加步骤 ③′（§5.5） |
| 高 | 锁超时后 `finally` 释放未持有的锁 | `_acquire_all` 返回 `held`，只释放 `held`（§4.2/§4.4） |
| 高 | `prune` 造成同资源两把锁 | S2 不提供 `prune`（§4.5） |
| 高 | T7 用「锁超时返回」冒充不死锁 | 断言都 `OK` + 版本推进 + `_locks>=2`（§8 T7） |
| 中 | `stage` 用 `setdefault` → 二次 stage 误报冲突 | 改为覆盖（§5.3） |
| 中 | 重建无版本单调校验 | 版本回退则忽略（§5.7） |
| 中 | `get()` 对 bytes 触发 `json.loads` 崩溃 | try/except 回退原样 bytes（§5.1） |
| 中 | 「不存在」与「内容为 None」歧义 | 约定内容不得为 None（§5.1/§5.2） |
| 中 | 锁超时返回 `COMMIT_UNRECOVERABLE` 语义错 | 改 `UPSTREAM_FAILED`（§3.2） |
| 中 | `lock_timeout_ms` 未定义/未注入 | 提为 `Harness.lock_timeout_s`，默认 5s，T7 设 30s（§4.4） |
| 中 | `is_valid` 可能在 L3 内取 L0 锁 | 规定 `is_valid` 无锁只读（§6.4） |
| 低 | `acb.subscriptions` 未纳入复验 | 阶段一 `stage` 同步写 `read_set`，等价；差异记录在案（§5.4） |
