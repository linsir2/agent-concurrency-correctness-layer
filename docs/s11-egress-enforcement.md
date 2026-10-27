# S11：egress 强制点（外部库）+ 收据增量索引 + fencing 收口

| 项目 | 内容 |
|---|---|
| 版本 | **v1.2（实现完成 + 终审修订）** |
| 日期 | 2026-10-27 |
| 上游 | [S10 v1.2](s10-cross-process-reconciliation.md) §9.6（K-44/K-45/K-46） |
| 硬约束 | **调用外部库/外部工具，不自研** |
| 分波 | **S11a**（统一 gate + Action 字段，P0）→ **S11b**（K-44 + K-46，P0）→ **S11c**（DNS/SSRF，P1）→ **S11d**（Landlock 外部库，P1）→ **S11e**（netns/代理，P2，部署级） |
| 修订 | v1.0 → v1.1：修 4 P0 + 5 P1（§9） |

---

## 0. 调研结论（决定 8 个设计选择）

### 0.1 「不可绕过」的判定标准

| 方案 | 真不可绕过？ | 理由 |
|---|---|---|
| seccomp-BPF / Landlock | ✅ 内核 LSM，不可逆 | 但 Landlock 网络只到 **TCP 端口**（ABI v4=6.7；UDP 要 v10=6.15+） |
| netns 无路由 + nftables | ✅ 内核路由层默认拒绝 | **需 `CAP_NET_ADMIN`（root）** |
| eBPF cgroup hook | ✅ 内核态 | 需写 BPF C（触「自研」边界） |
| gVisor / Kata / Firecracker | ✅ 接近是 | 需容器运行时 |
| bwrap / firejail / nsjail | ❌ **否** | 用户态工具，**有逃逸 CVE**（bwrap CVE-2026-87766/41163） |
| Envoy / mitmproxy | ❌ 单独否 | 流量可绕过；**必须叠加 netns 强制** |

### 0.2 三条硬约束（评审确认，v1.0 全错）

1. **单进程 harness 拓扑不兼容**：agent 与 harness 同进程 → 无法「只把 agent 的 socket 放进 netns」；把整个 harness 放进 netns 会**同时切断 harness 自身出站**（`llm.py` 的 urllib）。→ **netns 只能用于子进程沙箱**，不能用于进程内。
2. **netns 无路由与 L7 代理物理互斥**：无路由 → 沙箱连不到宿主代理（`ENETUNREACH`）→ 第 1 层永远收不到流量。要收流量必须加 veth + 路由指向代理 → **推翻「无路由默认拒绝」**。二者是**替代关系**，不是叠加。
3. **`sandlock` API 是编造的**：真实 API 是 `Sandbox(fs_readable=..., net_allow=..., http_allow=...)` + **子进程** `sandbox.run([...])` 或进程内 `confine(sandbox)`；`confine()` **只认 Landlock 字段**，显式拒绝 network/seccomp/HTTP 配置。且 sandlock 要求 **Linux 6.12+ / Landlock ABI v6**。

### 0.3 外部库（真实可用）

| 库 | 真实 API | 许可证 |
|---|---|---|
| `sandlock` | `Sandbox(...)` + `sandbox.run(argv)` / `confine(sb)` | Apache-2.0 |
| `py-landlock` | `Landlock().allow_read(...).apply()` | MIT |
| `pyroute2` | `netns.create()` / `ipr.link('add', kind='veth')` | GPL-2.0-or-later OR Apache-2.0 |
| `mitmproxy` | addon `def request(self, flow)` + `tflow.tflow()` 测试 | MIT |
| `docker` SDK | 经容器运行时 | Apache-2.0 |

### 0.4 依赖策略（修 P0：默认无库 = 无强制）

**决策**：egress 库放 **`dependencies`（必需）**，或提供 **`--egress-required` 启动开关**（默认 fail-closed：无库 → 拒绝启动）。
**理由**：放 optional 等于「攻击者不装就绕过」——目标不成立。

### 0.5 内部接线点（调研确认）

| 接线点 | 位置 | 问题 |
|---|---|---|
| `_gateway_check` | [runtime.py:163-178](harness/runtime.py) | **仅覆盖 `tool`**；`token is None → False`（**不是**「无字段放行」） |
| `types.Action` | [types.py:257-266](harness/types.py) | **无 `network`/`data_class` 字段** → `getattr(...,None)` 恒放行 |
| `CapabilityToken` | [policy.py:21-46](harness/policy.py) | network 只精确 host；data_class 只字符串相等（**无分级序**） |
| 真实出站 | [llm.py:113-119](harness/llm.py) | `urllib` → `socket.create_connection` → **`getaddrinfo`**（不是 `gethostbyname`） |
| `_commit_staged` | [runtime.py:350-358](harness/runtime.py) | **直接调 `commit_batch`，不走 `_do_action`** → 统一前置覆盖不到 |

---

## 1. 目标

| # | 目标 | 判据 |
|---|---|---|
| G1 | `_gateway_check` 统一前置 + `Action` 显式字段 | S11-1 |
| G2 | K-44 收据增量索引 | S11-2 |
| G3 | K-46 fencing 绑 WAL（锁序正确） | S11-3 |
| G4 | DNS 钉住 + SSRF 阻断（完整 IP 列表） | S11-4 |
| G5 | Landlock（真实 `sandlock` API，子进程） | S11-5 |
| G6 | netns/代理（部署级，显式降级） | S11-6 |

---

## 2. S11a：统一 gate + Action 字段（P0）

### 2.1 语义拆分（修 P1：deny-by-default 与「无字段放行」冲突）

```python
def _gateway_check(self, acb, action) -> bool:
    """两句语义，不可合并：
    ① token 必须存在（deny-by-default）
    ② network/data_class 字段缺失 → 该维度不约束（本地动作如 stage/commit 不经 egress）
    """
    token = self.h._effective_token(acb)
    if token is None:
        return False                                  # ①
    network = action.network                          # 显式字段，不再 getattr
    if network and not self.h.policy.allows_network(token, network):
        return False                                  # ②
    data_class = action.data_class
    if data_class and not self.h.policy.allows_data_class(token, data_class):
        return False
    return True
```

### 2.2 覆盖范围（修 P1：`_commit_staged` 绕过）

- **统一前置只覆盖 `_do_action`**（5 个 kind：stage/commit/tool/request_hitl/finish）
- `_commit_staged` / `handle_commit_result` 重试**显式声明为「本地动作，不经 egress」**（文档 + 断言固定边界）
- **穷举断言**：遍历 `_HANDLERS` 全部 kind，断言带 `network="evil.com"` 的 action 必被拦，且 handler **未被执行**

### 2.3 Action 字段（向后兼容）

```python
# types.Action
network: Optional[str] = None
data_class: Optional[str] = None
```

**回归防护**：`bench/workloads.py` 的 `freeze` 用 `asdict(Decision)` → **Action 增字段会改 sha256** → 必须先 `regen_report`（R1 风险）。

---

## 3. S11b：K-44 + K-46（P0，纯 Python）

### 3.1 K-44：增量索引（修 P2：与回扫/截断/并发交互）

```python
# storage/wal.py
self._receipt_index: list[tuple[int, dict]] = []
self._receipt_index_seq: int = 0        # 已索引到的 seq

def receipts_from(self, watermark: int) -> list[dict]:
    with self._lock:                     # ← 索引读写进锁（修：原方案无锁）
        if self._receipt_index_seq < self._seq:
            for frame in self._frames[self._indexed_frame:]:   # 只解析新增帧
                ...
            self._receipt_index_seq = self._seq
    return [p for s, p in self._receipt_index if s > watermark]
```

**关键**：
- `_rescan_from_disk` 重建 `_frames` 时**同步重置** `_indexed_frame = 0` 与 `_receipt_index_seq = 0`（否则错位）
- `receipts_from` 仍走 `_scan_receipts` 做**链连续性校验**（索引只当「待处理队列」，不替代校验）
- WAL 截断/retention 后 seq 重置 → 索引游标一并重置

### 3.2 K-46：fencing 绑 WAL（修 P1：锁序违规）

**问题**：`_try_acquire_writer_lease` 在 `store.lock`（order=3）内调 `bump_epoch`；若 `bump_epoch` 改取 WAL 锁（order=2）→ **反向获取** → `LockOrderViolation` → 接管路径崩。

**修法**：新增独立 `epoch` 锁并纳入全序：
```
resource(0) → budget(1) → receipts(2) → wal(3) → epoch(4) → store(5) → eventlog(6)
```
`bump_epoch` 只取 `epoch` 锁（不与其它锁嵌套）；`wal.epoch` 与 WAL 同目录（`wal.log.epoch`），WAL 重建时一并清理。

---

## 4. S11c：DNS 钉住 + SSRF（P1，纯 Python）

### 4.1 真实出站路径（修 P1：注入点错误）

`llm._post` 走 `urllib → socket.create_connection → getaddrinfo`。**钉住逻辑必须包住 `getaddrinfo`**，不是 `gethostbyname`。

```python
# harness/egress/resolver.py
def resolve_and_pin(host: str, resolver=socket.getaddrinfo) -> str:
    infos = resolver(host, 0, proto=socket.IPPROTO_TCP)   # 取全 A/AAAA
    ips = {i[4][0] for i in infos}
    for ip in ips:
        if is_forbidden(ip):        # 完整列表，见 4.2
            raise EgressDenied(f"SSRF: {host} → {ip}")
    return sorted(ips)[0]           # 钉住，后续只连这个 IP
```

### 4.2 完整 IP 列表（修 P1：缺云 metadata）

```python
def is_forbidden(ip: str) -> bool:
    a = ipaddress.ip_address(ip)
    if a.is_private or a.is_loopback or a.is_link_local or a.is_multicast or a.is_reserved:
        return True
    # 云 metadata（IPv4 + IPv6）
    CLOUD_METADATA = {"169.254.169.254", "169.254.169.255", "100.100.100.200",
                      "fd00:ec2::254", "169.254.170.2"}
    return ip in CLOUD_METADATA
```

**连接后复核**：`connect` 后 `getpeername()` 比对钉住 IP；**重定向禁止二次解析**。

---

## 5. S11d：Landlock（P1，真实 API）

```python
# harness/egress/lsm.py（用真实 sandlock API）
try:
    from sandlock import Sandbox                      # Apache-2.0
    _HAS_SANDLOCK = True
except ImportError:
    _HAS_SANDLOCK = False

def run_sandboxed(argv: list[str], allow_hosts: set[str]) -> int:
    """**子进程**沙箱（不用进程内 confine —— 不可逆且会锁死主进程）"""
    if not _HAS_SANDLOCK:
        raise EgressUnavailable("sandlock 未安装 → fail-closed")
    sb = Sandbox(fs_readable=["/usr", "/etc", "/workspace"],
                 net_allow=[f"{h}:443" for h in allow_hosts] + ["udp://:53"],
                 http_allow=[...])
    return sb.run(argv)                              # 子进程，可重启
```

**关键**：
- **探测内核版本**：`landlock_abi_version()` < 6 → fail-closed
- **不用进程内 `confine()`**（不可逆，写错即锁死主进程）
- **UDP 需 ABI v10**（6.15+）→ 不足则 DNS 口径不可控 → 显式降级

---

## 6. S11e：netns / L7 代理（P2，部署级）

**诚实定位**：这是**部署级**能力，**不是库能力**。单进程 harness 无法进程内 netns。

```python
# harness/egress/netns.py（外部工具，需 root）
def spawn_in_netns(argv: list[str], name: str) -> subprocess.Popen:
    ip = shutil.which("ip")
    if ip is None:
        raise EgressUnavailable("ip 不可用 → 部署级 egress 不可用")
    subprocess.run([ip, "netns", "add", name], check=True)   # 需 CAP_NET_ADMIN
    return subprocess.Popen([ip, "netns", "exec", name, *argv])
```

**二选一**（不可叠加）：
- **A：netns 无路由**（默认拒绝，无代理）
- **B：netns + veth + 路由到代理**（有 L7 ACL，但放弃「无路由」语义）

**降级**：无 root / 无 `ip` → `EgressUnavailable`，**显式登记**（不静默 skip）。

---

## 7. 测试（三件套：正对照 + 负对照 + 环境 invalid 显式登记）

| # | 用例 | 正对照 | 负对照 |
|---|---|---|---|
| S11-1 | 统一 gate | 遍历 `_HANDLERS` 全 kind，带 evil network → 拦且 handler 未执行 | monkeypatch gate 恒 True → handler 被调用 |
| S11-2 | K-44 增量 | 枚举计数：N 次 commit 总量 ≤ N+K（线性） | 全量实现 ≥ N(N-1)/2（二次）→ 必超 |
| S11-3 | K-46 fencing | 共享 WAL 不同 store root → bump 后旧实例被拒 | 旧设计（store.json）→ 互不 fence（反例） |
| S11-4 | DNS/SSRF | `getaddrinfo` 注入两次不同 IP → 只解析一次 | 直连 `getaddrinfo` 绕过 → 被 peer 复核拦 |
| S11-5 | Landlock | `sandbox.run` 子进程连未授权端口 → 失败 | 未 sandbox 的子进程 → 成功（反例） |
| S11-6 | netns | `unshare -Urn` 子进程 connect → `ENETUNREACH`（非 ECONNREFUSED） | 去掉 `--net` → 结果不同 |
| S11-7 | 降级 | 无 `sandlock`/`ip` → `EgressUnavailable`（拒服务） | 库在（fake）→ 返回 True 且真跑 |
| S11-8 | Action 兼容 | 现有 250 用例的 Action 构造不受影响 | — |

**环境 invalid 登记**：`unshare` 抛 `EPERM` → `pytest.xfail`（**不计 pass**）+ 在报告显式登记。

---

## 8. 迁移清单

| 文件 | 变更 |
|---|---|
| `harness/egress/__init__.py`（新） | 编排 + `EgressUnavailable` |
| `harness/egress/resolver.py`（新） | DNS 钉住 + 完整 SSRF 列表 |
| `harness/egress/lsm.py`（新） | 真实 `sandlock` API（子进程） |
| `harness/egress/netns.py`（新） | 外部工具封装（部署级） |
| `harness/policy.py` | `allows_network` 通配/CIDR/端口；`allows_data_class` 分级序 |
| `harness/types.py` | `Action` 增 `network`/`data_class` |
| `harness/runtime.py` | `_gateway_check` 统一前置；`_commit_staged` 显式排除 |
| `harness/storage/wal.py` | K-44 增量索引（锁内 + 回扫重置）；K-46 `wal.epoch` |
| `harness/harness.py` | `_wire_wal_fencing` 改绑 WAL；epoch 锁纳入全序 |
| `harness/storage/_lock.py` | `LOCK_ORDER` 加 `budget`/`epoch` |
| `pyproject.toml` | egress 库入 `dependencies`（或 `--egress-required` 开关） |
| `tests/` | `test_s11_egress.py`、`test_s11_receipt_index.py` |

---

## 9. 评审修订记录（v1.0 → v1.1）

### 9.1 P0（4 项）

| # | v1.0 问题 | 修法 |
|---|---|---|
| **P0-1** | **netns 需 root + 单进程拓扑不兼容**：无法「只把 agent socket 放进 netns」；整个 harness 放进会切断自身出站 | netns 定位为**子进程沙箱**（部署级）；进程内不做 |
| **P0-2** | **`sandlock` API 是编造的**（`Landlock`/`HttpAcl`/`apply()` 不存在）；`confine()` 只认 Landlock 字段；要求 6.12+ | 用真实 `Sandbox(...)` + **子进程** `sandbox.run()`；探测内核版本 |
| **P0-3** | **netns 无路由与 L7 代理物理互斥**（无路由 → 连不到代理） | 明确为**二选一**（A 无路由 / B veth+代理），不叠加 |
| **P0-4** | **默认部署无 egress 库 = 无强制**（optional-deps 可被不装绕过） | egress 库入 `dependencies` 或 `--egress-required` fail-closed 启动 |

### 9.2 P1（5 项）

| # | 问题（反例） | 修法 |
|---|---|---|
| 1 | **`_gateway_check` 提升破坏 `test_s9_1`**（裸 `Harness()` 无 token → 被拦成 DENIED）；「无字段放行」与 deny-by-default **自相矛盾** | 语义拆分（① token 必须存在 ② 字段缺失则该维度不约束）；受影响路径显式装 token |
| 2 | **`_commit_staged` 不走 `_do_action`** → 统一前置覆盖不到 | 显式声明为「本地动作」+ 断言固定边界 |
| 3 | **DNS 钉住可被绕过**（应用层函数，agent 直接 `getaddrinfo`）；注入点错（`gethostbyname` vs 真实 `getaddrinfo`）；IP 列表缺 IPv6/CGNAT/**阿里云 metadata `100.100.100.200`** | 包住 `getaddrinfo` + `getpeername` 复核 + 完整列表 |
| 4 | **K-44 索引与回扫/截断/并发交互**：`_rescan_from_disk` 重建 `_frames` 但索引不失效；索引无锁；丢掉链校验 | 回扫时重置索引游标；索引读写进锁；保留 `_scan_receipts` 校验 |
| 5 | **K-46 锁序违规**：`store.lock` 内调 `bump_epoch`（改取 WAL 锁）→ 反向获取 → 接管崩 | 新增独立 `epoch` 锁并纳入 `LOCK_ORDER` |

### 9.3 P2（已修）

| 问题 | 修法 |
|---|---|
| S11-1/2/3 默认 skip → **假绿** | 三件套（正/负对照 + `xfail` 显式登记，**不计 pass**） |
| S11-5「O(N) 非 O(N²)」判据不可证 | 枚举计数 + 线性上界 + 二次下界（双向钉死） |
| `flow.metadata["data_class"]` 无写入源 | 抽纯函数 `decide_acl(host, data_class)`（不依赖 mitmproxy 也可测） |
| `bench freeze` 因 Action 增字段改 sha256 | 先 `regen_report` 再跑 CI |
| 覆盖率门禁（新增降级分支不可覆盖） | `# pragma: no cover` 或补降级用例 |

### 9.4 新增已知边界（v1.1）

| # | 边界 |
|---|---|
| K-49 | netns/代理为**部署级**，需 root；库只提供薄封装 + 显式降级 |
| K-50 | Landlock 网络只到 **TCP 端口**；UDP 需 ABI v10（6.15+）→ 不足则 DNS 口径不可控 |
| K-51 | `_commit_staged` 为本地动作，**不经 egress gate**（显式声明） |
| K-52 | 应用层 DNS 钉住**不能替代**内核层强制（agent 可直连 IP） |

---

## 10. 决策（v1.1 定案）

| # | 问题 | 结论 |
|---|---|---|
| Q1 | egress 库放哪 | **`dependencies`（必需）** 或 `--egress-required` fail-closed |
| Q2 | 外部二进制 vs Python 绑定 | **两者**：netns/nftables 走 `ip`/`nft`；LSM 走 `sandlock` |
| Q3 | 强制点优先级 | **S11a（统一 gate）→ S11b（K-44/K-46）→ S11c（DNS）→ S11d（Landlock）→ S11e（netns，部署级）** |
| Q4 | netns 与代理 | **二选一**（A 无路由 / B veth+代理），不叠加 |
| Q5 | 进程内 `confine()` | **不用**（不可逆，写错锁死主进程）→ 用子进程 `sandbox.run()` |

---

## 11. 实现落点与环境探测登记（S11c/d/e 落地，2026-10-27）

### 11.1 探测命令与结果（本仓库运行环境，如实记录）

| 探测项 | 命令 | 结果 | 结论 |
|---|---|---|---|
| 内核 | `uname -r` | `6.18.5` | ≥ 6.12，**但**见下一行 |
| Landlock ABI | `syscall(444, NULL, 0, LANDLOCK_CREATE_RULESET_VERSION)` | `-1 / errno 38 (ENOSYS)` → **ABI=0** | 容器内 Landlock **不可用**（syscall 被过滤/未编入） |
| `sandlock` | `pip install sandlock` | `No matching distribution found` | **装不了**（索引无此包） |
| `py-landlock` | `pip index versions py-landlock` | 同上 | **装不了**（备选也不可用） |
| `mitmproxy` | `pip index versions mitmproxy` | `12.2.1` 可解析 | 可用（仅 L7，单独不构成强制，§0.1） |
| `pyroute2` | `pip index versions pyroute2` | `0.9.6` 可解析 | 可用（netns 编程式替代 `ip` 的候选） |
| `ip netns` | `ip netns add <name>` | `mount --make-shared /run/netns failed: EPERM` | 无 `CAP_NET_ADMIN` → **部署级 netns 不可用** |
| `unshare -Urn` | `unshare -Urn true` | 成功 | userns 内 netns **可用**（S11-6 实跑口径） |

### 11.2 依赖策略落点（§0.4 / Q1 的第二条路径）

`sandlock` / `py-landlock` 均不可解析，故不能放 `dependencies`（会导致安装即失败）。
采用 §0.4 允许的 **fail-closed 开关**：

| 位置 | 内容 |
|---|---|
| `pyproject.toml` | `dependencies = []`；`optional-dependencies.sandbox = ["sandlock"]`、`landlock-alt = ["py-landlock"]`、`proxy = ["mitmproxy"]`、`egress = [三者]` |
| `Harness.__init__(egress_required=...)` | `True`=**构造即校验**，无强制点 → `EgressUnavailable`（生产拒绝启动）；`None`（默认）=构造可跑但**任何出站** fail-closed；`False`=显式降级并留痕（`h.egress.degradations`） |
| 环境变量 | `HARNESS_EGRESS_REQUIRED=1/0` 覆盖默认值（部署侧无需改代码） |

**为什么默认不是 `True`**：单进程 harness 的本地动作（stage/commit）不经 egress（K-51），
把「无库」等同于「不能构造 Harness」会让纯本地路径全部无法运行；但**出站仍然 fail-closed**
（无强制点 → `EgressUnavailable`），「不装库就绕过」的目标不成立。生产入口必须显式
`egress_required=True`（或 `HARNESS_EGRESS_REQUIRED=1`）。

### 11.3 降级方案（**不自研** seccomp / Landlock）

| 优先级 | 方案 | 前置条件 | 落点 |
|---|---|---|---|
| 1 | `sandlock` 子进程 `sandbox.run(argv)` | 索引可解析 + Landlock ABI ≥ 6 | `harness/egress/lsm.py`（已实现，`.[sandbox]`） |
| 2 | `py-landlock`（MIT）`Landlock().allow_read(...).apply()` 包装**子进程** | 同上，仅 ABI 门槛略低 | 替换 `lsm.py` 的 `Sandbox` 适配层（`build_sandbox` / `run_sandboxed` 接口不变） |
| 3 | 部署级 `ip netns`（root + `CAP_NET_ADMIN`）或 `unshare -Urn`（userns，无需 root） | 见 §11.1 | `harness/egress/netns.py` + 本文件 §6 |
| 4 | 以上都不可用 | — | **拒绝出站**（`EgressUnavailable`），不得用应用层 DNS 钉住冒充内核强制（K-52） |

### 11.4 测试登记（三件套：正对照 + 负对照 + 环境 invalid 显式登记）

`tests/test_s11_egress.py`（52 passed / 2 xfailed）+ `tests/_egress_env.py`（登记表，
`pytest_terminal_summary` 统一打印）：

| 用例 | 本环境结果 | 说明 |
|---|---|---|
| S11-4 DNS 钉住 / SSRF / peer 复核 / 重定向 | **实跑通过**（10 项，含 17 个禁止 IP 参数化） | 纯 Python，不依赖内核能力 |
| S11-5 正对照（Landlock 子进程连未授权端口） | **xfail**：`sandlock 未安装` | 环境 invalid 显式登记，**不计 pass** |
| S11-5 负对照（未沙箱子进程连本地端口 → 成功） | **实跑通过** | 证明目标端口本身可达 |
| S11-6 正对照（`unshare -Urn` → `ENETUNREACH`） | **实跑通过** | 断言 errno 名，非 `ECONNREFUSED` |
| S11-6 负对照（去掉 `--net` → 结果不同） | **实跑通过** | `unshare -Ur` 下本地端口 `OK` |
| S11-6 `ip netns`（CAP_NET_ADMIN） | **xfail**：`mount --make-shared ... EPERM` | 显式登记，**不计 pass** |
| S11-7 降级 / 假后端真跑 / 三态 guard | **实跑通过** | fake `sandlock` 的返回码来自**真跑**子进程（`exit 7`） |

**禁止静默 skip**：`test_s11_7_no_silent_skip` 扫描本文件源码，禁止 `skip` 语义；
所有 `pytest.xfail` 必须经 `_egress_env.register()` 生成 reason。

---

## 11. 实现后对抗评审（v1.1 → v1.2）

S11 分三波（S11a/b → S11c/d/e → 终审修复），随后终审。**5 高 + 11 中/低已修**。

### 11.1 环境探测（决定依赖策略）

| 探测 | 结果 | 影响 |
|---|---|---|
| 内核 | 6.18.5 | 版本够 |
| Landlock ABI | `ENOSYS` → **ABI=0** | 容器内 Landlock 不可用 |
| `pip install sandlock` | **不可用** | 装不了 |
| `ip netns add` | `EPERM`（无 CAP_NET_ADMIN） | 部署级 netns 不可用 |
| `unshare -Urn` | 成功 | S11-6 正对照**真跑** |

**决策**：egress 库放 `optional-dependencies` + **出站 fail-closed**（`EgressGuard` 三态 + `HARNESS_EGRESS_REQUIRED`）；生产入口显式 `egress_required=True`。

### 11.2 高（5 项）

| # | 问题（反例） | 修法 |
|---|---|---|
| **H-1** | **egress 未接线**：`EgressGuard` 只在 `__init__` 构造，**无出站路径调 `ensure()`**；`llm._post` 直接 `urlopen`（SSRF + 凭证外泄） | `llm._post` 入口 `guard.ensure()` + `build_opener()`（补 `HTTPErrorProcessor`）+ `pinned_connection`；**3xx 抛 `EgressDenied`，不跟随** |
| **H-2** | **minted token 一次性**：`Signer.verify` 的 jti 去重把第二次验签当重放 → 多动作任务第 2 个动作必失败 | `PolicyEngine._resolve` 按 token 值**缓存已验签 payload**；jti 去重只承担消息重放语义 |
| **H-3** | **干净重启被永久 fence**：`__init__` 从不调 `_restore_epoch()` → h1 commit+bump(1)+close → 新 h2 同 root/WAL 首写 `REJECT_STALE` | `__init__` 取租约**前**调 `_restore_epoch()` |
| **H-4** | **WAL 重建 epoch 归零 → 旧写者复活** | 新增 `wal.log.generation`（UUID）；append 前校验 generation，旧写者一律 `FencingRejected` |
| **H-5** | **并发构造 Wal 截断在途帧**：`__init__` 的 `_load()` 不在锁内 → 截断写者未写完的 body → `WalCorrupt` | `_load()` 整体包进 `wal.lock` |

### 11.3 中/低（11 项，择要）

| 问题 | 修法 |
|---|---|
| 接管窗口（先落租约后 bump） | **先 bump_epoch 再写租约**；落租约前复读，被抢则放弃 |
| MODE_A/B 未互斥（fail-open） | netns 模式写标记文件，复用前校验，不匹配拒绝 |
| `data_class` 默认 `"*"` fail-open | 默认改最低等级 `public` |
| ABI 6–9 仍加 `udp://:53` 不检查 DNS | `required=True` 要求 `dns_enforceable` |
| `receipts_from` O(N²) | 用 `bisect` 从 watermark 切片 |
| `receipts_from` 不保留链校验 | 标注为 **RAW API**，校验唯一归 `_scan_receipts` |
| netns 失败无清理 / veth 全局名 | try/finally 清理；`{name}-h`/`{name}-ns` |
| 垃圾 token 放行 | `resolve_token` 校验可解析 + 身份绑定 |
| 3 个**恒真测试**（线性/边界/重定向） | 重写：`records()` 调用为 0 + 索引量 > 0 + 索引==全量；本地 302 server 真拒绝 |

### 11.4 降级方案（不自研 seccomp/Landlock）

1. 索引可解析且 ABI≥6 → 装 `.[sandbox]` 用 `sandlock`
2. 否则用 `py-landlock` 包装**子进程**（接口不变）
3. 部署级用 `ip netns`（root）或 `unshare -Urn`（userns，已实测 `ENETUNREACH`）
4. 都不可用 → **拒绝出站**，绝不拿应用层 DNS 钉住冒充内核强制（K-52）

### 11.5 新增已知边界（v1.2）

| # | 边界 |
|---|---|
| K-53 | `sandlock` 在 PyPI 不可用（探测结论）→ 降级路径见 §11.4 |
| K-54 | Landlock ABI=0（容器内）→ LSM 强制不可用；`unshare -Urn` 可跑 |
| K-55 | egress 库为 optional；生产需显式 `egress_required=True` 才 fail-closed 启动 |
| K-56 | `wal.log.generation` 与 WAL 同目录；整目录清理会轮换 generation（旧写者被拒，方向安全） |
