# TLA+ 模型（I1 / I2 / I4 / I7）

S24 差距清单第 5 项「TLA+ 模型」的最小交付：对 `harness/spec/invariants.py`
中的 4 条核心不变量给出 TLA+ 模型，并用 TLC 对「正确」与「broken」两套配置实测。
语义对齐 `docs/multi-agent-harness-design-final.md` §18.4 与 §5.4
（`ws.prepare` / `ws.apply` 的 CAS + 短临界区，I7 只建模账本状态机）。

设计约束：状态空间小（`Writers = {w1, w2}`、`Resources = {r1}`），TLC 秒级完成；
每套正确配置都校验 `TypeOK` + 对应不变量；broken 配置**只改一个开关常量**。

| 编号 | 模型文件 | 被验证的不变量 | 正确 cfg 状态数（生成 / 去重） |
|---|---|---|---|
| I1 单写者提交 | `I1SingleWriter.tla` | `AtMostOneWriter` | 27 / 15 |
| I2 无丢失更新 | `I2NoLostUpdate.tla` | `NoLostUpdate` | 23 / 14 |
| I4 能力封闭 | `I4Capability.tla` | `CapabilityClosed` | 3 / 2 |
| I7 effect 有效一次 | `I7EffectExactlyOnce.tla` | `EffectAtMostOnce` | 21 / 8 |

## 模型语义

### I1 单写者提交（`AtMostOneWriter`）

写者先非阻塞预留（TCC intent reserve，允许同一资源多人同时预留），再进入提交层短
临界区；`Commit` 一步完成「版本递增 + 释放资源锁」，对应 §5.4 `ws.apply` 中的
`atomic_replace` 与 `version += 1` 在同一临界区内原子发生。

```tla
AtMostOneWriter ==
    \A r \in Resources :
        Cardinality({w \in Writers : pc[w] = "crit" /\ target[w] = r}) <= 1
```

### I2 无丢失更新（`NoLostUpdate`）

`Read` 记录 `base_version`（对应 `acb.read_set`）；`Commit` 只有在
`base_version == 当前版本` 时才生效（§5.4 的 CAS）。每次成功提交必须消耗一个
不同的 base 版本，因此：

```tla
NoLostUpdate ==
    \A r \in Resources :
        commits[r] = Cardinality(bases[r])
```

### I4 能力封闭（`CapabilityClosed`）

写者持有能力集合 `Held(w)`，资源要求 `NeededCaps`；`Access` 只在
`NeededCaps \subseteq Held(w)` 时启用（fail-closed）。所有访问记录必须落在授权表内：

```tla
CapabilityClosed == accessed \subseteq AuthorizedPairs
```

> TLC 的 `.cfg` 不支持元组常量，故能力表用集合常量 `WriteHolders` 表达：
> `w1` 只读，`w2` 持有 `write`，资源 `r1` 要求 `write`。

### I7 effect 有效一次（`EffectAtMostOnce`）

只建模**账本状态机**，不建模外部世界（§18.4 的 LIMBO 教训）。`Deliver` 是
at-least-once（允许重复投递）；`Apply` 在 `Dedup = TRUE` 时要求
`ledger[idem_key] = "none"`，由账本按幂等键去重：

```tla
EffectAtMostOnce ==
    \A k \in Keys : applyCount[k] <= 1
```

## 如何运行 TLC

本仓库不引入任何运行时依赖，TLC 由外部 `tla2tools.jar` 提供。

```bash
curl -sSL -o /tmp/tla2tools.jar \
  https://github.com/tlaplus/tlaplus/releases/latest/download/tla2tools.jar

export TLA2TOOLS_JAR=/tmp/tla2tools.jar
python3 scripts/check_tla.py                 # 4 正确 + 4 broken 全跑
python3 scripts/check_tla.py --only I2       # 只跑一个模型
python3 scripts/check_tla.py --require       # CI 门禁：缺 Java/jar → exit 2
python3 scripts/check_tla.py --jar /path/to/tla2tools.jar
```

- 正确 cfg 期望 TLC exit 0；
- broken cfg 期望 TLC exit 非 0 且输出含 `Invariant <名字> is violated.`；
- 单次 TLC 超时 60s；未加 `--require` 且缺 Java/jar 时打印 `SKIP` 并 exit 0。

手工运行单次 TLC（`-deadlock` 关闭终态死锁检测，`-metadir` 避免污染仓库）：

```bash
java -cp /tmp/tla2tools.jar tlc2.TLC -deadlock -workers 1 \
  -metadir /tmp/tlc-states -config spec/tla/I1SingleWriter.cfg spec/tla/I1SingleWriter.tla
```

pytest（无 jar 时 skip，设置了 `TLA2TOOLS_JAR` 却缺 Java 时 fail）：

```bash
TLA2TOOLS_JAR=/tmp/tla2tools.jar python3 -m pytest tests/test_s24_tla.py -q
```

## broken 配置演示的反例（实测 TLC exit 12）

| 文件 | 开关 | 反例 |
|---|---|---|
| `I1SingleWriter.broken.cfg` | `EnforceMutex = FALSE` | 去掉提交层互斥：`w1`、`w2` 同时处于 `r1` 的 `"crit"` → `AtMostOneWriter` 违反 |
| `I2NoLostUpdate.broken.cfg` | `EnforceCAS = FALSE` | 两写者均读到版本 0 后各自提交 → `commits[r1] = 2` 而 `bases[r1] = {0}`（基数为 1）→ `NoLostUpdate` 违反 |
| `I4Capability.broken.cfg` | `EnforceCaps = FALSE` | 只读的 `w1` 访问要求 `write` 的 `r1` → `accessed = {<<w1, r1>>}` 不在授权表内 → `CapabilityClosed` 违反 |
| `I7EffectExactlyOnce.broken.cfg` | `Dedup = FALSE` | 同一个 `k1` 投递→生效→重复投递→再次生效 → `applyCount[k1] = 2` → `EffectAtMostOnce` 违反 |

## 抽象边界

- 每个模型只建模单资源（`Resources = {r1}`）上的提交层行为；写者内部的多步骤
  工具执行与 LLM 推理不在模型内（对应「临界区必须短」的设计原则）。
- I2 抽象掉 rebase 的具体内容，只用「`Rebase` 后重新读取最新版本」表示
  `handle_commit_result` 的冲突处理路径。
- I7 抽象掉外部副作用与 in-flight 上限，因此它验证的是**账本层面**的
  at-most-once；真实的 exactly-once 依赖 §5.8 EffectJournal 的持久化对账。
- 模型只检查安全不变量（safety），不检查活性（liveness / 公平性）。
