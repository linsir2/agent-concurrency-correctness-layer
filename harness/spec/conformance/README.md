# RSC conformance corpus

把 `harness/spec/` 的四份契约（资源模型 / 准入协议 / 收据 / 不变量）导出为
**语言无关的 JSON 测试向量**。任何语言的实现（Python / Go / TS / Rust）只要
能解析 JSON，就可以自测「我是否符合 RSC 规范」，不需要读本仓库的 Python 代码。

## 目录

```text
manifest.json            语料清单：版本、文件 sha256、向量总数、可执行不变量
invariants.json          I1–I14 目录（编号 / 名称 / 描述 / 参考测试名）
vectors/protocol.json    准入真值表（GRANT / WAIT / DIE，8 种组合）
vectors/resources.json   资源类型 → 版本语义；路径 → 资源类型
vectors/receipt-build.json   收据构建：payload sha256 + HMAC 签名（冻结值）
vectors/receipt-verify.json  链校验三态：true / null / false
```

## 一条向量的结构

```json
{
  "id": "receipt.verify.tampered_signature",
  "kind": "receipt.verify",
  "description": "链校验三态：tampered_signature",
  "invariants": ["I13"],
  "input": {"receipts": [], "secret": "…"},
  "expect": {"result": false}
}
```

- `kind` 决定怎么执行（见下表）；
- `expect` 是**冻结字面量**，不是由被测实现现场生成——这样才能发现规范漂移；
- `invariants` 声明该向量覆盖哪条不变量（`manifest.totals.executable_invariants`
  由它汇总）。

## 执行协议（第三方 runner 最小约定）

| `kind` | `input` | `expect` |
|---|---|---|
| `protocol.decide` | `contract_conflict` / `read_write_conflict` / `write_write_overlap`（布尔） | `verdict` ∈ GRANT/WAIT/DIE，`reason` |
| `resources.semantics` | `kind`（资源类型名） | `semantics` ∈ monotonic/content_hash/idempotency_key |
| `resources.path` | `path` | `kind`（参考实现的推断结果） |
| `receipt.build` | `build_receipt` 的构造参数（另有顶层 `secret`） | `payload_sha256`（SHA-256 of UTF-8 payload）、`signature`、`signed` |
| `receipt.verify` | `receipts`（收据 dict 列表）、`secret` | `result` ∈ true / null / false（三态） |

收据 payload 的规范形式：JSON、`sort_keys=True`、字段集合见
`harness/spec/receipt.py` 的 `Receipt.payload()`；hash-chain 链接值 =
`sha256(payload)[:16]`；签名 = `HMAC-SHA256(HMAC(auth_secret, "receipt-v1"), payload)`
全量 64 hex；`secret` 为空 → `signature == ""`（未签名，不是篡改）。

## 自检

```bash
python3 -m harness.spec.corpus --check        # 磁盘语料 vs 参考实现，exit 0/1
python3 -m harness.spec.corpus --check --json # 机器可读报告
python3 -m harness.spec.corpus --write        # 开发时显式重写（会改仓库文件）
```

`--check` 同时做三件事：文件 sha256 与 manifest 一致、每条向量对参考实现执行通过、
manifest 的计数/可执行不变量汇总一致。**默认只读**，不会写任何文件。

## 版本规则

- 加字段 / 加向量 → `corpus_version` minor（向后兼容）；
- 删字段 / 改语义 / 改冻结期望 → major（旧实现需要迁移）；
- 第三方实现应按 `corpus_version` 钉住自己声明兼容的语料版本。

## 不覆盖的边界

I6 / I8–I12 / I14 需要真实调度、崩溃注入或时间推进，不是纯函数可判定的；
语料只在 `invariants.json` 里登记它们由哪条测试覆盖，**不伪造向量**。
签名之外的安全属性（密钥管理、传输）不在本语料范围内。
