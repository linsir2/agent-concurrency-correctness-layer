"""S24-5：合规语料（conformance corpus）—— 把 `harness/spec` 的契约导出成
**语言无关、可执行**的测试向量，第三方实现可据此自测。

设计要点（对齐 JSON Schema Test Suite / W3C test suite 的通行做法）：

- **一份语料 = 一个目录**：`manifest.json` + `vectors/*.json` + `invariants.json`；
- **向量只依赖 JSON 解析**：每条用例自带 `kind`（怎么执行）、`input`、`expect`
  （**冻结字面量**），期望值绝不由被测实现现场生成；
- **版本化**：`corpus_version`（加字段 = minor，删/改语义 = major）；
- **本模块是参考实现**：`--check` 校验磁盘语料与本实现一致（防规范漂移），
  `--write` 才显式重新生成（**默认只读**）。

语料不覆盖的东西（诚实边界）：签名算法之外的实现细节、I6/I8–I12/I14 的
运行期语义（需要真实调度/崩溃注入，语料只登记「由哪条测试覆盖」）。
"""

from __future__ import annotations

import hashlib
import json
import pathlib
import sys
from collections.abc import Mapping

from .invariants import INVARIANTS
from .protocol import decide
from .receipt import Receipt, build_receipt, verify_chain_detailed
from .resources import RESOURCE_KINDS, ResourceKind, ResourceSpec, resource_kind

SCHEMA_VERSION = 1
CORPUS_VERSION = "1.1.0"
SPEC_VERSION = "1.0.0"

# 语料目录（相对 `harness/spec/`）
CORPUS_ROOT = pathlib.Path(__file__).resolve().parent / "conformance"
_VECTOR_FILES = ("vectors/protocol.json", "vectors/resources.json",
                 "vectors/receipt-build.json", "vectors/receipt-verify.json",
                 "vectors/fencing.json", "vectors/plan.json",
                 "invariants.json")

# 冻结用例里使用的签名密钥（公开值，不是生产 secret）
_FIXTURE_SECRET = "corpus-fixture-secret"


def _canon(obj) -> bytes:
    """规范 JSON 字节：排序键 + 2 空格缩进 + 末尾换行（可 diff、可哈希）。"""
    return (json.dumps(obj, sort_keys=True, indent=2, ensure_ascii=False)
            + "\n").encode("utf-8")


def _sha256_file(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------- 向量
def _protocol_vectors() -> list[dict]:
    """GRANT / WAIT / DIE 的完整真值表（3 个布尔 = 8 种组合）。"""
    out: list[dict] = []
    names = ("contract_conflict", "read_write_conflict", "write_write_overlap")
    for mask in range(8):
        flags = {name: bool(mask & (1 << i)) for i, name in enumerate(names)}
        verdict = decide(**flags)
        hit = [n for n in names if flags[n]]
        out.append({
            "id": "protocol.decide." + "".join(
                "1" if flags[n] else "0" for n in names),
            "kind": "protocol.decide",
            "description": ("准入真值表：" + ", ".join(hit)) if hit else "无冲突 → 放行",
            "invariants": ["I1"],
            "input": flags,
            "expect": {"verdict": verdict.verdict.value,
                       "reason": verdict.reason},
        })
    return out


def _resource_vectors() -> list[dict]:
    """资源类型 → 版本语义；路径 → 资源类型（参考实现）。"""
    out: list[dict] = []
    for kind in sorted(RESOURCE_KINDS, key=lambda k: k.value):
        spec = ResourceSpec(kind=kind, path="x")
        out.append({
            "id": f"resources.semantics.{kind.value}",
            "kind": "resources.semantics",
            "description": f"{kind.value} 的版本语义",
            "invariants": ["I2"],
            "input": {"kind": kind.value},
            "expect": {"semantics": spec.version_semantics()},
        })
    for path, expect in (("a.py", "FILE"), ("a.ts", "FILE"), ("a.json", "FILE"),
                         ("a.sql", "DB_ROW"), ("README", "FILE")):
        out.append({
            "id": f"resources.path.{path}",
            "kind": "resources.path",
            "description": f"{path} 推断的资源类型",
            "invariants": ["I2"],
            "input": {"path": path},
            "expect": {"kind": expect},
        })
    return out


def _fencing_vectors() -> list[dict]:
    """I3：真实 `LeaseState` 的确定性 trace（epoch 单调 + 旧 epoch 被拒）。"""
    def vec(vid, desc, ops, epochs, outcomes, authorized):
        return {"id": vid, "kind": "fencing.trace", "description": desc,
                "invariants": ["I3"], "input": {"ops": ops},
                "expect": {"epochs": epochs, "outcomes": outcomes,
                           "authorized": authorized}}

    return [
        vec("fencing.takeover",
            "advance 后旧 epoch 的 acquire 被 fence；旧 epoch 不再授权",
            [{"op": "acquire", "owner": "A", "instance": "i1",
              "now_ms": 0, "ttl_ms": 10},
             {"op": "advance", "owner": "A", "instance": "i1", "epoch": 1,
              "now_ms": 10, "ttl_ms": 10},
             {"op": "acquire", "owner": "B", "instance": "i2",
              "now_ms": 10, "ttl_ms": 10, "expected_epoch": 1},
             {"op": "authorizes", "owner": "A", "instance": "i1",
              "epoch": 1, "now_ms": 10}],
            [1, 2, 2], ["acquired", "advanced", "fenced"], [False]),
        vec("fencing.expire",
            "租约过期后接管 epoch+1；旧持有者不再授权",
            [{"op": "acquire", "owner": "A", "instance": "i1",
              "now_ms": 0, "ttl_ms": 10},
             {"op": "acquire", "owner": "B", "instance": "i2",
              "now_ms": 20, "ttl_ms": 10},
             {"op": "authorizes", "owner": "A", "instance": "i1",
              "epoch": 1, "now_ms": 20}],
            [1, 2], ["acquired", "acquired"], [False]),
        vec("fencing.renew",
            "同持有者续租 epoch 不变，仍授权",
            [{"op": "acquire", "owner": "A", "instance": "i1",
              "now_ms": 0, "ttl_ms": 10},
             {"op": "renew", "owner": "A", "instance": "i1", "epoch": 1,
              "now_ms": 5, "ttl_ms": 10},
             {"op": "authorizes", "owner": "A", "instance": "i1",
              "epoch": 1, "now_ms": 10}],
            [1, 1], ["acquired", "renewed"], [True]),
        vec("fencing.release",
            "主动释放后 epoch 保留；新持有者 epoch+1，旧 epoch 失效",
            [{"op": "acquire", "owner": "A", "instance": "i1",
              "now_ms": 0, "ttl_ms": 10},
             {"op": "release", "owner": "A", "instance": "i1", "epoch": 1},
             {"op": "acquire", "owner": "B", "instance": "i2",
              "now_ms": 1, "ttl_ms": 10},
             {"op": "authorizes", "owner": "A", "instance": "i1",
              "epoch": 1, "now_ms": 1}],
            [1, 1, 2], ["acquired", "released", "acquired"], [False]),
        vec("fencing.stale_acquire",
            "expected_epoch 落后 → fenced，epoch 不变；当前持有者仍授权",
            [{"op": "acquire", "owner": "A", "instance": "i1",
              "now_ms": 0, "ttl_ms": 10},
             {"op": "acquire", "owner": "B", "instance": "i2",
              "now_ms": 0, "ttl_ms": 10, "expected_epoch": 999},
             {"op": "authorizes", "owner": "A", "instance": "i1",
              "epoch": 1, "now_ms": 0}],
            [1, 1], ["acquired", "fenced"], [True]),
        vec("fencing.monotonic",
            "连续 advance 三次：epoch 严格 +1",
            [{"op": "acquire", "owner": "A", "instance": "i1",
              "now_ms": 0, "ttl_ms": 10},
             {"op": "advance", "owner": "A", "instance": "i1", "epoch": 1,
              "now_ms": 1, "ttl_ms": 10},
             {"op": "advance", "owner": "A", "instance": "i1", "epoch": 2,
              "now_ms": 2, "ttl_ms": 10},
             {"op": "advance", "owner": "A", "instance": "i1", "epoch": 3,
              "now_ms": 3, "ttl_ms": 10}],
            [1, 2, 3, 4], ["acquired", "advanced", "advanced", "advanced"], []),
    ]


def _plan_vectors() -> list[dict]:
    """I9：逻辑依赖图的环检测（生产用 `_has_cycle`）。"""
    cases = (
        ("plan.dag", [["a", "b"], ["a", "c"], ["b", "d"], ["c", "d"]], True),
        ("plan.two_cycle", [["a", "b"], ["b", "a"]], False),
        ("plan.self_loop", [["a", "a"]], False),
        ("plan.unknown_endpoint", [["a", "zz"]], True),
        ("plan.duplicate_edge", [["a", "b"], ["a", "b"]], True),
        ("plan.diamond", [["a", "b"], ["a", "c"], ["b", "d"], ["c", "d"]], True),
    )
    return [{"id": vid, "kind": "plan.acyclic", "description": f"环检测：{vid}",
             "invariants": ["I9"], "input": {"edges": edges},
             "expect": {"acyclic": acyclic}}
            for vid, edges, acyclic in cases]


def _build_kwargs() -> list[dict]:
    """4 个收据构建用例（构造参数，不含 secret）。"""
    return [
        {"receipt_id": "r-1", "run_id": "run-1", "task_id": "t-1",
         "logical_subgoal_id": "g-1", "resource": "app/a.py",
         "version_before": 0, "version_after": 1},
        {"receipt_id": "r-2", "run_id": "run-1", "task_id": "t-1",
         "logical_subgoal_id": "g-1", "resource": "app/b.py",
         "version_before": 1, "version_after": 2,
         "prev_receipt_hash": "deadbeefdeadbeef"},
        {"receipt_id": "r-3", "run_id": "run-1", "task_id": "t-2",
         "logical_subgoal_id": "g-2", "resource": "effect:pay",
         "version_before": 0, "version_after": 1,
         "effect_id": "e-1", "idem_key": "k-1"},
        {"receipt_id": "r-4", "run_id": "run-2", "task_id": "t-3",
         "logical_subgoal_id": "g-3", "resource": "app/c.py",
         "version_before": 7, "version_after": 8,
         "compensation_ref": "cmp-1"},
    ]


def _receipt_build_vectors() -> list[dict]:
    out: list[dict] = []
    for i, kwargs in enumerate(_build_kwargs()):
        signed = i != 0                       # 第 1 条不签名（secret=""）
        secret = _FIXTURE_SECRET if signed else ""
        r = build_receipt(**kwargs, secret=secret)
        out.append({
            "id": f"receipt.build.{i}",
            "kind": "receipt.build",
            "description": ("已签名的收据：payload 与 HMAC 全量 64 hex"
                            if signed else "未签名：signature 为空串"),
            "invariants": ["I13"],
            "input": dict(kwargs),
            "secret": secret,
            "expect": {
                "payload_sha256": hashlib.sha256(
                    r.payload().encode("utf-8")).hexdigest(),
                "signature": r.signature,
                "signed": r.signed,
            },
        })
    return out


def _receipt_verify_vectors() -> list[dict]:
    """链校验三态（True / None / False）的用例。"""
    def _chain(n: int, *, sign: bool = True) -> list[Receipt]:
        out: list[Receipt] = []
        prev = "0" * 16
        for i in range(n):
            r = build_receipt(receipt_id=f"v-{i}", run_id="run", task_id="t",
                              logical_subgoal_id="g", resource="r",
                              version_before=i, version_after=i + 1,
                              prev_receipt_hash=prev,
                              secret=_FIXTURE_SECRET if sign else "")
            prev = hashlib.sha256(r.payload().encode("utf-8")).hexdigest()[:16]
            out.append(r)
        return out

    signed1 = _chain(1)
    signed2 = _chain(2)
    unsigned = _chain(1, sign=False)
    tampered = _chain(1)
    tampered[0].signature = "0" * 64
    broken = _chain(2)
    broken[1].prev_receipt_hash = "f" * 16
    dup = _chain(2)
    dup[1].receipt_id = dup[0].receipt_id

    cases = [
        ("empty", [], _FIXTURE_SECRET, False),
        ("single_signed", signed1, _FIXTURE_SECRET, True),
        ("chain_signed", signed2, _FIXTURE_SECRET, True),
        ("unsigned", unsigned, _FIXTURE_SECRET, None),
        ("chain_only_no_secret", signed2, "", None),
        ("tampered_signature", tampered, _FIXTURE_SECRET, False),
        ("broken_prev_hash", broken, _FIXTURE_SECRET, False),
        ("duplicate_receipt_id", dup, _FIXTURE_SECRET, False),
    ]
    out: list[dict] = []
    for name, receipts, secret, expect in cases:
        payloads = []
        for r in receipts:
            item = r.to_dict()
            item.pop("signed", None)          # 派生字段，不参与语料
            payloads.append(item)
        out.append({
            "id": f"receipt.verify.{name}",
            "kind": "receipt.verify",
            "description": f"链校验三态：{name}",
            "invariants": ["I13"],
            "input": {"receipts": payloads, "secret": secret},
            "expect": {"result": expect},
        })
    return out


# ---------------------------------------------------------------- 组装
def build_corpus() -> dict[str, bytes]:
    """返回 `{相对路径: 规范 JSON 字节}`（含 manifest，`manifest` 最后算）。"""
    files: dict[str, bytes] = {
        "vectors/protocol.json": _canon({"vectors": _protocol_vectors()}),
        "vectors/resources.json": _canon({"vectors": _resource_vectors()}),
        "vectors/receipt-build.json": _canon(
            {"vectors": _receipt_build_vectors()}),
        "vectors/receipt-verify.json": _canon(
            {"vectors": _receipt_verify_vectors()}),
        "vectors/fencing.json": _canon({"vectors": _fencing_vectors()}),
        "vectors/plan.json": _canon({"vectors": _plan_vectors()}),
        "invariants.json": _canon({"invariants": [
            {"id": i.id, "name": i.name, "description": i.description,
             "test": i.test} for i in INVARIANTS]}),
    }
    vector_files = [p for p in files if p.startswith("vectors/") and p.endswith(".json")]
    total = 0
    entries = []
    executable: set[str] = set()
    for rel in vector_files:
        doc = json.loads(files[rel].decode("utf-8"))
        vectors = doc.get("vectors", [])
        total += len(vectors)
        for vec in vectors:
            executable.update(vec.get("invariants") or [])
        entries.append({"path": rel, "sha256": _sha256_file(files[rel]),
                        "vectors": len(vectors)})
    entries.sort(key=lambda e: e["path"])
    files["manifest.json"] = _canon({
        "schema_version": SCHEMA_VERSION,
        "corpus_version": CORPUS_VERSION,
        "spec_version": SPEC_VERSION,
        "files": entries,
        "totals": {
            "files": len(entries),
            "vectors": total,
            "invariants": len(INVARIANTS),
            "executable_invariants": sorted(executable),
        },
    })
    return files


# ---------------------------------------------------------------- 执行
def _run_vector(vec: dict) -> None:
    """执行一条向量；不符即抛 `AssertionError`（带 id）。"""
    kind = vec["kind"]
    data, expect = vec["input"], vec["expect"]
    if kind == "protocol.decide":
        got = decide(**data)
        if got.verdict.value != expect["verdict"] or got.reason != expect["reason"]:
            raise AssertionError(f"verdict={got.verdict.value} reason={got.reason!r}")
        return
    if kind == "resources.semantics":
        spec = ResourceSpec(kind=ResourceKind(data["kind"]), path="x")
        if spec.version_semantics() != expect["semantics"]:
            raise AssertionError(f"semantics={spec.version_semantics()!r}")
        return
    if kind == "resources.path":
        if resource_kind(data["path"]).value != expect["kind"]:
            raise AssertionError(f"kind={resource_kind(data['path']).value!r}")
        return
    if kind == "receipt.build":
        r = build_receipt(**data, secret=vec.get("secret", ""))
        payload_sha = hashlib.sha256(r.payload().encode("utf-8")).hexdigest()
        if (payload_sha != expect["payload_sha256"]
                or r.signature != expect["signature"]
                or r.signed is not expect["signed"]):
            raise AssertionError(
                f"payload_sha={payload_sha} signature={r.signature!r} "
                f"signed={r.signed}")
        return
    if kind == "receipt.verify":
        receipts = [Receipt.from_dict(d) for d in data["receipts"]]
        got = verify_chain_detailed(receipts, data.get("secret", ""))
        if got is not expect["result"]:
            raise AssertionError(f"result={got!r}")
        return
    if kind == "fencing.trace":
        # 延迟导入：避免 `corpus` 在导入期把 orchestration/storage 全链拉起
        from ..storage.lease import LeaseState
        state = LeaseState()
        epochs: list[int] = []
        outcomes: list[str] = []
        authorized: list[bool] = []
        for op in data.get("ops") or []:
            name = op.get("op")
            kwargs = {k: v for k, v in op.items() if k != "op"}
            if name == "authorizes":
                authorized.append(state.authorizes(**kwargs))
            elif name in ("acquire", "renew", "release", "advance"):
                grant = getattr(state, name)(**kwargs)
                epochs.append(int(grant.epoch))
                outcomes.append(str(grant.outcome))
            else:
                raise AssertionError(f"未知 fencing op：{name}")
        if (epochs != list(expect["epochs"])
                or outcomes != list(expect["outcomes"])
                or authorized != list(expect["authorized"])):
            raise AssertionError(
                f"epochs={epochs} outcomes={outcomes} authorized={authorized}")
        return
    if kind == "plan.acyclic":
        from ..orchestration import _has_cycle
        got = _has_cycle(data.get("edges") or [])
        if (not got) is not expect["acyclic"]:
            raise AssertionError(f"acyclic={not got}")
        return
    raise AssertionError(f"未知 kind：{kind}")


def run_corpus(corpus: Mapping[str, bytes]) -> dict:
    """执行全部向量 + 校验 manifest 的 sha/计数；返回 `{ok, passed, failed, errors}`。"""
    errors: list[dict] = []
    try:
        manifest = json.loads(corpus["manifest.json"].decode("utf-8"))
    except KeyError:
        return {"ok": False, "passed": 0, "failed": 1,
                "errors": [{"id": "manifest", "error": "缺少 manifest.json"}]}
    except json.JSONDecodeError as exc:
        return {"ok": False, "passed": 0, "failed": 1,
                "errors": [{"id": "manifest", "error": f"manifest 非法 JSON: {exc}"}]}

    passed = failed = 0
    seen: set[str] = set()
    vector_ids: list[str] = []
    executable: set[str] = set()
    total = 0
    for entry in manifest.get("files", []):
        rel = entry.get("path", "")
        raw = corpus.get(rel)
        if raw is None:
            errors.append({"id": rel, "error": "manifest 引用的文件不存在"})
            failed += 1
            continue
        if _sha256_file(raw) != entry.get("sha256"):
            errors.append({"id": rel, "error": "sha256 与 manifest 不一致"})
            failed += 1
        try:
            doc = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            errors.append({"id": rel, "error": f"非法 JSON: {exc}"})
            failed += 1
            continue
        vectors = doc.get("vectors", [])
        if len(vectors) != entry.get("vectors"):
            errors.append({"id": rel, "error": "向量数与 manifest 不一致"})
            failed += 1
        total += len(vectors)
        for vec in vectors:
            vid = vec.get("id", "")
            if not vid or vid in seen:
                errors.append({"id": vid or rel, "error": "id 缺失或重复"})
                failed += 1
                continue
            seen.add(vid)
            vector_ids.append(vid)
            executable.update(vec.get("invariants") or [])
            try:
                _run_vector(vec)
                passed += 1
            except AssertionError as exc:
                errors.append({"id": vid, "error": str(exc)})
                failed += 1
    totals = manifest.get("totals", {})
    if total != totals.get("vectors"):
        errors.append({"id": "manifest", "error": "totals.vectors 不一致"})
        failed += 1
    if sorted(executable) != totals.get("executable_invariants"):
        errors.append({"id": "manifest", "error": "totals.executable_invariants 不一致"})
        failed += 1
    known = {i.id for i in INVARIANTS}
    if totals.get("invariants") != len(known):
        errors.append({"id": "manifest", "error": "totals.invariants 不一致"})
        failed += 1
    return {"ok": not errors, "passed": passed, "failed": failed,
            "errors": errors, "executable_invariants": sorted(executable),
            "vectors": vector_ids}


# ---------------------------------------------------------------- 磁盘
def check_corpus(root: pathlib.Path | None = None) -> dict:
    """磁盘语料 vs 本实现：返回 `{ok, drift, run}`（`drift` 是差异文件清单）。"""
    root = pathlib.Path(root or CORPUS_ROOT)
    expected = build_corpus()
    drift: list[str] = []
    for rel, data in expected.items():
        path = root / rel
        if not path.is_file() or path.read_bytes() != data:
            drift.append(rel)
    # 磁盘上多出来的文件也算漂移（避免孤儿向量悄悄存在）
    for path in root.rglob("*.json") if root.is_dir() else []:
        rel = str(path.relative_to(root)).replace("\\", "/")
        if rel not in expected:
            drift.append(rel)
    disk = {rel: (root / rel).read_bytes()
            for rel in expected if (root / rel).is_file()}
    run = run_corpus(disk) if disk else {"ok": False, "passed": 0, "failed": 1,
                                         "errors": [{"id": "corpus", "error": "语料目录为空"}]}
    return {"ok": not drift and run["ok"], "drift": sorted(drift), "run": run}


def write_corpus(root: pathlib.Path | None = None) -> None:
    """显式把语料写盘（开发用；生产门禁只用 `check_corpus`）。"""
    root = pathlib.Path(root or CORPUS_ROOT)
    for rel, data in build_corpus().items():
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)


def main(argv: list[str] | None = None) -> int:
    """`python3 -m harness.spec.corpus [--check|--write] [--json] [--root DIR]`。

    退出码：0 通过；1 语料漂移/向量失败；2 用法错误。
    """
    import argparse

    ap = argparse.ArgumentParser(
        prog="rsc-corpus", description="RSC 合规语料自检 / 导出")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--check", action="store_true",
                      help="校验磁盘语料与本实现一致（默认）")
    mode.add_argument("--write", action="store_true", help="重新生成语料")
    ap.add_argument("--root", default=None, help="语料根目录（默认 harness/spec/conformance）")
    ap.add_argument("--json", action="store_true", help="输出 JSON 报告")
    args = ap.parse_args(argv)

    root = pathlib.Path(args.root) if args.root else CORPUS_ROOT
    if args.write:
        write_corpus(root)
        if args.json:
            print(json.dumps({"ok": True, "written": str(root)}, ensure_ascii=False))
        else:
            print(f"已写入语料：{root}")
        return 0

    report = check_corpus(root)
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(f"corpus ok={report['ok']} passed={report['run']['passed']} "
              f"failed={report['run']['failed']}")
        for rel in report["drift"]:
            print("  漂移:", rel)
        for err in report["run"]["errors"]:
            print("  失败:", err["id"], err["error"])
    return 0 if report["ok"] else 1


if __name__ == "__main__":                    # pragma: no cover
    sys.exit(main())
