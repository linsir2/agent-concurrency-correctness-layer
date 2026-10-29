"""S24-5：合规语料（conformance corpus）自检。

RIPR：
- Reachability：磁盘语料存在且与 `build_corpus()` 逐字节一致（无漂移）；
- Infection：改动任何一条向量的期望值 / 删文件 / 加孤儿文件 → 必须被发现；
- Propagation：`main(["--check"])` 的退出码 0/1/2 与 JSON 报告可用；
- Revealability：manifest 的 sha256 / 计数 / 可执行不变量汇总与向量一致。

语料是「对外契约」，因此这里断言的是**冻结性**，不是实现内部结构。
"""

from __future__ import annotations

import hashlib
import json
import pathlib
import re

import pytest

from harness.spec import corpus as C
from harness.spec.invariants import INVARIANT_IDS, INVARIANTS
from harness.spec.receipt import build_receipt


def _dict() -> dict[str, bytes]:
    return C.build_corpus()


def _mutate_expect(corpus: dict[str, bytes], rel: str, idx: int,
                   key: str, value) -> dict[str, bytes]:
    """改一条向量的 `expect[key]`，并同步 manifest 的 sha（只测语义漂移）。"""
    out = dict(corpus)
    doc = json.loads(out[rel].decode("utf-8"))
    doc["vectors"][idx]["expect"][key] = value
    out[rel] = (json.dumps(doc, sort_keys=True, indent=2,
                           ensure_ascii=False) + "\n").encode("utf-8")
    manifest = json.loads(out["manifest.json"].decode("utf-8"))
    for entry in manifest["files"]:
        if entry["path"] == rel:
            entry["sha256"] = hashlib.sha256(out[rel]).hexdigest()
    out["manifest.json"] = (json.dumps(manifest, sort_keys=True, indent=2,
                                      ensure_ascii=False) + "\n").encode("utf-8")
    return out


# ---------------------------------------------------------------- 基线
def test_disk_corpus_matches_reference():
    report = C.check_corpus()
    assert report["ok"], report
    assert report["drift"] == []
    assert report["run"]["failed"] == 0
    assert report["run"]["passed"] > 0


def test_all_vectors_execute():
    result = C.run_corpus(_dict())
    assert result["ok"], result["errors"]
    assert result["failed"] == 0


def test_manifest_counts_and_invariants():
    corpus = _dict()
    manifest = json.loads(corpus["manifest.json"].decode("utf-8"))
    assert manifest["schema_version"] == C.SCHEMA_VERSION
    assert manifest["corpus_version"] == C.CORPUS_VERSION
    assert manifest["totals"]["invariants"] == len(INVARIANT_IDS)
    assert manifest["totals"]["vectors"] >= 30
    # 14 条不变量全部登记，且 id 唯一
    inv = json.loads(corpus["invariants.json"].decode("utf-8"))["invariants"]
    assert [i["id"] for i in inv] == list(INVARIANT_IDS)
    assert len({i["id"] for i in inv}) == 14
    # 可执行不变量 = 向量里真实出现的集合（不许声称没覆盖的）
    seen: set[str] = set()
    for rel in manifest["files"]:
        if not rel["path"].startswith("vectors/"):
            continue
        for vec in json.loads(corpus[rel["path"]].decode("utf-8"))["vectors"]:
            seen.update(vec.get("invariants") or ())
    assert manifest["totals"]["executable_invariants"] == sorted(seen)
    assert "I13" in seen


def test_invariant_test_pointers_exist():
    """`invariants.json` 里的参考测试名必须真实存在（防止文档腐化）。"""
    tests_dir = pathlib.Path(__file__).resolve().parent
    names: set[str] = set()
    for path in tests_dir.glob("test_*.py"):
        names.update(re.findall(r"^def (test_\w+)",
                                path.read_text(encoding="utf-8"), re.MULTILINE))
    missing = [(i.id, i.test) for i in INVARIANTS if i.test not in names]
    assert not missing, missing


def test_frozen_signature_is_real_hmac():
    """冻结签名必须等于当前 `build_receipt` 的输出（不是随手写的串）。"""
    corpus = _dict()
    doc = json.loads(corpus["vectors/receipt-build.json"].decode("utf-8"))
    signed = [v for v in doc["vectors"] if v["expect"]["signed"]]
    assert signed, "语料里至少要有 1 条已签名收据"
    for vec in signed:
        r = build_receipt(**vec["input"], secret=vec["secret"])
        assert r.signature == vec["expect"]["signature"]
        assert len(r.signature) == 64
        assert hashlib.sha256(r.payload().encode("utf-8")).hexdigest() \
            == vec["expect"]["payload_sha256"]


# ---------------------------------------------------------------- I3 / I9
def test_fencing_vectors_execute_and_are_frozen():
    corpus = _dict()
    doc = json.loads(corpus["vectors/fencing.json"].decode("utf-8"))
    ids = [v["id"] for v in doc["vectors"]]
    assert len(ids) == 6 and "fencing.takeover" in ids
    assert C.run_corpus(corpus)["ok"]
    mutated = _mutate_expect(corpus, "vectors/fencing.json", 0,
                             "outcomes", ["acquired", "advanced", "fenced", "x"])
    assert not C.run_corpus(mutated)["ok"]


def test_plan_vectors_execute_and_are_frozen():
    corpus = _dict()
    doc = json.loads(corpus["vectors/plan.json"].decode("utf-8"))
    ids = [v["id"] for v in doc["vectors"]]
    assert len(ids) == 6 and "plan.self_loop" in ids
    assert C.run_corpus(corpus)["ok"]
    mutated = _mutate_expect(corpus, "vectors/plan.json", 2, "acyclic", True)
    assert not C.run_corpus(mutated)["ok"]


def test_corpus_version_and_executable_invariants():
    manifest = json.loads(_dict()["manifest.json"].decode("utf-8"))
    assert manifest["corpus_version"] == "1.1.0"
    assert {"I3", "I9"} <= set(manifest["totals"]["executable_invariants"])
    assert manifest["totals"]["vectors"] == 47


# ---------------------------------------------------------------- 漂移
def test_changed_expect_is_detected():
    corpus = _mutate_expect(_dict(), "vectors/protocol.json", 0,
                            "verdict", "DIE")
    result = C.run_corpus(corpus)
    assert not result["ok"]
    assert any(e["id"].startswith("protocol.decide") for e in result["errors"])


def test_tampered_file_breaks_manifest_sha():
    corpus = dict(_dict())
    corpus["vectors/protocol.json"] += b" "
    result = C.run_corpus(corpus)
    assert not result["ok"]
    assert any("sha256" in e["error"] for e in result["errors"])


def test_unknown_vector_kind_fails_closed():
    corpus = _dict()
    rel = "vectors/protocol.json"
    doc = json.loads(corpus[rel].decode("utf-8"))
    doc["vectors"][0]["kind"] = "protocol.unknown"
    corpus[rel] = (json.dumps(doc, sort_keys=True, indent=2,
                              ensure_ascii=False) + "\n").encode("utf-8")
    manifest = json.loads(corpus["manifest.json"].decode("utf-8"))
    for entry in manifest["files"]:
        if entry["path"] == rel:
            entry["sha256"] = hashlib.sha256(corpus[rel]).hexdigest()
    corpus["manifest.json"] = (json.dumps(manifest, sort_keys=True, indent=2,
                                          ensure_ascii=False) + "\n").encode("utf-8")
    result = C.run_corpus(corpus)
    assert not result["ok"]
    assert any("未知 kind" in e["error"] for e in result["errors"])


def test_missing_and_orphan_files_are_drift(tmp_path):
    C.write_corpus(tmp_path)
    assert C.check_corpus(tmp_path)["ok"]
    (tmp_path / "vectors" / "protocol.json").unlink()
    assert "vectors/protocol.json" in C.check_corpus(tmp_path)["drift"]
    C.write_corpus(tmp_path)
    (tmp_path / "vectors" / "bonus.json").write_text('{"vectors": []}',
                                                     encoding="utf-8")
    assert "vectors/bonus.json" in C.check_corpus(tmp_path)["drift"]


# ---------------------------------------------------------------- CLI
def test_main_check_exit_zero_and_json(capsys):
    assert C.main(["--check", "--json"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["ok"] is True
    assert out["run"]["failed"] == 0


def test_main_check_exit_one_on_drift(tmp_path, capsys):
    C.write_corpus(tmp_path)
    path = tmp_path / "invariants.json"
    path.write_text("{}\n", encoding="utf-8")
    assert C.main(["--check", "--root", str(tmp_path)]) == 1
    assert "漂移" in capsys.readouterr().out


def test_main_write_then_check(tmp_path, capsys):
    assert C.main(["--write", "--root", str(tmp_path)]) == 0
    assert C.main(["--check", "--root", str(tmp_path)]) == 0
    assert (tmp_path / "manifest.json").is_file()
    capsys.readouterr()


def test_main_rejects_unknown_args(capsys):
    with pytest.raises(SystemExit) as exc:
        C.main(["--nope"])
    assert exc.value.code == 2
    capsys.readouterr()
