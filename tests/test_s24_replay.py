"""S24-9：真实 token 基准的离线回放（`bench/llm_replay.py`）。

RIPR：
- Reachability：录制 fixture 被加载、每个 turn 的 usage 都参与汇总；
- Infection：删掉重试 turn → `rework_rate` 必须变 0（必杀变异）；
- Propagation：`total_tokens`/`token_multiplier` 由 turn 精确重算，不读缓存数字；
- Revealability：`provenance=live`、`claim_scope`、无 key 时 `skipped` 且**无数字**。
"""

from __future__ import annotations

import copy
import json

import pytest

import bench.llm_replay as B


def _doc() -> dict:
    return copy.deepcopy(B.load_fixture())


# ---------------------------------------------------------------- fixture
def test_fixture_is_live_and_paired():
    doc = B.load_fixture()
    assert doc["provenance"] == "live"
    assert doc["sample_size"] == 1
    assert doc["claim_scope"].startswith("paired_")
    assert doc["arms"]["naive"]["outcome"]["success"] is False
    assert doc["arms"]["naive"]["outcome"]["lost_update"] is True
    assert doc["arms"]["rsc"]["outcome"]["success"] is True
    assert doc["arms"]["rsc"]["outcome"]["first_conflict"] is True
    # 两条件共享同一次 A/B 调用
    assert set(doc["arms"]["naive"]["turn_ids"]) <= set(doc["arms"]["rsc"]["turn_ids"])


def test_turn_arithmetic_is_consistent():
    doc = B.load_fixture()
    for tid, turn in doc["turns"].items():
        assert turn["usage_missing"] is False
        assert turn["total_tokens"] == (turn["prompt_tokens"]
                                        + turn["completion_tokens"]), tid


# ---------------------------------------------------------------- 指标
def test_metrics_are_recomputed_from_turns():
    doc = _doc()
    got = B.compute_metrics(doc)
    naive = got["conditions"]["naive"]
    rsc = got["conditions"]["rsc"]
    assert naive["total_tokens"] == sum(
        doc["turns"][t]["total_tokens"] for t in doc["arms"]["naive"]["turn_ids"])
    assert rsc["total_tokens"] == sum(
        doc["turns"][t]["total_tokens"] for t in doc["arms"]["rsc"]["turn_ids"])
    assert naive["retry_turns"] == 0
    assert rsc["retry_turns"] == 1
    assert rsc["rework_rate"] == pytest.approx(1 / 3, abs=1e-4)
    assert got["token_multiplier"] > 1.0
    assert got["token_multiplier"] == round(
        rsc["total_tokens"] / naive["total_tokens"], 4)


def test_killer_mutation_removing_retry_changes_metrics():
    doc = _doc()
    before = B.compute_metrics(doc)
    doc["arms"]["rsc"]["turn_ids"] = ["a", "b"]      # 假装没有返工
    after = B.compute_metrics(doc)
    assert before["conditions"]["rsc"]["rework_rate"] > 0
    assert after["conditions"]["rsc"]["rework_rate"] == 0
    assert after["conditions"]["rsc"]["total_tokens"] < \
        before["conditions"]["rsc"]["total_tokens"]
    assert after["token_multiplier"] != before["token_multiplier"]


def test_missing_usage_is_rejected():
    doc = _doc()
    doc["turns"]["a"]["usage_missing"] = True
    with pytest.raises(ValueError):
        B.validate_fixture(doc)


def test_total_mismatch_is_rejected():
    doc = _doc()
    doc["turns"]["a"]["total_tokens"] += 1
    with pytest.raises(ValueError):
        B.validate_fixture(doc)


def test_dangling_turn_id_is_rejected():
    doc = _doc()
    doc["arms"]["naive"]["turn_ids"].append("nope")
    with pytest.raises(ValueError):
        B.validate_fixture(doc)


# ---------------------------------------------------------------- CLI
def test_cli_replay_json(capsys):
    assert B.main(["--fixture", str(B.FIXTURE), "--json"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["provenance"] == "live"
    assert out["token_multiplier"] > 1


def test_cli_writes_markdown_report(tmp_path, capsys):
    path = tmp_path / "report.md"
    assert B.main(["--fixture", str(B.FIXTURE), "--report", str(path)]) == 0
    text = path.read_text(encoding="utf-8")
    assert "token 倍数" in text and "rework_rate" in text
    capsys.readouterr()


def test_live_without_key_skips_without_numbers(monkeypatch, capsys, tmp_path):
    def _raise(cls, env=None):
        raise B.MissingAPIKey("no key")

    monkeypatch.setattr(B.LLMConfig, "from_env", classmethod(_raise))
    out_path = tmp_path / "fixture.json"
    assert B.main(["--live", "--json", "--out", str(out_path)]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "skipped"
    assert "token_multiplier" not in payload
    assert not out_path.exists()                    # 不落任何「数字」


def test_require_live_exits_three_without_key(monkeypatch, capsys):
    def _raise(cls, env=None):
        raise B.MissingAPIKey("no key")

    monkeypatch.setattr(B.LLMConfig, "from_env", classmethod(_raise))
    assert B.main(["--live", "--require-live"]) == 3
    capsys.readouterr()
