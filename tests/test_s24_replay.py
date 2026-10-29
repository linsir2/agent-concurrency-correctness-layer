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
import pathlib

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


# ---------------------------------------------------------------- v2 / n=5
def _v2(n: int = 5) -> dict:
    base = B.load_fixture()
    samples = []
    for i in range(n):
        factor = i + 1
        turns = copy.deepcopy(base["turns"])
        for turn in turns.values():
            for key in ("prompt_tokens", "completion_tokens"):
                turn[key] = int(turn[key]) * factor
            turn["total_tokens"] = turn["prompt_tokens"] + turn["completion_tokens"]
        samples.append({"sample_id": f"s{i}", "turns": turns,
                        "arms": copy.deepcopy(base["arms"])})
    return {"schema_version": 2, "scenario": "x", "provenance": "live",
            "model": "m", "sample_size": n, "claim_scope": "c",
            "request": {"temperature": 0.0, "max_tokens": 64,
                        "enable_thinking": False},
            "samples": samples}


def test_n5_fixture_is_live_and_thinking_off():
    path = pathlib.Path(B.__file__).parent / "fixtures" / "llm_replay.n5.json"
    doc = B.load_fixture(path)
    assert doc["schema_version"] == 2 and len(doc["samples"]) == 5
    assert doc["request"]["enable_thinking"] is False
    got = B.compute_metrics(doc)
    assert got["n_valid"] == 5 and got["n_rejected"] == 0
    for sample in doc["samples"]:
        for turn in sample["turns"].values():
            assert turn["reasoning_tokens"] == 0
            assert turn["finish_reason"] != "length"


def test_v1_is_backward_compatible():
    got = B.compute_metrics(B.load_fixture())
    assert "n_valid" not in got and "token_multiplier_stats" not in got
    assert got["token_multiplier"] == pytest.approx(1.3909, abs=1e-3)


def test_v2_reports_median_and_range():
    got = B.compute_metrics(_v2(5))
    assert got["n_valid"] == 5 and got["n_rejected"] == 0
    assert got["token_multiplier"] == pytest.approx(1.3909, abs=1e-3)
    assert got["token_multiplier_stats"]["range"] == 0
    assert got["rework_rate_delta_stats"]["median"] == pytest.approx(1 / 3, abs=1e-3)
    text = B.write_report(got)
    assert "描述性统计" in text and "极差" in text


def test_v2_rejects_length_and_usage_missing_samples():
    doc = _v2(5)
    doc["samples"][1]["turns"]["a"]["finish_reason"] = "length"
    doc["samples"][2]["turns"]["b"]["usage_missing"] = True
    got = B.compute_metrics(doc)
    assert got["n_valid"] == 3 and got["n_rejected"] == 2
    reasons = {r["sample_id"]: r["reason"] for r in got["rejected"]}
    assert "截断" in reasons["s1"] and "usage" in reasons["s2"]


def test_v2_all_invalid_is_rejected():
    doc = _v2(2)
    for sample in doc["samples"]:
        sample["turns"]["a"]["finish_reason"] = "length"
    with pytest.raises(ValueError):
        B.compute_metrics(doc)


def test_v2_n1_has_no_range_keys():
    got = B.compute_metrics(_v2(1))
    assert got["sample_size"] == 1
    assert "token_multiplier_stats" not in got


def test_cli_passes_n_and_thinking(monkeypatch, capsys):
    seen = {}
    doc = _v2(1)

    def fake_capture(**kw):
        seen.update(kw)
        return doc

    monkeypatch.setattr(B, "capture_live", fake_capture)
    assert B.main(["--live", "--n", "1", "--enable-thinking",
                   "--max-tokens", "8", "--json"]) == 0
    assert seen == {"via_proxy": False, "n": 1,
                    "enable_thinking": True, "max_tokens": 8}
    out = json.loads(capsys.readouterr().out)
    assert "token_multiplier_stats" not in out


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
