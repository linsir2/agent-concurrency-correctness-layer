"""S24 差距清单第 5 项：TLA+ 模型（I1/I2/I4/I7）的资产与 TLC 实测。

- 静态用例（资产存在、模块名/不变量名/开关名正确）**始终执行**；
- TLC 用例需要 Java + tla2tools.jar：未设置 `TLA2TOOLS_JAR` 时 `pytest.skip`；
  设置了 `TLA2TOOLS_JAR` 却缺 Java 时 `pytest.fail`（配置错误不得静默通过）。
"""

from __future__ import annotations

import importlib.util
import os
import pathlib
import shutil
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent
TLA_DIR = ROOT / "spec" / "tla"
SCRIPT = ROOT / "scripts" / "check_tla.py"

# 编号 -> (模块名, 被验证的不变量名, broken cfg 改动的开关名)
CASES: dict[str, tuple[str, str, str]] = {
    "I1": ("I1SingleWriter", "AtMostOneWriter", "EnforceMutex"),
    "I2": ("I2NoLostUpdate", "NoLostUpdate", "EnforceCAS"),
    "I4": ("I4Capability", "CapabilityClosed", "EnforceCaps"),
    "I7": ("I7EffectExactlyOnce", "EffectAtMostOnce", "Dedup"),
    "I3": ("I3Fencing", "I3FencingMonotonic", "EnforceFence"),
}


def _lines(text: str) -> list[str]:
    return [ln.strip() for ln in text.splitlines() if ln.strip()]


@pytest.fixture(scope="module")
def tla_checker():
    """加载 scripts/check_tla.py（它不是包，按文件路径导入）。"""
    spec = importlib.util.spec_from_file_location("s24_check_tla", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _require_tlc() -> tuple[str, str]:
    """返回 (java, jar)；缺 jar 时 skip，配置错误时 fail。"""
    jar = os.environ.get("TLA2TOOLS_JAR")
    if not jar:
        pytest.skip("未设置 TLA2TOOLS_JAR（TLC 实测需要 tla2tools.jar）")
    jar_path = pathlib.Path(jar)
    if not jar_path.is_file():
        pytest.fail(f"TLA2TOOLS_JAR 指向的文件不存在: {jar}")
    java = shutil.which("java")
    if java is None:
        pytest.fail("已设置 TLA2TOOLS_JAR 但找不到 java（Java 运行时缺失）")
    return java, str(jar_path)


@pytest.mark.parametrize("key", sorted(CASES))
def test_assets_exist_and_names(key: str) -> None:
    """4 个资产（.tla / .cfg / .broken.cfg）存在，模块名与不变量名正确。"""
    module, invariant, switch = CASES[key]
    tla_path = TLA_DIR / f"{module}.tla"
    cfg_path = TLA_DIR / f"{module}.cfg"
    broken_path = TLA_DIR / f"{module}.broken.cfg"
    for path in (tla_path, cfg_path, broken_path):
        assert path.is_file(), f"缺少资产文件: {path}"

    tla = tla_path.read_text(encoding="utf-8")
    assert f"---- MODULE {module} ----" in tla, f"{module}.tla 模块名不匹配"
    assert "TypeOK ==" in tla, f"{module}.tla 缺少 TypeOK"
    assert f"{invariant} ==" in tla, f"{module}.tla 缺少不变量 {invariant}"

    correct = cfg_path.read_text(encoding="utf-8")
    broken = broken_path.read_text(encoding="utf-8")
    assert f"INVARIANT {invariant}" in correct
    assert "INVARIANT TypeOK" in correct
    assert f"{switch} = TRUE" in correct
    assert f"{switch} = FALSE" in broken
    # broken cfg 只改一个开关，其余行完全一致
    only_correct = [ln for ln in _lines(correct) if ln not in _lines(broken)]
    only_broken = [ln for ln in _lines(broken) if ln not in _lines(correct)]
    assert only_correct == [f"{switch} = TRUE"], only_correct
    assert only_broken == [f"{switch} = FALSE"], only_broken


def test_readme_documents_all_models() -> None:
    """README 必须说明每个模型：模块名、不变量名、broken 反例。"""
    readme = (TLA_DIR / "README.md").read_text(encoding="utf-8")
    for module, invariant, _switch in CASES.values():
        assert module in readme, f"README 缺少模块 {module}"
        assert invariant in readme, f"README 缺少不变量 {invariant}"
    assert "broken" in readme
    assert "TLA2TOOLS_JAR" in readme
    assert "EventuallyCommitted" in readme and "SF_vars" in readme


@pytest.mark.parametrize("key", sorted(CASES))
def test_correct_model_passes(key: str, tla_checker) -> None:
    """正确 cfg：TLC exit 0（I1/I2/I4/I7 在模型上成立）。"""
    java, jar = _require_tlc()
    module, _invariant, _switch = CASES[key]
    rc, out = tla_checker.run_tlc(
        java, jar, TLA_DIR / f"{module}.tla", TLA_DIR / f"{module}.cfg"
    )
    assert rc == 0, f"{module}.cfg 期望 exit 0，实际 {rc}\n{tla_checker.tail(out)}"


@pytest.mark.parametrize("key", sorted(CASES))
def test_broken_model_finds_counterexample(key: str, tla_checker) -> None:
    """broken cfg：TLC exit 非 0，且输出明确报告不变量被违反。"""
    java, jar = _require_tlc()
    module, invariant, _switch = CASES[key]
    rc, out = tla_checker.run_tlc(
        java, jar, TLA_DIR / f"{module}.tla", TLA_DIR / f"{module}.broken.cfg"
    )
    assert rc != 0, f"{module}.broken.cfg 期望失败，实际 exit 0\n{tla_checker.tail(out)}"
    assert f"Invariant {invariant} is violated" in out, (
        f"{module}.broken.cfg 未报告 {invariant} 违反\n{tla_checker.tail(out)}"
    )


def test_liveness_assets_exist() -> None:
    for name in ("I1SingleWriter.liveness.cfg", "I1SingleWriter.unfair.cfg"):
        assert (TLA_DIR / name).is_file()
    tla = (TLA_DIR / "I1SingleWriter.tla").read_text(encoding="utf-8")
    assert "FairSpec ==" in tla and "SF_vars(Commit(w))" in tla
    assert "EventuallyCommitted ==" in tla


def test_liveness_fair_model_passes(tla_checker) -> None:
    java, jar = _require_tlc()
    rc, out = tla_checker.run_tlc(
        java, jar, TLA_DIR / "I1SingleWriter.tla",
        TLA_DIR / "I1SingleWriter.liveness.cfg")
    assert rc == 0, tla_checker.tail(out)


def test_liveness_unfair_model_finds_counterexample(tla_checker) -> None:
    java, jar = _require_tlc()
    rc, out = tla_checker.run_tlc(
        java, jar, TLA_DIR / "I1SingleWriter.tla",
        TLA_DIR / "I1SingleWriter.unfair.cfg")
    assert rc != 0
    assert "Temporal properties were violated" in out
    assert "Invariant" not in out
