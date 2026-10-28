"""S24-P0：文档真值守卫（`scripts/check_docs_consistency.py`）。

RIPR：
- Reachability：脚本真的读到 milestones/positioning/README/manifest；
- Infection：逐条还原已知腐化（MS9 未开工 / MS5 缺 VerifierRunner /
  矩阵状态翻转 / L4 塞 Trace / 向量数过期）都必须产生 finding；
- Propagation：`main()` 退出码 0/1；
- Revealability：finding 带文件名与原因。
"""

from __future__ import annotations

import importlib.util
import pathlib
import shutil

ROOT = pathlib.Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts" / "check_docs_consistency.py"
_FILES = (
    "docs/milestones.md",
    "docs/positioning-and-roadmap.md",
    "docs/s24-gap-analysis-and-repositioning.md",
    "README.md",
    "harness/spec/conformance/manifest.json",
)


def _module():
    spec = importlib.util.spec_from_file_location("s24_docs_check", SCRIPT)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _copy_tree(tmp_path: pathlib.Path) -> pathlib.Path:
    for rel in _FILES:
        dst = tmp_path / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(ROOT / rel, dst)
    return tmp_path


def test_head_is_consistent():
    checker = _module()
    assert checker.check_docs(ROOT) == []


def test_ms9_not_started_is_detected(tmp_path):
    root = _copy_tree(tmp_path)
    path = root / "docs/milestones.md"
    lines = path.read_text(encoding="utf-8").splitlines()
    hit = [i for i, ln in enumerate(lines) if ln.startswith("| MS9 |")]
    assert hit
    lines[hit[-1]] = lines[hit[-1]].replace("✅ 完成（S16）", "⏳ 未开工")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    findings = _module().check_docs(root)
    assert any("MS9" in f["message"] for f in findings)


def test_ms5_missing_verifier_is_detected(tmp_path):
    root = _copy_tree(tmp_path)
    path = root / "docs/milestones.md"
    text = path.read_text(encoding="utf-8")
    path.write_text(text.replace("**VerifierRunner**", "**X**", 1),
                    encoding="utf-8")
    findings = _module().check_docs(root)
    assert any("VerifierRunner" in f["message"] for f in findings)


def test_matrix_status_drift_is_detected(tmp_path):
    root = _copy_tree(tmp_path)
    path = root / "docs/positioning-and-roadmap.md"
    text = path.read_text(encoding="utf-8")
    old = "| **我们** | **✅** | **✅** | **✅** | **✅** | **🟡** | **✅** |"
    new = "| **我们** | **✅** | **✅** | **✅** | **✅** | **✅** | **🟡** |"
    assert old in text
    path.write_text(text.replace(old, new, 1), encoding="utf-8")
    findings = _module().check_docs(root)
    assert any("语义验证" in f["message"] for f in findings)
    assert any("worktree" in f["message"] for f in findings)


def test_trace_in_l4_is_detected(tmp_path):
    root = _copy_tree(tmp_path)
    path = root / "docs/positioning-and-roadmap.md"
    text = path.read_text(encoding="utf-8")
    path.write_text(text.replace("BudgetManager/PolicyEngine（Trace 不交付",
                                 "BudgetManager/PolicyEngine/Trace（已交付",
                                 1), encoding="utf-8")
    findings = _module().check_docs(root)
    assert any("Trace" in f["message"] for f in findings)


def test_vector_count_drift_is_detected(tmp_path):
    root = _copy_tree(tmp_path)
    path = root / "README.md"
    text = path.read_text(encoding="utf-8")
    path.write_text(text.replace("35 条", "34 条", 1), encoding="utf-8")
    findings = _module().check_docs(root)
    assert any("语料条数" in f["message"] for f in findings)


def test_cli_exit_codes(tmp_path, capsys):
    checker = _module()
    assert checker.main(["--root", str(ROOT)]) == 0
    root = _copy_tree(tmp_path)
    (root / "README.md").write_text("no counts here\n", encoding="utf-8")
    assert checker.main(["--root", str(root)]) == 1
    capsys.readouterr()
