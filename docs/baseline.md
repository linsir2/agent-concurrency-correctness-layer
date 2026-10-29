# 重构基线（S0 冻结）

| 项 | 值 |
|---|---|
| 分支 | `refactor/kernel-split`（基于 `999b8c3`） |
| 日期 | 2026-10-29 |
| Python | 3.14.x |
| 依赖 | `dependencies = []`（未安装 `pysyncobj` / `sandlock` / `mcp` / `runsc`） |

## 测试基线（重构前）

```
python3 -m pytest tests/ -q
956 collected
871 passed / 22 skipped / 67 xfailed / 1 failed
```

唯一失败：`tests/test_s22_mcp.py::test_c9_build_mcp_server_registers_tools`
→ `ImportError: 需要 MCP SDK`（缺可选依赖却硬失败，S6 修）。

67 个 xfail 全部来自缺失的可选能力：`pysyncobj`（Raft）、`sandlock`（Landlock）、
非 root（netns）、`runsc`（gVisor）。**按设计显式 xfail，不是假绿。**

## 行为基线（重构后必须逐字一致）

```
python3 examples/quickstart/run.py
[S1] semantic_conflict / verify_failed / verified=True
[S2] {"cpu": "500m", "replicas": 3, "timeout": "30s"}，version=3
[S3] version=1，dedup=True，receipt 相同
[S4] Wait(BUDGET_EXHAUSTED)，version=1
```

## 文档基线

```
python3 scripts/check_docs_consistency.py   → docs ok: 0 findings
python3 -m harness.spec.corpus --check      → 47 / 0
```

> `README.md:94-95` 的 `924 passed / 23 skipped / 4 xfailed` 与本次实测不符，
> 属于 S6 要移除的易变数字。
