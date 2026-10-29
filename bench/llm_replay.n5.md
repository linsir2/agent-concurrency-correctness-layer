# LLM token 基准（config_conflict）

- provenance: **live**（model=qwen3.7-flash，n=5，valid=5）
- claim_scope: `paired_token_cost_of_workflow_turns`

| 条件 | turns | total tokens | 重试 turns | rework_rate | success |
|---|---|---|---|---|---|
| naive | 2.0 | 112.0 | 0.0 | 0.0 | 0.0 |
| rsc | 3.0 | 173.0 | 1.0 | 0.3333 | 1.0 |

- **token 倍数**（rsc ÷ naive）：**1.5446**
- rework_rate 增量：0.3333
- token 倍数中位数 1.5446（min 1.5446 / max 1.5446 / 极差 0.0）
- rework_rate 增量中位数 0.3333（极差 0.0）
- 被剔除样本：0

> 描述性统计（n>1），不作显著性结论；token 来自上游 `usage`，`reasoning_tokens` 已含在 completion 内。
