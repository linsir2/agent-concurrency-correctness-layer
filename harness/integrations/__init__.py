"""S22 集成适配器（宿主框架接入面）。

本包**不**在 `harness/__init__.py` 中导入 —— 适配器依赖可选包
（`langgraph` / `mcp`），只有显式 `import harness.integrations.*` 时才加载。
"""
