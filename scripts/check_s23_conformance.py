#!/usr/bin/env python3
"""S23 方案 ↔ 实现 一致性核对（可执行）。

用法：`python3 scripts/check_s23_conformance.py`（exit 0 = 全部一致）。
对应 `docs/s23-integration-plan.md` §1/§3/§6/§9。

分两类：
1. **契约/必杀变异**：关键防护被删除时这里必须失败（不是只查文件存在）；
2. **运行期最小场景**：门面新增的归属/声明写校验、sidecar 上限与协议版本。
"""

from __future__ import annotations

import inspect
import pathlib
import sys
import tempfile
import tomllib

_ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

ok: list[str] = []
bad: list[tuple[str, str]] = []


def chk(name: str, cond: bool, detail: str = "") -> None:
    (ok if cond else bad).append((name, detail))


def _src(rel: str) -> str:
    return (_ROOT / rel).read_text(encoding="utf-8")


from harness import Harness, RscSession
from harness.facade import Deny, Token
from harness.integrations import http_sidecar

# ---------------------------------------------------- 交付物
for rel in ("harness/integrations/_common.py",
            "harness/integrations/crewai.py",
            "harness/integrations/openai_agents.py",
            "harness/integrations/ag2.py",
            "harness/integrations/http_sidecar.py",
            "tests/test_s23_crewai.py",
            "tests/test_s23_openai_agents.py",
            "tests/test_s23_ag2.py",
            "tests/test_s23_sidecar.py",
            "tests/test_s23_example.py",
            "examples/three_agents/run.py",
            "examples/three_agents/README.md"):
    chk(f"交付存在 {rel}", (_ROOT / rel).exists())

_py = tomllib.loads(_src("pyproject.toml"))
_extras = _py["project"]["optional-dependencies"]
for extra in ("crewai", "openai-agents", "ag2", "integrations"):
    chk(f"extras.{extra} 已声明", extra in _extras)
chk("extras 不含空 sidecar", "sidecar" not in _extras)

# ---------------------------------------------------- 门面契约（评审修复）
chk("RscSession.owner_of 存在", callable(getattr(RscSession, "owner_of", None)))
chk("RscSession.declared_writes 存在",
    callable(getattr(RscSession, "declared_writes", None)))
_begin = inspect.getsource(RscSession.begin)
chk("begin 记录声明写集", '"rsc_writes"' in _begin)
_apply = inspect.getsource(RscSession._apply_locked)
chk("apply 拒绝未声明资源", "undeclared_write" in _apply)

# ---------------------------------------------------- sidecar 契约
chk("sidecar 体限 1 MiB", http_sidecar.MAX_BODY == 1 << 20)
chk("sidecar backlog 在 bind 前设置",
    http_sidecar._SidecarServer.request_queue_size == 64)
_side = _src("harness/integrations/http_sidecar.py")
chk("sidecar 连接读超时", "timeout = 10" in _side)
chk("sidecar token 归属校验", "_owned(" in _side)
chk("sidecar 不接受 agent_id 请求参数", 'data.get("agent_id")' not in _side)
chk("sidecar 每 agent 一把密钥", "keys: dict[str, str]" in _side)
chk("sidecar 未声明写映射 400",
    http_sidecar._deny_status(Deny("undeclared_write:x")) == 400)
chk("sidecar stale_view 映射 409",
    http_sidecar._deny_status(Deny("stale", code="stale_view")) == 409)

# ---------------------------------------------------- CrewAI 契约
_crew = _src("harness/integrations/crewai.py")
chk("CrewAI 覆写 run", "def run(self, *args" in _crew)
chk("CrewAI 禁用缓存", "cache_function=_never_cache" in _crew)
chk("CrewAI 幂等键剔除 root", "key_kwargs.pop(self.root_arg" in _crew)
chk("CrewAI 缺 root fail-closed", "tool_must_accept_root" in _crew)
chk("CrewAI 未提交即 abort", "session.abort(tok.token" in _crew)
for n in ("test_root_is_excluded_from_idempotency_key",
          "test_missing_root_is_fail_closed",
          "test_budget_exhausted_is_soft_wait",
          "test_stale_write_maps_to_rsc_conflict",
          "test_same_args_retry_returns_same_receipt"):
    chk(f"CrewAI 用例 {n}", f"def {n}(" in _src("tests/test_s23_crewai.py"))

# ---------------------------------------------------- OpenAI 契约
_oai = _src("harness/integrations/openai_agents.py")
chk("OpenAI 参数 JSON 规范化", "_parsed_args" in _oai)
chk("OpenAI 工具异常自撤 token", "_abandon(call_id)" in _oai)
chk("OpenAI schema 去 root", "_strip_param(" in _oai)
chk("OpenAI 硬拒用 raise_exception", '"raise_exception"' in _oai)
chk("OpenAI 注入 input/output guardrail",
    "tool_input_guardrails=[" in _oai and "tool_output_guardrails=[" in _oai)
for n in ("test_arguments_are_canonicalized_for_idempotency",
          "test_tool_exception_aborts_token",
          "test_missing_tool_call_id_trips",
          "test_happy_path_commits_and_strips_root"):
    chk(f"OpenAI 用例 {n}", f"def {n}(" in _src("tests/test_s23_openai_agents.py"))

# ---------------------------------------------------- AG2 契约
_ag2 = _src("harness/integrations/ag2.py")
chk("AG2 中间件入口", "async def on_tool_execution" in _ag2)
chk("AG2 参数注入", "serialized_arguments" in _ag2)
chk("AG2 短路返回事件", "ToolResultEvent.from_call" in _ag2)
chk("AG2 幂等键剔除 root", "key_args.pop(self.root_arg" in _ag2)
for n in ("test_root_is_excluded_from_idempotency_key",
          "test_stale_write_returns_conflict_event",
          "test_budget_exhausted_short_circuits",
          "test_undeclared_tool_passthrough"):
    chk(f"AG2 用例 {n}", f"def {n}(" in _src("tests/test_s23_ag2.py"))

# ---------------------------------------------------- 测试有效性与文档
for rel in ("tests/test_s23_crewai.py", "tests/test_s23_openai_agents.py",
            "tests/test_s23_ag2.py"):
    chk(f"{rel} 用 importorskip", "pytest.importorskip" in _src(rel))
_side_test = _src("tests/test_s23_sidecar.py")
for code in ("401", "403", "409", "411", "413", "501"):
    chk(f"sidecar 用例覆盖 {code}", code in _side_test or f"== {code}" in _side_test)
chk("sidecar 用例覆盖归属", "test_token_owner_cannot_be_taken_over" in _side_test)
chk("sidecar 用例覆盖未声明写", "test_apply_rejects_undeclared_resource" in _side_test)
_doc = _src("docs/s23-integration-plan.md")
chk("文档有评审记录 §9", "## 9. 对抗式评审与修复" in _doc)
chk("文档记录 P0 归属校验", "P0" in _doc and "token 归属" in _doc)
chk("文档写明每 agent 一把密钥", "每个 agent 一把密钥" in _doc)
chk("文档写明 CrewAI 无 scoped_hooks", "1.15.32" in _doc and "scoped_hooks" in _doc)
chk("README 链到 S23 文档", "s23-integration-plan.md" in _src("README.md"))
chk("示例声明 lost update", "lost update" in _src("examples/three_agents/run.py"))

# ---------------------------------------------------- 运行期最小场景
with tempfile.TemporaryDirectory() as tmp:
    h = Harness(run_limits=10, egress_required=False,
                checkpoint_root=str(pathlib.Path(tmp) / "ckpt"))
    session = RscSession(h, tenant="t", min_budget=1,
                         state_dir=str(pathlib.Path(tmp) / "state"))
    tok = session.begin("a1", "i1", writes=["a.md"])
    chk("运行期 begin 返回 Token", isinstance(tok, Token))
    if isinstance(tok, Token):
        chk("运行期 owner_of 正确", session.owner_of(tok.token) == "a1")
        chk("运行期 declared_writes 正确",
            session.declared_writes(tok.token) == ("a.md",))
        chk("运行期 apply 拒绝未声明资源",
            isinstance(session.apply(tok.token, [("b.md", b"x")],
                                     idempotency_key="k"), Deny))
    chk("运行期 owner_of 未知 token → None",
        session.owner_of("does-not-exist") is None)

    try:
        http_sidecar.build_sidecar(session, keys={})
        chk("sidecar 空密钥拒绝构造", False)
    except ValueError:
        chk("sidecar 空密钥拒绝构造", True)

    srv = http_sidecar.build_sidecar(session, keys={"K": "a1"})
    chk("sidecar HTTP/1.1", srv.RequestHandlerClass.protocol_version == "HTTP/1.1")
    srv.server_close()

print(f"通过 {len(ok)} / 失败 {len(bad)}")
for name, detail in bad:
    print("  ❌", name, detail)
sys.exit(1 if bad else 0)
