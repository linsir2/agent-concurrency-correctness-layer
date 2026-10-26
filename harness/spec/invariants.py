"""conformance suite 的不变量语料（文档 §18.4）。

每条不变量：编号 / 名称 / 描述 / 覆盖它的测试名。
**规范与实现分离**：本表是「契约」，测试是实现。
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Invariant:
    id: str
    name: str
    description: str
    test: str


INVARIANTS: tuple[Invariant, ...] = (
    Invariant("I1", "单写者提交", "同一资源同一时刻只有一个写生效（提交层 CAS）",
              "test_i1_single_writer_commit"),
    Invariant("I2", "无丢失更新", "覆盖写被拒并 rebase",
              "test_i2_no_lost_update"),
    Invariant("I3", "fencing 单调", "revoke 后旧租约被拒",
              "test_i3_fencing_monotonic"),
    Invariant("I4", "能力封闭", "越权访问被拒",
              "test_capability_denied_blocks_tool"),
    Invariant("I5", "预算守恒", "consumed ≤ reserved ≤ limits；父约束不被子突破",
              "test_i5_budget_conservation"),
    Invariant("I6", "上下文因果一致", "materialized + ordered events 是一致前缀",
              "test_i6_context_consistent_prefix"),
    Invariant("I7", "effect 有效一次", "重复投递去重（幂等键）",
              "test_i7_effect_exactly_once"),
    Invariant("I8", "事件原子", "资源提交与 WorkspaceEvent 同事务",
              "test_i8_event_atomic_with_replace"),
    Invariant("I9", "计划有效", "Plan 无环、footprint 完整",
              "test_i9_plan_valid"),
    Invariant("I10", "隔离", "Z3 私有 worktree 对共享态不可见",
              "test_i10_isolation"),
    Invariant("I11", "有界", "cursor 不落后于保留窗口",
              "test_a7_event_storm_cursor_snapshot_replay"),
    Invariant("I12", "终止/公平", "每个 task 在期限内到达终态",
              "test_a11_finalize_not_triggered_with_nonterminal"),
    Invariant("I13", "可审计", "收据可离线验证",
              "test_i13_receipt_chain"),
    Invariant("I14", "无进展终止", "NO_PROGRESS 检测后 FAILED",
              "test_a13_per_action_guard_budget"),
)

INVARIANT_IDS = tuple(i.id for i in INVARIANTS)
