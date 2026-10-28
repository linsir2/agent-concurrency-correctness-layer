------------------------------ MODULE I7EffectExactlyOnce ------------------------------
(* I7「effect 有效一次」：只建模**账本状态机**，不建模外部世界
 * （design §18.4 的 LIMBO 教训：无 in-flight 上界时纯验证无法保证 exactly-once）。
 *
 * 语义：投递（Deliver）是 at-least-once，允许重复；账本按键 `idem_key`
 * 去重（Dedup = TRUE 时只有 ledger[k] = "none" 才允许 Apply）。
 * broken 模型（Dedup = FALSE）去掉去重守卫，同一 idem_key 可被生效多次。
 *)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS Writers, Keys, MaxApplies, Dedup

ASSUME Writers # {}
ASSUME Keys # {}

VARIABLES pending, ledger, applyCount

vars == <<pending, ledger, applyCount>>

TypeOK ==
    /\ pending \in [Writers -> Keys \cup {"none"}]
    /\ ledger \in [Keys -> {"none", "applied"}]
    /\ applyCount \in [Keys -> 0..MaxApplies]

Init ==
    /\ pending = [w \in Writers |-> "none"]
    /\ ledger = [k \in Keys |-> "none"]
    /\ applyCount = [k \in Keys |-> 0]

(* 投递 effect 请求（可重复投递，携带 idem_key）。 *)
Deliver(w, k) ==
    /\ pending' = [pending EXCEPT ![w] = k]
    /\ UNCHANGED <<ledger, applyCount>>

(* 账本去重后生效：正确模型要求该 idem_key 尚未生效。 *)
Apply(w) ==
    LET k == pending[w] IN
        /\ k # "none"
        /\ (Dedup => ledger[k] = "none")
        /\ applyCount[k] < MaxApplies
        /\ ledger' = [ledger EXCEPT ![k] = "applied"]
        /\ applyCount' = [applyCount EXCEPT ![k] = applyCount[k] + 1]
        /\ pending' = [pending EXCEPT ![w] = "none"]

Next == (\E w \in Writers : \E k \in Keys : Deliver(w, k))
        \/ (\E w \in Writers : Apply(w))

Spec == Init /\ [][Next]_vars

(* I7：任意 idem_key 在账本上最多生效一次。 *)
EffectAtMostOnce ==
    \A k \in Keys : applyCount[k] <= 1

================================================================================
