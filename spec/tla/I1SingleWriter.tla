------------------------------ MODULE I1SingleWriter ------------------------------
(* I1「单写者提交」（design §18.4 / §5.4 apply 的短临界区）。
 *
 * 抽象：写者先做非阻塞预留（TCC intent reserve，允许多人同时预留同一资源），
 * 再进入提交层短临界区；`Commit` 一步完成「版本递增 + 释放资源锁」，
 * 对应 `atomic_replace` 与 `version += 1` 在同一临界区内原子发生。
 *
 * 正确模型（EnforceMutex = TRUE）：进临界区要求资源锁空闲 —— 同一资源
 * 同一时刻只有一个写者在临界区。
 * broken 模型（EnforceMutex = FALSE）：去掉锁守卫（等价于「校验在临界区外」
 * 或「提交层无互斥」），两个写者可同时进入同一资源的临界区。
 *)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS Writers, Resources, MaxVer, EnforceMutex

ASSUME Writers # {}
ASSUME Resources # {}

NoHolder == "NONE"

VARIABLES pc, target, holder, ver

vars == <<pc, target, holder, ver>>

TypeOK ==
    /\ pc \in [Writers -> {"outside", "reserved", "crit", "committed"}]
    /\ target \in [Writers -> Resources]
    /\ holder \in [Resources -> Writers \cup {NoHolder}]
    /\ ver \in [Resources -> 0..MaxVer]

Init ==
    /\ pc = [w \in Writers |-> "outside"]
    /\ target = [w \in Writers |-> CHOOSE r \in Resources : TRUE]
    /\ holder = [r \in Resources |-> NoHolder]
    /\ ver = [r \in Resources |-> 0]

(* 非阻塞预留：同一资源的多个意图可以并存（GRANT / DIE 语义）。 *)
Reserve(w) ==
    \E r \in Resources :
        /\ pc[w] = "outside"
        /\ pc' = [pc EXCEPT ![w] = "reserved"]
        /\ target' = [target EXCEPT ![w] = r]
        /\ UNCHANGED <<holder, ver>>

(* 进入提交层短临界区：正确模型要求资源锁空闲。 *)
EnterCrit(w) ==
    /\ pc[w] = "reserved"
    /\ (EnforceMutex => holder[target[w]] = NoHolder)
    /\ pc' = [pc EXCEPT ![w] = "crit"]
    /\ holder' = [holder EXCEPT ![target[w]] = w]
    /\ UNCHANGED <<target, ver>>

(* 临界区内原子提交：版本递增与释放锁在同一步。 *)
Commit(w) ==
    /\ pc[w] = "crit"
    /\ ver[target[w]] < MaxVer
    /\ pc' = [pc EXCEPT ![w] = "committed"]
    /\ ver' = [ver EXCEPT ![target[w]] = ver[target[w]] + 1]
    /\ holder' = [holder EXCEPT ![target[w]] = NoHolder]
    /\ UNCHANGED target

(* 放弃本次提交（CONFLICT / REJECT_STALE 后重新预留）。 *)
Abort(w) ==
    /\ pc[w] = "crit"
    /\ pc' = [pc EXCEPT ![w] = "outside"]
    /\ holder' = [holder EXCEPT ![target[w]] = NoHolder]
    /\ UNCHANGED <<target, ver>>

Next == \E w \in Writers : Reserve(w) \/ EnterCrit(w) \/ Commit(w) \/ Abort(w)

Spec == Init /\ [][Next]_vars

(* I1：任一资源同一时刻至多一个写者处于提交临界区。 *)
AtMostOneWriter ==
    \A r \in Resources :
        Cardinality({w \in Writers : pc[w] = "crit" /\ target[w] = r}) <= 1

================================================================================
