------------------------------ MODULE I3Fencing ------------------------------
(* I3「fencing 单调」：租约/epoch 只增不减，旧 epoch 的提交必须被拒。
 *
 * 抽象：`Grant` 记录写者拿到的 epoch；`Fence` 由监督者推进 epoch；
 * `Commit` 模拟「提交点复核」——`EnforceFence=TRUE` 时旧 epoch 被拒
 * （staleCommits 不增长），`FALSE` 时旧 epoch 被接受（staleCommits 增长）。
 *)

EXTENDS Naturals, TLC

CONSTANTS Writers, MaxEpoch, EnforceFence

ASSUME Writers # {}

VARIABLES epoch, prevEpoch, leaseEpoch, staleCommits

vars == <<epoch, prevEpoch, leaseEpoch, staleCommits>>

TypeOK ==
    /\ epoch \in 0..MaxEpoch
    /\ prevEpoch \in 0..MaxEpoch
    /\ leaseEpoch \in [Writers -> 0..MaxEpoch]
    /\ staleCommits \in 0..(MaxEpoch * 2)

Init ==
    /\ epoch = 0
    /\ prevEpoch = 0
    /\ leaseEpoch = [w \in Writers |-> 0]
    /\ staleCommits = 0

Grant(w) ==
    /\ leaseEpoch' = [leaseEpoch EXCEPT ![w] = epoch]
    /\ UNCHANGED <<epoch, prevEpoch, staleCommits>>

Fence ==
    /\ epoch < MaxEpoch
    /\ prevEpoch' = epoch
    /\ epoch' = epoch + 1
    /\ UNCHANGED <<leaseEpoch, staleCommits>>

Commit(w) ==
    /\ prevEpoch' = epoch
    (* EnforceFence=TRUE：旧 epoch 被拒，不计数；FALSE：旧 epoch 被接受，计数 *)
    /\ staleCommits' = staleCommits +
         (IF ~EnforceFence /\ leaseEpoch[w] # epoch THEN 1 ELSE 0)
    /\ UNCHANGED <<epoch, leaseEpoch>>

Next == \E w \in Writers : Grant(w) \/ Commit(w) \/ Fence

Spec == Init /\ [][Next]_vars

I3FencingMonotonic ==
    /\ epoch >= prevEpoch
    /\ staleCommits = 0

=============================================================================
