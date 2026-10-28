------------------------------ MODULE I2NoLostUpdate ------------------------------
(* I2「无丢失更新」：对应 design §5.4 `ws.apply` 的 CAS 校验
 *   base_version[resource] == resource.version 才允许提交。
 *
 * `Read` 记录 base_version（acb.read_set）；`Commit` 只有在 base_version
 * 仍等于当前版本时才生效。每次成功提交消耗一个不同的 base 版本。
 *
 * broken 模型（EnforceCAS = FALSE）：去掉 CAS 守卫，两个写者都基于版本 0
 * 提交时，后提交者覆盖前者 → 丢失更新（`commits > |bases|`）。
 *)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS Writers, Resources, MaxVer, EnforceCAS

ASSUME Writers # {}
ASSUME Resources # {}

VARIABLES ver, base, target, status, bases, commits

vars == <<ver, base, target, status, bases, commits>>

TypeOK ==
    /\ ver \in [Resources -> 0..MaxVer]
    /\ base \in [Writers \X Resources -> 0..MaxVer]
    /\ target \in [Writers -> Resources]
    /\ status \in [Writers -> {"idle", "ready", "committed"}]
    /\ bases \in [Resources -> SUBSET (0..MaxVer)]
    /\ commits \in [Resources -> 0..Cardinality(Writers)]

Init ==
    /\ ver = [r \in Resources |-> 0]
    /\ base = [w \in Writers, r \in Resources |-> 0]
    /\ target = [w \in Writers |-> CHOOSE r \in Resources : TRUE]
    /\ status = [w \in Writers |-> "idle"]
    /\ bases = [r \in Resources |-> {}]
    /\ commits = [r \in Resources |-> 0]

(* 读最新版本并记录 base_version（对应 acb.read_set）。 *)
Read(w) ==
    \E r \in Resources :
        /\ status[w] = "idle"
        /\ status' = [status EXCEPT ![w] = "ready"]
        /\ target' = [target EXCEPT ![w] = r]
        /\ base' = [base EXCEPT ![w, r] = ver[r]]
        /\ UNCHANGED <<ver, bases, commits>>

(* 提交：CAS —— base_version 必须等于当前版本。 *)
Commit(w) ==
    LET r == target[w] IN
        /\ status[w] = "ready"
        /\ (EnforceCAS => base[w, r] = ver[r])
        /\ ver[r] < MaxVer
        /\ ver' = [ver EXCEPT ![r] = ver[r] + 1]
        /\ bases' = [bases EXCEPT ![r] = bases[r] \cup {base[w, r]}]
        /\ commits' = [commits EXCEPT ![r] = commits[r] + 1]
        /\ status' = [status EXCEPT ![w] = "committed"]
        /\ UNCHANGED <<base, target>>

(* 冲突后 rebase：重新读取最新版本再试（对应 handle_commit_result）。 *)
Rebase(w) ==
    /\ status[w] = "ready"
    /\ status' = [status EXCEPT ![w] = "idle"]
    /\ UNCHANGED <<ver, base, target, bases, commits>>

Next == \E w \in Writers : Read(w) \/ Commit(w) \/ Rebase(w)

Spec == Init /\ [][Next]_vars

(* I2：成功提交次数 == 已消耗 base 版本的个数。
 * 若两个提交共用同一个 base 版本，后者就是「丢失更新」。 *)
NoLostUpdate ==
    \A r \in Resources :
        commits[r] = Cardinality(bases[r])

================================================================================
