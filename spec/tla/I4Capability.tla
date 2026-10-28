------------------------------ MODULE I4Capability ------------------------------
(* I4「能力封闭」：只能访问 capability 授权的资源（design §18.4）。
 *
 * 抽象：每个写者持有能力集合 `Held(w)`；资源要求 `NeededCaps`。
 * 正确模型（EnforceCaps = TRUE）里，`Access` 仅在
 * `NeededCaps \subseteq Held(w)` 时启用（fail-closed）；
 * broken 模型（EnforceCaps = FALSE）去掉该守卫 = 关闭能力检查，
 * 低权写者也能产生访问记录 → 越权访问被记账。
 *
 * 说明：TLC 的 .cfg 不支持元组常量，故能力表用集合常量表达：
 *   Writers / Resources 为模型值集合，`WriteHolders` 是持有 write 能力的写者集合。
 *)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS Writers, Resources, WriteHolders, EnforceCaps

ASSUME Writers # {}
ASSUME Resources # {}

Caps == {"read", "write"}

(* 每个写者至少持有 read；WriteHolders 中的写者额外持有 write。 *)
Held(w) == {"read"} \cup (IF w \in WriteHolders THEN {"write"} ELSE {})

(* 本模型中被访问的资源都要求 write 能力。 *)
NeededCaps == {"write"}

Authorized(w, r) == NeededCaps \subseteq Held(w)

VARIABLES accessed

TypeOK == accessed \in SUBSET (Writers \X Resources)

Init == accessed = {}

(* 一次资源访问（工具执行边界）：正确模型要求 capability 授权。 *)
Access(w, r) ==
    /\ (EnforceCaps => Authorized(w, r))
    /\ accessed' = accessed \cup {<<w, r>>}

Next == \E w \in Writers : \E r \in Resources : Access(w, r)

Spec == Init /\ [][Next]_accessed

AuthorizedPairs == {p \in Writers \X Resources : NeededCaps \subseteq Held(p[1])}

(* I4：所有被访问的 (写者, 资源) 都在 capability 授权表内。 *)
CapabilityClosed == accessed \subseteq AuthorizedPairs

================================================================================
