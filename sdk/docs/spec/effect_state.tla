---- MODULE effect_state ----
EXTENDS Naturals

\* One-row model of the durable intent protocol.
CONSTANT Owners

VARIABLES effect_state, fence, owner, decision_allowed, committed_count

States == {"INTENDED", "ATTEMPTING", "COMMITTED", "ABORTED", "UNKNOWN"}

Init ==
  /\ effect_state = "INTENDED"
  /\ fence = 0
  /\ owner = "none"
  /\ decision_allowed = FALSE
  /\ committed_count = 0

\* ActionLedger.claim_side_effecting()
\* CAS preconditions: expected_owner/expected_fence match prior row on reclaim.
Claim(o) ==
  /\ o \in Owners
  /\ effect_state \in {"INTENDED", "ABORTED", "UNKNOWN"}
  /\ fence' = fence + 1
  /\ owner' = o
  /\ effect_state' = "INTENDED"
  /\ decision_allowed' = FALSE
  /\ committed_count' = committed_count

\* ActionLedger.record_decision(allowed=TRUE|FALSE)
\* CAS preconditions: expected_owner = owner, expected_fence = fence,
\* expected_effect_state = INTENDED.
RecordDecisionAllow(o, f) ==
  /\ o = owner
  /\ f = fence
  /\ effect_state = "INTENDED"
  /\ effect_state' = "ATTEMPTING"
  /\ decision_allowed' = TRUE
  /\ UNCHANGED <<fence, owner, committed_count>>

RecordDecisionDeny(o, f) ==
  /\ o = owner
  /\ f = fence
  /\ effect_state = "INTENDED"
  /\ effect_state' = "ABORTED"
  /\ decision_allowed' = FALSE
  /\ UNCHANGED <<fence, owner, committed_count>>

\* ActionLedger.complete()
\* CAS preconditions: expected_owner = owner, expected_fence = fence,
\* expected_effect_state = ATTEMPTING.
Complete(o, f) ==
  /\ o = owner
  /\ f = fence
  /\ effect_state = "ATTEMPTING"
  /\ decision_allowed = TRUE
  /\ effect_state' = "COMMITTED"
  /\ committed_count' = committed_count + 1
  /\ UNCHANGED <<fence, owner, decision_allowed>>

\* ActionLedger.fail(... failed_after_effect=False)
\* CAS preconditions: expected_owner = owner, expected_fence = fence.
FailBeforeEffect(o, f) ==
  /\ o = owner
  /\ f = fence
  /\ effect_state \in {"INTENDED", "ATTEMPTING"}
  /\ effect_state' = "ABORTED"
  /\ UNCHANGED <<fence, owner, decision_allowed, committed_count>>

\* ActionLedger.fail(... failed_after_effect=True) / mark_unknown()
\* CAS preconditions: expected_owner = owner, expected_fence = fence.
FailAfterEffect(o, f) ==
  /\ o = owner
  /\ f = fence
  /\ effect_state = "ATTEMPTING"
  /\ effect_state' = "UNKNOWN"
  /\ UNCHANGED <<fence, owner, decision_allowed, committed_count>>

MarkUnknown(o, f) ==
  /\ o = owner
  /\ f = fence
  /\ effect_state \in {"INTENDED", "ATTEMPTING"}
  /\ effect_state' = "UNKNOWN"
  /\ UNCHANGED <<fence, owner, decision_allowed, committed_count>>

\* Stale-fence write rejected by CAS (no state change).
\* Models rejected complete/fail/record_decision from superseded owner/fence.
StaleFenceWrite(staleOwner, staleFence) ==
  /\ staleOwner # owner \/ staleFence # fence
  /\ UNCHANGED <<effect_state, fence, owner, decision_allowed, committed_count>>

Next ==
  \E o \in Owners :
      Claim(o)
      \/ \E f \in Nat :
           RecordDecisionAllow(o, f)
           \/ RecordDecisionDeny(o, f)
           \/ Complete(o, f)
           \/ FailBeforeEffect(o, f)
           \/ FailAfterEffect(o, f)
           \/ MarkUnknown(o, f)
           \/ StaleFenceWrite(o, f)

TypeOK ==
  /\ effect_state \in States
  /\ fence \in Nat
  /\ owner \in Owners \cup {"none"}
  /\ decision_allowed \in BOOLEAN
  /\ committed_count \in Nat

AtMostOneCommitted ==
  committed_count <= 1

Spec == Init /\ [][Next]_<<effect_state, fence, owner, decision_allowed, committed_count>>

THEOREM Spec => []TypeOK
THEOREM Spec => []AtMostOneCommitted
====
