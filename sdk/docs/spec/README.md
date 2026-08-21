# EffectState spec notes

This folder contains a lightweight TLA+ model of the one-row effect protocol in
`sdk/docs/spec/effect_state.tla`.

What it models:
- One durable row with unified `EffectState` (`INTENDED`, `ATTEMPTING`,
  `COMMITTED`, `ABORTED`, `UNKNOWN`), plus fencing metadata (`owner`, `fence`).
- The legal protocol edges: `INTENDED -> ATTEMPTING -> COMMITTED|ABORTED|UNKNOWN`.
- CAS-guarded stale writes as no-op transitions (`StaleFenceWrite`) to model
  rejected stale owner/fence mutations.
- Safety invariants:
  - `TypeOK` (well-typed state)
  - `AtMostOneCommitted` (`committed_count <= 1`)

Action mapping to runtime methods:
- `Claim` -> `ActionLedger.claim_side_effecting` (claim/reclaim fence bump)
- `RecordDecisionAllow` / `RecordDecisionDeny` -> `ActionLedger.record_decision`
  with expected owner/fence and expected intent `INTENDED`
- `Complete` -> `ActionLedger.complete` with expected owner/fence and
  `ATTEMPTING` precondition
- `FailBeforeEffect` -> `ActionLedger.fail(... failed_after_effect=False)`
- `FailAfterEffect` / `MarkUnknown` ->
  `ActionLedger.fail(... failed_after_effect=True)` /
  `ActionLedger.mark_unknown`
- `StaleFenceWrite` -> rejected stale `complete` / `fail` /
  `record_decision` CAS attempts

Test mapping in this repo:
- Transition matrix seed: `sdk/tests/test_effect_state_machine.py`
- Deterministic exhaustive interleavings:
  `sdk/mycelium/verify/scenarios/state_machine_exhaustive.py`
- Verify simulation invariant sweep:
  `sdk/mycelium/verify/scenarios/simulation.py`

Optional manual TLC run (not in CI):
1. Install TLA+ tools locally (or use the Toolbox CLI).
2. From this directory run:
   - `tlc -simulate effect_state.tla`
   - optionally add depth/seed flags for repeatable traces.

This spec artifact is documentation and model-checking support only; CI does not
execute TLC.
