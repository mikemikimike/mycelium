"""Deterministic exhaustive EffectState interleavings over in-memory storage."""

from __future__ import annotations

import threading
import time
import warnings
from dataclasses import replace
from typing import Any

from mycelium import (
    ActionLedger,
    EffectState,
    InMemoryLedgerStorage,
    LedgerEntry,
    LedgerHardBlockError,
    LedgerOutcomeAlreadySetError,
    RetryPermission,
    SideEffectClass,
    Spendability,
    TerminalOutcome,
    ToolCapability,
    ToolTransitionBinding,
    TransitionScope,
    derive_effect_id_for_call,
    execution_scope,
)
from mycelium.verify.invariants import check_at_most_one_committed_effect_state
from mycelium.verify.registry import ScenarioContext, verify_scenario
from mycelium.verify.types import VerificationEvidence, VerificationStatus


def _decision(allowed: bool) -> dict[str, Any]:
    return {"allowed": allowed, "verdicts": [], "denied_reasons": []}


def _scope() -> TransitionScope:
    return TransitionScope(
        thread_id="verify-state-machine",
        run_id="verify-state-machine",
    )


def _idempotent_binding() -> ToolTransitionBinding:
    return ToolTransitionBinding.for_tool(
        agent_id="verify",
        policy_version="verify",
        side_effect_class=SideEffectClass.IDEMPOTENT_MUTATE,
        spendability=Spendability.MULTI_USE,
        retry_permission=RetryPermission.SAFE_RETRY,
    )


def _blind_binding() -> ToolTransitionBinding:
    return ToolTransitionBinding.for_tool(
        agent_id="verify",
        policy_version="verify",
        side_effect_class=SideEffectClass.KEYED_MUTATE,
        capability=ToolCapability.BLIND,
        provider_idempotency_key_param="idempotency_key",
        provider_idempotency_key_ttl=300.0,
    )


def _expire_lease(storage: InMemoryLedgerStorage, request_id: str) -> None:
    entry = storage.get(request_id)
    if entry is None:
        return
    storage.set(
        replace(
            entry,
            lease_until=time.time() - 1.0,
            last_heartbeat_at=time.time() - 3600.0,
        )
    )


def _craft_expired_crash_row(
    storage: InMemoryLedgerStorage,
    *,
    request_id: str,
    binding: ToolTransitionBinding,
    kwargs: dict[str, Any],
    crash_step: int,
) -> None:
    """Persist a crashed intermediate row with an already-expired lease.

    crash_step=1 → INTENDED (claimed, no decision)
    crash_step=2 → ATTEMPTING (decision recorded, body never finished)
    """
    effect_id = derive_effect_id_for_call("charge", (), kwargs, binding)
    decision = _decision(True) if crash_step >= 2 else None
    phase = (
        EffectState.ATTEMPTING.value if crash_step >= 2 else EffectState.INTENDED.value
    )
    storage.set(
        LedgerEntry(
            request_id=request_id,
            tool="charge",
            args=[],
            kwargs=dict(kwargs),
            status="in-flight",
            terminal_outcome=TerminalOutcome.IN_FLIGHT.value,
            owner="dead-worker",
            lease_until=time.time() - 1.0,
            last_heartbeat_at=time.time() - 3600.0,
            effect_phase=phase,
            decision=decision,
            effect_protocol_required=True,
            effect_id=effect_id,
        )
    )


def _assert_at_most_one_committed(
    storage: InMemoryLedgerStorage,
    *,
    effect_id: str | None,
    failures: list[str],
    label: str,
) -> None:
    if not effect_id:
        failures.append(f"{label}: missing effect_id")
        return
    rows = [entry for entry in storage.list_all() if entry.effect_id == effect_id]
    for violation in check_at_most_one_committed_effect_state(rows):
        failures.append(f"{label}: {violation.message}")


def _run_stale_fence_interleaving(stale_op: str) -> tuple[list[str], list[str]]:
    failures: list[str] = []
    decisions: list[str] = []
    storage = InMemoryLedgerStorage()
    ledger_a = ActionLedger(storage=storage, poll_interval=0.001, poll_timeout=1.0)
    ledger_b = ActionLedger(storage=storage, poll_interval=0.001, poll_timeout=1.0)
    binding = _idempotent_binding()
    request_id = f"stale-{stale_op}"
    kwargs = {"amount": 10, "tool_call_id": f"stale-{stale_op}-call"}
    with execution_scope(_scope()):
        entry_a = ledger_a.claim_side_effecting(request_id, "charge", (), kwargs, binding)
    _expire_lease(storage, request_id)
    with execution_scope(_scope()):
        entry_b = ledger_b.claim_side_effecting(
            request_id,
            "charge",
            (),
            kwargs,
            binding,
        )
    if entry_b.fence != entry_a.fence + 1:
        failures.append(
            f"{stale_op}: reclaim fence mismatch, "
            f"expected {entry_a.fence + 1}, got {entry_b.fence}"
        )
        return failures, decisions
    if stale_op != "record_decision":
        with execution_scope(_scope()):
            ledger_b.record_decision(
                request_id,
                _decision(True),
                expected_owner=entry_b.owner,
                expected_fence=entry_b.fence,
            )
    stale_rejected = False
    try:
        with execution_scope(_scope()):
            if stale_op == "complete":
                ledger_a.complete(
                    request_id,
                    {"charged": "stale"},
                    _expected_owner=entry_a.owner,
                    _expected_fence=entry_a.fence,
                )
            elif stale_op == "fail":
                ledger_a.fail(
                    request_id,
                    RuntimeError("stale"),
                    failed_after_effect=False,
                    _expected_owner=entry_a.owner,
                    _expected_fence=entry_a.fence,
                )
            else:
                ledger_a.record_decision(
                    request_id,
                    _decision(True),
                    expected_owner=entry_a.owner,
                    expected_fence=entry_a.fence,
                )
    except LedgerOutcomeAlreadySetError:
        stale_rejected = True
    if not stale_rejected:
        failures.append(f"{stale_op}: stale write unexpectedly succeeded")
    with execution_scope(_scope()):
        if stale_op == "record_decision":
            ledger_b.record_decision(
                request_id,
                _decision(True),
                expected_owner=entry_b.owner,
                expected_fence=entry_b.fence,
            )
        ledger_b.complete(
            request_id,
            {"charged": True},
            _expected_owner=entry_b.owner,
            _expected_fence=entry_b.fence,
        )
    stored = storage.get(request_id)
    _assert_at_most_one_committed(
        storage,
        effect_id=stored.effect_id if stored is not None else None,
        failures=failures,
        label=f"stale-{stale_op}",
    )
    decisions.append(f"stale-{stale_op}: stale-fence write rejected after fence takeover")
    return failures, decisions


def _run_crash_resume_matrix(target: str) -> tuple[list[str], list[str]]:
    failures: list[str] = []
    decisions: list[str] = []
    expected = {
        "completed": EffectState.COMMITTED,
        "not_executed": EffectState.ABORTED,
        "unknown": EffectState.UNKNOWN,
    }[target]
    for crash_step in range(0, 3):
        storage = InMemoryLedgerStorage()
        binding = _idempotent_binding()
        request_id = f"crash-{target}-{crash_step}"
        kwargs = {"amount": 20, "tool_call_id": f"crash-{target}-{crash_step}"}
        # Deterministic crash points: craft expired intermediate rows so
        # resume never depends on wall-clock lease expiry of a live claim.
        if crash_step >= 1:
            _craft_expired_crash_row(
                storage,
                request_id=request_id,
                binding=binding,
                kwargs=kwargs,
                crash_step=crash_step,
            )
        ledger_b = ActionLedger(storage=storage, poll_interval=0.001, poll_timeout=1.0)
        try:
            with execution_scope(_scope()):
                resumed = ledger_b.claim_side_effecting(
                    request_id,
                    "charge",
                    (),
                    kwargs,
                    binding,
                )
                current = storage.get(resumed.request_id)
                if current is None:
                    failures.append(f"{request_id}: missing row after resume claim")
                    continue
                if current.decision is None:
                    ledger_b.record_decision(
                        resumed.request_id,
                        _decision(True),
                        expected_owner=resumed.owner,
                        expected_fence=resumed.fence,
                    )
                if target == "completed":
                    ledger_b.complete(
                        resumed.request_id,
                        {"charged": True},
                        _expected_owner=resumed.owner,
                        _expected_fence=resumed.fence,
                    )
                elif target == "not_executed":
                    ledger_b.fail(
                        resumed.request_id,
                        RuntimeError("provider not executed"),
                        failed_after_effect=False,
                        _expected_owner=resumed.owner,
                        _expected_fence=resumed.fence,
                    )
                else:
                    ledger_b.mark_unknown(
                        resumed.request_id,
                        error="ambiguous",
                        _expected_owner=resumed.owner,
                        _expected_fence=resumed.fence,
                    )
        except LedgerHardBlockError as exc:
            failures.append(f"{request_id}: unexpected HARD_BLOCK: {exc}")
            continue
        final = storage.get(request_id)
        if final is None:
            failures.append(f"{request_id}: final row missing")
            continue
        if final.resolved_effect_state() != expected:
            got = final.resolved_effect_state().value
            failures.append(
                f"{request_id}: expected {expected.value}, got {got}"
            )
        _assert_at_most_one_committed(
            storage,
            effect_id=final.effect_id,
            failures=failures,
            label=request_id,
        )
        decisions.append(
            f"{request_id}: crash@step{crash_step} resumed to "
            f"{final.resolved_effect_state().value}"
        )
    return failures, decisions


def _run_blind_unknown_no_retry() -> tuple[list[str], list[str], int]:
    failures: list[str] = []
    decisions: list[str] = []
    executions = 0
    storage = InMemoryLedgerStorage()
    ledger_a = ActionLedger(storage=storage, poll_interval=0.001, poll_timeout=1.0)
    ledger_b = ActionLedger(storage=storage, poll_interval=0.001, poll_timeout=1.0)
    binding = _blind_binding()
    request_id = "blind-unknown"
    kwargs = {
        "amount": 5,
        "idempotency_key": "blind-key",
        "tool_call_id": "blind-call",
    }
    with execution_scope(_scope()):
        claim = ledger_a.claim_side_effecting(
            request_id, "send_payment", (), kwargs, binding
        )
        ledger_a.record_decision(
            request_id,
            _decision(True),
            expected_owner=claim.owner,
            expected_fence=claim.fence,
        )
        executions += 1
        ledger_a.mark_unknown(
            request_id,
            error="timeout after maybe crossed",
            _expected_owner=claim.owner,
            _expected_fence=claim.fence,
        )
    blocked = False
    try:
        with execution_scope(_scope()):
            ledger_b.claim_side_effecting(
                request_id, "send_payment", (), kwargs, binding
            )
    except LedgerHardBlockError:
        blocked = True
    if not blocked:
        failures.append("blind-unknown: redispatch unexpectedly bypassed HARD_BLOCK")
    if executions != 1:
        failures.append(f"blind-unknown: expected 1 execution, got {executions}")
    final = storage.get(request_id)
    if final is None:
        failures.append("blind-unknown: final row missing")
    else:
        _assert_at_most_one_committed(
            storage,
            effect_id=final.effect_id,
            failures=failures,
            label="blind-unknown",
        )
    decisions.append(
        "blind-unknown: UNKNOWN stayed parked and body counter remained 1"
    )
    return failures, decisions, executions


def _run_concurrent_intended_claim_race() -> tuple[list[str], list[str]]:
    failures: list[str] = []
    decisions: list[str] = []
    storage = InMemoryLedgerStorage()
    ledger_a = ActionLedger(storage=storage, poll_interval=0.001, poll_timeout=1.0)
    ledger_b = ActionLedger(storage=storage, poll_interval=0.001, poll_timeout=1.0)
    binding = _idempotent_binding()
    kwargs = {"amount": 42, "tool_call_id": "race-call"}
    request_a = "race-a"
    request_b = "race-b"
    results: dict[str, Any] = {}
    errors: dict[str, BaseException] = {}
    first_returned = threading.Event()
    lock = threading.Lock()

    def _claim(name: str, ledger: ActionLedger, request_id: str) -> None:
        try:
            with execution_scope(_scope()):
                entry = ledger.claim_side_effecting(
                    request_id, "charge", (), kwargs, binding
                )
            with lock:
                results[name] = entry
                first_returned.set()
        except BaseException as exc:  # pragma: no cover - defensive
            with lock:
                errors[name] = exc
                first_returned.set()

    t_a = threading.Thread(target=_claim, args=("A", ledger_a, request_a))
    t_b = threading.Thread(target=_claim, args=("B", ledger_b, request_b))
    t_a.start()
    t_b.start()
    if not first_returned.wait(timeout=1.0):
        failures.append("race: no worker returned from initial claim")
    with lock:
        winner_name = next(iter(results.keys()), None)
    if winner_name is None:
        failures.append(f"race: no winner, errors={errors!r}")
        t_a.join(timeout=1.0)
        t_b.join(timeout=1.0)
        return failures, decisions
    winner_entry = results[winner_name]
    winner_ledger = ledger_a if winner_name == "A" else ledger_b
    with execution_scope(_scope()):
        winner_ledger.record_decision(
            winner_entry.request_id,
            _decision(True),
            expected_owner=winner_entry.owner,
            expected_fence=winner_entry.fence,
        )
        winner_ledger.complete(
            winner_entry.request_id,
            {"charged": "race"},
            _expected_owner=winner_entry.owner,
            _expected_fence=winner_entry.fence,
        )
    t_a.join(timeout=1.0)
    t_b.join(timeout=1.0)
    if errors:
        failures.append(f"race: claim error(s) observed: {errors!r}")
    if set(results.keys()) != {"A", "B"}:
        failures.append(
            f"race: expected both workers to return, got {sorted(results.keys())}"
        )
    if len(results) == 2:
        first = results["A"]
        second = results["B"]
        if first.request_id != second.request_id:
            failures.append(
                "race: workers did not converge on a single canonical request row "
                f"({first.request_id!r} vs {second.request_id!r})"
            )
        if first.effect_id != second.effect_id:
            failures.append(
                "race: workers disagreed on effect_id for shared INTENDED claim"
            )
    rows = storage.list_all()
    if len(rows) != 1:
        failures.append(f"race: expected exactly one ledger row, found {len(rows)}")
    effect_id = rows[0].effect_id if rows else None
    _assert_at_most_one_committed(
        storage,
        effect_id=effect_id,
        failures=failures,
        label="race",
    )
    decisions.append(
        "race: concurrent INTENDED claims converged to one row and one COMMITTED"
    )
    return failures, decisions


@verify_scenario("state-machine-exhaustive")
def run_state_machine_exhaustive(ctx: ScenarioContext) -> VerificationEvidence:
    started = time.time()
    failures: list[str] = []
    decisions: list[str] = []
    total_executions = 0

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=r".*InMemoryLedgerStorage.*",
            category=UserWarning,
        )

        for stale_op in ("complete", "fail", "record_decision"):
            stale_failures, stale_decisions = _run_stale_fence_interleaving(stale_op)
            failures.extend(stale_failures)
            decisions.extend(stale_decisions)

        for target in ("completed", "not_executed", "unknown"):
            case_failures, case_decisions = _run_crash_resume_matrix(target)
            failures.extend(case_failures)
            decisions.extend(case_decisions)

        blind_failures, blind_decisions, executions = _run_blind_unknown_no_retry()
        failures.extend(blind_failures)
        decisions.extend(blind_decisions)
        total_executions += executions

        race_failures, race_decisions = _run_concurrent_intended_claim_race()
        failures.extend(race_failures)
        decisions.extend(race_decisions)

    isolation = getattr(ctx, "isolation", None)
    backend = getattr(isolation, "backend", "memory")
    namespace = getattr(
        getattr(isolation, "namespace", None),
        "prefix",
        "state-machine-exhaustive",
    )
    observed = "; ".join(failures or decisions)
    ok = not failures
    return VerificationEvidence(
        scenario="state-machine-exhaustive",
        backend=str(backend),
        namespace=str(namespace),
        attempts=3 + (3 * 3) + 1 + 1,
        body_executions=total_executions,
        ledger_decisions=decisions,
        terminal_outcome="COMMITTED" if ok else "FAILED",
        duration=time.time() - started,
        expected_behavior=(
            "INTENDED->ATTEMPTING->COMMITTED|ABORTED|UNKNOWN transitions remain "
            "fenced; at-most-one COMMITTED row holds under crash/resume and race "
            "interleavings"
        ),
        observed_behavior=observed,
        artifacts=[],
        limitations=[],
        status=VerificationStatus.PASS if ok else VerificationStatus.FAIL,
        summary=(
            "exhaustive state-machine interleavings held"
            if ok
            else "; ".join(failures)[:200]
        ),
        remediation="" if ok else "Inspect failed interleaving(s) in observed_behavior.",
    )


__all__ = ["run_state_machine_exhaustive"]
