"""EffectState state-machine checks: transitions, legacy rows, and crash paths."""

from __future__ import annotations

import time
from dataclasses import replace
from typing import Any

import pytest

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
    execution_scope,
    get_ledger,
    ledger_sync,
    side_effect,
)


def _decision(allowed: bool) -> dict[str, Any]:
    return {"allowed": allowed, "verdicts": [], "denied_reasons": []}


def _binding() -> ToolTransitionBinding:
    return ToolTransitionBinding.for_tool(
        agent_id="state-machine",
        policy_version="1",
        side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
    )


def _blind_binding() -> ToolTransitionBinding:
    return ToolTransitionBinding.for_tool(
        agent_id="state-machine",
        policy_version="1",
        side_effect_class=SideEffectClass.KEYED_MUTATE,
        provider_idempotency_key_param="idempotency_key",
        provider_idempotency_key_ttl=300.0,
        capability=ToolCapability.BLIND,
    )


def _scope() -> TransitionScope:
    return TransitionScope(thread_id="thread-1", run_id="run-1")


def test_effect_state_transition_matrix_and_illegal_cas_rejections() -> None:
    storage = InMemoryLedgerStorage()
    ledger = ActionLedger(storage=storage)
    binding = _binding()

    # INTENDED -> ATTEMPTING -> COMMITTED
    allowed = ledger.claim_side_effecting(
        "req-allowed", "tool", (), {"case": "allowed"}, binding
    )
    ledger.record_decision(
        "req-allowed",
        _decision(True),
        expected_fence=allowed.fence,
    )
    committed = ledger.complete("req-allowed", {"ok": True}, expected_fence=allowed.fence)
    assert committed.resolved_effect_state() == EffectState.COMMITTED

    # INTENDED -> ABORTED
    denied = ledger.claim_side_effecting(
        "req-denied", "tool", (), {"case": "denied"}, binding
    )
    denied_row = ledger.record_decision(
        "req-denied",
        _decision(False),
        expected_fence=denied.fence,
    )
    assert denied_row.resolved_effect_state() == EffectState.ABORTED

    # ATTEMPTING -> UNKNOWN (BLIND-like after-effect failure)
    unknown = ledger.claim_side_effecting(
        "req-unknown", "tool", (), {"case": "unknown"}, binding
    )
    ledger.record_decision(
        "req-unknown",
        _decision(True),
        expected_fence=unknown.fence,
    )
    unknown_row = ledger.mark_unknown(
        "req-unknown",
        expected_fence=unknown.fence,
        error="ambiguous",
    )
    assert unknown_row.resolved_effect_state() == EffectState.UNKNOWN

    # ATTEMPTING -> ABORTED (failure before effect after allow)
    failed = ledger.claim_side_effecting(
        "req-fail-before", "tool", (), {"case": "fail-before"}, binding
    )
    ledger.record_decision(
        "req-fail-before",
        _decision(True),
        expected_fence=failed.fence,
    )
    failed_row = ledger.fail(
        "req-fail-before",
        RuntimeError("failed before effect"),
        failed_after_effect=False,
        expected_fence=failed.fence,
    )
    assert failed_row.resolved_effect_state() == EffectState.ABORTED

    # ILLEGAL: complete before decision (still INTENDED)
    no_decision = ledger.claim_side_effecting(
        "req-no-decision", "tool", (), {"case": "no-decision"}, binding
    )
    with pytest.raises(LedgerOutcomeAlreadySetError):
        ledger.complete(
            "req-no-decision",
            {"ok": True},
            expected_fence=no_decision.fence,
        )

    # ILLEGAL: once COMMITTED, further mutations must fail.
    with pytest.raises(LedgerOutcomeAlreadySetError):
        ledger.mark_unknown(
            "req-allowed",
            expected_fence=allowed.fence,
            error="late change",
        )

    # ILLEGAL: ABORTED row cannot transition back to ATTEMPTING.
    with pytest.raises(LedgerOutcomeAlreadySetError):
        ledger.record_decision(
            "req-denied",
            _decision(True),
            expected_fence=denied.fence,
        )

    # ILLEGAL overwrite: storage claim cannot blind-overwrite UNKNOWN.
    stored_unknown = ledger.get("req-unknown")
    assert stored_unknown is not None
    fresh_claim = replace(
        stored_unknown,
        terminal_outcome=TerminalOutcome.IN_FLIGHT.value,
        effect_phase=EffectState.INTENDED.value,
        decision=None,
        fence=stored_unknown.fence + 1,
        owner="new-owner",
    )
    outcome, existing = storage.try_claim_inflight(fresh_claim)
    assert outcome == "in_flight"
    assert existing is not None
    assert existing.resolved_effect_state() == EffectState.UNKNOWN


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (
            {
                "request_id": "legacy-completed",
                "tool": "tool",
                "args": [],
                "kwargs": {},
                "status": "completed",
                "terminal_outcome": "COMPLETED",
                "effect_phase": "INTENDED",
            },
            EffectState.COMMITTED,
        ),
        (
            {
                "request_id": "legacy-failed-after",
                "tool": "tool",
                "args": [],
                "kwargs": {},
                "status": "failed",
                "terminal_outcome": "FAILED_AFTER_EFFECT",
            },
            EffectState.UNKNOWN,
        ),
        (
            {
                "request_id": "legacy-blocked-not-crossed",
                "tool": "tool",
                "args": [],
                "kwargs": {},
                "status": "failed",
                "terminal_outcome": "BLOCKED",
                "side_effect_boundary": "not_crossed",
            },
            EffectState.ABORTED,
        ),
        (
            {
                "request_id": "legacy-blocked-maybe-crossed",
                "tool": "tool",
                "args": [],
                "kwargs": {},
                "status": "failed",
                "terminal_outcome": "BLOCKED",
                "side_effect_boundary": "maybe_crossed",
            },
            EffectState.UNKNOWN,
        ),
        (
            {
                "request_id": "legacy-expired-not-crossed",
                "tool": "tool",
                "args": [],
                "kwargs": {},
                "status": "in-flight",
                "terminal_outcome": "EXPIRED",
                "side_effect_boundary": "not_crossed",
            },
            EffectState.ABORTED,
        ),
        (
            {
                "request_id": "legacy-inflight-attempting",
                "tool": "tool",
                "args": [],
                "kwargs": {},
                "status": "in-flight",
                "terminal_outcome": "IN_FLIGHT",
                "effect_phase": "ATTEMPTING",
                "decision": _decision(True),
            },
            EffectState.ATTEMPTING,
        ),
        (
            {
                "request_id": "legacy-inflight-aborted",
                "tool": "tool",
                "args": [],
                "kwargs": {},
                "status": "in-flight",
                "terminal_outcome": "IN_FLIGHT",
                "effect_phase": "ABORTED",
                "decision": _decision(False),
            },
            EffectState.ABORTED,
        ),
        (
            {
                "request_id": "legacy-inflight-intended",
                "tool": "tool",
                "args": [],
                "kwargs": {},
                "status": "in-flight",
                "terminal_outcome": "IN_FLIGHT",
                "effect_phase": "ATTEMPTING",
            },
            EffectState.INTENDED,
        ),
        (
            {
                "request_id": "legacy-garbage-terminal",
                "tool": "tool",
                "args": [],
                "kwargs": {},
                "status": "in-flight",
                "terminal_outcome": "GARBAGE",
                "effect_phase": "ABORTED",
                "decision": _decision(False),
            },
            EffectState.ABORTED,
        ),
    ],
)
def test_legacy_deserialization_resolves_unified_effect_state(
    raw: dict[str, Any], expected: EffectState
) -> None:
    entry = LedgerEntry.from_dict(raw)
    assert entry.resolved_effect_state() == expected


def _idempotent_safe_retry_binding() -> ToolTransitionBinding:
    # Same override style as test_spendability_override_allows_expired_reclaim:
    # multi_use + SAFE_RETRY is what authorizes EXPIRED + not_crossed reclaim.
    return ToolTransitionBinding.for_tool(
        agent_id="state-machine",
        policy_version="1",
        side_effect_class=SideEffectClass.IDEMPOTENT_MUTATE,
        spendability=Spendability.MULTI_USE,
        retry_permission=RetryPermission.SAFE_RETRY,
    )


def _craft_intended_expired(
    storage: InMemoryLedgerStorage,
    *,
    request_id: str,
    tool: str,
    kwargs: dict[str, Any],
) -> None:
    storage.set(
        LedgerEntry(
            request_id=request_id,
            tool=tool,
            args=[],
            kwargs=dict(kwargs),
            status="in-flight",
            terminal_outcome=TerminalOutcome.IN_FLIGHT.value,
            owner="dead-worker",
            lease_until=time.time() - 1,
            effect_phase=EffectState.INTENDED.value,
            decision=None,
            effect_protocol_required=True,
        )
    )


def test_crash_before_attempting_idempotent_class_auto_reclaims() -> None:
    """EXPIRED + INTENDED auto-reclaims only for multi_use + SAFE_RETRY."""
    binding = _idempotent_safe_retry_binding()
    attempts = {"n": 0}

    # Table-driven across two independent pre-attempt crashes.
    for crash_round in (1, 2):
        storage = InMemoryLedgerStorage()
        provider_results: list[dict[str, float]] = []

        @ledger_sync(storage=storage, transition_binding=binding)
        def charge(amount: float) -> dict[str, float]:
            attempts["n"] += 1
            result = {"amount": amount}
            provider_results.append(result)
            return result

        ledger = get_ledger(charge)
        assert ledger is not None
        tool_call_id = f"c-idem-{crash_round}"
        with execution_scope(_scope()):
            request_id = ledger.derive_request_id(
                "charge",
                (),
                {"amount": 10.0, "tool_call_id": tool_call_id},
                transition_binding=binding,
            )
        _craft_intended_expired(
            storage,
            request_id=request_id,
            tool="charge",
            kwargs={"amount": 10.0, "tool_call_id": tool_call_id},
        )

        with execution_scope(_scope()):
            assert charge(amount=10.0, tool_call_id=tool_call_id) == {"amount": 10.0}
            # Redispatch after commit must not duplicate the provider effect.
            assert charge(amount=10.0, tool_call_id=tool_call_id) == {"amount": 10.0}

        assert provider_results == [{"amount": 10.0}]
        stored = ledger.get(request_id)
        assert stored is not None
        assert stored.resolved_effect_state() == EffectState.COMMITTED

    assert attempts["n"] == 2


def test_crash_before_attempting_strict_class_fails_closed() -> None:
    """EXPIRED + INTENDED + non_idempotent_mutate must hard-block, never re-run."""
    storage = InMemoryLedgerStorage()
    binding = _binding()
    attempts = {"n": 0}

    @ledger_sync(storage=storage, transition_binding=binding)
    def charge(amount: float) -> dict[str, float]:
        attempts["n"] += 1
        return {"amount": amount}

    ledger = get_ledger(charge)
    assert ledger is not None
    with execution_scope(_scope()):
        request_id = ledger.derive_request_id(
            "charge",
            (),
            {"amount": 10.0, "tool_call_id": "c-strict"},
            transition_binding=binding,
        )
    _craft_intended_expired(
        storage,
        request_id=request_id,
        tool="charge",
        kwargs={"amount": 10.0, "tool_call_id": "c-strict"},
    )

    with execution_scope(_scope()):
        with pytest.raises(LedgerHardBlockError, match="manual reconciliation"):
            charge(amount=10.0, tool_call_id="c-strict")

    assert attempts["n"] == 0
    stored = ledger.get(request_id)
    assert stored is not None
    # Fail-closed park: EXPIRED reclaim is refused and the row is sealed for
    # operator/reconciler attention (not auto-reclaimed back to ATTEMPTING).
    assert stored.resolved_effect_state() == EffectState.ABORTED
    assert stored.terminal_outcome == TerminalOutcome.BLOCKED.value


def test_crash_during_attempting_blind_becomes_unknown_and_hard_blocks() -> None:
    storage = InMemoryLedgerStorage()
    binding = _blind_binding()
    attempts = {"n": 0}

    @ledger_sync(storage=storage, transition_binding=binding)
    def send_payment(amount: float, idempotency_key: str) -> dict[str, str]:
        attempts["n"] += 1
        with side_effect():
            raise RuntimeError("timeout after maybe-crossed")

    with execution_scope(_scope()):
        with pytest.raises(RuntimeError):
            send_payment(amount=10.0, idempotency_key="k1", tool_call_id="c1")
        with pytest.raises(LedgerHardBlockError):
            send_payment(amount=10.0, idempotency_key="k1", tool_call_id="c1")

    assert attempts["n"] == 1
    ledger = get_ledger(send_payment)
    assert ledger is not None
    with execution_scope(_scope()):
        request_id = ledger.derive_request_id(
            "send_payment",
            (),
            {"amount": 10.0, "idempotency_key": "k1", "tool_call_id": "c1"},
            transition_binding=binding,
        )
    stored = ledger.get(request_id)
    assert stored is not None
    assert stored.resolved_effect_state() == EffectState.UNKNOWN

    fresh_claim = replace(
        stored,
        terminal_outcome=TerminalOutcome.IN_FLIGHT.value,
        effect_phase=EffectState.INTENDED.value,
        decision=None,
        fence=stored.fence + 1,
        owner="new-worker",
    )
    outcome, existing = storage.try_claim_inflight(fresh_claim)
    assert outcome == "in_flight"
    assert existing is not None
    assert existing.resolved_effect_state() == EffectState.UNKNOWN
