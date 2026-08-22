"""Tests for tool capability typing (probeability axis).

Change 3 of the effect-commit protocol adds an explicit *probeability* axis
orthogonal to :class:`SideEffectClass`:

- ``IDEMPOTENT`` — accepts the effect_id; retries are always safe.
- ``QUERYABLE`` — the outcome can be probed (provider idempotency key or a
  registered ``Reconciler``); recovery resolves the ambiguous state by probing.
- ``BLIND`` — neither; an ambiguous entry parks for operator reconciliation and
  is **never** auto-retried.

``resolve_capability`` is conservative: an explicit declaration may tighten to
BLIND freely, but may only loosen to QUERYABLE/IDEMPOTENT when the class plus
the available mechanism supports it — otherwise it refuses.
"""

from __future__ import annotations

import pytest

from mycelium import (
    InMemoryLedgerStorage,
    LedgerHardBlockError,
    ReconcileResult,
    SideEffectClass,
    TerminalOutcome,
    ToolCapability,
    ToolCapabilityDeclarationError,
    ToolTransitionBinding,
    TransitionScope,
    execution_scope,
    get_ledger,
    ledger_sync,
    record_external_operation,
    resolve_capability,
    side_effect,
)
from mycelium.transition import default_capability, parse_capability

# --- resolve_capability derivation table -----------------------------------


def test_read_and_idempotent_mutate_are_idempotent() -> None:
    assert default_capability(SideEffectClass.READ) == ToolCapability.IDEMPOTENT
    assert (
        default_capability(SideEffectClass.IDEMPOTENT_MUTATE)
        == ToolCapability.IDEMPOTENT
    )


def test_keyed_mutate_queryable_with_provider_key_else_blind() -> None:
    assert (
        default_capability(SideEffectClass.KEYED_MUTATE, has_provider_key=True)
        == ToolCapability.QUERYABLE
    )
    assert (
        default_capability(SideEffectClass.KEYED_MUTATE, has_reconciler=True)
        == ToolCapability.QUERYABLE
    )
    assert default_capability(SideEffectClass.KEYED_MUTATE) == ToolCapability.BLIND


def test_non_idempotent_mutate_queryable_only_with_reconciler() -> None:
    assert (
        default_capability(
            SideEffectClass.NON_IDEMPOTENT_MUTATE, has_reconciler=True
        )
        == ToolCapability.QUERYABLE
    )
    # A provider key alone does not make a non-idempotent tool queryable.
    assert (
        default_capability(
            SideEffectClass.NON_IDEMPOTENT_MUTATE, has_provider_key=True
        )
        == ToolCapability.BLIND
    )
    assert (
        default_capability(SideEffectClass.NON_IDEMPOTENT_MUTATE)
        == ToolCapability.BLIND
    )


def test_irreversible_is_always_blind() -> None:
    assert default_capability(SideEffectClass.IRREVERSIBLE) == ToolCapability.BLIND
    assert (
        default_capability(
            SideEffectClass.IRREVERSIBLE,
            has_reconciler=True,
            has_provider_key=True,
        )
        == ToolCapability.BLIND
    )


# --- explicit declaration: tighten freely, loosen only when supported ------


def test_explicit_tighten_to_blind_always_allowed() -> None:
    assert (
        resolve_capability(
            SideEffectClass.IDEMPOTENT_MUTATE, ToolCapability.BLIND
        )
        == ToolCapability.BLIND
    )
    assert (
        resolve_capability(
            SideEffectClass.KEYED_MUTATE,
            ToolCapability.BLIND,
            has_provider_key=True,
        )
        == ToolCapability.BLIND
    )


def test_explicit_loosen_supported_is_honored() -> None:
    assert (
        resolve_capability(
            SideEffectClass.KEYED_MUTATE,
            ToolCapability.QUERYABLE,
            has_provider_key=True,
        )
        == ToolCapability.QUERYABLE
    )
    assert (
        resolve_capability(
            SideEffectClass.NON_IDEMPOTENT_MUTATE,
            ToolCapability.QUERYABLE,
            has_reconciler=True,
        )
        == ToolCapability.QUERYABLE
    )


def test_explicit_loosen_unsupported_refuses() -> None:
    with pytest.raises(ToolCapabilityDeclarationError):
        resolve_capability(
            SideEffectClass.IRREVERSIBLE, ToolCapability.QUERYABLE
        )
    with pytest.raises(ToolCapabilityDeclarationError):
        resolve_capability(
            SideEffectClass.IRREVERSIBLE, ToolCapability.IDEMPOTENT
        )
    with pytest.raises(ToolCapabilityDeclarationError):
        resolve_capability(
            SideEffectClass.NON_IDEMPOTENT_MUTATE,
            ToolCapability.IDEMPOTENT,
            has_reconciler=True,
        )


def test_parse_capability() -> None:
    assert parse_capability("idempotent") == ToolCapability.IDEMPOTENT
    assert parse_capability("queryable") == ToolCapability.QUERYABLE
    assert parse_capability("blind") == ToolCapability.BLIND
    assert parse_capability(ToolCapability.BLIND) == ToolCapability.BLIND
    with pytest.raises(ValueError, match="invalid capability"):
        parse_capability("teleport")


# --- binding wiring ---------------------------------------------------------


def test_binding_derives_capability_default() -> None:
    binding = ToolTransitionBinding.for_tool(
        agent_id="demo",
        policy_version="1",
        side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
    )
    assert binding.capability == ToolCapability.BLIND
    assert binding.explicit_capability is None


def test_binding_keyed_with_provider_key_is_queryable() -> None:
    binding = ToolTransitionBinding.for_tool(
        agent_id="demo",
        policy_version="1",
        side_effect_class=SideEffectClass.KEYED_MUTATE,
        provider_idempotency_key_param="idempotency_key",
    )
    assert binding.capability == ToolCapability.QUERYABLE


def test_binding_explicit_blind_wins_over_provider_key() -> None:
    binding = ToolTransitionBinding.for_tool(
        agent_id="demo",
        policy_version="1",
        side_effect_class=SideEffectClass.KEYED_MUTATE,
        provider_idempotency_key_param="idempotency_key",
        capability=ToolCapability.BLIND,
    )
    assert binding.capability == ToolCapability.BLIND
    assert binding.explicit_capability == ToolCapability.BLIND


def test_binding_explicit_blind_stays_blind_with_effect_key_propagation() -> None:
    binding = ToolTransitionBinding.for_tool(
        agent_id="demo",
        policy_version="1",
        side_effect_class=SideEffectClass.KEYED_MUTATE,
        provider_idempotency_key_param="idempotency_key",
        propagate_effect_id_as_provider_key=True,
        capability=ToolCapability.BLIND,
    )
    assert binding.capability == ToolCapability.BLIND
    assert binding.effective_capability(has_reconciler=True) == ToolCapability.BLIND


def test_binding_explicit_queryable_no_mechanism_is_deferred_to_reconciler() -> None:
    """Explicit QUERYABLE on plain non-idempotent w/o provider key: floor stays
    BLIND, but effective_capability(has_reconciler=True) upgrades it."""
    binding = ToolTransitionBinding.for_tool(
        agent_id="demo",
        policy_version="1",
        side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
        capability=ToolCapability.QUERYABLE,
    )
    assert binding.capability == ToolCapability.BLIND
    assert binding.explicit_capability == ToolCapability.QUERYABLE
    assert (
        binding.effective_capability(has_reconciler=True)
        == ToolCapability.QUERYABLE
    )
    assert (
        binding.effective_capability(has_reconciler=False)
        == ToolCapability.BLIND
    )


def test_binding_explicit_queryable_on_irreversible_refused() -> None:
    with pytest.raises(ToolCapabilityDeclarationError):
        ToolTransitionBinding.for_tool(
            agent_id="demo",
            policy_version="1",
            side_effect_class=SideEffectClass.IRREVERSIBLE,
            capability=ToolCapability.QUERYABLE,
        )


# --- BLIND enforcement: never auto-retry an ambiguous entry -----------------


def _scope() -> TransitionScope:
    return TransitionScope(thread_id="t1", run_id="r1")


class _StubReconciler:
    def __init__(self, result: ReconcileResult) -> None:
        self._result = result
        self.calls: list[str] = []

    def reconcile(self, entry) -> ReconcileResult:
        self.calls.append(entry.request_id)
        return self._result


def test_blind_unknown_parks_and_never_retries() -> None:
    """BLIND tool: an UNKNOWN entry parks for reconciliation, body never re-runs
    even with a valid provider idempotency key + TTL."""
    storage = InMemoryLedgerStorage()
    binding = ToolTransitionBinding.for_tool(
        agent_id="demo",
        policy_version="1",
        side_effect_class=SideEffectClass.KEYED_MUTATE,
        provider_idempotency_key_param="idempotency_key",
        provider_idempotency_key_ttl=300.0,
        capability=ToolCapability.BLIND,  # declaration wins over key
    )
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

    assert attempts["n"] == 1  # body never re-executed

    # The unified EffectState (not just terminal_outcome) reports the row as
    # UNKNOWN, and a fresh claim attempt on the raw storage layer refuses to
    # overwrite it — the same fail-closed guarantee the hard-block above
    # already proved end-to-end, checked directly at the storage boundary.
    ledger_inst = get_ledger(send_payment)
    assert ledger_inst is not None
    with execution_scope(_scope()):
        request_id = ledger_inst.derive_request_id(
            "send_payment",
            (),
            {"amount": 10.0, "idempotency_key": "k1", "tool_call_id": "c1"},
            transition_binding=binding,
        )
    stored = ledger_inst.get(request_id)
    assert stored is not None
    assert stored.resolved_effect_state().value == "UNKNOWN"

    from dataclasses import replace as _replace

    fresh_claim = _replace(
        stored,
        terminal_outcome=TerminalOutcome.IN_FLIGHT.value,
        effect_phase="INTENDED",
        decision=None,
        fence=stored.fence + 1,
        owner="a-new-worker",
    )
    outcome, existing = storage.try_claim_inflight(fresh_claim)
    assert outcome == "in_flight"
    assert existing is not None
    assert existing.resolved_effect_state().value == "UNKNOWN"


def test_blind_park_still_releasable_by_operator() -> None:
    """Operator release still resolves a parked BLIND entry (NOT_EXECUTED)."""
    storage = InMemoryLedgerStorage()
    binding = ToolTransitionBinding.for_tool(
        agent_id="demo",
        policy_version="1",
        side_effect_class=SideEffectClass.KEYED_MUTATE,
        provider_idempotency_key_param="idempotency_key",
        provider_idempotency_key_ttl=300.0,
        capability=ToolCapability.BLIND,
    )
    attempts = {"n": 0}

    @ledger_sync(storage=storage, transition_binding=binding)
    def send_payment(amount: float, idempotency_key: str) -> dict[str, str]:
        attempts["n"] += 1
        with side_effect():
            if attempts["n"] == 1:
                raise RuntimeError("timeout after maybe-crossed")
            return {"status": "sent"}

    ledger_inst = get_ledger(send_payment)
    assert ledger_inst is not None

    with execution_scope(_scope()):
        with pytest.raises(RuntimeError):
            send_payment(amount=10.0, idempotency_key="k1", tool_call_id="c1")

        request_id = ledger_inst.derive_request_id(
            "send_payment",
            (),
            {"amount": 10.0, "idempotency_key": "k1", "tool_call_id": "c1"},
            transition_binding=binding,
        )

        with pytest.raises(LedgerHardBlockError):
            send_payment(amount=10.0, idempotency_key="k1", tool_call_id="c1")

        ledger_inst.release(
            request_id,
            verified="not_executed",
            by="ops@example.com",
            reason="manually verified no charge",
        )

        result = send_payment(amount=10.0, idempotency_key="k1", tool_call_id="c1")

    assert result == {"status": "sent"}
    assert attempts["n"] == 2


def test_blind_failed_before_effect_still_retries() -> None:
    """BLIND parking only applies to ambiguous entries: a clean
    FAILED_BEFORE_EFFECT (not_crossed) is unambiguous and retries safely."""
    storage = InMemoryLedgerStorage()
    binding = ToolTransitionBinding.for_tool(
        agent_id="demo",
        policy_version="1",
        side_effect_class=SideEffectClass.KEYED_MUTATE,
        provider_idempotency_key_param="idempotency_key",
        provider_idempotency_key_ttl=300.0,
        capability=ToolCapability.BLIND,
    )
    attempts = {"n": 0}

    @ledger_sync(storage=storage, transition_binding=binding)
    def send_payment(amount: float, idempotency_key: str) -> dict[str, str]:
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise RuntimeError("gateway rejected before charge")
        return {"status": "sent"}

    with execution_scope(_scope()):
        with pytest.raises(RuntimeError):
            send_payment(amount=10.0, idempotency_key="k1", tool_call_id="c1")

        result = send_payment(amount=10.0, idempotency_key="k1", tool_call_id="c1")

    assert attempts["n"] == 2
    assert result == {"status": "sent"}


# --- QUERYABLE + reconciler: probe resolves ATTEMPTING → COMMITTED/ABORTED --


def _queryable_binding() -> ToolTransitionBinding:
    return ToolTransitionBinding.for_tool(
        agent_id="demo",
        policy_version="1",
        side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
        capability=ToolCapability.QUERYABLE,
    )


def test_queryable_reconciler_probe_commits() -> None:
    """QUERYABLE tool + reconciler: ATTEMPTING → COMMITTED via probe EXECUTED."""
    storage = InMemoryLedgerStorage()
    reconciler = _StubReconciler(ReconcileResult.completed({"charged": True}))
    calls: list[float] = []

    @ledger_sync(
        storage=storage,
        transition_binding=_queryable_binding(),
        reconciler=reconciler,
    )
    def charge(amount: float) -> dict[str, bool]:
        calls.append(amount)
        with side_effect():
            record_external_operation("pi_1")
            raise RuntimeError("provider timeout")

    with execution_scope(_scope()):
        with pytest.raises(RuntimeError):
            charge(amount=10.0, tool_call_id="c1")

        result = charge(amount=10.0, tool_call_id="c1")

    assert result == {"charged": True}
    assert len(calls) == 1  # body not re-executed
    assert len(reconciler.calls) == 1
    entry = storage.get(reconciler.calls[0])
    assert entry.resolved_terminal_outcome() == TerminalOutcome.COMPLETED


def test_queryable_reconciler_probe_aborts_allows_single_reexec() -> None:
    """QUERYABLE tool + reconciler: ATTEMPTING → ABORTED via probe NOT_EXECUTED,
    which allows exactly one re-execution."""
    storage = InMemoryLedgerStorage()
    reconciler = _StubReconciler(ReconcileResult.not_executed())
    attempts = {"n": 0}

    @ledger_sync(
        storage=storage,
        transition_binding=_queryable_binding(),
        reconciler=reconciler,
    )
    def charge(amount: float) -> dict[str, bool]:
        attempts["n"] += 1
        with side_effect():
            if attempts["n"] == 1:
                record_external_operation("pi_1")
                raise RuntimeError("provider timeout")
            return {"charged": True}

    with execution_scope(_scope()):
        with pytest.raises(RuntimeError):
            charge(amount=10.0, tool_call_id="c1")

        result = charge(amount=10.0, tool_call_id="c1")

    assert result == {"charged": True}
    assert attempts["n"] == 2
    assert len(reconciler.calls) == 1


def test_queryable_without_reconciler_fails_closed_parks() -> None:
    """QUERYABLE declared but no reconciler → fail closed to BLIND parking; the
    ambiguous entry hard-blocks and the body never re-runs."""
    storage = InMemoryLedgerStorage()
    attempts = {"n": 0}

    @ledger_sync(storage=storage, transition_binding=_queryable_binding())
    def charge(amount: float) -> dict[str, bool]:
        attempts["n"] += 1
        with side_effect():
            record_external_operation("pi_1")
            raise RuntimeError("provider timeout")

    with execution_scope(_scope()):
        with pytest.raises(RuntimeError):
            charge(amount=10.0, tool_call_id="c1")

        with pytest.raises(LedgerHardBlockError):
            charge(amount=10.0, tool_call_id="c1")

    assert attempts["n"] == 1


def test_queryable_without_reconciler_warns_when_retry_would_otherwise_fire() -> None:
    """A QUERYABLE tool whose only would-be probe mechanism is a missing
    reconciler fails closed to BLIND parking *with a warning* on the path where
    the gate would otherwise permit a retry (provider key + valid TTL window)."""
    from mycelium import LedgerEntry, SideEffectBoundary
    from mycelium.action_ledger import ActionLedger

    ledger_inst = ActionLedger(storage=InMemoryLedgerStorage())
    binding = ToolTransitionBinding.for_tool(
        agent_id="demo",
        policy_version="1",
        side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
        capability=ToolCapability.QUERYABLE,
    )
    ambiguous = LedgerEntry(
        request_id="x",
        tool="charge",
        args=[],
        kwargs={},
        status="failed",
        terminal_outcome=TerminalOutcome.UNKNOWN.value,
        side_effect_boundary=SideEffectBoundary.MAYBE_CROSSED.value,
    )
    with pytest.warns(UserWarning, match="failing closed to blind"):
        parked = ledger_inst._blind_never_retries("charge", binding, ambiguous)
    assert parked is True


# --- backward compatibility -------------------------------------------------


def test_backward_compat_binding_without_capability() -> None:
    """A binding built without a capability derives the conservative default
    and does not change existing retry behavior for FAILED_BEFORE_EFFECT."""
    binding = ToolTransitionBinding.for_tool(
        agent_id="demo",
        policy_version="1",
        side_effect_class=SideEffectClass.IDEMPOTENT_MUTATE,
    )
    assert binding.capability == ToolCapability.IDEMPOTENT
    assert binding.explicit_capability is None


# --- YAML surface -----------------------------------------------------------


def test_config_parses_capability_key() -> None:
    from mycelium import load_config_from_string

    yaml_text = """
transition:
  agent_id: demo
  policy_version: "1"
tools:
  send_payment:
    side_effect_class: keyed_mutate
    provider_idempotency_key_param: idempotency_key
    capability: blind
"""
    config = load_config_from_string(yaml_text)
    tool_config = config.tools["send_payment"]
    assert tool_config.capability == ToolCapability.BLIND
    binding = config.tool_transition_binding(tool_config)
    assert binding is not None
    assert binding.capability == ToolCapability.BLIND


def test_config_capability_omitted_derives_default() -> None:
    from mycelium import load_config_from_string

    yaml_text = """
transition:
  agent_id: demo
  policy_version: "1"
tools:
  send_payment:
    side_effect_class: keyed_mutate
    provider_idempotency_key_param: idempotency_key
"""
    config = load_config_from_string(yaml_text)
    tool_config = config.tools["send_payment"]
    assert tool_config.capability is None
    binding = config.tool_transition_binding(tool_config)
    assert binding is not None
    assert binding.capability == ToolCapability.QUERYABLE


def test_config_rejects_invalid_capability() -> None:
    from mycelium import ConfigError, load_config_from_string

    yaml_text = """
transition:
  agent_id: demo
  policy_version: "1"
tools:
  send_payment:
    side_effect_class: keyed_mutate
    capability: teleport
"""
    with pytest.raises(ConfigError, match="invalid capability"):
        load_config_from_string(yaml_text)
