"""Deterministic effect identity: destination-aware transition keys."""

from __future__ import annotations

import threading
import time

import pytest

from mycelium import (
    ActionLedger,
    InMemoryLedgerStorage,
    LedgerEntry,
    SideEffectClass,
    ToolTransitionBinding,
    TransitionScope,
    apply_entity_guard,
    derive_transition_key_for_call,
    enforce_entity_guard,
    execution_scope,
    get_ledger,
    ledger_sync,
)
from mycelium.entity_guard import (
    DEST_EMAIL,
    ApprovedDestination,
    DestinationAllow,
    DestinationSpec,
    EntityDecision,
    EntityGuardPolicy,
    ToolDestinationPolicy,
    reset_entity_guard_state,
)
from mycelium.transition import (
    build_transition_preimage,
    derive_effect_id,
    derive_effect_id_for_call,
)
from mycelium.verify.invariants import (
    check_at_most_one_committed_effect_state,
    check_no_duplicate_effect_ids,
)


@pytest.fixture(autouse=True)
def _reset_entity_guard_state() -> None:
    reset_entity_guard_state()
    yield
    reset_entity_guard_state()


def _binding() -> ToolTransitionBinding:
    return ToolTransitionBinding.for_tool(
        agent_id="payment-agent",
        policy_version="2026.07.1",
        side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
    )


def _email_policy() -> EntityGuardPolicy:
    return EntityGuardPolicy(
        enabled=True,
        missing_policy="error",
        policy_version="test",
        tools={
            "send_email": ToolDestinationPolicy(
                destinations=(
                    DestinationSpec(
                        path="recipient",
                        dest_type=DEST_EMAIL,
                        allow=DestinationAllow(
                            addresses=frozenset(
                                {
                                    "billing@customer.com",
                                    "ops@customer.com",
                                }
                            ),
                            domains=frozenset({"customer.com"}),
                        ),
                    ),
                )
            ),
        },
    )


def test_effect_id_aliases_exported_from_package_root() -> None:
    from mycelium import derive_effect_id as root_effect_id
    from mycelium import derive_effect_id_for_call as root_effect_id_for_call

    assert root_effect_id is derive_effect_id
    assert root_effect_id_for_call is derive_effect_id_for_call


def test_v2_preimage_includes_empty_destination_by_default() -> None:
    preimage = build_transition_preimage(
        scope=TransitionScope(thread_id="t1", run_id="r1", node="pay"),
        dispatch_id="call_abc",
        tool="send_payment",
        args=(100.0,),
        kwargs={"recipient": "acct_1"},
        side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
        agent_id="payment-agent",
        policy_version="2026.07.1",
    )
    assert preimage["destination"] == []


def test_effect_id_aliases_transition_key() -> None:
    preimage = build_transition_preimage(
        scope=TransitionScope(thread_id="t1", run_id="r1"),
        dispatch_id="call_1",
        tool="send_payment",
        args=(),
        kwargs={"amount": 10},
        side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
        agent_id="agent",
        policy_version="1",
        destination=("email:billing@customer.com",),
    )
    assert derive_effect_id(preimage) == derive_effect_id(preimage)
    assert len(derive_effect_id(preimage)) == 64


def test_different_destination_produces_different_effect_id() -> None:
    binding = _binding()
    base = {"amount": 10.0, "tool_call_id": "call_1"}
    _, kwargs_a, _ = enforce_entity_guard(
        "send_email",
        (),
        {"recipient": "billing@customer.com", **base},
        policy=_email_policy(),
    )
    _, kwargs_b, _ = enforce_entity_guard(
        "send_email",
        (),
        {"recipient": "ops@customer.com", **base},
        policy=_email_policy(),
    )
    with execution_scope(TransitionScope(thread_id="thread-1", run_id="run-1")):
        key_a = derive_effect_id_for_call("send_email", (), kwargs_a, binding)
        key_b = derive_effect_id_for_call("send_email", (), kwargs_b, binding)
    assert key_a != key_b


def test_entity_guard_email_canonicalization_stabilizes_effect_id() -> None:
    binding = _binding()
    base = {"amount": 10.0, "tool_call_id": "call_1"}
    _, kwargs_lower, _ = enforce_entity_guard(
        "send_email",
        (),
        {"recipient": "billing@customer.com", **base},
        policy=_email_policy(),
    )
    _, kwargs_upper, _ = enforce_entity_guard(
        "send_email",
        (),
        {"recipient": "Billing@Customer.COM", **base},
        policy=_email_policy(),
    )
    with execution_scope(TransitionScope(thread_id="thread-1", run_id="run-1")):
        key_lower = derive_effect_id_for_call(
            "send_email", (), kwargs_lower, binding
        )
        key_upper = derive_effect_id_for_call(
            "send_email", (), kwargs_upper, binding
        )
    assert key_lower == key_upper


def test_same_destination_redispatch_reuses_effect_id() -> None:
    binding = _binding()
    executions: list[str] = []

    @ledger_sync(storage=InMemoryLedgerStorage(), transition_binding=binding)
    def send_email(recipient: str, amount: float) -> dict[str, str]:
        executions.append(recipient)
        return {"recipient": recipient, "status": "sent"}

    wrapped = apply_entity_guard(send_email, _email_policy(), tool_name="send_email")
    kwargs = {
        "recipient": "billing@customer.com",
        "amount": 10.0,
        "tool_call_id": "call_email_1",
    }

    with execution_scope(TransitionScope(thread_id="thread-1", run_id="run-1")):
        r1 = wrapped(**kwargs)
        r2 = wrapped(**kwargs)

    assert len(executions) == 1
    assert r1 == r2


def test_effect_id_for_call_matches_transition_key_for_call() -> None:
    binding = _binding()
    kwargs = {"amount": 10.0, "tool_call_id": "call_1"}
    with execution_scope(TransitionScope(thread_id="thread-1", run_id="run-1")):
        assert derive_effect_id_for_call("send_payment", (), kwargs, binding) == (
            derive_transition_key_for_call("send_payment", (), kwargs, binding)
        )


def test_claimed_entry_stores_effect_id_field() -> None:
    """LedgerEntry.effect_id is populated on claim and, for the default
    derived-request_id path, equals request_id (derive_request_id's fallback
    is the same derivation as derive_effect_id_for_call)."""
    binding = _binding()

    @ledger_sync(storage=InMemoryLedgerStorage(), transition_binding=binding)
    def send_payment(amount: float) -> dict[str, str]:
        return {"status": "sent"}

    with execution_scope(TransitionScope(thread_id="thread-1", run_id="run-1")):
        send_payment(amount=10.0, tool_call_id="call_effect_id_1")

    ledger_inst = get_ledger(send_payment)
    assert ledger_inst is not None
    with execution_scope(TransitionScope(thread_id="thread-1", run_id="run-1")):
        expected_request_id = ledger_inst.derive_request_id(
            "send_payment", (), {"amount": 10.0, "tool_call_id": "call_effect_id_1"},
            transition_binding=binding,
        )
    stored = ledger_inst.get(expected_request_id)
    assert stored is not None
    assert stored.effect_id is not None
    assert stored.effect_id == stored.request_id


def test_unclassified_claim_effect_id_falls_back_to_request_id() -> None:
    """No binding to derive from (legacy claim()) still gets a non-null
    effect_id — it falls back to request_id, same as idempotency_key."""
    ledger_inst = ActionLedger(storage=InMemoryLedgerStorage())
    entry = ledger_inst.claim("legacy-req-1", "legacy_tool", (), {})
    assert entry.effect_id == "legacy-req-1"


def test_legacy_row_without_effect_id_field_infers_from_request_id() -> None:
    """from_dict on a pre-schema-2 row (no effect_id key at all) infers it."""
    raw = {
        "request_id": "legacy-row-1",
        "tool": "legacy_tool",
        "args": [],
        "kwargs": {},
        "status": "completed",
        "terminal_outcome": "COMPLETED",
    }
    assert "effect_id" not in raw
    assert "schema_version" not in raw
    entry = LedgerEntry.from_dict(raw)
    assert entry.effect_id == "legacy-row-1"
    assert entry.schema_version == 1


def test_concurrent_workers_same_call_collide_on_one_row() -> None:
    """Two concurrent workers deriving the same identity from the same
    (tool, params, destination) — no explicit request_id — collide on the
    same durable row: the provider body runs exactly once, and the unified
    EffectState at-most-one-COMMITTED invariant holds across both workers'
    observed entries."""
    storage = InMemoryLedgerStorage()
    binding = _binding()
    executions: list[str] = []

    @ledger_sync(storage=storage, transition_binding=binding)
    def send_email(recipient: str, amount: float) -> dict[str, str]:
        executions.append(recipient)
        time.sleep(0.05)
        return {"recipient": recipient, "status": "sent"}

    wrapped = apply_entity_guard(send_email, _email_policy(), tool_name="send_email")
    kwargs = {
        "recipient": "billing@customer.com",
        "amount": 10.0,
        "tool_call_id": "call_concurrent_1",
    }

    results: list[dict[str, str]] = []
    errors: list[BaseException] = []

    def _worker() -> None:
        with execution_scope(TransitionScope(thread_id="thread-1", run_id="run-1")):
            try:
                results.append(wrapped(**kwargs))
            except BaseException as exc:  # noqa: BLE001 — surfaced via assertion below
                errors.append(exc)

    t1 = threading.Thread(target=_worker, daemon=True)
    t2 = threading.Thread(target=_worker, daemon=True)
    t1.start()
    t2.start()
    t1.join(timeout=10)
    t2.join(timeout=10)

    assert not errors, f"unexpected worker errors: {errors}"
    assert len(executions) == 1, "the provider body must run exactly once"
    assert len(results) == 2
    assert results[0] == results[1]

    entries = storage.list_all()
    assert len(entries) == 1
    assert check_at_most_one_committed_effect_state(entries) == []
    assert check_no_duplicate_effect_ids(entries) == []
    assert entries[0].resolved_effect_state().value == "COMMITTED"


def test_explicit_request_id_redispatch_collides_on_effect_id_row() -> None:
    storage = InMemoryLedgerStorage()
    binding = _binding()
    ledger_inst = ActionLedger(storage=storage)
    kwargs = {
        "recipient": "billing@customer.com",
        "amount": 10.0,
        "tool_call_id": "call_effect_collision_1",
    }

    with execution_scope(TransitionScope(thread_id="thread-1", run_id="run-1")):
        first = ledger_inst.claim_side_effecting(
            "audit-req-1",
            "send_email",
            (),
            kwargs,
            binding,
        )
        ledger_inst.record_decision(
            first.request_id,
            {"allowed": True, "verdicts": [], "denied_reasons": []},
            expected_owner=first.owner,
            expected_fence=first.fence,
        )
        ledger_inst.complete(first.request_id, {"ok": True}, expected_fence=first.fence)
        replay = ledger_inst.claim_side_effecting(
            "audit-req-2",
            "send_email",
            (),
            kwargs,
            binding,
        )

    assert replay.request_id == "audit-req-1"
    assert replay.result == {"ok": True}
    assert len(storage.list_all()) == 1


def test_explicit_destination_in_preimage() -> None:
    decision = EntityDecision(
        tool="send_email",
        destinations=(
            ApprovedDestination(
                path="recipient",
                dest_class=DEST_EMAIL,
                entity="billing@customer.com",
            ),
        ),
        policy_version="test",
        decision="allow",
    )
    preimage = build_transition_preimage(
        scope=TransitionScope(thread_id="t1", run_id="r1"),
        dispatch_id="call_1",
        tool="send_email",
        args=(),
        kwargs={"recipient": "billing@customer.com"},
        side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
        agent_id="agent",
        policy_version="1",
        destination=("email:billing@customer.com",),
    )
    assert preimage["destination"] == ["email:billing@customer.com"]
    del decision
