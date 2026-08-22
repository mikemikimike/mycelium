"""Effect-id to provider-key propagation for keyed side-effect tools."""

from __future__ import annotations

import pytest

from mycelium import (
    InMemoryLedgerStorage,
    SideEffectClass,
    ToolTransitionBinding,
    TransitionScope,
    derive_effect_id_for_call,
    execution_scope,
    get_ledger,
    ledger_sync,
)


def _scope() -> TransitionScope:
    return TransitionScope(thread_id="t-prop", run_id="r-prop")


def _binding(*, propagate: bool = True) -> ToolTransitionBinding:
    return ToolTransitionBinding.for_tool(
        agent_id="demo",
        policy_version="1",
        side_effect_class=SideEffectClass.KEYED_MUTATE,
        provider_idempotency_key_param="idempotency_key",
        propagate_effect_id_as_provider_key=propagate,
    )


def test_keyed_mutate_injects_effect_key_and_stamps_ledger() -> None:
    seen_keys: list[str | None] = []
    binding = _binding(propagate=True)

    @ledger_sync(storage=InMemoryLedgerStorage(), transition_binding=binding)
    def send_payment(amount: float, idempotency_key: str | None = None) -> dict[str, str | None]:
        seen_keys.append(idempotency_key)
        return {"status": "sent", "key": idempotency_key}

    with execution_scope(_scope()):
        send_payment(amount=10.0, tool_call_id="call-prop-1")

    ledger_inst = get_ledger(send_payment)
    assert ledger_inst is not None
    with execution_scope(_scope()):
        request_id = ledger_inst.derive_request_id(
            "send_payment",
            (),
            {"amount": 10.0, "tool_call_id": "call-prop-1"},
            transition_binding=binding,
        )
    entry = ledger_inst.get(request_id)
    assert entry is not None
    assert entry.effect_id is not None
    expected = f"mycelium:{entry.effect_id}"
    assert entry.provider_idempotency_key == expected
    assert seen_keys == [expected]


def test_retry_without_kwarg_reuses_same_stored_provider_key() -> None:
    seen_keys: list[str | None] = []
    attempts = {"n": 0}
    binding = _binding(propagate=True)

    @ledger_sync(storage=InMemoryLedgerStorage(), transition_binding=binding)
    def send_payment(amount: float, idempotency_key: str | None = None) -> dict[str, str]:
        seen_keys.append(idempotency_key)
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise RuntimeError("timeout")
        return {"status": "sent"}

    with execution_scope(_scope()):
        with pytest.raises(RuntimeError):
            send_payment(amount=10.0, tool_call_id="call-prop-2")
        send_payment(amount=10.0, tool_call_id="call-prop-2")

    assert len(seen_keys) == 2
    assert seen_keys[0] is not None
    assert seen_keys[0] == seen_keys[1]


def test_explicit_provider_kwarg_overrides_propagation() -> None:
    seen_keys: list[str | None] = []
    binding = _binding(propagate=True)

    @ledger_sync(storage=InMemoryLedgerStorage(), transition_binding=binding)
    def send_payment(amount: float, idempotency_key: str | None = None) -> dict[str, str | None]:
        seen_keys.append(idempotency_key)
        return {"status": "sent", "key": idempotency_key}

    with execution_scope(_scope()):
        send_payment(amount=10.0, idempotency_key="host-key", tool_call_id="call-prop-3")

    ledger_inst = get_ledger(send_payment)
    assert ledger_inst is not None
    with execution_scope(_scope()):
        request_id = ledger_inst.derive_request_id(
            "send_payment",
            (),
            {
                "amount": 10.0,
                "idempotency_key": "host-key",
                "tool_call_id": "call-prop-3",
            },
            transition_binding=binding,
        )
    entry = ledger_inst.get(request_id)
    assert entry is not None
    assert entry.provider_idempotency_key == "host-key"
    assert seen_keys == ["host-key"]


def test_no_injection_when_flag_disabled() -> None:
    seen_keys: list[str | None] = []
    binding = _binding(propagate=False)

    @ledger_sync(storage=InMemoryLedgerStorage(), transition_binding=binding)
    def send_payment(amount: float, idempotency_key: str | None = None) -> dict[str, str | None]:
        seen_keys.append(idempotency_key)
        return {"status": "sent", "key": idempotency_key}

    with execution_scope(_scope()):
        send_payment(amount=10.0, tool_call_id="call-prop-4")

    ledger_inst = get_ledger(send_payment)
    assert ledger_inst is not None
    with execution_scope(_scope()):
        request_id = ledger_inst.derive_request_id(
            "send_payment",
            (),
            {"amount": 10.0, "tool_call_id": "call-prop-4"},
            transition_binding=binding,
        )
    entry = ledger_inst.get(request_id)
    assert entry is not None
    assert entry.provider_idempotency_key is None
    assert seen_keys == [None]


def test_effect_identity_stable_between_injected_and_explicit_provider_key() -> None:
    binding = _binding(propagate=True)
    with execution_scope(_scope()):
        injected_identity = derive_effect_id_for_call(
            "send_payment",
            (),
            {"amount": 10.0, "tool_call_id": "call-prop-5"},
            binding,
        )
        explicit_identity = derive_effect_id_for_call(
            "send_payment",
            (),
            {
                "amount": 10.0,
                "idempotency_key": "host-key",
                "tool_call_id": "call-prop-5",
            },
            binding,
        )
    assert injected_identity == explicit_identity


def test_retry_identity_stable_between_injected_and_explicit_same_key() -> None:
    seen_keys: list[str | None] = []
    attempts = {"n": 0}
    binding = _binding(propagate=True)

    @ledger_sync(storage=InMemoryLedgerStorage(), transition_binding=binding)
    def send_payment(amount: float, idempotency_key: str | None = None) -> dict[str, str]:
        seen_keys.append(idempotency_key)
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise RuntimeError("timeout")
        return {"status": "sent"}

    with execution_scope(_scope()):
        with pytest.raises(RuntimeError):
            send_payment(amount=10.0, tool_call_id="call-prop-6")
        assert seen_keys[0] is not None
        send_payment(
            amount=10.0,
            idempotency_key=seen_keys[0],
            tool_call_id="call-prop-6",
        )

    assert attempts["n"] == 2
