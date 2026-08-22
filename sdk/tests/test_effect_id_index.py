"""Effect-id secondary index behavior across claim paths."""

from __future__ import annotations

import time
from dataclasses import replace
from pathlib import Path

import pytest

from mycelium import (
    ActionLedger,
    FileLedgerStorage,
    InMemoryLedgerStorage,
    LedgerHardBlockError,
    SideEffectBoundary,
    SideEffectClass,
    TerminalOutcome,
    ToolTransitionBinding,
    TransitionScope,
    execution_scope,
)


def _binding() -> ToolTransitionBinding:
    return ToolTransitionBinding.for_tool(
        agent_id="effect-index-tests",
        policy_version="1",
        side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
    )


def test_identical_effect_different_explicit_request_ids_reuses_single_row() -> None:
    storage = InMemoryLedgerStorage()
    ledger = ActionLedger(storage=storage)
    binding = _binding()
    kwargs = {"amount": 10.0, "recipient": "acct_1", "tool_call_id": "call_same_effect_1"}

    with execution_scope(TransitionScope(thread_id="t1", run_id="r1")):
        first = ledger.claim_side_effecting("audit-1", "send_payment", (), kwargs, binding)
        ledger.record_decision(
            first.request_id,
            {"allowed": True, "verdicts": [], "denied_reasons": []},
            expected_owner=first.owner,
            expected_fence=first.fence,
        )
        ledger.complete(first.request_id, {"charged": True}, expected_fence=first.fence)
        second = ledger.claim_side_effecting("audit-2", "send_payment", (), kwargs, binding)

    assert second.request_id == first.request_id
    assert second.result == {"charged": True}
    assert len(storage.list_all()) == 1


def test_redispatch_after_crash_collides_on_effect_id_and_does_not_create_second_row() -> None:
    storage = InMemoryLedgerStorage()
    ledger = ActionLedger(storage=storage, lease_ttl=0.05, poll_timeout=0.2, poll_interval=0.01)
    binding = _binding()
    kwargs = {"amount": 10.0, "recipient": "acct_2", "tool_call_id": "call_crash_effect_1"}

    with execution_scope(TransitionScope(thread_id="t1", run_id="r1")):
        first = ledger.claim_side_effecting("audit-crash-1", "send_payment", (), kwargs, binding)
        ledger.record_decision(
            first.request_id,
            {"allowed": True, "verdicts": [], "denied_reasons": []},
            expected_owner=first.owner,
            expected_fence=first.fence,
        )
        ledger.advance_boundary(
            first.request_id,
            SideEffectBoundary.MAYBE_CROSSED,
            expected_owner=first.owner,
            expected_fence=first.fence,
        )
        current = ledger.get(first.request_id)
        assert current is not None
        expired = replace(current, lease_until=time.time() - 1)
        storage.set(expired)
        with pytest.raises(LedgerHardBlockError):
            ledger.claim_side_effecting("audit-crash-2", "send_payment", (), kwargs, binding)

    rows = storage.list_all()
    assert len(rows) == 1
    assert rows[0].request_id == "audit-crash-1"


def test_legacy_schema1_row_get_by_effect_id_lazy_indexes(tmp_path: Path) -> None:
    storage = FileLedgerStorage(tmp_path / "legacy-ledger.json")

    def _seed_legacy(data: dict[str, dict[str, object]]) -> None:
        data["legacy-req-1"] = {
            "request_id": "legacy-req-1",
            "tool": "send_payment",
            "args": [],
            "kwargs": {"amount": 10.0, "recipient": "acct_legacy", "tool_call_id": "legacy_call_1"},
            "status": "completed",
            "terminal_outcome": TerminalOutcome.COMPLETED.value,
            "result": {"charged": True},
        }

    storage._file.read_modify_write(_seed_legacy)
    loaded = storage.get_by_effect_id("legacy-req-1")
    assert loaded is not None
    assert loaded.request_id == "legacy-req-1"
    assert loaded.effect_id == "legacy-req-1"


def test_unclassified_claim_path_unchanged() -> None:
    storage = InMemoryLedgerStorage()
    ledger = ActionLedger(storage=storage)

    first = ledger.claim("legacy-1", "legacy_tool", (), {"amount": 1})
    second = ledger.claim("legacy-2", "legacy_tool", (), {"amount": 1})

    assert first.request_id == "legacy-1"
    assert second.request_id == "legacy-2"
    assert first.effect_id == "legacy-1"
    assert second.effect_id == "legacy-2"
    assert len(storage.list_all()) == 2
