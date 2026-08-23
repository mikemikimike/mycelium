"""Authoritative effect_id index and cross-request dedupe checks."""

from __future__ import annotations

import json
import threading
import time
from dataclasses import replace
from pathlib import Path

import pytest

from mycelium import (
    ActionLedger,
    FileLedgerStorage,
    InMemoryLedgerStorage,
    LedgerEntry,
    SideEffectClass,
    SqliteLedgerStorage,
    ToolTransitionBinding,
    TransitionScope,
    execution_scope,
)
from mycelium.verify.invariants import check_unique_effect_id_index


def _binding() -> ToolTransitionBinding:
    return ToolTransitionBinding.for_tool(
        agent_id="effect-id-tests",
        policy_version="1",
        side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
    )


def _decision(allowed: bool) -> dict[str, object]:
    return {"allowed": allowed, "verdicts": [], "denied_reasons": []}


def _exercise_explicit_alias(
    ledger: ActionLedger,
    *,
    first_request_id: str,
    second_request_id: str,
) -> tuple[LedgerEntry, str]:
    binding = _binding()
    kwargs_a = {"amount": 10.0, "tool_call_id": "call-1", "request_id": first_request_id}
    kwargs_b = {"amount": 10.0, "tool_call_id": "call-1", "request_id": second_request_id}
    with execution_scope(TransitionScope(thread_id="t1", run_id="r1")):
        first = ledger.claim_side_effecting(first_request_id, "charge", (), dict(kwargs_a), binding)
        ledger.record_decision(
            first_request_id,
            _decision(True),
            expected_owner=first.owner,
            expected_fence=first.fence,
        )
        ledger.complete(
            first_request_id,
            {"charged": True},
            _expected_owner=first.owner,
            _expected_fence=first.fence,
        )
        replay = ledger.claim_side_effecting(
            second_request_id,
            "charge",
            (),
            dict(kwargs_b),
            binding,
        )
    return replay, str(first.effect_id)


def test_inmemory_effect_id_resolves_to_canonical_request() -> None:
    storage = InMemoryLedgerStorage()
    ledger = ActionLedger(storage=storage)
    replay, effect_id = _exercise_explicit_alias(
        ledger,
        first_request_id="req-a",
        second_request_id="req-b",
    )

    assert replay.request_id == "req-a"
    assert replay.is_terminal_completed()
    assert "req-b" in replay.request_id_aliases
    assert storage.resolve_request_id(effect_id) == "req-a"
    by_effect = storage.get_by_effect_id(effect_id)
    assert by_effect is not None
    assert by_effect.request_id == "req-a"
    assert len(storage.list_all()) == 1


def test_inmemory_effect_index_remains_complete_for_unique_rows_and_misses() -> None:
    storage = InMemoryLedgerStorage()
    entries = [
        LedgerEntry(
            request_id=f"request-{index}",
            effect_id=f"effect-{index}",
            tool="charge",
            args=[index],
            kwargs={"amount": index},
            status="completed",
            terminal_outcome="completed",
            started_at=float(index),
        )
        for index in range(512)
    ]
    for entry in entries:
        storage.set(entry)

    assert storage.list_all() == entries
    assert [
        storage.get_by_effect_id(f"effect-{index}") for index in range(len(entries))
    ] == entries
    assert storage.get_by_effect_id("missing-effect") is None
    assert storage.resolve_request_id("missing-effect") is None
    assert storage.list_all() == entries


def test_inmemory_effect_index_rejects_duplicates_and_repairs_stale_keys() -> None:
    storage = InMemoryLedgerStorage()
    canonical = LedgerEntry(
        request_id="canonical",
        effect_id="shared-effect",
        tool="charge",
        args=[],
        kwargs={},
        status="completed",
        terminal_outcome="completed",
        started_at=2.0,
    )
    duplicate = replace(canonical, request_id="duplicate", started_at=1.0)
    storage.set(canonical)
    storage.set(duplicate)

    assert storage.list_all() == [canonical]
    assert storage.get_by_effect_id("shared-effect") == canonical

    replacement = replace(canonical, effect_id="replacement-effect")
    storage.set(replacement)
    assert storage.resolve_request_id("shared-effect") is None
    assert storage.get_by_effect_id("replacement-effect") == replacement
    assert storage.list_all() == [replacement]


def test_file_storage_repairs_missing_sidecar_index(tmp_path: Path) -> None:
    path = tmp_path / "ledger.json"
    storage = FileLedgerStorage(path)
    ledger = ActionLedger(storage=storage)
    replay, effect_id = _exercise_explicit_alias(
        ledger,
        first_request_id="file-a",
        second_request_id="file-b",
    )
    assert replay.request_id == "file-a"

    index_path = path.with_suffix(path.suffix + ".effect-index.json")
    assert index_path.exists()
    index_path.unlink()
    assert storage.resolve_request_id(effect_id) == "file-a"
    assert index_path.exists()


def test_file_storage_legacy_schema_row_without_effect_id_repairs_index(tmp_path: Path) -> None:
    path = tmp_path / "legacy-ledger.json"
    raw = {
        "legacy-row-1": {
            "request_id": "legacy-row-1",
            "tool": "charge",
            "args": [],
            "kwargs": {"request_id": "legacy-row-1"},
            "status": "completed",
            "terminal_outcome": "completed",
            "started_at": 1.0,
            "schema_version": 1,
        }
    }
    path.write_text(json.dumps(raw), encoding="utf-8")
    storage = FileLedgerStorage(path)
    index_path = path.with_suffix(path.suffix + ".effect-index.json")
    assert not index_path.exists()
    assert storage.resolve_request_id("legacy-row-1") == "legacy-row-1"
    by_effect = storage.get_by_effect_id("legacy-row-1")
    assert by_effect is not None
    assert by_effect.request_id == "legacy-row-1"
    assert index_path.exists()


def test_sqlite_storage_effect_id_lookup(tmp_path: Path) -> None:
    storage = SqliteLedgerStorage(tmp_path / "ledger.db")
    ledger = ActionLedger(storage=storage)
    replay, effect_id = _exercise_explicit_alias(
        ledger,
        first_request_id="sqlite-a",
        second_request_id="sqlite-b",
    )
    assert replay.request_id == "sqlite-a"
    assert storage.resolve_request_id(effect_id) == "sqlite-a"
    row = storage.get_by_effect_id(effect_id)
    assert row is not None
    assert row.request_id == "sqlite-a"


def test_redis_storage_effect_id_lookup(monkeypatch) -> None:
    fakeredis = pytest.importorskip("fakeredis")
    fake = fakeredis.FakeRedis(decode_responses=True)

    def from_url(*args: object, **kwargs: object):
        return fake

    redis = pytest.importorskip("redis")

    monkeypatch.setattr(redis.Redis, "from_url", from_url)

    from mycelium import RedisLedgerStorage

    storage = RedisLedgerStorage("redis://test")
    ledger = ActionLedger(storage=storage)
    replay, effect_id = _exercise_explicit_alias(
        ledger,
        first_request_id="redis-a",
        second_request_id="redis-b",
    )
    assert replay.request_id == "redis-a"
    assert storage.resolve_request_id(effect_id) == "redis-a"
    row = storage.get_by_effect_id(effect_id)
    assert row is not None
    assert row.request_id == "redis-a"


def test_redis_two_worker_mismatched_request_ids_single_effect(monkeypatch) -> None:
    fakeredis = pytest.importorskip("fakeredis")
    fake = fakeredis.FakeRedis(decode_responses=True)

    def from_url(*args: object, **kwargs: object):
        return fake

    redis = pytest.importorskip("redis")
    monkeypatch.setattr(redis.Redis, "from_url", from_url)

    from mycelium import RedisLedgerStorage, ledger_sync

    storage = RedisLedgerStorage("redis://test")
    binding = _binding()
    started = threading.Event()
    executions: list[str] = []
    results: dict[str, dict[str, bool]] = {}
    errors: list[BaseException] = []

    @ledger_sync(
        storage=storage,
        transition_binding=binding,
        poll_interval=0.001,
        poll_timeout=2.0,
    )
    def charge(amount: float) -> dict[str, bool]:
        executions.append("run")
        started.set()
        time.sleep(0.05)
        return {"charged": True}

    def _worker(name: str, request_id: str) -> None:
        try:
            with execution_scope(TransitionScope(thread_id="redis-two-worker", run_id="r1")):
                results[name] = charge(
                    amount=10.0,
                    request_id=request_id,
                    tool_call_id="redis-two-worker-call",
                )
        except BaseException as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    first = threading.Thread(target=_worker, args=("a", "order-1"), daemon=True)
    second = threading.Thread(target=_worker, args=("b", "order-2"), daemon=True)

    first.start()
    assert started.wait(timeout=2.0)
    second.start()
    first.join(timeout=5.0)
    second.join(timeout=5.0)

    assert not first.is_alive()
    assert not second.is_alive()
    assert not errors
    assert results["a"] == {"charged": True}
    assert results["b"] == {"charged": True}
    assert len(executions) == 1
    entries = storage.list_all()
    assert len(entries) == 1
    assert entries[0].request_id == "order-1"
    assert "order-2" in entries[0].request_id_aliases


def test_unique_effect_id_invariant_reports_duplicate_rows() -> None:
    entries = [
        LedgerEntry(
            request_id="dup-a",
            tool="charge",
            args=[],
            kwargs={},
            status="completed",
            effect_id="same-effect",
        ),
        LedgerEntry(
            request_id="dup-b",
            tool="charge",
            args=[],
            kwargs={},
            status="completed",
            effect_id="same-effect",
        ),
    ]
    violations = check_unique_effect_id_index(entries)
    assert len(violations) == 1
    assert "multiple request_ids" in violations[0].message
