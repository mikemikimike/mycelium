"""Ledger schema migration API and CLI coverage."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from mycelium import (
    InMemoryLedgerStorage,
    LedgerEntry,
    LedgerMigrationError,
    LedgerSchemaVersionError,
    SqliteLedgerStorage,
    TerminalOutcome,
    apply_ledger_migration,
    inspect_ledger_schema_versions,
    plan_ledger_migration,
)
from mycelium.__main__ import main


def _entry(request_id: str, *, version: int, outcome: str = "COMPLETED") -> LedgerEntry:
    return LedgerEntry(
        request_id=request_id,
        tool="send_email",
        args=[],
        kwargs={},
        status="completed" if outcome == "COMPLETED" else "in-flight",
        terminal_outcome=outcome,
        schema_version=version,
    )


def test_plan_and_apply_v1_to_v2_are_explicit_and_idempotent() -> None:
    storage = InMemoryLedgerStorage()
    storage.set(_entry("legacy-1", version=1))
    storage.set(_entry("current-2", version=2))

    plan = plan_ledger_migration(storage)
    assert plan.total_entries == 2
    assert plan.pending_entries == 1
    assert plan.current_entries == 1
    assert plan.version_counts == {1: 1, 2: 1}

    result = apply_ledger_migration(storage)
    assert result.migrated_entries == 1
    assert result.unchanged_entries == 1
    migrated = storage.get("legacy-1")
    assert migrated is not None
    assert migrated.schema_version == 2
    assert migrated.effect_id == "legacy-1"
    assert migrated.request_id_aliases == ("legacy-1",)

    again = apply_ledger_migration(storage)
    assert again.migrated_entries == 0
    assert again.unchanged_entries == 2


def test_future_schema_and_downgrade_fail_closed() -> None:
    storage = InMemoryLedgerStorage()
    storage.set(_entry("future", version=3))
    plan = plan_ledger_migration(storage)
    assert plan.unsupported_versions == (3,)
    assert not plan.can_apply
    with pytest.raises(LedgerMigrationError, match="unsupported schema"):
        apply_ledger_migration(storage)
    with pytest.raises(LedgerMigrationError, match="downgrades are not supported"):
        plan_ledger_migration(storage, target_version=1)


def test_runtime_reader_accepts_legacy_and_rejects_invalid_or_future_versions() -> None:
    raw = _entry("version-check", version=2).to_dict()
    raw.pop("schema_version")
    legacy = LedgerEntry.from_dict(raw)
    assert legacy.schema_version == 1
    assert legacy.effect_id == "version-check"

    for invalid in (True, 0, 1.5, "not-an-integer"):
        raw["schema_version"] = invalid
        with pytest.raises(LedgerSchemaVersionError, match="schema_version"):
            LedgerEntry.from_dict(raw)

    raw["schema_version"] = 3
    with pytest.raises(LedgerSchemaVersionError, match="newer than this runtime"):
        LedgerEntry.from_dict(raw)


def test_active_v1_requires_explicit_override() -> None:
    storage = InMemoryLedgerStorage()
    storage.set(_entry("active", version=1, outcome=TerminalOutcome.IN_FLIGHT.value))
    plan = plan_ledger_migration(storage)
    assert plan.active_pending_entries == 1
    with pytest.raises(LedgerMigrationError, match="IN_FLIGHT"):
        apply_ledger_migration(storage)
    result = apply_ledger_migration(storage, allow_active=True)
    assert result.migrated_entries == 1


def _write_legacy_file(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "legacy-cli": {
                    "request_id": "legacy-cli",
                    "tool": "send_email",
                    "args": [],
                    "kwargs": {},
                    "status": "completed",
                    "terminal_outcome": "COMPLETED",
                }
            }
        ),
        encoding="utf-8",
    )


def test_cli_file_plan_apply_and_verify(tmp_path: Path, capsys) -> None:
    ledger_path = tmp_path / "ledger.json"
    _write_legacy_file(ledger_path)

    assert main(["migrate", "--plan", "--file", str(ledger_path)]) == 0
    planned = capsys.readouterr().out
    assert "migrate=1" in planned
    assert "No ledger rows were changed" in planned
    assert "schema_version" not in ledger_path.read_text(encoding="utf-8")

    assert main(["migrate", "--apply", "--file", str(ledger_path)]) == 0
    applied = capsys.readouterr().out
    assert "migrated=1" in applied
    raw = json.loads(ledger_path.read_text(encoding="utf-8"))["legacy-cli"]
    assert raw["effect_id"] == "legacy-cli"
    assert raw["request_id_aliases"] == ["legacy-cli"]
    assert raw["schema_version"] == 2

    assert main(["migrate", "--plan", "--file", str(ledger_path), "--json"]) == 0
    verified = json.loads(capsys.readouterr().out)
    assert verified["backends"][0]["pending_entries"] == 0


def test_cli_cleanly_refuses_future_file_schema(tmp_path: Path, capsys) -> None:
    ledger_path = tmp_path / "future.json"
    payload = _entry("future-cli", version=3).to_dict()
    ledger_path.write_text(
        json.dumps({"future-cli": payload}),
        encoding="utf-8",
    )

    assert main(["migrate", "--plan", "--file", str(ledger_path)]) == 2
    assert "newer than this runtime" in capsys.readouterr().err


def test_cli_sqlite_plan_and_apply(tmp_path: Path, capsys) -> None:
    ledger_path = tmp_path / "ledger.db"
    storage = SqliteLedgerStorage(ledger_path)
    storage.set(_entry("legacy-sqlite", version=1))

    assert main(["migrate", "--plan", "--sqlite", str(ledger_path)]) == 0
    assert "migrate=1" in capsys.readouterr().out
    assert main(["migrate", "--apply", "--sqlite", str(ledger_path)]) == 0
    assert "migrated=1" in capsys.readouterr().out

    migrated = storage.get("legacy-sqlite")
    assert migrated is not None
    assert migrated.schema_version == 2


def test_raw_file_schema_inspection_is_read_only(tmp_path: Path) -> None:
    ledger_path = tmp_path / "ledger.json"
    rows = {
        "legacy": _entry("legacy", version=1).to_dict(),
        "current": _entry("current", version=2).to_dict(),
        "future": _entry("future", version=3).to_dict(),
    }
    ledger_path.write_text(json.dumps(rows), encoding="utf-8")
    before = ledger_path.read_bytes()

    versions = inspect_ledger_schema_versions(
        {"storage": "file", "path": str(ledger_path)}
    )

    assert versions == {1: 1, 2: 1, 3: 1}
    assert ledger_path.read_bytes() == before


def test_raw_sqlite_schema_inspection_does_not_create_or_update_storage(
    tmp_path: Path,
) -> None:
    missing = tmp_path / "missing.db"
    assert inspect_ledger_schema_versions(
        {"storage": "sqlite", "path": str(missing)}
    ) == {}
    assert not missing.exists()

    ledger_path = tmp_path / "ledger.db"
    storage = SqliteLedgerStorage(ledger_path)
    storage.set(_entry("legacy", version=1))
    with sqlite3.connect(ledger_path) as conn:
        payload = _entry("future", version=3).to_dict()
        conn.execute(
            "INSERT INTO mycelium_action_ledger (request_id, payload) VALUES (?, ?)",
            ("future", json.dumps(payload)),
        )
        conn.commit()
    before = ledger_path.read_bytes()

    versions = inspect_ledger_schema_versions(
        {"storage": "sqlite", "path": str(ledger_path)}
    )

    assert versions == {1: 1, 3: 1}
    assert ledger_path.read_bytes() == before


def test_cli_requires_plan_or_apply() -> None:
    with pytest.raises(SystemExit) as caught:
        main(["migrate", "--file", "ledger.json"])
    assert caught.value.code == 2
