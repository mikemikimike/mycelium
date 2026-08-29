"""Tests for ``mycelium doctor`` (read-only production-safety verification)."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from mycelium import (
    DoctorStatus,
    LedgerEntry,
    SqliteLedgerStorage,
    exit_code_for_report,
    load_config_from_string,
    run_doctor,
    run_doctor_on_config,
)
from mycelium.__main__ import main
from mycelium.completion_contract import reset_completion_terminal_state
from mycelium.doctor.render import render_human, render_json
from mycelium.storage._helpers import redact_secrets


@pytest.fixture(autouse=True)
def _reset_adapters() -> None:
    import importlib

    reset_completion_terminal_state()
    budget_llm = importlib.import_module("mycelium.budget_llm")
    budget_llm.reset_llm_budget_state()
    yield
    reset_completion_terminal_state()
    budget_llm.reset_llm_budget_state()


def _write(tmp_path: Path, text: str, name: str = "mycelium.yaml") -> Path:
    path = tmp_path / name
    path.write_text(text, encoding="utf-8")
    return path


def _single_node_prod(tmp_path: Path, extra: str = "") -> str:
    return f"""
profile: production
deployment:
  topology: single_node
transition:
  agent_id: a
  policy_version: "2026.08.1"
action_ledger:
  storage: sqlite
  path: {tmp_path / "ledger.db"}
  tools: [charge, search]
  unclassified_policy: warn
  request_identity_policy: require_explicit
outcome_emit:
  storage: file
  path: {tmp_path / "outcomes.jsonl"}
  on_failure: error
tools:
  charge:
    side_effect_class: non_idempotent_mutate
    request_id_from: order_id
  search:
    side_effect_class: read
{extra}
"""


def _multi_node_prod(extra: str = "") -> str:
    return f"""
profile: production
deployment:
  topology: multi_node
integrations:
  langgraph:
    enabled: false
transition:
  agent_id: a
  policy_version: "2026.08.1"
action_ledger:
  storage: postgres
  dsn: postgresql://alice:s3cret@db.example/mycelium
  table: mycelium_action_ledger
  tools: [charge]
  request_identity_policy: require_explicit
outcome_emit:
  storage: postgres
  url: postgresql://alice:s3cret@db.example/mycelium
  table: mycelium_outcomes
  on_failure: error
tools:
  charge:
    side_effect_class: keyed_mutate
    request_id_from: order_id
{extra}
"""


def test_fully_protected_single_node(tmp_path: Path) -> None:
    path = _write(tmp_path, _single_node_prod(tmp_path))
    report = run_doctor(path, connectivity=False)
    assert report.failure_count == 0
    assert report.production_ready is True
    assert report.distributed_ready is False
    assert exit_code_for_report(report) == 0
    assert any(c.id == "ledger.backend" and c.status == DoctorStatus.PASS for c in report.checks)
    assert any(c.id == "ledger.schema" and c.status == DoctorStatus.SKIP for c in report.checks)


def _doctor_schema_check(tmp_path: Path, version: int):
    ledger_path = tmp_path / "ledger.db"
    storage = SqliteLedgerStorage(ledger_path)
    entry = LedgerEntry(
        request_id=f"schema-{version}",
        tool="charge",
        args=[],
        kwargs={},
        status="completed",
        terminal_outcome="COMPLETED",
        schema_version=min(version, 2),
    )
    storage.set(entry)
    if version > 2:
        payload = entry.to_dict()
        payload["schema_version"] = version
        with sqlite3.connect(ledger_path) as conn:
            conn.execute(
                "UPDATE mycelium_action_ledger SET payload = ? WHERE request_id = ?",
                (json.dumps(payload), entry.request_id),
            )
            conn.commit()
    cfg = load_config_from_string(_single_node_prod(tmp_path))
    report = run_doctor_on_config(cfg, connectivity=True)
    return next(c for c in report.checks if c.id == "ledger.schema")


@pytest.mark.parametrize(
    ("version", "expected"),
    [(1, DoctorStatus.WARN), (2, DoctorStatus.PASS), (3, DoctorStatus.FAIL)],
)
def test_doctor_reports_ledger_schema_versions(
    tmp_path: Path, version: int, expected: DoctorStatus
) -> None:
    check = _doctor_schema_check(tmp_path, version)
    assert check.status == expected
    assert f"v{version}=1" in check.details


def test_fully_protected_multi_node_postgres(monkeypatch: pytest.MonkeyPatch) -> None:
    # Avoid real network: skip connectivity.
    cfg = load_config_from_string(_multi_node_prod())
    report = run_doctor_on_config(cfg, connectivity=False)
    assert report.failure_count == 0
    assert report.production_ready is True
    assert report.distributed_ready is True
    assert any("PostgreSQL" in c.summary for c in report.checks if c.category == "Outcomes")


def test_redis_outcomes_with_and_without_persistence(tmp_path: Path) -> None:
    with pytest.raises(Exception):
        # production load rejects redis without persistence
        load_config_from_string(
            f"""
profile: production
outcome_emit:
  storage: redis
  url: redis://localhost/0
action_ledger:
  storage: sqlite
  path: {tmp_path / "l.db"}
  tools: [charge]
tools:
  charge:
    side_effect_class: non_idempotent_mutate
"""
        )

    cfg = load_config_from_string(
        """
profile: production
deployment:
  topology: multi_node
action_ledger:
  storage: redis
  url: redis://localhost/0
  tools: [charge]
outcome_emit:
  storage: redis
  url: redis://localhost/0
  persistence: required
  on_failure: error
tools:
  charge:
    side_effect_class: non_idempotent_mutate
    request_id_from: order_id
transition:
  agent_id: a
  policy_version: "1"
"""
    )
    report = run_doctor_on_config(cfg, connectivity=False)
    assert report.failure_count == 0
    outcome = next(c for c in report.checks if c.id == "outcomes.backend")
    assert outcome.evidence == "operator_asserted"
    assert "cannot verify" in outcome.details.lower() or "AOF" in outcome.details


def test_development_config_with_warnings() -> None:
    cfg = load_config_from_string(
        """
tools:
  charge:
    side_effect_class: non_idempotent_mutate
  mystery: {}
"""
    )
    report = run_doctor_on_config(cfg, connectivity=False)
    assert report.production_ready is False
    assert report.warning_count >= 1
    assert exit_code_for_report(report) == 0
    assert exit_code_for_report(report, strict=True) == 1


def test_missing_business_request_identity_hint(tmp_path: Path) -> None:
    cfg = load_config_from_string(_single_node_prod(tmp_path).replace(
        "    request_id_from: order_id\n", ""
    ))
    report = run_doctor_on_config(cfg, connectivity=False)
    assert any(c.id == "identity.policy" and c.status == DoctorStatus.PASS for c in report.checks)
    assert any(c.id == "identity.request_id_from_hint" for c in report.checks)


def test_consequential_memory_ledger_fails_load(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
profile: production
outcome_emit:
  storage: file
  path: ./o.jsonl
action_ledger:
  storage: memory
  tools: [charge]
tools:
  charge:
    side_effect_class: non_idempotent_mutate
transition:
  agent_id: a
  policy_version: "1"
""",
    )
    report = run_doctor(path, connectivity=False)
    assert report.load_error
    assert exit_code_for_report(report) == 2


def test_guards_missing_strict_run_identity_fails_load(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        f"""
profile: production
outcome_emit:
  storage: file
  path: {tmp_path / "o.jsonl"}
loop_guard:
  missing_run_id_policy: warn
  storage: memory
tools:
  ping: {{}}
""",
    )
    report = run_doctor(path, connectivity=False)
    assert report.load_error is not None
    assert exit_code_for_report(report) == 2


def test_completion_configured_adapter_unwired_dev() -> None:
    cfg = load_config_from_string(
        """
profile: development
completion:
  storage: memory
  required: [done]
tools:
  ping: {}
"""
    )
    report = run_doctor_on_config(cfg, connectivity=False)
    check = next(c for c in report.checks if c.id == "completion.adapter")
    assert check.status == DoctorStatus.WARN


def test_completion_unwired_production_fails_load(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        f"""
profile: production
outcome_emit:
  storage: file
  path: {tmp_path / "o.jsonl"}
completion:
  storage: memory
  required: [done]
tools:
  ping: {{}}
""",
    )
    report = run_doctor(path, connectivity=False)
    assert report.load_error
    assert "terminal" in (report.load_error or "").lower() or "completion" in (
        report.load_error or ""
    ).lower()
    assert exit_code_for_report(report) == 2


def test_langgraph_installed_not_selected_is_not_enough() -> None:
    # Even if langgraph importable, disabled integration must not look wired.
    cfg = load_config_from_string(
        """
profile: development
integrations:
  langgraph:
    enabled: false
completion:
  storage: memory
  required: [x]
tools: {}
"""
    )
    report = run_doctor_on_config(cfg, connectivity=False)
    check = next(c for c in report.checks if c.id == "completion.adapter")
    assert check.status == DoctorStatus.WARN
    assert "not enough" in check.remediation.lower() or "enabled" in check.remediation


def test_budget_unwired_dev_warns() -> None:
    cfg = load_config_from_string(
        """
budget:
  storage: memory
  max_tokens: 100
tools: {}
"""
    )
    report = run_doctor_on_config(cfg, connectivity=False)
    check = next(c for c in report.checks if c.id == "budget.adapter")
    assert check.status == DoctorStatus.WARN


def test_cost_limit_without_resolver_warns_in_dev() -> None:
    cfg = load_config_from_string(
        """
integrations:
  langgraph:
    enabled: true
budget:
  storage: memory
  max_usd: 1.0
tools: {}
"""
    )
    # May warn on adapter install failure OR cost resolver — either is a warning/fail signal
    report = run_doctor_on_config(cfg, connectivity=False)
    budget_checks = [c for c in report.checks if c.category == "Budget"]
    assert budget_checks
    assert any(c.status in (DoctorStatus.WARN, DoctorStatus.FAIL) for c in budget_checks)


def test_missing_outcome_storage_production_load_error(tmp_path: Path) -> None:
    path = _write(tmp_path, "profile: production\ntools:\n  ping: {}\n")
    report = run_doctor(path, connectivity=False)
    assert report.load_error
    assert exit_code_for_report(report) == 2


def test_file_backend_under_multi_node_fails(tmp_path: Path) -> None:
    text = _single_node_prod(tmp_path).replace(
        "topology: single_node", "topology: multi_node"
    )
    cfg = load_config_from_string(text)
    report = run_doctor_on_config(cfg, connectivity=False)
    assert any(
        c.id == "topology.multi_node" and c.status == DoctorStatus.FAIL
        for c in report.checks
    )
    assert report.distributed_ready is False
    assert exit_code_for_report(report) == 1


def test_file_state_backend_under_multi_node_fails(tmp_path: Path) -> None:
    cfg = load_config_from_string(
        f"""
deployment:
  topology: multi_node
state_backend:
  storage: file
  path: {tmp_path / "state.json"}
loop_guard: {{}}
tools: {{}}
"""
    )
    report = run_doctor_on_config(cfg, connectivity=False)
    assert any(
        check.id == "state_backend.backend"
        and check.status == DoctorStatus.FAIL
        for check in report.checks
    )
    assert any(
        check.id == "topology.multi_node" and check.status == DoctorStatus.FAIL
        for check in report.checks
    )


def test_postgres_state_backend_satisfies_multi_node_topology() -> None:
    cfg = load_config_from_string(
        """
deployment:
  topology: multi_node
state_backend:
  storage: postgres
  dsn: postgresql://localhost/mycelium
loop_guard: {}
scope_guard:
  allowed_tools: [read]
tools:
  read: {}
"""
    )
    report = run_doctor_on_config(cfg, connectivity=False)
    assert any(
        check.id == "state_backend.backend"
        and check.status == DoctorStatus.PASS
        for check in report.checks
    )
    assert any(
        check.id == "topology.multi_node" and check.status == DoctorStatus.PASS
        for check in report.checks
    )


def test_backend_connection_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from mycelium.doctor import connectivity as conn

    def boom(dsn: str, *, timeout_seconds: float = 2.0) -> Any:
        return conn.ProbeResult(
            ok=False,
            kind="timeout",
            message="connection timed out postgresql://***@db/mycelium",
        )

    monkeypatch.setattr(conn, "probe_postgres", boom)
    cfg = load_config_from_string(_multi_node_prod())
    report = run_doctor_on_config(cfg, connectivity=True, timeout_seconds=0.1)
    assert any(
        c.status == DoctorStatus.FAIL and "connectivity" in c.id for c in report.checks
    )


def test_credential_redaction_in_load_error(tmp_path: Path) -> None:
    # Force a postgres outcome build path via doctor connectivity message
    raw = "postgresql://alice:s3cret@db/mycelium password=s3cret"
    assert "s3cret" not in redact_secrets(raw)


def test_human_and_json_output(tmp_path: Path) -> None:
    path = _write(tmp_path, _single_node_prod(tmp_path))
    report = run_doctor(path, connectivity=False)
    human = render_human(report)
    assert "Mycelium Doctor" in human
    assert "Production ready:" in human
    payload = json.loads(render_json(report))
    assert payload["overall_status"] in {"PASS", "WARN", "FAIL", "SKIP"}
    assert "checks" in payload
    assert "pass_count" in payload
    # stable key order from sort_keys
    assert list(payload.keys()) == sorted(payload.keys())


def test_cli_exit_codes(tmp_path: Path) -> None:
    good = _write(tmp_path, _single_node_prod(tmp_path), "good.yaml")
    assert main(["doctor", "-c", str(good), "--no-connectivity"]) == 0
    bad = _write(
        tmp_path,
        _single_node_prod(tmp_path).replace(
            "topology: single_node", "topology: multi_node"
        ),
        "bad.yaml",
    )
    assert main(["doctor", "-c", str(bad), "--no-connectivity"]) == 1
    missing = tmp_path / "nope.yaml"
    assert main(["doctor", "-c", str(missing), "--no-connectivity"]) == 2


def test_cli_json_and_strict(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    path = _write(
        tmp_path,
        """
tools:
  mystery: {}
""",
    )
    code = main(["doctor", "-c", str(path), "--json", "--no-connectivity"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["warning_count"] >= 1
    code_strict = main(
        ["doctor", "-c", str(path), "--strict", "--no-connectivity"]
    )
    assert code_strict == 1


def test_doctor_fix_adds_only_version_and_schema_metadata(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = _write(
        tmp_path,
        """# user comment
tools:
  mystery: {}
""",
    )
    original = path.read_text(encoding="utf-8")

    code = main(["doctor", "-c", str(path), "--fix", "--json", "--no-connectivity"])
    captured = capsys.readouterr()
    assert code == 0
    assert json.loads(captured.out)["warning_count"] >= 1
    assert "fixed [config.version]" in captured.err

    updated = path.read_text(encoding="utf-8")
    assert updated.endswith(original)
    assert updated.startswith("# yaml-language-server:")
    assert "config_version: 1\n" in updated
    schema_path = tmp_path / "mycelium.schema.json"
    assert json.loads(schema_path.read_text(encoding="utf-8"))["$id"]

    main(["doctor", "-c", str(path), "--fix", "--json", "--no-connectivity"])
    second = capsys.readouterr()
    assert second.err == ""
    assert path.read_text(encoding="utf-8") == updated


def test_doctor_fix_refuses_invalid_or_future_config(tmp_path: Path) -> None:
    from mycelium.doctor.fixes import apply_conservative_fixes

    invalid = tmp_path / "invalid.yaml"
    invalid.write_text("tools: [", encoding="utf-8")
    future = tmp_path / "future.yaml"
    future.write_text("config_version: 99\n", encoding="utf-8")

    assert apply_conservative_fixes(invalid) == []
    assert apply_conservative_fixes(future) == []
    assert invalid.read_text(encoding="utf-8") == "tools: ["
    assert future.read_text(encoding="utf-8") == "config_version: 99\n"


def test_doctor_never_executes_tools(tmp_path: Path) -> None:
    calls = {"n": 0}

    def charge(order_id: str) -> str:
        calls["n"] += 1
        raise AssertionError("tool executed during doctor")

    # Register a module attribute the config could resolve — doctor must not import-call it.
    import sys
    import types

    mod = types.ModuleType("doctor_probe_tools")
    mod.charge = charge  # type: ignore[attr-defined]
    sys.modules["doctor_probe_tools"] = mod
    try:
        path = _write(
            tmp_path,
            f"""
profile: production
deployment:
  topology: single_node
transition:
  agent_id: a
  policy_version: "1"
action_ledger:
  storage: sqlite
  path: {tmp_path / "l.db"}
  tools: [charge]
outcome_emit:
  storage: file
  path: {tmp_path / "o.jsonl"}
tools:
  charge:
    callable: doctor_probe_tools:charge
    side_effect_class: non_idempotent_mutate
    request_id_from: order_id
""",
        )
        report = run_doctor(path, connectivity=False)
        assert report.load_error is None
        assert calls["n"] == 0
    finally:
        del sys.modules["doctor_probe_tools"]


def test_existing_cli_commands_unchanged() -> None:
    with pytest.raises(SystemExit) as exc:
        main(["outcomes", "dttr", "--help"])
    assert exc.value.code == 0


def test_verbose_includes_evidence(tmp_path: Path) -> None:
    path = _write(tmp_path, _single_node_prod(tmp_path))
    report = run_doctor(path, connectivity=False)
    text = render_human(report, verbose=True)
    assert "evidence=" in text


def test_secret_args_omitted_is_skip_not_warn(tmp_path: Path) -> None:
    path = _write(tmp_path, _single_node_prod(tmp_path))
    report = run_doctor(path, connectivity=False)
    scanning = next(c for c in report.checks if c.id == "secrets.scanning")
    assert scanning.status == DoctorStatus.SKIP
    host = next(c for c in report.checks if c.id == "secrets.host_logs")
    assert host.evidence == "not_verifiable"
    assert host.status == DoctorStatus.SKIP
    assert not any(
        c.id.startswith("secrets.") and c.status in (DoctorStatus.WARN, DoctorStatus.FAIL)
        for c in report.checks
    )


def test_secret_args_enabled_reports_fail_closed(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        _single_node_prod(
            tmp_path,
            extra="""
secret_args:
  enabled: true
  policy: error
  allow_fields: [authorization]
""",
        ),
    )
    report = run_doctor(path, connectivity=False)
    scanning = next(c for c in report.checks if c.id == "secrets.scanning")
    assert scanning.status == DoctorStatus.PASS
    closed = next(c for c in report.checks if c.id == "secrets.production_fail_closed")
    assert closed.status == DoctorStatus.PASS
    allow = next(c for c in report.checks if c.id == "secrets.allow_fields")
    assert allow.status == DoctorStatus.WARN
