"""Tests for ``mycelium verify`` empirical scenarios."""

from __future__ import annotations

import json
import multiprocessing as mp
import os
import time
from pathlib import Path

import pytest

from mycelium import IsolationRefused, VerificationStatus, load_config_from_string, run_verify
from mycelium.__main__ import main
from mycelium.completion_contract import reset_completion_terminal_state
from mycelium.verify.engine import exit_code_for_verify
from mycelium.verify.isolation import (
    IsolationGateStorage,
    IsolationSession,
    VerificationNamespace,
    establish_isolation,
    register_isolation_adapter,
)
from mycelium.verify.render import render_human, render_json
from mycelium.verify.types import VerificationEvidence

_TIMEOUT_MARKER: Path | None = None


def _hanging_scenario(ctx) -> VerificationEvidence:
    proc = mp.get_context("fork").Process(target=time.sleep, args=(60,))
    proc.start()
    ctx.owned_procs.append(proc)
    assert _TIMEOUT_MARKER is not None
    _TIMEOUT_MARKER.write_text(str(proc.pid), encoding="utf-8")
    time.sleep(60)
    raise AssertionError("deadline was not enforced")


def _late_tracking_scenario(ctx) -> VerificationEvidence:
    deadline = time.monotonic() + ctx.timeout_seconds * 0.9
    while time.monotonic() < deadline:
        time.sleep(0.001)
    ctx.isolation.track(
        ctx.isolation.namespace.request_id("redispatch", "late-timeout")
    )
    Path(ctx.isolation.artifact_file("late-timeout-")).write_text(
        "retained timeout evidence", encoding="utf-8"
    )
    time.sleep(60)
    raise AssertionError("deadline was not enforced")


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


def _sqlite_dev(tmp_path: Path, extra: str = "") -> str:
    return f"""
transition:
  agent_id: verify-agent
  policy_version: "1"
action_ledger:
  storage: sqlite
  path: {tmp_path / "app-ledger.db"}
  tools: [charge]
tools:
  charge:
    callable: verify_probe_tools:charge
    side_effect_class: non_idempotent_mutate
    request_id_from: order_id
{extra}
"""


def _memory_dev() -> str:
    return """
transition:
  agent_id: verify-agent
  policy_version: "1"
action_ledger:
  storage: memory
  tools: [search]
tools:
  search:
    side_effect_class: read
"""


def test_redispatch_pass_across_reconstructed_ledger(tmp_path: Path) -> None:
    path = _write(tmp_path, _sqlite_dev(tmp_path))
    report = run_verify(
        path,
        scenarios=["redispatch"],
        connectivity=False,
        timeout_seconds=15,
    )
    assert report.scenarios[0].status == VerificationStatus.PASS
    assert report.scenarios[0].body_executions == 1
    assert report.empirically_verified is True


def test_contention_multiprocess(tmp_path: Path) -> None:
    path = _write(tmp_path, _sqlite_dev(tmp_path))
    report = run_verify(
        path,
        scenarios=["contention"],
        connectivity=False,
        rounds=3,
        workers=2,
        timeout_seconds=20,
    )
    assert report.scenarios[0].status == VerificationStatus.PASS
    assert report.scenarios[0].body_executions == 1


def test_storage_outage_fail_closed(tmp_path: Path) -> None:
    path = _write(tmp_path, _sqlite_dev(tmp_path))
    report = run_verify(path, scenarios=["storage-outage"], connectivity=False, timeout_seconds=15)
    evidence = report.scenarios[0]
    assert evidence.status == VerificationStatus.PASS
    joined = " ".join(evidence.ledger_decisions)
    assert "body-start" in joined
    assert "boundary-write" in joined
    assert "complete" in joined


def test_contention_and_reconcile_helpers_reject_false_pass(tmp_path: Path) -> None:
    from types import SimpleNamespace

    from mycelium.verify.workers import (
        concurrent_reconcile_failure,
        contention_round_failure,
    )

    def _lines(name: str, text: str) -> str:
        path = tmp_path / name
        path.write_text(text, encoding="utf-8")
        return str(path)

    clean = [SimpleNamespace(pid=1, exitcode=0), SimpleNamespace(pid=2, exitcode=0)]
    killed = [SimpleNamespace(pid=1, exitcode=0), SimpleNamespace(pid=2, exitcode=-9)]
    matching = _lines("out.txt", "{'charged': True}\n{'charged': True}\n")
    empty = _lines("empty.txt", "")
    err = _lines("err.txt", "RuntimeError: boom\n")
    mixed = _lines("mixed.txt", "{'a': 1}\n{'b': 2}\n")
    one = _lines("one.txt", "{'charged': True}\n")
    peer_err = _lines("peer.txt", "LedgerHardBlockError: blocked\n")
    ready = _lines("ready.txt", "ready\nready\n")
    partial_ready = _lines("partial-ready.txt", "ready\n")

    assert (
        contention_round_failure(
            clean,
            executions=1,
            out_file=matching,
            err_file=empty,
            workers=2,
            ready_file=ready,
        )
        is None
    )
    assert contention_round_failure(
        clean, executions=0, out_file=matching, err_file=empty, workers=2
    )
    assert contention_round_failure(
        killed, executions=1, out_file=matching, err_file=empty, workers=2
    )
    assert contention_round_failure(clean, executions=1, out_file=matching, err_file=err, workers=2)
    assert contention_round_failure(clean, executions=1, out_file=one, err_file=empty, workers=2)
    assert contention_round_failure(clean, executions=1, out_file=mixed, err_file=empty, workers=2)
    assert contention_round_failure(
        clean,
        executions=1,
        out_file=matching,
        err_file=empty,
        workers=2,
        ready_file=partial_ready,
    )

    assert (
        concurrent_reconcile_failure(
            clean, executions=1, out_file=one, err_file=peer_err, workers=2
        )
        is None
    )
    assert concurrent_reconcile_failure(
        clean, executions=0, out_file=empty, err_file=err, workers=2
    )
    assert concurrent_reconcile_failure(
        clean, executions=1, out_file=empty, err_file=peer_err, workers=2
    )
    assert concurrent_reconcile_failure(
        killed, executions=1, out_file=one, err_file=peer_err, workers=2
    )
    assert concurrent_reconcile_failure(clean, executions=1, out_file=one, err_file=err, workers=2)


def test_cleanup_failure_is_reported(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = _write(tmp_path, _sqlite_dev(tmp_path))
    from mycelium.verify.isolation import IsolationSession

    def boom(self, *, keep_artifacts: bool = False) -> None:
        raise RuntimeError("injected cleanup failure")

    monkeypatch.setattr(IsolationSession, "cleanup", boom)
    report = run_verify(path, scenarios=["redispatch"], connectivity=False, timeout_seconds=15)
    assert report.isolation_status == VerificationStatus.WARN
    assert "cleanup failed" in (report.isolation_detail or "")
    assert "injected cleanup failure" in (report.isolation_detail or "")
    assert report.warning_count >= 1
    assert report.production_ready is False
    assert report.scenarios[0].status == VerificationStatus.PASS
    assert exit_code_for_verify(report) == 0
    assert exit_code_for_verify(report, strict=True) == 1


def test_worker_crash_phases(tmp_path: Path) -> None:
    path = _write(tmp_path, _sqlite_dev(tmp_path))
    report = run_verify(path, scenarios=["worker-crash"], connectivity=False, timeout_seconds=25)
    assert report.scenarios[0].status == VerificationStatus.PASS


def test_ambiguous_effect_and_reconcile(tmp_path: Path) -> None:
    path = _write(tmp_path, _sqlite_dev(tmp_path))
    report = run_verify(
        path,
        scenarios=["ambiguous-effect", "reconcile"],
        connectivity=False,
        timeout_seconds=20,
        workers=2,
    )
    statuses = {item.scenario: item.status for item in report.scenarios}
    assert statuses["ambiguous-effect"] == VerificationStatus.PASS
    assert statuses["reconcile"] == VerificationStatus.PASS, report.scenarios[1].observed_behavior


def test_memory_skips_multiprocess_and_warns_redispatch(tmp_path: Path) -> None:
    path = _write(tmp_path, _memory_dev())
    report = run_verify(
        path,
        scenarios=["redispatch", "contention", "worker-crash"],
        connectivity=False,
    )
    by_name = {item.scenario: item.status for item in report.scenarios}
    assert by_name["redispatch"] == VerificationStatus.WARN
    assert by_name["contention"] == VerificationStatus.SKIP
    assert by_name["worker-crash"] == VerificationStatus.SKIP
    assert report.empirically_verified is False
    assert "single-node" in report.isolation_detail or report.backend == "memory"


def test_file_and_sqlite_single_node_label(tmp_path: Path) -> None:
    file_yaml = f"""
transition:
  agent_id: a
  policy_version: "1"
action_ledger:
  storage: file
  path: {tmp_path / "real-ledger.json"}
"""
    path = _write(tmp_path, file_yaml)
    report = run_verify(path, scenarios=["redispatch"], connectivity=False)
    assert "single-node" in report.isolation_detail
    # Must not write into the application's real ledger file.
    real = tmp_path / "real-ledger.json"
    if real.exists():
        assert "mycelium:verify:" not in real.read_text(encoding="utf-8")


def test_namespace_gate_and_collision(tmp_path: Path) -> None:
    cfg = load_config_from_string(_sqlite_dev(tmp_path))
    a = establish_isolation(cfg)
    b = establish_isolation(cfg)
    assert a.namespace.prefix != b.namespace.prefix
    gated = a.open_storage()
    with pytest.raises(IsolationRefused):
        gated.get("production-charge-1")
    a.cleanup()
    b.cleanup()


def test_isolation_refusal_unknown_backend(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
transition:
  agent_id: a
  policy_version: "1"
action_ledger:
  storage: custom_backend
""",
    )
    report = run_verify(path, scenarios=["redispatch"], connectivity=False)
    assert report.refused is True
    assert exit_code_for_verify(report) == 3


def test_credential_redaction(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
transition:
  agent_id: a
  policy_version: "1"
action_ledger:
  storage: postgres
  dsn: postgresql://alice:s3cret@127.0.0.1:1/mycelium
""",
    )
    report = run_verify(path, scenarios=["redispatch"], connectivity=False)
    blob = json.dumps(report.to_dict())
    assert "s3cret" not in blob
    assert "s3cret" not in (report.isolation_detail or "")
    assert "s3cret" not in (report.framework_error or "")


def test_doctor_blocking_prevents_empirical_run(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        f"""
profile: production
deployment:
  topology: multi_node
transition:
  agent_id: a
  policy_version: "1"
action_ledger:
  storage: file
  path: {tmp_path / "l.json"}
  tools: [charge]
  request_identity_policy: require_explicit
outcome_emit:
  storage: file
  path: {tmp_path / "o.jsonl"}
tools:
  charge:
    side_effect_class: non_idempotent_mutate
    request_id_from: order_id
""",
    )
    report = run_verify(path, scenarios=["redispatch"], connectivity=False)
    assert report.scenarios == []
    assert report.empirically_verified is False
    assert report.production_ready is False
    assert exit_code_for_verify(report) == 1


def test_never_executes_application_tool_or_llm(tmp_path: Path) -> None:
    calls = {"n": 0}

    def charge(order_id: str) -> str:
        calls["n"] += 1
        raise AssertionError("application tool executed during verify")

    import sys
    import types

    mod = types.ModuleType("verify_probe_tools")
    mod.charge = charge  # type: ignore[attr-defined]
    sys.modules["verify_probe_tools"] = mod
    try:
        path = _write(tmp_path, _sqlite_dev(tmp_path))
        report = run_verify(path, scenarios=["redispatch"], connectivity=False, timeout_seconds=15)
        assert report.scenarios[0].status == VerificationStatus.PASS
        assert calls["n"] == 0
    finally:
        del sys.modules["verify_probe_tools"]


def test_human_json_strict_exit_codes(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    path = _write(tmp_path, _sqlite_dev(tmp_path))
    assert (
        main(
            [
                "verify",
                "-c",
                str(path),
                "--scenario",
                "redispatch",
                "--no-connectivity",
                "--json",
            ]
        )
        == 0
    )
    out = capsys.readouterr().out
    payload = json.loads(out)
    assert payload["overall_status"] in {"PASS", "WARN", "FAIL", "SKIP", "ERROR"}
    assert "scenarios" in payload
    assert "doctor" in payload
    assert list(payload.keys()) == sorted(payload.keys())
    human_path = _write(tmp_path, _memory_dev(), "mem.yaml")
    code = main(
        [
            "verify",
            "-c",
            str(human_path),
            "--scenario",
            "redispatch",
            "--no-connectivity",
            "--strict",
        ]
    )
    assert code == 1
    missing = tmp_path / "nope.yaml"
    assert main(["verify", "-c", str(missing), "--scenario", "all"]) == 2
    assert main(["verify", "-c", str(path)]) == 2


def test_human_rendering(tmp_path: Path) -> None:
    path = _write(tmp_path, _sqlite_dev(tmp_path))
    report = run_verify(path, scenarios=["redispatch"], connectivity=False)
    text = render_human(report)
    assert "Mycelium Verify" in text
    assert "Doctor:" in text
    assert "Empirically verified:" in text
    payload = json.loads(render_json(report))
    assert "empirically_verified" in payload


def test_unknown_scenario_is_framework_error(tmp_path: Path) -> None:
    path = _write(tmp_path, _sqlite_dev(tmp_path))
    report = run_verify(path, scenarios=["not-a-scenario"], connectivity=False)
    assert report.framework_error is not None
    assert exit_code_for_verify(report) == 2


def test_existing_doctor_cli_unchanged() -> None:
    with pytest.raises(SystemExit) as exc:
        main(["doctor", "--help"])
    assert exc.value.code == 0


def test_cli_smoke_each_scenario(tmp_path: Path) -> None:
    path = _write(tmp_path, _sqlite_dev(tmp_path))
    for name in (
        "redispatch",
        "contention",
        "storage-outage",
        "worker-crash",
        "ambiguous-effect",
        "reconcile",
        "secret-in-args",
        "entity-guard",
        "destructive-confirm",
        "authority-window",
        "use-time-currency",
        "state-machine-exhaustive",
    ):
        code = main(
            [
                "verify",
                "-c",
                str(path),
                "--scenario",
                name,
                "--no-connectivity",
                "--rounds",
                "2",
                "--workers",
                "2",
                "--timeout",
                "25",
            ]
        )
        assert code == 0, name


def test_keep_artifacts_and_cleanup(tmp_path: Path) -> None:
    path = _write(tmp_path, _sqlite_dev(tmp_path))
    report = run_verify(
        path,
        scenarios=["redispatch"],
        connectivity=False,
        keep_artifacts=True,
    )
    assert report.scenarios[0].artifacts
    assert all(Path(item).exists() for item in report.scenarios[0].artifacts)
    report2 = run_verify(path, scenarios=["redispatch"], connectivity=False)
    assert report2.scenarios[0].status == VerificationStatus.PASS
    assert all(not Path(item).exists() for item in report2.scenarios[0].artifacts)


def test_scenario_timeout_terminates_process_group(tmp_path: Path, monkeypatch) -> None:
    import mycelium.verify.registry as registry

    global _TIMEOUT_MARKER
    _TIMEOUT_MARKER = tmp_path / "worker.pid"
    monkeypatch.setitem(registry._REGISTRY, "redispatch", _hanging_scenario)
    path = _write(tmp_path, _sqlite_dev(tmp_path))

    started = time.monotonic()
    report = run_verify(
        path,
        scenarios=["redispatch"],
        connectivity=False,
        timeout_seconds=0.25,
    )

    assert time.monotonic() - started < 3
    assert report.scenarios[0].status == VerificationStatus.ERROR
    assert exit_code_for_verify(report) == 1
    pid = int(_TIMEOUT_MARKER.read_text(encoding="utf-8"))
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            break
        time.sleep(0.02)
    else:
        pytest.fail(f"verifier subprocess {pid} survived timeout")


def test_timeout_drains_late_tracking_before_cleanup(tmp_path: Path, monkeypatch) -> None:
    import mycelium.verify.registry as registry
    from mycelium.verify.isolation import IsolationSession

    monkeypatch.setitem(registry._REGISTRY, "redispatch", _late_tracking_scenario)
    cleaned: list[str] = []
    original_cleanup = IsolationSession.cleanup

    def capture_cleanup(self, *, keep_artifacts: bool = False) -> None:
        cleaned.extend(self.tracked_ids)
        original_cleanup(self, keep_artifacts=keep_artifacts)

    monkeypatch.setattr(IsolationSession, "cleanup", capture_cleanup)
    path = _write(tmp_path, _sqlite_dev(tmp_path))
    report = run_verify(
        path,
        scenarios=["redispatch"],
        connectivity=False,
        timeout_seconds=0.5,
    )

    assert report.scenarios[0].status == VerificationStatus.ERROR
    assert any(request_id.endswith(":late-timeout") for request_id in cleaned)


def test_probe_failure_removes_partial_artifacts(tmp_path: Path) -> None:
    artifact_root = tmp_path / "probe-artifacts"

    def opener(namespace, raw, workdir):
        artifact_root.mkdir()
        return IsolationSession(
            namespace=namespace,
            backend="probe-failure",
            topology_label="test",
            restart_capable=False,
            multiprocess_capable=False,
            persistence_asserted=False,
            _artifact_tmp=artifact_root,
            _factory=lambda: (_ for _ in ()).throw(ConnectionError("probe failed")),
        )

    register_isolation_adapter("probe-failure", opener)
    config = load_config_from_string(
        "action_ledger:\n  storage: probe-failure\n",
    )
    with pytest.raises(IsolationRefused):
        establish_isolation(config)
    assert not artifact_root.exists()


def test_probe_failure_reports_retained_artifacts(tmp_path: Path, monkeypatch) -> None:
    import mycelium.verify.isolation as isolation

    artifact_root = tmp_path / "retained-probe-artifacts"

    def opener(namespace, raw, workdir):
        artifact_root.mkdir()
        (artifact_root / "probe.txt").write_text("probe evidence", encoding="utf-8")
        return IsolationSession(
            namespace=namespace,
            backend="probe-retained",
            topology_label="test",
            restart_capable=False,
            multiprocess_capable=False,
            persistence_asserted=False,
            _artifact_tmp=artifact_root,
            _factory=lambda: (_ for _ in ()).throw(ConnectionError("probe failed")),
        )

    monkeypatch.setitem(isolation._ADAPTERS, "sqlite", opener)
    path = _write(tmp_path, _sqlite_dev(tmp_path))
    report = run_verify(
        path,
        scenarios=["redispatch"],
        connectivity=False,
        keep_artifacts=True,
    )

    assert report.refused is True
    assert report.artifacts == [str(artifact_root), str(artifact_root / "probe.txt")]
    assert all(Path(artifact).exists() for artifact in report.artifacts)
    payload = json.loads(render_json(report))
    human = render_human(report)
    assert payload["artifacts"] == report.artifacts
    assert all(artifact in human for artifact in report.artifacts)


def test_timeout_reports_retained_artifacts(tmp_path: Path, monkeypatch) -> None:
    import mycelium.verify.registry as registry

    monkeypatch.setitem(registry._REGISTRY, "redispatch", _late_tracking_scenario)
    path = _write(tmp_path, _sqlite_dev(tmp_path))
    report = run_verify(
        path,
        scenarios=["redispatch"],
        connectivity=False,
        timeout_seconds=0.5,
        keep_artifacts=True,
    )

    artifacts = report.scenarios[0].artifacts
    assert artifacts
    assert all(Path(artifact).exists() for artifact in artifacts)
    human = render_human(report)
    payload = json.loads(render_json(report))
    assert all(artifact in human for artifact in artifacts)
    assert payload["scenarios"][0]["artifacts"] == artifacts


def test_isolation_gate_storage_type() -> None:
    ns = VerificationNamespace(
        run_id="x",
        prefix="mycelium:verify:x:",
        started_at=0.0,
        backend="memory",
    )
    from mycelium.action_ledger import InMemoryLedgerStorage

    gate = IsolationGateStorage(InMemoryLedgerStorage(), ns)
    assert gate.get("mycelium:verify:x:ok") is None
    with pytest.raises(IsolationRefused):
        gate.get("other")


@pytest.mark.parametrize("scenario", ["all"])
def test_all_order_sqlite(tmp_path: Path, scenario: str) -> None:
    path = _write(tmp_path, _sqlite_dev(tmp_path))
    report = run_verify(
        path,
        scenarios=[scenario],
        connectivity=False,
        rounds=2,
        workers=2,
        timeout_seconds=40,
    )
    names = [item.scenario for item in report.scenarios]
    assert names == [
        "redispatch",
        "contention",
        "storage-outage",
        "worker-crash",
        "ambiguous-effect",
        "reconcile",
        "secret-in-args",
        "entity-guard",
        "destructive-confirm",
        "authority-window",
        "use-time-currency",
        "state-machine-exhaustive",
        "simulation",
    ]
    assert all(item.status == VerificationStatus.PASS for item in report.scenarios)
    assert report.empirically_verified is True


def test_live_redis_optional() -> None:
    pytest.importorskip("redis")
    from backend_gates import require_redis_or_skip

    require_redis_or_skip()
    pytest.skip("live redis verify covered when MYCELIUM_REDIS_URL is reachable — use CLI")


def test_live_postgres_optional() -> None:
    from backend_gates import require_postgres_dsn_or_skip

    dsn = require_postgres_dsn_or_skip()
    assert dsn
    pytest.skip("live postgres verify is opt-in; DSN present")
