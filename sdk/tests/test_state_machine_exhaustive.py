"""Fast direct test for the in-process exhaustive state-machine scenario."""

from __future__ import annotations

from types import SimpleNamespace

from mycelium.verify.registry import ScenarioContext, known_scenarios
from mycelium.verify.scenarios.state_machine_exhaustive import (
    run_state_machine_exhaustive,
)
from mycelium.verify.types import VerificationStatus


def _context() -> ScenarioContext:
    isolation = SimpleNamespace(
        backend="memory",
        namespace=SimpleNamespace(prefix="test-state-machine-exhaustive"),
    )
    return ScenarioContext(
        isolation=isolation,
        timeout_seconds=2.0,
        rounds=1,
        workers=2,
        keep_artifacts=False,
    )


def test_state_machine_exhaustive_direct_passes() -> None:
    evidence = run_state_machine_exhaustive(_context())
    assert evidence.status == VerificationStatus.PASS, evidence.observed_behavior
    assert evidence.scenario == "state-machine-exhaustive"


def test_state_machine_exhaustive_registered() -> None:
    assert "state-machine-exhaustive" in known_scenarios()
