"""Generated config artifacts stay synchronized with the typed model."""

from __future__ import annotations

from pathlib import Path

from mycelium import load_config
from mycelium.config_artifacts import render_config_example, render_config_reference

SDK_ROOT = Path(__file__).resolve().parents[1]


def test_checked_in_config_artifacts_are_current(tmp_path: Path) -> None:
    reference = render_config_reference()
    example = render_config_example()
    assert (SDK_ROOT / "docs" / "CONFIG_REFERENCE.md").read_text(
        encoding="utf-8"
    ) == reference
    assert (SDK_ROOT / "examples" / "mycelium.generated.example.yaml").read_text(
        encoding="utf-8"
    ) == example

    path = tmp_path / "mycelium.yaml"
    path.write_text(example, encoding="utf-8")
    config = load_config(path)
    assert config.transition is not None
    assert config.transition.agent_id == "example-agent"
