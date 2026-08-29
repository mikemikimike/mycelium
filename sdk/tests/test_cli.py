"""Tests for the mycelium CLI."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from mycelium import load_config
from mycelium.__main__ import main
from mycelium.transition import SideEffectClass


def test_init_writes_quickstart_template_by_default(tmp_path: Path) -> None:
    out = tmp_path / "mycelium.yaml"
    assert main(["init", "-o", str(out)]) == 0
    text = out.read_text(encoding="utf-8")
    assert "transition:" in text
    assert "integrations:" in text
    assert "langgraph:" in text
    assert "enabled: true" in text
    assert "action_ledger:" in text
    assert "storage: sqlite" in text
    assert "mycelium-ledger.db" in text
    assert "unclassified_policy: strict" in text
    assert "reclaim_requires_death_signal: true" in text
    assert "subagent_task" in text
    assert "side_effect_class: non_idempotent_mutate" in text
    assert "send_payment" not in text

    config = load_config(out)
    assert config.transition is not None
    assert config.langgraph_enabled
    assert config.transition.agent_id.startswith("<TODO:")
    assert "agent id" in config.transition.agent_id

    assert config.tools["subagent_task"].side_effect_class == SideEffectClass.NON_IDEMPOTENT_MUTATE
    assert config.tools["subagent_task"].callable_path == "your_package.tools:subagent_task"


def test_init_writes_full_template(tmp_path: Path) -> None:
    out = tmp_path / "mycelium.yaml"
    assert main(["init", "--full", "-o", str(out)]) == 0
    text = out.read_text(encoding="utf-8")
    assert "transition:" in text
    assert "action_ledger:" in text
    assert "tools: {}" in text
    assert "tasks: {}" in text
    assert "side_effect_class: read" in text
    assert "side_effect_class: idempotent_mutate" in text
    assert "side_effect_class: keyed_mutate" in text
    assert "side_effect_class: non_idempotent_mutate" in text
    assert "side_effect_class: irreversible" in text
    assert "spendability" in text
    assert "multi_use" in text
    assert "non_replayable" in text
    assert "<TODO: your_read_tool>" not in text
    assert "retry_permission: manual_reconciliation_required" not in text

    config = load_config(out)
    assert config.transition is not None
    assert config.tools == {}
    assert config.tasks == {}
    assert config.registry_allowed == []


def test_init_minimal_template(tmp_path: Path) -> None:
    out = tmp_path / "mycelium.yaml"
    assert main(["init", "--minimal", "-o", str(out)]) == 0
    text = out.read_text(encoding="utf-8")
    assert "transition:" in text
    assert "action_ledger:" in text
    assert "send_payment" not in text
    assert "subagent_task" not in text
    assert load_config(out).transition is not None


def test_demo_runs(capsys) -> None:
    assert main(["demo"]) == 0
    out = capsys.readouterr().out
    assert "Mycelium feature demo" in out
    assert "langgraph-7417-duplicate-execution" in out
    assert "langgraph/issues/7417" in out
    assert "PASS" in out
    assert "transition envelope" in out
    assert "side_effect_class: non_idempotent_mutate" in out
    assert "load_config" in out
    assert "@ledger_sync()" not in out
    assert "@config.apply" in out


def test_init_refuses_overwrite(tmp_path: Path) -> None:
    out = tmp_path / "mycelium.yaml"
    out.write_text("existing", encoding="utf-8")
    assert main(["init", "-o", str(out)]) == 1
    assert out.read_text(encoding="utf-8") == "existing"


def test_init_force_overwrite(tmp_path: Path) -> None:
    out = tmp_path / "mycelium.yaml"
    out.write_text("existing", encoding="utf-8")
    assert main(["init", "-o", str(out), "--force"]) == 0
    assert "action_ledger:" in out.read_text(encoding="utf-8")


def test_config_schema_prints_json(capsys) -> None:
    assert main(["config", "schema"]) == 0
    schema = json.loads(capsys.readouterr().out)
    assert schema["properties"]["config_version"]["const"] == 1


def test_config_schema_writes_file(tmp_path: Path) -> None:
    out = tmp_path / "mycelium.schema.json"
    assert main(["config", "schema", "--output", str(out)]) == 0
    schema = json.loads(out.read_text(encoding="utf-8"))
    assert schema["$id"].endswith("mycelium-config-v1.json")


def test_config_docs_and_example_are_generated_from_model(capsys) -> None:
    assert main(["config", "docs"]) == 0
    docs = capsys.readouterr().out
    assert "# Mycelium configuration reference" in docs
    assert "`config_version`" in docs
    assert "## Transition" in docs

    assert main(["config", "example"]) == 0
    example = capsys.readouterr().out
    assert "config_version: 1" in example
    assert "unclassified_policy: strict" in example


def test_init_detect_finds_framework_and_decorated_tool(tmp_path: Path, capsys) -> None:
    (tmp_path / "pyproject.toml").write_text(
        '[project]\ndependencies = ["langgraph"]\n', encoding="utf-8"
    )
    package = tmp_path / "agent"
    package.mkdir()
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "tools.py").write_text(
        "from langchain_core.tools import tool\n"
        "@tool\n"
        "def send_email(address: str) -> str:\n"
        "    return address\n",
        encoding="utf-8",
    )
    output = tmp_path / "mycelium.yaml"

    assert main(["init", "--detect", "--project", str(tmp_path), "-o", str(output)]) == 0
    config = load_config(output)
    assert config.langgraph_enabled
    assert config.tools["send_email"].callable_path == "agent.tools:send_email"
    assert (
        config.tools["send_email"].side_effect_class
        == SideEffectClass.NON_IDEMPOTENT_MUTATE
    )
    schema = json.loads((tmp_path / "mycelium.schema.json").read_text(encoding="utf-8"))
    assert schema["properties"]["config_version"]["const"] == 1
    stdout = capsys.readouterr().out
    assert "Detected frameworks: langchain, langgraph" in stdout
    assert "mutation was assumed for safety" in stdout


def test_init_modes_are_mutually_exclusive() -> None:
    try:
        main(["init", "--detect", "--minimal"])
    except SystemExit as exc:
        assert exc.code == 2
    else:  # pragma: no cover - argparse must reject this combination
        raise AssertionError("mutually exclusive init modes were accepted")


def test_run_rejects_missing_command_and_unsafe_python_flags(
    tmp_path: Path,
    capsys,
) -> None:
    config = tmp_path / "mycelium.yaml"
    config.write_text(
        """
action_ledger: {storage: memory, tools: [print_once]}
tools:
  print_once:
    callable: builtins:print
""",
        encoding="utf-8",
    )

    assert main(["run", "--config", str(config), "--"]) == 2
    assert "missing command" in capsys.readouterr().err

    assert (
        main(
            [
                "run",
                "--config",
                str(config),
                "--",
                sys.executable,
                "-S",
                "-c",
                "pass",
            ]
        )
        == 2
    )
    assert "disable safe Mycelium startup instrumentation" in capsys.readouterr().err
