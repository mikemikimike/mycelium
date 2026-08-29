"""Small, conservative fixes for ``mycelium doctor --fix``."""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

import yaml

from mycelium.config_schema import CONFIG_VERSION, config_json_schema


@dataclass(frozen=True)
class AppliedFix:
    id: str
    summary: str
    path: Path


def _atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    mode = path.stat().st_mode if path.exists() else None
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, prefix=f".{path.name}.", delete=False
    ) as handle:
        handle.write(text)
        temporary = Path(handle.name)
    if mode is not None:
        os.chmod(temporary, mode)
    temporary.replace(path)


def apply_conservative_fixes(config_path: str | Path) -> list[AppliedFix]:
    """Apply only format metadata and IDE support; never infer runtime policy."""

    path = Path(config_path)
    if not path.is_file():
        return []
    try:
        original = path.read_text(encoding="utf-8")
        parsed = yaml.safe_load(original)
    except (OSError, UnicodeError, yaml.YAMLError):
        return []
    if parsed is None:
        parsed = {}
    if not isinstance(parsed, dict):
        return []
    existing_version = parsed.get("config_version")
    if existing_version not in (None, CONFIG_VERSION):
        return []

    fixes: list[AppliedFix] = []
    additions: list[str] = []
    if "yaml-language-server:" not in original:
        additions.append("# yaml-language-server: $schema=./mycelium.schema.json")
        fixes.append(AppliedFix("config.schema_hint", "added YAML editor schema hint", path))
    if "config_version" not in parsed:
        additions.append(f"config_version: {CONFIG_VERSION}")
        fixes.append(AppliedFix("config.version", "added explicit config format version", path))
    if additions:
        _atomic_write(path, "\n".join(additions) + "\n" + original)

    schema_path = path.with_name("mycelium.schema.json")
    if not schema_path.exists():
        _atomic_write(
            schema_path,
            json.dumps(config_json_schema(), indent=2, sort_keys=True) + "\n",
        )
        fixes.append(AppliedFix("config.schema", "generated JSON Schema for IDEs", schema_path))
    return fixes


__all__ = ["AppliedFix", "apply_conservative_fixes"]
