"""Regenerate checked-in configuration documentation and example files."""

from __future__ import annotations

from pathlib import Path

from mycelium.config_artifacts import render_config_example, render_config_reference

SDK_ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    outputs = {
        SDK_ROOT / "docs" / "CONFIG_REFERENCE.md": render_config_reference(),
        SDK_ROOT / "examples" / "mycelium.generated.example.yaml": render_config_example(),
    }
    for path, content in outputs.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        print(f"Wrote {path.relative_to(SDK_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
