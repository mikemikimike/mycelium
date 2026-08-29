#!/usr/bin/env python3
"""Regenerate the reviewed public API contract used by CI."""

from __future__ import annotations

import json
import sys
from pathlib import Path

SDK_ROOT = Path(__file__).resolve().parents[1]
SNAPSHOT_PATH = SDK_ROOT / "api-snapshot.json"
sys.path.insert(0, str(SDK_ROOT))

import mycelium  # noqa: E402
import mycelium.experimental  # noqa: E402
import mycelium.integrations  # noqa: E402
import mycelium.runtime  # noqa: E402
from mycelium.api_stability import API_NAMESPACES, DEPRECATIONS  # noqa: E402


def build_snapshot() -> dict[str, object]:
    """Return a deterministic representation of every supported namespace."""

    modules = (
        mycelium,
        mycelium.runtime,
        mycelium.integrations,
        mycelium.experimental,
    )
    return {
        "schema_version": 1,
        "namespaces": {
            module.__name__: sorted(module.__all__)
            for module in modules
        },
        "stability": {
            name: metadata.stability
            for name, metadata in sorted(API_NAMESPACES.items())
        },
        "deprecations": {
            name: {
                "since": metadata.since,
                "replacement": metadata.replacement,
                "remove_in": metadata.remove_in,
                "reason": metadata.reason,
            }
            for name, metadata in sorted(DEPRECATIONS.items())
        },
    }


def main() -> int:
    SNAPSHOT_PATH.write_text(
        json.dumps(build_snapshot(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"Wrote {SNAPSHOT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
