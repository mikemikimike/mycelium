"""Compatibility contract for Mycelium's documented import namespaces."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import mycelium
import mycelium.experimental
import mycelium.integrations
import mycelium.runtime
from mycelium.api_stability import (
    API_NAMESPACES,
    DEPRECATIONS,
    ApiDeprecation,
    deprecated,
)

_SNAPSHOT = Path(__file__).resolve().parents[1] / "api-snapshot.json"


def _actual_namespaces() -> dict[str, list[str]]:
    modules = (
        mycelium,
        mycelium.runtime,
        mycelium.integrations,
        mycelium.experimental,
    )
    return {module.__name__: sorted(module.__all__) for module in modules}


def test_public_api_matches_reviewed_snapshot() -> None:
    expected = json.loads(_SNAPSHOT.read_text(encoding="utf-8"))

    assert expected["schema_version"] == 1
    assert _actual_namespaces() == expected["namespaces"], (
        "public API changed; preserve compatibility or deliberately regenerate "
        "sdk/api-snapshot.json with sdk/scripts/update_api_snapshot.py"
    )
    assert {
        name: metadata.stability for name, metadata in API_NAMESPACES.items()
    } == expected["stability"]


def test_runtime_exports_keep_package_root_identity() -> None:
    for name in mycelium.runtime.__all__:
        assert hasattr(mycelium, name), f"historical root export missing: {name}"
        assert getattr(mycelium.runtime, name) is getattr(mycelium, name)


def test_all_snapshot_symbols_are_importable() -> None:
    modules = {
        module.__name__: module
        for module in (
            mycelium,
            mycelium.runtime,
            mycelium.integrations,
            mycelium.experimental,
        )
    }
    expected = json.loads(_SNAPSHOT.read_text(encoding="utf-8"))
    for namespace, names in expected["namespaces"].items():
        for name in names:
            assert hasattr(modules[namespace], name), f"{namespace}.{name} is not importable"


def test_deprecation_metadata_is_complete_and_snapshotted() -> None:
    expected = json.loads(_SNAPSHOT.read_text(encoding="utf-8"))["deprecations"]
    actual = {
        name: {
            "since": metadata.since,
            "replacement": metadata.replacement,
            "remove_in": metadata.remove_in,
            "reason": metadata.reason,
        }
        for name, metadata in DEPRECATIONS.items()
    }
    assert actual == expected
    for symbol, metadata in DEPRECATIONS.items():
        assert symbol == metadata.symbol
        assert metadata.since
        assert metadata.replacement


def test_deprecated_decorator_warns_and_preserves_metadata() -> None:
    metadata = ApiDeprecation(
        symbol="mycelium.old_helper",
        since="1.37.0",
        replacement="mycelium.new_helper",
        remove_in="2.0.0",
    )

    @deprecated(metadata)
    def old_helper(value: int) -> int:
        return value + 1

    with pytest.warns(DeprecationWarning, match="mycelium.new_helper"):
        assert old_helper(1) == 2
    assert old_helper.__name__ == "old_helper"
    assert old_helper.__mycelium_deprecation__ == metadata
