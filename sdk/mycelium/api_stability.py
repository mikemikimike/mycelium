"""Public API stability and deprecation metadata.

Mycelium keeps historical package-root imports working. New code can use the
smaller namespaces described by :data:`API_NAMESPACES`; this module provides
the metadata and warning mechanism needed for deliberate future migrations.
"""

from __future__ import annotations

import warnings
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from functools import wraps
from types import MappingProxyType
from typing import Any, TypeVar


@dataclass(frozen=True)
class ApiNamespace:
    """Stability contract for one documented import namespace."""

    name: str
    stability: str
    audience: str


@dataclass(frozen=True)
class ApiDeprecation:
    """Machine-readable lifecycle metadata for a deprecated public symbol."""

    symbol: str
    since: str
    replacement: str
    remove_in: str | None = None
    reason: str | None = None

    def message(self) -> str:
        text = f"{self.symbol} is deprecated since Mycelium {self.since}"
        if self.remove_in is not None:
            text += f" and is planned for removal in {self.remove_in}"
        text += f"; use {self.replacement} instead"
        if self.reason:
            text += f". {self.reason}"
        return text


API_NAMESPACES: Mapping[str, ApiNamespace] = MappingProxyType(
    {
        "mycelium": ApiNamespace(
            name="mycelium",
            stability="stable",
            audience="Recommended API plus backward-compatible historical exports.",
        ),
        "mycelium.runtime": ApiNamespace(
            name="mycelium.runtime",
            stability="stable",
            audience="Low-level runtime, storage, transition, and operator APIs.",
        ),
        "mycelium.integrations": ApiNamespace(
            name="mycelium.integrations",
            stability="stable",
            audience="Optional framework adapters.",
        ),
        "mycelium.experimental": ApiNamespace(
            name="mycelium.experimental",
            stability="preview",
            audience="Opt-in APIs that may change before promotion to stable.",
        ),
        "mycelium._internal": ApiNamespace(
            name="mycelium._internal",
            stability="internal",
            audience="Implementation details with no compatibility guarantee.",
        ),
    }
)

# Add entries only when an API starts its deprecation period. Keeping this
# registry explicit makes review, docs generation, and compatibility tests
# deterministic. Existing package-root exports are aliases, not deprecated.
DEPRECATIONS: Mapping[str, ApiDeprecation] = MappingProxyType({})


def deprecation_for(symbol: str) -> ApiDeprecation | None:
    """Return lifecycle metadata for ``symbol``, if it is deprecated."""

    return DEPRECATIONS.get(symbol)


def warn_deprecated(symbol: str, *, stacklevel: int = 2) -> None:
    """Emit the standardized warning registered for ``symbol``."""

    metadata = deprecation_for(symbol)
    if metadata is None:
        raise KeyError(f"no deprecation metadata registered for {symbol!r}")
    warnings.warn(metadata.message(), DeprecationWarning, stacklevel=stacklevel)


_CallableT = TypeVar("_CallableT", bound=Callable[..., Any])


def deprecated(metadata: ApiDeprecation) -> Callable[[_CallableT], _CallableT]:
    """Decorate a function with warning behavior and lifecycle metadata."""

    def decorate(func: _CallableT) -> _CallableT:
        @wraps(func)
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            warnings.warn(metadata.message(), DeprecationWarning, stacklevel=2)
            return func(*args, **kwargs)

        setattr(wrapped, "__mycelium_deprecation__", metadata)
        return wrapped  # type: ignore[return-value]

    return decorate


__all__ = [
    "API_NAMESPACES",
    "DEPRECATIONS",
    "ApiDeprecation",
    "ApiNamespace",
    "deprecated",
    "deprecation_for",
    "warn_deprecated",
]
