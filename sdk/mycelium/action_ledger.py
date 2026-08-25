"""ActionLedger: durable action records and idempotency guard."""

from __future__ import annotations

import asyncio
import functools
import hashlib
import inspect
import json
import logging
import os
import socket
import threading
import time
import uuid
import warnings
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator, Mapping
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field, replace
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar

from mycelium.reconcile import Reconciler, ReconcileResult, ReconcileStatus
from mycelium.session import Session, _session_var
from mycelium.storage._helpers import (
    claim_inflight_outcome,
    default_try_claim_inflight,
    lease_allows_renew,
    with_lease,
)
from mycelium.storage.json_file import LockedJsonDictFile
from mycelium.tool_boundary import ToolBoundaryError
from mycelium.transition import (
    CONSEQUENTIAL_SIDE_EFFECT_CLASSES,
    LEDGER_KWARG_KEYS,
    REQUEST_IDENTITY_POLICIES,
    REQUEST_IDENTITY_POLICY_DERIVED,
    REQUEST_IDENTITY_POLICY_REQUIRE_EXPLICIT,
    EffectState,
    LeaseValidity,
    MissingRequestIdentityError,
    SideEffectBoundary,
    SideEffectClass,
    TerminalOutcome,
    ToolCapability,
    ToolTransitionBinding,
    args_fingerprint,
    derive_dispatch_id,
    derive_effect_id_for_call,
    derive_transition_key_for_call,
    extract_provider_idempotency_key,
    get_active_dispatch_id,
    get_active_execution_scope,
    get_active_handoff,
    has_worker_death_evidence,
    legacy_status_from_terminal,
    parse_explicit_request_id,
    request_id_from_argument,
    resolve_effect_state,
    resolve_lease_validity,
    resolve_scope,
    resolve_terminal_outcome,
    should_propagate_effect_id_as_provider_key,
    terminal_from_legacy_status,
)
from mycelium.transition_resolution import (
    TransitionGate,
    hard_block_message,
    repair_transition_fields,
    resolve_read_only_gate,
    resolve_side_effect_gate,
    soft_block_message,
    transition_needs_repair,
)

if TYPE_CHECKING:
    from mycelium.audit_receipt import AuditReceiptEmitter
    from mycelium.operator_auth import OperatorAuthorizer
    from mycelium.outcome_emit import OutcomeEmitter

P = ParamSpec("P")
R = TypeVar("R")

DEFAULT_LEASE_TTL = 3600.0
DEFAULT_POLL_INTERVAL = 0.05
DEFAULT_POLL_TIMEOUT = 300.0
# Renew at 1/3 of lease TTL so a still-running owner stays HELD before peers see EXPIRED.
DEFAULT_LEASE_RENEW_RATIO = 1.0 / 3.0
MIN_LEASE_RENEW_INTERVAL = 0.01
# Default grace window for worker-death evidence: 2x the lease TTL.
DEFAULT_PRESUMED_DEAD_AFTER_RATIO = 2.0

_logger = logging.getLogger(__name__)


class LedgerError(Exception):
    """Raised when the action ledger cannot record or verify an action."""


class LedgerSchemaVersionError(LedgerError):
    """Raised when a durable ledger row uses an invalid or future schema."""


class LedgerPendingError(Exception):
    """Raised when the same request is already in-flight."""


class LedgerPollTimeoutError(LedgerError):
    """Raised when polling an in-flight transition times out."""


class LedgerHardBlockError(LedgerError):
    """Raised when a side-effecting transition requires manual reconciliation."""


class LedgerSoftBlockError(LedgerError):
    """Raised when a reversible (read-only) transition is deferred.

    Signals an ambiguous ``UNKNOWN`` / ``BLOCKED`` outcome on a read-only tool.
    Unlike :class:`LedgerHardBlockError`, re-running the tool is safe, so this
    is a *deferral* the caller may retry later rather than a terminal stop. Only
    raised when the ledger is configured with ``defer_read_only_unknown=True``.
    """


class LedgerReleaseRefusedError(LedgerError):
    """Raised when an operator release is rejected (fail-closed).

    Covers unknown request ids, releasing a ``COMPLETED`` transition, and
    releasing an ``IN_FLIGHT`` transition whose lease is still held (a worker
    may be alive).
    """


class LedgerAlreadyResolvedError(LedgerError):
    """Raised when releasing a transition that already has an operator resolution.

    Release is one-shot: a recorded human verification is never overwritten.
    """


class LedgerOutcomeAlreadySetError(LedgerError):
    """Raised when a terminal-outcome write is refused because the transition
    already has a terminal outcome (the outcome is one-shot).  Analogous to
    HTTP 409 Conflict: a stale worker or late duplicate tried to write an
    outcome after the transition was already resolved elsewhere.

    Pre-upgrade behaviour silently overwrote the true outcome.  This exception
    is the new fail-closed guard.
    """


class LedgerWorkerAliveError(LedgerError):
    """Raised when a worker-death assertion is refused because the worker appears alive.

    Covers ``mark_worker_dead`` on an entry whose ``last_heartbeat_at`` is
    within the grace window, and ``release()`` of an EXPIRED entry whose
    heartbeat is still recent.
    """


class LedgerStorageUnavailableError(LedgerError):
    """Raised when the durable storage backend fails mid-operation.

    Fail-closed contract: storage down during a claim means the tool never
    runs; storage down after the effect (``complete`` / failure recording)
    propagates and leaves the entry ``IN_FLIGHT``, which later resolves via
    lease expiry → ``EXPIRED`` → hard-block/reconcile. The original backend
    exception is preserved as ``__cause__``.
    """


# Verified outcomes accepted by ActionLedger.release().
OPERATOR_RESOLUTION_COMPLETED = "completed"
OPERATOR_RESOLUTION_NOT_EXECUTED = "not_executed"

# Stored terminal-outcome values that resolution paths (release, reconcile)
# will accept from existing entries.  IN_FLIGHT (None) and COMPLETED are missing
# because resolution paths should never see them at write time.
_RESOLUTION_ACCEPTED_STORED_OUTCOMES: frozenset[str] = frozenset(
    {
        TerminalOutcome.IN_FLIGHT.value,
        TerminalOutcome.BLOCKED.value,
        TerminalOutcome.UNKNOWN.value,
        TerminalOutcome.FAILED_AFTER_EFFECT.value,
        TerminalOutcome.FAILED_BEFORE_EFFECT.value,
    }
)

# Stored terminal-outcome values that **the NOT_EXECUTED reset** accepts.
# Excludes ``IN_FLIGHT`` so two reconcilers racing ``NOT_EXECUTED``
# cannot both transition ``IN_FLIGHT → IN_FLIGHT`` — only the first
# writer wins; the second sees ``IN_FLIGHT`` and fails the CAS.
# EXPIRED entries (stored ``IN_FLIGHT`` with expired lease) are advanced
# to ``BLOCKED`` before the CAS (see ``_apply_reconcile_result``).
_RECONCILE_NOT_EXECUTED_OUTCOMES: frozenset[str] = frozenset(
    {
        TerminalOutcome.BLOCKED.value,
        TerminalOutcome.UNKNOWN.value,
        TerminalOutcome.FAILED_AFTER_EFFECT.value,
        TerminalOutcome.FAILED_BEFORE_EFFECT.value,
    }
)

# Opt-in same-key UNKNOWN retry (param + TTL still VALID). claim_inflight
# treats UNKNOWN as non-claimable so peers do not blind-overwrite; this CAS
# is the only authorized reset path after the gate returns ALLOW.
_UNKNOWN_SAME_KEY_RETRY_OUTCOMES: frozenset[str] = frozenset(
    {
        TerminalOutcome.UNKNOWN.value,
    }
)

# Policies for tools ledgered without a transition_binding (unclassified).
# "warn": legacy behavior + a one-time warning when a failed entry is
# reclaimed. "strict": route the claim through claim_side_effecting with a
# conservative synthesized binding so failed retries hard-block — this is
# the write-ahead-ordering-complete path (INTENDED -> ATTEMPTING/ABORTED CAS
# before any body execution), the same protocol every classified
# consequential tool goes through.
#
# ActionLedger.__init__ keeps "warn" as the constructor default for backward
# compatibility: every existing unclassified `claim()` caller (tests and
# hosts) that has not opted in would otherwise silently start hard-blocking
# failed retries. YAML tool templates already default to "strict" for new
# deployments (see mycelium/templates/); `mycelium doctor` accepts "warn" as
# a valid, non-erroring choice. Hosts that want every claim() — classified or
# not — to go through the full effect-commit protocol should pass
# `unclassified_policy="strict"` explicitly. `MyceliumConfig.apply_tool`
# additionally defaults this to "strict" when `profile: production` and
# `action_ledger.unclassified_policy` is omitted.
UNCLASSIFIED_POLICY_WARN = "warn"
UNCLASSIFIED_POLICY_STRICT = "strict"

# Opt-in identity-conflict / args-drift gate (AF-002 Ring 3). Default off
# preserves the intentional contract that same dispatch ticket + different
# args is a new transition (see test_semantic_identity).
ARGS_DRIFT_OFF = "off"
ARGS_DRIFT_SOFT = "soft"
ARGS_DRIFT_HARD = "hard"
ARGS_DRIFT_POLICIES = frozenset({ARGS_DRIFT_OFF, ARGS_DRIFT_SOFT, ARGS_DRIFT_HARD})

# Conservative binding synthesized for "strict" unclassified claims:
# NON_IDEMPOTENT_MUTATE yields MANUAL_RECONCILIATION_REQUIRED + SINGLE_USE
# from the existing class defaults. Request-id derivation stays legacy.
_UNCLASSIFIED_BINDING = ToolTransitionBinding.for_tool(
    agent_id="unclassified",
    policy_version="unclassified",
    side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
)


def _ledger_owner() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"


# Boundary ordering: a transition may only move forward toward CROSSED.
_BOUNDARY_RANK: dict[SideEffectBoundary, int] = {
    SideEffectBoundary.NOT_CROSSED: 0,
    SideEffectBoundary.MAYBE_CROSSED: 1,
    SideEffectBoundary.CROSSED: 2,
}

# Expected terminal outcomes for a wrapper-path transition write (IN_FLIGHT).
_IN_FLIGHT_OUTCOMES: frozenset[str] = frozenset({TerminalOutcome.IN_FLIGHT.value})


# Resolved outcomes that park a transition until a human releases it.
_STUCK_OUTCOMES = frozenset(
    {
        TerminalOutcome.BLOCKED,
        TerminalOutcome.UNKNOWN,
        TerminalOutcome.FAILED_AFTER_EFFECT,
        TerminalOutcome.EXPIRED,
    }
)


def _format_heartbeat_age(entry: LedgerEntry, *, now: float) -> str:
    """Human-readable age of the last heartbeat (or started_at fallback)."""
    ref = entry.last_heartbeat_at if entry.last_heartbeat_at is not None else entry.started_at
    age = now - ref
    if age < 60:
        return f"{int(age)}s ago"
    if age < 3600:
        return f"{int(age // 60)}m ago"
    return f"{int(age // 3600)}h ago"


def _grace_remaining(
    entry: LedgerEntry,
    *,
    now: float,
    presumed_dead_after: float,
) -> str:
    """Human-readable time until the grace window elapses."""
    ref = entry.last_heartbeat_at if entry.last_heartbeat_at is not None else entry.started_at
    remaining = presumed_dead_after - (now - ref)
    if remaining <= 0:
        return "now"
    if remaining < 60:
        return f"{int(remaining)}s"
    if remaining < 3600:
        return f"{int(remaining // 60)}m"
    return f"{int(remaining // 3600)}h"


def _is_stuck_transition(
    entry: LedgerEntry,
    resolved: TerminalOutcome,
    *,
    now: float,
    in_flight_stuck_after: float,
) -> bool:
    """Whether a transition needs operator attention (see list_transitions)."""
    if resolved in _STUCK_OUTCOMES:
        return True
    if resolved == TerminalOutcome.IN_FLIGHT and in_flight_stuck_after > 0:
        return now - entry.started_at > in_flight_stuck_after
    return False


@contextmanager
def _storage_errors(operation: str) -> Iterator[None]:
    """Re-raise backend storage failures as :class:`LedgerStorageUnavailableError`.

    Only wraps exceptions raised by the storage layer itself — ``LedgerError``
    subclasses (policy refusals, hard blocks) pass through unchanged, and tool
    exceptions never reach this boundary (the claim path never runs tool code).
    The backend exception is preserved as ``__cause__``.
    """
    try:
        yield
    except LedgerError:
        raise
    except Exception as exc:
        raise LedgerStorageUnavailableError(
            f"ledger storage unavailable during {operation}: {type(exc).__name__}: {exc}"
        ) from exc


@dataclass(frozen=True)
class _ActiveTransition:
    """The side-effecting transition currently executing on this task/thread."""

    ledger: ActionLedger
    request_id: str
    binding: ToolTransitionBinding | None
    call_kwargs: Mapping[str, Any]
    owner: str | None
    fence: int


_active_transition_var: ContextVar[_ActiveTransition | None] = ContextVar(
    "mycelium_active_transition",
    default=None,
)

# Set when _apply_reconcile_result or _raise_hard_block re-reads the entry and
# finds it already claimed by another thread (CAS-loss or stale snapshot).
# The claim loop checks this flag: if set, an IN_FLIGHT return means "poll",
# not "this thread won the fresh claim".
_reconcile_cas_lost: threading.local = threading.local()

# Set when a claim consumed a NOT_EXECUTED verdict (reconciler NOT_EXECUTED or
# an operator release verified "not_executed") and won the fresh in-flight
# claim. The @ledger wrapper reads this right after the claim to tag the
# resulting tool-body run as an *authorized* re-execution (never a silent
# duplicate). A ContextVar keeps concurrent async tasks isolated.
_outcome_reexec_authorized: ContextVar[bool] = ContextVar(
    "mycelium_outcome_reexec_authorized",
    default=False,
)


def get_active_transition() -> _ActiveTransition | None:
    """Return the transition currently executing in this context, if any."""
    return _active_transition_var.get()


def _advance_active_boundary(boundary: SideEffectBoundary) -> None:
    active = _active_transition_var.get()
    if active is None:
        warnings.warn(
            "side-effect boundary marker used outside a ledgered tool; ignored",
            stacklevel=3,
        )
        return
    active.ledger.advance_boundary(
        active.request_id,
        boundary,
        expected_owner=active.owner,
        expected_fence=active.fence,
    )


def mark_maybe_crossed() -> None:
    """Mark the active transition as ``maybe_crossed``.

    Call immediately before performing the external operation. If the tool
    raises or the process crashes after this point, the durable entry retains
    ``maybe_crossed`` so a redispatch hard-blocks instead of re-executing a
    possibly-already-applied side effect.

    Time-bounded authority and use-time currency are re-validated here
    (use phase) before the boundary advances. Expired authority or a
    stale/changed fact hard-blocks and never marks ``maybe_crossed``.
    """
    from mycelium.use_time_currency import enforce_use_boundary

    active = _active_transition_var.get()
    enforce_use_boundary(kwargs=active.call_kwargs if active is not None else {})
    _advance_active_boundary(SideEffectBoundary.MAYBE_CROSSED)


async def mark_maybe_crossed_async() -> None:
    """Asynchronously validate and mark the active transition as ``maybe_crossed``."""
    from mycelium.use_time_currency import enforce_use_boundary_async

    active = _active_transition_var.get()
    await enforce_use_boundary_async(kwargs=active.call_kwargs if active is not None else {})
    _advance_active_boundary(SideEffectBoundary.MAYBE_CROSSED)


def mark_crossed() -> None:
    """Mark the active transition as ``crossed`` (effect definitely happened)."""
    _advance_active_boundary(SideEffectBoundary.CROSSED)


def record_external_operation(ref: str) -> None:
    """Attach the provider's operation handle to the active transition.

    ``ref`` is the external system's identifier for the effect this call
    produced — a provider id (e.g. Stripe ``pi_...``) or the idempotency key
    sent to the provider. It is stored durably so an ambiguous transition
    (``UNKNOWN`` / ``FAILED_AFTER_EFFECT`` / ``maybe_crossed``) can later be
    reconciled against the provider instead of hard-blocking blindly.

    Record it as early as possible — ideally the idempotency key *before* the
    call, or the returned id immediately after — inside ``side_effect()``.
    """
    active = _active_transition_var.get()
    if active is None:
        warnings.warn(
            "record_external_operation() used outside a ledgered tool; ignored",
            stacklevel=2,
        )
        return
    active.ledger.attach_external_operation_ref(
        active.request_id,
        ref,
        expected_owner=active.owner,
        expected_fence=active.fence,
    )


def renew_lease(*, lease_ttl: float | None = None) -> None:
    """Extend the active transition's execution lease.

    ``@ledger`` / ``@ledger_sync`` already auto-renew while the tool body runs.
    Call this for an extra mid-flight bump, or when driving
    :meth:`ActionLedger.claim_side_effecting` yourself without the decorator.
    Peers still ``POLL`` on a held lease; incomplete durable fields are healed
    via ``ActionLedger.repair_transition``. Lease is resolution metadata (not
    part of ``transition_key``).

    Outside a ledgered tool this is a no-op with a warning.
    """
    active = _active_transition_var.get()
    if active is None:
        warnings.warn(
            "renew_lease() used outside a ledgered tool; ignored",
            stacklevel=2,
        )
        return
    active.ledger.renew_lease(
        active.request_id,
        lease_ttl=lease_ttl,
        _expected_owner=active.owner,
        _expected_fence=active.fence,
    )


def _resolve_lease_renew_interval(
    lease_ttl: float,
    lease_renew_interval: float | None,
) -> float | None:
    """Return seconds between auto-renew ticks, or ``None`` to disable.

    ``lease_renew_interval <= 0`` disables auto-renew. ``None`` means
    ``lease_ttl * DEFAULT_LEASE_RENEW_RATIO`` (floored at
    :data:`MIN_LEASE_RENEW_INTERVAL`). Unbounded leases (``lease_ttl <= 0``)
    never auto-renew.
    """
    if lease_ttl <= 0:
        return None
    if lease_renew_interval is not None:
        if lease_renew_interval <= 0:
            return None
        return lease_renew_interval
    return max(lease_ttl * DEFAULT_LEASE_RENEW_RATIO, MIN_LEASE_RENEW_INTERVAL)


@contextmanager
def _lease_auto_renew(
    ledger: ActionLedger,
    request_id: str,
    *,
    owner: str | None,
    fence: int,
) -> Iterator[None]:
    """Background owner heartbeat while a ledgered tool body executes.

    Keeps ``lease_until`` ahead of wall clock so redispatched peers stay on
    ``POLL`` instead of treating a still-running worker as ``EXPIRED``.
    """
    interval = _resolve_lease_renew_interval(
        ledger._lease_ttl,
        ledger._lease_renew_interval,
    )
    if interval is None:
        yield
        return

    stop = threading.Event()

    def _loop() -> None:
        while not stop.wait(interval):
            try:
                ledger.renew_lease(
                    request_id,
                    lease_ttl=ledger._lease_ttl,
                    _expected_owner=owner,
                    _expected_fence=fence,
                )
            except LedgerError as exc:
                _logger.warning(
                    "lease auto-renew stopped for %s: %s",
                    request_id,
                    exc,
                )
                return
            except Exception:
                _logger.exception(
                    "lease auto-renew failed for %s; will retry",
                    request_id,
                )

    thread = threading.Thread(
        target=_loop,
        name=f"mycelium-lease-renew:{request_id[:16]}",
        daemon=True,
    )
    thread.start()
    try:
        yield
    finally:
        stop.set()
        thread.join(timeout=max(interval, MIN_LEASE_RENEW_INTERVAL) + 1.0)


# LedgerEntry.schema_version. Bumped for the effect-commit protocol
# completion: rows now carry a durable `effect_id` (schema 2). Legacy rows
# missing the field load as schema 1 and infer `effect_id` from `request_id`
# (see LedgerEntry.from_dict) — this is a read-time inference, not a storage
# migration, so old rows keep working unchanged.
LEDGER_ENTRY_SCHEMA_VERSION = 2


def _read_ledger_entry_schema_version(data: Mapping[str, Any]) -> int:
    raw = data.get("schema_version", 1)
    if isinstance(raw, bool):
        raise LedgerSchemaVersionError("ledger schema_version must be an integer >= 1")
    if not isinstance(raw, (int, str)):
        raise LedgerSchemaVersionError(
            f"ledger schema_version must be an integer, got {raw!r}"
        )
    try:
        version = int(raw)
    except (TypeError, ValueError) as exc:
        raise LedgerSchemaVersionError(
            f"ledger schema_version must be an integer, got {raw!r}"
        ) from exc
    if version < 1:
        raise LedgerSchemaVersionError(
            f"ledger schema_version must be >= 1, got {version}"
        )
    if version > LEDGER_ENTRY_SCHEMA_VERSION:
        raise LedgerSchemaVersionError(
            f"ledger schema {version} is newer than this runtime supports "
            f"({LEDGER_ENTRY_SCHEMA_VERSION}); upgrade Mycelium before reading it"
        )
    return version


@contextmanager
def side_effect() -> Iterator[None]:
    """Wrap the external operation of a side-effecting tool.

    On entry the active transition advances to ``maybe_crossed``; on clean exit
    to ``crossed``. If the body raises, the boundary stays ``maybe_crossed`` so
    the failure is classified as ambiguous (``UNKNOWN``) rather than
    ``FAILED_BEFORE_EFFECT``::

        @ledger_sync(transition_binding=binding)
        def send_payment(amount, recipient):
            with side_effect():
                return gateway.charge(amount, recipient)

    Use-time authority expiry is enforced inside :func:`mark_maybe_crossed`
    immediately before the boundary advances — after leases, queues, and
    backoff, and before any provider call.
    """
    mark_maybe_crossed()
    yield
    mark_crossed()


@asynccontextmanager
async def side_effect_async() -> AsyncIterator[None]:
    """Wrap an async tool's external operation with async final validation."""
    await mark_maybe_crossed_async()
    yield
    mark_crossed()


@dataclass(frozen=True)
class LedgerEntry:
    """Immutable record of a single tool invocation."""

    request_id: str
    tool: str
    args: list[Any]
    kwargs: dict[str, Any]
    status: str  # legacy: "in-flight" | "completed" | "failed"
    terminal_outcome: str = TerminalOutcome.IN_FLIGHT.value
    # Kleppmann fencing token. Every successful claim atomically bumps the
    # stored fence; the claimed entry carries it, and every later mutation must
    # match the stored fence or the storage CAS rejects the write. A worker
    # whose claim was superseded holds a stale (lower) fence and is refused at
    # the point of mutation — independent of its own lease clock. Old rows
    # without a fence load as 0.
    fence: int = 0
    result: Any = None
    error: str | None = None
    started_at: float = field(default_factory=time.time)
    finished_at: float | None = None
    lease_until: float | None = None
    owner: str | None = None
    idempotency_key: str | None = None
    receipt_ref: str | None = None
    side_effect_boundary: str = SideEffectBoundary.NOT_CROSSED.value
    external_operation_ref: str | None = None
    provider_idempotency_key: str | None = None
    provider_key_first_attempt_at: float | None = None
    # Operator release (manual reconciliation) audit fields. Set once by
    # ActionLedger.release(); "not_executed" is consumed by the next claim.
    # Worker-death signal fields. ``last_heartbeat_at`` is set on claim and
    # updated by ``renew_lease()``; the auto-renew loop maintains it with no
    # further changes.  ``worker_dead_asserted_*`` is stamped by
    # ``mark_worker_dead()`` / ``mark_worker_dead_for()`` — the channel for
    # orchestrator death events (k8s OOM-kill hooks, LangGraph redispatch
    # sweeps) and humans.
    last_heartbeat_at: float | None = None
    worker_dead_asserted_by: str | None = None
    worker_dead_asserted_at: float | None = None

    # Operator release (manual reconciliation) audit fields. Set once by
    # ActionLedger.release(); "not_executed" is consumed by the next claim.
    operator_resolution: str | None = None  # "completed" | "not_executed"
    resolved_by: str | None = None
    resolution_reason: str | None = None
    resolved_at: float | None = None
    released_from_outcome: str | None = None

    # Optional state-authority / decision pass-through (audit only — enforcement
    # lives in ``state_authority.StateAuthority``, not in claim resolution).
    decision_id: str | None = None
    state_ref: str | None = None

    # Durable record of the single-decision-point evaluation (Change 2). The
    # serialized :class:`mycelium.decision.Decision` — every registered
    # predicate's verdict — stamped atomically with the INTENDED -> ATTEMPTING
    # transition under the same fenced CAS. ``None`` when no decision was
    # recorded (timeless paths, older rows).
    decision: dict[str, Any] | None = None
    # Storage field for the unified WAL intent (mycelium.transition.EffectState).
    # Kept as ``effect_phase`` (not renamed to ``effect_state``) for
    # serialization compatibility with every existing stored row and every
    # existing raw-string comparison in this module; every value this field
    # holds is a member of EffectState. ``terminal_outcome`` remains the
    # legacy read alias (also carries UNKNOWN/BLOCKED/FAILED_* detail).
    # New protocol-gating code must not compare this field directly — call
    # resolved_effect_state() / resolve_effect_state(entry), which correctly
    # folds in UNKNOWN and legacy rows that predate this field.
    effect_phase: str = EffectState.INTENDED.value
    effect_protocol_required: bool = False

    # Stable effect identity (mycelium.transition.derive_effect_id_for_call):
    # deterministic hash of (scope, tool, canonicalized args/kwargs,
    # destination). This is the authoritative dedup identity for consequential
    # tools: storage backends maintain an effect_id -> canonical request_id
    # mapping and claim paths resolve through it before any side-effect write.
    # ``request_id`` remains the physical row key for backward compatibility.
    # Unclassified claim() rows still fall back to request_id.
    effect_id: str | None = None
    # Audit trail of host-supplied request ids that resolved onto this
    # canonical effect row via effect_id dedupe (includes request_id itself).
    request_id_aliases: tuple[str, ...] = ()
    # Schema version for this row's shape. See LEDGER_ENTRY_SCHEMA_VERSION.
    schema_version: int = LEDGER_ENTRY_SCHEMA_VERSION

    # Thin handoff / causation audit (optional). Set via ``handoff_scope`` or
    # kwargs; does not grant capabilities or change claim gates.
    parent_request_id: str | None = None
    handoff_id: str | None = None

    def __post_init__(self) -> None:
        # Match from_dict / claim: durable key defaults to request_id.
        if self.idempotency_key is None:
            object.__setattr__(self, "idempotency_key", self.request_id)
        # effect_id must always be present on a stored row (see field
        # docstring above); tools with no binding to derive it from (the
        # unclassified claim() path) fall back to request_id, same as
        # idempotency_key.
        if self.effect_id is None:
            object.__setattr__(self, "effect_id", self.request_id)
        aliases = tuple(
            str(item) for item in self.request_id_aliases if item is not None and str(item)
        )
        if self.request_id not in aliases:
            aliases = aliases + (self.request_id,)
        object.__setattr__(self, "request_id_aliases", aliases)

    def resolved_terminal_outcome(self, *, now: float | None = None) -> TerminalOutcome:
        return resolve_terminal_outcome(
            self.terminal_outcome,
            lease_until=self.lease_until,
            now=now,
        )

    def resolved_effect_state(self) -> EffectState:
        """Unified WAL intent (mycelium.transition.EffectState) for this row.

        The legacy-safe read path: works for rows written before this field
        existed as well as current rows. Prefer this (or
        :func:`mycelium.transition.resolve_effect_state`) over comparing
        ``effect_phase`` / ``terminal_outcome`` directly.
        """
        return resolve_effect_state(self)

    def lease_validity(self, *, now: float | None = None) -> LeaseValidity:
        """Return whether this entry's execution lease is still held."""
        return resolve_lease_validity(self.lease_until, now=now)

    def is_terminal_completed(self, *, now: float | None = None) -> bool:
        return self.resolved_terminal_outcome(now=now) == TerminalOutcome.COMPLETED

    def is_reclaimable(self, *, now: float | None = None) -> bool:
        outcome = self.resolved_terminal_outcome(now=now)
        return outcome in (
            TerminalOutcome.EXPIRED,
            TerminalOutcome.FAILED_BEFORE_EFFECT,
            TerminalOutcome.FAILED_AFTER_EFFECT,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "request_id": self.request_id,
            "tool": self.tool,
            "args": self.args,
            "kwargs": self.kwargs,
            "status": self.status,
            "terminal_outcome": self.terminal_outcome,
            "fence": self.fence,
            "result": self.result,
            "error": self.error,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "lease_until": self.lease_until,
            "owner": self.owner,
            "idempotency_key": self.idempotency_key,
            "receipt_ref": self.receipt_ref,
            "side_effect_boundary": self.side_effect_boundary,
            "external_operation_ref": self.external_operation_ref,
            "provider_idempotency_key": self.provider_idempotency_key,
            "provider_key_first_attempt_at": self.provider_key_first_attempt_at,
            "last_heartbeat_at": self.last_heartbeat_at,
            "worker_dead_asserted_by": self.worker_dead_asserted_by,
            "worker_dead_asserted_at": self.worker_dead_asserted_at,
            "operator_resolution": self.operator_resolution,
            "resolved_by": self.resolved_by,
            "resolution_reason": self.resolution_reason,
            "resolved_at": self.resolved_at,
            "released_from_outcome": self.released_from_outcome,
            "decision_id": self.decision_id,
            "state_ref": self.state_ref,
            "decision": self.decision,
            "effect_phase": self.effect_phase,
            "effect_protocol_required": self.effect_protocol_required,
            "effect_id": self.effect_id,
            "request_id_aliases": list(self.request_id_aliases),
            "schema_version": self.schema_version,
            "parent_request_id": self.parent_request_id,
            "handoff_id": self.handoff_id,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> LedgerEntry:
        schema_version = _read_ledger_entry_schema_version(data)
        status = str(data["status"])
        lease_until = float(data["lease_until"]) if data.get("lease_until") is not None else None
        terminal_raw = data.get("terminal_outcome")
        if terminal_raw is None:
            terminal_outcome = terminal_from_legacy_status(
                status,
                lease_until=lease_until,
            ).value
        else:
            terminal_outcome = str(terminal_raw)
        request_id = str(data["request_id"])
        return cls(
            request_id=request_id,
            tool=str(data["tool"]),
            args=list(data.get("args") or []),
            kwargs=dict(data.get("kwargs") or {}),
            status=status,
            terminal_outcome=terminal_outcome,
            fence=int(data.get("fence") or 0),
            result=data.get("result"),
            error=data.get("error"),
            started_at=float(data.get("started_at", time.time())),
            finished_at=data.get("finished_at"),
            lease_until=lease_until,
            owner=data.get("owner"),
            idempotency_key=data.get("idempotency_key") or request_id,
            receipt_ref=data.get("receipt_ref"),
            side_effect_boundary=str(
                data.get("side_effect_boundary", SideEffectBoundary.NOT_CROSSED.value)
            ),
            external_operation_ref=data.get("external_operation_ref"),
            provider_idempotency_key=data.get("provider_idempotency_key"),
            provider_key_first_attempt_at=data.get("provider_key_first_attempt_at"),
            last_heartbeat_at=data.get("last_heartbeat_at"),
            worker_dead_asserted_by=data.get("worker_dead_asserted_by"),
            worker_dead_asserted_at=data.get("worker_dead_asserted_at"),
            operator_resolution=data.get("operator_resolution"),
            resolved_by=data.get("resolved_by"),
            resolution_reason=data.get("resolution_reason"),
            resolved_at=data.get("resolved_at"),
            released_from_outcome=data.get("released_from_outcome"),
            decision_id=(str(data["decision_id"]) if data.get("decision_id") is not None else None),
            state_ref=(str(data["state_ref"]) if data.get("state_ref") is not None else None),
            decision=(dict(data["decision"]) if data.get("decision") is not None else None),
            effect_phase=str(data.get("effect_phase") or EffectState.INTENDED.value),
            effect_protocol_required=bool(data.get("effect_protocol_required", False)),
            # Legacy rows (schema 1) have no effect_id: infer it from
            # request_id, which is exactly what it would equal for the
            # (default) derived-request_id path anyway.
            effect_id=str(data.get("effect_id") or request_id),
            request_id_aliases=tuple(
                str(item)
                for item in (data.get("request_id_aliases") or (request_id,))
                if item is not None and str(item)
            ),
            schema_version=schema_version,
            parent_request_id=(
                str(data["parent_request_id"])
                if data.get("parent_request_id") is not None
                else None
            ),
            handoff_id=(str(data["handoff_id"]) if data.get("handoff_id") is not None else None),
        )


def _has_allowed_attempting_decision(entry: LedgerEntry) -> bool:
    """True when an allowed decision was durably recorded at ATTEMPTING.

    Gates on the durable ``decision`` field alone, not on the current
    ``resolve_effect_state``: a row that crashed or was marked UNKNOWN while
    ATTEMPTING keeps its recorded allowed decision and must remain completable
    by the reconciler / operator. ``record_decision`` only stamps a decision
    during the ``INTENDED -> ATTEMPTING | ABORTED`` CAS, so a present, allowed
    decision provably means the row passed the single decision point.
    """
    if entry.decision is None:
        return False
    from mycelium.decision import Decision

    try:
        return Decision.from_dict(entry.decision).allowed
    except (KeyError, TypeError, ValueError):
        return False


class LedgerStorage:
    """Backend interface for durable action ledger records."""

    def get(self, request_id: str) -> LedgerEntry | None:
        """Return the entry for request_id, or None if not found."""
        raise NotImplementedError

    def set(self, entry: LedgerEntry) -> None:
        """Persist entry, replacing any existing entry with the same request_id."""
        raise NotImplementedError

    def try_claim_inflight(
        self,
        entry: LedgerEntry,
        *,
        lease_ttl: float = DEFAULT_LEASE_TTL,
    ) -> tuple[str, LedgerEntry | None]:
        """Atomically claim an in-flight entry.

        Returns ``("claimed", None)``, ``("completed", entry)``, or
        ``("in_flight", entry)``. Redis/Postgres backends override with
        atomic primitives; file storage uses an exclusive lock.
        """
        return default_try_claim_inflight(
            self,
            entry,
            lease_ttl=lease_ttl,
        )

    def try_transition(
        self,
        entry: LedgerEntry,
        *,
        expected_terminal_outcomes: frozenset[str],
        expected_owner: str | None = None,
        require_lease_held_at: float | None = None,
        expected_fence: int | None = None,
        expected_effect_state: str | None = None,
    ) -> bool:
        """Atomically write *entry* only if the stored entry's terminal outcome
        is one of *expected_terminal_outcomes* (and *expected_owner* matches,
        when set).

        When ``expected_fence`` is set, also refuse unless the stored entry's
        fence equals it (Kleppmann fencing — a superseded worker holds a lower
        fence and is rejected here regardless of its lease clock).

        When ``require_lease_held_at`` is set, also refuse if the stored lease
        is already expired at that timestamp (renew path — closes TOCTOU
        between get and write).

        Returns ``True`` when the write succeeds, ``False`` when the pre-condition
        is not met (caller raises ``LedgerOutcomeAlreadySetError``).

        When ``expected_effect_state`` is set, compare the stored
        ``effect_phase`` against that unified ``EffectState`` member string.

        The default implementation performs a get+set (single-process only).
        Override with an atomic compare-and-swap for multi-process backends.
        """
        existing = self.get(entry.request_id)
        if existing is None:
            return False
        if existing.terminal_outcome not in expected_terminal_outcomes:
            return False
        if expected_owner is not None and existing.owner != expected_owner:
            return False
        if expected_fence is not None and existing.fence != expected_fence:
            return False
        if expected_effect_state is not None and existing.effect_phase != expected_effect_state:
            return False
        if require_lease_held_at is not None and not lease_allows_renew(
            existing.lease_until, now=require_lease_held_at
        ):
            return False
        self.set(entry)
        return True

    def list_all(self) -> list[LedgerEntry]:
        """Return all entries. Intended for debugging/auditing only."""
        raise NotImplementedError

    def resolve_request_id(self, effect_id: str) -> str | None:
        """Resolve ``effect_id`` to its canonical ``request_id``.

        Default implementation scans all rows (legacy-safe, deterministic).
        Backends with secondary indexes should override.
        """
        candidates: list[LedgerEntry] = []
        for entry in self.list_all():
            ref = str(getattr(entry, "effect_id", None) or entry.request_id)
            if ref == effect_id:
                candidates.append(entry)
        if not candidates:
            return None
        candidates.sort(
            key=lambda item: (float(getattr(item, "started_at", 0.0) or 0.0), item.request_id)
        )
        return candidates[0].request_id

    def get_by_effect_id(self, effect_id: str) -> LedgerEntry | None:
        """Return the canonical row for ``effect_id``, if present."""
        request_id = self.resolve_request_id(effect_id)
        if request_id is None:
            return None
        return self.get(request_id)


class InMemoryLedgerStorage(LedgerStorage):
    """Default in-memory storage. Survives within the process only.

    Thread-safe via ``_lock`` (``threading.RLock``) so concurrent in-process
    claims and transitions do not lose writes.  Multi-process users must
    choose a durable backend.
    """

    def __init__(self) -> None:
        self._entries: dict[str, LedgerEntry] = {}
        self._effect_index: dict[str, str] = {}
        self._lock = threading.RLock()

    @staticmethod
    def _effect_ref(entry: LedgerEntry) -> str:
        return str(entry.effect_id or entry.request_id)

    def _resolve_effect_locked(self, effect_id: str) -> str | None:
        canonical = self._effect_index.get(effect_id)
        if canonical is not None:
            row = self._entries.get(canonical)
            if row is not None and self._effect_ref(row) == effect_id:
                return canonical
            self._effect_index.pop(effect_id, None)
        candidates = [row for row in self._entries.values() if self._effect_ref(row) == effect_id]
        if not candidates:
            return None
        candidates.sort(
            key=lambda item: (float(getattr(item, "started_at", 0.0) or 0.0), item.request_id)
        )
        canonical = candidates[0].request_id
        self._effect_index[effect_id] = canonical
        return canonical

    def get(self, request_id: str) -> LedgerEntry | None:
        with self._lock:
            return self._entries.get(request_id)

    def set(self, entry: LedgerEntry) -> None:
        with self._lock:
            effect_id = self._effect_ref(entry)
            canonical = self._resolve_effect_locked(effect_id)
            if (
                canonical is not None
                and canonical != entry.request_id
                and entry.request_id not in self._entries
            ):
                return
            self._entries[entry.request_id] = entry
            if canonical is None or canonical == entry.request_id:
                self._effect_index[effect_id] = entry.request_id

    def try_claim_inflight(
        self,
        entry: LedgerEntry,
        *,
        lease_ttl: float = DEFAULT_LEASE_TTL,
    ) -> tuple[str, LedgerEntry | None]:
        with self._lock:
            now = time.time()
            effect_id = self._effect_ref(entry)
            canonical = self._resolve_effect_locked(effect_id)
            active_request_id = canonical or entry.request_id
            existing = self._entries.get(active_request_id)
            outcome = claim_inflight_outcome(existing, now=now)
            if outcome == "completed":
                return "completed", existing
            if outcome == "in_flight":
                return "in_flight", existing
            claim_entry = (
                entry
                if active_request_id == entry.request_id
                else replace(entry, request_id=active_request_id)
            )
            leased = with_lease(claim_entry, now=now, lease_ttl=lease_ttl, prior=existing)
            self._entries[active_request_id] = leased
            self._effect_index[effect_id] = active_request_id
            return "claimed", None

    def try_transition(
        self,
        entry: LedgerEntry,
        *,
        expected_terminal_outcomes: frozenset[str],
        expected_owner: str | None = None,
        require_lease_held_at: float | None = None,
        expected_fence: int | None = None,
        expected_effect_state: str | None = None,
    ) -> bool:
        with self._lock:
            existing = self._entries.get(entry.request_id)
            if existing is None:
                return False
            if existing.terminal_outcome not in expected_terminal_outcomes:
                return False
            if expected_owner is not None and existing.owner != expected_owner:
                return False
            if expected_fence is not None and existing.fence != expected_fence:
                return False
            if expected_effect_state is not None and existing.effect_phase != expected_effect_state:
                return False
            if require_lease_held_at is not None and not lease_allows_renew(
                existing.lease_until, now=require_lease_held_at
            ):
                return False
            # Route the write through set() so subclass hooks / failure
            # injection (and any future durability wrappers) still see it.
            # RLock allows re-entry from set() while we hold the CAS lock.
            self.set(entry)
            return True

    def list_all(self) -> list[LedgerEntry]:
        with self._lock:
            return list(self._entries.values())

    def resolve_request_id(self, effect_id: str) -> str | None:
        with self._lock:
            return self._resolve_effect_locked(effect_id)

    def get_by_effect_id(self, effect_id: str) -> LedgerEntry | None:
        with self._lock:
            request_id = self._resolve_effect_locked(effect_id)
            if request_id is None:
                return None
            return self._entries.get(request_id)


class FileLedgerStorage(LedgerStorage):
    """JSON-file-backed storage with ``fcntl`` + threading locking.

    The ``fcntl`` lock guards across processes; the ``threading.Lock`` guards
    across threads within the same process (``flock`` has process-level
    semantics on macOS/Linux, so multiple threads cannot rely on it alone).
    """

    def __init__(self, path: str | Path) -> None:
        ledger_path = Path(path)
        self._file = LockedJsonDictFile(ledger_path)
        self._effect_index_path = ledger_path.with_suffix(ledger_path.suffix + ".effect-index.json")
        self._lock = threading.Lock()

    @staticmethod
    def _effect_ref_from_raw(raw: dict[str, Any], request_id: str) -> str:
        return str(raw.get("effect_id") or request_id)

    @staticmethod
    def _started_at_from_raw(raw: dict[str, Any]) -> float:
        value = raw.get("started_at")
        try:
            return float(value) if value is not None else 0.0
        except (TypeError, ValueError):
            return 0.0

    def _load_effect_index_unlocked(self) -> dict[str, str]:
        if not self._effect_index_path.exists():
            return {}
        try:
            with self._effect_index_path.open("r", encoding="utf-8") as handle:
                loaded = json.load(handle)
        except (json.JSONDecodeError, OSError):
            return {}
        if not isinstance(loaded, dict):
            return {}
        index: dict[str, str] = {}
        for effect_id, request_id in loaded.items():
            if (
                isinstance(effect_id, str)
                and effect_id
                and isinstance(request_id, str)
                and request_id
            ):
                index[effect_id] = request_id
        return index

    def _save_effect_index_unlocked(self, index: dict[str, str]) -> None:
        tmp = self._effect_index_path.with_suffix(self._effect_index_path.suffix + ".tmp")
        with tmp.open("w", encoding="utf-8") as handle:
            json.dump(index, handle, indent=2, default=str)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, self._effect_index_path)
        try:
            dir_fd = os.open(str(self._effect_index_path.parent), os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(dir_fd)
        except OSError:
            pass
        finally:
            os.close(dir_fd)

    def _resolve_effect_locked(
        self,
        data: dict[str, dict[str, Any]],
        index: dict[str, str],
        effect_id: str,
    ) -> tuple[str | None, bool]:
        dirty = False
        canonical = index.get(effect_id)
        if canonical is not None:
            raw = data.get(canonical)
            if raw is not None and self._effect_ref_from_raw(raw, canonical) == effect_id:
                return canonical, False
            index.pop(effect_id, None)
            dirty = True
        candidates: list[tuple[float, str]] = []
        for request_id, raw in data.items():
            if self._effect_ref_from_raw(raw, request_id) == effect_id:
                candidates.append((self._started_at_from_raw(raw), request_id))
        if not candidates:
            return None, dirty
        candidates.sort(key=lambda item: (item[0], item[1]))
        canonical = candidates[0][1]
        if index.get(effect_id) != canonical:
            index[effect_id] = canonical
            dirty = True
        return canonical, dirty

    def get(self, request_id: str) -> LedgerEntry | None:
        def read(data: dict[str, dict[str, Any]]) -> LedgerEntry | None:
            raw = data.get(request_id)
            if raw is None:
                return None
            return LedgerEntry.from_dict(raw)

        with self._lock:
            return self._file.read_modify_write_no_save(read)

    def set(self, entry: LedgerEntry) -> None:
        def mutate(data: dict[str, dict[str, Any]]) -> None:
            index = self._load_effect_index_unlocked()
            effect_id = str(entry.effect_id or entry.request_id)
            canonical, dirty = self._resolve_effect_locked(data, index, effect_id)
            if (
                canonical is not None
                and canonical != entry.request_id
                and entry.request_id not in data
            ):
                if dirty:
                    self._save_effect_index_unlocked(index)
                return
            data[entry.request_id] = entry.to_dict()
            if canonical is None or canonical == entry.request_id:
                if index.get(effect_id) != entry.request_id:
                    index[effect_id] = entry.request_id
                    dirty = True
            if dirty:
                self._save_effect_index_unlocked(index)

        with self._lock:
            self._file.read_modify_write(mutate)

    def try_claim_inflight(
        self,
        entry: LedgerEntry,
        *,
        lease_ttl: float = DEFAULT_LEASE_TTL,
    ) -> tuple[str, LedgerEntry | None]:
        outcome: list[tuple[str, LedgerEntry | None]] = []

        def mutate(data: dict[str, dict[str, Any]]) -> None:
            index = self._load_effect_index_unlocked()
            effect_id = str(entry.effect_id or entry.request_id)
            canonical, dirty = self._resolve_effect_locked(data, index, effect_id)
            active_request_id = canonical or entry.request_id
            raw = data.get(active_request_id)
            existing = LedgerEntry.from_dict(raw) if raw is not None else None
            now = time.time()
            result = claim_inflight_outcome(existing, now=now)
            if result == "completed":
                if dirty:
                    self._save_effect_index_unlocked(index)
                outcome.append(("completed", existing))
                return
            if result == "in_flight":
                if dirty:
                    self._save_effect_index_unlocked(index)
                outcome.append(("in_flight", existing))
                return
            claim_entry = (
                entry
                if active_request_id == entry.request_id
                else replace(entry, request_id=active_request_id)
            )
            leased = with_lease(claim_entry, now=now, lease_ttl=lease_ttl, prior=existing)
            data[active_request_id] = leased.to_dict()
            if index.get(effect_id) != active_request_id:
                index[effect_id] = active_request_id
                dirty = True
            if dirty:
                self._save_effect_index_unlocked(index)
            outcome.append(("claimed", None))

        with self._lock:
            self._file.read_modify_write(mutate)
        return outcome[0]

    def try_transition(
        self,
        entry: LedgerEntry,
        *,
        expected_terminal_outcomes: frozenset[str],
        expected_owner: str | None = None,
        require_lease_held_at: float | None = None,
        expected_fence: int | None = None,
        expected_effect_state: str | None = None,
    ) -> bool:
        result: list[bool] = []

        def mutate(data: dict[str, dict[str, Any]]) -> None:
            index = self._load_effect_index_unlocked()
            dirty = False
            raw = data.get(entry.request_id)
            if raw is None:
                result.append(False)
                return
            existing = LedgerEntry.from_dict(raw)
            if existing.terminal_outcome not in expected_terminal_outcomes:
                result.append(False)
                return
            if expected_owner is not None and existing.owner != expected_owner:
                result.append(False)
                return
            if expected_fence is not None and existing.fence != expected_fence:
                result.append(False)
                return
            if expected_effect_state is not None and existing.effect_phase != expected_effect_state:
                result.append(False)
                return
            if require_lease_held_at is not None and not lease_allows_renew(
                existing.lease_until, now=require_lease_held_at
            ):
                result.append(False)
                return
            data[entry.request_id] = entry.to_dict()
            effect_id = str(entry.effect_id or entry.request_id)
            canonical, canonical_dirty = self._resolve_effect_locked(data, index, effect_id)
            dirty = dirty or canonical_dirty
            if canonical is None or canonical == entry.request_id:
                if index.get(effect_id) != entry.request_id:
                    index[effect_id] = entry.request_id
                    dirty = True
            if dirty:
                self._save_effect_index_unlocked(index)
            result.append(True)

        with self._lock:
            self._file.read_modify_write(mutate)
        return result[0]

    def list_all(self) -> list[LedgerEntry]:
        def read(data: dict[str, dict[str, Any]]) -> list[LedgerEntry]:
            return [LedgerEntry.from_dict(raw) for raw in data.values()]

        with self._lock:
            return self._file.read_modify_write_no_save(read)

    def resolve_request_id(self, effect_id: str) -> str | None:
        def read(data: dict[str, dict[str, Any]]) -> str | None:
            index = self._load_effect_index_unlocked()
            canonical, dirty = self._resolve_effect_locked(data, index, effect_id)
            if dirty:
                self._save_effect_index_unlocked(index)
            return canonical

        with self._lock:
            return self._file.read_modify_write_no_save(read)

    def get_by_effect_id(self, effect_id: str) -> LedgerEntry | None:
        def read(data: dict[str, dict[str, Any]]) -> LedgerEntry | None:
            index = self._load_effect_index_unlocked()
            canonical, dirty = self._resolve_effect_locked(data, index, effect_id)
            if dirty:
                self._save_effect_index_unlocked(index)
            if canonical is None:
                return None
            raw = data.get(canonical)
            if raw is None:
                return None
            return LedgerEntry.from_dict(raw)

        with self._lock:
            return self._file.read_modify_write_no_save(read)


class ActionLedger:
    """Durable ledger of tool invocations for idempotency and audit."""

    def __init__(
        self,
        storage: LedgerStorage | None = None,
        *,
        lease_ttl: float = DEFAULT_LEASE_TTL,
        lease_renew_interval: float | None = None,
        poll_interval: float = DEFAULT_POLL_INTERVAL,
        poll_timeout: float | None = DEFAULT_POLL_TIMEOUT,
        reconciler: Reconciler | None = None,
        defer_read_only_unknown: bool = False,
        audit_emitter: AuditReceiptEmitter | None = None,
        outcome_emitter: OutcomeEmitter | None = None,
        operator_authorizer: OperatorAuthorizer | None = None,
        unclassified_policy: str = UNCLASSIFIED_POLICY_WARN,
        on_args_drift: str = ARGS_DRIFT_SOFT,
        reclaim_requires_death_signal: bool = False,
        presumed_dead_after: float | None = None,
        request_identity_policy: str = REQUEST_IDENTITY_POLICY_DERIVED,
    ) -> None:
        self._storage = storage if storage is not None else InMemoryLedgerStorage()
        self._lease_ttl = lease_ttl
        # None → renew at lease_ttl/3 while @ledger tool bodies run; <=0 disables.
        self._lease_renew_interval = lease_renew_interval
        self._poll_interval = poll_interval
        self._poll_timeout = poll_timeout
        self._reconciler = reconciler
        # Read-only UNKNOWN/BLOCKED gate resolution: when False (default) the
        # ambiguous state is safely re-run (SOFT_BLOCK -> retry); when True the
        # claim raises LedgerSoftBlockError so the caller can defer the retry.
        self._defer_read_only_unknown = defer_read_only_unknown
        # Optional receipt sink for operator releases (release() emits here).
        self._audit_emitter = audit_emitter
        # Optional resolution-telemetry sink (see mycelium.outcome_emit).
        self._outcome_emitter = outcome_emitter
        # Optional host policy for authenticating and authorizing releases.
        # None preserves the documented legacy honesty model.
        self._operator_authorizer = operator_authorizer
        if unclassified_policy not in (
            UNCLASSIFIED_POLICY_WARN,
            UNCLASSIFIED_POLICY_STRICT,
        ):
            raise ValueError(
                f"unclassified_policy must be {UNCLASSIFIED_POLICY_WARN!r} or "
                f"{UNCLASSIFIED_POLICY_STRICT!r}, got {unclassified_policy!r}"
            )
        # Policy for claims without a transition_binding (unclassified tools).
        self._unclassified_policy = unclassified_policy
        if on_args_drift not in ARGS_DRIFT_POLICIES:
            raise ValueError(
                f"on_args_drift must be one of {sorted(ARGS_DRIFT_POLICIES)}, got {on_args_drift!r}"
            )
        # Default soft: same dispatch ticket (request_id / tool_call_id) with
        # different tool args → ToolBoundaryError (hard → LedgerHardBlockError;
        # off restores the old "new args = new transition" escape hatch).
        # Default off: same ticket + different args remains a new transition.
        self._on_args_drift = on_args_drift
        self._memory_warned_tools: set[str] = set()
        self._unclassified_warned_tools: set[str] = set()
        # Worker-death signal: when True, EXPIRED entries cannot be reclaimed
        # without affirmative death evidence (mark_worker_dead or heartbeat
        # older than presumed_dead_after). Default False for backward compat;
        # mycelium init scaffolds True — enable in production.
        self._reclaim_requires_death_signal = reclaim_requires_death_signal
        # Grace window: seconds since last heartbeat (or started_at) after
        # which a worker is presumed dead. Default 2x lease_ttl.
        self._presumed_dead_after = (
            presumed_dead_after
            if presumed_dead_after is not None
            else lease_ttl * DEFAULT_PRESUMED_DEAD_AFTER_RATIO
        )
        if request_identity_policy not in REQUEST_IDENTITY_POLICIES:
            raise ValueError(
                "request_identity_policy must be "
                f"{REQUEST_IDENTITY_POLICY_DERIVED!r} or "
                f"{REQUEST_IDENTITY_POLICY_REQUIRE_EXPLICIT!r}, "
                f"got {request_identity_policy!r}"
            )
        self._request_identity_policy = request_identity_policy

    # --- storage boundary (fail-closed; see LedgerStorageUnavailableError) ---

    def _get_entry(self, request_id: str) -> LedgerEntry | None:
        with _storage_errors("get"):
            return self._storage.get(request_id)

    def _set_entry(self, entry: LedgerEntry) -> None:
        with _storage_errors("set"):
            self._storage.set(entry)

    def _try_claim_inflight(
        self,
        entry: LedgerEntry,
        *,
        lease_ttl: float,
    ) -> tuple[str, LedgerEntry | None]:
        with _storage_errors("try_claim_inflight"):
            return self._storage.try_claim_inflight(entry, lease_ttl=lease_ttl)

    def _try_transition(
        self,
        entry: LedgerEntry,
        *,
        expected_from: frozenset[str] | None = None,
        expected_owner: str | None = None,
        require_lease_held_at: float | None = None,
        expected_fence: int | None = None,
        expected_effect_state: str | None = None,
    ) -> bool:
        """Atomically write *entry* subject to outcome/owner/fence pre-conditions.

        Returns ``True`` on success, ``False`` when the stored entry's
        terminal outcome is not in *expected_from* (or owner / fence mismatch).
        The caller raises ``LedgerOutcomeAlreadySetError`` on ``False``.
        """
        outcomes = expected_from if expected_from is not None else _IN_FLIGHT_OUTCOMES
        fence = entry.fence if expected_fence is None else expected_fence
        with _storage_errors("try_transition"):
            return self._storage.try_transition(
                entry,
                expected_terminal_outcomes=outcomes,
                expected_owner=expected_owner,
                require_lease_held_at=require_lease_held_at,
                expected_fence=fence,
                expected_effect_state=expected_effect_state,
            )

    def _list_all_entries(self) -> list[LedgerEntry]:
        with _storage_errors("list_all"):
            return self._storage.list_all()

    def _resolve_request_id_for_effect(self, effect_id: str) -> str | None:
        with _storage_errors("resolve_request_id"):
            return self._storage.resolve_request_id(effect_id)

    def _get_entry_by_effect_id(self, effect_id: str) -> LedgerEntry | None:
        with _storage_errors("get_by_effect_id"):
            return self._storage.get_by_effect_id(effect_id)

    def _enforce_args_drift(
        self,
        tool: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        *,
        request_id: str,
        existing: LedgerEntry | None,
        binding: ToolTransitionBinding | None = None,
        incoming_request_id: str | None = None,
    ) -> None:
        """Block when the same dispatch ticket is reused with different args.

        Default ``on_args_drift="soft"`` refuses the second body (pitch:
        corrupted upstream args must not double-execute). ``off`` is an
        explicit escape hatch; ``hard`` freezes for a human.

        1. Same storage key (``request_id``) with a prior entry whose
           ``args_fingerprint`` differs → conflict.
        2. Same ``tool_call_id`` / ``request_id`` dispatch ticket under a
           *different* transition key (args are in the key) → conflict,
           but only within the same run isolation scope (``run_id``, else
           ``thread_id``). Other runs are ignored.

        Provider idempotency-key kwargs are excluded from the fingerprint
        (same as transition-key derivation) so the dedicated provider-key
        gate can still hard-block a key mismatch.

        Soft raises :class:`ToolBoundaryError`; hard raises
        :class:`LedgerHardBlockError`.

        An explicit host ``request_id`` is the transition identity. Reusing it
        with a different tool, scope, or meaningful arguments is always
        fail-closed (``off`` does not dual-execute that ticket).
        """
        exclude = _args_drift_exclude_keys(binding)
        incoming_fp = _args_drift_fingerprint(args, kwargs, exclude=exclude)
        conflict: LedgerEntry | None = None
        explicit = None
        try:
            explicit = parse_explicit_request_id(kwargs)
        except ValueError:
            explicit = None

        alias_redispatch = (
            incoming_request_id is not None
            and incoming_request_id != request_id
            and existing is not None
        )
        if existing is not None:
            stored_fp = _args_drift_fingerprint(
                tuple(existing.args), dict(existing.kwargs), exclude=exclude
            )
            if alias_redispatch:
                alias_kwargs = {
                    key: value for key, value in kwargs.items() if key != "request_id"
                }
                alias_fp = _args_drift_fingerprint(args, alias_kwargs, exclude=exclude)
                stored_alias_kwargs = {
                    key: value
                    for key, value in dict(existing.kwargs).items()
                    if key != "request_id"
                }
                stored_alias_fp = _args_drift_fingerprint(
                    tuple(existing.args), stored_alias_kwargs, exclude=exclude
                )
                if (
                    existing.tool != tool
                    or (
                        not alias_redispatch
                        and _identity_scopes_differ(existing, kwargs, binding)
                    )
                    or stored_alias_fp != alias_fp
                ):
                    self._raise_identity_conflict(
                        tool,
                        request_id=incoming_request_id,
                        conflict=existing,
                    )
            elif explicit is not None and (
                existing.tool != tool
                or _identity_scopes_differ(existing, kwargs, binding)
                or stored_fp != incoming_fp
            ):
                # Host-owned request_id is the identity: mismatch is
                # fail-closed even when on_args_drift is off.
                self._raise_identity_conflict(tool, request_id=request_id, conflict=existing)
            elif stored_fp != incoming_fp:
                conflict = existing

        if self._on_args_drift == ARGS_DRIFT_OFF:
            return

        if conflict is None:
            dispatch_id = derive_dispatch_id(kwargs)
            if dispatch_id is not None:
                incoming_scope = _args_drift_scope_key(kwargs)
                for entry in self._list_all_entries():
                    if entry.request_id == request_id:
                        continue
                    if entry.tool != tool:
                        continue
                    entry_kwargs = dict(entry.kwargs)
                    if not _args_drift_scopes_match(
                        incoming_scope, _args_drift_scope_key(entry_kwargs)
                    ):
                        continue
                    entry_dispatch = derive_dispatch_id(entry_kwargs)
                    if entry_dispatch != dispatch_id:
                        continue
                    stored_fp = _args_drift_fingerprint(
                        tuple(entry.args), entry_kwargs, exclude=exclude
                    )
                    if stored_fp != incoming_fp:
                        conflict = entry
                        break

        if conflict is None:
            return

        self._raise_identity_conflict(tool, request_id=request_id, conflict=conflict)

    def _raise_identity_conflict(
        self,
        tool: str,
        *,
        request_id: str,
        conflict: LedgerEntry,
    ) -> None:
        message = (
            f"Args drift / identity conflict for tool {tool!r}: dispatch ticket "
            f"already recorded with a different tool, scope, or arguments "
            f"(prior request_id={conflict.request_id!r}, "
            f"incoming request_id={request_id!r}, prior tool={conflict.tool!r}). "
            f"Mint a new request_id / tool_call_id for a genuinely new intent."
        )
        if self._on_args_drift == ARGS_DRIFT_HARD:
            raise LedgerHardBlockError(message)
        raise ToolBoundaryError(
            message,
            violation="args_drift",
            tool_name=tool,
            llm_message=(
                f"Identity conflict: {tool!r} was already claimed with a different "
                "tool, scope, or arguments for this dispatch ticket. Mint a new "
                "request_id / tool_call_id for a new intent, or reuse the original "
                "identity. The tool body was not executed."
            ),
            recovery_hint=(
                "Reuse the original tool, scope, and arguments for this ticket, "
                "or issue a new request_id / tool_call_id."
            ),
        )

    # --- resolution telemetry (opt-in; never raises, never disturbs the path) ---

    def _emit_outcome(
        self,
        *,
        request_id: str,
        tool: str,
        event: str,
        gate: str | None = None,
        terminal_outcome: TerminalOutcome | None = None,
        boundary: SideEffectBoundary | None = None,
        side_effect_class: SideEffectClass | None = None,
        tool_body_executed: bool = False,
        dispatch_attempt: int | None = None,
        authorized_reexec: bool = False,
        owner: str | None = None,
        error_class: str | None = None,
        policy_version: str | None = None,
    ) -> None:
        """Emit one outcome row, backfilling state from the stored entry.

        Fail-closed emitters re-raise; warn-mode emitters log and swallow
        so telemetry cannot alter claim/CAS/reconcile semantics.
        """
        if self._outcome_emitter is None:
            return
        try:
            entry = self.get(request_id)
        except Exception:
            entry = None
        run_id: str | None = None
        external_operation_ref: str | None = None
        resolution_reason: str | None = None
        parent_request_id: str | None = None
        handoff_id: str | None = None
        if entry is not None:
            if terminal_outcome is None:
                terminal_outcome = entry.resolved_terminal_outcome()
            if boundary is None:
                boundary = SideEffectBoundary(entry.side_effect_boundary)
            stored_kwargs = dict(entry.kwargs or {})
            raw_run = stored_kwargs.get("run_id")
            run_id = str(raw_run) if raw_run else None
            external_operation_ref = entry.external_operation_ref
            resolution_reason = entry.resolution_reason
            parent_request_id = entry.parent_request_id
            handoff_id = entry.handoff_id
        if not run_id:
            scope = get_active_execution_scope()
            if scope is not None and scope.run_id:
                run_id = scope.run_id
        try:
            self._outcome_emitter.emit_event(
                tool=tool,
                request_id=request_id,
                event=event,
                gate=gate,
                terminal_outcome=(terminal_outcome.value if terminal_outcome is not None else None),
                side_effect_boundary=boundary.value if boundary is not None else None,
                side_effect_class=(
                    side_effect_class.value if side_effect_class is not None else None
                ),
                tool_body_executed=tool_body_executed,
                dispatch_attempt=dispatch_attempt,
                authorized_reexec=authorized_reexec,
                owner=owner,
                error_class=error_class,
                run_id=run_id,
                policy_version=policy_version,
                external_operation_ref=external_operation_ref,
                resolution_reason=resolution_reason,
                parent_request_id=parent_request_id,
                handoff_id=handoff_id,
            )
        except Exception:
            if getattr(self._outcome_emitter, "fail_closed", False):
                raise
            _logger.exception("failed to emit outcome row for %s", request_id)

    # --- one-time operator warnings ---

    def _warn_if_volatile_side_effect_storage(
        self,
        tool: str,
        binding: ToolTransitionBinding,
    ) -> None:
        """Warn once per (ledger, tool) when a side-effecting claim uses memory.

        Memory is the legitimate dev/demo backend, so this is a warning, not
        an error — but the no-duplicate-side-effects guarantee only holds
        within the process while claims live in ``InMemoryLedgerStorage``.
        """
        if binding.side_effect_class == SideEffectClass.READ:
            return
        if not isinstance(self._storage, InMemoryLedgerStorage):
            return
        if tool in self._memory_warned_tools:
            return
        self._memory_warned_tools.add(tool)
        warnings.warn(
            f"Tool {tool!r} is side-effecting ({binding.side_effect_class.value}) "
            "but its ActionLedger uses InMemoryLedgerStorage: claims are not "
            "durable across processes or restarts, so the duplicate-side-effect "
            "guard only holds within this process. Use file/sqlite/redis/postgres "
            "storage beyond local dev/demo.",
            stacklevel=3,
        )

    def _warn_unclassified_retry(self, tool: str, existing: LedgerEntry | None) -> None:
        """Warn once per tool before a binding-less claim reclaims a failed entry.

        Without a ``transition_binding`` Mycelium cannot know whether the tool
        has side effects, so the legacy claim path reclaims failed entries —
        which may duplicate an external effect. Set
        ``unclassified_policy="strict"`` to hard-block these retries instead.
        """
        if existing is None or tool in self._unclassified_warned_tools:
            return
        if existing.resolved_terminal_outcome() not in (
            TerminalOutcome.FAILED_BEFORE_EFFECT,
            TerminalOutcome.FAILED_AFTER_EFFECT,
        ):
            return
        self._unclassified_warned_tools.add(tool)
        warnings.warn(
            f"Tool {tool!r} was ledgered without a transition_binding, so "
            "Mycelium cannot know whether it has side effects — retrying its "
            "previously-failed claim may duplicate an external effect. Declare "
            "side_effect_class / a transition_binding, or set "
            "unclassified_policy='strict' to hard-block failed retries.",
            stacklevel=4,
        )

    # --- public API ---

    def get(self, request_id: str) -> LedgerEntry | None:
        return self._get_entry(request_id)

    def list_transitions(
        self,
        *,
        stuck: bool = False,
        tool: str | None = None,
        outcome: TerminalOutcome | None = None,
        parent_request_id: str | None = None,
        in_flight_stuck_after: float = DEFAULT_LEASE_TTL,
    ) -> list[LedgerEntry]:
        """List ledger entries for operator triage (read-only).

        ``stuck=True`` keeps transitions that need a human: resolved terminal
        outcome ``BLOCKED`` / ``UNKNOWN`` / ``FAILED_AFTER_EFFECT`` /
        ``EXPIRED``, plus ``IN_FLIGHT`` entries older than
        ``in_flight_stuck_after`` seconds (an in-flight entry whose lease can
        never expire — e.g. unbounded — would otherwise be invisible forever).
        ``tool`` filters by tool name; ``outcome`` filters by the resolved
        terminal outcome (lease validity applied). ``parent_request_id`` keeps
        children of a handoff parent (thin causation audit). Sorted oldest first.
        """
        now = time.time()
        entries: list[LedgerEntry] = []
        for entry in self._list_all_entries():
            if tool is not None and entry.tool != tool:
                continue
            if parent_request_id is not None and entry.parent_request_id != parent_request_id:
                continue
            resolved = entry.resolved_terminal_outcome(now=now)
            if outcome is not None and resolved != outcome:
                continue
            if stuck and not _is_stuck_transition(
                entry,
                resolved,
                now=now,
                in_flight_stuck_after=in_flight_stuck_after,
            ):
                continue
            entries.append(entry)
        entries.sort(key=lambda entry: entry.started_at)
        return entries

    def wait_for_transition(
        self,
        request_id: str,
        *,
        poll_interval: float | None = None,
        poll_timeout: float | None = None,
    ) -> LedgerEntry:
        """Block until ``request_id`` leaves ``IN_FLIGHT`` (sync peer wait).

        DX helper for custom claim/redispatch loops. Decorator claim paths
        already poll; use this when coordinating outside ``@ledger`` /
        ``@ledger_sync``. Does not reclaim, reconcile, or mark ``UNKNOWN`` on
        timeout — callers own the next gate. Raises
        :class:`LedgerError` if the entry is missing;
        :class:`LedgerPollTimeoutError` if still in-flight at deadline.
        """
        interval = self._poll_interval if poll_interval is None else poll_interval
        timeout = self._poll_timeout if poll_timeout is None else poll_timeout
        poll_deadline = time.time() + timeout if timeout is not None else None
        while True:
            current = self.get(request_id)
            if current is None:
                raise LedgerError(f"Cannot wait for unknown request {request_id!r}")
            outcome = current.resolved_terminal_outcome()
            if outcome != TerminalOutcome.IN_FLIGHT:
                return current
            if poll_deadline is not None and time.time() >= poll_deadline:
                raise LedgerPollTimeoutError(f"Timed out waiting for request {request_id!r}")
            time.sleep(interval)

    async def wait_for_transition_async(
        self,
        request_id: str,
        *,
        poll_interval: float | None = None,
        poll_timeout: float | None = None,
    ) -> LedgerEntry:
        """Async peer wait for LangGraph-style redispatches (``asyncio.sleep``).

        Same semantics as :meth:`wait_for_transition`, but does not block the
        event loop. Prefer this from async tools / custom nodes when a peer
        already holds the lease and you want the terminal entry without
        re-claiming.
        """
        interval = self._poll_interval if poll_interval is None else poll_interval
        timeout = self._poll_timeout if poll_timeout is None else poll_timeout
        poll_deadline = time.time() + timeout if timeout is not None else None
        while True:
            current = self.get(request_id)
            if current is None:
                raise LedgerError(f"Cannot wait for unknown request {request_id!r}")
            outcome = current.resolved_terminal_outcome()
            if outcome != TerminalOutcome.IN_FLIGHT:
                return current
            if poll_deadline is not None and time.time() >= poll_deadline:
                raise LedgerPollTimeoutError(f"Timed out waiting for request {request_id!r}")
            await asyncio.sleep(interval)

    def release(
        self,
        request_id: str,
        *,
        verified: str,
        result: Any = None,
        by: str,
        reason: str,
        credential: str | None = None,
    ) -> LedgerEntry:
        """Record a human verification that releases a hard-blocked transition.

        This is a *recorded verification*, not an unblock: the operator must
        first check the external provider (via ``external_operation_ref`` /
        ``provider_idempotency_key`` on the entry) and attest to one of two
        verified outcomes:

        - ``verified="completed"`` — the effect happened. The transition is
          marked completed with ``result``; the next redispatch returns it
          without re-executing.
        - ``verified="not_executed"`` — the effect provably never happened.
          Only the resolution is stamped here; the next claim consumes it and
          grants exactly one re-execution (one-shot).

        Fail-closed (typed exceptions): unknown request, already-resolved
        entry (one-shot, never overwritten), already-``COMPLETED`` transition,
        and ``IN_FLIGHT`` with a still-held lease are all refused. Entries are
        never deleted — the release is stamped on the durable record so
        ``provider_idempotency_key`` enforcement and audit history survive.
        """
        if verified not in (
            OPERATOR_RESOLUTION_COMPLETED,
            OPERATOR_RESOLUTION_NOT_EXECUTED,
        ):
            raise LedgerReleaseRefusedError(
                f"verified must be {OPERATOR_RESOLUTION_COMPLETED!r} or "
                f"{OPERATOR_RESOLUTION_NOT_EXECUTED!r}, got {verified!r}"
            )
        if not by:
            raise LedgerReleaseRefusedError("release requires an operator identity ('by')")
        if not reason:
            raise LedgerReleaseRefusedError("release requires a reason")
        existing = self._get_entry(request_id)
        if existing is None:
            raise LedgerReleaseRefusedError(f"Cannot release unknown request {request_id!r}")
        if self._operator_authorizer is not None:
            from mycelium.operator_auth import OperatorReleaseRequest

            authorization = OperatorReleaseRequest(
                operator_id=by,
                request_id=request_id,
                tool=existing.tool,
                verified=verified,
            )
            try:
                allowed = self._operator_authorizer.authorize_release(
                    authorization,
                    credential=credential,
                )
            except Exception as exc:
                raise LedgerReleaseRefusedError("operator authorization failed closed") from exc
            if not allowed:
                raise LedgerReleaseRefusedError(
                    f"operator {by!r} is not authorized to release request {request_id!r}"
                )
        if existing.operator_resolution is not None:
            raise LedgerAlreadyResolvedError(
                f"Request {request_id!r} already has an operator resolution "
                f"({existing.operator_resolution!r} by {existing.resolved_by!r}); "
                "release is one-shot"
            )
        now = time.time()
        outcome = existing.resolved_terminal_outcome(now=now)
        if outcome == TerminalOutcome.COMPLETED:
            raise LedgerReleaseRefusedError(
                f"Cannot release request {request_id!r}: already COMPLETED"
            )
        if outcome == TerminalOutcome.IN_FLIGHT:
            # Resolved IN_FLIGHT means the lease is HELD or UNBOUNDED (an
            # expired lease resolves to EXPIRED). A worker may still be alive.
            raise LedgerReleaseRefusedError(
                f"Cannot release request {request_id!r}: IN_FLIGHT with a "
                f"{existing.lease_validity(now=now).value} lease — wait for "
                "the lease to expire (EXPIRED is releasable)"
            )
        if outcome == TerminalOutcome.EXPIRED:
            # EXPIRED with a recent heartbeat means the worker may still be
            # alive (GC pause, storage partition, silently failing auto-renew).
            # When reclaim_requires_death_signal is on, refuse until the grace
            # window elapses or death is asserted.
            if self._reclaim_requires_death_signal and not has_worker_death_evidence(
                existing,
                now=now,
                presumed_dead_after=self._presumed_dead_after,
            ):
                grace = _grace_remaining(
                    existing,
                    now=now,
                    presumed_dead_after=self._presumed_dead_after,
                )
                raise LedgerWorkerAliveError(
                    f"Cannot release request {request_id!r}: EXPIRED but "
                    f"worker appears alive "
                    f"({_format_heartbeat_age(existing, now=now)}) — "
                    f"grace window elapses in {grace}. "
                    "Use mark_worker_dead() first, or wait for the grace window."
                )
        if verified == OPERATOR_RESOLUTION_COMPLETED:
            if existing.effect_protocol_required and not _has_allowed_attempting_decision(
                existing
            ):
                raise LedgerReleaseRefusedError(
                    f"Cannot release request {request_id!r} as completed: "
                    "no allowed durable ATTEMPTING decision"
                )
            entry = replace(
                existing,
                status=legacy_status_from_terminal(TerminalOutcome.COMPLETED),
                terminal_outcome=TerminalOutcome.COMPLETED.value,
                result=_evidence_value(result),
                finished_at=now,
                lease_until=None,
                side_effect_boundary=SideEffectBoundary.CROSSED.value,
                effect_phase=EffectState.COMMITTED.value,
                operator_resolution=OPERATOR_RESOLUTION_COMPLETED,
                resolved_by=by,
                resolution_reason=reason,
                resolved_at=now,
                released_from_outcome=outcome.value,
            )
        else:
            entry = replace(
                existing,
                operator_resolution=OPERATOR_RESOLUTION_NOT_EXECUTED,
                resolved_by=by,
                resolution_reason=reason,
                resolved_at=now,
                released_from_outcome=outcome.value,
            )
        if not self._try_transition(
            entry,
            expected_from=_RESOLUTION_ACCEPTED_STORED_OUTCOMES,
            expected_owner=existing.owner,
            expected_fence=existing.fence,
        ):
            raise LedgerAlreadyResolvedError(
                f"Cannot release request {request_id!r}: transition superseded"
            )
        self._emit_outcome(
            request_id=request_id,
            tool=entry.tool,
            event="release",
            gate="RELEASE",
            terminal_outcome=entry.resolved_terminal_outcome(now=now),
            boundary=SideEffectBoundary(entry.side_effect_boundary),
            authorized_reexec=(verified == OPERATOR_RESOLUTION_NOT_EXECUTED),
            owner=by,
        )
        if self._audit_emitter is not None:
            receipt = self._audit_emitter.emit_release_receipt(
                entry,
                verified=verified,
                by=by,
                reason=reason,
            )
            entry = self.attach_receipt_ref(
                request_id,
                receipt.receipt_id,
                expected_owner=entry.owner,
                expected_fence=entry.fence,
            )
        return entry

    def _new_inflight_entry(
        self,
        request_id: str,
        tool: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        *,
        binding: ToolTransitionBinding | None = None,
        _provider_key_first_attempt_at: float | None = None,
        _provider_idempotency_key: str | None = None,
        _effect_id: str | None = None,
    ) -> LedgerEntry:
        bound = _bind_args(args, kwargs)
        boundary = (
            binding.side_effect_boundary_default.value
            if binding is not None
            else SideEffectBoundary.NOT_CROSSED.value
        )
        provider_key = _provider_idempotency_key
        if provider_key is None and binding is not None:
            provider_key = extract_provider_idempotency_key(kwargs, binding)
        if provider_key is not None and _provider_key_first_attempt_at is None:
            pkey_first_attempt: float | None = time.time()
        else:
            pkey_first_attempt = _provider_key_first_attempt_at
        decision_raw = kwargs.get("decision_id")
        state_ref_raw = kwargs.get("state_ref")
        parent_raw = kwargs.get("parent_request_id")
        handoff_raw = kwargs.get("handoff_id")
        active_handoff = get_active_handoff()
        if parent_raw is None and active_handoff is not None:
            parent_raw = active_handoff.parent_request_id
        if handoff_raw is None and active_handoff is not None:
            handoff_raw = active_handoff.handoff_id
        stored_args, stored_kwargs = _evidence_args(bound["args"], bound["kwargs"])
        # Stable effect identity, present whenever a binding is available to
        # derive it from (classified tools only — unclassified claim() has no
        # side-effect class and stays effect_id=None). Same derivation as
        # derive_request_id's fallback, so request_id == effect_id whenever
        # request_id itself was derived (the default) rather than explicit.
        effect_id = (
            _effect_id
            if _effect_id is not None
            else (
                derive_effect_id_for_call(tool, args, kwargs, binding)
                if binding is not None
                else None
            )
        )
        return LedgerEntry(
            request_id=request_id,
            tool=tool,
            args=stored_args,
            kwargs=stored_kwargs,
            status=legacy_status_from_terminal(TerminalOutcome.IN_FLIGHT),
            terminal_outcome=TerminalOutcome.IN_FLIGHT.value,
            owner=_ledger_owner(),
            idempotency_key=request_id,
            side_effect_boundary=boundary,
            provider_idempotency_key=provider_key,
            provider_key_first_attempt_at=pkey_first_attempt,
            decision_id=str(decision_raw) if decision_raw is not None else None,
            state_ref=str(state_ref_raw) if state_ref_raw is not None else None,
            parent_request_id=str(parent_raw) if parent_raw is not None else None,
            handoff_id=str(handoff_raw) if handoff_raw is not None else None,
            effect_protocol_required=binding is not None,
            effect_id=effect_id,
        )

    def claim(
        self,
        request_id: str,
        tool: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        *,
        lease_ttl: float | None = None,
    ) -> LedgerEntry:
        """Claim a request idempotency key before execution.

        Returns the existing completed entry if the request already succeeded.
        Raises LedgerPendingError if the request is currently in-flight.

        This is the legacy *unclassified* path (no ``transition_binding``), so
        Mycelium cannot know whether the tool has side effects. With
        ``unclassified_policy="warn"`` (default) a reclaim of a
        previously-failed entry proceeds but emits a one-time warning per
        tool. With ``unclassified_policy="strict"`` the claim is routed
        through :meth:`claim_side_effecting` with a conservative synthesized
        binding (``non_idempotent_mutate``): failed retries hard-block and an
        in-flight request polls instead of raising ``LedgerPendingError``.
        Request-id derivation stays legacy either way — only the resolution
        gate changes.
        """
        if self._unclassified_policy == UNCLASSIFIED_POLICY_STRICT:
            return self.claim_side_effecting(
                request_id,
                tool,
                args,
                kwargs,
                _UNCLASSIFIED_BINDING,
                lease_ttl=lease_ttl,
            )
        ttl = self._lease_ttl if lease_ttl is None else lease_ttl
        prior = self._get_entry(request_id)
        self._enforce_args_drift(tool, args, kwargs, request_id=request_id, existing=prior)
        self._warn_unclassified_retry(tool, prior)
        entry = self._new_inflight_entry(request_id, tool, args, kwargs)
        outcome, existing = self._try_claim_inflight(entry, lease_ttl=ttl)
        if outcome == "completed" and existing is not None:
            self._enforce_args_drift(tool, args, kwargs, request_id=request_id, existing=existing)
            return existing
        if outcome == "in_flight":
            raise LedgerPendingError(f"Tool {tool!r} request {request_id!r} is already in-flight")
        claimed = self.get(request_id)
        return claimed if claimed is not None else entry

    def claim_read_only(
        self,
        request_id: str,
        tool: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        *,
        lease_ttl: float | None = None,
        poll_interval: float | None = None,
        poll_timeout: float | None = None,
    ) -> LedgerEntry:
        """Claim or resolve a read-only tool transition.

        Resolution paths:
        - **Return** cached result when already completed
        - **Poll** while another worker holds a valid in-flight lease
        - **Reclaim** when the in-flight lease is stale (``EXPIRED``)
        - **Retry** after a previous failed attempt
        """
        ttl = self._lease_ttl if lease_ttl is None else lease_ttl
        interval = self._poll_interval if poll_interval is None else poll_interval
        timeout = self._poll_timeout if poll_timeout is None else poll_timeout
        poll_deadline = time.time() + timeout if timeout is not None else None

        while True:
            existing = self.get(request_id)
            self._enforce_args_drift(tool, args, kwargs, request_id=request_id, existing=existing)
            if existing is not None:
                gate = resolve_read_only_gate(existing)
                if gate == TransitionGate.REPAIR:
                    self.repair_transition(request_id)
                    continue
                if gate == TransitionGate.RECLAIM and self._reclaim_requires_death_signal:
                    if not has_worker_death_evidence(
                        existing,
                        now=time.time(),
                        presumed_dead_after=self._presumed_dead_after,
                    ):
                        self._poll_read_only(
                            request_id,
                            interval=interval,
                            poll_deadline=poll_deadline,
                        )
                        continue
                if gate == TransitionGate.SOFT_BLOCK:
                    return self._resolve_read_only_soft_block(
                        request_id, tool, args, kwargs, existing
                    )

            entry = self._new_inflight_entry(request_id, tool, args, kwargs)
            outcome, existing = self._try_claim_inflight(entry, lease_ttl=ttl)
            if outcome == "completed" and existing is not None:
                self._enforce_args_drift(
                    tool, args, kwargs, request_id=request_id, existing=existing
                )
                return existing
            if outcome == "claimed":
                claimed = self.get(request_id)
                return claimed if claimed is not None else entry
            if outcome == "in_flight":
                self._poll_read_only(
                    request_id,
                    interval=interval,
                    poll_deadline=poll_deadline,
                )
                continue
            raise LedgerError(f"Unexpected claim outcome {outcome!r} for read-only tool {tool!r}")

    def _resolve_read_only_soft_block(
        self,
        request_id: str,
        tool: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        existing: LedgerEntry,
    ) -> LedgerEntry:
        """Resolve a read-only ``SOFT_BLOCK`` (``UNKNOWN`` / ``BLOCKED``).

        Re-running a read-only tool is always safe, so by default the ambiguous
        entry is reset to a fresh in-flight claim and the tool runs exactly once
        more. When the ledger is configured with ``defer_read_only_unknown``,
        raise :class:`LedgerSoftBlockError` instead so an expensive read can be
        deferred and retried by the caller (cost-dependent).
        """
        if self._defer_read_only_unknown:
            raise LedgerSoftBlockError(
                soft_block_message(existing, tool=tool, request_id=request_id)
            )
        fresh = self._new_inflight_entry(request_id, tool, args, kwargs)
        fresh = replace(fresh, fence=existing.fence + 1)
        if not self._try_transition(
            fresh,
            expected_from=frozenset({existing.terminal_outcome}),
            expected_owner=existing.owner,
            expected_fence=existing.fence,
        ):
            raise LedgerOutcomeAlreadySetError(
                f"Cannot retry read-only request {request_id!r}: transition superseded"
            )
        return fresh

    def _poll_read_only(
        self,
        request_id: str,
        *,
        interval: float,
        poll_deadline: float | None,
    ) -> None:
        """Wait until a read-only transition leaves the in-flight state."""
        while True:
            if poll_deadline is not None and time.time() >= poll_deadline:
                raise LedgerPollTimeoutError(f"Timed out polling read-only request {request_id!r}")
            time.sleep(interval)
            current = self.get(request_id)
            if current is None:
                return
            outcome = current.resolved_terminal_outcome()
            if outcome == TerminalOutcome.COMPLETED:
                return
            if outcome in (
                TerminalOutcome.FAILED_BEFORE_EFFECT,
                TerminalOutcome.FAILED_AFTER_EFFECT,
            ):
                return
            if outcome == TerminalOutcome.EXPIRED:
                return
            if outcome == TerminalOutcome.IN_FLIGHT:
                continue
            return

    async def claim_read_only_async(
        self,
        request_id: str,
        tool: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        *,
        lease_ttl: float | None = None,
        poll_interval: float | None = None,
        poll_timeout: float | None = None,
    ) -> LedgerEntry:
        """Async variant of :meth:`claim_read_only` for read-only tool polling."""
        ttl = self._lease_ttl if lease_ttl is None else lease_ttl
        interval = self._poll_interval if poll_interval is None else poll_interval
        timeout = self._poll_timeout if poll_timeout is None else poll_timeout
        poll_deadline = time.time() + timeout if timeout is not None else None

        while True:
            existing = self.get(request_id)
            self._enforce_args_drift(tool, args, kwargs, request_id=request_id, existing=existing)
            if existing is not None:
                gate = resolve_read_only_gate(existing)
                if gate == TransitionGate.REPAIR:
                    self.repair_transition(request_id)
                    continue
                if gate == TransitionGate.RECLAIM and self._reclaim_requires_death_signal:
                    if not has_worker_death_evidence(
                        existing,
                        now=time.time(),
                        presumed_dead_after=self._presumed_dead_after,
                    ):
                        await self._poll_read_only_async(
                            request_id,
                            interval=interval,
                            poll_deadline=poll_deadline,
                        )
                        continue
                if gate == TransitionGate.SOFT_BLOCK:
                    return self._resolve_read_only_soft_block(
                        request_id, tool, args, kwargs, existing
                    )

            entry = self._new_inflight_entry(request_id, tool, args, kwargs)
            outcome, existing = self._try_claim_inflight(entry, lease_ttl=ttl)
            if outcome == "completed" and existing is not None:
                self._enforce_args_drift(
                    tool, args, kwargs, request_id=request_id, existing=existing
                )
                return existing
            if outcome == "claimed":
                claimed = self.get(request_id)
                return claimed if claimed is not None else entry
            if outcome == "in_flight":
                await self._poll_read_only_async(
                    request_id,
                    interval=interval,
                    poll_deadline=poll_deadline,
                )
                continue
            raise LedgerError(f"Unexpected claim outcome {outcome!r} for read-only tool {tool!r}")

    async def _poll_read_only_async(
        self,
        request_id: str,
        *,
        interval: float,
        poll_deadline: float | None,
    ) -> None:
        while True:
            if poll_deadline is not None and time.time() >= poll_deadline:
                raise LedgerPollTimeoutError(f"Timed out polling read-only request {request_id!r}")
            await asyncio.sleep(interval)
            current = self.get(request_id)
            if current is None:
                return
            outcome = current.resolved_terminal_outcome()
            if outcome == TerminalOutcome.COMPLETED:
                return
            if outcome in (
                TerminalOutcome.FAILED_BEFORE_EFFECT,
                TerminalOutcome.FAILED_AFTER_EFFECT,
            ):
                return
            if outcome == TerminalOutcome.EXPIRED:
                return
            if outcome == TerminalOutcome.IN_FLIGHT:
                continue
            return

    def _raise_hard_block(
        self,
        request_id: str,
        tool: str,
        existing: LedgerEntry,
        *,
        binding: ToolTransitionBinding | None = None,
        now: float | None = None,
    ) -> LedgerEntry:
        current = self.get(request_id)
        if current is not None:
            curr_outcome = current.resolved_terminal_outcome(now=now)
            if curr_outcome == TerminalOutcome.IN_FLIGHT:
                _reconcile_cas_lost.val = True
                return current
            if curr_outcome == TerminalOutcome.COMPLETED:
                return current
            if curr_outcome == TerminalOutcome.EXPIRED:
                boundary = SideEffectBoundary(current.side_effect_boundary)
                if boundary == SideEffectBoundary.NOT_CROSSED:
                    error = (
                        "stale in-flight lease with not_crossed boundary; "
                        "reclaim only if an external_operation_ref reconcile "
                        "proves NOT_EXECUTED"
                    )
                else:
                    error = (
                        "stale in-flight lease; side-effect boundary "
                        f"{boundary.value} — effect may have happened"
                    )
                try:
                    existing = self.mark_blocked(
                        request_id,
                        error=error,
                        _expected_from=_IN_FLIGHT_OUTCOMES,
                        _expected_owner=current.owner,
                        _expected_fence=current.fence,
                    )
                except LedgerOutcomeAlreadySetError:
                    again = self.get(request_id)
                    if again is not None:
                        _reconcile_cas_lost.val = True
                        return again
                    existing = current
        message = hard_block_message(
            existing, tool=tool, request_id=request_id, binding=binding, now=now
        )
        raise LedgerHardBlockError(message)

    def _apply_reconcile_result(
        self,
        request_id: str,
        tool: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        binding: ToolTransitionBinding,
        result: Any,
        observed_entry: LedgerEntry,
        _preserved_pkey_first_attempt: float | None = None,
        _cas_race_returns_none: bool = False,
    ) -> LedgerEntry | None:
        """Map a reconcile result onto the ledger.

        ``COMPLETED`` marks the transition done (redispatch returns the stored
        result, no re-execution). ``NOT_EXECUTED`` resets the entry to a fresh
        in-flight claim so the tool runs exactly once. ``UNKNOWN`` returns None
        so the caller hard-blocks.

        When ``_cas_race_returns_none`` is True (operator-resolution path),
        a lost CAS returns None so the caller can fall through. Otherwise
        (reconciler path) the winner's entry is returned so the claim loop
        polls instead of hard-blocking.
        """
        if result.status == ReconcileStatus.COMPLETED:
            if observed_entry.effect_protocol_required and not (
                _has_allowed_attempting_decision(observed_entry)
            ):
                return None
            try:
                return self.complete(
                    request_id,
                    result.result,
                    _expected_from=_RESOLUTION_ACCEPTED_STORED_OUTCOMES,
                    _expected_owner=observed_entry.owner,
                    _expected_fence=observed_entry.fence,
                )
            except LedgerOutcomeAlreadySetError:
                if _cas_race_returns_none:
                    return None
                _reconcile_cas_lost.val = True
                return self.get(request_id)
        if result.status == ReconcileStatus.NOT_EXECUTED:
            if _preserved_pkey_first_attempt is None:
                if observed_entry.provider_idempotency_key is not None:
                    _preserved_pkey_first_attempt = observed_entry.provider_key_first_attempt_at
            fresh = self._new_inflight_entry(
                request_id,
                tool,
                args,
                kwargs,
                binding=binding,
                _provider_key_first_attempt_at=_preserved_pkey_first_attempt,
            )
            now = time.time()
            fresh = replace(
                fresh,
                fence=observed_entry.fence + 1,
                lease_until=(now + self._lease_ttl if self._lease_ttl > 0 else None),
                last_heartbeat_at=now,
            )
            # EXPIRED entries have stored terminal ``IN_FLIGHT`` (lease is
            # resolved at read time).  Advance past ``IN_FLIGHT`` first so the
            # CAS below cannot race on ``IN_FLIGHT → IN_FLIGHT``.
            expected_from = _RECONCILE_NOT_EXECUTED_OUTCOMES
            if observed_entry.resolved_terminal_outcome(now=now) in (TerminalOutcome.EXPIRED,):
                try:
                    self.mark_blocked(
                        request_id,
                        error="reconciling expired entry as NOT_EXECUTED",
                        _expected_from=_IN_FLIGHT_OUTCOMES,
                        _expected_owner=observed_entry.owner,
                        _expected_fence=observed_entry.fence,
                    )
                except LedgerOutcomeAlreadySetError:
                    pass
                expected_from = frozenset({TerminalOutcome.BLOCKED.value})
            if not self._try_transition(
                fresh,
                expected_from=expected_from,
                expected_owner=observed_entry.owner,
                expected_fence=observed_entry.fence,
            ):
                if _cas_race_returns_none:
                    return None
                _reconcile_cas_lost.val = True
                return self.get(request_id)
            # The fresh claim was won by this caller, which will run the tool
            # body exactly once — mark that run as an authorized re-execution
            # so outcome telemetry can tell it apart from a silent duplicate.
            _outcome_reexec_authorized.set(True)
            return fresh
        return None

    def _capability_for(self, binding: ToolTransitionBinding) -> ToolCapability:
        """Effective capability for this ledger — reconciler presence drives QUERYABLE.

        A bound :class:`~mycelium.reconcile.Reconciler` is the concrete
        "queryable" mechanism, so it can loosen the binding's conservative floor
        (e.g. ``NON_IDEMPOTENT_MUTATE`` BLIND → QUERYABLE). An explicit ``BLIND``
        declaration always wins and is never loosened.
        """
        return binding.effective_capability(has_reconciler=self._reconciler is not None)

    def _entry_is_ambiguous(self, entry: LedgerEntry) -> bool:
        """Whether an effect's outcome is unknown (may or may not have happened).

        A ``FAILED_BEFORE_EFFECT`` or ``EXPIRED`` entry whose boundary is still
        ``not_crossed`` is not ambiguous — the effect provably never crossed the
        boundary, so it stays safe to retry (or death-signal reclaim) regardless
        of probeability. Ambiguity is ``UNKNOWN`` / ``FAILED_AFTER_EFFECT`` (the
        outcome itself is unknown or the effect definitely fired), or *any*
        ``maybe_crossed`` / ``crossed`` boundary. Only ambiguous entries are
        subject to BLIND parking — that is exactly the "did the blind effect
        happen?" case.
        """
        outcome = entry.resolved_terminal_outcome()
        if outcome in (
            TerminalOutcome.UNKNOWN,
            TerminalOutcome.FAILED_AFTER_EFFECT,
        ):
            return True
        boundary = SideEffectBoundary(entry.side_effect_boundary)
        return boundary in (
            SideEffectBoundary.MAYBE_CROSSED,
            SideEffectBoundary.CROSSED,
        )

    def _blind_never_retries(
        self,
        tool: str,
        binding: ToolTransitionBinding,
        existing: LedgerEntry,
    ) -> bool:
        """Whether this tool must park (never auto-retry) an ambiguous entry.

        BLIND: no way to probe the outcome — never auto-redispatch an entry
        whose effect may have crossed the boundary. QUERYABLE without a
        reconciler present fails closed to the same parking behaviour (with a
        warning) rather than silently auto-retrying a second effect. An
        unambiguous ``FAILED_BEFORE_EFFECT`` / ``not_crossed`` entry is never
        parked here — it provably did not happen.
        """
        if not self._entry_is_ambiguous(existing):
            return False
        capability = self._capability_for(binding)
        has_provider_key = binding.provider_idempotency_key_param is not None
        # A tool that intended to be QUERYABLE but has no probe mechanism (no
        # reconciler bound, no provider idempotency key) fails closed to BLIND
        # parking — with a warning so the misconfiguration is visible.
        intended_queryable = (
            binding.explicit_capability == ToolCapability.QUERYABLE
            or binding.capability == ToolCapability.QUERYABLE
        )
        if (
            capability == ToolCapability.BLIND
            and intended_queryable
            and not has_provider_key
            and self._reconciler is None
        ):
            warnings.warn(
                f"tool {tool!r} declares capability=queryable but no Reconciler "
                "is bound and no provider idempotency key is configured; "
                "failing closed to blind behaviour — the ambiguous entry parks "
                "for operator reconciliation instead of auto-retrying.",
                stacklevel=2,
            )
            return True
        if capability == ToolCapability.BLIND:
            return True
        # QUERYABLE with a provider idempotency key needs no reconciler: the
        # same-key retry gate already validated the dedupe window.
        return False

    def _attempt_reconcile(
        self,
        request_id: str,
        tool: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        existing: LedgerEntry,
        binding: ToolTransitionBinding,
    ) -> LedgerEntry | None:
        """Reconcile an ambiguous transition; None means fall through to block.

        Fail-closed: a missing reconciler, missing ref, or a raising reconciler
        all resolve to None (hard-block).
        """
        if self._reconciler is None or not existing.external_operation_ref:
            return None
        try:
            result = self._reconciler.reconcile(existing)
        except Exception:
            return None
        return self._apply_reconcile_result(
            request_id,
            tool,
            args,
            kwargs,
            binding,
            result,
            existing,
        )

    async def _attempt_reconcile_async(
        self,
        request_id: str,
        tool: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        existing: LedgerEntry,
        binding: ToolTransitionBinding,
    ) -> LedgerEntry | None:
        """Async variant of :meth:`_attempt_reconcile`.

        Prefers ``reconcile_async`` when the reconciler provides it, otherwise
        falls back to the sync :meth:`Reconciler.reconcile`.
        """
        if self._reconciler is None or not existing.external_operation_ref:
            return None
        try:
            reconcile_async = getattr(self._reconciler, "reconcile_async", None)
            if reconcile_async is not None:
                result = await reconcile_async(existing)
            else:
                result = self._reconciler.reconcile(existing)
        except Exception:
            return None
        return self._apply_reconcile_result(
            request_id,
            tool,
            args,
            kwargs,
            binding,
            result,
            existing,
        )

    def _consume_operator_resolution(
        self,
        request_id: str,
        tool: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        existing: LedgerEntry,
        binding: ToolTransitionBinding,
    ) -> LedgerEntry | None:
        """Consume an unconsumed operator ``not_executed`` release, if present.

        An operator release is the human-issued, durably stored equivalent of
        ``ReconcileResult.not_executed()``, so it reuses the same machinery:
        the entry resets to a fresh in-flight claim and the tool may execute
        exactly once. The fresh entry has ``operator_resolution=None`` (the
        release is one-shot) but carries the audit fields forward. Race
        characteristics match the Reconciler NOT_EXECUTED path (plain
        ``storage.set``).
        """
        if existing.operator_resolution != OPERATOR_RESOLUTION_NOT_EXECUTED:
            return None
        _preserved = (
            existing.provider_key_first_attempt_at
            if existing.provider_idempotency_key is not None
            else None
        )
        fresh = self._apply_reconcile_result(
            request_id,
            tool,
            args,
            kwargs,
            binding,
            ReconcileResult.not_executed(),
            existing,
            _preserved_pkey_first_attempt=_preserved,
            _cas_race_returns_none=True,
        )
        if fresh is None:
            return None
        stamped = replace(
            fresh,
            resolved_by=existing.resolved_by,
            resolution_reason=existing.resolution_reason,
            resolved_at=existing.resolved_at,
            released_from_outcome=existing.released_from_outcome,
        )
        if not self._try_transition(
            stamped,
            expected_from=frozenset({fresh.terminal_outcome}),
            expected_owner=fresh.owner,
            expected_fence=fresh.fence,
        ):
            raise LedgerOutcomeAlreadySetError(
                f"Cannot consume release for {request_id!r}: transition superseded"
            )
        return stamped

    def _reconcile_or_hard_block(
        self,
        request_id: str,
        tool: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        existing: LedgerEntry,
        binding: ToolTransitionBinding,
    ) -> LedgerEntry:
        released = self._consume_operator_resolution(
            request_id, tool, args, kwargs, existing, binding
        )
        if released is not None:
            return released
        resolved = self._attempt_reconcile(request_id, tool, args, kwargs, existing, binding)
        if resolved is not None:
            return resolved
        return self._raise_hard_block(request_id, tool, existing, binding=binding)

    async def _reconcile_or_hard_block_async(
        self,
        request_id: str,
        tool: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        existing: LedgerEntry,
        binding: ToolTransitionBinding,
    ) -> LedgerEntry:
        released = self._consume_operator_resolution(
            request_id, tool, args, kwargs, existing, binding
        )
        if released is not None:
            return released
        resolved = await self._attempt_reconcile_async(
            request_id, tool, args, kwargs, existing, binding
        )
        if resolved is not None:
            return resolved
        return self._raise_hard_block(request_id, tool, existing, binding=binding)

    def _prefer_settle_before_unknown_allow(
        self,
        request_id: str,
        tool: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        existing: LedgerEntry,
        binding: ToolTransitionBinding,
    ) -> LedgerEntry | None:
        """Prefer operator release / Reconciler before same-key UNKNOWN re-exec.

        Returns a settled entry when resolution succeeded, else ``None`` so the
        claim loop may fall through to the opt-in same-key retry (provider
        dedupe still within ``provider_idempotency_key_ttl``).
        """
        if existing.resolved_terminal_outcome() != TerminalOutcome.UNKNOWN:
            return None
        released = self._consume_operator_resolution(
            request_id, tool, args, kwargs, existing, binding
        )
        if released is not None:
            return released
        return self._attempt_reconcile(request_id, tool, args, kwargs, existing, binding)

    async def _prefer_settle_before_unknown_allow_async(
        self,
        request_id: str,
        tool: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        existing: LedgerEntry,
        binding: ToolTransitionBinding,
    ) -> LedgerEntry | None:
        """Async variant of :meth:`_prefer_settle_before_unknown_allow`."""
        if existing.resolved_terminal_outcome() != TerminalOutcome.UNKNOWN:
            return None
        released = self._consume_operator_resolution(
            request_id, tool, args, kwargs, existing, binding
        )
        if released is not None:
            return released
        return await self._attempt_reconcile_async(
            request_id, tool, args, kwargs, existing, binding
        )

    def _reset_unknown_for_same_key_retry(
        self,
        request_id: str,
        tool: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        existing: LedgerEntry,
        binding: ToolTransitionBinding,
        *,
        lease_ttl: float,
    ) -> LedgerEntry | None:
        """CAS-reset ``UNKNOWN`` → fresh in-flight for opt-in same-key retry.

        ``try_claim_inflight`` refuses to overwrite ``UNKNOWN`` (fail-closed for
        peers). After the gate has ALLOW'd within the provider key window, this
        is the authorized transition — same shape as Reconciler ``NOT_EXECUTED``.

        A BLIND tool (or a QUERYABLE tool with no reconciler) never opts into
        same-key retry even with a valid provider key + TTL: BLIND declaration
        wins, so it parks for operator reconciliation instead.
        """
        if self._blind_never_retries(tool, binding, existing):
            return None
        pkey_first = (
            existing.provider_key_first_attempt_at
            if existing.provider_idempotency_key is not None
            else None
        )
        explicit_provider_key = extract_provider_idempotency_key(kwargs, binding)
        fresh = self._new_inflight_entry(
            request_id,
            tool,
            args,
            kwargs,
            binding=binding,
            _provider_key_first_attempt_at=pkey_first,
            _provider_idempotency_key=(
                explicit_provider_key
                if explicit_provider_key is not None
                else existing.provider_idempotency_key
            ),
        )
        now = time.time()
        fresh = replace(
            fresh,
            fence=existing.fence + 1,
            lease_until=(now + lease_ttl if lease_ttl > 0 else None),
            last_heartbeat_at=now,
        )
        if not self._try_transition(
            fresh,
            expected_from=_UNKNOWN_SAME_KEY_RETRY_OUTCOMES,
            expected_owner=existing.owner,
            expected_fence=existing.fence,
        ):
            return None
        _outcome_reexec_authorized.set(True)
        return fresh

    def _record_request_id_alias(self, canonical_request_id: str, supplied_request_id: str) -> None:
        """Best-effort audit stamp for explicit request-id aliases."""
        if canonical_request_id == supplied_request_id:
            return
        existing = self.get(canonical_request_id)
        if existing is None:
            return
        if supplied_request_id in existing.request_id_aliases:
            return
        updated = replace(
            existing,
            request_id_aliases=existing.request_id_aliases + (supplied_request_id,),
        )
        self._try_transition(
            updated,
            expected_from=frozenset({existing.terminal_outcome}),
            expected_owner=existing.owner,
            expected_fence=existing.fence,
        )

    def _canonical_request_id_for_effect(
        self,
        *,
        effect_id: str,
        request_id: str,
    ) -> str:
        canonical = self._resolve_request_id_for_effect(effect_id)
        if canonical is None:
            return request_id
        if canonical != request_id:
            self._record_request_id_alias(canonical, request_id)
        return canonical

    @staticmethod
    def _effective_incoming_provider_key(
        *,
        binding: ToolTransitionBinding,
        kwargs: dict[str, Any],
        effect_id: str,
        existing: LedgerEntry | None,
    ) -> str | None:
        incoming = extract_provider_idempotency_key(kwargs, binding)
        if incoming is not None:
            return incoming
        if not should_propagate_effect_id_as_provider_key(binding):
            return None
        if existing is not None and existing.provider_idempotency_key is not None:
            return existing.provider_idempotency_key
        return effect_id

    def claim_side_effecting(
        self,
        request_id: str,
        tool: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        binding: ToolTransitionBinding,
        *,
        lease_ttl: float | None = None,
        poll_interval: float | None = None,
        poll_timeout: float | None = None,
    ) -> LedgerEntry:
        """Claim or resolve a side-effecting tool transition."""
        self._warn_if_volatile_side_effect_storage(tool, binding)
        ttl = self._lease_ttl if lease_ttl is None else lease_ttl
        interval = self._poll_interval if poll_interval is None else poll_interval
        timeout = self._poll_timeout if poll_timeout is None else poll_timeout
        poll_deadline = time.time() + timeout if timeout is not None else None
        effect_id = derive_effect_id_for_call(tool, args, kwargs, binding)

        while True:
            claim_kwargs = _claim_kwargs(dict(kwargs), _drop_ledger_keys(dict(kwargs)))
            canonical_request_id = self._canonical_request_id_for_effect(
                effect_id=effect_id,
                request_id=request_id,
            )
            existing = self.get(canonical_request_id)
            explicit_provider_key = extract_provider_idempotency_key(kwargs, binding)
            incoming_key = self._effective_incoming_provider_key(
                binding=binding,
                kwargs=kwargs,
                effect_id=effect_id,
                existing=existing,
            )
            self._enforce_args_drift(
                tool,
                args,
                claim_kwargs,
                request_id=canonical_request_id,
                existing=existing,
                binding=binding,
                incoming_request_id=request_id,
            )
            if existing is not None:
                gate = resolve_side_effect_gate(
                    existing,
                    binding,
                    incoming_provider_idempotency_key=incoming_key,
                )
                if gate == TransitionGate.REPAIR:
                    self.repair_transition(canonical_request_id)
                    continue
                if gate == TransitionGate.RETURN:
                    return self.get(canonical_request_id) or existing
                if gate == TransitionGate.HARD_BLOCK:
                    entry = self._reconcile_or_hard_block(
                        canonical_request_id, tool, args, kwargs, existing, binding
                    )
                    if entry.resolved_terminal_outcome() == TerminalOutcome.IN_FLIGHT:
                        if getattr(_reconcile_cas_lost, "val", False):
                            _reconcile_cas_lost.val = False
                            self._poll_side_effecting(
                                canonical_request_id,
                                tool=tool,
                                interval=interval,
                                poll_deadline=poll_deadline,
                            )
                            continue
                    return entry
                if gate == TransitionGate.POLL:
                    self._poll_side_effecting(
                        canonical_request_id,
                        tool=tool,
                        interval=interval,
                        poll_deadline=poll_deadline,
                    )
                    continue
                if gate == TransitionGate.ALLOW:
                    settled = self._prefer_settle_before_unknown_allow(
                        canonical_request_id, tool, args, kwargs, existing, binding
                    )
                    if settled is not None:
                        return settled
                    if existing.resolved_terminal_outcome() == TerminalOutcome.UNKNOWN:
                        reset = self._reset_unknown_for_same_key_retry(
                            canonical_request_id,
                            tool,
                            args,
                            kwargs,
                            existing,
                            binding,
                            lease_ttl=ttl,
                        )
                        if reset is not None:
                            return reset
                        if self._blind_never_retries(tool, binding, existing):
                            return self._raise_hard_block(
                                canonical_request_id, tool, existing, binding=binding
                            )
                        continue
                    if self._blind_never_retries(tool, binding, existing):
                        return self._raise_hard_block(
                            canonical_request_id, tool, existing, binding=binding
                        )
                    if self._reclaim_requires_death_signal and not has_worker_death_evidence(
                        existing,
                        now=time.time(),
                        presumed_dead_after=self._presumed_dead_after,
                    ):
                        self._poll_side_effecting(
                            canonical_request_id,
                            tool=tool,
                            interval=interval,
                            poll_deadline=poll_deadline,
                        )
                        continue

            _old_pkey_attempt = (
                existing.provider_key_first_attempt_at
                if existing is not None and existing.provider_idempotency_key is not None
                else None
            )
            entry = self._new_inflight_entry(
                canonical_request_id,
                tool,
                args,
                claim_kwargs,
                binding=binding,
                _provider_key_first_attempt_at=_old_pkey_attempt,
                _provider_idempotency_key=(
                    explicit_provider_key
                    if explicit_provider_key is not None
                    else (
                        existing.provider_idempotency_key
                        if existing is not None
                        else None
                    )
                ),
                _effect_id=effect_id,
            )
            outcome, existing = self._try_claim_inflight(entry, lease_ttl=ttl)
            if outcome == "completed" and existing is not None:
                return existing
            if outcome == "in_flight" and existing is not None:
                canonical_request_id = existing.request_id
                incoming_key = self._effective_incoming_provider_key(
                    binding=binding,
                    kwargs=kwargs,
                    effect_id=effect_id,
                    existing=existing,
                )
                gate = resolve_side_effect_gate(
                    existing,
                    binding,
                    incoming_provider_idempotency_key=incoming_key,
                )
                if gate == TransitionGate.REPAIR:
                    self.repair_transition(canonical_request_id)
                    continue
                if gate == TransitionGate.RETURN:
                    return existing
                if gate == TransitionGate.HARD_BLOCK:
                    entry = self._reconcile_or_hard_block(
                        canonical_request_id, tool, args, kwargs, existing, binding
                    )
                    if entry.resolved_terminal_outcome() == TerminalOutcome.IN_FLIGHT:
                        if getattr(_reconcile_cas_lost, "val", False):
                            _reconcile_cas_lost.val = False
                            self._poll_side_effecting(
                                canonical_request_id,
                                tool=tool,
                                interval=interval,
                                poll_deadline=poll_deadline,
                            )
                            continue
                    return entry
                if gate == TransitionGate.ALLOW and self._blind_never_retries(
                    tool, binding, existing
                ):
                    self._poll_side_effecting(
                        canonical_request_id,
                        tool=tool,
                        interval=interval,
                        poll_deadline=poll_deadline,
                    )
                    continue
                if gate == TransitionGate.ALLOW and self._reclaim_requires_death_signal:
                    if not has_worker_death_evidence(
                        existing,
                        now=time.time(),
                        presumed_dead_after=self._presumed_dead_after,
                    ):
                        self._poll_side_effecting(
                            canonical_request_id,
                            tool=tool,
                            interval=interval,
                            poll_deadline=poll_deadline,
                        )
                        continue
                self._poll_side_effecting(
                    canonical_request_id,
                    tool=tool,
                    interval=interval,
                    poll_deadline=poll_deadline,
                )
                continue
            if outcome == "claimed":
                claimed = self.get(canonical_request_id)
                return claimed if claimed is not None else entry
            if existing is not None:
                canonical_request_id = existing.request_id
                entry = self._reconcile_or_hard_block(
                    canonical_request_id, tool, args, kwargs, existing, binding
                )
                if entry.resolved_terminal_outcome() == TerminalOutcome.IN_FLIGHT:
                    if getattr(_reconcile_cas_lost, "val", False):
                        _reconcile_cas_lost.val = False
                        self._poll_side_effecting(
                            canonical_request_id,
                            tool=tool,
                            interval=interval,
                            poll_deadline=poll_deadline,
                        )
                        continue
                    return entry
                return entry
            raise LedgerError(
                f"Unexpected claim outcome {outcome!r} for side-effecting tool {tool!r} "
                f"(request_id={canonical_request_id!r})"
            )

    def _poll_side_effecting(
        self,
        request_id: str,
        *,
        tool: str,
        interval: float,
        poll_deadline: float | None,
    ) -> None:
        """Wait for an in-flight side-effecting transition; never auto-reclaim.

        When the lease expires mid-poll, return so the outer claim loop can
        re-resolve the gate and attempt provider reconcile before hard-blocking.
        """
        while True:
            if poll_deadline is not None and time.time() >= poll_deadline:
                current = self.get(request_id)
                if current is not None:
                    try:
                        self.mark_unknown(
                            request_id,
                            error="timed out polling in-flight side-effecting transition",
                            _expected_from=_IN_FLIGHT_OUTCOMES,
                            _expected_owner=current.owner,
                            _expected_fence=current.fence,
                        )
                    except LedgerOutcomeAlreadySetError:
                        return
                    raise LedgerHardBlockError(
                        hard_block_message(
                            current,
                            tool=tool,
                            request_id=request_id,
                        )
                    )
                raise LedgerPollTimeoutError(
                    f"Timed out polling side-effecting request {request_id!r}"
                )
            time.sleep(interval)
            current = self.get(request_id)
            if current is None:
                return
            outcome = current.resolved_terminal_outcome()
            if outcome == TerminalOutcome.COMPLETED:
                return
            # Leave EXPIRED to the outer claim loop so HARD_BLOCK can attempt
            # reconcile (EXPIRED + not_crossed + external_operation_ref →
            # reclaim only when the provider proves NOT_EXECUTED).
            if outcome == TerminalOutcome.EXPIRED:
                return
            if outcome == TerminalOutcome.IN_FLIGHT:
                continue
            if outcome in (
                TerminalOutcome.FAILED_BEFORE_EFFECT,
                TerminalOutcome.FAILED_AFTER_EFFECT,
                TerminalOutcome.BLOCKED,
                TerminalOutcome.UNKNOWN,
            ):
                return

    async def claim_side_effecting_async(
        self,
        request_id: str,
        tool: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        binding: ToolTransitionBinding,
        *,
        lease_ttl: float | None = None,
        poll_interval: float | None = None,
        poll_timeout: float | None = None,
    ) -> LedgerEntry:
        """Async variant of :meth:`claim_side_effecting`."""
        self._warn_if_volatile_side_effect_storage(tool, binding)
        ttl = self._lease_ttl if lease_ttl is None else lease_ttl
        interval = self._poll_interval if poll_interval is None else poll_interval
        timeout = self._poll_timeout if poll_timeout is None else poll_timeout
        poll_deadline = time.time() + timeout if timeout is not None else None
        effect_id = derive_effect_id_for_call(tool, args, kwargs, binding)

        while True:
            claim_kwargs = _claim_kwargs(dict(kwargs), _drop_ledger_keys(dict(kwargs)))
            canonical_request_id = self._canonical_request_id_for_effect(
                effect_id=effect_id,
                request_id=request_id,
            )
            existing = self.get(canonical_request_id)
            explicit_provider_key = extract_provider_idempotency_key(kwargs, binding)
            incoming_key = self._effective_incoming_provider_key(
                binding=binding,
                kwargs=kwargs,
                effect_id=effect_id,
                existing=existing,
            )
            self._enforce_args_drift(
                tool,
                args,
                claim_kwargs,
                request_id=canonical_request_id,
                existing=existing,
                binding=binding,
                incoming_request_id=request_id,
            )
            if existing is not None:
                gate = resolve_side_effect_gate(
                    existing,
                    binding,
                    incoming_provider_idempotency_key=incoming_key,
                )
                if gate == TransitionGate.REPAIR:
                    self.repair_transition(canonical_request_id)
                    continue
                if gate == TransitionGate.RETURN:
                    return self.get(canonical_request_id) or existing
                if gate == TransitionGate.HARD_BLOCK:
                    entry = await self._reconcile_or_hard_block_async(
                        canonical_request_id, tool, args, kwargs, existing, binding
                    )
                    if entry.resolved_terminal_outcome() == TerminalOutcome.IN_FLIGHT:
                        if getattr(_reconcile_cas_lost, "val", False):
                            _reconcile_cas_lost.val = False
                            await self._poll_side_effecting_async(
                                canonical_request_id,
                                tool=tool,
                                interval=interval,
                                poll_deadline=poll_deadline,
                            )
                            continue
                    return entry
                if gate == TransitionGate.POLL:
                    await self._poll_side_effecting_async(
                        canonical_request_id,
                        tool=tool,
                        interval=interval,
                        poll_deadline=poll_deadline,
                    )
                    continue
                if gate == TransitionGate.ALLOW:
                    settled = await self._prefer_settle_before_unknown_allow_async(
                        canonical_request_id, tool, args, kwargs, existing, binding
                    )
                    if settled is not None:
                        return settled
                    if existing.resolved_terminal_outcome() == TerminalOutcome.UNKNOWN:
                        reset = self._reset_unknown_for_same_key_retry(
                            canonical_request_id,
                            tool,
                            args,
                            kwargs,
                            existing,
                            binding,
                            lease_ttl=ttl,
                        )
                        if reset is not None:
                            return reset
                        if self._blind_never_retries(tool, binding, existing):
                            return self._raise_hard_block(
                                canonical_request_id, tool, existing, binding=binding
                            )
                        continue
                    if self._blind_never_retries(tool, binding, existing):
                        return self._raise_hard_block(
                            canonical_request_id, tool, existing, binding=binding
                        )
                    if self._reclaim_requires_death_signal and not has_worker_death_evidence(
                        existing,
                        now=time.time(),
                        presumed_dead_after=self._presumed_dead_after,
                    ):
                        await self._poll_side_effecting_async(
                            canonical_request_id,
                            tool=tool,
                            interval=interval,
                            poll_deadline=poll_deadline,
                        )
                        continue

            _old_pkey_attempt = (
                existing.provider_key_first_attempt_at
                if existing is not None and existing.provider_idempotency_key is not None
                else None
            )
            entry = self._new_inflight_entry(
                canonical_request_id,
                tool,
                args,
                claim_kwargs,
                binding=binding,
                _provider_key_first_attempt_at=_old_pkey_attempt,
                _provider_idempotency_key=(
                    explicit_provider_key
                    if explicit_provider_key is not None
                    else (
                        existing.provider_idempotency_key
                        if existing is not None
                        else None
                    )
                ),
                _effect_id=effect_id,
            )
            outcome, existing = self._try_claim_inflight(entry, lease_ttl=ttl)
            if outcome == "completed" and existing is not None:
                return existing
            if outcome == "in_flight" and existing is not None:
                canonical_request_id = existing.request_id
                incoming_key = self._effective_incoming_provider_key(
                    binding=binding,
                    kwargs=kwargs,
                    effect_id=effect_id,
                    existing=existing,
                )
                gate = resolve_side_effect_gate(
                    existing,
                    binding,
                    incoming_provider_idempotency_key=incoming_key,
                )
                if gate == TransitionGate.REPAIR:
                    self.repair_transition(canonical_request_id)
                    continue
                if gate == TransitionGate.RETURN:
                    return existing
                if gate == TransitionGate.HARD_BLOCK:
                    entry = await self._reconcile_or_hard_block_async(
                        canonical_request_id, tool, args, kwargs, existing, binding
                    )
                    if entry.resolved_terminal_outcome() == TerminalOutcome.IN_FLIGHT:
                        if getattr(_reconcile_cas_lost, "val", False):
                            _reconcile_cas_lost.val = False
                            await self._poll_side_effecting_async(
                                canonical_request_id,
                                tool=tool,
                                interval=interval,
                                poll_deadline=poll_deadline,
                            )
                            continue
                    return entry
                if gate == TransitionGate.ALLOW and self._blind_never_retries(
                    tool, binding, existing
                ):
                    await self._poll_side_effecting_async(
                        canonical_request_id,
                        tool=tool,
                        interval=interval,
                        poll_deadline=poll_deadline,
                    )
                    continue
                if gate == TransitionGate.ALLOW and self._reclaim_requires_death_signal:
                    if not has_worker_death_evidence(
                        existing,
                        now=time.time(),
                        presumed_dead_after=self._presumed_dead_after,
                    ):
                        await self._poll_side_effecting_async(
                            canonical_request_id,
                            tool=tool,
                            interval=interval,
                            poll_deadline=poll_deadline,
                        )
                        continue
                await self._poll_side_effecting_async(
                    canonical_request_id,
                    tool=tool,
                    interval=interval,
                    poll_deadline=poll_deadline,
                )
                continue
            if outcome == "claimed":
                claimed = self.get(canonical_request_id)
                return claimed if claimed is not None else entry
            if existing is not None:
                canonical_request_id = existing.request_id
                entry = await self._reconcile_or_hard_block_async(
                    canonical_request_id, tool, args, kwargs, existing, binding
                )
                if entry.resolved_terminal_outcome() == TerminalOutcome.IN_FLIGHT:
                    if getattr(_reconcile_cas_lost, "val", False):
                        _reconcile_cas_lost.val = False
                        await self._poll_side_effecting_async(
                            canonical_request_id,
                            tool=tool,
                            interval=interval,
                            poll_deadline=poll_deadline,
                        )
                        continue
                    return entry
                return entry
            raise LedgerError(
                f"Unexpected claim outcome {outcome!r} for side-effecting tool {tool!r} "
                f"(request_id={canonical_request_id!r})"
            )

    async def _poll_side_effecting_async(
        self,
        request_id: str,
        *,
        tool: str,
        interval: float,
        poll_deadline: float | None,
    ) -> None:
        while True:
            if poll_deadline is not None and time.time() >= poll_deadline:
                current = self.get(request_id)
                if current is not None:
                    try:
                        self.mark_unknown(
                            request_id,
                            error="timed out polling in-flight side-effecting transition",
                            _expected_from=_IN_FLIGHT_OUTCOMES,
                            _expected_owner=current.owner,
                            _expected_fence=current.fence,
                        )
                    except LedgerOutcomeAlreadySetError:
                        return
                    raise LedgerHardBlockError(
                        hard_block_message(
                            current,
                            tool=tool,
                            request_id=request_id,
                        )
                    )
                raise LedgerPollTimeoutError(
                    f"Timed out polling side-effecting request {request_id!r}"
                )
            await asyncio.sleep(interval)
            current = self.get(request_id)
            if current is None:
                return
            outcome = current.resolved_terminal_outcome()
            if outcome == TerminalOutcome.COMPLETED:
                return
            # Leave EXPIRED to the outer claim loop so HARD_BLOCK can attempt
            # reconcile (EXPIRED + not_crossed + external_operation_ref →
            # reclaim only when the provider proves NOT_EXECUTED).
            if outcome == TerminalOutcome.EXPIRED:
                return
            if outcome == TerminalOutcome.IN_FLIGHT:
                continue
            if outcome in (
                TerminalOutcome.FAILED_BEFORE_EFFECT,
                TerminalOutcome.FAILED_AFTER_EFFECT,
                TerminalOutcome.BLOCKED,
                TerminalOutcome.UNKNOWN,
            ):
                return

    def complete(
        self,
        request_id: str,
        result: Any,
        *,
        expected_fence: int | None = None,
        _expected_from: frozenset[str] | None = None,
        _expected_owner: str | None = None,
        _expected_fence: int | None = None,
    ) -> LedgerEntry:
        existing = self._get_entry(request_id)
        if existing is None:
            raise LedgerError(f"Cannot complete unknown request {request_id!r}")
        if expected_fence is not None and _expected_fence is not None:
            if expected_fence != _expected_fence:
                raise LedgerError("conflicting expected fence values")
        fence = expected_fence if expected_fence is not None else _expected_fence
        if fence is None:
            raise LedgerError(f"Completing request {request_id!r} requires the claim fence")
        if existing.effect_protocol_required and not _has_allowed_attempting_decision(existing):
            raise LedgerOutcomeAlreadySetError(
                f"Cannot complete request {request_id!r}: no durable ATTEMPTING decision"
            )
        entry = replace(
            existing,
            status=legacy_status_from_terminal(TerminalOutcome.COMPLETED),
            terminal_outcome=TerminalOutcome.COMPLETED.value,
            result=_evidence_value(result),
            finished_at=time.time(),
            lease_until=None,
            side_effect_boundary=SideEffectBoundary.CROSSED.value,
            effect_phase=EffectState.COMMITTED.value,
        )
        if not self._try_transition(
            entry,
            expected_from=_expected_from,
            expected_owner=_expected_owner,
            expected_fence=fence,
            expected_effect_state=(
                EffectState.ATTEMPTING.value
                if existing.effect_protocol_required
                else None
            ),
        ):
            current = self._get_entry(request_id)
            raise LedgerOutcomeAlreadySetError(
                f"Cannot complete request {request_id!r}: "
                f"terminal outcome already set to "
                f"{current.terminal_outcome if current else '?'} "
                f"(expected from {_expected_from or {'IN_FLIGHT'}})"
                + (
                    f", owner mismatch (expected {_expected_owner})"
                    if _expected_owner is not None
                    else ""
                )
            )
        return entry

    def fail(
        self,
        request_id: str,
        error: BaseException,
        *,
        failed_after_effect: bool = False,
        expected_fence: int | None = None,
        _expected_from: frozenset[str] | None = None,
        _expected_owner: str | None = None,
        _expected_fence: int | None = None,
    ) -> LedgerEntry:
        existing = self._get_entry(request_id)
        if existing is None:
            raise LedgerError(f"Cannot fail unknown request {request_id!r}")
        if expected_fence is not None and _expected_fence is not None:
            if expected_fence != _expected_fence:
                raise LedgerError("conflicting expected fence values")
        fence = expected_fence if expected_fence is not None else _expected_fence
        if fence is None:
            raise LedgerError(f"Failing request {request_id!r} requires the claim fence")
        terminal = (
            TerminalOutcome.FAILED_AFTER_EFFECT
            if failed_after_effect
            else TerminalOutcome.FAILED_BEFORE_EFFECT
        )
        boundary = (
            SideEffectBoundary.CROSSED.value
            if failed_after_effect
            else existing.side_effect_boundary
        )
        entry = replace(
            existing,
            status=legacy_status_from_terminal(terminal),
            terminal_outcome=terminal.value,
            error=_evidence_error(error),
            finished_at=time.time(),
            lease_until=None,
            side_effect_boundary=boundary,
            effect_phase=(
                existing.effect_phase
                if failed_after_effect
                and existing.effect_protocol_required
                and existing.effect_phase == EffectState.ATTEMPTING.value
                and existing.decision is not None
                else EffectState.ABORTED.value
            ),
        )
        if not self._try_transition(
            entry,
            expected_from=_expected_from,
            expected_owner=_expected_owner,
            expected_fence=fence,
        ):
            current = self._get_entry(request_id)
            raise LedgerOutcomeAlreadySetError(
                f"Cannot fail request {request_id!r}: "
                f"terminal outcome already set to "
                f"{current.terminal_outcome if current else '?'} "
                f"(expected from {_expected_from or {'IN_FLIGHT'}}"
                + (
                    f", owner mismatch (expected {_expected_owner})"
                    if _expected_owner is not None
                    else ""
                )
            )
        return entry

    def attach_receipt_ref(
        self,
        request_id: str,
        receipt_ref: str,
        *,
        expected_owner: str | None = None,
        expected_fence: int | None = None,
    ) -> LedgerEntry:
        existing = self._get_entry(request_id)
        if existing is None:
            raise LedgerError(f"Cannot attach receipt to unknown request {request_id!r}")
        if expected_fence is None:
            raise LedgerError(f"Attaching a receipt to {request_id!r} requires the claim fence")
        entry = replace(existing, receipt_ref=receipt_ref)
        if not self._try_transition(
            entry,
            expected_from=frozenset({existing.terminal_outcome}),
            expected_owner=expected_owner,
            expected_fence=expected_fence,
        ):
            raise LedgerOutcomeAlreadySetError(
                f"Cannot attach receipt to {request_id!r}: transition superseded"
            )
        return entry

    def attach_external_operation_ref(
        self,
        request_id: str,
        ref: str,
        *,
        expected_owner: str | None = None,
        expected_fence: int | None = None,
    ) -> LedgerEntry:
        """Store the provider's operation handle on a transition entry.

        Durable and used later for reconciliation. Backs
        :func:`record_external_operation`.
        """
        existing = self._get_entry(request_id)
        if existing is None:
            raise LedgerError(
                f"Cannot attach external operation ref to unknown request {request_id!r}"
            )
        if expected_fence is None:
            raise LedgerError(
                f"Attaching an external operation to {request_id!r} requires the claim fence"
            )
        if existing.effect_protocol_required and not _has_allowed_attempting_decision(existing):
            raise LedgerOutcomeAlreadySetError(
                f"Cannot attach external operation ref to {request_id!r}: "
                "no durable ATTEMPTING decision"
            )
        entry = replace(existing, external_operation_ref=ref)
        if not self._try_transition(
            entry,
            expected_from=frozenset({existing.terminal_outcome}),
            expected_owner=expected_owner,
            expected_fence=expected_fence,
            expected_effect_state=(
                EffectState.ATTEMPTING.value if existing.effect_protocol_required else None
            ),
        ):
            raise LedgerOutcomeAlreadySetError(
                f"Cannot attach external operation ref to {request_id!r}: transition superseded"
            )
        return entry

    def attach_provider_idempotency_key(
        self,
        request_id: str,
        provider_key: str,
        *,
        expected_owner: str | None = None,
        expected_fence: int | None = None,
    ) -> LedgerEntry:
        """Persist provider idempotency key on the claimed transition row.

        Used by wrapper-path auto-propagation (effect_id -> provider key):
        after ATTEMPTING decision CAS succeeds, before tool body starts.
        """
        existing = self._get_entry(request_id)
        if existing is None:
            raise LedgerError(
                f"Cannot attach provider idempotency key to unknown request {request_id!r}"
            )
        if expected_fence is None:
            raise LedgerError(
                f"Attaching provider idempotency key to {request_id!r} requires the claim fence"
            )
        key = str(provider_key)
        stored_provider_key = existing.provider_idempotency_key
        if stored_provider_key is not None and stored_provider_key != key:
            raise LedgerOutcomeAlreadySetError(
                f"Cannot attach provider idempotency key to {request_id!r}: key mismatch "
                f"({existing.provider_idempotency_key!r} != {key!r})"
            )
        first_attempt = existing.provider_key_first_attempt_at
        if first_attempt is None:
            first_attempt = time.time()
        entry = replace(
            existing,
            provider_idempotency_key=key,
            provider_key_first_attempt_at=first_attempt,
        )
        if not self._try_transition(
            entry,
            expected_from=frozenset({existing.terminal_outcome}),
            expected_owner=expected_owner,
            expected_fence=expected_fence,
            expected_effect_state=(
                EffectState.ATTEMPTING.value if existing.effect_protocol_required else None
            ),
        ):
            raise LedgerOutcomeAlreadySetError(
                f"Cannot attach provider idempotency key to {request_id!r}: transition superseded"
            )
        return entry

    def renew_lease(
        self,
        request_id: str,
        *,
        lease_ttl: float | None = None,
        now: float | None = None,
        expected_fence: int | None = None,
        _expected_owner: str | None = None,
        _expected_fence: int | None = None,
    ) -> LedgerEntry:
        """Extend ``lease_until`` for an in-flight transition.

        Owner-side heartbeat for long work: keeps peers on ``POLL`` instead of
        opening reclaim. This is the renew half of the ``REPAIR`` taxonomy
        (heal incomplete durable fields via :meth:`repair_transition`; extend a
        still-held lease here). Only applies while the stored terminal outcome
        is still ``IN_FLIGHT`` (before lease expiry is applied). Renewing after
        the lease has already expired raises :class:`LedgerError` — reclaim /
        reconcile must run instead of silently re-asserting ownership.

        Uses CAS (``try_transition``) with owner + lease-held preconditions so
        a concurrent complete / reclaim / expiry cannot be clobbered by a
        stale renew (TOCTOU).

        Backs :func:`renew_lease`.
        """
        existing = self._get_entry(request_id)
        if existing is None:
            raise LedgerError(f"Cannot renew lease for unknown request {request_id!r}")
        if expected_fence is not None and _expected_fence is not None:
            if expected_fence != _expected_fence:
                raise LedgerError("conflicting expected fence values")
        fence = expected_fence if expected_fence is not None else _expected_fence
        if fence is None:
            raise LedgerError(f"Renewing request {request_id!r} requires the claim fence")
        now = now if now is not None else time.time()
        stored = (
            existing.terminal_outcome
            if isinstance(existing.terminal_outcome, TerminalOutcome)
            else TerminalOutcome(str(existing.terminal_outcome))
        )
        if stored != TerminalOutcome.IN_FLIGHT:
            raise LedgerError(
                f"Cannot renew lease for request {request_id!r}: "
                f"terminal_outcome is {stored.value}, not IN_FLIGHT"
            )
        validity = resolve_lease_validity(existing.lease_until, now=now)
        if validity == LeaseValidity.EXPIRED:
            raise LedgerError(
                f"Cannot renew lease for request {request_id!r}: "
                "lease already expired — reclaim or reconcile instead"
            )
        ttl = self._lease_ttl if lease_ttl is None else lease_ttl
        if ttl <= 0:
            raise LedgerError("lease_ttl must be positive to renew")
        entry = replace(existing, lease_until=now + ttl, last_heartbeat_at=now)
        if not self._try_transition(
            entry,
            expected_from=_IN_FLIGHT_OUTCOMES,
            expected_owner=(existing.owner if _expected_owner is None else _expected_owner),
            require_lease_held_at=now,
            expected_fence=fence,
        ):
            current = self._get_entry(request_id)
            if current is None:
                raise LedgerError(f"Cannot renew lease for unknown request {request_id!r}")
            current_outcome = (
                current.terminal_outcome
                if isinstance(current.terminal_outcome, TerminalOutcome)
                else TerminalOutcome(str(current.terminal_outcome))
            )
            if current_outcome != TerminalOutcome.IN_FLIGHT:
                raise LedgerError(
                    f"Cannot renew lease for request {request_id!r}: "
                    f"terminal_outcome is {current_outcome.value}, not IN_FLIGHT"
                )
            if current.owner != existing.owner:
                raise LedgerError(
                    f"Cannot renew lease for request {request_id!r}: "
                    "owner changed (reclaimed by peer)"
                )
            if resolve_lease_validity(current.lease_until, now=now) == (LeaseValidity.EXPIRED):
                raise LedgerError(
                    f"Cannot renew lease for request {request_id!r}: "
                    "lease already expired — reclaim or reconcile instead"
                )
            raise LedgerError(
                f"Cannot renew lease for request {request_id!r}: "
                "concurrent transition rejected renew"
            )
        return entry

    def repair_transition(self, request_id: str) -> LedgerEntry:
        """Heal incomplete durable transition fields before re-resolving.

        Fills missing ``idempotency_key`` / ``side_effect_boundary`` / terminal
        alignment. Does not renew a peer lease and does not execute the tool.
        Claim loops call this when the gate is ``REPAIR``, then re-resolve.
        """
        existing = self._get_entry(request_id)
        if existing is None:
            raise LedgerError(f"Cannot repair unknown request {request_id!r}")
        updates = repair_transition_fields(existing)
        if not updates:
            if transition_needs_repair(existing):
                raise LedgerError(
                    f"Cannot repair request {request_id!r}: incomplete context "
                    "with no safe field updates"
                )
            return existing
        entry = replace(existing, **updates)
        if transition_needs_repair(entry):
            raise LedgerError(
                f"Cannot repair request {request_id!r}: still incomplete after safe field updates"
            )
        if not self._try_transition(
            entry,
            expected_from=frozenset({existing.terminal_outcome}),
            expected_owner=existing.owner,
            expected_fence=existing.fence,
        ):
            raise LedgerOutcomeAlreadySetError(
                f"Cannot repair request {request_id!r}: transition superseded"
            )
        return entry

    def mark_blocked(
        self,
        request_id: str,
        *,
        error: str | None = None,
        expected_fence: int | None = None,
        _expected_from: frozenset[str] | None = None,
        _expected_owner: str | None = None,
        _expected_fence: int | None = None,
    ) -> LedgerEntry:
        existing = self._get_entry(request_id)
        if existing is None:
            raise LedgerError(f"Cannot block unknown request {request_id!r}")
        if expected_fence is not None and _expected_fence is not None:
            if expected_fence != _expected_fence:
                raise LedgerError("conflicting expected fence values")
        fence = expected_fence if expected_fence is not None else _expected_fence
        if fence is None:
            raise LedgerError(f"Blocking request {request_id!r} requires the claim fence")
        entry = replace(
            existing,
            status=legacy_status_from_terminal(TerminalOutcome.BLOCKED),
            terminal_outcome=TerminalOutcome.BLOCKED.value,
            error=error,
            finished_at=time.time(),
            lease_until=None,
            effect_phase=(
                existing.effect_phase
                if existing.effect_protocol_required
                and existing.effect_phase == EffectState.ATTEMPTING.value
                and existing.decision is not None
                else EffectState.ABORTED.value
            ),
        )
        if not self._try_transition(
            entry,
            expected_from=_expected_from,
            expected_owner=_expected_owner,
            expected_fence=fence,
        ):
            current = self._get_entry(request_id)
            raise LedgerOutcomeAlreadySetError(
                f"Cannot block request {request_id!r}: "
                f"terminal outcome already set to "
                f"{current.terminal_outcome if current else '?'}"
            )
        return entry

    def mark_unknown(
        self,
        request_id: str,
        *,
        error: str | None = None,
        expected_fence: int | None = None,
        _expected_from: frozenset[str] | None = None,
        _expected_owner: str | None = None,
        _expected_fence: int | None = None,
    ) -> LedgerEntry:
        existing = self._get_entry(request_id)
        if existing is None:
            raise LedgerError(f"Cannot mark unknown request {request_id!r}")
        if expected_fence is not None and _expected_fence is not None:
            if expected_fence != _expected_fence:
                raise LedgerError("conflicting expected fence values")
        fence = expected_fence if expected_fence is not None else _expected_fence
        if fence is None:
            raise LedgerError(f"Marking request {request_id!r} unknown requires the claim fence")
        entry = replace(
            existing,
            status=legacy_status_from_terminal(TerminalOutcome.UNKNOWN),
            terminal_outcome=TerminalOutcome.UNKNOWN.value,
            error=error,
            finished_at=time.time(),
            lease_until=None,
            effect_phase=(
                existing.effect_phase
                if existing.effect_protocol_required
                and existing.effect_phase == EffectState.ATTEMPTING.value
                and existing.decision is not None
                else EffectState.ABORTED.value
            ),
        )
        if not self._try_transition(
            entry,
            expected_from=_expected_from,
            expected_owner=_expected_owner,
            expected_fence=fence,
        ):
            current = self._get_entry(request_id)
            raise LedgerOutcomeAlreadySetError(
                f"Cannot mark unknown request {request_id!r}: "
                f"terminal outcome already set to "
                f"{current.terminal_outcome if current else '?'}"
            )
        return entry

    def mark_worker_dead(
        self,
        owner: str,
        *,
        by: str,
        reason: str,
        now: float | None = None,
        override_heartbeat: bool = False,
    ) -> list[LedgerEntry]:
        """Assert that all transitions owned by *owner* are from a dead worker.

        Scans ``list_all()`` and stamps ``worker_dead_asserted_by`` /
        ``worker_dead_asserted_at`` on every entry whose ``owner`` matches and
        whose resolved outcome is ``IN_FLIGHT`` or ``EXPIRED``.  Entries whose
        ``last_heartbeat_at`` (falling back to ``started_at``) is within the
        grace window (``presumed_dead_after``) are **refused** — you cannot
        declare a currently-heartbeating worker dead.  Pass
        ``override_heartbeat=True`` to bypass this check when the operator has
        direct evidence of death (e.g. they killed the pod).  Bypassing may
        cause a duplicate effect if the worker is still alive.

        This is the channel for orchestrator events (k8s OOM-kill hooks,
        LangGraph redispatch sweeps) and humans.

        Returns the list of stamped entries (may be empty if no matching entries
        exist).
        """
        if not by:
            raise LedgerReleaseRefusedError("mark_worker_dead requires an operator identity ('by')")
        if not reason:
            raise LedgerReleaseRefusedError("mark_worker_dead requires a reason")
        now = now if now is not None else time.time()
        stamped: list[LedgerEntry] = []
        for entry in self._storage.list_all():
            if entry.owner != owner:
                continue
            resolved = entry.resolved_terminal_outcome(now=now)
            if resolved not in (TerminalOutcome.IN_FLIGHT, TerminalOutcome.EXPIRED):
                continue
            # Refuse if the worker appears alive (recent heartbeat).
            if not override_heartbeat and not has_worker_death_evidence(
                entry, now=now, presumed_dead_after=self._presumed_dead_after
            ):
                grace = _grace_remaining(
                    entry,
                    now=now,
                    presumed_dead_after=self._presumed_dead_after,
                )
                raise LedgerWorkerAliveError(
                    f"Cannot mark worker dead for owner {owner!r}: request "
                    f"{entry.request_id!r} has recent heartbeat "
                    f"({_format_heartbeat_age(entry, now=now)}) — "
                    f"grace window elapses in {grace}"
                )
            stored_reason = f"{reason} (heartbeat overridden)" if override_heartbeat else reason
            dead_entry = replace(
                entry,
                worker_dead_asserted_by=by,
                worker_dead_asserted_at=now,
                resolution_reason=stored_reason,
            )
            if not self._try_transition(
                dead_entry,
                expected_from=frozenset({entry.terminal_outcome}),
                expected_owner=entry.owner,
                expected_fence=entry.fence,
            ):
                continue
            stamped.append(dead_entry)
        return stamped

    def mark_worker_dead_for(
        self,
        request_id: str,
        *,
        by: str,
        reason: str,
        now: float | None = None,
        override_heartbeat: bool = False,
    ) -> LedgerEntry:
        """Assert that a specific transition's worker is dead.

        Per-entry variant of :meth:`mark_worker_dead`.  Stamps
        ``worker_dead_asserted_by`` / ``worker_dead_asserted_at`` on the named
        entry.  Refuses if the entry's ``last_heartbeat_at`` (or
        ``started_at`` fallback) is within the grace window
        (``presumed_dead_after``) **unless** ``override_heartbeat=True``.

        When ``override_heartbeat=True``, the liveness check is bypassed and
        ``" (heartbeat overridden)`` is appended to *reason* in the stored
        audit trail.  Use this only when the operator has direct evidence the
        worker is dead (e.g. they killed the pod themselves).  Bypassing the
        check may cause a duplicate effect if the worker is still alive.
        """
        if not by:
            raise LedgerReleaseRefusedError(
                "mark_worker_dead_for requires an operator identity ('by')"
            )
        if not reason:
            raise LedgerReleaseRefusedError("mark_worker_dead_for requires a reason")
        now = now if now is not None else time.time()
        existing = self._get_entry(request_id)
        if existing is None:
            raise LedgerError(f"Cannot mark worker dead for unknown request {request_id!r}")
        resolved = existing.resolved_terminal_outcome(now=now)
        if resolved not in (TerminalOutcome.IN_FLIGHT, TerminalOutcome.EXPIRED):
            raise LedgerReleaseRefusedError(
                f"Cannot mark worker dead for request {request_id!r}: "
                f"resolved outcome is {resolved.value}, not IN_FLIGHT or EXPIRED"
            )
        if not override_heartbeat and not has_worker_death_evidence(
            existing, now=now, presumed_dead_after=self._presumed_dead_after
        ):
            grace = _grace_remaining(
                existing,
                now=now,
                presumed_dead_after=self._presumed_dead_after,
            )
            raise LedgerWorkerAliveError(
                f"Cannot mark worker dead for request {request_id!r}: "
                f"worker appears alive "
                f"({_format_heartbeat_age(entry=existing, now=now)}) — "
                f"grace window elapses in {grace}. "
                "Use --override-heartbeat if the operator has direct evidence "
                "of death (bypasses liveness check; may cause a duplicate "
                "effect if the worker is alive)."
            )
        stored_reason = f"{reason} (heartbeat overridden)" if override_heartbeat else reason
        entry = replace(
            existing,
            worker_dead_asserted_by=by,
            worker_dead_asserted_at=now,
            resolution_reason=stored_reason,
        )
        if not self._try_transition(
            entry,
            expected_from=frozenset({existing.terminal_outcome}),
            expected_owner=existing.owner,
            expected_fence=existing.fence,
        ):
            raise LedgerOutcomeAlreadySetError(
                f"Cannot mark worker dead for {request_id!r}: transition superseded"
            )
        return entry

    def advance_boundary(
        self,
        request_id: str,
        boundary: SideEffectBoundary,
        *,
        expected_owner: str | None = None,
        expected_fence: int | None = None,
    ) -> LedgerEntry:
        """Move an entry's side-effect boundary forward (monotonic).

        Only advances toward ``CROSSED`` and never regresses, so concurrent or
        out-of-order markers cannot weaken a stronger recorded boundary. Backs
        the :func:`side_effect` marker used by side-effecting tools.
        """
        existing = self._get_entry(request_id)
        if existing is None:
            raise LedgerError(f"Cannot advance boundary for unknown request {request_id!r}")
        if expected_fence is None:
            raise LedgerError(f"Advancing request {request_id!r} requires the claim fence")
        if existing.effect_protocol_required and not _has_allowed_attempting_decision(existing):
            raise LedgerOutcomeAlreadySetError(
                f"Cannot advance boundary for {request_id!r}: "
                "no durable ATTEMPTING decision"
            )
        current = SideEffectBoundary(existing.side_effect_boundary)
        entry = (
            existing
            if _BOUNDARY_RANK[boundary] <= _BOUNDARY_RANK[current]
            else replace(existing, side_effect_boundary=boundary.value)
        )
        if not self._try_transition(
            entry,
            expected_from=frozenset({existing.terminal_outcome}),
            expected_owner=expected_owner,
            expected_fence=expected_fence,
            expected_effect_state=(
                EffectState.ATTEMPTING.value if existing.effect_protocol_required else None
            ),
        ):
            raise LedgerOutcomeAlreadySetError(
                f"Cannot advance boundary for {request_id!r}: transition superseded"
            )
        return entry

    def record_decision(
        self,
        request_id: str,
        decision: dict[str, Any],
        *,
        expected_owner: str | None = None,
        expected_fence: int | None = None,
    ) -> LedgerEntry:
        """Stamp the single-decision-point result onto the entry atomically.

        The write is the ``INTENDED -> ATTEMPTING`` transition: it goes through
        the same fenced compare-and-swap as every other in-flight mutation, so a
        superseded worker (stale fence) cannot record a decision — and therefore
        cannot smuggle in an effect the current-fence decision would deny. The
        entry stays ``IN_FLIGHT``; only the durable ``decision`` field changes.
        """
        existing = self._get_entry(request_id)
        if existing is None:
            raise LedgerError(f"Cannot record decision for unknown request {request_id!r}")
        if expected_fence is None:
            raise LedgerError(f"Recording a decision for {request_id!r} requires the claim fence")
        from mycelium.decision import Decision

        try:
            parsed = Decision.from_dict(decision)
        except (KeyError, TypeError, ValueError) as exc:
            raise LedgerError(f"Invalid decision for request {request_id!r}: {exc}") from exc
        from mycelium.secret_protection import sanitize_for_decision_evidence

        parsed = Decision.from_dict(sanitize_for_decision_evidence(parsed.to_dict()))
        entry = replace(
            existing,
            decision=parsed.to_dict(),
            effect_phase=(
                EffectState.ATTEMPTING.value if parsed.allowed else EffectState.ABORTED.value
            ),
        )
        if not self._try_transition(
            entry,
            expected_from=_IN_FLIGHT_OUTCOMES,
            expected_owner=expected_owner,
            expected_fence=expected_fence,
            expected_effect_state=EffectState.INTENDED.value,
        ):
            raise LedgerOutcomeAlreadySetError(
                f"Cannot record decision for {request_id!r}: "
                "transition superseded (stale fence/owner or already resolved)"
            )
        return entry

    # --- request id derivation ---

    def derive_request_id(
        self,
        tool: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        *,
        transition_binding: ToolTransitionBinding | None = None,
        identity_kwargs: dict[str, Any] | None = None,
    ) -> str:
        """Determine the request id for a tool invocation.

        An explicit ``request_id`` kwarg is the transition identity: retries
        that reuse it map to the same ledger entry. The host must derive it
        from a stable, server-owned business record
        (``f"charge-order:{order_id}"``), not from model output.

        ``request_id_from`` on the binding mints
        ``{tool}:{field}:{value}`` when ``request_id`` is omitted.

        When both are omitted:

        * ``require_explicit`` + a consequential side-effect class raises
          :class:`MissingRequestIdentityError` (no ``tool_call_id`` /
          random fallback).
        * ``derived`` (default) keeps the previous identity:
          transition key, then ``tool_call_id``, Session hash, or UUID.

        ``request_id`` is never part of the argument fingerprint and is not
        forwarded to the wrapped tool.
        """
        lookup = identity_kwargs if identity_kwargs is not None else kwargs
        explicit = parse_explicit_request_id(kwargs) or parse_explicit_request_id(lookup)
        if explicit is not None:
            return explicit

        field = transition_binding.request_id_from if transition_binding is not None else None
        if field:
            return request_id_from_argument(tool, field, lookup)

        if (
            self._request_identity_policy == REQUEST_IDENTITY_POLICY_REQUIRE_EXPLICIT
            and transition_binding is not None
            and transition_binding.side_effect_class in CONSEQUENTIAL_SIDE_EFFECT_CLASSES
        ):
            raise MissingRequestIdentityError(tool=tool)

        if transition_binding is not None:
            return derive_transition_key_for_call(tool, args, kwargs, transition_binding)

        if "tool_call_id" in kwargs:
            return str(kwargs["tool_call_id"])
        active_dispatch_id = get_active_dispatch_id()
        if active_dispatch_id is not None:
            return active_dispatch_id

        session = _session_var.get()
        if session is not None:
            return self._session_request_id(session, tool, args, kwargs)

        warnings.warn(
            f"Tool {tool!r} has no request_id, tool_call_id, or Session; "
            "ActionLedger cannot deduplicate this call. A random UUID will be used.",
            stacklevel=4,
        )
        return f"no-session:{tool}:{uuid.uuid4()}"

    def _session_request_id(
        self, session: Session, tool: str, args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> str:
        # Stable within the process for the lifetime of the Session object.
        run_key = f"run-{id(session)}"
        args_hash = self._hash_args(args, kwargs)
        return f"{run_key}:{tool}:{args_hash}"

    @staticmethod
    def _hash_args(args: tuple[Any, ...], kwargs: dict[str, Any]) -> str:
        from mycelium.secret_protection import fingerprint_args, get_active_secret_policy

        policy = get_active_secret_policy()
        if policy is not None and policy.enabled:
            digest = fingerprint_args(args, kwargs)
            return digest[:16]
        payload = json.dumps(
            {"args": args, "kwargs": kwargs},
            sort_keys=True,
            default=str,
        )
        return hashlib.sha256(payload.encode()).hexdigest()[:16]


def _bind_args(args: tuple[Any, ...], kwargs: dict[str, Any]) -> dict[str, Any]:
    """Store a serializable snapshot of the call arguments."""
    return {
        "args": list(args),
        "kwargs": dict(kwargs),
    }


def _drop_ledger_keys(kwargs: dict[str, Any]) -> dict[str, Any]:
    """Remove Mycelium bookkeeping keys before calling the actual tool."""
    return {k: v for k, v in kwargs.items() if k not in LEDGER_KWARG_KEYS}


def _identity_scopes_differ(
    existing: LedgerEntry,
    kwargs: dict[str, Any],
    binding: ToolTransitionBinding | None,
) -> bool:
    """True when incoming scope does not match the stored claim's scope."""
    scope_from = binding.scope_from if binding is not None else {}
    incoming = resolve_scope(scope_from=scope_from, kwargs=kwargs)
    stored = _scope_from_stored_kwargs(dict(existing.kwargs), scope_from)
    return (
        incoming.thread_id != stored[0]
        or incoming.run_id != stored[1]
        or incoming.node != stored[2]
    )


def _scope_from_stored_kwargs(
    kwargs: dict[str, Any],
    scope_from: dict[str, str],
) -> tuple[str, str, str]:
    """Read scope from a stored claim only — not the current execution_scope."""
    resolved = {"thread_id": "", "run_id": "", "node": ""}
    for field_name, source in scope_from.items():
        if field_name not in resolved:
            continue
        if source in kwargs and kwargs[source]:
            resolved[field_name] = str(kwargs[source])
    for field_name in resolved:
        if field_name in kwargs and kwargs[field_name]:
            resolved[field_name] = str(kwargs[field_name])
    return (resolved["thread_id"], resolved["run_id"], resolved["node"])


def _args_drift_exclude_keys(
    binding: ToolTransitionBinding | None,
) -> frozenset[str]:
    """Keys omitted from args-drift fingerprints (provider-key gate owns these)."""
    if binding is None or binding.provider_idempotency_key_param is None:
        return frozenset()
    return frozenset({binding.provider_idempotency_key_param})


def _evidence_value(value: Any) -> Any:
    from mycelium.destructive_confirm import (
        get_active_destructive_policy,
        sanitize_destructive_evidence,
    )
    from mycelium.entity_guard import get_active_entity_policy, sanitize_entity_evidence
    from mycelium.secret_protection import get_active_secret_policy, sanitize_for_evidence

    result = value
    policy = get_active_secret_policy()
    if policy is not None and policy.enabled:
        result = sanitize_for_evidence(result)
    if get_active_entity_policy() is not None:
        if isinstance(result, dict):
            _args, scrubbed = sanitize_entity_evidence((), result)
            del _args
            result = scrubbed
    if get_active_destructive_policy() is not None and isinstance(result, dict):
        _args, scrubbed = sanitize_destructive_evidence((), result)
        del _args
        return scrubbed
    return result


def _evidence_args(args: Any, kwargs: Any) -> tuple[list[Any], dict[str, Any]]:
    from mycelium.destructive_confirm import (
        get_active_destructive_policy,
        sanitize_destructive_evidence,
    )
    from mycelium.entity_guard import get_active_entity_policy, sanitize_entity_evidence
    from mycelium.secret_protection import get_active_secret_policy, sanitize_secrets

    out_args: list[Any] = list(args)
    out_kwargs: dict[str, Any] = dict(kwargs)
    policy = get_active_secret_policy()
    if policy is not None and policy.enabled:
        safe = sanitize_secrets(
            {"args": out_args, "kwargs": out_kwargs},
            entropy_detection=policy.entropy_detection,
            allow_fields=policy.allow_fields,
        )
        out_args, out_kwargs = list(safe["args"]), dict(safe["kwargs"])
    if get_active_entity_policy() is not None:
        out_args, out_kwargs = sanitize_entity_evidence(out_args, out_kwargs)
    if get_active_destructive_policy() is not None:
        out_args, out_kwargs = sanitize_destructive_evidence(out_args, out_kwargs)
    return out_args, out_kwargs


def _evidence_error(error: BaseException) -> str:
    from mycelium.secret_protection import (
        get_active_secret_policy,
        sanitize_exception,
        sanitize_text,
    )

    policy = get_active_secret_policy()
    if policy is None or not policy.enabled:
        return f"{type(error).__name__}: {error}"
    safe = sanitize_exception(error)
    return sanitize_text(f"{type(safe).__name__}: {safe}")


def _args_drift_fingerprint(
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    *,
    exclude: frozenset[str],
) -> str:
    from mycelium.secret_protection import fingerprint_args, get_active_secret_policy

    filtered = (
        kwargs
        if not exclude
        else {key: value for key, value in kwargs.items() if key not in exclude}
    )
    policy = get_active_secret_policy()
    digest = (
        fingerprint_args(args, filtered)
        if policy is not None and policy.enabled
        else args_fingerprint(args, filtered)
    )
    from mycelium.entity_guard import destination_fingerprint, get_active_entity_decision

    dests = destination_fingerprint(get_active_entity_decision())
    from mycelium.destructive_confirm import (
        destructive_fingerprint,
        get_active_destructive_decision,
        get_active_destructive_policy,
        sanitize_destructive_evidence,
    )

    if get_active_destructive_policy() is not None:
        scrubbed_args, scrubbed_kwargs = sanitize_destructive_evidence(args, filtered)
        digest = (
            fingerprint_args(tuple(scrubbed_args), scrubbed_kwargs)
            if policy is not None and policy.enabled
            else args_fingerprint(tuple(scrubbed_args), scrubbed_kwargs)
        )
    destructive = destructive_fingerprint(get_active_destructive_decision())
    from mycelium.use_time_currency import (
        get_pending_use_time_facts,
        use_time_fingerprint,
    )

    currency = use_time_fingerprint(get_pending_use_time_facts())
    extra = tuple(dests) + tuple(destructive) + tuple(currency)
    if not extra:
        return digest
    import hashlib

    return hashlib.sha256(f"{digest}|{'|'.join(extra)}".encode()).hexdigest()


def _args_drift_scope_key(kwargs: dict[str, Any]) -> str | None:
    """Return ``run_id`` or fallback ``thread_id`` for args-drift isolation."""
    scope = get_active_execution_scope()
    run_id = kwargs.get("run_id") or (scope.run_id if scope else None)
    if run_id:
        return str(run_id)
    thread_id = kwargs.get("thread_id") or (scope.thread_id if scope else None)
    if thread_id:
        return str(thread_id)
    return None


def _args_drift_scopes_match(incoming: str | None, stored: str | None) -> bool:
    """True when both sides share a scope, or both are unscoped (legacy)."""
    if incoming is None and stored is None:
        return True
    if incoming is None or stored is None:
        return False
    return incoming == stored


def _canonical_call_mapping(
    func: Callable[..., Any], args: tuple[Any, ...], kwargs: Mapping[str, Any]
) -> dict[str, Any]:
    mapping = dict(kwargs)
    try:
        bound = inspect.signature(func).bind_partial(*args, **kwargs)
        bound.apply_defaults()
        mapping.update(bound.arguments)
    except (TypeError, ValueError):
        pass
    return mapping


def _use_boundary_call_mapping(
    func: Callable[..., Any],
    args: tuple[Any, ...],
    clean_kwargs: Mapping[str, Any],
    dispatch_kwargs: Mapping[str, Any],
) -> dict[str, Any]:
    """Canonical tool args for use-boundary validation.

    Authorize-time ``request_id`` is retained for call comparisons / validators.
    Configured request bindings at USE compare against the current trusted
    ``dispatch_scope`` (see use_time_currency._current_context_ids), not this
    frozen authorize-time copy.
    """
    mapping = _canonical_call_mapping(func, args, clean_kwargs)
    request_id = dispatch_kwargs.get("request_id")
    if isinstance(request_id, str) and request_id.strip():
        mapping["request_id"] = request_id
    return mapping


def _claim_kwargs(kwargs: dict[str, Any], clean_kwargs: dict[str, Any]) -> dict[str, Any]:
    """Kwargs for claim: tool args plus optional bookkeeping pass-through.

    ``state_ref`` / ``decision_id`` / handoff ids / dispatch and scope ids are
    bookkeeping (excluded from the tool body and ``args_fingerprint``) but must
    still reach ``_new_inflight_entry`` for audit and the opt-in args-drift gate
    (same dispatch ticket + different args, scoped by ``run_id`` / ``thread_id``).
    """
    claim_kwargs = dict(clean_kwargs)
    for key in (
        "decision_id",
        "state_ref",
        "parent_request_id",
        "handoff_id",
        "request_id",
        "tool_call_id",
        "thread_id",
        "run_id",
        "node",
    ):
        if key in kwargs and kwargs[key] is not None:
            claim_kwargs[key] = kwargs[key]
    scope = get_active_execution_scope()
    if scope is not None:
        for key, value in (
            ("thread_id", scope.thread_id),
            ("run_id", scope.run_id),
            ("node", scope.node),
        ):
            if key not in claim_kwargs and value:
                claim_kwargs[key] = value
    return claim_kwargs


def _emit_tool_receipt(
    audit_emitter: AuditReceiptEmitter | None,
    ledger: ActionLedger,
    request_id: str,
    *,
    expected_owner: str | None,
    expected_fence: int,
) -> None:
    if audit_emitter is None:
        return
    entry = ledger.get(request_id)
    if entry is None:
        return
    outcome = entry.resolved_terminal_outcome()
    if outcome not in (
        TerminalOutcome.COMPLETED,
        TerminalOutcome.FAILED_BEFORE_EFFECT,
        TerminalOutcome.FAILED_AFTER_EFFECT,
    ):
        return
    receipt = audit_emitter.emit_from_tool_entry(entry)
    ledger.attach_receipt_ref(
        request_id,
        receipt.receipt_id,
        expected_owner=expected_owner,
        expected_fence=expected_fence,
    )


def _is_read_only_binding(
    transition_binding: ToolTransitionBinding | None,
) -> bool:
    return (
        transition_binding is not None
        and transition_binding.side_effect_class == SideEffectClass.READ
    )


def _claim_for_transition(
    ledger: ActionLedger,
    request_id: str,
    tool_name: str,
    args: tuple[Any, ...],
    clean_kwargs: dict[str, Any],
    transition_binding: ToolTransitionBinding | None,
) -> LedgerEntry:
    if _is_read_only_binding(transition_binding):
        return ledger.claim_read_only(request_id, tool_name, args, clean_kwargs)
    if transition_binding is not None:
        return ledger.claim_side_effecting(
            request_id,
            tool_name,
            args,
            clean_kwargs,
            transition_binding,
        )
    return ledger.claim(request_id, tool_name, args, clean_kwargs)


async def _claim_for_transition_async(
    ledger: ActionLedger,
    request_id: str,
    tool_name: str,
    args: tuple[Any, ...],
    clean_kwargs: dict[str, Any],
    transition_binding: ToolTransitionBinding | None,
) -> LedgerEntry:
    if _is_read_only_binding(transition_binding):
        return await ledger.claim_read_only_async(request_id, tool_name, args, clean_kwargs)
    if transition_binding is not None:
        return await ledger.claim_side_effecting_async(
            request_id,
            tool_name,
            args,
            clean_kwargs,
            transition_binding,
        )
    return ledger.claim(request_id, tool_name, args, clean_kwargs)


def _record_failure(
    ledger: ActionLedger,
    request_id: str,
    exc: BaseException,
    *,
    _expected_owner: str | None = None,
    _expected_fence: int | None = None,
) -> None:
    """Record a tool failure with the terminal outcome implied by the boundary.

    ``not_crossed`` → ``FAILED_BEFORE_EFFECT`` (safe to retry per policy),
    ``maybe_crossed`` → ``UNKNOWN`` (ambiguous; hard-block for reconcile),
    ``crossed`` → ``FAILED_AFTER_EFFECT`` (effect happened; hard-block).

    When *_expected_owner* / *_expected_fence* are set, the write also fences on
    the stored entry's ``owner`` / ``fence`` (wrapper-path). A stale worker whose
    claim was superseded holds a lower fence and is rejected here.
    """
    entry = ledger.get(request_id)
    boundary = (
        SideEffectBoundary(entry.side_effect_boundary)
        if entry is not None
        else SideEffectBoundary.NOT_CROSSED
    )
    if boundary == SideEffectBoundary.CROSSED:
        ledger.fail(
            request_id,
            exc,
            failed_after_effect=True,
            _expected_owner=_expected_owner,
            _expected_fence=_expected_fence,
        )
    elif boundary == SideEffectBoundary.MAYBE_CROSSED:
        ledger.mark_unknown(
            request_id,
            error=f"{type(exc).__name__}: {exc}",
            _expected_owner=_expected_owner,
            _expected_fence=_expected_fence,
        )
    else:
        ledger.fail(
            request_id,
            exc,
            _expected_owner=_expected_owner,
            _expected_fence=_expected_fence,
        )


def _record_boundary_decision(
    ledger: ActionLedger,
    request_id: str,
    *,
    tool: str,
    args: tuple[Any, ...],
    kwargs: Mapping[str, Any],
    transition_key: str | None,
    auth_decision: Any,
    currency_decision: Any,
    owner: str | None,
    fence: int | None,
) -> Any:
    """Evaluate the registered predicates and stamp the Decision atomically.

    This is the single decision point: run at the ``INTENDED -> ATTEMPTING``
    boundary after the final-boundary checks passed and before body_start. The
    built-in authority + currency predicates read the already-computed
    ``auth_decision`` / ``currency_decision`` (no re-run, no double-enforcement);
    host-registered predicates decide over the same immutable snapshot. The
    result is written under the same fenced CAS as every in-flight mutation, so
    a superseded worker cannot record — or act on — a stale decision.
    """
    from mycelium.decision import (
        Decision,
        DecisionIntent,
        build_snapshot,
        emit_policy_outcomes_after_decision,
        get_decision_engine,
        get_decision_evidence,
    )
    from mycelium.secret_protection import sanitize_for_decision_evidence

    evidence_args, evidence_kwargs = get_decision_evidence(tuple(args), kwargs)
    intent = DecisionIntent(
        tool=tool,
        args=evidence_args,
        kwargs=evidence_kwargs,
        request_id=request_id,
        transition_key=transition_key,
    )
    snapshot = build_snapshot(
        intent,
        authority_decision=auth_decision,
        currency_decision=currency_decision,
    )
    decision = get_decision_engine().evaluate(intent, snapshot)
    decision = Decision.from_dict(sanitize_for_decision_evidence(decision.to_dict()))
    try:
        ledger.record_decision(
            request_id,
            decision.to_dict(),
            expected_owner=owner,
            expected_fence=fence,
        )
    except LedgerOutcomeAlreadySetError:
        _logger.warning(
            "could not record decision for %s: transition superseded "
            "(stale fence/owner) — refusing to advance",
            request_id,
        )
        raise
    emit_policy_outcomes_after_decision(tool, request_id)
    return decision


def _boundary_denial_facts(
    blocked: Exception,
    *,
    authority_offset: int,
    currency_offset: int,
) -> tuple[Any, Any]:
    from mycelium.authority_window import (
        AuthorityExpiredError,
        get_authority_decisions,
    )
    from mycelium.use_time_currency import get_use_time_decisions

    authority = get_authority_decisions()[authority_offset:]
    currency = get_use_time_decisions()[currency_offset:]
    auth_decision = authority[-1] if authority else None
    currency_decision = currency[-1] if currency else None
    denied = SimpleNamespace(
        decision="denied",
        reason=getattr(blocked, "reason", None)
        or getattr(blocked, "violation", None)
        or type(blocked).__name__,
    )
    if isinstance(blocked, AuthorityExpiredError) and auth_decision is None:
        auth_decision = denied
    elif currency_decision is None:
        currency_decision = denied
    return auth_decision, currency_decision


def _raise_denied_decision(request_id: str, decision: Any) -> None:
    if not decision.allowed:
        raise LedgerHardBlockError(
            f"decision denied for {request_id!r}: "
            f"{'; '.join(decision.denied_reasons) or 'policy predicate refused'}"
        )


def _ensure_provider_key_for_execution(
    *,
    ledger: ActionLedger,
    request_id: str,
    transition_binding: ToolTransitionBinding | None,
    claimed_entry: LedgerEntry,
    clean_kwargs: dict[str, Any],
    call_mapping: dict[str, Any],
    owner: str | None,
    fence: int,
) -> LedgerEntry:
    """Inject and persist provider key from effect_id when policy requests it."""
    if transition_binding is None:
        return claimed_entry
    param = transition_binding.provider_idempotency_key_param
    if param is None:
        return claimed_entry
    if clean_kwargs.get(param) is not None:
        return claimed_entry
    if not should_propagate_effect_id_as_provider_key(transition_binding):
        return claimed_entry
    provider_key = claimed_entry.provider_idempotency_key or claimed_entry.effect_id
    if provider_key is None:
        raise LedgerError(
            f"Cannot derive provider idempotency key for {request_id!r}: missing effect_id"
        )
    updated = ledger.attach_provider_idempotency_key(
        request_id,
        provider_key,
        expected_owner=owner,
        expected_fence=fence,
    )
    clean_kwargs[param] = provider_key
    call_mapping[param] = provider_key
    active = _active_transition_var.get()
    if active is not None and active.request_id == request_id:
        _active_transition_var.set(
            replace(
                active,
                call_kwargs={**dict(active.call_kwargs), param: provider_key},
            )
        )
    return updated


def _identity_lookup_kwargs(
    func: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> dict[str, Any]:
    """Merge bound positional names so ``request_id_from`` can see them."""
    merged = dict(kwargs)
    try:
        bound = inspect.signature(func).bind_partial(*args, **kwargs)
        for name, value in bound.arguments.items():
            if name not in merged and name not in {"args", "kwargs"}:
                merged[name] = value
    except (TypeError, ValueError):
        return merged
    return merged


def _run_ledgered(
    func: Callable[P, R],
    tool_name: str,
    ledger: ActionLedger,
    args: P.args,
    kwargs: P.kwargs,
    audit_emitter: AuditReceiptEmitter | None = None,
    transition_binding: ToolTransitionBinding | None = None,
) -> R:
    identity_kwargs = _identity_lookup_kwargs(func, args, kwargs)
    request_id = ledger.derive_request_id(
        tool_name,
        args,
        kwargs,
        transition_binding=transition_binding,
        identity_kwargs=identity_kwargs,
    )
    clean_kwargs = _drop_ledger_keys(kwargs)
    claim_kwargs = _claim_kwargs(kwargs, clean_kwargs)
    _outcome_reexec_authorized.set(False)
    try:
        existing = _claim_for_transition(
            ledger,
            request_id,
            tool_name,
            args,
            claim_kwargs,
            transition_binding,
        )
    except LedgerHardBlockError:
        try:
            ledger._emit_outcome(
                request_id=request_id,
                tool=tool_name,
                event="resolution",
                gate="HARD_BLOCK",
                error_class="LedgerHardBlockError",
            )
        except Exception:
            _logger.exception(
                "could not emit HARD_BLOCK outcome for %s; original ledger error follows",
                request_id,
            )
        raise
    except LedgerSoftBlockError:
        try:
            ledger._emit_outcome(
                request_id=request_id,
                tool=tool_name,
                event="resolution",
                gate="SOFT_BLOCK",
                error_class="LedgerSoftBlockError",
            )
        except Exception:
            _logger.exception(
                "could not emit SOFT_BLOCK outcome for %s; original ledger error follows",
                request_id,
            )
        raise
    request_id = existing.request_id
    if existing.is_terminal_completed():
        ledger._emit_outcome(
            request_id=request_id,
            tool=tool_name,
            event="resolution",
            gate="RETURN",
            terminal_outcome=TerminalOutcome.COMPLETED,
        )
        return existing.result

    owner = _ledger_owner()
    fence = existing.fence
    authorized_reexec = _outcome_reexec_authorized.get()
    side_effect_class = (
        transition_binding.side_effect_class if transition_binding is not None else None
    )
    ledger._emit_outcome(
        request_id=request_id,
        tool=tool_name,
        event="resolution",
        gate="ALLOW",
        terminal_outcome=TerminalOutcome.IN_FLIGHT,
        side_effect_class=side_effect_class,
        authorized_reexec=authorized_reexec,
        owner=owner,
    )

    call_mapping = _use_boundary_call_mapping(func, args, clean_kwargs, kwargs)
    token = _active_transition_var.set(
        _ActiveTransition(
            ledger,
            request_id,
            transition_binding,
            call_mapping,
            owner,
            fence,
        )
    )
    try:
        from mycelium.authority_window import (
            AuthorityExpiredError,
            get_authority_decisions,
        )
        from mycelium.decision import finalize_policy_facts_at_boundary
        from mycelium.use_time_currency import (
            UseTimeCurrencyError,
            enforce_use_boundary,
            get_use_time_decisions,
        )

        finalize_policy_facts_at_boundary()
        blocked: AuthorityExpiredError | UseTimeCurrencyError | None = None
        authority_offset = len(get_authority_decisions())
        currency_offset = len(get_use_time_decisions())
        try:
            auth_decision, currency_decision = enforce_use_boundary(kwargs=call_mapping)
        except (AuthorityExpiredError, UseTimeCurrencyError) as exc:
            blocked = exc
            auth_decision, currency_decision = _boundary_denial_facts(
                blocked,
                authority_offset=authority_offset,
                currency_offset=currency_offset,
            )
            event = (
                "use_time_currency"
                if isinstance(blocked, UseTimeCurrencyError)
                else "authority_window"
            )
            try:
                ledger._emit_outcome(
                    request_id=request_id,
                    tool=tool_name,
                    event=event,
                    gate="DENY",
                    terminal_outcome=TerminalOutcome.FAILED_BEFORE_EFFECT,
                    side_effect_class=side_effect_class,
                    tool_body_executed=False,
                    authorized_reexec=authorized_reexec,
                    owner=owner,
                    error_class=type(blocked).__name__,
                    policy_version=(
                        transition_binding.policy_version
                        if transition_binding is not None
                        else None
                    ),
                )
            except Exception:
                _logger.exception(
                    "could not emit %s denial for %s",
                    event,
                    request_id,
                )

        if getattr(auth_decision, "decision", "skipped") == "allowed":
            try:
                ledger._emit_outcome(
                    request_id=request_id,
                    tool=tool_name,
                    event="authority_window",
                    gate="ALLOW",
                    terminal_outcome=TerminalOutcome.IN_FLIGHT,
                    side_effect_class=side_effect_class,
                    tool_body_executed=False,
                    authorized_reexec=authorized_reexec,
                    owner=owner,
                    policy_version=getattr(auth_decision, "policy_version", None)
                    or (
                        transition_binding.policy_version
                        if transition_binding is not None
                        else None
                    ),
                )
            except Exception:
                _logger.exception(
                    "could not emit authority_window allow for %s",
                    request_id,
                )

        if getattr(currency_decision, "decision", "skipped") == "allowed":
            try:
                ledger._emit_outcome(
                    request_id=request_id,
                    tool=tool_name,
                    event="use_time_currency",
                    gate="ALLOW",
                    terminal_outcome=TerminalOutcome.IN_FLIGHT,
                    side_effect_class=side_effect_class,
                    tool_body_executed=False,
                    authorized_reexec=authorized_reexec,
                    owner=owner,
                    policy_version=currency_decision.policy_version
                    or (
                        transition_binding.policy_version
                        if transition_binding is not None
                        else None
                    ),
                )
            except Exception:
                _logger.exception(
                    "could not emit use_time_currency allow for %s",
                    request_id,
                )

        decision = _record_boundary_decision(
            ledger,
            request_id,
            tool=tool_name,
            args=args,
            kwargs=call_mapping,
            transition_key=(
                derive_transition_key_for_call(tool_name, args, dict(kwargs), transition_binding)
                if transition_binding is not None
                else None
            ),
            auth_decision=auth_decision,
            currency_decision=currency_decision,
            owner=owner,
            fence=fence,
        )
        if blocked is not None:
            raise blocked
        from mycelium.decision import get_policy_blocked_error

        policy_blocked = get_policy_blocked_error()
        if policy_blocked is not None:
            raise policy_blocked
        _raise_denied_decision(request_id, decision)
        existing = _ensure_provider_key_for_execution(
            ledger=ledger,
            request_id=request_id,
            transition_binding=transition_binding,
            claimed_entry=existing,
            clean_kwargs=clean_kwargs,
            call_mapping=call_mapping,
            owner=owner,
            fence=fence,
        )

        ledger._emit_outcome(
            request_id=request_id,
            tool=tool_name,
            event="body_start",
            terminal_outcome=TerminalOutcome.IN_FLIGHT,
            side_effect_class=side_effect_class,
            tool_body_executed=True,
            authorized_reexec=authorized_reexec,
            owner=owner,
        )

        from mycelium.secret_protection import (
            get_active_secret_policy,
            resolve_declared_secret_fields,
        )

        policy = get_active_secret_policy()
        extra = policy.secret_fields if policy is not None else frozenset()
        exec_args, exec_kwargs = resolve_declared_secret_fields(
            func, args, clean_kwargs, extra_fields=extra
        )
        with _lease_auto_renew(
            ledger,
            request_id,
            owner=owner,
            fence=fence,
        ):
            result = func(*exec_args, **exec_kwargs)
    except (AuthorityExpiredError, UseTimeCurrencyError) as blocked:
        try:
            _record_failure(
                ledger, request_id, blocked, _expected_owner=owner, _expected_fence=fence
            )
            _emit_tool_receipt(
                audit_emitter,
                ledger,
                request_id,
                expected_owner=owner,
                expected_fence=fence,
            )
        except LedgerOutcomeAlreadySetError:
            pass
        except Exception:
            _logger.exception(
                "could not record use-boundary denial for %s",
                request_id,
            )
        raise
    except Exception as exc:
        from mycelium.secret_protection import (
            get_active_secret_policy,
        )
        from mycelium.secret_protection import (
            sanitize_exception as _sanitize_exc,
        )

        policy = get_active_secret_policy()
        if policy is not None and policy.enabled:
            exc = _sanitize_exc(exc)
        # A storage failure while recording the failure must not mask the
        # original tool exception — log it, then re-raise the tool's own error.
        # An outcome-already-set error also does not mask — the transition was
        # resolved elsewhere after the tool started.
        try:
            _record_failure(ledger, request_id, exc, _expected_owner=owner, _expected_fence=fence)
            _emit_tool_receipt(
                audit_emitter,
                ledger,
                request_id,
                expected_owner=owner,
                expected_fence=fence,
            )
        except LedgerOutcomeAlreadySetError:
            _logger.warning(
                "outcome already set for %s while recording failure "
                "(transition resolved elsewhere after tool started) — "
                "re-raising original exception",
                request_id,
            )
        except Exception:
            _logger.exception(
                "could not record failure for %s (storage down?); original tool error follows",
                request_id,
            )
        try:
            ledger._emit_outcome(
                request_id=request_id,
                tool=tool_name,
                event="body_fail",
                side_effect_class=side_effect_class,
                authorized_reexec=authorized_reexec,
                owner=owner,
                error_class=type(exc).__name__,
                policy_version=(
                    transition_binding.policy_version if transition_binding is not None else None
                ),
            )
        except Exception:
            _logger.exception(
                "could not emit body_fail outcome for %s; original tool error follows",
                request_id,
            )
        raise exc
    finally:
        _active_transition_var.reset(token)

    try:
        ledger.complete(request_id, result, _expected_owner=owner, _expected_fence=fence)
        complete_ok = True
    except LedgerOutcomeAlreadySetError:
        _logger.warning(
            "outcome already set for %s while completing "
            "(transition resolved elsewhere after tool started) — "
            "tool result discarded",
            request_id,
        )
        complete_ok = False
    _emit_tool_receipt(
        audit_emitter,
        ledger,
        request_id,
        expected_owner=owner,
        expected_fence=fence,
    )
    ledger._emit_outcome(
        request_id=request_id,
        tool=tool_name,
        event="body_complete" if complete_ok else "body_fail",
        side_effect_class=side_effect_class,
        authorized_reexec=authorized_reexec,
        owner=owner,
        error_class=None if complete_ok else "LedgerOutcomeAlreadySetError",
        policy_version=(
            transition_binding.policy_version if transition_binding is not None else None
        ),
    )
    return result


async def _run_ledgered_async(
    func: Callable[P, Awaitable[R]],
    tool_name: str,
    ledger: ActionLedger,
    args: P.args,
    kwargs: P.kwargs,
    audit_emitter: AuditReceiptEmitter | None = None,
    transition_binding: ToolTransitionBinding | None = None,
) -> R:
    identity_kwargs = _identity_lookup_kwargs(func, args, kwargs)
    request_id = ledger.derive_request_id(
        tool_name,
        args,
        kwargs,
        transition_binding=transition_binding,
        identity_kwargs=identity_kwargs,
    )
    clean_kwargs = _drop_ledger_keys(kwargs)
    claim_kwargs = _claim_kwargs(kwargs, clean_kwargs)
    _outcome_reexec_authorized.set(False)
    try:
        existing = await _claim_for_transition_async(
            ledger,
            request_id,
            tool_name,
            args,
            claim_kwargs,
            transition_binding,
        )
    except LedgerHardBlockError:
        try:
            ledger._emit_outcome(
                request_id=request_id,
                tool=tool_name,
                event="resolution",
                gate="HARD_BLOCK",
                error_class="LedgerHardBlockError",
            )
        except Exception:
            _logger.exception(
                "could not emit HARD_BLOCK outcome for %s; original ledger error follows",
                request_id,
            )
        raise
    except LedgerSoftBlockError:
        try:
            ledger._emit_outcome(
                request_id=request_id,
                tool=tool_name,
                event="resolution",
                gate="SOFT_BLOCK",
                error_class="LedgerSoftBlockError",
            )
        except Exception:
            _logger.exception(
                "could not emit SOFT_BLOCK outcome for %s; original ledger error follows",
                request_id,
            )
        raise
    request_id = existing.request_id
    if existing.is_terminal_completed():
        ledger._emit_outcome(
            request_id=request_id,
            tool=tool_name,
            event="resolution",
            gate="RETURN",
            terminal_outcome=TerminalOutcome.COMPLETED,
        )
        return existing.result

    owner = _ledger_owner()
    fence = existing.fence
    authorized_reexec = _outcome_reexec_authorized.get()
    side_effect_class = (
        transition_binding.side_effect_class if transition_binding is not None else None
    )
    ledger._emit_outcome(
        request_id=request_id,
        tool=tool_name,
        event="resolution",
        gate="ALLOW",
        terminal_outcome=TerminalOutcome.IN_FLIGHT,
        side_effect_class=side_effect_class,
        authorized_reexec=authorized_reexec,
        owner=owner,
    )

    call_mapping = _use_boundary_call_mapping(func, args, clean_kwargs, kwargs)
    token = _active_transition_var.set(
        _ActiveTransition(
            ledger,
            request_id,
            transition_binding,
            call_mapping,
            owner,
            fence,
        )
    )
    try:
        from mycelium.authority_window import (
            AuthorityExpiredError,
            get_authority_decisions,
        )
        from mycelium.decision import finalize_policy_facts_at_boundary
        from mycelium.use_time_currency import (
            UseTimeCurrencyError,
            enforce_use_boundary_async,
            get_use_time_decisions,
        )

        finalize_policy_facts_at_boundary()
        blocked: AuthorityExpiredError | UseTimeCurrencyError | None = None
        authority_offset = len(get_authority_decisions())
        currency_offset = len(get_use_time_decisions())
        try:
            auth_decision, currency_decision = await enforce_use_boundary_async(kwargs=call_mapping)
        except (AuthorityExpiredError, UseTimeCurrencyError) as exc:
            blocked = exc
            auth_decision, currency_decision = _boundary_denial_facts(
                blocked,
                authority_offset=authority_offset,
                currency_offset=currency_offset,
            )
            event = (
                "use_time_currency"
                if isinstance(blocked, UseTimeCurrencyError)
                else "authority_window"
            )
            try:
                ledger._emit_outcome(
                    request_id=request_id,
                    tool=tool_name,
                    event=event,
                    gate="DENY",
                    terminal_outcome=TerminalOutcome.FAILED_BEFORE_EFFECT,
                    side_effect_class=side_effect_class,
                    tool_body_executed=False,
                    authorized_reexec=authorized_reexec,
                    owner=owner,
                    error_class=type(blocked).__name__,
                    policy_version=(
                        transition_binding.policy_version
                        if transition_binding is not None
                        else None
                    ),
                )
            except Exception:
                _logger.exception(
                    "could not emit %s denial for %s",
                    event,
                    request_id,
                )

        if getattr(auth_decision, "decision", "skipped") == "allowed":
            try:
                ledger._emit_outcome(
                    request_id=request_id,
                    tool=tool_name,
                    event="authority_window",
                    gate="ALLOW",
                    terminal_outcome=TerminalOutcome.IN_FLIGHT,
                    side_effect_class=side_effect_class,
                    tool_body_executed=False,
                    authorized_reexec=authorized_reexec,
                    owner=owner,
                    policy_version=getattr(auth_decision, "policy_version", None)
                    or (
                        transition_binding.policy_version
                        if transition_binding is not None
                        else None
                    ),
                )
            except Exception:
                _logger.exception(
                    "could not emit authority_window allow for %s",
                    request_id,
                )

        if getattr(currency_decision, "decision", "skipped") == "allowed":
            try:
                ledger._emit_outcome(
                    request_id=request_id,
                    tool=tool_name,
                    event="use_time_currency",
                    gate="ALLOW",
                    terminal_outcome=TerminalOutcome.IN_FLIGHT,
                    side_effect_class=side_effect_class,
                    tool_body_executed=False,
                    authorized_reexec=authorized_reexec,
                    owner=owner,
                    policy_version=currency_decision.policy_version
                    or (
                        transition_binding.policy_version
                        if transition_binding is not None
                        else None
                    ),
                )
            except Exception:
                _logger.exception(
                    "could not emit use_time_currency allow for %s",
                    request_id,
                )

        decision = _record_boundary_decision(
            ledger,
            request_id,
            tool=tool_name,
            args=args,
            kwargs=call_mapping,
            transition_key=(
                derive_transition_key_for_call(tool_name, args, dict(kwargs), transition_binding)
                if transition_binding is not None
                else None
            ),
            auth_decision=auth_decision,
            currency_decision=currency_decision,
            owner=owner,
            fence=fence,
        )
        if blocked is not None:
            raise blocked
        from mycelium.decision import get_policy_blocked_error

        policy_blocked = get_policy_blocked_error()
        if policy_blocked is not None:
            raise policy_blocked
        _raise_denied_decision(request_id, decision)
        existing = _ensure_provider_key_for_execution(
            ledger=ledger,
            request_id=request_id,
            transition_binding=transition_binding,
            claimed_entry=existing,
            clean_kwargs=clean_kwargs,
            call_mapping=call_mapping,
            owner=owner,
            fence=fence,
        )

        ledger._emit_outcome(
            request_id=request_id,
            tool=tool_name,
            event="body_start",
            terminal_outcome=TerminalOutcome.IN_FLIGHT,
            side_effect_class=side_effect_class,
            tool_body_executed=True,
            authorized_reexec=authorized_reexec,
            owner=owner,
        )

        from mycelium.secret_protection import (
            get_active_secret_policy,
            resolve_declared_secret_fields,
        )

        policy = get_active_secret_policy()
        extra = policy.secret_fields if policy is not None else frozenset()
        exec_args, exec_kwargs = resolve_declared_secret_fields(
            func, args, clean_kwargs, extra_fields=extra
        )
        with _lease_auto_renew(
            ledger,
            request_id,
            owner=owner,
            fence=fence,
        ):
            result = await func(*exec_args, **exec_kwargs)
    except (AuthorityExpiredError, UseTimeCurrencyError) as blocked:
        try:
            _record_failure(
                ledger, request_id, blocked, _expected_owner=owner, _expected_fence=fence
            )
            _emit_tool_receipt(
                audit_emitter,
                ledger,
                request_id,
                expected_owner=owner,
                expected_fence=fence,
            )
        except LedgerOutcomeAlreadySetError:
            pass
        except Exception:
            _logger.exception(
                "could not record use-boundary denial for %s",
                request_id,
            )
        raise
    except Exception as exc:
        from mycelium.secret_protection import (
            get_active_secret_policy,
        )
        from mycelium.secret_protection import (
            sanitize_exception as _sanitize_exc,
        )

        policy = get_active_secret_policy()
        if policy is not None and policy.enabled:
            exc = _sanitize_exc(exc)
        # A storage failure while recording the failure must not mask the
        # original tool exception — log it, then re-raise the tool's own error.
        # An outcome-already-set error also does not mask — the transition was
        # resolved elsewhere after the tool started.
        try:
            _record_failure(ledger, request_id, exc, _expected_owner=owner, _expected_fence=fence)
            _emit_tool_receipt(
                audit_emitter,
                ledger,
                request_id,
                expected_owner=owner,
                expected_fence=fence,
            )
        except LedgerOutcomeAlreadySetError:
            _logger.warning(
                "outcome already set for %s while recording failure "
                "(transition resolved elsewhere after tool started) — "
                "re-raising original exception",
                request_id,
            )
        except Exception:
            _logger.exception(
                "could not record failure for %s (storage down?); original tool error follows",
                request_id,
            )
        try:
            ledger._emit_outcome(
                request_id=request_id,
                tool=tool_name,
                event="body_fail",
                side_effect_class=side_effect_class,
                authorized_reexec=authorized_reexec,
                owner=owner,
                error_class=type(exc).__name__,
                policy_version=(
                    transition_binding.policy_version if transition_binding is not None else None
                ),
            )
        except Exception:
            _logger.exception(
                "could not emit body_fail outcome for %s; original tool error follows",
                request_id,
            )
        raise exc
    finally:
        _active_transition_var.reset(token)

    try:
        ledger.complete(request_id, result, _expected_owner=owner, _expected_fence=fence)
        complete_ok = True
    except LedgerOutcomeAlreadySetError:
        _logger.warning(
            "outcome already set for %s while completing "
            "(transition resolved elsewhere after tool started) — "
            "tool result discarded",
            request_id,
        )
        complete_ok = False
    _emit_tool_receipt(
        audit_emitter,
        ledger,
        request_id,
        expected_owner=owner,
        expected_fence=fence,
    )
    ledger._emit_outcome(
        request_id=request_id,
        tool=tool_name,
        event="body_complete" if complete_ok else "body_fail",
        side_effect_class=side_effect_class,
        authorized_reexec=authorized_reexec,
        owner=owner,
        error_class=None if complete_ok else "LedgerOutcomeAlreadySetError",
        policy_version=(
            transition_binding.policy_version if transition_binding is not None else None
        ),
    )
    return result


def _mark_ledgered(wrapper: Callable[..., Any], ledger: ActionLedger) -> None:
    wrapper._mycelium_ledger = True  # type: ignore[attr-defined]
    wrapper._mycelium_ledger_instance = ledger  # type: ignore[attr-defined]


def ledger(
    storage: LedgerStorage | None = None,
    audit_emitter: AuditReceiptEmitter | None = None,
    transition_binding: ToolTransitionBinding | None = None,
    *,
    outcome_emitter: OutcomeEmitter | None = None,
    operator_authorizer: OperatorAuthorizer | None = None,
    lease_ttl: float | None = None,
    lease_renew_interval: float | None = None,
    poll_interval: float | None = None,
    poll_timeout: float | None = None,
    reconciler: Reconciler | None = None,
    defer_read_only_unknown: bool = False,
    unclassified_policy: str = UNCLASSIFIED_POLICY_WARN,
    on_args_drift: str = ARGS_DRIFT_SOFT,
    reclaim_requires_death_signal: bool = False,
    presumed_dead_after: float | None = None,
    request_identity_policy: str = REQUEST_IDENTITY_POLICY_DERIVED,
) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]:
    """Decorator that records async tool invocations in an ActionLedger.

    While the tool body runs, Mycelium auto-extends the execution lease
    (default every ``lease_ttl / 3``). Pass ``lease_renew_interval=0`` to
    disable; use :func:`renew_lease` for an extra manual bump.
    """

    ledger_kwargs: dict[str, float | bool | None | str] = {}
    if lease_ttl is not None:
        ledger_kwargs["lease_ttl"] = lease_ttl
    if lease_renew_interval is not None:
        ledger_kwargs["lease_renew_interval"] = lease_renew_interval
    if poll_interval is not None:
        ledger_kwargs["poll_interval"] = poll_interval
    if poll_timeout is not None:
        ledger_kwargs["poll_timeout"] = poll_timeout
    if reclaim_requires_death_signal:
        ledger_kwargs["reclaim_requires_death_signal"] = True
    if presumed_dead_after is not None:
        ledger_kwargs["presumed_dead_after"] = presumed_dead_after
    action_ledger = ActionLedger(
        storage=storage,
        reconciler=reconciler,
        defer_read_only_unknown=defer_read_only_unknown,
        audit_emitter=audit_emitter,
        outcome_emitter=outcome_emitter,
        operator_authorizer=operator_authorizer,
        unclassified_policy=unclassified_policy,
        on_args_drift=on_args_drift,
        request_identity_policy=request_identity_policy,
        **ledger_kwargs,
    )

    def decorator(func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        tool_name = func.__name__

        @functools.wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            return await _run_ledgered_async(
                func,
                tool_name,
                action_ledger,
                args,
                kwargs,
                audit_emitter,
                transition_binding,
            )

        _mark_ledgered(wrapper, action_ledger)
        return wrapper

    return decorator


def ledger_sync(
    storage: LedgerStorage | None = None,
    audit_emitter: AuditReceiptEmitter | None = None,
    transition_binding: ToolTransitionBinding | None = None,
    *,
    outcome_emitter: OutcomeEmitter | None = None,
    operator_authorizer: OperatorAuthorizer | None = None,
    lease_ttl: float | None = None,
    lease_renew_interval: float | None = None,
    poll_interval: float | None = None,
    poll_timeout: float | None = None,
    reconciler: Reconciler | None = None,
    defer_read_only_unknown: bool = False,
    unclassified_policy: str = UNCLASSIFIED_POLICY_WARN,
    on_args_drift: str = ARGS_DRIFT_SOFT,
    reclaim_requires_death_signal: bool = False,
    presumed_dead_after: float | None = None,
    request_identity_policy: str = REQUEST_IDENTITY_POLICY_DERIVED,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Decorator that records sync tool invocations in an ActionLedger.

    While the tool body runs, Mycelium auto-extends the execution lease
    (default every ``lease_ttl / 3``). Pass ``lease_renew_interval=0`` to
    disable; use :func:`renew_lease` for an extra manual bump.
    """

    ledger_kwargs: dict[str, float | bool | None | str] = {}
    if lease_ttl is not None:
        ledger_kwargs["lease_ttl"] = lease_ttl
    if lease_renew_interval is not None:
        ledger_kwargs["lease_renew_interval"] = lease_renew_interval
    if poll_interval is not None:
        ledger_kwargs["poll_interval"] = poll_interval
    if poll_timeout is not None:
        ledger_kwargs["poll_timeout"] = poll_timeout
    if reclaim_requires_death_signal:
        ledger_kwargs["reclaim_requires_death_signal"] = True
    if presumed_dead_after is not None:
        ledger_kwargs["presumed_dead_after"] = presumed_dead_after
    action_ledger = ActionLedger(
        storage=storage,
        reconciler=reconciler,
        defer_read_only_unknown=defer_read_only_unknown,
        audit_emitter=audit_emitter,
        outcome_emitter=outcome_emitter,
        operator_authorizer=operator_authorizer,
        unclassified_policy=unclassified_policy,
        on_args_drift=on_args_drift,
        request_identity_policy=request_identity_policy,
        **ledger_kwargs,
    )

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        tool_name = func.__name__

        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            return _run_ledgered(
                func,
                tool_name,
                action_ledger,
                args,
                kwargs,
                audit_emitter,
                transition_binding,
            )

        _mark_ledgered(wrapper, action_ledger)
        return wrapper

    return decorator


def get_ledger(func: Callable[..., Any]) -> ActionLedger | None:
    """Return the ActionLedger attached to a wrapped function, if any."""
    return getattr(func, "_mycelium_ledger_instance", None)


__all__ = [
    "ActionLedger",
    "DEFAULT_LEASE_RENEW_RATIO",
    "DEFAULT_LEASE_TTL",
    "DEFAULT_POLL_INTERVAL",
    "DEFAULT_POLL_TIMEOUT",
    "DEFAULT_PRESUMED_DEAD_AFTER_RATIO",
    "OPERATOR_RESOLUTION_COMPLETED",
    "OPERATOR_RESOLUTION_NOT_EXECUTED",
    "ARGS_DRIFT_OFF",
    "ARGS_DRIFT_SOFT",
    "ARGS_DRIFT_HARD",
    "ARGS_DRIFT_POLICIES",
    "UNCLASSIFIED_POLICY_WARN",
    "UNCLASSIFIED_POLICY_STRICT",
    "REQUEST_IDENTITY_POLICY_DERIVED",
    "REQUEST_IDENTITY_POLICY_REQUIRE_EXPLICIT",
    "REQUEST_IDENTITY_POLICIES",
    "MissingRequestIdentityError",
    "FileLedgerStorage",
    "InMemoryLedgerStorage",
    "LedgerAlreadyResolvedError",
    "LedgerEntry",
    "LedgerError",
    "LedgerHardBlockError",
    "LedgerPendingError",
    "LedgerPollTimeoutError",
    "LedgerReleaseRefusedError",
    "LedgerStorage",
    "LedgerStorageUnavailableError",
    "LedgerWorkerAliveError",
    "MIN_LEASE_RENEW_INTERVAL",
    "TerminalOutcome",
    "get_ledger",
    "ledger",
    "ledger_sync",
    "mark_crossed",
    "mark_maybe_crossed",
    "mark_maybe_crossed_async",
    "record_external_operation",
    "renew_lease",
    "side_effect",
    "side_effect_async",
]
