"""Transition envelope: rich idempotency keys for side-effecting tools."""

from __future__ import annotations

import hashlib
import json
import time
from contextlib import AbstractContextManager
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from typing import Any

from mycelium._compat import StrEnum

TRANSITION_SCHEMA = "mycelium.transition/v2"
EFFECT_SCHEMA = TRANSITION_SCHEMA

SCOPE_FIELDS = ("thread_id", "run_id", "node")

REQUEST_IDENTITY_POLICY_DERIVED = "derived"
REQUEST_IDENTITY_POLICY_REQUIRE_EXPLICIT = "require_explicit"
REQUEST_IDENTITY_POLICIES = frozenset(
    {
        REQUEST_IDENTITY_POLICY_DERIVED,
        REQUEST_IDENTITY_POLICY_REQUIRE_EXPLICIT,
    }
)

LEDGER_KWARG_KEYS = frozenset(
    {
        "request_id",
        "tool_call_id",
        "thread_id",
        "run_id",
        "node",
        # State-authority / decision metadata — not part of tool-arg identity.
        "state_ref",
        "decision_id",
        # Handoff / causation audit — not part of tool-arg identity.
        "parent_request_id",
        "handoff_id",
    }
)


class MissingRequestIdentityError(Exception):
    """Raised when a consequential tool has no host-owned business identity.

    ``request_id`` must come from a server-owned business record. It is not
    ``tool_call_id``, ``run_id``, ``thread_id``, or a random fallback.
    """

    def __init__(
        self,
        *,
        tool: str,
        field: str | None = None,
        detail: str | None = None,
    ) -> None:
        self.tool = tool
        self.field = field
        if detail:
            message = detail
        elif field:
            message = (
                f"Tool {tool!r} request_id_from={field!r} is missing or empty. "
                "The configured field must be a non-empty value from a "
                "server-owned business record, not model-generated data. "
                "The tool was not claimed or executed."
            )
        else:
            message = (
                f"Tool {tool!r} requires a stable host-owned request_id "
                f"(request_identity_policy={REQUEST_IDENTITY_POLICY_REQUIRE_EXPLICIT!r}). "
                f"Pass request_id derived from a server-owned business record "
                f'(for example request_id=f"{tool}:{{order_id}}") or set '
                f"tools.{tool}.request_id_from. Do not use tool_call_id, "
                "run_id, thread_id, or a random id. The tool was not claimed "
                "or executed."
            )
        super().__init__(message)


class SideEffectClass(StrEnum):
    """Per-tool side-effect classification for retry/redispatch policy.

    Classes describe *effect semantics*, not business domains:

    - ``read`` — no external mutation
    - ``idempotent_mutate`` — mutation; retry-safe as-is
    - ``keyed_mutate`` — safe only with the same provider idempotency key
    - ``non_idempotent_mutate`` — second call = second effect
    - ``irreversible`` — no compensation; ambiguity requires human reconcile
    """

    READ = "read"
    IDEMPOTENT_MUTATE = "idempotent_mutate"
    KEYED_MUTATE = "keyed_mutate"
    NON_IDEMPOTENT_MUTATE = "non_idempotent_mutate"
    IRREVERSIBLE = "irreversible"


CONSEQUENTIAL_SIDE_EFFECT_CLASSES = frozenset(
    {
        SideEffectClass.IDEMPOTENT_MUTATE,
        SideEffectClass.KEYED_MUTATE,
        SideEffectClass.NON_IDEMPOTENT_MUTATE,
        SideEffectClass.IRREVERSIBLE,
    }
)


# Legacy YAML / API names accepted by :func:`parse_side_effect_class`.
SIDE_EFFECT_CLASS_ALIASES: dict[str, SideEffectClass] = {
    "read_only": SideEffectClass.READ,
    "idempotent_write": SideEffectClass.IDEMPOTENT_MUTATE,
    "external_api_mutation": SideEffectClass.KEYED_MUTATE,
    "non_idempotent_write": SideEffectClass.NON_IDEMPOTENT_MUTATE,
    "payment": SideEffectClass.NON_IDEMPOTENT_MUTATE,
    "email": SideEffectClass.NON_IDEMPOTENT_MUTATE,
    "subagent": SideEffectClass.NON_IDEMPOTENT_MUTATE,
    "onchain_action": SideEffectClass.IRREVERSIBLE,
}


class ToolCapability(StrEnum):
    """Per-tool *probeability* axis — orthogonal to :class:`SideEffectClass`.

    Where ``SideEffectClass`` describes *idempotency* (is a second call safe?),
    capability describes *probeability* (can an in-flight effect's outcome be
    looked up afterward?). Retry/redispatch policy needs both: an irreversible
    onchain call with no way to ask "did it happen?" must never be
    auto-redispatched even though its idempotency class already forbids retry.

    - ``idempotent`` — accepts your ``effect_id``; retries are safe (covers
      ``read`` + ``idempotent_mutate``).
    - ``queryable`` — the outcome can be probed by ``effect_id`` (a registered
      :class:`~mycelium.reconcile.Reconciler` or a provider idempotency key);
      recovery resolves ``ATTEMPTING → COMMITTED/ABORTED`` by probing.
    - ``blind`` — neither; ``UNKNOWN`` parks for reconciliation. A blind tool is
      **never** auto-retried.
    """

    IDEMPOTENT = "idempotent"
    QUERYABLE = "queryable"
    BLIND = "blind"


class ToolCapabilityDeclarationError(Exception):
    """Raised when an explicit capability loosens beyond what the tool supports.

    Declaration honesty: an explicit capability may always *tighten* (to
    ``BLIND``), but may only *loosen* (to ``QUERYABLE`` / ``IDEMPOTENT``) when
    the side-effect class plus available mechanism (provider idempotency key or
    reconciler) actually supports it. Otherwise a tool could silently opt into
    duplicate effects.
    """


class TerminalOutcome(StrEnum):
    """Terminal or in-progress state of a side-effect transition."""

    IN_FLIGHT = "IN_FLIGHT"
    COMPLETED = "COMPLETED"
    FAILED_BEFORE_EFFECT = "FAILED_BEFORE_EFFECT"
    FAILED_AFTER_EFFECT = "FAILED_AFTER_EFFECT"
    EXPIRED = "EXPIRED"
    BLOCKED = "BLOCKED"
    UNKNOWN = "UNKNOWN"


class EffectState(StrEnum):
    """Unified durable WAL intent for a side-effecting transition.

    The single authoritative state machine for a ledger row's *intent*,
    replacing the historical split across ``effect_phase`` (INTENDED /
    ATTEMPTING / COMMITTED / ABORTED) and ``terminal_outcome`` (which also
    carries ``UNKNOWN``, ``BLOCKED``, and the ``FAILED_*`` split). A
    third-party reimplementer needs to track only this one enum plus the
    fenced CAS that guards every transition between its members::

        INTENDED -> ATTEMPTING -> COMMITTED | ABORTED | UNKNOWN

    - ``INTENDED``: durable row exists; no provider side effect yet; safe to
      retry.
    - ``ATTEMPTING``: decision allowed + recorded; the provider boundary may
      (or may not yet) be crossed.
    - ``COMMITTED``: exactly-once effect recorded (maps to legacy
      ``TerminalOutcome.COMPLETED``).
    - ``ABORTED``: decision denied, or failed before any effect; nothing
      happened; safe to retry.
    - ``UNKNOWN``: terminal-until-resolved; fail-closed for redispatch,
      especially for ``BLIND`` tools — no task redispatch while any attached
      effect remains ``UNKNOWN``.

    ``LedgerEntry`` keeps the historical ``effect_phase`` string field as the
    storage field for on-disk/serialization compatibility (old rows load
    unchanged, and every value it holds today is a member of this enum). New
    protocol-gating code must branch on :func:`resolve_effect_state` (or
    ``LedgerEntry.resolved_effect_state()``) rather than on the raw
    ``effect_phase`` / ``terminal_outcome`` fields — only the resolver
    correctly folds in ``UNKNOWN`` and legacy rows.
    """

    INTENDED = "INTENDED"
    ATTEMPTING = "ATTEMPTING"
    COMMITTED = "COMMITTED"
    ABORTED = "ABORTED"
    UNKNOWN = "UNKNOWN"


class LeaseValidity(StrEnum):
    """Whether an in-flight execution lease is still held.

    Lease is resolution metadata, not part of ``transition_key``. Gates check
    validity before reclaim/retry: ``HELD`` → poll; ``EXPIRED`` → reclaim or
    hard-block by class; ``UNBOUNDED`` → no TTL (never auto-expires).
    """

    HELD = "HELD"
    EXPIRED = "EXPIRED"
    UNBOUNDED = "UNBOUNDED"


class ProviderKeyValidity(StrEnum):
    """Whether a provider idempotency key is still within its validity window.

    When a tool declares ``provider_idempotency_key_ttl``, the first attempt's
    timestamp is recorded on the ledger entry.  On a subsequent same-key retry
    the gate checks whether the window has expired — if so the provider may
    have purged the key, making the retry unsafe.  For ``UNKNOWN``, declaring
    both the key param and this TTL is the opt-in that allows same-key retry
    while the window is still ``VALID`` (Reconciler remains preferred).

    ``UNTRACKED`` means the tool has no TTL configured (existing behaviour
    unchanged — ``UNKNOWN`` stays hard-blocked).
    """

    VALID = "VALID"
    EXPIRED = "EXPIRED"
    UNTRACKED = "UNTRACKED"


def provider_key_validity(
    entry: Any,
    binding: ToolTransitionBinding,
    *,
    now: float | None = None,
) -> ProviderKeyValidity:
    """Classify whether a provider idempotency key's validity window has elapsed.

    Returns ``UNTRACKED`` when the binding has no ``provider_idempotency_key_ttl``
    (the existing idempotency-key retry behaviour is unchanged; ``UNKNOWN``
    same-key retry stays off).  ``VALID`` when the elapsed time since the first
    attempt is still within the declared window.  ``EXPIRED`` when the window
    has passed — the provider may have purged its deduplication state, so
    same-key ``FAILED_BEFORE_EFFECT`` / ``UNKNOWN`` retries harden to
    ``HARD_BLOCK``.
    """
    ttl = binding.provider_idempotency_key_ttl
    if ttl is None:
        return ProviderKeyValidity.UNTRACKED
    first = getattr(entry, "provider_key_first_attempt_at", None)
    if first is None:
        first = getattr(entry, "started_at", None)
    if first is None:
        return ProviderKeyValidity.UNTRACKED
    now = now if now is not None else time.time()
    if (now - first) >= ttl:
        return ProviderKeyValidity.EXPIRED
    return ProviderKeyValidity.VALID


def resolve_lease_validity(
    lease_until: float | None,
    *,
    now: float | None = None,
) -> LeaseValidity:
    """Classify the execution lease window for resolution.

    Call this *before* deciding whether a duplicate dispatch may reclaim or
    must poll. ``lease_until`` is not hashed into the transition key — it is
    mutable (renewable) while the same transition stays in flight.
    """
    if lease_until is None:
        return LeaseValidity.UNBOUNDED
    now = now if now is not None else time.time()
    if now >= lease_until:
        return LeaseValidity.EXPIRED
    return LeaseValidity.HELD


def has_worker_death_evidence(
    entry: Any,
    *,
    now: float | None = None,
    presumed_dead_after: float,
) -> bool:
    """Classify whether there is affirmative evidence a worker is gone.

    Returns ``True`` when any of:

    * ``worker_dead_asserted_at`` is set (explicit death signal from an
      orchestrator or human), **or**
    * the last heartbeat (``last_heartbeat_at``, falling back to ``started_at``
      when absent) is older than ``presumed_dead_after`` seconds ago.

    Call this *before* deciding whether an EXPIRED entry may be reclaimed or
    released.  It is a pure function so gates and tests share the same logic.
    """
    now = now if now is not None else time.time()

    # Explicit death assertion always counts.
    if getattr(entry, "worker_dead_asserted_at", None) is not None:
        return True

    # Heartbeat-based: fall back to started_at when no heartbeat recorded.
    reference = getattr(entry, "last_heartbeat_at", None)
    if reference is None:
        reference = getattr(entry, "started_at", None)
    if reference is None:
        # No timestamp at all — treat as evidence-less.
        return False

    return (now - reference) >= presumed_dead_after


class SideEffectBoundary(StrEnum):
    """Whether an external side-effect boundary was crossed."""

    NOT_CROSSED = "not_crossed"
    MAYBE_CROSSED = "maybe_crossed"
    CROSSED = "crossed"


class RetryPermission(StrEnum):
    """Whether an automatic retry/redispatch is permitted."""

    SAFE_RETRY = "safe_retry"
    RETRY_ONLY_WITH_SAME_PROVIDER_IDEMPOTENCY_KEY = (
        "retry_only_with_same_provider_idempotency_key"
    )
    MANUAL_RECONCILIATION_REQUIRED = "manual_reconciliation_required"
    NEVER_RETRY_AUTOMATICALLY = "never_retry_automatically"


class Spendability(StrEnum):
    """Whether an intent may produce an external effect more than once.

    Orthogonal to :class:`SideEffectClass`: class describes *what kind* of
    effect; spendability describes *how many times* the same intent may spend.

    - ``multi_use`` — intent may produce effects again (reads, idempotent upserts)
    - ``single_use`` — one effect; after COMPLETED return stored result; ambiguity
      hard-blocks
    - ``non_replayable`` — under any ambiguity, hard-block / reconcile (never
      auto-retry a fuzzy second spend)
    """

    MULTI_USE = "multi_use"
    SINGLE_USE = "single_use"
    NON_REPLAYABLE = "non_replayable"


DEFAULT_RETRY_PERMISSION: dict[SideEffectClass, RetryPermission] = {
    SideEffectClass.READ: RetryPermission.SAFE_RETRY,
    SideEffectClass.IDEMPOTENT_MUTATE: RetryPermission.SAFE_RETRY,
    SideEffectClass.KEYED_MUTATE: (
        RetryPermission.RETRY_ONLY_WITH_SAME_PROVIDER_IDEMPOTENCY_KEY
    ),
    SideEffectClass.NON_IDEMPOTENT_MUTATE: (
        RetryPermission.MANUAL_RECONCILIATION_REQUIRED
    ),
    SideEffectClass.IRREVERSIBLE: RetryPermission.NEVER_RETRY_AUTOMATICALLY,
}


DEFAULT_SPENDABILITY: dict[SideEffectClass, Spendability] = {
    SideEffectClass.READ: Spendability.MULTI_USE,
    SideEffectClass.IDEMPOTENT_MUTATE: Spendability.MULTI_USE,
    SideEffectClass.KEYED_MUTATE: Spendability.SINGLE_USE,
    SideEffectClass.NON_IDEMPOTENT_MUTATE: Spendability.SINGLE_USE,
    SideEffectClass.IRREVERSIBLE: Spendability.NON_REPLAYABLE,
}


STRICT_SIDE_EFFECT_CLASSES = frozenset(
    {
        SideEffectClass.NON_IDEMPOTENT_MUTATE,
        SideEffectClass.IRREVERSIBLE,
    }
)


def is_strict_side_effect(side_effect_class: SideEffectClass) -> bool:
    """Return whether a class requires strict hard-block-on-ambiguity resolution."""
    return side_effect_class in STRICT_SIDE_EFFECT_CLASSES


def blocks_on_ambiguous_replay(spendability: Spendability) -> bool:
    """Whether ambiguous terminal states must hard-block rather than reclaim/retry."""
    return spendability in (
        Spendability.SINGLE_USE,
        Spendability.NON_REPLAYABLE,
    )


def allows_failed_before_retry(side_effect_class: SideEffectClass) -> bool:
    """Whether ``FAILED_BEFORE_EFFECT`` may be automatically retried."""
    return resolve_retry_permission(side_effect_class, None) in (
        RetryPermission.SAFE_RETRY,
        RetryPermission.RETRY_ONLY_WITH_SAME_PROVIDER_IDEMPOTENCY_KEY,
    )


def parse_side_effect_boundary(value: Any) -> SideEffectBoundary:
    if not isinstance(value, str):
        raise ValueError("side_effect_boundary must be a string")
    try:
        return SideEffectBoundary(value)
    except ValueError as exc:
        allowed = ", ".join(member.value for member in SideEffectBoundary)
        raise ValueError(
            f"invalid side_effect_boundary {value!r}; expected one of: {allowed}"
        ) from exc


def parse_retry_permission(value: Any) -> RetryPermission:
    if not isinstance(value, str):
        raise ValueError("retry_permission must be a string")
    try:
        return RetryPermission(value)
    except ValueError as exc:
        allowed = ", ".join(member.value for member in RetryPermission)
        raise ValueError(
            f"invalid retry_permission {value!r}; expected one of: {allowed}"
        ) from exc


def parse_spendability(value: Any) -> Spendability:
    """Parse and validate a spendability value from YAML."""
    if not isinstance(value, str):
        raise ValueError("spendability must be a string")
    try:
        return Spendability(value)
    except ValueError as exc:
        allowed = ", ".join(member.value for member in Spendability)
        raise ValueError(
            f"invalid spendability {value!r}; expected one of: {allowed}"
        ) from exc


def resolve_retry_permission(
    side_effect_class: SideEffectClass,
    explicit: RetryPermission | None,
) -> RetryPermission:
    if explicit is not None:
        return explicit
    return DEFAULT_RETRY_PERMISSION[side_effect_class]


def resolve_spendability(
    side_effect_class: SideEffectClass,
    explicit: Spendability | None,
) -> Spendability:
    """Resolve spendability from an explicit override or class default."""
    if explicit is not None:
        return explicit
    return DEFAULT_SPENDABILITY[side_effect_class]


# Ordering from loosest (safest to retry) to tightest (never retry). A higher
# rank is a stricter promise; an explicit declaration may only move *up* this
# scale freely (tighten), never down (loosen) beyond what the class supports.
_CAPABILITY_RANK: dict[ToolCapability, int] = {
    ToolCapability.IDEMPOTENT: 0,
    ToolCapability.QUERYABLE: 1,
    ToolCapability.BLIND: 2,
}


def default_capability(
    side_effect_class: SideEffectClass,
    *,
    has_reconciler: bool = False,
    has_provider_key: bool = False,
) -> ToolCapability:
    """Derive the *conservative* capability for a class + available mechanism.

    - ``read`` / ``idempotent_mutate`` → ``IDEMPOTENT`` (retry-safe as-is).
    - ``keyed_mutate`` → ``QUERYABLE`` when a provider idempotency key or a
      reconciler exists, else ``BLIND``.
    - ``non_idempotent_mutate`` → ``QUERYABLE`` only when a reconciler exists,
      else ``BLIND``.
    - ``irreversible`` → ``BLIND`` (never loosened by derivation).
    """
    if side_effect_class in (
        SideEffectClass.READ,
        SideEffectClass.IDEMPOTENT_MUTATE,
    ):
        return ToolCapability.IDEMPOTENT
    if side_effect_class == SideEffectClass.KEYED_MUTATE:
        if has_provider_key or has_reconciler:
            return ToolCapability.QUERYABLE
        return ToolCapability.BLIND
    if side_effect_class == SideEffectClass.NON_IDEMPOTENT_MUTATE:
        if has_reconciler:
            return ToolCapability.QUERYABLE
        return ToolCapability.BLIND
    return ToolCapability.BLIND


def resolve_capability(
    side_effect_class: SideEffectClass,
    explicit: ToolCapability | None = None,
    *,
    has_reconciler: bool = False,
    has_provider_key: bool = False,
) -> ToolCapability:
    """Resolve a tool's capability from an explicit declaration or class default.

    Without ``explicit`` this returns :func:`default_capability`. An explicit
    declaration may always *tighten* to the conservative floor (e.g. down to
    ``BLIND``), but may only *loosen* to ``QUERYABLE`` / ``IDEMPOTENT`` when the
    class plus mechanism supports it. A dishonest loosening raises
    :class:`ToolCapabilityDeclarationError` — a tool may never silently opt into
    duplicate effects.
    """
    derived = default_capability(
        side_effect_class,
        has_reconciler=has_reconciler,
        has_provider_key=has_provider_key,
    )
    if explicit is None:
        return derived
    if _CAPABILITY_RANK[explicit] >= _CAPABILITY_RANK[derived]:
        return explicit
    raise ToolCapabilityDeclarationError(
        f"cannot declare capability={explicit.value!r} for "
        f"side_effect_class={side_effect_class.value!r} "
        f"(has_provider_key={has_provider_key}, has_reconciler={has_reconciler}); "
        f"the conservative floor is {derived.value!r}. A looser capability "
        "would let this tool silently opt into duplicate effects. Provide a "
        "reconciler / provider idempotency key, or declare a tighter capability."
    )


def parse_capability(value: Any) -> ToolCapability:
    """Parse and validate a ``capability`` value from YAML/API input."""
    if isinstance(value, ToolCapability):
        return value
    if not isinstance(value, str):
        raise ValueError("capability must be a string")
    try:
        return ToolCapability(value)
    except ValueError as exc:
        allowed = ", ".join(member.value for member in ToolCapability)
        raise ValueError(
            f"invalid capability {value!r}; expected one of: {allowed}"
        ) from exc


def resolve_side_effect_boundary_default(
    explicit: SideEffectBoundary | None,
) -> SideEffectBoundary:
    if explicit is not None:
        return explicit
    return SideEffectBoundary.NOT_CROSSED


def parse_terminal_outcome(value: Any) -> TerminalOutcome:
    if isinstance(value, TerminalOutcome):
        return value
    if not isinstance(value, str):
        raise ValueError("terminal_outcome must be a string")
    try:
        return TerminalOutcome(value)
    except ValueError as exc:
        allowed = ", ".join(member.value for member in TerminalOutcome)
        raise ValueError(
            f"invalid terminal_outcome {value!r}; expected one of: {allowed}"
        ) from exc


def terminal_from_legacy_status(
    status: str,
    *,
    lease_until: float | None = None,
    now: float | None = None,
) -> TerminalOutcome:
    """Infer ``terminal_outcome`` from legacy v1.2 ``status`` values."""
    if status == "completed":
        return TerminalOutcome.COMPLETED
    if status == "failed":
        return TerminalOutcome.FAILED_BEFORE_EFFECT
    if status == "in-flight":
        if resolve_lease_validity(lease_until, now=now) == LeaseValidity.EXPIRED:
            return TerminalOutcome.EXPIRED
        return TerminalOutcome.IN_FLIGHT
    return TerminalOutcome.UNKNOWN


def legacy_status_from_terminal(terminal_outcome: TerminalOutcome) -> str:
    """Map ``terminal_outcome`` to legacy ``status`` for backward compatibility."""
    if terminal_outcome == TerminalOutcome.COMPLETED:
        return "completed"
    if terminal_outcome in (
        TerminalOutcome.FAILED_BEFORE_EFFECT,
        TerminalOutcome.FAILED_AFTER_EFFECT,
        TerminalOutcome.BLOCKED,
        TerminalOutcome.UNKNOWN,
    ):
        return "failed"
    return "in-flight"


def resolve_terminal_outcome(
    terminal_outcome: TerminalOutcome | str,
    *,
    lease_until: float | None,
    now: float | None = None,
) -> TerminalOutcome:
    """Return the effective terminal outcome after lease-validity check.

    For ``IN_FLIGHT`` entries, lease validity is consulted first: an
    ``EXPIRED`` lease becomes ``TerminalOutcome.EXPIRED`` so resolution can
    reclaim or hard-block; a ``HELD`` / ``UNBOUNDED`` lease stays in-flight
    (poll).
    """
    outcome = (
        terminal_outcome
        if isinstance(terminal_outcome, TerminalOutcome)
        else parse_terminal_outcome(terminal_outcome)
    )
    if outcome == TerminalOutcome.IN_FLIGHT:
        if resolve_lease_validity(lease_until, now=now) == LeaseValidity.EXPIRED:
            return TerminalOutcome.EXPIRED
    return outcome


def resolve_effect_state(entry: Any) -> EffectState:
    """Derive the unified :class:`EffectState` for a ledger row.

    Pure and duck-typed over ``terminal_outcome`` / ``side_effect_boundary`` /
    ``effect_phase`` / ``decision`` attributes, so it works on ``LedgerEntry``
    *and* on any legacy row shape loaded through it — every existing row, old
    or new, maps onto the one durable WAL intent without a storage migration.
    Liveness (lease validity, worker death evidence) is a separate axis and is
    deliberately not consulted here: ``EffectState`` answers "where is this
    effect in the protocol", not "is the worker still alive".

    Precedence:

    1. ``terminal_outcome == COMPLETED`` -> ``COMMITTED``, unconditionally
       (even if a stale ``effect_phase`` says otherwise).
    2. Ambiguous rows — ``terminal_outcome`` is ``UNKNOWN`` /
       ``FAILED_AFTER_EFFECT`` (the outcome itself is unresolved, or the
       effect definitely fired), or *any* terminal whose
       ``side_effect_boundary`` is ``maybe_crossed`` / ``crossed`` (the
       provider call may have fired, e.g. a hard-blocked stale-lease
       ``BLOCKED`` row) -> ``UNKNOWN``. This deliberately widens the literal
       "``BLOCKED`` -> ``ABORTED``" legacy mapping: a ``BLOCKED`` row with a
       ``not_crossed`` boundary provably never reached the provider (safe,
       ``ABORTED``), but a ``BLOCKED`` row with ``maybe_crossed`` /
       ``crossed`` is exactly the "might have happened" case ``UNKNOWN``
       exists to express — collapsing it into ``ABORTED`` would silently
       relabel an ambiguous effect as safe-to-retry.
    3. Unambiguous ``FAILED_BEFORE_EFFECT`` / ``BLOCKED`` / legacy-stored
       ``EXPIRED`` (boundary ``not_crossed``) -> ``ABORTED``.
    4. Still ``IN_FLIGHT`` (active, or crashed before any terminal write
       landed): ``effect_phase == "ATTEMPTING"`` with a decision recorded ->
       ``ATTEMPTING`` (the boundary may already be ``maybe_crossed`` /
       ``crossed`` — that is within the definition of ``ATTEMPTING``, not a
       separate branch); ``effect_phase == "ABORTED"`` with a decision
       recorded (the brief window between a denying ``record_decision`` and
       the follow-up ``fail()``) -> ``ABORTED``; otherwise -> ``INTENDED``.
    """
    terminal_raw = getattr(entry, "terminal_outcome", None)
    try:
        terminal = (
            parse_terminal_outcome(terminal_raw) if terminal_raw else TerminalOutcome.IN_FLIGHT
        )
    except ValueError:
        terminal = TerminalOutcome.IN_FLIGHT

    if terminal == TerminalOutcome.COMPLETED:
        return EffectState.COMMITTED

    boundary_raw = getattr(entry, "side_effect_boundary", None)
    try:
        boundary = (
            parse_side_effect_boundary(boundary_raw)
            if boundary_raw
            else SideEffectBoundary.NOT_CROSSED
        )
    except ValueError:
        boundary = SideEffectBoundary.NOT_CROSSED

    ambiguous = terminal in (
        TerminalOutcome.UNKNOWN,
        TerminalOutcome.FAILED_AFTER_EFFECT,
    ) or boundary in (SideEffectBoundary.MAYBE_CROSSED, SideEffectBoundary.CROSSED)

    if terminal in (
        TerminalOutcome.FAILED_BEFORE_EFFECT,
        TerminalOutcome.BLOCKED,
        TerminalOutcome.EXPIRED,
        TerminalOutcome.UNKNOWN,
        TerminalOutcome.FAILED_AFTER_EFFECT,
    ):
        return EffectState.UNKNOWN if ambiguous else EffectState.ABORTED

    # terminal is IN_FLIGHT: either actively being worked, or crashed before
    # any terminal write landed. Resolve from effect_phase + decision.
    phase = str(getattr(entry, "effect_phase", "") or "")
    decision = getattr(entry, "decision", None)
    if decision is not None and phase == EffectState.ATTEMPTING.value:
        return EffectState.ATTEMPTING
    if decision is not None and phase == EffectState.ABORTED.value:
        return EffectState.ABORTED
    return EffectState.INTENDED


@dataclass(frozen=True)
class TransitionConfig:
    """Deployment-level transition settings from YAML ``transition:``."""

    agent_id: str
    policy_version: str
    scope_from: dict[str, str] = field(default_factory=dict)
    lease_ttl: float | None = None
    lease_renew_interval: float | None = None
    poll_interval: float | None = None
    poll_timeout: float | None = None
    reclaim_requires_death_signal: bool = True
    presumed_dead_after: float | None = None


@dataclass(frozen=True)
class ToolTransitionBinding:
    """Per-tool binding used when deriving a transition key at runtime."""

    agent_id: str
    policy_version: str
    side_effect_class: SideEffectClass
    scope_from: dict[str, str] = field(default_factory=dict)
    retry_permission: RetryPermission = RetryPermission.MANUAL_RECONCILIATION_REQUIRED
    side_effect_boundary_default: SideEffectBoundary = SideEffectBoundary.NOT_CROSSED
    spendability: Spendability = Spendability.SINGLE_USE
    capability: ToolCapability = ToolCapability.BLIND
    explicit_capability: ToolCapability | None = None
    provider_idempotency_key_param: str | None = None
    propagate_effect_id_as_provider_key: bool = False
    provider_idempotency_key_ttl: float | None = None
    request_id_from: str | None = None

    @classmethod
    def for_tool(
        cls,
        *,
        agent_id: str,
        policy_version: str,
        side_effect_class: SideEffectClass,
        scope_from: dict[str, str] | None = None,
        retry_permission: RetryPermission | None = None,
        side_effect_boundary: SideEffectBoundary | None = None,
        spendability: Spendability | None = None,
        capability: ToolCapability | None = None,
        provider_idempotency_key_param: str | None = None,
        propagate_effect_id_as_provider_key: bool = False,
        provider_idempotency_key_ttl: float | None = None,
        request_id_from: str | None = None,
    ) -> ToolTransitionBinding:
        # Binding-level capability sees only the statically declared provider
        # idempotency key. Reconciler presence lives on the ledger and can only
        # *tighten to a promise* at recovery time via
        # :meth:`effective_capability` — never loosen past this floor.
        has_provider_key = provider_idempotency_key_param is not None
        if capability is None:
            resolved_capability = default_capability(
                side_effect_class,
                has_reconciler=False,
                has_provider_key=has_provider_key,
            )
        else:
            # A declaration that cannot be honored even with a reconciler (e.g.
            # QUERYABLE on IRREVERSIBLE) is refused now. A declaration that would
            # be honored only once a reconciler is bound is deferred: the floor
            # stays conservative here and the ledger re-resolves via
            # :meth:`effective_capability` once it knows a reconciler exists.
            resolve_capability(
                side_effect_class,
                capability,
                has_reconciler=True,
                has_provider_key=has_provider_key,
            )
            floor = default_capability(
                side_effect_class,
                has_reconciler=False,
                has_provider_key=has_provider_key,
            )
            resolved_capability = (
                capability
                if _CAPABILITY_RANK[capability] >= _CAPABILITY_RANK[floor]
                else floor
            )
        return cls(
            agent_id=agent_id,
            policy_version=policy_version,
            side_effect_class=side_effect_class,
            scope_from=dict(scope_from or {}),
            retry_permission=resolve_retry_permission(
                side_effect_class, retry_permission
            ),
            side_effect_boundary_default=resolve_side_effect_boundary_default(
                side_effect_boundary
            ),
            spendability=resolve_spendability(side_effect_class, spendability),
            capability=resolved_capability,
            explicit_capability=capability,
            provider_idempotency_key_param=provider_idempotency_key_param,
            propagate_effect_id_as_provider_key=propagate_effect_id_as_provider_key,
            provider_idempotency_key_ttl=provider_idempotency_key_ttl,
            request_id_from=request_id_from,
        )

    def effective_capability(self, *, has_reconciler: bool) -> ToolCapability:
        """Resolve the capability given whether a reconciler is bound.

        This is where reconciler presence drives ``QUERYABLE`` for recovery. The
        binding stores a conservative floor (no reconciler assumed); a ledger
        that holds a :class:`~mycelium.reconcile.Reconciler` re-resolves here.
        The result may only loosen from the floor when the class + mechanism
        supports it — an explicit ``BLIND`` always wins. When a declaration's
        loosening is not (yet) justified — e.g. explicit ``QUERYABLE`` on a plain
        ``non_idempotent_mutate`` with no reconciler bound — this fails closed to
        the conservative floor instead of raising (honesty already validated at
        construction time).
        """
        has_provider_key = self.provider_idempotency_key_param is not None
        floor = default_capability(
            self.side_effect_class,
            has_reconciler=has_reconciler,
            has_provider_key=has_provider_key,
        )
        explicit = self.explicit_capability
        if explicit is None:
            return floor
        if _CAPABILITY_RANK[explicit] >= _CAPABILITY_RANK[floor]:
            return explicit
        return floor


@dataclass(frozen=True)
class TransitionScope:
    """Execution scope for a single agent run / graph step."""

    thread_id: str = ""
    run_id: str = ""
    node: str = ""
    destructive_grants: tuple[object, ...] = ()


_execution_scope_var: ContextVar[TransitionScope | None] = ContextVar(
    "mycelium_execution_scope",
    default=None,
)

_dispatch_id_var: ContextVar[str | None] = ContextVar(
    "mycelium_dispatch_id",
    default=None,
)


@dataclass(frozen=True)
class HandoffLink:
    """Audit causation: child transitions were caused by this parent handoff.

    Thin handoff identity — does not grant capabilities or block freestyle
    calls. Enforcement of at-most-once remains per-tool claim keys.
    """

    parent_request_id: str
    handoff_id: str | None = None


_handoff_var: ContextVar[HandoffLink | None] = ContextVar(
    "mycelium_handoff",
    default=None,
)


def get_active_execution_scope() -> TransitionScope | None:
    """Return the active execution scope, if any."""
    return _execution_scope_var.get()


def get_active_dispatch_id() -> str | None:
    """Return the framework dispatch identity active for this call, if any."""
    return _dispatch_id_var.get()


def get_active_handoff() -> HandoffLink | None:
    """Return the active handoff causation link, if any."""
    return _handoff_var.get()


def execution_scope(scope: TransitionScope) -> AbstractContextManager[TransitionScope]:
    """Context manager that sets the active execution scope."""
    return _ExecutionScopeContext(scope)


def dispatch_scope(dispatch_id: str) -> AbstractContextManager[str]:
    """Set a framework-supplied dispatch identity for transition derivation."""
    return _DispatchScopeContext(dispatch_id)


def handoff_scope(
    parent_request_id: str,
    *,
    handoff_id: str | None = None,
) -> AbstractContextManager[HandoffLink]:
    """Mark subsequent ledger claims as caused by ``parent_request_id``.

    Use after a supervisor/spawn transition to glue subagent tool claims to
    that parent for audit. Nested scopes replace the active link; exit
    restores the previous one.
    """
    return _HandoffScopeContext(
        HandoffLink(
            parent_request_id=str(parent_request_id),
            handoff_id=str(handoff_id) if handoff_id is not None else None,
        )
    )


class _ExecutionScopeContext(AbstractContextManager[TransitionScope]):
    def __init__(self, scope: TransitionScope) -> None:
        self._scope = scope
        self._token: Token[TransitionScope | None] | None = None

    def __enter__(self) -> TransitionScope:
        self._token = _execution_scope_var.set(self._scope)
        return self._scope

    def __exit__(self, *_: Any) -> bool:
        if self._token is not None:
            _execution_scope_var.reset(self._token)
            self._token = None
        return False


class _DispatchScopeContext(AbstractContextManager[str]):
    def __init__(self, dispatch_id: str) -> None:
        self._dispatch_id = dispatch_id
        self._token: Token[str | None] | None = None

    def __enter__(self) -> str:
        self._token = _dispatch_id_var.set(self._dispatch_id)
        return self._dispatch_id

    def __exit__(self, *_: Any) -> bool:
        if self._token is not None:
            _dispatch_id_var.reset(self._token)
            self._token = None
        return False


class _HandoffScopeContext(AbstractContextManager[HandoffLink]):
    def __init__(self, link: HandoffLink) -> None:
        self._link = link
        self._token: Token[HandoffLink | None] | None = None

    def __enter__(self) -> HandoffLink:
        self._token = _handoff_var.set(self._link)
        return self._link

    def __exit__(self, *_: Any) -> bool:
        if self._token is not None:
            _handoff_var.reset(self._token)
            self._token = None
        return False


def parse_side_effect_class(value: Any) -> SideEffectClass:
    """Parse and validate a side_effect_class value from YAML.

    Accepts the five canonical classes and legacy aliases
    (``read_only``, ``payment``, ``subagent``, …).
    """
    if not isinstance(value, str):
        raise ValueError("side_effect_class must be a string")
    alias = SIDE_EFFECT_CLASS_ALIASES.get(value)
    if alias is not None:
        return alias
    try:
        return SideEffectClass(value)
    except ValueError as exc:
        allowed = ", ".join(member.value for member in SideEffectClass)
        raise ValueError(
            f"invalid side_effect_class {value!r}; expected one of: {allowed}"
        ) from exc


def canonical_json(payload: dict[str, Any]) -> str:
    """Serialize a mapping to deterministic JSON for hashing."""
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def _tool_kwargs(kwargs: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in kwargs.items() if key not in LEDGER_KWARG_KEYS}


def args_fingerprint(args: tuple[Any, ...], kwargs: dict[str, Any]) -> str:
    """Hash canonical tool arguments, excluding Mycelium bookkeeping keys."""
    payload = {"args": args, "kwargs": _tool_kwargs(kwargs)}
    return hashlib.sha256(canonical_json(payload).encode()).hexdigest()


def parse_explicit_request_id(kwargs: dict[str, Any]) -> str | None:
    """Return a host-supplied ``request_id`` after validating it.

    Absent ``request_id`` means “derive identity as before” (``tool_call_id``
    / transition key). When the key is present it must be a non-empty string
    — empty, whitespace-only, and non-string values are rejected so a caller
    cannot accidentally mint a blank identity.
    """
    if "request_id" not in kwargs:
        return None
    value = kwargs["request_id"]
    if not isinstance(value, str) or not value.strip():
        raise ValueError(
            f"request_id must be a non-empty string, got {value!r}"
        )
    return value


def request_id_from_argument(
    tool: str,
    field: str,
    kwargs: dict[str, Any],
) -> str:
    """Build ``{tool}:{field}:{value}`` from a trusted host argument."""
    if field not in kwargs:
        raise MissingRequestIdentityError(tool=tool, field=field)
    value = kwargs[field]
    if not isinstance(value, str) or not value.strip():
        raise MissingRequestIdentityError(tool=tool, field=field)
    return f"{tool}:{field}:{value.strip()}"


def derive_dispatch_id(kwargs: dict[str, Any]) -> str | None:
    """Return explicit dispatch identity, then any active framework identity."""
    if "tool_call_id" in kwargs:
        return str(kwargs["tool_call_id"])
    explicit = parse_explicit_request_id(kwargs)
    if explicit is not None:
        return explicit
    return get_active_dispatch_id()


def resolve_scope(
    *,
    scope_from: dict[str, str],
    kwargs: dict[str, Any],
) -> TransitionScope:
    """Merge active execution scope with kwargs and configured bindings."""
    base = get_active_execution_scope() or TransitionScope()
    resolved = {
        "thread_id": base.thread_id,
        "run_id": base.run_id,
        "node": base.node,
    }
    for field_name, source in scope_from.items():
        if field_name not in SCOPE_FIELDS:
            continue
        if source in kwargs:
            resolved[field_name] = str(kwargs[source])
    for field_name in SCOPE_FIELDS:
        if field_name in kwargs:
            resolved[field_name] = str(kwargs[field_name])
    return TransitionScope(**resolved)


def build_transition_preimage(
    *,
    scope: TransitionScope,
    dispatch_id: str | None,
    tool: str,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    side_effect_class: SideEffectClass,
    agent_id: str,
    policy_version: str,
    destination: tuple[str, ...] | None = None,
) -> dict[str, Any]:
    """Build the versioned preimage hashed into a transition / effect key."""
    if destination is None:
        from mycelium.entity_guard import (
            destination_fingerprint,
            get_active_entity_decision,
        )

        destination = destination_fingerprint(get_active_entity_decision())
    preimage: dict[str, Any] = {
        "schema": TRANSITION_SCHEMA,
        "scope": {
            "thread_id": scope.thread_id,
            "run_id": scope.run_id,
            "node": scope.node,
        },
        "tool": tool,
        "args_fingerprint": args_fingerprint(args, kwargs),
        "destination": list(destination),
        "side_effect_class": side_effect_class.value,
        "agent_id": agent_id,
        "policy_version": policy_version,
    }
    if dispatch_id is not None:
        preimage["dispatch_id"] = dispatch_id
    return preimage


def derive_transition_key(preimage: dict[str, Any]) -> str:
    """Hash a transition preimage into a durable transition key."""
    return hashlib.sha256(canonical_json(preimage).encode()).hexdigest()


def derive_effect_id(preimage: dict[str, Any]) -> str:
    """Hash an effect preimage into a stable effect identity (transition key)."""
    return derive_transition_key(preimage)


def extract_provider_idempotency_key(
    kwargs: dict[str, Any],
    binding: ToolTransitionBinding,
) -> str | None:
    """Return the declared provider idempotency key from a call's kwargs.

    Returns ``None`` when the tool does not opt into enforcement (no
    ``provider_idempotency_key_param``) or the kwarg is absent.
    """
    param = binding.provider_idempotency_key_param
    if param is None:
        return None
    value = kwargs.get(param)
    return str(value) if value is not None else None


def derive_transition_key_for_call(
    tool: str,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    binding: ToolTransitionBinding,
) -> str:
    """Derive the transition key for a tool invocation.

    When the tool declares a ``provider_idempotency_key_param``, that kwarg is
    excluded from the args fingerprint so a retry that changes the key still
    maps to the *same* transition. This lets the gate compare the stored key
    against the incoming one and hard-block a retry that does not reuse it.
    """
    scope = resolve_scope(scope_from=binding.scope_from, kwargs=kwargs)
    dispatch_id = derive_dispatch_id(kwargs)
    fingerprint_kwargs = kwargs
    if binding.provider_idempotency_key_param is not None:
        fingerprint_kwargs = {
            key: value
            for key, value in kwargs.items()
            if key != binding.provider_idempotency_key_param
        }
    preimage = build_transition_preimage(
        scope=scope,
        dispatch_id=dispatch_id,
        tool=tool,
        args=args,
        kwargs=fingerprint_kwargs,
        side_effect_class=binding.side_effect_class,
        agent_id=binding.agent_id,
        policy_version=binding.policy_version,
    )
    return derive_transition_key(preimage)


def derive_effect_id_for_call(
    tool: str,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    binding: ToolTransitionBinding,
) -> str:
    """Derive the stable effect identity for a tool invocation."""
    return derive_transition_key_for_call(tool, args, kwargs, binding)


__all__ = [
    "CONSEQUENTIAL_SIDE_EFFECT_CLASSES",
    "LEDGER_KWARG_KEYS",
    "MissingRequestIdentityError",
    "REQUEST_IDENTITY_POLICIES",
    "REQUEST_IDENTITY_POLICY_DERIVED",
    "REQUEST_IDENTITY_POLICY_REQUIRE_EXPLICIT",
    "SCOPE_FIELDS",
    "SIDE_EFFECT_CLASS_ALIASES",
    "EFFECT_SCHEMA",
    "TRANSITION_SCHEMA",
    "SideEffectClass",
    "SideEffectBoundary",
    "RetryPermission",
    "Spendability",
    "ToolCapability",
    "ToolCapabilityDeclarationError",
    "DEFAULT_RETRY_PERMISSION",
    "DEFAULT_SPENDABILITY",
    "STRICT_SIDE_EFFECT_CLASSES",
    "TerminalOutcome",
    "EffectState",
    "resolve_effect_state",
    "LeaseValidity",
    "ProviderKeyValidity",
    "ToolTransitionBinding",
    "TransitionConfig",
    "HandoffLink",
    "TransitionScope",
    "args_fingerprint",
    "blocks_on_ambiguous_replay",
    "build_transition_preimage",
    "canonical_json",
    "derive_dispatch_id",
    "parse_explicit_request_id",
    "request_id_from_argument",
    "derive_effect_id",
    "derive_effect_id_for_call",
    "derive_transition_key",
    "derive_transition_key_for_call",
    "dispatch_scope",
    "extract_provider_idempotency_key",
    "execution_scope",
    "get_active_dispatch_id",
    "get_active_execution_scope",
    "get_active_handoff",
    "handoff_scope",
    "legacy_status_from_terminal",
    "parse_side_effect_class",
    "parse_retry_permission",
    "parse_side_effect_boundary",
    "parse_spendability",
    "parse_capability",
    "default_capability",
    "resolve_capability",
    "parse_terminal_outcome",
    "resolve_lease_validity",
    "provider_key_validity",
    "resolve_retry_permission",
    "resolve_spendability",
    "resolve_side_effect_boundary_default",
    "resolve_scope",
    "resolve_terminal_outcome",
    "allows_failed_before_retry",
    "is_strict_side_effect",
    "terminal_from_legacy_status",
    "has_worker_death_evidence",
]
