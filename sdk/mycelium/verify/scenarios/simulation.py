"""Simulation: deterministic invariant checks over crash + fence-takeover sweeps.

This is the moat scenario: the empirical runs are timing-dependent, but the
*assertions* are pure and deterministic. After every crash phase and every
two-worker fence takeover, the scenario feeds the observed ledger rows and
provider effect log into the invariant checks in
:mod:`mycelium.verify.invariants` and requires:

* at most one COMPLETED ledger entry per effect_id, ever;
* at most one EffectState.COMMITTED ledger entry per effect_id (asserted on
  the unified EffectState view, and checked consistent with terminal_outcome);
* every provider-side effect maps to at most one COMPLETED entry.

Fence acquisition/loss is proven deterministically in-process over a shared
backend: worker A claims (fence N) and stalls; worker B reclaims (fence N+1);
A's stale-fence write is CAS-rejected; B's completion is the sole COMMITTED row.
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any

from mycelium.action_ledger import LedgerOutcomeAlreadySetError
from mycelium.transition import (
    SideEffectBoundary,
    SideEffectClass,
    TerminalOutcome,
    ToolTransitionBinding,
    TransitionScope,
    execution_scope,
)
from mycelium.verify.invariants import (
    check_at_most_one_committed,
    check_at_most_one_committed_effect_state,
    check_effect_state_consistency,
    check_no_duplicate_effect_ids,
    check_provider_mapping,
)
from mycelium.verify.registry import ScenarioContext, verify_scenario
from mycelium.verify.types import VerificationEvidence, VerificationStatus
from mycelium.verify.workers import (
    SYNTHETIC_TOOL,
    crash_worker,
    join_workers,
    make_ledger,
    read_lines,
    reconcile_worker,
    spawn_workers,
    terminate_owned,
)

_PHASES = ("after_claim", "after_body_start", "after_boundary", "after_effect")


def _wait_ready(path: str, timeout: float) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if os.path.exists(path) and Path(path).read_text(encoding="utf-8").strip():
            return True
        time.sleep(0.02)
    return False


def _idempotent_binding() -> ToolTransitionBinding:
    return ToolTransitionBinding.for_tool(
        agent_id="mycelium-verify",
        policy_version="verify",
        side_effect_class=SideEffectClass.IDEMPOTENT_MUTATE,
    )


def _run_crash_sweep(
    ctx: ScenarioContext,
    iso: Any,
    work: Path,
    spec: dict[str, Any],
    lease_ttl: float,
) -> tuple[list[str], list[str], list[str], int]:
    failures: list[str] = []
    limitations: list[str] = []
    decisions: list[str] = []
    total_exec = 0
    scenario_request_ids: list[str] = []
    for phase in _PHASES:
        request_id = iso.track(iso.namespace.request_id("sim", phase))
        scenario_request_ids.append(request_id)
        exec_file = str(work / f"{phase}-exec.txt")
        ready_file = str(work / f"{phase}-ready.txt")
        err_file = str(work / f"{phase}-err.txt")
        effect_file = str(work / f"{phase}-fx.txt")
        out_file = str(work / f"{phase}-out.txt")
        payload = {
            **spec,
            "phase": phase,
            "request_id": request_id,
            "exec_file": exec_file,
            "ready_file": ready_file,
            "err_file": err_file,
            "effect_file": effect_file,
            "out_file": out_file,
            "op_id": f"op-{phase}",
        }
        procs = spawn_workers(crash_worker, [payload])
        ctx.owned_procs.extend(procs)
        if not _wait_ready(ready_file, min(ctx.timeout_seconds, 8.0)):
            failures.append(f"{phase}: worker never reached crash marker")
            terminate_owned(procs)
            continue
        for proc in procs:
            if proc.is_alive():
                proc.kill()
        join_workers(procs, timeout=2.0)
        time.sleep(lease_ttl + 0.3)

        if phase in {"after_claim", "after_body_start"}:
            recovery_ledger = make_ledger(
                iso.open_storage(),
                binding=_idempotent_binding(),
                lease_ttl=lease_ttl,
            )
            recovery_ledger.release(
                request_id,
                verified="not_executed",
                by="simulation",
                reason=f"crash injected at {phase} before provider boundary",
            )

        recovery_payload = {
            **payload,
            "reconcile_status": ("COMPLETED" if phase == "after_effect" else "NOT_EXECUTED"),
            "poll_timeout": min(ctx.timeout_seconds, 8.0),
            "omit_op_id": True,
        }
        recovery = spawn_workers(reconcile_worker, [recovery_payload])
        ctx.owned_procs.extend(recovery)
        join_workers(recovery, timeout=min(ctx.timeout_seconds, 10.0))
        recovered = bool(read_lines(out_file))
        if phase == "after_boundary":
            if recovered:
                failures.append(f"{phase}: ambiguous transition was re-executed")
            else:
                decisions.append(f"{phase}: redispatch hard-blocked before provider")
        elif not recovered:
            errors = read_lines(err_file)
            failures.append(
                f"{phase}: supported recovery did not resolve redispatch"
                + (f" ({'; '.join(errors)})" if errors else "")
            )
        else:
            decisions.append(f"{phase}: redispatch recovered")
        total_exec += len(read_lines(exec_file))

        storage = iso.open_fresh_client()
        entries = [e for e in storage.list_all() if e.request_id in scenario_request_ids]
        violations = check_at_most_one_committed(entries)
        for item in violations:
            failures.append(f"{phase}: {item.message}")
        for item in check_at_most_one_committed_effect_state(entries):
            failures.append(f"{phase}: {item.message}")
        for item in check_effect_state_consistency(entries):
            failures.append(f"{phase}: {item.message}")
        provider_effects = read_lines(effect_file)
        if provider_effects:
            mapped, warnings = check_provider_mapping(
                entries,
                [(request_id, provider_id) for provider_id in provider_effects],
            )
            for item in mapped:
                failures.append(f"{phase}: {item.message}")
            limitations.extend(f"{phase}: {msg}" for msg in warnings)
        decisions.append(
            f"{phase}: post-recovery provider invariant held ({len(provider_effects)} attempts)"
        )
    return failures, limitations, decisions, total_exec


def _run_fence_takeover(
    iso: Any,
    work: Path,
) -> tuple[list[str], list[str]]:
    failures: list[str] = []
    decisions: list[str] = []
    provider_effects: list[tuple[str, str]] = []
    request_id = iso.track(iso.namespace.request_id("sim", "fence"))
    redispatch_request_id = f"{request_id}:redispatch"
    binding = _idempotent_binding()
    kwargs = {"thread_id": "verify", "run_id": "verify"}

    a = make_ledger(iso.open_storage(), binding=binding, lease_ttl=0.3)
    with execution_scope(TransitionScope(thread_id="verify", run_id="verify")):
        entry_a = a.claim_side_effecting(request_id, SYNTHETIC_TOOL, (1,), kwargs, binding)
    fence_a = entry_a.fence
    owner_a = entry_a.owner
    time.sleep(0.7)

    b = make_ledger(iso.open_storage(), binding=binding, lease_ttl=30.0)
    with execution_scope(TransitionScope(thread_id="verify", run_id="verify")):
        entry_b = b.claim_side_effecting(
            redispatch_request_id,
            SYNTHETIC_TOOL,
            (1,),
            kwargs,
            binding,
        )
    fence_b = entry_b.fence
    if fence_b <= fence_a:
        failures.append(f"fence takeover: B fence {fence_b} not above A fence {fence_a}")
        return failures, decisions
    decisions.append(f"fence takeover: A={fence_a} -> B={fence_b}")

    try:
        with execution_scope(TransitionScope(thread_id="verify", run_id="verify")):
            a.advance_boundary(
                request_id,
                SideEffectBoundary.MAYBE_CROSSED,
                expected_owner=owner_a,
                expected_fence=fence_a,
            )
        failures.append(
            f"fence takeover: stale worker A crossed provider boundary with "
            f"fence {fence_a} while stored fence is {fence_b}"
        )
    except LedgerOutcomeAlreadySetError:
        decisions.append("fence takeover: stale A rejected before provider boundary")

    try:
        with execution_scope(TransitionScope(thread_id="verify", run_id="verify")):
            a.complete(
                request_id,
                {"charged": "STALE"},
                _expected_owner=owner_a,
                _expected_fence=fence_a,
            )
        failures.append(
            f"fence takeover: stale worker A completed with fence {fence_a} "
            f"while stored fence is {fence_b}"
        )
    except LedgerOutcomeAlreadySetError:
        decisions.append("fence takeover: stale A rejected by fence CAS")

    with execution_scope(TransitionScope(thread_id="verify", run_id="verify")):
        b.record_decision(
            request_id,
            {"allowed": True, "verdicts": [], "denied_reasons": []},
            expected_owner=entry_b.owner,
            expected_fence=fence_b,
        )
        b.advance_boundary(
            request_id,
            SideEffectBoundary.MAYBE_CROSSED,
            expected_owner=entry_b.owner,
            expected_fence=fence_b,
        )
        provider_id = "fence-provider-B"
        b.attach_external_operation_ref(
            request_id,
            provider_id,
            expected_owner=entry_b.owner,
            expected_fence=fence_b,
        )
        provider_effects.append((request_id, provider_id))
        b.advance_boundary(
            request_id,
            SideEffectBoundary.CROSSED,
            expected_owner=entry_b.owner,
            expected_fence=fence_b,
        )
        b.complete(
            request_id,
            {"charged": True},
            _expected_owner=entry_b.owner,
            _expected_fence=fence_b,
        )

    final = iso.open_fresh_client().get(request_id)
    if final is None:
        failures.append("fence takeover: final entry missing")
        return failures, decisions
    if final.resolved_terminal_outcome() != TerminalOutcome.COMPLETED:
        failures.append(
            f"fence takeover: final outcome {final.terminal_outcome!r}, expected COMPLETED"
        )
    if final.fence != fence_b:
        failures.append(f"fence takeover: final fence {final.fence} != B fence {fence_b}")
    if final.result != {"charged": True}:
        failures.append("fence takeover: stale A write leaked into final entry")
    duplicate_effect_rows = check_no_duplicate_effect_ids(iso.open_fresh_client().list_all())
    for violation in duplicate_effect_rows:
        failures.append(f"fence takeover: {violation.message}")
    for violation in check_at_most_one_committed([final]):
        failures.append(f"fence takeover: {violation.message}")
    for violation in check_at_most_one_committed_effect_state([final]):
        failures.append(f"fence takeover: {violation.message}")
    for violation in check_effect_state_consistency([final]):
        failures.append(f"fence takeover: {violation.message}")
    mapped, warnings = check_provider_mapping([final], provider_effects)
    for violation in mapped:
        failures.append(f"fence takeover: {violation.message}")
    for warning in warnings:
        failures.append(f"fence takeover: {warning}")
    if not failures:
        decisions.append(
            "fence takeover: B sole COMMITTED row and provider attempt; A stale write refused"
        )
    return failures, decisions


@verify_scenario("simulation")
def run_simulation(ctx: ScenarioContext) -> VerificationEvidence:
    started = time.time()
    iso = ctx.isolation
    if not iso.multiprocess_capable:
        return VerificationEvidence(
            scenario="simulation",
            backend=iso.backend,
            namespace=iso.namespace.prefix,
            duration=time.time() - started,
            expected_behavior=(
                "at-most-one-COMMITTED invariant holds across crash sweep and "
                "two-worker fence takeover"
            ),
            observed_behavior="backend cannot share crash state across processes",
            limitations=["memory cannot prove crash durability"],
            status=VerificationStatus.SKIP,
            summary="Simulation skipped (not multiprocess-capable)",
            remediation="Use file/sqlite/postgres/redis.",
        )

    work = iso.artifact_dir("simulation-")
    lease_ttl = 1.0
    spec = {
        **iso.worker_payload,
        "run_id": iso.namespace.run_id,
        "prefix_ns": iso.namespace.prefix,
        "lease_ttl": lease_ttl,
        "reclaim_requires_death_signal": True,
    }
    failures: list[str] = []
    limitations: list[str] = []
    decisions: list[str] = []
    total_exec = 0
    try:
        crash_failures, crash_limitations, crash_decisions, crash_exec = _run_crash_sweep(
            ctx, iso, work, spec, lease_ttl
        )
        failures.extend(crash_failures)
        limitations.extend(crash_limitations)
        decisions.extend(crash_decisions)
        total_exec += crash_exec

        fence_failures, fence_decisions = _run_fence_takeover(iso, work)
        failures.extend(fence_failures)
        decisions.extend(fence_decisions)
    finally:
        terminate_owned(ctx.owned_procs)

    ok = not failures
    if iso.backend in {"file", "sqlite"}:
        limitations.append("single-node verification only")
    return VerificationEvidence(
        scenario="simulation",
        backend=iso.backend,
        namespace=iso.namespace.prefix,
        attempts=len(_PHASES) + 1,
        body_executions=total_exec,
        ledger_decisions=decisions,
        terminal_outcome="COMMITTED",
        duration=time.time() - started,
        expected_behavior=(
            "for every effect_id at most one COMPLETED ledger entry, and every "
            "provider effect maps to at most one COMMITTED row; a superseded "
            "worker's stale-fence write is rejected"
        ),
        observed_behavior="; ".join(failures or decisions),
        artifacts=[str(work)],
        limitations=limitations,
        status=VerificationStatus.PASS if ok else VerificationStatus.FAIL,
        summary=("at-most-one-COMMITTED invariant held" if ok else "; ".join(failures)[:200]),
        remediation="" if ok else "Inspect the invariant violations above.",
    )
