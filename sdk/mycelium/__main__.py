"""CLI entrypoint: init, demo, run, migrate, and operational utilities."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time
from importlib import resources
from pathlib import Path
from typing import Any

from mycelium.transition import TerminalOutcome

_TEMPLATE_QUICKSTART = "mycelium.quickstart.yaml"
_TEMPLATE_FULL = "mycelium.template.yaml"
_TEMPLATE_MINIMAL = "mycelium.minimal.yaml"

# Direct operator-storage flags fall back to these environment variables, so
# operator machines without the app's mycelium.yaml can still reach the ledger.
_ENV_LEDGER_FILE = "MYCELIUM_LEDGER_FILE"
_ENV_REDIS_URL = "MYCELIUM_REDIS_URL"
_ENV_POSTGRES_DSN = "MYCELIUM_POSTGRES_DSN"
_ENV_SQLITE_PATH = "MYCELIUM_SQLITE_PATH"
_ENV_OUTCOME_FILE = "MYCELIUM_OUTCOME_FILE"
_ENV_ADAPTER_REPORT_SIGNING_KEY = "MYCELIUM_ADAPTER_REPORT_SIGNING_KEY"


def _load_template(*, full: bool, minimal: bool) -> tuple[str, str]:
    if full:
        filename = _TEMPLATE_FULL
        label = "full"
    elif minimal:
        filename = _TEMPLATE_MINIMAL
        label = "minimal"
    else:
        filename = _TEMPLATE_QUICKSTART
        label = "quickstart"
    path = resources.files("mycelium") / "templates" / filename
    return path.read_text(encoding="utf-8"), label


def cmd_init(
    output: Path,
    *,
    full: bool,
    minimal: bool,
    force: bool,
    detect: bool = False,
    project: Path = Path("."),
) -> int:
    if output.exists() and not force:
        print(f"error: {output} already exists (use --force to overwrite)", file=sys.stderr)
        return 1
    if detect:
        from mycelium.config_detect import render_detected_config, write_schema_sidecar

        text, detection = render_detected_config(project)
        label = "detected"
    else:
        text, label = _load_template(full=full, minimal=minimal)
        detection = None
    output.write_text(text, encoding="utf-8")
    print(f"Wrote {output} ({label} template)")
    if detection is not None:
        schema_path = write_schema_sidecar(output, force=force)
        frameworks = ", ".join(detection.frameworks) or "none"
        print(
            f"Detected frameworks: {frameworks}; decorated tools: "
            f"{len(detection.tools)}; scanned Python files: {detection.scanned_files}."
        )
        if schema_path is not None:
            print(f"Wrote {schema_path} (IDE schema)")
        print("Review detected tools before production use; mutation was assumed for safety.")
    elif label == "quickstart":
        print(
            "Next: install mycelium-runtime[langgraph], fill the IDs/callable path, "
            "then use 'mycelium run -- python -m your_package.app'."
        )
        print("Try: mycelium demo")
    else:
        print("Next: edit tool/task names, then load_config(...) in your agent code.")
    print(
        "Prefer wrappers (mycelium run / @ledger_sync). For a custom tool loop, "
        "see sdk/README.md — Manual integration (claim → execute → complete)."
    )
    return 0


def cmd_config_schema(output: Path | None) -> int:
    """Print or write the JSON Schema for the current config version."""
    from mycelium.config import config_json_schema

    text = json.dumps(config_json_schema(), indent=2, sort_keys=True) + "\n"
    if output is None:
        print(text, end="")
    else:
        output.write_text(text, encoding="utf-8")
        print(f"Wrote {output}")
    return 0


def _write_or_print_config_artifact(text: str, output: Path | None) -> int:
    if output is None:
        print(text, end="")
    else:
        output.write_text(text, encoding="utf-8")
        print(f"Wrote {output}")
    return 0


def cmd_config_docs(output: Path | None) -> int:
    """Print or write reference Markdown generated from JSON Schema."""
    from mycelium.config_artifacts import render_config_reference

    return _write_or_print_config_artifact(render_config_reference(), output)


def cmd_config_example(output: Path | None) -> int:
    """Print or write a model-validated starter configuration."""
    from mycelium.config_artifacts import render_config_example

    return _write_or_print_config_artifact(render_config_example(), output)


def cmd_demo(*, redis: bool = False, slow: bool = False) -> int:
    from mycelium.quickstart import run_demo

    return run_demo(redis=redis, slow=slow)


def _adapter_report_signing_key(env_name: str) -> str:
    value = os.environ.get(env_name, "").strip()
    if not value:
        raise ValueError(f"environment variable {env_name!r} is not set")
    return value


def cmd_providers_verify(args: argparse.Namespace) -> int:
    """Run the synthetic provider suite and write a signed report."""

    from mycelium.provider_conformance import (
        adapter_report_json,
        create_adapter_verification_report,
    )
    from mycelium.providers import get_provider_conformance_fixture

    try:
        signing_key = _adapter_report_signing_key(args.signing_key_env)
        fixture = get_provider_conformance_fixture(args.adapter)
        report = create_adapter_verification_report(
            fixture,
            signing_key=signing_key,
            signer_key_id=args.key_id or args.signing_key_env,
        )
    except (OSError, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    rendered = adapter_report_json(report) + "\n"
    if args.output is None:
        print(rendered, end="")
    else:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
        print(f"Wrote signed adapter report: {args.output}")
        print(f"Adapter {report.adapter_name}: {report.status}")
    return 0 if report.verified else 1


def cmd_providers_verify_report(args: argparse.Namespace) -> int:
    """Verify a stored adapter report's signature and passing status."""

    from mycelium.provider_conformance import (
        AdapterVerificationReport,
        adapter_report_is_verified,
        adapter_report_matches_fixture,
        verify_adapter_report_signature,
    )
    from mycelium.providers import get_provider_conformance_fixture

    try:
        signing_key = _adapter_report_signing_key(args.signing_key_env)
        report = AdapterVerificationReport.from_dict(
            json.loads(args.report.read_text(encoding="utf-8"))
        )
    except (KeyError, json.JSONDecodeError, OSError, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    authentic = verify_adapter_report_signature(report, signing_key)
    try:
        fixture = get_provider_conformance_fixture(report.adapter_name)
    except ValueError:
        fixture = None
    source_matches = fixture is not None and adapter_report_matches_fixture(report, fixture)
    verified = fixture is not None and adapter_report_is_verified(
        report, signing_key, fixture=fixture
    )
    if args.json:
        print(
            json.dumps(
                {
                    "adapter": report.adapter_name,
                    "authentic": authentic,
                    "report_status": report.status,
                    "source_matches": source_matches,
                    "verified": verified,
                },
                indent=2,
                sort_keys=True,
            )
        )
    else:
        print(
            f"Adapter {report.adapter_name}: signature="
            f"{'valid' if authentic else 'invalid'} status={report.status} "
            f"source={'matches' if source_matches else 'changed'}"
        )
    return 0 if verified else 1


def _validated_python_command(command: list[str]) -> list[str]:
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        raise ValueError("missing command after '--'")

    executable = shutil.which(command[0])
    if executable is None:
        raise ValueError(f"Python executable not found: {command[0]!r}")
    if Path(executable).resolve() != Path(sys.executable).resolve():
        raise ValueError(
            f"'mycelium run' requires the current Python interpreter; use {sys.executable!r}"
        )
    forbidden = {"-E", "-I", "-S"}
    present = forbidden.intersection(command[1:])
    if present:
        flags = ", ".join(sorted(present))
        raise ValueError(f"Python flag(s) {flags} disable safe Mycelium startup instrumentation")
    return [executable, *command[1:]]


def cmd_run(config_path: Path, command: list[str]) -> int:
    """Replace this process with an auto-instrumented Python command."""
    from mycelium.auto_instrumentation import AUTO_CONFIG_ENV, AUTO_ENABLED_ENV
    from mycelium.config import ConfigError, load_config

    resolved_config = config_path.resolve()
    try:
        config = load_config(resolved_config)
        config.auto_instrumentation_targets()
        child_command = _validated_python_command(command)
    except (ConfigError, OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    bootstrap_dir = Path(__file__).resolve().parent / "auto_instrumentation" / "site_bootstrap"
    env = dict(os.environ)
    # sitecustomize imports tools before the interpreter prepends CWD to
    # sys.path (``python -m`` / ``-c``). Keep CWD importable for fresh
    # project layouts like ``my_app.tools`` without requiring PYTHONPATH=.
    pythonpath_parts = [str(bootstrap_dir), os.getcwd()]
    current_pythonpath = env.get("PYTHONPATH")
    if current_pythonpath:
        pythonpath_parts.append(current_pythonpath)
    env["PYTHONPATH"] = os.pathsep.join(pythonpath_parts)
    env[AUTO_ENABLED_ENV] = "1"
    env[AUTO_CONFIG_ENV] = str(resolved_config)

    try:
        os.execvpe(child_command[0], child_command, env)
    except OSError as exc:
        print(f"error: cannot start {child_command[0]!r}: {exc}", file=sys.stderr)
        return 127
    return 127


def _add_operator_storage_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "-c",
        "--config",
        type=Path,
        default=None,
        help="mycelium.yaml to read ledger storage from (default: ./mycelium.yaml)",
    )
    parser.add_argument(
        "--file",
        dest="file_path",
        type=Path,
        default=None,
        help=f"JSON file ledger path (or ${_ENV_LEDGER_FILE}); overrides --config",
    )
    parser.add_argument(
        "--redis-url",
        default=None,
        help=f"Redis ledger URL (or ${_ENV_REDIS_URL}); overrides --config",
    )
    parser.add_argument(
        "--postgres-dsn",
        default=None,
        help=f"Postgres ledger DSN (or ${_ENV_POSTGRES_DSN}); overrides --config",
    )
    parser.add_argument(
        "--sqlite",
        dest="sqlite_path",
        type=Path,
        default=None,
        help=f"SQLite ledger DB path (or ${_ENV_SQLITE_PATH}); overrides --config",
    )


def _operator_storage_configs(args: argparse.Namespace) -> list[dict[str, Any]]:
    """Resolve normalized ledger-storage config dicts from flags/env or config.

    Direct flags (--file/--redis-url/--postgres-dsn/--sqlite, with env fallback)
    win over --config. Config mode reuses each tool's normalized ``ledger:``
    section, deduplicating tools that share one backend.
    """
    from mycelium.config import ConfigError, load_config

    file_path = args.file_path or os.environ.get(_ENV_LEDGER_FILE)
    redis_url = args.redis_url or os.environ.get(_ENV_REDIS_URL)
    postgres_dsn = args.postgres_dsn or os.environ.get(_ENV_POSTGRES_DSN)
    sqlite_path = args.sqlite_path or os.environ.get(_ENV_SQLITE_PATH)
    direct: list[dict[str, Any]] = []
    if file_path:
        direct.append({"storage": "file", "path": str(file_path)})
    if redis_url:
        direct.append({"storage": "redis", "url": redis_url})
    if postgres_dsn:
        direct.append({"storage": "postgres", "dsn": postgres_dsn})
    if sqlite_path:
        direct.append({"storage": "sqlite", "path": str(sqlite_path)})
    if direct:
        return direct

    config_path = args.config if args.config is not None else Path("mycelium.yaml")
    if not config_path.is_file():
        raise ConfigError(
            f"no ledger storage specified and config not found: {config_path} "
            "(pass --config, or --file/--redis-url/--postgres-dsn/--sqlite)"
        )
    config = load_config(config_path)
    raws: list[dict[str, Any]] = []
    seen: set[str] = set()
    for tool in config.tools.values():
        if tool.ledger is None:
            continue
        key = json.dumps(tool.ledger, sort_keys=True, default=str)
        if key in seen:
            continue
        seen.add(key)
        raws.append(tool.ledger)
    if not raws:
        raise ConfigError(f"{config_path} declares no tool ledger storage")
    return raws


def _operator_ledgers(args: argparse.Namespace) -> list[Any]:
    """Build ActionLedgers over the resolved operator storage backends."""
    from mycelium.action_ledger import ActionLedger

    return [ActionLedger(storage=storage) for storage in _operator_storages(args)]


def _operator_storages(args: argparse.Namespace) -> list[Any]:
    """Build the resolved durable operator storage backends."""
    from mycelium.config import ConfigError, MyceliumConfig

    storages: list[Any] = []
    for raw in _operator_storage_configs(args):
        storage_type = raw.get("storage", "memory")
        if storage_type == "memory":
            raise ConfigError(
                "ledger storage is 'memory', which lives inside the agent "
                "process — the CLI cannot reach it. Use the Python API "
                "(ActionLedger.list_transitions() / ActionLedger.release(...)) "
                "from a process sharing that storage, or point the CLI at a "
                "durable backend with --file/--redis-url/--postgres-dsn/--sqlite"
            )
        storages.append(MyceliumConfig._build_ledger_storage(raw))
    return storages


def _find_operator_entry(args: argparse.Namespace, request_id: str) -> tuple[Any, Any]:
    """Return (ledger, entry) for request_id across the resolved backends."""
    from mycelium.config import ConfigError

    ledgers = _operator_ledgers(args)
    for ledger in ledgers:
        entry = ledger.get(request_id)
        if entry is not None:
            return ledger, entry
    raise ConfigError(f"no ledger entry found for request {request_id!r}")


def _format_age(age_seconds: float) -> str:
    if age_seconds < 0:
        age_seconds = 0
    if age_seconds < 60:
        return f"{int(age_seconds)}s"
    if age_seconds < 3600:
        return f"{int(age_seconds // 60)}m"
    if age_seconds < 86400:
        return f"{int(age_seconds // 3600)}h"
    return f"{int(age_seconds // 86400)}d"


def _format_ts(timestamp: float | None) -> str:
    if timestamp is None:
        return "-"
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(timestamp))


def _next_action_hint(entry: Any, resolved: Any) -> str:
    from mycelium.transition import TerminalOutcome

    rid = entry.request_id
    if resolved in (
        TerminalOutcome.BLOCKED,
        TerminalOutcome.UNKNOWN,
        TerminalOutcome.FAILED_AFTER_EFFECT,
    ):
        ref = f" (ref {entry.external_operation_ref})" if entry.external_operation_ref else ""
        return (
            f"verify effect with provider{ref}, then: mycelium transitions release "
            f"{rid} --verified completed|not-executed --by ... --reason ..."
        )
    if resolved == TerminalOutcome.EXPIRED:
        if entry.worker_dead_asserted_at is None:
            return (
                f"worker died mid-flight; if reclaim_requires_death_signal is on, "
                f"mark dead first: mycelium transitions mark-dead {rid} --by ... --reason ...; "
                f"otherwise release {rid} --verified completed|not-executed"
            )
        return (
            f"worker died mid-flight; verify with provider, then release {rid} "
            "--verified completed|not-executed"
        )
    # Stuck IN_FLIGHT: old but lease still held/unbounded.
    return (
        f"lease still held; confirm the worker ({entry.owner}) is dead and the "
        f"effect never ran before releasing {rid}"
    )


def _entry_row(entry: Any, now: float) -> dict[str, Any]:
    resolved = entry.resolved_terminal_outcome(now=now)
    row = entry.to_dict()
    row["resolved_outcome"] = resolved.value
    row["age_seconds"] = max(0.0, now - entry.started_at)
    row["last_heartbeat_at"] = entry.last_heartbeat_at
    row["worker_dead_asserted_by"] = entry.worker_dead_asserted_by
    row["worker_dead_asserted_at"] = entry.worker_dead_asserted_at
    return row


def cmd_transitions_list(args: argparse.Namespace) -> int:
    from mycelium.config import ConfigError

    try:
        ledgers = _operator_ledgers(args)
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    now = time.time()
    outcome = TerminalOutcome(args.outcome) if args.outcome is not None else None
    entries = []
    next_cursors: list[str] = []
    for ledger in ledgers:
        if args.limit is not None or args.cursor is not None:
            page = ledger.list_transitions_page(
                limit=args.limit or 100,
                cursor=args.cursor,
                stuck=args.stuck,
                tool=args.tool,
                outcome=outcome,
                parent_request_id=getattr(args, "parent", None),
            )
            entries.extend(page.entries)
            if page.next_cursor is not None:
                next_cursors.append(page.next_cursor)
        else:
            entries.extend(
                ledger.list_transitions(
                    stuck=args.stuck,
                    tool=args.tool,
                    outcome=outcome,
                    parent_request_id=getattr(args, "parent", None),
                )
            )
    entries.sort(key=lambda entry: entry.started_at)
    if args.json:
        from mycelium.secret_protection import sanitize_for_evidence

        rows = sanitize_for_evidence([_entry_row(entry, now) for entry in entries])
        payload: Any = rows
        if args.limit is not None or args.cursor is not None:
            payload = {
                "transitions": rows,
                "next_cursor": next_cursors[0] if len(next_cursors) == 1 else None,
            }
        print(json.dumps(payload, indent=2, default=str))
        return 0
    if not entries:
        print("no transitions found" + (" (stuck only)" if args.stuck else ""))
        return 0
    for entry in entries:
        resolved = entry.resolved_terminal_outcome(now=now)
        age = _format_age(now - entry.started_at)
        line = f"{entry.request_id}  {entry.tool}  {resolved.value}  age={age}"
        if entry.parent_request_id is not None:
            line += f"  parent={entry.parent_request_id}"
        if entry.operator_resolution is not None:
            line += f"  [released: {entry.operator_resolution} by {entry.resolved_by}]"
        print(line)
        if args.stuck:
            print(f"    next: {_next_action_hint(entry, resolved)}")
    return 0


def cmd_transitions_export(args: argparse.Namespace) -> int:
    from mycelium.config import ConfigError
    from mycelium.secret_protection import sanitize_for_evidence
    from mycelium.transition import TerminalOutcome

    try:
        ledgers = _operator_ledgers(args)
        outcome = TerminalOutcome(args.outcome) if args.outcome else None
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        count = 0
        with output.open("w", encoding="utf-8") as handle:
            for ledger in ledgers:
                cursor: str | None = None
                while True:
                    page = ledger.list_transitions_page(
                        limit=args.page_size,
                        cursor=cursor,
                        tool=args.tool,
                        outcome=outcome,
                    )
                    for entry in page.entries:
                        row = sanitize_for_evidence(entry.to_dict())
                        handle.write(json.dumps(row, default=str, sort_keys=True) + "\n")
                        count += 1
                    cursor = page.next_cursor
                    if cursor is None:
                        break
    except (ConfigError, OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(f"exported {count} transitions to {output}")
    return 0


def cmd_transitions_prune(args: argparse.Namespace) -> int:
    from mycelium.budget_guard import parse_duration_seconds
    from mycelium.config import ConfigError
    from mycelium.secret_protection import sanitize_for_evidence
    from mycelium.transition import TerminalOutcome

    try:
        ledgers = _operator_ledgers(args)
        before = None
        if args.older_than is not None:
            before = time.time() - parse_duration_seconds(args.older_than)
        outcomes = (
            frozenset(TerminalOutcome(value) for value in args.outcome) if args.outcome else None
        )
        candidates: list[Any] = []
        plans: list[tuple[Any, list[Any]]] = []
        for ledger in ledgers:
            selected, _ = ledger.prune_transitions(
                before=before,
                outcomes=outcomes,
                dry_run=True,
                limit=args.page_size,
            )
            candidates.extend(selected)
            plans.append((ledger, selected))
        if args.archive is not None:
            archive = Path(args.archive)
            archive.parent.mkdir(parents=True, exist_ok=True)
            with archive.open("w", encoding="utf-8") as handle:
                for entry in candidates:
                    handle.write(
                        json.dumps(
                            sanitize_for_evidence(entry.to_dict()),
                            default=str,
                            sort_keys=True,
                        )
                        + "\n"
                    )
        deleted = 0
        if args.execute:
            for ledger, selected in plans:
                deleted += ledger.delete_transitions([entry.request_id for entry in selected])
        mode = "would prune" if not args.execute else "pruned"
        print(f"{mode} {len(candidates) if not args.execute else deleted} transitions")
        if args.archive is not None:
            print(f"archive: {args.archive}")
    except (ConfigError, OSError, ValueError, NotImplementedError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return 0


def cmd_transitions_show(args: argparse.Namespace) -> int:
    from mycelium.config import ConfigError

    try:
        _, entry = _find_operator_entry(args, args.request_id)
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    resolved = entry.resolved_terminal_outcome()
    print(f"request_id: {entry.request_id}")
    print(f"tool: {entry.tool}")
    from mycelium.secret_protection import sanitize_for_evidence, sanitize_text

    print(f"args: {json.dumps(sanitize_for_evidence(entry.args), default=str)}")
    print(f"kwargs: {json.dumps(sanitize_for_evidence(entry.kwargs), default=str)}")
    print(f"status: {entry.status}")
    print(f"resolved_outcome: {resolved.value}")
    print(f"parent_request_id: {entry.parent_request_id or '-'}")
    print(f"handoff_id: {entry.handoff_id or '-'}")
    print(f"side_effect_boundary: {entry.side_effect_boundary}")
    print(f"started_at: {_format_ts(entry.started_at)}")
    print(f"finished_at: {_format_ts(entry.finished_at)}")
    print(f"lease_until: {_format_ts(entry.lease_until)}")
    print(f"owner: {entry.owner or '-'}")
    print(f"idempotency_key: {entry.idempotency_key}")
    print(f"provider_idempotency_key: {entry.provider_idempotency_key or '-'}")
    pkey_first = entry.provider_key_first_attempt_at
    if pkey_first is not None:
        pkey_age = time.time() - pkey_first
        print(f"provider_key_first_attempt_at: {_format_ts(pkey_first)} (age={pkey_age:.1f}s)")
    else:
        print("provider_key_first_attempt_at: -")
    print(f"external_operation_ref: {entry.external_operation_ref or '-'}")
    print(f"error: {sanitize_text(entry.error) if entry.error else '-'}")
    print(f"result: {json.dumps(sanitize_for_evidence(entry.result), default=str)}")
    print(f"receipt_ref: {entry.receipt_ref or '-'}")
    print(f"operator_resolution: {entry.operator_resolution or '-'}")
    print(f"resolved_by: {entry.resolved_by or '-'}")
    print(f"resolution_reason: {entry.resolution_reason or '-'}")
    print(f"resolved_at: {_format_ts(entry.resolved_at)}")
    print(f"released_from_outcome: {entry.released_from_outcome or '-'}")
    print(f"last_heartbeat_at: {_format_ts(entry.last_heartbeat_at)}")
    print(f"worker_dead_asserted_by: {entry.worker_dead_asserted_by or '-'}")
    print(f"worker_dead_asserted_at: {_format_ts(entry.worker_dead_asserted_at)}")
    return 0


def cmd_transitions_release(args: argparse.Namespace) -> int:
    from mycelium.action_ledger import (
        OPERATOR_RESOLUTION_COMPLETED,
        OPERATOR_RESOLUTION_NOT_EXECUTED,
        LedgerError,
    )
    from mycelium.config import ConfigError

    verified = (
        OPERATOR_RESOLUTION_COMPLETED
        if args.verified == "completed"
        else OPERATOR_RESOLUTION_NOT_EXECUTED
    )
    result = None
    if args.result_json is not None:
        if verified != OPERATOR_RESOLUTION_COMPLETED:
            print(
                "error: --result-json only applies to --verified completed",
                file=sys.stderr,
            )
            return 2
        try:
            result = json.loads(args.result_json)
        except json.JSONDecodeError as exc:
            print(f"error: invalid --result-json: {exc}", file=sys.stderr)
            return 2
    try:
        ledger, _ = _find_operator_entry(args, args.request_id)
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    try:
        entry = ledger.release(
            args.request_id,
            verified=verified,
            result=result,
            by=args.by,
            reason=args.reason,
        )
    except LedgerError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        f"released {entry.request_id} ({entry.tool}): verified={verified} "
        f"by={args.by} from={entry.released_from_outcome}"
    )
    if verified == OPERATOR_RESOLUTION_COMPLETED:
        print("next redispatch returns the recorded result without re-executing")
    else:
        print("next agent redispatch re-executes exactly once (release consumed)")
    return 0


def cmd_transitions_mark_dead(args: argparse.Namespace) -> int:
    from mycelium.config import ConfigError

    try:
        ledger, _ = _find_operator_entry(args, args.request_id)
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    try:
        entry = ledger.mark_worker_dead_for(
            args.request_id,
            by=args.by,
            reason=args.reason,
            override_heartbeat=args.override_heartbeat,
        )
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        f"marked worker dead for {entry.request_id} ({entry.tool}): "
        f"asserted_by={entry.worker_dead_asserted_by}"
    )
    return 0


def cmd_migrate(args: argparse.Namespace) -> int:
    """Plan or apply explicit ActionLedger schema migrations."""
    from mycelium.action_ledger import LedgerSchemaVersionError
    from mycelium.config import ConfigError
    from mycelium.ledger_migrations import (
        LedgerMigrationError,
        apply_ledger_migration,
        plan_ledger_migration,
    )

    try:
        storages = _operator_storages(args)
        plans = [
            plan_ledger_migration(storage, target_version=args.target_version)
            for storage in storages
        ]
    except (
        ConfigError,
        LedgerMigrationError,
        LedgerSchemaVersionError,
        OSError,
        ValueError,
    ) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    payload: dict[str, Any] = {
        "mode": "plan" if args.plan else "apply",
        "target_version": args.target_version,
        "backends": [
            {"backend": index, **plan.to_dict()} for index, plan in enumerate(plans, start=1)
        ],
    }
    unsupported = any(not plan.can_apply for plan in plans)

    if args.plan:
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
        else:
            print(f"Ledger migration plan: target schema {args.target_version}")
            for index, plan in enumerate(plans, start=1):
                versions = (
                    ", ".join(
                        f"v{version}={count}"
                        for version, count in sorted(plan.version_counts.items())
                    )
                    or "empty"
                )
                print(
                    f"backend {index}: total={plan.total_entries} "
                    f"migrate={plan.pending_entries} current={plan.current_entries} "
                    f"active={plan.active_pending_entries} ({versions})"
                )
                if plan.unsupported_versions:
                    print(f"  unsupported versions: {list(plan.unsupported_versions)}")
            print("No ledger rows were changed.")
        return 1 if unsupported else 0

    if unsupported:
        print("error: migration refused because unsupported schema versions exist", file=sys.stderr)
        return 1

    try:
        results = [
            apply_ledger_migration(
                storage,
                target_version=args.target_version,
                allow_active=bool(args.allow_active),
            )
            for storage in storages
        ]
    except (LedgerMigrationError, LedgerSchemaVersionError, OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    payload["backends"] = [
        {"backend": index, **result.to_dict()} for index, result in enumerate(results, start=1)
    ]
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        for index, result in enumerate(results, start=1):
            print(
                f"backend {index}: migrated={result.migrated_entries} "
                f"unchanged={result.unchanged_entries} schema={result.target_version}"
            )
        print("Ledger migration complete. Run 'mycelium migrate --plan' to verify.")
    return 0


def cmd_state_migrate(args: argparse.Namespace) -> int:
    """Plan or copy legacy guard state into ``state_backend``."""

    from mycelium.config import ConfigError, load_config
    from mycelium.state_migrations import (
        StateMigrationError,
        apply_state_migration,
        plan_state_migration,
    )

    try:
        cfg = load_config(args.config)
        plan = plan_state_migration(cfg)
    except (ConfigError, StateMigrationError, OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    if args.plan:
        payload = {"mode": "plan", **plan.to_dict()}
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
        else:
            print(
                "Shared-state migration plan: "
                f"total={plan.total_records} migrate={plan.pending_records} "
                f"unchanged={plan.unchanged_records} conflicts={plan.conflicting_records}"
            )
            for feature, count in sorted(plan.feature_counts.items()):
                print(f"  {feature}: {count}")
            print("No state records were changed.")
        return 1 if not plan.can_apply else 0

    if not plan.can_apply:
        print("error: migration refused because destination conflicts exist", file=sys.stderr)
        return 1
    try:
        result = apply_state_migration(cfg)
    except (StateMigrationError, OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    payload = {"mode": "apply", **result.to_dict()}
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(
            f"Shared-state migration complete: migrated={result.migrated_records} "
            f"unchanged={result.unchanged_records}"
        )
        print("Switch migrated feature storage to 'shared', then run mycelium doctor.")
    return 0


def _outcome_storage_config(
    args: argparse.Namespace,
) -> tuple[dict[str, Any], float | None]:
    """Resolve the outcome-log storage config + long_running_after override."""
    from mycelium.config import ConfigError, load_config

    file_path = args.outcome_file or os.environ.get(_ENV_OUTCOME_FILE)
    if file_path:
        return {"storage": "file", "path": str(file_path)}, None

    config_path = args.config if args.config is not None else Path("mycelium.yaml")
    if not config_path.is_file():
        raise ConfigError(
            f"no outcome log specified and config not found: {config_path} "
            "(pass --file, or --config with an 'outcome_emit' section)"
        )
    config = load_config(config_path)
    if config.outcome_emit is None:
        raise ConfigError(
            f"{config_path} declares no 'outcome_emit' section; "
            "add one (storage: file, path: ...) or pass --file"
        )
    long_running_after = config.outcome_emit.get("long_running_after")
    return config.outcome_emit, long_running_after


def cmd_outcomes_dttr(args: argparse.Namespace) -> int:
    from mycelium.config import ConfigError, MyceliumConfig
    from mycelium.outcome_emit import compute_dttr

    try:
        raw, config_long_running = _outcome_storage_config(args)
        storage = MyceliumConfig._build_outcome_storage(raw)
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    long_running_after = args.long_running_after
    if long_running_after is None:
        long_running_after = config_long_running
    report = compute_dttr(
        storage.list_all(),
        long_running_after=(float(long_running_after) if long_running_after is not None else None),
    )
    if args.json:
        print(json.dumps(report.to_dict(), indent=2, default=str))
        return 0
    print(f"DTTR: {report.dttr:.4f}  (target: 0.0)")
    print(
        f"silent duplicates: {report.silent_duplicates}  "
        f"long-running or redispatched: {report.long_running_or_redispatched}  "
        f"transitions: {report.transitions}"
    )
    for item in report.per_transition:
        marker = " *" if item.long_running_or_redispatched else ""
        print(
            f"  {item.request_id}  {item.tool}  "
            f"execs={item.body_executions}  silent={item.silent_duplicates}  "
            f"resolutions={item.resolution_events}  dur={item.duration_seconds:.1f}s"
            f"{marker}"
        )
    if report.long_running_or_redispatched == 0 and report.transitions > 0:
        print(
            "no long-running or redispatched transitions: DTTR is undefined "
            "(denominator forced to 1)"
        )
    return 0


def _loop_guard_from_args(args: argparse.Namespace) -> Any:
    """Build a LoopGuard from --config / --file operator flags."""
    from mycelium.config import ConfigError, load_config
    from mycelium.loop_guard import FileLoopGuardStorage, LoopGuard

    if getattr(args, "file", None):
        return LoopGuard(FileLoopGuardStorage(args.file))

    config_path = Path(args.config) if args.config else Path("mycelium.yaml")
    if not config_path.is_file():
        raise ConfigError(
            f"no loop_guard storage specified and config not found: {config_path} "
            "(pass --config or --file)"
        )
    config = load_config(config_path)
    if config.loop_guard is None:
        raise ConfigError(f"{config_path} declares no loop_guard section")
    guard = config.build_loop_guard()
    if guard is None:
        raise ConfigError(f"{config_path} declares no loop_guard section")
    storage_type = config.loop_guard.get("storage", "memory")
    if storage_type == "memory":
        raise ConfigError(
            "loop_guard storage is 'memory', which lives inside the agent "
            "process — the CLI cannot reach it. Use --file or configure "
            "loop_guard.storage: file"
        )
    return guard


def _scope_guard_from_args(args: argparse.Namespace) -> Any:
    """Build a ScopeGuard from --config / --file operator flags."""
    from mycelium.config import ConfigError, load_config
    from mycelium.scope_guard import FileScopeGuardStorage, ScopeGrant, ScopeGuard

    if getattr(args, "file", None):
        allowed = [
            s.strip() for s in (getattr(args, "allowed_tools", None) or "").split(",") if s.strip()
        ]
        grant = ScopeGrant(allowed_tools=frozenset(allowed)) if allowed else None
        return ScopeGuard(FileScopeGuardStorage(args.file), default_grant=grant)

    config_path = Path(args.config) if args.config else Path("mycelium.yaml")
    if not config_path.is_file():
        raise ConfigError(
            f"no scope_guard storage specified and config not found: {config_path} "
            "(pass --config or --file)"
        )
    config = load_config(config_path)
    if config.scope_guard is None:
        raise ConfigError(f"{config_path} declares no scope_guard section")
    guard = config.build_scope_guard()
    if guard is None:
        raise ConfigError(f"{config_path} declares no scope_guard section")
    storage_type = config.scope_guard.get("storage", "memory")
    if storage_type == "memory":
        raise ConfigError(
            "scope_guard storage is 'memory', which lives inside the agent "
            "process — the CLI cannot reach it. Use --file or configure "
            "scope_guard.storage: file"
        )
    return guard


def cmd_scope_status(args: argparse.Namespace) -> int:
    from mycelium.config import ConfigError

    try:
        guard = _scope_guard_from_args(args)
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.run_id:
        state = guard.get_state(args.run_id)
        states = [state] if state is not None else []
    else:
        states = guard.storage.list_all()

    if args.json:
        print(json.dumps([s.to_dict() for s in states], indent=2, default=str))
        return 0

    if not states:
        print("(no scope-guard frozen grants)")
        return 0

    for state in states:
        print(f"{state.scope_key}  tools={sorted(state.grant.allowed_tools)}")
    return 0


def cmd_scope_bind(args: argparse.Namespace) -> int:
    from mycelium.config import ConfigError
    from mycelium.scope_guard import ScopeGrant, ScopeWidenRefusedError

    try:
        guard = _scope_guard_from_args(args)
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    allowed = [s.strip() for s in (args.allowed_tools or "").split(",") if s.strip()]
    if allowed:
        grant = ScopeGrant(allowed_tools=frozenset(allowed))
    elif guard.default_grant is not None:
        grant = guard.default_grant
    else:
        print(
            "error: pass --allowed-tools or configure scope_guard.allowed_tools",
            file=sys.stderr,
        )
        return 1

    try:
        state = guard.bind(args.run_id, grant)
    except ScopeWidenRefusedError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(f"bound {state.scope_key}: tools={sorted(state.grant.allowed_tools)}")
    return 0


def _completion_from_args(args: argparse.Namespace) -> Any:
    """Build a CompletionContract from --config / --file operator flags."""
    from mycelium.completion_contract import (
        CompletionContract,
        FileCompletionStorage,
    )
    from mycelium.config import ConfigError, load_config

    if getattr(args, "file", None):
        required = [
            s.strip() for s in (getattr(args, "required", None) or "").split(",") if s.strip()
        ]
        optional = [
            s.strip() for s in (getattr(args, "optional", None) or "").split(",") if s.strip()
        ]
        if not required and not optional:
            raise ConfigError(
                "when using --file without a config, pass --required "
                "and/or --optional (comma-separated ids)"
            )
        return CompletionContract(
            FileCompletionStorage(args.file),
            required=required,
            optional=optional,
        )

    config_path = Path(args.config) if args.config else Path("mycelium.yaml")
    if not config_path.is_file():
        raise ConfigError(
            f"no completion storage specified and config not found: {config_path} "
            "(pass --config or --file)"
        )
    config = load_config(config_path)
    if config.completion is None:
        raise ConfigError(f"{config_path} declares no completion section")
    contract = config.build_completion_contract()
    if contract is None:
        raise ConfigError(f"{config_path} declares no completion section")
    storage_type = config.completion.get("storage", "memory")
    if storage_type == "memory":
        raise ConfigError(
            "completion storage is 'memory', which lives inside the agent "
            "process — the CLI cannot reach it. Use --file or configure "
            "completion.storage: file"
        )
    return contract


def cmd_completion_status(args: argparse.Namespace) -> int:
    from mycelium.config import ConfigError

    try:
        contract = _completion_from_args(args)
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.run_id:
        state = contract.get_state(args.run_id)
        states = [state] if state is not None else []
        if not states:
            # Bind template so status shows pending checklist for a new run id.
            state = contract.bind_run(args.run_id)
            states = [state]
    else:
        states = contract.storage.list_all()

    if args.json:
        print(json.dumps([s.to_dict() for s in states], indent=2, default=str))
        return 0

    if not states:
        print("(no completion-contract runs)")
        return 0

    for state in states:
        pending_r = state.pending_required()
        pending_o = state.pending_optional()
        flags = []
        if pending_r:
            flags.append("REFUSE_IF_TERMINAL")
        elif pending_o:
            flags.append("WARN_OPTIONAL")
        else:
            flags.append("ALLOW")
        flag_s = f" [{' '.join(flags)}]" if flags else ""
        print(f"{state.scope_key}  required={state.required}  optional={state.optional}{flag_s}")
        for sid in state.required + [x for x in state.optional if x not in state.required]:
            mark = state.marks.get(sid)
            if mark is None:
                kind = "required" if sid in state.required else "optional"
                print(f"  {sid}: pending ({kind})")
            else:
                reason = f" reason={mark.reason!r}" if mark.reason else ""
                print(f"  {sid}: {mark.status}{reason}")
        if pending_r:
            print(f"  → mark required ids then retry complete_run / END (pending: {pending_r})")
    return 0


def cmd_completion_mark(args: argparse.Namespace) -> int:
    from mycelium.completion_contract import CompletionMarkError
    from mycelium.config import ConfigError

    try:
        contract = _completion_from_args(args)
        state = contract.mark(
            args.subtask_id,
            args.status,
            reason=args.reason,
            scope_key=args.run_id,
        )
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except CompletionMarkError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    mark = state.marks[args.subtask_id]
    print(
        f"marked {args.subtask_id}={mark.status} on run {state.scope_key}"
        + (f" reason={mark.reason!r}" if mark.reason else "")
    )
    return 0


def cmd_loops_status(args: argparse.Namespace) -> int:
    from mycelium.config import ConfigError

    try:
        guard = _loop_guard_from_args(args)
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.run_id:
        state = guard.get_state(args.run_id)
        states = [state] if state is not None else []
        if not states:
            print(f"no loop-guard state for run {args.run_id!r}", file=sys.stderr)
            return 1
    else:
        states = guard.storage.list_all()
        if args.stuck:
            states = [s for s in states if s.hard_blocked]

    if args.json:
        print(json.dumps([s.to_dict() for s in states], indent=2, default=str))
        return 0

    if not states:
        print("(no loop-guard runs)")
        return 0

    for state in states:
        flags = []
        if state.hard_blocked:
            flags.append("HARD")
        if state.allow_once_hash:
            flags.append("ALLOW_ONCE")
        if state.operator_resolution:
            flags.append(f"released:{state.operator_resolution}")
        flag_s = f" [{' '.join(flags)}]" if flags else ""
        print(
            f"{state.scope_key}  streak={state.streak}  "
            f"last_hash={(state.last_hash or '-')[:12]}  "
            f"soft={len(state.soft_issued)}{flag_s}"
        )
        if state.hard_blocked and state.operator_resolution is None:
            print(
                f"  → mycelium loops release {state.scope_key} "
                f"--verified clear|allow-once|abort-run --by … --reason …"
            )
    return 0


def cmd_loops_release(args: argparse.Namespace) -> int:
    from mycelium.action_ledger import (
        LedgerAlreadyResolvedError,
        LedgerReleaseRefusedError,
    )
    from mycelium.config import ConfigError

    try:
        guard = _loop_guard_from_args(args)
        state = guard.release(
            args.run_id,
            verified=args.verified,
            by=args.by,
            reason=args.reason,
        )
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except (LedgerReleaseRefusedError, LedgerAlreadyResolvedError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(
        f"released run {state.scope_key} --verified {state.operator_resolution} "
        f"by {state.resolved_by}"
    )
    return 0


def _budget_guard_from_args(args: argparse.Namespace) -> Any:
    """Build a BudgetGuard from --config / --file operator flags."""
    from mycelium.budget_guard import BudgetGuard, FileBudgetGuardStorage
    from mycelium.config import ConfigError, load_config

    if getattr(args, "file", None):
        return BudgetGuard(
            FileBudgetGuardStorage(args.file),
            max_steps=10**9,  # CLI inspect/release only; ceilings unused
        )

    config_path = Path(args.config) if args.config else Path("mycelium.yaml")
    if not config_path.is_file():
        raise ConfigError(
            f"no budget storage specified and config not found: {config_path} "
            "(pass --config or --file)"
        )
    config = load_config(config_path)
    if config.budget is None:
        raise ConfigError(f"{config_path} declares no budget section")
    guard = config.build_budget_guard()
    if guard is None:
        raise ConfigError(f"{config_path} declares no budget section")
    storage_type = config.budget.get("storage", "memory")
    if storage_type == "memory":
        raise ConfigError(
            "budget storage is 'memory', which lives inside the agent "
            "process — the CLI cannot reach it. Use --file or configure "
            "budget.storage: file|sqlite|redis|postgres"
        )
    return guard


def cmd_budget_status(args: argparse.Namespace) -> int:
    from mycelium.config import ConfigError

    try:
        guard = _budget_guard_from_args(args)
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.run_id:
        state = guard.get_state(args.run_id)
        states = [state] if state is not None else []
    else:
        states = guard.storage.list_all()

    if args.json:
        payload = []
        for state in states:
            row = state.to_dict()
            row["remaining_budget"] = state.remaining(guard.ceilings).to_dict()
            payload.append(row)
        print(json.dumps(payload, indent=2, default=str))
        return 0

    if not states:
        print("(no budget-guard runs)")
        return 0
    for state in states:
        remaining = state.remaining(guard.ceilings)
        print(
            f"{state.scope_key}  steps={state.steps}  tokens={state.tokens}  "
            f"usd={state.usd:.4f}  hard_blocked={state.hard_blocked}  "
            f"blocked={state.blocked_dimension!r}"
        )
        print(
            f"  remaining_budget: duration={remaining.duration_seconds}  "
            f"steps={remaining.steps}  tokens={remaining.tokens}  "
            f"usd={remaining.usd}"
        )
        if state.hard_blocked and state.operator_resolution is None:
            print(
                f"  → mycelium budget release {state.scope_key} "
                f"--verified clear|allow-once|abort-run --by … --reason …"
            )
    return 0


def cmd_budget_release(args: argparse.Namespace) -> int:
    from mycelium.action_ledger import (
        LedgerAlreadyResolvedError,
        LedgerReleaseRefusedError,
    )
    from mycelium.config import ConfigError

    try:
        guard = _budget_guard_from_args(args)
        state = guard.release(
            args.run_id,
            verified=args.verified,
            by=args.by,
            reason=args.reason,
        )
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except (LedgerReleaseRefusedError, LedgerAlreadyResolvedError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        f"released run {state.scope_key} --verified {state.operator_resolution} "
        f"by {state.resolved_by}"
    )
    return 0


def cmd_doctor(args: argparse.Namespace) -> int:
    """Production-safety verification with narrowly scoped opt-in fixes."""
    import sys

    from mycelium.doctor import exit_code_for_report, run_doctor
    from mycelium.doctor.render import write_report

    if args.fix:
        from mycelium.doctor.fixes import apply_conservative_fixes

        for fix in apply_conservative_fixes(args.config):
            print(f"fixed [{fix.id}] {fix.summary}: {fix.path}", file=sys.stderr)

    report = run_doctor(
        args.config,
        connectivity=not args.no_connectivity,
        timeout_seconds=float(args.timeout),
        verbose=bool(args.verbose),
    )
    write_report(
        report,
        as_json=bool(args.json),
        verbose=bool(args.verbose),
        stream=sys.stdout,
    )
    return exit_code_for_report(report, strict=bool(args.strict))


def cmd_verify(args: argparse.Namespace) -> int:
    """Empirical verification, with an explicitly optional cluster mode."""
    import sys

    if args.verify_attestation:
        from mycelium.verify.cluster import (
            DeploymentAttestation,
            deployment_attestation_is_verified,
        )

        if args.cluster or args.scenario:
            print(
                "error: --verify-attestation cannot be combined with --cluster or --scenario",
                file=sys.stderr,
            )
            return 2
        key_env = str(args.attestation_key_env or "")
        signing_key = os.environ.get(key_env, "") if key_env else ""
        try:
            if not signing_key:
                raise ValueError("--attestation-key-env must name a non-empty environment variable")
            attestation = DeploymentAttestation.from_dict(
                json.loads(args.verify_attestation.read_text(encoding="utf-8"))
            )
            valid = deployment_attestation_is_verified(attestation, signing_key)
        except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
            print(f"error: invalid deployment attestation: {exc}", file=sys.stderr)
            return 2
        payload = {
            "attestation_id": attestation.attestation_id,
            "status": attestation.status,
            "signature_valid": valid,
            "signer_key_id": attestation.signer_key_id,
        }
        print(json.dumps(payload, indent=2, sort_keys=True) if args.json else payload)
        return 0 if valid else 1

    if bool(args.cluster):
        from mycelium.verify.cluster import cluster_exit_code, run_cluster_verify

        if args.scenario:
            print("error: --cluster cannot be combined with --scenario", file=sys.stderr)
            return 2
        result = run_cluster_verify(
            args.config,
            timeout_seconds=float(args.timeout),
            connectivity=not args.no_connectivity,
            keep_artifacts=bool(args.keep_artifacts),
        )
        rendered = json.dumps(result.to_dict(), indent=2, sort_keys=True)
        if args.json:
            print(rendered)
        else:
            print(f"Cluster verification: {result.status}")
            if result.attestation is not None:
                print(f"Attestation: {result.attestation.attestation_id}")
                for check in result.attestation.checks:
                    print(f"  [{check.status}] {check.name}: {check.detail}")
            if result.error:
                print(f"Error: {result.error}", file=sys.stderr)
        if args.attestation_output:
            if result.attestation is None:
                print("error: no attestation was produced", file=sys.stderr)
            else:
                try:
                    args.attestation_output.write_text(
                        json.dumps(result.attestation.to_dict(), indent=2, sort_keys=True) + "\n",
                        encoding="utf-8",
                    )
                except OSError as exc:
                    print(f"error: could not write attestation: {exc}", file=sys.stderr)
                    return 2
        return cluster_exit_code(result)

    from mycelium.verify import exit_code_for_verify, run_verify
    from mycelium.verify.render import write_report

    selected = list(args.scenario or [])
    if not selected:
        print("error: --scenario is required (repeatable, or 'all')", file=sys.stderr)
        return 2
    report = run_verify(
        args.config,
        scenarios=selected,
        timeout_seconds=float(args.timeout),
        rounds=int(args.rounds),
        workers=int(args.workers),
        keep_artifacts=bool(args.keep_artifacts),
        connectivity=not args.no_connectivity,
    )
    write_report(report, as_json=bool(args.json), stream=sys.stdout)
    if args.json and report.isolation_detail and report.refused:
        print(report.isolation_detail, file=sys.stderr)
    return exit_code_for_verify(report, strict=bool(args.strict))


def main(argv: list[str] | None = None) -> int:
    from mycelium.action_ledger import LEDGER_ENTRY_SCHEMA_VERSION

    parser = argparse.ArgumentParser(
        prog="mycelium",
        description="Mycelium runtime: scaffold config and utilities",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    doctor_parser = sub.add_parser(
        "doctor",
        help="Verify production safety configuration and wiring",
        description=(
            "Verification that Mycelium protections are actually "
            "configured and detectably wired — not merely installed. Never "
            "executes application tools, never calls an LLM, never writes "
            "ledger/outcome rows. It is read-only unless --fix is supplied; "
            "fixes are limited to version/schema metadata. "
            "CI gate: mycelium doctor --config mycelium.yaml --strict --json"
        ),
    )
    doctor_parser.add_argument(
        "-c",
        "--config",
        type=Path,
        default=Path("mycelium.yaml"),
        help="Config path (default: ./mycelium.yaml)",
    )
    doctor_parser.add_argument(
        "--json",
        action="store_true",
        help="Emit stable machine-readable JSON for CI",
    )
    doctor_parser.add_argument(
        "--strict",
        action="store_true",
        help="Treat warnings as failures (exit 1)",
    )
    doctor_parser.add_argument(
        "--verbose",
        action="store_true",
        help="Include check ids, evidence kinds, and details",
    )
    doctor_parser.add_argument(
        "--no-connectivity",
        action="store_true",
        help="Skip safe backend connectivity probes",
    )
    doctor_parser.add_argument(
        "--fix",
        action="store_true",
        help="Add only safe config-version and IDE-schema metadata, then verify",
    )
    doctor_parser.add_argument(
        "--timeout",
        type=float,
        default=2.0,
        help="Connectivity probe timeout in seconds (default: 2)",
    )

    verify_parser = sub.add_parser(
        "verify",
        help="Empirically verify guarantees with synthetic or opt-in cluster scenarios",
        description=(
            "Run Doctor, then exercise Mycelium's production guarantees against "
            "the configured storage backend using synthetic operations only. "
            "Normal mode never executes application tools, calls an LLM, or contacts "
            "a provider. Optional --cluster requires an explicitly configured sandbox. "
            "CI: mycelium verify --config mycelium.yaml --scenario all --strict --json"
        ),
    )
    verify_parser.add_argument(
        "-c",
        "--config",
        type=Path,
        default=Path("mycelium.yaml"),
        help="Config path (default: ./mycelium.yaml)",
    )
    verify_parser.add_argument(
        "--scenario",
        action="append",
        default=[],
        metavar="NAME",
        help=(
            "Scenario to run (repeatable): redispatch, contention, "
            "worker-crash, storage-outage, ambiguous-effect, reconcile, "
            "secret-in-args, entity-guard, destructive-confirm, "
            "authority-window, use-time-currency, or all"
        ),
    )
    verify_parser.add_argument(
        "--cluster",
        action="store_true",
        help=(
            "Run the optional two-worker cluster fault test and emit a signed "
            "deployment attestation; requires verify.cluster.enabled: true"
        ),
    )
    verify_parser.add_argument(
        "--attestation-output",
        type=Path,
        help="Also write the signed cluster attestation JSON to this file",
    )
    verify_parser.add_argument(
        "--verify-attestation",
        type=Path,
        metavar="FILE",
        help="Verify a previously produced deployment attestation instead of running tests",
    )
    verify_parser.add_argument(
        "--attestation-key-env",
        metavar="ENV",
        help="Environment variable containing the key for --verify-attestation",
    )
    verify_parser.add_argument(
        "--json",
        action="store_true",
        help="Emit stable machine-readable JSON on stdout (diagnostics on stderr)",
    )
    verify_parser.add_argument(
        "--strict",
        action="store_true",
        help="Treat warnings as failures (exit 1)",
    )
    verify_parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="Per-scenario and subprocess timeout in seconds (default: 30)",
    )
    verify_parser.add_argument(
        "--rounds",
        type=int,
        default=5,
        help="Contention rounds (default: 5)",
    )
    verify_parser.add_argument(
        "--workers",
        type=int,
        default=2,
        help="Contention workers, 2-8 (default: 2)",
    )
    verify_parser.add_argument(
        "--keep-artifacts",
        action="store_true",
        help="Retain namespaced synthetic evidence instead of cleaning up",
    )
    verify_parser.add_argument(
        "--no-connectivity",
        action="store_true",
        help="Skip Doctor's backend connectivity probes",
    )

    init_parser = sub.add_parser(
        "init",
        help="Create mycelium.yaml in the current project",
        description=(
            "Scaffold mycelium.yaml for the wrapper path "
            "(mycelium run / @config.apply / @ledger_sync). Prefer wrappers. "
            "For a custom tool loop that needs explicit claim → execute → complete "
            "(PROCEED/SKIP-style), see the SDK README section "
            "'Manual integration (claim → execute → complete)' — there is no "
            "YAML switch; that API is called from your code."
        ),
    )
    init_parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("mycelium.yaml"),
        help="Output path (default: ./mycelium.yaml)",
    )
    init_mode = init_parser.add_mutually_exclusive_group()
    init_mode.add_argument(
        "--full",
        action="store_true",
        help="Reference template with all guards (not the default on-ramp)",
    )
    init_mode.add_argument(
        "--minimal",
        action="store_true",
        help="Smaller multi-guard scaffold (not the default on-ramp)",
    )
    init_mode.add_argument(
        "--detect",
        action="store_true",
        help="Inspect this project and create a conservative tailored starter",
    )
    init_parser.add_argument(
        "--project",
        type=Path,
        default=Path("."),
        help="Project directory scanned by --detect (default: current directory)",
    )
    init_parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite an existing file",
    )

    config_parser = sub.add_parser(
        "config",
        help="Inspect the versioned mycelium.yaml configuration contract",
    )
    config_sub = config_parser.add_subparsers(dest="config_command", required=True)
    config_schema_parser = config_sub.add_parser(
        "schema",
        help="Print JSON Schema for IDEs, agents, and CI",
    )
    config_schema_parser.add_argument(
        "-o",
        "--output",
        type=Path,
        help="Write the schema to a file instead of stdout",
    )
    config_docs_parser = config_sub.add_parser(
        "docs",
        help="Print Markdown reference generated from the typed model",
    )
    config_docs_parser.add_argument(
        "-o",
        "--output",
        type=Path,
        help="Write Markdown to a file instead of stdout",
    )
    config_example_parser = config_sub.add_parser(
        "example",
        help="Print a model-validated example configuration",
    )
    config_example_parser.add_argument(
        "-o",
        "--output",
        type=Path,
        help="Write YAML to a file instead of stdout",
    )

    demo_parser = sub.add_parser(
        "demo",
        help="Feature tour: without/with Mycelium + gates, repair, reconcile, release",
    )
    demo_parser.add_argument(
        "--redis",
        action="store_true",
        help=(
            "Also run the two-worker real-Redis Cloud-style redispatch proof "
            "(requires Redis; MYCELIUM_TEST_REDIS_URL or localhost db 15)"
        ),
    )
    demo_parser.add_argument(
        "--slow",
        action="store_true",
        help="Pause between lines/sections for screen recording (~30–40s total)",
    )
    run_parser = sub.add_parser(
        "run",
        help="Run a Python command with YAML callables auto-instrumented",
    )
    run_parser.add_argument(
        "-c",
        "--config",
        type=Path,
        default=Path("mycelium.yaml"),
        help="Config path (default: ./mycelium.yaml)",
    )
    run_parser.add_argument(
        "child_command",
        nargs=argparse.REMAINDER,
        help="Python command after '--'",
    )

    migrate_parser = sub.add_parser(
        "migrate",
        help="Plan or apply ActionLedger schema migrations",
        description=(
            "Upgrade durable ActionLedger rows using explicit version-to-version rules. "
            "Stop workers and back up the ledger before --apply."
        ),
    )
    _add_operator_storage_args(migrate_parser)
    migrate_mode = migrate_parser.add_mutually_exclusive_group(required=True)
    migrate_mode.add_argument(
        "--plan",
        action="store_true",
        help="Inspect versions and show changes without modifying ledger rows",
    )
    migrate_mode.add_argument(
        "--apply",
        action="store_true",
        help="Apply the migration (back up the ledger and stop workers first)",
    )
    migrate_parser.add_argument(
        "--target-version",
        type=int,
        default=LEDGER_ENTRY_SCHEMA_VERSION,
        help=(
            f"Target ledger schema version (default: current schema {LEDGER_ENTRY_SCHEMA_VERSION})"
        ),
    )
    migrate_parser.add_argument(
        "--allow-active",
        action="store_true",
        help="Allow IN_FLIGHT rows after workers are confirmed stopped",
    )
    migrate_parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable migration plan or result",
    )

    state_parser = sub.add_parser(
        "state",
        help="Manage the unified guard state backend",
    )
    state_sub = state_parser.add_subparsers(dest="state_command", required=True)
    state_migrate_parser = state_sub.add_parser(
        "migrate",
        help="Copy legacy guard state into state_backend",
        description=(
            "Copy loop, scope, completion, state-flush, and audit-receipt records "
            "without deleting or overwriting their source records."
        ),
    )
    state_migrate_parser.add_argument(
        "-c",
        "--config",
        type=Path,
        default=Path("mycelium.yaml"),
        help="Config containing source feature storage and destination state_backend",
    )
    state_migrate_mode = state_migrate_parser.add_mutually_exclusive_group(required=True)
    state_migrate_mode.add_argument(
        "--plan",
        action="store_true",
        help="Read-only preview of records to copy",
    )
    state_migrate_mode.add_argument(
        "--apply",
        action="store_true",
        help="Copy records without deleting source state",
    )
    state_migrate_parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable output",
    )

    providers_parser = sub.add_parser(
        "providers",
        help="Verify shipped read-only provider reconciliation adapters",
    )
    providers_sub = providers_parser.add_subparsers(dest="providers_command", required=True)
    providers_verify_parser = providers_sub.add_parser(
        "verify",
        help="Run adversarial conformance tests and sign the report",
        description=(
            "Run synthetic lag, ambiguity, duplicate, malformed-handle, and "
            "forbidden-write checks. This does not contact the live provider."
        ),
    )
    providers_verify_parser.add_argument(
        "adapter",
        choices=("gmail",),
        help="Shipped provider adapter to verify",
    )
    providers_verify_parser.add_argument(
        "--signing-key-env",
        default=_ENV_ADAPTER_REPORT_SIGNING_KEY,
        help=(
            "Environment variable containing the HMAC signing key "
            f"(default: {_ENV_ADAPTER_REPORT_SIGNING_KEY})"
        ),
    )
    providers_verify_parser.add_argument(
        "--key-id",
        default=None,
        help="Non-secret signer key identifier stored in the report",
    )
    providers_verify_parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        help="Write the signed JSON report to this path (default: stdout)",
    )

    providers_report_parser = providers_sub.add_parser(
        "verify-report",
        help="Verify a signed adapter report",
    )
    providers_report_parser.add_argument("report", type=Path)
    providers_report_parser.add_argument(
        "--signing-key-env",
        default=_ENV_ADAPTER_REPORT_SIGNING_KEY,
        help=(
            "Environment variable containing the HMAC verification key "
            f"(default: {_ENV_ADAPTER_REPORT_SIGNING_KEY})"
        ),
    )
    providers_report_parser.add_argument(
        "--json", action="store_true", help="Emit machine-readable verification"
    )

    transitions_parser = sub.add_parser(
        "transitions",
        help="Operator triage and release of stuck (hard-blocked) transitions",
    )
    transitions_sub = transitions_parser.add_subparsers(dest="transitions_command", required=True)

    list_parser = transitions_sub.add_parser(
        "list", help="List ledger transitions (optionally only stuck ones)"
    )
    _add_operator_storage_args(list_parser)
    list_parser.add_argument(
        "--stuck",
        action="store_true",
        help="Only transitions that need operator attention, with next-action hints",
    )
    list_parser.add_argument("--tool", default=None, help="Filter by tool name")
    list_parser.add_argument(
        "--outcome",
        choices=[item.value for item in TerminalOutcome],
        default=None,
        help="Filter by resolved terminal outcome",
    )
    list_parser.add_argument("--limit", type=int, default=None, help="Return one bounded page")
    list_parser.add_argument("--cursor", default=None, help="Opaque cursor from a prior page")
    list_parser.add_argument(
        "--parent",
        default=None,
        metavar="REQUEST_ID",
        help="Only children of this handoff parent request_id",
    )
    list_parser.add_argument("--json", action="store_true", help="Machine-readable JSON output")

    export_parser = transitions_sub.add_parser(
        "export", help="Export transitions as sanitized newline-delimited JSON"
    )
    _add_operator_storage_args(export_parser)
    export_parser.add_argument("--output", required=True)
    export_parser.add_argument("--tool", default=None)
    export_parser.add_argument("--outcome", choices=[item.value for item in TerminalOutcome])
    export_parser.add_argument("--page-size", type=int, default=500)

    prune_parser = transitions_sub.add_parser(
        "prune", help="Apply the transition retention policy (dry-run by default)"
    )
    _add_operator_storage_args(prune_parser)
    prune_parser.add_argument(
        "--older-than",
        default=None,
        metavar="DURATION",
        help="Override retention age (for example 30d)",
    )
    prune_parser.add_argument(
        "--outcome",
        action="append",
        default=None,
        choices=[item.value for item in TerminalOutcome],
        help="Eligible stored outcome; repeat to select multiple",
    )
    prune_parser.add_argument(
        "--archive", default=None, help="Write candidates to NDJSON before deletion"
    )
    prune_parser.add_argument("--page-size", type=int, default=500)
    prune_mode = prune_parser.add_mutually_exclusive_group()
    prune_mode.add_argument("--dry-run", action="store_true", help="Preview only (the default)")
    prune_mode.add_argument(
        "--execute", action="store_true", help="Permanently delete eligible rows"
    )

    show_parser = transitions_sub.add_parser(
        "show", help="Show one transition with provider-verification fields"
    )
    _add_operator_storage_args(show_parser)
    show_parser.add_argument("request_id")

    release_parser = transitions_sub.add_parser(
        "release",
        help="Record a human verification releasing a hard-blocked transition",
    )
    _add_operator_storage_args(release_parser)
    release_parser.add_argument("request_id")
    release_parser.add_argument(
        "--verified",
        required=True,
        choices=["completed", "not-executed"],
        help="completed: effect happened (supply --result-json); "
        "not-executed: effect provably never ran (grants one re-execution)",
    )
    release_parser.add_argument(
        "--result-json",
        default=None,
        help="JSON result recorded for --verified completed",
    )
    release_parser.add_argument("--by", required=True, help="Operator identity (audit stamp)")
    release_parser.add_argument("--reason", required=True, help="Why the release is justified")

    mark_dead_parser = transitions_sub.add_parser(
        "mark-dead",
        help="Assert a worker is dead so reclaim can proceed",
    )
    _add_operator_storage_args(mark_dead_parser)
    mark_dead_parser.add_argument("request_id")
    mark_dead_parser.add_argument("--by", required=True, help="Operator identity (audit stamp)")
    mark_dead_parser.add_argument("--reason", required=True, help="Why the worker is believed dead")
    mark_dead_parser.add_argument(
        "--override-heartbeat",
        action="store_true",
        default=False,
        help="Skip liveness check (use only when operator has direct evidence "
        "of death — bypass may cause a duplicate effect if worker is alive)",
    )

    loops_parser = sub.add_parser(
        "loops",
        help="Operator triage and release of AF-003 loop-guard hard-blocks",
    )
    loops_sub = loops_parser.add_subparsers(dest="loops_command", required=True)

    loops_status = loops_sub.add_parser("status", help="Show loop-guard state for runs")
    loops_status.add_argument(
        "run_id",
        nargs="?",
        default=None,
        help="Optional run_id / scope key (default: list all)",
    )
    loops_status.add_argument(
        "-c",
        "--config",
        type=Path,
        default=Path("mycelium.yaml"),
        help="Config path with loop_guard: (default: ./mycelium.yaml)",
    )
    loops_status.add_argument(
        "--file",
        type=Path,
        default=None,
        help="Direct path to loop_guard JSON file storage",
    )
    loops_status.add_argument(
        "--stuck",
        action="store_true",
        help="Only hard-blocked runs",
    )
    loops_status.add_argument("--json", action="store_true", help="Machine-readable JSON output")

    loops_release = loops_sub.add_parser(
        "release",
        help="Record a human verification releasing a loop-guard hard-block",
    )
    loops_release.add_argument("run_id")
    loops_release.add_argument(
        "-c",
        "--config",
        type=Path,
        default=Path("mycelium.yaml"),
        help="Config path with loop_guard: (default: ./mycelium.yaml)",
    )
    loops_release.add_argument(
        "--file",
        type=Path,
        default=None,
        help="Direct path to loop_guard JSON file storage",
    )
    loops_release.add_argument(
        "--verified",
        required=True,
        choices=["clear", "allow-once", "abort-run"],
        help="clear: wipe counters; allow-once: one matching action; abort-run: keep frozen",
    )
    loops_release.add_argument("--by", required=True, help="Operator identity (audit stamp)")
    loops_release.add_argument("--reason", required=True, help="Why the release is justified")

    completion_parser = sub.add_parser(
        "completion",
        help="AF-007 completion contract: status and mark subtasks before terminal",
    )
    completion_sub = completion_parser.add_subparsers(dest="completion_command", required=True)

    completion_status = completion_sub.add_parser(
        "status", help="Show completion-contract checklist for runs"
    )
    completion_status.add_argument(
        "run_id",
        nargs="?",
        default=None,
        help="Optional run_id / scope key (default: list all)",
    )
    completion_status.add_argument(
        "-c",
        "--config",
        type=Path,
        default=Path("mycelium.yaml"),
        help="Config path with completion: (default: ./mycelium.yaml)",
    )
    completion_status.add_argument(
        "--file",
        type=Path,
        default=None,
        help="Direct path to completion JSON file storage",
    )
    completion_status.add_argument(
        "--required",
        default="",
        help="Comma-separated required ids (with --file, no config)",
    )
    completion_status.add_argument(
        "--optional",
        default="",
        help="Comma-separated optional ids (with --file, no config)",
    )
    completion_status.add_argument(
        "--json", action="store_true", help="Machine-readable JSON output"
    )

    completion_mark = completion_sub.add_parser(
        "mark",
        help="Mark a subtask success|failed|abandoned for a run",
    )
    completion_mark.add_argument("run_id", help="run_id / scope key")
    completion_mark.add_argument("subtask_id", help="Checklist id to mark")
    completion_mark.add_argument(
        "--status",
        required=True,
        choices=["success", "failed", "abandoned"],
        help="Resolution status (abandoned requires --reason)",
    )
    completion_mark.add_argument(
        "--reason",
        default=None,
        help="Required when --status abandoned",
    )
    completion_mark.add_argument(
        "-c",
        "--config",
        type=Path,
        default=Path("mycelium.yaml"),
        help="Config path with completion: (default: ./mycelium.yaml)",
    )
    completion_mark.add_argument(
        "--file",
        type=Path,
        default=None,
        help="Direct path to completion JSON file storage",
    )
    completion_mark.add_argument(
        "--required",
        default="",
        help="Comma-separated required ids (with --file, no config)",
    )
    completion_mark.add_argument(
        "--optional",
        default="",
        help="Comma-separated optional ids (with --file, no config)",
    )

    budget_parser = sub.add_parser(
        "budget",
        help="Budget guard: status and release for cost/time/step ceilings",
    )
    budget_sub = budget_parser.add_subparsers(dest="budget_command", required=True)

    budget_status = budget_sub.add_parser(
        "status", help="Show remaining budget and hard-block state for runs"
    )
    budget_status.add_argument(
        "run_id",
        nargs="?",
        default=None,
        help="Optional run_id / scope key (default: list all)",
    )
    budget_status.add_argument(
        "-c",
        "--config",
        type=Path,
        default=Path("mycelium.yaml"),
        help="Config path with budget: (default: ./mycelium.yaml)",
    )
    budget_status.add_argument(
        "--file",
        type=Path,
        default=None,
        help="Direct path to budget JSON file storage",
    )
    budget_status.add_argument("--json", action="store_true", help="Machine-readable JSON output")

    budget_release = budget_sub.add_parser(
        "release",
        help="Operator release for a hard-blocked budget run",
    )
    budget_release.add_argument("run_id")
    budget_release.add_argument(
        "--verified",
        required=True,
        choices=["clear", "allow-once", "abort-run"],
        help="clear resets meters; allow-once permits one overage step; abort-run keeps blocked",
    )
    budget_release.add_argument(
        "--by",
        required=True,
        help="Operator identity (audit stamp, not authentication)",
    )
    budget_release.add_argument(
        "--reason",
        required=True,
        help="Why this overage / abort is authorized",
    )
    budget_release.add_argument(
        "-c",
        "--config",
        type=Path,
        default=Path("mycelium.yaml"),
        help="Config path with budget: (default: ./mycelium.yaml)",
    )
    budget_release.add_argument(
        "--file",
        type=Path,
        default=None,
        help="Direct path to budget JSON file storage",
    )

    scope_parser = sub.add_parser(
        "scope",
        help="AF-008 scope-escalation guard: inspect or bind frozen allowlists",
    )
    scope_sub = scope_parser.add_subparsers(dest="scope_command", required=True)

    scope_status = scope_sub.add_parser("status", help="Show frozen tool allowlists for runs")
    scope_status.add_argument(
        "run_id",
        nargs="?",
        default=None,
        help="Optional run_id / thread_id scope key",
    )
    scope_status.add_argument(
        "-c",
        "--config",
        type=Path,
        default=Path("mycelium.yaml"),
        help="Config path with scope_guard: (default: ./mycelium.yaml)",
    )
    scope_status.add_argument(
        "--file",
        type=Path,
        default=None,
        help="Direct path to scope-guard JSON file storage",
    )
    scope_status.add_argument(
        "--allowed-tools",
        default="",
        help="Comma-separated tools (with --file, optional default grant)",
    )
    scope_status.add_argument("--json", action="store_true", help="Machine-readable JSON output")

    scope_bind = scope_sub.add_parser(
        "bind", help="Freeze (or narrow) a tool allowlist for a run_id"
    )
    scope_bind.add_argument("run_id", help="run_id / thread_id to freeze")
    scope_bind.add_argument(
        "--allowed-tools",
        default="",
        help="Comma-separated tool allowlist (default: config grant)",
    )
    scope_bind.add_argument(
        "-c",
        "--config",
        type=Path,
        default=Path("mycelium.yaml"),
        help="Config path with scope_guard: (default: ./mycelium.yaml)",
    )
    scope_bind.add_argument(
        "--file",
        type=Path,
        default=None,
        help="Direct path to scope-guard JSON file storage",
    )

    outcomes_parser = sub.add_parser(
        "outcomes",
        help="Outcome telemetry: compute DTTR over emitted resolution rows",
    )
    outcomes_sub = outcomes_parser.add_subparsers(dest="outcomes_command", required=True)

    dttr_parser = outcomes_sub.add_parser(
        "dttr",
        help="Compute the Duplicate Tool Transition Rate (DTTR) over an outcome log; target is 0.0",
    )
    dttr_parser.add_argument(
        "-c",
        "--config",
        type=Path,
        default=None,
        help="mycelium.yaml to read the outcome_emit storage from (default: ./mycelium.yaml)",
    )
    dttr_parser.add_argument(
        "--file",
        dest="outcome_file",
        type=Path,
        default=None,
        help=f"NDJSON outcome log path (or ${_ENV_OUTCOME_FILE}); overrides --config",
    )
    dttr_parser.add_argument(
        "--long-running-after",
        dest="long_running_after",
        type=float,
        default=None,
        help="seconds; transitions older than this count as long-running "
        "(default: outcome_emit.long_running_after, else disabled)",
    )
    dttr_parser.add_argument("--json", action="store_true", help="Machine-readable JSON output")

    args = parser.parse_args(argv)
    if args.command == "init":
        return cmd_init(
            args.output,
            full=args.full,
            minimal=args.minimal,
            force=args.force,
            detect=args.detect,
            project=args.project,
        )
    if args.command == "demo":
        return cmd_demo(redis=args.redis, slow=args.slow)
    if args.command == "run":
        return cmd_run(args.config, args.child_command)
    if args.command == "migrate":
        return cmd_migrate(args)
    if args.command == "config":
        if args.config_command == "schema":
            return cmd_config_schema(args.output)
        if args.config_command == "docs":
            return cmd_config_docs(args.output)
        if args.config_command == "example":
            return cmd_config_example(args.output)
    if args.command == "state":
        if args.state_command == "migrate":
            return cmd_state_migrate(args)
    if args.command == "providers":
        if args.providers_command == "verify":
            return cmd_providers_verify(args)
        if args.providers_command == "verify-report":
            return cmd_providers_verify_report(args)
    if args.command == "transitions":
        if args.transitions_command == "list":
            return cmd_transitions_list(args)
        if args.transitions_command == "show":
            return cmd_transitions_show(args)
        if args.transitions_command == "export":
            return cmd_transitions_export(args)
        if args.transitions_command == "prune":
            return cmd_transitions_prune(args)
        if args.transitions_command == "release":
            return cmd_transitions_release(args)
        if args.transitions_command == "mark-dead":
            return cmd_transitions_mark_dead(args)
    if args.command == "loops":
        if args.loops_command == "status":
            return cmd_loops_status(args)
        if args.loops_command == "release":
            return cmd_loops_release(args)
    if args.command == "budget":
        if args.budget_command == "status":
            return cmd_budget_status(args)
        if args.budget_command == "release":
            return cmd_budget_release(args)
    if args.command == "completion":
        if args.completion_command == "status":
            return cmd_completion_status(args)
        if args.completion_command == "mark":
            return cmd_completion_mark(args)
    if args.command == "scope":
        if args.scope_command == "status":
            return cmd_scope_status(args)
        if args.scope_command == "bind":
            return cmd_scope_bind(args)
    if args.command == "outcomes":
        if args.outcomes_command == "dttr":
            return cmd_outcomes_dttr(args)
    if args.command == "doctor":
        return cmd_doctor(args)
    if args.command == "verify":
        return cmd_verify(args)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
