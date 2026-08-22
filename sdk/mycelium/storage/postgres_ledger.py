"""Postgres-backed ledger storage with INSERT ... ON CONFLICT claim semantics."""

from __future__ import annotations

import json
import re
import time
from collections.abc import Callable
from typing import Any, TypeVar

from mycelium.storage._helpers import ClaimOutcome, claim_inflight_outcome, with_lease

E = TypeVar("E")

_TABLE_RE = re.compile(r"^[a-z][a-z0-9_]*$")


def _validate_table_name(table: str) -> str:
    if not _TABLE_RE.fullmatch(table):
        raise ValueError(
            f"invalid Postgres table name {table!r}; use lowercase letters, digits, underscores"
        )
    return table


def _require_psycopg() -> Any:
    try:
        import psycopg
        from psycopg import sql
    except ImportError as exc:
        raise ImportError(
            "Postgres storage requires the 'psycopg' package. "
            "Install with: pip install 'mycelium-runtime[postgres]'"
        ) from exc
    return psycopg, sql


def _payload_dict(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    return dict(json.loads(raw))


class PostgresEntryStorage:
    """Generic Postgres table store for ledger entries keyed by request_id."""

    def __init__(
        self,
        dsn: str,
        *,
        table: str,
        from_dict: Callable[[dict[str, Any]], E],
    ) -> None:
        psycopg, sql = _require_psycopg()
        self._psycopg = psycopg
        self._sql = sql
        self._dsn = dsn
        self._table = _validate_table_name(table)
        self._from_dict = from_dict
        self._schema_ready = False

    def _table_id(self) -> Any:
        return self._sql.Identifier(self._table)

    def _ensure_schema(self) -> None:
        if self._schema_ready:
            return
        query = self._sql.SQL(
            "CREATE TABLE IF NOT EXISTS {} (request_id TEXT PRIMARY KEY, payload JSONB NOT NULL)"
        ).format(self._table_id())
        effect_index = self._sql.SQL(
            "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} "
            "((COALESCE(payload->>'effect_id', request_id)))"
        ).format(
            self._sql.Identifier(f"{self._table}_effect_id_idx"),
            self._table_id(),
        )
        with self._psycopg.connect(self._dsn) as conn:
            conn.execute(query)
            conn.execute(effect_index)
            conn.commit()
        self._schema_ready = True

    def get(self, request_id: str) -> E | None:
        self._ensure_schema()
        query = self._sql.SQL("SELECT payload FROM {} WHERE request_id = %s").format(
            self._table_id()
        )
        with self._psycopg.connect(self._dsn) as conn:
            row = conn.execute(query, (request_id,)).fetchone()
        if row is None:
            return None
        return self._from_dict(_payload_dict(row[0]))

    def set(self, entry: E) -> None:
        self._ensure_schema()
        payload = json.loads(json.dumps(entry.to_dict(), default=str))
        query = self._sql.SQL(
            "INSERT INTO {} (request_id, payload) VALUES (%s, %s::jsonb) "
            "ON CONFLICT (request_id) DO UPDATE SET payload = EXCLUDED.payload"
        ).format(self._table_id())
        with self._psycopg.connect(self._dsn) as conn:
            conn.execute(query, (entry.request_id, json.dumps(payload)))
            conn.commit()

    def get_by_effect_id(self, effect_id: str) -> E | None:
        self._ensure_schema()
        query = self._sql.SQL(
            "SELECT payload FROM {} WHERE COALESCE(payload->>'effect_id', request_id) = %s"
        ).format(self._table_id())
        with self._psycopg.connect(self._dsn) as conn:
            row = conn.execute(query, (effect_id,)).fetchone()
        if row is None:
            return None
        return self._from_dict(_payload_dict(row[0]))

    def try_claim_inflight(
        self,
        entry: E,
        *,
        lease_ttl: float = 3600.0,
    ) -> tuple[ClaimOutcome, E | None]:
        self._ensure_schema()
        now = time.time()
        fresh = with_lease(entry, now=now, lease_ttl=lease_ttl)
        payload = json.loads(json.dumps(fresh.to_dict(), default=str))
        insert_query = self._sql.SQL(
            "INSERT INTO {} (request_id, payload) VALUES (%s, %s::jsonb) "
            "ON CONFLICT (request_id) DO NOTHING RETURNING request_id"
        ).format(self._table_id())
        select_by_effect_for_update = self._sql.SQL(
            "SELECT request_id, payload FROM {} "
            "WHERE COALESCE(payload->>'effect_id', request_id) = %s FOR UPDATE"
        ).format(self._table_id())
        select_for_update = self._sql.SQL(
            "SELECT payload FROM {} WHERE request_id = %s FOR UPDATE"
        ).format(self._table_id())
        update_reclaim = self._sql.SQL(
            "UPDATE {} SET payload = %s::jsonb WHERE request_id = %s RETURNING request_id"
        ).format(self._table_id())

        with self._psycopg.connect(self._dsn) as conn:
            with conn.transaction():
                inserted = conn.execute(
                    insert_query,
                    (entry.request_id, json.dumps(payload)),
                ).fetchone()
                if inserted is not None:
                    return "claimed", None

                effect_id = str(getattr(entry, "effect_id", "") or "")
                if effect_id:
                    effect_row = conn.execute(
                        select_by_effect_for_update,
                        (effect_id,),
                    ).fetchone()
                    if effect_row is not None:
                        canonical = self._from_dict(_payload_dict(effect_row[1]))
                        # Same request_id: fall through to the request-keyed
                        # reclaim path (EXPIRED/FAILED must be reclaimable).
                        if canonical.request_id != entry.request_id:
                            outcome = claim_inflight_outcome(canonical, now=now)
                            if outcome == "completed":
                                return "completed", canonical
                            return "in_flight", canonical

                row = conn.execute(select_for_update, (entry.request_id,)).fetchone()
                if row is None:
                    return "claimed", None

                existing = self._from_dict(_payload_dict(row[0]))
                outcome = claim_inflight_outcome(existing, now=now)
                if outcome == "completed":
                    return "completed", existing
                if outcome == "in_flight":
                    return "in_flight", existing

                reclaim_entry = with_lease(entry, now=now, lease_ttl=lease_ttl, prior=existing)
                reclaim_payload = json.loads(json.dumps(reclaim_entry.to_dict(), default=str))
                reclaimed = conn.execute(
                    update_reclaim,
                    (json.dumps(reclaim_payload), entry.request_id),
                ).fetchone()
                if reclaimed is not None:
                    return "claimed", None
                return "in_flight", existing

    def try_transition(
        self,
        entry: E,
        *,
        expected_terminal_outcomes: frozenset[str],
        expected_owner: str | None = None,
        require_lease_held_at: float | None = None,
        expected_fence: int | None = None,
        expected_effect_state: str | None = None,
    ) -> bool:
        self._ensure_schema()
        table = self._table_id()
        payload = json.loads(json.dumps(entry.to_dict(), default=str))
        # Build WHERE clause: terminal_outcome IN (...) [AND owner = ...]
        # [AND fence matches] [AND lease held or unbounded]
        extra_clauses = self._sql.SQL("")
        params: list[Any] = [
            json.dumps(payload),
            entry.request_id,
            list(expected_terminal_outcomes),
        ]
        if expected_owner is not None:
            extra_clauses = self._sql.SQL("{} AND payload->>'owner' = %s").format(extra_clauses)
            params.append(expected_owner)
        if expected_fence is not None:
            # COALESCE so old rows (payload without a fence) read as 0.
            extra_clauses = self._sql.SQL(
                "{} AND COALESCE((payload->>'fence')::bigint, 0) = %s"
            ).format(extra_clauses)
            params.append(expected_fence)
        if expected_effect_state is not None:
            extra_clauses = self._sql.SQL(
                "{} AND COALESCE(payload->>'effect_phase', 'INTENDED') = %s"
            ).format(extra_clauses)
            params.append(expected_effect_state)
        if require_lease_held_at is not None:
            # NULL lease_until = unbounded; else must still be in the future.
            extra_clauses = self._sql.SQL(
                "{} AND (payload->>'lease_until' IS NULL "
                "OR (payload->>'lease_until')::double precision > %s)"
            ).format(extra_clauses)
            params.append(require_lease_held_at)
        query = self._sql.SQL(
            "UPDATE {} SET payload = %s::jsonb "
            "WHERE request_id = %s "
            "AND payload->>'terminal_outcome' = ANY(%s) {} "
            "RETURNING request_id"
        ).format(table, extra_clauses)
        with self._psycopg.connect(self._dsn) as conn:
            row = conn.execute(query, tuple(params)).fetchone()
            conn.commit()
        return row is not None

    def list_all(self) -> list[E]:
        self._ensure_schema()
        query = self._sql.SQL("SELECT payload FROM {}").format(self._table_id())
        with self._psycopg.connect(self._dsn) as conn:
            rows = conn.execute(query).fetchall()
        return [self._from_dict(_payload_dict(row[0])) for row in rows]


class PostgresLedgerStorage:
    """Postgres storage for :class:`~mycelium.action_ledger.LedgerEntry`."""

    def __init__(
        self,
        dsn: str,
        *,
        table: str = "mycelium_action_ledger",
    ) -> None:
        from mycelium.action_ledger import LedgerEntry

        self._inner = PostgresEntryStorage(
            dsn,
            table=table,
            from_dict=LedgerEntry.from_dict,
        )

    def get(self, request_id: str) -> Any:
        return self._inner.get(request_id)

    def set(self, entry: Any) -> None:
        self._inner.set(entry)

    def get_by_effect_id(self, effect_id: str) -> Any:
        return self._inner.get_by_effect_id(effect_id)

    def try_claim_inflight(
        self,
        entry: Any,
        *,
        lease_ttl: float = 3600.0,
    ) -> tuple[ClaimOutcome, Any | None]:
        return self._inner.try_claim_inflight(entry, lease_ttl=lease_ttl)

    def list_all(self) -> list[Any]:
        return self._inner.list_all()

    def try_transition(
        self,
        entry: Any,
        *,
        expected_terminal_outcomes: frozenset[str],
        expected_owner: str | None = None,
        require_lease_held_at: float | None = None,
        expected_fence: int | None = None,
        expected_effect_state: str | None = None,
    ) -> bool:
        return self._inner.try_transition(
            entry,
            expected_terminal_outcomes=expected_terminal_outcomes,
            expected_owner=expected_owner,
            require_lease_held_at=require_lease_held_at,
            expected_fence=expected_fence,
            expected_effect_state=expected_effect_state,
        )


class PostgresTaskLedgerStorage:
    """Postgres storage for :class:`~mycelium.task_ledger.TaskLedgerEntry`."""

    def __init__(
        self,
        dsn: str,
        *,
        table: str = "mycelium_task_ledger",
    ) -> None:
        from mycelium.task_ledger import TaskLedgerEntry

        self._inner = PostgresEntryStorage(
            dsn,
            table=table,
            from_dict=TaskLedgerEntry.from_dict,
        )

    def get(self, request_id: str) -> Any:
        return self._inner.get(request_id)

    def set(self, entry: Any) -> None:
        self._inner.set(entry)

    def try_claim_inflight(
        self,
        entry: Any,
        *,
        lease_ttl: float = 3600.0,
    ) -> tuple[ClaimOutcome, Any | None]:
        return self._inner.try_claim_inflight(entry, lease_ttl=lease_ttl)

    def list_all(self) -> list[Any]:
        return self._inner.list_all()
