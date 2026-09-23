#!/usr/bin/env python3
"""Build fail-closed, independently deduplicated FX edge evidence.

This module deliberately adds no strategy families.  It evaluates the existing
forecast surface at the canonical unit requested by the source audit:

    family x pair x horizon x UTC-session x executable-spread bucket

Raw observations are retained, while effective observations collapse continuous
market episodes that share a signed currency factor.  The generated allocator
is counterfactual and shadow-only; it cannot submit an order.
"""

from __future__ import annotations

import argparse
import gc
import hashlib
import heapq
import json
import math
import sqlite3
import statistics
import tempfile
import weakref
from array import array
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator

try:
    from oanda_news_event_reaction_model import surprise_coverage
    from oanda_proof_cohort_registry import ensure_cohorts
    from oanda_prospective_governance import build_governance
    from oanda_strategy_archetypes import strategy_archetype
except ModuleNotFoundError:
    from trad.oanda_news_event_reaction_model import surprise_coverage
    from trad.oanda_proof_cohort_registry import ensure_cohorts
    from trad.oanda_prospective_governance import build_governance
    from trad.oanda_strategy_archetypes import strategy_archetype


ROOT = Path(__file__).resolve().parent
STATE = ROOT / "data" / "oanda_training_manager" / "state"
REPORTS = ROOT / "data" / "oanda_training_manager" / "reports" / "edge_evidence"
DEFAULT_SOURCE = STATE / "strategy_shadow_outcomes_v1.sqlite"
DEFAULT_DATABASE = STATE / "edge_evidence_v1.sqlite"
DEFAULT_JSON = STATE / "edge_evidence_v1.json"
DEFAULT_MARKDOWN = REPORTS / "EDGE_EVIDENCE_CURRENT.md"
DEFAULT_REGISTRY = ROOT / "docs" / "BACKTEST_AND_MODEL_REGISTRY.md"
DEFAULT_NEWS_DATABASE = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "local_news_sentiment"
    / "local_news_sentiment_v1.sqlite"
)
DEFAULT_MACRO_SURPRISE_STATE = STATE / "macro_surprise_v1.json"
DEFAULT_MACRO_SURPRISE_DATABASE = STATE / "macro_surprise_v1.sqlite"
DEFAULT_MODEL_GAP_ROADMAP = ROOT / "config" / "modern_model_gap_roadmap.json"
DEFAULT_GOVERNANCE_CONFIG = ROOT / "config" / "prospective_replication_governance_v1.json"
DEFAULT_COHORT_STATE = STATE / "proof_cohort_registry_v1.json"
DEFAULT_CANDIDATE_COHORT_DATABASE = STATE / "candidate_cohort_registry_v1.sqlite"
DEFAULT_CANDIDATE_COHORT_STATE = STATE / "candidate_cohort_registry_v1.json"

PRIMARY_SEED = "volatility_squeeze_breakout"
CONDITIONAL_COMPONENTS = (
    "cusum_breakout",
    "session_range_breakout",
    "inverse_correlation_veto",
    "ema_trend_cross",
)
SMALL_POSITIVE_CANDIDATES = ("cci_reversion", "supervised_return_rank")

ProgressCallback = Callable[[str, dict[str, Any]], None]


_SPOOL_COLUMNS = (
    "spool_id", "row_id", "event_id", "horizon_sec", "entry_time",
    "entry_epoch", "entry_day", "entry_week", "family", "instrument",
    "direction", "executable_pips", "net_pips", "spread_pips", "session",
    "liquidity", "max_favorable_pips", "max_adverse_pips",
    "first_positive_sec", "configured_stop_hit_sec",
    "configured_target_hit_sec", "archetype", "entry_minute",
)


class SpilledRows:
    """Replayable, disk-backed outcome rows used by the production-sized build.

    A prior build retained one Python dict plus a factor ``set`` for every
    usable outcome.  At roughly 1.9 million outcomes that consumed 4.8 GiB,
    before the 14-million-row forecast-contract audit had finished.  This
    store keeps only bounded SQLite fetch batches in Python and lets each
    evidence group be evaluated independently.  The temporary database is
    never an evidence source and is deleted when the build closes it.
    """

    def __init__(
        self,
        *,
        temporary_directory: tempfile.TemporaryDirectory[str] | None = None,
        connection: sqlite3.Connection | None = None,
        table: str = "bounded_rows",
        where_sql: str = "",
        where_args: tuple[Any, ...] = (),
        owner: "SpilledRows | None" = None,
    ) -> None:
        self._owner = owner
        self._temporary_directory = temporary_directory
        if connection is None:
            if temporary_directory is None:
                temporary_directory = tempfile.TemporaryDirectory(
                    prefix="oanda-edge-evidence-"
                )
                self._temporary_directory = temporary_directory
            path = Path(temporary_directory.name) / "bounded_rows.sqlite"
            connection = sqlite3.connect(path, timeout=30.0)
            connection.execute("PRAGMA journal_mode=OFF")
            connection.execute("PRAGMA synchronous=OFF")
            connection.execute("PRAGMA temp_store=FILE")
            connection.execute("PRAGMA cache_size=-32768")
        self.connection = connection
        self.table = str(table)
        self.where_sql = str(where_sql)
        self.where_args = tuple(where_args)
        self._closed = False
        self._length: int | None = None
        if owner is None:
            self._finalizer = weakref.finalize(
                self, SpilledRows._cleanup, connection, self._temporary_directory
            )
        else:
            self._finalizer = None

    @staticmethod
    def _cleanup(
        connection: sqlite3.Connection,
        temporary_directory: tempfile.TemporaryDirectory[str] | None,
    ) -> None:
        try:
            connection.close()
        except sqlite3.Error:
            pass
        if temporary_directory is not None:
            try:
                temporary_directory.cleanup()
            except OSError:
                pass

    @classmethod
    def create(cls) -> "SpilledRows":
        result = cls()
        result.connection.executescript(
            """
            CREATE TABLE bounded_rows (
                spool_id INTEGER PRIMARY KEY,
                row_id INTEGER NOT NULL,
                event_id TEXT NOT NULL,
                horizon_sec INTEGER NOT NULL,
                entry_time TEXT NOT NULL,
                entry_epoch REAL NOT NULL,
                entry_day TEXT NOT NULL,
                entry_week TEXT NOT NULL,
                family TEXT NOT NULL,
                instrument TEXT NOT NULL,
                direction TEXT NOT NULL,
                executable_pips REAL NOT NULL,
                net_pips REAL NOT NULL,
                spread_pips REAL NOT NULL,
                session TEXT NOT NULL,
                liquidity TEXT NOT NULL,
                max_favorable_pips REAL NOT NULL,
                max_adverse_pips REAL NOT NULL,
                first_positive_sec REAL,
                configured_stop_hit_sec REAL,
                configured_target_hit_sec REAL,
                archetype TEXT NOT NULL,
                entry_minute INTEGER NOT NULL
            );
            CREATE TABLE scratch_episode_events (
                component_id INTEGER NOT NULL,
                event_id TEXT NOT NULL
            );
            CREATE INDEX scratch_episode_events_component
                ON scratch_episode_events(component_id,event_id);
            CREATE TABLE scratch_episodes (
                episode_id TEXT PRIMARY KEY,
                entry_epoch REAL NOT NULL,
                entry_day TEXT NOT NULL,
                entry_week TEXT NOT NULL,
                instrument TEXT NOT NULL,
                session TEXT NOT NULL,
                liquidity TEXT NOT NULL,
                net_pips REAL NOT NULL,
                executable_pips REAL NOT NULL,
                spread_pips REAL NOT NULL,
                max_favorable_pips REAL NOT NULL,
                max_adverse_pips REAL NOT NULL,
                target_before_stop REAL NOT NULL,
                raw_count INTEGER NOT NULL,
                factor_count INTEGER NOT NULL
            );
            """
        )
        return result

    def finish_loading(self) -> None:
        self.connection.commit()
        self.connection.executescript(
            """
            CREATE INDEX bounded_rows_cell ON bounded_rows(
                family,instrument,horizon_sec,session,liquidity,
                entry_epoch,event_id
            );
            CREATE INDEX bounded_rows_family ON bounded_rows(
                family,horizon_sec,entry_epoch,event_id
            );
            CREATE INDEX bounded_rows_archetype ON bounded_rows(
                archetype,horizon_sec,entry_epoch,event_id
            );
            CREATE INDEX bounded_rows_minute ON bounded_rows(
                horizon_sec,entry_minute,spool_id
            );
            """
        )
        self.connection.commit()

    def append_many(self, values: list[tuple[Any, ...]]) -> None:
        self.connection.executemany(
            "INSERT INTO bounded_rows VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            values,
        )
        self._length = None

    def view(
        self, condition: str = "", args: tuple[Any, ...] = (), *, table: str | None = None
    ) -> "SpilledRows":
        clauses = [value for value in (self.where_sql, condition) if value]
        return SpilledRows(
            connection=self.connection,
            table=table or self.table,
            where_sql=" AND ".join(f"({value})" for value in clauses),
            where_args=(*self.where_args, *args),
            owner=self._owner or self,
        )

    def _select_sql(self, columns: str, *, order_by: str = "spool_id") -> str:
        where = f" WHERE {self.where_sql}" if self.where_sql else ""
        order = f" ORDER BY {order_by}" if order_by else ""
        return f"SELECT {columns} FROM {self.table}{where}{order}"

    def iter_ordered(self, order_by: str = "spool_id") -> Iterator[dict[str, Any]]:
        cursor = self.connection.execute(
            self._select_sql(",".join(_SPOOL_COLUMNS), order_by=order_by),
            self.where_args,
        )
        for values in cursor:
            row = dict(zip(_SPOOL_COLUMNS, values))
            row["factors"] = signed_currency_factors(
                str(row["instrument"]), str(row["direction"])
            )
            yield row

    def __iter__(self) -> Iterator[dict[str, Any]]:
        return self.iter_ordered()

    def __len__(self) -> int:
        if self._length is None:
            where = f" WHERE {self.where_sql}" if self.where_sql else ""
            self._length = int(
                self.connection.execute(
                    f"SELECT COUNT(*) FROM {self.table}{where}", self.where_args
                ).fetchone()[0]
            )
        return self._length

    def grouped_keys(self, columns: tuple[str, ...]) -> Iterator[tuple[Any, ...]]:
        names = ",".join(columns)
        where = f" WHERE {self.where_sql}" if self.where_sql else ""
        # MIN(spool_id) reproduces first-seen dict insertion order for stable
        # sorting ties in the legacy in-memory implementation.
        query = (
            f"SELECT {names} FROM {self.table}{where} GROUP BY {names} "
            "ORDER BY MIN(spool_id)"
        )
        yield from self.connection.execute(query, self.where_args)

    def close(self) -> None:
        if self._owner is not None or self._closed:
            return
        self._closed = True
        if self._finalizer is not None:
            self._finalizer()


@dataclass(slots=True)
class _EpisodeComponent:
    component_id: int
    first_key: tuple[float, str]
    first_row: dict[str, Any]
    last_epoch: float
    raw_count: int = 0
    executable_sum: float = 0.0
    net_sum: float = 0.0
    spread_sum: float = 0.0
    favorable_sum: float = 0.0
    adverse_sum: float = 0.0
    target_before_stop_sum: float = 0.0
    factors: set[str] | None = None


def emit_progress(
    callback: ProgressCallback | None,
    phase: str,
    **details: Any,
) -> None:
    """Publish best-effort build progress without changing evidence results."""

    if callback is None:
        return
    try:
        callback(str(phase), dict(details))
    except Exception:
        # Diagnostics must never alter, truncate, or reject an evidence build.
        return


def _open_snapshot_reader(database_path: Path) -> sqlite3.Connection:
    """Open a read-only source connection used for one bounded statement.

    WAL readers keep their end mark until the statement/transaction ends.  A
    single cursor over the production-sized forecast ledger can therefore pin
    gigabytes of otherwise checkpointable WAL for hours.  Snapshot scans below
    freeze an immutable rowid high-water and reopen the database for each
    bounded page, so every page releases its read mark before Python processes
    or spools the batch.
    """

    connection = sqlite3.connect(
        f"file:{database_path.resolve().as_posix()}?mode=ro",
        uri=True,
        timeout=30.0,
    )
    connection.execute("PRAGMA query_only=ON")
    connection.execute("PRAGMA busy_timeout=30000")
    return connection


def iter_immutable_snapshot_batches(
    database_path: Path,
    *,
    table: str,
    columns: str,
    maximum_rowid: int,
    minimum_rowid: int = 0,
    where_sql: str = "",
    where_args: tuple[Any, ...] = (),
    batch_size: int = 25_000,
    rowid_column: str = "rowid",
) -> Iterator[list[tuple[Any, ...]]]:
    """Yield exact, ordered pages from an append-only rowid snapshot.

    ``columns`` must begin with ``rowid_column``.  Each page is fully fetched
    and its connection is closed before the page is yielded.  New appends are
    excluded by ``maximum_rowid``; update/delete triggers on the canonical
    ledgers make the resulting logical snapshot equivalent to one long read
    transaction without pinning the live WAL between pages.
    """

    ceiling = max(0, int(maximum_rowid))
    page_size = max(1, int(batch_size))
    last_rowid = min(ceiling, max(0, int(minimum_rowid)))
    extra = f" AND ({where_sql})" if where_sql else ""
    while last_rowid < ceiling:
        connection = _open_snapshot_reader(database_path)
        try:
            batch = connection.execute(
                f"""
                SELECT {columns} FROM \"{table}\"
                WHERE \"{rowid_column}\">? AND \"{rowid_column}\"<=?
                {extra}
                ORDER BY \"{rowid_column}\" LIMIT ?
                """,
                (last_rowid, ceiling, *where_args, page_size),
            ).fetchall()
        finally:
            connection.close()
        if not batch:
            break
        next_rowid = int(batch[-1][0])
        if next_rowid <= last_rowid:
            raise RuntimeError(
                f"non-advancing immutable snapshot scan for {table}: "
                f"{next_rowid} <= {last_rowid}"
            )
        last_rowid = next_rowid
        yield batch


def count_immutable_snapshot_rows(
    database_path: Path,
    *,
    table: str,
    maximum_rowid: int,
    rowid_column: str = "rowid",
    rowid_window: int = 250_000,
) -> int:
    """Count a frozen append-only snapshot without one full-table reader."""

    ceiling = max(0, int(maximum_rowid))
    window = max(1, int(rowid_window))
    total = 0
    lower = 0
    while lower < ceiling:
        upper = min(ceiling, lower + window)
        connection = _open_snapshot_reader(database_path)
        try:
            total += int(
                connection.execute(
                    f"SELECT COUNT(*) FROM \"{table}\" "
                    f"WHERE \"{rowid_column}\">? AND \"{rowid_column}\"<=?",
                    (lower, upper),
                ).fetchone()[0]
            )
        finally:
            connection.close()
        lower = upper
    return total


def forecast_contract_cache_identity(
    connection: sqlite3.Connection,
    database_path: Path,
) -> str | None:
    """Return a source identity only for a trigger-enforced immutable ledger.

    The evidence worker rebuilds whenever outcomes mature, even when no new
    forecast was captured. Forecast contract and unavailable-microstructure
    diagnostics are pure functions of the frozen canonical forecast snapshot,
    so the same audit can be reused while its high-water is unchanged. Reuse
    is deliberately disabled unless both update and delete immutability
    triggers are present. The database file identity and relevant schema are
    included so replacing the source or changing its contract fails toward a
    fresh exact scan.
    """

    schema_rows = connection.execute(
        """
        SELECT type,name,tbl_name,COALESCE(sql,'')
        FROM sqlite_master
        WHERE tbl_name='canonical_forecasts'
        ORDER BY type,name
        """
    ).fetchall()
    trigger_sql = {
        str(name): str(sql or "").upper()
        for object_type, name, _table, sql in schema_rows
        if str(object_type) == "trigger"
    }
    update_trigger = trigger_sql.get("canonical_forecasts_no_update", "")
    delete_trigger = trigger_sql.get("canonical_forecasts_no_delete", "")
    if not (
        "BEFORE UPDATE ON CANONICAL_FORECASTS" in update_trigger
        and "RAISE" in update_trigger
        and "BEFORE DELETE ON CANONICAL_FORECASTS" in delete_trigger
        and "RAISE" in delete_trigger
    ):
        return None
    try:
        stat = database_path.stat()
    except OSError:
        return None
    # A zero inode/file-index cannot distinguish an in-place source from a
    # replacement, so fail closed to a scan on such filesystems.
    if not int(stat.st_ino):
        return None
    return stable_hash(
        {
            "database": str(database_path.resolve()),
            "device": int(stat.st_dev),
            "file_id": int(stat.st_ino),
            "schema": schema_rows,
        }
    )


def resolve_forecast_contract_snapshot(
    database_path: Path,
    *,
    maximum_rowid: int,
    source_identity: str | None,
    audit_cache: dict[str, Any] | None = None,
    progress_callback: ProgressCallback | None = None,
    batch_size: int = 25_000,
) -> tuple[dict[str, int], dict[str, int], str]:
    """Return an exact audit, extending only a verified immutable snapshot.

    The cache retains the exact disk-backed set of payload digests, not an
    approximate filter. New rows can therefore update duplicate counts and all
    additive contract diagnostics by scanning only ``(old_highwater, new_highwater]``.
    """

    cached = audit_cache if isinstance(audit_cache, dict) else None
    cached_contract: Any = None
    cached_unavailable: Any = None
    cached_seen: Any = None
    cached_highwater = 0
    cache_valid = False
    if source_identity and cached:
        cached_contract = cached.get("forecast_contract")
        cached_unavailable = cached.get("unavailable_inputs")
        cached_seen = cached.get("seen_payloads_connection")
        cached_highwater = int(cached.get("maximum_rowid") or 0)
        cache_valid = bool(
            cached.get("source_identity") == source_identity
            and 0 <= cached_highwater <= int(maximum_rowid)
            and isinstance(cached_contract, dict)
            and isinstance(cached_unavailable, dict)
            and isinstance(cached_seen, sqlite3.Connection)
            and int(cached_contract.get("captured") or 0)
            == int(cached_unavailable.get("forecast_rows_inspected") or 0)
            and int(cached_contract.get("duplicate_payload_rows_scanned") or 0)
            == int(cached_contract.get("captured") or 0)
        )
        if cache_valid:
            try:
                cached_seen.execute("SELECT 1 FROM payloads LIMIT 1").fetchone()
            except sqlite3.Error:
                cache_valid = False
        if cache_valid and cached_highwater == int(maximum_rowid):
            contract = {str(key): int(value) for key, value in cached_contract.items()}
            unavailable = {
                str(key): int(value) for key, value in cached_unavailable.items()
            }
            emit_progress(
                progress_callback,
                "inventorying_forecast_contract",
                inventory_step="reusing_immutable_forecast_contract_audit",
                forecast_contract_audit_cache_hit=True,
                forecast_snapshot_highwater_rowid=int(maximum_rowid),
                **contract,
                **unavailable,
            )
            return contract, unavailable, "cache_hit"
        if cache_valid and cached_highwater < int(maximum_rowid):
            cached_seen.execute("SAVEPOINT forecast_contract_extension")
            try:
                contract, unavailable = audit_forecast_contract_snapshot(
                    database_path,
                    minimum_rowid=cached_highwater,
                    maximum_rowid=maximum_rowid,
                    initial_contract=cached_contract,
                    initial_unavailable=cached_unavailable,
                    seen_payloads=cached_seen,
                    progress_callback=progress_callback,
                    batch_size=batch_size,
                )
            except Exception:
                cached_seen.execute("ROLLBACK TO forecast_contract_extension")
                cached_seen.execute("RELEASE forecast_contract_extension")
                raise
            else:
                cached_seen.execute("RELEASE forecast_contract_extension")
            cached.update(
                {
                    "maximum_rowid": int(maximum_rowid),
                    "forecast_contract": dict(contract),
                    "unavailable_inputs": dict(unavailable),
                }
            )
            return contract, unavailable, "incremental_extension"

    if cached is not None:
        close_forecast_contract_cache(cached)
    seen_payloads = _new_seen_payload_store() if source_identity and cached is not None else None
    try:
        contract, unavailable = audit_forecast_contract_snapshot(
            database_path,
            maximum_rowid=maximum_rowid,
            seen_payloads=seen_payloads,
            progress_callback=progress_callback,
            batch_size=batch_size,
        )
    except Exception:
        if seen_payloads is not None:
            seen_payloads.close()
        raise
    if source_identity and cached is not None and seen_payloads is not None:
        cached.update(
            {
                "source_identity": source_identity,
                "maximum_rowid": int(maximum_rowid),
                "forecast_contract": dict(contract),
                "unavailable_inputs": dict(unavailable),
                "seen_payloads_connection": seen_payloads,
            }
        )
    return contract, unavailable, "full_scan"


def _new_seen_payload_store() -> sqlite3.Connection:
    """Create an automatically deleted exact payload-membership store."""

    connection = sqlite3.connect("")
    connection.execute("PRAGMA journal_mode=OFF")
    connection.execute("PRAGMA synchronous=OFF")
    connection.execute("PRAGMA temp_store=FILE")
    connection.execute("CREATE TABLE payloads(value TEXT PRIMARY KEY) WITHOUT ROWID")
    return connection


def close_forecast_contract_cache(audit_cache: dict[str, Any] | None) -> None:
    """Release the temporary exact payload index retained by a worker."""

    if not isinstance(audit_cache, dict):
        return
    connection = audit_cache.get("seen_payloads_connection")
    if isinstance(connection, sqlite3.Connection):
        try:
            connection.close()
        except sqlite3.Error:
            pass
    audit_cache.clear()


def count_duplicate_forecast_payloads(
    connection: sqlite3.Connection,
    *,
    progress_callback: ProgressCallback | None = None,
    batch_size: int = 50_000,
) -> tuple[int, int]:
    """Count repeated forecast payloads exactly without an unindexed SQL sort.

    The canonical table is append-only.  A bounded sequential scan is both
    cheaper than COUNT(DISTINCT) on the multi-million-row ledger and able to
    publish honest progress during a long evidence cycle.
    """
    # An in-process set retained every 64-byte payload digest.  The live
    # ledger now contains more than 14 million forecasts, so that diagnostic
    # alone could add well over a gigabyte to the evidence worker.  An empty
    # SQLite filename creates an automatically deleted, disk-backed database.
    seen = sqlite3.connect("")
    seen.execute("PRAGMA journal_mode=OFF")
    seen.execute("PRAGMA synchronous=OFF")
    seen.execute("PRAGMA temp_store=FILE")
    seen.execute("CREATE TABLE payloads(value TEXT PRIMARY KEY) WITHOUT ROWID")
    duplicates = scanned = 0
    cursor = connection.execute("SELECT payload_sha256 FROM canonical_forecasts")
    try:
        while True:
            batch = cursor.fetchmany(max(1, int(batch_size)))
            if not batch:
                break
            before = seen.total_changes
            seen.executemany(
                "INSERT OR IGNORE INTO payloads VALUES (?)",
                ((str(payload_hash),) for (payload_hash,) in batch),
            )
            inserted = seen.total_changes - before
            duplicates += len(batch) - inserted
            scanned += len(batch)
            emit_progress(
                progress_callback,
                "inventorying_forecast_contract",
                inventory_step="counting_duplicate_payloads_disk_bounded",
                duplicate_scan_rows=scanned,
                duplicate_payloads=duplicates,
            )
    finally:
        seen.close()
    return duplicates, scanned


def audit_unavailable_microstructure(
    connection: sqlite3.Connection,
    *,
    progress_callback: ProgressCallback | None = None,
    batch_size: int = 25_000,
) -> dict[str, int]:
    """Inspect zero/unavailable depth contracts with bounded progress."""
    counts = {
        "forecast_rows_inspected": 0,
        "order_book_available_rows": 0,
        "position_book_available_rows": 0,
        "pricing_depth_informative_rows": 0,
        "malformed_forecast_json_rows": 0,
    }
    cursor = connection.execute("SELECT forecast_json FROM canonical_forecasts")
    while True:
        batch = cursor.fetchmany(max(1, int(batch_size)))
        if not batch:
            break
        for (payload_text,) in batch:
            counts["forecast_rows_inspected"] += 1
            try:
                payload = json.loads(payload_text)
                micro = payload.get("microstructure_at_entry") or {}
                order = float(micro.get("order_book_available") or 0.0)
                position = float(micro.get("position_book_available") or 0.0)
                bid = float(micro.get("bid_total_liquidity") or 0.0)
                ask = float(micro.get("ask_total_liquidity") or 0.0)
            except (AttributeError, TypeError, ValueError, json.JSONDecodeError):
                counts["malformed_forecast_json_rows"] += 1
                continue
            counts["order_book_available_rows"] += int(order > 0.0)
            counts["position_book_available_rows"] += int(position > 0.0)
            counts["pricing_depth_informative_rows"] += int(bid > 0.0 or ask > 0.0)
        emit_progress(
            progress_callback,
            "inventorying_forecast_contract",
            inventory_step="auditing_unavailable_microstructure",
            **counts,
        )
    return counts


def audit_forecast_contract(
    connection: sqlite3.Connection,
    *,
    progress_callback: ProgressCallback | None = None,
    batch_size: int = 25_000,
) -> tuple[dict[str, int], dict[str, int]]:
    """Compute contract, duplicate, and unavailable-input diagnostics in one scan."""
    contract = {
        "captured": 0,
        "tracked_for_outcome": 0,
        "signal": 0,
        "miss": 0,
        "unknown_model_version": 0,
        "unknown_feature_version": 0,
        "duplicate_payloads": 0,
        "duplicate_payload_rows_scanned": 0,
    }
    unavailable = {
        "forecast_rows_inspected": 0,
        "order_book_available_rows": 0,
        "position_book_available_rows": 0,
        "pricing_depth_informative_rows": 0,
        "malformed_forecast_json_rows": 0,
    }
    seen_payloads = sqlite3.connect("")
    seen_payloads.execute("PRAGMA journal_mode=OFF")
    seen_payloads.execute("PRAGMA synchronous=OFF")
    seen_payloads.execute("PRAGMA temp_store=FILE")
    seen_payloads.execute(
        "CREATE TABLE payloads(value TEXT PRIMARY KEY) WITHOUT ROWID"
    )
    cursor = connection.execute(
        """
        SELECT track_outcome,kind,model_version,feature_version,
               payload_sha256,forecast_json
        FROM canonical_forecasts
        """
    )
    try:
        while True:
            batch = cursor.fetchmany(max(1, int(batch_size)))
            if not batch:
                break
            payload_batch: list[tuple[str]] = []
            for track, kind, model_version, feature_version, payload_sha, forecast_json in batch:
                contract["captured"] += 1
                contract["tracked_for_outcome"] += int(track or 0)
                contract["signal"] += int(kind == "signal")
                contract["miss"] += int(kind == "miss")
                contract["unknown_model_version"] += int(model_version == "unknown")
                contract["unknown_feature_version"] += int(feature_version == "unknown")
                payload_batch.append((str(payload_sha),))
                contract["duplicate_payload_rows_scanned"] += 1

                unavailable["forecast_rows_inspected"] += 1
                try:
                    payload = json.loads(forecast_json)
                    micro = payload.get("microstructure_at_entry") or {}
                    order = float(micro.get("order_book_available") or 0.0)
                    position = float(micro.get("position_book_available") or 0.0)
                    bid = float(micro.get("bid_total_liquidity") or 0.0)
                    ask = float(micro.get("ask_total_liquidity") or 0.0)
                except (AttributeError, TypeError, ValueError, json.JSONDecodeError):
                    unavailable["malformed_forecast_json_rows"] += 1
                    continue
                unavailable["order_book_available_rows"] += int(order > 0.0)
                unavailable["position_book_available_rows"] += int(position > 0.0)
                unavailable["pricing_depth_informative_rows"] += int(bid > 0.0 or ask > 0.0)
            before = seen_payloads.total_changes
            seen_payloads.executemany(
                "INSERT OR IGNORE INTO payloads VALUES (?)", payload_batch
            )
            contract["duplicate_payloads"] += (
                len(payload_batch) - (seen_payloads.total_changes - before)
            )
            emit_progress(
                progress_callback,
                "inventorying_forecast_contract",
                inventory_step="single_pass_contract_duplicate_microstructure_audit_disk_bounded",
                **contract,
                **unavailable,
            )
    finally:
        seen_payloads.close()
    return contract, unavailable


def audit_forecast_contract_snapshot(
    database_path: Path,
    *,
    maximum_rowid: int,
    minimum_rowid: int = 0,
    initial_contract: dict[str, int] | None = None,
    initial_unavailable: dict[str, int] | None = None,
    seen_payloads: sqlite3.Connection | None = None,
    progress_callback: ProgressCallback | None = None,
    batch_size: int = 25_000,
) -> tuple[dict[str, int], dict[str, int]]:
    """Audit or extend a frozen forecast snapshot without a long WAL reader."""

    contract = {
        "captured": 0,
        "tracked_for_outcome": 0,
        "signal": 0,
        "miss": 0,
        "unknown_model_version": 0,
        "unknown_feature_version": 0,
        "duplicate_payloads": 0,
        "duplicate_payload_rows_scanned": 0,
    }
    unavailable = {
        "forecast_rows_inspected": 0,
        "order_book_available_rows": 0,
        "position_book_available_rows": 0,
        "pricing_depth_informative_rows": 0,
        "malformed_forecast_json_rows": 0,
    }
    if initial_contract:
        contract.update(
            {key: int(value) for key, value in initial_contract.items() if key in contract}
        )
    if initial_unavailable:
        unavailable.update(
            {
                key: int(value)
                for key, value in initial_unavailable.items()
                if key in unavailable
            }
        )
    owns_seen_payloads = seen_payloads is None
    if seen_payloads is None:
        seen_payloads = _new_seen_payload_store()
    try:
        for batch in iter_immutable_snapshot_batches(
            database_path,
            table="canonical_forecasts",
            columns=(
                "rowid,track_outcome,kind,model_version,feature_version,"
                "payload_sha256,forecast_json"
            ),
            maximum_rowid=maximum_rowid,
            minimum_rowid=minimum_rowid,
            batch_size=batch_size,
        ):
            payload_batch: list[tuple[str]] = []
            for (
                _rowid,
                track,
                kind,
                model_version,
                feature_version,
                payload_sha,
                forecast_json,
            ) in batch:
                contract["captured"] += 1
                contract["tracked_for_outcome"] += int(track or 0)
                contract["signal"] += int(kind == "signal")
                contract["miss"] += int(kind == "miss")
                contract["unknown_model_version"] += int(model_version == "unknown")
                contract["unknown_feature_version"] += int(feature_version == "unknown")
                payload_batch.append((str(payload_sha),))
                contract["duplicate_payload_rows_scanned"] += 1

                unavailable["forecast_rows_inspected"] += 1
                try:
                    payload = json.loads(forecast_json)
                    micro = payload.get("microstructure_at_entry") or {}
                    order = float(micro.get("order_book_available") or 0.0)
                    position = float(micro.get("position_book_available") or 0.0)
                    bid = float(micro.get("bid_total_liquidity") or 0.0)
                    ask = float(micro.get("ask_total_liquidity") or 0.0)
                except (AttributeError, TypeError, ValueError, json.JSONDecodeError):
                    unavailable["malformed_forecast_json_rows"] += 1
                    continue
                unavailable["order_book_available_rows"] += int(order > 0.0)
                unavailable["position_book_available_rows"] += int(position > 0.0)
                unavailable["pricing_depth_informative_rows"] += int(
                    bid > 0.0 or ask > 0.0
                )
            before = seen_payloads.total_changes
            seen_payloads.executemany(
                "INSERT OR IGNORE INTO payloads VALUES (?)", payload_batch
            )
            contract["duplicate_payloads"] += (
                len(payload_batch) - (seen_payloads.total_changes - before)
            )
            emit_progress(
                progress_callback,
                "inventorying_forecast_contract",
                inventory_step=(
                    "snapshot_paged_incremental_contract_audit"
                    if int(minimum_rowid) > 0
                    else "snapshot_paged_contract_duplicate_microstructure_audit"
                ),
                forecast_snapshot_start_rowid=int(minimum_rowid),
                forecast_snapshot_highwater_rowid=int(maximum_rowid),
                **contract,
                **unavailable,
            )
    finally:
        if owns_seen_payloads:
            seen_payloads.close()
    return contract, unavailable


def ensure_primary_seed_cohort(
    source_database: Path,
    governance: dict[str, Any],
    *,
    database_path: Path = DEFAULT_CANDIDATE_COHORT_DATABASE,
    state_path: Path = DEFAULT_CANDIDATE_COHORT_STATE,
) -> dict[str, Any]:
    strategy_source = ROOT / "oanda_practice_shadow_strategy_lab.py"
    source_sha = hashlib.sha256(strategy_source.read_bytes()).hexdigest()
    feature_contract = {
        "conditions": [
            "pair", "session", "liquidity", "direction", "volatility_regime",
            "squeeze_duration_proxy", "squeeze_width_ratio", "session_range_location",
            "spread_percentile_proxy", "stop_geometry", "target_geometry",
            "entry_sequence", "cross_pair_factor_confirmation", "macro_event_proximity",
        ],
        "forecast_time_only": True,
    }
    connection = sqlite3.connect(f"file:{source_database.resolve().as_posix()}?mode=ro", uri=True)
    try:
        pre_count = int(
            connection.execute(
                "SELECT COUNT(*) FROM canonical_forecasts WHERE family=?",
                (PRIMARY_SEED,),
            ).fetchone()[0]
        )
    except sqlite3.OperationalError:
        pre_count = 0
    finally:
        connection.close()
    start = utc_now()
    active = ensure_cohorts(
        database_path,
        state_path,
        [
            {
                "family": PRIMARY_SEED,
                "source_sha256": f"sha256:{source_sha}",
                "model_specification": {
                    "model": "existing_volatility_squeeze_breakout_v1",
                    "implementation": "frozen_by_full_strategy_source_hash",
                },
                "hyperparameters": {
                    "implementation": "embedded_in_frozen_source",
                    "post_lock_tuning": False,
                },
                "feature_schema_version": "sha256:" + stable_hash(feature_contract),
                "training_policy": {"mode": "rule_based_no_fit"},
                "forecast_contract_version": "canonical_forecast_v1",
                "cost_model_version": "executable_bid_ask_plus_0.25pip_modeled_slippage_v1",
                "dimensions": feature_contract,
                "deduplication_rules": {
                    "unit": "family_pair_horizon_session_liquidity",
                    "episode": "shared_signed_currency_factor_within_horizon",
                },
                "promotion_thresholds": {
                    "discovery": governance.get("discovery") or {},
                    "sequential": governance.get("sequential") or {},
                    "power": governance.get("power") or {},
                    "minimum_economic_edge_pips": governance.get("minimum_economic_edge_pips") or {},
                    "graduation": governance.get("graduation") or {},
                },
                "initial_training_cutoff_utc": "not_applicable_rule_based",
                "initial_training_dataset_sha256": "not_applicable_rule_based",
                "pre_governance_forecast_count": pre_count,
                "cohort_start_utc": start,
            }
        ],
    )
    state = read_json(state_path)
    cohort_id = active.get(PRIMARY_SEED)
    cohort = next(
        (row for row in state.get("cohorts") or [] if row.get("cohort_id") == cohort_id),
        {},
    )
    return {
        "cohort_id": cohort_id,
        "cohort_start_utc": cohort.get("cohort_start_utc"),
        "pre_governance_forecast_count": int(cohort.get("pre_governance_forecast_count") or 0),
        "policy": "only forecasts at or after cohort start enter governed squeeze replication",
        "can_place_orders": False,
    }


def structured_macro_coverage(
    news_database: Path,
    state_path: Path = DEFAULT_MACRO_SURPRISE_STATE,
) -> dict[str, Any]:
    topic_projection = surprise_coverage(news_database)
    try:
        ledger = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return {**topic_projection, "ledger_status": "macro_ledger_state_missing"}
    return {
        "status": str(ledger.get("status") or topic_projection.get("status") or "unknown"),
        "ledger_status": str(ledger.get("status") or "unknown"),
        "release_count": int(ledger.get("release_count") or 0),
        "scheduled_release_count": int(ledger.get("scheduled_release_count") or 0),
        "actual_count": int(ledger.get("actual_count") or 0),
        "consensus_count": int(ledger.get("consensus_count") or 0),
        "actual_and_consensus_count": int(ledger.get("actual_and_consensus_count") or 0),
        "standardized_surprise_count": int(ledger.get("standardized_surprise_count") or 0),
        "revision_count": int(ledger.get("total_revisions") or 0),
        "consensus_observation_count": int(ledger.get("consensus_observation_count") or 0),
        "causal_consensus_observation_count": int(
            ledger.get("causal_consensus_observation_count") or 0
        ),
        "reaction_sample_count": int(ledger.get("reaction_sample_count") or 0),
        "ledger_database": str(ledger.get("ledger_database") or ""),
        "topic_projection": topic_projection,
        # Compatibility names used by the concise Markdown summary.
        "topics": int(ledger.get("release_count") or 0),
        "actual_and_consensus_topics": int(ledger.get("actual_and_consensus_count") or 0),
    }


@dataclass(frozen=True)
class EvidenceThresholds:
    minimum_effective_n: int = 30
    minimum_average_net_pips: float = 0.10
    minimum_profit_factor: float = 1.10
    minimum_lower_confidence_pips: float = 0.0
    minimum_time_blocks: int = 4
    minimum_positive_time_block_fraction: float = 0.67
    maximum_best_episode_profit_share: float = 0.50
    maximum_average_net_at_one_pip_slippage: float = 0.0


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def finite(value: Any, default: float = 0.0) -> float:
    try:
        output = float(value)
    except (TypeError, ValueError):
        return default
    return output if math.isfinite(output) else default


def parse_time(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def session_bucket(value: Any) -> str:
    parsed = value if isinstance(value, datetime) else parse_time(value)
    if parsed is None:
        return "unknown"
    hour = parsed.hour
    if hour < 7:
        return "asia"
    if hour < 13:
        return "london"
    if hour < 17:
        return "overlap"
    if hour < 22:
        return "new_york"
    return "rollover"


def liquidity_bucket(spread_pips: Any) -> str:
    spread = finite(spread_pips, math.inf)
    if spread <= 2.0:
        return "liquid_le_2"
    if spread <= 3.0:
        return "normal_2_to_3"
    if spread <= 5.0:
        return "elevated_3_to_5"
    return "wide_gt_5"


def signed_currency_factors(instrument: str, direction: str) -> set[str]:
    parts = str(instrument or "").upper().split("_")
    if len(parts) != 2:
        return {f"instrument:{instrument}:{direction}"}
    base, quote = parts
    base_side = "long" if str(direction).lower() in {"buy", "long"} else "short"
    quote_side = "short" if base_side == "long" else "long"
    return {f"currency:{base}:{base_side}", f"currency:{quote}:{quote_side}"}


def stable_hash(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode(
        "utf-8"
    )
    return hashlib.sha256(encoded).hexdigest()


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def horizon_value(mapping: dict[str, Any], horizon_sec: int, default: float) -> float:
    ordered = sorted((int(key), finite(value, default)) for key, value in mapping.items())
    for maximum, value in ordered:
        if horizon_sec <= maximum:
            return value
    return ordered[-1][1] if ordered else default


def minimum_economic_edge(
    horizon_sec: int, liquidity: str, governance: dict[str, Any]
) -> float:
    schedule = governance.get("minimum_economic_edge_pips") or {}
    bucket = schedule.get(str(liquidity)) or schedule.get("wide_gt_5") or {}
    return horizon_value(bucket, horizon_sec, 1.0)


def clipped_mean_pvalue(values: list[float], null_mean: float, bound: float) -> float:
    if not values:
        return 1.0
    clipped = [max(-bound, min(bound, finite(value))) for value in values]
    excess = statistics.fmean(clipped) - float(null_mean)
    if excess <= 0.0:
        return 1.0
    return min(1.0, math.exp(-len(clipped) * excess * excess / (2.0 * bound * bound)))


def alpha_spending_confidence_sequence(
    values: list[float], *, alpha: float, bound: float
) -> tuple[float, float]:
    """Time-uniform clipped-mean interval using summable alpha spending."""

    if not values:
        return -bound, bound
    clipped = [max(-bound, min(bound, finite(value))) for value in values]
    count = len(clipped)
    alpha_n = max(1e-15, float(alpha) * 6.0 / (math.pi * math.pi * count * count))
    radius = 2.0 * bound * math.sqrt(math.log(1.0 / alpha_n) / (2.0 * count))
    mean = statistics.fmean(clipped)
    return mean - radius, mean + radius


def profit_factor(values: Iterable[float]) -> float:
    sample = [finite(value) for value in values]
    gains = sum(value for value in sample if value > 0.0)
    losses = -sum(value for value in sample if value < 0.0)
    if losses <= 1e-12:
        return 999.0 if gains > 0.0 else 0.0
    return gains / losses


def lower_confidence(values: list[float]) -> float:
    if len(values) < 2:
        return -999.0
    return statistics.fmean(values) - 1.645 * statistics.stdev(values) / math.sqrt(
        len(values)
    )


def maximum_drawdown(values: list[float]) -> float:
    equity = peak = 0.0
    drawdown = 0.0
    for value in values:
        equity += value
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
    return drawdown


def percentile(values: list[float], probability: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    position = max(0.0, min(1.0, probability)) * (len(ordered) - 1)
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def positive_concentration(rows: list[dict[str, Any]], key: str) -> float:
    contributions: dict[str, float] = defaultdict(float)
    for row in rows:
        contributions[str(row.get(key) or "unknown")] += max(
            0.0, finite(row.get("net_pips"))
        )
    total = sum(contributions.values())
    return max(contributions.values(), default=0.0) / total if total > 0.0 else 0.0


def top_fraction_profit_share(values: list[float], fraction: float = 0.05) -> float:
    positive = sorted((value for value in values if value > 0.0), reverse=True)
    total = sum(positive)
    if total <= 0.0:
        return 0.0
    take = max(1, int(math.ceil(len(values) * fraction)))
    return sum(positive[:take]) / total


def currency_profit_concentration(rows: list[dict[str, Any]]) -> float:
    contributions: dict[str, float] = defaultdict(float)
    for row in rows:
        parts = str(row.get("instrument") or "").split("_")
        if len(parts) != 2:
            continue
        positive = max(0.0, finite(row.get("net_pips"))) * 0.5
        contributions[parts[0]] += positive
        contributions[parts[1]] += positive
    total = sum(contributions.values())
    return max(contributions.values(), default=0.0) / total if total > 0.0 else 0.0


def removal_average(rows: list[dict[str, Any]], key: str) -> dict[str, Any]:
    contributions: dict[str, float] = defaultdict(float)
    for row in rows:
        contributions[str(row.get(key) or "unknown")] += max(
            0.0, finite(row.get("net_pips"))
        )
    removed = max(contributions, key=contributions.get) if contributions else None
    remaining = [
        finite(row.get("net_pips"))
        for row in rows
        if removed is None or str(row.get(key) or "unknown") != removed
    ]
    return {
        "removed": removed,
        "remaining_n": len(remaining),
        "avg_net_pips_after_removal": (
            round(statistics.fmean(remaining), 6) if remaining else None
        ),
    }


def currency_removal_average(rows: list[dict[str, Any]]) -> dict[str, Any]:
    contributions: dict[str, float] = defaultdict(float)
    for row in rows:
        parts = str(row.get("instrument") or "").split("_")
        for currency in parts if len(parts) == 2 else []:
            contributions[currency] += max(0.0, finite(row.get("net_pips"))) * 0.5
    removed = max(contributions, key=contributions.get) if contributions else None
    remaining = [
        finite(row.get("net_pips"))
        for row in rows
        if removed is None or removed not in str(row.get("instrument") or "").split("_")
    ]
    return {
        "removed": removed,
        "remaining_n": len(remaining),
        "avg_net_pips_after_removal": (
            round(statistics.fmean(remaining), 6) if remaining else None
        ),
    }


def top_fraction_removal_average(values: list[float], fraction: float = 0.05) -> dict[str, Any]:
    take = max(1, int(math.ceil(len(values) * fraction))) if values else 0
    remaining = sorted(values, reverse=True)[take:]
    return {
        "removed_n": take,
        "remaining_n": len(remaining),
        "avg_net_pips_after_removal": (
            round(statistics.fmean(remaining), 6) if remaining else None
        ),
    }


def collapse_factor_episodes(
    rows: list[dict[str, Any]], horizon_sec: int
) -> list[dict[str, Any]]:
    """Collapse correlated forecasts into continuous signed-factor episodes."""

    ordered = sorted(rows, key=lambda row: (finite(row.get("entry_epoch")), row["event_id"]))
    if not ordered:
        return []
    parents = list(range(len(ordered)))

    def find(index: int) -> int:
        while parents[index] != index:
            parents[index] = parents[parents[index]]
            index = parents[index]
        return index

    def union(left: int, right: int) -> None:
        left_root, right_root = find(left), find(right)
        if left_root != right_root:
            parents[right_root] = left_root

    gap = max(60.0, float(horizon_sec))
    latest_by_factor: dict[str, int] = {}
    for right, current in enumerate(ordered):
        current_factors = set(current.get("factors") or ())
        for factor in current_factors:
            left = latest_by_factor.get(str(factor))
            if left is not None and current["entry_epoch"] - ordered[left]["entry_epoch"] <= gap:
                union(left, right)
            latest_by_factor[str(factor)] = right

    groups: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for index, row in enumerate(ordered):
        groups[find(index)].append(row)
    episodes: list[dict[str, Any]] = []
    for members in groups.values():
        event_ids = sorted(str(row["event_id"]) for row in members)
        first = min(members, key=lambda row: row["entry_epoch"])
        episodes.append(
            {
                "episode_id": "episode_" + stable_hash(event_ids)[:24],
                "event_id": event_ids[0],
                "entry_epoch": first["entry_epoch"],
                "entry_day": first["entry_day"],
                "entry_week": first.get("entry_week") or first["entry_day"],
                "instrument": first["instrument"],
                "session": first["session"],
                "liquidity": first["liquidity"],
                "net_pips": statistics.fmean(finite(row["net_pips"]) for row in members),
                "executable_pips": statistics.fmean(
                    finite(row["executable_pips"]) for row in members
                ),
                "spread_pips": statistics.fmean(
                    finite(row["spread_pips"]) for row in members
                ),
                "max_favorable_pips": statistics.fmean(
                    finite(row.get("max_favorable_pips")) for row in members
                ),
                "max_adverse_pips": statistics.fmean(
                    finite(row.get("max_adverse_pips")) for row in members
                ),
                "target_before_stop": statistics.fmean(
                    1.0
                    if row.get("configured_target_hit_sec") is not None
                    and (
                        row.get("configured_stop_hit_sec") is None
                        or finite(row.get("configured_target_hit_sec"))
                        < finite(row.get("configured_stop_hit_sec"))
                    )
                    else 0.0
                    for row in members
                ),
                "raw_count": len(members),
                "factor_count": len(
                    set().union(*(set(row.get("factors") or ()) for row in members))
                ),
            }
        )
    return sorted(episodes, key=lambda row: (row["entry_epoch"], row["episode_id"]))


def _stream_json_list_sha256(values: Iterable[str]) -> tuple[str, str]:
    """Hash a sorted JSON string list exactly like ``stable_hash(list)``."""

    digest = hashlib.sha256()
    digest.update(b"[")
    first_value = ""
    first = True
    for value in values:
        text = str(value)
        if first:
            first_value = text
            first = False
        else:
            digest.update(b",")
        digest.update(
            json.dumps(text, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        )
    digest.update(b"]")
    return digest.hexdigest(), first_value


def _materialize_spilled_episodes(
    rows: SpilledRows, horizon_sec: int
) -> tuple[int, float, set[str], set[str], set[str]]:
    """Collapse one ordered group while retaining only active components.

    Event ids live in the temporary SQLite database.  Union-by-size updates
    the smaller component on a merge, avoiding an unbounded Python list even
    when one currency factor remains continuously active for days.
    """

    connection = rows.connection
    connection.execute("DELETE FROM scratch_episode_events")
    connection.execute("DELETE FROM scratch_episodes")
    components: dict[int, _EpisodeComponent] = {}
    latest_by_factor: dict[str, tuple[float, int]] = {}
    expiry_heap: list[tuple[float, int, int]] = []
    versions: dict[int, int] = {}
    next_component = 0
    raw_n = 0
    raw_net_sum = 0.0
    instruments: set[str] = set()
    sessions: set[str] = set()
    liquidities: set[str] = set()
    gap = max(60.0, float(horizon_sec))

    def add_row(component: _EpisodeComponent, row: dict[str, Any]) -> None:
        component.raw_count += 1
        component.last_epoch = max(component.last_epoch, finite(row["entry_epoch"]))
        component.executable_sum += finite(row["executable_pips"])
        component.net_sum += finite(row["net_pips"])
        component.spread_sum += finite(row["spread_pips"])
        component.favorable_sum += finite(row.get("max_favorable_pips"))
        component.adverse_sum += finite(row.get("max_adverse_pips"))
        target_time = row.get("configured_target_hit_sec")
        stop_time = row.get("configured_stop_hit_sec")
        component.target_before_stop_sum += float(
            target_time is not None
            and (stop_time is None or finite(target_time) < finite(stop_time))
        )
        if component.factors is None:
            component.factors = set()
        component.factors.update(str(value) for value in row.get("factors") or ())
        key = (finite(row["entry_epoch"]), str(row["event_id"]))
        if key < component.first_key:
            component.first_key = key
            component.first_row = row
        connection.execute(
            "INSERT INTO scratch_episode_events VALUES (?,?)",
            (component.component_id, str(row["event_id"])),
        )

    def merge(left_id: int, right_id: int) -> int:
        if left_id == right_id:
            return left_id
        left, right = components[left_id], components[right_id]
        if left.raw_count < right.raw_count:
            left_id, right_id = right_id, left_id
            left, right = right, left
        connection.execute(
            "UPDATE scratch_episode_events SET component_id=? WHERE component_id=?",
            (left_id, right_id),
        )
        left.raw_count += right.raw_count
        left.executable_sum += right.executable_sum
        left.net_sum += right.net_sum
        left.spread_sum += right.spread_sum
        left.favorable_sum += right.favorable_sum
        left.adverse_sum += right.adverse_sum
        left.target_before_stop_sum += right.target_before_stop_sum
        left.last_epoch = max(left.last_epoch, right.last_epoch)
        if right.first_key < left.first_key:
            left.first_key, left.first_row = right.first_key, right.first_row
        if left.factors is None:
            left.factors = set()
        left.factors.update(right.factors or ())
        del components[right_id]
        versions[left_id] = versions.get(left_id, 0) + 1
        versions.pop(right_id, None)
        for factor, (epoch, component_id) in list(latest_by_factor.items()):
            if component_id == right_id:
                latest_by_factor[factor] = (epoch, left_id)
        return left_id

    def finalize(component_id: int) -> None:
        component = components.pop(component_id)
        ids = (
            str(value[0])
            for value in connection.execute(
                "SELECT event_id FROM scratch_episode_events "
                "WHERE component_id=? ORDER BY event_id",
                (component_id,),
            )
        )
        event_hash, _first_event_id = _stream_json_list_sha256(ids)
        count = max(1, component.raw_count)
        first = component.first_row
        connection.execute(
            "INSERT INTO scratch_episodes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "episode_" + event_hash[:24],
                component.first_key[0],
                str(first["entry_day"]),
                str(first.get("entry_week") or first["entry_day"]),
                str(first["instrument"]),
                str(first["session"]),
                str(first["liquidity"]),
                component.net_sum / count,
                component.executable_sum / count,
                component.spread_sum / count,
                component.favorable_sum / count,
                component.adverse_sum / count,
                component.target_before_stop_sum / count,
                component.raw_count,
                len(component.factors or ()),
            ),
        )
        connection.execute(
            "DELETE FROM scratch_episode_events WHERE component_id=?",
            (component_id,),
        )
        versions.pop(component_id, None)

    def finalize_expired(current_epoch: float) -> None:
        threshold = current_epoch - gap
        while expiry_heap and expiry_heap[0][0] < threshold:
            last_epoch, component_id, version = heapq.heappop(expiry_heap)
            component = components.get(component_id)
            if component is None or versions.get(component_id) != version:
                continue
            if component.last_epoch != last_epoch or component.last_epoch >= threshold:
                continue
            finalize(component_id)

    for row in rows.iter_ordered("entry_epoch,event_id"):
        raw_n += 1
        raw_net_sum += finite(row["net_pips"])
        instruments.add(str(row["instrument"]))
        sessions.add(str(row["session"]))
        liquidities.add(str(row["liquidity"]))
        epoch = finite(row["entry_epoch"])
        finalize_expired(epoch)
        factors = {str(value) for value in row.get("factors") or ()}
        roots = {
            component_id
            for factor in factors
            for latest_epoch, component_id in [latest_by_factor.get(factor, (-math.inf, -1))]
            if component_id in components and epoch - latest_epoch <= gap
        }
        if roots:
            root = min(roots)
            for candidate in sorted(roots - {root}):
                root = merge(root, candidate)
        else:
            next_component += 1
            root = next_component
            components[root] = _EpisodeComponent(
                component_id=root,
                first_key=(epoch, str(row["event_id"])),
                first_row=row,
                last_epoch=epoch,
                factors=set(),
            )
            versions[root] = 0
        add_row(components[root], row)
        versions[root] = versions.get(root, 0) + 1
        heapq.heappush(
            expiry_heap, (components[root].last_epoch, root, versions[root])
        )
        for factor in factors:
            latest_by_factor[factor] = (epoch, root)
    for component_id in sorted(
        components,
        key=lambda value: components[value].first_key,
    ):
        finalize(component_id)
    connection.execute(
        "CREATE INDEX IF NOT EXISTS scratch_episodes_order "
        "ON scratch_episodes(entry_epoch,episode_id)"
    )
    connection.commit()
    return raw_n, raw_net_sum, instruments, sessions, liquidities


def _ordered_episode_rows(connection: sqlite3.Connection) -> Iterator[tuple[Any, ...]]:
    yield from connection.execute(
        "SELECT entry_day,entry_week,instrument,session,liquidity,net_pips,"
        "executable_pips,spread_pips,max_favorable_pips,max_adverse_pips,"
        "target_before_stop FROM scratch_episodes ORDER BY entry_epoch,episode_id"
    )


def summarize_spilled_rows(
    rows: SpilledRows,
    *,
    horizon_sec: int,
    slippage_pips: float,
) -> dict[str, Any]:
    governance = read_json(DEFAULT_GOVERNANCE_CONFIG)
    raw_n, raw_net_sum, instruments, sessions, liquidities = (
        _materialize_spilled_episodes(rows, horizon_sec)
    )
    connection = rows.connection
    episode_rows = list(_ordered_episode_rows(connection))
    # Episode rows are already compact tuples and represent effective, not raw,
    # observations.  Keeping only this reduced surface preserves exact Python
    # floating/statistical semantics while avoiding per-outcome dict retention.
    values = array("d", (finite(row[5]) for row in episode_rows))
    executable_values = array("d", (finite(row[6]) for row in episode_rows))
    effective_n = len(values)
    day_values: dict[str, list[float]] = defaultdict(list)
    for row in episode_rows:
        day_values[str(row[0])].append(finite(row[5]))
    block_averages = [statistics.fmean(sample) for sample in day_values.values()]
    positive_blocks = sum(value > 0.0 for value in block_averages)

    def sql_order_stat(probability: float) -> float:
        if not effective_n:
            return 0.0
        position = max(0.0, min(1.0, probability)) * (effective_n - 1)
        lower, upper = int(math.floor(position)), int(math.ceil(position))
        requested = sorted({lower, upper})
        found = {
            offset: finite(
                connection.execute(
                    "SELECT net_pips FROM scratch_episodes "
                    "ORDER BY net_pips LIMIT 1 OFFSET ?", (offset,)
                ).fetchone()[0]
            )
            for offset in requested
        }
        if lower == upper:
            return found[lower]
        weight = position - lower
        return found[lower] * (1.0 - weight) + found[upper] * weight

    p05 = sql_order_stat(0.05)
    median_value = sql_order_stat(0.5)
    tail = [value for value in values if value <= p05]
    shock_results: dict[str, Any] = {}
    for total_slippage in (0.0, 0.25, 0.5, 1.0):
        shocked = array("d", (value - total_slippage for value in executable_values))
        gains = sum(value for value in shocked if value > 0.0)
        losses = -sum(value for value in shocked if value < 0.0)
        factor = 999.0 if losses <= 1e-12 and gains > 0 else (
            0.0 if losses <= 1e-12 else gains / losses
        )
        shock_results[f"total_slippage_{total_slippage:.2f}_pips"] = {
            "avg_net_pips": round(statistics.fmean(shocked), 6) if shocked else 0.0,
            "profit_factor": round(factor, 6),
            "win_rate": round(sum(value > 0.0 for value in shocked) / len(shocked), 6)
            if shocked else 0.0,
        }
    first_liquidity = str(episode_rows[0][4]) if episode_rows else "wide_gt_5"
    minimum_edge = minimum_economic_edge(horizon_sec, first_liquidity, governance)
    sequential = governance.get("sequential") or {}
    bound = horizon_value(
        sequential.get("clip_pips_by_max_horizon_sec") or {}, horizon_sec, 250.0
    )
    clipped = array("d", (max(-bound, min(bound, value)) for value in values))
    if clipped:
        count = len(clipped)
        alpha_n = max(
            1e-15,
            finite(sequential.get("alpha"), 0.01)
            * 6.0 / (math.pi * math.pi * count * count),
        )
        radius = 2.0 * bound * math.sqrt(math.log(1.0 / alpha_n) / (2.0 * count))
        clipped_mean = statistics.fmean(clipped)
        sequential_lcb, sequential_ucb = clipped_mean - radius, clipped_mean + radius
    else:
        sequential_lcb, sequential_ucb = -bound, bound
    standard_deviation = statistics.stdev(values) if effective_n >= 2 else 0.0
    ordinary_radius = (
        1.645 * standard_deviation / math.sqrt(effective_n)
        if effective_n >= 2 else math.inf
    )
    z_total = 2.487
    required_n = (
        int(math.ceil((z_total * standard_deviation / max(1e-9, minimum_edge)) ** 2))
        if standard_deviation > 0.0 else 0
    )
    mde = z_total * standard_deviation / math.sqrt(effective_n) if values else math.inf

    contributions: dict[str, dict[str, float]] = {
        name: defaultdict(float) for name in ("instrument", "entry_day", "entry_week", "session")
    }
    totals: dict[str, dict[str, list[float]]] = {
        name: defaultdict(lambda: [0.0, 0.0])
        for name in ("instrument", "entry_day", "entry_week", "session")
    }
    currency_positive: dict[str, float] = defaultdict(float)
    currency_totals: dict[str, list[float]] = defaultdict(lambda: [0.0, 0.0])
    mfe_sum = mae_sum = target_sum = 0.0
    for row in episode_rows:
        day, week, instrument, session = map(str, row[:4])
        net = finite(row[5])
        for name, key in (
            ("instrument", instrument), ("entry_day", day),
            ("entry_week", week), ("session", session),
        ):
            contributions[name][key] += max(0.0, net)
            totals[name][key][0] += net
            totals[name][key][1] += 1.0
        parts = instrument.split("_")
        if len(parts) == 2:
            for currency in parts:
                currency_positive[currency] += max(0.0, net) * 0.5
                currency_totals[currency][0] += net
                currency_totals[currency][1] += 1.0
        mfe_sum += finite(row[8])
        mae_sum += finite(row[9])
        target_sum += finite(row[10])
    total_positive = sum(max(0.0, value) for value in values)
    total_net = sum(values)

    def concentration(name: str) -> float:
        sample = contributions[name]
        total = sum(sample.values())
        return max(sample.values(), default=0.0) / total if total > 0.0 else 0.0

    def removal(name: str) -> dict[str, Any]:
        sample = contributions[name]
        removed = max(sample, key=sample.get) if sample else None
        excluded = totals[name].get(str(removed), [0.0, 0.0])
        remaining_n = effective_n - int(excluded[1])
        return {
            "removed": removed,
            "remaining_n": remaining_n,
            "avg_net_pips_after_removal": (
                round((total_net - excluded[0]) / remaining_n, 6)
                if remaining_n else None
            ),
        }

    removed_currency = (
        max(currency_positive, key=currency_positive.get) if currency_positive else None
    )
    currency_excluded = currency_totals.get(str(removed_currency), [0.0, 0.0])
    currency_remaining_n = effective_n - int(currency_excluded[1])

    def top_removal(fraction: float) -> dict[str, Any]:
        take = max(1, int(math.ceil(effective_n * fraction))) if effective_n else 0
        ordered = sorted(values, reverse=True)
        remaining = ordered[take:]
        return {
            "removed_n": take,
            "remaining_n": len(remaining),
            "avg_net_pips_after_removal": (
                round(statistics.fmean(remaining), 6) if remaining else None
            ),
        }

    positive_sorted = sorted((value for value in values if value > 0.0), reverse=True)
    top_take = max(1, int(math.ceil(effective_n * 0.05))) if effective_n else 0
    gains = sum(value for value in values if value > 0.0)
    losses = -sum(value for value in values if value < 0.0)
    pf = 999.0 if losses <= 1e-12 and gains > 0 else (
        0.0 if losses <= 1e-12 else gains / losses
    )
    clipped_mean = statistics.fmean(clipped) if clipped else 0.0

    return {
        "raw_n": raw_n,
        "effective_n": effective_n,
        "deduplication_ratio": round(effective_n / raw_n, 6) if raw_n else 0.0,
        "pair_count": len(instruments),
        "session_count": len(sessions),
        "liquidity_bucket_count": len(liquidities),
        "day_count": len(day_values),
        "avg_raw_net_pips": round(raw_net_sum / raw_n, 6) if raw_n else 0.0,
        "avg_net_pips": round(statistics.fmean(values), 6) if values else 0.0,
        "median_net_pips": round(median_value, 6),
        "sample_std_pips": round(standard_deviation, 6),
        "minimum_economic_edge_pips": round(minimum_edge, 6),
        "minimum_detectable_edge_pips": round(mde, 6) if math.isfinite(mde) else None,
        "additional_effective_episodes_for_power": max(0, required_n - effective_n),
        "one_sided_pvalue_zero_bounded": round(
            1.0 if not clipped or clipped_mean <= 0.0
            else min(1.0, math.exp(-effective_n * clipped_mean * clipped_mean / (2.0 * bound * bound))),
            12,
        ),
        "one_sided_pvalue_minimum_edge_bounded": round(
            1.0 if not clipped or clipped_mean - minimum_edge <= 0.0
            else min(1.0, math.exp(-effective_n * (clipped_mean - minimum_edge) ** 2 / (2.0 * bound * bound))),
            12,
        ),
        "unadjusted_upper_confidence_pips": (
            round(statistics.fmean(values) + ordinary_radius, 6)
            if values and math.isfinite(ordinary_radius) else None
        ),
        "time_uniform_lower_bound_pips": round(sequential_lcb, 6),
        "time_uniform_upper_bound_pips": round(sequential_ucb, 6),
        "sequential_method": str(
            sequential.get("method") or "alpha_spending_hoeffding_clipped_mean_v1"
        ),
        "sequential_clip_bound_pips": round(bound, 6),
        "win_rate": round(sum(value > 0.0 for value in values) / effective_n, 6)
        if values else 0.0,
        "cost_clearance_probability": round(
            sum(value > 0.0 for value in values) / effective_n, 6
        ) if values else 0.0,
        "average_max_favorable_pips": round(mfe_sum / effective_n, 6) if effective_n else 0.0,
        "average_max_adverse_pips": round(mae_sum / effective_n, 6) if effective_n else 0.0,
        "mfe_to_mae_ratio": round((mfe_sum / effective_n) / max(1e-9, mae_sum / effective_n), 6)
        if effective_n else 0.0,
        "target_before_stop_rate": round(target_sum / effective_n, 6) if effective_n else 0.0,
        "profit_factor": round(pf, 6),
        "lower_confidence_pips": round(
            -999.0 if effective_n < 2
            else statistics.fmean(values) - 1.645 * standard_deviation / math.sqrt(effective_n),
            6,
        ),
        "maximum_drawdown_pips": round(maximum_drawdown(values), 6),
        "p05_net_pips": round(p05, 6),
        "expected_shortfall_pips": round(statistics.fmean(tail), 6) if tail else 0.0,
        "time_block_count": len(block_averages),
        "positive_time_block_fraction": round(positive_blocks / len(block_averages), 6)
        if block_averages else 0.0,
        "best_pair_profit_share": round(concentration("instrument"), 6),
        "best_day_profit_share": round(concentration("entry_day"), 6),
        "best_session_profit_share": round(concentration("session"), 6),
        "best_week_profit_share": round(concentration("entry_week"), 6),
        "best_currency_profit_share": round(
            max(currency_positive.values(), default=0.0) / sum(currency_positive.values())
            if sum(currency_positive.values()) > 0.0 else 0.0,
            6,
        ),
        "best_five_percent_profit_share": round(
            sum(positive_sorted[:top_take]) / total_positive if total_positive > 0.0 else 0.0,
            6,
        ),
        "concentration_removal_stress": {
            "best_pair": removal("instrument"),
            "best_day": removal("entry_day"),
            "best_week": removal("entry_week"),
            "best_session": removal("session"),
            "best_currency": {
                "removed": removed_currency,
                "remaining_n": currency_remaining_n,
                "avg_net_pips_after_removal": (
                    round((total_net - currency_excluded[0]) / currency_remaining_n, 6)
                    if currency_remaining_n else None
                ),
            },
            "best_episode": top_removal(1.0 / max(1, effective_n)),
            "best_five_percent": top_removal(0.05),
        },
        "best_episode_profit_share": round(
            max((max(0.0, value) for value in values), default=0.0) / max(1e-12, total_positive),
            6,
        ) if values else 0.0,
        "modeled_slippage_pips": float(slippage_pips),
        "cost_shocks": shock_results,
    }


def summarize_rows(
    rows: list[dict[str, Any]] | SpilledRows,
    *,
    horizon_sec: int,
    slippage_pips: float,
) -> dict[str, Any]:
    if isinstance(rows, SpilledRows):
        return summarize_spilled_rows(
            rows, horizon_sec=horizon_sec, slippage_pips=slippage_pips
        )
    governance = read_json(DEFAULT_GOVERNANCE_CONFIG)
    episodes = collapse_factor_episodes(rows, horizon_sec)
    values = [finite(row["net_pips"]) for row in episodes]
    raw_values = [finite(row["net_pips"]) for row in rows]
    day_values: dict[str, list[float]] = defaultdict(list)
    for row in episodes:
        day_values[row["entry_day"]].append(finite(row["net_pips"]))
    block_averages = [statistics.fmean(values) for values in day_values.values()]
    positive_blocks = sum(value > 0.0 for value in block_averages)
    p05 = percentile(values, 0.05)
    tail = [value for value in values if value <= p05]
    shock_results = {}
    for total_slippage in (0.0, 0.25, 0.5, 1.0):
        shocked = [
            finite(row["executable_pips"]) - total_slippage for row in episodes
        ]
        shock_results[f"total_slippage_{total_slippage:.2f}_pips"] = {
            "avg_net_pips": round(statistics.fmean(shocked), 6) if shocked else 0.0,
            "profit_factor": round(profit_factor(shocked), 6),
            "win_rate": round(sum(value > 0.0 for value in shocked) / len(shocked), 6)
            if shocked
            else 0.0,
        }
    minimum_edge = minimum_economic_edge(
        horizon_sec, str(rows[0].get("liquidity") if rows else "wide_gt_5"), governance
    )
    sequential = governance.get("sequential") or {}
    bound = horizon_value(
        sequential.get("clip_pips_by_max_horizon_sec") or {}, horizon_sec, 250.0
    )
    sequential_lcb, sequential_ucb = alpha_spending_confidence_sequence(
        values, alpha=finite(sequential.get("alpha"), 0.01), bound=bound
    )
    standard_deviation = statistics.stdev(values) if len(values) >= 2 else 0.0
    ordinary_radius = (
        1.645 * standard_deviation / math.sqrt(len(values)) if len(values) >= 2 else math.inf
    )
    power = governance.get("power") or {}
    z_total = 2.487  # one-sided alpha=.05 plus 80% power; frozen in config v1
    required_n = (
        int(math.ceil((z_total * standard_deviation / max(1e-9, minimum_edge)) ** 2))
        if standard_deviation > 0.0
        else 0
    )
    mde = z_total * standard_deviation / math.sqrt(len(values)) if values else math.inf
    return {
        "raw_n": len(rows),
        "effective_n": len(episodes),
        "deduplication_ratio": round(len(episodes) / len(rows), 6) if rows else 0.0,
        "pair_count": len({row["instrument"] for row in rows}),
        "session_count": len({row["session"] for row in rows}),
        "liquidity_bucket_count": len({row["liquidity"] for row in rows}),
        "day_count": len(day_values),
        "avg_raw_net_pips": round(statistics.fmean(raw_values), 6) if raw_values else 0.0,
        "avg_net_pips": round(statistics.fmean(values), 6) if values else 0.0,
        "median_net_pips": round(statistics.median(values), 6) if values else 0.0,
        "sample_std_pips": round(standard_deviation, 6),
        "minimum_economic_edge_pips": round(minimum_edge, 6),
        "minimum_detectable_edge_pips": round(mde, 6) if math.isfinite(mde) else None,
        "additional_effective_episodes_for_power": max(0, required_n - len(values)),
        "one_sided_pvalue_zero_bounded": round(
            clipped_mean_pvalue(values, 0.0, bound), 12
        ),
        "one_sided_pvalue_minimum_edge_bounded": round(
            clipped_mean_pvalue(values, minimum_edge, bound), 12
        ),
        "unadjusted_upper_confidence_pips": round(
            statistics.fmean(values) + ordinary_radius, 6
        ) if values and math.isfinite(ordinary_radius) else None,
        "time_uniform_lower_bound_pips": round(sequential_lcb, 6),
        "time_uniform_upper_bound_pips": round(sequential_ucb, 6),
        "sequential_method": str(
            sequential.get("method") or "alpha_spending_hoeffding_clipped_mean_v1"
        ),
        "sequential_clip_bound_pips": round(bound, 6),
        "win_rate": round(sum(value > 0.0 for value in values) / len(values), 6)
        if values
        else 0.0,
        "cost_clearance_probability": round(
            sum(value > 0.0 for value in values) / len(values), 6
        ) if values else 0.0,
        "average_max_favorable_pips": round(
            statistics.fmean(finite(row.get("max_favorable_pips")) for row in episodes), 6
        ) if episodes else 0.0,
        "average_max_adverse_pips": round(
            statistics.fmean(finite(row.get("max_adverse_pips")) for row in episodes), 6
        ) if episodes else 0.0,
        "mfe_to_mae_ratio": round(
            statistics.fmean(finite(row.get("max_favorable_pips")) for row in episodes)
            / max(1e-9, statistics.fmean(finite(row.get("max_adverse_pips")) for row in episodes)),
            6,
        ) if episodes else 0.0,
        "target_before_stop_rate": round(
            statistics.fmean(finite(row.get("target_before_stop")) for row in episodes), 6
        ) if episodes else 0.0,
        "profit_factor": round(profit_factor(values), 6),
        "lower_confidence_pips": round(lower_confidence(values), 6),
        "maximum_drawdown_pips": round(maximum_drawdown(values), 6),
        "p05_net_pips": round(p05, 6),
        "expected_shortfall_pips": round(statistics.fmean(tail), 6) if tail else 0.0,
        "time_block_count": len(block_averages),
        "positive_time_block_fraction": round(
            positive_blocks / len(block_averages), 6
        )
        if block_averages
        else 0.0,
        "best_pair_profit_share": round(positive_concentration(episodes, "instrument"), 6),
        "best_day_profit_share": round(positive_concentration(episodes, "entry_day"), 6),
        "best_session_profit_share": round(positive_concentration(episodes, "session"), 6),
        "best_week_profit_share": round(positive_concentration(episodes, "entry_week"), 6),
        "best_currency_profit_share": round(currency_profit_concentration(episodes), 6),
        "best_five_percent_profit_share": round(top_fraction_profit_share(values), 6),
        "concentration_removal_stress": {
            "best_pair": removal_average(episodes, "instrument"),
            "best_day": removal_average(episodes, "entry_day"),
            "best_week": removal_average(episodes, "entry_week"),
            "best_session": removal_average(episodes, "session"),
            "best_currency": currency_removal_average(episodes),
            "best_episode": top_fraction_removal_average(values, 1.0 / max(1, len(values))),
            "best_five_percent": top_fraction_removal_average(values, 0.05),
        },
        "best_episode_profit_share": round(
            max((max(0.0, value) for value in values), default=0.0)
            / max(1e-12, sum(max(0.0, value) for value in values)),
            6,
        )
        if values
        else 0.0,
        "modeled_slippage_pips": float(slippage_pips),
        "cost_shocks": shock_results,
    }


def gate_distances(
    summary: dict[str, Any], thresholds: EvidenceThresholds
) -> list[dict[str, Any]]:
    shock_one = (
        summary.get("cost_shocks", {})
        .get("total_slippage_1.00_pips", {})
        .get("avg_net_pips", -999.0)
    )
    specs = (
        ("effective_sample_size", summary["effective_n"], thresholds.minimum_effective_n, "min"),
        (
            "after_cost_expectancy",
            summary["avg_net_pips"],
            thresholds.minimum_average_net_pips,
            "min",
        ),
        (
            "profit_factor",
            summary["profit_factor"],
            thresholds.minimum_profit_factor,
            "min",
        ),
        (
            "lower_confidence_bound",
            summary["lower_confidence_pips"],
            thresholds.minimum_lower_confidence_pips,
            "min",
        ),
        ("time_block_coverage", summary["time_block_count"], thresholds.minimum_time_blocks, "min"),
        (
            "time_block_stability",
            summary["positive_time_block_fraction"],
            thresholds.minimum_positive_time_block_fraction,
            "min",
        ),
        (
            "one_pip_slippage_stress",
            shock_one,
            thresholds.maximum_average_net_at_one_pip_slippage,
            "min",
        ),
        (
            "episode_profit_concentration",
            summary["best_episode_profit_share"],
            thresholds.maximum_best_episode_profit_share,
            "max",
        ),
    )
    output = []
    for name, observed, required, mode in specs:
        observed_value, required_value = finite(observed), finite(required)
        passed = observed_value >= required_value if mode == "min" else observed_value <= required_value
        distance = (
            observed_value - required_value
            if mode == "min"
            else required_value - observed_value
        )
        output.append(
            {
                "gate": name,
                "observed": round(observed_value, 6),
                "required": round(required_value, 6),
                "distance": round(distance, 6),
                "passed": passed,
            }
        )
    return output


def source_table(connection: sqlite3.Connection) -> tuple[str, str]:
    tables = {
        row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    if "canonical_outcomes" in tables:
        if connection.execute(
            "SELECT 1 FROM canonical_outcomes LIMIT 1"
        ).fetchone():
            return "canonical_outcomes", "immutable_trigger_enforced"
    return "outcomes", "legacy_insert_only_not_trigger_enforced"


def load_rows(
    database_path: Path,
    *,
    slippage_pips: float,
    maximum_delay_sec: float,
    maximum_row_id: int | None = None,
    progress_callback: ProgressCallback | None = None,
    bounded_memory: bool = False,
    forecast_contract_cache: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]] | SpilledRows, dict[str, Any]]:
    emit_progress(
        progress_callback,
        "inventorying_source",
        outcome_memory_mode=(
            "disk_spooled_group_at_a_time_v1" if bounded_memory else "legacy_in_memory"
        ),
    )
    # Freeze immutable high-waters in one brief read transaction, then close
    # it before any multi-million-row parsing or spooling begins.  All large
    # scans below reopen for one bounded rowid page at a time.
    connection = _open_snapshot_reader(database_path)
    connection.execute("BEGIN")
    table, immutability = source_table(connection)
    tables = {
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }
    source_columns = {
        str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})")
    }
    requested_ceiling = (
        None if maximum_row_id is None else max(0, int(maximum_row_id))
    )
    maximum_row = int(
        connection.execute(
            f"SELECT COALESCE(MAX(row_id),0) FROM \"{table}\""
            + (" WHERE row_id<=?" if requested_ceiling is not None else ""),
            (() if requested_ceiling is None else (requested_ceiling,)),
        ).fetchone()[0]
    )
    forecast_snapshot_highwater = (
        int(
            connection.execute(
                "SELECT COALESCE(MAX(rowid),0) FROM canonical_forecasts"
            ).fetchone()[0]
        )
        if "canonical_forecasts" in tables
        else 0
    )
    forecast_cache_identity = (
        forecast_contract_cache_identity(
            connection,
            database_path,
        )
        if "canonical_forecasts" in tables and forecast_snapshot_highwater
        else None
    )
    integrity_snapshot_highwater = (
        int(
            connection.execute(
                "SELECT COALESCE(MAX(rowid),0) FROM forecast_integrity_events"
            ).fetchone()[0]
        )
        if "forecast_integrity_events" in tables
        else 0
    )
    connection.rollback()
    connection.close()
    total_rows = count_immutable_snapshot_rows(
        database_path,
        table=table,
        maximum_rowid=maximum_row,
        rowid_column="row_id",
    )
    emit_progress(
        progress_callback,
        "inventorying_source",
        source_rows=int(total_rows),
        source_highwater_row_id=int(maximum_row),
    )
    # The canonical table enforces UNIQUE(event_id,horizon_sec), so this exact
    # diagnostic is zero without an expensive GROUP BY reader.  Legacy test or
    # migration databases retain the original calculation.
    if immutability == "immutable_trigger_enforced":
        duplicate_count = 0
    else:
        connection = _open_snapshot_reader(database_path)
        try:
            duplicate_count = int(
                connection.execute(
                    f"""
                    SELECT COALESCE(SUM(n - 1), 0) FROM (
                        SELECT COUNT(*) AS n FROM \"{table}\"
                        WHERE row_id<=?
                        GROUP BY event_id, horizon_sec HAVING COUNT(*) > 1
                    )
                    """,
                    (maximum_row,),
                ).fetchone()[0]
            )
        finally:
            connection.close()
    path_expressions = [
        name if name in source_columns else "NULL AS " + name
        for name in (
            "max_favorable_pips",
            "max_adverse_pips",
            "first_positive_sec",
            "configured_stop_hit_sec",
            "configured_target_hit_sec",
        )
    ]
    diagnostics_expression = (
        "diagnostics_json"
        if "diagnostics_json" in source_columns
        else "NULL AS diagnostics_json"
    )
    outcome_columns = (
        "row_id,event_id,horizon_sec,observed_utc,lane_id,family,profile,"
        "instrument,direction,theoretical_pips,outcome_delay_sec,entry_time,"
        f"entry_spread_pips,{', '.join(path_expressions)},{diagnostics_expression}"
    )
    rows: list[dict[str, Any]] | SpilledRows = (
        SpilledRows.create() if bounded_memory else []
    )
    spool_batch: list[tuple[Any, ...]] = []
    spool_id = 0
    late = invalid_time = pre_boundary = invalid_boundary_time = 0
    scanned = 0
    source_batches = iter_immutable_snapshot_batches(
        database_path,
        table=table,
        columns=outcome_columns,
        maximum_rowid=maximum_row,
        where_sql="kind='signal'",
        batch_size=25_000,
        rowid_column="row_id",
    )
    for values in (
        values for source_batch in source_batches for values in source_batch
    ):
        scanned += 1
        if scanned % 50_000 == 0:
            emit_progress(
                progress_callback,
                "loading_rows",
                rows_scanned=scanned,
                usable_signal_outcomes=len(rows),
                late_outcomes_excluded=late,
                pre_boundary_outcomes_excluded=pre_boundary,
                invalid_boundary_times_excluded=invalid_boundary_time,
                invalid_entry_times_excluded=invalid_time,
                source_rows=int(total_rows),
                source_highwater_row_id=int(maximum_row),
            )
        horizon = int(values[2])
        delay = finite(values[10])
        permitted_delay = min(maximum_delay_sec, max(15.0, horizon * 0.10))
        if delay > permitted_delay:
            late += 1
            continue
        diagnostics: dict[str, Any] = {}
        if values[18]:
            try:
                candidate = json.loads(str(values[18]))
                if isinstance(candidate, dict):
                    diagnostics = candidate
            except (TypeError, ValueError, json.JSONDecodeError):
                diagnostics = {}
        forecast_recorded = parse_time(diagnostics.get("forecast_recorded_utc"))
        snapshot_generated = parse_time(
            diagnostics.get("quote_snapshot_generated_utc")
        )
        if diagnostics.get("maturity_worker") == "canonical_quote_snapshot_v1":
            if forecast_recorded is None or snapshot_generated is None:
                invalid_boundary_time += 1
                continue
            boundary_delay = (
                snapshot_generated
                - forecast_recorded
                - timedelta(seconds=horizon)
            ).total_seconds()
            if boundary_delay < 0.0:
                pre_boundary += 1
                continue
            if boundary_delay > maximum_delay_sec:
                late += 1
                continue
            # Trust the actual snapshot boundary, not the older wall-clock
            # delay field written before the boundary contract was hardened.
            delay = boundary_delay
        parsed = forecast_recorded or parse_time(values[11])
        if parsed is None:
            invalid_time += 1
            continue
        executable = finite(values[9])
        instrument, direction = str(values[7]), str(values[8])
        row = {
                "row_id": int(values[0]),
                "event_id": str(values[1]),
                "horizon_sec": horizon,
                "observed_utc": str(values[3]),
                "lane_id": str(values[4]),
                "family": str(values[5]),
                "profile": str(values[6]),
                "instrument": instrument,
                "direction": direction,
                "executable_pips": executable,
                "net_pips": executable - slippage_pips,
                "outcome_delay_sec": delay,
                "entry_time": str(values[11]),
                "entry_epoch": parsed.timestamp(),
                "entry_day": parsed.date().isoformat(),
                "entry_week": f"{parsed.isocalendar().year}-W{parsed.isocalendar().week:02d}",
                "spread_pips": finite(values[12]),
                "session": session_bucket(parsed),
                "liquidity": liquidity_bucket(values[12]),
                "factors": signed_currency_factors(instrument, direction),
                "max_favorable_pips": finite(values[13]),
                "max_adverse_pips": finite(values[14]),
                "first_positive_sec": (
                    None if values[15] is None else finite(values[15])
                ),
                "configured_stop_hit_sec": (
                    None if values[16] is None else finite(values[16])
                ),
                "configured_target_hit_sec": (
                    None if values[17] is None else finite(values[17])
                ),
            }
        if isinstance(rows, SpilledRows):
            spool_id += 1
            spool_batch.append(
                (
                    spool_id,
                    row["row_id"], row["event_id"], row["horizon_sec"],
                    row["entry_time"], row["entry_epoch"], row["entry_day"],
                    row["entry_week"], row["family"], row["instrument"],
                    row["direction"], row["executable_pips"], row["net_pips"],
                    row["spread_pips"], row["session"], row["liquidity"],
                    row["max_favorable_pips"], row["max_adverse_pips"],
                    row["first_positive_sec"], row["configured_stop_hit_sec"],
                    row["configured_target_hit_sec"],
                    strategy_archetype(str(row["family"])),
                    int(row["entry_epoch"] // 60),
                )
            )
            if len(spool_batch) >= 10_000:
                rows.append_many(spool_batch)
                spool_batch.clear()
        else:
            rows.append(row)
    if isinstance(rows, SpilledRows):
        if spool_batch:
            rows.append_many(spool_batch)
            spool_batch.clear()
        emit_progress(
            progress_callback,
            "indexing_bounded_outcome_spool",
            usable_signal_outcomes=len(rows),
            outcome_memory_mode="disk_spooled_group_at_a_time_v1",
        )
        rows.finish_loading()
    emit_progress(
        progress_callback,
        "loading_rows_complete",
        rows_scanned=scanned,
        usable_signal_outcomes=len(rows),
        source_rows=int(total_rows),
        source_highwater_row_id=int(maximum_row),
    )
    emit_progress(
        progress_callback,
        "inventorying_forecast_contract",
        inventory_step="resolving_immutable_forecast_contract_snapshot",
    )
    forecast_count = 0
    emit_progress(
        progress_callback,
        "inventorying_forecast_contract",
        inventory_step="grouping_integrity_events",
        forecast_snapshot_highwater_rowid=int(forecast_snapshot_highwater),
    )
    integrity_counts: dict[str, int] = {}
    if "forecast_integrity_events" in tables and integrity_snapshot_highwater:
        for integrity_batch in iter_immutable_snapshot_batches(
            database_path,
            table="forecast_integrity_events",
            columns="rowid,event_type",
            maximum_rowid=integrity_snapshot_highwater,
            batch_size=50_000,
        ):
            for _rowid, event_type in integrity_batch:
                key = str(event_type)
                integrity_counts[key] = integrity_counts.get(key, 0) + 1
    forecast_contract = {
        "captured": forecast_count,
        "tracked_for_outcome": 0,
        "signal": 0,
        "miss": 0,
        "unknown_model_version": 0,
        "unknown_feature_version": 0,
        "duplicate_payloads": 0,
    }
    unavailable_inputs = {
        "forecast_rows_inspected": 0,
        "order_book_available_rows": 0,
        "position_book_available_rows": 0,
        "pricing_depth_informative_rows": 0,
        "policy": "unavailable fields remain excluded from active model eligibility",
    }
    forecast_contract_audit_mode = "exact_snapshot_scan_v1"
    if "canonical_forecasts" in tables and forecast_snapshot_highwater:
        emit_progress(
            progress_callback,
            "inventorying_forecast_contract",
            inventory_step="summarizing_forecast_contract",
            forecast_snapshot_highwater_rowid=int(forecast_snapshot_highwater),
        )
        (
            contract_audit,
            unavailable_audit,
            forecast_contract_resolution,
        ) = resolve_forecast_contract_snapshot(
            database_path,
            maximum_rowid=forecast_snapshot_highwater,
            source_identity=forecast_cache_identity,
            audit_cache=forecast_contract_cache,
            progress_callback=progress_callback,
        )
        forecast_contract_audit_mode = {
            "cache_hit": "immutable_snapshot_cache_hit_v1",
            "incremental_extension": "immutable_snapshot_incremental_extension_v1",
            "full_scan": "exact_snapshot_scan_v1",
        }[forecast_contract_resolution]
        forecast_count = int(contract_audit.get("captured") or 0)
        forecast_contract.update(contract_audit)
        unavailable_inputs.update(unavailable_audit)
    emit_progress(
        progress_callback,
        "inventorying_forecast_contract_complete",
        canonical_forecasts=forecast_count,
        forecast_rows_inspected=unavailable_inputs["forecast_rows_inspected"],
    )
    return rows, {
        "database": str(database_path.resolve()),
        "table": table,
        "immutability": immutability,
        "source_rows": int(total_rows),
        "source_highwater_row_id": int(maximum_row),
        "requested_maximum_row_id": (
            None if maximum_row_id is None else int(maximum_row_id)
        ),
        "usable_signal_outcomes": len(rows),
        "late_outcomes_excluded": late,
        "pre_boundary_outcomes_excluded": pre_boundary,
        "invalid_boundary_times_excluded": invalid_boundary_time,
        "invalid_entry_times_excluded": invalid_time,
        "duplicate_event_horizons": duplicate_count,
        "maximum_outcome_delay_sec": float(maximum_delay_sec),
        "canonical_forecasts": forecast_count,
        "forecast_snapshot_highwater_rowid": int(forecast_snapshot_highwater),
        "forecast_contract_audit_mode": forecast_contract_audit_mode,
        "integrity_snapshot_highwater_rowid": int(integrity_snapshot_highwater),
        "source_read_mode": "immutable_highwater_paged_transactions_v1",
        "forecast_contract": forecast_contract,
        "integrity_event_counts": integrity_counts,
        "unavailable_microstructure_inputs": unavailable_inputs,
    }


def build_archetype_results(
    rows: list[dict[str, Any]] | SpilledRows,
    *,
    thresholds: EvidenceThresholds,
    slippage_pips: float,
) -> list[dict[str, Any]]:
    if isinstance(rows, SpilledRows):
        output: list[dict[str, Any]] = []
        for archetype, horizon in rows.grouped_keys(("archetype", "horizon_sec")):
            members = rows.view(
                "archetype=? AND horizon_sec=?", (str(archetype), int(horizon))
            )
            summary = summarize_rows(
                members, horizon_sec=int(horizon), slippage_pips=slippage_pips
            )
            where = f" WHERE {members.where_sql}" if members.where_sql else ""
            family_count = int(
                members.connection.execute(
                    f"SELECT COUNT(DISTINCT family) FROM {members.table}{where}",
                    members.where_args,
                ).fetchone()[0]
            )
            gates = gate_distances(summary, thresholds)
            output.append(
                {
                    "archetype": str(archetype),
                    "horizon_sec": int(horizon),
                    "family_count": family_count,
                    **summary,
                    "eligible": all(gate["passed"] for gate in gates),
                    "gate_distances": gates,
                }
            )
        output.sort(
            key=lambda row: (
                bool(row["eligible"]),
                finite(row["lower_confidence_pips"], -999.0),
                int(row["effective_n"]),
            ),
            reverse=True,
        )
        return output
    grouped: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(strategy_archetype(str(row["family"])), int(row["horizon_sec"]))].append(
            row
        )
    output = []
    for (archetype, horizon), members in grouped.items():
        summary = summarize_rows(
            members, horizon_sec=horizon, slippage_pips=slippage_pips
        )
        gates = gate_distances(summary, thresholds)
        output.append(
            {
                "archetype": archetype,
                "horizon_sec": horizon,
                "family_count": len({row["family"] for row in members}),
                **summary,
                "eligible": all(gate["passed"] for gate in gates),
                "gate_distances": gates,
            }
        )
    output.sort(
        key=lambda row: (
            bool(row["eligible"]),
            finite(row["lower_confidence_pips"], -999.0),
            int(row["effective_n"]),
        ),
        reverse=True,
    )
    return output


def build_cell_results(
    rows: list[dict[str, Any]] | SpilledRows,
    *,
    thresholds: EvidenceThresholds,
    slippage_pips: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if isinstance(rows, SpilledRows):
        cells: list[dict[str, Any]] = []
        for family, instrument, horizon, session, liquidity in rows.grouped_keys(
            ("family", "instrument", "horizon_sec", "session", "liquidity")
        ):
            members = rows.view(
                "family=? AND instrument=? AND horizon_sec=? AND session=? AND liquidity=?",
                (family, instrument, int(horizon), session, liquidity),
            )
            # Cells are the canonical evaluation unit and are naturally
            # narrow.  Materialize only this one cell to retain the proven
            # legacy calculation byte-for-byte; release it before the next.
            bounded_members = list(members)
            summary = summarize_rows(
                bounded_members,
                horizon_sec=int(horizon),
                slippage_pips=slippage_pips,
            )
            gates = gate_distances(summary, thresholds)
            cells.append(
                {
                    "cell_id": stable_hash(
                        (family, instrument, int(horizon), session, liquidity)
                    ),
                    "family": str(family),
                    "instrument": str(instrument),
                    "horizon_sec": int(horizon),
                    "session": str(session),
                    "liquidity_bucket": str(liquidity),
                    **summary,
                    "eligible": all(gate["passed"] for gate in gates),
                    "gate_distances": gates,
                }
            )
        family_results: list[dict[str, Any]] = []
        for family, horizon in rows.grouped_keys(("family", "horizon_sec")):
            members = rows.view(
                "family=? AND horizon_sec=?", (family, int(horizon))
            )
            summary = summarize_rows(
                members, horizon_sec=int(horizon), slippage_pips=slippage_pips
            )
            gates = gate_distances(summary, thresholds)
            family_results.append(
                {
                    "family": str(family),
                    "horizon_sec": int(horizon),
                    **summary,
                    "eligible": all(gate["passed"] for gate in gates),
                    "gate_distances": gates,
                }
            )
        cells.sort(
            key=lambda row: (
                bool(row["eligible"]),
                finite(row["lower_confidence_pips"], -999.0),
                int(row["effective_n"]),
            ),
            reverse=True,
        )
        family_results.sort(
            key=lambda row: (
                bool(row["eligible"]),
                finite(row["lower_confidence_pips"], -999.0),
                int(row["effective_n"]),
            ),
            reverse=True,
        )
        return cells, family_results
    cell_groups: dict[tuple[str, str, int, str, str], list[dict[str, Any]]] = defaultdict(list)
    family_groups: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        cell_groups[
            (
                row["family"],
                row["instrument"],
                row["horizon_sec"],
                row["session"],
                row["liquidity"],
            )
        ].append(row)
        family_groups[(row["family"], row["horizon_sec"])].append(row)

    cells = []
    for (family, instrument, horizon, session, liquidity), members in cell_groups.items():
        summary = summarize_rows(members, horizon_sec=horizon, slippage_pips=slippage_pips)
        gates = gate_distances(summary, thresholds)
        cells.append(
            {
                "cell_id": stable_hash((family, instrument, horizon, session, liquidity)),
                "family": family,
                "instrument": instrument,
                "horizon_sec": horizon,
                "session": session,
                "liquidity_bucket": liquidity,
                **summary,
                "eligible": all(row["passed"] for row in gates),
                "gate_distances": gates,
            }
        )
    family_results = []
    for (family, horizon), members in family_groups.items():
        summary = summarize_rows(members, horizon_sec=horizon, slippage_pips=slippage_pips)
        gates = gate_distances(summary, thresholds)
        family_results.append(
            {
                "family": family,
                "horizon_sec": horizon,
                **summary,
                "eligible": all(row["passed"] for row in gates),
                "gate_distances": gates,
            }
        )
    cells.sort(
        key=lambda row: (
            bool(row["eligible"]),
            finite(row["lower_confidence_pips"], -999.0),
            int(row["effective_n"]),
        ),
        reverse=True,
    )
    family_results.sort(
        key=lambda row: (
            bool(row["eligible"]),
            finite(row["lower_confidence_pips"], -999.0),
            int(row["effective_n"]),
        ),
        reverse=True,
    )
    return cells, family_results


def incremental_component_tests(
    rows: list[dict[str, Any]] | SpilledRows, slippage_pips: float
) -> list[dict[str, Any]]:
    seed = [row for row in rows if row["family"] == PRIMARY_SEED]
    component_map: dict[tuple[str, str, int, int], list[str]] = defaultdict(list)
    for row in rows:
        if row["family"] not in CONDITIONAL_COMPONENTS:
            continue
        key = (
            row["family"],
            row["instrument"],
            row["horizon_sec"],
            int(row["entry_epoch"] // 60),
        )
        component_map[key].append(row["direction"])
    output = []
    for family in CONDITIONAL_COMPONENTS:
        states: dict[str, list[float]] = defaultdict(list)
        for row in seed:
            key = (
                family,
                row["instrument"],
                row["horizon_sec"],
                int(row["entry_epoch"] // 60),
            )
            directions = component_map.get(key) or []
            state = (
                "aligned"
                if row["direction"] in directions
                else "opposed"
                if directions
                else "absent"
            )
            states[state].append(finite(row["executable_pips"]) - slippage_pips)
        baseline = [finite(row["executable_pips"]) - slippage_pips for row in seed]
        baseline_avg = statistics.fmean(baseline) if baseline else 0.0
        output.append(
            {
                "seed_family": PRIMARY_SEED,
                "component_family": family,
                "method": "same-pair/horizon/entry-minute conditional diagnostic; not causal proof",
                "baseline_n": len(baseline),
                "baseline_avg_net_pips": round(baseline_avg, 6),
                "states": {
                    name: {
                        "n": len(values),
                        "avg_net_pips": round(statistics.fmean(values), 6) if values else 0.0,
                        "incremental_vs_baseline_pips": round(
                            (statistics.fmean(values) if values else 0.0) - baseline_avg, 6
                        ),
                        "profit_factor": round(profit_factor(values), 6),
                    }
                    for name, values in sorted(states.items())
                },
            }
        )
    return output


def _candidate_forecast_metadata(
    database_path: Path, families: set[str]
) -> dict[str, dict[str, Any]]:
    """Load only candidate forecast-time fields; never derive them post-outcome."""

    if not families:
        return {}
    connection = sqlite3.connect(
        f"file:{database_path.resolve().as_posix()}?mode=ro", uri=True, timeout=30.0
    )
    connection.execute("PRAGMA query_only=ON")
    if not connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='canonical_forecasts'"
    ).fetchone():
        connection.close()
        return {}
    placeholders = ",".join("?" for _ in families)
    output: dict[str, dict[str, Any]] = {}
    for event_id, payload_text in connection.execute(
        f"SELECT event_id,forecast_json FROM canonical_forecasts WHERE family IN ({placeholders})",
        tuple(sorted(families)),
    ):
        try:
            payload = json.loads(payload_text)
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict):
            continue
        diagnostics = payload.get("forecast_diagnostics") or {}
        output[str(event_id)] = {
            "signal": diagnostics.get("signal") or {},
            "economic_gates": diagnostics.get("economic_gates") or {},
            "volatility_regime": str(payload.get("volatility_regime") or "unknown"),
            "stop_loss_pips": finite(payload.get("stop_loss_pips")),
            "take_profit_r": finite(payload.get("take_profit_r")),
            "predicted_magnitude_pips": finite(payload.get("predicted_magnitude_pips")),
            "probability_up": payload.get("probability_up"),
        }
    connection.close()
    return output


def _numeric_bucket(value: Any, cuts: tuple[float, ...], labels: tuple[str, ...]) -> str:
    number = finite(value, math.nan)
    if not math.isfinite(number):
        return "unknown"
    for cut, label in zip(cuts, labels):
        if number <= cut:
            return label
    return labels[-1]


def _macro_release_epochs(
    path: Path = DEFAULT_MACRO_SURPRISE_DATABASE,
) -> list[tuple[float, set[str]]]:
    if not path.is_file():
        return []
    connection = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)
    connection.execute("PRAGMA query_only=ON")
    try:
        source_rows = connection.execute(
            "SELECT scheduled_utc,currencies_json FROM macro_release_latest WHERE scheduled_utc<>''"
        ).fetchall()
    except sqlite3.OperationalError:
        source_rows = []
    connection.close()
    output = []
    for scheduled, currencies_json in source_rows:
        parsed = parse_time(scheduled)
        if parsed is None:
            continue
        try:
            currencies = set(json.loads(currencies_json))
        except (TypeError, json.JSONDecodeError):
            currencies = set()
        output.append((parsed.timestamp(), {str(value) for value in currencies}))
    return output


def candidate_condition_results(
    rows: list[dict[str, Any]] | SpilledRows,
    *,
    metadata: dict[str, dict[str, Any]],
    thresholds: EvidenceThresholds,
    slippage_pips: float,
    macro_releases: list[tuple[float, set[str]]] | None = None,
) -> list[dict[str, Any]]:
    """Replicate the primary seed under predeclared forecast-time conditions."""

    seed = sorted(
        (row for row in rows if row["family"] == PRIMARY_SEED),
        key=lambda row: row["entry_epoch"],
    )
    if not seed:
        return []
    previous: dict[tuple[str, str], float] = {}
    episode_map: dict[tuple[int, int], list[dict[str, Any]]] = defaultdict(list)
    for row in seed:
        episode_map[(row["horizon_sec"], int(row["entry_epoch"] // 300))].append(row)
    annotated: list[tuple[dict[str, Any], dict[str, str]]] = []
    release_rows = macro_releases or []
    for row in seed:
        meta = metadata.get(row["event_id"]) or {}
        signal = meta.get("signal") or {}
        squeeze = signal.get("squeeze_width_ratio")
        pos20 = signal.get("pos20")
        key = (row["instrument"], row["direction"])
        prior = previous.get(key)
        previous[key] = row["entry_epoch"]
        repeat = prior is not None and row["entry_epoch"] - prior <= row["horizon_sec"]
        peers = episode_map[(row["horizon_sec"], int(row["entry_epoch"] // 300))]
        confirmations = sum(
            1
            for peer in peers
            if peer["event_id"] != row["event_id"] and row["factors"] & peer["factors"]
        )
        pair_currencies = set(str(row["instrument"]).split("_"))
        relevant_release_deltas = [
            row["entry_epoch"] - epoch
            for epoch, currencies in release_rows
            if not currencies or pair_currencies.intersection(currencies)
        ]
        macro_proximity = "none_within_60m"
        if any(-1800.0 <= delta < 0.0 for delta in relevant_release_deltas):
            macro_proximity = "pre_release_30m"
        elif any(0.0 <= delta <= 3600.0 for delta in relevant_release_deltas):
            macro_proximity = "post_release_60m"
        stop = finite(meta.get("stop_loss_pips"))
        spread = max(0.01, finite(row.get("spread_pips")))
        conditions = {
            "pair": row["instrument"],
            "session": row["session"],
            "liquidity": row["liquidity"],
            "direction": row["direction"],
            "volatility_regime": str(meta.get("volatility_regime") or "unknown"),
            "squeeze_duration_proxy": _numeric_bucket(
                signal.get("volume_ratio_30"), (0.8, 1.3, math.inf),
                ("quiet", "normal", "persistent_activity"),
            ),
            "squeeze_width_ratio": _numeric_bucket(
                squeeze, (0.35, 0.55, math.inf), ("tight", "moderate", "loose")
            ),
            "session_range_location": _numeric_bucket(
                pos20, (0.1, 0.9, math.inf), ("near_low", "middle", "near_high")
            ),
            "spread_percentile_proxy": _numeric_bucket(
                spread, (1.5, 3.0, math.inf), ("low", "middle", "high")
            ),
            "stop_geometry": _numeric_bucket(
                stop / spread if stop else math.nan,
                (2.0, 4.0, math.inf),
                ("tight_le_2x_spread", "medium_2_4x", "wide_gt_4x"),
            ),
            "target_geometry": _numeric_bucket(
                meta.get("take_profit_r"), (1.0, 2.0, math.inf),
                ("le_1r", "one_to_two_r", "gt_2r"),
            ),
            "entry_sequence": "repeat_breakout" if repeat else "first_breakout",
            "cross_pair_factor_confirmation": (
                "none" if confirmations == 0 else "one" if confirmations == 1 else "multiple"
            ),
            "macro_event_proximity": macro_proximity,
        }
        annotated.append((row, conditions))
    groups: dict[tuple[int, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row, conditions in annotated:
        for dimension, value in conditions.items():
            groups[(row["horizon_sec"], dimension, value)].append(row)
    output = []
    for (horizon, dimension, value), members in groups.items():
        summary = summarize_rows(members, horizon_sec=horizon, slippage_pips=slippage_pips)
        gates = gate_distances(summary, thresholds)
        output.append(
            {
                "family": PRIMARY_SEED,
                "horizon_sec": horizon,
                "dimension": dimension,
                "value": value,
                **summary,
                "eligible": all(gate["passed"] for gate in gates),
                "gate_distances": gates,
            }
        )
    output.sort(
        key=lambda row: (
            bool(row["eligible"]),
            finite(row["lower_confidence_pips"], -999.0),
            int(row["effective_n"]),
        ),
        reverse=True,
    )
    return output


def archetype_incremental_tests(
    rows: list[dict[str, Any]] | SpilledRows, slippage_pips: float
) -> list[dict[str, Any]]:
    """Measure selection lift when two distinct archetypes agree.

    This is a conditional diagnostic, not a fitted ensemble. Same-archetype
    multiplicity is collapsed before comparisons, so indicator variants cannot
    manufacture consensus.
    """

    if isinstance(rows, SpilledRows):
        baseline_values: dict[str, array] = defaultdict(lambda: array("d"))
        for row in rows:
            baseline_values[str(row["archetype"])].append(finite(row["net_pips"]))
        connection = rows.connection
        connection.execute("DROP TABLE IF EXISTS scratch_archetype_agreements")
        connection.execute(
            "CREATE TABLE scratch_archetype_agreements AS "
            "SELECT *,CAST('' AS TEXT) AS left_archetype,"
            "CAST('' AS TEXT) AS right_archetype FROM bounded_rows WHERE 0"
        )
        insert_columns = ",".join((*_SPOOL_COLUMNS, "left_archetype", "right_archetype"))
        insert_sql = (
            f"INSERT INTO scratch_archetype_agreements ({insert_columns}) VALUES ("
            + ",".join("?" for _ in range(len(_SPOOL_COLUMNS) + 2)) + ")"
        )
        pending: list[tuple[Any, ...]] = []
        current_key: tuple[Any, ...] | None = None
        current_members: list[dict[str, Any]] = []

        def flush_group(members: list[dict[str, Any]]) -> None:
            if not members:
                return
            by_archetype: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(
                lambda: defaultdict(list)
            )
            for member in members:
                by_archetype[str(member["archetype"])][str(member["direction"])].append(member)
            names = sorted(by_archetype)
            for index, left in enumerate(names):
                for right in names[index + 1 :]:
                    for direction in sorted(set(by_archetype[left]) & set(by_archetype[right])):
                        representative = by_archetype[left][direction][0]
                        pending.append(
                            tuple(representative[name] for name in _SPOOL_COLUMNS)
                            + (left, right)
                        )

        for row in rows.iter_ordered(
            "instrument,horizon_sec,entry_minute,spool_id"
        ):
            key = (row["instrument"], row["horizon_sec"], row["entry_minute"])
            if current_key is not None and key != current_key:
                flush_group(current_members)
                if len(pending) >= 10_000:
                    connection.executemany(insert_sql, pending)
                    pending.clear()
                current_members = []
            current_key = key
            current_members.append(row)
        flush_group(current_members)
        if pending:
            connection.executemany(insert_sql, pending)
            pending.clear()
        connection.executescript(
            """
            CREATE INDEX scratch_archetype_agreement_pair
            ON scratch_archetype_agreements(
                left_archetype,right_archetype,entry_epoch,event_id
            );
            """
        )
        connection.commit()
        baseline_avg = {
            name: statistics.fmean(values)
            for name, values in baseline_values.items() if values
        }
        output: list[dict[str, Any]] = []
        pair_query = (
            "SELECT left_archetype,right_archetype "
            "FROM scratch_archetype_agreements GROUP BY left_archetype,right_archetype "
            "ORDER BY MIN(spool_id)"
        )
        for left, right in connection.execute(pair_query):
            # The legacy implementation stores one representative per event
            # id.  Event ids are unique in canonical outcomes; retain that
            # exact surface and median-horizon rule.
            members = rows.view(
                "left_archetype=? AND right_archetype=?",
                (left, right),
                table="scratch_archetype_agreements",
            )
            horizons = array(
                "q",
                (
                    int(value[0])
                    for value in connection.execute(
                        "SELECT horizon_sec FROM scratch_archetype_agreements "
                        "WHERE left_archetype=? AND right_archetype=? ORDER BY spool_id",
                        (left, right),
                    )
                ),
            )
            if not horizons:
                continue
            horizon = int(statistics.median(horizons))
            summary = summarize_rows(
                members, horizon_sec=horizon, slippage_pips=slippage_pips
            )
            if summary["effective_n"] < 2:
                continue
            strongest = max(
                baseline_avg.get(str(left), -math.inf),
                baseline_avg.get(str(right), -math.inf),
            )
            output.append(
                {
                    "left_archetype": str(left),
                    "right_archetype": str(right),
                    "agreement_only": True,
                    **summary,
                    "strongest_constituent_avg_net_pips": round(strongest, 6),
                    "selection_lift_pips": round(summary["avg_net_pips"] - strongest, 6),
                    "method": "same pair/horizon/minute; archetype deduplicated; diagnostic only",
                }
            )
        output.sort(
            key=lambda row: (row["selection_lift_pips"], row["effective_n"]),
            reverse=True,
        )
        connection.execute("DROP TABLE scratch_archetype_agreements")
        connection.commit()
        return output

    baselines: dict[str, list[dict[str, Any]]] = defaultdict(list)
    episodes: dict[tuple[str, int, int], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        archetype = strategy_archetype(str(row["family"]))
        baselines[archetype].append(row)
        episodes[(row["instrument"], row["horizon_sec"], int(row["entry_epoch"] // 60))].append(row)
    pair_rows: dict[tuple[str, str], dict[str, dict[str, Any]]] = defaultdict(dict)
    for members in episodes.values():
        by_archetype: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
        for row in members:
            by_archetype[strategy_archetype(str(row["family"]))][row["direction"]].append(row)
        names = sorted(by_archetype)
        for index, left in enumerate(names):
            for right in names[index + 1 :]:
                aligned = set(by_archetype[left]) & set(by_archetype[right])
                for direction in aligned:
                    representative = (by_archetype[left][direction] + by_archetype[right][direction])[0]
                    pair_rows[(left, right)][representative["event_id"]] = representative
    output = []
    baseline_avg = {
        name: statistics.fmean(finite(row["net_pips"]) for row in members)
        for name, members in baselines.items()
        if members
    }
    for (left, right), unique in pair_rows.items():
        members = list(unique.values())
        if not members:
            continue
        horizon = int(statistics.median(row["horizon_sec"] for row in members))
        summary = summarize_rows(members, horizon_sec=horizon, slippage_pips=slippage_pips)
        if summary["effective_n"] < 2:
            continue
        strongest = max(baseline_avg.get(left, -math.inf), baseline_avg.get(right, -math.inf))
        output.append(
            {
                "left_archetype": left,
                "right_archetype": right,
                "agreement_only": True,
                **summary,
                "strongest_constituent_avg_net_pips": round(strongest, 6),
                "selection_lift_pips": round(summary["avg_net_pips"] - strongest, 6),
                "method": "same pair/horizon/minute; archetype deduplicated; diagnostic only",
            }
        )
    output.sort(key=lambda row: (row["selection_lift_pips"], row["effective_n"]), reverse=True)
    return output


def historical_reconciliation(
    registry_path: Path, family_results: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    try:
        registry_text = registry_path.read_text(encoding="utf-8")
    except OSError:
        registry_text = ""
    current = {row["family"]: row for row in family_results}
    candidates = (PRIMARY_SEED, *CONDITIONAL_COMPONENTS, *SMALL_POSITIVE_CANDIDATES)
    output = []
    for family in candidates:
        historical_reference = family in registry_text
        if family == "ema_trend_cross" and "Moving-Average Crossover" in registry_text:
            historical_reference = True
        recent = current.get(family) or {}
        output.append(
            {
                "candidate": family,
                "historical_reference_found": historical_reference,
                "historical_result": (
                    "candidate-specific reference present; reproduce before comparison"
                    if historical_reference
                    else "no compatible candidate-specific result located in registry"
                ),
                "recent_shadow_effective_n": int(recent.get("effective_n") or 0),
                "recent_shadow_avg_net_pips": recent.get("avg_net_pips"),
                "same_code": "unverified",
                "same_features": "unverified",
                "same_labels": "unverified",
                "same_costs": "unverified",
                "same_horizon": "unverified",
                "reproducible": False,
                "merge_policy": "never numerically combine until every parity field is verified",
            }
        )
    return output


def model_source_reconciliation(
    database_path: Path, roadmap_path: Path = DEFAULT_MODEL_GAP_ROADMAP
) -> list[dict[str, Any]]:
    """Separate implemented/backtested code from current canonical evidence."""

    try:
        roadmap = json.loads(roadmap_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        roadmap = {}
    connection = sqlite3.connect(
        f"file:{database_path.resolve().as_posix()}?mode=ro", uri=True, timeout=30.0
    )
    connection.execute("PRAGMA query_only=ON")
    counts = dict(
        connection.execute(
            "SELECT family,COUNT(*) FROM canonical_forecasts GROUP BY family"
        ).fetchall()
    ) if connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='canonical_forecasts'"
    ).fetchone() else {}
    connection.close()
    rows = []
    classes = roadmap.get("model_classes") or roadmap.get("classes") or []
    if not classes:
        classes = [
            {
                "model_class": str(model),
                "implementation_status": phase.get("status") or "documented",
                "bounded_test_status": phase.get("evidence") or "see_roadmap",
                "production_eligible": False,
            }
            for phase in (roadmap.get("phases") or [])
            if isinstance(phase, dict)
            for model in (phase.get("models") or [])
        ]
    if isinstance(classes, dict):
        classes = [dict(value, model_class=key) if isinstance(value, dict) else {"model_class": key} for key, value in classes.items()]
    for item in classes if isinstance(classes, list) else []:
        if not isinstance(item, dict):
            continue
        name = str(item.get("model_class") or item.get("class") or item.get("name") or "")
        rows.append(
            {
                "model_class": name,
                "implementation_status": str(item.get("implementation_status") or item.get("status") or "documented"),
                "bounded_test_status": str(item.get("bounded_test_status") or item.get("validation_status") or "see_roadmap"),
                "production_eligible": bool(item.get("production_eligible") or False),
                "canonical_forecast_count": int(counts.get(name, 0)),
                "evidence_status": "current_canonical" if counts.get(name, 0) else "historical_or_stale_only",
            }
        )
    for family in (
        "ridge_return_repaired",
        "modern_tabular_probabilistic_repaired",
        "cross_pair_graph_transfer",
        "probabilistic_state_space",
    ):
        rows.append(
            {
                "model_class": family,
                "implementation_status": "prospective_proof_worker",
                "bounded_test_status": "unit_and_live_snapshot_smoke_validated",
                "production_eligible": False,
                "canonical_forecast_count": int(counts.get(family, 0)),
                "evidence_status": "collecting" if counts.get(family, 0) else "awaiting_first_capture",
            }
        )
    return rows


def initialize_evidence_database(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30.0)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS runs (
            run_id TEXT PRIMARY KEY,
            generated_utc TEXT NOT NULL,
            source_database TEXT NOT NULL,
            source_table TEXT NOT NULL,
            source_highwater_row_id INTEGER NOT NULL,
            source_rows INTEGER NOT NULL,
            usable_rows INTEGER NOT NULL,
            thresholds_json TEXT NOT NULL,
            integrity_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS cells (
            run_id TEXT NOT NULL,
            cell_id TEXT NOT NULL,
            family TEXT NOT NULL,
            instrument TEXT NOT NULL,
            horizon_sec INTEGER NOT NULL,
            session_bucket TEXT NOT NULL,
            liquidity_bucket TEXT NOT NULL,
            raw_n INTEGER NOT NULL,
            effective_n INTEGER NOT NULL,
            avg_net_pips REAL NOT NULL,
            profit_factor REAL NOT NULL,
            lower_confidence_pips REAL NOT NULL,
            eligible INTEGER NOT NULL,
            evidence_json TEXT NOT NULL,
            PRIMARY KEY(run_id, cell_id)
        );
        CREATE TABLE IF NOT EXISTS family_evidence (
            run_id TEXT NOT NULL,
            family TEXT NOT NULL,
            horizon_sec INTEGER NOT NULL,
            raw_n INTEGER NOT NULL,
            effective_n INTEGER NOT NULL,
            avg_net_pips REAL NOT NULL,
            profit_factor REAL NOT NULL,
            lower_confidence_pips REAL NOT NULL,
            eligible INTEGER NOT NULL,
            evidence_json TEXT NOT NULL,
            PRIMARY KEY(run_id, family, horizon_sec)
        );
        CREATE TABLE IF NOT EXISTS current_cell_evidence (
            cell_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            refreshed_utc TEXT NOT NULL,
            family TEXT NOT NULL,
            instrument TEXT NOT NULL,
            horizon_sec INTEGER NOT NULL,
            session_bucket TEXT NOT NULL,
            liquidity_bucket TEXT NOT NULL,
            raw_n INTEGER NOT NULL,
            effective_n INTEGER NOT NULL,
            avg_net_pips REAL NOT NULL,
            minimum_economic_edge_pips REAL NOT NULL,
            unadjusted_upper_confidence_pips REAL,
            time_uniform_lower_bound_pips REAL NOT NULL,
            time_uniform_upper_bound_pips REAL NOT NULL,
            minimum_detectable_edge_pips REAL,
            additional_effective_episodes_for_power INTEGER NOT NULL,
            multiplicity_survivor INTEGER NOT NULL,
            graduation_stage TEXT NOT NULL,
            evidence_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS current_evidence_state (
            singleton INTEGER PRIMARY KEY CHECK(singleton=1),
            run_id TEXT NOT NULL,
            generated_utc TEXT NOT NULL,
            source_highwater_row_id INTEGER NOT NULL,
            cell_count INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS candidate_condition_evidence (
            run_id TEXT NOT NULL,
            family TEXT NOT NULL,
            horizon_sec INTEGER NOT NULL,
            dimension TEXT NOT NULL,
            value TEXT NOT NULL,
            raw_n INTEGER NOT NULL,
            effective_n INTEGER NOT NULL,
            avg_net_pips REAL NOT NULL,
            lower_confidence_pips REAL NOT NULL,
            eligible INTEGER NOT NULL,
            evidence_json TEXT NOT NULL,
            PRIMARY KEY(run_id,family,horizon_sec,dimension,value)
        );
        CREATE TABLE IF NOT EXISTS economic_outcome_labels (
            run_id TEXT NOT NULL,
            event_id TEXT NOT NULL,
            horizon_sec INTEGER NOT NULL,
            family TEXT NOT NULL,
            instrument TEXT NOT NULL,
            direction TEXT NOT NULL,
            entry_time TEXT NOT NULL,
            executable_return_pips REAL NOT NULL,
            modeled_after_cost_pips REAL NOT NULL,
            cleared_total_cost INTEGER NOT NULL,
            max_favorable_pips REAL NOT NULL,
            max_adverse_pips REAL NOT NULL,
            first_positive_sec REAL,
            target_before_stop INTEGER,
            best_alternative_net_pips REAL,
            holding_incremental_vs_best_alternative_pips REAL,
            was_best_available_pair INTEGER,
            label_json TEXT NOT NULL,
            PRIMARY KEY(run_id,event_id,horizon_sec)
        );
        CREATE TABLE IF NOT EXISTS canonical_economic_outcome_labels (
            event_id TEXT NOT NULL,
            horizon_sec INTEGER NOT NULL,
            first_evidence_run_id TEXT NOT NULL,
            family TEXT NOT NULL,
            instrument TEXT NOT NULL,
            direction TEXT NOT NULL,
            entry_time TEXT NOT NULL,
            executable_return_pips REAL NOT NULL,
            modeled_after_cost_pips REAL NOT NULL,
            label_json TEXT NOT NULL,
            PRIMARY KEY(event_id,horizon_sec)
        );
        CREATE TRIGGER IF NOT EXISTS canonical_economic_labels_no_update
        BEFORE UPDATE ON canonical_economic_outcome_labels
        BEGIN SELECT RAISE(ABORT, 'canonical economic labels are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS canonical_economic_labels_no_delete
        BEFORE DELETE ON canonical_economic_outcome_labels
        BEGIN SELECT RAISE(ABORT, 'canonical economic labels are immutable'); END;
        CREATE TABLE IF NOT EXISTS immutable_daily_snapshots (
            utc_day TEXT PRIMARY KEY,
            frozen_utc TEXT NOT NULL,
            source_table TEXT NOT NULL,
            row_count INTEGER NOT NULL,
            minimum_row_id INTEGER NOT NULL,
            maximum_row_id INTEGER NOT NULL,
            evidence_sha256 TEXT NOT NULL
        );
        CREATE TRIGGER IF NOT EXISTS daily_snapshots_no_update
        BEFORE UPDATE ON immutable_daily_snapshots
        BEGIN SELECT RAISE(ABORT, 'daily snapshots are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS daily_snapshots_no_delete
        BEFORE DELETE ON immutable_daily_snapshots
        BEGIN SELECT RAISE(ABORT, 'daily snapshots are immutable'); END;
        CREATE TABLE IF NOT EXISTS immutable_forecast_daily_snapshots (
            utc_day TEXT PRIMARY KEY,
            frozen_utc TEXT NOT NULL,
            row_count INTEGER NOT NULL,
            minimum_row_id INTEGER NOT NULL,
            maximum_row_id INTEGER NOT NULL,
            evidence_sha256 TEXT NOT NULL
        );
        CREATE TRIGGER IF NOT EXISTS forecast_daily_snapshots_no_update
        BEFORE UPDATE ON immutable_forecast_daily_snapshots
        BEGIN SELECT RAISE(ABORT, 'forecast daily snapshots are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS forecast_daily_snapshots_no_delete
        BEFORE DELETE ON immutable_forecast_daily_snapshots
        BEGIN SELECT RAISE(ABORT, 'forecast daily snapshots are immutable'); END;
        """
    )
    return connection


def freeze_completed_days(
    evidence_connection: sqlite3.Connection,
    source_database: Path,
    table: str,
) -> list[dict[str, Any]]:
    source = sqlite3.connect(
        f"file:{source_database.resolve().as_posix()}?mode=ro", uri=True, timeout=30.0
    )
    source.execute("PRAGMA query_only=ON")
    today = datetime.now(timezone.utc).date().isoformat()
    frozen = {
        str(day): int(maximum)
        for day, maximum in evidence_connection.execute(
            "SELECT utc_day,maximum_row_id FROM immutable_daily_snapshots"
        )
    }
    highwater = max(frozen.values(), default=0)
    pending: dict[str, dict[str, Any]] = {}
    for row in source.execute(
        f"""
        SELECT row_id,event_id,horizon_sec,theoretical_pips,entry_time,
               exit_time,substr(observed_utc,1,10) FROM {table}
        WHERE row_id>? AND substr(observed_utc,1,10)<? ORDER BY row_id
        """,
        (highwater, today),
    ):
        day = str(row[6] or "")
        if not day:
            continue
        if day in frozen:
            source.close()
            raise RuntimeError(
                f"late outcome row targets frozen UTC day {day}; immutable snapshot refused"
            )
        item = pending.setdefault(
            day,
            {"digest": hashlib.sha256(), "count": 0, "minimum": 0, "maximum": 0},
        )
        row_id = int(row[0])
        item["minimum"] = row_id if item["count"] == 0 else min(item["minimum"], row_id)
        item["maximum"] = max(item["maximum"], row_id)
        item["digest"].update(
            json.dumps(row[:6], separators=(",", ":"), default=str).encode("utf-8")
        )
        item["count"] += 1
    inserted = []
    for day, item in sorted(pending.items()):
        digest = item["digest"].hexdigest()
        evidence_connection.execute(
            """
            INSERT OR IGNORE INTO immutable_daily_snapshots (
                utc_day, frozen_utc, source_table, row_count, minimum_row_id,
                maximum_row_id, evidence_sha256
            ) VALUES (?,?,?,?,?,?,?)
            """,
            (
                day, utc_now(), table, item["count"], item["minimum"],
                item["maximum"], digest,
            ),
        )
        inserted.append({"utc_day": day, "row_count": item["count"], "sha256": digest})
    source.close()
    return inserted


def freeze_completed_forecast_days(
    evidence_connection: sqlite3.Connection,
    source_database: Path,
) -> list[dict[str, Any]]:
    source = sqlite3.connect(
        f"file:{source_database.resolve().as_posix()}?mode=ro", uri=True, timeout=30.0
    )
    source.execute("PRAGMA query_only=ON")
    if not source.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='canonical_forecasts'"
    ).fetchone():
        source.close()
        return []
    today = datetime.now(timezone.utc).date().isoformat()
    frozen = {
        str(day): int(maximum)
        for day, maximum in evidence_connection.execute(
            "SELECT utc_day,maximum_row_id FROM immutable_forecast_daily_snapshots"
        )
    }
    highwater = max(frozen.values(), default=0)
    pending: dict[str, dict[str, Any]] = {}
    for row in source.execute(
        """
        SELECT rowid,event_id,recorded_utc,model_version,feature_version,
               data_cutoff_utc,horizons_json,payload_sha256,
               substr(recorded_utc,1,10)
        FROM canonical_forecasts
        WHERE rowid>? AND substr(recorded_utc,1,10)<? ORDER BY rowid
        """,
        (highwater, today),
    ):
        day = str(row[8] or "")
        if not day:
            continue
        if day in frozen:
            source.close()
            raise RuntimeError(
                f"late forecast row targets frozen UTC day {day}; immutable snapshot refused"
            )
        item = pending.setdefault(
            day,
            {"digest": hashlib.sha256(), "count": 0, "minimum": 0, "maximum": 0},
        )
        row_id = int(row[0])
        item["minimum"] = row_id if item["count"] == 0 else min(item["minimum"], row_id)
        item["maximum"] = max(item["maximum"], row_id)
        item["digest"].update(
            json.dumps(row[:8], separators=(",", ":"), default=str).encode("utf-8")
        )
        item["count"] += 1
    inserted: list[dict[str, Any]] = []
    for day, item in sorted(pending.items()):
        record = {
            "utc_day": day,
            "row_count": item["count"],
            "minimum_row_id": item["minimum"],
            "maximum_row_id": item["maximum"],
            "evidence_sha256": item["digest"].hexdigest(),
        }
        evidence_connection.execute(
            """
            INSERT OR IGNORE INTO immutable_forecast_daily_snapshots (
                utc_day,frozen_utc,row_count,minimum_row_id,maximum_row_id,
                evidence_sha256
            ) VALUES (?,?,?,?,?,?)
            """,
            (
                day,
                utc_now(),
                record["row_count"],
                record["minimum_row_id"],
                record["maximum_row_id"],
                record["evidence_sha256"],
            ),
        )
        inserted.append(record)
    source.close()
    return inserted


class SpilledEconomicLabels:
    """Replayable label iterator backed by the bounded outcome spool."""

    def __init__(self, rows: SpilledRows, slippage_pips: float) -> None:
        self.rows = rows
        self.slippage_pips = float(slippage_pips)
        self._prepared = False

    def __len__(self) -> int:
        return len(self.rows)

    def _prepare(self) -> None:
        if self._prepared:
            return
        connection = self.rows.connection
        connection.execute("DROP TABLE IF EXISTS scratch_best_instrument")
        connection.execute("DROP TABLE IF EXISTS scratch_alternative_rank")
        where = f" WHERE {self.rows.where_sql}" if self.rows.where_sql else ""
        connection.execute(
            f"""
            CREATE TABLE scratch_best_instrument AS
            SELECT horizon_sec,entry_minute,instrument,
                   MAX(executable_pips-?) AS net_pips
            FROM {self.rows.table}{where}
            GROUP BY horizon_sec,entry_minute,instrument
            """,
            (self.slippage_pips, *self.rows.where_args),
        )
        connection.execute(
            """
            CREATE TABLE scratch_alternative_rank AS
            SELECT * FROM (
                SELECT horizon_sec,entry_minute,instrument,net_pips,
                       ROW_NUMBER() OVER (
                           PARTITION BY horizon_sec,entry_minute
                           ORDER BY net_pips DESC,instrument DESC
                       ) AS alternative_rank
                FROM scratch_best_instrument
            ) WHERE alternative_rank<=2
            """
        )
        connection.execute(
            "CREATE INDEX scratch_alternative_lookup ON scratch_alternative_rank("
            "horizon_sec,entry_minute,alternative_rank,instrument)"
        )
        connection.commit()
        self._prepared = True

    def iter_after(self, source_row_highwater: int = 0) -> Iterator[dict[str, Any]]:
        self._prepare()
        base_where = f"({self.rows.where_sql}) AND " if self.rows.where_sql else ""
        columns = ",".join(f"r.{name}" for name in _SPOOL_COLUMNS)
        query = f"""
            SELECT {columns},
                   (SELECT a.net_pips FROM scratch_alternative_rank a
                    WHERE a.horizon_sec=r.horizon_sec
                      AND a.entry_minute=r.entry_minute
                      AND a.instrument<>r.instrument
                    ORDER BY a.alternative_rank LIMIT 1) AS best_alternative
            FROM {self.rows.table} r
            WHERE {base_where}r.row_id>?
            ORDER BY r.spool_id
        """
        args = (*self.rows.where_args, int(source_row_highwater))
        for values in self.rows.connection.execute(query, args):
            row = dict(zip(_SPOOL_COLUMNS, values[: len(_SPOOL_COLUMNS)]))
            best_alternative = values[-1]
            own = finite(row["executable_pips"]) - self.slippage_pips
            stop_time = row.get("configured_stop_hit_sec")
            target_time = row.get("configured_target_hit_sec")
            target_before_stop = (
                None if target_time is None and stop_time is None
                else bool(
                    target_time is not None
                    and (stop_time is None or target_time < stop_time)
                )
            )
            yield {
                "_source_row_id": int(row.get("row_id") or 0),
                "event_id": row["event_id"],
                "horizon_sec": int(row["horizon_sec"]),
                "family": row["family"],
                "instrument": row["instrument"],
                "direction": row["direction"],
                "entry_time": row["entry_time"],
                "executable_return_pips": round(finite(row["executable_pips"]), 6),
                "modeled_after_cost_pips": round(own, 6),
                "cleared_total_cost": own > 0.0,
                "max_favorable_pips": round(finite(row.get("max_favorable_pips")), 6),
                "max_adverse_pips": round(finite(row.get("max_adverse_pips")), 6),
                "first_positive_sec": row.get("first_positive_sec"),
                "target_before_stop": target_before_stop,
                "best_alternative_net_pips": (
                    None if best_alternative is None else round(finite(best_alternative), 6)
                ),
                "holding_incremental_vs_best_alternative_pips": (
                    None if best_alternative is None
                    else round(own - finite(best_alternative), 6)
                ),
                "was_best_available_pair": (
                    None if best_alternative is None else own >= finite(best_alternative)
                ),
                "label_contract": "executable bid/ask endpoint plus modeled slippage; contemporaneous alternatives use same horizon and entry minute",
            }

    def __iter__(self) -> Iterator[dict[str, Any]]:
        return self.iter_after(0)


def persist_run(
    path: Path,
    report: dict[str, Any],
    cells: list[dict[str, Any]],
    family_results: list[dict[str, Any]],
    economic_labels: list[dict[str, Any]] | SpilledEconomicLabels,
    candidate_conditions: list[dict[str, Any]],
    progress_callback: ProgressCallback | None = None,
) -> dict[str, list[dict[str, Any]]]:
    connection = initialize_evidence_database(path)
    run_id = report["run_id"]
    integrity = report["integrity"]
    utc_day = str(report["generated_utc"])[:10]
    previous_label_highwater = int(connection.execute(
        "SELECT COALESCE(MAX(source_highwater_row_id),0) FROM runs"
    ).fetchone()[0])
    persist_full_daily_evidence = connection.execute(
        """
        SELECT 1 FROM runs r JOIN cells c ON c.run_id=r.run_id
        WHERE substr(r.generated_utc,1,10)=? LIMIT 1
        """,
        (utc_day,),
    ).fetchone() is None
    connection.execute(
        "INSERT OR IGNORE INTO runs VALUES (?,?,?,?,?,?,?,?,?)",
        (
            run_id,
            report["generated_utc"],
            integrity["database"],
            integrity["table"],
            integrity["source_highwater_row_id"],
            integrity["source_rows"],
            integrity["usable_signal_outcomes"],
            json.dumps(report["thresholds"], sort_keys=True),
            json.dumps(integrity, sort_keys=True),
        ),
    )
    emit_progress(
        progress_callback, "persisting_evidence",
        persistence_step="writing_current_cells", cells_completed=len(cells),
    )
    # This is a replaceable operational cache, not proof evidence.  It lets
    # the separate lifecycle worker classify every current cell without
    # copying an unbounded full-cell snapshot on every 15-minute refresh.
    # Immutable daily evidence and governance snapshots remain unchanged.
    connection.execute("DELETE FROM current_cell_evidence")
    connection.executemany(
        """
        INSERT INTO current_cell_evidence VALUES (
            ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
        )
        """,
        (
            (
                row["cell_id"],
                run_id,
                report["generated_utc"],
                row["family"],
                row["instrument"],
                row["horizon_sec"],
                row["session"],
                row["liquidity_bucket"],
                row["raw_n"],
                row["effective_n"],
                row["avg_net_pips"],
                row["minimum_economic_edge_pips"],
                row.get("unadjusted_upper_confidence_pips"),
                row["time_uniform_lower_bound_pips"],
                row["time_uniform_upper_bound_pips"],
                row.get("minimum_detectable_edge_pips"),
                row["additional_effective_episodes_for_power"],
                int(bool(row.get("multiplicity_survivor"))),
                str(row.get("graduation_stage") or "continue_collecting"),
                json.dumps(row, sort_keys=True, separators=(",", ":")),
            )
            for row in cells
        ),
    )
    connection.execute(
        """
        INSERT INTO current_evidence_state VALUES (1,?,?,?,?)
        ON CONFLICT(singleton) DO UPDATE SET
            run_id=excluded.run_id,
            generated_utc=excluded.generated_utc,
            source_highwater_row_id=excluded.source_highwater_row_id,
            cell_count=excluded.cell_count
        """,
        (
            run_id,
            report["generated_utc"],
            integrity["source_highwater_row_id"],
            len(cells),
        ),
    )
    if persist_full_daily_evidence:
        connection.executemany(
        """
        INSERT OR IGNORE INTO cells VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            (
                run_id,
                row["cell_id"],
                row["family"],
                row["instrument"],
                row["horizon_sec"],
                row["session"],
                row["liquidity_bucket"],
                row["raw_n"],
                row["effective_n"],
                row["avg_net_pips"],
                row["profit_factor"],
                row["lower_confidence_pips"],
                int(bool(row["eligible"])),
                json.dumps(row, sort_keys=True),
            )
            for row in cells
        ),
        )
    before_labels = connection.total_changes
    if isinstance(economic_labels, SpilledEconomicLabels):
        incremental_label_count = int(
            economic_labels.rows.connection.execute(
                f"SELECT COUNT(*) FROM {economic_labels.rows.table} WHERE "
                + (
                    f"({economic_labels.rows.where_sql}) AND row_id>?"
                    if economic_labels.rows.where_sql else "row_id>?"
                ),
                (*economic_labels.rows.where_args, previous_label_highwater),
            ).fetchone()[0]
        )
        incremental_labels: Iterable[dict[str, Any]] = economic_labels.iter_after(
            previous_label_highwater
        )
    else:
        incremental_label_count = sum(
            1 for row in economic_labels
            if int(row.get("_source_row_id") or 0) > previous_label_highwater
        )
        incremental_labels = (
            row for row in economic_labels
            if int(row.get("_source_row_id") or 0) > previous_label_highwater
        )
    connection.executemany(
        """
        INSERT OR IGNORE INTO canonical_economic_outcome_labels VALUES (
            ?,?,?,?,?,?,?,?,?,?
        )
        """,
        (
            (
                row["event_id"],
                row["horizon_sec"],
                run_id,
                row["family"],
                row["instrument"],
                row["direction"],
                row["entry_time"],
                row["executable_return_pips"],
                row["modeled_after_cost_pips"],
                json.dumps(
                    {
                        key: value for key, value in row.items()
                        if not str(key).startswith("_")
                    },
                    sort_keys=True,
                ),
            )
            for row in incremental_labels
        ),
    )
    new_canonical_labels = connection.total_changes - before_labels
    emit_progress(
        progress_callback, "persisting_evidence",
        persistence_step="economic_labels_written",
        economic_labels_completed=len(economic_labels),
        economic_labels_attempted_incrementally=incremental_label_count,
        previous_canonical_label_highwater=previous_label_highwater,
        new_canonical_economic_labels=new_canonical_labels,
    )
    if persist_full_daily_evidence:
        connection.executemany(
        "INSERT OR IGNORE INTO family_evidence VALUES (?,?,?,?,?,?,?,?,?,?)",
        (
            (
                run_id,
                row["family"],
                row["horizon_sec"],
                row["raw_n"],
                row["effective_n"],
                row["avg_net_pips"],
                row["profit_factor"],
                row["lower_confidence_pips"],
                int(bool(row["eligible"])),
                json.dumps(row, sort_keys=True),
            )
            for row in family_results
        ),
        )
        connection.executemany(
        "INSERT OR IGNORE INTO candidate_condition_evidence VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (
            (
                run_id,
                row["family"],
                row["horizon_sec"],
                row["dimension"],
                row["value"],
                row["raw_n"],
                row["effective_n"],
                row["avg_net_pips"],
                row["lower_confidence_pips"],
                int(bool(row["eligible"])),
                json.dumps(row, sort_keys=True),
            )
            for row in candidate_conditions
        ),
        )
    try:
        emit_progress(
            progress_callback, "persisting_evidence",
            persistence_step="freezing_completed_outcome_days",
        )
        outcome_frozen = freeze_completed_days(
            connection, Path(integrity["database"]), integrity["table"]
        )
        emit_progress(
            progress_callback, "persisting_evidence",
            persistence_step="freezing_completed_forecast_days",
            newly_frozen_outcome_days=len(outcome_frozen),
        )
        forecast_frozen = freeze_completed_forecast_days(
            connection, Path(integrity["database"])
        )
        emit_progress(
            progress_callback, "persisting_evidence",
            persistence_step="committing_atomic_evidence_transaction",
            newly_frozen_outcome_days=len(outcome_frozen),
            newly_frozen_forecast_days=len(forecast_frozen),
        )
        connection.commit()
    except Exception:
        connection.rollback()
        connection.close()
        raise
    connection.close()
    return {
        "outcome_days": outcome_frozen,
        "forecast_days": forecast_frozen,
        "full_cell_snapshot_persisted": persist_full_daily_evidence,
        "new_canonical_economic_labels": new_canonical_labels,
        "economic_labels_attempted_incrementally": incremental_label_count,
        "previous_canonical_label_highwater": previous_label_highwater,
    }


def build_economic_labels(
    rows: list[dict[str, Any]] | SpilledRows,
    *,
    slippage_pips: float,
) -> list[dict[str, Any]] | SpilledEconomicLabels:
    """Create executable allocator targets without fitting on the same sample."""

    if isinstance(rows, SpilledRows):
        return SpilledEconomicLabels(rows, slippage_pips)

    grouped: dict[tuple[int, int], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(int(row["horizon_sec"]), int(row["entry_epoch"] // 60))].append(row)
    alternative_rankings: dict[tuple[int, int], list[tuple[float, str]]] = {}
    for key, members in grouped.items():
        best_by_instrument: dict[str, float] = {}
        for member in members:
            instrument = str(member["instrument"])
            value = finite(member["executable_pips"]) - slippage_pips
            best_by_instrument[instrument] = max(
                value, best_by_instrument.get(instrument, -math.inf)
            )
        alternative_rankings[key] = sorted(
            ((value, instrument) for instrument, value in best_by_instrument.items()),
            reverse=True,
        )[:2]
    output: list[dict[str, Any]] = []
    for row in rows:
        own = finite(row["executable_pips"]) - slippage_pips
        ranking = alternative_rankings[
            (int(row["horizon_sec"]), int(row["entry_epoch"] // 60))
        ]
        best_alternative = next(
            (value for value, instrument in ranking if instrument != row["instrument"]),
            None,
        )
        stop_time = row.get("configured_stop_hit_sec")
        target_time = row.get("configured_target_hit_sec")
        target_before_stop = (
            None
            if target_time is None and stop_time is None
            else bool(target_time is not None and (stop_time is None or target_time < stop_time))
        )
        output.append(
            {
                "_source_row_id": int(row.get("row_id") or 0),
                "event_id": row["event_id"],
                "horizon_sec": int(row["horizon_sec"]),
                "family": row["family"],
                "instrument": row["instrument"],
                "direction": row["direction"],
                "entry_time": row["entry_time"],
                "executable_return_pips": round(finite(row["executable_pips"]), 6),
                "modeled_after_cost_pips": round(own, 6),
                "cleared_total_cost": own > 0.0,
                "max_favorable_pips": round(finite(row.get("max_favorable_pips")), 6),
                "max_adverse_pips": round(finite(row.get("max_adverse_pips")), 6),
                "first_positive_sec": row.get("first_positive_sec"),
                "target_before_stop": target_before_stop,
                "best_alternative_net_pips": (
                    None if best_alternative is None else round(best_alternative, 6)
                ),
                "holding_incremental_vs_best_alternative_pips": (
                    None
                    if best_alternative is None
                    else round(own - best_alternative, 6)
                ),
                "was_best_available_pair": (
                    None if best_alternative is None else own >= best_alternative
                ),
                "label_contract": "executable bid/ask endpoint plus modeled slippage; contemporaneous alternatives use same horizon and entry minute",
            }
        )
    return output


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(path)


def write_markdown(path: Path, report: dict[str, Any]) -> None:
    integrity = report["integrity"]
    contract = integrity.get("forecast_contract") or {}
    microstructure = integrity.get("unavailable_microstructure_inputs") or {}
    surprise = report.get("structured_macro_surprise") or {}
    lines = [
        "# Edge Evidence and Candidate Replication",
        "",
        f"Generated: `{report['generated_utc']}`",
        "",
        "This is a fail-closed research report. It cannot place orders or promote a lane.",
        "",
        "## Integrity",
        "",
        f"- Source: `{integrity['table']}` ({integrity['immutability']}).",
        f"- Source rows: `{integrity['source_rows']:,}`; usable exact-horizon signal outcomes: `{integrity['usable_signal_outcomes']:,}`.",
        f"- Late outcomes excluded: `{integrity['late_outcomes_excluded']:,}`; pre-boundary snapshots excluded: `{integrity.get('pre_boundary_outcomes_excluded', 0):,}`; invalid boundary times: `{integrity.get('invalid_boundary_times_excluded', 0):,}`; duplicate event/horizons: `{integrity['duplicate_event_horizons']:,}`.",
        f"- Canonical pre-outcome forecasts captured since deployment: `{integrity['canonical_forecasts']:,}`.",
        f"- Exact-horizon tolerance: `<= {finite(integrity.get('maximum_outcome_delay_sec')):.1f}s`; forecasts outside it are excluded, never backfilled.",
        f"- Version contract: `{int(contract.get('unknown_model_version') or 0)}` unknown model versions; `{int(contract.get('unknown_feature_version') or 0)}` unknown feature versions; `{int(contract.get('duplicate_payloads') or 0)}` duplicate payload hashes.",
        "",
        "## Decision",
        "",
        f"- Observed canonical cells: `{report['cell_count']:,}`.",
        f"- Fully passing cells: `{report['passing_cell_count']:,}`.",
        f"- Multiplicity-adjusted discovery candidates: `{report.get('discovery_candidate_count', 0):,}`; locked confirmations: `{report.get('confirmation_passing_count', 0):,}`.",
        f"- Counterfactual allocator action: **{report['shadow_allocator']['action']}**.",
        "",
        "## Prospective replication governance",
        "",
    ]
    governance = report.get("prospective_governance") or {}
    census = governance.get("evidence_census") or {}
    multiplicity = governance.get("multiple_testing") or {}
    ladder = governance.get("graduation_ladder") or {}
    lines.extend(
        [
            f"- Governance fingerprint: `{governance.get('governance_sha256', 'unavailable')}`.",
            f"- Hierarchical FDR: `{multiplicity.get('family_survivors', 0)}` family/horizon survivors and `{multiplicity.get('cell_survivors', 0)}` cell survivors.",
            f"- Effective-N median: `{(census.get('effective_n_distribution') or {}).get('p50', 0)}`; cells at N>=50/100/200/500: `{(census.get('effective_n_threshold_counts') or {}).get('50', 0)}` / `{(census.get('effective_n_threshold_counts') or {}).get('100', 0)}` / `{(census.get('effective_n_threshold_counts') or {}).get('200', 0)}` / `{(census.get('effective_n_threshold_counts') or {}).get('500', 0)}`.",
            f"- Positive point EV / unadjusted LCB / multiplicity survivors: `{census.get('positive_point_ev_cells', 0)}` / `{census.get('positive_unadjusted_lcb_cells', 0)}` / `{census.get('multiplicity_survivor_cells', 0)}`.",
            f"- Economically inadequate at current power: `{census.get('economically_inadequate_with_current_power_cells', 0)}`; blocked only by sample size: `{census.get('blocked_only_by_sample_size_cells', 0)}`.",
            f"- Graduation counts: A contract valid `{(ladder.get('A_contract_validity') or {}).get('passed', False)}`; B discovery `{(ladder.get('B_discovery_candidate') or {}).get('count', 0)}`; C confirmation `{(ladder.get('C_locked_prospective_confirmation') or {}).get('count', 0)}`; D practice canary `{(ladder.get('D_practice_canary') or {}).get('count', 0)}`.",
            "- Repeated ordinary confidence intervals cannot promote a cell. Time-uniform clipped-mean confidence sequences are reported; selection can only open a later untouched confirmation cohort.",
            "- Pooled graph/tabular confidence can prioritize research, but promotion requires direct prospective evidence in the exact cell.",
            "",
            "### Frozen proof cohorts",
            "",
            "| Family | Cohort | Forecasts | Matured | Pairs | Untagged legacy excluded |",
            "|---|---|---:|---:|---:|---:|",
        ]
    )
    proof = governance.get("proof_cohorts") or {}
    legacy = proof.get("pre_governance_untagged_forecasts") or {}
    for row in proof.get("cohorts") or []:
        lines.append(
            f"| {row['family']} | `{row['cohort_id']}` | {row['forecast_count']} | {row['matured_outcome_count']} | {row['pair_count']} | {int(legacy.get(row['family'], 0))} |"
        )
    lines.extend(
        [
            "",
            "## Strongest cells (diagnostic, not promotion)",
            "",
            "| Family | Pair | Horizon | Session | Cost | Raw N | Effective N | Avg net | PF | LCB | Failed gates |",
            "|---|---|---:|---|---|---:|---:|---:|---:|---:|---|",
        ]
    )
    for row in report["top_cells"][:30]:
        failures = ", ".join(
            gate["gate"] for gate in row["gate_distances"] if not gate["passed"]
        )
        lines.append(
            f"| {row['family']} | {row['instrument']} | {row['horizon_sec']} | {row['session']} | {row['liquidity_bucket']} | "
            f"{row['raw_n']} | {row['effective_n']} | {row['avg_net_pips']:.3f} | {row['profit_factor']:.2f} | "
            f"{row['lower_confidence_pips']:.3f} | {failures or 'none'} |"
        )
    lines.extend(
        [
            "",
            "## Archetype evidence (vote-counting disabled)",
            "",
            "| Archetype | Horizon | Families | Raw N | Effective N | Avg net | PF | LCB | Eligible |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---|",
        ]
    )
    for row in report.get("archetype_horizons", [])[:16]:
        lines.append(
            f"| {row['archetype']} | {row['horizon_sec']} | {row['family_count']} | "
            f"{row['raw_n']} | {row['effective_n']} | {row['avg_net_pips']:.3f} | "
            f"{row['profit_factor']:.2f} | {row['lower_confidence_pips']:.3f} | "
            f"{'yes' if row['eligible'] else 'no'} |"
        )
    lines.extend(
        [
            "",
            "## Promotion gate failures",
            "",
            "| Gate | Failing cells |",
            "|---|---:|",
        ]
    )
    for gate, count in report.get("gate_failure_counts", {}).items():
        lines.append(f"| {gate} | {count:,} |")
    lines.extend(
        [
            "",
            "## Candidate replication (low sample; diagnostic only)",
            "",
            f"Primary squeeze cohort: `{(report.get('primary_seed_cohort') or {}).get('cohort_id', 'unavailable')}`, started `{(report.get('primary_seed_cohort') or {}).get('cohort_start_utc', 'unavailable')}`; `{int((report.get('primary_seed_cohort') or {}).get('pre_governance_forecast_count') or 0):,}` earlier forecasts are preserved but excluded from governed squeeze replication.",
            "",
            "| Family | Horizon | Raw N | Effective N | Avg net | PF | LCB | Eligible |",
            "|---|---:|---:|---:|---:|---:|---:|---|",
        ]
    )
    for row in report.get("candidate_family_horizons", [])[:24]:
        lines.append(
            f"| {row['family']} | {row['horizon_sec']} | {row['raw_n']} | "
            f"{row['effective_n']} | {row['avg_net_pips']:.3f} | "
            f"{row['profit_factor']:.2f} | {row['lower_confidence_pips']:.3f} | "
            f"{'yes' if row['eligible'] else 'no'} |"
        )
    lines.extend(
        [
            "",
            "## Primary-seed conditional replication",
            "",
            "Every condition below was present at forecast time. Results remain low-sample diagnostics and cannot promote a lane.",
            "",
            "| Dimension | Value | Horizon | Raw N | Effective N | Avg net | PF | LCB | Eligible |",
            "|---|---|---:|---:|---:|---:|---:|---:|---|",
        ]
    )
    for row in report.get("candidate_condition_results", [])[:40]:
        lines.append(
            f"| {row['dimension']} | {row['value']} | {row['horizon_sec']} | "
            f"{row['raw_n']} | {row['effective_n']} | {row['avg_net_pips']:.3f} | "
            f"{row['profit_factor']:.2f} | {row['lower_confidence_pips']:.3f} | "
            f"{'yes' if row['eligible'] else 'no'} |"
        )
    lines.extend(
        [
            "",
            "## Archetype incremental-information diagnostics",
            "",
            "Same-archetype family variants are collapsed; lifts compare distinct-archetype agreement with the stronger constituent baseline.",
            "",
            "| Archetype A | Archetype B | Raw N | Effective N | Avg net | Strongest constituent | Selection lift | LCB |",
            "|---|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in report.get("archetype_incremental_tests", [])[:24]:
        lines.append(
            f"| {row['left_archetype']} | {row['right_archetype']} | {row['raw_n']} | "
            f"{row['effective_n']} | {row['avg_net_pips']:.3f} | "
            f"{row['strongest_constituent_avg_net_pips']:.3f} | "
            f"{row['selection_lift_pips']:.3f} | {row['lower_confidence_pips']:.3f} |"
        )
    lines.extend(
        [
            "",
            "## Historical-versus-live reconciliation",
            "",
            "| Candidate | Historical artifact | Recent effective N | Recent avg net | Parity verified | Reproducible |",
            "|---|---|---:|---:|---|---|",
        ]
    )
    for row in report.get("historical_live_reconciliation", []):
        recent = row.get("recent_shadow_avg_net_pips")
        recent_text = "n/a" if recent is None else f"{finite(recent):.3f}"
        parity = all(
            row.get(key) is True
            for key in ("same_code", "same_features", "same_labels", "same_costs", "same_horizon")
        )
        lines.append(
            f"| {row['candidate']} | {'found' if row['historical_reference_found'] else 'not located'} | "
            f"{row['recent_shadow_effective_n']} | {recent_text} | "
            f"{'yes' if parity else 'no'} | {'yes' if row['reproducible'] else 'no'} |"
        )
    lines.extend(
        [
            "",
            "## Model-source reconciliation",
            "",
            "Implemented or historically backtested is not equivalent to a current canonical contributor.",
            "",
            "| Model/source | Implementation | Current canonical forecasts | Evidence status | Production eligible |",
            "|---|---|---:|---|---|",
        ]
    )
    for row in report.get("model_source_reconciliation", []):
        lines.append(
            f"| {row['model_class']} | {row['implementation_status']} | "
            f"{row['canonical_forecast_count']} | {row['evidence_status']} | "
            f"{'yes' if row['production_eligible'] else 'no'} |"
        )
    lines.extend(
        [
            "",
            "## Source availability decisions",
            "",
            f"- Order-book availability: `{int(microstructure.get('order_book_available_rows') or 0):,}` / `{int(microstructure.get('forecast_rows_inspected') or 0):,}` forecast rows.",
            f"- Position-book availability: `{int(microstructure.get('position_book_available_rows') or 0):,}` / `{int(microstructure.get('forecast_rows_inspected') or 0):,}`.",
            f"- Informative pricing-depth rows: `{int(microstructure.get('pricing_depth_informative_rows') or 0):,}`. These inputs remain excluded from eligibility.",
            f"- Structured macro ledger: `{int(surprise.get('release_count') or surprise.get('topics') or 0):,}` releases; `{int(surprise.get('scheduled_release_count') or 0):,}` scheduled; `{int(surprise.get('actual_count') or 0):,}` actuals; `{int(surprise.get('consensus_count') or 0):,}` consensuses; `{int(surprise.get('actual_and_consensus_count') or surprise.get('actual_and_consensus_topics') or 0):,}` usable surprises (`{surprise.get('status') or 'unknown'}`).",
            f"- Causal consensus observations: `{int(surprise.get('causal_consensus_observation_count') or 0):,}`; immutable post-release quote samples: `{int(surprise.get('reaction_sample_count') or 0):,}`.",
            "- General news remains narrative/context evidence; it is not treated as a numeric macro surprise substitute.",
            "",
            "## Candidate policy",
            "",
            "`volatility_squeeze_breakout` is the primary replication seed. CUSUM, session-range, inverse-correlation, and EMA are evaluated only as conditional components. CCI reversion and supervised return rank remain low-sample hypotheses. Historical and recent results are never numerically merged until code, features, labels, costs, and horizons match.",
            "",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text("\n".join(lines), encoding="utf-8")
    temporary.replace(path)


def build_report(
    source_database: Path,
    *,
    evidence_database: Path,
    output_json: Path,
    output_markdown: Path,
    registry_path: Path,
    news_database: Path = DEFAULT_NEWS_DATABASE,
    slippage_pips: float = 0.25,
    maximum_delay_sec: float = 15.0,
    thresholds: EvidenceThresholds | None = None,
    governance_config_path: Path = DEFAULT_GOVERNANCE_CONFIG,
    cohort_state_path: Path = DEFAULT_COHORT_STATE,
    candidate_cohort_database_path: Path | None = None,
    candidate_cohort_state_path: Path | None = None,
    progress_callback: ProgressCallback | None = None,
    forecast_contract_cache: dict[str, Any] | None = None,
) -> dict[str, Any]:
    selected_thresholds = thresholds or EvidenceThresholds()
    emit_progress(progress_callback, "loading_rows")
    rows, integrity = load_rows(
        source_database,
        slippage_pips=slippage_pips,
        maximum_delay_sec=maximum_delay_sec,
        progress_callback=progress_callback,
        bounded_memory=True,
        forecast_contract_cache=forecast_contract_cache,
    )
    emit_progress(
        progress_callback,
        "building_cells",
        usable_signal_outcomes=len(rows),
        source_highwater_row_id=integrity["source_highwater_row_id"],
    )
    cells, family_results = build_cell_results(
        rows,
        thresholds=selected_thresholds,
        slippage_pips=slippage_pips,
    )
    emit_progress(
        progress_callback,
        "building_archetypes",
        cells_completed=len(cells),
        family_horizons_completed=len(family_results),
    )
    archetype_results = build_archetype_results(
        rows,
        thresholds=selected_thresholds,
        slippage_pips=slippage_pips,
    )
    emit_progress(
        progress_callback,
        "building_economic_labels",
        archetype_horizons_completed=len(archetype_results),
    )
    economic_labels = build_economic_labels(rows, slippage_pips=slippage_pips)
    legacy_gate_passing = [row for row in cells if row["eligible"]]
    governance_config = read_json(governance_config_path)
    selected_candidate_cohort_database = (
        candidate_cohort_database_path
        or (
            DEFAULT_CANDIDATE_COHORT_DATABASE
            if source_database.resolve() == DEFAULT_SOURCE.resolve()
            else evidence_database.with_name("candidate_cohort_registry_v1.sqlite")
        )
    )
    selected_candidate_cohort_state = (
        candidate_cohort_state_path
        or (
            DEFAULT_CANDIDATE_COHORT_STATE
            if source_database.resolve() == DEFAULT_SOURCE.resolve()
            else output_json.with_name("candidate_cohort_registry_v1.json")
        )
    )
    primary_seed_cohort = ensure_primary_seed_cohort(
        source_database,
        governance_config,
        database_path=selected_candidate_cohort_database,
        state_path=selected_candidate_cohort_state,
    )
    emit_progress(
        progress_callback,
        "building_candidate_replication",
        economic_labels_completed=len(economic_labels),
    )
    seed_start = parse_time(primary_seed_cohort.get("cohort_start_utc"))
    seed_start_epoch = seed_start.timestamp() if seed_start else math.inf
    candidate_names = {
        PRIMARY_SEED,
        *CONDITIONAL_COMPONENTS,
        *SMALL_POSITIVE_CANDIDATES,
    }
    if isinstance(rows, SpilledRows):
        ordered_candidates = tuple(sorted(candidate_names))
        placeholders = ",".join("?" for _ in ordered_candidates)
        candidate_analysis_rows: list[dict[str, Any]] | SpilledRows = rows.view(
            f"family IN ({placeholders}) AND (family<>? OR entry_epoch>=?)",
            (*ordered_candidates, PRIMARY_SEED, seed_start_epoch),
        )
    else:
        candidate_analysis_rows = [
            row
            for row in rows
            if row["family"] in candidate_names
            and (row["family"] != PRIMARY_SEED or row["entry_epoch"] >= seed_start_epoch)
        ]
    candidate_cells, candidate_family_results = build_cell_results(
        candidate_analysis_rows,
        thresholds=selected_thresholds,
        slippage_pips=slippage_pips,
    )
    candidate_metadata = _candidate_forecast_metadata(source_database, candidate_names)
    candidate_conditions = candidate_condition_results(
        candidate_analysis_rows,
        metadata=candidate_metadata,
        thresholds=selected_thresholds,
        slippage_pips=slippage_pips,
        macro_releases=_macro_release_epochs(),
    )
    generated = utc_now()
    run_id = "edge_" + stable_hash(
        {
            "generated": generated,
            "source": integrity["database"],
            "highwater": integrity["source_highwater_row_id"],
            "thresholds": asdict(selected_thresholds),
        }
    )[:24]
    emit_progress(
        progress_callback,
        "building_governance",
        run_id=run_id,
        candidate_cells_completed=len(candidate_cells),
        candidate_conditions_completed=len(candidate_conditions),
    )
    governance = build_governance(
        cells=cells,
        families=family_results,
        integrity=integrity,
        source_database=source_database,
        cohort_state_path=cohort_state_path,
        evidence_database=evidence_database,
        config=governance_config,
    )
    discovery_candidates = [
        row for row in cells if row.get("graduation_stage") == "B_discovery_candidate"
    ]
    # A discovery window cannot certify itself.  Confirmation cohorts are
    # immutable and must start strictly after a candidate lock, so none are
    # manufactured automatically here.
    passing: list[dict[str, Any]] = []
    emit_progress(
        progress_callback,
        "assembling_report",
        run_id=run_id,
        cells_completed=len(cells),
    )
    report = {
        "schema_version": 2,
        "run_id": run_id,
        "generated_utc": generated,
        "research_only": True,
        "can_place_orders": False,
        "can_promote": False,
        "evaluation_unit": "family x pair x horizon x UTC session x executable spread bucket",
        "independence_contract": "continuous episodes sharing a signed currency factor collapse to one averaged observation",
        "cost_contract": {
            "entry_exit_prices": "executable bid/ask theoretical_pips from source",
            "modeled_slippage_pips": slippage_pips,
            "stress_total_slippage_pips": [0.0, 0.25, 0.5, 1.0],
        },
        "thresholds": asdict(selected_thresholds),
        "integrity": integrity,
        "cell_count": len(cells),
        "passing_cell_count": len(passing),
        "legacy_gate_passing_count": len(legacy_gate_passing),
        "discovery_candidate_count": len(discovery_candidates),
        "confirmation_passing_count": 0,
        "family_horizon_count": len(family_results),
        "archetype_horizon_count": len(archetype_results),
        "economic_outcome_label_count": len(economic_labels),
        "top_cells": cells[:100],
        "passing_cells": passing,
        "legacy_gate_passing_cells": legacy_gate_passing,
        "discovery_candidates": discovery_candidates,
        "candidate_cells": candidate_cells,
        "candidate_family_horizons": candidate_family_results,
        "archetype_horizons": archetype_results,
        "gate_failure_counts": {
            gate: sum(
                1
                for cell in cells
                for item in cell["gate_distances"]
                if item["gate"] == gate and not item["passed"]
            )
            for gate in sorted(
                {
                    item["gate"]
                    for cell in cells
                    for item in cell["gate_distances"]
                }
            )
        },
        "primary_seed_cohort": primary_seed_cohort,
        "conditional_component_tests": incremental_component_tests(
            candidate_analysis_rows, slippage_pips
        ),
        "candidate_condition_results": candidate_conditions,
        "archetype_incremental_tests": archetype_incremental_tests(rows, slippage_pips),
        "historical_live_reconciliation": historical_reconciliation(
            registry_path, family_results
        ),
        "model_source_reconciliation": model_source_reconciliation(source_database),
        "structured_macro_surprise": structured_macro_coverage(news_database),
        "prospective_governance": governance,
        "negative_controls": [
            row
            for row in sorted(
                (item for item in family_results if item["effective_n"] >= 30),
                key=lambda item: item["avg_net_pips"],
            )[:20]
        ],
        "shadow_allocator": {
            "action": "no_trade",
            "reason": "no cell has completed selection-independent locked prospective confirmation",
            "ranked_cell_ids": [row["cell_id"] for row in discovery_candidates[:20]],
        },
    }
    emit_progress(
        progress_callback,
        "persisting_evidence",
        run_id=run_id,
        cells_completed=len(cells),
        economic_labels_completed=len(economic_labels),
    )
    report["newly_frozen_daily_snapshots"] = persist_run(
        evidence_database,
        report,
        cells,
        family_results,
        economic_labels,
        candidate_conditions,
        progress_callback=progress_callback,
    )
    emit_progress(progress_callback, "writing_report", run_id=run_id)
    atomic_json(output_json, report)
    write_markdown(output_markdown, report)
    emit_progress(
        progress_callback,
        "report_complete",
        run_id=run_id,
        cells_completed=len(cells),
        passing_cells=len(passing),
    )
    if isinstance(rows, SpilledRows):
        rows.close()
        gc.collect()
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-database", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--evidence-database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--output-json", type=Path, default=DEFAULT_JSON)
    parser.add_argument("--output-markdown", type=Path, default=DEFAULT_MARKDOWN)
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument("--news-database", type=Path, default=DEFAULT_NEWS_DATABASE)
    parser.add_argument("--slippage-pips", type=float, default=0.25)
    parser.add_argument("--maximum-delay-sec", type=float, default=15.0)
    parser.add_argument("--governance-config", type=Path, default=DEFAULT_GOVERNANCE_CONFIG)
    parser.add_argument("--cohort-state", type=Path, default=DEFAULT_COHORT_STATE)
    args = parser.parse_args()
    result = build_report(
        args.source_database,
        evidence_database=args.evidence_database,
        output_json=args.output_json,
        output_markdown=args.output_markdown,
        registry_path=args.registry,
        news_database=args.news_database,
        slippage_pips=max(0.0, args.slippage_pips),
        maximum_delay_sec=max(0.0, args.maximum_delay_sec),
        governance_config_path=args.governance_config,
        cohort_state_path=args.cohort_state,
    )
    print(
        json.dumps(
            {
                "run_id": result["run_id"],
                "cell_count": result["cell_count"],
                "passing_cell_count": result["passing_cell_count"],
                "usable_signal_outcomes": result["integrity"]["usable_signal_outcomes"],
                "allocator_action": result["shadow_allocator"]["action"],
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()


__all__ = [
    "EvidenceThresholds",
    "build_cell_results",
    "build_archetype_results",
    "build_report",
    "collapse_factor_episodes",
    "gate_distances",
    "incremental_component_tests",
    "liquidity_bucket",
    "session_bucket",
    "signed_currency_factors",
    "summarize_rows",
]
