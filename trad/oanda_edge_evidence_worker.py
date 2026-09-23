#!/usr/bin/env python3
"""Continuously refresh the fail-closed edge-evidence report.

This process is research-only. It reads the immutable forecast/outcome ledger,
freezes completed UTC days, and rebuilds diagnostics. It has no broker, feed
publishing, promotion, or order capability.
"""

from __future__ import annotations

import argparse
import gc
import hashlib
import json
import os
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import psutil
except ImportError:  # Optional diagnostics; evidence generation still works.
    psutil = None

try:
    from oanda_edge_evidence import (
        DEFAULT_DATABASE,
        DEFAULT_CANDIDATE_COHORT_DATABASE,
        DEFAULT_CANDIDATE_COHORT_STATE,
        DEFAULT_COHORT_STATE,
        DEFAULT_GOVERNANCE_CONFIG,
        DEFAULT_JSON,
        DEFAULT_MACRO_SURPRISE_DATABASE,
        DEFAULT_MACRO_SURPRISE_STATE,
        DEFAULT_MARKDOWN,
        DEFAULT_NEWS_DATABASE,
        DEFAULT_REGISTRY,
        DEFAULT_SOURCE,
        build_report,
        close_forecast_contract_cache,
        utc_now,
    )
except ModuleNotFoundError:
    from trad.oanda_edge_evidence import (
        DEFAULT_DATABASE,
        DEFAULT_CANDIDATE_COHORT_DATABASE,
        DEFAULT_CANDIDATE_COHORT_STATE,
        DEFAULT_COHORT_STATE,
        DEFAULT_GOVERNANCE_CONFIG,
        DEFAULT_JSON,
        DEFAULT_MACRO_SURPRISE_DATABASE,
        DEFAULT_MACRO_SURPRISE_STATE,
        DEFAULT_MARKDOWN,
        DEFAULT_NEWS_DATABASE,
        DEFAULT_REGISTRY,
        DEFAULT_SOURCE,
        build_report,
        close_forecast_contract_cache,
        utc_now,
    )

STATE = Path(__file__).resolve().parent / "data" / "oanda_training_manager" / "state"
DEFAULT_INPUT_CHECKPOINT = STATE / "edge_evidence_input_checkpoint_v1.json"
DEFAULT_MINIMUM_REBUILD_INTERVAL_SEC = 21600.0


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp"
    )
    try:
        temporary.write_text(
            json.dumps(value, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        for attempt in range(8):
            try:
                os.replace(temporary, path)
                return
            except PermissionError:
                if attempt == 7:
                    raise
                time.sleep(min(0.5, 0.01 * (2**attempt)))
    finally:
        temporary.unlink(missing_ok=True)


def file_snapshot(path: Path) -> dict[str, Any]:
    try:
        stat = path.stat()
    except OSError:
        return {"path": str(path.resolve()), "exists": False}
    material = f"{path.resolve()}|{stat.st_size}|{stat.st_mtime_ns}"
    return {
        "path": str(path.resolve()),
        "exists": True,
        "size_bytes": int(stat.st_size),
        "modified_ns": int(stat.st_mtime_ns),
        "snapshot_id": hashlib.sha256(material.encode("utf-8")).hexdigest()[:24],
    }


def content_sha256(path: Path) -> str | None:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return None


def output_integrity(*paths: Path) -> dict[str, dict[str, Any]]:
    """Return content-addressed identities for persisted report outputs."""
    return {
        str(path.resolve()): {
            "size_bytes": int(path.stat().st_size) if path.is_file() else None,
            "sha256": content_sha256(path),
        }
        for path in paths
    }


def checkpoint_outputs_match(
    checkpoint: dict[str, Any], *paths: Path
) -> bool:
    expected = checkpoint.get("output_integrity") or {}
    if not isinstance(expected, dict) or not expected:
        return False
    current = output_integrity(*paths)
    return all(
        isinstance(expected.get(key), dict)
        and expected[key].get("sha256")
        and expected[key].get("sha256") == value.get("sha256")
        and expected[key].get("size_bytes") == value.get("size_bytes")
        for key, value in current.items()
    )


def semantic_json_sha256(path: Path) -> str | None:
    """Hash JSON meaning while excluding volatile publication timestamps."""
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None

    def normalize(item: Any) -> Any:
        if isinstance(item, dict):
            return {
                key: normalize(child)
                for key, child in sorted(item.items())
                if key not in {"generated_utc", "updated_at", "last_updated_utc"}
            }
        if isinstance(item, list):
            return [normalize(child) for child in item]
        return item

    canonical = json.dumps(normalize(value), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def macro_state_sha256(path: Path) -> str | None:
    """Hash only macro fields consumed by the edge report."""
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    fields = (
        "status", "release_count", "scheduled_release_count", "actual_count",
        "consensus_count", "actual_and_consensus_count",
        "standardized_surprise_count", "total_revisions",
        "consensus_observation_count", "causal_consensus_observation_count",
        "reaction_sample_count", "ledger_database",
    )
    canonical = json.dumps(
        {field: value.get(field) for field in fields},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def sqlite_logical_snapshot(path: Path, role: str) -> dict[str, Any]:
    """Fingerprint logical highwaters, not WAL/checkpoint filesystem churn."""
    if not path.is_file():
        return {"path": str(path.resolve()), "exists": False, "role": role}
    specifications = {
        "source": {
            "outcomes": "MIN(row_id),MAX(row_id)",
            "canonical_outcomes": "MIN(row_id),MAX(row_id)",
            "canonical_forecasts": "MIN(rowid),MAX(rowid)",
            "forecast_integrity_events": "MIN(row_id),MAX(row_id)",
        },
        "news": {
            "topic_events": "COUNT(*),MIN(rowid),MAX(rowid)",
        },
        "macro": {
            "macro_release_revisions": "MIN(row_id),MAX(row_id)",
            "macro_consensus_observations": "COUNT(*),MIN(rowid),MAX(rowid)",
            "macro_reaction_samples": "COUNT(*),MIN(rowid),MAX(rowid)",
        },
        "candidate_cohorts": {
            "proof_cohorts": "COUNT(*),MIN(rowid),MAX(rowid),MAX(cohort_id)",
            "proof_cohort_transitions": "COUNT(*),MIN(rowid),MAX(rowid),MAX(observed_utc)",
        },
    }
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(
            f"file:{path.resolve().as_posix()}?mode=ro", uri=True, timeout=5.0
        )
        connection.execute("PRAGMA query_only=ON")
        schema_rows = connection.execute(
            "SELECT type,name,COALESCE(sql,'') FROM sqlite_master "
            "WHERE type IN ('table','index','trigger') ORDER BY type,name"
        ).fetchall()
        present = {
            row[0] for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        highwaters = {}
        for table, expression in specifications[role].items():
            if table in present:
                # SQLite's min/max shortcut applies to one aggregate at a
                # time.  ``SELECT MIN(rowid),MAX(rowid)`` scans the complete
                # 14M-row forecast ledger and pins its WAL during what should
                # be a cheap worker input fingerprint.  Independent scalar
                # subqueries retain the exact tuple while allowing indexed
                # MIN/MAX lookups.  COUNT remains exact for the smaller roles.
                aggregates = [
                    item.strip() for item in expression.split(",") if item.strip()
                ]
                query = "SELECT " + ",".join(
                    f'(SELECT {item} FROM "{table}")' for item in aggregates
                )
                highwaters[table] = list(
                    connection.execute(query).fetchone()
                )
        schema = json.dumps(schema_rows, separators=(",", ":"), default=str)
        return {
            "path": str(path.resolve()),
            "exists": True,
            "role": role,
            "schema_sha256": hashlib.sha256(schema.encode("utf-8")).hexdigest(),
            "highwaters": highwaters,
        }
    except (OSError, sqlite3.Error) as exc:
        # A transiently unreadable dependency must change the fingerprint and
        # therefore fail toward a rebuild, never toward a false no-change skip.
        return {
            **file_snapshot(path),
            "role": role,
            "logical_snapshot_error": f"{type(exc).__name__}: {exc}",
        }
    finally:
        if connection is not None:
            connection.close()


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def input_fingerprint(
    *, source_database: Path, news_database: Path, registry: Path,
) -> dict[str, Any]:
    """Fingerprint inputs that require rebuilding the multi-gigabyte evidence report.

    News and macro-coverage state feed only the report's point-in-time
    diagnostic coverage block.  They do not participate in cell statistics,
    lifecycle gates, allocator evidence, or authorization.  Treating routine
    topic/release/reaction growth as a heavy input caused repeated full scans
    of the multi-million-row price/outcome ledger.  Keep both logical
    highwaters as diagnostics, but do not let them invalidate the governed
    price/outcome evidence report.
    """
    content_paths = [
        registry, DEFAULT_GOVERNANCE_CONFIG,
        Path(build_report.__code__.co_filename).resolve(),
    ]
    semantic_payload = {
        "database_snapshots": [
            sqlite_logical_snapshot(source_database, "source"),
            sqlite_logical_snapshot(
                DEFAULT_CANDIDATE_COHORT_DATABASE, "candidate_cohorts"
            ),
        ],
        "content_sha256": {
            str(path.resolve()): content_sha256(path) for path in content_paths
        },
        "cohort_definition_sha256": {
            str(path.resolve()): semantic_json_sha256(path)
            for path in (
                DEFAULT_COHORT_STATE,
                DEFAULT_CANDIDATE_COHORT_STATE,
            )
        },
    }
    canonical = json.dumps(
        semantic_payload, sort_keys=True, separators=(",", ":")
    )
    return {
        **semantic_payload,
        "diagnostic_database_snapshots": [
            sqlite_logical_snapshot(news_database, "news"),
            sqlite_logical_snapshot(DEFAULT_MACRO_SURPRISE_DATABASE, "macro"),
        ],
        "diagnostic_macro_state_sha256": macro_state_sha256(
            DEFAULT_MACRO_SURPRISE_STATE
        ),
        "fingerprint_sha256": hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
    }


def checkpoint_matches_semantic_inputs(
    checkpoint: dict[str, Any], current: dict[str, Any]
) -> bool:
    """Recognize a prior checkpoint when only news/macro diagnostics differ."""

    if not checkpoint or not current:
        return False
    prior_databases = [
        row
        for row in checkpoint.get("database_snapshots") or []
        if isinstance(row, dict) and row.get("role") not in {"news", "macro"}
    ]
    current_databases = [
        row
        for row in current.get("database_snapshots") or []
        if isinstance(row, dict) and row.get("role") not in {"news", "macro"}
    ]
    current_content = current.get("content_sha256") or {}
    prior_content = checkpoint.get("content_sha256") or {}
    return bool(
        prior_databases == current_databases
        and all(prior_content.get(path) == digest for path, digest in current_content.items())
        and checkpoint.get("cohort_definition_sha256")
        == current.get("cohort_definition_sha256")
    )


def _utc_age_seconds(value: Any) -> float | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return max(0.0, (datetime.now(timezone.utc) - parsed).total_seconds())


def _snapshot_by_role(payload: dict[str, Any], role: str) -> dict[str, Any]:
    for row in payload.get("database_snapshots") or []:
        if isinstance(row, dict) and row.get("role") == role:
            return row
    return {}


def checkpoint_can_defer_append_only_growth(
    checkpoint: dict[str, Any],
    current: dict[str, Any],
    *,
    minimum_rebuild_interval_sec: float,
) -> bool:
    """Defer only ordinary source-ledger growth inside a bounded snapshot cadence.

    A code, governance, cohort, schema, path, or availability change must still
    rebuild immediately. Only source highwaters may differ. This prevents a
    continuously appended outcome ledger from keeping the read-only worker in
    a permanent multi-hour rescan loop while preserving prompt invalidation of
    every material research-contract change.
    """
    age = _utc_age_seconds(checkpoint.get("completed_utc"))
    if age is None or age >= max(0.0, float(minimum_rebuild_interval_sec)):
        return False
    if checkpoint.get("content_sha256") != current.get("content_sha256"):
        return False
    if checkpoint.get("cohort_definition_sha256") != current.get(
        "cohort_definition_sha256"
    ):
        return False
    if _snapshot_by_role(checkpoint, "candidate_cohorts") != _snapshot_by_role(
        current, "candidate_cohorts"
    ):
        return False
    previous_source = _snapshot_by_role(checkpoint, "source")
    current_source = _snapshot_by_role(current, "source")
    if not previous_source or not current_source:
        return False
    for key in ("path", "exists", "role", "schema_sha256"):
        if previous_source.get(key) != current_source.get(key):
            return False
    # Empty/no-change highwaters belong to the exact-fingerprint path. This
    # branch is specifically for a healthy append-only lag.
    return previous_source.get("highwaters") != current_source.get("highwaters")


def process_rss_bytes() -> int | None:
    if psutil is None:
        return None
    try:
        return int(psutil.Process(os.getpid()).memory_info().rss)
    except (OSError, psutil.Error):
        return None


class ProgressHeartbeat:
    """Write liveness separately from the large completed-report artifact."""

    def __init__(
        self,
        path: Path,
        *,
        source_database: Path,
        evidence_database: Path,
        interval_sec: float = 15.0,
    ) -> None:
        self.path = Path(path)
        self.source_database = Path(source_database)
        self.evidence_database = Path(evidence_database)
        self.interval_sec = max(1.0, float(interval_sec))
        self.started_monotonic = time.monotonic()
        self.started_utc = utc_now()
        self.phase_started_monotonic = self.started_monotonic
        self.last_progress_monotonic = self.started_monotonic
        self.last_progress_utc = self.started_utc
        self.phase = "starting"
        self.status = "running"
        self.progress_sequence = 0
        self.cycles = 0
        self.errors = 0
        self.last_error = ""
        self.details: dict[str, Any] = {}
        self._lock = threading.Lock()
        self._publish_lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name="edge-evidence-heartbeat",
            daemon=True,
        )

    def start(self) -> None:
        self.publish()
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=max(2.0, self.interval_sec * 2.0))
        self.publish()

    def update(self, phase: str, details: dict[str, Any] | None = None) -> None:
        with self._lock:
            now = time.monotonic()
            if phase != self.phase:
                self.phase_started_monotonic = now
            self.phase = str(phase)
            self.last_progress_monotonic = now
            self.last_progress_utc = utc_now()
            self.progress_sequence += 1
            if details:
                self.details.update(details)
        self.publish()

    def set_result(
        self,
        *,
        cycles: int,
        errors: int,
        last_error: str,
        compact: dict[str, Any],
    ) -> None:
        with self._lock:
            self.cycles = int(cycles)
            self.errors = int(errors)
            self.last_error = str(last_error)
            self.status = "degraded" if last_error else "running"
            self.details.update(compact)
        self.publish()

    def payload(self) -> dict[str, Any]:
        with self._lock:
            now = time.monotonic()
            rss = process_rss_bytes()
            details = dict(self.details)
            details.update(
                {
                    "source_database": file_snapshot(self.source_database),
                    "evidence_database": file_snapshot(self.evidence_database),
                    "rss_bytes": rss,
                    "rss_mb": None if rss is None else round(rss / 1_048_576.0, 1),
                }
            )
            return {
                "schema_version": 2,
                "generated_utc": utc_now(),
                "updated_at": utc_now(),
                "started_at": self.started_utc,
                "pid": os.getpid(),
                "worker": "oanda_edge_evidence_worker",
                "role": "research_only_evidence_builder",
                "research_only": True,
                "can_place_orders": False,
                "can_promote": False,
                "status": self.status,
                "phase": self.phase,
                "phase_age_sec": round(now - self.phase_started_monotonic, 3),
                "progress_age_sec": round(now - self.last_progress_monotonic, 3),
                "last_progress_utc": self.last_progress_utc,
                "progress_sequence": self.progress_sequence,
                "uptime_sec": round(now - self.started_monotonic, 3),
                "cycles": self.cycles,
                "errors": self.errors,
                "last_error": self.last_error,
                "details": details,
            }

    def publish(self) -> None:
        with self._publish_lock:
            atomic_json(self.path, self.payload())

    def wait(self, seconds: float) -> bool:
        return self._stop.wait(max(0.0, float(seconds)))

    def _run(self) -> None:
        while not self._stop.wait(self.interval_sec):
            self.publish()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-database", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--evidence-database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--output-json", type=Path, default=DEFAULT_JSON)
    parser.add_argument("--output-markdown", type=Path, default=DEFAULT_MARKDOWN)
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument("--news-database", type=Path, default=DEFAULT_NEWS_DATABASE)
    parser.add_argument("--heartbeat", type=Path, default=STATE / "edge_evidence_worker_v1.json")
    parser.add_argument("--input-checkpoint", type=Path, default=DEFAULT_INPUT_CHECKPOINT)
    parser.add_argument("--heartbeat-sec", type=float, default=15.0)
    parser.add_argument("--interval-sec", type=float, default=900.0)
    parser.add_argument(
        "--minimum-rebuild-interval-sec",
        type=float,
        default=DEFAULT_MINIMUM_REBUILD_INTERVAL_SEC,
        help=(
            "Minimum age of a verified completed snapshot before ordinary "
            "append-only source growth triggers another full rebuild."
        ),
    )
    parser.add_argument("--duration-sec", type=float, default=0.0)
    args = parser.parse_args()
    stop_at = time.monotonic() + args.duration_sec if args.duration_sec > 0 else None
    cycles = errors = skipped_unchanged = 0
    # Forecast rows are trigger-enforced immutable. Outcome maturation can
    # require a new report without changing that multi-million-row snapshot,
    # so retain its exact contract audit for this worker lifetime. The evidence
    # module validates source file identity, schema, triggers, and high-water
    # before every reuse.
    forecast_contract_cache: dict[str, Any] = {}
    heartbeat = ProgressHeartbeat(
        args.heartbeat,
        source_database=args.source_database,
        evidence_database=args.evidence_database,
        interval_sec=args.heartbeat_sec,
    )
    heartbeat.start()
    try:
        while True:
            error = ""
            compact: dict[str, Any] = {}
            heartbeat.update("starting_cycle", {"next_cycle": cycles + 1})
            fingerprint = input_fingerprint(
                source_database=args.source_database,
                news_database=args.news_database,
                registry=args.registry,
            )
            checkpoint = read_json(args.input_checkpoint)
            heartbeat.update(
                "input_snapshot_captured",
                {
                    "input_fingerprint_sha256": fingerprint["fingerprint_sha256"],
                    "completed_report_fingerprint_sha256": checkpoint.get(
                        "fingerprint_sha256"
                    ),
                    "completed_report_age_sec": _utc_age_seconds(
                        checkpoint.get("completed_utc")
                    ),
                },
            )
            outputs_match = bool(
                args.output_json.is_file()
                and args.output_markdown.is_file()
                and checkpoint_outputs_match(
                    checkpoint, args.output_json, args.output_markdown
                )
            )
            exact_or_migratable = bool(
                checkpoint.get("fingerprint_sha256")
                == fingerprint["fingerprint_sha256"]
                or checkpoint_matches_semantic_inputs(checkpoint, fingerprint)
            )
            bounded_append_only_lag = bool(
                not exact_or_migratable
                and outputs_match
                and checkpoint_can_defer_append_only_growth(
                    checkpoint,
                    fingerprint,
                    minimum_rebuild_interval_sec=args.minimum_rebuild_interval_sec,
                )
            )
            if outputs_match and (exact_or_migratable or bounded_append_only_lag):
                skipped_unchanged += 1
                compact = dict(checkpoint.get("last_completed_report") or {})
                if (
                    exact_or_migratable
                    and checkpoint.get("fingerprint_sha256")
                    != fingerprint["fingerprint_sha256"]
                ):
                    atomic_json(
                        args.input_checkpoint,
                        {
                            **fingerprint,
                            "completed_utc": checkpoint.get("completed_utc") or utc_now(),
                            "last_completed_report": compact,
                            "output_integrity": output_integrity(
                                args.output_json, args.output_markdown
                            ),
                            "research_only": True,
                            "can_place_orders": False,
                            "can_promote": False,
                            "checkpoint_migration": "news_macro_diagnostic_split_v2",
                        },
                    )
                compact["skipped_unchanged_cycles"] = skipped_unchanged
                heartbeat.set_result(
                    cycles=cycles, errors=errors, last_error="", compact=compact
                )
                heartbeat.update(
                    (
                        "idle_bounded_snapshot_lag"
                        if bounded_append_only_lag
                        else "idle_unchanged_inputs"
                    ),
                    {
                        "sleep_sec": max(60.0, args.interval_sec),
                        "input_fingerprint_sha256": fingerprint[
                            "fingerprint_sha256"
                        ],
                        "completed_report_fingerprint_sha256": checkpoint.get(
                            "fingerprint_sha256"
                        ),
                        "completed_report_age_sec": _utc_age_seconds(
                            checkpoint.get("completed_utc")
                        ),
                        "minimum_rebuild_interval_sec": max(
                            0.0, float(args.minimum_rebuild_interval_sec)
                        ),
                        "append_only_snapshot_lag": bounded_append_only_lag,
                    },
                )
                if stop_at is not None and time.monotonic() >= stop_at:
                    return 0
                if heartbeat.wait(max(60.0, args.interval_sec)):
                    return 0
                continue
            try:
                report = build_report(
                    args.source_database,
                    evidence_database=args.evidence_database,
                    output_json=args.output_json,
                    output_markdown=args.output_markdown,
                    registry_path=args.registry,
                    news_database=args.news_database,
                    progress_callback=heartbeat.update,
                    forecast_contract_cache=forecast_contract_cache,
                )
                cycles += 1
                compact = {
                    "run_id": report["run_id"],
                    "usable_signal_outcomes": report["integrity"]["usable_signal_outcomes"],
                    "source_highwater_row_id": report["integrity"][
                        "source_highwater_row_id"
                    ],
                    "cell_count": report["cell_count"],
                    "passing_cell_count": report["passing_cell_count"],
                    "allocator_action": report["shadow_allocator"]["action"],
                }
                atomic_json(
                    args.input_checkpoint,
                    {
                        **fingerprint,
                        "completed_utc": utc_now(),
                        "last_completed_report": compact,
                        "output_integrity": output_integrity(
                            args.output_json, args.output_markdown
                        ),
                        "research_only": True,
                        "can_place_orders": False,
                        "can_promote": False,
                    },
                )
            except Exception as exc:
                errors += 1
                error = f"{type(exc).__name__}: {exc}"
            finally:
                # ``build_report`` returns the full cell/evidence document.
                # Keeping that local alive during the 15-minute idle interval
                # retained several gigabytes even though only ``compact`` is
                # needed after persistence.  Release it before sleeping so a
                # read-only governance worker cannot starve the live stack.
                if "report" in locals():
                    report = None
                gc.collect()
            heartbeat.set_result(
                cycles=cycles,
                errors=errors,
                last_error=error,
                compact=compact,
            )
            heartbeat.update(
                "idle_between_cycles",
                {"sleep_sec": max(60.0, args.interval_sec)},
            )
            if stop_at is not None and time.monotonic() >= stop_at:
                return 0
            if heartbeat.wait(max(60.0, args.interval_sec)):
                return 0
    finally:
        close_forecast_contract_cache(forecast_contract_cache)
        heartbeat.stop()


if __name__ == "__main__":
    raise SystemExit(main())
