#!/usr/bin/env python3
"""Preflight the canonical spike/blurb factor-reconstruction evidence surface.

The preflight is deliberately read-only with respect to market/source evidence.
It fingerprints retained executable candles, reconciles the legacy movement-label
period against those files, and inventories the canonical movement and source
databases.  It never infers missing prices, assigns a direction to an unresolved
legacy label, promotes a candidate, authorizes an account, or places an order.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import math
import os
import sqlite3
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping


ROOT = Path(__file__).resolve().parent
CONFIG = ROOT / "config" / "spike_blurb_factor_reconstruction_v1.json"
STATE = ROOT / "data" / "oanda_training_manager" / "state"
CANDLES = ROOT / "data" / "oanda_training_manager" / "candles"
LEGACY_TAGS = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "news_event_tags"
    / "significant_move_news_tags.csv"
)
MOVEMENT_DB = STATE / "movement_news_episode_research_v1.sqlite"
SOURCE_DB = STATE / "source_governance_v1.sqlite"
MACRO_DB = STATE / "macro_surprise_v1.sqlite"
DIRECT_RESPONSE_DB = STATE / "direct_source_response_v1.sqlite"
REPORT_ROOT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "spike_blurb_factor_reconstruction"
)
OUTPUT_JSON = REPORT_ROOT / "SPIKE_BLURB_RECONSTRUCTION_PREFLIGHT_V1.json"
OUTPUT_MD = REPORT_ROOT / "SPIKE_BLURB_RECONSTRUCTION_PREFLIGHT_V1.md"

EXECUTABLE_COLUMNS = (
    "bid_open",
    "bid_high",
    "bid_low",
    "bid_close",
    "ask_open",
    "ask_high",
    "ask_low",
    "ask_close",
)


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parse_epoch(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        number = float(value)
        if math.isfinite(number) and number > 1_000_000_000:
            return int(number)
    except (TypeError, ValueError):
        pass
    try:
        parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return int(parsed.timestamp())


def iso_epoch(epoch: int | None) -> str:
    if epoch is None:
        return ""
    return dt.datetime.fromtimestamp(epoch, dt.timezone.utc).isoformat()


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def load_contract(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("contract_must_be_object")
    required_safety = {
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_authorize": False,
        "can_promote": False,
        "supported_execution_decision": "no_trade",
    }
    for key, expected in required_safety.items():
        if type(payload.get(key)) is not type(expected) or payload.get(key) != expected:
            raise ValueError(f"unsafe_contract_field:{key}")
    instruments = payload.get("expected_instruments")
    currencies = payload.get("expected_currencies")
    horizons = payload.get("horizons_minutes")
    if not isinstance(instruments, list) or len(instruments) != 68 or len(set(instruments)) != 68:
        raise ValueError("expected_instrument_universe_invalid")
    if not isinstance(currencies, list) or len(currencies) != 21 or len(set(currencies)) != 21:
        raise ValueError("expected_currency_universe_invalid")
    if not isinstance(horizons, list) or not horizons or horizons != sorted(set(horizons)):
        raise ValueError("horizon_universe_invalid")
    if horizons[0] != 1 or horizons[-1] != 43_200:
        raise ValueError("horizon_boundary_invalid")
    derived = sorted({currency for pair in instruments for currency in str(pair).split("_")})
    if derived != sorted(currencies):
        raise ValueError("pair_currency_universe_mismatch")
    return payload


def _finite(value: Any) -> bool:
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def audit_candle_file(path: Path, instrument: str) -> dict[str, Any]:
    row_count = 0
    valid_timestamp_rows = 0
    executable_rows = 0
    malformed_rows = 0
    duplicate_or_nonmonotonic_rows = 0
    first_epoch: int | None = None
    last_epoch: int | None = None
    prior_epoch: int | None = None
    observed_instruments: set[str] = set()
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        columns = list(reader.fieldnames or [])
        for row in reader:
            row_count += 1
            epoch = parse_epoch(row.get("time") or row.get("datetime"))
            observed = str(row.get("instrument") or "").strip()
            if observed:
                observed_instruments.add(observed)
            if epoch is None:
                malformed_rows += 1
                continue
            valid_timestamp_rows += 1
            if prior_epoch is not None and epoch <= prior_epoch:
                duplicate_or_nonmonotonic_rows += 1
            prior_epoch = epoch
            first_epoch = epoch if first_epoch is None else min(first_epoch, epoch)
            last_epoch = epoch if last_epoch is None else max(last_epoch, epoch)
            if all(_finite(row.get(column)) for column in EXECUTABLE_COLUMNS):
                executable_rows += 1
    missing_columns = sorted(
        {"time", "datetime", "instrument", *EXECUTABLE_COLUMNS}.difference(columns)
    )
    timestamp_column_present = "time" in columns or "datetime" in columns
    if not timestamp_column_present:
        missing_columns = [value for value in missing_columns if value not in {"time", "datetime"}]
        missing_columns.append("time_or_datetime")
    complete = (
        row_count > 0
        and not missing_columns
        and observed_instruments == {instrument}
        and valid_timestamp_rows == row_count
        and executable_rows == row_count
        and duplicate_or_nonmonotonic_rows == 0
    )
    return {
        "instrument": instrument,
        "path": str(path.resolve()),
        "bytes": path.stat().st_size,
        "sha256": file_sha256(path),
        "columns": columns,
        "row_count": row_count,
        "valid_timestamp_rows": valid_timestamp_rows,
        "executable_bid_ask_rows": executable_rows,
        "malformed_rows": malformed_rows,
        "duplicate_or_nonmonotonic_rows": duplicate_or_nonmonotonic_rows,
        "observed_instruments": sorted(observed_instruments),
        "first_utc": iso_epoch(first_epoch),
        "last_utc": iso_epoch(last_epoch),
        "first_epoch": first_epoch,
        "last_epoch": last_epoch,
        "missing_columns": sorted(set(missing_columns)),
        "contract_complete": complete,
    }


def audit_candle_archive(root: Path, expected: Iterable[str]) -> dict[str, Any]:
    expected_set = set(expected)
    actual_paths = {path.stem.removesuffix("_M1"): path for path in root.glob("*_M1.csv")}
    records = [
        audit_candle_file(actual_paths[instrument], instrument)
        for instrument in sorted(expected_set.intersection(actual_paths))
    ]
    first_epochs = [record["first_epoch"] for record in records if record["first_epoch"]]
    last_epochs = [record["last_epoch"] for record in records if record["last_epoch"]]
    manifest_material = [
        {key: record[key] for key in ("instrument", "bytes", "sha256", "row_count", "first_utc", "last_utc")}
        for record in records
    ]
    return {
        "expected_count": len(expected_set),
        "actual_expected_count": len(records),
        "missing_instruments": sorted(expected_set.difference(actual_paths)),
        "unexpected_instruments": sorted(set(actual_paths).difference(expected_set)),
        "complete_file_count": sum(bool(record["contract_complete"]) for record in records),
        "total_rows": sum(int(record["row_count"]) for record in records),
        "total_bytes": sum(int(record["bytes"]) for record in records),
        "earliest_utc": iso_epoch(min(first_epochs)) if first_epochs else "",
        "latest_utc": iso_epoch(max(last_epochs)) if last_epochs else "",
        "common_start_utc": iso_epoch(max(first_epochs)) if first_epochs else "",
        "common_end_utc": iso_epoch(min(last_epochs)) if last_epochs else "",
        "price_manifest_sha256": sha256_bytes(canonical_json(manifest_material).encode("utf-8")),
        "files": records,
    }


def audit_legacy_tags(path: Path, candle_records: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    coverage = {
        str(record["instrument"]): (record.get("first_epoch"), record.get("last_epoch"))
        for record in candle_records
    }
    rows = 0
    malformed = 0
    exact_range_candidates = 0
    has_news_match = 0
    predictive_mapping = 0
    directional_mapping = 0
    verified_source = 0
    first_epoch: int | None = None
    last_epoch: int | None = None
    instruments: set[str] = set()
    match_states: Counter[str] = Counter()
    causal_relations: Counter[str] = Counter()
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            rows += 1
            instrument = str(row.get("instrument") or "")
            start = parse_epoch(row.get("start_utc"))
            end = parse_epoch(row.get("end_utc"))
            if "_" not in instrument or start is None or end is None or end <= start:
                malformed += 1
                continue
            instruments.add(instrument)
            first_epoch = start if first_epoch is None else min(first_epoch, start)
            last_epoch = start if last_epoch is None else max(last_epoch, start)
            match_state = str(row.get("news_match_status") or "unknown")
            relation = str(row.get("primary_causal_relation") or "unknown")
            match_states[match_state] += 1
            causal_relations[relation] += 1
            if match_state == "matched":
                has_news_match += 1
            if str(row.get("primary_predictive_eligible") or "").lower() == "true":
                predictive_mapping += 1
            if str(row.get("primary_expected_pair_direction") or "") in {"LONG", "SHORT"}:
                directional_mapping += 1
            if str(row.get("primary_source_verified") or "").lower() == "true":
                verified_source += 1
            candle_start, candle_end = coverage.get(instrument, (None, None))
            if candle_start is not None and candle_end is not None and candle_start <= start <= end <= candle_end:
                exact_range_candidates += 1
    return {
        "path": str(path.resolve()),
        "sha256": file_sha256(path),
        "row_count": rows,
        "malformed_rows": malformed,
        "instrument_count": len(instruments),
        "first_start_utc": iso_epoch(first_epoch),
        "last_start_utc": iso_epoch(last_epoch),
        "news_matched_rows": has_news_match,
        "predictive_mapping_rows": predictive_mapping,
        "directional_mapping_rows": directional_mapping,
        "verified_source_rows": verified_source,
        "within_current_candle_range_rows": exact_range_candidates,
        "requires_historical_price_reacquisition_rows": rows - malformed - exact_range_candidates,
        "news_match_states": dict(sorted(match_states.items())),
        "causal_relation_states": dict(sorted(causal_relations.items())),
        "retained_limit": "legacy labels do not retain executable direction or magnitude",
        "evidence_class": "outcome_selected_diagnostic_not_forecast_proof",
    }


def open_readonly(path: Path) -> sqlite3.Connection:
    if not path.exists():
        raise FileNotFoundError(path)
    database = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True, timeout=60)
    database.execute("PRAGMA query_only=ON")
    return database


def _table_count(database: sqlite3.Connection, table: str) -> int:
    return int(database.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])


def audit_database(path: Path, kind: str) -> dict[str, Any]:
    database = open_readonly(path)
    tables = [
        row[0]
        for row in database.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        if not str(row[0]).startswith("sqlite_")
    ]
    counts = {table: _table_count(database, table) for table in tables}
    result: dict[str, Any] = {
        "path": str(path.resolve()),
        "bytes": path.stat().st_size,
        "tables": counts,
        "sqlite_integrity": str(database.execute("PRAGMA integrity_check").fetchone()[0]),
    }
    if kind == "movement" and "movement_episodes" in tables:
        row = database.execute(
            "SELECT MIN(entry_utc),MAX(entry_utc),COUNT(DISTINCT instrument),"
            "COUNT(DISTINCT market_episode_id),COUNT(DISTINCT horizon_min) FROM movement_episodes"
        ).fetchone()
        result["coverage"] = {
            "first_entry_utc": row[0],
            "last_entry_utc": row[1],
            "instrument_count": int(row[2]),
            "market_episode_count": int(row[3]),
            "horizon_count": int(row[4]),
        }
    elif kind == "source" and "source_events" in tables:
        row = database.execute(
            "SELECT MIN(first_seen_at_utc),MAX(first_seen_at_utc),COUNT(DISTINCT source_id),"
            "COUNT(DISTINCT source_family),COUNT(DISTINCT story_cluster_id),"
            "SUM(CASE WHEN source_population='official' OR source_family LIKE 'official%' THEN 1 ELSE 0 END) "
            "FROM source_events"
        ).fetchone()
        result["coverage"] = {
            "first_seen_utc": row[0],
            "last_seen_utc": row[1],
            "source_id_count": int(row[2] or 0),
            "source_family_count": int(row[3] or 0),
            "story_cluster_count": int(row[4] or 0),
            "official_classified_event_count": int(row[5] or 0),
        }
    elif kind == "macro":
        result["coverage"] = {
            "release_revision_count": counts.get("macro_release_revisions", 0),
            "causal_consensus_observation_count": counts.get("macro_consensus_observations", 0),
            "reaction_sample_count": counts.get("macro_reaction_samples", 0),
        }
    elif kind == "response":
        result["coverage"] = {
            "source_observation_count": counts.get("source_observations", 0),
            "response_target_count": counts.get("response_targets", 0),
        }
    database.close()
    return result


def build_preflight(
    *,
    contract_path: Path = CONFIG,
    candle_root: Path = CANDLES,
    legacy_tags: Path = LEGACY_TAGS,
    movement_db: Path = MOVEMENT_DB,
    source_db: Path = SOURCE_DB,
    macro_db: Path = MACRO_DB,
    response_db: Path = DIRECT_RESPONSE_DB,
) -> dict[str, Any]:
    contract = load_contract(contract_path)
    candles = audit_candle_archive(candle_root, contract["expected_instruments"])
    legacy = audit_legacy_tags(legacy_tags, candles["files"])
    databases = {
        "movement": audit_database(movement_db, "movement"),
        "source": audit_database(source_db, "source"),
        "macro": audit_database(macro_db, "macro"),
        "direct_response": audit_database(response_db, "response"),
    }
    blockers: list[dict[str, Any]] = []
    if candles["actual_expected_count"] != contract["expected_instrument_count"]:
        blockers.append({"id": "missing_current_pair_files", "count": len(candles["missing_instruments"])})
    if candles["complete_file_count"] != contract["expected_instrument_count"]:
        blockers.append(
            {
                "id": "incomplete_executable_candle_files",
                "count": contract["expected_instrument_count"] - candles["complete_file_count"],
            }
        )
    historical_gap = int(legacy["requires_historical_price_reacquisition_rows"])
    if historical_gap:
        blockers.append({"id": "legacy_executable_price_archive_gap", "count": historical_gap})
    if int(databases["macro"]["coverage"]["causal_consensus_observation_count"]) == 0:
        blockers.append({"id": "causal_pre_release_consensus_absent", "count": 1})
    snapshot_core = {
        "schema_version": 1,
        "contract_id": contract["contract_id"],
        "cohort_id": contract["cohort_id"],
        "generated_utc": utc_now(),
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "supported_execution_decision": "no_trade",
        "config_path": str(contract_path.resolve()),
        "config_sha256": file_sha256(contract_path),
        "expected_horizons_minutes": contract["horizons_minutes"],
        "expected_currency_count": contract["expected_currency_count"],
        "expected_instrument_count": contract["expected_instrument_count"],
        "price_archive": candles,
        "legacy_reconciliation": legacy,
        "database_inventory": databases,
        "blockers": blockers,
        "next_build_steps": [
            "materialize immutable movement/source/factor/response/entry ledger schema",
            "reconstruct all retained bid/ask movement candidates at every frozen horizon",
            "reacquire or formally classify missing historical executable candle windows",
            "trace movement candidates to the purest source without using later information for entry",
            "extract numeric factor deltas and lower-grade semantic factors with explicit uncertainty",
            "replay event-first response-detected entry arms and retain no-trade controls",
        ],
    }
    snapshot_core["snapshot_sha256"] = sha256_bytes(canonical_json(snapshot_core).encode("utf-8"))
    return snapshot_core


def render_report(snapshot: Mapping[str, Any]) -> str:
    price = snapshot["price_archive"]
    legacy = snapshot["legacy_reconciliation"]
    databases = snapshot["database_inventory"]
    movement = databases["movement"]["coverage"]
    source = databases["source"]["coverage"]
    lines = [
        "# Spike/Blurb Factor Reconstruction — Preflight V1",
        "",
        f"- Generated: `{snapshot['generated_utc']}`",
        f"- Contract: `{snapshot['contract_id']}`",
        f"- Snapshot SHA-256: `{snapshot['snapshot_sha256']}`",
        "- Safety: **research-only / execution-ineligible / no-trade**",
        "",
        "## Reconstructable evidence now",
        "",
        f"- Executable M1 pair files: **{price['actual_expected_count']}/{price['expected_count']}**",
        f"- Fully contract-complete files: **{price['complete_file_count']}/{price['expected_count']}**",
        f"- Retained executable rows: **{price['total_rows']:,}**",
        f"- Archive span: `{price['earliest_utc']}` through `{price['latest_utc']}`",
        f"- Common all-pair span: `{price['common_start_utc']}` through `{price['common_end_utc']}`",
        f"- Canonical movement episodes: **{databases['movement']['tables'].get('movement_episodes', 0):,}** across **{movement['instrument_count']}** instruments and **{movement['market_episode_count']:,}** factor episodes",
        f"- Source events: **{databases['source']['tables'].get('source_events', 0):,}** from **{source['source_id_count']:,}** source IDs",
        f"- Source story clusters: **{source['story_cluster_count']:,}**",
        f"- Direct-source observations/targets: **{databases['direct_response']['coverage']['source_observation_count']:,} / {databases['direct_response']['coverage']['response_target_count']:,}**",
        "",
        "## Legacy reconciliation",
        "",
        f"- Legacy move labels: **{legacy['row_count']:,}** across **{legacy['instrument_count']}** instruments",
        f"- Historical span: `{legacy['first_start_utc']}` through `{legacy['last_start_utc']}`",
        f"- Within current local candle ranges: **{legacy['within_current_candle_range_rows']:,}**",
        f"- Require historical executable price reacquisition: **{legacy['requires_historical_price_reacquisition_rows']:,}**",
        f"- News-matched: **{legacy['news_matched_rows']:,}**; directional mapping retained: **{legacy['directional_mapping_rows']:,}**",
        "",
        "The legacy rows are outcome-selected diagnostic labels. Missing direction or bid/ask history is an evidence gap, not a losing forecast and not permission to synthesize prices.",
        "",
        "## Blocking evidence gaps",
        "",
    ]
    for blocker in snapshot["blockers"]:
        lines.append(f"- `{blocker['id']}`: **{int(blocker['count']):,}**")
    lines.extend(["", "## Frozen next build", ""])
    for index, step in enumerate(snapshot["next_build_steps"], start=1):
        lines.append(f"{index}. {step.capitalize()}.")
    lines.extend(
        [
            "",
            "The output is a coverage/integrity preflight, not evidence of predictive edge. It cannot promote, authorize, route, or place an order.",
            "",
        ]
    )
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=CONFIG)
    parser.add_argument("--candles", type=Path, default=CANDLES)
    parser.add_argument("--legacy-tags", type=Path, default=LEGACY_TAGS)
    parser.add_argument("--movement-db", type=Path, default=MOVEMENT_DB)
    parser.add_argument("--source-db", type=Path, default=SOURCE_DB)
    parser.add_argument("--macro-db", type=Path, default=MACRO_DB)
    parser.add_argument("--response-db", type=Path, default=DIRECT_RESPONSE_DB)
    parser.add_argument("--output-json", type=Path, default=OUTPUT_JSON)
    parser.add_argument("--output-md", type=Path, default=OUTPUT_MD)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    snapshot = build_preflight(
        contract_path=args.config,
        candle_root=args.candles,
        legacy_tags=args.legacy_tags,
        movement_db=args.movement_db,
        source_db=args.source_db,
        macro_db=args.macro_db,
        response_db=args.response_db,
    )
    atomic_text(args.output_json, json.dumps(snapshot, indent=2, sort_keys=True) + "\n")
    atomic_text(args.output_md, render_report(snapshot))
    print(
        json.dumps(
            {
                "snapshot_sha256": snapshot["snapshot_sha256"],
                "current_pair_files": snapshot["price_archive"]["actual_expected_count"],
                "legacy_labels": snapshot["legacy_reconciliation"]["row_count"],
                "legacy_price_reacquisition_required": snapshot["legacy_reconciliation"]["requires_historical_price_reacquisition_rows"],
                "blockers": snapshot["blockers"],
                "output_json": str(args.output_json),
                "output_md": str(args.output_md),
                "supported_execution_decision": "no_trade",
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
