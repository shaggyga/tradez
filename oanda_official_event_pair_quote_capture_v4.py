"""Append-boundary pair quotes for the dedicated research price stream.

The old all-68 and V1/V2/V3 ledgers keep their contracts. This separate table
is populated only when a new raw official observation is committed. It reuses
V1's pair arithmetic, with an explicit successor metadata contract: a complete
account universe is not required, but every supplied count and current-source
identity must agree. Missing, stale and nontradeable pairs remain exclusions.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
from pathlib import Path
import sqlite3
from typing import Any, Mapping

ROOT = Path(__file__).resolve().parent
SCHEMA_VERSION = "official_event_pair_quote_capture_v4"
CONTRACT_ID = "official_event_pair_quote_capture_v4_dedicated_append_read_clock_20260913"
COHORT_ID = "official_event_pair_quote_capture_v4_20260913a"
SNAPSHOT_PRODUCER = "practice_007_dedicated_quote_stream"
V1_SHA256 = "40ceed485e6143a6388324f68a29d5786e30f28c3c2f2b79a28cbe49dabae1d0"
MAX_SNAPSHOT_AGE_SEC = 5.0
MAX_CAPTURE_LATENCY_SEC = 15.0


def iso_utc(value: dt.datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("aware_clock_required")
    return value.astimezone(dt.timezone.utc).isoformat()


def parse_time(value: Any) -> dt.datetime | None:
    if not isinstance(value, str):
        return None
    try:
        stamp = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        return stamp.astimezone(dt.timezone.utc) if stamp.tzinfo is not None else None
    except ValueError:
        return None


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _legacy():
    path = ROOT / "oanda_official_event_pair_quote_capture_v1.py"
    if hashlib.sha256(path.read_bytes()).hexdigest() != V1_SHA256:
        raise ValueError("pinned_pair_quote_v1_changed")
    # Lazy import avoids the legacy module's fast-lane import cycle.
    import oanda_official_event_pair_quote_capture_v1 as legacy
    return legacy


def _binding() -> dict[str, str]:
    return {
        "contract_id": CONTRACT_ID,
        "cohort_id": COHORT_ID,
        "producer_source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "required_v1_sha256": V1_SHA256,
        "snapshot_producer": SNAPSHOT_PRODUCER,
        "append_owner_sha256": hashlib.sha256((ROOT/"oanda_official_release_fast_lane.py").read_bytes()).hexdigest(),
        "quote_transport_sha256": hashlib.sha256((ROOT/"oanda_quote_transport.py").read_bytes()).hexdigest(),
    }


def initialize(connection: sqlite3.Connection, activated_utc: dt.datetime) -> dict[str, Any]:
    """Create a new empty cohort once, or verify its immutable source binding."""
    _legacy()
    connection.execute("CREATE TABLE IF NOT EXISTS official_pair_capture_v4_binding "
                       "(singleton INTEGER PRIMARY KEY CHECK(singleton=1), payload_json TEXT NOT NULL)")
    for action in ("UPDATE", "DELETE"):
        connection.execute(f"CREATE TRIGGER IF NOT EXISTS pair_v4_binding_no_{action.lower()} "
                           f"BEFORE {action} ON official_pair_capture_v4_binding BEGIN "
                           "SELECT RAISE(ABORT,'append_only:pair_v4_binding'); END")
    existing = connection.execute("SELECT payload_json FROM official_pair_capture_v4_binding").fetchone()
    binding = _binding()
    if existing:
        retained = json.loads(existing[0])
        if any(retained.get(key) != value for key, value in binding.items()):
            raise ValueError("pair_capture_v4_binding_changed_start_new_cohort")
        if parse_time(retained.get("activated_utc")) is None:
            raise ValueError("pair_capture_v4_activation_invalid")
        return retained
    # A pre-existing differently owned table must never become this cohort.
    occupied = connection.execute("SELECT name FROM sqlite_master WHERE type='table' "
                                  "AND name='official_event_pair_quote_capture'").fetchone()
    if occupied:
        raise ValueError("pair_capture_v4_refuses_existing_unbound_table")
    connection.execute("""CREATE TABLE official_event_pair_quote_capture (
        capture_id TEXT PRIMARY KEY, observation_id TEXT NOT NULL UNIQUE,
        source_id TEXT NOT NULL, source_contract_id TEXT NOT NULL,
        event_first_known_utc TEXT NOT NULL, captured_utc TEXT NOT NULL,
        detection_latency_seconds REAL NOT NULL, timing_quality TEXT NOT NULL,
        eligible_quote_count INTEGER NOT NULL, explicitly_tradeable_quote_count INTEGER NOT NULL,
        exact_all_68_available INTEGER NOT NULL CHECK(exact_all_68_available IN (0,1)),
        raw_observation_payload_sha256 TEXT NOT NULL, quote_snapshot_payload_sha256 TEXT NOT NULL,
        capture_payload_json TEXT NOT NULL,
        research_only INTEGER NOT NULL CHECK(research_only=1),
        execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
        can_authorize INTEGER NOT NULL CHECK(can_authorize=0),
        can_promote INTEGER NOT NULL CHECK(can_promote=0),
        contract_id TEXT NOT NULL, cohort_id TEXT NOT NULL, activated_utc TEXT NOT NULL)
    """)
    for action in ("UPDATE", "DELETE"):
        connection.execute(f"CREATE TRIGGER pair_v4_no_{action.lower()} BEFORE {action} "
                           "ON official_event_pair_quote_capture BEGIN "
                           "SELECT RAISE(ABORT,'append_only:pair_v4_capture'); END")
    binding["activated_utc"] = iso_utc(activated_utc)
    connection.execute("INSERT INTO official_pair_capture_v4_binding VALUES (1,?)", (canonical_json(binding),))
    connection.commit()
    return binding


def snapshot_metadata_reasons(snapshot: Mapping[str, Any], captured: dt.datetime) -> list[str]:
    reasons: list[str] = []
    if type(snapshot.get("schema_version")) is not int or snapshot.get("schema_version") != 3:
        reasons.append("quote_snapshot_schema_mismatch")
    if snapshot.get("producer") != SNAPSHOT_PRODUCER:
        reasons.append("quote_snapshot_producer_mismatch")
    if snapshot.get("research_only") is not True:
        reasons.append("quote_snapshot_research_contract_missing")
    stamp = parse_time(snapshot.get("generated_utc"))
    if stamp is None:
        reasons.append("quote_snapshot_clock_missing_or_naive")
    elif not 0 <= (captured - stamp).total_seconds() <= MAX_SNAPSHOT_AGE_SEC:
        reasons.append("quote_snapshot_stale_or_after_read")
    quotes = snapshot.get("quotes")
    quotes = quotes if isinstance(quotes, Mapping) else {}
    expected = set(_legacy().EXPECTED_INSTRUMENTS)
    if any(type(name) is not str or name not in expected for name in quotes):
        reasons.append("unexpected_or_noncanonical_instruments")
    if not quotes:
        reasons.append("empty_quote_snapshot")
    count = len(quotes)
    coverage = snapshot.get("coverage")
    coverage = coverage if isinstance(coverage, Mapping) else {}
    for name, actual, required in (
        ("quote_count", snapshot.get("quote_count"), count),
        ("current_quote_count", coverage.get("current_quote_count"), count),
        ("last_known_quote_count", coverage.get("last_known_quote_count"), count),
        ("retained_last_known_count", coverage.get("retained_last_known_count"), 0),
    ):
        if type(actual) is not int or actual != required:
            reasons.append(f"inconsistent_{name}")
    generation = snapshot.get("connection_generation")
    if (type(generation) is not int or generation <= 0
            or type(coverage.get("connection_generation")) is not int
            or coverage.get("connection_generation") != generation):
        reasons.append("connection_generation_missing_or_mismatched")
    retained = coverage.get("retained_last_known_instruments")
    if retained not in (None, []):
        reasons.append("retained_last_known_quotes_present")
    return reasons


def build_capture(observation: Mapping[str, Any], snapshot: Mapping[str, Any],
                  captured: dt.datetime, binding: Mapping[str, Any], *,
                  prospective: bool, snapshot_error: str = "") -> dict[str, Any]:
    """Reuse V1 numeric rows but apply only the explicitly new metadata rules."""
    iso_utc(captured)
    legacy = _legacy()
    # Bad shapes/count types must become a retained refusal, not abort raw news.
    try:
        base = legacy.build_capture(observation, snapshot, captured)
    except (ValueError, TypeError, OverflowError):
        base = legacy.build_capture(observation, {}, captured)
    reasons = snapshot_metadata_reasons(snapshot, captured)
    event = parse_time(observation.get("first_seen_utc"))
    activation = parse_time(binding.get("activated_utc"))
    if prospective is not True:
        reasons.append("input_not_prospective")
    if event is None or activation is None or event < activation or captured < activation:
        reasons.append("event_or_capture_before_cohort_activation")
    if event is not None and not 0 <= (captured - event).total_seconds() <= MAX_CAPTURE_LATENCY_SEC:
        reasons.append("capture_latency_outside_fifteen_seconds")
    if snapshot_error:
        reasons.append(f"quote_snapshot_read_failed:{snapshot_error}")
    eligible = {}
    invalid_counts: dict[str, int] = {}
    raw_quotes = snapshot.get("quotes")
    raw_quotes = raw_quotes if isinstance(raw_quotes, Mapping) else {}
    for pair, row in base["pair_rows"].items():
        raw = raw_quotes.get(pair)
        raw = raw if isinstance(raw, Mapping) else {}
        # V1 permits coercions/naive clocks; this cohort requires real numeric
        # prices and an aware venue clock. Tradeability remains exact True.
        if raw and (any(type(raw.get(key)) not in (int, float) for key in ("bid", "ask", "pip"))
                    or any(raw.get(key, 0) <= 0 for key in ("bid", "ask", "pip"))):
            row["invalid_reason"] = "invalid_numeric_quote"
        if raw and parse_time(raw.get("time")) is None:
            row["invalid_reason"] = "invalid_quote_time"
        row["eligible"] = bool(not reasons and not row["invalid_reason"])
        if row["invalid_reason"]:
            invalid_counts[row["invalid_reason"]] = invalid_counts.get(row["invalid_reason"], 0) + 1
        if row["eligible"]:
            eligible[pair] = {key: value for key, value in row.items()
                              if key not in ("eligible", "invalid_reason")}
    currencies = sorted({currency for pair in eligible for currency in pair.split("_")})
    graph = legacy._graph_components(sorted(eligible))
    # The retained legacy input fingerprint encodes nonfinite numeric tokens;
    # they are refused by V1's pair arithmetic and never enter output scalars.
    # Keep that exact input identity instead of converting a NaN to zero/null.
    canonical_snapshot = legacy.canonical_json(snapshot)
    base.update(
        schema_version=SCHEMA_VERSION, contract_id=CONTRACT_ID, cohort_id=COHORT_ID,
        activated_utc=binding["activated_utc"],
        capture_id=sha256(f"{observation['observation_id']}|{CONTRACT_ID}|{COHORT_ID}"),
        metadata_invalid_reasons=sorted(set(reasons)),
        eligible_quotes=eligible, eligible_quote_count=len(eligible),
        eligible_instruments=sorted(eligible), eligible_currencies=currencies,
        eligible_currency_count=len(currencies), currency_graph_components=graph,
        eligible_currency_graph_connected=len(graph) == 1,
        exact_all_68_available=len(eligible) == 68,
        invalid_reason_counts=invalid_counts,
        timing_quality=("prospective_pair_quote_snapshot_invalid" if reasons else
                        "prospective_per_pair_executable_quotes" if eligible else
                        "prospective_no_pair_quote_eligible"),
        input_prospective_observation=prospective is True,
        quote_snapshot_payload_sha256=sha256(canonical_snapshot),
        quote_snapshot_schema_version=snapshot.get("schema_version"),
        quote_snapshot_producer=snapshot.get("producer"),
        capture_clock_source="quote_snapshot_read_observed_utc",
        quote_snapshot_read_observed_utc=iso_utc(captured),
        entry_source_binding=dict(binding),
    )
    return base


def insert_capture(connection: sqlite3.Connection, capture: Mapping[str, Any]) -> None:
    keys = ("capture_id", "observation_id", "source_id", "source_contract_id",
            "event_first_known_utc", "captured_utc", "detection_latency_seconds",
            "timing_quality", "eligible_quote_count", "explicitly_tradeable_quote_count")
    connection.execute("INSERT INTO official_event_pair_quote_capture VALUES ("
                       + ",".join("?" for _ in range(21)) + ")",
                       (*[capture[key] for key in keys], int(capture["exact_all_68_available"]),
                        capture["raw_observation_payload_sha256"], capture["quote_snapshot_payload_sha256"],
                        canonical_json(capture), 1, 0, 0, 0,
                        CONTRACT_ID, COHORT_ID, capture["activated_utc"]))


def counts(connection: sqlite3.Connection) -> dict[str, int]:
    row = connection.execute("SELECT COUNT(*),COALESCE(SUM(eligible_quote_count),0),"
                             "COALESCE(SUM(eligible_quote_count>0),0) "
                             "FROM official_event_pair_quote_capture WHERE contract_id=? AND cohort_id=?",
                             (CONTRACT_ID, COHORT_ID)).fetchone()
    return dict(capture_count=int(row[0]), eligible_pair_quote_total=int(row[1]),
                captures_with_eligible_pairs=int(row[2]))
