#!/usr/bin/env python3
"""Prospective, research-only adaptive support/resistance band observer.

The worker is deliberately isolated from Practice 007.  It reads immutable or
read-only market artifacts, commits band encounters before a future M5 entry
bar can exist, and later appends executable bid/ask outcomes.  It has no OANDA
client, credentials, signal-feed, lifecycle, authorization, promotion, order,
or position-management import.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import sqlite3
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

try:
    from oanda_worker_heartbeat import WorkerHeartbeat
except ModuleNotFoundError:  # Package imports used by tests.
    from trad.oanda_worker_heartbeat import WorkerHeartbeat

try:
    from oanda_level_band_contract_v2 import (
        CONTRACT_ID as GEOMETRY_CONTRACT_ID,
        BandVersion,
        MarketBar,
        aggregate_complete_m1_to_m5,
        atr_at,
        build_pivot_band_versions,
        confirmed_pivot_anchors,
        distance_to_band,
        frozen_approach_descriptors,
        iso,
        physical_approach_side,
        pip_size,
        prior_completed_week_bands,
    )
except ModuleNotFoundError:  # Package imports used by tests.
    from trad.oanda_level_band_contract_v2 import (
        CONTRACT_ID as GEOMETRY_CONTRACT_ID,
        BandVersion,
        MarketBar,
        aggregate_complete_m1_to_m5,
        atr_at,
        build_pivot_band_versions,
        confirmed_pivot_anchors,
        distance_to_band,
        frozen_approach_descriptors,
        iso,
        physical_approach_side,
        pip_size,
        prior_completed_week_bands,
    )


ROOT = Path(__file__).resolve().parent
DATA_ROOT = ROOT / "data" / "oanda_training_manager"
STATE = DATA_ROOT / "state"
CANDLES = DATA_ROOT / "candles"
DEFAULT_UPDATE_REPORT = STATE / "all68_m1_forward_update_v1.json"
DEFAULT_QUOTES = STATE / "practice_007_market_quotes_v1.json"
DEFAULT_CLOCK = STATE / "clock_integrity_v1.json"
DEFAULT_LEDGER = STATE / "causal_level_band_prospective_v1.sqlite"
DEFAULT_RUNTIME = STATE / "causal_level_band_runtime_v1.json"
DEFAULT_OUTPUT = STATE / "causal_level_band_prospective_v1.json"
DEFAULT_HEARTBEAT = STATE / "causal_level_band_prospective_heartbeat_v1.json"
DEFAULT_PROGRESS_HEARTBEAT = (
    STATE / "causal_level_band_prospective_progress_heartbeat_v1.json"
)
SCHEMA_VERSION = "causal_level_band_prospective_v1"
COLLECTOR_CONTRACT_ID = "causal_level_band_prospective_v1_frozen_20260827a"
OBSERVATION_CLOCK_CONTRACT_ID = (
    "level_band_clock_integrity_no_cached_offset_v1_20260827"
)
HORIZONS_SEC = (900, 1800, 3600)
PRIMARY_HORIZON_SEC = 3600
QUOTE_MAX_AGE_SEC = 15.0
UPDATE_REPORT_MAX_AGE_SEC = 900.0
CONTEXT_MAX_AGE_SEC = 900.0
CLOCK_MAX_AGE_SEC = 90.0
MAX_SOURCE_DELAY_BEFORE_CENSOR_SEC = 1800.0
TAIL_M1_ROWS = 18_000
# Full history is needed while constructing prior-week and causal pivot bands,
# but the live context only needs a bounded recent tail for descriptors and
# one-hour outcome maturation.  Retaining every construction bar for all 68
# pairs needlessly holds roughly a million Python objects in memory.
RETAIN_M1_ROWS = 720
RETAIN_M5_ROWS = 600
POLICY = {
    "research_only": True,
    "execution_eligible": False,
    "can_place_orders": False,
    "can_authorize": False,
    "can_promote": False,
    "broker_access": False,
    "credentials_access": False,
    "signal_feed_write": False,
    "lifecycle_write": False,
    "selected_pair_or_side": False,
    "response_probabilities_available": False,
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_utc(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def digest(*parts: Any) -> str:
    return sha256_bytes("\x1f".join(str(part) for part in parts).encode("utf-8"))


def atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(temporary, path)


def trusted_observation_time(
    local_now: datetime,
    integrity_path: Path,
    maximum_state_age_sec: float = CLOCK_MAX_AGE_SEC,
) -> tuple[datetime, dict[str, Any]]:
    """Use only a fresh independent clock-integrity artifact; fail closed."""

    local = (
        local_now.replace(tzinfo=timezone.utc)
        if local_now.tzinfo is None else local_now.astimezone(timezone.utc)
    )
    state = read_json(integrity_path)
    generated = parse_utc(state.get("generated_utc"))
    age = max(0.0, (local - generated).total_seconds()) if generated else math.inf
    fresh = bool(
        generated
        and age <= maximum_state_age_sec
        and str(state.get("status") or "") in {"ok", "mitigated"}
    )
    trusted = bool(
        fresh
        and state.get("timestamp_normalization_trusted") is True
        and state.get("host_clock_synchronized") is True
    )
    return local, {
        "contract_id": OBSERVATION_CLOCK_CONTRACT_ID,
        "status": "aligned" if trusted else "clock_integrity_untrusted",
        "trusted_for_prospective_evidence": trusted,
        "normalized_utc": iso(local),
        "integrity_generated_utc": iso(generated) if generated else None,
        "integrity_age_sec": round(age, 3) if math.isfinite(age) else None,
        "host_clock_synchronized": state.get("host_clock_synchronized") is True,
        "timestamp_normalization_trusted": (
            state.get("timestamp_normalization_trusted") is True
        ),
        "cached_executor_offset_permitted": False,
        "applied_offset_sec": 0.0,
    }


def collection_identity() -> dict[str, str]:
    worker = Path(__file__).resolve()
    geometry = ROOT / "oanda_level_band_contract_v2.py"
    worker_sha = sha256_file(worker)
    geometry_sha = sha256_file(geometry)
    definition_sha = digest(
        COLLECTOR_CONTRACT_ID, GEOMETRY_CONTRACT_ID, worker_sha, geometry_sha,
        HORIZONS_SEC, PRIMARY_HORIZON_SEC, QUOTE_MAX_AGE_SEC,
        UPDATE_REPORT_MAX_AGE_SEC, CONTEXT_MAX_AGE_SEC,
    )
    return {
        "contract_id": COLLECTOR_CONTRACT_ID,
        "geometry_contract_id": GEOMETRY_CONTRACT_ID,
        "cohort_id": f"level_band_prospective_20260827a.{definition_sha[:16]}",
        "definition_sha256": definition_sha,
        "worker_sha256": worker_sha,
        "geometry_sha256": geometry_sha,
    }


def _strict_float(row: Mapping[str, Any], name: str) -> float:
    try:
        value = float(row[name])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"invalid_{name}") from exc
    if not math.isfinite(value) or value <= 0.0:
        raise ValueError(f"invalid_{name}")
    return value


def _tail_csv_lines(path: Path, limit: int) -> tuple[str, list[str], os.stat_result]:
    """Read a bounded complete tail while detecting concurrent append."""

    before = path.stat()
    with path.open("rb") as handle:
        header_bytes = handle.readline()
        header_end = handle.tell()
        handle.seek(0, os.SEEK_END)
        end = handle.tell()
        if end <= header_end:
            raise ValueError("empty_csv")
        handle.seek(end - 1)
        if handle.read(1) not in {b"\n", b"\r"}:
            raise ValueError("partial_csv_append")
        position = end
        chunks: list[bytes] = []
        newline_count = 0
        block = 1 << 20
        while position > header_end and newline_count <= limit + 1:
            take = min(block, position - header_end)
            position -= take
            handle.seek(position)
            chunk = handle.read(take)
            chunks.append(chunk)
            newline_count += chunk.count(b"\n")
        data = b"".join(reversed(chunks))
        if position > header_end and b"\n" in data:
            data = data.split(b"\n", 1)[1]
    after = path.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise ValueError("csv_changed_during_read")
    try:
        header = header_bytes.decode("utf-8-sig").rstrip("\r\n")
        lines = data.decode("utf-8").splitlines()[-limit:]
    except UnicodeDecodeError as exc:
        raise ValueError("csv_decode_error") from exc
    return header, lines, after


def load_m1_tail(
    path: Path,
    instrument: str,
    limit: int = TAIL_M1_ROWS,
    cutoff: datetime | None = None,
) -> tuple[list[MarketBar], dict[str, Any]]:
    """Strictly load a bounded native-midpoint BAM M1 CSV tail.

    When ``cutoff`` is supplied, rows appended by a newer in-progress all-68
    update are validated but excluded.  The published all-68 report is the
    completed-batch watermark; consuming only through that watermark prevents
    a sequential refresh from creating a mixed-generation snapshot.
    """

    header, lines, stat = _tail_csv_lines(path, limit)
    reader = csv.DictReader([header, *lines])
    required = {
        "open", "high", "low", "close", "bid_open", "bid_high", "bid_low",
        "bid_close", "ask_open", "ask_high", "ask_low", "ask_close",
    }
    if not required.issubset(set(reader.fieldnames or [])):
        raise ValueError("csv_missing_bam_columns")
    bars: list[MarketBar] = []
    row_hashes: list[str] = []
    previous: datetime | None = None
    file_last: datetime | None = None
    ignored_after_cutoff = 0
    for raw in reader:
        timestamp = parse_utc(raw.get("datetime") or raw.get("time"))
        if timestamp is None:
            raise ValueError("invalid_csv_timestamp")
        if previous is not None and timestamp <= previous:
            raise ValueError("duplicate_or_unsorted_csv_timestamp")
        previous = timestamp
        file_last = timestamp
        row_instrument = str(raw.get("instrument") or instrument).upper()
        if row_instrument != instrument.upper():
            raise ValueError("csv_instrument_mismatch")
        granularity = str(raw.get("granularity") or "M1").upper()
        if granularity != "M1":
            raise ValueError("csv_granularity_mismatch")
        bar = MarketBar(
            timestamp=timestamp,
            minutes=1,
            mid_open=_strict_float(raw, "open"),
            mid_high=_strict_float(raw, "high"),
            mid_low=_strict_float(raw, "low"),
            mid_close=_strict_float(raw, "close"),
            bid_open=_strict_float(raw, "bid_open"),
            bid_high=_strict_float(raw, "bid_high"),
            bid_low=_strict_float(raw, "bid_low"),
            bid_close=_strict_float(raw, "bid_close"),
            ask_open=_strict_float(raw, "ask_open"),
            ask_high=_strict_float(raw, "ask_high"),
            ask_low=_strict_float(raw, "ask_low"),
            ask_close=_strict_float(raw, "ask_close"),
        )
        # Native midpoint geometry must be internally valid; bid/ask economics
        # are independently validated by the pure contract during aggregation.
        if not (
            bar.mid_low <= min(bar.mid_open, bar.mid_close) <= bar.mid_high
            and bar.mid_low <= max(bar.mid_open, bar.mid_close) <= bar.mid_high
            and bar.ask_open >= bar.bid_open
            and bar.ask_close >= bar.bid_close
        ):
            raise ValueError("invalid_csv_bam_bar")
        if cutoff is not None and timestamp > cutoff:
            ignored_after_cutoff += 1
            continue
        bars.append(bar)
        row_hashes.append(digest(*[raw.get(name) for name in reader.fieldnames or []]))
    if len(bars) < 2_000:
        raise ValueError("insufficient_m1_tail")
    return bars, {
        "path": str(path),
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
        "rows": len(bars),
        "first_utc": iso(bars[0].timestamp),
        "last_utc": iso(bars[-1].timestamp),
        "file_last_utc": iso(file_last) if file_last else None,
        "report_cutoff_utc": iso(cutoff) if cutoff else None,
        "ignored_rows_after_report_cutoff": ignored_after_cutoff,
        "last_row_sha256": row_hashes[-1],
        "tail_sha256": digest(*row_hashes),
    }


def next_m5_boundary(value: datetime) -> datetime:
    epoch = value.timestamp()
    boundary = (math.floor(epoch / 300.0) + 1.0) * 300.0
    return datetime.fromtimestamp(boundary, timezone.utc)


def open_ledger(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=30.0)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=FULL")
    db.execute("PRAGMA foreign_keys=ON")
    db.execute("PRAGMA busy_timeout=30000")
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS cohort_registry (
          cohort_id TEXT PRIMARY KEY,contract_id TEXT NOT NULL,
          geometry_contract_id TEXT NOT NULL,created_at_utc TEXT NOT NULL,
          definition_sha256 TEXT NOT NULL,definition_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS band_forecasts (
          forecast_id TEXT PRIMARY KEY,cohort_id TEXT NOT NULL,
          contract_id TEXT NOT NULL,issued_at_utc TEXT NOT NULL,
          data_cutoff_utc TEXT NOT NULL,source_observed_utc TEXT NOT NULL,
          instrument TEXT NOT NULL,decision_bar_utc TEXT NOT NULL,
          planned_entry_utc TEXT NOT NULL,band_id TEXT NOT NULL,
          band_version_id TEXT NOT NULL,level_source TEXT NOT NULL,
          level_name TEXT NOT NULL,physical_role TEXT NOT NULL,
          approach_side INTEGER NOT NULL CHECK(approach_side IN (-1,1)),
          band_lower REAL NOT NULL,band_center REAL NOT NULL,band_upper REAL NOT NULL,
          decision_bid REAL NOT NULL,decision_ask REAL NOT NULL,pip REAL NOT NULL,
          spread_pips REAL NOT NULL,frozen_break_price REAL NOT NULL,
          frozen_reject_price REAL NOT NULL,horizons_json TEXT NOT NULL,
          features_json TEXT NOT NULL,dependencies_json TEXT NOT NULL,
          payload_sha256 TEXT NOT NULL,research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          can_authorize INTEGER NOT NULL CHECK(can_authorize=0),
          selected_side TEXT CHECK(selected_side IS NULL),
          UNIQUE(cohort_id,instrument,band_version_id,planned_entry_utc,approach_side),
          FOREIGN KEY(cohort_id) REFERENCES cohort_registry(cohort_id)
        );
        CREATE TABLE IF NOT EXISTS band_entries (
          forecast_id TEXT PRIMARY KEY,entry_utc TEXT NOT NULL,bid_open REAL NOT NULL,
          ask_open REAL NOT NULL,source_observed_utc TEXT NOT NULL,
          source_row_sha256 TEXT NOT NULL,payload_sha256 TEXT NOT NULL,
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          FOREIGN KEY(forecast_id) REFERENCES band_forecasts(forecast_id)
        );
        CREATE TABLE IF NOT EXISTS band_outcomes (
          outcome_id TEXT PRIMARY KEY,forecast_id TEXT NOT NULL,horizon_sec INTEGER NOT NULL,
          matured_at_utc TEXT NOT NULL,maturity_utc TEXT NOT NULL,label TEXT NOT NULL,
          exact_contiguous INTEGER NOT NULL CHECK(exact_contiguous=1),
          bounce_net_pips REAL NOT NULL,break_net_pips REAL NOT NULL,
          payload_json TEXT NOT NULL,payload_sha256 TEXT NOT NULL,
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          UNIQUE(forecast_id,horizon_sec),
          FOREIGN KEY(forecast_id) REFERENCES band_forecasts(forecast_id)
        );
        CREATE TABLE IF NOT EXISTS band_censors (
          censor_id TEXT PRIMARY KEY,forecast_id TEXT NOT NULL,horizon_sec INTEGER NOT NULL,
          observed_at_utc TEXT NOT NULL,reason TEXT NOT NULL,payload_json TEXT NOT NULL,
          payload_sha256 TEXT NOT NULL,research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          UNIQUE(forecast_id,horizon_sec),
          FOREIGN KEY(forecast_id) REFERENCES band_forecasts(forecast_id)
        );
        CREATE TABLE IF NOT EXISTS coverage_events (
          coverage_id TEXT PRIMARY KEY,cohort_id TEXT NOT NULL,cycle_id TEXT NOT NULL,
          observed_at_utc TEXT NOT NULL,instrument TEXT NOT NULL,status TEXT NOT NULL,
          reason TEXT NOT NULL,source_cutoff_utc TEXT,payload_json TEXT NOT NULL,
          payload_sha256 TEXT NOT NULL,research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          UNIQUE(cohort_id,cycle_id,instrument),
          FOREIGN KEY(cohort_id) REFERENCES cohort_registry(cohort_id)
        );
        CREATE TABLE IF NOT EXISTS integrity_events (
          event_id TEXT PRIMARY KEY,cohort_id TEXT NOT NULL,observed_at_utc TEXT NOT NULL,
          instrument TEXT,event_type TEXT NOT NULL,detail_json TEXT NOT NULL,
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          FOREIGN KEY(cohort_id) REFERENCES cohort_registry(cohort_id)
        );
        CREATE TRIGGER IF NOT EXISTS band_forecasts_no_update BEFORE UPDATE ON band_forecasts BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS band_forecasts_no_delete BEFORE DELETE ON band_forecasts BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS band_entries_no_update BEFORE UPDATE ON band_entries BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS band_entries_no_delete BEFORE DELETE ON band_entries BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS band_outcomes_no_update BEFORE UPDATE ON band_outcomes BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS band_outcomes_no_delete BEFORE DELETE ON band_outcomes BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS band_censors_no_update BEFORE UPDATE ON band_censors BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS band_censors_no_delete BEFORE DELETE ON band_censors BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS coverage_events_no_update BEFORE UPDATE ON coverage_events BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS coverage_events_no_delete BEFORE DELETE ON coverage_events BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS integrity_events_no_update BEFORE UPDATE ON integrity_events BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS integrity_events_no_delete BEFORE DELETE ON integrity_events BEGIN SELECT RAISE(ABORT,'append_only'); END;
        """
    )
    db.commit()
    return db


def register_cohort(
    db: sqlite3.Connection, identity: Mapping[str, str], observed_at: datetime
) -> None:
    definition = canonical_json({**identity, "policy": POLICY})
    existing = db.execute(
        "SELECT definition_sha256,definition_json FROM cohort_registry WHERE cohort_id=?",
        (identity["cohort_id"],),
    ).fetchone()
    if existing is not None:
        if (
            str(existing["definition_sha256"]) != identity["definition_sha256"]
            or str(existing["definition_json"]) != definition
        ):
            raise RuntimeError("cohort_identity_collision")
        return
    db.execute(
        "INSERT INTO cohort_registry VALUES (?,?,?,?,?,?)",
        (
            identity["cohort_id"], identity["contract_id"],
            identity["geometry_contract_id"], iso(observed_at),
            identity["definition_sha256"], definition,
        ),
    )
    db.commit()


@dataclass
class PairContext:
    instrument: str
    m1: list[MarketBar]
    m5: list[MarketBar]
    bands: list[BandVersion]
    cutoff_utc: datetime
    source: dict[str, Any]
    report_error: str = ""


def load_runtime(path: Path, cohort_id: str) -> dict[str, Any]:
    payload = read_json(path)
    if payload.get("cohort_id") != cohort_id:
        return {"cohort_id": cohort_id, "visits": {}, "last_report_generated_utc": ""}
    visits = payload.get("visits")
    payload["visits"] = visits if isinstance(visits, dict) else {}
    return payload


def _pair_rows(report: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        str(row.get("instrument") or "").upper(): dict(row)
        for row in report.get("pairs") or []
        if isinstance(row, Mapping) and str(row.get("instrument") or "")
    }


def build_context(
    instrument: str,
    row: Mapping[str, Any],
    observed_at: datetime,
) -> PairContext:
    error = str(row.get("error") or "")
    if error:
        raise ValueError("source_cycle_error")
    path = Path(str(row.get("path") or CANDLES / f"{instrument}_M1.csv"))
    after_last = parse_utc(row.get("after_last"))
    if after_last is None:
        raise ValueError("missing_report_cutoff")
    bars, source = load_m1_tail(path, instrument, cutoff=after_last)
    if after_last is None or bars[-1].timestamp != after_last:
        raise ValueError("report_csv_cutoff_mismatch")
    if bars[-1].known_at > observed_at + timedelta(seconds=1):
        raise ValueError("future_source_bar")
    age = max(0.0, (observed_at - bars[-1].known_at).total_seconds())
    if age > CONTEXT_MAX_AGE_SEC:
        raise ValueError("stale_m1_context")
    m5 = aggregate_complete_m1_to_m5(bars)
    if len(m5) < 300:
        raise ValueError("insufficient_m5_context")
    recent_m5_gap_count = sum(
        m5[index].timestamp - m5[index - 1].timestamp != timedelta(minutes=5)
        for index in range(max(1, len(m5) - 6), len(m5))
    )
    decision_index = len(m5) - 1
    anchors = confirmed_pivot_anchors(instrument, m5, decision_index)
    bands = build_pivot_band_versions(anchors)
    bands.extend(prior_completed_week_bands(instrument, m5, observed_at))
    if not bands:
        raise ValueError("no_causal_bands")
    cutoff_utc = bars[-1].known_at
    construction_m1_rows = len(bars)
    construction_m5_rows = len(m5)
    retained_m1 = bars[-RETAIN_M1_ROWS:]
    retained_m5 = m5[-RETAIN_M5_ROWS:]
    source.update({
        "report_after_last_utc": iso(after_last),
        "context_age_sec": round(age, 3),
        "construction_m1_rows": construction_m1_rows,
        "construction_m5_rows": construction_m5_rows,
        "retained_m1_rows": len(retained_m1),
        "retained_m5_rows": len(retained_m5),
        "band_count": len(bands),
        "recent_m5_gap_count": recent_m5_gap_count,
    })
    return PairContext(
        instrument=instrument,
        m1=retained_m1,
        m5=retained_m5,
        bands=sorted(bands, key=lambda value: (value.center, value.band_version_id)),
        cutoff_utc=cutoff_utc,
        source=source,
    )


def quote_row(
    quotes: Mapping[str, Any], instrument: str, observed_at: datetime
) -> tuple[dict[str, Any] | None, str]:
    raw = (quotes.get("quotes") or {}).get(instrument)
    if not isinstance(raw, Mapping):
        return None, "missing_quote"
    try:
        bid, ask = float(raw.get("bid")), float(raw.get("ask"))
        venue_pip = float(raw.get("pip"))
    except (TypeError, ValueError):
        return None, "invalid_quote_number"
    quote_time = parse_utc(raw.get("time"))
    if (
        not math.isfinite(bid) or not math.isfinite(ask)
        or bid <= 0.0 or ask < bid or quote_time is None
        or not math.isfinite(venue_pip) or venue_pip <= 0.0
    ):
        return None, "invalid_quote"
    age = max(0.0, (observed_at - quote_time).total_seconds())
    if quote_time > observed_at + timedelta(seconds=1):
        return None, "future_quote"
    if age > QUOTE_MAX_AGE_SEC:
        return None, "stale_quote"
    return {
        "bid": bid,
        "ask": ask,
        "mid": 0.5 * (bid + ask),
        "pip": venue_pip,
        "time": quote_time,
        "age_sec": age,
        "source": str(raw.get("source") or ""),
        "snapshot_sha256": digest(
            instrument, raw.get("time"), bid, ask, venue_pip
        ),
    }, ""


def insert_integrity(
    db: sqlite3.Connection,
    identity: Mapping[str, str],
    observed_at: datetime,
    event_type: str,
    detail: Mapping[str, Any],
    instrument: str = "",
) -> None:
    detail_json = canonical_json(detail)
    event_id = "integrity_" + digest(
        identity["cohort_id"], event_type, instrument, iso(observed_at), detail_json
    )[:28]
    db.execute(
        "INSERT OR IGNORE INTO integrity_events VALUES (?,?,?,?,?,1,0)",
        (
            event_id, identity["cohort_id"], iso(observed_at), instrument or None,
            event_type, detail_json,
        ),
    )


def insert_coverage(
    db: sqlite3.Connection,
    identity: Mapping[str, str],
    cycle_id: str,
    observed_at: datetime,
    instrument: str,
    status: str,
    reason: str,
    cutoff: datetime | None,
    detail: Mapping[str, Any],
) -> None:
    payload = canonical_json(detail)
    payload_sha = sha256_bytes(payload.encode("utf-8"))
    coverage_id = "coverage_" + digest(
        identity["cohort_id"], cycle_id, instrument
    )[:28]
    db.execute(
        "INSERT OR IGNORE INTO coverage_events VALUES (?,?,?,?,?,?,?,?,?,?,1,0)",
        (
            coverage_id, identity["cohort_id"], cycle_id, iso(observed_at),
            instrument, status, reason, iso(cutoff) if cutoff else None,
            payload, payload_sha,
        ),
    )


def _visit_key(instrument: str, band: BandVersion, side: int) -> str:
    return f"{instrument}|{band.band_id}|{side}"


def _forecast_payload(
    identity: Mapping[str, str],
    context: PairContext,
    band: BandVersion,
    side: int,
    quote: Mapping[str, Any],
    observed_at: datetime,
    planned_entry: datetime,
    descriptors: Mapping[str, Any],
    clock: Mapping[str, Any],
    report_sha: str,
    alternatives: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "cohort_id": identity["cohort_id"],
        "contract_id": identity["contract_id"],
        "geometry_contract_id": identity["geometry_contract_id"],
        "issued_at_utc": iso(observed_at),
        "data_cutoff_utc": iso(context.cutoff_utc),
        "source_observed_utc": iso(observed_at),
        "instrument": context.instrument,
        "decision_bar_utc": iso(context.m5[-1].timestamp),
        "planned_entry_utc": iso(planned_entry),
        "band_id": band.band_id,
        "band_version_id": band.band_version_id,
        "level_source": band.source,
        "level_name": band.name,
        "physical_role": str(descriptors["physical_role"]),
        "approach_side": side,
        "band_lower": band.lower,
        "band_center": band.center,
        "band_upper": band.upper,
        "decision_bid": float(quote["bid"]),
        "decision_ask": float(quote["ask"]),
        "pip": float(descriptors["pip"]),
        "spread_pips": float(descriptors["spread_pips"]),
        "frozen_break_price": float(descriptors["frozen_break_price"]),
        "frozen_reject_price": float(descriptors["frozen_reject_price"]),
        "horizons_sec": list(HORIZONS_SEC),
        "features": dict(descriptors),
        "dependencies": {
            "report_sha256": report_sha,
            "quote_snapshot_sha256": quote["snapshot_sha256"],
            "quote_time_utc": iso(quote["time"]),
            "quote_source": quote["source"],
            "m1_tail_sha256": context.source["tail_sha256"],
            "m1_last_row_sha256": context.source["last_row_sha256"],
            "clock": dict(clock),
            "overlapping_band_alternatives": list(alternatives),
        },
        "research_only": True,
        "execution_eligible": False,
        "can_authorize": False,
        "selected_side": None,
    }


def insert_forecast(
    db: sqlite3.Connection,
    payload: Mapping[str, Any],
) -> tuple[str, bool]:
    payload_json = canonical_json(payload)
    payload_sha = sha256_bytes(payload_json.encode("utf-8"))
    forecast_id = "bandfc_" + digest(
        payload["cohort_id"], payload["instrument"], payload["band_version_id"],
        payload["planned_entry_utc"], payload["approach_side"],
        payload["dependencies"]["quote_time_utc"],
    )[:28]
    existing = db.execute(
        "SELECT payload_sha256 FROM band_forecasts WHERE forecast_id=?",
        (forecast_id,),
    ).fetchone()
    if existing is not None:
        if str(existing["payload_sha256"]) != payload_sha:
            raise RuntimeError("forecast_idempotency_collision")
        return forecast_id, False
    same_visit = db.execute(
        """SELECT forecast_id FROM band_forecasts WHERE cohort_id=? AND
        instrument=? AND band_version_id=? AND planned_entry_utc=? AND approach_side=?""",
        (
            payload["cohort_id"], payload["instrument"], payload["band_version_id"],
            payload["planned_entry_utc"], payload["approach_side"],
        ),
    ).fetchone()
    if same_visit is not None:
        # A crash after the immutable insert but before the mutable rearm file
        # is allowed to rediscover the physical visit, never to duplicate it.
        return str(same_visit["forecast_id"]), False
    db.execute(
        """INSERT INTO band_forecasts VALUES (
        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NULL)""",
        (
            forecast_id, payload["cohort_id"], payload["contract_id"],
            payload["issued_at_utc"], payload["data_cutoff_utc"],
            payload["source_observed_utc"], payload["instrument"],
            payload["decision_bar_utc"], payload["planned_entry_utc"],
            payload["band_id"], payload["band_version_id"], payload["level_source"],
            payload["level_name"], payload["physical_role"], payload["approach_side"],
            payload["band_lower"], payload["band_center"], payload["band_upper"],
            payload["decision_bid"], payload["decision_ask"], payload["pip"],
            payload["spread_pips"], payload["frozen_break_price"],
            payload["frozen_reject_price"], canonical_json(payload["horizons_sec"]),
            canonical_json(payload["features"]), canonical_json(payload["dependencies"]),
            payload_sha, 1, 0, 0,
        ),
    )
    return forecast_id, True


def append_entry(
    db: sqlite3.Connection,
    forecast: sqlite3.Row,
    m5: Sequence[MarketBar],
    observed_at: datetime,
) -> bool:
    existing = db.execute(
        "SELECT 1 FROM band_entries WHERE forecast_id=?", (forecast["forecast_id"],)
    ).fetchone()
    if existing is not None:
        return False
    planned = parse_utc(forecast["planned_entry_utc"])
    issued = parse_utc(forecast["issued_at_utc"])
    if planned is None or issued is None or planned <= issued:
        raise RuntimeError("nonprospective_planned_entry")
    bar = next((row for row in m5 if row.timestamp == planned), None)
    if bar is None or bar.known_at > observed_at:
        return False
    payload = {
        "forecast_id": forecast["forecast_id"],
        "entry_utc": iso(planned),
        "bid_open": bar.bid_open,
        "ask_open": bar.ask_open,
        "source_observed_utc": iso(observed_at),
        "source_row_sha256": digest(asdict(bar)),
        "research_only": True,
        "execution_eligible": False,
    }
    payload_json = canonical_json(payload)
    db.execute(
        "INSERT INTO band_entries VALUES (?,?,?,?,?,?,?,1,0)",
        (
            forecast["forecast_id"], payload["entry_utc"], payload["bid_open"],
            payload["ask_open"], payload["source_observed_utc"],
            payload["source_row_sha256"], sha256_bytes(payload_json.encode("utf-8")),
        ),
    )
    return True


def _path_metrics(
    bars: Sequence[MarketBar], entry_bid: float, entry_ask: float,
    pip: float, long_side: bool,
) -> dict[str, Any]:
    if long_side:
        terminal = (bars[-1].bid_close - entry_ask) / pip
        mfe = max((row.bid_high - entry_ask) / pip for row in bars)
        mae = min((row.bid_low - entry_ask) / pip for row in bars)
        clear = next(
            (index for index, row in enumerate(bars) if row.bid_high > entry_ask), None
        )
    else:
        terminal = (entry_bid - bars[-1].ask_close) / pip
        mfe = max((entry_bid - row.ask_low) / pip for row in bars)
        mae = min((entry_bid - row.ask_high) / pip for row in bars)
        clear = next(
            (index for index, row in enumerate(bars) if row.ask_low < entry_bid), None
        )
    return {
        "terminal_net_pips": terminal,
        "mfe_net_pips": mfe,
        "mae_net_pips": mae,
        "time_to_cost_clear_interval_min": (
            None if clear is None else [clear * 5, (clear + 1) * 5]
        ),
    }


def mature_payload(
    forecast: sqlite3.Row,
    entry: sqlite3.Row,
    path: Sequence[MarketBar],
    horizon_sec: int,
    observed_at: datetime,
) -> dict[str, Any]:
    side = int(forecast["approach_side"])
    lower, upper = float(forecast["band_lower"]), float(forecast["band_upper"])
    break_price = float(forecast["frozen_break_price"])
    reject_price = float(forecast["frozen_reject_price"])
    contact_seen = False
    break_seen = False
    reject_seen = False
    first_contact_offset: int | None = None
    first_break_offset: int | None = None
    first_reject_offset: int | None = None
    ambiguous_offset: int | None = None
    for index, bar in enumerate(path):
        contact = bar.mid_high >= lower if side == 1 else bar.mid_low <= upper
        if contact and not contact_seen:
            contact_seen, first_contact_offset = True, index
        if not contact_seen:
            continue
        broke = bar.mid_high >= break_price if side == 1 else bar.mid_low <= break_price
        rejected = bar.mid_low <= reject_price if side == 1 else bar.mid_high >= reject_price
        if broke and rejected:
            ambiguous_offset = index
            break
        if broke and not break_seen:
            break_seen, first_break_offset = True, index
        if rejected and not reject_seen:
            reject_seen, first_reject_offset = True, index
        if break_seen or reject_seen:
            break
    if ambiguous_offset is not None:
        label = "ambiguous_intrabar"
    elif not contact_seen:
        label = "no_contact"
    elif reject_seen and not break_seen:
        label = "contact_reject"
    elif break_seen:
        later = path[int(first_break_offset or 0):]
        reclaimed = any(
            row.mid_close < upper if side == 1 else row.mid_close > lower
            for row in later
        )
        label = "false_break_reclaim" if reclaimed else "penetrate_hold"
    else:
        label = "contact_no_resolution"
    pip = float(forecast["pip"])
    long_path = _path_metrics(
        path, float(entry["bid_open"]), float(entry["ask_open"]), pip, True
    )
    short_path = _path_metrics(
        path, float(entry["bid_open"]), float(entry["ask_open"]), pip, False
    )
    break_arm = long_path if side == 1 else short_path
    bounce_arm = short_path if side == 1 else long_path
    maturity = path[-1].known_at
    return {
        "forecast_id": forecast["forecast_id"],
        "horizon_sec": horizon_sec,
        "matured_at_utc": iso(observed_at),
        "maturity_utc": iso(maturity),
        "label": label,
        "contact_required_for_rejection": True,
        "first_contact_bar_offset": first_contact_offset,
        "first_break_bar_offset": first_break_offset,
        "first_reject_bar_offset": first_reject_offset,
        "ambiguous_bar_offset": ambiguous_offset,
        "approach_side": side,
        "bounce_arm": bounce_arm,
        "break_arm": break_arm,
        "long_path": long_path,
        "short_path": short_path,
        "entry_spread_pips": (
            float(entry["ask_open"]) - float(entry["bid_open"])
        ) / pip,
        "exit_spread_pips": (path[-1].ask_close - path[-1].bid_close) / pip,
        "path_sha256": digest(*[asdict(row) for row in path]),
        "exact_contiguous": True,
        "selected_side": None,
        "research_only": True,
        "execution_eligible": False,
    }


def append_outcome(
    db: sqlite3.Connection,
    forecast: sqlite3.Row,
    entry: sqlite3.Row,
    m5: Sequence[MarketBar],
    horizon_sec: int,
    observed_at: datetime,
) -> tuple[bool, str]:
    if db.execute(
        "SELECT 1 FROM band_outcomes WHERE forecast_id=? AND horizon_sec=?",
        (forecast["forecast_id"], horizon_sec),
    ).fetchone() is not None:
        return False, "already_matured"
    if db.execute(
        "SELECT 1 FROM band_censors WHERE forecast_id=? AND horizon_sec=?",
        (forecast["forecast_id"], horizon_sec),
    ).fetchone() is not None:
        return False, "already_censored"
    planned = parse_utc(forecast["planned_entry_utc"])
    if planned is None:
        return False, "invalid_planned_entry"
    maturity = planned + timedelta(seconds=horizon_sec)
    if observed_at < maturity:
        return False, "not_due"
    steps = horizon_sec // 300
    expected = [planned + timedelta(minutes=5 * offset) for offset in range(steps)]
    index = {row.timestamp: row for row in m5}
    if not all(timestamp in index for timestamp in expected):
        return False, "missing_exact_path"
    path = [index[timestamp] for timestamp in expected]
    if path[-1].known_at != maturity or path[-1].known_at > observed_at:
        return False, "path_not_observed"
    payload = mature_payload(forecast, entry, path, horizon_sec, observed_at)
    payload_json = canonical_json(payload)
    payload_sha = sha256_bytes(payload_json.encode("utf-8"))
    outcome_id = "bandout_" + digest(
        forecast["forecast_id"], horizon_sec, forecast["cohort_id"]
    )[:28]
    db.execute(
        """INSERT INTO band_outcomes (
        outcome_id,forecast_id,horizon_sec,matured_at_utc,maturity_utc,label,
        exact_contiguous,bounce_net_pips,break_net_pips,payload_json,
        payload_sha256,research_only,execution_eligible
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,1,0)""",
        (
            outcome_id, forecast["forecast_id"], horizon_sec,
            payload["matured_at_utc"], payload["maturity_utc"], payload["label"],
            1, payload["bounce_arm"]["terminal_net_pips"],
            payload["break_arm"]["terminal_net_pips"], payload_json, payload_sha,
        ),
    )
    return True, "matured"


def append_censor(
    db: sqlite3.Connection,
    forecast: sqlite3.Row,
    horizon_sec: int,
    observed_at: datetime,
    reason: str,
) -> bool:
    if db.execute(
        "SELECT 1 FROM band_censors WHERE forecast_id=? AND horizon_sec=?",
        (forecast["forecast_id"], horizon_sec),
    ).fetchone() is not None:
        return False
    payload = {
        "forecast_id": forecast["forecast_id"],
        "horizon_sec": horizon_sec,
        "observed_at_utc": iso(observed_at),
        "reason": reason,
        "late_recovery_can_create_proof": False,
        "research_only": True,
        "execution_eligible": False,
    }
    payload_json = canonical_json(payload)
    payload_sha = sha256_bytes(payload_json.encode("utf-8"))
    censor_id = "bandcensor_" + digest(
        forecast["forecast_id"], horizon_sec, reason
    )[:28]
    db.execute(
        "INSERT INTO band_censors VALUES (?,?,?,?,?,?,?,1,0)",
        (
            censor_id, forecast["forecast_id"], horizon_sec, iso(observed_at),
            reason, payload_json, payload_sha,
        ),
    )
    return True


def ledger_summary(
    db: sqlite3.Connection, cohort_id: str
) -> dict[str, Any]:
    forecasts = int(db.execute(
        "SELECT COUNT(*) FROM band_forecasts WHERE cohort_id=?", (cohort_id,)
    ).fetchone()[0])
    entries = int(db.execute(
        "SELECT COUNT(*) FROM band_entries e JOIN band_forecasts f USING(forecast_id) WHERE f.cohort_id=?",
        (cohort_id,),
    ).fetchone()[0])
    outcomes = int(db.execute(
        "SELECT COUNT(*) FROM band_outcomes o JOIN band_forecasts f USING(forecast_id) WHERE f.cohort_id=?",
        (cohort_id,),
    ).fetchone()[0])
    censors = int(db.execute(
        "SELECT COUNT(*) FROM band_censors c JOIN band_forecasts f USING(forecast_id) WHERE f.cohort_id=?",
        (cohort_id,),
    ).fetchone()[0])
    cells = []
    for row in db.execute(
        """SELECT o.horizon_sec,COUNT(*) n,
        AVG(o.bounce_net_pips) bounce_avg_net_pips,
        AVG(o.break_net_pips) break_avg_net_pips,
        AVG(o.bounce_net_pips>0) bounce_after_cost_win_rate,
        AVG(o.break_net_pips>0) break_after_cost_win_rate
        FROM band_outcomes o JOIN band_forecasts f USING(forecast_id)
        WHERE f.cohort_id=? GROUP BY o.horizon_sec ORDER BY o.horizon_sec""",
        (cohort_id,),
    ):
        cells.append(dict(row))
    labels = {
        str(row["label"]): int(row["n"])
        for row in db.execute(
            """SELECT o.label,COUNT(*) n FROM band_outcomes o
            JOIN band_forecasts f USING(forecast_id) WHERE f.cohort_id=?
            GROUP BY o.label ORDER BY o.label""",
            (cohort_id,),
        )
    }
    return {
        "forecasts": forecasts,
        "entries": entries,
        "outcomes": outcomes,
        "censors": censors,
        "pending_entries": max(0, forecasts - entries - censors),
        "horizon_cells": cells,
        "response_labels": labels,
    }


def compatible_forecasts_for_maturation(
    db: sqlite3.Connection,
    identity: Mapping[str, str],
) -> list[sqlite3.Row]:
    """Return frozen forecasts whose outcome contract remains compatible.

    Operational fixes intentionally roll the collecting cohort because the
    worker source hash changes.  Already-issued forecasts must nevertheless
    mature under their frozen bands, barriers, entry time, and horizons.  The
    collector may therefore finish forecasts from earlier source-hash cohorts
    only when both the collector contract and geometry contract are identical.
    Results remain attributed to each original cohort.
    """

    return db.execute(
        """SELECT f.* FROM band_forecasts f
        JOIN cohort_registry c USING(cohort_id)
        WHERE f.contract_id=? AND c.geometry_contract_id=?
        ORDER BY f.issued_at_utc,f.forecast_id""",
        (identity["contract_id"], identity["geometry_contract_id"]),
    ).fetchall()


class ProspectiveLevelBandWorker:
    def __init__(
        self,
        *,
        update_report: Path = DEFAULT_UPDATE_REPORT,
        quotes: Path = DEFAULT_QUOTES,
        clock: Path = DEFAULT_CLOCK,
        ledger: Path = DEFAULT_LEDGER,
        runtime: Path = DEFAULT_RUNTIME,
        output: Path = DEFAULT_OUTPUT,
        heartbeat: Path = DEFAULT_HEARTBEAT,
    ) -> None:
        self.update_report_path = update_report
        self.quotes_path = quotes
        self.clock_path = clock
        self.ledger_path = ledger
        self.runtime_path = runtime
        self.output_path = output
        self.heartbeat_path = heartbeat
        self.identity = collection_identity()
        self.db = open_ledger(ledger)
        now = utc_now()
        register_cohort(self.db, self.identity, now)
        self.runtime = load_runtime(runtime, self.identity["cohort_id"])
        self.contexts: dict[str, PairContext] = {}
        self.last_coverage: dict[str, dict[str, Any]] = {}

    def close(self) -> None:
        self.db.commit()
        self.db.close()

    def refresh_contexts(
        self, report: Mapping[str, Any], observed_at: datetime
    ) -> tuple[str, str]:
        generated = parse_utc(report.get("generated_utc"))
        if generated is None:
            return "", "missing_update_report_clock"
        age = max(0.0, (observed_at - generated).total_seconds())
        if age > UPDATE_REPORT_MAX_AGE_SEC:
            return iso(generated), "stale_update_report"
        generated_iso = iso(generated)
        if generated_iso == self.runtime.get("last_report_generated_utc") and self.contexts:
            return generated_iso, ""
        cycle_id = "levelctx_" + digest(
            self.identity["cohort_id"], generated_iso
        )[:24]
        rows = _pair_rows(report)
        coverage: dict[str, dict[str, Any]] = {}
        for instrument, row in sorted(rows.items()):
            status, reason = "ready", ""
            cutoff: datetime | None = None
            detail: dict[str, Any] = {
                "report_error": str(row.get("error") or ""),
                "report_after_last": row.get("after_last"),
            }
            try:
                context = build_context(instrument, row, observed_at)
                self.contexts[instrument] = context
                cutoff = context.cutoff_utc
                detail.update(context.source)
            except (OSError, ValueError) as exc:
                status, reason = "blocked", str(exc)
                context = self.contexts.get(instrument)
                if context is not None:
                    cutoff = context.cutoff_utc
                    detail["previous_context_retained_for_display_only"] = True
            coverage[instrument] = {
                "instrument": instrument,
                "status": status,
                "reason": reason,
                "source_cutoff_utc": iso(cutoff) if cutoff else None,
            }
            insert_coverage(
                self.db, self.identity, cycle_id, observed_at, instrument,
                status, reason, cutoff, detail,
            )
        self.last_coverage = coverage
        self.runtime["last_report_generated_utc"] = generated_iso
        self.db.commit()
        return generated_iso, ""

    def _record_forecasts_and_display(
        self,
        quotes_payload: Mapping[str, Any],
        observed_at: datetime,
        clock_state: Mapping[str, Any],
        report_sha: str,
        globally_allowed: bool,
    ) -> tuple[list[dict[str, Any]], int, dict[str, int]]:
        display: list[dict[str, Any]] = []
        inserted = 0
        reasons: dict[str, int] = {}
        visits = self.runtime.setdefault("visits", {})
        for instrument in sorted(self.last_coverage):
            coverage = self.last_coverage[instrument]
            context = self.contexts.get(instrument)
            if coverage["status"] != "ready" or context is None:
                reasons[coverage.get("reason") or "context_blocked"] = (
                    reasons.get(coverage.get("reason") or "context_blocked", 0) + 1
                )
                continue
            quote, quote_reason = quote_row(quotes_payload, instrument, observed_at)
            if quote is None:
                reasons[quote_reason] = reasons.get(quote_reason, 0) + 1
                continue
            previous_mid = context.m1[-1].mid_close
            candidates: list[tuple[float, int, str, BandVersion, int]] = []
            for band in context.bands:
                side = physical_approach_side(quote["mid"], previous_mid, band)
                if side is None:
                    continue
                candidates.append((
                    distance_to_band(quote["mid"], band), -band.anchor_count,
                    band.band_version_id, band, side,
                ))
            if not candidates:
                reasons["inside_band_side_unknown"] = reasons.get(
                    "inside_band_side_unknown", 0
                ) + 1
                continue
            # One deterministic representative prevents overlapping bands from
            # multiplying a single physical encounter into fake consensus.
            selected = min(candidates)
            _, _, _, band, side = selected
            alternatives = [
                {
                    "band_version_id": row[3].band_version_id,
                    "distance_pips": row[0] / quote["pip"],
                    "approach_side": row[4],
                }
                for row in sorted(candidates)[1:4]
            ]
            try:
                descriptors = frozen_approach_descriptors(
                    instrument, context.m1, context.m5, band, side,
                    quote["mid"], quote["bid"], quote["ask"], observed_at,
                    venue_pip=quote["pip"],
                )
            except ValueError as exc:
                reasons[str(exc)] = reasons.get(str(exc), 0) + 1
                continue
            distance_pips = float(descriptors["distance_to_band_pips"])
            zone_pips = float(descriptors["approach_zone_pips"])
            rearm_pips = float(descriptors["rearm_distance_pips"])
            v3 = float(descriptors["approach_velocity_3_pips_per_min"])
            v1 = float(descriptors["approach_velocity_1_pips_per_min"])
            key = _visit_key(instrument, band, side)
            visit = visits.get(key) if isinstance(visits.get(key), dict) else {}
            armed = bool(visit.get("armed", True))
            if not armed and distance_pips >= rearm_pips:
                armed = True
                visit = {"armed": True, "rearmed_at_utc": iso(observed_at)}
            near = 0.0 < distance_pips <= zone_pips
            approaching = v1 > 0.0 and v3 > 0.0
            trigger = bool(globally_allowed and armed and near and approaching)
            planned_entry = next_m5_boundary(observed_at)
            forecast_id = ""
            if trigger:
                payload = _forecast_payload(
                    self.identity, context, band, side, quote, observed_at,
                    planned_entry, descriptors, clock_state, report_sha, alternatives,
                )
                try:
                    forecast_id, was_inserted = insert_forecast(self.db, payload)
                except RuntimeError as exc:
                    insert_integrity(
                        self.db, self.identity, observed_at, str(exc),
                        {"band_version_id": band.band_version_id}, instrument,
                    )
                    raise
                if was_inserted:
                    inserted += 1
                    armed = False
                    visit = {
                        "armed": False,
                        "visit_started_utc": iso(observed_at),
                        "forecast_id": forecast_id,
                        "band_version_id": band.band_version_id,
                    }
            visit.update({
                "armed": armed,
                "last_seen_utc": iso(observed_at),
                "last_distance_pips": distance_pips,
                "last_band_version_id": band.band_version_id,
            })
            visits[key] = visit
            state = "inside_band" if distance_pips == 0.0 else (
                "approaching" if near and approaching else
                "near_moving_away" if near else "far"
            )
            display.append({
                "instrument": instrument,
                "band_id": band.band_id,
                "band_version_id": band.band_version_id,
                "source": band.source,
                "name": band.name,
                "origin_kind": band.origin_kind,
                "physical_role": descriptors["physical_role"],
                "approach_side": side,
                "band_lower": band.lower,
                "band_center": band.center,
                "band_upper": band.upper,
                "anchor_count": band.anchor_count,
                "distance_pips": distance_pips,
                "distance_atr": descriptors["distance_to_band_atr"],
                "approach_zone_pips": zone_pips,
                "velocity_1_pips_per_min": descriptors["approach_velocity_1_pips_per_min"],
                "velocity_3_pips_per_min": v3,
                "acceleration_pips_per_min2": descriptors["approach_acceleration_pips_per_min2"],
                "time_to_contact_min": descriptors["estimated_time_to_contact_min"],
                "path_efficiency_5": descriptors["path_efficiency_5"],
                "monotonicity_5": descriptors["approach_monotonicity_5"],
                "impulse_toward_band_atr": descriptors["impulse_toward_band_atr"],
                "equilibrium_stretch_atr": descriptors["equilibrium_stretch_atr"],
                "atr_m5_pips": descriptors["atr_m5_pips"],
                "spread_pips": descriptors["spread_pips"],
                "quote_age_sec": quote["age_sec"],
                "quote_time_utc": iso(quote["time"]),
                "context_cutoff_utc": iso(context.cutoff_utc),
                "state": state,
                "armed": armed,
                "forecast_issued_this_cycle": bool(trigger and forecast_id),
                "forecast_id": forecast_id or None,
                "bounce_hypothesis": "short" if side == 1 else "long",
                "break_hypothesis": "long" if side == 1 else "short",
                "response_probability_state": "absent_no_frozen_model",
                "empirical_cost_clearance_state": "unknown_collecting_prospective_outcomes",
                "research_only": True,
                "execution_eligible": False,
                "can_authorize": False,
            })
        self.db.commit()
        display.sort(key=lambda row: (
            row["distance_atr"], -row["velocity_3_pips_per_min"],
            -row["anchor_count"], row["instrument"],
        ))
        return display, inserted, reasons

    def _mature(self, observed_at: datetime) -> dict[str, int]:
        counts = {"entries": 0, "outcomes": 0, "censors": 0}
        forecasts = compatible_forecasts_for_maturation(self.db, self.identity)
        for forecast in forecasts:
            context = self.contexts.get(str(forecast["instrument"]))
            if context is None:
                continue
            if append_entry(self.db, forecast, context.m5, observed_at):
                counts["entries"] += 1
            entry = self.db.execute(
                "SELECT * FROM band_entries WHERE forecast_id=?",
                (forecast["forecast_id"],),
            ).fetchone()
            planned = parse_utc(forecast["planned_entry_utc"])
            for horizon in HORIZONS_SEC:
                if entry is not None:
                    inserted, reason = append_outcome(
                        self.db, forecast, entry, context.m5, horizon, observed_at
                    )
                    if inserted:
                        counts["outcomes"] += 1
                        continue
                else:
                    reason = "missing_exact_entry_bar"
                if (
                    planned is not None
                    and observed_at >= planned + timedelta(
                        seconds=horizon + MAX_SOURCE_DELAY_BEFORE_CENSOR_SEC
                    )
                    and reason in {"missing_exact_entry_bar", "missing_exact_path"}
                    and append_censor(self.db, forecast, horizon, observed_at, reason)
                ):
                    counts["censors"] += 1
        self.db.commit()
        return counts

    def run_once(
        self,
        now: datetime | None = None,
        heartbeat: WorkerHeartbeat | None = None,
    ) -> dict[str, Any]:
        started = time.monotonic()
        local_now = now or utc_now()
        if heartbeat is not None:
            heartbeat.mark_progress(phase="loading_inputs")
        observed_at, clock_state = trusted_observation_time(
            local_now, self.clock_path
        )
        report = read_json(self.update_report_path)
        quotes = read_json(self.quotes_path)
        if heartbeat is not None:
            heartbeat.mark_progress(phase="refreshing_contexts")
        report_generated, report_reason = self.refresh_contexts(report, observed_at)
        report_sha = (
            sha256_file(self.update_report_path)
            if self.update_report_path.is_file() else ""
        )
        globally_allowed = bool(
            clock_state["trusted_for_prospective_evidence"]
            and not report_reason
            and str(report.get("source", {}).get("environment") or "") == "practice"
            and str(report.get("source", {}).get("price") or "") == "BAM"
        )
        if heartbeat is not None:
            heartbeat.mark_progress(phase="recording_forecasts")
        display, inserted, rejection_counts = self._record_forecasts_and_display(
            quotes, observed_at, clock_state, report_sha, globally_allowed
        )
        if heartbeat is not None:
            heartbeat.mark_progress(phase="maturing_outcomes")
        matured = self._mature(observed_at)
        if heartbeat is not None:
            heartbeat.mark_progress(phase="summarizing_ledger")
        ledger = ledger_summary(self.db, self.identity["cohort_id"])
        coverage_rows = list(self.last_coverage.values())
        ready_count = sum(row["status"] == "ready" for row in coverage_rows)
        status = "running" if globally_allowed else "blocked"
        payload = {
            "schema_version": SCHEMA_VERSION,
            "contract_id": self.identity["contract_id"],
            "geometry_contract_id": self.identity["geometry_contract_id"],
            "cohort_id": self.identity["cohort_id"],
            "generated_utc": iso(observed_at),
            "status": status,
            "block_reason": (
                "" if globally_allowed else
                report_reason or str(clock_state.get("status") or "blocked")
            ),
            "clock": clock_state,
            "update_report_generated_utc": report_generated,
            "update_report_sha256": report_sha,
            "instrument_count": len(coverage_rows),
            "ready_context_count": ready_count,
            "blocked_context_count": len(coverage_rows) - ready_count,
            "current_valid_quote_band_count": len(display),
            "forecast_inserted_this_cycle": inserted,
            "maturation_this_cycle": matured,
            "coverage": coverage_rows,
            "coverage_reasons": rejection_counts,
            "top_approaching_bands": display[:20],
            "ledger": ledger,
            "policy": POLICY,
            "methodology": {
                "bands": "causal_incremental_m5_pivot_clusters_plus_prior_completed_week_pivots",
                "trigger": "outside_to_zone_distance_decreasing_and_v3_positive_once_per_visit",
                "entry": "future_m5_boundary_committed_before_bar_exists",
                "labels": [
                    "no_contact", "contact_reject", "penetrate_hold",
                    "false_break_reclaim", "ambiguous_intrabar",
                    "contact_no_resolution",
                ],
                "contact_required_before_rejection": True,
                "barriers_frozen_before_entry": True,
                "response_probabilities": "absent_until_frozen_purged_model_exists",
                "counterfactual_arms": ["always_bounce", "always_break"],
                "selection_or_order_effect": "none",
            },
            "database": str(self.ledger_path),
            "runtime_seconds": round(time.monotonic() - started, 3),
        }
        if heartbeat is not None:
            heartbeat.mark_progress(phase="publishing_cycle")
        atomic_json(self.output_path, payload)
        self.runtime.update({
            "cohort_id": self.identity["cohort_id"],
            "updated_utc": iso(observed_at),
        })
        atomic_json(self.runtime_path, self.runtime)
        atomic_json(self.heartbeat_path, {
            "schema_version": 1,
            "worker": "causal_level_band_prospective",
            "status": status,
            "generated_utc": iso(observed_at),
            "cohort_id": self.identity["cohort_id"],
            "ready_context_count": ready_count,
            "forecast_count": ledger["forecasts"],
            "outcome_count": ledger["outcomes"],
            "research_only": True,
            "execution_eligible": False,
            "can_place_orders": False,
        })
        return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--update-report", type=Path, default=DEFAULT_UPDATE_REPORT)
    parser.add_argument("--quotes", type=Path, default=DEFAULT_QUOTES)
    parser.add_argument("--clock", type=Path, default=DEFAULT_CLOCK)
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER)
    parser.add_argument("--runtime", type=Path, default=DEFAULT_RUNTIME)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--heartbeat", type=Path, default=DEFAULT_HEARTBEAT)
    parser.add_argument(
        "--progress-heartbeat",
        type=Path,
        default=DEFAULT_PROGRESS_HEARTBEAT,
    )
    parser.add_argument("--interval-sec", type=float, default=30.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    worker = ProspectiveLevelBandWorker(
        update_report=args.update_report,
        quotes=args.quotes,
        clock=args.clock,
        ledger=args.ledger,
        runtime=args.runtime,
        output=args.output,
        heartbeat=args.heartbeat,
    )
    started = time.monotonic()
    cycles_completed = 0
    try:
        with WorkerHeartbeat(
            args.progress_heartbeat,
            worker="causal_level_band_prospective",
            role="research_only_progress",
            interval_sec=5.0,
        ) as heartbeat:
            while True:
                heartbeat.mark_progress(
                    phase="starting_cycle",
                    cycles_completed=cycles_completed,
                )
                payload = worker.run_once(heartbeat=heartbeat)
                cycles_completed += 1
                heartbeat.mark_progress(
                    phase="cycle_complete",
                    cycles_completed=cycles_completed,
                    runtime_seconds=payload.get("runtime_seconds"),
                )
                if args.once:
                    print(json.dumps(payload, indent=2, sort_keys=True))
                    break
                if (
                    args.duration_sec > 0
                    and time.monotonic() - started >= args.duration_sec
                ):
                    break
                heartbeat.update(
                    phase="sleeping",
                    sleep_seconds=max(5.0, args.interval_sec),
                )
                time.sleep(max(5.0, args.interval_sec))
    finally:
        worker.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
