#!/usr/bin/env python3
"""Join every retained major FX move to causal forecast and source coverage.

This is a research-only gap census.  It deliberately keeps three evidence
classes separate:

* legacy significant-move labels identify historical coverage requirements;
* canonical movement/news episodes contain executable hindsight labels;
* post-catalog prospective ledgers contain genuinely timestamped forecasts.

An unavailable historical forecast is an evidence gap, never a model failure.
No result produced here can promote, authorize, or place an order.
"""

from __future__ import annotations

import argparse
import bisect
import csv
import datetime as dt
import hashlib
import json
import math
import os
import sqlite3
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import oanda_market_sentiment_ticker as market_ticker
from oanda_instrument_pips import fallback_pip_size


ROOT = Path(__file__).resolve().parent
CONFIG = ROOT / "config" / "major_move_gap_census_v1.json"
STATE = ROOT / "data" / "oanda_training_manager" / "state"
REPORT_ROOT = ROOT / "data" / "oanda_training_manager" / "reports" / "major_move_gap_census"
MOVEMENT_DB = STATE / "movement_news_episode_research_v1.sqlite"
MOVEMENT_SUMMARY = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "movement_news_episode_research"
    / "MOVEMENT_NEWS_EPISODE_RESEARCH_20260809.json"
)
LEGACY_TAGS = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "news_event_tags"
    / "significant_move_news_tags.csv"
)
LEGACY_CANDLE_ROOT = ROOT / "data" / "oanda_training_manager" / "candles"
TOP_SIGNAL_DB = STATE / "top_signal_position_ledger_v1.sqlite"
H1_FORECAST_DB = STATE / "one_hour_shadow_forecasts_v1.sqlite"
PROSPECTIVE_DB = STATE / "executable_opportunity_prospective_v1.sqlite"
SOURCE_DB = STATE / "source_governance_v1.sqlite"
OUTPUT_JSON = REPORT_ROOT / "MAJOR_MOVE_GAP_CENSUS_CURRENT.json"
OUTPUT_MD = REPORT_ROOT / "MAJOR_MOVE_GAP_CENSUS_CURRENT.md"
OUTPUT_CSV = REPORT_ROOT / "MAJOR_MOVE_GAP_CENSUS_DETAIL_CURRENT.csv"
PRIOR_OUTPUT_JSON = REPORT_ROOT / "MAJOR_MOVE_GAP_CENSUS_PRE_CAUSAL_FACTOR_V1_20260827.json"
PRIOR_OUTPUT_MD = REPORT_ROOT / "MAJOR_MOVE_GAP_CENSUS_PRE_CAUSAL_FACTOR_V1_20260827.md"
PRIOR_OUTPUT_CSV = REPORT_ROOT / "MAJOR_MOVE_GAP_CENSUS_DETAIL_PRE_CAUSAL_FACTOR_V1_20260827.csv"
MAJOR_CURRENCIES = {"USD", "EUR", "GBP", "JPY", "CHF", "AUD", "CAD", "NZD"}
FACTOR_ASSIGNMENT_CONTRACT_ID = (
    "major_move_factor_assignment_v2_causal_all68_m1_20260827"
)
PREVIOUS_FACTOR_ASSIGNMENT_CONTRACT_ID = (
    "major_move_factor_assignment_v1_token_recurrence"
)
FACTOR_STRENGTH_HORIZONS = tuple(int(value) for value in market_ticker.WINDOWS_MINUTES)
FACTOR_STRENGTH_EXPECTED_OBSERVATIONS = 68
FACTOR_STRENGTH_MIN_OBSERVATIONS = 60
FACTOR_STRENGTH_MAX_PAIR_AGE_SEC = 180
FACTOR_STRENGTH_AMBIGUITY_BPS = 0.5


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def stable_id(prefix: str, *parts: Any) -> str:
    material = "|".join(str(part) for part in parts)
    return f"{prefix}_{hashlib.sha256(material.encode('utf-8')).hexdigest()[:24]}"


def atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(text, encoding="utf-8")
    os.replace(temporary, path)


def preserve_pre_causal_factor_outputs(
    output_json: Path, output_md: Path, output_csv: Path
) -> dict[str, str]:
    """Preserve the last V1 current artifacts once before publishing V2."""

    if output_json.resolve() != OUTPUT_JSON.resolve():
        return {}
    existing = read_json(output_json)
    if (
        not existing
        or existing.get("factor_assignment_contract_id")
        == FACTOR_ASSIGNMENT_CONTRACT_ID
    ):
        return {}
    archives = (
        (output_json, PRIOR_OUTPUT_JSON),
        (output_md, PRIOR_OUTPUT_MD),
        (output_csv, PRIOR_OUTPUT_CSV),
    )
    preserved: dict[str, str] = {}
    for source, destination in archives:
        if not source.exists():
            continue
        if not destination.exists():
            atomic_text(destination, source.read_text(encoding="utf-8"))
        preserved[source.suffix.lstrip(".") or source.name] = str(
            destination.resolve()
        )
    return preserved


def read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def open_readonly(path: Path) -> sqlite3.Connection:
    if not path.exists():
        raise FileNotFoundError(path)
    connection = sqlite3.connect(
        f"file:{path.resolve().as_posix()}?mode=ro", uri=True, timeout=60
    )
    connection.execute("PRAGMA query_only=ON")
    return connection


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


def iso_epoch(epoch: int | float | None) -> str:
    if epoch is None:
        return ""
    return dt.datetime.fromtimestamp(float(epoch), dt.timezone.utc).isoformat()


def safe_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _payload_currencies(payload_json: str) -> set[str]:
    try:
        payload = json.loads(payload_json or "{}")
    except json.JSONDecodeError:
        return set()
    currencies: set[str] = set()
    for source in (payload, payload.get("raw_payload")):
        if not isinstance(source, Mapping):
            continue
        for key in ("currencies", "direct_currencies"):
            values = source.get(key)
            if isinstance(values, list):
                currencies.update(str(value).upper() for value in values if value)
    return currencies


def causal_effective_epoch(
    recorded_effective: Any,
    payload_json: str,
) -> tuple[int | None, bool]:
    """Return a fail-closed knowledge time for detail-enriched records."""

    effective = parse_epoch(recorded_effective)
    if effective is None:
        return None, False
    try:
        payload = json.loads(payload_json or "{}")
    except json.JSONDecodeError:
        return effective, False
    raw = payload.get("raw_payload")
    if not isinstance(raw, Mapping):
        raw = payload
    if not isinstance(raw, Mapping) or not truthy(raw.get("detail_enriched")):
        return effective, False
    detail_available = parse_epoch(raw.get("detail_available_utc"))
    if detail_available is not None and detail_available > effective:
        return detail_available, True
    return effective, False


def load_source_index(
    path: Path,
    *,
    minimum_effective_epoch: int | None = None,
    maximum_effective_epoch: int | None = None,
) -> tuple[dict[str, dict[str, Any]], str | None]:
    db = open_readonly(path)
    has_causal_view = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='view' AND name='source_events_causal_v1'"
    ).fetchone() is not None
    source_event_relation = (
        "source_events_causal_v1" if has_causal_view else "source_events"
    )
    # The governance database is deliberately comprehensive and can contain
    # hundreds of thousands of historical source versions.  A recent-move
    # census only needs the exact knowledge-time interval spanned by those
    # moves.  Loading the entire source ledger caused multi-gigabyte memory
    # growth and multi-minute rebuilds.
    where: list[str] = []
    parameters: list[str] = []
    if minimum_effective_epoch is not None:
        where.append("effective_from_utc >= ?")
        parameters.append(iso_epoch(minimum_effective_epoch))
    if maximum_effective_epoch is not None:
        where.append("effective_from_utc <= ?")
        parameters.append(iso_epoch(maximum_effective_epoch))
    where_sql = f" WHERE {' AND '.join(where)}" if where else ""
    global_highwater_row = db.execute(
        f"SELECT MAX(effective_from_utc) FROM {source_event_relation}"
    ).fetchone()
    global_highwater = str(global_highwater_row[0]) if global_highwater_row and global_highwater_row[0] else None
    available_columns = {
        str(row[1]) for row in db.execute(f"PRAGMA table_info({source_event_relation})")
    }
    optional_columns = (
        "published_at_utc",
        "first_seen_at_utc",
        "retrieved_at_utc",
        "revised_at_utc",
        "event_version",
        "supersedes_source_event_id",
    )
    optional_select = ",".join(
        name if name in available_columns else f"NULL AS {name}"
        for name in optional_columns
    )
    rows = db.execute(
        f"""SELECT source_event_id,source_id,source_population,event_type,story_cluster_id,
                  effective_from_utc,valid_until_utc,superseded_at_utc,base_currency,
                  quote_currency,payload_json,{optional_select}
           FROM {source_event_relation}{where_sql}
           ORDER BY effective_from_utc,source_event_id""",
        parameters,
    )
    raw: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for values in rows:
        effective, knowledge_time_adjusted = causal_effective_epoch(
            values[5], str(values[10] or "{}")
        )
        if effective is None:
            continue
        currencies = {str(value).upper() for value in (values[8], values[9]) if value}
        currencies.update(_payload_currencies(str(values[10] or "")))
        if not currencies:
            continue
        effective_utc = iso_epoch(effective)
        event = {
            "source_event_id": str(values[0]),
            "source_id": str(values[1]),
            "source_population": str(values[2]),
            "event_type": str(values[3]),
            "story_cluster_id": str(values[4] or ""),
            "effective_from_utc": effective_utc,
            "recorded_effective_from_utc": str(values[5]),
            "knowledge_time_adjusted_for_detail": knowledge_time_adjusted,
            "effective_epoch": effective,
            "valid_until_epoch": parse_epoch(values[6]),
            "superseded_epoch": parse_epoch(values[7]),
            "payload_json": str(values[10] or "{}"),
            "published_at_utc": str(values[11] or ""),
            "first_seen_at_utc": str(values[12] or ""),
            "retrieved_at_utc": str(values[13] or ""),
            "revised_at_utc": str(values[14] or ""),
            "event_version": int(values[15] or 1),
            "supersedes_source_event_id": str(values[16] or ""),
        }
        for currency in currencies:
            raw[currency].append(event)
    db.close()
    output: dict[str, dict[str, Any]] = {}
    for currency, events in raw.items():
        events.sort(key=lambda row: (int(row["effective_epoch"]), row["source_event_id"]))
        output[currency] = {
            "events": events,
            "epochs": [int(row["effective_epoch"]) for row in events],
        }
    return output, global_highwater


def relevant_source_events(
    source_index: Mapping[str, Any],
    currencies: Iterable[str],
    start_epoch: int,
    end_epoch: int,
    causal_at_entry: bool,
) -> list[dict[str, Any]]:
    found: dict[str, dict[str, Any]] = {}
    for currency in currencies:
        indexed = source_index.get(str(currency).upper(), {})
        # ``load_source_index`` owns these sorted sequences and this lookup is
        # read-only.  Reusing them avoids copying a currency's complete event
        # history for every narrow time-window query.
        if isinstance(indexed, Mapping):
            events = indexed.get("events") or ()
            epochs = indexed.get("epochs") or ()
        else:
            events = ()
            epochs = ()
        left = bisect.bisect_left(epochs, start_epoch)
        right = bisect.bisect_right(epochs, end_epoch)
        for event in events[left:right]:
            if causal_at_entry:
                valid_until = event.get("valid_until_epoch")
                superseded = event.get("superseded_epoch")
                if valid_until is not None and int(valid_until) <= end_epoch:
                    continue
                if superseded is not None and int(superseded) <= end_epoch:
                    continue
            found[str(event["source_event_id"])] = event
    return sorted(
        found.values(), key=lambda row: (int(row["effective_epoch"]), row["source_event_id"])
    )


def source_is_directional(event: Mapping[str, Any]) -> bool:
    """Return only a causally publishable forward direction.

    ``directional_evidence`` also covers retrospective price recaps and
    research-only semantic hypotheses.  Counting that field as a pre-entry
    directional source materially overstated news coverage in the miss
    census.  The census needs the stricter publication boundary.
    """
    try:
        payload = json.loads(str(event.get("payload_json") or "{}"))
    except json.JSONDecodeError:
        return False
    raw = payload.get("raw_payload")
    candidate = raw if isinstance(raw, Mapping) else payload
    return bool(
        candidate.get("directional_publish_eligible")
        and not candidate.get("reports_prior_market_move")
        and not candidate.get("context_only")
        and not candidate.get("directional_research_only")
    )


def map_source_state(
    row: Mapping[str, Any], source_index: Mapping[str, Any], lookback_minutes: int
) -> dict[str, int]:
    entry = int(row["start_epoch"])
    end = int(row["end_epoch"])
    currencies = [str(row["base_currency"]), str(row["quote_currency"])]
    pre = relevant_source_events(
        source_index, currencies, entry - lookback_minutes * 60, entry, True
    )
    during = relevant_source_events(source_index, currencies, entry + 1, end, False)
    return {
        "pre_entry_source_count": len(pre),
        "pre_entry_story_count": len(
            {event["story_cluster_id"] or event["source_event_id"] for event in pre}
        ),
        "pre_entry_official_count": sum(
            str(event["source_population"]).startswith("official") for event in pre
        ),
        "pre_entry_directional_count": sum(source_is_directional(event) for event in pre),
        "in_window_source_count": len(during),
        "in_window_story_count": len(
            {event["story_cluster_id"] or event["source_event_id"] for event in during}
        ),
    }


def normalize_side(value: Any) -> int:
    text = str(value or "").strip().lower()
    if text in {"1", "+1", "long", "buy", "up"}:
        return 1
    if text in {"-1", "short", "sell", "down"}:
        return -1
    try:
        return 1 if float(value) > 0 else -1 if float(value) < 0 else 0
    except (TypeError, ValueError):
        return 0


def percentile(values: Sequence[float], quantile: float) -> float:
    ordered = sorted(float(value) for value in values if math.isfinite(float(value)))
    if not ordered:
        return math.nan
    if len(ordered) == 1:
        return ordered[0]
    position = max(0.0, min(1.0, float(quantile))) * (len(ordered) - 1)
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def nonoverlapping_extremes(
    records: Sequence[dict[str, Any]], horizon_sec: int
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    occupied: list[int] = []
    for record in sorted(
        records,
        key=lambda row: (-float(row.get("endpoint_after_cost_pips") or 0.0), int(row["start_epoch"])),
    ):
        epoch = int(record["start_epoch"])
        position = bisect.bisect_left(occupied, epoch)
        neighbors = occupied[max(0, position - 1) : position + 1]
        if any(abs(epoch - prior) < int(horizon_sec) for prior in neighbors):
            continue
        bisect.insort(occupied, epoch)
        selected.append(record)
    return sorted(selected, key=lambda row: int(row["start_epoch"]))


def current_movement_contract(summary_path: Path, database: Path) -> str:
    summary = read_json(summary_path)
    contract = str(summary.get("contract_id") or "")
    if contract:
        return contract
    db = open_readonly(database)
    row = db.execute(
        "SELECT contract_id FROM research_contracts ORDER BY created_utc DESC LIMIT 1"
    ).fetchone()
    db.close()
    if not row:
        raise RuntimeError("No movement/news research contract is available")
    return str(row[0])


def base_row(
    *,
    inventory: str,
    move_id: str,
    instrument: str,
    start_epoch: int,
    end_epoch: int,
    selected_side: int,
) -> dict[str, Any]:
    base, quote = instrument.split("_", 1)
    return {
        "inventory": inventory,
        "move_id": move_id,
        "instrument": instrument,
        "base_currency": base,
        "quote_currency": quote,
        "start_epoch": int(start_epoch),
        "start_utc": iso_epoch(start_epoch),
        "end_epoch": int(end_epoch),
        "end_utc": iso_epoch(end_epoch),
        "horizon_sec": int(end_epoch - start_epoch),
        "horizon_min": int(round((end_epoch - start_epoch) / 60.0)),
        "selected_side": int(selected_side),
        "selected_side_label": "long" if selected_side > 0 else "short" if selected_side < 0 else "unknown",
        "gross_magnitude_pips": None,
        "endpoint_after_cost_pips": None,
        "modeled_cost_pips": None,
        "pre_entry_source_count": 0,
        "pre_entry_story_count": 0,
        "pre_entry_official_count": 0,
        "pre_entry_directional_count": 0,
        "in_window_source_count": 0,
        "news_match_status": "unknown",
        "source_evidence_class": "unknown",
        "top_signal_match": None,
        "top_signal_cycle": None,
        "independent_h1_models": [],
        "prospective_forecast": None,
        "gap_tags": [],
        "primary_gap": "unclassified",
        "action_branch": "unclassified",
    }


def load_canonical_episodes(
    database: Path, summary_path: Path
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    contract = current_movement_contract(summary_path, database)
    db = open_readonly(database)
    columns = [row[1] for row in db.execute("PRAGMA table_info(movement_episodes)")]
    rows = db.execute(
        "SELECT " + ",".join(columns) + " FROM movement_episodes WHERE contract_id=?",
        (contract,),
    )
    output: list[dict[str, Any]] = []
    for values in rows:
        source = dict(zip(columns, values))
        row = base_row(
            inventory="canonical_top1_movement",
            move_id=str(source["episode_id"]),
            instrument=str(source["instrument"]),
            start_epoch=int(source["entry_epoch"]),
            end_epoch=int(source["exit_epoch"]),
            selected_side=normalize_side(source["selected_side"]),
        )
        for name in (
            "gross_magnitude_pips",
            "endpoint_after_cost_pips",
            "entry_spread_pips",
            "exit_spread_pips",
            "mfe_pips",
            "mae_pips",
            "selection_threshold_pips",
            "selection_quantile",
            "pre_entry_source_count",
            "pre_entry_story_count",
            "pre_entry_official_count",
            "pre_entry_directional_count",
            "in_window_source_count",
            "in_window_story_count",
        ):
            row[name] = source.get(name)
        row["modeled_cost_pips"] = float(source["gross_magnitude_pips"]) - float(
            source["endpoint_after_cost_pips"]
        )
        row["signed_currency_factor"] = str(source["signed_currency_factor"])
        row["native_market_episode_id"] = str(source["market_episode_id"])
        row["news_match_status"] = (
            "pre_entry_mapped" if int(source["pre_entry_source_count"]) else "no_pre_entry_mapping"
        )
        row["source_evidence_class"] = "knowledge_time_mapped"
        output.append(row)
    integrity = db.execute("PRAGMA integrity_check").fetchone()[0]
    db.close()
    return output, {
        "contract_id": contract,
        "rows": len(output),
        "sqlite_integrity": integrity,
        "first_start_utc": min((row["start_utc"] for row in output), default=""),
        "last_start_utc": max((row["start_utc"] for row in output), default=""),
    }


def load_legacy_significant_tags(path: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    output: list[dict[str, Any]] = []
    malformed = 0
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for source in csv.DictReader(handle):
            start = parse_epoch(source.get("start_utc"))
            end = parse_epoch(source.get("end_utc"))
            instrument = str(source.get("instrument") or "")
            if start is None or end is None or end <= start or "_" not in instrument:
                malformed += 1
                continue
            move_id = str(source.get("move_id") or source.get("movement_key") or "")
            row = base_row(
                inventory="legacy_significant_2h_labels",
                move_id=move_id or stable_id("legacy_move", instrument, start, end),
                instrument=instrument,
                start_epoch=start,
                end_epoch=end,
                selected_side=0,
            )
            matched = str(source.get("news_match_status") or "unknown")
            tag_count = int(safe_float(source.get("news_tag_count")) or 0)
            predictive = truthy(source.get("primary_predictive_eligible"))
            relation = str(source.get("primary_causal_relation") or "")
            row.update(
                {
                    "news_match_status": matched,
                    "pre_entry_source_count": tag_count if predictive else 0,
                    "in_window_source_count": tag_count if relation == "POST_HOC_EXPLANATION" else 0,
                    "primary_event_id": str(source.get("primary_event_id") or ""),
                    "primary_event_utc": str(source.get("primary_event_utc") or ""),
                    "primary_headline": str(source.get("primary_headline") or ""),
                    "primary_causal_relation": relation,
                    "primary_predictive_eligible": predictive,
                    "source_evidence_class": "legacy_tag_mapping_only",
                    "legacy_metric_limit": "direction_and_executable_magnitude_not_retained_in_tag_file",
                }
            )
            output.append(row)
    return output, {
        "rows": len(output),
        "malformed_rows": malformed,
        "first_start_utc": min((row["start_utc"] for row in output), default=""),
        "last_start_utc": max((row["start_utc"] for row in output), default=""),
    }


def pip_size(instrument: str) -> float:
    return fallback_pip_size(instrument)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _nearest_candle_index(times: Sequence[int], target: int, tolerance: int) -> int | None:
    position = bisect.bisect_left(times, int(target))
    candidates = [index for index in (position - 1, position) if 0 <= index < len(times)]
    if not candidates:
        return None
    nearest = min(candidates, key=lambda index: (abs(times[index] - int(target)), times[index]))
    return nearest if abs(times[nearest] - int(target)) <= int(tolerance) else None


def closest_factor_strength_horizon(horizon_minutes: Any) -> int:
    try:
        duration = max(1.0, float(horizon_minutes))
    except (TypeError, ValueError):
        duration = 15.0
    return min(
        FACTOR_STRENGTH_HORIZONS,
        key=lambda horizon: (abs(float(horizon) - duration), horizon),
    )


def factor_surface_requests(
    rows: Sequence[Mapping[str, Any]],
) -> set[tuple[int, int]]:
    output: set[tuple[int, int]] = set()
    for row in rows:
        end_epoch = parse_epoch(row.get("end_epoch") or row.get("end_utc"))
        if end_epoch is None:
            continue
        horizon_minutes = float(row.get("horizon_sec") or 0.0) / 60.0
        output.add(
            (int(end_epoch), closest_factor_strength_horizon(horizon_minutes))
        )
    return output


def collect_causal_factor_observations(
    instrument: str,
    candles: Sequence[Mapping[str, Any]],
    requests: Sequence[tuple[int, int]],
    output: dict[tuple[int, int], dict[str, dict[str, Any]]],
    *,
    maximum_pair_age_sec: int = FACTOR_STRENGTH_MAX_PAIR_AGE_SEC,
) -> None:
    """Collect pair returns using only completed candles at each watermark.

    OANDA M1 candle timestamps identify the bar *open*.  The close fields are
    therefore knowledge-time safe only at ``epoch + 60``.  Treating the open
    clock as the close clock leaked as much as one minute into an end-bound
    factor surface, especially when a move endpoint landed exactly on a minute.
    """

    if not candles:
        return
    close_times = [int(candle["epoch"]) + 60 for candle in candles]
    first = close_times[0]
    last = close_times[-1]
    for end_epoch, horizon in requests:
        target_epoch = int(end_epoch) - int(horizon) * 60
        if end_epoch < first or target_epoch > last:
            continue
        end_index = bisect.bisect_right(close_times, int(end_epoch)) - 1
        old_index = bisect.bisect_right(close_times, target_epoch) - 1
        if end_index < 0 or old_index < 0 or old_index > end_index:
            continue
        end_clock = close_times[end_index]
        old_clock = close_times[old_index]
        end_age = int(end_epoch) - end_clock
        old_age = target_epoch - old_clock
        if (
            end_age < 0
            or old_age < 0
            or end_age > int(maximum_pair_age_sec)
            or old_age > int(maximum_pair_age_sec)
        ):
            continue
        latest = candles[end_index]
        old = candles[old_index]
        latest_mid = (
            float(latest["bid_close"]) + float(latest["ask_close"])
        ) / 2.0
        old_mid = (float(old["bid_close"]) + float(old["ask_close"])) / 2.0
        if latest_mid <= 0.0 or old_mid <= 0.0:
            continue
        spread_bps = max(
            0.0,
            (float(latest["ask_close"]) - float(latest["bid_close"]))
            / latest_mid
            * 10_000.0,
        )
        output.setdefault((end_epoch, horizon), {})[instrument] = {
            "latest_epoch": end_clock,
            "old_epoch": old_clock,
            "spread_bps": spread_bps,
            "return_bps": math.log(latest_mid / old_mid) * 10_000.0,
        }


def solve_causal_factor_strength_surfaces(
    requests: Sequence[tuple[int, int]],
    observations: Mapping[tuple[int, int], Mapping[str, Mapping[str, Any]]],
    expected_instruments: Sequence[str],
    *,
    minimum_observation_count: int = FACTOR_STRENGTH_MIN_OBSERVATIONS,
    expected_observation_count: int = FACTOR_STRENGTH_EXPECTED_OBSERVATIONS,
) -> tuple[dict[tuple[int, int], dict[str, Any]], dict[str, Any]]:
    expected = sorted(set(str(value) for value in expected_instruments if value))
    expected_currencies = {
        currency for instrument in expected for currency in instrument.split("_", 1)
    }
    surfaces: dict[tuple[int, int], dict[str, Any]] = {}
    for end_epoch, horizon in sorted(set(requests)):
        values = dict(observations.get((end_epoch, horizon)) or {})
        pair_moves = {
            instrument: {
                "latest_epoch": int(value.get("latest_epoch") or 0),
                "spread_bps": float(value.get("spread_bps") or 0.0),
                "windows": {
                    str(horizon): {
                        "return_bps": float(value.get("return_bps") or 0.0)
                    }
                },
            }
            for instrument, value in values.items()
        }
        solved = market_ticker.solve_currency_strength(pair_moves, horizon)
        observation_count = int(solved.get("observation_count") or 0)
        strengths = dict(solved.get("currency_strength_bps") or {})
        missing_instruments = sorted(set(expected) - set(values))
        missing_currencies = sorted(expected_currencies - set(strengths))
        latest_clocks = [
            int(value.get("latest_epoch") or 0) for value in values.values()
        ]
        newest_clock = max(latest_clocks, default=0)
        oldest_age = (
            max(int(end_epoch) - clock for clock in latest_clocks)
            if latest_clocks
            else None
        )
        valid = observation_count >= int(minimum_observation_count)
        status = (
            "insufficient_fresh_pair_coverage"
            if not valid
            else "ready_with_logged_pair_exclusions"
            if missing_instruments
            else "ready"
        )
        surfaces[(end_epoch, horizon)] = {
            "valid": valid,
            "status": status,
            "horizon_minutes": horizon,
            "move_end_utc": iso_epoch(end_epoch),
            "as_of_utc": iso_epoch(newest_clock) if newest_clock else "",
            "as_of_age_sec": (
                max(0, int(end_epoch) - newest_clock) if newest_clock else None
            ),
            "oldest_pair_age_sec": oldest_age,
            "observation_count": observation_count,
            "expected_observation_count": int(expected_observation_count),
            "coverage_pct": round(
                100.0 * observation_count / max(1, int(expected_observation_count)),
                3,
            ),
            "currency_count": int(solved.get("currency_count") or 0),
            "currency_strength_bps": strengths if valid else {},
            "missing_instruments": missing_instruments,
            "missing_currencies": missing_currencies,
            "source_contract_id": market_ticker.SCHEMA_VERSION,
        }
    ready = sum(bool(surface.get("valid")) for surface in surfaces.values())
    meta = {
        "contract_id": FACTOR_ASSIGNMENT_CONTRACT_ID,
        "source_contract_id": market_ticker.SCHEMA_VERSION,
        "requested_surface_count": len(set(requests)),
        "valid_surface_count": ready,
        "invalid_surface_count": len(surfaces) - ready,
        "expected_pair_observations": int(expected_observation_count),
        "minimum_fresh_pair_observations": int(minimum_observation_count),
        "maximum_pair_age_sec": FACTOR_STRENGTH_MAX_PAIR_AGE_SEC,
        "declared_horizons_minutes": list(FACTOR_STRENGTH_HORIZONS),
        "watermark": "at_or_before_move_end",
        "role": "after_the_fact_factor_clustering_only",
    }
    return surfaces, meta


def recover_legacy_executable_paths(
    rows: Sequence[dict[str, Any]],
    candle_root: Path,
    tolerance_sec: int = 300,
    *,
    factor_rows: Sequence[Mapping[str, Any]] = (),
    factor_strength_surfaces: dict[tuple[int, int], dict[str, Any]] | None = None,
    factor_strength_meta: dict[str, Any] | None = None,
    factor_minimum_observations: int = FACTOR_STRENGTH_MIN_OBSERVATIONS,
    factor_expected_observations: int = FACTOR_STRENGTH_EXPECTED_OBSERVATIONS,
    factor_maximum_pair_age_sec: int = FACTOR_STRENGTH_MAX_PAIR_AGE_SEC,
) -> dict[str, Any]:
    """Recover only legacy labels that overlap the retained executable BAM archive.

    The legacy labels were selected after their outcomes and remain diagnostic.
    Missing archive coverage stays unavailable; no midpoint or synthetic spread is
    substituted for executable bid/ask candles.
    """

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row["instrument"])].append(row)
    factor_requests = factor_surface_requests(factor_rows)
    factor_observations: dict[
        tuple[int, int], dict[str, dict[str, Any]]
    ] = {}
    expected_instruments = sorted(
        path.stem.removesuffix("_M1")
        for path in candle_root.glob("*_M1.csv")
    )
    recovered = 0
    no_file = 0
    no_timestamp_coverage = 0
    invalid_candles = 0
    recovered_files: dict[str, dict[str, Any]] = {}
    examples: list[dict[str, Any]] = []
    instruments = sorted(set(grouped) | set(expected_instruments))
    for instrument in instruments:
        instrument_rows = grouped.get(instrument, [])
        path = candle_root / f"{instrument}_M1.csv"
        if not path.exists():
            no_file += len(instrument_rows)
            continue
        candles: list[dict[str, Any]] = []
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            for source in csv.DictReader(handle):
                epoch = parse_epoch(source.get("time") or source.get("datetime"))
                values = {
                    name: safe_float(source.get(name))
                    for name in (
                        "bid_open", "bid_high", "bid_low", "bid_close",
                        "ask_open", "ask_high", "ask_low", "ask_close",
                    )
                }
                if epoch is None or any(value is None for value in values.values()):
                    invalid_candles += 1
                    continue
                candles.append({"epoch": epoch, **values})
        candles.sort(key=lambda candle: int(candle["epoch"]))
        times = [int(candle["epoch"]) for candle in candles]
        collect_causal_factor_observations(
            instrument,
            candles,
            factor_requests,
            factor_observations,
            maximum_pair_age_sec=factor_maximum_pair_age_sec,
        )
        file_used = False
        for row in instrument_rows:
            entry_index = _nearest_candle_index(
                times, int(row["start_epoch"]), int(tolerance_sec)
            )
            exit_index = _nearest_candle_index(
                times, int(row["end_epoch"]), int(tolerance_sec)
            )
            if entry_index is None or exit_index is None or exit_index < entry_index:
                no_timestamp_coverage += 1
                row["legacy_replay_status"] = "executable_archive_unavailable"
                continue
            entry = candles[entry_index]
            exit_candle = candles[exit_index]
            pip = pip_size(instrument)
            entry_mid = (float(entry["bid_open"]) + float(entry["ask_open"])) / 2.0
            exit_mid = (
                float(exit_candle["bid_close"]) + float(exit_candle["ask_close"])
            ) / 2.0
            side = 1 if exit_mid > entry_mid else -1 if exit_mid < entry_mid else 0
            if side == 0:
                no_timestamp_coverage += 1
                row["legacy_replay_status"] = "flat_endpoint"
                continue
            path_slice = candles[entry_index : exit_index + 1]
            if side > 0:
                executable = (
                    float(exit_candle["bid_close"]) - float(entry["ask_open"])
                ) / pip
                path_returns = [
                    (float(candle["bid_high"]) - float(entry["ask_open"])) / pip
                    for candle in path_slice
                ]
                adverse_returns = [
                    (float(candle["bid_low"]) - float(entry["ask_open"])) / pip
                    for candle in path_slice
                ]
            else:
                executable = (
                    float(entry["bid_open"]) - float(exit_candle["ask_close"])
                ) / pip
                path_returns = [
                    (float(entry["bid_open"]) - float(candle["ask_low"])) / pip
                    for candle in path_slice
                ]
                adverse_returns = [
                    (float(entry["bid_open"]) - float(candle["ask_high"])) / pip
                    for candle in path_slice
                ]
            gross = abs(exit_mid - entry_mid) / pip
            entry_spread = (float(entry["ask_open"]) - float(entry["bid_open"])) / pip
            exit_spread = (
                float(exit_candle["ask_close"]) - float(exit_candle["bid_close"])
            ) / pip
            row.update(
                {
                    "selected_side": side,
                    "selected_side_label": "long" if side > 0 else "short",
                    "gross_magnitude_pips": gross,
                    "endpoint_after_cost_pips": executable,
                    "modeled_cost_pips": gross - executable,
                    "entry_spread_pips": entry_spread,
                    "exit_spread_pips": exit_spread,
                    "mfe_pips": max(path_returns),
                    "mae_pips": min(adverse_returns),
                    "legacy_replay_status": "recovered_executable_bam",
                    "legacy_entry_observed_utc": iso_epoch(int(entry["epoch"])),
                    "legacy_exit_observed_utc": iso_epoch(int(exit_candle["epoch"])),
                    "source_evidence_class": "legacy_tag_mapping_plus_local_executable_replay",
                    "legacy_metric_limit": "hindsight_label_with_executable_path_no_historical_candidate_universe",
                }
            )
            recovered += 1
            file_used = True
            if len(examples) < 20:
                examples.append(
                    {
                        "move_id": row["move_id"],
                        "instrument": instrument,
                        "start_utc": row["start_utc"],
                        "end_utc": row["end_utc"],
                        "side": row["selected_side_label"],
                        "gross_magnitude_pips": gross,
                        "endpoint_after_cost_pips": executable,
                    }
                )
        if file_used:
            recovered_files[instrument] = {
                "path": str(path.resolve()),
                "bytes": path.stat().st_size,
                "sha256": file_sha256(path),
            }
    surfaces, surface_meta = solve_causal_factor_strength_surfaces(
        factor_requests,
        factor_observations,
        expected_instruments,
        minimum_observation_count=factor_minimum_observations,
        expected_observation_count=factor_expected_observations,
    )
    surface_meta["candle_root"] = str(candle_root.resolve())
    surface_meta["candle_file_count"] = len(expected_instruments)
    surface_meta["maximum_pair_age_sec"] = int(factor_maximum_pair_age_sec)
    if factor_strength_surfaces is not None:
        factor_strength_surfaces.update(surfaces)
    if factor_strength_meta is not None:
        factor_strength_meta.update(surface_meta)
    return {
        "attempted_rows": len(rows),
        "recovered_rows": recovered,
        "unavailable_rows": len(rows) - recovered,
        "missing_pair_file_rows": no_file,
        "timestamp_unavailable_rows": no_timestamp_coverage,
        "invalid_candle_rows": invalid_candles,
        "tolerance_sec": int(tolerance_sec),
        "candle_root": str(candle_root.resolve()),
        "recovered_files": recovered_files,
        "examples": examples,
        "evidence_class": "retrospective_diagnostic_not_proof",
    }


def load_top_signal_index(database: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    db = open_readonly(database)
    query = """
        SELECT id,signal_id,opened_epoch,target_epoch,instrument,direction,side,
               horizon_sec,family,lane_id,policy_state,signal_eligible,validated,
               direction_conflict,blocked_by_json,confidence,projected_net_pips,
               projected_net_pips_per_hour,gross_to_spread,entry_spread_pips,
               measurement_version,maturity_valid,maturity_reason
        FROM positions ORDER BY opened_epoch,id
    """
    by_pair_horizon: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    by_horizon: dict[int, list[dict[str, Any]]] = defaultdict(list)
    total = 0
    minimum: dict[int, float] = {}
    maximum: dict[int, float] = {}
    for values in db.execute(query):
        (
            row_id,
            signal_id,
            opened,
            target,
            instrument,
            direction,
            side,
            horizon,
            family,
            lane_id,
            policy_state,
            eligible,
            validated,
            conflict,
            blocked,
            confidence,
            projected,
            projected_hour,
            gross_to_spread,
            spread,
            measurement,
            maturity_valid,
            maturity_reason,
        ) = values
        record = {
            "id": int(row_id),
            "signal_id": str(signal_id),
            "opened_epoch": float(opened),
            "target_epoch": float(target),
            "instrument": str(instrument),
            "direction": normalize_side(direction or side),
            "horizon_sec": int(horizon),
            "family": str(family),
            "lane_id": str(lane_id),
            "policy_state": str(policy_state),
            "signal_eligible": bool(eligible),
            "validated": bool(validated),
            "direction_conflict": bool(conflict),
            "blocked_by": str(blocked or ""),
            "confidence": float(confidence or 0.0),
            "projected_net_pips": float(projected or 0.0),
            "projected_net_pips_per_hour": float(projected_hour or 0.0),
            "gross_to_spread": float(gross_to_spread or 0.0),
            "entry_spread_pips": float(spread or 0.0),
            "measurement_version": str(measurement or ""),
            "maturity_valid": None if maturity_valid is None else bool(maturity_valid),
            "maturity_reason": str(maturity_reason or ""),
        }
        key = (record["instrument"], record["horizon_sec"])
        by_pair_horizon[key].append(record)
        by_horizon[record["horizon_sec"]].append(record)
        minimum[record["horizon_sec"]] = min(
            minimum.get(record["horizon_sec"], record["opened_epoch"]), record["opened_epoch"]
        )
        maximum[record["horizon_sec"]] = max(
            maximum.get(record["horizon_sec"], record["opened_epoch"]), record["opened_epoch"]
        )
        total += 1
    db.close()
    index = {
        "by_pair_horizon": dict(by_pair_horizon),
        "by_horizon": dict(by_horizon),
        "times_pair_horizon": {
            key: [float(record["opened_epoch"]) for record in records]
            for key, records in by_pair_horizon.items()
        },
        "times_horizon": {
            key: [float(record["opened_epoch"]) for record in records]
            for key, records in by_horizon.items()
        },
        "minimum": minimum,
        "maximum": maximum,
    }
    return index, {
        "rows": total,
        "horizons": sorted(minimum),
        "first_utc_by_horizon": {str(key): iso_epoch(value) for key, value in minimum.items()},
        "last_utc_by_horizon": {str(key): iso_epoch(value) for key, value in maximum.items()},
    }


def _best_active(
    records: Sequence[dict[str, Any]],
    times: Sequence[float],
    start_epoch: int,
    end_epoch: int,
    target_tolerance: int,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    position = bisect.bisect_right(times, float(start_epoch))
    exact: list[dict[str, Any]] = []
    overlapping: list[dict[str, Any]] = []
    for record in records[max(0, position - 32) : position]:
        if float(record["target_epoch"]) < float(start_epoch):
            continue
        overlapping.append(record)
        if abs(float(record["target_epoch"]) - float(end_epoch)) <= target_tolerance:
            exact.append(record)
    exact_row = min(
        exact,
        key=lambda row: (abs(float(row["target_epoch"]) - end_epoch), -float(row["opened_epoch"])),
        default=None,
    )
    overlap_row = max(overlapping, key=lambda row: float(row["opened_epoch"]), default=None)
    return exact_row, overlap_row


def match_top_signal(
    row: dict[str, Any], index: Mapping[str, Any], target_tolerance: int
) -> None:
    pair_key = (str(row["instrument"]), int(row["horizon_sec"]))
    records = index["by_pair_horizon"].get(pair_key, [])
    times = index["times_pair_horizon"].get(pair_key, [])
    exact, overlap = _best_active(
        records, times, int(row["start_epoch"]), int(row["end_epoch"]), target_tolerance
    )
    row["top_signal_match"] = exact
    row["top_signal_overlap"] = overlap
    horizon = int(row["horizon_sec"])
    cycle_records = index["by_horizon"].get(horizon, [])
    cycle_times = index["times_horizon"].get(horizon, [])
    cycle_exact, cycle_overlap = _best_active(
        cycle_records,
        cycle_times,
        int(row["start_epoch"]),
        int(row["end_epoch"]),
        target_tolerance,
    )
    row["top_signal_cycle"] = cycle_exact or cycle_overlap


def classify_top_signal(signal: Mapping[str, Any], actual_side: int) -> str:
    predicted = normalize_side(signal.get("direction"))
    if not actual_side or not predicted:
        return "selected_signal_not_direction_scorable"
    if predicted != actual_side:
        return "selected_wrong_direction"
    if bool(signal.get("direction_conflict")):
        return "selected_correct_direction_conflicted"
    if not bool(signal.get("signal_eligible")) or not bool(signal.get("validated")):
        return "selected_correct_but_unvalidated"
    if float(signal.get("projected_net_pips") or 0.0) <= 0.0:
        return "selected_correct_nonpositive_projected_edge"
    blocked = str(signal.get("blocked_by") or "").strip()
    if blocked not in {"", "[]", "{}", "null"}:
        return "selected_correct_but_blocked"
    return "selected_directional_catch_shadow"


def summarize_h1_models(models: Sequence[Mapping[str, Any]], actual_side: int) -> dict[str, Any]:
    latest: dict[str, Mapping[str, Any]] = {}
    for model in models:
        family = str(model.get("family") or model.get("model_id") or "unknown")
        prior = latest.get(family)
        if prior is None or float(model.get("generated_epoch") or 0.0) > float(
            prior.get("generated_epoch") or 0.0
        ):
            latest[family] = model
    rows = list(latest.values())
    directions = [normalize_side(row.get("direction")) for row in rows]
    directions = [value for value in directions if value]
    vote = sum(directions)
    consensus = 1 if vote > 0 else -1 if vote < 0 else 0
    confidences = [
        abs(2.0 * float(row.get("probability_up") or 0.5) - 1.0) for row in rows
    ]
    magnitudes = [float(row.get("predicted_magnitude_pips") or 0.0) for row in rows]
    return {
        "model_count": len(rows),
        "families": sorted(latest),
        "long_votes": sum(value > 0 for value in directions),
        "short_votes": sum(value < 0 for value in directions),
        "consensus_side": consensus,
        "consensus_correct": bool(actual_side and consensus == actual_side),
        "direction_conflict": consensus == 0 and bool(directions),
        "median_direction_confidence": percentile(confidences, 0.5) if confidences else None,
        "median_predicted_magnitude_pips": percentile(magnitudes, 0.5) if magnitudes else None,
        "raw_models": rows,
    }


def scan_h1_forecasts(
    database: Path,
    target_rows: Sequence[dict[str, Any]],
    recent_cutoff_epoch: int,
    target_tolerance: int,
    quantile: float,
    minimum_multiple: float,
) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]], dict[str, Any]]:
    targets: dict[str, list[tuple[int, int, str]]] = defaultdict(list)
    target_starts: dict[str, list[int]] = {}
    for row in target_rows:
        if int(row["horizon_sec"]) == 3600 and int(row.get("selected_side") or 0):
            targets[str(row["instrument"])].append(
                (int(row["start_epoch"]), int(row["end_epoch"]), str(row["move_id"]))
            )
    for instrument in targets:
        targets[instrument].sort()
        target_starts[instrument] = [value[0] for value in targets[instrument]]

    matches: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    recent: dict[tuple[str, int], dict[str, Any]] = {}
    db = open_readonly(database)
    query = """
        SELECT family,model_id,instrument,generated_epoch,target_epoch,direction,
               probability_up,predicted_signed_pips,predicted_magnitude_pips,
               executable_net_pips,signed_mid_move_pips,entry_spread_pips,
               exit_spread_pips
        FROM outcomes WHERE horizon_sec=3600
    """
    scanned = 0
    minimum_epoch: float | None = None
    maximum_epoch: float | None = None
    cursor = db.execute(query)
    while True:
        batch = cursor.fetchmany(20_000)
        if not batch:
            break
        for values in batch:
            (
                family,
                model_id,
                instrument,
                generated,
                target,
                direction,
                probability_up,
                predicted_signed,
                predicted_magnitude,
                executable_net,
                signed_move,
                entry_spread,
                exit_spread,
            ) = values
            generated = float(generated)
            target = float(target)
            instrument = str(instrument)
            scanned += 1
            minimum_epoch = generated if minimum_epoch is None else min(minimum_epoch, generated)
            maximum_epoch = generated if maximum_epoch is None else max(maximum_epoch, generated)
            model = {
                "family": str(family),
                "model_id": str(model_id),
                "generated_epoch": generated,
                "target_epoch": target,
                "direction": normalize_side(direction),
                "probability_up": float(probability_up),
                "predicted_signed_pips": float(predicted_signed),
                "predicted_magnitude_pips": float(predicted_magnitude),
                "executable_net_pips": float(executable_net),
            }
            starts = target_starts.get(instrument)
            if starts:
                left = bisect.bisect_left(starts, int(generated))
                right = bisect.bisect_right(starts, int(generated) + target_tolerance)
                for candidate_index in range(left, right):
                    start, end, move_id = targets[instrument][candidate_index]
                    if generated > start or abs(target - end) > target_tolerance:
                        continue
                    prior = matches[move_id].get(str(family))
                    if prior is None or (
                        abs(target - end), -generated
                    ) < (
                        abs(float(prior["target_epoch"]) - end),
                        -float(prior["generated_epoch"]),
                    ):
                        matches[move_id][str(family)] = model

            if generated <= recent_cutoff_epoch:
                continue
            key = (instrument, int(round(generated)))
            record = recent.get(key)
            if record is None:
                cost = (float(entry_spread) + float(exit_spread)) / 2.0
                room = abs(float(signed_move)) - cost
                record = base_row(
                    inventory="post_catalog_h1_shadow_path",
                    move_id=stable_id("post_h1_move", instrument, key[1]),
                    instrument=instrument,
                    start_epoch=key[1],
                    end_epoch=int(round(target)),
                    selected_side=1 if float(signed_move) > 0 else -1 if float(signed_move) < 0 else 0,
                )
                record.update(
                    {
                        "gross_magnitude_pips": abs(float(signed_move)),
                        "endpoint_after_cost_pips": room,
                        "modeled_cost_pips": cost,
                        "entry_spread_pips": float(entry_spread),
                        "exit_spread_pips": float(exit_spread),
                        "source_evidence_class": "prospective_forecast_path",
                        "news_match_status": "pending_knowledge_time_remap",
                    }
                )
                recent[key] = record
            record["independent_h1_models"].append(model)
    integrity = db.execute("PRAGMA integrity_check").fetchone()[0]
    db.close()

    selected: list[dict[str, Any]] = []
    by_pair: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in recent.values():
        if float(record.get("endpoint_after_cost_pips") or 0.0) > 0.0:
            by_pair[str(record["instrument"])].append(record)
    for records in by_pair.values():
        threshold = percentile(
            [float(record["endpoint_after_cost_pips"]) for record in records], quantile
        )
        candidates = [
            record
            for record in records
            if float(record["endpoint_after_cost_pips"]) >= threshold
            and float(record["gross_magnitude_pips"])
            >= minimum_multiple * float(record["modeled_cost_pips"])
        ]
        for record in candidates:
            record["selection_quantile"] = quantile
            record["selection_threshold_pips"] = threshold
        selected.extend(nonoverlapping_extremes(candidates, 3600))
    return (
        {move_id: list(families.values()) for move_id, families in matches.items()},
        selected,
        {
            "rows_scanned": scanned,
            "first_generated_utc": iso_epoch(minimum_epoch),
            "last_generated_utc": iso_epoch(maximum_epoch),
            "matched_episode_count": len(matches),
            "post_catalog_major_moves": len(selected),
            "sqlite_integrity": integrity,
        },
    )


def load_prospective_major_moves(
    database: Path, quantile: float, minimum_multiple: float
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    db = open_readonly(database)
    query = """
        SELECT f.forecast_id,f.cohort_id,f.issued_at_utc,f.entry_epoch,
               f.knowledge_time_utc,f.instrument,f.horizon_sec,f.entry_spread_pips,
               f.modeled_entry_cost_pips,f.predicted_clear_probability,
               f.predicted_up_probability,f.predicted_direction,
               f.predicted_direction_confidence,f.predicted_magnitude_pips,
               f.predicted_ev_pips,f.passed_frozen_gate,o.signed_move_pips,
               o.absolute_move_pips,o.modeled_cost_pips,o.movement_cleared_cost,
               o.predicted_side_net_pips,o.direction_correct
        FROM forecasts f JOIN outcomes o ON o.forecast_id=f.forecast_id
        ORDER BY f.entry_epoch,f.instrument,f.horizon_sec
    """
    by_group: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    scanned = 0
    minimum_epoch: int | None = None
    maximum_epoch: int | None = None
    for values in db.execute(query):
        (
            forecast_id,
            cohort_id,
            issued,
            entry_epoch,
            knowledge_time,
            instrument,
            horizon,
            entry_spread,
            entry_cost,
            clear_probability,
            up_probability,
            predicted_direction,
            direction_confidence,
            predicted_magnitude,
            predicted_ev,
            gate_pass,
            signed_move,
            absolute_move,
            modeled_cost,
            movement_cleared,
            predicted_side_net,
            direction_correct,
        ) = values
        scanned += 1
        epoch = int(entry_epoch)
        horizon = int(horizon)
        minimum_epoch = epoch if minimum_epoch is None else min(minimum_epoch, epoch)
        maximum_epoch = epoch if maximum_epoch is None else max(maximum_epoch, epoch)
        room = float(absolute_move) - float(modeled_cost)
        if room <= 0.0:
            continue
        side = 1 if float(signed_move) > 0 else -1 if float(signed_move) < 0 else 0
        row = base_row(
            inventory="prospective_h5_h15_path",
            move_id=stable_id("prospective_move", forecast_id),
            instrument=str(instrument),
            start_epoch=epoch,
            end_epoch=epoch + horizon,
            selected_side=side,
        )
        row.update(
            {
                "gross_magnitude_pips": float(absolute_move),
                "endpoint_after_cost_pips": room,
                "modeled_cost_pips": float(modeled_cost),
                "entry_spread_pips": float(entry_spread),
                "source_evidence_class": "prospective_forecast_path",
                "news_match_status": "pending_knowledge_time_remap",
                "prospective_forecast": {
                    "forecast_id": str(forecast_id),
                    "cohort_id": str(cohort_id),
                    "issued_at_utc": str(issued),
                    "knowledge_time_utc": str(knowledge_time),
                    "direction": int(predicted_direction),
                    "predicted_clear_probability": float(clear_probability),
                    "predicted_up_probability": float(up_probability),
                    "predicted_direction_confidence": float(direction_confidence),
                    "predicted_magnitude_pips": float(predicted_magnitude),
                    "predicted_ev_pips": float(predicted_ev),
                    "passed_frozen_gate": bool(gate_pass),
                    "predicted_side_net_pips": float(predicted_side_net),
                    "direction_correct": bool(direction_correct),
                    "movement_cleared_cost": bool(movement_cleared),
                    "modeled_entry_cost_pips": float(entry_cost),
                },
            }
        )
        by_group[(str(instrument), horizon)].append(row)
    integrity = db.execute("PRAGMA integrity_check").fetchone()[0]
    db.close()

    selected: list[dict[str, Any]] = []
    for (_, horizon), records in by_group.items():
        threshold = percentile(
            [float(record["endpoint_after_cost_pips"]) for record in records], quantile
        )
        candidates = [
            record
            for record in records
            if float(record["endpoint_after_cost_pips"]) >= threshold
            and float(record["gross_magnitude_pips"])
            >= minimum_multiple * float(record["modeled_cost_pips"])
        ]
        for record in candidates:
            record["selection_quantile"] = quantile
            record["selection_threshold_pips"] = threshold
        selected.extend(nonoverlapping_extremes(candidates, horizon))
    return selected, {
        "matured_forecasts_scanned": scanned,
        "major_moves": len(selected),
        "first_entry_utc": iso_epoch(minimum_epoch),
        "last_entry_utc": iso_epoch(maximum_epoch),
        "sqlite_integrity": integrity,
    }


def remap_recent_news(
    rows: Iterable[dict[str, Any]], source_database: Path, lookback_minutes: int
) -> dict[str, Any]:
    materialized = [
        row
        for row in rows
        if row.get("source_evidence_class") == "prospective_forecast_path"
    ]
    if not materialized:
        return {"rows_remapped": 0, "source_event_highwater_utc": None}
    lower = min(int(row["start_epoch"]) for row in materialized) - lookback_minutes * 60
    upper = max(int(row["end_epoch"]) for row in materialized)
    source_index, highwater = load_source_index(
        source_database,
        minimum_effective_epoch=lower,
        maximum_effective_epoch=upper,
    )
    mapped = 0
    for row in materialized:
        result = map_source_state(row, source_index, lookback_minutes)
        for name in (
            "pre_entry_source_count",
            "pre_entry_story_count",
            "pre_entry_official_count",
            "pre_entry_directional_count",
            "in_window_source_count",
            "in_window_story_count",
        ):
            row[name] = int(result.get(name) or 0)
        row["news_match_status"] = (
            "pre_entry_mapped" if row["pre_entry_source_count"] else "no_pre_entry_mapping"
        )
        row["source_evidence_class"] = "prospective_knowledge_time_mapped"
        mapped += 1
    return {"rows_remapped": mapped, "source_event_highwater_utc": highwater}


def classify_prospective(
    forecast: Mapping[str, Any], actual_side: int, config: Mapping[str, Any]
) -> str:
    predicted = normalize_side(forecast.get("direction"))
    passed = bool(forecast.get("passed_frozen_gate"))
    if predicted != actual_side:
        return "frozen_gate_wrong_direction" if passed else "wrong_direction_gate_suppressed"
    if passed:
        return "frozen_gate_directional_catch"
    if float(forecast.get("predicted_clear_probability") or 0.0) < float(
        config.get("minimum_clear_probability", 0.55)
    ):
        return "correct_direction_low_cost_clearance"
    if float(forecast.get("predicted_direction_confidence") or 0.0) < float(
        config.get("minimum_direction_confidence", 0.10)
    ):
        return "correct_direction_underconfident"
    required = float(config.get("minimum_predicted_magnitude_cost_ratio", 1.5)) * float(
        forecast.get("modeled_entry_cost_pips") or 0.0
    )
    if float(forecast.get("predicted_magnitude_pips") or 0.0) < required:
        return "correct_direction_underpredicted_magnitude"
    if float(forecast.get("predicted_ev_pips") or 0.0) <= 0.0:
        return "correct_direction_negative_ev"
    return "correct_direction_other_gate_failure"


def _signed_factor_score(
    token: str, strengths: Mapping[str, Any]
) -> float | None:
    currency = str(token or "")[:-1]
    suffix = str(token or "")[-1:]
    if currency not in strengths or suffix not in {"+", "-"}:
        return None
    value = safe_float(strengths.get(currency))
    if value is None:
        return None
    return value if suffix == "+" else -value


def assign_factor_episodes(
    rows: list[dict[str, Any]],
    config: Mapping[str, Any],
    *,
    strength_surfaces: Mapping[tuple[int, int], Mapping[str, Any]] | None = None,
) -> int:
    groups: dict[tuple[bool, int, int], list[dict[str, Any]]] = defaultdict(list)
    surfaces = strength_surfaces or {}
    short_window = int(config.get("factor_window_short_sec", 900))
    long_window = int(config.get("factor_window_long_sec", 3600))
    for row in rows:
        known = bool(int(row.get("selected_side") or 0))
        window = short_window if int(row["horizon_sec"]) <= short_window else long_window
        bucket = int((int(row["start_epoch"]) + window // 2) // window)
        if known:
            side = int(row["selected_side"])
            tokens = [
                f"{row['base_currency']}{'+' if side > 0 else '-'}",
                f"{row['quote_currency']}{'-' if side > 0 else '+'}",
            ]
        else:
            tokens = [str(row["base_currency"]), str(row["quote_currency"])]
        row["factor_tokens"] = tokens
        groups[(known, window, bucket)].append(row)

    representatives: dict[str, dict[str, Any]] = {}
    for (known, window, bucket), group in groups.items():
        counts = Counter(token for row in group for token in row["factor_tokens"])
        for row in group:
            horizon = closest_factor_strength_horizon(
                float(row.get("horizon_sec") or 0.0) / 60.0
            )
            end_epoch = int(row.get("end_epoch") or 0)
            surface = dict(surfaces.get((end_epoch, horizon)) or {})
            strengths = surface.get("currency_strength_bps") or {}
            scored = {
                token: _signed_factor_score(token, strengths)
                for token in row["factor_tokens"]
            }
            if known and bool(surface.get("valid")) and all(
                value is not None for value in scored.values()
            ):
                ordered = sorted(
                    row["factor_tokens"],
                    key=lambda token: (-float(scored[token]), token),
                )
                primary = ordered[0]
                margin = float(scored[ordered[0]]) - float(scored[ordered[1]])
                method = "causal_all68_currency_strength"
                margin_unit = "bps"
            else:
                ordered = sorted(
                    row["factor_tokens"],
                    key=lambda token: (-counts[token], token),
                )
                primary = ordered[0]
                margin = float(counts[ordered[0]] - counts[ordered[1]])
                method = (
                    "time_bucket_token_recurrence_fallback"
                    if known
                    else "unsigned_time_bucket_token_recurrence"
                )
                margin_unit = "token_count"
            factor_id = stable_id(
                "factor_episode",
                FACTOR_ASSIGNMENT_CONTRACT_ID,
                "directional" if known else "unsigned",
                window,
                bucket,
                primary,
            )
            row["factor_episode_id"] = factor_id
            row["factor_primary_token"] = primary
            row["factor_assignment_contract_id"] = FACTOR_ASSIGNMENT_CONTRACT_ID
            row["factor_primary_method"] = method
            row["factor_strength_surface_status"] = str(
                surface.get("status")
                or ("direction_unknown" if not known else "unavailable")
            )
            row["factor_strength_horizon_minutes"] = horizon
            row["factor_strength_as_of_utc"] = str(
                surface.get("as_of_utc") or ""
            )
            row["factor_strength_as_of_age_sec"] = surface.get("as_of_age_sec")
            row["factor_strength_oldest_pair_age_sec"] = surface.get(
                "oldest_pair_age_sec"
            )
            row["factor_strength_observation_count"] = int(
                surface.get("observation_count") or 0
            )
            row["factor_strength_expected_observation_count"] = int(
                surface.get("expected_observation_count")
                or FACTOR_STRENGTH_EXPECTED_OBSERVATIONS
            )
            row["factor_strength_coverage_pct"] = surface.get("coverage_pct")
            row["factor_strength_currency_count"] = int(
                surface.get("currency_count") or 0
            )
            row["factor_strength_missing_instrument_count"] = len(
                surface.get("missing_instruments") or []
            )
            row["factor_strength_missing_currencies"] = list(
                surface.get("missing_currencies") or []
            )
            row["factor_strength_source_contract_id"] = str(
                surface.get("source_contract_id") or ""
            )
            row["factor_primary_scores_bps"] = {
                token: round(float(value), 6)
                for token, value in scored.items()
                if value is not None
            }
            row["factor_primary_margin"] = round(margin, 6)
            row["factor_primary_margin_unit"] = margin_unit
            row["factor_primary_ambiguous"] = bool(
                margin <= FACTOR_STRENGTH_AMBIGUITY_BPS
                if method == "causal_all68_currency_strength"
                else margin <= 0.0
            )
            current = representatives.get(factor_id)
            current_value = (
                float(current.get("endpoint_after_cost_pips") or -math.inf)
                if current
                else -math.inf
            )
            candidate_value = float(row.get("endpoint_after_cost_pips") or -math.inf)
            if current is None or (candidate_value, -int(row["start_epoch"])) > (
                current_value,
                -int(current["start_epoch"]),
            ):
                representatives[factor_id] = row
    for row in rows:
        row["factor_representative"] = representatives[row["factor_episode_id"]] is row
    return len(representatives)


def assign_liquidity_bucket(row: dict[str, Any]) -> None:
    cost = safe_float(row.get("modeled_cost_pips"))
    currencies = {str(row["base_currency"]), str(row["quote_currency"])}
    if cost is None:
        bucket = "historical_cost_unknown"
        multiple = None
    elif currencies <= MAJOR_CURRENCIES and cost <= 5.0:
        bucket = "liquid_major"
        multiple = float(row.get("endpoint_after_cost_pips") or 0.0) / max(cost, 1e-9)
    elif cost <= 10.0:
        bucket = "standard_cost"
        multiple = float(row.get("endpoint_after_cost_pips") or 0.0) / max(cost, 1e-9)
    else:
        bucket = "wide_cost"
        multiple = float(row.get("endpoint_after_cost_pips") or 0.0) / max(cost, 1e-9)
    row["liquidity_bucket"] = bucket
    row["after_cost_multiple"] = multiple


def apply_gap_classification(
    row: dict[str, Any], top_index: Mapping[str, Any], config: Mapping[str, Any]
) -> None:
    tags: list[str] = []
    actual_side = int(row.get("selected_side") or 0)
    prospective = row.get("prospective_forecast")
    top = row.get("top_signal_match")
    h1_summary = summarize_h1_models(row.get("independent_h1_models") or [], actual_side)
    row["independent_h1_summary"] = h1_summary

    if not actual_side:
        primary = "historical_direction_not_retained"
        tags += ["historical_replay_required", "historical_direction_not_retained"]
    elif prospective:
        primary = classify_prospective(prospective, actual_side, config)
        tags.append(primary)
    elif top:
        primary = classify_top_signal(top, actual_side)
        tags.append(primary)
        if h1_summary["model_count"] and h1_summary["consensus_side"]:
            if normalize_side(top.get("direction")) != actual_side and h1_summary["consensus_correct"]:
                tags.append("selected_signal_overrode_correct_h1_consensus")
            if normalize_side(top.get("direction")) == actual_side and not h1_summary["consensus_correct"]:
                tags.append("selected_signal_beat_h1_consensus")
    else:
        horizon = int(row["horizon_sec"])
        minimum = top_index["minimum"].get(horizon)
        maximum = top_index["maximum"].get(horizon)
        start = int(row["start_epoch"])
        if minimum is None:
            primary = "forecast_horizon_not_recorded"
        elif start < minimum:
            primary = "precedes_recorded_bot_history"
        elif start > maximum + horizon:
            primary = "after_retained_top_signal_history"
        elif row.get("top_signal_cycle"):
            primary = "cross_sectional_selection_gap"
            tags.append("full_candidate_set_not_retained")
        else:
            primary = "missing_top_signal_decision_snapshot"
        tags.append(primary)
        if h1_summary["model_count"]:
            if h1_summary["direction_conflict"]:
                tags.append("independent_h1_direction_conflict")
            elif h1_summary["consensus_correct"]:
                tags.append("unselected_move_had_correct_h1_consensus")
            else:
                tags.append("independent_h1_consensus_wrong")

    pre = int(row.get("pre_entry_source_count") or 0)
    official = int(row.get("pre_entry_official_count") or 0)
    directional = int(row.get("pre_entry_directional_count") or 0)
    during = int(row.get("in_window_source_count") or 0)
    if pre == 0:
        tags.append("no_causal_pre_entry_source")
        if during > 0:
            tags.append("source_arrived_during_move_only")
    if official == 0:
        tags.append("no_official_pre_entry_source")
    if directional == 0:
        tags.append("no_directional_pre_entry_source")
    if str(row.get("source_evidence_class") or "").startswith("legacy_tag_mapping"):
        tags.append("legacy_source_mapping_not_current_governance")

    branch = "forecast_history_replay"
    if primary in {
        "selected_wrong_direction",
        "wrong_direction_gate_suppressed",
        "frozen_gate_wrong_direction",
    } or "independent_h1_consensus_wrong" in tags:
        branch = "direction_model"
    elif primary in {
        "correct_direction_low_cost_clearance",
        "correct_direction_underconfident",
        "correct_direction_underpredicted_magnitude",
        "correct_direction_negative_ev",
        "correct_direction_other_gate_failure",
        "selected_correct_nonpositive_projected_edge",
    }:
        branch = "magnitude_cost_calibration"
    elif primary == "cross_sectional_selection_gap" or "unselected_move_had_correct_h1_consensus" in tags:
        branch = "cross_sectional_allocator"
    elif "independent_h1_direction_conflict" in tags:
        branch = "independence_conflict"
    elif "source_arrived_during_move_only" in tags:
        branch = "source_latency"
    elif "no_causal_pre_entry_source" in tags:
        branch = "causal_source_coverage"

    row["primary_gap"] = primary
    row["action_branch"] = branch
    row["gap_tags"] = sorted(set(tags))


def compact_signal(signal: Mapping[str, Any] | None) -> str:
    if not signal:
        return ""
    return canonical_json(
        {
            "signal_id": signal.get("signal_id"),
            "instrument": signal.get("instrument"),
            "direction": signal.get("direction"),
            "family": signal.get("family"),
            "policy_state": signal.get("policy_state"),
            "signal_eligible": signal.get("signal_eligible"),
            "validated": signal.get("validated"),
            "direction_conflict": signal.get("direction_conflict"),
            "confidence": signal.get("confidence"),
            "projected_net_pips": signal.get("projected_net_pips"),
            "blocked_by": signal.get("blocked_by"),
        }
    )


def write_detail(path: Path, rows: Sequence[dict[str, Any]]) -> None:
    columns = [
        "inventory",
        "move_id",
        "factor_episode_id",
        "factor_representative",
        "factor_primary_token",
        "factor_assignment_contract_id",
        "factor_primary_method",
        "factor_strength_horizon_minutes",
        "factor_primary_scores_bps_json",
        "factor_primary_margin",
        "factor_primary_margin_unit",
        "factor_primary_ambiguous",
        "factor_strength_surface_status",
        "factor_strength_observation_count",
        "factor_strength_expected_observation_count",
        "factor_strength_coverage_pct",
        "factor_strength_as_of_utc",
        "factor_strength_as_of_age_sec",
        "factor_strength_oldest_pair_age_sec",
        "factor_strength_missing_instrument_count",
        "factor_strength_missing_currencies_json",
        "instrument",
        "start_utc",
        "end_utc",
        "horizon_min",
        "selected_side_label",
        "gross_magnitude_pips",
        "endpoint_after_cost_pips",
        "modeled_cost_pips",
        "liquidity_bucket",
        "after_cost_multiple",
        "primary_gap",
        "action_branch",
        "gap_tags_json",
        "top_signal_json",
        "top_signal_cycle_instrument",
        "independent_h1_model_count",
        "independent_h1_consensus_side",
        "independent_h1_consensus_correct",
        "prospective_forecast_json",
        "pre_entry_source_count",
        "pre_entry_story_count",
        "pre_entry_official_count",
        "pre_entry_directional_count",
        "in_window_source_count",
        "news_match_status",
        "source_evidence_class",
        "legacy_replay_status",
        "legacy_entry_observed_utc",
        "legacy_exit_observed_utc",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            h1 = row.get("independent_h1_summary") or {}
            cycle = row.get("top_signal_cycle") or {}
            writer.writerow(
                {
                    "inventory": row["inventory"],
                    "move_id": row["move_id"],
                    "factor_episode_id": row["factor_episode_id"],
                    "factor_representative": int(bool(row["factor_representative"])),
                    "factor_primary_token": row["factor_primary_token"],
                    "factor_assignment_contract_id": row.get(
                        "factor_assignment_contract_id", ""
                    ),
                    "factor_primary_method": row.get("factor_primary_method", ""),
                    "factor_strength_horizon_minutes": row.get(
                        "factor_strength_horizon_minutes"
                    ),
                    "factor_primary_scores_bps_json": canonical_json(
                        row.get("factor_primary_scores_bps") or {}
                    ),
                    "factor_primary_margin": row.get("factor_primary_margin"),
                    "factor_primary_margin_unit": row.get(
                        "factor_primary_margin_unit", ""
                    ),
                    "factor_primary_ambiguous": int(
                        bool(row.get("factor_primary_ambiguous"))
                    ),
                    "factor_strength_surface_status": row.get(
                        "factor_strength_surface_status", ""
                    ),
                    "factor_strength_observation_count": row.get(
                        "factor_strength_observation_count", 0
                    ),
                    "factor_strength_expected_observation_count": row.get(
                        "factor_strength_expected_observation_count", 0
                    ),
                    "factor_strength_coverage_pct": row.get(
                        "factor_strength_coverage_pct"
                    ),
                    "factor_strength_as_of_utc": row.get(
                        "factor_strength_as_of_utc", ""
                    ),
                    "factor_strength_as_of_age_sec": row.get(
                        "factor_strength_as_of_age_sec"
                    ),
                    "factor_strength_oldest_pair_age_sec": row.get(
                        "factor_strength_oldest_pair_age_sec"
                    ),
                    "factor_strength_missing_instrument_count": row.get(
                        "factor_strength_missing_instrument_count", 0
                    ),
                    "factor_strength_missing_currencies_json": canonical_json(
                        row.get("factor_strength_missing_currencies") or []
                    ),
                    "instrument": row["instrument"],
                    "start_utc": row["start_utc"],
                    "end_utc": row["end_utc"],
                    "horizon_min": row["horizon_min"],
                    "selected_side_label": row["selected_side_label"],
                    "gross_magnitude_pips": row.get("gross_magnitude_pips"),
                    "endpoint_after_cost_pips": row.get("endpoint_after_cost_pips"),
                    "modeled_cost_pips": row.get("modeled_cost_pips"),
                    "liquidity_bucket": row.get("liquidity_bucket", ""),
                    "after_cost_multiple": row.get("after_cost_multiple"),
                    "primary_gap": row["primary_gap"],
                    "action_branch": row["action_branch"],
                    "gap_tags_json": canonical_json(row["gap_tags"]),
                    "top_signal_json": compact_signal(row.get("top_signal_match")),
                    "top_signal_cycle_instrument": cycle.get("instrument", ""),
                    "independent_h1_model_count": h1.get("model_count", 0),
                    "independent_h1_consensus_side": h1.get("consensus_side", 0),
                    "independent_h1_consensus_correct": int(bool(h1.get("consensus_correct"))),
                    "prospective_forecast_json": canonical_json(row["prospective_forecast"]) if row.get("prospective_forecast") else "",
                    "pre_entry_source_count": row.get("pre_entry_source_count", 0),
                    "pre_entry_story_count": row.get("pre_entry_story_count", 0),
                    "pre_entry_official_count": row.get("pre_entry_official_count", 0),
                    "pre_entry_directional_count": row.get("pre_entry_directional_count", 0),
                    "in_window_source_count": row.get("in_window_source_count", 0),
                    "news_match_status": row.get("news_match_status", ""),
                    "source_evidence_class": row.get("source_evidence_class", ""),
                    "legacy_replay_status": row.get("legacy_replay_status", ""),
                    "legacy_entry_observed_utc": row.get("legacy_entry_observed_utc", ""),
                    "legacy_exit_observed_utc": row.get("legacy_exit_observed_utc", ""),
                }
            )
    os.replace(temporary, path)


def counter_rows(rows: Sequence[dict[str, Any]], key: str, representatives: bool = False) -> list[dict[str, Any]]:
    source = [row for row in rows if not representatives or row["factor_representative"]]
    counts = Counter(str(row.get(key) or "unknown") for row in source)
    return [{key: name, "count": count} for name, count in counts.most_common()]


def inventory_summary(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row["inventory"])].append(row)
    output = []
    for name, group in sorted(grouped.items()):
        exact = sum(bool(row.get("top_signal_match")) for row in group)
        output.append(
            {
                "inventory": name,
                "raw_moves": len(group),
                "factor_episodes": len({row["factor_episode_id"] for row in group}),
                "instruments": len({row["instrument"] for row in group}),
                "first_start_utc": min(row["start_utc"] for row in group),
                "last_start_utc": max(row["start_utc"] for row in group),
                "direction_scorable": sum(bool(row["selected_side"]) for row in group),
                "exact_top_signal_matches": exact,
                "exact_top_signal_match_rate": exact / len(group) if group else 0.0,
                "pre_entry_source_mapping_rate": sum(int(row.get("pre_entry_source_count") or 0) > 0 for row in group) / len(group),
                "pre_entry_directional_mapping_rate": sum(int(row.get("pre_entry_directional_count") or 0) > 0 for row in group) / len(group),
            }
        )
    return output


def render_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Major-Move Bot Coverage and Gap Census",
        "",
        f"Generated: `{payload['generated_utc']}`",
        "",
        "Research-only. Every retained major-move row is used, but unavailable historical forecasts are reported as evidence gaps rather than model losses. No result can promote or authorize execution.",
        "",
        f"- Raw move rows: **{payload['raw_move_rows']:,}**",
        f"- Deduplicated factor episodes: **{payload['factor_episode_count']:,}**",
        (
            "- Causal strength-assigned rows: "
            f"**{payload['factor_assignment']['causal_row_count']:,}**; "
            "fallback rows: "
            f"**{payload['factor_assignment']['fallback_row_count']:,}**"
        ),
        f"- Direction-scorable rows: **{payload['direction_scorable_rows']:,}**",
        f"- Exact top-signal matches: **{payload['exact_top_signal_matches']:,}**",
        f"- Confirmed execution candidates created: **0**",
        "",
        "## Inventory coverage",
        "",
        "| Inventory | Raw moves | Factor episodes | Instruments | Range | Direction scorable | Exact top-signal match | Pre-entry source | Directional source |",
        "|---|---:|---:|---:|---|---:|---:|---:|---:|",
    ]
    for row in payload["inventories"]:
        lines.append(
            f"| {row['inventory']} | {row['raw_moves']:,} | {row['factor_episodes']:,} | {row['instruments']:,} | "
            f"{row['first_start_utc'][:10]} to {row['last_start_utc'][:10]} | {row['direction_scorable']:,} | "
            f"{row['exact_top_signal_match_rate']:.1%} | {row['pre_entry_source_mapping_rate']:.1%} | "
            f"{row['pre_entry_directional_mapping_rate']:.1%} |"
        )
    lines += [
        "",
        "## Primary gaps (raw rows)",
        "",
        "| Gap | Rows |",
        "|---|---:|",
    ]
    for row in payload["primary_gaps_raw"][:30]:
        lines.append(f"| {row['primary_gap']} | {row['count']:,} |")
    lines += [
        "",
        "## Primary gaps (factor-episode representatives)",
        "",
        "| Gap | Independent episode representatives |",
        "|---|---:|",
    ]
    for row in payload["primary_gaps_effective"][:30]:
        lines.append(f"| {row['primary_gap']} | {row['count']:,} |")
    lines += [
        "",
        "## Highest liquid-major gaps after recorded bot history began",
        "",
        "| UTC | Pair | Horizon | Side | Net room (pips) | Gap | Branch |",
        "|---|---|---:|---|---:|---|---|",
    ]
    for row in payload["largest_scorable_gaps"]:
        lines.append(
            f"| {row['start_utc']} | {row['instrument']} | {row['horizon_min']}m | "
            f"{row['selected_side_label']} | {row['endpoint_after_cost_pips']:.3f} | "
            f"{row['primary_gap']} | {row['action_branch']} |"
        )
    lines += [
        "",
        "## Largest liquid-major moves preceding recorded bot history",
        "",
        "| UTC | Pair | Horizon | Side | Net room (pips) | Coverage gap |",
        "|---|---|---:|---|---:|---|",
    ]
    for row in payload["largest_uncovered_history"]:
        lines.append(
            f"| {row['start_utc']} | {row['instrument']} | {row['horizon_min']}m | "
            f"{row['selected_side_label']} | {row['endpoint_after_cost_pips']:.3f} | "
            f"{row['primary_gap']} |"
        )
    lines += [
        "",
        "## Gap-driven next work",
        "",
    ]
    raw_branches = {
        row["action_branch"]: int(row["count"])
        for row in payload["action_branches"]
    }
    for index, row in enumerate(payload["action_branches_effective"], 1):
        branch = row["action_branch"]
        lines.append(
            f"{index}. **{branch}** — {row['count']:,} independent factor-episode "
            f"representatives ({raw_branches.get(branch, 0):,} raw move rows)."
        )
    lines += [
        "",
        "## Interpretation boundaries",
        "",
        "- Legacy significant-move tags are coverage requirements. Their tag file does not retain actual move direction or executable magnitude, and most predate the bot's timestamped forecast history.",
        "- Canonical and prospective move labels are selected after the outcome. They locate failure modes; they do not prove a tradable entry rule.",
        "- A correct shadow direction is not a qualified trade. Costs, confidence, selection, multiplicity, untouched confirmation, and lifecycle authorization remain independent gates.",
        "- Factor representatives prevent simultaneous pairs expressing the same signed currency shock from inflating gap counts. Raw rows remain in the detail file.",
        "- When retained M1 bid/ask coverage is sufficient, `factor_primary_token` is selected from a synchronized 21-currency strength surface frozen at or before the move endpoint. Rows outside retained coverage use an explicitly labeled time-bucket recurrence fallback and remain approximate.",
        "- Raw pip magnitudes are not comparable across all 68 instruments. The headline rankings are restricted to major-currency pairs with modeled cost at or below five pips; wide-cost and exotic rows remain in the census and branch counts.",
        "- The complete historical candidate universe was not retained at every decision timestamp. A cross-sectional selection gap therefore identifies an instrumentation/replay need, not a known profitable rejected order.",
        "",
        f"Detail: `{payload['detail_csv']}`",
        "",
        "Execution decision remains `no_trade`.",
        "",
    ]
    return "\n".join(lines)


def run(
    *,
    config_path: Path = CONFIG,
    movement_database: Path = MOVEMENT_DB,
    movement_summary: Path = MOVEMENT_SUMMARY,
    legacy_tags: Path = LEGACY_TAGS,
    top_signal_database: Path = TOP_SIGNAL_DB,
    h1_database: Path = H1_FORECAST_DB,
    prospective_database: Path = PROSPECTIVE_DB,
    source_database: Path = SOURCE_DB,
    output_json: Path = OUTPUT_JSON,
    output_md: Path = OUTPUT_MD,
    output_csv: Path = OUTPUT_CSV,
) -> dict[str, Any]:
    config = read_json(config_path)
    generated = utc_now()
    canonical, canonical_meta = load_canonical_episodes(movement_database, movement_summary)
    legacy, legacy_meta = load_legacy_significant_tags(legacy_tags)
    configured_replay_root = Path(
        str(config.get("legacy_executable_replay_root") or LEGACY_CANDLE_ROOT)
    )
    if not configured_replay_root.is_absolute():
        configured_replay_root = ROOT / configured_replay_root
    canonical_cutoff = max((int(row["start_epoch"]) for row in canonical), default=0)
    tolerance = int(config.get("exact_target_tolerance_sec", 300))
    quantile = float(config.get("prospective_episode_quantile", 0.99))
    minimum_multiple = float(config.get("minimum_gross_cost_multiple", 2.0))

    h1_matches, post_h1, h1_meta = scan_h1_forecasts(
        h1_database,
        canonical,
        canonical_cutoff,
        tolerance,
        quantile,
        minimum_multiple,
    )
    for row in canonical:
        row["independent_h1_models"] = h1_matches.get(str(row["move_id"]), [])
    prospective, prospective_meta = load_prospective_major_moves(
        prospective_database, quantile, minimum_multiple
    )
    recent_rows = post_h1 + prospective
    news_meta = remap_recent_news(
        recent_rows,
        source_database,
        int(config.get("pre_entry_news_lookback_min", 360)),
    )
    rows = legacy + canonical + recent_rows
    factor_strength_surfaces: dict[tuple[int, int], dict[str, Any]] = {}
    factor_strength_meta: dict[str, Any] = {}
    legacy_replay_meta = recover_legacy_executable_paths(
        legacy,
        configured_replay_root,
        int(config.get("legacy_executable_replay_tolerance_sec", 300)),
        factor_rows=rows,
        factor_strength_surfaces=factor_strength_surfaces,
        factor_strength_meta=factor_strength_meta,
        factor_minimum_observations=int(
            config.get(
                "factor_strength_min_fresh_pairs",
                FACTOR_STRENGTH_MIN_OBSERVATIONS,
            )
        ),
        factor_expected_observations=int(
            config.get(
                "factor_strength_expected_pairs",
                FACTOR_STRENGTH_EXPECTED_OBSERVATIONS,
            )
        ),
        factor_maximum_pair_age_sec=int(
            config.get(
                "factor_strength_max_pair_age_sec",
                FACTOR_STRENGTH_MAX_PAIR_AGE_SEC,
            )
        ),
    )
    legacy_meta["executable_replay"] = legacy_replay_meta

    top_index, top_meta = load_top_signal_index(top_signal_database)
    for row in rows:
        assign_liquidity_bucket(row)
        match_top_signal(row, top_index, tolerance)
        apply_gap_classification(row, top_index, config)
    factor_count = assign_factor_episodes(
        rows, config, strength_surfaces=factor_strength_surfaces
    )

    rows.sort(key=lambda row: (int(row["start_epoch"]), row["instrument"], int(row["horizon_sec"]), row["inventory"]))
    prior_archives = preserve_pre_causal_factor_outputs(
        output_json, output_md, output_csv
    )
    write_detail(output_csv, rows)
    coverage_only_gaps = {
        "historical_direction_not_retained",
        "precedes_recorded_bot_history",
        "after_retained_top_signal_history",
    }
    largest = sorted(
        [
            row
            for row in rows
            if row["factor_representative"]
            and row["liquidity_bucket"] == "liquid_major"
            and row.get("endpoint_after_cost_pips") is not None
            and row["primary_gap"] not in {"selected_directional_catch_shadow", "frozen_gate_directional_catch"}
            and row["primary_gap"] not in coverage_only_gaps
        ],
        key=lambda row: -float(row["endpoint_after_cost_pips"]),
    )[: int(config.get("top_report_rows", 30))]
    uncovered = sorted(
        [
            row
            for row in rows
            if row["factor_representative"]
            and row["liquidity_bucket"] == "liquid_major"
            and row.get("endpoint_after_cost_pips") is not None
            and row["primary_gap"] in coverage_only_gaps
        ],
        key=lambda row: -float(row["endpoint_after_cost_pips"]),
    )[:15]
    causal_factor_rows = sum(
        row.get("factor_primary_method") == "causal_all68_currency_strength"
        for row in rows
    )
    fallback_factor_rows = len(rows) - causal_factor_rows
    payload = {
        "schema_version": 2,
        "generated_utc": generated,
        "research_id": str(config.get("research_id") or "major_move_gap_census_v1"),
        "factor_assignment_contract_id": FACTOR_ASSIGNMENT_CONTRACT_ID,
        "supersedes_factor_assignment_contract_id": (
            PREVIOUS_FACTOR_ASSIGNMENT_CONTRACT_ID
        ),
        "prior_output_archive": prior_archives,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "execution_decision": "no_trade",
        "raw_move_rows": len(rows),
        "factor_episode_count": factor_count,
        "factor_assignment": {
            **factor_strength_meta,
            "causal_row_count": causal_factor_rows,
            "fallback_row_count": fallback_factor_rows,
            "ambiguous_causal_row_count": sum(
                row.get("factor_primary_method")
                == "causal_all68_currency_strength"
                and bool(row.get("factor_primary_ambiguous"))
                for row in rows
            ),
            "method_counts": dict(
                Counter(str(row.get("factor_primary_method") or "unknown") for row in rows)
            ),
        },
        "direction_scorable_rows": sum(bool(row["selected_side"]) for row in rows),
        "exact_top_signal_matches": sum(bool(row.get("top_signal_match")) for row in rows),
        "inventories": inventory_summary(rows),
        "primary_gaps_raw": counter_rows(rows, "primary_gap"),
        "primary_gaps_effective": counter_rows(rows, "primary_gap", representatives=True),
        "action_branches": counter_rows(rows, "action_branch"),
        "action_branches_effective": counter_rows(
            rows, "action_branch", representatives=True
        ),
        "gap_tags_raw": [
            {"gap_tag": name, "count": count}
            for name, count in Counter(tag for row in rows for tag in row["gap_tags"]).most_common()
        ],
        "largest_scorable_gaps": [
            {
                "inventory": row["inventory"],
                "move_id": row["move_id"],
                "factor_episode_id": row["factor_episode_id"],
                "instrument": row["instrument"],
                "start_utc": row["start_utc"],
                "horizon_min": row["horizon_min"],
                "selected_side_label": row["selected_side_label"],
                "endpoint_after_cost_pips": float(row["endpoint_after_cost_pips"]),
                "primary_gap": row["primary_gap"],
                "action_branch": row["action_branch"],
                "gap_tags": row["gap_tags"],
            }
            for row in largest
        ],
        "largest_uncovered_history": [
            {
                "inventory": row["inventory"],
                "move_id": row["move_id"],
                "factor_episode_id": row["factor_episode_id"],
                "instrument": row["instrument"],
                "start_utc": row["start_utc"],
                "horizon_min": row["horizon_min"],
                "selected_side_label": row["selected_side_label"],
                "endpoint_after_cost_pips": float(row["endpoint_after_cost_pips"]),
                "primary_gap": row["primary_gap"],
            }
            for row in uncovered
        ],
        "inputs": {
            "canonical_movement": canonical_meta,
            "legacy_significant_tags": legacy_meta,
            "top_signal": top_meta,
            "independent_h1": h1_meta,
            "prospective_h5_h15": prospective_meta,
            "source_remap": news_meta,
        },
        "detail_csv": str(output_csv.resolve()),
        "limitations": [
            "retrospective move selection cannot establish a causal trading rule",
            "legacy tag rows do not retain direction or executable magnitude; only rows overlapping the hashed local BAM archive are retrospectively recovered",
            "timestamped top-signal history begins after most historical moves",
            "the full rejected candidate set was not retained for every historical decision",
            "source presence is context, not proof of causal direction",
            "factor rows outside retained synchronized M1 coverage use an explicit conservative time-bucket fallback",
            "raw pips are not comparable across all instruments; headline rankings use liquid major-currency pairs",
        ],
    }
    atomic_text(output_json, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    atomic_text(output_md, render_markdown(payload))
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=CONFIG)
    parser.add_argument("--movement-database", type=Path, default=MOVEMENT_DB)
    parser.add_argument("--movement-summary", type=Path, default=MOVEMENT_SUMMARY)
    parser.add_argument("--legacy-tags", type=Path, default=LEGACY_TAGS)
    parser.add_argument("--top-signal-database", type=Path, default=TOP_SIGNAL_DB)
    parser.add_argument("--h1-database", type=Path, default=H1_FORECAST_DB)
    parser.add_argument("--prospective-database", type=Path, default=PROSPECTIVE_DB)
    parser.add_argument("--source-database", type=Path, default=SOURCE_DB)
    parser.add_argument("--output-json", type=Path, default=OUTPUT_JSON)
    parser.add_argument("--output-md", type=Path, default=OUTPUT_MD)
    parser.add_argument("--output-csv", type=Path, default=OUTPUT_CSV)
    parser.add_argument("--interval-sec", type=float, default=0.0)
    parser.add_argument("--duration-sec", type=float, default=604800.0)
    args = parser.parse_args()
    stop = time.monotonic() + max(0.0, float(args.duration_sec))
    while True:
        payload = run(
            config_path=args.config,
            movement_database=args.movement_database,
            movement_summary=args.movement_summary,
            legacy_tags=args.legacy_tags,
            top_signal_database=args.top_signal_database,
            h1_database=args.h1_database,
            prospective_database=args.prospective_database,
            source_database=args.source_database,
            output_json=args.output_json,
            output_md=args.output_md,
            output_csv=args.output_csv,
        )
        print(
            json.dumps(
                {
                    key: payload[key]
                    for key in (
                        "generated_utc",
                        "raw_move_rows",
                        "factor_episode_count",
                        "exact_top_signal_matches",
                        "execution_decision",
                    )
                },
                indent=2,
            ),
            flush=True,
        )
        if float(args.interval_sec) <= 0.0 or time.monotonic() >= stop:
            return 0
        time.sleep(min(float(args.interval_sec), max(0.0, stop - time.monotonic())))


if __name__ == "__main__":
    raise SystemExit(main())
