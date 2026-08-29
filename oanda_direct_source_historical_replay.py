#!/usr/bin/env python3
"""Historical discovery replay for source-native official releases.

Uses source-reported timestamps and OANDA practice GET-only bid/ask candles.
Because many records were collected later and lack point-in-time consensus or
vintage guarantees, results are availability-counterfactual diagnostics only;
they can generate hypotheses but can never enter prospective proof cohorts.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
import sqlite3
import statistics
import urllib.parse
import urllib.request
from collections import defaultdict
from pathlib import Path
from typing import Any

from oanda_news_feed_backtest import readonly_price_token
from oanda_instrument_pips import resolve_pip_size

ROOT = Path(__file__).resolve().parent
STATE = ROOT / "data" / "oanda_training_manager" / "state"
MACRO = STATE / "macro_surprise_v1.sqlite"
CREDS = ROOT / "creds"
SOURCE_CONFIG = ROOT / "config" / "news_sources_v1.json"
CLOCK_OVERRIDES = ROOT / "config" / "official_historical_clock_overrides_v1.json"
OUTPUT = ROOT / "data" / "oanda_training_manager" / "reports" / "direct_source_response" / "DIRECT_SOURCE_HISTORICAL_REPLAY_CURRENT.json"
REPORT = ROOT / "data" / "oanda_training_manager" / "reports" / "direct_source_response" / "DIRECT_SOURCE_HISTORICAL_REPLAY_CURRENT.md"
HORIZONS = (60, 300, 900, 3600)
CONFIRMATION_DELAYS = (60, 120, 300, 600, 900)
CONFIRMATION_MINIMUM_FACTOR_BPS = (0.0, 1.0, 2.0, 4.0)
CONFIRMATION_MINIMUM_AGREEMENT = (0.5, 2.0 / 3.0, 0.8)
INSTRUMENTS = (
    "AUD_CAD", "AUD_CHF", "AUD_HKD", "AUD_JPY", "AUD_NZD", "AUD_SGD", "AUD_USD",
    "CAD_CHF", "CAD_HKD", "CAD_JPY", "CAD_SGD", "CHF_HKD", "CHF_JPY", "CHF_ZAR",
    "EUR_AUD", "EUR_CAD", "EUR_CHF", "EUR_CZK", "EUR_DKK", "EUR_GBP", "EUR_HKD",
    "EUR_HUF", "EUR_JPY", "EUR_NOK", "EUR_NZD", "EUR_PLN", "EUR_SEK", "EUR_SGD",
    "EUR_TRY", "EUR_USD", "EUR_ZAR", "GBP_AUD", "GBP_CAD", "GBP_CHF", "GBP_HKD",
    "GBP_JPY", "GBP_NZD", "GBP_PLN", "GBP_SGD", "GBP_USD", "GBP_ZAR", "HKD_JPY",
    "NZD_CAD", "NZD_CHF", "NZD_HKD", "NZD_JPY", "NZD_SGD", "NZD_USD", "SGD_CHF",
    "SGD_JPY", "TRY_JPY", "USD_CAD", "USD_CHF", "USD_CNH", "USD_CZK", "USD_DKK",
    "USD_HKD", "USD_HUF", "USD_JPY", "USD_MXN", "USD_NOK", "USD_PLN", "USD_SEK",
    "USD_SGD", "USD_THB", "USD_TRY", "USD_ZAR", "ZAR_JPY",
)
PAIR_MAP = {
    currency: tuple(
        instrument for instrument in INSTRUMENTS
        if currency in instrument.split("_")
    )
    for currency in sorted({leg for instrument in INSTRUMENTS for leg in instrument.split("_")})
}
CURRENCY_ALIASES = {"UK": "GBP", "RMB": "CNH", "CNY": "CNH"}


def parse_utc(value: Any) -> dt.datetime | None:
    if not value:
        return None
    try:
        return dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def iso(value: dt.datetime) -> str:
    return value.astimezone(dt.timezone.utc).isoformat()


def markdown_metric(value: Any, spec: str) -> str:
    return format(value, spec) if value is not None else "n/a"


def atomic(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def initial_numeric_with_latest_identity(
    rows: list[Any],
) -> list[tuple[Any, Any]]:
    """Pair the first observed payload with the latest corrected identity.

    Later revisions may repair currency/source classification, while the
    earliest observed actual/previous values remain the conservative numeric
    view for this discovery replay.  Using one row for both concerns either
    leaks later numeric revisions or freezes known classification defects.
    """
    first: dict[str, Any] = {}
    latest: dict[str, Any] = {}
    for row in rows:
        key = str(row["release_key"])
        first.setdefault(key, row)
        latest[key] = row
    return [(first[key], latest[key]) for key in first]


def source_clock(
    row: Any,
    identity_row: Any,
    contract: dict[str, Any] | None = None,
) -> tuple[dt.datetime | None, str]:
    """Return the best source-native historical clock without changing causality.

    Numeric values remain from the first immutable observation. A later
    provenance-only revision may repair the exact official publication clock,
    just as it may repair currency identity. This is used only by the labeled
    availability-counterfactual replay, never by prospective proof cohorts.
    """

    try:
        initial_payload = json.loads(row["payload_json"] or "{}")
    except (TypeError, ValueError, json.JSONDecodeError, KeyError):
        initial_payload = {}
    try:
        identity_payload = json.loads(identity_row["payload_json"] or "{}")
    except (TypeError, ValueError, json.JSONDecodeError, KeyError):
        identity_payload = {}
    stamp = parse_utc(identity_payload.get("source_native_published_utc"))
    if stamp is not None:
        return stamp, "latest_verified_source_native_publication"
    contract = contract or {}
    stamp = parse_utc(
        (contract.get("release_utc_by_reference") or {}).get(
            str(row["reference_period"] or "")
        )
    )
    if stamp is not None:
        return stamp, str(
            contract.get("clock_basis")
            or "frozen_verified_official_release_schedule"
        )
    if bool(initial_payload.get("published_time_inferred")):
        return None, "inferred_collection_clock_not_release_time"
    stamp_text = str(row["source_reported_update_utc"] or row["scheduled_utc"] or "")
    first_seen = str(initial_payload.get("first_seen_utc") or "")
    stamp = parse_utc(stamp_text)
    if stamp is not None and stamp_text != first_seen:
        return stamp, "initial_source_reported_or_scheduled"
    if stamp is not None:
        return None, "collection_clock_not_source_native"
    return None, "missing_source_reported_timestamp"


def official_series_contracts(path: Path) -> dict[str, dict[str, Any]]:
    """Load only verified/direct, source-frozen series identity and clocks."""

    try:
        configured = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}
    result: dict[str, dict[str, Any]] = {}
    for source in configured.get("sources") or []:
        if not isinstance(source, dict) or not source.get("verified") or not source.get("direct"):
            continue
        currencies = [str(value).upper() for value in source.get("currencies") or []]
        source_rows = list(source.get("series") or [])
        if source.get("event_series_id"):
            source_rows.append(source)
        for series in source_rows:
            if not isinstance(series, dict):
                continue
            series_id = str(series.get("series_id") or series.get("event_series_id") or "")
            if not series_id:
                continue
            release_map = {
                str(key): str(value)
                for key, value in (series.get("release_utc_by_reference") or {}).items()
                if parse_utc(value) is not None
            }
            candidate = {
                "currency": currencies[0] if len(currencies) == 1 else "",
                "release_utc_by_reference": release_map,
                "source_contract_id": str(source.get("source_contract_id") or ""),
                "clock_basis": str(
                    series.get("clock_basis")
                    or source.get("clock_basis")
                    or "frozen_verified_official_release_schedule"
                ),
                "clock_evidence_url": str(
                    series.get("clock_evidence_url")
                    or source.get("clock_evidence_url")
                    or ""
                ),
                "clock_evidence": str(
                    series.get("clock_evidence")
                    or source.get("clock_evidence")
                    or ""
                ),
            }
            prior = result.get(series_id)
            if prior and prior.get("currency") not in ("", candidate["currency"]):
                continue
            result[series_id] = candidate
    return result


def reviewed_historical_clock_overrides(
    path: Path, review_root: Path = ROOT
) -> dict[str, dict[str, Any]]:
    """Load narrow, archive-only clock repairs from separately reviewed bytes.

    These rows only repair source time for the explicitly labeled historical
    availability-counterfactual replay. They cannot alter the prospective
    ledger, create surprise or direction, or enter proof/execution paths.
    """

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    if payload.get("research_only") is not True or payload.get("execution_eligible") is not False:
        return {}
    if payload.get("evidence_class") != "historical_availability_counterfactual_clock_repair":
        return {}
    result: dict[str, dict[str, Any]] = {}
    for row in payload.get("series") or []:
        if not isinstance(row, dict):
            continue
        certificate_relative = str(row.get("source_review_certificate") or "")
        certificate_sha256 = str(row.get("source_review_certificate_sha256") or "").lower()
        try:
            root_resolved = review_root.resolve(strict=True)
            certificate = (root_resolved / certificate_relative).resolve(strict=True)
            certificate.relative_to(root_resolved)
            certificate_bytes = certificate.read_bytes()
            certificate_payload = json.loads(certificate_bytes.decode("utf-8"))
        except (OSError, ValueError, UnicodeDecodeError, json.JSONDecodeError):
            continue
        if (
            len(certificate_sha256) != 64
            or hashlib.sha256(certificate_bytes).hexdigest() != certificate_sha256
            or not isinstance(certificate_payload, dict)
            or certificate_payload.get("verdict")
            != "register_disabled_research_only_exact_bytes"
            or (certificate_payload.get("registration") or {}).get("enabled") is not False
            or (certificate_payload.get("registration") or {}).get("execution_eligible") is not False
        ):
            continue
        series_id = str(row.get("event_series_id") or "")
        currency = str(row.get("currency") or "").upper()
        release_map = {
            str(reference): str(stamp)
            for reference, stamp in (row.get("release_utc_by_reference") or {}).items()
            if parse_utc(stamp) is not None
        }
        if (
            not series_id
            or currency not in PAIR_MAP
            or not release_map
            or row.get("proof_eligible") is not False
            or row.get("execution_eligible") is not False
            or row.get("direction") is not None
            or row.get("consensus") is not None
            or row.get("surprise") is not None
        ):
            continue
        result[series_id] = {
            "currency": currency,
            "release_utc_by_reference": release_map,
            "source_contract_id": str(row.get("source_contract_id") or ""),
            "clock_basis": "reviewed_archive_exact_clock_counterfactual_only",
        }
    return result


def load_events(
    path: Path,
    source_config: Path = SOURCE_CONFIG,
    clock_overrides: Path = CLOCK_OVERRIDES,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    db = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    rows = db.execute(
        """SELECT * FROM macro_release_revisions WHERE actual_value IS NOT NULL
           AND source_verified=1 AND source_direct=1 ORDER BY row_id"""
    ).fetchall()
    db.close()
    contracts = official_series_contracts(source_config)
    for series_id, override in reviewed_historical_clock_overrides(clock_overrides).items():
        prior = contracts.get(series_id)
        if prior and prior.get("currency") not in ("", override["currency"]):
            continue
        merged = dict(prior or {})
        merged["currency"] = override["currency"]
        merged["source_contract_id"] = override["source_contract_id"]
        merged["clock_basis"] = override["clock_basis"]
        merged["release_utc_by_reference"] = {
            **dict(merged.get("release_utc_by_reference") or {}),
            **dict(override["release_utc_by_reference"]),
        }
        contracts[series_id] = merged
    valid, excluded = [], []
    valid_identities: set[tuple[str, str, str, str]] = set()
    excluded_identities: set[tuple[str, str, str, str]] = set()
    for row, identity_row in initial_numeric_with_latest_identity(rows):
        contract = contracts.get(str(row["event_series_id"])) or {}
        stamp, clock_basis = source_clock(row, identity_row, contract)
        try: currencies = json.loads(identity_row["currencies_json"] or "[]")
        except (TypeError, ValueError, json.JSONDecodeError): currencies = []
        reason = ""
        normalized_currency = (
            CURRENCY_ALIASES.get(str(currencies[0]).upper(), str(currencies[0]).upper())
            if len(currencies) == 1 else ""
        )
        currency_mapping_basis = "latest_immutable_release_identity" if normalized_currency else "unavailable"
        if not normalized_currency and contract.get("currency"):
            normalized_currency = str(contract["currency"])
            currency_mapping_basis = "frozen_verified_official_series_contract"
        if stamp is None: reason = clock_basis
        elif normalized_currency not in PAIR_MAP: reason = "unsupported_currency_mapping"
        payload = {"release_key": row["release_key"], "event_series_id": row["event_series_id"],
                   "event_name": row["event_name"], "currency": normalized_currency,
                   "source_time_utc": iso(stamp) if stamp else None, "collected_utc": row["causal_known_utc"],
                   "source_clock_basis": clock_basis,
                   "currency_mapping_basis": currency_mapping_basis,
                   "actual_value": row["actual_value"], "previous_value": row["previous_value"],
                   "revised_previous_value": row["revised_previous_value"], "consensus_value": row["consensus_value"],
                   "standardized_surprise": row["standardized_surprise"], "unit": row["unit"],
                   "reference_period": row["reference_period"], "source_id": row["source_id"],
                   "source_url": row["source_url"],
                   "source_contract_id": str(contract.get("source_contract_id") or ""),
                   "release_clock_evidence_url": str(contract.get("clock_evidence_url") or ""),
                   "release_clock_evidence": str(contract.get("clock_evidence") or ""),
                   "exclusion_reason": reason}
        if reason:
            exclusion_identity = (
                str(row["event_series_id"] or ""),
                str(row["reference_period"] or ""),
                repr(row["actual_value"]),
                reason,
            )
            if exclusion_identity in excluded_identities:
                continue
            excluded_identities.add(exclusion_identity)
            excluded.append(payload)
        else:
            valid_identity = (
                str(row["event_series_id"] or ""),
                str(row["reference_period"] or ""),
                repr(row["actual_value"]),
                str(payload["source_time_utc"] or ""),
            )
            if valid_identity in valid_identities:
                continue
            valid_identities.add(valid_identity)
            valid.append(payload)
    return valid, excluded


def fetch_candles(token: str, base_url: str, instrument: str, start: dt.datetime, end: dt.datetime) -> list[dict[str, Any]]:
    params = {"price":"BAM", "granularity":"M1", "from":iso(start).replace("+00:00","Z"),
              "to":iso(end).replace("+00:00","Z"), "smooth":"false"}
    request = urllib.request.Request(
        f"{base_url}/v3/instruments/{urllib.parse.quote(instrument)}/candles?{urllib.parse.urlencode(params)}",
        headers={"Authorization":f"Bearer {token}", "Accept":"application/json"}, method="GET")
    with urllib.request.urlopen(request, timeout=30) as response:
        raw = json.loads(response.read().decode("utf-8"))
    result=[]
    for candle in raw.get("candles") or []:
        stamp=parse_utc(candle.get("time")); bid=candle.get("bid") or {}; ask=candle.get("ask") or {}; mid=candle.get("mid") or {}
        if not stamp or not candle.get("complete",True): continue
        try:
            result.append({"time":stamp,"bid_o":float(bid["o"]),"bid_h":float(bid["h"]),"bid_l":float(bid["l"]),
                           "ask_o":float(ask["o"]),"ask_h":float(ask["h"]),"ask_l":float(ask["l"]),
                           "mid_o":float(mid["o"]),"mid_h":float(mid["h"]),"mid_l":float(mid["l"])})
        except (KeyError,TypeError,ValueError): continue
    return result


def evaluate_pair(event: dict[str,Any], instrument: str, candles: list[dict[str,Any]]) -> list[dict[str,Any]]:
    source_time=parse_utc(event["source_time_utc"]); currency=event["currency"]
    if source_time is None: return []
    entry=next((row for row in candles if row["time"]>=source_time),None)
    if entry is None: return []
    base,quote=instrument.split("_"); orientation=1 if base==currency else -1 if quote==currency else 0
    if not orientation: return []
    pip=resolve_pip_size(instrument)
    entry_mid=entry["mid_o"]; entry_spread=(entry["ask_o"]-entry["bid_o"])/pip
    confirmations = {
        delay: next(
            (
                row for row in candles
                if row["time"] >= entry["time"] + dt.timedelta(seconds=delay)
                and row["time"] <= entry["time"] + dt.timedelta(seconds=delay + 60)
            ),
            None,
        )
        for delay in CONFIRMATION_DELAYS
    }
    output=[]
    for horizon in HORIZONS:
        target=entry["time"]+dt.timedelta(seconds=horizon)
        exit_row=next((row for row in candles if row["time"]>=target),None)
        if exit_row is None or abs((exit_row["time"]-target).total_seconds())>60: continue
        window=[row for row in candles if entry["time"]<=row["time"]<=exit_row["time"]]
        currency_bps=orientation*(exit_row["mid_o"]/entry_mid-1)*10000
        currency_pips=orientation*(exit_row["mid_o"]-entry_mid)/pip
        if orientation==1:
            strengthening=(exit_row["bid_o"]-entry["ask_o"])/pip; weakening=(entry["bid_o"]-exit_row["ask_o"])/pip
            favorable=max((row["mid_h"]-entry_mid)/pip for row in window); adverse=min((row["mid_l"]-entry_mid)/pip for row in window)
        else:
            strengthening=(entry["bid_o"]-exit_row["ask_o"])/pip; weakening=(exit_row["bid_o"]-entry["ask_o"])/pip
            favorable=max((entry_mid-row["mid_l"])/pip for row in window); adverse=min((entry_mid-row["mid_h"])/pip for row in window)
        confirmation_paths: dict[str, dict[str, float]] = {}
        for delay, confirmation in confirmations.items():
            if confirmation is None or confirmation["time"] >= exit_row["time"]:
                continue
            confirmation_bps=orientation*(confirmation["mid_o"]/entry_mid-1)*10000
            confirmation_pips=orientation*(confirmation["mid_o"]-entry_mid)/pip
            confirmation_spread=(confirmation["ask_o"]-confirmation["bid_o"])/pip
            if orientation==1:
                post_strengthening=(exit_row["bid_o"]-confirmation["ask_o"])/pip
                post_weakening=(confirmation["bid_o"]-exit_row["ask_o"])/pip
            else:
                post_strengthening=(confirmation["bid_o"]-exit_row["ask_o"])/pip
                post_weakening=(exit_row["bid_o"]-confirmation["ask_o"])/pip
            confirmation_paths[str(delay)] = {
                "currency_return_bps": confirmation_bps,
                "currency_return_pips": confirmation_pips,
                "entry_spread_pips": confirmation_spread,
                "post_strengthening_after_cost_pips": post_strengthening,
                "post_weakening_after_cost_pips": post_weakening,
            }
        h1_confirmation = confirmation_paths.get("60") or {}
        output.append({"release_key":event["release_key"],"event_series_id":event["event_series_id"],"event_name":event["event_name"],
                       "source_time_utc":event["source_time_utc"],"currency":currency,"instrument":instrument,
                       "orientation":orientation,"horizon_sec":horizon,"entry_time":iso(entry["time"]),
                       "entry_spread_pips":entry_spread,"currency_return_bps":currency_bps,"currency_return_pips":currency_pips,
                       "absolute_move_pips":abs(currency_pips),"strengthening_after_cost_pips":strengthening,
                       "weakening_after_cost_pips":weakening,"best_after_cost_pips":max(strengthening,weakening),
                       "movement_cleared_cost":max(strengthening,weakening)>0,"max_favorable_pips":favorable,
                       "max_adverse_pips":adverse,
                       "confirmation_paths": confirmation_paths,
                       "h1_confirmation_currency_return_bps":h1_confirmation.get("currency_return_bps"),
                       "h1_confirmation_currency_return_pips":h1_confirmation.get("currency_return_pips"),
                       "h1_confirmation_entry_spread_pips":h1_confirmation.get("entry_spread_pips"),
                       "post_confirmation_strengthening_after_cost_pips":h1_confirmation.get("post_strengthening_after_cost_pips"),
                       "post_confirmation_weakening_after_cost_pips":h1_confirmation.get("post_weakening_after_cost_pips"),
                       "actual_value":event["actual_value"],"previous_value":event["previous_value"],
                       "consensus_value":event["consensus_value"],"availability_counterfactual":True,"proof_eligible":False})
    return output


def independent_episode_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Collapse same-currency/same-clock release bundles before inference."""
    grouped: dict[tuple[str, str, int], dict[str, dict[str, Any]]] = defaultdict(dict)
    for row in rows:
        key = (
            str(row.get("currency") or ""),
            str(row.get("source_time_utc") or ""),
            int(row.get("horizon_sec") or 0),
        )
        grouped[key].setdefault(str(row.get("instrument") or ""), row)
    output = []
    for (currency, source_time, horizon), by_pair in sorted(grouped.items()):
        pair_rows = list(by_pair.values())
        if not pair_rows:
            continue
        output.append({
            "currency": currency,
            "source_time_utc": source_time,
            "horizon_sec": horizon,
            "pair_count": len(pair_rows),
            "release_keys": sorted({str(row.get("release_key") or "") for row in rows
                                    if str(row.get("currency") or "") == currency
                                    and str(row.get("source_time_utc") or "") == source_time
                                    and int(row.get("horizon_sec") or 0) == horizon}),
            "median_currency_return_bps": statistics.median(
                row["currency_return_bps"] for row in pair_rows
            ),
            "median_absolute_currency_bps": statistics.median(
                abs(row["currency_return_bps"]) for row in pair_rows
            ),
            "median_absolute_move_pips": statistics.median(
                row["absolute_move_pips"] for row in pair_rows
            ),
            "median_best_after_cost_pips": statistics.median(
                row["best_after_cost_pips"] for row in pair_rows
            ),
            "cost_clear_pair_fraction": statistics.fmean(
                row["movement_cleared_cost"] for row in pair_rows
            ),
        })
    return output


def paired_magnitude_differences(
    event_rows: list[dict[str, Any]],
    control_rows: list[dict[str, Any]],
    *,
    control_offset_minutes: int = 120,
) -> list[dict[str, Any]]:
    """Return one paired event-minus-control magnitude row per factor clock."""
    control_by_key = {
        (
            str(row.get("currency") or ""),
            str(row.get("source_time_utc") or ""),
            int(row.get("horizon_sec") or 0),
        ): row
        for row in control_rows
    }
    paired = []
    for row in event_rows:
        event_time = parse_utc(row.get("source_time_utc"))
        if event_time is None:
            continue
        control_time = iso(event_time - dt.timedelta(minutes=control_offset_minutes))
        control = control_by_key.get(
            (
                str(row.get("currency") or ""),
                control_time,
                int(row.get("horizon_sec") or 0),
            )
        )
        if control is None:
            continue
        paired.append(
            {
                "currency": row.get("currency"),
                "source_time_utc": row.get("source_time_utc"),
                "horizon_sec": row.get("horizon_sec"),
                "event_absolute_pips": row.get("median_absolute_move_pips"),
                "control_absolute_pips": control.get("median_absolute_move_pips"),
                "event_minus_control_absolute_pips": (
                    float(row["median_absolute_move_pips"])
                    - float(control["median_absolute_move_pips"])
                ),
                "event_absolute_currency_bps": row.get(
                    "median_absolute_currency_bps"
                ),
                "control_absolute_currency_bps": control.get(
                    "median_absolute_currency_bps"
                ),
                "event_minus_control_absolute_currency_bps": (
                    float(row["median_absolute_currency_bps"])
                    - float(control["median_absolute_currency_bps"])
                ),
            }
        )
    return paired


def difference_interval(values: list[float]) -> dict[str, Any]:
    if not values:
        return {"n": 0, "mean": None, "median": None, "normal_95_lcb": None,
                "normal_95_ucb": None, "positive_fraction": None}
    mean = statistics.fmean(values)
    standard_error = (
        statistics.stdev(values) / math.sqrt(len(values))
        if len(values) >= 2 else None
    )
    return {
        "n": len(values),
        "mean": mean,
        "median": statistics.median(values),
        "normal_95_lcb": mean - 1.96 * standard_error if standard_error is not None else None,
        "normal_95_ucb": mean + 1.96 * standard_error if standard_error is not None else None,
        "positive_fraction": statistics.fmean(value > 0 for value in values),
    }


def episode_concentration(
    rows: list[dict[str, Any]],
    metric_key: str = "event_minus_control_absolute_pips",
) -> dict[str, Any]:
    """Describe whether one event dominates an event-control magnitude result."""

    if not rows:
        return {
            "largest_absolute_episode_pips": None,
            "largest_absolute_episode_currency": "",
            "largest_absolute_episode_time_utc": "",
            "largest_absolute_share": None,
            "mean_excluding_largest_absolute_episode": None,
        }
    ranked = sorted(
        rows,
        key=lambda row: abs(float(row[metric_key])),
        reverse=True,
    )
    largest = ranked[0]
    absolute_total = sum(
        abs(float(row[metric_key])) for row in ranked
    )
    remainder = [
        float(row[metric_key]) for row in ranked[1:]
    ]
    return {
        "largest_absolute_episode_pips": float(
            largest[metric_key]
        ),
        "largest_absolute_episode_currency": str(largest.get("currency") or ""),
        "largest_absolute_episode_time_utc": str(
            largest.get("source_time_utc") or ""
        ),
        "largest_absolute_share": (
            abs(float(largest[metric_key]))
            / absolute_total if absolute_total else None
        ),
        "mean_excluding_largest_absolute_episode": (
            statistics.fmean(remainder) if remainder else None
        ),
    }


def technical_confirmation_episode_rows(
    rows: list[dict[str, Any]],
    *,
    confirmation_delay_sec: int = 60,
    minimum_abs_factor_bps: float = 0.0,
    minimum_direction_agreement: float = 0.5,
    maximum_entry_spread_pips: float = 3.0,
) -> list[dict[str, Any]]:
    """Evaluate one frozen cross-pair confirmation rule per release clock."""
    grouped: dict[tuple[str, str, int], dict[str, dict[str, Any]]] = defaultdict(dict)
    for row in rows:
        path = (row.get("confirmation_paths") or {}).get(
            str(confirmation_delay_sec)
        )
        if path is None and confirmation_delay_sec == 60:
            path = {
                "currency_return_bps": row.get("h1_confirmation_currency_return_bps"),
                "entry_spread_pips": row.get("h1_confirmation_entry_spread_pips"),
                "post_strengthening_after_cost_pips": row.get(
                    "post_confirmation_strengthening_after_cost_pips"
                ),
                "post_weakening_after_cost_pips": row.get(
                    "post_confirmation_weakening_after_cost_pips"
                ),
            }
        if path is None or path.get("currency_return_bps") is None:
            continue
        row = {**row, "selected_confirmation_path": path}
        key = (
            str(row.get("currency") or ""),
            str(row.get("source_time_utc") or ""),
            int(row.get("horizon_sec") or 0),
        )
        grouped[key].setdefault(str(row.get("instrument") or ""), row)
    output=[]
    for (currency,source_time,horizon),by_pair in sorted(grouped.items()):
        pair_rows=list(by_pair.values())
        factor_bps=statistics.median(
            float(row["selected_confirmation_path"]["currency_return_bps"])
            for row in pair_rows
        )
        direction=(
            "strengthen" if factor_bps>0
            else "weaken" if factor_bps<0
            else "abstain"
        )
        chosen=min(
            pair_rows,
            key=lambda row:(
                (
                    float(row["selected_confirmation_path"]["entry_spread_pips"])
                    if row["selected_confirmation_path"].get("entry_spread_pips")
                    is not None else 1e9
                ),
                str(row.get("instrument") or ""),
            ),
        )
        chosen_path = chosen["selected_confirmation_path"]
        spread=(
            float(chosen_path["entry_spread_pips"])
            if chosen_path.get("entry_spread_pips") is not None else 1e9
        )
        agreement = statistics.fmean(
            (
                float(row["selected_confirmation_path"]["currency_return_bps"]) > 0
                if factor_bps > 0
                else float(row["selected_confirmation_path"]["currency_return_bps"]) < 0
            )
            for row in pair_rows
        ) if factor_bps != 0 else 0.0
        if (
            direction=="abstain"
            or abs(factor_bps) < minimum_abs_factor_bps
            or agreement < minimum_direction_agreement
            or spread>maximum_entry_spread_pips
        ):
            continue
        continuation=float(
            chosen_path["post_strengthening_after_cost_pips"]
            if direction=="strengthen"
            else chosen_path["post_weakening_after_cost_pips"]
        )
        reversal=float(
            chosen_path["post_weakening_after_cost_pips"]
            if direction=="strengthen"
            else chosen_path["post_strengthening_after_cost_pips"]
        )
        output.append({
            "currency":currency,
            "source_time_utc":source_time,
            "horizon_sec":horizon,
            "pair_count":len(pair_rows),
            "factor_confirmation_bps":factor_bps,
            "direction_agreement":agreement,
            "confirmation_delay_sec":confirmation_delay_sec,
            "minimum_abs_factor_bps":minimum_abs_factor_bps,
            "minimum_direction_agreement":minimum_direction_agreement,
            "predicted_currency_direction":direction,
            "instrument":chosen.get("instrument"),
            "entry_spread_pips":spread,
            "continuation_after_cost_pips":continuation,
            "reversal_after_cost_pips":reversal,
            "proof_eligible":False,
        })
    return output


def technical_confirmation_rule_sweep(
    event_rows: list[dict[str, Any]],
    control_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Evaluate a predeclared discovery grid without selecting a trade rule."""

    output = []
    for delay in CONFIRMATION_DELAYS:
        for threshold in CONFIRMATION_MINIMUM_FACTOR_BPS:
            for agreement in CONFIRMATION_MINIMUM_AGREEMENT:
                event = technical_confirmation_episode_rows(
                    event_rows,
                    confirmation_delay_sec=delay,
                    minimum_abs_factor_bps=threshold,
                    minimum_direction_agreement=agreement,
                )
                control = technical_confirmation_episode_rows(
                    control_rows,
                    confirmation_delay_sec=delay,
                    minimum_abs_factor_bps=threshold,
                    minimum_direction_agreement=agreement,
                )
                for horizon in HORIZONS:
                    if horizon <= delay:
                        continue
                    output.append({
                        "confirmation_delay_sec": delay,
                        "minimum_abs_factor_bps": threshold,
                        "minimum_direction_agreement": agreement,
                        "horizon_sec": horizon,
                        "event": technical_confirmation_summary([
                            row for row in event if row["horizon_sec"] == horizon
                        ]),
                        "control": technical_confirmation_summary([
                            row for row in control if row["horizon_sec"] == horizon
                        ]),
                        "proof_eligible": False,
                    })
    return output


def technical_confirmation_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    continuation=[float(row["continuation_after_cost_pips"]) for row in rows]
    reversal=[float(row["reversal_after_cost_pips"]) for row in rows]
    return {
        "continuation":difference_interval(continuation),
        "continuation_win_rate":(
            statistics.fmean(value>0 for value in continuation)
            if continuation else None
        ),
        "reversal":difference_interval(reversal),
        "reversal_win_rate":(
            statistics.fmean(value>0 for value in reversal)
            if reversal else None
        ),
        "proof_eligible":False,
    }


def run(macro:Path=MACRO,creds:Path=CREDS,output:Path=OUTPUT,report:Path=REPORT,source_config:Path=SOURCE_CONFIG,clock_overrides:Path=CLOCK_OVERRIDES)->dict[str,Any]:
    events,excluded=load_events(macro,source_config,clock_overrides); token,base_url=readonly_price_token(creds); rows=[]; control_rows=[]; errors=[]; candle_cache={}; candle_request_count=0; candle_cache_hits=0
    for event in events:
        stamp=parse_utc(event["source_time_utc"])
        for pair in PAIR_MAP[event["currency"]]:
            try:
                cache_key=(pair,stamp)
                if cache_key in candle_cache:
                    candles=candle_cache[cache_key]
                    candle_cache_hits+=1
                else:
                    candles=fetch_candles(token,base_url,pair,stamp-dt.timedelta(minutes=122),stamp+dt.timedelta(minutes=67))
                    candle_cache[cache_key]=candles
                    candle_request_count+=1
                rows.extend(evaluate_pair(event,pair,candles))
                control={**event,"release_key":"control|"+event["release_key"],"event_name":"matched non-event control",
                         "source_time_utc":iso(stamp-dt.timedelta(minutes=120))}
                control_rows.extend(evaluate_pair(control,pair,candles))
            except Exception as exc: errors.append({"release_key":event["release_key"],"instrument":pair,"error":f"{type(exc).__name__}: {exc}"})
    episode=[]
    groups=defaultdict(list)
    for row in rows: groups[(row["release_key"],row["horizon_sec"])].append(row)
    for (release,horizon),group in sorted(groups.items()):
        episode.append({"release_key":release,"event_series_id":group[0]["event_series_id"],"event_name":group[0]["event_name"],
                        "source_time_utc":group[0]["source_time_utc"],"horizon_sec":horizon,"pair_count":len(group),
                        "median_currency_return_bps":statistics.median(x["currency_return_bps"] for x in group),
                        "median_absolute_currency_bps":statistics.median(abs(x["currency_return_bps"]) for x in group),
                        "median_absolute_move_pips":statistics.median(x["absolute_move_pips"] for x in group),
                        "median_best_after_cost_pips":statistics.median(x["best_after_cost_pips"] for x in group),
                        "cost_clear_pair_fraction":statistics.fmean(x["movement_cleared_cost"] for x in group)})
    control_episode=[]; control_groups=defaultdict(list)
    for row in control_rows: control_groups[(row["release_key"],row["horizon_sec"])].append(row)
    for (release,horizon),group in sorted(control_groups.items()):
        control_episode.append({"release_key":release,"horizon_sec":horizon,
                                "median_absolute_move_pips":statistics.median(x["absolute_move_pips"] for x in group),
                                "median_oracle_best_after_cost_pips":statistics.median(x["best_after_cost_pips"] for x in group),
                                "cost_clear_pair_fraction":statistics.fmean(x["movement_cleared_cost"] for x in group)})
    independent_episode = independent_episode_rows(rows)
    independent_control_episode = independent_episode_rows(control_rows)
    paired_magnitude = paired_magnitude_differences(
        independent_episode,
        independent_control_episode,
    )
    technical_confirmation=technical_confirmation_episode_rows(rows)
    control_technical_confirmation=technical_confirmation_episode_rows(control_rows)
    technical_by_horizon={}
    for horizon in HORIZONS:
        if horizon<=60:
            continue
        technical_by_horizon[str(horizon)]={
            "event":technical_confirmation_summary([
                row for row in technical_confirmation
                if row["horizon_sec"]==horizon
            ]),
            "control":technical_confirmation_summary([
                row for row in control_technical_confirmation
                if row["horizon_sec"]==horizon
            ]),
        }
    technical_rule_sweep = technical_confirmation_rule_sweep(rows, control_rows)
    comparison=[]
    for horizon in HORIZONS:
        event_h=[x for x in independent_episode if x["horizon_sec"]==horizon]
        control_h=[x for x in independent_control_episode if x["horizon_sec"]==horizon]
        if event_h and control_h:
            event_abs=statistics.fmean(x["median_absolute_move_pips"] for x in event_h); control_abs=statistics.fmean(x["median_absolute_move_pips"] for x in control_h)
            event_abs_bps=statistics.fmean(x["median_absolute_currency_bps"] for x in event_h); control_abs_bps=statistics.fmean(x["median_absolute_currency_bps"] for x in control_h)
            event_oracle=statistics.fmean(x["median_best_after_cost_pips"] for x in event_h); control_oracle=statistics.fmean(x["median_best_after_cost_pips"] for x in control_h)
            paired_summary=difference_interval([
                float(x["event_minus_control_absolute_pips"])
                for x in paired_magnitude if x["horizon_sec"]==horizon
            ])
            concentration=episode_concentration([
                x for x in paired_magnitude if x["horizon_sec"]==horizon
            ], "event_minus_control_absolute_currency_bps")
            paired_bps_summary=difference_interval([
                float(x["event_minus_control_absolute_currency_bps"])
                for x in paired_magnitude if x["horizon_sec"]==horizon
            ])
            comparison.append({"horizon_sec":horizon,"event_n":len(event_h),"control_n":len(control_h),
                               "event_mean_absolute_pips":event_abs,"control_mean_absolute_pips":control_abs,
                               "event_minus_control_absolute_pips":event_abs-control_abs,
                               "event_mean_absolute_currency_bps":event_abs_bps,
                               "control_mean_absolute_currency_bps":control_abs_bps,
                               "event_minus_control_absolute_currency_bps":event_abs_bps-control_abs_bps,
                               "event_mean_oracle_best_after_cost_pips":event_oracle,"control_mean_oracle_best_after_cost_pips":control_oracle,
                               "event_minus_control_oracle_pips":event_oracle-control_oracle,
                               "event_cost_clear_fraction":statistics.fmean(x["cost_clear_pair_fraction"] for x in event_h),
                               "control_cost_clear_fraction":statistics.fmean(x["cost_clear_pair_fraction"] for x in control_h),
                               "paired_magnitude_difference":paired_summary,
                               "paired_currency_bps_difference":paired_bps_summary,
                               "episode_concentration":concentration})
    payload={"schema_version":1,"generated_utc":iso(dt.datetime.now(dt.timezone.utc)),"research_only":True,"execution_eligible":False,
             "orders_placed":0,"environment":"practice","endpoint":"GET instrument candles only","evidence_class":"availability_counterfactual_discovery",
             "proof_eligible":False,"limitations":["collection occurred after source time for most events","no causal pre-release consensus","historical source values may lack vintage guarantees","selection and evaluation use the same small sample"],
             "source_event_count":len(events),
             "candle_request_count":candle_request_count,
             "candle_cache_hits":candle_cache_hits,
             "candle_cache_entry_count":len(candle_cache),
             "independent_source_time_count":len({
                 (x["currency"], x["source_time_utc"]) for x in events
             }),
             "unique_source_timestamp_count":len({
                 x["source_time_utc"] for x in events
             }),
             "instrument_universe_count":len(INSTRUMENTS),"currency_universe_count":len(PAIR_MAP),
             "mapped_source_currencies":sorted({event["currency"] for event in events}),
             "excluded_source_events":excluded,"pair_horizon_rows":len(rows),"matched_control_pair_horizon_rows":len(control_rows),
             "episode_horizon_rows":episode,
             "independent_episode_horizon_rows":independent_episode,
             "independent_control_episode_horizon_rows":independent_control_episode,
             "paired_magnitude_difference_rows":paired_magnitude,
             "technical_confirmation_episode_rows":technical_confirmation,
             "matched_control_technical_confirmation_episode_rows":control_technical_confirmation,
             "technical_confirmation_by_horizon":technical_by_horizon,
             "technical_confirmation_rule_sweep":technical_rule_sweep,
             "matched_control_comparison":comparison,"errors":errors,"details":rows}
    atomic(output,json.dumps(payload,indent=2,sort_keys=True))
    lines=["# Direct-Source Historical Discovery Replay","",f"Generated: `{payload['generated_utc']}`","",
           "Availability-counterfactual research only. It cannot promote or execute.",
           "The paired 95% bounds are descriptive normal approximations on a selected small sample; they are not sequential, FDR-adjusted, or confirmation evidence.","",
           f"- Replayable official releases: **{len(events)}**",f"- Excluded releases: **{len(excluded)}**",
           f"- Independent currency-factor/source-time episodes: **{payload['independent_source_time_count']}**",
           f"- Unique publication timestamps: **{payload['unique_source_timestamp_count']}**",
           f"- Currency / instrument universe: **{len(PAIR_MAP)} / {len(INSTRUMENTS)}**",
           f"- Mapped source currencies: **{', '.join(payload['mapped_source_currencies']) or 'none'}**",
           f"- Pair/horizon outcomes: **{len(rows)}**",f"- Matched control outcomes: **{len(control_rows)}**",f"- Fetch errors: **{len(errors)}**","",
           "## Currency-factor event windows versus matched non-event controls","",
           "Cross-currency magnitude is aggregated in basis points. Raw pips are retained in JSON for executable pair-level diagnostics but are not comparable across currencies.","",
           "| Horizon | Paired N | Event absolute bps | Control absolute bps | Mean difference | Median difference | Mean ex-largest | Largest source | Event cost-clear | Control cost-clear |",
           "|---:|---:|---:|---:|---:|---:|---:|---|---:|---:|"]
    for row in comparison:
        paired=row["paired_currency_bps_difference"]
        concentration=row["episode_concentration"]
        largest = "none"
        if (
            concentration.get("largest_absolute_episode_pips") is not None
            and concentration.get("largest_absolute_share") is not None
        ):
            largest=(
                f"{concentration['largest_absolute_episode_currency']} "
                f"{concentration['largest_absolute_episode_pips']:+.2f} bps "
                f"({concentration['largest_absolute_share']:.1%})"
            )
        excluding = concentration.get("mean_excluding_largest_absolute_episode")
        excluding_text = f"{excluding:.3f}" if excluding is not None else "n/a"
        lines.append(f"| {row['horizon_sec']} | {paired['n']} | {row['event_mean_absolute_currency_bps']:.3f} | {row['control_mean_absolute_currency_bps']:.3f} | {paired['mean']:.3f} | {paired['median']:.3f} | {excluding_text} | {largest} | {row['event_cost_clear_fraction']:.1%} | {row['control_cost_clear_fraction']:.1%} |")
    lines += ["","## One-minute currency-factor confirmation","",
              "Direction is inferred only after one minute from the median cross-pair currency move; the selected pair is the lowest-spread eligible leg at confirmation. This is same-window discovery, not proof.","",
              "| Horizon | Event N | Event continuation | Event win | Control continuation | Control win | Event reversal |",
              "|---:|---:|---:|---:|---:|---:|---:|"]
    for horizon,summary in technical_by_horizon.items():
        event=summary["event"]; control=summary["control"]
        lines.append(
            f"| {horizon} | {event['continuation']['n']} | "
            f"{markdown_metric(event['continuation']['mean'], '.3f')} | "
            f"{markdown_metric(event['continuation_win_rate'], '.1%')} | "
            f"{markdown_metric(control['continuation']['mean'], '.3f')} | "
            f"{markdown_metric(control['continuation_win_rate'], '.1%')} | "
            f"{markdown_metric(event['reversal']['mean'], '.3f')} |"
        )
    ranked_rules = sorted(
        (
            row for row in technical_rule_sweep
            if row["event"]["continuation"]["n"] >= 8
        ),
        key=lambda row: row["event"]["continuation"]["mean"],
        reverse=True,
    )
    positive_rules = sum(
        row["event"]["continuation"]["mean"] > 0
        for row in technical_rule_sweep
        if row["event"]["continuation"]["n"] >= 8
    )
    lines += ["", "## Predeclared confirmation-delay and abstention sweep", "",
              "This is a multiple-tested discovery grid. The table shows the best observed rows only for diagnosis; selecting any row creates a new hypothesis that requires an untouched prospective cohort.", "",
              f"- Rule/horizon rows with N >= 8: **{sum(row['event']['continuation']['n'] >= 8 for row in technical_rule_sweep)}**",
              f"- Positive point-estimate rows among them: **{positive_rules}**", "",
              "| Delay | Min factor bps | Min agreement | Horizon | N | Event net | Win | Control net | Reversal net |",
              "|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for row in ranked_rules[:20]:
        event=row["event"]; control=row["control"]
        lines.append(
            f"| {row['confirmation_delay_sec']} | {row['minimum_abs_factor_bps']:.1f} | "
            f"{row['minimum_direction_agreement']:.1%} | {row['horizon_sec']} | "
            f"{event['continuation']['n']} | {event['continuation']['mean']:.3f} | "
            f"{event['continuation_win_rate']:.1%} | "
            f"{control['continuation']['mean']:.3f} | {event['reversal']['mean']:.3f} |"
        )
    lines += ["","`Best after cost` below is an oracle magnitude diagnostic, not a forecasted direction.","",
           "| Release | Time | Horizon | Pairs | Median currency bps | Median absolute pips | Median best after cost | Cost-clear pairs |",
           "|---|---|---:|---:|---:|---:|---:|---:|"]
    for row in episode:
        lines.append(f"| {row['event_name']} | {row['source_time_utc']} | {row['horizon_sec']} | {row['pair_count']} | {row['median_currency_return_bps']:.3f} | {row['median_absolute_move_pips']:.3f} | {row['median_best_after_cost_pips']:.3f} | {row['cost_clear_pair_fraction']:.1%} |")
    lines += ["","These results may generate frozen hypotheses only. They are not prospective confirmation.",""]
    atomic(report,"\n".join(lines)); return payload


def main()->int:
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument("--macro",type=Path,default=MACRO);parser.add_argument("--creds",type=Path,default=CREDS);parser.add_argument("--source-config",type=Path,default=SOURCE_CONFIG);parser.add_argument("--clock-overrides",type=Path,default=CLOCK_OVERRIDES);parser.add_argument("--output",type=Path,default=OUTPUT);parser.add_argument("--report",type=Path,default=REPORT);args=parser.parse_args();run(args.macro,args.creds,args.output,args.report,args.source_config,args.clock_overrides);return 0


if __name__=="__main__":raise SystemExit(main())
