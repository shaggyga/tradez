#!/usr/bin/env python3
"""Prospective research audit for the 2026-08-27 08:30 ET USD release bundle.

The event bundle contains the Census Advance Economic Indicators release and
the Department of Labor weekly unemployment-insurance claims report.  This
program records publisher transport changes, local knowledge-time records,
pre-event narrative/technical state, and later price responses.  It is
diagnostic only: it cannot create a signal, authorize an account, or trade.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import email.utils
import hashlib
import io
import json
import math
import re
import sqlite3
import statistics
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from pypdf import PdfReader

from oanda_instrument_pips import fallback_pip_size


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
STATE = DATA / "state"
NEWS = DATA / "local_news_sentiment"
REPORT_ROOT = DATA / "reports" / "week_to_date_event_move_audit" / "us_0830_release_live_case_v1"
PROBE_LOG = REPORT_ROOT / "US_0830_SOURCE_PROBES_V1.jsonl"
OUTPUT_JSON = REPORT_ROOT / "US_0830_RELEASE_CASE_V1.json"
OUTPUT_MD = REPORT_ROOT / "US_0830_RELEASE_CASE_V1.md"
OUTPUT_SHA = REPORT_ROOT / "US_0830_RELEASE_CASE_V1.sha256"

EVENT_UTC = dt.datetime(2026, 8, 27, 12, 30, tzinfo=dt.timezone.utc)
CUTOFF_UTC = dt.datetime(2026, 8, 27, 13, 0, tzinfo=dt.timezone.utc)
HORIZONS_MIN = (1, 5, 15, 30, 60)
CONTRACT_ID = "us_0830_release_live_case_v1_exact_knowledge_time_20260827"
USER_AGENT = (
    "ForexResearchNewsCollector/1.0 "
    "(causal research; bounded exact-clock official PDF probe)"
)
UTC = dt.timezone.utc


class AuditError(RuntimeError):
    """Raised when an audit condition could create misleading evidence."""


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def iso(value: dt.datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def parse_time(value: Any) -> dt.datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = email.utils.parsedate_to_datetime(text)
        except (TypeError, ValueError):
            return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(UTC)


def atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.{time.time_ns()}.tmp")
    # Keep artifact bytes identical to the bytes used by the SHA sidecar on
    # Windows.  Path.write_text's default newline translation would otherwise
    # turn LF into CRLF after the digest was calculated.
    temporary.write_text(text, encoding="utf-8", newline="\n")
    temporary.replace(path)


def append_jsonl(path: Path, row: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(canonical_json(dict(row)) + "\n")


def read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise AuditError(f"expected_json_object:{path}")
    return value


def fetch_bytes(url: str, accept: str) -> tuple[bytes, dict[str, Any]]:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": accept,
            "Accept-Encoding": "identity",
            "User-Agent": USER_AGENT,
        },
    )
    observed = dt.datetime.now(UTC)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = response.read(8_000_001)
            if len(payload) > 8_000_000:
                raise AuditError("publisher_payload_over_8mb")
            metadata = {
                "observed_utc": iso(observed),
                "status": int(getattr(response, "status", 200)),
                "final_url": str(response.geturl()),
                "content_type": str(response.headers.get("Content-Type") or ""),
                "last_modified": str(response.headers.get("Last-Modified") or ""),
                "etag": str(response.headers.get("ETag") or ""),
                "content_bytes": len(payload),
                "content_sha256": hashlib.sha256(payload).hexdigest(),
                "error": "",
            }
            return payload, metadata
    except urllib.error.HTTPError as exc:
        return b"", {
            "observed_utc": iso(observed),
            "status": int(exc.code),
            "final_url": url,
            "content_type": "",
            "last_modified": "",
            "etag": "",
            "content_bytes": 0,
            "content_sha256": "",
            "error": f"HTTP {exc.code}",
        }
    except OSError as exc:
        return b"", {
            "observed_utc": iso(observed),
            "status": 0,
            "final_url": url,
            "content_type": "",
            "last_modified": "",
            "etag": "",
            "content_bytes": 0,
            "content_sha256": "",
            "error": str(exc),
        }


def _clean_xml_text(value: Any) -> str:
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", str(value or ""))).strip()


def parse_census_rss(payload: bytes) -> list[dict[str, Any]]:
    if not payload:
        return []
    root = ET.fromstring(payload)
    rows: list[dict[str, Any]] = []
    for item in root.findall("./channel/item"):
        title = str(item.findtext("title") or "").strip()
        published = parse_time(item.findtext("pubDate"))
        if published is None:
            continue
        if abs((published - EVENT_UTC).total_seconds()) > 120:
            continue
        rows.append(
            {
                "title": title,
                "guid": str(item.findtext("guid") or "").strip(),
                "url": str(item.findtext("link") or "").strip(),
                "published_utc": iso(published),
                "description": _clean_xml_text(item.findtext("description")),
            }
        )
    return rows


def pdf_text(payload: bytes) -> str:
    if not payload.startswith(b"%PDF-"):
        return ""
    reader = PdfReader(io.BytesIO(payload), strict=True)
    return "\n".join(page.extract_text() or "" for page in reader.pages[:12])


def parse_dol_claims_text(text: str) -> dict[str, Any]:
    """Parse only the fixed opening paragraphs of an official DOL UI PDF.

    This parser remains independent of the separately allowlisted production
    parser so the audit can detect disagreements.  Neither path provides a
    directional mapping or execution authorization.
    """

    normalized = re.sub(r"\s+", " ", text).strip()
    release = re.search(
        r"EMBARGOED UNTIL\s+8:30 A\.M\.\s+\(Eastern\)\s+"
        r"(?P<weekday>\w+)\s*,?\s*(?P<date>[A-Z][a-z]+\s+\d{1,2},\s+20\d{2})",
        normalized,
        flags=re.I,
    )
    initial = re.search(
        r"week ending (?P<week>[A-Z][a-z]+\s+\d{1,2}),\s+the advance figure for "
        r"seasonally adjusted initial claims was (?P<actual>[\d,]+),\s+"
        r"a(?:n)?\s+(?P<change_word>increase|decrease) of (?P<change>[\d,]+)\s+"
        r"from the previous week's revised level\.\s+The previous week's level "
        r"was revised (?P<revision_word>up|down) by (?P<revision>[\d,]+) from "
        r"(?P<unrevised>[\d,]+) to (?P<revised>[\d,]+)",
        normalized,
        flags=re.I,
    )
    continued = re.search(
        r"advance number for seasonally adjusted insured unemployment during the "
        r"week ending (?P<week>[A-Z][a-z]+\s+\d{1,2}) was (?P<actual>[\d,]+),\s+"
        r"a(?:n)?\s+(?P<change_word>increase|decrease) of (?P<change>[\d,]+) from "
        r"the previous week's revised level\.\s+The previous week's level was "
        r"revised (?P<revision_word>up|down)(?: by)? (?P<revision>[\d,]+) from "
        r"(?P<unrevised>[\d,]+) to (?P<revised>[\d,]+)",
        normalized,
        flags=re.I,
    )
    insured_rate = re.search(
        r"advance seasonally adjusted insured unemployment rate was "
        r"(?P<actual>\d+(?:\.\d+)?) percent for the week ending "
        r"(?P<week>[A-Z][a-z]+\s+\d{1,2}),\s+"
        r"(?:(?P<unchanged>unchanged) from|a(?:n)?\s+"
        r"(?P<change_word>increase|decrease) of "
        r"(?P<change>\d+(?:\.\d+)?) percentage points? from) "
        r"the previous week's unrevised rate(?: of "
        r"(?P<previous>\d+(?:\.\d+)?) percent)?\.",
        normalized,
        flags=re.I,
    )
    if release is None or initial is None:
        return {}

    def integer(group: str, match: re.Match[str]) -> int:
        return int(match.group(group).replace(",", ""))

    result: dict[str, Any] = {
        "release_date_text": release.group("date"),
        "initial_claims": {
            "week_ending": initial.group("week"),
            "actual": integer("actual", initial),
            "previous_revised": integer("revised", initial),
            "previous_unrevised": integer("unrevised", initial),
            "change": (-1 if initial.group("change_word").lower() == "decrease" else 1)
            * integer("change", initial),
            "revision": (-1 if initial.group("revision_word").lower() == "down" else 1)
            * integer("revision", initial),
        },
        "consensus": None,
        "directional_interpretation": "unavailable_without_causal_consensus_and_rate_repricing",
        "parser_scope": "independent_audit_parser_not_shared_with_runtime",
    }
    if continued is not None:
        continued_result = {
            "week_ending": continued.group("week"),
            "actual": integer("actual", continued),
            "previous_revised": integer("revised", continued),
            "previous_unrevised": integer("unrevised", continued),
            "change": (-1 if continued.group("change_word").lower() == "decrease" else 1)
            * integer("change", continued),
            "revision": (-1 if continued.group("revision_word").lower() == "down" else 1)
            * integer("revision", continued),
        }
        if (
            continued_result["previous_revised"]
            - continued_result["previous_unrevised"]
            != continued_result["revision"]
            or continued_result["actual"]
            - continued_result["previous_revised"]
            != continued_result["change"]
        ):
            return {}
        result["continued_claims"] = continued_result
    if insured_rate is not None:
        rate_actual = float(insured_rate.group("actual"))
        if insured_rate.group("unchanged"):
            rate_change = 0.0
        else:
            magnitude = float(insured_rate.group("change"))
            rate_change = (
                -magnitude
                if insured_rate.group("change_word").lower() == "decrease"
                else magnitude
            )
        rate_previous = (
            float(insured_rate.group("previous"))
            if insured_rate.group("previous") is not None
            else rate_actual - rate_change
        )
        if abs((rate_actual - rate_previous) - rate_change) > 1e-9:
            return {}
        result["insured_unemployment_rate"] = {
            "week_ending": insured_rate.group("week"),
            "actual": rate_actual,
            "previous_unrevised": rate_previous,
            "change": rate_change,
        }
    return result


def compact_signal_snapshot(payload: Mapping[str, Any]) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for raw in payload.get("top_signals") or []:
        if not isinstance(raw, Mapping):
            continue
        instrument = str(raw.get("instrument") or "")
        if "USD" not in instrument.split("_"):
            continue
        breakdown: list[dict[str, Any]] = []
        for item in raw.get("horizon_breakdown") or []:
            if not isinstance(item, Mapping):
                continue
            horizon = int(item.get("horizon_sec") or -1)
            if horizon not in {60, 300, 900, 1800, 3600}:
                continue
            breakdown.append(
                {
                    "horizon_sec": horizon,
                    "direction": item.get("direction"),
                    "probability_up": item.get("probability_up"),
                    "raw_probability_up": item.get("raw_probability_up"),
                    "projected_net_pips": item.get("projected_net_pips"),
                    "signal_confidence": item.get("signal_confidence"),
                    "signal_eligible": bool(item.get("signal_eligible")),
                    "signal_blocked_by": list(item.get("signal_blocked_by") or []),
                    "best_model_id": item.get("best_model_id"),
                }
            )
        rows.append(
            {
                "instrument": instrument,
                "direction": raw.get("direction"),
                "preferred_horizon_sec": raw.get("preferred_horizon_sec"),
                "signal_confidence": raw.get("signal_confidence"),
                "signal_eligible": bool(raw.get("signal_eligible")),
                "projected_net_pips": raw.get("projected_net_pips"),
                "direction_conflict": bool(raw.get("direction_conflict")),
                "horizon_breakdown": breakdown,
            }
        )
    return {
        "updated_at": payload.get("updated_at"),
        "selected": payload.get("selected"),
        "qualified_signal_count": payload.get("qualified_signal_count"),
        "nonconflicting_qualified_signal_count": payload.get(
            "nonconflicting_qualified_signal_count"
        ),
        "usd_pair_rows": rows,
    }


def compact_local_snapshot(observed: dt.datetime) -> dict[str, Any]:
    def read(path: Path) -> dict[str, Any]:
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
            return value if isinstance(value, dict) else {}
        except (OSError, json.JSONDecodeError):
            return {}

    narrative = read(STATE / "continuous_narrative_meter_v12.json")
    sealed_currencies = narrative.get("currencies") or {}
    if not isinstance(sealed_currencies, Mapping):
        sealed_currencies = {}
    partial = narrative.get("partial_live") or {}
    partial_currencies = partial.get("currencies") or {} if isinstance(partial, Mapping) else {}
    if not isinstance(partial_currencies, Mapping):
        partial_currencies = {}
    preflight = read(STATE / "event_technical_preflight_v1.json")
    matching_events = [
        row
        for row in preflight.get("events") or []
        if isinstance(row, Mapping)
        and str(row.get("scheduled_utc") or "").replace("Z", "+00:00")
        in {EVENT_UTC.isoformat(), "2026-08-27T08:30:00-04:00"}
    ]
    signal_path = STATE / "practice_007_signal_snapshot_research_v1.json"
    signal = read(signal_path)
    return {
        "record_kind": "local_state_snapshot",
        "observed_utc": iso(observed),
        "narrative": {
            "meter_contract_id": narrative.get("meter_contract_id"),
            "sealed_clock_utc": narrative.get("sealed_clock_utc"),
            "generated_utc": narrative.get("generated_utc"),
            "integrity_status": narrative.get("integrity_status"),
            "sealed_usd": sealed_currencies.get("USD"),
            "partial_clock_utc": partial.get("clock_utc") if isinstance(partial, Mapping) else None,
            "partial_usd": partial_currencies.get("USD"),
        },
        "technical_preflight": {
            "contract_id": preflight.get("contract_id"),
            "generated_utc": preflight.get("generated_utc"),
            "matching_events": matching_events,
        },
        "signal_snapshot": compact_signal_snapshot(signal),
        "file_hashes": {
            str(path.name): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in (
                STATE / "continuous_narrative_meter_v12.json",
                STATE / "event_technical_preflight_v1.json",
                signal_path,
            )
            if path.exists()
        },
    }


def source_probe(source: str) -> dict[str, Any]:
    if source == "census":
        payload, metadata = fetch_bytes(
            "https://www.census.gov/economic-indicators/indicator.xml",
            "application/xml,text/xml;q=0.9,*/*;q=0.1",
        )
        return {
            "record_kind": "source_probe",
            "source": source,
            **metadata,
            "event_items": parse_census_rss(payload) if metadata["status"] == 200 else [],
        }
    if source == "dol":
        payload, metadata = fetch_bytes(
            "https://www.dol.gov/ui/data.pdf", "application/pdf"
        )
        text = pdf_text(payload) if metadata["status"] == 200 else ""
        return {
            "record_kind": "source_probe",
            "source": source,
            **metadata,
            "release": parse_dol_claims_text(text),
        }
    raise AuditError(f"unknown_source:{source}")


def material_probe_signature(row: Mapping[str, Any]) -> str:
    source = str(row.get("source") or "").strip()
    selected: dict[str, Any] = {
        "source": row.get("source"),
        "status": row.get("status"),
        "error": row.get("error"),
    }
    if source == "census":
        # The RSS transport has been observed alternating byte-identical-size
        # edge representations.  Exact target items, not whole-feed bytes,
        # define material release availability.
        selected["event_items"] = row.get("event_items")
    else:
        # DOL's fixed official PDF bytes are the versioned release object.
        # Parsed fields are deliberately excluded: a parser repair must not
        # relabel an unchanged publisher payload as a new material release.
        selected["content_sha256"] = row.get("content_sha256")
    return hashlib.sha256(canonical_json(selected).encode("utf-8")).hexdigest()


def recovered_probe_signatures(
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, str]:
    """Recover the last material comparison state without rewriting history."""

    signatures: dict[str, str] = {}
    for row in rows:
        if row.get("record_kind") != "source_probe":
            continue
        source = str(row.get("source") or "").strip()
        if source in {"census", "dol"}:
            signatures[source] = material_probe_signature(row)
    return signatures


def run_watch(*, stop_utc: dt.datetime = CUTOFF_UTC + dt.timedelta(minutes=1)) -> None:
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)
    signatures = recovered_probe_signatures(load_probe_log())
    last_snapshot_minute = ""
    while True:
        now = dt.datetime.now(UTC)
        for source in ("census", "dol"):
            row = source_probe(source)
            signature = material_probe_signature(row)
            if signatures.get(source) != signature:
                row["material_change"] = True
                append_jsonl(PROBE_LOG, row)
                signatures[source] = signature
        if EVENT_UTC - dt.timedelta(minutes=6) <= now <= stop_utc:
            minute = now.strftime("%Y-%m-%dT%H:%M")
            if minute != last_snapshot_minute:
                append_jsonl(PROBE_LOG, compact_local_snapshot(now))
                last_snapshot_minute = minute
        if now >= stop_utc:
            break
        seconds_to_event = (EVENT_UTC - now).total_seconds()
        delay = 60.0
        if -600 <= seconds_to_event <= 360:
            delay = 5.0
        elif seconds_to_event <= 900:
            delay = 15.0
        time.sleep(min(delay, max(1.0, (stop_utc - now).total_seconds())))


def load_candles(start: dt.datetime, end: dt.datetime) -> dict[str, list[dict[str, Any]]]:
    panel: dict[str, list[dict[str, Any]]] = {}
    for path in sorted((DATA / "candles").glob("*_M1.csv")):
        instrument = path.name.removesuffix("_M1.csv")
        rows: list[dict[str, Any]] = []
        with path.open("r", encoding="utf-8", newline="") as handle:
            for raw in csv.DictReader(handle):
                stamp = parse_time(raw.get("datetime") or raw.get("time"))
                if stamp is None or not start <= stamp < end:
                    continue
                try:
                    rows.append(
                        {
                            "timestamp": stamp,
                            "bid_open": float(raw["bid_open"]),
                            "ask_open": float(raw["ask_open"]),
                            "bid_close": float(raw["bid_close"]),
                            "ask_close": float(raw["ask_close"]),
                        }
                    )
                except (KeyError, TypeError, ValueError):
                    continue
        if rows:
            rows.sort(key=lambda row: row["timestamp"])
            panel[instrument] = rows
    return panel


def load_quote_history() -> tuple[dict[str, list[dict[str, Any]]], str | None]:
    """Load the executable quote-minute ledger used by the live mover view."""

    payload = read_json(DATA / "market_sentiment_ticker" / "quote_history.json")
    rows = payload.get("rows") or []
    event_epoch = EVENT_UTC.timestamp()
    cutoff_epoch = CUTOFF_UTC.timestamp()
    panel: dict[str, list[dict[str, Any]]] = {}
    for raw in rows:
        try:
            last_epoch = float(raw["last_epoch"])
            instrument = str(raw["instrument"])
            row = {
                "instrument": instrument,
                "last_epoch": last_epoch,
                "close_bid": float(raw["close_bid"]),
                "close_ask": float(raw["close_ask"]),
                "pip": float(raw.get("pip") or fallback_pip_size(instrument)),
            }
        except (KeyError, TypeError, ValueError):
            continue
        if event_epoch - 15 * 60 <= last_epoch < cutoff_epoch + 60:
            panel.setdefault(instrument, []).append(row)
    for instrument in panel:
        panel[instrument].sort(key=lambda row: float(row["last_epoch"]))
    return panel, payload.get("generated_utc")


def quote_endpoint_path(
    rows: Sequence[Mapping[str, Any]], horizon_min: int
) -> dict[str, Any] | None:
    """Measure exact boundaries with last executable quote and disclosed age."""

    start_epoch = EVENT_UTC.timestamp()
    end_epoch = (EVENT_UTC + dt.timedelta(minutes=horizon_min)).timestamp()
    entry = max(
        (row for row in rows if float(row["last_epoch"]) < start_epoch),
        key=lambda row: float(row["last_epoch"]),
        default=None,
    )
    exit_quote = max(
        (row for row in rows if float(row["last_epoch"]) < end_epoch),
        key=lambda row: float(row["last_epoch"]),
        default=None,
    )
    if entry is None or exit_quote is None:
        return None
    bid_open = float(entry["close_bid"])
    ask_open = float(entry["close_ask"])
    bid_close = float(exit_quote["close_bid"])
    ask_close = float(exit_quote["close_ask"])
    mid_open = (bid_open + ask_open) / 2.0
    mid_close = (bid_close + ask_close) / 2.0
    return {
        "start_utc": iso(EVENT_UTC),
        "end_utc": iso(EVENT_UTC + dt.timedelta(minutes=horizon_min)),
        "entry_quote_utc": iso(dt.datetime.fromtimestamp(float(entry["last_epoch"]), UTC)),
        "exit_quote_utc": iso(dt.datetime.fromtimestamp(float(exit_quote["last_epoch"]), UTC)),
        "entry_quote_age_sec": round(start_epoch - float(entry["last_epoch"]), 6),
        "exit_quote_age_sec": round(end_epoch - float(exit_quote["last_epoch"]), 6),
        "mid_open": mid_open,
        "mid_close": mid_close,
        "mid_return_bps": (mid_close / mid_open - 1.0) * 10_000.0,
        "bid_open": bid_open,
        "ask_open": ask_open,
        "bid_close": bid_close,
        "ask_close": ask_close,
        "pip": float(entry["pip"]),
    }


def quote_factor_response(
    panel: Mapping[str, Sequence[Mapping[str, Any]]], horizon: int
) -> dict[str, Any]:
    contributions: dict[str, list[float]] = {}
    paths: list[dict[str, Any]] = []
    for instrument, rows in panel.items():
        path = quote_endpoint_path(rows, horizon)
        if path is None:
            continue
        move = float(path["mid_return_bps"])
        base, quote = instrument.split("_", 1)
        contributions.setdefault(base, []).append(move)
        contributions.setdefault(quote, []).append(-move)
        pip = float(path["pip"])
        gross_pips = (float(path["mid_close"]) - float(path["mid_open"])) / pip
        long_net = (float(path["bid_close"]) - float(path["ask_open"])) / pip
        short_net = (float(path["bid_open"]) - float(path["ask_close"])) / pip
        observed_net = long_net if gross_pips > 0 else short_net if gross_pips < 0 else 0.0
        paths.append(
            {
                "instrument": instrument,
                "mid_return_bps": round(move, 5),
                "gross_pips": round(gross_pips, 4),
                "observed_direction": "up" if move > 0 else "down" if move < 0 else "flat",
                "observed_after_cost_pips": round(observed_net, 4),
                "entry_spread_pips": round((float(path["ask_open"]) - float(path["bid_open"])) / pip, 4),
                "entry_quote_age_sec": path["entry_quote_age_sec"],
                "exit_quote_age_sec": path["exit_quote_age_sec"],
            }
        )
    scores = {currency: statistics.median(values) for currency, values in contributions.items() if values}
    ranking = sorted(scores, key=lambda currency: scores[currency], reverse=True)
    usd_paths = [row for row in paths if "USD" in row["instrument"].split("_")]
    usd_paths.sort(key=lambda row: abs(float(row["mid_return_bps"])), reverse=True)
    usd = scores.get("USD")
    return {
        "horizon_min": horizon,
        "measurement": "executable_quote_endpoint_carry_forward_with_age",
        "matured": len(paths) == 68,
        "usable_pair_count": len(paths),
        "currency_count": len(scores),
        "usd_factor_score_bps": round(float(usd), 5) if usd is not None else None,
        "usd_factor_rank": ranking.index("USD") + 1 if "USD" in ranking else None,
        "currency_scores_bps": {key: round(value, 5) for key, value in sorted(scores.items())},
        "usd_pair_cost_clearing_count": sum(float(row["observed_after_cost_pips"]) > 0 for row in usd_paths),
        "usd_pair_count": len(usd_paths),
        "all_pair_cost_clearing_count": sum(float(row["observed_after_cost_pips"]) > 0 for row in paths),
        "all_pair_count": len(paths),
        "stale_entry_over_60s_count": sum(float(row["entry_quote_age_sec"]) > 60.0 for row in paths),
        "stale_exit_over_60s_count": sum(float(row["exit_quote_age_sec"]) > 60.0 for row in paths),
        "max_entry_quote_age_sec": round(max((float(row["entry_quote_age_sec"]) for row in paths), default=0.0), 6),
        "max_exit_quote_age_sec": round(max((float(row["exit_quote_age_sec"]) for row in paths), default=0.0), 6),
        "top_usd_pair_paths": usd_paths[:8],
    }


def exact_path(rows: Sequence[Mapping[str, Any]], horizon_min: int) -> dict[str, Any] | None:
    selected = [
        row
        for row in rows
        if EVENT_UTC <= row["timestamp"] < EVENT_UTC + dt.timedelta(minutes=horizon_min)
    ]
    if len(selected) != horizon_min or selected[0]["timestamp"] != EVENT_UTC:
        return None
    first, last = selected[0], selected[-1]
    bid_open, ask_open = float(first["bid_open"]), float(first["ask_open"])
    bid_close, ask_close = float(last["bid_close"]), float(last["ask_close"])
    mid_open = (bid_open + ask_open) / 2.0
    mid_close = (bid_close + ask_close) / 2.0
    return {
        "start_utc": iso(first["timestamp"]),
        "end_utc": iso(last["timestamp"] + dt.timedelta(minutes=1)),
        "mid_open": mid_open,
        "mid_close": mid_close,
        "mid_return_bps": (mid_close / mid_open - 1.0) * 10_000.0,
        "bid_open": bid_open,
        "ask_open": ask_open,
        "bid_close": bid_close,
        "ask_close": ask_close,
    }


def factor_response(panel: Mapping[str, Sequence[Mapping[str, Any]]], horizon: int) -> dict[str, Any]:
    contributions: dict[str, list[float]] = {}
    paths: list[dict[str, Any]] = []
    for instrument, rows in panel.items():
        path = exact_path(rows, horizon)
        if path is None:
            continue
        move = float(path["mid_return_bps"])
        base, quote = instrument.split("_", 1)
        contributions.setdefault(base, []).append(move)
        contributions.setdefault(quote, []).append(-move)
        pip = fallback_pip_size(instrument)
        gross_pips = (float(path["mid_close"]) - float(path["mid_open"])) / pip
        long_net = (float(path["bid_close"]) - float(path["ask_open"])) / pip
        short_net = (float(path["bid_open"]) - float(path["ask_close"])) / pip
        observed_net = long_net if gross_pips > 0 else short_net if gross_pips < 0 else 0.0
        paths.append(
            {
                "instrument": instrument,
                "mid_return_bps": round(move, 5),
                "gross_pips": round(gross_pips, 4),
                "observed_direction": "up" if move > 0 else "down" if move < 0 else "flat",
                "observed_after_cost_pips": round(observed_net, 4),
                "entry_spread_pips": round((float(path["ask_open"]) - float(path["bid_open"])) / pip, 4),
            }
        )
    scores = {
        currency: statistics.median(values)
        for currency, values in contributions.items()
        if values
    }
    ranking = sorted(scores, key=lambda currency: scores[currency], reverse=True)
    usd_paths = [row for row in paths if "USD" in row["instrument"].split("_")]
    usd_paths.sort(key=lambda row: abs(float(row["mid_return_bps"])), reverse=True)
    usd = scores.get("USD")
    return {
        "horizon_min": horizon,
        "matured": len(paths) == 68,
        "usable_pair_count": len(paths),
        "currency_count": len(scores),
        "usd_factor_score_bps": round(float(usd), 5) if usd is not None else None,
        "usd_factor_rank": ranking.index("USD") + 1 if "USD" in ranking else None,
        "currency_scores_bps": {key: round(value, 5) for key, value in sorted(scores.items())},
        "usd_pair_cost_clearing_count": sum(float(row["observed_after_cost_pips"]) > 0 for row in usd_paths),
        "usd_pair_count": len(usd_paths),
        "top_usd_pair_paths": usd_paths[:8],
    }


def pre_event_technicals(panel: Mapping[str, Sequence[Mapping[str, Any]]]) -> dict[str, Any]:
    def return_to_event(rows: Sequence[Mapping[str, Any]], minutes: int) -> float | None:
        selected = [
            row
            for row in rows
            if EVENT_UTC - dt.timedelta(minutes=minutes) <= row["timestamp"] < EVENT_UTC
        ]
        if len(selected) != minutes:
            return None
        start = (float(selected[0]["bid_open"]) + float(selected[0]["ask_open"])) / 2.0
        end = (float(selected[-1]["bid_close"]) + float(selected[-1]["ask_close"])) / 2.0
        return (end / start - 1.0) * 10_000.0

    windows: dict[str, Any] = {}
    usd_instruments = [instrument for instrument in panel if "USD" in instrument.split("_")]
    for minutes in (5, 15, 60):
        contributions: list[float] = []
        pair_rows: list[dict[str, Any]] = []
        for instrument in usd_instruments:
            move = return_to_event(panel[instrument], minutes)
            if move is None:
                continue
            base, _quote = instrument.split("_", 1)
            usd_move = move if base == "USD" else -move
            contributions.append(usd_move)
            pair_rows.append({"instrument": instrument, "usd_relative_bps": round(usd_move, 5)})
        median = statistics.median(contributions) if contributions else None
        windows[str(minutes)] = {
            "usable_usd_pairs": len(contributions),
            "usd_factor_median_bps": round(median, 5) if median is not None else None,
            "direction": "USD_UP" if median and median > 0 else "USD_DOWN" if median and median < 0 else "NEUTRAL",
            "pair_rows": sorted(pair_rows, key=lambda row: abs(float(row["usd_relative_bps"])), reverse=True),
        }
    return windows


def load_probe_log() -> list[dict[str, Any]]:
    if not PROBE_LOG.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in PROBE_LOG.read_text(encoding="utf-8").splitlines():
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            rows.append(value)
    return rows


def local_news_records() -> list[dict[str, Any]]:
    path = NEWS / "local_news_sentiment_v1.sqlite"
    connection = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """
            SELECT event_id,source_id,published_utc,first_seen_utc,last_seen_utc,
                   headline,summary,source_url,payload_json
              FROM articles
             WHERE (source_id IN ('census_economic_indicators',
                                  'dol_eta_ui_claims_official_search_v1',
                                  'dol_eta_ui_claims_scheduled_direct_pdf_v1')
                    OR lower(headline) LIKE '%unemployment insurance weekly claims%')
               AND published_utc >= '2026-08-27T12:20:00'
               AND published_utc <= '2026-08-27T13:30:00'
             ORDER BY first_seen_utc,event_id
            """
        ).fetchall()
    finally:
        connection.close()
    output: list[dict[str, Any]] = []
    for row in rows:
        payload = json.loads(str(row["payload_json"]))
        output.append(
            {
                "event_id": row["event_id"],
                "observation_surface": "main_semantic_collector",
                "source_id": row["source_id"],
                "published_utc": row["published_utc"],
                "first_seen_utc": row["first_seen_utc"],
                "last_seen_utc": row["last_seen_utc"],
                "headline": row["headline"],
                "summary": row["summary"],
                "source_url": row["source_url"],
                "actual": payload.get("actual"),
                "actual_value": payload.get("actual_value"),
                "previous": payload.get("previous"),
                "previous_value": payload.get("previous_value"),
                "revised_previous": payload.get("revised_previous"),
                "revised_previous_value": payload.get("revised_previous_value"),
                "consensus": payload.get("consensus"),
                "consensus_value": payload.get("consensus_value"),
                "consensus_capture_state": payload.get("consensus_capture_state"),
                "numeric_causal_known_utc": payload.get("numeric_causal_known_utc"),
                "detail_available_utc": payload.get("detail_available_utc"),
                "numeric_extraction_contract_id": payload.get("numeric_extraction_contract_id"),
                "event_series_id": payload.get("event_series_id"),
                "reference_period": payload.get("reference_period"),
                "embedded_release_utc": payload.get("embedded_release_utc"),
                "source_native_components": payload.get("source_native_components"),
                "release_components": payload.get("release_components"),
                "directional_publish_eligible": bool(payload.get("directional_publish_eligible")),
                "execution_eligible": bool(payload.get("execution_eligible")),
                "material_update_id": payload.get("material_update_id"),
                "material_content_sha256": payload.get("material_content_sha256"),
                "document_revision_number": payload.get("document_revision_number"),
                "is_material_revision": bool(payload.get("is_material_revision")),
                "supersedes_material_content_sha256": payload.get(
                    "supersedes_material_content_sha256"
                ),
                "revision_id": payload.get("revision_id"),
                "revised_at_utc": payload.get("revised_at_utc"),
                "raw_payload_hash": payload.get("raw_payload_hash"),
            }
        )
    return output


def fast_lane_records() -> list[dict[str, Any]]:
    path = NEWS / "official_release_fast_lane_v4.sqlite"
    if not path.exists():
        return []
    connection = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """
            SELECT observation_id,source_id,first_seen_utc,material_sha256,
                   raw_payload_json,prospective_observation,listing_bootstrap,
                   observation_clock_trusted,collector_contract_id
              FROM official_release_observation
             WHERE source_id='dol_eta_ui_claims_scheduled_direct_pdf_v1'
               AND first_seen_utc >= '2026-08-27T12:20:00'
               AND first_seen_utc <= '2026-08-27T13:30:00'
             ORDER BY first_seen_utc,observation_id
            """
        ).fetchall()
    finally:
        connection.close()
    output: list[dict[str, Any]] = []
    for row in rows:
        payload = json.loads(str(row["raw_payload_json"]))
        output.append(
            {
                "event_id": row["observation_id"],
                "observation_surface": "official_release_fast_lane_v4",
                "source_id": row["source_id"],
                "published_utc": payload.get("published_utc"),
                "first_seen_utc": row["first_seen_utc"],
                "last_seen_utc": row["first_seen_utc"],
                "headline": payload.get("title"),
                "summary": payload.get("summary"),
                "source_url": payload.get("url") or payload.get("detail_source_url"),
                "actual": payload.get("actual"),
                "actual_value": payload.get("actual_value"),
                "previous": payload.get("previous"),
                "previous_value": payload.get("previous_value"),
                "revised_previous": payload.get("revised_previous"),
                "revised_previous_value": payload.get("revised_previous_value"),
                "consensus": payload.get("consensus"),
                "consensus_value": payload.get("consensus_value"),
                "consensus_capture_state": payload.get("consensus_capture_state"),
                "numeric_causal_known_utc": payload.get("numeric_causal_known_utc"),
                "detail_available_utc": payload.get("detail_available_utc"),
                "numeric_extraction_contract_id": payload.get(
                    "numeric_extraction_contract_id"
                ),
                "event_series_id": payload.get("event_series_id"),
                "reference_period": payload.get("reference_period"),
                "embedded_release_utc": payload.get("embedded_release_utc"),
                "source_native_components": payload.get("source_native_components"),
                "release_components": payload.get("release_components"),
                "directional_publish_eligible": bool(
                    payload.get("directional_publish_eligible")
                ),
                "execution_eligible": False,
                "material_update_id": payload.get("material_update_id"),
                "material_content_sha256": payload.get("material_content_sha256")
                or row["material_sha256"],
                "document_revision_number": payload.get("document_revision_number"),
                "is_material_revision": bool(payload.get("is_material_revision")),
                "supersedes_material_content_sha256": payload.get(
                    "supersedes_material_content_sha256"
                ),
                "revision_id": payload.get("revision_id"),
                "revised_at_utc": payload.get("revised_at_utc"),
                "raw_payload_hash": payload.get("raw_payload_hash"),
                "prospective_observation": bool(row["prospective_observation"]),
                "listing_bootstrap": bool(row["listing_bootstrap"]),
                "observation_clock_trusted": bool(row["observation_clock_trusted"]),
                "collector_contract_id": row["collector_contract_id"],
            }
        )
    return output


def narrative_rows() -> list[dict[str, Any]]:
    path = STATE / "continuous_narrative_meter_v12.sqlite"
    connection = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """
            SELECT m.clock_utc,m.model_scores_json,m.attention_level,
                   m.attention_acceleration,m.new_story_count,m.active_story_count,
                   m.source_family_count,m.agreement,m.novelty,m.trusted_story_count,
                   m.forward_timely_story_count,m.source_families_json,m.story_ids_json,
                   m.classification_versions_json,m.evidence_class,m.created_utc,
                   s.sealed_at_utc,s.input_story_count
              FROM currency_meter AS m
              JOIN bucket_seals AS s
                ON s.meter_contract_id=m.meter_contract_id AND s.clock_utc=m.clock_utc
             WHERE m.currency='USD'
               AND m.clock_utc >= '2026-08-27T12:20:00'
               AND m.clock_utc <= '2026-08-27T13:00:00'
             ORDER BY m.clock_utc
            """
        ).fetchall()
    finally:
        connection.close()
    output: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        for key in (
            "model_scores_json",
            "source_families_json",
            "story_ids_json",
            "classification_versions_json",
        ):
            item[key.removesuffix("_json")] = json.loads(item.pop(key))
        output.append(item)
    return output


def latest_probe_versions(rows: Sequence[Mapping[str, Any]], source: str) -> list[dict[str, Any]]:
    selected = [dict(row) for row in rows if row.get("record_kind") == "source_probe" and row.get("source") == source]
    selected.sort(key=lambda row: str(row.get("observed_utc") or ""))
    return selected


def knowledge_clock_summary(
    census_versions: Sequence[Mapping[str, Any]],
    dol_versions: Sequence[Mapping[str, Any]],
    local_records: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Separate publisher availability, fast-lane, and broad-collector clocks."""

    def latency(value: Any) -> float | None:
        stamp = parse_time(value)
        if stamp is None:
            return None
        return round((stamp - EVENT_UTC).total_seconds(), 6)

    census_release = next(
        (
            dict(row)
            for row in census_versions
            if any(
                str(item.get("published_utc") or "").startswith("2026-08-27T12:30:00")
                for item in (row.get("event_items") or [])
            )
        ),
        None,
    )
    dol_release = next(
        (
            dict(row)
            for row in dol_versions
            if (row.get("release") or {}).get("release_date_text") == "August 27, 2026"
        ),
        None,
    )
    dol_complete_parse = next(
        (
            dict(row)
            for row in dol_versions
            if (row.get("release") or {}).get("release_date_text") == "August 27, 2026"
            and (row.get("release") or {}).get("continued_claims")
            and (row.get("release") or {}).get("insured_unemployment_rate")
        ),
        None,
    )

    def earliest(surface: str, source: str) -> dict[str, Any] | None:
        selected = [
            dict(row)
            for row in local_records
            if row.get("observation_surface") == surface and row.get("source_id") == source
        ]
        selected.sort(key=lambda row: str(row.get("first_seen_utc") or ""))
        return selected[0] if selected else None

    fast_dol = earliest(
        "official_release_fast_lane_v4", "dol_eta_ui_claims_scheduled_direct_pdf_v1"
    )
    broad_dol = earliest(
        "main_semantic_collector", "dol_eta_ui_claims_scheduled_direct_pdf_v1"
    )
    broad_census = earliest("main_semantic_collector", "census_economic_indicators")

    def local_clock(row: Mapping[str, Any] | None) -> dict[str, Any] | None:
        if row is None:
            return None
        first_seen = row.get("first_seen_utc")
        detail = row.get("detail_available_utc") or row.get("numeric_causal_known_utc")
        return {
            "detail_available_utc": detail,
            "detail_latency_sec": latency(detail),
            "first_seen_utc": first_seen,
            "first_seen_latency_sec": latency(first_seen),
        }

    return {
        "independent_official_probe": {
            "census": (
                {
                    "first_available_utc": census_release.get("observed_utc"),
                    "latency_sec": latency(census_release.get("observed_utc")),
                    "target_item_count": len(census_release.get("event_items") or []),
                    "content_sha256": census_release.get("content_sha256"),
                    "last_modified": census_release.get("last_modified"),
                }
                if census_release
                else None
            ),
            "dol": (
                {
                    "first_available_utc": dol_release.get("observed_utc"),
                    "latency_sec": latency(dol_release.get("observed_utc")),
                    "content_sha256": dol_release.get("content_sha256"),
                    "last_modified": dol_release.get("last_modified"),
                    "complete_audit_parse_utc": (
                        dol_complete_parse.get("observed_utc") if dol_complete_parse else None
                    ),
                    "complete_audit_parse_latency_sec": latency(
                        dol_complete_parse.get("observed_utc") if dol_complete_parse else None
                    ),
                    "parser_note": (
                        "source contained all three components at first availability; "
                        "the later complete audit view reflects an independent-parser repair, "
                        "not a publisher revision"
                    ),
                }
                if dol_release
                else None
            ),
        },
        "production": {
            "dol_fast_lane": local_clock(fast_dol),
            "dol_broad_collector": local_clock(broad_dol),
            "census_broad_collector": local_clock(broad_census),
            "census_fast_lane": None,
        },
    }


def horizon_unmatured_reason(horizon: int, generated: dt.datetime) -> str | None:
    if horizon == 60:
        return "60m_unavailable_at_0900_cutoff"
    if EVENT_UTC + dt.timedelta(minutes=horizon) > min(generated, CUTOFF_UTC):
        return "beyond_report_cutoff"
    return None


def compile_report(now: dt.datetime | None = None) -> dict[str, Any]:
    generated = now or dt.datetime.now(UTC)
    probe_rows = load_probe_log()
    panel = load_candles(EVENT_UTC - dt.timedelta(minutes=65), CUTOFF_UTC + dt.timedelta(minutes=1))
    quote_panel, quote_history_generated_utc = load_quote_history()
    responses = []
    candle_crosscheck = []
    for horizon in HORIZONS_MIN:
        reason = horizon_unmatured_reason(horizon, generated)
        if reason:
            responses.append({"horizon_min": horizon, "matured": False, "reason": reason})
            candle_crosscheck.append({"horizon_min": horizon, "matured": False, "reason": reason})
        else:
            responses.append(quote_factor_response(quote_panel, horizon))
            candle_crosscheck.append(factor_response(panel, horizon))
    census_versions = latest_probe_versions(probe_rows, "census")
    dol_versions = latest_probe_versions(probe_rows, "dol")
    local_snapshots = [
        dict(row) for row in probe_rows if row.get("record_kind") == "local_state_snapshot"
    ]
    local_records = local_news_records() + fast_lane_records()
    local_records.sort(
        key=lambda row: (str(row.get("first_seen_utc") or ""), str(row.get("event_id") or ""))
    )
    payload: dict[str, Any] = {
        "schema_version": 1,
        "contract_id": CONTRACT_ID,
        "generated_utc": iso(generated),
        "event_utc": iso(EVENT_UTC),
        "cutoff_utc": iso(CUTOFF_UTC),
        "event_bundle": [
            {
                "publisher": "U.S. Census Bureau",
                "event": "Advance Economic Indicators (trade, wholesale inventories, retail inventories)",
                "official_url": "https://www.census.gov/economic-indicators/indicator.xml",
            },
            {
                "publisher": "U.S. Department of Labor",
                "event": "Unemployment Insurance Weekly Claims",
                "official_url": "https://www.dol.gov/ui/data.pdf",
            },
        ],
        "attribution_policy": {
            "one_factor_episode": True,
            "reason": "simultaneous_USD_releases_cannot_be_separated_from_this_single_clock",
            "causal_consensus_available": any(
                row.get("consensus_value") is not None for row in local_records
            ),
            "directional_forecast_claimed": False,
            "calendar_is_not_direction": True,
            "late_or_public_consensus_rejected": True,
            "research_only": True,
            "execution_eligible": False,
        },
        "official_source_probes": {
            "census_versions": census_versions,
            "dol_versions": dol_versions,
        },
        "knowledge_clock_summary": knowledge_clock_summary(
            census_versions, dol_versions, local_records
        ),
        "local_knowledge_time_records": local_records,
        "local_state_snapshots": local_snapshots,
        "v12_usd_narrative_timeline": narrative_rows(),
        "pre_event_technical_context": pre_event_technicals(panel),
        "all_68_factor_response": responses,
        "completed_m1_candle_crosscheck": candle_crosscheck,
        "integrity": {
            "priced_pair_count": len(panel),
            "quote_history_pair_count": len(quote_panel),
            "quote_history_generated_utc": quote_history_generated_utc,
            "expected_pair_count": 68,
            "exact_68_pair_panel": len(quote_panel) == 68,
            "60m_explicitly_unavailable_at_cutoff": True,
            "orders_placed": 0,
            "orders_closed": 0,
            "execution_enabled_by_this_audit": False,
        },
        "research_only": True,
        "execution_eligible": False,
    }
    payload["report_sha256"] = hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()
    return payload


def render_markdown(payload: Mapping[str, Any]) -> str:
    records = payload["local_knowledge_time_records"]
    responses = payload["all_68_factor_response"]
    lines = [
        "# U.S. 08:30 ET Release Bundle — Prospective Case Audit",
        "",
        f"Generated `{payload['generated_utc']}`; evidence cutoff `{payload['cutoff_utc']}`. Research-only; no order or authorization path.",
        "",
        "## Evidence disposition",
        "",
        "Census Advance Economic Indicators and DOL weekly claims share one 08:30 ET clock, so this report treats the response as one USD factor episode. It does not post-hoc assign the move to one release. No causal pre-release consensus means no directional forecast or win/loss claim.",
        "",
        "## Local release capture",
        "",
        "| Surface | Source | Published | First seen | Series | Actual | Previous/revised | Consensus | Directional? |",
        "|---|---|---|---|---|---:|---:|---:|---|",
    ]
    for row in records:
        previous = row.get("revised_previous_value")
        if previous is None:
            previous = row.get("previous_value")
        lines.append(
            f"| {row.get('observation_surface')} | {row.get('source_id')} | "
            f"{row.get('published_utc')} | {row.get('first_seen_utc')} | "
            f"{row.get('event_series_id') or row.get('headline')} | {row.get('actual_value')} | "
            f"{previous} | {row.get('consensus_value')} | {row.get('directional_publish_eligible')} |"
        )
    if not records:
        lines.append("| — | — | — | — | No local release row by cutoff | — | — | — | False |")
    lines += [
        "",
        "## Knowledge-time clocks",
        "",
    ]
    clocks = payload["knowledge_clock_summary"]
    official = clocks["independent_official_probe"]
    production = clocks["production"]
    for label, row in (
        ("Independent Census official probe", official.get("census")),
        ("Independent DOL official probe", official.get("dol")),
        ("DOL production fast lane", production.get("dol_fast_lane")),
        ("DOL broad semantic collector", production.get("dol_broad_collector")),
        ("Census broad semantic collector", production.get("census_broad_collector")),
    ):
        if not row:
            lines.append(f"- {label}: unavailable by compilation time.")
            continue
        observed = row.get("first_available_utc") or row.get("first_seen_utc")
        delay = row.get("latency_sec")
        if delay is None:
            delay = row.get("first_seen_latency_sec")
        detail = row.get("detail_available_utc")
        suffix = f"; detail available `{detail}`" if detail else ""
        lines.append(f"- {label}: `{observed}` (`+{delay}` seconds){suffix}.")
    lines += [
        "",
        "## All-68 USD response",
        "",
        "| Horizon | Matured | Pair coverage | USD score | USD rank | All / USD pairs beating cost | Stale entry/exit >60s |",
        "|---:|---|---:|---:|---:|---:|---:|",
    ]
    for row in responses:
        lines.append(
            f"| {row.get('horizon_min')}m | {row.get('matured')} | {row.get('usable_pair_count', 0)}/68 | "
            f"{row.get('usd_factor_score_bps')} bps | {row.get('usd_factor_rank')} | "
            f"{row.get('all_pair_cost_clearing_count', 0)}/{row.get('all_pair_count', 0)} / "
            f"{row.get('usd_pair_cost_clearing_count', 0)}/{row.get('usd_pair_count', 0)} | "
            f"{row.get('stale_entry_over_60s_count', 0)}/{row.get('stale_exit_over_60s_count', 0)} |"
        )
    lines += [
        "",
        "The 60-minute response is deliberately unavailable at the 09:00 ET cutoff; it is not approximated from a partial window.",
        "",
        "## Pre-event state",
        "",
    ]
    for minutes, row in payload["pre_event_technical_context"].items():
        lines.append(
            f"- {minutes}m USD factor momentum: `{row.get('usd_factor_median_bps')}` bps, `{row.get('direction')}`, coverage `{row.get('usable_usd_pairs')}/20`."
        )
    timeline = payload["v12_usd_narrative_timeline"]
    if timeline:
        pre = [row for row in timeline if str(row.get("clock_utc")) <= "2026-08-27T12:30:00"]
        latest_pre = pre[-1] if pre else timeline[0]
        lines.append(
            f"- V12 latest sealed pre-event USD bucket `{latest_pre.get('clock_utc')}`: models `{latest_pre.get('model_scores')}`, attention `{latest_pre.get('attention_level')}`, acceleration `{latest_pre.get('attention_acceleration')}`."
        )
    else:
        lines.append("- V12 sealed USD rows were unavailable at compilation time.")
    lines += [
        "",
        "## Guardrails",
        "",
        "- Calendar presence was not interpreted as direction.",
        "- The allowlisted DOL direct-PDF parser emits research-only records and cannot authorize execution.",
        "- Post-release/public forecasts were not imported as causal consensus.",
        "- No discretionary order, close, gate change, promotion, or real-money route occurred.",
        "",
        f"Artifact hash: `{payload['report_sha256']}`.",
        "",
    ]
    return "\n".join(lines)


def write_report(payload: Mapping[str, Any]) -> None:
    text = json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    markdown = render_markdown(payload)
    atomic_write(OUTPUT_JSON, text)
    atomic_write(OUTPUT_MD, markdown)
    sha_lines = [
        f"{hashlib.sha256(text.encode('utf-8')).hexdigest()}  {OUTPUT_JSON.name}",
        f"{hashlib.sha256(markdown.encode('utf-8')).hexdigest()}  {OUTPUT_MD.name}",
    ]
    atomic_write(OUTPUT_SHA, "\n".join(sha_lines) + "\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--watch", action="store_true", help="poll official sources through the cutoff")
    parser.add_argument("--compile", action="store_true", help="compile current partial/final artifacts")
    parser.add_argument("--probe-once", action="store_true", help="append one source and local-state probe")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not any((args.watch, args.compile, args.probe_once)):
        args.compile = True
    if args.probe_once:
        for source in ("census", "dol"):
            append_jsonl(PROBE_LOG, source_probe(source))
        append_jsonl(PROBE_LOG, compact_local_snapshot(dt.datetime.now(UTC)))
    if args.watch:
        run_watch()
    if args.compile or args.watch:
        payload = compile_report()
        write_report(payload)
        print(OUTPUT_MD)
        print(OUTPUT_JSON)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
