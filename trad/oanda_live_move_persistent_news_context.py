#!/usr/bin/env python3
"""Attach still-active official/news factors to the current mover diagnostic.

The existing live mover join deliberately asks a narrow question: what was
known shortly before the move began?  Some policy/geopolitical records declare
longer research horizons, however, and disappearing them from the audit after
two hours hides mapping errors.  This companion view keeps those records
visible only for their already-declared relevance horizon.  It never changes
the narrow causal vote and has no broker, authorization, or promotion surface.
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import oanda_live_move_news_snapshot as live
import oanda_major_move_gap_census as census
import oanda_move_first_news_case_audit as news_audit


ROOT = Path(__file__).resolve().parent
STATE = ROOT / "data" / "oanda_training_manager" / "state"
REPORTS = ROOT / "data" / "oanda_training_manager" / "reports"
DEFAULT_SNAPSHOT = STATE / "live_move_news_snapshot_v1.json"
DEFAULT_SOURCES = STATE / "source_governance_v1.sqlite"
DEFAULT_OUTPUT = STATE / "live_move_persistent_news_context_v2.json"
DEFAULT_REPORT = REPORTS / "live_move_news" / "LIVE_MOVE_PERSISTENT_CONTEXT_V2.md"
CONTRACT_ID = "live_move_persistent_news_context_v2_direct_authority_20260825"
MAXIMUM_LOOKBACK_MINUTES = 1440


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def payload(event: Mapping[str, Any]) -> Mapping[str, Any]:
    try:
        value = json.loads(str(event.get("payload_json") or "{}"))
    except json.JSONDecodeError:
        return {}
    raw = value.get("raw_payload") if isinstance(value, Mapping) else None
    return raw if isinstance(raw, Mapping) else value if isinstance(value, Mapping) else {}


def declared_horizon_minutes(event: Mapping[str, Any]) -> int:
    candidate = payload(event)
    values: list[float] = []
    for key in (
        "relevance_window_minutes",
        "post_window_minutes",
        "estimated_reaction_horizon_minutes",
    ):
        try:
            number = float(candidate.get(key))
        except (TypeError, ValueError):
            continue
        if number > 0:
            values.append(number)
    return min(MAXIMUM_LOOKBACK_MINUTES, max(0, int(max(values, default=0))))


def active_persistent_events(
    events: Sequence[Mapping[str, Any]], *, entry_epoch: int
) -> list[dict[str, Any]]:
    """Keep only events whose frozen declared horizon reaches the entry."""

    output: list[dict[str, Any]] = []
    for source in events:
        event = dict(source)
        effective = census.parse_epoch(event.get("effective_from_utc"))
        if effective is None:
            effective = event.get("effective_epoch")
        try:
            effective_epoch = int(effective)
        except (TypeError, ValueError):
            continue
        horizon = declared_horizon_minutes(event)
        if not horizon or effective_epoch > entry_epoch:
            continue
        if effective_epoch + horizon * 60 < entry_epoch:
            continue
        valid_until = event.get("valid_until_epoch")
        superseded = event.get("superseded_epoch")
        if valid_until is not None and int(valid_until) <= entry_epoch:
            continue
        if superseded is not None and int(superseded) <= entry_epoch:
            continue
        event["declared_horizon_minutes"] = horizon
        event["minutes_before_entry"] = round((entry_epoch - effective_epoch) / 60.0, 3)
        output.append(event)
    return output


def alignment(side: int, actual: int) -> str:
    return "aligned" if side == actual else "opposed" if side == -actual else "neutral"


def direct_authority_relevant(
    event: Mapping[str, Any], base: str, quote: str
) -> bool:
    """Separate an authority's own currency leg from inferred spillovers."""

    candidate = payload(event)
    direct = {
        str(value).upper()
        for value in (
            candidate.get("direct_currencies")
            or candidate.get("source_currencies")
            or []
        )
        if str(value).strip()
    }
    return bool(direct.intersection({base.upper(), quote.upper()}))


def enrich_case(
    case: Mapping[str, Any], source_index: Mapping[str, Any]
) -> dict[str, Any]:
    result = dict(case)
    instrument = str(result.get("instrument") or "")
    if "_" not in instrument:
        return result
    base, quote = instrument.split("_", 1)
    entry = census.parse_epoch(result.get("start_utc"))
    actual = live.move_side(result.get("move_direction"))
    if entry is None or not actual:
        return result
    candidates = census.relevant_source_events(
        source_index,
        (base, quote),
        int(entry - MAXIMUM_LOOKBACK_MINUTES * 60),
        int(entry),
        True,
    )
    active = active_persistent_events(candidates, entry_epoch=int(entry))
    official_active = [
        event
        for event in active
        if str(event.get("source_population") or "").startswith("official")
    ]
    direct_authority_active = [
        event
        for event in official_active
        if direct_authority_relevant(event, base, quote)
    ]
    independent_story_count = len(
        {
            news_audit.independent_story_key(event)
            for event in active
            if news_audit.independent_story_key(event)
        }
    )
    broad = news_audit.directional_vote(active, base, quote, int(entry))
    official = news_audit.directional_vote(
        active, base, quote, int(entry), official_only=True
    )
    direct_authority = news_audit.directional_vote(
        direct_authority_active, base, quote, int(entry), official_only=True
    )
    broad_side = int(broad.get("side") or 0)
    official_side = int(official.get("side") or 0)
    direct_authority_side = int(direct_authority.get("side") or 0)
    result.update(
        {
            "persistent_active_event_count": len(active),
            "persistent_independent_story_count": independent_story_count,
            "persistent_research_side": (
                "long" if broad_side > 0 else "short" if broad_side < 0 else "neutral"
            ),
            "persistent_research_alignment": alignment(broad_side, actual),
            "persistent_official_side": (
                "long"
                if official_side > 0
                else "short"
                if official_side < 0
                else "neutral"
            ),
            "persistent_official_alignment": alignment(official_side, actual),
            "persistent_direct_authority_side": (
                "long"
                if direct_authority_side > 0
                else "short"
                if direct_authority_side < 0
                else "neutral"
            ),
            "persistent_direct_authority_alignment": alignment(
                direct_authority_side, actual
            ),
            "persistent_context_stories": live._context_stories(active, base, quote, 8),
            "persistent_official_stories": live._context_stories(
                official_active, base, quote, 8
            ),
            "persistent_direct_authority_stories": live._context_stories(
                direct_authority_active, base, quote, 8
            ),
        }
    )
    return result


def render(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Live mover persistent-news context",
        "",
        f"Generated: `{payload.get('generated_utc')}`",
        "",
        "Declared-horizon research context only; it does not alter the narrow causal vote.",
        "",
        "| Pair | Move | Raw events | Stories | Persistent side | Alignment | Official publisher | Alignment | Direct authority | Alignment |",
        "|---|---:|---:|---:|---:|---|---:|---|---:|---|",
    ]
    for row in payload.get("movers") or []:
        lines.append(
            f"| {row.get('instrument')} | {row.get('move_direction')} | "
            f"{row.get('persistent_active_event_count')} | "
            f"{row.get('persistent_independent_story_count')} | "
            f"{row.get('persistent_research_side')} | "
            f"{row.get('persistent_research_alignment')} | "
            f"{row.get('persistent_official_side')} | "
            f"{row.get('persistent_official_alignment')} | "
            f"{row.get('persistent_direct_authority_side')} | "
            f"{row.get('persistent_direct_authority_alignment')} |"
        )
    return "\n".join(lines) + "\n"


def run(
    *,
    snapshot_path: Path = DEFAULT_SNAPSHOT,
    source_database: Path = DEFAULT_SOURCES,
    output_path: Path = DEFAULT_OUTPUT,
    report_path: Path = DEFAULT_REPORT,
) -> dict[str, Any]:
    source = read_json(snapshot_path)
    movers = [row for row in source.get("movers") or [] if isinstance(row, Mapping)]
    starts = [census.parse_epoch(row.get("start_utc")) for row in movers]
    valid = [int(value) for value in starts if value is not None]
    lower = min(valid) - MAXIMUM_LOOKBACK_MINUTES * 60 if valid else int(time.time())
    upper = max(valid) if valid else int(time.time())
    source_index, highwater = census.load_source_index(
        source_database,
        minimum_effective_epoch=lower,
        maximum_effective_epoch=upper,
    )
    rows = [enrich_case(row, source_index) for row in movers]
    output = {
        "schema_version": 1,
        "contract_id": CONTRACT_ID,
        "generated_utc": utc_now(),
        "input_snapshot_contract_id": source.get("contract_id"),
        "input_snapshot_generated_utc": source.get("generated_utc"),
        "source_history_highwater_utc": highwater,
        "maximum_lookback_minutes": MAXIMUM_LOOKBACK_MINUTES,
        "mover_count": len(rows),
        "movers": rows,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_promote": False,
        "supported_decision": "diagnostic_only",
    }
    live.atomic_json(output_path, output)
    live.atomic_text(report_path, render(output))
    return output


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, default=DEFAULT_SNAPSHOT)
    parser.add_argument("--sources", type=Path, default=DEFAULT_SOURCES)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--interval-sec", type=float, default=0.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    args = parser.parse_args()
    started = time.time()
    while True:
        result = run(
            snapshot_path=args.snapshot,
            source_database=args.sources,
            output_path=args.output,
            report_path=args.report,
        )
        print(json.dumps({"generated_utc": result["generated_utc"], "mover_count": result["mover_count"]}), flush=True)
        if args.interval_sec <= 0:
            return 0
        if args.duration_sec > 0 and time.time() - started >= args.duration_sec:
            return 0
        time.sleep(max(1.0, args.interval_sec))


if __name__ == "__main__":
    raise SystemExit(main())
