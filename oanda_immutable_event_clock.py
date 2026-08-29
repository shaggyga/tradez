#!/usr/bin/env python3
"""Append or reconstruct the research-only immutable event-clock ledger."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
from pathlib import Path

try:
    from forex_system.ingestion.immutable_event_clock import (
        DEFAULT_MAXIMUM_CLOCK_ATTESTATION_AGE_SEC,
        collect_to_ledger,
        reconstruct_as_of,
        snapshot_count,
    )
except ModuleNotFoundError:
    from src.forex_system.ingestion.immutable_event_clock import (
        DEFAULT_MAXIMUM_CLOCK_ATTESTATION_AGE_SEC,
        collect_to_ledger,
        reconstruct_as_of,
        snapshot_count,
    )


ROOT = Path(__file__).resolve().parent
EVENT_ROOT = ROOT / "data" / "oanda_training_manager" / "news_event_tags"
DEFAULT_EVENTS = EVENT_ROOT / "events_latest.json"
DEFAULT_MANIFEST = EVENT_ROOT / "manifest.json"
DEFAULT_DATABASE = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "research_ledgers"
    / "immutable_event_clock_v2.sqlite"
)
DEFAULT_CLOCK_INTEGRITY = (
    ROOT / "data" / "oanda_training_manager" / "state" / "clock_integrity_v1.json"
)


def parse_utc(value: str) -> dt.datetime:
    text = value.strip().replace("Z", "+00:00")
    parsed = dt.datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError("timestamp must include a UTC offset")
    return parsed.astimezone(dt.timezone.utc)


def bounded_clock_attestation_age(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("clock-attestation age must be numeric") from exc
    if (
        not math.isfinite(parsed)
        or parsed < 0.0
        or parsed > DEFAULT_MAXIMUM_CLOCK_ATTESTATION_AGE_SEC
    ):
        raise argparse.ArgumentTypeError(
            "clock-attestation age must be between 0 and "
            f"{DEFAULT_MAXIMUM_CLOCK_ATTESTATION_AGE_SEC:g} seconds"
        )
    return parsed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=Path, default=DEFAULT_EVENTS)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--clock-integrity", type=Path, default=DEFAULT_CLOCK_INTEGRITY)
    parser.add_argument(
        "--maximum-clock-attestation-age-sec",
        type=bounded_clock_attestation_age,
        default=DEFAULT_MAXIMUM_CLOCK_ATTESTATION_AGE_SEC,
    )
    parser.add_argument("--as-of", type=parse_utc)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    if args.as_of is None:
        result = collect_to_ledger(
            events_path=args.events,
            manifest_path=args.manifest,
            database_path=args.database,
            clock_integrity_path=args.clock_integrity,
            maximum_clock_attestation_age_sec=args.maximum_clock_attestation_age_sec,
        )
        payload = {
            **result,
            "database_artifact_name": args.database.name,
            "snapshot_count": snapshot_count(args.database),
            "research_only": True,
            "execution_eligible": False,
            "supported_execution_decision": "no_trade",
        }
    else:
        payload = reconstruct_as_of(args.database, args.as_of)
    rendered = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
