#!/usr/bin/env python3
"""V4-bound source-conditioned currency-rank comparison (research only).

This immutable adapter cohort consumes only proof-eligible forecasts from the
V4 causal source-response ledger.  It never falls back to or imports V1/V2/V3
source evidence or either older adapter ledger.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
from pathlib import Path
import sqlite3
import time
from typing import Any, Iterator, Mapping, Sequence

import oanda_causal_source_factor_response_map_v4 as source_v4
import oanda_source_conditioned_currency_rank_v1 as v1
import oanda_source_conditioned_currency_rank_v2 as v2


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
STATE = DATA / "state"
REPORT_ROOT = DATA / "reports" / "source_conditioned_currency_rank"

SOURCE_V4 = source_v4.OUTPUT_DATABASE
DEFAULT_TICKER = v1.DEFAULT_TICKER
DEFAULT_QUOTES = v1.DEFAULT_QUOTES
DEFAULT_QUOTE_BARS = v1.DEFAULT_QUOTE_BARS
DEFAULT_LEDGER = STATE / "source_conditioned_currency_rank_v3.sqlite"
DEFAULT_STATE = STATE / "source_conditioned_currency_rank_v3.json"
DEFAULT_REPORT = REPORT_ROOT / "SOURCE_CONDITIONED_CURRENCY_RANK_V3.md"
DEFAULT_MANIFEST = ROOT / "config" / "source_conditioned_currency_rank_v3.json"

SCHEMA_VERSION = "source_conditioned_currency_rank_v3"
CONTRACT_ID = "source_conditioned_currency_rank_v3_v4_input_20260828"
BASE_COHORT_ID = (
    "source_conditioned_currency_rank_v3_prospective_20260828T160000Z"
)
PARENT_CONTRACT_ID = v2.CONTRACT_ID
REQUIRED_SOURCE_CONTRACT_ID = source_v4.CONTRACT_ID
INPUT_MODE = "required_v4_default_no_v1_v2_v3_fallback"
ARMS = v1.ARMS

_V1_ADAPTER_DEFINITION_SHA256 = v1.adapter_definition_sha256
_V1_LOAD_SOURCE_FORECASTS = v1.load_source_forecasts
_V1_RENDER_REPORT = v1.render_report
_V1_RUN_CYCLE = v1.run_cycle


def adapter_definition_sha256() -> str:
    return v1.digest(
        CONTRACT_ID,
        v1.file_sha256(Path(__file__)),
        v1.file_sha256(DEFAULT_MANIFEST),
        REQUIRED_SOURCE_CONTRACT_ID,
        PARENT_CONTRACT_ID,
    )


def resolve_source_database(explicit: Path | None = None) -> Path:
    if explicit is not None:
        return explicit.resolve()
    return SOURCE_V4.resolve()


def load_source_forecasts(path: Path) -> list[dict[str, Any]]:
    rows = _V1_LOAD_SOURCE_FORECASTS(path)
    if path.resolve() == SOURCE_V4.resolve():
        foreign = sorted(
            {
                str(row.get("source_contract_id") or "")
                for row in rows
                if str(row.get("source_contract_id") or "")
                != REQUIRED_SOURCE_CONTRACT_ID
            }
        )
        if foreign:
            raise ValueError(f"v4 source database contract contamination:{foreign}")
    return rows


def _render_report_v3(snapshot: Mapping[str, Any]) -> str:
    report = _V1_RENDER_REPORT(snapshot)
    return report.replace(
        "# Source-Conditioned Currency Rank V1",
        "# Source-Conditioned Currency Rank V3",
        1,
    )


@contextmanager
def _v3_contract() -> Iterator[None]:
    replacements = {
        "SOURCE_V2": SOURCE_V4,
        "DEFAULT_LEDGER": DEFAULT_LEDGER,
        "DEFAULT_STATE": DEFAULT_STATE,
        "DEFAULT_REPORT": DEFAULT_REPORT,
        "DEFAULT_MANIFEST": DEFAULT_MANIFEST,
        "SCHEMA_VERSION": SCHEMA_VERSION,
        "CONTRACT_ID": CONTRACT_ID,
        "BASE_COHORT_ID": BASE_COHORT_ID,
        "adapter_definition_sha256": adapter_definition_sha256,
        "resolve_source_database": resolve_source_database,
        "load_source_forecasts": load_source_forecasts,
        "render_report": _render_report_v3,
    }
    previous = {name: getattr(v1, name) for name in replacements}
    try:
        for name, value in replacements.items():
            setattr(v1, name, value)
        yield
    finally:
        for name, value in previous.items():
            setattr(v1, name, value)


def open_ledger(path: Path = DEFAULT_LEDGER) -> sqlite3.Connection:
    with _v3_contract():
        return v1.open_ledger(path)


def run_cycle(
    *,
    source_database: Path | None = None,
    ticker_path: Path = DEFAULT_TICKER,
    quotes_path: Path = DEFAULT_QUOTES,
    quote_bars_database: Path = DEFAULT_QUOTE_BARS,
    ledger_path: Path = DEFAULT_LEDGER,
    state_path: Path = DEFAULT_STATE,
    report_path: Path = DEFAULT_REPORT,
    observed_utc=None,
    max_source_capture_lag_sec: float = 120.0,
    max_quote_age_sec: float = 90.0,
    max_outcome_alignment_sec: float = 90.0,
) -> dict[str, Any]:
    with _v3_contract():
        snapshot = _V1_RUN_CYCLE(
            source_database=source_database,
            ticker_path=ticker_path,
            quotes_path=quotes_path,
            quote_bars_database=quote_bars_database,
            ledger_path=ledger_path,
            state_path=state_path,
            report_path=report_path,
            observed_utc=observed_utc,
            max_source_capture_lag_sec=max_source_capture_lag_sec,
            max_quote_age_sec=max_quote_age_sec,
            max_outcome_alignment_sec=max_outcome_alignment_sec,
        )
        snapshot["source_database_mode"] = (
            "explicit_test_or_replay_override"
            if source_database is not None
            else "required_v4_default"
        )
        snapshot["source_input_status"] = (
            "ready"
            if Path(snapshot["source_database"]).exists()
            else "missing_required_v4_fail_closed"
        )
        snapshot["parent_adapter_contract_id"] = PARENT_CONTRACT_ID
        snapshot["required_source_contract_id"] = REQUIRED_SOURCE_CONTRACT_ID
        snapshot["input_mode"] = INPUT_MODE
        snapshot["policy"].update(
            {
                "default_source_contract": INPUT_MODE,
                "v1_v2_v3_source_fallback": False,
                "v1_v2_adapter_ledger_imported": False,
                "required_source_contract_id": REQUIRED_SOURCE_CONTRACT_ID,
            }
        )
        v1.atomic_write(
            state_path, json.dumps(snapshot, indent=2, sort_keys=True) + "\n"
        )
        v1.atomic_write(report_path, _render_report_v3(snapshot))
        return snapshot


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-database", type=Path)
    parser.add_argument("--ticker", type=Path, default=DEFAULT_TICKER)
    parser.add_argument("--quotes", type=Path, default=DEFAULT_QUOTES)
    parser.add_argument("--quote-bars-database", type=Path, default=DEFAULT_QUOTE_BARS)
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--max-source-capture-lag-sec", type=float, default=120.0)
    parser.add_argument("--max-quote-age-sec", type=float, default=90.0)
    parser.add_argument("--max-outcome-alignment-sec", type=float, default=90.0)
    parser.add_argument("--interval-sec", type=float, default=5.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    parser.add_argument("--once", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    started = time.monotonic()
    while True:
        snapshot = run_cycle(
            source_database=args.source_database,
            ticker_path=args.ticker,
            quotes_path=args.quotes,
            quote_bars_database=args.quote_bars_database,
            ledger_path=args.ledger,
            state_path=args.state,
            report_path=args.report,
            max_source_capture_lag_sec=args.max_source_capture_lag_sec,
            max_quote_age_sec=args.max_quote_age_sec,
            max_outcome_alignment_sec=args.max_outcome_alignment_sec,
        )
        print(json.dumps(snapshot, sort_keys=True), flush=True)
        if args.once or (
            args.duration_sec > 0
            and time.monotonic() - started >= args.duration_sec
        ):
            break
        time.sleep(max(1.0, args.interval_sec))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "BASE_COHORT_ID",
    "CONTRACT_ID",
    "DEFAULT_LEDGER",
    "DEFAULT_MANIFEST",
    "DEFAULT_REPORT",
    "DEFAULT_STATE",
    "INPUT_MODE",
    "REQUIRED_SOURCE_CONTRACT_ID",
    "SCHEMA_VERSION",
    "SOURCE_V4",
    "adapter_definition_sha256",
    "load_source_forecasts",
    "open_ledger",
    "resolve_source_database",
    "run_cycle",
]
