#!/usr/bin/env python3
"""V8-bound source-conditioned currency-rank comparison (research only).

This clean adapter cohort consumes only proof-eligible V8 forecasts. It never
falls back to or imports V1-V7 source evidence or V1-V6 adapter decisions. The
explicit no-trade arm remains the zero-value reference policy.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
import os
from pathlib import Path
import sqlite3
import time
from typing import Any, Iterator, Sequence

import oanda_causal_source_factor_response_map_v8 as source_v8
import oanda_source_conditioned_currency_rank_v6 as v6


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
STATE = DATA / "state"
REPORT_ROOT = DATA / "reports" / "source_conditioned_currency_rank"

SOURCE_V8 = source_v8.OUTPUT_DATABASE
DEFAULT_TICKER = v6.DEFAULT_TICKER
DEFAULT_QUOTES = v6.DEFAULT_QUOTES
DEFAULT_QUOTE_BARS = v6.DEFAULT_QUOTE_BARS
DEFAULT_LEDGER = STATE / "source_conditioned_currency_rank_v7.sqlite"
DEFAULT_STATE = STATE / "source_conditioned_currency_rank_v7.json"
DEFAULT_REPORT = REPORT_ROOT / "SOURCE_CONDITIONED_CURRENCY_RANK_V7.md"
DEFAULT_MANIFEST = ROOT / "config" / "source_conditioned_currency_rank_v7.json"

SCHEMA_VERSION = "source_conditioned_currency_rank_v7_no_trade_v1"
CONTRACT_ID = (
    "source_conditioned_currency_rank_v7_v8_input_explicit_no_trade_20260901"
)
BASE_COHORT_ID = (
    "source_conditioned_currency_rank_v7_no_trade_prospective_20260901T114500Z"
)
PARENT_CONTRACT_ID = v6.CONTRACT_ID
REQUIRED_SOURCE_CONTRACT_ID = source_v8.CONTRACT_ID
INPUT_MODE = "required_v8_default_no_v1_v2_v3_v4_v5_v6_v7_fallback"
ARMS = v6.ARMS
NO_TRADE_BASELINE_CONTRACT_ID = (
    "source_conditioned_currency_rank_v7_no_trade_zero_value_v1_20260901"
)


def adapter_definition_sha256() -> str:
    return v6.v5.v1.digest(
        CONTRACT_ID,
        v6.v5.v1.file_sha256(Path(__file__)),
        v6.v5.v1.file_sha256(DEFAULT_MANIFEST),
        REQUIRED_SOURCE_CONTRACT_ID,
        PARENT_CONTRACT_ID,
        NO_TRADE_BASELINE_CONTRACT_ID,
    )


@contextmanager
def _v7_contract() -> Iterator[None]:
    replacements = {
        "SOURCE_V7": SOURCE_V8,
        "DEFAULT_LEDGER": DEFAULT_LEDGER,
        "DEFAULT_STATE": DEFAULT_STATE,
        "DEFAULT_REPORT": DEFAULT_REPORT,
        "DEFAULT_MANIFEST": DEFAULT_MANIFEST,
        "SCHEMA_VERSION": SCHEMA_VERSION,
        "CONTRACT_ID": CONTRACT_ID,
        "BASE_COHORT_ID": BASE_COHORT_ID,
        "PARENT_CONTRACT_ID": PARENT_CONTRACT_ID,
        "REQUIRED_SOURCE_CONTRACT_ID": REQUIRED_SOURCE_CONTRACT_ID,
        "INPUT_MODE": INPUT_MODE,
        "ARMS": ARMS,
        "NO_TRADE_BASELINE_CONTRACT_ID": NO_TRADE_BASELINE_CONTRACT_ID,
        "adapter_definition_sha256": adapter_definition_sha256,
    }
    previous = {name: getattr(v6, name) for name in replacements}
    try:
        for name, value in replacements.items():
            setattr(v6, name, value)
        yield
    finally:
        for name, value in previous.items():
            setattr(v6, name, value)


def resolve_source_database(explicit: Path | None = None) -> Path:
    if explicit is not None:
        return explicit.resolve()
    return SOURCE_V8.resolve()


def load_source_forecasts(path: Path) -> list[dict[str, Any]]:
    with _v7_contract():
        rows = v6.load_source_forecasts(path)
    foreign = sorted(
        {
            str(row.get("source_contract_id") or "")
            for row in rows
            if str(row.get("source_contract_id") or "")
            != REQUIRED_SOURCE_CONTRACT_ID
        }
    )
    if foreign:
        raise ValueError(f"v8 source database contract contamination:{foreign}")
    return rows


def source_forecast_inventory(path: Path, *, cutoff_utc) -> dict[str, Any]:
    with _v7_contract():
        inventory = v6.source_forecast_inventory(path, cutoff_utc=cutoff_utc)
    foreign = sorted(
        contract
        for contract in inventory.get("contract_counts", {})
        if contract != REQUIRED_SOURCE_CONTRACT_ID
    )
    if foreign:
        raise ValueError(f"v8 source database inventory contamination:{foreign}")
    return inventory


def open_ledger(path: Path = DEFAULT_LEDGER) -> sqlite3.Connection:
    with _v7_contract():
        return v6.open_ledger(path)


def _render_report_v7(snapshot: dict[str, Any]) -> str:
    report = v6._render_report_v6(snapshot)
    return report.replace(
        "# Source-Conditioned Currency Rank V6",
        "# Source-Conditioned Currency Rank V7",
        1,
    ).replace("required_v7_default", "required_v8_default")


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
    # The V6 adapter publishes its own intermediate contract before this V7
    # wrapper adds the V8 lineage and inert-policy fields.  Publishing that
    # intermediate document to the live V7 path creates a short fail-closed
    # integrity race.  Build both inherited documents at private paths and
    # expose only the completed V7 snapshot through the final atomic writes.
    stage_token = f"{os.getpid()}.{time.time_ns()}.v7-stage"
    staged_state_path = state_path.with_name(
        f".{state_path.name}.{stage_token}"
    )
    staged_report_path = report_path.with_name(
        f".{report_path.name}.{stage_token}"
    )
    try:
        with _v7_contract():
            snapshot = v6.run_cycle(
                source_database=source_database,
                ticker_path=ticker_path,
                quotes_path=quotes_path,
                quote_bars_database=quote_bars_database,
                ledger_path=ledger_path,
                state_path=staged_state_path,
                report_path=staged_report_path,
                observed_utc=observed_utc,
                max_source_capture_lag_sec=max_source_capture_lag_sec,
                max_quote_age_sec=max_quote_age_sec,
                max_outcome_alignment_sec=max_outcome_alignment_sec,
            )
        snapshot["source_database_mode"] = (
            "explicit_test_or_replay_override"
            if source_database is not None
            else "required_v8_default"
        )
        snapshot["source_input_status"] = (
            "ready"
            if Path(snapshot["source_database"]).exists()
            else "missing_required_v8_fail_closed"
        )
        snapshot["parent_adapter_contract_id"] = PARENT_CONTRACT_ID
        snapshot["required_source_contract_id"] = REQUIRED_SOURCE_CONTRACT_ID
        snapshot["input_mode"] = INPUT_MODE
        snapshot["policy"].update(
            {
                "default_source_contract": INPUT_MODE,
                "v1_v2_v3_v4_v5_v6_v7_source_fallback": False,
                "v1_v2_v3_v4_v5_v6_adapter_ledger_imported": False,
                "required_source_contract_id": REQUIRED_SOURCE_CONTRACT_ID,
                "comparison_arms": list(ARMS),
                "no_trade_baseline_contract_id": NO_TRADE_BASELINE_CONTRACT_ID,
                "no_trade_is_zero_value_counterfactual": True,
                "no_trade_quote_required": False,
                "no_trade_order_submitted": False,
                "abstain_inventory_is_diagnostic_only": True,
                "abstain_rows_can_trigger_rank_decisions": False,
            }
        )
        v6.v5.v1.atomic_write(
            state_path, json.dumps(snapshot, indent=2, sort_keys=True) + "\n"
        )
        v6.v5.v1.atomic_write(report_path, _render_report_v7(snapshot))
        return snapshot
    finally:
        staged_state_path.unlink(missing_ok=True)
        staged_report_path.unlink(missing_ok=True)


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
            args.duration_sec > 0 and time.monotonic() - started >= args.duration_sec
        ):
            break
        time.sleep(max(1.0, args.interval_sec))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "ARMS",
    "BASE_COHORT_ID",
    "CONTRACT_ID",
    "DEFAULT_LEDGER",
    "DEFAULT_MANIFEST",
    "DEFAULT_REPORT",
    "DEFAULT_STATE",
    "INPUT_MODE",
    "NO_TRADE_BASELINE_CONTRACT_ID",
    "PARENT_CONTRACT_ID",
    "REQUIRED_SOURCE_CONTRACT_ID",
    "SCHEMA_VERSION",
    "SOURCE_V8",
    "adapter_definition_sha256",
    "load_source_forecasts",
    "open_ledger",
    "resolve_source_database",
    "run_cycle",
    "source_forecast_inventory",
]
