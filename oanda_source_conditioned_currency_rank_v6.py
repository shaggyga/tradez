#!/usr/bin/env python3
"""V7-bound source-conditioned currency-rank comparison (research only).

This clean adapter cohort consumes only proof-eligible V7 forecasts.  It never
falls back to or imports V1-V6 source evidence or V1-V5 adapter decisions.
The explicit no-trade arm remains the zero-value reference policy.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
from pathlib import Path
import sqlite3
import time
from typing import Any, Iterator, Mapping, Sequence

import oanda_causal_source_factor_response_map_v7 as source_v7
import oanda_source_conditioned_currency_rank_v5 as v5


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
STATE = DATA / "state"
REPORT_ROOT = DATA / "reports" / "source_conditioned_currency_rank"

SOURCE_V7 = source_v7.OUTPUT_DATABASE
DEFAULT_TICKER = v5.DEFAULT_TICKER
DEFAULT_QUOTES = v5.DEFAULT_QUOTES
DEFAULT_QUOTE_BARS = v5.DEFAULT_QUOTE_BARS
DEFAULT_LEDGER = STATE / "source_conditioned_currency_rank_v6.sqlite"
DEFAULT_STATE = STATE / "source_conditioned_currency_rank_v6.json"
DEFAULT_REPORT = REPORT_ROOT / "SOURCE_CONDITIONED_CURRENCY_RANK_V6.md"
DEFAULT_MANIFEST = ROOT / "config" / "source_conditioned_currency_rank_v6.json"

SCHEMA_VERSION = "source_conditioned_currency_rank_v6_no_trade_v1"
CONTRACT_ID = (
    "source_conditioned_currency_rank_v6_v7_input_explicit_no_trade_20260901"
)
BASE_COHORT_ID = (
    "source_conditioned_currency_rank_v6_no_trade_prospective_20260901T023000Z"
)
PARENT_CONTRACT_ID = v5.CONTRACT_ID
REQUIRED_SOURCE_CONTRACT_ID = source_v7.CONTRACT_ID
INPUT_MODE = "required_v7_default_no_v1_v2_v3_v4_v5_v6_fallback"
ARMS = v5.ARMS
NO_TRADE_BASELINE_CONTRACT_ID = (
    "source_conditioned_currency_rank_v6_no_trade_zero_value_v1_20260901"
)

_BASE_LOAD_SOURCE_FORECASTS = v5._V1_LOAD_SOURCE_FORECASTS
_BASE_OPEN_LEDGER = v5._V1_OPEN_LEDGER
_BASE_RUN_CYCLE = v5.run_cycle


def adapter_definition_sha256() -> str:
    return v5.v1.digest(
        CONTRACT_ID,
        v5.v1.file_sha256(Path(__file__)),
        v5.v1.file_sha256(DEFAULT_MANIFEST),
        REQUIRED_SOURCE_CONTRACT_ID,
        PARENT_CONTRACT_ID,
        NO_TRADE_BASELINE_CONTRACT_ID,
    )


def resolve_source_database(explicit: Path | None = None) -> Path:
    if explicit is not None:
        return explicit.resolve()
    return SOURCE_V7.resolve()


def load_source_forecasts(path: Path) -> list[dict[str, Any]]:
    rows = _BASE_LOAD_SOURCE_FORECASTS(path)
    if path.resolve() == SOURCE_V7.resolve():
        foreign = sorted(
            {
                str(row.get("source_contract_id") or "")
                for row in rows
                if str(row.get("source_contract_id") or "")
                != REQUIRED_SOURCE_CONTRACT_ID
            }
        )
        if foreign:
            raise ValueError(f"v7 source database contract contamination:{foreign}")
    return rows


def source_forecast_inventory(
    path: Path, *, cutoff_utc
) -> dict[str, Any]:
    """Inventory all V7 rows without making abstentions actionable.

    ``source_forecast_rows`` intentionally counts only rows the rank adapter
    may consume. That made a healthy producer with explicit abstentions look
    identical to an empty producer. This parallel, point-in-time inventory
    leaves the eligibility query and decision path unchanged.
    """

    cutoff = cutoff_utc.astimezone(v5.v1.UTC)
    cutoff_text = v5.v1.iso(cutoff)
    empty = {
        "count_basis": "issued_utc_at_or_before_adapter_generated_utc",
        "cutoff_utc": cutoff_text,
        "database_available": path.exists(),
        "database_integrity": "missing" if not path.exists() else "unknown",
        "total_rows": 0,
        "rank_eligible_rows": 0,
        "prospective_proof_flag_rows": 0,
        "abstain_rows": 0,
        "excluded_from_rank_rows": 0,
        "distinct_event_count": 0,
        "distinct_factor_observation_count": 0,
        "forecast_state_counts": {},
        "abstain_reason_counts": {},
        "currency_counts": {},
        "horizon_counts": {},
        "contract_counts": {},
        "oldest_issued_utc": None,
        "latest_issued_utc": None,
        "status": "missing_database" if not path.exists() else "no_rows_at_cutoff",
    }
    if not path.exists():
        return empty

    connection = sqlite3.connect(
        f"file:{path.as_posix()}?mode=ro", uri=True, timeout=15.0
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    connection.execute("PRAGMA busy_timeout=15000")
    try:
        connection.execute("BEGIN")
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        if "source_factor_forecast" not in tables:
            raise ValueError("source factor database lacks forecast inventory table")
        columns = {
            str(row[1])
            for row in connection.execute(
                "PRAGMA table_info(source_factor_forecast)"
            )
        }
        required = {
            "forecast_id", "factor_observation_id", "canonical_event_id",
            "currency", "horizon_min", "issued_utc", "forecast_state",
            "abstain_reason", "prospective_proof_eligible",
            "probability_strengthening", "predicted_currency_factor_bps",
            "predicted_absolute_factor_bps", "contract_id",
        }
        missing = required - columns
        if missing:
            raise ValueError(
                f"source forecast inventory schema missing: {sorted(missing)}"
            )
        where = "julianday(issued_utc) <= julianday(?)"
        parameters = (cutoff_text,)
        summary = connection.execute(
            f"""
            SELECT COUNT(*) total_rows,
                   SUM(CASE WHEN forecast_state='forecast'
                                  AND prospective_proof_eligible=1
                                  AND probability_strengthening IS NOT NULL
                                  AND predicted_currency_factor_bps IS NOT NULL
                                  AND predicted_absolute_factor_bps IS NOT NULL
                            THEN 1 ELSE 0 END) rank_eligible_rows,
                   SUM(CASE WHEN prospective_proof_eligible=1
                            THEN 1 ELSE 0 END) prospective_proof_flag_rows,
                   SUM(CASE WHEN forecast_state='abstain'
                            THEN 1 ELSE 0 END) abstain_rows,
                   COUNT(DISTINCT canonical_event_id) distinct_event_count,
                   COUNT(DISTINCT factor_observation_id)
                       distinct_factor_observation_count,
                   MIN(issued_utc) oldest_issued_utc,
                   MAX(issued_utc) latest_issued_utc
              FROM source_factor_forecast
             WHERE {where}
            """,
            parameters,
        ).fetchone()

        def grouped(column: str, *, extra_where: str = "") -> dict[str, int]:
            clause = where + extra_where
            return {
                str(row[0]): int(row[1])
                for row in connection.execute(
                    f"""
                    SELECT {column},COUNT(*)
                      FROM source_factor_forecast
                     WHERE {clause}
                     GROUP BY {column}
                     ORDER BY COUNT(*) DESC,{column}
                    """,
                    parameters,
                )
                if str(row[0] or "")
            }

        total = int(summary["total_rows"] or 0)
        eligible = int(summary["rank_eligible_rows"] or 0)
        abstain = int(summary["abstain_rows"] or 0)
        contracts = grouped("contract_id")
        if path.resolve() == SOURCE_V7.resolve():
            foreign = sorted(
                contract
                for contract in contracts
                if contract != REQUIRED_SOURCE_CONTRACT_ID
            )
            if foreign:
                raise ValueError(
                    f"v7 source database inventory contamination:{foreign}"
                )
        if eligible:
            status = "rank_eligible_rows_available"
        elif total and abstain == total:
            status = "all_rows_abstain"
        elif total:
            status = "no_rank_eligible_rows"
        else:
            status = "no_rows_at_cutoff"
        return {
            **empty,
            "database_integrity": str(
                connection.execute("PRAGMA quick_check(1)").fetchone()[0]
            ),
            "total_rows": total,
            "rank_eligible_rows": eligible,
            "prospective_proof_flag_rows": int(
                summary["prospective_proof_flag_rows"] or 0
            ),
            "abstain_rows": abstain,
            "excluded_from_rank_rows": total - eligible,
            "distinct_event_count": int(summary["distinct_event_count"] or 0),
            "distinct_factor_observation_count": int(
                summary["distinct_factor_observation_count"] or 0
            ),
            "forecast_state_counts": grouped("forecast_state"),
            "abstain_reason_counts": grouped(
                "abstain_reason", extra_where=" AND forecast_state='abstain'"
            ),
            "currency_counts": grouped("currency"),
            "horizon_counts": grouped("horizon_min"),
            "contract_counts": contracts,
            "oldest_issued_utc": summary["oldest_issued_utc"],
            "latest_issued_utc": summary["latest_issued_utc"],
            "status": status,
        }
    finally:
        connection.close()


def open_ledger(path: Path = DEFAULT_LEDGER) -> sqlite3.Connection:
    connection = _BASE_OPEN_LEDGER(path)
    v5.ensure_no_trade_schema(connection)
    return connection


def _render_report_v6(snapshot: Mapping[str, Any]) -> str:
    report = v5._V1_RENDER_REPORT(snapshot)
    report = report.replace(
        "# Source-Conditioned Currency Rank V1",
        "# Source-Conditioned Currency Rank V6",
        1,
    )
    report = report.replace(
        (
            "Research-only comparison of price rank, causal-source rank, and "
            "source direction with price/spread timing."
        ),
        (
            "Research-only comparison of price rank, V7 causal-source rank, "
            "source direction with price/spread timing, and an explicit "
            "zero-value no-trade baseline."
        ),
        1,
    )
    inventory = snapshot.get("source_forecast_inventory") or {}
    reasons = inventory.get("abstain_reason_counts") or {}
    diagnostic = [
        "",
        "## V7 producer inventory",
        "",
        (
            "This point-in-time inventory distinguishes source rows produced "
            "from rows the rank adapter may consume. It does not relax the "
            "forecast or proof-eligibility filters."
        ),
        "",
        (
            "- All source rows / rank-eligible rows / abstentions: "
            f"**{int(inventory.get('total_rows') or 0)} / "
            f"{int(inventory.get('rank_eligible_rows') or 0)} / "
            f"{int(inventory.get('abstain_rows') or 0)}**"
        ),
        (
            "- Distinct source events / factor observations: "
            f"**{int(inventory.get('distinct_event_count') or 0)} / "
            f"{int(inventory.get('distinct_factor_observation_count') or 0)}**"
        ),
        f"- Producer state: **{inventory.get('status') or 'unknown'}**",
    ]
    if reasons:
        diagnostic.extend(
            [
                "",
                "| Abstention reason | Rows |",
                "|---|---:|",
                *[
                    f"| {reason} | {int(count)} |"
                    for reason, count in reasons.items()
                ],
            ]
        )
    marker = "\n## Boundaries"
    block = "\n".join(diagnostic)
    return report.replace(marker, f"\n{block}\n{marker}", 1)


@contextmanager
def _v6_contract() -> Iterator[None]:
    replacements = {
        "SOURCE_V6": SOURCE_V7,
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
        "resolve_source_database": resolve_source_database,
        "load_source_forecasts": load_source_forecasts,
        "open_ledger": open_ledger,
        "_render_report_v5": _render_report_v6,
    }
    previous = {name: getattr(v5, name) for name in replacements}
    try:
        for name, value in replacements.items():
            setattr(v5, name, value)
        yield
    finally:
        for name, value in previous.items():
            setattr(v5, name, value)


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
    observed = (observed_utc or v5.v1.utc_now()).astimezone(v5.v1.UTC)
    source_path = resolve_source_database(source_database)
    inventory = source_forecast_inventory(source_path, cutoff_utc=observed)
    with _v6_contract():
        snapshot = _BASE_RUN_CYCLE(
            source_database=source_database,
            ticker_path=ticker_path,
            quotes_path=quotes_path,
            quote_bars_database=quote_bars_database,
            ledger_path=ledger_path,
            state_path=state_path,
            report_path=report_path,
            observed_utc=observed,
            max_source_capture_lag_sec=max_source_capture_lag_sec,
            max_quote_age_sec=max_quote_age_sec,
            max_outcome_alignment_sec=max_outcome_alignment_sec,
        )
        snapshot["source_database_mode"] = (
            "explicit_test_or_replay_override"
            if source_database is not None
            else "required_v7_default"
        )
        snapshot["source_input_status"] = (
            "ready"
            if Path(snapshot["source_database"]).exists()
            else "missing_required_v7_fail_closed"
        )
        snapshot["parent_adapter_contract_id"] = PARENT_CONTRACT_ID
        snapshot["required_source_contract_id"] = REQUIRED_SOURCE_CONTRACT_ID
        snapshot["input_mode"] = INPUT_MODE
        snapshot["source_forecast_rows_semantics"] = (
            "rank_eligible_non_abstaining_rows_loaded_by_adapter"
        )
        snapshot["source_forecast_inventory"] = inventory
        snapshot["policy"].update(
            {
                "default_source_contract": INPUT_MODE,
                "v1_v2_v3_v4_v5_v6_source_fallback": False,
                "v1_v2_v3_v4_v5_adapter_ledger_imported": False,
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
        v5.v1.atomic_write(
            state_path, json.dumps(snapshot, indent=2, sort_keys=True) + "\n"
        )
        v5.v1.atomic_write(report_path, _render_report_v6(snapshot))
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
    "SOURCE_V7",
    "adapter_definition_sha256",
    "load_source_forecasts",
    "open_ledger",
    "resolve_source_database",
    "run_cycle",
]
