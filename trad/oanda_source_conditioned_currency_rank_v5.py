#!/usr/bin/env python3
"""V6-bound source-conditioned currency-rank comparison (research only).

This clean adapter cohort consumes only proof-eligible V6 forecasts. It never
falls back to or imports V1-V5 source evidence or V1-V4 adapter decisions.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timedelta
import json
from pathlib import Path
import sqlite3
import time
from typing import Any, Iterator, Mapping, Sequence

import oanda_causal_source_factor_response_map_v6 as source_v6
import oanda_source_conditioned_currency_rank_v1 as v1
import oanda_source_conditioned_currency_rank_v4 as v4


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
STATE = DATA / "state"
REPORT_ROOT = DATA / "reports" / "source_conditioned_currency_rank"

SOURCE_V6 = source_v6.OUTPUT_DATABASE
DEFAULT_TICKER = v1.DEFAULT_TICKER
DEFAULT_QUOTES = v1.DEFAULT_QUOTES
DEFAULT_QUOTE_BARS = v1.DEFAULT_QUOTE_BARS
DEFAULT_LEDGER = STATE / "source_conditioned_currency_rank_v5.sqlite"
DEFAULT_STATE = STATE / "source_conditioned_currency_rank_v5.json"
DEFAULT_REPORT = REPORT_ROOT / "SOURCE_CONDITIONED_CURRENCY_RANK_V5.md"
DEFAULT_MANIFEST = ROOT / "config" / "source_conditioned_currency_rank_v5.json"

SCHEMA_VERSION = "source_conditioned_currency_rank_v5_no_trade_v2"
CONTRACT_ID = (
    "source_conditioned_currency_rank_v5_v6_input_explicit_no_trade_20260829"
)
BASE_COHORT_ID = (
    "source_conditioned_currency_rank_v5_no_trade_prospective_20260829T140000Z"
)
PARENT_CONTRACT_ID = v4.CONTRACT_ID
REQUIRED_SOURCE_CONTRACT_ID = source_v6.CONTRACT_ID
INPUT_MODE = "required_v6_default_no_v1_v2_v3_v4_v5_fallback"
ARMS = (*v1.ARMS, "no_trade")
NO_TRADE_BASELINE_CONTRACT_ID = (
    "source_conditioned_currency_rank_v5_no_trade_zero_value_v1_20260829"
)

_V1_LOAD_SOURCE_FORECASTS = v1.load_source_forecasts
_V1_OPEN_LEDGER = v1.open_ledger
_V1_RENDER_REPORT = v1.render_report
_V1_RUN_CYCLE = v1.run_cycle


def adapter_definition_sha256() -> str:
    return v1.digest(
        CONTRACT_ID,
        v1.file_sha256(Path(__file__)),
        v1.file_sha256(DEFAULT_MANIFEST),
        REQUIRED_SOURCE_CONTRACT_ID,
        PARENT_CONTRACT_ID,
        NO_TRADE_BASELINE_CONTRACT_ID,
    )


def resolve_source_database(explicit: Path | None = None) -> Path:
    if explicit is not None:
        return explicit.resolve()
    return SOURCE_V6.resolve()


def load_source_forecasts(path: Path) -> list[dict[str, Any]]:
    rows = _V1_LOAD_SOURCE_FORECASTS(path)
    if path.resolve() == SOURCE_V6.resolve():
        foreign = sorted(
            {
                str(row.get("source_contract_id") or "")
                for row in rows
                if str(row.get("source_contract_id") or "")
                != REQUIRED_SOURCE_CONTRACT_ID
            }
        )
        if foreign:
            raise ValueError(f"v6 source database contract contamination:{foreign}")
    return rows


def _render_report_v5(snapshot: Mapping[str, Any]) -> str:
    report = _V1_RENDER_REPORT(snapshot)
    report = report.replace(
        "# Source-Conditioned Currency Rank V1",
        "# Source-Conditioned Currency Rank V5",
        1,
    )
    return report.replace(
        "Research-only comparison of price rank, causal-source rank, and source direction with price/spread timing.",
        (
            "Research-only comparison of price rank, causal-source rank, source "
            "direction with price/spread timing, and an explicit zero-value "
            "no-trade baseline."
        ),
        1,
    )


def ensure_no_trade_schema(connection: sqlite3.Connection) -> None:
    """Create the append-only V5 no-trade comparison ledger.

    The earlier rank schemas intentionally admit only market-position arms.
    V5 keeps that immutable surface intact and adds a separate, foreign-keyed
    baseline table.  A baseline is one counterfactual per frozen decision and
    matures to exactly zero without requesting a market quote or formulating
    an order.
    """

    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS rank_v5_no_trade_forecast (
          forecast_id TEXT PRIMARY KEY,
          decision_id TEXT NOT NULL UNIQUE,
          arm TEXT NOT NULL CHECK(arm='no_trade'),
          horizon_min INTEGER NOT NULL,
          issued_utc TEXT NOT NULL,
          maturity_utc TEXT NOT NULL,
          predicted_gross_pips REAL NOT NULL CHECK(predicted_gross_pips=0.0),
          predicted_after_cost_pips REAL NOT NULL
            CHECK(predicted_after_cost_pips=0.0),
          action TEXT NOT NULL CHECK(action='no_trade'),
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          can_place_orders INTEGER NOT NULL CHECK(can_place_orders=0),
          can_authorize INTEGER NOT NULL CHECK(can_authorize=0),
          can_promote INTEGER NOT NULL CHECK(can_promote=0),
          contract_id TEXT NOT NULL,
          adapter_cohort_id TEXT NOT NULL,
          payload_json TEXT NOT NULL,
          FOREIGN KEY(decision_id) REFERENCES rank_decision(decision_id)
        );
        CREATE TABLE IF NOT EXISTS rank_v5_no_trade_outcome (
          forecast_id TEXT PRIMARY KEY,
          maturity_utc TEXT NOT NULL,
          evaluated_utc TEXT NOT NULL,
          gross_pips REAL NOT NULL CHECK(gross_pips=0.0),
          executable_after_cost_pips REAL NOT NULL
            CHECK(executable_after_cost_pips=0.0),
          realized_cost_pips REAL NOT NULL CHECK(realized_cost_pips=0.0),
          order_submitted INTEGER NOT NULL CHECK(order_submitted=0),
          action TEXT NOT NULL CHECK(action='no_trade'),
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          can_place_orders INTEGER NOT NULL CHECK(can_place_orders=0),
          can_authorize INTEGER NOT NULL CHECK(can_authorize=0),
          can_promote INTEGER NOT NULL CHECK(can_promote=0),
          contract_id TEXT NOT NULL,
          payload_json TEXT NOT NULL,
          FOREIGN KEY(forecast_id)
            REFERENCES rank_v5_no_trade_forecast(forecast_id)
        );
        CREATE TRIGGER IF NOT EXISTS rank_v5_no_trade_forecast_no_update
          BEFORE UPDATE ON rank_v5_no_trade_forecast
          BEGIN SELECT RAISE(ABORT,'rank_v5_no_trade_forecast is append-only'); END;
        CREATE TRIGGER IF NOT EXISTS rank_v5_no_trade_forecast_no_delete
          BEFORE DELETE ON rank_v5_no_trade_forecast
          BEGIN SELECT RAISE(ABORT,'rank_v5_no_trade_forecast is append-only'); END;
        CREATE TRIGGER IF NOT EXISTS rank_v5_no_trade_outcome_no_update
          BEFORE UPDATE ON rank_v5_no_trade_outcome
          BEGIN SELECT RAISE(ABORT,'rank_v5_no_trade_outcome is append-only'); END;
        CREATE TRIGGER IF NOT EXISTS rank_v5_no_trade_outcome_no_delete
          BEFORE DELETE ON rank_v5_no_trade_outcome
          BEGIN SELECT RAISE(ABORT,'rank_v5_no_trade_outcome is append-only'); END;
        """
    )
    connection.commit()


def persist_no_trade_baselines(
    connection: sqlite3.Connection, *, observed_utc: datetime
) -> dict[str, int]:
    """Persist and mature the exact zero-value arm for current V5 decisions."""

    ensure_no_trade_schema(connection)
    inserted_forecasts = 0
    inserted_outcomes = 0
    rows = connection.execute(
        """
        SELECT decision_id,horizon_min,decision_cutoff_utc,adapter_cohort_id
        FROM rank_decision
        WHERE contract_id=?
        ORDER BY decision_cutoff_utc,decision_id
        """,
        (CONTRACT_ID,),
    ).fetchall()
    for row in rows:
        decision_id = str(row[0])
        horizon_min = int(row[1])
        issued = v1.parse_time(row[2])
        if issued is None:
            raise ValueError(f"rank V5 decision has invalid clock: {decision_id}")
        maturity = issued + timedelta(minutes=horizon_min)
        forecast_id = "source_rank_no_trade_" + v1.digest(
            decision_id, NO_TRADE_BASELINE_CONTRACT_ID
        )[:32]
        payload = {
            "forecast_id": forecast_id,
            "decision_id": decision_id,
            "arm": "no_trade",
            "action": "no_trade",
            "horizon_min": horizon_min,
            "issued_utc": v1.iso(issued),
            "maturity_utc": v1.iso(maturity),
            "predicted_gross_pips": 0.0,
            "predicted_after_cost_pips": 0.0,
            "quote_required": False,
            "order_submitted": False,
            "research_only": True,
            "execution_eligible": False,
            "can_place_orders": False,
            "can_authorize": False,
            "can_promote": False,
            "contract_id": NO_TRADE_BASELINE_CONTRACT_ID,
            "adapter_contract_id": CONTRACT_ID,
            "adapter_cohort_id": str(row[3]),
        }
        before = connection.total_changes
        connection.execute(
            """
            INSERT OR IGNORE INTO rank_v5_no_trade_forecast VALUES (
              ?,?,'no_trade',?,?,?,0.0,0.0,'no_trade',1,0,0,0,0,?,?,?
            )
            """,
            (
                forecast_id,
                decision_id,
                horizon_min,
                v1.iso(issued),
                v1.iso(maturity),
                NO_TRADE_BASELINE_CONTRACT_ID,
                str(row[3]),
                json.dumps(payload, sort_keys=True, separators=(",", ":")),
            ),
        )
        inserted_forecasts += connection.total_changes - before
        if maturity > observed_utc:
            continue
        outcome = {
            "forecast_id": forecast_id,
            "arm": "no_trade",
            "action": "no_trade",
            "maturity_utc": v1.iso(maturity),
            "evaluated_utc": v1.iso(observed_utc),
            "gross_pips": 0.0,
            "executable_after_cost_pips": 0.0,
            "realized_cost_pips": 0.0,
            "quote_required": False,
            "order_submitted": False,
            "research_only": True,
            "execution_eligible": False,
            "can_place_orders": False,
            "can_authorize": False,
            "can_promote": False,
            "contract_id": NO_TRADE_BASELINE_CONTRACT_ID,
        }
        before = connection.total_changes
        connection.execute(
            """
            INSERT OR IGNORE INTO rank_v5_no_trade_outcome VALUES (
              ?,?,?,0.0,0.0,0.0,0,'no_trade',1,0,0,0,0,?,?
            )
            """,
            (
                forecast_id,
                v1.iso(maturity),
                v1.iso(observed_utc),
                NO_TRADE_BASELINE_CONTRACT_ID,
                json.dumps(outcome, sort_keys=True, separators=(",", ":")),
            ),
        )
        inserted_outcomes += connection.total_changes - before
    connection.commit()
    return {
        "inserted_forecasts": inserted_forecasts,
        "inserted_outcomes": inserted_outcomes,
    }


def add_no_trade_summary(
    connection: sqlite3.Connection, summary: dict[str, Any]
) -> dict[str, Any]:
    """Attach baseline counts to the immutable cohort-local report blocks."""

    ensure_no_trade_schema(connection)
    by_cohort = {
        str(row[0]): row
        for row in connection.execute(
            """
            SELECT f.adapter_cohort_id,COUNT(*),COUNT(o.forecast_id)
            FROM rank_v5_no_trade_forecast AS f
            LEFT JOIN rank_v5_no_trade_outcome AS o
              ON o.forecast_id=f.forecast_id
            WHERE f.contract_id=?
            GROUP BY f.adapter_cohort_id
            """,
            (NO_TRADE_BASELINE_CONTRACT_ID,),
        )
    }
    output = dict(summary)
    output["cohorts"] = [dict(row) for row in summary.get("cohorts") or []]
    for cohort in output["cohorts"]:
        counts = by_cohort.get(str(cohort.get("adapter_cohort_id") or ""))
        forecasts = int(counts[1] or 0) if counts is not None else 0
        outcomes = int(counts[2] or 0) if counts is not None else 0
        arms = list(cohort.get("arms") or [])
        arms.append(
            {
                "arm": "no_trade",
                "forecasts": forecasts,
                "selected": forecasts,
                "abstained": 0,
                "outcomes": outcomes,
                "mean_after_cost_pips": 0.0 if outcomes else None,
                "after_cost_win_rate": 0.0 if outcomes else None,
                "zero_value_outcomes": outcomes,
            }
        )
        cohort["arms"] = sorted(arms, key=lambda row: str(row.get("arm") or ""))
    output["no_trade_baseline_forecasts"] = sum(
        int(row[1] or 0) for row in by_cohort.values()
    )
    output["no_trade_baseline_outcomes"] = sum(
        int(row[2] or 0) for row in by_cohort.values()
    )
    return output


@contextmanager
def _v5_contract() -> Iterator[None]:
    replacements = {
        "SOURCE_V2": SOURCE_V6,
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
        "open_ledger": open_ledger,
        "render_report": _render_report_v5,
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
    connection = _V1_OPEN_LEDGER(path)
    ensure_no_trade_schema(connection)
    return connection


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
    with _v5_contract():
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
        connection = open_ledger(ledger_path)
        try:
            baseline_updates = persist_no_trade_baselines(
                connection,
                observed_utc=(observed_utc or v1.utc_now()).astimezone(v1.UTC),
            )
            snapshot["ledger"] = add_no_trade_summary(
                connection, v1.ledger_summary(connection)
            )
            integrity = str(connection.execute("PRAGMA quick_check(1)").fetchone()[0])
        finally:
            connection.close()
        snapshot["matured_no_trade_outcomes"] = int(
            baseline_updates["inserted_outcomes"]
        )
        snapshot["matured_outcomes"] = int(snapshot.get("matured_outcomes") or 0) + int(
            baseline_updates["inserted_outcomes"]
        )
        snapshot["comparison_arms"] = list(ARMS)
        snapshot["no_trade_baseline_contract_id"] = (
            NO_TRADE_BASELINE_CONTRACT_ID
        )
        snapshot["no_trade_baseline"] = {
            **baseline_updates,
            "forecast_count": int(
                snapshot["ledger"].get("no_trade_baseline_forecasts") or 0
            ),
            "outcome_count": int(
                snapshot["ledger"].get("no_trade_baseline_outcomes") or 0
            ),
            "value_pips": 0.0,
            "quote_required": False,
            "order_submitted": False,
            "research_only": True,
            "execution_eligible": False,
            "can_place_orders": False,
            "can_authorize": False,
            "can_promote": False,
        }
        snapshot["sqlite_integrity"] = integrity
        snapshot["source_database_mode"] = (
            "explicit_test_or_replay_override"
            if source_database is not None
            else "required_v6_default"
        )
        snapshot["source_input_status"] = (
            "ready"
            if Path(snapshot["source_database"]).exists()
            else "missing_required_v6_fail_closed"
        )
        snapshot["parent_adapter_contract_id"] = PARENT_CONTRACT_ID
        snapshot["required_source_contract_id"] = REQUIRED_SOURCE_CONTRACT_ID
        snapshot["input_mode"] = INPUT_MODE
        snapshot["policy"].update(
            {
                "default_source_contract": INPUT_MODE,
                "v1_v2_v3_v4_v5_source_fallback": False,
                "v1_v2_v3_v4_adapter_ledger_imported": False,
                "required_source_contract_id": REQUIRED_SOURCE_CONTRACT_ID,
                "comparison_arms": list(ARMS),
                "no_trade_baseline_contract_id": (
                    NO_TRADE_BASELINE_CONTRACT_ID
                ),
                "no_trade_is_zero_value_counterfactual": True,
                "no_trade_quote_required": False,
                "no_trade_order_submitted": False,
            }
        )
        v1.atomic_write(
            state_path, json.dumps(snapshot, indent=2, sort_keys=True) + "\n"
        )
        v1.atomic_write(report_path, _render_report_v5(snapshot))
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
    "BASE_COHORT_ID",
    "CONTRACT_ID",
    "DEFAULT_LEDGER",
    "DEFAULT_MANIFEST",
    "DEFAULT_REPORT",
    "DEFAULT_STATE",
    "INPUT_MODE",
    "NO_TRADE_BASELINE_CONTRACT_ID",
    "REQUIRED_SOURCE_CONTRACT_ID",
    "SCHEMA_VERSION",
    "SOURCE_V6",
    "adapter_definition_sha256",
    "add_no_trade_summary",
    "ensure_no_trade_schema",
    "load_source_forecasts",
    "open_ledger",
    "persist_no_trade_baselines",
    "resolve_source_database",
    "run_cycle",
]
