#!/usr/bin/env python3
"""Causal V12 narrative join for the movement-first FX diagnostic.

V5 remains frozen in its original files.  This prospective V6 contract reads
only the append-only V12 meter database and, for every detected mover, selects
the latest completed bucket that both ended and was sealed no later than the
move start.  The provisional ``partial_live`` JSON view is deliberately not an
input.  This module has no broker, authorization, lifecycle, promotion, or
execution surface.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Mapping, Sequence

import oanda_live_move_news_snapshot as v5
import oanda_major_move_gap_census as census
import oanda_market_sentiment_ticker as market_ticker
from oanda_continuous_narrative_meter_v12 import (
    METER_ACTIVATED_UTC,
    METER_CONTRACT_ID,
    SEAL_GRACE_SECONDS,
)


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
STATE = DATA / "state"
DEFAULT_MOVES = STATE / "practice_007_latest_moves_v1.json"
DEFAULT_SOURCES = STATE / "source_governance_v1.sqlite"
DEFAULT_NARRATIVE_DATABASE = STATE / "continuous_narrative_meter_v12.sqlite"
DEFAULT_QUOTES = STATE / "practice_007_market_quotes_v1.json"
DEFAULT_FACTOR_HISTORY = DATA / "market_sentiment_ticker" / "quote_history.json"
DEFAULT_OUTPUT = STATE / "live_move_news_snapshot_v6r2.json"
DEFAULT_HISTORY = STATE / "live_move_news_cases_v6r2.sqlite"
DEFAULT_REPORT = DATA / "reports" / "live_move_news" / "LIVE_MOVE_NEWS_CURRENT_V6R2.md"

CONTRACT_ID = "live_move_news_snapshot_v6r2_precise_v12_meter_at_move_start_20260827"
CONTRACT_ACTIVATED_UTC = dt.datetime(2026, 8, 27, 8, 5, tzinfo=dt.timezone.utc)
SCHEMA_VERSION = 6
PAIR_MODEL_ID = "narrative_acceleration_v1"
CURRENCY_DIRECTION_THRESHOLD = 0.05
PAIR_DIRECTION_THRESHOLD = 0.10


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _read_only_database(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(
        f"file:{path.resolve().as_posix()}?mode=ro", uri=True, timeout=30.0
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    return connection


def _json_object(value: Any) -> dict[str, Any]:
    try:
        parsed = json.loads(str(value or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _finite(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    return number if number == number and abs(number) != float("inf") else 0.0


def _precise_epoch(value: Any) -> float | None:
    """Parse timestamps without truncating sub-second seal availability."""

    if value in (None, ""):
        return None
    try:
        number = float(value)
        if number == number and abs(number) != float("inf") and number > 1_000_000_000:
            return number
    except (TypeError, ValueError):
        pass
    try:
        parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.timestamp()


def _currency_state(row: sqlite3.Row) -> dict[str, Any]:
    scores = _json_object(row["model_scores_json"])
    score = _finite(scores.get(PAIR_MODEL_ID))
    return {
        "currency": str(row["currency"]),
        "score": round(score, 9),
        "direction": (
            "POSITIVE"
            if score > CURRENCY_DIRECTION_THRESHOLD
            else "NEGATIVE"
            if score < -CURRENCY_DIRECTION_THRESHOLD
            else "NEUTRAL"
        ),
        "model_id": PAIR_MODEL_ID,
        "model_scores": {str(key): round(_finite(value), 9) for key, value in scores.items()},
        "attention_level": round(_finite(row["attention_level"]), 9),
        "attention_acceleration": round(_finite(row["attention_acceleration"]), 9),
        "new_story_count": int(row["new_story_count"]),
        "active_story_count": int(row["active_story_count"]),
        "source_family_count": int(row["source_family_count"]),
        "agreement": round(_finite(row["agreement"]), 9),
        "novelty": round(_finite(row["novelty"]), 9),
        "trusted_story_count": int(row["trusted_story_count"]),
        "forward_timely_story_count": int(row["forward_timely_story_count"]),
        "evidence_class": str(row["evidence_class"]),
        "row_created_utc": str(row["created_utc"]),
        "research_only": True,
        "execution_eligible": False,
    }


def _unavailable_pair_state(instrument: str, reason: str) -> tuple[dict[str, Any], dict[str, Any]]:
    base, quote = instrument.split("_", 1) if "_" in instrument else (instrument, "")
    state = {
        "base": base,
        "quote": quote,
        "score": 0.0,
        "direction": "NEUTRAL",
        "state_available": False,
        "research_only": True,
        "execution_eligible": False,
    }
    provenance = {
        "meter_contract_id": METER_CONTRACT_ID,
        "meter_clock_utc": None,
        "meter_sealed_at_utc": None,
        "meter_state_kind": "unavailable",
        "availability_reason": reason,
        "clock_semantics": "clock_utc_is_closed_bucket_end",
        "causal_join_rule": (
            "max(clock_utc) where clock_utc<=move_start and sealed_at_utc<=move_start"
        ),
        "partial_live_excluded": True,
        "research_only": True,
        "execution_eligible": False,
    }
    return state, provenance


def causal_pair_state(
    connection: sqlite3.Connection,
    instrument: str,
    *,
    move_start_epoch: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return the exact V12 state that was sealed by the mover start."""

    if "_" not in instrument:
        return _unavailable_pair_state(instrument, "invalid_instrument")
    base, quote = instrument.split("_", 1)
    seals = connection.execute(
        """SELECT clock_utc,sealed_at_utc,seal_grace_seconds,
                  currency_row_count,input_story_count,research_only,
                  execution_eligible
           FROM bucket_seals
           WHERE meter_contract_id=?
           ORDER BY clock_utc DESC""",
        (METER_CONTRACT_ID,),
    ).fetchall()
    selected: sqlite3.Row | None = None
    for seal in seals:
        clock_epoch = _precise_epoch(seal["clock_utc"])
        sealed_epoch = _precise_epoch(seal["sealed_at_utc"])
        if (
            clock_epoch is not None
            and sealed_epoch is not None
            and clock_epoch <= move_start_epoch
            and sealed_epoch <= move_start_epoch
            and int(seal["currency_row_count"]) == 21
            and int(seal["research_only"]) == 1
            and int(seal["execution_eligible"]) == 0
        ):
            selected = seal
            break
    if selected is None:
        return _unavailable_pair_state(
            instrument,
            "no_complete_v12_bucket_was_sealed_at_or_before_move_start",
        )
    rows = connection.execute(
        """SELECT * FROM currency_meter
           WHERE meter_contract_id=? AND clock_utc=?
             AND currency IN (?,?)""",
        (METER_CONTRACT_ID, str(selected["clock_utc"]), base, quote),
    ).fetchall()
    by_currency = {str(row["currency"]): row for row in rows}
    if base not in by_currency or quote not in by_currency:
        return _unavailable_pair_state(
            instrument, "sealed_bucket_missing_required_currency_leg"
        )
    base_state = _currency_state(by_currency[base])
    quote_state = _currency_state(by_currency[quote])
    score = max(-1.0, min(1.0, base_state["score"] - quote_state["score"]))
    pair_state = {
        "base": base,
        "quote": quote,
        "score": round(score, 9),
        "direction": (
            "LONG"
            if score > PAIR_DIRECTION_THRESHOLD
            else "SHORT"
            if score < -PAIR_DIRECTION_THRESHOLD
            else "NEUTRAL"
        ),
        "state_available": True,
        "model_id": PAIR_MODEL_ID,
        "base_currency_state": base_state,
        "quote_currency_state": quote_state,
        "research_only": True,
        "execution_eligible": False,
    }
    provenance = {
        "meter_contract_id": METER_CONTRACT_ID,
        "meter_clock_utc": str(selected["clock_utc"]),
        "meter_sealed_at_utc": str(selected["sealed_at_utc"]),
        "meter_state_kind": "immutable_completed_bucket",
        "meter_currency_row_count": int(selected["currency_row_count"]),
        "meter_input_story_count": int(selected["input_story_count"]),
        "meter_seal_grace_seconds": int(selected["seal_grace_seconds"]),
        "clock_semantics": "clock_utc_is_closed_bucket_end",
        "causal_join_rule": (
            "max(clock_utc) where clock_utc<=move_start and sealed_at_utc<=move_start"
        ),
        "partial_live_excluded": True,
        "research_only": True,
        "execution_eligible": False,
    }
    return pair_state, provenance


def meter_database_status(path: Path) -> dict[str, Any]:
    try:
        connection = _read_only_database(path)
        try:
            integrity = str(connection.execute("PRAGMA quick_check").fetchone()[0])
            registry = connection.execute(
                "SELECT * FROM meter_contract_registry WHERE meter_contract_id=?",
                (METER_CONTRACT_ID,),
            ).fetchone()
            latest = connection.execute(
                "SELECT MAX(clock_utc),MAX(sealed_at_utc),COUNT(*) "
                "FROM bucket_seals WHERE meter_contract_id=?",
                (METER_CONTRACT_ID,),
            ).fetchone()
        finally:
            connection.close()
    except (OSError, sqlite3.Error) as exc:
        return {"ok": False, "error": f"{type(exc).__name__}:{exc}"}
    return {
        "ok": bool(
            integrity == "ok"
            and registry is not None
            and int(registry["research_only"]) == 1
            and int(registry["execution_eligible"]) == 0
        ),
        "sqlite_quick_check": integrity,
        "meter_contract_id": METER_CONTRACT_ID,
        "meter_activated_utc": str(registry["activated_utc"]) if registry else None,
        "bucket_minutes": int(registry["bucket_minutes"]) if registry else None,
        "seal_grace_seconds": int(registry["seal_grace_seconds"]) if registry else None,
        "bucket_semantics": str(registry["bucket_semantics"]) if registry else None,
        "latest_sealed_clock_utc": str(latest[0]) if latest and latest[0] else None,
        "latest_sealed_at_utc": str(latest[1]) if latest and latest[1] else None,
        "sealed_bucket_count": int(latest[2]) if latest else 0,
    }


def stable_case_id(row: Mapping[str, Any]) -> str:
    material = "|".join(
        (
            CONTRACT_ID,
            str(row.get("instrument") or ""),
            str(row.get("start_utc") or ""),
            str(row.get("move_direction") or ""),
        )
    )
    return "live_move_news_case_v6_" + hashlib.sha256(
        material.encode("utf-8")
    ).hexdigest()[:32]


def connect_history(path: Path) -> sqlite3.Connection:
    connection = v5.connect_history(path)
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS mover_case_contract_registry(
            contract_id TEXT PRIMARY KEY,
            activated_utc TEXT NOT NULL,
            meter_contract_id TEXT NOT NULL,
            meter_database TEXT NOT NULL,
            narrative_join_semantics TEXT NOT NULL,
            partial_live_excluded INTEGER NOT NULL CHECK(partial_live_excluded=1),
            research_only INTEGER NOT NULL CHECK(research_only=1),
            execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
            created_utc TEXT NOT NULL
        );
        CREATE TRIGGER IF NOT EXISTS mover_case_contract_registry_no_update
            BEFORE UPDATE ON mover_case_contract_registry
            BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS mover_case_contract_registry_no_delete
            BEFORE DELETE ON mover_case_contract_registry
            BEGIN SELECT RAISE(ABORT,'append_only'); END;
        """
    )
    connection.commit()
    return connection


def record_cases(
    path: Path,
    rows: Sequence[Mapping[str, Any]],
    *,
    recorded_utc: str,
    meter_database: Path,
) -> dict[str, int]:
    connection = connect_history(path)
    inserted = 0
    try:
        with connection:
            connection.execute(
                "INSERT OR IGNORE INTO mover_case_contract_registry VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    CONTRACT_ID,
                    CONTRACT_ACTIVATED_UTC.isoformat(),
                    METER_CONTRACT_ID,
                    str(meter_database.resolve()),
                    (
                        "latest complete V12 bucket with clock_utc and sealed_at_utc "
                        "both at_or_before mover start"
                    ),
                    1,
                    1,
                    0,
                    recorded_utc,
                ),
            )
            for source in rows:
                row = dict(source)
                case_id = stable_case_id(row)
                row["case_id"] = case_id
                before = connection.total_changes
                connection.execute(
                    "INSERT OR IGNORE INTO mover_cases VALUES (?,?,?,?,?,?)",
                    (
                        case_id,
                        recorded_utc,
                        str(row.get("instrument") or ""),
                        str(row.get("start_utc") or ""),
                        str(row.get("end_utc") or ""),
                        json.dumps(row, sort_keys=True, separators=(",", ":")),
                    ),
                )
                inserted += int(connection.total_changes > before)
        total = int(connection.execute("SELECT COUNT(*) FROM mover_cases").fetchone()[0])
        integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    finally:
        connection.close()
    if integrity != "ok":
        raise sqlite3.IntegrityError(f"mover history integrity:{integrity}")
    return {"inserted": inserted, "total": total}


def render_report(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Live move/news snapshot V6",
        "",
        f"Generated: `{payload.get('generated_utc')}`",
        "",
        (
            "Research-only movement-first diagnostic. Narrative state comes only "
            "from an immutable V12 bucket sealed by the move start; partial-live "
            "state is excluded. It cannot place orders."
        ),
        "",
        "| Pair | Move | Net pips | Start | Meter clock | Sealed at | Explanation |",
        "|---|---:|---:|---|---|---|---|",
    ]
    for row in payload.get("movers") or []:
        provenance = row.get("continuous_narrative_provenance") or {}
        lines.append(
            f"| {row.get('instrument')} | {row.get('move_direction')} | "
            f"{row.get('executable_net_pips')} | {row.get('start_utc')} | "
            f"{provenance.get('meter_clock_utc')} | "
            f"{provenance.get('meter_sealed_at_utc')} | "
            f"{row.get('explanation_state')} |"
        )
    lines.extend(
        [
            "",
            "Strict direction still requires forward-timely publish-eligible evidence.",
            "The narrative arm remains unvalidated shadow research.",
            "",
        ]
    )
    return "\n".join(lines)


def run(
    *,
    moves_path: Path = DEFAULT_MOVES,
    source_database: Path = DEFAULT_SOURCES,
    narrative_database: Path = DEFAULT_NARRATIVE_DATABASE,
    quotes_path: Path = DEFAULT_QUOTES,
    factor_history_path: Path = DEFAULT_FACTOR_HISTORY,
    output_path: Path = DEFAULT_OUTPUT,
    history_path: Path = DEFAULT_HISTORY,
    report_path: Path = DEFAULT_REPORT,
    lookback_minutes: int = 120,
    mover_count: int = 10,
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    observed = (now or dt.datetime.now(dt.timezone.utc)).astimezone(dt.timezone.utc)
    generated = observed.isoformat()
    generated_epoch = int(observed.timestamp())
    moves_state = v5.read_json(moves_path)
    movers = v5.selected_movers(moves_state, mover_count)
    quotes_state = v5.read_json(quotes_path)
    quotes = quotes_state.get("quotes") or {}
    if not isinstance(quotes, Mapping):
        quotes = {}
    starts = [census.parse_epoch(row.get("start_utc")) for row in movers]
    ends = [census.parse_epoch(row.get("end_utc")) for row in movers]
    valid_starts = [value for value in starts if value is not None]
    valid_ends = [value for value in ends if value is not None]
    minimum = (
        int(min(valid_starts) - lookback_minutes * 60)
        if valid_starts
        else generated_epoch - lookback_minutes * 60
    )
    maximum = int(max(valid_ends)) if valid_ends else generated_epoch
    source_index, source_highwater = census.load_source_index(
        source_database,
        minimum_effective_epoch=minimum,
        maximum_effective_epoch=maximum,
    )
    meter_status = meter_database_status(narrative_database)
    meter_connection: sqlite3.Connection | None = None
    if meter_status.get("ok"):
        meter_connection = _read_only_database(narrative_database)
    cases: list[dict[str, Any]] = []
    try:
        for mover in movers:
            instrument = str(mover.get("instrument") or "")
            start_epoch = census.parse_epoch(mover.get("start_utc"))
            if start_epoch is None or meter_connection is None:
                pair_state, provenance = _unavailable_pair_state(
                    instrument,
                    "meter_database_unavailable_or_invalid"
                    if meter_connection is None
                    else "invalid_move_start",
                )
            else:
                pair_state, provenance = causal_pair_state(
                    meter_connection,
                    instrument,
                    move_start_epoch=int(start_epoch),
                )
            case = v5.mover_case(
                mover,
                source_index,
                {instrument: pair_state},
                generated_epoch=generated_epoch,
                lookback_minutes=lookback_minutes,
            )
            case["continuous_narrative_provenance"] = provenance
            cases.append(case)
    finally:
        if meter_connection is not None:
            meter_connection.close()
    factor_history = market_ticker.load_history(factor_history_path)
    factor_surfaces = v5.build_causal_factor_strength_surfaces(cases, factor_history)
    factor_episode_count = v5.assign_factor_episodes(
        cases, strength_surfaces=factor_surfaces
    )
    for row in cases:
        row["observation_utc"] = generated
        v5.attach_entry_quote(
            row,
            quotes.get(str(row.get("instrument") or "")) or {},
            observation_epoch=generated_epoch,
        )
        row["contract_id"] = CONTRACT_ID
        row["case_id"] = stable_case_id(row)
    if observed >= CONTRACT_ACTIVATED_UTC:
        history = record_cases(
            history_path,
            cases,
            recorded_utc=generated,
            meter_database=narrative_database,
        )
    else:
        history = {"inserted": 0, "total": 0}
    payload = {
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "contract_activated_utc": CONTRACT_ACTIVATED_UTC.isoformat(),
        "generated_utc": generated,
        "moves_generated_utc": moves_state.get("generated_utc"),
        "quotes_generated_utc": quotes_state.get("generated_utc"),
        "source_history_highwater_utc": source_highwater,
        "narrative_join_contract": {
            "meter_database": str(narrative_database.resolve()),
            "meter_contract_id": METER_CONTRACT_ID,
            "meter_activation_utc": METER_ACTIVATED_UTC.isoformat(),
            "pair_model_id": PAIR_MODEL_ID,
            "currency_direction_threshold": CURRENCY_DIRECTION_THRESHOLD,
            "pair_direction_threshold": PAIR_DIRECTION_THRESHOLD,
            "clock_semantics": "clock_utc_is_closed_bucket_end",
            "availability_semantics": (
                "latest complete bucket with clock_utc<=move_start and "
                "sealed_at_utc<=move_start"
            ),
            "partial_live_excluded": True,
            "sealed_v12_database_only": True,
            "research_only": True,
            "execution_eligible": False,
        },
        "narrative_meter_database_status": meter_status,
        "narrative_causally_available_mover_count": sum(
            bool((row.get("continuous_narrative_state") or {}).get("state_available"))
            for row in cases
        ),
        "narrative_unavailable_mover_count": sum(
            not bool((row.get("continuous_narrative_state") or {}).get("state_available"))
            for row in cases
        ),
        "factor_strength_history": str(factor_history_path.resolve()),
        "factor_strength_contract": {
            "source_contract_id": market_ticker.SCHEMA_VERSION,
            "expected_pair_observations": v5.FACTOR_STRENGTH_EXPECTED_OBSERVATIONS,
            "minimum_fresh_pair_observations": v5.FACTOR_STRENGTH_MIN_OBSERVATIONS,
            "maximum_pair_age_sec": v5.FACTOR_STRENGTH_MAX_PAIR_AGE_SEC,
            "declared_horizons_minutes": list(v5.FACTOR_STRENGTH_HORIZONS),
            "watermark": "at_or_before_move_end",
            "role": "after_the_fact_factor_clustering_only",
        },
        "factor_strength_surface_count": len(factor_surfaces),
        "causal_factor_strength_mover_count": sum(
            row.get("factor_primary_method") == "causal_all68_currency_strength"
            for row in cases
        ),
        "fallback_factor_mover_count": sum(
            row.get("factor_primary_method") == "time_bucket_token_recurrence_fallback"
            for row in cases
        ),
        "lookback_minutes": int(lookback_minutes),
        "mover_count": len(cases),
        "factor_episode_count": factor_episode_count,
        "factor_representative_count": sum(
            bool(row.get("factor_representative")) for row in cases
        ),
        "fresh_entry_quote_count": sum(
            bool(row.get("entry_quote_fresh")) for row in cases
        ),
        "strict_directional_mover_count": sum(
            row["strict_forward_alignment"] != "no_strict_direction" for row in cases
        ),
        "strict_aligned_mover_count": sum(
            row["strict_forward_alignment"] == "aligned" for row in cases
        ),
        "history_database": str(history_path.resolve()),
        "inserted_case_count": history["inserted"],
        "retained_case_count": history["total"],
        "movers": cases,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_promote": False,
        "broker_access": False,
        "supported_decision": "diagnostic_only",
    }
    v5.atomic_json(output_path, payload)
    v5.atomic_text(report_path, render_report(payload))
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--moves", type=Path, default=DEFAULT_MOVES)
    parser.add_argument("--sources", type=Path, default=DEFAULT_SOURCES)
    parser.add_argument(
        "--narrative-database", type=Path, default=DEFAULT_NARRATIVE_DATABASE
    )
    parser.add_argument("--quotes", type=Path, default=DEFAULT_QUOTES)
    parser.add_argument("--factor-history", type=Path, default=DEFAULT_FACTOR_HISTORY)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--history", type=Path, default=DEFAULT_HISTORY)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--lookback-minutes", type=int, default=120)
    parser.add_argument("--mover-count", type=int, default=10)
    parser.add_argument("--interval-sec", type=float, default=0.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    args = parser.parse_args()
    started = time.monotonic()
    while True:
        payload = run(
            moves_path=args.moves,
            source_database=args.sources,
            narrative_database=args.narrative_database,
            quotes_path=args.quotes,
            factor_history_path=args.factor_history,
            output_path=args.output,
            history_path=args.history,
            report_path=args.report,
            lookback_minutes=max(1, args.lookback_minutes),
            mover_count=max(1, args.mover_count),
        )
        print(
            json.dumps(
                {
                    "generated_utc": payload["generated_utc"],
                    "mover_count": payload["mover_count"],
                    "narrative_causally_available_mover_count": payload[
                        "narrative_causally_available_mover_count"
                    ],
                    "retained_case_count": payload["retained_case_count"],
                },
                sort_keys=True,
            ),
            flush=True,
        )
        if args.interval_sec <= 0:
            return 0
        if args.duration_sec > 0 and time.monotonic() - started >= args.duration_sec:
            return 0
        time.sleep(max(1.0, args.interval_sec))


if __name__ == "__main__":
    raise SystemExit(main())
