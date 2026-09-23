#!/usr/bin/env python3
"""Freeze the 2026-08-27 ECB accounts release and its all-68 response.

This is a point-in-time research diagnostic.  It records the official event
clock, local RSS/detail/fast-lane clocks, the last causally sealed V12 EUR
narrative state, pre-event technical state, and exact completed-M1 responses
at 1/5/15/30/60 minutes.  It cannot authorize, promote, submit, or close an
order.  A post-release move is not represented as a forecast or surprise.
"""

from __future__ import annotations

import argparse
import bisect
import datetime as dt
import hashlib
import json
import os
import sqlite3
import statistics
from pathlib import Path
from typing import Any, Mapping, Sequence

import oanda_current_week_move_case_audit_v1 as weekly


ROOT = Path(__file__).resolve().parent
STATE = ROOT / "data" / "oanda_training_manager" / "state"
NEWS = ROOT / "data" / "oanda_training_manager" / "local_news_sentiment"
REPORT_ROOT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "major_move_case_audits"
    / "ecb_accounts_20260827_v1"
)
OUTPUT_JSON = REPORT_ROOT / "ECB_ACCOUNTS_20260827_CASE.json"
OUTPUT_MD = REPORT_ROOT / "ECB_ACCOUNTS_20260827_CASE.md"
PROBE_JSONL = REPORT_ROOT / "ECB_ACCOUNTS_OFFICIAL_PROBE_V1.jsonl"
EVENT_UTC = dt.datetime(2026, 8, 27, 11, 30, tzinfo=dt.timezone.utc)
HORIZONS = (1, 5, 15, 30, 60)
CONTRACT_ID = "ecb_accounts_live_case_exact_completed_m1_v1_20260827"
CALENDAR_SOURCE = "ecb_monetary_policy_accounts_calendar_2026"
POLICY_BASELINES = ROOT / "config" / "official_policy_statement_baselines_v2_20260816.json"
UTC = dt.timezone.utc


def iso_utc(value: dt.datetime) -> str:
    return value.astimezone(UTC).isoformat()


def parse_utc(value: Any) -> dt.datetime:
    return weekly.parse_utc(str(value))


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def stable_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def _point_at_or_after(
    rows: Sequence[Mapping[str, Any]], at: dt.datetime, max_delay_seconds: int = 120
) -> Mapping[str, Any] | None:
    stamps = [row["timestamp"] for row in rows]
    index = bisect.bisect_left(stamps, at)
    if index >= len(rows):
        return None
    row = rows[index]
    if (row["timestamp"] - at).total_seconds() > max_delay_seconds:
        return None
    return row


def _point_before(
    rows: Sequence[Mapping[str, Any]], at: dt.datetime, max_delay_seconds: int = 180
) -> Mapping[str, Any] | None:
    """Return the completed candle whose close is at or before ``at``.

    Candle timestamps denote their open.  Looking strictly before the target
    makes a 11:30 -> 11:31 response use only the 11:30 candle, avoiding the
    extra-bar inclusion in a generic at-or-after endpoint lookup.
    """

    stamps = [row["timestamp"] for row in rows]
    index = bisect.bisect_left(stamps, at) - 1
    if index < 0:
        return None
    row = rows[index]
    expected_close = row["timestamp"] + dt.timedelta(minutes=1)
    if (at - expected_close).total_seconds() > max_delay_seconds:
        return None
    return row


def exact_pair_path(
    panel: Mapping[str, Sequence[Mapping[str, Any]]],
    instrument: str,
    start: dt.datetime,
    end: dt.datetime,
) -> dict[str, Any]:
    rows = panel.get(instrument) or []
    first = _point_at_or_after(rows, start)
    last = _point_before(rows, end)
    if first is None or last is None or last["timestamp"] < first["timestamp"]:
        raise weekly.WeeklyCaseError(
            f"exact_price_path_missing:{instrument}:{iso_utc(start)}:{iso_utc(end)}"
        )
    pip = weekly.pip_size(instrument)
    midpoint_pips = (float(last["mid_close"]) - float(first["mid_open"])) / pip
    observed_sign = 1 if midpoint_pips > 0 else -1 if midpoint_pips < 0 else 0
    long_net = (float(last["bid_close"]) - float(first["ask_open"])) / pip
    short_net = (float(first["bid_open"]) - float(last["ask_close"])) / pip
    return {
        "start_utc": iso_utc(first["timestamp"]),
        "target_utc": iso_utc(end),
        "last_completed_candle_utc": iso_utc(last["timestamp"]),
        "observed_side": (
            "long" if observed_sign > 0 else "short" if observed_sign < 0 else "flat"
        ),
        "midpoint_move_pips": round(midpoint_pips, 6),
        "long_after_cost_pips": round(long_net, 6),
        "short_after_cost_pips": round(short_net, 6),
        "observed_after_cost_pips": round(
            long_net if observed_sign > 0 else short_net if observed_sign < 0 else 0.0,
            6,
        ),
        "entry_spread_pips": round(
            (float(first["ask_open"]) - float(first["bid_open"])) / pip, 6
        ),
    }


def exact_factor_response(
    panel: Mapping[str, Sequence[Mapping[str, Any]]],
    start: dt.datetime,
    end: dt.datetime,
) -> dict[str, Any]:
    contributions: dict[str, list[float]] = {}
    pair_paths: list[dict[str, Any]] = []
    expected_instruments = sorted(panel)
    for instrument in expected_instruments:
        try:
            path = exact_pair_path(panel, instrument, start, end)
        except weekly.WeeklyCaseError:
            continue
        rows = panel[instrument]
        first = _point_at_or_after(rows, start)
        last = _point_before(rows, end)
        if first is None or last is None or float(first["mid_open"]) <= 0:
            continue
        move_bps = (
            float(last["mid_close"]) / float(first["mid_open"]) - 1.0
        ) * 10_000.0
        base, quote = instrument.split("_", 1)
        contributions.setdefault(base, []).append(move_bps)
        contributions.setdefault(quote, []).append(-move_bps)
        pair_paths.append({"instrument": instrument, **path, "move_bps": round(move_bps, 6)})
    scores = {
        currency: round(statistics.median(values), 6)
        for currency, values in contributions.items()
        if values
    }
    ranking = sorted(scores, key=lambda currency: (scores[currency], currency), reverse=True)
    return {
        "expected_pair_count": len(expected_instruments),
        "usable_pair_count": len(pair_paths),
        "missing_or_stale_pair_count": len(expected_instruments) - len(pair_paths),
        "missing_or_stale_pairs": sorted(
            set(expected_instruments) - {row["instrument"] for row in pair_paths}
        ),
        "currency_count": len(scores),
        "scores_bps": scores,
        "ranking_strongest_to_weakest": ranking,
        "all_pair_paths": pair_paths,
    }


def factor_delta_bps(
    post: Mapping[str, Any], pre: Mapping[str, Any], currency: str
) -> dict[str, float | None]:
    """Return a diagnostic equal-window factor comparison."""

    post_scores = post.get("scores_bps") or {}
    pre_scores = pre.get("scores_bps") or {}
    post_value = post_scores.get(currency)
    pre_value = pre_scores.get(currency)
    delta = (
        round(float(post_value) - float(pre_value), 6)
        if post_value is not None and pre_value is not None
        else None
    )
    return {
        "pre_event_equal_horizon_factor_bps": pre_value,
        "post_event_factor_bps": post_value,
        "post_minus_pre_factor_bps": delta,
    }


def _technical_pair_state(
    panel: Mapping[str, Sequence[Mapping[str, Any]]], instrument: str
) -> dict[str, Any]:
    rows = [row for row in panel.get(instrument, []) if row["timestamp"] < EVENT_UTC]
    closes = [float(row["mid_close"]) for row in rows]
    pip = weekly.pip_size(instrument)
    if len(closes) < 60:
        return {"instrument": instrument, "state": "insufficient_completed_m1_history"}
    last = closes[-1]
    sma5 = statistics.mean(closes[-5:])
    sma20 = statistics.mean(closes[-20:])
    sma60 = statistics.mean(closes[-60:])
    prior20 = closes[-21:-1]
    return {
        "instrument": instrument,
        "state": "ready",
        "last_mid_close": last,
        "sma5": round(sma5, 8),
        "sma20": round(sma20, 8),
        "sma60": round(sma60, 8),
        "sma5_minus_sma20_pips": round((sma5 - sma20) / pip, 6),
        "sma20_minus_sma60_pips": round((sma20 - sma60) / pip, 6),
        "return_5m_pips": round((last - closes[-6]) / pip, 6),
        "return_15m_pips": round((last - closes[-16]) / pip, 6),
        "return_60m_pips": round((last - closes[-60]) / pip, 6),
        "breakout_20m": (
            "up" if last > max(prior20) else "down" if last < min(prior20) else "inside"
        ),
        "trend_stack": (
            "up" if sma5 > sma20 > sma60 else "down" if sma5 < sma20 < sma60 else "mixed"
        ),
    }


def _read_rows(database: Path, sql: str, params: Sequence[Any]) -> list[dict[str, Any]]:
    if not database.exists():
        return []
    connection = sqlite3.connect(f"file:{database.resolve().as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        return [dict(row) for row in connection.execute(sql, tuple(params)).fetchall()]
    finally:
        connection.close()


def _decode_json_field(rows: list[dict[str, Any]], field: str) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        try:
            item[field] = json.loads(str(item.get(field) or "{}"))
        except json.JSONDecodeError:
            item[field] = {}
        output.append(item)
    return output


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            rows.append(value)
    return rows


def source_capture() -> dict[str, Any]:
    probe_rows = [
        row
        for row in _read_jsonl(PROBE_JSONL)
        if "mg260827" in str(row.get("url") or "").lower()
    ]
    calendar_rows = _decode_json_field(
        _read_rows(
            NEWS / "local_news_sentiment_v1.sqlite",
            """
            SELECT event_id,source_id,headline,published_utc,first_seen_utc,
                   source_url,payload_json
            FROM articles WHERE source_id=? ORDER BY first_seen_utc,event_id
            """,
            [CALENDAR_SOURCE],
        ),
        "payload_json",
    )
    rss_rows = _decode_json_field(
        _read_rows(
            NEWS / "local_news_sentiment_v1.sqlite",
            """
            SELECT event_id,source_id,headline,published_utc,first_seen_utc,
                   source_url,payload_json
            FROM articles
            WHERE source_id='ecb_press'
              AND (lower(headline) LIKE '%account of the monetary policy%'
                   OR lower(source_url) LIKE '%/press/accounts/%')
              AND first_seen_utc>=? AND first_seen_utc<?
            ORDER BY first_seen_utc,event_id
            """,
            [iso_utc(EVENT_UTC - dt.timedelta(hours=2)), iso_utc(EVENT_UTC + dt.timedelta(hours=4))],
        ),
        "payload_json",
    )
    fast_rows = _decode_json_field(
        _read_rows(
            NEWS / "official_release_fast_lane_v4.sqlite",
            """
            SELECT * FROM official_release_observation
            WHERE source_id='ecb_press' AND first_seen_utc>=? AND first_seen_utc<?
            ORDER BY first_seen_utc,observation_id
            """,
            [iso_utc(EVENT_UTC - dt.timedelta(hours=2)), iso_utc(EVENT_UTC + dt.timedelta(hours=4))],
        ),
        "raw_payload_json",
    )
    fast_rows = [
        row
        for row in fast_rows
        if "account of the monetary policy" in str(row["raw_payload_json"].get("title") or "").lower()
        or "/press/accounts/" in str(row["raw_payload_json"].get("url") or "").lower()
    ]
    observation_ids = [row["observation_id"] for row in fast_rows]
    mapping_rows: list[dict[str, Any]] = []
    if observation_ids:
        placeholders = ",".join("?" for _ in observation_ids)
        mapping_rows = _decode_json_field(
            _read_rows(
                NEWS / "official_release_fast_mapping_v3.sqlite",
                f"""
                SELECT * FROM official_release_mapping
                WHERE observation_id IN ({placeholders})
                ORDER BY mapped_utc,mapping_id
                """,
                observation_ids,
            ),
            "mapping_payload_json",
        )
    first_seen_values = [
        parse_utc(row["first_seen_utc"])
        for row in [*rss_rows, *fast_rows]
        if row.get("first_seen_utc")
    ]
    earliest = min(first_seen_values) if first_seen_values else None
    probe_seen = min(
        (parse_utc(row["observed_utc"]) for row in probe_rows if row.get("observed_utc")),
        default=None,
    )
    detail_seen = min(
        (
            parse_utc(row["raw_payload_json"]["detail_available_utc"])
            for row in fast_rows
            if row.get("raw_payload_json", {}).get("detail_available_utc")
        ),
        default=None,
    )
    mapped_seen = min(
        (parse_utc(row["mapped_utc"]) for row in mapping_rows if row.get("mapped_utc")),
        default=None,
    )
    broad_seen = min(
        (parse_utc(row["first_seen_utc"]) for row in rss_rows if row.get("first_seen_utc")),
        default=None,
    )
    return {
        "official_probe_rows": probe_rows,
        "calendar_rows": calendar_rows,
        "rss_detail_rows": rss_rows,
        "fast_lane_rows": fast_rows,
        "mapping_rows": mapping_rows,
        "earliest_release_seen_utc": iso_utc(earliest) if earliest else "",
        "release_latency_seconds": (
            round((earliest - EVENT_UTC).total_seconds(), 3) if earliest else None
        ),
        "official_probe_first_seen_utc": iso_utc(probe_seen) if probe_seen else "",
        "official_probe_latency_seconds": (
            round((probe_seen - EVENT_UTC).total_seconds(), 3) if probe_seen else None
        ),
        "production_detail_available_utc": iso_utc(detail_seen) if detail_seen else "",
        "production_detail_latency_seconds": (
            round((detail_seen - EVENT_UTC).total_seconds(), 3) if detail_seen else None
        ),
        "production_mapping_utc": iso_utc(mapped_seen) if mapped_seen else "",
        "production_mapping_latency_seconds": (
            round((mapped_seen - EVENT_UTC).total_seconds(), 3) if mapped_seen else None
        ),
        "broad_collector_first_seen_utc": iso_utc(broad_seen) if broad_seen else "",
        "broad_collector_latency_seconds": (
            round((broad_seen - EVENT_UTC).total_seconds(), 3) if broad_seen else None
        ),
        "production_minus_probe_latency_seconds": (
            round((earliest - probe_seen).total_seconds(), 3)
            if earliest and probe_seen
            else None
        ),
        "single_event_episode_id": "official_episode_" + stable_hash(["EUR", iso_utc(EVENT_UTC)])[:20],
        "deduplication_semantics": (
            "calendar clock and RSS/detail are one scheduled EUR event episode; "
            "they are not independent evidence"
        ),
    }


def prior_policy_baseline() -> dict[str, Any]:
    try:
        payload = json.loads(POLICY_BASELINES.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return {"available": False}
    row = next(
        (
            item
            for item in payload.get("baselines") or []
            if isinstance(item, Mapping)
            and item.get("currency") == "EUR"
            and item.get("source_id") == "ecb_press"
        ),
        None,
    )
    if not isinstance(row, Mapping):
        return {"available": False}
    summary = str(row.get("summary") or "")
    return {
        "available": True,
        "baseline_contract_id": payload.get("contract_id"),
        "published_utc": row.get("published_utc"),
        "known_utc": row.get("known_utc"),
        "headline": row.get("headline"),
        "source_url": row.get("source_url"),
        "raw_payload_sha256": row.get("raw_payload_sha256"),
        "summary_excerpt": summary[:1800],
        "use_policy": row.get("use_policy"),
    }


def sealed_narrative_before_event() -> dict[str, Any]:
    database = STATE / "continuous_narrative_meter_v12.sqlite"
    seals = _read_rows(
        database,
        """
        SELECT * FROM bucket_seals
        WHERE clock_utc<=? AND sealed_at_utc<=?
        ORDER BY clock_utc DESC LIMIT 1
        """,
        [iso_utc(EVENT_UTC), iso_utc(EVENT_UTC)],
    )
    if not seals:
        return {"available": False}
    seal = seals[0]
    rows = _decode_json_field(
        _read_rows(
            database,
            """
            SELECT currency,model_scores_json,attention_level,attention_acceleration,
                   new_story_count,active_story_count,source_family_count,agreement,
                   novelty,trusted_story_count,forward_timely_story_count,
                   evidence_class,created_utc
            FROM currency_meter WHERE clock_utc=? ORDER BY currency
            """,
            [seal["clock_utc"]],
        ),
        "model_scores_json",
    )
    return {
        "available": True,
        "seal": seal,
        "eur": next((row for row in rows if row["currency"] == "EUR"), None),
        "all_21_currency_rows": rows,
        "causal_rule": "clock_utc<=event_utc and sealed_at_utc<=event_utc",
    }


def v7_event_window() -> list[dict[str, Any]]:
    rows = _decode_json_field(
        _read_rows(
            STATE / "live_move_news_cases_v7r3.sqlite",
            """
            SELECT case_id,first_recorded_utc,instrument,start_utc,end_utc,case_json
            FROM mover_cases WHERE start_utc>=? AND start_utc<?
            ORDER BY start_utc,case_id
            """,
            [iso_utc(EVENT_UTC - dt.timedelta(minutes=5)), iso_utc(EVENT_UTC + dt.timedelta(minutes=65))],
        ),
        "case_json",
    )
    output: list[dict[str, Any]] = []
    for row in rows:
        case = row["case_json"]
        token = str(case.get("factor_primary_token") or "")
        if "EUR" not in str(row["instrument"]).split("_") and not token.startswith("EUR"):
            continue
        output.append(
            {
                "case_id": row["case_id"],
                "instrument": row["instrument"],
                "start_utc": row["start_utc"],
                "end_utc": row["end_utc"],
                "move_bps": case.get("move_bps"),
                "gross_pips": case.get("gross_pips"),
                "executable_net_pips": case.get("executable_net_pips"),
                "factor_primary_token": token,
                "factor_episode_id": case.get("factor_episode_id"),
                "factor_episode_canonical_root_id": case.get("factor_episode_canonical_root_id"),
                "factor_representative": case.get("factor_representative"),
                "factor_episode_member_count": case.get("factor_episode_member_count"),
                "forward_shadow_arms": case.get("forward_shadow_arms"),
                "strict_forward_side": case.get("strict_forward_side"),
                "strict_forward_alignment": case.get("strict_forward_alignment"),
                "continuous_narrative_state": case.get("continuous_narrative_state"),
                "explanation_state": case.get("explanation_state"),
                "research_only": True,
                "execution_eligible": False,
            }
        )
    return output


def compile_case(cutoff: dt.datetime | None = None) -> dict[str, Any]:
    frozen = (cutoff or dt.datetime.now(UTC)).astimezone(UTC)
    panel = weekly._load_candles(
        weekly.CANDLE_ROOT,
        EVENT_UTC - dt.timedelta(minutes=70),
        min(frozen, EVENT_UTC + dt.timedelta(minutes=65)),
    )
    technical_pairs = [
        _technical_pair_state(panel, instrument)
        for instrument in sorted(panel)
        if "EUR" in instrument.split("_")
    ]
    pre_factors: dict[str, Any] = {}
    for horizon in HORIZONS:
        try:
            pre_factors[str(horizon)] = exact_factor_response(
                panel, EVENT_UTC - dt.timedelta(minutes=horizon), EVENT_UTC
            )
        except weekly.WeeklyCaseError:
            pre_factors[str(horizon)] = {"maturity_state": "unavailable"}
    responses: list[dict[str, Any]] = []
    for horizon in HORIZONS:
        target = EVENT_UTC + dt.timedelta(minutes=horizon)
        if frozen < target + dt.timedelta(seconds=1):
            responses.append(
                {
                    "horizon_minutes": horizon,
                    "target_utc": iso_utc(target),
                    "maturity_state": "not_matured_at_frozen_cutoff",
                }
            )
            continue
        response = exact_factor_response(panel, EVENT_UTC, target)
        pre_response = pre_factors.get(str(horizon)) or {}
        eur_factor_delta = factor_delta_bps(response, pre_response, "EUR")
        ranking = response["ranking_strongest_to_weakest"]
        eur_rank = ranking.index("EUR") + 1 if "EUR" in ranking else 0
        eur_paths = [
            row for row in response["all_pair_paths"] if "EUR" in row["instrument"].split("_")
        ]
        eur_sign = 1 if float(response["scores_bps"].get("EUR", 0.0)) > 0 else -1
        for row in eur_paths:
            base, quote = row["instrument"].split("_")
            pair_sign = 1 if row["observed_side"] == "long" else -1 if row["observed_side"] == "short" else 0
            expressed_eur_sign = pair_sign if base == "EUR" else -pair_sign if quote == "EUR" else 0
            row["eur_factor_consistent"] = expressed_eur_sign == eur_sign
        consistent = [row for row in eur_paths if row["eur_factor_consistent"]]
        representative = max(
            consistent or eur_paths,
            key=lambda row: (float(row["observed_after_cost_pips"]), -float(row["entry_spread_pips"])),
            default=None,
        )
        responses.append(
            {
                "horizon_minutes": horizon,
                "target_utc": iso_utc(target),
                "maturity_state": "matured_completed_m1",
                "eur_factor_bps": response["scores_bps"].get("EUR"),
                "pre_event_equal_horizon_eur_factor_bps": (
                    eur_factor_delta["pre_event_equal_horizon_factor_bps"]
                ),
                "post_minus_pre_eur_factor_bps": (
                    eur_factor_delta["post_minus_pre_factor_bps"]
                ),
                "eur_rank_from_strongest": eur_rank,
                "eur_extreme_rank": (
                    min(eur_rank, len(ranking) - eur_rank + 1) if eur_rank else 0
                ),
                "usable_pair_count": response["usable_pair_count"],
                "expected_pair_count": response["expected_pair_count"],
                "missing_or_stale_pair_count": response["missing_or_stale_pair_count"],
                "missing_or_stale_pairs": response["missing_or_stale_pairs"],
                "currency_count": response["currency_count"],
                "currency_scores_bps": response["scores_bps"],
                "ranking_strongest_to_weakest": ranking,
                "cost_clearing_eur_consistent_pair_count": sum(
                    bool(row["eur_factor_consistent"])
                    and float(row["observed_after_cost_pips"]) > 0
                    for row in eur_paths
                ),
                "representative_eur_path": representative,
                "eur_pair_paths": eur_paths,
                "all_68_pair_paths": response["all_pair_paths"],
            }
        )
    return {
        "schema_version": 1,
        "contract_id": CONTRACT_ID,
        "generated_utc": iso_utc(dt.datetime.now(UTC)),
        "frozen_cutoff_utc": iso_utc(frozen),
        "event": {
            "event_name": "ECB Account of the Monetary Policy Meeting held on 22-23 July 2026",
            "event_series_id": "ecb_monetary_policy_accounts",
            "scheduled_utc": iso_utc(EVENT_UTC),
            "scheduled_local": "2026-08-27T13:30:00+02:00",
            "official_schedule_url": "https://www.ecb.europa.eu/press/calendars/weekly/html/index.en.html",
            "official_accounts_index_url": "https://www.ecb.europa.eu/press/accounts/html/index.en.html",
            "currency": "EUR",
            "causal_consensus_available": False,
            "causal_rate_repricing_available": False,
            "surprise_state": "unresolved_no_causal_consensus_or_rate_repricing",
            "pre_registered_direction": "neutral_unresolved",
            "incremental_information_test": (
                "compare the accounts with the already-known 23 July statement; "
                "newly disclosed dissent, option balance, and risk emphasis are diagnostic only"
            ),
        },
        "prior_policy_baseline": prior_policy_baseline(),
        "source_capture": source_capture(),
        "sealed_v12_pre_event": sealed_narrative_before_event(),
        "pre_event_technical": {
            "factor_windows_minutes": pre_factors,
            "eur_pair_states": technical_pairs,
            "indicator_contract": (
                "completed M1 only: 5/15/60m returns, SMA5/20/60 stack, "
                "20m breakout; all known strictly before 11:30 UTC"
            ),
        },
        "responses": responses,
        "v7r3_event_window_cases": v7_event_window(),
        "policy": {
            "research_only": True,
            "execution_eligible": False,
            "can_authorize": False,
            "can_promote": False,
            "post_hoc_direction_is_forecast": False,
            "pair_breadth_is_independent_evidence": False,
            "supported_decision": "diagnostic_only",
        },
    }


def render_markdown(payload: Mapping[str, Any]) -> str:
    event = payload["event"]
    source = payload["source_capture"]
    lines = [
        "# ECB monetary-policy accounts — live all-68 case",
        "",
        f"Frozen at `{payload['frozen_cutoff_utc']}` under `{payload['contract_id']}`.",
        "",
        "## Event and causal boundary",
        "",
        f"- Official clock: **{event['scheduled_utc']}** (13:30 CET/CEST schedule entry).",
        f"- Bounded official probe first saw the release at `{source.get('official_probe_first_seen_utc') or 'not seen'}`; latency `{source.get('official_probe_latency_seconds')}` seconds.",
        f"- Local release first seen: `{source['earliest_release_seen_utc'] or 'not yet seen'}`; latency `{source['release_latency_seconds']}` seconds.",
        f"- Production detail available at `{source.get('production_detail_available_utc') or 'not seen'}` (T+`{source.get('production_detail_latency_seconds')}`s); semantic mapping at `{source.get('production_mapping_utc') or 'not seen'}` (T+`{source.get('production_mapping_latency_seconds')}`s).",
        f"- Broad collector first seen at `{source.get('broad_collector_first_seen_utc') or 'not seen'}` (T+`{source.get('broad_collector_latency_seconds')}`s); production-minus-probe detection gap `{source.get('production_minus_probe_latency_seconds')}`s.",
        f"- Calendar rows: **{len(source['calendar_rows'])}**; ECB RSS/detail rows: **{len(source['rss_detail_rows'])}**; fast-lane rows: **{len(source['fast_lane_rows'])}**.",
        "- Causal consensus and rate repricing are unavailable; semantic interpretation and price response cannot establish surprise direction.",
        "- Calendar, RSS, detail, and mapping rows are one EUR event episode—not independent confirmations.",
        "",
        "## Fixed completed-M1 response",
        "",
        "| Horizon | State | EUR factor | Post−pre | Rank | Usable pairs | Cost-clearing EUR expressions | Representative path |",
        "|---:|---|---:|---:|---:|---:|---:|---|",
    ]
    for row in payload["responses"]:
        representative = row.get("representative_eur_path") or {}
        path = (
            f"{representative.get('instrument')} {representative.get('observed_side')} "
            f"{representative.get('observed_after_cost_pips'):+.2f}p"
            if representative
            else "—"
        )
        factor = row.get("eur_factor_bps")
        factor_delta = row.get("post_minus_pre_eur_factor_bps")
        lines.append(
            f"| {row['horizon_minutes']}m | {row['maturity_state']} | "
            f"{factor if factor is not None else '—'} | "
            f"{factor_delta if factor_delta is not None else '—'} | "
            f"{row.get('eur_rank_from_strongest','—')} | "
            f"{row.get('usable_pair_count','—')} | {row.get('cost_clearing_eur_consistent_pair_count','—')} | {path} |"
        )
    narrative = payload["sealed_v12_pre_event"]
    lines.extend(["", "## Pre-event state", ""])
    if narrative.get("available"):
        eur = narrative.get("eur") or {}
        lines.append(
            f"- Last sealed V12 bucket: `{narrative['seal']['clock_utc']}`, sealed `{narrative['seal']['sealed_at_utc']}`; "
            f"EUR narrative scores `{json.dumps(eur.get('model_scores_json') or {}, sort_keys=True)}`."
        )
    else:
        lines.append("- No causally sealed V12 bucket was available before the event.")
    lines.extend(
        [
            f"- V7R3 EUR/event-window mover cases retained: **{len(payload['v7r3_event_window_cases'])}**.",
            "- Technical inputs are completed-M1 5/15/60-minute returns, SMA5/20/60 stack, 20-minute breakout, and the all-68 cross-currency factor surface.",
            "",
            "## Interpretation guardrail",
            "",
            "This artifact is a research-only observation. It does not call the best post-release pair or horizon a forecast, does not infer a policy surprise without a causal expectation/rates benchmark, and cannot authorize an order.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cutoff-utc")
    args = parser.parse_args()
    cutoff = parse_utc(args.cutoff_utc) if args.cutoff_utc else None
    payload = compile_case(cutoff)
    payload["content_sha256"] = stable_hash(payload)
    atomic_text(OUTPUT_JSON, json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n")
    atomic_text(OUTPUT_MD, render_markdown(payload))
    print(json.dumps({
        "output_json": str(OUTPUT_JSON),
        "output_md": str(OUTPUT_MD),
        "frozen_cutoff_utc": payload["frozen_cutoff_utc"],
        "matured_horizons": [
            row["horizon_minutes"] for row in payload["responses"]
            if row["maturity_state"] == "matured_completed_m1"
        ],
        "release_first_seen_utc": payload["source_capture"]["earliest_release_seen_utc"],
        "sha256": payload["content_sha256"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
