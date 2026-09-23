#!/usr/bin/env python3
"""Build a frozen, read-only week-to-date Forex project recap.

The compiler regenerates the existing official-event response audit and the
retained live-move reconstruction at one explicit cutoff, then combines their
results with point-in-time operational/source snapshots.  It writes only
timestamped report artifacts.  It has no broker, lifecycle, authorization,
promotion, policy, or process-control surface.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import sqlite3
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

import oanda_current_week_official_event_response_audit_v1 as official_audit
import oanda_wtd_live_move_reconstruction as move_audit


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
STATE = DATA / "state"
REPORTS = DATA / "reports"
OUTPUT_DIRECTORY = REPORTS / "week_to_date_event_move_audit"
LIVE_CASE_DIRECTORY = REPORTS / "live_case_audits"
MAJOR_MOVE_CASE_DIRECTORY = REPORTS / "major_move_case_audits"
UTC = dt.timezone.utc
NEW_YORK = ZoneInfo("America/New_York")
CONTRACT_ID = (
    "week_to_date_project_event_move_recap_v5_cutoff_safe_snapshot_clocks_20260904"
)
DEFAULT_MATERIAL_THRESHOLD_BPS = 15.0
PRACTICE_ACCOUNT_ID = "101-001-37981792-007"
EXPECTED_CURRENCY_COUNT = 21
EXPECTED_INSTRUMENT_COUNT = 68
ACCOUNT_SNAPSHOT_LOG_GLOB = "account_snapshot_007_supervised_*.out.log"
NEWS_COLLECTOR_LOG_GLOB = "local_news_sentiment_supervised_*.out.log"
NARRATIVE_METER_SQLITE = STATE / "continuous_narrative_meter_v12.sqlite"

SNAPSHOT_PATHS = {
    "news_outcome": STATE / "news_outcome_improvement_audit_v2.json",
    "narrative_meter": STATE / "continuous_narrative_meter_v12.json",
    "account": STATE / "account_007_dashboard_v1.json",
    "source_coverage": DATA / "local_news_sentiment" / "source_coverage_latest.json",
    "central_bank_coverage": REPORTS
    / "official_central_bank_coverage"
    / "OFFICIAL_CENTRAL_BANK_COVERAGE_CURRENT.json",
    "economic_feed_completeness": REPORTS
    / "economic_feed_completeness"
    / "ECONOMIC_FEED_COMPLETENESS_CURRENT.json",
    "macro_consensus_access": STATE / "macro_consensus_access_audit_v1.json",
    "daily_rate_context": STATE / "official_daily_rate_context_v1.json",
    "clock_integrity": STATE / "clock_integrity_v1.json",
    "storage_headroom": STATE / "storage_headroom_v1.json",
    "project_integrity": STATE / "project_integrity_audit_v1.json",
}


class RecapError(RuntimeError):
    pass


def utc_now() -> dt.datetime:
    return dt.datetime.now(tz=UTC)


def parse_datetime(value: str) -> dt.datetime:
    parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def iso_utc(value: dt.datetime) -> str:
    return value.astimezone(UTC).isoformat()


def local_week_start(as_of: dt.datetime) -> dt.datetime:
    local = as_of.astimezone(NEW_YORK)
    monday = local - dt.timedelta(days=local.weekday())
    return monday.replace(hour=0, minute=0, second=0, microsecond=0)


def canonical_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")


def sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def atomic_write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_bytes(payload)
    os.replace(temporary, path)


def file_record(path: Path, payload: bytes | None = None) -> dict[str, Any]:
    raw = path.read_bytes() if payload is None else payload
    stat = path.stat()
    return {
        "path": str(path.resolve()),
        "sha256": sha256(raw),
        "bytes": len(raw),
        "modified_utc": iso_utc(dt.datetime.fromtimestamp(stat.st_mtime, tz=UTC)),
    }


def payload_clock(payload: Mapping[str, Any]) -> dt.datetime | None:
    # A mutable snapshot can describe an old market/source ``as_of_utc`` while
    # having been regenerated after the requested research cutoff.  The latter
    # is the relevant knowledge clock: accepting the stale as-of timestamp first
    # can admit a post-cutoff replacement (and, during collector startup, a
    # transient partial-coverage state) into a frozen report.  Prefer the
    # publication clocks and use ``as_of_utc`` only for legacy payloads that do
    # not expose when the snapshot itself was written.
    for key in ("generated_utc", "recorded_utc", "time", "as_of_utc"):
        value = payload.get(key)
        if not value:
            continue
        try:
            return parse_datetime(str(value))
        except (TypeError, ValueError):
            continue
    return None


def capture_json_snapshot(path: Path, cutoff: dt.datetime) -> dict[str, Any]:
    if not path.exists():
        return {
            "path": str(path.resolve()),
            "available": False,
            "cutoff_state": "missing",
            "payload": {},
        }
    raw = path.read_bytes()
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {
            **file_record(path, raw),
            "available": False,
            "cutoff_state": "invalid_json",
            "payload": {},
        }
    if not isinstance(payload, Mapping):
        payload = {}
    clock = payload_clock(payload)
    after_cutoff = clock is not None and clock > cutoff
    return {
        **file_record(path, raw),
        "available": not after_cutoff,
        "cutoff_state": "excluded_after_cutoff" if after_cutoff else "included",
        "state_clock_utc": iso_utc(clock) if clock else None,
        "payload": dict(payload) if not after_cutoff else {},
    }


def capture_state(cutoff: dt.datetime) -> dict[str, dict[str, Any]]:
    snapshots = {
        name: capture_json_snapshot(path, cutoff)
        for name, path in SNAPSHOT_PATHS.items()
    }
    # The account dashboard and V12 JSON are continuously replaced.  A report
    # launched just after an exact requested cutoff must not either lose these
    # fields or admit the replacement written after that cutoff.  Both workers
    # retain bounded append-only evidence from which an earlier snapshot can be
    # selected without changing the research cutoff.
    if not snapshots["account"].get("available"):
        fallback = capture_account_log_snapshot(cutoff)
        if fallback is not None:
            snapshots["account"] = fallback
    if not snapshots["narrative_meter"].get("available"):
        fallback = capture_narrative_meter_sqlite_snapshot(cutoff)
        if fallback is not None:
            snapshots["narrative_meter"] = fallback
    if (
        not snapshots["source_coverage"].get("available")
        or not snapshots["central_bank_coverage"].get("available")
    ):
        source_fallbacks = capture_collector_log_source_snapshots(cutoff)
        if not snapshots["source_coverage"].get("available") and source_fallbacks.get(
            "source_coverage"
        ):
            snapshots["source_coverage"] = source_fallbacks["source_coverage"]
        if not snapshots["central_bank_coverage"].get(
            "available"
        ) and source_fallbacks.get("central_bank_coverage"):
            snapshots["central_bank_coverage"] = source_fallbacks[
                "central_bank_coverage"
            ]
    return snapshots


def _bounded_snapshot_record(
    *,
    path: Path,
    payload: Mapping[str, Any],
    state_clock: dt.datetime,
    cutoff_state: str,
    selection_basis: str,
) -> dict[str, Any]:
    selected_bytes = canonical_bytes(payload)
    modified = (
        iso_utc(dt.datetime.fromtimestamp(path.stat().st_mtime, tz=UTC))
        if path.exists()
        else None
    )
    return {
        "path": str(path.resolve()),
        # Hash the selected bounded record, not the live backing file, because
        # the latter may legitimately contain later rows by report completion.
        "sha256": sha256(selected_bytes),
        "bytes": len(selected_bytes),
        "modified_utc": modified,
        "available": True,
        "cutoff_state": cutoff_state,
        "state_clock_utc": iso_utc(state_clock),
        "selection_basis": selection_basis,
        "payload": dict(payload),
    }


def capture_account_log_snapshot(
    cutoff: dt.datetime, logs_directory: Path | None = None
) -> dict[str, Any] | None:
    """Select the latest append-only Practice-007 record at/before cutoff."""

    logs = logs_directory or (DATA / "logs")
    selected: tuple[dt.datetime, Mapping[str, Any], Path] | None = None
    for path in logs.glob(ACCOUNT_SNAPSHOT_LOG_GLOB):
        try:
            lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            continue
        for line in lines:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(row, Mapping) or row.get("event") != "account_snapshot":
                continue
            try:
                clock = parse_datetime(str(row.get("time")))
            except (TypeError, ValueError):
                continue
            if clock > cutoff or (selected is not None and clock <= selected[0]):
                continue
            account = {
                "account_id": PRACTICE_ACCOUNT_ID,
                "env": "practice",
                "balance": row.get("balance"),
                "NAV": row.get("nav"),
                "pl": row.get("pl"),
                "unrealizedPL": row.get("unrealizedPL"),
                "openTradeCount": row.get("openTradeCount"),
                "pendingOrderCount": row.get("pendingOrderCount"),
                # The append-only status row does not carry marginUsed.  Zero
                # is only implied when the same bounded row proves no trades.
                "marginUsed": (
                    0.0 if int(row.get("openTradeCount") or 0) == 0 else None
                ),
                "ok": int(row.get("ok_count") or 0) > 0,
            }
            selected = (clock, {"time": iso_utc(clock), "accounts": [account]}, path)
    if selected is None:
        return None
    clock, payload, path = selected
    return _bounded_snapshot_record(
        path=path,
        payload=payload,
        state_clock=clock,
        cutoff_state="included_historical_account_log_record",
        selection_basis="latest_append_only_account_snapshot_at_or_before_cutoff",
    )


def capture_collector_log_source_snapshots(
    cutoff: dt.datetime, logs_directory: Path | None = None
) -> dict[str, dict[str, Any]]:
    """Recover broad/central coverage from one pre-cutoff collector record.

    Collector stdout is append-only and each completed status record embeds the
    exact broad-universe and central-bank health summaries then known.  Later
    lines in the same physical file are ignored, and the selected row—not the
    growing file—is hashed into report provenance.
    """

    logs = logs_directory or (DATA / "logs")
    selected: tuple[dt.datetime, Mapping[str, Any], Path] | None = None
    for path in logs.glob(NEWS_COLLECTOR_LOG_GLOB):
        try:
            lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            continue
        for line in lines:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(row, Mapping):
                continue
            broad = row.get("source_coverage")
            central = row.get("official_central_bank_coverage")
            if not isinstance(broad, Mapping) or not isinstance(central, Mapping):
                continue
            value = row.get("heartbeat_utc") or row.get("generated_utc")
            try:
                clock = parse_datetime(str(value))
            except (TypeError, ValueError):
                continue
            if clock > cutoff or (selected is not None and clock <= selected[0]):
                continue
            selected = (clock, row, path)
    if selected is None:
        return {}
    clock, row, path = selected
    broad_source = dict(row["source_coverage"])
    central_source = dict(row["official_central_bank_coverage"])
    broad_pair_count = int(broad_source.get("pair_count") or 0)
    broad_payload = {
        "as_of_utc": iso_utc(clock),
        "currency_count": broad_source.get("currency_count"),
        "pair_count": broad_source.get("pair_count"),
        "all_currencies_configured": broad_source.get(
            "all_currencies_configured"
        ),
        "all_pairs_emitted": broad_pair_count == EXPECTED_INSTRUMENT_COUNT,
        "pair_coverage_tier_counts": broad_source.get(
            "pair_coverage_tier_counts"
        ),
        "collector_contract_id": row.get("collector_contract_id"),
        "collector_schema_version": row.get("schema_version"),
        "pre_cutoff_collector_history": True,
    }
    central_payload = {
        "as_of_utc": iso_utc(clock),
        "contract_id": (
            "embedded_pre_cutoff_central_bank_coverage_from_"
            + str(row.get("collector_contract_id") or "collector")
        ),
        "contract_complete": central_source.get("contract_complete"),
        "minimum_operational_complete": central_source.get(
            "minimum_operational_complete"
        ),
        "fully_healthy": central_source.get("fully_healthy"),
        "currency_summary": central_source.get("currency_summary"),
        "pair_summary": central_source.get("pair_summary"),
        "global_blockers": central_source.get("global_blockers"),
        "pre_cutoff_collector_history": True,
        "non_event_structural_snapshot": False,
    }
    basis = "latest_append_only_collector_status_at_or_before_cutoff"
    return {
        "source_coverage": _bounded_snapshot_record(
            path=path,
            payload=broad_payload,
            state_clock=clock,
            cutoff_state="included_historical_collector_source_coverage",
            selection_basis=basis,
        ),
        "central_bank_coverage": _bounded_snapshot_record(
            path=path,
            payload=central_payload,
            state_clock=clock,
            cutoff_state="included_historical_collector_central_bank_coverage",
            selection_basis=basis,
        ),
    }


def capture_narrative_meter_sqlite_snapshot(
    cutoff: dt.datetime, sqlite_path: Path | None = None
) -> dict[str, Any] | None:
    """Rebuild compact V12 status from a bucket sealed by the cutoff.

    A bucket whose market clock equals the cutoff but which sealed afterward is
    deliberately excluded.  The query is read-only and all selected rows are
    bounded on both market clock and knowledge/seal time.
    """

    path = sqlite_path or NARRATIVE_METER_SQLITE
    if not path.exists():
        return None
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)
        connection.execute("BEGIN")
        seal = connection.execute(
            """
            SELECT meter_contract_id, clock_utc, sealed_at_utc,
                   seal_grace_seconds, currency_row_count, input_story_count
            FROM bucket_seals
            WHERE clock_utc <= ? AND sealed_at_utc <= ?
            ORDER BY clock_utc DESC, sealed_at_utc DESC
            LIMIT 1
            """,
            (iso_utc(cutoff), iso_utc(cutoff)),
        ).fetchone()
        if seal is None:
            return None
        contract_id, clock_text, sealed_text, grace, currency_rows, input_stories = seal
        clock = parse_datetime(str(clock_text))
        sealed_at = parse_datetime(str(sealed_text))
        meter_count, distinct_currencies = connection.execute(
            """
            SELECT COUNT(*), COUNT(DISTINCT currency)
            FROM currency_meter
            WHERE meter_contract_id = ? AND clock_utc = ? AND created_utc <= ?
            """,
            (contract_id, clock_text, iso_utc(cutoff)),
        ).fetchone()
        integrity_events = connection.execute(
            """
            SELECT COUNT(*) FROM seal_integrity_events
            WHERE meter_contract_id = ? AND detected_utc <= ?
            """,
            (contract_id, iso_utc(cutoff)),
        ).fetchone()[0]
        registry = connection.execute(
            """
            SELECT research_only, execution_eligible
            FROM meter_contract_registry WHERE meter_contract_id = ?
            """,
            (contract_id,),
        ).fetchone()
        research_only = bool(registry[0]) if registry else True
        execution_eligible = bool(registry[1]) if registry else False
    except (sqlite3.Error, TypeError, ValueError):
        return None
    finally:
        if connection is not None:
            connection.close()
    complete = (
        int(currency_rows or 0) == EXPECTED_CURRENCY_COUNT
        and int(meter_count or 0) == EXPECTED_CURRENCY_COUNT
        and int(distinct_currencies or 0) == EXPECTED_CURRENCY_COUNT
    )
    payload = {
        "meter_contract_id": contract_id,
        "generated_utc": iso_utc(sealed_at),
        "sealed_through_utc": iso_utc(clock),
        "seal_grace_seconds": grace,
        "currency_count": int(distinct_currencies or 0),
        "instrument_count": EXPECTED_INSTRUMENT_COUNT,
        "integrity_status": "ok" if complete and not integrity_events else "attention",
        "selection": {
            "input_story_count_at_sealed_clock": int(input_stories or 0),
            "snapshot_reconstructed_from_bounded_rows": True,
        },
        "research_only": research_only,
        "execution_eligible": execution_eligible,
    }
    return _bounded_snapshot_record(
        path=path,
        payload=payload,
        state_clock=sealed_at,
        cutoff_state="included_historical_sealed_meter_bucket",
        selection_basis=(
            "latest_bucket_with_clock_and_sealed_at_at_or_before_cutoff"
        ),
    )


def _compact_response(response: Mapping[str, Any]) -> dict[str, Any]:
    path = dict(response.get("representative_path") or {})
    post = dict(response.get("post_observation_path") or {})
    return {
        "horizon_minutes": response.get("horizon_minutes"),
        "maturity_utc": response.get("maturity_utc"),
        "maturity_state": response.get("maturity_state", "legacy_unspecified"),
        "currency_factor_bps": response.get("currency_factor_bps"),
        "currency_factor_rank_from_strongest": response.get(
            "currency_factor_rank_from_strongest"
        ),
        "currency_factor_extreme_rank": response.get("currency_factor_extreme_rank"),
        "usable_pair_count": response.get("usable_pair_count"),
        "cost_clearing_factor_consistent_pair_count": response.get(
            "cost_clearing_factor_consistent_pair_count"
        ),
        "instrument": path.get("instrument"),
        "observed_side": path.get("observed_side"),
        "start_utc": path.get("start_utc"),
        "end_utc": path.get("end_utc"),
        "signed_midpoint_move_pips": path.get("midpoint_move_pips"),
        "observed_after_cost_pips": path.get("observed_after_cost_pips"),
        "post_observation_after_cost_pips": post.get("observed_after_cost_pips"),
        "entry_spread_pips": path.get("entry_spread_pips"),
        "liquidity_cost_bucket": path.get("liquidity_cost_bucket"),
        "factor_consistent": path.get("factor_consistent"),
        "source_currency_dominant": path.get("source_currency_dominant"),
    }


def _explicit_causal_consensus(event: Mapping[str, Any]) -> bool:
    if event.get("consensus_value") is None:
        return False
    if event.get("consensus_observed_before_release") is True:
        return True
    observed = event.get("consensus_observed_utc")
    event_utc = event.get("event_utc")
    if not observed or not event_utc:
        return False
    try:
        return parse_datetime(str(observed)) < parse_datetime(str(event_utc))
    except ValueError:
        return False


def compact_official_events(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for event in payload.get("event_episode_representatives") or []:
        responses = [_compact_response(row) for row in event.get("responses") or []]
        best = event.get("best_retrospective_response")
        rows.append(
            {
                "event_episode_id": event.get("event_episode_id"),
                "event_id": event.get("event_id"),
                "episode_source_item_count": event.get("episode_source_item_count", 1),
                "event_utc": event.get("event_utc"),
                "clock_basis": event.get("clock_basis"),
                "first_seen_utc": event.get("first_seen_utc"),
                "source_latency_seconds": event.get("source_latency_seconds"),
                "source_id": event.get("source_id"),
                "source_url": event.get("source_url"),
                "currency": event.get("currency"),
                "category": event.get("category"),
                "headline": event.get("headline"),
                "actual_value": event.get("actual_value"),
                "consensus_value": event.get("consensus_value"),
                "previous_value": event.get("previous_value"),
                "causal_pre_release_consensus": _explicit_causal_consensus(event),
                "source_score": event.get("source_score"),
                "source_change_information": event.get("source_change_information"),
                "supporting_statistical_artifact": event.get(
                    "supporting_statistical_artifact"
                ),
                "direction_state": event.get("direction_state"),
                "calendar_only": bool(event.get("calendar_only")),
                "best_retrospective_response": _compact_response(best) if best else None,
                "fixed_horizon_responses": responses,
                "research_only": True,
                "execution_eligible": False,
                "selection_warning": (
                    "The best pair/side/horizon is selected after the outcome and is not a "
                    "forecast win or proof of source attribution."
                ),
            }
        )
    return rows


def compact_move_episode(row: Mapping[str, Any]) -> dict[str, Any]:
    tech = dict(row.get("causal_m1_technical_state") or {})
    story = dict(row.get("top_broad_story") or {})
    gross = row.get("gross_pips")
    net = row.get("executable_net_pips")
    cost = None
    if gross is not None and net is not None:
        cost = round(max(0.0, abs(float(gross)) - float(net)), 6)
    return {
        "factor_episode_id": row.get("factor_episode_id"),
        "factor": row.get("factor_primary_token"),
        "factor_category": row.get("factor_episode_category"),
        "factor_member_count": row.get("factor_episode_member_count"),
        "instrument": row.get("instrument"),
        "direction": row.get("move_direction"),
        "start_utc": row.get("start_utc"),
        "end_utc": row.get("end_utc"),
        "duration_minutes": row.get("duration_minutes"),
        "move_bps": row.get("move_bps"),
        "gross_pips": gross,
        "estimated_recorded_cost_pips": cost,
        "executable_net_pips": net,
        "broad_news_state": row.get("broad_news_state"),
        "broad_news_side": row.get("broad_news_side"),
        "strict_news_state": row.get("strict_news_state"),
        "strict_news_side": row.get("strict_news_side"),
        "strict_independent_story_count": row.get("strict_news_story_count"),
        "technical_trend_15m_state": row.get("technical_trend_15m_state"),
        "technical_breakout_20m_state": row.get("technical_breakout_20m_state"),
        "technical_exhaustion_60m_state": row.get(
            "technical_exhaustion_60m_state"
        ),
        "technical_at_move_start": {
            key: tech.get(key)
            for key in (
                "return_5m_pips",
                "return_15m_pips",
                "return_60m_pips",
                "velocity_5m_pips_per_min",
                "sma5_minus_sma20_pips",
                "sma20_minus_sma60_pips",
                "ema5_minus_ema20_pips",
                "atr14_pips",
                "range_position_60m",
                "spread_pips",
            )
        },
        "closest_broad_source": {
            "source_id": story.get("source_id"),
            "headline": story.get("headline"),
            "effective_from_utc": story.get("effective_from_utc"),
            "minutes_before_entry": story.get("minutes_before_entry"),
            "directional_publish_eligible": story.get(
                "directional_publish_eligible"
            ),
            "side": story.get("side"),
        }
        if story
        else None,
        "selection_warning": (
            "The live mover process detected the segment retrospectively; this is not a "
            "prospective entry record."
        ),
    }


def _snapshot_payload(snapshots: Mapping[str, Mapping[str, Any]], name: str) -> dict[str, Any]:
    snapshot = snapshots.get(name) or {}
    payload = snapshot.get("payload") or {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def compact_account(payload: Mapping[str, Any]) -> dict[str, Any]:
    account = dict((payload.get("accounts") or [{}])[0])
    return {
        "time": payload.get("time"),
        "account_id": account.get("account_id"),
        "environment": account.get("env"),
        "balance": account.get("balance"),
        "nav": account.get("NAV"),
        "cumulative_pl": account.get("pl"),
        "unrealized_pl": account.get("unrealizedPL"),
        "open_trades": account.get("openTradeCount"),
        "pending_orders": account.get("pendingOrderCount"),
        "margin_used": account.get("marginUsed"),
        "ok": account.get("ok"),
    }


def compact_source_readiness(
    snapshots: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    coverage = _snapshot_payload(snapshots, "source_coverage")
    central = _snapshot_payload(snapshots, "central_bank_coverage")
    economic = _snapshot_payload(snapshots, "economic_feed_completeness")
    consensus = _snapshot_payload(snapshots, "macro_consensus_access")
    rates = _snapshot_payload(snapshots, "daily_rate_context")
    meter = _snapshot_payload(snapshots, "narrative_meter")
    return {
        "broad_source_coverage": {
            "as_of_utc": coverage.get("as_of_utc"),
            "currency_count": coverage.get("currency_count"),
            "pair_count": coverage.get("pair_count"),
            "all_currencies_configured": coverage.get("all_currencies_configured"),
            "all_pairs_emitted": coverage.get("all_pairs_emitted"),
            "pair_coverage_tier_counts": coverage.get("pair_coverage_tier_counts"),
        },
        "official_central_bank_coverage": {
            "as_of_utc": central.get("as_of_utc"),
            "contract_id": central.get("contract_id"),
            "contract_complete": central.get("contract_complete"),
            "minimum_operational_complete": central.get(
                "minimum_operational_complete"
            ),
            "fully_healthy": central.get("fully_healthy"),
            "currency_summary": central.get("currency_summary"),
            "pair_summary": central.get("pair_summary"),
            "global_blockers": central.get("global_blockers"),
        },
        "economic_feed_completeness": {
            key: economic.get(key)
            for key in (
                "generated_utc",
                "currency_count",
                "future_event_clock_count",
                "minimum_feed_ready_count",
                "minimum_feed_ready_pair_legs",
                "total_pair_legs",
                "structured_numeric_parser_count",
                "actual_observation_currency_count",
                "prospective_actual_observation_currency_count",
                "causal_consensus_currency_count",
                "prospective_surprise_ready_count",
            )
        },
        "causal_consensus": {
            "status": consensus.get("status"),
            "accessible": consensus.get("causal_consensus_provider_accessible"),
            "unlock_requirement": consensus.get("unlock_requirement"),
        },
        "rate_context": {
            "generated_utc": rates.get("generated_utc"),
            "status": rates.get("status"),
            "currencies": (rates.get("totals") or {}).get("currencies"),
            "prospective_rows": (rates.get("totals") or {}).get("prospective_rows"),
            "intraday_rate_confirmation": rates.get("intraday_rate_confirmation"),
            "limitations": rates.get("limitations"),
        },
        "causal_narrative_meter": {
            "contract_id": meter.get("meter_contract_id"),
            "generated_utc": meter.get("generated_utc"),
            "sealed_through_utc": meter.get("sealed_through_utc"),
            "seal_grace_seconds": meter.get("seal_grace_seconds"),
            "currency_count": meter.get("currency_count"),
            "instrument_count": meter.get("instrument_count"),
            "integrity_status": meter.get("integrity_status"),
            "selection": meter.get("selection"),
            "research_only": meter.get("research_only"),
            "execution_eligible": meter.get("execution_eligible"),
        },
    }


def compact_news_scorecard(payload: Mapping[str, Any]) -> dict[str, Any]:
    summary = dict(payload.get("summary") or {})
    return {
        "recorded_utc": payload.get("recorded_utc") or payload.get("generated_utc"),
        "current_effective_theses": summary.get("current_effective_theses"),
        "wins": summary.get("wins"),
        "misses_or_gaps": summary.get("misses_or_gaps"),
        "reason_counts": summary.get("reason_counts") or {},
        "interpretation": (
            "These are diagnostic broad-context/event/technical classifications, not "
            "strict publishable event wins."
        ),
    }


def discover_live_cases(
    week_start_utc: dt.datetime, cutoff: dt.datetime
) -> list[dict[str, Any]]:
    rows = []
    candidates: set[Path] = set()
    for root in (LIVE_CASE_DIRECTORY, MAJOR_MOVE_CASE_DIRECTORY):
        if root.exists():
            candidates.update(path.resolve() for path in root.rglob("*.md"))
    # Release-specific case folders live beneath the recap directory.  Do not
    # rediscover the generated recap itself, which is written at the root.
    if OUTPUT_DIRECTORY.exists():
        candidates.update(
            path.resolve()
            for path in OUTPUT_DIRECTORY.rglob("*.md")
            if path.parent.resolve() != OUTPUT_DIRECTORY.resolve()
        )
    for path in sorted(candidates):
        modified = dt.datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
        raw = path.read_bytes()
        companion = path.with_suffix(".json")
        hash_manifest = path.with_suffix(".sha256")
        metadata: Mapping[str, Any] = {}
        evidence_cutoff: dt.datetime | None = None
        generated: dt.datetime | None = None
        if companion.exists():
            try:
                candidate_metadata = json.loads(companion.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                candidate_metadata = {}
            if isinstance(candidate_metadata, Mapping):
                metadata = candidate_metadata
                for key in ("cutoff_utc", "evidence_cutoff_utc", "frozen_cutoff_utc"):
                    if not metadata.get(key):
                        continue
                    try:
                        evidence_cutoff = parse_datetime(str(metadata[key]))
                    except ValueError:
                        pass
                    break
                if metadata.get("generated_utc"):
                    try:
                        generated = parse_datetime(str(metadata["generated_utc"]))
                    except ValueError:
                        pass
        # A diagnostic case can be compiled after the report cutoff while its
        # underlying evidence remains explicitly frozen at/before the cutoff.
        # Its inventory presence never changes event/move counts or attribution.
        if evidence_cutoff is not None:
            if not week_start_utc <= evidence_cutoff <= cutoff:
                continue
        elif not week_start_utc <= modified <= cutoff:
            continue
        first_line = raw.decode("utf-8", errors="replace").splitlines()[0:1]
        attribution = dict(metadata.get("attribution_policy") or {})
        research_only = bool(metadata.get("research_only", True))
        execution_eligible = bool(metadata.get("execution_eligible", False))
        directional_claimed = bool(attribution.get("directional_forecast_claimed", False))
        companion_record = file_record(companion) if companion.exists() else None
        manifest_record = file_record(hash_manifest) if hash_manifest.exists() else None
        rows.append(
            {
                **file_record(path, raw),
                "title": (first_line[0].lstrip("# ") if first_line else path.stem),
                "artifact_role": "diagnostic_case_inventory_only",
                "evidence_cutoff_utc": (
                    iso_utc(evidence_cutoff) if evidence_cutoff else None
                ),
                "artifact_generated_utc": iso_utc(generated) if generated else None,
                "artifact_generated_after_report_cutoff": bool(
                    generated is not None and generated > cutoff
                ),
                "research_only": research_only,
                "execution_eligible": execution_eligible,
                "directional_forecast_claimed": directional_claimed,
                "retrospective_directional_win_claimed": False,
                "companion_json": companion_record,
                "hash_manifest": manifest_record,
                "inventory_warning": (
                    "Inventory only: inclusion does not backdate production capture, "
                    "create source attribution, or count as a directional win."
                ),
            }
        )
    return rows


def _count_strict_directional_matches(episodes: Sequence[Mapping[str, Any]]) -> int:
    return sum(str(row.get("strict_news_state")) == "aligned" for row in episodes)


def compose_report(
    *,
    cutoff: dt.datetime,
    compiled_utc: dt.datetime,
    official: Mapping[str, Any],
    moves: Mapping[str, Any],
    snapshots: Mapping[str, Mapping[str, Any]],
    upstream_sources: Sequence[Mapping[str, Any]],
    live_cases: Sequence[Mapping[str, Any]],
    material_threshold_bps: float = DEFAULT_MATERIAL_THRESHOLD_BPS,
) -> dict[str, Any]:
    start_local = local_week_start(cutoff)
    start_utc = start_local.astimezone(UTC)
    official_events = compact_official_events(official)
    all_move_episodes = list(moves.get("episodes") or [])
    material_episodes = sorted(
        [
            compact_move_episode(row)
            for row in all_move_episodes
            if abs(float(row.get("move_bps") or 0.0)) >= material_threshold_bps
        ],
        key=lambda row: abs(float(row.get("move_bps") or 0.0)),
        reverse=True,
    )
    causal_consensus_count = sum(
        bool(row["causal_pre_release_consensus"]) for row in official_events
    )
    account = compact_account(_snapshot_payload(snapshots, "account"))
    report: dict[str, Any] = {
        "schema_version": 2,
        "contract_id": CONTRACT_ID,
        "generated_utc": iso_utc(compiled_utc),
        "frozen_cutoff_utc": iso_utc(cutoff),
        "week": {
            "definition": "America/New_York Monday 00:00 through frozen cutoff",
            "start_local": start_local.isoformat(),
            "start_utc": iso_utc(start_utc),
            "end_utc": iso_utc(cutoff),
        },
        "scope": {
            "instruments": 68,
            "currencies": 21,
            "account": "OANDA Practice 007",
            "official_event_definition": (
                "substantive verified official release or policy item, with calendar-only "
                "clocks labeled separately"
            ),
            "move_definition": moves.get("universe", {}).get(
                "clear_move_definition"
            ),
            "move_scope": moves.get("universe", {}).get("live_selection_scope"),
        },
        "executive_counts": {
            "official_source_items": official.get(
                "deduplicated_official_source_item_count", 0
            ),
            "independent_official_event_clocks": len(official_events),
            "calendar_only_event_clocks": sum(
                bool(row.get("calendar_only")) for row in official_events
            ),
            "official_event_clocks_with_posthoc_cost_clearing_response": official.get(
                "cost_clearing_event_count", 0
            ),
            "strict_timing_movement_candidates": official.get(
                "strict_attribution_candidate_count", 0
            ),
            "source_direction_resolved": official.get("direction_resolved_count", 0),
            "source_direction_aligned": official.get("direction_aligned_count", 0),
            "source_direction_opposed": official.get("direction_opposed_count", 0),
            "causal_pre_release_consensus_observations": causal_consensus_count,
            "physical_move_records": moves.get("universe", {}).get(
                "physical_record_count", 0
            ),
            "logical_move_cases": moves.get("universe", {}).get(
                "logical_raw_case_count", 0
            ),
            "factor_deduplicated_move_episodes": moves.get(
                "factor_assignment", {}
            ).get("factor_episode_count", len(all_move_episodes)),
            "material_factor_episodes": len(material_episodes),
            "material_threshold_bps": material_threshold_bps,
            "strict_publishable_directional_move_matches": _count_strict_directional_matches(
                all_move_episodes
            ),
            "live_case_audits": len(live_cases),
        },
        "official_event_clocks": official_events,
        "movement_census": {
            "contract_id": moves.get("contract_id"),
            "factor_contract_id": moves.get("factor_contract_id"),
            "universe": moves.get("universe"),
            "factor_assignment": moves.get("factor_assignment"),
            "magnitude_thresholds": moves.get("magnitude_thresholds"),
            "news_alignment": moves.get("news_mapping"),
            "technical_alignment": moves.get("technical_state"),
            "material_threshold_bps": material_threshold_bps,
            "material_episodes": material_episodes,
            "selection_warning": (
                "The retained universe is the live top-ten clear-mover selection per snapshot, "
                "not every possible 68-pair interval and not an independent episode sample."
            ),
        },
        "news_outcome_scorecard": compact_news_scorecard(
            _snapshot_payload(snapshots, "news_outcome")
        ),
        "source_readiness_and_gaps": compact_source_readiness(snapshots),
        "mapping_architecture": {
            "earlier": (
                "headline/article pool -> semantic currency score -> pair fan-out -> nearby "
                "mover/context join -> technical state -> fixed-window outcome"
            ),
            "current": (
                "original authority/vendor -> immutable publication/first-seen/revision clocks "
                "-> story/event dedup -> calendar/release/recap split -> semantic and publish "
                "gates -> sealed 21-currency meter -> base-minus-quote pair state -> all-68 "
                "causal factor/episode -> technical timing -> executable cost/path -> immutable "
                "outcome -> proof governance -> no_trade or exact canary"
            ),
            "current_controls": [
                "Official facts, broad context, strict direction, narrative state, and technical timing are separate evidence arms.",
                "Five-minute narrative clocks seal only after bucket close plus grace; partial live state is never proof eligible.",
                "Syndications collapse to story clusters and correlated pairs collapse to signed-currency episodes.",
                "Completed-M1 features exclude the still-open bar and bind to the move-start knowledge clock.",
                "Economics use executable bid/ask cost, MFE/MAE, latency, cost-clearance time, and giveback.",
                "Retrospective best pair/horizon selection is labeled discovery and cannot authorize an entry.",
            ],
        },
        "indicators_attached_to_move_clocks": [
            "completed-M1 5/15/60-minute return and five-minute velocity",
            "SMA5-SMA20, SMA20-SMA60, EMA5-EMA20, 20-minute breakout, 60-minute range/exhaustion, ATR14",
            "all-68 least-squares currency strength, factor breadth, residual, coverage watermark, and factor conflict",
            "executable bid/ask spread and liquidity/cost bucket",
            "MFE, MAE, cost-clearance time, order-latency/missed-entry proxy, and giveback",
            "quote-update intensity and executable-quote imbalance over 5/30/120 seconds in detailed live cases",
            "shadow trend, breakout, reversion, cross-sectional, pattern, Kalman/Markov/AR, ridge, tabular, graph-transfer, and state-space families (not independent votes)",
        ],
        "operational_snapshot": {
            "account": account,
            "clock_integrity": {
                key: _snapshot_payload(snapshots, "clock_integrity").get(key)
                for key in (
                    "generated_utc",
                    "status",
                    "host_clock_synchronized",
                    "timestamp_normalization_trusted",
                )
            },
            "project_integrity": {
                key: _snapshot_payload(snapshots, "project_integrity").get(key)
                for key in ("generated_utc", "status", "failures")
            },
            "storage": {
                "generated_utc": _snapshot_payload(
                    snapshots, "storage_headroom"
                ).get("generated_utc"),
                "status": _snapshot_payload(snapshots, "storage_headroom").get(
                    "status"
                ),
                "disk": _snapshot_payload(snapshots, "storage_headroom").get("disk"),
                "automatic_evidence_deletion": _snapshot_payload(
                    snapshots, "storage_headroom"
                ).get("automatic_evidence_deletion"),
                "automatic_vacuum": _snapshot_payload(
                    snapshots, "storage_headroom"
                ).get("automatic_vacuum"),
            },
        },
        "live_case_audits": list(live_cases),
        "remaining_evidence_gaps": [
            "Causal pre-release consensus is absent unless an event contains an explicit pre-release observation clock.",
            "Daily rates are context, not timestamp-safe intraday OIS/futures repricing around releases.",
            "Broad story proximity cannot establish causality; many matches remain recaps, repeats, late arrivals, or unrelated context.",
            "A causal intraday oil/commodity transmission path remains missing for NOK and high-beta attribution.",
            "Independent prospectively frozen event repetitions remain too sparse for a tradable news-plus-technical rule.",
        ],
        "input_snapshots": [
            {
                key: snapshot.get(key)
                for key in (
                    "path",
                    "sha256",
                    "bytes",
                    "modified_utc",
                    "state_clock_utc",
                    "available",
                    "cutoff_state",
                    "selection_basis",
                )
            }
            for snapshot in snapshots.values()
        ],
        "upstream_artifacts": list(upstream_sources),
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_promote": False,
        "supported_decision": "no_trade",
    }
    report["payload_without_self_hash_sha256"] = sha256(canonical_bytes(report))
    return report


def _fmt(value: Any, digits: int = 1, signed: bool = True) -> str:
    if value is None:
        return "—"
    try:
        sign = "+" if signed else ""
        return f"{float(value):{sign}.{digits}f}"
    except (TypeError, ValueError):
        return str(value)


def _cell(value: Any, limit: int = 72) -> str:
    if value is None or value == "":
        return "—"
    text = str(value).replace("|", "/").replace("\n", " ").strip()
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _latency(value: Any) -> str:
    if value is None:
        return "unknown"
    seconds = float(value)
    if seconds < 0:
        return f"pre-known {abs(seconds) / 86400:.1f}d"
    return f"+{seconds:.0f}s" if seconds < 300 else f"+{seconds / 60:.1f}m"


def render_markdown(report: Mapping[str, Any]) -> str:
    ec = dict(report["executive_counts"])
    account = dict(report["operational_snapshot"]["account"])
    movement = dict(report["movement_census"])
    news = dict(movement.get("news_alignment") or {})
    tech = dict(movement.get("technical_alignment") or {})
    broad = dict(news.get("broad") or {})
    strict = dict(news.get("strict") or {})
    lines = [
        "# Frozen week-to-date Forex project, event, and movement recap",
        "",
        f"Compiled: `{report['generated_utc']}`  ",
        f"Frozen cutoff: `{report['frozen_cutoff_utc']}`  ",
        f"Week: `{report['week']['start_local']}` through the frozen cutoff.",
        "",
        "> **Supported decision: `no_trade`.** This report is retrospective research only. It cannot place orders, promote a hypothesis, change policy, or authorize Practice 007.",
        "",
        "## Executive result",
        "",
        f"- Official source items: **{ec['official_source_items']}**, collapsed to **{ec['independent_official_event_clocks']}** independent five-minute currency/event clocks.",
        f"- Calendar-only clocks: **{ec['calendar_only_event_clocks']}**; these prove schedule coverage, not release observation.",
        f"- Official clocks with any post-hoc cost-clearing path: **{ec['official_event_clocks_with_posthoc_cost_clearing_response']}**.",
        f"- Strict timing/movement candidates: **{ec['strict_timing_movement_candidates']}**; source direction aligned/opposed: **{ec['source_direction_aligned']}/{ec['source_direction_opposed']}**.",
        f"- Causally clocked pre-release consensus observations: **{ec['causal_pre_release_consensus_observations']}**.",
        f"- Move history: **{ec['physical_move_records']}** physical rows, **{ec['logical_move_cases']}** logical cases, **{ec['factor_deduplicated_move_episodes']}** factor episodes.",
        f"- Material episodes >= {ec['material_threshold_bps']:.1f} bps: **{ec['material_factor_episodes']}**; strict pre-move directional matches: **{ec['strict_publishable_directional_move_matches']}**.",
        f"- Practice 007: balance/NAV **${account.get('balance')}/${account.get('nav')}**, cumulative P/L **${account.get('cumulative_pl')}**, open trades/orders **{account.get('open_trades')}/{account.get('pending_orders')}**.",
        "",
        "A cost-clearing move after an official clock is not automatically a news forecast win. Pair, side, and horizon are selected after the outcome unless the strict source-direction and causal-timing fields say otherwise.",
        "",
        "## All independent official event clocks",
        "",
        "| # | Clock UTC | CCY | Source / event | Timing | A / causal C / P | Direction | Best post-hoc path | Factor | Gross | Spread | Net |",
        "|---:|---|---|---|---|---|---|---|---:|---:|---:|---:|",
    ]
    for index, event in enumerate(report["official_event_clocks"], 1):
        best = dict(event.get("best_retrospective_response") or {})
        values = (
            f"{_cell(event.get('actual_value'), 10)} / "
            f"{_cell(event.get('consensus_value'), 10) if event.get('causal_pre_release_consensus') else '—'} / "
            f"{_cell(event.get('previous_value'), 10)}"
        )
        label = f"{_cell(event.get('source_id'), 26)} — {_cell(event.get('headline'), 54)}"
        path = (
            f"{_cell(best.get('instrument'), 12)} {_cell(best.get('observed_side'), 5)} "
            f"{_cell(best.get('horizon_minutes'), 4)}m"
        )
        lines.append(
            f"| {index} | `{event.get('event_utc')}` | {event.get('currency')} | {label} | {_latency(event.get('source_latency_seconds'))}; {_cell(event.get('clock_basis'), 24)} | {values} | {_cell(event.get('direction_state'), 28)} | {path} | {_fmt(best.get('currency_factor_bps'), 2)} bps | {_fmt(best.get('signed_midpoint_move_pips'), 1)}p | {_fmt(best.get('entry_spread_pips'), 1, False)}p | **{_fmt(best.get('observed_after_cost_pips'), 1)}p** |"
        )
    if not report["official_event_clocks"]:
        lines.append("| — | — | — | No substantive official clock observed | — | — | — | — | — | — | — | — |")

    thresholds = dict(movement.get("magnitude_thresholds") or {})
    lines += [
        "",
        "The companion JSON retains all fixed 5/15/60/120-minute responses and explicitly labels horizons that had not matured at the cutoff.",
        "",
        "## Factor-deduplicated movement census",
        "",
        f"Magnitude counts: >=5 bps **{thresholds.get('gte_5_bps', 0)}**, >=10 **{thresholds.get('gte_10_bps', 0)}**, >=15 **{thresholds.get('gte_15_bps', 0)}**, >=20 **{thresholds.get('gte_20_bps', 0)}**, >=25 **{thresholds.get('gte_25_bps', 0)}**.",
        "",
        f"Broad-context alignment: **{broad.get('aligned', 0)} aligned / {broad.get('opposed', 0)} opposed / {broad.get('neutral_or_conflicted', 0)} neutral-conflicted / {broad.get('no_relevant_source', 0)} no source**. Strict alignment: **{strict.get('aligned', 0)} aligned / {strict.get('opposed', 0)} opposed / {strict.get('neutral_or_conflicted', 0)} neutral-conflicted / {strict.get('no_relevant_source', 0)} no source**.",
        "",
        f"### Complete >= {movement['material_threshold_bps']:.1f} bps set at the cutoff",
        "",
        "| # | Factor | Pair | Start -> end UTC | Move | Gross | Cost | Net | Broad / strict | Trend / breakout / exhaustion | Closest broad source |",
        "|---:|---|---|---|---:|---:|---:|---:|---|---|---|",
    ]
    for index, row in enumerate(movement["material_episodes"], 1):
        source = dict(row.get("closest_broad_source") or {})
        technical = (
            f"{row.get('technical_trend_15m_state')} / "
            f"{row.get('technical_breakout_20m_state')} / "
            f"{row.get('technical_exhaustion_60m_state')}"
        )
        lines.append(
            f"| {index} | {row.get('factor')} | {row.get('instrument')} | `{_cell(row.get('start_utc'), 27)}` -> `{_cell(row.get('end_utc'), 27)}` | {_fmt(row.get('move_bps'), 2)} bps | {_fmt(row.get('gross_pips'), 1)}p | {_fmt(row.get('estimated_recorded_cost_pips'), 1, False)}p | **{_fmt(row.get('executable_net_pips'), 1)}p** | {row.get('broad_news_state')} / {row.get('strict_news_state')} | {technical} | {_cell(source.get('headline'), 60)} |"
        )
    if not movement["material_episodes"]:
        lines.append("| — | — | — | — | — | — | — | — | — | — | No retained episode met the threshold |")

    score = dict(report["news_outcome_scorecard"])
    reasons = dict(score.get("reason_counts") or {})
    trend = dict(tech.get("trend_15m_vs_move") or {})
    breakout = dict(tech.get("breakout_20m_vs_move") or {})
    lines += [
        "",
        "## News and technical scorecard",
        "",
        f"The retained on-demand audit has **{score.get('current_effective_theses')}** effective theses, **{score.get('wins')}** diagnostic wins, and **{score.get('misses_or_gaps')}** misses/gaps. These are not strict publishable event wins.",
        "",
        f"Largest overlapping failure labels: strict signal absent **{reasons.get('strict_signal_absence', 0)}**; no after-cost edge **{reasons.get('no_economic_edge', 0)}**; bad direction/entry **{reasons.get('bad_direction_or_entry', 0)}**; verification gap **{reasons.get('verification_gap', 0)}**; latency/decay **{reasons.get('latency_or_decay_gap', 0)}**; giveback/reversal **{reasons.get('giveback_or_reversal', 0)}**; directional mapping gap **{reasons.get('directional_mapping_gap', 0)}**.",
        "",
        f"Completed-M1 descriptive labels: 15-minute trend **{trend.get('aligned', 0)} aligned / {trend.get('opposed', 0)} opposed / {trend.get('neutral', 0)} neutral**; 20-minute breakout **{breakout.get('inside', 0)} inside / {breakout.get('opposed', 0)} opposed / {breakout.get('aligned', 0)} aligned**. Swing-start selection mechanically biases the trend comparison; it is not forecast accuracy.",
        "",
        "### Indicators attached to causal move clocks",
        "",
    ]
    lines.extend(f"- {item}" for item in report["indicators_attached_to_move_clocks"])

    readiness = dict(report["source_readiness_and_gaps"])
    broad_ready = dict(readiness["broad_source_coverage"])
    central = dict(readiness["official_central_bank_coverage"])
    ccy_summary = dict(central.get("currency_summary") or {})
    pair_summary = dict(central.get("pair_summary") or {})
    economic = dict(readiness["economic_feed_completeness"])
    rates = dict(readiness["rate_context"])
    meter = dict(readiness["causal_narrative_meter"])
    lines += [
        "",
        "## Source readiness and unresolved gaps",
        "",
        f"- Broad live source map: **{broad_ready.get('currency_count')}/21 currencies**, **{broad_ready.get('pair_count')}/68 pairs**; all currencies configured `{broad_ready.get('all_currencies_configured')}`, all pairs emitted `{broad_ready.get('all_pairs_emitted')}`.",
        f"- Central-bank map: configured **{ccy_summary.get('configured_complete')}/21**, release-operational **{ccy_summary.get('release_operational')}/21**, release-healthy **{ccy_summary.get('release_healthy')}/21**; both legs operational **{pair_summary.get('both_legs_operational')}/68**.",
        f"- Economic minimum: future clocks **{economic.get('future_event_clock_count')}/21**, numeric parsers **{economic.get('structured_numeric_parser_count')}/21**, actual-observation currencies **{economic.get('actual_observation_currency_count')}/21**, prospectively observed actuals **{economic.get('prospective_actual_observation_currency_count')}/21**.",
        f"- Causal consensus: **{economic.get('causal_consensus_currency_count')}/21**; prospective surprise-ready: **{economic.get('prospective_surprise_ready_count')}/21**; provider state `{readiness['causal_consensus'].get('status')}`.",
        f"- Rate context: **{rates.get('currencies')} currencies**, **{rates.get('prospective_rows')} prospective rows**, intraday event-time confirmation `{rates.get('intraday_rate_confirmation')}`.",
        f"- V12 narrative meter: **{meter.get('currency_count')}/21 currencies**, **{meter.get('instrument_count')}/68 pairs**, sealed through `{meter.get('sealed_through_utc')}`, integrity `{meter.get('integrity_status')}`; research-only `{meter.get('research_only')}`.",
        "",
    ]
    lines.extend(f"- {gap}" for gap in report["remaining_evidence_gaps"])

    architecture = dict(report["mapping_architecture"])
    lines += [
        "",
        "## Mapping architecture: before versus current",
        "",
        "### Earlier path",
        "",
        f"`{architecture['earlier']}`",
        "",
        "### Current governed path",
        "",
        f"`{architecture['current']}`",
        "",
    ]
    lines.extend(
        f"{index}. {item}"
        for index, item in enumerate(architecture["current_controls"], 1)
    )

    lines += ["", "## Material live case-audit inventory", ""]
    if report["live_case_audits"]:
        for case in report["live_case_audits"]:
            timing = ""
            if case.get("evidence_cutoff_utc"):
                timing = (
                    f"; evidence cutoff `{case.get('evidence_cutoff_utc')}`; "
                    f"generated after report cutoff "
                    f"`{case.get('artifact_generated_after_report_cutoff')}`"
                )
            lines.append(
                f"- `{case.get('title')}` — `{case.get('path')}` — SHA-256 "
                f"`{case.get('sha256')}`{timing}; diagnostic inventory only, "
                "not a directional win or backdated production capture."
            )
            companion = dict(case.get("companion_json") or {})
            manifest = dict(case.get("hash_manifest") or {})
            if companion:
                lines.append(
                    f"  - Companion JSON `{companion.get('path')}` — SHA-256 "
                    f"`{companion.get('sha256')}`"
                )
            if manifest:
                lines.append(
                    f"  - Hash manifest `{manifest.get('path')}` — SHA-256 "
                    f"`{manifest.get('sha256')}`"
                )
    else:
        lines.append("- No separately written live case audit existed in this frozen window.")

    integrity = dict(report["operational_snapshot"]["project_integrity"])
    storage = dict(report["operational_snapshot"]["storage"])
    disk = dict(storage.get("disk") or {})
    lines += [
        "",
        "## Operational boundary",
        "",
        f"- Account `{account.get('account_id')}` is `{account.get('environment')}` only; open trades/orders `{account.get('open_trades')}/{account.get('pending_orders')}` and margin used `{account.get('margin_used')}`.",
        "- No report, source collector, meter, outcome worker, or research model can place or close a trade or self-promote.",
        "- New practice entry still requires an exact fresh authorization plus independent lifecycle confirmation; real-money routing remains disabled.",
        f"- Captured project-integrity state: `{integrity.get('status')}`. Captured storage state: `{storage.get('status')}`; free space `{disk.get('free_gib')}` GiB.",
        "- A stale or post-cutoff state snapshot is excluded and labeled in the companion JSON rather than silently mixed into the report.",
        "",
        "## Evidence artifacts",
        "",
    ]
    for source in report["upstream_artifacts"]:
        lines.append(
            f"- `{source.get('path')}` — SHA-256 `{source.get('sha256')}`"
        )
    for source in report["input_snapshots"]:
        lines.append(
            f"- `{source.get('path')}` — `{source.get('cutoff_state')}`"
            f"{'; ' + str(source.get('selection_basis')) if source.get('selection_basis') else ''}"
            f" — selected-record SHA-256 `{source.get('sha256', 'unavailable')}`"
        )
    lines += [
        "",
        "The companion JSON retains every official event clock and fixed declared horizon, plus every material factor episode and its causal news/technical state. Full sub-threshold move rows remain in the hashed WTD reconstruction artifact.",
        "",
    ]
    return "\n".join(lines)


def write_report(
    report: Mapping[str, Any], output_directory: Path = OUTPUT_DIRECTORY
) -> dict[str, Any]:
    cutoff = parse_datetime(str(report["frozen_cutoff_utc"]))
    stamp = cutoff.strftime("%Y%m%dT%H%M%SZ")
    base = f"WEEK_TO_DATE_PROJECT_EVENT_MOVE_RECAP_{stamp}"
    json_path = output_directory / f"{base}.json"
    markdown_path = output_directory / f"{base}.md"
    hash_path = output_directory / f"{base}.sha256"
    json_bytes = (json.dumps(report, indent=2, ensure_ascii=False, sort_keys=True) + "\n").encode(
        "utf-8"
    )
    markdown_bytes = (render_markdown(report) + "\n").encode("utf-8")
    atomic_write(json_path, json_bytes)
    atomic_write(markdown_path, markdown_bytes)
    hashes = (
        f"{sha256(markdown_bytes)} *{markdown_path.name}\n"
        f"{sha256(json_bytes)} *{json_path.name}\n"
    ).encode("ascii")
    atomic_write(hash_path, hashes)
    return {
        "json": str(json_path.resolve()),
        "markdown": str(markdown_path.resolve()),
        "sha256": str(hash_path.resolve()),
        "json_sha256": sha256(json_bytes),
        "markdown_sha256": sha256(markdown_bytes),
    }


def run(
    *,
    cutoff: dt.datetime | None = None,
    output_directory: Path = OUTPUT_DIRECTORY,
    material_threshold_bps: float = DEFAULT_MATERIAL_THRESHOLD_BPS,
) -> tuple[dict[str, Any], dict[str, Any]]:
    frozen = (cutoff or utc_now()).astimezone(UTC)
    if frozen > utc_now() + dt.timedelta(seconds=1):
        raise RecapError("future_cutoff_not_allowed")
    start_local = local_week_start(frozen)
    start_utc = start_local.astimezone(UTC)

    # Capture volatile status files first.  Expensive audit compilation cannot
    # move their evidence clocks beyond the declared cutoff afterward.
    snapshots = capture_state(frozen)
    official = official_audit.compile_audit(
        official_audit.NEWS_DB,
        official_audit.CANDLE_ROOT,
        week_start=start_utc,
        week_end=frozen,
    )
    moves = move_audit.run(as_of=frozen)
    move_path = Path(str((moves.get("artifacts") or {}).get("json")))
    upstream = [
        {
            "path": "embedded:official_event_response_audit",
            "sha256": sha256(canonical_bytes(official)),
            "contract_id": official.get("contract_id"),
            "frozen_cutoff_utc": official.get("frozen_cutoff_utc")
            or official.get("week_end_utc"),
        },
        {
            **file_record(move_path),
            "contract_id": moves.get("contract_id"),
            "frozen_cutoff_utc": moves.get("generated_utc"),
        },
    ]
    live_cases = discover_live_cases(start_utc, frozen)
    compiled = utc_now()
    report = compose_report(
        cutoff=frozen,
        compiled_utc=compiled,
        official=official,
        moves=moves,
        snapshots=snapshots,
        upstream_sources=upstream,
        live_cases=live_cases,
        material_threshold_bps=material_threshold_bps,
    )
    artifacts = write_report(report, output_directory)
    return report, artifacts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--as-of-utc",
        default="",
        help="Frozen inclusive evidence cutoff; defaults to the invocation clock.",
    )
    parser.add_argument(
        "--output-directory", type=Path, default=OUTPUT_DIRECTORY
    )
    parser.add_argument(
        "--material-threshold-bps",
        type=float,
        default=DEFAULT_MATERIAL_THRESHOLD_BPS,
    )
    args = parser.parse_args()
    report, artifacts = run(
        cutoff=parse_datetime(args.as_of_utc) if args.as_of_utc else None,
        output_directory=args.output_directory,
        material_threshold_bps=args.material_threshold_bps,
    )
    print(
        json.dumps(
            {
                "contract_id": report["contract_id"],
                "frozen_cutoff_utc": report["frozen_cutoff_utc"],
                "official_event_clocks": report["executive_counts"][
                    "independent_official_event_clocks"
                ],
                "factor_episodes": report["executive_counts"][
                    "factor_deduplicated_move_episodes"
                ],
                "material_factor_episodes": report["executive_counts"][
                    "material_factor_episodes"
                ],
                "strict_directional_matches": report["executive_counts"][
                    "strict_publishable_directional_move_matches"
                ],
                "supported_decision": report["supported_decision"],
                "artifacts": artifacts,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
