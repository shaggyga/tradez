#!/usr/bin/env python3
"""Append an on-demand review of matured FX-news hits, misses, and mover gaps.

The review is deliberately downstream of the immutable signal/news and live
mover ledgers.  It opens both inputs read-only, collapses pair fan-out to one
event/factor thesis, separates liquidity buckets, records immutable diagnoses,
and refreshes an evidence-gated shadow-improvement queue.  It has no broker,
authorization, lifecycle, promotion, or execution surface and has no recurring
polling mode.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
import sqlite3
import statistics
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
STATE = DATA / "state"
DEFAULT_SIGNAL_DATABASE = (
    DATA
    / "reports"
    / "practice_007_signal_news_monitor"
    / "signal_news_monitor_v4.sqlite"
)
DEFAULT_MOVER_DATABASE = STATE / "live_move_news_cases_v1.sqlite"
DEFAULT_DATABASE = STATE / "news_outcome_improvement_audit_v2.sqlite"
DEFAULT_OUTPUT = STATE / "news_outcome_improvement_audit_v2.json"
DEFAULT_QUEUE = STATE / "news_improvement_queue_v2.json"
DEFAULT_REPORT = (
    DATA
    / "reports"
    / "news_outcome_improvement"
    / "NEWS_OUTCOME_IMPROVEMENT_CURRENT.md"
)
DEFAULT_LOG = DATA / "logs" / "news_outcome_improvement_audit_v2.jsonl"

# V2 starts a new immutable diagnostic cohort.  V1 was exercised during the
# implementation preflight before the source fingerprint and horizon-level
# effective-evidence rules were frozen; preserving it avoids rewriting history.
CONTRACT_ID = "news_outcome_improvement_audit_v2_20260826"
SIGNAL_CONTRACT_ID = "practice_007_signal_news_monitor_v7"
MOVER_CASE_CONTRACT_ID = (
    "live_move_news_snapshot_v5_causal_factor_strength_20260827"
)
MOVER_OUTCOME_CONTRACT_ID = "live_move_news_forward_outcomes_v1_20260824"
LOOKBACK_HOURS = 168.0

SAFE_POLICY = {
    "research_only": True,
    "execution_eligible": False,
    "can_place_orders": False,
    "can_promote": False,
    "can_modify_execution_policy": False,
    "broker_access": False,
    # Retain the V2 field name because it is already embedded in immutable
    # diagnosis rows. It describes what one invocation may automate, not a
    # polling cadence; REFRESH_POLICY below is authoritative for scheduling.
    "automatic_scope": [
        "read_immutable_outcomes",
        "diagnose_event_level_results",
        "refresh_evidence_gated_shadow_queue",
        "record_append_only_diagnostics",
    ],
}

REFRESH_POLICY = {
    "refresh_mode": "on_demand_append_only",
    "recurring_polling": False,
}

KNOWN_REASON_CODES = frozenset(
    {
        "cross_pair_factor_duplication",
        "direction_right_magnitude_below_cost",
        "direction_wrong",
        "economic_sign_ambiguous",
        "global_context_overreach",
        "official_source_not_bound_at_decision",
        "technical_confirmation_absent",
        "technical_confirmation_opposed",
        "wide_spread_concentration",
        "directional_mapping_gap",
        "latency_or_decay_gap",
        "verification_gap",
        "strict_signal_absence",
        "source_attribution_gap",
        "mapping_conflict",
        "giveback_or_reversal",
        "bad_direction_or_entry",
        "stale_context_after_reversal",
        "news_direction_error",
        "no_economic_edge",
    }
)

MACRO_CATEGORIES = frozenset({"inflation", "labor", "growth", "policy"})
GLOBAL_CATEGORIES = frozenset(
    {
        "risk_on",
        "risk_off",
        "geopolitical",
        "market_news",
        "trade_policy",
        "commodity",
    }
)


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def iso_utc(value: dt.datetime | None = None) -> str:
    return (value or utc_now()).astimezone(dt.timezone.utc).isoformat()


def parse_utc(value: Any) -> dt.datetime | None:
    try:
        parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def finite(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def stable_hash(value: Any) -> str:
    material = json.dumps(
        value, sort_keys=True, separators=(",", ":"), default=str
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    atomic_text(
        path,
        json.dumps(value, indent=2, sort_keys=True, allow_nan=False, default=str)
        + "\n",
    )


def open_readonly(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(
        f"file:{path.resolve().as_posix()}?mode=ro",
        uri=True,
        timeout=5.0,
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    return connection


def canonical_category(value: Any) -> str:
    category = str(value or "").strip().lower()
    aliases = {
        "inflation": "inflation",
        "inflation_release": "inflation",
        "inflation_context": "inflation",
        "labor_market": "labor",
        "labor_release": "labor",
        "labour_release": "labor",
        "growth": "growth",
        "growth_release": "growth",
        "business_activity_release": "growth",
        "central_bank": "policy",
        "monetary_policy": "policy",
        "interest_rates": "policy",
        "policy_statement": "policy",
        "risk_on_deescalation": "risk_on",
        "risk_off_geopolitical_or_financial": "risk_off",
        "commodity_shock": "commodity",
    }
    return aliases.get(category, category or "unclassified")


def cost_bucket(spread_pips: Any) -> str:
    spread = finite(spread_pips, math.inf)
    if spread <= 2.0:
        return "liquid"
    if spread <= 5.0:
        return "moderate"
    if spread <= 15.0:
        return "wide"
    return "very_wide"


def signed_factor_tokens(instrument: str, direction: str) -> tuple[str, str]:
    try:
        base, quote = instrument.split("_", 1)
    except ValueError:
        return ("UNKNOWN", "UNKNOWN")
    side = 1 if direction == "long" else -1
    return (
        f"{base}{'+' if side > 0 else '-'}",
        f"{quote}{'-' if side > 0 else '+'}",
    )


def source_is_official(source: Any) -> bool:
    value = str(source or "").strip().lower()
    markers = (
        "bank of ",
        "central bank",
        "reserve bank",
        "federal reserve",
        "statistics",
        "statistical office",
        "bureau of labor",
        "treasury",
        "ministry",
        "government",
        "eurostat",
        "cftc",
    )
    return any(marker in value for marker in markers)


def technical_state(rows: Sequence[Mapping[str, Any]]) -> str:
    relationships = Counter(
        str(row.get("relationship") or "neutral") for row in rows
    )
    if relationships["opposed"] > relationships["aligned"]:
        return "opposed"
    if relationships["aligned"] > 0:
        return "aligned"
    return "absent"


def bucket_metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for bucket in ("liquid", "moderate", "wide", "very_wide"):
        selected = [row for row in rows if cost_bucket(row.get("entry_spread_pips")) == bucket]
        values = [finite(row.get("executable_pips")) for row in selected]
        output[bucket] = {
            "raw_n": len(selected),
            "direction_hit_rate": (
                round(sum(bool(row.get("direction_hit")) for row in selected) / len(selected), 6)
                if selected
                else None
            ),
            "after_cost_win_rate": (
                round(sum(bool(row.get("beat_spread")) for row in selected) / len(selected), 6)
                if selected
                else None
            ),
            "median_after_cost_pips": (
                round(statistics.median(values), 6) if values else None
            ),
            "mean_after_cost_pips": (
                round(statistics.fmean(values), 6) if values else None
            ),
        }
    return output


def primary_cost_rows(rows: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    low_cost = [
        row for row in rows if finite(row.get("entry_spread_pips"), math.inf) <= 5.0
    ]
    return low_cost or list(rows)


def event_diagnosis(
    rows: Sequence[Mapping[str, Any]], *, stream: str
) -> dict[str, Any]:
    ordered = sorted(
        rows,
        key=lambda row: (
            finite(row.get("entry_spread_pips"), math.inf),
            str(row.get("instrument") or ""),
        ),
    )
    representative = ordered[0]
    primary = primary_cost_rows(ordered)
    hit_rate = sum(bool(row.get("direction_hit")) for row in primary) / len(primary)
    win_rate = sum(bool(row.get("beat_spread")) for row in primary) / len(primary)
    representative_result = str(representative.get("result_class") or "")
    if representative_result == "captured_after_cost" and win_rate >= 0.5:
        verdict = "event_after_cost_win"
    elif hit_rate >= 0.5 and win_rate <= (1.0 / 3.0):
        verdict = "direction_right_magnitude_below_cost"
    elif hit_rate <= (1.0 / 3.0):
        verdict = "direction_wrong"
    else:
        verdict = "mixed_cross_pair_propagation"

    category = canonical_category(representative.get("category"))
    headline = str(representative.get("headline") or "")
    reason_codes: list[str] = []
    risk_flags: list[str] = []
    if len(ordered) > 1:
        risk_flags.append("cross_pair_factor_duplication")
    if verdict == "direction_right_magnitude_below_cost":
        reason_codes.append("direction_right_magnitude_below_cost")
    elif verdict == "direction_wrong":
        reason_codes.append("direction_wrong")
    if (
        category in {"inflation", "labor", "policy"}
        and verdict != "event_after_cost_win"
    ):
        reason_codes.append("economic_sign_ambiguous")
        risk_flags.append("official_source_not_bound_at_decision")
    if category in GLOBAL_CATEGORIES and len(ordered) >= 4 and win_rate < 0.5:
        reason_codes.append("global_context_overreach")
    if sum(cost_bucket(row.get("entry_spread_pips")) == "very_wide" for row in ordered) > max(1, len(ordered) // 4):
        reason_codes.append("wide_spread_concentration")
    technical = technical_state(ordered)
    if technical == "absent" and verdict != "event_after_cost_win":
        reason_codes.append("technical_confirmation_absent")
    elif technical == "opposed" and verdict != "event_after_cost_win":
        reason_codes.append("technical_confirmation_opposed")
    if category in MACRO_CATEGORIES and not source_is_official(
        representative.get("source_name")
    ):
        risk_flags.append("official_source_not_bound_at_decision")

    tokens = Counter(
        token
        for row in ordered
        for token in signed_factor_tokens(
            str(row.get("instrument") or ""), str(row.get("direction") or "")
        )
    )
    dominant, dominant_count = tokens.most_common(1)[0] if tokens else ("UNKNOWN", 0)
    group_identity = {
        "stream": stream,
        "topic_id": representative.get("topic_id"),
        "horizon_min": int(representative.get("horizon_min") or 0),
        "decision_ids": sorted(str(row.get("decision_id") or "") for row in ordered),
        "results": [
            [
                row.get("decision_id"),
                row.get("status"),
                row.get("outcome_utc"),
                row.get("result_class"),
                row.get("executable_pips"),
            ]
            for row in ordered
        ],
    }
    diagnosis_id = "news_event_diag_" + stable_hash(
        [CONTRACT_ID, group_identity]
    )[:32]
    return {
        "diagnosis_id": diagnosis_id,
        "stream": stream,
        "group_key": f"{representative.get('topic_id')}|H{int(representative.get('horizon_min') or 0)}",
        "decided_utc": representative.get("decided_utc"),
        "topic_id": representative.get("topic_id"),
        "horizon_min": int(representative.get("horizon_min") or 0),
        "headline": headline,
        "category": category,
        "source_name": representative.get("source_name"),
        "verdict": verdict,
        "reason_codes": sorted(set(reason_codes)),
        "risk_flags": sorted(set(risk_flags)),
        "raw_pair_n": len(ordered),
        "effective_thesis_n": 1,
        "dominant_signed_currency_factor": dominant,
        "dominant_factor_share": (
            round(dominant_count / len(ordered), 6) if ordered else None
        ),
        "primary_cost_scope": (
            "spread_at_most_5_pips" if len(primary) != len(ordered) else "all_rows"
        ),
        "primary_n": len(primary),
        "primary_direction_hit_rate": round(hit_rate, 6),
        "primary_after_cost_win_rate": round(win_rate, 6),
        "primary_median_after_cost_pips": round(
            statistics.median(finite(row.get("executable_pips")) for row in primary),
            6,
        ),
        "representative_policy": "lowest_entry_spread_then_instrument_pre_outcome",
        "representative": {
            "decision_id": representative.get("decision_id"),
            "instrument": representative.get("instrument"),
            "entry_spread_pips": finite(representative.get("entry_spread_pips")),
            "result_class": representative_result,
            "executable_pips": finite(representative.get("executable_pips")),
            "relationship": representative.get("relationship") or "neutral",
        },
        "technical_confirmation": technical,
        "cost_buckets": bucket_metrics(ordered),
        "input_fingerprint": stable_hash(group_identity),
        "evidence_unit": f"news_event|{representative.get('topic_id')}",
        **SAFE_POLICY,
    }


def rejected_diagnosis(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    ordered = sorted(
        rows,
        key=lambda row: (
            finite(row.get("entry_spread_pips"), math.inf),
            str(row.get("instrument") or ""),
        ),
    )
    representative = ordered[0]
    prospective = str(representative.get("decision_kind") or "") == (
        "prospective_gate_counterfactual"
    )
    primary = primary_cost_rows(ordered)
    win_rate = sum(bool(row.get("beat_spread")) for row in primary) / len(primary)
    reason = str(representative.get("rejection_reason") or "")
    if prospective and win_rate >= (2.0 / 3.0):
        verdict = "profitable_rejected_shadow"
    elif prospective:
        verdict = "supported_rejection"
    else:
        verdict = "retrospective_followthrough_diagnostic"
    reasons: list[str] = []
    if verdict == "profitable_rejected_shadow":
        if reason == "no_forward_pair_direction":
            reasons.append("directional_mapping_gap")
        elif reason == "late_or_stale":
            reasons.append("latency_or_decay_gap")
        elif reason == "unverified_or_uncorroborated":
            reasons.append("verification_gap")
    identity = {
        "topic_id": representative.get("topic_id"),
        "horizon_min": representative.get("horizon_min"),
        "rejection_reason": reason,
        "decision_kind": representative.get("decision_kind"),
        "rows": [
            [
                row.get("decision_id"),
                row.get("status"),
                row.get("outcome_utc"),
                row.get("result_class"),
                row.get("executable_pips"),
            ]
            for row in ordered
        ],
    }
    return {
        "diagnosis_id": "rejected_news_diag_" + stable_hash(
            [CONTRACT_ID, identity]
        )[:32],
        "stream": "rejected_news_shadow",
        "group_key": (
            f"{representative.get('topic_id')}|H{representative.get('horizon_min')}|"
            f"{reason}|{representative.get('decision_kind')}"
        ),
        "decided_utc": representative.get("decided_utc"),
        "topic_id": representative.get("topic_id"),
        "horizon_min": int(representative.get("horizon_min") or 0),
        "headline": representative.get("headline"),
        "category": canonical_category(representative.get("category")),
        "source_name": representative.get("source_name"),
        "verdict": verdict,
        "reason_codes": reasons,
        "risk_flags": [],
        "rejection_reason": reason,
        "decision_kind": representative.get("decision_kind"),
        "prospective_miss_counted": verdict == "profitable_rejected_shadow",
        "raw_pair_n": len(ordered),
        "effective_thesis_n": 1,
        "primary_after_cost_win_rate": round(win_rate, 6),
        "representative_policy": "lowest_entry_spread_then_instrument_pre_outcome",
        "representative": {
            "decision_id": representative.get("decision_id"),
            "instrument": representative.get("instrument"),
            "entry_spread_pips": finite(representative.get("entry_spread_pips")),
            "result_class": representative.get("result_class"),
            "executable_pips": finite(representative.get("executable_pips")),
        },
        "cost_buckets": bucket_metrics(ordered),
        "input_fingerprint": stable_hash(identity),
        "evidence_unit": f"rejected_news_event|{representative.get('topic_id')}",
        **SAFE_POLICY,
    }


def event_age_minutes(case: Mapping[str, Any]) -> float | None:
    observed = parse_utc(case.get("observation_utc"))
    clocks = [
        parse_utc(row.get("effective_from_utc"))
        for row in case.get("recent_context_stories") or []
        if isinstance(row, Mapping)
    ]
    clocks = [value for value in clocks if value is not None]
    if observed is None or not clocks:
        return None
    return max(0.0, (observed - max(clocks)).total_seconds() / 60.0)


def mover_diagnosis(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    by_arm: dict[str, Mapping[str, Any]] = {}
    for row in sorted(
        rows,
        key=lambda item: (
            str(item.get("observation_utc") or ""),
            str(item.get("case_id") or ""),
            str(item.get("arm") or ""),
        ),
    ):
        by_arm.setdefault(str(row.get("arm") or ""), row)
    reference = (
        by_arm.get("technical_continuation")
        or by_arm.get("broad_context_direction")
        or next(iter(by_arm.values()))
    )
    case = reference.get("case") or {}
    broad = by_arm.get("broad_context_direction")
    technical = by_arm.get("technical_continuation")
    strict = by_arm.get("strict_forward_direction")
    reasons: list[str] = []
    explanation = str(case.get("explanation_state") or "")
    if strict is None and int(case.get("pre_move_event_count") or 0) > 0:
        reasons.append("strict_signal_absence")
    if explanation == "no_relevant_source_event":
        reasons.append("source_attribution_gap")
    elif explanation == "context_only_conflicted_or_pair_neutral":
        reasons.append("mapping_conflict")
    continuation_net = finite(technical.get("after_cost_pips")) if technical else None
    broad_net = finite(broad.get("after_cost_pips")) if broad else None
    if technical is not None and continuation_net is not None and continuation_net <= 0.0:
        mfe = finite(technical.get("mfe_pips"))
        reasons.append("giveback_or_reversal" if mfe > 0.0 else "bad_direction_or_entry")
    age = event_age_minutes(case)
    if broad is not None and broad_net is not None and broad_net <= 0.0 and age is not None and age > 60.0:
        reasons.append("stale_context_after_reversal")
    if broad_net is not None and continuation_net is not None:
        if broad_net <= 0.0 < continuation_net:
            reasons.append("news_direction_error")
        elif broad_net <= 0.0 and continuation_net <= 0.0:
            reasons.append("no_economic_edge")
    if broad_net is not None and broad_net > 0.0:
        verdict = "broad_context_after_cost_win"
    elif strict is not None and finite(strict.get("after_cost_pips")) > 0.0:
        verdict = "strict_forward_after_cost_win"
    elif continuation_net is not None and continuation_net > 0.0:
        verdict = "technical_continuation_after_cost_win"
    elif reasons:
        verdict = "mover_followthrough_miss_or_gap"
    else:
        verdict = "mover_diagnostic_neutral"
    identity = {
        "factor_episode_id": reference.get("factor_episode_id"),
        "horizon_min": reference.get("horizon_min"),
        "outcomes": sorted(
            [
                row.get("outcome_id"),
                row.get("arm"),
                row.get("after_cost_pips"),
            ]
            for row in by_arm.values()
        ),
    }
    return {
        "diagnosis_id": "mover_factor_diag_" + stable_hash(
            [CONTRACT_ID, identity]
        )[:32],
        "stream": "live_mover_factor",
        "group_key": (
            f"{reference.get('factor_episode_id')}|H{reference.get('horizon_min')}"
        ),
        "decided_utc": reference.get("observation_utc"),
        "factor_episode_id": reference.get("factor_episode_id"),
        "horizon_min": int(reference.get("horizon_min") or 0),
        "instrument": reference.get("instrument"),
        "verdict": verdict,
        "reason_codes": sorted(set(reasons)),
        "risk_flags": [],
        "raw_pair_n": len({str(row.get("case_id") or "") for row in rows}),
        "effective_thesis_n": 1,
        "explanation_state": explanation,
        "source_event_age_minutes": round(age, 6) if age is not None else None,
        "arms": {
            arm: {
                "case_id": row.get("case_id"),
                "instrument": row.get("instrument"),
                "after_cost_pips": finite(row.get("after_cost_pips")),
                "mfe_pips": finite(row.get("mfe_pips")),
                "mae_pips": finite(row.get("mae_pips")),
            }
            for arm, row in sorted(by_arm.items())
        },
        "input_fingerprint": stable_hash(identity),
        "evidence_unit": f"mover_factor|{reference.get('factor_episode_id')}",
        **SAFE_POLICY,
    }


def load_signal_diagnoses(
    path: Path, *, cutoff_utc: str
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not path.is_file():
        raise FileNotFoundError(path)
    connection = open_readonly(path)
    try:
        accepted = [
            dict(row)
            for row in connection.execute(
                """
                SELECT d.*,
                       COALESCE(o.relationship,'neutral') AS relationship,
                       o.signal_direction,o.signal_confidence,
                       o.signal_expected_net_pips,o.signal_eligible
                FROM independent_news_decisions d
                LEFT JOIN observations o
                  ON o.observed_utc=d.decided_utc AND o.instrument=d.instrument
                WHERE d.status='matured' AND d.decided_utc>=?
                ORDER BY d.decided_utc,d.topic_id,d.horizon_min,d.instrument
                """,
                (cutoff_utc,),
            )
        ]
        rejected = [
            dict(row)
            for row in connection.execute(
                """
                SELECT r.*
                FROM rejected_news_shadows r
                WHERE r.status='matured' AND r.decided_utc>=?
                ORDER BY r.decided_utc,r.topic_id,r.horizon_min,r.instrument
                """,
                (cutoff_utc,),
            )
        ]
        accepted_groups: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
        for row in accepted:
            accepted_groups[(str(row["topic_id"]), int(row["horizon_min"]))].append(row)
        rejected_groups: dict[tuple[str, int, str, str], list[dict[str, Any]]] = defaultdict(list)
        for row in rejected:
            key = (
                str(row["topic_id"]),
                int(row["horizon_min"]),
                str(row["rejection_reason"]),
                str(row["decision_kind"]),
            )
            rejected_groups[key].append(row)
        diagnoses = [
            event_diagnosis(rows, stream="accepted_news_decision")
            for _, rows in sorted(accepted_groups.items())
        ] + [
            rejected_diagnosis(rows)
            for _, rows in sorted(rejected_groups.items())
        ]
        highwater = connection.execute(
            """
            SELECT MAX(value) FROM (
              SELECT MAX(COALESCE(outcome_utc,decided_utc)) value
              FROM independent_news_decisions
              UNION ALL
              SELECT MAX(COALESCE(outcome_utc,decided_utc)) value
              FROM rejected_news_shadows
            )
            """
        ).fetchone()[0]
        return diagnoses, {
            "database": str(path.resolve()),
            "contract_id": SIGNAL_CONTRACT_ID,
            "accepted_matured_pair_rows": len(accepted),
            "rejected_matured_pair_rows": len(rejected),
            "accepted_effective_theses": len(accepted_groups),
            "rejected_effective_theses": len(rejected_groups),
            "highwater_utc": highwater,
        }
    finally:
        connection.close()


def load_mover_diagnoses(
    path: Path, *, cutoff_utc: str
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not path.is_file():
        raise FileNotFoundError(path)
    connection = open_readonly(path)
    try:
        rows: list[dict[str, Any]] = []
        for raw in connection.execute(
            """
            SELECT o.outcome_id,o.matured_utc,o.case_id,o.arm,o.horizon_min,
                   o.after_cost_pips,o.outcome_json,c.first_recorded_utc,c.case_json
            FROM mover_case_outcomes o
            JOIN mover_cases c ON c.case_id=o.case_id
            WHERE o.case_contract_id=? AND c.first_recorded_utc>=?
            ORDER BY c.first_recorded_utc,o.case_id,o.horizon_min,o.arm
            """,
            (MOVER_CASE_CONTRACT_ID, cutoff_utc),
        ):
            row = dict(raw)
            try:
                case = json.loads(str(row.pop("case_json") or "{}"))
                outcome = json.loads(str(row.pop("outcome_json") or "{}"))
            except json.JSONDecodeError:
                continue
            if not isinstance(case, dict) or not isinstance(outcome, dict):
                continue
            if not bool(case.get("factor_representative")):
                continue
            row.update(outcome)
            row["case"] = case
            row["factor_episode_id"] = str(case.get("factor_episode_id") or row["case_id"])
            row["observation_utc"] = str(case.get("observation_utc") or row["first_recorded_utc"])
            rows.append(row)
        groups: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            groups[(str(row["factor_episode_id"]), int(row["horizon_min"]))].append(row)
        diagnoses = [
            mover_diagnosis(group_rows)
            for _, group_rows in sorted(groups.items())
        ]
        highwater = connection.execute(
            "SELECT MAX(matured_utc) FROM mover_case_outcomes WHERE case_contract_id=?",
            (MOVER_CASE_CONTRACT_ID,),
        ).fetchone()[0]
        return diagnoses, {
            "database": str(path.resolve()),
            "case_contract_id": MOVER_CASE_CONTRACT_ID,
            "outcome_contract_id": MOVER_OUTCOME_CONTRACT_ID,
            "matured_factor_arm_rows": len(rows),
            "effective_factor_horizon_theses": len(groups),
            "highwater_utc": highwater,
        }
    finally:
        connection.close()


def open_output_database(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30.0)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS diagnoses (
            diagnosis_id TEXT PRIMARY KEY,
            first_recorded_utc TEXT NOT NULL,
            stream TEXT NOT NULL,
            group_key TEXT NOT NULL,
            verdict TEXT NOT NULL,
            diagnosis_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS ix_news_diagnoses_stream_group
          ON diagnoses(stream,group_key,first_recorded_utc);
        CREATE TABLE IF NOT EXISTS audit_snapshots (
            source_fingerprint TEXT PRIMARY KEY,
            generated_utc TEXT NOT NULL,
            diagnosis_count INTEGER NOT NULL,
            queue_count INTEGER NOT NULL,
            summary_json TEXT NOT NULL
        );
        CREATE TRIGGER IF NOT EXISTS diagnoses_no_update
          BEFORE UPDATE ON diagnoses BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS diagnoses_no_delete
          BEFORE DELETE ON diagnoses BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS audit_snapshots_no_update
          BEFORE UPDATE ON audit_snapshots BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS audit_snapshots_no_delete
          BEFORE DELETE ON audit_snapshots BEGIN SELECT RAISE(ABORT,'append_only'); END;
        """
    )
    connection.commit()
    return connection


PROPOSALS: dict[str, dict[str, Any]] = {
    "economic_sign_ambiguous": {
        "title": "Require policy-repricing confirmation for ambiguous macro signs",
        "priority": 100,
        "shadow_rule": "separate semantic direction from causal surprise and rate repricing; abstain when unresolved",
        "external_blocker": "timestamp-safe intraday policy-rate repricing is incomplete",
    },
    "direction_right_magnitude_below_cost": {
        "title": "Add magnitude-versus-cost admission model",
        "priority": 95,
        "shadow_rule": "require forecast movement to clear executable spread and stressed slippage",
    },
    "stale_context_after_reversal": {
        "title": "Split first response, continuation, and reversal with event decay",
        "priority": 92,
        "shadow_rule": "freeze source-age half-life and start a new reversal segment after an opposing clear move",
    },
    "global_context_overreach": {
        "title": "Give direct local macro evidence precedence over global context",
        "priority": 90,
        "shadow_rule": "global risk may condition but cannot overwrite a fresh issuing-currency event",
    },
    "official_source_not_bound_at_decision": {
        "title": "Bind third-party detection to the first authoritative release",
        "priority": 88,
        "shadow_rule": "retain discovery clock and separately measure direct-authority availability and agreement",
    },
    "directional_mapping_gap": {
        "title": "Evaluate missing directional mappings as a separate shadow arm",
        "priority": 85,
        "shadow_rule": "freeze latent semantic side; require repeated independent cost-clearing episodes",
    },
    "strict_signal_absence": {
        "title": "Increase strict source-to-currency mapping coverage",
        "priority": 82,
        "shadow_rule": "map exact issuing currency and measurable driver without borrowing broad context",
    },
    "source_attribution_gap": {
        "title": "Audit unexplained movers against authoritative source clocks",
        "priority": 80,
        "shadow_rule": "open one source-gap case per factor episode, never per pair",
    },
    "giveback_or_reversal": {
        "title": "Fit horizon-aware hold and reversal diagnostics",
        "priority": 78,
        "shadow_rule": "record MFE, MAE, first-clearance time, and giveback before proposing exits",
    },
    "news_direction_error": {
        "title": "Compare news direction against contemporaneous technical response",
        "priority": 76,
        "shadow_rule": "news supplies thesis; technical reaction supplies timing/veto in a frozen interaction arm",
    },
    "wide_spread_concentration": {
        "title": "Keep event results split by liquidity and cost bucket",
        "priority": 74,
        "shadow_rule": "never allow wide-spread pair sums to determine the liquid event verdict",
    },
    "latency_or_decay_gap": {
        "title": "Measure source arrival against the tradable reaction window",
        "priority": 72,
        "shadow_rule": "archive publication, first-seen, decision, and first-cost-clearance clocks",
    },
    "verification_gap": {
        "title": "Measure corroboration value without weakening source gates",
        "priority": 70,
        "shadow_rule": "compare verified, independently corroborated, and single-source cohorts separately",
    },
    "technical_confirmation_absent": {
        "title": "Test news-plus-technical timing against news-only entry",
        "priority": 68,
        "shadow_rule": "freeze confirmation definitions before observing the next event cohort",
    },
    "technical_confirmation_opposed": {
        "title": "Test an opposing-technical abstention veto",
        "priority": 66,
        "shadow_rule": "abstain in shadow when reaction direction contradicts the causal thesis",
    },
    "mapping_conflict": {
        "title": "Preserve conflicting currency channels instead of flattening",
        "priority": 64,
        "shadow_rule": "retain separate growth, inflation, policy, risk, and commodity channels",
    },
    "bad_direction_or_entry": {
        "title": "Separate forecast error from entry-timing error",
        "priority": 62,
        "shadow_rule": "compare decision-time, first-break, pullback, and no-entry counterfactuals",
    },
    "direction_wrong": {
        "title": "Falsify the current event-direction mapping",
        "priority": 60,
        "shadow_rule": "retain current, flipped, and abstain controls in a new untouched cohort",
    },
    "no_economic_edge": {
        "title": "Retire event-followthrough cells that cannot clear cost",
        "priority": 58,
        "shadow_rule": "apply the governed futility boundary at factor-episode level",
    },
}


def build_queue(diagnoses: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    causes: dict[str, set[str]] = defaultdict(set)
    examples: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in diagnoses:
        if str(row.get("verdict")) in {
            "event_after_cost_win",
            "supported_rejection",
            "retrospective_followthrough_diagnostic",
        }:
            continue
        for reason in row.get("reason_codes") or []:
            if reason not in PROPOSALS:
                continue
            causes[str(reason)].add(
                str(row.get("evidence_unit") or row.get("group_key") or "")
            )
            if len(examples[str(reason)]) < 3:
                examples[str(reason)].append(
                    {
                        "group_key": row.get("group_key"),
                        "headline": row.get("headline"),
                        "verdict": row.get("verdict"),
                    }
                )
        for flag in row.get("risk_flags") or []:
            if flag not in PROPOSALS:
                continue
            causes[str(flag)].add(
                str(row.get("evidence_unit") or row.get("group_key") or "")
            )
    items = []
    for reason, groups in causes.items():
        proposal = PROPOSALS[reason]
        evidence_count = len(groups)
        state = (
            "blocked_external_input"
            if proposal.get("external_blocker")
            else "ready_for_versioned_shadow_test"
            if evidence_count >= 3
            else "collecting_shadow_evidence"
        )
        items.append(
            {
                "proposal_id": "news_improvement_" + stable_hash(
                    [CONTRACT_ID, reason]
                )[:24],
                "reason_code": reason,
                "title": proposal["title"],
                "state": state,
                "priority_score": int(proposal["priority"]) + min(20, evidence_count),
                "effective_diagnosed_theses": evidence_count,
                "shadow_rule": proposal["shadow_rule"],
                "external_blocker": proposal.get("external_blocker"),
                "examples": examples[reason],
                "new_cohort_required": True,
                "minimum_independent_confirmation_episodes": 20,
                "automatic_execution_change": False,
                **SAFE_POLICY,
            }
        )
    items.sort(key=lambda row: (-int(row["priority_score"]), row["reason_code"]))
    return {
        "schema_version": 1,
        "contract_id": CONTRACT_ID,
        "generated_utc": iso_utc(),
        "queue_count": len(items),
        "next_best_shadow_improvement": items[0] if items else None,
        "items": items,
        "selection_contract": (
            "rank predefined failure classes by base priority and independent "
            "diagnosed thesis count; never use pair fan-out as evidence"
        ),
        "graduation_contract": (
            "versioned shadow cohort, independent episodes, after-cost proof, "
            "multiplicity control, untouched confirmation, then narrow practice canary"
        ),
        **SAFE_POLICY,
        **REFRESH_POLICY,
    }


def render(payload: Mapping[str, Any]) -> str:
    summary = payload.get("summary") or {}
    queue = payload.get("queue") or {}
    lines = [
        "# News outcome improvement record",
        "",
        f"Canonical record: `{payload.get('recorded_utc')}`",
        f"Last integrity-preserving review: `{payload.get('evaluated_utc')}`",
        "",
        "This on-demand review appends new diagnoses and queues shadow improvements only. It does not poll continuously and cannot change execution.",
        "",
        f"- New immutable diagnoses: **{summary.get('inserted_diagnoses', 0)}**",
        f"- Retained immutable diagnoses: **{summary.get('retained_diagnoses', 0)}**",
        f"- Effective event/factor theses in window: **{summary.get('current_effective_theses', 0)}**",
        f"- Wins: **{summary.get('wins', 0)}**; misses/gaps: **{summary.get('misses_or_gaps', 0)}**",
        f"- Open evidence-gated proposals: **{queue.get('queue_count', 0)}**",
        "",
        "| Rank | Proposed shadow improvement | State | Effective diagnoses |",
        "|---:|---|---|---:|",
    ]
    for index, row in enumerate((queue.get("items") or [])[:10], start=1):
        lines.append(
            f"| {index} | {row['title']} | {row['state']} | "
            f"{row['effective_diagnosed_theses']} |"
        )
    lines.extend(
        [
            "",
            "## Latest event decisions",
            "",
            "| Decision | Class | Raw pairs | Effective N | Primary win rate | Causes |",
            "|---|---|---:|---:|---:|---|",
        ]
    )
    events = list(payload.get("latest_event_diagnoses") or [])
    for row in events[:12]:
        lines.append(
            f"| {str(row.get('headline') or row.get('group_key'))[:80]} | "
            f"{row.get('verdict')} | {row.get('raw_pair_n')} | "
            f"{row.get('effective_thesis_n')} | "
            f"{row.get('primary_after_cost_win_rate')} | "
            f"{', '.join(row.get('reason_codes') or []) or 'none'} |"
        )
    lines.extend(
        [
            "",
            "Pair fan-out is coverage, not independent evidence. Liquidity buckets are reported separately and no summed exotic-pair P/L determines an event verdict.",
            "",
        ]
    )
    return "\n".join(lines)


def run_once(
    *,
    signal_database: Path = DEFAULT_SIGNAL_DATABASE,
    mover_database: Path = DEFAULT_MOVER_DATABASE,
    database: Path = DEFAULT_DATABASE,
    output: Path = DEFAULT_OUTPUT,
    queue_path: Path = DEFAULT_QUEUE,
    report: Path = DEFAULT_REPORT,
    log_path: Path = DEFAULT_LOG,
    lookback_hours: float = LOOKBACK_HOURS,
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    observed = (now or utc_now()).astimezone(dt.timezone.utc)
    cutoff = observed - dt.timedelta(hours=max(1.0, float(lookback_hours)))
    errors: list[str] = []
    diagnoses: list[dict[str, Any]] = []
    inputs: dict[str, Any] = {}
    try:
        signal_rows, signal_input = load_signal_diagnoses(
            signal_database, cutoff_utc=iso_utc(cutoff)
        )
        diagnoses.extend(signal_rows)
        inputs["signal_news"] = signal_input
    except (FileNotFoundError, sqlite3.Error) as exc:
        errors.append(f"signal_news:{type(exc).__name__}:{exc}")
    try:
        mover_rows, mover_input = load_mover_diagnoses(
            mover_database, cutoff_utc=iso_utc(cutoff)
        )
        diagnoses.extend(mover_rows)
        inputs["live_movers"] = mover_input
    except (FileNotFoundError, sqlite3.Error) as exc:
        errors.append(f"live_movers:{type(exc).__name__}:{exc}")
    diagnoses.sort(
        key=lambda row: (
            str(row.get("decided_utc") or ""),
            str(row.get("diagnosis_id") or ""),
        ),
        reverse=True,
    )
    queue = build_queue(diagnoses)
    queue["generated_utc"] = iso_utc(observed)
    source_fingerprint = stable_hash(
        {
            "contract": CONTRACT_ID,
            "inputs": inputs,
            "diagnoses": [row["diagnosis_id"] for row in diagnoses],
        }
    )
    inserted = 0
    output_db = open_output_database(database)
    try:
        for row in diagnoses:
            before = output_db.total_changes
            output_db.execute(
                "INSERT OR IGNORE INTO diagnoses VALUES (?,?,?,?,?,?)",
                (
                    row["diagnosis_id"],
                    iso_utc(observed),
                    row["stream"],
                    row["group_key"],
                    row["verdict"],
                    json.dumps(row, sort_keys=True, separators=(",", ":")),
                ),
            )
            inserted += int(output_db.total_changes > before)
        retained = int(output_db.execute("SELECT COUNT(*) FROM diagnoses").fetchone()[0])
        snapshot_before = output_db.total_changes
        snapshot_summary = {
            "errors": errors,
            "input_highwaters": {
                key: value.get("highwater_utc") for key, value in inputs.items()
            },
        }
        output_db.execute(
            "INSERT OR IGNORE INTO audit_snapshots VALUES (?,?,?,?,?)",
            (
                source_fingerprint,
                iso_utc(observed),
                len(diagnoses),
                int(queue["queue_count"]),
                json.dumps(snapshot_summary, sort_keys=True),
            ),
        )
        inserted_snapshot = int(output_db.total_changes > snapshot_before)
        output_db.commit()
        snapshot_row = output_db.execute(
            """
            SELECT generated_utc, diagnosis_count, queue_count
            FROM audit_snapshots
            WHERE source_fingerprint=?
            """,
            (source_fingerprint,),
        ).fetchone()
        if snapshot_row is None:
            raise RuntimeError("canonical audit snapshot was not retained")
        recorded_utc = str(snapshot_row[0])
        recorded_diagnosis_count = int(snapshot_row[1])
        recorded_queue_count = int(snapshot_row[2])
        integrity = str(output_db.execute("PRAGMA quick_check").fetchone()[0])
    finally:
        output_db.close()

    win_verdicts = {
        "event_after_cost_win",
        "broad_context_after_cost_win",
        "strict_forward_after_cost_win",
        "technical_continuation_after_cost_win",
    }
    miss_verdicts = {
        "direction_right_magnitude_below_cost",
        "direction_wrong",
        "mixed_cross_pair_propagation",
        "profitable_rejected_shadow",
        "mover_followthrough_miss_or_gap",
    }
    summary = {
        "current_effective_theses": len(diagnoses),
        "wins": sum(row["verdict"] in win_verdicts for row in diagnoses),
        "misses_or_gaps": sum(row["verdict"] in miss_verdicts for row in diagnoses),
        "inserted_diagnoses": inserted,
        "retained_diagnoses": retained,
        "inserted_snapshot": inserted_snapshot,
        "reason_counts": dict(
            sorted(
                Counter(
                    reason
                    for row in diagnoses
                    for reason in row.get("reason_codes") or []
                ).items()
            )
        ),
        "verdict_counts": dict(
            sorted(Counter(str(row["verdict"]) for row in diagnoses).items())
        ),
    }
    queue["recorded_utc"] = recorded_utc
    queue["evaluated_utc"] = iso_utc(observed)
    payload = {
        "schema_version": 1,
        "contract_id": CONTRACT_ID,
        "generated_utc": iso_utc(observed),
        "evaluated_utc": iso_utc(observed),
        "recorded_utc": recorded_utc,
        "canonical_snapshot": {
            "source_fingerprint": source_fingerprint,
            "diagnosis_count": recorded_diagnosis_count,
            "queue_count": recorded_queue_count,
        },
        "status": "ok" if not errors else "degraded_missing_input",
        "lookback_hours": float(lookback_hours),
        "source_fingerprint": source_fingerprint,
        "input_contracts": {
            "signal_news": SIGNAL_CONTRACT_ID,
            "mover_cases": MOVER_CASE_CONTRACT_ID,
            "mover_outcomes": MOVER_OUTCOME_CONTRACT_ID,
        },
        "inputs": inputs,
        "input_errors": errors,
        "database": str(database.resolve()),
        "sqlite_integrity": integrity,
        "summary": summary,
        "queue": queue,
        "latest_diagnoses": diagnoses[:40],
        "latest_event_diagnoses": [
            row
            for row in diagnoses
            if row.get("stream")
            in {"accepted_news_decision", "rejected_news_shadow"}
        ][:40],
        "latest_mover_diagnoses": [
            row for row in diagnoses if row.get("stream") == "live_mover_factor"
        ][:40],
        "known_reason_codes": sorted(KNOWN_REASON_CODES),
        "supported_execution_decision": "no_change_diagnostic_only",
        **SAFE_POLICY,
        **REFRESH_POLICY,
    }
    atomic_json(output, payload)
    atomic_json(queue_path, queue)
    atomic_text(report, render(payload))
    if inserted_snapshot:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a", encoding="utf-8") as stream:
            stream.write(
                json.dumps(
                    {
                        "generated_utc": payload["generated_utc"],
                        "source_fingerprint": source_fingerprint,
                        "summary": summary,
                        "input_errors": errors,
                        "next_best_shadow_improvement": queue.get(
                            "next_best_shadow_improvement"
                        ),
                    },
                    sort_keys=True,
                )
                + "\n"
            )
    return payload


def validate_existing(
    output: Path = DEFAULT_OUTPUT,
    database: Path = DEFAULT_DATABASE,
    *,
    maximum_future_skew_sec: float = 300.0,
) -> tuple[bool, list[str]]:
    errors: list[str] = []
    try:
        payload = json.loads(output.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return False, [f"state_unreadable:{type(exc).__name__}"]
    if payload.get("contract_id") != CONTRACT_ID:
        errors.append("contract_mismatch")
    generated = parse_utc(payload.get("generated_utc"))
    if generated is None:
        errors.append("record_timestamp_invalid")
    elif (generated - utc_now()).total_seconds() > maximum_future_skew_sec:
        errors.append("record_timestamp_in_future")
    for key, expected in {**SAFE_POLICY, **REFRESH_POLICY}.items():
        if payload.get(key) != expected:
            errors.append(f"unsafe_or_missing:{key}")
    if payload.get("sqlite_integrity") != "ok":
        errors.append("reported_sqlite_integrity")
    try:
        connection = open_readonly(database)
        try:
            if connection.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                errors.append("database_integrity")
            retained = int(connection.execute("SELECT COUNT(*) FROM diagnoses").fetchone()[0])
            snapshot = connection.execute(
                """
                SELECT generated_utc, diagnosis_count, queue_count
                FROM audit_snapshots
                WHERE source_fingerprint=?
                """,
                (str(payload.get("source_fingerprint") or ""),),
            ).fetchone()
            trigger_names = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='trigger'"
                ).fetchall()
            }
        finally:
            connection.close()
    except (FileNotFoundError, sqlite3.Error):
        errors.append("database_unreadable")
        retained = -1
        snapshot = None
        trigger_names = set()
    required_triggers = {
        "diagnoses_no_update",
        "diagnoses_no_delete",
        "audit_snapshots_no_update",
        "audit_snapshots_no_delete",
    }
    if not required_triggers <= trigger_names:
        errors.append("append_only_triggers_missing")
    if retained != int((payload.get("summary") or {}).get("retained_diagnoses") or 0):
        errors.append("diagnosis_count_mismatch")
    queue = payload.get("queue") or {}
    if int(queue.get("queue_count") or 0) != len(queue.get("items") or []):
        errors.append("queue_count_mismatch")
    canonical_snapshot = payload.get("canonical_snapshot") or {}
    if snapshot is None:
        errors.append("canonical_snapshot_missing")
    else:
        recorded_utc, diagnosis_count, queue_count = snapshot
        if str(payload.get("recorded_utc") or "") != str(recorded_utc):
            errors.append("recorded_utc_mismatch")
        if str(canonical_snapshot.get("source_fingerprint") or "") != str(
            payload.get("source_fingerprint") or ""
        ):
            errors.append("canonical_snapshot_fingerprint_mismatch")
        if int(canonical_snapshot.get("diagnosis_count") or 0) != int(
            diagnosis_count
        ) or int((payload.get("summary") or {}).get("current_effective_theses") or 0) != int(
            diagnosis_count
        ):
            errors.append("canonical_snapshot_diagnosis_count_mismatch")
        if int(canonical_snapshot.get("queue_count") or 0) != int(
            queue_count
        ) or int(queue.get("queue_count") or 0) != int(queue_count):
            errors.append("canonical_snapshot_queue_count_mismatch")
    if any(
        reason not in KNOWN_REASON_CODES
        for reason in (payload.get("summary") or {}).get("reason_counts") or {}
    ):
        errors.append("unknown_reason_code")
    return not errors, errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--signal-database", type=Path, default=DEFAULT_SIGNAL_DATABASE)
    parser.add_argument("--mover-database", type=Path, default=DEFAULT_MOVER_DATABASE)
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--queue", type=Path, default=DEFAULT_QUEUE)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--log", type=Path, default=DEFAULT_LOG)
    parser.add_argument("--lookback-hours", type=float, default=LOOKBACK_HOURS)
    parser.add_argument("--validate-existing", action="store_true")
    args = parser.parse_args(argv)
    if args.validate_existing:
        valid, errors = validate_existing(args.output, args.database)
        print(json.dumps({"valid": valid, "errors": errors}, sort_keys=True))
        return 0 if valid else 2
    payload = run_once(
        signal_database=args.signal_database,
        mover_database=args.mover_database,
        database=args.database,
        output=args.output,
        queue_path=args.queue,
        report=args.report,
        log_path=args.log,
        lookback_hours=max(1.0, args.lookback_hours),
    )
    print(
        json.dumps(
            {
                "generated_utc": payload["generated_utc"],
                "recorded_utc": payload["recorded_utc"],
                "status": payload["status"],
                "summary": payload["summary"],
                "queue_count": payload["queue"]["queue_count"],
            },
            sort_keys=True,
        ),
        flush=True,
    )
    return 0 if payload["status"] == "ok" else 2


if __name__ == "__main__":
    raise SystemExit(main())
