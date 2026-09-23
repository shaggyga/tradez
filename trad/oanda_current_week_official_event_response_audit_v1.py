#!/usr/bin/env python3
"""Audit current-week official currency events against 68-pair responses.

This is a retrospective research diagnostic.  It rebuilds current classifier
semantics from retained point-in-time payloads, collapses repeated collector
rows, separates scheduled-calendar observations from actual releases, and
measures fixed 5/15/60/120-minute executable responses.  It cannot authorize,
promote, submit, or close an order.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import sqlite3
from pathlib import Path
from typing import Any, Mapping, Sequence

import oanda_current_week_move_case_audit_v1 as weekly
import oanda_local_news_sentiment as news


ROOT = Path(__file__).resolve().parent
NEWS_DB = weekly.NEWS_DB
CANDLE_ROOT = weekly.CANDLE_ROOT
REPORT_ROOT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "major_move_case_audits"
    / "current_week_official_event_responses_v1"
)
OUTPUT_JSON = REPORT_ROOT / "CURRENT_WEEK_OFFICIAL_EVENT_RESPONSES_V1.json"
OUTPUT_MD = REPORT_ROOT / "CURRENT_WEEK_OFFICIAL_EVENT_RESPONSES_V1.md"
CONTRACT_ID = "current_week_official_event_response_audit_v3_material_dedup_20260904"
HORIZONS = (5, 15, 60, 120)
UTC = dt.timezone.utc

MACRO_RELEASE_CATEGORIES = frozenset(
    {
        "business_activity_release",
        "employment_release",
        "growth_release",
        "inflation_release",
        "labor_release",
        "production_release",
        "retail_sales_release",
        "trade_balance_release",
    }
)
POLICY_HEADLINE_RE = re.compile(
    r"\b(?:cash rate|central bank|fomc|interest rate|minutes|monetary policy|"
    r"official cash rate|outlook for economic activity and prices|policy rate|"
    r"rate decision|rate statement|target range)\b",
    flags=re.I,
)
SUPPORTING_ARTIFACT_RE = re.compile(
    r"\b(?:data tables?|dataset|time series|volumes by standard occupation classification)\b",
    flags=re.I,
)
OBSERVED_CHANGE_RE = re.compile(
    r"\b(?:accelerated|contracted|decreased|fell|higher|increased|lower|rose|"
    r"slowed|strengthened|unchanged|weakened)\b",
    flags=re.I,
)


class OfficialEventAuditError(RuntimeError):
    pass


def current_utc_week_window(now: dt.datetime | None = None) -> tuple[dt.datetime, dt.datetime]:
    """Return the current Monday-00:00 UTC through-now evidence window.

    The previous implementation silently defaulted to the completed
    2026-08-17 week.  A report titled ``current week`` must instead bind its
    window to its generation clock.  Explicit caller-supplied windows remain
    supported for reproducible historical audits.
    """

    end = now or dt.datetime.now(UTC)
    if end.tzinfo is None:
        end = end.replace(tzinfo=UTC)
    else:
        end = end.astimezone(UTC)
    start = (end - dt.timedelta(days=end.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    return start, end


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def stable_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def _normalized_identity(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "-", str(value or "").lower()).strip("-")


def _as_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    try:
        payload = json.loads(str(row.get("payload_json") or "{}"))
    except json.JSONDecodeError:
        payload = {}
    payload = payload if isinstance(payload, dict) else {}
    payload.update(
        {
            "event_id": row["event_id"],
            "source_id": row["source_id"],
            "source_name": row["source_name"],
            "source_kind": row["source_kind"],
            "source_quality": row["source_quality"],
            "source_verified": bool(row["source_verified"]),
            "title": row["headline"],
            "headline": row["headline"],
            "summary": row["summary"],
            "url": row["source_url"],
            "source_url": row["source_url"],
            "published_utc": row["published_utc"],
            "first_seen_utc": row["first_seen_utc"],
        }
    )
    return payload


def _single_currency(article: Mapping[str, Any]) -> str:
    direct = {
        str(value).upper()
        for value in article.get("source_currencies") or []
        if str(value).strip()
    }
    research = {
        str(value).upper()
        for value in (article.get("research_currency_scores") or {})
        if str(value).strip()
    }
    candidates = direct or research
    if len(candidates) == 1:
        return next(iter(candidates))
    return ""


def _event_clock(
    article: Mapping[str, Any],
    week_start: dt.datetime,
    week_end: dt.datetime,
) -> tuple[dt.datetime | None, str]:
    scheduled = weekly.parse_utc(article["scheduled_utc"]) if article.get("scheduled_utc") else None
    published = weekly.parse_utc(article["published_utc"]) if article.get("published_utc") else None
    actual_present = article.get("actual_value") is not None
    source_role = str(article.get("source_role") or "")
    policy_present = bool(
        article.get("official_policy_release")
        or article.get("official_duration_liquidity_policy")
        or (
            article.get("category") == "monetary_policy"
            and "calendar" not in source_role
        )
    )
    if scheduled is not None:
        if not week_start <= scheduled < week_end:
            return None, "outside_week"
        if actual_present or policy_present:
            return scheduled, "scheduled_release_clock"
        return scheduled, "calendar_only_clock"
    if published is not None and week_start <= published < week_end:
        return published, "publisher_or_observation_clock"
    return None, "outside_week"


def _substantive_official_event(article: Mapping[str, Any], clock_basis: str) -> bool:
    """Reject newly discovered archive/navigation rows as current events.

    Official-domain provenance alone is not an event clock. A prospectively
    retained calendar clock, a current structured value, a dated statistical
    release, or a recognisable dated policy publication is required.
    """

    if clock_basis in {"calendar_only_clock", "scheduled_release_clock"}:
        return True
    if bool(article.get("published_time_inferred")):
        return False
    if article.get("actual_value") is not None:
        return True
    category = str(article.get("category") or "")
    role = str(article.get("source_role") or "")
    if role == "primary_statistical_release" and category in MACRO_RELEASE_CATEGORIES:
        return True
    if category == "sovereign_duration_liquidity_policy":
        return True
    if category in {
        "fx_intervention",
        "risk_off_geopolitical_or_financial",
        "risk_on_geopolitical_or_financial",
    }:
        return bool(article.get("relevant"))
    if role in {"primary_policy_release", "primary_policy_communication"}:
        return bool(POLICY_HEADLINE_RE.search(str(article.get("headline") or "")))
    return False


def _supporting_statistical_artifact(article: Mapping[str, Any]) -> bool:
    return bool(SUPPORTING_ARTIFACT_RE.search(str(article.get("headline") or "")))


def _has_source_change_information(article: Mapping[str, Any]) -> bool:
    if article.get("actual_value") is not None or article.get("release_components"):
        return True
    if (
        str(article.get("category") or "") == "monetary_policy"
        and POLICY_HEADLINE_RE.search(str(article.get("headline") or ""))
    ):
        return True
    current_text = f"{article.get('headline') or ''}. {str(article.get('summary') or '')[:3000]}"
    return bool(OBSERVED_CHANGE_RE.search(current_text))


def _dedup_key(article: Mapping[str, Any], currency: str) -> tuple[str, ...]:
    material_identity = news.structured_material_identity(article)
    if material_identity:
        # A structured release with a stable publisher identity and identical
        # economic values is one event even when a date-only feed caused old
        # collector versions to infer a new observation clock on every poll.
        # The raw rows remain immutable; only this retrospective view collapses
        # them. A changed value/revision/component set gets a new identity.
        return (
            "structured_material",
            str(article.get("source_id") or ""),
            currency,
            material_identity,
        )
    identity = (
        article.get("event_series_id")
        or article.get("event_name")
        or article.get("headline")
    )
    return (
        "legacy_event",
        str(article.get("source_id") or ""),
        currency,
        _normalized_identity(identity),
        str(article.get("reference_period") or ""),
        str(article.get("actual_value") if article.get("actual_value") is not None else ""),
        str(article.get("scheduled_utc") or ""),
    )


def load_official_events(
    database: Path,
    week_start: dt.datetime,
    week_end: dt.datetime,
) -> list[dict[str, Any]]:
    connection = sqlite3.connect(f"file:{database.resolve().as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """
            SELECT * FROM articles
            WHERE source_verified=1 AND source_quality>=0.9
              AND (
                (first_seen_utc>=? AND first_seen_utc<?)
                OR (
                  first_seen_utc < ?
                  AND
                  datetime(json_extract(payload_json,'$.scheduled_utc')) >= datetime(?)
                  AND datetime(json_extract(payload_json,'$.scheduled_utc')) < datetime(?)
                )
              )
            ORDER BY first_seen_utc,event_id
            """,
            (
                week_start.isoformat(),
                week_end.isoformat(),
                week_end.isoformat(),
                week_start.isoformat(),
                week_end.isoformat(),
            ),
        ).fetchall()
    finally:
        connection.close()
    grouped: dict[tuple[str, ...], dict[str, Any]] = {}
    for source_row in rows:
        raw = _as_payload(dict(source_row))
        first_seen = weekly.parse_utc(raw["first_seen_utc"])
        article = news.classify_article(raw, first_seen=first_seen)
        currency = _single_currency(article)
        if not currency:
            continue
        clock, basis = _event_clock(article, week_start, week_end)
        if clock is None:
            continue
        if not _substantive_official_event(article, basis):
            continue
        # A source listing/calendar without an actual value is retained as a
        # clock/coverage diagnostic, never represented as an observed release.
        article["audit_currency"] = currency
        article["audit_event_utc"] = weekly.iso_utc(clock)
        article["audit_clock_basis"] = basis
        key = _dedup_key(article, currency)
        current = grouped.get(key)
        if current is None or weekly.parse_utc(article["first_seen_utc"]) < weekly.parse_utc(current["first_seen_utc"]):
            grouped[key] = article
    return sorted(
        grouped.values(),
        key=lambda row: (row["audit_event_utc"], row["source_id"], row["headline"]),
    )


def _currency_factor_sign(factor: Mapping[str, Any], currency: str) -> int:
    value = float((factor.get("scores_bps") or {}).get(currency, 0.0))
    return 1 if value > 0 else -1 if value < 0 else 0


def _pair_expresses_factor(pair: str, side: str, currency: str, factor_sign: int) -> bool:
    if not factor_sign:
        return False
    base, quote = pair.split("_", 1)
    pair_sign = 1 if side == "long" else -1 if side == "short" else 0
    currency_sign = pair_sign if currency == base else -pair_sign if currency == quote else 0
    return currency_sign == factor_sign


def response_at_horizon(
    panel: Mapping[str, Sequence[Mapping[str, Any]]],
    event_time: dt.datetime,
    currency: str,
    horizon: int,
) -> dict[str, Any]:
    end = event_time + dt.timedelta(minutes=horizon)
    factor = weekly._factor_scores(panel, event_time, end)
    factor_sign = _currency_factor_sign(factor, currency)
    scores = factor.get("scores_bps") or {}
    ranking = list(factor.get("ranking") or [])
    try:
        rank_from_strongest = ranking.index(currency) + 1
        extreme_rank = min(rank_from_strongest, len(ranking) - rank_from_strongest + 1)
    except ValueError:
        rank_from_strongest = 0
        extreme_rank = 0
    paths: list[dict[str, Any]] = []
    for pair in sorted(panel):
        if currency not in pair.split("_"):
            continue
        try:
            path = weekly._pair_path(panel, pair, event_time, end)
        except weekly.WeeklyCaseError:
            continue
        path = {"instrument": pair, **path}
        path["factor_consistent"] = _pair_expresses_factor(
            pair, path["observed_side"], currency, factor_sign
        )
        base, quote = pair.split("_", 1)
        other_currency = quote if currency == base else base
        path["other_currency"] = other_currency
        path["source_currency_factor_bps"] = round(float(scores.get(currency, 0.0)), 6)
        path["other_currency_factor_bps"] = round(float(scores.get(other_currency, 0.0)), 6)
        path["source_currency_dominant"] = (
            abs(path["source_currency_factor_bps"]) >= abs(path["other_currency_factor_bps"])
        )
        path["liquidity_cost_bucket"] = (
            "entry_spread_le_5p" if float(path["entry_spread_pips"]) <= 5.0 else "entry_spread_gt_5p"
        )
        paths.append(path)
    preferred = [
        row
        for row in paths
        if row["factor_consistent"]
        and row["source_currency_dominant"]
        and float(row["entry_spread_pips"]) <= 5.0
    ]
    dominant_fallback = [
        row for row in paths if row["factor_consistent"] and row["source_currency_dominant"]
    ]
    factor_fallback = [row for row in paths if row["factor_consistent"]]
    ranked = preferred or dominant_fallback or factor_fallback or paths
    ranked.sort(
        key=lambda row: (
            float(row["observed_after_cost_pips"]),
            -float(row["entry_spread_pips"]),
            row["instrument"],
        ),
        reverse=True,
    )
    representative = ranked[0] if ranked else None
    return {
        "horizon_minutes": horizon,
        "currency_factor_bps": round(float((factor.get("scores_bps") or {}).get(currency, 0.0)), 6),
        "currency_factor_rank_from_strongest": rank_from_strongest,
        "currency_factor_extreme_rank": extreme_rank,
        "usable_pair_count": int(factor.get("usable_pair_count") or 0),
        "cost_clearing_factor_consistent_pair_count": sum(
            bool(row["factor_consistent"])
            and float(row["observed_after_cost_pips"]) > 0.0
            for row in paths
        ),
        "representative_path": representative,
    }


def _response_is_attribution_candidate(
    response: Mapping[str, Any],
    *,
    calendar_only: bool,
    source_latency_seconds: float,
    source_change_information: bool = True,
    supporting_artifact: bool = False,
) -> bool:
    path = response.get("representative_path")
    post_observation_path = response.get("post_observation_path")
    if (
        calendar_only
        or not isinstance(path, Mapping)
        or not isinstance(post_observation_path, Mapping)
    ):
        return False
    return bool(
        -5.0 <= source_latency_seconds <= 300.0
        and source_change_information
        and not supporting_artifact
        and abs(float(response.get("currency_factor_bps") or 0.0)) >= 3.0
        and 0 < int(response.get("currency_factor_extreme_rank") or 0) <= 5
        and bool(path.get("factor_consistent"))
        and bool(path.get("source_currency_dominant"))
        and float(path.get("observed_after_cost_pips") or 0.0) >= 5.0
        and post_observation_path.get("observed_side") == path.get("observed_side")
        and float(post_observation_path.get("observed_after_cost_pips") or 0.0) >= 5.0
    )


def _with_knowledge_time_paths(
    panel: Mapping[str, Sequence[Mapping[str, Any]]],
    response: Mapping[str, Any],
    event_time: dt.datetime,
    first_seen: dt.datetime,
) -> dict[str, Any]:
    result = dict(response)
    path = response.get("representative_path")
    result["pre_observation_path"] = None
    result["post_observation_path"] = None
    if not isinstance(path, Mapping):
        return result
    end = event_time + dt.timedelta(minutes=int(response["horizon_minutes"]))
    instrument = str(path["instrument"])
    if event_time < first_seen < end:
        try:
            result["pre_observation_path"] = weekly._pair_path(
                panel, instrument, event_time, first_seen
            )
        except weekly.WeeklyCaseError:
            pass
    knowledge_time = max(event_time, first_seen)
    if knowledge_time < end:
        try:
            result["post_observation_path"] = weekly._pair_path(
                panel, instrument, knowledge_time, end
            )
        except weekly.WeeklyCaseError:
            pass
    return result


def _event_episode_id(row: Mapping[str, Any]) -> str:
    event_time = weekly.parse_utc(row["event_utc"])
    five_minute_bucket = int(event_time.timestamp()) // 300
    return "official_episode_" + stable_hash(
        [row["currency"], five_minute_bucket]
    )[:20]


def _episode_representative_rank(row: Mapping[str, Any]) -> tuple[int, int, int, str]:
    basis_rank = {
        "scheduled_release_clock": 3,
        "publisher_or_observation_clock": 2,
        "calendar_only_clock": 1,
    }.get(str(row.get("clock_basis") or ""), 0)
    return (
        1 if row.get("actual_value") is not None else 0,
        1 if float(row.get("source_score") or 0.0) != 0.0 else 0,
        basis_rank,
        str(row.get("event_id") or ""),
    )


def compile_audit(
    database: Path = NEWS_DB,
    candle_root: Path = CANDLE_ROOT,
    week_start: dt.datetime | None = None,
    week_end: dt.datetime | None = None,
) -> dict[str, Any]:
    default_start, default_end = current_utc_week_window()
    start = week_start or default_start
    end = week_end or default_end
    if start.tzinfo is None:
        start = start.replace(tzinfo=UTC)
    else:
        start = start.astimezone(UTC)
    if end.tzinfo is None:
        end = end.replace(tzinfo=UTC)
    else:
        end = end.astimezone(UTC)
    if end <= start:
        raise OfficialEventAuditError("week_end_must_follow_week_start")
    events = load_official_events(database, start, end)
    if not events:
        raise OfficialEventAuditError("no_official_events")
    event_times = [weekly.parse_utc(row["audit_event_utc"]) for row in events]
    panel = weekly._load_candles(
        candle_root,
        min(event_times) - dt.timedelta(minutes=65),
        max(event_times) + dt.timedelta(minutes=max(HORIZONS) + 5),
    )
    if len(panel) != 68:
        raise OfficialEventAuditError(f"exact_68_pair_panel_required:{len(panel)}")
    audited: list[dict[str, Any]] = []
    for article in events:
        event_time = weekly.parse_utc(article["audit_event_utc"])
        first_seen = weekly.parse_utc(article["first_seen_utc"])
        responses: list[dict[str, Any]] = []
        for horizon in HORIZONS:
            maturity = event_time + dt.timedelta(minutes=horizon)
            if maturity > end:
                responses.append(
                    {
                        "horizon_minutes": horizon,
                        "maturity_utc": weekly.iso_utc(maturity),
                        "maturity_state": "not_matured_at_frozen_cutoff",
                        "currency_factor_bps": None,
                        "currency_factor_rank_from_strongest": None,
                        "currency_factor_extreme_rank": None,
                        "usable_pair_count": 0,
                        "cost_clearing_factor_consistent_pair_count": 0,
                        "representative_path": None,
                        "pre_observation_path": None,
                        "post_observation_path": None,
                    }
                )
                continue
            response = _with_knowledge_time_paths(
                panel,
                response_at_horizon(
                    panel, event_time, article["audit_currency"], horizon
                ),
                event_time,
                first_seen,
            )
            response["maturity_utc"] = weekly.iso_utc(maturity)
            response["maturity_state"] = "matured_at_frozen_cutoff"
            responses.append(response)
        viable = [
            row
            for row in responses
            if row["representative_path"] is not None
        ]
        best = max(
            viable,
            key=lambda row: float(row["representative_path"]["observed_after_cost_pips"]),
            default=None,
        )
        source_scores = article.get("currency_scores") or article.get("research_currency_scores") or {}
        source_score = float(source_scores.get(article["audit_currency"], 0.0))
        factor_sign = 0 if best is None else (1 if best["currency_factor_bps"] > 0 else -1 if best["currency_factor_bps"] < 0 else 0)
        source_sign = 1 if source_score > 0 else -1 if source_score < 0 else 0
        if not source_sign:
            direction_state = "source_direction_unresolved"
        elif source_sign == factor_sign:
            direction_state = "source_direction_aligned"
        else:
            direction_state = "source_direction_opposed"
        source_latency_seconds = round((first_seen - event_time).total_seconds(), 3)
        supporting_artifact = _supporting_statistical_artifact(article)
        source_change_information = _has_source_change_information(article)
        attribution_responses = [
            row
            for row in responses
            if _response_is_attribution_candidate(
                row,
                calendar_only=article["audit_clock_basis"] == "calendar_only_clock",
                source_latency_seconds=source_latency_seconds,
                source_change_information=source_change_information,
                supporting_artifact=supporting_artifact,
            )
        ]
        best_attribution = max(
            attribution_responses,
            key=lambda row: float(row["post_observation_path"]["observed_after_cost_pips"]),
            default=None,
        )
        audited_row = {
                "event_id": article["event_id"],
                "source_id": article["source_id"],
                "headline": article["headline"],
                "source_url": article["source_url"],
                "currency": article["audit_currency"],
                "event_utc": article["audit_event_utc"],
                "clock_basis": article["audit_clock_basis"],
                "first_seen_utc": article["first_seen_utc"],
                "source_latency_seconds": source_latency_seconds,
                "category": article["category"],
                "actual_value": article.get("actual_value"),
                "consensus_value": article.get("consensus_value"),
                "previous_value": article.get("previous_value"),
                "source_score": source_score,
                "source_change_information": source_change_information,
                "supporting_statistical_artifact": supporting_artifact,
                "direction_state": direction_state,
                "responses": responses,
                "best_retrospective_response": best,
                "best_attribution_candidate": best_attribution,
                "calendar_only": article["audit_clock_basis"] == "calendar_only_clock",
                "research_only": True,
                "execution_eligible": False,
            }
        audited_row["event_episode_id"] = _event_episode_id(audited_row)
        audited.append(audited_row)
    episode_members: dict[str, list[dict[str, Any]]] = {}
    for row in audited:
        episode_members.setdefault(row["event_episode_id"], []).append(row)
    episode_representatives: list[dict[str, Any]] = []
    for episode_id, members in episode_members.items():
        representative = max(members, key=_episode_representative_rank)
        representative = dict(representative)
        representative["episode_source_item_count"] = len(members)
        representative["episode_headlines"] = sorted({row["headline"] for row in members})
        representative["event_episode_id"] = episode_id
        episode_representatives.append(representative)
    episode_representatives.sort(key=lambda row: (row["event_utc"], row["event_episode_id"]))
    cost_clearing = [
        row
        for row in episode_representatives
        if row["best_retrospective_response"] is not None
        and float(row["best_retrospective_response"]["representative_path"]["observed_after_cost_pips"]) > 0.0
    ]
    cost_clearing.sort(
        key=lambda row: float(row["best_retrospective_response"]["representative_path"]["observed_after_cost_pips"]),
        reverse=True,
    )
    attribution_candidates = [
        row for row in episode_representatives if row["best_attribution_candidate"] is not None
    ]
    attribution_candidates.sort(
        key=lambda row: float(row["best_attribution_candidate"]["post_observation_path"]["observed_after_cost_pips"]),
        reverse=True,
    )
    payload = {
        "schema_version": 1,
        "contract_id": CONTRACT_ID,
        "generated_utc": weekly.iso_utc(dt.datetime.now(UTC)),
        "week_start_utc": weekly.iso_utc(start),
        "week_end_utc": weekly.iso_utc(end),
        "frozen_cutoff_utc": weekly.iso_utc(end),
        "pair_universe_count": len(panel),
        "deduplicated_official_source_item_count": len(audited),
        "independent_event_clock_count": len(episode_representatives),
        "calendar_only_event_count": sum(row["calendar_only"] for row in episode_representatives),
        "cost_clearing_event_count": len(cost_clearing),
        "strict_attribution_candidate_count": len(attribution_candidates),
        "direction_resolved_count": sum(row["source_score"] != 0 for row in episode_representatives),
        "direction_aligned_count": sum(row["direction_state"] == "source_direction_aligned" for row in episode_representatives),
        "direction_opposed_count": sum(row["direction_state"] == "source_direction_opposed" for row in episode_representatives),
        "events": audited,
        "event_episode_representatives": episode_representatives,
        "top_cost_clearing_events": cost_clearing[:30],
        "strict_attribution_candidates": attribution_candidates,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "execution_decision": "no_trade",
    }
    payload["audit_hash"] = stable_hash(payload)
    return payload


def render_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Current-week official event responses",
        "",
        f"Generated: `{payload['generated_utc']}`",
        "",
        "Retrospective research only. The representative path is selected after the outcome and is not a forecast.",
        "",
        f"- Deduplicated official source items: **{payload['deduplicated_official_source_item_count']}**.",
        f"- Independent five-minute currency/event clocks: **{payload['independent_event_clock_count']}**.",
        f"- Calendar-only clocks: **{payload['calendar_only_event_count']}**.",
        f"- Events with at least one cost-clearing fixed-horizon response: **{payload['cost_clearing_event_count']}**.",
        f"- Strict movement/source attribution candidates: **{payload['strict_attribution_candidate_count']}**.",
        f"- Source direction resolved: **{payload['direction_resolved_count']}**; aligned **{payload['direction_aligned_count']}**, opposed **{payload['direction_opposed_count']}**.",
        "- Execution decision: **no_trade**.",
        "",
        "## Strict attribution candidates",
        "",
        "| Event | Clock | Currency | Pair | Horizon | Post-seen net | Factor | Latency | Source direction |",
        "|---|---|---|---|---:|---:|---:|---:|---|",
    ]
    for row in payload["strict_attribution_candidates"]:
        best = row["best_attribution_candidate"]
        path = best["representative_path"]
        post = best["post_observation_path"]
        lines.append(
            f"| {row['headline'][:70]} | {row['event_utc']} | {row['currency']} | {path['instrument']} | "
            f"{best['horizon_minutes']}m | {post['observed_after_cost_pips']:+.1f}p | "
            f"{best['currency_factor_bps']:+.2f}bps | {row['source_latency_seconds']:.1f}s | {row['direction_state']} |"
        )
    lines.extend(
        [
            "",
            "## Largest event-clock responses (post-hoc discovery only)",
            "",
            "These select the best pair and horizon after the fact and therefore are not causal matches by themselves.",
            "",
            "| Event | Clock | Currency | Pair | Horizon | Net | Spread bucket | Factor |",
            "|---|---|---|---|---:|---:|---|---:|",
        ]
    )
    for row in payload["top_cost_clearing_events"][:15]:
        best = row["best_retrospective_response"]
        path = best["representative_path"]
        lines.append(
            f"| {row['headline'][:70]} | {row['event_utc']} | {row['currency']} | {path['instrument']} | "
            f"{best['horizon_minutes']}m | {path['observed_after_cost_pips']:+.1f}p | "
            f"{path['liquidity_cost_bucket']} | {best['currency_factor_bps']:+.2f}bps |"
        )
    lines.extend(
        [
            "",
            "## Interpretation safeguards",
            "",
            "- Repeated source rows are one event, not repeated evidence.",
            "- Calendar-only rows measure coverage; they are not actual releases.",
            "- Empty source direction means abstention, not a failed call.",
            "- Pair breadth is one currency-factor episode, not independent wins.",
            "- Best response is a retrospective movement diagnostic only.",
            "",
            f"Audit hash: `{payload['audit_hash']}`",
            "",
        ]
    )
    return "\n".join(lines)


def write_outputs(payload: Mapping[str, Any], report_root: Path = REPORT_ROOT) -> None:
    atomic_text(report_root / OUTPUT_JSON.name, json.dumps(payload, indent=2, ensure_ascii=False))
    atomic_text(report_root / OUTPUT_MD.name, render_markdown(payload))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--news-database", type=Path, default=NEWS_DB)
    parser.add_argument("--candle-root", type=Path, default=CANDLE_ROOT)
    parser.add_argument("--report-root", type=Path, default=REPORT_ROOT)
    parser.add_argument(
        "--week-start-utc",
        type=weekly.parse_utc,
        help="Optional inclusive UTC start for a reproducible historical audit.",
    )
    parser.add_argument(
        "--week-end-utc",
        type=weekly.parse_utc,
        help="Optional exclusive UTC end for a reproducible historical audit.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = compile_audit(
        args.news_database,
        args.candle_root,
        week_start=args.week_start_utc,
        week_end=args.week_end_utc,
    )
    write_outputs(payload, args.report_root)
    print(
        json.dumps(
            {
                "output": str(args.report_root / OUTPUT_MD.name),
                "events": payload["independent_event_clock_count"],
                "cost_clearing": payload["cost_clearing_event_count"],
                "strict_attribution_candidates": payload["strict_attribution_candidate_count"],
                "resolved": payload["direction_resolved_count"],
                "aligned": payload["direction_aligned_count"],
                "opposed": payload["direction_opposed_count"],
                "execution_decision": payload["execution_decision"],
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
