#!/usr/bin/env python3
"""Read-only historical quality audit for the FX narrative/news corpus.

The audit deliberately separates four populations which are often confused:

* the current article view used for collection diagnostics;
* immutable/versioned source-governance history;
* point-in-time rows that can be replayed without publication-clock leakage;
* movement-conditioned blurb research, which is hypothesis discovery only.

It performs no network access and contains no broker or lifecycle write path.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import os
import sqlite3
import statistics
import tempfile
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
DEFAULT_NEWS = DATA / "local_news_sentiment" / "local_news_sentiment_v1.sqlite"
DEFAULT_GOVERNANCE = DATA / "state" / "source_governance_v1.json"
DEFAULT_CANDLES = DATA / "candles"
DEFAULT_BLURB = DATA / "state" / "spike_blurb_factor_reconstruction_v1.sqlite"
DEFAULT_GDELT = DATA / "reports" / "news_mapping" / "GDELT_ATTENTION_MAPPING_AUDIT_20260808.json"
DEFAULT_DIRECTION = DATA / "reports" / "news_blurb_direction_attribution" / "NEWS_BLURB_DIRECTION_ATTRIBUTION_V1.json"
DEFAULT_OUTPUT = DATA / "reports" / "news_historical_quality" / "NEWS_HISTORICAL_QUALITY_V1.json"
DEFAULT_REPORT = DATA / "reports" / "news_historical_quality" / "NEWS_HISTORICAL_QUALITY_V1.md"

CURRENCIES = (
    "AUD", "CAD", "CHF", "CNH", "CZK", "DKK", "EUR", "GBP", "HKD",
    "HUF", "JPY", "MXN", "NOK", "NZD", "PLN", "SEK", "SGD", "THB",
    "TRY", "USD", "ZAR",
)
CAUSAL_CONSENSUS_STATES = {
    "prospectively_captured_pre_release",
    "causal_pre_release_snapshot",
}


def parse_json(value: Any, default: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value or ""))
    except (TypeError, ValueError, json.JSONDecodeError):
        return default


def parse_time(value: Any) -> dt.datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def quantile(values: Iterable[float], fraction: float) -> float | None:
    ordered = sorted(float(value) for value in values if math.isfinite(float(value)))
    if not ordered:
        return None
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * fraction
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle, temporary = tempfile.mkstemp(prefix=path.name + ".", dir=path.parent)
    try:
        with os.fdopen(handle, "w", encoding="utf-8", newline="") as stream:
            stream.write(value)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def load_articles(path: Path) -> list[dict[str, Any]]:
    connection = sqlite3.connect(
        f"file:{path.resolve().as_posix()}?mode=ro", uri=True, timeout=30.0
    )
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """
            SELECT event_id,source_id,source_name,source_kind,source_verified,
                   published_utc,first_seen_utc,last_seen_utc,headline,relevant,
                   category,currencies_json,currency_scores_json,
                   generic_sentiment_score,directional_confidence,severity,
                   movement_potential,duplicate_count,payload_json
            FROM articles ORDER BY first_seen_utc,event_id
            """
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        connection.close()


def first_and_last_csv_row(path: Path) -> tuple[dict[str, str], dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        first = next(reader, None)
        last = first
        for row in reader:
            last = row
    if first is None or last is None:
        raise ValueError(f"empty candle archive: {path}")
    return first, last


def candle_coverage(path: Path) -> dict[str, Any]:
    pairs: dict[str, dict[str, Any]] = {}
    currency_ranges: dict[str, list[tuple[dt.datetime, dt.datetime]]] = defaultdict(list)
    for source in sorted(path.glob("*_M1.csv")):
        instrument = source.stem.removesuffix("_M1")
        pieces = instrument.split("_")
        if len(pieces) != 2:
            continue
        try:
            first, last = first_and_last_csv_row(source)
            start = parse_time(first.get("datetime") or first.get("time"))
            end = parse_time(last.get("datetime") or last.get("time"))
        except (OSError, ValueError):
            continue
        if start is None or end is None or end < start:
            continue
        pairs[instrument] = {
            "start_utc": start.isoformat(),
            "end_utc": end.isoformat(),
            "bytes": source.stat().st_size,
        }
        for currency in pieces:
            currency_ranges[currency].append((start, end))
    starts = [parse_time(row["start_utc"]) for row in pairs.values()]
    ends = [parse_time(row["end_utc"]) for row in pairs.values()]
    starts = [value for value in starts if value]
    ends = [value for value in ends if value]
    return {
        "pair_count": len(pairs),
        "currency_count": len(currency_ranges),
        "total_bytes": sum(row["bytes"] for row in pairs.values()),
        "earliest_start_utc": min(starts).isoformat() if starts else None,
        "latest_start_utc": max(starts).isoformat() if starts else None,
        "earliest_end_utc": min(ends).isoformat() if ends else None,
        "latest_end_utc": max(ends).isoformat() if ends else None,
        "pairs": pairs,
        "_currency_ranges": currency_ranges,
    }


def price_covered(
    stamp: dt.datetime | None,
    currencies: Iterable[str],
    ranges: Mapping[str, list[tuple[dt.datetime, dt.datetime]]],
) -> bool:
    if stamp is None:
        return False
    return any(
        start <= stamp <= end
        for currency in currencies
        for start, end in ranges.get(currency, [])
    )


def _story_id(row: Mapping[str, Any], payload: Mapping[str, Any]) -> str:
    explicit = payload.get("event_lineage_id") or payload.get("story_cluster_id")
    if explicit:
        return str(explicit)
    headline = " ".join(str(row.get("headline") or "").lower().split())
    # Most discovery headlines append a publisher after the final separator.
    return headline.rsplit(" - ", 1)[0] or str(row.get("event_id") or "")


def audit_articles(
    rows: Iterable[Mapping[str, Any]],
    currency_ranges: Mapping[str, list[tuple[dt.datetime, dt.datetime]]],
) -> dict[str, Any]:
    rows = list(rows)
    counts = Counter()
    sources = Counter()
    kinds = Counter()
    categories = Counter()
    classifiers = Counter()
    days = Counter()
    stories: set[str] = set()
    latencies: list[float] = []
    currency_rows: dict[str, Counter[str]] = {currency: Counter() for currency in CURRENCIES}
    replay_story_ids: set[str] = set()
    observed_times: list[dt.datetime] = []

    for row in rows:
        counts["articles"] += 1
        sources[str(row.get("source_id") or "unknown")] += 1
        kinds[str(row.get("source_kind") or "unknown")] += 1
        categories[str(row.get("category") or "unknown")] += 1
        payload = parse_json(row.get("payload_json"), {})
        classifier = str(payload.get("classification_version") or "unknown")
        classifiers[classifier] += 1
        first_seen = parse_time(row.get("first_seen_utc"))
        published = parse_time(row.get("published_utc"))
        if first_seen:
            observed_times.append(first_seen)
            days[first_seen.date().isoformat()] += 1
        if first_seen and published:
            latency = (first_seen - published).total_seconds()
            if latency >= 0:
                latencies.append(latency)
            else:
                counts["published_after_first_seen"] += 1

        currencies = sorted(
            {
                str(value).upper()
                for value in parse_json(row.get("currencies_json"), [])
                if str(value).upper() in CURRENCIES
            }
        )
        scores = {
            str(key).upper(): float(value)
            for key, value in parse_json(row.get("currency_scores_json"), {}).items()
            if str(key).upper() in CURRENCIES
            and isinstance(value, (int, float))
            and not isinstance(value, bool)
            and math.isfinite(float(value))
            and abs(float(value)) > 1e-12
        }
        story = _story_id(row, payload)
        stories.add(story)
        mapped = bool(currencies)
        relevant = bool(row.get("relevant"))
        verified = bool(row.get("source_verified"))
        direct = payload.get("source_direct") is True
        clock_trusted = payload.get("observation_clock_trusted") is True
        timely = payload.get("forward_signal_timely") is True
        structured = payload.get("structured_event") is True
        actual = payload.get("actual_value")
        consensus = payload.get("consensus_value")
        consensus_state = str(payload.get("consensus_capture_state") or "")
        covered = price_covered(first_seen, currencies, currency_ranges)
        directional = bool(scores)
        replay = bool(clock_trusted and timely and relevant and mapped and directional and covered)
        official_replay = bool(replay and verified and direct)
        causal_surprise = bool(
            structured and actual is not None and consensus is not None
            and consensus_state in CAUSAL_CONSENSUS_STATES
        )
        diagnostic_direction = bool(relevant and mapped and directional and covered)
        diagnostic_timely = bool(diagnostic_direction and timely)
        diagnostic_trusted = bool(diagnostic_direction and clock_trusted)

        for name, flag in (
            ("mapped", mapped), ("relevant", relevant), ("verified", verified),
            ("direct", direct), ("clock_trusted", clock_trusted),
            ("forward_timely", timely), ("directional", directional),
            ("structured", structured), ("price_covered", covered),
            ("diagnostic_direction_candidates", diagnostic_direction),
            ("diagnostic_timely_direction_candidates", diagnostic_timely),
            ("diagnostic_trusted_direction_candidates", diagnostic_trusted),
            ("replay_candidates", replay),
            ("official_replay_candidates", official_replay),
            ("causal_surprise_ready", causal_surprise),
        ):
            counts[name] += int(flag)
        counts["structured_actual_without_causal_consensus"] += int(
            structured and actual is not None and not causal_surprise
        )
        if replay:
            replay_story_ids.add(story)
        for currency in currencies:
            target = currency_rows[currency]
            target["mapped"] += 1
            target["relevant"] += int(relevant)
            target["directional"] += int(currency in scores)
            target["verified"] += int(verified)
            target["direct"] += int(direct)
            target["replay_candidates"] += int(replay and currency in scores)
            target["official_replay_candidates"] += int(
                official_replay and currency in scores
            )
            target["causal_surprise_ready"] += int(causal_surprise)

    span_days = 0.0
    if observed_times:
        span_days = (max(observed_times) - min(observed_times)).total_seconds() / 86400.0
    daily_values = list(days.values())
    top_source_rows = [
        {"source_id": key, "articles": value}
        for key, value in sources.most_common(25)
    ]
    return {
        "counts": dict(counts),
        "first_seen_min_utc": min(observed_times).isoformat() if observed_times else None,
        "first_seen_max_utc": max(observed_times).isoformat() if observed_times else None,
        "calendar_span_days": round(span_days, 3),
        "observed_utc_days": len(days),
        "independent_current_view_stories": len(stories),
        "replay_candidate_stories": len(replay_story_ids),
        "current_view_story_redundancy_fraction": round(
            1.0 - len(stories) / len(rows), 6
        ) if rows else None,
        "latency_seconds": {
            "n": len(latencies),
            "p50": quantile(latencies, 0.50),
            "p90": quantile(latencies, 0.90),
            "p99": quantile(latencies, 0.99),
        },
        "daily_article_count": {
            "p50": quantile(daily_values, 0.50),
            "p90": quantile(daily_values, 0.90),
            "maximum": max(daily_values) if daily_values else None,
        },
        "per_currency": {key: dict(value) for key, value in currency_rows.items()},
        "top_sources": top_source_rows,
        "top_five_source_fraction": (
            sum(row["articles"] for row in top_source_rows[:5]) / len(rows)
            if rows else None
        ),
        "source_kind_counts": dict(kinds.most_common()),
        "category_counts": dict(categories.most_common()),
        "classifier_counts": dict(classifiers.most_common()),
    }


def recovered_blurb_counts(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {"available": False}
    connection = sqlite3.connect(
        f"file:{path.resolve().as_posix()}?mode=ro", uri=True, timeout=10.0
    )
    try:
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        wanted = (
            "factor_observations", "source_attributions",
            "factor_response_observations", "response_detections",
            "response_entry_outcomes", "representative_cases",
            "unmatched_research_episodes", "verified_external_source_cases",
        )
        counts = {}
        for name in wanted:
            counts[name] = (
                int(connection.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0])
                if name in tables else None
            )
        return {
            "available": True,
            "counts": counts,
            "evidence_class": "movement_conditioned_hypothesis_discovery_not_forecast_backtest",
        }
    finally:
        connection.close()


def read_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def historical_evidence_summary(gdelt: Mapping[str, Any], direction: Mapping[str, Any]) -> dict[str, Any]:
    gdelt_news = [
        row for row in gdelt.get("arm_summaries", [])
        if isinstance(row, Mapping) and row.get("arm") == "news_only"
    ]
    published = (
        direction.get("arms", {}).get("published_directional", {})
        if isinstance(direction.get("arms"), Mapping) else {}
    )
    return {
        "gdelt_generic_tone": {
            "market_days": gdelt.get("market_days"),
            "independent_currency_story_rows": gdelt.get("mapping_quality", {}).get("independent_currency_story_rows") if isinstance(gdelt.get("mapping_quality"), Mapping) else None,
            "news_only": gdelt_news,
            "conclusion": "no_confirmed_edge_short_window_generic_tone_negative_at_15m_and_60m",
        },
        "movement_conditioned_direction": {
            "metrics": published.get("metrics") if isinstance(published, Mapping) else None,
            "by_liquidity": published.get("by_liquidity") if isinstance(published, Mapping) else None,
            "conclusion": "selection_biased_discovery_population_not_prediction_accuracy",
        },
    }


def render_report(payload: Mapping[str, Any]) -> str:
    audit = payload["article_current_view"]
    counts = audit["counts"]
    immutable = payload["immutable_governance"]
    prices = payload["price_archive"]
    evidence = payload["historical_evidence"]
    gdelt = evidence["gdelt_generic_tone"]
    lines = [
        "# News historical quality baseline",
        "",
        f"Generated: `{payload['generated_utc']}`",
        "",
        "Research-only, read-only audit. It cannot authorize or place orders.",
        "",
        "## Bottom line",
        "",
        f"- Current article view: **{counts.get('articles', 0):,}** rows across **{audit['observed_utc_days']}** UTC days / **{audit['calendar_span_days']:.1f}** calendar days.",
        f"- Immutable governance: **{immutable.get('events', 0):,}** versions, **{immutable.get('story_clusters', 0):,}** canonical story clusters, **{immutable.get('supersessions', 0):,}** supersessions.",
        f"- Point-in-time directional replay candidates: **{counts.get('replay_candidates', 0):,}** article rows / **{audit['replay_candidate_stories']:,}** story identities.",
        f"- Retrospectively classifiable, price-covered direction rows: **{counts.get('diagnostic_direction_candidates', 0):,}**; forward-timely subset: **{counts.get('diagnostic_timely_direction_candidates', 0):,}**.",
        f"- Direct verified official replay candidates: **{counts.get('official_replay_candidates', 0):,}**.",
        f"- Causally captured actual-versus-consensus surprises: **{counts.get('causal_surprise_ready', 0):,}**.",
        f"- Executable OANDA M1 archive: **{prices['pair_count']}/68 pairs**, **{prices['currency_count']}/21 currencies**, {prices['total_bytes'] / 1024**2:.1f} MiB.",
        "- Supported assessment: **diagnostic history exists; the dataset is not yet a prospective news-alpha proof set**.",
        "",
        f"The five largest discovery feeds contribute **{float(audit.get('top_five_source_fraction') or 0):.1%}** of articles. The dominant limitations are short live history, broad-media concentration/syndication, retrospective classifier evolution, and zero causal consensus surprises. Raw row count is not independent evidence.",
        "",
        "## Replay-quality funnel",
        "",
        "| Gate | Rows | Share of current view |",
        "|---|---:|---:|",
    ]
    total = max(1, int(counts.get("articles", 0)))
    labels = (
        ("Mapped to at least one of 21 currencies", "mapped"),
        ("Marked FX-relevant", "relevant"),
        ("Nonzero currency direction", "directional"),
        ("Trusted observation clock", "clock_trusted"),
        ("Marked forward-timely", "forward_timely"),
        ("Overlaps retained OANDA M1", "price_covered"),
        ("Relevant + mapped + direction + price (retrospective diagnostic)", "diagnostic_direction_candidates"),
        ("Diagnostic rows also marked forward-timely", "diagnostic_timely_direction_candidates"),
        ("Diagnostic rows also have trusted clock", "diagnostic_trusted_direction_candidates"),
        ("All replay gates", "replay_candidates"),
        ("All replay gates + direct verified", "official_replay_candidates"),
        ("Causal actual/consensus surprise", "causal_surprise_ready"),
    )
    for label, key in labels:
        value = int(counts.get(key, 0))
        lines.append(f"| {label} | {value:,} | {value / total:.1%} |")
    lines += [
        "",
        "## Currency balance",
        "",
        "| Currency | Mapped | Relevant | Directional | Replay | Official replay | Causal surprise |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for currency, row in audit["per_currency"].items():
        lines.append(
            f"| {currency} | {row.get('mapped', 0):,} | {row.get('relevant', 0):,} | "
            f"{row.get('directional', 0):,} | {row.get('replay_candidates', 0):,} | "
            f"{row.get('official_replay_candidates', 0):,} | {row.get('causal_surprise_ready', 0):,} |"
        )
    lines += [
        "",
        "## Existing historical tests",
        "",
        f"The frozen GDELT baseline covered **{gdelt.get('market_days') or 0} market days** and **{gdelt.get('independent_currency_story_rows') or 0}** independent currency/story rows.",
        "",
        "| Horizon | N | After-cost win | Mean net bps | Ex-best-day bps |",
        "|---:|---:|---:|---:|---:|",
    ]
    for row in gdelt.get("news_only", []):
        lines.append(
            f"| {row.get('horizon_minutes')}m | {row.get('raw_n')} | "
            f"{float(row.get('after_cost_win_rate') or 0):.1%} | "
            f"{float(row.get('mean_net_bps') or 0):.3f} | "
            f"{float(row.get('mean_without_best_day_bps') or 0):.3f} |"
        )
    lines += [
        "",
        "Generic media tone did not show an after-cost edge at 15m or 60m. The apparently positive 4h mean failed the ex-best-day check. The recovered blurb/movement corpus is valuable for generating factor and timing rules, but because it selected moves first it cannot estimate live win rate.",
        "",
        "## Interpretation",
        "",
        "Collection breadth is strong: all 21 currencies and all 68 executable pairs are represented. Backtest quality is currently weak: the clean live observation window is measured in weeks, not regimes; official numeric releases mostly lack causal consensus; and classifier versions were improved after seeing historical misses. Those rows belong in discovery, followed by frozen prospective confirmation.",
        "",
        "The next honest test is an all-decision-clock comparison of source-only, price-only, combined, and timestamp-permuted placebo arms. It must include no-news/no-move controls and executable bid/ask costs; it must not select episodes because a spike occurred.",
        "",
    ]
    return "\n".join(lines)


def run(
    *,
    news: Path = DEFAULT_NEWS,
    governance: Path = DEFAULT_GOVERNANCE,
    candles: Path = DEFAULT_CANDLES,
    blurb: Path = DEFAULT_BLURB,
    gdelt: Path = DEFAULT_GDELT,
    direction: Path = DEFAULT_DIRECTION,
    output: Path = DEFAULT_OUTPUT,
    report: Path = DEFAULT_REPORT,
) -> dict[str, Any]:
    price = candle_coverage(candles)
    ranges = price.pop("_currency_ranges")
    article_audit = audit_articles(load_articles(news), ranges)
    governance_state = read_json(governance)
    immutable = governance_state.get("counts", {}) if isinstance(governance_state.get("counts"), Mapping) else {}
    payload = {
        "schema_version": "news_historical_quality_v1",
        "generated_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "orders_placed": 0,
        "supported_action": "build_all_decision_clock_news_price_placebo_backtest",
        "article_current_view": article_audit,
        "immutable_governance": immutable,
        "price_archive": price,
        "recovered_blurb_research": recovered_blurb_counts(blurb),
        "historical_evidence": historical_evidence_summary(
            read_json(gdelt), read_json(direction)
        ),
        "readiness": {
            "currency_coverage": "complete" if price["currency_count"] == 21 else "incomplete",
            "pair_price_coverage": "complete" if price["pair_count"] == 68 else "incomplete",
            "point_in_time_backtest": "diagnostic_only_short_and_classifier_adaptive",
            "causal_macro_surprise": "blocked_no_pre_release_consensus" if not article_audit["counts"].get("causal_surprise_ready") else "available",
            "news_alpha": "not_demonstrated_after_cost",
        },
    }
    atomic_text(output, json.dumps(payload, indent=2, sort_keys=True))
    atomic_text(report, render_report(payload))
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--news", type=Path, default=DEFAULT_NEWS)
    parser.add_argument("--governance", type=Path, default=DEFAULT_GOVERNANCE)
    parser.add_argument("--candles", type=Path, default=DEFAULT_CANDLES)
    parser.add_argument("--blurb", type=Path, default=DEFAULT_BLURB)
    parser.add_argument("--gdelt", type=Path, default=DEFAULT_GDELT)
    parser.add_argument("--direction", type=Path, default=DEFAULT_DIRECTION)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    args = parser.parse_args()
    payload = run(**vars(args))
    print(json.dumps({
        "output": str(args.output),
        "report": str(args.report),
        "supported_action": payload["supported_action"],
        "counts": payload["article_current_view"]["counts"],
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
