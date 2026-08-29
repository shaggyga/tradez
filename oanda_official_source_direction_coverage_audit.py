#!/usr/bin/env python3
"""Audit the official-source path from listing to timely currency mapping.

This is a read-only, research-only coverage report.  It distinguishes broad
listing inventory from documents whose details were causally available and
whose currency interpretation was timely.  It cannot promote or execute.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Mapping

import oanda_local_news_sentiment as news


ROOT = Path(__file__).resolve().parent
DEFAULT_DATABASE = (
    ROOT / "data" / "oanda_training_manager" / "state" / "source_governance_v1.sqlite"
)
DEFAULT_CONFIG = ROOT / "config" / "news_sources_v1.json"
REPORT_ROOT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "official_source_direction_coverage"
)
DEFAULT_JSON = REPORT_ROOT / "OFFICIAL_SOURCE_DIRECTION_COVERAGE_CURRENT.json"
DEFAULT_MD = REPORT_ROOT / "OFFICIAL_SOURCE_DIRECTION_COVERAGE_CURRENT.md"
OFFICIAL_POPULATIONS = {
    "official_policy_publisher",
    "official_macro_publisher",
    "official_rates_curve",
}
RATE_SETTING_PRIORITY = {
    "fed_monetary_policy": 1,
    "ecb_press": 1,
    "boe_news": 1,
    "boc_press": 1,
    "rba_media": 1,
    "boj_updates": 1,
    "snb_monetary_policy": 1,
    "snb_press": 1,
    "norges_press": 1,
    "riksbank_press": 1,
    "cnb_press": 1,
    "tcmb_press": 1,
    "hkma_press_api": 2,
    "japan_mof_international_policy": 2,
    "japan_mof_press_conferences_ja": 2,
    "us_treasury_press": 2,
    "us_treasury_daily_yield_curve": 1,
}


def open_readonly(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(
        f"file:{path.resolve().as_posix()}?mode=ro",
        uri=True,
        timeout=60,
    )
    connection.execute("PRAGMA query_only=ON")
    return connection


def raw_payload(payload_json: str) -> dict[str, Any]:
    try:
        payload = json.loads(payload_json)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    raw = payload.get("raw_payload") if isinstance(payload, Mapping) else None
    return dict(raw) if isinstance(raw, Mapping) else dict(payload)


def value_present(value: Any) -> bool:
    """Return true only for a real scalar value, never an empty placeholder."""
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def event_metrics(raw: Mapping[str, Any]) -> dict[str, bool]:
    direction = bool(raw.get("currency_scores") or raw.get("directional_bias"))
    detail = bool(
        raw.get("detail_enriched")
        or raw.get("detail_available_utc")
        or raw.get("detail_text_characters")
    )
    actual = value_present(raw.get("actual")) or value_present(
        raw.get("actual_value")
    )
    return {
        "detail": detail,
        "directional": direction,
        "timely_directional": bool(direction and raw.get("forward_signal_timely")),
        "actual": actual,
        "prospective_actual": bool(actual and raw.get("prospective_eligible")),
        "consensus": value_present(raw.get("consensus"))
        or value_present(raw.get("consensus_value")),
        "causal_consensus": bool(
            raw.get("consensus_observed_before_release") is True
            or str(raw.get("consensus_capture_state") or "").strip().lower()
            in {"captured_pre_release", "pre_release_verified"}
        ),
        "structured": bool(raw.get("structured_event")),
        "published_time_inferred": bool(raw.get("published_time_inferred")),
        "official_policy_release": bool(raw.get("official_policy_release")),
        "prospective_eligible": bool(raw.get("prospective_eligible")),
    }


def render_markdown(payload: Mapping[str, Any]) -> str:
    totals = payload["totals"]
    lines = [
        "# Official Source Direction Coverage",
        "",
        f"Generated: `{payload['generated_utc']}`",
        "",
        "Research-only. This report measures the causal path `official listing -> usable detail -> direction -> timely direction`; it cannot alter lifecycle, authorization, or execution.",
        "",
        f"- Append-only official policy/macro/rates rows: **{totals['rows']:,}**",
        f"- Unique latest-version documents/observations: **{totals['unique_documents']:,}**",
        f"- Usable document detail: **{totals['detail']:,}**",
        f"- Directional mappings: **{totals['directional']:,}**",
        f"- Timely directional mappings: **{totals['timely_directional']:,}**",
        f"- Retained actuals / prospective actuals: **{totals['actual']:,} / {totals['prospective_actual']:,}**",
        f"- Nonempty consensus fields: **{totals['consensus']:,}**",
        f"- Causally certified pre-release consensus observations: **{totals['causal_consensus']:,}**",
        "",
        "| Priority | Source | Population | Rows | Documents | Prospective | Detail | Directional | Timely | Actual | Prospective actual | Consensus field | Causal pre-release | Current blocker |",
        "|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in payload["sources"]:
        lines.append(
            f"| {row['priority']} | {row['source_id']} | {row['source_population']} | "
            f"{row['rows']:,} | {row['unique_documents']:,} | "
            f"{row['prospective_eligible']:,} | "
            f"{row['detail']:,} | {row['directional']:,} | "
            f"{row['timely_directional']:,} | {row['actual']:,} | "
            f"{row['prospective_actual']:,} | "
            f"{row['consensus']:,} | {row['causal_consensus']:,} | {row['blocker']} |"
        )
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- Listing breadth is not prediction coverage. A row only becomes a causal directional input when the underlying document/value was available, mapped, and timely.",
            "- Neutral detail is retained; a central-bank document must not be forced bullish or bearish merely because it was published.",
            "- Missing pre-release consensus remains an external acquisition blocker and is never backfilled from post-release pages.",
            "- Rate-setting and intervention authorities are prioritized ahead of more media searches.",
            "",
            "Execution decision remains `no_trade`.",
            "",
        ]
    )
    return "\n".join(lines)


def run(
    database: Path = DEFAULT_DATABASE,
    config_path: Path = DEFAULT_CONFIG,
    output_json: Path = DEFAULT_JSON,
    output_md: Path = DEFAULT_MD,
) -> dict[str, Any]:
    config = news.load_json(config_path, {})
    source_config = {
        str(row.get("source_id") or ""): row
        for row in (config.get("sources") or [])
        if isinstance(row, Mapping)
    }
    connection = open_readonly(database)
    try:
        has_causal_view = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='view' AND name='source_events_causal_v1'"
        ).fetchone() is not None
        source_event_relation = (
            "source_events_causal_v1" if has_causal_view else "source_events"
        )
        placeholders = ",".join("?" for _ in OFFICIAL_POPULATIONS)
        rows = connection.execute(
            f"""
            SELECT source_event_id, provider_event_id, source_id,
                   source_population, source_family, event_version, payload_json
            FROM {source_event_relation}
            WHERE source_population IN ({placeholders})
            """,
            tuple(sorted(OFFICIAL_POPULATIONS)),
        ).fetchall()
    finally:
        connection.close()

    by_source: dict[str, Counter[str]] = defaultdict(Counter)
    populations: dict[str, str] = {}
    families: dict[str, str] = {}
    latest: dict[tuple[str, str], tuple[int, str, str]] = {}
    for source_event_id, provider_event_id, source_id, population, family, event_version, payload_json in rows:
        source_id = str(source_id)
        populations[source_id] = str(population)
        families[source_id] = str(family)
        by_source[source_id]["rows"] += 1
        key = (source_id, str(provider_event_id))
        candidate = (int(event_version or 0), str(source_event_id), str(payload_json))
        if key not in latest or candidate[:2] > latest[key][:2]:
            latest[key] = candidate

    for (source_id, _provider_event_id), (_version, _event_id, payload_json) in latest.items():
        metrics = event_metrics(raw_payload(payload_json))
        by_source[source_id]["unique_documents"] += 1
        for name, present in metrics.items():
            by_source[source_id][name] += int(present)

    source_rows: list[dict[str, Any]] = []
    for source_id, counts in by_source.items():
        configured = source_config.get(source_id) or {}
        priority = RATE_SETTING_PRIORITY.get(source_id, 3)
        if populations[source_id] == "official_rates_curve" and counts[
            "prospective_eligible"
        ] < 2:
            blocker = "only_one_prospective_daily_curve_no_intraday_repricing"
        elif not counts["detail"]:
            blocker = "listing_without_usable_detail"
        elif not counts["directional"]:
            blocker = "detail_without_directional_mapping"
        elif not counts["timely_directional"]:
            blocker = "direction_available_too_late"
        elif not counts["causal_consensus"] and populations[source_id] == "official_macro_publisher":
            blocker = "missing_pre_release_consensus"
        else:
            blocker = "collect_independent_outcomes"
        source_rows.append(
            {
                "priority": priority,
                "source_id": source_id,
                "source_population": populations[source_id],
                "source_family": families[source_id],
                "configured_currencies": list(configured.get("currencies") or []),
                "detail_enrichment": str(configured.get("detail_enrichment") or ""),
                "rows": counts["rows"],
                "unique_documents": counts["unique_documents"],
                "prospective_eligible": counts["prospective_eligible"],
                "detail": counts["detail"],
                "directional": counts["directional"],
                "timely_directional": counts["timely_directional"],
                "actual": counts["actual"],
                "prospective_actual": counts["prospective_actual"],
                "consensus": counts["consensus"],
                "causal_consensus": counts["causal_consensus"],
                "structured": counts["structured"],
                "official_policy_release": counts["official_policy_release"],
                "published_time_inferred": counts["published_time_inferred"],
                "blocker": blocker,
            }
        )
    source_rows.sort(key=lambda row: (row["priority"], -row["rows"], row["source_id"]))
    totals = sum(by_source.values(), Counter())
    for key in (
        "rows",
        "unique_documents",
        "detail",
        "directional",
        "timely_directional",
        "actual",
        "prospective_actual",
        "consensus",
        "causal_consensus",
        "structured",
        "official_policy_release",
        "published_time_inferred",
        "prospective_eligible",
    ):
        totals.setdefault(key, 0)
    payload = {
        "schema_version": 1,
        "generated_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "research_only": True,
        "execution_eligible": False,
        "execution_decision": "no_trade",
        "database": str(database),
        "totals": {key: int(value) for key, value in totals.items()},
        "sources": source_rows,
    }
    news.atomic_write_json(output_json, payload)
    news.atomic_write_text(output_md, render_markdown(payload))
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--output-json", type=Path, default=DEFAULT_JSON)
    parser.add_argument("--output-md", type=Path, default=DEFAULT_MD)
    args = parser.parse_args()
    payload = run(args.database, args.config, args.output_json, args.output_md)
    print(json.dumps({"generated_utc": payload["generated_utc"], "totals": payload["totals"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
