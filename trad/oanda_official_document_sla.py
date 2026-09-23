#!/usr/bin/env python3
"""Audit first-party policy listing and document-detail latency.

The audit is diagnostic only.  It deduplicates article versions by canonical
document URL and separately measures discovery/listing latency and usable
document-text latency so a timely title cannot be confused with timely text.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sqlite3
import time
import urllib.parse
from pathlib import Path
from typing import Any


UTC = dt.timezone.utc
LISTING_SLA_SEC = 120.0
DETAIL_SLA_SEC = 300.0
PROSPECTIVE_SLA_START_UTC = "2026-08-10T14:17:04.447501+00:00"


def parse_epoch(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return dt.datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return None


def iso_utc(value: dt.datetime | None = None) -> str:
    return (value or dt.datetime.now(UTC)).astimezone(UTC).isoformat()


def canonical_url(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    parts = urllib.parse.urlsplit(raw)
    return urllib.parse.urlunsplit(
        (parts.scheme.lower(), parts.netloc.lower(), parts.path, parts.query, "")
    )


def load_official_policy_rows(database: Path) -> list[dict[str, Any]]:
    connection = sqlite3.connect(
        f"file:{database.resolve().as_posix()}?mode=ro", uri=True, timeout=30.0
    )
    connection.execute("PRAGMA query_only=ON")
    try:
        rows = connection.execute(
            """
            SELECT event_id,source_id,published_utc,first_seen_utc,payload_json
            FROM articles
            WHERE source_verified=1
            ORDER BY first_seen_utc,event_id
            """
        ).fetchall()
    finally:
        connection.close()
    output = []
    for event_id, source_id, published, first_seen, raw in rows:
        try:
            payload = json.loads(str(raw))
        except (TypeError, json.JSONDecodeError):
            continue
        if not bool(payload.get("official_policy_release")):
            continue
        output.append(
            {
                "event_id": str(event_id), "source_id": str(source_id),
                "published_utc": str(published or payload.get("published_utc") or ""),
                "first_seen_utc": str(first_seen or payload.get("first_seen_utc") or ""),
                **payload,
            }
        )
    return output


def build_document_sla(
    rows: list[dict[str, Any]],
    *,
    listing_sla_sec: float = LISTING_SLA_SEC,
    detail_sla_sec: float = DETAIL_SLA_SEC,
    prospective_start_utc: str = PROSPECTIVE_SLA_START_UTC,
    active_source_ids: set[str] | None = None,
    as_of_utc: str | None = None,
) -> dict[str, Any]:
    prospective_start = parse_epoch(prospective_start_utc)
    as_of = parse_epoch(as_of_utc) if as_of_utc else dt.datetime.now(UTC).timestamp()
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        url = canonical_url(
            row.get("detail_source_url") or row.get("source_url") or row.get("external_id")
        )
        key = url or str(row.get("event_lineage_id") or row.get("event_id") or "")
        if key:
            grouped.setdefault(key, []).append(row)
    documents = []
    for key, versions in grouped.items():
        published_values = [parse_epoch(row.get("published_utc")) for row in versions]
        published_values = [value for value in published_values if value is not None]
        first_seen_values = [parse_epoch(row.get("first_seen_utc")) for row in versions]
        first_seen_values = [value for value in first_seen_values if value is not None]
        detail_values = [parse_epoch(row.get("detail_available_utc")) for row in versions]
        detail_values = [value for value in detail_values if value is not None]
        scheduled_values = [parse_epoch(row.get("scheduled_utc")) for row in versions]
        scheduled_values = [value for value in scheduled_values if value is not None]
        published = min(published_values) if published_values else None
        first_seen = min(first_seen_values) if first_seen_values else None
        detail = min(detail_values) if detail_values else None
        scheduled = min(scheduled_values) if scheduled_values else None
        listing_latency = (
            max(0.0, first_seen - published)
            if published is not None and first_seen is not None else None
        )
        detail_latency = (
            max(0.0, detail - published)
            if published is not None and detail is not None else None
        )
        representative = max(
            versions,
            key=lambda row: (
                int(bool(row.get("detail_enriched"))),
                len(str(row.get("summary") or "")),
                str(row.get("first_seen_utc") or ""),
            ),
        )
        archive = str(representative.get("detail_archive_path") or "")
        representative_source_id = str(representative.get("source_id") or "")
        source_active = bool(
            active_source_ids is None
            or representative_source_id in active_source_ids
        )
        # A pre-registered decision clock is not yet a published policy
        # document.  Treating its collector-created placeholder as a document
        # made every future central-bank meeting look like a missing-PDF SLA
        # incident.  It becomes document-SLA eligible only after its scheduled
        # release boundary (or when usable detail is actually observed).
        future_schedule_placeholder = bool(
            scheduled is not None and as_of is not None and scheduled > as_of
            and detail is None
        )
        document = {
            "document_key": key,
            "source_id": representative_source_id,
            "source_active": source_active,
            "headline": str(representative.get("headline") or ""),
            "policy_document_type": str(representative.get("policy_document_type") or ""),
            "source_url": canonical_url(
                representative.get("detail_source_url")
                or representative.get("source_url")
                or representative.get("external_id")
            ),
            "published_utc": None if published is None else iso_utc(dt.datetime.fromtimestamp(published, UTC)),
            "first_listed_utc": None if first_seen is None else iso_utc(dt.datetime.fromtimestamp(first_seen, UTC)),
            "detail_available_utc": None if detail is None else iso_utc(dt.datetime.fromtimestamp(detail, UTC)),
            "scheduled_utc": None if scheduled is None else iso_utc(dt.datetime.fromtimestamp(scheduled, UTC)),
            "future_schedule_placeholder": future_schedule_placeholder,
            "listing_latency_sec": listing_latency,
            "detail_latency_sec": detail_latency,
            "listing_sla_met": listing_latency is not None and listing_latency <= listing_sla_sec,
            "detail_sla_met": detail_latency is not None and detail_latency <= detail_sla_sec,
            "detail_enriched": detail is not None,
            "detail_archive_path": archive,
            "archive_present": bool(archive and Path(archive).is_file()),
            "version_rows": len(versions),
            "prospective_sla_eligible": bool(
                published is not None
                and prospective_start is not None
                and published >= prospective_start
                and source_active
                and not future_schedule_placeholder
                and not any(bool(row.get("source_listing_bootstrap")) for row in versions)
            ),
        }
        documents.append(document)
    documents.sort(key=lambda row: (str(row["published_utc"] or ""), row["document_key"]))
    known_listing = [row for row in documents if row["listing_latency_sec"] is not None]
    known_detail = [row for row in documents if row["detail_latency_sec"] is not None]
    alarms = []
    for row in documents:
        if not row["prospective_sla_eligible"]:
            continue
        if row["listing_latency_sec"] is not None and not row["listing_sla_met"]:
            alarms.append({"type": "listing_sla_miss", **row})
        if not row["detail_sla_met"]:
            alarms.append(
                {
                    "type": "detail_missing" if row["detail_latency_sec"] is None else "detail_sla_miss",
                    **row,
                }
            )
    return {
        "schema_version": 1,
        "generated_utc": iso_utc(),
        "status": "ok",
        "sla": {"listing_sec": listing_sla_sec, "usable_detail_sec": detail_sla_sec},
        "prospective_sla_start_utc": prospective_start_utc,
        "counts": {
            "documents": len(documents),
            "prospective_documents": sum(
                int(row["prospective_sla_eligible"]) for row in documents
            ),
            "historical_diagnostic_documents": sum(
                int(not row["prospective_sla_eligible"]) for row in documents
            ),
            "future_schedule_placeholders": sum(
                int(row["future_schedule_placeholder"]) for row in documents
            ),
            "known_listing_latency": len(known_listing),
            "listing_sla_met": sum(
                int(row["listing_sla_met"] and row["prospective_sla_eligible"])
                for row in documents
            ),
            "detail_available": len(known_detail),
            "detail_sla_met": sum(
                int(row["detail_sla_met"] and row["prospective_sla_eligible"])
                for row in documents
            ),
            "archived_documents": sum(int(row["archive_present"]) for row in documents),
            "alarms": len(alarms),
        },
        "documents": documents[-250:],
        "alarms": alarms[-250:],
        "contract": {
            "deduplication": "canonical_document_url",
            "listing_time": "earliest_local_first_seen_across_versions",
            "detail_time": "earliest_usable_document_text_across_versions",
            "listing_and_detail_are_separate": True,
        },
        "research_only": True,
        "can_place_orders": False,
        "real_money_routing": False,
    }


def render_report(payload: dict[str, Any]) -> str:
    counts = payload["counts"]
    lines = [
        "# Official Policy Document SLA", "",
        f"Generated: `{payload['generated_utc']}`", "",
        f"- Documents: **{counts['documents']}**",
        f"- Prospective SLA documents: **{counts['prospective_documents']}**",
        f"- Future schedule placeholders (not documents yet): **{counts['future_schedule_placeholders']}**",
        f"- Historical diagnostics (excluded from prospective SLA): **{counts['historical_diagnostic_documents']}**",
        f"- Listing SLA met: **{counts['listing_sla_met']} / {counts['prospective_documents']}**",
        f"- Usable-detail SLA met: **{counts['detail_sla_met']} / {counts['prospective_documents']}**",
        f"- Archived documents: **{counts['archived_documents']}**",
        f"- Current SLA alarms: **{counts['alarms']}**", "",
        "| Published | Source | Document | Evidence | Listing lag | Detail lag | PDF/archive |",
        "|---|---|---|---|---:|---:|---|",
    ]
    for row in payload["documents"][-100:]:
        listing = "unknown" if row["listing_latency_sec"] is None else f"{row['listing_latency_sec']:.1f}s"
        detail = "missing" if row["detail_latency_sec"] is None else f"{row['detail_latency_sec']:.1f}s"
        archive = "yes" if row["archive_present"] else "no"
        evidence = (
            "future schedule"
            if row.get("future_schedule_placeholder")
            else "prospective"
            if row["prospective_sla_eligible"]
            else "historical diagnostic"
        )
        headline = str(row["headline"]).replace("|", "/")[:100]
        lines.append(
            f"| {row['published_utc'] or 'unknown'} | {row['source_id']} | {headline} | {evidence} | {listing} | {detail} | {archive} |"
        )
    lines += ["", "This report is diagnostic only and cannot authorize execution.", ""]
    return "\n".join(lines)


def atomic_write(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    try:
        temporary.write_text(value, encoding="utf-8")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def active_news_source_ids(config: Path | None) -> set[str] | None:
    if config is None or not config.is_file():
        return None
    try:
        payload = json.loads(config.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return {
        str(source.get("source_id") or "")
        for source in payload.get("sources") or []
        if isinstance(source, dict)
        and source.get("enabled", True)
        and source.get("runtime_supported", True)
        and source.get("source_id")
    }


def run_once(
    database: Path,
    state: Path,
    report: Path,
    config: Path | None = None,
) -> dict[str, Any]:
    payload = build_document_sla(
        load_official_policy_rows(database),
        active_source_ids=active_news_source_ids(config),
    )
    atomic_write(state, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    atomic_write(report, render_report(payload))
    return payload


def main() -> int:
    root = Path(__file__).resolve().parent
    data = root / "data" / "oanda_training_manager"
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", type=Path, default=data / "local_news_sentiment" / "local_news_sentiment_v1.sqlite")
    parser.add_argument("--state", type=Path, default=data / "state" / "official_document_sla_v1.json")
    parser.add_argument("--report", type=Path, default=data / "reports" / "source_governance" / "OFFICIAL_DOCUMENT_SLA_CURRENT.md")
    parser.add_argument(
        "--config",
        type=Path,
        default=root / "config" / "news_sources_v1.json",
    )
    parser.add_argument("--interval-sec", type=float, default=60.0)
    parser.add_argument("--duration-sec", type=float, default=604800.0)
    args = parser.parse_args()
    deadline = time.monotonic() + max(0.0, args.duration_sec)
    while time.monotonic() < deadline:
        run_once(args.database, args.state, args.report, args.config)
        time.sleep(max(5.0, args.interval_sec))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
