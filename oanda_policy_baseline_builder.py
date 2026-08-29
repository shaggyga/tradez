#!/usr/bin/env python3
"""Freeze latest compatible first-party policy documents as prior context.

The output is a baseline corpus, never outcome evidence.  It preserves the
actual local availability time and payload hash, and groups documents by
publisher currency and document class so unlike artifacts are never compared.
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
from typing import Any, Mapping

from oanda_policy_statement_breakout_research import (
    configured_currency_contracts,
    iso,
    parse_utc,
    policy_document_class,
    source_native_policy_currencies,
)


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
NEWS_DB = DATA / "local_news_sentiment" / "local_news_sentiment_v1.sqlite"
SOURCES = ROOT / "config" / "news_sources_v1.json"
OUTPUT = ROOT / "config" / "official_policy_statement_baselines_v2_20260816.json"
REPORT = (
    DATA
    / "reports"
    / "currency_event_technical_coverage"
    / "POLICY_BASELINE_COVERAGE_CURRENT.md"
)
UTC = dt.timezone.utc
CONTRACT_ID = "official_policy_statement_baselines_v2_20260816"


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False),
        encoding="utf-8",
    )
    os.replace(temporary, path)


def load_candidates(
    database: Path, sources_path: Path, as_of: dt.datetime
) -> list[dict[str, Any]]:
    if not database.exists():
        return []
    contracts = configured_currency_contracts(sources_path)
    connection = sqlite3.connect(
        f"file:{database.as_posix()}?mode=ro", uri=True, timeout=10
    )
    rows = connection.execute(
        "SELECT event_id,payload_json,first_seen_utc FROM articles "
        "ORDER BY published_utc,event_id"
    ).fetchall()
    connection.close()
    candidates: list[dict[str, Any]] = []
    for event_id, payload_json, first_seen in rows:
        try:
            payload = json.loads(payload_json or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        summary = re.sub(r"\s+", " ", str(payload.get("summary") or "")).strip()
        if (
            not bool(payload.get("source_verified"))
            or not bool(payload.get("source_direct"))
            or not bool(payload.get("official_policy_release"))
            or bool(payload.get("structured_event"))
            or len(summary) < 250
        ):
            continue
        known = parse_utc(payload.get("causal_known_utc") or first_seen)
        published = parse_utc(payload.get("published_utc"))
        if known is None or published is None or known > as_of:
            continue
        source_id = str(payload.get("source_id") or "")
        currencies = source_native_policy_currencies(
            source_id,
            payload.get("direct_currencies") or payload.get("currencies") or [],
            contracts,
        )
        for currency in currencies:
            canonical = {
                "event_id": str(event_id),
                "source_id": source_id,
                "currency": currency,
                "document_class": policy_document_class(
                    payload.get("headline"), payload.get("source_url")
                ),
                "headline": str(payload.get("headline") or ""),
                "known_utc": iso(known),
                "published_utc": iso(published),
                "source_url": str(payload.get("source_url") or ""),
                "summary": summary[:4_000],
                "source_listing_bootstrap": bool(
                    payload.get("source_listing_bootstrap")
                ),
                "detail_available_utc": str(
                    payload.get("detail_available_utc") or ""
                ),
            }
            canonical["raw_payload_sha256"] = hashlib.sha256(
                str(payload_json).encode("utf-8")
            ).hexdigest()
            candidates.append(canonical)
    return candidates


def freeze_latest(candidates: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    latest: dict[tuple[str, str, str], dict[str, Any]] = {}
    for raw in candidates:
        row = dict(raw)
        key = (
            str(row.get("source_id") or ""),
            str(row.get("currency") or ""),
            str(row.get("document_class") or ""),
        )
        prior = latest.get(key)
        rank = (
            str(row.get("known_utc") or ""),
            str(row.get("published_utc") or ""),
            str(row.get("source_url") or ""),
        )
        prior_rank = (
            str((prior or {}).get("known_utc") or ""),
            str((prior or {}).get("published_utc") or ""),
            str((prior or {}).get("source_url") or ""),
        )
        if prior is None or rank > prior_rank:
            latest[key] = row
    return [
        {
            **row,
            "origin": "retained_first_party_policy_context_snapshot",
            "use_policy": "prior_context_only_not_outcome_evidence",
            "research_only": True,
            "execution_eligible": False,
        }
        for _, row in sorted(latest.items())
    ]


def build(
    database: Path = NEWS_DB,
    sources_path: Path = SOURCES,
    output: Path = OUTPUT,
    report: Path = REPORT,
    as_of: dt.datetime | None = None,
) -> dict[str, Any]:
    as_of = (as_of or dt.datetime.now(UTC)).astimezone(UTC)
    baselines = freeze_latest(load_candidates(database, sources_path, as_of))
    payload = {
        "schema_version": 2,
        "contract_id": CONTRACT_ID,
        "created_utc": iso(as_of),
        "research_only": True,
        "can_execute": False,
        "can_authorize": False,
        "material_change_requires_new_contract": True,
        "baselines": baselines,
    }
    _atomic_json(output, payload)
    currencies = sorted({row["currency"] for row in baselines})
    lines = [
        "# Policy baseline coverage",
        "",
        f"Generated: `{iso(as_of)}`",
        "",
        f"- Frozen compatible baselines: **{len(baselines)}**",
        f"- Currencies: **{len(currencies)}** ({', '.join(currencies)})",
        "- Use: prior context only; never outcome evidence or execution authorization.",
        "",
        "| Currency | Source | Class | Headline | Known locally |",
        "|---|---|---|---|---|",
    ]
    for row in baselines:
        lines.append(
            f"| {row['currency']} | {row['source_id']} | {row['document_class']} | "
            f"{row['headline']} | {row['known_utc']} |"
        )
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=NEWS_DB)
    parser.add_argument("--sources", type=Path, default=SOURCES)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    parser.add_argument("--report", type=Path, default=REPORT)
    args = parser.parse_args()
    payload = build(args.database, args.sources, args.output, args.report)
    print(
        json.dumps(
            {
                "contract_id": payload["contract_id"],
                "baselines": len(payload["baselines"]),
                "currencies": sorted(
                    {row["currency"] for row in payload["baselines"]}
                ),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
