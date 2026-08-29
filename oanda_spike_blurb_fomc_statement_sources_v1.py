#!/usr/bin/env python3
"""Freeze exact official FOMC statements for source-proximate research.

This collector has no price, forecast, account, allocator, authorization, or
execution dependency.  It retrieves the Federal Reserve's December 2023
baseline statement and the 21 regular statements in the already-frozen FOMC
generalization population.  Raw HTML and a deterministic statement-body
projection are append-only in the canonical spike/blurb database.

The pages are retrieved retrospectively.  Their official release clocks are
valid event clocks, but the retrieval itself is not prospective evidence and
cannot prove contemporaneous market expectations or rate repricing.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import html
import json
import re
import sqlite3
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Iterable, Mapping

import oanda_spike_blurb_fomc_response_generalization_v1 as fomc
from oanda_spike_blurb_rbnz_schedule_cohort_v1 import (
    DATABASE,
    canonical_json,
    immutable_insert,
    sha256_bytes,
    sha256_file,
    stable_id,
    utc_now,
)


CONTRACT_ID = "spike_blurb_fomc_statement_sources_v1_20260820"
SCHEMA_VERSION = 1
FREEZE_UTC = "2026-08-20T18:30:00+00:00"
BASELINE_DATE = "2023-12-13"
BASELINE_RELEASE_UTC = "2023-12-13T19:00:00+00:00"
REQUEST_TIMEOUT_SEC = 30.0
USER_AGENT = "ForexSourceResearch/1.0 (+local research; official source archive)"
REPORT_ROOT = Path(__file__).resolve().parent / (
    "data/oanda_training_manager/reports/spike_blurb_factor_reconstruction/"
    "fomc_statement_sources_v1"
)


class VisibleTextParser(HTMLParser):
    """Minimal deterministic visible-text projection for official HTML."""

    HIDDEN = {"script", "style", "noscript", "svg"}

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.hidden_depth = 0
        self.parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        del attrs
        if tag.lower() in self.HIDDEN:
            self.hidden_depth += 1

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() in self.HIDDEN and self.hidden_depth:
            self.hidden_depth -= 1

    def handle_data(self, data: str) -> None:
        if not self.hidden_depth and data.strip():
            self.parts.append(data)


def normalized_visible_text(raw_html: bytes) -> str:
    decoded = raw_html.decode("utf-8-sig", errors="replace")
    parser = VisibleTextParser()
    parser.feed(decoded)
    parser.close()
    return re.sub(r"\s+", " ", html.unescape(" ".join(parser.parts))).strip()


def extract_statement_body(raw_html: bytes) -> tuple[str, str]:
    """Return release-clock text and statement body, excluding page chrome."""

    visible = normalized_visible_text(raw_html)
    release_match = re.search(
        r"For release at 2:00 p\.m\.\s+(?:EST|EDT)", visible, flags=re.I
    )
    if release_match is None:
        raise ValueError("official_release_clock_marker_missing")
    end_match = re.search(
        r"For media inquiries[, ]", visible[release_match.end() :], flags=re.I
    )
    if end_match is None:
        raise ValueError("official_statement_end_marker_missing")
    body_start = release_match.end()
    body_end = body_start + end_match.start()
    statement = re.sub(r"\s+", " ", visible[body_start:body_end]).strip()
    if len(statement) < 300:
        raise ValueError("official_statement_body_too_short")
    required = ("Committee", "federal funds rate", "Voting")
    missing = [token for token in required if token.lower() not in statement.lower()]
    if missing:
        raise ValueError("official_statement_required_text_missing:" + ",".join(missing))
    return release_match.group(0), statement


def source_specs() -> list[dict[str, Any]]:
    rows = [
        {
            "event_date": BASELINE_DATE,
            "event_id": "fomc_policy_20231213_statement_baseline_v1",
            "event_clock_utc": BASELINE_RELEASE_UTC,
            "source_url": fomc.statement_url(BASELINE_DATE),
            "role": "prior_statement_baseline",
        }
    ]
    for event in fomc.validate_schedule():
        rows.append(
            {
                "event_date": str(event["event_date"]),
                "event_id": str(event["event_id"]),
                "event_clock_utc": str(event["event_clock_utc"]),
                "source_url": str(event["release_url"]),
                "role": "analysis_event",
            }
        )
    if len(rows) != 22 or len({row["event_date"] for row in rows}) != 22:
        raise RuntimeError("exact_baseline_plus_twenty_one_unique_events_required")
    if any(
        not str(row["source_url"]).startswith(
            "https://www.federalreserve.gov/newsevents/pressreleases/monetary"
        )
        for row in rows
    ):
        raise RuntimeError("non_federal_reserve_source_url")
    return rows


def ensure_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS fomc_statement_source_contracts (
          contract_id TEXT PRIMARY KEY, contract_json TEXT NOT NULL,
          builder_sha256 TEXT NOT NULL, source_population_sha256 TEXT NOT NULL,
          frozen_utc TEXT NOT NULL, research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          forecast_proof_eligible INTEGER NOT NULL CHECK(forecast_proof_eligible=0)
        );
        CREATE TABLE IF NOT EXISTS fomc_statement_source_documents (
          document_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL, event_id TEXT NOT NULL,
          event_date TEXT NOT NULL, event_clock_utc TEXT NOT NULL, source_url TEXT NOT NULL,
          document_role TEXT NOT NULL, retrieved_utc TEXT NOT NULL, http_status INTEGER NOT NULL,
          latency_ms INTEGER NOT NULL, response_headers_json TEXT NOT NULL,
          coverage_state TEXT NOT NULL, error_text TEXT NOT NULL,
          raw_payload_sha256 TEXT NOT NULL, raw_payload_gzip BLOB NOT NULL,
          release_clock_text TEXT NOT NULL, statement_text TEXT NOT NULL,
          statement_text_sha256 TEXT NOT NULL, document_json TEXT NOT NULL,
          row_sha256 TEXT NOT NULL, source_known_at_event_clock INTEGER NOT NULL,
          retrospective_retrieval INTEGER NOT NULL,
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          forecast_proof_eligible INTEGER NOT NULL CHECK(forecast_proof_eligible=0),
          UNIQUE(contract_id,event_date),
          FOREIGN KEY(contract_id) REFERENCES fomc_statement_source_contracts(contract_id)
        );
        CREATE TRIGGER IF NOT EXISTS fomc_statement_source_contract_no_update
          BEFORE UPDATE ON fomc_statement_source_contracts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_statement_source_contract_no_delete
          BEFORE DELETE ON fomc_statement_source_contracts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_statement_source_document_no_update
          BEFORE UPDATE ON fomc_statement_source_documents BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_statement_source_document_no_delete
          BEFORE DELETE ON fomc_statement_source_documents BEGIN SELECT RAISE(ABORT,'immutable'); END;
        """
    )


def freeze_contract(connection: sqlite3.Connection, specs: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    rows = [dict(row) for row in specs]
    population_sha = sha256_bytes(canonical_json(rows).encode("utf-8"))
    contract = {
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "builder_sha256": sha256_file(Path(__file__).resolve()),
        "source_population_sha256": population_sha,
        "source_count": len(rows),
        "source_authority": "Board of Governors of the Federal Reserve System",
        "source_domain": "www.federalreserve.gov",
        "retrieval_semantics": "retrospective_exact_official_page_archive",
        "event_clock_semantics": "official_statement_for_release_at_1400_eastern",
        "causal_limitations": {
            "causal_pre_release_consensus": "unavailable",
            "event_time_policy_path_repricing": "unavailable",
            "retrieval_was_prospective": False,
            "source_text_may_support_retrospective_explanation": True,
            "source_text_alone_may_assign_execution_direction": False,
        },
        "research_only": True,
        "execution_eligible": False,
        "forecast_proof_eligible": False,
        "supported_execution_decision": "no_trade",
        "frozen_utc": FREEZE_UTC,
    }
    existing = connection.execute(
        "SELECT contract_json,builder_sha256,source_population_sha256 "
        "FROM fomc_statement_source_contracts WHERE contract_id=?",
        (CONTRACT_ID,),
    ).fetchone()
    if existing is not None:
        if (
            str(existing[0]) != canonical_json(contract)
            or str(existing[1]) != contract["builder_sha256"]
            or str(existing[2]) != population_sha
        ):
            raise RuntimeError("frozen_source_contract_mismatch")
        return contract
    connection.execute(
        "INSERT INTO fomc_statement_source_contracts VALUES(?,?,?,?,?,?,?,?)",
        (
            CONTRACT_ID,
            canonical_json(contract),
            contract["builder_sha256"],
            population_sha,
            FREEZE_UTC,
            1,
            0,
            0,
        ),
    )
    return contract


def fetch_source(spec: Mapping[str, Any]) -> dict[str, Any]:
    request = urllib.request.Request(
        str(spec["source_url"]),
        headers={"User-Agent": USER_AGENT, "Accept": "text/html"},
    )
    started = time.perf_counter()
    retrieved = utc_now()
    status = 0
    headers: dict[str, str] = {}
    raw = b""
    error = ""
    try:
        with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SEC) as response:
            status = int(response.status)
            raw = response.read()
            headers = {
                str(key).lower(): str(value)
                for key, value in response.headers.items()
                if str(key).lower() in {"content-type", "last-modified", "etag", "date"}
            }
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, OSError) as exc:
        error = f"{type(exc).__name__}:{exc}"[:1000]
    latency_ms = int(round((time.perf_counter() - started) * 1000.0))
    release_clock = ""
    statement = ""
    coverage = "transport_error"
    if status == 200 and raw:
        try:
            release_clock, statement = extract_statement_body(raw)
            coverage = "exact_official_statement"
        except ValueError as exc:
            coverage = "content_invalid"
            error = str(exc)
    return {
        **dict(spec),
        "retrieved_utc": retrieved,
        "http_status": status,
        "latency_ms": latency_ms,
        "response_headers": headers,
        "coverage_state": coverage,
        "error_text": error,
        "raw_payload": raw,
        "raw_payload_sha256": sha256_bytes(raw),
        "release_clock_text": release_clock,
        "statement_text": statement,
        "statement_text_sha256": sha256_bytes(statement.encode("utf-8")),
    }


DOCUMENT_COLUMNS = (
    "document_id",
    "contract_id",
    "event_id",
    "event_date",
    "event_clock_utc",
    "source_url",
    "document_role",
    "retrieved_utc",
    "http_status",
    "latency_ms",
    "response_headers_json",
    "coverage_state",
    "error_text",
    "raw_payload_sha256",
    "raw_payload_gzip",
    "release_clock_text",
    "statement_text",
    "statement_text_sha256",
    "document_json",
    "row_sha256",
    "source_known_at_event_clock",
    "retrospective_retrieval",
    "research_only",
    "execution_eligible",
    "forecast_proof_eligible",
)


def store_document(connection: sqlite3.Connection, row: Mapping[str, Any]) -> None:
    raw = bytes(row["raw_payload"])
    material = {
        key: value
        for key, value in row.items()
        if key not in {"raw_payload", "response_headers"}
    }
    material["response_headers"] = dict(row["response_headers"])
    material.update(
        {
            "contract_id": CONTRACT_ID,
            "source_known_at_event_clock": 1,
            "retrospective_retrieval": 1,
            "research_only": 1,
            "execution_eligible": 0,
            "forecast_proof_eligible": 0,
        }
    )
    document_id = stable_id(
        "fomc_statement_source", CONTRACT_ID, row["event_date"], row["raw_payload_sha256"]
    )
    document_json = canonical_json(material)
    values = (
        document_id, CONTRACT_ID, row["event_id"], row["event_date"],
        row["event_clock_utc"], row["source_url"], row["role"], row["retrieved_utc"],
        int(row["http_status"]), int(row["latency_ms"]),
        canonical_json(row["response_headers"]), row["coverage_state"], row["error_text"],
        row["raw_payload_sha256"], gzip.compress(raw, compresslevel=9, mtime=0),
        row["release_clock_text"], row["statement_text"], row["statement_text_sha256"],
        document_json, sha256_bytes(document_json.encode("utf-8")), 1, 1, 1, 0, 0,
    )
    stored = dict(zip(DOCUMENT_COLUMNS, values, strict=True))
    immutable_insert(
        connection,
        "fomc_statement_source_documents",
        stored,
        "document_id",
    )


def snapshot(connection: sqlite3.Connection, contract: Mapping[str, Any]) -> dict[str, Any]:
    rows = list(
        connection.execute(
            "SELECT event_date,coverage_state,http_status,raw_payload_sha256,"
            "statement_text_sha256,LENGTH(raw_payload_gzip),LENGTH(statement_text) "
            "FROM fomc_statement_source_documents WHERE contract_id=? ORDER BY event_date",
            (CONTRACT_ID,),
        )
    )
    exact = sum(str(row[1]) == "exact_official_statement" for row in rows)
    result = {
        "schema_version": SCHEMA_VERSION,
        "generated_utc": utc_now(),
        "contract_id": CONTRACT_ID,
        "builder_sha256": contract["builder_sha256"],
        "source_population_sha256": contract["source_population_sha256"],
        "expected_document_count": 22,
        "stored_document_count": len(rows),
        "exact_document_count": exact,
        "documents": [
            {
                "event_date": str(row[0]),
                "coverage_state": str(row[1]),
                "http_status": int(row[2]),
                "raw_payload_sha256": str(row[3]),
                "statement_text_sha256": str(row[4]),
                "compressed_bytes": int(row[5]),
                "statement_characters": int(row[6]),
            }
            for row in rows
        ],
        "sqlite_integrity": str(connection.execute("PRAGMA integrity_check").fetchone()[0]),
        "research_only": True,
        "execution_eligible": False,
        "forecast_proof_eligible": False,
        "supported_execution_decision": "no_trade",
    }
    result["snapshot_sha256"] = sha256_bytes(canonical_json(result).encode("utf-8"))
    return result


def write_report(result: Mapping[str, Any]) -> None:
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)
    (REPORT_ROOT / "FOMC_STATEMENT_SOURCES_V1.json").write_text(
        json.dumps(result, indent=2, sort_keys=True), encoding="utf-8"
    )
    lines = [
        "# FOMC official statement source archive V1",
        "",
        f"Generated: `{result['generated_utc']}`",
        "",
        f"- Exact official documents: **{result['exact_document_count']} / {result['expected_document_count']}**",
        f"- SQLite integrity: **{result['sqlite_integrity']}**",
        "- Retrieval: retrospective official-page archive; event clocks are official release clocks.",
        "- Missing by design: causal pre-release consensus and event-time policy-path repricing.",
        "- Operational effect: none; research-only, execution-ineligible, and no-trade.",
        "",
        "| Date | Coverage | HTTP | Statement characters |",
        "|---|---|---:|---:|",
    ]
    lines.extend(
        f"| {row['event_date']} | {row['coverage_state']} | {row['http_status']} | {row['statement_characters']} |"
        for row in result["documents"]
    )
    (REPORT_ROOT / "FOMC_STATEMENT_SOURCES_V1.md").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )


def build(*, database_path: Path = DATABASE) -> dict[str, Any]:
    specs = source_specs()
    connection = sqlite3.connect(database_path, timeout=120)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute("PRAGMA foreign_keys=ON")
    ensure_schema(connection)
    contract = freeze_contract(connection, specs)
    connection.commit()
    existing = {
        str(row[0])
        for row in connection.execute(
            "SELECT event_date FROM fomc_statement_source_documents WHERE contract_id=?",
            (CONTRACT_ID,),
        )
    }
    for spec in specs:
        if str(spec["event_date"]) in existing:
            continue
        row = fetch_source(spec)
        if row["coverage_state"] != "exact_official_statement":
            connection.close()
            raise RuntimeError(
                f"official_statement_source_unavailable:{spec['event_date']}:"
                f"{row['coverage_state']}:{row['error_text']}"
            )
        store_document(connection, row)
        connection.commit()
    result = snapshot(connection, contract)
    connection.close()
    if result["stored_document_count"] != 22 or result["exact_document_count"] != 22:
        raise RuntimeError("exact_twenty_two_document_archive_required")
    write_report(result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=DATABASE)
    args = parser.parse_args()
    result = build(database_path=args.database)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "CONTRACT_ID",
    "build",
    "ensure_schema",
    "extract_statement_body",
    "normalized_visible_text",
    "source_specs",
]
