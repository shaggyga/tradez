#!/usr/bin/env python3
"""Format-compatible completion of the frozen FOMC source archive V1.

V1 stopped after 20 exact documents when the June 2026 statement replaced the
historical ``Voting ...`` sentence with ``approved ... by a 12-0 vote``.  This
repair freezes the exact V1 parent bytes and rows, fetches only the two missing
official pages, and accepts either official vote formulation.  It changes no
source population, timestamp, price, factor, forecast, or execution policy.
"""

from __future__ import annotations

import argparse
import gzip
import json
import re
import sqlite3
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Iterable, Mapping

import oanda_spike_blurb_fomc_statement_sources_v1 as v1
from oanda_spike_blurb_rbnz_schedule_cohort_v1 import (
    DATABASE,
    canonical_json,
    immutable_insert,
    sha256_bytes,
    sha256_file,
    stable_id,
    utc_now,
)


CONTRACT_ID = "spike_blurb_fomc_statement_sources_v2_20260820"
SCHEMA_VERSION = 1
FREEZE_UTC = "2026-08-20T18:45:00+00:00"
EXPECTED_PARENT_DOCUMENTS = 20
EXPECTED_REPAIR_DATES = ("2026-06-17", "2026-07-29")
REPORT_ROOT = Path(__file__).resolve().parent / (
    "data/oanda_training_manager/reports/spike_blurb_factor_reconstruction/"
    "fomc_statement_sources_v2"
)


def extract_statement_body(raw_html: bytes) -> tuple[str, str]:
    visible = v1.normalized_visible_text(raw_html)
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
    required = ("Committee", "federal funds rate")
    missing = [token for token in required if token.lower() not in statement.lower()]
    if missing:
        raise ValueError("official_statement_required_text_missing:" + ",".join(missing))
    if not re.search(
        r"\bVoting (?:for|against)\b|\bapproved\b.{0,120}\bby\b.{0,40}\bvote\b",
        statement,
        flags=re.I,
    ):
        raise ValueError("official_statement_vote_text_missing")
    return release_match.group(0), statement


def ensure_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS fomc_statement_source_v2_contracts (
          contract_id TEXT PRIMARY KEY, parent_contract_id TEXT NOT NULL,
          contract_json TEXT NOT NULL, builder_sha256 TEXT NOT NULL,
          parent_builder_sha256 TEXT NOT NULL, parent_snapshot_sha256 TEXT NOT NULL,
          source_population_sha256 TEXT NOT NULL, frozen_utc TEXT NOT NULL,
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          forecast_proof_eligible INTEGER NOT NULL CHECK(forecast_proof_eligible=0)
        );
        CREATE TABLE IF NOT EXISTS fomc_statement_source_v2_repair_documents (
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
          FOREIGN KEY(contract_id) REFERENCES fomc_statement_source_v2_contracts(contract_id)
        );
        CREATE TABLE IF NOT EXISTS fomc_statement_source_v2_resolutions (
          resolution_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL, event_date TEXT NOT NULL,
          document_id TEXT NOT NULL, source_generation TEXT NOT NULL,
          resolution_json TEXT NOT NULL, row_sha256 TEXT NOT NULL,
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          forecast_proof_eligible INTEGER NOT NULL CHECK(forecast_proof_eligible=0),
          UNIQUE(contract_id,event_date)
        );
        CREATE TRIGGER IF NOT EXISTS fomc_statement_source_v2_contract_no_update
          BEFORE UPDATE ON fomc_statement_source_v2_contracts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_statement_source_v2_contract_no_delete
          BEFORE DELETE ON fomc_statement_source_v2_contracts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_statement_source_v2_document_no_update
          BEFORE UPDATE ON fomc_statement_source_v2_repair_documents BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_statement_source_v2_document_no_delete
          BEFORE DELETE ON fomc_statement_source_v2_repair_documents BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_statement_source_v2_resolution_no_update
          BEFORE UPDATE ON fomc_statement_source_v2_resolutions BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_statement_source_v2_resolution_no_delete
          BEFORE DELETE ON fomc_statement_source_v2_resolutions BEGIN SELECT RAISE(ABORT,'immutable'); END;
        """
    )


def parent_snapshot(connection: sqlite3.Connection) -> tuple[str, list[dict[str, Any]], list[dict[str, Any]]]:
    contract = connection.execute(
        "SELECT contract_json,builder_sha256,source_population_sha256 "
        "FROM fomc_statement_source_contracts WHERE contract_id=?",
        (v1.CONTRACT_ID,),
    ).fetchone()
    if contract is None:
        raise RuntimeError("frozen_v1_source_contract_missing")
    parent_rows = [
        {
            "document_id": str(row[0]),
            "event_date": str(row[1]),
            "raw_payload_sha256": str(row[2]),
            "statement_text_sha256": str(row[3]),
            "coverage_state": str(row[4]),
        }
        for row in connection.execute(
            "SELECT document_id,event_date,raw_payload_sha256,statement_text_sha256,coverage_state "
            "FROM fomc_statement_source_documents WHERE contract_id=? ORDER BY event_date",
            (v1.CONTRACT_ID,),
        )
    ]
    specs = v1.source_specs()
    parent_dates = {row["event_date"] for row in parent_rows}
    missing = [dict(row) for row in specs if str(row["event_date"]) not in parent_dates]
    if len(parent_rows) != EXPECTED_PARENT_DOCUMENTS:
        raise RuntimeError(f"exact_twenty_parent_documents_required:{len(parent_rows)}")
    if any(row["coverage_state"] != "exact_official_statement" for row in parent_rows):
        raise RuntimeError("nonexact_parent_source_document")
    if tuple(str(row["event_date"]) for row in missing) != EXPECTED_REPAIR_DATES:
        raise RuntimeError("unexpected_v1_missing_source_dates")
    material = {
        "parent_contract_json": str(contract[0]),
        "parent_builder_sha256": str(contract[1]),
        "source_population_sha256": str(contract[2]),
        "parent_documents": parent_rows,
        "missing_specs": missing,
    }
    return sha256_bytes(canonical_json(material).encode("utf-8")), parent_rows, missing


def freeze_contract(
    connection: sqlite3.Connection,
    snapshot_sha: str,
    missing: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    missing_rows = [dict(row) for row in missing]
    parent = connection.execute(
        "SELECT builder_sha256,source_population_sha256 FROM fomc_statement_source_contracts "
        "WHERE contract_id=?",
        (v1.CONTRACT_ID,),
    ).fetchone()
    assert parent is not None
    contract = {
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "parent_contract_id": v1.CONTRACT_ID,
        "builder_sha256": sha256_file(Path(__file__).resolve()),
        "parent_builder_sha256": str(parent[0]),
        "parent_snapshot_sha256": snapshot_sha,
        "source_population_sha256": str(parent[1]),
        "repair_dates": [str(row["event_date"]) for row in missing_rows],
        "repair_scope": "accept_official_vote_or_voting_formulation_only",
        "source_population_changed": False,
        "event_clocks_changed": False,
        "research_only": True,
        "execution_eligible": False,
        "forecast_proof_eligible": False,
        "supported_execution_decision": "no_trade",
        "frozen_utc": FREEZE_UTC,
    }
    existing = connection.execute(
        "SELECT contract_json,builder_sha256,parent_snapshot_sha256 "
        "FROM fomc_statement_source_v2_contracts WHERE contract_id=?",
        (CONTRACT_ID,),
    ).fetchone()
    if existing is not None:
        if tuple(map(str, existing)) != (
            canonical_json(contract), str(contract["builder_sha256"]), snapshot_sha
        ):
            raise RuntimeError("frozen_v2_source_contract_mismatch")
        return contract
    connection.execute(
        "INSERT INTO fomc_statement_source_v2_contracts VALUES(?,?,?,?,?,?,?,?,?,?,?)",
        (
            CONTRACT_ID, v1.CONTRACT_ID, canonical_json(contract),
            contract["builder_sha256"], str(parent[0]), snapshot_sha, str(parent[1]),
            FREEZE_UTC, 1, 0, 0,
        ),
    )
    return contract


def fetch_source(spec: Mapping[str, Any]) -> dict[str, Any]:
    request = urllib.request.Request(
        str(spec["source_url"]),
        headers={"User-Agent": v1.USER_AGENT, "Accept": "text/html"},
    )
    started = time.perf_counter()
    retrieved = utc_now()
    status = 0
    headers: dict[str, str] = {}
    raw = b""
    error = ""
    try:
        with urllib.request.urlopen(request, timeout=v1.REQUEST_TIMEOUT_SEC) as response:
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
            coverage = "exact_official_statement_v2_vote_format"
        except ValueError as exc:
            coverage = "content_invalid"
            error = str(exc)
    return {
        **dict(spec), "retrieved_utc": retrieved, "http_status": status,
        "latency_ms": latency_ms, "response_headers": headers,
        "coverage_state": coverage, "error_text": error, "raw_payload": raw,
        "raw_payload_sha256": sha256_bytes(raw), "release_clock_text": release_clock,
        "statement_text": statement,
        "statement_text_sha256": sha256_bytes(statement.encode("utf-8")),
    }


DOCUMENT_COLUMNS = (
    "document_id", "contract_id", "event_id", "event_date", "event_clock_utc",
    "source_url", "document_role", "retrieved_utc", "http_status", "latency_ms",
    "response_headers_json", "coverage_state", "error_text", "raw_payload_sha256",
    "raw_payload_gzip", "release_clock_text", "statement_text", "statement_text_sha256",
    "document_json", "row_sha256", "source_known_at_event_clock",
    "retrospective_retrieval", "research_only", "execution_eligible",
    "forecast_proof_eligible",
)


def store_document(connection: sqlite3.Connection, row: Mapping[str, Any]) -> str:
    material = {
        key: value for key, value in row.items() if key not in {"raw_payload", "response_headers"}
    }
    material["response_headers"] = dict(row["response_headers"])
    material.update(
        {
            "contract_id": CONTRACT_ID, "source_known_at_event_clock": 1,
            "retrospective_retrieval": 1, "research_only": 1,
            "execution_eligible": 0, "forecast_proof_eligible": 0,
        }
    )
    document_id = stable_id(
        "fomc_statement_source_v2", CONTRACT_ID, row["event_date"], row["raw_payload_sha256"]
    )
    document_json = canonical_json(material)
    values = (
        document_id, CONTRACT_ID, row["event_id"], row["event_date"], row["event_clock_utc"],
        row["source_url"], row["role"], row["retrieved_utc"], int(row["http_status"]),
        int(row["latency_ms"]), canonical_json(row["response_headers"]),
        row["coverage_state"], row["error_text"], row["raw_payload_sha256"],
        gzip.compress(bytes(row["raw_payload"]), compresslevel=9, mtime=0),
        row["release_clock_text"], row["statement_text"], row["statement_text_sha256"],
        document_json, sha256_bytes(document_json.encode("utf-8")), 1, 1, 1, 0, 0,
    )
    immutable_insert(
        connection, "fomc_statement_source_v2_repair_documents",
        dict(zip(DOCUMENT_COLUMNS, values, strict=True)), "document_id",
    )
    return document_id


def store_resolution(
    connection: sqlite3.Connection, *, event_date: str, document_id: str, generation: str
) -> None:
    payload = {
        "resolution_id": stable_id("fomc_statement_source_v2_resolution", CONTRACT_ID, event_date),
        "contract_id": CONTRACT_ID, "event_date": event_date, "document_id": document_id,
        "source_generation": generation, "research_only": 1,
        "execution_eligible": 0, "forecast_proof_eligible": 0,
    }
    encoded = canonical_json(payload)
    payload.update({"resolution_json": encoded, "row_sha256": sha256_bytes(encoded.encode("utf-8"))})
    immutable_insert(
        connection, "fomc_statement_source_v2_resolutions", payload, "resolution_id"
    )


def snapshot(connection: sqlite3.Connection, contract: Mapping[str, Any]) -> dict[str, Any]:
    resolutions = [
        {
            "event_date": str(row[0]), "document_id": str(row[1]),
            "source_generation": str(row[2]),
        }
        for row in connection.execute(
            "SELECT event_date,document_id,source_generation FROM fomc_statement_source_v2_resolutions "
            "WHERE contract_id=? ORDER BY event_date", (CONTRACT_ID,)
        )
    ]
    repair_count = int(
        connection.execute(
            "SELECT COUNT(*) FROM fomc_statement_source_v2_repair_documents WHERE contract_id=?",
            (CONTRACT_ID,),
        ).fetchone()[0]
    )
    result = {
        "schema_version": SCHEMA_VERSION, "generated_utc": utc_now(),
        "contract_id": CONTRACT_ID, "builder_sha256": contract["builder_sha256"],
        "parent_contract_id": v1.CONTRACT_ID,
        "parent_snapshot_sha256": contract["parent_snapshot_sha256"],
        "source_population_sha256": contract["source_population_sha256"],
        "parent_document_count": EXPECTED_PARENT_DOCUMENTS,
        "repair_document_count": repair_count, "resolved_document_count": len(resolutions),
        "resolutions": resolutions,
        "sqlite_integrity": str(connection.execute("PRAGMA integrity_check").fetchone()[0]),
        "research_only": True, "execution_eligible": False,
        "forecast_proof_eligible": False, "supported_execution_decision": "no_trade",
    }
    result["snapshot_sha256"] = sha256_bytes(canonical_json(result).encode("utf-8"))
    return result


def write_report(result: Mapping[str, Any]) -> None:
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)
    (REPORT_ROOT / "FOMC_STATEMENT_SOURCES_V2.json").write_text(
        json.dumps(result, indent=2, sort_keys=True), encoding="utf-8"
    )
    lines = [
        "# FOMC official statement source archive V2", "",
        f"Generated: `{result['generated_utc']}`", "",
        f"- Resolved official statements: **{result['resolved_document_count']} / 22**",
        f"- Reused immutable V1 documents: **{result['parent_document_count']}**",
        f"- V2 vote-format repairs: **{result['repair_document_count']}**",
        f"- SQLite integrity: **{result['sqlite_integrity']}**",
        "- Scope: source-format compatibility only; no factor, price, or execution change.",
        "- Consensus and event-time policy-path repricing remain unavailable.", "",
        "| Date | Source generation | Document |", "|---|---|---|",
    ]
    lines.extend(
        f"| {row['event_date']} | {row['source_generation']} | `{row['document_id']}` |"
        for row in result["resolutions"]
    )
    (REPORT_ROOT / "FOMC_STATEMENT_SOURCES_V2.md").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )


def build(*, database_path: Path = DATABASE) -> dict[str, Any]:
    connection = sqlite3.connect(database_path, timeout=120)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute("PRAGMA foreign_keys=ON")
    ensure_schema(connection)
    snapshot_sha, parent_rows, missing = parent_snapshot(connection)
    contract = freeze_contract(connection, snapshot_sha, missing)
    connection.commit()
    repair_by_date = {
        str(row[0]): str(row[1])
        for row in connection.execute(
            "SELECT event_date,document_id FROM fomc_statement_source_v2_repair_documents "
            "WHERE contract_id=?", (CONTRACT_ID,)
        )
    }
    for spec in missing:
        event_date = str(spec["event_date"])
        if event_date not in repair_by_date:
            fetched = fetch_source(spec)
            if fetched["coverage_state"] != "exact_official_statement_v2_vote_format":
                connection.close()
                raise RuntimeError(
                    f"official_statement_v2_source_unavailable:{event_date}:"
                    f"{fetched['coverage_state']}:{fetched['error_text']}"
                )
            repair_by_date[event_date] = store_document(connection, fetched)
            connection.commit()
    for row in parent_rows:
        store_resolution(
            connection, event_date=str(row["event_date"]),
            document_id=str(row["document_id"]), generation="v1_exact",
        )
    for event_date, document_id in sorted(repair_by_date.items()):
        store_resolution(
            connection, event_date=event_date, document_id=document_id,
            generation="v2_vote_format_repair",
        )
    connection.commit()
    result = snapshot(connection, contract)
    connection.close()
    if result["resolved_document_count"] != 22 or result["repair_document_count"] != 2:
        raise RuntimeError("exact_twenty_two_resolved_documents_required")
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


__all__ = ["CONTRACT_ID", "build", "ensure_schema", "extract_statement_body", "parent_snapshot"]
