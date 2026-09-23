#!/usr/bin/env python3
"""Archive and normalize exact official FOMC projection releases.

This source-only cohort captures the December 2023 Summary of Economic
Projections baseline and every quarterly SEP through June 2026.  It preserves
the official HTML and extracts the table-1 median projections plus the
explicit prior-projection comparison printed in the same release.

The pages were retrieved retrospectively.  Their official 14:00 Eastern
release clocks are valid event clocks, but this cohort is discovery evidence,
not prospective forecast proof.  It has no price, account, allocator,
authorization, promotion, or execution dependency.
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
import urllib.request
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Iterable, Mapping


ROOT = Path(__file__).resolve().parent
DATABASE = ROOT / "data/oanda_training_manager/state/spike_blurb_factor_reconstruction_v1.sqlite"
REPORT_ROOT = ROOT / (
    "data/oanda_training_manager/reports/spike_blurb_factor_reconstruction/"
    "fomc_sep_sources_v1"
)
CONTRACT_ID = "spike_blurb_fomc_sep_sources_v1_20260820"
SCHEMA_VERSION = 1
FREEZE_UTC = "2026-08-20T19:10:00+00:00"
REQUEST_TIMEOUT_SEC = 30.0
USER_AGENT = "ForexSourceResearch/1.0 (+local research; official source archive)"


SEP_CLOCKS: tuple[tuple[str, str], ...] = (
    ("2023-12-13", "2023-12-13T19:00:00+00:00"),
    ("2024-03-20", "2024-03-20T18:00:00+00:00"),
    ("2024-06-12", "2024-06-12T18:00:00+00:00"),
    ("2024-09-18", "2024-09-18T18:00:00+00:00"),
    ("2024-12-18", "2024-12-18T19:00:00+00:00"),
    ("2025-03-19", "2025-03-19T18:00:00+00:00"),
    ("2025-06-18", "2025-06-18T18:00:00+00:00"),
    ("2025-09-17", "2025-09-17T18:00:00+00:00"),
    ("2025-12-10", "2025-12-10T19:00:00+00:00"),
    ("2026-03-18", "2026-03-18T18:00:00+00:00"),
    ("2026-06-17", "2026-06-17T18:00:00+00:00"),
)


VARIABLE_PREFIXES: tuple[tuple[str, str], ...] = (
    ("change in real gdp", "real_gdp_growth"),
    ("unemployment rate", "unemployment_rate"),
    ("pce inflation", "pce_inflation"),
    ("core pce inflation", "core_pce_inflation"),
    ("federal funds rate", "federal_funds_rate"),
)


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def sha256_file(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def stable_id(*parts: Any) -> str:
    return sha256_bytes("|".join(str(part) for part in parts).encode("utf-8"))


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def immutable_insert(
    connection: sqlite3.Connection,
    table: str,
    row: Mapping[str, Any],
    primary_key: str,
) -> None:
    existing = connection.execute(
        f"SELECT * FROM {table} WHERE {primary_key}=?", (row[primary_key],)
    ).fetchone()
    columns = list(row)
    if existing is not None:
        names = [item[1] for item in connection.execute(f"PRAGMA table_info({table})")]
        if dict(zip(names, existing)) != dict(row):
            raise RuntimeError(f"immutable_conflict:{table}:{row[primary_key]}")
        return
    connection.execute(
        f"INSERT INTO {table} ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",
        tuple(row[column] for column in columns),
    )


def _clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(value)).strip()


class ProjectionTableParser(HTMLParser):
    """Collect table rows without depending on pandas/lxml/BeautifulSoup."""

    HIDDEN = {"script", "style", "noscript", "svg"}

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.hidden_depth = 0
        self.table_depth = 0
        self.current_table: list[list[dict[str, Any]]] | None = None
        self.current_row: list[dict[str, Any]] | None = None
        self.current_cell: dict[str, Any] | None = None
        self.tables: list[list[list[dict[str, Any]]]] = []
        self.visible_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        if tag in self.HIDDEN:
            self.hidden_depth += 1
            return
        if self.hidden_depth:
            return
        if tag == "table":
            self.table_depth += 1
            if self.table_depth == 1:
                self.current_table = []
            return
        if self.table_depth != 1:
            return
        if tag == "tr":
            self.current_row = []
        elif tag in {"th", "td"} and self.current_row is not None:
            self.current_cell = {
                "tag": tag,
                "attrs": {str(key).lower(): str(value or "") for key, value in attrs},
                "parts": [],
            }

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in self.HIDDEN:
            if self.hidden_depth:
                self.hidden_depth -= 1
            return
        if self.hidden_depth:
            return
        if tag in {"th", "td"} and self.current_cell is not None:
            cell = dict(self.current_cell)
            cell["text"] = _clean_text(" ".join(cell.pop("parts")))
            if self.current_row is not None:
                self.current_row.append(cell)
            self.current_cell = None
        elif tag == "tr" and self.table_depth == 1:
            if self.current_table is not None and self.current_row:
                self.current_table.append(self.current_row)
            self.current_row = None
        elif tag == "table":
            if self.table_depth == 1 and self.current_table is not None:
                self.tables.append(self.current_table)
                self.current_table = None
            if self.table_depth:
                self.table_depth -= 1

    def handle_data(self, data: str) -> None:
        if self.hidden_depth or not data.strip():
            return
        self.visible_parts.append(data)
        if self.current_cell is not None:
            self.current_cell["parts"].append(data)


def _variable_name(label: str) -> str | None:
    normalized = re.sub(r"\d+$", "", _clean_text(label).lower()).strip()
    for prefix, name in VARIABLE_PREFIXES:
        if normalized.startswith(prefix):
            return name
    return None


def _numeric_value(text: str) -> float | None:
    value = _clean_text(text).replace("−", "-")
    if not re.fullmatch(r"-?\d+(?:\.\d+)?", value):
        return None
    return float(value)


def parse_projection_release(raw_html: bytes) -> dict[str, Any]:
    decoded = raw_html.decode("utf-8-sig", errors="replace")
    parser = ProjectionTableParser()
    parser.feed(decoded)
    parser.close()
    visible = _clean_text(" ".join(parser.visible_parts))
    release = re.search(r"For release at 2:00 p\.m\.,?\s+(?:EST|EDT)", visible, flags=re.I)
    if release is None:
        raise ValueError("official_release_clock_marker_missing")
    if "Summary of Economic Projections" not in visible:
        raise ValueError("official_sep_title_missing")

    candidates = []
    for table in parser.tables:
        labels = [cell.get("text", "") for row in table for cell in row if cell["tag"] == "th"]
        if any("Projected appropriate policy path" in label for label in labels) and any(
            _variable_name(label) == "federal_funds_rate" for label in labels
        ):
            candidates.append(table)
    if len(candidates) != 1:
        raise ValueError(f"official_sep_table_count_invalid:{len(candidates)}")
    table = candidates[0]
    header_text: dict[str, str] = {}
    for row in table:
        for cell in row:
            identity = cell["attrs"].get("id", "")
            if identity:
                header_text[identity] = cell.get("text", "")

    projections: dict[str, dict[str, Any]] = {}
    active_variable: str | None = None
    for row in table:
        stub = next((cell for cell in row if cell["tag"] == "th"), None)
        if stub is None:
            continue
        label = _clean_text(stub.get("text", ""))
        named = _variable_name(label)
        role = "current"
        comparison_label = ""
        if named is not None:
            active_variable = named
            projections.setdefault(named, {"current": {}, "prior": {}, "comparison_label": ""})
        elif label.lower().endswith("projection") and active_variable is not None:
            named = active_variable
            role = "prior"
            comparison_label = label
            projections[named]["comparison_label"] = label
        else:
            continue

        for cell in row:
            if cell["tag"] != "td":
                continue
            ids = cell["attrs"].get("headers", "").split()
            referenced = [_clean_text(header_text.get(identity, "")) for identity in ids]
            if not any(text.lower().startswith("median") for text in referenced):
                continue
            horizon = next(
                (
                    text
                    for text in referenced
                    if re.fullmatch(r"\d{4}", text) or text.lower() == "longer run"
                ),
                None,
            )
            number = _numeric_value(cell.get("text", ""))
            if horizon is not None and number is not None:
                projections[named][role][horizon.lower().replace(" ", "_")] = number

    expected = {name for _, name in VARIABLE_PREFIXES}
    if set(projections) != expected:
        raise ValueError("official_sep_variable_set_invalid")
    for variable, values in projections.items():
        minimum = 3 if variable == "core_pce_inflation" else 4
        if len(values["current"]) < minimum or len(values["prior"]) < minimum:
            raise ValueError(f"official_sep_projection_coverage_invalid:{variable}")
        if not values["comparison_label"]:
            raise ValueError(f"official_sep_prior_label_missing:{variable}")
    return {
        "release_clock_text": release.group(0),
        "projection_table": projections,
        "visible_text_sha256": sha256_bytes(visible.encode("utf-8")),
    }


def source_specs() -> list[dict[str, Any]]:
    rows = []
    for event_date, event_clock_utc in SEP_CLOCKS:
        compact = event_date.replace("-", "")
        rows.append(
            {
                "event_id": f"fomc_sep_{compact}_v1",
                "event_date": event_date,
                "event_clock_utc": event_clock_utc,
                "source_url": (
                    "https://www.federalreserve.gov/monetarypolicy/"
                    f"fomcprojtabl{compact}.htm"
                ),
                "role": "baseline" if event_date == "2023-12-13" else "analysis_event",
            }
        )
    if len(rows) != 11 or len({row["event_date"] for row in rows}) != 11:
        raise RuntimeError("exact_eleven_sep_sources_required")
    return rows


def ensure_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS fomc_sep_source_contracts_v1 (
          contract_id TEXT PRIMARY KEY, contract_json TEXT NOT NULL,
          builder_sha256 TEXT NOT NULL, source_population_sha256 TEXT NOT NULL,
          frozen_utc TEXT NOT NULL, research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          forecast_proof_eligible INTEGER NOT NULL CHECK(forecast_proof_eligible=0)
        );
        CREATE TABLE IF NOT EXISTS fomc_sep_source_documents_v1 (
          document_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL, event_id TEXT NOT NULL,
          event_date TEXT NOT NULL, event_clock_utc TEXT NOT NULL, source_url TEXT NOT NULL,
          document_role TEXT NOT NULL, retrieved_utc TEXT NOT NULL, http_status INTEGER NOT NULL,
          latency_ms INTEGER NOT NULL, response_headers_json TEXT NOT NULL,
          raw_payload_sha256 TEXT NOT NULL, raw_payload_gzip BLOB NOT NULL,
          release_clock_text TEXT NOT NULL, release_clock_zone_label TEXT NOT NULL,
          clock_zone_label_matches_event_clock INTEGER NOT NULL,
          projection_json TEXT NOT NULL,
          visible_text_sha256 TEXT NOT NULL, document_json TEXT NOT NULL,
          row_sha256 TEXT NOT NULL, retrospective_retrieval INTEGER NOT NULL CHECK(retrospective_retrieval=1),
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          forecast_proof_eligible INTEGER NOT NULL CHECK(forecast_proof_eligible=0),
          UNIQUE(contract_id,event_date)
        );
        CREATE TABLE IF NOT EXISTS fomc_sep_projection_values_v1 (
          value_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL, document_id TEXT NOT NULL,
          event_id TEXT NOT NULL, event_date TEXT NOT NULL, variable_name TEXT NOT NULL,
          projection_role TEXT NOT NULL, comparison_label TEXT NOT NULL,
          horizon TEXT NOT NULL, value_pct REAL NOT NULL, value_json TEXT NOT NULL,
          row_sha256 TEXT NOT NULL, research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          forecast_proof_eligible INTEGER NOT NULL CHECK(forecast_proof_eligible=0),
          UNIQUE(contract_id,event_date,variable_name,projection_role,horizon)
        );
        CREATE TRIGGER IF NOT EXISTS fomc_sep_contract_v1_no_update BEFORE UPDATE ON fomc_sep_source_contracts_v1 BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_sep_contract_v1_no_delete BEFORE DELETE ON fomc_sep_source_contracts_v1 BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_sep_document_v1_no_update BEFORE UPDATE ON fomc_sep_source_documents_v1 BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_sep_document_v1_no_delete BEFORE DELETE ON fomc_sep_source_documents_v1 BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_sep_value_v1_no_update BEFORE UPDATE ON fomc_sep_projection_values_v1 BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_sep_value_v1_no_delete BEFORE DELETE ON fomc_sep_projection_values_v1 BEGIN SELECT RAISE(ABORT,'immutable'); END;
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
        "retrieval_semantics": "retrospective_exact_official_sep_archive",
        "event_clock_semantics": "official_sep_for_release_at_1400_eastern",
        "value_semantics": "table_1_median_and_embedded_prior_projection",
        "causal_limitations": {
            "pre_release_consensus": "unavailable",
            "event_time_market_repricing": "unavailable",
            "retrieval_was_prospective": False,
            "historical_source_bytes_prove_prospective_availability": False,
        },
        "research_only": True,
        "execution_eligible": False,
        "forecast_proof_eligible": False,
        "supported_execution_decision": "no_trade",
        "frozen_utc": FREEZE_UTC,
    }
    existing = connection.execute(
        "SELECT contract_json FROM fomc_sep_source_contracts_v1 WHERE contract_id=?",
        (CONTRACT_ID,),
    ).fetchone()
    if existing is not None:
        if str(existing[0]) != canonical_json(contract):
            raise RuntimeError("immutable_fomc_sep_source_contract_conflict")
        return contract
    connection.execute(
        "INSERT INTO fomc_sep_source_contracts_v1 VALUES(?,?,?,?,?,?,?,?)",
        (CONTRACT_ID, canonical_json(contract), contract["builder_sha256"], population_sha,
         FREEZE_UTC, 1, 0, 0),
    )
    return contract


def fetch_source(spec: Mapping[str, Any]) -> tuple[bytes, dict[str, str], int, str, int]:
    request = urllib.request.Request(
        str(spec["source_url"]), headers={"User-Agent": USER_AGENT, "Accept": "text/html"}
    )
    started = time.perf_counter()
    retrieved = utc_now()
    with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SEC) as response:
        status = int(response.status)
        raw = response.read()
        headers = {
            str(key).lower(): str(value)
            for key, value in response.headers.items()
            if str(key).lower() in {"content-type", "last-modified", "etag", "date"}
        }
    latency_ms = int(round((time.perf_counter() - started) * 1000.0))
    if status != 200 or len(raw) < 10_000:
        raise RuntimeError(f"official_sep_fetch_invalid:{spec['event_date']}:{status}:{len(raw)}")
    return raw, headers, latency_ms, retrieved, status


def store_document(
    connection: sqlite3.Connection, spec: Mapping[str, Any], raw: bytes,
    headers: Mapping[str, str], latency_ms: int, retrieved_utc: str, status: int,
) -> tuple[str, int]:
    parsed = parse_projection_release(raw)
    expected_zone = "EST" if str(spec["event_clock_utc"])[11:13] == "19" else "EDT"
    zone_label = parsed["release_clock_text"].upper().split()[-1]
    clock_zone_label_matches = zone_label == expected_zone
    document_id = stable_id("fomc_sep_source_document_v1", CONTRACT_ID, spec["event_date"])
    projection_json = canonical_json(parsed["projection_table"])
    document_payload = {
        "document_id": document_id,
        "contract_id": CONTRACT_ID,
        "event_id": spec["event_id"],
        "event_date": spec["event_date"],
        "event_clock_utc": spec["event_clock_utc"],
        "source_url": spec["source_url"],
        "document_role": spec["role"],
        "retrieved_utc": retrieved_utc,
        "http_status": status,
        "latency_ms": latency_ms,
        "response_headers": dict(headers),
        "raw_payload_sha256": sha256_bytes(raw),
        "release_clock_text": parsed["release_clock_text"],
        "release_clock_zone_label": zone_label,
        "clock_zone_label_matches_event_clock": clock_zone_label_matches,
        "projection_table": parsed["projection_table"],
        "visible_text_sha256": parsed["visible_text_sha256"],
        "retrospective_retrieval": True,
        "research_only": True,
        "execution_eligible": False,
        "forecast_proof_eligible": False,
    }
    row = {
        "document_id": document_id,
        "contract_id": CONTRACT_ID,
        "event_id": str(spec["event_id"]),
        "event_date": str(spec["event_date"]),
        "event_clock_utc": str(spec["event_clock_utc"]),
        "source_url": str(spec["source_url"]),
        "document_role": str(spec["role"]),
        "retrieved_utc": retrieved_utc,
        "http_status": status,
        "latency_ms": latency_ms,
        "response_headers_json": canonical_json(dict(headers)),
        "raw_payload_sha256": document_payload["raw_payload_sha256"],
        "raw_payload_gzip": gzip.compress(raw, compresslevel=9, mtime=0),
        "release_clock_text": parsed["release_clock_text"],
        "release_clock_zone_label": zone_label,
        "clock_zone_label_matches_event_clock": int(clock_zone_label_matches),
        "projection_json": projection_json,
        "visible_text_sha256": parsed["visible_text_sha256"],
        "document_json": canonical_json(document_payload),
        "row_sha256": sha256_bytes(canonical_json(document_payload).encode("utf-8")),
        "retrospective_retrieval": 1,
        "research_only": 1,
        "execution_eligible": 0,
        "forecast_proof_eligible": 0,
    }
    immutable_insert(connection, "fomc_sep_source_documents_v1", row, "document_id")
    value_count = 0
    for variable, states in parsed["projection_table"].items():
        for role in ("current", "prior"):
            for horizon, value in states[role].items():
                value_id = stable_id(
                    "fomc_sep_projection_value_v1", CONTRACT_ID, spec["event_date"],
                    variable, role, horizon,
                )
                value_payload = {
                    "value_id": value_id,
                    "contract_id": CONTRACT_ID,
                    "document_id": document_id,
                    "event_id": spec["event_id"],
                    "event_date": spec["event_date"],
                    "variable_name": variable,
                    "projection_role": role,
                    "comparison_label": states["comparison_label"],
                    "horizon": horizon,
                    "value_pct": float(value),
                    "research_only": True,
                    "execution_eligible": False,
                    "forecast_proof_eligible": False,
                }
                value_row = {
                    **{key: value_payload[key] for key in (
                        "value_id", "contract_id", "document_id", "event_id", "event_date",
                        "variable_name", "projection_role", "comparison_label", "horizon",
                        "value_pct",
                    )},
                    "value_json": canonical_json(value_payload),
                    "row_sha256": sha256_bytes(canonical_json(value_payload).encode("utf-8")),
                    "research_only": 1,
                    "execution_eligible": 0,
                    "forecast_proof_eligible": 0,
                }
                immutable_insert(connection, "fomc_sep_projection_values_v1", value_row, "value_id")
                value_count += 1
    return document_id, value_count


def write_report(result: Mapping[str, Any]) -> None:
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)
    (REPORT_ROOT / "FOMC_SEP_SOURCES_V1.json").write_text(
        json.dumps(dict(result), indent=2, sort_keys=True), encoding="utf-8"
    )
    lines = [
        "# FOMC SEP official source archive V1", "",
        f"- Official SEP pages: **{result['source_documents']}/11**",
        f"- Normalized projection values: **{result['projection_values']:,}**",
        f"- Official page zone-label discrepancies: **{result['clock_zone_label_discrepancies']}**",
        f"- SQLite integrity: **{result['sqlite_integrity']}**", "",
        "The archive is retrospective, research-only, and direction-abstaining. It preserves",
        "exact official projection levels and embedded prior-projection comparisons, but does",
        "not contain pre-release consensus or event-time market repricing.", "",
        "Supported execution decision: **no_trade**.",
    ]
    (REPORT_ROOT / "FOMC_SEP_SOURCES_V1.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def build(*, database_path: Path = DATABASE) -> dict[str, Any]:
    specs = source_specs()
    connection = sqlite3.connect(database_path, timeout=120.0)
    connection.execute("PRAGMA foreign_keys=ON")
    ensure_schema(connection)
    contract = freeze_contract(connection, specs)
    connection.commit()
    total_values = 0
    for spec in specs:
        raw, headers, latency, retrieved, status = fetch_source(spec)
        _, count = store_document(connection, spec, raw, headers, latency, retrieved, status)
        total_values += count
        connection.commit()
    integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    documents = int(connection.execute(
        "SELECT COUNT(*) FROM fomc_sep_source_documents_v1 WHERE contract_id=?", (CONTRACT_ID,)
    ).fetchone()[0])
    values = int(connection.execute(
        "SELECT COUNT(*) FROM fomc_sep_projection_values_v1 WHERE contract_id=?", (CONTRACT_ID,)
    ).fetchone()[0])
    zone_discrepancies = int(connection.execute(
        "SELECT COUNT(*) FROM fomc_sep_source_documents_v1 "
        "WHERE contract_id=? AND clock_zone_label_matches_event_clock=0", (CONTRACT_ID,)
    ).fetchone()[0])
    snapshot_rows = connection.execute(
        "SELECT row_sha256 FROM fomc_sep_source_documents_v1 WHERE contract_id=? ORDER BY event_date",
        (CONTRACT_ID,),
    ).fetchall()
    connection.close()
    if documents != 11 or values != total_values:
        raise RuntimeError(f"fomc_sep_source_count_mismatch:{documents}:{values}:{total_values}")
    result = {
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "builder_sha256": contract["builder_sha256"],
        "source_population_sha256": contract["source_population_sha256"],
        "source_documents": documents,
        "projection_values": values,
        "clock_zone_label_discrepancies": zone_discrepancies,
        "snapshot_sha256": sha256_bytes(canonical_json(snapshot_rows).encode("utf-8")),
        "sqlite_integrity": integrity,
        "research_only": True,
        "forecast_proof_eligible": False,
        "execution_eligible": False,
        "supported_execution_decision": "no_trade",
        "generated_utc": utc_now(),
    }
    write_report(result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=DATABASE)
    args = parser.parse_args()
    print(json.dumps(build(database_path=args.database), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
