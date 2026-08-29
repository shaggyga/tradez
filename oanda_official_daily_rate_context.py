#!/usr/bin/env python3
"""Collect official daily rate/funding context for configured currencies.

This is a slow H4/H24 research source. It deliberately cannot populate the
intraday OIS/policy-futures contract used by news verification. Initial remote
histories are bootstrap current views; only subsequently observed new dates are
prospective. Existing U.S. Treasury causal metadata is preserved when bridged.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import io
import json
import math
import os
import re
import sqlite3
import time
import urllib.request
import zipfile
from itertools import combinations
from pathlib import Path
from typing import Any, Mapping
from xml.etree import ElementTree as ET

from oanda_local_news_sentiment import normalized_observation_time


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
STATE = DATA / "state"
CONFIG = ROOT / "config" / "official_daily_rate_context_v1.json"
DB = STATE / "official_daily_rate_context_v1.sqlite"
OUTPUT = STATE / "official_daily_rate_context_v1.json"
REPORT = DATA / "reports" / "official_daily_rates" / "OFFICIAL_DAILY_RATE_CONTEXT_CURRENT.md"
ARCHIVE = DATA / "source_archives" / "official_daily_rate_context_v1"
UTC = dt.timezone.utc


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def stable_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def iso(value: dt.datetime) -> str:
    return value.astimezone(UTC).isoformat()


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(value, encoding="utf-8")
    for attempt in range(12):
        try:
            os.replace(temporary, path)
            return
        except PermissionError:
            if attempt == 11:
                temporary.unlink(missing_ok=True)
                raise
            time.sleep(min(0.5, 0.025 * (2**attempt)))


def atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    atomic_text(path, json.dumps(value, indent=2, sort_keys=True))


def atomic_bytes(path: Path, value: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_bytes(value)
    os.replace(temporary, path)


def source_contract(config: Mapping[str, Any], source: Mapping[str, Any]) -> dict[str, Any]:
    collector_sha = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    definition = {
        "root_contract_id": config.get("contract_id"),
        "source": source,
        "collector_sha256": collector_sha,
    }
    definition_sha = stable_hash(definition)
    namespace = "".join(
        character if character.isalnum() or character in {"_", "-"} else "_"
        for character in str(config.get("contract_id") or "official_daily_rate_context")
    )
    cohort_start = "".join(
        character for character in str(config.get("cohort_start_utc") or "undated")
        if character.isdigit()
    )[:14] or "undated"
    return {
        "cohort_id": f"{namespace}.discovery.{cohort_start}.{definition_sha[:16]}",
        "cohort_definition_sha256": definition_sha,
        "collector_sha256": collector_sha,
        "source_contract_id": str(source.get("source_contract_id") or ""),
        "source_id": str(source.get("source_id") or ""),
        "provider": str(source.get("provider") or ""),
        "currency": str(source.get("currency") or ""),
        "series_id": str(source.get("series_id") or ""),
        "comparison_group": str(
            source.get("comparison_group") or "two_year_market_rate_context"
        ),
        "tenor_label": str(source.get("tenor_label") or "2Y"),
        "rate_measure": str(source.get("rate_measure") or "two_year_rate"),
        "initial_remote_history_proof_eligible": False,
        "new_date_first_observed_prospective": True,
        "intraday_rate_confirmation": False,
        "direction_policy": "abstain",
        "material_change_requires_new_cohort": True,
        "research_only": True,
        "execution_eligible": False,
    }


def connect(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=30)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=FULL")
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS daily_rate_observations (
          observation_id TEXT PRIMARY KEY, cohort_id TEXT NOT NULL,
          source_contract_id TEXT NOT NULL, source_id TEXT NOT NULL,
          provider TEXT NOT NULL, currency TEXT NOT NULL,
          series_id TEXT NOT NULL, first_seen_utc TEXT NOT NULL,
          rate_date TEXT NOT NULL, rate_pct REAL NOT NULL,
          version INTEGER NOT NULL, supersedes_observation_id TEXT,
          observation_kind TEXT NOT NULL, bootstrap_current_view INTEGER NOT NULL,
          prospective_eligible INTEGER NOT NULL,
          upstream_observation_id TEXT, upstream_prospective_eligible INTEGER NOT NULL,
          substantive_sha256 TEXT NOT NULL, raw_archive_sha256 TEXT NOT NULL,
          source_contract_json TEXT NOT NULL,
          UNIQUE(cohort_id,rate_date,substantive_sha256)
        );
        CREATE INDEX IF NOT EXISTS ix_daily_rate_latest
          ON daily_rate_observations(currency,rate_date,version DESC);
        CREATE TABLE IF NOT EXISTS collection_cycles (
          cycle_id TEXT PRIMARY KEY, observed_utc TEXT NOT NULL,
          status TEXT NOT NULL, source_count INTEGER NOT NULL,
          succeeded_count INTEGER NOT NULL, inserted_rows INTEGER NOT NULL,
          prospective_rows INTEGER NOT NULL, errors_json TEXT NOT NULL
        );
        CREATE TRIGGER IF NOT EXISTS daily_rate_no_update
          BEFORE UPDATE ON daily_rate_observations BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS daily_rate_no_delete
          BEFORE DELETE ON daily_rate_observations BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS daily_rate_cycle_no_update
          BEFORE UPDATE ON collection_cycles BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS daily_rate_cycle_no_delete
          BEFORE DELETE ON collection_cycles BEGIN SELECT RAISE(ABORT,'append_only'); END;
        """
    )
    db.commit()
    return db


def fetch_bytes(url: str) -> bytes:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "forex-research/1.0",
            "Accept": "text/csv,application/json,text/html",
        },
    )
    with urllib.request.urlopen(request, timeout=40) as response:
        return response.read()


def parse_ecb_csv(raw: bytes) -> list[dict[str, Any]]:
    rows = []
    for row in csv.DictReader(io.StringIO(raw.decode("utf-8-sig"))):
        value = finite(row.get("OBS_VALUE"))
        date = str(row.get("TIME_PERIOD") or "")
        if value is not None and date:
            rows.append({"rate_date": date, "rate_pct": value})
    return rows


def parse_boc_json(raw: bytes, series_id: str) -> list[dict[str, Any]]:
    payload = json.loads(raw.decode("utf-8-sig"))
    rows = []
    for row in payload.get("observations") or []:
        cell = row.get(series_id) if isinstance(row, Mapping) else None
        value = finite(cell.get("v")) if isinstance(cell, Mapping) else None
        date = str(row.get("d") or "") if isinstance(row, Mapping) else ""
        if value is not None and date:
            rows.append({"rate_date": date, "rate_pct": value})
    return rows


def parse_mof_jgb_csv(raw: bytes) -> list[dict[str, Any]]:
    """Parse Japan MOF constant-maturity JGB history and retain the 2Y tenor."""
    text = raw.decode("utf-8-sig", errors="replace")
    records = list(csv.reader(io.StringIO(text)))
    if len(records) < 3:
        return []
    header_index = next(
        (index for index, row in enumerate(records) if row and row[0].strip() == "Date"),
        None,
    )
    if header_index is None:
        return []
    header = [cell.strip() for cell in records[header_index]]
    try:
        date_index = header.index("Date")
        value_index = header.index("2Y")
    except ValueError:
        return []
    rows = []
    for record in records[header_index + 1 :]:
        if len(record) <= max(date_index, value_index):
            continue
        value = finite(record[value_index])
        raw_date = record[date_index].strip()
        if value is None or not raw_date:
            continue
        try:
            rate_date = dt.datetime.strptime(raw_date, "%Y/%m/%d").date().isoformat()
        except ValueError:
            continue
        rows.append({"rate_date": rate_date, "rate_pct": value})
    return rows


def parse_rba_f2_csv(raw: bytes, series_id: str) -> list[dict[str, Any]]:
    """Parse RBA F2 daily government-yield CSV by its explicit series ID."""
    records = list(csv.reader(io.StringIO(raw.decode("utf-8-sig", errors="replace"))))
    series_row_index = next(
        (index for index, row in enumerate(records) if row and row[0].strip() == "Series ID"),
        None,
    )
    if series_row_index is None:
        return []
    series_row = records[series_row_index]
    try:
        value_index = next(
            index for index, value in enumerate(series_row) if value.strip() == series_id
        )
    except StopIteration:
        return []
    rows = []
    for record in records[series_row_index + 1 :]:
        if len(record) <= value_index:
            continue
        value = finite(record[value_index])
        raw_date = record[0].strip() if record else ""
        if value is None or not raw_date:
            continue
        try:
            rate_date = dt.datetime.strptime(raw_date, "%d-%b-%Y").date().isoformat()
        except ValueError:
            continue
        rows.append({"rate_date": rate_date, "rate_pct": value})
    return rows


def parse_mas_sgs_benchmark_html(raw: bytes) -> list[dict[str, Any]]:
    """Parse the MAS daily SGS benchmark table's two-year bond yield.

    MAS labels yield cells explicitly.  Their order is 6-month T-bill,
    1-year T-bill, then 2/5/10/... year bond yields; therefore the third yield
    cell is the two-year benchmark.  Rows lacking that complete schema fail
    closed instead of shifting to another tenor.
    """

    text = raw.decode("utf-8", errors="replace")
    rows: list[dict[str, Any]] = []
    for table_row in re.findall(r"<tr\b[^>]*>(.*?)</tr>", text, flags=re.I | re.S):
        date_match = re.search(
            r'<td\b[^>]*class=["\']date["\'][^>]*>\s*([^<]+?)\s*</td>',
            table_row,
            flags=re.I | re.S,
        )
        if date_match is None:
            continue
        yields = re.findall(
            r'<td\b[^>]*class=["\']yield["\'][^>]*>\s*([^<]*?)\s*</td>',
            table_row,
            flags=re.I | re.S,
        )
        if len(yields) < 3:
            continue
        value = finite(yields[2])
        if value is None:
            continue
        try:
            rate_date = dt.datetime.strptime(
                date_match.group(1).strip(), "%d %b %Y"
            ).date().isoformat()
        except ValueError:
            continue
        rows.append({"rate_date": rate_date, "rate_pct": value})
    return rows


def parse_hkma_daily_rate_json(
    raw: bytes,
    *,
    value_field: str,
) -> list[dict[str, Any]]:
    """Parse one explicitly configured daily rate from an HKMA Open API."""

    payload = json.loads(raw.decode("utf-8-sig"))
    records = (payload.get("result") or {}).get("records") or []
    rows = []
    for record in records:
        if not isinstance(record, Mapping):
            continue
        date = str(record.get("end_of_day") or record.get("end_of_date") or "")
        value = finite(record.get(value_field))
        if not date or value is None:
            continue
        try:
            rate_date = dt.date.fromisoformat(date).isoformat()
        except ValueError:
            continue
        rows.append({"rate_date": rate_date, "rate_pct": value})
    rows.sort(key=lambda row: row["rate_date"])
    return rows


def _xlsx_cell_value(cell: ET.Element, shared_strings: list[str]) -> str | None:
    namespace = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
    value = cell.find(namespace + "v")
    if value is None or value.text is None:
        return None
    if cell.attrib.get("t") == "s":
        try:
            return shared_strings[int(value.text)]
        except (IndexError, ValueError):
            return None
    return value.text


def parse_boe_latest_yield_zip(
    raw: bytes, *, workbook_name: str, sheet_name: str, maturity_years: float
) -> list[dict[str, Any]]:
    """Parse one maturity from the Bank of England's bounded latest-data ZIP."""
    spreadsheet_namespace = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
    office_rel_namespace = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}"
    package_rel_namespace = "{http://schemas.openxmlformats.org/package/2006/relationships}"
    with zipfile.ZipFile(io.BytesIO(raw)) as outer:
        candidates = {
            Path(name).name.casefold(): name for name in outer.namelist() if not name.endswith("/")
        }
        member = candidates.get(workbook_name.casefold())
        if member is None:
            raise ValueError(f"missing workbook in BOE ZIP: {workbook_name}")
        workbook_bytes = outer.read(member)
    with zipfile.ZipFile(io.BytesIO(workbook_bytes)) as workbook:
        shared_strings: list[str] = []
        if "xl/sharedStrings.xml" in workbook.namelist():
            shared_root = ET.fromstring(workbook.read("xl/sharedStrings.xml"))
            for item in shared_root.findall(spreadsheet_namespace + "si"):
                shared_strings.append("".join(
                    node.text or "" for node in item.iter(spreadsheet_namespace + "t")
                ))
        workbook_root = ET.fromstring(workbook.read("xl/workbook.xml"))
        relationship_id = None
        for sheet in workbook_root.iter(spreadsheet_namespace + "sheet"):
            if sheet.attrib.get("name") == sheet_name:
                relationship_id = sheet.attrib.get(office_rel_namespace + "id")
                break
        if relationship_id is None:
            raise ValueError(f"missing BOE worksheet: {sheet_name}")
        relationships = ET.fromstring(workbook.read("xl/_rels/workbook.xml.rels"))
        target = None
        for relationship in relationships.findall(package_rel_namespace + "Relationship"):
            if relationship.attrib.get("Id") == relationship_id:
                target = relationship.attrib.get("Target")
                break
        if not target:
            raise ValueError(f"missing BOE worksheet relationship: {sheet_name}")
        target = target.lstrip("/")
        sheet_path = target if target.startswith("xl/") else "xl/" + target
        sheet_root = ET.fromstring(workbook.read(sheet_path))
    rows_by_number: dict[int, dict[str, str]] = {}
    for row in sheet_root.iter(spreadsheet_namespace + "row"):
        try:
            row_number = int(row.attrib.get("r") or "0")
        except ValueError:
            continue
        cells: dict[str, str] = {}
        for cell in row.findall(spreadsheet_namespace + "c"):
            reference = str(cell.attrib.get("r") or "")
            column = "".join(character for character in reference if character.isalpha())
            value = _xlsx_cell_value(cell, shared_strings)
            if column and value is not None:
                cells[column] = value
        rows_by_number[row_number] = cells
    maturity_column = None
    for column, value in rows_by_number.get(4, {}).items():
        parsed = finite(value)
        if parsed is not None and abs(parsed - maturity_years) < 1e-8:
            maturity_column = column
            break
    if maturity_column is None:
        raise ValueError(f"missing BOE maturity: {maturity_years}")
    rows = []
    excel_epoch = dt.date(1899, 12, 30)
    for row_number in sorted(number for number in rows_by_number if number >= 6):
        cells = rows_by_number[row_number]
        excel_date = finite(cells.get("A"))
        value = finite(cells.get(maturity_column))
        if excel_date is None or value is None:
            continue
        rate_date = (excel_epoch + dt.timedelta(days=int(excel_date))).isoformat()
        rows.append({"rate_date": rate_date, "rate_pct": value})
    return rows


def load_treasury_rows(path: Path) -> tuple[list[dict[str, Any]], str]:
    db = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True, timeout=10)
    rows = [
        {
            "rate_date": row[3], "rate_pct": float(row[4]),
            "first_seen_utc": row[2], "upstream_observation_id": row[0],
            "upstream_prospective_eligible": bool(row[5]),
        }
        for row in db.execute(
            """SELECT observation_id,cohort_id,observed_utc,yield_date,two_year_pct,
                      prospective_eligible
               FROM rate_observations ORDER BY yield_date,observation_version"""
        )
    ]
    integrity = str(db.execute("PRAGMA quick_check").fetchone()[0])
    db.close()
    return rows, stable_hash({"rows": rows, "upstream_integrity": integrity})


def acquire_source(
    source: Mapping[str, Any], archive: Path
) -> tuple[list[dict[str, Any]], str]:
    kind = str(source.get("kind") or "")
    if kind == "local_treasury_sqlite":
        return load_treasury_rows(ROOT / str(source["database"]))
    raw = fetch_bytes(str(source["url"]))
    raw_sha = hashlib.sha256(raw).hexdigest()
    archive.mkdir(parents=True, exist_ok=True)
    if kind in {"ecb_csv", "mof_jgb_csv", "rba_f2_csv"}:
        suffix = ".csv"
    elif kind == "boe_latest_yield_zip":
        suffix = ".zip"
    elif kind == "mas_sgs_benchmark_html":
        suffix = ".html"
    else:
        suffix = ".json"
    raw_path = archive / f"{raw_sha}{suffix}"
    if not raw_path.exists():
        atomic_bytes(raw_path, raw)
    if kind == "ecb_csv":
        return parse_ecb_csv(raw), raw_sha
    if kind == "boc_valet_json":
        return parse_boc_json(raw, str(source["series_id"])), raw_sha
    if kind == "mof_jgb_csv":
        return parse_mof_jgb_csv(raw), raw_sha
    if kind == "rba_f2_csv":
        return parse_rba_f2_csv(raw, str(source["series_id"])), raw_sha
    if kind == "boe_latest_yield_zip":
        return parse_boe_latest_yield_zip(
            raw,
            workbook_name=str(source["workbook_name"]),
            sheet_name=str(source["sheet_name"]),
            maturity_years=float(source["maturity_years"]),
        ), raw_sha
    if kind == "mas_sgs_benchmark_html":
        return parse_mas_sgs_benchmark_html(raw), raw_sha
    if kind == "hkma_daily_rate_json":
        return parse_hkma_daily_rate_json(
            raw, value_field=str(source["value_field"])
        ), raw_sha
    raise ValueError(f"unsupported source kind: {kind}")


def ingest_source(
    db: sqlite3.Connection,
    *,
    rows: list[Mapping[str, Any]],
    observed: dt.datetime,
    contract: Mapping[str, Any],
    raw_sha: str,
) -> tuple[int, int]:
    cohort = str(contract["cohort_id"])
    initial = int(db.execute(
        "SELECT COUNT(*) FROM daily_rate_observations WHERE cohort_id=?", (cohort,)
    ).fetchone()[0]) == 0
    maximum_prior_date = db.execute(
        "SELECT MAX(rate_date) FROM daily_rate_observations WHERE cohort_id=?", (cohort,)
    ).fetchone()[0]
    inserted = prospective = 0
    for row in rows:
        date = str(row.get("rate_date") or "")
        value = finite(row.get("rate_pct"))
        if not date or value is None:
            continue
        substantive = stable_hash({"date": date, "value": value})
        prior = db.execute(
            """SELECT observation_id,substantive_sha256,version
               FROM daily_rate_observations WHERE cohort_id=? AND rate_date=?
               ORDER BY version DESC,rowid DESC LIMIT 1""",
            (cohort, date),
        ).fetchone()
        if prior and str(prior[1]) == substantive:
            continue
        upstream_eligible = bool(row.get("upstream_prospective_eligible"))
        is_new = bool(not initial and prior is None and maximum_prior_date and date > str(maximum_prior_date))
        eligible = upstream_eligible or is_new
        if upstream_eligible:
            kind = "upstream_prospective_import"
        elif initial:
            kind = "bootstrap_current_view"
        elif is_new:
            kind = "new_rate_date_first_observed"
        else:
            kind = "revision_or_late_history"
        first_seen = str(row.get("first_seen_utc") or iso(observed))
        version = int(prior[2]) + 1 if prior else 1
        observation_id = "daily_rate_" + stable_hash(
            (cohort, date, substantive, version)
        )[:30]
        cursor = db.execute(
            "INSERT OR IGNORE INTO daily_rate_observations VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                observation_id, cohort, contract["source_contract_id"], contract["source_id"],
                contract["provider"], contract["currency"], contract["series_id"],
                first_seen, date, value, version, prior[0] if prior else None, kind,
                int(initial and not upstream_eligible), int(eligible),
                row.get("upstream_observation_id"), int(upstream_eligible), substantive,
                raw_sha, canonical_json(contract),
            ),
        )
        changed = int(cursor.rowcount > 0)
        inserted += changed
        prospective += changed * int(eligible)
    db.commit()
    return inserted, prospective


def build_state(
    db: sqlite3.Connection, active_cohorts: Mapping[str, str] | None = None
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    currencies: dict[str, Any] = {}
    if active_cohorts is None:
        active_cohorts = dict(db.execute(
            "SELECT currency,MAX(cohort_id) FROM daily_rate_observations GROUP BY currency"
        ).fetchall())
    for currency, cohort_id in sorted(active_cohorts.items()):
        rows = db.execute(
            """SELECT observation_id,cohort_id,source_contract_id,source_id,provider,
                      first_seen_utc,rate_date,rate_pct,observation_kind,
                      prospective_eligible,source_contract_json
               FROM daily_rate_observations WHERE currency=? AND cohort_id=?
               ORDER BY rate_date DESC,version DESC,rowid DESC LIMIT 2""",
            (currency, cohort_id),
        ).fetchall()
        if not rows:
            continue
        latest, prior = rows[0], rows[1] if len(rows) > 1 else None
        change_bps = (
            None if prior is None
            else round((float(latest[7]) - float(prior[7])) * 100.0, 12)
        )
        try:
            contract = json.loads(str(latest[10] or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            contract = {}
        currencies[str(currency)] = {
            "observation_id": latest[0], "cohort_id": latest[1],
            "source_contract_id": latest[2], "source_id": latest[3],
            "provider": latest[4], "observed_utc": latest[5],
            "rate_date": latest[6], "rate_pct": float(latest[7]),
            "change_bps_1d": change_bps, "observation_kind": latest[8],
            "prospective_eligible": bool(latest[9]),
            "intraday_rate_confirmation": False,
            "comparison_group": str(
                contract.get("comparison_group")
                or "two_year_market_rate_context"
            ),
            "tenor_label": str(contract.get("tenor_label") or "2Y"),
            "rate_measure": str(contract.get("rate_measure") or "two_year_rate"),
        }
    differentials = []
    for left, right in combinations(sorted(currencies), 2):
        a, b = currencies[left], currencies[right]
        comparable = a["comparison_group"] == b["comparison_group"]
        aligned = comparable and a["rate_date"] == b["rate_date"]
        delta_a, delta_b = a.get("change_bps_1d"), b.get("change_bps_1d")
        differentials.append(
            {
                "pair": f"{left}_{right}", "base_currency": left, "quote_currency": right,
                "rate_date": a["rate_date"] if aligned else None,
                "aligned_date": aligned,
                "comparable_measure": comparable,
                "comparison_group": (
                    a["comparison_group"] if comparable else None
                ),
                "differential_pct": round(a["rate_pct"] - b["rate_pct"], 12) if aligned else None,
                "change_differential_bps_1d": (
                    round(delta_a - delta_b, 12)
                    if aligned and delta_a is not None and delta_b is not None else None
                ),
                "prospective_eligible": bool(
                    aligned and a["prospective_eligible"] and b["prospective_eligible"]
                ),
                "direction_policy": "abstain",
            }
        )
    return currencies, differentials


def run_once(
    *,
    config_path: Path = CONFIG,
    database: Path = DB,
    output: Path = OUTPUT,
    report: Path = REPORT,
    archive: Path = ARCHIVE,
    observed: dt.datetime | None = None,
    acquired: Mapping[str, tuple[list[dict[str, Any]], str]] | None = None,
) -> dict[str, Any]:
    config = read_json(config_path)
    if observed is None:
        observed, clock = normalized_observation_time(dt.datetime.now(UTC))
    else:
        observed = observed.astimezone(UTC)
        clock = {"source": "provided_test_time", "trusted_for_prospective_evidence": True}
    db = connect(database)
    errors: dict[str, str] = {}
    inserted = prospective = succeeded = 0
    contracts = []
    for source in config.get("sources") or []:
        if not isinstance(source, Mapping):
            continue
        contract = source_contract(config, source)
        contracts.append(contract)
        try:
            rows, raw_sha = (
                acquired[str(source["source_id"])] if acquired is not None
                else acquire_source(source, archive)
            )
            source_inserted, source_prospective = ingest_source(
                db, rows=rows, observed=observed, contract=contract, raw_sha=raw_sha
            )
            inserted += source_inserted
            prospective += source_prospective
            succeeded += 1
        except Exception as exc:  # source isolation is intentional
            errors[str(source.get("source_id") or "unknown")] = f"{type(exc).__name__}: {exc}"
    active_cohorts = {str(contract["currency"]): str(contract["cohort_id"]) for contract in contracts}
    currencies, differentials = build_state(db, active_cohorts)
    cycle_id = "daily_rate_cycle_" + stable_hash(
        (iso(observed), [c["cohort_id"] for c in contracts], errors)
    )[:28]
    db.execute(
        "INSERT OR IGNORE INTO collection_cycles VALUES (?,?,?,?,?,?,?,?)",
        (
            cycle_id, iso(observed), "ok" if not errors else "partial",
            len(contracts), succeeded, inserted, prospective, canonical_json(errors),
        ),
    )
    db.commit()
    cohort_ids = list(active_cohorts.values())
    placeholders = ",".join("?" for _ in cohort_ids)
    totals = {
        "observations": int(db.execute(
            f"SELECT COUNT(*) FROM daily_rate_observations WHERE cohort_id IN ({placeholders})",
            cohort_ids,
        ).fetchone()[0]),
        "prospective_rows": int(db.execute(
            f"SELECT COUNT(*) FROM daily_rate_observations WHERE prospective_eligible=1 AND cohort_id IN ({placeholders})",
            cohort_ids,
        ).fetchone()[0]),
        "currencies": len(currencies),
        "rate_dates": int(db.execute(
            f"SELECT COUNT(DISTINCT rate_date) FROM daily_rate_observations WHERE cohort_id IN ({placeholders})",
            cohort_ids,
        ).fetchone()[0]),
        "all_cohort_observations": int(db.execute(
            "SELECT COUNT(*) FROM daily_rate_observations"
        ).fetchone()[0]),
    }
    integrity = str(db.execute("PRAGMA quick_check").fetchone()[0])
    db.close()
    payload = {
        "schema_version": 1, "generated_utc": iso(observed),
        "status": "ok" if not errors else "partial", "research_only": True,
        "execution_eligible": False, "can_place_orders": False, "can_promote": False,
        "direction_policy": "abstain", "intraday_rate_confirmation": False,
        "contracts": contracts, "currencies": currencies,
        "cross_currency_differentials": differentials,
        "cycle": {"inserted_rows": inserted, "prospective_rows": prospective,
                  "succeeded_sources": succeeded, "errors": errors},
        "totals": totals, "database_integrity": integrity,
        "observation_clock": clock, "limitations": config.get("limitations") or [],
        "supported_execution_decision": "no_trade",
    }
    atomic_json(output, payload)
    lines = [
        "# Official Daily Rate Context", "", f"Generated: `{payload['generated_utc']}`", "",
        "Research-only H4/H24 context. This is not intraday OIS or policy-futures confirmation.", "",
        f"- Currencies: **{totals['currencies']}**",
        f"- Immutable observations: **{totals['observations']}**",
        f"- Prospective observations: **{totals['prospective_rows']}**",
        f"- SQLite integrity: **{integrity}**", "",
        "| Currency | Date | Measure | Rate | 1d change | Prospective | Source |",
        "|---|---|---|---:|---:|---|---|",
    ]
    for currency, row in sorted(currencies.items()):
        change = row.get("change_bps_1d")
        lines.append(
            f"| {currency} | {row['rate_date']} | {row['tenor_label']} {row['rate_measure']} | {row['rate_pct']:.4f}% | "
            f"{change:.3f} bps | {'yes' if row['prospective_eligible'] else 'no'} | {row['source_id']} |"
            if change is not None else
            f"| {currency} | {row['rate_date']} | {row['tenor_label']} {row['rate_measure']} | {row['rate_pct']:.4f}% | n/a | "
            f"{'yes' if row['prospective_eligible'] else 'no'} | {row['source_id']} |"
        )
    atomic_text(report, "\n".join(lines) + "\n")
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=CONFIG)
    parser.add_argument("--database", type=Path, default=DB)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    parser.add_argument("--report", type=Path, default=REPORT)
    parser.add_argument("--archive", type=Path, default=ARCHIVE)
    parser.add_argument("--interval-sec", type=float, default=21600)
    parser.add_argument("--duration-sec", type=float, default=0)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    stop = time.monotonic() + args.duration_sec if args.duration_sec > 0 else None
    while True:
        result = run_once(
            config_path=args.config, database=args.database, output=args.output,
            report=args.report, archive=args.archive,
        )
        if args.once or stop is None or time.monotonic() >= stop:
            print(json.dumps(result, sort_keys=True))
            return 0
        time.sleep(max(60.0, args.interval_sec))


if __name__ == "__main__":
    raise SystemExit(main())
