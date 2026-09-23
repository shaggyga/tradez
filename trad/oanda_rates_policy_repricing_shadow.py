#!/usr/bin/env python3
"""Immutable, research-only event-time rate-repricing collector and replay.

The default contract deliberately has no connected intraday source. It can
publish access/readiness diagnostics but cannot manufacture a confirmation.
A future permitted connector may append normalized observations through JSONL
only after its source contract is explicitly connected. Replays use knowledge
time, not the latest value in the database.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import hmac
import json
import math
import os
import sqlite3
import stat
from pathlib import Path
from typing import Any, Mapping


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
STATE = DATA / "state"
CONFIG = ROOT / "config" / "rates_policy_repricing_shadow_v2.json"
DB = STATE / "rates_policy_repricing_shadow_v1.sqlite"
OUTPUT = STATE / "rates_policy_repricing_shadow_v1.json"
REPORT = DATA / "reports" / "rates_policy_repricing" / "RATES_POLICY_REPRICING_SHADOW_CURRENT.md"
ARCHIVE = DATA / "source_archives" / "rates_policy_repricing_shadow_v2"
UTC = dt.timezone.utc


class ContractError(ValueError):
    """Raised when an observation cannot satisfy the frozen source contract."""


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def stable_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ContractError(f"configuration is not an object: {path}")
    return value


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    atomic_text(path, json.dumps(value, indent=2, sort_keys=True) + "\n")


def atomic_bytes(path: Path, value: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_bytes(value)
    os.replace(temporary, path)


def parse_utc(value: Any, field: str) -> dt.datetime:
    text = str(value or "").strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError as exc:
        raise ContractError(f"{field} is not ISO-8601: {value!r}") from exc
    if parsed.tzinfo is None:
        raise ContractError(f"{field} must contain an explicit UTC offset")
    return parsed.astimezone(UTC)


def iso(value: dt.datetime) -> str:
    return value.astimezone(UTC).isoformat()


def finite(value: Any, field: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ContractError(f"{field} is not numeric") from exc
    if not math.isfinite(result):
        raise ContractError(f"{field} must be finite")
    return result


def connect(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    database = sqlite3.connect(path, timeout=30)
    database.execute("PRAGMA journal_mode=WAL")
    database.execute("PRAGMA synchronous=FULL")
    database.executescript(
        """
        CREATE TABLE IF NOT EXISTS rate_observations (
          observation_id TEXT PRIMARY KEY,
          cohort_id TEXT NOT NULL,
          source_id TEXT NOT NULL,
          source_contract_id TEXT NOT NULL,
          provider TEXT NOT NULL,
          currency TEXT NOT NULL,
          instrument TEXT NOT NULL,
          tenor_label TEXT NOT NULL,
          rate_pct REAL NOT NULL,
          source_timestamp_utc TEXT NOT NULL,
          retrieved_utc TEXT NOT NULL,
          observed_utc TEXT NOT NULL,
          raw_payload_sha256 TEXT NOT NULL,
          observation_version INTEGER NOT NULL,
          supersedes_observation_id TEXT,
          causal_intraday_eligible INTEGER NOT NULL,
          contract_json TEXT NOT NULL,
          clock_source TEXT NOT NULL,
          clock_trusted INTEGER NOT NULL,
          clock_contract_id TEXT NOT NULL,
          clock_attestation_verified INTEGER NOT NULL,
          raw_archive_path TEXT NOT NULL,
          raw_archive_sha256_verified INTEGER NOT NULL,
          UNIQUE(cohort_id,instrument,source_timestamp_utc,raw_payload_sha256)
        );
        CREATE INDEX IF NOT EXISTS ix_rate_replay
          ON rate_observations(instrument,observed_utc,retrieved_utc,source_timestamp_utc);
        CREATE TABLE IF NOT EXISTS collection_cycles (
          cycle_id TEXT PRIMARY KEY,
          observed_utc TEXT NOT NULL,
          decision_cutoff_utc TEXT NOT NULL,
          imported_rows INTEGER NOT NULL,
          rejected_rows INTEGER NOT NULL,
          rejection_reasons_json TEXT NOT NULL
        );
        CREATE TRIGGER IF NOT EXISTS rate_observation_no_update
          BEFORE UPDATE ON rate_observations BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS rate_observation_no_delete
          BEFORE DELETE ON rate_observations BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS rate_cycle_no_update
          BEFORE UPDATE ON collection_cycles BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS rate_cycle_no_delete
          BEFORE DELETE ON collection_cycles BEGIN SELECT RAISE(ABORT,'append_only'); END;
        """
    )
    columns = {
        str(row[1]) for row in database.execute("PRAGMA table_info(rate_observations)")
    }
    if "clock_source" not in columns:
        database.execute(
            "ALTER TABLE rate_observations ADD COLUMN clock_source TEXT NOT NULL DEFAULT 'legacy_untrusted'"
        )
    if "clock_trusted" not in columns:
        database.execute(
            "ALTER TABLE rate_observations ADD COLUMN clock_trusted INTEGER NOT NULL DEFAULT 0"
        )
    if "clock_contract_id" not in columns:
        database.execute(
            "ALTER TABLE rate_observations ADD COLUMN clock_contract_id TEXT NOT NULL DEFAULT ''"
        )
    if "clock_attestation_verified" not in columns:
        database.execute(
            "ALTER TABLE rate_observations ADD COLUMN clock_attestation_verified INTEGER NOT NULL DEFAULT 0"
        )
    if "raw_archive_path" not in columns:
        database.execute(
            "ALTER TABLE rate_observations ADD COLUMN raw_archive_path TEXT NOT NULL DEFAULT ''"
        )
    if "raw_archive_sha256_verified" not in columns:
        database.execute(
            "ALTER TABLE rate_observations ADD COLUMN raw_archive_sha256_verified INTEGER NOT NULL DEFAULT 0"
        )
    database.commit()
    return database


def source_contract(config: Mapping[str, Any], source: Mapping[str, Any]) -> dict[str, Any]:
    definition = {
        "root_contract_id": str(config.get("contract_id") or ""),
        "cohort_start_utc": str(config.get("cohort_start_utc") or ""),
        "source": source,
    }
    digest = stable_hash(definition)
    start = "".join(c for c in str(config.get("cohort_start_utc") or "") if c.isdigit())[:14]
    return {
        **source,
        "cohort_id": f"rates_policy_repricing.discovery.{start or 'undated'}.{digest[:16]}",
        "cohort_definition_sha256": digest,
        "root_contract_id": str(config.get("contract_id") or ""),
        "cohort_start_utc": str(config.get("cohort_start_utc") or ""),
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "direction_policy": "abstain",
    }


def load_jsonl(path: Path | None) -> list[dict[str, Any]]:
    if path is None:
        return []
    rows: list[dict[str, Any]] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ContractError(f"invalid JSONL at line {line_number}") from exc
        if not isinstance(value, dict):
            raise ContractError(f"JSONL line {line_number} is not an object")
        rows.append(value)
    return rows


def clock_attestation_message(values: Mapping[str, Any]) -> bytes:
    fields = (
        "source_id", "source_contract_id", "provider", "currency", "instrument",
        "tenor_label", "rate_pct", "source_timestamp_utc", "retrieved_utc",
        "observed_utc", "clock_source", "clock_contract_id", "raw_payload_sha256",
    )
    return canonical_json({field: values.get(field) for field in fields}).encode("utf-8")


def clock_attestation_digest(values: Mapping[str, Any], secret: str) -> str:
    return hmac.new(
        secret.encode("utf-8"), clock_attestation_message(values), hashlib.sha256
    ).hexdigest()


def verify_clock_attestation(
    values: Mapping[str, Any], contract: Mapping[str, Any]
) -> bool:
    configured_contract = str(contract.get("clock_contract_id") or "")
    supplied_contract = str(values.get("clock_contract_id") or "")
    allowed_sources = {
        str(value) for value in contract.get("allowed_clock_sources") or [] if str(value)
    }
    secret_name = str(contract.get("clock_attestation_hmac_env") or "")
    secret = os.environ.get(secret_name, "") if secret_name else ""
    supplied_digest = str(values.get("clock_attestation_hmac_sha256") or "").lower()
    if not (
        configured_contract
        and supplied_contract == configured_contract
        and str(values.get("clock_source") or "") in allowed_sources
        and secret
        and len(supplied_digest) == 64
    ):
        return False
    expected = clock_attestation_digest(values, secret)
    return hmac.compare_digest(expected, supplied_digest)


def archive_raw_payload(
    row: Mapping[str, Any], *, contract: Mapping[str, Any], archive_root: Path
) -> tuple[str, bool]:
    source_id = str(contract.get("source_id") or "")
    intake_text = str(contract.get("raw_intake_root") or "").strip()
    if not intake_text:
        raise ContractError("raw intake root is not configured for source")
    intake_path = Path(intake_text)
    raw_path = Path(str(row.get("raw_payload_path") or ""))
    if not intake_path.is_absolute() or not raw_path.is_absolute():
        raise ContractError("raw intake root and payload path must be absolute")
    intake_lexical = Path(os.path.abspath(str(intake_path)))
    raw_lexical = Path(os.path.abspath(str(raw_path)))
    try:
        relative = raw_lexical.relative_to(intake_lexical)
    except ValueError as exc:
        raise ContractError("raw payload path is outside configured intake root") from exc
    if not intake_lexical.is_dir() or intake_lexical.is_symlink():
        raise ContractError("configured raw intake root is unavailable or a symlink")
    current = intake_lexical
    for component in relative.parts:
        current = current / component
        if current.is_symlink():
            raise ContractError("raw payload path contains a symlink")
    try:
        intake_resolved = intake_lexical.resolve(strict=True)
        raw_resolved = raw_lexical.resolve(strict=True)
        raw_resolved.relative_to(intake_resolved)
        mode = raw_resolved.stat().st_mode
    except (OSError, ValueError) as exc:
        raise ContractError("raw payload path is unavailable or escapes intake root") from exc
    if not stat.S_ISREG(mode):
        raise ContractError("raw payload path is not a regular file")
    expected = str(row.get("raw_payload_sha256") or "").lower()
    try:
        raw = raw_resolved.read_bytes()
    except OSError as exc:
        raise ContractError("raw payload could not be read") from exc
    observed = hashlib.sha256(raw).hexdigest()
    if observed != expected:
        raise ContractError("raw payload SHA-256 does not match declared hash")
    safe_source = "".join(
        character if character.isalnum() or character in {"_", "-"} else "_"
        for character in source_id
    ) or "unknown"
    destination = archive_root / safe_source / f"{observed}.raw"
    if not destination.exists():
        atomic_bytes(destination, raw)
    try:
        verified = hashlib.sha256(destination.read_bytes()).hexdigest() == expected
    except OSError as exc:
        raise ContractError("archived raw payload could not be verified") from exc
    if not verified:
        raise ContractError("archived raw payload SHA-256 verification failed")
    return str(destination.resolve()), True


def normalize_observation(
    row: Mapping[str, Any], contract: Mapping[str, Any], *, archive_root: Path
) -> dict[str, Any]:
    expected = {
        "source_id": str(contract.get("source_id") or ""),
        "source_contract_id": str(contract.get("source_contract_id") or ""),
        "provider": str(contract.get("provider") or ""),
        "currency": str(contract.get("currency") or "").upper(),
        "instrument": str(contract.get("instrument") or ""),
        "tenor_label": str(contract.get("tenor_label") or ""),
    }
    for field, expected_value in expected.items():
        actual = str(row.get(field) or "")
        if field == "currency":
            actual = actual.upper()
        if actual != expected_value:
            raise ContractError(f"{field} does not match configured source contract")
    source_time = parse_utc(row.get("source_timestamp_utc"), "source_timestamp_utc")
    retrieved = parse_utc(row.get("retrieved_utc"), "retrieved_utc")
    observed = parse_utc(row.get("observed_utc"), "observed_utc")
    if not source_time <= retrieved <= observed:
        raise ContractError("knowledge clocks must satisfy source <= retrieved <= observed")
    clock_source = str(row.get("clock_source") or "").strip()
    if not clock_source:
        raise ContractError("clock_source is required")
    clock_contract_id = str(row.get("clock_contract_id") or "").strip()
    payload_hash = str(row.get("raw_payload_sha256") or "").lower()
    if len(payload_hash) != 64 or any(c not in "0123456789abcdef" for c in payload_hash):
        raise ContractError("raw_payload_sha256 must be a lowercase SHA-256 digest")
    cohort_start = parse_utc(contract.get("cohort_start_utc"), "cohort_start_utc")
    prospective = bool(source_time >= cohort_start and retrieved >= cohort_start)
    normalized = {
        **expected,
        "cohort_id": str(contract["cohort_id"]),
        "rate_pct": finite(row.get("rate_pct"), "rate_pct"),
        "source_timestamp_utc": iso(source_time),
        "retrieved_utc": iso(retrieved),
        "observed_utc": iso(observed),
        "raw_payload_sha256": payload_hash,
        "contract_json": canonical_json(contract),
        "clock_source": clock_source,
        "clock_contract_id": clock_contract_id,
    }
    clock_verified = verify_clock_attestation(
        {
            **normalized,
            "clock_attestation_hmac_sha256": row.get("clock_attestation_hmac_sha256"),
        },
        contract,
    )
    raw_archive_path, raw_verified = archive_raw_payload(
        row, contract=contract, archive_root=archive_root
    )
    normalized.update({
        "clock_trusted": clock_verified,
        "clock_attestation_verified": clock_verified,
        "raw_archive_path": raw_archive_path,
        "raw_archive_sha256_verified": raw_verified,
        "causal_intraday_eligible": bool(
            contract.get("causal_intraday_eligible")
            and prospective
            and clock_verified
            and raw_verified
        ),
    })
    return normalized


def append_observation(database: sqlite3.Connection, row: Mapping[str, Any]) -> bool:
    prior = database.execute(
        """SELECT observation_id,observation_version,raw_payload_sha256
             FROM rate_observations
            WHERE cohort_id=? AND instrument=? AND source_timestamp_utc=?
            ORDER BY observation_version DESC,rowid DESC LIMIT 1""",
        (row["cohort_id"], row["instrument"], row["source_timestamp_utc"]),
    ).fetchone()
    if prior and str(prior[2]) == str(row["raw_payload_sha256"]):
        return False
    version = int(prior[1]) + 1 if prior else 1
    observation_id = "rate_repricing_" + stable_hash(
        (row["cohort_id"], row["instrument"], row["source_timestamp_utc"],
         row["raw_payload_sha256"], version)
    )[:30]
    cursor = database.execute(
        """INSERT OR IGNORE INTO rate_observations
             (observation_id,cohort_id,source_id,source_contract_id,provider,
              currency,instrument,tenor_label,rate_pct,source_timestamp_utc,
              retrieved_utc,observed_utc,raw_payload_sha256,observation_version,
              supersedes_observation_id,causal_intraday_eligible,contract_json,
              clock_source,clock_trusted,clock_contract_id,
              clock_attestation_verified,raw_archive_path,
              raw_archive_sha256_verified)
             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            observation_id, row["cohort_id"], row["source_id"],
            row["source_contract_id"], row["provider"], row["currency"],
            row["instrument"], row["tenor_label"], row["rate_pct"],
            row["source_timestamp_utc"], row["retrieved_utc"], row["observed_utc"],
            row["raw_payload_sha256"], version, prior[0] if prior else None,
            int(bool(row["causal_intraday_eligible"])), row["contract_json"],
            row["clock_source"], int(bool(row["clock_trusted"])),
            row["clock_contract_id"], int(bool(row["clock_attestation_verified"])),
            row["raw_archive_path"], int(bool(row["raw_archive_sha256_verified"])),
        ),
    )
    return cursor.rowcount > 0


def _latest_versions_as_of(
    database: sqlite3.Connection,
    cutoff: dt.datetime,
    active_identities: set[tuple[str, str, str, str]],
) -> list[dict[str, Any]]:
    rows = database.execute(
        """SELECT observation_id,cohort_id,source_id,source_contract_id,provider,
                  currency,instrument,tenor_label,rate_pct,source_timestamp_utc,
                  retrieved_utc,observed_utc,causal_intraday_eligible,
                  observation_version,clock_source,clock_trusted
                  ,clock_contract_id,clock_attestation_verified,raw_archive_path,
                  raw_archive_sha256_verified
             FROM rate_observations
            WHERE retrieved_utc<=? AND observed_utc<=?
            ORDER BY source_timestamp_utc,observation_version,rowid""",
        (iso(cutoff), iso(cutoff)),
    ).fetchall()
    latest: dict[tuple[str, str, str], tuple[Any, ...]] = {}
    for row in rows:
        identity = (str(row[1]), str(row[2]), str(row[3]), str(row[6]))
        if identity not in active_identities:
            continue
        latest[(str(row[1]), str(row[6]), str(row[9]))] = row
    names = [
        "observation_id", "cohort_id", "source_id", "source_contract_id",
        "provider", "currency", "instrument", "tenor_label", "rate_pct",
        "source_timestamp_utc", "retrieved_utc", "observed_utc",
        "causal_intraday_eligible", "observation_version",
        "clock_source", "clock_trusted",
        "clock_contract_id", "clock_attestation_verified", "raw_archive_path",
        "raw_archive_sha256_verified",
    ]
    return [dict(zip(names, row)) for row in latest.values()]


def _window_change(
    rows: list[dict[str, Any]], current: Mapping[str, Any], seconds: int, tolerance: int
) -> float | None:
    current_time = parse_utc(current["source_timestamp_utc"], "source_timestamp_utc")
    target = current_time - dt.timedelta(seconds=seconds)
    candidates = []
    for row in rows:
        if (
            row["cohort_id"] != current["cohort_id"]
            or row["source_id"] != current["source_id"]
            or row["source_contract_id"] != current["source_contract_id"]
            or row["instrument"] != current["instrument"]
        ):
            continue
        row_time = parse_utc(row["source_timestamp_utc"], "source_timestamp_utc")
        distance = abs((row_time - target).total_seconds())
        if distance <= tolerance:
            candidates.append((distance, row_time, row))
    if not candidates:
        return None
    prior = min(candidates, key=lambda item: (item[0], item[1]))[2]
    return round((float(current["rate_pct"]) - float(prior["rate_pct"])) * 100.0, 12)


def replay_as_of(
    database: sqlite3.Connection,
    cutoff: dt.datetime,
    *,
    tolerance_sec: int,
    active_identities: set[tuple[str, str, str, str]],
) -> list[dict[str, Any]]:
    rows = [row for row in _latest_versions_as_of(database, cutoff, active_identities)
            if bool(row["causal_intraday_eligible"])]
    current_by_instrument: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        key = (str(row["cohort_id"]), str(row["instrument"]))
        prior = current_by_instrument.get(key)
        if prior is None or str(row["source_timestamp_utc"]) > str(prior["source_timestamp_utc"]):
            current_by_instrument[key] = row
    output = []
    for current in current_by_instrument.values():
        output.append({
            **current,
            "decision_cutoff_utc": iso(cutoff),
            "change_bps_15m": _window_change(rows, current, 900, tolerance_sec),
            "change_bps_60m": _window_change(rows, current, 3600, tolerance_sec),
            "research_only": True,
            "execution_eligible": False,
            "direction_policy": "abstain",
        })
    return sorted(output, key=lambda row: (row["currency"], row["instrument"]))


def readiness(config: Mapping[str, Any]) -> dict[str, Any]:
    sources = [source_contract(config, source) for source in config.get("sources") or []
               if isinstance(source, Mapping)]
    source_rows = []
    for source in sources:
        secret_name = str(source.get("clock_attestation_hmac_env") or "")
        attestation_ready = bool(
            source.get("clock_contract_id")
            and source.get("allowed_clock_sources")
            and secret_name
            and os.environ.get(secret_name)
        )
        intake_text = str(source.get("raw_intake_root") or "").strip()
        intake_ready = bool(
            intake_text
            and Path(intake_text).is_absolute()
            and Path(intake_text).is_dir()
            and not Path(intake_text).is_symlink()
        )
        source_rows.append({
            "cohort_id": source.get("cohort_id"),
            "source_id": source.get("source_id"),
            "source_contract_id": source.get("source_contract_id"),
            "provider": source.get("provider"),
            "currency": source.get("currency"),
            "instrument": source.get("instrument"),
            "connected": bool(source.get("connected")),
            "ingest_enabled": bool(source.get("ingest_enabled")),
            "causal_intraday_eligible": bool(source.get("causal_intraday_eligible")),
            "clock_attestation_ready": attestation_ready,
            "raw_archive_required": bool(source.get("raw_archive_required")),
            "raw_intake_ready": intake_ready,
            "access_state": source.get("access_state"),
            "purpose": source.get("purpose"),
        })
    return {
        "configured_sources": len(sources),
        "connected_sources": sum(bool(source.get("connected")) for source in sources),
        "ingest_enabled_sources": sum(bool(source.get("ingest_enabled")) for source in sources),
        "causal_intraday_connected_sources": sum(
            bool(source.get("connected") and source.get("ingest_enabled")
                 and source.get("causal_intraday_eligible")
                 and source.get("raw_archive_required")
                 and source.get("raw_intake_root")
                 and Path(str(source.get("raw_intake_root"))).is_absolute()
                 and Path(str(source.get("raw_intake_root"))).is_dir()
                 and not Path(str(source.get("raw_intake_root"))).is_symlink()
                 and source.get("clock_contract_id")
                 and source.get("allowed_clock_sources")
                 and os.environ.get(str(source.get("clock_attestation_hmac_env") or "")))
            for source in sources
        ),
        "sources": source_rows,
        "official_release_transports": config.get("official_release_transports") or [],
        "blockers": config.get("blockers") or [],
    }


def run_once(
    *,
    config_path: Path = CONFIG,
    database_path: Path = DB,
    output_path: Path = OUTPUT,
    report_path: Path = REPORT,
    archive_root: Path = ARCHIVE,
    import_path: Path | None = None,
    observed: dt.datetime | None = None,
    decision_cutoff: dt.datetime | None = None,
) -> dict[str, Any]:
    config = read_json(config_path)
    now = (observed or dt.datetime.now(UTC)).astimezone(UTC)
    cutoff = (decision_cutoff or now).astimezone(UTC)
    contracts = {
        str(source.get("source_id") or ""): source_contract(config, source)
        for source in config.get("sources") or [] if isinstance(source, Mapping)
    }
    active_identities = {
        (
            str(contract["cohort_id"]), str(contract.get("source_id") or ""),
            str(contract.get("source_contract_id") or ""),
            str(contract.get("instrument") or ""),
        )
        for contract in contracts.values()
    }
    database = connect(database_path)
    imported = 0
    rejected: list[str] = []
    for index, raw in enumerate(load_jsonl(import_path), 1):
        source_id = str(raw.get("source_id") or "")
        contract = contracts.get(source_id)
        if contract is None:
            rejected.append(f"row_{index}:unknown_source")
            continue
        if not bool(contract.get("connected") and contract.get("ingest_enabled")):
            rejected.append(f"row_{index}:source_not_connected")
            continue
        try:
            normalized = normalize_observation(raw, contract, archive_root=archive_root)
            if parse_utc(normalized["observed_utc"], "observed_utc") > now:
                raise ContractError("observation clock is later than collection cycle")
            imported += int(append_observation(database, normalized))
        except ContractError as exc:
            rejected.append(f"row_{index}:{exc}")
    database.commit()
    observations = replay_as_of(
        database, cutoff, tolerance_sec=int(config.get("window_tolerance_sec") or 90),
        active_identities=active_identities,
    )
    cycle_id = "rate_repricing_cycle_" + stable_hash(
        (iso(now), iso(cutoff), imported, rejected)
    )[:28]
    database.execute(
        "INSERT OR IGNORE INTO collection_cycles VALUES (?,?,?,?,?,?)",
        (cycle_id, iso(now), iso(cutoff), imported, len(rejected), canonical_json(rejected)),
    )
    database.commit()
    integrity = str(database.execute("PRAGMA quick_check").fetchone()[0])
    stored_identities = database.execute(
        "SELECT cohort_id,source_id,source_contract_id,instrument FROM rate_observations"
    ).fetchall()
    all_total = len(stored_identities)
    active_total = sum(
        tuple(str(value) for value in identity) in active_identities
        for identity in stored_identities
    )
    database.close()
    source_readiness = readiness(config)
    payload = {
        "schema_version": 2,
        "generated_utc": iso(now),
        "decision_cutoff_utc": iso(cutoff),
        "contract_id": config.get("contract_id"),
        "state": config.get("state"),
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_promote": False,
        "direction_policy": "abstain",
        "source_readiness": source_readiness,
        "cycle": {"imported_rows": imported, "rejected_rows": len(rejected),
                  "rejection_reasons": rejected},
        "replay": {"observation_count": len(observations), "observations": observations},
        "totals": {
            "active_contract_immutable_observations": active_total,
            "all_cohort_immutable_observations": all_total,
            "excluded_nonactive_contract_observations": all_total - active_total,
        },
        "database_integrity": integrity,
        "supported_execution_decision": "no_trade",
    }
    atomic_json(output_path, payload)
    lines = [
        "# Rates / Policy Repricing Shadow Readiness", "",
        f"Generated: `{payload['generated_utc']}`", "",
        "Research only. Missing rate observations remain unavailable; they are never zero-filled.", "",
        f"- Connected sources: **{source_readiness['connected_sources']}**",
        f"- Causal intraday sources: **{source_readiness['causal_intraday_connected_sources']}**",
        f"- Active-contract immutable observations: **{active_total}**",
        f"- Excluded non-active-contract observations: **{all_total - active_total}**",
        f"- Replay observations at cutoff: **{len(observations)}**",
        f"- SQLite integrity: **{integrity}**", "", "## Blockers", "",
    ]
    lines.extend(f"- {blocker}" for blocker in source_readiness["blockers"])
    lines.extend(["", "Supported execution decision: **no_trade**", ""])
    atomic_text(report_path, "\n".join(lines))
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=CONFIG)
    parser.add_argument("--database", type=Path, default=DB)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    parser.add_argument("--report", type=Path, default=REPORT)
    parser.add_argument("--archive", type=Path, default=ARCHIVE)
    parser.add_argument("--import-jsonl", type=Path)
    parser.add_argument("--decision-cutoff-utc")
    args = parser.parse_args()
    cutoff = parse_utc(args.decision_cutoff_utc, "decision_cutoff_utc") if args.decision_cutoff_utc else None
    result = run_once(
        config_path=args.config, database_path=args.database, output_path=args.output,
        report_path=args.report, archive_root=args.archive,
        import_path=args.import_jsonl, decision_cutoff=cutoff,
    )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
