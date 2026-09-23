#!/usr/bin/env python3
"""Prospectively map newly collected news into source governance quickly.

The complete source-governance reconciliation intentionally performs expensive
identity, reliability, source-card and historical structured-row checks.  It is
the audit backstop, not an intraminute transport. This worker reads only
new committed input rows after its durable sequence checkpoint and appends
their already-frozen semantic representation to the same
governed source-event registry. Later revisions remain the responsibility of
the full reconciliation so poll refreshes cannot masquerade as new events.

V3 separately attests an independent read of committed mappings. That clock
does not claim that its later attestation was already visible: a consumer must
record its own first observation before using the mapping as prospective proof.
The worker is research-only
and has no account, authorization, promotion or broker surface.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Mapping

import oanda_source_governance as governance


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
STATE = DATA / "state"

DEFAULT_NEWS = governance.DEFAULT_NEWS
DEFAULT_DATABASE = governance.DEFAULT_DATABASE
DEFAULT_STATE = STATE / "source_governance_news_fast_lane_v3.json"

PRIOR_CONTRACT_ID = "news_source_governance_fast_lane_v1_prospective_20260901"
PRIOR_COHORT_ID = "news_source_governance_fast_lane_v1_20260901T125500Z"
PRIOR_ACTIVATED_UTC = dt.datetime(2026, 9, 1, 12, 55, tzinfo=dt.timezone.utc)
PRIOR_RETIREMENT_REASON = (
    "permanently_invalid_last_seen_refresh_rows_crossed_activation_boundary"
)
LEGACY_CONTRACT_ID = "news_source_governance_fast_lane_v2_first_seen_prospective_20260901"
LEGACY_COHORT_ID = "news_source_governance_fast_lane_v2_20260901T131500Z"
CONTRACT_ID = "news_source_governance_fast_lane_v3_committed_visibility_20260905"
COHORT_ID = "news_source_governance_fast_lane_v3_20260905"
ACTIVATED_UTC = dt.datetime(2026, 9, 5, tzinfo=dt.timezone.utc)
MAXIMUM_INPUT_ROWS = 1000


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def parse_time(value: Any) -> dt.datetime | None:
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    try:
        temporary.write_text(
            json.dumps(value, indent=2, sort_keys=True), encoding="utf-8"
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode(
            "utf-8"
        )
    ).hexdigest()


def _ensure_legacy_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS news_fast_lane_import_receipts (
            receipt_id TEXT PRIMARY KEY,
            source_event_id TEXT NOT NULL,
            source_id TEXT NOT NULL,
            provider_event_id TEXT NOT NULL,
            raw_payload_sha256 TEXT NOT NULL,
            input_first_seen_utc TEXT NOT NULL,
            input_last_seen_utc TEXT NOT NULL,
            governance_available_utc TEXT NOT NULL,
            observed_existing INTEGER NOT NULL,
            adapter_contract_id TEXT NOT NULL,
            adapter_cohort_id TEXT NOT NULL,
            adapter_activated_utc TEXT NOT NULL,
            research_only INTEGER NOT NULL,
            execution_eligible INTEGER NOT NULL,
            can_authorize INTEGER NOT NULL,
            UNIQUE(source_event_id,adapter_contract_id)
        );
        CREATE INDEX IF NOT EXISTS ix_news_fast_lane_available
            ON news_fast_lane_import_receipts(
                governance_available_utc,source_event_id
            );
        CREATE TRIGGER IF NOT EXISTS news_fast_lane_imports_no_update
        BEFORE UPDATE ON news_fast_lane_import_receipts BEGIN
            SELECT RAISE(ABORT,'news fast-lane receipts are immutable');
        END;
        CREATE TRIGGER IF NOT EXISTS news_fast_lane_imports_no_delete
        BEFORE DELETE ON news_fast_lane_import_receipts BEGIN
            SELECT RAISE(ABORT,'news fast-lane receipts are immutable');
        END;
        CREATE VIEW IF NOT EXISTS source_events_fast_mapped_v1 AS
            SELECT event.*,
                   receipt.governance_available_utc,
                   CASE
                     WHEN event.effective_from_utc >= receipt.governance_available_utc
                     THEN event.effective_from_utc
                     ELSE receipt.governance_available_utc
                   END AS operational_effective_from_utc,
                   receipt.observed_existing AS fast_lane_observed_existing,
                   receipt.adapter_contract_id AS fast_lane_adapter_contract_id,
                   receipt.adapter_cohort_id AS fast_lane_adapter_cohort_id
            FROM source_events_causal_v1 AS event
            JOIN news_fast_lane_import_receipts AS receipt
              ON receipt.source_event_id=event.source_event_id;
        """
    )
    connection.execute(
        f"""CREATE VIEW IF NOT EXISTS source_events_fast_mapped_v2 AS
            SELECT event.*,
                   receipt.governance_available_utc,
                   CASE
                     WHEN event.effective_from_utc >= receipt.governance_available_utc
                     THEN event.effective_from_utc
                     ELSE receipt.governance_available_utc
                   END AS operational_effective_from_utc,
                   receipt.observed_existing AS fast_lane_observed_existing,
                   receipt.adapter_contract_id AS fast_lane_adapter_contract_id,
                   receipt.adapter_cohort_id AS fast_lane_adapter_cohort_id
            FROM source_events_causal_v1 AS event
            JOIN news_fast_lane_import_receipts AS receipt
              ON receipt.source_event_id=event.source_event_id
            WHERE receipt.adapter_contract_id='{LEGACY_CONTRACT_ID}'"""
    )
    connection.commit()


def latest_contracts(connection: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    rows = connection.execute(
        """SELECT source_id,contract_json
           FROM source_contracts
           ORDER BY created_utc,rowid"""
    ).fetchall()
    for source_id, contract_json in rows:
        try:
            contract = json.loads(str(contract_json or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if isinstance(contract, dict) and contract.get("source_contract_id"):
            output[str(source_id)] = contract
    return output


def resolve_source_event_id(
    connection: sqlite3.Connection, event: Mapping[str, Any]
) -> str | None:
    row = connection.execute(
        """SELECT event.source_event_id
           FROM source_events AS event
           WHERE event.source_id=? AND event.provider_event_id=?
             AND event.raw_payload_sha256=?
             AND NOT EXISTS (
                 SELECT 1 FROM source_event_quarantines AS quarantine
                 WHERE quarantine.source_event_id=event.source_event_id
             )
           ORDER BY event.event_version DESC,event.rowid DESC LIMIT 1""",
        (
            str(event.get("source_id") or ""),
            str(event.get("provider_event_id") or ""),
            str(event.get("raw_payload_sha256") or ""),
        ),
    ).fetchone()
    return str(row[0]) if row else None


def ensure_schema(connection: sqlite3.Connection) -> None:
    """Add V3 objects without rewriting any historical receipt or view."""
    _ensure_legacy_schema(connection)
    connection.executescript("""
        CREATE TABLE IF NOT EXISTS news_fast_lane_batches_v3 (
            batch_seq INTEGER PRIMARY KEY AUTOINCREMENT,
            contract_id TEXT NOT NULL,cohort_id TEXT NOT NULL,
            input_identity TEXT NOT NULL,previous_rowid INTEGER NOT NULL,
            input_rowid INTEGER NOT NULL,input_anchor_event_id TEXT,
            scan_started_utc TEXT NOT NULL,statistics_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS news_fast_lane_mappings_v3 (
            source_event_id TEXT PRIMARY KEY,batch_seq INTEGER NOT NULL,
            source_id TEXT NOT NULL,provider_event_id TEXT NOT NULL,
            raw_payload_sha256 TEXT NOT NULL,input_rowid INTEGER NOT NULL,
            input_first_seen_utc TEXT NOT NULL,input_last_seen_utc TEXT NOT NULL,
            observed_existing INTEGER NOT NULL,research_only INTEGER NOT NULL,
            execution_eligible INTEGER NOT NULL,can_authorize INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS ix_news_fast_lane_mapping_batch_v3
            ON news_fast_lane_mappings_v3(batch_seq,source_event_id);
        CREATE TABLE IF NOT EXISTS news_fast_lane_visibility_v3 (
            batch_seq INTEGER PRIMARY KEY,mapping_visible_utc TEXT NOT NULL,
            availability_basis TEXT NOT NULL,consumer_first_observation_required INTEGER NOT NULL
        );
        CREATE VIEW IF NOT EXISTS source_events_fast_mapped_v3 AS
            SELECT event.*,mapping.batch_seq AS fast_lane_batch_seq,
                visibility.mapping_visible_utc,
                visibility.availability_basis,
                visibility.consumer_first_observation_required,
                NULL AS operational_effective_from_utc,
                mapping.observed_existing AS fast_lane_observed_existing,
                batch.contract_id AS fast_lane_adapter_contract_id,
                batch.cohort_id AS fast_lane_adapter_cohort_id
            FROM source_events_causal_v1 AS event
            JOIN news_fast_lane_mappings_v3 AS mapping USING(source_event_id)
            JOIN news_fast_lane_batches_v3 AS batch USING(batch_seq)
            JOIN news_fast_lane_visibility_v3 AS visibility USING(batch_seq);
    """)
    for table in ("news_fast_lane_batches_v3", "news_fast_lane_mappings_v3", "news_fast_lane_visibility_v3"):
        for action in ("update", "delete"):
            connection.execute(
                f"CREATE TRIGGER IF NOT EXISTS {table}_no_{action} "
                f"BEFORE {action.upper()} ON {table} BEGIN "
                "SELECT RAISE(ABORT,'news fast-lane V3 records are immutable'); END;"
            )
    connection.commit()


def progress_path(state_path: Path) -> Path:
    return state_path.with_name(state_path.stem + ".progress.json")


def base_state(*, generated_utc: str, status: str) -> dict[str, Any]:
    return {
        "schema_version": 3,"contract_id": CONTRACT_ID,"cohort_id": COHORT_ID,
        "activated_utc": ACTIVATED_UTC.isoformat(),"generated_utc": generated_utc,
        "status": status,"research_only": True,"execution_eligible": False,
        "can_place_orders": False,"can_promote": False,"can_authorize": False,
        "real_money_routing": False,"supported_decision": "no_trade",
        "scan_clock": "committed_article_rowid_with_identity_anchor",
        "revision_handling": "deferred_to_full_source_governance_reconciliation",
        "operational_view": "source_events_fast_mapped_v3",
        "availability_basis": "post_commit_independent_mapping_read",
        "consumer_first_observation_required": True,
        "retained_invalid_prior_contract_id": PRIOR_CONTRACT_ID,
        "retained_invalid_prior_reason": PRIOR_RETIREMENT_REASON,
        "retained_legacy_contract_id": LEGACY_CONTRACT_ID,
        "legacy_clock_historical_impact": "not_established",
    }


class InputCheckpointError(sqlite3.DatabaseError):
    """A replaced/pruned input needs explicit reconciliation; never rewind."""


def _latest_batch(connection: sqlite3.Connection) -> dict[str, Any]:
    row = connection.execute(
        "SELECT batch_seq,input_identity,input_rowid,input_anchor_event_id "
        "FROM news_fast_lane_batches_v3 ORDER BY batch_seq DESC LIMIT 1"
    ).fetchone()
    return dict(zip(("batch_seq","input_identity","input_rowid","input_anchor_event_id"),row)) if row else {}


def _read_input_batch(news_database: Path, checkpoint: Mapping[str, Any], observed: dt.datetime) -> dict[str, Any]:
    """Anchor, committed high-watermark and rows come from one WAL snapshot.

    Articles have a prunable TEXT primary key. Rowid alone is not an immutable
    sequence, so loss/replacement of the durable anchor fails closed.
    """
    info = news_database.stat()
    identity = stable_hash((str(news_database.resolve()),info.st_dev,info.st_ino))
    if checkpoint and checkpoint.get("input_identity") != identity:
        raise InputCheckpointError("input database identity changed; reconciliation required")
    cursor = int(checkpoint.get("input_rowid") or 0)
    connection = sqlite3.connect(f"file:{news_database.resolve().as_posix()}?mode=ro",uri=True,timeout=5.0)
    try:
        connection.execute("BEGIN")
        if cursor:
            anchor = connection.execute("SELECT event_id FROM articles WHERE rowid=?",(cursor,)).fetchone()
            if not anchor or str(anchor[0]) != checkpoint.get("input_anchor_event_id"):
                raise InputCheckpointError("input sequence anchor changed; reconciliation required")
        high = int(connection.execute("SELECT COALESCE(MAX(rowid),0) FROM articles").fetchone()[0])
        rows = list(governance.article_events(
            news_database,connection=connection,after_rowid=cursor,maximum_rowid=high,
            limit=MAXIMUM_INPUT_ROWS,include_verified_structured_recheck=False,
        ))
        # Never advance past a committed row whose first-seen clock is still
        # in the future. It remains at the head of the next bounded batch.
        prefix = []
        for row in rows:
            seen = parse_time(row.get("first_seen_utc"))
            if seen is not None and seen > observed:
                break
            prefix.append(row)
        last = prefix[-1] if prefix else None
        return {"rows":prefix,"input_identity":identity,"committed_high_watermark":high,
            "input_rowid":int(last["input_rowid"]) if last else cursor,
            "input_anchor_event_id":str(last["event_id"]) if last else checkpoint.get("input_anchor_event_id"),
            "future_row_deferred":len(prefix)<len(rows)}
    finally:
        connection.close()


def _attest_pending(connection: sqlite3.Connection, database_path: Path, now: dt.datetime | None) -> int:
    """Attest only independently readable mappings, after their first commit.

    This does NOT attest that the subsequent visibility record/view existed
    at mapping_visible_utc. Consumer observation is a separate required gate.
    """
    pending = connection.execute(
        "SELECT batch.batch_seq FROM news_fast_lane_batches_v3 AS batch "
        "LEFT JOIN news_fast_lane_visibility_v3 AS visibility USING(batch_seq) "
        "WHERE visibility.batch_seq IS NULL ORDER BY batch.batch_seq"
    ).fetchall()
    attested = 0
    for (sequence,) in pending:
        reader = sqlite3.connect(f"file:{database_path.resolve().as_posix()}?mode=ro",uri=True,timeout=5.0)
        try:
            reader.execute("BEGIN")
            batch = reader.execute("SELECT scan_started_utc FROM news_fast_lane_batches_v3 WHERE batch_seq=?",(sequence,)).fetchone()
            mismatches = reader.execute(
                "SELECT COUNT(*) FROM news_fast_lane_mappings_v3 AS mapping "
                "LEFT JOIN source_events AS event USING(source_event_id) WHERE batch_seq=? AND "
                "(event.source_event_id IS NULL OR event.raw_payload_sha256<>mapping.raw_payload_sha256 "
                "OR event.source_id<>mapping.source_id OR event.provider_event_id<>mapping.provider_event_id)",
                (sequence,),
            ).fetchone()[0]
            count = int(reader.execute("SELECT COUNT(*) FROM news_fast_lane_mappings_v3 WHERE batch_seq=?",(sequence,)).fetchone()[0])
            if batch is None or mismatches:
                raise sqlite3.IntegrityError("committed mapping visibility check failed")
            visible = (now or dt.datetime.now(dt.timezone.utc)).astimezone(dt.timezone.utc)
            if visible < parse_time(batch[0]):
                raise sqlite3.IntegrityError("mapping observation clock moved backwards")
        finally:
            reader.close()
        inserted = connection.execute(
            "INSERT OR IGNORE INTO news_fast_lane_visibility_v3 VALUES (?,?,?,1)",
            (sequence,visible.isoformat(),"post_commit_independent_mapping_read"),
        ).rowcount
        connection.commit()
        attested += count if inserted else 0
    return attested


def observe_mapping(database_path: Path, source_event_id: str, *, observer_id: str,
                    clock: Any = None) -> dict[str, Any] | None:
    """Return a consumer-owned first-read record; caller preserves it with inputs.

    Read on the consumer connection first, then sample its clock. The mapping
    clock alone never supplies prospective availability. Do not backfill this
    acknowledgement into a decision already made before the read.
    """
    if not observer_id.strip():
        raise ValueError("consumer observer_id is required")
    connection = sqlite3.connect(f"file:{database_path.resolve().as_posix()}?mode=ro",uri=True,timeout=5.0)
    connection.row_factory = sqlite3.Row
    try:
        row = connection.execute(
            "SELECT source_event_id,raw_payload_sha256,effective_from_utc,mapping_visible_utc,"
            "fast_lane_batch_seq FROM source_events_fast_mapped_v3 WHERE source_event_id=?",
            (source_event_id,),
        ).fetchone()
        if row is None:
            return None
        observed = (clock or (lambda: dt.datetime.now(dt.timezone.utc)))().astimezone(dt.timezone.utc)
        clocks = [parse_time(row["effective_from_utc"]),parse_time(row["mapping_visible_utc"])]
        if any(value is None for value in clocks) or observed < max(clocks):
            raise ValueError("consumer observation clock precedes mapping knowledge")
        return {"contract_id":CONTRACT_ID,"source_event_id":row["source_event_id"],
            "raw_payload_sha256":row["raw_payload_sha256"],"batch_seq":row["fast_lane_batch_seq"],
            "observer_id":observer_id,"consumer_observed_utc":observed.isoformat(),
            "mapping_visible_utc":row["mapping_visible_utc"],
            "prospective_available_utc":max(observed,*clocks).isoformat(),
            "availability_basis":"consumer_read_of_attested_mapping",
            "research_only":True,"execution_eligible":False,"can_authorize":False}
    finally:
        connection.close()


def acknowledgement_usable_at(acknowledgement: Mapping[str, Any] | None, decision_utc: dt.datetime) -> bool:
    """A saved consumer observation is necessary, never sufficient to trade."""
    if not acknowledgement or acknowledgement.get("contract_id") != CONTRACT_ID:
        return False
    available = parse_time(acknowledgement.get("prospective_available_utc"))
    observed = parse_time(acknowledgement.get("consumer_observed_utc"))
    visible = parse_time(acknowledgement.get("mapping_visible_utc"))
    return bool(available and observed and visible and acknowledgement.get("observer_id")
        and acknowledgement.get("source_event_id") and acknowledgement.get("raw_payload_sha256")
        and acknowledgement.get("availability_basis")=="consumer_read_of_attested_mapping"
        and available >= observed >= visible and available <= decision_utc.astimezone(dt.timezone.utc)
        and acknowledgement.get("research_only") is True
        and acknowledgement.get("execution_eligible") is False and acknowledgement.get("can_authorize") is False)


def run(*,news_database: Path = DEFAULT_NEWS,database_path: Path = DEFAULT_DATABASE,
        state_path: Path = DEFAULT_STATE,now: dt.datetime | None = None) -> dict[str, Any]:
    observed = (now or dt.datetime.now(dt.timezone.utc)).astimezone(dt.timezone.utc)
    generated = observed.isoformat()
    if observed < ACTIVATED_UTC:
        payload = {**base_state(generated_utc=generated,status="waiting_for_activation"),
            "input_row_count":0,"new_receipt_count":0,"scan_cursor_rowid":0}
        atomic_json(state_path,payload)
        return payload
    connection = None
    checkpoint: dict[str, Any] = {}
    batch: dict[str, Any] = {}
    phase = "registry"
    try:
        connection = governance.connect_registry(database_path)
        connection.execute("PRAGMA busy_timeout=5000")
        ensure_schema(connection)
        recovered_receipts = _attest_pending(connection,database_path,now)
        checkpoint = _latest_batch(connection)
        phase = "input"
        batch = _read_input_batch(news_database,checkpoint,observed)
        rows = batch["rows"]
        atomic_json(progress_path(state_path),{**base_state(generated_utc=generated,status="building"),
            "scan_cursor_rowid":int(checkpoint.get("input_rowid") or 0),
            "committed_batch_seq":int(checkpoint.get("batch_seq") or 0),"input_row_count":len(rows)})
        phase = "registry"
        statistics = dict.fromkeys(("input_row_count","inserted_source_event_count","inserted_story_assignment_count",
            "new_receipt_count","observed_existing_receipt_count","rejected_untrusted_count",
            "rejected_unknown_source_count","rejected_preactivation_count","unresolved_source_event_count"),0)
        statistics["input_row_count"] = len(rows)
        if rows or not checkpoint:
            connection.execute("BEGIN IMMEDIATE")
            if _latest_batch(connection) != checkpoint:
                raise sqlite3.OperationalError("another adapter advanced the committed checkpoint; retry")
            contracts = latest_contracts(connection)
            candidates = []
            for row in rows:
                first_seen = parse_time(row.get("first_seen_utc"))
                if first_seen is None or first_seen < ACTIVATED_UTC:
                    statistics["rejected_preactivation_count"] += 1
                    continue
                contract = contracts.get(str(row.get("source_id") or ""))
                if contract is None:
                    statistics["rejected_unknown_source_count"] += 1
                    continue
                event = governance._event_payload(row,contract,observed_utc=generated)
                if event is None:
                    statistics["rejected_untrusted_count"] += 1
                    continue
                inserted = bool(governance.insert_source_event(connection,event))
                statistics["inserted_source_event_count"] += int(inserted)
                event_id = resolve_source_event_id(connection,event)
                if not event_id:
                    # Do not commit a cursor across an unresolved source row.
                    raise sqlite3.IntegrityError("source event resolution failed; input checkpoint retained")
                event["source_event_id"] = event_id
                statistics["inserted_story_assignment_count"] += int(governance.insert_story_assignment(connection,event,assigned_utc=generated))
                candidates.append((row,event,not inserted))
            sequence = connection.execute(
                "INSERT INTO news_fast_lane_batches_v3 (contract_id,cohort_id,input_identity,previous_rowid,"
                "input_rowid,input_anchor_event_id,scan_started_utc,statistics_json) VALUES (?,?,?,?,?,?,?,?)",
                (CONTRACT_ID,COHORT_ID,batch["input_identity"],int(checkpoint.get("input_rowid") or 0),
                 batch["input_rowid"],batch["input_anchor_event_id"],generated,json.dumps(
                     {key:value for key,value in statistics.items() if key not in
                      {"new_receipt_count","observed_existing_receipt_count"}},sort_keys=True)),
            ).lastrowid
            for row,event,existing in candidates:
                inserted = connection.execute(
                    "INSERT OR IGNORE INTO news_fast_lane_mappings_v3 VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (event["source_event_id"],sequence,event["source_id"],event["provider_event_id"],
                     event["raw_payload_sha256"],row["input_rowid"],row["first_seen_utc"],row["last_seen_utc"],int(existing),1,0,0),
                ).rowcount
                statistics["new_receipt_count"] += inserted
                statistics["observed_existing_receipt_count"] += int(bool(inserted and existing))
            # Checkpoint and mapping data commit together. A crash before the
            # separate visibility attestation leaves recoverable pending data.
            connection.commit()
            recovered_receipts += _attest_pending(connection,database_path,now)
        latest = _latest_batch(connection)
        sequence = int(latest["batch_seq"])
        visibility = connection.execute("SELECT mapping_visible_utc FROM news_fast_lane_visibility_v3 WHERE batch_seq=?",(sequence,)).fetchone()
        if visibility is None:
            raise sqlite3.IntegrityError("latest mapping batch lacks visibility attestation")
        total = int(connection.execute("SELECT COUNT(*) FROM news_fast_lane_mappings_v3 WHERE batch_seq<=?",(sequence,)).fetchone()[0])
        legacy = {contract:int(connection.execute("SELECT COUNT(*) FROM news_fast_lane_import_receipts WHERE adapter_contract_id=?",(contract,)).fetchone()[0]) for contract in (PRIOR_CONTRACT_ID,LEGACY_CONTRACT_ID)}
        completed = generated if now is not None else utc_now()
        payload = {**base_state(generated_utc=completed,status="ok"),**statistics,
            "new_receipt_count":recovered_receipts,"total_receipt_count":total,
            "scan_cursor_rowid":int(checkpoint.get("input_rowid") or 0),
            "next_scan_cursor_rowid":int(latest["input_rowid"]),"committed_batch_seq":sequence,
            "input_identity":latest["input_identity"],"input_anchor_event_id":latest["input_anchor_event_id"],
            "committed_input_high_watermark":batch["committed_high_watermark"],
            "future_row_deferred":batch["future_row_deferred"],"scan_started_utc":generated,
            "mapping_visible_utc":visibility[0],
            "retained_invalid_prior_receipt_count":legacy[PRIOR_CONTRACT_ID],
            "retained_legacy_receipt_count":legacy[LEGACY_CONTRACT_ID],
            "sqlite_integrity":"deferred_to_shared_governance_integrity","transaction_committed":True,
            "database":str(database_path.resolve()),"news_database":str(news_database.resolve())}
        atomic_json(state_path,payload)
        atomic_json(progress_path(state_path),payload)
        return payload
    except (OSError,sqlite3.Error) as exc:
        if connection is not None:
            # A failed second commit may follow a successful mapping/checkpoint
            # commit. Report that authoritative checkpoint, never the older JSON.
            try:
                connection.rollback()
                checkpoint = _latest_batch(connection)
            except sqlite3.Error:
                pass
        status = "input_checkpoint_reconciliation_required" if isinstance(exc,InputCheckpointError) else (
            "input_temporarily_unavailable" if phase=="input" else "retryable_database_error")
        payload = {**base_state(generated_utc=generated if now is not None else utc_now(),status=status),
            "scan_cursor_rowid":int(checkpoint.get("input_rowid") or 0),
            "committed_batch_seq":int(checkpoint.get("batch_seq") or 0),
            "input_row_count":len(batch.get("rows",[])),"error_type":type(exc).__name__,
            "retry_without_cursor_advance":True}
        atomic_json(progress_path(state_path),payload)
        return payload
    finally:
        if connection is not None:
            connection.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--news-database", type=Path, default=DEFAULT_NEWS)
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--interval-sec", type=float, default=0.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    args = parser.parse_args()
    started = time.time()
    while True:
        run(
            news_database=args.news_database,
            database_path=args.database,
            state_path=args.state,
        )
        if args.interval_sec <= 0.0 or (
            args.duration_sec > 0.0 and time.time() - started >= args.duration_sec
        ):
            break
        time.sleep(max(10.0, args.interval_sec))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
