"""Append-only, source-specific official policy-decision fact ledger.

The worker deliberately does one narrow job: bind an exact official-source
parser to a canonical decision-fact schema.  It does not use the generic news
vote/action regexes, infer a currency direction, contact a broker, promote a
hypothesis, or authorize an order.  Incomplete facts are retained only as
bounded collection attempts and always abstain.

The first adapter is the already reviewed SARB MPC exact parser.  Its frozen
schedule was captured before the September and November 2026 decisions.  The
worker polls only after a scheduled decision time, discards source text after
hashing/parsing, and starts with a new prospective cohort.  July 2026 remains
historical context and is never fetched or relabelled as prospective evidence.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence


ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from forex_system.ingestion import sarb_mpc_decision_exact_v2 as sarb  # noqa: E402


CONFIG = ROOT / "config" / "official_policy_decision_fact_v1.json"
STATE = ROOT / "data" / "oanda_training_manager" / "state"
DB = STATE / "official_policy_decision_fact_v1.sqlite"
OUTPUT = STATE / "official_policy_decision_fact_v1.json"
HEARTBEAT = STATE / "official_policy_decision_fact_heartbeat_v1.json"
REPORT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "official_policy_decision_fact"
    / "OFFICIAL_POLICY_DECISION_FACT_CURRENT.md"
)

SCHEMA_VERSION = "official_policy_decision_fact_v1"
CONTRACT_ID = "official_policy_decision_fact_v1_sarb_exact_binding_20260827"
COHORT_ID = "official_policy_decision_fact_v1_sarb_exact_binding_20260827"
UTC = dt.timezone.utc
ALLOWED_ACTIONS = frozenset({"cut", "hold", "hike"})
NO_TRADE_POLICY = {
    "research_only": True,
    "execution_eligible": False,
    "can_authorize": False,
    "can_promote": False,
    "broker_access": False,
    "direction_policy": "abstain",
    "supported_decision": "collect_structured_policy_facts_only",
}


class PolicyDecisionFactError(RuntimeError):
    """Raised when the bounded fact contract cannot be proved."""


def utc_now() -> dt.datetime:
    return dt.datetime.now(UTC)


def iso(value: dt.datetime) -> str:
    return value.astimezone(UTC).isoformat()


def parse_utc(value: Any) -> dt.datetime:
    try:
        parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise PolicyDecisionFactError("invalid_utc_timestamp") from exc
    if parsed.tzinfo is None:
        raise PolicyDecisionFactError("naive_utc_timestamp")
    return parsed.astimezone(UTC)


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def sha256_json(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def finite(value: Any, *, field: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise PolicyDecisionFactError(f"{field}_missing_or_non_numeric") from exc
    if not math.isfinite(number):
        raise PolicyDecisionFactError(f"{field}_nonfinite")
    return number


def read_config(path: Path = CONFIG) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PolicyDecisionFactError("invalid_worker_config") from exc
    if not isinstance(payload, Mapping):
        raise PolicyDecisionFactError("worker_config_not_object")
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise PolicyDecisionFactError("worker_schema_mismatch")
    if payload.get("contract_id") != CONTRACT_ID:
        raise PolicyDecisionFactError("worker_contract_mismatch")
    if payload.get("cohort_id") != COHORT_ID:
        raise PolicyDecisionFactError("worker_cohort_mismatch")
    for key, expected in NO_TRADE_POLICY.items():
        if payload.get(key) != expected:
            raise PolicyDecisionFactError(f"unsafe_worker_policy:{key}")
    fact_policy = payload.get("fact_policy") or {}
    if fact_policy.get("generic_vote_regex_allowed") is not False:
        raise PolicyDecisionFactError("generic_vote_regex_must_be_disabled")
    if fact_policy.get("full_text_retention") is not False:
        raise PolicyDecisionFactError("full_text_retention_must_be_disabled")
    adapters = payload.get("source_adapters") or []
    if len(adapters) != 1 or adapters[0].get("source_id") != sarb.SOURCE_ID:
        raise PolicyDecisionFactError("only_exact_sarb_adapter_is_allowed")
    if adapters[0].get("source_contract_id") != sarb.CONTRACT_ID:
        raise PolicyDecisionFactError("sarb_contract_mismatch")
    return dict(payload)


def validate_source_contract(config: Mapping[str, Any]) -> dict[str, Any]:
    adapter = (config.get("source_adapters") or [])[0]
    contract_path = ROOT / str(adapter.get("source_contract_path") or "")
    manifest = sarb.load_contract_manifest(contract_path)
    if manifest.get("source_id") != sarb.SOURCE_ID:
        raise PolicyDecisionFactError("source_manifest_id_mismatch")
    if manifest.get("source_contract_id") != sarb.CONTRACT_ID:
        raise PolicyDecisionFactError("source_manifest_contract_mismatch")
    if manifest.get("research_only") is not True:
        raise PolicyDecisionFactError("source_manifest_not_research_only")
    if manifest.get("execution_eligible") is not False:
        raise PolicyDecisionFactError("source_manifest_execution_enabled")
    return manifest


def schedule_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in sarb.EXPECTED_SCHEDULE_FACTS:
        event_date = str(item["event_date"])
        event = dt.date.fromisoformat(event_date)
        rows.append(
            {
                "event_id": f"sarb_mpc_decision:{event_date}",
                "currency": "ZAR",
                "authority_id": "sarb",
                "source_id": sarb.SOURCE_ID,
                "source_contract_id": sarb.CONTRACT_ID,
                "source_cohort_id": sarb.COHORT_ID,
                "event_date": event_date,
                "scheduled_utc": iso(parse_utc(item["scheduled_utc"])),
                "evidence_boundary": str(item["evidence_boundary"]),
                "statement_url": (
                    "https://www.resbank.co.za/en/home/publications/"
                    "publication-detail-pages/statements/monetary-policy-statements/"
                    f"{event.year}/{event.strftime('%B').lower()}"
                ),
            }
        )
    return rows


def normalized_fact(parsed: Mapping[str, Any]) -> dict[str, Any]:
    """Convert one exact-parser record into the canonical fact contract."""

    if parsed.get("source_id") != sarb.SOURCE_ID:
        raise PolicyDecisionFactError("unexpected_source_id")
    if parsed.get("source_contract_id") != sarb.CONTRACT_ID:
        raise PolicyDecisionFactError("unexpected_source_contract")
    if parsed.get("currency") != "ZAR":
        raise PolicyDecisionFactError("unexpected_currency")
    action = str(parsed.get("action") or "")
    if action not in ALLOWED_ACTIONS:
        raise PolicyDecisionFactError("unsupported_action")
    current_rate = finite(parsed.get("policy_rate_pct"), field="current_policy_rate")
    try:
        decision_bps = int(parsed.get("decision_basis_points"))
    except (TypeError, ValueError) as exc:
        raise PolicyDecisionFactError("decision_basis_points_missing") from exc
    if decision_bps < 0:
        raise PolicyDecisionFactError("decision_basis_points_negative")
    if action == "hold" and decision_bps != 0:
        raise PolicyDecisionFactError("hold_has_nonzero_delta")
    if action != "hold" and decision_bps <= 0:
        raise PolicyDecisionFactError("directional_action_has_no_delta")
    signed_delta_bps = (
        decision_bps if action == "hike" else -decision_bps if action == "cut" else 0
    )
    prior_rate = current_rate - signed_delta_bps / 100.0
    if not math.isfinite(prior_rate) or prior_rate < 0:
        raise PolicyDecisionFactError("derived_prior_rate_invalid")

    votes = parsed.get("votes")
    if not isinstance(votes, Sequence) or isinstance(votes, (str, bytes)) or not votes:
        raise PolicyDecisionFactError("structured_votes_missing")
    normalized_votes: list[dict[str, Any]] = []
    for raw in votes:
        if not isinstance(raw, Mapping):
            raise PolicyDecisionFactError("structured_vote_not_object")
        try:
            count = int(raw.get("count"))
            basis_points = int(raw.get("basis_points"))
        except (TypeError, ValueError) as exc:
            raise PolicyDecisionFactError("structured_vote_invalid_numeric") from exc
        preference = str(raw.get("preference") or "")
        if count <= 0 or preference not in ALLOWED_ACTIONS or basis_points < 0:
            raise PolicyDecisionFactError("structured_vote_invalid")
        if preference == "hold" and basis_points != 0:
            raise PolicyDecisionFactError("hold_vote_has_nonzero_delta")
        if preference != "hold" and basis_points <= 0:
            raise PolicyDecisionFactError("directional_vote_has_no_delta")
        normalized_votes.append(
            {"count": count, "preference": preference, "basis_points": basis_points}
        )
    vote_total = sum(row["count"] for row in normalized_votes)
    if vote_total != int(parsed.get("vote_total_observed") or -1):
        raise PolicyDecisionFactError("vote_total_mismatch")

    scheduled = parse_utc(parsed.get("scheduled_utc"))
    first_seen = parse_utc(parsed.get("statement_first_seen_utc"))
    causal_known = parse_utc(parsed.get("decision_causal_known_utc"))
    if causal_known != max(scheduled, first_seen):
        raise PolicyDecisionFactError("causal_clock_mismatch")
    if first_seen < scheduled:
        raise PolicyDecisionFactError("pre_schedule_first_seen")

    prospective = parsed.get("prospective_observation") is True
    completeness = (
        "complete_prospective_research_fact"
        if prospective
        else "complete_historical_not_proof_eligible"
    )
    fact_material = {
        "source_id": sarb.SOURCE_ID,
        "source_contract_id": sarb.CONTRACT_ID,
        "source_cohort_id": sarb.COHORT_ID,
        "event_id": str(parsed.get("event_id") or ""),
        "event_date": str(parsed.get("event_date") or ""),
        "currency": "ZAR",
        "authority_id": "sarb",
        "action": action,
        "current_policy_rate_pct": round(current_rate, 10),
        "prior_policy_rate_pct": round(prior_rate, 10),
        "signed_decision_delta_bps": signed_delta_bps,
        "prior_rate_derivation": "current_rate_minus_signed_decision_delta",
        "votes": normalized_votes,
        "vote_total": vote_total,
        "scheduled_utc": iso(scheduled),
        "publisher_published_utc": None,
        "statement_first_seen_utc": iso(first_seen),
        "publication_clock_utc": iso(first_seen),
        "publication_clock_basis": "collector_first_seen_floor",
        "causal_known_utc": iso(causal_known),
        "prospective_observation": prospective,
        "completeness_state": completeness,
        "direction": None,
        "currency_bias": None,
        "consensus": None,
        "surprise": None,
        "research_only": True,
        "execution_eligible": False,
        "can_authorize": False,
        "can_promote": False,
        "supported_decision": "abstain",
        "statement_source_url": str(parsed.get("statement_source_url") or ""),
        "statement_source_sha256": str(parsed.get("statement_source_sha256") or ""),
        "source_decision_facts_sha256": str(parsed.get("decision_facts_sha256") or ""),
        "independent_episode_key": str(parsed.get("independent_episode_key") or ""),
    }
    if not fact_material["event_id"] or not fact_material["statement_source_sha256"]:
        raise PolicyDecisionFactError("source_identity_incomplete")
    fact_material["fact_id"] = "policy_fact_" + sha256_json(fact_material)
    fact_material["fact_contract_id"] = CONTRACT_ID
    fact_material["fact_cohort_id"] = COHORT_ID
    return fact_material


def ensure_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=FULL;
        CREATE TABLE IF NOT EXISTS policy_fact_contracts (
          contract_id TEXT PRIMARY KEY,
          cohort_id TEXT NOT NULL,
          contract_json TEXT NOT NULL,
          created_utc TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS policy_fact_schedule (
          event_id TEXT PRIMARY KEY,
          currency TEXT NOT NULL,
          authority_id TEXT NOT NULL,
          source_id TEXT NOT NULL,
          source_contract_id TEXT NOT NULL,
          source_cohort_id TEXT NOT NULL,
          event_date TEXT NOT NULL,
          scheduled_utc TEXT NOT NULL,
          evidence_boundary TEXT NOT NULL,
          statement_url TEXT NOT NULL,
          fact_contract_id TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS policy_fact_collection_attempts (
          attempt_id TEXT PRIMARY KEY,
          event_id TEXT NOT NULL,
          attempted_utc TEXT NOT NULL,
          statement_url TEXT NOT NULL,
          result_state TEXT NOT NULL,
          http_status INTEGER,
          response_bytes INTEGER,
          response_sha256 TEXT,
          rejection_reason TEXT NOT NULL,
          fact_contract_id TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS official_policy_decision_facts (
          fact_id TEXT PRIMARY KEY,
          event_id TEXT NOT NULL UNIQUE,
          source_id TEXT NOT NULL,
          source_contract_id TEXT NOT NULL,
          source_cohort_id TEXT NOT NULL,
          currency TEXT NOT NULL,
          authority_id TEXT NOT NULL,
          event_date TEXT NOT NULL,
          action TEXT NOT NULL,
          current_policy_rate_pct REAL NOT NULL,
          prior_policy_rate_pct REAL NOT NULL,
          signed_decision_delta_bps INTEGER NOT NULL,
          prior_rate_derivation TEXT NOT NULL,
          votes_json TEXT NOT NULL,
          vote_total INTEGER NOT NULL,
          scheduled_utc TEXT NOT NULL,
          publisher_published_utc TEXT,
          statement_first_seen_utc TEXT NOT NULL,
          publication_clock_utc TEXT NOT NULL,
          publication_clock_basis TEXT NOT NULL,
          causal_known_utc TEXT NOT NULL,
          prospective_observation INTEGER NOT NULL,
          completeness_state TEXT NOT NULL,
          fact_payload_json TEXT NOT NULL,
          inserted_utc TEXT NOT NULL,
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          can_authorize INTEGER NOT NULL CHECK(can_authorize=0),
          can_promote INTEGER NOT NULL CHECK(can_promote=0),
          fact_contract_id TEXT NOT NULL,
          fact_cohort_id TEXT NOT NULL
        );
        """
    )
    for table in (
        "policy_fact_contracts",
        "policy_fact_schedule",
        "policy_fact_collection_attempts",
        "official_policy_decision_facts",
    ):
        connection.executescript(
            f"""
            CREATE TRIGGER IF NOT EXISTS {table}_append_only_update
            BEFORE UPDATE ON {table}
            BEGIN SELECT RAISE(ABORT, 'append_only:{table}:update'); END;
            CREATE TRIGGER IF NOT EXISTS {table}_append_only_delete
            BEFORE DELETE ON {table}
            BEGIN SELECT RAISE(ABORT, 'append_only:{table}:delete'); END;
            """
        )
    connection.commit()


def register_contract_and_schedule(
    connection: sqlite3.Connection,
    config: Mapping[str, Any],
    now: dt.datetime,
) -> None:
    connection.execute(
        "INSERT OR IGNORE INTO policy_fact_contracts VALUES (?,?,?,?)",
        (CONTRACT_ID, COHORT_ID, canonical_json(config), iso(now)),
    )
    for row in schedule_rows():
        connection.execute(
            "INSERT OR IGNORE INTO policy_fact_schedule VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                row["event_id"], row["currency"], row["authority_id"],
                row["source_id"], row["source_contract_id"], row["source_cohort_id"],
                row["event_date"], row["scheduled_utc"], row["evidence_boundary"],
                row["statement_url"], CONTRACT_ID,
            ),
        )
    connection.commit()


def insert_fact(connection: sqlite3.Connection, fact: Mapping[str, Any], now: dt.datetime) -> bool:
    cursor = connection.execute(
        """INSERT OR IGNORE INTO official_policy_decision_facts VALUES
           (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            fact["fact_id"], fact["event_id"], fact["source_id"],
            fact["source_contract_id"], fact["source_cohort_id"], fact["currency"],
            fact["authority_id"], fact["event_date"], fact["action"],
            fact["current_policy_rate_pct"], fact["prior_policy_rate_pct"],
            fact["signed_decision_delta_bps"], fact["prior_rate_derivation"],
            canonical_json(fact["votes"]), fact["vote_total"], fact["scheduled_utc"],
            fact["publisher_published_utc"], fact["statement_first_seen_utc"],
            fact["publication_clock_utc"], fact["publication_clock_basis"],
            fact["causal_known_utc"], int(bool(fact["prospective_observation"])),
            fact["completeness_state"], canonical_json(fact), iso(now), 1, 0, 0, 0,
            CONTRACT_ID, COHORT_ID,
        ),
    )
    connection.commit()
    return bool(cursor.rowcount)


def record_attempt(
    connection: sqlite3.Connection,
    *,
    event_id: str,
    attempted: dt.datetime,
    statement_url: str,
    result_state: str,
    http_status: int | None,
    response: bytes | None,
    rejection_reason: str,
) -> None:
    response_hash = hashlib.sha256(response).hexdigest() if response is not None else ""
    material = {
        "event_id": event_id,
        "attempted_utc": iso(attempted),
        "statement_url": statement_url,
        "result_state": result_state,
        "http_status": http_status,
        "response_bytes": len(response) if response is not None else None,
        "response_sha256": response_hash,
        "rejection_reason": rejection_reason,
        "fact_contract_id": CONTRACT_ID,
    }
    attempt_id = "policy_attempt_" + sha256_json(material)
    connection.execute(
        "INSERT OR IGNORE INTO policy_fact_collection_attempts VALUES (?,?,?,?,?,?,?,?,?,?)",
        (
            attempt_id, event_id, material["attempted_utc"], statement_url,
            result_state, http_status, material["response_bytes"], response_hash,
            rejection_reason, CONTRACT_ID,
        ),
    )
    connection.commit()


def fetch_bounded(url: str, *, timeout: int, maximum_bytes: int, user_agent: str) -> tuple[int, bytes]:
    request = urllib.request.Request(
        url,
        headers={"User-Agent": user_agent, "Accept": "text/html,application/xhtml+xml"},
        method="GET",
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310 - fixed official URL
        status = int(getattr(response, "status", 200))
        payload = response.read(maximum_bytes + 1)
    if len(payload) > maximum_bytes:
        raise PolicyDecisionFactError("source_response_too_large")
    return status, payload


def due_events(connection: sqlite3.Connection, now: dt.datetime, lag_seconds: int) -> list[dict[str, Any]]:
    existing = {
        str(row[0])
        for row in connection.execute("SELECT event_id FROM official_policy_decision_facts")
    }
    output: list[dict[str, Any]] = []
    for row in schedule_rows():
        scheduled = parse_utc(row["scheduled_utc"])
        if row["event_id"] in existing:
            continue
        if "prospective" not in row["evidence_boundary"]:
            continue
        if scheduled <= now <= scheduled + dt.timedelta(seconds=lag_seconds):
            output.append(row)
    return output


def atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(text, encoding="utf-8")
    temporary.replace(path)


def snapshot(connection: sqlite3.Connection, now: dt.datetime, cycle: Mapping[str, Any]) -> dict[str, Any]:
    counts = {
        "scheduled_events": int(connection.execute("SELECT COUNT(*) FROM policy_fact_schedule").fetchone()[0]),
        "collection_attempts": int(connection.execute("SELECT COUNT(*) FROM policy_fact_collection_attempts").fetchone()[0]),
        "facts": int(connection.execute("SELECT COUNT(*) FROM official_policy_decision_facts").fetchone()[0]),
        "prospective_complete_facts": int(
            connection.execute(
                "SELECT COUNT(*) FROM official_policy_decision_facts WHERE prospective_observation=1 AND completeness_state='complete_prospective_research_fact'"
            ).fetchone()[0]
        ),
    }
    upcoming = [
        row for row in schedule_rows() if parse_utc(row["scheduled_utc"]) > now
    ]
    integrity = str(connection.execute("PRAGMA quick_check").fetchone()[0])
    return {
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "cohort_id": COHORT_ID,
        "generated_utc": iso(now),
        "source_contract_validated": True,
        "enabled_source_adapters": [sarb.SOURCE_ID],
        "configured_currency_count": 1,
        "configured_currencies": ["ZAR"],
        "counts": counts,
        "cycle": dict(cycle),
        "next_scheduled_event": upcoming[0] if upcoming else None,
        "sqlite_integrity": integrity,
        "policy": dict(NO_TRADE_POLICY),
        "generic_vote_regex_used": False,
        "incomplete_fact_action": "abstain_and_record_bounded_attempt_only",
    }


def render_report(payload: Mapping[str, Any]) -> str:
    counts = payload["counts"]
    next_event = payload.get("next_scheduled_event") or {}
    return "\n".join(
        (
            "# Official Policy Decision Fact V1",
            "",
            f"Generated: `{payload['generated_utc']}`",
            "",
            "Research-only append-only fact ledger. No broker, execution, promotion, or authorization surface.",
            "",
            f"- Exact source adapters: **{', '.join(payload['enabled_source_adapters'])}**",
            f"- Scheduled events: **{counts['scheduled_events']}**",
            f"- Collection attempts: **{counts['collection_attempts']}**",
            f"- Canonical decision facts: **{counts['facts']}**",
            f"- Complete prospective facts: **{counts['prospective_complete_facts']}**",
            f"- Next scheduled event: **{next_event.get('event_date', 'none')}** at `{next_event.get('scheduled_utc', '')}`",
            "- Incomplete or unverified fact: **abstain**",
            "- Generic vote regex: **disabled**",
            f"- SQLite integrity: **{payload['sqlite_integrity']}**",
            "",
        )
    )


def run_cycle(
    *,
    now: dt.datetime | None = None,
    database: Path = DB,
    output: Path = OUTPUT,
    report: Path = REPORT,
    fetcher: Callable[..., tuple[int, bytes]] = fetch_bounded,
) -> dict[str, Any]:
    observed = (now or utc_now()).astimezone(UTC)
    config = read_config()
    validate_source_contract(config)
    collection = config.get("collection") or {}
    timeout = int(collection.get("request_timeout_seconds") or 20)
    maximum_bytes = int(collection.get("maximum_response_bytes") or 2_097_152)
    lag_seconds = int(collection.get("poll_after_scheduled_seconds") or 259_200)
    user_agent = str(collection.get("user_agent") or "forex-policy-fact-research/1.0")
    database.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(database, timeout=30.0)
    connection.execute("PRAGMA busy_timeout=30000")
    inserted = attempted = rejected = 0
    errors: list[str] = []
    try:
        ensure_schema(connection)
        register_contract_and_schedule(connection, config, observed)
        due = due_events(connection, observed, lag_seconds)
        for event in due:
            attempted += 1
            response: bytes | None = None
            status: int | None = None
            attempt_time = utc_now() if now is None else observed
            try:
                status, response = fetcher(
                    event["statement_url"],
                    timeout=timeout,
                    maximum_bytes=maximum_bytes,
                    user_agent=user_agent,
                )
                if status != 200:
                    raise PolicyDecisionFactError(f"http_status_{status}")
                # Knowledge begins no earlier than receipt of the complete,
                # bounded response. Tests can inject a frozen clock; live
                # collection always advances the clock after the read.
                attempt_time = utc_now() if now is None else observed
                parsed = sarb.parse_official_decision(
                    response,
                    source_url=event["statement_url"],
                    statement_first_seen_utc=attempt_time,
                )
                fact = normalized_fact(parsed)
                inserted += int(insert_fact(connection, fact, observed))
                record_attempt(
                    connection,
                    event_id=event["event_id"],
                    attempted=attempt_time,
                    statement_url=event["statement_url"],
                    result_state="complete_fact_inserted",
                    http_status=status,
                    response=response,
                    rejection_reason="",
                )
            except (PolicyDecisionFactError, sarb.SarbMpcDecisionSourceError, urllib.error.URLError, TimeoutError) as exc:
                rejected += 1
                reason = f"{type(exc).__name__}:{exc}"
                errors.append(reason)
                record_attempt(
                    connection,
                    event_id=event["event_id"],
                    attempted=attempt_time,
                    statement_url=event["statement_url"],
                    result_state="incomplete_fact_abstain",
                    http_status=status,
                    response=response,
                    rejection_reason=reason,
                )
        cycle = {
            "due_events": len(due),
            "attempted": attempted,
            "inserted_facts": inserted,
            "rejected_incomplete": rejected,
            "errors": errors,
        }
        payload = snapshot(connection, observed, cycle)
    finally:
        connection.close()
    atomic_write(output, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    atomic_write(report, render_report(payload))
    return payload


def run_loop(interval_sec: int, duration_sec: int) -> None:
    deadline = time.monotonic() + max(1, duration_sec)
    while True:
        started = utc_now()
        try:
            payload = run_cycle(now=started)
            heartbeat = {
                "schema_version": SCHEMA_VERSION,
                "observed_utc": payload["generated_utc"],
                "state": "healthy",
                "counts": payload["counts"],
                "policy": dict(NO_TRADE_POLICY),
            }
        except Exception as exc:  # supervisor-visible fail-closed heartbeat
            heartbeat = {
                "schema_version": SCHEMA_VERSION,
                "observed_utc": iso(utc_now()),
                "state": "error",
                "error": f"{type(exc).__name__}:{exc}",
                "policy": dict(NO_TRADE_POLICY),
            }
        atomic_write(HEARTBEAT, json.dumps(heartbeat, indent=2, sort_keys=True) + "\n")
        if time.monotonic() >= deadline:
            return
        time.sleep(max(5, interval_sec))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--interval-sec", type=int, default=300)
    parser.add_argument("--duration-sec", type=int, default=0)
    args = parser.parse_args()
    if args.duration_sec > 0:
        run_loop(args.interval_sec, args.duration_sec)
    else:
        run_cycle()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
