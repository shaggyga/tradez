#!/usr/bin/env python3
"""Independent verifier for the dual-ledger official-event quote cohort.

No producer, broker, semantic classifier, lifecycle, or authorization module is
imported.  Identity, clocks, raw-ledger lineage, append-only protection,
per-pair counts, and safety flags are rebuilt from immutable bytes.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import time
from typing import Any, Mapping, Sequence
import urllib.parse


ROOT = Path(__file__).resolve().parent
LOCAL_NEWS = ROOT / "data" / "oanda_training_manager" / "local_news_sentiment"
FAST_DATABASE = LOCAL_NEWS / "official_release_fast_lane_v4.sqlite"
MAIN_DATABASE = LOCAL_NEWS / "local_news_sentiment_v1.sqlite"
CAPTURE_DATABASE = LOCAL_NEWS / "official_event_pair_quote_capture_v2.sqlite"
CAPTURE_STATE = LOCAL_NEWS / "official_event_pair_quote_capture_latest_v2.json"
OUTPUT_PATH = LOCAL_NEWS / "official_event_pair_quote_capture_verifier_latest_v2.json"
HEARTBEAT_PATH = LOCAL_NEWS / "official_event_pair_quote_capture_verifier_heartbeat_v2.json"

SCHEMA_VERSION = "official_event_pair_quote_capture_v2_verifier"
VERIFIER_CONTRACT_ID = "official_event_pair_quote_capture_v2_independent_verifier_20260904"
CAPTURE_SCHEMA_VERSION = "official_event_pair_quote_capture_v2"
CAPTURE_CONTRACT_ID = "official_event_pair_quote_capture_v2_dual_ledger_earliest_durable_20260904"
CAPTURE_COHORT_ID = "official_event_pair_quote_capture_v2_20260904a"
ACTIVATED_UTC = dt.datetime(2026, 9, 4, 15, 30, tzinfo=dt.timezone.utc)
FAST_COLLECTOR_CONTRACT_ID = "official_release_fast_lane_v4_selection_v2_authoritative_communications_20260828T150000Z"
FAST_COLLECTOR_COHORT_ID = "official_release_fast_lane_v4_communications_20260828T150000Z"
MAIN_COLLECTOR_CONTRACT_ID = "local_news_incremental_source_commit_v74_persistent_html_bootstrap_quarantine_20260827"
MAIN_COLLECTOR_COHORT_ID = MAIN_COLLECTOR_CONTRACT_ID
EXPECTED_INSTRUMENTS = tuple(
    """
    AUD_CAD AUD_CHF AUD_HKD AUD_JPY AUD_NZD AUD_SGD AUD_USD
    CAD_CHF CAD_HKD CAD_JPY CAD_SGD CHF_HKD CHF_JPY CHF_ZAR
    EUR_AUD EUR_CAD EUR_CHF EUR_CZK EUR_DKK EUR_GBP EUR_HKD EUR_HUF
    EUR_JPY EUR_NOK EUR_NZD EUR_PLN EUR_SEK EUR_SGD EUR_TRY EUR_USD EUR_ZAR
    GBP_AUD GBP_CAD GBP_CHF GBP_HKD GBP_JPY GBP_NZD GBP_PLN GBP_SGD GBP_USD GBP_ZAR
    HKD_JPY NZD_CAD NZD_CHF NZD_HKD NZD_JPY NZD_SGD NZD_USD SGD_CHF SGD_JPY
    TRY_JPY USD_CAD USD_CHF USD_CNH USD_CZK USD_DKK USD_HKD USD_HUF USD_JPY
    USD_MXN USD_NOK USD_PLN USD_SEK USD_SGD USD_THB USD_TRY USD_ZAR ZAR_JPY
    """.split()
)
SOURCE_IDS = tuple(
    sorted(
        """
        banxico_policy_decisions_direct_v1 bea_releases boc_press boc_speeches
        boe_news boe_speeches boj_updates bot_mpc_decisions_direct_v2
        census_economic_indicators cleveland_fed_speeches cnb_press
        dol_eta_ui_claims_scheduled_direct_pdf_v1 ecb_press
        ecb_statistical_press_releases fed_monetary_policy fed_speeches
        hkma_press_html japan_mof_international_policy japan_mof_press_conferences_ja
        kansas_city_fed_news_releases mas_monetary_policy_api_direct_v1
        mnb_policy_decisions_direct_v1 mnb_policy_decisions_hu_direct_v1
        nationalbanken_press_direct_v1 nbp_mpc_releases_api_direct_v1
        new_zealand_rbnz_ocr_snapshot_direct_v1 norges_press norges_speeches
        pboc_official_releases_direct_v1 rba_media rba_monetary_policy_minutes_html
        rba_speeches richmond_fed_speeches riksbank_monetary_policy_html
        riksbank_press riksbank_speeches sarb_publications_rss_direct_v1
        snb_monetary_policy snb_press south_africa_sarb_policy_rate_direct_v1
        stats_sa_ppi_scheduled_direct_pdf_v1 tcmb_press
        """.split()
    )
)
MAXIMUM_STATE_AGE_SECONDS = 90.0
_PREACTIVATION_CACHE: dict[tuple[str, str], set[str]] = {}


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def iso_utc(value: dt.datetime | None = None) -> str:
    current = value or utc_now()
    if current.tzinfo is None:
        current = current.replace(tzinfo=dt.timezone.utc)
    return current.astimezone(dt.timezone.utc).isoformat()


def parse_time(value: Any) -> dt.datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _payload(raw: str) -> dict[str, Any]:
    try:
        value = json.loads(str(raw or "{}"))
    except (TypeError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def canonical_url(value: Any) -> str:
    text = str(value or "").strip()
    if not text.lower().startswith(("http://", "https://")):
        return ""
    try:
        parsed = urllib.parse.urlsplit(text)
    except ValueError:
        return ""
    host = (parsed.hostname or "").lower().strip(".")
    if not host:
        return ""
    port = f":{parsed.port}" if parsed.port and parsed.port not in (80, 443) else ""
    path = re.sub(r"/{2,}", "/", parsed.path or "/")
    if path != "/":
        path = path.rstrip("/")
    return urllib.parse.urlunsplit(("https", host + port, path, parsed.query, ""))


def canonical_event_identity(row: Mapping[str, Any]) -> tuple[str, str]:
    payload = _payload(str(row.get("raw_payload_json") or ""))
    external_id = str(payload.get("external_id") or "").strip()
    url = ""
    for value in (
        external_id, row.get("source_url"), payload.get("url"),
        payload.get("source_url"), payload.get("publisher_url"),
        payload.get("detail_source_url"),
    ):
        url = canonical_url(value)
        if url:
            break
    exact_clock = bool(row.get("publisher_time_eligible")) or not bool(
        payload.get("published_time_inferred")
    )
    published = parse_time(payload.get("published_utc"))
    if url:
        key = f"url:{url}"
        if exact_clock and published is not None:
            key += f"|published:{iso_utc(published)}"
    elif external_id:
        key = f"external:{row.get('source_id') or ''}:{external_id.lower()}"
    else:
        headline = re.sub(
            r"[^a-z0-9]+", " ",
            str(payload.get("headline") or payload.get("title") or row.get("headline") or "").lower(),
        ).strip()
        if not headline:
            return "", ""
        published_key = "" if published is None else iso_utc(published)
        key = f"headline:{row.get('source_id') or ''}:{headline}|published:{published_key}"
    return key, sha256_text(f"official_event_v2|{key}")


def _alias_id(ledger_name: str, upstream_id: str) -> str:
    return sha256_text(
        f"{ledger_name}|{upstream_id}|{CAPTURE_CONTRACT_ID}|{CAPTURE_COHORT_ID}"
    )


def _raw_main(database: Path, event_id: str) -> dict[str, Any] | None:
    connection = sqlite3.connect(f"file:{database.as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        row = connection.execute(
            """SELECT event_id,source_id,source_quality,source_verified,published_utc,
                      first_seen_utc,headline,source_url,payload_json
               FROM articles WHERE event_id=?""", (event_id,),
        ).fetchone()
    finally:
        connection.close()
    if row is None:
        return None
    payload = _payload(str(row["payload_json"] or ""))
    reasons = []
    if int(row["source_verified"] or 0) != 1 or float(row["source_quality"] or 0) < 0.9:
        reasons.append("main_source_not_verified_high_quality")
    if payload.get("source_direct") is not True:
        reasons.append("main_source_not_direct")
    if payload.get("observation_clock_trusted") is not True:
        reasons.append("main_observation_clock_not_trusted")
    if payload.get("source_listing_bootstrap") is True:
        reasons.append("main_listing_bootstrap")
    if payload.get("collector_contract_id") != MAIN_COLLECTOR_CONTRACT_ID:
        reasons.append("main_collector_contract_mismatch")
    if payload.get("collector_cohort_id") != MAIN_COLLECTOR_COHORT_ID:
        reasons.append("main_collector_cohort_mismatch")
    return {
        "ledger_name": "main_news", "upstream_observation_id": str(row["event_id"]),
        "source_id": str(row["source_id"]), "source_first_seen_utc": str(row["first_seen_utc"]),
        "source_url": str(row["source_url"] or ""), "headline": str(row["headline"] or ""),
        "publisher_time_eligible": not bool(payload.get("published_time_inferred")),
        "raw_payload_json": str(row["payload_json"] or ""),
        "base_ineligibility_reasons": reasons,
    }


def _raw_fast(database: Path, observation_id: str) -> dict[str, Any] | None:
    connection = sqlite3.connect(f"file:{database.as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        row = connection.execute(
            "SELECT * FROM official_release_observation WHERE observation_id=?",
            (observation_id,),
        ).fetchone()
    finally:
        connection.close()
    if row is None:
        return None
    payload = _payload(str(row["raw_payload_json"] or ""))
    reasons = []
    if int(row["prospective_observation"] or 0) != 1:
        reasons.append("fast_not_prospective")
    if int(row["listing_bootstrap"] or 0) != 0:
        reasons.append("fast_listing_bootstrap")
    if int(row["identity_preexisting"] or 0) != 0:
        reasons.append("fast_identity_preexisting")
    if int(row["publisher_time_eligible"] or 0) != 1:
        reasons.append("fast_publisher_time_ineligible")
    if int(row["observation_clock_trusted"] or 0) != 1:
        reasons.append("fast_observation_clock_not_trusted")
    if row["collector_contract_id"] != FAST_COLLECTOR_CONTRACT_ID:
        reasons.append("fast_collector_contract_mismatch")
    if row["collector_cohort_id"] != FAST_COLLECTOR_COHORT_ID:
        reasons.append("fast_collector_cohort_mismatch")
    return {
        "ledger_name": "fast_lane", "upstream_observation_id": str(row["observation_id"]),
        "source_id": str(row["source_id"]), "source_first_seen_utc": str(row["first_seen_utc"]),
        "source_url": str(payload.get("url") or ""),
        "headline": str(payload.get("title") or payload.get("headline") or ""),
        "publisher_time_eligible": bool(row["publisher_time_eligible"]),
        "raw_payload_json": str(row["raw_payload_json"] or ""),
        "base_ineligibility_reasons": reasons,
    }


def _preactivation_identities(fast_database: Path, main_database: Path) -> set[str]:
    cache_key = (str(fast_database.resolve()), str(main_database.resolve()))
    if cache_key in _PREACTIVATION_CACHE:
        return _PREACTIVATION_CACHE[cache_key]
    placeholders = ",".join("?" for _ in SOURCE_IDS)
    identities: set[str] = set()
    connection = sqlite3.connect(f"file:{fast_database.as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            f"SELECT * FROM official_release_observation WHERE source_id IN ({placeholders}) AND first_seen_utc<?",
            (*SOURCE_IDS, iso_utc(ACTIVATED_UTC)),
        ).fetchall()
    finally:
        connection.close()
    for row in rows:
        payload = _payload(str(row["raw_payload_json"] or ""))
        item = {
            "source_id": row["source_id"], "source_url": payload.get("url"),
            "publisher_time_eligible": bool(row["publisher_time_eligible"]),
            "raw_payload_json": row["raw_payload_json"],
        }
        if canonical := canonical_event_identity(item)[1]:
            identities.add(canonical)
    connection = sqlite3.connect(f"file:{main_database.as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            f"SELECT source_id,source_url,headline,payload_json FROM articles WHERE source_id IN ({placeholders}) AND first_seen_utc<?",
            (*SOURCE_IDS, iso_utc(ACTIVATED_UTC)),
        ).fetchall()
    finally:
        connection.close()
    for row in rows:
        payload = _payload(str(row["payload_json"] or ""))
        item = {
            "source_id": row["source_id"], "source_url": row["source_url"],
            "headline": row["headline"],
            "publisher_time_eligible": not bool(payload.get("published_time_inferred")),
            "raw_payload_json": row["payload_json"],
        }
        if canonical := canonical_event_identity(item)[1]:
            identities.add(canonical)
    _PREACTIVATION_CACHE[cache_key] = identities
    return identities


def _verify_pair_payload(payload: Mapping[str, Any], prefix: str) -> list[str]:
    failures: list[str] = []
    pair_rows = payload.get("pair_rows")
    pair_rows = pair_rows if isinstance(pair_rows, Mapping) else {}
    if set(pair_rows) != set(EXPECTED_INSTRUMENTS):
        failures.append(f"{prefix}:pair_universe_mismatch")
    metadata = payload.get("metadata_invalid_reasons") or []
    eligible = 0
    for instrument in EXPECTED_INSTRUMENTS:
        row = pair_rows.get(instrument)
        row = row if isinstance(row, Mapping) else {}
        calculated = bool(
            not metadata
            and not row.get("invalid_reason")
            and row.get("tradeable") is True
            and isinstance(row.get("bid"), (int, float))
            and isinstance(row.get("ask"), (int, float))
            and float(row.get("ask") or 0) > float(row.get("bid") or 0)
            and isinstance(row.get("pip"), (int, float))
            and float(row.get("pip") or 0) > 0
            and parse_time(row.get("quote_time_utc")) is not None
        )
        if bool(row.get("eligible")) != calculated:
            failures.append(f"{prefix}:{instrument}:eligibility_mismatch")
        eligible += int(calculated)
    if int(payload.get("eligible_quote_count") or 0) != eligible:
        failures.append(f"{prefix}:eligible_quote_count_mismatch")
    if bool(payload.get("exact_all_68_available")) != (eligible == 68):
        failures.append(f"{prefix}:exact_all_68_mismatch")
    return failures


def verify(
    *,
    fast_database: Path = FAST_DATABASE,
    main_database: Path = MAIN_DATABASE,
    capture_database: Path = CAPTURE_DATABASE,
    capture_state_path: Path = CAPTURE_STATE,
    now_utc: dt.datetime | None = None,
) -> dict[str, Any]:
    observed = (now_utc or utc_now()).astimezone(dt.timezone.utc)
    failures: list[str] = []
    state = read_json(capture_state_path)
    state_time = parse_time(state.get("generated_utc"))
    state_age = None if state_time is None else (observed - state_time).total_seconds()
    if state_time is None or state_age is None or state_age < -2 or state_age > MAXIMUM_STATE_AGE_SECONDS:
        failures.append("producer_state_stale_or_invalid")
    for key, value in {
        "schema_version": CAPTURE_SCHEMA_VERSION,
        "contract_id": CAPTURE_CONTRACT_ID,
        "cohort_id": CAPTURE_COHORT_ID,
        "activated_utc": iso_utc(ACTIVATED_UTC),
        "status": "ok",
    }.items():
        if state.get(key) != value:
            failures.append(f"producer_state_{key}_mismatch")

    aliases: list[sqlite3.Row] = []
    captures: list[sqlite3.Row] = []
    integrity = "missing"
    trigger_count = 0
    try:
        connection = sqlite3.connect(f"file:{capture_database.as_posix()}?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        integrity = str(connection.execute("PRAGMA quick_check").fetchone()[0])
        trigger_count = int(
            connection.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name IN (?,?,?,?)",
                (
                    "official_event_alias_no_update", "official_event_alias_no_delete",
                    "official_event_pair_quote_capture_v2_no_update",
                    "official_event_pair_quote_capture_v2_no_delete",
                ),
            ).fetchone()[0]
        )
        aliases = connection.execute("SELECT * FROM official_event_alias ORDER BY row_observed_utc,alias_id").fetchall()
        captures = connection.execute("SELECT * FROM official_event_pair_quote_capture ORDER BY captured_utc,capture_id").fetchall()
        connection.close()
    except (OSError, sqlite3.Error) as exc:
        failures.append(f"capture_database_error:{type(exc).__name__}:{exc}")
    if integrity != "ok":
        failures.append(f"capture_database_integrity:{integrity}")
    if trigger_count != 4:
        failures.append("append_only_trigger_mismatch")

    preexisting = _preactivation_identities(fast_database, main_database) if aliases else set()
    alias_payloads: dict[str, dict[str, Any]] = {}
    for row in aliases:
        prefix = f"alias:{row['alias_id']}"
        try:
            payload = json.loads(str(row["alias_payload_json"]))
        except (TypeError, ValueError):
            failures.append(f"{prefix}:invalid_payload")
            continue
        alias_payloads[str(row["alias_id"])] = payload
        raw = (
            _raw_fast(fast_database, str(row["upstream_observation_id"]))
            if row["ledger_name"] == "fast_lane"
            else _raw_main(main_database, str(row["upstream_observation_id"]))
            if row["ledger_name"] == "main_news"
            else None
        )
        if raw is None:
            failures.append(f"{prefix}:raw_lineage_missing")
            continue
        identity_key, canonical_event_id = canonical_event_identity(raw)
        if str(row["alias_id"]) != _alias_id(str(row["ledger_name"]), str(row["upstream_observation_id"])):
            failures.append(f"{prefix}:alias_id_mismatch")
        if str(row["canonical_event_id"]) != canonical_event_id:
            failures.append(f"{prefix}:canonical_event_id_mismatch")
        if payload.get("canonical_identity_key") != identity_key:
            failures.append(f"{prefix}:canonical_identity_key_mismatch")
        if row["raw_payload_sha256"] != sha256_text(raw["raw_payload_json"]):
            failures.append(f"{prefix}:raw_payload_hash_mismatch")
        source_time = parse_time(raw["source_first_seen_utc"])
        row_time = parse_time(row["row_observed_utc"])
        actionable = parse_time(row["actionable_event_utc"])
        if source_time is None or row_time is None or actionable != max(source_time, row_time):
            failures.append(f"{prefix}:actionable_clock_mismatch")
        existed = canonical_event_id in preexisting
        if bool(row["identity_preexisting_before_activation"]) != existed:
            failures.append(f"{prefix}:preactivation_identity_mismatch")
        reasons = list(raw.get("base_ineligibility_reasons") or [])
        if source_time is None or source_time < ACTIVATED_UTC:
            reasons.append("source_first_seen_before_activation")
        if not canonical_event_id:
            reasons.append("canonical_publisher_identity_missing")
        if existed:
            reasons.append("canonical_identity_preexisting_before_activation")
        expected_state = "eligible_new_prospective_alias" if not reasons else "rejected_alias"
        if row["eligibility_state"] != expected_state:
            failures.append(f"{prefix}:eligibility_state_mismatch")
        if any(bool(payload.get(key)) for key in ("execution_eligible", "can_authorize", "can_promote")):
            failures.append(f"{prefix}:unsafe_alias_payload")
        if payload.get("contract_id") != CAPTURE_CONTRACT_ID or payload.get("cohort_id") != CAPTURE_COHORT_ID:
            failures.append(f"{prefix}:contract_mismatch")

    capture_by_alias: dict[str, sqlite3.Row] = {}
    for row in captures:
        prefix = f"capture:{row['capture_id']}"
        try:
            payload = json.loads(str(row["capture_payload_json"]))
        except (TypeError, ValueError):
            failures.append(f"{prefix}:invalid_payload")
            continue
        winner = alias_payloads.get(str(row["winner_alias_id"]))
        if winner is None or winner.get("eligibility_state") != "eligible_new_prospective_alias":
            failures.append(f"{prefix}:winner_alias_invalid")
            continue
        capture_by_alias[str(row["winner_alias_id"])] = row
        expected_id = sha256_text(
            f"{row['canonical_event_id']}|{CAPTURE_CONTRACT_ID}|{CAPTURE_COHORT_ID}"
        )
        if row["capture_id"] != expected_id:
            failures.append(f"{prefix}:capture_id_mismatch")
        for key in ("canonical_event_id", "source_first_seen_utc", "row_observed_utc", "actionable_event_utc"):
            if str(row[key]) != str(winner.get(key) or "") or str(payload.get(key) or "") != str(row[key]):
                failures.append(f"{prefix}:{key}_mismatch")
        captured = parse_time(row["captured_utc"])
        actionable = parse_time(row["actionable_event_utc"])
        if captured is None or actionable is None or captured < actionable:
            failures.append(f"{prefix}:capture_clock_precedes_actionable")
        elif not math.isclose(float(row["detection_latency_seconds"]), (captured - actionable).total_seconds(), abs_tol=1e-6):
            failures.append(f"{prefix}:detection_latency_mismatch")
        if row["raw_observation_payload_sha256"] != winner.get("raw_payload_sha256"):
            failures.append(f"{prefix}:raw_hash_lineage_mismatch")
        failures.extend(_verify_pair_payload(payload, prefix))
        if payload.get("semantic_mapping_read_before_capture") is not False:
            failures.append(f"{prefix}:semantic_mapping_boundary_mismatch")
        if any(bool(payload.get(key)) for key in ("execution_eligible", "can_place_orders", "can_authorize", "can_promote")):
            failures.append(f"{prefix}:unsafe_capture_payload")
        if payload.get("contract_id") != CAPTURE_CONTRACT_ID or payload.get("cohort_id") != CAPTURE_COHORT_ID:
            failures.append(f"{prefix}:contract_mismatch")

    counts = {
        "alias_count": len(aliases),
        "eligible_alias_count": sum(row["eligibility_state"] == "eligible_new_prospective_alias" for row in aliases),
        "preactivation_identity_rejection_count": sum(int(row["identity_preexisting_before_activation"]) for row in aliases),
        "capture_count": len(captures),
        "per_pair_ready_count": sum(row["timing_quality"] == "prospective_per_pair_executable_quotes" for row in captures),
        "exact_all_68_count": sum(int(row["exact_all_68_available"]) for row in captures),
        "eligible_pair_quote_total": sum(int(row["eligible_quote_count"]) for row in captures),
    }
    state_counts = state.get("counts") if isinstance(state.get("counts"), Mapping) else {}
    for key, value in counts.items():
        if int(state_counts.get(key) or 0) != int(value):
            failures.append(f"producer_state_count_mismatch:{key}")
    unique = sorted(set(failures))
    return {
        "schema_version": SCHEMA_VERSION,
        "verifier_contract_id": VERIFIER_CONTRACT_ID,
        "capture_contract_id": CAPTURE_CONTRACT_ID,
        "capture_cohort_id": CAPTURE_COHORT_ID,
        "verified_at_utc": iso_utc(observed),
        "verified": not unique,
        "status": "verified" if not unique else "failed",
        "failure_count": len(unique),
        "failures": unique,
        "producer_state_age_seconds": None if state_age is None else round(state_age, 6),
        "database_integrity": integrity,
        "append_only_trigger_count": trigger_count,
        "counts": counts,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_authorize": False,
        "can_promote": False,
        "supported_execution_decision": "no_trade",
    }


def run_cycle(
    *,
    fast_database: Path = FAST_DATABASE,
    main_database: Path = MAIN_DATABASE,
    capture_database: Path = CAPTURE_DATABASE,
    capture_state_path: Path = CAPTURE_STATE,
    output_path: Path = OUTPUT_PATH,
    heartbeat_path: Path = HEARTBEAT_PATH,
) -> dict[str, Any]:
    result = verify(
        fast_database=fast_database,
        main_database=main_database,
        capture_database=capture_database,
        capture_state_path=capture_state_path,
    )
    write_json_atomic(output_path, result)
    write_json_atomic(
        heartbeat_path,
        {
            "schema_version": 1,
            "worker": "oanda_official_event_pair_quote_capture_v2_verifier",
            "status": result["status"],
            "updated_at": result["verified_at_utc"],
            "phase": "independent_dual_ledger_verification",
            "details": {
                "verified": result["verified"],
                "failure_count": result["failure_count"],
                "capture_contract_id": CAPTURE_CONTRACT_ID,
                "capture_cohort_id": CAPTURE_COHORT_ID,
                **result["counts"],
            },
        },
    )
    return result


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fast-database", type=Path, default=FAST_DATABASE)
    parser.add_argument("--main-database", type=Path, default=MAIN_DATABASE)
    parser.add_argument("--capture-database", type=Path, default=CAPTURE_DATABASE)
    parser.add_argument("--capture-state", type=Path, default=CAPTURE_STATE)
    parser.add_argument("--output", type=Path, default=OUTPUT_PATH)
    parser.add_argument("--heartbeat", type=Path, default=HEARTBEAT_PATH)
    parser.add_argument("--interval-sec", type=float, default=30.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--quiet", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    started = time.monotonic()
    exit_code = 0
    while True:
        result = verify(
            fast_database=args.fast_database,
            main_database=args.main_database,
            capture_database=args.capture_database,
            capture_state_path=args.capture_state,
        )
        write_json_atomic(args.output, result)
        write_json_atomic(
            args.heartbeat,
            {
                "schema_version": 1,
                "worker": "oanda_official_event_pair_quote_capture_v2_verifier",
                "status": result["status"],
                "updated_at": result["verified_at_utc"],
                "phase": "independent_dual_ledger_verification",
                "details": {"verified": result["verified"], "failure_count": result["failure_count"], **result["counts"]},
            },
        )
        if not args.quiet:
            print(canonical_json(result), flush=True)
        if not result["verified"]:
            exit_code = 1
        if args.once or (args.duration_sec > 0 and time.monotonic() - started >= args.duration_sec):
            return exit_code
        time.sleep(max(1.0, args.interval_sec))


if __name__ == "__main__":
    raise SystemExit(main())
