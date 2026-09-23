#!/usr/bin/env python3
"""Audit whether configured credentials can supply causal macro consensus.

The report never writes credential values or credentialed request URLs.  It is
an operational/source-governance diagnostic only and has no broker path.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Mapping

from oanda_alfred_macro_relative_strength_research import atomic
from oanda_macro_surprise_ledger import (
    CONSENSUS_ARCHIVE,
    current_consensus_contract,
    validate_consensus_projection_row,
)


ROOT = Path(__file__).resolve().parent
STATE = ROOT / "data" / "oanda_training_manager" / "state"
OUTPUT = STATE / "macro_consensus_access_audit_v1.json"
REPORT = ROOT / "data" / "oanda_training_manager" / "reports" / "macro_consensus" / "MACRO_CONSENSUS_ACCESS_CURRENT.md"
UTC = dt.timezone.utc
IMPORT = STATE / "macro_consensus_import_v1.jsonl"
COLLECTOR_STATE = STATE / "macro_consensus_prospective_v1.json"
TE_SCHEMA_DOC = "https://docs.tradingeconomics.com/economic_calendar/schema/"
FINNHUB_API_DOC = "https://finnhub.io/docs/api"


def present(name: str, environment: Mapping[str, str] | None = None) -> bool:
    values = environment if environment is not None else os.environ
    return bool(str(values.get(name) or "").strip())


def classify_finnhub(
    status_code: int | None,
    event_count: int | None,
    future_estimate_count: int | None = None,
) -> str:
    if status_code == 403:
        return "credential_present_but_premium_calendar_forbidden"
    if status_code == 200 and future_estimate_count:
        return "calendar_accessible_future_estimates_present"
    if status_code == 200 and event_count is not None:
        return "calendar_accessible_no_future_estimates_in_probe"
    if status_code is None:
        return "not_probed"
    return "calendar_probe_failed"


def classify_trading_economics(
    status_code: int | None,
    event_count: int | None,
    future_consensus_count: int | None,
) -> str:
    if status_code in {401, 403}:
        return "credential_present_but_calendar_access_forbidden"
    if status_code == 200 and future_consensus_count:
        return "calendar_accessible_future_consensus_present"
    if status_code == 200 and event_count is not None:
        return "calendar_accessible_no_future_consensus_in_probe"
    if status_code is None:
        return "not_probed"
    return "calendar_probe_failed"


def numeric(value: Any) -> bool:
    try:
        float(value)
    except (TypeError, ValueError):
        return False
    return True


def parse_time(value: Any) -> dt.datetime | None:
    try:
        parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def probe_trading_economics(key: str, now: dt.datetime) -> dict[str, Any]:
    query = urllib.parse.urlencode(
        {"c": key, "importance": 2, "values": "true", "f": "json"}
    )
    request = urllib.request.Request(
        "https://api.tradingeconomics.com/calendar?" + query,
        headers={"User-Agent": "forex-source-audit/2.0"},
    )
    status = None
    event_count = None
    future_consensus_count = None
    error_class = ""
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            status = int(response.status)
            payload = json.loads(response.read())
        rows = payload if isinstance(payload, list) else []
        event_count = len(rows)
        future_consensus_count = sum(
            1
            for row in rows
            if isinstance(row, Mapping)
            and (parse_time(row.get("Date")) or now) > now
            and numeric(row.get("ForecastValue"))
            and str(row.get("DateSpan") or "0") in {"0", "0.0"}
            and not numeric(row.get("ActualValue"))
            and not str(row.get("Actual") or "").strip()
        )
    except urllib.error.HTTPError as exc:
        status = int(exc.code)
        error_class = type(exc).__name__
    except Exception as exc:
        error_class = type(exc).__name__
    return {
        "http_status": status,
        "event_count": event_count,
        "future_consensus_count": future_consensus_count,
        "access_state": classify_trading_economics(
            status, event_count, future_consensus_count
        ),
        "error_class": error_class,
        "credential_or_url_exposed": False,
    }


def probe_finnhub(key: str, now: dt.datetime) -> dict[str, Any]:
    start = now.date().isoformat()
    end = (now.date() + dt.timedelta(days=7)).isoformat()
    query = urllib.parse.urlencode({"from": start, "to": end, "token": key})
    request = urllib.request.Request(
        "https://finnhub.io/api/v1/calendar/economic?" + query,
        headers={"User-Agent": "forex-source-audit/1.0"},
    )
    status = None
    event_count = None
    future_estimate_count = None
    error_class = ""
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            status = int(response.status)
            payload = json.loads(response.read())
        event_count = len(payload.get("economicCalendar") or [])
        future_estimate_count = sum(
            1
            for row in (payload.get("economicCalendar") or [])
            if isinstance(row, Mapping)
            and numeric(row.get("estimate"))
            and (parse_time(row.get("time")) or now) > now
        )
    except urllib.error.HTTPError as exc:
        status = int(exc.code)
        error_class = type(exc).__name__
    except Exception as exc:
        error_class = type(exc).__name__
    return {
        "http_status": status,
        "event_count": event_count,
        "future_estimate_count": future_estimate_count,
        "access_state": classify_finnhub(
            status, event_count, future_estimate_count
        ),
        "error_class": error_class,
        "credential_or_url_exposed": False,
    }


def audit_import(
    path: Path,
    now: dt.datetime,
    *,
    archive_path: Path | None = CONSENSUS_ARCHIVE,
    expected_contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    counts = {
        "rows": 0,
        "causal_v2_eligible": 0,
        "post_release": 0,
        "clock_untrusted": 0,
        "actual_present_or_unknown": 0,
        "missing_v2_provenance": 0,
        "frozen_contract_mismatch": 0,
        "source_timestamp_after_capture": 0,
        "archive_missing_or_mismatch": 0,
        "invalid_snapshot_sha256": 0,
        "malformed": 0,
        "other_rejected": 0,
    }
    if not path.is_file():
        return {"state": "absent", **counts}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        counts["rows"] += 1
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            counts["malformed"] += 1
            continue
        if not isinstance(row, Mapping):
            counts["malformed"] += 1
            continue
        rejection = validate_consensus_projection_row(
            row,
            expected_contract=dict(expected_contract or current_consensus_contract()),
            archive_path=archive_path,
        )
        if not rejection:
            counts["causal_v2_eligible"] += 1
        elif rejection in {
            "missing_identity_value_or_timestamp",
            "missing_observation_id",
            "missing_provider_calendar_id",
            "provider_event_version_mismatch",
        }:
            counts["missing_v2_provenance"] += 1
        elif rejection == "captured_at_or_after_release":
            counts["post_release"] += 1
        elif rejection == "capture_clock_untrusted":
            counts["clock_untrusted"] += 1
        elif rejection == "actual_present_or_state_unknown_at_capture":
            counts["actual_present_or_unknown"] += 1
        elif rejection == "frozen_consensus_contract_mismatch":
            counts["frozen_contract_mismatch"] += 1
        elif rejection == "source_timestamp_after_capture":
            counts["source_timestamp_after_capture"] += 1
        elif rejection == "provider_snapshot_archive_missing_or_mismatch":
            counts["archive_missing_or_mismatch"] += 1
        elif rejection == "invalid_provider_snapshot_sha256":
            counts["invalid_snapshot_sha256"] += 1
        else:
            counts["other_rejected"] += 1
    state = "causal_v2_rows_present" if counts["causal_v2_eligible"] else "no_causal_v2_rows"
    return {"state": state, **counts, "audited_utc": now.isoformat()}


def collector_state(path: Path, now: dt.datetime) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"state": "absent_or_unreadable", "age_sec": None}
    generated = parse_time(value.get("generated_utc"))
    age = (now - generated).total_seconds() if generated is not None else None
    return {
        "state": str(value.get("status") or "unknown"),
        "age_sec": round(age, 3) if age is not None else None,
        "stale": age is None or age > 9_000,
        "credential_present": bool(value.get("credential_present")),
        "causal_observations": int(
            ((value.get("totals") or {}).get("causal_observations") or 0)
        ),
    }


def run(
    output_path: Path = OUTPUT,
    report_path: Path = REPORT,
    *,
    environment: Mapping[str, str] | None = None,
    probe_network: bool = True,
    import_path: Path = IMPORT,
    collector_state_path: Path = COLLECTOR_STATE,
    archive_path: Path | None = CONSENSUS_ARCHIVE,
) -> dict[str, Any]:
    env = environment if environment is not None else os.environ
    now = dt.datetime.now(UTC)
    te_present = present("TRADING_ECONOMICS_API_KEY", env) or present("TE_API_KEY", env)
    finnhub_present = present("FINNHUB_API_KEY", env)
    av_present = present("ALPHA_VANTAGE_API_KEY", env)
    fred_present = present("FRED_API_KEY", env)
    te = {
        "http_status": None,
        "event_count": None,
        "future_consensus_count": None,
        "access_state": "missing_credential" if not te_present else "not_probed",
        "error_class": "",
        "credential_or_url_exposed": False,
    }
    if te_present and probe_network:
        te_key = str(env.get("TRADING_ECONOMICS_API_KEY") or env.get("TE_API_KEY"))
        te = probe_trading_economics(te_key, now)
    finnhub = {
        "http_status": None,
        "event_count": None,
        "access_state": "missing_credential" if not finnhub_present else "not_probed",
        "error_class": "",
        "credential_or_url_exposed": False,
    }
    if finnhub_present and probe_network:
        finnhub = probe_finnhub(str(env.get("FINNHUB_API_KEY")), now)
    providers = [
        {
            "provider": "Trading Economics",
            "credential_present": te_present,
            "role": "configured_pre_release_consensus",
            "official_documentation": TE_SCHEMA_DOC,
            **te,
        },
        {
            "provider": "Finnhub",
            "credential_present": finnhub_present,
            "role": "alternative_economic_calendar_with_estimate_actual_previous",
            "official_documentation": FINNHUB_API_DOC,
            "entitlement": "premium_access_required_by_official_documentation",
            **finnhub,
        },
        {
            "provider": "Alpha Vantage",
            "credential_present": av_present,
            "role": "news_sentiment_and_us_indicators_not_pre_release_consensus",
            "access_state": "not_a_consensus_source",
        },
        {
            "provider": "FRED_ALFRED",
            "credential_present": fred_present,
            "role": "official_vintages_and_initial_releases_not_market_consensus",
            "access_state": "not_a_consensus_source",
        },
    ]
    accessible = any(
        row.get("access_state") in {
            "calendar_accessible_future_consensus_present",
            "calendar_accessible_future_estimates_present",
        }
        for row in providers
    )
    import_audit = audit_import(import_path, now, archive_path=archive_path)
    collector = collector_state(collector_state_path, now)
    causal_ready = bool(
        accessible and import_audit["causal_v2_eligible"] > 0
    )
    result = {
        "schema_version": 2,
        "generated_utc": now.isoformat(),
        "providers": providers,
        "causal_consensus_provider_accessible": accessible,
        "causal_consensus_ready": causal_ready,
        "import_audit": import_audit,
        "collector_state": collector,
        "status": (
            "causal_v2_observations_available"
            if causal_ready
            else "provider_accessible_awaiting_causal_v2_capture"
            if accessible
            else "blocked_no_permitted_pre_release_consensus_access"
        ),
        "unlock_requirement": (
            "Connect a permitted provider entitlement that exposes future survey consensus, then let the V2 response-complete collector archive untouched pre-release snapshots. The current Finnhub credential lacks its premium Economic Calendar entitlement; Trading Economics has no configured credential."
        ),
        "free_provider_currently_accessible": False,
        "post_release_values_never_promoted": True,
        "secrets_persisted": False,
        "research_only": True,
        "execution_eligible": False,
        "supported_execution_decision": "no_trade",
    }
    atomic(output_path, json.dumps(result, indent=2, sort_keys=True) + "\n")
    lines = [
        "# Macro Consensus Access Audit",
        "",
        f"Generated: `{result['generated_utc']}`",
        "",
        f"Status: **{result['status']}**.",
        "",
        "| Provider | Credential | Role | Access state | HTTP |",
        "|---|---|---|---|---:|",
    ]
    for row in providers:
        lines.append(
            f"| {row['provider']} | {'present' if row['credential_present'] else 'missing'} | "
            f"{row['role']} | {row['access_state']} | {row.get('http_status') or 'n/a'} |"
        )
    lines += [
        "",
        f"V2 causal import rows: **{import_audit['causal_v2_eligible']}**; "
        f"post-release rows: **{import_audit['post_release']}**; "
        f"missing V2 provenance: **{import_audit['missing_v2_provenance']}**.",
        "",
        f"Collector state: **{collector['state']}** "
        f"({'stale' if collector.get('stale') else 'current'}).",
        "",
        result["unlock_requirement"],
        "",
        "No credential value or credentialed request URL is stored in this audit.",
        "",
    ]
    atomic(report_path, "\n".join(lines))
    return result


if __name__ == "__main__":
    run()
