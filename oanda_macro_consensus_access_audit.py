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


ROOT = Path(__file__).resolve().parent
STATE = ROOT / "data" / "oanda_training_manager" / "state"
OUTPUT = STATE / "macro_consensus_access_audit_v1.json"
REPORT = ROOT / "data" / "oanda_training_manager" / "reports" / "macro_consensus" / "MACRO_CONSENSUS_ACCESS_CURRENT.md"
UTC = dt.timezone.utc


def present(name: str, environment: Mapping[str, str] | None = None) -> bool:
    values = environment if environment is not None else os.environ
    return bool(str(values.get(name) or "").strip())


def classify_finnhub(status_code: int | None, event_count: int | None) -> str:
    if status_code == 403:
        return "credential_present_but_economic_calendar_access_forbidden"
    if status_code == 200 and event_count is not None:
        return "calendar_accessible"
    if status_code is None:
        return "not_probed"
    return "calendar_probe_failed"


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
    error_class = ""
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            status = int(response.status)
            payload = json.loads(response.read())
        event_count = len(payload.get("economicCalendar") or [])
    except urllib.error.HTTPError as exc:
        status = int(exc.code)
        error_class = type(exc).__name__
    except Exception as exc:
        error_class = type(exc).__name__
    return {
        "http_status": status,
        "event_count": event_count,
        "access_state": classify_finnhub(status, event_count),
        "error_class": error_class,
        "credential_or_url_exposed": False,
    }


def run(
    output_path: Path = OUTPUT,
    report_path: Path = REPORT,
    *,
    environment: Mapping[str, str] | None = None,
    probe_network: bool = True,
) -> dict[str, Any]:
    env = environment if environment is not None else os.environ
    now = dt.datetime.now(UTC)
    te_present = present("TRADING_ECONOMICS_API_KEY", env) or present("TE_API_KEY", env)
    finnhub_present = present("FINNHUB_API_KEY", env)
    av_present = present("ALPHA_VANTAGE_API_KEY", env)
    fred_present = present("FRED_API_KEY", env)
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
            "access_state": "credential_present_not_probed" if te_present else "missing_credential",
        },
        {
            "provider": "Finnhub",
            "credential_present": finnhub_present,
            "role": "alternative_economic_calendar_with_estimate_actual_previous",
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
        row.get("access_state") == "calendar_accessible" for row in providers
    )
    result = {
        "schema_version": 1,
        "generated_utc": now.isoformat(),
        "providers": providers,
        "causal_consensus_provider_accessible": accessible,
        "status": "provider_accessible_requires_causal_capture_validation" if accessible else "blocked_no_accessible_consensus_provider",
        "unlock_requirement": "Provide a licensed Trading Economics calendar credential or Finnhub plan with Economic Calendar access, then begin prospective pre-release snapshots.",
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
        result["unlock_requirement"],
        "",
        "No credential value or credentialed request URL is stored in this audit.",
        "",
    ]
    atomic(report_path, "\n".join(lines))
    return result


if __name__ == "__main__":
    run()
