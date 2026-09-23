"""Bounded secure adapter for RBNZ's maintained public policy snapshot.

The historical /en/... OCR route is not the maintained navigation target.
This adapter uses the official /monetary-policy page and reuses the existing
OCR parser. HTTP403 stays an explicit publisher-access failure, with no proxy,
header impersonation, insecure TLS, search-result ingestion or inferred bias.
It is an optional adapter; registry activation belongs to a separate review.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path
import ssl
import urllib.error
import urllib.parse
import urllib.request

URL = "https://www.rbnz.govt.nz/monetary-policy"
CONTRACT_ID = "rbnz_official_policy_snapshot_secure_v1_20260913"
MAX_BYTES = 2 * 1024 * 1024
MAX_TIMEOUT_SEC = 20


def now_utc():
    return dt.datetime.now(dt.timezone.utc)


def source_definition():
    return dict(source_id="new_zealand_rbnz_ocr_snapshot_direct_v1",
        name="Reserve Bank of New Zealand official policy snapshot",
        kind="rbnz_ocr_snapshot", url=URL, currencies=["NZD"], verified=True, direct=True,
        retrieval_via="direct_official_rbnz_policy_snapshot", source_role="primary_policy_release",
        quality=1.0, table_id="rbnz_current_ocr", event_series_id="new_zealand_official_cash_rate",
        event_name="New Zealand Official Cash Rate", event_country="New Zealand",
        event_timezone="Pacific/Auckland", unit="percent",
        source_contract_id=CONTRACT_ID, source_cohort_id=CONTRACT_ID,
        numeric_extraction_contract_id="new_zealand_rbnz_ocr_snapshot_v2_source_reported_clock_20260816",
        directional_research_only=True, trusted_domains=["rbnz.govt.nz"])


def _trusted(url):
    parsed = urllib.parse.urlsplit(url)
    return (parsed.scheme == "https" and parsed.hostname == "www.rbnz.govt.nz"
            and parsed.port in (None, 443) and parsed.username is None and parsed.password is None)


class OfficialRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if not _trusted(newurl):
            raise ValueError("rbnz_redirect_outside_secure_official_authority")
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def parse_snapshot(payload, *, observed_utc, source=None):
    if not isinstance(payload, bytes) or not 0 < len(payload) <= MAX_BYTES:
        raise ValueError("rbnz_payload_size_invalid")
    if observed_utc.tzinfo is None:
        raise ValueError("rbnz_observation_clock_must_be_aware")
    import oanda_local_news_sentiment as news
    row_source = source_definition() if source is None else dict(source)
    if row_source.get("url") != URL or row_source.get("source_contract_id") != CONTRACT_ID:
        raise ValueError("rbnz_source_contract_mismatch")
    rows = news.parse_rbnz_ocr_snapshot(payload, row_source)
    for row in rows:
        released = news.parse_datetime(row.get("published_utc"))
        if released is None or released > observed_utc:
            raise ValueError("rbnz_source_update_clock_missing_or_future")
        row.update(source_body_sha256=hashlib.sha256(payload).hexdigest(),
                   source_body_observed_utc=observed_utc.astimezone(dt.timezone.utc).isoformat(),
                   historical_original_known_claim=False,
                   snapshot_is_full_decision_body=False,
                   numeric_direction_policy="abstain_and_learn_response")
    return rows


def fetch_snapshot(*, opener=None, clock=now_utc):
    start = clock()
    result = dict(contract_id=CONTRACT_ID, source_id=source_definition()["source_id"],
                  requested_url=URL, attempted_utc=start.isoformat(), secure_transport=True,
                  research_only=True, can_place_orders=False, can_authorize=False,
                  can_promote=False, status="unavailable", rows=[], error="",
                  numeric_consensus_available=False, full_decision_body_available=False)
    if opener is None:
        opener = urllib.request.build_opener(OfficialRedirect(),
                    urllib.request.HTTPSHandler(context=ssl.create_default_context()))
    request = urllib.request.Request(URL, headers={
        "User-Agent": "ForexResearchNewsCollector/1.0 (causal research; low-rate polling)",
        "Accept": "text/html,application/xhtml+xml", "Accept-Encoding": "identity"})
    try:
        with opener.open(request, timeout=MAX_TIMEOUT_SEC) as response:
            result["http_status"] = int(response.status)
            result["final_url"] = response.geturl()
            if result["http_status"] != 200 or not _trusted(result["final_url"]):
                raise ValueError("rbnz_response_identity_or_status_invalid")
            if response.headers.get_content_type() not in ("text/html", "application/xhtml+xml"):
                raise ValueError("rbnz_response_is_not_html")
            payload = response.read(MAX_BYTES+1)
        read_at = clock()
        if read_at < start or (read_at-start).total_seconds() > MAX_TIMEOUT_SEC:
            raise ValueError("rbnz_read_clock_or_elapsed_bound")
        result["response_bytes"] = len(payload)
        result["rows"] = parse_snapshot(payload, observed_utc=read_at)
        result["source_body_sha256"] = hashlib.sha256(payload).hexdigest()
        result["status"] = "snapshot_parsed"
        result["source_body_observed_utc"] = read_at.isoformat()
    except urllib.error.HTTPError as exc:
        result["http_status"] = exc.code
        result["status"] = "publisher_access_blocked" if exc.code == 403 else "http_error"
        result["error"] = f"HTTP{exc.code}"
    except (OSError, ValueError, urllib.error.URLError) as exc:
        result["error"] = f"{type(exc).__name__}:{exc}"
    result["completed_utc"] = clock().isoformat()
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.receipt.exists():
        raise ValueError("receipt_exists_choose_new_path")
    result = fetch_snapshot()
    args.receipt.parent.mkdir(parents=True, exist_ok=True)
    with args.receipt.open("x", encoding="utf-8") as handle:
        json.dump(result, handle, indent=2, sort_keys=True)
    print(json.dumps({key: result.get(key) for key in ("status", "http_status", "error", "completed_utc")}))
    return 0 if result["status"] == "snapshot_parsed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
