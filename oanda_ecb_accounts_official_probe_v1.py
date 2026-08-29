#!/usr/bin/env python3
"""Bounded read-only probe for the 2026-08-27 ECB accounts publication."""

from __future__ import annotations

import argparse
import datetime as dt
import email.utils
import hashlib
import json
import os
import re
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Mapping


ROOT = Path(__file__).resolve().parent
REPORT_ROOT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "major_move_case_audits"
    / "ecb_accounts_20260827_v1"
)
OUTPUT = REPORT_ROOT / "ECB_ACCOUNTS_OFFICIAL_PROBE_V1.jsonl"
HEARTBEAT = REPORT_ROOT / "ECB_ACCOUNTS_OFFICIAL_PROBE_HEARTBEAT_V1.json"
RSS_URL = "https://www.ecb.europa.eu/rss/press.html"
INDEX_URL = "https://www.ecb.europa.eu/press/accounts/html/index.en.html"
UTC = dt.timezone.utc
ACCOUNT_TEXT = re.compile(r"account of the monetary policy meeting", re.I)
ACCOUNT_LINK = re.compile(r"/press/accounts/2026/", re.I)
CURRENT_EVENT = re.compile(r"(?:mg260827|22\s*[-–]\s*23 july 2026)", re.I)


def now_utc() -> dt.datetime:
    return dt.datetime.now(UTC)


def iso_utc(value: dt.datetime | None = None) -> str:
    return (value or now_utc()).astimezone(UTC).isoformat()


def parse_utc(value: str) -> dt.datetime:
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def fetch(url: str, timeout: float = 12.0) -> tuple[bytes, Mapping[str, str], int]:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "forex-research-ecb-accounts-probe/1.0",
            "Accept": "application/rss+xml,text/html;q=0.9,*/*;q=0.1",
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read(8_000_000), dict(response.headers.items()), int(response.status)


def rss_candidates(payload: bytes) -> list[dict[str, str]]:
    root = ET.fromstring(payload)
    output: list[dict[str, str]] = []
    for item in root.findall(".//item"):
        title = "".join(item.findtext("title") or "").strip()
        link = "".join(item.findtext("link") or "").strip()
        description = "".join(item.findtext("description") or "").strip()
        material = f"{title} {description} {link}"
        if not (
            (ACCOUNT_TEXT.search(material) or ACCOUNT_LINK.search(link))
            and CURRENT_EVENT.search(material)
        ):
            continue
        output.append(
            {
                "title": title,
                "url": urllib.parse.urljoin(RSS_URL, link),
                "published_raw": "".join(item.findtext("pubDate") or "").strip(),
                "guid": "".join(item.findtext("guid") or "").strip(),
            }
        )
    return output


def index_candidates(payload: bytes) -> list[dict[str, str]]:
    text = payload.decode("utf-8", errors="replace")
    output: list[dict[str, str]] = []
    for href, label in re.findall(
        r"<a\b[^>]*href=['\"]([^'\"]+)['\"][^>]*>(.*?)</a>", text, flags=re.I | re.S
    ):
        url = urllib.parse.urljoin(INDEX_URL, href)
        clean_label = re.sub(r"<[^>]+>", " ", label)
        clean_label = re.sub(r"\s+", " ", clean_label).strip()
        material = f"{clean_label} {url}"
        if not (
            (ACCOUNT_LINK.search(url) or ACCOUNT_TEXT.search(clean_label))
            and CURRENT_EVENT.search(material)
        ):
            continue
        output.append({"title": clean_label, "url": url, "published_raw": "", "guid": ""})
    dedup = {row["url"]: row for row in output if row["url"]}
    return list(dedup.values())


def normalized_header_clock(headers: Mapping[str, str], name: str) -> str:
    raw = str(headers.get(name) or headers.get(name.lower()) or "")
    try:
        parsed = email.utils.parsedate_to_datetime(raw)
    except (TypeError, ValueError):
        return ""
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return iso_utc(parsed)


def append_observation(payload: Mapping[str, Any]) -> None:
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True, ensure_ascii=False) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def run_probe(start: dt.datetime, end: dt.datetime, interval: float) -> int:
    seen: set[str] = set()
    errors = 0
    polls = 0
    while now_utc() <= end:
        current = now_utc()
        if current < start:
            atomic_json(
                HEARTBEAT,
                {
                    "status": "waiting_for_probe_window",
                    "generated_utc": iso_utc(current),
                    "start_utc": iso_utc(start),
                    "end_utc": iso_utc(end),
                    "research_only": True,
                    "execution_eligible": False,
                },
            )
            time.sleep(min(15.0, max(1.0, (start - current).total_seconds())))
            continue
        polls += 1
        cycle_errors: list[str] = []
        candidates: list[dict[str, str]] = []
        for kind, url, parser in (
            ("rss", RSS_URL, rss_candidates),
            ("accounts_index", INDEX_URL, index_candidates),
        ):
            observed = now_utc()
            try:
                body, headers, status = fetch(url)
                parsed = parser(body)
                for row in parsed:
                    item = {
                        "schema_version": 1,
                        "probe_contract_id": "ecb_accounts_official_bounded_probe_v1_20260827",
                        "transport": kind,
                        "observed_utc": iso_utc(observed),
                        "server_date_utc": normalized_header_clock(headers, "Date"),
                        "listing_url": url,
                        "listing_http_status": status,
                        "listing_content_sha256": hashlib.sha256(body).hexdigest(),
                        **row,
                        "research_only": True,
                        "execution_eligible": False,
                        "can_authorize": False,
                    }
                    identity = hashlib.sha256(
                        json.dumps(
                            [kind, item["url"], item["listing_content_sha256"]],
                            separators=(",", ":"),
                        ).encode("utf-8")
                    ).hexdigest()
                    if identity in seen:
                        continue
                    seen.add(identity)
                    if item["url"]:
                        try:
                            detail, detail_headers, detail_status = fetch(item["url"])
                            item.update(
                                {
                                    "detail_http_status": detail_status,
                                    "detail_server_date_utc": normalized_header_clock(
                                        detail_headers, "Date"
                                    ),
                                    "detail_last_modified_utc": normalized_header_clock(
                                        detail_headers, "Last-Modified"
                                    ),
                                    "detail_content_bytes": len(detail),
                                    "detail_content_sha256": hashlib.sha256(detail).hexdigest(),
                                }
                            )
                        except Exception as exc:  # network diagnostics are retained
                            item["detail_error"] = str(exc)[:500]
                    append_observation(item)
                    candidates.append(row)
            except Exception as exc:  # bounded probe must keep monitoring the other path
                errors += 1
                cycle_errors.append(f"{kind}:{str(exc)[:300]}")
        atomic_json(
            HEARTBEAT,
            {
                "status": "observed_release" if seen else "polling_release_paths",
                "generated_utc": iso_utc(),
                "start_utc": iso_utc(start),
                "end_utc": iso_utc(end),
                "poll_count": polls,
                "observation_identity_count": len(seen),
                "cycle_candidate_count": len(candidates),
                "total_errors": errors,
                "cycle_errors": cycle_errors,
                "research_only": True,
                "execution_eligible": False,
            },
        )
        time.sleep(max(5.0, interval))
    atomic_json(
        HEARTBEAT,
        {
            "status": "complete",
            "generated_utc": iso_utc(),
            "start_utc": iso_utc(start),
            "end_utc": iso_utc(end),
            "poll_count": polls,
            "observation_identity_count": len(seen),
            "total_errors": errors,
            "research_only": True,
            "execution_eligible": False,
        },
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-utc", default="2026-08-27T11:27:00+00:00")
    parser.add_argument("--end-utc", default="2026-08-27T11:40:00+00:00")
    parser.add_argument("--interval-sec", type=float, default=15.0)
    args = parser.parse_args()
    return run_probe(parse_utc(args.start_utc), parse_utc(args.end_utc), args.interval_sec)


if __name__ == "__main__":
    raise SystemExit(main())
