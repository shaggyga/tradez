"""Chronological, spread-aware replay for the local deterministic news feed.

This tool is research-only.  It reads retained news and local M1 bid/ask
candles, compares the stored classifier with the current classifier, and
writes an audit report.  It never calls a broker or changes account state.
"""

from __future__ import annotations

import argparse
import ast
import bisect
import csv
import datetime as dt
import hashlib
import inspect
import json
import math
import os
import sqlite3
import statistics
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

import oanda_local_news_sentiment as news
try:
    from oanda_news_research_report_io_v1 import read_json_snapshot,new_run_report_path,publish_new_json_report
except ModuleNotFoundError:
    from trad.oanda_news_research_report_io_v1 import read_json_snapshot,new_run_report_path,publish_new_json_report
try:
    from oanda_news_reaction_contract_v2 import ENDPOINT_CONTRACT,aware_time,endpoint_returns,finite_number,positive_integer
    from oanda_instrument_pips_v2 import CONTRACT as PIP_CONTRACT,resolve_pip_contract
except ModuleNotFoundError:
    from trad.oanda_news_reaction_contract_v2 import ENDPOINT_CONTRACT,aware_time,endpoint_returns,finite_number,positive_integer
    from trad.oanda_instrument_pips_v2 import CONTRACT as PIP_CONTRACT,resolve_pip_contract


ROOT = Path(__file__).resolve().parent
DEFAULT_DATABASE = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "local_news_sentiment"
    / "local_news_sentiment_v1.sqlite"
)
DEFAULT_CONFIG = ROOT / "config" / "news_sources_v1.json"
DEFAULT_CANDLE_ROOT = ROOT / "data" / "oanda_training_manager" / "candles"
DEFAULT_INSTRUMENT_METADATA = DEFAULT_CANDLE_ROOT / "instrument_metadata_v1.json"
DEFAULT_REPORT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "news_feed_backtest"
    / "news_feed_exact_endpoints_v6_latest.json"
)
DEFAULT_RECLASSIFICATION_CACHE = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "cache"
    / "news_reclassification_cache_v1.json"
)
DEFAULT_CREDS = ROOT / "creds"
DEFAULT_PAIRS = tuple(news.event_tagger.discover_instruments())
UTC = dt.timezone.utc
SCHEMA_VERSION = "local_news_topic_movement_backtest_v6"
EVENT_CLUSTER_WINDOW_MINUTES = 30
EVENT_CLUSTER_DIRECTION_AGREEMENT = 0.80


def parse_timestamp(value: Any) -> dt.datetime | None:
    return news.parse_datetime(value)


def _load_pip_contracts_snapshot(path: Path = DEFAULT_INSTRUMENT_METADATA):
    payload,identity=read_json_snapshot(path,allow_missing=True)
    instruments=payload.get('instruments',{})
    if not isinstance(instruments,Mapping):raise ValueError('instrument_metadata_mapping_required')
    return {str(instrument):resolve_pip_contract(str(instrument),metadata) for instrument,metadata in instruments.items()},identity


def load_pip_contracts(path: Path = DEFAULT_INSTRUMENT_METADATA) -> dict[str,dict[str,Any]]:
    return _load_pip_contracts_snapshot(path)[0]


def load_pip_sizes(path: Path = DEFAULT_INSTRUMENT_METADATA) -> dict[str, float]:
    return {pair: receipt["pip"] for pair, receipt in load_pip_contracts(path).items()}


def load_key_value_creds(path: Path) -> dict[str, Any]:
    output: dict[str, Any] = {}
    if not path.exists():
        return output
    for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, value = line.split("=", 1)
        name = name.strip()
        if not name.startswith("OANDA_"):
            continue
        try:
            output[name] = ast.literal_eval(value.strip())
        except (SyntaxError, ValueError):
            output[name] = value.strip().strip("\"'")
    return output


def readonly_price_token(creds_path: Path) -> tuple[str, str]:
    creds = load_key_value_creds(creds_path)
    practice_token = (
        creds.get("OANDA_API_KEY")
        or creds.get("OANDA_ACCESS_TOKEN")
        or creds.get("OANDA_TOKEN")
        or creds.get("OANDA_API_TOKEN")
        or os.environ.get("OANDA_API_KEY")
    )
    live_token = (
        creds.get("OANDA_LIVE_API_KEY")
        or creds.get("OANDA_API_KEY_LIVE")
        or creds.get("OANDA_LIVE_API_TOKEN")
        or creds.get("OANDA_API_TOKEN_LIVE")
    )
    if practice_token:
        return str(practice_token), "https://api-fxpractice.oanda.com"
    if live_token:
        return str(live_token), "https://api-fxtrade.oanda.com"
    raise RuntimeError(f"no OANDA read-only price token found in {creds_path}")


def last_csv_timestamp(path: Path) -> dt.datetime | None:
    if not path.exists():
        return None
    with path.open("rb") as handle:
        handle.seek(0, 2)
        position = handle.tell()
        line = bytearray()
        while position > 0:
            position -= 1
            handle.seek(position)
            character = handle.read(1)
            if character in {b"\n", b"\r"}:
                if line:
                    break
                continue
            line.extend(character)
    if not line:
        return None
    values = next(csv.reader([bytes(reversed(line)).decode("utf-8", errors="ignore")]))
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        header = next(csv.reader(handle))
    time_index = header.index("datetime") if "datetime" in header else header.index("time")
    return parse_timestamp(values[time_index])


def fetch_candle_page(
    *,
    token: str,
    base_url: str,
    pair: str,
    end_time: dt.datetime | None,
    count: int = 5000,
) -> dict[str, Any]:
    parameters = {
        "price": "BAM",
        "granularity": "M1",
        "count": str(min(5000, max(10, count))),
    }
    if end_time is not None:
        parameters["to"] = news.iso_utc(end_time).replace("+00:00", "Z")
    url = (
        f"{base_url}/v3/instruments/{urllib.parse.quote(pair)}/candles?"
        f"{urllib.parse.urlencode(parameters)}"
    )
    request = urllib.request.Request(
        url,
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=30.0) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read(1000).decode("utf-8", errors="ignore")
        raise RuntimeError(f"OANDA price GET failed HTTP {exc.code}: {detail}") from exc


def candle_csv_row(
    candle: Mapping[str, Any],
    *,
    pair: str,
) -> dict[str, Any] | None:
    if not bool(candle.get("complete", True)):
        return None
    timestamp = parse_timestamp(candle.get("time"))
    if timestamp is None:
        return None
    mid = candle.get("mid") or {}
    bid = candle.get("bid") or {}
    ask = candle.get("ask") or {}
    try:
        bid_close = float(bid.get("c"))
        ask_close = float(ask.get("c"))
    except (TypeError, ValueError):
        return None
    pip_multiplier = 100.0 if pair.endswith("_JPY") else 10_000.0
    return {
        "time": str(candle.get("time") or ""),
        "datetime": news.iso_utc(timestamp),
        "instrument": pair,
        "granularity": "M1",
        "open": mid.get("o", ""),
        "high": mid.get("h", ""),
        "low": mid.get("l", ""),
        "close": mid.get("c", ""),
        "volume": candle.get("volume", ""),
        "bid_open": bid.get("o", ""),
        "bid_high": bid.get("h", ""),
        "bid_low": bid.get("l", ""),
        "bid_close": bid.get("c", ""),
        "ask_open": ask.get("o", ""),
        "ask_high": ask.get("h", ""),
        "ask_low": ask.get("l", ""),
        "ask_close": ask.get("c", ""),
        "spread_pips": (ask_close - bid_close) * pip_multiplier,
        "_timestamp": timestamp,
    }


def refresh_candle_file(
    path: Path,
    *,
    pair: str,
    token: str,
    base_url: str,
    max_requests: int,
) -> dict[str, Any]:
    before = last_csv_timestamp(path)
    end_time: dt.datetime | None = None
    fetched: dict[dt.datetime, dict[str, Any]] = {}
    request_count = 0
    for _ in range(max(1, max_requests)):
        payload = fetch_candle_page(
            token=token,
            base_url=base_url,
            pair=pair,
            end_time=end_time,
        )
        request_count += 1
        page_rows = [
            row
            for candle in payload.get("candles") or []
            if (row := candle_csv_row(candle, pair=pair)) is not None
        ]
        if not page_rows:
            break
        for row in page_rows:
            if before is None or row["_timestamp"] > before:
                fetched[row["_timestamp"]] = row
        earliest = min(row["_timestamp"] for row in page_rows)
        if before is not None and earliest <= before:
            break
        next_end = earliest - dt.timedelta(seconds=1)
        if end_time is not None and next_end >= end_time:
            raise RuntimeError(f"candle pagination made no progress for {pair}")
        end_time = next_end
        time.sleep(0.05)

    rows = [fetched[key] for key in sorted(fetched)]
    if rows:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            header = next(csv.reader(handle))
        with path.open("a", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=header,
                extrasaction="ignore",
                lineterminator="\n",
            )
            writer.writerows(rows)
    after = rows[-1]["_timestamp"] if rows else before
    return {
        "pair": pair,
        "requests": request_count,
        "rows_appended": len(rows),
        "before_last_utc": news.iso_utc(before) if before else None,
        "after_last_utc": news.iso_utc(after) if after else None,
    }


def refresh_candles(
    *,
    pairs: Sequence[str],
    candle_root: Path,
    creds_path: Path,
    max_requests: int,
) -> dict[str, Any]:
    token, base_url = readonly_price_token(creds_path)
    rows: list[dict[str, Any]] = []
    for index, pair in enumerate(pairs, 1):
        print(f"[news-backtest-price-refresh] {index}/{len(pairs)} {pair}", flush=True)
        rows.append(
            refresh_candle_file(
                candle_root / f"{pair}_M1.csv",
                pair=pair,
                token=token,
                base_url=base_url,
                max_requests=max_requests,
            )
        )
    return {
        "status": "ok",
        "endpoint_type": "instrument_candles_get_only",
        "environment": "practice" if "fxpractice" in base_url else "live",
        "orders_placed": 0,
        "pairs": rows,
    }


def load_articles(
    database: Path,
    *,
    since: dt.datetime,
) -> list[dict[str, Any]]:
    connection = sqlite3.connect(database, timeout=30.0)
    try:
        rows = connection.execute(
            """
            SELECT payload_json, first_seen_utc, last_seen_utc, duplicate_count
            FROM articles
            WHERE published_utc >= ?
            ORDER BY first_seen_utc, event_id
            """,
            (news.iso_utc(since),),
        ).fetchall()
    finally:
        connection.close()
    output: list[dict[str, Any]] = []
    for payload_json, first_seen, last_seen, duplicate_count in rows:
        try:
            payload = json.loads(payload_json)
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        payload["first_seen_utc"] = first_seen
        payload["causal_known_utc"] = article_causal_known_utc(
            payload,
            first_seen,
        )
        payload["last_seen_utc"] = last_seen
        payload["duplicate_observation_count"] = int(duplicate_count or 0)
        payload["corroboration_count"] = int(
            payload.get("corroboration_count") or 0
        )
        output.append(payload)
    return output


def article_causal_known_utc(
    payload: Mapping[str, Any],
    first_seen_utc: Any,
) -> str:
    """Prevent current enriched text from being replayed at listing time."""

    first_seen = parse_timestamp(first_seen_utc)
    if first_seen is None:
        return str(first_seen_utc or "")
    if not bool(payload.get("detail_enriched")):
        return news.iso_utc(first_seen)
    detail_available = parse_timestamp(payload.get("detail_available_utc"))
    if detail_available is not None and detail_available > first_seen:
        return news.iso_utc(detail_available)
    return news.iso_utc(first_seen)


SOURCE_PROVENANCE_FIELDS = (
    "detail_archive_path",
    "detail_available_utc",
    "detail_content_bytes",
    "detail_content_sha256",
    "detail_enriched",
    "detail_enrichment_kind",
    "detail_enrichment_research_only",
    "detail_source_url",
    "detail_text_characters",
    "official_policy_release",
    "policy_document_type",
    "source_listing_bootstrap",
)


def preserve_source_provenance(
    classified: Mapping[str, Any],
    source_record: Mapping[str, Any],
) -> dict[str, Any]:
    """Overlay immutable fetch/provenance facts on a reclassified article.

    ``classify_article`` intentionally rebuilds semantic fields, but it cannot
    reconstruct when an enriched official document became available.  Losing
    those fields makes headline-only and later full-text versions tie during
    topic clustering, allowing the earlier neutral listing to suppress the
    later causal interpretation.
    """

    row = dict(classified)
    for key in SOURCE_PROVENANCE_FIELDS:
        if key in source_record:
            row[key] = source_record.get(key)
    return row


def load_topic_history(
    database: Path,
    *,
    since: dt.datetime,
) -> list[dict[str, Any]]:
    connection = sqlite3.connect(database, timeout=30.0)
    try:
        table = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='topic_events'"
        ).fetchone()
        if not table:
            return []
        rows = connection.execute(
            """
            SELECT payload_json
            FROM topic_events
            WHERE first_known_utc >= ?
            ORDER BY first_known_utc, topic_id
            """,
            (news.iso_utc(since),),
        ).fetchall()
    finally:
        connection.close()
    output: list[dict[str, Any]] = []
    for (payload_json,) in rows:
        try:
            payload = json.loads(payload_json)
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        payload["topic_clustered"] = True
        output.append(payload)
    return output


def reclassify_articles(
    articles: Sequence[Mapping[str, Any]],
    *,
    config_path: Path,
) -> list[dict[str, Any]]:
    config = news.load_json(config_path, {})
    sources = {
        str(row.get("source_id") or ""): row
        for row in (config.get("sources") or [])
        if isinstance(row, dict)
    }
    output: list[dict[str, Any]] = []
    for previous in articles:
        first_seen = parse_timestamp(previous.get("first_seen_utc"))
        if first_seen is None:
            continue
        source = sources.get(str(previous.get("source_id") or "")) or {}
        current = news.classify_article(
            {
                "source_id": previous.get("source_id"),
                "source_name": previous.get("source_name"),
                "source_kind": previous.get("source_kind"),
                "source_quality": previous.get("source_quality"),
                "source_verified": previous.get("source_verified"),
                "source_direct": previous.get("source_direct", source.get("direct", True)),
                "retrieval_via": previous.get(
                    "retrieval_via",
                    source.get("retrieval_via", "direct"),
                ),
                "publisher_url": previous.get("publisher_url"),
                "source_currencies": list(source.get("currencies") or []),
                "source_role": previous.get(
                    "source_role",
                    source.get("source_role", source.get("role", "unspecified")),
                ),
                "structured_event": previous.get("structured_event"),
                "external_id": previous.get("external_id"),
                "scheduled_utc": previous.get("scheduled_utc"),
                "source_reported_update_utc": previous.get(
                    "source_reported_update_utc"
                ),
                "published_time_inferred": previous.get(
                    "published_time_inferred"
                ),
                "actual_value": previous.get("actual_value"),
                "actual": previous.get("actual"),
                "consensus_value": previous.get("consensus_value"),
                "consensus": previous.get("consensus"),
                "previous_value": previous.get("previous_value"),
                "previous": previous.get("previous"),
                "revised_previous_value": previous.get(
                    "revised_previous_value"
                ),
                "revised_previous": previous.get("revised_previous"),
                "detail_enriched": previous.get("detail_enriched"),
                "detail_enrichment_kind": previous.get(
                    "detail_enrichment_kind"
                ),
                "detail_enrichment_research_only": previous.get(
                    "detail_enrichment_research_only"
                ),
                "detail_available_utc": previous.get("detail_available_utc"),
                "detail_content_sha256": previous.get("detail_content_sha256"),
                "detail_content_bytes": previous.get("detail_content_bytes"),
                "detail_text_characters": previous.get(
                    "detail_text_characters"
                ),
                "detail_source_url": previous.get("detail_source_url"),
                "detail_archive_path": previous.get("detail_archive_path"),
                "source_listing_bootstrap": previous.get(
                    "source_listing_bootstrap"
                ),
                "official_policy_release": previous.get(
                    "official_policy_release"
                ),
                "policy_document_type": previous.get("policy_document_type"),
                "title": previous.get("headline"),
                "summary": previous.get("summary"),
                "url": previous.get("source_url"),
                "published_utc": previous.get("published_utc"),
            },
            first_seen=first_seen,
        )
        current = preserve_source_provenance(current, previous)
        current["first_seen_utc"] = str(previous.get("first_seen_utc") or "")
        current["causal_known_utc"] = str(
            previous.get("causal_known_utc")
            or article_causal_known_utc(
                previous,
                previous.get("first_seen_utc"),
            )
        )
        current["last_seen_utc"] = str(previous.get("last_seen_utc") or "")
        current["duplicate_observation_count"] = int(
            previous.get("duplicate_observation_count") or 0
        )
        output.append(current)
    return output


def classification_cache_key(
    articles: Sequence[Mapping[str, Any]],
    *,
    config_path: Path,
) -> str:
    """Fingerprint every input that can change deterministic classification."""

    digest = hashlib.sha256()
    digest.update(str(news.CLASSIFICATION_VERSION).encode("utf-8"))
    digest.update(config_path.read_bytes())
    news_path = Path(str(news.__file__ or ""))
    if news_path.exists():
        digest.update(news_path.read_bytes())
    for article in articles:
        digest.update(
            json.dumps(
                dict(article),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            ).encode("utf-8")
        )
        digest.update(b"\n")
    return digest.hexdigest()


def classification_context_key(*, config_path: Path) -> str:
    digest = hashlib.sha256()
    digest.update(str(news.CLASSIFICATION_VERSION).encode("utf-8"))
    digest.update(config_path.read_bytes())
    news_path = Path(str(news.__file__ or ""))
    if news_path.exists():
        digest.update(news_path.read_bytes())
    # The adapter decides which point-in-time provenance fields are presented
    # to the classifier.  Fingerprint only that classification path so report
    # or scoring changes do not invalidate a large deterministic cache, while
    # provenance/causality changes always do.
    for function in (
        article_causal_known_utc,
        preserve_source_provenance,
        reclassify_articles,
    ):
        digest.update(inspect.getsource(function).encode("utf-8"))
    return digest.hexdigest()


def article_classification_key(article: Mapping[str, Any]) -> str:
    """Fingerprint only fields that influence deterministic classification."""

    relevant = {
        key: article.get(key)
        for key in (
            "source_id",
            "source_name",
            "source_kind",
            "source_quality",
            "source_verified",
            "source_direct",
            "retrieval_via",
            "publisher_url",
            "headline",
            "summary",
            "source_url",
            "published_utc",
            "first_seen_utc",
            "causal_known_utc",
        )
    }
    return hashlib.sha256(
        json.dumps(
            relevant,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    ).hexdigest()


def reclassify_articles_cached(
    articles: Sequence[Mapping[str, Any]],
    *,
    config_path: Path,
    cache_path: Path,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Reuse deterministic classifications only under an exact content key."""

    cache_key = classification_cache_key(articles, config_path=config_path)
    context_key = classification_context_key(config_path=config_path)
    cached = news.load_json(cache_path, {})
    cached_entries = (
        cached.get("entries")
        if isinstance(cached, Mapping)
        and cached.get("classification_context_key") == context_key
        else {}
    )
    entries: dict[str, dict[str, Any]] = {
        str(key): dict(value)
        for key, value in (
            cached_entries.items() if isinstance(cached_entries, Mapping) else ()
        )
        if isinstance(value, Mapping)
    }
    article_keys = [article_classification_key(article) for article in articles]
    missing_by_key: dict[str, Mapping[str, Any]] = {}
    for key, article in zip(article_keys, articles):
        if key not in entries:
            missing_by_key.setdefault(key, article)
    if missing_by_key:
        missing_inputs = list(missing_by_key.values())
        classified = reclassify_articles(
            missing_inputs,
            config_path=config_path,
        )
        if len(classified) == len(missing_inputs):
            for key, row in zip(missing_by_key, classified):
                entries[key] = row
        else:
            for key, article in missing_by_key.items():
                single = reclassify_articles([article], config_path=config_path)
                if single:
                    entries[key] = single[0]

    rows: list[dict[str, Any]] = []
    for key, article in zip(article_keys, articles):
        cached_row = entries.get(key)
        if not cached_row:
            continue
        row = dict(cached_row)
        row = preserve_source_provenance(row, article)
        row["first_seen_utc"] = str(article.get("first_seen_utc") or "")
        row["causal_known_utc"] = str(
            article.get("causal_known_utc")
            or article_causal_known_utc(
                article,
                article.get("first_seen_utc"),
            )
        )
        row["last_seen_utc"] = str(article.get("last_seen_utc") or "")
        row["duplicate_observation_count"] = int(
            article.get("duplicate_observation_count") or 0
        )
        rows.append(row)

    # Retain only entries needed by the current source-history slice.  The
    # cache is reproducible derived data, not causal evidence.
    retained_entries = {
        key: entries[key] for key in dict.fromkeys(article_keys) if key in entries
    }
    news.atomic_write_json(
        cache_path,
        {
            "schema_version": 2,
            "cache_key": cache_key,
            "classification_context_key": context_key,
            "classifier_version": news.CLASSIFICATION_VERSION,
            "created_utc": news.iso_utc(),
            "article_count": len(rows),
            "entries": retained_entries,
        },
    )
    return rows, {
        "cache_hit": not missing_by_key,
        "cache_hits": len(articles) - len(missing_by_key),
        "cache_misses": len(missing_by_key),
        "cache_key": cache_key,
        "classification_context_key": context_key,
        "cache_path": str(cache_path),
    }


def _line_timestamp(line: bytes) -> dt.datetime | None:
    first = line.split(b",", 1)[0].decode("ascii", errors="ignore")
    return parse_timestamp(first)


def recent_csv_lines(path: Path, *, since: dt.datetime) -> tuple[str, list[str]]:
    """Read only the recent tail of a large chronological candle CSV."""

    with path.open("rb") as handle:
        header_bytes = handle.readline()
        data_start = handle.tell()
        handle.seek(0, 2)
        position = handle.tell()
        carry = b""
        selected: list[bytes] = []
        finished = False
        while position > data_start and not finished:
            start = max(data_start, position - 1_048_576)
            handle.seek(start)
            block = handle.read(position - start) + carry
            pieces = block.split(b"\n")
            if start > data_start:
                carry = pieces[0]
                complete = pieces[1:]
            else:
                carry = b""
                complete = pieces
            for line in reversed(complete):
                line = line.strip(b"\r")
                if not line:
                    continue
                timestamp = _line_timestamp(line)
                if timestamp is None:
                    continue
                if timestamp < since:
                    finished = True
                    break
                selected.append(line)
            position = start
        selected.reverse()
    return (
        header_bytes.decode("utf-8-sig", errors="ignore").strip(),
        [line.decode("utf-8", errors="ignore") for line in selected],
    )


def load_candles(
    path: Path,
    *,
    since: dt.datetime,
) -> list[dict[str, Any]]:
    header, lines = recent_csv_lines(path, since=since)
    if not header or not lines:
        return []
    rows: list[dict[str, Any]] = []
    for raw in csv.DictReader([header, *lines]):
        timestamp = parse_timestamp(raw.get("datetime") or raw.get("time"))
        if timestamp is None:
            continue
        try:
            bid_open = float(raw.get("bid_open") or "nan")
            ask_open = float(raw.get("ask_open") or "nan")
        except ValueError:
            continue
        if not math.isfinite(bid_open) or not math.isfinite(ask_open):
            continue
        rows.append(
            {
                "timestamp": timestamp,
                "bid_open": bid_open,
                "ask_open": ask_open,
                "bid_high": news.safe_float(raw.get("bid_high"), bid_open),
                "bid_low": news.safe_float(raw.get("bid_low"), bid_open),
                "ask_high": news.safe_float(raw.get("ask_high"), ask_open),
                "ask_low": news.safe_float(raw.get("ask_low"), ask_open),
            }
        )
    rows.sort(key=lambda row: row["timestamp"])
    return rows


def directional_calls(
    articles: Sequence[Mapping[str, Any]],
    pairs: Sequence[str],
    *,
    threshold: float = 0.12,
    weight_by_source_quality: bool = False,
) -> list[dict[str, Any]]:
    """Create one first-known directional call per topic and pair."""

    candidates: list[dict[str, Any]] = []
    for article in news.cluster_articles(articles):
        first_seen = parse_timestamp(
            article.get("causal_known_utc") or article.get("first_seen_utc")
        )
        if first_seen is None:
            continue
        scores = article.get("currency_scores") or {}
        for pair in pairs:
            base, quote = news.split_instrument(pair)
            raw_pair_score = news.safe_float(scores.get(base)) - news.safe_float(
                scores.get(quote)
            )
            source_weight = 1.0
            if weight_by_source_quality:
                source_weight = (
                    news.safe_float(article.get("source_quality"), 0.65)
                    * max(
                        0.10,
                        news.safe_float(
                            article.get("directional_confidence"),
                            0.0,
                        ),
                    )
                )
            pair_score = raw_pair_score * source_weight
            if abs(pair_score) < threshold:
                continue
            direct = set(article.get("direct_currencies") or ())
            currency_exposure_groups = sorted(
                f"{currency}:{'LONG' if news.safe_float(scores.get(currency)) > 0 else 'SHORT'}"
                for currency in direct
                if abs(news.safe_float(scores.get(currency))) > 0
            )
            direct_count = int(base in direct) + int(quote in direct)
            evidence_quality = (
                "TWO_SIDED_DIRECT"
                if direct_count == 2
                else "ONE_SIDED_DIRECT"
                if direct_count == 1
                else "GLOBAL_TOPIC_PROXY"
                if article.get("scope") == "all_pairs"
                else "INFERRED_ONLY"
            )
            candidates.append(
                {
                    "pair": pair,
                    "signal_utc": first_seen,
                    "direction": "LONG" if pair_score > 0 else "SHORT",
                    "pair_score": pair_score,
                    "raw_pair_score": raw_pair_score,
                    "source_weight": source_weight,
                    "headline": article.get("headline"),
                    "headline_key": news.normalized_headline(
                        article.get("headline")
                    ),
                    "event_id": article.get("event_id"),
                    "topic_id": article.get("topic_id") or article.get("event_id"),
                    "topic_signature": article.get("topic_signature"),
                    "topic_tags": article.get("topic_tags") or [],
                    "topic_article_count": int(article.get("topic_article_count") or 1),
                    "distinct_source_count": int(
                        article.get("distinct_source_count") or 1
                    ),
                    "evidence_quality": evidence_quality,
                    "category": article.get("category"),
                    "structured_event": bool(article.get("structured_event")),
                    "source_role": article.get("source_role"),
                    "source_name": article.get("source_name"),
                    "source_ids": article.get("source_ids")
                    or [article.get("source_id")],
                    "source_names": article.get("source_names")
                    or [article.get("source_name")],
                    "source_verified": bool(article.get("source_verified")),
                    "source_direct": bool(article.get("source_direct")),
                    "intervention_status": article.get("intervention_status"),
                    "reports_prior_market_move": bool(
                        article.get("reports_prior_market_move")
                    ),
                    "forward_signal_timely": article.get(
                        "forward_signal_timely"
                    ),
                    "official_policy_release": bool(
                        article.get("official_policy_release")
                    ),
                    "detail_available_utc": article.get("detail_available_utc"),
                    "causal_known_utc": article.get("causal_known_utc"),
                    "currency_exposure_groups": currency_exposure_groups,
                }
            )
    candidates.sort(key=lambda row: (row["signal_utc"], row["pair"]))
    output: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for row in candidates:
        topic_key = str(row.get("topic_id") or row.get("headline_key") or "")
        key = (topic_key, row["pair"])
        if not topic_key or key in seen:
            continue
        seen.add(key)
        output.append(row)
    return output


def catalyst_class(row: Mapping[str, Any]) -> str:
    """Keep economically different news populations out of one score."""

    category = str(row.get("category") or "")
    if bool(row.get("official_policy_release")):
        return "official_policy_text"
    if bool(row.get("structured_event")):
        return "scheduled_numeric_release"
    if category == "fx_intervention":
        return "official_or_reported_intervention"
    if category == "risk_off_geopolitical_or_financial":
        return "geopolitical_or_systemic_risk"
    if category == "commodity_shock":
        return "commodity_or_terms_of_trade"
    if category in {
        "inflation_release",
        "labor_release",
        "growth_release",
        "manufacturing_release",
        "business_activity_release",
        "trade_balance_release",
    }:
        return "unstructured_macro_report"
    return "other_news"


def inverse_direction_research_calls(
    calls: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Flip a frozen set of calls for a diagnostic mean-reversion ablation.

    The ablation is deliberately explicit and research-only.  It preserves
    the original story/topic identity and knowledge-time timestamp so it can
    be compared with the source interpretation on the exact same opportunity
    set.  It must not be used to authorize or route an order.
    """

    output: list[dict[str, Any]] = []
    for raw in calls:
        direction = str(raw.get("direction") or "").upper()
        if direction not in {"LONG", "SHORT"}:
            continue
        row = dict(raw)
        row["direction"] = "SHORT" if direction == "LONG" else "LONG"
        row["pair_score"] = -news.safe_float(raw.get("pair_score"), 0.0)
        row["raw_pair_score"] = -news.safe_float(
            raw.get("raw_pair_score"), 0.0
        )
        row["research_ablation"] = "INVERSE_DIRECTION"
        row["predictive_eligible"] = False
        row["execution_eligible"] = False
        output.append(row)
    return output


def official_policy_release_calls(
    calls: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Select verified direct policy-release calls without changing direction."""

    output: list[dict[str, Any]] = []
    for raw in calls:
        if not (
            bool(raw.get("official_policy_release"))
            and bool(raw.get("source_direct"))
            and bool(raw.get("source_verified"))
        ):
            continue
        row = dict(raw)
        row["research_ablation"] = "OFFICIAL_POLICY_RELEASE_ONLY"
        row["predictive_eligible"] = False
        row["execution_eligible"] = False
        output.append(row)
    return output


def fresh_catalyst_research_calls(
    calls: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Keep explicitly timely, non-recap, release-grade catalyst calls."""

    output: list[dict[str, Any]] = []
    for raw in calls:
        if bool(raw.get("reports_prior_market_move")):
            continue
        if raw.get("forward_signal_timely") is not True:
            continue
        release_grade = bool(raw.get("source_verified")) or int(
            raw.get("distinct_source_count") or 0
        ) >= 2
        if not release_grade:
            continue
        row = dict(raw)
        row["research_ablation"] = "FRESH_RELEASE_GRADE_CATALYST"
        row["predictive_eligible"] = False
        row["execution_eligible"] = False
        output.append(row)
    return output


def continuation_calls_after_price_shock(
    calls: Sequence[Mapping[str, Any]],
    candles_by_pair: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    pip_sizes: Mapping[str, float],
    lookback_minutes: int = 15,
) -> list[dict[str, Any]]:
    """Keep only first-known reports that follow an aligned completed-price shock.

    This is the chronological continuation ablation. It does not assume the
    headline caused the first wave and it never reads candles at or after the
    local first-seen timestamp.
    """

    output: list[dict[str, Any]] = []
    for raw in calls:
        if not bool(raw.get("reports_prior_market_move")):
            continue
        pair = str(raw.get("pair") or "")
        candles = list(candles_by_pair.get(pair) or ())
        if len(candles) < 2:
            continue
        times = [row["timestamp"] for row in candles]
        signal_time = raw.get("signal_utc")
        if not isinstance(signal_time, dt.datetime):
            continue
        end_index = bisect.bisect_left(times, signal_time) - 1
        start_target = signal_time - dt.timedelta(minutes=lookback_minutes)
        start_index = bisect.bisect_right(times, start_target) - 1
        if start_index < 0 or end_index <= start_index:
            continue
        start_row = candles[start_index]
        end_row = candles[end_index]
        if (signal_time - end_row["timestamp"]).total_seconds() > 300:
            continue
        pip = news.safe_float(pip_sizes.get(pair), 0.0)
        if pip <= 0:
            pip = 0.01 if pair.endswith("_JPY") else 0.0001
        start_mid = 0.5 * (
            news.safe_float(start_row.get("bid_open"))
            + news.safe_float(start_row.get("ask_open"))
        )
        end_mid = 0.5 * (
            news.safe_float(end_row.get("bid_open"))
            + news.safe_float(end_row.get("ask_open"))
        )
        signed_pips = (end_mid - start_mid) / pip
        spread_pips = (
            news.safe_float(end_row.get("ask_open"))
            - news.safe_float(end_row.get("bid_open"))
        ) / pip
        percentage_floor = start_mid * 0.00060 / pip
        threshold_pips = max(1.0, 3.0 * spread_pips, percentage_floor)
        aligned = (
            signed_pips > 0 and str(raw.get("direction")) == "LONG"
        ) or (
            signed_pips < 0 and str(raw.get("direction")) == "SHORT"
        )
        if not aligned or abs(signed_pips) < threshold_pips:
            continue
        output.append(
            {
                **dict(raw),
                "causal_relation": "CONTINUATION_SIGNAL",
                "prior_shock_lookback_minutes": lookback_minutes,
                "prior_shock_signed_pips": round(signed_pips, 6),
                "prior_shock_threshold_pips": round(threshold_pips, 6),
                "prior_shock_end_utc": news.iso_utc(end_row["timestamp"]),
                "predictive_eligible": True,
            }
        )
    return output


def restore_secondary_research_scores(
    articles: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Restore suppressed singleton macro scores for an explicit ablation.

    These rows are never used by the live publisher.  The scenario exists so
    the point-in-time report can measure whether the stricter release-grade
    source policy helped or hurt out of sample.
    """

    output: list[dict[str, Any]] = []
    for article in articles:
        row = dict(article)
        research_scores = row.get("research_currency_scores")
        if (
            not row.get("currency_scores")
            and isinstance(research_scores, Mapping)
            and research_scores
        ):
            row["currency_scores"] = dict(research_scores)
            row["directional_bias"] = {
                str(currency): (
                    "BULLISH"
                    if news.safe_float(value) > 0
                    else "BEARISH"
                )
                for currency, value in research_scores.items()
                if news.safe_float(value) != 0
            }
            row["directional_evidence"] = True
        output.append(row)
    return output


def score_call(
    call: Mapping[str, Any], candles: Sequence[Mapping[str, Any]], horizon_minutes: int,
    *, pip_size: float | None = None, pip_contract: Mapping[str, Any] | None = None,
    as_of_utc: Any = None,
) -> dict[str, Any] | None:
    horizon_minutes = positive_integer(horizon_minutes, "horizon_minutes")
    signal_time = aware_time(call.get("signal_utc"), "signal_utc")
    asof = aware_time(dt.datetime.now(UTC) if as_of_utc is None else as_of_utc, "as_of_utc")
    target_time = signal_time + dt.timedelta(minutes=horizon_minutes)
    if not candles:
        return None
    times = [aware_time(row.get("timestamp"), "candle_timestamp") for row in candles]
    if any(later <= earlier for earlier, later in zip(times, times[1:])):
        raise ValueError("news_v2_candle_clocks_must_be_unique_increasing")
    entry_index, exit_index = bisect.bisect_left(times, signal_time), bisect.bisect_left(times, target_time)
    if entry_index >= len(candles) or exit_index >= len(candles):
        return None
    entry, exit_row = candles[entry_index], candles[exit_index]
    if (times[entry_index] - signal_time).total_seconds() > 300 or (times[exit_index] - target_time).total_seconds() > 300:
        return None
    explicit_pip = pip_contract.get("pip") if pip_contract is not None else pip_size
    endpoint = endpoint_returns(
        pair=call.get("pair"), direction=call.get("direction"), horizon_minutes=horizon_minutes,
        signal_utc=signal_time, entry_utc=times[entry_index], nominal_target_utc=target_time,
        exit_utc=times[exit_index], as_of_utc=asof,
        entry_bid=entry.get("bid_open"), entry_ask=entry.get("ask_open"),
        exit_bid=exit_row.get("bid_open"), exit_ask=exit_row.get("ask_open"),
        pip_metadata={"instrument": call.get("pair"), "pip": explicit_pip} if explicit_pip is not None else None)
    if pip_contract is not None:
        if (pip_contract.get("contract") != PIP_CONTRACT
                or pip_contract.get("instrument") != endpoint["pair"]
                or not math.isclose(finite_number(pip_contract.get("pip"), "pip", positive=True), endpoint["pip_size"], rel_tol=1e-12)):
            raise ValueError("news_v2_pip_provenance_mismatch")
        endpoint["pip_contract"] = dict(pip_contract)
    long_side = endpoint["direction"] == "LONG"
    entry_price = endpoint["entry_ask"] if long_side else endpoint["entry_bid"]
    exit_price = endpoint["exit_bid"] if long_side else endpoint["exit_ask"]
    pnl_pips = ((exit_price-entry_price) if long_side else (entry_price-exit_price)) / endpoint["pip_size"]
    # The exit is an M1 open. Do not include that bar's subsequent highs/lows.
    path = candles[entry_index:exit_index]
    side = "bid" if long_side else "ask"
    path_prices = []
    fallback_count = 0
    for row in path:
        values = []
        for field in (side+"_high", side+"_low"):
            if row.get(field) is None:
                fallback_count += 1
                value = row.get(side+"_open")
            else:
                value = row[field]
            values.append(finite_number(value, field, positive=True))
        path_prices.extend(values)
    path_prices += [endpoint["entry_bid"] if long_side else endpoint["entry_ask"], exit_price]
    excursions = [((value-entry_price) if long_side else (entry_price-value))/endpoint["pip_size"] for value in path_prices]
    return {**call, **endpoint,
        "entry_price": entry_price, "exit_price": exit_price,
        "pnl_pips": round(pnl_pips, 6),
        "return_pct": round(((exit_price-entry_price) if long_side else (entry_price-exit_price))/entry_price*100.0, 9),
        "mfe_pips": round(max(excursions), 6), "mae_pips": round(min(excursions), 6),
        "excursion_contract": "M1 extrema strictly before exit open plus exact endpoints;not_tick_path_execution_proof",
        "excursion_missing_extrema_open_proxy_fields": fallback_count,
        "entry_spread_pips": round((endpoint["entry_ask"]-endpoint["entry_bid"])/endpoint["pip_size"], 6),
        "exit_spread_pips": round((endpoint["exit_ask"]-endpoint["exit_bid"])/endpoint["pip_size"], 6),
        "entry_spread_pct": round((endpoint["entry_ask"]-endpoint["entry_bid"])/entry_price*100.0, 9),
        "profitable": pnl_pips > 0}


def summarize(rows: Sequence[Mapping[str,Any]],call_count: int) -> dict[str,Any]:
    values=[];invalid=0
    for row in rows:
        if not isinstance(row,Mapping) or row.get('endpoint_contract')!=ENDPOINT_CONTRACT:
            invalid+=1;continue
        try:
            recomputed=endpoint_returns(pair=row.get('pair'),direction=row.get('direction'),horizon_minutes=row.get('horizon_minutes'),
                signal_utc=row.get('signal_utc'),entry_utc=row.get('entry_utc'),nominal_target_utc=row.get('nominal_target_utc'),
                exit_utc=row.get('exit_utc'),as_of_utc=row.get('as_of_utc'),entry_bid=row.get('entry_bid'),entry_ask=row.get('entry_ask'),
                exit_bid=row.get('exit_bid'),exit_ask=row.get('exit_ask'),pip_metadata={'pip':row.get('pip_size')})
            values.append(recomputed['follow_net_bps'])
        except (TypeError,ValueError):invalid+=1
    profitable=sum(value>0 for value in values)
    return {'directional_calls':call_count,'scored_calls':len(rows),'price_coverage':round(len(rows)/call_count,6) if call_count else 0.,
        'valid_exact_endpoint_return_calls':len(values),'invalid_exact_endpoint_return_calls':invalid,
        'return_unit':'bps','return_method':'chosen_side_four_exact_quotes_divided_by_common_entry_mid_notional;equal_notional_per_call',
        'profitable_calls':profitable,'profitable_after_spread_fraction':profitable/len(values) if values else None,
        'average_executable_bps':math.fsum(values)/len(values) if values else None,
        'median_executable_bps':statistics.median(values) if values else None,
        'sum_executable_bps':math.fsum(values) if values else None,
        'sum_scope':'sum_of_normalized_call_returns;not_compounded_portfolio_return',
        'native_pip_aggregation_scope':'native_pips_retained_per_call_and_pair_only;not_cross_pair_comparable'}


def normalized_return_summary(values: Sequence[float]) -> dict[str, Any]:
    """Summarize normalized returns without letting one outlier define an edge."""

    returns = [float(value) for value in values if math.isfinite(float(value))]
    if not returns:
        return {
            "observations": 0,
            "profitable_observations": 0,
            "win_rate": None,
            "average_return_pct": None,
            "median_return_pct": None,
            "trimmed_5pct_average_return_pct": None,
            "leave_best_out_average_return_pct": None,
            "best_return_pct": None,
            "worst_return_pct": None,
        }
    ordered = sorted(returns)
    trim_count = int(len(ordered) * 0.05)
    trimmed = (
        ordered[trim_count:-trim_count]
        if trim_count > 0 and len(ordered) > 2 * trim_count
        else ordered
    )
    without_best = list(returns)
    without_best.remove(max(without_best))
    return {
        "observations": len(returns),
        "profitable_observations": sum(value > 0 for value in returns),
        "win_rate": round(sum(value > 0 for value in returns) / len(returns), 6),
        "average_return_pct": round(statistics.fmean(returns), 9),
        "median_return_pct": round(statistics.median(returns), 9),
        "trimmed_5pct_average_return_pct": round(
            statistics.fmean(trimmed),
            9,
        ),
        "leave_best_out_average_return_pct": (
            round(statistics.fmean(without_best), 9) if without_best else None
        ),
        "best_return_pct": round(max(returns), 9),
        "worst_return_pct": round(min(returns), 9),
    }


def multi_horizon_topic_paths(
    rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Describe event-time continuation/reversal paths after pair fan-out."""

    grouped: dict[str, dict[int, list[float]]] = defaultdict(
        lambda: defaultdict(list)
    )
    headlines: dict[str, str] = {}
    for row in rows:
        topic_id = str(row.get("topic_id") or row.get("event_id") or "")
        horizon = int(row.get("horizon_minutes") or 0)
        if not topic_id or horizon <= 0:
            continue
        grouped[topic_id][horizon].append(
            news.safe_float(row.get("return_pct"))
        )
        headlines.setdefault(topic_id, str(row.get("headline") or ""))
    output: list[dict[str, Any]] = []
    for topic_id, horizon_values in sorted(grouped.items()):
        returns = {
            horizon: statistics.fmean(values)
            for horizon, values in horizon_values.items()
            if values
        }
        if not returns:
            continue
        ordered = sorted(returns)
        shortest = returns[ordered[0]]
        longest = returns[ordered[-1]]
        middle = [returns[horizon] for horizon in ordered[1:-1]]
        if shortest > 0 and longest > 0 and any(value <= 0 for value in middle):
            path = "initial_move_then_reversal_then_recovery"
        elif shortest > 0 and longest > 0:
            path = "persistent_continuation"
        elif shortest <= 0 and longest > 0:
            path = "delayed_continuation"
        elif shortest > 0 and longest <= 0:
            path = "initial_move_then_fade"
        else:
            path = "mapped_direction_rejected"
        output.append(
            {
                "topic_id": topic_id,
                "headline": headlines.get(topic_id, ""),
                "pair_leg_count": max(len(values) for values in horizon_values.values()),
                "path_class": path,
                "equal_weight_return_pct_by_horizon": {
                    str(horizon): round(returns[horizon], 9)
                    for horizon in ordered
                },
                "research_only": True,
                "execution_eligible": False,
            }
        )
    return output


def _pretrade_pair_rank(row: Mapping[str, Any]) -> tuple[float, float, str]:
    """Rank a pair using only values available when the news call was made."""

    return (
        -abs(news.safe_float(row.get("pair_score"))),
        news.safe_float(row.get("entry_spread_pct"), 1e9),
        str(row.get("pair") or ""),
    )


def _direction_maps_agree(
    left: Mapping[str, str],
    right: Mapping[str, str],
    *,
    minimum_agreement: float = EVENT_CLUSTER_DIRECTION_AGREEMENT,
) -> bool:
    common = set(left).intersection(right)
    if not common:
        return False
    agreement = sum(left[pair] == right[pair] for pair in common) / len(common)
    return agreement >= minimum_agreement


def event_cluster_outcome_summary(
    rows: Sequence[Mapping[str, Any]],
    *,
    cluster_window_minutes: int = EVENT_CLUSTER_WINDOW_MINUTES,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Collapse pair fan-out and overlapping headlines into causal episodes.

    Each topic first becomes one equal-weight pair basket plus one deterministic
    pre-trade pair selected by absolute call strength and entry spread. Topics
    within the fixed time window are the same episode only when their pair-side
    maps agree. The episode score uses its first-known topic, preventing a later
    headline from receiving separate credit for the same market move.
    """

    topic_groups: dict[tuple[int, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        horizon = int(row.get("horizon_minutes") or 0)
        topic_id = str(row.get("topic_id") or row.get("event_id") or "")
        if horizon <= 0 or not topic_id:
            continue
        topic_groups[(horizon, topic_id)].append(row)

    topics_by_horizon: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for (horizon, topic_id), group in sorted(topic_groups.items()):
        ordered_group = sorted(group, key=_pretrade_pair_rank)
        selected = ordered_group[0]
        signal_times = [
            parsed
            for row in group
            if (parsed := parse_timestamp(row.get("signal_utc"))) is not None
        ]
        if not signal_times:
            continue
        returns = [news.safe_float(row.get("return_pct")) for row in group]
        topics_by_horizon[horizon].append(
            {
                "topic_id": topic_id,
                "signal_time": min(signal_times),
                "headline": str(selected.get("headline") or ""),
                "category": str(selected.get("category") or ""),
                "catalyst_class": catalyst_class(selected),
                "pair_leg_count": len(group),
                "direction_map": {
                    str(row.get("pair") or ""): str(row.get("direction") or "")
                    for row in group
                    if row.get("pair") and row.get("direction")
                },
                "equal_weight_pair_basket_return_pct": statistics.fmean(returns),
                "selected_pair": str(selected.get("pair") or ""),
                "selected_pair_return_pct": news.safe_float(
                    selected.get("return_pct")
                ),
            }
        )

    summaries: dict[str, Any] = {}
    episode_rows: list[dict[str, Any]] = []
    window = dt.timedelta(minutes=max(1, int(cluster_window_minutes)))
    for horizon, topics in sorted(topics_by_horizon.items()):
        clusters: list[dict[str, Any]] = []
        for topic in sorted(topics, key=lambda row: row["signal_time"]):
            matching_cluster = None
            for cluster in reversed(clusters):
                if topic["signal_time"] - cluster["start_time"] > window:
                    break
                if any(
                    _direction_maps_agree(
                        topic["direction_map"],
                        member["direction_map"],
                    )
                    for member in cluster["members"]
                ):
                    matching_cluster = cluster
                    break
            if matching_cluster is None:
                clusters.append(
                    {
                        "start_time": topic["signal_time"],
                        "members": [topic],
                    }
                )
            else:
                matching_cluster["members"].append(topic)

        topic_basket_returns = [
            topic["equal_weight_pair_basket_return_pct"] for topic in topics
        ]
        topic_selected_returns = [
            topic["selected_pair_return_pct"] for topic in topics
        ]
        event_basket_returns: list[float] = []
        event_selected_returns: list[float] = []
        for index, cluster in enumerate(clusters, start=1):
            members = sorted(cluster["members"], key=lambda row: row["signal_time"])
            first = members[0]
            event_basket_returns.append(
                first["equal_weight_pair_basket_return_pct"]
            )
            event_selected_returns.append(first["selected_pair_return_pct"])
            episode_rows.append(
                {
                    "event_cluster_id": f"H{horizon}_{index:04d}",
                    "horizon_minutes": horizon,
                    "cluster_start_utc": news.iso_utc(first["signal_time"]),
                    "cluster_end_utc": news.iso_utc(
                        max(member["signal_time"] for member in members)
                    ),
                    "topic_count": len(members),
                    "topic_ids": [member["topic_id"] for member in members],
                    "first_topic_id": first["topic_id"],
                    "first_headline": first["headline"],
                    "first_category": first["category"],
                    "first_catalyst_class": first["catalyst_class"],
                    "first_topic_pair_leg_count": first["pair_leg_count"],
                    "first_topic_equal_weight_pair_basket_return_pct": round(
                        first["equal_weight_pair_basket_return_pct"],
                        9,
                    ),
                    "first_topic_selected_pair": first["selected_pair"],
                    "first_topic_selected_pair_return_pct": round(
                        first["selected_pair_return_pct"],
                        9,
                    ),
                    "research_only": True,
                    "execution_eligible": False,
                }
            )

        summaries[str(horizon)] = {
            "pair_leg_count": sum(topic["pair_leg_count"] for topic in topics),
            "topic_count": len(topics),
            "event_cluster_count": len(clusters),
            "cluster_policy": {
                "window_minutes": int(cluster_window_minutes),
                "minimum_common_pair_direction_agreement": (
                    EVENT_CLUSTER_DIRECTION_AGREEMENT
                ),
                "episode_scoring": "first_known_topic_only",
            },
            "topic_equal_weight_pair_baskets": normalized_return_summary(
                topic_basket_returns
            ),
            "topic_pretrade_selected_single_pairs": normalized_return_summary(
                topic_selected_returns
            ),
            "event_first_topic_equal_weight_pair_baskets": (
                normalized_return_summary(event_basket_returns)
            ),
            "event_first_topic_pretrade_selected_single_pairs": (
                normalized_return_summary(event_selected_returns)
            ),
        }
    return summaries, episode_rows


def episode_summary_by_catalyst_class(
    episode_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Summarize factor/event-collapsed episodes by source economics."""

    grouped: dict[tuple[int, str], list[float]] = defaultdict(list)
    for row in episode_rows:
        horizon = int(row.get("horizon_minutes") or 0)
        catalyst = str(row.get("first_catalyst_class") or "other_news")
        if horizon <= 0:
            continue
        grouped[(horizon, catalyst)].append(
            news.safe_float(
                row.get("first_topic_equal_weight_pair_basket_return_pct")
            )
        )
    return [
        {
            "horizon_minutes": horizon,
            "catalyst_class": catalyst,
            **normalized_return_summary(values),
            "research_only": True,
            "execution_eligible": False,
        }
        for (horizon, catalyst), values in sorted(grouped.items())
    ]


def topic_outcome_summary(
    rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, int], list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        tags = row.get("topic_tags") or ["#untagged"]
        for tag in tags:
            groups[
                (
                    str(tag),
                    str(row.get("pair") or ""),
                    int(row.get("horizon_minutes") or 0),
                )
            ].append(row)
    output: list[dict[str, Any]] = []
    for (tag, pair, horizon), group in sorted(groups.items()):
        pips = [news.safe_float(row.get("pnl_pips")) for row in group]
        output.append(
            {
                "topic_tag": tag,
                "pair": pair,
                "horizon_minutes": horizon,
                "sample_size": len(group),
                "profitable_after_spread_fraction": round(
                    sum(value > 0 for value in pips) / len(pips),
                    6,
                ),
                "average_executable_pips": round(statistics.fmean(pips), 6),
                "median_executable_pips": round(statistics.median(pips), 6),
                "average_mfe_pips": round(
                    statistics.fmean(
                        news.safe_float(row.get("mfe_pips")) for row in group
                    ),
                    6,
                ),
                "average_mae_pips": round(
                    statistics.fmean(
                        news.safe_float(row.get("mae_pips")) for row in group
                    ),
                    6,
                ),
            }
        )
    return output


def source_outcome_summary(
    rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, int], list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        source_ids = [
            str(value)
            for value in row.get("source_ids") or ()
            if str(value or "")
        ] or ["unknown"]
        for source_id in source_ids:
            groups[
                (
                    source_id,
                    str(row.get("pair") or ""),
                    int(row.get("horizon_minutes") or 0),
                )
            ].append(row)
    output: list[dict[str, Any]] = []
    for (source_id, pair, horizon), group in sorted(groups.items()):
        pips = [news.safe_float(row.get("pnl_pips")) for row in group]
        output.append(
            {
                "source_id": source_id,
                "pair": pair,
                "horizon_minutes": horizon,
                "sample_size": len(group),
                "profitable_after_spread_fraction": round(
                    sum(value > 0 for value in pips) / len(pips),
                    6,
                ),
                "average_executable_pips": round(statistics.fmean(pips), 6),
                "median_executable_pips": round(statistics.median(pips), 6),
                "average_mfe_pips": round(
                    statistics.fmean(
                        news.safe_float(row.get("mfe_pips")) for row in group
                    ),
                    6,
                ),
                "average_mae_pips": round(
                    statistics.fmean(
                        news.safe_float(row.get("mae_pips")) for row in group
                    ),
                    6,
                ),
            }
        )
    return output


def currency_basket_outcome_summary(
    rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, int], list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        topic_id = str(row.get("topic_id") or row.get("event_id") or "")
        horizon = int(row.get("horizon_minutes") or 0)
        for exposure in row.get("currency_exposure_groups") or ():
            groups[(topic_id, str(exposure), horizon)].append(row)
    output: list[dict[str, Any]] = []
    for (topic_id, exposure, horizon), group in sorted(groups.items()):
        ranked = sorted(
            group,
            key=lambda row: (
                -abs(news.safe_float(row.get("pair_score"))),
                news.safe_float(row.get("entry_spread_pct"), 1e9),
                str(row.get("pair") or ""),
            ),
        )
        top_three = ranked[:3]
        returns = [news.safe_float(row.get("return_pct")) for row in group]
        top_three_returns = [
            news.safe_float(row.get("return_pct")) for row in top_three
        ]
        output.append(
            {
                "topic_id": topic_id,
                "currency_exposure_group": exposure,
                "horizon_minutes": horizon,
                "leg_count": len(group),
                "selection_policy": (
                    "absolute_first_known_pair_score_then_entry_spread"
                ),
                "selected_single_pair": ranked[0].get("pair"),
                "selected_single_return_pct": round(
                    news.safe_float(ranked[0].get("return_pct")),
                    9,
                ),
                "equal_weight_top3_pairs": [
                    row.get("pair") for row in top_three
                ],
                "equal_weight_top3_return_pct": round(
                    statistics.fmean(top_three_returns),
                    9,
                ),
                "equal_weight_all_legs_return_pct": round(
                    statistics.fmean(returns),
                    9,
                ),
                "hindsight_best_pair": max(
                    group,
                    key=lambda row: news.safe_float(row.get("return_pct")),
                ).get("pair"),
                "hindsight_best_return_pct": round(max(returns), 9),
                "research_only": True,
                "execution_eligible": False,
            }
        )
    return output


def classifier_audit(
    baseline: Sequence[Mapping[str, Any]],
    current: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    changed = 0
    generic_risk_suppressed = 0
    failed_deescalations_corrected = 0
    official_neutral_retained = 0
    for before, after in zip(baseline, current):
        comparison_fields = (
            "relevant",
            "context_only",
            "category",
            "currency_scores",
            "risk_off_score",
            "risk_on_score",
        )
        if any(before.get(field) != after.get(field) for field in comparison_fields):
            changed += 1
        if (
            news.safe_float(before.get("risk_off_score")) > 0
            and news.safe_float(after.get("risk_off_score")) == 0
        ):
            generic_risk_suppressed += 1
        if (
            news.safe_float(before.get("risk_on_score")) > 0
            and news.safe_float(after.get("risk_on_score")) == 0
            and news.safe_float(after.get("risk_off_score")) > 0
        ):
            failed_deescalations_corrected += 1
        if (
            bool(after.get("official_policy_release"))
            and bool(after.get("relevant"))
            and not after.get("currency_scores")
        ):
            official_neutral_retained += 1
    clustered = news.cluster_articles(current)
    return {
        "retained_articles": len(baseline),
        "changed_classifications": changed,
        "generic_risk_false_positives_suppressed": generic_risk_suppressed,
        "failed_deescalations_corrected": failed_deescalations_corrected,
        "official_neutral_policy_releases_retained": official_neutral_retained,
        "published_topic_clusters": len(clustered),
        "semantic_or_syndication_rows_collapsed": len(current) - len(clustered),
        "clusters_with_distinct_source_corroboration": sum(
            int(row.get("corroboration_count") or 0) > 0 for row in clustered
        ),
        "repeat_poll_observations": sum(
            int(row.get("duplicate_observation_count") or 0)
            for row in current
        ),
    }


def run_backtest(
    *,
    database: Path = DEFAULT_DATABASE,
    config_path: Path = DEFAULT_CONFIG,
    candle_root: Path = DEFAULT_CANDLE_ROOT,
    report_path: Path | None = None,
    instrument_metadata_path: Path = DEFAULT_INSTRUMENT_METADATA,
    pairs: Sequence[str] = DEFAULT_PAIRS,
    horizons: Sequence[int] = (15, 60, 240),
    since: dt.datetime,
    reclassification_cache: Path = DEFAULT_RECLASSIFICATION_CACHE,
    candle_refresh_result: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    report_path=new_run_report_path(DEFAULT_REPORT) if report_path is None else Path(report_path)
    if Path(report_path).exists():
        raise ValueError("refuse_overwriting_existing_news_report;legacy_news_replay_report_targets_also_forbidden")
    scoring_as_of = dt.datetime.now(UTC).isoformat()
    pip_contracts,metadata_identity = _load_pip_contracts_snapshot(instrument_metadata_path)
    pip_sizes = {pair: receipt["pip"] for pair, receipt in pip_contracts.items()}
    baseline = load_articles(database, since=since)
    current_articles, cache_audit = reclassify_articles_cached(
        baseline,
        config_path=config_path,
        cache_path=reclassification_cache,
    )
    current_topics = news.cluster_articles(current_articles)
    archived_topics = load_topic_history(database, since=since)
    current_topic_ids = {
        str(row.get("topic_id") or "") for row in current_topics
    }
    current = [
        *current_topics,
        *[
            row
            for row in archived_topics
            if str(row.get("topic_id") or "") not in current_topic_ids
        ],
    ]
    scenarios: dict[str, tuple[Sequence[Mapping[str, Any]], dict[str, Any]]] = {
        "stored_baseline": (baseline, {}),
        "cleaned_secondary_permissive_research": (
            restore_secondary_research_scores(current),
            {},
        ),
        "cleaned_current": (current, {}),
        "cleaned_source_quality_weighted": (
            current,
            {"weight_by_source_quality": True},
        ),
        "no_news_control": ((), {}),
    }
    calls = {
        scenario: directional_calls(articles, pairs, **options)
        for scenario, (articles, options) in scenarios.items()
    }
    calls["cleaned_inverse_direction_research"] = (
        inverse_direction_research_calls(calls.get("cleaned_current") or ())
    )
    # Official document details are deliberately research-only when their full
    # text arrived after the normal forward-reaction window.  Restore those
    # scores solely for this causal event-time diagnostic; the filter remains
    # non-predictive and non-executable.
    secondary_research_base = directional_calls(
        restore_secondary_research_scores(current),
        pairs,
    )
    calls["cleaned_official_policy_release_only"] = official_policy_release_calls(
        secondary_research_base
    )
    calls["cleaned_fresh_catalyst_only"] = fresh_catalyst_research_calls(
        secondary_research_base
    )
    all_signal_times = [
        row["signal_utc"]
        for version_calls in calls.values()
        for row in version_calls
    ]
    candle_since = (
        min(all_signal_times) - dt.timedelta(minutes=15)
        if all_signal_times
        else since
    )
    candles: dict[str, list[dict[str, Any]]] = {}
    candle_ranges: dict[str, Any] = {}
    for pair in pairs:
        path = candle_root / f"{pair}_M1.csv"
        rows = load_candles(path, since=candle_since) if path.exists() else []
        candles[pair] = rows
        candle_ranges[pair] = {
            "rows": len(rows),
            "first_utc": news.iso_utc(rows[0]["timestamp"]) if rows else None,
            "last_utc": news.iso_utc(rows[-1]["timestamp"]) if rows else None,
        }
    calls["cleaned_continuation_after_price_shock"] = (
        continuation_calls_after_price_shock(
            calls.get("cleaned_current") or (),
            candles,
            pip_sizes=pip_sizes,
        )
    )

    results: dict[str, Any] = {}
    detail_rows: list[dict[str, Any]] = []
    for version, version_calls in calls.items():
        by_horizon: dict[str, Any] = {}
        for horizon in horizons:
            scored = [
                result
                for call in version_calls
                if (
                    result := score_call(
                        call,
                        candles.get(str(call["pair"])) or [],
                        int(horizon),
                        pip_size=pip_sizes.get(str(call["pair"])),
                        pip_contract=pip_contracts.get(str(call["pair"])),
                        as_of_utc=scoring_as_of,
                    )
                )
                is not None
            ]
            for row in scored:
                detail_rows.append({"version": version, **row})
            by_horizon[str(horizon)] = summarize(scored, len(version_calls))
        results[version] = {
            "classifier_version": (
                news.CLASSIFICATION_VERSION
                if version.startswith("cleaned_")
                else "stored_payload_versions"
            ),
            "directional_call_count": len(version_calls),
            "by_horizon_minutes": by_horizon,
        }

    cleaned_detail_rows = [
        row for row in detail_rows if row.get("version") == "cleaned_current"
    ]
    robust_horizon_validation, event_cluster_outcomes = (
        event_cluster_outcome_summary(cleaned_detail_rows)
    )
    inverse_detail_rows = [
        row
        for row in detail_rows
        if row.get("version") == "cleaned_inverse_direction_research"
    ]
    inverse_horizon_validation, inverse_event_cluster_outcomes = (
        event_cluster_outcome_summary(inverse_detail_rows)
    )
    official_policy_detail_rows = [
        row
        for row in detail_rows
        if row.get("version") == "cleaned_official_policy_release_only"
    ]
    official_policy_horizon_validation, official_policy_event_clusters = (
        event_cluster_outcome_summary(official_policy_detail_rows)
    )
    fresh_catalyst_detail_rows = [
        row
        for row in detail_rows
        if row.get("version") == "cleaned_fresh_catalyst_only"
    ]
    fresh_catalyst_horizon_validation, fresh_catalyst_event_clusters = (
        event_cluster_outcome_summary(fresh_catalyst_detail_rows)
    )
    report = {
        "schema_version": SCHEMA_VERSION,
        "endpoint_contract": ENDPOINT_CONTRACT,
        "scoring_as_of_utc": scoring_as_of,
        "reaction_input_unit": "exact_four_quote_endpoints;bps_recomputed_by_v2_consumer",
        "instrument_metadata_source": metadata_identity,
        "output_path":str(report_path.absolute()),
        "publication_contract":"immutable_new_run_no_overwrite",
        "generated_utc": news.iso_utc(),
        "status": "ok",
        "policy": {
            "research_only": True,
            "execution_eligible": False,
            "broker_calls": 0,
            "orders_placed": 0,
            "chronological_first_seen_only": True,
            "bid_ask_spread_included": True,
            "source_release_grade_ablation": True,
            "source_quality_weighted_ablation": True,
            "inverse_direction_research_ablation": True,
            "official_policy_release_only_ablation": True,
            "fresh_catalyst_only_ablation": True,
            "continuation_after_completed_price_shock_ablation": True,
            "no_news_control": True,
            "live_execution_gates_applied": False,
        },
        "since_utc": news.iso_utc(since),
        "pairs": list(pairs),
        "horizons_minutes": list(horizons),
        "instrument_metadata": {
            "path": str(instrument_metadata_path),
            "pip_size_count": len(pip_sizes),
            "all_pairs_covered": set(pairs).issubset(set(pip_sizes)),
        },
        "classifier_audit": classifier_audit(baseline, current_articles),
        "reclassification_cache": cache_audit,
        "historical_topic_count": len(current),
        "archived_topic_count": len(archived_topics),
        "results": results,
        "scenario_contract": {
            "stored_baseline": "historically stored classifier payloads",
            "cleaned_secondary_permissive_research": (
                "current rules with suppressed singleton secondary macro "
                "scores restored for research only"
            ),
            "cleaned_current": (
                "current release-grade policy: verified publisher or "
                "distinct-source corroboration for macro direction"
            ),
            "cleaned_source_quality_weighted": (
                "cleaned current direction scaled by source quality and "
                "directional confidence before thresholding"
            ),
            "cleaned_inverse_direction_research": (
                "research-only exact direction flip of cleaned_current on "
                "the same first-known topic/pair opportunity set; never "
                "eligible for authorization or execution"
            ),
            "cleaned_official_policy_release_only": (
                "verified direct official policy releases scored from their "
                "causal-known timestamp; research-only event-time control"
            ),
            "cleaned_fresh_catalyst_only": (
                "explicitly timely non-recap verified/corroborated catalysts; "
                "research-only event-time control"
            ),
            "cleaned_continuation_after_price_shock": (
                "first-known intervention reports that explicitly describe a "
                "prior move and follow an aligned completed 15-minute M1 shock"
            ),
            "no_news_control": "no news-triggered trades",
        },
        "candle_ranges": candle_ranges,
        "scored_call_details": detail_rows,
        "topic_pair_horizon_outcomes": topic_outcome_summary(
            [
                row
                for row in detail_rows
                if row.get("version") == "cleaned_current"
            ]
        ),
        "source_pair_horizon_outcomes": source_outcome_summary(
            cleaned_detail_rows
        ),
        "robust_horizon_validation": robust_horizon_validation,
        "event_cluster_outcomes": event_cluster_outcomes,
        "direction_ablation_validation": {
            "mapped_direction": robust_horizon_validation,
            "inverse_direction_research": inverse_horizon_validation,
            "selection_warning": (
                "The inverse arm is a same-window discovery ablation. Any "
                "apparently positive cell requires a frozen definition, "
                "factor/episode deduplication, and untouched prospective "
                "confirmation before it can be considered evidence."
            ),
        },
        "inverse_event_cluster_outcomes": inverse_event_cluster_outcomes,
        "official_policy_horizon_validation": (
            official_policy_horizon_validation
        ),
        "official_policy_event_clusters": official_policy_event_clusters,
        "official_policy_multi_horizon_paths": multi_horizon_topic_paths(
            official_policy_detail_rows
        ),
        "fresh_catalyst_horizon_validation": (
            fresh_catalyst_horizon_validation
        ),
        "fresh_catalyst_event_clusters": fresh_catalyst_event_clusters,
        "fresh_catalyst_by_class": episode_summary_by_catalyst_class(
            fresh_catalyst_event_clusters
        ),
        "continuation_currency_basket_outcomes": currency_basket_outcome_summary(
            [
                row
                for row in detail_rows
                if row.get("version")
                == "cleaned_continuation_after_price_shock"
            ]
        ),
    }
    if candle_refresh_result is not None:report["candle_refresh"]=dict(candle_refresh_result)
    publish_new_json_report(report_path,report)
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--candle-root", type=Path, default=DEFAULT_CANDLE_ROOT)
    parser.add_argument(
        "--instrument-metadata",
        type=Path,
        default=DEFAULT_INSTRUMENT_METADATA,
    )
    parser.add_argument("--report", type=Path, default=None, help="Absent target path; default creates immutable unique run report")
    parser.add_argument(
        "--reclassification-cache",
        type=Path,
        default=DEFAULT_RECLASSIFICATION_CACHE,
    )
    parser.add_argument("--pairs", nargs="*", default=list(DEFAULT_PAIRS))
    parser.add_argument("--horizons", nargs="*", type=int, default=[15, 60, 240])
    parser.add_argument("--since", default="2026-07-28T00:00:00Z")
    parser.add_argument("--refresh-candles", action="store_true")
    parser.add_argument("--creds", type=Path, default=DEFAULT_CREDS)
    parser.add_argument("--max-candle-requests", type=int, default=8)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    since = parse_timestamp(args.since)
    if since is None:
        raise SystemExit(f"invalid --since: {args.since}")
    refresh_result = None
    if args.refresh_candles:
        refresh_result = refresh_candles(
            pairs=args.pairs,
            candle_root=args.candle_root,
            creds_path=args.creds,
            max_requests=args.max_candle_requests,
        )
    report = run_backtest(
        database=args.database,
        config_path=args.config,
        candle_root=args.candle_root,
        instrument_metadata_path=args.instrument_metadata,
        report_path=args.report,
        pairs=args.pairs,
        horizons=args.horizons,
        since=since,
        reclassification_cache=args.reclassification_cache,
        candle_refresh_result=refresh_result,
    )
    print(
        json.dumps(
            {
                "status": report["status"],
                "classifier_audit": report["classifier_audit"],
                "results": report["results"],
                "report": report["output_path"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
