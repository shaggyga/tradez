"""Bounded local research feature producer; no broker, model, fit or order path.

Reads named candle tails and the dedicated quote JSON only. A source read is
recorded as a new observation of its original values, never a new source clock.
Missing/stale inputs remain explicit. No historical files are deleted.
"""
from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import io
import json
import math
import os
from pathlib import Path
import re
import shutil
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import oanda_research_feature_calculator_v1 as calculator
from oanda_feature_observations_v1 import PURE_QUOTE_FEATURES_V1, archive_observation_snapshot, build_observation_frame, canonical_bytes, capture_feature_group, observation_source_sha256, parse_utc, payload_sha256
from oanda_feature_research_clock_v1 import read_verified_clock, validate_clock_state

ROOT = Path(__file__).resolve().parent
DATA_ROOT = ROOT / "data" / "oanda_training_manager"
WORKER = "oanda_research_feature_observation_worker_v1"
SCHEMA = "research_existing_feature_observations_v1_20260913"
FLAGS = dict(research_only=True, can_place_orders=False, can_authorize=False, can_promote=False, account_eligible=False)
MAX_PAIRS = 68
MAX_ROWS = 1024
MAX_TAIL_BYTES = 256 * 1024
MAX_SOURCE_BYTES = 64 * 1024 * 1024
MAX_ENVELOPE_BYTES = 24 * 1024 * 1024
NATIVE_SECONDS = {"M1": 60, "M5": 300, "H1": 3600}
GROUP_SECONDS = {"M1": 60, "M5": 300, "M10": 600, "M15": 900, "M30": 1800, "H1": 3600, "H2": 7200, "H3": 10800, "H4": 14400}
PAIR = re.compile(r"[A-Z]{3}_[A-Z]{3}")
NEWS_NAMES = ("context_balance", "context_volume_log", "context_signed_fraction", "context_mean_age_hours", "vetted_balance", "vetted_volume_log", "vetted_conflict_fraction", "vetted_remaining_hours")


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def checked_path(path):
    path = Path(path).absolute()
    if os.name == "nt" and path.drive.upper() != "C:":
        raise ValueError("C_drive_required")
    if any(item.is_symlink() or (hasattr(item, "is_junction") and item.is_junction()) for item in (path, *path.parents)):
        raise ValueError("reparse_path_refused")
    return path


def atomic_json(path, payload):
    path = checked_path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name("." + uuid.uuid4().hex + ".tmp")
    try:
        temporary.write_bytes(canonical_bytes(payload))
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def immutable_json(path, payload):
    path = checked_path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name("." + uuid.uuid4().hex + ".tmp")
    try:
        temporary.write_bytes(canonical_bytes(payload))
        os.link(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def require_verified_clock(path, *, clock=utc_now):
    verdict = read_verified_clock(path, now_epoch=parse_utc(clock()).timestamp())
    if not verdict["valid"]:
        raise ValueError("research_clock_unverified:" + verdict["reason"])
    return verdict


def read_json_source(path, *, max_bytes=4 * 1024 * 1024, clock=utc_now):
    path = checked_path(path)
    before = path.stat()
    if before.st_size > max_bytes:
        raise ValueError("source_json_byte_bound")
    with path.open("rb") as handle:
        raw = handle.read(max_bytes + 1)
    observed = clock()
    after = path.stat()
    if len(raw) > max_bytes or (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise ValueError("source_json_changed_or_exceeded_bound")
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError("source_json_object_required")
    return payload, {"source_name": path.name, "source_bytes": len(raw), "source_sha256": hashlib.sha256(raw).hexdigest(), "source_read_completed_utc": observed}


def _number(value):
    number = float(value)
    if not math.isfinite(number) or abs(number) > 1e12:
        raise ValueError("finite_bounded_source_number_required")
    return number


def capture_scalar_group(values, *, input_timeframe, clock):
    """Retain every scalar without copying/re-hashing source price arrays.

    Source CSV tail receipts retain input identity. Tiny temporary tails let
    the existing missing/default classifier inspect its exact lookback; those
    tails are excluded from the persisted envelope and declared by length.
    """
    compact = {name: value for name, value in values.items() if value is None or isinstance(value, (str, bool, int, float))}
    for name, limit in (("closes", 20), ("volumes", 31), ("spread_history_pips", 13)):
        if isinstance(values.get(name), (list, tuple)):
            compact[name] = values[name][-limit:]
    if isinstance(values.get("series_origins"), dict):
        compact["series_origins"] = values["series_origins"]
    result = capture_feature_group(compact, input_timeframe=input_timeframe, clock=clock)
    for name in ("closes", "volumes", "spread_history_pips"):
        result["nested_lineage"].pop(name, None)
    result["source_array_fields"] = {name: len(value) for name, value in values.items() if isinstance(value, (list, tuple))}
    result["array_storage"] = "not_copied; named_source_tail_receipts_and_array_lengths_retained"
    return result


def read_candle_tail(path, pair, timeframe, *, clock=utc_now):
    path = checked_path(path)
    before = path.stat()
    with path.open("rb") as handle:
        header = handle.readline(4097)
        if len(header) > 4096 or not header.endswith(b"\n"):
            raise ValueError("candle_header_bound")
        offset = max(len(header), before.st_size - MAX_TAIL_BYTES)
        handle.seek(offset)
        raw = handle.read(MAX_TAIL_BYTES + 1)
    observed = clock()
    after = path.stat()
    if len(raw) > MAX_TAIL_BYTES or (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise ValueError("candle_source_changed_or_exceeded_bound")
    if raw and not raw.endswith(b"\n"):
        raise ValueError("candle_partial_last_record")
    if offset > len(header):
        raw = raw.split(b"\n", 1)[-1]
    fields = next(csv.reader([header.decode("utf-8-sig").strip()]))
    if len(fields) != len(set(fields)) or not {"instrument", "granularity", "open", "high", "low", "close", "volume"}.issubset(fields):
        raise ValueError("candle_schema_unsupported")
    rows = list(csv.DictReader(io.StringIO(raw.decode("utf-8")), fieldnames=fields))
    rows = rows[-MAX_ROWS:]
    cutoff = parse_utc(observed)
    if cutoff is None:
        raise ValueError("source_read_clock_required")
    parsed = []
    for row in rows:
        if None in row or any(value is None for value in row.values()) or row["instrument"] != pair or row["granularity"] != timeframe:
            raise ValueError("candle_row_identity_or_width_invalid")
        clocks = [parse_utc(row[key]) for key in ("time", "datetime") if key in row]
        if not clocks or any(stamp is None for stamp in clocks) or len(set(clocks)) != 1:
            raise ValueError("candle_original_clock_invalid")
        stamp = clocks[0]
        seconds = NATIVE_SECONDS[timeframe]
        if stamp.timestamp() % seconds or stamp + timedelta(seconds=seconds) > cutoff:
            raise ValueError("candle_unaligned_or_incomplete_at_read")
        candle = {"time": stamp.isoformat(), "complete": True, "volume": _number(row["volume"])}
        if candle["volume"] < 0:
            raise ValueError("candle_volume_negative")
        for side, prefix in (("mid", ""), ("bid", "bid_"), ("ask", "ask_")):
            values = {short: _number(row[prefix + long]) for short, long in (("o", "open"), ("h", "high"), ("l", "low"), ("c", "close"))}
            if not 0 < values["l"] <= min(values["o"], values["c"]) <= max(values["o"], values["c"]) <= values["h"]:
                raise ValueError("candle_ohlc_invalid")
            candle[side] = values
        if candle["ask"]["c"] < candle["bid"]["c"]:
            raise ValueError("candle_spread_invalid")
        if parsed and stamp <= parse_utc(parsed[-1]["time"]):
            raise ValueError("candle_duplicate_or_unsorted")
        parsed.append(candle)
    # A gap ends supported rolling history; it is never bridged or filled.
    suffix = len(parsed) - 1
    while suffix > 0 and (parse_utc(parsed[suffix]["time"]) - parse_utc(parsed[suffix-1]["time"])).total_seconds() == NATIVE_SECONDS[timeframe]:
        suffix -= 1
    retained = parsed[max(0, suffix):]
    receipt = {"source_name": path.name, "source_bytes": len(header) + len(raw), "source_tail_sha256": hashlib.sha256(header + raw).hexdigest(), "source_tail_offset": offset, "source_file_size": before.st_size, "source_read_completed_utc": observed, "parsed_rows": len(parsed), "contiguous_rows": len(retained), "history_before_tail_not_read": offset > len(header), "discarded_rows_before_gap": max(0, suffix), "complete_basis": "existing_forward_archive_schema_and_elapsed_bar_end"}
    return retained, receipt


def build_research_observation(quote_payload, candles_by_pair, *, source_read_completed_utc, generated_utc=None, source_receipts=None, book_payload=None, clock=utc_now, max_cycle_seconds=20.0, monotonic=time.monotonic):
    started = monotonic()
    read_at = parse_utc(source_read_completed_utc)
    source_at = parse_utc(quote_payload.get("generated_utc"))
    if read_at is None or source_at is None or source_at > read_at:
        raise ValueError("quote_snapshot_source_clock_invalid")
    if quote_payload.get("producer") != "practice_007_dedicated_quote_stream":
        raise ValueError("dedicated_quote_producer_required")
    quotes = quote_payload.get("quotes")
    if not isinstance(quotes, dict) or not 1 <= len(quotes) <= MAX_PAIRS or any(not PAIR.fullmatch(pair) or pair[:3] == pair[4:] or not isinstance(row, dict) for pair, row in quotes.items()):
        raise ValueError("bounded_quote_universe_required")
    if type(quote_payload.get("schema_version")) is not int or quote_payload["schema_version"] != 1 or quote_payload.get("research_only") is not True or type(quote_payload.get("quote_count")) is not int or quote_payload["quote_count"] != len(quotes):
        raise ValueError("dedicated_quote_snapshot_schema_invalid")
    retained = set((quote_payload.get("coverage") or {}).get("retained_last_known_instruments") or [])
    primary, views, coverage, observed, accepted, exclusions = {}, {}, {}, {}, {}, []
    book_rows = (book_payload or {}).get("instruments") or {}
    metadata = {pair: dict(book_rows.get(pair) or {}) for pair in quotes}
    for pair, quote in sorted(quotes.items()):
        reason = None
        stamp = parse_utc(quote.get("time")) if isinstance(quote, dict) else None
        try:
            bid, ask, pip = (_number(quote[key]) for key in ("bid", "ask", "pip"))
            if not 0 < bid <= ask or not 1e-8 <= pip <= .1:
                raise ValueError("invalid_quote_values")
        except (ValueError, TypeError, KeyError):
            reason, pip = "invalid_quote_values", None
        if stamp is None or stamp > source_at:
            reason = reason or "quote_clock_unknown_or_after_source_snapshot"
        elif (read_at - stamp).total_seconds() > 30:
            reason = reason or "stale_quote"
        if pair in retained:
            reason = reason or "retained_last_known_quote"
        if quote.get("tradeable") is not True:
            reason = reason or "quote_not_explicitly_tradeable"
        if reason:
            exclusions.append({"instrument": pair, "reason": reason, "quote_time": str((quote or {}).get("time") or "")})
        else:
            accepted[pair] = dict(quote)
        if pip is None or monotonic() - started > max_cycle_seconds:
            primary[pair] = {name: None for name in calculator.ma_names()}
            views[pair] = {}
            coverage[pair] = {"structural_status": "unavailable", "rich_ma_status": "unavailable", "rich_ma_reason": "invalid_pip_or_cycle_time_bound"}
        else:
            primary[pair], views[pair], coverage[pair] = calculator.calculate_pair(pair, candles_by_pair.get(pair) or {}, pip)
        observed[pair] = clock()
    # Existing microstructure formulas see only admissible original quotes.
    # Missing/unknown metadata remains None/default with captured state labels.
    eligible_cross = {}
    for timeframe, seconds in GROUP_SECONDS.items():
        scoped = primary if timeframe == "M1" else {pair: groups.get(timeframe) or {} for pair, groups in views.items()}
        eligible_cross[timeframe] = {pair for pair, values in scoped.items() if (origin := parse_utc(values.get("candle_time"))) is not None and 0 <= (read_at - origin).total_seconds() - seconds <= seconds + 75 and all(isinstance(values.get(name), (int, float)) and math.isfinite(values[name]) for name in ("r1_pips", "r3_pips", "r5_pips", "m1_atr14_pips"))}
    calculator.augment_observed_caches(primary, views, accepted, metadata, eligible_cross_pairs=eligible_cross)
    captured_at = clock()
    pairs = {}
    for pair, features in primary.items():
        quote = quotes[pair]
        component = {"quote_feature_source_utc": str(quote.get("time") or "") if pair in accepted else "", "quote_snapshot_generated_utc": source_at.isoformat(), "source_read_completed_utc": read_at.isoformat(), "structural_calculated_utc": observed[pair], "order_book_source_utc": str(metadata[pair].get("order_book_time_utc") or ""), "position_book_source_utc": str(metadata[pair].get("position_book_time_utc") or "")}
        group_clock = {"observed_utc": captured_at, "clock_basis": "verified_local_source_read_then_calculation", "component_clocks": component}
        primary_scalars = {name: value for name, value in features.items() if name not in PURE_QUOTE_FEATURES_V1 and not name.startswith(("order_book_", "position_book_"))}
        groups = {"primary": capture_scalar_group(primary_scalars, input_timeframe="M1", clock=group_clock)}
        for timeframe in calculator.TIMEFRAMES:
            if timeframe == "M1":
                continue  # Exact M1 structural alias is already in primary.
            values = views[pair].get(timeframe) or {name: None for name in calculator.STRUCTURAL_NAMES}
            values = {name: value for name, value in values.items() if name not in PURE_QUOTE_FEATURES_V1 and not name.startswith(("order_book_", "position_book_"))}
            groups["timeframe:" + timeframe] = capture_scalar_group(values, input_timeframe=timeframe, clock=group_clock)
        micro_values = {**metadata[pair], **{name: value for name, value in features.items() if name in PURE_QUOTE_FEATURES_V1}}
        micro_values.update(bid=quote.get("bid"), ask=quote.get("ask"))
        groups["microstructure"] = capture_scalar_group(micro_values, input_timeframe="QUOTE", clock={**group_clock, "bar_complete_utc": component["quote_feature_source_utc"]})
        # Existing news-field identities remain explicit missingness until a
        # verified causal publication adapter supplies their actual lineage.
        groups["news"] = capture_feature_group({name: None for name in NEWS_NAMES}, input_timeframe="UNKNOWN", clock={"clock_basis": "unknown_news_publication"})
        pairs[pair] = {"quote": dict(quote), "groups": groups, "coverage": {**coverage[pair], "group_aliases": {"timeframe:M1": "primary", "pure_quote_fields": "microstructure"}, "quote_accepted": pair in accepted, "quote_exclusion_reason": next((row["reason"] for row in exclusions if row["instrument"] == pair), None), "families": {"existing_rich_ma_M1": coverage[pair].get("rich_ma_status"), "existing_structural_timeframes": {tf: "available" if tf in views[pair] else "source_unavailable" for tf in calculator.TIMEFRAMES}, "microstructure": "observed_values_only", "news_classifier": "unavailable_no_verified_publication_adapter", "supervised_and_pattern_forecasts": "model_outputs_not_loaded", "lane_evaluation_only_features": "not_materialized_by_existing_snapshot_calculators"}}}
    generated = generated_utc or clock()
    generated_at = parse_utc(generated)
    if generated_at is None or not read_at <= parse_utc(captured_at) <= generated_at:
        raise ValueError("observation_calculation_clock_order_invalid")
    native_ages, structural_fresh = {}, {tf: 0 for tf in calculator.TIMEFRAMES}
    rich_materialized, rich_fresh = 0, 0
    for pair, values in primary.items():
        origin = parse_utc(values.get("candle_time"))
        age = (generated_at - origin).total_seconds() - 60 if origin is not None else None
        native_ages[pair] = age
        materialized = coverage[pair].get("rich_ma_status") == "available"
        rich_materialized += materialized
        rich_fresh += materialized and age is not None and 0 <= age <= 135
        for tf, seconds in GROUP_SECONDS.items():
            group_values = values if tf == "M1" else views[pair].get(tf) or {}
            tf_origin = parse_utc(group_values.get("candle_time"))
            tf_age = (generated_at - tf_origin).total_seconds() - seconds if tf_origin else None
            if tf_age is not None and 0 <= tf_age <= seconds + 75 and isinstance(group_values.get("r1_pips"), (int, float)):
                structural_fresh[tf] += 1
    readiness = {"basis": "source_bar_completion_age_at_generation; final_mapper_checks_remain_authoritative", "native_M1_age_sec_by_pair": native_ages, "rich_M1_materialized_pairs": rich_materialized, "rich_M1_fresh_pairs": rich_fresh, "rich_M1_stale_or_missing_pairs": len(quotes) - rich_fresh, "structural_fresh_pairs_by_timeframe": structural_fresh, "fresh_quote_pairs_at_source_read": len(accepted), "news_ready_pairs": 0, "model_output_pairs": 0, "continuous_native_M1_freshness_verified": False}
    source = {"producer_id": WORKER, "producer_contract_id": SCHEMA, **calculator.source_identity(), "worker_source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), "clock_guard_source_sha256": hashlib.sha256((ROOT / "oanda_feature_research_clock_v1.py").read_bytes()).hexdigest(), "observation_implementation_sha256": observation_source_sha256(), "bounded_history_rows": MAX_ROWS}
    source_hash = payload_sha256({"quote_payload": quote_payload, "receipts": source_receipts or {}})
    return {"schema_version": 1, "snapshot_id": generated_at.strftime("research_%Y%m%dT%H%M%S%f_") + source_hash[:16], "generated_utc": generated, "generated_epoch": generated_at.timestamp(), "observation_source": source, "source_inputs": {"source_read_completed_utc": source_read_completed_utc, "quote_source_payload_sha256": payload_sha256(quote_payload), "source_receipts": source_receipts or {}, "historical_source_rows_copied": False}, "instruments": {}, "observation_inputs": {"schema_version": "feature_observation_inputs_v1", "quote_component_clocks_v1": True, "instruments": pairs}, "coverage": {"expected_feature_instrument_count": MAX_PAIRS, "expected_feature_instruments": sorted(quotes), "observed_universe_count": len(quotes), "accepted_instrument_count": len(accepted), "excluded_instrument_count": len(exclusions), "quote_exclusions": exclusions, "family_readiness": readiness, "all_configured_feature_families_materialized": False}, **FLAGS}


def check_storage(root, *, now, max_daily_bytes, minimum_free_bytes):
    root = checked_path(root)
    root.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(root).free < minimum_free_bytes + MAX_ENVELOPE_BYTES + 65536:
        raise ValueError("archive_free_space_floor")
    day = root / now.strftime("date=%Y%m%d")
    total, count = 0, 0
    for hour in range(24):
        directory = checked_path(day / f"hour={hour:02d}")
        if directory.exists():
            for path in directory.glob("obs_*.json.gz"):
                checked_path(path)
                total += path.stat().st_size
                count += 1
                if count > 5000 or total + MAX_ENVELOPE_BYTES + 65536 > max_daily_bytes:
                    raise ValueError("archive_daily_byte_or_file_bound")
    return {"daily_archive_bytes_before": total, "daily_archive_files_before": count}


def run_cycle(args, *, clock=utc_now):
    started = time.monotonic()
    initial_clock = require_verified_clock(args.clock_state, clock=clock)
    budget = check_storage(args.archive_root, now=parse_utc(clock()), max_daily_bytes=args.max_daily_archive_mib * 1024 * 1024, minimum_free_bytes=args.minimum_free_mib * 1024 * 1024)
    quotes, quote_receipt = read_json_source(args.quote_snapshot, clock=clock)
    raw_quotes = quotes.get("quotes") or {}
    if len(raw_quotes) > MAX_PAIRS or any(not PAIR.fullmatch(pair) for pair in raw_quotes):
        raise ValueError("bounded_quote_universe_required")
    receipts, candles, source_bytes = {"quotes": quote_receipt, "candles": {}}, {}, quote_receipt["source_bytes"]
    for pair in sorted(raw_quotes):
        candles[pair], receipts["candles"][pair] = {}, {}
        for timeframe in NATIVE_SECONDS:
            if time.monotonic() - started > args.max_cycle_sec or source_bytes + MAX_TAIL_BYTES + 4096 > MAX_SOURCE_BYTES:
                receipts["candles"][pair][timeframe] = {"status": "unavailable", "reason": "cycle_input_bound"}
                continue
            try:
                rows, receipt = read_candle_tail(Path(args.candle_root) / f"{pair}_{timeframe}.csv", pair, timeframe, clock=clock)
                candles[pair][timeframe] = rows
                receipts["candles"][pair][timeframe] = receipt
                source_bytes += receipt["source_bytes"]
            except (OSError, ValueError, KeyError, UnicodeError) as exc:
                receipts["candles"][pair][timeframe] = {"status": "unavailable", "reason": type(exc).__name__ + ":" + str(exc)[:120]}
    books = None
    if args.book_snapshot:
        try:
            books, receipt = read_json_source(args.book_snapshot, clock=clock)
            receipts["books"] = receipt
        except (OSError, ValueError) as exc:
            receipts["books"] = {"status": "unavailable", "reason": type(exc).__name__}
    read_complete = clock()
    snapshot = build_research_observation(quotes, candles, source_read_completed_utc=read_complete, source_receipts=receipts, book_payload=books, clock=clock, max_cycle_seconds=max(0.0, args.max_cycle_sec - (time.monotonic() - started)))
    frame = build_observation_frame(snapshot)
    # Reserve both source and normalized representations before publication.
    if len(canonical_bytes({"original_snapshot": snapshot, "frame": frame})) > MAX_ENVELOPE_BYTES:
        raise ValueError("research_envelope_byte_bound")
    completion_proof = validate_clock_state(initial_clock["evidence"]["clock_state"], now_epoch=parse_utc(clock()).timestamp())
    if not completion_proof["valid"]:
        raise ValueError("initial_research_clock_expired:" + completion_proof["reason"])
    clock_verdict = require_verified_clock(args.clock_state, clock=clock)
    def before_publish():
        nonlocal clock_verdict
        if time.monotonic() - started > args.max_cycle_sec:
            raise ValueError("cycle_time_bound_before_publication")
        proof = validate_clock_state(initial_clock["evidence"]["clock_state"], now_epoch=parse_utc(clock()).timestamp())
        if not proof["valid"]:
            raise ValueError("initial_research_clock_expired_before_publication:" + proof["reason"])
        clock_verdict = require_verified_clock(args.clock_state, clock=clock)
    path = archive_observation_snapshot(snapshot, args.archive_root, before_publish=before_publish)
    published = clock()
    receipt = {"schema_version": "feature_observation_publication_receipt_v1", "snapshot_id": frame["snapshot_id"], "source_schema_id": frame["source_schema_id"], "payload_sha256": frame["source_payload_sha256"], "frame_sha256": payload_sha256(frame), "archive_relative_path": path.relative_to(args.archive_root).as_posix(), "archive_sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "source_read_completed_utc": read_complete, "publication_completed_utc": published, "publication_scope": "archive_visible_before_this_receipt", "producer_source": snapshot["observation_source"], **FLAGS}
    receipt_path = path.with_suffix(".publication.json")
    if receipt_path.exists():
        raise ValueError("publication_receipt_identity_collision")
    immutable_json(receipt_path, receipt)
    return {"status": "published", "archive": str(path), "publication_receipt": str(receipt_path), "last_publication_completed_utc": published, "coverage": snapshot["coverage"], "source_bytes": source_bytes, "clock_verification": clock_verdict, "elapsed_sec": round(time.monotonic()-started, 3), **budget}


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quote-snapshot", type=Path, required=True)
    parser.add_argument("--candle-root", type=Path, required=True)
    parser.add_argument("--book-snapshot", type=Path)
    parser.add_argument("--archive-root", type=Path, required=True)
    parser.add_argument("--heartbeat", type=Path, required=True)
    parser.add_argument("--clock-state", type=Path, required=True)
    parser.add_argument("--interval-sec", type=float, default=60)
    parser.add_argument("--duration-sec", type=float, default=3600)
    parser.add_argument("--max-cycle-sec", type=float, default=30)
    parser.add_argument("--max-daily-archive-mib", type=int, default=4096)
    parser.add_argument("--minimum-free-mib", type=int, default=4096)
    args = parser.parse_args(argv)
    if not 60 <= args.interval_sec <= 3600 or not 1 <= args.duration_sec <= 604800 or not 1 <= args.max_cycle_sec <= 30 or not 32 <= args.max_daily_archive_mib <= 4096 or not 64 <= args.minimum_free_mib <= 65536:
        raise ValueError("bounded_worker_parameters_required")
    return args


def main(argv=None):
    args = parse_args(argv)
    deadline = time.monotonic() + args.duration_sec
    last_success = None
    while time.monotonic() < deadline:
        started = time.monotonic()
        heartbeat = {"worker": WORKER, "updated_at": utc_now(), "phase": "reading_sources", "last_success": last_success, **FLAGS}
        atomic_json(args.heartbeat, heartbeat)
        try:
            result = run_cycle(args)
            last_success = result
            heartbeat.update(phase="cycle_complete", result=result, last_success=last_success)
        except (OSError, ValueError, TypeError, KeyError) as exc:
            heartbeat.update(phase="cycle_failed", result={"status": "unavailable", "error_type": type(exc).__name__, "reason": str(exc)[:240]})
        heartbeat["updated_at"] = utc_now()
        atomic_json(args.heartbeat, heartbeat)
        pause = min(args.interval_sec - (time.monotonic()-started), deadline-time.monotonic())
        if pause > 0:
            time.sleep(pause)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
