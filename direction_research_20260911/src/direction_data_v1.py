"""Offline, source-bound historical prices and original as-of news projection.

No live module is imported and no model is fitted. CSV candle-close availability
is a retrospective convention, not evidence of the original provider receipt.
News identity and feature calculations reuse exact, hash-pinned source functions.
An absent retained news context is missing, never an invented neutral observation.
"""
from __future__ import annotations

import ast
import copy
import datetime as dt
import gzip
import hashlib
import io
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import threading
import time
from contextlib import closing
from types import SimpleNamespace
from typing import Any, Mapping

import numpy as np
import pandas as pd

SCHEMA = "direction_retrospective_dataset_v1_20260911"
SOURCE_HASHES = {
    "oanda_source_governance.py": "c4b7793dcef1b9176079a959019f8b69059fbd6644fddf1e8718f924d3a13950",
    "oanda_news_causal_aggregation_guard_v1.py": "2a7a05204dee5191cb3f53d3d5a8082b7e725842e6af65aa6dd1ae7f6292dcf9",
    "oanda_causal_forecast_inputs_joint_news_v2.py": "ade4fc5eb2aea195d7609964d7b8979ca433a28028e059c65adc1b1b404a277c",
}
NEWS_FEATURES = (
    "context_balance", "context_volume_log", "context_signed_fraction", "context_mean_age_hours",
    "vetted_balance", "vetted_volume_log", "vetted_conflict_fraction", "vetted_remaining_hours",
)
DEFAULT_LIMITS = {
    "price_file_bytes": 64 * 1024 * 1024, "all_price_bytes": 2 * 1024**3,
    "selected_price_rows": 1_000_000, "news_rows": 50_000,
    "news_raw_bytes": 512 * 1024**2, "news_row_bytes": 1024**2,
    "news_read_seconds": 90, "projection_bytes": 768 * 1024**2,
}
CLASSIFICATIONS = {
    "local_fx_news_rules_20260904_v164_conflict_duration_recap_guard",
    "local_fx_news_rules_20260907_v165_causal_member_admission",
}
MAPPING_CONTRACT = "news_source_governance_fast_lane_v3_committed_visibility_20260905"


def _encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _epoch(value):
    if isinstance(value, str):
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError("aware_clock_required")
        value = parsed.timestamp()
    if type(value) not in (float, int) or not math.isfinite(value) or value <= 0:
        raise ValueError("finite_positive_clock_required")
    return float(value)


def _iso(value):
    return dt.datetime.fromtimestamp(_epoch(value), dt.timezone.utc).isoformat()


def _signature(stat):
    return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns


def _stable_bytes(path, limit):
    path = Path(path)
    with path.open("rb") as handle:
        before = path.stat()
        raw = handle.read(limit + 1)
        opened = os.fstat(handle.fileno())
        after = path.stat()
    if len(raw) > limit:
        raise ValueError("source_byte_bound")
    if _signature(before) != _signature(opened) or _signature(before) != _signature(after):
        raise ValueError("source_changed_during_read")
    return raw


def _source_bytes(source_root):
    result = {}
    for name, expected in SOURCE_HASHES.items():
        raw = _stable_bytes(Path(source_root) / name, 2 * 1024**2)
        if _sha(raw) != expected:
            raise ValueError("source_binding_changed:" + name)
        result[name] = raw
    return result


def _extract(raw, names, namespace, filename):
    """Compile only selected original pure definitions; skip imports and writers."""
    tree = ast.parse(raw.decode("utf-8-sig"), filename=filename)
    chosen = []
    found = set()
    for node in tree.body:
        node_names = {node.name} if isinstance(node, (ast.FunctionDef, ast.ClassDef)) else set()
        if isinstance(node, ast.Assign):
            node_names = {t.id for t in node.targets if isinstance(t, ast.Name)}
        if node_names & names:
            chosen.append(node)
            found.update(node_names & names)
    if found != names:
        raise ValueError("source_definition_closure_incomplete")
    future = ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0)
    selected = ast.fix_missing_locations(ast.Module(body=[future, *chosen], type_ignores=[]))
    exec(compile(selected, filename, "exec"), namespace)


def _helpers(sources):
    for name, expected in SOURCE_HASHES.items():
        if name not in sources or _sha(sources[name]) != expected:
            raise ValueError("source_binding_changed:" + name)
    guard_ns = {"__name__": "_isolated_direction_news_guard"}
    guard_name = "oanda_news_causal_aggregation_guard_v1.py"
    exec(compile(sources[guard_name], guard_name, "exec"), guard_ns)
    guard = SimpleNamespace(**guard_ns)
    governance = {"json": json, "hashlib": hashlib, "Mapping": Mapping, "Any": Any}
    _extract(sources["oanda_source_governance.py"], {
        "NON_SUBSTANTIVE_ARTICLE_PAYLOAD_FIELDS", "NON_SUBSTANTIVE_NESTED_RAW_FIELDS",
        "canonical_json", "stable_hash", "normalized_publisher_headline",
        "substantive_article_payload", "source_event_substantive_sha256",
    }, governance, "oanda_source_governance.py")
    frame = {"json": json, "hashlib": hashlib, "dt": dt, "math": math, "copy": copy, "re": re,
             "_LOCK": threading.RLock(), "_FRAME_CACHE": {}, "_module": lambda name: guard,
             "GUARD_VERSION": guard.GUARD_VERSION}
    _extract(sources["oanda_causal_forecast_inputs_joint_news_v2.py"], {
        "_encoded", "_digest", "_clock", "_epoch", "_iso", "_original_known", "_frame", "_pair_features",
    }, frame, "oanda_causal_forecast_inputs_joint_news_v2.py")
    return governance, guard, frame


def _price_frame(raw, pair, start_epoch, end_epoch):
    if not raw.endswith(b"\n"):
        raise ValueError("partial_csv_source")
    required = {"time", "instrument", "close"}
    columns = pd.read_csv(io.BytesIO(raw), nrows=0).columns
    if not required.issubset(columns):
        raise ValueError("price_schema_missing")
    used = [c for c in ("time", "instrument", "close", "bid_close", "ask_close", "complete", "granularity") if c in columns]
    source = pd.read_csv(io.BytesIO(raw), usecols=used)
    if not source["time"].astype(str).str.contains(r"(?:Z|[+-]\d{2}:\d{2})$", regex=True).all():
        raise ValueError("aware_price_timestamp_required")
    times = pd.to_datetime(source["time"], utc=True, errors="raise")
    epoch = times.astype("int64").to_numpy() / 1e9
    keep = (epoch >= start_epoch) & (epoch + 60 <= end_epoch)
    rows = source.loc[keep].copy()
    selected = epoch[keep]
    if not len(rows):
        raise ValueError("pair_has_no_selected_prices:" + pair)
    if np.any(selected % 60 != 0) or np.any(np.diff(selected) <= 0):
        raise ValueError("duplicate_unordered_or_nonminute_price")
    if not (rows["instrument"] == pair).all():
        raise ValueError("price_instrument_mismatch")
    if "complete" in rows and not rows["complete"].astype(str).str.lower().isin(["true", "1"]).all():
        raise ValueError("incomplete_price_bar")
    if "granularity" in rows and not (rows["granularity"] == "M1").all():
        raise ValueError("wrong_price_granularity")
    mid = pd.to_numeric(rows["close"], errors="raise").to_numpy(dtype=float)
    bid = pd.to_numeric(rows["bid_close"], errors="raise").to_numpy(dtype=float) if "bid_close" in rows else np.full(len(rows), np.nan)
    ask = pd.to_numeric(rows["ask_close"], errors="raise").to_numpy(dtype=float) if "ask_close" in rows else np.full(len(rows), np.nan)
    if not np.all(np.isfinite(mid) & (mid > 0)):
        raise ValueError("invalid_mid_price")
    actual = np.isfinite(bid) & np.isfinite(ask)
    if np.any(np.isfinite(bid) != np.isfinite(ask)) or np.any(actual & ((bid <= 0) | (ask < bid))):
        raise ValueError("invalid_bid_ask_price")
    return pd.DataFrame({"epoch": selected.astype("int64"), "available_epoch": selected.astype("int64") + 60,
                         "mid": mid, "bid": bid, "ask": ask})


def _validated_member(row, governance, guard, frame, observed_limit):
    if row["mapped_sha"] != row["raw_payload_sha256"] or row["contract_id"] != MAPPING_CONTRACT:
        raise ValueError("news_mapping_binding_mismatch")
    if row["availability_basis"] != "post_commit_independent_mapping_read" or row["consumer_first_observation_required"] != 1:
        raise ValueError("news_mapping_availability_contract")
    if governance["source_event_substantive_sha256"](row) != row["raw_payload_sha256"]:
        raise ValueError("news_original_payload_hash_mismatch")
    visible = _epoch(row["mapping_visible_utc"])
    effective = _epoch(row["effective_from_utc"])
    if not effective <= visible <= observed_limit:
        raise ValueError("future_news_mapping")
    raw = json.loads(row["payload_json"]).get("raw_payload")
    if not isinstance(raw, dict) or raw.get("observation_clock_trusted") is not True:
        raise ValueError("news_member_observation_untrusted")
    if raw.get("classification_version") not in CLASSIFICATIONS:
        raise ValueError("unsupported_original_news_classification")
    member = {k: copy.deepcopy(raw.get(k)) for k in guard.MEMBER_KEYS}
    if not isinstance(member.get("event_id"), str) or not 1 <= len(member["event_id"]) <= 200:
        raise ValueError("invalid_original_member_identity")
    member["observed_available_utc"] = _iso(max(visible, _epoch(member["observed_available_utc"]) if member.get("observed_available_utc") else visible))
    if _epoch(member["observed_available_utc"]) > observed_limit:
        raise ValueError("original_news_available_after_snapshot")
    known = frame["_original_known"](member)
    if known > observed_limit:
        raise ValueError("original_news_known_after_snapshot")
    scores = member.get("currency_scores")
    if not isinstance(scores, dict) or len(scores) > 32 or any(
        re.fullmatch("[A-Z]{3}", str(k)) is None or type(v) not in (int, float)
        or not math.isfinite(v) or not -1 <= v <= 1 for k, v in scores.items()
    ):
        raise ValueError("historical_news_score_invalid")
    return {"source_event_id": row["source_event_id"], "original_payload_sha256": _sha(row["payload_json"].encode()),
            "mapping_visible_epoch": visible, "effective_epoch": effective, "member": member}


def _news_rows(database, start_epoch, end_epoch, observed_epoch, helpers, limits):
    governance, guard, frame = helpers
    selected, members, seen = [], [], set()
    total = 0
    started = time.monotonic()
    with closing(sqlite3.connect(Path(database).resolve().as_uri() + "?mode=ro", uri=True, timeout=2)) as con:
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA query_only=ON")
        con.execute("BEGIN")
        con.set_progress_handler(lambda: int(time.monotonic() - started > limits["news_read_seconds"]), 10_000)
        coverage = con.execute("SELECT MIN(mapping_visible_utc), MAX(mapping_visible_utc), COUNT(*) FROM news_fast_lane_visibility_v3").fetchone()
        if coverage[0] is None:
            raise ValueError("committed_news_history_missing")
        query = """SELECT e.source_event_id,e.raw_payload_sha256,e.payload_json,e.effective_from_utc,
            m.raw_payload_sha256 mapped_sha,v.mapping_visible_utc,v.availability_basis,
            v.consumer_first_observation_required,b.contract_id
            FROM news_fast_lane_mappings_v3 m JOIN source_events e USING(source_event_id)
            JOIN news_fast_lane_batches_v3 b USING(batch_seq)
            JOIN news_fast_lane_visibility_v3 v USING(batch_seq)
            WHERE v.mapping_visible_utc>=? AND v.mapping_visible_utc<=?
            ORDER BY v.mapping_visible_utc,e.source_event_id"""
        cursor = con.execute(query, (_iso(start_epoch), _iso(end_epoch)))
        while True:
            page = cursor.fetchmany(128)
            if not page:
                break
            for raw_row in page:
                row = dict(raw_row)
                size = len(row["payload_json"].encode())
                total += size
                if len(selected) >= limits["news_rows"] or total > limits["news_raw_bytes"] or size > limits["news_row_bytes"]:
                    raise ValueError("complete_news_capture_bound_exceeded")
                if time.monotonic() - started > limits["news_read_seconds"]:
                    raise ValueError("news_read_deadline")
                identity = row["source_event_id"]
                if not isinstance(identity, str) or not identity or identity in seen:
                    raise ValueError("duplicate_or_invalid_news_identity")
                seen.add(identity)
                members.append(_validated_member(row, governance, guard, frame, observed_epoch))
                selected.append(row)
        con.rollback()
    return selected, members, {
        "archive_first_mapping_utc": coverage[0], "archive_last_mapping_utc": coverage[1],
        "archive_visibility_batches": coverage[2], "selected_rows": len(selected), "raw_wrapper_bytes": total,
        "selected_first_mapping_utc": selected[0]["mapping_visible_utc"] if selected else None,
        "selected_last_mapping_utc": selected[-1]["mapping_visible_utc"] if selected else None,
        "complete_bounded_query": True, "collection_continuity_established": False,
        "read_transaction": "SQLite mode=ro, query_only, BEGIN coherent transaction; no immutable URI bypass",
    }


def _write_bytes(root, name, raw):
    path = root / name
    with path.open("xb") as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())
    return {"path": name, "sha256": _sha(raw), "bytes": len(raw)}


def capture_dataset(candle_root, governance_db, pairs, start_epoch, end_epoch, output_root, *,
                    source_root, news_start_epoch=None, limits=None):
    """Freeze selected observations; end_epoch includes only candles closed by it.

    pairs=None freezes every *_M1.csv filename before reading. No selection is
    based on outcomes. Missing pairs abort. Output must be a new directory.
    """
    start_epoch, end_epoch = _epoch(start_epoch), _epoch(end_epoch)
    news_start_epoch = _epoch(news_start_epoch if news_start_epoch is not None else start_epoch)
    now = time.time()
    if not news_start_epoch <= end_epoch <= now or start_epoch >= end_epoch:
        raise ValueError("invalid_or_future_capture_interval")
    bounds = {**DEFAULT_LIMITS, **(limits or {})}
    if any(type(v) not in (int, float) or not math.isfinite(v) or v <= 0 for v in bounds.values()):
        raise ValueError("positive_capture_limits_required")
    candle_root, root = Path(candle_root).resolve(), Path(output_root).resolve()
    if root == Path(source_root).resolve() or root.is_relative_to(Path(source_root).resolve()):
        raise ValueError("snapshot_must_be_outside_canonical_source")
    available = sorted(p.name[:-7] for p in candle_root.glob("*_M1.csv"))
    selected_pairs = available if pairs is None else sorted(pairs)
    if not selected_pairs or len(selected_pairs) != len(set(selected_pairs)) or any(
        re.fullmatch("[A-Z]{3}_[A-Z]{3}", p) is None or p not in available for p in selected_pairs
    ):
        raise ValueError("invalid_or_missing_pair_selection")
    sources = _source_bytes(source_root)
    helpers = _helpers(sources)
    root.mkdir(parents=True, exist_ok=False)
    files = {}
    for name, raw in sources.items():
        files[name] = _write_bytes(root, name + ".txt", raw)
    prices, price_provenance = {}, []
    source_bytes = selected_rows = 0
    for pair in selected_pairs:
        path = candle_root / (pair + "_M1.csv")
        raw = _stable_bytes(path, bounds["price_file_bytes"])
        source_bytes += len(raw)
        if source_bytes > bounds["all_price_bytes"]:
            raise ValueError("all_price_source_byte_bound")
        frame = _price_frame(raw, pair, start_epoch, end_epoch)
        selected_rows += len(frame)
        if selected_rows > bounds["selected_price_rows"]:
            raise ValueError("selected_price_row_bound")
        prices[pair] = json.loads(frame.to_json(orient="records", double_precision=15))
        price_provenance.append({"pair": pair, "source_path": str(path), "source_sha256": _sha(raw),
            "source_bytes": len(raw), "selected_rows": len(frame), "first_epoch": int(frame.epoch.iloc[0]),
            "last_epoch": int(frame.epoch.iloc[-1]), "bid_ask_rows": int((frame.bid.notna() & frame.ask.notna()).sum()),
            "gap_intervals": int((frame.epoch.diff() > 60).sum()), "read_completed_epoch": time.time()})
    raw_news, members, news_provenance = _news_rows(governance_db, news_start_epoch, end_epoch, now, helpers, bounds)
    payload = {"prices": prices, "original_news_rows": raw_news}
    encoded = _encoded(payload)
    if len(encoded) > bounds["projection_bytes"]:
        raise ValueError("projection_byte_bound")
    files["observations"] = _write_bytes(root, "observations.json.gz", gzip.compress(encoded, mtime=0))
    manifest = {
        "schema_version": SCHEMA, "research_only": True, "execution_eligible": False,
        "created_epoch": time.time(), "source_read_started_epoch": now,
        "price_start_epoch": start_epoch, "news_start_epoch": news_start_epoch, "end_epoch": end_epoch,
        "pairs": selected_pairs, "pair_selection": "all_sorted_available_filenames" if pairs is None else "explicit_before_outcomes",
        "available_pair_filenames": available, "source_bindings": SOURCE_HASHES,
        "loader_sha256": _sha(Path(__file__).read_bytes()), "files": files,
        "limits": bounds, "price_sources": price_provenance, "news_source_path": str(Path(governance_db).resolve()),
        "news_provenance": news_provenance, "selected_price_rows": selected_rows,
        "projection_raw_bytes": len(encoded), "projection_sha256": _sha(encoded),
        "price_availability": "Retrospective candle-close convention epoch+60; no original provider receipt evidence; backfill can be present.",
        "news_availability": "Original known clocks and max(original observed availability, committed mapping visibility). Consumer observation now is not backdated.",
        "coverage_policy": "No retained pair context is NaN/missing. Retained event mappings do not establish successful empty polls or uninterrupted collection.",
        "cost_scope": "Retained M1 bid/ask closes; endpoint cost proxy, not executable contemporaneous order quotes or slippage.",
        "validation_scope": "Retrospective discovery only; chronological splitting does not convert reconstructed inputs into prospective evidence.",
    }
    manifest["manifest_sha256"] = _sha(_encoded(manifest))
    _write_bytes(root, "manifest.json", _encoded(manifest))
    return manifest


def load_dataset(snapshot_root):
    root = Path(snapshot_root).resolve()
    manifest = json.loads(_stable_bytes(root / "manifest.json", 4 * 1024**2))
    if manifest.get("schema_version") != SCHEMA or manifest.get("manifest_sha256") != _sha(_encoded({k: v for k, v in manifest.items() if k != "manifest_sha256"})):
        raise ValueError("dataset_manifest_mismatch")
    if manifest.get("source_bindings") != SOURCE_HASHES or manifest.get("research_only") is not True or manifest.get("execution_eligible") is not False:
        raise ValueError("dataset_contract_mismatch")
    loaded = {}
    for key, descriptor in manifest["files"].items():
        path = (root / descriptor["path"]).resolve()
        if path.parent != root:
            raise ValueError("snapshot_path_escape")
        raw = _stable_bytes(path, descriptor["bytes"])
        if len(raw) != descriptor["bytes"] or _sha(raw) != descriptor["sha256"]:
            raise ValueError("snapshot_file_mismatch:" + key)
        loaded[key] = raw
    helpers = _helpers({k: loaded[k] for k in SOURCE_HASHES})
    with gzip.GzipFile(fileobj=io.BytesIO(loaded["observations"])) as handle:
        raw = handle.read(manifest["limits"]["projection_bytes"] + 1)
    if len(raw) != manifest["projection_raw_bytes"] or _sha(raw) != manifest["projection_sha256"]:
        raise ValueError("observation_projection_mismatch")
    payload = json.loads(raw)
    if sorted(payload["prices"]) != manifest["pairs"]:
        raise ValueError("snapshot_pair_binding")
    prices = {pair: pd.DataFrame(rows).astype({"epoch": "int64", "available_epoch": "int64", "mid": "float64", "bid": "float64", "ask": "float64"}) for pair, rows in payload["prices"].items()}
    members = [_validated_member(row, *helpers, manifest["source_read_started_epoch"]) for row in payload["original_news_rows"]]
    news = {"news_capture_sha256": manifest["projection_sha256"], "history": members}
    return {"prices": prices, "news": news, "manifest": manifest, "_helpers": helpers}


def project_news(dataset, epochs, pairs=None):
    """Project original eight features at decision=minute-start epoch+60.

    Rows with no retained pair context have NaN features; successful empty
    collection is not asserted. Features within observed context retain the
    original guard's zero directional scores when no member qualifies.
    """
    manifest = dataset["manifest"]
    chosen = manifest["pairs"] if pairs is None else list(pairs)
    if len(set(chosen)) != len(chosen) or any(p not in manifest["pairs"] for p in chosen):
        raise ValueError("unregistered_pair")
    anchors = [_epoch(e) for e in epochs]
    if len(set(anchors)) != len(anchors) or any(e % 60 for e in anchors):
        raise ValueError("unique_minute_anchors_required")
    calculations = dataset["_helpers"][2]
    output = {pair: [] for pair in chosen}
    for epoch in anchors:
        decision = epoch + 60
        history_valid = manifest["news_start_epoch"] + 3600 <= decision <= manifest["end_epoch"]
        frame = calculations["_frame"](dataset["news"], decision) if history_valid else None
        for pair in chosen:
            base, quote = pair.split("_")
            context = [r for r in frame["members"] if base in r["scores"] or quote in r["scores"]] if frame else []
            vetted = [r for r in frame["directional"] if base in r["scores"] or quote in r["scores"]] if frame else []
            if context:
                values, expires = calculations["_pair_features"](frame, pair, decision)
                status = "retained_pair_context"
            else:
                values, expires = [float("nan")] * len(NEWS_FEATURES), None
                status = "no_retained_pair_context" if history_valid else "unavailable_history"
            output[pair].append({"epoch": int(epoch), "decision_epoch": int(decision), **dict(zip(NEWS_FEATURES, values)),
                "news_status": status, "news_present": bool(context), "context_members": len(context),
                "vetted_topics": len(vetted), "available_max_epoch": frame["available_max_epoch"] if frame and context else None,
                "direction_expires_epoch": expires, "collection_continuity_established": False})
    return {pair: pd.DataFrame(rows) for pair, rows in output.items()}
