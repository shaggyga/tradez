"""Synthetic 68-pair producer/archive/reader integration, outside project Git."""
import gzip
import hashlib
import itertools
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

PROJECT = Path(r"C:\Users\zmoor\Documents\forex\trad")
sys.path.insert(0, str(PROJECT))
import oanda_feature_observations_v1 as observation
from oanda_feature_move_mapping_v1 import read_feature_move_map
from test_oanda_feature_observations_v1 import extracted, writer_namespace


def test_full_universe_writer_archive_reader_and_pair_filter(tmp_path):
    started = time.perf_counter()
    currencies = "USD EUR JPY GBP CHF AUD CAD NZD SEK NOK DKK PLN CZK HUF TRY ZAR MXN SGD HKD CNH THB".split()
    pairs = [f"{base}_{quote}" for base, quote in itertools.combinations(currencies, 2)][:68]
    assert len(pairs) == len(set(pairs)) == 68
    assert len({code for pair in pairs for code in pair.split("_")}) == 21
    start = datetime(2026, 9, 13, 15, 0, tzinfo=timezone.utc)
    archive_root = tmp_path / "observations"
    expanded_bytes = 0
    archive_paths = []
    last_frame = None
    namespace = writer_namespace([])
    def archive(payload, root):
        nonlocal expanded_bytes, last_frame
        frame = observation.build_observation_frame(payload)
        identity = {"schema_version": observation.ARCHIVE_SCHEMA, "snapshot_id": frame["snapshot_id"], "source_schema_id": frame["source_schema_id"]}
        envelope = {"schema_version": observation.ARCHIVE_SCHEMA, "payload_sha256": frame["source_payload_sha256"], "source_identity": identity, "original_snapshot": observation.normalize_json(payload), "frame": frame}
        size = len(observation.canonical_bytes(envelope))
        assert expanded_bytes + size <= 20 * 1024 * 1024, "fixture expanded-byte budget exceeded before archive write"
        expanded_bytes += size
        path = observation.archive_observation_snapshot(payload, root)
        archive_paths.append(path)
        last_frame = frame
        return path
    namespace["archive_observation_snapshot"] = archive
    for index in range(8):
        stamp = start + timedelta(minutes=5 * index)
        origin = stamp - timedelta(minutes=1)
        namespace["time"] = SimpleNamespace(time=lambda: stamp.timestamp())
        writer = extracted("oanda_practice_shadow_strategy_lab.py", "write_live_model_feature_snapshot", namespace)
        features, quotes, metadata, timeframes = {}, {}, {}, {}
        for pair_index, pair in enumerate(pairs):
            # Exactly 200 primary scalars, plus an explicit alias and book clock.
            scalar = {f"f{field:x}": index * index if index < 7 else 36 + field % 3 for field in range(196)}
            scalar["f0"] = index * index
            scalar.update(candle_time=origin.isoformat(), pip=.0001, live_spread_pips=1.0, order_book_near_5_imbalance=.9 if index == 7 else index / 100)
            assert len(scalar) == 200
            features[pair] = scalar
            mid = 1.0 + pair_index / 100 + (0 if pair_index == 0 else index / 10000)
            quotes[pair] = SimpleNamespace(bid=mid, ask=mid+.0001, time=stamp.isoformat(), source="synthetic_fixture", tradeable=True)
            metadata[pair] = {"order_book_time_utc": stamp.isoformat(), "order_book_near_5_imbalance": scalar["order_book_near_5_imbalance"]}
            timeframes[pair] = {"M1": {"candle_time": origin.isoformat(), "f0": scalar["f0"]}}
        clocks = {"primary_observed_utc": stamp.isoformat(), "timeframe_observed_utc": stamp.isoformat(), "quote_feature_capture_utc": stamp.isoformat(), "quote_observed_utc_by_instrument": {pair: stamp.isoformat() for pair in pairs}}
        accepted = writer(tmp_path / "latest.json", f"synthetic-universe-{index}", features, quotes, metadata, timeframes, observation_archive_root=archive_root, observation_clocks=clocks)
        assert accepted == 68
    write_seconds = time.perf_counter() - started
    assert len(archive_paths) == 8
    first_pair = last_frame["instruments"][pairs[0]]
    assert first_pair["groups"]["primary"]["feature_ids"]["f0"] == first_pair["groups"]["timeframe:M1"]["feature_ids"]["f0"]
    assert first_pair["groups"]["timeframe:M1"]["aliases"]["f0"]["canonical_group"] == "primary"
    assert first_pair["groups"]["primary"]["feature_clocks"]["order_book_near_5_imbalance"]["input_timeframe"] == "BOOK"
    reading = time.perf_counter()
    full = read_feature_move_map(archive_root, as_of_utc=stamp.isoformat(), window_sec=300)
    full_read_seconds = time.perf_counter() - reading
    assert full["status"] == "available", full["summary"]
    assert full["archive"]["errors"] == []
    assert full["archive"]["files_read"] == 8
    assert full["archive"]["bytes_read"] == expanded_bytes
    assert full["summary"]["pair_count"] == 68
    assert {row["instrument"] for row in full["pairs"]} == set(pairs)
    assert all(row["status"] == "available" for row in full["pairs"])
    assert full["summary"]["pairs_with_no_price_change"] == 1
    assert full["summary"]["unavailable_reasons"] == {}, full["summary"]
    selected = pairs[-1]
    reading = time.perf_counter()
    filtered = read_feature_move_map(archive_root, as_of_utc=stamp.isoformat(), window_sec=300, instrument=selected)
    filtered_read_seconds = time.perf_counter() - reading
    assert filtered["status"] == "available"
    assert len(filtered["feature_changes"]) == 50
    assert all(row["instrument"] == selected for row in filtered["feature_changes"])
    book = next(row for row in filtered["feature_changes"] if row["feature_name"] == "order_book_near_5_imbalance")
    assert book["status"] == "available" and book["input_timeframe"] == "BOOK"
    assert book["history_count"] >= 5
    f0 = next(row for row in filtered["feature_changes"] if row["feature_name"] == "f0")
    assert f0["aliases"] == ["timeframe:M1"]
    assert f0["change"] == 13 and f0["history_count"] >= 5
    receipt = {"status": "passed", "synthetic_only": True, "pairs": 68, "currency_codes": 21, "primary_scalar_fields_per_pair": 200, "frames": 8, "frame_spacing_sec": 300, "archive_expanded_bytes": expanded_bytes, "archive_compressed_bytes": sum(path.stat().st_size for path in archive_paths), "writer_seconds": round(write_seconds, 3), "full_reader_seconds": round(full_read_seconds, 3), "filtered_reader_seconds": round(filtered_read_seconds, 3), "total_seconds": round(time.perf_counter()-started, 3), "full_summary": full["summary"], "filtered_summary": filtered["summary"], "book_row": book, "alias_row": f0, "source_sha256": {name: hashlib.sha256((PROJECT/name).read_bytes()).hexdigest() for name in ("oanda_feature_observations_v1.py", "oanda_feature_move_mapping_v1.py", "oanda_practice_shadow_strategy_lab.py")}, "canonical_runtime_data_accessed": False}
    (tmp_path.parent / "UNIVERSE_INTEGRATION_RECEIPT.json").write_text(json.dumps(receipt, indent=2), encoding="utf-8")
    print(json.dumps({key: receipt[key] for key in ("status", "archive_expanded_bytes", "archive_compressed_bytes", "writer_seconds", "full_reader_seconds", "filtered_reader_seconds", "total_seconds")}))
