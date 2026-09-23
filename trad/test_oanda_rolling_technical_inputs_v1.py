"""Focused causal/input-integrity tests; no network, fits or source mutations."""
import csv
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

import oanda_rolling_technical_inputs_v1 as inputs

START = 1_800_000_000
PAIR = "EUR_USD"


def row(at=START, **changes):
    text = datetime.fromtimestamp(at, timezone.utc).isoformat()
    out = {"time": text, "datetime": text, "instrument": PAIR, "granularity": "M1",
           "open": 1.2, "high": 1.22, "low": 1.18, "close": 1.21,
           "bid_open": 1.199, "bid_high": 1.219, "bid_low": 1.179, "bid_close": 1.209,
           "ask_open": 1.201, "ask_high": 1.221, "ask_low": 1.181, "ask_close": 1.211,
           "volume": 15}
    out.update(changes)
    return out


def write_csv(path, rows):
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader(); writer.writerows(rows)
    return path


def test_tail_keeps_gaps_and_completion_provenance(tmp_path):
    path = write_csv(tmp_path/"x.csv", [row(), row(START+180)])
    before = path.read_bytes()
    arrays, receipt = inputs.read_csv_tail(path, PAIR, START+240)
    assert arrays["time"].tolist() == [START, START+180]
    assert arrays["time"].dtype == np.int64
    assert all(arrays[k].dtype == np.float64 for k in inputs.FIELDS if k != "time")
    assert receipt["completion_basis"] == ["inferred_bar_end"]*2
    assert receipt["observed_epoch"] == START+240
    assert isinstance(receipt["read_completed_epoch"], float)
    assert receipt["row_hashes"] == [hashlib.sha256(x).hexdigest() for x in before.splitlines(keepends=True)[1:]]
    assert path.read_bytes() == before


def test_explicit_incomplete_and_future_rows_are_deferred(tmp_path):
    records = [row(complete=True), row(START+60, complete=False, close="nan"),
               row(START+120, complete=True)]
    path = write_csv(tmp_path/"x.csv", records)
    arrays, receipt = inputs.read_csv_tail(path, PAIR, START+150)
    assert arrays["time"].tolist() == [START]
    assert receipt["completion_basis"] == ["explicit_complete"]
    assert receipt["skipped_rows"] == {"explicit_incomplete": 1, "bar_end_after_observation": 1}


def test_partial_tail_does_not_block_previous_completed_rows(tmp_path):
    path = write_csv(tmp_path/"x.csv", [row()])
    with path.open("ab") as handle: handle.write(b"2027-01-15T")
    arrays, receipt = inputs.read_csv_tail(path, PAIR, START+60)
    assert arrays["time"].tolist() == [START]
    assert receipt["deferred_partial_tail_bytes"] == len(b"2027-01-15T")


def test_tail_bounds_and_row_limit_are_explicit(tmp_path):
    path = write_csv(tmp_path/"x.csv", [row(START+60*i) for i in range(20)])
    arrays, receipt = inputs.read_csv_tail(path, PAIR, START+1200, max_rows=3, max_bytes=1600)
    assert arrays["time"].tolist() == [START+60*i for i in (17, 18, 19)]
    assert receipt["source_range_end"]-receipt["source_range_start"] <= 1600
    assert receipt["range_truncated"] is True
    with pytest.raises(ValueError, match="bounded_tail"):
        inputs.read_csv_tail(path, PAIR, START+1200, max_bytes=inputs.MAX_TAIL_BYTES+1)


@pytest.mark.parametrize("change,error", [
    ({"instrument": "GBP_USD"}, "pair_mismatch"),
    ({"granularity": "M5"}, "timeframe_mismatch"),
    ({"close": "nan"}, "invalid_candle_number"),
    ({"volume": -1}, "invalid_candle_number"),
    ({"high": 1.19}, "ohlc_geometry"),
    ({"bid_close": 1.215}, "bid_mid_ask"),
    ({"complete": "maybe"}, "completion_flag"),
    ({"datetime": "2027-01-15T08:00:00"}, "aware_original"),
    ({"time": "2027-01-15T08:00:00.000000001Z"}, "minute_aligned"),
])
def test_invalid_selected_inputs_are_not_zero_filled(tmp_path, change, error):
    path = write_csv(tmp_path/"x.csv", [row(**change)])
    with pytest.raises(ValueError, match=error):
        inputs.read_csv_tail(path, PAIR, START+60)


def test_clock_order_is_checked_across_csv_chunks(tmp_path):
    path = write_csv(tmp_path/"x.csv", [row(), row()])
    chunks = inputs.iter_csv(path, PAIR, START+60, batch_rows=1)
    assert next(chunks)[0]["time"].tolist() == [START]
    with pytest.raises(ValueError, match="duplicate_or_unordered"):
        next(chunks)


def test_header_and_clock_aliases_must_agree(tmp_path):
    path = write_csv(tmp_path/"x.csv", [row(datetime=datetime.fromtimestamp(START+60, timezone.utc).isoformat())])
    with pytest.raises(ValueError, match="clock_missing_or_conflicting"):
        inputs.read_csv_tail(path, PAIR, START+120)
    path.write_text("time,time,open\n", encoding="utf-8")
    with pytest.raises(ValueError, match="duplicate_csv_columns"):
        inputs.read_csv_tail(path, PAIR, START+120)


def test_csv_arbitrary_chunks_match_single_tail(tmp_path):
    records = [row(START+60*i) for i in [0, 1, 5, 6, 7]]
    path = write_csv(tmp_path/"x.csv", records)
    expected, expected_receipt = inputs.read_csv_tail(path, PAIR, START+600)
    chunks = list(inputs.iter_csv(path, PAIR, START+600, batch_rows=2))
    for key in inputs.FIELDS:
        np.testing.assert_array_equal(np.concatenate([a[key] for a, _ in chunks]), expected[key])
    assert sum([r["row_hashes"] for _, r in chunks], []) == expected_receipt["row_hashes"]


def test_csv_append_is_deferred_until_next_fixed_extent_traversal(tmp_path):
    path = write_csv(tmp_path/"x.csv", [row(), row(START+60)])
    chunks = inputs.iter_csv(path, PAIR, START+180, batch_rows=1)
    assert next(chunks)[0]["time"].tolist() == [START]
    with path.open("a", encoding="utf-8", newline="") as handle:
        csv.DictWriter(handle, fieldnames=list(row())).writerow(row(START+120))
    assert next(chunks)[0]["time"].tolist() == [START+60]
    with pytest.raises(StopIteration): next(chunks)
    assert inputs.read_csv_tail(path, PAIR, START+180)[0]["time"].tolist()[-1] == START+120


def test_csv_replacement_between_chunks_is_refused(tmp_path, monkeypatch):
    path = write_csv(tmp_path/"x.csv", [row(), row(START+60)])
    chunks = inputs.iter_csv(path, PAIR, START+120, batch_rows=1)
    next(chunks)
    # Windows may refuse replacement of this open file. Simulate the file
    # identity result to exercise the same guard without depending on sharing.
    original_stat = Path.stat
    def replaced_stat(self, *args, **kwargs):
        value = original_stat(self, *args, **kwargs)
        if self == path:
            return SimpleNamespace(st_dev=value.st_dev, st_ino=value.st_ino+1,
                                   st_size=value.st_size, st_mtime_ns=value.st_mtime_ns)
        return value
    monkeypatch.setattr(Path, "stat", replaced_stat)
    with pytest.raises(ValueError, match="replaced"):
        next(chunks)


def test_parquet_and_csv_share_numeric_contract(tmp_path):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    records = [row(START+60*i) for i in [0, 1, 5, 6]]
    path = write_csv(tmp_path/"x.csv", records)
    parquet = tmp_path/"x.parquet"
    # Historical Parquet may omit the filename-bound identity columns.
    historical = [{k:v for k,v in r.items() if k not in ("instrument", "granularity")} for r in records]
    pq.write_table(pa.Table.from_pylist(historical), parquet)
    chunks = list(inputs.iter_parquet(parquet, PAIR, START+420, batch_rows=2))
    expected, receipt = inputs.read_csv_tail(path, PAIR, START+420)
    for name in inputs.FIELDS:
        np.testing.assert_array_equal(np.concatenate([a[name] for a, _ in chunks]), expected[name])
    assert sum([r["row_value_hashes"] for _, r in chunks], []) == receipt["row_value_hashes"]
    assert all(r["completion_counts"] == {"inferred_bar_end": 2} for _,r in chunks)


def test_parquet_duplicate_across_batch_is_rejected(tmp_path):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    path = tmp_path/"x.parquet"
    pq.write_table(pa.Table.from_pylist([row(), row()]), path)
    chunks = inputs.iter_parquet(path, PAIR, START+60, batch_rows=1)
    next(chunks)
    with pytest.raises(ValueError, match="duplicate_or_unordered"):
        next(chunks)


def test_discovery_verifies_receipts_without_claiming_full_ohlc_seal(tmp_path):
    recipe = {"cutoff_ns": START*1_000_000_000, "raw_rows": 2,
              "source_receipts": {}, "source_receipt_sha256": {}}
    for lane, name in [("canonical_c_m1", "EUR_USD_M1.csv"), ("reacquired_m1_20260725", "EUR_USD_M1.parquet")]:
        path = tmp_path/(lane+".json")
        raw = json.dumps({"source": {"path": str(tmp_path/name), "bytes": 100},
                          "retained_projection": {"columns": ["time_ns", "mid", "bid", "ask", "complete"]}}).encode()
        path.write_bytes(raw)
        recipe["source_receipts"][lane] = path.name
        recipe["source_receipt_sha256"][lane] = hashlib.sha256(raw).hexdigest()
    path = tmp_path/"manifest.json"
    raw = json.dumps({"schema": inputs.MANIFEST_SCHEMA, "pairs": {PAIR: recipe}}).encode()
    path.write_bytes(raw)
    path.with_suffix(".seal.json").write_text(json.dumps({"manifest_sha256": hashlib.sha256(raw).hexdigest()}))
    result = inputs.discover_archive_sources(path)[PAIR]
    assert result["cutoff_epoch"] == START
    assert result["reacquired_path"].endswith("EUR_USD_M1.parquet")
    assert result["source_receipts"]["canonical_c_m1"]["current_original_bytes_verified"] is False
    (tmp_path/"canonical_c_m1.json").write_text("{}")
    with pytest.raises(ValueError, match="source_receipt_hash"):
        inputs.discover_archive_sources(path)
