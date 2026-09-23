from __future__ import annotations

import hashlib
import io
import json
import subprocess
import sys
import zipfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import quote_input_qualification_v2 as quotes
from publication import sha256_file

ORIGIN = quotes.runner.parse_utc_epoch("2024-06-24T00:00:00Z")
TARGET = ORIGIN + 86400


def parquet_bytes(times: list[str], bids: list[float] | None = None, asks: list[float] | None = None) -> bytes:
    columns = {"datetime": times, "close": [1.11] * len(times)}
    if bids is not None:
        columns["bid_close"] = bids
    if asks is not None:
        columns["ask_close"] = asks
    buffer = io.BytesIO()
    pq.write_table(pa.table(columns), buffer, row_group_size=1)
    return buffer.getvalue()


def test_exact_quote_rows_are_found_across_groups_and_offset_forms():
    raw = parquet_bytes(["2024-06-23T23:59:00Z", "2024-06-23T20:00:00-04:00", "2024-06-25T00:00:00Z"],
                        [1.09, 1.1, 1.2], [1.10, 1.12, 1.21])
    result = quotes.qualify_member(raw, "EUR_USD", ORIGIN, TARGET)
    assert result["status"] == "both_candle_close_pairs_valid"
    assert result["parquet_row_groups"] == 3
    assert result["origin"]["bid_close"] == 1.1
    assert result["origin"]["ask_close"] == 1.12
    assert result["origin"]["spread_price_units"] == pytest.approx(0.02)
    assert result["target"]["bid_close"] == 1.2
    assert result["target"]["assumed_bar_ready_epoch"] == TARGET + 60


@pytest.mark.parametrize("bid,ask", [(float("inf"), 1.2), (1.1, float("nan")), (0.0, 1.2), (-1.0, 1.2), (1.3, 1.2)])
def test_nonfinite_nonpositive_or_crossed_quotes_fail_closed(bid: float, ask: float):
    raw = parquet_bytes(["2024-06-24T00:00:00Z", "2024-06-25T00:00:00Z"], [bid, 1.1], [ask, 1.2])
    result = quotes.qualify_member(raw, "EUR_USD", ORIGIN, TARGET)
    assert result["origin"]["status"] == "invalid_bid_ask_close"
    assert result["origin"]["invalid_quote_row_count"] == 1
    assert result["target"]["status"] == "valid_candle_close_pair"
    assert result["status"] == "candle_close_pair_unavailable_or_invalid"


def test_duplicate_exact_origin_and_missing_target_are_reported_separately():
    raw = parquet_bytes(["2024-06-24T00:00:00Z", "2024-06-24T00:00:00+00:00"], [1.1, 1.1], [1.2, 1.2])
    result = quotes.qualify_member(raw, "EUR_USD", ORIGIN, TARGET)
    assert result["origin"]["status"] == "duplicate_exact_bar"
    assert result["origin"]["duplicate_extra_count"] == 1
    assert result["target"]["status"] == "missing_exact_bar"
    assert result["target"]["missing_count"] == 1


def test_midpoint_only_fixture_is_not_mislabeled_as_quote_data():
    raw = parquet_bytes(["2024-06-24T00:00:00Z", "2024-06-25T00:00:00Z"])
    result = quotes.qualify_member(raw, "EUR_USD", ORIGIN, TARGET)
    assert not result["bid_ask_close_columns_present"]
    assert result["origin"]["status"] == "missing_bid_ask_columns"
    assert result["target"]["status"] == "missing_bid_ask_columns"


def make_archive(root: Path) -> tuple[Path, Path]:
    archive_path, manifest_path = root / "quotes.zip", root / "INPUT_ARCHIVES.json"
    members = []
    raw = parquet_bytes(["2024-06-24T00:00:00Z", "2024-06-25T00:00:00Z"], [1.1, 1.2], [1.11, 1.22])
    with zipfile.ZipFile(archive_path, "x") as archive:
        for index in range(68):
            instrument = f"PAIR{index:02d}"
            name = f"{instrument}.parquet"
            archive.writestr(name, raw)
            members.append({"path": name, "instrument": instrument, "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()})
    manifest_path.write_bytes(quotes.runner.json_bytes({"schema": "forex_portable_audited_inputs_v1",
        "input_tier": "synthetic_contract_fixture.v2", "archives": [{"path": "inputs/long_m1_68.zip",
        "sha256": sha256_file(archive_path), "members": members}]}))
    return archive_path, manifest_path


def test_cli_qualifies_all68_with_source_integrity_and_explicit_provenance_limits(tmp_path: Path):
    archive, manifest = make_archive(tmp_path)
    output = tmp_path / "report.json"
    result = subprocess.run([sys.executable, "-I", "-B", str(Path(quotes.__file__)), "--archive", str(archive),
                             "--input-manifest", str(manifest), "--output", str(output)],
                            capture_output=True, text=True, timeout=30, check=False)
    assert result.returncode == 0, result.stderr
    report = json.loads(output.read_bytes())
    assert report["counts"]["both_candle_close_pairs_valid"] == 68
    assert report["counts"]["origin"]["valid_pairs"] == 68
    assert report["source_integrity"]["member_sha256_and_size_verified_count"] == 68
    assert not report["source_integrity"]["archive_container_sha256_recomputed"]
    assert not report["execution_ready"]
    assert report["clock_contract"]["actual_quote_arrival_provenance"] == "unverified"


def test_changed_member_bytes_reject_the_whole_qualification(tmp_path: Path):
    archive, manifest = make_archive(tmp_path)
    data = json.loads(manifest.read_bytes())
    data["archives"][0]["members"][0]["sha256"] = "f" * 64
    manifest.write_bytes(quotes.runner.json_bytes(data))
    with pytest.raises(ValueError, match="source_member_hash_or_size_mismatch"):
        quotes.qualify_archive(archive, manifest, ORIGIN, 86400)
