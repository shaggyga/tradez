from __future__ import annotations

import hashlib
import json

import pyarrow as pa
import pyarrow.parquet as pq
import sqlite3

from trad.oanda_shadow_archive_integrity import audit, reconcile_detail_counts


def test_manifested_and_legacy_parts_are_distinguished(tmp_path) -> None:
    valid = tmp_path / "valid.parquet"
    legacy = tmp_path / "legacy.parquet"
    pq.write_table(pa.table({"row_id": [1, 2], "value": [3.0, 4.0]}), valid)
    pq.write_table(pa.table({"row_id": [3]}), legacy)
    digest = hashlib.sha256(valid.read_bytes()).hexdigest()
    valid.with_suffix(".manifest.json").write_text(json.dumps({
        "parquet_path": str(valid), "row_count": 2, "column_count": 2,
        "size_bytes": valid.stat().st_size, "sha256": digest,
    }), encoding="utf-8")
    result = audit(tmp_path)
    assert result["status"] == "ok_with_legacy_unmanifested"
    assert result["validated_manifest_parts"] == 1
    assert result["legacy_unmanifested_parts"] == 1
    assert result["total_rows"] == 3
    assert result["total_unique_rows"] == 3


def test_exact_duplicate_row_ids_do_not_inflate_reconciliation(tmp_path) -> None:
    day = tmp_path / "2026-08-10"
    day.mkdir()
    pq.write_table(pa.table({"row_id": [1, 2]}), day / "first.parquet")
    pq.write_table(pa.table({"row_id": [2, 3]}), day / "second.parquet")
    result = audit(tmp_path)
    assert result["total_rows"] == 4
    assert result["total_unique_rows"] == 3
    assert result["duplicate_rows"] == 1
    assert result["unique_rows_by_observed_date"]["2026-08-10"] == 3


def test_tampered_manifested_part_fails(tmp_path) -> None:
    target = tmp_path / "part.parquet"
    pq.write_table(pa.table({"row_id": [1]}), target)
    target.with_suffix(".manifest.json").write_text(json.dumps({
        "parquet_path": str(target), "row_count": 2, "column_count": 1,
        "size_bytes": target.stat().st_size, "sha256": "bad",
    }), encoding="utf-8")
    result = audit(tmp_path)
    assert result["status"] == "failed"
    assert result["failures"]


def test_reconciliation_localizes_gap_by_observed_date(tmp_path) -> None:
    archive = tmp_path / "archive"
    day = archive / "2026-08-07"
    day.mkdir(parents=True)
    pq.write_table(pa.table({"row_id": [1, 2]}), day / "part.parquet")
    result = audit(archive)
    rollup = tmp_path / "rollup.sqlite"
    db = sqlite3.connect(rollup)
    db.execute("CREATE TABLE hourly_rollups(source_name TEXT, hour_utc TEXT, sample_count INTEGER)")
    db.execute("INSERT INTO hourly_rollups VALUES (?,?,?)", ("source", "2026-08-07T00:00:00+00:00", 3))
    db.commit(); db.close()
    source = tmp_path / "source.sqlite"
    db = sqlite3.connect(source)
    db.execute("CREATE TABLE outcomes(observed_utc TEXT)")
    db.commit(); db.close()
    evidence = tmp_path / "evidence.sqlite"
    db = sqlite3.connect(evidence)
    db.execute("CREATE TABLE immutable_daily_snapshots(utc_day TEXT,row_count INTEGER,evidence_sha256 TEXT)")
    db.execute("INSERT INTO immutable_daily_snapshots VALUES (?,?,?)", ("2026-08-07", 3, "abc"))
    db.commit(); db.close()
    reconciliation = reconcile_detail_counts(
        result, rollup=rollup, source=source, evidence=evidence
    )
    assert reconciliation["detail_gap_vs_rollup"] == 1
    assert reconciliation["nonzero_gap_dates"] == [{
        "observed_date": "2026-08-07", "rollup_rows": 3,
        "archived_rows": 2, "live_rows": 0, "detail_gap": 1,
        "frozen_snapshot_rows": 3, "frozen_snapshot_sha256": "abc",
        "snapshot_matches_rollup": True,
    }]
    assert reconciliation["gap_dates_have_matching_frozen_snapshot"] is True


def test_live_detail_ahead_of_rollup_is_not_a_missing_archive_gap(tmp_path) -> None:
    archive = tmp_path / "archive"
    archive.mkdir()
    result = audit(archive)
    rollup = tmp_path / "rollup.sqlite"
    db = sqlite3.connect(rollup)
    db.execute("CREATE TABLE hourly_rollups(source_name TEXT, hour_utc TEXT, sample_count INTEGER)")
    db.execute("INSERT INTO hourly_rollups VALUES (?,?,?)", ("source", "2026-08-10T00:00:00+00:00", 1))
    db.commit(); db.close()
    source = tmp_path / "source.sqlite"
    db = sqlite3.connect(source)
    db.execute("CREATE TABLE outcomes(observed_utc TEXT)")
    db.executemany(
        "INSERT INTO outcomes VALUES (?)",
        [("2026-08-10T00:01:00+00:00",), ("2026-08-10T00:02:00+00:00",)],
    )
    db.commit(); db.close()
    reconciliation = reconcile_detail_counts(result, rollup=rollup, source=source)
    assert reconciliation["detail_gap_vs_rollup"] == -1
    assert reconciliation["historical_missing_detail_rows"] == 0
    assert reconciliation["live_detail_ahead_of_rollup_rows"] == 1
    assert reconciliation["positive_gap_dates"] == []
    assert reconciliation["gap_dates_have_matching_frozen_snapshot"] is True
    assert reconciliation["interpretation"] == "live_detail_ahead_of_rollup_expected"
