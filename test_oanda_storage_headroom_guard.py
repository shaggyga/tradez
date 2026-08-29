import gzip
import json
from datetime import datetime, timedelta, timezone

import oanda_storage_headroom_guard as guard
import oanda_verified_log_archiver as archiver


def test_storage_classification_has_absolute_and_fractional_guards():
    total = 1000 * 1024**3
    assert guard.classify(free_bytes=65 * 1024**3, total_bytes=total) == "ok"
    assert guard.classify(free_bytes=40 * 1024**3, total_bytes=total) == "warning"
    assert guard.classify(free_bytes=20 * 1024**3, total_bytes=total) == "critical"


def test_growth_projection_counts_positive_database_growth_without_wal_netting():
    now = datetime(2026, 8, 19, 2, 0, tzinfo=timezone.utc)
    paths = ["a.sqlite", "b.sqlite"]
    previous = {
        "generated_utc": (now - timedelta(days=1)).isoformat(),
        "databases": [
            {"path": paths[0], "allocated_bytes": 10_000, "wal_bytes": 2_000},
            {"path": paths[1], "allocated_bytes": 8_000, "wal_bytes": 4_000},
        ],
    }
    current = [
        {"path": paths[0], "allocated_bytes": 15_000, "wal_bytes": 2_000},
        {"path": paths[1], "allocated_bytes": 8_000, "wal_bytes": 1_000},
    ]

    result = guard.growth_projection(
        previous=previous,
        generated=now.isoformat(),
        databases=current,
        free_bytes=100 * 1024**3,
    )

    assert result["usable"] is True
    assert result["positive_growth_bytes"] == 5_000
    assert result["estimated_positive_growth_bytes_per_day"] == 5_000
    assert result["databases"][1]["delta_bytes"] == -3_000


def test_growth_projection_refuses_short_noisy_window():
    now = datetime(2026, 8, 19, 2, 0, tzinfo=timezone.utc)
    previous = {
        "generated_utc": (now - timedelta(seconds=60)).isoformat(),
        "databases": [{"path": "a.sqlite", "allocated_bytes": 1, "wal_bytes": 0}],
    }
    result = guard.growth_projection(
        previous=previous,
        generated=now.isoformat(),
        databases=[{"path": "a.sqlite", "allocated_bytes": 1_000_000, "wal_bytes": 0}],
        free_bytes=100 * 1024**3,
    )
    assert result["usable"] is False
    assert result["status"] == "insufficient_observation_window"
    assert result["estimated_positive_growth_bytes_per_day"] == 0.0


def test_verified_rotated_log_archive_round_trips_before_removal(tmp_path):
    root = tmp_path / "logs"
    root.mkdir()
    source = root / "worker.part_20260801.jsonl"
    raw = b'{"a":1}\n{"b":2}\n'
    source.write_bytes(raw)

    result = archiver.archive_one(source, root)

    archive = root / "worker.part_20260801.jsonl.gz"
    manifest = json.loads(
        (root / "worker.part_20260801.jsonl.gz.manifest.json").read_text()
    )
    assert not source.exists()
    assert gzip.open(archive, "rb").read() == raw
    assert result["round_trip_verified"] is True
    assert manifest["line_count"] == 2


def test_archiver_refuses_active_or_out_of_root_logs(tmp_path):
    root = tmp_path / "logs"
    root.mkdir()
    active = root / "active.jsonl"
    active.write_text("{}\n", encoding="utf-8")
    try:
        archiver.archive_one(active, root)
    except ValueError as exc:
        assert "refusing" in str(exc)
    else:
        raise AssertionError("active log was accepted")


def test_archiver_preserves_lifetime_totals_on_empty_followup(tmp_path):
    root = tmp_path / "logs"
    root.mkdir()
    state = tmp_path / "state.json"
    source = root / "worker.part_old.jsonl"
    source.write_bytes(b"{}\n")
    old = source.stat().st_mtime - 72 * 3600
    import os
    os.utime(source, (old, old))

    first = archiver.run(root=root, state=state, minimum_age_hours=48)
    second = archiver.run(root=root, state=state, minimum_age_hours=48)

    assert first["lifetime"]["archived_file_count"] == 1
    assert second["archived_file_count"] == 0
    assert second["lifetime"]["archived_file_count"] == 1
    assert second["lifetime"]["reclaimed_bytes"] == max(
        0,
        second["lifetime"]["source_bytes"]
        - second["lifetime"]["compressed_bytes"]
        - second["lifetime"]["manifest_bytes"],
    )
