from __future__ import annotations

import json
import ast
from datetime import datetime, timezone
from pathlib import Path

import pytest

from oanda_sequential_replay_source_pack import identity_manifest
from src.forex_system.research.sequential_replay_source_pack_v1 import (
    build_packed_slice,
    deterministic_gzip,
    read_source,
    schedule_clocks,
    validate_config,
)


ROOT = Path(__file__).resolve().parent
HEADER = (
    "time,datetime,instrument,granularity,open,high,low,close,bid_open,bid_high,"
    "bid_low,bid_close,ask_open,ask_high,ask_low,ask_close,spread_pips,volume"
)
REQUIRED = HEADER.split(",")


def iso(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000000000Z")


def make_source(path: Path, *, start: int, minutes: int, missing: set[int] | None = None) -> None:
    rows = [HEADER]
    for offset in range(minutes):
        epoch = start + offset * 60
        if missing and epoch in missing:
            continue
        stamp = iso(epoch)
        rows.append(
            f"{stamp},{stamp},EUR_USD,M1,1.1000,1.1002,1.0998,1.1001,"
            "1.0999,1.1001,1.0997,1.1000,1.1001,1.1003,1.0999,1.1002,2.0,10"
        )
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")


def test_schedule_is_fixed_before_quote_coverage() -> None:
    assert schedule_clocks(1_800, 3_000, 5) == (1_800, 2_100, 2_400, 2_700)


def test_missing_execution_quote_is_recorded_without_removing_clock(tmp_path: Path) -> None:
    session_start = 1_800_000_000
    first_source = session_start - 61 * 60
    missing_execution = session_start + 60
    source_path = tmp_path / "EUR_USD_M1.csv"
    make_source(source_path, start=first_source, minutes=67, missing={missing_execution})
    source = read_source(
        source_path,
        instrument="EUR_USD",
        required_columns=REQUIRED,
        maximum_bytes=1_000_000,
    )
    schedule = {
        "decision_cadence_min": 5,
        "feature_lookback_min": 60,
        "execution_delay_min": 1,
        "feedback_horizon_min": 5,
    }
    packed = build_packed_slice(
        source,
        session={
            "session_key": "fixture",
            "start_utc": iso(session_start),
            "end_utc_exclusive": iso(session_start + 5 * 60),
        },
        schedule=schedule,
        compresslevel=9,
        maximum_archive_raw_bytes=1_000_000,
    )
    assert packed.manifest["scheduled_clock_count"] == 1
    assert packed.manifest["execution_ready_clock_count"] == 0
    assert packed.manifest["coverage_failure_count"] == 1
    assert packed.manifest["coverage_failures"][0]["execution_quote_ready"] is False


def test_deterministic_gzip_and_identity_ignore_append_only_capture_metadata(tmp_path: Path) -> None:
    raw = b"header\nrow\n"
    assert deterministic_gzip(raw) == deterministic_gzip(raw)
    row = {key: 0 for key in (
        "session_start_epoch", "session_end_epoch", "slice_first_epoch", "slice_last_epoch",
        "scheduled_clock_count", "context_ready_clock_count", "execution_ready_clock_count",
        "feedback_ready_clock_count", "fully_ready_clock_count", "coverage_failure_count",
        "expected_minute_count", "present_minute_count", "missing_minute_count", "raw_bytes",
        "gzip_bytes",
    )}
    row.update({
        "instrument": "EUR_USD", "session_key": "fixture",
        "scheduled_clock_sha256": "a", "coverage_failures_sha256": "b",
        "missing_minutes_sha256": "c", "raw_sha256": "d", "gzip_sha256": "e",
        "source_file_sha256_at_capture": "old", "source_file_bytes_at_capture": 1,
    })
    first = identity_manifest(row)
    row["source_file_sha256_at_capture"] = "new"
    row["source_file_bytes_at_capture"] = 2
    assert identity_manifest(row) == first


def test_config_freezes_exact_68_pair_universe() -> None:
    config = json.loads((ROOT / "config" / "sequential_replay_source_pack_v1.json").read_text(encoding="utf-8"))
    validate_config(config)
    assert len(config["source"]["expected_instruments"]) == 68
    assert len(config["schedule"]["sessions"]) == 3


def test_wednesday_expansion_is_calendar_fixed_and_marks_prior_discovery() -> None:
    config = json.loads(
        (
            ROOT
            / "config"
            / "sequential_replay_source_pack_wednesday_expansion_v1.json"
        ).read_text(encoding="utf-8")
    )
    validate_config(config)
    sessions = config["schedule"]["sessions"]
    assert len(config["source"]["expected_instruments"]) == 68
    assert len(sessions) == 7
    assert sum(
        len(schedule_clocks(parse_start, parse_end, 5))
        for parse_start, parse_end in (
            (
                int(datetime.fromisoformat(row["start_utc"].replace("Z", "+00:00")).timestamp()),
                int(datetime.fromisoformat(row["end_utc_exclusive"].replace("Z", "+00:00")).timestamp()),
            )
            for row in sessions
        )
    ) == 336
    assert config["independence"]["prior_discovery_session_keys"] == [
        "20260826_wed_overlap_prior_discovery"
    ]
    assert config["independence"]["new_scheduled_clock_count"] == 288
    assert config["independence"]["prior_discovery_scheduled_clock_count"] == 48


def test_source_size_and_identity_fail_closed(tmp_path: Path) -> None:
    source_path = tmp_path / "EUR_USD_M1.csv"
    make_source(source_path, start=1_800_000_000, minutes=2)
    with pytest.raises(ValueError, match="source size outside contract"):
        read_source(
            source_path,
            instrument="EUR_USD",
            required_columns=REQUIRED,
            maximum_bytes=10,
        )
    with pytest.raises(ValueError, match="source identity mismatch"):
        read_source(
            source_path,
            instrument="USD_JPY",
            required_columns=REQUIRED,
            maximum_bytes=1_000_000,
        )


def test_source_pack_code_has_no_execution_imports() -> None:
    forbidden = ("oandapy", "requests", "account_007", "authorization", "execution")
    paths = (
        ROOT / "src" / "forex_system" / "research" / "sequential_replay_source_pack_v1.py",
        ROOT / "oanda_sequential_replay_source_pack.py",
        ROOT / "oanda_sequential_replay_source_pack_verifier.py",
    )
    imports: list[str] = []
    for path in paths:
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.extend(alias.name.lower() for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                imports.append(str(node.module or "").lower())
    joined = "\n".join(imports)
    for token in forbidden:
        assert token not in joined
