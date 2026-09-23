from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from pathlib import Path

import oanda_executable_move_census_v3 as subject
import oanda_executable_move_census_v3f_verifier as verifier
import oanda_executable_move_census_v3g_verifier as g_verifier
import oanda_executable_move_census_v3h_verifier as h_verifier


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_frozen_v3f_contract_is_literal_and_inert() -> None:
    assert _sha(verifier.CONFIG_PATH) == verifier.FROZEN_CONFIG_SHA256
    assert _sha(Path(subject.__file__)) == verifier.FROZEN_PRODUCER_SHA256
    assert _sha(verifier.SOURCE_PRODUCER_PATH) == verifier.FROZEN_SOURCE_PRODUCER_SHA256
    assert _sha(verifier.QUOTE_TRANSPORT_PATH) == verifier.FROZEN_QUOTE_TRANSPORT_SHA256
    assert verifier.COHORT_ID == "all68_executable_move_census_v3_20260901f"
    assert verifier.CAPTURE_CONTRACT_ID.endswith("retry_incident_logged")
    assert verifier.CAPTURE_COHORT_ID.endswith("20260901f")
    config = json.loads(verifier.CONFIG_PATH.read_text(encoding="utf-8"))
    assert config["historical_row_import_count"] == 0
    assert config["no_backfill"] is True
    assert config["research_only"] is True
    assert config["can_trade"] is False
    assert config["can_authorize"] is False
    assert config["can_promote"] is False


def test_v3f_projection_freshness_matches_bounded_full_reconstruction() -> None:
    original = verifier.prior.base.MAXIMUM_LATEST_AGE_SEC
    assert verifier.MAXIMUM_LATEST_AGE_SEC == 600.0
    with verifier.cohort_f_contract():
        assert verifier.prior.base.MAXIMUM_LATEST_AGE_SEC == 600.0
        assert verifier.prior.base.MAXIMUM_QUOTE_AGE_SEC == 30.0
        assert verifier.prior.base.MAXIMUM_CAPTURE_DELAY_SEC == 55.0
    assert verifier.prior.base.MAXIMUM_LATEST_AGE_SEC == original


def test_frozen_v3g_contract_is_literal_inert_and_zero_import() -> None:
    assert _sha(g_verifier.CONFIG_PATH) == g_verifier.FROZEN_CONFIG_SHA256
    assert _sha(Path(subject.__file__)) == g_verifier.FROZEN_PRODUCER_SHA256
    assert _sha(g_verifier.SOURCE_PRODUCER_PATH) == (
        g_verifier.FROZEN_SOURCE_PRODUCER_SHA256
    )
    assert _sha(g_verifier.QUOTE_TRANSPORT_PATH) == (
        g_verifier.FROZEN_QUOTE_TRANSPORT_SHA256
    )
    assert g_verifier.COHORT_ID == "all68_executable_move_census_v3_20260902g"
    assert g_verifier.CAPTURE_COHORT_ID.endswith("20260902g")
    assert "exclusive_quote_owner" in g_verifier.CAPTURE_CONTRACT_ID
    config = json.loads(g_verifier.CONFIG_PATH.read_text(encoding="utf-8"))
    assert config["parent_cohort_id"] == verifier.COHORT_ID
    assert config["parent_evidence_status"] == "permanently_invalid"
    assert config["historical_row_import_count"] == 0
    assert config["no_backfill"] is True
    assert config["research_only"] is True
    assert config["can_trade"] is False
    assert config["can_authorize"] is False
    assert config["can_promote"] is False


def test_v3g_checkpoint_is_bound_to_g_not_inherited_cohort(tmp_path: Path) -> None:
    checkpoint = tmp_path / "checkpoint.json"
    runtime_cache = {
        "database_path": "census-g.sqlite",
        "frame_count": 7,
        "frame_chain_sha256": "frame-chain",
        "last_generated_utc": "2026-09-02T11:36:00Z",
        "evaluation_rows": {("2026-09-02T11:30:00Z", 1): "row-hash"},
        "factor_episodes": {"episode": "episode-hash"},
    }

    g_verifier.write_runtime_checkpoint(checkpoint, runtime_cache)
    payload = json.loads(checkpoint.read_text(encoding="utf-8"))

    assert payload["cohort_id"] == g_verifier.COHORT_ID
    assert payload["pins"] == g_verifier.checkpoint_pins()
    assert payload["pins"]["verifier_sha256"] == _sha(Path(g_verifier.__file__))
    assert g_verifier.load_runtime_checkpoint(checkpoint) == runtime_cache

    payload["cohort_id"] = verifier.COHORT_ID
    payload.pop("content_sha256_excluding_this_field")
    payload["content_sha256_excluding_this_field"] = g_verifier.base.base.sha(
        g_verifier.base.base.canonical(payload)
    )
    checkpoint.write_text(json.dumps(payload), encoding="utf-8")
    assert g_verifier.load_runtime_checkpoint(checkpoint) == {}


def test_frozen_v3h_contract_is_literal_inert_and_zero_import() -> None:
    assert _sha(h_verifier.CONFIG_PATH) == h_verifier.FROZEN_CONFIG_SHA256
    assert _sha(Path(subject.__file__)) == h_verifier.FROZEN_PRODUCER_SHA256
    assert _sha(h_verifier.SOURCE_PRODUCER_PATH) == (
        h_verifier.FROZEN_SOURCE_PRODUCER_SHA256
    )
    assert _sha(h_verifier.QUOTE_TRANSPORT_PATH) == (
        h_verifier.FROZEN_QUOTE_TRANSPORT_SHA256
    )
    assert h_verifier.COHORT_ID == "all68_executable_move_census_v3_20260902h"
    assert h_verifier.CAPTURE_COHORT_ID.endswith("20260902h")
    config = json.loads(h_verifier.CONFIG_PATH.read_text(encoding="utf-8"))
    assert config["parent_cohort_id"] == g_verifier.COHORT_ID
    assert config["parent_evidence_status"] == "permanently_invalid"
    assert config["historical_row_import_count"] == 0
    assert config["no_backfill"] is True
    assert config["research_only"] is True
    assert config["can_trade"] is False
    assert config["can_authorize"] is False
    assert config["can_promote"] is False


def test_v3h_checkpoint_is_bound_to_h_not_inherited_cohort(tmp_path: Path) -> None:
    checkpoint = tmp_path / "checkpoint.json"
    runtime_cache = {
        "database_path": "census-h.sqlite",
        "frame_count": 1,
        "frame_chain_sha256": "frame-chain-h",
        "last_generated_utc": "2026-09-02T14:40:00Z",
        "evaluation_rows": {("2026-09-02T14:40:00Z", 1): "row-hash"},
        "factor_episodes": {"episode-h": "episode-hash"},
    }
    h_verifier.write_runtime_checkpoint(checkpoint, runtime_cache)
    payload = json.loads(checkpoint.read_text(encoding="utf-8"))
    assert payload["cohort_id"] == h_verifier.COHORT_ID
    assert payload["pins"] == h_verifier.checkpoint_pins()
    assert payload["pins"]["verifier_sha256"] == _sha(Path(h_verifier.__file__))
    assert h_verifier.load_runtime_checkpoint(checkpoint) == runtime_cache


def test_v3h_semantic_diagnostics_are_inherited_from_f_verifier() -> None:
    assert callable(h_verifier.previous.previous.semantic_validity_diagnostics)


def test_semantic_diagnostics_ignore_append_only_suffix_after_verified_prefix(
    tmp_path: Path,
) -> None:
    database = tmp_path / "census.sqlite"
    connection = sqlite3.connect(database)
    try:
        connection.executescript(
            """
            CREATE TABLE frames (
                frame_id TEXT PRIMARY KEY,
                scheduled_utc TEXT NOT NULL,
                market_open INTEGER NOT NULL,
                valid_count INTEGER NOT NULL
            );
            CREATE TABLE frame_quotes (
                frame_id TEXT NOT NULL,
                state TEXT NOT NULL,
                reason TEXT NOT NULL
            );
            """
        )
        connection.executemany(
            "INSERT INTO frames VALUES (?,?,?,?)",
            [
                ("frame-1", "2026-09-02T06:00:00Z", 1, 1),
                ("frame-2", "2026-09-02T06:01:00Z", 1, 0),
            ],
        )
        connection.executemany(
            "INSERT INTO frame_quotes VALUES (?,?,?)",
            [
                ("frame-1", "valid", ""),
                ("frame-2", "invalid", "source_identity_mismatch"),
            ],
        )
        connection.commit()
    finally:
        connection.close()

    observed = verifier.semantic_validity_diagnostics(
        database,
        verified_frame_count=1,
    )

    assert observed["current_database_frames"] == 2
    assert observed["verified_frame_scope"] == 1
    assert observed["valid_quote_rows"] == 1
    assert observed["invalid_quote_rows"] == 0
    assert observed["source_identity_mismatch_rows"] == 0
    assert observed["zero_valid_open_market_frames"] == 0


def test_retrying_capture_preserves_each_failure_then_succeeds(tmp_path: Path) -> None:
    attempts = 0
    now_epoch = [1_788_282_001.0]

    def operation(**_: object) -> dict[str, object]:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise ValueError(f"transient-{attempts}")
        return {
            "status": "ok",
            "frame_inserted": True,
            "frame_count": 1,
            "latest_frame_utc": "2026-09-01T21:00:00Z",
        }

    def clock() -> float:
        return now_epoch[0]

    def sleeper(_: float) -> None:
        now_epoch[0] += 1.0

    args = argparse.Namespace(
        config=tmp_path / "config.json",
        quotes=tmp_path / "quotes.json",
        database=tmp_path / "census.sqlite",
        heartbeat=tmp_path / "heartbeat.json",
        incident_log=tmp_path / "incidents.jsonl",
    )
    result, error, completed, attempt_count = subject.capture_target_with_retries(
        args,
        target_epoch=1_788_282_000.0,
        last_result={},
        capture_operation=operation,
        clock=clock,
        sleeper=sleeper,
    )

    assert completed is True
    assert error == ""
    assert attempt_count == 3
    assert result["frame_inserted"] is True
    incidents = [
        json.loads(line)
        for line in args.incident_log.read_text(encoding="utf-8").splitlines()
    ]
    assert [row["incident_type"] for row in incidents] == [
        "capture_attempt_failed",
        "capture_attempt_failed",
    ]
    assert all(row["terminal_for_target_minute"] is False for row in incidents)
    heartbeat = json.loads(args.heartbeat.read_text(encoding="utf-8"))
    assert heartbeat["attempt_count"] == 3
    assert heartbeat["status"] == "ok"
    assert heartbeat["can_trade"] is False


def test_source_contract_mismatch_fails_closed_without_retry_storm(
    tmp_path: Path,
) -> None:
    attempts = 0

    def operation(**_: object) -> dict[str, object]:
        nonlocal attempts
        attempts += 1
        raise RuntimeError("source_contract_mismatch:schema='2'!='3'")

    args = argparse.Namespace(
        config=tmp_path / "config.json",
        quotes=tmp_path / "quotes.json",
        database=tmp_path / "census.sqlite",
        heartbeat=tmp_path / "heartbeat.json",
        incident_log=tmp_path / "incidents.jsonl",
    )
    _, error, completed, attempt_count = subject.capture_target_with_retries(
        args,
        target_epoch=1_788_282_000.0,
        last_result={},
        capture_operation=operation,
        clock=lambda: 1_788_282_001.0,
        sleeper=lambda _: None,
    )

    assert completed is False
    assert attempt_count == 1
    assert attempts == 1
    assert "source_contract_mismatch" in error
    incidents = [
        json.loads(line)
        for line in args.incident_log.read_text(encoding="utf-8").splitlines()
    ]
    assert [row["incident_type"] for row in incidents] == [
        "capture_attempt_failed",
        "capture_target_terminal_failure",
    ]
    assert incidents[-1]["terminal_for_target_minute"] is True
