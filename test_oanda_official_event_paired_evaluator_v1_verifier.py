from __future__ import annotations

import datetime as dt
import hashlib
import json
from pathlib import Path
import sqlite3

import pytest

import oanda_official_event_paired_evaluator_v1 as producer
import oanda_official_event_paired_evaluator_v1_verifier as verifier
import test_oanda_official_event_paired_evaluator_v1 as fixture


def _create_table(connection: sqlite3.Connection, name: str, row: dict) -> None:
    definitions = []
    for key, value in row.items():
        kind = "REAL" if isinstance(value, float) else "INTEGER" if isinstance(value, int) else "TEXT"
        definitions.append(f'"{key}" {kind}')
    connection.execute(f'CREATE TABLE "{name}" ({",".join(definitions)})')
    columns = ",".join(f'"{key}"' for key in row)
    marks = ",".join("?" for _ in row)
    connection.execute(
        f'INSERT INTO "{name}" ({columns}) VALUES ({marks})', tuple(row.values())
    )
    connection.commit()


def _seed(
    tmp_path: Path,
    *,
    issued_sec: int = 20,
    include_decision: bool = True,
    include_horizon: bool = True,
    capture_latency_seconds: float | None = None,
    source_cohort_id: str | None = None,
    mapping_sec: int = 10,
) -> dict[str, Path | dt.datetime]:
    release_path = tmp_path / "release.sqlite"
    mapping_path = tmp_path / "mapping.sqlite"
    horizon_path = tmp_path / "horizon.sqlite"
    output_path = tmp_path / "paired.sqlite"
    capture = fixture.capture_row()
    source = fixture.source_row()
    mapping = fixture.mapping_row(mapped_sec=mapping_sec)
    if capture_latency_seconds is not None:
        capture_payload = json.loads(capture["capture_payload_json"])
        capture_payload["capture_latency_seconds"] = capture_latency_seconds
        capture["capture_payload_json"] = fixture.canonical(capture_payload)
    if source_cohort_id is not None:
        source["source_cohort_id"] = source_cohort_id
    feature_bytes, feature = fixture.feature_snapshot(rising=False)

    release = sqlite3.connect(release_path)
    _create_table(release, "official_release_quote_capture", capture)
    _create_table(release, "official_release_observation", source)
    release.close()
    mapping_connection = sqlite3.connect(mapping_path)
    _create_table(mapping_connection, "official_release_mapping", mapping)
    mapping_connection.close()

    decision, arms = producer.build_decision(
        capture_row=capture,
        source_row=source,
        mapping_row=mapping,
        feature_snapshot_bytes=feature_bytes,
        feature_snapshot=feature,
        issued_utc=fixture.EVENT + dt.timedelta(seconds=issued_sec),
        config_bytes=producer.CONFIG_PATH.read_bytes(),
        producer_sha256=producer.normalized_producer_source_sha256(),
    )
    output = producer.open_database(output_path)
    try:
        if include_decision:
            assert producer.insert_decision(output, decision, arms)
            if include_horizon:
                capture_horizon, quote_horizon = fixture.horizon_fixture(decision)
                horizon_input = producer._horizon_input(
                    decision, capture_horizon, quote_horizon
                )
                outcomes = producer._outcome_rows(decision, arms, horizon_input)
                assert producer.insert_horizon_and_outcomes(
                    output, horizon_input, outcomes
                )
    finally:
        output.close()

    horizon = sqlite3.connect(horizon_path)
    capture_horizon, quote_horizon = fixture.horizon_fixture(decision)
    _create_table(horizon, "official_event_horizon_capture", capture_horizon)
    _create_table(horizon, "official_event_horizon_quote", quote_horizon)
    if not include_horizon:
        horizon.execute("DELETE FROM official_event_horizon_capture")
        horizon.execute("DELETE FROM official_event_horizon_quote")
        horizon.commit()
    horizon.close()
    return {
        "release": release_path,
        "mapping": mapping_path,
        "horizon": horizon_path,
        "output": output_path,
        "event": fixture.EVENT,
    }


def _verify(
    paths: dict[str, Path | dt.datetime],
    *,
    seconds: int = 70,
    config: Path | None = None,
    authority_map: Path | None = None,
    fast_lane_source: Path | None = None,
) -> dict:
    return verifier.verify_paired_evaluator_database(
        output_database=Path(paths["output"]),
        release_database=Path(paths["release"]),
        mapping_database=Path(paths["mapping"]),
        horizon_database=Path(paths["horizon"]),
        config_path=config or verifier.CONFIG_PATH,
        producer_source_path=Path(producer.__file__),
        authority_map_path=authority_map or verifier.AUTHORITY_MAP_PATH,
        fast_lane_source_path=fast_lane_source or verifier.FAST_LANE_SOURCE_PATH,
        observed_utc=fixture.EVENT + dt.timedelta(seconds=seconds),
    )


def _drop_update(path: Path, table: str) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.execute(f"DROP TRIGGER {table}_no_update")
    return connection


def test_clean_paired_ledger_verifies_without_importing_producer(tmp_path: Path) -> None:
    paths = _seed(tmp_path)
    result = _verify(paths)
    assert result["verified"] is True, result["failures"]
    assert result["counts"]["decisions"] == 1
    assert result["counts"]["arms"] == 5
    assert result["counts"]["horizon_inputs"] == 1
    assert result["counts"]["outcomes"] == 15
    assert result["entry_economics_role"] == verifier.ENTRY_ECONOMICS_ROLE
    assert result["practice_latency_proof_state"] == "not_measured_by_this_counterfactual_cohort"


def test_late_terminal_invalid_decision_is_verified_as_honest_evidence(tmp_path: Path) -> None:
    paths = _seed(tmp_path, issued_sec=56)
    result = _verify(paths)
    assert result["verified"] is True, result["failures"]
    assert result["counts"]["invalid_decisions"] == 1


def test_missing_due_decision_fails_schedule_completeness(tmp_path: Path) -> None:
    paths = _seed(tmp_path, include_decision=False, include_horizon=False)
    result = _verify(paths, seconds=56)
    assert result["verified"] is False
    assert result["counts"]["due_missing_decisions"] == 1
    assert any("due_decision_missing" in item for item in result["failures"])


def test_technical_direction_substitution_is_detected(tmp_path: Path) -> None:
    paths = _seed(tmp_path)
    connection = _drop_update(Path(paths["output"]), "paired_event_decision")
    connection.execute(
        "UPDATE paired_event_decision SET technical_pair_side='buy'"
    )
    connection.commit()
    connection.close()
    result = _verify(paths)
    assert result["verified"] is False
    assert any("technical_side" in item for item in result["failures"])
    assert any("trigger:paired_event_decision_no_update" in item for item in result["failures"])


def test_mentioned_currency_cannot_replace_issuer(tmp_path: Path) -> None:
    paths = _seed(tmp_path)
    connection = _drop_update(Path(paths["output"]), "paired_event_decision")
    connection.execute("UPDATE paired_event_decision SET issuer_currency='JPY'")
    connection.commit()
    connection.close()
    result = _verify(paths)
    assert result["verified"] is False
    assert any("issuer_leakage" in item for item in result["failures"])


def test_outcome_aware_pair_substitution_is_detected(tmp_path: Path) -> None:
    paths = _seed(tmp_path)
    connection = _drop_update(Path(paths["output"]), "paired_event_decision")
    connection.execute("UPDATE paired_event_decision SET selected_instrument='USD_JPY'")
    connection.commit()
    connection.close()
    result = _verify(paths)
    assert result["verified"] is False
    assert any("outcome_blind_pair_selection" in item for item in result["failures"])


def test_missing_flipped_control_fails_complete_arm_grid(tmp_path: Path) -> None:
    paths = _seed(tmp_path)
    connection = sqlite3.connect(Path(paths["output"]))
    connection.execute("PRAGMA foreign_keys=OFF")
    connection.execute("DROP TRIGGER paired_event_arm_no_delete")
    connection.execute("DELETE FROM paired_event_arm WHERE arm_name='official_flipped_control'")
    connection.commit()
    connection.close()
    result = _verify(paths)
    assert result["verified"] is False
    assert any("arm_count" in item or "arm_grid" in item for item in result["failures"])


def test_available_invalid_or_valid_horizon_cannot_be_omitted(tmp_path: Path) -> None:
    paths = _seed(tmp_path)
    connection = sqlite3.connect(Path(paths["output"]))
    connection.execute("PRAGMA foreign_keys=OFF")
    connection.execute("DROP TRIGGER paired_event_outcome_no_delete")
    connection.execute("DROP TRIGGER paired_event_horizon_input_no_delete")
    connection.execute("DELETE FROM paired_event_outcome")
    connection.execute("DELETE FROM paired_event_horizon_input")
    connection.commit()
    connection.close()
    result = _verify(paths)
    assert result["verified"] is False
    assert result["counts"]["available_missing_paired_horizons"] == 1
    assert any("available_horizon_omitted" in item for item in result["failures"])


@pytest.mark.parametrize("mode", ["spread_twice", "slippage_twice"])
def test_cost_double_counting_is_detected(tmp_path: Path, mode: str) -> None:
    paths = _seed(tmp_path)
    connection = _drop_update(Path(paths["output"]), "paired_event_outcome")
    if mode == "spread_twice":
        connection.execute(
            """
            UPDATE paired_event_outcome
            SET net_after_cost_pips=net_after_cost_pips-spread_cost_pips
            WHERE outcome_state='trade' AND slippage_stress_pips=0.0
            """
        )
    else:
        connection.execute(
            """
            UPDATE paired_event_outcome
            SET net_after_cost_pips=net_after_cost_pips-slippage_stress_pips
            WHERE outcome_state='trade' AND slippage_stress_pips=0.5
            """
        )
    connection.commit()
    connection.close()
    result = _verify(paths)
    assert result["verified"] is False
    assert any("slippage_once" in item for item in result["failures"])


def test_pair_dependent_factor_key_cannot_inflate_effective_n(tmp_path: Path) -> None:
    paths = _seed(tmp_path)
    connection = _drop_update(Path(paths["output"]), "paired_event_arm")
    connection.execute(
        "UPDATE paired_event_arm SET signed_currency_factor_id='pair-dependent-forgery' "
        "WHERE action_state='trade'"
    )
    connection.commit()
    connection.close()
    result = _verify(paths)
    assert result["verified"] is False
    assert any("issuer_factor_dedupe" in item for item in result["failures"])


def test_upstream_mapping_drift_after_seal_is_detected(tmp_path: Path) -> None:
    paths = _seed(tmp_path)
    connection = sqlite3.connect(Path(paths["mapping"]))
    row = connection.execute(
        "SELECT mapping_payload_json FROM official_release_mapping"
    ).fetchone()
    payload = json.loads(row[0])
    payload["currency_scores"]["USD"] = -0.9
    connection.execute(
        "UPDATE official_release_mapping SET mapping_payload_json=?",
        (fixture.canonical(payload),),
    )
    connection.commit()
    connection.close()
    result = _verify(paths)
    assert result["verified"] is False
    assert any("mapping_payload_drift" in item for item in result["failures"])


def test_forged_decision_state_and_reason_are_reconstructed(tmp_path: Path) -> None:
    paths = _seed(tmp_path)
    connection = _drop_update(Path(paths["output"]), "paired_event_decision")
    connection.execute(
        "UPDATE paired_event_decision SET decision_state='invalid',invalid_reason='self-consistent-looking'"
    )
    connection.commit()
    connection.close()
    result = _verify(paths)
    assert result["verified"] is False
    assert any("decision_state_reconstruction" in item for item in result["failures"])
    assert any("invalid_reason_reconstruction" in item for item in result["failures"])


def test_frozen_config_bytes_cannot_be_reformatted(tmp_path: Path) -> None:
    paths = _seed(tmp_path)
    payload = json.loads(verifier.CONFIG_PATH.read_text(encoding="utf-8"))
    config = tmp_path / "reformatted.json"
    config.write_text(json.dumps(payload), encoding="utf-8")
    result = _verify(paths, config=config)
    assert result["verified"] is False
    assert "config:file_sha256:mismatch" in result["failures"]


@pytest.mark.parametrize(
    ("kwargs", "seconds"),
    [
        ({"capture_latency_seconds": 1.0}, 70),
        ({"source_cohort_id": "unregistered-source-cohort"}, 70),
        ({"issued_sec": 61, "mapping_sec": 61}, 90),
    ],
)
def test_honest_terminal_invalid_inputs_remain_verifiable(
    tmp_path: Path, kwargs: dict, seconds: int
) -> None:
    paths = _seed(tmp_path, **kwargs)
    result = _verify(paths, seconds=seconds)
    assert result["verified"] is True, result["failures"]
    assert result["counts"]["invalid_decisions"] == 1


def test_entry_capture_clock_rewrite_is_detected(tmp_path: Path) -> None:
    paths = _seed(tmp_path)
    release = sqlite3.connect(Path(paths["release"]))
    release.row_factory = sqlite3.Row
    row = release.execute("SELECT * FROM official_release_quote_capture").fetchone()
    payload = json.loads(row["capture_payload_json"])
    rewritten_clock = fixture.EVENT + dt.timedelta(seconds=20)
    payload["captured_utc"] = producer.iso_utc(rewritten_clock)
    payload["capture_latency_seconds"] = 20.0
    release.execute(
        "UPDATE official_release_quote_capture SET captured_utc=?,capture_payload_json=?",
        (producer.iso_utc(rewritten_clock), fixture.canonical(payload)),
    )
    release.commit()
    release.close()
    result = _verify(paths)
    assert result["verified"] is False
    assert any(
        "input_capture_payload_drift" in item
        or "input_capture_row_drift" in item
        for item in result["failures"]
    )


def test_horizon_attempt_clock_rewrite_is_detected(tmp_path: Path) -> None:
    paths = _seed(tmp_path)
    horizon = sqlite3.connect(Path(paths["horizon"]))
    horizon.execute(
        "UPDATE official_event_horizon_capture SET attempted_utc=?,attempt_delay_sec=?",
        (
            producer.iso_utc(fixture.EVENT + dt.timedelta(minutes=3)),
            120.0,
        ),
    )
    horizon.commit()
    horizon.close()
    result = _verify(paths)
    assert result["verified"] is False
    assert any(
        "horizon_row_bytes" in item or "horizon_capture_attempt_clock_invalid" in item
        for item in result["failures"]
    )


def test_manifest_identity_and_append_only_trigger_are_verified(tmp_path: Path) -> None:
    paths = _seed(tmp_path)
    connection = sqlite3.connect(Path(paths["output"]))
    connection.execute("DROP TRIGGER paired_event_cohort_manifest_no_update")
    connection.execute(
        "UPDATE paired_event_cohort_manifest SET required_classification_version='forged'"
    )
    connection.commit()
    connection.close()
    result = _verify(paths)
    assert result["verified"] is False
    assert "manifest:required_classification_version:mismatch" in result["failures"]
    assert any(
        "trigger:paired_event_cohort_manifest_no_update" in item
        for item in result["failures"]
    )


def test_authority_map_byte_drift_is_detected(tmp_path: Path) -> None:
    paths = _seed(tmp_path)
    payload = json.loads(verifier.AUTHORITY_MAP_PATH.read_text(encoding="utf-8"))
    payload["tamper_marker"] = True
    authority = tmp_path / "authority.json"
    authority.write_text(json.dumps(payload), encoding="utf-8")
    result = _verify(paths, authority_map=authority)
    assert result["verified"] is False
    assert "authority_map:file_sha256:mismatch" in result["failures"]
    assert any("authority_map_bytes_drift" in item for item in result["failures"])


def test_earlier_non_v151_mapping_is_not_selected(tmp_path: Path) -> None:
    paths = _seed(tmp_path)
    wrong = fixture.mapping_row(
        mapped_sec=1,
        classification_version="local_fx_news_rules_obsolete_v150",
    )
    connection = sqlite3.connect(Path(paths["mapping"]))
    columns = tuple(wrong)
    connection.execute(
        f"INSERT INTO official_release_mapping ({','.join(columns)}) "
        f"VALUES ({','.join('?' for _ in columns)})",
        tuple(wrong[column] for column in columns),
    )
    connection.commit()
    connection.close()
    result = _verify(paths)
    assert result["verified"] is True, result["failures"]


def test_historical_target_quote_backfilled_at_later_attempt_is_detected(
    tmp_path: Path,
) -> None:
    paths = _seed(tmp_path)
    horizon = sqlite3.connect(Path(paths["horizon"]))
    horizon.row_factory = sqlite3.Row
    capture = horizon.execute(
        "SELECT * FROM official_event_horizon_capture"
    ).fetchone()
    payload = json.loads(capture["payload_json"])
    later = fixture.EVENT + dt.timedelta(minutes=1, seconds=10)
    payload["capture_read_started_utc"] = producer.iso_utc(later)
    payload["attempted_utc"] = producer.iso_utc(later)
    payload["attempt_delay_sec"] = 10.0
    # Quote clocks remain at the historical target while their recorded age
    # remains zero, simulating a later backfill masquerading as live capture.
    payload_json = fixture.canonical(payload)
    horizon.execute(
        """
        UPDATE official_event_horizon_capture
        SET capture_read_started_utc=?,attempted_utc=?,attempt_delay_sec=?,
            payload_json=?,payload_sha256=?
        """,
        (
            producer.iso_utc(later),
            producer.iso_utc(later),
            10.0,
            payload_json,
            hashlib.sha256(payload_json.encode()).hexdigest(),
        ),
    )
    horizon.commit()
    capture = horizon.execute(
        "SELECT * FROM official_event_horizon_capture"
    ).fetchone()
    quote = horizon.execute(
        "SELECT * FROM official_event_horizon_quote WHERE instrument='EUR_USD'"
    ).fetchone()
    output = sqlite3.connect(Path(paths["output"]))
    output.row_factory = sqlite3.Row
    decision = output.execute("SELECT * FROM paired_event_decision").fetchone()
    state, reason, *_ = verifier._expected_horizon_input(
        decision, capture, quote
    )
    output.close()
    horizon.close()
    assert state == "invalid"
    assert "horizon_component_validation_failed" in reason
    result = _verify(paths)
    assert result["verified"] is False


def test_dependency_source_identity_drift_is_detected(tmp_path: Path) -> None:
    paths = _seed(tmp_path)
    altered = tmp_path / "fast_lane_dependency.py"
    altered.write_bytes(verifier.FAST_LANE_SOURCE_PATH.read_bytes() + b"\n")
    result = _verify(paths, fast_lane_source=altered)
    assert result["verified"] is False
    assert "dependency:fast_lane:file_sha256:mismatch" in result["failures"]
    assert any("fast_lane_source_sha_mismatch" in item for item in result["failures"])


def test_atomic_json_publish_recovers_from_transient_windows_access_denied(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "verifier-state.json"
    real_replace = verifier.os.replace
    attempts = 0
    sleeps: list[float] = []

    def transient_replace(source: Path, destination: Path) -> None:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise PermissionError(5, "Access is denied")
        real_replace(source, destination)

    monkeypatch.setattr(verifier.os, "replace", transient_replace)
    monkeypatch.setattr(verifier.time, "sleep", sleeps.append)

    verifier.write_json_atomic(target, {"status": "verified", "attempt": 3})

    assert attempts == 3
    assert sleeps == [0.01, 0.02]
    assert json.loads(target.read_text(encoding="utf-8")) == {
        "attempt": 3,
        "status": "verified",
    }
    assert list(tmp_path.glob(".verifier-state.json.*.tmp")) == []


def test_atomic_json_publish_raises_after_bounded_windows_failure_and_cleans_temp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "verifier-heartbeat.json"
    target.write_text('{"status":"previous"}\n', encoding="utf-8")
    attempts = 0
    sleeps: list[float] = []

    def permanently_denied(source: Path, destination: Path) -> None:
        del source, destination
        nonlocal attempts
        attempts += 1
        raise PermissionError(5, "Access is denied")

    monkeypatch.setattr(verifier.os, "replace", permanently_denied)
    monkeypatch.setattr(verifier.time, "sleep", sleeps.append)

    with pytest.raises(PermissionError, match="Access is denied"):
        verifier.write_json_atomic(target, {"status": "verified"})

    assert attempts == 8
    assert sleeps == [0.01, 0.02, 0.04, 0.08, 0.16, 0.32, 0.5]
    assert json.loads(target.read_text(encoding="utf-8")) == {
        "status": "previous"
    }
    assert list(tmp_path.glob(".verifier-heartbeat.json.*.tmp")) == []
