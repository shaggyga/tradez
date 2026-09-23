from __future__ import annotations

import copy
import csv
import json
import sqlite3
from pathlib import Path

import numpy as np
import pytest

import oanda_counterfactual_sim_gym as runner
import oanda_counterfactual_sim_gym_verifier as verifier
from src.forex_system.research.counterfactual_sim_gym_v1 import (
    CONTRACT_ID,
    PairMarket,
    build_arm_definitions,
    collapse_effective_n,
    evaluate_signal,
    simulate_order,
)


def market(
    *,
    epochs=(60, 120, 180, 240, 300, 360),
    bid_open=None,
    bid_high=None,
    bid_low=None,
    bid_close=None,
    ask_open=None,
    ask_high=None,
    ask_low=None,
    ask_close=None,
) -> PairMarket:
    count = len(epochs)
    bid_open = bid_open or [1.0000 + index * 0.0001 for index in range(count)]
    bid_high = bid_high or [value + 0.0002 for value in bid_open]
    bid_low = bid_low or [value - 0.0001 for value in bid_open]
    bid_close = bid_close or [value + 0.0001 for value in bid_open]
    ask_open = ask_open or [value + 0.0002 for value in bid_open]
    ask_high = ask_high or [value + 0.0002 for value in bid_high]
    ask_low = ask_low or [value + 0.0002 for value in bid_low]
    ask_close = ask_close or [value + 0.0002 for value in bid_close]
    bid_close_array = np.asarray(bid_close, dtype=float)
    ask_close_array = np.asarray(ask_close, dtype=float)
    return PairMarket(
        instrument="EUR_USD",
        pip=0.0001,
        epochs=np.asarray(epochs, dtype=np.int64),
        bid_open=np.asarray(bid_open, dtype=float),
        bid_high=np.asarray(bid_high, dtype=float),
        bid_low=np.asarray(bid_low, dtype=float),
        bid_close=bid_close_array,
        ask_open=np.asarray(ask_open, dtype=float),
        ask_high=np.asarray(ask_high, dtype=float),
        ask_low=np.asarray(ask_low, dtype=float),
        ask_close=ask_close_array,
        mid_close=(bid_close_array + ask_close_array) / 2.0,
        source_sha256="0" * 64,
        source_prefix_bytes=0,
    )


def test_long_and_short_use_executable_sides_without_double_counting_spread():
    sample = market(
        epochs=(60,),
        bid_open=[1.0000],
        bid_high=[1.0006],
        bid_low=[0.9999],
        bid_close=[1.0005],
        ask_open=[1.0002],
        ask_high=[1.0008],
        ask_low=[1.0001],
        ask_close=[1.0007],
    )
    long = simulate_order(
        sample,
        decision_epoch=60,
        side=1,
        entry_delay_min=0,
        horizon_min=1,
        exit_policy={"id": "endpoint", "kind": "endpoint"},
        round_trip_slippage_pips=0.25,
        moderate_stress_slippage_pips=0.5,
        severe_stress_slippage_pips=1.0,
    )
    short = simulate_order(
        sample,
        decision_epoch=60,
        side=-1,
        entry_delay_min=0,
        horizon_min=1,
        exit_policy={"id": "endpoint", "kind": "endpoint"},
        round_trip_slippage_pips=0.25,
        moderate_stress_slippage_pips=0.5,
        severe_stress_slippage_pips=1.0,
    )
    assert long.status == "filled"
    assert long.executable_net_pips == pytest.approx(2.75)
    assert short.executable_net_pips == pytest.approx(-7.25)
    assert long.entry_spread_pips == pytest.approx(2.0)


def test_same_bar_stop_and_target_uses_adverse_first():
    sample = market(
        epochs=(60,),
        bid_open=[1.0000],
        bid_high=[1.0006],
        bid_low=[0.9998],
        bid_close=[1.0004],
        ask_open=[1.0002],
        ask_high=[1.0008],
        ask_low=[1.0000],
        ask_close=[1.0006],
    )
    result = simulate_order(
        sample,
        decision_epoch=60,
        side=1,
        entry_delay_min=0,
        horizon_min=1,
        exit_policy={"id": "fixed", "kind": "fixed", "stop_pips": 3.0, "target_pips": 3.0},
        round_trip_slippage_pips=0.0,
        moderate_stress_slippage_pips=0.5,
        severe_stress_slippage_pips=1.0,
    )
    assert result.exit_reason == "stop"
    assert result.executable_net_pips == pytest.approx(-3.0)
    assert result.exit_bid == pytest.approx(0.9999)
    assert result.exit_ask == pytest.approx(1.0001)
    assert result.exit_price_kind == "modeled_adverse_first_barrier_fill"


def test_stop_inside_wide_entry_market_is_rejected_instead_of_fabricating_fill():
    sample = market(
        epochs=(60,),
        bid_open=[1.0000],
        bid_high=[1.0001],
        bid_low=[0.9999],
        bid_close=[1.0000],
        ask_open=[1.0008],
        ask_high=[1.0009],
        ask_low=[1.0007],
        ask_close=[1.0008],
    )
    result = simulate_order(
        sample,
        decision_epoch=60,
        side=1,
        entry_delay_min=0,
        horizon_min=1,
        exit_policy={"id": "fixed", "kind": "fixed", "stop_pips": 3.0, "target_pips": 6.0},
        round_trip_slippage_pips=0.25,
        moderate_stress_slippage_pips=0.5,
        severe_stress_slippage_pips=1.0,
    )
    assert result.status == "invalid"
    assert result.exclusion_reason == "stop_inside_entry_market"


def test_missing_exact_entry_is_not_backfilled():
    result = simulate_order(
        market(epochs=(60, 180)),
        decision_epoch=120,
        side=1,
        entry_delay_min=0,
        horizon_min=1,
        exit_policy={"id": "endpoint", "kind": "endpoint"},
        round_trip_slippage_pips=0.25,
        moderate_stress_slippage_pips=0.5,
        severe_stress_slippage_pips=1.0,
    )
    assert result.status == "not_filled"
    assert result.exclusion_reason == "missing_exact_entry_quote"


def test_signal_uses_completed_past_and_rejects_gap():
    sample = market(epochs=tuple(range(60, 1260, 60)))
    rule = {"id": "m5", "kind": "momentum", "lookback_min": 5}
    signal = evaluate_signal(sample, 10, rule, round_trip_slippage_pips=0.25)
    assert signal is not None
    assert signal.knowledge_cutoff_epoch == int(sample.epochs[10]) + 60
    gapped = market(epochs=(60, 120, 180, 240, 300, 420, 480, 540, 600, 660, 720))
    assert evaluate_signal(gapped, 6, rule, round_trip_slippage_pips=0.25) is None


def test_flat_moving_average_rounding_dust_is_not_a_directional_signal():
    count = 20
    sample = market(
        epochs=tuple(range(60, (count + 1) * 60, 60)),
        bid_open=[1.09204] * count,
        bid_high=[1.09205] * count,
        bid_low=[1.09203] * count,
        bid_close=[1.09204] * count,
        ask_open=[1.09232] * count,
        ask_high=[1.09233] * count,
        ask_low=[1.09231] * count,
        ask_close=[1.09232] * count,
    )
    assert (
        evaluate_signal(
            sample,
            15,
            {"id": "sma_5_15", "kind": "moving_average_spread", "fast_min": 5, "slow_min": 15},
            round_trip_slippage_pips=0.25,
        )
        is None
    )


def test_jpy_propagation_and_duplicate_variants_do_not_inflate_effective_n():
    rows = [
        {"market_episode_id": "e1", "signed_currency_factors": ["JPY:+", "EUR:-"]},
        {"market_episode_id": "e1", "signed_currency_factors": ["JPY:+", "USD:-"]},
        {"market_episode_id": "e1", "signed_currency_factors": ["JPY:+", "USD:-"]},
        {"market_episode_id": "e1", "signed_currency_factors": ["AUD:+", "NZD:-"]},
        {"market_episode_id": "e2", "signed_currency_factors": ["JPY:+", "GBP:-"]},
    ]
    assert collapse_effective_n(rows) == 3


def test_arm_identity_is_deterministic_and_material_policy_change_rolls_identity():
    config = json.loads((runner.DEFAULT_CONFIG).read_text(encoding="utf-8"))
    first = build_arm_definitions(config)
    assert [arm.arm_id for arm in first] == [arm.arm_id for arm in build_arm_definitions(config)]
    matched = {}
    for arm in first:
        key = (
            arm.signal_rule_id,
            arm.entry_delay_min,
            arm.horizon_min,
            arm.reentry_gap_min,
            json.dumps(dict(arm.exit_policy), sort_keys=True),
        )
        matched.setdefault(key, set()).add(arm.comparator)
    assert matched
    assert all(
        comparators
        == {"as_signaled", "flipped_direction", "deterministic_random_side"}
        for comparators in matched.values()
    )
    changed = copy.deepcopy(config)
    changed["execution_grid"]["exit_policies"][0]["kind"] = "time_stop"
    changed["execution_grid"]["exit_policies"][0]["horizon_fraction"] = 0.5
    assert {arm.arm_id for arm in first} != {arm.arm_id for arm in build_arm_definitions(changed)}


def test_cohort_reactivation_is_rejected_and_tables_are_append_only(tmp_path: Path):
    database = tmp_path / "gym.sqlite"
    connection = runner.connect(database)
    common = dict(
        connection=connection,
        experiment_key="gym",
        config_sha="1" * 64,
        runner_sha="2" * 64,
        core_sha="3" * 64,
        observed_utc="2026-08-29T00:00:00+00:00",
    )
    runner.register_cohort(cohort_id="A", contract={"version": "A"}, **common)
    runner.register_cohort(cohort_id="B", contract={"version": "B"}, **common)
    with pytest.raises(ValueError, match="reactivation"):
        runner.register_cohort(cohort_id="A", contract={"version": "A"}, **common)
    with pytest.raises(sqlite3.DatabaseError, match="append_only"):
        connection.execute("UPDATE sim_cohorts SET experiment_key='changed' WHERE cohort_id='A'")
    connection.close()


def _write_fixture(path: Path, minutes: int = 220, instrument: str = "EUR_USD") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "time", "datetime", "instrument", "granularity", "open", "high", "low", "close",
        "bid_open", "bid_high", "bid_low", "bid_close", "ask_open", "ask_high", "ask_low",
        "ask_close", "spread_pips", "volume",
    ]
    start = 1_788_000_000 - (1_788_000_000 % 60)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        price = 1.1000
        for index in range(minutes):
            epoch = start + index * 60
            timestamp = runner.iso(epoch)
            bid_open = price
            bid_close = price + (0.0001 if (index // 20) % 2 == 0 else -0.00008)
            ask_open = bid_open + 0.0002
            ask_close = bid_close + 0.0002
            writer.writerow(
                {
                    "time": timestamp,
                    "datetime": timestamp,
                    "instrument": instrument,
                    "granularity": "M1",
                    "open": (bid_open + ask_open) / 2,
                    "high": max(bid_open, bid_close) + 0.0002,
                    "low": min(bid_open, bid_close) - 0.0002,
                    "close": (bid_close + ask_close) / 2,
                    "bid_open": bid_open,
                    "bid_high": max(bid_open, bid_close) + 0.0001,
                    "bid_low": min(bid_open, bid_close) - 0.0001,
                    "bid_close": bid_close,
                    "ask_open": ask_open,
                    "ask_high": max(ask_open, ask_close) + 0.0001,
                    "ask_low": min(ask_open, ask_close) - 0.0001,
                    "ask_close": ask_close,
                    "spread_pips": 2.0,
                    "volume": 10,
                }
            )
            price = bid_close


def test_tiny_historical_run_is_deterministic_and_structurally_verified(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    candle_root = tmp_path / "candles"
    _write_fixture(candle_root / "EUR_USD_M1.csv")
    config = json.loads(runner.DEFAULT_CONFIG.read_text(encoding="utf-8"))
    config["source"]["relative_root"] = str(candle_root)
    config["sampling"].update({"history_days": 1, "cadence_min": 5})
    config["signal_rules"] = [
        {"id": "momentum_5m", "kind": "momentum", "lookback_min": 5, "minimum_move_cost_ratio": 0.0}
    ]
    config["execution_grid"] = {
        "entry_delay_min": [0, 1],
        "horizon_min": [5],
        "reentry_gap_min": [0],
        "exit_policies": [{"id": "endpoint", "kind": "endpoint"}],
    }
    config["historical_partitions"]["embargo_min"] = 11
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    artifact_root = tmp_path / "artifacts"
    monkeypatch.setattr(runner, "ARTIFACT_ROOT", artifact_root)
    monkeypatch.setattr(verifier, "ARTIFACT_ROOT", artifact_root)
    monkeypatch.setattr(
        verifier,
        "DEFAULT_OUTPUT",
        artifact_root / "counterfactual_sim_gym_verifier_v1.json",
    )
    database = artifact_root / config["storage"]["database_relative_path"]
    first = runner.run(
        config_path=config_path,
        requested_pairs=["EUR_USD"],
    )
    second = runner.run(
        config_path=config_path,
        requested_pairs=["EUR_USD"],
    )
    assert first["cohort_id"] == second["cohort_id"]
    assert first["statistics_sha256"] == second["statistics_sha256"]
    assert first["filled_outcome_count"] > 0
    assert first["virtual_order_intent_count"] > first["decision_count"]
    assert first["unique_outcome_part_count"] > 0
    assert first["normalized_ledger_roots"]["intent_map"]["count"] == first["virtual_order_intent_count"]
    audit = verifier.verify(database, cohort_id=first["cohort_id"])
    assert audit["verified"] is True, audit["failures"]
    assert audit["supported_decision"] == "no_trade"


def test_runner_contract_rejects_barrier_exit_until_independent_replay_exists():
    config = json.loads(runner.DEFAULT_CONFIG.read_text(encoding="utf-8"))
    config["execution_grid"]["exit_policies"] = [
        {
            "id": "fixed",
            "kind": "fixed",
            "stop_pips": 3.0,
            "target_pips": 6.0,
        }
    ]
    with pytest.raises(ValueError, match="independently verifies endpoint exits only"):
        runner.validate_effective_config(config)


def test_two_pair_all_pair_effective_aggregation_handles_shared_clocks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    candle_root = tmp_path / "candles"
    _write_fixture(candle_root / "EUR_USD_M1.csv", instrument="EUR_USD")
    _write_fixture(candle_root / "USD_JPY_M1.csv", instrument="USD_JPY")
    config = json.loads(runner.DEFAULT_CONFIG.read_text(encoding="utf-8"))
    config["source"]["relative_root"] = str(candle_root)
    config["source"]["initial_instruments"] = ["EUR_USD", "USD_JPY"]
    config["sampling"].update({"history_days": 1, "cadence_min": 5})
    config["signal_rules"] = [
        {"id": "momentum_5m", "kind": "momentum", "lookback_min": 5, "minimum_move_cost_ratio": 0.0}
    ]
    config["execution_grid"] = {
        "entry_delay_min": [0],
        "horizon_min": [5],
        "reentry_gap_min": [0],
        "exit_policies": [{"id": "endpoint", "kind": "endpoint"}],
    }
    config["historical_partitions"]["embargo_min"] = 10
    config_path = tmp_path / "config_two.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    artifact_root = tmp_path / "artifacts_two"
    monkeypatch.setattr(runner, "ARTIFACT_ROOT", artifact_root)
    result = runner.run(config_path=config_path)
    assert result["instrument_count"] == 2
    assert result["filled_outcome_count"] > 0
    connection = sqlite3.connect(result["database"])
    rows = connection.execute(
        "SELECT aggregate_json FROM sim_cell_aggregates "
        "WHERE cohort_id=? AND scope='all_pairs' AND instrument='ALL'",
        (result["cohort_id"],),
    ).fetchall()
    connection.close()
    assert rows
    assert all(json.loads(row[0])["effective_n"] <= json.loads(row[0])["raw_n"] for row in rows)


def _build_verified_adversarial_fixture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[Path, Path, str]:
    """Create one tiny, independently verified ledger for destructive copies."""

    candle_root = tmp_path / "candles"
    _write_fixture(candle_root / "EUR_USD_M1.csv")
    config = json.loads(runner.DEFAULT_CONFIG.read_text(encoding="utf-8"))
    config["source"]["relative_root"] = str(candle_root)
    config["sampling"].update({"history_days": 1, "cadence_min": 5})
    config["signal_rules"] = [
        {
            "id": "momentum_5m",
            "kind": "momentum",
            "lookback_min": 5,
            "minimum_move_cost_ratio": 0.0,
        }
    ]
    config["execution_grid"] = {
        "entry_delay_min": [0],
        "horizon_min": [5],
        "reentry_gap_min": [0],
        "exit_policies": [{"id": "endpoint", "kind": "endpoint"}],
    }
    config["historical_partitions"]["embargo_min"] = 10
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    artifact_root = tmp_path / "artifacts"
    monkeypatch.setattr(runner, "ARTIFACT_ROOT", artifact_root)
    monkeypatch.setattr(verifier, "ARTIFACT_ROOT", artifact_root)
    monkeypatch.setattr(
        verifier,
        "DEFAULT_OUTPUT",
        artifact_root / "counterfactual_sim_gym_verifier_v1.json",
    )
    result = runner.run(config_path=config_path, requested_pairs=["EUR_USD"])
    database = Path(result["database"])
    audit = verifier.verify(database, cohort_id=result["cohort_id"])
    assert audit["verified"] is True, audit["failures"]
    return artifact_root, database, str(result["cohort_id"])


def _mutate_while_restoring_append_only_triggers(
    database: Path,
    table: str,
    operation,
) -> None:
    connection = sqlite3.connect(database)
    triggers = list(
        connection.execute(
            "SELECT name,sql FROM sqlite_master "
            "WHERE type='trigger' AND tbl_name=? ORDER BY name",
            (table,),
        )
    )
    assert len(triggers) == 2
    for name, _ in triggers:
        connection.execute(f'DROP TRIGGER "{name}"')
    operation(connection)
    for _, sql in triggers:
        connection.execute(str(sql))
    connection.commit()
    connection.close()


def test_independent_replay_rejects_self_consistently_rehashed_outcome_tamper(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    _, database, cohort_id = _build_verified_adversarial_fixture(tmp_path, monkeypatch)

    def tamper(connection: sqlite3.Connection) -> None:
        connection.row_factory = sqlite3.Row
        row = connection.execute(
            "SELECT * FROM sim_outcome_parts "
            "WHERE cohort_id=? AND status='filled' ORDER BY outcome_id LIMIT 1",
            (cohort_id,),
        ).fetchone()
        assert row is not None
        outcome = json.loads(str(row["outcome_json"]))
        outcome["executable_net_pips"] = float(outcome["executable_net_pips"]) + 1000.0
        outcome_json = verifier.canonical_json(outcome)
        identity = {
            "cohort_id": str(row["cohort_id"]),
            "clock_id": str(row["clock_id"]),
            "side": int(row["side"]),
            "entry_delay_min": int(row["entry_delay_min"]),
            "horizon_min": int(row["horizon_min"]),
            "exit_policy_id": str(row["exit_policy_id"]),
            "status": str(row["status"]),
            "exclusion_reason": str(row["exclusion_reason"]),
            "outcome_sha256": verifier.stable_hash(outcome),
            "outcome_json": outcome_json,
        }
        connection.execute(
            "UPDATE sim_outcome_parts SET outcome_sha256=?,outcome_json=?,row_sha256=? "
            "WHERE outcome_id=?",
            (
                identity["outcome_sha256"],
                outcome_json,
                verifier.stable_hash(identity),
                str(row["outcome_id"]),
            ),
        )

    _mutate_while_restoring_append_only_triggers(database, "sim_outcome_parts", tamper)
    audit = verifier.verify(database, cohort_id=cohort_id)
    assert audit["verified"] is False
    assert audit["checks"]["normalized_rows"]["failures"]["outcome_parts"] >= 1
    assert not any(
        failure.startswith("append_only_trigger_missing")
        for failure in audit["failures"]
    )


def test_independent_replay_rejects_omitted_expected_intent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    _, database, cohort_id = _build_verified_adversarial_fixture(tmp_path, monkeypatch)

    def omit(connection: sqlite3.Connection) -> None:
        intent = connection.execute(
            "SELECT intent_id FROM sim_intent_map "
            "WHERE cohort_id=? ORDER BY intent_id LIMIT 1",
            (cohort_id,),
        ).fetchone()
        assert intent is not None
        connection.execute("DELETE FROM sim_intent_map WHERE intent_id=?", (intent[0],))

    _mutate_while_restoring_append_only_triggers(database, "sim_intent_map", omit)
    audit = verifier.verify(database, cohort_id=cohort_id)
    assert audit["verified"] is False
    assert audit["checks"]["normalized_rows"]["failures"]["intent_map"] >= 1
    expected = audit["checks"]["independent_replay_expected"]["intent_count"]
    observed = audit["checks"]["normalized_rows"]["roots"]["intent_map"]["count"]
    assert expected == observed + 1
    assert not any(
        failure.startswith("append_only_trigger_missing")
        for failure in audit["failures"]
    )


@pytest.mark.parametrize(
    "archive_name",
    ("../escape.gz", "nested/../../escape.gz", "", ".", "C:/escape.gz"),
)
def test_archive_path_traversal_and_absolute_paths_are_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    archive_name: str,
):
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    monkeypatch.setattr(verifier, "ARTIFACT_ROOT", artifact_root)
    with pytest.raises(ValueError, match="relative|traversal"):
        verifier.contained_archive_path(archive_name)


def test_oversized_archive_declarations_and_files_are_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    archive = tmp_path / "source.csv.gz"
    archive.write_bytes(b"0123456789")
    monkeypatch.setattr(verifier, "MAX_FROZEN_SOURCE_BYTES", 8)
    with pytest.raises(ValueError, match="frozen source byte count"):
        verifier.bounded_gzip_payload(archive, 9)
    monkeypatch.setattr(verifier, "MAX_FROZEN_SOURCE_BYTES", 1024)
    monkeypatch.setattr(verifier, "MAX_COMPRESSED_SOURCE_BYTES", 8)
    with pytest.raises(ValueError, match="compressed source byte count"):
        verifier.bounded_gzip_payload(archive, 1)


def test_archive_symlink_is_rejected_when_platform_supports_symlinks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    target = tmp_path / "outside.gz"
    target.write_bytes(b"not an archive")
    link = artifact_root / "linked.gz"
    try:
        link.symlink_to(target)
    except OSError as exc:
        pytest.skip(f"symlink creation is unavailable on this host: {exc}")
    monkeypatch.setattr(verifier, "ARTIFACT_ROOT", artifact_root)
    with pytest.raises(ValueError, match="link/junction"):
        verifier.contained_archive_path("linked.gz")


def test_archive_link_boundary_is_exercised_when_host_cannot_create_symlink(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    linked = artifact_root / "linked.gz"
    linked.write_bytes(b"ordinary file standing in for a platform link")
    monkeypatch.setattr(verifier, "ARTIFACT_ROOT", artifact_root)
    original = verifier._is_link_or_junction
    monkeypatch.setattr(
        verifier,
        "_is_link_or_junction",
        lambda path: path == linked or original(path),
    )
    with pytest.raises(ValueError, match="link/junction"):
        verifier.contained_archive_path("linked.gz")
