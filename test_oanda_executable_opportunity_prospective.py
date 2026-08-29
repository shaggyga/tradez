import datetime as dt
import json
import sqlite3

import oanda_executable_opportunity_prospective as proof


UTC = dt.timezone.utc


def test_current_prospective_model_is_frozen_v2_h30_extension() -> None:
    assert proof.CONFIG.name == "executable_opportunity_ranking_v2.json"
    assert proof.ARTIFACT.name == "executable_opportunity_ranking_v2"


def test_candidate_epoch_is_last_aligned_completed_minute() -> None:
    assert proof.candidate_epoch(1_000, 300) == 900
    assert proof.candidate_epoch(1_800, 900) == 1_800


def test_frozen_gate_requires_all_conditions() -> None:
    config = {
        "minimum_clear_probability": 0.55,
        "minimum_direction_confidence": 0.10,
        "minimum_predicted_magnitude_cost_ratio": 1.5,
    }
    row = {
        "predicted_clear_probability": 0.60,
        "predicted_direction_confidence": 0.20,
        "predicted_magnitude_pips": 3.1,
        "entry_cost_pips": 2.0,
        "predicted_ev_pips": 0.1,
    }
    assert proof.gate(row, config)
    assert not proof.gate({**row, "predicted_ev_pips": -0.01}, config)
    assert not proof.gate({**row, "predicted_clear_probability": 0.50}, config)


def test_magnitude_gate_is_distinct_from_directional_trade_gate() -> None:
    config = {
        "minimum_clear_probability": 0.55,
        "minimum_predicted_magnitude_cost_ratio": 1.5,
    }
    row = {
        "predicted_clear_probability": 0.70,
        "predicted_magnitude_pips": 3.1,
        "entry_cost_pips": 2.0,
    }
    assert (
        row["predicted_clear_probability"] >= config["minimum_clear_probability"]
        and row["predicted_magnitude_pips"]
        >= config["minimum_predicted_magnitude_cost_ratio"] * row["entry_cost_pips"]
    )


def test_forecast_ledger_is_immutable(tmp_path) -> None:
    db = proof.open_ledger(tmp_path / "proof.sqlite")
    columns = db.execute("PRAGMA table_info(forecasts)").fetchall()
    values = []
    for _, name, kind, required, default, primary in columns:
        if name in {"research_only"}: values.append(1)
        elif name in {"execution_eligible"}: values.append(0)
        elif "INT" in kind: values.append(1)
        elif "REAL" in kind: values.append(1.0)
        else: values.append(name)
    db.execute(f"INSERT INTO forecasts VALUES ({','.join('?' for _ in values)})", values)
    db.commit()
    try:
        db.execute("UPDATE forecasts SET instrument='USD_JPY'")
    except sqlite3.IntegrityError as error:
        assert "immutable forecasts" in str(error)
    else:
        raise AssertionError("immutable forecast update was accepted")
    db.close()


def test_material_collector_change_creates_new_collection_cohort() -> None:
    manifest = {"cohort_id": "frozen_model_v1"}
    first = proof.prospective_contract(manifest, collector_sha256="a" * 64)
    second = proof.prospective_contract(manifest, collector_sha256="b" * 64)
    assert first["cohort_id"] != second["cohort_id"]
    assert first["model_cohort_id"] == "frozen_model_v1"
    assert first["supersedes_cohort_id"] == "frozen_model_v1"


def test_clock_integrity_alignment_never_applies_cached_executor_offset(tmp_path) -> None:
    integrity = tmp_path / "clock.json"
    integrity.write_text(json.dumps({
        "generated_utc": "2026-08-17T08:30:00+00:00",
        "status": "ok",
        "source_fresh": True,
        "timestamp_normalization_trusted": True,
        "host_clock_synchronized": True,
        "broker_clock_lead_sec": -0.124,
        "broker_clock_sample_count": 128,
        "external_https_clock": {
            "status": "ok",
            "offset_sec": -0.421,
            "round_trip_ms": 84.3,
            "precision_sec": 1.0,
        },
    }), encoding="utf-8")
    local = dt.datetime(2026, 8, 17, 8, 30, 1, tzinfo=UTC)

    observed, diagnostic = proof.collector_observation_time(
        local, integrity_path=integrity
    )

    assert observed == local
    assert diagnostic["source"] == "clock_integrity_synchronized_host"
    assert diagnostic["applied_offset_sec"] == 0.0
    assert diagnostic["cached_executor_offset_permitted"] is False
    assert diagnostic["trusted_for_prospective_evidence"] is True


def test_stale_clock_integrity_fails_closed_even_when_old_state_claims_sync(tmp_path) -> None:
    integrity = tmp_path / "clock.json"
    integrity.write_text(json.dumps({
        "generated_utc": "2026-08-17T08:00:00+00:00",
        "status": "ok",
        "source_fresh": True,
        "timestamp_normalization_trusted": True,
        "host_clock_synchronized": True,
        "broker_clock_lead_sec": -30.0,
        "broker_clock_sample_count": 128,
        "external_https_clock": {
            "status": "ok",
            "offset_sec": -30.0,
            "round_trip_ms": 50.0,
            "precision_sec": 1.0,
        },
    }), encoding="utf-8")
    local = dt.datetime(2026, 8, 17, 8, 30, 1, tzinfo=UTC)

    observed, diagnostic = proof.collector_observation_time(
        local, integrity_path=integrity, maximum_state_age_sec=90.0
    )

    assert observed == local
    assert diagnostic["source"] == "unavailable"
    assert diagnostic["integrity_state_fresh"] is False
    assert diagnostic["trusted_for_prospective_evidence"] is False


def test_fresh_external_clock_can_correct_an_unsynchronized_host(tmp_path) -> None:
    integrity = tmp_path / "clock.json"
    integrity.write_text(json.dumps({
        "generated_utc": "2026-08-17T08:30:00+00:00",
        "status": "mitigated",
        "source_fresh": True,
        "timestamp_normalization_trusted": True,
        "host_clock_synchronized": False,
        "broker_clock_lead_sec": 14.5,
        "broker_clock_sample_count": 128,
        "external_https_clock": {
            "status": "ok",
            "offset_sec": 15.0,
            "round_trip_ms": 90.0,
            "precision_sec": 1.0,
        },
    }), encoding="utf-8")
    local = dt.datetime(2026, 8, 17, 8, 30, 1, tzinfo=UTC)

    observed, diagnostic = proof.collector_observation_time(
        local, integrity_path=integrity
    )

    assert observed == local + dt.timedelta(seconds=15)
    assert diagnostic["source"] == "clock_integrity_oanda_https_date"
    assert diagnostic["applied_offset_sec"] == 15.0
    assert diagnostic["trusted_for_prospective_evidence"] is True


def test_summary_never_mixes_collector_cohorts_for_proof(tmp_path) -> None:
    db = proof.open_ledger(tmp_path / "proof.sqlite")
    columns = db.execute("PRAGMA table_info(forecasts)").fetchall()
    for index, cohort_id in enumerate(("collector_a", "collector_b"), start=1):
        values = []
        for _, name, kind, _required, _default, _primary in columns:
            if name == "forecast_id":
                values.append(f"forecast_{index}")
            elif name == "cohort_id":
                values.append(cohort_id)
            elif name == "entry_epoch":
                values.append(index)
            elif name == "passed_frozen_gate":
                values.append(0)
            elif name == "research_only":
                values.append(1)
            elif name == "execution_eligible":
                values.append(0)
            elif "INT" in kind:
                values.append(1)
            elif "REAL" in kind:
                values.append(1.0)
            else:
                values.append(name)
        db.execute(
            f"INSERT INTO forecasts VALUES ({','.join('?' for _ in values)})",
            values,
        )
    db.commit()
    config = {
        "minimum_clear_probability": 0.55,
        "minimum_predicted_magnitude_cost_ratio": 1.5,
    }

    assert proof.summary(db, config)["forecasts"] == 2
    assert proof.summary(db, config, cohort_id="collector_a")["forecasts"] == 1
    assert proof.summary(db, config, cohort_id="collector_b")["forecasts"] == 1
    db.close()


def test_bounded_loader_reads_feature_window_and_exact_outcomes(tmp_path) -> None:
    source = tmp_path / "minutes.sqlite"
    db = sqlite3.connect(source)
    db.execute(
        "CREATE TABLE quote_intensity_minutes_v1 ("
        "minute_epoch INTEGER,instrument TEXT,last_mid REAL,average_spread_pips REAL,"
        "updates INTEGER,imbalance_5s REAL,imbalance_30s REAL,imbalance_120s REAL)"
    )
    rows = [
        (1_000, "EUR_USD", 1.0, 1.0, 1, 0.0, 0.0, 0.0),
        (2_200, "EUR_USD", 1.1, 1.0, 1, 0.0, 0.0, 0.0),
        (4_000, "EUR_USD", 1.2, 1.0, 1, 0.0, 0.0, 0.0),
        (9_000, "EUR_USD", 1.3, 1.0, 1, 0.0, 0.0, 0.0),
    ]
    db.executemany("INSERT INTO quote_intensity_minutes_v1 VALUES (?,?,?,?,?,?,?,?)", rows)
    db.commit()
    db.close()

    by_pair, instruments, loaded = proof.load_required_minutes(
        source,
        feature_epochs={4_000},
        outcome_epochs={9_000},
    )
    assert instruments == ["EUR_USD"]
    assert set(by_pair["EUR_USD"]) == {2_200, 4_000, 9_000}
    assert loaded == 3


def test_required_outcome_epochs_excludes_already_matured(tmp_path) -> None:
    db = proof.open_ledger(tmp_path / "proof.sqlite")
    columns = [row[1] for row in db.execute("PRAGMA table_info(forecasts)")]
    values = []
    for name in columns:
        if name == "forecast_id": values.append("pending")
        elif name in {"research_only"}: values.append(1)
        elif name in {"execution_eligible"}: values.append(0)
        elif name == "entry_epoch": values.append(100)
        elif name == "horizon_sec": values.append(60)
        elif name in {"passed_frozen_gate", "predicted_direction"}: values.append(1)
        elif name.endswith("_json"): values.append("{}")
        elif name.endswith("_pips") or "probability" in name or "confidence" in name or name == "entry_mid": values.append(1.0)
        else: values.append(name)
    db.execute(f"INSERT INTO forecasts VALUES ({','.join('?' for _ in values)})", values)
    db.commit()
    assert proof.required_outcome_epochs(db, 159) == set()
    assert proof.required_outcome_epochs(db, 160) == {160}
    db.close()


def test_pending_forecast_count_tracks_only_unresolved_rows(tmp_path) -> None:
    db = proof.open_ledger(tmp_path / "proof.sqlite")
    columns = [row[1] for row in db.execute("PRAGMA table_info(forecasts)")]
    values = []
    for name in columns:
        if name == "forecast_id": values.append("pending-one")
        elif name == "cohort_id": values.append("unit-cohort")
        elif name in {"research_only"}: values.append(1)
        elif name in {"execution_eligible", "passed_frozen_gate"}: values.append(0)
        elif "INT" in next(row[2] for row in db.execute("PRAGMA table_info(forecasts)") if row[1] == name): values.append(1)
        elif "REAL" in next(row[2] for row in db.execute("PRAGMA table_info(forecasts)") if row[1] == name): values.append(1.0)
        else: values.append(name)
    db.execute(f"INSERT INTO forecasts VALUES ({','.join('?' for _ in values)})", values)
    db.commit()

    assert proof.pending_forecast_count(db) == 1
    outcome_columns = db.execute("PRAGMA table_info(outcomes)").fetchall()
    outcome_values = []
    for _index, name, kind, _required, _default, _primary in outcome_columns:
        if name == "forecast_id": outcome_values.append("pending-one")
        elif name == "execution_eligible": outcome_values.append(0)
        elif name == "research_only": outcome_values.append(1)
        elif "INT" in kind: outcome_values.append(1)
        elif "REAL" in kind: outcome_values.append(1.0)
        elif name.endswith("_json"): outcome_values.append("{}")
        else: outcome_values.append(name)
    db.execute(
        f"INSERT INTO outcomes VALUES ({','.join('?' for _ in outcome_values)})",
        outcome_values,
    )
    db.commit()
    assert proof.pending_forecast_count(db) == 0
    db.close()


def _insert_forecast(
    db: sqlite3.Connection,
    forecast_id: str,
    *,
    cohort_id: str = "unit-cohort",
    entry_epoch: int = 100,
    horizon_sec: int = 60,
    instrument: str = "EUR_USD",
) -> None:
    columns = db.execute("PRAGMA table_info(forecasts)").fetchall()
    values = []
    for _index, name, kind, _required, _default, _primary in columns:
        if name == "forecast_id": values.append(forecast_id)
        elif name == "cohort_id": values.append(cohort_id)
        elif name == "entry_epoch": values.append(entry_epoch)
        elif name == "horizon_sec": values.append(horizon_sec)
        elif name == "instrument": values.append(instrument)
        elif name == "research_only": values.append(1)
        elif name in {"execution_eligible", "passed_frozen_gate"}: values.append(0)
        elif "INT" in kind: values.append(1)
        elif "REAL" in kind: values.append(1.0)
        elif name.endswith("_json"): values.append("{}")
        else: values.append(name)
    db.execute(
        f"INSERT INTO forecasts VALUES ({','.join('?' for _ in values)})",
        values,
    )


def _insert_outcome(db: sqlite3.Connection, forecast_id: str) -> None:
    columns = db.execute("PRAGMA table_info(outcomes)").fetchall()
    values = []
    for _index, name, kind, _required, _default, _primary in columns:
        if name == "forecast_id": values.append(forecast_id)
        elif name == "research_only": values.append(1)
        elif name == "execution_eligible": values.append(0)
        elif "INT" in kind: values.append(1)
        elif "REAL" in kind: values.append(1.0)
        else: values.append(name)
    db.execute(
        f"INSERT INTO outcomes VALUES ({','.join('?' for _ in values)})",
        values,
    )


def test_pending_queue_bootstraps_legacy_rows_then_remains_trigger_exact(
    tmp_path,
) -> None:
    path = tmp_path / "proof.sqlite"
    db = proof.open_ledger(path)
    db.execute("DROP TRIGGER forecasts_pending_outcome_queue_v1")
    db.execute("DROP TRIGGER outcomes_pending_outcome_queue_v1")
    db.execute("DELETE FROM prospective_runtime_metadata_v1")
    _insert_forecast(db, "legacy-pending", entry_epoch=1_000, horizon_sec=300)
    db.commit()
    db.close()

    db = proof.open_ledger(path)
    assert proof.pending_forecast_count(db) == 1
    assert proof.required_outcome_epochs(db, 1_299) == set()
    assert proof.required_outcome_epochs(db, 1_300) == {1_300}

    _insert_forecast(db, "new-pending", entry_epoch=2_000, horizon_sec=60)
    assert proof.pending_forecast_count(db) == 2
    _insert_outcome(db, "legacy-pending")
    assert proof.pending_forecast_count(db) == 1
    assert proof.required_outcome_epochs(db, 3_000) == {2_060}
    db.close()


def test_exact_summary_cache_is_invalidated_by_every_evidence_insert(
    tmp_path,
) -> None:
    db = proof.open_ledger(tmp_path / "proof.sqlite")
    config = {
        "minimum_clear_probability": 0.55,
        "minimum_predicted_magnitude_cost_ratio": 1.5,
    }
    _insert_forecast(db, "first")
    assert proof.exact_cached_summary(db, config)["forecasts"] == 1
    assert db.execute(
        "SELECT COUNT(*) FROM prospective_exact_summary_cache_v1"
    ).fetchone()[0] == 1

    _insert_forecast(db, "second", entry_epoch=200)
    assert db.execute(
        "SELECT COUNT(*) FROM prospective_exact_summary_cache_v1"
    ).fetchone()[0] == 0
    assert proof.exact_cached_summary(db, config)["forecasts"] == 2

    _insert_outcome(db, "first")
    assert db.execute(
        "SELECT COUNT(*) FROM prospective_exact_summary_cache_v1"
    ).fetchone()[0] == 0
    assert proof.exact_cached_summary(db, config)["matured"] == 1
    db.close()


def test_pending_queries_use_bounded_derived_queue_not_forecast_history(
    tmp_path,
) -> None:
    db = proof.open_ledger(tmp_path / "proof.sqlite")
    for index in range(2_000):
        forecast_id = f"matured-{index}"
        _insert_forecast(db, forecast_id, entry_epoch=index * 60)
        _insert_outcome(db, forecast_id)
    _insert_forecast(db, "only-pending", entry_epoch=500_000, horizon_sec=300)
    db.commit()

    due_plan = " ".join(
        str(row[3])
        for row in db.execute(
            "EXPLAIN QUERY PLAN SELECT DISTINCT exit_epoch "
            "FROM pending_outcome_queue_v1 WHERE exit_epoch<=?",
            (1_000_000,),
        )
    )
    count_plan = " ".join(
        str(row[3])
        for row in db.execute(
            "EXPLAIN QUERY PLAN SELECT COUNT(*) FROM pending_outcome_queue_v1"
        )
    )
    assert "forecasts" not in due_plan
    assert "forecasts" not in count_plan
    assert "pending_outcome_queue_v1_exit_epoch" in due_plan

    def vm_steps(sql: str) -> int:
        steps = 0

        def count_step() -> int:
            nonlocal steps
            steps += 1
            return 0

        db.set_progress_handler(count_step, 1)
        try:
            db.execute(sql, (1_000_000,)).fetchall()
        finally:
            db.set_progress_handler(None, 0)
        return steps

    legacy_steps = vm_steps(
        "SELECT DISTINCT f.entry_epoch+f.horizon_sec "
        "FROM forecasts f LEFT JOIN outcomes o ON o.forecast_id=f.forecast_id "
        "WHERE o.forecast_id IS NULL AND f.entry_epoch+f.horizon_sec<=?"
    )
    queue_steps = vm_steps(
        "SELECT DISTINCT exit_epoch FROM pending_outcome_queue_v1 "
        "WHERE exit_epoch<=?"
    )
    assert queue_steps * 20 < legacy_steps
    assert proof.required_outcome_epochs(db, 1_000_000) == {500_300}
    assert proof.pending_forecast_count(db) == 1
    db.close()


def test_source_clock_reads_only_recent_highwater_buckets(tmp_path) -> None:
    path = tmp_path / "minutes.sqlite"
    db = sqlite3.connect(path)
    db.execute(
        "CREATE TABLE quote_intensity_minutes_v1 ("
        "minute_epoch INTEGER NOT NULL,instrument TEXT NOT NULL,"
        "last_broker_time TEXT NOT NULL,last_event_epoch REAL NOT NULL,"
        "PRIMARY KEY(minute_epoch,instrument))"
    )
    db.executemany(
        "INSERT INTO quote_intensity_minutes_v1 VALUES (?,?,?,?)",
        [
            (100, "OLD_PAIR", "old", 999_999.0),
            (9_940, "EUR_USD", "recent-a", 9_999.0),
            (10_000, "USD_JPY", "recent-b", 10_005.0),
        ],
    )
    db.commit()
    db.close()

    clock = proof.source_clock(path)
    assert clock["source_highwater_epoch"] == 10_000
    assert clock["last_event_epoch"] == 10_005.0
    assert clock["last_broker_time"] == "recent-b"
    assert clock["instrument_count"] == 3
    assert clock["recent_source_instrument_count"] == 2
    assert clock["clock_scan_contract"] == "bounded_latest_two_minute_buckets_v1"
    assert clock["instrument_coverage_lookback_sec"] == 3_600


def test_maturity_capture_consumes_only_the_exact_pending_queue_row(
    tmp_path,
) -> None:
    ledger = proof.open_ledger(tmp_path / "proof.sqlite")
    _insert_forecast(
        ledger,
        "due-forecast",
        entry_epoch=100,
        horizon_sec=60,
        instrument="EUR_USD",
    )
    source_path = tmp_path / "minutes.sqlite"
    source_db = sqlite3.connect(source_path)
    source_db.execute(
        "CREATE TABLE quote_intensity_minutes_v1 ("
        "minute_epoch INTEGER NOT NULL,instrument TEXT NOT NULL,"
        "last_broker_time TEXT NOT NULL,last_event_epoch REAL NOT NULL,"
        "PRIMARY KEY(minute_epoch,instrument))"
    )
    source_db.execute(
        "INSERT INTO quote_intensity_minutes_v1 VALUES (?,?,?,?)",
        (160, "EUR_USD", "2026-08-28T18:00:00Z", 160.5),
    )
    source_db.commit()
    source_db.close()

    proof.source._PIP_SIZES = {"EUR_USD": 0.0001}
    matured = proof.mature_outcomes(
        ledger,
        {"EUR_USD": {160: {"mid": 1.0010, "spread": 1.0}}},
        {"latest_completed_epoch": 160},
        0.1,
        dt.datetime(2026, 8, 28, 18, 0, tzinfo=UTC),
        source_db=source_path,
    )

    assert matured == 1
    assert proof.pending_forecast_count(ledger) == 0
    outcome = ledger.execute(
        "SELECT forecast_id,signed_move_pips,modeled_cost_pips "
        "FROM outcomes WHERE forecast_id='due-forecast'"
    ).fetchone()
    assert outcome is not None
    assert outcome[0] == "due-forecast"
    ledger.close()
