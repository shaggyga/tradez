import json
import sqlite3

import oanda_opportunity_decision_level_monitor as monitor


def _fixture(tmp_path):
    state = tmp_path / "state.json"
    state.write_text(json.dumps({"collection_cohort": {"cohort_id": "clean"}}))
    db_path = tmp_path / "ledger.sqlite"
    db = sqlite3.connect(db_path)
    db.execute("""CREATE TABLE forecasts(
        forecast_id TEXT,cohort_id TEXT,entry_epoch INTEGER,issued_at_utc TEXT,horizon_sec INTEGER,instrument TEXT,
        predicted_clear_probability REAL,predicted_magnitude_pips REAL,
        modeled_entry_cost_pips REAL,predicted_direction INTEGER,feature_json TEXT)""")
    db.execute("""CREATE TABLE outcomes(
        forecast_id TEXT,movement_cleared_cost INTEGER,direction_correct INTEGER,
        predicted_side_net_pips REAL,signed_move_pips REAL)""")
    pairs = [
        ("a","EUR_USD",.9,3,1,1,1,2),
        ("b","GBP_JPY",.8,3,1,-1,1,1),
        ("c","AUD_CAD",.7,3,1,1,0,-1),
        ("d","EUR_CHF",.95,3,1,1,1,4),
    ]
    for fid,pair,pclear,mag,cost,side,correct,net in pairs:
        features=json.dumps({"return_5m_pips": side, "return_15m_pips": side})
        db.execute("INSERT INTO forecasts VALUES (?,?,?,?,?,?,?,?,?,?,?)",(fid,"clean",100,"2026-08-14T16:00:00+00:00",300,pair,pclear,mag,cost,side,features))
        db.execute("INSERT INTO outcomes VALUES (?,?,?,?,?)",(fid,int(net>0),correct,net,side*(net+cost)))
    db.execute("INSERT INTO forecasts VALUES (?,?,?,?,?,?,?,?,?,?,?)",("old","old",100,"2026-08-14T16:00:00+00:00",300,"USD_JPY",.99,9,1,1,"{}"))
    db.commit(); db.close()
    return state, db_path


def test_decision_level_monitor_filters_exact_cohort_and_collapses_rows(tmp_path):
    state, db_path = _fixture(tmp_path)
    result = monitor.build(state, db_path)
    assert result["collector_cohort_id"] == "clean"
    assert result["raw_pair_rows"] == 4
    assert result["decision_epochs"] == 1
    assert result["top_one"]["selected_rows"] == 1
    assert result["top_one"]["average_predicted_side_net_pips"] == 4.0
    h300 = result["direction_ablation"]["by_horizon"]["300"]
    assert h300["frozen_model"]["matured_nonabstaining_rows"] == 1
    assert h300["frozen_model"]["average_after_cost_pips"] == 4.0
    assert h300["inverse_model"]["average_after_cost_pips"] == -6.0
    # EUR/CHF blocks EUR/USD; the remaining two disjoint pairs create exactly three.
    assert result["exactly_three_currency_disjoint"]["selected_rows"] == 3
    assert result["exactly_three_currency_disjoint"]["decisions"] == 1


def test_disjoint_selector_returns_empty_when_three_are_unavailable():
    rows = [
        {"instrument":"EUR_USD","predicted_clear_probability":.9,"predicted_magnitude_pips":2,"modeled_entry_cost_pips":1},
        {"instrument":"EUR_JPY","predicted_clear_probability":.8,"predicted_magnitude_pips":2,"modeled_entry_cost_pips":1},
        {"instrument":"USD_JPY","predicted_clear_probability":.7,"predicted_magnitude_pips":2,"modeled_entry_cost_pips":1},
    ]
    assert monitor.select_disjoint_three(rows) == []


def test_run_once_refreshes_json_and_markdown_artifacts(tmp_path):
    state, db_path = _fixture(tmp_path)
    output = tmp_path / "current.json"
    report = tmp_path / "current.md"
    result = monitor.run_once(state, db_path, output, report)
    assert json.loads(output.read_text())["decision_epochs"] == 1
    assert "Opportunity Decision-Level Monitor" in report.read_text()
    assert "### By horizon" in report.read_text()
    assert result["can_place_orders"] is False
