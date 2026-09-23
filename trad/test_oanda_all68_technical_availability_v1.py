"""Availability contract tests; owned fixtures only, no live stores or workers."""
from copy import deepcopy
import json
import sqlite3
import zlib

import pytest

import oanda_all68_technical_availability_v1 as report


def fixture_inputs():
    config = json.loads(report.DEFAULT_CONFIG.read_bytes())
    registry = report.kernel.feature_registry()
    names = [row["name"] for row in registry]
    now = 1789538000.
    t = int((now-90)//60)*60
    published = t+65
    env = {"generated_epoch": now-1, "generated_utc": report.utc(now-1),
           "publication_generation": "fixture-generation", "operations_runtime_id": "a"*64,
           "pairs": {}}
    status = {"publication_generation": env["publication_generation"], "operations_runtime_id": "a"*64}
    quotes = {"generated_utc": report.utc(now-1), "connection_generation": 9,
              "coverage": {"retained_last_known_instruments": []}, "quotes": {}}
    heartbeat = {"updated_at": report.utc(now-1),
                 "details": {"stream": {"connected": True, "connection_generation": 9}}}
    retained = {}
    for pair in config["pairs"]:
        values = {name: 1. for name in names}
        feature_hash = report.digest(report.encoded(list(values.values())))
        env["pairs"][pair] = {
            "availability": {"status": "current", "bar_start_utc": report.utc(t),
                "bar_end_utc": report.utc(t+60), "published_utc": report.utc(published),
                "feature_hash": feature_hash, "available_features": len(names), "feature_count": len(names)},
            "observation": {"values": values, "feature_hash": feature_hash,
                            "bar_start_epoch": t, "published_epoch": published},
            "continuity": {"schema": report.continuity.SCHEMA,
                "continuity": {"latest_observed_bar_start_epoch": t, "consecutive_suffix_rows": 700,
                               "missing_intervals": []},
                "feature_support": {"features": {row["name"]: {"category": "available",
                    "lookback_bars": row["lookback_bars"],
                    "required_start_epoch": t-(row["lookback_bars"]-1)*60} for row in registry}}}}
        retained[pair] = {"t": t, "feature_hash": feature_hash, "values": list(values.values()),
            "available_count": len(names), "feature_count": len(names), "published_epoch": published,
            "bar": {"time": t, "high": 1.2, "low": 1.1}, "receipt_id": "b"*64,
            "input_hash": "c"*64, "first_observed_epoch": t+61,
            "receipt": {"source_path": pair+"_M1.csv", "source_read_completed_epoch": t+62,
                        "source_tail_sha256": "d"*64}}
        quotes["quotes"][pair] = {"bid": 1.1, "ask": 1.2, "time": report.utc(now-2), "tradeable": True}
    return config, env, status, quotes, heartbeat, retained, now


def build(parts, **kwargs):
    config, env, status, quotes, heartbeat, retained, now = parts
    return report.build_report(config, env, status, quotes, heartbeat, retained, {}, now=now, **kwargs)


def missing(parts, pair, names, category):
    _, env, _, _, _, retained, _ = parts
    state = env["pairs"][pair]
    for name in names:
        state["observation"]["values"][name] = None
        state["continuity"]["feature_support"]["features"][name]["category"] = category
    values = list(state["observation"]["values"].values())
    feature_hash = report.digest(report.encoded(values))
    count = sum(value is not None for value in values)
    retained[pair].update(values=values, feature_hash=feature_hash, available_count=count)
    state["availability"].update(feature_hash=feature_hash, available_features=count)
    state["observation"]["feature_hash"] = feature_hash


def test_all68_exact_coverage_and_original_clocks():
    parts = fixture_inputs()
    result = build(parts)
    assert result["reported_pair_count"] == 68
    assert result["all_configured_pairs_reported"] is True
    assert result["counts"] == {"complete": 68}
    row = result["pairs"]["EUR_USD"]
    assert row["features"]["finite_count"] == 216
    assert row["features"]["publication_epoch"] == parts[5]["EUR_USD"]["published_epoch"]
    assert row["features"]["bar_age_seconds"] == parts[6]-parts[5]["EUR_USD"]["t"]-60
    assert result["numerical_recomputation_performed"] is False
    assert result["can_place_orders"] is False


def test_exact_support_and_confirmed_denominator_are_distinct():
    parts = fixture_inputs()
    missing(parts, "EUR_USD", ["m1__return_60_bps"], "missing_elapsed_support")
    missing(parts, "CAD_SGD", report.ZERO_RANGE_FEATURES, "undefined_or_numerical")
    parts[5]["CAD_SGD"]["bar"].update(high=1.1, low=1.1)
    result = build(parts)
    assert result["pairs"]["EUR_USD"]["features"]["missing_counts_by_reason"] == {"missing_elapsed_support": 1}
    cad = result["pairs"]["CAD_SGD"]["features"]
    assert cad["missing_counts_by_reason"] == {"undefined_or_numerical": 3}
    assert set(cad["confirmed_zero_range_denominator_features"]) == report.ZERO_RANGE_FEATURES


def test_current_connection_does_not_make_old_quote_current():
    parts = fixture_inputs()
    parts[3]["quotes"]["EUR_USD"]["time"] = report.utc(parts[6]-90)
    parts[3]["quotes"]["EUR_TRY"].update(time=report.utc(parts[6]-86400), tradeable=False)
    result = build(parts)
    assert result["pairs"]["EUR_USD"]["quote"]["current_connection"] is True
    assert result["pairs"]["EUR_USD"]["status"] == "quote_stale"
    assert result["pairs"]["EUR_TRY"]["status"] == "not_tradeable"
    assert result["pairs"]["EUR_TRY"]["quote"]["age_seconds"] == 86400


@pytest.mark.parametrize("problem", ["future_envelope", "generation", "runtime", "malformed_pairs"])
def test_invalid_global_binding_keeps68_unavailable_rows(problem):
    parts = fixture_inputs()
    if problem == "future_envelope":
        parts[1]["generated_epoch"] = parts[6]+1
    elif problem == "generation":
        parts[2]["publication_generation"] = "other"
    elif problem == "runtime":
        parts[2]["operations_runtime_id"] = "other"
    else:
        parts[1]["pairs"] = None
    result = build(parts)
    assert len(result["pairs"]) == 68
    assert result["feature_status_counts"] == {"unavailable": 68}
    assert all(row["features"]["finite_count"] is None for row in result["pairs"].values())


@pytest.mark.parametrize("problem", ["future_publication", "value_mismatch", "bad_support", "malformed_state"])
def test_pair_failure_is_explicit_and_does_not_hide_other67(problem):
    parts = fixture_inputs()
    pair = "EUR_USD"
    if problem == "future_publication":
        parts[5][pair]["published_epoch"] = parts[6]+1
    elif problem == "value_mismatch":
        parts[1]["pairs"][pair]["observation"]["values"]["m1__return_60_bps"] = 9.
    elif problem == "bad_support":
        parts[1]["pairs"][pair]["continuity"]["feature_support"]["features"]["m1__return_60_bps"]["required_start_epoch"] += 60
    else:
        parts[1]["pairs"][pair] = None
    result = build(parts)
    assert result["pairs"][pair]["features"]["status"] == "unavailable"
    assert result["pairs"][pair]["features"]["finite_count"] is None
    assert result["feature_status_counts"] == {"complete": 67, "unavailable": 1}


def test_underlying_bar_age_is_rechecked_at_read_time():
    parts = fixture_inputs()
    parts = (*parts[:-1], parts[-1]+200)
    parts[1].update(generated_epoch=parts[6]-1, generated_utc=report.utc(parts[6]-1))
    parts[3]["generated_utc"] = report.utc(parts[6]-1)
    parts[4]["updated_at"] = report.utc(parts[6]-1)
    result = build(parts)
    assert result["feature_status_counts"] == {"stale": 68}
    assert result["pairs"]["EUR_USD"]["features"]["finite_count"] == 216


def test_gap_scope_clips_both_ends_and_excludes_trailing_latency():
    parts = fixture_inputs()
    t = parts[5]["EUR_USD"]["t"]
    low = t-602*60
    gaps = [{"start_epoch": low-120, "end_epoch": low+60, "kind": "broker_omitted_requested_minute", "location": "interior_gap"},
            {"start_epoch": t+60, "end_epoch": t+120, "kind": "missing_unknown", "location": "interior_gap"},
            {"start_epoch": t+60, "end_epoch": t+60, "kind": "missing_unknown", "location": "trailing_unobserved"}]
    parts[1]["pairs"]["EUR_USD"]["continuity"]["continuity"]["missing_intervals"] = gaps
    result = build(parts)
    assert result["pairs"]["EUR_USD"]["features"]["support_gap_minutes_by_evidence"] == {"broker_omitted_requested_minute": 2}


def test_unreadable_inputs_still_report_all68(tmp_path, monkeypatch):
    config = fixture_inputs()[0]
    config["output_root"] = str(tmp_path)
    monkeypatch.setattr(report, "verify_dependencies", lambda *args: ({}, {"fixture": True}))
    result = report.capture(config, quote_path=tmp_path/"absent_quotes.json", heartbeat_path=tmp_path/"absent_heartbeat.json")
    assert len(result["pairs"]) == 68
    assert result["source_errors"]
    assert result["feature_status_counts"] == {"unavailable": 68}
    assert result["quote_status_counts"] == {"unavailable": 68}


def test_malformed_and_oversize_json_are_explicit(tmp_path):
    path = tmp_path/"input.json"
    for raw in (b'{', b'[]', b'{"x":NaN}'):
        path.write_bytes(raw)
        with pytest.raises(ValueError):
            report.read_json(path, 100)
    path.write_bytes(b'{"x":1}')
    with pytest.raises(ValueError, match="source_byte_bound"):
        report.read_json(path, 2)


def test_helper_or_config_binding_failure_cannot_publish_valid_features(monkeypatch):
    parts = fixture_inputs()
    result = build(parts, source_errors={"identity_binding": "frozen_source_binding_changed"})
    assert result["feature_status_counts"] == {"unavailable": 68}
    config, reference = report.read_config(report.DEFAULT_CONFIG)
    monkeypatch.setattr(report, "IMPORTED_SOURCE_BINDINGS", {"oanda_rolling_technical_features_v1.py": "0"*64})
    with pytest.raises(ValueError, match="imported_helper_binding_changed"):
        report.verify_dependencies(config, reference)


def test_read_only_store_join_rejects_bad_receipt_hash(tmp_path):
    parts = fixture_inputs()
    config, env, _, _, _, retained, _ = parts
    pair = "EUR_USD"
    r = retained[pair]
    database = tmp_path/"owned.sqlite"
    connection = sqlite3.connect(database)
    connection.executescript('''CREATE TABLE observations(pair,t,feature_hash,values_blob,feature_count,available_count,published);
        CREATE TABLE bars(pair,t,body,input_hash,first_observed,receipt_id);
        CREATE TABLE receipts(id,pair,observed,body);''')
    receipt = {"instrument":pair,"observed_epoch":r["first_observed_epoch"]}
    receipt_raw = report.encoded(receipt)
    receipt_id = report.digest(receipt_raw)
    bar_raw = report.encoded(r["bar"])
    connection.execute("INSERT INTO observations VALUES(?,?,?,?,?,?,?)", (pair,r["t"],r["feature_hash"],zlib.compress(report.encoded(r["values"])),216,216,r["published_epoch"]))
    connection.execute("INSERT INTO bars VALUES(?,?,?,?,?,?)", (pair,r["t"],bar_raw,report.digest(bar_raw),r["first_observed_epoch"],receipt_id))
    connection.execute("INSERT INTO receipts VALUES(?,?,?,?)", (receipt_id,pair,r["first_observed_epoch"],receipt_raw))
    connection.commit()
    before = database.read_bytes()
    values, errors = report.read_store_evidence(database, [pair], env)
    assert not errors and values[pair]["receipt_id"] == receipt_id
    assert database.read_bytes() == before
    connection.execute("UPDATE receipts SET body=?", (b'{}',)); connection.commit()
    values, errors = report.read_store_evidence(database, [pair], env)
    assert not values and "hash_mismatch" in errors[pair]
    connection.close()


def test_cli_refuses_to_overwrite_original_source(tmp_path):
    with pytest.raises(ValueError, match="report_output_must_not_replace_source"):
        report.main(["--once", "--output", str(report.DEFAULT_QUOTES)])
