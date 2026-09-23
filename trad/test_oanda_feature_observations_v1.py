"""Synthetic observation tests; producer functions are extracted without imports."""
import ast
import gzip
import hashlib
import json
import math
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import oanda_feature_observations_v1 as observations
from oanda_intrahour_forecast_contract import contract_payload, filter_context_views, flatten_context_views, structural_feature_view


ROOT = Path(__file__).parent
STAMP = "2026-09-13T16:01:10+00:00"
ORIGIN = "2026-09-13T16:00:00+00:00"


def snapshot():
    clock = {"observed_utc": STAMP, "clock_basis": "producer_capture"}
    primary = {"candle_time": ORIGIN, "r1_pips": 2.0, "same": 4.0, "primary_only": 7.0, "missing": None, "bad": math.nan, "pattern_count_forecasts": {"arm": {"value": 3.0, "source_id": "synthetic"}}}
    structural = {"candle_time": ORIGIN, "r1_pips": 1.0, "same": 4.0, "missing": None}
    return {"schema_version": 1, "snapshot_id": "synthetic-cycle", "generated_utc": STAMP, "generated_epoch": datetime.fromisoformat(STAMP).timestamp(), "coverage": {"accepted_instrument_count": 1, "excluded_instrument_count": 1, "quote_exclusions": [{"instrument": "USD_JPY", "reason": "stale_quote"}]}, "observation_source": {"producer_id": "synthetic", "producer_contract_id": "fixture-v1"}, "instruments": {"EUR_USD": {"quote": {"bid": 1.1, "ask": 1.1001, "time": STAMP}, "features": {"primary_only": 7.0}, "timeframe_features": {"M1": structural}, "unified_forecast_features": {"m1__same": 4.0}, "series": {"M1": [1.0, 1.1]}}}, "observation_inputs": {"instruments": {"EUR_USD": {"groups": {"primary": observations.capture_feature_group(primary, input_timeframe="M1", clock=clock), "timeframe:M1": observations.capture_feature_group(structural, input_timeframe="M1", clock=clock)}, "coverage": {"quote_accepted": True}}, "USD_JPY": {"quote": {"time": "2026-09-13T15:00:00Z"}, "groups": {"primary": observations.capture_feature_group({"candle_time": ORIGIN, "r1_pips": 5.0}, input_timeframe="M1", clock=clock)}, "coverage": {"quote_accepted": False, "quote_exclusion_reason": "stale_quote"}}}}}


def extracted(filename, name, overrides=None):
    source = ROOT / filename
    tree = ast.parse(source.read_text(encoding="utf-8-sig"))
    node = next(item for item in tree.body if isinstance(item, ast.FunctionDef) and item.name == name)
    namespace = {"Any": Any, "Path": Path, "datetime": datetime, "timezone": timezone, "time": time, "math": math, "hashlib": hashlib, "os": os, "__file__": str(source)}
    namespace.update(overrides or {})
    module = ast.Module(body=[ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0), node], type_ignores=[])
    exec(compile(ast.fix_missing_locations(module), str(source), "exec"), namespace)
    return namespace[name]


def test_complete_scalars_missingness_conflicts_and_aliases():
    frame = observations.build_observation_frame(snapshot())
    row = frame["instruments"]["EUR_USD"]
    primary, m1 = row["groups"]["primary"], row["groups"]["timeframe:M1"]
    assert primary["values"]["primary_only"] == 7.0
    assert primary["value_states"]["missing"] == "missing"
    assert primary["value_states"]["bad"] == "nonfinite"
    assert primary["nonfinite_values"]["bad"] == {observations.NONFINITE_TAG: "nan"}
    assert primary["feature_ids"]["same"] == m1["feature_ids"]["same"]
    assert m1["aliases"]["same"]["canonical_group"] == "primary"
    assert primary["feature_ids"]["r1_pips"] != m1["feature_ids"]["r1_pips"]
    assert "r1_pips" in primary["conflicts"]
    assert set(row["groups"]) == {"primary", "timeframe:M1"}
    assert row["unified_aliases"]["m1__same"]["feature_id"] == primary["feature_ids"]["same"]
    assert "payload" not in primary["nested_lineage"]["pattern_count_forecasts"]
    assert frame["instruments"]["USD_JPY"]["groups"]["primary"]["values"]["r1_pips"] == 5.0
    assert frame["instruments"]["USD_JPY"]["coverage"]["quote_accepted"] is False


def test_source_clocks_never_invented_from_new_generation():
    raw = {"snapshot_id": "legacy", "generated_utc": STAMP, "instruments": {"EUR_USD": {"features": {"candle_time": ORIGIN, "value": 2.0}, "quote": {"time": STAMP}}}}
    group = observations.build_observation_frame(raw)["instruments"]["EUR_USD"]["groups"]["primary"]
    assert group["observed_utc"] == ""
    assert group["clock_basis"] == "unknown"
    assert group["status"] == "clock_unavailable"
    assert group["bar_complete_utc"] == "2026-09-13T16:01:00+00:00"
    assert observations.parse_utc("2026-09-13T16:01:00") is None
    assert observations.parse_utc("bad") is None


def test_defaults_do_not_become_measured_zero_or_neutral():
    result = observations.capture_feature_group({"supervised_ready": False, "supervised_rank_percentile": 0.0, "supervised_expected_net_pips": None, "volume_ratio_12": 1.0, "volumes": [2.0], "pos20": 0.5, "closes": [1.0] * 20, "quote_receive_age_sec": 0.0, "actual_zero": 0.0}, input_timeframe="M1")
    assert result["value_states"]["supervised_rank_percentile"] == "default"
    assert result["value_states"]["supervised_expected_net_pips"] == "missing"
    assert result["value_states"]["volume_ratio_12"] == "default"
    assert result["value_states"]["pos20"] == "default_flat_range"
    assert result["value_states"]["quote_receive_age_sec"] == "unknown_or_default"
    assert result["value_states"]["actual_zero"] == "observed"


def test_lossless_original_archive_and_content_identity(tmp_path):
    raw = snapshot()
    path = observations.archive_observation_snapshot(raw, tmp_path)
    before = path.read_bytes()
    assert observations.archive_observation_snapshot(raw, tmp_path) == path
    assert path.read_bytes() == before
    with gzip.open(path, "rt", encoding="utf-8") as stream:
        envelope = json.load(stream)
    assert envelope["original_snapshot"] == observations.normalize_json(raw)
    assert envelope["payload_sha256"] == observations.payload_sha256(raw)
    assert envelope["original_snapshot"]["observation_inputs"]["instruments"]["EUR_USD"]["groups"]["primary"]["nested_lineage"]["pattern_count_forecasts"]["payload"]["arm"]["value"] == 3.0
    raw["instruments"]["EUR_USD"]["features"]["primary_only"] = 9.0
    with pytest.raises(ValueError, match="identity"):
        observations.archive_observation_snapshot(raw, tmp_path)
    raw["generated_utc"] = "2026-09-14T16:01:10Z"
    with pytest.raises(ValueError, match="identity"):
        observations.archive_observation_snapshot(raw, tmp_path)
    assert len(list(tmp_path.glob("date=*/hour=*/obs_*.json.gz"))) == 1
    assert path.read_bytes() == before


def test_empty_observation_and_envelope_bound(tmp_path, monkeypatch):
    raw = {"snapshot_id": "empty", "generated_utc": STAMP, "instruments": {}, "coverage": {"accepted_instrument_count": 0, "quote_exclusions": [{"instrument": "EUR_USD", "reason": "missing_quote"}]}}
    path = observations.archive_observation_snapshot(raw, tmp_path / "empty")
    with gzip.open(path, "rt", encoding="utf-8") as stream:
        assert json.load(stream)["frame"]["coverage"] == raw["coverage"]
    monkeypatch.setattr(observations, "MAX_ARCHIVE_BYTES", 16)
    with pytest.raises(ValueError, match="bounded"):
        observations.archive_observation_snapshot(raw, tmp_path / "too_large")
    assert not (tmp_path / "too_large").exists()


def writer_namespace(events, failure=False):
    def archive(payload, root):
        events.append("archive")
        if failure:
            raise OSError("synthetic archive failure")
        return observations.archive_observation_snapshot(payload, root)
    def atomic(path, payload):
        events.append("latest")
        path.write_text(json.dumps(payload), encoding="utf-8")
    return {"np": SimpleNamespace(integer=int, floating=float), "safe_float": lambda value, default=0.0: float(value) if value is not None else default, "outcome_quote_rejection_reason": lambda quote: "missing_quote" if quote is None else "", "structural_feature_view": structural_feature_view, "filter_context_views": filter_context_views, "flatten_context_views": flatten_context_views, "intrahour_contract_payload": contract_payload, "capture_feature_group": observations.capture_feature_group, "normalize_observation_json": observations.normalize_json, "OBSERVATION_PRODUCER_CONTRACT": observations.PRODUCER_CONTRACT, "observation_source_sha256": observations.observation_source_sha256, "archive_observation_snapshot": archive, "atomic_json": atomic}


def test_producer_archives_excluded_features_before_latest_publication(tmp_path):
    events = []
    writer = extracted("oanda_practice_shadow_strategy_lab.py", "write_live_model_feature_snapshot", writer_namespace(events))
    path = tmp_path / "latest.json"
    assert writer(path, "excluded-only", {"EUR_USD": {"candle_time": ORIGIN, "score": 3.0, "missing": None}}, {}, {}, observation_archive_root=tmp_path / "observations") == 0
    assert events == ["archive", "latest"]
    latest = json.loads(path.read_text())
    assert latest["instruments"] == {}
    frame = observations.build_observation_frame(latest)
    assert frame["instruments"]["EUR_USD"]["groups"]["primary"]["values"]["score"] == 3.0
    assert frame["instruments"]["EUR_USD"]["coverage"]["quote_exclusion_reason"] == "missing_quote"


def test_producer_failure_does_not_claim_latest_history(tmp_path):
    events = []
    writer = extracted("oanda_practice_shadow_strategy_lab.py", "write_live_model_feature_snapshot", writer_namespace(events, failure=True))
    latest = tmp_path / "latest.json"
    latest.write_text("prior", encoding="utf-8")
    with pytest.raises(OSError, match="synthetic"):
        writer(latest, "fails", {}, {}, {}, observation_archive_root=tmp_path / "observations")
    assert latest.read_text() == "prior"
    assert events == ["archive"]


def test_default_writer_archive_stays_inside_requested_output_directory(tmp_path):
    events = []
    writer = extracted("oanda_practice_shadow_strategy_lab.py", "write_live_model_feature_snapshot", writer_namespace(events))
    writer(tmp_path / "latest.json", "local-default", {}, {}, {})
    assert len(list((tmp_path / "feature_observations_v1").glob("date=*/hour=*/obs_*.json.gz"))) == 1


def test_model_outputs_and_metadata_are_distinct_and_source_change_splits_cohort():
    raw = snapshot()
    primary = raw["observation_inputs"]["instruments"]["EUR_USD"]["groups"]["primary"]
    primary.update(observations.capture_feature_group({"candle_time": ORIGIN, "supervised_ready": True, "supervised_expected_net_pips": 2.0, "r1_pips": 3.0, "input_timeframe_seconds": 60}, input_timeframe="M1", clock={"observed_utc": STAMP, "clock_basis": "producer_capture"}))
    first = observations.build_observation_frame(raw)
    groups = first["instruments"]["EUR_USD"]["groups"]
    assert "supervised_expected_net_pips" not in groups["primary"]["values"]
    assert groups["model_output:primary"]["group_kind"] == "model_output"
    assert groups["model_output:primary"]["values"]["supervised_expected_net_pips"] == 2.0
    assert groups["primary"]["value_kinds"]["candle_time"] == "metadata"
    raw["observation_source"]["observation_implementation_sha256"] = "synthetic-v2"
    assert observations.build_observation_frame(raw)["source_schema_id"] != first["source_schema_id"]


def test_m5_components_and_books_use_own_clocks_and_canonical_ids():
    raw = snapshot()
    clock = {"observed_utc": STAMP, "clock_basis": "producer_capture", "component_clocks": {"order_book_source_utc": "2026-09-13T15:00:00Z"}}
    captures = raw["observation_inputs"]["instruments"]["EUR_USD"]["groups"]
    captures["primary"] = observations.capture_feature_group({"candle_time": ORIGIN, "m5_r1_pips": 2.0, "m5_r3_pips": 4.0, "series_origins": {"M5": "2026-09-13T15:55:00Z"}, "order_book_near_5_imbalance": .3, "position_book_near_5_imbalance": .4}, input_timeframe="M1", clock=clock)
    captures["timeframe:M5"] = observations.capture_feature_group({"candle_time": "2026-09-13T15:55:00Z", "r1_pips": 2.0}, input_timeframe="M5", clock=clock)
    groups = observations.build_observation_frame(raw)["instruments"]["EUR_USD"]["groups"]
    primary = groups["primary"]
    assert primary["feature_clocks"]["m5_r1_pips"]["input_timeframe"] == "M5"
    assert primary["feature_clocks"]["m5_r1_pips"]["bar_complete_utc"] == "2026-09-13T16:00:00+00:00"
    assert primary["feature_ids"]["m5_r1_pips"] == groups["timeframe:M5"]["feature_ids"]["r1_pips"]
    assert groups["timeframe:M5"]["aliases"]["r1_pips"]["canonical_group"] == "primary"
    assert primary["feature_clocks"]["order_book_near_5_imbalance"]["observed_utc"] == "2026-09-13T15:00:00+00:00"
    assert primary["feature_clocks"]["position_book_near_5_imbalance"]["clock_basis"] == "unknown_book_source"
    captures["primary"]["nested_lineage"].pop("series_origins")
    assert observations.build_observation_frame(raw)["instruments"]["EUR_USD"]["groups"]["primary"]["feature_clocks"]["m5_r1_pips"]["clock_basis"] == "unknown_component_completion"


def test_writer_preserves_component_clocks_before_final_quote_refresh(tmp_path):
    writer = extracted("oanda_practice_shadow_strategy_lab.py", "write_live_model_feature_snapshot", writer_namespace([]))
    features = {"candle_time": ORIGIN, "r1_pips": 2.0, "live_spread_pips": .4}
    earlier = "2026-09-13T15:55:00Z"
    quote = SimpleNamespace(bid=1.1, ask=1.1001, time=STAMP, source="synthetic", tradeable=True)
    clocks = {"primary_observed_utc": STAMP, "timeframe_observed_utc": STAMP, "quote_observed_utc_by_instrument": {"EUR_USD": earlier}, "primary_calculated_utc_by_instrument": {"EUR_USD": "2026-09-13T16:01:01Z"}}
    path = tmp_path / "latest.json"
    writer(path, "clocked", {"EUR_USD": features}, {"EUR_USD": quote}, {"EUR_USD": {"order_book_time_utc": earlier}}, observation_clocks=clocks)
    original = json.loads(path.read_text())
    assert original["observation_source"]["observation_implementation_sha256"] == observations.observation_source_sha256()
    row = observations.build_observation_frame(original)["instruments"]["EUR_USD"]
    assert row["quote"]["time"] == STAMP
    assert row["groups"]["primary"]["component_clocks"]["quote_feature_source_utc"] == earlier
    assert row["groups"]["primary"]["component_clocks"]["primary_calculated_utc"] == "2026-09-13T16:01:01Z"


def test_legacy_archive_rows_never_gain_primary_training_frame(tmp_path, monkeypatch):
    recorded = []
    def parquet(table, path, **kwargs):
        recorded.extend(table)
        path.write_bytes(b"synthetic-legacy-parquet")
    pa = SimpleNamespace(Table=SimpleNamespace(from_pylist=lambda rows: rows))
    pq = SimpleNamespace(write_table=parquet)
    pa.parquet = pq
    monkeypatch.setitem(sys.modules, "pyarrow", pa)
    monkeypatch.setitem(sys.modules, "pyarrow.parquet", pq)
    archive = extracted("oanda_model_gap_live_signal_worker.py", "archive_feature_snapshot", {"finite": lambda value, default=0.0: float(value) if value is not None else default, "archive_observation_snapshot": observations.archive_observation_snapshot})
    raw = snapshot()
    path = archive(raw, tmp_path / "legacy", observation_archive_root=tmp_path / "observations")
    before = path.read_bytes()
    assert {row["input_timeframe"] for row in recorded} == {"M1", "UNIFIED_MTF"}
    assert all("primary_only" not in row for row in recorded)
    assert archive(raw, tmp_path / "legacy", observation_archive_root=tmp_path / "observations") == path
    assert path.read_bytes() == before
    assert len(recorded) == 2
    archive(raw, tmp_path / "other_legacy")
    assert len(list((tmp_path / "other_legacy" / "feature_observations_v1").glob("date=*/hour=*/obs_*.json.gz"))) == 1
