"""Bounded historical replay/export contracts; no fitted models or network."""
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path

import numpy as np
import pytest

SPEC = importlib.util.spec_from_file_location("rolling_history_tool", Path(__file__).with_name("build_rolling_technical_history.py"))
history = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(history)


def candles(count=750):
    rng=np.random.default_rng(183)
    close=1.15+np.cumsum(rng.normal(0,.00004,count))
    return {"time":1_800_000_000+np.arange(count,dtype=np.int64)*60,
        "open":close-.00001,"high":close+.00005,"low":close-.00006,"close":close,
        "bid_close":close-.00003,"ask_close":close+.00003,
        "volume":rng.integers(2,100,count).astype(float)}


def test_realistic_prefix_chunk_bits_missingness_and_elapsed_formulas():
    data=candles();data["time"][400:]+=180
    features,report=history.validate_arrays(data,"EUR_USD",.0001,chunk_rows=137)
    assert report["status"]=="passed"
    assert report["one_shot_sha256"]==report["chunked_sha256"]
    assert report["gap_count"]==1
    assert report["elapsed_return_checks"]["15"]["exact_formula_equal"] is True
    assert report["one_vs_fifteen_different_value_rows"]>0
    assert np.isnan(features["m1__return_15_pips"][400:415]).all()


def test_exact_comparison_distinguishes_signed_zero_and_missing():
    assert not history._equal(np.array([0.]),np.array([-0.]))
    assert history._equal(np.array([np.nan,1.]),np.array([np.nan,1.]))
    assert not history._equal(np.array([np.nan]),np.array([0.]))
    assert not history._equal(np.array([np.inf]),np.array([np.nan]))


def test_empty_sample_cannot_claim_historical_parity_acceptance():
    with pytest.raises(ValueError,match="nonempty_completed"):
        history.validate_arrays(candles(0),"EUR_USD",.0001)


def test_aliases_are_registry_only_and_one_vs_fifteen_formula_bug_fails(monkeypatch):
    result=history.registry_checks()
    assert result["unique_names_and_aliases"]
    assert result["alias_columns_materialized"] is False
    original=history.kernel.compute_features
    def bad(*args,**kwargs):
        values=original(*args,**kwargs)
        values["m1__return_15_pips"]=values["m1__return_1_pips"].copy()
        return values
    monkeypatch.setattr(history.kernel,"compute_features",bad)
    with pytest.raises(ValueError,match="elapsed_return_formula_failed:15"):
        history.validate_arrays(candles(100),"EUR_USD",.0001)


def test_primary_precedence_does_not_export_borrowed_pre_cutoff_history():
    data=candles(50);cutoff=int(data["time"][20])
    old=history.primary_mask(data,"reacquired_prefix",cutoff)
    new=history.primary_mask(data,"canonical_prefix",cutoff)
    assert old.sum()==21 and new.sum()==29 and not (old&new).any()
    features=history.kernel.compute_features(data,"EUR_USD",.0001)
    exported=history.export_features_with_primary_support(features,data,"canonical_prefix",cutoff)
    assert np.isnan(exported["m1__return_15_pips"][:36]).all()
    assert np.isfinite(exported["m1__return_15_pips"][36:]).all()
    assert np.isfinite(exported["m1__historical_spread_pips"][21:]).all()


def test_default_mode_is_validation_and_bounds_are_explicit():
    args=history.parse_args(["--manifest","manifest.json","--output","new-output"])
    assert args.export is False and args.max_rows_per_source==4096
    assert args.max_export_rows==100000
    with pytest.raises(SystemExit):
        history.parse_args(["--manifest","x","--output","y","--export","--validate-only"])
    args.max_rows_per_source=1_000_000
    with pytest.raises(ValueError,match="bounded_source_row"):
        history.run(args)


def test_output_never_overwrites_existing_directory(tmp_path):
    with pytest.raises(ValueError,match="new_unlinked"):
        history._new_output(tmp_path)
    created=history._new_output(tmp_path/"new")
    assert (created/"pairs").is_dir()


def _fixture_source(tmp_path):
    pa=pytest.importorskip("pyarrow");pq=pytest.importorskip("pyarrow.parquet")
    import csv
    rows=[]
    for i in range(32):
        at=1_800_000_000+i*60;c=1.15+i*.00001
        rows.append({"time":datetime.fromtimestamp(at,timezone.utc).isoformat(),
            "instrument":"EUR_USD","granularity":"M1","open":c,"high":c+.00005,
            "low":c-.00005,"close":c,"bid_close":c-.00002,"ask_close":c+.00002,"volume":10})
    csv_path=tmp_path/"source.csv"
    with csv_path.open("w",newline="",encoding="utf-8") as h:
        writer=csv.DictWriter(h,fieldnames=list(rows[0]));writer.writeheader();writer.writerows(rows)
    parquet=tmp_path/"source.parquet";pq.write_table(pa.Table.from_pylist(rows),parquet)
    metadata=tmp_path/"metadata.json";metadata.write_text(json.dumps({"pairs":{"EUR_USD":{"pip_size":.0001}}}))
    recipe={"pair":"EUR_USD","canonical_path":str(csv_path),"reacquired_path":str(parquet),
        "cutoff_epoch":1_800_000_000+15*60,"sealed_last_utc":datetime.fromtimestamp(1_800_000_000+31*60,timezone.utc).isoformat(),
        "sealed_raw_rows":32,"manifest_sha256":"fixture"}
    return metadata,recipe


def test_bounded_export_has_no_duplicate_sample_rows_or_original_raw_columns(tmp_path,monkeypatch):
    metadata,recipe=_fixture_source(tmp_path)
    monkeypatch.setattr(history.inputs,"discover_archive_sources",lambda _: {"EUR_USD":recipe})
    monkeypatch.setattr(history.time,"time",lambda: 1_800_100_000.)
    args=history.parse_args(["--manifest",str(tmp_path/"unused.json"),"--output",str(tmp_path/"out"),
        "--pair-metadata",str(metadata),"--export","--max-rows-per-source","32","--max-export-rows","20","--minimum-free-mib","1"])
    report=history.run(args)
    assert report["status"]=="passed"
    assert report["selected_rows_validated"]==96
    assert report["primary_sample_rows"]==32
    assert report["exported_rows"]==20
    assert report["full_archive_features_built"] is False
    pq=pytest.importorskip("pyarrow.parquet")
    times=[]
    for path in args.output.glob("*_features.parquet"):
        table=pq.read_table(path);times.extend(table["bar_start_epoch"].to_pylist())
        assert "close" not in table.column_names
        assert "return_lag_00_pips" not in table.column_names
    assert len(times)==len(set(times))==20
