"""Primary-range selection, causality, validation and nonmutation regressions."""
from datetime import datetime, timezone
import csv
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

import oanda_rolling_technical_inputs_v1 as inputs
import oanda_rolling_technical_ranges_v1 as ranges

START = 1_800_000_000
PAIR = "EUR_USD"


def row(index=0, **changes):
    clock = datetime.fromtimestamp(START+60*index, timezone.utc).isoformat()
    result = {"time": clock, "datetime": clock, "instrument": PAIR, "granularity": "M1",
              "open": 1.2, "high": 1.23, "low": 1.18, "close": 1.21,
              "bid_close": 1.209, "ask_close": 1.211, "volume": 7}
    result.update(changes)
    return result


def csv_file(tmp_path, rows):
    path = tmp_path/(PAIR+"_M1.csv")
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    return path


def parquet_file(tmp_path, rows):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    path = tmp_path/(PAIR+"_M1.parquet")
    rows = [{k:v for k,v in r.items() if k not in ("instrument", "granularity")} for r in rows]
    pq.write_table(pa.Table.from_pylist(rows), path, row_group_size=3)
    return path


def recipe(csv_path=None, parquet_path=None, cutoff=START-60):
    return {"pair": PAIR, "canonical_path": str(csv_path), "reacquired_path": str(parquet_path),
            "cutoff_epoch": cutoff, "manifest_path": "fixture_manifest.json",
            "manifest_sha256": "fixture_metadata_only"}


def read(source, start=START, end=START+600, observed=START+3600, **kw):
    return ranges.read_range(PAIR, source, start, end, observed, **kw)


def test_primary_seam_uses_old_through_cutoff_and_canonical_after(tmp_path):
    old = parquet_file(tmp_path, [row(i) for i in range(8)])
    new = csv_file(tmp_path, [row(i, close=1.22, bid_close=1.219, ask_close=1.221) for i in range(10)])
    before = {p:p.read_bytes() for p in (old,new)}
    arrays, receipt = read(recipe(new, old, START+180), batch_rows=2)
    assert arrays["time"].tolist() == [START+60*i for i in range(10)]
    np.testing.assert_array_equal(arrays["close"], [1.21]*4+[1.22]*6)
    assert [s["retained_rows"] for s in receipt["sources"]] == [4,6]
    assert receipt["future_outcome_eligibility_filter"] is False
    assert receipt["original_first_observed_epoch"] is None
    assert receipt["observed_epoch"] == START+3600
    assert all(p.read_bytes()==b for p,b in before.items())


def test_post_seam_query_never_opens_unneeded_parquet(tmp_path):
    new = csv_file(tmp_path, [row(i) for i in range(10)])
    arrays, receipt = read(recipe(new, "missing.parquet"), START+120, START+300)
    assert arrays["time"].tolist() == [START+120, START+180, START+240]
    assert receipt["sources"][0]["selected"] is False
    assert receipt["requested_elapsed_minutes"] == 3


def test_pre_seam_query_does_not_require_canonical_csv(tmp_path):
    old = parquet_file(tmp_path, [row(i) for i in range(10)])
    arrays, receipt = read(recipe("missing.csv", old, START+600), START+120, START+300)
    assert arrays["time"].tolist() == [START+120, START+180, START+240]
    assert receipt["sources"][1]["selected"] is False


def test_empty_and_missing_target_do_not_filter_feature_origin(tmp_path):
    new = csv_file(tmp_path, [row(0),row(1),row(4)])
    arrays, receipt = read(recipe(new), START, START+600)
    assert arrays["time"].tolist() == [START,START+60,START+240]
    assert receipt["internal_gaps"] == 1
    assert receipt["internal_missing_minutes"] == 2
    assert receipt["unobserved_requested_minutes"] == 7
    empty, rec = read(recipe(new), START+600, START+660)
    assert all(len(x)==0 for x in empty.values())
    assert rec["first_start_epoch"] is None and rec["retained_rows"] == 0


def test_completion_flags_and_observation_cutoff_remain_separate(tmp_path):
    path = csv_file(tmp_path, [row(0, complete=True),row(1,complete=False,close="nan"),
                               row(2,complete=True),row(3,complete=True)])
    arrays, receipt = read(recipe(path), observed=START+150.25)
    assert arrays["time"].tolist() == [START]
    selected = receipt["sources"][1]
    assert selected["completion_counts"] == {"explicit_complete":1}
    assert selected["skipped_rows"]["explicit_incomplete"] == 1
    assert selected["skipped_rows"]["bar_end_after_observation"] == 2
    assert receipt["observed_epoch"] == START+150.25
    assert selected["read_completed_epoch"] > selected["read_started_epoch"]


@pytest.mark.parametrize("fmt", ["csv","parquet"])
def test_selected_full_ohlc_uses_existing_numeric_validation(tmp_path, fmt):
    values = [row(0),row(1,high=1.19),row(2)]
    path = csv_file(tmp_path,values) if fmt=="csv" else parquet_file(tmp_path,values)
    rec = recipe(path) if fmt=="csv" else recipe(None,path,START+600)
    with pytest.raises(ValueError,match="ohlc_geometry"):
        read(rec)


@pytest.mark.parametrize("fmt", ["csv","parquet"])
def test_outside_range_numeric_rows_are_not_claimed_validated(tmp_path,fmt):
    values = [row(0, high=1.19),row(1),row(2, high=1.19)]
    path = csv_file(tmp_path,values) if fmt=="csv" else parquet_file(tmp_path,values)
    rec = recipe(path) if fmt=="csv" else recipe(None,path,START+600)
    arrays, receipt = read(rec,START+60,START+120)
    assert arrays["time"].tolist() == [START+60]
    selected = next(s for s in receipt["sources"] if s["selected"])
    assert selected["skipped_rows"]["outside_primary_requested_range"] == 2
    assert "OHLC" in selected["nonselected_row_validation"]


@pytest.mark.parametrize("fmt", ["csv","parquet"])
def test_duplicate_clocks_detected_across_batches(tmp_path,fmt):
    values=[row(0),row(1),row(1),row(2)]
    path=csv_file(tmp_path,values) if fmt=="csv" else parquet_file(tmp_path,values)
    rec=recipe(path) if fmt=="csv" else recipe(None,path,START+600)
    with pytest.raises(ValueError,match="duplicate_or_unordered"):
        read(rec,batch_rows=2)


@pytest.mark.parametrize("change,error", [
    ({"time":"2027-01-15T08:00:00.000000001Z"},"minute_aligned"),
    ({"time":"2027-01-15T08:00:00"},"aware_original"),
    ({"datetime":"2027-01-15T08:01:00+00:00"},"missing_or_conflicting"),
])
def test_parquet_clock_screening_has_same_precision_and_alias_guards(tmp_path,change,error):
    path=parquet_file(tmp_path,[row(**change)])
    with pytest.raises(ValueError,match=error):
        read(recipe(None,path,START+600))


def test_batch_size_does_not_change_values_or_selected_hashes(tmp_path):
    path=csv_file(tmp_path,[row(i) for i in range(40)])
    a,ra=read(recipe(path),end=START+2400,batch_rows=3)
    b,rb=read(recipe(path),end=START+2400,batch_rows=13)
    for key in inputs.FIELDS: np.testing.assert_array_equal(a[key],b[key])
    assert ra["normalized_arrays_sha256"] == rb["normalized_arrays_sha256"]
    assert ra["sources"][1]["selected_original_rows_sha256"] == rb["sources"][1]["selected_original_rows_sha256"]


def test_adapter_and_range_numeric_parity(tmp_path):
    path=csv_file(tmp_path,[row(i) for i in [0,1,2,5,6,7]])
    expected,_=inputs.read_csv_tail(path,PAIR,START+3600,max_rows=50)
    arrays,_=read(recipe(path))
    for key in inputs.FIELDS: np.testing.assert_array_equal(arrays[key],expected[key])


def test_partial_final_csv_record_deferred(tmp_path):
    path=csv_file(tmp_path,[row(0),row(1)])
    with path.open("ab") as handle: handle.write(b"2027-01-15T")
    arrays,receipt=read(recipe(path))
    assert len(arrays["time"]) == 2
    assert receipt["sources"][1]["deferred_partial_tail_bytes"] == 11


def test_append_after_open_is_deferred_and_prefix_is_verified(tmp_path,monkeypatch):
    path=csv_file(tmp_path,[row(0),row(1)])
    original=inputs._normalize
    called=False
    def append_once(*args,**kwargs):
        nonlocal called
        if not called:
            called=True
            with path.open("a",newline="",encoding="utf-8") as handle:
                csv.DictWriter(handle,fieldnames=list(row())).writerow(row(2))
        return original(*args,**kwargs)
    monkeypatch.setattr(inputs,"_normalize",append_once)
    arrays,receipt=read(recipe(path))
    assert arrays["time"].tolist() == [START,START+60]
    assert receipt["sources"][1]["deferred_appended_bytes"] > 0


def test_rewrite_plus_append_cannot_evade_prefix_hash(tmp_path,monkeypatch):
    path=csv_file(tmp_path,[row(0),row(1)])
    original=inputs._normalize
    called=False
    def mutate_once(*args,**kwargs):
        nonlocal called
        if not called:
            called=True
            old=path.read_bytes()
            path.write_bytes(old.replace(b"1.21,",b"1.22,",1))
            with path.open("a",newline="",encoding="utf-8") as handle:
                csv.DictWriter(handle,fieldnames=list(row())).writerow(row(2))
        return original(*args,**kwargs)
    monkeypatch.setattr(inputs,"_normalize",mutate_once)
    with pytest.raises(ValueError,match="captured_prefix_changed"):
        read(recipe(path))


def test_replacement_during_read_is_rejected(tmp_path,monkeypatch):
    path=csv_file(tmp_path,[row(0),row(1)])
    original_normalize=inputs._normalize
    original_stat=Path.stat
    changed=False
    def observe(*args,**kwargs):
        nonlocal changed
        changed=True
        return original_normalize(*args,**kwargs)
    def replaced_stat(self,*args,**kwargs):
        value=original_stat(self,*args,**kwargs)
        if changed and self==path:
            return SimpleNamespace(st_dev=value.st_dev,st_ino=value.st_ino+1,
                                   st_size=value.st_size,st_mtime_ns=value.st_mtime_ns)
        return value
    monkeypatch.setattr(inputs,"_normalize",observe)
    monkeypatch.setattr(Path,"stat",replaced_stat)
    with pytest.raises(ValueError,match="replaced"):
        read(recipe(path))


def test_row_cap_across_primary_sources_has_no_silent_truncation(tmp_path):
    old=parquet_file(tmp_path,[row(i) for i in range(5)])
    new=csv_file(tmp_path,[row(i) for i in range(10)])
    with pytest.raises(ValueError,match="row_cap_exceeded"):
        read(recipe(new,old,START+180),max_rows=6)
    exact,_=read(recipe(new,old,START+180),max_rows=10)
    assert len(exact["time"])==10


def test_scope_and_filename_are_explicit(tmp_path):
    path=csv_file(tmp_path,[row()])
    with pytest.raises(ValueError,match="matching_primary"):
        read({**recipe(path),"pair":"GBP_USD"})
    with pytest.raises(ValueError,match="minute_epoch"):
        read(recipe(path),start=START+1)
    with pytest.raises(ValueError,match="180_days"):
        read(recipe(path),end=START+181*86400)
    with pytest.raises(ValueError,match="materialization_row_cap"):
        read(recipe(path),max_rows=ranges.MAX_ROWS+1)
    alternate=tmp_path/'unidentified.csv'
    alternate.write_bytes(path.read_bytes())
    with pytest.raises(ValueError,match="filename_mismatch"):
        read(recipe(alternate))


def test_changed_loaded_input_source_binding_rejected(tmp_path,monkeypatch):
    path=csv_file(tmp_path,[row()])
    monkeypatch.setattr(ranges,"_INPUTS_SOURCE_SHA256","incorrect")
    with pytest.raises(ValueError,match="source_changed_since_import"):
        read(recipe(path))
