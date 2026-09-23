from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone

import pytest

from oanda_feature_move_mapping_v1 import build_feature_move_map, read_feature_move_map, read_feature_move_maps

START = datetime(2026, 9, 7, 12, tzinfo=timezone.utc)


def frame(index, *, price=None, rsi=None):
    now = START + timedelta(minutes=index)
    price = 1.1 + index * .0001 if price is None else price
    values = {"rsi14": 40 + index % 3 if rsi is None else rsi,
              "atr14_pips": 2 + index % 4, "momentum_pips": index - 20,
              "breakout": index >= 20, "flat": 1., "missing": None}
    group = {"input_timeframe": "M1", "observed_utc": now.isoformat(),
             "bar_complete_utc": now.isoformat(), "clock_basis": "producer_capture",
             "values": values, "value_states": {key: "missing" if value is None else "observed" for key, value in values.items()},
             "feature_ids": {key: f"EUR_USD/M1/{key}" for key in values}}
    return {"schema_version": "feature_observation_frame_v1", "snapshot_id": f"s{index}",
            "generated_utc": now.isoformat(), "generated_epoch": now.timestamp(),
            "source_schema_id": "test-schema-v1", "source_payload_sha256": f"test-{index}",
            "coverage": {"quote_exclusions": []}, "instruments": {
                "EUR_USD": {"quote": {"bid": price-.00005, "ask": price+.00005, "time": now.isoformat()},
                            "groups": {"timeframe:M1": group}}}}


def series():
    return [frame(i, rsi=70 if i == 20 else None) for i in range(21)]


def build(frames, **kwargs):
    return build_feature_move_map(frames, as_of_utc=(START+timedelta(minutes=20)).isoformat(), **kwargs)


def by_name(result, name):
    return next(row for row in result["feature_changes"] if row["feature_name"] == name)


def test_two_way_mapping_preserves_real_units_price_move_and_quiet_features():
    result = build(series())
    rsi = by_name(result, "rsi14")
    assert rsi["before"] == 40
    assert rsi["after"] == 70
    assert rsi["change"] == 30
    assert rsi["change_pct"] is None
    assert rsi["units"] == "points"
    assert rsi["actual_window_sec"] == 300
    assert rsi["unusual_percentile"] == 100
    assert rsi["history_count"] == 11
    assert rsi["pair_move_pct"] == pytest.approx(100*((1.102)/(1.1015)-1))
    assert by_name(result, "flat")["ranking_reason"] == "constant_past_changes"
    assert result["summary"]["unchanged"] >= 1
    assert result["can_place_orders"] is False


def test_forward_evaluation_receives_controls_and_missing_rows_before_display_limit():
    result = build(series(), top_limit=1, include_all_comparisons=True)
    assert len(result["feature_changes"]) == 1
    assert len(result["all_feature_changes"]) == result["summary"]["total_feature_comparisons"] == 6
    names = {row["feature_name"]: row for row in result["all_feature_changes"]}
    assert names["missing"]["status"] == "unavailable"
    assert names["flat"]["change"] == 0
    assert "all_feature_changes" not in build(series(), top_limit=1)


def test_full_comparison_reader_keeps_empty_archive_explicit(tmp_path):
    now = (START + timedelta(minutes=20)).isoformat()
    result = read_feature_move_map(tmp_path / "missing", as_of_utc=now, include_all_comparisons=True)
    assert result["all_feature_changes"] == []
    assert not (tmp_path / "missing").exists()
    with pytest.raises(ValueError, match="boolean_full_comparison"):
        read_feature_move_map(tmp_path, as_of_utc=now, include_all_comparisons="yes")


def test_large_feature_change_with_no_price_move_is_retained():
    frames = [frame(i, price=1.1, rsi=70 if i == 20 else None) for i in range(21)]
    result = build(frames)
    assert by_name(result, "rsi14")["change"] == 30
    assert by_name(result, "rsi14")["pair_move_pct"] == 0
    assert result["summary"]["pairs_with_no_price_change"] == 1


def test_unusual_percentile_excludes_current_window_and_future_frames():
    frames = series()
    expected = build(frames)
    frames += [frame(21, rsi=1_000_000)]
    assert by_name(build(frames), "rsi14") == by_name(expected, "rsi14")
    assert build(frames)["summary"]["rejected_frames"] == {"invalid_or_future_frame_clock": 1}


def test_aliases_are_not_double_counted_and_conflicts_are_visible():
    frames = series()
    for item in frames:
        pair = item["instruments"]["EUR_USD"]
        pair["groups"]["primary"] = deepcopy(pair["groups"]["timeframe:M1"])
    result = build(frames)
    assert result["summary"]["total_feature_comparisons"] == 6
    frames[-1]["instruments"]["EUR_USD"]["groups"]["primary"]["values"]["rsi14"] = 30
    assert by_name(build(frames), "rsi14")["reason"] == "feature_conflicting_alias"


def test_third_later_alias_cannot_erase_a_conflicting_value():
    frames = series()
    groups = frames[-1]["instruments"]["EUR_USD"]["groups"]
    groups["a:first"] = deepcopy(groups["timeframe:M1"])
    groups["a:first"]["observed_utc"] = (START + timedelta(minutes=20, seconds=-1)).isoformat()
    groups["b:conflict"] = deepcopy(groups["a:first"])
    groups["b:conflict"]["values"]["rsi14"] = 30
    row = by_name(build(frames), "rsi14")
    assert row["reason"] == "feature_conflicting_alias"
    assert row["change"] is None
    assert set(row["aliases"]) == {"b:conflict", "timeframe:M1"}


def test_nonfinite_missing_and_defaults_cannot_become_measured_zero_changes():
    frames = series()
    group = frames[-1]["instruments"]["EUR_USD"]["groups"]["timeframe:M1"]
    group["value_states"]["flat"] = "default"
    result = build(frames)
    assert by_name(result, "missing")["status"] == "unavailable"
    assert by_name(result, "missing")["change"] is None
    assert by_name(result, "flat")["reason"] == "feature_default"


def test_irregular_snapshots_report_actual_elapsed_time():
    frames = series()
    item = frames[15]
    stamp = START + timedelta(minutes=15, seconds=-20)
    item["generated_utc"] = stamp.isoformat()
    item["generated_epoch"] = stamp.timestamp()
    item["instruments"]["EUR_USD"]["quote"]["time"] = stamp.isoformat()
    group = item["instruments"]["EUR_USD"]["groups"]["timeframe:M1"]
    group["observed_utc"] = group["bar_complete_utc"] = stamp.isoformat()
    result = build(frames)
    assert result["pairs"][0]["actual_window_sec"] == 320
    assert by_name(result, "rsi14")["actual_window_sec"] == 320
    assert result["window_sec"] == 300


@pytest.mark.parametrize("component", ["quote", "feature"])
def test_baseline_children_cannot_postdate_their_own_snapshot(component):
    frames = series()
    snapshot = frames[15]
    stamp = (START + timedelta(minutes=15, seconds=-10)).isoformat()
    snapshot["generated_utc"] = stamp
    pair = snapshot["instruments"]["EUR_USD"]
    group = pair["groups"]["timeframe:M1"]
    if component == "quote":
        group["observed_utc"] = group["bar_complete_utc"] = stamp
        row = build(frames)["pairs"][0]
    else:
        pair["quote"]["time"] = stamp
        row = by_name(build(frames), "rsi14")
    assert row["status"] == "unavailable"
    assert row["reason"] == component + "_after_snapshot"


@pytest.mark.parametrize("component", ["quote", "feature"])
def test_baseline_snapshot_ceiling_does_not_loosen_window_freshness(component):
    frames = series()
    snapshot = frames[15]
    stamp = (START + timedelta(minutes=15, seconds=-70)).isoformat()
    old = (START + timedelta(minutes=15, seconds=-130)).isoformat()
    snapshot["generated_utc"] = stamp
    pair = snapshot["instruments"]["EUR_USD"]
    group = pair["groups"]["timeframe:M1"]
    group["observed_utc"] = group["bar_complete_utc"] = stamp
    pair["quote"]["time"] = stamp
    if component == "quote":
        pair["quote"]["time"] = old
    else:
        group["observed_utc"] = group["bar_complete_utc"] = old
    frames.pop(14)  # The selected baseline is now the delayed snapshot.
    result = build(frames)
    row = result["pairs"][0] if component == "quote" else by_name(result, "rsi14")
    assert row["status"] == "unavailable"
    assert row["reason"] == ("quote_stale" if component == "quote" else "feature_observation_stale")


def test_historical_reference_features_obey_their_own_snapshot_ceiling():
    frames = series()
    snapshot = frames[5]
    stamp = (START + timedelta(minutes=5, seconds=-10)).isoformat()
    snapshot["generated_utc"] = stamp
    group = snapshot["instruments"]["EUR_USD"]["groups"]["timeframe:M1"]
    coherent = deepcopy(frames)
    good = coherent[5]["instruments"]["EUR_USD"]["groups"]["timeframe:M1"]
    good["observed_utc"] = good["bar_complete_utc"] = stamp
    invalid_row = by_name(build(frames), "rsi14")
    valid_row = by_name(build(coherent), "rsi14")
    assert invalid_row["status"] == "available"
    assert invalid_row["history_count"] == valid_row["history_count"] - 1
    assert invalid_row["change"] == valid_row["change"]


def test_missing_baseline_does_not_erase_price_or_feature_rows():
    result = build([frame(20)])
    assert result["pairs"][0]["reason"] == "baseline_snapshot_missing"
    assert by_name(result, "rsi14")["after"] == 42
    assert by_name(result, "rsi14")["reason"] == "baseline_snapshot_missing"
    assert result["status"] == "waiting_for_comparable_features"


@pytest.mark.parametrize("fault,expected", [
    ("future", "feature_from_future"), ("stale", "feature_observation_stale"),
    ("unknown", "feature_clock_unknown"), ("incomplete", "bar_incomplete_at_observation"),
])
def test_group_clocks_override_fresh_quote_and_generation(fault, expected):
    frames = series()
    group = frames[-1]["instruments"]["EUR_USD"]["groups"]["timeframe:M1"]
    if fault == "future":
        group["observed_utc"] = (START+timedelta(minutes=21)).isoformat()
    elif fault == "stale":
        group["observed_utc"] = (START+timedelta(minutes=18)).isoformat()
    elif fault == "unknown":
        group["clock_basis"] = "unknown"
    else:
        group["bar_complete_utc"] = (START+timedelta(minutes=21)).isoformat()
    assert by_name(build(frames), "rsi14")["reason"] == expected
    assert build(frames)["pairs"][0]["status"] == "available"


def test_stale_and_future_quotes_are_not_current_pair_moves():
    frames = series()
    frames[-1]["instruments"]["EUR_USD"]["quote"]["time"] = (START+timedelta(minutes=21)).isoformat()
    assert build(frames)["pairs"][0]["reason"] == "quote_from_future"
    frames[-1]["instruments"]["EUR_USD"]["quote"]["time"] = START.isoformat()
    assert build(frames)["pairs"][0]["reason"] == "quote_stale"


def test_signed_near_zero_features_and_boolean_transitions_have_no_percentage():
    result = build(series())
    assert by_name(result, "momentum_pips")["change"] == 5
    assert by_name(result, "momentum_pips")["change_pct"] is None
    assert by_name(result, "breakout")["status"] == "state_transition"
    assert by_name(result, "breakout")["change_pct"] is None


def test_definitions_cannot_be_mixed_and_snapshot_collisions_fail():
    frames = series()
    frames[-1]["source_schema_id"] = "new-definition"
    assert by_name(build(frames), "rsi14")["reason"] == "feature_schema_changed"
    frames = series()
    collision = deepcopy(frames[-1])
    collision["instruments"]["EUR_USD"]["quote"]["bid"] += .00001
    with pytest.raises(ValueError, match="conflicting_snapshot_identity"):
        build(frames + [collision])


def test_price_row_retained_for_current_excluded_pair():
    frames = series()
    frames[-1]["instruments"] = {}
    frames[-1]["coverage"]["quote_exclusions"] = [{"instrument": "EUR_USD", "reason": "quote_stale"}]
    result = build(frames)
    assert result["pairs"][0]["instrument"] == "EUR_USD"
    assert result["pairs"][0]["move_pct"] is None
    assert result["summary"]["coverage"]["quote_exclusions"]


def test_read_missing_archive_does_not_create_paths(tmp_path):
    target = tmp_path / "not-created"
    result = read_feature_move_map(target, as_of_utc=(START+timedelta(minutes=20)).isoformat())
    assert result["status"] == "waiting_for_observations"
    assert result["archive"]["files_read"] == 0
    assert not target.exists()


def test_stale_snapshot_is_reported_even_when_latest_id_is_present():
    result = build(series()[:-2])
    assert result["status"] == "stale_observations"
    assert result["feature_changes"] == []


def test_old_quote_dependent_values_cannot_use_a_fresh_final_quote():
    frames = series()
    for item in frames:
        group = item["instruments"]["EUR_USD"]["groups"]["timeframe:M1"]
        group["values"]["depth_imbalance"] = .2
        group["value_states"]["depth_imbalance"] = "observed"
        group["feature_ids"]["depth_imbalance"] = "EUR_USD/QUOTE/depth_imbalance"
        group["component_clocks"] = {"quote_feature_source_utc": item["generated_utc"]}
    frames[-1]["instruments"]["EUR_USD"]["groups"]["timeframe:M1"]["component_clocks"]["quote_feature_source_utc"] = START.isoformat()
    assert by_name(build(frames), "depth_imbalance")["reason"] == "feature_source_quote_stale"
    assert by_name(build(frames), "rsi14")["status"] == "available"


def original_snapshot(index):
    from oanda_feature_observations_v1 import capture_feature_group
    normalized = frame(index, rsi=70 if index == 20 else None)
    pair = normalized["instruments"]["EUR_USD"]
    values = dict(pair["groups"]["timeframe:M1"]["values"])
    values["candle_time"] = (START+timedelta(minutes=index-1)).isoformat()
    clock = {"observed_utc": normalized["generated_utc"], "clock_basis": "producer_capture"}
    return {"schema_version": 1, "snapshot_id": normalized["snapshot_id"],
            "generated_utc": normalized["generated_utc"], "generated_epoch": normalized["generated_epoch"],
            "coverage": {"expected_feature_instruments": ["EUR_USD"], "quote_exclusions": []},
            "observation_source": {"producer_id": "synthetic-test", "producer_contract_id": "test-v1"},
            "instruments": {}, "observation_inputs": {"instruments": {
                "EUR_USD": {"quote": pair["quote"], "groups": {
                    "primary": capture_feature_group(values, input_timeframe="M1", clock=clock)}}}}}


def test_real_archive_to_mapper_recreates_values_and_rejects_frame_tampering(tmp_path):
    import gzip
    import json
    from oanda_feature_observations_v1 import archive_observation_snapshot
    for index in range(21):
        last = archive_observation_snapshot(original_snapshot(index), tmp_path)
    result = read_feature_move_map(tmp_path, as_of_utc=(START+timedelta(minutes=20)).isoformat())
    assert result["archive"]["files_read"] == 21
    assert result["archive"]["errors"] == []
    assert by_name(result, "rsi14")["before"] == 40
    assert by_name(result, "rsi14")["after"] == 70
    assert by_name(result, "rsi14")["unusual_percentile"] == 100
    with gzip.open(last, "rt", encoding="utf-8") as handle:
        envelope = json.load(handle)
    envelope["frame"]["instruments"]["EUR_USD"]["groups"]["primary"]["values"]["rsi14"] = 99
    with gzip.open(last, "wt", encoding="utf-8") as handle:
        json.dump(envelope, handle)
    tampered = read_feature_move_map(tmp_path, as_of_utc=(START+timedelta(minutes=20)).isoformat())
    assert "ValueError:frame_recreation_mismatch" in tampered["archive"]["errors"]
    assert tampered["archive"]["files_read"] == 20


def test_archive_reader_checks_original_payload_hash(tmp_path):
    import gzip
    import json
    from oanda_feature_observations_v1 import archive_observation_snapshot
    path = archive_observation_snapshot(original_snapshot(20), tmp_path)
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        envelope = json.load(handle)
    envelope["original_snapshot"]["snapshot_id"] = "tampered"
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        json.dump(envelope, handle)
    result = read_feature_move_map(tmp_path, as_of_utc=(START+timedelta(minutes=20)).isoformat())
    assert result["archive"]["errors"] == ["ValueError:snapshot_hash_mismatch"]


def test_multiple_windows_share_one_archive_read_and_retain_independent_results(tmp_path, monkeypatch):
    import oanda_feature_move_mapping_v1 as mapping
    from oanda_feature_observations_v1 import archive_observation_snapshot
    for index in range(21):
        archive_observation_snapshot(original_snapshot(index), tmp_path)
    original_open = mapping.gzip.open
    reads = []

    def count_open(path, *args, **kwargs):
        reads.append(str(path))
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(mapping.gzip, "open", count_open)
    results = mapping.read_feature_move_maps(
        tmp_path, as_of_utc=(START + timedelta(minutes=20)).isoformat(),
        include_all_comparisons=True)
    assert len(reads) == len(set(reads)) == 21
    assert set(results) == {300, 900, 3600}
    assert by_name(results[300], "rsi14")["status"] == "available"
    assert by_name(results[900], "rsi14")["status"] == "available"
    assert by_name(results[3600], "rsi14")["reason"] == "baseline_snapshot_missing"
    assert all(len(result["all_feature_changes"]) >= 6 for result in results.values())
    assert results[300]["archive"]["errors"] is not results[900]["archive"]["errors"]


def test_reference_only_reader_keeps_required_endpoints_and_reports_sampling(tmp_path):
    import os
    from oanda_feature_observations_v1 import archive_observation_snapshot
    for index in range(81):
        path = archive_observation_snapshot(original_snapshot(index), tmp_path)
        stamp = (START + timedelta(minutes=index)).timestamp()
        os.utime(path, (stamp, stamp))
    now = (START + timedelta(minutes=80)).isoformat()
    results = read_feature_move_maps(
        tmp_path, as_of_utc=now, window_secs=(300,),
        include_all_comparisons=True, reference_only=True)
    result = results[300]
    assert result["archive"]["bounded_sample"] is True
    assert result["archive"]["selection_stop"] == "required_reference_candidates_only"
    assert result["archive"]["files_read"] < result["archive"]["files_considered"]
    assert by_name(result, "rsi14")["actual_window_sec"] == 300


def test_shared_window_preparation_matches_public_build_and_has_no_alias_leak(tmp_path, monkeypatch):
    import oanda_feature_move_mapping_v1 as mapping
    from oanda_feature_observations_v1 import archive_observation_snapshot, build_observation_frame
    originals = [original_snapshot(index) for index in range(21)]
    frames = [build_observation_frame(value) for value in originals]
    for value in originals:
        archive_observation_snapshot(value, tmp_path)
    prepare = mapping._prepare_frames
    calls = []

    def counted(frames, now, **kwargs):
        calls.append(len(frames))
        return prepare(frames, now, **kwargs)

    monkeypatch.setattr(mapping, "_prepare_frames", counted)
    instant = (START + timedelta(minutes=20)).isoformat()
    shared = mapping.read_feature_move_maps(tmp_path, as_of_utc=instant, include_all_comparisons=True)
    assert calls == [21]
    for window, value in shared.items():
        direct = mapping.build_feature_move_map(frames, as_of_utc=instant, window_sec=window,
                                                include_all_comparisons=True)
        assert {key: item for key, item in value.items() if key != "archive"} == direct
    by_name(shared[300], "rsi14")["aliases"].append("caller-change")
    assert "caller-change" not in by_name(shared[900], "rsi14")["aliases"]


def test_archive_read_budget_counts_failed_expansion(tmp_path, monkeypatch):
    import oanda_feature_move_mapping_v1 as mapping
    from oanda_feature_observations_v1 import archive_observation_snapshot
    archive_observation_snapshot(original_snapshot(20), tmp_path)
    monkeypatch.setattr(mapping, "MAX_ARCHIVE_SCAN_BYTES", 100)
    result = read_feature_move_map(tmp_path, as_of_utc=(START+timedelta(minutes=20)).isoformat())
    assert result["archive"]["bounded_sample"]
    assert result["archive"]["bytes_read"] == 101
    assert result["archive"]["files_read"] == 0


def test_streamed_lossless_history_reaches_all_three_reference_windows(tmp_path, monkeypatch):
    import os
    import oanda_feature_move_mapping_v1 as mapping
    from oanda_feature_observations_v1 import archive_observation_snapshot, build_observation_frame
    # Expanded original/normalized envelopes exceed the retained-state cap.
    # Every window still gets its full historical baseline without changing
    # feature units, missingness, clocks or descriptive-reference arithmetic.
    originals=[]
    for index in range(0,481,5):
        raw=original_snapshot(index);raw['unranked_retained_original']='x'*16384
        path=archive_observation_snapshot(raw,tmp_path)
        stamp=(START+timedelta(minutes=index)).timestamp();os.utime(path,(stamp,stamp))
        originals.append(raw)
    monkeypatch.setattr(mapping,'MAX_READ_BYTES',512*1024)
    monkeypatch.setattr(mapping,'MAX_ARCHIVE_SCAN_BYTES',16*1024*1024)
    monkeypatch.setattr(mapping,'REFERENCE_SCAN_TARGET_BYTES',128*1024)
    now=(START+timedelta(minutes=480)).isoformat()
    shared=mapping.read_feature_move_maps(tmp_path,as_of_utc=now,include_all_comparisons=True)
    frames=[build_observation_frame(raw) for raw in originals]
    for window,result in shared.items():
        assert 16<=result['archive']['files_read']<97
        assert result['archive']['bytes_read']>mapping.REFERENCE_SCAN_TARGET_BYTES
        assert result['archive']['retained_projection_bytes']<=mapping.MAX_READ_BYTES
        assert result['archive']['reader_version']==mapping.READER_VERSION
        assert not result['archive']['errors']
        assert by_name(result,'rsi14')['history_count']>=5
        identities={s['snapshot_id'] for s in result['source_snapshots']}
        selected=[f for f in frames if f['snapshot_id'] in identities]
        direct=mapping.build_feature_move_map(selected,as_of_utc=now,window_sec=window,include_all_comparisons=True)
        assert result['all_feature_changes']==direct['all_feature_changes']
        assert result['pairs']==direct['pairs']


def test_projection_memory_limit_remains_explicit(tmp_path,monkeypatch):
    import oanda_feature_move_mapping_v1 as mapping
    from oanda_feature_observations_v1 import archive_observation_snapshot
    archive_observation_snapshot(original_snapshot(20),tmp_path)
    monkeypatch.setattr(mapping,'MAX_READ_BYTES',10)
    result=read_feature_move_map(tmp_path,as_of_utc=(START+timedelta(minutes=20)).isoformat())
    assert result['archive']['errors']==['retained_projection_or_value_bound']
    assert result['archive']['bounded_sample']
    assert result['archive']['files_read']==0


def test_dense_eight_hour_archive_keeps_exact_endpoints_before_file_count_sampling(tmp_path,monkeypatch):
    import os
    import oanda_feature_move_mapping_v1 as mapping
    from oanda_feature_observations_v1 import archive_observation_snapshot
    for index in range(481):
        path=archive_observation_snapshot(original_snapshot(index),tmp_path)
        stamp=(START+timedelta(minutes=index)).timestamp();os.utime(path,(stamp,stamp))
    monkeypatch.setattr(mapping,'REFERENCE_SCAN_TARGET_BYTES',1)
    results=mapping.read_feature_move_maps(tmp_path,as_of_utc=(START+timedelta(minutes=480)).isoformat())
    for result in results.values():
        assert result['archive']['files_considered']==481
        assert result['archive']['files_read']<32
        assert result['archive']['bounded_sample']
        assert by_name(result,'rsi14')['actual_window_sec']==result['window_sec']
        assert by_name(result,'rsi14')['history_count']>=5


def test_lossless_projection_never_collapses_conflicting_aliases_or_stale_clocks():
    import oanda_feature_move_mapping_v1 as mapping
    frames=series()
    for value in frames:
        groups=value['instruments']['EUR_USD']['groups']
        groups['alias']=deepcopy(groups['timeframe:M1'])
    frames[-1]['instruments']['EUR_USD']['groups']['alias']['values']['rsi14']=99
    frames[-1]['instruments']['EUR_USD']['groups']['timeframe:M1']['bar_complete_utc']=START.isoformat()
    now=(START+timedelta(minutes=20)).timestamp()
    projections=[];digests={}
    import hashlib
    for value in frames:
        projected,_=mapping._project_verified_frame(value);projections.append(projected)
        digests[id(projected)]=hashlib.sha256(mapping._canonical(value)).hexdigest()
    prepared=mapping._prepare_frames(projections,now,recreated_digests=digests)
    compact=mapping._build_feature_move_map(projections,as_of_utc=(START+timedelta(minutes=20)).isoformat(),prepared=prepared)
    assert compact==build(frames)
    assert by_name(compact,'rsi14')['reason']=='feature_conflicting_alias'


@pytest.mark.parametrize('case,reason',[
    ('valid',None),('unverified','event_publication_receipt_unknown'),
    ('missing_expiry','event_publication_receipt_unknown'),
    ('future_publication','event_publication_clock_invalid'),
    ('wrong_completion','event_publication_clock_invalid'),
    ('expired','event_evidence_expired'),('invalid_expiry','event_publication_clock_invalid')])
def test_event_features_keep_original_receipt_and_expiry(case,reason):
    import oanda_feature_move_mapping_v1 as mapping
    now=(START+timedelta(minutes=20)).timestamp()
    publication=now-1800;expires=now+60
    iso=lambda n:datetime.fromtimestamp(n,timezone.utc).isoformat()
    item={'feature_name':'supervised_probability_up','group':'model','input_timeframe':'EVENT',
          'value_state':'observed','observed_utc':iso(now),'clock_basis':'verified_model_publication',
          'bar_complete_utc':iso(publication),'component_clocks':{'source_publication_utc':iso(publication),
              'valid_until_utc':iso(expires),'publication_receipt_verified':True}}
    if case=='unverified':item['component_clocks']['publication_receipt_verified']=1
    if case=='missing_expiry':item['component_clocks'].pop('valid_until_utc')
    if case=='future_publication':item['component_clocks']['source_publication_utc']=iso(now+1)
    if case=='wrong_completion':item['bar_complete_utc']=iso(publication-1)
    if case=='expired':item['component_clocks']['valid_until_utc']=iso(now)
    if case=='invalid_expiry':item['component_clocks']['valid_until_utc']=iso(publication)
    assert mapping._entry_reason(item,now,75)==reason
    if case=='valid':
        # A fresh re-read cannot extend the original publication expiry.
        item['observed_utc']=iso(expires)
        assert mapping._entry_reason(item,expires,75)=='event_evidence_expired'


def test_higher_timeframe_calendar_opt_in_preserves_original_bar_clock():
    import oanda_feature_move_mapping_v1 as mapping
    completed=datetime(2026,9,11,20,tzinfo=timezone.utc).timestamp()
    observed=datetime(2026,9,13,23,tzinfo=timezone.utc).timestamp()
    iso=lambda n:datetime.fromtimestamp(n,timezone.utc).isoformat()
    item={'feature_name':'rsi14','group':'timeframe:H4','input_timeframe':'H4','value_state':'observed',
          'observed_utc':iso(observed),'bar_complete_utc':iso(completed),'clock_basis':'complete_native_candles',
          'component_clocks':{}}
    assert mapping._entry_reason(item,observed,75)=='feature_bar_stale'
    item['component_clocks']['candle_calendar_contract']='fx_weekend_ny_friday_1700_sunday_1700_v1'
    assert mapping._entry_reason(item,observed,75) is None
    assert item['bar_complete_utc']==iso(completed)
    item['input_timeframe']='M1'
    assert mapping._entry_reason(item,observed,75)=='feature_calendar_unknown_or_unsupported'
    item['input_timeframe']='H4';item['component_clocks']['candle_calendar_contract']='unknown'
    assert mapping._entry_reason(item,observed,75)=='feature_calendar_unknown_or_unsupported'


def test_shared_currency_mapping_preserves_base_quote_orientation():
    frames = series()
    for index, item in enumerate(frames):
        template = deepcopy(item["instruments"]["EUR_USD"])
        template["groups"] = {}
        template["quote"]["bid"] = 1.2 - index*.0001
        template["quote"]["ask"] = 1.2001 - index*.0001
        item["instruments"]["USD_CAD"] = template
    result = build(frames)
    related = next(row for row in by_name(result, "rsi14")["related_pair_moves"] if row["instrument"] == "USD_CAD")
    assert related["shared_currencies"] == ["USD"]
    assert related["base_currency"] == "USD"
    assert related["move_pct"] < 0


def test_pair_filter_is_applied_before_display_limit():
    frames = series()
    for index, item in enumerate(frames):
        copied = deepcopy(item["instruments"]["EUR_USD"])
        group = copied["groups"]["timeframe:M1"]
        group["feature_ids"] = {key: "USD_CAD/M1/"+key for key in group["values"]}
        item["instruments"]["USD_CAD"] = copied
    result = build(frames, instrument="USD_CAD", top_limit=1)
    assert result["instrument_filter"] == "USD_CAD"
    assert len(result["feature_changes"]) == 1
    assert result["feature_changes"][0]["instrument"] == "USD_CAD"
    assert len(result["pairs"]) == 2


def test_unknown_timeframe_cannot_skip_bar_freshness():
    frames = series()
    frames[-1]["instruments"]["EUR_USD"]["groups"]["timeframe:M1"]["input_timeframe"] = "UNKNOWN"
    assert by_name(build(frames), "rsi14")["reason"] == "feature_timeframe_unknown"


def test_component_clock_overrides_enclosing_timeframe_and_alias():
    frames = series()
    for item in frames:
        group = item["instruments"]["EUR_USD"]["groups"]["timeframe:M1"]
        group["values"]["m5_r1_pips"] = 1.
        group["value_states"]["m5_r1_pips"] = "observed"
        group["feature_ids"]["m5_r1_pips"] = "EUR_USD/M5/r1_pips"
        group["feature_clocks"] = {"m5_r1_pips": {
            "input_timeframe": "M5", "observed_utc": item["generated_utc"],
            "bar_complete_utc": item["generated_utc"], "clock_basis": "component_capture"}}
    result = build(frames)
    assert by_name(result, "m5_r1_pips")["input_timeframe"] == "M5"
    assert by_name(result, "m5_r1_pips")["status"] == "available"
    frames[-1]["instruments"]["EUR_USD"]["groups"]["timeframe:M1"]["feature_clocks"] = {}
    assert by_name(build(frames), "m5_r1_pips")["reason"] == "feature_clock_unknown"


def test_book_clock_is_not_replaced_by_current_quote_time():
    frames = series()
    for item in frames:
        group = item["instruments"]["EUR_USD"]["groups"]["timeframe:M1"]
        group["values"]["order_book_imbalance"] = .2
        group["value_states"]["order_book_imbalance"] = "observed"
        group["feature_ids"]["order_book_imbalance"] = "EUR_USD/BOOK/imbalance"
        group["feature_clocks"] = {"order_book_imbalance": {
            "input_timeframe": "BOOK", "observed_utc": item["generated_utc"],
            "clock_basis": "source_book_time", "bar_complete_utc": item["generated_utc"]}}
    assert by_name(build(frames), "order_book_imbalance")["status"] == "available"
    override = frames[-1]["instruments"]["EUR_USD"]["groups"]["timeframe:M1"]["feature_clocks"]["order_book_imbalance"]
    override["observed_utc"] = START.isoformat()
    assert by_name(build(frames), "order_book_imbalance")["reason"] == "feature_observation_stale"


@pytest.mark.parametrize("value", [None, [], 12, "bad"])
def test_malformed_archive_envelopes_return_an_explicit_read_error(tmp_path, value):
    import gzip
    import json
    folder = tmp_path / "date=20260907" / "hour=12"
    folder.mkdir(parents=True)
    with gzip.open(folder / "obs_invalid.json.gz", "wt", encoding="utf-8") as stream:
        json.dump(value, stream)
    result = read_feature_move_map(tmp_path, as_of_utc=(START+timedelta(minutes=20)).isoformat())
    assert result["archive"]["errors"] == ["ValueError:archive_object_required"]
    assert result["archive"]["files_read"] == 0


def test_extreme_finite_numbers_cannot_create_nonfinite_json():
    import json
    frames = series()
    for index in (15, 20):
        pair = frames[index]["instruments"]["EUR_USD"]
        price = 1e-300 if index == 15 else 1e300
        pair["quote"].update(bid=price, ask=price)
        pair["groups"]["timeframe:M1"]["values"]["rsi14"] = -1e308 if index == 15 else 1e308
    result = build(frames)
    assert result["pairs"][0]["reason"] == "nonfinite_derived_price_move"
    assert by_name(result, "rsi14")["reason"] == "nonfinite_derived_feature_change"
    json.dumps(result, allow_nan=False)
