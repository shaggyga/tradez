"""Repaired publication integration and complete bounded original-news history."""
import copy
import datetime as dt
import gzip
import hashlib
import json
import math
from pathlib import Path
import sqlite3

import pytest
import oanda_causal_forecast_inputs_joint_news_v2 as inputs
import oanda_news_causal_aggregation_guard_v1 as guard
import oanda_source_governance as governance
from test_oanda_causal_forecast_inputs_joint_news_v1 import NOW, member, make_sources as make_legacy_sources


def replace_history(database, count, *, now=NOW):
    """Real disposable rows with original per-event visibility and payload hashes."""
    with sqlite3.connect(database) as con:
        for table in ('news_fast_lane_mappings_v3','news_fast_lane_batches_v3','news_fast_lane_visibility_v3','source_events'):
            con.execute('DELETE FROM '+table)
        for index in range(count):
            epoch=now-48*3600+10+index*20
            raw=member(epoch,index)
            payload=json.dumps({'raw_payload':raw},sort_keys=True)
            sha=governance.source_event_substantive_sha256({'payload_json':payload})
            con.execute('INSERT INTO source_events VALUES(?,?,?,?)',(raw['event_id'],sha,payload,inputs._iso(epoch)))
            con.execute('INSERT INTO news_fast_lane_mappings_v3 VALUES(?,?,?)',(raw['event_id'],sha,index))
            con.execute('INSERT INTO news_fast_lane_batches_v3 VALUES(?,?)',(index,'news_source_governance_fast_lane_v3_committed_visibility_20260905'))
            con.execute('INSERT INTO news_fast_lane_visibility_v3 VALUES(?,?,?,?)',(index,inputs._iso(epoch+1),'post_commit_independent_mapping_read',1))


def make_sources(tmp_path, *, instrument='EUR_USD', pip_size=.0001, now=NOW):
    candles,_,database=make_legacy_sources(tmp_path,instrument=instrument,pip_size=pip_size,now=now)
    from test_oanda_local_news_sentiment_repair_v1 import make_repaired_fixture
    snapshot=make_repaired_fixture(Path(tmp_path),now=now)['snapshot']
    current=Path(tmp_path)/'local_news_sentiment_repair_v1/current_news_v1.json'
    current.parent.mkdir(parents=True,exist_ok=True)
    current.write_text(json.dumps(snapshot),encoding='utf-8')
    return candles,current,database


def make_joint_capture(tmp_path, *, instrument='EUR_USD', pip_size=.0001, now=NOW):
    candles,_,_=make_sources(tmp_path,instrument=instrument,pip_size=pip_size,now=now)
    descriptor=inputs.capture_news_inputs(tmp_path,clock=lambda:now)
    capture=inputs.capture_inputs(candles,instrument,pip_size=pip_size,news_capture=descriptor,clock=lambda:now)
    assert capture['status']=='ready',capture['reasons']
    assert capture['available_families']==[inputs.FAMILY],capture['family_readiness']
    return capture,descriptor,now


def reseal(capture):
    capture['source_capture_sha256']=inputs._digest({key:value for key,value in capture.items() if key!='source_capture_sha256'})
    return capture


def test_complete_history_over_former_row_limit_retains_last_event(tmp_path):
    _,_,database=make_legacy_sources(tmp_path)
    replace_history(database,5501)
    rows,start,diagnostics=inputs._history(database,NOW,guard)
    assert len(rows)==len({row['source_event_id'] for row in rows})==5501
    assert rows[-1]['source_event_id']=='joint_fixture_5500'
    assert diagnostics['complete'] is True and diagnostics['truncated'] is False
    assert diagnostics['selected_rows']==5501 and diagnostics['compact_history_bytes']==len(inputs._encoded(rows))
    assert start==NOW-48*3600+11
    assert rows[-1]['mapping_visible_epoch']==NOW-48*3600+10+5500*20+1


def test_invalid_original_beyond_former_limit_cannot_hide_in_tail(tmp_path):
    _,_,database=make_legacy_sources(tmp_path);replace_history(database,5002)
    with sqlite3.connect(database) as con:
        con.execute("UPDATE source_events SET payload_json=replace(payload_json,'bulletin','forged') WHERE source_event_id='joint_fixture_5001'")
    with pytest.raises(ValueError,match='original_payload_hash'):
        inputs._history(database,NOW,guard)


@pytest.mark.parametrize('limit,value,reason',[('MAX_HISTORY_ROWS',3,'row_bound'),('MAX_HISTORY_RAW_BYTES',50,'byte_bound'),('MAX_HISTORY_ROW_BYTES',20,'byte_bound'),('MAX_NEWS_BYTES',50,'compact_byte_bound')])
def test_history_capacity_limits_withhold_complete_capture(tmp_path,monkeypatch,limit,value,reason):
    _,_,database=make_legacy_sources(tmp_path);monkeypatch.setattr(inputs,limit,value)
    with pytest.raises(ValueError,match=reason):inputs._history(database,NOW,guard)


def test_duplicate_mapping_is_rejected_without_retiming_original(tmp_path):
    _,_,database=make_legacy_sources(tmp_path)
    with sqlite3.connect(database) as con:
        con.execute('INSERT INTO news_fast_lane_mappings_v3 SELECT source_event_id,raw_payload_sha256,batch_seq+1 FROM news_fast_lane_mappings_v3 LIMIT 1')
    with pytest.raises(ValueError,match='duplicate_or_invalid_event_identity'):inputs._history(database,NOW,guard)


def test_history_real_loop_deadline_is_checked(tmp_path,monkeypatch):
    _,_,database=make_legacy_sources(tmp_path)
    monkeypatch.setattr(inputs,'MAX_HISTORY_READ_SEC',-1)
    with pytest.raises((ValueError,sqlite3.OperationalError)):inputs._history(database,NOW,guard)


def test_paged_history_uses_single_pinned_transaction(tmp_path,monkeypatch):
    _,_,database=make_legacy_sources(tmp_path);replace_history(database,260)
    with sqlite3.connect(database) as con:con.execute('PRAGMA journal_mode=WAL')
    original=governance.source_event_substantive_sha256;written=[]
    def concurrent(row):
        if not written:
            written.append(True)
            with sqlite3.connect(database) as con:
                raw=member(NOW-20,99000);payload=json.dumps({'raw_payload':raw},sort_keys=True);sha=original({'payload_json':payload})
                con.execute('INSERT INTO source_events VALUES(?,?,?,?)',(raw['event_id'],sha,payload,inputs._iso(NOW-20)))
                con.execute('INSERT INTO news_fast_lane_mappings_v3 VALUES(?,?,?)',(raw['event_id'],sha,99000))
                con.execute('INSERT INTO news_fast_lane_batches_v3 VALUES(?,?)',(99000,'news_source_governance_fast_lane_v3_committed_visibility_20260905'))
                con.execute('INSERT INTO news_fast_lane_visibility_v3 VALUES(?,?,?,?)',(99000,inputs._iso(NOW-19),'post_commit_independent_mapping_read',1))
        return original(row)
    monkeypatch.setattr(governance,'source_event_substantive_sha256',concurrent)
    rows,_,_=inputs._history(database,NOW,guard)
    assert len(rows)==260 and all(row['source_event_id']!='joint_fixture_99000' for row in rows)
    assert inputs._history(database,NOW,guard)[2]['selected_rows']==261


def test_new_schema_refuses_old_capture_and_old_current_path(tmp_path):
    import oanda_causal_forecast_inputs_joint_news_v1 as old
    from test_oanda_causal_forecast_inputs_joint_news_v1 import make_joint_capture as make_old
    capture,_,_=make_old(tmp_path)
    with pytest.raises(ValueError,match='joint_input_not_ready'):inputs.validate_capture(capture)
    with pytest.raises(FileNotFoundError):inputs.capture_news_inputs(tmp_path,clock=lambda:NOW)
    assert inputs.NUMERICAL_MODULE==old.NUMERICAL_MODULE
    assert inputs.CLASSIFICATION_VERSION==old.CLASSIFICATION_VERSION


def test_real_repaired_capture_fit_and_original_news_clocks(tmp_path):
    capture,descriptor,now=make_joint_capture(tmp_path)
    inputs.validate_capture(capture)
    result=inputs.compute_predictions(capture,clock=lambda:now+1)
    assert result['status']=='ready',result['reasons']
    diagnostics=result['predictions'][inputs.FAMILY]['diagnostics']
    assert diagnostics['training_news_available_max_epoch']<=diagnostics['training_news_feature_cutoff_max_epoch']<=diagnostics['training_label_maturity_max_epoch']<=capture['max_bar_close_epoch']
    assert capture['news_evidence_epoch']<=capture['news_generated_epoch']<=capture['news_first_observed_epoch']<=capture['first_observed_epoch']<=result['computation_started_epoch']
    assert result['history_diagnostics']==capture['history_diagnostics']
    for value in (capture,result,inputs._load_news(descriptor)):
        assert value['research_only'] is True
        assert all(value[key] is False for key in ('can_place_orders','can_promote','can_authorize','account_eligible','proof_eligible'))


@pytest.mark.parametrize('offset',[-1,301,901])
def test_repaired_capture_preserves_actual_fit_freshness(tmp_path,offset):
    capture,_,now=make_joint_capture(tmp_path)
    result=inputs.compute_predictions(capture,clock=lambda:now+offset)
    assert result['status']=='abstain' and result['predictions']=={}


def test_cached_capture_separates_storage_and_history_roots(tmp_path):
    _,_,database=make_sources(tmp_path)
    first=inputs.capture_news_inputs(tmp_path,storage_root=tmp_path/'first',clock=lambda:NOW)
    second=inputs.capture_news_inputs(tmp_path,storage_root=tmp_path/'second',clock=lambda:NOW+1)
    assert Path(first['news_capture_path']).parent==tmp_path/'first'
    assert Path(second['news_capture_path']).parent==tmp_path/'second'
    assert inputs._load_news(second)['first_observed_epoch']==NOW+1
    missing=tmp_path/'missing_history.sqlite'
    with pytest.raises(sqlite3.OperationalError):inputs.capture_news_inputs(tmp_path,history_path=missing,storage_root=tmp_path/'first',clock=lambda:NOW+2)
    assert not missing.exists()


@pytest.mark.parametrize('field',['news_frames','current_news_features','news_expires_epoch','history_diagnostics','proof_eligible','can_authorize'])
def test_resealed_repaired_projection_tamper_rejected(tmp_path,field):
    capture,_,_=make_joint_capture(tmp_path)
    if field=='news_frames':capture[field][next(iter(capture[field]))][0]+=.1
    elif field=='current_news_features':capture[field][0]+=.1
    elif field=='history_diagnostics':capture[field]['selected_rows']+=1
    elif field in ('proof_eligible','can_authorize'):capture[field]=True
    else:capture[field]+=1
    with pytest.raises(ValueError):inputs.validate_capture(reseal(capture))


def test_retained_outer_reconciliation_tamper_rejected(tmp_path):
    _,descriptor,_=make_joint_capture(tmp_path)
    news=inputs._load_news(descriptor);news.pop('news_capture_sha256')
    news['current_snapshot']['source_evidence']['row_count']+=1
    news['current_snapshot_canonical_sha256']=inputs._digest(news['current_snapshot'])
    changed=inputs._store_capture(tmp_path/'tampered',news)
    with pytest.raises(ValueError):inputs._load_news(changed)


def test_shared_compression_and_source_bindings_still_reject_corruption(tmp_path,monkeypatch):
    _,descriptor,_=make_joint_capture(tmp_path)
    bindings=inputs._bindings();bindings['oanda_news_topic_identity_reconciliation_v1.py']='0'*64
    monkeypatch.setattr(inputs,'_bindings',lambda:bindings)
    with pytest.raises(ValueError,match='source_or_policy'):inputs._load_news(descriptor)


def test_verified_cached_bytes_need_no_second_full_canonical_hash(tmp_path,monkeypatch):
    _,descriptor,_=make_joint_capture(tmp_path);original=inputs._load_news(descriptor)
    def refuse_rehash(value):raise AssertionError('already verified immutable cached bytes were rehashed')
    monkeypatch.setattr(inputs,'_digest',refuse_rehash)
    copied=inputs._load_news(descriptor);copied['history'].clear()
    assert inputs._load_news(descriptor)==original


def test_changed_shared_file_cannot_use_verified_cache(tmp_path):
    _,descriptor,_=make_joint_capture(tmp_path);inputs._load_news(descriptor)
    path=Path(descriptor['news_capture_path']);raw=json.loads(gzip.decompress(path.read_bytes()))
    raw['history'][0]['member']['currency_scores']['EUR']=-.99
    path.write_bytes(gzip.compress(json.dumps(raw).encode(),mtime=0))
    with pytest.raises(ValueError,match='hash_mismatch'):inputs._load_news(descriptor)


def test_simultaneous_pairs_share_one_complete_history_read(tmp_path,monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    make_sources(tmp_path);calls=[];original=inputs._history
    def counted(*args,**kwargs):
        calls.append(1);return original(*args,**kwargs)
    monkeypatch.setattr(inputs,'_history',counted)
    with ThreadPoolExecutor(max_workers=4) as pool:
        captures=list(pool.map(lambda _:inputs.capture_news_inputs(tmp_path,clock=lambda:NOW),range(8)))
    assert len(calls)==1 and all(capture==captures[0] for capture in captures)
    assert inputs._load_news(captures[0])['first_observed_epoch']==NOW
