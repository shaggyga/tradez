"""Real disposable price/history/current-proof captures; never a broker seam."""
import copy
import csv
import datetime as dt
import hashlib
import gzip
import json
import math
from pathlib import Path
import sqlite3

import pytest
import oanda_causal_forecast_inputs_joint_news_v1 as inputs
import oanda_news_causal_aggregation_guard_v1 as guard
import oanda_source_governance as governance

NOW=1788811201.0
OLD_VERSION='local_fx_news_rules_20260904_v164_conflict_duration_recap_guard'

def member(epoch, index, *, version=OLD_VERSION, score=None, observed=None, verified=False):
    value=math.sin(index*.71)*.8 if score is None else score
    return {'event_id':f'joint_fixture_{index}', 'headline':f'Central bank economic bulletin edition {index} reports current activity',
        'source_id':f'publisher_{index}', 'source_name':f'Publisher {index}', 'publisher_url':f'https://publisher{index}.example',
        'source_url':f'https://publisher{index}.example/report', 'source_verified':verified, 'source_direct':verified,
        'source_quality':.8, 'directional_confidence':.6, 'published_utc':inputs._iso(epoch-10),
        'first_seen_utc':inputs._iso(epoch), 'causal_known_utc':inputs._iso(epoch),
        'observed_available_utc':inputs._iso(observed) if observed else None,
        'currency_scores':{'EUR':value},'forward_signal_timely':True,'forward_timeliness_limit_minutes':30,
        'estimated_reaction_horizon_minutes':60,'reports_prior_market_move':False,'context_only':False,
        'classification_version':version,'category':'economic_activity','scope':'currencies','direct_currencies':['EUR'],
        'inferred_currencies':[],'semantic_claims':[], 'observation_clock_trusted':True}

def make_sources(tmp_path, *, instrument='EUR_USD', pip_size=.0001, now=NOW, count=2100, current_verified=False):
    root=Path(tmp_path);candles=root/'candles';candles.mkdir(parents=True,exist_ok=True)
    end=int(now//60)*60-60;start=end-(count-1)*60
    with (candles/f'{instrument}_M1.csv').open('w',encoding='utf-8',newline='') as handle:
        writer=csv.writer(handle);writer.writerow(['datetime','instrument','granularity','complete','close'])
        for i in range(count):
            writer.writerow([inputs._iso(start+i*60),instrument,'M1','true',1.15+pip_size*(.004*i+3*math.sin(i/95)+math.sin(i/17))])
    database=root/'state/source_governance_v1.sqlite';database.parent.mkdir(exist_ok=True)
    with sqlite3.connect(database) as con:
        con.executescript('''CREATE TABLE source_events(source_event_id TEXT PRIMARY KEY,raw_payload_sha256 TEXT,payload_json TEXT,effective_from_utc TEXT);
          CREATE TABLE news_fast_lane_mappings_v3(source_event_id TEXT,raw_payload_sha256 TEXT,batch_seq INTEGER);
          CREATE TABLE news_fast_lane_batches_v3(batch_seq INTEGER PRIMARY KEY,contract_id TEXT);
          CREATE TABLE news_fast_lane_visibility_v3(batch_seq INTEGER PRIMARY KEY,mapping_visible_utc TEXT,availability_basis TEXT,consumer_first_observation_required INTEGER);''')
        for i,epoch in enumerate(range((start//900)*900, end,900)):
            raw=member(epoch+1,i);payload=json.dumps({'raw_payload':raw},sort_keys=True)
            sha=governance.source_event_substantive_sha256({'payload_json':payload})
            con.execute('INSERT INTO source_events VALUES(?,?,?,?)',(raw['event_id'],sha,payload,inputs._iso(epoch+1)))
            con.execute('INSERT INTO news_fast_lane_mappings_v3 VALUES(?,?,?)',(raw['event_id'],sha,i))
            con.execute('INSERT INTO news_fast_lane_batches_v3 VALUES(?,?)',(i,'news_source_governance_fast_lane_v3_committed_visibility_20260905'))
            con.execute('INSERT INTO news_fast_lane_visibility_v3 VALUES(?,?,?,?)',(i,inputs._iso(epoch+2),'post_commit_independent_mapping_read',1))
    raw=member(now-80,9000,version=guard.CLASSIFICATION_VERSION,score=.4,verified=current_verified)
    original={'topic_id':'joint_current_fixture','published_utc':raw['published_utc'],'first_seen_utc':raw['first_seen_utc'],
        'causal_known_utc':raw['causal_known_utc'],'post_window_minutes':60,'headline':raw['headline']}
    asof=dt.datetime.fromtimestamp(now-5,dt.timezone.utc)
    topic=guard.guard_topic(original,[raw],as_of=asof)
    snapshot=guard.build_current_news_snapshot([topic],as_of=asof)
    snapshot['generated_utc']=inputs._iso(now-1)
    snapshot['source_bindings']={name:inputs._bindings()[name] for name in inputs.CURRENT_SOURCE_FILES}
    current=root/'local_news_sentiment/joint_news_current_v1.json';current.parent.mkdir(exist_ok=True)
    current.write_text(json.dumps(snapshot),encoding='utf-8')
    return candles,current,database

def make_joint_capture(tmp_path, *, instrument='EUR_USD', pip_size=.0001, now=NOW):
    candles,_,_=make_sources(tmp_path,instrument=instrument,pip_size=pip_size,now=now)
    descriptor=inputs.capture_news_inputs(Path(tmp_path),clock=lambda:now)
    capture=inputs.capture_inputs(candles,instrument,pip_size=pip_size,clock=lambda:now,news_capture=descriptor)
    assert capture['status']=='ready',capture['reasons']
    assert capture['available_families']==[inputs.FAMILY],capture['family_readiness']
    return capture,descriptor,now

def reseal(cap):
    cap['source_capture_sha256']=inputs._digest({k:v for k,v in cap.items() if k!='source_capture_sha256'});return cap

@pytest.fixture
def captured(tmp_path):return make_joint_capture(tmp_path)

def test_real_capture_fit_and_all_causal_bounds(captured):
    cap,descriptor,now=captured;inputs.validate_capture(cap)
    result=inputs.compute_predictions(cap,clock=lambda:now+1)
    assert result['status']=='ready',result['reasons']
    prediction=result['predictions'][inputs.FAMILY];diag=prediction['diagnostics']
    assert 48<=diag['training_rows']<=140
    assert diag['training_news_available_max_epoch']<=diag['training_news_feature_cutoff_max_epoch']<=diag['training_label_maturity_max_epoch']<=cap['max_bar_close_epoch']
    assert cap['news_evidence_epoch']<=cap['news_generated_epoch']<=cap['news_first_observed_epoch']==cap['news_available_epoch']<=cap['first_observed_epoch']<=result['computation_started_epoch']
    for key in ('news_capture_sha256','news_evidence_epoch','news_generated_epoch','news_first_observed_epoch','news_available_epoch','news_expires_epoch'):
        assert result[key]==cap[key]
    assert prediction['expected_signed_pips']==pytest.approx(diag['neutral_news_ablation_expected_pips']+diag['news_ablation_difference_pips'])
    assert 'not_causal_impact' in diag['news_ablation_scope']
    assert 'uncalibrated' in diag['probability_scope']
    assert not any(key in cap for key in ('history','current_snapshot','current_members'))

def test_shared_news_cache_is_immutable_and_keeps_actual_observation(captured):
    cap,descriptor,now=captured;before=inputs._load_news(descriptor)
    loaded=inputs._load_news(descriptor);loaded['current_members'].clear()
    assert inputs._load_news(descriptor)==before
    later=inputs.capture_news_inputs(Path(descriptor['news_capture_path']).parents[2],clock=lambda:now+10)
    assert later==descriptor and inputs._load_news(later)['first_observed_epoch']==now

def test_cached_shared_news_rechecks_current_source_bindings(captured,monkeypatch):
    _,descriptor,_=captured;inputs._load_news(descriptor);bindings=inputs._bindings();bindings['oanda_news_causal_aggregation_guard_v1.py']='0'*64
    monkeypatch.setattr(inputs,'_bindings',lambda:bindings)
    with pytest.raises(ValueError,match='source_or_policy'):inputs._load_news(descriptor)

@pytest.mark.parametrize('field',['news_frames','current_news_features','news_evidence_epoch','news_generated_epoch','news_first_observed_epoch','news_available_epoch','news_expires_epoch','readiness_status','available_families','training_news_availability_by_epoch'])
def test_resealed_pair_projection_tamper_rejected(captured,field):
    cap,_,_=captured;cap=copy.deepcopy(cap)
    if field=='news_frames':cap[field][next(iter(cap[field]))][0]+=.1
    elif field=='current_news_features':cap[field][0]+=.1
    elif field=='readiness_status':cap[field]='partial'
    elif field=='available_families':cap[field]=[]
    elif field=='training_news_availability_by_epoch':cap[field][next(iter(cap[field]))]+=1
    else:cap[field]+=1
    with pytest.raises(ValueError):inputs.validate_capture(reseal(cap))

@pytest.mark.parametrize('offset',[-1,301,901])
def test_real_computation_withholds_bad_actual_clocks(captured,offset):
    cap,_,now=captured;result=inputs.compute_predictions(cap,clock=lambda:now+offset)
    assert result['status']=='abstain' and result['predictions']=={}

def test_expiry_during_fit_withholds_result(captured):
    cap,_,now=captured;times=iter([now+1,cap['news_expires_epoch']+1])
    result=inputs.compute_predictions(cap,clock=lambda:next(times))
    assert result['status']=='abstain' and result['predictions']=={} and any('during_computation' in s for s in result['reasons'])

@pytest.mark.parametrize('kind',['missing_guard_binding','corrupt_proof','future_source','stale_source','bad_history_hash','bad_visibility','wrapper_orders','wrapper_research','wrapper_state','wrapper_count','missing_proof','conflicting_duplicate'])
def test_ingestion_rejects_untrusted_current_or_history(tmp_path,kind):
    _,current,database=make_sources(tmp_path)
    snapshot=json.loads(current.read_text(encoding='utf-8'))
    if kind=='missing_guard_binding':snapshot['source_bindings'].pop('oanda_news_causal_aggregation_guard_v1.py')
    elif kind=='corrupt_proof':snapshot['topics'][0]['currency_scores']={'EUR':.8}
    elif kind=='future_source':snapshot['generated_utc']=inputs._iso(NOW+1)
    elif kind=='stale_source':snapshot['as_of_utc']=inputs._iso(NOW-301)
    elif kind=='bad_history_hash':
        with sqlite3.connect(database) as con:con.execute("UPDATE source_events SET payload_json=replace(payload_json,'bulletin','forged')")
    elif kind=='bad_visibility':
        with sqlite3.connect(database) as con:con.execute("UPDATE news_fast_lane_visibility_v3 SET consumer_first_observation_required=0")
    elif kind=='wrapper_orders':snapshot['can_place_orders']=True
    elif kind=='wrapper_research':snapshot['research_only']=False
    elif kind=='wrapper_state':snapshot['news_state']='directional'
    elif kind=='wrapper_count':snapshot['topic_count']+=1
    elif kind=='missing_proof':snapshot['topics'][0].pop('causal_aggregation_guard')
    elif kind=='conflicting_duplicate':snapshot['topics'].append(copy.deepcopy(snapshot['topics'][0]));snapshot['topics'][-1]['headline']='forged'
    current.write_text(json.dumps(snapshot),encoding='utf-8')
    with pytest.raises(ValueError):inputs.capture_news_inputs(tmp_path,clock=lambda:NOW)

def test_clock_is_sampled_after_retained_snapshot_bytes(tmp_path):
    make_sources(tmp_path);times=iter([NOW-2,NOW,NOW])
    descriptor=inputs.capture_news_inputs(tmp_path,clock=lambda:next(times))
    assert inputs._load_news(descriptor)['first_observed_epoch']==NOW

def test_context_waits_for_member_actual_observation(captured):
    _,descriptor,_=captured;news=inputs._load_news(descriptor);row=copy.deepcopy(news['history'][3]);t=row['mapping_visible_epoch']+10
    row['member']['observed_available_utc']=inputs._iso(t+1)
    news={**news,'news_capture_sha256':'future-context-fixture','history':[row]}
    assert inputs._frame(news,t)['members']==[]
    assert inputs._frame(news,t+1)['available_max_epoch']==t+1

def test_consumer_acknowledgement_does_not_renew_forward_expiry(tmp_path):
    candles,current,_=make_sources(tmp_path,current_verified=True)
    descriptor=inputs.capture_news_inputs(tmp_path,clock=lambda:NOW)
    news=inputs._load_news(descriptor);member_time=inputs._original_known(news['current_members'][0])
    frame=inputs._frame(news,NOW,live=True)
    assert frame['directional'][0]['expires_epoch']==member_time+3600
    cap=inputs.capture_inputs(candles,'EUR_USD',pip_size=.0001,clock=lambda:NOW,news_capture=descriptor)
    assert cap['status']=='ready' and cap['available_families']==[]
    assert any('unseen_in_training' in s for s in cap['family_readiness'][inputs.FAMILY]['reasons'])

def test_shared_file_tamper_rejected_even_after_cache(captured):
    _,descriptor,_=captured;inputs._load_news(descriptor);path=Path(descriptor['news_capture_path'])
    value=json.loads(gzip.decompress(path.read_bytes()));value['first_observed_epoch']+=1;path.write_bytes(gzip.compress(json.dumps(value).encode(),mtime=0))
    with pytest.raises(ValueError,match='hash_mismatch'):inputs._load_news(descriptor)

@pytest.mark.parametrize('kind',['truncated','trailing_member','invalid','bomb'])
def test_shared_compression_rejects_malformed_or_oversized_payload(captured,kind):
    _,descriptor,_=captured;path=Path(descriptor['news_capture_path']);raw=path.read_bytes()
    if kind=='truncated':raw=raw[:-8]
    elif kind=='trailing_member':raw+=gzip.compress(b'{}',mtime=0)
    elif kind=='invalid':raw=b'not compressed'
    else:raw=gzip.compress(b' '* (inputs.MAX_NEWS_BYTES+1),mtime=0)
    path.write_bytes(raw)
    with pytest.raises(ValueError,match='compression'):inputs._load_news(descriptor)

def test_compressed_storage_keeps_canonical_hash_and_original_clocks(captured):
    _,descriptor,_=captured;path=Path(descriptor['news_capture_path']);stored=path.read_bytes();raw=gzip.decompress(stored);value=json.loads(raw)
    assert path.name==descriptor['news_capture_sha256']+'.json.gz'
    assert len(stored)<len(raw)/3
    assert inputs._digest({k:v for k,v in value.items() if k!='news_capture_sha256'})==descriptor['news_capture_sha256']
    assert value['first_observed_epoch']==NOW

@pytest.mark.parametrize('field',['current_members','snapshot_hash','snapshot_flags','history_future_mapping','history_bad_version','history_bad_score'])
def test_resealed_shared_projection_cannot_override_retained_proof(captured,field):
    _,descriptor,_=captured;value=inputs._load_news(descriptor)
    if field=='current_members':value['current_members'][0]['currency_scores']['EUR']=.9
    elif field=='snapshot_hash':value['current_snapshot_canonical_sha256']='0'*64
    elif field=='snapshot_flags':value['current_snapshot']['can_place_orders']=True
    elif field=='history_future_mapping':value['history'][0]['mapping_visible_epoch']=NOW+1
    elif field=='history_bad_version':value['history'][0]['member']['classification_version']='current_classification_as_past'
    else:value['history'][0]['member']['currency_scores']['EUR']=float('inf')
    value.pop('news_capture_sha256')
    with pytest.raises(ValueError):
        forged=inputs._store_capture(Path(descriptor['news_capture_path']).parent,value)
        inputs._load_news(forged)

def test_concurrent_shared_store_publishes_complete_immutable_bytes(captured):
    from concurrent.futures import ThreadPoolExecutor
    _,descriptor,_=captured;value=inputs._load_news(descriptor);value.pop('news_capture_sha256')
    root=Path(descriptor['news_capture_path']).parent/'concurrent'
    with ThreadPoolExecutor(max_workers=4) as pool:
        results=list(pool.map(lambda _:inputs._store_capture(root,value),range(8)))
    assert all(result==results[0] for result in results)
    assert inputs._load_news(results[0])['first_observed_epoch']==NOW

def test_missing_current_news_remains_unavailable(tmp_path):
    candles,current,_=make_sources(tmp_path);current.unlink()
    cap=inputs.capture_inputs(candles,'EUR_USD',pip_size=.0001,clock=lambda:NOW)
    assert cap['status']=='abstain' and cap['available_families']==[]
