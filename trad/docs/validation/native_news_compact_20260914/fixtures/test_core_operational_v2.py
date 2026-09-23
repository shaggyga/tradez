"""Real owned collector/transport/IO/CSV readiness path; numerical fits forbidden."""
import copy
import csv
import datetime as dt
import hashlib
import json
import math
from pathlib import Path

import pytest
import revision_joint_inputs_v3 as joint
import revision_transport_v4 as transport

io=joint.news_io
BASE=dt.datetime(2026,9,12,10,tzinfo=dt.timezone.utc)
PAIRS=('AUD_CAD','AUD_CHF','AUD_HKD','AUD_JPY','AUD_NZD','AUD_SGD','AUD_USD','CAD_CHF','CAD_HKD','CAD_JPY','CAD_SGD',
 'CHF_HKD','CHF_JPY','CHF_ZAR','EUR_AUD','EUR_CAD','EUR_CHF','EUR_CZK','EUR_DKK','EUR_GBP','EUR_HKD','EUR_HUF','EUR_JPY',
 'EUR_NOK','EUR_NZD','EUR_PLN','EUR_SEK','EUR_SGD','EUR_TRY','EUR_USD','EUR_ZAR','GBP_AUD','GBP_CAD','GBP_CHF','GBP_HKD',
 'GBP_JPY','GBP_NZD','GBP_PLN','GBP_SGD','GBP_USD','GBP_ZAR','HKD_JPY','NZD_CAD','NZD_CHF','NZD_HKD','NZD_JPY','NZD_SGD',
 'NZD_USD','SGD_CHF','SGD_JPY','TRY_JPY','USD_CAD','USD_CHF','USD_CNH','USD_CZK','USD_DKK','USD_HKD','USD_HUF','USD_JPY',
 'USD_MXN','USD_NOK','USD_PLN','USD_SEK','USD_SGD','USD_THB','USD_TRY','USD_ZAR','ZAR_JPY')

def at(minute):return BASE+dt.timedelta(minutes=minute)
def epoch(minute):return at(minute).timestamp()
def clock_state(minute):
    return {'status':'ok','generated_utc':at(minute).isoformat(),'timestamp_normalization_trusted':True,
      'host_clock_synchronized':True,'source_fresh':True,'broker_clock_lead_sec':.2,'broker_clock_sample_count':128,'source_age_sec':0}

def proof_clock(minute):
    counter=[0]
    def read():
        counter[0]+=.01
        return epoch(minute)+counter[0],clock_state(minute)
    return read

def source_identity():
    return {'source_id':'A','name':'query_A','kind':'rss','source_contract_id':'contract_A',
      'source_cohort_id':'cohort_A','currencies':['USD'],'verified':True,'quality':.8}

def add_article(path,ordinal,minute):
    stamp=at(minute)
    raw={'event_id':'owned_event_'+str(ordinal),'source_id':'A','source_name':'query_A','source_kind':'rss','source_role':'primary_policy_release','source_direct':True,
      'source_contract_id':'contract_A','source_cohort_id':'cohort_A','source_quality':.8,'source_verified':True,
      'first_seen_utc':stamp.isoformat(),'last_seen_utc':stamp.isoformat(),'published_utc':stamp.isoformat(),
      'causal_known_utc':stamp.isoformat(),'title':'Federal Reserve raises interest rate',
      'summary':'Federal Reserve raises interest rate. The policy committee confirmed its decision.',
      'url':'https://www.federalreserve.gov/fixture-policy/'+str(ordinal),'source_url':'https://www.federalreserve.gov/fixture-policy/'+str(ordinal),
      'domain':'www.federalreserve.gov','source_currencies':['USD'],'structured_event':True,'external_id':'owned_statement_'+str(ordinal),'event_series_id':'usd_policy_statement','scheduled_utc':stamp.isoformat(),'observation_clock_trusted':True,
      'observation_clock_source':'owned_fixture_attested','collector_contract_id':io.collector.COLLECTOR_CONTRACT_ID,
      'collector_cohort_id':io.collector.COLLECTOR_COHORT_ID,'observation_time_contract_id':io.collector.OBSERVATION_TIME_CONTRACT_ID}
    article=io.collector.classify_article(raw,first_seen=stamp)
    assert type(article['structured_event']) is bool and article['structured_event'] is True
    assert article['relevant'] is True and article['currency_scores'], {k:article.get(k) for k in ('relevant','currency_scores','context_reason','source_role')}
    state={k:raw[k] for k in ('collector_contract_id','collector_cohort_id','observation_time_contract_id','observation_clock_trusted','observation_clock_source')}
    con=io.collector.open_database(path)
    try:io.collector.upsert_articles(con,[article],stamp,classification_clock_provider=lambda:(stamp,state));con.commit()
    finally:con.close()

def health(config,minute,status='ok'):
    heartbeat={'schema_version':io.collector.SCHEMA_VERSION,'classification_version':io.adapter.guard.CLASSIFICATION_VERSION,
      'collector_contract_id':io.collector.COLLECTOR_CONTRACT_ID,'collector_cohort_id':io.collector.COLLECTOR_COHORT_ID,
      'generated_utc':at(minute).isoformat(),'last_progress_utc':at(minute).isoformat(),
      'cycle_started_utc':at(minute-.1).isoformat(),'status':'cycle_complete','cycle_in_progress':False}
    latest={'schema_version':io.collector.SCHEMA_VERSION,'collector_contract_id':io.collector.COLLECTOR_CONTRACT_ID,
      'collector_cohort_id':io.collector.COLLECTOR_COHORT_ID,'generated_utc':at(minute).isoformat(),'status':status,
      'policy':{'research_only':True,'execution_eligible':False},'source_health':{'healthy':1,'degraded':0},
      'sources':[{'source_id':'A','status':'ok'}]}
    for key,value in (('clock_path',clock_state(minute)),('heartbeat_path',heartbeat),('latest_path',latest)):
        Path(config[key]).write_bytes(io.encode(value))

def make(root):
    base=transport._owners();source=root/'collector.sqlite'
    con=io.collector.open_database(source);con.commit();con.close()
    policy=io.reader.policy_for(io.reader.source_identities([source_identity()]),{
      'collector_contract_id':io.collector.COLLECTOR_CONTRACT_ID,'collector_cohort_id':io.collector.COLLECTOR_COHORT_ID,
      'observation_time_contract_id':io.collector.OBSERVATION_TIME_CONTRACT_ID})
    archive=root/'archive';archive.mkdir();state=root/'transport_state';state.mkdir()
    config={'schema_version':base.CONFIG,'cohort_id':'owned_flat_operational_v1','consumer_id':'owned_all68',
      'publication_path':str(root/'publisher.sqlite'),'observation_path':str(root/'consumer.sqlite'),
      'archive_root':str(archive),'clock_path':str(root/'clock_integrity_v1.json'),
      'heartbeat_path':str(root/'collector_heartbeat_v1.json'),'latest_path':str(root/'collector_latest_v1.json'),
      'policy':policy,'input_identity':io.reader._identity(source)}
    base_path=root/'base_config.json';base_path.write_bytes(io.encode(config))
    transport_config={'schema_version':transport.CONFIG,'news_io_config_path':str(base_path),
      'news_io_config_sha256':hashlib.sha256(base_path.read_bytes()).hexdigest(),'state_root':str(state),
      'interval_sec':60,'duration_sec':172800}
    transport_path=root/'transport_config.json';transport_path.write_bytes(io.encode(transport_config))
    config={**config,'schema_version':io.CONFIG,'transport_config_path':str(transport_path),
      'transport_config_sha256':hashlib.sha256(transport_path.read_bytes()).hexdigest()}
    runner=transport.open_runner(str(transport_path),config['transport_config_sha256'])
    return source,config,runner

def prices(root):
    root.mkdir()
    path=root/'GBP_USD_M1.csv'
    with path.open('w',encoding='utf-8',newline='') as stream:
        writer=csv.writer(stream);writer.writerow(['datetime','instrument','granularity','complete','close'])
        for minute in range(-20,766):
            writer.writerow([at(minute).isoformat(),'GBP_USD','M1','true',1.2+.000001*(minute+20)+.00001*math.sin(minute/15)])
    return path

def test_real_cold_context_readiness_and_pair_isolation(tmp_path,monkeypatch):
    source,config,runner=make(tmp_path)
    try:
        observation_minutes=[]
        for ordinal,origin in enumerate([15*i for i in range(48)]+[765]):
            known=origin+.1+ordinal*.001
            add_article(source,ordinal,known)
            result=transport.run_cycle(runner,clock_provider=proof_clock(origin+.5))
            assert result['status']=='ready',result
            observation_minutes.append(origin+.5)
        health(config,766.1)
        session=io.create_session(config)
        capture=io.capture_shared(session,clock=lambda:epoch(766.1))
        calls=[];original=io.historical_features
        def history_once(*a,**k):calls.append((a,k));return original(*a,**k)
        monkeypatch.setattr(io,'historical_features',history_once)
        share=joint.history.prepare_universal_history_share(session,capture,list(PAIRS))
        assert len(calls)==1
        root=tmp_path/'prices';path=prices(root)
        missing=joint.capture_inputs(root,'EUR_USD',pip_size=.0001,session=session,news_capture=capture,history_share=share,clock=lambda:epoch(766.2))
        assert missing['status']=='abstain' and missing['reasons']
        good=joint.capture_inputs(root,'GBP_USD',pip_size=.0001,session=session,news_capture=capture,history_share=share,clock=lambda:epoch(766.2))
        assert good['status']=='ready',good['reasons']
        assert good['readiness_status']=='ready',good['family_readiness']
        assert good['available_families']==[joint.FAMILY]
        joint.validate_capture(good,session=session,news_capture=capture,history_share=share)
        assert len(calls)==1
        assert len(good['news_frames'])==48
        assert len({tuple(v[:4]) for v in good['news_frames'].values()})>=8
        assert sum(v[1]>0 for v in good['news_frames'].values())>=12
        assert good['feature_decision_epoch']==epoch(766.2)
        assert good['reference_start_epoch']==int(epoch(765))
        assert all(v['available_epoch']<=v['origin']+60 for v in good['training_news_points'] if v['coverage_usable'])
        # Real predictor receives unchanged rows/labels/34-feature arguments; fitting remains a forbidden spy.
        numeric=joint._model();seen=[]
        def prediction_spy(rows,cutoff,**kwargs):
            readiness=numeric.family_readiness(rows,cutoff,**{k:v for k,v in kwargs.items() if k!='families'})
            seen.append((copy.deepcopy(rows),cutoff,copy.deepcopy(kwargs)))
            assert readiness[joint.FAMILY]['ready']
            diagnostics={'training_row_start_epochs_by_pair':{'GBP_USD':sorted(map(int,good['news_frames']))}}
            return {joint.FAMILY:(.5,.6,diagnostics)},readiness
        monkeypatch.setattr(numeric,'predict_with_readiness',prediction_spy)
        predictions=joint.compute_predictions(good,session=session,news_capture=capture,history_share=share,clock=lambda:epoch(766.3))
        assert predictions['status']=='ready',predictions['reasons']
        assert len(seen)==1 and seen[0][1]==good['reference_start_epoch']
        assert seen[0][2]['news_frames']==good['news_frames'] and seen[0][2]['current_news']==good['current_news_features']
        assert len(calls)==1
        # Recovery of the previously missing pair uses its own actual CSV; other pair remains unchanged.
        Path(root/'EUR_USD_M1.csv').write_text(path.read_text(encoding='utf-8').replace('GBP_USD','EUR_USD'),encoding='utf-8')
        repaired=joint.capture_inputs(root,'EUR_USD',pip_size=.0001,session=session,news_capture=capture,history_share=share,clock=lambda:epoch(766.4))
        assert repaired['status']=='ready' and repaired['readiness_status']=='ready',repaired['reasons']
        meta=joint.history.history_share_metadata(share)
        report={'status':'real_owned_readiness_and_one_batch_all68_passed','observations':len(observation_minutes),
          'training_contexts':len(good['news_frames']),'pairs':list(PAIRS),'history_batches':len(calls),
          'readiness':good['family_readiness'],'current_news_features':good['current_news_features'],
          'history_share':meta,'missing_pair_reasons':missing['reasons'],'repaired_pair_status':repaired['readiness_status'],
          'feature_decision_epoch':good['feature_decision_epoch'],'reference_start_epoch':good['reference_start_epoch'],
          'native_target_epoch':good['reference_start_epoch']+3660,'prediction_call_count':len(seen),
          'actual_fits':0,'actual_market_data':False,'prospective_health_or_forecast_performance_proven':False}
        (tmp_path/'CORE_OPERATIONAL_PROOF.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
    finally:transport.close_runner(runner)
