"""Disposable source-bound summaries and isolated JS; no server or broker I/O."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess

import pytest
import oanda_operational_dashboard_selection_v1 as d

NOW=1789345000.0
ROOT=Path(__file__).parent


def save(path,value):
    path.parent.mkdir(parents=True,exist_ok=True);raw=d.encoded(value);path.write_bytes(raw)
    return hashlib.sha256(raw).hexdigest()


def fixture(tmp_path):
    root=tmp_path/'trad';root.mkdir()
    selection=dict(schema_version=d.SCHEMA,activated_epoch=NOW-10,research_only=True,
                   **{k:False for k in d.FLAGS[:3]},source_bindings={},
                   features={'archive_path':'data/oanda_training_manager/repair/feature_observations_v2'})
    for name in d.SOURCE_FILES:
        raw=b'# fixture';(root/name).write_bytes(raw);selection['source_bindings'][name]=hashlib.sha256(raw).hexdigest()
    originals={}
    for kind,spec in d.KINDS.items():
        raw=b'# worker';(root/spec[5]).write_bytes(raw)
        bindings={spec[5]:hashlib.sha256(raw).hexdigest()}
        pairs={};families={}
        for family in spec[7]:
            contract={'cohorts':{family:kind+'.'+family}}
            families[family]={'contract':contract,'contract_sha256':d.digest(contract)}
        registry=dict(schema_version=spec[0],research_only=True,**{k:False for k in d.FLAGS},
                      source_bindings=bindings,pairs={'EUR_USD':{'pip_size':.0001,'families':families}})
        registry_path=root/'config'/f'{kind}.json';registry_sha=save(registry_path,registry)
        study=root/'data/oanda_training_manager/repair'/spec[4]
        activation=dict(schema_version=spec[3],status='activated_empty',research_only=True,**{k:False for k in d.FLAGS},
            activation_completed_epoch=NOW-5000,source_bindings=bindings,study_root=str(study),
            registry_file_sha256=registry_sha,registry_sha256=d.digest(registry))
        activation_sha=save(study/'activation_receipt.json',activation)
        selection[kind]=dict(registry_path=str(registry_path.relative_to(root)),registry_sha256=registry_sha,
                            study_path=str(study.relative_to(root)),activation_sha256=activation_sha)
        slots={}
        for family,registered in families.items():
            ref=NOW-100;issued=NOW-90;target=ref+3600;published=NOW-80;consumed=NOW-79
            arm=dict(instrument='EUR_USD',family=family,cohort_id=registered['contract']['cohorts'][family],
                reference_epoch=ref,issued_epoch=issued,target_epoch=target,horizon_sec=3600,
                reference_mid='1.2',pip_size=.0001,probability_up=.6,predicted_return_bps=2,side=1)
            publication=dict(instrument='EUR_USD',family=family,horizon_sec=3600,reference_epoch=ref,
                issued_epoch=issued,publication_epoch=published,consumption_epoch=consumed,target_epoch=target,
                reference_available_epoch=ref+1,publication_verified=True,consumption_verified=True,
                forecast_sha256='f'*64,publication_receipt_sha256='a'*64,consumer_receipt_sha256='b'*64)
            if kind=='joint':
                anchor=dict(origin_close_epoch=ref,target_close_epoch=target,origin_mid_hex=float(1.2).hex(),
                            native_probability_up_hex=float(.6).hex())
                arm.update(research_only=True,**{k:False for k in d.FLAGS},native_anchor=anchor,native_anchor_sha256=d.digest(anchor))
                publication['native_anchor_sha256']=d.digest(anchor)
            publication['forecasts']=[arm]
            slots[family]=dict(status='forecast',reason='forecast',observed_epoch=NOW-3,activated_epoch=NOW-5001,
                cohort_id=arm['cohort_id'],contract_sha256=registered['contract_sha256'],
                counts={'publication':2,'outcomes':1},latest_forecast=publication,current_readiness={'status':'ready'},last_attempt=None)
        summary=dict(schema_version=spec[1],registry_sha256=d.digest(registry),generated_epoch=NOW-3,
                     research_only=True,**{k:False for k in d.FLAGS},rows=[dict(instrument='EUR_USD',pip_size=.0001,families=slots)])
        heartbeat=dict(schema_version=spec[2],registry_sha256=d.digest(registry),generated_epoch=NOW-2,
                       research_only=True,**{k:False for k in d.FLAGS},summary_generated_epoch=NOW-3,
                       summary_read_completed_epoch=NOW-2.5,errors=0,last_error='',heartbeat_publication_errors=0)
        originals[kind]=(study,summary,heartbeat)
    def write():
        for study,summary,heartbeat in originals.values():
            summary['payload_sha256']=d.digest({k:v for k,v in summary.items() if k!='payload_sha256'})
            heartbeat['summary_sha256']=d.digest(summary)
            save(study/'summary.json',summary);save(study/'heartbeat.json',heartbeat)
        save(root/d.POINTER,selection)
    write()
    return root,selection,originals,write


def test_current_price_and_native_joint_keep_versions_probability_and_target(tmp_path):
    root,_,_,_=fixture(tmp_path)
    value=d.read_dashboard_sources(root,now_epoch=NOW)
    assert value['status']=='current'
    assert value['price']['study_version']==3 and value['joint']['study_version']=='joint_v7'
    assert value['price']['ledger_counts']=={'publication':4,'outcomes':2}
    arm=value['joint']['rows'][0]['active_forecasts'][0]
    assert arm['probability_up']==.6 and arm['target_epoch']==NOW+3500
    assert 'not independent ledger rescore' in arm['display_evidence_scope']


def test_absent_pointer_preserves_legacy_but_invalid_pointer_never_falls_back(tmp_path):
    assert d.read_dashboard_sources(tmp_path,now_epoch=NOW) is None
    root,selection,_,write=fixture(tmp_path);selection['can_place_orders']=True;write()
    value=d.read_dashboard_sources(root,now_epoch=NOW)
    assert value['selected'] is True and value['status']=='unavailable'
    assert value['price']['rows']==[] and value['joint']['rows']==[] and value['features'] is None


@pytest.mark.parametrize('mode',['stale','future','authority','hash','wrong_pair','wrong_cohort','wrong_target','native_anchor','worker_changed'])
def test_invalid_native_summary_does_not_hide_valid_price_view(tmp_path,mode):
    root,selection,originals,write=fixture(tmp_path);study,s,h=originals['joint'];slot=next(iter(s['rows'][0]['families'].values()));p=slot['latest_forecast']
    if mode=='stale':s['generated_epoch']=NOW-91
    if mode=='future':h['generated_epoch']=NOW+1
    if mode=='authority':s['can_place_orders']=True
    if mode=='hash':p['forecast_sha256']='bad'
    if mode=='wrong_pair':p['forecasts'][0]['instrument']='GBP_USD'
    if mode=='wrong_cohort':p['forecasts'][0]['cohort_id']='legacy'
    if mode=='wrong_target':p['target_epoch']+=1
    if mode=='native_anchor':p['forecasts'][0]['native_anchor']['origin_mid_hex']=float(1.3).hex()
    if mode=='worker_changed':(root/d.KINDS['joint'][5]).write_bytes(b'changed')
    write();value=d.read_dashboard_sources(root,now_epoch=NOW)
    assert value['status']=='partial_unavailable' and value['joint']['rows']==[]
    assert value['price']['status']=='current'


def test_blocked_inputs_remain_current_observation_without_a_forecast(tmp_path):
    root,_,originals,write=fixture(tmp_path);slot=next(iter(originals['joint'][1]['rows'][0]['families'].values()))
    slot.update(status='warming',reason='insufficient_joint_training_history',latest_forecast=None)
    write();value=d.read_dashboard_sources(root,now_epoch=NOW)['joint']
    assert value['status']=='current' and value['forecast_pair_count']==0
    assert value['reason_counts']=={'insufficient_joint_training_history':1}


def test_original_expired_forecast_is_excluded_without_renewing_its_clock(tmp_path):
    root,_,originals,write=fixture(tmp_path);slot=next(iter(originals['price'][1]['rows'][0]['families'].values()))
    p=slot['latest_forecast'];p['reference_epoch']=NOW-3601;p['target_epoch']=NOW-1
    p['forecasts'][0].update(reference_epoch=p['reference_epoch'],target_epoch=p['target_epoch'])
    write();value=d.read_dashboard_sources(root,now_epoch=NOW)['price']
    assert value['status']=='current' and len(value['rows'][0]['active_forecasts'])==1


def test_wrong_summary_heartbeat_generation_and_changed_selection_source_refused(tmp_path,monkeypatch):
    monkeypatch.setattr(d.time,'sleep',lambda _:None)
    root,_,originals,_=fixture(tmp_path);study,_,h=originals['joint'];h['summary_sha256']='0'*64;save(study/'heartbeat.json',h)
    assert d.read_dashboard_sources(root,now_epoch=NOW)['joint']['reason']=='summary_heartbeat_generation_mismatch'
    (root/'oanda_main_signal_dashboard.html').write_bytes(b'changed')
    value=d.read_dashboard_sources(root,now_epoch=NOW)
    assert value['status']=='unavailable' and value['reason']=='dashboard_source_changed'


def test_generation_retry_waits_only_for_matching_new_publication(tmp_path,monkeypatch):
    root,selection,originals,_=fixture(tmp_path);study,summary,heartbeat=originals['price']
    correct=deepcopy(heartbeat);heartbeat['summary_sha256']='0'*64;save(study/'heartbeat.json',heartbeat)
    waits=[]
    def pause(delay):
        waits.append(delay)
        if len(waits)==3:save(study/'heartbeat.json',correct)
    monkeypatch.setattr(d.time,'sleep',pause)
    result=d.read_study(root,selection,'price',NOW)
    assert result['status']=='current' and waits==[.5]*3
    assert result['generated_epoch']==summary['generated_epoch']


def test_generation_retry_is_bounded_and_does_not_wait_out_future_timestamp(tmp_path,monkeypatch):
    root,selection,originals,_=fixture(tmp_path);study,_,heartbeat=originals['price']
    heartbeat['summary_sha256']='0'*64;save(study/'heartbeat.json',heartbeat);waits=[]
    monkeypatch.setattr(d.time,'sleep',waits.append)
    with pytest.raises(d.SelectionError,match='summary_heartbeat_generation_mismatch'):
        d.read_study(root,selection,'price',NOW)
    assert waits==[.5]*11
    heartbeat['generated_epoch']=NOW+1;save(study/'heartbeat.json',heartbeat);waits.clear()
    with pytest.raises(d.SelectionError,match='stale_or_future_summary'):
        d.read_study(root,selection,'price',NOW)
    assert waits==[]


def test_completed_read_clock_prevents_start_clock_race_without_reclocking_source(tmp_path):
    root,selection,originals,_=fixture(tmp_path)
    result=d.read_study(root,selection,'joint',NOW-5,observed_clock=lambda:NOW)
    assert result['observed_epoch']==NOW and result['worker_observation']['age_sec']==2
    assert result['generated_epoch']==originals['joint'][1]['generated_epoch']


def test_collection_replaces_only_old_study_with_actual_native_observation(tmp_path):
    root,_,originals,write=fixture(tmp_path)
    h=originals['joint'][2];h.update(phase='research_collection',errors=3,last_error='retained_failure')
    slot=next(iter(originals['joint'][1]['rows'][0]['families'].values()))
    slot.update(status='warming',reason='observed_news_failure_generation:9',latest_forecast=None);write()
    joint=d.read_dashboard_sources(root,now_epoch=NOW)['joint']
    legacy={'observations':{'study':{'status':'stale','current':False,'age_sec':500000},
        'quote_stream':{'status':'current','current':True,'age_sec':1},
        'account':{'status':'current','current':True,'age_sec':2}}}
    before=deepcopy(legacy);result=d.project_collection_status(legacy,joint,now_epoch=NOW)
    study=result['observations']['study']
    assert study['current'] is True and study['age_sec']==2 and study['generated_epoch']==NOW-2
    assert study['phase']=='research_collection' and study['reported_cumulative_errors']==3
    assert study['last_error']=='retained_failure' and study['reason_counts']=={'observed_news_failure_generation:9':1}
    assert result['running'] is True and result['forecasting']['blocked'] is True
    assert legacy==before and result['observations']['account']==before['observations']['account']
    failed=d.project_collection_status(legacy,d.unavailable('joint','producer_reports_failure'),now_epoch=NOW)
    assert failed['running'] is False and failed['observations']['study']['current'] is False
    assert failed['observations']['study']['reason']=='producer_reports_failure'
    assert failed['observations']['study']['age_sec'] is None


def test_selected_paths_cannot_escape_or_use_legacy_feature_archive(tmp_path):
    root,selection,_,write=fixture(tmp_path)
    selection['features']['archive_path']='../outside/feature_observations_v2';write()
    assert d.read_dashboard_sources(root,now_epoch=NOW)['status']=='unavailable'
    selection['features']['archive_path']='data/oanda_training_manager/feature_observations_v1';write()
    assert d.read_dashboard_sources(root,now_epoch=NOW)['reason']=='feature_archive_version'


def test_feature_api_uses_selected_v2_archive_and_refuses_invalid_selection():
    from test_oanda_feature_move_dashboard_v1 import scope,payload
    env=scope(use_current_selection=True);calls=[]
    def reader(path,**kwargs):calls.append(path);return payload()
    selection={'features':{'archive_path':'data/oanda_training_manager/repair/feature_observations_v2'}}
    result=env['build_feature_move_response'](300,reader=reader,now_epoch=NOW,selection_reader=lambda *args:selection)
    assert result['status']=='available' and calls==[ROOT/'data/oanda_training_manager/repair/feature_observations_v2']
    def broken(*args):raise d.SelectionError('invalid_pointer')
    calls.clear();result=env['build_feature_move_response'](300,reader=reader,now_epoch=NOW,selection_reader=broken)
    assert result['status']=='unavailable' and calls==[]


def test_frontend_native_mode_bypasses_retired_ledger_and_expires_targets(tmp_path):
    node=shutil.which('node')
    if not node:pytest.skip('node unavailable')
    html=(ROOT/'oanda_main_signal_dashboard.html').read_text(encoding='utf-8')
    names=['jointForecastActivity','tableForecastActivity']
    functions=[]
    for name in names:
        start=html.index('    function '+name+'(')
        end=html.index('\n    ',start+8)
        if name=='jointForecastActivity':end=html.index('\n    function jointForecastCell',start)
        functions.append(html[start:end])
    script='\n'.join(functions)+"""
const now=Date.now()/1000;
const arm={family:'ridge_price_news_v1',publication_epoch:now-1,reference_epoch:now-100,target_epoch:now+3500};
const data={operational_dashboard:{selected:true},joint_v3_ledger_observation:{},joint_price_news_forecasts:{study_version:'joint_v7',status:'current',research_only:true,can_place_orders:false,can_promote:false,generated_epoch:now-2,rows:[{instrument:'EUR_USD',observed_epoch:now-2,active_forecasts:[arm]}]}};
if(tableForecastActivity(data).forecast!==1)throw Error('native hidden');
arm.target_epoch=now-1;if(tableForecastActivity(data).forecast!==0)throw Error('expired visible');
console.log('native selection and expiry passed');
"""
    run=subprocess.run([node,'-e',script],capture_output=True,text=True,timeout=15)
    assert run.returncode==0,run.stderr
