"""Independent family display contracts; synthetic files, no broker or runtime."""
import copy
import hashlib
import json
from pathlib import Path
import shutil
import socket
import sqlite3
import subprocess

import pytest
import oanda_practice_live_dashboard as dashboard

NOW=1788800400.0
STATE='probabilistic_state_space'
RIDGE='ridge_return_repaired'
FLAGS={'research_only':True,**{key:False for key in ('can_place_orders','can_promote','can_authorize','account_eligible','proof_eligible','historical_rows_imported')}}

def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()

@pytest.fixture(autouse=True)
def isolated(monkeypatch):
    with dashboard._PAIR_SUMMARY_CACHE_LOCK:dashboard._PAIR_SUMMARY_CACHE.clear()
    def forbidden(*args,**kwargs):raise AssertionError('No databases or network in dashboard fixtures')
    monkeypatch.setattr(sqlite3,'connect',forbidden)
    monkeypatch.setattr(socket.socket,'connect',forbidden)

@pytest.fixture
def publication(tmp_path):
    project=tmp_path/'trad';root=project/'data/oanda_training_manager';root.mkdir(parents=True)
    raw=b'# frozen fixture\n'
    bindings={name:hashlib.sha256(raw).hexdigest() for name in dashboard._PAIR_V2_FORECAST_SOURCE_BINDINGS}
    for name in bindings:(project/name).write_bytes(raw)
    pairs={}
    for pair in ('EUR_USD','GBP_USD','USD_JPY'):
        pip=.01 if pair.endswith('JPY') else .0001
        families={}
        for family in (STATE,RIDGE):
            contract={**FLAGS,'schema_version':'causal_pair_family_ledger_v2_20260907','instrument':pair,'family':family,'pip_size':pip,
                      'cohorts':{family:pair+'.'+family},'source_bindings':bindings,'numeric_model_source_sha256':bindings['oanda_pair_local_models_v2.py'],
                      'dependency_versions':{'python':'fixture'},'input_timeframe':'M1','horizon_sec':3600}
            families[family]={'contract':contract,'contract_sha256':digest(contract)}
        pairs[pair]={'pip_size':pip,'families':families}
    registry={**FLAGS,'schema_version':'pair_local_forecast_registry_v2_20260907','registry_id':'pair_local_forecast_study_v2_20260907',
              'collection_enabled':True,'source_bindings':bindings,'dependency_versions':{'python':'fixture'},'pairs':pairs}
    rows=[]
    for pair,entry in pairs.items():
        slots={}
        for family,registered in entry['families'].items():
            blocked=family==RIDGE and pair=='EUR_USD'
            diag={'current_real_prices':20,'current_real_returns':19,'current_span_sec':1800,'session_start_epoch':NOW-7200,
                  'current_window_start_epoch':NOW-2160,'retained_real_rows':100,'mature_exact_h1_training_rows':10 if blocked else 30,
                  'required_ridge_training_rows':24,'maximum_current_gap_sec':180,'ready':not blocked,'status':'blocked' if blocked else 'ready',
                  'reasons':['mature_exact_h1_training_rows:10<24'] if blocked else []}
            active=pair!='USD_JPY' and not blocked
            forecast=None
            if active:
                reference=NOW-100 if family==STATE else NOW-200
                arm={'family':family,'cohort_id':registered['contract']['cohorts'][family],'instrument':pair,'pip_size':entry['pip_size'],
                     'horizon_sec':3600,'reference_epoch':reference,'reference_mid':'1.16234567890123456789','issued_epoch':reference+5,
                     'target_epoch':reference+3600,'side':1,'probability_up':.65,'predicted_return_bps':3.75}
                forecast={'decision_id':pair+'.'+family,'instrument':pair,'family':family,'horizon_sec':3600,
                          'publication_epoch':reference+6,'consumption_epoch':reference+7,'publication_verified_epoch':NOW-4,
                          'reference_epoch':reference,'reference_available_epoch':reference+1,'issued_epoch':reference+5,'target_epoch':reference+3600,
                          'forecast_sha256':'e'*64,'publication_verified':True,'consumption_verified':True,'forecasts':[arm]}
                seal_forecast(forecast)
            slots[family]={'status':'forecast' if active else 'warming' if blocked else 'unavailable','reason':'forecast' if active else 'mature_exact_h1_training_rows:10<24' if blocked else 'no_fresh_quote',
                           'observed_epoch':NOW-3,'activated_epoch':NOW-1000,'contract_sha256':registered['contract_sha256'],'cohort_id':registered['contract']['cohorts'][family],
                           'current_readiness':{'observed_epoch':NOW-250,'status':'blocked' if blocked else 'ready','reason':'|'.join(diag['reasons']),'diagnostics':diag},
                           'last_attempt':{'epoch':NOW-500,'reason':'historical_attempt_reason','attempt_id':pair+'.'+family,'bucket':100},'latest_forecast':forecast,
                           'counts':{key:(1 if active else 0) for key in ('quotes','attempts','diagnostics','inputs','forecasts','publication','consumption','entries','outcomes','exclusions')}}
        rows.append({'instrument':pair,'pip_size':entry['pip_size'],'families':slots})
    summary={**FLAGS,'schema_version':'pair_local_forecast_summary_v2_20260907','registry_sha256':digest(registry),'generated_epoch':NOW-2,'rows':rows}
    heartbeat={**FLAGS,'schema_version':'pair_local_forecast_heartbeat_v2_20260907','registry_sha256':digest(registry),'generated_epoch':NOW-1}
    config=project/'config/pair_local_forecast_study_v2_20260907.json';summary_path=root/'pair_local_forecast_study_v2/summary.json';hb_path=summary_path.with_name('heartbeat.json')
    def save():
        summary['registry_sha256']=heartbeat['registry_sha256']=digest(registry)
        summary['payload_sha256']=digest({k:v for k,v in summary.items() if k!='payload_sha256'})
        heartbeat['summary_sha256']=digest(summary)
        for path,value in ((config,registry),(summary_path,summary),(hb_path,heartbeat)):
            path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(value),encoding='utf-8')
    save()
    return root,registry,summary,heartbeat,save

def seal_forecast(forecast):
    forecast['publication_receipt_sha256']=digest({'epoch':forecast['publication_epoch'],'forecast_sha':forecast['forecast_sha256']})
    forecast['consumer_receipt_sha256']=digest({'epoch':forecast['consumption_epoch'],'forecast_sha':forecast['forecast_sha256'],'publication_sha':forecast['publication_receipt_sha256']})

def read(fixture,now=NOW):return dashboard.summarize_pair_local_forecasts_v2(fixture[0],now_epoch=now)
def slot(fixture,pair=0,family=STATE):return fixture[2]['rows'][pair]['families'][family]

def test_single_model_remains_active_while_ridge_needs_training(publication):
    result=read(publication)
    assert result['status']=='current',result
    assert result['counts']=={'forecast':2,'unavailable':1}
    assert result['family_counts'][STATE]['forecast']==2
    assert result['family_counts'][RIDGE]['forecast']==1
    first=result['rows'][0]
    assert [arm['family'] for arm in first['active_forecasts']]==[STATE]
    assert first['families'][RIDGE]['status']=='warming'
    assert '10/24 mature H1 labels' in first['families'][RIDGE]['current_readiness']['reason_label']
    assert first['families'][RIDGE]['current_readiness']['observed_epoch']==NOW-250
    assert first['families'][RIDGE]['last_attempt']['epoch']==NOW-500
    assert result['ledger_counts']['publication']==3

def test_each_family_preserves_own_original_reference_and_target(publication):
    arms=read(publication)['rows'][1]['active_forecasts']
    assert {a['target_epoch'] for a in arms}=={NOW+3500,NOW+3400}
    assert all(a['target_epoch']-a['reference_epoch']==3600 for a in arms)
    assert all(a['reference_mid']=='1.16234567890123456789' for a in arms)

@pytest.mark.parametrize('field,value', [('publication_verified',False),('consumption_verified',False),('publication_receipt_sha256','a'*64),
    ('consumer_receipt_sha256','a'*64),('consumption_epoch',NOW+1),('publication_verified_epoch',NOW-100),
    ('issued_epoch',NOW-101),('reference_available_epoch',NOW-1001),('family',RIDGE),('target_epoch',NOW+3501),('forecasts',[])])
def test_invalid_family_never_hides_valid_sibling(publication,field,value):
    slot(publication,1)[ 'latest_forecast'][field]=value;publication[4]()
    row=read(publication)['rows'][1]
    assert row['families'][STATE]['status']=='unavailable'
    assert [arm['family'] for arm in row['active_forecasts']]==[RIDGE]

@pytest.mark.parametrize('field,value',[('family',RIDGE),('pip_size',.01),('cohort_id','other'),('target_epoch',NOW+3501),
    ('side',True),('probability_up',.99),('reference_mid',0),('predicted_return_bps','NaN')])
def test_invalid_arm_is_isolated(publication,field,value):
    slot(publication,1)['latest_forecast']['forecasts'][0][field]=value;publication[4]()
    assert [arm['family'] for arm in read(publication)['rows'][1]['active_forecasts']]==[RIDGE]

def test_missing_family_slot_is_explicit_and_sibling_remains(publication):
    del publication[2]['rows'][1]['families'][RIDGE];publication[4]()
    row=read(publication)['rows'][1]
    assert row['families'][RIDGE]['reason']=='missing_registered_family_slot'
    assert len(row['active_forecasts'])==1

def test_readiness_clock_is_input_observation_not_last_attempt_or_heartbeat(publication):
    value=slot(publication,0,RIDGE)
    value['last_attempt']['epoch']=NOW-700
    publication[4]()
    parsed=read(publication)['rows'][0]['families'][RIDGE]
    assert parsed['current_readiness']['observed_epoch']==NOW-250
    assert parsed['last_attempt']['epoch']==NOW-700
    assert parsed['observed_epoch']==NOW-3

def test_stale_input_does_not_retime_or_remove_unexpired_forecast(publication):
    value=slot(publication)
    value['current_readiness']['observed_epoch']=NOW-1000
    value['current_readiness']['diagnostics']['current_window_start_epoch']=NOW-3000
    publication[4]()
    row=read(publication)['rows'][0]
    assert row['families'][STATE]['current_readiness']['status']=='unavailable'
    assert row['active_forecasts'][0]['target_epoch']==NOW+3500

def test_stale_readiness_cannot_count_as_ready_or_warming_without_forecast(publication):
    value=slot(publication,0,RIDGE)
    value['current_readiness']['observed_epoch']=NOW-1000
    value['current_readiness']['diagnostics']['current_window_start_epoch']=NOW-3000
    publication[4]()
    row=read(publication)['rows'][0]
    assert row['families'][RIDGE]['status']=='unavailable'
    assert row['families'][RIDGE]['reason']=='stale_pair_input'
    assert row['status']=='forecast'

@pytest.mark.parametrize('case',['future_observation','future_source','counts','unavailable_missing_clock'])
def test_readiness_validation_and_initial_empty_state(publication,case):
    value=slot(publication)
    if case=='future_observation':value['current_readiness']['observed_epoch']=NOW+1
    elif case=='future_source':value['current_readiness']['diagnostics']['current_window_start_epoch']=NOW-100
    elif case=='counts':value['current_readiness']['diagnostics']['current_real_returns']=50
    else:value['current_readiness']={'observed_epoch':None,'status':'unavailable','reason':'awaiting_pair_input_read','diagnostics':{}}
    publication[4]();parsed=read(publication)['rows'][0]['families'][STATE]
    assert parsed['status']==('forecast' if case=='unavailable_missing_clock' else 'unavailable')

def test_cached_generation_requires_exact_heartbeat_binding_and_original_clocks(publication):
    first=read(publication);old_hb=copy.deepcopy(publication[3]);old_raw=(publication[0]/'pair_local_forecast_study_v2/summary.json').read_bytes()
    publication[2]['generated_epoch']=NOW-.5;publication[3]['generated_epoch']=NOW-.25;publication[4]()
    (publication[0]/'pair_local_forecast_study_v2/heartbeat.json').write_text(json.dumps(old_hb))
    result=read(publication)
    assert result['status']=='current'
    assert result['summary_sha256']==first['summary_sha256']
    assert result['retained_generation']['heartbeat_summary_sha256']==first['summary_sha256']
    assert result['rows']==first['rows']
    assert result['publication_read_failures'][0]['reason']=='pair_summary_generation_mismatch'
    assert any(entry[2]==old_raw for entries in dashboard._PAIR_SUMMARY_CACHE.values() for entry in entries)

@pytest.mark.parametrize('case',['tamper','future','stale','wrong_heartbeat','source_changed'])
def test_cache_cannot_hide_true_invalidity(publication,case):
    read(publication)
    path=publication[0]/'pair_local_forecast_study_v2/summary.json'
    if case=='tamper':
        raw=json.loads(path.read_text());raw['rows'][0]['pip_size']=.01;path.write_text(json.dumps(raw))
    elif case=='future':publication[2]['generated_epoch']=NOW+1;publication[4]()
    elif case=='stale':return_assert=read(publication,NOW+100);assert return_assert['status']=='unavailable';return
    elif case=='wrong_heartbeat':
        hb=copy.deepcopy(publication[3]);hb['summary_sha256']='f'*64;path.with_name('heartbeat.json').write_text(json.dumps(hb))
    else:(publication[0].parent.parent/'oanda_pair_local_models_v2.py').write_text('# changed')
    assert read(publication)['status']=='unavailable'

@pytest.mark.parametrize('filename,limit',[('summary.json',1024*1024),('heartbeat.json',65536)])
def test_v2_publication_read_size_bound(publication,filename,limit):
    (publication[0]/'pair_local_forecast_study_v2'/filename).write_bytes(b' '*(limit+1))
    assert read(publication)['reason']=='pair_summary_size_limit'

def test_clock_is_observed_after_retained_bytes(publication,monkeypatch):
    clocks=iter([NOW-4,NOW]);monkeypatch.setattr(dashboard.time,'time',lambda:next(clocks))
    assert dashboard.summarize_pair_local_forecasts_v2(publication[0])['status']=='current'

@pytest.mark.parametrize('errors,expected',[(0,0),(3,3),(False,None),(-1,None),('0',None)])
def test_worker_errors_come_from_current_bound_heartbeat_or_remain_unknown(publication,errors,expected):
    publication[3].update(errors=errors,heartbeat_publication_errors=2)
    publication[4]()
    observed=read(publication)['worker_observation']
    assert observed['errors']==expected
    assert observed['heartbeat_publication_errors']==2
    assert observed['generated_epoch']==NOW-1

@pytest.mark.parametrize('case',['missing','valid','wrong_hash','future','malformed'])
def test_primary_selection_requires_explicit_bound_activation(publication,case):
    v2=read(publication);legacy={'status':'current','rows':[{'instrument':'LEGACY'}]}
    selector={'schema_version':'pair_forecast_primary_selection_v1_20260907','selected':'v2','registry_sha256':v2['registry_sha256'],'activated_epoch':NOW-1}
    if case=='wrong_hash':selector['registry_sha256']='f'*64
    if case=='future':selector['activated_epoch']=NOW+1
    path=publication[0].parent.parent/'config/pair_forecast_primary_current.json'
    if case!='missing':path.write_text('[]' if case=='malformed' else json.dumps(selector))
    result=dashboard.select_primary_pair_forecasts(publication[0],legacy,v2,now_epoch=NOW)
    if case=='missing':assert result['study_version']==1
    elif case=='valid':assert result['study_version']==2 and result['rows']==v2['rows']
    else:assert result['status']=='unavailable' and result['rows']==[] and result['study_version']==2

def collection_projection(publication,now=NOW):
    primary=read(publication)
    primary['primary_selection']={'selected':'v2','registry_sha256':primary['registry_sha256'],'activated_epoch':NOW-1000}
    legacy={'selected_study':'eurusd_v1','expected_model_count':2,'label':'Legacy missing 61 EUR/USD minutes',
            'model_attempts':{'counts':{'publication':19}},'forecasting':{'state':'blocked_warmup','blocked':True},
            'observations':{'study':{'status':'stale','current':False,'reported_cumulative_errors':19},
                            'quote_stream':{'status':'current','current':True},'account':{'status':'current','current':True}}}
    return legacy,primary,dashboard.project_primary_collection_status(legacy,primary,now_epoch=now)

def test_primary_collection_api_uses_v2_coverage_and_preserves_independent_feed(publication):
    publication[3].update(errors=0,heartbeat_publication_errors=1);publication[4]()
    legacy,primary,result=collection_projection(publication)
    assert result['selected_study']=='pair_family_v2'
    assert result['expected_model_count']==6
    assert result['status']=='running' and result['running'] is True
    assert result['forecasting']['state']=='published' and result['forecasting']['blocked'] is False
    assert result['forecasting']['pairs_with_forecast']==2
    assert result['forecasting']['family_forecast_counts']=={STATE:2,RIDGE:1}
    assert result['observations']['study']['reported_cumulative_errors']==0
    assert result['observations']['study']['reported_cumulative_heartbeat_publication_errors']==1
    assert result['observations']['quote_stream']==legacy['observations']['quote_stream']
    assert result['model_attempts']['counts']['publication']==3
    assert '61' not in json.dumps(result)
    assert result['warmup']['required_common_m1_bars'] is None
    assert legacy['forecasting']['state']=='blocked_warmup'

@pytest.mark.parametrize('case',['stale_summary','future_summary','stale_worker','future_worker','stale_projection','invalid_selector','unavailable','inert_flag'])
def test_primary_collection_never_falls_back_when_v2_is_unavailable(publication,case):
    legacy,primary,_=collection_projection(publication)
    if case=='stale_summary':primary['generated_epoch']=NOW-100
    elif case=='future_summary':primary['generated_epoch']=NOW+1
    elif case=='stale_worker':primary['worker_observation']['generated_epoch']=NOW-100
    elif case=='future_worker':primary['worker_observation']['generated_epoch']=NOW+1
    elif case=='stale_projection':primary['observed_epoch']=NOW-100
    elif case=='invalid_selector':primary['primary_selection']['registry_sha256']='f'*64
    elif case=='inert_flag':primary['can_place_orders']=True
    else:primary['status']='unavailable'
    result=dashboard.project_primary_collection_status(legacy,primary,now_epoch=NOW)
    assert result['selected_study']=='pair_family_v2'
    assert result['running'] is False and result['status']=='unavailable'
    assert result['forecasting']['state']=='unavailable'
    assert result['observations']['study']['reported_cumulative_errors'] is None
    assert result['model_attempts']['counts']=={}
    assert '61' not in json.dumps(result)

def test_v1_collection_is_unchanged_until_v2_selected():
    legacy={'selected_study':'eurusd_v1','forecasting':{'state':'blocked_warmup'}}
    assert dashboard.project_primary_collection_status(legacy,{'study_version':1},now_epoch=NOW) is legacy

def test_collection_current_forecast_count_rechecks_original_target(publication):
    legacy,primary,_=collection_projection(publication)
    primary['rows'][0]['active_forecasts'][0]['target_epoch']=NOW
    result=dashboard.project_primary_collection_status(legacy,primary,now_epoch=NOW)
    assert result['forecasting']['pairs_with_forecast']==1
    assert result['forecasting']['active_family_forecasts']==2

def test_collection_account_unavailability_is_separate_from_worker(publication):
    legacy,primary,_=collection_projection(publication)
    legacy['observations']['account']={'status':'stale','current':False}
    result=dashboard.project_primary_collection_status(legacy,primary,now_epoch=NOW)
    assert result['running'] is True
    assert result['observations']['account']['current'] is False
    assert 'account:stale' in result['unavailable_observations']

def render(data,now=NOW):
    node=shutil.which('node') or r'C:\Users\zmoor\.cache\codex-runtimes\codex-primary-runtime\dependencies\node\bin\node.exe'
    html=Path(dashboard.__file__).with_name('oanda_main_signal_dashboard.html').read_text(encoding='utf-8')
    functions=html[html.index('    let marketOverviewWindow='):html.index('    function renderAvailableSignals(data)')]
    activity=html[html.index('    function pairCoverageActivity(data)'):html.index('    function supplementalDiagnosticHtml(data)')]
    harness="const elements={};const $=id=>elements[id]??={innerHTML:''};const num=(v,d=2)=>Number(v).toFixed(d);const signed=(v,d=2)=>(Number(v)>=0?'+':'')+Number(v).toFixed(d);const cls=()=>'';const esc=v=>String(v??'').replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));\n"
    harness+=f'Date.now=()=>{now*1000};\n'+functions+activity+'\nconst data='+json.dumps(data)+";renderMarketOverview(data);process.stdout.write(JSON.stringify({html:elements['market-overview'].innerHTML,activity:pairCoverageActivity(data)}));"
    return json.loads(subprocess.run([node],input=harness,capture_output=True,text=True,encoding='utf-8',check=True,timeout=15).stdout)

def test_renderer_independent_family_cost_readiness_and_historical_attempt(publication):
    data={'pair_local_forecasts':read(publication),'market_overview':{'generated_epoch':NOW,'rows':[
        {'instrument':'EUR_USD','status':'current','quote_age_sec':3,'quote_epoch':NOW-3,'bid':1.1623,'ask':1.1625,'mid':1.1624,'spread_pips':2,'changes':{'15m':{'return_bps':1,'price_change_pips':1.16}}}]}}
    result=render(data);html=result['html']
    for phrase in ('State space: Up','Ridge: Waiting for model inputs','10/24 mature H1 labels','Readiness input observed','Last attempt','Expected move +3.75 bps','before costs','Current quoted spread: 1.72 bps','uncalibrated p(up)','1 model predictions · H1','2 model predictions · H1'):
        assert phrase in html,phrase
    assert 'Buy' not in html and 'Sell' not in html and 'Latest readiness observation' not in html
    assert result['activity']['family_forecast_counts']=={STATE:2,RIDGE:1}
    assert result['activity']['no_quote']==1
    stale=render(data,NOW+91)['html'];assert 'State space: Up' not in stale

def test_renderer_never_fills_v2_missing_forecasts_with_legacy_eurusd(publication):
    result=read(publication);result.update(status='unavailable')
    legacy={'status':'in_progress','instrument':'EUR_USD','publication_epoch':NOW-10,'target_epoch':NOW+3600,'forecasts':[{'family':STATE,'side':1,'predicted_return_bps':99}]}
    html=render({'pair_local_forecasts':result,'collection_status':{'model_attempts':{'latest_published_forecast':legacy}}})['html']
    assert 'State space: Up' not in html and '99.00 bps' not in html

def test_independent_h1_target_expiry_keeps_other_family_active(publication):
    value=slot(publication,1,STATE);value['activated_epoch']=NOW-10000
    forecast=value['latest_forecast'];reference=NOW-3599
    forecast.update(reference_epoch=reference,reference_available_epoch=reference+1,issued_epoch=reference+5,
                    publication_epoch=reference+6,consumption_epoch=reference+7,target_epoch=NOW+1)
    forecast['forecasts'][0].update(reference_epoch=reference,issued_epoch=reference+5,target_epoch=NOW+1)
    seal_forecast(forecast);publication[4]()
    before=read(publication)
    result=render({'pair_local_forecasts':before},NOW+2)
    assert result['activity']['family_forecast_counts']=={STATE:1,RIDGE:1}
    assert 'H1 target elapsed; next forecast pending' in result['html']
    after=read(publication,NOW+2)
    assert [arm['family'] for arm in after['rows'][1]['active_forecasts']]==[RIDGE]
