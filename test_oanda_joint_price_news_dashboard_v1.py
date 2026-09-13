"""Joint display must bind retained publications, source clocks and ablations."""
import copy
import hashlib
import json
from pathlib import Path
import socket
import sqlite3
import pytest
import oanda_practice_live_dashboard as dashboard
import test_oanda_pair_forecast_dashboard_v2 as baseline

NOW=baseline.NOW;FAMILY='ridge_price_news_v1'

@pytest.fixture(autouse=True)
def isolated(monkeypatch):
    with dashboard._PAIR_SUMMARY_CACHE_LOCK:dashboard._PAIR_SUMMARY_CACHE.clear()
    def forbidden(*args,**kwargs):raise AssertionError('Dashboard fixture must not open databases or network')
    monkeypatch.setattr(sqlite3,'connect',forbidden);monkeypatch.setattr(socket.socket,'connect',forbidden)

@pytest.fixture
def publication(tmp_path):
    root,registry,summary,hb,_=baseline.publication.__wrapped__(tmp_path)
    project=root.parent.parent;raw=b'# registered joint display fixture\n'
    bindings={name:hashlib.sha256(raw).hexdigest() for name in dashboard._JOINT_NEWS_FORECAST_SOURCE_BINDINGS}
    for name in bindings:(project/name).write_bytes(raw)
    registry.update(schema_version='joint_price_news_registry_v2_20260907',registry_id='joint_price_news_study_v2_20260907',source_bindings=bindings)
    for pair,entry in registry['pairs'].items():
        registered=entry['families'][baseline.STATE];contract=registered['contract']
        contract.update(schema_version='causal_joint_price_news_ledger_v1_20260907',family=FAMILY,cohorts={FAMILY:pair+'.'+FAMILY},source_bindings=bindings,numeric_model_source_sha256=bindings['oanda_joint_price_news_models_v1.py'])
        registered['contract_sha256']=baseline.digest(contract);entry['families']={FAMILY:registered}
    for row in summary['rows']:
        pair=row['instrument'];registered=registry['pairs'][pair]['families'][FAMILY];slot=row['families'][baseline.STATE]
        slot.update(contract_sha256=registered['contract_sha256'],cohort_id=registered['contract']['cohorts'][FAMILY])
        diag={'current_real_prices':20,'current_real_returns':19,'current_span_sec':1800,'mature_exact_h1_training_rows':60,
              'required_joint_training_rows':48,'nonzero_news_context_training_rows':30,'distinct_news_context_patterns':12,
              'vetted_news_training_rows':2,'ready':True,'status':'ready','reasons':[]}
        slot['current_readiness']={'observed_epoch':NOW-250,'status':'ready','reason':'','diagnostics':diag,
            'price_bar_close_epoch':NOW-300,'news_evidence_epoch':NOW-280,'news_expires_epoch':NOW+20}
        forecast=slot['latest_forecast']
        if forecast:
            forecast['family']=FAMILY;arm=forecast['forecasts'][0];arm.update(family=FAMILY,cohort_id=registered['contract']['cohorts'][FAMILY])
            expected=float(arm['predicted_return_bps'])*float(arm['reference_mid'])/10000/float(arm['pip_size'])
            arm.update(news_capture_sha256='f'*64,news_evidence_epoch=NOW-280,news_generated_epoch=NOW-270,
                news_first_observed_epoch=NOW-250,news_available_epoch=NOW-250,news_expires_epoch=NOW+20,
                input_contributions={'matched_price_only_expected_pips':expected-.2,'neutral_news_ablation_expected_pips':expected-.1,
                    'news_ablation_difference_pips':.1,'training_rows':60,'nonzero_news_context_training_rows':30,'vetted_news_training_rows':2,
                    'news_feature_names':['context_balance','context_volume_log','context_signed_fraction','context_mean_age_hours','vetted_balance','vetted_volume_log','vetted_conflict_fraction','vetted_remaining_hours'],
                    'current_news_features':[.4,.69,1.,.2,0,0,0,0],
                    'news_ablation_scope':'same_fitted_model_with_documented_neutral_news_features_not_causal_impact',
                    'training_scope':'retrospective_original_member_archive_not_prior_prospective_performance',
                    'probability_scope':'uncalibrated_model_estimate_not_verified_accuracy'})
        row['families']={FAMILY:slot}
    summary['schema_version']='joint_price_news_forecast_summary_v2_20260907';hb['schema_version']='joint_price_news_forecast_heartbeat_v2_20260907'
    def save():
        summary['registry_sha256']=hb['registry_sha256']=baseline.digest(registry)
        summary['payload_sha256']=baseline.digest({k:v for k,v in summary.items() if k!='payload_sha256'})
        hb['summary_sha256']=baseline.digest(summary)
        for path,value in ((project/'config/joint_price_news_study_v2_20260907.json',registry),(root/'joint_price_news_study_v2/summary.json',summary),(root/'joint_price_news_study_v2/heartbeat.json',hb)):
            path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(value),encoding='utf-8')
    save();return root,registry,summary,hb,save

def read(fixture,now=NOW):return dashboard.summarize_joint_price_news_forecasts(fixture[0],now_epoch=now)
def slot(fixture,index=0):return fixture[2]['rows'][index]['families'][FAMILY]
def arm(fixture,index=0):return slot(fixture,index)['latest_forecast']['forecasts'][0]

def test_verified_single_joint_model_preserves_original_clocks_and_learned_ablation(publication):
    result=read(publication);assert result['status']=='current',result
    assert result['counts']=={'forecast':2,'unavailable':1}
    actual=result['rows'][0]['active_forecasts'][0]
    assert actual['family']==FAMILY and actual['target_epoch']==NOW+3500
    assert actual['news_first_observed_epoch']==NOW-250
    assert actual['input_contributions']['news_ablation_difference_pips']==.1
    assert '60/48 mature H1 labels' in result['rows'][0]['families'][FAMILY]['current_readiness']['reason_label']

def test_expired_current_news_does_not_erase_valid_original_h1_forecast(publication):
    value=slot(publication);value['current_readiness']['news_expires_epoch']=NOW-1
    source=arm(publication)
    source.update(news_evidence_epoch=NOW-350,news_generated_epoch=NOW-340,news_first_observed_epoch=NOW-330,news_available_epoch=NOW-330,news_expires_epoch=NOW-50)
    publication[4]();result=read(publication)['rows'][0]
    assert result['active_forecasts'][0]['news_expires_epoch']==NOW-50
    assert result['active_forecasts'][0]['target_epoch']==NOW+3500
    assert result['families'][FAMILY]['current_readiness']['status']=='unavailable'

@pytest.mark.parametrize('field,value',[('news_evidence_epoch',NOW+1),('news_generated_epoch',NOW-400),('news_first_observed_epoch',NOW+1),
    ('news_available_epoch',NOW-249),('news_expires_epoch',NOW-96),('news_capture_sha256','invalid'),('news_expires_epoch',NOW+999)])
def test_invalid_original_news_publication_withholds_only_affected_pair(publication,field,value):
    arm(publication)[field]=value;publication[4]();result=read(publication)
    assert result['rows'][0]['active_forecasts']==[]
    assert result['rows'][1]['active_forecasts']

@pytest.mark.parametrize('field,value',[('news_ablation_difference_pips',99),('matched_price_only_expected_pips','NaN'),
    ('news_ablation_scope','causal_news_impact'),('training_scope','verified_prospective_success'),('probability_scope','verified_accuracy'),
    ('training_rows',True),('nonzero_news_context_training_rows',11),('current_news_features',[0]),('news_feature_names',[])])
def test_invalid_learned_contribution_cannot_be_displayed(publication,field,value):
    arm(publication)['input_contributions'][field]=value;publication[4]()
    assert read(publication)['rows'][0]['active_forecasts']==[]

@pytest.mark.parametrize('field,value',[('price_bar_close_epoch',NOW-901),('news_evidence_epoch',NOW-301),('news_expires_epoch',NOW-1),('news_evidence_epoch',None)])
def test_actual_current_input_expiry_withholds_ready_slot_without_forecast(publication,field,value):
    source=slot(publication,2);source.update(status='ready',reason='ready');source['current_readiness'][field]=value
    publication[4]();result=read(publication)['rows'][2]
    assert result['status']=='unavailable' and result['active_forecasts']==[]

def test_crossed_generation_uses_only_exact_fresh_heartbeat_bound_bytes(publication):
    old=read(publication);old_sha=old['summary_sha256'];old_target=old['rows'][0]['active_forecasts'][0]['target_epoch']
    publication[2]['generated_epoch']+=1;publication[4]()
    publication[3]['summary_sha256']=old_sha
    (publication[0]/'joint_price_news_study_v2/heartbeat.json').write_text(json.dumps(publication[3]),encoding='utf-8')
    current=read(publication)
    assert current['status']=='current' and current['retained_generation']['summary_sha256']==old_sha
    assert current['rows'][0]['active_forecasts'][0]['target_epoch']==old_target

def test_source_tamper_stale_and_wrong_registry_never_fallback_to_price_only(publication):
    project=publication[0].parent.parent;(project/'oanda_joint_price_news_models_v1.py').write_text('changed',encoding='utf-8')
    result=read(publication)
    assert result['status']=='unavailable' and not any(r.get('active_forecasts') for r in result['rows'])

def market_data(publication):
    return {'joint_price_news_forecasts':read(publication),'market_overview':{'generated_epoch':NOW,'rows':[
        {'instrument':'EUR_USD','status':'current','quote_age_sec':3,'quote_epoch':NOW-3,'bid':1.1623,'ask':1.1625,'mid':1.1624,'spread_pips':2,
         'changes':{'15m':{'return_bps':1,'price_change_pips':1.16}}}]}}

def test_renderer_displays_correct_additive_model_terms_and_separate_price_comparison(publication):
    html=baseline.render(market_data(publication))['html']
    for phrase in ('Combined: Up','Same model without news','News adjustment +0.10p','Separate matched price-only estimate:',
                   'not causal news impact','Other price-only models','Current quoted spread: 1.72 bps','2 combined · 0 price-only · 1 unavailable',
                   'Price + news · H1','not verified accuracy','60 retrospective training rows','<th>Forecast</th>','Position'):
        assert phrase in html,phrase
    assert 'Buy' not in html and 'Sell' not in html
    assert 'Same model without news +4.26p' in html
    assert 'Separate matched price-only estimate: +4.16p' in html

def test_client_stale_summary_does_not_display_combined_prediction(publication):
    data=market_data(publication);html=baseline.render(data,NOW+91)['html']
    assert 'Combined: Up' not in html and 'Combined forecast unavailable' in html

def test_client_comparison_collector_v1_cannot_enter_primary_combined_field(publication):
    data=market_data(publication);data['joint_price_news_forecasts']['study_version']='joint_v1'
    html=baseline.render(data)['html']
    assert 'Combined: Up' not in html and 'Combined forecast unavailable' in html

def test_client_rechecks_original_issue_news_clock(publication):
    data=market_data(publication);data['joint_price_news_forecasts']['rows'][0]['active_forecasts'][0]['news_available_epoch']=NOW+1
    html=baseline.render(data)['html']
    assert html.count('Combined: Up')==1

def test_client_expired_readiness_does_not_masquerade_as_missing_training(publication):
    source=slot(publication,2);source.update(status='ready',reason='ready');publication[4]()
    html=baseline.render(market_data(publication),NOW+21)['html']
    assert 'Current price or news input unavailable' in html
    assert html.count('Combined: Up')==2

def test_forecast_and_readiness_text_is_escaped(publication):
    source=slot(publication,2);source['last_attempt']['reason']='<img src=x onerror=alert(1)>';publication[4]()
    html=baseline.render(market_data(publication))['html']
    assert '<img src=x' not in html and '&lt;img src=x' in html

def test_primary_joint_collection_preserves_price_only_comparison_scope(publication):
    comparison={'selected_study':'pair_family_v2','running':True,'observations':{'quote_stream':{'current':True,'status':'current'},'account':{'current':False,'status':'stale'}}}
    result=dashboard.project_joint_collection_status(comparison,read(publication),now_epoch=NOW)
    assert result['selected_study']=='joint_price_news_v2' and result['running'] is True
    assert result['expected_model_count']==3 and result['forecasting']['pairs_with_forecast']==2
    assert result['model_attempts']['scope']=='joint_price_news_v2_ledgers_only'
    assert result['observations']['quote_stream']['current'] is True and result['observations']['account']['current'] is False
    assert comparison['selected_study']=='pair_family_v2' and comparison['running'] is True

@pytest.mark.parametrize('case',['stale','future','worker_stale','old_version','missing_rows','source_unavailable','inert_flag'])
def test_primary_joint_collection_never_uses_healthy_price_only_as_fallback(publication,case):
    comparison={'running':True,'selected_study':'pair_family_v2'};joint=read(publication)
    if case=='stale':joint['generated_epoch']=NOW-91
    elif case=='future':joint['observed_epoch']=NOW+1
    elif case=='worker_stale':joint['worker_observation']['generated_epoch']=NOW-91
    elif case=='old_version':joint['study_version']='joint_v1'
    elif case=='missing_rows':joint['rows']=[]
    elif case=='source_unavailable':joint['status']='unavailable'
    else:joint['can_place_orders']=True
    result=dashboard.project_joint_collection_status(comparison,joint,now_epoch=NOW)
    assert result['running'] is False and result['selected_study']=='joint_price_news_v2'
    assert result['model_attempts']['counts']=={} and result['forecasting']['pairs_with_forecast']==0

def test_joint_v2_identity_does_not_accept_comparison_collector_v1_registry(publication):
    publication[1]['registry_id']='joint_price_news_study_v1_20260907';publication[4]()
    assert read(publication)['status']=='unavailable'

def test_empty_unavailable_joint_source_cannot_fall_through_to_price_header(publication):
    import test_oanda_news_pair_dashboard as news_tests
    html=Path(dashboard.__file__).with_name('oanda_main_signal_dashboard.html').read_text(encoding='utf-8')
    functions=html[html.index('    function jointForecastActivity(data)'):html.index('    function renderMarketOverview(data)')]
    functions+=html[html.index('    function collectionHeaderState(data)'):html.index('    function supplementalDiagnosticHtml(data)')]
    data={'joint_price_news_forecasts':{'status':'unavailable','rows':[]},'pair_local_forecasts':{'status':'current','study_version':2,
        'generated_epoch':NOW,'research_only':True,'can_place_orders':False,'can_promote':False,'rows':[{'instrument':'EUR_USD',
         'observed_epoch':NOW,'status':'ready','families':{},'active_forecasts':[]}]}}
    result=news_tests.render_script(functions,'const data='+json.dumps(data)+';process.stdout.write(JSON.stringify(collectionHeaderState(data)));')
    assert result['text']=='Combined forecast status unavailable'


def mixed_market_data(publication,tmp_path):
    data=market_data(publication)
    data['pair_local_forecasts']=baseline.read(baseline.publication.__wrapped__(tmp_path/'independent_price_fixture'))
    joint=data['joint_price_news_forecasts']['rows'][1]
    joint['active_forecasts']=[]
    joint['status']='warming'
    joint['families'][FAMILY].update(status='warming',reason_label='Needs mature joint H1 labels')
    joint['families'][FAMILY]['current_readiness']['reason_label']='39/48 mature H1 labels'
    return data


def test_available_price_only_forecasts_are_inline_with_joint_blocker_and_original_h1(publication,tmp_path):
    import re
    data=mixed_market_data(publication,tmp_path)
    price=data['pair_local_forecasts']['rows'][1]['active_forecasts']
    price[1].update(side=-1,predicted_return_bps=-2)
    before=copy.deepcopy(price)
    html=baseline.render(data)['html']
    inline=re.search(r'<div class="price-only-forecast">(.*?)<div class="joint-blocker overview-note">',html,re.S).group(1)
    assert '1 combined · 1 price-only · 1 unavailable' in html
    assert 'Price-only forecast · news not included' in inline
    assert 'State space: Up' in inline and 'Ridge: Down' in inline
    assert inline.count('H1 target')==2 and inline.count('Issued ')==4
    assert 'Current quoted spread: unavailable' in inline
    assert 'Needs mature joint H1 labels' in html and '39/48 mature H1 labels' in html
    assert 'uncalibrated p(up)' in inline and 'not verified accuracy' in html
    assert 'Combined: ' not in inline and 'News adjustment' not in inline
    assert '<th>Forecast</th>' in html and 'Buy' not in html and 'Sell' not in html
    assert data['pair_local_forecasts']['rows'][1]['active_forecasts']==before


@pytest.mark.parametrize('case',['stale_source','future_source','wrong_study','unsafe_source','stale_row','future_row','stale_families','elapsed_targets','future_publication'])
def test_price_only_inline_visibility_requires_its_own_current_verified_source(publication,tmp_path,case):
    data=mixed_market_data(publication,tmp_path);source=data['pair_local_forecasts'];row=source['rows'][1]
    if case=='stale_source':source['generated_epoch']=NOW-91
    elif case=='future_source':source['generated_epoch']=NOW+1
    elif case=='wrong_study':source['study_version']=1
    elif case=='unsafe_source':source['can_place_orders']=True
    elif case=='stale_row':row['observed_epoch']=NOW-91
    elif case=='future_row':row['observed_epoch']=NOW+1
    elif case=='stale_families':
        for family in row['families'].values():family['observed_epoch']=NOW-91
    elif case=='elapsed_targets':
        for arm in row['active_forecasts']:arm['target_epoch']=NOW
    else:
        for arm in row['active_forecasts']:arm['publication_epoch']=NOW+1
    html=baseline.render(data)['html']
    assert '<div class="price-only-forecast">' not in html
    assert '1 combined · 0 price-only · 2 unavailable' in html
    assert 'Combined: Up' in html


def test_one_valid_price_family_is_enough_without_averaging_or_hiding_joint_status(publication,tmp_path):
    data=mixed_market_data(publication,tmp_path)
    data['pair_local_forecasts']['rows'][1]['active_forecasts'][1]['target_epoch']=NOW
    html=baseline.render(data)['html']
    inline=html.split('<div class="price-only-forecast">',1)[1].split('<div class="joint-blocker overview-note">',1)[0]
    assert '1 combined · 1 price-only · 1 unavailable' in html
    assert 'State space: Up' in inline and 'Ridge: Up' not in inline
    assert '1 model predictions · H1' in inline and 'Needs mature joint H1 labels' in html


def test_valid_joint_stays_primary_and_price_models_remain_collapsed(publication,tmp_path):
    data=mixed_market_data(publication,tmp_path);html=baseline.render(data)['html']
    first=html.split('<tbody>',1)[1].split('</tr>',1)[0]
    assert 'Combined: Up' in first
    assert 'data-state-key="price-only-models-EUR_USD"><summary>Other price-only models</summary>' in first
    assert 'Price-only forecast · news not included' not in first


def test_unavailable_joint_can_show_explicit_price_reference_without_joint_or_news_fallback(publication,tmp_path):
    data=mixed_market_data(publication,tmp_path)
    data['joint_price_news_forecasts']={'status':'unavailable','study_version':'joint_v2','rows':[]}
    html=baseline.render(data)['html']
    assert 'Combined forecast coverage unavailable · 2 price-only · 1 unavailable' in html
    assert html.count('<div class="price-only-forecast">')==2
    assert 'Combined forecast unavailable' in html and 'Combined: Up' not in html
    assert 'Needs verified price and news inputs' in html and 'News adjustment' not in html
