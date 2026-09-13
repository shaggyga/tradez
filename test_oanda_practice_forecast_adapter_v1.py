"""Pure/adversarial adapter tests; no broker requests or actual ledger reads."""
import ast
import copy
from decimal import Decimal, localcontext
import hashlib
import json
from pathlib import Path

import pytest

import oanda_practice_forecast_adapter_v1 as a

ROOT = Path(__file__).resolve().parent
NOW = 1800000000.0
REGISTRY_RAW = (ROOT / 'config/joint_price_news_study_v3_20260908.json').read_bytes()
REGISTRY = json.loads(REGISTRY_RAW)


def fixture():
    flags = dict(research_only=True, **{k: False for k in a.INERT_FLAGS})
    spec = dict(origin_registry_sha256=a.REGISTRY_SHA256, origin_activation_sha256=a.ACTIVATION_SHA256,
                observer_source_bindings=dict(a.OBSERVER_BINDINGS), original_source_bindings=REGISTRY['source_bindings'],
                dependency_versions=REGISTRY['dependency_versions'], time_budget_sec=8, **flags)
    rows = []
    for pair, item in sorted(REGISTRY['pairs'].items()):
        rows.append(dict(instrument=pair, pip_size=item['pip_size'], families={a.FAMILY:
            dict(status='unavailable', reason='no_verified_published_forecast', observed_epoch=NOW-2)}))
    row = next(row for row in rows if row['instrument'] == 'EUR_USD')
    contract_spec = REGISTRY['pairs']['EUR_USD']['families'][a.FAMILY]
    contract = contract_spec['contract']
    forecast_sha = 'a' * 64
    publication_sha = a.digest(dict(epoch=NOW-199, forecast_sha=forecast_sha))
    arm = dict(instrument='EUR_USD', family=a.FAMILY, cohort_id=contract['cohorts'][a.FAMILY],
        pip_size='0.0001', horizon_sec=3600, reference_epoch=NOW-300, reference_mid='1.10000',
        issued_epoch=NOW-200, target_epoch=NOW+3300, side=1, probability_up=.6,
        predicted_return_bps=10.0, news_capture_sha256='e'*64,
        news_evidence_epoch=NOW-220, news_generated_epoch=NOW-215,
        news_first_observed_epoch=NOW-210, news_available_epoch=NOW-210, news_expires_epoch=NOW+80,
        input_contributions={'matched_price_only_expected_pips':2.0})
    forecast = dict(instrument='EUR_USD', family=a.FAMILY, horizon_sec=3600,
        publication_verified=True, consumption_verified=True, forecasts=[arm],
        reference_epoch=NOW-300, reference_available_epoch=NOW-290, issued_epoch=NOW-200,
        publication_epoch=NOW-199, consumption_epoch=NOW-198,
        publication_verified_epoch=NOW-3, target_epoch=NOW+3300, decision_id='b'*64,
        forecast_sha256=forecast_sha, publication_receipt_sha256=publication_sha,
        consumer_receipt_sha256=a.digest(dict(epoch=NOW-198, forecast_sha=forecast_sha, publication_sha=publication_sha)))
    row['families'][a.FAMILY] = dict(status='forecast', observed_epoch=NOW-2,
        activated_epoch=NOW-1000, contract_sha256=contract_spec['contract_sha256'],
        origin_registry_sha256=a.REGISTRY_SHA256, origin_study='joint_v3', cohort_id=arm['cohort_id'],
        latest_forecast=forecast,
        ledger_observation=dict(contract_and_activation_verified=True, query_only=True,
            original_published_forecast=True, original_row_identity_binding_sha256='c'*64,
            read_started_epoch=NOW-5, forecast_join_read_started_epoch=NOW-4,
            forecast_join_read_completed_epoch=NOW-3, read_completed_epoch=NOW-3,
            verification_completed_epoch=NOW-2, latest_original_target_epoch=NOW+3300))
    summary = dict(schema_version='joint_v3_ledger_observer_summary_v2_20260909',
                   registry_sha256=a.digest(spec), generated_epoch=NOW-1, rows=rows, **flags)
    report = dict(schema_version='joint_v3_ledger_status_observer_v2_20260909',
        status='complete_observation', started_epoch=NOW-6, completed_epoch=NOW-1,
        observer_spec=spec, summary=summary, **flags)
    reseal(report)
    return report


def reseal(report):
    report['observer_spec_sha256'] = a.digest(report['observer_spec'])
    summary = report['summary']
    summary['registry_sha256'] = report['observer_spec_sha256']
    summary.pop('payload_sha256', None)
    summary['payload_sha256'] = a.digest(summary)
    report['summary_sha256'] = a.digest(summary)
    report.pop('payload_sha256', None)
    report['payload_sha256'] = a.digest(report)
    return report


def slot(report):
    return next(row for row in report['summary']['rows'] if row['instrument']=='EUR_USD')['families'][a.FAMILY]


def arm(report):
    return slot(report)['latest_forecast']['forecasts'][0]


def build(report=None, **kwargs):
    raw = a.encoded(report or fixture())
    options = dict(expected_report_sha256=hashlib.sha256(raw).hexdigest(), decision_epoch=NOW,
                   instruments=['EUR_USD'])
    options.update(kwargs)
    return a.build_candidates(raw, REGISTRY_RAW, **options)


def test_original_identity_terminal_and_inert_authority():
    signal = build()['signals'][0]
    assert signal['side']==1 and signal['direction']=='buy'
    assert Decimal(signal['expected_terminal_price'])==Decimal('1.1011')
    assert signal['original_target_epoch']==NOW+3300
    assert signal['available_epoch']==NOW-198
    assert signal['adapter_observed_epoch']==NOW-1
    assert signal['source_authority']['research_only'] is True
    assert signal['source_authority']['account_eligible'] is False
    assert signal['can_place_orders'] is False
    assert signal['model_version']==REGISTRY['pairs']['EUR_USD']['families'][a.FAMILY]['contract']['model_version']
    assert signal['uncertainty']['residual_scale'] is None
    assert signal['controls']['published_price_v2_status']=='not_collected_by_this_adapter'


def test_sell_is_original_signed_move():
    report=fixture();arm(report).update(side=-1, probability_up=.4, predicted_return_bps=-10)
    signal=build(reseal(report))['signals'][0]
    assert signal['side']==-1 and Decimal(signal['expected_terminal_price'])==Decimal('1.0989')


def test_decimal_context_does_not_change_result():
    before=build()
    with localcontext() as context:
        context.prec=3
        context.rounding='ROUND_UP'
        after=build()
    assert after==before


def test_canonical_encoding_matches_frozen_observer_for_unicode():
    value={'diagnostic':'\u0394 price'}
    assert a.encoded(value)==json.dumps(value,sort_keys=True,separators=(',', ':'),allow_nan=False).encode()


def test_exact_input_copy_and_stable_origin_id():
    report=fixture(); original=copy.deepcopy(report)
    one=build(report);two=build(report,decision_epoch=NOW+1)
    assert report==original
    assert one['signals'][0]['trial_candidate_id']==two['signals'][0]['trial_candidate_id']
    assert one['signals'][0]['expected_terminal_price']==two['signals'][0]['expected_terminal_price']
    one['signals'][0]['original_news_clocks']['news_evidence_epoch']=1
    assert build(report)['signals'][0]['original_news_clocks']['news_evidence_epoch']==NOW-220


@pytest.mark.parametrize('side,bps,p,reason',[(0,0,.5,'original_forecast_neutral'),
    (True,10,.6,'side_invalid'),(1,-10,.6,'original_direction_mismatch'),
    (-1,10,.4,'original_direction_mismatch'),(1,10,.4,'probability_direction_mismatch'),
    (-1,-10,.6,'probability_direction_mismatch'),(1,10,1.1,'original_direction_mismatch')])
def test_direction_refusals(side,bps,p,reason):
    report=fixture();arm(report).update(side=side,predicted_return_bps=bps,probability_up=p)
    result=build(reseal(report))
    assert result['signals']==[] and result['rejected'][0]['reason']==reason


@pytest.mark.parametrize('key,value,reason',[
    ('publication_verified',False,'original_publication_or_consumption_unverified'),
    ('consumption_verified',False,'original_publication_or_consumption_unverified'),
    ('forecast_sha256','z'*64,'invalid_hash'),
    ('publication_receipt_sha256','f'*64,'publication_receipt_identity'),
    ('consumer_receipt_sha256','f'*64,'consumer_receipt_identity'),
    ('instrument','GBP_USD','forecast_identity'),
    ('target_epoch',NOW+3301,'original_forecast_clock_order'),
    ('consumption_epoch',NOW+1,'original_forecast_clock_order'),
    ('publication_verified_epoch',NOW-1,'original_forecast_clock_order'),
    ('reference_available_epoch',NOW-350,'original_forecast_clock_order')])
def test_original_chain_refusals(key,value,reason):
    report=fixture();slot(report)['latest_forecast'][key]=value
    result=build(reseal(report))
    assert result['signals']==[] and result['rejected'][0]['reason']==reason


@pytest.mark.parametrize('key,value,reason',[
    ('query_only',False,'original_ledger_not_verified'),
    ('contract_and_activation_verified',False,'original_ledger_not_verified'),
    ('original_row_identity_binding_sha256','bad','invalid_hash'),
    ('forecast_join_read_started_epoch',NOW,'ledger_observation_clock_order'),
    ('latest_original_target_epoch',NOW+3600,'original_target_identity')])
def test_ledger_refusals(key,value,reason):
    report=fixture();slot(report)['ledger_observation'][key]=value
    assert build(reseal(report))['rejected'][0]['reason']==reason


def test_news_validity_is_original_issue_not_reread_refresh():
    report=fixture();arm(report)['news_expires_epoch']=NOW-190
    assert build(reseal(report))['candidate_count']==1
    arm(report)['news_expires_epoch']=NOW-201
    assert build(reseal(report))['rejected'][0]['reason']=='original_news_clock_window'


@pytest.mark.parametrize('value',[True,False,float('nan'),float('inf'),-1,0,'1800000000'])
def test_invalid_decision_clocks(value):
    with pytest.raises(a.AdapterError,match='invalid_clock'):
        build(decision_epoch=value)


def test_inclusive_signal_age_and_next_instant():
    report=fixture()
    assert build(report,maximum_signal_age_sec=200)['candidate_count']==1
    assert build(report,maximum_signal_age_sec=199.99)['rejected'][0]['reason']=='original_forecast_stale'


def test_inclusive_target_window_and_next_instant():
    report=fixture()
    assert build(report,minimum_remaining_sec=3300)['candidate_count']==1
    assert build(report,minimum_remaining_sec=3300.001)['rejected'][0]['reason']=='original_target_too_near_or_elapsed'


@pytest.mark.parametrize('kwargs,reason',[
    ({'maximum_signal_age_sec':901},'maximum_signal_age_policy'),
    ({'maximum_signal_age_sec':True},'maximum_signal_age_policy'),
    ({'minimum_remaining_sec':59},'minimum_remaining_policy'),
    ({'minimum_remaining_sec':float('nan')},'minimum_remaining_policy'),
    ({'maximum_observation_age_sec':91},'maximum_observation_age_policy'),
    ({'instruments':['EUR_USD','EUR_USD']},'instrument_policy')])
def test_policy_bounds(kwargs,reason):
    with pytest.raises(a.AdapterError,match=reason):build(**kwargs)


def test_all_registered_pairs_retained_no_silent_drops():
    result=build(instruments=None)
    assert result['requested_pairs']==68 and result['candidate_count']==1 and result['rejection_count']==67
    assert len({r['instrument'] for r in result['signals']+result['rejected']})==68
    assert build(instruments=['ZZZ_USD'])['rejected']==[dict(instrument='ZZZ_USD',reason='unsupported_instrument')]


def test_summary_missing_and_duplicate_pair_fail_whole_batch():
    report=fixture();report['summary']['rows'][1]=copy.deepcopy(report['summary']['rows'][0])
    with pytest.raises(a.AdapterError,match='observer_pair_identity'):build(reseal(report))


@pytest.mark.parametrize('mutator,reason',[
    (lambda r:r.update(can_place_orders=True),'original_research_authority_changed'),
    (lambda r:r['observer_spec']['observer_source_bindings'].update({'other.py':'f'*64}),'observer_source_closure_identity'),
    (lambda r:r['observer_spec'].update(origin_activation_sha256='f'*64),'observer_source_closure_identity'),
    (lambda r:r['observer_spec'].update(time_budget_sec=9),'observer_source_closure_identity'),
    (lambda r:r.update(completed_epoch=NOW+1),'observer_report_clock_window'),
    (lambda r:r.update(started_epoch=NOW),'observer_report_clock_window'),
    (lambda r:r['summary'].update(generated_epoch=NOW),'summary_identity')])
def test_report_and_closure_refusal(mutator,reason):
    report=fixture();mutator(report)
    with pytest.raises(a.AdapterError,match=reason):build(reseal(report))


def test_tamper_without_reseal_and_wrong_raw_hash():
    report=fixture();arm(report)['predicted_return_bps']=99
    with pytest.raises(a.AdapterError,match='observer_report_seal'):build(report)
    with pytest.raises(a.AdapterError,match='observer_report_identity'):build(expected_report_sha256='e'*64)


def test_duplicate_json_keys_and_oversized_report():
    raw=b'{"x":1,"x":2}'
    with pytest.raises(a.AdapterError,match='duplicate_key'):
        a.build_candidates(raw,REGISTRY_RAW,expected_report_sha256=hashlib.sha256(raw).hexdigest(),decision_epoch=NOW)
    raw=b' '*(a.MAX_REPORT_BYTES+1)
    with pytest.raises(a.AdapterError,match='observer_report_bytes'):
        a.build_candidates(raw,REGISTRY_RAW,expected_report_sha256=hashlib.sha256(raw).hexdigest(),decision_epoch=NOW)


def test_import_is_side_effect_free_and_no_broker_or_database_import():
    tree=ast.parse((ROOT/'oanda_practice_forecast_adapter_v1.py').read_text())
    imports={n.names[0].name for n in ast.walk(tree) if isinstance(n,ast.Import)}
    assert not imports & {'requests','sqlite3','subprocess','joblib','pickle'}
    for node in tree.body:
        if isinstance(node,ast.Expr):assert isinstance(node.value,ast.Constant)
        if isinstance(node,ast.Assign):assert not any(isinstance(n,ast.Call) for n in ast.walk(node.value))


def test_read_wrapper_refuses_wrong_root_without_import_or_read(tmp_path):
    with pytest.raises(a.AdapterError,match='canonical_project_root_required'):
        a.read_candidates(tmp_path,NOW,clock=lambda:NOW)


def test_read_wrapper_clock_rollback_prevents_observer_import():
    with pytest.raises(a.AdapterError,match='adapter_clock_regression'):
        a.read_candidates(ROOT,NOW,clock=lambda:NOW-1)


def test_real_observer_import_with_mocked_ro_execution(monkeypatch):
    # Import performs source reads only; replace the scan before any DB access.
    import oanda_joint_v3_ledger_status_observer_v2 as observer
    report=fixture(); report['started_epoch']=NOW;report['completed_epoch']=NOW
    report['summary']['generated_epoch']=NOW
    # Build observed fixture clocks corresponding to the actual wrapper call.
    ledger=slot(report)['ledger_observation']
    for key in ('read_started_epoch','forecast_join_read_started_epoch','forecast_join_read_completed_epoch',
                'read_completed_epoch','verification_completed_epoch'):ledger[key]=NOW
    slot(report)['observed_epoch']=NOW
    slot(report)['latest_forecast']['publication_verified_epoch']=NOW
    reseal(report)
    calls=[]
    def fake_scan(registry_path,study_root,**kwargs):
        calls.append((registry_path,study_root,kwargs))
        assert kwargs['expected_observer_source_bindings']==a.OBSERVER_BINDINGS
        assert kwargs['expected_activation_sha256']==a.ACTIVATION_SHA256
        return copy.deepcopy(report)
    monkeypatch.setattr(observer,'observe_joint_v3',fake_scan)
    result=a.read_candidates(ROOT,NOW,instruments=['EUR_USD'],clock=lambda:NOW)
    assert result['candidate_count']==1 and len(calls)==1
    assert result['observer_report']==report
    assert result['requested_epoch']==result['decision_epoch']==NOW
    assert result['payload_sha256']==a.digest({k:v for k,v in result.items() if k!='payload_sha256'})


def test_wrapper_completion_is_after_both_mapping_calls(monkeypatch):
    import oanda_joint_v3_ledger_status_observer_v2 as observer
    report=fixture(); report['started_epoch']=report['completed_epoch']=NOW
    report['summary']['generated_epoch']=NOW
    ledger=slot(report)['ledger_observation']
    for key in ('read_started_epoch','forecast_join_read_started_epoch','forecast_join_read_completed_epoch',
                'read_completed_epoch','verification_completed_epoch'):ledger[key]=NOW
    slot(report)['observed_epoch']=NOW
    slot(report)['latest_forecast']['publication_verified_epoch']=NOW
    reseal(report)
    monkeypatch.setattr(observer,'observe_joint_v3',lambda *args,**kwargs:copy.deepcopy(report))
    current=[NOW]
    original=a.build_candidates
    def expensive_mapping(*args,**kwargs):
        result=original(*args,**kwargs)
        current[0]+=1
        return result
    monkeypatch.setattr(a,'build_candidates',expensive_mapping)
    result=a.read_candidates(ROOT,NOW,instruments=['EUR_USD'],clock=lambda:current[0])
    assert result['decision_epoch']==NOW+1
    assert result['adapter_completed_epoch']==NOW+2
    assert result['decision_epoch_scope'].startswith('adapter_information_cutoff')
