from copy import deepcopy
from decimal import Decimal
import pytest

import oanda_native_curve_outcomes_v1 as m
from oanda_forecast_curve_contract_v1 import CurveContractError
from test_oanda_forecast_curve_contract_v1 import chain, inputs, SOURCES

METADATA = dict(instrument='EUR_USD', base_currency='EUR', quote_currency='USD', pip_size='.0001')

def receipt_chain(**kwargs):
    return chain(input_context={'price_convention': 'official_midpoint',
        'historical_ingestion_equivalence_proven': False}, **kwargs)

def rows():
    return [dict(bar_start_epoch=t, available_epoch=1080, complete=True,
        mid_close='1.1004', bid_close='1.1003', ask_close='1.1005') for t in range(1010, 1075, 5)]

def captured(raw=None, **overrides):
    raw = rows() if raw is None else raw
    fields = dict(instrument='EUR_USD', raw_source_sha256='a'*64, granularity='S5',
        complete_provider_response=True, coverage_start_label_epoch=raw[0]['bar_start_epoch'],
        coverage_end_label_epoch=raw[-1]['bar_start_epoch'], read_started_epoch=1079,
        read_completed_epoch=1080, source_receipt_sha256='b'*64)
    args = dict(instrument='EUR_USD', source_sha256='a'*64,
        coverage_start_label_epoch=fields['coverage_start_label_epoch'],
        coverage_end_label_epoch=fields['coverage_end_label_epoch'], complete_range=True,
        read_started_epoch=1079, read_completed_epoch=1080,
        source_attestation=fields, clock=lambda:1080.5)
    args.update(overrides)
    return m.capture_outcome_candles(raw, **args)

def entry(**overrides):
    q = dict(instrument='EUR_USD', quote_id='fixture-quote', bid='1.1000', ask='1.1002',
             market_epoch=1008, available_epoch=1008.1, tradeable=True)
    q.update(overrides)
    return m.capture_entry_quote(q, source_sha256='c'*64, clock=lambda:1008.2)

def scored(capture=None, chain_kwargs=None, **overrides):
    _, curve, pub, con = receipt_chain(**(chain_kwargs or {}))
    args = dict(expected_source_bindings=SOURCES, metadata=METADATA, clock=lambda:1081)
    args.update(overrides)
    return m.score_curve(curve,pub,con,captured() if capture is None else capture,**args)

def view(report,index=0,kind='retained_training_target'):
    return report['nodes'][index]['views'][kind]

def test_three_native_nodes_two_views_are_not_six_independent_trials():
    report = scored()
    assert report['status']=='evaluated' and len(report['nodes'])==3
    v=view(report)
    assert v['actual_selected_label_epoch']==1015 and v['actual_selected_price_epoch']==1020
    assert Decimal(v['actual_move_pips'])==4 and Decimal(v['absolute_error_pips'])==1
    assert Decimal(v['zero_baseline_absolute_error_pips'])==4 and Decimal(v['brier'])==Decimal('.09')
    assert v['direction_correct'] is True
    result=m.aggregate_reports([report])
    assert result['unique_evaluated_curves']==1 and result['unique_reference_price_clocks']==1
    assert result['independent_sample_size'] is None
    assert all(g['scored_nodes']==1 for g in result['groups'])
    assert len(result['groups'])==6 and {g['horizon_sec'] for g in result['groups']}=={15,30,60}

def test_actual_late_bar_window_is_distinct_from_missing_exact_nominal():
    raw=[r for r in rows() if r['bar_start_epoch']!=1015]
    report=scored(captured(raw),chain_kwargs={'target_selection_policy':{
        'kind':'first_complete_bar_at_or_after_nominal','maximum_delay_sec':7}})
    exact=view(report,kind='nominal_exact');native=view(report)
    assert exact['status']=='unavailable'
    assert native['actual_selected_label_epoch']==1020 and native['actual_selected_price_epoch']==1025
    assert native['selection_delay_sec']==5
    assert native['outcome_available_epoch']==1080.5
    assert report['nodes'][0]['original_target_epoch']==1020

def test_window_probability_event_match_is_explicit_even_when_actual_bar_is_nominal():
    report=scored(chain_kwargs={'target_selection_policy':{
        'kind':'first_complete_bar_at_or_after_nominal','maximum_delay_sec':7}})
    assert view(report,kind='nominal_exact')['probability_event_matches_training_target'] is False
    assert view(report)['probability_event_matches_training_target'] is True
    groups=m.aggregate_reports([report])['groups']
    assert sum(g['probability_event_mismatch_count'] for g in groups if g['view']=='nominal_exact')==3

def test_price_conventions_remain_distinct_at_same_mba_source():
    raw=rows()
    for row in raw:row['mid_close']='1.10045'
    cap=captured(raw)
    official=scored(cap)
    _,c,p,k=chain(input_context={'price_convention':'ba_derived_midpoint',
                                 'historical_ingestion_equivalence_proven':False})
    ba=m.score_curve(c,p,k,cap,expected_source_bindings=SOURCES,metadata=METADATA,clock=lambda:1081)
    assert Decimal(view(official)['actual_move_pips'])==Decimal('4.5')
    assert Decimal(view(ba)['actual_move_pips'])==4
    assert ba['historical_ingestion_equivalence_proven'] is False
    assert view(ba)['official_mid_close']=='1.10045'
    agg=m.aggregate_reports([official,ba])
    assert len(agg['groups'])==12

@pytest.mark.parametrize('scope',['engineering_replay','synthetic_fixture'])
def test_nonprospective_curves_are_not_rescored_as_forward_evidence(scope):
    with pytest.raises(CurveContractError,match='nonprospective_computation_cannot_issue'):
        receipt_chain(scope=scope)
    _,c,p,k=receipt_chain();c['prepared_curve']['scope']=scope
    result=m.score_curve(c,p,k,captured(),expected_source_bindings=SOURCES,metadata=METADATA,clock=lambda:1081)
    assert result['status']=='withheld' and not result['nodes']

def test_absent_price_convention_refuses_instead_of_guessing():
    _,c,p,k=chain()
    r=m.score_curve(c,p,k,captured(),expected_source_bindings=SOURCES,metadata=METADATA,clock=lambda:1081)
    assert r['reason_code']=='explicit_price_convention_required'

def test_source_clocks_are_retained_not_replaced_by_target_market_clock():
    r=scored()
    assert r['target_capture_first_observed_epoch']==1080.5
    assert view(r)['source_row_available_epoch']==1080
    assert view(r)['outcome_available_epoch']==1080.5

@pytest.mark.parametrize('field,value',[
    ('complete',False),('bar_start_epoch',1011),('available_epoch',1074),
    ('mid_close',1.1004),('bid_close','1.1006'),('mid_close','NaN')])
def test_invalid_or_unavailable_source_rows_fail_closed(field,value):
    raw=rows();raw[-1][field]=value
    with pytest.raises(CurveContractError):captured(raw)

@pytest.mark.parametrize('field,value',[
    ('instrument','GBP_USD'),('raw_source_sha256','d'*64),('granularity','M1'),
    ('complete_provider_response',False),('coverage_start_label_epoch',1005),
    ('read_completed_epoch',1079),('source_receipt_sha256','invalid')])
def test_query_attestation_is_required_separately_from_selfsealed_rows(field,value):
    cap=captured();att=deepcopy(cap['source_query_attestation']);att[field]=value
    with pytest.raises(CurveContractError):captured(source_attestation=att)

def test_duplicate_source_labels_and_unattested_extended_domains_reject():
    raw=rows();raw.insert(1,deepcopy(raw[0]))
    with pytest.raises(CurveContractError):captured(raw)
    with pytest.raises(CurveContractError):captured(coverage_start_label_epoch=1005)

def test_resealed_capture_cannot_alter_semantic_price_epoch():
    cap=captured();cap['rows'][0]['price_epoch']+=1
    cap['capture_sha256']=m.content_hash({k:v for k,v in cap.items() if k!='capture_sha256'})
    with pytest.raises(CurveContractError,match='semantic'):m.validate_candle_capture(cap)

def test_outside_source_domain_is_missing_not_inferred_absence_or_forwardfill():
    cap=captured(rows()[3:])
    r=scored(cap)
    assert view(r)['reason_code']=='source_domain_starts_after_nominal_target'
    assert view(r,1)['status']=='scored'

def test_missing_end_of_window_is_insufficient_coverage_not_no_bar():
    raw=[r for r in rows() if r['bar_start_epoch']<1015]
    r=scored(captured(raw),chain_kwargs={'target_selection_policy':{
        'kind':'first_complete_bar_at_or_after_nominal','maximum_delay_sec':7}})
    assert view(r)['reason_code']=='target_window_not_covered_by_source'

def test_no_complete_bar_within_attested_window_does_not_select_later_bar():
    raw=[r for r in rows() if r['bar_start_epoch'] not in (1015,1020)]
    r=scored(captured(raw),chain_kwargs={'target_selection_policy':{
        'kind':'first_complete_bar_at_or_after_nominal','maximum_delay_sec':7}})
    assert view(r)['reason_code']=='provider_reported_no_complete_target_bar'

def test_future_capture_or_scoring_before_consumption_is_withheld():
    r=scored(clock=lambda:1080.4)
    assert r['status']=='withheld' and r['reason_code']=='future_receipt_at_scoring'

def test_zero_actual_return_has_explicit_direction_exclusion_and_strict_up_brier():
    raw=rows()
    for row in raw:row.update(mid_close='1.1000',bid_close='1.0999',ask_close='1.1001')
    r=scored(captured(raw));v=view(r)
    assert v['zero_outcome'] is True and v['direction_correct'] is None
    assert Decimal(v['brier'])==Decimal('.49')
    assert m.aggregate_reports([r])['groups'][0]['direction_denominator']==0

def test_neutral_prediction_is_not_a_hidden_entry_and_has_explicit_count():
    points=inputs()['points']
    for point in points:point['predicted_signed_pips']='0'
    r=scored(chain_kwargs={'points':points},entry_quote=entry());v=view(r)
    assert v['neutral_prediction'] and v['direction_correct'] is None
    assert v['executable']['original_prediction_side_net_bps'] is None

def test_entry_bidask_uses_own_exact_side_denominators_and_original_entry_clocks():
    r=scored(entry_quote=entry());e=view(r)['executable']
    assert e['status']=='scored_bid_ask_cost_diagnostic'
    assert e['target_tradeability_observed'] is e['broker_execution_observed'] is False
    assert Decimal(e['long_net_pips'])==1
    assert Decimal(e['short_net_pips'])==-5
    assert abs(Decimal(e['long_net_bps'])-Decimal('.0001')/Decimal('1.1002')*10000)<Decimal('1e-25')
    assert abs(Decimal(e['short_net_bps'])+Decimal('.0005')/Decimal('1.1000')*10000)<Decimal('1e-25')
    assert e['entry_first_observed_epoch']==1008.2
    assert e['original_prediction_side_positive'] is True

def test_preissue_quote_cannot_be_attached_as_a_later_prospective_entry():
    e=entry(market_epoch=1005,available_epoch=1005.1)
    result=view(scored(entry_quote=e))['executable']
    assert result['status']=='unavailable' and 'after_consumption' in result['reason_code']


def test_current_recent_prepublication_tick_independently_read_after_consumption_is_eligible():
    e=entry(market_epoch=1005,available_epoch=1008.1)
    result=view(scored(entry_quote=e))['executable']
    assert result['status']=='scored_bid_ask_cost_diagnostic'
    assert result['entry_market_epoch']==1005 and result['entry_available_epoch']==1008.1

def test_backfilled_entry_observed_after_target_is_not_a_prospective_fill():
    e=m.capture_entry_quote(dict(instrument='EUR_USD',quote_id='late',bid='1.1',ask='1.1002',
        market_epoch=1008,available_epoch=1021,tradeable=True),source_sha256='c'*64,clock=lambda:1021)
    result=view(scored(entry_quote=e))['executable']
    assert result['status']=='unavailable' and 'before_original_target' in result['reason_code']

@pytest.mark.parametrize('field,value',[('tradeable',False),('tradeable',1),('bid',1.1),
    ('available_epoch',1009),('market_epoch',940),('instrument','USD_USD')])
def test_entry_boundary_rejects_unavailable_nonexact_stale_or_false_tradeability(field,value):
    with pytest.raises(CurveContractError):entry(**{field:value})

def test_missing_entry_does_not_hide_original_prediction_scores():
    r=scored()
    assert view(r)['status']=='scored'
    assert view(r)['executable']['reason_code']=='no_independently_observed_entry'

def test_receipt_tamper_is_withheld_without_original_score_or_promoted_scope():
    _,c,p,k=receipt_chain();p['publication_completed_epoch']+=.1
    r=m.score_curve(c,p,k,captured(),expected_source_bindings=SOURCES,metadata=METADATA,clock=lambda:1081)
    assert r['status']=='withheld' and not r['nodes']
    assert r['can_promote'] is r['can_place_orders'] is False

def test_aggregation_refuses_duplicate_nodes_instead_of_inflating_counts():
    r=scored()
    with pytest.raises(CurveContractError,match='duplicate_curve_node_view'):m.aggregate_reports([r,r])

def test_explicit_current_metadata_scale_converts_pips_without_changing_price_score():
    r=scored(metadata={**METADATA,'pip_size':'.001'})
    assert Decimal(view(r)['actual_move_pips'])==Decimal('.4')
    assert Decimal(view(r)['actual_move_native_pips'])==4

def test_no_mutation_and_repeatable_exact_scoring():
    cap=captured();before=deepcopy(cap)
    assert scored(cap)==scored(cap) and cap==before


def test_rows_cannot_borrow_an_earlier_availability_than_source_query_receipt():
    raw=rows();raw[0]['available_epoch']=1015
    with pytest.raises(CurveContractError,match='row_availability_differs_from_source_read'):
        captured(raw)


def test_expired_at_issue_node_is_not_scored_as_an_original_forecast():
    import oanda_forecast_curve_contract_v1 as c
    prepared=c.prepare_curve(**inputs(input_context={'price_convention':'official_midpoint'}))
    issued=c.issue_curve(prepared,expected_source_bindings=SOURCES,clock=lambda:1025)
    pub=c.publication_receipt(issued,persisted_bytes_sha256=c.content_hash(issued),
        publication_started_epoch=1025.1,expected_source_bindings=SOURCES,clock=lambda:1025.2)
    con=c.consume_curve(issued,pub,expected_source_bindings=SOURCES,clock=lambda:1025.3)
    r=m.score_curve(issued,pub,con,captured(),expected_source_bindings=SOURCES,metadata=METADATA,clock=lambda:1081)
    assert view(r)['reason_code']=='node_not_issued_as_forecast'
    assert view(r,1)['status']=='scored'


def test_missing_publication_receipt_never_creates_scored_output():
    _,curve,_,consumption=receipt_chain()
    r=m.score_curve(curve,{},consumption,captured(),expected_source_bindings=SOURCES,metadata=METADATA,clock=lambda:1081)
    assert r['status']=='withheld' and r['nodes']==[]


def rolling_capture(first,last,read,*,mid='1.1004',derived=1200,source='a',receipt='b',omit=()):
    raw=[dict(bar_start_epoch=t,available_epoch=read,complete=True,mid_close=mid,
        bid_close='1.1003',ask_close='1.1005') for t in range(first,last+1,5) if t not in omit]
    att=dict(instrument='EUR_USD',granularity='S5',raw_source_sha256=source*64,
        complete_provider_response=True,coverage_start_label_epoch=first,
        coverage_end_label_epoch=last,read_started_epoch=read-1,read_completed_epoch=read,
        source_receipt_sha256=receipt*64)
    return m.capture_outcome_candles(raw,instrument='EUR_USD',source_sha256=source*64,
        coverage_start_label_epoch=first,coverage_end_label_epoch=last,complete_range=True,
        read_started_epoch=read-1,read_completed_epoch=read,source_attestation=att,clock=lambda:derived)

def combined(captures,**kwargs):
    _,curve,pub,con=receipt_chain(**kwargs)
    return m.score_curve_from_captures(curve,pub,con,captures,
        expected_source_bindings=SOURCES,metadata=METADATA,clock=lambda:1300)

def test_separate_rolling_queries_score_all_native_targets_without_merging_source_domains():
    caps=[rolling_capture(1010,1020,1026,source='a',receipt='b'),
          rolling_capture(1025,1035,1041,source='c',receipt='d'),
          rolling_capture(1055,1065,1071,source='e',receipt='f')]
    r=combined(caps)
    assert r['status']=='evaluated' and r['selected_source_capture_count']==3
    assert all(view(r,i)['status']=='scored' for i in range(3))
    assert view(r)['selected_source_capture']['coverage_end_label_epoch']==1020
    assert view(r,1)['selected_source_capture']['coverage_start_label_epoch']==1025
    assert 'target_capture_sha256' not in r
    assert len(m.aggregate_reports([r])['groups'])==6

def test_wrapper_uses_original_source_read_clock_not_later_derived_seal_clock_or_return():
    early=rolling_capture(1010,1070,1080,mid='1.1001',derived=1250,source='e',receipt='f')
    late=rolling_capture(1010,1070,1090,mid='1.1004',derived=1200,source='a',receipt='b')
    r=combined([late,early])
    assert Decimal(view(r)['actual_move_pips'])==1
    selected=view(r)['selected_source_capture']
    assert selected['original_source_read_completed_epoch']==1080
    assert selected['derived_capture_first_observed_epoch']==1250
    assert view(r)['outcome_available_epoch']==1250
    assert combined([early,late])==r

def test_wrapper_earliest_complete_missing_window_is_not_replaced_by_later_winning_bar():
    early=rolling_capture(1010,1070,1080,omit=(1015,1020),source='a',receipt='b')
    later=rolling_capture(1010,1070,1090,mid='1.1003',source='c',receipt='d')
    r=combined([later,early],target_selection_policy={
        'kind':'first_complete_bar_at_or_after_nominal','maximum_delay_sec':7})
    assert view(r)['reason_code']=='provider_reported_no_complete_target_bar'
    assert view(r)['selected_source_capture']['original_source_read_completed_epoch']==1080

def test_wrapper_keeps_uncovered_target_missing_without_stitching_adjacent_domains():
    caps=[rolling_capture(1010,1010,1020,source='a',receipt='b'),
          rolling_capture(1020,1070,1080,source='c',receipt='d')]
    r=combined(caps,target_selection_policy={
        'kind':'first_complete_bar_at_or_after_nominal','maximum_delay_sec':7})
    assert view(r)['reason_code']=='no_decisive_source_capture'
    assert view(r,1)['status']=='scored'

def test_same_raw_receipt_duplicate_derivatives_collapse_but_conflicting_mapping_withholds():
    early=rolling_capture(1010,1070,1080,derived=1200)
    later=rolling_capture(1010,1070,1080,derived=1250)
    r=combined([later,early])
    assert r['unique_source_observation_count']==1
    assert view(r)['selected_source_capture']['derived_capture_first_observed_epoch']==1200
    bad=rolling_capture(1010,1070,1080,mid='1.1001',derived=1250)
    r=combined([early,bad])
    assert r['status']=='withheld' and r['reason_code']=='same_source_receipt_conflicting_mapping'

def test_wrapper_rejects_empty_future_or_tampered_capture_set():
    assert combined([])['status']=='withheld'
    future=rolling_capture(1010,1070,1080,derived=1301)
    assert combined([future])['reason_code']=='future_receipt_at_scoring'
    bad=rolling_capture(1010,1070,1080);bad['rows'][0]['mid_close']='1.2'
    assert combined([bad])['status']=='withheld'

def test_wrapper_selected_views_have_unique_denominators_and_entry_status():
    _,c,p,k=receipt_chain()
    r=m.score_curve_from_captures(c,p,k,[rolling_capture(1010,1070,1080)],
        expected_source_bindings=SOURCES,metadata=METADATA,entry_quote=entry(),clock=lambda:1300)
    assert r['status']=='evaluated'
    assert all(view(r,i)['executable']['status']=='scored_bid_ask_cost_diagnostic' for i in range(3))
    assert all(g['executable_non_neutral_denominator']==1 for g in m.aggregate_reports([r])['groups'])
