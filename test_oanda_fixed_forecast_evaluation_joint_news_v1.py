"""Exact scoring and pair isolation under the generic frozen protocol."""
from copy import deepcopy
from decimal import Decimal, localcontext
import json

import pytest

from oanda_fixed_forecast_evaluation_joint_news_v1 import (
    evaluate, exact_baselines, jsonable, loads_exact_prices, validate_protocol,
)
from test_oanda_causal_forecast_ledger_joint_news_v1 import (
    completed, contract_fixture, FAMILIES, capture_validation_seam, opened, ready_attempt, quote,
)


@pytest.mark.parametrize('record,field,value', [
    ('entry', 'instrument', 'GBP_USD'), ('target', 'instrument', 'GBP_USD'),
    ('entry', 'pip_size', .001), ('target', 'pip_size', .001),
])
def test_cross_pair_or_pip_quotes_cannot_supply_outcomes(opened, record, field, value):
    ledger, clock, _ = opened
    _, _, entry, target = completed(ledger, clock)
    data, protocol = ledger.export_evaluation()
    quote_id = (entry if record == 'entry' else target)['quote_id']
    for quote in data['quotes']:
        if quote['quote_id'] == quote_id:
            quote[field] = value
    report = evaluate(data, protocol)
    assert report['coverage']['scored_decisions'] == 0
    assert sum(report['quote_exclusions'].values()) == 1


@pytest.mark.parametrize('field,value', [('instrument', 'GBP_USD'), ('pip_size', .001),
    ('pip_size', None), ('cohort_id', 'causal_pair_local_v1_20260907.GBP_USD.ridge_return_repaired')])
def test_wrong_pair_forecast_invalidates_single_family_score(opened, field, value):
    ledger, clock, _ = opened
    completed(ledger, clock)
    data, protocol = ledger.export_evaluation()
    data['decisions'][0]['forecasts'][0][field] = value
    report = evaluate(data, protocol)
    assert report['coverage']['scored_decisions'] == 0
    assert report['decision_exclusion_counts']['forecast_clock_identity_or_value_invalid'] == 1


@pytest.mark.parametrize('field,value', [('instrument', 'GBP_USD'), ('instrument', None),
    ('pip_size', .001), ('pip_size', None)])
def test_decision_identity_cannot_disagree_with_its_forecasts(opened, field, value):
    ledger, clock, _ = opened
    completed(ledger, clock)
    data, protocol = ledger.export_evaluation()
    data['decisions'][0][field] = value
    report = evaluate(data, protocol)
    assert report['coverage']['scored_decisions'] == 0
    assert report['decision_exclusion_counts']['decision_pair_or_pip_mismatch'] == 1


@pytest.mark.parametrize('pip', [.0001, .001, .01])
def test_protocol_pip_metadata_is_explicit_and_preserved_through_exact_json(pip):
    protocol = contract_fixture('HKD_JPY', pip)['evaluation_protocol']
    validate_protocol(protocol)
    restored = loads_exact_prices(json.dumps(protocol))
    validate_protocol(restored)
    assert restored['pip_size'] == Decimal(str(pip))


@pytest.mark.parametrize('field,value', [('horizon_sec', 7200), ('input_timeframe', 'M5'),
    ('pip_size', .1), ('pip_size', True), ('extra_cost_stress_bps', [0,.5]),
    ('extra_cost_stress_bps', [0,.5,1,2]), ('quote_max_age_sec', 61)])
def test_fixed_pair_protocol_parameters_cannot_be_relaxed(field, value):
    protocol = contract_fixture()['evaluation_protocol']
    protocol[field] = value
    with pytest.raises(ValueError):
        validate_protocol(protocol)


def test_rolling_baseline_only_uses_requested_pair_labels():
    labels = [
        {'event_id':'eur', 'instrument':'EUR_USD', 'horizon_sec':3600, 'target_epoch':100, 'available_epoch':101, 'up':True},
        {'event_id':'huf', 'instrument':'USD_HUF', 'horizon_sec':3600, 'target_epoch':100, 'available_epoch':101, 'up':False},
    ]
    baseline = exact_baselines(200, labels, instrument='USD_HUF', horizon_sec=3600, lookback=100, min_labels=1)
    assert baseline['rolling_class_rate']['training_event_ids'] == ['huf']
    with localcontext() as context:
        context.prec = 80
        assert baseline['rolling_class_rate']['probability_up'] == Decimal(1)/Decimal(3)


def test_scoring_does_not_depend_on_callers_decimal_precision(opened):
    ledger, clock, _ = opened
    completed(ledger, clock)
    data, protocol = ledger.export_evaluation()
    expected = evaluate(data, protocol)
    with localcontext() as context:
        context.prec = 3
        actual = evaluate(data, protocol)
    assert jsonable(actual['decisions']) == jsonable(expected['decisions'])
    assert actual['coverage'] == expected['coverage']


def test_evaluator_does_not_mutate_input_pair_prices_or_clocks(opened):
    ledger, clock, _ = opened
    completed(ledger, clock)
    data, protocol = ledger.export_evaluation()
    before = deepcopy((data, protocol))
    evaluate(data, protocol)
    assert (data, protocol) == before


def test_sibling_forecast_cannot_change_own_single_family_denominator(opened):
    ledger,clock,_=opened;completed(ledger,clock)
    data,protocol=ledger.export_evaluation()
    sibling=deepcopy(data['decisions'][0]['forecasts'][0])
    sibling['family']='foreign_price_only_family'
    sibling['forecast_id']+=':foreign'
    data['decisions'][0]['forecasts'].append(sibling)
    report=evaluate(data,protocol)
    assert report['coverage']['scored_decisions']==0
    assert report['decision_exclusion_counts']['missing_duplicate_or_unexpected_family']==1


def test_duplicate_reference_fails_closed_for_external_corrupt_input(opened):
    ledger,clock,_=opened;completed(ledger,clock)
    data,protocol=ledger.export_evaluation();extra=deepcopy(data['decisions'][0])
    extra['decision_id']+=':other';extra['forecasts'][0]['forecast_id']+=':other'
    data['decisions'].append(extra)
    with pytest.raises(ValueError,match='duplicate_market_reference_epoch'):evaluate(data,protocol)


def test_protocol_must_select_exactly_one_bound_family():
    protocol=contract_fixture()['evaluation_protocol']
    other='foreign_price_only_family'
    protocol['cohorts'][other]=f'joint_price_news_v1_20260907.EUR_USD.{other}'
    with pytest.raises(ValueError,match='one_exact_local_family'):validate_protocol(protocol)


def test_evaluator_rejects_cached_inputs_that_expired_before_issue(opened):
    ledger,clock,_=opened;completed(ledger,clock)
    data,protocol=ledger.export_evaluation();arm=data['decisions'][0]['forecasts'][0]
    arm['feature_cutoff_epoch']=arm['issued_epoch']-900.001
    report=evaluate(data,protocol)
    assert report['coverage']['scored_decisions']==0
    assert 'input_stale_at_issue' in report['exclusions'][0]['forecast_errors'][ledger.contract['family']]


@pytest.mark.parametrize('field',['input_source_observed_epoch','computation_started_epoch','computation_completed_epoch'])
def test_actual_capture_and_computation_clocks_are_retained_and_required(opened,field):
    ledger,clock,_=opened;completed(ledger,clock)
    data,protocol=ledger.export_evaluation();data['decisions'][0]['forecasts'][0].pop(field)
    report=evaluate(data,protocol)
    assert report['coverage']['scored_decisions']==0
    assert 'missing_or_invalid_computation_clocks' in report['exclusions'][0]['forecast_errors'][ledger.contract['family']]


@pytest.mark.parametrize('field', ['news_evidence_epoch','news_generated_epoch','news_first_observed_epoch',
    'news_available_epoch','news_expires_epoch','news_capture_sha256','training_news_available_max_epoch',
    'training_news_feature_cutoff_max_epoch'])
def test_external_score_input_requires_news_provenance(opened,field):
    ledger,clock,_=opened;completed(ledger,clock)
    data,protocol=ledger.export_evaluation();data['decisions'][0]['forecasts'][0].pop(field)
    report=evaluate(data,protocol)
    assert report['coverage']['scored_decisions']==0
    assert report['decision_exclusion_counts']['forecast_clock_identity_or_value_invalid']==1


@pytest.mark.parametrize('kind',['stale','expired','ack_retimestamped','decision_hash','future_training',
    'training_diagnostics','boolean_clock'])
def test_external_clock_or_news_binding_contamination_cannot_score(opened,kind):
    ledger,clock,_=opened;completed(ledger,clock)
    data,protocol=ledger.export_evaluation();decision=data['decisions'][0];arm=decision['forecasts'][0]
    if kind=='stale':arm['news_evidence_epoch']=arm['issued_epoch']-300.001
    elif kind=='expired':arm['news_expires_epoch']=arm['issued_epoch']-.001
    elif kind=='ack_retimestamped':arm['news_available_epoch']+=.1
    elif kind=='decision_hash':decision['news_capture_sha256']='f'*64
    elif kind=='future_training':arm['training_news_available_max_epoch']=arm['training_news_feature_cutoff_max_epoch']+1
    elif kind=='training_diagnostics':arm['diagnostics']['training_label_maturity_max_epoch']-=1
    elif kind=='boolean_clock':arm['news_generated_epoch']=True
    report=evaluate(data,protocol)
    assert report['coverage']['scored_decisions']==0
    assert report['decision_exclusion_counts']['forecast_clock_identity_or_value_invalid']==1


@pytest.mark.parametrize('age',[300,301,None,True])
def test_news_tolerance_is_exactly_registered_300_seconds(age):
    protocol=contract_fixture()['evaluation_protocol'];protocol['maximum_news_age_sec']=age
    if age==300:validate_protocol(protocol)
    else:
        with pytest.raises(ValueError):validate_protocol(protocol)


@pytest.mark.parametrize('neutral_side',[-1,0,1])
def test_comparators_use_exact_same_executable_quotes_and_denominator(opened,neutral_side):
    ledger,clock,_=opened;attempt,reference,capture,result=ready_attempt(ledger,clock)
    diag=result['predictions'][ledger.contract['family']]['diagnostics']
    diag['matched_price_only_expected_pips']=float(Decimal('.001')/Decimal(str(ledger.contract['pip_size'])))
    diag['neutral_news_ablation_expected_pips']=neutral_side*diag['matched_price_only_expected_pips']
    ledger.issue(attempt,capture,result,after_commit=clock.advance)
    clock.advance();ledger.consume();clock.advance();entry=quote(ledger,clock,bid='1.1004',ask='1.1006');ledger.settle()
    clock.value=reference['market_epoch']+3600;target=quote(ledger,clock,bid='1.1010',ask='1.1012');ledger.settle()
    data,protocol=ledger.export_evaluation();report=evaluate(data,protocol)
    assert report['coverage']['scored_decisions']==report['ablation_comparison']['decisions']==1
    paired=report['decisions'][0]['ablation_comparison'];scores=paired['scores'];joint=report['decisions'][0]['scores'][ledger.contract['family']]
    assert paired['reference_quote_id']==reference['quote_id'] and paired['entry_quote_id']==entry['quote_id']
    assert paired['target_quote_id']==target['quote_id'] and paired['target_epoch']==reference['market_epoch']+3600
    # The nonterminating pips-to-bps ratio is rounded at the documented80-digit
    # metric boundary before the unchanged exact-price scorer receives it.
    assert scores['matched_price_only']['absolute_error_bps']<Decimal('1e-75')
    assert scores['matched_price_only']['net_price_move']==Decimal('.0004')
    expected_net={-1:Decimal('-.0008'),0:Decimal('0'),1:Decimal('.0004')}[neutral_side]
    assert scores['neutral_news_ablation']['net_price_move']==expected_net
    assert scores['neutral_news_ablation']['side']==neutral_side
    for name,score in scores.items():
        assert score['probability_up'] is None and score['brier'] is None
        assert 'probability_direction' not in score and 'up_label' not in score
        summary=report['ablation_comparison']['comparators'][name]
        assert summary['decisions']==1 and summary['mean_brier'] is None
        with localcontext() as context:
            context.prec=80
            assert summary['mean_joint_minus_comparator_absolute_error_bps']==joint['absolute_error_bps']-score['absolute_error_bps']
            assert summary['mean_joint_minus_comparator_net_bps']==joint['net_bps']-score['net_bps']
    assert set(report['summaries'])=={ledger.contract['family'],'fair_coin','zero_move','no_trade','rolling_class_rate'}


@pytest.mark.parametrize('field',['matched_price_only_expected_pips','neutral_news_ablation_expected_pips'])
def test_missing_comparison_cannot_silently_shrink_paired_sample(opened,field):
    ledger,clock,_=opened;completed(ledger,clock)
    data,protocol=ledger.export_evaluation();data['decisions'][0]['forecasts'][0]['diagnostics'].pop(field)
    report=evaluate(data,protocol)
    assert report['coverage']['scored_decisions']==report['ablation_comparison']['decisions']==0
    assert 'missing_or_invalid_preissue_comparison_prediction' in report['exclusions'][0]['forecast_errors'][ledger.contract['family']]
    assert all(value['decisions']==0 for value in report['ablation_comparison']['comparators'].values())


def test_exact_json_comparator_magnitudes_retain_all_supplied_digits():
    from oanda_fixed_forecast_evaluation_joint_news_v1 import loads_exact_prices
    value=loads_exact_prices('{"diagnostics":{"matched_price_only_expected_pips":1.2345678901234567890123456789,"neutral_news_ablation_expected_pips":-0.12345678901234567890123456789}}')
    assert value['diagnostics']['matched_price_only_expected_pips']==Decimal('1.2345678901234567890123456789')
    assert value['diagnostics']['neutral_news_ablation_expected_pips']==Decimal('-.12345678901234567890123456789')
