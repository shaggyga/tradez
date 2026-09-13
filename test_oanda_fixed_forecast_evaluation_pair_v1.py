"""Exact scoring and pair isolation under the generic frozen protocol."""
from copy import deepcopy
from decimal import Decimal, localcontext
import json

import pytest

from oanda_fixed_forecast_evaluation_pair_v1 import (
    evaluate, exact_baselines, jsonable, loads_exact_prices, validate_protocol,
)
from test_oanda_causal_forecast_ledger_pair_v1 import (
    completed, contract_fixture, FAMILIES, minimal_capture_validation_seam, opened,
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
    assert report['coverage']['paired_scored_decisions'] == 0
    assert sum(report['quote_exclusions'].values()) == 1


@pytest.mark.parametrize('field,value', [('instrument', 'GBP_USD'), ('pip_size', .001),
    ('pip_size', None), ('cohort_id', 'causal_pair_local_v1_20260907.GBP_USD.ridge_return_repaired')])
def test_wrong_pair_forecast_invalidates_paired_comparison(opened, field, value):
    ledger, clock, _ = opened
    completed(ledger, clock)
    data, protocol = ledger.export_evaluation()
    data['decisions'][0]['forecasts'][0][field] = value
    report = evaluate(data, protocol)
    assert report['coverage']['paired_scored_decisions'] == 0
    assert report['decision_exclusion_counts']['forecast_clock_identity_or_value_invalid'] == 1


@pytest.mark.parametrize('field,value', [('instrument', 'GBP_USD'), ('instrument', None),
    ('pip_size', .001), ('pip_size', None)])
def test_decision_identity_cannot_disagree_with_its_forecasts(opened, field, value):
    ledger, clock, _ = opened
    completed(ledger, clock)
    data, protocol = ledger.export_evaluation()
    data['decisions'][0][field] = value
    report = evaluate(data, protocol)
    assert report['coverage']['paired_scored_decisions'] == 0
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
