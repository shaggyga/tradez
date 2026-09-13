"""Adversarial fixture-only checks of successor publication and quote clocks."""
from copy import deepcopy
import json
from pathlib import Path
import sqlite3
import zlib

import pytest

from oanda_causal_forecast_ledger import CausalForecastLedger, FAMILIES, SCHEMA, digest
from oanda_fixed_forecast_evaluation import evaluate
import oanda_causal_forecast_inputs as input_module


class Clock:
    def __init__(self, value=1_800_000_000.0):
        self.value = value

    def __call__(self):
        return self.value

    def advance(self, seconds=1):
        self.value += seconds
        return self.value


def contract_fixture():
    cohorts = {family: 'causal_four_family_v1_20260906.fixture.' + family for family in FAMILIES}
    protocol = {
        'schema_version': 'fixed_forecast_evaluation_protocol_v1',
        'contract_id': 'fixture-four-family-evaluation',
        'proof_eligible': False, 'account_eligible': False, 'collection_enabled': False,
        'instrument': 'EUR_USD', 'input_timeframe': 'M1', 'horizon_sec': 3600,
        'historical_start_utc': '2026-01-01T00:00:00+00:00',
        'historical_end_utc': '2027-01-01T00:00:00+00:00',
        'model_version': 'sha256:' + 'a' * 64,
        'feature_version': 'sha256:' + 'b' * 64,
        'cohorts': cohorts,
        'baselines': ['fair_coin', 'zero_move', 'no_trade', 'rolling_class_rate'],
        'rolling_lookback': 100, 'rolling_min_labels': 20,
        'quote_max_age_sec': 60, 'maximum_entry_delay_sec': 60,
        'maximum_target_quote_delay_sec': 60,
        'extra_cost_stress_bps': [0.0, .5, 1.0],
    }
    return {
        'schema_version': SCHEMA, 'contract_id': 'fixture-separate-collection-v1',
        'research_only': True, 'account_eligible': False, 'proof_eligible': False,
        'can_place_orders': False, 'can_authorize': False, 'can_promote': False,
        'historical_rows_imported': False,
        'instrument': 'EUR_USD', 'input_timeframe': 'M1', 'horizon_sec': 3600,
        'cohorts': deepcopy(cohorts), 'evaluation_protocol': protocol,
        'model_version': protocol['model_version'], 'feature_version': protocol['feature_version'],
        'numeric_model_source_sha256': 'c' * 64, 'dependency_versions': {'python': 'fixture-only'},
        'cadence_sec': 900, 'maximum_build_sec': 120,
        'quote_max_age_sec': 60, 'maximum_entry_delay_sec': 60,
        'maximum_target_quote_delay_sec': 60,
    }


@pytest.fixture(autouse=True)
def minimal_capture_validation_seam(monkeypatch):
    """Boundary fixtures omit candle bytes; real-capture integration is unpatched."""
    real_validation = input_module.validate_capture

    def check(capture):
        if capture.get('fixture_only') is True:
            return
        return real_validation(capture)

    monkeypatch.setattr(input_module, 'validate_capture', check)


@pytest.fixture
def opened(tmp_path):
    clock = Clock()
    contract = contract_fixture()
    ledger = CausalForecastLedger(tmp_path / 'fresh.sqlite', contract, clock=clock, activate=True)
    try:
        yield ledger, clock, contract
    finally:
        ledger.close()


def quote(ledger, clock, *, market=None, available=None, bid=1.1000, ask=1.1002, **changes):
    payload = {
        'instrument': 'EUR_USD', 'market_epoch': clock.value if market is None else market,
        'available_epoch': clock.value if available is None else available,
        'bid': bid, 'ask': ask, 'tradeable': True,
    }
    payload.update(changes)
    return ledger.observe_quote(payload)


def ready_attempt(ledger, clock):
    clock.advance()
    reference = quote(ledger, clock)
    bucket = int(reference['market_epoch'] // ledger.contract['cadence_sec'])
    assert ledger.reserve_attempt(bucket, reference)
    clock.advance()
    capture = {
        'status': 'ready', 'first_observed_epoch': clock.value, 'fixture_only': True,
        'max_bar_close_epoch': clock.value - 1,
        'model_source_sha256': ledger.contract['numeric_model_source_sha256'],
        'dependency_versions': deepcopy(ledger.contract['dependency_versions']),
        'research_only': True, 'account_eligible': False, 'can_place_orders': False,
    }
    capture['source_capture_sha256'] = digest(capture)
    result = {
        'status': 'ready', 'model_source_sha256': capture['model_source_sha256'],
        'dependency_versions': deepcopy(capture['dependency_versions']),
        'source_capture_sha256': capture['source_capture_sha256'],
        'input_capture_sha256': digest(capture),
        'computed_epoch': clock.value + .5,
        'predictions': {
            family: {'probability_up': .75, 'expected_signed_pips': 2.0, 'side': 1,
                     'diagnostics': {'fixture_only': True}}
            for family in FAMILIES
        },
    }
    clock.advance()
    return bucket, reference, capture, result


def issued(ledger, clock):
    bucket, reference, capture, result = ready_attempt(ledger, clock)
    decision_id = ledger.publish(bucket, capture, result, after_commit=clock.advance)
    clock.advance()
    ledger.consume_publications()
    return decision_id, reference, capture, result


def row(ledger, table):
    return dict(ledger.db.execute('SELECT * FROM ' + table).fetchone())


def completed(ledger, clock, *, side=1):
    bucket, reference, capture, result = ready_attempt(ledger, clock)
    for prediction in result['predictions'].values():
        prediction['side'] = side
    identity = ledger.publish(bucket, capture, result, after_commit=clock.advance)
    clock.advance()
    ledger.consume_publications()
    clock.advance()
    entry = quote(ledger, clock, bid=1.1004, ask=1.1006)
    ledger.settle()
    clock.value = reference['market_epoch'] + 3600
    target = quote(ledger, clock, bid=1.1010, ask=1.1012)
    ledger.settle()
    return identity, reference, entry, target


def test_requires_explicit_activation_and_retains_actual_clock(tmp_path):
    path = tmp_path / 'new.sqlite'
    clock = Clock()
    with pytest.raises(ValueError, match='activation'):
        CausalForecastLedger(path, contract_fixture(), clock=clock)
    clock.advance(100)
    ledger = CausalForecastLedger(path, contract_fixture(), clock=clock, activate=True)
    try:
        assert ledger.activated_epoch == clock.value
        assert row(ledger, 'activation')['contract_sha'] == digest(contract_fixture())
    finally:
        ledger.close()


def test_no_forecast_publication_visible_before_commit(opened):
    ledger, clock, _ = opened
    bucket, reference, capture, result = ready_attempt(ledger, clock)
    checks = []

    def committed_but_not_receipted():
        with sqlite3.connect(ledger.path.as_uri() + '?mode=ro', uri=True) as reader:
            checks.append((reader.execute('SELECT COUNT(*) FROM forecasts').fetchone()[0],
                           reader.execute('SELECT COUNT(*) FROM publication').fetchone()[0]))
        clock.advance()

    identity = ledger.publish(bucket, capture, result, after_commit=committed_but_not_receipted)
    assert checks == [(1, 0)]
    assert row(ledger, 'publication')['id'] == identity
    assert ledger.counts()['consumption'] == ledger.counts()['entries'] == 0
    payload = json.loads(row(ledger, 'forecasts')['payload'])
    assert payload['target_epoch'] == reference['market_epoch'] + 3600
    assert row(ledger, 'publication')['epoch'] > payload['forecasts'][0]['issued_epoch']


def test_independent_reader_cannot_see_uncommitted_consumer_row(opened):
    ledger, clock, _ = opened
    identity, _, _, _ = issued(ledger, clock)
    ledger.db.execute('BEGIN')
    try:
        ledger.db.execute('INSERT INTO diagnostics VALUES(?,?,?)', (99999999, clock.value, '{}'))
        assert ledger._read('SELECT * FROM diagnostics WHERE bucket=99999999') == []
        assert ledger._read('SELECT id FROM consumption WHERE id=?', (identity,))[0]['id'] == identity
    finally:
        ledger.db.rollback()


def test_crash_after_forecast_commit_recovers_at_actual_later_time(tmp_path):
    clock = Clock()
    path = tmp_path / 'crash.sqlite'
    contract = contract_fixture()
    ledger = CausalForecastLedger(path, contract, clock=clock, activate=True)
    bucket, reference, capture, result = ready_attempt(ledger, clock)

    def crash():
        raise RuntimeError('fixture crash after forecast commit')

    with pytest.raises(RuntimeError, match='fixture crash'):
        ledger.publish(bucket, capture, result, after_commit=crash)
    original = row(ledger, 'forecasts')
    assert ledger.counts()['publication'] == 0
    ledger.close()
    clock.advance(90)
    reopened = CausalForecastLedger(path, contract, clock=clock)
    try:
        reopened.recover_publications()
        assert row(reopened, 'publication')['epoch'] == clock.value
        assert row(reopened, 'forecasts') == original
        clock.advance()
        reopened.consume_publications()
        clock.advance()
        entry = quote(reopened, clock)
        reopened.settle()
        assert row(reopened, 'entries')['quote_id'] == entry['quote_id']
        assert json.loads(original['payload'])['target_epoch'] == reference['market_epoch'] + 3600
    finally:
        reopened.close()


def test_recovery_after_target_excludes_without_resetting_horizon(tmp_path):
    clock = Clock()
    path = tmp_path / 'late.sqlite'
    contract = contract_fixture()
    ledger = CausalForecastLedger(path, contract, clock=clock, activate=True)
    bucket, reference, capture, result = ready_attempt(ledger, clock)
    with pytest.raises(RuntimeError):
        ledger.publish(bucket, capture, result, after_commit=lambda: (_ for _ in ()).throw(RuntimeError()))
    ledger.close()
    clock.value = reference['market_epoch'] + 3601
    reopened = CausalForecastLedger(path, contract, clock=clock)
    try:
        reopened.settle()
        assert row(reopened, 'publication')['epoch'] == clock.value
        assert row(reopened, 'exclusions')['reason'] == 'missing_later_entry_before_deadline'
        assert reopened.counts()['entries'] == reopened.counts()['outcomes'] == 0
        assert row(reopened, 'forecasts')['target'] == reference['market_epoch'] + 3600
    finally:
        reopened.close()


def test_only_strictly_later_market_tick_can_enter(opened):
    ledger, clock, _ = opened
    _, reference, _, _ = issued(ledger, clock)
    consumed = clock.value
    clock.advance()
    equal_market = quote(ledger, clock, market=consumed)
    ledger.settle()
    assert ledger.counts()['entries'] == 0
    clock.advance()
    valid = quote(ledger, clock)
    ledger.settle()
    assert row(ledger, 'entries')['quote_id'] == valid['quote_id']
    clock.value = reference['market_epoch'] + 3600
    quote(ledger, clock, bid=1.1010, ask=1.1012)
    ledger.settle()
    data, protocol = ledger.export_evaluation()
    report = evaluate(data, protocol)
    assert report['coverage']['paired_scored_decisions'] == 1
    assert report['decisions'][0]['entry']['quote_id'] == valid['quote_id']
    assert report['decisions'][0]['entry']['quote_id'] != equal_market['quote_id']


def test_duplicate_tick_preserves_first_actual_observation(opened):
    ledger, clock, _ = opened
    clock.advance()
    original = quote(ledger, clock)
    clock.advance(10)
    repeated = quote(ledger, clock, market=original['market_epoch'])
    assert repeated == original
    assert ledger.counts()['quotes'] == 1
    assert row(ledger, 'quotes')['available'] == original['available_epoch']


def test_numeric_quote_identity_cannot_reset_first_observation(opened):
    ledger, clock, _ = opened
    clock.advance()
    original = quote(ledger, clock, market=int(clock.value), bid=1, ask=1)
    clock.advance()
    duplicate = quote(ledger, clock, market=float(original['market_epoch']), bid=1.0, ask=1.0)
    assert duplicate == original
    assert ledger.counts()['quotes'] == 1


def test_supplied_past_observation_cannot_backdate_new_quote(opened):
    ledger, clock, _ = opened
    clock.advance(20)
    now = clock.value
    try:
        retained = quote(ledger, clock, market=now - 5, available=now - 4)
    except ValueError:
        assert ledger.counts()['quotes'] == 0
    else:
        assert retained['available_epoch'] >= now


@pytest.mark.parametrize('changes', [
    {'instrument': 'GBP_USD'}, {'tradeable': False}, {'tradeable': 1},
    {'bid': 0}, {'bid': 1.2, 'ask': 1.1}, {'bid': float('nan')},
    {'ask': float('inf')}, {'available_epoch': True},
])
def test_invalid_quotes_rejected(opened, changes):
    ledger, clock, _ = opened
    clock.advance()
    with pytest.raises(ValueError):
        quote(ledger, clock, **changes)
    assert ledger.counts()['quotes'] == 0


def test_stale_and_preactivation_quotes_rejected(opened):
    ledger, clock, _ = opened
    with pytest.raises(ValueError):
        quote(ledger, clock)
    clock.advance(100)
    with pytest.raises(ValueError):
        quote(ledger, clock, market=clock.value-61)
    with pytest.raises(ValueError):
        quote(ledger, clock, market=clock.value+1)


def test_stale_reference_and_forged_reference_cannot_reserve(opened):
    ledger, clock, _ = opened
    clock.advance()
    reference = quote(ledger, clock)
    bucket = int(clock.value // ledger.contract['cadence_sec'])
    forged = dict(reference, bid=reference['bid']+.1)
    with pytest.raises(ValueError, match='reference_not_observed'):
        ledger.reserve_attempt(bucket, forged)
    clock.advance(61)
    with pytest.raises(ValueError, match='stale_reference'):
        ledger.reserve_attempt(bucket, reference)
    assert ledger.counts()['attempts'] == 0


def test_cadence_reservation_prevents_reranking_same_attempt(opened):
    ledger, clock, _ = opened
    bucket, reference, capture, result = ready_attempt(ledger, clock)
    assert ledger.reserve_attempt(bucket, reference) is False
    ledger.publish(bucket, capture, result)
    with pytest.raises((ValueError, sqlite3.IntegrityError)):
        ledger.publish(bucket, capture, result)
    assert ledger.counts()['forecasts'] == 1


def test_arbitrary_bucket_cannot_bypass_fixed_cadence(opened):
    ledger, clock, _ = opened
    clock.advance()
    reference = quote(ledger, clock)
    valid_bucket = int(clock.value // ledger.contract['cadence_sec'])
    with pytest.raises(ValueError):
        ledger.reserve_attempt(valid_bucket + 1, reference)
    assert ledger.counts()['attempts'] == 0
    assert ledger.reserve_attempt(valid_bucket, reference)


@pytest.mark.parametrize('field,value', [
    ('probability_up', -0.01), ('probability_up', 1.01), ('probability_up', float('nan')),
    ('probability_up', True), ('expected_signed_pips', float('inf')),
    ('side', True), ('side', 1.0), ('side', 2),
])
def test_invalid_model_output_never_partially_publishes(opened, field, value):
    ledger, clock, _ = opened
    bucket, _, capture, result = ready_attempt(ledger, clock)
    result['predictions'][FAMILIES[-1]][field] = value
    with pytest.raises(ValueError):
        ledger.publish(bucket, capture, result)
    assert ledger.counts()['inputs'] == ledger.counts()['forecasts'] == 0
    assert ledger.counts()['publication'] == 0


def test_incomplete_family_set_is_one_abstention(opened):
    ledger, clock, _ = opened
    bucket, _, capture, result = ready_attempt(ledger, clock)
    del result['predictions'][FAMILIES[-1]]
    with pytest.raises(ValueError, match='all_four_families_required'):
        ledger.publish(bucket, capture, result)
    assert ledger.counts()['forecasts'] == 0


@pytest.mark.parametrize('kind', ['future_capture', 'future_maturity', 'pre_attempt_capture', 'slow_build'])
def test_unavailable_or_late_inputs_fail_closed(opened, kind):
    ledger, clock, _ = opened
    bucket, _, capture, result = ready_attempt(ledger, clock)
    if kind == 'future_capture': capture['first_observed_epoch'] = clock.value+1
    if kind == 'future_maturity': capture['max_bar_close_epoch'] = clock.value+1
    if kind == 'pre_attempt_capture': capture['first_observed_epoch'] = row(ledger, 'attempts')['epoch']-.5
    if kind == 'slow_build': clock.advance(301)
    with pytest.raises(ValueError):
        ledger.publish(bucket, capture, result)
    assert ledger.counts()['forecasts'] == ledger.counts()['publication'] == 0


@pytest.mark.parametrize('kind', ['result_capture', 'result_source', 'result_dependencies'])
def test_forged_or_mixed_capture_result_rejected(opened, kind):
    ledger, clock, _ = opened
    bucket, _, capture, result = ready_attempt(ledger, clock)
    if kind == 'result_capture': result['source_capture_sha256'] = 'd'*64
    if kind == 'result_source': result['model_source_sha256'] = 'd'*64
    if kind == 'result_dependencies': result['dependency_versions'] = {'python': 'different'}
    with pytest.raises(ValueError):
        ledger.publish(bucket, capture, result)
    assert ledger.counts()['forecasts'] == 0


@pytest.mark.parametrize('computed', ['before_capture', 'at_issue', 'after_issue', 'not_numeric'])
def test_model_completion_must_precede_original_issue(opened, computed):
    ledger, clock, _ = opened
    bucket, _, capture, result = ready_attempt(ledger, clock)
    result['computed_epoch'] = {
        'before_capture': capture['first_observed_epoch'] - 1,
        'at_issue': clock.value,
        'after_issue': clock.value + 1,
        'not_numeric': True,
    }[computed]
    with pytest.raises(ValueError):
        ledger.publish(bucket, capture, result)
    assert ledger.counts()['forecasts'] == 0


@pytest.mark.parametrize('forge_capture', [False, True])
def test_real_captured_bytes_and_numerical_models_integrate_without_clock_substitution(tmp_path, forge_capture):
    # This path uses the actual CSV capture validator and numerical functions.
    # Both source and ledger databases are synthetic fixtures under tmp_path.
    from trad.test_oanda_causal_forecast_inputs import START, write_fixture

    candles = tmp_path / 'candles'
    candles.mkdir()
    write_fixture(candles, count=335)
    clock = Clock(START + 335*60 - 1)
    contract = contract_fixture()
    contract['numeric_model_source_sha256'] = input_module.NUMERICAL_SOURCE_SHA256
    contract['dependency_versions'] = input_module.dependency_versions()
    ledger = CausalForecastLedger(tmp_path/'real-capture.sqlite', contract, clock=clock, activate=True)
    try:
        clock.advance()
        reference = quote(ledger, clock)
        bucket = int(clock.value // contract['cadence_sec'])
        assert ledger.reserve_attempt(bucket, reference)
        clock.advance()
        capture = input_module.capture_inputs(candles, clock=clock)
        assert capture['status'] == 'ready', capture['reasons']
        clock.advance()
        result = input_module.compute_predictions(capture, clock=clock)
        assert result['status'] == 'ready', result['reasons']
        clock.advance()
        if forge_capture:
            capture['max_bar_close_epoch'] -= 60
            with pytest.raises(ValueError, match='input_capture_hash_mismatch'):
                ledger.publish(bucket, capture, result)
            assert ledger.counts()['forecasts'] == 0
            return
        ledger.publish(bucket, capture, result, after_commit=clock.advance)
        clock.advance()
        ledger.consume_publications()
        clock.advance()
        quote(ledger, clock, bid=1.1004, ask=1.1006)
        ledger.settle()
        clock.value = reference['market_epoch'] + 3600
        quote(ledger, clock, bid=1.1010, ask=1.1012)
        ledger.settle()
        data, protocol = ledger.export_evaluation()
        report = evaluate(data, protocol)
        assert report['coverage']['paired_scored_decisions'] == 1
        forecasts = data['decisions'][0]['forecasts']
        assert len(forecasts) == 4
        for forecast in forecasts:
            assert forecast['input_source_observed_epoch'] == capture['first_observed_epoch']
            assert forecast['features_available_epoch'] == result['computed_epoch']
            assert forecast['training_labels_available_max_epoch'] == result['computed_epoch']
            assert forecast['training_label_maturity_max_epoch'] == capture['max_bar_close_epoch']
            assert result['computed_epoch'] < forecast['issued_epoch'] < forecast['committed_available_epoch']
            assert forecast['target_epoch'] == reference['market_epoch'] + 3600
            assert forecast['proof_eligible'] is forecast['account_eligible'] is False
    finally:
        ledger.close()


def test_late_entry_is_excluded_once(opened):
    ledger, clock, _ = opened
    issued(ledger, clock)
    clock.advance(61)
    quote(ledger, clock)
    ledger.settle()
    assert row(ledger, 'exclusions')['reason'] == 'missing_later_entry_before_deadline'
    assert ledger.counts()['entries'] == 0
    ledger.settle()
    assert ledger.counts()['exclusions'] == 1


def test_entry_at_delay_boundary_is_valid(opened):
    ledger, clock, _ = opened
    issued(ledger, clock)
    clock.advance(60)
    valid = quote(ledger, clock)
    ledger.settle()
    assert row(ledger, 'entries')['quote_id'] == valid['quote_id']


def test_missing_original_target_quote_excluded_once(opened):
    ledger, clock, _ = opened
    _, reference, _, _ = issued(ledger, clock)
    clock.advance()
    quote(ledger, clock)
    ledger.settle()
    clock.value = reference['market_epoch'] + 3661
    quote(ledger, clock)
    ledger.settle()
    assert row(ledger, 'exclusions')['reason'] == 'missing_quote_at_original_target'
    assert ledger.counts()['outcomes'] == 0
    ledger.settle()
    assert ledger.counts()['exclusions'] == 1


def test_target_at_delay_boundary_is_valid(opened):
    ledger, clock, _ = opened
    _, reference, _, _ = issued(ledger, clock)
    clock.advance()
    quote(ledger, clock)
    ledger.settle()
    clock.value = reference['market_epoch'] + 3660
    target = quote(ledger, clock)
    ledger.settle()
    assert row(ledger, 'outcomes')['quote_id'] == target['quote_id']
    assert row(ledger, 'forecasts')['target'] == reference['market_epoch'] + 3600


def test_pre_target_market_tick_cannot_become_target(opened):
    ledger, clock, _ = opened
    _, reference, _, _ = issued(ledger, clock)
    clock.advance()
    quote(ledger, clock)
    ledger.settle()
    clock.value = reference['market_epoch'] + 3600
    quote(ledger, clock, market=clock.value-.1)
    ledger.settle()
    assert ledger.counts()['outcomes'] == 0
    clock.advance()
    target = quote(ledger, clock)
    ledger.settle()
    assert row(ledger, 'outcomes')['quote_id'] == target['quote_id']


def test_earliest_eligible_quotes_remain_selected_even_when_later_prices_improve(opened):
    ledger, clock, _ = opened
    _, reference, _, _ = issued(ledger, clock)
    clock.advance()
    first_entry = quote(ledger, clock, bid=1.1010, ask=1.1012)
    clock.advance()
    quote(ledger, clock, bid=1.1000, ask=1.1002)
    ledger.settle()
    assert row(ledger, 'entries')['quote_id'] == first_entry['quote_id']
    clock.value = reference['market_epoch'] + 3600
    first_target = quote(ledger, clock, bid=1.1000, ask=1.1002)
    clock.advance()
    quote(ledger, clock, bid=1.1010, ask=1.1012)
    ledger.settle()
    assert row(ledger, 'outcomes')['quote_id'] == first_target['quote_id']
    data, protocol = ledger.export_evaluation()
    scored = evaluate(data, protocol)['decisions'][0]
    assert scored['entry']['quote_id'] == first_entry['quote_id']
    assert scored['target']['quote_id'] == first_target['quote_id']


@pytest.mark.parametrize('side', [1, -1, 0])
def test_all_four_cohorts_export_and_bid_ask_stress_costs(opened, side):
    ledger, clock, contract = opened
    identity, reference, entry, target = completed(ledger, clock, side=side)
    data, protocol = ledger.export_evaluation()
    report = evaluate(data, protocol)
    assert report['coverage']['paired_scored_decisions'] == 1
    assert report['coverage']['total_forecasts'] == 4
    assert report['proof_eligible'] is report['account_eligible'] is False
    assert report['independent_sample_size'] is None
    decision = report['decisions'][0]
    assert decision['decision_id'] == identity
    assert decision['target_epoch'] == reference['market_epoch'] + 3600
    assert decision['actual_holding_sec'] < 3600
    expected_raw = target['bid']-entry['ask'] if side == 1 else entry['bid']-target['ask'] if side == -1 else 0
    expected_net = 10000*expected_raw/((entry['bid']+entry['ask'])/2)
    for family in FAMILIES:
        assert report['per_family_coverage'][family]['paired_scored'] == 1
        score = decision['scores'][family]
        assert score['side'] == side
        assert score['net_bps'] == pytest.approx(expected_net)
        assert score['stress_net_bps']['0.5'] == pytest.approx(expected_net - .5*bool(side))
        assert score['stress_net_bps']['1.0'] == pytest.approx(expected_net - 1.0*bool(side))
    assert decision['scores']['no_trade']['net_bps'] == 0
    assert decision['rolling_baseline_training']['probability_up'] == .5
    assert {f['cohort_id'] for f in data['decisions'][0]['forecasts']} == set(contract['cohorts'].values())
    stored_input = row(ledger, 'inputs')
    assert digest(json.loads(zlib.decompress(stored_input['payload']))) == stored_input['id']


def test_export_rejects_forged_reference_and_target_shift(opened):
    ledger, clock, _ = opened
    completed(ledger, clock)
    data, protocol = ledger.export_evaluation()
    forged = deepcopy(data)
    forged['decisions'][0]['forecasts'][0]['reference_mid'] += .01
    report = evaluate(forged, protocol)
    assert report['coverage']['paired_scored_decisions'] == 0
    assert report['decision_exclusion_counts']['forecast_reference_price_mismatch'] == 1
    forged = deepcopy(data)
    forged['decisions'][0]['target_epoch'] += 100
    report = evaluate(forged, protocol)
    assert report['coverage']['paired_scored_decisions'] == 0
    assert report['decision_exclusion_counts']['target_shifted_from_original_reference'] == 1


def test_export_uses_independently_committed_database_snapshot(opened):
    ledger, clock, _ = opened
    completed(ledger, clock)
    ledger.db.execute('BEGIN')
    try:
        ledger.db.execute('INSERT INTO diagnostics VALUES(?,?,?)', (99999999, clock.value, '{}'))
        data, _ = ledger.export_evaluation()
        assert data['collection_counts']['diagnostics'] == 0
        assert ledger.db.in_transaction
        assert ledger.counts()['diagnostics'] == 1
    finally:
        ledger.db.rollback()


def test_unconsumed_forecasts_are_visible_as_exclusions_in_strict_export(opened):
    ledger, clock, _ = opened
    bucket, _, capture, result = ready_attempt(ledger, clock)
    ledger.publish(bucket, capture, result)
    data, protocol = ledger.export_evaluation()
    report = evaluate(data, protocol)
    assert report['coverage']['total_decisions'] == 1
    assert report['coverage']['total_forecasts'] == 4
    assert report['coverage']['paired_scored_decisions'] == 0
    assert report['decision_exclusion_counts']['forecast_clock_identity_or_value_invalid'] == 1


def test_clock_regression_fails_before_new_evidence(opened):
    ledger, clock, _ = opened
    issued(ledger, clock)
    before = ledger.counts()
    clock.value -= 1
    with pytest.raises(ValueError, match='wall_clock_regression'):
        quote(ledger, clock)
    assert ledger.counts() == before
    with pytest.raises(ValueError, match='clock_regression'):
        ledger.export_evaluation()


def test_clock_regression_is_detected_after_reopen(tmp_path):
    clock = Clock()
    path = tmp_path/'clock.sqlite'
    contract = contract_fixture()
    ledger = CausalForecastLedger(path, contract, clock=clock, activate=True)
    issued(ledger, clock)
    ledger.close()
    clock.value -= 100
    reopened = CausalForecastLedger(path, contract, clock=clock)
    try:
        with pytest.raises(ValueError, match='clock_regression'):
            reopened.export_evaluation()
    finally:
        reopened.close()


@pytest.mark.parametrize('table', ['contract', 'activation', 'clocks', 'quotes', 'attempts',
                                 'diagnostics', 'inputs', 'forecasts', 'publication',
                                 'consumption', 'entries', 'outcomes'])
@pytest.mark.parametrize('operation', ['UPDATE', 'DELETE'])
def test_retained_rows_cannot_be_updated_or_deleted(opened, table, operation):
    ledger, clock, _ = opened
    completed(ledger, clock)
    ledger.diagnostic(123, {'fixture_only': True})
    first = row(ledger, table)
    column = next(iter(first))
    query = f'UPDATE {table} SET {column}={column}' if operation == 'UPDATE' else f'DELETE FROM {table}'
    with pytest.raises(sqlite3.IntegrityError, match='immutable_evidence'):
        with ledger.db:
            ledger.db.execute(query)
    assert row(ledger, table) == first


@pytest.mark.parametrize('operation', ['UPDATE', 'DELETE'])
def test_exclusions_are_immutable(opened, operation):
    ledger, clock, _ = opened
    issued(ledger, clock)
    clock.advance(61)
    ledger.settle()
    query = 'UPDATE exclusions SET reason=reason' if operation == 'UPDATE' else 'DELETE FROM exclusions'
    with pytest.raises(sqlite3.IntegrityError, match='immutable_evidence'):
        with ledger.db:
            ledger.db.execute(query)


def test_reopen_requires_identical_contract(tmp_path):
    clock = Clock()
    path = tmp_path/'contract.sqlite'
    original = contract_fixture()
    ledger = CausalForecastLedger(path, original, clock=clock, activate=True)
    ledger.close()
    changed = deepcopy(original)
    changed['contract_id'] += '-mutated'
    with pytest.raises(ValueError, match='immutable_contract_mismatch'):
        CausalForecastLedger(path, changed, clock=clock)


def test_callers_cannot_mutate_retained_contract(opened):
    ledger, clock, original = opened
    original['cohorts'][FAMILIES[0]] = 'silently-changed-cohort'
    assert ledger.contract['cohorts'][FAMILIES[0]] != 'silently-changed-cohort'
    assert digest(ledger.contract) == ledger.contract_hash


@pytest.mark.parametrize('key,value', [
    ('research_only', False), ('account_eligible', True), ('proof_eligible', True),
    ('can_authorize', True), ('can_promote', True), ('can_place_orders', True),
    ('historical_rows_imported', True), ('quote_max_age_sec', 0),
    ('maximum_entry_delay_sec', -1), ('maximum_target_quote_delay_sec', float('inf')),
    ('maximum_build_sec', True), ('cadence_sec', 0),
    ('input_timeframe', 'M5'),
])
def test_unsafe_or_unbounded_contract_rejected(tmp_path, key, value):
    contract = contract_fixture()
    contract[key] = value
    with pytest.raises(ValueError):
        ledger = CausalForecastLedger(tmp_path/'invalid.sqlite', contract, clock=Clock(), activate=True)
        ledger.close()


@pytest.mark.parametrize('key', ['cohorts', 'model_version', 'feature_version', 'input_timeframe', 'quote_max_age_sec',
                               'maximum_entry_delay_sec', 'maximum_target_quote_delay_sec'])
def test_evaluation_and_collection_contracts_cannot_diverge(tmp_path, key):
    contract = contract_fixture()
    if key == 'cohorts': contract['evaluation_protocol'][key][FAMILIES[0]] = 'different'
    elif key in ('model_version', 'feature_version', 'input_timeframe'): contract['evaluation_protocol'][key] = 'different'
    else: contract['evaluation_protocol'][key] += 1
    with pytest.raises(ValueError):
        ledger = CausalForecastLedger(tmp_path/'mismatch.sqlite', contract, clock=Clock(), activate=True)
        ledger.close()


def test_unrelated_existing_database_is_not_modified(tmp_path):
    path = tmp_path/'unrelated.sqlite'
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE important_user_data(value TEXT)')
        db.execute("INSERT INTO important_user_data VALUES('preserve')")
    before = path.read_bytes()
    with pytest.raises(ValueError):
        ledger = CausalForecastLedger(path, contract_fixture(), clock=Clock(), activate=True)
        ledger.close()
    assert path.read_bytes() == before
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall() == [('important_user_data',)]
