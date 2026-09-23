from copy import deepcopy
from decimal import Context, localcontext
import json

import pytest

import oanda_m1_risk_distribution_contract_v1 as m
import test_oanda_m1_mba_research_capture_v1 as capture_fixture

REFERENCE = 1788939660


def training_artifact():
    fits = {}
    for pair in m.PAIRS:
        for horizon in m.labels.HORIZONS:
            for label in m.labels.CONTINUOUS_LABELS:
                values = [-1., 0., 2.] if label == 'signed_terminal_bps' else [0.1, 1., 3.]
                fits[f'{pair}/{horizon}/{label}'] = dict(
                    status='fitted_training_quantiles', fit_scope='training_only_common_support',
                    method='inverted_cdf', original_probability_available=False,
                    quantiles=list(m.QUANTILES), training_rows=1000,
                    training_origin_sha256='a' * 64,
                    training_reference_max_epoch=REFERENCE - 100000,
                    training_target_max_epoch=REFERENCE - 99000,
                    empirical=values, volatility_normalized=values)
    return dict(created_epoch=REFERENCE - 90000, fits=fits, research_only=True,
                can_place_orders=False, can_promote=False, can_authorize=False,
                execution_eligible=False, account_eligible=False, proof_eligible=False,
                forecast_issued=False)


@pytest.fixture
def inputs(monkeypatch):
    monkeypatch.setattr(capture_fixture, 'NOW', REFERENCE + 1.)
    raw, receipt, mapping = capture_fixture.make_capture(monkeypatch)
    fit_raw = m.canonical(training_artifact())
    # Synthetic training fixtures test contract semantics. The separately run
    # real-input preflight must use the production-pinned historical artifact.
    monkeypatch.setattr(m, 'TRAINING_ARTIFACT_SHA256', m.sha(fit_raw))
    return dict(input_raw=raw, input_receipt=receipt,
        metadata=capture_fixture.metadata(), fit_raw=fit_raw,
        input_consumption=dict(raw_sha256=m.sha(raw), receipt_sha256=m.digest(receipt),
                               read_started_epoch=REFERENCE + 2., read_completed_epoch=REFERENCE + 2.5),
        cohort_id=m.COHORT_ID, scheduled_reference_price_epoch=REFERENCE,
        expected_source_bindings={'oanda_m1_risk_distribution_contract_v1.py': '1' * 64})


def issue(inputs, clocks=None):
    ticks = iter(clocks or [REFERENCE + 3., REFERENCE + 4.])
    return m.issue_distribution(**inputs, clock=lambda: next(ticks))


def chain(inputs):
    issued = issue(inputs)
    pub = m.publication_receipt(issued, persisted_bytes_sha256=m.digest(issued),
        publication_started_epoch=REFERENCE + 4.5,
        expected_source_bindings=inputs['expected_source_bindings'], clock=lambda: REFERENCE + 5.)
    consumed = m.consumption_receipt(issued, pub, read_started_epoch=REFERENCE + 5.5,
        expected_source_bindings=inputs['expected_source_bindings'], clock=lambda: REFERENCE + 6.)
    return issued, pub, consumed


def validation_inputs(inputs):
    return {k: inputs[k] for k in ('input_raw', 'input_receipt', 'metadata', 'fit_raw',
                                   'expected_source_bindings')}


def reseal(value, key):
    value[key] = m.digest({k: v for k, v in value.items() if k != key})
    return value


def test_all_methods_labels_horizons_and_original_prices_are_retained(inputs):
    original = deepcopy(inputs)
    issued, pub, consumed = chain(inputs)
    assert inputs == original
    assert len(issued['nodes']) == 72
    assert len({(v['horizon_minutes'], v['label'], v['method']) for v in issued['nodes']}) == 72
    assert {v['target_price_epoch'] for v in issued['nodes']} == {REFERENCE + h * 60 for h in (15, 30, 60)}
    assert all(v['target_price_epoch'] == v['target_label_epoch'] + 60 for v in issued['nodes'])
    assert issued['origin_row']['price_epoch'] == REFERENCE
    assert issued['reference_mid'] == issued['origin_row']['mid']['close']
    assert all(issued[k] is expected for k, expected in m.AUTHORITY.items())
    assert issued['quantile_interpolation'] is False
    assert m.validate_chain(issued, pub, consumed, **validation_inputs(inputs)) == issued


def test_low_ambient_decimal_precision_does_not_change_predictions(inputs):
    normal = issue(inputs)
    with localcontext(Context(prec=3)):
        low = issue(inputs)
    assert low == normal


@pytest.mark.parametrize('pair', m.PAIRS)
def test_all_three_pair_price_conventions_reconstruct(inputs, monkeypatch, pair):
    raw, receipt, _ = capture_fixture.make_capture(monkeypatch, pair=pair)
    value = deepcopy(inputs)
    value.update(input_raw=raw, input_receipt=receipt, metadata=capture_fixture.metadata(pair))
    value['input_consumption'].update(raw_sha256=m.sha(raw), receipt_sha256=m.digest(receipt))
    issued = issue(value)
    assert issued['instrument'] == pair and len(issued['nodes']) == 72
    assert m.validate_issue(issued, **validation_inputs(value)) == issued


@pytest.mark.parametrize('kind', ['node', 'origin', 'mapping', 'method', 'target', 'training_rows', 'extra_node'])
def test_resealed_numerical_or_source_tamper_is_rejected(inputs, kind):
    issued = issue(inputs)
    if kind == 'node': issued['nodes'][0]['quantile_bps'][1] += 0.001
    if kind == 'origin': issued['origin_row']['mid']['close'] = '1.2'
    if kind == 'mapping': issued['input_mapping']['first_observed_epoch'] += 0.01
    if kind == 'method': issued['nodes'][0]['method'] = 'selected_winner'
    if kind == 'target': issued['nodes'][0]['target_price_epoch'] += 60
    if kind == 'training_rows': issued['nodes'][0]['training_rows'] += 1
    if kind == 'extra_node': issued['nodes'].append(deepcopy(issued['nodes'][0]))
    reseal(issued, 'issued_sha256')
    with pytest.raises(ValueError, match='replay_mismatch'):
        m.validate_issue(issued, **validation_inputs(inputs))


@pytest.mark.parametrize('field', ['can_place_orders', 'can_promote', 'positions_managed', 'proof_eligible'])
def test_authority_tamper_is_rejected(inputs, field):
    issued = issue(inputs)
    issued[field] = True
    reseal(issued, 'issued_sha256')
    with pytest.raises(ValueError, match='authority'):
        m.validate_issue(issued, **validation_inputs(inputs))


@pytest.mark.parametrize('kind', ['raw', 'receipt', 'consumption_hash', 'capture_future',
                                  'consume_future', 'consume_before_capture', 'cohort', 'grid',
                                  'latest_reference', 'source_binding'])
def test_original_input_identity_and_clocks(inputs, kind):
    value = deepcopy(inputs)
    if kind == 'raw': value['input_raw'] += b' '
    if kind == 'receipt': value['input_receipt']['request_duration_sec'] += 0.1
    if kind == 'consumption_hash': value['input_consumption']['receipt_sha256'] = '0' * 64
    if kind == 'capture_future':
        value['input_receipt']['capture_completed_epoch'] = REFERENCE + 4.
        capture_fixture.reseal(value['input_receipt'])
        value['input_consumption']['receipt_sha256'] = m.digest(value['input_receipt'])
    if kind == 'consume_future': value['input_consumption']['read_completed_epoch'] = REFERENCE + 4.
    if kind == 'consume_before_capture': value['input_consumption']['read_started_epoch'] = REFERENCE + 1.15
    if kind == 'cohort': value['cohort_id'] = 'some_other_study'
    if kind == 'grid': value['scheduled_reference_price_epoch'] += 60
    if kind == 'latest_reference': value['scheduled_reference_price_epoch'] -= 300
    if kind == 'source_binding': value['expected_source_bindings']['bad_path.py'] = '0' * 64
    with pytest.raises(ValueError): issue(value)


@pytest.mark.parametrize('clocks', [[REFERENCE + 21., REFERENCE + 21.],
                                   [REFERENCE + 3., REFERENCE + 21.],
                                   [REFERENCE + 3., REFERENCE + 2.]])
def test_late_or_regressed_computation_is_withheld(inputs, clocks):
    with pytest.raises(ValueError): issue(inputs, clocks)


@pytest.mark.parametrize('kind', ['artifact_hash', 'future_artifact', 'future_training',
                                   'unordered_quantiles', 'negative_unsigned',
                                   'unsupported_training', 'wrong_levels'])
def test_fixed_training_contract_and_cutoff(inputs, monkeypatch, kind):
    value = deepcopy(inputs)
    artifact = json.loads(value['fit_raw'])
    cell = artifact['fits']['EUR_USD/15/absolute_terminal_bps']
    if kind == 'future_artifact': artifact['created_epoch'] = REFERENCE + 10
    if kind == 'future_training': cell['training_target_max_epoch'] = REFERENCE + 100
    if kind == 'unordered_quantiles': cell['empirical'] = [3, 1, 2]
    if kind == 'negative_unsigned': cell['empirical'] = [-1, 1, 2]
    if kind == 'unsupported_training': cell['training_rows'] = 1
    if kind == 'wrong_levels': cell['quantiles'] = [0.2, 0.5, 0.8]
    value['fit_raw'] = m.canonical(artifact)
    if kind == 'artifact_hash': value['fit_raw'] += b' '
    else: monkeypatch.setattr(m, 'TRAINING_ARTIFACT_SHA256', m.sha(value['fit_raw']))
    with pytest.raises(ValueError): issue(value)


def test_exact_same_session_warm_support_is_required(inputs, monkeypatch):
    raw, receipt, _ = capture_fixture.make_capture(monkeypatch, gap=250)
    value = deepcopy(inputs)
    value.update(input_raw=raw, input_receipt=receipt)
    value['input_consumption'].update(raw_sha256=m.sha(raw), receipt_sha256=m.digest(receipt))
    with pytest.raises(ValueError, match='204_real_same_session'):
        issue(value)


def test_flat_price_scale_is_unavailable_not_floored(inputs, monkeypatch):
    payload = capture_fixture.payload()
    for row in payload['candles']:
        for side in ('mid', 'bid', 'ask'):
            row[side] = deepcopy(payload['candles'][0][side])
    raw, receipt, _, _ = capture_fixture.fake_capture(monkeypatch, capture_fixture.raw_of(payload))
    value = deepcopy(inputs)
    value.update(input_raw=raw, input_receipt=receipt)
    value['input_consumption'].update(raw_sha256=m.sha(raw), receipt_sha256=m.digest(receipt))
    with pytest.raises(ValueError, match='zero_or_nonfinite_past_volatility'):
        issue(value)


@pytest.mark.parametrize('kind', ['wrong_issue_bytes', 'preissue_publication', 'late_publication',
                                  'prepublication_consumer', 'late_consumer', 'wrong_consumer_identity'])
def test_durable_publication_and_independent_consumption_clocks(inputs, kind):
    issued, pub, consumed = chain(inputs)
    source = inputs['expected_source_bindings']
    with pytest.raises(ValueError):
        if kind == 'wrong_issue_bytes':
            m.publication_receipt(issued, persisted_bytes_sha256=issued['issued_sha256'],
                publication_started_epoch=REFERENCE + 5, expected_source_bindings=source,
                clock=lambda: REFERENCE + 6)
        elif kind in ('preissue_publication', 'late_publication'):
            m.publication_receipt(issued, persisted_bytes_sha256=m.digest(issued),
                publication_started_epoch=REFERENCE + (2 if kind == 'preissue_publication' else 5),
                expected_source_bindings=source,
                clock=lambda: REFERENCE + (3 if kind == 'preissue_publication' else 21))
        elif kind in ('prepublication_consumer', 'late_consumer'):
            m.consumption_receipt(issued, pub,
                read_started_epoch=REFERENCE + (4 if kind == 'prepublication_consumer' else 6),
                expected_source_bindings=source,
                clock=lambda: REFERENCE + (6 if kind == 'prepublication_consumer' else 21))
        else:
            consumed['consumed_issue_bytes_sha256'] = '0' * 64
            reseal(consumed, 'consumption_sha256')
            m.validate_chain(issued, pub, consumed, **validation_inputs(inputs))


def test_mutation_after_issue_cannot_change_sealed_output(inputs):
    issued = issue(inputs)
    before = m.canonical(issued)
    inputs['metadata']['pip_size'] = '0.1'
    inputs['input_consumption']['read_completed_epoch'] = 999
    inputs['expected_source_bindings'].clear()
    assert m.canonical(issued) == before


def test_huge_clock_fails_with_named_value_error():
    with pytest.raises(ValueError, match='invalid_clock'):
        m.epoch(10**1000)
