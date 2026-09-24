import copy
import pytest

from contracts import fingerprint
from causal_convex_blend_v2 import fit_snapshot, apply_snapshot, score, sensitivity


def fixture(n=12, pairs=20, value=3.0):
    rows, outcomes = [], {}
    for i in range(n):
        epoch = 86460 + 21600 * i
        for j in range(pairs):
            pair = f'PAIR_{j}'
            rid = pair + ':' + str(epoch)
            target = 'technical_endpoint_midpoint_elapsed_15m'
            rows.append({'record_id': rid, 'instrument': pair, 'target_id': target, 'decision_epoch': epoch,
                         'available_epoch': epoch+2, 'group': 'legacy26', 'procedure': 'frozen', 'selected_fit_id': 'fit',
                         'selected_fit_cutoff': 0, 'base_model_ids': {m: fingerprint({'fit_id': 'fit', 'method': m}) for m in ('ridge','recovered_hgb')},
                         'base_model_ready_epochs': {'ridge': 0, 'recovered_hgb': 0},
                         'ridge_prediction_bps': 4.0, 'recovered_hgb_prediction_bps': 0.0,
                         'blend_prediction_bps': 2.0, 'zero_prediction_bps': 0.0})
            outcomes[rid, target] = {'record_id': rid, 'target_id': target, 'label_end_epoch': epoch+900,
                                     'available_epoch': epoch+900, 'value': value}
    contract = {'minimum_distinct_origins': 8, 'minimum_distinct_utc_days': 3, 'minimum_distinct_pairs': 20}
    return rows, outcomes, contract


def fit(rows, outcomes, c, cutoff=10**9):
    return fit_snapshot(rows, outcomes, cutoff, ['legacy26', 'technical_endpoint_midpoint_elapsed_15m', 'frozen'], 15, c)


def test_analytical_weight_matches_known_least_squares_optimum():
    rows, outcomes, c = fixture()
    snapshot = fit(rows, outcomes, c)
    assert snapshot['status'] == 'fitted'
    assert snapshot['parameters']['ridge_weight'] == pytest.approx(.75)
    assert snapshot['support']['rows'] == 240


@pytest.mark.parametrize('value,expected', [(-2, 0), (6, 1)])
def test_constraint_clips_to_declared_boundary(value, expected):
    rows, outcomes, c = fixture(value=value)
    snapshot = fit(rows, outcomes, c)
    assert snapshot['parameters']['ridge_weight'] == expected
    assert snapshot['parameters']['weight_at_boundary']


@pytest.mark.parametrize('n,pairs', [(7,20), (8,20), (12,19)])
def test_support_thresholds_do_not_fallback(n, pairs):
    rows, outcomes, c = fixture(n=n, pairs=pairs)
    snapshot = fit(rows, outcomes, c)
    assert snapshot['status'] == 'insufficient_distinct_support'
    assert snapshot['parameters'] is None


def test_equal_predictions_are_explicitly_unidentifiable():
    rows, outcomes, c = fixture()
    for row in rows: row['recovered_hgb_prediction_bps'] = row['ridge_prediction_bps']
    snapshot = fit(rows, outcomes, c)
    assert snapshot['status'] == 'unidentifiable_equal_base_predictions'
    assert snapshot['parameters'] is None


def test_future_and_current_origin_values_cannot_change_earlier_snapshot():
    rows, outcomes, c = fixture()
    cutoff = 86460 + 10 * 21600
    expected = fit(rows, outcomes, c, cutoff)
    assert expected['status'] == 'fitted'
    changed_rows, changed_outcomes = copy.deepcopy(rows), copy.deepcopy(outcomes)
    for row in changed_rows:
        if row['decision_epoch'] >= cutoff:
            row['ridge_prediction_bps'] = 100000.0
            changed_outcomes[row['record_id'], row['target_id']]['value'] = -1e9
    assert expected == fit(changed_rows, changed_outcomes, c, cutoff)


@pytest.mark.parametrize('mutation', ['in_sample', 'not_ready', 'wrong_model', 'wrong_target_end', 'early_label'])
def test_training_identity_and_clock_refusals(mutation):
    rows, outcomes, c = fixture()
    row = rows[0]
    outcome = outcomes[row['record_id'], row['target_id']]
    if mutation == 'in_sample': row['selected_fit_cutoff'] = row['decision_epoch']
    if mutation == 'not_ready': row['base_model_ready_epochs']['ridge'] = row['decision_epoch']+1
    if mutation == 'wrong_model': row['base_model_ids']['ridge'] = 'wrong'
    if mutation == 'wrong_target_end': outcome['label_end_epoch'] += 60
    if mutation == 'early_label': outcome['available_epoch'] = row['decision_epoch']
    with pytest.raises(ValueError): fit(rows, outcomes, c)


def test_apply_never_uses_current_training_example_and_keeps_outcomes_out():
    rows, outcomes, c = fixture()
    cutoff = 86460 + 10 * 21600
    snapshot = fit(rows, outcomes, c, cutoff)
    current = rows[10 * 20]
    result = apply_snapshot(current, snapshot, 'expanding_prefix')
    assert result['learned_prediction_bps'] == pytest.approx(3)
    assert result['outcomes_revealed'] is False
    assert result['production_available_epoch'] is None
    assert 'outcome_bps' not in result
    forged = copy.deepcopy(snapshot)
    forged['training_membership'].append({'record_id': current['record_id']})
    forged['layer_id'] = fingerprint({k:v for k,v in forged.items() if k != 'layer_id'})
    with pytest.raises(ValueError, match='current_example'):
        apply_snapshot(current, forged, 'expanding_prefix')


def test_future_snapshot_and_parameter_tampering_refused():
    rows, outcomes, c = fixture()
    snapshot = fit(rows, outcomes, c)
    with pytest.raises(ValueError, match='future_snapshot'):
        apply_snapshot(rows[0], snapshot, 'frozen_prefix')
    snapshot['parameters']['ridge_weight'] = .3
    with pytest.raises(ValueError, match='identity'):
        apply_snapshot(rows[-1], snapshot, 'frozen_prefix')


def test_assessment_uses_identical_rows_for_every_comparator():
    rows, outcomes, c = fixture()
    cutoff = 86460 + 10 * 21600
    snapshot = fit(rows, outcomes, c, cutoff)
    forecasts = [apply_snapshot(r, snapshot, 'expanding_prefix') for r in rows[200:]]
    result = score(forecasts, outcomes, 15, 10**9)
    assert result['mature_rows'] == 40
    assert result['overall']['methods']['causal_convex_blend']['mae_bps'] == pytest.approx(0, abs=1e-14)
    assert result['overall']['paired_deltas']['fixed_equal_half_blend']['mae_bps'] == pytest.approx(-1, abs=1e-14)
    assert score(forecasts, outcomes, 15, cutoff)['mature_rows'] == 0
    assert all(r['interval'] is None for r in sensitivity(forecasts, outcomes, 15, 10**9))
