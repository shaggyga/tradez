"""Focused offline integration tests for the separately versioned exact scorer."""
from copy import deepcopy
from decimal import Context, Decimal, localcontext
import json
import unittest

import oanda_fixed_forecast_evaluation_exact as evaluator
from oanda_exact_price_scoring import quote_midpoint


def protocol():
    return {'schema_version': evaluator.PROTOCOL_SCHEMA, 'contract_id': 'offline_exact_fixture',
            'proof_eligible': False, 'account_eligible': False, 'collection_enabled': False,
            'instrument': 'EUR_USD', 'horizon_sec': 3600, 'input_timeframe': 'M1',
            'cohorts': {name: 'fixture:'+name for name in ('one', 'two', 'three', 'four')},
            'model_version': 'fixture-model', 'feature_version': 'fixture-features',
            'baselines': ['fair_coin', 'zero_move', 'no_trade', 'rolling_class_rate'],
            'historical_start_utc': '2026-09-01T00:00:00+00:00',
            'historical_end_utc': '2026-09-05T00:00:00+00:00',
            'quote_max_age_sec': 90, 'maximum_entry_delay_sec': 120,
            'maximum_target_quote_delay_sec': 120, 'extra_cost_stress_bps': [0, 1, 2],
            'rolling_lookback': 100, 'rolling_min_labels': 1}


def quote(identity, market, available, bid, ask):
    return {'quote_id': identity, 'instrument': 'EUR_USD', 'tradeable': True,
            'market_epoch': market, 'available_epoch': available, 'bid': bid, 'ask': ask}


def add_decision(dataset, p, *, offset=0, reference_prices=('1.35055', '1.35073'),
                 target_prices=('1.35054', '1.35074')):
    reference = evaluator.epoch(p['historical_start_utc']) + 1800 + offset
    identity = 'd'+str(offset)
    reference_quote = quote(identity+':reference', reference, reference+1, *reference_prices)
    entry = quote(identity+':entry', reference+12, reference+13, *reference_prices)
    target = quote(identity+':target', reference+3601, reference+3602, *target_prices)
    forecasts = []
    for index, family in enumerate(p['cohorts']):
        forecasts.append({'forecast_id': identity+':'+family, 'family': family,
            'cohort_id': p['cohorts'][family], 'instrument': p['instrument'],
            'horizon_sec': 3600, 'input_timeframe': 'M1', 'model_version': p['model_version'],
            'feature_version': p['feature_version'], 'issued_epoch': reference+2+index,
            'committed_available_epoch': reference+6+index, 'reference_epoch': reference,
            'target_epoch': reference+3600, 'reference_mid': quote_midpoint(reference_quote),
            'feature_cutoff_epoch': reference-60, 'features_available_epoch': reference,
            'training_label_maturity_max_epoch': reference-3600,
            'training_labels_available_max_epoch': reference-3500,
            'side': 1, 'probability_up': '.8', 'predicted_return_bps': '1'})
    decision = {'decision_id': identity, 'reference_epoch': reference, 'target_epoch': reference+3600,
                'reference_quote_id': reference_quote['quote_id'], 'forecasts': forecasts}
    dataset['decisions'].append(decision)
    dataset['quotes'].extend([reference_quote, entry, target])
    dataset['observed_cutoff_epoch'] = max(dataset['observed_cutoff_epoch'], reference+7200)
    return decision


def fixture(**kwargs):
    p = protocol()
    data = {'schema_version': evaluator.SCHEMA, 'observed_cutoff_epoch': 0, 'decisions': [], 'quotes': []}
    add_decision(data, p, **kwargs)
    return data, p


class ExactEvaluatorTests(unittest.TestCase):
    def test_changed_spread_flat_has_no_false_hits_and_keeps_four_families(self):
        data, p = fixture()
        before = deepcopy((data, p))
        report = evaluator.evaluate(data, p)
        self.assertEqual(report['schema_version'], 'fixed_forecast_evaluation_report_exact_v2')
        self.assertEqual(report['coverage']['paired_scored_decisions'], 1)
        self.assertEqual(report['outcome_counts'], {'flat': 1})
        self.assertEqual(report['decisions'][0]['actual_midpoint_move'], Decimal(0))
        for family in p['cohorts']:
            score = report['decisions'][0]['scores'][family]
            self.assertFalse(score['direction_correct'])
            self.assertEqual(score['brier'], Decimal('.64'))
            self.assertFalse(score['positive_after_spread'])
            self.assertEqual(report['paired_summaries'][family]['direction_hit_rate_when_directional'], 0)
        self.assertFalse(report['proof_eligible'])
        self.assertFalse(report['account_eligible'])
        self.assertFalse(report['runtime_started'])
        self.assertEqual((data, p), before)

    def test_real_tiny_move_survives_scoring_and_labels(self):
        tiny = '1.'+'0'*99+'1'
        data, p = fixture(reference_prices=('1', '1'), target_prices=(tiny, tiny))
        report = evaluator.evaluate(data, p)
        self.assertEqual(report['outcome_counts'], {'up': 1})
        row = report['decisions'][0]
        self.assertEqual(row['actual_midpoint_move'], Decimal('1e-100'))
        for family in p['cohorts']:
            self.assertTrue(row['scores'][family]['direction_correct'])
            self.assertTrue(row['scores'][family]['positive_after_spread'])

    def test_exact_reference_anchor_rejects_old_tolerance_mismatch(self):
        data, p = fixture(reference_prices=('1', '1'), target_prices=('1.001', '1.001'))
        data['decisions'][0]['forecasts'][0]['reference_mid'] = '1.0000000000001'
        report = evaluator.evaluate(data, p)
        self.assertEqual(report['coverage']['paired_scored_decisions'], 0)
        self.assertEqual(report['decision_exclusion_counts']['forecast_reference_price_mismatch'], 1)

    def test_crossed_quotes_cannot_be_accepted_after_float_collapse(self):
        data, p = fixture(reference_prices=('1', '1'), target_prices=('1', '1'))
        target = data['quotes'][2]
        target['bid'], target['ask'] = '1.00000000000000000001', '1'
        self.assertEqual(float(target['bid']), float(target['ask']))
        report = evaluator.evaluate(data, p)
        self.assertEqual(report['quote_exclusions'], {'invalid_bid_ask': 1})
        self.assertEqual(report['coverage']['paired_scored_decisions'], 0)
        self.assertEqual(report['decision_exclusion_counts']['missing_original_target_quote'], 1)

    def test_exact_json_price_numbers_and_native_clock_semantics(self):
        text = '{"bid":1.00000000000000000001,"ask":1.00000000000000000002,"market_epoch":1788222600.123456789,"probability_up":0.1234567890123456789,"extra_cost_stress_bps":[0.1234567890123456789]}'
        parsed = evaluator.loads_exact_prices(text)
        self.assertEqual(parsed['bid'], Decimal('1.00000000000000000001'))
        self.assertEqual(parsed['ask'], Decimal('1.00000000000000000002'))
        self.assertEqual(parsed['probability_up'], Decimal('0.1234567890123456789'))
        self.assertIsInstance(parsed['market_epoch'], float)
        self.assertEqual(parsed['market_epoch'], json.loads(text)['market_epoch'])
        self.assertIsInstance(parsed['extra_cost_stress_bps'][0], Decimal)

    def test_rolling_labels_are_rebuilt_from_exact_flat_then_true_up(self):
        data, p = fixture()  # Previously false positive float move, now flat.
        add_decision(data, p, offset=7200, reference_prices=('1', '1'), target_prices=('1.01', '1.01'))
        add_decision(data, p, offset=14400, reference_prices=('1', '1'), target_prices=('1.02', '1.02'))
        report = evaluator.evaluate(data, p)
        self.assertEqual(report['coverage']['paired_scored_decisions'], 3)
        one, two, three = [r['rolling_baseline_training'] for r in report['decisions']]
        self.assertTrue(one['warmup_fallback'])
        self.assertEqual(two['n_training_labels'], 1)
        with localcontext(Context(prec=evaluator.METRIC_PRECISION)):
            self.assertEqual(two['probability_up'], Decimal(1)/3)
        self.assertEqual(two['side'], -1)
        self.assertEqual(three['n_training_labels'], 2)
        self.assertEqual(three['probability_up'], Decimal('.5'))
        self.assertEqual(three['side'], 0)

    def test_future_labels_cannot_train_earlier_decisions(self):
        data, p = fixture()
        add_decision(data, p, offset=60)
        report = evaluator.evaluate(data, p)
        for row in report['decisions']:
            self.assertEqual(row['rolling_baseline_training']['n_training_labels'], 0)
            self.assertTrue(row['rolling_baseline_training']['warmup_fallback'])

    def test_original_forecast_clock_gates_remain_fail_closed(self):
        edits = [('issued_epoch', lambda f: f['reference_epoch']-1),
                 ('committed_available_epoch', lambda f: f['target_epoch']),
                 ('features_available_epoch', lambda f: f['issued_epoch']+1),
                 ('feature_cutoff_epoch', lambda f: f['features_available_epoch']+1),
                 ('training_labels_available_max_epoch', lambda f: f['issued_epoch']),
                 ('training_label_maturity_max_epoch', lambda f: f['training_labels_available_max_epoch']+1),
                 ('target_epoch', lambda f: f['target_epoch']+1)]
        for field, value in edits:
            data, p = fixture()
            f = data['decisions'][0]['forecasts'][0]
            f[field] = value(f)
            with self.subTest(field=field):
                report = evaluator.evaluate(data, p)
                self.assertEqual(report['coverage']['paired_scored_decisions'], 0)
                self.assertEqual(report['decision_exclusion_counts']['forecast_clock_identity_or_value_invalid'], 1)

    def test_entry_must_follow_publication_and_target_receipt_is_bounded(self):
        for mode in ('entry_before_publication', 'target_received_too_late', 'not_tradeable', 'stale'):
            data, p = fixture()
            reference = data['decisions'][0]['reference_epoch']
            with self.subTest(mode=mode):
                if mode == 'entry_before_publication':
                    data['quotes'][1]['market_epoch'] = reference+8
                    data['quotes'][1]['available_epoch'] = reference+9
                elif mode == 'target_received_too_late':
                    data['quotes'][2]['market_epoch'] = reference+3600+120
                    data['quotes'][2]['available_epoch'] = reference+3600+121
                elif mode == 'not_tradeable':
                    data['quotes'][2]['tradeable'] = False
                else:
                    data['quotes'][2]['available_epoch'] = data['quotes'][2]['market_epoch']+91
                report = evaluator.evaluate(data, p)
                self.assertEqual(report['coverage']['paired_scored_decisions'], 0)

    def test_conflicting_quote_identity_and_duplicate_forecasts_fail_closed(self):
        data, p = fixture()
        conflict = deepcopy(data['quotes'][2])
        conflict['ask'] = '1.350740000000000000001'
        data['quotes'].append(conflict)
        with self.assertRaisesRegex(ValueError, 'conflicting_quote_identity'):
            evaluator.evaluate(data, p)
        data, p = fixture()
        data['decisions'][0]['forecasts'][1]['forecast_id'] = data['decisions'][0]['forecasts'][0]['forecast_id']
        with self.assertRaisesRegex(ValueError, 'duplicate_forecast_id'):
            evaluator.evaluate(data, p)

    def test_decimal_summaries_and_json_transport_are_explicit(self):
        data, p = fixture()
        report = evaluator.evaluate(data, p)
        encoded = evaluator.jsonable(report)
        self.assertEqual(encoded['paired_summaries']['one']['mean_brier'], '0.64')
        self.assertEqual(encoded['paired_deltas']['one']['mean_brier_minus_fair_coin'], '0.39')
        self.assertIsInstance(encoded['decisions'][0]['reference_epoch'], float)
        self.assertIsInstance(encoded['decisions'][0]['reference_quote']['bid'], str)
        self.assertEqual(json.loads(json.dumps(encoded, allow_nan=False)), encoded)
        self.assertIsNone(report['independent_sample_size'])

    def test_valid_extreme_source_range_does_not_invalidate_derived_metrics(self):
        data, p = fixture(reference_prices=('1e-512', '1e-512'), target_prices=('1e512', '1e512'))
        report = evaluator.evaluate(data, p)
        self.assertEqual(report['coverage']['paired_scored_decisions'], 1)
        self.assertEqual(report['outcome_counts'], {'up': 1})
        self.assertTrue(report['paired_summaries']['one']['rmse_bps'].is_finite())
        self.assertGreater(report['paired_summaries']['one']['rmse_bps'], Decimal('1e1000'))
        self.assertEqual(evaluator.decimal_mean([Decimal('1e-700')]), Decimal('1e-700'))

    def test_probability_just_outside_range_cannot_float_collapse_to_one(self):
        data, p = fixture()
        data['decisions'][0]['forecasts'][0]['probability_up'] = '1.000000000000000000001'
        report = evaluator.evaluate(data, p)
        self.assertEqual(report['coverage']['paired_scored_decisions'], 0)
        self.assertIn('probability_out_of_range', report['exclusions'][0]['forecast_errors']['one'])

    def test_low_precision_caller_context_does_not_change_labels_or_metrics(self):
        data, p = fixture()
        ordinary = evaluator.evaluate(data, p)
        with localcontext() as context:
            context.prec = 2
            constrained = evaluator.evaluate(data, p)
        ordinary.pop('generated_utc')
        constrained.pop('generated_utc')
        self.assertEqual(ordinary, constrained)


if __name__ == '__main__':
    unittest.main()
