"""Offline price-scoring regression fixtures; no runtime or database access."""
from copy import deepcopy
from decimal import Decimal, ROUND_DOWN, getcontext, localcontext
import json
import unittest

import oanda_exact_price_scoring as scoring


# Exact retained price strings copied from broad_signal_final_verification.json
# SHA256 f99dd7ded33fbe708c2db314b8d5414406af635ac84c947da06b4c8b5bf60c60.
# IDs are traceability evidence, not inputs to the scoring algorithm.
FALSE_FLOAT_FLATS = (
    (63144, 'USD_JPY', -1, '160.206', '160.22', '160.205', '160.221', False),
    (63360, 'GBP_NZD', 1, '2.29258', '2.29387', '2.29254', '2.29391', False),
    (63702, 'GBP_USD', 1, '1.35053', '1.35071', '1.35052', '1.35072', False),
    (63704, 'GBP_USD', 1, '1.35055', '1.35073', '1.35054', '1.35074', True),
    (64174, 'AUD_USD', -1, '0.71383', '0.71395', '0.71382', '0.71396', False),
    (64218, 'AUD_USD', -1, '0.71412', '0.71426', '0.71413', '0.71425', False),
    (64578, 'USD_CAD', -1, '1.39343', '1.3936', '1.39342', '1.39361', False),
    (66345, 'EUR_DKK', 1, '7.47456', '7.47583', '7.47457', '7.47582', False),
    (66783, 'EUR_USD', 1, '1.15888', '1.15903', '1.15887', '1.15904', True),
    (67912, 'USD_CAD', 1, '1.38025', '1.38044', '1.38026', '1.38043', True),
    (68562, 'SGD_CHF', -1, '0.63617', '0.63631', '0.63616', '0.63632', True),
    (68770, 'GBP_USD', 1, '1.35377', '1.35392', '1.35376', '1.35393', True),
    (68787, 'EUR_GBP', 1, '0.85922', '0.85935', '0.85923', '0.85934', True),
    (69532, 'EUR_GBP', 1, '0.85923', '0.85936', '0.85922', '0.85937', False),
    (69565, 'EUR_DKK', 1, '7.47367', '7.47494', '7.47366', '7.47495', False),
    (69841, 'GBP_CHF', -1, '1.09312', '1.09355', '1.0931', '1.09357', True),
    (70053, 'USD_HKD', 1, '7.84023', '7.84057', '7.84025', '7.84055', True),
    (70124, 'EUR_GBP', 1, '0.85862', '0.85875', '0.85861', '0.85876', False),
)


def quote(bid, ask=None):
    return {'bid': bid, 'ask': bid if ask is None else ask}


class ExactPriceScoringTests(unittest.TestCase):
    def score(self, reference=None, target=None, **kwargs):
        return scoring.score_prediction(reference or quote('1'), target or quote('1.01'),
                                        **({'direction': 1, 'probability_up': '.8'} | kwargs))

    def test_all_eighteen_retained_flat_rows_and_eight_spurious_hits(self):
        legacy_hits = []
        for identity, instrument, side, eb, ea, tb, ta, spurious in FALSE_FLOAT_FLATS:
            with self.subTest(id=identity, instrument=instrument):
                move_float = (float(tb)+float(ta))/2-(float(eb)+float(ea))/2
                self.assertNotEqual(move_float, 0)
                self.assertEqual(side*move_float > 0, spurious)
                if spurious:
                    legacy_hits.append(identity)
                for convert in (str, float, Decimal):
                    result = self.score(quote(convert(eb), convert(ea)), quote(convert(tb), convert(ta)), direction=side)
                    self.assertEqual(result['outcome_class'], 'flat')
                    self.assertEqual(result['actual_midpoint_move'], Decimal(0))
                    self.assertEqual(result['actual_return_bps'], Decimal(0))
                    self.assertFalse(result['direction_correct'])
                    self.assertFalse(result['positive_after_spread'])
        self.assertEqual(legacy_hits, [63704, 66783, 67912, 68562, 68770, 68787, 69841, 70053])

    def test_changed_spread_with_exact_same_midpoint(self):
        for side in (-1, 1):
            with self.subTest(side=side):
                result = self.score(quote('1.10', '1.12'), quote('1.09', '1.13'), direction=side)
                self.assertEqual(result['outcome_class'], 'flat')
                self.assertEqual(result['net_price_move'], Decimal('-.03'))
                self.assertEqual(result['roundtrip_spread_price'], Decimal('.03'))
                self.assertEqual(result['up_label'], 0)
                self.assertEqual(result['brier_up_vs_not_up'], Decimal('.64'))

    def test_real_move_below_float_resolution_is_not_flat(self):
        tiny = '1.' + '0'*99 + '1'
        up = self.score(quote('1'), quote(tiny))
        down = self.score(quote(tiny), quote('1'), direction=-1)
        self.assertEqual(float(tiny), 1.0)
        for result, label in ((up, 'up'), (down, 'down')):
            with self.subTest(label=label):
                self.assertEqual(result['outcome_class'], label)
                self.assertTrue(result['direction_correct'])
                self.assertTrue(result['positive_after_spread'])
                self.assertEqual(abs(result['actual_midpoint_move']), Decimal('1e-100'))

    def test_tiny_net_move_is_classified_from_exact_price_difference(self):
        target_ask = '1.'+'0'*99+'1'
        result = self.score(quote('1', target_ask), quote(target_ask), probability_up=1)
        self.assertTrue(result['direction_correct'])
        self.assertEqual(result['net_price_move'], 0)
        self.assertFalse(result['positive_after_spread'])

    def test_buy_and_sell_executable_prices_and_reference_vs_entry(self):
        reference = quote('99', '101')
        entry = quote('102', '104')
        target = quote('105', '107')
        buy = self.score(reference, target, entry_quote=entry)
        sell = self.score(reference, target, entry_quote=entry, direction=-1)
        self.assertEqual(buy['actual_return_bps'], 600)
        self.assertEqual(buy['net_price_move'], 1)
        self.assertEqual(sell['net_price_move'], -5)
        self.assertTrue(buy['direction_correct'])
        self.assertFalse(sell['direction_correct'])
        with localcontext() as context:
            context.prec = scoring.METRIC_PRECISION
            self.assertEqual(buy['net_bps'], Decimal(10000)/103)

    def test_correct_direction_can_lose_after_spread(self):
        result = self.score(quote('1', '1.02'), quote('1.005', '1.025'))
        self.assertTrue(result['direction_correct'])
        self.assertEqual(result['net_price_move'], Decimal('-.015'))
        self.assertFalse(result['positive_after_spread'])

    def test_exotic_pips_are_explicit_and_do_not_change_price_class(self):
        cases = [('USD_THB', '.01', '33.285', '33.307', '33.290', '33.309'),
                 ('HKD_JPY', '.0001', '19.83631', '19.83871', '19.86901', '19.87128'),
                 ('USD_HUF', '.01', '317.564', '317.800', '317.630', '317.844'),
                 ('EUR_HUF', '.01', '368.005', '368.955', '368.017', '369.105')]
        for instrument, pip, eb, ea, tb, ta in cases:
            with self.subTest(instrument=instrument):
                result = self.score(quote(eb, ea), quote(tb, ta), pip_size=pip, expected_signed_pips='2')
                expected_net = (Decimal(tb)-Decimal(ea))/Decimal(pip)
                self.assertEqual(result['net_pips'], expected_net)
                self.assertEqual(result['pip_size'], Decimal(pip))
                self.assertEqual(result['outcome_class'], 'up')
                self.assertEqual(result['direction_correct'], True)
        self.assertEqual(self.score(quote('19.83631', '19.83871'), quote('19.86901', '19.87128'),
                                   pip_size='.0001')['net_pips'], Decimal('303'))

    def test_prediction_error_is_signed_and_uses_reference_price(self):
        result = self.score(quote('100'), quote('99'), direction=-1, probability_up='.2',
                            pip_size='.01', expected_signed_pips='-100')
        self.assertEqual(result['actual_return_bps'], -100)
        self.assertEqual(result['predicted_return_bps'], -100)
        self.assertEqual(result['absolute_error_bps'], 0)
        self.assertEqual(result['squared_error_bps'], 0)
        self.assertEqual(result['brier_up_vs_not_up'], Decimal('.04'))
        self.assertEqual(result['actual_signed_pips'], -100)
        self.assertEqual(result['gross_directional_pips'], 100)

    def test_direct_prediction_bps_and_exact_midpoint_adapter(self):
        result = self.score(quote('100'), quote('99'), direction=-1, predicted_return_bps='-99')
        self.assertEqual(result['absolute_error_bps'], 1)
        self.assertEqual(result['squared_error_bps'], 1)
        self.assertEqual(scoring.quote_midpoint(quote('1.35055', '1.35073')), Decimal('1.35064'))
        with self.assertRaises(ValueError):
            self.score(expected_signed_pips='1', pip_size='.01', predicted_return_bps='1')

    def test_emitted_direction_is_not_replaced_by_probability(self):
        result = self.score(direction=-1, probability_up='.9')
        self.assertTrue(result['emitted_side_differs_from_probability_direction'])
        self.assertFalse(result['direction_correct'])
        self.assertEqual(result['direction'], -1)
        self.assertEqual(result['probability_direction'], 1)

    def test_abstention_and_exact_spread_stress(self):
        result = self.score(direction=0, probability_up='.5', extra_cost_stress_bps=['0', '.1', '1'])
        self.assertEqual(result['net_price_move'], 0)
        self.assertFalse(result['direction_correct'])
        self.assertFalse(result['positive_after_spread'])
        self.assertEqual(result['stress_net_bps'], {'0': Decimal(0), '0.1': Decimal(0), '1': Decimal(0)})
        active = self.score(extra_cost_stress_bps=['.1'])
        self.assertEqual(active['net_bps'], 100)
        self.assertEqual(active['stress_net_bps']['0.1'], Decimal('99.9'))

    def test_inputs_and_caller_decimal_context_are_unchanged(self):
        reference, target = quote('1.35055', '1.35073'), quote('1.35054', '1.35074')
        original = deepcopy((reference, target))
        normal = self.score(reference, target)
        before = getcontext().copy()
        with localcontext() as context:
            context.prec, context.rounding = 2, ROUND_DOWN
            low_precision = self.score(reference, target)
            self.assertEqual(context.prec, 2)
            self.assertEqual(context.rounding, ROUND_DOWN)
        self.assertEqual(normal, low_precision)
        self.assertEqual((reference, target), original)
        self.assertEqual(getcontext().prec, before.prec)
        self.assertEqual(getcontext().rounding, before.rounding)

    def test_json_transport_keeps_decimal_text_and_explicit_schema(self):
        result = self.score(probability_up=Decimal('.12345678901234567890123456789'))
        output = json.loads(json.dumps(scoring.to_jsonable(result), allow_nan=False))
        self.assertEqual(output['probability_up'], '.12345678901234567890123456789'.replace('.', '0.', 1))
        self.assertEqual(output['scoring_version'], scoring.SCORING_VERSION)
        self.assertIsInstance(output['direction_correct'], bool)
        self.assertEqual(output['net_bps'], '100.00')
        for value in (float('nan'), 1.2, Decimal('Infinity'), {1: 'x'}):
            with self.subTest(value=str(value)), self.assertRaises(ValueError):
                scoring.to_jsonable(value)

    def test_invalid_or_missing_quotes_fail_closed(self):
        bad_quotes = [{}, {'bid': '1'}, {'ask': '1'}, None, [],
                      quote('0'), quote('-1'), quote('1.2', '1.1'), quote(True),
                      quote('NaN'), quote('Infinity'), quote(float('nan')), quote(float('inf')),
                      quote('1_000'), quote(' 1'), quote('1e513'), quote('1e-513')]
        for value in bad_quotes:
            for position in ('reference', 'target', 'entry'):
                if position == 'entry' and value is None:
                    continue  # None is the documented reference-quote default.
                with self.subTest(quote=str(value), position=position), self.assertRaises(ValueError):
                    args = [quote('1'), quote('1.01')]
                    kwargs = {'direction': 1, 'probability_up': '.5'}
                    if position == 'reference':
                        args[0] = value
                    elif position == 'target':
                        args[1] = value
                    else:
                        kwargs['entry_quote'] = value
                    scoring.score_prediction(*args, **kwargs)

    def test_invalid_parameters_fail_closed(self):
        cases = [{'direction': x} for x in (True, False, 1.0, 'buy', 2, None)]
        cases += [{'probability_up': x} for x in (True, '-.01', '1.01', 'NaN', None)]
        cases += [{'pip_size': x} for x in ('0', '-.1', 'NaN', True)]
        cases += [{'expected_signed_pips': '1'}, {'expected_signed_pips': 'NaN', 'pip_size': '.01'},
                  {'extra_cost_stress_bps': ['-.1']}, {'extra_cost_stress_bps': ['.1', '.10']},
                  {'extra_cost_stress_bps': '1'}, {'extra_cost_stress_bps': [str(i) for i in range(33)]}]
        for kwargs in cases:
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                self.score(**kwargs)


if __name__ == '__main__':
    unittest.main()
