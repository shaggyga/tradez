import datetime as dt
import unittest

from oanda_news_interpretation_v1 import interpret_headline
import oanda_local_news_sentiment as collector


class InterpretationTests(unittest.TestCase):
    def claims(self, text, kind):
        return [c for c in interpret_headline(text)["claims"] if c["kind"] == kind]

    def test_policy_action_variants_and_owner(self):
        for text, owner, direction in [
            ("ECB Raises Rates to 2.5% as Energy Inflation Surges", "EUR", 1),
            ("Fed raises interest rates for first time since 2023", "USD", 1),
            ("Bank of England has cut its policy rate", "GBP", -1),
            ("Bank of Japan hikes rates", "JPY", 1),
        ]:
            with self.subTest(text=text):
                c = self.claims(text, "policy_rate_action")[0]
                self.assertEqual((c['currency'], c['direction']), (owner, direction))

    def test_hawkish_fed_is_not_hawkish_euro(self):
        text = "EUR/USD outlook: Hawkish Fed puts euro on the ropes"
        stance = self.claims(text, "policy_stance")
        self.assertEqual([(c['currency'], c['direction']) for c in stance], [('USD', 1)])
        reaction = self.claims(text, "reported_currency_response")[0]
        self.assertEqual((reaction['currency'], reaction['direction'], reaction['status']), ('EUR', -1, 'retrospective'))

    def test_negated_and_hypothetical_actions_abstain(self):
        for text in ["Fed may raise rates", "Fed will not raise rates", "If ECB raises rates, euro may rise", "Fed denies it raised rates", "Fed expects to raise rates"]:
            with self.subTest(text=text):
                self.assertTrue(all(c['direction'] is None for c in self.claims(text, 'policy_rate_action')))

    def test_two_bank_clauses_have_separate_directions(self):
        claims = self.claims('Fed raises rates while ECB cuts rates', 'policy_rate_action')
        self.assertEqual([(c['currency'], c['direction']) for c in claims], [('USD', 1), ('EUR', -1)])
        self.assertFalse(self.claims('Fed says ECB raises rates', 'policy_rate_action'))

    def test_stance_negation_and_relative_stance_abstain(self):
        for text in ['Fed is not hawkish', 'Less hawkish Fed supports euro', 'Fed may turn hawkish']:
            self.assertTrue(all(c['direction'] is None for c in self.claims(text, 'policy_stance')))

    def test_inflation_level_is_not_surprise(self):
        result = interpret_headline('US inflation held steady at 3.4% as high fuel prices persist')
        self.assertEqual(result['surprise'], 'unknown')
        self.assertTrue(all(c['direction'] is None for c in result['claims']))
        self.assertEqual({c['kind'] for c in result['claims']}, {'inflation_context', 'energy_context'})

    def test_oil_relief_keeps_explicit_response_and_ambiguous_mechanism(self):
        text = 'Weaker oil gives politically-hit euro mild relief'
        self.assertEqual(self.claims(text, 'reported_currency_response')[0]['direction'], 1)
        self.assertIsNone(self.claims(text, 'energy_context')[0]['direction'])

    def test_rebound_is_retrospective_not_forecast(self):
        result = interpret_headline('EUR/USD (1.1492) Rebounds From 1.1460 Low After Fed Hike')
        self.assertEqual(result['claims'][0]['status'], 'retrospective')
        self.assertFalse(result['forecast_eligible'])
        future = self.claims('EUR/USD may rebound tomorrow', 'reported_pair_move')[0]
        self.assertIsNone(future['direction'])

    def test_opinion_is_not_fact(self):
        c = self.claims('UBS sees EUR/USD long opportunity on central bank dynamics', 'analyst_pair_view')[0]
        self.assertEqual((c['direction'], c['status']), (1, 'opinion'))

    def test_forecast_section_with_explicit_past_rebound(self):
        text = 'EUR/USD Price Forecast - EUR/USD (1.1492) Rebounds From 1.1460 Low After Fed Hike - 1.1535 Target in Focus'
        c = self.claims(text, 'reported_pair_move')[0]
        self.assertEqual((c['direction'], c['status']), (1, 'retrospective'))
        c = self.claims('EUR/USD may rebound from 1.1460 low', 'reported_pair_move')[0]
        self.assertIsNone(c['direction'])

    def test_unknown_stays_unknown(self):
        self.assertEqual(interpret_headline('A quiet morning')['interpretation_status'], 'unresolved')

    def test_relative_stance_is_a_change_not_an_absolute_position(self):
        for headline, sign in [('NBP Governor turns less dovish, remains far from rate-hike pricing', 1),
                               ('Less hawkish Fed supports euro', -1),
                               ('ECB turns more dovish', -1), ('RBA remains more hawkish', 1)]:
            with self.subTest(headline=headline):
                c = self.claims(headline, 'policy_stance_change')[0]
                self.assertEqual(c['direction'], sign)
                self.assertFalse(self.claims(headline, 'policy_stance'))

    def test_falling_despite_hawkish_policy_preserves_both(self):
        text = 'Why is the Australian Dollar falling despite RBA hawkish stance?'
        self.assertEqual(self.claims(text, 'policy_stance')[0]['direction'], 1)
        self.assertEqual(self.claims(text, 'reported_currency_response')[0]['direction'], -1)
        self.assertEqual(self.claims(text, 'competing_drivers')[0]['mechanism'], 'countervailing')

    def test_oil_and_jobs_compete_without_synthetic_currency_legs(self):
        text = 'The Canadian Dollar gains as Oil rally offsets jobs gloom'
        claims = interpret_headline(text)['claims']
        self.assertEqual({c['currency'] for c in claims if c['currency']}, {'CAD'})
        self.assertEqual(self.claims(text, 'reported_currency_response')[0]['direction'], 1)
        self.assertTrue(self.claims(text, 'labour_context'))
        self.assertTrue(self.claims(text, 'competing_drivers'))

    def test_bets_are_expectations_not_bank_action(self):
        text = 'Sterling slips versus dollar as firm US PPI and $100 oil stoke Fed hawkish bets'
        self.assertEqual(self.claims(text, 'reported_currency_response')[0]['currency'], 'GBP')
        c = self.claims(text, 'policy_stance')[0]
        self.assertEqual((c['currency'], c['status']), ('USD', 'market_expectation'))

    def test_hypothetical_currency_move_and_negated_shift_abstain(self):
        self.assertIsNone(self.claims('Canadian dollar may fall despite oil strength', 'reported_currency_response')[0]['direction'])
        self.assertIsNone(self.claims('Fed is not less dovish', 'policy_stance_change')[0]['direction'])

    def test_actual_classifier_integration_retains_guards(self):
        now = dt.datetime(2026, 9, 30, tzinfo=dt.timezone.utc)
        raw = {'title': 'ECB Raises Rates to 2.5% as Energy Inflation Surges',
               'summary': '', 'source_id': 'test', 'source_name': 'test',
               'url': 'https://example.com/a', 'published_utc': now.isoformat()}
        row = collector.classify_article(raw, first_seen=now)
        self.assertIn('headline_interpretation', row)
        self.assertFalse(row['headline_interpretation']['forecast_eligible'])
        self.assertFalse(row['execution_eligible'])
        self.assertFalse(row['can_place_orders'])


if __name__ == '__main__':
    unittest.main()
