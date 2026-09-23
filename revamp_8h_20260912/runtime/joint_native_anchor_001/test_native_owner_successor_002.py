"""Unchanged owner fixtures plus narrowly expanded successor boundaries."""
from copy import deepcopy
import math
from unittest.mock import patch
import test_native_ledger_owner_v1 as base
import oanda_causal_forecast_ledger_joint_news_v5 as ledger
import oanda_fixed_forecast_evaluation_joint_news_v3 as ev
import native_m1_ledger_v1 as store
import native_m1_outcome_v1 as outcome

class SuccessorOwner(base.NativeOwner):
    def test_policy_numeric_aliases_rejected_in_protocol(self):
        for key in ('maximum_source_observations','maximum_source_blobs','maximum_target_records'):
            with self.subTest(key=key):
                p=deepcopy(self.contract['evaluation_protocol']);p['native_outcome_policy'][key]=float(p['native_outcome_policy'][key])
                with self.assertRaises(ValueError):ev.validate_protocol(p)
    def test_contract_horizon_requires_integer_identity(self):
        c=deepcopy(self.contract);c['horizon_sec']=3600.0
        with self.assertRaisesRegex(ValueError,'fixed_pair_h1'):ledger.validate_contract(c)
    def test_export_read_completion_is_later_than_owner_knowledge(self):
        self.issue();self.owner.consume_publications();self.mature()
        self.owner.observe_native_source(self.csv,clock_path=self.clockpath);self.owner.settle()
        original=store.verified_admission
        def delayed(*args,**kw):
            result=original(*args,**kw);self.clock.advance(2);return result
        with patch.object(store,'verified_admission',delayed):data,p=self.owner.export_evaluation()
        self.assertLess(data['export_read_started_epoch'],data['export_read_completed_epoch'])
        self.assertEqual(data['current_export_consumable_no_earlier_than_epoch'],data['export_read_completed_epoch'])
        self.assertLessEqual(data['native_scores'][0]['visibility']['outcome_available_epoch'],data['export_read_started_epoch'])
        self.assertEqual(ev.evaluate(data,p)['native_scores']['n'],1)
    def test_per_forecast_refusal_does_not_block_next_forecast(self):
        # Actual owner loop and actual score function; publication/source seams
        # are tiny prepared rows, not additional model fits or live forecasts.
        self.issue();self.owner.consume_publications();publications=self.owner._native_publications()
        bad=deepcopy(publications[0]);bad['forecast']['decision_id']='bad_owned_forecast'
        good=deepcopy(publications[0]);good['forecast']['decision_id']='good_owned_forecast'
        called=[]
        def selected(owner,forecasts,**kwargs):
            identity=forecasts[0]['decision_id'];called.append(identity)
            value=1e308 if identity.startswith('bad') else 1.1002
            outcome.score_native(forecasts[0]['forecasts'][0]['native_anchor'],value)
            return 1
        with patch.object(self.owner,'_native_publications',lambda:[bad,good]),patch.object(store,'process_sources'),patch.object(store,'settle',selected):
            self.assertEqual(self.owner.settle(),1)
        self.assertEqual(called,['bad_owned_forecast','good_owned_forecast'])
        self.assertEqual(self.owner.last_native_settlement_report['failed_forecast_count'],1)
        self.assertEqual(self.owner.last_native_settlement_report['status'],'partial_refusal')
    def test_matched_comparators_use_same_target_without_probabilities(self):
        self.issue();self.owner.consume_publications();self.mature()
        self.owner.observe_native_source(self.csv,clock_path=self.clockpath);self.owner.settle()
        data,p=self.owner.export_evaluation();result=ev.evaluate(data,p)
        actual=float.fromhex(data['native_scores'][0]['outcome']['score']['actual_signed_pips_hex'])
        for name,expected in (('matched_price_only',1.3),('neutral_news_ablation',1.2)):
            row=result['preissue_native_comparisons'][name]
            self.assertEqual(row['mae_pips'],abs(expected-actual));self.assertEqual(row['n'],1)
            self.assertEqual(row['direction_accuracy'],1.);self.assertIsNone(row['probability_metrics'])
    def test_comparator_boolean_is_not_neutral_scalar(self):
        self.issue();self.owner.consume_publications();data,p=self.owner.export_evaluation()
        d=data['publications'][0]['forecast'];a={**d['forecasts'][0],'committed_available_epoch':data['publications'][0]['consumed_epoch']}
        a['diagnostics']['matched_price_only_expected_pips']=False
        self.assertTrue(ev.forecast_errors(a,d,p))
    def test_registered_candle_path_blocks_alternative_source_before_read(self):
        path=self.root/'different'/'EUR_USD_M1.csv'
        with patch.object(outcome,'capture_source',side_effect=AssertionError('must_not_read')):
            with self.assertRaisesRegex(ValueError,'registered_candle'):self.owner.observe_native_source(path,clock_path=self.clockpath)
    def test_exact_target_metadata_and_origin_flags_are_not_aliases(self):
        self.issue();self.owner.consume_publications();self.mature()
        self.owner.observe_native_source(self.csv,clock_path=self.clockpath);self.owner.settle()
        data,p=self.owner.export_evaluation()
        cases=(('target_bar_start_epoch',float(self.cutoff+3600)),('original_origin_preserved',1),
            ('target_mid_hex','0x01.19a027525460bp+0'),('later_origin_present',1),('later_origin_equals_original',1))
        for key,value in cases:
            with self.subTest(key=key):
                changed=deepcopy(data);row=changed['native_scores'][0];row['outcome']['target'][key]=value
                row['visibility']['score_sha256']=ledger.digest(row['outcome'])
                with self.assertRaises(ValueError):ev.evaluate(changed,p)
    def test_forecast_identity_cannot_name_another_decision(self):
        self.issue();self.owner.consume_publications();data,p=self.owner.export_evaluation()
        d=data['publications'][0]['forecast'];a={**d['forecasts'][0],'committed_available_epoch':data['publications'][0]['consumed_epoch']}
        a['forecast_id']='another:'+ev.FAMILY
        self.assertTrue(ev.forecast_errors(a,d,p))
