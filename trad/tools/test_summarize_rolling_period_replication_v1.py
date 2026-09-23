"""Synthetic complete-window artifacts; no historical results or fitting."""
import argparse
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from tools import summarize_rolling_period_replication_v1 as summary
from tools import test_summarize_rolling_specialists_v1 as fixture
from tools import test_summarize_rolling_model_comparison_v1 as old_fixture


def errors(n, mae=.4, rmse=.7):
    return {'rows': n, 'mae_bps': mae if n else None,
            'rmse_bps': rmse if n else None, 'mean_error_bps': .1 if n else None}


def component_payload(group, h):
    rows = []
    for split in summary.SPLITS:
        for cohort, count in zip(summary.COHORTS, (10, 6, 4)):
            for component in summary.COMPONENTS:
                n = count // 2 if component == 'positive_magnitude' else count-count//2 if component == 'nonpositive_magnitude' else count
                key = hashlib.sha256(f'{h}_{split}_{cohort}_{component}'.encode()).hexdigest()
                r = {'context': f'{group}_{h}m', 'horizon_minutes': h, 'group': group,
                    'split': split, 'cohort': cohort, 'component': component,
                    'cohort_endpoint_origins': count, 'component_label_rows_before_baseline_support': n,
                    'excluded_insufficient_component_TRAIN_support': 0, 'matched_rows': n,
                    'matched_pair_clock_sha256': key, 'matched_target_float64_sha256': key,
                    'matched_counts_by_pair': {'EUR_USD': n},
                    'model_before_baseline_support': errors(n), 'model': errors(n),
                    'TRAIN_pair_mean': errors(n, .6, .9), 'TRAIN_pair_median': errors(n, .5, .95),
                    'mae_improvement_vs_TRAIN_median_bps': .1, 'rmse_improvement_vs_TRAIN_mean_bps': .2}
                if component in ('long_exit_cost', 'short_exit_cost'):
                    r['current_quote_wing_persistence'] = {'current_input': 'entry_short' if component == 'long_exit_cost' else 'entry_long', 'score': errors(n, .7, 1.)}
                    r['mae_improvement_vs_persistence_bps'] = .3
                    r['rmse_improvement_vs_persistence_bps'] = .3
                rows.append(r)
    return {'schema': summary.COMPONENT_SCHEMA, 'horizon_minutes': h, 'group': group,
            'baseline_parameters_sha256': str(h)[0]*64, 'rows': rows, 'scope': 'TRAIN only'}


def record_variant(root, group, h, name):
    tag = f'{group}_{h}m_{name}'
    scores = fixture.metric(h, tag)
    return {'forecast': {'path': f'forecasts/{tag}.parquet', 'sha256': '0'*64, 'rows': 24},
            'metrics': fixture.write(root, f'metrics/{tag}.json', scores),
            'diagnostics': fixture.write(root, f'diagnostics/{tag}.json', fixture.diagnostic(h, tag, scores))}


def window(root, start):
    root.mkdir()
    m = {'schema': summary.RESULT_SCHEMA, 'status': 'complete', **summary.COUNTERS,
         'assessment_rows': 24, 'boundaries': {'start': start, 'train_end': start+42*86400,
              'validation_end': start+49*86400, 'end': start+56*86400},
         'family_design': summary.family.group_manifest(), 'family_horizon_minutes': 60,
         'inputs_root': str(root.parent/(root.name+'_inputs')), 'contexts': {}, 'family_contexts': {}, 'comparators': {}}
    for group in summary.GROUPS:
        for h in summary.HORIZONS:
            tag = f'{group}_{h}m'
            m['contexts'][tag] = {'group': group, 'horizon_minutes': h,
                'variants': {v: record_variant(root, group, h, v) for v in summary.VARIANTS},
                'probability_calibration_scores': fixture.write(root, f'diagnostics/{tag}_probability.json', fixture.probability()),
                'component_baseline_scores': fixture.write(root, f'diagnostics/{tag}_component.json', component_payload(group, h)),
                'final_model': {'path': f'models/{tag}.joblib', 'sha256': '1'*64},
                'head_forecasts': {'path': f'heads/{tag}.parquet', 'sha256': '2'*64, 'rows': 24}}
    for group in summary.family.GROUPS:
        if group == 'full_compact50':
            anchor = m['contexts']['compact50_60m']
            ctx = {'group': group, 'horizon_minutes': 60, 'model': copy.deepcopy(anchor['final_model']),
                   'head_forecasts': copy.deepcopy(anchor['head_forecasts']),
                   'component_baseline_scores': copy.deepcopy(anchor['component_baseline_scores']),
                   'variants': {v: copy.deepcopy(anchor['variants'][v]) for v in summary.family.MEAN_ARMS}}
        else:
            ctx = {'group': group, 'horizon_minutes': 60,
                   'model': {'path': f'models/{group}.joblib', 'sha256': '3'*64},
                   'head_forecasts': {'path': f'heads/{group}.parquet', 'sha256': '4'*64, 'rows': 24},
                   'component_baseline_scores': fixture.write(root, f'diagnostics/{group}_component.json', component_payload(group, 60)),
                   'variants': {v: record_variant(root, group, 60, v) for v in summary.family.MEAN_ARMS}}
        m['family_contexts'][group] = ctx
    for h in summary.HORIZONS:
        for g in summary.GROUPS:
            for learner in ('ridge', 'hgb'):
                tag = f'comparator_{g}_{learner}_{h}m'
                m['comparators'][tag] = {'name': tag, 'horizon_minutes': h, 'group': g, 'learner': learner,
                    'forecast': {'path': f'forecast/{tag}.parquet', 'sha256': '5'*64, 'rows': 24},
                    'metrics': fixture.write(root, f'metrics/{tag}.json', old_fixture.metrics(h, tag))}
        for name in summary.original.CONTROLS:
            tag = f'comparator_{name}_{h}m'
            m['comparators'][tag] = {'name': name, 'horizon_minutes': h,
                'forecast': {'path': f'forecast/{tag}.parquet', 'sha256': '6'*64, 'rows': 24},
                'metrics': fixture.write(root, f'metrics/{tag}.json', old_fixture.metrics(h, tag))}
    fixture.write(root, 'RESULTS.json', m)
    return m


def two_windows(directory):
    roots = [Path(directory)/'earlier', Path(directory)/'later']
    manifests = [window(roots[0], 1704067200), window(roots[1], 1704067200+56*86400)]
    return roots, manifests


class PeriodReplicationSummaryTests(unittest.TestCase):
    def test_every_window_split_gate_cohort_and_component_retained(self):
        with tempfile.TemporaryDirectory() as d:
            roots, _ = two_windows(d)
            r = summary.summarize_periods(reversed(roots))
            self.assertEqual(r['window_count'], 2)
            self.assertEqual(r['replication_gate_period_rows'], 160)
            self.assertEqual(r['family_gate_period_rows'], 128)
            self.assertEqual(r['contemporaneous_comparator_period_rows'], 72)
            for w in r['windows']:
                self.assertEqual(w['assessment_rows'], 24)
                self.assertEqual(len(w['family_forecast_and_policy_deltas']), 168)
                self.assertEqual(len(w['family_component_error_deltas']), 210)
                self.assertEqual(len(w['replication_component_baselines']), 120)
                self.assertEqual(len(w['family_component_baselines']), 240)
                self.assertTrue(all(x['matched_forecast_population_verified'] for x in w['family_forecast_and_policy_deltas']))
                self.assertTrue(all(set(x['cohorts']) == set(summary.COHORTS) for x in w['replication_rows']+w['family_rows']))
            self.assertEqual(r['models_promoted'], 0)
            json.dumps(r, allow_nan=False)
            self.assertIn('30-minute family question remains unresolved', summary.markdown_report(r))

    def test_partial_missing_and_overlapping_windows_refused(self):
        with tempfile.TemporaryDirectory() as d:
            roots, ms = two_windows(d)
            ms[0]['status'] = 'running'; fixture.write(roots[0], 'RESULTS.json', ms[0])
            with self.assertRaisesRegex(ValueError, 'complete_v2'):
                summary.summarize_periods(roots)
            ms[0]['status'] = 'complete'; del ms[0]['family_contexts']['drop_calendar']
            fixture.write(roots[0], 'RESULTS.json', ms[0])
            with self.assertRaisesRegex(ValueError, 'eight_family'):
                summary.summarize_periods(roots)
            with self.assertRaisesRegex(ValueError, 'two_distinct'):
                summary.summarize_periods([roots[1], roots[1]])
            ms[0]['family_contexts']['drop_calendar'] = copy.deepcopy(ms[1]['family_contexts']['drop_calendar'])
            fixture.write(roots[0], 'RESULTS.json', ms[0])
            ms[1]['boundaries'] = {k: v-7*86400 for k, v in ms[1]['boundaries'].items()}
            fixture.write(roots[1], 'RESULTS.json', ms[1])
            with self.assertRaisesRegex(ValueError, 'windows_must_be_separate'):
                summary.summarize_periods(roots)

    def test_bound_metric_tampering_and_path_escape_refused(self):
        with tempfile.TemporaryDirectory() as d:
            roots, ms = two_windows(d)
            rec = ms[0]['contexts']['compact38_30m']['variants']['direct']['metrics']
            (roots[0]/rec['path']).write_text('{}')
            with self.assertRaisesRegex(ValueError, 'hash_mismatch'):
                summary.summarize_periods(roots)
            rec['path'] = '../outside.json'; fixture.write(roots[0], 'RESULTS.json', ms[0])
            with self.assertRaisesRegex(ValueError, 'inside_result'):
                summary.summarize_periods(roots)

    def test_family_full_anchor_must_reuse_exact_reference(self):
        with tempfile.TemporaryDirectory() as d:
            roots, ms = two_windows(d)
            ms[0]['family_contexts']['full_compact50']['head_forecasts']['sha256'] = '7'*64
            fixture.write(roots[0], 'RESULTS.json', ms[0])
            with self.assertRaisesRegex(ValueError, 'reuse_exact_final'):
                summary.summarize_periods(roots)

    def test_family_population_hashes_prevent_equal_count_substitution(self):
        with tempfile.TemporaryDirectory() as d:
            roots, ms = two_windows(d)
            rec = ms[0]['family_contexts']['drop_peers']['component_baseline_scores']
            payload = json.loads((roots[0]/rec['path']).read_bytes())
            payload['rows'][0]['matched_pair_clock_sha256'] = '9'*64
            ms[0]['family_contexts']['drop_peers']['component_baseline_scores'] = fixture.write(roots[0], rec['path'], payload)
            fixture.write(roots[0], 'RESULTS.json', ms[0])
            with self.assertRaisesRegex(ValueError, 'population_hash_mismatch'):
                summary.summarize_periods(roots)

    def test_component_support_exclusions_keep_scores_but_null_unverified_error_delta(self):
        with tempfile.TemporaryDirectory() as d:
            roots, _ = two_windows(d); w = summary._window(roots[0])
            components = copy.deepcopy(w['family_component_baselines'])
            for r in components:
                if r['component'] == 'expected_absolute_move':
                    r['matched_rows'] -= 1
                    r['excluded_insufficient_component_TRAIN_support'] = 1
            deltas, _ = summary._family_deltas(w['family_rows'], components)
            self.assertTrue(all(not x['matched_forecast_population_verified'] for x in deltas))
            self.assertTrue(all(x['mae_delta_removed_minus_full_bps'] is None and x['rmse_delta_removed_minus_full_bps'] is None for x in deltas))
            self.assertTrue(all('net_mean_delta_removed_minus_full_by_extra_cost_bps' in x for x in deltas))

    def test_quoted_only_single_period_case_is_not_dropped(self):
        with tempfile.TemporaryDirectory() as d:
            roots, ms = two_windows(d)
            def mutate(scores, diag):
                for gate in summary.GATES:
                    for split, net in zip(summary.SPLITS, (-2., -.2)):
                        for phase in ('primary', 'one_minute_entry_delay'):
                            p = scores[gate][split][phase]['policies'][summary.specialist.POLICIES[gate]]
                            for c in summary.COHORTS:
                                for cost, scenario in p['cohorts'][c]['cost_scenarios'].items():
                                    scenario['mean_net_bps'] = net+1-float(cost)
                        for c in summary.COHORTS:
                            s = diag['periods'][split]['cohorts'][c]['selection'][gate]
                            s['mean_actual_net_after_extra_1bp_bps'] = net
                            s['mean_predicted_net_after_extra_1bp_bps'] = net+.5
            fixture.mutate_variant(roots[0], ms[0], 'compact38_30m', 'direct', mutate)
            result = summary.summarize_periods(roots)
            flags = next(f for f in result['windows'][0]['replication_positive_period_flags']
                         if f['context'] == 'compact38_30m' and f['variant'] == 'direct' and f['gate'] == summary.GATES[0])
            c = flags['cohorts']['full_endpoint']
            self.assertEqual(c['positive_periods_by_extra_cost_bps']['0.0'], ['later_development_test'])
            self.assertEqual(c['positive_periods_by_extra_cost_bps']['1.0'], [])
            self.assertIn('Positive cases in any cohort', summary.markdown_report(result))

    def test_family_meta_addition_and_future_decision_filter_refused(self):
        with tempfile.TemporaryDirectory() as d:
            roots, ms = two_windows(d)
            family = ms[0]['family_contexts']['drop_peers']
            family['variants']['mixture_calibrated'] = copy.deepcopy(family['variants']['direct'])
            fixture.write(roots[0], 'RESULTS.json', ms[0])
            with self.assertRaisesRegex(ValueError, 'H1_base_direct_raw_only'):
                summary.summarize_periods(roots)
            del family['variants']['mixture_calibrated']; fixture.write(roots[0], 'RESULTS.json', ms[0])
            def mutate(scores, diag):
                scores[summary.GATES[0]]['validation']['primary']['decision_rule']['future_label_masks_used_for_decisions'] = True
            fixture.mutate_variant(roots[0], ms[0], 'compact38_30m', 'direct', mutate)
            with self.assertRaisesRegex(ValueError, 'decision_contract'):
                summary.summarize_periods(roots)

    def test_component_cost_wing_and_gain_checks(self):
        with tempfile.TemporaryDirectory() as d:
            roots, ms = two_windows(d)
            rec = ms[0]['contexts']['compact38_30m']['component_baseline_scores']
            p = json.loads((roots[0]/rec['path']).read_bytes())
            row = next(r for r in p['rows'] if r['component'] == 'long_exit_cost')
            row['current_quote_wing_persistence']['current_input'] = 'entry_long'
            ms[0]['contexts']['compact38_30m']['component_baseline_scores'] = fixture.write(roots[0], rec['path'], p)
            fixture.write(roots[0], 'RESULTS.json', ms[0])
            with self.assertRaisesRegex(ValueError, 'quote_wing'):
                summary.summarize_periods(roots)

    def test_output_is_separate_new_directory_and_never_overwrites(self):
        with tempfile.TemporaryDirectory() as d:
            roots, _ = two_windows(d)
            with self.assertRaisesRegex(ValueError, 'new_output_outside'):
                summary.run(argparse.Namespace(comparison=roots, output=roots[0]/'new', old_reference=None))
            out = Path(d)/'report'
            summary.run(argparse.Namespace(comparison=roots, output=out, old_reference=None))
            self.assertTrue((out/'SUMMARY.json').is_file())
            self.assertTrue((out/'SUMMARY.md').is_file())
            with self.assertRaisesRegex(ValueError, 'new_output_outside'):
                summary.run(argparse.Namespace(comparison=roots, output=out, old_reference=None))

    def test_optional_old_reference_must_match_both_window_pins(self):
        with tempfile.TemporaryDirectory() as d:
            roots, ms = two_windows(d)
            prior, _ = fixture.fixture(d)
            digest = hashlib.sha256((prior/'RESULTS.json').read_bytes()).hexdigest()
            for root, m in zip(roots, ms):
                m['prior_specialist_results_sha256'] = digest
                fixture.write(root, 'RESULTS.json', m)
            result = summary.summarize_periods(roots, old_reference_root=prior)
            self.assertEqual(result['old_examined_reference']['results_sha256'], digest)
            self.assertEqual(result['old_examined_reference']['variants'], 28)
            ms[1]['prior_specialist_results_sha256'] = '8'*64
            fixture.write(roots[1], 'RESULTS.json', ms[1])
            with self.assertRaisesRegex(ValueError, 'old_reference_must_match'):
                summary.summarize_periods(roots, old_reference_root=prior)


if __name__ == '__main__':
    unittest.main()
