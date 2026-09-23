"""Summarize two COMPLETE period replications without fitting or partial reads.

Read hash-bound metrics/diagnostics only. Forecast/model references are retained
as manifest declarations; numerical replay belongs to the independent auditor.
Every window, split, gate and path cohort remains separate. No pooled winner.
"""
from __future__ import annotations

import argparse
import copy
from datetime import datetime, timezone
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import oanda_rolling_family_design_v1 as family
from tools import summarize_rolling_specialists_v1 as specialist
from tools import summarize_rolling_model_comparison_v1 as original

RESULT_SCHEMA = 'rolling_specialist_replication_v2_20260915'
SCHEMA = 'rolling_period_replication_summary_v1_20260915'
COMPONENT_SCHEMA = 'rolling_replication_component_baselines_v1'
VARIANTS = ('direct', 'mixture_raw', 'mixture_calibrated', 'direct_ridge', 'direct_context_hgb')
GROUPS, HORIZONS = specialist.GROUPS, specialist.HORIZONS
GATES, SPLITS, COHORTS = specialist.GATES, specialist.SPLITS, specialist.COHORTS
COMPONENTS = ('expected_absolute_move', 'positive_magnitude', 'nonpositive_magnitude',
              'long_exit_cost', 'short_exit_cost')
COUNTERS = {'completed_base_bundles': 20, 'completed_variants': 20,
            'completed_family_refits': 7, 'completed_family_mean_variants': 16,
            'completed_comparator_fits': 8}


def _read(root, record, receipts):
    return specialist._bound(root, record, record['path'], receipts)


def _boundaries(value):
    if set(value) != {'start', 'train_end', 'validation_end', 'end'}:
        raise ValueError('exact_window_boundaries_required')
    ordered = [original._count(value[k], k) for k in ('start', 'train_end', 'validation_end', 'end')]
    if not all(a < b for a, b in zip(ordered, ordered[1:])) or any(t % 60 for t in ordered):
        raise ValueError('ordered_minute_window_boundaries_required')
    return {k: value[k] for k in ('start', 'train_end', 'validation_end', 'end')}


def _variant_rows(root, manifest, context, variants, receipts, *, section):
    out = []
    n = original._count(manifest['assessment_rows'], 'assessment_rows')
    for name in variants:
        entry = context['variants'][name]
        if entry['forecast'].get('rows') != n:
            raise ValueError('forecast_manifest_must_retain_all_original_origins')
        scores = _read(root, entry['metrics'], receipts)
        diag = _read(root, entry['diagnostics'], receipts)
        if (scores.get('schema') != 'rolling_specialist_scoring_v1_20260915'
                or scores['future_labels_used_for_decisions'] is not False
                or scores['mean_forecasts_changed_by_cost_gate'] is not False
                or scores['all_original_assessment_origins'] != n
                or diag['horizon_minutes'] != context['horizon_minutes']
                or diag['future_cohorts_are_entry_filters'] is not False):
            raise ValueError('specialist_scope_or_original_population_mismatch')
        for gate in GATES:
            if set(scores[gate]) != set(SPLITS):
                raise ValueError('both_separate_assessment_splits_required')
            if sum(scores[gate][s]['primary']['input_origins'] for s in SPLITS) != n:
                raise ValueError('original_assessment_count_mismatch')
            for split in SPLITS:
                r = specialist._row(context, name, gate, split, scores, diag,
                                    entry['metrics'], entry['diagnostics'])
                if r['issued_forecasts'] != r['input_origins']:
                    raise ValueError('learned_forecasts_must_issue_on_every_origin')
                r.update(section=section, forecast_manifest_reference=copy.deepcopy(entry['forecast']))
                out.append(r)
        for split in SPLITS:
            if scores[GATES[0]][split]['primary']['forecast_cohorts'] != scores[GATES[1]][split]['primary']['forecast_cohorts']:
                raise ValueError('changing_cost_gate_must_preserve_forecast_errors')
    return out


def _validate_error(value, n):
    if original._count(value['rows'], 'error_rows') != n:
        raise ValueError('component_same_row_error_count_required')
    for name in ('mae_bps', 'rmse_bps', 'mean_error_bps'):
        x = original._finite(value[name], name)
        if n and (x is None or name != 'mean_error_bps' and x < 0):
            raise ValueError('finite_component_error_required')
        if not n and x is not None:
            raise ValueError('empty_component_error_must_be_null')


def _component_rows(payload, group, h, metric_rows, *, source_group=None):
    inner_group = source_group or group
    if (payload.get('schema') != COMPONENT_SCHEMA or payload['group'] != inner_group
            or payload['horizon_minutes'] != h or not isinstance(payload['scope'], str)):
        raise ValueError('registered_component_baseline_scope_required')
    digest = payload['baseline_parameters_sha256']
    if not isinstance(digest, str) or len(digest) != 64:
        raise ValueError('baseline_parameter_hash_required')
    expected = {(s, c, k) for s in SPLITS for c in COHORTS for k in COMPONENTS}
    got = {(r['split'], r['cohort'], r['component']) for r in payload['rows']}
    if got != expected or len(payload['rows']) != len(expected):
        raise ValueError('exact_30_component_comparisons_required')
    scores = {(r['split'], c): r['cohorts'][c]['forecast_errors']['scored_forecasts']
              for r in metric_rows if r['variant'] == 'direct' and r['gate'] == GATES[0]
              for c in COHORTS}
    result = []
    for r in payload['rows']:
        if (r['group'] != inner_group or r['horizon_minutes'] != h
                or r['context'] != f'{inner_group}_{h}m'
                or r['cohort_endpoint_origins'] != scores[r['split'], r['cohort']]):
            raise ValueError('component_and_forecast_cohort_identity_required')
        n = original._count(r['matched_rows'], 'matched_rows')
        before = original._count(r['component_label_rows_before_baseline_support'], 'before_support')
        excluded = original._count(r['excluded_insufficient_component_TRAIN_support'], 'excluded_support')
        if n + excluded != before or before > r['cohort_endpoint_origins']:
            raise ValueError('component_support_partition_required')
        if sum(original._count(v, 'pair_rows') for v in r['matched_counts_by_pair'].values()) != n:
            raise ValueError('component_pair_counts_must_sum')
        for k in ('matched_pair_clock_sha256', 'matched_target_float64_sha256'):
            if not isinstance(r[k], str) or len(r[k]) != 64:
                raise ValueError('component_population_hash_required')
        for k in ('model', 'TRAIN_pair_mean', 'TRAIN_pair_median'):
            _validate_error(r[k], n)
        _validate_error(r['model_before_baseline_support'], before)
        for key, baseline, error in (
            ('mae_improvement_vs_TRAIN_median_bps', 'TRAIN_pair_median', 'mae_bps'),
            ('rmse_improvement_vs_TRAIN_mean_bps', 'TRAIN_pair_mean', 'rmse_bps')):
            if not specialist._equal_number(r[key], original._difference(r[baseline][error], r['model'][error])):
                raise ValueError('component_baseline_gain_arithmetic_mismatch')
        if r['component'] in ('long_exit_cost', 'short_exit_cost'):
            p = r['current_quote_wing_persistence']
            expected_input = 'entry_short' if r['component'] == 'long_exit_cost' else 'entry_long'
            if p['current_input'] != expected_input:
                raise ValueError('asymmetric_quote_wing_persistence_required')
            _validate_error(p['score'], n)
            for error in ('mae', 'rmse'):
                if not specialist._equal_number(r[f'{error}_improvement_vs_persistence_bps'],
                        original._difference(p['score'][error+'_bps'], r['model'][error+'_bps'])):
                    raise ValueError('persistence_gain_arithmetic_mismatch')
        item = copy.deepcopy(r)
        item.update(source_group=inner_group, group=group, context=f'{group}_{h}m',
                    baseline_parameters_sha256=digest)
        result.append(item)
    return result


def _comparator_row(tag, entry, split, source, record):
    # The current runner's compact records do not pretend to contain the older
    # runner's training_selection structure. Preserve actual provenance only.
    p, d = source['primary'], source['one_minute_entry_delay']
    h = entry['horizon_minutes']
    for report in (p, d):
        if (report['horizon_minutes'] != h or report['decision_rule']['fixed_margin_bps'] != 1.
                or report['decision_rule']['future_label_masks_used_for_decisions'] is not False
                or report['decision_rule']['comparison_mask_used_for_decisions'] is not False
                or report['evaluation_cost']['primary_scenario_key'] != '1.0'):
            raise ValueError('comparator_fixed_origin_gate_required')
    for policy in p['policies']:
        a, b = p['policies'][policy], d['policies'][policy]
        if a['decisions'] != b['decisions'] or a['decision_pair_clock_side_sha256'] != b['decision_pair_clock_side_sha256']:
            raise ValueError('comparator_delay_changed_decisions')
    cohorts = {c: original._cohort(p, d, c) for c in COHORTS}
    for getter in (lambda x: x['forecast_errors']['scored_forecasts'], lambda x: x['nonoverlap_scored_decisions']):
        if getter(cohorts[COHORTS[0]]) != sum(getter(cohorts[c]) for c in COHORTS[1:]):
            raise ValueError('comparator_path_cohorts_partition_required')
    return {'cell': tag, 'name': entry['name'], 'horizon_minutes': h,
            'learner': entry.get('learner'), 'group': entry.get('group', 'control'),
            'split': split, 'gate': GATES[0], 'cohorts': cohorts,
            'input_origins': p['input_origins'], 'issued_forecasts': p['issued_forecasts'],
            'comparison_origins': p['comparison_origins'],
            'own_coverage_forecast_errors': original._errors(p['own_coverage_forecast_scores_without_comparator_restriction']),
            'metrics_artifact': record, 'forecast_manifest_reference': copy.deepcopy(entry['forecast']),
            'training_provenance': copy.deepcopy(entry.get('training_selection')),
            'comparison_scope': 'same-window primary observed-spread gate; not the secondary expected-cost gate'}


def _family_deltas(rows, component_rows):
    index = {(r['group'], r['variant'], r['gate'], r['split']): r for r in rows}
    comps = {(r['group'], r['split'], r['cohort'], r['component']): r for r in component_rows}
    deltas, component_deltas = [], []
    for group in family.GROUPS[1:]:
        for split in SPLITS:
            for c in COHORTS:
                ca, cb = (comps[g, split, c, 'expected_absolute_move'] for g in ('full_compact50', group))
                matched = (ca['matched_rows'] == ca['cohort_endpoint_origins'] == cb['matched_rows'] == cb['cohort_endpoint_origins']
                           and ca['matched_pair_clock_sha256'] == cb['matched_pair_clock_sha256']
                           and ca['matched_target_float64_sha256'] == cb['matched_target_float64_sha256'])
                if ca['baseline_parameters_sha256'] != cb['baseline_parameters_sha256']:
                    raise ValueError('family_baselines_must_share_TRAIN_parameters')
                if ca['matched_rows'] == ca['cohort_endpoint_origins'] == cb['matched_rows'] == cb['cohort_endpoint_origins'] and not matched:
                    raise ValueError('family_common_score_population_hash_mismatch')
                for variant in family.MEAN_ARMS:
                    for gate in GATES:
                        a, b = (index[g, variant, gate, split] for g in ('full_compact50', group))
                        ac, bc = a['cohorts'][c], b['cohorts'][c]
                        ae, be = ac['forecast_errors'], bc['forecast_errors']
                        if matched and ae['scored_forecasts'] != be['scored_forecasts']:
                            raise ValueError('verified_family_population_count_mismatch')
                        deltas.append({'reference_group': 'full_compact50', 'candidate_group': group,
                            'variant': variant, 'gate': gate, 'split': split, 'cohort': c,
                            'matched_forecast_population_verified': matched,
                            'reference_scored_forecasts': ae['scored_forecasts'], 'candidate_scored_forecasts': be['scored_forecasts'],
                            'mae_delta_removed_minus_full_bps': original._difference(be['mae_bps'], ae['mae_bps']) if matched else None,
                            'rmse_delta_removed_minus_full_bps': original._difference(be['rmse_bps'], ae['rmse_bps']) if matched else None,
                            'reference_scored_decisions': ac['nonoverlap_scored_decisions'], 'candidate_scored_decisions': bc['nonoverlap_scored_decisions'],
                            'net_mean_delta_removed_minus_full_by_extra_cost_bps': {k: original._difference(bc['net_cost_scenarios'][k]['mean_net_bps'], ac['net_cost_scenarios'][k]['mean_net_bps']) for k in ('0.0', '1.0', '2.0')},
                            'same_original_decision_digest': a['nonoverlap_decision_sha256'] == b['nonoverlap_decision_sha256'],
                            'interpretation': 'positive error delta means removal worsens error; own-policy means can use different decisions, not a matched-trade effect'})
                for component in COMPONENTS:
                    a, b = (comps[g, split, c, component] for g in ('full_compact50', group))
                    same = all(a[k] == b[k] for k in ('matched_rows', 'matched_pair_clock_sha256', 'matched_target_float64_sha256', 'baseline_parameters_sha256'))
                    if not same:
                        raise ValueError('component_family_same_population_required')
                    component_deltas.append({'reference_group': 'full_compact50', 'candidate_group': group,
                        'split': split, 'cohort': c, 'component': component, 'matched_rows': a['matched_rows'],
                        'matched_pair_clock_sha256': a['matched_pair_clock_sha256'],
                        'mae_delta_removed_minus_full_bps': original._difference(b['model']['mae_bps'], a['model']['mae_bps']),
                        'rmse_delta_removed_minus_full_bps': original._difference(b['model']['rmse_bps'], a['model']['rmse_bps'])})
    return deltas, component_deltas


def _window(root):
    root = Path(root).resolve(); path = root/'RESULTS.json'
    raw = path.read_bytes(); m = specialist._load(raw)
    if m.get('schema') != RESULT_SCHEMA or m.get('status') != 'complete':
        raise ValueError('complete_v2_period_replication_required')
    if any(m.get(k) != v for k, v in COUNTERS.items()):
        raise ValueError('all_declared_replication_and_family_runs_required')
    if m.get('family_design') != family.group_manifest() or m.get('family_horizon_minutes') != 60:
        raise ValueError('exact_frozen_H1_family_design_required')
    boundaries = _boundaries(m['boundaries'])
    expected = {f'{g}_{h}m' for g in GROUPS for h in HORIZONS}
    if set(m['contexts']) != expected or set(m['family_contexts']) != set(family.GROUPS):
        raise ValueError('four_replication_and_eight_family_contexts_required')
    receipts = {path: specialist._hash(raw)}
    replication, family_rows, components, family_components = [], [], [], []
    probability = {}
    for tag in sorted(expected):
        ctx = m['contexts'][tag]
        if tag != f"{ctx['group']}_{ctx['horizon_minutes']}m" or set(ctx['variants']) != set(VARIANTS):
            raise ValueError('exact_replication_context_and_five_variants_required')
        rs = _variant_rows(root, m, ctx, VARIANTS, receipts, section='chronological_replication')
        p = specialist._probability(_read(root, ctx['probability_calibration_scores'], receipts))
        for r in rs:
            for c in COHORTS:
                raw_p = p['periods'][r['split']][c]['raw']
                head_p = r['cohorts'][c]['components']['positive_probability']['model']
                if raw_p['rows'] != head_p['rows'] or any(not specialist._equal_number(raw_p[k], head_p[k]) for k in ('brier', 'log_loss')):
                    raise ValueError('raw_and_calibrated_probability_identity_required')
        probability[tag] = p; replication += rs
        record = ctx['component_baseline_scores']
        components += _component_rows(_read(root, record, receipts), ctx['group'], ctx['horizon_minutes'], rs)
    anchor = m['contexts']['compact50_60m']
    for group in family.GROUPS:
        ctx = m['family_contexts'][group]
        if ctx['group'] != group or ctx['horizon_minutes'] != 60 or set(ctx['variants']) != set(family.MEAN_ARMS):
            raise ValueError('family_scope_H1_base_direct_raw_only')
        if group == 'full_compact50':
            for field, anchor_field in (('model', 'final_model'), ('head_forecasts', 'head_forecasts'), ('component_baseline_scores', 'component_baseline_scores')):
                if ctx[field] != anchor[anchor_field]:
                    raise ValueError('full_family_anchor_must_reuse_exact_final_artifacts')
            if any(ctx['variants'][v] != anchor['variants'][v] for v in family.MEAN_ARMS):
                raise ValueError('full_family_anchor_must_reuse_exact_variants')
        rs = _variant_rows(root, m, ctx, family.MEAN_ARMS, receipts, section='H1_final_base_family_removal')
        family_rows += rs
        family_components += _component_rows(_read(root, ctx['component_baseline_scores'], receipts), group, 60, rs,
                                             source_group='compact50' if group == 'full_compact50' else group)
    records = m['comparators']
    specifications = {f'comparator_{g}_{learner}_{h}m': (h, g, learner) for h in HORIZONS for g in GROUPS for learner in ('ridge', 'hgb')}
    specifications.update({f'comparator_{name}_{h}m': (h, None, name) for h in HORIZONS for name in original.CONTROLS})
    if set(records) != set(specifications):
        raise ValueError('exact_18_contemporaneous_comparators_required')
    comparator_rows = []
    for tag, (h, group, learner) in specifications.items():
        entry = records[tag]
        if entry['horizon_minutes'] != h or (group is not None and (entry.get('group') != group or entry.get('learner') != learner)) or (group is None and entry['name'] != learner):
            raise ValueError('comparator_identity_required')
        metrics = _read(root, entry['metrics'], receipts)
        if set(metrics) != set(SPLITS) or sum(metrics[s]['primary']['input_origins'] for s in SPLITS) != m['assessment_rows']:
            raise ValueError('comparator_original_split_population_required')
        comparator_rows += [_comparator_row(tag, entry, s, metrics[s], entry['metrics']) for s in SPLITS]
    deltas, component_deltas = _family_deltas(family_rows, family_components)
    for p, digest in receipts.items():
        if specialist._hash(p.read_bytes()) != digest:
            raise ValueError('input_changed_during_period_summary')
    return {'comparison_root': str(root), 'results_sha256': specialist._hash(raw),
            'window_id': datetime.fromtimestamp(boundaries['start'], timezone.utc).strftime('%Y-%m-%d')+'_'+datetime.fromtimestamp(boundaries['end'], timezone.utc).strftime('%Y-%m-%d'),
            'boundaries': boundaries, 'assessment_rows': m['assessment_rows'], 'declared_counters': {k: m[k] for k in COUNTERS},
            'replication_rows': replication, 'family_rows': family_rows, 'comparator_rows': comparator_rows,
            'probability_calibration': probability, 'replication_component_baselines': components,
            'family_component_baselines': family_components, 'family_forecast_and_policy_deltas': deltas,
            'family_component_error_deltas': component_deltas,
            'replication_positive_period_flags': specialist._flags(replication),
            'family_positive_period_flags': specialist._flags(family_rows),
            'verified_metric_files_and_manifests': {str(p): d for p, d in receipts.items()},
            'source_bindings': copy.deepcopy(m.get('source_bindings', {})),
            'inputs_root': m.get('inputs_root'), 'inputs_sha256': m.get('inputs_sha256'),
            'prior_specialist_results_sha256': m.get('prior_specialist_results_sha256')}


def summarize_periods(comparison_roots, *, old_reference_root=None):
    source_paths = (Path(__file__).resolve(), Path(family.__file__).resolve(),
                    Path(specialist.__file__).resolve(), Path(original.__file__).resolve())
    own_sources = {str(p): specialist._hash(p.read_bytes()) for p in source_paths}
    roots = [Path(p).resolve() for p in comparison_roots]
    if len(roots) != 2 or len(set(roots)) != 2:
        raise ValueError('exactly_two_distinct_complete_period_roots_required')
    windows = sorted((_window(p) for p in roots), key=lambda w: w['boundaries']['start'])
    if windows[0]['boundaries']['end'] > windows[1]['boundaries']['start']:
        raise ValueError('new_historical_windows_must_be_separate')
    reference = specialist.summarize_specialists(old_reference_root) if old_reference_root else None
    if reference and any(w['prior_specialist_results_sha256'] != reference['results_sha256'] for w in windows):
        raise ValueError('old_reference_must_match_both_declared_source_studies')
    # Recheck both manifests after the second window/reference reads.
    for w in windows:
        for p, digest in w['verified_metric_files_and_manifests'].items():
            if specialist._hash(Path(p).read_bytes()) != digest:
                raise ValueError('input_changed_during_two_window_summary')
    if any(specialist._hash(Path(p).read_bytes()) != digest for p, digest in own_sources.items()):
        raise ValueError('summary_source_changed_during_read')
    return {'schema': SCHEMA, 'windows': windows, 'window_count': 2,
            'replication_gate_period_rows': sum(len(w['replication_rows']) for w in windows),
            'family_gate_period_rows': sum(len(w['family_rows']) for w in windows),
            'contemporaneous_comparator_period_rows': sum(len(w['comparator_rows']) for w in windows),
            'family_design': family.group_manifest(), 'old_examined_reference': reference,
            'summary_source_bindings': own_sources,
            'automatic_selection': False, 'models_promoted': 0, 'can_place_orders': False,
            'limits': [
                'Historical periods remain separate; this is not untouched forward confirmation or a pooled winner search.',
                'Family contribution is H1 only. The 30-minute family question remains unresolved.',
                'Eight frozen input groups; fixed 53 columns, removed technical fields NaN, current entry costs and pair identity retained.',
                'Family arms are final base direct/raw mixture only. Calibration and contextual meta arms belong to the separate full-context chronological replication.',
                'Family error comparisons require matching common-origin and target hashes; unsupported coverage yields null error deltas.',
                'Removing a related family tests conditional representation contribution, not causal or independent importance. Aliases are not extra inputs.',
                'Each policy has its own original decisions; mean net differences are not paired-trade treatment effects.',
                'All gates preserve original forecast values. Current-window comparators use the primary observed-spread gate only.',
                'Future path availability is for scoring only, never entry filtering; delay preserves decisions but may change scored coverage.',
                'Raw probability is separate from calibrated probability; direct-only Ridge can reverse its input slope.',
                'Terminal magnitude is not intrahorizon volatility or management/stop accuracy. Exit-cost skill is not trading acceptance.',
                'Related currency pairs and decision clocks are dependent. Candle endpoint bps are not fills, portfolio returns or dollar profit.',
                'Metrics/diagnostics and manifests are hash verified; models/forecast rows are not loaded, replayed, rescored or refitted by this summarizer.',
                'ARIMA is conditional OLS, not recreation of older maximum-likelihood fits; inactive means are null, not losses.'],
            'generated_utc': datetime.now(timezone.utc).isoformat()}


def markdown_report(report):
    fmt = specialist._fmt
    lines = ['# Separate historical periods and H1 family contribution', '',
             'Every period, gate and cohort is retained. The tables show full endpoints; JSON preserves all three path cohorts and complete component comparisons. No model is selected or promoted.', '']
    for w in report['windows']:
        lines += [f"## Window {w['window_id']}", '', f"Original assessment origins: {w['assessment_rows']:,}. Dates and counts belong to this window, not the earlier reference.", '']
        for key, title in (('replication_rows', 'Chronological full-context replication'), ('family_rows', 'H1 final base-head family removals')):
            lines += [f'### {title}', '', '| Context / mean | Gate / period | Forecast n | MAE / RMSE delta vs zero | Decisions / scored | Net +0 / +1 / +2 | PF +1 | Delay +1 (n) | Times / days / pairs | Leave best day / pair | Return / cost optimism |',
                      '|---|---|---:|---|---:|---|---:|---|---|---|---|']
            for r in w[key]:
                c = r['cohorts']['full_endpoint']; e = c['forecast_errors']; pop = c['components']['selection']['scored_decisions']
                sel = c['components']['selection']
                lines.append(f"| {r['context']} / {r['variant']} | {r['gate']} / {r['split']} | {e['scored_forecasts']:,} | {fmt(e['mae_delta_vs_no_change_bps'])} / {fmt(e['rmse_delta_vs_no_change_bps'])} | {c['nonoverlap_decisions_before_score_masks']} / {c['nonoverlap_scored_decisions']} | {' / '.join(fmt(c['net_cost_scenarios'][k]['mean_net_bps']) for k in ('0.0','1.0','2.0'))} | {fmt(c['net_cost_scenarios']['1.0']['profit_factor'])} | {fmt(c['one_minute_delay']['mean_net_1bp_bps'])} ({c['one_minute_delay']['scored_decisions']}) | {pop['unique_original_utc_decision_times']} / {pop['utc_days']} / {pop['pairs']} | {fmt(c['primary_1bp_day_concentration']['leave_best_group_out_mean_net_bps'])} / {fmt(c['primary_1bp_pair_concentration']['leave_best_group_out_mean_net_bps'])} | {fmt(sel['mean_selected_return_optimism_bps'])} / {fmt(sel['mean_selected_exit_cost_optimism_bps'])} |")
            lines.append('')
        lines += ['### Full-context positive-return probability', '',
                  'These probabilities are separately raw or calibrated; family-removal head diagnostics remain raw.', '',
                  '| Context | Period / cohort | n | Raw / calibrated / TRAIN-prior Brier | Raw / calibrated / TRAIN-prior log loss |',
                  '|---|---|---:|---|---|']
        for tag, payload in w['probability_calibration'].items():
            for split in SPLITS:
                for cohort in COHORTS:
                    p = payload['periods'][split][cohort]
                    lines.append(f"| {tag} | {split} / {cohort} | {p['raw']['rows']} | {' / '.join(fmt(p[k]['brier'],6) for k in ('raw','calibrated','TRAIN_pair_prior'))} | {' / '.join(fmt(p[k]['log_loss'],6) for k in ('raw','calibrated','TRAIN_pair_prior'))} |")
        lines.append('')
        lines += ['### Family error changes, compared with the full 50 inputs', '',
                  'Positive error changes mean removal worsens predictions. Policy differences use each model’s own decisions.', '',
                  '| Removed group / mean | Period | Forecasts | MAE / RMSE change | Net +1 change | Same population verified |',
                  '|---|---|---:|---|---:|---|']
        for d in w['family_forecast_and_policy_deltas']:
            if d['gate'] == GATES[0] and d['cohort'] == COHORTS[0]:
                lines.append(f"| {d['candidate_group']} / {d['variant']} | {d['split']} | {d['candidate_scored_forecasts']} | {fmt(d['mae_delta_removed_minus_full_bps'])} / {fmt(d['rmse_delta_removed_minus_full_bps'])} | {fmt(d['net_mean_delta_removed_minus_full_by_extra_cost_bps']['1.0'])} | {d['matched_forecast_population_verified']} |")
        lines += ['', '### Positive cases in any cohort, including quoted-only', '',
                  '| Section / context / mean | Gate / cohort | Positive periods +0 / +1 / +2 | Minimum scored n |', '|---|---|---|---:|']
        for section, key in (('replication', 'replication_positive_period_flags'), ('family', 'family_positive_period_flags')):
            for r in w[key]:
                for cohort, flag in r['cohorts'].items():
                    if any(flag['positive_in_any_period_by_extra_cost_bps'].values()):
                        periods = ['/'.join(flag['positive_periods_by_extra_cost_bps'][k]) or 'none' for k in ('0.0','1.0','2.0')]
                        lines.append(f"| {section} / {r['context']} / {r['variant']} | {r['gate']} / {cohort} | {' ; '.join(periods)} | {flag['minimum_scored_decisions_across_periods']} |")
        lines += ['', '### Component skill against TRAIN-only references', '',
                  'Positive gains mean lower error. MAE compares TRAIN medians; RMSE compares TRAIN means. Costs additionally retain current quote-wing persistence.', '',
                  '| Section / context | Period | Component | Matched / unsupported | MAE / RMSE gain | Persistence MAE / RMSE gain |',
                  '|---|---|---|---:|---|---|']
        for section, key in (('replication', 'replication_component_baselines'), ('family', 'family_component_baselines')):
            for r in w[key]:
                if r['cohort'] == COHORTS[0]:
                    lines.append(f"| {section} / {r['context']} | {r['split']} | {r['component']} | {r['matched_rows']} / {r['excluded_insufficient_component_TRAIN_support']} | {fmt(r['mae_improvement_vs_TRAIN_median_bps'])} / {fmt(r['rmse_improvement_vs_TRAIN_mean_bps'])} | {fmt(r.get('mae_improvement_vs_persistence_bps'))} / {fmt(r.get('rmse_improvement_vs_persistence_bps'))} |")
        lines += ['', '### Same-window original model/control comparators', '', '| Comparator | Period | Forecasts | MAE / RMSE delta vs zero | Scored decisions | Net +0 / +1 / +2 | Delay +1 (n) |', '|---|---|---:|---|---:|---|---|']
        for r in w['comparator_rows']:
            c = r['cohorts'][COHORTS[0]]; e = c['forecast_errors']
            lines.append(f"| {r['cell']} | {r['split']} | {e['scored_forecasts']} | {fmt(e['mae_delta_vs_no_change_bps'])} / {fmt(e['rmse_delta_vs_no_change_bps'])} | {c['nonoverlap_scored_decisions']} | {' / '.join(fmt(c['net_cost_scenarios'][k]['mean_net_bps']) for k in ('0.0','1.0','2.0'))} | {fmt(c['one_minute_delay']['mean_net_1bp_bps'])} ({c['one_minute_delay']['scored_decisions']}) |")
        lines.append('')
    lines += ['## Limits', ''] + ['- '+s for s in report['limits']]
    if report['old_examined_reference']:
        old = report['old_examined_reference']
        lines += ['', '## Earlier examined reference', '',
                  f"The JSON also retains the sealed earlier specialist summary, RESULTS SHA256 {old['results_sha256']}. Its dates and seven-arm experiment remain a separate reference, not extra observations in either new window."]
    return '\n'.join(lines)+'\n'


def run(args):
    output = Path(args.output).resolve()
    roots = [Path(p).resolve() for p in args.comparison]
    if output.exists() or any(output.is_relative_to(r) for r in roots):
        raise ValueError('new_output_outside_comparison_roots_required')
    report = summarize_periods(roots, old_reference_root=getattr(args, 'old_reference', None))
    if any(w['inputs_root'] and output.is_relative_to(Path(w['inputs_root']).resolve()) for w in report['windows']):
        raise ValueError('output_cannot_modify_prepared_inputs')
    if report['old_examined_reference'] and output.is_relative_to(Path(report['old_examined_reference']['comparison_root'])):
        raise ValueError('output_cannot_modify_old_reference')
    output.mkdir(parents=True, exist_ok=False)
    import json
    (output/'SUMMARY.json').write_text(json.dumps(report, indent=2, sort_keys=True, allow_nan=False)+'\n', encoding='utf-8')
    (output/'SUMMARY.md').write_text(markdown_report(report), encoding='utf-8')
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--comparison', type=Path, action='append', required=True)
    parser.add_argument('--old-reference', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    result = run(parser.parse_args())
    import json
    print(json.dumps({k: result[k] for k in ('window_count', 'replication_gate_period_rows', 'family_gate_period_rows', 'contemporaneous_comparator_period_rows')}))
