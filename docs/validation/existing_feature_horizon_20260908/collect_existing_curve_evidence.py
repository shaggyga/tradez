"""Bounded JSON/source/opaque-byte audit; never imports project or model objects."""
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path

WORK = Path(__file__).resolve().parent
C = WORK.parent / 'trad'
D = Path('D:/forex/trad')
U = Path('C:/Users/zmoor/AppData/Local/ForexResearchData/unified_intrahour_v1')
F = C / 'fresh_m1_intrahour/reports/feature_forecast_full_20260713_v4'
R = C / 'fresh_m1_intrahour/reports/htf_corrected_arima_features_tier1_2026_20260712'
T = C / 'fresh_m1_intrahour/reports/htf_validation_replay_corrected_execution_20260710_v2'
sources = {}

def sha(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        while block := stream.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()

def retain(label, path):
    if path.stat().st_size > 8 * 1024 * 1024:
        raise ValueError('bounded_metadata_file_too_large')
    raw = path.read_bytes()
    target = WORK / 'retained' / label
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open('xb') as stream:
        stream.write(raw)
    sources[label] = {'original_path': str(path), 'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest(), 'retained_path': str(target)}
    return json.loads(raw) if path.suffix == '.json' else None

def artifact(path, expected=None):
    value = {'path': str(path), 'exists': path.is_file(), 'deserialized': False}
    if path.is_file():
        value['bytes'] = path.stat().st_size
        if value['bytes'] > 64 * 1024 * 1024:
            value['hash_check'] = 'not_performed_large_opaque_artifact'
        else:
            value['sha256'] = sha(path)
            value['expected_sha256'] = expected
            value['matches_retained_manifest'] = value['sha256'] == expected if expected else None
            if expected and not value['matches_retained_manifest']:
                raise ValueError('artifact_manifest_mismatch')
    return value

def scalars(value):
    return {k: v for k, v in value.items() if not isinstance(v, (dict, list))}

def main():
    begun = datetime.now(timezone.utc).isoformat()
    unified = retain('unified/UNIFIED_FORECAST_VALIDATION.json', D / 'fresh_m1_intrahour/reports/unified_forecast_full_validation_native_pip_20260725/UNIFIED_FORECAST_VALIDATION.json')
    matrix = retain('unified/matrix_manifest.json', U / 'matrix_manifest.json')
    retain('unified/build_manifest.json', U / 'build_manifest.json')
    retain('unified/UNIFIED_FORECAST_RUN_COMPLETE.json', D / 'fresh_m1_intrahour/reports/unified_forecast_full_validation_native_pip_20260725/UNIFIED_FORECAST_RUN_COMPLETE.json')
    retain('unified/unified_feature_registry.csv', U / 'unified_feature_registry.csv')
    second = retain('second/second_ridge_fit_v1.json', D / 'data/oanda_training_manager/reports/second_ridge_fit_v1.json')
    models = retain('second/second_ridge_models_v1.json', D / 'data/oanda_training_manager/state/second_ridge_models_v1.json')
    experiment = retain('htf/exp_20260702_022306_e09b53ec8d.json', D / 'data/oanda_training_manager/continuous_research/experiments/exp_20260702_022306_e09b53ec8d.json')
    timing = retain('htf/HTF_FEATURE_TIMING_AUDIT.json', T / 'HTF_FEATURE_TIMING_AUDIT.json')
    retain('htf/HTF_FINAL_SUMMARY.json', T / 'HTF_FINAL_SUMMARY.json')
    corrected = retain('htf/HTF_CORRECTED_FINAL_SUMMARY.json', R / 'HTF_CORRECTED_FINAL_SUMMARY.json')
    corrected_report = retain('htf/HTF_CORRECTED_REBUILD_REPORT.json', R / 'HTF_CORRECTED_REBUILD_REPORT.json')
    retain('htf/HTF_CORRECTED_LABEL_AUDIT.json', R / 'HTF_CORRECTED_LABEL_AUDIT.json')
    benchmark = retain('features/FEATURE_FORECAST_REPORT.json', F / 'FEATURE_FORECAST_REPORT.json')
    retain('features/FEATURE_FORECAST_REPORT.md', F / 'FEATURE_FORECAST_REPORT.md')
    index = retain('features/offline_research_forecast_bundle_index.json', F / 'offline_research_forecast_bundle_index.json')
    two_stage = retain('features/TWO_STAGE_FORECAST_REPORT.json', F / 'TWO_STAGE_FORECAST_REPORT.json')
    retain('features/run_manifest.json', F / 'run_manifest.json')
    panels = {}
    for name in ['M5', 'M15', 'M30', 'H1', 'H2']:
        path = D / 'data/oanda_training_manager/reports/multihorizon_panel_model_20260802' / (name + '_V2_FULL_FINAL.json')
        panels[name] = retain('panel/' + path.name, path)
    retain('panel/BETTER_MODELS_AUDIT_20260802.md', D / 'data/oanda_training_manager/reports/multihorizon_panel_model_20260802/BETTER_MODELS_AUDIT_20260802.md')
    source_names = [
        (D, 'fresh_m1_intrahour/src/unified_forecast.py'),
        (D, 'fresh_m1_intrahour/src/htf_validation_replay.py'),
        (D, 'fresh_m1_intrahour/src/htf_corrected_rebuild.py'),
        (D, 'fresh_m1_intrahour/src/feature_forecast_benchmark.py'),
        (C, 'oanda_second_forecast.py'), (C, 'oanda_second_forecast_fit.py'),
        (C, 'oanda_second_forecast_runner.py'), (C, 'oanda_multihorizon_panel_model.py'),
        (C, 'oanda_shared_timeframe_horizon_panel.py'),
    ]
    for root, name in source_names:
        retain('source/' + ('D_' if root == D else 'C_') + Path(name).name, root / name)
    bundles = {}
    for target, item in index['target_bundles'].items():
        bundles[target] = {'canonical_C': artifact(F / item['path'], item['sha256']),
                           'D_copy': artifact(D / F.relative_to(C) / item['path'], item['sha256']),
                           'candidate_id': item['candidate_id'], 'target_definition': item['target_definition']}
    model_rows = [row for pairs in models['models'].values() for row in pairs.values()]
    if len(model_rows) != 884 or len(models['feature_names']) != 14:
        raise ValueError('second_surface_count_mismatch')
    coeff_shapes = []
    for row in model_rows:
        # Numeric JSON is inspected as data, never executed or deserialized as an object.
        coeff_shapes.append({key: len(value) for key, value in row.items() if key in ['coefficients', 'means', 'scales'] and isinstance(value, list)})
    unified_final = unified['untouched_final']['selected_model']
    evidence = {
        'status': 'completed_bounded_read_only_audit', 'started_utc': begun,
        'finished_utc': datetime.now(timezone.utc).isoformat(),
        'unified_795': {
            'capability': 'trained_multioutput_midpoint_endpoint_curve',
            'schema_version_in_report_and_matrix': unified['feature_schema_version'],
            'current_D_source_schema': 'unified_intrahour_features_v5_opportunity',
            'source_version_compatibility_not_assumed': True,
            'numeric_features': matrix['numeric_feature_count'], 'categorical_features': matrix['categorical_features'],
            'matrix_rows': matrix['rows'], 'matrix_pairs': matrix['pairs'],
            'matrix_start_utc': matrix['start_utc'], 'matrix_end_utc': matrix['end_utc'],
            'matrix_sampling_cadence_minutes': matrix['timestamp_cadence_minutes'],
            'large_matrix_hash_reference_only': matrix['sha256'], 'large_matrix_rehashed': False,
            'horizons_minutes': [row['horizon_minutes'] for row in unified_final['per_horizon']],
            'winner': {k: unified['selection']['winner'][k] for k in ['model_candidate', 'feature_count', 'feature_family', 'train_rows']},
            'split': unified['split'], 'final_rows': unified['untouched_final']['rows'],
            'final_summary': scalars(unified_final), 'final_per_horizon': unified_final['per_horizon'],
            'tradability_final': unified['tradability_diagnostic']['final'],
            'artifact': artifact(Path(unified['artifact']), unified['artifact_sha256']),
            'verdict': unified['verdict'], 'deployment_eligible': unified['deployment_eligible'],
            'caveats': ['Raw native-pip averages across pairs are not dollar or basis-point returns.',
                'Tradability selection ranks native-pip magnitudes across pairs; this can overweight high-pip-scale exotics.',
                'The report records a historical chronological final split, not current prospective postcommit-publication evidence.',
                'Current source v5 and retained fitted artifact v4 require explicit schema compatibility before reuse.'],
        },
        'second_ridge': {
            'feature_count': len(models['feature_names']), 'feature_names': models['feature_names'],
            'horizons_sec': models['horizons_sec'], 'pairs': len(models['models']), 'parameter_surfaces': len(model_rows),
            'pair_specific_surfaces': second['pair_specific_model_count'], 'pooled_proxy_surfaces': second['proxy_model_count'],
            'proxy_pairs': [row['instrument'] for row in second['pairs'] if row.get('proxy_models')],
            'profiles_per_surface': 3, 'historical_profile_gate_passes': second['historical_gate_passes'],
            'fitted_utc': second['fitted_utc'], 'horizon_summary': second['horizon_summary'],
            'multi_horizon_smoothing': second['multi_horizon_smoothing'],
            'numeric_parameter_shape_examples': coeff_shapes[:2],
            'artifact': artifact(D / 'data/oanda_training_manager/state/second_ridge_models_v1.json'),
            'canonical_C_artifact_exists': (C / 'data/oanda_training_manager/state/second_ridge_models_v1.json').is_file(),
            'caveats': ['S5 historical close features and S1 live quote features are separate observation contracts requiring parity checks.',
                'Target matching accepts the first S5 point from nominal target through target+7 seconds.',
                'The 60/20/20 splits purge horizon+7 seconds; alpha, orientation, thresholds and log-horizon smoothing are selected on validation.',
                'Historical lower95 uses row standard error, not horizon/event-block dependence adjustment.',
                'The 87 profile passes reuse holdout for gate decisions and are neither87 independent models nor a fresh prospective pass.',
                'Weighted horizon n includes overlapping rows and repeated fast/balanced/strict profiles; do not sum as unique decisions.'],
        },
        'archived_227': {
            'experiment_id': experiment['experiment_id'], 'model_type': experiment['spec']['model_type'],
            'feature_count': len(experiment['result']['features']), 'fold_count': experiment['result']['fold_count'],
            'retained_mean_auc': experiment['result']['mean_auc'], 'retained_gate': experiment['result']['gate'],
            'timing_audit': timing, 'checkpoint_pointer_in_experiment_manifest': False,
            'artifact_scope': 'Completed fits/forecast evidence exist; this bounded manifest/filename trace did not resolve a uniquely bound reusable227-feature checkpoint.',
        },
        'corrected_220': {
            'feature_scope': '220 safe source features after removal of seven full-history metadata fields',
            'target': 'exact120-minute completed-bar M1 path/endpoint',
            'selected_model': corrected['selected_model'],
            'research_development_before_gate': scalars(corrected['model_research_development_before_viability_gate']),
            'research_diagnostic_before_gate': scalars(corrected['model_research_evaluation_before_viability_gate']),
            'selected_diagnostic_result': scalars(corrected['model_diagnostic_evaluation_result']),
            'signal_checks': corrected['signal_checks'], 'verdict': corrected['verdict'],
            'production_blockers_as_recorded_in_July': corrected_report['production_blockers'],
            'caveat': 'July statement that no later candles existed is dated; this audit does not repeat it as a current availability claim. The historical2026 sample was already inspected.',
        },
        'feature_forecast_v4': {
            'source_features': 220, 'causal_interactions': 12, 'pair_identity_features': 15,
            'candidate_selection_can_reduce_columns': True, 'horizon_minutes': 120,
            'target_bundles': bundles, 'two_stage_policy': index['two_stage_policy'],
            'two_stage_report': two_stage,
            'scope': 'Real saved offline direction and cost-survival models; the profitable-movement target does not predict which side wins.',
        },
        'panel_full_history': panels,
        'source_and_report_bindings': sources,
        'limits': {'maximum_retained_metadata_bytes_per_file': 8 * 1024 * 1024, 'maximum_opaque_artifact_hash_bytes': 64 * 1024 * 1024},
        'runtime_mutations': False, 'broker_calls': False, 'training_runs': 0, 'scoring_runs': 0,
        'databases_opened': 0, 'serialized_model_loads': 0,
    }
    path = WORK / 'EXISTING_CURVE_MODEL_EVIDENCE_20260908.json'
    with path.open('x', encoding='utf8') as stream:
        json.dump(evidence, stream, indent=2)
    print(json.dumps({'path': str(path), 'sha256': sha(path), 'unified_artifact': evidence['unified_795']['artifact'],
                      'feature_bundles': bundles, 'source_report_count': len(sources)}, indent=2))

if __name__ == '__main__':
    main()
