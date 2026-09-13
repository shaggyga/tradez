"""Read bounded saved model metadata; never import or deserialize project models."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

ROOT = Path(r'C:\Users\zmoor\Documents\forex\trad')
DROOT = Path(r'D:\forex\trad')
OUT = Path(__file__).resolve().parent


def load(path):
    path = Path(path)
    if not path.is_file():
        return None
    if path.stat().st_size > 12_000_000:
        raise ValueError('metadata exceeds bounded read: ' + str(path))
    return json.loads(path.read_text(encoding='utf-8-sig'))


def evidence(path, expected=None):
    path = Path(path)
    result = dict(path=str(path), present=path.is_file())
    if result['present']:
        result['bytes'] = path.stat().st_size
        if result['bytes'] <= 12_000_000:
            result['sha256'] = hashlib.sha256(path.read_bytes()).hexdigest()
            if expected:
                result['matches_recorded_sha256'] = result['sha256'] == expected
        else:
            result['sha256_not_reverified_reason'] = 'bounded metadata audit; no bulk checkpoint reads'
    if expected:
        result['recorded_sha256'] = expected
    return result


def historical_path(text):
    path = Path(text)
    if path.is_absolute():
        return path
    return (DROOT.parent if path.parts[0].lower() == 'trad' else DROOT) / path


def artifacts(value):
    found = {}
    def visit(node):
        if isinstance(node, dict):
            for key, item in node.items():
                if isinstance(item, str) and Path(item).suffix.lower() in {'.joblib', '.ckpt', '.pth', '.pt', '.safetensors', '.pkl'}:
                    path = historical_path(item)
                    found[str(path)] = evidence(path, node.get('sha256') if key == 'path' else None)
                elif isinstance(item, (list, dict)):
                    visit(item)
        elif isinstance(node, list):
            for item in node:
                visit(item)
    visit(value)
    return list(found.values())


completion_path = ROOT / 'data/oanda_training_manager/model_space/model_gap_completion_latest.json'
completion = load(completion_path)
source_evidence = {key: evidence(row['path'], row.get('sha256')) for key,row in completion['sources'].items()}
reports = []
for name in ('shared_panel_model_benchmark_latest.json','shared_panel_neural_benchmark_latest.json',
             'shared_panel_neural_long_horizon_benchmark_latest.json','remaining_market_validation_latest.json',
             'foundation_market_benchmark_latest.json','unified_model_gap_market_latest.json'):
    path = DROOT / 'data/oanda_training_manager/reports/modern_model_gap' / name
    report = load(path)
    if report is None:
        reports.append(evidence(path))
        continue
    reports.append({**evidence(path), 'generated_utc':report.get('generated_utc'),
                    'summary':report.get('summary'), 'model_ids':[r.get('model') for r in report.get('results',[])],
                    'artifact_file_evidence':artifacts(report)})

weight_meta = load(ROOT / 'data/oanda_training_manager/model_space/foundation_weight_sync_latest.json')
weights = []
for row in weight_meta['records']:
    manifest = load(row['manifest'])
    weights.append({'model':row['model'], 'recorded_status':row['status'],
                    'destination':row['destination'], 'destination_present':Path(row['destination']).is_dir(),
                    'manifest':evidence(row['manifest']),
                    'recorded_total_bytes':row.get('total_bytes'),
                    'recorded_file_count':row.get('file_count'),
                    'recorded_revision':row.get('resolved_revision'),
                    'manifest_keys':sorted(manifest) if isinstance(manifest,dict) else None,
                    'checkpoint_content_hashes_reverified':False})

profiles = load(ROOT / 'config/model_gap_runtime_profiles_recovery_c.json')
runtime_evidence = {key:{'python':row['python'],'python_present':Path(row['python']).is_file(),
                         'package_imports_retested':False,'models':row['models']}
                    for key,row in profiles['profiles'].items()}
bindings = ['config/modern_model_gap_roadmap.json','MODEL_FEATURE_SPACE.md',
            'docs/BACKTEST_AND_MODEL_REGISTRY.md','oanda_model_gap_registry.py',
            'oanda_model_gap_completion.py','oanda_model_gap_full_matrix.py',
            'oanda_model_gap_unified_report.py','oanda_proof_shadow_predictors.py',
            'oanda_cross_pair_graph_adapter.py','oanda_model_gap_market_validation.py',
            'oanda_currency_lead_lag_shadow.py','oanda_model_space_agenda.py',
            'oanda_strategy_lab_ensemble_replay.py','oanda_ensemble_candidate_backtester.py',
            'src/forex_system/features/currency_state_engine.py',
            'config/shadow_runtime_retirements_v1.json',
            'config/causal_forecast_study_v1_io_r2_20260906.json',
            'data/oanda_training_manager/reports/unique_predictor_watch_20260804/currency_lead_lag_network_v1.json']
report = {
    'schema_version':1, 'generated_utc':datetime.now(timezone.utc).isoformat(),
    'scope':'Bounded read-only source and saved metadata inventory; no model imports, checkpoint deserialization, network, fitting or live database access.',
    'completion_summary_source':evidence(completion_path),
    'historical_completion_summary':completion['summary'],
    'historical_models':[{key:row.get(key) for key in ('model','family','evidence_level','market_backtest_complete',
                         'market_performance_passed','production_eligible','account_wired','runtime_blocked',
                         'runtime_reason','runtime_status','weight_status')} for row in completion['models']],
    'historical_source_evidence_now':source_evidence,
    'historical_reports_now':reports, 'foundation_weights_now':weights,
    'configured_runtime_files_now':runtime_evidence,
    'runtime_blockers_as_recorded':profiles['explicit_blockers'],
    'current_c_project_model_directory_file_count':len(list((ROOT/'data/oanda_training_manager/models').glob('*'))),
    'c_project_modern_model_gap_reports_present':(ROOT/'data/oanda_training_manager/reports/modern_model_gap/unified_model_gap_market_latest.json').is_file(),
    'c_project_full_matrix_report_present':(ROOT/'data/oanda_training_manager/training_sets/model_gap_full_matrix/full_matrix_latest.json').is_file(),
    'current_study_heartbeat_observation':load(ROOT/'data/oanda_training_manager/causal_forecast_study_v1_io_r2/heartbeat.json'),
    'source_bindings':[evidence(ROOT/name) for name in bindings],
    'limitations':['Historical summary completion is not current runtime qualification or trading evidence.',
                   'Large checkpoint content hashes, current package imports and training reproducibility were not retested.',
                   'The model list is not a count of independent forecasts; four current research families share price inputs.',
                   'No new performance experiment or claim of profitable alpha is made.'],
}
destination = OUT/'model_inventory_evidence.json'
with destination.open('x',encoding='utf-8') as handle:
    json.dump(report,handle,indent=2,sort_keys=True,allow_nan=False)
    handle.write('\n')
print(json.dumps({'output':str(destination),'bytes':destination.stat().st_size,
                  'reports':len(reports),'artifact_records':sum(len(r.get('artifact_file_evidence',[])) for r in reports),
                  'source_hash_matches':{key:row.get('matches_recorded_sha256') for key,row in source_evidence.items()}},sort_keys=True))
