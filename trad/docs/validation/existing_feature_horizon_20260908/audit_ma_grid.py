"""Read retained MA-grid metadata and hash artifacts; never load fitted objects."""
from collections import Counter
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
import importlib.util
import json

BASE = Path(__file__).resolve().parent
C = BASE.parent / 'trad'
D = Path('D:/forex/trad')

def digest(path):
    h = sha256()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()

def source(path):
    return {'path': str(path), 'exists': path.is_file(),
            'bytes': path.stat().st_size if path.is_file() else None,
            'sha256': digest(path) if path.is_file() else None}

started = datetime.now(timezone.utc).isoformat()
report_path = D / 'data/oanda_training_manager/reports/ma_feature_grid/ma_feature_grid_latest.json'
report = json.loads(report_path.read_text(encoding='utf-8-sig'))
artifact_path = Path(report['artifact'])
artifact = source(artifact_path)
assert artifact['sha256'] == report['artifact_sha256']
assert report['planned_cell_count'] == 702 and report['fitted_cell_count'] == 610
assert report['unsupported_cell_count'] == 92

# This local module was inspected before import. Only its deterministic feature-
# name builder is called; no artifact deserialization or prediction is performed.
spec = importlib.util.spec_from_file_location('audited_ma_feature_names', C / 'oanda_ma_feature_grid.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
actual_names = {frame: list(module.ma_feature_names(frame)) for frame in report['timeframes']}
actual_counts = {frame: len(names) for frame, names in actual_names.items()}
assert actual_counts == report['contract']['feature_count_by_timeframe']
assert all(len(names) == len(set(names)) for names in actual_names.values())

validation_cells, holdout_cells, both = [], [], []
frame_rows = []
for row in report['timeframe_reports']:
    frame = row['timeframe']
    valid = set(row['validation_pass_horizons'])
    holdout = set(row['holdout_pass_horizons'])
    validation_cells.extend((frame, horizon) for horizon in sorted(valid))
    holdout_cells.extend((frame, horizon) for horizon in sorted(holdout))
    both.extend((frame, horizon) for horizon in sorted(valid & holdout))
    frame_rows.append({key: row.get(key) for key in (
        'timeframe', 'feature_count', 'status', 'sampled_rows', 'source_rows_read',
        'split_rows', 'validation_pass_horizons', 'holdout_pass_horizons',
        'unsupported_horizons_sec', 'three_split_execution_authorized')})

metric_keys = ('timeframe', 'horizon_sec', 'split', 'status', 'n', 'pair_count',
               'direction_accuracy', 'balanced_accuracy', 'predicted_up_fraction',
               'brier_score', 'signed_pip_mae', 'signed_pip_r2', 'magnitude_pip_mae',
               'executable_average_net_pips', 'executable_win_rate',
               'positive_pair_fraction', 'exact_cost_fraction')
selected_metrics = [{key: row.get(key) for key in metric_keys} for row in report['grid']
                    if row.get('timeframe') == 'M1' and row.get('horizon_sec') in (60, 300, 600, 900, 1800, 3600)
                    and row.get('split') in ('validation', 'holdout')]
source_comparison = []
for name in ('oanda_ma_feature_grid.py', 'oanda_ma_feature_grid_fit.py'):
    c, d = source(C / name), source(D / name)
    source_comparison.append({'name': name, 'canonical_c': c, 'historical_d': d,
                              'equal': c['sha256'] == d['sha256']})

payload = {
    'schema_version': 'ma_grid_existing_curve_audit_v1_20260908',
    'started_utc': started, 'finished_utc': datetime.now(timezone.utc).isoformat(),
    'status': 'completed_readonly_metadata_and_source_audit',
    'report': source(report_path), 'reported_generated_at': report['generated_at'],
    'artifact': artifact, 'artifact_matches_retained_report': True,
    'canonical_c_artifact': source(C / 'data/oanda_training_manager/models/ma_feature_grid/ma_feature_grid_latest.joblib'),
    'source_comparison': source_comparison,
    'actual_feature_name_counts_from_current_pure_builder': actual_counts,
    'feature_names_by_timeframe': actual_names,
    'feature_count_match_to_report': True,
    'timeframes': report['timeframes'], 'horizons_sec': report['horizons_sec'],
    'instrument_count': report['instrument_count'],
    'planned_cells': 702, 'fitted_cells': 610, 'unsupported_cells': 92,
    'reported_validation_pass_cells': validation_cells,
    'reported_holdout_pass_cells': holdout_cells,
    'same_cells_passing_both': both,
    'fit_configuration': report['fit'],
    'feature_exclusions': report['contract']['feature_exclusions'],
    'cost_policy': report['contract']['cost_policy'],
    'targets': report['contract']['targets'],
    'timeframe_summary': frame_rows,
    'm1_selected_validation_and_holdout_metrics': selected_metrics,
    'grid_status_split_counts': dict(Counter((str(row.get('status')) + ':' + str(row.get('split'))) for row in report['grid'])),
    'interpretation': 'A real trained multi-horizon technical model exists, with separate direction, signed-pip and magnitude heads. It does not consume news. Its saved results do not qualify it for account execution and are not current prospective evidence.',
    'limits': [
        'Artifact bytes and report hash binding were checked without deserializing the joblib object.',
        'Metrics are retained historical results, not re-scored predictions; no raw outcome replay was performed.',
        'Per-pair chronological split fractions are recorded. This report does not record a purged split policy, although newer fitter source supports purged alternatives; the saved run is not certified purged by this audit.',
        'Validation calibration and evaluation share the validation segment; the holdout segment must be kept separate.',
        'Native pips across different currencies/horizons are not account-dollar P/L and are not aggregated here.',
        'No same timeframe/horizon cell passes both retained validation and holdout gates. This limits execution eligibility, not the existence of research predictions.',
    ],
    'pure_function_calls': ['ma_feature_names for 27 declared timeframes'],
    'model_objects_loaded': 0, 'model_predictions': 0, 'training_runs': 0,
    'broker_calls': 0, 'runtime_writes': False,
}
target = BASE / 'MA_GRID_EXISTING_CURVE_AUDIT_20260908.json'
with target.open('x', encoding='utf-8') as handle:
    json.dump(payload, handle, indent=2, allow_nan=False)
    handle.write('\n')
print(json.dumps({'path': str(target), 'sha256': digest(target), 'artifact_hash_match': True,
                  'm1_features': actual_counts['M1'], 'validation_pass_cells': validation_cells,
                  'holdout_pass_cells': holdout_cells, 'same_cells_passing_both': both,
                  'source_equality': {row['name']: row['equal'] for row in source_comparison},
                  'm1_holdout': [row for row in selected_metrics if row['split'] == 'holdout']}))
