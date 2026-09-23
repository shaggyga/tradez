"""Isolated, hash-bound inspection/synthetic inference of the existing bundle.

Never fit or score historical data. The legacy import path is local to this
inspection process and is not installed into the active project's namespace.
"""
import hashlib
import io
import json
from pathlib import Path
import sys
import warnings

root = Path(__file__).resolve().parent
sys.path.insert(0, str(root / 'src' / 'fresh_m1_intrahour'))
import joblib
import numpy as np
import pandas as pd
import sklearn

path = root / 'models/two_hour_cost_survival_offline_research_forecast_bundle.joblib'
expected = '70f065144063c9a79fad5cec5bc95574722f36a4a6736199425ad95d702168a5'
raw = path.read_bytes()
if hashlib.sha256(raw).hexdigest() != expected:
    raise SystemExit('Audited bundle identity mismatch.')
result = {'schema_version': 'existing_opportunity_bundle_inspection_v1_20260911',
          'artifact_sha256': expected, 'artifact_bytes': len(raw),
          'scope': 'Exact retained artifact, synthetic shape/inference check only; no retraining, historical rescore or market forecast.',
          'versions': {'python': sys.version.split()[0], 'sklearn': sklearn.__version__,
                       'numpy': np.__version__, 'pandas': pd.__version__},
          'account_eligible': False, 'can_place_orders': False, 'components': []}
with warnings.catch_warnings(record=True) as observed:
    warnings.simplefilter('always')
    try:
        bundle = joblib.load(io.BytesIO(raw))
        if bundle['target_id'] != 'two_hour_cost_survival' or bundle['target_definition']['kind'] != 'opportunity':
            raise ValueError('Existing bundle target differs from the declared role.')
        result.update(candidate_id=bundle['candidate_id'], target_id=bundle['target_id'],
            target_definition=bundle['target_definition'], training_rows=bundle['training_rows'],
            training_decision_start=str(bundle['training_decision_start']),
            training_decision_end=str(bundle['training_decision_end']),
            training_label_end=str(bundle['training_label_end']),
            trained_at_utc=str(bundle['trained_at_utc']), development_gate=bundle['development_gate'])
        for component in bundle['components']:
            names = component['features']
            frame = pd.DataFrame(np.zeros((2, len(names))), columns=names)
            frame.iloc[1, ::7] = np.nan
            probabilities = component['estimator'].predict_proba(frame)
            valid = (probabilities.shape == (2, 2) and np.isfinite(probabilities).all()
                     and (probabilities >= 0).all() and (probabilities <= 1).all()
                     and np.allclose(probabilities.sum(axis=1), 1))
            if not valid:
                raise ValueError('Synthetic inference did not return finite binary probabilities.')
            result['components'].append({'model_id': component['model_id'], 'spec': component['spec'],
                'feature_names': names, 'selected_feature_names': component['selected_features'],
                'input_feature_count': len(names), 'selected_feature_count': len(component['selected_features']),
                'estimator_class': type(component['estimator']).__name__, 'synthetic_inference_valid': True})
        result['status'] = 'artifact_loaded_synthetic_inference_passed'
    except Exception as error:
        result.update(status='compatibility_unresolved', error_type=type(error).__name__,
                      error_message=str(error)[:300])
    result['warnings'] = [{'category': type(w.message).__name__, 'message': str(w.message)[:400]}
                          for w in observed]
dest = root / 'OPPORTUNITY_BUNDLE_COMPATIBILITY_20260911.json'
dest.write_text(json.dumps(result, indent=2, default=str) + '\n', encoding='utf-8')
print(json.dumps({k: result[k] for k in ('status', 'versions', 'warnings')}))
print(json.dumps({'record': str(dest), 'sha256': hashlib.sha256(dest.read_bytes()).hexdigest(),
                  'components': [{k: c[k] for k in ('model_id', 'input_feature_count', 'selected_feature_count',
                                                    'synthetic_inference_valid')} for c in result['components']]}))
