"""Isolated shape/inference check of the exact retained HGB opportunity bundle.

The legacy pickle import alias is confined to this process. No fit, historical
score, market prediction, source substitution or deployment is performed.
"""
import argparse
from collections import Counter
import hashlib
import io
import json
from pathlib import Path
import sys
import warnings


def check():
    root = Path(__file__).resolve().parent
    sys.path.insert(0, str(root/'src/fresh_m1_intrahour'))
    import joblib
    import numpy as np
    import pandas as pd
    import sklearn
    expected = '70f065144063c9a79fad5cec5bc95574722f36a4a6736199425ad95d702168a5'
    raw = (root/'models/two_hour_cost_survival_offline_research_forecast_bundle.joblib').read_bytes()
    if hashlib.sha256(raw).hexdigest() != expected:
        raise ValueError('audited_artifact_identity_mismatch')
    result = {'scope': 'synthetic_shape_and_inference_only', 'artifact_sha256': expected,
              'versions': {'python': sys.version.split()[0], 'sklearn': sklearn.__version__,
                           'numpy': np.__version__, 'pandas': pd.__version__}, 'components': [],
              'new_fit': False, 'historical_rescore': False, 'direction_signal': False,
              'account_eligible': False, 'can_place_orders': False}
    with warnings.catch_warnings(record=True) as observed:
        warnings.simplefilter('always')
        bundle = joblib.load(io.BytesIO(raw))
        if bundle['target_id'] != 'two_hour_cost_survival' or bundle['target_definition']['kind'] != 'opportunity':
            raise ValueError('original_opportunity_target_required')
        for component in bundle['components']:
            names = component['features']
            if len(names) != len(set(names)):
                raise ValueError('duplicate_original_feature')
            frame = pd.DataFrame(np.zeros((2, len(names))), columns=names)
            frame.iloc[1, ::7] = np.nan
            probabilities = component['estimator'].predict_proba(frame)
            if not (probabilities.shape == (2, 2) and np.isfinite(probabilities).all()
                    and (probabilities >= 0).all() and (probabilities <= 1).all()
                    and np.allclose(probabilities.sum(axis=1), 1)):
                raise ValueError('invalid_synthetic_probabilities')
            result['components'].append({'model_id': component['model_id'], 'input_feature_count': len(names),
                'selected_feature_count': len(component['selected_features']), 'feature_names': names,
                'selected_feature_names': component['selected_features']})
        counts = Counter((type(w.message).__name__, str(w.message)[:400]) for w in observed)
        result['warnings'] = [{'category': category, 'message': message, 'count': count}
                              for (category, message), count in sorted(counts.items())]
    result['status'] = 'artifact_loaded_synthetic_inference_passed'
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, help='Write a new receipt without replacing previous evidence.')
    args = parser.parse_args()
    result = check()
    if args.output:
        with args.output.open('x', encoding='utf-8') as stream:
            json.dump(result, stream, indent=2)
            stream.write('\n')
    print(json.dumps({**{k: v for k, v in result.items() if k != 'components'},
        'components': [{k: v for k, v in c.items() if not k.endswith('names')} for c in result['components']]}, indent=2))


if __name__ == '__main__':
    main()
