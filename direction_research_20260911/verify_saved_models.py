"""Read-only recreation check of retained research models and forecast rows."""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import warnings

import joblib
import numpy as np
import pandas as pd
from threadpoolctl import threadpool_limits


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def verify(prepared, evaluated, output):
    prepared, evaluated = Path(prepared), Path(evaluated)
    receipts=json.loads((prepared/'PREPARED.json').read_text())
    result=json.loads((evaluated/'RESULTS.json').read_text())
    if result['input_manifest_sha256'] != digest(prepared/'PREPARED.json'):
        raise ValueError('prepared_manifest_mismatch')
    for name,meta in receipts['files'].items():
        if digest(prepared/name) != meta['sha256']:
            raise ValueError('prepared_file_mismatch')
    panel=pd.read_parquet(prepared/'panel.parquet')
    predictions=pd.read_parquet(evaluated/'predictions.parquet')
    keys=['instrument','epoch','horizon_minutes']
    if panel.duplicated(keys).any() or predictions.duplicated(keys).any():
        raise ValueError('duplicate_forecast_key')
    checked=predictions[keys+['fold']].merge(panel,on=keys,how='left',validate='one_to_one')
    if checked.return_bps.isna().any():
        raise ValueError('forecast_source_row_missing')
    for column in ('return_bps','long_net_bps','short_net_bps','label_available_epoch','news_available'):
        if not np.array_equal(predictions[column].to_numpy(),checked[column].to_numpy(),equal_nan=True):
            raise ValueError('retained_forecast_label_changed:'+column)
    identities=pd.get_dummies(pd.Categorical(checked.instrument,
        categories=receipts['source_manifest']['pairs']),prefix='pair',dtype=float)
    checked=pd.concat([checked.reset_index(drop=True),identities.reset_index(drop=True)],axis=1)
    models=[]; warning_counts=Counter()
    with threadpool_limits(limits=1):
        for fold in result['folds']:
            if fold['status']!='completed':
                continue
            mask=(checked.fold==fold['fold']) & (checked.horizon_minutes==fold['horizon_minutes'])
            frame=checked.loc[mask]
            saved=predictions.loc[mask]
            if len(frame)!=fold['validation_rows']:
                raise ValueError('validation_denominator_mismatch')
            for arm,meta in fold['models'].items():
                path=evaluated/meta['artifact']
                if digest(path)!=meta['sha256']:
                    raise ValueError('model_artifact_mismatch')
                with warnings.catch_warnings(record=True) as caught:
                    warnings.simplefilter('always')
                    bundle=joblib.load(path)
                warning_counts.update(type(w.message).__name__+': '+str(w.message) for w in caught)
                if bundle['can_place_orders'] is not False or bundle['can_promote'] is not False:
                    raise ValueError('artifact_authority_mismatch')
                if bundle['trained_label_maturity_max']>=bundle['validation_start']:
                    raise ValueError('model_training_future_label')
                expected=saved['p__'+arm].to_numpy()
                actual=bundle['pipeline'].predict_proba(frame[bundle['columns']])[:,1]
                maximum=float(np.max(np.abs(actual-expected)))
                if not np.allclose(actual,expected,rtol=0,atol=1e-12):
                    raise ValueError('saved_forecast_not_recreated')
                models.append({'artifact':meta['artifact'],'forecast_rows':len(frame),
                    'maximum_absolute_probability_difference':maximum,'sha256':meta['sha256']})
    receipt={'verified':True,'scope':'same_saved_artifacts_same_retained_feature_rows_no_refit',
        'prepared_sha256':digest(prepared/'PREPARED.json'),'results_sha256':digest(evaluated/'RESULTS.json'),
        'predictions_sha256':digest(evaluated/'predictions.parquet'),
        'models_verified':len(models),'model_prediction_values_verified':sum(m['forecast_rows'] for m in models),
        'maximum_probability_difference':max(m['maximum_absolute_probability_difference'] for m in models),
        'models':models,'warnings':dict(warning_counts),'can_place_orders':False,'can_promote':False}
    with Path(output).open('x',encoding='utf-8') as handle:
        json.dump(receipt,handle,indent=2,sort_keys=True,allow_nan=False)
    print(json.dumps({k:receipt[k] for k in ('verified','models_verified',
        'model_prediction_values_verified','maximum_probability_difference')}),flush=True)


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--prepared',required=True);parser.add_argument('--evaluated',required=True)
    parser.add_argument('--output',required=True)
    args=parser.parse_args()
    verify(args.prepared,args.evaluated,args.output)
