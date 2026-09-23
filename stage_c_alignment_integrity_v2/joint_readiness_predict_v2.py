"""Reproduce selected original predictions with cached retained weights; never fit."""
import hashlib,io
import joblib
import numpy as np
import pandas as pd
from threadpoolctl import threadpool_limits
from matched_campaign_models_v2 import validate_fit as validate_base,FEATURES
from rich_family_models_v2 import validate_fit as validate_rich,frame

class RetainedPredictor:
    def __init__(self,reader):self.reader=reader;self.cache={}
    def blob(self,alias,name):
        root=self.reader.paths[alias];expected=self.reader.dependencies[alias]['payloads'][name]
        with (root/name).open('rb') as f:raw=f.read(64*1024*1024+1)
        if len(raw)>64*1024*1024 or hashlib.sha256(raw).hexdigest()!=expected:raise ValueError('scheduled_model_consumed_bytes_changed')
        return raw
    def predict(self,task,observations,views):
        group=task['group'];h=task['horizon_minutes'];cutoff=task['fit_cutoff'];key=task['fit_id']
        if key not in self.cache:
            if group=='legacy26':
                name=f'fit_{h}_{cutoff}';meta=self.reader.read('baseline',name+'.json');raw=self.blob('baseline',name+'.joblib');validate_base(meta,raw)
                models={'recovered_hgb':joblib.load(io.BytesIO(raw))}
            else:
                name=f'fit_{group}_{h}_{cutoff}';meta=self.reader.read('family',name+'.json')
                blobs={m:self.blob('family',name+'_'+m+'.joblib') for m in ('ridge','recovered_hgb')};validate_rich(meta,blobs)
                models={m:joblib.load(io.BytesIO(raw)) for m,raw in blobs.items()}
            if meta['fit_id']!=key or meta['fit_cutoff']!=cutoff:raise ValueError('scheduled_model_identity_mismatch')
            self.cache[key]=(meta,models)
        meta,models=self.cache[key]
        valid=[]
        for o in observations:
            if task['joint_ready_epoch']>o['origin_epoch']:raise ValueError('joint_model_not_ready_for_prediction')
            if o['features'] is None or o['available_epoch']>o['origin_epoch']:continue
            if not views[o['record_id']]['full228_cost2']['shared_legacy_population_eligible']:raise ValueError('shared_assessment_support_drift')
            valid.append(o)
        if not valid:return {}
        with threadpool_limits(limits=1):
            if group=='legacy26':
                x=np.asarray([o['features'] for o in valid],dtype=np.float64);ridge=meta['ridge']
                values={'ridge':np.column_stack((np.ones(len(valid)),(x-np.asarray(ridge['mean']))/np.asarray(ridge['scale'])))@np.asarray(ridge['coefficient']),
                    'recovered_hgb':models['recovered_hgb'].predict(pd.DataFrame(x,columns=FEATURES))}
            else:
                x=frame([views[o['record_id']][group]['values'] for o in valid],meta['feature_names'])
                values={m:model.predict(x) for m,model in models.items()}
        if any(not np.isfinite(v).all() for v in values.values()):raise ValueError('nonfinite_scheduled_prediction')
        return {(o['record_id'],m):float(value) for m,vals in values.items() for o,value in zip(valid,vals)}
