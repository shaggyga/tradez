"""Original weights, shared learned preprocessing and origin-local prediction reuse."""
import json
import numpy as np
import pandas as pd
from threadpoolctl import threadpool_limits
from joint_readiness_predict_v2 import RetainedPredictor,FEATURES
from rich_family_models_v2 import frame,transform_state

class SharedCurvePredictor(RetainedPredictor):
    def __init__(self,reader):
        super().__init__(reader);self.origin=None;self.values={};self.frames={};self.transforms={};self.validated=set();self.predict_calls=0;self.transform_calls=0
    def begin_origin(self,origin):
        if type(origin) is not int:raise ValueError('integer_curve_origin_required')
        self.origin=origin;self.values={};self.frames={};self.transforms={};self.predict_calls=0;self.transform_calls=0
    def predict(self,task,observations,views):
        if self.origin is None or any(o['origin_epoch']!=self.origin for o in observations):raise ValueError('curve_cache_origin_mismatch')
        if task['joint_ready_epoch']>self.origin:raise ValueError('joint_model_not_ready_for_prediction')
        valid=[o for o in observations if o['features'] is not None and o['available_epoch']<=self.origin]
        key=task['fit_id'];group=task['group']
        # Fingerprint inputs before reuse so a caller cannot change a same-origin request.
        from contracts import fingerprint
        input_id=fingerprint([{'observation':o,'view':views[o['record_id']][group] if group!='legacy26' else None} for o in valid])
        if key in self.values:
            previous,result=self.values[key]
            if previous!=input_id:raise ValueError('same_origin_prediction_input_changed')
            return dict(result)
        if key not in self.cache:super().predict(task,[],{})
        meta,models=self.cache[key]
        if group!='legacy26' and key not in self.validated:
            states=[transform_state(model,meta['feature_names']) for model in models.values()]
            if any(s!=meta['fitted_transform'] for s in states):raise ValueError('shared_actual_transform_mismatch')
            params=[tuple((type(step).__name__,step.get_params(deep=False)) for _,step in model.steps[:-1]) for model in models.values()]
            if json.dumps(params[0],sort_keys=True,allow_nan=True)!=json.dumps(params[1],sort_keys=True,allow_nan=True):raise ValueError('shared_transform_configuration_mismatch')
            self.validated.add(key)
        for o in valid:
            if not views[o['record_id']]['full228_cost2']['shared_legacy_population_eligible']:raise ValueError('shared_assessment_support_drift')
            if group!='legacy26' and views[o['record_id']][group]['feature_names']!=meta['feature_names']:raise ValueError('curve_feature_order_mismatch')
        if not valid:self.values[key]=(input_id,{});return {}
        with threadpool_limits(limits=1):
            if group=='legacy26':
                if group not in self.frames:self.frames[group]=(input_id,np.asarray([o['features'] for o in valid],dtype=np.float64))
                elif self.frames[group][0]!=input_id:raise ValueError('same_origin_frame_input_changed')
                x=self.frames[group][1];ridge=meta['ridge']
                values={'ridge':np.column_stack((np.ones(len(valid)),(x-np.asarray(ridge['mean']))/np.asarray(ridge['scale'])))@np.asarray(ridge['coefficient']),'recovered_hgb':models['recovered_hgb'].predict(pd.DataFrame(x,columns=FEATURES))}
            else:
                if group not in self.frames:self.frames[group]=(input_id,frame([views[o['record_id']][group]['values'] for o in valid],meta['feature_names']))
                elif self.frames[group][0]!=input_id:raise ValueError('same_origin_frame_input_changed')
                x=self.frames[group][1];transform_key=(group,meta['fitted_transform']['sha256'])
                if transform_key not in self.transforms:
                    self.transforms[transform_key]=models['ridge'][:-1].transform(x);self.transform_calls+=1
                transformed=self.transforms[transform_key]
                values={m:model.steps[-1][1].predict(transformed) for m,model in models.items()}
        if any(not np.isfinite(v).all() for v in values.values()):raise ValueError('nonfinite_curve_prediction')
        result={(o['record_id'],m):float(value) for m,vals in values.items() for o,value in zip(valid,vals)}
        self.values[key]=(input_id,result);self.predict_calls+=1;return dict(result)
