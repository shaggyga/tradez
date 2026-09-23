"""Synthetic all-68 fixture: actual ridge fits, elapsed 24h/2d/5d targets."""
import json
from pathlib import Path


def fixture():
    universe=json.loads((Path(__file__).resolve().parent/'FITTED_FIXTURE_UNIVERSE.json').read_text())['instruments']
    start=1700000000;day=86400
    targets=[{'target_id':f'synthetic_midpoint_return_elapsed_{n}d','horizon_seconds':n*day} for n in (1,2,5)]
    observations=[];outcomes=[]
    for d in range(20):
        for j,pair in enumerate(universe):
            epoch=start+d*day;key=f'{pair}:{epoch}'
            x=[d/10,(j-34)/34,((d+j)%7)/7]
            observations.append({'record_id':key,'instrument':pair,'origin_epoch':epoch,'available_epoch':epoch,
                'features':None if d==12 and j==67 else x})
            for target in targets:
                end=epoch+target['horizon_seconds']
                outcomes.append({'record_id':key,'target_id':target['target_id'],'label_end_epoch':end,'available_epoch':end+60,
                    'value':1.5*x[0]-.3*x[1]+.2*x[2]+target['horizon_seconds']/day/10})
    return {'schema_version':'forex_fitted_fixture.v2','universe':universe,'observations':observations,'outcomes':outcomes,
        'contract':{'mode':'offline_synthetic_qualification','broker_access':False,'training_origin_start':start,
            'fit_cutoffs':[start+d*day for d in (10,12,14)],'decision_epochs':[start+d*day for d in range(10,17)],
            'fit_latency_seconds':60,'prediction_latency_seconds':2,'evaluation_window_seconds':day,
            'ridge_lambda':20.0,'min_training_rows':100,'targets':targets}}
