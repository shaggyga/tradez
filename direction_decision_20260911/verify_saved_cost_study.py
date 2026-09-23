"""Recreate retained estimates and exercise the four-head consumer in replay."""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import sys
import time
import warnings

import joblib
import numpy as np
import pandas as pd
from threadpoolctl import threadpool_limits

sys.path.insert(0,str(Path(__file__).resolve().parent/'src'))
import signed_cost_models_v1 as model
import cost_curve_contract_v1 as curve_contract


def digest(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def object_digest(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()


def verify(evaluation,output):
    target=Path(output)
    # Refuse both output collisions before loading models or creating partial proof.
    for path in (target,target.with_name('FOUR_HEAD_REPLAY_EXAMPLE.json')):
        if path.exists():raise FileExistsError('recreation_output_exists:'+str(path))
    target.parent.mkdir(parents=True,exist_ok=True)
    root=Path(evaluation);result=json.loads((root/'RESULTS.json').read_text())
    spec=json.loads((root/'FROZEN_RUN.json').read_text())['spec']
    for name,key in (('input_panel.parquet','input_panel_sha256'),('predictions.parquet','predictions_sha256')):
        if digest(root/name)!=result[key]:raise ValueError('retained_file_hash_changed')
    for name,expected in result['source_bindings'].items():
        if digest(Path(__file__).parent/'src'/name)!=expected:raise ValueError('numerical_source_changed')
    inputs=pd.read_parquet(root/'input_panel.parquet');predictions=pd.read_parquet(root/'predictions.parquet')
    keys=['instrument','epoch','horizon_minutes']
    if inputs.duplicated(keys).any() or predictions.duplicated(keys).any():raise ValueError('duplicate_key')
    joined=predictions[keys].merge(inputs,on=keys,how='left',validate='one_to_one')
    for field in ('return_bps','long_net_bps','short_net_bps','label_available_epoch'):
        if not np.array_equal(joined[field].to_numpy(),predictions[field].to_numpy(),equal_nan=True):raise ValueError('label_or_row_mismatch')
    model_bundles={};verified=[];warnings_count=Counter();value_count=0
    with threadpool_limits(limits=1):
        for h in result['horizons']:
            mask=joined.horizon_minutes==h['horizon_minutes'];frame=joined.loc[mask];saved=predictions.loc[mask]
            for group,meta in h['artifacts'].items():
                path=root/meta['file']
                if digest(path)!=meta['sha256']:raise ValueError('artifact_hash_mismatch')
                with warnings.catch_warnings(record=True) as caught:
                    warnings.simplefilter('always');bundle=joblib.load(path)
                warnings_count.update(type(w.message).__name__+': '+str(w.message) for w in caught)
                if bundle['can_place_orders'] is not False or bundle['can_promote'] is not False:raise ValueError('artifact_authority')
                if bundle['calibration_label_max_epoch']>=frame.epoch.min()+60:raise ValueError('calibration_lookahead')
                values,_=model.predict_bundle(bundle,frame)
                maximum=0.
                for name,value in values.items():
                    retained=saved[group+'__'+name].to_numpy()
                    maximum=max(maximum,float(np.max(np.abs(value-retained))))
                    if not np.array_equal(value,retained):raise ValueError('prediction_recreation_mismatch')
                    value_count+=len(value)
                for method in ('direct_signed_mean','mixture_raw','mixture_calibrated'):
                    side,long,short=model.decide(values[method],frame.known_entry_long_cost_bps,frame.known_entry_short_cost_bps,
                        values['long_exit'],values['short_exit'],spec['entry_margin_bps'])
                    arm=group+'__'+method
                    if not np.array_equal(side,saved['side__'+arm]):raise ValueError('decision_side_mismatch')
                    if not np.array_equal(long,saved['long__'+arm]) or not np.array_equal(short,saved['short__'+arm]):raise ValueError('decision_economics_mismatch')
                verified.append({'artifact':meta['file'],'rows':len(frame),'maximum_absolute_difference':maximum,'sha256':meta['sha256']})
                model_bundles[(group,h['horizon_minutes'])]=(bundle,meta['sha256'])
    # First available common four-horizon timestamp per pair, selected by clocks only.
    common=predictions.groupby(['instrument','epoch']).horizon_minutes.nunique()
    chosen=common[common==4].reset_index().groupby('instrument',sort=True).first().reset_index()
    replay=[];example=None
    for _,key in chosen.iterrows():
        pair=key['instrument'];t=int(key['epoch']);reference=t+60
        selected=joined[(joined.instrument==pair)&(joined.epoch==t)].sort_values('horizon_minutes')
        retained=predictions[(predictions.instrument==pair)&(predictions.epoch==t)].sort_values('horizon_minutes')
        first=selected.iloc[0];quote={'pair':pair,'mid':float(first.mid),'bid':float(first.bid),'ask':float(first.ask),
            'observed_epoch':reference,'price_epoch':reference,'source_sha256':digest(root/'input_panel.parquet'),
            'price_kind':'retained_candle_close_proxy'}
        for group in result['groups']:
            for method in ('direct_signed_mean','mixture_raw','mixture_calibrated'):
                heads=[]
                for _,row in retained.iterrows():
                    heads.append({'horizon_minutes':int(row.horizon_minutes),'expected_return_bps':float(row[group+'__'+method]),
                        'expected_long_exit_cost_bps':float(row[group+'__long_exit']),
                        'expected_short_exit_cost_bps':float(row[group+'__short_exit']),
                        'probability_up':float(row[group+'__probability_positive_'+('raw' if method=='mixture_raw' else 'calibrated')])})
                bundles=[model_bundles[(group,h)] for h in spec['horizons_minutes']]
                envelope={'pair':pair,'reference_epoch':reference,'reference_mid':float(first.mid),
                    'available_epoch':reference,'feature_available_epoch':reference,
                    'trained_label_max_epoch':max(b[0]['trained_label_max_epoch'] for b in bundles),
                    'calibration_label_max_epoch':max(b[0]['calibration_label_max_epoch'] for b in bundles),
                    'model_sha256':object_digest([b[1] for b in bundles]),
                    'feature_sha256':object_digest({'input_panel_sha256':result['input_panel_sha256'],'pair':pair,'epoch':t}),
                    'heads':heads}
                assessment=curve_contract.assess_curve(envelope,quote,decision_epoch=reference)
                curve_contract.validate_assessment(assessment,envelope,quote,decision_epoch=reference)
                for head in assessment['heads']:
                    horizon=head['horizon_minutes'];row=retained[retained.horizon_minutes==horizon].iloc[0]
                    expected=int(row['side__'+group+'__'+method])
                    if head['side']!=expected:raise ValueError('four_head_adapter_policy_mismatch')
                replay.append({'pair':pair,'group':group,'mean_method':method,'reference_epoch':reference,
                    'heads':4,'assessment_sha256':assessment['assessment_sha256']})
                if example is None and pair=='EUR_USD' and group=='combined' and method=='mixture_calibrated':
                    example={'scope':'historical_replay_clock_only_not_an_actual_issued_forecast',
                        'clock_warning':'Model artifacts were fitted after this historical reference; reference/availability clocks here are simulation inputs only.',
                        'curve':envelope,'quote':quote,'assessment':assessment}
    if example is None:raise ValueError('expected_replay_pair_missing')
    try:
        curve_contract.assess_curve(example['curve'],example['quote'],decision_epoch=time.time())
    except ValueError as exc:stale_reason=str(exc)
    else:raise ValueError('historical_curve_accepted_now')
    receipt={'verified':True,'scope':'retained_model_recreation_and_explicit_historical_consumer_replay',
        'model_bundles_recreated':len(verified),'numeric_values_recreated':value_count,
        'maximum_absolute_difference':max(v['maximum_absolute_difference'] for v in verified),
        'replay_curves_checked':len(replay),'replay_heads_checked':4*len(replay),
        'historical_curve_refused_at_current_time':True,'historical_refusal_reason':stale_reason,
        'model_checks':verified,'replay_checks':replay,'warnings':dict(warnings_count),
        'results_sha256':digest(root/'RESULTS.json'),'curve_contract_sha256':digest(Path(__file__).parent/'src/cost_curve_contract_v1.py'),
        'can_place_orders':False,'can_promote':False,'actual_forecasts_published':0}
    with target.open('x',encoding='utf-8') as h:json.dump(receipt,h,indent=2,sort_keys=True,allow_nan=False)
    with target.with_name('FOUR_HEAD_REPLAY_EXAMPLE.json').open('x',encoding='utf-8') as h:json.dump(example,h,indent=2,sort_keys=True,allow_nan=False)
    print(json.dumps({k:receipt[k] for k in ('verified','model_bundles_recreated','numeric_values_recreated',
        'replay_curves_checked','replay_heads_checked','historical_curve_refused_at_current_time')}),flush=True)


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--evaluation',required=True);parser.add_argument('--output',required=True)
    args=parser.parse_args();verify(args.evaluation,args.output)
