"""Fixed-cost, chronological research on a preserved, already inspected cohort."""
from __future__ import annotations
import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
from pathlib import Path
import warnings

import joblib
import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score
from threadpoolctl import threadpool_limits

import signed_cost_models_v1 as model


def digest(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def encoded(value):return json.dumps(value,indent=2,sort_keys=True,allow_nan=False)+'\n'
def epoch(value):return int(pd.Timestamp(value).timestamp())


def load_inputs(predecessor):
    root=Path(predecessor).resolve();prepared=json.loads((root/'prepared_001/PREPARED.json').read_text())
    for name,meta in prepared['files'].items():
        if digest(root/'prepared_001'/name)!=meta['sha256']:raise ValueError('prepared_source_changed')
    module_path=root/'source_release/src/direction_data_v1.py'
    if digest(module_path)!=prepared['source_bindings']['direction_data_v1.py']:raise ValueError('source_loader_changed')
    definition=importlib.util.spec_from_file_location('_retained_direction_data',module_path)
    loader=importlib.util.module_from_spec(definition);definition.loader.exec_module(loader)
    data=loader.load_dataset(root/'snapshot_001')
    if digest(root/'snapshot_001/manifest.json')!=prepared['snapshot_manifest_sha256']:
        raise ValueError('source_snapshot_changed')
    prices=pd.concat([p.assign(instrument=pair) for pair,p in data['prices'].items()],ignore_index=True)
    panel=model.exit_targets(pd.read_parquet(root/'prepared_001/panel.parquet'),prices)
    identities=pd.get_dummies(pd.Categorical(panel.instrument,categories=data['manifest']['pairs']),
                             prefix='pair',dtype=float)
    panel=pd.concat([panel.reset_index(drop=True),identities.reset_index(drop=True)],axis=1)
    groups={name:prepared['feature_groups'][name]+['known_entry_long_cost_bps','known_entry_short_cost_bps']+list(identities.columns)
            for name in ('technical','combined')}
    return panel,groups,prepared


def split(frame,spec):
    decision=frame.epoch+60;maturity=frame.label_available_epoch
    a,b,c,d=(epoch(spec[k]) for k in ('training_start','calibration_start','assessment_start','assessment_end'))
    train=frame[(decision>=a)&(decision<b)&(maturity<b)].copy()
    calibration=frame[(decision>=b)&(decision<c)&(maturity<c)].copy()
    assessment=frame[(decision>=c)&(decision<d)&(maturity<d)].copy()
    return train,calibration,assessment


def probability_metrics(r,p):
    r=np.asarray(r,dtype=float);p=np.asarray(p,dtype=float)
    y=(r>0).astype(int);q=np.clip(p,1e-12,1-1e-12);nonzero=r!=0
    side=np.sign(p-.5);predicted=nonzero&(side!=0)
    return {'rows':len(r),'positive':int(y.sum()),'flat':int((r==0).sum()),
        'brier_positive_including_flats':float(np.mean((p-y)**2)),
        'log_loss_positive_including_flats':float(-np.mean(y*np.log(q)+(1-y)*np.log(1-q))),
        'auc_positive_including_flats':float(roc_auc_score(y,p)) if len(np.unique(y))==2 else None,
        'nonflat_directional_decisions':int(predicted.sum()),
        'nonflat_direction_accuracy':float(np.mean(side[predicted]==np.sign(r[predicted]))) if predicted.any() else None}


def decision_metrics(frame,side,mu,expected_long,expected_short):
    side=np.asarray(side);r=frame.return_bps.to_numpy();chosen=side!=0
    actual=np.where(side>0,frame.long_net_bps,np.where(side<0,frame.short_net_bps,0.))
    if not np.isfinite(actual).all():raise ValueError('unmatched_cost_support')
    expect=np.where(side>0,expected_long,np.where(side<0,expected_short,0.))
    nonflat=chosen&(r!=0)
    return {'opportunities':len(frame),'selected':int(chosen.sum()),'coverage':float(chosen.mean()),
        'mean_net_bps_all_opportunities':float(np.mean(actual)),
        'mean_net_bps_selected':float(np.mean(actual[chosen])) if chosen.any() else None,
        'mean_net_bps_selected_extra_1bp_cost':float(np.mean(actual[chosen])-1) if chosen.any() else None,
        'mean_net_bps_all_extra_1bp_cost':float(np.mean(actual-chosen.astype(float))),
        'selected_profitable_fraction':float(np.mean(actual[chosen]>0)) if chosen.any() else None,
        'selected_nonflat_direction_accuracy':float(np.mean(side[nonflat]==np.sign(r[nonflat]))) if nonflat.any() else None,
        'selected_nonflat_outcomes':int(nonflat.sum()),'selected_flat_outcomes':int((chosen&(r==0)).sum()),
        'forecast_mean_mae_bps':float(np.mean(np.abs(np.asarray(mu)-r))),
        'forecast_mean_mse_bps2':float(np.mean((np.asarray(mu)-r)**2)),
        'mean_expected_net_selected':float(np.mean(expect[chosen])) if chosen.any() else None,
        'cost_scope':'hypothetical_M1_bid_ask_endpoint_not_account_profit_or_fill_proof'}


def component_errors(actual,prediction):
    actual=np.asarray(actual,dtype=float);prediction=np.asarray(prediction,dtype=float)
    if actual.shape!=prediction.shape or not np.isfinite(actual).all() or not np.isfinite(prediction).all():
        raise ValueError('invalid_component_metric')
    if not len(actual):return {'rows':0,'mae_bps':None,'mse_bps2':None,'bias_bps':None}
    delta=prediction-actual
    return {'rows':len(actual),'mae_bps':float(np.mean(np.abs(delta))),
            'mse_bps2':float(np.mean(delta**2)),'bias_bps':float(np.mean(delta)),
            'mean_actual_bps':float(np.mean(actual)),'mean_prediction_bps':float(np.mean(prediction))}


def empirical(train,assessment):
    key='tech_rate_bps_per_min_15m'
    t=train.assign(momentum_sign=np.sign(train[key]).astype(int))
    grouped=t.groupby(['instrument','momentum_sign']).return_bps.agg(['mean','size'])
    pooled=t.groupby('momentum_sign').return_bps.mean().to_dict();grand=float(t.return_bps.mean())
    mu=[]
    for pair,signal in zip(assessment.instrument,np.sign(assessment[key]).astype(int)):
        value=grouped.loc[(pair,signal)] if (pair,signal) in grouped.index else None
        mu.append(float(value['mean']) if value is not None and value['size']>=30 else pooled.get(signal,grand))
    exits={}
    for name in ('long','short'):
        col='future_'+name+'_exit_cost_bps';by_pair=t.groupby('instrument')[col].mean()
        exits[name]=assessment.instrument.map(by_pair).fillna(float(t[col].mean())).to_numpy()
    return np.asarray(mu),exits['long'],exits['short']


def run(predecessor,output,spec_path):
    spec_bytes=Path(spec_path).read_bytes();spec=json.loads(spec_bytes)
    root=Path(output);root.mkdir(parents=True,exist_ok=False)
    bindings={name:digest(Path(__file__).parent/name) for name in
              ('signed_cost_models_v1.py','run_signed_cost_study_v1.py')}
    frozen={'spec':spec,'spec_sha256':hashlib.sha256(spec_bytes).hexdigest(),'source_bindings':bindings,
        'started_utc':datetime.now(timezone.utc).isoformat(),
        'previous_completed_result_sha256':digest(Path(predecessor)/'evaluation_001/RESULTS.json'),
        'previous_prepared_sha256':digest(Path(predecessor)/'prepared_001/PREPARED.json'),
        'history_already_inspected':True,'can_place_orders':False,'can_promote':False}
    (root/'FROZEN_RUN.json').write_text(encoded(frozen))
    panel,groups,prepared=load_inputs(predecessor)
    (root/'FEATURE_GROUPS.json').write_text(encoded(groups))
    panel.to_parquet(root/'input_panel.parquet',index=False)
    outputs=[];summaries=[];warnings_count=Counter()
    with threadpool_limits(limits=1):
        for horizon in spec['horizons_minutes']:
            train,calibration,assess=split(panel[panel.horizon_minutes==horizon].reset_index(drop=True),spec)
            if assess.empty:raise ValueError('empty_assessment')
            kept=['instrument','epoch','horizon_minutes','label_available_epoch','return_bps','long_net_bps','short_net_bps',
                'known_entry_long_cost_bps','known_entry_short_cost_bps','future_long_exit_cost_bps','future_short_exit_cost_bps','mid']
            out=assess[kept].copy();out['date']=pd.to_datetime(out.epoch+60,unit='s',utc=True).dt.strftime('%Y-%m-%d')
            all_values={};probability={};clips={};artifacts={};components={}
            e_long=assess.known_entry_long_cost_bps.to_numpy();e_short=assess.known_entry_short_cost_bps.to_numpy()
            for group,columns in groups.items():
                print(f'Fitting signed mean / costs H{horizon}m {group}',flush=True)
                with warnings.catch_warnings(record=True) as caught:
                    warnings.simplefilter('always');bundle=model.fit_bundle(train,calibration,columns=columns,spec=spec)
                warnings_count.update(type(w.message).__name__+': '+str(w.message) for w in caught)
                if bundle['calibration_label_max_epoch']>=assess.epoch.min()+60:raise ValueError('calibration_labels_overlap_assessment')
                values,clip=model.predict_bundle(bundle,assess);clips[group]=clip
                positive=assess.return_bps.to_numpy()>0
                components[group]={
                    'positive_mean_given_positive':component_errors(assess.return_bps.to_numpy()[positive],values['positive'][positive]),
                    'nonpositive_magnitude_given_nonpositive':component_errors(-assess.return_bps.to_numpy()[~positive],values['nonpositive'][~positive]),
                    'long_exit_cost':component_errors(assess.future_long_exit_cost_bps,values['long_exit']),
                    'short_exit_cost':component_errors(assess.future_short_exit_cost_bps,values['short_exit'])}
                for field,value in values.items():out[group+'__'+field]=value
                probability[group]={name:probability_metrics(assess.return_bps,values['probability_positive_'+name]) for name in ('raw','calibrated')}
                bundle.update(horizon_minutes=horizon,instrument_universe=prepared['source_manifest']['pairs'],spec_sha256=frozen['spec_sha256'])
                file=root/f'h{horizon}_{group}.joblib';joblib.dump(bundle,file)
                artifacts[group]={'file':file.name,'sha256':digest(file),'training_rows':bundle['training_rows'],
                    'calibration_rows':bundle['calibration_rows'],'trained_label_max_epoch':bundle['trained_label_max_epoch'],
                    'calibration_label_max_epoch':bundle['calibration_label_max_epoch'],'calibration':bundle['calibration']}
                for method in ('direct_signed_mean','mixture_raw','mixture_calibrated'):
                    name=group+'__'+method;mu=values[method]
                    side,long,short=model.decide(mu,e_long,e_short,values['long_exit'],values['short_exit'],spec['entry_margin_bps'])
                    all_values[name]=(side,mu,long,short)
            mu,long_exit,short_exit=empirical(train,assess)
            side,long,short=model.decide(mu,e_long,e_short,long_exit,short_exit,spec['entry_margin_bps'])
            all_values['empirical_momentum_mean']=(side,mu,long,short)
            reversal=-np.sign(assess.tech_rate_bps_per_min_15m.to_numpy()).astype(int)
            # Control side is deterministic; zero means are placeholders, no expected-return claim.
            zero=np.zeros(len(assess));all_values['always_reversal']=(reversal,zero,zero,zero)
            all_values['no_trade']=(np.zeros(len(assess),dtype=int),zero,zero,zero)
            metrics={};by_date={};by_pair={}
            for name,(side,mu,long,short) in all_values.items():
                out['side__'+name]=side;out['mean__'+name]=mu
                out['long__'+name]=long;out['short__'+name]=short
                metrics[name]=decision_metrics(assess,side,mu,long,short)
                for date,idx in out.groupby('date').indices.items():
                    by_date.setdefault(date,{})[name]=decision_metrics(assess.iloc[idx],side[idx],mu[idx],long[idx],short[idx])
                for pair,idx in assess.groupby('instrument').indices.items():
                    by_pair.setdefault(pair,{})[name]=decision_metrics(assess.iloc[idx],side[idx],mu[idx],long[idx],short[idx])
            for container in (metrics,*by_date.values(),*by_pair.values()):
                for name in ('always_reversal','no_trade'):
                    container[name]['mean_expected_net_selected']=None
            summaries.append({'horizon_minutes':horizon,'training_rows':len(train),'calibration_rows':len(calibration),
                'assessment_rows':len(assess),'assessment_pairs':int(assess.instrument.nunique()),'artifacts':artifacts,
                'probability':probability,'component_errors':components,'negative_prediction_clips':clips,'metrics':metrics,'by_date':by_date,'by_pair':by_pair})
            outputs.append(out)
    predictions=pd.concat(outputs,ignore_index=True);predictions.to_parquet(root/'predictions.parquet',index=False)
    if any(digest(Path(__file__).parent/name)!=expected for name,expected in bindings.items()):
        raise ValueError('source_changed_during_fit')
    result={'study_id':spec['study_id'],'completed_utc':datetime.now(timezone.utc).isoformat(),'spec_sha256':frozen['spec_sha256'],
        'source_bindings':bindings,'groups':groups,'horizons':summaries,'assessment_rows':len(predictions),
        'warnings':dict(warnings_count),'input_panel_sha256':digest(root/'input_panel.parquet'),
        'predictions_sha256':digest(root/'predictions.parquet'),'history_already_inspected':True,
        'can_place_orders':False,'can_promote':False,'selected_model':None,
        'limits':['Only2assessmentdates; allhistoryalreadyinspected.','Overlapping hypotheticalpositions; noportfolioorstopmodel.',
                  'Future cost is an estimated output; actual future cost only scores outcomes.','No claimed magnitude or path calibration.']}
    (root/'RESULTS.json').write_text(encoded(result))
    print(json.dumps({'completed':True,'assessment_rows':len(predictions),'model_bundles':8,'results':str(root/'RESULTS.json')}),flush=True)


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--predecessor',required=True)
    parser.add_argument('--output',required=True);parser.add_argument('--spec',required=True)
    args=parser.parse_args();run(args.predecessor,args.output,args.spec)
