"""Run fixed same-origin development comparisons; never enables trading.

Four declared input groups x four horizons x Ridge/HGB. All models fit only
the original-clock TRAIN sample and mature endpoint labels; later periods
never tune preprocessing, recipes, stopping or the entry threshold. Predictions
are issued on every later origin before future label masks are applied.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import gc
import hashlib
import importlib.metadata
import json
from pathlib import Path
import shutil
import sys
import time

ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0,str(ROOT))
import joblib
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.linear_model import Ridge
from threadpoolctl import threadpool_limits
from oanda_rolling_model_design_v1 import SCHEMA,HORIZONS,RECIPES,feature_groups,learner_inputs
from oanda_rolling_model_scoring_v1 import evaluate_predictions,_forecast_scores
from oanda_rolling_technical_dataset_v1 import file_sha,key_hash
from oanda_rolling_technical_endpoint_labels_v1 import checked_path

CHUNK_ROWS=8192
MAX_OUTPUT_BYTES=3*1024**3


def save(path,value):
    Path(path).write_text(json.dumps(value,indent=2,sort_keys=True,allow_nan=False)+'\n',encoding='utf-8')


def assert_pins(bindings):
    if any(file_sha(ROOT/n)!=h for n,h in bindings.items()):
        raise ValueError('comparison_bound_source_changed')


def check_storage(output):
    used=sum(p.stat().st_size for sub in ('models','forecasts','metrics') for p in (output/sub).iterdir())
    if used>MAX_OUTPUT_BYTES:raise ValueError('comparison_output_cap_exceeded')
    if shutil.disk_usage(output).free<32*1024**3:raise ValueError('comparison_drive_reserve')


def load_inputs(prepared,quotes):
    """Explicit schema extraction; labels never enter the feature matrix."""
    prepared,quotes=Path(prepared).resolve(),Path(quotes).resolve()
    pm=json.loads((prepared/'PREPARED.json').read_bytes())
    qm=json.loads((quotes/'QUOTE_PANEL.json').read_bytes())
    if (pm['schema']!=SCHEMA or pm['status']!='complete' or qm['status']!='complete'
            or pm['base_sha256']!=qm['base_manifest_sha256'] or pm['overlay_sha256']!=qm['endpoint_manifest_sha256']
            or set(pm['pairs'])!=set(qm['pairs'])
            or len(pm['pairs'])!=68):
        raise ValueError('complete_matched_68_pair_inputs_required')
    names=pm['feature_names'];feature_groups(names)
    total=pm['rows']; pair_names=sorted(pm['pairs'])
    data={'x':np.empty((total,len(names)),dtype=np.float32),'time':np.empty(total,dtype=np.int64),
          'split':np.empty(total,dtype=np.int8),'pair_id':np.empty(total,dtype=np.int16),
          'spread':np.empty(total,dtype=np.float64)}
    for h in HORIZONS:
        for stem in ('y','long','short','arima','momentum','delay_long','delay_short'):
            data[f'{stem}_{h}']=np.empty(total,dtype=np.float64)
        for stem in ('valid','strict','eligible','delay_valid'):
            data[f'{stem}_{h}']=np.empty(total,dtype=bool)
    offset=0
    for pair_id,pair in enumerate(pair_names):
        r=pm['pairs'][pair];qr=qm['pairs'][pair]
        ip=checked_path(prepared,r['path'],r['sha256'])
        qp=checked_path(quotes,qr['path'],qr['sha256'])
        checked_path(quotes,qr['receipt_path'],qr['receipt_sha256'])
        qt=pq.ParquetFile(qp).read()
        clocks=qt['bar_start_epoch'].to_numpy()
        if len(clocks)!=qr['rows'] or key_hash(clocks)!=qr['key_sha256'] or np.any(clocks[1:]<=clocks[:-1]):
            raise ValueError('quote_panel_clock_identity_required')
        if any(v!=pair for v in qt['instrument'].to_pylist()):
            raise ValueError('quote_pair_identity_required')
        with np.load(ip,allow_pickle=False) as arrays:
            t=arrays['time']; n=len(t); sl=slice(offset,offset+n)
            if n!=r['rows'] or key_hash(t)!=r['key_sha256']:
                raise ValueError('prepared_pair_clock_identity_required')
            index=np.searchsorted(clocks,t)
            if np.any(index>=len(clocks)) or not np.array_equal(clocks[index],t):
                raise ValueError('exact_original_quote_join_required')
            if arrays['x'].shape!=(n,len(names)) or arrays['x'].dtype!=np.float32:
                raise ValueError('registered_preprocessed_matrix_required')
            for k in ('x','time','split'):data[k][sl]=arrays[k]
            data['pair_id'][sl]=pair_id
            split_strings=qt['origin_split'].to_numpy()[index]
            expected=np.where(split_strings=='train',0,np.where(split_strings=='validation',1,2))
            if not np.array_equal(expected,arrays['split']):
                raise ValueError('quote_and_prepared_split_identity_required')
            data['spread'][sl]=qt['quote__entry_spread_bps'].to_numpy()[index]
            mid=qt['quote__mid_close'].to_numpy()
            for h in HORIZONS:
                for stem in ('y','long','short','valid','strict','eligible'):
                    data[f'{stem}_{h}'][sl]=arrays[f'{stem}_{h}']
                data[f'arima_{h}'][sl]=qt[f'forecast__arima110_conditional_ols__{h}m_bps'].to_numpy()[index]
                for stem,field in [('delay_long','long_net_bps'),('delay_short','short_net_bps'),('delay_valid','valid')]:
                    data[f'{stem}_{h}'][sl]=qt[f'delayed_label__{h}m__{field}'].to_numpy()[index]
                past=np.searchsorted(clocks,t-h*60); safe=np.minimum(past,len(clocks)-1)
                valid=(past<len(clocks))&(clocks[safe]==t-h*60)&np.isfinite(mid[index])&np.isfinite(mid[safe])&(mid[safe]>0)
                with np.errstate(divide='ignore',invalid='ignore',over='ignore'):
                    momentum=(mid[index]/mid[safe]-1.)*10000.
                momentum[~valid|~np.isfinite(momentum)]=np.nan
                data[f'momentum_{h}'][sl]=momentum
        offset+=n
    if offset!=total:raise ValueError('complete_prepared_population_required')
    data['pair_names']=pair_names
    return data,pm,qm


def training_mask(data,horizon):
    mask=(data['split']==0)&data[f'valid_{horizon}']&data[f'eligible_{horizon}']
    if np.any(mask&~np.isfinite(data[f'y_{horizon}'])):
        raise ValueError('valid_training_label_must_be_finite')
    return mask


def make_estimator(learner,width):
    if learner=='ridge':return Ridge(**RECIPES['ridge'])
    if learner=='hgb':
        categorical=np.zeros(width,dtype=bool);categorical[-1]=True
        return HistGradientBoostingRegressor(**RECIPES['hgb'],categorical_features=categorical)
    raise ValueError('unknown_learner')


def matrix(data,rows,columns,learner):
    z=data['x'][rows][:,columns]
    x=learner_inputs(z,data['pair_id'][rows],learner,len(data['pair_names']))
    return x.astype(np.float64) if learner=='ridge' else x


def predict_chunks(model,data,rows,columns,learner):
    out=np.empty(len(rows),dtype=np.float64)
    for start in range(0,len(rows),CHUNK_ROWS):
        selected=rows[start:start+CHUNK_ROWS]
        out[start:start+len(selected)]=model.predict(matrix(data,selected,columns,learner))
    if not np.isfinite(out).all():raise ValueError('learned_forecast_must_be_finite_on_every_assessment_origin')
    return out


def pair_prior(data,h,assessment):
    mask=training_mask(data,h)
    pred=np.full(len(assessment),np.nan)
    params={}
    for i,pair in enumerate(data['pair_names']):
        y=data[f'y_{h}'][mask&(data['pair_id']==i)]
        status='available' if len(y)>=20 else 'insufficient_training_labels'
        values={'status':status,'training_rows':len(y),'mean_bps':float(np.mean(y)) if len(y) else None,
                'p_up':float(np.mean(y>0)) if len(y) else None,'p_down':float(np.mean(y<0)) if len(y) else None,
                'p_flat':float(np.mean(y==0)) if len(y) else None}
        params[pair]=values
        if status=='available':pred[data['pair_id'][assessment]==i]=values['mean_bps']
    return pred,params


def save_forecast(output,tag,data,assessment,prediction):
    path=output/'forecasts'/(tag+'.parquet')
    table=pa.table({'pair_id':data['pair_id'][assessment], 'bar_start_epoch':data['time'][assessment],
                    'split':data['split'][assessment],'predicted_bps':pa.array(prediction,mask=~np.isfinite(prediction))})
    pq.write_table(table,path,compression='zstd',use_dictionary=['pair_id','split'],row_group_size=8192)
    actual=pq.ParquetFile(path).read()
    if not table.equals(actual):raise ValueError('forecast_table_readback_failed')
    got=actual['predicted_bps'].to_numpy(); valid=np.isfinite(prediction)
    if not np.array_equal(np.isnan(got),np.isnan(prediction)) or not np.array_equal(got[valid].view(np.uint64),prediction[valid].view(np.uint64)):
        raise ValueError('forecast_finite_bits_readback_failed')
    return {'path':path.relative_to(output).as_posix(),'sha256':file_sha(path),'bytes':path.stat().st_size,'rows':len(prediction)}


def score_variant(data,assessment,prediction,h,prior):
    reports={}
    pairs=np.array(data['pair_names'])[data['pair_id'][assessment]]
    # Common comparison coverage depends only on input availability and fitted
    # TRAIN support, never on future returns, spreads or profit.
    common=np.isfinite(data[f'arima_{h}'][assessment])&np.isfinite(data[f'momentum_{h}'][assessment])&np.isfinite(prior)
    for split_id,split_name in ((1,'validation'),(2,'later_development_test')):
        m=data['split'][assessment]==split_id; rows=assessment[m]; p=prediction[m]
        args=(data['time'][rows],pairs[m],p,data[f'y_{h}'][rows],data[f'long_{h}'][rows],data[f'short_{h}'][rows],
              data[f'valid_{h}'][rows],data[f'strict_{h}'][rows],data[f'eligible_{h}'][rows],data['spread'][rows])
        primary=evaluate_predictions(*args,horizon_minutes=h,comparison_origin_mask=common[m])
        own=np.isfinite(p)&data[f'valid_{h}'][rows]&data[f'eligible_{h}'][rows]
        primary['own_coverage_forecast_scores_without_comparator_restriction']=_forecast_scores(p[own],data[f'y_{h}'][rows][own])
        delay_valid=data[f'valid_{h}'][rows]&data[f'delay_valid_{h}'][rows]
        delayed=evaluate_predictions(data['time'][rows],pairs[m],p,data[f'y_{h}'][rows],
             data[f'delay_long_{h}'][rows],data[f'delay_short_{h}'][rows],delay_valid,
             data[f'strict_{h}'][rows]&delay_valid,data[f'eligible_{h}'][rows],data['spread'][rows],
             horizon_minutes=h,comparison_origin_mask=common[m])
        for policy in primary['policies']:
            if primary['policies'][policy]['decision_pair_clock_side_sha256']!=delayed['policies'][policy]['decision_pair_clock_side_sha256']:
                raise ValueError('execution_delay_must_not_change_original_decisions')
        delayed['execution_scope']='same original forecasts, signs, entry gate, holding reservations and terminal clock; enter exact next minute close; net normalized by delayed-entry mid; midpoint forecast errors still refer to original target return'
        reports[split_name]={'primary':primary,'one_minute_entry_delay':delayed}
    return reports


def baseline_training_description(name,supervised_record,quote_sha256):
    if name=='pair_train_mean':return {'method':'sampled_supervised_train_labels','selection':supervised_record}
    if name=='arima110_conditional_ols':
        return {'method':'zero-intercept AR1 log-increment conditional OLS on contiguous M1 TRAIN triplets',
                'quote_manifest_sha256':quote_sha256,
                'parameters':'per-pair hash-bound quote-panel receipt arima_params; exact declared training start/end',
                'uses_sampled_supervised_forecast_labels':False}
    return {'method':'no_fitting','uses_training_labels':False}


def run(args):
    prepared,quotes,output=(Path(p).resolve() for p in (args.prepared,args.quotes,args.output))
    if output.exists() or not output.is_relative_to(ROOT/'data'):
        raise ValueError('new_research_comparison_output_required')
    if shutil.disk_usage(output.parent).free-MAX_OUTPUT_BYTES<32*1024**3:
        raise ValueError('comparison_storage_reserve_required')
    pm=json.loads((prepared/'PREPARED.json').read_bytes());qm=json.loads((quotes/'QUOTE_PANEL.json').read_bytes())
    pins=dict(pm['source_bindings'])
    for n,h in qm['source_bindings'].items():
        if n in pins and pins[n]!=h:raise ValueError('incompatible_comparison_source_bindings')
        pins[n]=h
    for n in ['tools/run_rolling_model_comparison_v1.py','oanda_rolling_model_scoring_v1.py']:
        pins[n]=file_sha(ROOT/n)
    assert_pins(pins)
    output.mkdir()
    for sub in ('models','forecasts','metrics'):(output/sub).mkdir()
    report={'schema':SCHEMA,'status':'loading','started_utc':datetime.now(timezone.utc).isoformat(),
        'prepared_root':str(prepared),'prepared_sha256':file_sha(prepared/'PREPARED.json'),
        'quote_root':str(quotes),'quote_sha256':file_sha(quotes/'QUOTE_PANEL.json'),'source_bindings':pins,
        'groups':feature_groups(pm['feature_names']),'recipes':RECIPES,'horizons':list(HORIZONS),
        'training_sample':pm['training_sample'],'normalizer_scope':pm['normalizer_scope'],
        'assessment_population':'all original validation and later-development-test origins, before future masks',
        'matched_score_origins':'current ARIMA input available AND exact past-horizon midpoint available AND pair TRAIN mean supported; original-known comparator support only; own-coverage error scores also reported',
        'decision_rule':'sign(prediction), and threshold abs(prediction)>current quote spread bps+1; fixed before fitting; evaluate actual chosen-side bid/ask net minus0/1/2bps',
        'pair_context':'explicit causal instrument ID added as68 fixed one-hot indicators for Ridge or one categorical field for HGB; no other metadata admitted',
        'missingness':'Ridge uses zero after train-only standardization plus missing flags; HGB uses native NaN',
        'training_weights':'equal sampled pair/minute rows; no label-magnitude weighting or balancing',
        'learner_threads':4,'early_stopping_or_later_tuning':False,'learned_fit_count':0,'cells':{},'baselines':{},
        'holding_scope':'independent per-pair reservations, reset at each assessment split; not a continuous portfolio simulation',
        'models_promoted':0,'can_place_orders':False,'untouched_confirmation':False,
        'versions':{n:importlib.metadata.version(n) for n in ('numpy','pandas','pyarrow','scikit-learn','scipy','joblib','threadpoolctl')},
        'limits':['Previously examined development periods; no untouched test claim.','Endpoint proxies are not fills/account returns; per-pair nonoverlap does not remove cross-currency dependence.','The32 fixed learned cells are a descriptive comparison, not32 independent trials.','Compact/full comparisons differ in lookback support and representation, as well as field count.','Peer differences are derived base-minus-quote inputs, not independent information.','ARIMA conditional OLS is newly aligned; it is not the historical statsmodels MLE implementation.']}
    save(output/'RESULTS.json',report)
    began=time.monotonic()
    try:
        with threadpool_limits(limits=4):
            data,loaded_pm,loaded_qm=load_inputs(prepared,quotes)
            if loaded_pm!=pm or loaded_qm!=qm:raise ValueError('input_manifest_changed_during_loading')
            assessment=np.flatnonzero(data['split']!=0)
            report.update(status='running',prepared_rows=len(data['time']),assessment_rows=len(assessment),pair_names=data['pair_names'])
            priors={}
            for h in HORIZONS:
                priors[h],params=pair_prior(data,h,assessment)
                save(output/'models'/f'pair_prior_{h}m.json',params)
            for h in HORIZONS:
                mask=training_mask(data,h);train_rows=np.flatnonzero(mask)
                fit_keys=np.column_stack((data['pair_id'][mask].astype(np.int64),data['time'][mask]))
                train_record={'rows':len(train_rows),'pair_clock_sha256':hashlib.sha256(fit_keys.astype('<i8').tobytes()).hexdigest(),
                    'target_sha256':hashlib.sha256(data[f'y_{h}'][mask].astype('<f8').tobytes()).hexdigest(),
                    'counts_by_pair':{pair:int((mask&(data['pair_id']==i)).sum()) for i,pair in enumerate(data['pair_names'])}}
                for group,selected_names in report['groups'].items():
                    columns=[pm['feature_names'].index(n) for n in selected_names]
                    for learner in ('ridge','hgb'):
                        tag=f'{learner}_{group}_{h}m';started=time.monotonic()
                        x=matrix(data,train_rows,columns,learner)
                        model=make_estimator(learner,x.shape[1]);model.fit(x,data[f'y_{h}'][mask]);del x
                        fit_seconds=time.monotonic()-started
                        model_path=output/'models'/(tag+'.joblib')
                        joblib.dump({'estimator':model,'learner':learner,'selected_feature_names':selected_names,
                                     'prepared_sha256':report['prepared_sha256'],'pair_names':data['pair_names'],
                                     'horizon_minutes':h,'training_selection':train_record},model_path,compress=3)
                        pred=predict_chunks(model,data,assessment,columns,learner)
                        restored=joblib.load(model_path)['estimator']
                        replay=restored.predict(matrix(data,assessment[:CHUNK_ROWS],columns,learner))
                        if not np.array_equal(pred[:len(replay)],replay):raise ValueError('persisted_model_prediction_recreation_failed')
                        representatives=np.array([np.flatnonzero(data['pair_id'][assessment]==i)[0] for i in range(len(data['pair_names']))])
                        pair_replay=restored.predict(matrix(data,assessment[representatives],columns,learner))
                        if not np.allclose(pred[representatives],pair_replay,rtol=1e-12,atol=1e-10):
                            raise ValueError('all_pair_model_recreation_failed')
                        forecast=save_forecast(output,tag,data,assessment,pred)
                        metrics=score_variant(data,assessment,pred,h,priors[h])
                        metric_path=output/'metrics'/(tag+'.json');save(metric_path,metrics)
                        report['cells'][tag]={'learner':learner,'group':group,'horizon_minutes':h,'registered_input_count':len(selected_names),
                            'training_selection':train_record,'fit_seconds':round(fit_seconds,3),'total_seconds':round(time.monotonic()-started,3),
                            'model':{'path':model_path.relative_to(output).as_posix(),'sha256':file_sha(model_path)},
                            'recreated_prediction_rows':len(replay),'recreated_first_chunk_exact':True,
                            'additional_replayed_pair_representatives':len(representatives),'representative_replay_tolerance':{'rtol':1e-12,'atol_bps':1e-10},'forecast':forecast,
                            'metrics':{'path':metric_path.relative_to(output).as_posix(),'sha256':file_sha(metric_path)}}
                        report['learned_fit_count']+=1
                        assert_pins(pins);save(output/'RESULTS.json',report)
                        print(json.dumps({'cell':tag,'fit_seconds':round(fit_seconds,2),'completed_fits':report['learned_fit_count'],
                              'elapsed_seconds':round(time.monotonic()-began,1)}),flush=True)
                        del model,restored,pred,metrics;gc.collect()
                        check_storage(output)
                controls={'no_change':np.zeros(len(assessment)),'pair_train_mean':priors[h],
                    'arima110_conditional_ols':data[f'arima_{h}'][assessment],
                    'momentum_exact_past_horizon':data[f'momentum_{h}'][assessment],
                    'reversal_exact_past_horizon':-data[f'momentum_{h}'][assessment]}
                for name,pred in controls.items():
                    tag=f'{name}_{h}m';forecast=save_forecast(output,tag,data,assessment,pred)
                    metric_path=output/'metrics'/(tag+'.json');save(metric_path,score_variant(data,assessment,pred,h,priors[h]))
                    report['baselines'][tag]={'name':name,'horizon_minutes':h,
                        'training_description':baseline_training_description(name,train_record,report['quote_sha256']),
                        'forecast':forecast,'metrics':{'path':metric_path.relative_to(output).as_posix(),'sha256':file_sha(metric_path)}}
                    check_storage(output)
                    save(output/'RESULTS.json',report)
                    print(json.dumps({'baseline':tag,'elapsed_seconds':round(time.monotonic()-began,1)}),flush=True)
            assert_pins(pins)
            check_storage(output)
            if file_sha(prepared/'PREPARED.json')!=report['prepared_sha256'] or file_sha(quotes/'QUOTE_PANEL.json')!=report['quote_sha256']:
                raise ValueError('comparison_input_metadata_changed')
            for entry in [*report['cells'].values(),*report['baselines'].values()]:
                for k in ('model','metrics','forecast'):
                    if k in entry:checked_path(output,entry[k]['path'],entry[k]['sha256'])
            report.update(status='complete',completed_utc=datetime.now(timezone.utc).isoformat(),elapsed_seconds=round(time.monotonic()-began,3))
            save(output/'RESULTS.json',report)
    except BaseException as exc:
        report.update(status='failed',failure={'type':type(exc).__name__,'message':str(exc)})
        save(output/'RESULTS.json',report)
        raise
    return report


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--prepared',type=Path,required=True)
    p.add_argument('--quotes',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True)
    r=run(p.parse_args());print(json.dumps({k:r[k] for k in ('status','learned_fit_count','elapsed_seconds')}))
