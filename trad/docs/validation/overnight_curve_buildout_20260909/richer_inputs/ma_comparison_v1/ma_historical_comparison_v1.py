"""Retrospective, gap-strict MA/compact Ridge comparison; no runtime or broker I/O.

This is a new offline experiment. No opaque model is loaded, and no fitted
legacy weight is reused. Original ingestion/availability remains unproven.
"""
from __future__ import annotations
import argparse
from collections import Counter
import csv
from datetime import datetime, timezone
from decimal import Context, Decimal, localcontext
import hashlib
import io
import json
import math
import os
from pathlib import Path
import re
import sys
import time

import numpy as np
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler
from threadpoolctl import threadpool_limits

BASE=Path(__file__).resolve().parent
TRAD=BASE.parents[2]/'trad'
sys.path.insert(0,str(TRAD))
import oanda_ma_causal_features_v2 as ma
import oanda_pair_local_models_v2 as compact

VERSION='ma_compact_historical_engineering_v1_20260909'
PIPS={'EUR_USD':'0.0001','GBP_USD':'0.0001','USD_JPY':'0.01'}
HORIZONS=(15,30,60)
ALPHAS=(.1,1.,10.,100.,1000.)
MINIMUM={'train':256,'validation':64,'test':64}
MAX_BYTES=20*1024*1024
MAX_ROWS=100000
MAX_ORIGINS=20000
MAX_RUN_SEC=600
SOURCE_FILES=('oanda_ma_causal_features_v2.py','oanda_ma_feature_grid.py','oanda_pair_local_models_v2.py')
FLAGS=dict(research_only=True,can_place_orders=False,can_promote=False,can_authorize=False,
           account_eligible=False,execution_eligible=False,proof_eligible=False,forecast_issued=False)
COMPACT_NAMES=tuple(f'{w}m_{n}' for w in (1,5,15,30,60) for n in
    ('rate_pips_per_minute','span_ratio','missing_fraction','unavailable'))+(
    'rms_change_pips','maximum_gap_minutes','current_price_count_ratio','session_elapsed_hours')
POLICY=dict(version=VERSION,instruments=list(PIPS),pip_sizes=PIPS,horizons_minutes=list(HORIZONS),
    price_basis='archived_mid_close_unknown_historical_ingestion',price_clock='bar_label_plus60',
    target='exact_future_mid_close_in_same_strictly_contiguous_session',
    session='new_session_after_any_adjacent_label_gap_not_equal60sec; no fill; reset EMA at real session start',
    warmup_real_rows=204,anchor_grid_seconds=300,anchor_grid_basis='UTC_bar_label_mod300_equal0',
    families=['ma_causal643_ridge','compact_causal24_ridge','compact24_plus_ma643_ridge'],baselines=['zero_return','last60m_momentum_scaled_to_horizon'],
    split='per_pair_actual_time_range60/20/20; same cutoffs for all horizons/families',
    purge='training target price time strictly before validation cutoff; validation target strictly before test cutoff',
    scaler='training_only_StandardScaler_population_std; constants_scale1; no imputation',
    ridge='unpenalized intercept; alpha selected on validation_MAE; tie chooses larger alpha',
    alphas=list(ALPHAS),refit_after_validation=False,minimum_rows=MINIMUM,
    target_units='basis_points_relative_to_reference_mid_close',probability_model=None,
    bid_ask_cost='separate hypothetical bid/ask candle-close diagnostic; no observed tradeability, fills, slippage or USD sizing',
    maximum_source_bytes_per_pair=MAX_BYTES,maximum_rows_per_pair=MAX_ROWS,
    maximum_origins_per_pair=MAX_ORIGINS,maximum_run_seconds=MAX_RUN_SEC,linear_algebra_threads=1,
    dependence_reporting=dict(unit='UTC_calendar_date',minimum_dates_for_interval=10,
        resamples=1000,seed=20260909,interval='percentile95_MAE_baseline_minus_model',
        across_pair_pooling=False,if_pooled='one_shared_date_resample_across_all_pairs',
        multiple_comparison_adjusted=False,selection_or_promotion_from_test=False,
        neutral_cost_baseline='zero_hypothetical_no_position_when_bid_ask_domain_valid'),
    independent_sample_size=None,**FLAGS)

def need(ok,reason):
    if not ok: raise ValueError(reason)

def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()

def sha(raw): return hashlib.sha256(raw).hexdigest()
def load_json(path): return json.loads(Path(path).read_bytes())
def binding(path):
    raw=Path(path).read_bytes();return dict(path=str(path),sha256=sha(raw),bytes=len(raw))
def write_new(path,value):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('xb') as f:
        f.write(json.dumps(value,sort_keys=True,indent=2,allow_nan=False).encode()+b'\n')
        f.flush();os.fsync(f.fileno())
    return binding(path)
def now(): return time.time()
def iso(epoch): return datetime.fromtimestamp(epoch,timezone.utc).isoformat()

def source_bindings():
    return {name:binding(TRAD/name)['sha256'] for name in SOURCE_FILES}

def design(capture_path,capture_sha):
    need(binding(capture_path)['sha256']==capture_sha,'capture_manifest_hash_mismatch')
    capture=load_json(capture_path)
    need(capture.get('schema')=='ma_history_capture_20260909' and capture.get('research_only') is True,
         'capture_manifest_schema')
    need(set(capture['sources'])==set(PIPS),'capture_pair_inventory')
    return dict(policy=POLICY,capture_manifest=binding(capture_path),source_bindings=source_bindings(),
        implementation=binding(Path(__file__)),test_source=binding(BASE/'test_ma_historical_comparison_v1.py'),
        created_epoch=now(),created_utc=iso(now()),pre_evaluation=True,
        dependency_versions=dependencies(),prior_context=[binding(TRAD/p) for p in (
            'FOREX_PENDING_IMPROVEMENTS.md','docs/FOREX_FEATURE_DICTIONARY_CURRENT.md',
            'docs/validation/revamp_baseline_20260908/reconciliation/BACKLOG_RECONCILIATION_20260908.md')],**FLAGS)

def dependencies():
    import pandas, sklearn, scipy
    return dict(python=sys.version,numpy=np.__version__,pandas=pandas.__version__,sklearn=sklearn.__version__,scipy=scipy.__version__)

def _number(value):
    need(isinstance(value,str) and len(value)<=64 and re.fullmatch(r'[0-9]+(?:\.[0-9]+)?',value),'invalid_price_text')
    d=Decimal(value);need(d.is_finite() and 0<d<Decimal('1e12'),'invalid_positive_price');return d

def read_rows(raw,pair,observed):
    """Validate exact retained CSV. Optional bid/ask failures affect costs only."""
    need(pair in PIPS and 0<len(raw)<=MAX_BYTES and raw.endswith(b'\n'),'source_byte_bound_or_partial_tail')
    reader=csv.DictReader(io.StringIO(raw.decode('utf-8-sig')))
    names=reader.fieldnames
    need(names and len(names)==len(set(names)) and {'time','instrument','granularity','open','high','low','close'}<=set(names),'source_columns')
    rows=[];previous=None;cost_reasons=Counter()
    with localcontext(Context(prec=80)):
        for record in reader:
            need(len(rows)<MAX_ROWS and None not in record,'source_row_bound')
            need(record['instrument']==pair and record['granularity']=='M1','source_row_identity')
            stamp=record['time']
            need(re.fullmatch(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.0{1,9})?(?:Z|\+00:00)',stamp),'source_UTC_minute')
            label=int(datetime.fromisoformat(stamp.replace('Z','+00:00')).timestamp())
            need(label>0 and label%60==0 and (previous is None or label>previous),'source_duplicate_or_unordered')
            need(label+60<=observed,'source_future_at_actual_observation')
            if 'complete' in record:need(record['complete'].lower() in ('true','1'),'source_incomplete_bar')
            mid={k:_number(record[k]) for k in ('open','high','low','close')}
            need(mid['low']<=mid['open']<=mid['high'] and mid['low']<=mid['close']<=mid['high'],'mid_ohlc_geometry')
            cost_reason=None;bid=ask=None
            try:
                if not all(record.get(side+'_'+k) for side in ('bid','ask') for k in ('open','high','low','close')):
                    cost_reason='bid_ask_OHLC_unavailable'
                else:
                    ba={side:{k:_number(record[side+'_'+k]) for k in ('open','high','low','close')} for side in ('bid','ask')}
                    need(all(p['low']<=p['open']<=p['high'] and p['low']<=p['close']<=p['high'] for p in ba.values()),'bid_ask_OHLC_invalid')
                    need(all(ba['bid'][k]<=ba['ask'][k] for k in ('open','high','low','close')),'bid_ask_crossed')
                    bid,ask=str(ba['bid']['close']),str(ba['ask']['close'])
            except ValueError as exc:cost_reason=str(exc)
            if cost_reason:cost_reasons[cost_reason]+=1
            rows.append(dict(label_epoch=label,price_epoch=label+60,close=float(mid['close']),close_text=record['close'],
                bid_close=bid,ask_close=ask,cost_unavailable_reason=cost_reason))
            previous=label
    need(bool(rows),'source_empty')
    return rows,dict(cost_reasons)

def segments(rows):
    result=[];begin=0
    for i in range(1,len(rows)):
        if rows[i]['label_epoch']-rows[i-1]['label_epoch']!=60:
            result.append((begin,i));begin=i
    result.append((begin,len(rows)))
    return result

def split_cutoffs(rows):
    lo,hi=rows[0]['price_epoch'],rows[-1]['price_epoch']
    return (int((lo+.6*(hi-lo))//60*60),int((lo+.8*(hi-lo))//60*60))

def split_name(origin,target,cutoffs):
    validation,test=cutoffs
    if origin<validation:return 'train' if target<validation else 'purged_train_overlap'
    if origin<test:return 'validation' if target<test else 'purged_validation_overlap'
    return 'test'

def build_dataset(rows,pair):
    """Features remain prefix-causal; all qualifying sessions share timeline cutoffs."""
    groups=segments(rows);cutoffs=split_cutoffs(rows)
    all_origins=[];ma_chunks=[];compact_chunks=[];names=None
    ledger={h:[] for h in HORIZONS};counts={h:Counter() for h in HORIZONS}
    for segment_id,(begin,end) in enumerate(groups):
        block=rows[begin:end]
        eligible=[i for i in range(203,len(block)) if block[i]['label_epoch']%300==0]
        for h in HORIZONS:
            counts[h]['source_rows']+=len(block);counts[h]['warmup_rows']+=min(203,len(block))
            counts[h]['non_anchor_warmed_rows']+=max(0,len(block)-203)-len(eligible)
        if not eligible:continue
        values=np.array([r['close'] for r in block],dtype=float)
        x,names=ma.build_feature_matrix(values,eligible,float(PIPS[pair]),'M1')
        epochs=[r['label_epoch'] for r in block];prices=dict(zip(epochs,values))
        session={t:epochs[0] for t in epochs}
        c=np.vstack([compact._features(prices,epochs,session,epochs[i],float(PIPS[pair])) for i in eligible])
        need(np.isfinite(x).all() and np.isfinite(c).all(),'nonfinite_feature_no_imputation')
        offset=len(all_origins)
        ma_chunks.append(x);compact_chunks.append(c)
        for n,i in enumerate(eligible):
            origin=block[i]
            momentum=(origin['close']-block[i-60]['close'])/origin['close']*10000
            all_origins.append(dict(global_row=begin+i,label_epoch=origin['label_epoch'],price_epoch=origin['price_epoch'],
                segment_id=segment_id,segment_start_price_epoch=block[0]['price_epoch'],momentum60_bps=momentum))
            for h in HORIZONS:
                if i+h>=len(block):
                    counts[h]['target_outside_contiguous_segment']+=1;continue
                target=block[i+h]
                need(target['price_epoch']==origin['price_epoch']+h*60,'non_exact_target')
                split=split_name(origin['price_epoch'],target['price_epoch'],cutoffs)
                counts[h][split]+=1
                if split.startswith('purged_'):continue
                ledger[h].append(dict(feature_index=offset+n,origin_index=begin+i,target_index=begin+i+h,
                    reference_epoch=origin['price_epoch'],target_epoch=target['price_epoch'],
                    split=split,segment_id=segment_id,segment_start_price_epoch=block[0]['price_epoch']))
    need(len(all_origins)<=MAX_ORIGINS,'origin_bound')
    matrices={'ma_causal643_ridge':np.vstack(ma_chunks).astype(np.float64) if ma_chunks else np.empty((0,643)),
              'compact_causal24_ridge':np.vstack(compact_chunks) if compact_chunks else np.empty((0,24))}
    matrices['compact24_plus_ma643_ridge']=np.column_stack((matrices['compact_causal24_ridge'],matrices['ma_causal643_ridge']))
    return dict(rows=rows,origins=all_origins,matrices=matrices,labels=ledger,counts={h:dict(c) for h,c in counts.items()},
        feature_names={'ma_causal643_ridge':list(names or ma.metadata()['feature_count']*['unavailable']),
                       'compact_causal24_ridge':list(COMPACT_NAMES),
                       'compact24_plus_ma643_ridge':['compact__'+x for x in COMPACT_NAMES]+list(names or 643*['unavailable'])},
        inventory=dict(rows=len(rows),segments=len(groups),qualifying_segments=sum(b-a>=204 for a,b in groups),
            maximum_segment_rows=max(b-a for a,b in groups),warmed_rows=sum(max(0,b-a-203) for a,b in groups),
            anchor_origins=len(all_origins),first_price_epoch=rows[0]['price_epoch'],last_price_epoch=rows[-1]['price_epoch'],
            validation_cutoff_epoch=cutoffs[0],test_cutoff_epoch=cutoffs[1]))

def targets(data,records):
    with localcontext(Context(prec=80)):
        return np.array([float((Decimal(data['rows'][r['target_index']]['close_text'])-
            Decimal(data['rows'][r['origin_index']]['close_text']))/Decimal(data['rows'][r['origin_index']]['close_text'])*10000) for r in records])

def select_model(x_train,y_train,x_validation,y_validation,alphas=ALPHAS):
    """Cannot receive test data. Means/scales and each candidate use training only."""
    need(len(x_train)==len(y_train) and len(x_validation)==len(y_validation),'fit_shape')
    need(all(np.isfinite(v).all() for v in (x_train,y_train,x_validation,y_validation)),'nonfinite_fit_input')
    scaler=StandardScaler().fit(x_train)
    train=scaler.transform(x_train);validation=scaler.transform(x_validation)
    candidates=[];models={}
    for alpha in alphas:
        fitted=Ridge(alpha=alpha,fit_intercept=True,solver='cholesky').fit(train,y_train)
        predicted=fitted.predict(validation)
        error=float(np.mean(np.abs(predicted-y_validation)))
        need(math.isfinite(error),'nonfinite_validation')
        candidates.append(dict(alpha=float(alpha),validation_mae_bps=error));models[float(alpha)]=fitted
    winner=min(candidates,key=lambda c:(c['validation_mae_bps'],-c['alpha']))
    fitted=models[winner['alpha']]
    return dict(alpha=winner['alpha'],validation_grid=candidates,mean=scaler.mean_.tolist(),scale=scaler.scale_.tolist(),
        variance=scaler.var_.tolist(),coefficients=fitted.coef_.tolist(),intercept=float(fitted.intercept_),
        train_rows=len(y_train),validation_rows=len(y_validation),feature_count=x_train.shape[1],
        constant_training_features=int(np.sum(scaler.var_==0)),fit_scope='initial_training_block_only',
        estimator='sklearn_Ridge_cholesky_unpenalized_intercept',probability_model=None)

def predict(model,x):
    result=((x-np.asarray(model['mean']))/np.asarray(model['scale']))@np.asarray(model['coefficients'])+model['intercept']
    need(np.isfinite(result).all(),'nonfinite_prediction');return result

def cost_rows(data,records,predicted):
    values=[]
    with localcontext(Context(prec=80)):
        for r,p in zip(records,predicted):
            a,b=data['rows'][r['origin_index']],data['rows'][r['target_index']]
            side=int(p>0)-int(p<0)
            reason=a['cost_unavailable_reason'] or b['cost_unavailable_reason']
            net=None
            if not reason and side:
                difference=(Decimal(b['bid_close'])-Decimal(a['ask_close'])) if side>0 else (Decimal(a['bid_close'])-Decimal(b['ask_close']))
                net=float(difference/Decimal(a['close_text'])*10000)
            values.append(dict(side=side,net_bps=net,reason=reason or ('neutral_no_position' if not side else None)))
    return values

def metrics(actual,predicted,costs):
    signs=np.sign(predicted);truth=np.sign(actual);eligible=(signs!=0)&(truth!=0)
    nets=[x['net_bps'] for x in costs if x['net_bps'] is not None]
    return dict(rows=len(actual),mae_bps=float(np.mean(np.abs(predicted-actual))),
        direction_correct=int(np.sum((signs==truth)&eligible)),direction_denominator=int(np.sum(eligible)),
        direction_accuracy=float(np.mean(signs[eligible]==truth[eligible])) if np.any(eligible) else None,
        neutral_predictions=int(np.sum(signs==0)),zero_outcomes=int(np.sum(truth==0)),brier=None,probability_available=False,
        cost_diagnostic_rows=len(nets),mean_bid_ask_net_bps=float(np.mean(nets)) if nets else None,
        positive_cost_rows=sum(x>0 for x in nets),cost_unavailable_reasons=dict(Counter(x['reason'] for x in costs if x['reason'])),
        independent_sample_size=None)

def paired_dependence(records,actual,model,baseline,model_cost,baseline_cost):
    """Descriptive matched dates; no row-IID resampling or model selection."""
    dates={}
    for index,r in enumerate(records):
        day=datetime.fromtimestamp(r['reference_epoch'],timezone.utc).date().isoformat()
        dates.setdefault(day,[]).append(index)
    out=[]
    def usable_cost(c):
        if c['net_bps'] is not None:return c['net_bps']
        return 0. if c['reason']=='neutral_no_position' else None
    for day,indices in sorted(dates.items()):
        ix=np.asarray(indices);truth=np.sign(actual[ix]);left=np.sign(model[ix]);right=np.sign(baseline[ix])
        eligible=(truth!=0)&(left!=0)&(right!=0)
        costs=[(usable_cost(model_cost[i]),usable_cost(baseline_cost[i])) for i in indices]
        cost_deltas=[a-b for a,b in costs if a is not None and b is not None]
        mae_gain=np.abs(baseline[ix]-actual[ix])-np.abs(model[ix]-actual[ix])
        out.append(dict(UTC_date=day,rows=len(indices),mae_improvement_bps=float(np.mean(mae_gain)),
            mae_improvement_sum_bps=float(np.sum(mae_gain)),
            direction_paired_denominator=int(np.sum(eligible)),
            direction_accuracy_delta=float(np.mean((left[eligible]==truth[eligible]).astype(float)-
                (right[eligible]==truth[eligible]).astype(float))) if np.any(eligible) else None,
            mean_cost_delta_bps=float(np.mean(cost_deltas)) if cost_deltas else None,cost_paired_denominator=len(cost_deltas)))
    ci=None
    if len(out)>=10:
        rng=np.random.default_rng(20260909);g=np.array([x['mae_improvement_sum_bps'] for x in out]);n=np.array([x['rows'] for x in out])
        draws=rng.integers(0,len(out),size=(1000,len(out)))
        sample=g[draws].sum(axis=1)/n[draws].sum(axis=1)
        bounds=np.quantile(sample,[.025,.975])
        ci=dict(low=float(bounds[0]),high=float(bounds[1]),resamples=1000,seed=20260909,
                method='UTC_date_cluster_resampling_weighted_by_retained_rows')
    return dict(per_date=out,UTC_date_count=len(out),MAE_improvement_interval95=ci,
        interval_unavailable_reason='fewer_than10_test_UTC_dates' if ci is None else None,
        direction_comparison='same_nonzero_truth_and_both_nonneutral_predictions; zero_baseline_has_no_direction',
        cost_comparison='same_valid_bid_ask_domain; neutral_no_position_return0; no execution claim',
        positive_MAE_delta_means='model_lower_absolute_error',positive_cost_delta_means='model_higher_hypothetical_net',
        independent_sample_size=None,multiple_comparisons_adjusted=False,used_for_model_selection=False)

def run(design_path,output):
    began=now();plan=load_json(design_path)
    need(plan['policy']==POLICY and plan['pre_evaluation'] is True,'design_policy_changed')
    need(plan['implementation']==binding(Path(__file__)) and plan['test_source']==binding(BASE/'test_ma_historical_comparison_v1.py'),'design_source_changed')
    need(plan['source_bindings']==source_bindings() and plan['dependency_versions']==dependencies(),'source_or_dependency_changed')
    capture_path=Path(plan['capture_manifest']['path'])
    need(binding(capture_path)==plan['capture_manifest'],'capture_manifest_changed')
    output=Path(output).absolute();need(output.parent==BASE and not output.exists(),'unique_external_output_required')
    output.mkdir()
    capture=load_json(capture_path);datasets={};selections={};inventories={}
    def deadline():need(now()-began<=MAX_RUN_SEC,'comparison_runtime_budget_exceeded')
    with threadpool_limits(limits=1):
        for pair in PIPS:
            source=capture['sources'][pair];path=Path(source['retained_path'])
            need(path.parent==capture_path.parent/'private_sources' and path.name==pair+'_M1.csv','private_source_path_mismatch')
            raw=path.read_bytes();need(sha(raw)==source['retained_sha256'] and len(raw)==source['retained_bytes'],'retained_source_changed')
            need(source['source_stat_unchanged'] is True and source['read_started_epoch']<=source['read_completed_epoch'],'source_capture_clock')
            rows,cost_missing=read_rows(raw,pair,source['read_completed_epoch']);data=build_dataset(rows,pair)
            datasets[pair]=data;inventories[pair]={**data['inventory'],'cost_source_missingness':cost_missing,'horizon_counts':data['counts'],
                'source':binding(path),'original_ingestion_availability_proven':False,'feature_names':data['feature_names']}
            for h in HORIZONS:
                records=data['labels'][h];by={s:[r for r in records if r['split']==s] for s in MINIMUM}
                key=pair+'/'+str(h)
                shortage={s:len(by[s]) for s in MINIMUM if len(by[s])<MINIMUM[s]}
                if shortage:
                    selections[key]=dict(status='insufficient_history',split_counts={s:len(v) for s,v in by.items()},minimum=MINIMUM,shortage=shortage);continue
                selections[key]=dict(status='selected',split_counts={s:len(v) for s,v in by.items()},models={})
                for family,x in data['matrices'].items():
                    train,val=by['train'],by['validation']
                    selections[key]['models'][family]=select_model(x[[r['feature_index'] for r in train]],targets(data,train),
                        x[[r['feature_index'] for r in val]],targets(data,val))
                    deadline()
            print(json.dumps({'phase':'training_validation_complete','pair':pair,'origins':data['inventory']['anchor_origins']}),flush=True)
        # Durable pre-test selections: no final-test prediction or score above.
        selected_record=write_new(output/'PRE_TEST_SELECTIONS.json',dict(design=binding(design_path),source_bindings=source_bindings(),
            selected_epoch=now(),final_test_evaluated=False,selections=selections,inventory=inventories,**FLAGS))
        scores={};predictions=[];dependence={}
        for pair,data in datasets.items():
            for h in HORIZONS:
                key=pair+'/'+str(h);selection=selections[key]
                if selection['status']!='selected':continue
                records=[r for r in data['labels'][h] if r['split']=='test'];actual=targets(data,records)
                models={family:predict(model,data['matrices'][family][[r['feature_index'] for r in records]]) for family,model in selection['models'].items()}
                models['zero_return']=np.zeros(len(records))
                models['last60m_momentum_scaled_to_horizon']=np.array([data['origins'][r['feature_index']]['momentum60_bps']*h/60 for r in records])
                scores[key]={};all_costs={}
                for family,pred in models.items():
                    costs=cost_rows(data,records,pred);all_costs[family]=costs;scores[key][family]=metrics(actual,pred,costs)
                    for r,y,p,cost in zip(records,actual,pred,costs):
                        predictions.append(dict(instrument=pair,horizon_minutes=h,family=family,**r,
                            reference_mid_close=data['rows'][r['origin_index']]['close_text'],target_mid_close=data['rows'][r['target_index']]['close_text'],
                            actual_return_bps=float(y),prediction_bps=float(p),cost_diagnostic=cost))
                dependence[key]={}
                for family in POLICY['families']:
                    dependence[key][family]={}
                    for base in ('zero_return','last60m_momentum_scaled_to_horizon','compact_causal24_ridge'):
                        if base==family:continue
                        dependence[key][family][base]=paired_dependence(records,actual,models[family],models[base],all_costs[family],all_costs[base])
                deadline()
        predict_record=write_new(output/'FINAL_TEST_PREDICTIONS.json',predictions)
        dependence_record=write_new(output/'FINAL_TEST_DATE_BLOCK_DIAGNOSTICS.json',dependence)
    need(source_bindings()==plan['source_bindings'],'final_source_bindings_changed')
    for pair in PIPS:need(binding(capture['sources'][pair]['retained_path'])['sha256']==capture['sources'][pair]['retained_sha256'],'final_capture_changed')
    return write_new(output/'MA_COMPARISON_REPORT.json',dict(version=VERSION,status='completed',
        started_epoch=began,completed_epoch=now(),duration_sec=now()-began,design=binding(design_path),
        source_bindings=source_bindings(),selection_receipt=selected_record,predictions=predict_record,date_block_diagnostics=dependence_record,
        inventory=inventories,selections=selections,final_test_scores=scores,**FLAGS,
        limits=['Retrospective engineering; original source availability and midpoint ingestion are unknown.',
          'No old fitted weights or opaque model files loaded. Corrected features require this new fit; no compatibility claim.',
          'Only contiguous-session warmed grid origins qualify; counts/purges are retained and no gaps are filled.',
          'Alpha selected using validation MAE; final test evaluated once with frozen initial-training scaler/weights.',
          'Repeated horizons, nearby origins, currencies and market sessions are dependent; independent sample size is not inferred.',
          'Costs are bid/ask candle-close diagnostics without original tradeability, entry delay, slippage, financing or USD sizing.',
          'No calibrated probability model, Brier score, current forecast, broker order or deployment is created.']))

def main():
    parser=argparse.ArgumentParser()
    sub=parser.add_subparsers(dest='mode',required=True)
    p=sub.add_parser('prepare');p.add_argument('--capture',required=True);p.add_argument('--capture-sha256',required=True);p.add_argument('--output',required=True)
    p=sub.add_parser('run');p.add_argument('--design',required=True);p.add_argument('--output',required=True)
    args=parser.parse_args()
    if args.mode=='prepare':result=write_new(args.output,design(Path(args.capture),args.capture_sha256))
    else:
        out=Path(args.output).absolute();existed_before=out.exists()
        try:result=run(Path(args.design),Path(args.output))
        except Exception as exc:
            if not existed_before and out.parent==BASE and out.is_dir() and not (out/'COMPARISON_FAILED.json').exists():
                write_new(out/'COMPARISON_FAILED.json',dict(status='failed',error_type=type(exc).__name__,
                    reason=str(exc)[:500],observed_epoch=now(),implementation=binding(Path(__file__)),
                    partial_evidence_preserved=True,**FLAGS))
            raise
    print(json.dumps(result))

if __name__=='__main__':main()
