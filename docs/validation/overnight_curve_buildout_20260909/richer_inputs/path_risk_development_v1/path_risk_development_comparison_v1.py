"""Fixed train-only risk baselines on already-inspected retrospective history.

No learned direction, model selection, old weights, runtime writes or broker
access. Validation and test-named partitions are descriptive development only.
"""
import argparse
from collections import Counter
from datetime import datetime,timezone
import hashlib
import json
import math
import os
from pathlib import Path
import sys
import time

import numpy as np

BASE=Path(__file__).resolve().parent
TRAD=BASE.parents[2]/'trad'
sys.path.insert(0,str(TRAD))
import oanda_m1_path_risk_labels_v1 as labels

SCHEMA='m1_path_risk_development_comparison_v1_20260909'
PIPS={'EUR_USD':'0.0001','GBP_USD':'0.0001','USD_JPY':'0.01'}
Q=(.1,.5,.9)
MINIMUM={'train':256,'validation':64,'test':64}
SOURCE_FILES=('oanda_m1_path_risk_labels_v1.py',)
POLICY=dict(schema_version=SCHEMA,
    evidence_class='retrospective_development_after_MA_final_period_inspection_not_untouched_holdout',
    instruments=list(PIPS),pip_sizes=PIPS,horizons_minutes=list(labels.HORIZONS),
    labels=list(labels.ALL_LABELS),quantile_targets=list(labels.CONTINUOUS_LABELS),
    origin='completed_M1_mid_close_label_plus60; historical_ingestion_and_midpoint_provenance_unknown',
    sampling='all_contiguous_sessions;204_real_warm_rows;UTC_label_mod300_equal0',
    target='exact_same_session_origin_price_epoch_plus_native_horizon',
    path='exclude_origin_OHLC;complete_subsequent_minutes;include_initial_bidask_liquidation_value_for_cost_excursions',
    missing='no_fill_no_inferred_bidask_or_spread;path_and_endpoint_missingness_explicit',
    split='same_per_pair_actual_timeline60/20/20_as_MA_comparison',
    purge='target_strictly_before_next_block_cutoff; all_validation_and_test_results_development_only',
    methods=['training_empirical','training_empirical_volatility_scaled'],
    quantiles=list(Q),quantile_method='inverted_cdf',
    volatility='past60_consecutive_real_log_returns_RMS_times_sqrt_horizon_times10000',
    zero_volatility='both_quantile_methods_withheld_on_common_positive_scale_support;never_artificial_floor',
    training_support='same_label_valid_positive_scale_training_rows_for_both_methods',
    fitting='training_quantiles_only;no_validation_refit_no_hyperparameter_or_winner_selection',
    opportunity_probability='training_prevalence_only_of_either_side_endpoint_positive;not_direction',
    minimum_rows=MINIMUM,maximum_rows_per_pair=labels.MAX_ROWS,maximum_source_bytes_per_pair=labels.MAX_BYTES,
    maximum_origins_per_pair=20000,maximum_runtime_sec=600,
    score='pinball_each_quantile,median_MAE,80percent_interval_coverage_width,lower_upper_miss',
    dependence=dict(per_UTC_date=True,independent_sample_size=None,across_pair_pooling=False,
        CI='unadjusted_descriptive_date_cluster95_interval_for_pinball_improvement_only_if10_dates',
        seed=20260909,resamples=1000,if_pooled='same_date_draw_across_pairs'),
    observed_tradeability=False,broker_fills=False,calibrated_probability_claim=False,
    selected_for_promotion=False,**labels.FLAGS)

def need(ok,reason):
    if not ok:raise ValueError(reason)

def sha(raw):return hashlib.sha256(raw).hexdigest()
def binding(path):
    path=Path(path);raw=path.read_bytes();return dict(path=str(path),bytes=len(raw),sha256=sha(raw))
def load(path):return json.loads(Path(path).read_bytes())
def write_new(path,value):
    path=Path(path);path.parent.mkdir(exist_ok=True,parents=True)
    with path.open('xb') as f:
        f.write(json.dumps(value,indent=2,sort_keys=True,allow_nan=False).encode()+b'\n');f.flush();os.fsync(f.fileno())
    return binding(path)
def sources():return {name:binding(TRAD/name)['sha256'] for name in SOURCE_FILES}
def deps():return dict(python=sys.version,numpy=np.__version__)
def utc(t):return datetime.fromtimestamp(t,timezone.utc).isoformat()

def retained_protocol(path,began):
    path=Path(path);raw=path.read_bytes();plan=json.loads(raw)
    identity=dict(path=str(path),bytes=len(raw),sha256=sha(raw))
    created=plan.get('created_epoch')
    need(type(created) in (int,float) and math.isfinite(created) and 0<created<=began,'protocol_not_frozen_before_run')
    need(binding(path)==identity,'protocol_changed_during_initial_read')
    return plan,identity

def verify_protocol_identity(identity):
    need(binding(identity['path'])==identity,'frozen_protocol_changed_during_development')

def freeze(capture_path,expected_sha):
    need(binding(capture_path)['sha256']==expected_sha,'capture_manifest_binding')
    captured=load(capture_path)
    need(captured.get('schema')=='ma_history_capture_20260909' and set(captured['sources'])==set(PIPS), 'capture_inventory')
    return dict(schema_version=SCHEMA,policy=POLICY,created_epoch=time.time(),pre_scores=True,
        implementation=binding(Path(__file__)),test_source=binding(BASE/'test_path_risk_development_comparison_v1.py'),
        source_bindings=sources(),label_tests=binding(TRAD/'test_oanda_m1_path_risk_labels_v1.py'),
        dependency_versions=deps(),capture_manifest=binding(capture_path),
        prior_inspected_MA_result=binding(BASE.parent/'ma_comparison_v1/MA_COMPARISON_COMPACT_ASSESSMENT_20260909.json'),
        reuse_review=binding(BASE.parent/'direction_neutral_risk_review/DIRECTION_NEUTRAL_CURVE_RISK_REUSE_REVIEW_20260909.json'),**labels.FLAGS)

def cutoffs(rows):
    lo,hi=rows[0]['price_epoch'],rows[-1]['price_epoch']
    return int((lo+.6*(hi-lo))//60*60),int((lo+.8*(hi-lo))//60*60)

def block_name(origin,target,bounds):
    validation,test=bounds
    if origin<validation:return 'train' if target<validation else 'purged_train_overlap'
    if origin<test:return 'validation' if target<test else 'purged_validation_overlap'
    return 'test'

def dataset(rows,deadline=lambda:None):
    bounds=cutoffs(rows);segments=labels.contiguous_segments(rows)
    records={h:[] for h in labels.HORIZONS};counts={h:Counter() for h in labels.HORIZONS}
    origins=0;processed=0
    for segment_id,(begin,end) in enumerate(segments):
        block=rows[begin:end]
        eligible=[i for i in range(203,len(block)) if block[i]['label_epoch']%300==0]
        origins+=len(eligible)
        for h in labels.HORIZONS:
            counts[h]['warmup_rows']+=min(203,len(block))
            counts[h]['nonanchor_warmed_rows']+=max(0,len(block)-203)-len(eligible)
        for i in eligible:
            processed+=1
            if processed%100==0:deadline()
            for h in labels.HORIZONS:
                if i+h>=len(block):
                    counts[h]['target_outside_contiguous_segment']+=1;continue
                row=labels.label_origin(block,i,h)
                split=block_name(row['reference_price_epoch'],row['target_price_epoch'],bounds)
                counts[h][split]+=1
                if split.startswith('purged'):continue
                scale,reason=labels.past_volatility_scale(block,i,h)
                record=dict(reference_epoch=row['reference_price_epoch'],target_epoch=row['target_price_epoch'],
                    reference_label_epoch=row['reference_label_epoch'],origin_global_row=begin+i,
                    target_global_row=begin+i+h,segment_id=segment_id,split=split,
                    scale_bps=scale,scale_unavailable_reason=reason,values=row['values'],label_missingness=row['unavailable_reasons'])
                records[h].append(record)
        deadline()
    need(origins<=POLICY['maximum_origins_per_pair'],'origin_bound')
    return dict(records=records,counts={h:dict(c) for h,c in counts.items()},inventory=dict(source_rows=len(rows),
        first_price_epoch=rows[0]['price_epoch'],last_price_epoch=rows[-1]['price_epoch'],segments=len(segments),
        qualifying_segments=sum(b-a>=204 for a,b in segments),maximum_segment_rows=max(b-a for a,b in segments),
        warmed_rows=sum(max(0,b-a-203) for a,b in segments),anchor_origins=origins,
        validation_cutoff_epoch=bounds[0],test_cutoff_epoch=bounds[1]))

def fit_quantiles(y,scale):
    y=np.asarray(y,float);scale=np.asarray(scale,float)
    need(y.ndim==1 and scale.shape==y.shape and len(y)>0,'fit_shape')
    need(np.isfinite(y).all() and np.isfinite(scale).all() and (scale>0).all(),'finite_positive_training_support')
    return dict(training_rows=len(y),quantiles=list(Q),method='inverted_cdf',
        empirical=np.quantile(y,Q,method='inverted_cdf').tolist(),
        volatility_normalized=np.quantile(y/scale,Q,method='inverted_cdf').tolist(),
        fit_scope='training_only_common_support',original_probability_available=False)

def predictions(model,scale):
    scale=np.asarray(scale,float)
    need(scale.ndim==1 and np.isfinite(scale).all() and (scale>0).all(),'prediction_scale_support')
    return {'training_empirical':np.tile(model['empirical'],(len(scale),1)),
        'training_empirical_volatility_scaled':scale[:,None]*np.asarray(model['volatility_normalized'])[None,:]}

def scores(y,pred):
    y=np.asarray(y,float);pred=np.asarray(pred,float)
    need(pred.shape==(len(y),3) and len(y)>0 and np.isfinite(pred).all() and np.isfinite(y).all(), 'score_shape')
    need((np.diff(pred,axis=1)>=0).all(),'quantile_crossing')
    error=y[:,None]-pred
    pinball=np.maximum(np.asarray(Q)*error,(np.asarray(Q)-1)*error)
    below=y<pred[:,0];above=y>pred[:,2]
    return dict(rows=len(y),quantiles=list(Q),mean_pinball_by_quantile=pinball.mean(axis=0).tolist(),
        mean_pinball_across_quantiles=float(pinball.mean()),median_MAE_bps=float(np.mean(np.abs(error[:,1]))),
        interval_coverage=float(np.mean(~below&~above)),nominal_interval_coverage=.8,
        mean_interval_width_bps=float(np.mean(pred[:,2]-pred[:,0])),lower_miss_rate=float(np.mean(below)),
        upper_miss_rate=float(np.mean(above)),upper_tail_excess_mean_bps=float(np.mean(np.maximum(y-pred[:,2],0))),
        independent_sample_size=None,calibrated_coverage_claim=False)

def paired_dates(records,y,preds):
    groups={}
    for i,r in enumerate(records):groups.setdefault(utc(r['reference_epoch'])[:10],[]).append(i)
    result=[]
    for day,index in sorted(groups.items()):
        ix=np.asarray(index);m={name:scores(y[ix],p[ix]) for name,p in preds.items()}
        gain=m['training_empirical']['mean_pinball_across_quantiles']-m['training_empirical_volatility_scaled']['mean_pinball_across_quantiles']
        result.append(dict(UTC_date=day,rows=len(index),methods=m,scaled_pinball_improvement=gain,improvement_sum=gain*len(index)))
    interval=None
    if len(result)>=10:
        rng=np.random.default_rng(20260909);ix=rng.integers(0,len(result),size=(1000,len(result)))
        sums=np.array([r['improvement_sum'] for r in result]);n=np.array([r['rows'] for r in result])
        sample=sums[ix].sum(axis=1)/n[ix].sum(axis=1);low,high=np.quantile(sample,[.025,.975])
        interval=dict(low=float(low),high=float(high),seed=20260909,resamples=1000,adjusted_for_multiple_comparisons=False)
    return dict(per_UTC_date=result,UTC_date_count=len(result),scaled_pinball_improvement_interval95=interval,
        interval_unavailable_reason='fewer_than10_dates' if interval is None else None,
        independent_sample_size=None,used_for_selection=False)

def run(frozen_path,output):
    began=time.time();plan,protocol_identity=retained_protocol(frozen_path,began)
    need(plan['pre_scores'] is True and plan['policy']==POLICY,'protocol_changed')
    need(plan['implementation']==binding(Path(__file__)) and plan['test_source']==binding(BASE/'test_path_risk_development_comparison_v1.py'),'implementation_changed')
    need(plan['source_bindings']==sources() and plan['label_tests']==binding(TRAD/'test_oanda_m1_path_risk_labels_v1.py') and plan['dependency_versions']==deps(),'source_or_dependency_changed')
    capture_path=Path(plan['capture_manifest']['path']);need(binding(capture_path)==plan['capture_manifest'],'capture_manifest_changed')
    output=Path(output).absolute();need(output.parent==BASE and not output.exists(),'new_external_output_required');output.mkdir()
    capture=load(capture_path);datasets={};fitted={};inventories={};support={}
    def deadline():need(time.time()-began<=POLICY['maximum_runtime_sec'],'development_runtime_budget_exceeded')
    for pair in PIPS:
        src=capture['sources'][pair];path=Path(src['retained_path'])
        need(path.parent==capture_path.parent/'private_sources' and path.name==pair+'_M1.csv','private_source_path')
        raw=path.read_bytes();need(sha(raw)==src['retained_sha256'] and len(raw)==src['retained_bytes'],'private_source_binding')
        need(src['source_stat_unchanged'] is True and src['read_started_epoch']<=src['read_completed_epoch'],'source_observation_clock')
        rows,audit=labels.parse_csv(raw,pair,observed_epoch=src['read_completed_epoch']);data=dataset(rows,deadline)
        del rows,raw
        datasets[pair]=data;inventories[pair]=dict(**data['inventory'],source=binding(path),audit=audit,counts=data['counts'])
        for h in labels.HORIZONS:
            allrows=data['records'][h]
            for label in labels.CONTINUOUS_LABELS:
                key=f'{pair}/{h}/{label}'
                by={split:[r for r in allrows if r['split']==split and r['values'][label] is not None and r['scale_bps'] is not None] for split in MINIMUM}
                support[key]={split:dict(all_origins=sum(r['split']==split for r in allrows),common_quantile_support=len(by[split]),
                    label_unavailable=dict(Counter(r['label_missingness'].get(label) for r in allrows if r['split']==split and r['values'][label] is None)),
                    scale_unavailable=dict(Counter(r['scale_unavailable_reason'] for r in allrows if r['split']==split and r['scale_bps'] is None))) for split in MINIMUM}
                if any(len(by[split])<minimum for split,minimum in MINIMUM.items()):
                    fitted[key]=dict(status='insufficient_predeclared_support');continue
                train=by['train'];model=fit_quantiles([r['values'][label] for r in train],[r['scale_bps'] for r in train])
                model.update(status='fitted_training_quantiles',training_reference_max_epoch=max(r['reference_epoch'] for r in train),
                    training_target_max_epoch=max(r['target_epoch'] for r in train),training_origin_sha256=sha(json.dumps(train,sort_keys=True,separators=(',',':')).encode()))
                fitted[key]=model
            binary=[r for r in allrows if r['split']=='train' and r['values']['either_side_endpoint_positive'] is not None]
            fitted[f'{pair}/{h}/either_side_endpoint_positive']=dict(status='training_prevalence_only' if len(binary)>=MINIMUM['train'] else 'insufficient_predeclared_support',training_rows=len(binary),
                probability=float(np.mean([r['values']['either_side_endpoint_positive'] for r in binary])) if len(binary)>=MINIMUM['train'] else None,
                event='either_side_endpoint_after_observed_spread_positive_not_direction')
        deadline()
    # All train-only fits are durable before any validation/test score calculation.
    verify_protocol_identity(protocol_identity)
    fitted_binding=write_new(output/'PRE_SCORE_TRAINING_BASELINES.json',dict(created_epoch=time.time(),
        frozen_protocol=protocol_identity,fits=fitted,support=support,**labels.FLAGS))
    cells=[];binary_reports=[];artifacts=[]
    for pair,data in datasets.items():
        artifacts.append(write_new(output/(pair+'_retained_labels.json'),dict(instrument=pair,records=data['records'],**labels.FLAGS)))
        for h in labels.HORIZONS:
            allrows=data['records'][h]
            for label in labels.CONTINUOUS_LABELS:
                key=f'{pair}/{h}/{label}';model=fitted[key]
                for split in ('validation','test'):
                    rows=[r for r in allrows if r['split']==split and r['values'][label] is not None and r['scale_bps'] is not None]
                    item=dict(instrument=pair,horizon_minutes=h,label=label,partition=split,
                        evidence_class=POLICY['evidence_class'],support=support[key][split],status=model['status'])
                    if model['status']=='fitted_training_quantiles':
                        y=np.array([r['values'][label] for r in rows]);pred=predictions(model,[r['scale_bps'] for r in rows])
                        item.update(status='development_evaluated',methods={name:scores(y,p) for name,p in pred.items()},dependence=paired_dates(rows,y,pred))
                    cells.append(item)
            for split in ('validation','test'):
                rows=[r for r in allrows if r['split']==split and r['values']['either_side_endpoint_positive'] is not None]
                p=fitted[f'{pair}/{h}/either_side_endpoint_positive']['probability'];truth=np.array([r['values']['either_side_endpoint_positive'] for r in rows],float)
                binary_reports.append(dict(instrument=pair,horizon_minutes=h,partition=split,rows=len(rows),
                    event='either_side_endpoint_positive_not_direction',training_prior_probability=p,
                    observed_prevalence=float(truth.mean()) if len(rows) else None,
                    status='development_baseline_evaluated' if len(rows)>=MINIMUM[split] and p is not None else 'insufficient_predeclared_support',
                    prior_Brier=float(np.mean((truth-p)**2)) if len(rows)>=MINIMUM[split] and p is not None else None,
                    learned_probability_model=False,net_trading_return_claimed=False,
                    missing_cost_origins=sum(r['split']==split and r['values']['either_side_endpoint_positive'] is None for r in allrows),independent_sample_size=None))
        deadline()
    need(plan['source_bindings']==sources() and plan['implementation']==binding(Path(__file__)),'source_changed_during_development')
    verify_protocol_identity(protocol_identity)
    need(plan['capture_manifest']==binding(capture_path),'manifest_changed_during_development')
    for pair,inventory in inventories.items():
        current=binding(inventory['source']['path']);original=capture['sources'][pair]
        need(current==inventory['source'] and current['sha256']==original['retained_sha256'] and current['bytes']==original['retained_bytes'],
            'immutable_history_changed_during_development')
    result=dict(schema_version=SCHEMA,status='retrospective_development_completed',started_epoch=began,completed_epoch=time.time(),
        frozen_protocol=protocol_identity,pre_score_baselines=fitted_binding,source_bindings=sources(),
        dependency_versions=deps(),source_inventories=inventories,cells=cells,opportunity_prevalence_reports=binary_reports,
        artifacts=artifacts,policy=POLICY,independent_sample_size=None,
        limitations=['Same history and final time period were inspected previously; all outputs here are development, not fresh confirmation.',
            'No family, parameter, threshold, direction or management policy was selected from these results.',
            'Retained archive maturity is observed at capture, but original historical arrival and midpoint convention are unproven.',
            'OHLC envelopes do not determine barrier ordering, executable extreme price or broker fills.',
            'The baseline is a point-in-time original-horizon risk distribution; no conditional later management recalibration.'],**labels.FLAGS)
    out=write_new(output/'PATH_RISK_DEVELOPMENT_REPORT.json',result)
    return out

def main():
    ap=argparse.ArgumentParser();sub=ap.add_subparsers(dest='mode',required=True)
    f=sub.add_parser('freeze');f.add_argument('--capture',required=True);f.add_argument('--capture-sha256',required=True);f.add_argument('--output',required=True)
    r=sub.add_parser('run');r.add_argument('--protocol',required=True);r.add_argument('--output',required=True)
    args=ap.parse_args()
    result=write_new(args.output,freeze(args.capture,args.capture_sha256)) if args.mode=='freeze' else run(args.protocol,args.output)
    print(json.dumps(result))

if __name__=='__main__':main()
