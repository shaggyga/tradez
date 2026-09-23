"""Descriptive matched diagnostics from one already verified frozen assessment.

No SQLite, network, fitting or scorer invocation. The original economic scores
remain the authority; this supplement describes their common denominator and
the pre-issue estimates retained in the same hash-bound logical snapshots.
"""
from collections import Counter
from decimal import Context,Decimal,localcontext
from datetime import datetime,timezone
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import time
import zlib

BASE=Path(__file__).resolve().parent
ASSESSMENT=BASE/'actual_assessment_001'
REPORT_SHA='cd435169e303d1b867787f58a35a6ebbc54932e4358e5c1541a6493597ee88e3'
FAMILY='ridge_price_news_v1'
COMPARATORS=('matched_price_only','neutral_news_ablation')


def need(ok,reason):
    if not ok:raise ValueError(reason)


def digest(raw):return hashlib.sha256(raw).hexdigest()
def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()


def number(value):
    need(type(value) in (str,int,float,Decimal),'numeric_value_required')
    result=Decimal(str(value));need(result.is_finite(),'finite_value_required')
    return result


def mean(values):return None if not values else str(sum(values,Decimal(0))/len(values))


def correlation(x,y):
    need(len(x)==len(y),'correlation_denominator')
    if len(x)<2:return None
    mx=sum(x,Decimal(0))/len(x);my=sum(y,Decimal(0))/len(y)
    dx=[v-mx for v in x];dy=[v-my for v in y]
    xx=sum((v*v for v in dx),Decimal(0));yy=sum((v*v for v in dy),Decimal(0))
    return None if not xx or not yy else str(sum((a*b for a,b in zip(dx,dy)),Decimal(0))/(xx*yy).sqrt())


def read_bound(path,expected,cap):
    path=path.absolute();need(path.is_relative_to(BASE),'external_evidence_path')
    for part in reversed([path,*path.parents]):
        info=part.lstat();need(not stat.S_ISLNK(info.st_mode) and not getattr(info,'st_file_attributes',0)&1024,'evidence_reparse')
    with path.open('rb') as stream:raw=stream.read(cap+1)
    need(len(raw)<=cap and digest(raw)==expected,'evidence_hash_or_bound')
    return raw


def build_record(row,decision,instrument):
    need(row['instrument']==decision['instrument']==instrument,'matched_instrument')
    need(all(row[k]==decision[k] for k in ('decision_id','reference_epoch','target_epoch')),'matched_original_identity')
    need(len(decision['forecasts'])==1,'singleton_original_forecast')
    forecast=decision['forecasts'][0]
    need(forecast['family']==FAMILY and forecast['side']==row['scores'][FAMILY]['side'],'matched_original_forecast')
    need(all(forecast[k]==row[k] for k in ('instrument','reference_epoch','target_epoch')),'matched_original_forecast_clock')
    ablation=row['ablation_comparison']
    need(ablation['target_epoch']==row['target_epoch'] and all(ablation[k+'_quote_id']==row[k]['quote_id'] for k in ('reference','entry','target')),'matched_economic_endpoint')
    predictions={FAMILY:number(forecast['predicted_return_bps'])}
    scores={FAMILY:row['scores'][FAMILY]}
    for name in COMPARATORS:
        scores[name]=ablation['scores'][name]
        predictions[name]=number(scores[name]['predicted_return_bps'])
        need(scores[name]['probability_up'] is None and scores[name]['brier'] is None,'no_comparator_probability_claim')
        field=scores[name]['retained_diagnostic_field']
        pips=number(forecast['diagnostics'][field])
        midpoint=(number(row['reference']['bid'])+number(row['reference']['ask']))/2
        need(pips==number(scores[name]['predicted_signed_pips']) and
            10000*pips*number(decision['pip_size'])/midpoint==predictions[name],'retained_preissue_comparator_prediction')
    actual=number(row['actual_return_bps'])
    for name in (FAMILY,*COMPARATORS):
        need(scores[name]['side']==(1 if predictions[name]>0 else -1 if predictions[name]<0 else 0),'retained_direction_reconciliation')
    return dict(instrument=instrument,decision_id=row['decision_id'],reference_epoch=row['reference_epoch'],target_epoch=row['target_epoch'],
        news_capture_sha256=forecast['news_capture_sha256'],feature_cutoff_epoch=forecast['feature_cutoff_epoch'],
        actual=actual,predictions=predictions,scores=scores)


def summarize(records):
    with localcontext(Context(prec=80)):
        need(records and len({r['decision_id'] for r in records})==len(records),'unique_nonempty_completed_decisions')
        actual=[r['actual'] for r in records];joint=[r['predictions'][FAMILY] for r in records]
        comparisons={}
        for name in COMPARATORS:
            predicted=[r['predictions'][name] for r in records]
            matrix=Counter((r['scores'][FAMILY]['side'],r['scores'][name]['side']) for r in records)
            groups={}
            for match in (True,False):
                selected=[r for r in records if (r['scores'][FAMILY]['side']==r['scores'][name]['side']) is match]
                groups['same_direction' if match else 'different_direction']=dict(count=len(selected),
                    mean_joint_minus_comparator_net_bps=mean([number(r['scores'][FAMILY]['net_bps'])-number(r['scores'][name]['net_bps']) for r in selected]),
                    mean_joint_minus_comparator_absolute_error_bps=mean([number(r['scores'][FAMILY]['absolute_error_bps'])-number(r['scores'][name]['absolute_error_bps']) for r in selected]))
            difference=[a-b for a,b in zip(joint,predicted)]
            comparisons[name]=dict(common_valid_denominator=len(records),direction_pairs=[dict(joint_side=a,comparator_side=b,count=matrix[a,b]) for a in (-1,0,1) for b in (-1,0,1)],
                same_direction_count=groups['same_direction']['count'],different_direction_count=groups['different_direction']['count'],groups=groups,
                signed_prediction_pearson=correlation(joint,predicted),signed_error_pearson=correlation([a-y for a,y in zip(joint,actual)],[a-y for a,y in zip(predicted,actual)]),
                mean_joint_minus_comparator_prediction_bps=mean(difference),mean_absolute_prediction_difference_bps=mean([abs(v) for v in difference]),
                prediction_difference_actual_return_pearson=correlation(difference,actual),
                mean_joint_minus_comparator_net_bps=mean([number(r['scores'][FAMILY]['net_bps'])-number(r['scores'][name]['net_bps']) for r in records]),
                mean_joint_minus_comparator_absolute_error_bps=mean([number(r['scores'][FAMILY]['absolute_error_bps'])-number(r['scores'][name]['absolute_error_bps']) for r in records]))
        return dict(valid_original_completed_decisions=len(records),comparisons=comparisons,
            repeated_input_descriptors=dict(distinct_news_capture_hashes=len({r['news_capture_sha256'] for r in records}),
                distinct_price_feature_cutoff_epochs=len({r['feature_cutoff_epoch'] for r in records}),independent_sample_size=None),
            exact_zero_actual_return_count=sum(v==0 for v in actual),
            original_event_identity_sha256=digest(canonical(sorted((r['instrument'],r['decision_id'],r['reference_epoch'],r['target_epoch']) for r in records))))


def run():
    started=time.time();source=Path(__file__).read_bytes();report_path=ASSESSMENT/'V3_COMPLETED_PERFORMANCE_20260909.json'
    report=json.loads(read_bound(report_path,REPORT_SHA,2*1024*1024));need(report['status']=='passed' and report['ledgers']==68,'verified_original_assessment_required')
    records=[];bindings=[];read_bytes=0
    with localcontext(Context(prec=80)):
        for reference in report['ledger_results']:
            pair=reference['instrument'];need(re.fullmatch(r'[A-Z]{3}_[A-Z]{3}',pair),'pair_identity')
            path=ASSESSMENT/'results'/(pair+'.json');need(str(path)==reference['path'],'pair_result_path')
            pair_raw=read_bound(path,reference['sha256'],16*1024*1024);result=json.loads(pair_raw)
            need(result['status']=='passed' and not result['verification_errors'] and not result['original_completed_not_scored'],'verified_pair_required')
            evidence=result['evidence'];input_path=ASSESSMENT/'inputs'/(pair+'.json.gz');need(str(input_path)==evidence['path'],'input_path')
            raw=read_bound(input_path,evidence['sha256'],16*1024*1024)
            decoder=zlib.decompressobj(31);logical=decoder.decompress(raw,256*1024*1024+1)
            need(len(logical)<=256*1024*1024 and decoder.eof and not decoder.unconsumed_tail and not decoder.unused_data,'logical_input_bound')
            need(digest(logical)==evidence['logical_payload_sha256'],'logical_input_hash')
            read_bytes+=len(pair_raw)+len(raw)+len(logical);need(read_bytes<=4*1024**3,'total_external_input_bound')
            data=json.loads(logical);decisions=data['dataset']['decisions'];need(len(decisions)<=256,'original_decision_bound')
            by_id={r['decision_id']:r for r in decisions};need(len(by_id)==len(decisions),'duplicate_original_decision')
            records.extend(build_record(r,by_id[r['decision_id']],pair) for r in result['scored_rows'])
            bindings.append(dict(instrument=pair,result_sha256=reference['sha256'],input_sha256=evidence['sha256'],logical_payload_sha256=evidence['logical_payload_sha256'],scored_rows=len(result['scored_rows'])))
    result=summarize(records);need(len(records)==report['metrics'][FAMILY]['valid_completed_decisions'],'complete_common_denominator')
    need(Path(__file__).read_bytes()==source and digest(report_path.read_bytes())==REPORT_SHA,'source_or_report_changed')
    result.update(schema_version='joint_v3_matched_dependence_supplement_v1_20260909',status='passed',started_epoch=started,completed_epoch=time.time(),
        helper_sha256=digest(source),original_report=dict(path=str(report_path),sha256=REPORT_SHA),input_bindings=bindings,
        original_metrics=report['metrics'],original_paired_deltas=report['paired_deltas'],original_dependence=report['dependence'],
        total_external_input_bytes=read_bytes,read_only_scope='Only already retained external assessment files; no live DB, source-news replay, GET, model fit or economic rescoring.',
        limits=['Matched price-only and neutral-news values are the original pre-issue diagnostics on identical joint decisions and economic endpoints, not independent price-family forecasts.',
            'Neutral-news difference is an ablation of the same fitted model, not identified causal news impact. Joint versus price-only also includes fitted-model differences.',
            'All correlations are descriptive. Shared realized returns mechanically increase signed error correlation; overlapping H1 windows, currencies and source captures prevent independent-trial claims.',
            'Direction-agreement groups are predefined by the original forecasts, not selected by profitable outcomes. Primary paired means retain every valid original completed row.',
            'Brier exists only for original retained probability baselines; neither comparator has a probability. Signed forecast differences are not calibrated probabilities.',
            'Original scored MAE/net fields are preserved, not recalculated from independently rounded serialized return ratios. Comparator pre-issue pips and return conversion are reconciled with retained input diagnostics.',
            'Net bps use registered bid/ask endpoints, not actual trading, financing or USD account P&L. Snapshot clocks are per database and not globally atomic.'])
    output=BASE/'JOINT_MATCHED_DEPENDENCE_SUPPLEMENT_20260909.json';encoded=json.dumps(result,indent=2,sort_keys=True,allow_nan=False).encode()+b'\n'
    with output.open('xb') as stream:stream.write(encoded);stream.flush();os.fsync(stream.fileno())
    print(json.dumps(dict(path=str(output),sha256=digest(encoded),valid=len(records),comparisons=result['comparisons'])))


if __name__=='__main__':run()
