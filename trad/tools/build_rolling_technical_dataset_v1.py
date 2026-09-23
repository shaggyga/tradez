"""Materialize a real date interval of M1 features and later outcomes.

The existing sample validator is deliberately not used as a date importer.
Pair/week core shards and peer sidecars share exact original keys. No raw
archive copies, provider calls, model fitting or trading operations occur.
"""
from __future__ import annotations
import argparse
from collections import Counter
from datetime import datetime,timezone
import json
import hashlib
from pathlib import Path
import shutil
import sys
import time

ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0,str(ROOT))
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import oanda_rolling_technical_features_v1 as kernel
import oanda_rolling_technical_inputs_v1 as inputs
import oanda_rolling_technical_panel_v1 as panel
import oanda_rolling_technical_ranges_v1 as ranges
import oanda_rolling_technical_labels_v1 as labels
from oanda_rolling_technical_panel_batch_v1 import compute_peer_arrays
from oanda_rolling_technical_dataset_v1 import SCHEMA,origin_splits,split_maturity,key_hash,file_sha

SOURCE_NAMES=(str(Path(__file__).relative_to(ROOT)),
 'oanda_rolling_technical_features_v1.py','oanda_rolling_technical_inputs_v1.py',
 'oanda_rolling_technical_panel_v1.py','oanda_rolling_technical_ranges_v1.py',
 'oanda_rolling_technical_labels_v1.py','oanda_rolling_technical_panel_batch_v1.py',
 'oanda_rolling_technical_dataset_v1.py')


def epoch(text):
    dt=datetime.fromisoformat(text.replace('Z','+00:00'))
    if dt.tzinfo is None:
        dt=dt.replace(tzinfo=timezone.utc)
    if dt.microsecond:
        raise ValueError('whole_aligned_minute_boundary_required')
    at=int(dt.timestamp())
    if at%60:
        raise ValueError('aligned_minute_boundary_required')
    return at


def save(path,value):
    Path(path).write_text(json.dumps(value,sort_keys=True,indent=2,allow_nan=False)+'\n',encoding='utf-8')


def arrow(values):
    a=np.asarray(values)
    return pa.array(a,mask=~np.isfinite(a)) if a.dtype.kind=='f' else pa.array(a)


def write_shard(path,columns,output,report,args):
    table=pa.table({name:arrow(a) for name,a in columns.items()})
    reserve=table.nbytes*2+1024**2
    if report['bytes']+reserve>args.max_output_gib*1024**3 or shutil.disk_usage(output).free-reserve<args.minimum_free_gib*1024**3:
        raise ValueError('historical_dataset_storage_bound')
    if path.exists():
        raise ValueError('new_shard_required')
    dictionary=[name for name in table.column_names if pa.types.is_string(table[name].type)]
    pq.write_table(table,path,compression='zstd',row_group_size=8192,use_dictionary=dictionary)
    restored=pq.ParquetFile(path).read()
    if not restored.equals(table):
        raise ValueError('parquet_table_readback_mismatch')
    for name in table.column_names:
        if pa.types.is_floating(table[name].type):
            before=table[name].to_numpy();after=restored[name].to_numpy()
            mask=np.isfinite(before)
            if not np.array_equal(before[mask].view(np.uint64),after[mask].view(np.uint64)):
                raise ValueError('parquet_finite_bit_readback_mismatch:'+name)
    report['bytes']+=path.stat().st_size
    return {'path':str(path.relative_to(output)).replace('\\','/'),'sha256':file_sha(path),'bytes':path.stat().st_size,'rows':table.num_rows,
            'readback':'entire Arrow table, all finite float64 bits and missing masks verified'}


def update_summary(summary,features,mask):
    for name,a in features.items():
        finite=a[mask & np.isfinite(a)]
        s=summary.setdefault(name,{'finite_train_rows':0,'missing_train_rows':0,'minimum':None,'maximum':None})
        s['finite_train_rows']+=int(len(finite));s['missing_train_rows']+=int(mask.sum())-len(finite)
        if len(finite):
            lo=float(finite.min());hi=float(finite.max())
            s['minimum']=lo if s['minimum'] is None else min(lo,s['minimum'])
            s['maximum']=hi if s['maximum'] is None else max(hi,s['maximum'])


def run(args):
    start,train_end,val_end,end=map(epoch,(args.start,args.train_end,args.validation_end,args.end))
    origin_splits(np.array([],dtype=np.int64),start,train_end,val_end,end)
    if end-start>92*86400 or not 1<=args.max_output_gib<=16 or not 16<=args.minimum_free_gib<=128:
        raise ValueError('bounded_development_dataset_required')
    if end+3600>time.time():
        raise ValueError('historical_outcome_extension_must_be_in_past')
    manifest_sha=file_sha(args.manifest)
    source_recipes=inputs.discover_archive_sources(args.manifest)
    if any(recipe['manifest_sha256']!=manifest_sha for recipe in source_recipes.values()):
        raise ValueError('manifest_changed_while_discovering_sources')
    pairs=sorted(source_recipes)
    if len(pairs)!=68:
        raise ValueError('complete_68_pair_universe_required')
    pip_raw=Path(args.pair_metadata).read_bytes()
    pip_payload=json.loads(pip_raw)
    pip_sha=hashlib.sha256(pip_raw).hexdigest()
    pip_sizes={p:float(pip_payload['pairs'][p]['pip_size']) for p in pairs}
    output=Path(args.output).resolve()
    if output.exists() or not output.is_relative_to((ROOT/'data').resolve()):
        raise ValueError('new_project_data_output_required')
    output.mkdir(parents=True);(output/'shards').mkdir();(output/'receipts').mkdir()
    bindings={name:file_sha(ROOT/name) for name in SOURCE_NAMES}
    feature_registry=kernel.feature_registry()+panel.panel_registry()
    label_registry=labels.label_registry()
    eligibility_names=[f'label__{h}m__split_eligible' for h in (5,15,30,60)]
    label_names=[r['name'] for r in label_registry]+eligibility_names
    report={'schema':SCHEMA,'status':'building','started_utc':datetime.now(timezone.utc).isoformat(),
        'source_bindings':bindings,'source_manifest':str(Path(args.manifest).resolve()),'source_manifest_sha256':manifest_sha,
        'pair_metadata_path':str(Path(args.pair_metadata).resolve()),'pair_metadata_sha256':pip_sha,'pip_sizes':pip_sizes,
        'boundaries':{'start':start,'train_end':train_end,'validation_end':val_end,'end':end},
        'historically_opened_development_only':True,'untouched_confirmation':False,
        'availability':'Retrospective original minute END assumption; actual historical provider publication times are not reconstructed.',
        'label_purge':'target bar END strictly before origin split END; invalid labels retained separately from origin population',
        'feature_names':[r['name'] for r in feature_registry],'label_names':label_names,
        'feature_count':len(feature_registry),'origin_rows':0,'bytes':0,'pairs':{},'partitions':[],
        'research_only':True,'can_place_orders':False,'model_fits':0,'full_archive_materialized':False}
    save(output/'FEATURE_REGISTRY.json',feature_registry)
    save(output/'LABEL_REGISTRY.json',label_registry+[{'name':n,'role':'label_filter','model_input':False,'formula':'target_bar_end < original_split_end'} for n in eligibility_names])
    save(output/'DATASET.json',report)
    peer_inputs={};train_summary={};began=time.monotonic()
    for pair in pairs:
        retries=[]
        for attempt in range(5):
            try:
                data,receipt=ranges.read_range(pair,source_recipes[pair],start-602*60,end+60*60,time.time(),max_rows=150000)
                break
            except (OSError,ValueError) as exc:
                if attempt==4 or not any(word in str(exc).lower() for word in ('changed','replace','truncat','sharing','permission')):
                    raise
                retries.append(type(exc).__name__+':'+str(exc))
                time.sleep(.25)
        receipt['read_retries']=retries
        t=data['time'];origin=(t>=start)&(t<end)
        if not origin.any():
            raise ValueError('empty_pair_in_requested_window:'+pair)
        all_features=kernel.compute_features(data,pair,pip_sizes[pair])
        all_labels=labels.compute_outcomes(data,coverage_end_epoch=end+3600)
        times=t[origin];split=origin_splits(times,start,train_end,val_end,end)
        features={n:a[origin] for n,a in all_features.items()}
        outcomes={n:a[origin] for n,a in all_labels.items()}
        for h in (5,15,30,60):
            outcomes[f'label__{h}m__split_eligible']=split_maturity(times,h,start,train_end,val_end,end)
        peer_inputs[pair]={'time':times,'values':{f'm1__return_{h}_bps':features[f'm1__return_{h}_bps'] for h in panel.HORIZONS}}
        update_summary(train_summary,features,split=='train')
        week=(times-start)//(7*86400)
        pair_records=[]
        for block in np.unique(week):
            selected=np.flatnonzero(week==block)
            columns={'instrument':np.full(len(selected),pair),'bar_start_epoch':times[selected],
                'bar_end_epoch':times[selected]+60,'origin_split':split[selected]}
            columns.update({n:a[selected] for n,a in features.items()})
            columns.update({n:a[selected] for n,a in outcomes.items()})
            name=f'{pair}_{int(block):02d}'
            record={'pair':pair,'block':int(block),'key_sha256':key_hash(times[selected]),
                'split_counts':dict(Counter(split[selected])),'core':write_shard(output/'shards'/(name+'_core.parquet'),columns,output,report,args)}
            report['partitions'].append(record);pair_records.append(record)
        coverage={str(h):dict(Counter(outcomes[f'label__{h}m__state'])) for h in (5,15,30,60)}
        report['pairs'][pair]={'origin_rows':len(times),'input_context_and_outcome_rows':len(t),
            'split_counts':dict(Counter(split)),'outcome_states':coverage,'source_receipt':'receipts/'+pair+'.json'}
        report['origin_rows']+=len(times)
        save(output/'receipts'/(pair+'.json'),receipt)
        save(output/'DATASET.json',report)
        print(json.dumps({'stage':'core','pair':pair,'rows':len(times),'elapsed_seconds':round(time.monotonic()-began,1)}),flush=True)
    for pair,times,features,support in compute_peer_arrays(peer_inputs,start_epoch=start,end_epoch=end):
        split=origin_splits(times,start,train_end,val_end,end)
        update_summary(train_summary,features,split=='train')
        week=(times-start)//(7*86400)
        for record in (r for r in report['partitions'] if r['pair']==pair):
            selected=np.flatnonzero(week==record['block'])
            if key_hash(times[selected])!=record['key_sha256']:
                raise ValueError('peer_origin_join_mismatch')
            columns={'bar_start_epoch':times[selected]}
            columns.update({n:a[selected] for n,a in features.items()})
            columns.update({n:a[selected] for n,a in support.items()})
            record['peers']=write_shard(output/'shards'/f"{pair}_{record['block']:02d}_peers.parquet",columns,output,report,args)
        print(json.dumps({'stage':'peers','pair':pair,'rows':len(times)}),flush=True)
    for name,s in train_summary.items():
        s['all_missing_in_train']=s['finite_train_rows']==0
        s['constant_in_train']=s['finite_train_rows']>0 and s['minimum']==s['maximum']
    save(output/'TRAIN_FEATURE_SUMMARY.json',{'selection_scope':'all TRAIN origins only; no label or later-period selection',
        'statistical_correlation_dedup_performed':False,'features':train_summary,
        'candidate_nonconstant_inputs':[n for n in report['feature_names'] if not train_summary[n]['all_missing_in_train'] and not train_summary[n]['constant_in_train']]})
    if any(file_sha(ROOT/n)!=h for n,h in bindings.items()):
        raise ValueError('builder_source_changed_during_run')
    if file_sha(args.manifest)!=manifest_sha or file_sha(args.pair_metadata)!=pip_sha:
        raise ValueError('dataset_metadata_changed_during_run')
    report['readback']={'all_shards_verified':True,'core_peer_pair_minute_keys_checked':True,'finite_float64_bits_and_missing_masks_checked':True}
    report.update(status='complete',completed_utc=datetime.now(timezone.utc).isoformat(),elapsed_seconds=round(time.monotonic()-began,3))
    save(output/'DATASET.json',report)
    return report


def parse_args(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--manifest',type=Path,required=True)
    p.add_argument('--pair-metadata',type=Path,default=ROOT/'config/pair_local_operational_v2_20260913.json')
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--start',default='2026-07-13T00:00:00Z')
    p.add_argument('--train-end',default='2026-08-24T00:00:00Z')
    p.add_argument('--validation-end',default='2026-08-31T00:00:00Z')
    p.add_argument('--end',default='2026-09-07T00:00:00Z')
    p.add_argument('--max-output-gib',type=int,default=12)
    p.add_argument('--minimum-free-gib',type=int,default=32)
    return p.parse_args(argv)


if __name__=='__main__':
    arguments=parse_args()
    try:
        result=run(arguments)
    except Exception as exc:
        partial=Path(arguments.output)/'DATASET.json'
        if partial.exists():
            report=json.loads(partial.read_text('utf-8'))
            if report.get('schema')==SCHEMA and report.get('status')=='building':
                report.update(status='failed',error=type(exc).__name__+':'+str(exc))
                save(partial,report)
        raise
    print(json.dumps({k:result[k] for k in ('status','origin_rows','bytes','elapsed_seconds')}),flush=True)
