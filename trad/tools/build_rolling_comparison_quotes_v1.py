"""Build bounded aligned quote, causal ARIMA and delayed-cost comparison data.

Only accepted base origins are emitted. Source rows must match their original
receipts exactly. Current quotes and forecasts are separated by namespace from
future delayed-entry labels. Live collectors, trading and old weights are not
changed; the newly fitted ARIMA conditional-OLS coefficients are saved here.
"""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime,timezone
import hashlib
import json
import multiprocessing
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
import oanda_rolling_model_baselines_v1 as model
import oanda_rolling_technical_endpoint_labels_v1 as endpoint
import oanda_rolling_technical_inputs_v1 as inputs
import oanda_rolling_technical_labels_v1 as strict
import oanda_rolling_technical_ranges_v1 as ranges
from oanda_rolling_technical_dataset_v1 import SCHEMA as BASE_SCHEMA, file_sha, key_hash
from tools import build_rolling_technical_endpoints_v1 as overlay_builder

SCHEMA='rolling_model_quote_panel_v1_20260915'
SOURCE_NAMES=('oanda_rolling_model_baselines_v1.py','tools/build_rolling_comparison_quotes_v1.py')
CAP_BYTES=1024**3
METADATA_RESERVE=32*1024**2


def save(path,payload):
    Path(path).write_text(json.dumps(payload,sort_keys=True,indent=2,allow_nan=False)+'\n',encoding='utf-8')


def delayed_endpoint_outcomes(data,horizons=model.DEFAULT_HORIZONS):
    """Entry at exact t+1, exit at original t+h; both are candle-close proxies."""
    horizons=strict._horizons(horizons)
    if min(horizons)<2:
        raise ValueError('delayed_entry_horizon_must_exceed_one_minute')
    values=strict._normalize(data)
    times,mid,bid,ask=(values[n] for n in ('time','close','bid_close','ask_close'))
    n=len(times)
    entry=np.searchsorted(times,times+60)
    entry_safe=np.minimum(entry,max(0,n-1))
    entry_present=(entry<n)&(times[entry_safe]==times+60)
    output={}
    for h in horizons:
        target=np.searchsorted(times,times+h*60)
        safe=np.minimum(target,max(0,n-1))
        present=(target<n)&(times[safe]==times+h*60)
        valid=entry_present&present&np.isfinite(mid[entry_safe])&np.isfinite(bid[entry_safe])&np.isfinite(ask[entry_safe])&np.isfinite(bid[safe])&np.isfinite(ask[safe])
        with np.errstate(divide='ignore',invalid='ignore',over='ignore'):
            long=(bid[safe]-ask[entry_safe])/mid[entry_safe]*10000.
            short=(bid[entry_safe]-ask[safe])/mid[entry_safe]*10000.
        valid &= np.isfinite(long)&np.isfinite(short)
        long[~valid]=np.nan;short[~valid]=np.nan
        output[f'delayed_label__{h}m__long_net_bps']=long
        output[f'delayed_label__{h}m__short_net_bps']=short
        output[f'delayed_label__{h}m__valid']=valid
    return output


def column_registry():
    result=[{'name':n,'role':'origin_metadata','model_input':False,'future_information':False}
            for n in ('instrument','bar_start_epoch','bar_end_epoch','origin_split')]
    result.extend({'name':n,'role':'current_quote','model_input':False,'future_information':False,
                   'scope':'known at original completed candle end; auxiliary comparator/cost input, not added to the 228-feature registry'}
                  for n in ('quote__mid_close','quote__bid_close','quote__ask_close','quote__entry_spread_bps'))
    result.extend({'name':f'forecast__{model.ENGINE}__{h}m_bps','role':'baseline_forecast','model_input':False,
                   'future_information':False,'engine':model.ENGINE} for h in model.DEFAULT_HORIZONS)
    result.append({'name':f'forecast__{model.ENGINE}__state','role':'forecast_availability','model_input':False,'future_information':False})
    for h in model.DEFAULT_HORIZONS:
        for field in ('long_net_bps','short_net_bps','valid'):
            result.append({'name':f'delayed_label__{h}m__{field}','role':'label_metadata' if field=='valid' else 'label',
                'model_input':False,'future_information':True,'horizon_minutes':h,
                'entry':'exact origin_start+60 candle close','target':'unchanged origin_start+horizon*60 candle close',
                'denominator':'entry candle midpoint','scope':'bid/ask endpoint proxy, no fills/slippage/financing or full-path claim'})
    return result


def fit_pair_arima(data,boundaries):
    """Use exactly the declared fit interval; context remains prediction-only."""
    selected=(data['time']>=boundaries['start'])&(data['time']<boundaries['train_end'])
    params=model.fit_arima110(data['time'][selected],data['close'][selected],boundaries['train_end'])
    params.update(declared_fit_start_epoch=boundaries['start'],
        fit_input_start_rule='input candle START at/after declared start; initial two response bars unsupported after trimming',
        pre_start_context_used_for_fitting=False)
    return params


def _write(path,columns,job):
    table=pa.table({n:pa.array(a,mask=~np.isfinite(a)) if np.asarray(a).dtype.kind=='f' else pa.array(a) for n,a in columns.items()})
    disk_reserve=table.nbytes+1024**2
    if disk_reserve>job['pair_byte_cap']:
        raise ValueError('comparison_quote_pair_storage_bound')
    if shutil.disk_usage(job['output']).free-disk_reserve*job['workers']<job['minimum_free_bytes']:
        raise ValueError('comparison_quote_free_space_reserve')
    if path.exists():
        raise ValueError('new_comparison_quote_file_required')
    dictionaries=[f.name for f in table.schema if pa.types.is_string(f.type) or pa.types.is_large_string(f.type)]
    pq.write_table(table,path,compression='zstd',row_group_size=8192,use_dictionary=dictionaries)
    actual=pq.ParquetFile(path).read()
    if not table.equals(actual,check_metadata=True):
        raise ValueError('comparison_quote_table_readback_mismatch')
    for name,a in columns.items():
        if np.asarray(a).dtype.kind=='f' and not overlay_builder._float_bits_equal(a,actual[name].to_numpy()):
            raise ValueError('comparison_quote_float_bits_mismatch:'+name)
    return {'path':path.relative_to(job['output']).as_posix(),'sha256':file_sha(path),'bytes':path.stat().st_size,
            'rows':table.num_rows,'readback':'complete table, finite bits and missing masks verified'}


def _pair(job):
    overlay_builder.assert_pins(job['bindings'])
    base,overlay,output=map(Path,(job['base'],job['endpoints'],job['output']))
    if file_sha(base/'DATASET.json')!=job['base_sha256'] or file_sha(overlay/'ENDPOINT_DATASET.json')!=job['endpoint_sha256']:
        raise ValueError('accepted_dataset_manifest_changed')
    old_path=endpoint.checked_path(base,job['source_receipt'],job['source_receipt_sha256'])
    old=json.loads(old_path.read_bytes())
    retries=[]
    for attempt in range(3):
        try:
            data,current=ranges.read_range(job['pair'],job['recipe'],old['requested_start_epoch'],old['requested_end_epoch_exclusive'],
                                            old['observed_epoch'],max_rows=old['max_rows'],batch_rows=old['batch_rows'])
            break
        except (OSError,ValueError) as exc:
            if attempt==2 or not any(word in str(exc).lower() for word in overlay_builder.TRANSIENT):
                raise
            retries.append(type(exc).__name__+':'+str(exc));time.sleep(.2)
    identity=overlay_builder.verify_input_identity(old,current)
    params=fit_pair_arima(data,job['boundaries'])
    forecasts=model.predict_arima110(data['time'],data['close'],params)
    states=model.arima110_origin_states(data['time'],data['close'],params)
    delayed=delayed_endpoint_outcomes(data)
    primary=endpoint.compute_endpoint_outcomes(data,coverage_end_epoch=old['requested_end_epoch_exclusive'])
    origin=(data['time']>=job['boundaries']['start'])&(data['time']<job['boundaries']['end'])
    selected_times=data['time'][origin]
    actual_times=[];splits=[];parity_cells=0
    for record in job['partitions']:
        cp=endpoint.checked_path(base,record['core']['path'],record['core']['sha256'])
        endpoint.checked_path(base,record['peers']['path'],record['peers']['sha256'])
        core=pq.ParquetFile(cp).read(columns=['instrument','bar_start_epoch','bar_end_epoch','origin_split'])
        times=core['bar_start_epoch'].to_numpy()
        if (key_hash(times)!=record['key_sha256'] or len(times)!=record['core']['rows'] or
                np.any(core['bar_end_epoch'].to_numpy()!=times+60) or any(p!=job['pair'] for p in core['instrument'].to_pylist())):
            raise ValueError('base_quote_origin_identity_mismatch')
        side=job['endpoint_partitions'][str(record['block'])]
        if side['base_core_sha256']!=record['core']['sha256'] or side['base_peer_sha256']!=record['peers']['sha256']:
            raise ValueError('endpoint_base_partition_binding_mismatch')
        ep=endpoint.checked_path(overlay,side['path'],side['sha256'])
        names=['bar_start_epoch']+[f'endpoint_label__{h}m__{field}' for h in model.DEFAULT_HORIZONS
                                 for field in ('return_bps','long_net_bps','short_net_bps','midpoint_valid','bidask_endpoint_valid')]
        existing=pq.ParquetFile(ep).read(columns=names)
        if not np.array_equal(existing['bar_start_epoch'].to_numpy(),times):
            raise ValueError('endpoint_exact_pair_clock_join_required')
        indexes=np.searchsorted(data['time'],times)
        if np.any(indexes>=len(data['time'])) or not np.array_equal(data['time'][indexes],times):
            raise ValueError('base_origin_missing_from_identical_quote_source')
        for name in names[1:]:
            a=primary[name][indexes];b=existing[name].to_numpy()
            equal=overlay_builder._float_bits_equal(a,b) if np.asarray(a).dtype.kind=='f' else np.array_equal(a,b)
            if not equal:
                raise ValueError('accepted_endpoint_quote_parity_failed:'+name)
            parity_cells+=int(np.isfinite(a).sum()) if np.asarray(a).dtype.kind=='f' else len(a)
        actual_times.append(times);splits.append(core['origin_split'].to_numpy())
    if not np.array_equal(np.concatenate(actual_times),selected_times):
        raise ValueError('every_base_origin_must_be_retained_in_quote_panel')
    columns={'instrument':np.full(len(selected_times),job['pair']),'bar_start_epoch':selected_times,
        'bar_end_epoch':selected_times+60,'origin_split':np.concatenate(splits),
        'quote__mid_close':data['close'][origin],'quote__bid_close':data['bid_close'][origin],
        'quote__ask_close':data['ask_close'][origin],
        'quote__entry_spread_bps':(data['ask_close'][origin]-data['bid_close'][origin])/data['close'][origin]*10000.}
    columns.update({f'forecast__{model.ENGINE}__{h}m_bps':a[origin] for h,a in forecasts.items()})
    columns[f'forecast__{model.ENGINE}__state']=states[origin]
    columns.update({n:a[origin] for n,a in delayed.items()})
    path=output/'pairs'/(job['pair']+'.parquet')
    result=_write(path,columns,job)
    receipt={'pair':job['pair'],'source_receipt_path':job['source_receipt'],'source_receipt_sha256':job['source_receipt_sha256'],
        'source_identity':identity,'reread':current,'read_retries':retries,'arima_params':params,
        'arima_origin_state_counts':dict(Counter(states[origin])),
        'accepted_endpoint_parity_cells':parity_cells,
        'delayed_valid_counts':{str(h):int(columns[f'delayed_label__{h}m__valid'].sum()) for h in model.DEFAULT_HORIZONS}}
    rp=output/'receipts'/(job['pair']+'.json');save(rp,receipt)
    overlay_builder.assert_pins(job['bindings'])
    endpoint.checked_path(base,job['source_receipt'],job['source_receipt_sha256'])
    result.update(pair=job['pair'],key_sha256=key_hash(selected_times),receipt_path=rp.relative_to(output).as_posix(),
        receipt_sha256=file_sha(rp),receipt_bytes=rp.stat().st_size,arima_status=params['status'],arima_phi=params['phi'],
        arima_valid_regression_pairs=params['valid_regression_pairs'],arima_origin_state_counts=receipt['arima_origin_state_counts'],
        accepted_endpoint_parity_cells=parity_cells,delayed_valid_counts=receipt['delayed_valid_counts'])
    return result


def run(args):
    if not 1<=args.workers<=3 or not 16<=args.minimum_free_gib<=128:
        raise ValueError('bounded_comparison_quote_configuration_required')
    base,overlay=Path(args.base).resolve(),Path(args.endpoints).resolve()
    base_raw=(base/'DATASET.json').read_bytes();base_sha=hashlib.sha256(base_raw).hexdigest();baseline=json.loads(base_raw)
    endpoint_raw=(overlay/'ENDPOINT_DATASET.json').read_bytes();endpoint_sha=hashlib.sha256(endpoint_raw).hexdigest();overlay_meta=json.loads(endpoint_raw)
    if (baseline.get('schema')!=BASE_SCHEMA or baseline.get('status')!='complete' or len(baseline['pairs'])!=68 or
            overlay_meta.get('schema')!=endpoint.SCHEMA or overlay_meta.get('status')!='complete' or overlay_meta['base_manifest_sha256']!=base_sha):
        raise ValueError('complete_matched_68_pair_base_and_endpoint_overlay_required')
    if file_sha(baseline['source_manifest'])!=baseline['source_manifest_sha256']:
        raise ValueError('accepted_source_manifest_changed')
    recipes=inputs.discover_archive_sources(baseline['source_manifest'])
    if any(r['manifest_sha256']!=baseline['source_manifest_sha256'] for r in recipes.values()):
        raise ValueError('accepted_source_manifest_changed_during_discovery')
    bindings={**baseline['source_bindings'],**overlay_meta['source_bindings']}
    bindings.update({n:file_sha(ROOT/n) for n in SOURCE_NAMES});overlay_builder.assert_pins(bindings)
    output=Path(args.output).resolve()
    if (output.exists() or not output.is_relative_to((ROOT/'data').resolve()) or
            output.is_relative_to(base) or output.is_relative_to(overlay)):
        raise ValueError('new_independent_quote_panel_output_required')
    minimum=args.minimum_free_gib*1024**3
    if shutil.disk_usage(output.parent).free-CAP_BYTES<minimum:
        raise ValueError('one_gib_quote_budget_must_fit_above_drive_reserve')
    jobs=[]
    for pair in sorted(baseline['pairs']):
        rec=baseline['pairs'][pair]['source_receipt'];rp=endpoint.checked_path(base,rec)
        parts=[p for p in baseline['partitions'] if p['pair']==pair]
        endpoint_parts={str(p['block']):p for p in overlay_meta['partitions'] if p['pair']==pair}
        if set(endpoint_parts)!={str(p['block']) for p in parts}:
            raise ValueError('complete_endpoint_partition_map_required')
        jobs.append({'base':str(base),'endpoints':str(overlay),'output':str(output),'pair':pair,
            'base_sha256':base_sha,'endpoint_sha256':endpoint_sha,'recipe':recipes[pair],
            'source_receipt':rec,'source_receipt_sha256':file_sha(rp),'boundaries':baseline['boundaries'],
            'partitions':parts,'endpoint_partitions':endpoint_parts,'bindings':bindings,'workers':args.workers,
            'pair_byte_cap':(CAP_BYTES-METADATA_RESERVE)//68,'minimum_free_bytes':minimum})
    report={'schema':SCHEMA,'status':'building','base_root':str(base),'base_manifest_sha256':base_sha,
        'endpoint_root':str(overlay),'endpoint_manifest_sha256':endpoint_sha,'source_bindings':bindings,
        'boundaries':baseline['boundaries'],'workers':args.workers,'maximum_bytes':CAP_BYTES,'minimum_free_bytes':minimum,
        'origin_rows':0,'bytes':0,'pairs':{},'registered_model_inputs_added':False,'live_changes':False,
        'arima_engine':model.ENGINE,'arima_fit_scope':'each pair input candle START at/after declared training start and before cutoff; response candle END strictly before cutoff; pre-start context excluded from fitting',
        'delayed_cost_scope':'exact next-minute entry candle close to unchanged original horizon target, divided by entry midpoint',
        'future_labels_not_inputs':True,'started_utc':datetime.now(timezone.utc).isoformat()}
    created=False;began=time.monotonic()
    try:
        output.mkdir();created=True;(output/'pairs').mkdir();(output/'receipts').mkdir()
        (output/'BASE_DATASET.json').write_bytes(base_raw);(output/'BASE_ENDPOINTS.json').write_bytes(endpoint_raw)
        save(output/'QUOTE_COLUMN_REGISTRY.json',column_registry());save(output/'QUOTE_PANEL.json',report)
        if args.workers==1:
            results=map(_pair,jobs);pool=None
        else:
            pool=ProcessPoolExecutor(max_workers=args.workers,mp_context=multiprocessing.get_context('spawn'))
            pending=[pool.submit(_pair,job) for job in jobs];results=(f.result() for f in as_completed(pending))
        try:
            for result in results:
                pair=result.pop('pair');report['pairs'][pair]=result;report['origin_rows']+=result['rows']
                report['bytes']+=result['bytes']+result['receipt_bytes']
                if report['bytes']+METADATA_RESERVE>CAP_BYTES:
                    raise ValueError('quote_panel_global_byte_cap')
                save(output/'QUOTE_PANEL.json',report)
                print(json.dumps({'pair':pair,'rows':result['rows'],'completed_pairs':len(report['pairs']),
                                  'arima_status':result['arima_status'],'elapsed_seconds':round(time.monotonic()-began,1)}),flush=True)
        finally:
            if pool is not None:pool.shutdown(wait=True,cancel_futures=True)
        if set(report['pairs'])!=set(baseline['pairs']) or report['origin_rows']!=baseline['origin_rows']:
            raise ValueError('complete_unchanged_base_origin_population_required')
        for result in report['pairs'].values():
            endpoint.checked_path(output,result['path'],result['sha256'])
            endpoint.checked_path(output,result['receipt_path'],result['receipt_sha256'])
        for job in jobs:
            endpoint.checked_path(base,job['source_receipt'],job['source_receipt_sha256'])
            for p in job['partitions']:
                for kind in ('core','peers'):endpoint.checked_path(base,p[kind]['path'],p[kind]['sha256'])
            for p in job['endpoint_partitions'].values():endpoint.checked_path(overlay,p['path'],p['sha256'])
        overlay_builder.assert_pins(bindings)
        if file_sha(base/'DATASET.json')!=base_sha or file_sha(overlay/'ENDPOINT_DATASET.json')!=endpoint_sha:
            raise ValueError('accepted_dataset_manifest_changed')
        report.update(status='complete',completed_utc=datetime.now(timezone.utc).isoformat(),
            elapsed_seconds=round(time.monotonic()-began,3),registry_sha256=file_sha(output/'QUOTE_COLUMN_REGISTRY.json'))
        save(output/'QUOTE_PANEL.json',report)
    except BaseException as exc:
        report.update(status='failed',failure={'type':type(exc).__name__,'message':str(exc)})
        if created:save(output/'QUOTE_PANEL.json',report)
        raise
    return report


def parse_args(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base',type=Path,default=ROOT/'data/rolling_technical_training_20260915_v1')
    parser.add_argument('--endpoints',type=Path,default=ROOT/'data/rolling_technical_endpoints_20260915_v1')
    parser.add_argument('--output',type=Path,default=ROOT/'data/rolling_model_quote_panel_20260915_v2')
    parser.add_argument('--workers',type=int,default=3)
    parser.add_argument('--minimum-free-gib',type=int,default=32)
    return parser.parse_args(argv)


if __name__=='__main__':
    result=run(parse_args())
    print(json.dumps({k:result[k] for k in ('status','origin_rows','bytes','elapsed_seconds')}),flush=True)
