"""Read-only price ingestion into a shared rolling technical research dataset.

No broker client, news, model fitting, account state or trading imports.
Existing collectors remain owners of the original files. This worker owns
only its declared new output directory and refuses duplicate writers.
"""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import time

import numpy as np

import oanda_rolling_technical_features_v1 as kernel
import oanda_rolling_technical_inputs_v1 as inputs
import oanda_rolling_technical_panel_v1 as panel_kernel
from oanda_rolling_technical_store_v1 import TechnicalStore, RevisionDetected, SCHEMA, HORIZONS

ROOT=Path(__file__).resolve().parent
SOURCES=(Path(__file__).name,'oanda_rolling_technical_features_v1.py','oanda_rolling_technical_inputs_v1.py','oanda_rolling_technical_store_v1.py','oanda_rolling_technical_panel_v1.py')
FLAGS={'research_only':True,'can_place_orders':False,'can_promote':False,'model_training_performed':False}


def utc(epoch=None):
    return datetime.fromtimestamp(time.time() if epoch is None else epoch,timezone.utc).isoformat()


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def atomic_json(path,value):
    path=Path(path)
    temporary=path.with_name(path.name+'.tmp.'+str(os.getpid()))
    temporary.write_text(json.dumps(value,sort_keys=True,indent=2,allow_nan=False)+'\n',encoding='utf-8')
    os.replace(temporary,path)


@contextmanager
def owner_lock(root):
    path=Path(root)/'worker.lock'
    with path.open('a+b') as handle:
        handle.seek(0,2)
        if not handle.tell():
            handle.write(b'0');handle.flush()
        handle.seek(0)
        if os.name=='nt':
            import msvcrt
            try:
                msvcrt.locking(handle.fileno(),msvcrt.LK_NBLCK,1)
            except OSError:
                raise RuntimeError('rolling_dataset_writer_already_running') from None
        else:
            import fcntl
            fcntl.flock(handle.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
        try:
            yield
        finally:
            handle.seek(0)
            if os.name=='nt':
                msvcrt.locking(handle.fileno(),msvcrt.LK_UNLCK,1)
            else:
                fcntl.flock(handle.fileno(),fcntl.LOCK_UN)


def read_config(path):
    value=json.loads(Path(path).read_text('utf-8'))
    if value.get('schema')!=SCHEMA or value.get('can_place_orders') is not False or value.get('can_promote') is not False:
        raise ValueError('research_only_dataset_config_required')
    pairs=value['pairs']
    if len(pairs)!=68 or any(not isinstance(pip,(int,float)) or not math.isfinite(pip) or not 0<pip<=.1 for pip in pairs.values()):
        raise ValueError('explicit_68_pair_pip_metadata_required')
    for name,expected in value['source_bindings'].items():
        if name not in SOURCES or sha(ROOT/name)!=expected:
            raise ValueError('rolling_dataset_source_changed:'+name)
    if set(value['source_bindings'])!=set(SOURCES):
        raise ValueError('complete_source_bindings_required')
    root=Path(value['output_root']).resolve()
    if not root.is_relative_to((ROOT/'data').resolve()) or root == (ROOT/'data').resolve():
        raise ValueError('separate_research_output_directory_required')
    if root==Path(value['candle_root']).resolve():
        raise ValueError('output_must_not_be_input')
    return value


def contract(config):
    registry=kernel.feature_registry()
    return {'feature_names':[r['name'] for r in registry], 'feature_registry':registry,
            'kernel_metadata':kernel.schema_metadata(),'source_bindings':config['source_bindings'],
            'panel_registry':panel_kernel.panel_registry(),'panel_metadata':panel_kernel.panel_metadata(),
            'pairs':config['pairs'],'horizons_minutes':list(HORIZONS),
            'bar_availability':'bar_start+60 is inferred complete; original provider receipt absent in source CSV',
            'observation_availability':'actual read-completed and dataset publication clocks retained',
            'outcome_scope':'exact future minute-close bidask endpoint proxies, no fills/slippage/financing',**FLAGS}


def read_pair(path,pair,now,max_rows):
    for attempt in range(3):
        try:
            data,receipt=inputs.read_csv_tail(path,pair,observed_epoch=now,max_rows=max_rows)
            receipt['eligibility_cutoff_epoch']=now
            receipt['source_read_started_epoch']=now
            receipt['observed_epoch']=time.time()
            receipt['source_read_completed_epoch']=receipt['observed_epoch']
            return data,receipt
        except (OSError,ValueError) as exc:
            if attempt==2:
                raise
            time.sleep(.05)


def run_cycle(config,store,cache):
    begin=time.monotonic()
    now=time.time()
    root=Path(config['output_root'])
    free=shutil.disk_usage(root).free
    if free<config['minimum_free_bytes'] or store.size_bytes()>config['maximum_dataset_bytes']:
        raise RuntimeError('rolling_dataset_storage_guard')
    results={}
    families={r['name']:r['family'] for r in kernel.feature_registry()}
    total_inserted=total_settled=0
    for pair,pip in sorted(config['pairs'].items()):
        source=Path(config['candle_root'])/(pair+'_M1.csv')
        row={'status':'unavailable'}
        try:
            stat=source.stat()
            identity=(stat.st_size,stat.st_mtime_ns,stat.st_ino)
            if cache.get(pair)!=identity:
                data,receipt=read_pair(source,pair,time.time(),config['tail_rows'])
                if not len(data['time']):
                    raise ValueError('no_completed_source_rows')
                receipt.setdefault('observed_epoch',time.time())
                latest=store.latest_time(pair)
                # A missed backlog cannot silently be represented as continuous.
                if latest is not None and latest<int(data['time'][0]):
                    raise ValueError('backlog_exceeds_tail_run_historical_import')
                if latest is not None and latest>int(data['time'][-1]):
                    raise ValueError('source_latest_bar_regressed')
                if latest is not None:
                    needed=kernel.max_lookback_bars()-1
                    first_new=int(np.searchsorted(data['time'],latest,side='right'))
                    if first_new<needed and receipt.get('range_truncated',True):
                        data,receipt=read_pair(source,pair,time.time(),config['tail_rows']+needed)
                        first_new=int(np.searchsorted(data['time'],latest,side='right'))
                        if first_new<needed and receipt.get('range_truncated',True):
                            raise ValueError('backlog_context_exceeds_tail_run_historical_import')
                features=kernel.compute_features(data,pair,pip)
                lower=int(data['time'][max(0,len(data['time'])-config['bootstrap_rows'])]) if latest is None else latest+1
                inserted=store.ingest(pair,data,features,receipt,keep_from=lower)
                total_inserted+=inserted['inserted_observations']
                total_settled+=store.settle(pair)
                if not receipt.get('skipped_rows',{}).get('bar_end_after_observation',0):
                    cache[pair]=identity
            current=store.latest(pair)
            if current is None:
                raise ValueError('no_persisted_observation')
            age=time.time()-current['bar_end_epoch']
            available=Counter(families[n] for n,v in current['values'].items() if v is not None)
            missing=Counter(families[n] for n,v in current['values'].items() if v is None)
            row={'status':'current' if 0<=age<=config['maximum_bar_age_seconds'] else 'stale',
                 'bar_start_utc':utc(current['bar_start_epoch']),'bar_end_utc':utc(current['bar_end_epoch']),
                 'bar_age_seconds':round(age,3),'published_utc':utc(current['published_epoch']),
                 'available_features':current['available_features'],'feature_count':current['feature_count'],
                 'family_available':dict(available),'family_missing':dict(missing),
                 'missing_features':[n for n,v in current['values'].items() if v is None],
                 'missingness_basis':'feature-specific elapsed support, source availability or undefined denominator; never zero-filled',
                 'feature_hash':current['feature_hash']}
        except (OSError,ValueError,RuntimeError) as exc:
            row={'status':'unavailable','reason':type(exc).__name__+':'+str(exc)[:200]}
        results[pair]=row
    observations={}
    for pair in config['pairs']:
        current=store.latest(pair) if results[pair]['status']!='unavailable' else None
        if current is not None:
            age=time.time()-current['bar_end_epoch']
            results[pair]['bar_age_seconds']=round(age,3)
            results[pair]['status']='current' if 0<=age<=config['maximum_bar_age_seconds'] else 'stale'
        observations[pair]=current if results[pair]['status']=='current' else None
    present=[row for row in observations.values() if row is not None]
    panel_publication=None
    if present:
        target=max(row['bar_start_epoch'] for row in present)
        panel=panel_kernel.compute_panel(observations,target)
        panel_publication=store.publish_panel(panel,observations)
    counts=store.counts()
    published=time.time()
    report={'schema':SCHEMA,'generated_utc':utc(published),'generated_epoch':published,'pid':os.getpid(),
            'status':'collecting' if all(r['status']=='current' for r in results.values()) else 'partial',
            'universe_pairs':len(results),'pairs_with_observations':sum('available_features' in r for r in results.values()),
            'current_pairs':sum(r['status']=='current' for r in results.values()),
            'fully_populated_pairs':sum(r.get('available_features')==r.get('feature_count') and 'feature_count' in r for r in results.values()),
            'feature_count':len(store.names),'inserted_this_cycle':total_inserted,'outcomes_settled_this_cycle':total_settled,
            'peer_feature_count':len(panel_kernel.panel_registry()),
            'peer_panel':None if panel_publication is None else {
                'id':panel_publication['id'],'target_bar_start_utc':utc(panel_publication['target_bar_start_epoch']),
                'published_utc':utc(panel_publication['published_epoch']),
                'accepted_clock_pair_count':panel_publication['panel']['accepted_clock_pair_count'],
                'pair_status_counts':dict(Counter(r['status'] for r in panel_publication['panel']['by_pair'].values()))},
            'counts':counts,'outcome_states':{r[0]:r[1] for r in store.connection.execute('SELECT state,COUNT(*) FROM outcomes GROUP BY state')},
            'elapsed_seconds':round(time.monotonic()-begin,3),'dataset_bytes':store.size_bytes(),'disk_free_bytes':free,
            'pairs':results,'observation_scope':'technical_dataset_no_forecasts_or_trading_decisions',**FLAGS}
    atomic_json(root/'status.json',report)
    atomic_json(root/'latest_features.json',{'schema':SCHEMA,'generated_utc':utc(),
        'envelope_time_is_not_feature_freshness':True,
        'peer_panel_publication':panel_publication,
        'pairs':{pair:{'availability':results[pair],
                      'observation':observations[pair]}
                 for pair in config['pairs']},**FLAGS})
    return report


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',type=Path,required=True)
    parser.add_argument('--once',action='store_true')
    parser.add_argument('--duration-sec',type=int,default=604800)
    args=parser.parse_args(argv)
    if not 1<=args.duration_sec<=604800:
        raise ValueError('bounded_worker_duration_required')
    config=read_config(args.config)
    config_hash=sha(args.config)
    root=Path(config['output_root']);root.mkdir(parents=True,exist_ok=True)
    start=time.monotonic();cache={}
    with owner_lock(root):
        atomic_json(root/'owner.json',{'pid':os.getpid(),'started_utc':utc(),'duration_seconds':args.duration_sec,
                    'config_path':str(args.config.resolve()),'config_sha256':sha(args.config),**FLAGS})
        store=TechnicalStore(root/'technical.sqlite',contract(config),max_bytes=config['maximum_dataset_bytes'])
        try:
            atomic_json(root/'feature_registry.json',{'schema':SCHEMA,'metadata':kernel.schema_metadata(),
                        'features':kernel.feature_registry(),'peer_features':panel_kernel.panel_registry(),
                        'peer_metadata':panel_kernel.panel_metadata(),'contract':store.contract})
            while time.monotonic()-start<args.duration_sec:
                cycle=time.monotonic()
                try:
                    if sha(args.config)!=config_hash:
                        raise ValueError('running_dataset_config_changed_restart_separate_contract')
                    read_config(args.config)
                    report=run_cycle(config,store,cache)
                    print(json.dumps({k:report[k] for k in ('generated_utc','status','pairs_with_observations','current_pairs','fully_populated_pairs','inserted_this_cycle','elapsed_seconds')}),flush=True)
                except Exception as exc:
                    atomic_json(root/'status.json',{'schema':SCHEMA,'generated_utc':utc(),'status':'error',
                        'reason':type(exc).__name__+':'+str(exc)[:300],**FLAGS})
                    if args.once:
                        raise
                if args.once:
                    break
                time.sleep(max(1.,config['interval_seconds']-(time.monotonic()-cycle)))
        finally:
            store.close()


if __name__=='__main__':
    main()
