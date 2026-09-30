"""Inference from authenticated saved models and original live feature receipts.

No fitting, source acquisition, account access or trading. Selection is a dated
development-evidence decision, not proof of an optimal model or future edge.
"""
from pathlib import Path
import hashlib
import io
import json
import math
import os
import sqlite3
import shutil
import time
import uuid
import zlib

import joblib
import numpy as np
import pandas as pd
from threadpoolctl import threadpool_limits

import oanda_all68_technical_availability_v2 as availability
import oanda_rolling_model_design_v1 as design
import oanda_rolling_specialists_v1 as specialists
import oanda_rolling_technical_panel_v1 as panel

ROOT=Path(__file__).resolve().parent
SCHEMA='retained_forecast_connection_v1_20260930'
REGISTRY=ROOT/'config/retained_forecast_connection_20260930.json'
OUTPUT=ROOT/'data/retained_connection_20260930'
FLAGS={'can_place_orders':False,'can_promote':False,'models_fitted':0,'research_only':True}
CONTEXT=('m1__return_15_bps','m1__return_60_bps','m1__path_efficiency_15',
         'm1__return_vol_15_pips','m1__return_vol_60_pips','m1__spread_ratio_prior_120',
         'm1__utc_hour_sin','m1__utc_hour_cos')


def encoded(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()


def sha(raw):return hashlib.sha256(raw).hexdigest()


def checked(root,record):
    path=(root/record['path']).resolve()
    if not path.is_relative_to(root.resolve()) or not path.is_file():raise ValueError('contained_artifact_required')
    if path.stat().st_size>64*1024**2:raise ValueError('artifact_size_bound')
    raw=path.read_bytes()
    if sha(raw)!=record['sha256']:raise ValueError('artifact_hash_mismatch:'+record['path'])
    return raw


def capture_rows():
    """Use the original reader's source, receipt, clock and support checks."""
    cfg,reference=availability.read_config(availability.DEFAULT_CONFIG)
    operations,_=availability.verify_dependencies(cfg,reference,availability.DEFAULT_OPERATIONS_CONFIG)
    root=Path(cfg['output_root'])
    before,_=availability.read_json(root/'status.json',availability.MAX_SMALL_BYTES)
    envelope,envelope_ref=availability.read_json(root/'latest_features.json',availability.MAX_ENVELOPE_BYTES)
    after,_=availability.read_json(root/'status.json',availability.MAX_SMALL_BYTES)
    status=next((s for s in (before,after) if s.get('publication_generation')==envelope.get('publication_generation')),None)
    quotes,_=availability.read_json(availability.DEFAULT_QUOTES,availability.MAX_SMALL_BYTES)
    heartbeat,_=availability.read_json(availability.DEFAULT_HEARTBEAT,availability.MAX_SMALL_BYTES)
    retained,errors=availability.read_store_evidence(root/'technical.sqlite',cfg['pairs'],envelope,config=cfg,operations=operations)
    now=time.time()
    report=availability.build_report(cfg,envelope,status,quotes,heartbeat,retained,errors,now=now,
        source_references={'envelope':envelope_ref},maximum_quote_age=60)
    names=[v['name'] for v in availability.kernel.feature_registry()]
    rows={}
    for pair,state in report['pairs'].items():
        if state['status'] not in ('complete','partial'):continue
        item=retained[pair];bar=item['bar']
        close,bid,ask=(bar[k] for k in ('close','bid_close','ask_close'))
        if any(isinstance(x,bool) or not isinstance(x,(int,float)) or not math.isfinite(x) for x in (close,bid,ask)) or not 0<bid<=close<=ask:continue
        features=dict(zip(names,item['values']))
        features.update(known_entry_long_cost_bps=(ask-close)/close*10000,
                        known_entry_short_cost_bps=(close-bid)/close*10000)
        rows[pair]={'bar_start_epoch':item['t'],'values':features,
                    'entry_long':(ask-close)/close*10000,'entry_short':(close-bid)/close*10000,
                    'reference_mid':close,'feature_hash':item['feature_hash'],'receipt_id':item['receipt_id'],
                    'input_hash':item['input_hash'],'published_epoch':item['published_epoch'],
                    'captured_epoch':now}
    # Compute only the original exact-clock peer transform from verified rows.
    for at in sorted({v['bar_start_epoch'] for v in rows.values()}):
        aligned={p:v for p,v in rows.items() if v['bar_start_epoch']==at}
        peer=panel.compute_panel(aligned,int(at))
        for p,v in aligned.items():
            values=peer['by_pair'][p]['values']
            v['values'].update(values)
            v['panel_sha256']=peer['by_pair'][p]['provenance_sha256']
    return rows,report


class SavedConnection:
    def __init__(self,registry_path):
        self.path=Path(registry_path).resolve();raw=self.path.read_bytes()
        self.registry=json.loads(raw);self.registry_sha256=sha(raw)
        if self.registry.get('schema')!=SCHEMA or self.registry.get('models_fitted')!=0:
            raise ValueError('inference_only_registry_required')
        self.root=ROOT.parent
        pairs=self.registry['pairs']
        if len(pairs)!=68 or len(set(pairs))!=68:raise ValueError('exact_pair_universe_required')
        entries=self.registry['connections']
        if not 1<=len(entries)<=24 or len({e['id'] for e in entries})!=len(entries):raise ValueError('bounded_unique_connections_required')
        for e in entries:
            if (e['kind'] not in ('rolling','specialist','rich_pipeline') or type(e['horizon_minutes']) is not int
                or e['horizon_minutes']<=0 or not e['feature_names'] or len(set(e['feature_names']))!=len(e['feature_names'])
                or not e.get('selection_scope') or not e.get('evidence')):raise ValueError('exact_model_target_contract_required')
        for name,expected in self.registry['source_bindings'].items():
            path=(self.root/name).resolve()
            if not path.is_relative_to(self.root) or sha(path.read_bytes())!=expected:
                raise ValueError('inference_source_changed:'+name)
        self.normalizers={}
        for name,record in self.registry['normalizers'].items():
            with np.load(io.BytesIO(checked(self.root,record)),allow_pickle=False) as data:
                self.normalizers[name]={k:data[k].copy() for k in data.files}
        self.models={}
        for entry in self.registry['connections']:
            if entry['id'] in self.models:raise ValueError('duplicate_connection')
            self.models[entry['id']]=[joblib.load(io.BytesIO(checked(self.root,r))) for r in entry['models']]

    def verify_sources(self):
        if sha(self.path.read_bytes())!=self.registry_sha256:raise ValueError('registry_changed_reload_required')
        for name,expected in self.registry['source_bindings'].items():
            if sha((self.root/name).read_bytes())!=expected:raise ValueError('inference_source_changed:'+name)

    def predict_entry(self,entry,pairs,rows):
        names=entry['feature_names'];ids=np.array([self.registry['pairs'].index(p) for p in pairs],dtype=np.int64)
        if any(set(names)-set(rows[p]['values']) for p in pairs):raise ValueError('required_feature_keys_missing')
        x=np.array([[rows[p]['values'].get(n,np.nan) for n in names] for p in pairs],dtype=np.float64)
        models=self.models[entry['id']]
        left=np.array([rows[p]['entry_long'] for p in pairs]);right=np.array([rows[p]['entry_short'] for p in pairs])
        if entry['kind']=='rich_pipeline':
            frame=pd.DataFrame(x,columns=names)
            if list(models[0].feature_names_in_)!=names:raise ValueError('saved_rich_feature_order')
            return models[0].predict(frame)
        params=self.normalizers[entry['normalizer']]
        if any(params[k].shape!=(68,len(names)) for k in ('mean','scale','supported')):raise ValueError('normalizer_axes')
        z=np.empty_like(x,dtype=np.float32)
        for i,pid in enumerate(ids):
            per={k:params[k][pid] for k in ('mean','scale','supported')}
            z[i]=design.transform_inputs(x[i:i+1],per)[0]
        if entry['kind']=='rolling':
            model=models[0]
            if model['selected_feature_names']!=names or model['horizon_minutes']!=entry['horizon_minutes'] or model['pair_names']!=self.registry['pairs']:
                raise ValueError('saved_rolling_model_identity')
            return model['estimator'].predict(design.learner_inputs(z,ids,model['learner'],len(self.registry['pairs'])))
        if entry['kind']=='specialist':
            head=models[0]
            if head['horizon_minutes']!=entry['horizon_minutes'] or head['fold_index']!=4:raise ValueError('saved_specialist_identity')
            heads,_=specialists.predict_heads(head['bundle'],np.column_stack((z[:,:entry['width']],left,right,ids)))
            if entry['arm']=='mixture_raw':return heads['probability']*heads['positive']-(1-heads['probability'])*heads['nonpositive']
            context=z[:,[names.index(n) for n in CONTEXT]]
            meta=specialists.meta_features(heads,context,left,right)
            return specialists.predict_meta(models[1]['bundle'],meta,ids)
        raise ValueError('unknown_saved_model_adapter')

    def predict(self,rows,report,*,clock=time.time):
        now=clock()
        output=[];coverage=[]
        for entry in self.registry['connections']:
            pairs=[]
            for pair in self.registry['pairs']:
                row=rows.get(pair);reason='eligible'
                if row is None:reason=(report.get('pairs',{}).get(pair) or {}).get('status','input_unavailable')
                elif not row['bar_start_epoch']+60<=row['published_epoch']<=row['captured_epoch']<=now:reason='input_clock_order'
                elif not 0<=now-row['bar_start_epoch']-60<=180:reason='input_stale'
                elif now>=row['bar_start_epoch']+60+entry['horizon_minutes']*60:reason='target_already_mature'
                coverage.append({'instrument':pair,'connection':entry['id'],'horizon_minutes':entry['horizon_minutes'],'status':reason})
                if reason=='eligible':pairs.append(pair)
            if not pairs:continue
            try:
                with threadpool_limits(limits=1):values=self.predict_entry(entry,pairs,rows)
                if len(values)!=len(pairs) or not np.isfinite(values).all():raise ValueError('nonfinite_or_wrong_prediction_population')
            except Exception as exc:
                for c in coverage:
                    if c['connection']==entry['id'] and c['status']=='eligible':
                        c['status']='inference_unavailable';c['reason']=type(exc).__name__+':'+str(exc)[:160]
                continue
            issued=clock()
            for pair,value in zip(pairs,values):
                row=rows[pair]
                reason=None
                if issued<now:reason='publication_clock_reversed'
                elif issued>=row['bar_start_epoch']+60+entry['horizon_minutes']*60:reason='target_matured_during_inference'
                elif issued-row['bar_start_epoch']-60>180:reason='input_expired_during_inference'
                if reason:
                    next(c for c in coverage if c['connection']==entry['id'] and c['instrument']==pair)['status']=reason
                    continue
                output.append({'instrument':pair,'connection':entry['id'],'horizon_minutes':entry['horizon_minutes'],
                    'reference_epoch':row['bar_start_epoch']+60,'issued_epoch':issued,
                    'target_epoch':row['bar_start_epoch']+60+entry['horizon_minutes']*60,
                    'expected_return_bps':float(value),'no_change_control_bps':0.,'reference_mid':row['reference_mid'],
                    'feature_hash':row['feature_hash'],'receipt_id':row['receipt_id'],'input_hash':row['input_hash'],
                    'panel_sha256':row.get('panel_sha256'),
                    'selection_scope':entry['selection_scope'],'registry_sha256':self.registry_sha256,**FLAGS})
        return {'schema':SCHEMA,'generated_epoch':clock(),'registry_sha256':self.registry_sha256,
                'status':'current' if output else 'inputs_unavailable','forecasts':output,'coverage':coverage,
                'connections':self.registry['connections'],'unconnected_targets':self.registry['unconnected_targets'],
                'limits':self.registry['limits'],'data_counts':report.get('counts',{}),**FLAGS}


def atomic(path,value):
    raw=encoded(value)
    if len(raw)>4*1024**2:raise ValueError('publication_size_bound')
    tmp=path.with_name(path.name+'.'+uuid.uuid4().hex+'.tmp')
    try:
        tmp.write_bytes(raw)
        for attempt in range(5):
            try:tmp.replace(path);return
            except PermissionError:
                if attempt==4:raise
                time.sleep(.05)
    finally:tmp.unlink(missing_ok=True)


def publish_once(connection,output=OUTPUT):
    """Called by the existing bounded context worker; never starts another service."""
    output=Path(output);output.mkdir(parents=True,exist_ok=True)
    connection.verify_sources()
    if shutil.disk_usage(output).free<32*1024**3:raise ValueError('free_space_guard')
    database=output/'issued.sqlite'
    if database.exists() and database.stat().st_size>2*1024**3:raise ValueError('retained_issuance_store_limit')
    rows,report=capture_rows();value=connection.predict(rows,report)
    connection.verify_sources()
    # Persist full issued outputs compressed; the original feature store retains
    # input bytes under the hashes/receipts in each prediction. No issue backdating.
    raw=encoded(value)
    with sqlite3.connect(database,timeout=2) as db:
        db.execute('CREATE TABLE IF NOT EXISTS issues (id TEXT PRIMARY KEY, issued REAL NOT NULL, payload_zlib BLOB NOT NULL)')
        db.execute('INSERT INTO issues VALUES (?,?,?)',(sha(raw),value['generated_epoch'],zlib.compress(raw,6)))
    value['payload_sha256']=sha(raw)
    atomic(output/'current.json',value)
    return {'status':value['status'],'generated_epoch':value['generated_epoch'],'forecasts':len(value['forecasts']),
            'registry_sha256':connection.registry_sha256,'models_fitted':0}


def read_current(path=OUTPUT/'current.json',registry_path=REGISTRY,*,now=None):
    """Fail closed on source/registry drift, expiry, target or output mismatch."""
    now=time.time() if now is None else now
    try:
        path=Path(path)
        if path.stat().st_size>4*1024**2:raise ValueError('publication_size')
        v=json.loads(path.read_bytes());regraw=Path(registry_path).read_bytes();reg=json.loads(regraw)
        if (v.get('schema')!=SCHEMA or v.get('registry_sha256')!=sha(regraw)
            or any(v.get(k)!=x for k,x in FLAGS.items())
            or v.get('payload_sha256')!=sha(encoded({k:x for k,x in v.items() if k!='payload_sha256'}))
            or not 0<=now-v['generated_epoch']<=120):raise ValueError('publication_identity_or_freshness')
        for name,expected in reg['source_bindings'].items():
            checked(ROOT.parent,{'path':name,'sha256':expected})
        entries={e['id']:e for e in reg['connections']};seen=set();forecasts=[]
        for f in v['forecasts']:
            e=entries[f['connection']];key=(f['instrument'],f['connection'])
            if key in seen or f['instrument'] not in reg['pairs']:raise ValueError('forecast_population')
            seen.add(key)
            if (any(f.get(k)!=x for k,x in FLAGS.items()) or f['registry_sha256']!=sha(regraw)
                or f['horizon_minutes']!=e['horizon_minutes']
                or f['target_epoch']!=f['reference_epoch']+e['horizon_minutes']*60
                or not f['reference_epoch']<=f['issued_epoch']<=v['generated_epoch']<=now
                or not math.isfinite(f['expected_return_bps'])):raise ValueError('forecast_identity')
            if now-f['reference_epoch']<=180 and f['target_epoch']>now:forecasts.append(f)
        current={(f['instrument'],f['connection']) for f in forecasts}
        coverage=[{**c,'status':'expired'} if c['status']=='eligible' and (c['instrument'],c['connection']) not in current else c for c in v['coverage']]
        return {**v,'forecasts':forecasts,'coverage':coverage,'status':'current' if forecasts else 'inputs_unavailable',
                'connections':reg['connections'],'unconnected_targets':reg['unconnected_targets'],'limits':reg['limits']}
    except (OSError,ValueError,KeyError,TypeError,OverflowError):
        return {'schema':SCHEMA,'status':'unavailable','forecasts':[],'coverage':[],**FLAGS}
