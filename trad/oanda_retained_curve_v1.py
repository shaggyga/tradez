"""Frozen preserved curve states over exact saved same-base horizon panels."""
import hashlib
import importlib
import json
import math
from pathlib import Path
import time
from oanda_retained_projection_v1 import digest,load_original,stable_parent

KIND='retained_curve_shape'
HORIZONS=(360,720,1080,1440,1800,2160,2520,2880)


class Curve:
    def __init__(self,root,registry):
        self.root=Path(root);self.registry=registry;config=registry['curve'];record=config['state']
        path=(self.root/record['path']).resolve()
        if not path.is_relative_to(self.root.resolve()) or path.stat().st_size>16*1024**2:raise ValueError('curve_state_path_or_size')
        raw=path.read_bytes()
        if hashlib.sha256(raw).hexdigest()!=record['sha256']:raise ValueError('curve_state_identity')
        self.bundle=json.loads(raw);b=self.bundle
        original=load_original(root);self.original=importlib.import_module(original.__package__+'.curve_shape_layer_v2')
        fp=self.original.fingerprint
        contract_path=self.root/'stage_c_alignment_integrity_v2/CURVE_SHAPE_LAYER_CONTRACT_V2.json'
        if (hashlib.sha256(contract_path.read_bytes()).hexdigest()!=config['contract_sha256']
            or b['schema']!='retained_curve_state_bundle.v1' or b['contract']!=json.loads(contract_path.read_bytes())
            or b['contract_sha256']!=fp(b['contract']) or b['source_variant']!='raw_matched_expanding'
            or b['source_result_sha256']!=config['source_result_sha256']
            or b['contract']['feature_definition']['horizons_minutes']!=list(HORIZONS)
            or b['contract']['feature_definition']['anchor_horizon_minutes']!=360):raise ValueError('curve_contract_binding')
        entries={e['id']:e for e in registry['connections']};self.parents=b['parents']
        if len(self.parents)!=16 or {(p['arm'],p['horizon_minutes']) for p in self.parents.values()}!={(a,h) for a in ('ridge','recovered_hgb') for h in HORIZONS}:raise ValueError('curve_parent_population')
        for name,p in self.parents.items():
            e=entries[name]
            if e['kind']!='legacy26_matched' or any(e[k]!=p[k] for k in ('horizon_minutes','arm','original_model_id','fit_metadata')):raise ValueError('curve_parent_identity')
            if json.loads((self.root/e['fit_metadata']['path']).read_bytes())['fit_id']!=p['fit_id']:raise ValueError('curve_parent_fit')
        if set(b['eligibility_snapshots'])!=set(map(str,HORIZONS)):raise ValueError('curve_eligibility_inventory')
        for h,s in b['eligibility_snapshots'].items():
            if (s['layer_id']!=fp({k:v for k,v in s.items() if k!='layer_id'}) or s['status']!='fitted'
                or s['cutoff_epoch']!=b['cutoff_epoch'] or s['scope']!=['legacy26',f'technical_endpoint_midpoint_elapsed_{h}m','frozen']):raise ValueError('curve_saved_eligibility')
        if set(b['states'])!={'ridge','recovered_hgb'}:raise ValueError('curve_state_population')
        for s in b['states'].values():
            if (s['layer_id']!=fp({k:v for k,v in s.items() if k!='layer_id'}) or s['status']!='fitted'
                or s['cutoff_epoch']!=b['cutoff_epoch'] or s['contract']!=b['contract']['layer_fit']
                or s['contract_sha256']!=fp(s['contract']) or s['support']['membership_sha256']!=fp(s['training_membership'])):raise ValueError('curve_state_contract')
            model=s['parameters']
            for key,width in [('mean',8),('scale',8),('coefficient',9)]:
                if len(model[key])!=width or not all(math.isfinite(x) and (key!='scale' or x>0) for x in model[key]):raise ValueError('curve_parameter_shape')
        self.entries=[e for e in entries.values() if e['kind']==KIND]
        if len(self.entries)!=2 or {e['arm'] for e in self.entries}!={'ridge','recovered_hgb'}:raise ValueError('curve_connection_inventory')
        for e in self.entries:
            p=entries[e['parent']]
            if e['horizon_minutes']!=360 or p['horizon_minutes']!=360 or p['arm']!=e['arm'] or e['models'] or e['original_model_id']!=p['original_model_id'] or e['layer_id']!=b['states'][e['arm']]['layer_id']:raise ValueError('curve_connection_identity')

    def append(self,forecasts,coverage,registry_sha256,*,clock=time.time):
        start=clock();diagnostics=[]
        for e in self.entries:
            arm=e['arm'];s=self.bundle['states'][arm];selected={};values=[];failure=None
            try:
                for f in forecasts:
                    p=self.parents.get(f['connection'])
                    if p is None or p['arm']!=arm:continue
                    key=(f['instrument'],f['horizon_minutes'])
                    if (key in selected or f['instrument'] not in self.registry['pairs'] or f['registry_sha256']!=registry_sha256
                        or f['original_model_id']!=p['original_model_id'] or f['horizon_minutes']!=p['horizon_minutes']
                        or type(f['reference_epoch']) is not int or f['target_epoch']!=f['reference_epoch']+f['horizon_minutes']*60
                        or not s['cutoff_epoch']<=f['reference_epoch']<f['issued_epoch']<=start<f['target_epoch']
                        or not 0<=start-f['reference_epoch']<=180 or not math.isfinite(f['expected_return_bps'])
                        or f['can_place_orders'] is not False or f['can_promote'] is not False or f['research_only'] is not True or f['models_fitted']!=0):raise ValueError('curve_parent_clock_or_identity')
                    selected[key]=f
                for pair in self.registry['pairs']:
                    part=[selected.get((pair,h)) for h in HORIZONS]
                    if any(f is None for f in part):continue
                    if len({f['reference_epoch'] for f in part})!=1:continue
                    if len({f['reference_mid'] for f in part})!=1:raise ValueError('curve_reference_price_mismatch')
                    rows=[]
                    for f in part:
                        row={'record_id':pair+':'+str(f['reference_epoch']),'instrument':pair,'base_method':arm,'decision_epoch':f['reference_epoch'],
                         'horizon_minutes':f['horizon_minutes'],'target_id':f"technical_endpoint_midpoint_elapsed_{f['horizon_minutes']}m",
                         'available_epoch':f['issued_epoch'],'prediction_bps':f['expected_return_bps']}
                        row['forecast_id']=self.original.fingerprint(row);rows.append(row)
                    panel=self.original.join_curve(rows,HORIZONS,360)[0];panel.update(source_variant='raw_matched_expanding',variant='curve_shape')
                    learned=self.original.apply(panel,s)
                    if learned is None:raise ValueError('curve_saved_state_not_applicable')
                    values.append({**part[0],'connection':e['id'],'expected_return_bps':learned['prediction_bps'],
                        'selection_scope':e['selection_scope'],'input_hash':digest([stable_parent(f) for f in part]),'panel_sha256':s['layer_id'],
                        'input_support':{'finite':8,'expected':8,'scope':'exact-origin same-base saved horizon forecasts'},
                        'curve':{'layer_id':s['layer_id'],'horizons_minutes':list(HORIZONS),'training_cutoff_epoch':s['cutoff_epoch'],
                            'saved_eligibility_snapshot_ids':{h:x['layer_id'] for h,x in self.bundle['eligibility_snapshots'].items()},
                            'source_result_sha256':self.bundle['source_result_sha256'],'frozen_reuse_of_expanding_prefix':True}})
                issued=clock()
                if issued<start or any(issued-f['reference_epoch']>180 or issued>=f['target_epoch'] for f in values):raise ValueError('curve_publication_expiry')
                for f in values:f['issued_epoch']=issued
            except Exception as exc:failure=type(exc).__name__+':'+str(exc)[:160];values=[]
            present={f['instrument'] for f in values}
            coverage.extend({'instrument':pair,'connection':e['id'],'horizon_minutes':360,
                'status':'curve_inference_unavailable' if failure else 'eligible' if pair in present else 'incomplete_exact_origin_curve',
                **({'reason':failure} if failure else {})} for pair in self.registry['pairs'])
            forecasts.extend(values);diagnostics.append({'connection':e['id'],'layer_id':s['layer_id'],'forecasts':len(values),'reason':failure})
        return diagnostics
