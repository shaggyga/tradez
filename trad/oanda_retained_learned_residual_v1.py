"""Apply authenticated preserved residual states; never fit or consume outcomes."""
import hashlib
import importlib
import json
import math
from pathlib import Path
import time

from oanda_retained_projection_v1 import digest, load_original, stable_parent

KIND='retained_learned_residual'
CONTRACT={'minimum_distinct_origins':8,'minimum_distinct_utc_days':3,
          'minimum_distinct_pairs':20,'minimum_residual_weight':0.,'maximum_residual_weight':1.}


class LearnedResidual:
    def __init__(self,root,registry):
        self.root=Path(root);self.registry=registry;self.config=registry['learned_residual']
        module=load_original(root)
        self.original=importlib.import_module(module.__package__+'.currency_projection_residual_layer_v2')
        self.states={};entries={e['id']:e for e in registry['connections']}
        self.entries=[e for e in entries.values() if e['kind']==KIND]
        parents=registry['projection']['parents']
        if set(self.config['states'])!=set(parents):raise ValueError('residual_state_inventory')
        for parent,record in self.config['states'].items():
            path=(self.root/record['path']).resolve()
            if not path.is_relative_to(self.root.resolve()):raise ValueError('residual_state_path')
            raw=path.read_bytes()
            if len(raw)>4*1024**2 or hashlib.sha256(raw).hexdigest()!=record['sha256']:raise ValueError('residual_state_identity')
            v=json.loads(raw);s=v['snapshot'];support=s['support'];p=entries[parent]
            if (v['schema']!='retained_learned_residual_state.v1' or v['parent']!=parents[parent]
                or v['source_result_sha256']!=self.config['source_result_sha256']
                or s['layer_id']!=self.original.fingerprint({k:x for k,x in s.items() if k!='layer_id'})
                or s['contract']!=CONTRACT or s['contract_sha256']!=self.original.fingerprint(s['contract'])
                or s['status']!='fitted' or s['base_method']!=p['arm'] or s['horizon_minutes']!=p['horizon_minutes']
                or type(s['cutoff_epoch']) is not int or s['cutoff_epoch']!=self.config['cutoff_epoch']
                or not 0<=s['residual_weight']<=1 or s['base_models_refitted'] is not False or s['outcomes_revealed'] is not False
                or v['maximum_training_outcome_available_epoch']>s['cutoff_epoch']
                or support['membership_sha256']!=self.original.fingerprint(support['training_membership'])
                or support['rows']!=len(support['training_membership'])):raise ValueError('residual_state_contract_or_parent')
            for field in ('distinct_origins','distinct_utc_days','distinct_pairs'):
                if support[field]<CONTRACT['minimum_'+field]:raise ValueError('residual_state_support')
            self.states[parent]=s
        nonzero={p for p,s in self.states.items() if s['residual_weight']!=0}
        if len(self.entries)!=len(nonzero) or {e['parent'] for e in self.entries}!=nonzero:raise ValueError('residual_distinct_inventory')
        aliases={p:p+'__currency_projection' for p,s in self.states.items() if s['residual_weight']==0}
        if self.config['equivalent_connections']!=aliases:raise ValueError('residual_equivalence_inventory')
        for e in self.entries:
            p=entries[e['parent']]
            if (e['models'] or e['horizon_minutes']!=p['horizon_minutes'] or e['original_model_id']!=p['original_model_id']
                or e['layer_id']!=self.states[e['parent']]['layer_id']):raise ValueError('residual_connection_identity')

    def append(self,forecasts,coverage,registry_sha256,*,clock=time.time):
        start=clock();diagnostics=[]
        for e in self.entries:
            parent=e['parent'];s=self.states[parent];base=s['base_method'];h=s['horizon_minutes']
            ids={parent:'direct',parent+'__currency_projection':'currency_projection',parent+'__half_residual':'half_residual'}
            lookup={};result=[];failure=None
            try:
                for f in forecasts:
                    if f['connection'] not in ids:continue
                    key=(f['instrument'],ids[f['connection']])
                    if (key in lookup or f['instrument'] not in self.registry['pairs']
                        or f['original_model_id']!=e['original_model_id'] or f['registry_sha256']!=registry_sha256
                        or f['horizon_minutes']!=h or type(f['reference_epoch']) is not int
                        or f['target_epoch']!=f['reference_epoch']+h*60
                        or not s['cutoff_epoch']<=f['reference_epoch']<f['issued_epoch']<=start<f['target_epoch']
                        or not 0<=start-f['reference_epoch']<=180
                        or f['can_place_orders'] is not False or f['can_promote'] is not False
                        or f['research_only'] is not True or f['models_fitted']!=0
                        or not math.isfinite(f['expected_return_bps']) or f['expected_return_bps']<=-10000):
                        raise ValueError('residual_control_identity_clock_or_value')
                    lookup[key]=f
                for pair in self.registry['pairs']:
                    part={v:lookup.get((pair,v)) for v in ids.values()}
                    if any(f is None for f in part.values()):continue
                    direct=part['direct'];rows=[]
                    for variant,f in part.items():
                        if (f['reference_epoch']!=direct['reference_epoch'] or f['target_epoch']!=direct['target_epoch']
                            or f['reference_mid']!=direct['reference_mid']):raise ValueError('residual_control_alignment')
                        if variant!='direct' and (f['projection']['parent_connection']!=parent
                            or f['projection']['parent_forecast_sha256']!=digest(stable_parent(direct))):raise ValueError('residual_projection_parent')
                        row={'record_id':pair+':'+str(f['reference_epoch']),'instrument':pair,'base_method':base,'variant':variant,
                             'origin_epoch':f['reference_epoch'],'target_epoch':f['target_epoch'],'horizon_minutes':h,
                             'target_id':f'technical_endpoint_midpoint_elapsed_{h}m','available_epoch':f['issued_epoch'],
                             'prediction_bps':f['expected_return_bps']}
                        row['forecast_id']=self.original.fingerprint(row);rows.append(row)
                    learned=self.original.apply(rows,{(base,h):s})
                    if len(learned)!=1:raise ValueError('residual_application_population')
                    result.append({**direct,'connection':e['id'],'expected_return_bps':learned[0]['prediction_bps'],
                        'selection_scope':e['selection_scope'],'input_hash':digest([stable_parent(part[v]) for v in ids.values()]),
                        'panel_sha256':s['layer_id'],'input_support':part['currency_projection']['input_support'],
                        'projection':{'variant':'learned_residual','learned_residual':True,'parent_connection':parent,
                            'layer_id':s['layer_id'],'residual_weight':s['residual_weight'],'training_cutoff_epoch':s['cutoff_epoch'],
                            'source_result_sha256':self.config['source_result_sha256']}})
                issued=clock()
                if issued<start or any(issued-f['reference_epoch']>180 or issued>=f['target_epoch'] for f in result):raise ValueError('residual_publication_expiry')
                for f in result:f['issued_epoch']=issued
            except Exception as exc:
                failure=type(exc).__name__+':'+str(exc)[:160];result=[]
            present={f['instrument'] for f in result}
            coverage.extend({'instrument':pair,'connection':e['id'],'horizon_minutes':h,
                'status':'residual_inference_unavailable' if failure else 'eligible' if pair in present else 'exact_controls_unavailable',
                **({'reason':failure} if failure else {})} for pair in self.registry['pairs'])
            forecasts.extend(result);diagnostics.append({'connection':e['id'],'layer_id':s['layer_id'],'forecasts':len(result),'reason':failure})
        return diagnostics
