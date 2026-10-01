"""Fixed forecast-edge projection through the preserved currency solver."""
from collections import Counter,defaultdict
from dataclasses import dataclass
import hashlib,importlib,importlib.machinery,importlib.util,math,sys
from pathlib import Path
try:
    from .contracts import fingerprint
except ImportError:  # Original standalone research operators.
    from contracts import fingerprint

BASES=('ridge','recovered_hgb')
VARIANTS=('direct','currency_projection','half_residual')


def load_solver(root,pins):
    root=Path(root).resolve()
    for n,d in pins.items():
        p=root/n
        if p.is_symlink() or p.stat().st_size!=d['bytes'] or hashlib.sha256(p.read_bytes()).hexdigest()!=d['sha256']:
            raise ValueError('projection_preserved_solver_changed_before_import')
    prefix='_forex_projection_reuse_'+hashlib.sha256(str(root).encode()).hexdigest()[:16]
    for suffix,path in [('',root),('.features',root/'features'),('.contracts',root/'contracts')]:
        name=prefix+suffix
        if name in sys.modules:
            if list(sys.modules[name].__path__)!=[str(path)]:raise ValueError('projection_solver_namespace_shadow')
        else:
            spec=importlib.machinery.ModuleSpec(name,loader=None,is_package=True)
            module=importlib.util.module_from_spec(spec);module.__path__=[str(path)];sys.modules[name]=module
    module=importlib.import_module(prefix+'.features.currency_state_engine')
    if Path(module.__file__).resolve()!=root/'features/currency_state_engine.py':raise ValueError('projection_solver_wrong_origin')
    return module.solve_weighted_currency_state


@dataclass(frozen=True)
class ForecastEdge:
    instrument:str
    base_currency:str
    quote_currency:str
    forecast_log_return_bps:float
    forecast_available_epoch:int
    source_forecast_id:str
    weight:float=1.0

    @property
    def return_bps(self):return self.forecast_log_return_bps

    @property
    def end_epoch(self):return self.forecast_available_epoch


def project_frame(frame,c,solver):
    origin=frame['origin_epoch'];h=frame['horizon_minutes']
    if origin not in c['origins'] or h not in c['horizons_minutes']:raise ValueError('projection_unregistered_frame')
    source=[p for p in frame['predictions'] if p['variant']=='raw_unrestricted']
    cov=[p for p in frame['coverage'] if p['variant']=='raw_unrestricted']
    expected={(pair,base) for pair in c['universe'] for base in BASES}
    if len(cov)!=len(expected) or {(p['instrument'],p['base_method']) for p in cov}!=expected:raise ValueError('projection_all68_raw_coverage')
    by={}
    for p in source:
        key=p['instrument'],p['base_method']
        if key in by or p['forecast_id']!=fingerprint({k:v for k,v in p.items() if k!='forecast_id'}):raise ValueError('projection_original_prediction_identity')
        if (p['decision_epoch']!=origin or p['origin_epoch']!=origin or p['target_epoch']!=origin+h*60 or p['horizon_minutes']!=h or
            p['target_id']!=f'technical_endpoint_midpoint_elapsed_{h}m' or p['record_id']!=p['instrument']+':'+str(origin) or
            p['available_epoch']<=origin or p['available_epoch']>frame['reserved_ready_epoch']):raise ValueError('projection_original_clock_target')
        value=float(p['prediction_bps'])
        if not math.isfinite(value) or value<=-10000:raise ValueError('projection_invalid_simple_return')
        by[key]=p
    if set(by)!={(p['instrument'],p['base_method']) for p in cov if p['reason']=='eligible'}:raise ValueError('projection_source_eligibility_inventory')
    rows=[];coverage=[];diagnostics=[]
    currencies=sorted({x for pair in c['universe'] for x in pair.split('_')})
    for base in BASES:
        selected=sorted((p for p in source if p['base_method']==base),key=lambda p:p['instrument'])
        edges=[ForecastEdge(p['instrument'],*p['instrument'].split('_'),math.log1p(float(p['prediction_bps'])/10000)*10000,p['available_epoch'],p['forecast_id']) for p in selected]
        if any(abs(e.forecast_log_return_bps)>c['solver_policy']['return_clip_minimum_bps'] for e in edges):raise ValueError('projection_log_return_bound_no_silent_clipping')
        result=solver(edges,currencies=currencies,policy=c['solver_policy'])
        active=set(result.get('active_currencies',[])) if result['status'] in ('ok','degraded_disconnected') else set()
        strengths=result.get('strengths_bps',{})
        ready=max((p['available_epoch'] for p in selected),default=origin)+c['projection_slot_seconds']
        constituents=fingerprint(sorted(p['forecast_id'] for p in selected))
        diagnostics.append({'base_method':base,'status':result['status'],'active_currencies':sorted(active),'components':result['components'],
            'forecast_edge_count':len(edges),'matrix_rank':result.get('matrix_rank'),'condition_number':result.get('condition_number'),
            'forecast_factors_log_bps':strengths,'reserved_ready_epoch':ready,
            'constituent_forecast_ids':sorted(p['forecast_id'] for p in selected),'constituent_snapshot_sha256':constituents,
            'scope':'numeric_solver_reuse_on_forecast_edges; not_observed_currency_response_or_predictive_uncertainty'})
        for pair in c['universe']:
            p=by.get((pair,base));a,b=pair.split('_');projectable=p is not None and a in active and b in active
            for variant in VARIANTS:
                reason='base_unavailable' if p is None else 'eligible' if variant=='direct' or projectable else 'projection_component_unavailable'
                method=base+'__'+variant;coverage.append({'instrument':pair,'method':method,'origin_epoch':origin,'target_epoch':origin+h*60,'horizon_minutes':h,'reason':reason})
                if reason!='eligible':continue
                value=float(p['prediction_bps'])
                if variant!='direct':
                    original=math.log1p(value/10000)*10000;factor=strengths[a]-strengths[b]
                    projected=factor if variant=='currency_projection' else factor+0.5*(original-factor)
                    value=math.expm1(projected/10000)*10000
                row={'method':method,'base_method':base,'variant':variant,'instrument':pair,'record_id':p['record_id'],
                    'origin_epoch':origin,'target_epoch':p['target_epoch'],'target_id':p['target_id'],'horizon_minutes':h,
                    'prediction_bps':value,'available_epoch':p['available_epoch'] if variant=='direct' else ready,
                    'source_forecast_id':p['forecast_id'],'source_model_id':p['model_id'],'signed_fit_id':p['signed_fit_id'],
                    'source_available_epoch':p['available_epoch'],'observed_publication':False,'native_policy_admitted':False}
                row['constituent_snapshot_sha256']=constituents
                row['layer_definition_sha256']=fingerprint({'transform':'simple_to_log_factor_fixed_residual_v1','variant':variant,
                    'solver_policy':c['solver_policy'],'solver_source_hashes':c['solver_source_hashes'],'slot_seconds':c['projection_slot_seconds']})
                row['forecast_id']=fingerprint(row);rows.append(row)
    return {'origin_epoch':origin,'horizon_minutes':h,'coverage':coverage,'predictions':rows,'solver_diagnostics':diagnostics,'source_frame_sha256':fingerprint(frame)}


def assess(frames,outcomes,c):
    lookup={}
    for o in outcomes:
        key=o['record_id'],o['target_id']
        if key in lookup:raise ValueError('projection_duplicate_original_label')
        lookup[key]=o
    groups=defaultdict(list);daily=defaultdict(list);coverage=Counter();pairwise=defaultdict(dict);maturity=Counter()
    for f in frames:
        for x in f['coverage']:coverage[x['reason']]+=1
        for p in f['predictions']:
            o=lookup.get((p['record_id'],p['target_id']))
            if o is None or o['label_end_epoch']!=p['target_epoch'] or o['available_epoch']<p['target_epoch']:raise ValueError('projection_exact_original_label')
            status='not_mature' if o['available_epoch']>c['assessment_asof'] else 'unavailable' if o['value'] is None else 'mature'
            maturity[status]+=1
            if status!='mature':continue
            value=float(o['value'])
            if not math.isfinite(value):raise ValueError('projection_nonfinite_label')
            item=(p['prediction_bps'],value,p['record_id'],p['forecast_id'])
            groups[p['method'],p['horizon_minutes']].append(item);daily[p['method'],p['horizon_minutes'],p['origin_epoch']//86400].append(item)
            pairwise[p['base_method'],p['horizon_minutes'],p['record_id']][p['variant']]=item
    def score(xs):
        es=[p-y for p,y,_,_ in xs];nonzero=[(p,y) for p,y,_,_ in xs if p!=0 and y!=0]
        return {'mature_rows':len(xs),'mae_bps':math.fsum(map(abs,es))/len(es) if es else None,'mse_bps2':math.fsum(e*e for e in es)/len(es) if es else None,
            'bias_bps':math.fsum(es)/len(es) if es else None,'direction_nonzero_rows':len(nonzero),'direction_matches':sum(p*y>0 for p,y in nonzero),
            'support_sha256':fingerprint(sorted(x[2] for x in xs))}
    matched=[]
    for base in BASES:
        for h in c['horizons_minutes']:
            for variant in VARIANTS[1:]:
                pairs=[v for (b,hh,_),v in pairwise.items() if b==base and hh==h and variant in v and 'direct' in v]
                left=score([p[variant] for p in pairs]);right=score([p['direct'] for p in pairs])
                assert left['support_sha256']==right['support_sha256']
                matched.append({'base_method':base,'horizon_minutes':h,'variant':variant,'projected':left,'direct':right,
                    'mae_delta_bps':None if not pairs else left['mae_bps']-right['mae_bps'],'mse_delta_bps2':None if not pairs else left['mse_bps2']-right['mse_bps2']})
    return {'scores':[{'method':m,'horizon_minutes':h,**score(v)} for (m,h),v in sorted(groups.items())],
        'daily_scores':[{'method':m,'horizon_minutes':h,'utc_day_index':d,**score(v)} for (m,h,d),v in sorted(daily.items())],
        'matched_comparisons':matched,'coverage_reasons':dict(coverage),'outcome_status_counts':dict(maturity),'frames':len(frames),
        'coverage_slots':sum(len(f['coverage']) for f in frames),'predictions':sum(len(f['predictions']) for f in frames),
        'scope':'inspected_development; overlapping_horizons_and_currency_edges_dependent','confirmation':False,'independent_review':False}
