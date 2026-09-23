"""Reuse original research records for day-shift controls and raw-input leak checks."""
from collections import defaultdict,Counter
from blocked_time_controls_v2 import compare
from leak_positive_audit_v2 import build as leak_audit

def build(reader,raw_root,recipe):
    cfg=recipe['configuration'];c=reader.read('family','experiment_contract.json');base=reader.read('baseline','forecasts.json');outcomes=[]
    for pair in cfg['universe']:outcomes.extend(reader.read('rich','outcomes_'+pair+'.json')['outcomes'])
    indexed=defaultdict(list)
    for row in base:indexed[row['forecast']['target_id'],row['procedure'],row['method']].append(row)
    scopes=[];shifts=[]
    for group in c['groups']:
        for minutes in c['horizon_minutes']:
            target=f'technical_endpoint_midpoint_elapsed_{minutes}m'
            for procedure in c['procedures']:
                name=f'forecasts_{group}_{minutes}_{procedure}.json';rows=reader.read('family',name)
                for method in c['methods']:
                    selected=[r for r in rows if r['method']==method]
                    for control in ('zero','history_mean','ridge','recovered_hgb'):
                        original=indexed[target,procedure,control]
                        scope,values=compare(selected,original,outcomes,asof=c['evaluation_asof'],universe=cfg['universe'])
                        key={'group':group,'target_id':target,'procedure':procedure,'method':method,'control':control}
                        scopes.append({**key,**scope,'forecast_source':reader.source('family',name,selected),'control_source':reader.source('baseline','forecasts.json',original)})
                        shifts.extend({**key,**v} for v in values)
    if len(scopes)!=336:raise ValueError('all_declared_controls_required')
    leaks=leak_audit(raw_root,recipe['raw_manifest_sha256'],cfg)
    report={'status':'blocked_time_and_leak_positive_diagnostics','paired_comparisons':len(scopes),'day_shift_rows':len(shifts),'day_counts':sorted({s['day_count'] for s in scopes}),
        'balanced_pair_counts':sorted({len(s['balanced_instruments']) for s in scopes}),'raw_audit_rows':len(leaks),'raw_audit_status_counts':dict(Counter(r['status'] for r in leaks)),
        'models_fitted':0,'selected_model':None,'p_value':None,'effective_sample_size':None,'independent_observations_claimed':False,
        'limitations':['few_inspected_development_days','cyclic_wrap_artificial','balanced_subset_differs_from_original_scores','multiday_target_overlap','not_a_live_joint_policy','positive_leak_control_is_deliberately_invalid_and_never_admitted','canonical_216_pair_features_only_peer_features_previously_audited_separately'],
        'engineering_ready':False,'forecast_evidence_status':'development_association_controls_only','policy_evidence_status':'not_evaluated','demo_authorization_status':'not_granted','independent_review':False}
    return {'blocked_control_scope.json':scopes,'blocked_day_shifts.json':shifts,'future_leak_audit.json':leaks,'run_report.json':report}
