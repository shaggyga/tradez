"""Stateless predeclared nuisance fields; no outcome, future data or fitted transform."""
import hashlib,json
SEEDS=(20260922,20260923)
GROUPS=tuple(f'{kind}_seed{seed}' for seed in SEEDS for kind in ('noise8','legacy26_noise8'))
def specification(group):
    if group not in GROUPS:raise ValueError('declared_noise_group_required')
    kind,seed=group.rsplit('_seed',1)
    return kind,int(seed)
def names_for(group,legacy):
    kind,seed=specification(group)
    return (list(legacy) if kind=='legacy26_noise8' else [])+[f'nuisance_uniform_{i}' for i in range(8)]
def nuisance(pair,origin,seed):
    if not isinstance(pair,str) or type(origin) is not int or seed not in SEEDS:raise ValueError('declared_noise_key_required')
    values=[]
    for column in range(8):
        raw=json.dumps(['forex_nuisance_v1',seed,pair,origin,column],separators=(',',':')).encode()
        integer=int.from_bytes(hashlib.sha256(raw).digest()[:8],'big')>>11
        values.append(2*(integer/(2**53))-1)
    return values
def views_for(record,legacy):
    old=record['original_legacy_observation'];result={}
    for group in GROUPS:
        kind,seed=specification(group);values=nuisance(old['instrument'],old['origin_epoch'],seed)
        if kind=='legacy26_noise8':values=(list(old['features']) if old['features'] is not None else [None]*len(legacy))+values
        result[group]={'record_id':old['record_id'],'original_record_sha256':record['record_sha256'],'feature_names':names_for(group,legacy),'values':values,'shared_legacy_population_eligible':old['features'] is not None,'noise_seed':seed,'control_kind':kind}
    return result
