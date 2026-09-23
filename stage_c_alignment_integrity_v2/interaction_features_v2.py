"""Fixed local-state products; no marginal screening or fitted feature selection."""
from itertools import combinations
GROUPS=('legacy26_interactions10',)
AXES=('tech_rate_bps_per_min_1m','tech_rate_bps_per_min_15m','tech_rate_bps_per_min_60m','tech_rms_bps','tech_session_age_hours')
PAIRS=tuple(combinations(AXES,2))
def names_for(group,legacy):
    if group not in GROUPS or any(n not in legacy for n in AXES):raise ValueError('fixed_interaction_axes_required')
    return list(legacy)+['product__'+a+'__'+b for a,b in PAIRS]
def expand(values,legacy):
    if values is None:return [None]*(len(legacy)+len(PAIRS))
    if len(values)!=len(legacy):raise ValueError('original_legacy_width_required')
    by=dict(zip(legacy,values))
    return list(values)+[by[a]*by[b] for a,b in PAIRS]
def views_for(record,legacy):
    old=record['original_legacy_observation'];group=GROUPS[0]
    return {group:{'record_id':old['record_id'],'original_record_sha256':record['record_sha256'],'feature_names':names_for(group,legacy),'values':expand(old['features'],legacy),'shared_legacy_population_eligible':old['features'] is not None,'control_kind':'fixed_pairwise_local_state_products'}}
