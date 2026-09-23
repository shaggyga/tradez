"""Ordered feature-only views for a future matched comparison; no learned transform."""
from contracts import fingerprint
from rolling_registry_adapter_v2 import consume,definitions
COSTS=['known_entry_long_cost_bps','known_entry_short_cost_bps']
def feature_sets(lineage,legacy_names):
    return {'legacy26':list(legacy_names),'compact38_cost2':lineage['rolling_groups']['compact38']+COSTS,
        'compact50_cost2':lineage['rolling_groups']['compact50']+COSTS,'full228_cost2':[r['name'] for r in definitions()]+COSTS}
def view(record,*,asof,group,lineage,legacy_names):
    groups=feature_sets(lineage,legacy_names)
    if group not in groups:raise ValueError('registered_rich_feature_group_required')
    # Reuse the canonical identity/schema/availability checks for every path.
    consume(record,asof=asof,feature_names=[definitions()[0]['name']])
    if group=='legacy26':
        original=record['original_legacy_observation'];values=original['features']
        if values is None:values=[None]*len(legacy_names)
        if len(values)!=len(legacy_names):raise ValueError('original_legacy_feature_width_mismatch')
    else:
        values=consume(record,asof=asof,feature_names=groups[group][:-2])['values']+record['known_entry_costs_bps']
    return {'record_id':record['record_id'],'group':group,'feature_names':groups[group],'values':list(values),
        'missing_mask':[v is None for v in values],'shared_legacy_population_eligible':record['legacy_population_eligible'],
        'original_record_sha256':record['record_sha256'],'fit_performed':False,'outcomes_included':False,
        'transform_scope':'raw_deterministic_values; learned_imputation_scaling_and_missing_indicators_must_be_fit_on_allowed_training_prefix'}
