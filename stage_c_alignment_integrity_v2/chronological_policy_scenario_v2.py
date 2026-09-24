"""Explicit two-cohort dated scenario authority; original economic assumptions."""
import hashlib,json
from pathlib import Path
ROOT=Path(__file__).resolve().parent
CONTRACT='CHRONOLOGICAL_POLICY_CONTRACT_V2.json'
AUTHORITY='CHRONOLOGICAL_POLICY_AUTHORITY_V2.json'
PROFILE='chronological_layer_policy.v1'
TIER='historical_fitted_candle_policy_scenario.v1'
def contract(scenario_id=None):
    c=json.loads((ROOT/CONTRACT).read_bytes())
    if scenario_id is None:return c
    if scenario_id not in c['scenarios']:raise ValueError('chronological_policy_registered_scenario_required')
    return {**c,**c['cohorts'][c['scenarios'][scenario_id]['cohort']]}
def authority(cohort):
    c=contract();raw=(ROOT/AUTHORITY).read_bytes()
    if hashlib.sha256(raw).hexdigest()!=c['authority_sha256']:raise ValueError('chronological_policy_authority_changed')
    a=json.loads(raw)
    if a['native_identity']!=c['native_identity'] or a['extension_identity']!=c['extension_identity']:raise ValueError('chronological_policy_authority_parent_mismatch')
    return a['cohorts'][cohort]
def scenarios():
    c=contract()
    return {name:{'scenario_id':name,'input_tier':TIER,**{k:v for k,v in values.items() if k!='cohort'},
      'rollover_epoch':c['cohorts'][values['cohort']]['rollover_epoch'],
      'fill_rule':'full_pending_units_at_next_completed_M1_close_after_58s_delay',
      'arrival_rule':'assumed_M1_close_plus_two_seconds_model_availability','observed_execution':False,'broker_access':False}
      for name,values in c['scenarios'].items()}
def canonical_metadata(metadata):
    return {pair:{**row,'pip_size':str(row['pip_size'])} for pair,row in metadata.items()}
