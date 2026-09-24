"""Frozen dated scenarios and original-surface authority; no model loading."""
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
CONTRACT = 'LATER_LAYER_POLICY_CONTRACT_V2.json'
AUTHORITY = 'LATER_POLICY_AUTHORITY_V2.json'
PROFILE = 'later_remaining_layer_policy.v1'
TIER = 'historical_fitted_candle_policy_scenario.v1'


def contract():
    return json.loads((ROOT/CONTRACT).read_bytes())


def authority():
    c = contract(); raw = (ROOT/AUTHORITY).read_bytes()
    if hashlib.sha256(raw).hexdigest() != c['authority_sha256']:
        raise ValueError('later_policy_original_authority_changed')
    result = json.loads(raw)
    if result['surface_identity'] != c['surface_identity'] or result['source_recipe_sha256'] != c['surface_recipe_sha256']:
        raise ValueError('later_policy_authority_parent_mismatch')
    return result


def scenarios():
    c = contract()
    return {name: {'scenario_id': name, 'input_tier': TIER, **values, 'rollover_epoch': c['rollover_epoch'],
        'fill_rule': 'full_pending_units_at_next_completed_M1_close_after_58s_delay',
        'arrival_rule': 'assumed_M1_close_plus_two_seconds_model_availability',
        'observed_execution': False, 'broker_access': False}
        for name, values in c['scenarios'].items()}


def canonical_metadata(metadata):
    return {pair: {**row, 'pip_size': str(row['pip_size'])} for pair, row in metadata.items()}
