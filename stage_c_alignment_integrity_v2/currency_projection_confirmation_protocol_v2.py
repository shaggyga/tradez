"""Refuse confirmation candidates that reuse the inspected projection-policy dates."""
import json
from pathlib import Path
ROOT=Path(__file__).resolve().parent
def protocol():return json.loads((ROOT/'CURRENCY_PROJECTION_CONFIRMATION_PROTOCOL_V2.json').read_text())
def validate(candidate):
    p=protocol(); origins=candidate.get('origins',[])
    if len(origins)<p['required']['minimum_new_origins']:raise ValueError('confirmation_origin_count')
    if any(x<=p['last_inspected_target_epoch'] for x in origins):raise ValueError('confirmation_reuses_inspected_or_unmatured_origin')
    if candidate.get('methods')!=p['required']['all_methods']:raise ValueError('confirmation_method_inventory')
    if candidate.get('all68_market_quotes') is not True or candidate.get('mature_labels_before_assessment') is not True:raise ValueError('confirmation_input_qualification')
    return True
