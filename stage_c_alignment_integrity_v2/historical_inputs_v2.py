"""Verify prepared historical inputs without fitting or accessing the raw archive."""
import argparse
from collections import Counter
import hashlib
import json
from math import isfinite
from pathlib import Path

def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def read(path):return json.loads(path.read_text(encoding='utf-8'))

def validate_records(observations,outcomes,report,contract):
    members={m['instrument']:m for m in report['members']}
    if len(members)!=68:raise ValueError('all68_member_report_required')
    by_id={};counts=Counter();ready=Counter();mature={p:Counter() for p in members};missing={p:Counter() for p in members}
    for row in observations:
        key=row['record_id'];pair=row['instrument'];origin=row['origin_epoch']
        if key in by_id:raise ValueError('duplicate_observation')
        if pair not in members or key!=f'{pair}:{origin}':raise ValueError('observation_identity_mismatch')
        if type(origin) is not int or origin!=row['source_bar_start_epoch']+60 or row['available_epoch']!=origin:
            raise ValueError('observation_clock_contract_mismatch')
        if row['source_member_sha256']!=members[pair]['member_sha256']:raise ValueError('observation_source_mismatch')
        x=row['features']
        if x is not None and (len(x)!=5 or not all(type(v) in (int,float) and isfinite(v) for v in x)):
            raise ValueError('invalid_feature_vector')
        by_id[key]=row;counts[pair]+=1;ready[pair]+=int(x is not None)
    if set(counts)!=set(members):raise ValueError('missing_instrument_observations')
    seen=set();horizons={f'historical_gross_midpoint_elapsed_{h}m':h for h in contract['target_minutes']}
    for row in outcomes:
        key=(row['record_id'],row['target_id'])
        if key in seen:raise ValueError('duplicate_outcome')
        seen.add(key)
        if row['record_id'] not in by_id or row['target_id'] not in horizons:raise ValueError('outcome_join_or_target_mismatch')
        o=by_id[row['record_id']];h=horizons[row['target_id']];pair=o['instrument']
        if row['label_end_epoch']!=o['origin_epoch']+h*60 or row['available_epoch']!=row['label_end_epoch']:
            raise ValueError('outcome_clock_contract_mismatch')
        if row['source_member_sha256']!=members[pair]['member_sha256'] or row['execution_status']!='not_qualified':
            raise ValueError('outcome_provenance_or_claim_mismatch')
        present=row['value'] is not None
        if present and (type(row['value']) not in (int,float) or not isfinite(row['value'])):raise ValueError('nonfinite_outcome')
        if row['status']!=('ENDPOINT_AVAILABLE_ASSUMED_BAR_CLOSE' if present else 'UNAVAILABLE_ENDPOINT'):
            raise ValueError('outcome_status_mismatch')
        mature[pair][str(h)]+=int(present and o['features'] is not None and o['origin_epoch']<contract['fit_cutoff_epoch'] and row['available_epoch']<=contract['fit_cutoff_epoch'])
        missing[pair][str(h)]+=int(not present)
    if len(seen)!=len(observations)*len(horizons):raise ValueError('one_outcome_status_per_origin_target_required')
    for pair,m in members.items():
        if counts[pair]!=m['selected_origins'] or ready[pair]!=m['feature_ready_origins'] or dict(mature[pair])!=m['mature_by_fit'] or dict(missing[pair])!=m['missing_endpoint']:
            raise ValueError('member_support_report_mismatch')
    if len(observations)!=report['observation_records'] or len(outcomes)!=report['outcome_records']:
        raise ValueError('record_count_mismatch')
    return {'status':'verified_prepared_inputs_only','instruments':68,'observations':len(observations),'outcomes':len(outcomes),
        'mature_by_fit':{str(h):sum(x[str(h)] for x in mature.values()) for h in contract['target_minutes']},
        'historical_fit_performed':False,'execution_qualified':False,'independent_review':False}

def verify(root):
    report=read(root/'PREFLIGHT_REPORT.json');contract=read(root/'PREFLIGHT_CONTRACT.json')
    if sha(root/'PREFLIGHT_CONTRACT.json')!=report['contract_sha256']:raise ValueError('contract_hash_mismatch')
    for item in report['files']:
        if item['path'] not in {'historical_observations.jsonl','historical_outcomes.jsonl'}:raise ValueError('unexpected_input_member')
        p=root/item['path']
        if p.stat().st_size!=item['bytes'] or sha(p)!=item['sha256']:raise ValueError('input_hash_mismatch')
    observations=[json.loads(s) for s in (root/'historical_observations.jsonl').read_text().splitlines()]
    outcomes=[json.loads(s) for s in (root/'historical_outcomes.jsonl').read_text().splitlines()]
    result=validate_records(observations,outcomes,report,contract)
    result['input_hashes']={item['path']:item['sha256'] for item in report['files']}
    return result

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--root',type=Path,default=Path(__file__).resolve().parent)
    print(json.dumps(verify(p.parse_args().root),sort_keys=True))
