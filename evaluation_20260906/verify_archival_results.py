"""Independent recomputation of the paired archive diagnostic, without SQLite."""
from collections import defaultdict
from pathlib import Path
import hashlib
import json
import math
import statistics

OUT = Path(__file__).resolve().parent
ROOT = OUT.parent/'trad'
raw_path = OUT/'data/selected_candidate_rows.json'
rows = json.loads(raw_path.read_bytes())
protocol = json.loads((ROOT/'config/fixed_forecast_evaluation_v1_20260906.json').read_bytes())
saved = json.loads((OUT/'data/archival_scorecard.json').read_bytes())
groups = defaultdict(dict)
direction_probability_mismatches = 0
for row in rows:
    payload = json.loads(row['forecast_json'])
    assert hashlib.sha256(row['forecast_json'].encode()).hexdigest()==row['payload_sha256']
    assert payload['cohort_id']==protocol['cohorts'][row['family']]
    assert row['model_version']==protocol['model_version']
    assert row['feature_version']==protocol['feature_version']
    assert row['instrument']=='EUR_USD' and row['pip']==0.0001
    assert row['family'] not in groups[row['entry_time']]
    groups[row['entry_time']][row['family']]=row
    predicted_side = 'buy' if row['probability_up']>.5 else 'sell' if row['probability_up']<.5 else 'neutral'
    direction_probability_mismatches += predicted_side != row['direction']
assert len(rows)==1628 and len(groups)==407
paired=[]
for reference, group in groups.items():
    assert set(group)==set(protocol['cohorts'])
    if not all(row['outcome'] for row in group.values()): continue
    endpoints={(r['entry_bid'],r['entry_ask'],r['outcome']['exit_time'],r['outcome']['exit_bid'],r['outcome']['exit_ask']) for r in group.values()}
    if len(endpoints)==1: paired.append(group)
assert len(paired)==368
metrics={}
for family in protocol['cohorts']:
    briers=[];errors=[];zero_errors=[];net=[];hits=[]
    for group in paired:
        row=group[family];outcome=row['outcome'];payload=json.loads(row['forecast_json'])
        actual=((outcome['exit_bid']+outcome['exit_ask'])-(row['entry_bid']+row['entry_ask']))/2/row['pip']
        briers.append((row['probability_up']-(actual>0))**2)
        errors.append(abs(payload['expected_signed_pips']-actual))
        zero_errors.append(abs(actual))
        is_buy=row['direction']=='buy'
        net.append((outcome['exit_bid']-row['entry_ask'] if is_buy else row['entry_bid']-outcome['exit_ask'])/row['pip'])
        hits.append(actual*(1 if is_buy else -1)>0)
    actual_metrics={'brier_up_vs_nonup':statistics.fmean(briers),'signed_move_mae_pips':statistics.fmean(errors),
                    'zero_move_mae_pips':statistics.fmean(zero_errors),'mean_net_bid_ask_pips':statistics.fmean(net),
                    'gross_direction_accuracy_flats_miss':statistics.fmean(hits)}
    for key,value in actual_metrics.items(): assert math.isclose(value,saved['family_metrics'][family][key],rel_tol=1e-12,abs_tol=1e-10)
    metrics[family]=actual_metrics
result={'verified':True,'source_extract_sha256':hashlib.sha256(raw_path.read_bytes()).hexdigest(),
        'all_original_payload_hashes_verified':True,'all_exact_families_versions_cohorts_verified':True,
        'forecast_universe':len(rows),'shared_reference_epochs':len(groups),'paired_identical_endpoints':len(paired),
        'stored_direction_vs_probability_side_mismatches':direction_probability_mismatches,
        'metrics':metrics,'strict_causal_or_executable_proof':False,'production_database_connections':0}
(OUT/'archival_independent_verification.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
print(json.dumps(result,indent=2))
