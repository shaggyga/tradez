"""Describe every scored row in the frozen baseline; no runtime data is read."""
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal, localcontext, Context, ROUND_HALF_EVEN
import hashlib
import json
from pathlib import Path

WORK=Path(__file__).resolve().parent
BASELINE=WORK/'PERFORMANCE_BASELINE_20260907.json'
BASELINE_SHA='53c7c40091ee47e8faa91f6d747c8c9171e10a8f6e3b6904d073fc951a5ae6eb'
ROOT=Path(r'C:\Users\zmoor\Documents\forex\trad')
SCORER=ROOT/'oanda_exact_price_scoring.py'
SCORER_SHA='f60ee51da1419e70b31327ba30755c7879b2c4ebfbb54a7778fe689e0e119521'
FAMILIES=('probabilistic_state_space','ridge_return_repaired')

def sha(raw):
    return hashlib.sha256(raw).hexdigest()

def mean(values):
    return sum(values,Decimal(0))/len(values) if values else None

def jsonable(value):
    if isinstance(value,Decimal):return str(value)
    if isinstance(value,dict):return {k:jsonable(v) for k,v in value.items()}
    if isinstance(value,list):return [jsonable(v) for v in value]
    return value

def describe(rows, spread_recoverable):
    count=len(rows)
    families={}
    for family in FAMILIES:
        arms=[r['scores'][family] for r in rows]
        directional=[a for a in arms if a['side']]
        hits=sum(a['direction_correct'] for a in directional)
        wins=sum(a['positive_after_spread'] for a in directional)
        families[family]={'scored_decisions':count,'directional_decisions':len(directional),
            'abstentions':count-len(directional),'direction_hits':hits,
            'direction_hit_rate':Decimal(hits)/len(directional) if directional else None,
            'positive_after_spread':wins,
            'positive_after_spread_rate':Decimal(wins)/len(directional) if directional else None,
            'negative_after_spread':sum(Decimal(a['net_bps'])<0 for a in directional),
            'zero_after_spread':sum(Decimal(a['net_bps'])==0 for a in directional),
            'mean_net_bps_per_decision':mean([Decimal(a['net_bps']) for a in arms]),
            'stress_mean_net_bps_per_decision':{cost:mean([Decimal(a['stress_net_bps'][cost]) for a in arms]) for cost in ('0','0.5','1')}}
    drag=[]; midpoint_moves=[]
    if spread_recoverable:
        for row in rows:
            by_side={a['side']:Decimal(a['net_bps']) for a in row['scores'].values()}
            assert set(by_side)=={-1,1}
            cost=-(by_side[1]+by_side[-1])/2
            assert cost>=0
            drag.append(cost)
            midpoint_moves.append(abs((by_side[1]-by_side[-1])/2))
    return {'scored_decisions':count,'model_arms':2*count,'pair_count':len({r['pair'] for r in rows}),
            'pair_decision_counts':dict(sorted(Counter(r['pair'] for r in rows).items())),
            'outcome_counts':dict(Counter(r['outcome_class'] for r in rows)),
            'mean_absolute_reference_to_target_move_bps':mean([abs(Decimal(r['actual_return_bps'])) for r in rows]),
            'mean_reconstructed_roundtrip_spread_drag_bps':mean(drag) if spread_recoverable else None,
            'spread_reconstruction_denominator':count if spread_recoverable else 0,
            'spread_unavailable_reason':None if spread_recoverable else 'Compact baseline omits bid/ask prices; same-side net returns cannot isolate spread from entry-to-target movement.',
            'mean_reconstructed_absolute_entry_to_target_midpoint_move_bps':mean(midpoint_moves) if spread_recoverable else None,
            'spread_exceeds_absolute_entry_to_target_move':sum(d>m for d,m in zip(drag,midpoint_moves)) if spread_recoverable else None,
            'spread_equals_absolute_entry_to_target_move':sum(d==m for d,m in zip(drag,midpoint_moves)) if spread_recoverable else None,
            'families':families}

def main():
    raw=BASELINE.read_bytes()
    assert sha(raw)==BASELINE_SHA
    # Read only frozen mathematical source to verify denominator convention.
    assert sha(SCORER.read_bytes())==SCORER_SHA
    baseline=json.loads(raw); rows=[]
    for pair,record in sorted(baseline['pairs'].items()):
        assert record['status']=='evaluated'
        for source in record['fresh_scored_rows']:
            row={'pair':pair,**source}; rows.append(row)
            assert set(row['scores'])==set(FAMILIES)
            actual=Decimal(row['actual_return_bps'])
            for arm in row['scores'].values():
                assert arm['side'] in (-1,1)
                assert arm['direction_correct']==(arm['side']*actual>0)
                assert arm['positive_after_spread']==(Decimal(arm['net_bps'])>0)
    assert len(rows)==83
    agreement=[r for r in rows if r['scores'][FAMILIES[0]]['side']==r['scores'][FAMILIES[1]]['side']]
    disagreement=[r for r in rows if r not in agreement]
    assert len(agreement)+len(disagreement)==83
    for row in agreement:
        for key in ('direction_correct','positive_after_spread','net_bps'):
            assert row['scores'][FAMILIES[0]][key]==row['scores'][FAMILIES[1]][key]
    with localcontext(Context(prec=80,rounding=ROUND_HALF_EVEN)):
        groups={'all':describe(rows,False),'agreement':describe(agreement,False),'disagreement':describe(disagreement,True)}
        side_groups={str((left,right)):describe([r for r in rows if (r['scores'][FAMILIES[0]]['side'],r['scores'][FAMILIES[1]]['side'])==(left,right)],left!=right)
                     for left,right in ((1,1),(-1,-1),(1,-1),(-1,1))}
    result={'schema_version':'frozen_baseline_two_model_agreement_v1_20260907','status':'passed',
        'analyzed_utc':datetime.now(timezone.utc).isoformat(),'baseline_started_utc':baseline['started_utc'],
        'baseline_finished_utc':baseline['finished_utc'],'baseline_sha256':BASELINE_SHA,
        'analysis_helper_sha256':sha(Path(__file__).read_bytes()),'exact_scorer_sha256':SCORER_SHA,
        'source_scope':'Only the frozen baseline JSON plus unchanged scorer source were read; no runtime databases, current quotes, scorecards or APIs were read.',
        'selection_rule':'Partition all 83 previously scored decisions by equality of emitted side, without using outcome or profitability to select rows. No probability threshold is applied.',
        'groups':groups,'side_groups':side_groups,'side_group_order':list(FAMILIES),
        'partition_manifest':[{'pair':r['pair'],'decision_id':r['decision_id'],
            'group':'agreement' if r['scores'][FAMILIES[0]]['side']==r['scores'][FAMILIES[1]]['side'] else 'disagreement',
            'state_space_side':r['scores'][FAMILIES[0]]['side'],'ridge_side':r['scores'][FAMILIES[1]]['side']} for r in rows],
        'spread_reconstruction':{
            'formula':'−(net_long_bps + net_short_bps) / 2 = 10000 × (entry_spread + target_spread) / (2 × entry_mid)',
            'denominator_verified':'Both arms use the same selected entry midpoint denominator and same entry/target quotes in the frozen evaluator.',
            'precision':'Algebraically exact cancellation before ratio rounding. Baseline ratios were already rounded to 80-digit Decimal precision; reconstructed values retain that numerical limitation.',
            'move_formula':'(net_long_bps − net_short_bps) / 2 = 10000 × (target_mid − entry_mid) / entry_mid',
            'reference_move_warning':'The separately reported actual move uses reference_mid as denominator and reference time as anchor. It must not be subtracted from net return to claim pure spread cost.',
            'agreement_and_all_row_cost':'Unavailable from this compact artifact. No newer prices or assumed spreads were substituted.'},
        'checks':{'all_83_rows_partitioned':True,'direction_and_net_signs_reconciled':True,
                  'agreeing_arms_have_identical_direction_and_executable_return':True,'original_eurusd_companion_not_pooled':True},
        'limitations':['Descriptive post hoc split of one small observation batch; no threshold, promotion or trading rule is validated.',
            'Two models agreeing on one pair is distinct from currencies moving together or cross-pair factor research. Those predictor relationships were not measured here.',
            'Models share inputs and targets. Agreement does not create two independent confirmations.',
            'Overlapping H1 horizons and related currencies make pooled pair-decisions dependent; independent sample size is unknown.',
            'After-spread figures are hypothetical exact bid/ask outcomes, not realized account dollars or guaranteed fills; financing is omitted.']}
    output=WORK/'MODEL_AGREEMENT_BASELINE_20260907.json'
    with output.open('x',encoding='utf-8') as handle:json.dump(jsonable(result),handle,indent=2);handle.write('\n')
    print(json.dumps(jsonable({'output':str(output),'sha256':sha(output.read_bytes()),'groups':groups,'side_group_counts':{k:v['scored_decisions'] for k,v in side_groups.items()}}),indent=2))

if __name__=='__main__':main()
