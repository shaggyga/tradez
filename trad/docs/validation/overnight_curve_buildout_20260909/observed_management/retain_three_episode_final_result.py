"""Summarize exact accepted reports; no runtime or broker access."""
from decimal import Decimal,Context,localcontext
import hashlib
import json
from pathlib import Path
import time

BASE=Path(__file__).resolve().parent
PINS={
 'OBSERVED_THREE_EPISODE_FINAL_VERIFICATION_20260909.json':'cdaac1862e6d2d2153e65d394ff5bffada25dd505b43d186197144cf1286e5ce',
 'OBSERVED_THREE_EPISODE_FINAL_COST_ATTRIBUTION_20260909.json':'f9827417f4468f4f416838ac131bb3902443983566bb86f32566450fd38f5e13',
 'OBSERVED_THREE_EPISODE_FINAL_SUMMARY_20260909.json':'51da82b2599bcedf579e75727648fab15f236e9d061db402d929fd0cebd51fd7'}


def bind(path):
    raw=path.read_bytes();return {'path':str(path),'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)}


def save(path,value):
    raw=(json.dumps(value,sort_keys=True,indent=2,allow_nan=False)+'\n').encode()
    with path.open('xb') as stream:stream.write(raw)
    return bind(path)


def main():
    values=[];evidence=[]
    for name,want in PINS.items():
        path=BASE/name;item=bind(path);assert item['sha256']==want
        evidence.append(item);values.append(json.loads(path.read_bytes()))
    verified,cost,summary=values
    assert verified['status']=='passed' and not verified['issues']
    assert cost['status']=='completed' and cost['report_bytes_sha256']==evidence[0]['sha256']
    assert len(summary['input_file_evidence'])==1 and summary['input_file_evidence'][0]['sha256']==evidence[0]['sha256']
    assert summary['matched_completed_support']['eligible_episode_ids']==['episode_01','episode_02','episode_03']
    assert all(v['completed_matched_endpoint_eligible'] and not v['uncompleted_or_unobserved_slots'] and not v['missed_slots'] for v in verified['episodes'])
    with localcontext(Context(prec=96)):
        arms={}
        for name in cost['episodes'][0]['per_arm']:
            rows=[e['per_arm'][name] for e in cost['episodes']]
            sums={k:sum((Decimal(row[k]) for row in rows),Decimal(0)) for k in ('gross_midpoint_pnl_usd','spread_cost_usd','slippage_cost_usd','realized_usd')}
            assert sums['gross_midpoint_pnl_usd']-sums['spread_cost_usd']-sums['slippage_cost_usd']==sums['realized_usd']
            assert sums['realized_usd']==Decimal(summary['hypothetical_completed_scenario_totals_by_arm_usd'][name])
            arms[name]={**{k:str(v) for k,v in sums.items()},'completed_round_trips':sum(r['completed_round_trips'] for r in rows),
                'virtual_legs':sum(r['actual_virtual_legs'] for r in rows),'positive_episode_count':sum(Decimal(r['realized_usd'])>0 for r in rows),
                'negative_episode_count':sum(Decimal(r['realized_usd'])<0 for r in rows),'zero_episode_count':sum(Decimal(r['realized_usd'])==0 for r in rows)}
        curve,momentum=arms['usd_curve_manager'],arms['usd_momentum_manager']
        gross=Decimal(curve['gross_midpoint_pnl_usd'])-Decimal(momentum['gross_midpoint_pnl_usd'])
        cost_saving=sum((Decimal(momentum[k])-Decimal(curve[k]) for k in ('spread_cost_usd','slippage_cost_usd')),Decimal(0))
        net=Decimal(curve['realized_usd'])-Decimal(momentum['realized_usd'])
        assert gross+cost_saving==net==Decimal(summary['matched_completed_curve_minus_momentum_sum_usd'])
    result={'schema_version':'three_episode_final_matched_result_v1_20260909','generated_epoch':time.time(),
        'status':'three_predeclared_virtual_episodes_verified','input_evidence':evidence,'source_bindings':verified['source_bindings'],
        'registered_source_count':len(verified['source_bindings']),'verification_started_epoch':verified['verification_started_epoch'],
        'verification_completed_epoch':verified['verification_completed_epoch'],'verified_file_count':len(verified['verified_files']),
        'verified_bytes':verified['total_read_bytes'],'verification_issues':verified['issues'],
        'matched_support':summary['matched_completed_support'],'per_arm':arms,
        'per_episode':[{'episode_id':e['episode_id'],'original_target_epoch':e['native_target_epoch'],'verified_steps':e['verified_completed_steps'],
            'per_arm_realized_virtual_usd':{n:r['realized_usd'] for n,r in e['per_arm'].items()},
            'curve_minus_momentum_virtual_usd':e['matched_terminal_curve_minus_momentum_usd']} for e in verified['episodes']],
        'curve_minus_momentum_attribution':{'gross_midpoint_delta_usd':str(gross),'lower_costs_usd':str(cost_saving),'net_delta_usd':str(net)},
        'interpretation':'Curve management had lower losses than the matched momentum manager in each episode, but lost in all three and underperformed unchanged curve holding. Its aggregate relative advantage came from lower turnover/costs; gross midpoint performance was worse than momentum.',
        'limits':['Only three consecutive GBP_USD episodes on one date; independent sample size is unknown, not3 or178. No robust predictive or management efficacy claim.',
            'USD2500 is the predeclared research sizing scenario reset per episode; isolated arms are not simultaneous account positions or actual account PnL.',
            'Actual retained bid/ask half-spreads plus fixed0.1bps per-leg hypothetical slippage; no financing, commissions, market impact or broker-fill observation.',
            'Original fixed nominal native targets and declared target-window approximation remain unchanged. No post-outcome source, horizon, policy or parameter selection.',
            'Virtual action legs alone contribute realized cost attribution; no liquidation marks treated as trades.'],
        'new_model_fits':False,'runtime_writes':False,'broker_actions':False,'independent_sample_size':None}
    receipt=save(BASE/'OBSERVED_THREE_EPISODE_FINAL_RESULT_20260909.json',result)
    lines=['# Three fixed paper episodes — final verified result','',
        'All three predeclared GBP/USD episodes reached their original targets with all five virtual arms flat. The unchanged verifier reconciled178steps and2,727 retained files, with no missing slots, missed slots or verification issues. All20 registered source bindings matched.','',
        '| Separate virtual arm | Round trips | Gross midpoint USD | Spread USD | Slippage USD | Net USD |','|---|---:|---:|---:|---:|---:|']
    labels={'curve_hold_no_rotation':'Hold initial curve','usd_curve_manager':'Curve manager','usd_momentum_manager':'Matched momentum manager','legacy_momentum_reference':'Legacy selector / new momentum input','no_trade':'No trade'}
    for name in labels:
        row=arms[name];lines.append('| '+labels[name]+' | '+str(row['completed_round_trips'])+' | '+' | '.join(format(Decimal(row[k]),'.5f') for k in ('gross_midpoint_pnl_usd','spread_cost_usd','slippage_cost_usd','realized_usd'))+' |')
    lines+=['','The curve manager beat matched momentum in each episode, by a combined virtual$9.14874. That combines worse gross midpoint P&L of$5.36379 with lower spread/slippage costs of$14.51253. It still lost in all three episodes. Holding the initial curve position was the only active arm with positive aggregate net P&L.','',
        'The evidence supports lower turnover as the reason for the relative improvement over momentum. It does not establish a profitable predictor or a reliable management advantage. Three consecutive one-pair episodes are a small, dependent sample.','',
        'Each arm used the same predeclared USD2,500 research sizing scenario, clocks, quote rules and original target. Sums reset sizing per episode and are not actual account P&L. Costs include recorded bid/ask half-spreads and fixed0.1bps slippage per leg; no broker fills, financing, commissions or market impact were observed.','',
        'Original targets:07:43:50,08:42:55 and09:42:55 UTC on September9. Verification ran09:43:35.998–09:44:58.787 UTC. Original publication, consumption, plan, quote, settlement and evidence-read clocks remain in the bound reports.','',
        'Result JSON SHA256: '+receipt['sha256']+'.']
    report=BASE/'OBSERVED_THREE_EPISODE_FINAL_REVIEW_20260909.md'
    with report.open('x',encoding='utf-8',newline='\n') as f:f.write('\n'.join(lines)+'\n')
    acceptance={'schema_version':'three_episode_final_evidence_acceptance_v1_20260909','recorded_epoch':time.time(),'status':'completed_verified',
        'evidence':evidence+[receipt,bind(report),bind(Path(__file__)),bind(BASE/'OBSERVED_THREE_EPISODE_FINAL_EVALUATION_PREPARATION_20260909.json')],
        'new_test_execution':False,'source_changes':False,'runtime_actions':False,'GET':False,
        'method':'Unchanged accepted verifier, cost attribution and serializer, followed by exact Decimal aggregation of their stored results. No repeated prospective run or refit.',
        'support':summary['matched_completed_support']}
    print(json.dumps({'result':receipt,'review':bind(report),'acceptance':save(BASE/'OBSERVED_THREE_EPISODE_FINAL_ACCEPTANCE_20260909.json',acceptance)}))


if __name__=='__main__':main()
