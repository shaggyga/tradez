"""All-scope descriptive rotation outcomes, without selecting a winning policy."""
from collections import Counter
from decimal import Decimal,localcontext,Context
import json
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parent))
from publication import verify_completed_run
from fixed_blend_policy_operator_v2 import METHODS,SCENARIOS

read=lambda p:json.loads(p.read_bytes())

def build(runs,expected_identities):
    result=[];identities={}
    for method in METHODS:
        for scenario in SCENARIOS:
            name=method+'-'+scenario+'-reference';p=runs/name
            identity=read(p/'RUN_IDENTITY.json')
            if expected_identities.get(name)!=identity['fingerprint']:
                raise ValueError('blend_report_operator_identity_mismatch')
            verify_completed_run(p,identity);identities[name]=identity['fingerprint']
            report=read(p/'run_report.json');memory=read(p/'policy_state.json')
            ledger=[json.loads(line) for line in (p/'event_ledger.jsonl').read_text().splitlines()]
            decisions=[json.loads(line) for line in (p/'policy_decisions.jsonl').read_text().splitlines()]
            for arm,state in report['arms'].items():
                with localcontext(Context(prec=80)):
                    residual=Decimal(state['net_account_pnl_usd'])-Decimal(state['realized_usd'])-Decimal(state['financing_usd'])+Decimal(state['fees_usd'])
                if abs(residual)>Decimal('1e-40'):raise ValueError('blend_report_account_reconciliation')
                fills=[x for x in ledger if x['arm']==arm and x['kind']=='fill' and x['receipt']['status']=='filled']
                turns=Counter()
                for fill in fills:
                    for leg in fill['receipt']['legs']:turns[leg['instrument']]+=abs(leg['base_units'])
                selected=[d for d in decisions if d['arm']==arm];episodes=memory[arm]['episodes']
                sequence=[{'instrument':e['original_thesis']['instrument'],'side':e['original_thesis']['side'],
                           'entry_decision':e['entry_decision'],'closed_epoch':e.get('closed_epoch'),
                           'replacement_parent':e['replacement_parent']} for e in episodes]
                row={'method':method,'scenario':scenario,'arm':arm,
                     **{k:state[k] for k in ('net_account_pnl_usd','realized_usd','financing_usd','fees_usd','starting_capital_usd','return_fraction')},
                     'actions':report['actions'][arm],'decision_reasons':dict(Counter(d['reason'] for d in selected)),
                     'fills':len(fills),'turnover_absolute_base_units_by_instrument':dict(turns),
                     'turnover_scope':'executed absolute base units per instrument; currencies are not summed as USD',
                     'episodes':sequence,'filled_replacement_episodes':sum(e['replacement_parent'] is not None for e in episodes),
                     'consecutive_same_instrument_side_reversals':sum(a['instrument']==b['instrument'] and a['side']!=b['side'] for a,b in zip(sequence,sequence[1:])),
                     'reversal_scope':'consecutive filled episodes only; no arbitrary elapsed-time whipsaw threshold',
                     'empty_alternative_decisions':sum(not d['values'].get('alternatives') for d in selected),
                     'unavailable_mark_epochs':sorted({x['epoch'] for x in ledger if x['arms'][arm]['equity_usd'] is None}),
                     **report['valuation_coverage'][arm]}
                result.append(row)
    index={(r['method'],r['scenario'],r['arm']):r for r in result};deltas=[]
    for row in result:
        if row['method']!=METHODS[-1]:continue
        for base in METHODS[:-1]:
            reference=index[base,row['scenario'],row['arm']]
            with localcontext(Context(prec=80)):
                delta=Decimal(row['net_account_pnl_usd'])-Decimal(reference['net_account_pnl_usd'])
            deltas.append({'scenario':row['scenario'],'arm':row['arm'],'baseline':base,'net_usd_delta':str(delta),
                           'fill_count_delta':row['fills']-reference['fills']})
    return {'schema':'forex_fixed_remaining_policy_report.v1','scope':'single inspected two-day candle-scenario development cohort',
            'methods':list(METHODS),'scenarios':list(SCENARIOS),'rows':result,'paired_deltas':deltas,'run_identities':identities,
            'uncertainty':'not_estimated_single_dependent_development_cohort','independent_review':False,
            'engineering_ready':False,'policy_evidence_status':'declared_candle_scenarios_not_observed_execution','demo_authorization_status':'not_granted'}

def render(report):
    lines=['# Fixed blend remaining-target policy comparison','','Single inspected development cohort with hypothetical candle fills. All methods and arms are shown. No winner, protected evaluation or observed execution claim.','','| Method | Scenario | Policy | Net USD | Fills | Replacements | Missing decisions |','|---|---|---|---:|---:|---:|---:|']
    for r in report['rows']:
        lines.append('| '+ ' | '.join([r['method'],r['scenario'].replace('candle_',''),r['arm'],str(Decimal(r['net_account_pnl_usd']).quantize(Decimal('.0001'))),str(r['fills']),str(r['filled_replacement_episodes']),str(r['decision_values_unavailable'])])+' |')
    lines.extend(['','Full JSON retains all paired differences, executed turnover by instrument, decision reasons, episode sequences, reversal counts and missing marks. Cash and unavailable recovered-selector behavior are explicit. Continuous drawdown and intrabar whipsaw are not established.'])
    return '\n'.join(lines)+'\n'

if __name__=='__main__':
    import argparse
    import hashlib
    p=argparse.ArgumentParser();p.add_argument('--runs-dir',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    p.add_argument('--operator-receipt',type=Path,required=True);p.add_argument('--receipt-sha256',required=True);a=p.parse_args()
    if hashlib.sha256(a.operator_receipt.read_bytes()).hexdigest()!=a.receipt_sha256:
        raise ValueError('blend_report_receipt_pin_mismatch')
    receipt=read(a.operator_receipt)
    if receipt['status']!='completed_verified' or len(receipt['runs'])!=12:
        raise ValueError('blend_report_verified_operator_receipt_required')
    result=build(a.runs_dir,{x['run_id']:x['run_identity'] for x in receipt['runs']});a.output.mkdir(parents=True,exist_ok=True)
    (a.output/'RESULT_SUMMARY.json').write_text(json.dumps(result,indent=2,sort_keys=True)+'\n')
    (a.output/'RESULT_SUMMARY.md').write_text(render(result))
    print(json.dumps({'rows':len(result['rows']),'paired_comparisons':len(result['paired_deltas'])}))
