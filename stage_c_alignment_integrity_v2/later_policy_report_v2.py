"""All-arm later policy outcomes with explicit unpriced or unclosed endpoints."""
import argparse
from collections import Counter
from decimal import Decimal, Context, localcontext
import hashlib
import json
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT))
from publication import verify_completed_run
from later_policy_scenario_v2 import contract

read=lambda p:json.loads(p.read_bytes())


def terminal_status(state):
    if state['net_account_pnl_usd'] is None:
        if state['equity_usd'] is not None or state['return_fraction'] is not None:
            raise ValueError('later_report_missing_equity_consistency')
        return 'unpriced_terminal'
    if state['unrealized_usd'] is None: raise ValueError('later_report_known_equity_requires_unrealized')
    with localcontext(Context(prec=80)):
        residual=Decimal(state['net_account_pnl_usd'])-Decimal(state['realized_usd'])-Decimal(state['financing_usd'])+Decimal(state['fees_usd'])-Decimal(state['unrealized_usd'])
    if abs(residual)>Decimal('1e-40'):raise ValueError('later_report_account_reconciliation')
    return 'marked_open_terminal' if state['open_lot_count'] or Decimal(str(state['pending_order_units'])) else 'closed_terminal'


def paired(left,right,kind):
    comparable=left['terminal_status']==right['terminal_status']=='closed_terminal'
    with localcontext(Context(prec=80)):
        delta=str(Decimal(left['net_account_pnl_usd'])-Decimal(right['net_account_pnl_usd'])) if comparable else None
    return {'method':left['method'],'baseline':right['method'],'scenario':left['scenario'],'arm':left['arm'],
        'comparison_kind':kind,'net_usd_delta':delta,'status':'descriptive_comparable' if comparable else 'terminal_comparison_unavailable',
        'left_terminal':left['terminal_status'],'right_terminal':right['terminal_status'],
        'fill_count_delta':left['fills']-right['fills'],'replacement_episode_delta':left['filled_replacement_episodes']-right['filled_replacement_episodes']}


def build(runs,receipt):
    c=contract();expected={m+'-'+s+'-'+e for m in c['methods'] for s in c['scenarios'] for e in c['engines']}
    records={r['run_id']:r for r in receipt['runs']}
    if receipt['status']!='completed_verified' or len(records)!=56 or set(records)!=expected or len(receipt['runs'])!=56:
        raise ValueError('later_report_exact_verified56run_receipt_required')
    if any(r['status']!='completed_verified' for r in records.values()):raise ValueError('later_report_incomplete_operator_run')
    rows=[];identities={};coverage=[]
    for method in c['methods']:
        for scenario in c['scenarios']:
            name=method+'-'+scenario+'-reference';root=runs/name;identity=read(root/'RUN_IDENTITY.json')
            if identity['fingerprint']!=records[name]['run_identity']:raise ValueError('later_report_operator_identity_mismatch')
            verify_completed_run(root,identity);identities[name]=identity['fingerprint']
            report=read(root/'run_report.json');memory=read(root/'policy_state.json')
            ledger=[json.loads(line) for line in (root/'event_ledger.jsonl').read_text().splitlines()]
            decisions=[json.loads(line) for line in (root/'policy_decisions.jsonl').read_text().splitlines()]
            coverage.append({'method':method,'scenario':scenario,'all68_slots':len(records[name]['coverage']),
                'statuses':dict(Counter(r['status'] for r in records[name]['coverage'])),
                'source_statuses':dict(Counter(r['source_coverage'] for r in records[name]['coverage']))})
            for arm,state in report['arms'].items():
                status=terminal_status(state)
                fills=[r for r in ledger if r.get('arm')==arm and r['kind']=='fill' and r['receipt']['status']=='filled']
                turnover=Counter()
                for fill in fills:
                    for leg in fill['receipt']['legs']:turnover[leg['instrument']]+=abs(leg['base_units'])
                selected=[r for r in decisions if r['arm']==arm];episodes=memory[arm]['episodes']
                sequence=[{'instrument':e['original_thesis']['instrument'],'side':e['original_thesis']['side'],
                    'entry_decision':e['entry_decision'],'closed_epoch':e.get('closed_epoch'),'replacement_parent':e['replacement_parent']} for e in episodes]
                rows.append({'method':method,'scenario':scenario,'arm':arm,'terminal_status':status,
                    **{k:state[k] for k in ('net_account_pnl_usd','realized_usd','unrealized_usd','equity_usd','financing_usd','fees_usd',
                        'starting_capital_usd','return_fraction','open_lot_count','pending_order_units','rejected_events')},
                    'actions':report['actions'][arm],'decision_reasons':dict(Counter(d['reason'] for d in selected)),
                    'fills':len(fills),'turnover_absolute_base_units_by_instrument':dict(turnover),
                    'turnover_scope':'executed_absolute_base_units_per_instrument; currencies_not_summed_as_USD',
                    'episodes':sequence,'filled_replacement_episodes':sum(e['replacement_parent'] is not None for e in episodes),
                    'consecutive_same_instrument_side_reversals':sum(a['instrument']==b['instrument'] and a['side']!=b['side'] for a,b in zip(sequence,sequence[1:])),
                    'reversal_scope':'consecutive_filled_episodes_only; no_arbitrary_elapsed_threshold',
                    'empty_alternative_decisions':sum(not d['values'].get('alternatives') for d in selected),
                    'unavailable_mark_epochs':sorted({r['epoch'] for r in ledger if r['arms'][arm]['equity_usd'] is None}),
                    **report['valuation_coverage'][arm]})
    by={(r['method'],r['scenario'],r['arm']):r for r in rows};matched=[];availability=[]
    for base in ('ridge','recovered_hgb'):
        for mode in ('frozen','expanding'):
            raw=base+'__raw_matched_'+mode;signed=base+'__signed_only_'+mode;aug=base+'__magnitude_interaction_'+mode
            for scenario in c['scenarios']:
                # Source coverage equality is established independently of PnL.
                cov=[records[m+'-'+scenario+'-reference']['coverage'] for m in (raw,signed,aug)]
                def strip(values):return [{k:v for k,v in r.items() if k!='method'} for r in values]
                if not strip(cov[0])==strip(cov[1])==strip(cov[2]):raise ValueError('later_report_matched_coverage_mismatch')
                for arm in c['original_policy_contract']['policies']:
                    for left,right in ((signed,raw),(aug,raw),(aug,signed)):
                        matched.append(paired(by[left,scenario,arm],by[right,scenario,arm],'same_causal_forecast_support'))
                    availability.append(paired(by[raw,scenario,arm],by[base+'__raw_unrestricted',scenario,arm],
                        'availability_effect_only_not_layer_skill'))
    return {'schema_version':'forex_later_policy_report.v1','scope':'single_inspected_two_day_development_candle_scenario',
        'methods':c['methods'],'scenarios':list(c['scenarios']),'rows':rows,'paired_deltas':matched,
        'availability_effect_diagnostics':availability,'coverage':coverage,'run_identities':identities,
        'terminal_status_counts':dict(Counter(r['terminal_status'] for r in rows)),
        'uncertainty':'not_estimated_single_dependent_inspected_cohort','confirmation':False,'independent_review':False,**c['readiness']}


def render(report):
    lines=['# Later remaining-layer position policies','','All frozen variants and cost scenarios are retained. One inspected development cohort with hypothetical candle fills; no model selection or profitability claim.',
        '','| Forecast | Scenario | Fixed hold USD | Naive USD | Continuation USD | Hysteresis USD |','|---|---|---:|---:|---:|---:|']
    by={(r['method'],r['scenario'],r['arm']):r for r in report['rows']}
    for method in report['methods']:
        for scenario in report['scenarios']:
            cells=[]
            for arm in ('fixed_hold','naive','continuation','hysteresis'):
                r=by[method,scenario,arm]
                cells.append('unpriced' if r['net_account_pnl_usd'] is None else str(Decimal(r['net_account_pnl_usd']).quantize(Decimal('.0001')))+(' (open)' if r['terminal_status']=='marked_open_terminal' else ''))
            lines.append('| '+' | '.join([method,scenario.removeprefix('later_candle_'),*cells])+' |')
    lines+=['','Full JSON includes all six isolated policy arms, cash/recovered abstention,144 matched differences,48 separate availability diagnostics, terminal gaps, turnover by instrument, fees, financing, decisions and episodes.',
        '','Matched raw controls and calibrated variants have identical forecast availability. Unrestricted raw comparisons isolate availability effects and are not evidence of layer skill. No continuous-risk or intrabar-whipsaw claim.']
    return '\n'.join(lines)+'\n'


if __name__=='__main__':
    p=argparse.ArgumentParser()
    for n in ('runs-dir','operator-receipt','output'):p.add_argument('--'+n,type=Path,required=True)
    p.add_argument('--receipt-sha256',required=True);a=p.parse_args()
    if hashlib.sha256(a.operator_receipt.read_bytes()).hexdigest()!=a.receipt_sha256:raise ValueError('later_report_receipt_pin_mismatch')
    result=build(a.runs_dir,read(a.operator_receipt));a.output.mkdir(parents=True,exist_ok=True)
    (a.output/'RESULT_SUMMARY.json').write_text(json.dumps(result,sort_keys=True,indent=2)+'\n',encoding='utf-8')
    (a.output/'RESULT_SUMMARY.md').write_text(render(result),encoding='utf-8')
    print(json.dumps({'rows':len(result['rows']),'paired_comparisons':len(result['paired_deltas']),'terminal_status':result['terminal_status_counts']}))
