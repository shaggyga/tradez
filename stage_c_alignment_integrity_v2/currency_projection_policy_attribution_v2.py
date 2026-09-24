"""Describe completed policy paths without rerunning or selecting them."""
import argparse
import hashlib
import json
from collections import Counter
from pathlib import Path

ROOT=Path(__file__).resolve().parent
read=lambda p:json.loads(Path(p).read_bytes())
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()

def build(contract_path,runs):
    c=read(contract_path); parent=read(ROOT/'CURRENCY_PROJECTION_POLICY_CONTRACT_V2.json')
    if sha(ROOT/'CURRENCY_PROJECTION_POLICY_CONTRACT_V2.json')!=c['parent_contract_sha256']:raise ValueError('projection_attribution_parent_contract_changed')
    rows=[]; runs=Path(runs)
    for method in c['methods']:
        for scenario,item in parent['scenarios'].items():
            root=runs/(method+'-'+scenario+'-reference'); report=read(root/'run_report.json'); state=read(root/'policy_state.json')
            decisions=[json.loads(x) for x in (root/'policy_decisions.jsonl').read_text().splitlines()]
            ledger=[json.loads(x) for x in (root/'event_ledger.jsonl').read_text().splitlines()]
            if report['accounting_oracle']['status']!='verified' or len(decisions)!=54:raise ValueError('projection_attribution_parent_accounting')
            for arm,value in report['arms'].items():
                selected=[x for x in decisions if x['arm']==arm and x['action'] in ('ENTER','REPLACE')]
                fills=[x for x in ledger if x.get('arm')==arm and x['kind']=='fill' and x['receipt']['status']=='filled' for leg in x['receipt']['legs'] if leg['kind']=='open']
                finance=[x for x in ledger if x.get('arm')==arm and x['kind']=='financing']
                rows.append({'method':method,'scenario':scenario,'cohort':item['cohort'],'arm':arm,'selected':len(selected),'opened':len(fills),'selected_without_open':len(selected)-len(fills),'financing_statuses':dict(Counter(x['receipt']['status'] for x in finance)),'terminal_open_lots':value['open_lot_count'],'terminal_pending_units':value['pending_order_units'],'net_account_pnl_usd':value['net_account_pnl_usd'],'terminal_priced':value['net_account_pnl_usd'] is not None})
    if len(rows)!=len(c['methods'])*len(parent['scenarios'])*len(parent['expected_policy_arms']):raise ValueError('projection_attribution_inventory')
    return {'schema_version':c['schema_version'],'rows':rows,'path_count':24,'account_count':len(rows),'selection_total':sum(x['selected'] for x in rows),'opened_total':sum(x['opened'] for x in rows),'unopened_total':sum(x['selected_without_open'] for x in rows),'all_terminal_priced':all(x['terminal_priced'] for x in rows),'limitations':'descriptive dependent development scenarios; no winner selection, aggregation, confirmation, profitability or trading claim',**c['readiness']}

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--contract',type=Path,required=True);p.add_argument('--runs-dir',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();r=build(a.contract,a.runs_dir);a.output.mkdir(parents=True,exist_ok=True);(a.output/'ATTRIBUTION.json').write_text(json.dumps(r,sort_keys=True,indent=2)+'\n');print(json.dumps({'rows':len(r['rows']),'selected':r['selection_total'],'opened':r['opened_total'],'unopened':r['unopened_total']}))
