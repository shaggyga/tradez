"""Explicit paper entry-cost scenarios over authenticated observed inputs.

Reuses original Decimal sizing/conversion. This is not a calibrated conditional
value, fill, actual account balance, or permission to enter. Missing cost/risk
declarations remain missing; historical zero-cost defaults are never supplied.
"""
from pathlib import Path
from collections import Counter
from decimal import Decimal, localcontext
import argparse
import json
import sys
sys.path.insert(0,str(Path(__file__).resolve().parent))
import forex_paper_quote_observation as observations

prior=observations.prior
contract=observations.paper.binding.contract
SCHEMA='paper_entry_economic_scenario.v1'


def sources():
    return {**observations.sources(),'tools/forex_paper_entry_economics.py':prior.sha(prior.read(Path(__file__)))}


def evaluate(row, quotes, declaration, *, book_id, decision):
    """Declared scenarios only; never silently promote assumptions to observations."""
    need=prior.need; exact=contract.exact
    need(declaration['schema']==SCHEMA and declaration['evidence_tier']=='declared_paper_scenario','explicit_scenario_required')
    need(row['status']=='priced_observation','priced_observation_required')
    c=row['candidate'];pair=row['instrument'];terminal=c['original_target_epoch']
    need(declaration['book_id']==book_id and declaration['slot']==row['connection']+'/'+pair and
         declaration['receipt_sha256']==row['receipt_sha256'] and declaration['quote_id']==row['quote_id'], 'scenario_observation_identity')
    need(declaration['decision_epoch']==decision and declaration['terminal_epoch']==terminal,'scenario_clock_identity')
    need(declaration['costs_available_epoch']<=decision and declaration['costs_valid_from_epoch']<=decision and
         declaration['costs_valid_through_epoch']>=terminal,'cost_horizon_not_covered')
    for k in ('costs_available_epoch','costs_valid_from_epoch','costs_valid_through_epoch'):
        observations.paper.forecasts.native.epoch(declaration[k])
    need(declaration['valuation_convention']=='original_terminal_mid_frozen_current_spread_and_conversion', 'explicit_terminal_proxy_required')
    need(declaration['slippage_convention']=='separate_cost_once' and declaration['spread_in_gross'] is True,
         'cost_double_count_refused')
    need(declaration['financing_scope']=='entry_through_terminal_including_rollover','financing_scope_required')
    need(isinstance(declaration['cost_basis'],str) and bool(declaration['cost_basis'].strip()),'cost_basis_required')
    need(type(declaration['side']) is int and declaration['side'] in (-1,1),'entry_side')
    risk=declaration['risk'];need(risk['account_currency']=='USD' and risk['budget_kind']=='declared_paper_allocation','paper_budget_required')
    for key in ('notional_usd','maximum_notional_usd','available_margin_usd','margin_rate','maximum_loss_usd','declared_worst_loss_usd'):
        exact(risk[key],nonnegative=True)
    need(0<exact(risk['notional_usd'])<=exact(risk['maximum_notional_usd']) and
         0<exact(risk['margin_rate'])<=1 and exact(risk['declared_worst_loss_usd'])<=exact(risk['maximum_loss_usd']), 'declared_risk_capacity_refused')
    costs=declaration['future_costs_usd']
    need(set(costs)=={'fees','slippage','financing_debit','financing_credit'},'all_future_costs_required')
    costs={k:exact(v,nonnegative=True) for k,v in costs.items()}
    manager=prior.load_reference(observations.paper.ROOT/'trad')
    config=dict(metadata={pair:dict(base_currency=pair[:3])},notional_usd=risk['notional_usd'],quote_max_age_sec=30,
        sizing_policy='fixed_usd_notional_integer_base_units_at_decision')
    units,sizing=manager.size_at_decision(pair,quotes,decision,config)
    margin=sizing['decision_value_usd']*exact(risk['margin_rate'])
    need(margin<=exact(risk['available_margin_usd']),'declared_margin_refused')
    q=manager.quote_at(quotes,pair,decision,30)
    mid=exact(c['original_terminal_price_estimate']);spread=q['ask']-q['bid']
    terminal_bid,terminal_ask=mid-spread/2,mid+spread/2
    need(terminal_bid>0,'terminal_proxy_nonpositive')
    side=declaration['side'];entry=q['ask'] if side==1 else q['bid'];exit_price=terminal_bid if side==1 else terminal_ask
    gross=side*units*(exit_price-entry)
    usd,conversion=manager.convert_pnl_to_usd(gross,pair[4:],quotes,decision,30)
    net=usd-costs['fees']-costs['slippage']-costs['financing_debit']+costs['financing_credit']
    return dict(status='declared_entry_scenario_consistent',base_units=units,side=side,
        gross_quote_proxy=format(gross,'f'),gross_usd_proxy=format(usd,'f'),net_usd_proxy=format(net,'f'),
        cash_control_usd='0',declared_margin_usd=format(margin,'f'),margin_is_expense=False,
        future_costs_usd={k:format(v,'f') for k,v in costs.items()},
        sizing=manager.jsonable(sizing),conversion=manager.jsonable(conversion),
        terminal_price_scope='frozen_spread_proxy_not_executable_exit_or_conditional_prediction',
        risk_scope='supplied_paper_budget_not_observed_account_capacity_or_guaranteed_loss_bound',
        evidence_tier='declared_paper_scenario',declaration_sha256=observations.paper.digest(declaration),
        **observations.paper.binding.FLAGS)


def inspect(book,capture,joined_request,request):
    prior.need(request['schema']==SCHEMA and request['source_bindings']==sources(),'entry_source_identity')
    prior.need(request['observation_request_sha256']==observations.paper.digest(joined_request),'entry_observation_binding')
    joined,report=observations.inspect(book,capture,joined_request)
    proposals=request['declarations'];expected={r['slot'] for r in joined}
    prior.need(isinstance(proposals,dict) and set(proposals)<=expected,'entry_population')
    join=joined_request['combined_request'];decision=join['decision_epoch']
    quotes=observations.combined.quotes.map_receipts(join['quote_snapshot'],observed_epoch=join['quote_observed_epoch'],
        decision_epoch=decision,instruments=join['quote_snapshot']['instruments'])['quotes']
    rows=[]
    for row in joined:
        out=dict(status='entry_inputs_missing',reason='explicit_cost_and_paper_risk_declaration_required',**observations.paper.binding.FLAGS)
        if row['slot'] in proposals:
            try:
                with localcontext() as ctx:
                    ctx.prec=80
                    out=evaluate(row['observation'],quotes,proposals[row['slot']],
                        book_id=report['paper_report']['episode_id'],decision=decision)
            except (ValueError,TypeError,KeyError,ArithmeticError) as exc:
                out=dict(status='entry_inputs_refused',reason=type(exc).__name__+':'+str(exc),**observations.paper.binding.FLAGS)
        rows.append(dict(slot=row['slot'],**out))
    return rows,dict(schema=SCHEMA,population=len(rows),statuses=dict(Counter(r['status'] for r in rows)),
        scope='Declared entry arithmetic and input consistency only; no calibrated economics, action selection or fills',
        observation_request_sha256=request['observation_request_sha256'],**observations.paper.binding.FLAGS)


def run(book,capture,observation_path,request_path,expected,output,run_id,*,resume=False,max_new=None):
    prior.need(max_new is None or type(max_new) is int and max_new>0,'positive_chunk_limit')
    raw=prior.read(Path(request_path));prior.need(prior.sha(raw)==expected,'entry_request_external_hash')
    request=json.loads(raw);joined=json.loads(prior.read(Path(observation_path)))
    rows,report=inspect(book,capture,joined,request)
    payloads={f'rows_{i//32:04d}.json':prior.encoded(rows[i:i+32]) for i in range(0,len(rows),32)}
    payloads['REPORT.json']=prior.encoded(report)
    identity=prior.effective_run_identity(contract=dict(schema=SCHEMA,request_sha256=expected,required_payloads=sorted(payloads)),dependency_hashes=sources())
    pub=prior.RunPublisher(output,run_id,identity)
    if (pub.root/'COMPLETION_MANIFEST.json').exists():
        prior.verify_completed_run(pub.root,identity);return {'status':'verified_completed'}
    pub.acquire(recover=resume);complete=False;added=0;receipts=[]
    try:
        for name,body in payloads.items():
            if pub.read_verified_payload(name) is None:
                if max_new is not None and added>=max_new:return {'status':'checkpointed'}
                added+=1
            receipts.append(pub.write_or_validate_payload(name,body))
        pub.complete(receipts,set(payloads));complete=True
    finally:
        if not complete:pub.release()
    return {'status':'completed',**report}


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('book','capture','observation','request','output'):p.add_argument('--'+name,type=Path,required=True)
    p.add_argument('--sha256',required=True);p.add_argument('--run-id',default='entry-inputs')
    p.add_argument('--resume',action='store_true');p.add_argument('--max-new',type=int)
    a=p.parse_args();print(json.dumps(run(a.book,a.capture,a.observation,a.request,a.sha256,a.output,a.run_id,resume=a.resume,max_new=a.max_new)))
