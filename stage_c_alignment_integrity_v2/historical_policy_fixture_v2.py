"""Build one all-universe historical candle scenario from preserved fitted outputs."""
from copy import deepcopy
from contracts import fingerprint
from historical_native_input_v2 import (TIER,ORIGIN,TARGET,ROLLOVER,SCENARIOS,panel,
    financing_event_rates,make_packet,adapted_candidates,validate_historical_frame)
from policy_continuation_v2 import PolicyReplay,retrospective_policy_contract
from reference_accounting_adapter_v2 import DEFAULT_TRAD


def fixture(inputs,scenario_id='candle_zero_slippage_financing',trad_root=DEFAULT_TRAD):
    if set(inputs)!={'market','forecasts','models','observations'}:raise ValueError('exact_historical_policy_input_set_required')
    market=inputs['market'];scenario=deepcopy(SCENARIOS[scenario_id])
    if len(market['universe'])!=68 or set(market['metadata'])!=set(market['universe']):raise ValueError('historical_all68_universe_required')
    contract=retrospective_policy_contract(trad_root)
    contract['ledger']['reference_config']['metadata']=deepcopy(market['metadata'])
    contract['ledger']['reference_config']['slippage_bps_per_leg']=scenario['slippage_bps_per_leg']
    # Forecasts arrive two seconds after the completed minute. The next exact
    # candle close is 58 seconds later; do not backdate the forecast or quote.
    contract['ledger']['reference_config']['execution_delay_sec']=58
    config=PolicyReplay(contract,trad_root=trad_root).book.config
    points={}
    for r in market['rows']:
        key=(r['instrument'],r['price_epoch'])
        if key in points:raise ValueError('duplicate_market_input')
        points[key]=r
    models={m['model_id']:m for m in inputs['models'] if m.get('status')=='fitted'}
    observations={o['record_id']:o for o in inputs['observations']}
    if len(observations)!=len(inputs['observations']):raise ValueError('duplicate_feature_input')
    forecasts={}
    for f in inputs['forecasts']:
        if f['procedure']!='frozen':continue
        key=(f['forecast']['instrument'],f['conditioning_epoch'])
        if key in forecasts:raise ValueError('duplicate_fitted_forecast')
        forecasts[key]=f
    coverage=[];frames=[]
    def frame(kind,epoch,price_epoch):
        rows=[points[(pair,price_epoch)] for pair in market['universe']]
        return {'kind':kind,'epoch':epoch,'input_tier':TIER,'scenario':scenario,
                'market_points':rows,'quotes':panel(rows,epoch)}
    def execution(epoch):
        f=frame('execution',epoch,epoch)
        f['fills']={arm:{'units':'remaining','evidence_id':f'scenario-full-fill:{scenario_id}:{arm}:{epoch}'} for arm in contract['policies']}
        return f
    for origin in range(ORIGIN,TARGET,21600):
        f=frame('decision',origin+2,origin)
        f.update(target_epoch=TARGET,candidate_kind='curve',historical_packets=[],terminal=False)
        for pair in market['universe']:
            remaining=forecasts.get((pair,origin));reason='remaining_forecast_unavailable'
            if remaining:
                try:
                    packet=make_packet(remaining,models[remaining['forecast']['model_id']],
                        observations[f'{pair}:{origin}'],points[(pair,origin)],market['metadata'][pair],trad_root)
                    f['historical_packets'].append(packet);reason='prepared'
                except (ValueError,KeyError,TypeError) as exc:reason=str(exc)
            coverage.append({'instrument':pair,'conditioning_epoch':origin,'original_target_epoch':TARGET,'status':reason})
        f['candidates'],f['historical_refusals']=adapted_candidates(f,config,trad_root)
        frames.extend([f,execution(origin+60)])
    fin=frame('financing',ROLLOVER,ORIGIN+86400)
    fin.update(rates=financing_event_rates(fin['quotes'],ROLLOVER,scenario,trad_root),
               provenance_id='declared-single-rollover:'+fingerprint(scenario),accrual_period_id='cohort-single-scenario-rollover')
    frames.append(fin)
    terminal=frame('decision',TARGET-58,TARGET-60)
    terminal.update(target_epoch=TARGET,candidate_kind='curve',historical_packets=[],terminal=True,candidates=[],historical_refusals=[])
    frames.extend([terminal,execution(TARGET)])
    frames.sort(key=lambda f:f['epoch'])
    for f in frames:validate_historical_frame(f,config,trad_root)
    return contract,frames,coverage
