"""Two fitted methods on the same preserved all68 candle execution scenarios."""
import base64,json
from copy import deepcopy
from pathlib import Path
from contracts import fingerprint
from historical_native_input_v2 import TIER,ORIGIN,TARGET,ROLLOVER,SCENARIOS,panel,financing_event_rates,validate_historical_frame
from matched_remaining_native_v2 import EPOCHS,METHODS
from matched_policy_input_v2 import PROFILE,adapted_candidates
from policy_continuation_v2 import retrospective_policy_contract,PolicyReplay
from reference_accounting_adapter_v2 import DEFAULT_TRAD

def read(p):return json.loads(Path(p).read_text())

def prepare_input(market_path,technical,remaining):
    """Package exact completed dependency artifacts; the operator pins this file."""
    from publication import verify_completed_run
    for root in (technical,remaining):verify_completed_run(root,read(root/'RUN_IDENTITY.json'))
    market=read(market_path);observations=[o for pair in market['universe'] for o in read(technical/('pair_'+pair+'.json'))['observations'] if o['origin_epoch'] in EPOCHS]
    fits={str(h):{'fit':read(remaining/f'fit_{h}.json'),'tree_base64':base64.b64encode((remaining/f'fit_{h}.joblib').read_bytes()).decode('ascii')} for h in sorted((TARGET-t)//60 for t in EPOCHS)}
    return {'schema_version':PROFILE,'market':market,'observations':observations,'fits':fits,'references':read(remaining/'references.json'),'packets':read(remaining/'native_packets.json'),
        'dependency_identities':{'technical':read(technical/'RUN_IDENTITY.json'),'remaining':read(remaining/'RUN_IDENTITY.json')}}

def fixture(inputs,method,scenario_id='candle_zero_slippage_financing',trad_root=DEFAULT_TRAD):
    if inputs['schema_version']!=PROFILE or method not in METHODS:raise ValueError('matched_policy_input_or_method_required')
    market=inputs['market'];scenario=deepcopy(SCENARIOS[scenario_id]);universe=market['universe']
    if len(universe)!=68 or len(set(universe))!=68 or set(market['metadata'])!=set(universe):raise ValueError('matched_all68_universe_required')
    contract=retrospective_policy_contract(trad_root);config=contract['ledger']['reference_config']
    config['metadata']=deepcopy(market['metadata']);config['slippage_bps_per_leg']=scenario['slippage_bps_per_leg'];config['execution_delay_sec']=58
    config=PolicyReplay(contract,trad_root=trad_root).book.config
    points={(r['instrument'],r['price_epoch']):r for r in market['rows']}
    if len(points)!=len(market['rows']):raise ValueError('duplicate_matched_market_input')
    frames=[];coverage=[]
    def frame(kind,epoch,price_epoch):
        rows=[points[(pair,price_epoch)] for pair in universe]
        return {'kind':kind,'epoch':epoch,'input_tier':TIER,'scenario':scenario,'model_profile':PROFILE,'method':method,'market_points':rows,'quotes':panel(rows,epoch)}
    def execution(epoch):
        f=frame('execution',epoch,epoch);f['fills']={arm:{'units':'remaining','evidence_id':f'scenario-full-fill:{scenario_id}:{arm}:{epoch}'} for arm in contract['policies']};return f
    for origin in EPOCHS:
        f=frame('decision',origin+2,origin);h=(TARGET-origin)//60
        f.update(target_epoch=TARGET,candidate_kind='curve',terminal=False,
            matched_input={**deepcopy(inputs['fits'][str(h)]),'observations':[o for o in inputs['observations'] if o['origin_epoch']==origin],'references':inputs['references'][str(origin)]},
            historical_packets=[p for p in inputs['packets'] if p['method']==method and p['conditioning_epoch']==origin])
        f['candidates'],f['historical_refusals']=adapted_candidates(f,config,trad_root)
        accepted={p['instrument'] for p in f['candidates']};refusals={p['instrument']:p['reason'] for p in f['historical_refusals']}
        for pair in universe:coverage.append({'instrument':pair,'method':method,'conditioning_epoch':origin,'original_target_epoch':TARGET,'status':'admitted' if pair in accepted else refusals.get(pair,'remaining_forecast_unavailable')})
        frames.extend([f,execution(origin+60)])
    fin=frame('financing',ROLLOVER,ORIGIN+86400);fin.update(rates=financing_event_rates(fin['quotes'],ROLLOVER,scenario,trad_root),provenance_id='declared-single-rollover:'+fingerprint(scenario),accrual_period_id='cohort-single-scenario-rollover');frames.append(fin)
    terminal=frame('decision',TARGET-58,TARGET-60);terminal.update(target_epoch=TARGET,candidate_kind='curve',matched_input=None,historical_packets=[],terminal=True,candidates=[],historical_refusals=[])
    frames.extend([terminal,execution(TARGET)]);frames.sort(key=lambda f:f['epoch'])
    for f in frames:validate_historical_frame(f,config,trad_root)
    return contract,frames,coverage
