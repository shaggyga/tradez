"""Translate only cohort dates; reuse the unchanged original policy/risk contract."""
from copy import deepcopy
from contracts import fingerprint
from historical_native_input_v2 import panel, financing_event_rates, validate_historical_frame
from later_policy_scenario_v2 import PROFILE, TIER, contract, scenarios, authority
from later_policy_input_v2 import adapted_candidates
from policy_continuation_v2 import retrospective_policy_contract, PolicyReplay
from reference_accounting_adapter_v2 import DEFAULT_TRAD


def fixture(inputs, method, scenario_id, trad_root=DEFAULT_TRAD):
    c = contract(); a = authority(); market = inputs['market']; universe = market['universe']
    if method not in c['methods'] or scenario_id not in c['scenarios']:
        raise ValueError('later_policy_registered_method_scenario_required')
    if universe != c['universe'] or fingerprint(market['metadata']) != a['market_metadata_sha256']:
        raise ValueError('later_policy_original_market_inventory_required')
    policy = retrospective_policy_contract(trad_root)
    if policy != c['original_policy_contract'] or fingerprint(policy) != c['original_policy_contract_sha256']:
        raise ValueError('later_policy_original_risk_policy_parameters_changed')
    scenario = scenarios()[scenario_id]; config = policy['ledger']['reference_config']
    config['metadata'] = deepcopy(market['metadata']); config['slippage_bps_per_leg'] = scenario['slippage_bps_per_leg']
    config['execution_delay_sec'] = 58; config = PolicyReplay(policy, trad_root=trad_root).book.config
    points = {(r['instrument'], r['price_epoch']): r for r in market['rows']}
    if len(points) != len(market['rows']): raise ValueError('later_policy_unique_market_points_required')
    frames, coverage = [], []
    def make_frame(kind, epoch, price_epoch):
        rows = [points[pair, price_epoch] for pair in universe]
        return {'kind': kind, 'epoch': epoch, 'input_tier': TIER, 'scenario': scenario, 'model_profile': PROFILE,
                'method': method, 'market_points': rows, 'quotes': panel(rows, epoch)}
    def execution(epoch):
        f = make_frame('execution', epoch, epoch)
        f['fills'] = {arm: {'units': 'remaining', 'evidence_id': f'scenario-full-fill:{scenario_id}:{arm}:{epoch}'} for arm in policy['policies']}
        return f
    base, variant = method.split('__')
    for origin in c['origins']:
        source = inputs['frames'][str(origin)]; f = make_frame('decision', origin+2, origin)
        f.update(target_epoch=c['target_epoch'], candidate_kind='curve', terminal=False,
            later_input={'observations': sorted([o for o in inputs['observations'] if o['origin_epoch'] == origin], key=lambda r: r['instrument']),
                'predictions': [r for r in source['predictions'] if (r['base_method'], r['variant']) == (base, variant)],
                'coverage': [r for r in source['coverage'] if (r['base_method'], r['variant']) == (base, variant)],
                'source_frame_sha256': a['frames'][str(origin)]['source_frame_sha256']},
            historical_packets=[p for p in source['packets'] if (p['prediction']['base_method'], p['prediction']['variant']) == (base, variant)])
        f['candidates'], f['historical_refusals'] = adapted_candidates(f, config, trad_root)
        accepted = {p['instrument'] for p in f['candidates']}; refused = {p['instrument']: p['reason'] for p in f['historical_refusals']}
        original = {r['instrument']: r for r in f['later_input']['coverage']}
        for pair in universe:
            coverage.append({'instrument': pair, 'method': method, 'conditioning_epoch': origin,
                'original_target_epoch': c['target_epoch'], 'source_coverage': original[pair]['reason'],
                'status': 'admitted' if pair in accepted else refused.get(pair, original[pair]['reason'])})
        frames.extend([f, execution(origin+60)])
    fin = make_frame('financing', c['rollover_epoch'], c['rollover_price_epoch'])
    fin.update(rates=financing_event_rates(fin['quotes'], fin['epoch'], scenario, trad_root),
               provenance_id='declared-single-rollover:'+fingerprint(scenario), accrual_period_id='cohort-single-scenario-rollover')
    frames.append(fin)
    terminal = make_frame('decision', c['target_epoch']-58, c['target_epoch']-60)
    terminal.update(target_epoch=c['target_epoch'], candidate_kind='curve', later_input=None, historical_packets=[],
                    terminal=True, candidates=[], historical_refusals=[])
    frames.extend([terminal, execution(c['target_epoch'])]); frames.sort(key=lambda f: f['epoch'])
    for f in frames: validate_historical_frame(f, config, trad_root)
    return policy, frames, coverage
