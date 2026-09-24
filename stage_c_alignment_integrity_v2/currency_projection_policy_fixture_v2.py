"""Build dated all-68 policy frames from the qualified projection-native parent."""
from copy import deepcopy
from contracts import fingerprint
from historical_native_input_v2 import panel, financing_event_rates, validate_historical_frame
from policy_continuation_v2 import retrospective_policy_contract, PolicyReplay
from currency_projection_policy_input_v2 import PROFILE, adapted_candidates

TIER = 'historical_fitted_candle_policy_scenario.v1'

def scenario(contract, name):
    item = contract['scenarios'][name]; cohort = contract['cohorts'][item['cohort']]
    return {'scenario_id': name, 'input_tier': TIER, 'slippage_bps_per_leg': item['slippage_bps_per_leg'],
        'single_rollover_cost_bps_of_base_usd': item['single_rollover_cost_bps_of_base_usd'], 'rollover_epoch': cohort['rollover_epoch'],
        'fill_rule': 'full_pending_units_at_next_completed_M1_close_after_58s_delay',
        'arrival_rule': 'assumed_M1_close_plus_two_seconds_model_availability', 'observed_execution': False, 'broker_access': False}

def fixture(contract, native_frames, markets, method, scenario_name, trad_root):
    item = contract['scenarios'][scenario_name]; cohort_name = item['cohort']; cohort = contract['cohorts'][cohort_name]
    policy = retrospective_policy_contract(trad_root); sc = scenario(contract, scenario_name)
    market = markets[cohort_name]; declared = policy['ledger']['reference_config']
    declared['metadata'] = deepcopy(market['metadata']); declared['slippage_bps_per_leg'] = sc['slippage_bps_per_leg']; declared['execution_delay_sec'] = 58
    config = PolicyReplay(policy, trad_root=trad_root).book.config
    universe = market['universe']; points = {(row['instrument'], row['price_epoch']): row for row in market['rows']}
    if len(universe) != 68 or len(points) != len(market['rows']):
        raise ValueError('currency_projection_policy_all68_market_required')
    base, variant = method.split('__'); frames, coverage = [], []
    def make(kind, epoch, price_epoch):
        rows = [points[pair, price_epoch] for pair in universe]
        return {'kind': kind, 'epoch': epoch, 'input_tier': TIER, 'scenario': sc, 'model_profile': PROFILE, 'method': method,
            'cohort': cohort_name, 'market_points': rows, 'quotes': panel(rows, epoch)}
    def execution(epoch):
        frame = make('execution', epoch, epoch)
        frame['fills'] = {arm: {'units': 'remaining', 'evidence_id': f'scenario-full-fill:{scenario_name}:{arm}:{epoch}'} for arm in policy['policies']}
        return frame
    for origin in cohort['origins']:
        source = native_frames[cohort_name, origin]; pred = [x for x in source['predictions'] if (x['base_method'], x['variant']) == (base, variant)]
        packets = [x for x in source['packets'] if (x['prediction']['base_method'], x['prediction']['variant']) == (base, variant)]
        source_coverage = [x for x in source['coverage'] if (x['base_method'], x['variant']) == (base, variant)]
        if len(source_coverage) != 68 or {x['instrument'] for x in source_coverage} != set(universe) or len(pred) != len(packets):
            raise ValueError('currency_projection_policy_complete_coverage_required')
        frame = make('decision', origin + 2, origin)
        frame.update(target_epoch=cohort['target_epoch'], candidate_kind='curve', terminal=False, historical_packets=packets,
            projection_input_authority={'method': method, 'cohort': cohort_name, 'origin_epoch': origin, 'target_epoch': cohort['target_epoch'],
                'market_panel_sha256': fingerprint(frame['market_points']), 'packet_sha256': fingerprint(packets)})
        frame['candidates'], frame['historical_refusals'] = adapted_candidates(frame, config, trad_root)
        accepted = {x['instrument'] for x in frame['candidates']}; refused = {x['instrument']: x['reason'] for x in frame['historical_refusals']}
        original = {x['instrument']: x for x in source_coverage}
        coverage.extend({'instrument': pair, 'method': method, 'conditioning_epoch': origin, 'original_target_epoch': cohort['target_epoch'],
            'source_coverage': original[pair]['reason'], 'status': 'admitted' if pair in accepted else refused.get(pair, original[pair]['reason'])} for pair in universe)
        frames.extend([frame, execution(origin + 60)])
    fin = make('financing', cohort['rollover_epoch'], cohort['rollover_price_epoch'])
    fin.update(rates=financing_event_rates(fin['quotes'], fin['epoch'], sc, trad_root), provenance_id='declared-single-rollover:' + fingerprint(sc), accrual_period_id='cohort-single-scenario-rollover')
    terminal = make('decision', cohort['target_epoch'] - 58, cohort['target_epoch'] - 60)
    terminal.update(target_epoch=cohort['target_epoch'], candidate_kind='curve', terminal=True, historical_packets=[], candidates=[], historical_refusals=[],
        projection_input_authority={'method': method, 'cohort': cohort_name, 'origin_epoch': cohort['target_epoch'] - 60, 'target_epoch': cohort['target_epoch'], 'market_panel_sha256': fingerprint(terminal['market_points']), 'packet_sha256': fingerprint([])})
    frames.extend([fin, terminal, execution(cohort['target_epoch'])]); frames.sort(key=lambda row: row['epoch'])
    for frame in frames: validate_historical_frame(frame, config, trad_root)
    return policy, frames, coverage
