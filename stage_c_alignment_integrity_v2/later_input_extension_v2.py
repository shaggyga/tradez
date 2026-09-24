"""Reuse causal features/endpoints and preserve original rows exactly."""
from collections import Counter
from decimal import Decimal
from contracts import fingerprint
from causal_technical_adapter_v2 import pair_records,summarize
from later_surface_native_v2 import market_inputs
from historical_market_inputs_v2 import validate_record
from historical_native_input_v2 import panel
from reference_accounting_adapter_v2 import load_reference


def indexed(rows):
    result={}
    for r in rows:
        key=(r['record_id'],r['target_id'])
        if key in result:raise ValueError('extension_duplicate_outcome')
        result[key]=r
    return result


def verify_prefix(part,original,remaining):
    if part['instrument']!=original['instrument'] or part['quality']!=original['quality']:
        raise ValueError('extension_original_pair_quality_changed')
    for key in ('observations','path_outcomes','calendar_coverage'):
        if fingerprint(part[key][:len(original[key])])!=fingerprint(original[key]):
            raise ValueError('extension_original_'+key+'_changed')
    lookup=indexed(part['outcomes'])
    for key,rows in (('technical',original['outcomes']),('remaining',remaining)):
        try:matched=[lookup[r['record_id'],r['target_id']] for r in rows]
        except KeyError as exc:raise ValueError('extension_original_label_missing') from exc
        if fingerprint(matched)!=fingerprint(rows):raise ValueError('extension_original_'+key+'_labels_changed')
    return {'observations_identical':len(original['observations']),'technical_labels_identical':len(original['outcomes']),
        'remaining_labels_identical':len(remaining),'original_observation_sha256':fingerprint(original['observations']),
        'original_technical_labels_sha256':fingerprint(original['outcomes']),'original_remaining_labels_sha256':fingerprint(remaining)}


def prepare_pair(member,raw,original,remaining,c):
    part=pair_records(member['instrument'],raw,member['pip_size'],member['source_member_sha256'],c['technical_contract'])
    proof=verify_prefix(part,original,remaining)
    expected=list(range(c['technical_contract']['origin_start'],c['technical_contract']['origin_end'],c['technical_contract']['cadence_seconds']))
    if [r['origin_epoch'] for r in part['observations']]!=expected or len({r['record_id'] for r in part['observations']})!=len(expected):
        raise ValueError('extension_exact_origin_inventory')
    for o in part['observations']:
        if o['available_epoch']!=o['origin_epoch'] or o['source_bar_start_epoch']!=o['origin_epoch']-60:
            raise ValueError('extension_original_close_clock_required')
        if o['features'] is not None and len(o['features'])!=26:raise ValueError('extension_original26features_required')
    origins={r['record_id']:r['origin_epoch'] for r in part['observations']}
    for outcome in part['outcomes']:
        horizon=int(outcome['target_id'].removeprefix('technical_endpoint_midpoint_elapsed_').removesuffix('m'))
        if horizon not in c['technical_contract']['endpoint_minutes'] or outcome['label_end_epoch']!=origins[outcome['record_id']]+horizon*60 or outcome['available_epoch']!=outcome['label_end_epoch']:
            raise ValueError('extension_exact_target_maturity_required')
    if len(part['outcomes'])!=len(expected)*len(c['technical_contract']['endpoint_minutes']):raise ValueError('extension_full_endpoint_inventory')
    part['original_overlap_proof']=proof;return part


def plain(value):
    if isinstance(value,Decimal):return str(value)
    if isinstance(value,dict):return {k:plain(v) for k,v in value.items()}
    if isinstance(value,list):return [plain(v) for v in value]
    return value


def market_consumer(market,cohort,trad):
    ref=load_reference(trad);points={(r['instrument'],r['price_epoch']):r for r in market['rows']}
    if len(points)!=len(market['rows']):raise ValueError('extension_duplicate_market_point')
    for r in market['rows']:validate_record(r)
    phases=[('decision',t+2,t) for t in cohort['policy_origins']]+[('execution',t+60,t+60) for t in cohort['policy_origins']]
    phases.extend([('financing',cohort['origin']+86400+1,cohort['origin']+86400),
                   ('terminal_decision',cohort['target']-58,cohort['target']-60),('terminal_fill',cohort['target'],cohort['target'])])
    rows=[]
    for phase,epoch,price_epoch in sorted(phases,key=lambda x:x[1]):
        panel_points=[points[pair,price_epoch] for pair in market['universe']];quotes=panel(panel_points,epoch)
        for pair in market['universe']:
            conversions={};reasons=[]
            for kind,currency in (('base',pair[:3]),('quote',pair[4:])):
                try:conversions[kind]=plain(ref.usd_rates(currency,quotes,epoch,60))
                except (ValueError,KeyError):conversions[kind]=None;reasons.append(kind+'_USD_conversion_unavailable')
            if pair not in quotes:reasons.append('pair_quote_unavailable')
            rows.append({'cohort':cohort['name'],'phase':phase,'epoch':epoch,'price_epoch':price_epoch,'instrument':pair,
              'point_sha256':points[pair,price_epoch]['record_sha256'],'point_status':points[pair,price_epoch]['status'],
              'quote_available':pair in quotes,'conversions':conversions,'financing_conversion_inputs_available':not reasons,
              'unavailable_reasons':reasons,'scope':'input_availability_only; no_rate_or_charge_assumed'})
    if len(rows)!=19*68:raise ValueError('extension_full_phase_pair_coverage')
    return {'cohort':cohort['name'],'rows':rows,'phase_counts':dict(Counter(r['phase'] for r in rows)),
      'unavailable_reasons':dict(Counter(reason for r in rows for reason in r['unavailable_reasons'])),
      'policy_replay':False,'financing_applied':False,'execution_qualified':False}


def prepare_market(slices,manifest,cohort,original_market,trad):
    market=market_inputs(slices,manifest,{'policy_origins':cohort['policy_origins'],'common_target_epoch':cohort['target']})
    if cohort['name']=='original_overlap' and market!=original_market:raise ValueError('extension_original_market_changed')
    return {'market':market,'consumer':market_consumer(market,cohort,trad),
        'original_market_identical':cohort['name']=='original_overlap'}


def report(parts,markets,c):
    result=summarize(parts,c['technical_contract']);proof=[p['original_overlap_proof'] for p in parts]
    totals={'observations_identical':sum(p['observations_identical'] for p in proof),
      'technical_labels_identical':sum(p['technical_labels_identical'] for p in proof),
      'remaining_labels_identical':sum(p['remaining_labels_identical'] for p in proof)}
    if totals!={'observations_identical':5168,'technical_labels_identical':36176,'remaining_labels_identical':25840}:
        raise ValueError('extension_all_original_rows_required')
    if result['observation_rows']!=6800 or result['outcome_rows']!=81600 or result['calendar_blocked_rows']!=20400:
        raise ValueError('extension_expected_all68_counts')
    return {**result,'status':'completed_later_causal_input_extension','original_overlap':totals,
      'added_observations':1632,'cohorts':[{'cohort':m['consumer']['cohort'],'unavailable_reasons':m['consumer']['unavailable_reasons'],
        'original_market_identical':m['original_market_identical']} for m in markets],
      'base_model_fits':0,'base_model_loads':0,'layer_fits':0,'policy_replays':0,'api_calls':0,
      'confirmation':False,'independent_review':False,**c['readiness']}
