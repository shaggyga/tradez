"""Bounded offline verification of immutable virtual-management evidence.

No requests, fits, database operations or runtime writes. Replayed clocks are
the explicitly retained original computation clocks, never new observations.
CLI verifies the fixed registry/source closure; output is exclusive and external.
"""
import argparse
from collections import Counter
from copy import deepcopy
from decimal import Decimal, localcontext, Context, ROUND_FLOOR
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import time

TRAD=Path(r'C:\Users\zmoor\Documents\forex\trad')
sys.dont_write_bytecode=True
sys.path.insert(0,str(TRAD))
import oanda_observed_curve_management_v1 as manager
import oanda_observed_curve_management_worker_v1 as worker
import oanda_observed_management_io_v1 as io
import oanda_forecast_curve_contract_v1 as contract
import oanda_curve_management_adapter_v1 as adapter
import oanda_research_quote_receipt_v1 as quotes

SCHEMA='observed_management_offline_verification_v1_20260909'
MAX_TOTAL_BYTES=256*1024*1024
MAX_FILES=10000
MAX_STEPS=64

def need(ok,reason):
    if not ok:raise ValueError(reason)

def sha(raw):return hashlib.sha256(raw).hexdigest()

class Reader:
    def __init__(self,root):
        self.root=Path(root).absolute();self.records={};self.total=0;self.started=time.time()
    def exists(self,parts,name):return (self.root.joinpath(*parts)/name).is_file()
    def raw(self,parts,name):
        key='/'.join((*parts,name))
        raw=io.read_file(self.root,parts,name)
        self.total+=len(raw)
        need(self.total<=MAX_TOTAL_BYTES and len(self.records)<MAX_FILES,'verification_work_bound')
        record=dict(relative_path=key,bytes=len(raw),sha256=sha(raw),verified_read_epoch=time.time())
        need(key not in self.records or self.records[key]['sha256']==record['sha256'],'immutable_file_changed_during_verification')
        self.records[key]=record
        return raw
    def json(self,parts,name):return io.decode(self.raw(parts,name))
    def reference(self,ref,parts,name):
        need(isinstance(ref,dict) and all(ref.get(k) is v for k,v in io.FLAGS.items()),'file_publication_flags')
        need(ref.get('schema_version')==io.SCHEMA and ref.get('relative_path')=='/'.join((*parts,name)),'file_reference_path')
        raw=self.raw(parts,name)
        need(ref.get('sha256')==sha(raw) and ref.get('bytes')==len(raw),'file_reference_hash_or_size')
        start=contract.epoch(ref['publication_started_epoch']);end=contract.epoch(ref['publication_completed_epoch'])
        need(start<=end<=time.time(),'file_publication_clock')
        return raw

def validate_consumed(value,registered,expected_curve=None):
    need(isinstance(value,dict),'consumed_curve_object')
    contract.validate_consumption(value['curve'],value['publication'],value['consumption'],
        expected_source_bindings=registered['source_bindings'])
    curve=value['curve'];prepared=curve['prepared_curve']
    need(prepared['scope']=='current_research' and prepared['instrument']=='GBP_USD'
         and prepared['forecast_cohort']==registered['study_id']+'/GBP_USD/official_midpoint'
         and prepared['input_context'].get('price_convention')=='official_midpoint','original_curve_scope_or_identity')
    if expected_curve is not None:need(curve==expected_curve,'initial_curve_replaced')
    return curve

def verify_original_published_files(anchor,registered):
    descriptor=worker.files._validate_descriptor(anchor['descriptor'])
    original=Reader(Path(registered['output_root'])/'published')
    parts=('curves',descriptor['curve_sha256'])
    raw=original.raw(parts,'curve.json');published=original.raw(parts,'publication.json')
    need(sha(raw)==descriptor['persisted_curve_bytes_sha256']
         and sha(published)==descriptor['publication_bytes_sha256'],'original_published_bytes_disagree')
    need(io.decode(raw)==anchor['curve'] and io.decode(published)==anchor['publication'],'original_published_payload_disagrees')
    return dict(root=str(original.root),files=list(original.records.values()))

def independent_usd_sanity(before,plan,result):
    """Separate GBPUSD arithmetic; does not invoke manager execution helpers."""
    rows={}
    with localcontext(Context(prec=96)):
        selected=result.get('selected_quote_mapping')
        q=selected['quotes']['GBP_USD'] if selected else None
        for arm in manager.ARMS:
            old=before['arms'][arm];retained=result['arms'][arm];action=retained['virtual_action']
            cash=Decimal(old['realized_usd']);pos=deepcopy(old['position']);opened=closed=0
            if action['status']=='no_virtual_fill':
                need(not action['legs'] and retained['state_after']==old,'partial_failed_virtual_action')
            else:
                for leg in action['legs']:
                    need(q is not None and leg['instrument']=='GBP_USD','leg_without_common_quote')
                    bid,ask=Decimal(q['bid']),Decimal(q['ask']);mid=(bid+ask)/2;slip=mid*Decimal('0.1')/10000
                    side=leg['side'];units=leg['base_units'];opening=leg['kind']=='open'
                    need(side in (-1,1) and type(units) is int and units>0,'leg_side_or_units')
                    raw_price=ask if (side>0)==opening else bid
                    expected_price=raw_price+(side if opening else -side)*slip
                    need(Decimal(leg['price'])==expected_price and Decimal(leg['slippage_price'])==slip,'independent_leg_price_or_slippage')
                    need(leg['quote_id']==q['quote_id'] and leg['market_epoch']==q['market_epoch']
                         and leg['available_epoch']==q['available_epoch']
                         and leg['virtual_action_epoch']==result['selected_observation_epoch'],'independent_leg_quote_clock')
                    if opening:
                        dq=plan['decision_quote_mapping']['quotes']['GBP_USD']
                        expected_units=int((Decimal(2500)/Decimal(dq['ask'])).to_integral_value(rounding=ROUND_FLOOR))
                        need(pos is None and units==expected_units,'independent_decision_sizing')
                        pos=deepcopy(leg['position']);opened+=1
                        need(Decimal(pos['entry_price'])==expected_price and pos['base_units']==units
                             and pos['entry_decision_epoch']==plan['decision_epoch']
                             and pos['entry_epoch']==result['selected_observation_epoch']
                             and pos['original_target_epoch']==plan['native_target_epoch'],'independent_entry_binding')
                    else:
                        need(pos is not None and leg['kind']=='close' and leg['original_entry']==pos
                             and side==pos['side'] and units==pos['base_units'],'independent_close_position')
                        pnl=side*units*(expected_price-Decimal(pos['entry_price']))
                        need(Decimal(leg['realized_usd'])==pnl and Decimal(leg['quote_currency_pnl'])==pnl,'independent_USD_close_pnl')
                        cash+=pnl;pos=None;closed+=1
            need(Decimal(retained['state_after']['realized_usd'])==cash
                 and retained['state_after']['position']==pos,'independent_state_accounting')
            need(Decimal(action['realized_delta_usd'])==cash-Decimal(old['realized_usd']),'independent_realized_delta')
            liquidation=None
            if pos is None:liquidation=cash
            elif q is not None:
                bid,ask=Decimal(q['bid']),Decimal(q['ask']);slip=(bid+ask)/2*Decimal('0.1')/10000
                exit_price=(bid if pos['side']==1 else ask)-pos['side']*slip
                liquidation=cash+pos['side']*pos['base_units']*(exit_price-Decimal(pos['entry_price']))
            mark=retained['liquidation_mark']['liquidation_usd']
            need((mark is None)==(liquidation is None),'independent_liquidation_missingness')
            if mark is not None:need(Decimal(mark)==liquidation,'independent_liquidation_amount')
            rows[arm]=dict(open_legs=opened,close_legs=closed,realized_usd=str(cash),
                liquidation_usd=None if liquidation is None else str(liquidation),position_open=pos is not None)
    return rows

def verify_step(reader,parts,states,config,registered,initial_curve,previous_completed):
    retained=reader.json(parts,'information_frame.json');plan=reader.json(parts,'plan.json')
    pub=reader.json(parts,'plan_publication.json');result=reader.json(parts,'settlement.json')
    state_after=reader.json(parts,'state_after.json');complete=reader.json(parts,'step_completed.json')
    settlement_pub=reader.json(parts,'settlement_publication.json')
    raw_result=reader.reference(settlement_pub,parts,'settlement.json')
    need(io.decode(raw_result)==result,'settlement_publication_payload')
    need(previous_completed<=retained['decision_epoch']<=plan['plan_created_epoch']
         <=pub['publication_started_epoch']<=pub['publication_completed_epoch']
         <=result['as_of_epoch']<=result['settlement_computed_epoch']
         <=settlement_pub['publication_started_epoch']<=settlement_pub['publication_completed_epoch']
         <=complete['completed_epoch']<=time.time(),'step_computation_publication_order')
    frame={k:deepcopy(v) for k,v in retained.items() if k not in ('quote_capture_files','curve_consumption_file','momentum_source')}
    refs=retained.get('quote_capture_files')
    if refs is None:
        frame['quote_raw']=None
    else:
        frame['quote_raw']=reader.reference(refs['raw'],parts,'decision_quote_raw.json')
        original=io.decode(reader.reference(refs['receipt'],parts,'decision_quote_receipt.json'))
        need(original==frame['quote_receipt'],'decision_quote_receipt_disagrees')
        need(max(refs[k]['publication_completed_epoch'] for k in refs)<=plan['plan_created_epoch'],'decision_input_persistence_after_plan')
    if not frame['terminal']:
        consumed=io.decode(reader.reference(retained['curve_consumption_file'],parts,'curve_consumption.json'))
        validate_consumed(consumed,registered,initial_curve)
        mapped=quotes.map_quote_snapshot(frame['quote_raw'],frame['quote_receipt'],decision_epoch=frame['decision_epoch'],
            instruments=('GBP_USD',),maximum_quote_age_sec=5)
        candidate=adapter.candidate_for_target(consumed['curve'],consumed['publication'],consumed['consumption'],
            decision_epoch=frame['decision_epoch'],target_epoch=config['native_target_epoch'],
            quote=mapped['quotes'].get('GBP_USD'),metadata=registered['metadata']['GBP_USD'],
            expected_source_bindings=registered['source_bindings'],maximum_quote_age_sec=5,
            target_window_policy='nominal_management_boundary')
        key='curve_candidates' if candidate['status']=='available' else 'curve_refusals'
        need(frame[key]==[candidate],'original_curve_candidate_replay')
    source=retained.get('momentum_source');frame['momentum_source']=None
    if source is not None:
        raw=reader.reference(source['files']['raw'],parts,'momentum_raw.json')
        receipt=io.decode(reader.reference(source['files']['receipt'],parts,'momentum_receipt.json'))
        frame['momentum_source']=dict(raw_bytes=raw,receipt=receipt,consumption=source['consumption'])
    rebuilt=manager.plan_actions(states,frame,config,clock=lambda:plan['plan_created_epoch'])
    need(rebuilt==plan,'full_plan_replay_mismatch')
    observations=[];errors=[];quote_parts=(*parts,'quotes');quote_dir=reader.root.joinpath(*quote_parts)
    if quote_dir.exists():
        names=[]
        for index,path in enumerate(quote_dir.iterdir(),1):
            need(index<=manager.MAX_OBSERVATIONS*5,'quote_file_inventory_bound')
            names.append(path.name)
        names.sort()
        for name in names:
            if re.fullmatch(r'q\d{3}_observation.json',name):
                refs=reader.json(quote_parts,name);prefix=name.split('_')[0]
                raw=reader.reference(refs['raw'],quote_parts,prefix+'_raw.json')
                receipt=io.decode(reader.reference(refs['receipt'],quote_parts,prefix+'_receipt.json'))
                need(max(refs[k]['publication_completed_epoch'] for k in refs)<=result['as_of_epoch'],'observation_persisted_after_settlement_cutoff')
                observations.append(dict(raw_bytes=raw,receipt=receipt))
            elif re.fullmatch(r'q\d{3}_error.json',name):errors.append(reader.json(quote_parts,name))
    need(complete['quote_observation_count']==len(observations) and complete['quote_observation_errors']==errors,'observation_count_or_errors')
    rebuilt_result=manager.settle_actions(states,plan,pub,observations,as_of_epoch=result['as_of_epoch'],config=config,
        clock=lambda:result['settlement_computed_epoch'])
    need(rebuilt_result==result and result['states']==state_after,'full_settlement_or_state_replay_mismatch')
    need(complete['state_sha256']==state_after['state_sha256'] and complete['settlement_sha256']==result['settlement_sha256'],
         'step_completion_identity')
    arithmetic=independent_usd_sanity(states,plan,result)
    return state_after,dict(step_id=parts[-1],scheduled_epoch=plan['scheduled_epoch'],decision_epoch=plan['decision_epoch'],
        plan_created_epoch=plan['plan_created_epoch'],publication_completed_epoch=pub['publication_completed_epoch'],
        settlement_computed_epoch=result['settlement_computed_epoch'],completed_epoch=complete['completed_epoch'],
        terminal=plan['terminal'],cadence_lateness_sec=plan['cadence_lateness_sec'],
        selected_observation_epoch=result['selected_observation_epoch'],source_refusals=result['source_refusals'],
        candidate_refusals=plan['candidate_refusals'],
        curve_candidate_count=len(plan['prepared_candidate_evidence']['curve']),
        momentum_candidate_count=len(plan['prepared_candidate_evidence']['momentum']),
        decisions={arm:row['decision']['action'] for arm,row in result['arms'].items()},
        action_statuses={arm:row['virtual_action']['status'] for arm,row in result['arms'].items()},
        independent_arithmetic=arithmetic,state_sha256=state_after['state_sha256'])

def verify_episode(reader,episode_id,spec,registered):
    need(re.fullmatch(r'episode_0[1-3]',episode_id) is not None,'episode_identity')
    parts=('episodes',episode_id)
    retained_result=reader.json(parts,'episode_result.json') if reader.exists(parts,'episode_result.json') else None
    if not reader.exists(parts,'episode_config.json'):
        need(retained_result is None or retained_result.get('status')!='completed','claimed_completed_episode_missing_initialization')
        return dict(episode_id=episode_id,status='not_initialized' if retained_result is None else 'retained_withholding_or_failure',
            retained_episode_result=retained_result)
    if not reader.exists(parts,'initialization_publication.json'):
        need(retained_result is None or retained_result.get('status')!='completed','claimed_completed_episode_missing_initialization')
        return dict(episode_id=episode_id,status='initialization_partial',retained_episode_result=retained_result)
    config=reader.json(parts,'episode_config.json');manager.validate_config(config)
    need(config['source_bindings']==spec['source_bindings'],'episode_registry_source_closure')
    states=reader.json(parts,'initial_states.json');manager._state(states,config)
    need(states==manager.initial_states(config,clock=lambda:states['known_epoch']),'initial_state_not_empty')
    anchor=reader.json(parts,'initial_curve_consumption.json');curve=validate_consumed(anchor,registered)
    published_evidence=verify_original_published_files(anchor,registered)
    nominal=spec['episode_nominal_start_epochs'][int(episode_id[-2:])-1]
    rebuilt_config=worker.make_episode_config(spec,registered,anchor,episode_id,nominal,clock=lambda:config['created_epoch'])
    need(config==rebuilt_config,'episode_registry_schedule_or_config_mismatch')
    available=anchor['consumption']['available_epoch']
    need(curve['issued_epoch']<=available<=config['created_epoch'] and available-curve['issued_epoch']<=120
         and available+3300<=anchor['original_target_epoch']<=available+3600,'initial_anchor_original_availability')
    prepared=curve['prepared_curve'];node=next(n for n in prepared['nodes'] if n['node_id']==anchor['node_id'])
    need(node['horizon_sec']==3600 and config['curve_binding']['curve_sha256']==curve['curve_sha256']
         and config['curve_binding']['node_id']==node['node_id'],'episode_initial_anchor')
    initialized=reader.json(parts,'initialization_completed.json');initpub=reader.json(parts,'initialization_publication.json')
    reader.reference(initpub,parts,'initialization_completed.json')
    for key,name in [('config','episode_config.json'),('anchor','initial_curve_consumption.json'),
                     ('states','initial_states.json'),('schedule','decision_schedule.json')]:
        reader.reference(initialized['records'][key],parts,name)
        need(initialized['records'][key]['publication_completed_epoch']<=initialized['observed_epoch'],'initial_record_publication_order')
    need(initialized['initialized_state_sha256']==states['state_sha256']
         and initialized['original_start_epoch']==config['start_epoch']
         and initialized['observed_epoch']<=initpub['publication_started_epoch']
         <=initpub['publication_completed_epoch'],'initialization_publication_order')
    if initpub['publication_completed_epoch']>=config['start_epoch']:
        need(retained_result is None or retained_result.get('reason_code')=='initialization_not_durable_before_first_schedule',
             'late_initialization_not_withheld')
        return dict(episode_id=episode_id,status='initialization_late_withheld_or_result_pending',retained_episode_result=retained_result)
    schedule=reader.json(parts,'decision_schedule.json')['slots'];expected=[];current=config['start_epoch']
    while current<config['native_target_epoch']-1:
        expected.append(dict(scheduled_epoch=current,terminal=False));current+=60
    expected.append(dict(scheduled_epoch=config['native_target_epoch']-1,terminal=True))
    need(schedule==expected and len(schedule)<=MAX_STEPS,'predeclared_schedule_mismatch')
    complete=[];missing=[];missed=[];previous=initpub['publication_completed_epoch']
    for index,slot in enumerate(schedule):
        step_id='step_'+str(index).zfill(3);step_parts=(*parts,'steps',step_id)
        if reader.exists(step_parts,'step_completed.json'):
            states,item=verify_step(reader,step_parts,states,config,registered,curve,previous)
            need(item['scheduled_epoch']==slot['scheduled_epoch'] and item['terminal']==slot['terminal'],'step_schedule_identity')
            previous=item['completed_epoch'];complete.append(item)
        elif reader.exists(parts,'missed_'+step_id+'.json'):
            failure=reader.json(parts,'missed_'+step_id+'.json')
            need(not slot['terminal'] and failure['step_id']==step_id
                 and failure['scheduled_epoch']==slot['scheduled_epoch']
                 and failure['observed_epoch']>slot['scheduled_epoch']+10,'missed_slot_clock')
            missed.append(failure)
        else:missing.append(dict(step_id=step_id,scheduled_epoch=slot['scheduled_epoch'],terminal=slot['terminal'],
            retained_partial_directory=reader.root.joinpath(*step_parts).exists()))
    result=retained_result
    if result is not None and result['status']=='completed':
        need(not missing and result['final_state']==states and complete and complete[-1]['terminal'],'completed_episode_chain_incomplete')
    terminal=bool(complete and complete[-1]['terminal']);flat=all(s['position'] is None for s in states['arms'].values())
    status=('terminal_with_unverified_gaps' if terminal and missing else 'terminal_flat' if terminal and flat
            else 'terminal_unresolved' if terminal else 'in_progress_or_partial')
    final_metrics={arm:dict(realized_usd=s['realized_usd'],position_open=s['position'] is not None,
        latest_liquidation_usd=complete[-1]['independent_arithmetic'][arm]['liquidation_usd'] if complete else s['realized_usd'])
        for arm,s in states['arms'].items()}
    delta=None
    eligible=terminal and flat and not missing
    if eligible:
        with localcontext(Context(prec=96)):
            delta=str(Decimal(states['arms']['usd_curve_manager']['realized_usd'])-Decimal(states['arms']['usd_momentum_manager']['realized_usd']))
    return dict(episode_id=episode_id,status=status,registered_start_epoch=config['start_epoch'],
        original_published_source_evidence=published_evidence,
        native_target_epoch=config['native_target_epoch'],retained_episode_result_status=None if result is None else result['status'],
        verified_completed_steps=len(complete),uncompleted_or_unobserved_slots=missing,missed_slots=missed,
        steps=complete,final_state_sha256=states['state_sha256'],per_arm=final_metrics,
        terminal_all_five_flat=terminal and flat,completed_matched_endpoint_eligible=eligible,
        matched_terminal_curve_minus_momentum_usd=delta,
        independent_sample_size=None,broker_fills_observed=False)

def verify_all(registry_path,expected_sha):
    started=time.time();spec,registered=worker.load_spec(registry_path,expected_sha)
    reader=Reader(spec['output_root']);episodes=[];issues=[]
    for number in range(1,4):
        episode_id='episode_'+str(number).zfill(2)
        try:episodes.append(verify_episode(reader,episode_id,spec,registered))
        except (ValueError,KeyError,TypeError,OSError,StopIteration) as exc:
            issues.append(dict(episode_id=episode_id,reason_code=worker.reason(exc)))
    worker.verify_sources(spec)
    need(sha(Path(registry_path).read_bytes())==expected_sha,'registry_changed_during_verification')
    return dict(schema_version=SCHEMA,status='passed' if not issues else 'issues',
        verification_started_epoch=started,verification_completed_epoch=time.time(),
        registry_path=str(registry_path),registry_sha256=expected_sha,source_bindings=spec['source_bindings'],
        episodes=episodes,issues=issues,verified_files=list(reader.records.values()),total_read_bytes=reader.total,
        helper_sha256=sha(Path(__file__).read_bytes()),
        limitations=[
            'Passed means checked retained evidence reconciles, not profitable or fully operational.',
            'This is a bounded non-atomic observation of immutable files; unfinished and missing slots remain explicit.',
            'Replaying declared old computation clocks verifies semantics; it does not claim computations first ran during this audit.',
            'Original curve issue/publication/consumption semantics are replayed; original model fitting is not rerun.',
            'USD2500 sizing and slippage are research assumptions; no broker fill, liquidity or independent-trial claim.',
            'Current marks and realized cash on incomplete episodes are not completed matched management outcomes.'],
        runtime_writes=False,broker_actions=False,model_fits=False)

def main(argv=None):
    parser=argparse.ArgumentParser();parser.add_argument('--registry',required=True)
    parser.add_argument('--expected-sha256',required=True);parser.add_argument('--output',required=True)
    args=parser.parse_args(argv);output=Path(args.output).resolve()
    need(TRAD.resolve() not in output.parents,'verification_output_must_be_external')
    need(output.suffix=='.json' and output.parent.is_dir(),'existing_external_output_directory_required')
    report=verify_all(Path(args.registry),args.expected_sha256)
    raw=(json.dumps(report,indent=2,sort_keys=True,allow_nan=False)+'\n').encode()
    with output.open('xb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
    print(json.dumps(dict(path=str(output),sha256=sha(raw),status=report['status'],
        episodes=[dict(episode_id=e['episode_id'],status=e['status'],steps=e.get('verified_completed_steps',0)) for e in report['episodes']],issues=report['issues'])))
    return 0 if report['status']=='passed' else 1

if __name__=='__main__':raise SystemExit(main())
