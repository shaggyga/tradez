"""Predeclared, local-only virtual management episodes; never broker actions."""
import argparse
from copy import deepcopy
import hashlib
import json
import math
from pathlib import Path
import re
import time

import oanda_curve_management_adapter_v1 as adapter
import oanda_curve_management_replay_v1 as mechanics
import oanda_forecast_curve_contract_v1 as contract
import oanda_forecast_curve_file_store_v1 as files
import oanda_observed_curve_management_v1 as manager
import oanda_observed_management_io_v1 as io
import oanda_recovered_curve_pilot_v1 as pilot
import oanda_research_quote_receipt_v1 as quotes
import oanda_curve_momentum_observation_v1 as momentum

ROOT=Path(__file__).resolve().parent
SCHEMA='observed_management_worker_registry_v1_20260909'
STUDY='observed_curve_management_v1_20260909'
PILOT_REGISTRY_SHA='ae64f2cae6df44dad81b46deb16e95e4d5f67dabb7fc1e65289b96ba6b0fe2b2'
OUTPUT_ROOT=ROOT/'data/oanda_training_manager'/STUDY
NEW_SOURCES=('oanda_curve_momentum_observation_v1.py','oanda_curve_management_replay_v1.py',
    'oanda_observed_management_io_v1.py','oanda_observed_curve_management_v1.py',
    'oanda_observed_curve_management_worker_v1.py',
    'src/forex_system/research/sequential_portfolio_replay_v1.py')
LEGACY_POLICY=dict(minimum_score_cost_ratio=1.0,rotation_improvement_multiple=1.25,maximum_holding_min=60)
FLAGS={**manager.SAFETY,'orders_enabled':False,'manager_activation':False,'execution_eligible':False}


def reason(error):
    value=str(error)
    return value[:160] if re.fullmatch(r'[a-zA-Z0-9_:. -]{1,160}',value) else 'bounded_'+type(error).__name__


def verify_sources(spec):
    bindings=spec['source_bindings']
    for name,expected in bindings.items():
        if not isinstance(name,str) or not re.fullmatch(r'[a-z0-9_/]+\.py',name) or '..' in name:
            raise ValueError('management_source_path_invalid')
        if hashlib.sha256((ROOT/name).read_bytes()).hexdigest()!=expected:
            raise ValueError('management_bound_source_changed:'+name)


def load_spec(path,expected_sha):
    raw=files._read(Path(path).absolute())
    if len(raw)>contract.MAX_BYTES or hashlib.sha256(raw).hexdigest()!=expected_sha:
        raise ValueError('management_registry_bytes_mismatch')
    spec=io.decode(raw)
    if spec.get('schema_version')!=SCHEMA or spec.get('study_id')!=STUDY:
        raise ValueError('management_registry_identity')
    if any(spec.get(k) is not v for k,v in FLAGS.items()):
        raise ValueError('management_registry_authority')
    if Path(spec['output_root'])!=OUTPUT_ROOT or spec.get('pilot_registry_sha256')!=PILOT_REGISTRY_SHA:
        raise ValueError('management_registry_roots_or_pilot_binding')
    registered,_=pilot.load_registry(Path(spec['pilot_registry_path']),PILOT_REGISTRY_SHA)
    expected_names=set(registered['source_bindings'])|set(NEW_SOURCES)
    if set(spec.get('source_bindings',{}))!=expected_names:
        raise ValueError('management_registry_source_inventory')
    if any(spec['source_bindings'][k]!=v for k,v in registered['source_bindings'].items()):
        raise ValueError('management_registry_pilot_source_closure')
    starts=spec.get('episode_nominal_start_epochs')
    if (not isinstance(starts,list) or len(starts)!=3 or any(type(v) not in (int,float) or not math.isfinite(v) or v%60 for v in starts)
        or any(b-a!=3600 for a,b in zip(starts,starts[1:]))):
        raise ValueError('management_three_hourly_episodes_required')
    created=contract.epoch(spec.get('created_epoch'))
    if not created<starts[0] or not starts[-1]+3615<=spec.get('hard_stop_epoch',0)<=manager.HARD_STOP_EPOCH:
        raise ValueError('management_registry_schedule_or_hard_stop')
    if spec.get('policy')!=manager.POLICY or spec.get('momentum_policy')!=momentum.POLICY or spec.get('legacy_policy')!=LEGACY_POLICY:
        raise ValueError('management_registry_fixed_policy')
    verify_sources(spec)
    return spec,registered


def wait_until(target,stop,*,clock=time.time,sleep=time.sleep):
    while True:
        now=clock()
        if now>=stop:
            return False
        if now>=target:
            return True
        sleep(min(.5,target-now,stop-now))


def make_episode_config(spec,registered,anchor,episode_id,nominal,*,clock=time.time):
    curve=anchor['curve'];prepared=curve['prepared_curve']
    node=next(n for n in prepared['nodes'] if n['node_id']==anchor['node_id'])
    binding=dict(curve_id=curve['curve_id'],curve_sha256=curve['curve_sha256'],node_id=node['node_id'],
        model_id=node['model_id'],model_sha256=prepared['model_sha256'],forecast_cohort=prepared['forecast_cohort'],
        reference_epoch=prepared['reference_epoch'],issued_epoch=curve['issued_epoch'],
        original_target_epoch=node['original_target_epoch'],expected_terminal_price=node['expected_terminal_price'],
        source_bindings=deepcopy(prepared['source_bindings']),price_convention='official_midpoint')
    usd=dict(schema_version=mechanics.CONFIG_SCHEMA,account_currency='USD',maximum_open_positions=1,
        conversion_policy='direct_or_inverse_executable_bid_ask_no_triangulation',
        sizing_policy='fixed_usd_notional_integer_base_units_at_decision',
        curve_target_window_policy='nominal_management_boundary',notional_usd='2500',
        maximum_entry_spread_bps='5',maximum_holding_sec=3600,cadence_sec=60,execution_delay_sec=1,
        quote_max_age_sec=5,minimum_entry_cost_ratio='1',slippage_bps_per_leg='0.1',
        switch_incremental_hurdle_usd='0.05',feedback_horizon_sec=3600,
        legacy_policy=deepcopy(LEGACY_POLICY),
        metadata={'GBP_USD':{**deepcopy(registered['metadata']['GBP_USD']),'unit_increment':1}},**mechanics.SAFETY)
    config=dict(schema_version=manager.CONFIG_SCHEMA,study_id=STUDY+'/'+episode_id,created_epoch=clock(),
        start_epoch=nominal+60,native_target_epoch=node['original_target_epoch'],hard_stop_epoch=spec['hard_stop_epoch'],
        curve_binding=binding,source_bindings=deepcopy(spec['source_bindings']),usd_policy=usd,
        policy=deepcopy(manager.POLICY),momentum_policy=deepcopy(momentum.POLICY),
        scenario='2500_USD_research_notional_not_verified_account_size_or_margin',**manager.SAFETY)
    manager.validate_config(config)
    return config


def pick_anchor(registered,*,clock=time.time):
    root=Path(registered['output_root']);refusals=[]
    for cycle_id in io.recent_cycles(root):
        try:
            anchor=io.read_curve_anchor(root,cycle_id,registered['source_bindings'],registered['study_id'],clock=clock)
            return anchor,refusals
        except (ValueError,OSError,KeyError,TypeError) as error:
            refusals.append(dict(cycle_id=cycle_id,reason_code=reason(error)))
    return None,refusals


def latest_momentum_source(registered,*,clock=time.time):
    root=Path(registered['output_root']);refusals=[]
    for cycle_id in io.recent_cycles(root):
        try:
            observed=io.read_momentum_source(root,cycle_id,registered['metadata']['GBP_USD'],clock=clock)
            return observed,refusals
        except (ValueError,OSError,KeyError,TypeError) as error:
            refusals.append(dict(cycle_id=cycle_id,reason_code=reason(error)))
    return None,refusals


def persist_capture(root,parts,prefix,capture,*,clock=time.time):
    raw_ref=io.persist_bytes(root,parts,prefix+'_raw.json',capture['raw_bytes'],clock=clock)
    receipt_ref=io.persist_record(root,parts,prefix+'_receipt.json',capture['receipt'],clock=clock)
    return dict(raw=raw_ref,receipt=receipt_ref)


def run_step(spec,registered,anchor,config,states,episode_id,step_id,scheduled,terminal,*,clock=time.time,sleep=time.sleep):
    root=Path(spec['output_root']);parts=('episodes',episode_id,'steps',step_id)
    verify_sources(spec)
    consumed=None;source=None;source_refusals=[]
    if not terminal:
        consumed=files.consume_published_curve(Path(registered['output_root'])/'published',anchor['descriptor'],
            expected_source_bindings=registered['source_bindings'],persist_consumption=False,clock=clock)
        source,source_refusals=latest_momentum_source(registered,clock=clock)
    quote_failure=None
    try:
        quote_capture=quotes.capture_quote_snapshot(clock=clock)
    except (ValueError,OSError) as error:
        if not terminal:
            raise
        quote_capture=None;quote_failure=reason(error)
    cutoff=clock()
    frame=dict(scheduled_epoch=scheduled,decision_epoch=cutoff,terminal=terminal,
        quote_raw=None if quote_capture is None else quote_capture['raw_bytes'],
        quote_receipt=None if quote_capture is None else quote_capture['receipt'],
        curve_candidates=[],curve_refusals=[],momentum_refusals=source_refusals,
        momentum_source=None if source is None else dict(raw_bytes=source['raw_bytes'],receipt=source['receipt'],consumption=source['source_consumption']))
    if quote_failure is not None:
        frame['decision_quote_refusal']={'reason_code':quote_failure}
    if consumed is not None:
        mapped=quotes.map_quote_snapshot(quote_capture['raw_bytes'],quote_capture['receipt'],decision_epoch=cutoff,
            instruments=('GBP_USD',),maximum_quote_age_sec=5)
        candidate=adapter.candidate_for_target(consumed['curve'],consumed['publication'],consumed['consumption'],
            decision_epoch=cutoff,target_epoch=config['native_target_epoch'],quote=mapped['quotes'].get('GBP_USD'),
            metadata=registered['metadata']['GBP_USD'],expected_source_bindings=registered['source_bindings'],
            maximum_quote_age_sec=5,target_window_policy='nominal_management_boundary')
        frame['curve_candidates' if candidate['status']=='available' else 'curve_refusals'].append(candidate)
    retained={k:deepcopy(v) for k,v in frame.items() if k not in ('quote_raw','momentum_source')}
    retained['quote_capture_files']=None if quote_capture is None else persist_capture(root,parts,'decision_quote',quote_capture,clock=clock)
    if consumed is not None:
        retained['curve_consumption_file']=io.persist_record(root,parts,'curve_consumption.json',consumed,clock=clock)
    retained['momentum_source']=None
    if source is not None:
        source_files=persist_capture(root,parts,'momentum',source,clock=clock)
        retained['momentum_source']=dict(cycle_id=source['cycle_id'],files=source_files,
            consumption=source['source_consumption'],source_mapping_sha256=source['source_mapping_sha256'])
    io.persist_record(root,parts,'information_frame.json',retained,clock=clock)
    plan=manager.plan_actions(states,frame,config,clock=clock)
    verify_sources(spec)
    write=io.persist_record(root,parts,'plan.json',plan,clock=clock)
    publication=manager.make_plan_publication(plan,started_epoch=write['publication_started_epoch'],completed_epoch=write['publication_completed_epoch'])
    io.persist_record(root,parts,'plan_publication.json',publication,clock=clock)
    observations=[];observation_errors=[]
    result=manager.settle_actions(states,plan,publication,observations,as_of_epoch=clock(),config=config,clock=clock)
    io.persist_record(root,parts,'initial_settlement_status.json',result,clock=clock)
    attempt=0
    while result['status']=='pending_quote':
        due=result['observation_not_before_epoch'];deadline=result['observation_deadline_epoch']
        next_poll=max(due,clock()) if attempt==0 else min(clock()+1,deadline+.01)
        if not wait_until(next_poll,spec['hard_stop_epoch'],clock=clock,sleep=sleep):
            raise ValueError('management_hard_stop_before_settlement')
        if clock()<=deadline:
            try:
                observation=quotes.capture_quote_snapshot(clock=clock)
                refs=persist_capture(root,(*parts,'quotes'),'q'+str(attempt).zfill(3),observation,clock=clock)
                observations.append(observation)
                io.persist_record(root,(*parts,'quotes'),'q'+str(attempt).zfill(3)+'_observation.json',refs,clock=clock)
            except (ValueError,OSError) as error:
                event=dict(attempt=attempt,observed_epoch=clock(),reason_code=reason(error),**FLAGS)
                observation_errors.append(event)
                io.persist_record(root,(*parts,'quotes'),'q'+str(attempt).zfill(3)+'_error.json',event,clock=clock)
        attempt+=1
        if attempt>manager.MAX_OBSERVATIONS:
            raise ValueError('management_quote_poll_bound')
        result=manager.settle_actions(states,plan,publication,observations,as_of_epoch=clock(),config=config,clock=clock)
    published=io.persist_record(root,parts,'settlement.json',result,clock=clock)
    io.persist_record(root,parts,'settlement_publication.json',published,clock=clock)
    io.persist_record(root,parts,'state_after.json',result['states'],clock=clock)
    io.persist_record(root,parts,'step_completed.json',dict(completed_epoch=clock(),
        settlement_sha256=result['settlement_sha256'],state_sha256=result['states']['state_sha256'],
        terminal=terminal,quote_observation_count=len(observations),quote_observation_errors=observation_errors,**FLAGS),clock=clock)
    print(json.dumps(dict(episode_id=episode_id,step_id=step_id,status='completed',terminal=terminal,
        actions={arm:row['decision']['action'] for arm,row in result['arms'].items()},
        virtual_positions=sum(row['state_after']['position'] is not None for row in result['arms'].values()),
        actual_broker_actions=0,completed_epoch=clock())),flush=True)
    return result['states'],result


def run_episode(spec,registered,episode_id,nominal,*,clock=time.time,sleep=time.sleep):
    root=Path(spec['output_root']);parts=('episodes',episode_id)
    if not wait_until(nominal,spec['hard_stop_epoch'],clock=clock,sleep=sleep):
        return {'status':'hard_stop_before_episode'}
    if clock()>nominal+30:
        return {'status':'withheld','reason_code':'episode_initialization_schedule_missed'}
    verify_sources(spec)
    anchor,refusals=pick_anchor(registered,clock=clock)
    io.persist_record(root,parts,'anchor_selection.json',dict(observed_epoch=clock(),
        selection='most_recent_available_cycle_fixed_GBP_USD_official_M_admitted_native_H1',
        selected_cycle=None if anchor is None else anchor['cycle_id'],refusals=refusals,**FLAGS),clock=clock)
    if anchor is None:
        return {'status':'withheld','reason_code':'no_eligible_initial_curve'}
    config=make_episode_config(spec,registered,anchor,episode_id,nominal,clock=clock)
    initialization={}
    initialization['config']=io.persist_record(root,parts,'episode_config.json',config,clock=clock)
    initialization['anchor']=io.persist_record(root,parts,'initial_curve_consumption.json',anchor,clock=clock)
    states=manager.initial_states(config,clock=clock)
    initialization['states']=io.persist_record(root,parts,'initial_states.json',states,clock=clock)
    schedule=[];next_epoch=config['start_epoch'];terminal_epoch=config['native_target_epoch']-1
    while next_epoch<terminal_epoch:
        schedule.append((next_epoch,False));next_epoch+=60
    schedule.append((terminal_epoch,True))
    initialization['schedule']=io.persist_record(root,parts,'decision_schedule.json',dict(slots=[dict(scheduled_epoch=t,terminal=end) for t,end in schedule],**FLAGS),clock=clock)
    initialized=io.persist_record(root,parts,'initialization_completed.json',dict(records=initialization,
        initialized_state_sha256=states['state_sha256'],original_start_epoch=config['start_epoch'],
        observed_epoch=clock(),**FLAGS),clock=clock)
    io.persist_record(root,parts,'initialization_publication.json',initialized,clock=clock)
    if clock()>=config['start_epoch']:
        return dict(status='withheld',reason_code='initialization_not_durable_before_first_schedule',
            initial_state=states,actual_broker_actions=0)
    last_result=None;failures=[]
    for index,(scheduled,terminal) in enumerate(schedule):
        if not wait_until(scheduled,spec['hard_stop_epoch'],clock=clock,sleep=sleep):
            break
        step_id='step_'+str(index).zfill(3)
        if not terminal and clock()>scheduled+10:
            failure=dict(step_id=step_id,scheduled_epoch=scheduled,observed_epoch=clock(),
                reason_code='decision_schedule_slot_missed',**FLAGS)
            failures.append(failure);io.persist_record(root,parts,'missed_'+step_id+'.json',failure,clock=clock)
            continue
        try:
            states,last_result=run_step(spec,registered,anchor,config,states,episode_id,step_id,scheduled,terminal,clock=clock,sleep=sleep)
        except Exception as error:
            # Fail the episode rather than guessing whether an interrupted
            # publication/settlement had completed. Retain its last state.
            failure=dict(step_id=step_id,observed_epoch=clock(),reason_code=reason(error),**FLAGS)
            io.persist_record(root,parts,'episode_failure.json',failure,clock=clock)
            return dict(status='failed_requires_evidence_review',failure=failure,
                last_completed_state=states,unresolved_virtual_positions=sum(s['position'] is not None for s in states['arms'].values()))
    return dict(status='completed' if last_result is not None and last_result['terminal'] else 'incomplete',
        final_state=states,terminal_all_flat=last_result.get('terminal_all_flat') if last_result else None,
        original_target_epoch=config['native_target_epoch'],missed_slots=failures,
        realized_usd_by_arm={arm:s['realized_usd'] for arm,s in states['arms'].items()},
        independent_sample_size=None,actual_broker_actions=0)


def main(argv=None):
    parser=argparse.ArgumentParser()
    parser.add_argument('--registry',required=True);parser.add_argument('--expected-sha256',required=True)
    parser.add_argument('--validate-only',action='store_true')
    args=parser.parse_args(argv)
    spec,registered=load_spec(args.registry,args.expected_sha256)
    if args.validate_only:
        print(json.dumps(dict(status='registry_valid',source_count=len(spec['source_bindings']),actual_broker_actions=0)));return 0
    root=Path(spec['output_root'])
    with pilot.worker_lock(root):
        io.persist_record(root,(),'worker_started.json',dict(started_epoch=time.time(),registry_sha256=args.expected_sha256,**FLAGS))
        outcomes=[]
        for number,nominal in enumerate(spec['episode_nominal_start_epochs'],1):
            episode_id='episode_'+str(number).zfill(2)
            result=run_episode(spec,registered,episode_id,nominal)
            io.persist_record(root,('episodes',episode_id),'episode_result.json',{**result,'completed_epoch':time.time(),**FLAGS})
            outcomes.append(dict(episode_id=episode_id,status=result['status']))
            print(json.dumps(dict(episode_id=episode_id,status=result['status'],actual_broker_actions=0)),flush=True)
        io.persist_record(root,(),'worker_completed.json',dict(completed_epoch=time.time(),episodes=outcomes,**FLAGS))
    return 0


if __name__=='__main__':
    raise SystemExit(main())
