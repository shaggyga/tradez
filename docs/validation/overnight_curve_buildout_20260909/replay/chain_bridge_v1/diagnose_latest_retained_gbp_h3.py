"""One read-only retained H3/actual local quote compatibility diagnostic.

Only new external evidence/hypothetical flat state is written. No GET, account
state, issuer, study, manager, broker action or outcome/PnL calculation.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import time

HERE=Path(__file__).resolve().parent
ROOT=HERE.parents[2]/'trad'
OUTPUT=HERE/'actual_retained_compatibility_001'
REGISTRY=ROOT/'config/recovered_second_curve_pilot_v1_20260909.json'
REGISTRY_SHA='ae64f2cae6df44dad81b46deb16e95e4d5f67dabb7fc1e65289b96ba6b0fe2b2'
BRIDGE_SHA='e874d7fd633a00c7835ec7698eab3903d9b863a0bf98b7f884a0b6de54e42fa7'
PAPER_SHA='60689101fb63a8bcce2465d19f627b9b2ce357d1ee271c65298f255663d991e1'
MAX_CYCLES=600
MAX_SECONDS=90
MAX_CAPTURE_BYTES=8*1024*1024
STARTED=time.time()
INPUT_READS=[]

def need(ok,code):
    if not ok:raise ValueError(code)

def digest(raw):return hashlib.sha256(raw).hexdigest()

def write_new(name,raw):
    need(type(raw) is bytes and 0<len(raw)<=MAX_CAPTURE_BYTES,'external_record_bound')
    path=OUTPUT/name;start=time.time()
    with path.open('xb') as handle:handle.write(raw);handle.flush();os.fsync(handle.fileno())
    end=time.time()
    need(path.read_bytes()==raw,'external_write_readback')
    return dict(path=str(path),sha256=digest(raw),bytes=len(raw),write_started_epoch=start,write_completed_epoch=end)

def raw_read(path,maximum=2*1024*1024):
    need(time.time()-STARTED<=MAX_SECONDS,'diagnostic_wall_budget')
    started=time.time()
    # Reuse frozen path/descriptor primitive after its source closure is pinned.
    files._safe_components(path,require_file=True)
    before=path.stat()
    need(0<before.st_size<=maximum,'read_byte_bound')
    with path.open('rb') as handle:raw=handle.read(maximum+1);opened=os.fstat(handle.fileno())
    completed=time.time();after=path.stat()
    signature=lambda s:(s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns)
    need(signature(before)==signature(opened)==signature(after) and len(raw)==after.st_size,'source_changed_during_read')
    evidence=dict(path=str(path),raw_sha256=digest(raw),bytes=len(raw),read_started_epoch=started,read_completed_epoch=completed)
    INPUT_READS.append(evidence)
    return raw,evidence

def json_read(path,private_name=None):
    raw,evidence=raw_read(path);value=files._decode(raw)
    if private_name:write_new(private_name,raw)
    return value,{**{k:evidence[k] for k in ('read_started_epoch','read_completed_epoch')},'payload_sha256':contract.content_hash(value)},evidence

def check_sources(bindings):
    return {name:digest((ROOT/name).read_bytes()) for name in bindings}

def main():
    global files,contract
    need(not OUTPUT.exists(),'fresh_output_required')
    need(digest(REGISTRY.read_bytes())==REGISTRY_SHA,'pilot_registry_pin')
    registry=json.loads(REGISTRY.read_bytes());pins=dict(registry['source_bindings'])
    need(check_sources(pins)==pins,'pilot_source_pin')
    need(digest((ROOT/'oanda_curve_chain_management_bridge_v1.py').read_bytes())==BRIDGE_SHA,'bridge_source_pin')
    sys.path.insert(0,str(ROOT))
    import oanda_forecast_curve_file_store_v1 as files
    import oanda_forecast_curve_contract_v1 as contract
    import oanda_curve_chain_management_bridge_v1 as bridge
    import oanda_research_quote_receipt_v1 as quote_module
    import oanda_s5_mba_research_capture_v1 as candles
    import oanda_recovered_second_curve_v1 as recovered
    import oanda_recovered_curve_bridge_v1 as recovery_bridge
    pins.update(bridge.REUSED_BINDINGS);pins[bridge.SOURCE_NAME]=BRIDGE_SHA
    need(check_sources(pins)==pins,'all_dependency_pins')
    OUTPUT.mkdir()
    outcome=dict(schema_version='retained_curve_chain_bridge_diagnostic_v1_20260909',started_epoch=STARTED,
        deterministic_selection='latest cycle name with retained complete GBP_USD official_midpoint issued_observation; fixed original H3 native node; no return/side/target search',
        maximum_cycle_entries=MAX_CYCLES,maximum_seconds=MAX_SECONDS,**bridge.mechanics.SAFETY,
        diagnostic_only=True,manager_activation=False,account_state_used=False,broker_requests=0,
        new_forecasts_created=0,original_publications_changed=False,positions_or_fills_created=False,pnl_computed=False)
    try:
        registry,registry_read,registry_raw=json_read(REGISTRY,'pilot_registry.json')
        need(registry_raw['raw_sha256']==REGISTRY_SHA,'registry_changed_after_import')
        registry_read['payload_sha256']=REGISTRY_SHA
        pilot_root=Path(registry['output_root']);need(pilot_root==ROOT/'data/oanda_training_manager/recovered_second_curve_pilot_v1_20260909','pilot_root')
        cycles=files._directory(pilot_root,('cycles',));names=[];count=0
        for path in cycles.iterdir():
            count+=1;need(count<=MAX_CYCLES,'cycle_metadata_cap')
            if re.fullmatch(r'cycle_[0-9]{16,25}',path.name):names.append(path.name)
        names.sort(key=lambda name:int(name.split('_')[1]),reverse=True)
        found=None;examined=0
        for name in names:
            examined+=1;candidate=cycles/name/'gbp_usd/official_midpoint/issued_observation.json'
            if candidate.exists():found=candidate;break
        need(found is not None,'no_retained_gbp_official_issue')
        issue,issue_read,_=json_read(found,'original_issue_observation.json')
        need(issue.get('status')=='issued_and_consumed' and all(issue.get(k) is True for k in
            ('registry_issue_admitted','publication_completed','consumption_completed')),'latest_issue_not_complete')
        descriptor=files._validate_descriptor(issue['publication']['descriptor'])
        directory=files._directory(pilot_root/'published',('curves',descriptor['curve_sha256']))
        curve,curve_read,curve_file=json_read(directory/'curve.json','original_curve.json')
        publication,pub_read,pub_file=json_read(directory/'publication.json','original_publication.json')
        need(curve_file['raw_sha256']==descriptor['persisted_curve_bytes_sha256'] and pub_file['raw_sha256']==descriptor['publication_bytes_sha256'],'descriptor_actual_byte_binding')
        consumption,consumption_read,_=json_read(pilot_root/'published/consumptions'/(issue['consumption']['consumption_sha256']+'.json'),'original_consumption.json')
        need(consumption==issue['consumption'],'original_consumption_copy_mismatch')
        contract.validate_consumption(curve,publication,consumption,expected_source_bindings=registry['source_bindings'])
        prepared=curve['prepared_curve'];pair='GBP_USD'
        need(prepared['instrument']==pair and prepared['forecast_cohort']==registry['study_id']+'/GBP_USD/official_midpoint','cohort_identity')
        nodes=[n for n in prepared['nodes'] if n['horizon_sec']==10800]
        need(len(nodes)==1 and nodes[0]['status']=='forecast','fixed_native_h3_unavailable')
        node=nodes[0];target=node['original_target_epoch']
        outcome.update(cycle_id=found.parents[2].name,metadata_entries=count,cycle_names_examined=examined,
            instrument=pair,price_convention='official_midpoint',original_curve_sha256=curve['curve_sha256'],
            original_reference_epoch=prepared['reference_epoch'],original_issued_epoch=curve['issued_epoch'],
            original_horizon_sec=10800,original_target_epoch=target,node_id=node['node_id'])
        need(time.time()<target,'fixed_h3_target_elapsed_before_diagnostic')
        # Reconstruct only the retained original input/computation, at its recorded clocks.
        parts=found.parent
        model_input,_,mi_file=json_read(parts/'model_input.json','original_model_input.json')
        model_result,_,mr_file=json_read(parts/'computed_result.json','original_computed_result.json')
        need(mi_file['raw_sha256']==issue['model_input']['sha256'] and mr_file['raw_sha256']==issue['computed_result']['sha256'],'original_computation_file_binding')
        raw,raw_evidence=raw_read(parts.parent/'source_raw.json');write_new('original_s5_raw.json',raw)
        source_receipt,_,_=json_read(parts.parent/'capture_receipt.json','original_s5_capture_receipt.json')
        mapped=candles.map_verified_capture(raw,source_receipt,registry['metadata'][pair],'official_midpoint')
        rebuilt_input=recovered.capture_s5_rows(mapped['last13_feature_rows'],instrument=pair,
            pip_size=float(registry['metadata'][pair]['pip_size']),source_sha256=mapped['source_sha256'],
            scope='current_research',sampling_policy=registry['sampling_policy'],price_convention='official_midpoint',
            clock=lambda:model_input['first_observed_epoch'])
        need(rebuilt_input==model_input,'original_input_source_replay_mismatch')
        model_read_started=time.time();model=recovered.load_model(registry['model_path']);model_read_completed=time.time()
        need(model.artifact_sha256==registry['model_sha256'],'model_registry_identity')
        reconstructed=recovery_bridge.prepare_recovered_computation(model_input,model_result,model,
            source_bindings=registry['source_bindings'],forecast_cohort=prepared['forecast_cohort'],policy=registry['curve_policy'])
        need(reconstructed==prepared,'original_model_and_prepared_replay_mismatch')
        paper,_,paper_file=json_read(ROOT/'config/observed_curve_management_v1_20260909.json')
        need(paper_file['raw_sha256']==PAPER_SHA,'paper_registry_pin')
        episode,_,episode_file=json_read(Path(paper['output_root'])/'episodes/episode_01/episode_config.json')
        config=episode['usd_policy'];need(config['notional_usd']=='2500' and config['maximum_holding_sec']==3600,'paper_policy_reuse_identity')
        models={str(cell.horizon):cell.model_id for cell in model.cells if cell.instrument==pair}
        expected=dict(forecast_cohort=registry['study_id']+'/GBP_USD/official_midpoint',model_sha256=registry['model_sha256'],
            feature_version=recovered.FEATURE_VERSION,source_bindings=registry['source_bindings'],policy_sha256=registry['curve_policy']['policy_sha256'],
            model_ids_by_horizon=models,price_convention='official_midpoint',reference_price_kind='official_midpoint_S5_close',
            bar_duration_sec=5,target_selection_policy={'kind':'first_complete_bar_at_or_after_nominal','maximum_delay_sec':7})
        trust=dict(schema_version=bridge.TRUST_SCHEMA,registry_sha256=REGISTRY_SHA,usd_config_sha256=bridge.mechanics.digest(config),
            bridge_source_bindings={**bridge.REUSED_BINDINGS,bridge.SOURCE_NAME:BRIDGE_SHA},pairs={pair:expected},**bridge.mechanics.SAFETY)
        # This newly constructed state is explicitly hypothetical, never an account read.
        state={'position':None,'realized_usd':'0'};known=time.time()
        state_write=write_new('hypothetical_flat_state.json',contract.canonical_bytes(state))
        state,state_read,_=json_read(OUTPUT/'hypothetical_flat_state.json')
        captured=quote_module.capture_quote_snapshot()
        write_new('current_quote_raw.json',captured['raw_bytes']);write_new('current_quote_capture_receipt.json',contract.canonical_bytes(captured['receipt']))
        mapping=quote_module.map_quote_snapshot(captured['raw_bytes'],captured['receipt'],decision_epoch=time.time(),
            instruments=(pair,),maximum_quote_age_sec=5)
        write_new('current_quote_mapping.json',contract.canonical_bytes(mapping))
        write_new('derived_quote_inventory.json',contract.canonical_bytes(mapping['quotes']))
        quote_inventory,quotes_read,_=json_read(OUTPUT/'derived_quote_inventory.json')
        observation=dict(schema_version=bridge.OBSERVATION_SCHEMA,registry_read=registry_read,
            state_known_epoch=known,state_read=state_read,quotes_read=quotes_read,
            chain_reads={pair:{'curve':curve_read,'publication':pub_read,'consumption':consumption_read}})
        decision=time.time()
        need(decision<target,'fixed_h3_target_elapsed_before_decision')
        arguments=dict(decision_epoch=decision,management_target_epoch=target,usd_config=config,
            trusted_context=trust,observation_context=observation)
        chain={'curve':curve,'publication':publication,'consumption':consumption,'management_target_epoch':target}
        result=bridge.select_from_curve_chains([chain],quote_inventory,state,**arguments)
        computed=time.time()
        write_new('bridge_inputs.json',json.dumps({'chains':[chain],'quotes':quote_inventory,'state':state,'arguments':arguments},sort_keys=True,separators=(',',':'),allow_nan=False).encode())
        result_file=write_new('bridge_result.json',json.dumps(result,sort_keys=True,separators=(',',':'),allow_nan=False).encode())
        selected=result['selection'];row=result['evidence'][0];candidate=row['admission'].get('candidate') or {}
        outcome.update(status='diagnostic_completed',decision_epoch=decision,diagnostic_computed_epoch=computed,
            actual_quote_mapping_status=mapping['status'],actual_quote_refusals=mapping['refusals'],
            candidate_status=candidate.get('status'),candidate_side=candidate.get('side'),
            expected_terminal_price=candidate.get('expected_terminal_price'),expected_remaining_move_pips=candidate.get('expected_remaining_move_pips'),
            semantic_entry_admitted=row['admission']['new_entry']['semantic_admitted'],semantic_reason=row['admission']['reason_code'],
            prepared_entry_count=selected['entry_candidate_count'],prepared_continuation_count=selected['continuation_candidate_count'],
            diagnostic_decision=selected['decision'],refusals=result['refusals'],
            source_raw_and_original_inference_replayed=True,original_model_read=dict(path=registry['model_path'],sha256=model.artifact_sha256,
                read_started_epoch=model_read_started,read_completed_epoch=model_read_completed),
            reused_paper_policy_source=episode_file,hypothetical_state=dict(kind='new_hypothetical_flat_research_state_not_account_or_existing_paper_state',
                known_epoch=known,write=state_write,actual_read=state_read),
            actual_quote_read=captured['receipt'],derived_quote_read=quotes_read,bridge_result=result_file,
            target_window_policy=registry['target_window_policy'],training_target_maximum_delay_sec=7,
            target_remains_nominal_approximation=True,current_forecast_update_claim=False)
    except Exception as error:
        outcome.update(status='explicit_refusal_or_incomplete_diagnostic',reason_code=str(error)[:200] if isinstance(error,ValueError) else type(error).__name__)
    finally:
        outcome.update(completed_epoch=time.time(),input_file_reads=INPUT_READS,
            source_bindings=pins,source_bindings_unchanged=check_sources(pins)==pins,
            registry_unchanged=digest(REGISTRY.read_bytes())==REGISTRY_SHA,
            diagnostic_helper_sha256=digest(Path(__file__).read_bytes()),
            limitation='One actual retained-input/current-quote compatibility observation with newly hypothetical flat state; no account state, new issue, fresh predictor update, fill, manager session or PnL.')
        record=write_new('ACTUAL_RETAINED_H3_BRIDGE_DIAGNOSTIC_20260909.json',json.dumps(outcome,indent=2,sort_keys=True,allow_nan=False).encode())
        print(json.dumps({'status':outcome['status'],'reason_code':outcome.get('reason_code'),'decision':outcome.get('diagnostic_decision'),
            'target':outcome.get('original_target_epoch'),'receipt':record}))

if __name__=='__main__':main()
