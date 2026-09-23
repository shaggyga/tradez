"""Bounded prospective research collector; incapable of placing broker orders.

Only the dedicated practice-candle GET helper accesses OANDA. Current quote
evidence is read from the existing local research stream. This worker issues
separate research curves and decision observations, never manager instructions.
Every cycle and failed attempt is retained. The registered cutoff is mandatory.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sys
import time

import oanda_forecast_curve_contract_v1 as contract
import oanda_forecast_curve_file_store_v1 as files
import oanda_recovered_second_curve_v1 as recovered
from oanda_recovered_curve_bridge_v1 import prepare_recovered_computation
from oanda_curve_management_adapter_v1 import candidate_for_target
import oanda_s5_mba_research_capture_v1 as candles
import oanda_research_quote_receipt_v1 as quotes
import oanda_native_curve_outcomes_v1 as outcomes

ROOT = Path(__file__).resolve().parent
SCHEMA = 'recovered_second_curve_prospective_registry_v1_20260909'
STUDY_ID = 'recovered_second_curve_pilot_v1_20260909'
INSTRUMENTS = ('EUR_USD', 'GBP_USD', 'USD_JPY')
CONVENTIONS = ('official_midpoint', 'ba_derived_midpoint')
SOURCE_FILES = (
    'oanda_second_forecast.py', 'oanda_second_forecast_fit.py',
    'oanda_recovered_second_curve_v1.py', 'oanda_forecast_curve_contract_v1.py',
    'oanda_recovered_curve_bridge_v1.py', 'oanda_forecast_curve_file_store_v1.py',
    'oanda_curve_management_adapter_v1.py', 'oanda_s5_mba_research_capture_v1.py',
    'oanda_research_quote_receipt_v1.py', 'oanda_native_curve_outcomes_v1.py',
    'oanda_recovered_curve_pilot_v1.py', 'oanda_live_account_readonly_status.py',
    'oanda_practice_quote_stream.py', 'oanda_practice_shadow_strategy_lab.py',
)


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def source_bindings():
    return {name:_sha((ROOT/name).read_bytes()) for name in SOURCE_FILES}


def verify_sources(registry):
    if registry['source_bindings'] != source_bindings():
        raise contract.CurveContractError('pilot_registered_source_changed')


def _utc(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat()


def _reason(error):
    text = str(error)
    return text if re.fullmatch(r'[a-z][a-z0-9_]{1,100}', text) else 'operation_failed'


def _json(raw):
    def pairs(items):
        result = {}
        for key,value in items:
            if key in result:
                raise contract.CurveContractError('pilot_duplicate_json_key')
            result[key] = value
        return result
    value = json.loads(raw, object_pairs_hook=pairs,
        parse_constant=lambda _: (_ for _ in ()).throw(contract.CurveContractError('pilot_nonfinite_json')))
    contract.canonical_bytes(value)
    return value


def load_registry(path, expected_sha256):
    raw = files._read(Path(path).absolute())
    if _sha(raw) != expected_sha256:
        raise contract.CurveContractError('pilot_registry_bytes_mismatch')
    registry = _json(raw)
    if registry.get('schema_version') != SCHEMA or registry.get('study_id') != STUDY_ID:
        raise contract.CurveContractError('pilot_registry_identity')
    if any(registry.get(k) is not v for k,v in contract.AUTHORITY.items()):
        raise contract.CurveContractError('pilot_registry_authority')
    if registry.get('orders_enabled') is not False or registry.get('manager_activation') is not False:
        raise contract.CurveContractError('pilot_must_be_order_incapable')
    if registry.get('instruments') != list(INSTRUMENTS) or registry.get('price_conventions') != list(CONVENTIONS):
        raise contract.CurveContractError('pilot_inventory_changed')
    if registry.get('sampling_policy') != 'retained_fit_window' or registry.get('cycle_sec') != 60:
        raise contract.CurveContractError('pilot_sampling_or_cadence')
    if registry.get('target_window_policy') != 'nominal_management_boundary':
        raise contract.CurveContractError('pilot_explicit_target_approximation_required')
    created, cutoff, stop = map(contract.epoch, (
        registry.get('created_epoch'), registry.get('issue_cutoff_epoch'), registry.get('collection_stop_epoch')))
    if not created < cutoff < stop or stop-created > 9*3600 or stop-cutoff < 14400+180:
        raise contract.CurveContractError('pilot_stop_or_maturity_window_invalid')
    if registry.get('request_start_guard_sec') != 60:
        raise contract.CurveContractError('pilot_request_deadline_guard_invalid')
    expected_stop = datetime(2026,9,9,13,tzinfo=timezone.utc).timestamp()
    if stop != expected_stop:
        raise contract.CurveContractError('pilot_user_deadline_changed')
    expected_output = ROOT/'data/oanda_training_manager'/STUDY_ID
    if Path(registry.get('output_root','')) != expected_output:
        raise contract.CurveContractError('pilot_output_root_mismatch')
    if registry.get('model_sha256') != recovered.ARTIFACT_SHA256:
        raise contract.CurveContractError('pilot_model_identity')
    contract.validate_policy(registry['curve_policy'])
    if registry['curve_policy']['native_horizons_sec'] != list(recovered.HORIZONS):
        raise contract.CurveContractError('pilot_native_inventory')
    for instrument in INSTRUMENTS:
        meta = registry['metadata'][instrument]
        expected_pip = '0.01' if instrument == 'USD_JPY' else '0.0001'
        if meta != dict(instrument=instrument, base_currency=instrument[:3], quote_currency=instrument[4:], pip_size=expected_pip):
            raise contract.CurveContractError('pilot_instrument_metadata')
    verify_sources(registry)
    return registry, _sha(raw)


def write_record(root, parts, name, value):
    if not re.fullmatch(r'[a-z0-9_]{1,160}\.json', name):
        raise contract.CurveContractError('pilot_record_name')
    directory = files._directory(root, parts, create=True)
    raw = contract.canonical_bytes(value)
    path = directory/name
    created = files._write_exclusive(path,raw)
    if not created and files._read(path) != raw:
        raise contract.CurveContractError('pilot_existing_record_conflict')
    return dict(relative_path=str(path.relative_to(root)).replace('\\','/'), sha256=_sha(raw), bytes=len(raw))


def write_raw(root, parts, raw):
    directory = files._directory(root,parts,create=True)
    path = directory/'source_raw.json'
    created = files._write_exclusive(path,raw)
    if not created and files._read(path) != raw:
        raise contract.CurveContractError('pilot_existing_raw_conflict')
    return dict(relative_path=str(path.relative_to(root)).replace('\\','/'),sha256=_sha(raw),bytes=len(raw))


@contextmanager
def worker_lock(root):
    """OS lock is released on exit; persistent lock file is never deleted."""
    import msvcrt
    directory = files._directory(root,create=True)
    path = directory/'worker_lock.bin'
    try:
        with path.open('xb') as initial:
            initial.write(b'0'); initial.flush(); os.fsync(initial.fileno())
    except FileExistsError:
        pass
    files._safe_components(path,require_file=True)
    with path.open('r+b') as handle:
        try:
            msvcrt.locking(handle.fileno(),msvcrt.LK_NBLCK,1)
        except OSError:
            raise contract.CurveContractError('pilot_worker_already_locked') from None
        try:
            yield
        finally:
            handle.seek(0); msvcrt.locking(handle.fileno(),msvcrt.LK_UNLCK,1)


def attempt_variant(registry, model, mapping, convention, cycle_parts, *, clock=time.time):
    root = Path(registry['output_root'])
    instrument = mapping['instrument']
    parts = (*cycle_parts,convention)
    started = clock()
    attempt = dict(instrument=instrument,price_convention=convention,started_epoch=started,
        status='failed', source_receipt_sha256=mapping['source_receipt_sha256'],
        logical_issue_created=False,registry_issue_admitted=False,
        publication_completed=False,consumption_completed=False,**contract.AUTHORITY)
    write_record(root,parts,'attempt_started.json',attempt)
    try:
        if started >= registry['issue_cutoff_epoch']:
            raise contract.CurveContractError('pilot_issue_cutoff_reached')
        rows = mapping['last13_feature_rows']
        capture = recovered.capture_s5_rows(rows,instrument=instrument,
            pip_size=float(registry['metadata'][instrument]['pip_size']),
            source_sha256=mapping['source_sha256'],scope='current_research',
            sampling_policy=registry['sampling_policy'],price_convention=convention,clock=clock)
        capture_ref = write_record(root,parts,'model_input.json',capture)
        result = recovered.predict_curve(capture,model,clock=clock)
        result_ref = write_record(root,parts,'computed_result.json',result)
        attempt.update(model_input=capture_ref,computed_result=result_ref)
        verify_sources(registry)
        prepared = prepare_recovered_computation(capture,result,model,
            source_bindings=registry['source_bindings'],
            forecast_cohort=registry['study_id']+'/'+instrument+'/'+convention,
            policy=registry['curve_policy'])
        if clock() >= registry['issue_cutoff_epoch']:
            raise contract.CurveContractError('pilot_issue_cutoff_reached')
        curve = contract.issue_curve(prepared,expected_source_bindings=registry['source_bindings'],clock=clock)
        attempt.update(logical_issue_created=True,curve_id=curve['curve_id'],curve_sha256=curve['curve_sha256'],
            native_node_count=len(prepared['nodes']),reference_epoch=prepared['reference_epoch'],
            issued_epoch=curve['issued_epoch'])
        if curve['issued_epoch'] >= registry['issue_cutoff_epoch']:
            raise contract.CurveContractError('pilot_actual_issue_clock_past_cutoff')
        attempt.update(registry_issue_admitted=True,status='issued_not_published')
        write_record(root,parts,'issue_stage.json',attempt)
        publication = files.publish_curve(root/'published',curve,expected_source_bindings=registry['source_bindings'],clock=clock)
        attempt.update(publication_completed=True,status='published_not_consumed',publication=publication)
        write_record(root,parts,'publication_stage.json',attempt)
        consumed = files.consume_published_curve(root/'published',publication['descriptor'],
            expected_source_bindings=registry['source_bindings'],persist_consumption=True,clock=clock)
        attempt.update(status='issued_and_consumed',consumption_completed=True,
            consumption=consumed['consumption'])
        write_record(root,parts,'issued_observation.json',attempt)
        # Quote capture and entry receipts are factual observations, never broker fills.
        qcapture = quotes.capture_quote_snapshot(clock=clock)
        qraw, qreceipt = qcapture['raw_bytes'],qcapture['receipt']
        qraw_ref = write_raw(root,(*parts,'decision_quotes'),qraw)
        qreceipt_ref = write_record(root,(*parts,'decision_quotes'),'capture_receipt.json',qreceipt)
        decision = clock()
        qmapping = quotes.map_quote_snapshot(qraw,qreceipt,decision_epoch=decision,
            instruments=INSTRUMENTS,maximum_quote_age_sec=30)
        if qmapping['metadata'] != registry['metadata']:
            raise contract.CurveContractError('pilot_quote_metadata_mismatch')
        pair_quote = qmapping['quotes'].get(instrument)
        candidates = [candidate_for_target(curve,consumed['publication'],consumed['consumption'],
            decision_epoch=decision,target_epoch=node['original_target_epoch'],quote=pair_quote,
            metadata=registry['metadata'][instrument],expected_source_bindings=registry['source_bindings'],
            maximum_quote_age_sec=30,target_window_policy=registry['target_window_policy']) for node in prepared['nodes']]
        entry = outcomes.capture_entry_quote(pair_quote,source_sha256=qraw_ref['sha256'],clock=clock) if pair_quote is not None else None
        decision_record = dict(schema_version='recovered_curve_decision_observation_v1_20260909',
            curve_id=curve['curve_id'],curve_sha256=curve['curve_sha256'],decision_epoch=decision,
            quote_source=qraw_ref,quote_capture_receipt=qreceipt_ref,quote_mapping=qmapping,
            consumption=consumed['consumption'],candidates=candidates,entry_observation=entry,
            position_actions_performed=False,broker_fills_performed=False,**contract.AUTHORITY)
        attempt['decision_observation'] = write_record(root,parts,'decision_observation.json',decision_record)
        attempt['candidate_count'] = sum(row['status']=='available' for row in candidates)
    except Exception as error:
        attempt.update(error_type=type(error).__name__,reason_code=_reason(error))
        if attempt['status'] == 'issued_and_consumed':
            attempt['decision_status'] = 'failed_after_issue'
    attempt['completed_epoch'] = clock()
    write_record(root,parts,'attempt_completed.json',attempt)
    return attempt


def run_cycle(registry, model, *, session_id, clock=time.time, scheduled_epoch=None, skipped_slots=0):
    verify_sources(registry)
    root = Path(registry['output_root'])
    started = clock()
    scheduled = started if scheduled_epoch is None else contract.epoch(scheduled_epoch)
    if started < scheduled:
        raise contract.CurveContractError('pilot_cycle_started_before_schedule')
    cycle_id = 'cycle_'+str(time.time_ns())
    cycle = dict(schema_version='recovered_curve_pilot_cycle_v1_20260909',session_id=session_id,
        cycle_id=cycle_id,started_epoch=started,started_utc=_utc(started),
        scheduled_cycle_epoch=scheduled,start_delay_sec=started-scheduled,
        skipped_prior_schedule_slots=skipped_slots,pairs=[],**contract.AUTHORITY)
    write_record(root,('cycles',cycle_id),'cycle_started.json',cycle)
    for instrument in INSTRUMENTS:
        if clock()+registry['request_start_guard_sec'] >= registry['collection_stop_epoch']:
            break
        verify_sources(registry)
        parts = ('cycles',cycle_id,instrument.lower())
        pair = dict(instrument=instrument,status='failed',attempts=[])
        try:
            raw, receipt = candles.capture_once(instrument,credential_path=ROOT/'creds',metadata=registry['metadata'][instrument],clock=clock)
            pair['capture_receipt'] = write_record(root,parts,'capture_receipt.json',receipt)
            if receipt['status'] != 'captured_not_issued':
                pair['reason_code'] = receipt.get('error_reason','source_capture_failed')
            else:
                pair['raw_source'] = write_raw(root,parts,raw)
                pair['status'] = 'captured'
                for convention in CONVENTIONS:
                    mapping = candles.map_verified_capture(raw,receipt,registry['metadata'][instrument],convention)
                    write_record(root,(*parts,convention),'source_mapping.json',mapping)
                    if clock() < registry['issue_cutoff_epoch']:
                        pair['attempts'].append(attempt_variant(registry,model,mapping,convention,parts,clock=clock))
        except Exception as error:
            pair.update(error_type=type(error).__name__,reason_code=_reason(error))
        cycle['pairs'].append(pair)
    attempts=[a for pair in cycle['pairs'] for a in pair['attempts']]
    cycle.update(completed_epoch=clock(),curves_issued=sum(a.get('registry_issue_admitted') is True for a in attempts),
        curves_published=sum(a.get('publication_completed') is True for a in attempts),
        curves_consumed=sum(a.get('consumption_completed') is True for a in attempts))
    write_record(root,('cycles',cycle_id),'cycle_completed.json',cycle)
    print(json.dumps(dict(cycle_id=cycle_id,curves_issued=cycle['curves_issued'],
        curves_published=cycle['curves_published'],curves_consumed=cycle['curves_consumed'],
        captures=sum(pair['status']=='captured' for pair in cycle['pairs']),completed_epoch=cycle['completed_epoch'])),flush=True)
    return cycle


def next_initial_cycle(root, *, now, cycle_sec):
    """A restart must not immediately repeat the last completed scheduled slot."""
    directory = Path(root)/'cycles'
    if not directory.exists():
        return now
    files._safe_components(directory)
    names = sorted(child for child in directory.iterdir() if re.fullmatch(r'cycle_[0-9]{1,30}',child.name))
    if not names:
        return now
    # Even an interrupted cycle owns its slot; its immutable start record remains.
    last = names[-1]
    files._safe_components(last)
    start_path = last/'cycle_started.json'
    if not start_path.exists():
        raise contract.CurveContractError('pilot_prior_cycle_start_missing')
    record = _json(files._read(start_path))
    return max(now,contract.epoch(record['scheduled_cycle_epoch'])+cycle_sec)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--registry',type=Path,required=True)
    parser.add_argument('--registry-sha256',required=True)
    parser.add_argument('--once',action='store_true')
    args = parser.parse_args(argv)
    registry, registry_sha = load_registry(args.registry,args.registry_sha256)
    if time.time() >= registry['collection_stop_epoch']:
        raise contract.CurveContractError('pilot_user_deadline_elapsed')
    root = Path(registry['output_root'])
    with worker_lock(root):
        model = recovered.load_model(Path(registry['model_path']))
        session_id = 'session_'+str(time.time_ns())
        start = time.time()
        write_record(root,('sessions',session_id),'started.json',dict(session_id=session_id,
            pid=os.getpid(),started_epoch=start,registry_sha256=registry_sha,
            runtime=sys.version,once=args.once,**contract.AUTHORITY))
        cycle_count = 0
        next_cycle = next_initial_cycle(root,now=time.time(),cycle_sec=registry['cycle_sec'])
        skipped = 0
        try:
            while time.time() < registry['collection_stop_epoch']:
                while time.time() < min(next_cycle,registry['collection_stop_epoch']):
                    time.sleep(min(1,max(0,min(next_cycle,registry['collection_stop_epoch'])-time.time())))
                if time.time() >= registry['collection_stop_epoch']:
                    break
                run_cycle(registry,model,session_id=session_id,scheduled_epoch=next_cycle,skipped_slots=skipped)
                cycle_count += 1
                if args.once:
                    break
                next_cycle += registry['cycle_sec']
                skipped = max(0,math.floor((time.time()-next_cycle)/registry['cycle_sec'])+1)
                next_cycle += skipped*registry['cycle_sec']
        finally:
            write_record(root,('sessions',session_id),'stopped.json',dict(session_id=session_id,
                stopped_epoch=time.time(),completed_cycles=cycle_count,**contract.AUTHORITY))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
