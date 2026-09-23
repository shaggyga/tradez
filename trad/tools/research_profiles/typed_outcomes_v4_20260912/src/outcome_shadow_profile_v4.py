"""Named passive research profile: original forecasts, v2 outcome clocks.

This is a separate cohort intake, not an in-place upgrade of any v1 worker.
It has no feed promotion, broker, account, executor, process or service API.
"""
from __future__ import annotations
from contextlib import contextmanager
import hashlib
import json
import math
import os
from pathlib import Path
import tempfile
import time

from oanda_outcome_clock_v2 import CONTRACT,build_market_quote_snapshot,number,epoch
from oanda_forecast_ledger_v4 import LiveForecastLedgerV4,LEDGER_CONTRACT
from profile_probability_contract_v4 import validate_envelope,point_fields
from oanda_signal_probability_semantics_v2 import original_generation_clock
from oanda_typed_signal_feed_adapter_v2 import normalize_forecast_v2

PROFILE='outcome_typed_probability_shadow_v4_20260912'
ROLES=('typed_signals','shared_intake')
MAX_FORECASTS=10000
MAX_ROLE_RETAINED_JSON_BYTES=1024*1024*1024


def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode('utf-8')


def read_json(path,maximum_bytes=64*1024*1024):
    path=Path(path)
    if not path.exists():return {}
    with path.open('rb') as handle:raw=handle.read(maximum_bytes+1)
    if len(raw)>maximum_bytes:raise ValueError('source_file_bound_exceeded')
    value=json.loads(raw.decode('utf-8'))
    if not isinstance(value,dict):raise ValueError('source_mapping_required')
    return value


def atomic_json(path,value):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    data=canonical(value)
    descriptor,name=tempfile.mkstemp(prefix=path.name+'.',suffix='.tmp',dir=path.parent)
    temporary=Path(name)
    try:
        with os.fdopen(descriptor,'wb') as handle:
            handle.write(data);handle.flush();os.fsync(handle.fileno())
        os.replace(temporary,path)
    finally:
        if temporary.exists():temporary.unlink()


@contextmanager
def owned_role_lock(path):
    import msvcrt
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('a+b') as handle:
        if handle.tell()==0:handle.write(b'0');handle.flush()
        handle.seek(0)
        try:msvcrt.locking(handle.fileno(),msvcrt.LK_NBLCK,1)
        except OSError as exc:raise ValueError('profile_role_already_owned') from exc
        try:yield
        finally:
            handle.seek(0);msvcrt.locking(handle.fileno(),msvcrt.LK_UNLCK,1)


def prepare_paths(root,role):
    root=Path(root).resolve()
    if role not in ROLES:raise ValueError('unknown_profile_role')
    marker=root/'profile.json'
    expected={'profile':PROFILE,'clock_contract':CONTRACT,'ledger_contract':LEDGER_CONTRACT,'account_execution_authorized':False,
              'promotion_authorized':False,'legacy_ledger_migration':False}
    root.mkdir(parents=True,exist_ok=True)
    if marker.exists():
        if canonical(read_json(marker))!=canonical(expected):raise ValueError('profile_identity_mismatch')
    else:
        if any(root.iterdir()):raise ValueError('new_empty_profile_root_required')
        atomic_json(marker,expected)
    path=root/role
    path.mkdir(exist_ok=True)
    receipts=path/'forecast_receipts';receipts.mkdir(exist_ok=True)
    snapshots=path/'input_snapshots';snapshots.mkdir(exist_ok=True)
    return {'root':root,'role':path,'receipts':receipts,'snapshots':snapshots,
            'receipt_count':sum(1 for _ in receipts.glob('*.json')),
            'retained_json_bytes':sum(item.stat().st_size for directory in (receipts,snapshots) for item in directory.glob('*.json')),
            'ledger':path/'forecasts_v4.sqlite','state':path/'state_v4.json','lock':path/'worker_v4.lock'}


def pin_run_policy(paths,*,max_quote_age_sec,max_delay_sec,max_feature_age_sec=None,producer_identity=None):
    policy={'profile':PROFILE,'role':paths['role'].name,'max_quote_age_sec':number(max_quote_age_sec),
            'max_outcome_delay_sec':number(max_delay_sec),
            'max_feature_age_sec':None if max_feature_age_sec is None else number(max_feature_age_sec),
            'producer_identity':producer_identity}
    path=paths['role']/'science_policy_v4.json'
    if path.exists():
        if canonical(read_json(path))!=canonical(policy):raise ValueError('new_cohort_required_for_changed_clock_policy')
    else:atomic_json(path,policy)


def normalize_candidate(raw,inputs,observed_epoch,max_quote_age_sec):
    """Freeze original forecast origin/entry/curve; never reanchor to live price."""
    if not isinstance(raw,dict):raise ValueError('forecast_mapping_required')
    candidate=json.loads(canonical(raw))
    validate_envelope(candidate)
    origin=number(candidate.get('forecast_generated_epoch',candidate.get('generated_epoch')))
    if origin<=0 or origin>observed_epoch:raise ValueError('invalid_forecast_origin')
    if 'generated_epoch' in candidate and number(candidate['generated_epoch'])!=origin:
        raise ValueError('forecast_origin_fields_disagree')
    if origin!=number(inputs.get('generated_epoch')):raise ValueError('forecast_origin_differs_from_original_input')
    original_generation_clock(candidate)
    candidate['forecast_generated_epoch']=origin
    pair=candidate.get('instrument');identifier=candidate.get('id')
    if not isinstance(identifier,str) or not identifier or len(identifier)>256:raise ValueError('original_forecast_identity_required')
    if not isinstance(pair,str) or not pair:raise ValueError('forecast_instrument_required')
    for field in ('model_id','family','input_timeframe'):
        if not isinstance(candidate.get(field),str) or not candidate[field]:raise ValueError('forecast_identity_field_required')
    bid,ask,pip=number(candidate.get('bid')),number(candidate.get('ask')),number(candidate.get('pip'))
    if bid<=0 or ask<bid or pip<=0:raise ValueError('invalid_forecast_entry_values')
    metadata=candidate.get('producer_metadata')
    if metadata is not None and not isinstance(metadata,dict):raise ValueError('forecast_metadata_mapping_required')
    original_instruments=inputs.get('instruments')
    if not isinstance(original_instruments,dict):raise ValueError('original_instrument_mapping_required')
    original_pair=original_instruments.get(pair) or {}
    if not isinstance(original_pair,dict) or not isinstance(original_pair.get('timeframe_features',{}),dict):
        raise ValueError('original_entry_metadata_mapping_required')
    for entry_source in (original_pair.get('quote') or {},original_pair.get('features') or {},
                        (original_pair.get('timeframe_features') or {}).get(candidate['input_timeframe']) or {}):
        if not isinstance(entry_source,dict):raise ValueError('original_entry_metadata_mapping_required')
        for field in ('pip','pip_size'):
            if entry_source.get(field) is not None and number(entry_source[field])!=pip:
                raise ValueError('forecast_pip_differs_from_original_input')
    source=build_market_quote_snapshot(inputs,now=observed_epoch,max_quote_age_sec=max_quote_age_sec)
    quote=(source['instruments'].get(pair) or {}).get('quote')
    if not quote:raise ValueError('forecast_entry_clock_unavailable_or_stale')
    if quote['bid']!=bid or quote['ask']!=ask:raise ValueError('forecast_entry_differs_from_original_input')
    if quote['quote_epoch']>origin:raise ValueError('entry_quote_after_forecast_origin')
    curve=candidate.get('forecast_curve')
    if not isinstance(curve,dict) or not 1<=len(curve)<=64:raise ValueError('bounded_forecast_curve_required')
    targets=[];horizons=set()
    for horizon,point in list(curve.items()):
        seconds=number(horizon)
        if not seconds.is_integer() or not 0<seconds<=604800:raise ValueError('invalid_forecast_horizon')
        if int(seconds) in horizons:raise ValueError('duplicate_numeric_forecast_horizon')
        horizons.add(int(seconds))
        fields=point_fields(point)
        if point['horizon_sec']!=int(seconds):raise ValueError('horizon_fields_disagree')
        # Preserve the original typed point and source target exactly. Do not
        # call the old normalizer or turn side preference into midpoint P(up).
        targets.append(origin+seconds)
    if min(targets)<=observed_epoch:raise ValueError('forecast_observed_after_first_target')
    candidate['account_eligible']=False
    for point in curve.values():point['account_eligible']=False
    entry={'entry_quote_time':quote['time'],'entry_quote_epoch':quote['quote_epoch'],
                      'source_capture_epoch':quote['source_received_epoch'],'input_read_epoch':source['read_epoch']}
    candidate['research_entry_clock']={key:value for key,value in entry.items() if key!='input_read_epoch'}
    candidate['research_forecast_observed_epoch']=observed_epoch
    return candidate,entry


def record_forecast(paths,ledger,raw,inputs,*,observed_epoch,max_quote_age_sec,observation_receipt=None,intake_epoch=None):
    candidate,entry=normalize_candidate(raw,inputs,observed_epoch,max_quote_age_sec)
    if observation_receipt is not None:
        source_identity=observation_receipt.get('identity')
        if (observation_receipt.get('profile')!=PROFILE or observation_receipt.get('role') not in ('typed_signals',)
            or observation_receipt.get('clock_contract')!=CONTRACT or not isinstance(source_identity,dict)
            or hashlib.sha256(canonical(source_identity)).hexdigest()!=observation_receipt.get('identity_sha256')
            or canonical(source_identity.get('candidate'))!=canonical(candidate)
            or canonical(source_identity.get('source_forecast'))!=canonical(raw)
            or source_identity.get('source_payload_sha256')!=hashlib.sha256(canonical(inputs)).hexdigest()
            or number(observation_receipt.get('forecast_observed_epoch'))!=observed_epoch):
            raise ValueError('original_forecast_observation_proof_required')
    # Input identity excludes this observer's clocks but binds original source
    # payload. Same producer ID with changed economic/source inputs is refused.
    # Observation time belongs to the first retained receipt. Do not let a
    # repeated intake advance it or change the original publication identity.
    path=paths['receipts']/(hashlib.sha256(candidate['id'].encode()).hexdigest()+'.json')
    if path.exists():
        previous_observed=read_json(path)['identity']['candidate']['research_forecast_observed_epoch']
        if observation_receipt is not None and previous_observed!=observed_epoch:
            raise ValueError('original_forecast_observation_changed')
        candidate['research_forecast_observed_epoch']=previous_observed
    identity={'candidate':candidate,'source_forecast':raw,'source_payload_sha256':hashlib.sha256(canonical(inputs)).hexdigest(),
              'source_observation_receipt_sha256':None if observation_receipt is None else hashlib.sha256(canonical(observation_receipt)).hexdigest()}
    digest=hashlib.sha256(canonical(identity)).hexdigest()
    if path.exists():
        receipt=read_json(path)
        if receipt.get('identity_sha256')!=digest:raise ValueError('immutable_forecast_identity_collision')
    else:
        if paths['receipt_count']>=MAX_FORECASTS:raise ValueError('forecast_receipt_population_bound_exceeded')
        source_sha=identity.get('source_payload_sha256')
        if not isinstance(source_sha,str) or len(source_sha)!=64 or any(char not in '0123456789abcdef' for char in source_sha):
            raise ValueError('source_input_identity_invalid')
        expected_name=hashlib.sha256(identity['candidate']['id'].encode()).hexdigest()+'.json'
        if path.name!=expected_name:raise ValueError('forecast_receipt_filename_identity_invalid')
        snapshot_path=paths['snapshots']/(source_sha+'.json')
        if snapshot_path.exists():
            if hashlib.sha256(snapshot_path.read_bytes()).hexdigest()!=identity['source_payload_sha256']:
                raise ValueError('immutable_input_snapshot_changed')
        else:
            size=len(canonical(inputs))
            if size>64*1024*1024 or paths['retained_json_bytes']+size>MAX_ROLE_RETAINED_JSON_BYTES:
                raise ValueError('research_retained_json_storage_bound_exceeded')
            atomic_json(snapshot_path,inputs);paths['retained_json_bytes']+=size
        receipt={'profile':PROFILE,'role':paths['role'].name,'clock_contract':CONTRACT,
                 'cohort_policy_sha256':hashlib.sha256((paths['role']/'science_policy_v4.json').read_bytes()).hexdigest(),
                 'identity_sha256':digest,'identity':identity,'entry_clock':entry,
                 'forecast_observed_epoch':observed_epoch,
                 'receipt_semantics':'forecast seen by this process; not an external feed publication or order',
                 'account_execution_authorized':False}
        if observation_receipt is not None:
            if intake_epoch is None or number(intake_epoch)<observed_epoch:raise ValueError('future_source_forecast_observation')
            receipt.update(source_observation_receipt=observation_receipt,intake_observed_epoch=number(intake_epoch),
                receipt_semantics='original source observation retained; this process intake is recorded separately')
        size=len(canonical(receipt))
        if size>1024*1024 or paths['retained_json_bytes']+size>MAX_ROLE_RETAINED_JSON_BYTES:
            raise ValueError('research_retained_json_storage_bound_exceeded')
        atomic_json(path,receipt);paths['retained_json_bytes']+=size
        if canonical(read_json(path))!=canonical(receipt):raise ValueError('forecast_receipt_readback_failed')
        paths['receipt_count']+=1
    return ledger.register([receipt['identity']['candidate']],receipt['identity']['source_payload_sha256'])


def recover_receipts(paths,ledger):
    files=list(paths['receipts'].glob('*.json'))
    if len(files)>MAX_FORECASTS:raise ValueError('forecast_receipt_population_bound_exceeded')
    recovered=0
    for path in sorted(files):
        receipt=read_json(path)
        identity=receipt.get('identity')
        if (receipt.get('profile')!=PROFILE or receipt.get('role')!=paths['role'].name or
            receipt.get('clock_contract')!=CONTRACT or not isinstance(identity,dict) or
            receipt.get('cohort_policy_sha256')!=hashlib.sha256((paths['role']/'science_policy_v4.json').read_bytes()).hexdigest() or
            hashlib.sha256(canonical(identity)).hexdigest()!=receipt.get('identity_sha256')):
            raise ValueError('forecast_receipt_identity_invalid')
        source_sha=identity.get('source_payload_sha256')
        if not isinstance(source_sha,str) or len(source_sha)!=64 or any(char not in '0123456789abcdef' for char in source_sha):
            raise ValueError('source_input_identity_invalid')
        expected_name=hashlib.sha256(identity['candidate']['id'].encode()).hexdigest()+'.json'
        if path.name!=expected_name:raise ValueError('forecast_receipt_filename_identity_invalid')
        snapshot_path=paths['snapshots']/(source_sha+'.json')
        if hashlib.sha256(snapshot_path.read_bytes()).hexdigest()!=identity['source_payload_sha256']:
            raise ValueError('immutable_input_snapshot_changed')
        recovered+=ledger.register([identity['candidate']],identity['source_payload_sha256'])
    return recovered


def maintain(ledger,quote_reader,*,clock,max_quote_age_sec,max_delay_sec,batch_size):
    read_error=None
    try:payload=quote_reader()
    except (OSError,ValueError,TypeError) as exc:
        payload={};read_error=type(exc).__name__
    read_at=number(clock())
    try:snap=build_market_quote_snapshot(payload,now=read_at,max_quote_age_sec=max_quote_age_sec)
    except (TypeError,ValueError,OverflowError) as exc:
        # A broken read provides no source clock or price. It still produces a
        # fresh empty audit snapshot so overdue pending rows can be censored.
        read_error='invalid_quote_payload:'+type(exc).__name__
        snap=build_market_quote_snapshot({},now=read_at,max_quote_age_sec=max_quote_age_sec)
    result=ledger.mature(snap,max_delay_sec,batch_size=batch_size,now=clock())
    return {'maturity':result,'quote_status':snap['status'],'quote_count':snap['instrument_count'],
            'eligible_pairs':sorted(snap['instruments']),
            'read_epoch':snap['read_epoch'],'source_capture_epoch':snap['source_received_epoch'],
            'oldest_quote_epoch':snap.get('oldest_quote_epoch'),'rejections':snap.get('rejections'),
            'receipt_rejection':snap.get('receipt_rejection'),'read_error':read_error}


def run_profile(root,role,*,input_reader,quote_reader,producer,cycles=1,interval_sec=1,
                max_feature_age_sec=600,max_quote_age_sec=30,max_delay_sec=180,
                batch_size=5000,clock=time.time,sleep=time.sleep,producer_identity=None):
    """Bounded real profile loop; readers and original producer are explicit.

    A rejected/missing input or producer failure still reaches post-work quote
    capture and maturity. A genuine wall-clock rollback remains a hard failure.
    """
    if type(cycles) is not int or not 1<=cycles<=10000:raise ValueError('bounded_cycles_required')
    for value in (interval_sec,max_feature_age_sec,max_quote_age_sec,max_delay_sec):
        if number(value)<0:raise ValueError('nonnegative_profile_limits_required')
    paths=prepare_paths(root,role)
    with owned_role_lock(paths['lock']):
        pin_run_policy(paths,max_quote_age_sec=max_quote_age_sec,max_delay_sec=max_delay_sec,
                       max_feature_age_sec=max_feature_age_sec,producer_identity=producer_identity)
        ledger=LiveForecastLedgerV4(paths['ledger'])
        try:
            recovered=recover_receipts(paths,ledger)
            state={}
            for cycle in range(cycles):
                before=maintain(ledger,quote_reader,clock=clock,max_quote_age_sec=max_quote_age_sec,
                                max_delay_sec=max_delay_sec,batch_size=batch_size)
                status='no_forecasts';errors=[];recorded=0;forecasts=[];inputs={}
                try:
                    inputs=json.loads(canonical(input_reader()));read_at=number(clock())
                    generated=number(inputs.get('generated_epoch'))
                    if generated<=0 or generated>read_at:raise ValueError('invalid_feature_capture_clock')
                    if read_at-generated>max_feature_age_sec:raise ValueError('stale_feature_capture')
                    if not before['eligible_pairs']:raise ValueError('fresh_market_capture_required_before_model')
                    forecasts=producer(json.loads(canonical(inputs)))
                    if not isinstance(forecasts,list) or len(forecasts)>1000:raise ValueError('forecast_batch_bound_exceeded')
                except Exception as exc:
                    status='producer_or_input_blocked';errors.append(type(exc).__name__+':'+str(exc)[:160])
                    forecasts=[]
                after=maintain(ledger,quote_reader,clock=clock,max_quote_age_sec=max_quote_age_sec,
                               max_delay_sec=max_delay_sec,batch_size=batch_size)
                for raw in forecasts:
                    try:
                        if not isinstance(raw,dict):raise ValueError('forecast_mapping_required')
                        if raw.get('instrument') not in before['eligible_pairs'] or raw.get('instrument') not in after['eligible_pairs']:
                            raise ValueError('fresh_pair_capture_required_before_and_after_model')
                        observed=number(clock())
                        if observed<after['maturity']['evaluated_epoch']:raise ValueError('forecast_record_clock_rollback')
                        recorded+=record_forecast(paths,ledger,raw,inputs,observed_epoch=observed,max_quote_age_sec=max_quote_age_sec)
                    except (TypeError,ValueError) as exc:errors.append(str(exc))
                if forecasts:status='recorded' if recorded else 'forecasts_withheld_or_duplicate'
                updated=number(clock())
                if updated<after['maturity']['evaluated_epoch']:raise ValueError('profile_state_clock_rollback')
                state={'profile':PROFILE,'role':role,'clock_contract':CONTRACT,'cycle':cycle+1,'status':status,
                       'recorded_prediction_points':recorded,'recovered_prediction_points':recovered,
                       'pending_prediction_points':ledger.pending_predictions,'ledger_contract':LEDGER_CONTRACT,'before_work':before,'after_work':after,
                       'errors':errors,'account_execution_authorized':False,'promotion_authorized':False,
                       'updated_epoch':updated,'existing_v1_callers_upgraded':False}
                atomic_json(paths['state'],state)
                if cycle+1<cycles:sleep(min(number(interval_sec),30))
            return state
        finally:ledger.close()


def run_typed_source_profile(root,*,source,producer,**loop_options):
    """Explicit producer-to-content-ID adapter for this new profile only.

    The source name is part of pinned policy and identity. Existing normalized
    rows retain their supplied IDs; arbitrary IDs at the low-level caller are
    provenance, not a claim of independently verified producer uniqueness.
    """
    if not isinstance(source,str) or not source:raise ValueError('fixed_typed_source_name_required')
    producer_identity=loop_options.pop('producer_identity',None)
    identity={'typed_source_name':source,'normalizer_contract':'typed_signal_probability_v2',
              'source_producer_identity':producer_identity}
    def produce(inputs):
        values=producer(inputs)
        if not isinstance(values,list) or len(values)>1000:raise ValueError('forecast_batch_bound_exceeded')
        result=[]
        for raw in values:
            if isinstance(raw,dict) and str(raw.get('id','')).startswith('typed-v2-'):
                if raw.get('source')!=source or raw.get('legacy_consumer_compatible') is not False:
                    raise ValueError('normalized_typed_source_identity_mismatch')
                validate_envelope(raw)
                result.append(json.loads(canonical(raw)))
            else:
                result.append(normalize_forecast_v2(raw,source=source))
        return result
    return run_profile(root,'typed_signals',producer=produce,producer_identity=identity,**loop_options)


def read_profile_forecasts(source_root,*,destination_root=None):
    """Read only the typed producer role of this explicit v4 cohort."""
    root=Path(source_root).resolve();marker=read_json(root/'profile.json')
    if marker.get('profile')!=PROFILE or marker.get('ledger_contract')!=LEDGER_CONTRACT:
        raise ValueError('source_profile_identity_required')
    for role in ('typed_signals',):
        files=list((root/role/'forecast_receipts').glob('*.json'))
        if len(files)>MAX_FORECASTS:raise ValueError('source_receipt_population_bound_exceeded')
        for path in sorted(files):
            receipt=read_json(path);identity=receipt.get('identity')
            if (receipt.get('profile')!=PROFILE or receipt.get('role')!=role or not isinstance(identity,dict)
                or receipt.get('cohort_policy_sha256')!=hashlib.sha256((root/role/'science_policy_v4.json').read_bytes()).hexdigest()
                or hashlib.sha256(canonical(identity)).hexdigest()!=receipt.get('identity_sha256')):
                raise ValueError('source_forecast_receipt_changed')
            if destination_root is not None:
                destination=Path(destination_root).resolve()/'shared_intake/forecast_receipts'/path.name
                if destination.exists():
                    prior=read_json(destination)
                    if prior.get('identity',{}).get('source_observation_receipt_sha256')==hashlib.sha256(canonical(receipt)).hexdigest():
                        continue
            source_sha=identity['source_payload_sha256']
            if not isinstance(source_sha,str) or len(source_sha)!=64 or any(char not in '0123456789abcdef' for char in source_sha):
                raise ValueError('source_input_identity_invalid')
            snapshot_path=root/role/'input_snapshots'/(source_sha+'.json')
            snapshot=read_json(snapshot_path)
            if hashlib.sha256(canonical(snapshot)).hexdigest()!=source_sha:raise ValueError('source_input_snapshot_changed')
            yield {'receipt':receipt,'inputs':snapshot}


def run_shared_intake(root,*,receipt_reader,quote_reader,cycles=1,interval_sec=1,max_quote_age_sec=30,
                      max_delay_sec=180,batch_size=5000,clock=time.time,sleep=time.sleep,source_identity=None):
    """Versioned replacement for shared-tracker outcome plumbing, local only.

    It consumes proof-bearing v4 source receipts. A legacy feed row without
    its entry and original observation clocks is not silently grandfathered.
    """
    if type(cycles) is not int or not 1<=cycles<=10000:raise ValueError('bounded_cycles_required')
    for value in (interval_sec,max_quote_age_sec,max_delay_sec):
        if number(value)<0:raise ValueError('nonnegative_profile_limits_required')
    paths=prepare_paths(root,'shared_intake')
    with owned_role_lock(paths['lock']):
        pin_run_policy(paths,max_quote_age_sec=max_quote_age_sec,max_delay_sec=max_delay_sec,producer_identity=source_identity)
        ledger=LiveForecastLedgerV4(paths['ledger'])
        try:
            recovered=recover_receipts(paths,ledger)
            for cycle in range(cycles):
                before=maintain(ledger,quote_reader,clock=clock,max_quote_age_sec=max_quote_age_sec,
                                max_delay_sec=max_delay_sec,batch_size=batch_size)
                recorded=0;errors=[]
                try:
                    envelopes=iter(receipt_reader())
                    for index,envelope in enumerate(envelopes):
                        if index>=1000:break
                        try:
                            receipt=envelope['receipt'];observed=number(receipt['forecast_observed_epoch'])
                            recorded+=record_forecast(paths,ledger,receipt['identity']['source_forecast'],envelope['inputs'],
                                observed_epoch=observed,max_quote_age_sec=max_quote_age_sec,
                                observation_receipt=receipt,intake_epoch=clock())
                        except (TypeError,ValueError,KeyError) as exc:errors.append(type(exc).__name__+':'+str(exc)[:160])
                except Exception as exc:errors.append(type(exc).__name__+':'+str(exc)[:160])
                after=maintain(ledger,quote_reader,clock=clock,max_quote_age_sec=max_quote_age_sec,
                               max_delay_sec=max_delay_sec,batch_size=batch_size)
                updated=number(clock())
                if updated<after['maturity']['evaluated_epoch']:raise ValueError('profile_state_clock_rollback')
                state={'profile':PROFILE,'role':'shared_intake','ledger_contract':LEDGER_CONTRACT,'clock_contract':CONTRACT,
                       'cycle':cycle+1,'recorded_prediction_points':recorded,'recovered_prediction_points':recovered,
                       'pending_prediction_points':ledger.pending_predictions,'before_work':before,'after_work':after,
                       'errors':errors,'account_execution_authorized':False,'promotion_authorized':False,
                       'existing_v1_callers_upgraded':False,'updated_epoch':updated}
                atomic_json(paths['state'],state)
                if cycle+1<cycles:sleep(min(number(interval_sec),30))
            return state
        finally:ledger.close()
