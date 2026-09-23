"""Reuse pinned native curve receipts with explicit fresh-conditioning admission.

Only synthetic prospective-clock qualification is supported. Historical midpoint
forecasts and legacy price rebases do not become conditional forecasts by relabeling.
"""
from copy import deepcopy
import hashlib
import importlib.util
from pathlib import Path
import sys
from contracts import fingerprint
from reference_accounting_adapter_v2 import DEFAULT_TRAD

PREDECESSORS={'oanda_forecast_curve_contract_v1.py':'c03549586f922f6031790fce68a5afb5b98e4eda6157f20c4720b1adb05d0746',
 'oanda_curve_management_adapter_v1.py':'8fa6cccf1e2525d0186ab7abe0c68fccc4800db7b785a8b9f79f28c320ad1241'}
TIER='synthetic_fresh_conditional_native_curve.v1'

def load_native(trad_root=DEFAULT_TRAD):
    for name,digest in PREDECESSORS.items():
        p=(trad_root/name).resolve(strict=True)
        if hashlib.sha256(p.read_bytes()).hexdigest()!=digest:raise ValueError('native_predecessor_drift_before_import:'+name)
    modules=[]
    for name in PREDECESSORS:
        p=(trad_root/name).resolve();module_name=p.stem;cached=sys.modules.get(module_name)
        if cached is not None:
            if Path(cached.__file__).resolve()!=p:raise ValueError('native_cached_source_root_mismatch')
            modules.append(cached);continue
        spec=importlib.util.spec_from_file_location(module_name,p);module=importlib.util.module_from_spec(spec)
        sys.modules[module_name]=module;spec.loader.exec_module(module);modules.append(module)
    return tuple(modules)

def admit_frame(frame,ledger_config,trad_root=DEFAULT_TRAD):
    """Return qualified native rows and per-packet refusals; never interpolate."""
    c,adapter=load_native(trad_root)
    if frame.get('native_input_tier')!=TIER:raise ValueError('native_synthetic_qualification_tier_required')
    if set(frame.get('native_contract',{}))!={'maximum_conditioning_age_seconds','expected_source_bindings'}:raise ValueError('native_admission_contract_required')
    contract=frame['native_contract']
    if contract['maximum_conditioning_age_seconds']!=2:raise ValueError('frozen_conditioning_age_required')
    packets=frame.get('native_packets')
    if not isinstance(packets,list) or len(packets)>68:raise ValueError('bounded_native_packets_required')
    result=[];refusals=[];seen=set();epoch=frame['epoch'];target=frame['target_epoch']
    for packet in packets:
        pair=None
        try:
            if set(packet)!={'curve','publication','consumption','remaining_financing_usd_per_base_unit'}:raise ValueError('exact_native_packet_required')
            curve=packet['curve'];prepared=curve['prepared_curve'];pair=prepared['instrument']
            if pair in seen:raise ValueError('duplicate_native_instrument')
            seen.add(pair)
            context=prepared['input_context']
            if set(context)!={'input_tier','conditioning_kind','conditioning_epoch','thesis_version'} or context['input_tier']!=TIER or context['conditioning_kind']!='fresh_conditional_terminal_expectation':
                raise ValueError('fresh_conditional_contract_missing_legacy_rebase_refused')
            if context['conditioning_epoch']!=prepared['reference_epoch'] or not 0<=epoch-context['conditioning_epoch']<=2:
                raise ValueError('stale_conditional_information')
            if not isinstance(context['thesis_version'],str) or not context['thesis_version']:raise ValueError('native_thesis_version_required')
            meta=ledger_config['metadata'][pair]
            candidate=adapter.candidate_for_target(curve,packet['publication'],packet['consumption'],decision_epoch=epoch,target_epoch=target,
              quote=frame['quotes'].get(pair),metadata={'instrument':pair,**meta},expected_source_bindings=contract['expected_source_bindings'],
              maximum_quote_age_sec=float(ledger_config['quote_max_age_sec']),target_window_policy='exact_only')
            if candidate['status']!='available':raise ValueError(candidate['reason_code'])
            rates=packet['remaining_financing_usd_per_base_unit']
            if set(rates)!={'long','short'}:raise ValueError('signed_remaining_financing_required')
            for rate in rates.values():c.decimal_number(rate)
            candidate['original_adapter_candidate_sha256']=candidate.pop('candidate_sha256')
            candidate.update(thesis_version=context['thesis_version'],remaining_financing_usd_per_base_unit=deepcopy(rates),
              native_input_tier=TIER,native_packet_sha256=fingerprint(packet),conditional_information_epoch=context['conditioning_epoch'])
            candidate['candidate_sha256']=fingerprint(candidate)
            result.append(candidate)
        except (ValueError,KeyError,TypeError) as exc:
            refusals.append({'instrument':pair,'reason':str(exc),'packet_sha256':fingerprint(packet)})
    if len(seen)<len(packets) and any(r['reason']=='duplicate_native_instrument' for r in refusals):
        duplicates={r['instrument'] for r in refusals if r['reason']=='duplicate_native_instrument'}
        result=[r for r in result if r['instrument'] not in duplicates]
    return result,refusals

def prepare_frame(frame,ledger_config,trad_root=DEFAULT_TRAD):
    result=deepcopy(frame);candidates,refusals=admit_frame(frame,ledger_config,trad_root)
    result.update(candidate_kind='curve',candidates=candidates,native_refusals=refusals)
    return result

def validate_adapted_frame(frame,ledger_config,trad_root=DEFAULT_TRAD):
    candidates,refusals=admit_frame(frame,ledger_config,trad_root)
    if frame.get('candidates')!=candidates or frame.get('native_refusals')!=refusals:raise ValueError('adapted_native_frame_does_not_match_original_packets')
