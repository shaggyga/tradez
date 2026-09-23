"""Bounded local evidence I/O for a separate virtual management experiment.

No broker requests, account data, order functions or old-study writes. Reuse the
reviewed immutable-file primitives and preserve original publication clocks.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import time

import oanda_forecast_curve_contract_v1 as contract
import oanda_forecast_curve_file_store_v1 as files
import oanda_s5_mba_research_capture_v1 as candles

SCHEMA='observed_management_evidence_io_v1_20260909'
CYCLE=re.compile(r'cycle_[0-9]{16,25}')
MAX_CYCLE_ENTRIES=2048
FLAGS=dict(research_only=True,can_place_orders=False,can_promote=False,
    can_authorize=False,account_access=False,broker_access=False)


def location(root,parts):
    if (not isinstance(parts,(tuple,list)) or len(parts)>8 or
        any(not isinstance(part,str) or not re.fullmatch(r'[a-z0-9_]{1,96}',part) for part in parts)):
        raise ValueError('management_bounded_path_components_required')
    try:
        absolute=Path(os.path.abspath(os.fspath(root)))
    except (TypeError,ValueError,OSError):
        raise ValueError('management_root_invalid') from None
    return absolute,tuple(parts)


def decode(raw):
    def pairs(values):
        result={}
        for key,value in values:
            if key in result:
                raise ValueError('management_duplicate_json_key')
            result[key]=value
        return result
    return json.loads(raw,object_pairs_hook=pairs,parse_constant=lambda _:
        (_ for _ in ()).throw(ValueError('management_nonfinite_json')))


def read_file(root,parts,name):
    root,parts=location(root,parts)
    if not re.fullmatch(r'[a-z0-9_]{1,160}\.json',name):
        raise ValueError('management_record_name')
    directory=files._directory(root,parts,create=False)
    return files._read(directory/name)


def persist_bytes(root,parts,name,raw,*,clock=time.time):
    """Exclusive write, fsync and verified readback, with real completion clock."""
    root,parts=location(root,parts)
    if not isinstance(name,str) or not re.fullmatch(r'[a-z0-9_]{1,160}\.json',name):
        raise ValueError('management_record_name')
    if type(raw) is not bytes or not 0<len(raw)<=contract.MAX_BYTES:
        raise ValueError('management_record_byte_bound')
    started=contract.epoch(clock())
    directory=files._directory(root,parts,create=True)
    path=directory/name
    if not files._write_exclusive(path,raw):
        raise ValueError('management_record_already_exists')
    if files._read(path)!=raw:
        raise ValueError('management_record_readback_mismatch')
    completed=contract.epoch(clock())
    if completed<started:
        raise ValueError('management_publication_clock_reversed')
    return dict(schema_version=SCHEMA,relative_path=str(path.relative_to(root)).replace('\\','/'),
        bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest(),
        publication_started_epoch=started,publication_completed_epoch=completed,**FLAGS)


def persist_record(root,parts,name,value,*,clock=time.time):
    return persist_bytes(root,parts,name,contract.canonical_bytes(value),clock=clock)


def recent_cycles(root,limit=10):
    if type(limit) is not int or not 1<=limit<=20:
        raise ValueError('management_cycle_scan_bound')
    root,_=location(root,())
    directory=files._directory(root,('cycles',),create=False)
    # Names are only discovery hints. Every selected directory/file is checked
    # again by the immutable reader and its source/receipt binding.
    names=[]
    for count,path in enumerate(directory.iterdir(),1):
        if count>MAX_CYCLE_ENTRIES:
            raise ValueError('management_cycle_directory_scan_limit')
        if CYCLE.fullmatch(path.name):
            names.append(path.name)
    return sorted(names,reverse=True)[:limit]


def read_momentum_source(root,cycle_id,metadata,*,clock=time.time):
    if not isinstance(cycle_id,str) or not CYCLE.fullmatch(cycle_id):
        raise ValueError('management_cycle_identity')
    started=contract.epoch(clock())
    parts=('cycles',cycle_id,'gbp_usd')
    raw=read_file(root,parts,'source_raw.json')
    receipt=decode(read_file(root,parts,'capture_receipt.json'))
    completed=contract.epoch(clock())
    if not receipt['capture_completed_epoch']<=started<=completed:
        raise ValueError('management_source_read_clock_order')
    mapping=candles.map_verified_capture(raw,receipt,metadata,'official_midpoint')
    if mapping['instrument']!='GBP_USD':
        raise ValueError('management_source_pair')
    consumption=dict(raw_sha256=hashlib.sha256(raw).hexdigest(),receipt_sha256=receipt['receipt_sha256'],
        read_started_epoch=started,read_completed_epoch=completed)
    return dict(cycle_id=cycle_id,raw_bytes=raw,receipt=receipt,source_consumption=consumption,
        source_mapping_sha256=mapping['mapping_sha256'])


def read_curve_anchor(root,cycle_id,expected_source_bindings,study_id,*,clock=time.time):
    """Read one actual GBP/USD M publication and its admitted native H1 node."""
    root,_=location(root,())
    if not isinstance(cycle_id,str) or not CYCLE.fullmatch(cycle_id):
        raise ValueError('management_cycle_identity')
    parts=('cycles',cycle_id,'gbp_usd','official_midpoint')
    original=decode(read_file(root,parts,'issued_observation.json'))
    if (original.get('instrument')!='GBP_USD' or original.get('price_convention')!='official_midpoint'
        or original.get('status')!='issued_and_consumed'
        or any(original.get(k) is not True for k in ('registry_issue_admitted','publication_completed','consumption_completed'))):
        raise ValueError('management_original_issue_not_complete')
    consumed=files.consume_published_curve(root/'published',original['publication']['descriptor'],
        expected_source_bindings=expected_source_bindings,persist_consumption=False,clock=clock)
    curve=consumed['curve']
    prepared=curve['prepared_curve']
    if (original['curve_sha256']!=curve['curve_sha256'] or prepared['instrument']!='GBP_USD'
        or prepared['forecast_cohort']!=study_id+'/GBP_USD/official_midpoint'
        or prepared['input_context'].get('price_convention')!='official_midpoint'):
        raise ValueError('management_anchor_identity_mismatch')
    now=consumed['consumption']['available_epoch']
    if not 0<=now-curve['issued_epoch']<=120:
        raise ValueError('management_initial_curve_age_limit')
    nodes=[node for node in prepared['nodes'] if node['horizon_sec']==3600 and node['status']=='forecast']
    if len(nodes)!=1:
        raise ValueError('management_initial_native_h1_missing')
    node=nodes[0]
    if not any(a['node_id']==node['node_id'] and a['status']=='admitted' for a in curve['node_admission']):
        raise ValueError('management_initial_native_h1_not_admitted')
    if not now+3300<=node['original_target_epoch']<=now+3600:
        raise ValueError('management_initial_native_h1_remaining_bound')
    return dict(cycle_id=cycle_id,descriptor=original['publication']['descriptor'],
        original_issue_observation_sha256=contract.content_hash(original),
        original_target_epoch=node['original_target_epoch'],node_id=node['node_id'],**consumed)
