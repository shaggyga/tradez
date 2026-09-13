"""Bounded offline audit of immutable M1 distribution worker records.

No GET, account, fitting, runtime writes or checkpointing. All labels use the
unchanged registered pure outcome engine and caller-retained source receipts.
"""
from collections import Counter
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import time

BASE = Path(__file__).resolve().parent
TRAD = Path(r'C:/Users/zmoor/Documents/forex/trad')
sys.dont_write_bytecode = True
sys.path.insert(0, str(TRAD))
import oanda_m1_risk_distribution_worker_v1 as worker
import oanda_m1_risk_distribution_outcomes_v1 as outcomes

contract, mapper, files = worker.contract, worker.capture, worker.files
SCHEMA = 'retained_m1_risk_distribution_evaluation_v1_20260909'
MAX_CYCLES = 512
MAX_FILES = 20000
MAX_READ_BYTES = 512 * 1024 * 1024
ARTIFACT_NAMES = dict(raw='source_raw.json',capture_receipt='capture_receipt.json',
    input_consumption='input_consumption.json',mapping='source_mapping.json',
    issued='issued.json',publication='publication.json',consumption='consumption.json',
    consumer_read_evidence='consumer_read_evidence.json')


def need(ok, reason):
    if not ok:
        raise ValueError('retained_risk_' + reason)


def sha(raw): return hashlib.sha256(raw).hexdigest()


class Reader:
    def __init__(self, root, *, clock=time.time):
        self.root = Path(root); self.clock = clock; self.records = {}; self.total = 0
        files._safe_components(self.root)

    def exists(self, parts, name):
        return self.root.joinpath(*parts,name).exists()

    def read(self, parts, name):
        need(all(type(p) is str and re.fullmatch(r'[a-z0-9_]{1,64}',p) for p in parts)
             and re.fullmatch(r'[a-z0-9_]{1,64}\.json',name), 'internal_path')
        path = self.root.joinpath(*parts,name)
        begun = contract.epoch(self.clock()); raw = files._read(path)
        completed = contract.epoch(self.clock())
        need(begun <= completed, 'audit_read_clock')
        key = '/'.join((*parts,name)); hashed = sha(raw)
        need(key not in self.records or self.records[key]['sha256'] == hashed,
             'immutable_file_changed_during_audit')
        self.total += len(raw)
        need(self.total <= MAX_READ_BYTES and (key in self.records or len(self.records)<MAX_FILES),
             'audit_resource_bound')
        self.records[key] = dict(relative_path=key,sha256=hashed,bytes=len(raw),
            audit_read_started_epoch=begun,audit_read_completed_epoch=completed)
        return raw

    def json(self, parts, name): return contract.decode(self.read(parts,name))

    def directories(self, parts, pattern, limit):
        directory = self.root.joinpath(*parts)
        if not directory.exists(): return []
        files._safe_components(directory)
        names=[]
        for i,path in enumerate(directory.iterdir(),1):
            need(i <= limit, 'directory_inventory_bound')
            need(re.fullmatch(pattern,path.name), 'unexpected_directory_entry')
            files._safe_components(path)
            names.append(path.name)
        return sorted(names)

    def partial_inventory(self, parts):
        directory=self.root.joinpath(*parts)
        if not directory.exists():return []
        files._safe_components(directory)
        result=[]
        for count,path in enumerate(directory.iterdir(),1):
            need(count<=16,'partial_file_inventory_bound')
            need(path.name in {*ARTIFACT_NAMES.values(),'pair_completed.json'},'partial_file_name')
            raw=self.read(parts,path.name)
            result.append(dict(file=path.name,sha256=sha(raw),bytes=len(raw)))
        return sorted(result,key=lambda r:r['file'])


def authority(value):
    need(all(value.get(k) is v for k,v in contract.AUTHORITY.items()), 'record_authority')


def read_evidence(value, raw, filename):
    need(type(value) is dict and set(value)=={'file','bytes_sha256','byte_count',
        'read_started_epoch','read_completed_epoch'}, 'read_receipt_shape')
    need(value['file']==filename and value['bytes_sha256']==sha(raw)
         and type(value['byte_count']) is int and value['byte_count']==len(raw),
         'read_receipt_bytes')
    start,end=(contract.epoch(value[k]) for k in ('read_started_epoch','read_completed_epoch'))
    need(start <= end, 'read_receipt_clock')
    return start,end


def verify_artifacts(reader, parts, pair):
    artifacts=pair['artifacts']
    need(type(artifacts) is dict and set(artifacts)<=set(ARTIFACT_NAMES), 'artifact_inventory')
    result={}; clocks={}
    for key,ref in artifacts.items():
        need(type(ref) is dict and set(ref)=={'file','bytes_sha256','byte_count',
            'write_started_epoch','write_readback_completed_epoch'}, 'artifact_write_shape')
        name=ARTIFACT_NAMES[key];raw=reader.read(parts,name)
        need(ref['file']==name and ref['bytes_sha256']==sha(raw)
             and type(ref['byte_count']) is int and ref['byte_count']==len(raw), 'artifact_bytes')
        a,b=(contract.epoch(ref[k]) for k in ('write_started_epoch','write_readback_completed_epoch'))
        need(pair['started_epoch'] <= a <= b <= pair['completed_epoch'], 'artifact_write_clock')
        result[key]=raw;clocks[key]=(a,b)
    return result,clocks


def verify_sessions(reader, registry, registry_sha, fit_raw):
    result=[]
    for name in reader.directories(('sessions',),r'[0-9]{1,30}',32):
        parts=('sessions',name)
        if not reader.exists(parts,'started.json'):
            result.append(dict(session_id=name,status='partial_missing_started_record'));continue
        value=reader.json(parts,'started.json');authority(value)
        need(value.get('schema_version')=='m1_risk_worker_session_v1_20260909'
             and value['registry_sha256']==registry_sha, 'session_identity')
        start=contract.epoch(value['started_epoch'])
        a,b=read_evidence(value['training_artifact_read'],fit_raw,Path(registry['fit_artifact_path']).name)
        need(start <= a <= b <= contract.epoch(reader.clock()), 'session_training_read_clock')
        retained_fit=reader.read(parts,'training_artifact.json')
        persisted=value['training_artifact_persistence']
        need(retained_fit==fit_raw and set(persisted)=={'file','bytes_sha256','byte_count',
            'write_started_epoch','write_readback_completed_epoch'}
            and persisted['file']=='training_artifact.json' and persisted['bytes_sha256']==sha(fit_raw)
            and type(persisted['byte_count']) is int and persisted['byte_count']==len(fit_raw),
            'session_training_persistence_identity')
        pa,pb=(contract.epoch(persisted[k]) for k in ('write_started_epoch','write_readback_completed_epoch'))
        need(b<=pa<=pb<=contract.epoch(reader.clock()),'session_training_persistence_clock')
        stopped=None
        if reader.exists(parts,'stopped.json'):
            end=reader.json(parts,'stopped.json');authority(end)
            need(end['registry_sha256']==registry_sha and end['started_epoch']==start,
                 'session_stop_identity')
            stopped=contract.epoch(end['completed_epoch'])
            need(b <= stopped <= contract.epoch(reader.clock()), 'session_stop_clock')
        result.append(dict(session_id=name,status='retained_started',started_epoch=start,
            training_read_completed_epoch=b,training_persisted_epoch=pb,stopped_epoch=stopped))
    return result


def inspect_pair(reader, registry, sessions, cycle, pair):
    parts=('cycles',str(cycle),pair.lower())
    if not reader.exists(parts,'pair_completed.json'):
        return dict(instrument=pair,cycle_epoch=cycle,status='partial_missing_pair_completion',
            retained_unadmitted_files=reader.partial_inventory(parts)),None,None
    value=reader.json(parts,'pair_completed.json');authority(value)
    need(value['instrument']==pair and value['cycle_epoch']==cycle, 'pair_identity')
    scheduled=registry['first_reference_epoch']<=cycle<=registry['last_reference_epoch'] and cycle%300==60
    need(value['scheduled_issue'] is scheduled and type(value['capture_attempted']) is bool,
         'pair_schedule_or_attempt')
    need(type(value.get('capture_invoked',False)) is bool,'capture_invoked_type')
    begun,done=(contract.epoch(value[k]) for k in ('started_epoch','completed_epoch'))
    need(cycle <= begun <= done <= contract.epoch(reader.clock()), 'pair_clock')
    raw,write=verify_artifacts(reader,parts,value)
    observed=dict(instrument=pair,cycle_epoch=cycle,status=value['status'],phase=value['phase'],
        scheduled_issue=scheduled,capture_attempted=value['capture_attempted'],
        capture_invoked=value.get('capture_invoked',False),
        started_epoch=begun,completed_epoch=done,reason_code=value.get('reason_code'),
        capture_status=value.get('capture_status'),artifact_names=sorted(raw),
        source_disposition='no_verified_successful_source',accepted_forecast=False)
    if value['status']=='issued_published_consumed':
        need(set(raw)==set(ARTIFACT_NAMES) and value['phase']=='complete', 'false_complete_pair')
    elif value['status']=='captured_only':
        need(not scheduled and value['phase']=='capture_complete', 'false_captured_only')
    else: need(value['status']=='withheld_or_partial', 'unknown_pair_status')
    if 'capture_receipt' not in raw:
        need(value['status']=='withheld_or_partial', 'complete_without_capture')
        return observed,None,None
    receipt=contract.decode(raw['capture_receipt']);source=raw.get('raw',b'')
    need(value.get('capture_invoked') is True and
         value['capture_attempted'] is (receipt.get('request_started_epoch') is not None),
         'capture_attempt_receipt_mismatch')
    if receipt.get('request_started_epoch') is not None:
        request_start=contract.epoch(receipt['request_started_epoch'])
        need(begun<=request_start and request_start+mapper.MAX_SECONDS<=registry['collection_end_epoch'],
             'original_request_outside_pair_or_collection_window')
    need(receipt.get('schema_version')==mapper.SCHEMA and receipt.get('instrument')==pair
         and receipt.get('metadata')==registry['metadata'][pair]
         and receipt.get('source_bindings')==mapper.source_bindings()
         and all(receipt.get(k) is v for k,v in mapper.FLAGS.items()),'capture_receipt_identity')
    need(receipt.get('receipt_sha256')==contract.digest({k:v for k,v in receipt.items() if k!='receipt_sha256'}),
         'capture_receipt_seal')
    need(value.get('capture_status')==receipt.get('status')
         and contract.canonical(receipt.get('request'))==contract.canonical(mapper._request(pair))
         and contract.canonical(receipt.get('limits'))==contract.canonical(mapper.LIMITS),
         'capture_status_or_request_binding')
    need(receipt['raw_response']['sha256']==sha(source) and type(receipt['raw_response']['bytes']) is int
         and receipt['raw_response']['bytes']==len(source), 'capture_raw_reference')
    if not source:
        need(value.get('empty_raw_response')=={'bytes':0,'sha256':sha(b'')}, 'empty_raw_disposition')
    if receipt['status']!='captured_not_issued':
        need(receipt['status']=='failed' and value['status']=='withheld_or_partial', 'failed_capture_claimed_complete')
        if receipt.get('capture_completed_epoch') is not None:
            need(contract.epoch(receipt['capture_completed_epoch'])<=write['capture_receipt'][0],
                 'failed_capture_persistence_clock')
        observed['source_disposition']='retained_failed_capture'
        observed['original_capture_failure']=dict(status=receipt['status'],reason=receipt.get('error_reason'),
            read_completed_epoch=receipt.get('read_completed_epoch'),
            request_started_epoch=receipt.get('request_started_epoch'),
            original_first_observed_epoch=receipt.get('first_observed_epoch'),
            capture_completed_epoch=receipt.get('capture_completed_epoch'),raw_bytes=len(source),
            full_receipt_bytes_sha256=sha(raw['capture_receipt']),internal_receipt_sha256=receipt['receipt_sha256'])
        return observed,None,None
    need(source and 'raw' in write, 'successful_capture_without_raw')
    need(receipt['capture_completed_epoch'] <= write['raw'][0]
         and write['raw'][1] <= write['capture_receipt'][0], 'capture_persist_order')
    mapping=mapper.map_verified_capture(source,receipt,registry['metadata'][pair])
    document=mapper.mapping_document(mapping)
    source_record=dict(parts=list(parts),instrument=pair,source_sha256=sha(source),
        internal_receipt_sha256=receipt['receipt_sha256'],full_receipt_bytes_sha256=sha(raw['capture_receipt']),
        source_read_completed_epoch=receipt['read_completed_epoch'],capture_completed_epoch=receipt['capture_completed_epoch'],
        first_label_epoch=document['coverage']['first_complete_bar_label_epoch'],
        last_label_epoch=document['coverage']['last_complete_bar_label_epoch'],
        complete_rows=document['canonical_csv']['row_count'],mapping_sha256=document['mapping_sha256'])
    observed['source_disposition']='verified_source' if source_record['complete_rows'] else 'verified_empty_complete_domain'
    if 'input_consumption' in raw:
        inp=contract.decode(raw['input_consumption']);consumed=inp['input_consumption']
        a,b=read_evidence(inp['raw_read'],source,'source_raw.json')
        c,d=read_evidence(inp['receipt_read'],raw['capture_receipt'],'capture_receipt.json')
        need(write['capture_receipt'][1] <= consumed['read_started_epoch'] <= a <= b <= c <= d
             <= consumed['read_completed_epoch'] <= write['input_consumption'][0], 'input_persistence_or_read_clock')
        need(consumed['raw_sha256']==sha(source) and consumed['receipt_sha256']==sha(raw['capture_receipt']),
             'input_consumption_identity')
    if 'mapping' in raw:
        need(contract.decode(raw['mapping'])==document and 'input_consumption' in raw
             and write['input_consumption'][1] <= write['mapping'][0], 'stored_mapping_or_clock')
    if value['status']!='issued_published_consumed':return observed,source_record,None
    issued=contract.decode(raw['issued']);pub=contract.decode(raw['publication']);consume=contract.decode(raw['consumption'])
    reads=contract.decode(raw['consumer_read_evidence'])
    compatible=[s for s in sessions if s['status']=='retained_started'
        and s['training_read_completed_epoch'] <= issued['computation_started_epoch']
        and s['training_persisted_epoch'] <= begun
        and s['started_epoch'] <= begun and (s['stopped_epoch'] is None or done<=s['stopped_epoch'])]
    need(compatible, 'no_original_training_artifact_read')
    need(write['mapping'][1] <= issued['computation_started_epoch'] <= issued['issued_epoch']
         <= pub['publication_started_epoch'] <= write['issued'][0], 'issue_compute_or_write_clock')
    need(write['issued'][1] <= pub['publication_completed_epoch'] <= write['publication'][0],
         'publication_before_durable_issue')
    ia,ib=read_evidence(reads['issued'],raw['issued'],'issued.json')
    pa,pb=read_evidence(reads['publication'],raw['publication'],'publication.json')
    need(write['publication'][1] <= consume['read_started_epoch'] <= ia <= ib <= pa <= pb
         <= consume['read_completed_epoch'] <= write['consumption'][0]
         and write['consumption'][1] <= write['consumer_read_evidence'][0], 'consumer_persistence_or_read_clock')
    need(value['issued_sha256']==issued['issued_sha256'] and value['nodes']==72
         and value['issued_epoch']==issued['issued_epoch']
         and value['publication_completed_epoch']==pub['publication_completed_epoch']
         and value['consumption_completed_epoch']==consume['read_completed_epoch'], 'pair_completion_chain_identity')
    need(issued['reference_price_epoch']==cycle and issued['input_consumption']==consumed, 'original_cycle_input_binding')
    observed['accepted_forecast']=True
    observed['training_session_ids']=[s['session_id'] for s in compatible]
    return observed,source_record,dict(parts=list(parts),cycle_epoch=cycle,instrument=pair,
        issued_sha256=issued['issued_sha256'])


def planned_slots(registry, cycles, attempts, issues, reports, *, as_of_epoch):
    """Schedule coverage only; missing slots never enter outcome denominators."""
    cutoff=contract.epoch(as_of_epoch)
    observed={r['cycle_epoch'] for r in cycles}
    retained={(r['cycle_epoch'],r['instrument']):r for r in attempts}
    invalid={(r['cycle_epoch'],r['instrument']) for r in issues}
    verified={(r['reference_price_epoch'],r['instrument']) for r in reports}
    slots=[]
    for reference in range(int(registry['first_reference_epoch']),int(registry['last_reference_epoch'])+1,300):
        for pair in contract.PAIRS:
            key=(reference,pair);row=retained.get(key);deadline=reference+contract.MAX_ENTRY_AGE_SEC
            if key in verified:status='verified_original_chain'
            elif reference>cutoff:status='scheduled_not_started'
            elif key in invalid:status='retained_evidence_invalid'
            elif row and row['status']!='partial_missing_pair_completion':status='retained_withheld_or_partial'
            elif cutoff<=deadline:status='original_publication_deadline_not_elapsed'
            elif reference not in observed:status='missing_entire_scheduled_cycle'
            else:status='missing_pair_completion'
            slots.append(dict(instrument=pair,reference_price_epoch=reference,
                original_publication_deadline_epoch=deadline,status=status,
                retained_pair_status=row['status'] if row else None,
                retained_pair_reason=row.get('reason_code') if row else None))
    return dict(as_of_epoch=cutoff,slots=slots,status_counts=dict(Counter(r['status'] for r in slots)),
        predeclared_slots=len(slots),scope='Schedule evidence only; absent or partial slots are not scored zeros.')


def evaluate(registry_path, expected_sha, *, clock=time.time):
    started=contract.epoch(clock());own=Path(__file__).read_bytes()
    registry,registry_sha=worker.load_registry(registry_path,expected_sha,clock=clock)
    fit_raw=files._read(Path(registry['fit_artifact_path']))
    reader=Reader(registry['output_root'],clock=clock)
    sessions=verify_sessions(reader,registry,registry_sha,fit_raw)
    sources=[];attempts=[];issued=[];issues=[];cycles=[]
    for name in reader.directories(('cycles',),r'[0-9]{1,12}',MAX_CYCLES):
        cycle=int(name);need(cycle%60==0,'cycle_grid')
        parts=('cycles',name);completion=None
        if reader.exists(parts,'cycle_completed.json'):
            completion=reader.json(parts,'cycle_completed.json');authority(completion)
            need(completion['registry_sha256']==registry_sha and completion['cycle_epoch']==cycle
                 and completion['broker_order_count']==0, 'cycle_identity_or_authority')
            need(cycle <= completion['started_epoch'] <= completion['completed_epoch'] <= contract.epoch(clock()),
                 'cycle_completion_clock')
        pair_results=[]
        for pair in contract.PAIRS:
            try:
                observed,source,forecast=inspect_pair(reader,registry,sessions,cycle,pair)
                if completion:
                    retained=[r for r in completion['pairs'] if r['instrument']==pair]
                    need(len(retained)==1 and reader.json((*parts,pair.lower()),'pair_completed.json')==retained[0],
                         'cycle_pair_record_mismatch')
                    need(completion['started_epoch'] <= observed['started_epoch'] <= observed['completed_epoch']
                         <= completion['completed_epoch'], 'cycle_pair_clock')
                attempts.append(observed);pair_results.append(observed)
                if source:sources.append(source)
                if forecast:issued.append(forecast)
            except (ValueError,KeyError,TypeError,OSError) as error:
                issues.append(dict(cycle_epoch=cycle,instrument=pair,reason_code=worker.reason(error)))
        if completion:
            need(len(completion['pairs'])==3,'cycle_pair_count')
            counts={name:sum(key in r.get('artifacts',{}) for r in completion['pairs'])
                for name,key in (('retained_issue_count','issued'),('publication_count','publication'),
                                 ('consumption_count','consumption'))}
            counts['complete_chain_count']=sum(r['status']=='issued_published_consumed' for r in completion['pairs'])
            need(all(type(completion.get(k)) is int and completion[k]==v for k,v in counts.items()),
                 'cycle_stage_count')
        else:counts=None
        cycles.append(dict(cycle_epoch=cycle,completion_present=completion is not None,
            retained_stage_counts=counts,worker_recorded_skipped_prior_capture_cycle_epochs=(
                completion.get('skipped_prior_capture_cycle_epochs',[]) if completion else None)))
    reports=[];source_selection=[]
    for forecast in issued:
        try:
            parts=forecast['parts'];pair=forecast['instrument']
            value=reader.json(parts,'issued.json');pub=reader.json(parts,'publication.json');consume=reader.json(parts,'consumption.json')
            raw=reader.read(parts,'source_raw.json');receipt=reader.json(parts,'capture_receipt.json')
            relevant=sorted([s for s in sources if s['instrument']==pair and s['complete_rows']],
                key=lambda s:(s['source_read_completed_epoch'],s['source_sha256'],s['internal_receipt_sha256']))
            selected={}
            for h in contract.labels.HORIZONS:
                target=forecast['cycle_epoch']+h*60
                for domain_start in (target-60,forecast['cycle_epoch']):
                    first=next((s for s in relevant if s['source_read_completed_epoch']>=target
                        and s['first_label_epoch']<=domain_start and s['last_label_epoch']>=target-60),None)
                    if first:selected[(first['source_sha256'],first['internal_receipt_sha256'])]=first
            captures=[]
            for s in selected.values():
                sp=s['parts'];future_raw=reader.read(sp,'source_raw.json');future_receipt=reader.json(sp,'capture_receipt.json')
                captures.append(outcomes.capture_future_m1(future_raw,future_receipt,registry['metadata'][pair],clock=clock))
            report=outcomes.score_distribution_from_captures(value,pub,consume,captures,
                input_raw=raw,input_receipt=receipt,metadata=registry['metadata'][pair],fit_raw=fit_raw,
                expected_source_bindings=registry['source_bindings'],as_of_epoch=contract.epoch(clock()),clock=clock)
            reports.append(report)
            source_selection.append(dict(issued_sha256=value['issued_sha256'],selected_source_count=len(selected),
                policy='Earliest original read within each required endpoint/path domain; union of selected sources only, not merged rows.'))
        except (ValueError,KeyError,TypeError,OSError) as error:
            issues.append(dict(cycle_epoch=forecast['cycle_epoch'],instrument=forecast['instrument'],reason_code=worker.reason(error)))
    end=contract.epoch(clock())
    aggregate=outcomes.aggregate_reports(reports,as_of_epoch=end)
    schedule=planned_slots(registry,cycles,attempts,issues,reports,as_of_epoch=end)
    worker.source_check(registry)
    need(sha(files._read(Path(registry_path)))==expected_sha and sha(files._read(Path(registry['fit_artifact_path'])))==registry['fit_artifact_sha256']
         and Path(__file__).read_bytes()==own, 'final_source_or_registry_changed')
    return dict(schema_version=SCHEMA,status='issues' if issues else 'passed',
        registry_sha256=registry_sha,source_bindings=registry['source_bindings'],helper_sha256=sha(own),
        verification_started_epoch=started,verification_completed_epoch=contract.epoch(clock()),
        sessions=sessions,cycles=cycles,attempts=attempts,sources=sources,source_selection=source_selection,
        scheduled_issue_dispositions=schedule,
        reports=reports,aggregate=aggregate,issues=issues,verified_files=list(reader.records.values()),
        total_read_bytes=reader.total,attempt_status_counts=dict(Counter(r['status'] for r in attempts)),
        verified_chain_count=len(reports),scheduled_attempts_observed=sum(r.get('scheduled_issue',False) for r in attempts),
        limits=['Bounded non-atomic immutable-file observation, not an account, live-fill or independent-trial audit.',
            'Partial/missing or failed pair records never become completed chains; source capture failures and raw absence remain visible.',
            'Every score retains actual derived evaluation clocks and original source read clocks; report cutoffs can differ during this audit.',
            'Session training-read compatibility is checked by retained clocks; cycle files do not carry a unique session ID.',
            'No source query, fit, policy selection, retrospective forecast issue or runtime write occurs.'],
        **outcomes.FLAGS)


def save(path,value):
    raw=json.dumps(value,sort_keys=True,indent=2,allow_nan=False).encode()+b'\n'
    with path.open('xb') as out:out.write(raw);out.flush();os.fsync(out.fileno())
    need(path.read_bytes()==raw,'output_readback')
    return dict(path=path.name,sha256=sha(raw),bytes=len(raw))


def main(argv=None):
    p=argparse.ArgumentParser();p.add_argument('--registry',required=True);p.add_argument('--expected-sha256',required=True)
    p.add_argument('--output-directory',required=True);args=p.parse_args(argv)
    dest=Path(args.output_directory).absolute()
    need(dest.parent==BASE and not dest.exists(),'fresh_external_output_directory')
    files._safe_components(BASE);dest.mkdir()
    try:
        result=evaluate(args.registry,args.expected_sha256)
        report_refs=[]
        for i,report in enumerate(result.pop('reports')):
            report_refs.append(save(dest/f'distribution_{i:04}.json',report))
        result['distribution_report_files']=report_refs
        ref=save(dest/'RETAINED_RISK_DISTRIBUTION_EVALUATION.json',result)
        print(json.dumps(dict(status=result['status'],path=str(dest/ref['path']),sha256=ref['sha256'],
            verified_chain_count=result['verified_chain_count'],issues=len(result['issues']))))
        return 0 if result['status']=='passed' else 1
    except Exception as error:
        ref=save(dest/'EVALUATION_FAILED.json',dict(status='failed',reason_code=worker.reason(error),
            observed_epoch=time.time(),helper_sha256=sha(Path(__file__).read_bytes()),**outcomes.FLAGS))
        print(json.dumps(dict(status='failed',path=str(dest/ref['path']))));return 1


if __name__=='__main__':raise SystemExit(main())
