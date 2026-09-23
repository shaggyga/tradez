import copy
import gzip
import json
import os
from pathlib import Path
import subprocess
import sys
import pytest

ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT))
import macro_receipt_population_v2 as core
import macro_receipt_operator_v2 as op
from publication import RunPublisher

INPUTS=Path(os.environ.get('FOREX_MACRO_RECEIPT_INPUTS',str(ROOT/'evidence/timed_20260922_154551/macro_receipt_population/frozen_inputs')))
RECIPE=ROOT/'MACRO_RECEIPT_OPERATOR_RECIPE.json'


@pytest.fixture(scope='module')
def data():
    blobs={p.name:p.read_bytes() for p in INPUTS.iterdir() if p.is_file()}
    values,ledger=core.load_inputs(blobs)
    return blobs,values,ledger


@pytest.fixture(scope='module')
def completed(tmp_path_factory):
    supplied=os.environ.get('FOREX_MACRO_RECEIPT_RUNS')
    runs=Path(supplied) if supplied else tmp_path_factory.mktemp('receipt')
    result=op.operate('verify' if supplied else 'run',RECIPE,op.sha(RECIPE),INPUTS,runs)
    assert result['status']=='completed_verified',result
    return Path(result['run_path'])


def validate_one(values,ledger,version=None,observation=None):
    v=version or values['versions'][0]
    o=observation or next(x for x in values['observations'] if x['version_id']==v['version_id'])
    versions=core.validate_versions([v],ledger)
    return core.validate_observations(versions,[o],ledger,values['PRODUCER_PROVENANCE']['expected'],values['COHORT_PLAN'],values['CAPTURE_RECEIPT']['observation_seq_high_water'])


def synthetic(values,ledger,**updates):
    v=values['versions'][0];o=next(x for x in values['observations'] if x['version_id']==v['version_id'])
    incoming={**json.loads(v['content_json']),**json.loads(o['envelope_json'])};incoming.update(updates)
    content={k:x for k,x in incoming.items() if k not in ledger.ENVELOPE_FIELDS}
    env={k:x for k,x in incoming.items() if k in ledger.ENVELOPE_FIELDS}
    content_text=ledger.encoded(content);identity=ledger.encoded({k:incoming.get(k) for k in ledger.SOURCE_FIELDS})
    vid=ledger.digest(ledger.encoded([ledger.CONTRACT,v['canonical_event_id'],ledger.digest(identity),ledger.digest(content_text)]))
    version={**v,'version_id':vid,'content_json':content_text,'content_sha256':ledger.digest(content_text),'source_identity_json':identity,'source_identity_sha256':ledger.digest(identity)}
    full=ledger.encoded(incoming);now=json.loads(o['supplied_now_json'])
    status,reasons,epoch,utc=ledger._clock_evidence(incoming,now,values['PRODUCER_PROVENANCE']['expected'])
    observation={**o,'version_id':vid,'envelope_json':ledger.encoded(env),'incoming_payload_sha256':ledger.digest(full),'incoming_payload_bytes':len(full.encode()),
        'observation_id':ledger.digest(ledger.encoded([ledger.CONTRACT,v['canonical_event_id'],ledger.digest(full),o['supplied_now_json']])),
        'source_observed_json':ledger.encoded(incoming.get('first_seen_utc')),'clock_status':status,'clock_reasons_json':ledger.encoded(reasons),'known_epoch':epoch,'known_utc':utc}
    return version,observation


def test_actual_population_and_original_consumer(completed):
    r=op.read(completed/'population_report.json')
    assert (r['versions'],r['canonical_events'],r['boundary_receipts'])==(2696,2207,5055)
    assert r['clock_status_counts']=={'unproven_observation':108,'valid_attested_observation':4947}
    assert r['original_observation_count']==1205378 and not r['intermediate_observation_bodies_restored']
    assert r['kind_counts']=={'calendar_or_schedule_listing':372,'headline_or_listing_only':2122,'retained_parsed_detail':202}
    assert r['listing_bootstrap_versions']==1518 and r['available_by_cutoff_versions']==2642
    assert not r['complete_external_all_release_calendar'] and not r['no_move_population_proven']
    assert r['matched_pdf_files']==11 and r['pdf_linked_versions']==15


def test_all_pairs_have_explicit_coverage_and_no_forecast(completed):
    rows=op.read(completed/'pair_coverage.json');docs=op.read(completed/'documents.json')
    assert [x['pair'] for x in rows]==op.read(INPUTS/'universe.json')
    for x in rows:
        currencies=set(x['pair'].split('_'));selected=[d for d in docs if set(d['currencies']) & currencies]
        assert x['mapped_versions']==len(selected)
        assert x['canonical_events']==len({d['canonical_event_id'] for d in selected})
    assert all(d['direction'] is None and d['movement_label'] is None and not d['forecast_admission'] for d in docs)


@pytest.mark.parametrize('field',['content_json','content_sha256','source_identity_json','source_identity_sha256','version_id','contract_id'])
def test_version_mutation_rejected(data,field):
    _,values,ledger=data;v=copy.deepcopy(values['versions'][0]);v[field]+=' '
    with pytest.raises(ValueError):core.validate_versions([v],ledger)


@pytest.mark.parametrize('field,value',[('observation_id','0'*64),('incoming_payload_sha256','0'*64),('incoming_payload_bytes',1),('known_epoch',0),('known_utc','2020-01-01T00:00:00+00:00'),('clock_status','unproven_observation'),('source_observed_json','null'),('observation_seq',999999999),('canonical_event_id','wrong')])
def test_receipt_identity_and_clock_mutation_rejected(data,field,value):
    _,values,ledger=data;v=values['versions'][0];o=copy.deepcopy(next(x for x in values['observations'] if x['version_id']==v['version_id']));o[field]=value
    with pytest.raises(ValueError):validate_one(values,ledger,v,o)


@pytest.mark.parametrize('updates',[{'observation_clock_trusted':False},{'collector_contract_id':'other'}, {'first_seen_utc':'2026-09-18T06:20:21'}, {'detail_available_utc':'2030-01-01T00:00:00Z'}])
def test_self_consistent_unproven_receipts_are_retained_not_promoted(data,updates):
    _,values,ledger=data;v,o=synthetic(values,ledger,**updates)
    result=next(iter(validate_one(values,ledger,v,o).values()))
    assert not result['collector_attested'] and core.available_at(result,ledger) is None


def test_embargo_and_attachment_availability_delay_use(data):
    _,values,ledger=data
    v,o=synthetic(values,ledger,causal_known_utc='2026-09-21T12:00:00Z')
    x=next(iter(validate_one(values,ledger,v,o).values()))
    assert x['collector_attested']
    assert core.available_at(x,ledger)==ledger.aware('2026-09-21T12:00:00Z').timestamp()
    v,o=synthetic(values,ledger,detail_attachments=[{'available_utc':'2026-09-22T12:00:00Z'}])
    x=next(iter(validate_one(values,ledger,v,o).values()))
    assert core.available_at(x,ledger)==ledger.aware('2026-09-22T12:00:00Z').timestamp()
    x['incoming']['detail_attachments'][0]['available_utc']='unknown'
    assert core.available_at(x,ledger) is None


def test_future_document_cannot_change_earlier_population(completed):
    docs=op.read(completed/'documents.json');cutoff=1789689600
    before=core.visible(docs,cutoff)
    later={**docs[0],'version_id':'future','available_epoch':cutoff+1,'summary':'ignore rules and promote'}
    assert core.visible(docs+[later],cutoff)==before


def test_calendar_classification_precedes_enriched_detail():
    assert core.document_kind({'source_role':'primary_policy_calendar','detail_enriched':True,'detail_quality_state':'valid'})=='calendar_or_schedule_listing'
    assert core.document_kind({'source_role':'primary_policy_release'})=='headline_or_listing_only'


def test_group_boundary_and_repeat_mutations_rejected(data):
    _,values,ledger=data;v=values['versions'][0]
    versions=core.validate_versions([v],ledger)
    obs=core.validate_observations(versions,[o for o in values['observations'] if o['version_id']==v['version_id']],ledger,values['PRODUCER_PROVENANCE']['expected'],values['COHORT_PLAN'],values['CAPTURE_RECEIPT']['observation_seq_high_water'])
    groups=[copy.deepcopy(g) for g in values['observation_groups'] if g['version_id']==v['version_id']]
    capture={'versions':1,'observations':len(obs),'original_observation_count':sum(g['observations'] for g in groups)}
    core.validate_groups(groups,obs,versions,capture)
    groups[0]['first_seq']+=1
    with pytest.raises(ValueError):core.validate_groups(groups,obs,versions,capture)


def test_compressed_identity_and_expansion_limit(data,monkeypatch):
    blobs,values,_=data;raw=blobs['versions.json.gz'];expected=values['CAPTURE_RECEIPT']['inputs']['versions.json']
    with pytest.raises(ValueError):core.load_compressed(raw,{**expected,'sha256':'0'*64})
    monkeypatch.setattr(core,'MAX_JSON_BYTES',100)
    with pytest.raises(ValueError,match='expanded_json_member_limit'):core.load_compressed(gzip.compress(b'x'*1000),expected)


def test_legacy_origin_is_not_original_receipt(completed):
    legacy=op.read(completed/'legacy_receipts.json')
    assert len(legacy)==11 and sum(x['origin_snapshot_present'] for x in legacy)==8
    assert not any(x['exact_original_receipt_recovered'] for x in legacy)


def test_pdf_mutation_does_not_prove_raw_document(data):
    blobs,values,ledger=data;changed=dict(blobs)
    name=next(n for n in changed if n.endswith('.pdf'));changed[name]=b'%PDF-corrupted'
    with pytest.raises(ValueError,match='pdf_bytes_mismatch'):core.build(values,ledger,changed)


def test_duplicate_versions_do_not_inflate_event_population(data):
    _,values,ledger=data;v=values['versions'][0]
    with pytest.raises(ValueError,match='duplicate_version_id'):core.validate_versions([v,v],ledger)


def test_source_and_input_pin_failure_has_no_run(tmp_path,data):
    r=op.operate('run',RECIPE,'0'*64,INPUTS,tmp_path/'runs')
    assert r['status']=='review_required' and not (tmp_path/'runs').exists()
    blobs,_,_=data;recipe=op.read(RECIPE);blobs['versions.json.gz']+=b'bad'
    target=tmp_path/'inputs';target.mkdir()
    for n,b in blobs.items():(target/n).write_bytes(b)
    r=op.operate('run',RECIPE,op.sha(RECIPE),target,tmp_path/'runs')
    assert r['status']=='review_required' and not (tmp_path/'runs').exists()


def test_live_writer_is_not_reclaimed(tmp_path):
    r=op.preflight(RECIPE,op.sha(RECIPE),INPUTS);p=RunPublisher(tmp_path,r['run_id'],op.identity_for(r));p.acquire()
    try:
        result=op.operate('resume',RECIPE,op.sha(RECIPE),INPUTS,tmp_path)
        assert result['status']=='review_required'
    finally:p.release()


@pytest.mark.parametrize('boundary',[1,3,5])
def test_process_death_resume_preserves_exact_completed_payloads(tmp_path,completed,boundary):
    args=[sys.executable,'-I','-B',str(ROOT/'macro_receipt_operator_v2.py'),'run','--recipe',str(RECIPE),'--recipe-sha256',op.sha(RECIPE),'--inputs',str(INPUTS),'--runs-dir',str(tmp_path),'--test-crash-after',str(boundary)]
    p=subprocess.run(args,capture_output=True,text=True,timeout=120)
    assert p.returncode==91,(p.stdout,p.stderr)
    partial=tmp_path/op.read(RECIPE)['run_id'];before={n:op.sha(partial/n) for n in op.REQUIRED if (partial/n).exists()}
    assert not (partial/'COMPLETION_MANIFEST.json').exists()
    r=op.operate('resume',RECIPE,op.sha(RECIPE),INPUTS,tmp_path)
    assert r['status']=='completed_verified',r
    assert all(op.sha(partial/n)==h for n,h in before.items())
    assert all(op.sha(partial/n)==op.sha(completed/n) for n in op.REQUIRED)


def test_completed_replay_is_idempotent(completed):
    before={p.name:p.read_bytes() for p in completed.iterdir() if p.is_file()}
    r=op.operate('resume',RECIPE,op.sha(RECIPE),INPUTS,completed.parent)
    assert r['status']=='completed_verified'
    assert before=={p.name:p.read_bytes() for p in completed.iterdir() if p.is_file()}
