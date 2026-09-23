from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import pytest

sys.path.insert(0,str(Path(__file__).parent))
import evaluate_retained_risk_distributions_v1 as e
import test_oanda_m1_mba_research_capture_v1 as capture_fixture

REFERENCE=int(datetime(2026,9,9,8,1,tzinfo=timezone.utc).timestamp())


class Clock:
    def __init__(self,now):self.now=now
    def __call__(self):self.now+=.001;return self.now


def write(path,value):path.write_bytes(e.contract.canonical(value))


@pytest.fixture
def world(tmp_path,monkeypatch):
    original_root=e.worker.ROOT;root=tmp_path/'trad';root.mkdir()
    bindings={}
    for name in e.worker.SOURCE_NAMES:
        raw=(original_root/name).read_bytes();(root/name).write_bytes(raw);bindings[name]=e.sha(raw)
    monkeypatch.setattr(e.worker,'ROOT',root)
    fit={}
    for pair in e.contract.PAIRS:
        for h in e.contract.labels.HORIZONS:
            for label in e.contract.labels.CONTINUOUS_LABELS:
                fit[f'{pair}/{h}/{label}']=dict(status='fitted_training_quantiles',
                    fit_scope='training_only_common_support',method='inverted_cdf',
                    original_probability_available=False,quantiles=list(e.contract.QUANTILES),training_rows=300,
                    training_origin_sha256='a'*64,training_reference_max_epoch=REFERENCE-100000,
                    training_target_max_epoch=REFERENCE-90000,
                    empirical=[-1.,0.,1.] if label=='signed_terminal_bps' else [0.,1.,2.],
                    volatility_normalized=[-.1,0.,.1] if label=='signed_terminal_bps' else [0.,.1,.2])
    fit_raw=e.contract.canonical(dict(created_epoch=REFERENCE-80000,fits=fit,**e.contract.labels.FLAGS))
    fit_path=tmp_path/'fixed_fit.json';fit_path.write_bytes(fit_raw)
    monkeypatch.setattr(e.contract,'TRAINING_ARTIFACT_SHA256',e.sha(fit_raw))
    reg=dict(schema_version=e.worker.SCHEMA,registry_id=e.worker.REGISTRY_ID,cohort_id=e.contract.COHORT_ID,
        created_epoch=REFERENCE-60,first_reference_epoch=REFERENCE,last_reference_epoch=e.worker.LAST_REFERENCE,
        collection_end_epoch=e.worker.COLLECTION_END,capture_cadence_sec=60,capture_start_delay_sec=2,
        source_bindings=bindings,metadata={p:capture_fixture.metadata(p) for p in e.contract.PAIRS},
        fit_artifact_path=str(fit_path),fit_artifact_sha256=e.sha(fit_raw),
        output_root=str(root/'data/oanda_training_manager'/e.worker.REGISTRY_ID),
        registered_methods=list(e.contract.METHODS),registered_labels=list(e.contract.labels.CONTINUOUS_LABELS),
        registered_horizons_minutes=list(e.contract.labels.HORIZONS),
        forecast_policy=dict(all_methods_and_labels_required=True,maximum_entry_age_sec=20,
            minimum_same_session_rows=204,original_targets_retimed=False,refit_or_method_selection=False,
            risk_routing_to_management=False,missing_prices_filled=False,late_issue_backfill=False),**e.contract.AUTHORITY)
    path=tmp_path/'registry.json';write(path,reg);regsha=e.sha(path.read_bytes())
    response_state={'empty_failure':False}
    class Session:
        def __init__(self):self.trust_env=True
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def mount(self,*args):pass
        def get(self,url,**kwargs):
            pair=url.split('/')[-2]
            if response_state['empty_failure']:return capture_fixture.Response(b'',status=503)
            return capture_fixture.Response(capture_fixture.raw_of(capture_fixture.payload(pair)))
    monkeypatch.setattr(e.mapper.requests,'Session',Session)
    monkeypatch.setattr(e.mapper,'_token',lambda _: 'test-only-token')
    def cycle(epoch,failed=False):
        monkeypatch.setattr(capture_fixture,'NOW',epoch+1.)
        response_state['empty_failure']=failed
        e.worker.run(reg,regsha,clock=Clock(epoch+2.),sleep=lambda _:None,once=True)
    cycle(REFERENCE)
    return dict(registry=reg,path=path,sha=regsha,root=Path(reg['output_root']),cycle=cycle)


def evaluate(world):return e.evaluate(world['path'],world['sha'],clock=Clock(REFERENCE+10000.))


def pair_path(world,pair='eur_usd',cycle=REFERENCE):return world['root']/'cycles'/str(cycle)/pair


def mutate_pair(world,edit,pair='eur_usd',cycle_epoch=REFERENCE):
    path=pair_path(world,pair,cycle_epoch)/'pair_completed.json';value=json.loads(path.read_bytes());edit(value);write(path,value)
    cycle=path.parent.parent/'cycle_completed.json';body=json.loads(cycle.read_bytes())
    body['pairs']=[value if x['instrument'].lower()==pair else x for x in body['pairs']];write(cycle,body)


def test_real_worker_persistence_all_three_chains_pending_then_exact_outcomes(world):
    initial=evaluate(world)
    assert initial['status']=='passed' and initial['verified_chain_count']==3
    assert all(r['status_counts']=={'unavailable':72} for r in initial['reports'])
    world['cycle'](REFERENCE+3660)
    result=evaluate(world)
    assert result['status']=='passed' and result['verified_chain_count']==3
    assert all(r['status_counts']=={'scored':72} for r in result['reports'])
    assert result['aggregate']['unique_pair_origin_count']==3
    assert result['aggregate']['independent_sample_size'] is None
    assert all(r['unique_capture_count']==1 for r in result['reports'])


def test_counterfeit_durable_source_clock_cannot_precede_input_read(world):
    path=pair_path(world)/'input_consumption.json';inp=json.loads(path.read_bytes())
    mutate_pair(world,lambda value:value['artifacts']['capture_receipt'].update(
        write_readback_completed_epoch=inp['input_consumption']['read_started_epoch']+.01))
    result=evaluate(world)
    assert result['status']=='issues'
    assert any('input_persistence_or_read_clock' in r['reason_code'] for r in result['issues'])
    assert result['verified_chain_count']==2


def test_publication_cannot_claim_completion_before_actual_issue_readback(world):
    def edit(value):
        value['artifacts']['issued']['write_readback_completed_epoch']=value['publication_completed_epoch']+.001
    mutate_pair(world,edit)
    result=evaluate(world)
    assert any('publication_before_durable_issue' in r['reason_code'] for r in result['issues'])
    assert result['verified_chain_count']==2


def test_counterfeit_independent_consumer_read_is_refused(world):
    path=pair_path(world)/'consumer_read_evidence.json';value=json.loads(path.read_bytes())
    value['publication']['read_started_epoch']=value['issued']['read_started_epoch']-1
    write(path,value)
    mutate_pair(world,lambda row:row['artifacts']['consumer_read_evidence'].update(
        bytes_sha256=e.sha(path.read_bytes()),byte_count=len(path.read_bytes())))
    assert any('consumer_persistence_or_read_clock' in x['reason_code'] for x in evaluate(world)['issues'])


def test_missing_completed_pair_cannot_hide_behind_completed_cycle(world):
    path=pair_path(world)/'pair_completed.json';path.rename(path.with_suffix('.retained_missing'))
    result=evaluate(world);assert result['status']=='issues' and result['verified_chain_count']==2


def test_unfinished_cycle_and_pair_are_explicit_not_fabricated_forecast(world):
    folder=world['root']/'cycles'/str(REFERENCE+60);folder.mkdir()
    result=evaluate(world)
    partial=[x for x in result['attempts'] if x['cycle_epoch']==REFERENCE+60]
    assert len(partial)==3 and all(x['status']=='partial_missing_pair_completion' for x in partial)
    assert result['verified_chain_count']==3


def test_failed_empty_raw_capture_keeps_original_failure_and_no_source(world):
    world['cycle'](REFERENCE+60,failed=True)
    result=evaluate(world)
    failures=[x for x in result['attempts'] if x['cycle_epoch']==REFERENCE+60]
    assert all(x['source_disposition']=='retained_failed_capture' for x in failures)
    assert all(x['original_capture_failure']['raw_bytes']==0 for x in failures)
    assert all(x['original_capture_failure']['reason']=='m1_http_status' for x in failures)
    assert len(result['sources'])==3 and result['verified_chain_count']==3


def test_partial_publication_counts_are_not_complete_chain_counts(world,monkeypatch):
    original=e.contract.consumption_receipt
    def failed(*args,**kwargs):raise ValueError('fixture_consumer_refusal')
    monkeypatch.setattr(e.contract,'consumption_receipt',failed)
    world['cycle'](REFERENCE+300)
    monkeypatch.setattr(e.contract,'consumption_receipt',original)
    result=evaluate(world)
    assert result['status']=='passed' and result['verified_chain_count']==3
    counts=next(x for x in result['cycles'] if x['cycle_epoch']==REFERENCE+300)['retained_stage_counts']
    assert counts==dict(retained_issue_count=3,publication_count=3,consumption_count=0,complete_chain_count=0)
    assert all(x['phase']=='published' and not x['accepted_forecast'] for x in result['attempts']
               if x['cycle_epoch']==REFERENCE+300)


@pytest.mark.parametrize('change,reseal,reason',[
    (lambda x:x.update(error_reason='changed_reason'),False,'capture_receipt_seal'),
    (lambda x:x.update(source_bindings={}),True,'capture_receipt_identity'),
    (lambda x:x.update(capture_completed_epoch=REFERENCE+100000),True,'failed_capture_persistence_clock')])
def test_failed_capture_still_requires_original_bound_receipt(world,change,reseal,reason):
    cycle=REFERENCE+60;world['cycle'](cycle,failed=True)
    path=pair_path(world,cycle=cycle)/'capture_receipt.json';body=json.loads(path.read_bytes())
    change(body)
    if reseal:body['receipt_sha256']=e.contract.digest({k:v for k,v in body.items() if k!='receipt_sha256'})
    write(path,body)
    mutate_pair(world,lambda r:r['artifacts']['capture_receipt'].update(
        bytes_sha256=e.sha(path.read_bytes()),byte_count=len(path.read_bytes())),cycle_epoch=cycle)
    result=evaluate(world)
    assert any(reason in x['reason_code'] for x in result['issues'])


def test_partial_inventory_preserves_unadmitted_issue_file(world):
    folder=world['root']/'cycles'/str(REFERENCE+60)/'eur_usd';folder.mkdir(parents=True)
    original=(pair_path(world)/'issued.json').read_bytes();(folder/'issued.json').write_bytes(original)
    result=evaluate(world)
    partial=next(x for x in result['attempts'] if x['cycle_epoch']==REFERENCE+60 and x['instrument']=='EUR_USD')
    assert partial['retained_unadmitted_files']==[dict(file='issued.json',sha256=e.sha(original),bytes=len(original))]
    assert result['verified_chain_count']==3


def test_original_training_persisted_bytes_and_clock_required(world):
    path=next((world['root']/'sessions').glob('*/started.json'));value=json.loads(path.read_bytes())
    value['training_artifact_persistence']['write_started_epoch']=value['training_artifact_read']['read_started_epoch']-1
    write(path,value)
    with pytest.raises(ValueError,match='session_training_persistence_clock'):evaluate(world)


def test_invoked_without_original_request_start_does_not_become_attempt(world):
    mutate_pair(world,lambda r:r.update(capture_attempted=False))
    result=evaluate(world)
    assert any('capture_attempt_receipt_mismatch' in r['reason_code'] for r in result['issues'])
    assert result['verified_chain_count']==2


def test_source_mutation_refuses_audit_before_any_scoring(world):
    source=e.worker.ROOT/next(iter(e.worker.SOURCE_NAMES));source.write_bytes(source.read_bytes()+b'\n')
    with pytest.raises(ValueError,match='source_changed'):evaluate(world)


def test_training_read_receipt_hash_is_checked(world):
    path=next((world['root']/'sessions').glob('*/started.json'));value=json.loads(path.read_bytes())
    value['training_artifact_read']['bytes_sha256']='a'*64;write(path,value)
    with pytest.raises(ValueError,match='read_receipt_bytes'):evaluate(world)


def test_outside_output_exclusive_no_runtime_or_network_calls(world,tmp_path,monkeypatch):
    def forbidden(*args,**kwargs):raise AssertionError('unexpected_write_or_GET')
    monkeypatch.setattr(e.mapper,'capture_once',forbidden)
    monkeypatch.setattr(e.files,'_write_exclusive',forbidden)
    monkeypatch.setattr(e.worker,'run',forbidden)
    result=evaluate(world);assert result['status']=='passed'
    with pytest.raises(ValueError,match='fresh_external_output_directory'):
        e.main(['--registry',str(world['path']),'--expected-sha256',world['sha'],
            '--output-directory',str(e.TRAD/'unexpected')])
    path=tmp_path/'summary.json';e.save(path,{'status':'fixture'})
    with pytest.raises(FileExistsError):e.save(path,{'status':'overwrite'})


def test_schedule_inventory_wholly_absent_slots_and_not_yet_elapsed_deadlines():
    reg=dict(first_reference_epoch=REFERENCE,last_reference_epoch=REFERENCE+600)
    before=e.planned_slots(reg,[],[],[],[],as_of_epoch=REFERENCE+19)
    assert before['status_counts']==dict(original_publication_deadline_not_elapsed=3,scheduled_not_started=6)
    after=e.planned_slots(reg,[],[],[],[],as_of_epoch=REFERENCE+20)
    assert after['status_counts']==dict(missing_entire_scheduled_cycle=3,scheduled_not_started=6)
    assert after['predeclared_slots']==9


def test_schedule_inventory_keeps_invalid_partial_and_nonissue_minutes_distinct():
    reg=dict(first_reference_epoch=REFERENCE,last_reference_epoch=REFERENCE)
    attempts=[dict(cycle_epoch=REFERENCE,instrument='GBP_USD',status='withheld_or_partial',reason_code='not_ready'),
              dict(cycle_epoch=REFERENCE+60,instrument='USD_JPY',status='captured_only')]
    result=e.planned_slots(reg,[dict(cycle_epoch=REFERENCE)],attempts,
        [dict(cycle_epoch=REFERENCE,instrument='EUR_USD')],[],as_of_epoch=REFERENCE+120)
    assert result['status_counts']==dict(retained_evidence_invalid=1,retained_withheld_or_partial=1,missing_pair_completion=1)
    assert all(r['reference_price_epoch']==REFERENCE for r in result['slots'])


@pytest.mark.parametrize('failed',[False,True])
def test_prior_capture_cannot_be_relabelled_as_request_of_later_pair(world,failed):
    cycle=REFERENCE
    if failed:
        cycle+=60;world['cycle'](cycle,failed=True)
    path=pair_path(world,cycle=cycle)/'capture_receipt.json';receipt=json.loads(path.read_bytes())
    mutate_pair(world,lambda r:r.update(started_epoch=receipt['request_started_epoch']+.0005),cycle_epoch=cycle)
    result=evaluate(world)
    assert any('original_request_outside_pair_or_collection_window' in x['reason_code'] for x in result['issues'])


def test_present_original_request_must_leave_full_registered_request_budget(world):
    registry=deepcopy(world['registry']);registry['collection_end_epoch']=REFERENCE+10
    reader=e.Reader(world['root'],clock=Clock(REFERENCE+10000))
    with pytest.raises(ValueError,match='original_request_outside_pair_or_collection_window'):
        e.inspect_pair(reader,registry,[],REFERENCE,'EUR_USD')
