"""Synthetic immutable-I/O boundaries; no broker, news or market data involved."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import threading

import pytest

import oanda_forecast_curve_contract_v1 as c
import oanda_forecast_curve_file_store_v1 as store

SOURCES = {'fixture.py':'a'*64}


def curve():
    policy=c.make_policy(native_horizons_sec=[60,120],maximum_reference_age_sec=30,
        maximum_build_sec=10,maximum_issue_delay_sec=10,maximum_publication_delay_sec=10,
        maximum_decision_age_sec=300,minimum_remaining_sec=0)
    prepared=c.prepare_curve(instrument='EUR_USD',pip_size='0.0001',forecast_cohort='file_fixture',
        model_sha256='b'*64,feature_version='file_fixture',source_bindings=SOURCES,
        input_capture_sha256='c'*64,input_available_epoch=1201,reference_epoch=1200,
        reference_label_epoch=1140,reference_price='1.1000',reference_price_kind='mid_close',
        bar_duration_sec=60,model_fitted_epoch=1000,computation_started_epoch=1201,
        computed_epoch=1202,points=[dict(horizon_sec=h,target_epoch=1200+h,target_label_epoch=1140+h,
            model_id='fixture',predicted_signed_pips='3',probability_up='0.6',
            probability_scope='original_unconditional_uncalibrated') for h in (60,120)],
        policy=policy,computation_sha256='d'*64)
    return c.issue_curve(prepared,expected_source_bindings=SOURCES,clock=lambda:1203)


def clock(*values):
    iterator=iter(values)
    return lambda:next(iterator)


def publish(root, **kwargs):
    args=dict(expected_source_bindings=SOURCES,clock=clock(1204,1205,1206))
    args.update(kwargs)
    return store.publish_curve(root,curve(),**args)


def consume(root, descriptor, **kwargs):
    args=dict(expected_source_bindings=SOURCES,clock=lambda:1207)
    args.update(kwargs)
    return store.consume_published_curve(root,descriptor,**args)


def directory(root, result):
    return root/'curves'/result['descriptor']['curve_sha256']


def reseal_descriptor(value):
    value['descriptor_sha256']=c.content_hash({k:v for k,v in value.items() if k!='descriptor_sha256'})


def test_complete_files_are_canonical_and_observation_clocks_are_separate(tmp_path):
    published=publish(tmp_path)
    read=consume(tmp_path,published['descriptor'],persist_consumption=True,clock=clock(1207,1208))
    assert published['existing_record'] is False
    assert published['publication']['publication_started_epoch']==1204
    assert published['publication']['publication_completed_epoch']==1205
    assert published['receipt_persisted_observed_epoch']==1206
    assert read['curve']==curve()
    assert read['consumption']['available_epoch']==1207
    assert read['consumption_persisted_observed_epoch']==1208
    assert read['consumption_record_created'] is True
    assert c.validate_consumption(read['curve'],read['publication'],read['consumption'],expected_source_bindings=SOURCES)==read['consumption']
    for path in tmp_path.rglob('*.json'):
        assert c.canonical_bytes(json.loads(path.read_bytes()))==path.read_bytes()
    assert len(list(tmp_path.rglob('*.json')))==3


def test_existing_publication_preserves_original_bytes_and_clocks(tmp_path):
    first=publish(tmp_path)
    before={str(p):p.read_bytes() for p in tmp_path.rglob('*.json')}
    second=publish(tmp_path,clock=clock(1400,1401))
    assert second['existing_record'] is True
    assert second['publication']==first['publication']
    assert second['descriptor']==first['descriptor']
    assert second['receipt_persisted_observed_epoch']==1401
    assert {str(p):p.read_bytes() for p in tmp_path.rglob('*.json')}==before
    with pytest.raises(c.CurveContractError,match='curve_too_old_at_consumption'):
        consume(tmp_path,second['descriptor'],clock=lambda:1600)


def test_optional_consumption_record_is_immutable_and_idempotent(tmp_path):
    published=publish(tmp_path)
    first=consume(tmp_path,published['descriptor'],persist_consumption=True,clock=clock(1207,1208))
    second=consume(tmp_path,published['descriptor'],persist_consumption=True,clock=clock(1207,1209))
    assert first['consumption']==second['consumption']
    assert second['consumption_record_created'] is False
    assert len(list(tmp_path.rglob('*.json')))==3
    path=tmp_path/'consumptions'/(first['consumption']['consumption_sha256']+'.json')
    assert path.read_bytes()==c.canonical_bytes(first['consumption'])


def test_realistic_windows_root_keeps_full_hash_receipts_under_path_limit(tmp_path):
    # A realistic 150-character root permits curve.json but the former doubly
    # hashed consumption layout would exceed Windows' legacy 260-char limit.
    extension=max(1,149-len(str(tmp_path)))
    root=tmp_path/('r'*extension)
    published=publish(root)
    read=consume(root,published['descriptor'],persist_consumption=True,clock=clock(1207,1208))
    checksum=read['consumption']['consumption_sha256']
    old=root/'curves'/published['descriptor']['curve_sha256']/'consumptions'/(checksum+'.json')
    current=root/'consumptions'/(checksum+'.json')
    assert len(str(old))>260 and len(str(current))<260
    assert current.read_bytes()==c.canonical_bytes(read['consumption'])
    assert checksum==read['consumption']['consumption_sha256']
    assert consume(root,published['descriptor'])['curve']==curve()


def test_plain_read_has_no_filesystem_mutation(tmp_path):
    published=publish(tmp_path)
    before={str(p):p.stat().st_mtime_ns for p in tmp_path.rglob('*')}
    consume(tmp_path,published['descriptor'])
    assert {str(p):p.stat().st_mtime_ns for p in tmp_path.rglob('*')}==before


@pytest.mark.parametrize('field,value',[
    ('schema_version','other'),('curve_sha256','../outside'),('curve_sha256','a'*63),
    ('publication_sha256','a'*64),('persisted_curve_bytes_sha256','b'*64),
    ('publication_bytes_sha256','b'*64),('descriptor_sha256','0'*64),
    ('research_only',False),('can_place_orders',True),('execution_eligible',1),
])
def test_descriptor_tamper_is_withheld_without_reading_other_paths(tmp_path,field,value):
    published=publish(tmp_path)
    descriptor=deepcopy(published['descriptor'])
    descriptor[field]=value
    if field!='descriptor_sha256':
        reseal_descriptor(descriptor)
    with pytest.raises(c.CurveContractError):
        consume(tmp_path,descriptor)


def test_descriptor_cannot_supply_a_path(tmp_path):
    descriptor=publish(tmp_path)['descriptor']
    descriptor['path']='../outside.json'
    reseal_descriptor(descriptor)
    with pytest.raises(c.CurveContractError,match='descriptor_shape'):
        consume(tmp_path,descriptor)


def test_source_mismatch_prevents_any_publication(tmp_path):
    with pytest.raises(c.CurveContractError,match='unregistered_source_bindings'):
        publish(tmp_path,expected_source_bindings={'other.py':'e'*64})
    assert not list(tmp_path.iterdir())


def test_source_mismatch_rejects_existing_consumer(tmp_path):
    descriptor=publish(tmp_path)['descriptor']
    with pytest.raises(c.CurveContractError,match='unregistered_source_bindings'):
        consume(tmp_path,descriptor,expected_source_bindings={'other.py':'e'*64})


@pytest.mark.parametrize('filename',['curve.json','publication.json'])
def test_corrupt_or_truncated_immutable_file_is_rejected_and_not_replaced(tmp_path,filename):
    published=publish(tmp_path)
    path=directory(tmp_path,published)/filename
    path.write_bytes(b'{')
    with pytest.raises(c.CurveContractError):
        consume(tmp_path,published['descriptor'])
    with pytest.raises(c.CurveContractError):
        publish(tmp_path,clock=lambda:1207)
    assert path.read_bytes()==b'{'


def test_missing_receipt_means_retained_orphan_not_success(tmp_path,monkeypatch):
    original=store._write_exclusive
    def fail_receipt(path,raw):
        if path.name=='publication.json':
            raise c.CurveContractError('synthetic_publication_failure')
        return original(path,raw)
    monkeypatch.setattr(store,'_write_exclusive',fail_receipt)
    with pytest.raises(c.CurveContractError,match='synthetic_publication_failure'):
        publish(tmp_path)
    paths=list(tmp_path.rglob('*.json'))
    assert len(paths)==1 and paths[0].name=='curve.json'
    retained=paths[0].read_bytes()
    monkeypatch.setattr(store,'_write_exclusive',original)
    with pytest.raises(c.CurveContractError,match='orphan_or_in_progress'):
        publish(tmp_path,clock=lambda:1207)
    assert paths[0].read_bytes()==retained


def test_failed_fsync_is_not_reported_success_and_orphan_is_retained(tmp_path,monkeypatch):
    monkeypatch.setattr(store.os,'fsync',lambda _:(_ for _ in ()).throw(OSError('synthetic')))
    with pytest.raises(c.CurveContractError,match='write_or_sync_failed'):
        publish(tmp_path)
    assert len(list(tmp_path.rglob('curve.json')))==1
    assert not list(tmp_path.rglob('publication.json'))


def test_existing_curve_collision_does_not_change_bytes(tmp_path):
    path=tmp_path/'curves'/curve()['curve_sha256']
    path.mkdir(parents=True)
    (path/'curve.json').write_bytes(b'{}')
    with pytest.raises(c.CurveContractError,match='existing_curve_conflict'):
        publish(tmp_path)
    assert (path/'curve.json').read_bytes()==b'{}'


def test_bounded_read_refuses_large_payload(tmp_path):
    path=tmp_path/'large.json'
    with path.open('wb') as handle:
        handle.truncate(c.MAX_BYTES+1)
    with pytest.raises(c.CurveContractError,match='read_byte_limit'):
        store._read(path)


@pytest.mark.parametrize('raw',[b'{"x":1,"x":2}',b'{"x":NaN}',b'{ "x":1}',b'[]\n',b'\xff'])
def test_noncanonical_or_ambiguous_json_fails(raw):
    with pytest.raises(c.CurveContractError):
        store._decode(raw)


@pytest.mark.parametrize('kwargs',[
    {'clock':lambda:1202}, {'clock':clock(1204,1214)},
    {'clock':clock(1204,1205,1204)},
])
def test_actual_publisher_clock_cannot_go_backward_or_past_deadline(tmp_path,kwargs):
    with pytest.raises(c.CurveContractError):
        publish(tmp_path,**kwargs)


def test_consumer_clock_is_sampled_after_independent_reads(tmp_path,monkeypatch):
    published=publish(tmp_path)
    events=[]
    original=store._read
    def read(path):
        value=original(path)
        events.append(path.name)
        return value
    def observe():
        assert events==['curve.json','publication.json']
        return 1210
    monkeypatch.setattr(store,'_read',read)
    consumed=consume(tmp_path,published['descriptor'],clock=observe)
    assert consumed['consumption']['available_epoch']==1210


def test_concurrent_existing_publication_and_independent_reads(tmp_path):
    first=publish(tmp_path)
    with ThreadPoolExecutor(max_workers=6) as pool:
        operations=[pool.submit(publish,tmp_path,clock=lambda:1207) for _ in range(6)]
        operations += [pool.submit(consume,tmp_path,first['descriptor'],clock=lambda:1208) for _ in range(6)]
        values=[operation.result(timeout=10) for operation in operations]
    assert all(value['existing_record'] for value in values[:6])
    assert all(value['curve']==curve() for value in values[6:])
    assert len(list(tmp_path.rglob('*.json')))==2


def test_reader_refuses_during_first_publication_then_succeeds(tmp_path,monkeypatch):
    waiting,release=threading.Event(),threading.Event()
    original=store._write_exclusive
    def pause(path,raw):
        if path.name=='publication.json':
            waiting.set()
            assert release.wait(10)
        return original(path,raw)
    monkeypatch.setattr(store,'_write_exclusive',pause)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future=pool.submit(publish,tmp_path)
        assert waiting.wait(10)
        try:
            with pytest.raises(c.CurveContractError,match='orphan_or_in_progress'):
                publish(tmp_path,clock=lambda:1207)
        finally:
            release.set()
        published=future.result(timeout=10)
    assert consume(tmp_path,published['descriptor'])['curve']==curve()


def test_returned_mutation_cannot_change_persisted_source(tmp_path):
    first=publish(tmp_path)
    descriptor=deepcopy(first['descriptor'])
    first['publication']['publication_completed_epoch']=1
    consumed=consume(tmp_path,descriptor)
    consumed['curve']['prepared_curve']['reference_epoch']=1
    assert consume(tmp_path,descriptor)['curve']==curve()


def test_symlink_component_is_rejected_without_writing_through(tmp_path,monkeypatch):
    original=Path.lstat
    class Link:
        st_mode=stat.S_IFLNK
        st_file_attributes=0
    monkeypatch.setattr(Path,'lstat',lambda path:Link() if path==tmp_path else original(path))
    with pytest.raises(c.CurveContractError,match='reparse_point'):
        publish(tmp_path)


@pytest.mark.skipif(os.name!='nt',reason='Actual NTFS junction case is Windows-specific')
def test_actual_windows_junction_is_rejected(tmp_path):
    outside=tmp_path/'outside'
    outside.mkdir()
    linked=tmp_path/'junction'
    script=tmp_path/'create_junction.ps1'
    script.write_text('param([string]$LinkPath,[string]$TargetPath)\nNew-Item -ItemType Junction -Path $LinkPath -Target $TargetPath -ErrorAction Stop | Out-Null\n')
    subprocess.run(['powershell.exe','-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass','-File',str(script),str(linked),str(outside)],
        check=True,capture_output=True,timeout=15)
    with pytest.raises(c.CurveContractError,match='reparse_point'):
        publish(linked)
    assert not list(outside.iterdir())
