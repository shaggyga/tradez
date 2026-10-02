from pathlib import Path
from copy import deepcopy
import sys
import json
import shutil
import pytest
sys.path[:0] = [str(Path(__file__).resolve().parents[1]/'tools'), str(Path(__file__).parent)]
import forex_paper_quote_observation as m
import test_retained_management_contract as old


def fixture(tmp_path):
    context, capture, contract, _ = old.fixture(tmp_path)
    book = tmp_path/'book'
    m.paper.initialize(book, 'joined_test', context.manifest['registry_sha256'], clock=lambda: 1)
    event = m.paper.observe(book, capture, context.capture_sha256, clock=lambda: 10002)
    heartbeat = dict(updated_at='1970-01-01T02:46:42+00:00', status='running', phase='streaming',
        role='practice_007_quote_stream', details=dict(can_place_orders=False, instrument_count=68,
        stream=dict(connected=True, raw_price_observer_errors=0, connection_generation=2,
        exact_receipts=dict(session_id='fixture', connection_generation=2,
        transport=dict(enabled=True, thread_alive=True, last_error='', mirror_error='', last_success_age_sec=0.1)))))
    request = dict(schema=m.SCHEMA, source_bindings=m.sources(), event_head_sha256=event['event_head_sha256'],
        heartbeat=heartbeat, heartbeat_observed_epoch=10002, combined_request=contract['combined_request'])
    return book, capture, request


def test_full_join(tmp_path):
    book, capture, request = fixture(tmp_path)
    rows, report = m.inspect(book, capture, request)
    assert len(rows) == 136 and report['paper_books'] == 1 and report['positions'] == 0
    assert report['joined_report']['quote_pairs_validated'] == 68
    assert all(r['action'] == 'WAIT' and not r['action_eligible'] for r in rows)
    assert report['joined_report']['statuses']['priced_observation'] == 68
    assert report['recorded_book_events'] == 1


@pytest.mark.parametrize('fault', ['generation', 'session', 'stale', 'future', 'timezone', 'stopped',
    'disconnected', 'hook_error', 'write_error', 'mirror_error', 'thread', 'publication', 'scope', 'event', 'source'])
def test_refusals_through_actual_consumer(tmp_path, fault):
    book, capture, r = fixture(tmp_path); h=r['heartbeat']; s=h['details']['stream']; e=s['exact_receipts']
    if fault=='generation':e['connection_generation']=3
    elif fault=='session':e['session_id']='different'
    elif fault=='stale':h['updated_at']='1970-01-01T02:46:00+00:00'
    elif fault=='future':h['updated_at']='1970-01-01T02:46:43+00:00'
    elif fault=='timezone':h['updated_at']='1970-01-01T02:46:42'
    elif fault=='stopped':h['status']='stopped'
    elif fault=='disconnected':s['connected']=False
    elif fault=='hook_error':s['raw_price_observer_errors']=1
    elif fault=='write_error':e['transport']['last_error']='write_failed'
    elif fault=='mirror_error':e['transport']['mirror_error']='write_failed'
    elif fault=='thread':e['transport']['thread_alive']=False
    elif fault=='publication':e['transport']['last_success_age_sec']=16
    elif fault=='scope':h['details']['can_place_orders']=True
    elif fault=='event':r['event_head_sha256']='0'*64
    elif fault=='source':r['source_bindings']={}
    with pytest.raises(ValueError):m.inspect(book,capture,r)


def test_new_decision_cannot_relabel_old_paper_event(tmp_path):
    book,capture,r=fixture(tmp_path);r['combined_request']['decision_epoch']=10003
    with pytest.raises(ValueError, match='paper_quote_decision_mismatch'):m.inspect(book,capture,r)


def test_valid_worker_does_not_restore_missing_quote(tmp_path):
    book,capture,r=fixture(tmp_path);snap=r['combined_request']['quote_snapshot']
    pair=next(iter(snap['quotes']));del snap['quotes'][pair];snap['quote_count']-=1
    old.old.seal_snapshot(r['combined_request'])
    rows,report=m.inspect(book,capture,r)
    assert report['joined_report']['quote_pairs_validated']==67
    assert all(x['observation']['quote_status']=='missing' for x in rows if x['observation']['instrument']==pair)


def test_resume_exact_and_external_pin(tmp_path):
    book,capture,r=fixture(tmp_path);path=tmp_path/'request.json';raw=m.prior.encoded(r);path.write_bytes(raw)
    pin=m.prior.sha(raw);out=tmp_path/'runs'
    with pytest.raises(ValueError,match='request_external_hash'):m.run(book,capture,path,'0'*64,out,'bad')
    assert m.run(book,capture,path,pin,out,'partial',max_new=1)['status']=='checkpointed'
    assert m.run(book,capture,path,pin,out,'partial',resume=True)['status']=='completed'
    assert m.run(book,capture,path,pin,out,'whole')['status']=='completed'
    for file in list((out/'whole').glob('rows_*.json'))+[out/'whole/REPORT.json']:
        assert file.read_bytes()==(out/'partial'/file.name).read_bytes()


@pytest.mark.parametrize('bad_worker',[False,True])
def test_record_uses_actual_consumers_and_preserves_failure(tmp_path,monkeypatch,bad_worker):
    book,capture,r=fixture(tmp_path);before=len(list(book.glob('event_*')))
    heartbeat=tmp_path/'heartbeat.json';quote=tmp_path/'quote.json'
    if bad_worker:r['heartbeat']['details']['stream']['connected']=False
    heartbeat.write_bytes(m.prior.encoded(r['heartbeat']))
    quote.write_bytes(m.prior.encoded(r['combined_request']['quote_snapshot']))
    def capture_existing(out):
        shutil.copytree(capture,out)
        return {'manifest_sha256':m.prior.sha((out/'MANIFEST.json').read_bytes())}
    monkeypatch.setattr(m.prior,'capture',capture_existing)
    # The transport is covered separately; retain the real consumer in this test.
    monkeypatch.setattr(m.combined.quotes,'load_quote_snapshot',lambda p:json.loads(p.read_bytes()))
    out=tmp_path/'recorded'
    if bad_worker:
        with pytest.raises(ValueError,match='worker_stream_error'):m.record(book,out,quote,heartbeat,clock=lambda:10002)
        assert (out/'FAILURE.json').exists() and len(list(book.glob('event_*')))==before
        assert not (out/'RECEIPT.json').exists()
    else:
        result=m.record(book,out,quote,heartbeat,clock=lambda:10002)
        assert result['market_reads_only'] and not result['action_eligible']
        assert len(list(book.glob('event_*')))==before+1
        assert m.run(book,out/'capture',out/'REQUEST.json',result['request_sha256'],tmp_path/'replayed','one')['status']=='completed'
        with pytest.raises(FileExistsError):m.record(book,out,quote,heartbeat,clock=lambda:10002)
