from pathlib import Path
import sys,json
import pytest
sys.path[:0]=[str(Path(__file__).resolve().parents[1]/'tools'),str(Path(__file__).parent)]
import forex_paper_state_producer as m
import test_retained_management_contract as old


def fixture(tmp_path):
    context,capture,_,_=old.fixture(tmp_path)
    book=tmp_path/'book'
    m.initialize(book,'test_book',context.manifest['registry_sha256'],clock=lambda:1)
    event=m.observe(book,capture,context.capture_sha256,clock=lambda:10002)
    return book,capture,context.capture_sha256,event['event_head_sha256']


def test_actual_current_consumer(tmp_path):
    book,capture,pin,head=fixture(tmp_path)
    rows,report=m.inspect(book,head,capture,pin)
    assert report['population']==136 and report['paper_books']==1
    assert report['positions']==0 and report['actions']=={'WAIT':136}
    assert all(r['status']=='declared_flat_wait' and not r['action_eligible'] for r in rows)
    assert all(r['quote_status']=='exact_execution_receipt_not_bound' for r in rows)
    assert report['forecast_statuses']


def test_no_reinitialization(tmp_path):
    book,_,_,_=fixture(tmp_path);before=(book/'genesis.json').read_bytes()
    with pytest.raises(FileExistsError):m.initialize(book,'different','0'*64)
    assert (book/'genesis.json').read_bytes()==before


@pytest.mark.parametrize('fault',['position','action','parent','source','registry','clock','extra','gap','wrong_head'])
def test_journal_refusals(tmp_path,fault):
    book,capture,pin,head=fixture(tmp_path)
    p=book/('genesis.json' if fault=='source' else 'event_000000.json');v=json.loads(p.read_bytes())
    if fault=='position':v['position']={'units':1}
    elif fault=='action':v['action']='ENTER'
    elif fault=='parent':v['parent_sha256']='0'*64
    elif fault=='source':v['source_bindings']={}
    elif fault=='registry':v['registry_sha256']='0'*64
    elif fault=='clock':v['decision_epoch']=0
    elif fault=='wrong_head':head='0'*64
    elif fault=='extra':(book/'unknown.json').write_text('{}')
    elif fault=='gap':p.rename(book/'event_000001.json')
    if fault not in ('gap','extra','wrong_head'):p.write_bytes(m.prior.encoded(v))
    with pytest.raises(ValueError):m.inspect(book,head,capture,pin)


def test_wrong_capture_and_backdated_initialization(tmp_path):
    book,capture,pin,head=fixture(tmp_path)
    with pytest.raises(ValueError,match='event_capture_binding'):m.inspect(book,head,capture,'0'*64)
    c=m.forecasts.VerifiedCapture.load(capture,pin);late=tmp_path/'late'
    m.initialize(late,'late',c.manifest['registry_sha256'],clock=lambda:20000)
    with pytest.raises(ValueError,match='prospective_capture_order'):m.observe(late,capture,pin,clock=lambda:20001)
    assert not list(late.glob('event_*'))


def test_replay_resume_and_no_mutation(tmp_path):
    book,capture,pin,head=fixture(tmp_path);before={p.name:p.read_bytes() for p in book.iterdir()}
    output=tmp_path/'runs'
    assert m.replay(book,head,capture,pin,output,'partial',max_new=1)['status']=='checkpointed'
    assert m.replay(book,head,capture,pin,output,'partial',resume=True)['status']=='completed'
    assert m.replay(book,head,capture,pin,output,'whole')['status']=='completed'
    files=list((output/'whole').glob('rows_*.json'))+[output/'whole/REPORT.json']
    assert files and all(p.read_bytes()==(output/'partial'/p.name).read_bytes() for p in files)
    assert before=={p.name:p.read_bytes() for p in book.iterdir()}


def test_second_observation_retains_chain(tmp_path):
    book,capture,pin,head=fixture(tmp_path)
    second=m.observe(book,capture,pin,clock=lambda:10003)
    _,events,newhead=m.journal(book,second['event_head_sha256'])
    assert len(events)==2 and events[-1]['parent_sha256']==head and newhead!=head
    with pytest.raises(ValueError,match='externally_pinned_event_head'):m.journal(book,head)
