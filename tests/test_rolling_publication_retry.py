from pathlib import Path
import json,sys,threading,time,os
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'trad'))
import oanda_rolling_technical_worker_v2 as m

def test_retry_keeps_original_bytes_and_generation(tmp_path,monkeypatch):
    path=tmp_path/'status.json';path.write_text('old')
    replace=m.os.replace;calls=[];sleeps=[]
    def locked(source,target):
        calls.append(Path(source).read_bytes())
        if len(calls)<3:raise PermissionError('reader lock')
        replace(source,target)
    monkeypatch.setattr(m.os,'replace',locked);monkeypatch.setattr(m.time,'sleep',sleeps.append)
    value={'publication_generation':'original','generated_epoch':100.0}
    m.atomic_json(path,value)
    assert len(set(calls))==1 and path.read_bytes()==calls[0]
    assert json.loads(path.read_bytes())==value and sleeps==[.05,.1]

def test_permanent_lock_is_bounded_preserves_last_publication(tmp_path,monkeypatch):
    path=tmp_path/'status.json';path.write_bytes(b'old');sleeps=[];calls=[]
    def locked(*args):calls.append(1);raise PermissionError('locked')
    monkeypatch.setattr(m.os,'replace',locked);monkeypatch.setattr(m.time,'sleep',sleeps.append)
    with pytest.raises(PermissionError):m.atomic_json(path,{'new':1})
    assert len(calls)==6 and sleeps==[.05,.1,.2,.4,.8]
    assert path.read_bytes()==b'old' and list(tmp_path.iterdir())==[path]

@pytest.mark.skipif(os.name!='nt',reason='Windows replacement locking')
def test_native_windows_reader_lock_release(tmp_path):
    path=tmp_path/'status.json';path.write_bytes(b'old')
    handle=path.open('rb');errors=[]
    def publish():
        try:m.atomic_json(path,{'publication_generation':'new','generated_epoch':100.0})
        except Exception as e:errors.append(e)
    worker=threading.Thread(target=publish);worker.start()
    try:
        time.sleep(.12)
        assert handle.read()==b'old'
    finally:handle.close();worker.join(timeout=5)
    assert not worker.is_alive() and not errors
    assert json.loads(path.read_bytes())=={'publication_generation':'new','generated_epoch':100.0}
