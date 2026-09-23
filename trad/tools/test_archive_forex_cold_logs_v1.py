from pathlib import Path
import hashlib,json,os
import pytest
from tools import archive_forex_cold_logs_v1 as a

def sample(tmp_path):
    root=tmp_path/'logs';root.mkdir();p=root/'old_worker_20260901.out.log'
    p.write_bytes(b'old research stdout\n'*20000+bytes(range(256)))
    os.utime(p,ns=(a.CUTOFF_NS-86400*10**9,a.CUTOFF_NS-86400*10**9))
    s=p.stat();return root,p,{'bytes':s.st_size,'mtime_ns':s.st_mtime_ns}

def invoke(tmp_path,root,p,rec,**kw):
    return a.archive_file(p,tmp_path/'archive.gz',tmp_path/'receipt.json',rec,source_root=root,minimum_free=0,**kw)

def test_exact_archive_removal_and_restore(tmp_path):
    root,p,rec=sample(tmp_path);expected=a.sha(p)
    result=invoke(tmp_path,root,p,rec)
    assert result['original_removed'] and not p.exists()
    assert result['verification']['restored_sha256']==expected
    assert json.loads((tmp_path/'receipt.json').read_bytes())['status']=='archived_original_removed'
    restored=a.restore_copy(tmp_path/'receipt.json',tmp_path/'restored.log')
    assert restored['sha256']==expected and restored['bytes']==rec['bytes']
    with pytest.raises(ValueError,match='never_overwrites'):a.restore_copy(tmp_path/'receipt.json',tmp_path/'restored.log')

def test_existing_writer_is_refused_without_weaker_retry(tmp_path):
    root,p,rec=sample(tmp_path)
    h=a.K.CreateFileW(str(p),0x40000000,7,None,3,0,None)
    assert h!=a.ctypes.c_void_p(-1).value
    try:
        with pytest.raises(OSError):invoke(tmp_path,root,p,rec)
        assert p.exists() and not (tmp_path/'archive.gz').exists()
    finally:a.K.CloseHandle(h)

@pytest.mark.parametrize('field,delta',[('bytes',1),('mtime_ns',100)])
def test_changed_source_identity_is_preserved(tmp_path,field,delta):
    root,p,rec=sample(tmp_path);rec[field]+=delta
    with pytest.raises(ValueError,match='identity_mismatch'):invoke(tmp_path,root,p,rec)
    assert p.exists() and not (tmp_path/'archive.gz').exists()

def test_failed_full_restore_check_never_deletes_original(tmp_path):
    root,p,rec=sample(tmp_path);expected=a.sha(p)
    def fail(*args):raise ValueError('injected_restore_failure')
    with pytest.raises(ValueError,match='injected'):invoke(tmp_path,root,p,rec,verify=fail)
    assert p.exists() and a.sha(p)==expected and not (tmp_path/'receipt.json').exists()

def test_no_disposition_before_durable_receipt(tmp_path,monkeypatch):
    root,p,rec=sample(tmp_path);expected=a.sha(p)
    def fail(*args):raise OSError('injected_receipt_failure')
    monkeypatch.setattr(a,'save',fail)
    with pytest.raises(OSError,match='receipt_failure'):invoke(tmp_path,root,p,rec)
    assert p.exists() and a.sha(p)==expected

def test_hardlinks_are_preserved(tmp_path):
    root,p,rec=sample(tmp_path);os.link(p,root/'other.out.log')
    with pytest.raises(ValueError,match='single_link'):invoke(tmp_path,root,p,rec)
    assert p.exists() and not (tmp_path/'archive.gz').exists()

def test_readonly_attribute_is_not_cleared(tmp_path):
    root,p,rec=sample(tmp_path);p.chmod(0o444)
    try:
        with pytest.raises((ValueError,OSError)):invoke(tmp_path,root,p,rec)
        assert p.exists() and p.lstat().st_file_attributes&1
    finally:p.chmod(0o666)

def test_recent_file_is_not_eligible(tmp_path):
    root,p,rec=sample(tmp_path);rec['mtime_ns']=a.CUTOFF_NS
    with pytest.raises(ValueError,match='cutoff'):invoke(tmp_path,root,p,rec)
    assert p.exists()

def test_outside_root_and_nonstdout_never_opened(tmp_path):
    root,p,rec=sample(tmp_path)
    (tmp_path/'different').mkdir()
    with pytest.raises(ValueError,match='boundary'):a.bounded_file(p,tmp_path/'different')
    db=root/'evidence.sqlite';db.write_bytes(b'preserve')
    with pytest.raises(ValueError,match='stdout'):a.bounded_file(db,root)
    assert db.read_bytes()==b'preserve'

def test_corrupted_archive_cannot_restore_or_overwrite(tmp_path):
    root,p,rec=sample(tmp_path);invoke(tmp_path,root,p,rec)
    with (tmp_path/'archive.gz').open('ab') as f:f.write(b'corrupt')
    with pytest.raises(ValueError,match='archive_changed'):a.restore_copy(tmp_path/'receipt.json',tmp_path/'restored.log')
    assert not (tmp_path/'restored.log').exists()

def test_new_archive_required(tmp_path):
    root,p,rec=sample(tmp_path);(tmp_path/'archive.gz').write_bytes(b'keep')
    with pytest.raises(ValueError,match='new_archive'):invoke(tmp_path,root,p,rec)
    assert p.exists() and (tmp_path/'archive.gz').read_bytes()==b'keep'
