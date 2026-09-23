"""Staged only: synthetic transport, retained offline cases and Windows temp I/O."""
from copy import deepcopy
import ctypes
from ctypes import wintypes
import errno
import hashlib
import json
import os
from pathlib import Path
import socket
import sys

import pytest

import oanda_practice_trial_runtime_v3 as m
import oanda_practice_trial_runner_v2 as r
from test_oanda_practice_trial_runner_v2 import case, retain, entry_context, install_fake_submission, NOW


def test_candidate_bytes_are_original_reviewed_implementation():
    raw=(m.ROOT/m.CANDIDATE_NAME).read_bytes()
    assert hashlib.sha256(raw).hexdigest()==m.PINNED_SOURCES[m.CANDIDATE_NAME]
    assert b'import base_policy as base' in raw
    assert 'base_policy' not in sys.modules






def test_changed_source_refuses_before_compilation(tmp_path,monkeypatch):
    for name in m.PINNED_SOURCES:
        target=tmp_path/name;target.parent.mkdir(parents=True,exist_ok=True)
        target.write_bytes((m.ROOT/name).read_bytes())
    (tmp_path/m.CANDIDATE_NAME).write_bytes(b'raise AssertionError("must not execute changed bytes")')
    monkeypatch.setattr(m,'ROOT',tmp_path)
    with pytest.raises(ValueError,match='immutable_preflight_source_changed'):
        m.validate_immutable_order({}, {},expected_account_sha256='d'*64,expected_trial_id=r.TRIAL_ID)






def test_status_transient_permission_recovers_exact_bytes(tmp_path,monkeypatch):
    path=tmp_path/'status.json';path.write_bytes(b'old');real=m.os.replace;calls=[];sleeps=[]
    def replace(src,dst):
        calls.append(Path(src).read_bytes())
        if len(calls)<3:raise PermissionError(errno.EACCES,'private text',str(dst))
        return real(src,dst)
    monkeypatch.setattr(m.os,'replace',replace)
    out=m.atomic_status(path,{'state':'test'},sleeper=sleeps.append,clock=lambda:NOW)
    assert out['status']=='published' and out['attempts']==3
    assert calls==[path.read_bytes()]*3 and sleeps==[.05,.1]
    assert len(out['recovered_failures'])==2
    assert 'private text' not in json.dumps(out) and str(tmp_path) not in json.dumps(out)


def test_status_permission_exhaustion_is_bounded_and_retains_old_file(tmp_path,monkeypatch):
    path=tmp_path/'status.json';path.write_bytes(b'old');sleeps=[]
    def fail(*args):raise PermissionError(errno.EACCES,'never disclose this')
    monkeypatch.setattr(m.os,'replace',fail)
    with pytest.raises(m.StatusPublicationError) as caught:
        m.atomic_status(path,{'state':'new'},sleeper=sleeps.append,clock=lambda:NOW)
    out=caught.value.diagnostic
    assert out['attempts']==4 and sleeps==list(m.RETRY_DELAYS)
    assert path.read_bytes()==b'old' and len(list(tmp_path.glob('status.json.*.tmp')))==1
    assert all(x['operation']=='status_replace' for x in out['failures'])


def test_nonpermission_io_failure_is_not_retried(tmp_path,monkeypatch):
    def fail(*args):raise OSError(errno.ENOSPC,'secret volume')
    monkeypatch.setattr(m.os,'fsync',fail)
    with pytest.raises(m.StatusPublicationError) as caught:
        m.atomic_status(tmp_path/'status.json',{},sleeper=lambda _:pytest.fail('must not sleep'))
    assert caught.value.diagnostic['attempts']==1
    assert caught.value.diagnostic['failures'][0]['operation']=='status_fsync'


def test_status_failure_alone_does_not_create_cycle_error_or_exit(case,monkeypatch,tmp_path):
    retain(case);case.runner.status_path=tmp_path/'status.json'
    def fail(*args,**kwargs):
        raise m.StatusPublicationError({'status':'unavailable','attempts':4,'failures':[]})
    monkeypatch.setattr(r,'atomic_status',fail)
    for _ in range(2):
        out=case.runner.cycle_with_diagnostics()
        assert out['state']=='entries_halted_reconciliation_required'
        assert out['status_publication']['status']=='unavailable'
    events=[x[0] for x in case.ledger.db.execute('SELECT kind FROM events')]
    assert events.count('status_publication_failed')==2 and 'cycle_error' not in events
    assert case.ledger.session(NOW)['entries_today']==1 and len(case.ledger.intents(unresolved=True))==1


def test_cycle_error_plus_failed_error_status_never_double_fails(case,monkeypatch,tmp_path):
    case.runner.status_path=tmp_path/'status.json'
    def cycle():raise PermissionError(errno.EACCES,'credential=do-not-retain')
    monkeypatch.setattr(case.runner,'cycle',cycle)
    monkeypatch.setattr(r,'atomic_status',lambda *a,**k: (_ for _ in ()).throw(m.StatusPublicationError({'status':'unavailable'})))
    out=case.runner.cycle_with_diagnostics()
    assert out['state']=='operation_unavailable_no_new_order'
    body=case.ledger.db.execute("SELECT body FROM events WHERE kind='cycle_error'").fetchone()[0]
    event=json.loads(body)
    assert event['diagnostic']['errno']==errno.EACCES
    assert event['diagnostic']['traceback_frames'] and 'credential' not in body
    assert case.ledger.db.execute("SELECT count(*) FROM events WHERE kind='status_publication_failed'").fetchone()[0]==1


def test_diagnostic_ledger_failure_is_not_silently_swallowed(case,monkeypatch,tmp_path):
    case.runner.status_path=tmp_path/'status.json'
    monkeypatch.setattr(r,'atomic_status',lambda *a,**k: (_ for _ in ()).throw(m.StatusPublicationError({'status':'unavailable'})))
    monkeypatch.setattr(case.ledger,'event',lambda *a,**k: (_ for _ in ()).throw(OSError(errno.ENOSPC,'ledger unavailable')))
    with pytest.raises(OSError):case.runner.status('test')


def test_derived_counters_separate_claim_from_transport_without_rewrite(case):
    rows=[('a','not_submitted',None),('b','unknown',None),('c','unknown',{'status':'unresolved'}),
          ('d','filled_closed',{'read_completed_epoch':NOW})]
    for identity,status,receipt in rows:
        p=retain(case,identity=identity,client=identity)
        result={'status':status}
        if receipt is not None:result['submission_receipt']=receipt
        case.ledger.outcome(p['intent_id'],result)
    before=case.ledger.intents();out=case.ledger.accounting(NOW)
    assert out['total']['durable_claims']==4
    assert out['total']['claimed_definitely_not_submitted']==1
    assert out['total']['transport_evidenced_claims']==2
    assert out['total']['possibly_transmitted_or_confirmed_claims']==3
    assert out['total']['filled_records']==1 and out['total']['unresolved_records']==2
    assert case.ledger.session(NOW)['entries_today']==4 and case.ledger.intents()==before
    assert case.ledger.accounting(NOW+86400)['claim_day']['durable_claims']==0
    assert case.ledger.accounting(NOW+86400)['total']==out['total']


@pytest.mark.skipif(os.name!='nt',reason='native Windows sharing semantics')
def test_actual_windows_deny_delete_handle_recovers_after_release(tmp_path):
    path=tmp_path/'status.json';path.write_bytes(b'old')
    kernel=ctypes.WinDLL('kernel32',use_last_error=True)
    create=kernel.CreateFileW;create.argtypes=[wintypes.LPCWSTR,wintypes.DWORD,wintypes.DWORD,
        wintypes.LPVOID,wintypes.DWORD,wintypes.DWORD,wintypes.HANDLE];create.restype=wintypes.HANDLE
    close=kernel.CloseHandle;close.argtypes=[wintypes.HANDLE];close.restype=wintypes.BOOL
    handle=create(str(path),0x80000000,0x1|0x2,None,3,0,None)
    assert handle not in (None,ctypes.c_void_p(-1).value)
    held=[handle];sleeps=[]
    def release(delay):
        sleeps.append(delay)
        assert close(held.pop())
    try:
        out=m.atomic_status(path,{'state':'recovered'},sleeper=release)
        assert out['attempts']==2 and sleeps==[.05]
        assert json.loads(path.read_bytes())=={'state':'recovered'}
        assert out['recovered_failures'][0]['operation']=='status_replace'
        assert out['recovered_failures'][0]['winerror'] in {5,32,33}
    finally:
        if held:close(held.pop())


def test_runner_economic_callback_is_single_use_in_current_claim(case,monkeypatch):
    counts=install_fake_submission(case,monkeypatch)
    original=case.broker.submit_market
    outcomes=[]
    def twice(prepared,*,claim_submission,validate_before_submit):
        def callback(*args):
            first=validate_before_submit(*args)
            second=validate_before_submit(*args)
            outcomes.extend([first,second])
            return first
        return original(prepared,claim_submission=claim_submission,validate_before_submit=callback)
    monkeypatch.setattr(case.broker,'submit_market',twice)
    case.runner.enter(entry_context(),{'status':'available'})
    assert outcomes==[True,False] and counts['simulated_writes']==1


def test_runner_validator_cannot_pass_without_same_invocation_claim(case,monkeypatch):
    install_fake_submission(case,monkeypatch)
    def skipped_claim(p,*,claim_submission,validate_before_submit):
        assert validate_before_submit(p,case.broker.account_summary(),
            {'open_trades':case.broker.open_trades(),'pending_orders':case.broker.pending_orders()}) is False
        return {'status':'not_submitted','reason_code':'test_missing_claim'}
    monkeypatch.setattr(case.broker,'submit_market',skipped_claim)
    case.runner.enter(entry_context(),{'status':'available'})
    assert case.ledger.session(NOW)['entries_today']==0


def test_runner_still_counts_eighth_definite_no_post_claim(case,monkeypatch):
    for i in range(7):
        p=retain(case,identity=str(i),client=str(i))
        # Old-day instrument cooldown must not block this fixture's new pair.
        raw=case.ledger.db.execute('SELECT prepared FROM intents WHERE id=?',(str(i),)).fetchone()[0]
        p=json.loads(raw);p['order']['instrument']='EUR_USD'
        with case.ledger.db:
            case.ledger.db.execute('UPDATE intents SET prepared=? WHERE id=?',(r.encoded(p).decode(),str(i)))
        case.ledger.outcome(str(i),{'status':'not_submitted'})
    counts=install_fake_submission(case,monkeypatch,validation_quote={'tradeable':False})
    case.runner.enter(entry_context(),{'status':'available'})
    assert counts['simulated_writes']==0 and case.ledger.session(NOW)['entries_today']==8
    assert case.ledger.accounting(NOW)['total']['claimed_definitely_not_submitted']==8


def test_recovered_status_diagnostic_is_durable_and_counters_are_published(case,monkeypatch,tmp_path):
    path=tmp_path/'status.json';case.runner.status_path=path
    real=r.atomic_status
    def recovered(path,value,**kwargs):
        out=real(path,value,**kwargs);out['recovered_failures']=[{'operation':'status_replace','errno':13}]
        return out
    monkeypatch.setattr(r,'atomic_status',recovered)
    out=case.runner.status('synthetic')
    assert json.loads(path.read_bytes())['attempt_accounting']['total']['durable_claims']==0
    row=case.ledger.db.execute("SELECT body FROM events WHERE kind='status_publication_recovered'").fetchone()
    assert json.loads(row[0])['payload_sha256']==hashlib.sha256(path.read_bytes()).hexdigest()
    assert out['status_publication']['status']=='published'


def test_status_byte_bound_precedes_write(tmp_path,monkeypatch):
    monkeypatch.setattr(m,'MAX_STATUS_BYTES',8)
    with pytest.raises(ValueError,match='status_byte_bound'):
        m.atomic_status(tmp_path/'status.json',{'value':'too large'})
    assert list(tmp_path.iterdir())==[]


@pytest.mark.parametrize('bad',[float('nan'),float('inf'),0,-1,True,None])
def test_status_invalid_initial_clock_refuses_before_write(tmp_path,bad):
    with pytest.raises(ValueError,match='status_publication_clock_invalid'):
        m.atomic_status(tmp_path/'status.json',{},clock=lambda:bad)
    assert list(tmp_path.iterdir())==[]


def test_status_completion_clock_rollback_never_returns_success(tmp_path):
    times=iter([NOW,NOW-1])
    with pytest.raises(ValueError,match='status_publication_clock_rollback'):
        m.atomic_status(tmp_path/'status.json',{},clock=lambda:next(times))
    # Replacement already happened; the receipt must not pretend valid clocks.
    assert (tmp_path/'status.json').exists()


def test_future_payload_clock_refuses_before_publication(tmp_path):
    with pytest.raises(ValueError,match='status_payload_clock_after_publication_started'):
        m.atomic_status(tmp_path/'status.json',{'observed_epoch':NOW+1},clock=lambda:NOW)
    assert list(tmp_path.iterdir())==[]


def test_status_error_observation_clock_rollback_is_not_retried(tmp_path,monkeypatch):
    times=iter([NOW,NOW-1])
    monkeypatch.setattr(m.os,'replace',lambda *a: (_ for _ in ()).throw(PermissionError(errno.EACCES,'private')))
    with pytest.raises(ValueError,match='status_publication_clock_rollback'):
        m.atomic_status(tmp_path/'status.json',{},clock=lambda:next(times),sleeper=lambda _:pytest.fail('no retry'))
