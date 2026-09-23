"""Windows publication-failure fixtures; never start a worker or access its data."""
from contextlib import ExitStack
from copy import deepcopy
import errno
import json
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from test_oanda_causal_forecast_study_gap_v2 import (
    study, FixtureExecutor, FixtureFuture, FixtureLedger, LoopClock,
)


def sharing_error(code=5):
    error = OSError('fixture Windows sharing failure')
    error.winerror = code
    return error


def test_transient_replace_reuses_one_serialization_fsync_and_identical_bytes(tmp_path):
    path = tmp_path/'heartbeat.json'
    path.write_bytes(b'{"previous":true}')
    old = path.read_bytes()
    value = {'generated_epoch':1000.25,'target_epoch':4600.25,'can_place_orders':False}
    real_replace, real_encoded, real_fsync = study.os.replace, study.encoded, study.os.fsync
    attempted = []
    sleeps = []
    def replacing(source, destination):
        attempted.append((Path(source),Path(source).read_bytes()))
        if len(attempted) <= 3:
            assert path.read_bytes() == old
            raise PermissionError('fixture transient lock')
        return real_replace(source,destination)
    with patch.object(study.os,'replace',side_effect=replacing):
        with patch.object(study,'encoded',wraps=real_encoded) as encode:
            with patch.object(study.os,'fsync',wraps=real_fsync) as fsync:
                with patch.object(study.time,'sleep',side_effect=sleeps.append):
                    study.atomic_json(path,value)
    assert encode.call_count == fsync.call_count == 1
    assert len(attempted) == 4
    assert len({source for source,_ in attempted}) == 1
    assert len({raw for _,raw in attempted}) == 1
    assert json.loads(path.read_bytes()) == value
    assert sleeps == [.01,.02,.04]
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize('winerror',[5,32,33])
def test_known_windows_sharing_oserror_retries(tmp_path,winerror):
    path = tmp_path/'scorecard.json'
    real_replace = study.os.replace
    attempts = 0
    def replacing(source,destination):
        nonlocal attempts
        attempts += 1
        if attempts == 1: raise sharing_error(winerror)
        return real_replace(source,destination)
    with patch.object(study.os,'replace',side_effect=replacing):
        with patch.object(study.time,'sleep') as sleep:
            study.atomic_json(path,{'observed_cutoff_epoch':4000,'proof_eligible':False})
    assert attempts == 2
    sleep.assert_called_once_with(.01)
    assert json.loads(path.read_bytes())['observed_cutoff_epoch'] == 4000


def test_exhausted_retries_preserve_old_destination_and_exact_error(tmp_path):
    path = tmp_path/'heartbeat.json'
    path.write_bytes(b'{"old":1}')
    error = PermissionError('fixture destination remains locked')
    sleeps = []
    with patch.object(study.os,'replace',side_effect=error) as replacing:
        with patch.object(study.time,'sleep',side_effect=sleeps.append):
            with pytest.raises(PermissionError) as raised:
                study.atomic_json(path,{'generated_epoch':9999})
    assert raised.value is error
    assert replacing.call_count == 8
    assert sleeps == [.01,.02,.04,.08,.16,.32,.5]
    assert sum(sleeps) == pytest.approx(1.13)
    assert path.read_bytes() == b'{"old":1}'
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.skipif(study.os.name != 'nt',reason='Requires Windows file-sharing semantics')
def test_real_windows_reader_without_delete_sharing_preserves_old_then_recovers(tmp_path):
    import ctypes
    from ctypes import wintypes
    path = tmp_path/'heartbeat.json'
    path.write_bytes(b'{"old":1}')
    kernel = ctypes.WinDLL('kernel32',use_last_error=True)
    create = kernel.CreateFileW
    create.argtypes = [wintypes.LPCWSTR,wintypes.DWORD,wintypes.DWORD,wintypes.LPVOID,
                       wintypes.DWORD,wintypes.DWORD,wintypes.HANDLE]
    create.restype = wintypes.HANDLE
    close = kernel.CloseHandle
    close.argtypes = [wintypes.HANDLE]
    close.restype = wintypes.BOOL
    # Generic read; permit reads and writes, deliberately omit FILE_SHARE_DELETE.
    handle = create(str(path),0x80000000,0x1|0x2,None,3,0x80,None)
    if handle == ctypes.c_void_p(-1).value:
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        with patch.object(study.time,'sleep') as sleep:
            with pytest.raises(PermissionError) as raised:
                study.atomic_json(path,{'new':2})
        assert raised.value.winerror in (5,32,33)
        assert sleep.call_count == 7
        assert path.read_bytes() == b'{"old":1}'
        assert list(tmp_path.iterdir()) == [path]
    finally:
        assert close(handle)
    study.atomic_json(path,{'new':2})
    assert json.loads(path.read_bytes()) == {'new':2}


@pytest.mark.parametrize('code',[errno.ENOSPC,errno.EIO,errno.ENOENT])
def test_nonsharing_oserror_is_not_retried(tmp_path,code):
    path = tmp_path/'heartbeat.json'
    path.write_bytes(b'{"old":1}')
    error = OSError(code,'fixture permanent IO failure')
    with patch.object(study.os,'replace',side_effect=error) as replacing:
        with patch.object(study.time,'sleep') as sleep:
            with pytest.raises(OSError) as raised:
                study.atomic_json(path,{'new':2})
    assert raised.value is error
    replacing.assert_called_once()
    sleep.assert_not_called()
    assert path.read_bytes() == b'{"old":1}'


def test_successful_replace_is_not_reversed_by_cleanup_error(tmp_path):
    path = tmp_path/'heartbeat.json'
    with patch.object(Path,'unlink',side_effect=PermissionError('fixture cleanup antivirus lock')):
        study.atomic_json(path,{'new':2})
    assert json.loads(path.read_bytes()) == {'new':2}
    assert list(tmp_path.iterdir()) == [path]


def test_cleanup_error_does_not_mask_original_replace_failure(tmp_path):
    path = tmp_path/'scorecard.json'
    path.write_bytes(b'{"old":1}')
    replace_error = PermissionError('fixture replacement locked')
    cleanup_error = PermissionError('fixture cleanup locked')
    with patch.object(study.os,'replace',side_effect=replace_error):
        with patch.object(study.time,'sleep'):
            with patch.object(Path,'unlink',side_effect=cleanup_error):
                with pytest.raises(PermissionError) as raised:
                    study.atomic_json(path,{'new':2})
    assert raised.value is replace_error
    assert path.read_bytes() == b'{"old":1}'
    # Failed cleanup may retain only this invocation's complete temporary.
    remaining = [p for p in tmp_path.iterdir() if p != path]
    assert len(remaining) == 1
    assert json.loads(remaining[0].read_bytes()) == {'new':2}


def test_temporary_name_collision_does_not_delete_other_invocation_file(tmp_path):
    path = tmp_path/'heartbeat.json'
    path.write_bytes(b'{"old":1}')
    collision = path.with_name(path.name+f'.{study.os.getpid()}.123456.tmp')
    collision.write_bytes(b'belongs to a different invocation')
    with patch.object(study.time,'time_ns',return_value=123456):
        with patch.object(study.os,'replace') as replacing:
            with pytest.raises(FileExistsError):
                study.atomic_json(path,{'new':2})
    assert collision.read_bytes() == b'belongs to a different invocation'
    assert path.read_bytes() == b'{"old":1}'
    replacing.assert_not_called()


def test_invalid_serialization_keeps_last_good_publication(tmp_path):
    path = tmp_path/'scorecard.json'
    path.write_bytes(b'{"old":1}')
    with patch.object(study.os,'replace') as replacing:
        with pytest.raises(ValueError):
            study.atomic_json(path,{'invalid':float('nan')})
    replacing.assert_not_called()
    assert path.read_bytes() == b'{"old":1}'
    assert list(tmp_path.iterdir()) == [path]


def test_fsync_failure_does_not_publish_unflushed_candidate(tmp_path):
    path = tmp_path/'heartbeat.json'
    path.write_bytes(b'{"old":1}')
    with patch.object(study.os,'fsync',side_effect=OSError(errno.EIO,'fixture sync failed')):
        with patch.object(study.os,'replace') as replacing:
            with pytest.raises(OSError):
                study.atomic_json(path,{'new':2})
    replacing.assert_not_called()
    assert path.read_bytes() == b'{"old":1}'
    assert list(tmp_path.iterdir()) == [path]


def test_heartbeat_exhaustion_continues_quotes_and_records_errors_in_next_success(tmp_path):
    clock = LoopClock()
    ledger = FixtureLedger()
    executor = FixtureExecutor(FixtureFuture(delay=1000))
    contract = {'cadence_sec':900,'contract_id':'fixture-io-revision','numeric_model_source_sha256':'a'*64}
    study_path = tmp_path/'study'
    study_path.mkdir()
    heartbeat = study_path/'heartbeat.json'
    heartbeat.write_bytes(b'{"old":1}')
    real_replace = study.os.replace
    candidates = []
    attempts = 0
    def replacing(source,destination):
        nonlocal attempts
        if Path(destination) == heartbeat:
            attempts += 1
            raw = Path(source).read_bytes()
            candidate = json.loads(raw)
            if not candidates or candidates[-1] != candidate:
                candidates.append(deepcopy(candidate))
            if candidate.get('heartbeat_publication_errors',0) < 2:
                assert heartbeat.read_bytes() == b'{"old":1}'
                raise PermissionError('fixture repeated heartbeat lock')
        return real_replace(source,destination)
    def quote(path):
        return {'instrument':'EUR_USD','market_epoch':clock.wall()-1,'available_epoch':clock.wall(),
                'bid':1.1,'ask':1.1002,'tradeable':True}
    with ExitStack() as stack:
        stack.enter_context(patch.object(study,'ROOT',tmp_path))
        stack.enter_context(patch.object(study,'STUDY',study_path))
        stack.enter_context(patch.object(study,'load_contract',return_value=contract))
        stack.enter_context(patch.object(study,'CausalForecastLedger',return_value=ledger))
        stack.enter_context(patch.object(study,'ThreadPoolExecutor',return_value=executor))
        stack.enter_context(patch.object(study,'read_quote',side_effect=quote))
        stack.enter_context(patch.object(study.time,'time',clock.wall))
        stack.enter_context(patch.object(study.time,'monotonic',clock.mono))
        stack.enter_context(patch.object(study.time,'sleep',clock.sleep))
        stack.enter_context(patch.object(study.os,'replace',side_effect=replacing))
        study.run(tmp_path/'config.json',activate=True,duration_sec=6)
    published = json.loads(heartbeat.read_bytes())
    assert published['heartbeat_publication_errors'] == 2
    assert published['errors'] == 2
    assert published['can_place_orders'] is False and published['supported_decision'] == 'no_trade'
    assert len(ledger.quotes) >= 3 and ledger.settlements == len(ledger.quotes)
    assert len(executor.submitted) == 1 and not ledger.publications
    assert [row['heartbeat_publication_errors'] for row in candidates] == [0,1,2]
    assert attempts == 17
    assert ledger.closed and executor.stopped


def test_scorecard_retry_keeps_original_snapshot_cutoff_and_no_execution_authority(tmp_path):
    ledger = Mock()
    ledger.contract_hash = 'bound-fixture-contract'
    dataset = {'collection_counts':{'outcomes':0},'observed_cutoff_epoch':4000}
    protocol = {'proof_eligible':False}
    ledger.export_evaluation.return_value = (dataset,protocol)
    report = {'proof_eligible':False,'account_eligible':False,'observed_cutoff_epoch':4000,'paired_scored_decisions':0}
    destination = tmp_path/'scorecard.json'
    real_replace = study.os.replace
    attempts = []
    def replacing(source,target):
        attempts.append(Path(source).read_bytes())
        if len(attempts) <= 2: raise PermissionError('fixture scoreboard reader lock')
        return real_replace(source,target)
    with patch.object(study,'evaluate',return_value=deepcopy(report)) as evaluator:
        with patch.object(study.os,'replace',side_effect=replacing):
            with patch.object(study.time,'sleep'):
                produced = study.write_scorecard(ledger,destination)
    ledger.export_evaluation.assert_called_once_with()
    evaluator.assert_called_once_with(dataset,protocol)
    assert len(attempts) == 3 and len(set(attempts)) == 1
    assert json.loads(destination.read_bytes()) == produced
    assert produced['observed_cutoff_epoch'] == 4000
    assert produced['proof_eligible'] is produced['account_eligible'] is False
