import json
from pathlib import Path
import subprocess
import sys

import pytest

import oanda_operational_log_relay_v2 as relay


def test_segments_rotation_total_cap_and_exact_prefix_bytes(tmp_path):
    sink = relay.BoundedLogSink(tmp_path, segment_bytes=7, total_bytes=20)
    sink.write('stdout', b'a' * 15)
    sink.write('stderr', b'b' * 15)
    sink.close()
    files = list(tmp_path.glob('*.log'))
    assert sum(p.stat().st_size for p in files) == 20
    assert max(p.stat().st_size for p in files) <= 7
    assert sum(p.read_bytes().count(b'a') for p in files) == 15
    assert sum(p.read_bytes().count(b'b') for p in files) == 5
    assert sink.snapshot()['dropped_bytes'] == 10


def test_budget_survives_new_relay_generation_without_truncation(tmp_path):
    old = tmp_path / 'old_stdout.log'
    old.write_bytes(b'original')
    sink = relay.BoundedLogSink(tmp_path, segment_bytes=8, total_bytes=10)
    sink.write('stdout', b'new text')
    sink.close()
    assert old.read_bytes() == b'original'
    assert sum(p.stat().st_size for p in tmp_path.glob('*.log')) == 10
    assert sink.snapshot()['dropped_bytes'] == 6


def test_finite_child_exit_and_overflow_are_recorded_without_deadlock(tmp_path):
    target = tmp_path / 'logs'
    command = [sys.executable, '-B', str(Path(relay.__file__)), '--log-directory', str(target),
               '--segment-bytes', '1024', '--total-bytes', '2048', '--', sys.executable, '-c',
               'import sys;sys.stdout.buffer.write(b"a"*100000);sys.stderr.buffer.write(b"b"*100000);sys.exit(7)']
    result = subprocess.run(command, capture_output=True, timeout=20)
    assert result.returncode == 7, result.stderr
    state = json.loads((target / 'relay_status.json').read_text())
    assert state['child_exit_code'] == 7
    assert state['retained_bytes'] == 2048
    assert state['dropped_bytes'] == 200000 - 2048
    assert not result.stdout and not result.stderr


def test_second_owner_cannot_claim_same_role_directory(tmp_path):
    with relay.relay_lock(tmp_path):
        with pytest.raises(OSError):
            with relay.relay_lock(tmp_path):
                pass


def test_diagnostic_write_error_disables_log_writes_but_keeps_draining(tmp_path, monkeypatch):
    sink = relay.BoundedLogSink(tmp_path, segment_bytes=10, total_bytes=20)
    original = Path.open
    def fail(path, *args, **kwargs):
        if path.suffix == '.log':
            raise OSError('synthetic disk write failure')
        return original(path, *args, **kwargs)
    monkeypatch.setattr(Path, 'open', fail)
    sink.write('stdout', b'a' * 100)
    sink.write('stderr', b'b' * 100)
    assert sink.snapshot()['dropped_bytes'] == 200
    assert sink.snapshot()['io_errors'] == 1
    assert sink.snapshot()['logging_disabled_after_io_error'] is True
    sink.close()


@pytest.mark.parametrize('segment,total', [(0, 1), (10, 2), (True, 10)])
def test_invalid_bounds_fail_before_files_created(tmp_path, segment, total):
    directory = tmp_path / 'new'
    with pytest.raises(ValueError):
        relay.BoundedLogSink(directory, segment, total)
    assert not directory.exists()


@pytest.mark.parametrize('priority,expected', [('Normal', 0x20), ('BelowNormal', 0x4000)])
def test_actual_windows_child_priority_is_set_at_creation_even_from_below_normal_parent(tmp_path, priority, expected):
    target = tmp_path / 'priority'
    probe = ('import ctypes;from ctypes import wintypes;'
             'k=ctypes.WinDLL("kernel32",use_last_error=True);'
             'k.GetCurrentProcess.restype=wintypes.HANDLE;'
             'k.GetPriorityClass.argtypes=[wintypes.HANDLE];'
             'k.GetPriorityClass.restype=wintypes.DWORD;'
             'print(k.GetPriorityClass(k.GetCurrentProcess()))')
    command = [sys.executable, '-B', str(Path(relay.__file__)), '--log-directory', str(target),
               '--priority-class', priority, '--', sys.executable, '-B', '-c', probe]
    result = subprocess.run(command, capture_output=True, timeout=20,
                            creationflags=subprocess.BELOW_NORMAL_PRIORITY_CLASS | subprocess.CREATE_NO_WINDOW)
    assert result.returncode == 0, result.stderr
    observed = b''.join(p.read_bytes() for p in sorted(target.glob('*stdout*.log'))).strip()
    assert int(observed) == expected
    state = json.loads((target / 'relay_status.json').read_text())
    assert state['child_priority_class'] == priority


@pytest.mark.parametrize('priority', ['High', 'Realtime', 'normal', True, None])
def test_invalid_priority_refused_before_directory_or_child(tmp_path, priority):
    target = tmp_path / 'not_created'
    with pytest.raises(ValueError, match='priority'):
        relay.relay([sys.executable, '-c', 'raise RuntimeError("must not run")'], target, priority_class=priority)
    assert not target.exists()
