"""Bounded local process support for the passive currency meter.

No canonical imports, network client, broker, scheduler installation or order
surface. Windows children receive a hard memory job before their first thread
runs. Termination is confined to the child tree created by this invocation.
"""
from __future__ import annotations

import ctypes
from ctypes import wintypes
import json
import math
import os
from pathlib import Path
import subprocess
import threading
import time
import uuid


STDOUT_LIMIT = 32 * 1024
STDERR_LIMIT = 8 * 1024
DEFAULT_MEMORY_BYTES = 768 * 1024 * 1024


def _finite(value, name):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError('invalid_' + name)
    return float(value)


class ProcessLock:
    """OS-owned byte lock; a stale filename is never treated as a live owner.

    Retains the pattern of trad/oanda_model_gap_live_signal_worker.py's
    WorkerProcessLock without importing its promotion or account dependencies.
    """
    def __init__(self, path):
        self.path = Path(path)
        self.handle = None

    def acquire(self):
        if self.handle is not None:
            return True
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open('a+b')
        try:
            if handle.tell() == 0:
                handle.write(b'\0')
                handle.flush()
            handle.seek(0)
            if os.name == 'nt':
                import msvcrt
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            handle.close()
            return False
        self.handle = handle
        return True

    def release(self):
        handle, self.handle = self.handle, None
        if handle is None:
            return
        try:
            handle.seek(0)
            if os.name == 'nt':
                import msvcrt
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()


def choose_bucket(now_epoch, last_bucket=None):
    """Offer only the current closed bucket during its declared start window."""
    now = _finite(now_epoch, 'wall_clock')
    if now < 0:
        raise ValueError('invalid_wall_clock')
    bucket = math.floor((now - 60) / 300) * 300
    if last_bucket is not None:
        previous = _finite(last_bucket, 'last_bucket')
        if previous != int(previous) or int(previous) % 300:
            raise ValueError('unaligned_last_bucket')
        if previous >= bucket:
            return {'bucket_epoch': None, 'next_due_epoch': int(previous) + 390,
                    'reason': 'bucket_already_attempted' if previous == bucket else 'clock_before_prior_bucket'}
    if now < bucket + 90:
        return {'bucket_epoch': None, 'next_due_epoch': bucket + 90,
                'reason': 'waiting_for_closed_bucket_headroom'}
    if now > bucket + 210:
        return {'bucket_epoch': None, 'next_due_epoch': bucket + 390,
                'reason': 'missed_current_bucket_start_window'}
    return {'bucket_epoch': bucket, 'next_due_epoch': bucket + 390,
            'reason': 'current_closed_bucket_due'}


class ClockGuard:
    """Reject wall rollback and wall/monotonic discontinuity before publishing.

    Persist ``highwater`` after successful checks. A new process must seed the
    guard from that durable value; this class does not invent continuity across
    process lifetimes where no monotonic observation was retained.
    """
    def __init__(self, prior_highwater=0):
        self.highwater = _finite(prior_highwater, 'prior_highwater')
        if self.highwater < 0:
            raise ValueError('invalid_prior_highwater')
        self.last_wall = None
        self.last_monotonic = None

    def check(self, wall, monotonic):
        wall = _finite(wall, 'wall_clock')
        monotonic = _finite(monotonic, 'monotonic_clock')
        if wall < self.highwater:
            raise ValueError('wall_clock_rollback')
        discrepancy = 0.0
        if self.last_monotonic is not None:
            if monotonic < self.last_monotonic:
                raise ValueError('monotonic_clock_rollback')
            discrepancy = (wall - self.last_wall) - (monotonic - self.last_monotonic)
            if abs(discrepancy) > 2:
                raise ValueError('wall_monotonic_discontinuity')
        self.highwater = max(self.highwater, wall)
        self.last_wall, self.last_monotonic = wall, monotonic
        return {'highwater': self.highwater, 'wall_monotonic_discrepancy_seconds': discrepancy}


def atomic_json(path, value):
    """Durable temporary bytes plus bounded atomic replacement; errors escape."""
    path = Path(path)
    payload = json.dumps(value, sort_keys=True, indent=2, allow_nan=False).encode('utf-8') + b'\n'
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name('.' + path.name + '.' + str(os.getpid()) + '.' + uuid.uuid4().hex + '.tmp')
    try:
        with temporary.open('xb') as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        for attempt in range(8):
            try:
                os.replace(temporary, path)
                return
            except PermissionError:
                if attempt == 7:
                    raise
                time.sleep(min(0.5, 0.01 * (2 ** attempt)))
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass


class _IO_COUNTERS(ctypes.Structure):
    _fields_ = [(name, ctypes.c_ulonglong) for name in
                ('ReadOperationCount', 'WriteOperationCount', 'OtherOperationCount',
                 'ReadTransferCount', 'WriteTransferCount', 'OtherTransferCount')]


class _BASIC_LIMITS(ctypes.Structure):
    _fields_ = [('PerProcessUserTimeLimit', ctypes.c_longlong),
                ('PerJobUserTimeLimit', ctypes.c_longlong),
                ('LimitFlags', wintypes.DWORD),
                ('MinimumWorkingSetSize', ctypes.c_size_t),
                ('MaximumWorkingSetSize', ctypes.c_size_t),
                ('ActiveProcessLimit', wintypes.DWORD),
                ('Affinity', ctypes.c_size_t),
                ('PriorityClass', wintypes.DWORD),
                ('SchedulingClass', wintypes.DWORD)]


class _EXTENDED_LIMITS(ctypes.Structure):
    _fields_ = [('BasicLimitInformation', _BASIC_LIMITS),
                ('IoInfo', _IO_COUNTERS),
                ('ProcessMemoryLimit', ctypes.c_size_t),
                ('JobMemoryLimit', ctypes.c_size_t),
                ('PeakProcessMemoryUsed', ctypes.c_size_t),
                ('PeakJobMemoryUsed', ctypes.c_size_t)]


class _THREADENTRY32(ctypes.Structure):
    _fields_ = [('dwSize', wintypes.DWORD), ('cntUsage', wintypes.DWORD),
                ('th32ThreadID', wintypes.DWORD), ('th32OwnerProcessID', wintypes.DWORD),
                ('tpBasePri', wintypes.LONG), ('tpDeltaPri', wintypes.LONG),
                ('dwFlags', wintypes.DWORD)]


class _WindowsJob:
    def __init__(self, memory_bytes, max_processes=2):
        self.handle = None
        self.api = ctypes.WinDLL('kernel32', use_last_error=True)
        definitions = {
            'CreateJobObjectW': ([ctypes.c_void_p, wintypes.LPCWSTR], wintypes.HANDLE),
            'SetInformationJobObject': ([wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD], wintypes.BOOL),
            'QueryInformationJobObject': ([wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD, ctypes.c_void_p], wintypes.BOOL),
            'AssignProcessToJobObject': ([wintypes.HANDLE, wintypes.HANDLE], wintypes.BOOL),
            'TerminateJobObject': ([wintypes.HANDLE, wintypes.UINT], wintypes.BOOL),
            'CloseHandle': ([wintypes.HANDLE], wintypes.BOOL),
            'CreateToolhelp32Snapshot': ([wintypes.DWORD, wintypes.DWORD], wintypes.HANDLE),
            'Thread32First': ([wintypes.HANDLE, ctypes.POINTER(_THREADENTRY32)], wintypes.BOOL),
            'Thread32Next': ([wintypes.HANDLE, ctypes.POINTER(_THREADENTRY32)], wintypes.BOOL),
            'OpenThread': ([wintypes.DWORD, wintypes.BOOL, wintypes.DWORD], wintypes.HANDLE),
            'ResumeThread': ([wintypes.HANDLE], wintypes.DWORD),
        }
        for name, (args, result) in definitions.items():
            fn = getattr(self.api, name)
            fn.argtypes, fn.restype = args, result
        self.handle = self.api.CreateJobObjectW(None, None)
        if not self.handle:
            raise OSError('job_creation_failed')
        info = _EXTENDED_LIMITS()
        # A Windows venv redirector starts a real interpreter. A capture job
        # allows those two owned processes; an outer supervisor job allows four
        # for a worker plus its separately bounded capture. Aggregate as well as
        # per-process private commit remains capped. No descendant can break away.
        info.BasicLimitInformation.LimitFlags = 0x00000008 | 0x00000100 | 0x00000200 | 0x00002000
        info.BasicLimitInformation.ActiveProcessLimit = max_processes
        info.ProcessMemoryLimit = memory_bytes
        info.JobMemoryLimit = memory_bytes
        if not self.api.SetInformationJobObject(self.handle, 9, ctypes.byref(info), ctypes.sizeof(info)):
            self.close()
            raise OSError('job_memory_limit_install_failed')

    def attach_and_resume(self, child):
        if not self.api.AssignProcessToJobObject(self.handle, wintypes.HANDLE(int(child._handle))):
            raise OSError('job_child_assignment_failed')
        snapshot = self.api.CreateToolhelp32Snapshot(0x00000004, 0)
        if snapshot == ctypes.c_void_p(-1).value:
            raise OSError('child_thread_snapshot_failed')
        try:
            entry = _THREADENTRY32()
            entry.dwSize = ctypes.sizeof(entry)
            found = []
            okay = self.api.Thread32First(snapshot, ctypes.byref(entry))
            while okay:
                if entry.th32OwnerProcessID == child.pid:
                    found.append(entry.th32ThreadID)
                okay = self.api.Thread32Next(snapshot, ctypes.byref(entry))
            if len(found) != 1:
                raise OSError('suspended_child_primary_thread_not_unique')
            thread = self.api.OpenThread(0x0002, False, found[0])
            if not thread:
                raise OSError('child_primary_thread_open_failed')
            try:
                if self.api.ResumeThread(thread) != 1:
                    raise OSError('child_primary_thread_resume_failed')
            finally:
                self.api.CloseHandle(thread)
        finally:
            self.api.CloseHandle(snapshot)

    def peak_memory(self):
        info = _EXTENDED_LIMITS()
        if self.handle and self.api.QueryInformationJobObject(self.handle, 9, ctypes.byref(info), ctypes.sizeof(info), None):
            return int(info.PeakProcessMemoryUsed)
        return None

    def terminate(self):
        if self.handle:
            self.api.TerminateJobObject(self.handle, 1)

    def close(self):
        if self.handle:
            handle, self.handle = self.handle, None
            self.api.CloseHandle(handle)


def run_bounded(command, *, cwd, timeout_seconds=75, memory_bytes=DEFAULT_MEMORY_BYTES, max_processes=2):
    """Run one owned Windows child; return small structured output only.

    ``stdout`` is the decoded JSON object, or None. Raw stderr and exception
    messages are deliberately not returned; byte counts and fixed reason codes
    expose operational failure without echoing arbitrary source content.
    """
    timeout_seconds = _finite(timeout_seconds, 'timeout_seconds')
    memory = _finite(memory_bytes, 'memory_bytes')
    if timeout_seconds <= 0 or memory < 16 * 1024 * 1024 or memory != int(memory):
        raise ValueError('invalid_child_budget')
    if type(max_processes) is not int or max_processes not in (2, 4):
        raise ValueError('invalid_max_processes')
    if isinstance(command, (str, bytes)) or not command or not all(isinstance(v, (str, os.PathLike)) for v in command):
        raise ValueError('explicit_command_arguments_required')
    command = [os.fspath(v) for v in command]
    started = time.monotonic()
    counters = {'elapsed_seconds': 0.0, 'stdout_bytes': 0, 'stderr_bytes': 0,
                'peak_process_memory_bytes': None, 'memory_limit_bytes': int(memory),
                'memory_limit_enforced': False, 'max_processes': max_processes, 'child_pid': None}
    result = {'exit_code': None, 'reason': None, 'stdout': None, 'counters': counters}
    if os.name != 'nt':
        result['reason'] = 'hard_memory_limit_platform_unsupported'
        return result
    child = job = None
    buffers = {'stdout': bytearray(), 'stderr': bytearray()}
    limits = {'stdout': STDOUT_LIMIT, 'stderr': STDERR_LIMIT}
    overflow = threading.Event()
    read_failure = threading.Event()
    threads = []

    def drain(stream, name):
        try:
            while True:
                chunk = stream.read(4096)
                if not chunk:
                    break
                counters[name + '_bytes'] += len(chunk)
                room = max(0, limits[name] - len(buffers[name]))
                buffers[name].extend(chunk[:room])
                if counters[name + '_bytes'] > limits[name]:
                    overflow.set()
        except (OSError, ValueError):
            read_failure.set()
        finally:
            stream.close()

    try:
        job = _WindowsJob(int(memory), max_processes=max_processes)
        # CREATE_SUSPENDED closes the launch/limit race. The primary thread is
        # resumed only after assignment to this invocation's restrictive job.
        child = subprocess.Popen(command, cwd=Path(cwd).resolve(strict=True),
                                 stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, shell=False,
                                 creationflags=subprocess.CREATE_NO_WINDOW | 0x00000004)
        counters['child_pid'] = child.pid
        job.attach_and_resume(child)
        counters['memory_limit_enforced'] = True
        for name in ('stdout', 'stderr'):
            thread = threading.Thread(target=drain, args=(getattr(child, name), name), daemon=True)
            thread.start()
            threads.append(thread)
        deadline = started + timeout_seconds
        while child.poll() is None:
            if overflow.is_set():
                result['reason'] = 'stdout_limit_exceeded' if counters['stdout_bytes'] > STDOUT_LIMIT else 'stderr_limit_exceeded'
                job.terminate()
                break
            if time.monotonic() >= deadline:
                result['reason'] = 'child_timeout'
                job.terminate()
                break
            time.sleep(min(0.02, max(0.001, deadline - time.monotonic())))
        child.wait(timeout=5)
        result['exit_code'] = child.returncode
        for thread in threads:
            thread.join(timeout=2)
        if result['reason'] is None:
            if any(t.is_alive() for t in threads) or read_failure.is_set():
                result['reason'] = 'child_output_read_failed'
            elif counters['stdout_bytes'] > STDOUT_LIMIT:
                result['reason'] = 'stdout_limit_exceeded'
            elif counters['stderr_bytes'] > STDERR_LIMIT:
                result['reason'] = 'stderr_limit_exceeded'
            else:
                try:
                    parsed = json.loads(bytes(buffers['stdout']).decode('utf-8'),
                                        parse_constant=lambda _: (_ for _ in ()).throw(ValueError('nonfinite')))
                    if not isinstance(parsed, dict):
                        raise ValueError('object_required')
                    result['stdout'] = parsed
                    result['reason'] = 'completed' if child.returncode == 0 else 'child_failed'
                except (UnicodeError, ValueError, RecursionError):
                    result['reason'] = 'child_output_invalid_json' if child.returncode == 0 else 'child_failed'
    except (OSError, ValueError, subprocess.SubprocessError, AttributeError):
        result['reason'] = 'child_launch_or_limit_failed' if not counters['memory_limit_enforced'] else 'child_runtime_failed'
    finally:
        if child is not None and child.poll() is None:
            if job is not None:
                job.terminate()
            try:
                child.kill()
                child.wait(timeout=5)
            except (OSError, subprocess.SubprocessError):
                pass
        if child is not None:
            result['exit_code'] = child.poll()
        if job is not None:
            counters['peak_process_memory_bytes'] = job.peak_memory()
            job.close()
        for thread in threads:
            thread.join(timeout=1)
        if child is not None:
            for name in ('stdout', 'stderr'):
                stream = getattr(child, name)
                if stream is not None and not stream.closed:
                    stream.close()
        counters['elapsed_seconds'] = round(time.monotonic() - started, 6)
    return result
