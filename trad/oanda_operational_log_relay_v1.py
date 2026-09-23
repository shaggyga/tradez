"""Bounded diagnostic stdout/stderr relay; never interprets or alters child data.

The supervisor supplies an exact validated command. Each role has a fixed total
diagnostic byte budget across relay generations. Existing segments are retained.
When full, streams are drained and discarded with explicit counters; the worker
continues writing its authoritative status/data independently. No credentials or
command arguments are written to relay state. This is not a trading launcher.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import threading
import time
import uuid
from contextlib import contextmanager


def plain_path(path):
    path = Path(os.path.abspath(path))
    for node in (path, *path.parents):
        if node.exists() and (node.is_symlink() or getattr(node.lstat(), 'st_file_attributes', 0) & 1024):
            raise ValueError('reparse_log_path_refused')
    return path


class BoundedLogSink:
    """Thread-safe binary segments; limits apply before writing, never truncate."""
    def __init__(self, directory, segment_bytes=8 * 1024**2, total_bytes=128 * 1024**2):
        if type(segment_bytes) is not int or type(total_bytes) is not int or not 1 <= segment_bytes <= total_bytes:
            raise ValueError('positive_bounded_log_limits_required')
        self.directory = plain_path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.segment_bytes, self.total_bytes = segment_bytes, total_bytes
        self.retained_bytes = 0
        for item in self.directory.iterdir():
            if item.is_symlink() or getattr(item.lstat(), 'st_file_attributes', 0) & 1024:
                raise ValueError('reparse_log_entry_refused')
            if item.is_file() and item.name.endswith('.log'):
                self.retained_bytes += item.stat().st_size
        self.written_bytes = self.dropped_bytes = self.io_errors = 0
        self.logging_disabled = False
        self.streams = {}
        self.lock = threading.Lock()
        self.generation = uuid.uuid4().hex

    def write(self, stream, data):
        if stream not in ('stdout', 'stderr') or not isinstance(data, bytes):
            raise ValueError('binary_known_stream_required')
        with self.lock:
            remaining = data
            if self.logging_disabled:
                self.dropped_bytes += len(remaining)
                return
            while remaining:
                capacity = max(0, self.total_bytes - self.retained_bytes)
                if not capacity:
                    self.dropped_bytes += len(remaining)
                    break
                current = self.streams.get(stream)
                try:
                    if current is None or current[1] >= self.segment_bytes:
                        if current is not None:
                            current[0].close()
                        index = 0 if current is None else current[2] + 1
                        target = self.directory / f'{self.generation}_{stream}_{index:05d}.log'
                        current = [target.open('xb'), 0, index]
                        self.streams[stream] = current
                    count = min(len(remaining), capacity, self.segment_bytes - current[1])
                    current[0].write(remaining[:count])
                    current[0].flush()
                except (OSError, ValueError):
                    self.io_errors += 1
                    self.logging_disabled = True
                    self.dropped_bytes += len(remaining)
                    break
                current[1] += count
                self.retained_bytes += count
                self.written_bytes += count
                remaining = remaining[count:]

    def snapshot(self):
        with self.lock:
            return dict(retained_bytes=self.retained_bytes, written_bytes=self.written_bytes,
                        dropped_bytes=self.dropped_bytes, io_errors=self.io_errors,
                        logging_disabled_after_io_error=self.logging_disabled,
                        diagnostic_budget_exhausted=self.retained_bytes >= self.total_bytes,
                        segment_limit_bytes=self.segment_bytes, total_limit_bytes=self.total_bytes)

    def close(self):
        with self.lock:
            for handle, _, _ in self.streams.values():
                handle.close()


def atomic_state(path, value):
    target = path.with_name(f'.{path.name}.{uuid.uuid4().hex}.tmp')
    try:
        target.write_text(json.dumps(value, separators=(',', ':'), allow_nan=False), encoding='utf-8')
        os.replace(target, path)
    finally:
        target.unlink(missing_ok=True)


@contextmanager
def relay_lock(directory):
    """One relay per role; existing adopted workers are not claimed by this lock."""
    import msvcrt
    root = plain_path(directory)
    root.mkdir(parents=True, exist_ok=True)
    path = plain_path(root / 'relay.lock')
    with path.open('a+b') as handle:
        if handle.seek(0, 2) == 0:
            handle.write(b'\0')
            handle.flush()
        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        try:
            yield
        finally:
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)


def relay(command, directory, *, segment_bytes=8 * 1024**2, total_bytes=128 * 1024**2):
    sink = BoundedLogSink(directory, segment_bytes, total_bytes)
    child = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
    def drain(name, source):
        try:
            while True:
                chunk = source.read1(65536)
                if not chunk:
                    break
                sink.write(name, chunk)
        finally:
            source.close()
    threads = [threading.Thread(target=drain, args=(name, source), daemon=True)
               for name, source in [('stdout', child.stdout), ('stderr', child.stderr)]]
    for thread in threads:
        thread.start()
    def state():
        return dict(schema='operational_log_relay_v1_20260916', pid=os.getpid(), child_pid=child.pid,
                    observed_utc=datetime.now(timezone.utc).isoformat(), child_exit_code=child.poll(),
                    scope='Diagnostic streams only; authoritative worker outputs unaffected.', **sink.snapshot())
    try:
        while child.poll() is None:
            try:
                atomic_state(sink.directory / 'relay_status.json', state())
            except OSError:
                pass  # Diagnostic I/O must not stop draining a running child.
            time.sleep(1)
        for thread in threads:
            thread.join(timeout=10)
        atomic_state(sink.directory / 'relay_status.json', state())
        return child.returncode
    finally:
        sink.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--log-directory', required=True)
    parser.add_argument('--segment-bytes', type=int, default=8 * 1024**2)
    parser.add_argument('--total-bytes', type=int, default=128 * 1024**2)
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ['--'] else args.command
    if not command:
        parser.error('An explicit validated child command is required.')
    with relay_lock(args.log_directory):
        return relay(command, args.log_directory, segment_bytes=args.segment_bytes, total_bytes=args.total_bytes)


if __name__ == '__main__':
    raise SystemExit(main())
