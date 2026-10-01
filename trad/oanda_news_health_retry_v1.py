"""One new local capture after a strictly healthy recheck, never stale fallback."""
import os
import time


def install_health_read_retry(io, paths, *, record, sleep=time.sleep):
    """Retry only permission failures reading the configured health files.

    Every attempt executes the complete original bounded/path-checked read.
    Callers still validate bytes, source identity and their original clock.
    No capture is reused or made usable by this transport adapter.
    """
    normalize = lambda p: os.path.normcase(os.path.abspath(os.fspath(p)))
    allowed = frozenset(normalize(p) for p in paths)
    original = io.read_exact
    def read(value, limit):
        if normalize(value) not in allowed:
            return original(value, limit)
        for attempt in range(4):
            try:
                result = original(value, limit)
            except PermissionError:
                if attempt == 3:
                    record('health_read_retry_exhausted', path=normalize(value), attempts=4)
                    raise
                sleep((.02, .05, .1)[attempt])
            else:
                if attempt:
                    record('health_read_recovered', path=normalize(value),
                           attempts=attempt + 1, sha256=result[1]['sha256'])
                return result
    io.read_exact = read


def capture_with_health_retry(capture, args, kwargs, *, recheck, record):
    try:
        return capture(*args, **kwargs)
    except ValueError as original:
        if str(original) != 'upstream_collector_stale_or_future':
            raise
        # Recheck uses the original clock and collector validators. If health is
        # still refused, retain the original failure without another capture.
        try:
            proof = recheck()
        except (OSError, ValueError):
            raise original
        record('healthy_recheck_retry', original_refusal=str(original),
               health_observed_epoch=proof['observed_epoch'],
               health_files_sha256={k: v['sha256'] for k, v in proof['files'].items()})
        # A whole new capture must pass all original gates. No recursive retry,
        # no edited clocks, cached snapshot substitution or renewed handle.
        return capture(*args, **kwargs)


class NewsExecutor:
    def __init__(self, executor, capture, *, recheck, record):
        self.executor = executor
        self.capture = capture
        self.recheck = recheck
        self.record = record

    def submit(self, function, *args, **kwargs):
        if function is not self.capture:
            return self.executor.submit(function, *args, **kwargs)
        return self.executor.submit(capture_with_health_retry, function, args, kwargs,
                                    recheck=self.recheck, record=self.record)

    def shutdown(self, **kwargs):
        return self.executor.shutdown(**kwargs)
