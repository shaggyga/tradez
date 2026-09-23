"""Fixture-only independent ledger review; external actions and runtime writes denied."""
import os
from pathlib import Path
import sys
import threading
from urllib.parse import unquote, urlsplit

OUT = Path(__file__).resolve().parent
ROOT = OUT.parent / 'trad'
sys.dont_write_bytecode = True
os.environ['PYTHONDONTWRITEBYTECODE'] = '1'
os.environ['PYTEST_DISABLE_PLUGIN_AUTOLOAD'] = '1'
for key in ('OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS', 'LOKY_MAX_CPU_COUNT'):
    os.environ[key] = '1'
os.environ['TEMP'] = os.environ['TMP'] = str(OUT)
sys.path[:0] = [str(ROOT), str(ROOT.parent)]


def prohibited(*args, **kwargs):
    raise RuntimeError('Offline ledger tests forbid worker starts')


def guard(event, args):
    if event in {'socket.connect', 'socket.connect_ex', 'socket.bind', 'subprocess.Popen', 'os.system', 'os.startfile'}:
        raise RuntimeError('Offline ledger tests forbid external action: ' + event)
    if event == 'sqlite3.connect':
        name = str(args[0])
        if name == ':memory:':
            return
        if name.startswith('file:'):
            name = unquote(urlsplit(name).path)
            if os.name == 'nt' and name.startswith('/') and len(name) > 2 and name[2] == ':':
                name = name[1:]
        if not Path(name).resolve().is_relative_to(OUT):
            raise RuntimeError('Offline ledger tests forbid non-fixture SQLite')
    if event == 'open' and isinstance(args[0], (str, bytes, os.PathLike)):
        path, mode, flags = args
        writing = (isinstance(mode, str) and any(c in mode for c in 'wax+')) or (
            isinstance(flags, int) and flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND))
        if writing and not Path(os.fsdecode(path)).resolve().is_relative_to(OUT):
            raise RuntimeError('Offline ledger tests forbid non-fixture writes')


threading.Thread.start = prohibited
sys.addaudithook(guard)
import pytest

name = sys.argv[1] if len(sys.argv) > 1 else 'ledger_review_initial'
raise SystemExit(pytest.main(['-q', '-p', 'no:cacheprovider', '--noconftest',
    '--basetemp', str(OUT / (name + '_fixtures')),
    '--log-file', str(OUT / (name + '.log')),
    '--junitxml', str(OUT / (name + '.xml')),
    str(ROOT / 'test_oanda_causal_forecast_ledger.py')]))
