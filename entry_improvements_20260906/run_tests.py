"""Run targeted integration tests without runtime writes or external actions."""
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
os.environ['TEMP'] = os.environ['TMP'] = str(OUT)
sys.path[:0] = [str(ROOT), str(ROOT.parent)]

def prohibited(*args, **kwargs):
    raise RuntimeError('Offline integration check forbids worker starts')

def guard(event, args):
    if event in {'socket.connect','socket.connect_ex','socket.bind','subprocess.Popen','os.system','os.startfile'}:
        raise RuntimeError('Offline integration check forbids external actions: '+event)
    if event == 'sqlite3.connect':
        name = str(args[0])
        if name == ':memory:': return
        if name.startswith('file:'):
            name = unquote(urlsplit(name).path)
            if os.name == 'nt' and len(name) > 2 and name.startswith('/') and name[2] == ':':
                name = name[1:]
        if not Path(name).resolve().is_relative_to(OUT):
            raise RuntimeError('Offline integration check forbids non-fixture SQLite access')
    if event == 'open' and isinstance(args[0],(str,bytes,os.PathLike)):
        path, mode, flags = args
        writing = (isinstance(mode,str) and any(c in mode for c in 'wax+')) or (isinstance(flags,int) and flags & (os.O_WRONLY|os.O_RDWR|os.O_CREAT|os.O_TRUNC|os.O_APPEND))
        if writing and not Path(os.fsdecode(path)).resolve().is_relative_to(OUT):
            raise RuntimeError('Offline integration check forbids non-fixture writes')

threading.Thread.start = prohibited
sys.addaudithook(guard)
import pytest
tests = sys.argv[1:] or ['test_oanda_shadow_runtime_retirements.py',
    'test_oanda_causal_source_factor_response_map_v8.py',
    'test_oanda_source_conditioned_currency_rank_v7.py',
    'test_oanda_issue_register_validator.py']
# These unchanged quote-publisher fixtures intentionally start background
# threads. Keep them outside this task's no-worker test selection; the first
# guard rejection and all passing results are preserved in a separate XML.
blocked_thread_fixtures = [
    'test_research_snapshot_retains_seed_without_making_it_executable',
    'test_research_snapshot_preserves_oanda_tradeability_status',
    'test_initial_stream_snapshot_republishes_when_coverage_increases',
    'test_price_stream_reconnect_starts_clean_current_generation',
    'test_price_stream_startup_reclassifies_seed_as_retained',
]
raise SystemExit(pytest.main(['-q','-p','no:cacheprovider','--noconftest',
    '-k', ' and '.join('not '+name for name in blocked_thread_fixtures),
    '--basetemp',str(OUT/'integration_fixtures'), '--log-file',str(OUT/'integration.log'),
    '--junitxml',str(OUT/'integration_tests.xml'),*[str(ROOT/test) for test in tests]]))
