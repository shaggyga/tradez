"""Guarded fixture tests for compact integrity publication, no runtime calls."""
import os
from pathlib import Path
import sys
import threading
ROOT = Path(r'C:\Users\zmoor\Documents\forex\trad')
OUT = Path(__file__).resolve().parent
sys.path[:0] = [str(ROOT), str(ROOT.parent)]
sys.dont_write_bytecode = True
os.environ['PYTHONDONTWRITEBYTECODE'] = '1'
os.environ['PYTEST_DISABLE_PLUGIN_AUTOLOAD'] = '1'
os.environ['TEMP'] = str(OUT)
os.environ['TMP'] = str(OUT)
def guard(event, args):
    if event in {'socket.connect','socket.bind','subprocess.Popen','os.system','os.startfile'}:
        raise RuntimeError('Offline publication tests forbid external actions')
    if event == 'sqlite3.connect':
        value = str(args[0])
        if value != ':memory:':
            if value.startswith('file:'):
                value = value[5:].split('?',1)[0]
            if not Path(value).resolve().is_relative_to(OUT):
                raise RuntimeError('Offline tests forbid nonfixture SQLite')
    if event == 'open' and isinstance(args[0], (str,bytes,os.PathLike)):
        path, mode, flags = args
        writing = isinstance(mode,str) and any(c in mode for c in 'wax+')
        writing |= isinstance(flags,int) and bool(flags & (os.O_WRONLY|os.O_RDWR|os.O_CREAT|os.O_TRUNC|os.O_APPEND))
        if writing and not Path(os.fsdecode(path)).resolve().is_relative_to(OUT):
            raise RuntimeError('Offline tests forbid writes outside optimization fixtures')
def no_thread(*args, **kwargs):
    raise RuntimeError('Offline tests forbid worker starts')
threading.Thread.start = no_thread
sys.addaudithook(guard)
import pytest
code = pytest.main(['-q','-p','no:cacheprovider','-p','no:logging',str(ROOT/'test_oanda_integrity_publication.py'),str(ROOT/'test_oanda_project_integrity_audit.py')+'::test_stale_integrity_pass_cannot_overwrite_newer_publication',str(ROOT/'test_oanda_project_integrity_audit.py')+'::test_integrity_history_rotation_preserves_every_byte',str(ROOT/'test_oanda_project_integrity_audit.py')+'::test_owned_publication_rotates_oversized_history_before_append','--basetemp',str(OUT/'pytest_publication'),'--junitxml',str(OUT/'pytest_publication.xml')])
raise SystemExit(code)

