import os
from pathlib import Path
import sys
import threading
from urllib.parse import unquote,urlsplit
OUT=Path(__file__).resolve().parent
ROOT=OUT.parent/'trad'
sys.dont_write_bytecode=True
os.environ['PYTHONDONTWRITEBYTECODE']='1'
os.environ['PYTEST_DISABLE_PLUGIN_AUTOLOAD']='1'
os.environ['TEMP']=os.environ['TMP']=str(OUT)
sys.path[:0]=[str(ROOT),str(ROOT.parent)]
def within(value):
    if isinstance(value,(str,bytes,os.PathLike)):
        return Path(os.fsdecode(value)).resolve().is_relative_to(OUT)
    return False
def forbidden(*args,**kwargs):
    raise RuntimeError('Independent offline review forbids worker threads')
def guard(event,args):
    if event in {'socket.connect','socket.connect_ex','socket.bind','subprocess.Popen','os.system','os.startfile'}:
        raise RuntimeError('Independent offline review forbids external actions:'+event)
    if event=='sqlite3.connect':
        name=str(args[0])
        if name==':memory:': return
        if name.startswith('file:'):
            name=unquote(urlsplit(name).path)
            if os.name=='nt' and len(name)>2 and name.startswith('/') and name[2]==':': name=name[1:]
        if not within(name): raise RuntimeError('Independent review forbids non-fixture SQLite')
    if event=='open' and isinstance(args[0],(str,bytes,os.PathLike)):
        path,mode,flags=args
        writing=(isinstance(mode,str) and any(c in mode for c in 'wax+')) or (isinstance(flags,int) and flags & (os.O_WRONLY|os.O_RDWR|os.O_CREAT|os.O_TRUNC|os.O_APPEND))
        if writing and not within(path): raise RuntimeError('Independent review forbids non-fixture writes')
    if event in {'os.remove','os.rmdir','os.mkdir'} and not within(args[0]):
        raise RuntimeError('Independent review forbids non-fixture filesystem mutation')
    if event in {'os.rename','os.link','os.symlink'} and not all(within(p) for p in args[:2]):
        raise RuntimeError('Independent review forbids non-fixture filesystem move')
threading.Thread.start=forbidden
sys.addaudithook(guard)
import pytest
raise SystemExit(pytest.main(['-q','-p','no:cacheprovider','--noconftest','--basetemp',str(OUT/'fixtures'),'--junitxml',str(OUT/'study_io_tests.xml'),str(ROOT/'test_oanda_causal_forecast_study_io.py')]))
