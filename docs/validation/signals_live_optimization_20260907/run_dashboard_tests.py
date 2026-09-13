"""Fixture-only dashboard verification; writes only a new evidence run directory."""
import sys
sys.dont_write_bytecode=True
import os
from pathlib import Path
import json
from datetime import datetime,timezone
from urllib.parse import urlsplit,unquote
import hashlib
import shutil
import subprocess

OUT=Path(__file__).resolve().parent
ROOT=OUT.parent/'trad'
stamp=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
RUN=OUT/('dashboard_tests_'+stamp)
RUN.mkdir()
os.environ['TEMP']=str(RUN)
os.environ['TMP']=str(RUN)
import tempfile
tempfile.tempdir=str(RUN)
os.environ['PYTEST_DISABLE_PLUGIN_AUTOLOAD']='1'
os.environ['PYTHONDONTWRITEBYTECODE']='1'
denied=[]
node_path=shutil.which('node')
node_prefix=subprocess.list2cmdline([node_path,'-e'])+' ' if node_path else None


def guard(event,args):
    if event in {'socket.connect','socket.connect_ex','socket.bind','os.system','os.startfile'}:
        denied.append(event)
        raise RuntimeError('Dashboard fixture run forbids network/process activation')
    if event=='subprocess.Popen':
        executable,argv,*_=args
        windows_node=executable is None and isinstance(argv,str) and node_prefix is not None and argv.startswith(node_prefix)
        explicit_node=Path(str(executable)).name.lower() in {'node','node.exe'} and isinstance(argv,(list,tuple)) and '-e' in argv
        if not (windows_node or explicit_node):
            denied.append(event)
            raise RuntimeError('Only isolated Node -e rendering fixtures allowed')
    if event=='sqlite3.connect':
        value=str(args[0]);parsed=urlsplit(value)
        path=unquote(parsed.path) if parsed.scheme=='file' else value
        if path.startswith('/') and len(path)>2 and path[2]==':':path=path[1:]
        if not Path(path).resolve().is_relative_to(RUN.resolve()):
            denied.append(event)
            raise RuntimeError('Only disposable fixture databases allowed')
    if event=='open' and isinstance(args[0],(str,bytes,os.PathLike)):
        path,mode,flags=args
        writing=(isinstance(mode,str) and any(c in mode for c in 'wax+')) or (isinstance(flags,int) and flags&(os.O_WRONLY|os.O_RDWR|os.O_CREAT|os.O_TRUNC|os.O_APPEND))
        if writing and os.fsdecode(path).lower() not in {'nul',r'\\.\nul'} and not Path(os.fsdecode(path)).resolve().is_relative_to(RUN.resolve()):
            denied.append('write:'+str(path))
            raise RuntimeError('Writes restricted to new evidence run')


sys.addaudithook(guard)
sys.path.insert(0,str(ROOT))
import pytest
code=pytest.main(['-q','-p','no:cacheprovider','--basetemp',str(RUN/'fixtures'),
    '--junitxml',str(RUN/'tests.xml'),str(ROOT/'test_oanda_collection_dashboard_status.py'),
    str(ROOT/'test_oanda_executor_dashboard_diagnostics.py')])
receipt={'generated_utc':datetime.now(timezone.utc).isoformat(),'exit_code':code,'denied_attempts':denied,
    'source_hashes':{name:hashlib.sha256((ROOT/name).read_bytes()).hexdigest() for name in
        ('oanda_practice_live_dashboard.py','oanda_main_signal_dashboard.html','test_oanda_collection_dashboard_status.py')},
    'runtime_started':False,'production_database_connections':0,'run_directory':str(RUN)}
with (RUN/'receipt.json').open('x',encoding='utf-8') as handle:json.dump(receipt,handle,indent=2)
print(json.dumps(receipt,indent=2))
raise SystemExit(code)
