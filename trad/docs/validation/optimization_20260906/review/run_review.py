"""Independent focused compact-publication regression run, fixture-only."""
from pathlib import Path
import os
import sys
import threading

ROOT=Path(r"C:\Users\zmoor\Documents\forex\trad")
OUT=Path(__file__).resolve().parent
sys.dont_write_bytecode=True
os.environ["PYTHONDONTWRITEBYTECODE"]="1"
os.environ["PYTEST_DISABLE_PLUGIN_AUTOLOAD"]="1"
os.environ["TEMP"]=os.environ["TMP"]=str(OUT)
sys.path[:0]=[str(ROOT),str(ROOT.parent)]

def guard(event,args):
    if event in {"socket.connect","socket.connect_ex","socket.bind","subprocess.Popen","os.system","os.startfile"}:
        raise RuntimeError("Independent review blocked external action")
    if event=="sqlite3.connect":
        value=str(args[0])
        if value==":memory:":return
        if value.startswith("file:"):value=value[5:].split("?",1)[0]
        if not Path(value).resolve().is_relative_to(OUT):raise RuntimeError("Independent review blocked nonfixture SQLite")
    if event=="open" and isinstance(args[0],(str,bytes,os.PathLike)):
        value,mode,flags=args
        path=Path(os.fsdecode(value)).resolve()
        writing=isinstance(mode,str) and any(c in mode for c in "wax+")
        writing|=isinstance(flags,int) and bool(flags&(os.O_WRONLY|os.O_RDWR|os.O_CREAT|os.O_TRUNC|os.O_APPEND))
        if writing and not path.is_relative_to(OUT):raise RuntimeError("Independent review blocked nonfixture write")
        if not path.is_relative_to(OUT) and any(part.lower() in {"creds","creds.txt",".env"} for part in path.parts):
            raise RuntimeError("Independent review blocked private credential read")

def no_thread(*args,**kwargs):raise RuntimeError("Independent review blocked thread start")
threading.Thread.start=no_thread
sys.addaudithook(guard)
import pytest
raise SystemExit(pytest.main([
    "-q","-p","no:cacheprovider","-p","no:logging","--basetemp",str(OUT/"t"),
    str(ROOT/"test_oanda_integrity_publication.py"),
    str(ROOT/"test_forex_integrity_detail_export.py"),
    str(ROOT/"test_oanda_four_hour_best_improvement_pass.py"),
    "--junitxml="+str(OUT/"review_tests.xml"),*sys.argv[1:]
]))
