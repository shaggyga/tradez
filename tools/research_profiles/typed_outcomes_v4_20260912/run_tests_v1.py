"""Owned tiny SQLite fixtures; deny network, children, real artifacts and DBs."""
import datetime,hashlib,json,os
from pathlib import Path
import sys,tempfile,urllib.parse
HERE=Path(__file__).resolve().parent;PROJECT=HERE.parents[2]
suffix=sys.argv[1];assert suffix.isalnum()
OWNED=HERE/('owned_'+suffix);OWNED.mkdir();OUTPUT=HERE/('TESTS_'+suffix+'.json');assert not OUTPUT.exists()
os.environ['PYTEST_DISABLE_PLUGIN_AUTOLOAD']='1';sys.dont_write_bytecode=True;tempfile.tempdir=str(OWNED)
sys.path[:0]=[str(HERE/'src'),str(HERE/'tests')]
tripwires=[];db_paths=[]
def deny(reason):tripwires.append(reason);raise RuntimeError('outcome_v4_boundary:'+reason)
def pathof(value):
    text=os.fsdecode(value)
    if text.startswith('file:'):
        parsed=urllib.parse.urlsplit(text);text=urllib.parse.unquote(parsed.path)
        if parsed.netloc:deny('unc_sqlite')
        if len(text)>3 and text[0]=='/' and text[2]==':':text=text[1:]
    return Path(text).absolute()
def audit(event,args):
    if event in ('socket.connect','socket.getaddrinfo','socket.bind','subprocess.Popen','os.system'):deny(event)
    if event=='import' and args and any(str(args[0])==n or str(args[0]).startswith(n+'.') for n in ('joblib','oanda_technical_account_manager_auto','oandapyV20')):deny('forbidden_import:'+str(args[0]))
    if event=='sqlite3.connect':
        raw=args[0]
        if raw!=':memory:' and not pathof(raw).is_relative_to(OWNED):deny('unowned_database')
        db_paths.append(str(raw));return
    if event not in ('open','os.mkdir','os.remove','os.rename','os.rmdir') or not args or not isinstance(args[0],(str,bytes,os.PathLike)):return
    path=pathof(args[0]);name=os.fsdecode(args[0]).lower().replace('\\','/')
    if name.startswith(('d:','//')) or path.name.lower() in ('.env','credentials.json','creds'):deny('forbidden_path')
    if event=='open':
        mode,flags=args[1:3];writing=(isinstance(mode,str) and any(x in mode for x in 'wax+')) or (isinstance(flags,int) and bool(flags&(os.O_WRONLY|os.O_RDWR|os.O_CREAT|os.O_TRUNC)))
        if writing and not(path.is_relative_to(OWNED) or path==OUTPUT):deny('unowned_write')
        if not writing and path.is_relative_to(PROJECT) and not path.is_relative_to(HERE):deny('non_snapshot_project_read')
    elif not path.is_relative_to(OWNED):deny('unowned_mutation')
    if event=='os.rename' and not pathof(args[1]).is_relative_to(OWNED):deny('unowned_rename_destination')
sys.addaudithook(audit)
import pytest
cases=[]
class Capture:
    def pytest_runtest_logreport(self,report):
        if report.when=='call' or report.failed:cases.append({'nodeid':report.nodeid,'phase':report.when,'outcome':report.outcome,'longrepr':str(report.longrepr) if report.failed else None})
code=pytest.main([str(HERE/'tests'),'-q','--capture=sys','--tb=short','-p','no:cacheprovider',
    '--confcutdir='+str(HERE),'--basetemp='+str(OWNED/'pytest'),'--log-file='+str(OWNED/'pytest.log'),
    '--junitxml='+str(OWNED/'TESTS.xml')],plugins=[Capture()])
loaded={}
for name,module in sys.modules.items():
    if name.startswith(('oanda_','outcome_shadow_','profile_probability_')):
        path=Path(module.__file__);loaded[name]={'path':str(path),'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}
OUTPUT.write_text(json.dumps({'observed_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'pytest_exit_code':int(code),'cases':cases,'tripwires':tripwires,'database_opens':db_paths,
    'loaded_project_sources':loaded,'harness_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    'test_sources':{str(p.relative_to(HERE)):hashlib.sha256(p.read_bytes()).hexdigest() for p in (HERE/'tests').glob('*.py')},
    'scope':'Synthetic clocks/models and owned tiny databases only; no real artifacts, account, network, children or services.'},indent=2)+'\n')
raise SystemExit(code)
