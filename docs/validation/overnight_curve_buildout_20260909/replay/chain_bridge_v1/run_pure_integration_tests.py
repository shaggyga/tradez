"""One explicitly bounded pure integration run; no manager executable imports."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import xml.etree.ElementTree as ET

HERE=Path(__file__).resolve().parent
ROOT=HERE.parents[2]/'trad'
SOURCE_NAMES=['oanda_curve_chain_management_bridge_v1.py','oanda_curve_management_channels_v1.py',
    'oanda_curve_direction_admission_v1.py','oanda_curve_management_adapter_v1.py',
    'oanda_forecast_curve_contract_v1.py','oanda_curve_management_replay_v1.py',
    'src/forex_system/research/sequential_portfolio_replay_v1.py']
TEST_NAMES=['test_oanda_curve_chain_management_bridge_v1.py','test_oanda_curve_management_channels_v1.py',
    'test_oanda_curve_direction_admission_v1.py','test_oanda_curve_management_adapter_v1.py',
    'test_oanda_forecast_curve_contract_v1.py','test_oanda_curve_management_replay_v1.py']

def binding(path):
    raw=path.read_bytes();return dict(path=str(path),sha256=hashlib.sha256(raw).hexdigest(),bytes=len(raw))

def save(path,value):
    raw=value if isinstance(value,bytes) else (json.dumps(value,indent=2,sort_keys=True,allow_nan=False)+'\n').encode()
    with path.open('xb') as handle:handle.write(raw);handle.flush();os.fsync(handle.fileno())
    return binding(path)

def main():
    xml=HERE/'chain_bridge_pure_integration_tests.xml'
    before_path=HERE/'PURE_INTEGRATION_BEFORE_BINDINGS_20260909.json'
    need=[xml,before_path,HERE/'PURE_INTEGRATION_RESULT_20260909.json']
    assert not any(path.exists() for path in need)
    sources=[binding(ROOT/name) for name in SOURCE_NAMES+TEST_NAMES]
    args=[sys.executable,'-B','-m','pytest','-q','-p','no:cacheprovider',*TEST_NAMES,'--junitxml='+str(xml)]
    before=save(before_path,dict(started_epoch=time.time(),sources=sources,args=args,cwd=str(ROOT),
        scope='Safe pure contract/selector tests only; excludes all account manager modules and executables.'))
    started=time.time()
    proc=subprocess.run(args,cwd=ROOT,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,
        env={**os.environ,'PYTHONDONTWRITEBYTECODE':'1'},timeout=180)
    completed=time.time()
    log=save(HERE/'pure_integration_stdout.txt',proc.stdout)
    unchanged=all(binding(Path(item['path']))==item for item in sources)
    root=ET.parse(xml).getroot() if xml.exists() else None
    counts=None if root is None else dict(cases=len(list(root.iter('testcase'))),failures=len(list(root.iter('failure'))),
        errors=len(list(root.iter('error'))),skips=len(list(root.iter('skipped'))))
    result=dict(status='passed' if proc.returncode==0 and unchanged else 'failed',started_epoch=started,
        completed_epoch=completed,wall_duration_sec=completed-started,returncode=proc.returncode,
        source_bindings_unchanged=unchanged,before_bindings=before,stdout=log,xml=binding(xml) if xml.exists() else None,
        counts=counts,args=args,cwd=str(ROOT),source_bindings=sources,
        test_scope='One new integration run; do not sum overlapping cases with earlier focused runs.',
        manager_imports=False,broker_requests=False,runtime_actions=False)
    print(proc.stdout.decode(errors='replace'))
    print(json.dumps(save(HERE/'PURE_INTEGRATION_RESULT_20260909.json',result)))
    raise SystemExit(proc.returncode if unchanged else 3)

if __name__=='__main__':main()
