"""Record isolated gate verification; never import or execute the supervisor."""
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json
import xml.etree.ElementTree as ET

OUT = Path(__file__).resolve().parent
SUPERVISOR = OUT.parent/'trad/oanda_always_on_supervisor.ps1'


def binding(path):
    raw = path.read_bytes()
    return {'path':str(path.resolve()), 'sha256':hashlib.sha256(raw).hexdigest(), 'bytes':len(raw)}


def test_evidence(name, note):
    path = OUT/name
    suites = list(ET.parse(path).getroot().iter('testsuite'))
    counts = {key:sum(int(s.get(key,0)) for s in suites) for key in ('tests','failures','errors','skipped')}
    return dict(binding(path), counts=counts, note=note)


source = SUPERVISOR.read_text(encoding='utf-8-sig')
gate = source.split('$CausalStudyConfig = ',1)[1].split('\n$SupervisorMutex = ',1)[0]
test_source = (OUT/'test_supervisor_v2_registration_gate.py').read_text(encoding='utf-8')
assert '[Security.Cryptography.SHA256]::Create()' in gate
assert 'Import-Module' not in test_source
primary = test_evidence('supervisor_v2_dotnet_hash_gate_tests.xml',
    'Final 41 cases execute the extracted .NET hashing gate in ordinary inherited Windows PowerShell without modifying PSModulePath or importing Utility explicitly.')
assert primary['counts'] == {'tests':41,'failures':0,'errors':0,'skipped':0}
value = {
    'schema_version':'pair_v2_supervisor_gate_validation_20260907','status':'passed',
    'generated_utc':datetime.now(timezone.utc).isoformat(),
    'source_bindings':[binding(SUPERVISOR),binding(OUT/'test_supervisor_v2_registration_gate.py')],
    'registration_gate_text_sha256':hashlib.sha256(gate.encode()).hexdigest(),
    'primary_test_evidence':primary,
    'prior_test_evidence':[
        test_evidence('supervisor_v2_gate_tests.xml','40 passed, one valid-selector case failed before hash dependency repair; failure retained.'),
        test_evidence('supervisor_v2_gate_diagnostic_tests.xml','40 passed, one valid-selector failure; added diagnostic output identified unavailable Get-FileHash under inherited module path.'),
        test_evidence('supervisor_v2_gate_final_tests.xml','Intermediate 41 passed with explicit built-in Utility module loading. This established the missing command cause but did not validate the ordinary inherited restart environment; superseded by the primary .NET gate run.')],
    'distinct_final_pytest_case_count':41,
    'method':'Extract only the research allowlist, configuration/selection block and disabled-reason expression from current source. Execute synthetic configurations under isolated temporary trad directories. Never run the full supervisor, process launch/stop functions, credential prefix, global mutex, runtime loop or actual project config.',
    'cases':['Missing registry and malformed JSON','Wrong schema and strict boolean enabled/research-only checks',
        'All six inert flags reject true, string false, null and numeric zero',
        'Valid v2 without valid selection keeps both superseded studies and comparison pair v1',
        'Wrong selection schema/hash/version and zero/string/boolean/future activation epochs cannot retire legacy',
        'Valid selected v2 retires exactly shared-gap and EUR-local workers; pair v1 and account snapshot remain allowed',
        'Unsafe v2 registry cannot retire legacy even with matching selector',
        'Unknown order executor and promotion worker remain blocked in every case'],
    'review_findings_closed':[
        'Root changed selector activation to finite positive numeric epoch no later than current observation; JSON true and future timestamps are rejected.',
        'Inherited Windows PowerShell may lack Get-FileHash. Root replaced the new selector hashing call with .NET SHA256 over actual file bytes and disposed the hasher; final test uses the failing original environment without harness module loading.'],
    'runtime_restart_performed':False,'project_configuration_changed_by_tests':False,
    'broker_requests':0,'can_place_orders':False,
    'limits':['This bounded gate test does not replace root runtime restart, worker source/contract registration verification or live publication checks.',
              'Repeated test-run counts are retained separately, not summed into distinct coverage.'],
}
path = OUT/'SUPERVISOR_V2_GATE_VALIDATION_20260907.json'
with path.open('x',encoding='utf-8') as handle:
    json.dump(value,handle,indent=2,sort_keys=True,allow_nan=False)
    handle.write('\n')
print(json.dumps(binding(path)))
