"""Write a new offline test receipt; no market data or runtime operations."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import platform
import statistics
import sys
import time
import xml.etree.ElementTree as ET

OUT = Path(__file__).resolve().parent
ROOT = OUT.parent.parent / 'trad'
sys.dont_write_bytecode = True
sys.path.insert(0, str(ROOT))
import oanda_weekend_reopening_baseline as model
import test_oanda_weekend_reopening_baseline as fixtures

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

suite = ET.parse(OUT / 'integration_tests.xml').getroot().find('testsuite')
assert suite is not None and suite.attrib['failures'] == '0' and suite.attrib['errors'] == '0'
inputs = fixtures.inputs.__wrapped__()
timings = []
for _ in range(25):
    started = time.perf_counter()
    result = model.forecast_weekend_reopening(**inputs)
    timings.append(time.perf_counter() - started)
assert result['status'] == 'hypotheses'
contract = json.loads((ROOT / 'config/causal_forecast_study_v1_io_r2_20260906.json').read_bytes())
bindings = {name: {'expected': expected, 'actual': sha(ROOT / name)}
            for name, expected in contract['source_bindings'].items()}
assert all(row['expected'] == row['actual'] for row in bindings.values())
receipt = {
    'schema_version': 'weekend_reopening_baseline_validation_v1_20260907',
    'generated_utc': datetime.now(timezone.utc).isoformat(),
    'source_sha256': {name: sha(ROOT / name) for name in (
        'oanda_weekend_reopening_baseline.py', 'test_oanda_weekend_reopening_baseline.py',
        'oanda_exact_price_scoring.py')},
    'evidence_sha256': {'integration_tests.xml': sha(OUT / 'integration_tests.xml'),
                        'run_tests.py': sha(OUT / 'run_tests.py')},
    'tests': dict(suite.attrib), 'python_executable': sys.executable, 'python_version': platform.python_version(),
    'invocation': "& 'C:\\Users\\zmoor\\AppData\\Local\\CodexRuntimes\\timeseries312\\Scripts\\python.exe' -B '..\\warmup_repair_20260907\\weekend_baseline\\run_tests.py' test_oanda_weekend_reopening_baseline.py",
    'parameters': dict(model.PARAMETERS), 'parameters_sha256': result['parameters_sha256'],
    'fixture_timing': {'repetitions': 25, 'median_sec': statistics.median(timings),
                       'scope': 'pure helper, two synthetic price records; not whole-project performance'},
    'registered_six_source_bindings_unchanged': bindings,
    'historical_real_data_accuracy_established': False,
    'prospective_forecasts_created': 0, 'runtime_activated': False, 'database_writes': False,
    'external_orders_or_network': False, 'source_registration_changed': False,
    'limitations': [
        'Synthetic clocks are fixture clocks only; no historic or current market performance was tested.',
        'Calendar and actual observation provenance are caller assertions, not independent attestation.',
        'Late opening decisions and elapsed targets abstain; Monday replay cannot become a Sunday forecast.',
        'Fixed research spread/gap limits were not historically tuned and do not authorize execution.',
        'Future integration requires separate registration, immutable receipt publication, unique event enforcement, and independent executable entry/target quotes.',
    ],
}
destination = OUT / 'WEEKEND_BASELINE_VALIDATION_20260907.json'
with destination.open('x', encoding='utf-8') as handle:
    json.dump(receipt, handle, indent=2, allow_nan=False)
    handle.write('\n')
print(json.dumps({'receipt': str(destination), 'sha256': sha(destination), 'tests': suite.attrib['tests'],
                  'median_sec': statistics.median(timings)}))
