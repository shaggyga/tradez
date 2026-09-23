"""Create a new source-bound offline model validation receipt."""
from datetime import datetime, timezone
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import statistics
import sys
import time
import xml.etree.ElementTree as ET

for name in ('OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS'):
    os.environ[name] = '1'
OUT = Path(__file__).resolve().parent
ROOT = OUT.parent.parent / 'trad'
sys.dont_write_bytecode = True
sys.path.insert(0, str(ROOT))
import oanda_gap_aware_four_family_models as model
import test_oanda_gap_aware_four_family_models as fixtures

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

suite = ET.parse(OUT / 'integration_tests.xml').getroot().find('testsuite')
assert suite is not None and suite.attrib['failures'] == '0' and suite.attrib['errors'] == '0'
rows, cutoff = fixtures.make_rows()
timings = []
for _ in range(5):
    started = time.perf_counter()
    output = model.predict_all(rows, cutoff)
    timings.append(time.perf_counter() - started)
assert set(output) == set(model.FAMILIES)
contract = json.loads((ROOT / 'config/causal_forecast_study_v1_io_r2_20260906.json').read_bytes())
bindings = {name: {'expected': expected, 'actual': sha(ROOT / name)}
            for name, expected in contract['source_bindings'].items()}
assert all(row['expected'] == row['actual'] for row in bindings.values())
receipt = {
    'schema_version': 'gap_aware_four_family_offline_validation_v1_20260907',
    'generated_utc': datetime.now(timezone.utc).isoformat(),
    'source_sha256': {name: sha(ROOT / name) for name in (
        'oanda_gap_aware_four_family_models.py', 'test_oanda_gap_aware_four_family_models.py')},
    'evidence_sha256': {'integration_tests.xml': sha(OUT / 'integration_tests.xml'),
                        'run_tests.py': sha(OUT / 'run_tests.py')},
    'tests': dict(suite.attrib), 'python_executable': sys.executable,
    'dependencies': {'python': platform.python_version(),
                     'numpy': importlib.metadata.version('numpy'),
                     'scikit-learn': importlib.metadata.version('scikit-learn'),
                     'pytest': importlib.metadata.version('pytest')},
    'invocation': "OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1; C:\\Users\\zmoor\\AppData\\Local\\CodexRuntimes\\timeseries312\\Scripts\\python.exe -B ..\\warmup_repair_20260907\\gap_aware_models\\run_tests.py test_oanda_gap_aware_four_family_models.py",
    'parameters': dict(model.PARAMETERS),
    'fixture': {'scope': 'synthetic_clock_and_price_fixture_only', 'friday_rows_per_pair': 512,
                'sunday_rows_per_pair': 61, 'real_rows_per_pair': 573,
                'current_common_one_minute_intervals': 60,
                'families_returned': sorted(output),
                'training_rows_by_family': {family: values[2]['training_rows'] for family, values in output.items()},
                'state_observations': output[model.FAMILIES[3]][2]['observations']},
    'timing': {'repetitions': 5, 'median_sec': statistics.median(timings),
               'scope': 'four pure model fits on synthetic snapshots; not whole-system latency'},
    'registered_six_source_bindings_unchanged': bindings,
    'historical_real_data_accuracy_established': False,
    'prospective_forecasts_created': 0, 'runtime_activated': False,
    'database_writes': False, 'external_orders_or_network': False,
    'current_registration_modified': False,
    'limitations': [
        'This pure helper receives market START epochs, not actual availability clocks.',
        'Root capture must verify duplicates, source bytes, complete M1 bars, actual capture time and latest common cutoff.',
        'Retained historical labels become usable only after actual current observation and a later completion/issue clock.',
        'The return-gap policy conservatively requires every feature and H1 target interval; insufficient retained valid rows still abstain.',
        'Current features require61 common closes, not61 minutes of elapsed wall time or guaranteed forecasting readiness.',
        'Graph uses the original per-pair pip factor construction; this is not a normalized covariance, VECM or joint-return model.',
        'Source changes and fixed UTC sampling require separate registration; old predictions/proof are not relabeled.',
        'Tests and synthetic timings establish engineering behavior, not accuracy or economic edge.',
    ],
}
destination = OUT / 'GAP_AWARE_MODELS_VALIDATION_20260907.json'
with destination.open('x', encoding='utf-8') as handle:
    json.dump(receipt, handle, indent=2, allow_nan=False)
    handle.write('\n')
print(json.dumps({'receipt': str(destination), 'sha256': sha(destination), 'tests': suite.attrib['tests'],
                  'median_sec': statistics.median(timings), 'fixture': receipt['fixture']}))
