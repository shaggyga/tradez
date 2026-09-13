"""Freeze offline model tests and read-only companion integration review."""
from datetime import datetime, timezone
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import re
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
import oanda_eurusd_local_models_v1 as model
import test_oanda_eurusd_local_models_v1 as fixtures

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def canonical_sha(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()

suite = ET.parse(OUT / 'integration_tests.xml').getroot().find('testsuite')
assert suite is not None and suite.attrib['failures'] == '0' and suite.attrib['errors'] == '0'
rows, cutoff = fixtures.fixture_rows()
timings = []
for _ in range(5):
    started = time.perf_counter()
    output = model.predict_all(rows, cutoff)
    timings.append(time.perf_counter() - started)
assert set(output) == set(model.FAMILIES)
old_bindings = {}
for config in ('causal_forecast_study_v1_io_r2_20260906.json', 'causal_forecast_study_gap_v2_20260907.json'):
    contract = json.loads((ROOT / 'config' / config).read_bytes())
    bindings = {name: {'expected': expected, 'actual': sha(ROOT / name)}
                for name, expected in contract['source_bindings'].items()}
    assert all(row['expected'] == row['actual'] for row in bindings.values())
    old_bindings[config] = bindings
new_config_path = ROOT / 'config/causal_forecast_study_eurusd_v1_20260907.json'
new_config = json.loads(new_config_path.read_bytes())
assert new_config['numeric_model_source_sha256'] == sha(ROOT / 'oanda_eurusd_local_models_v1.py')
assert set(new_config['cohorts']) == set(model.FAMILIES)
new_bindings = {name: {'expected': expected, 'actual': sha(ROOT / name)}
                for name, expected in new_config['source_bindings'].items()}
assert all(row['expected'] == row['actual'] for row in new_bindings.values())
supervisor = ROOT / 'oanda_always_on_supervisor.ps1'
allow_section = re.search(r'\$ResearchCollectionNames\s*=\s*@\((.*?)\)', supervisor.read_text(), re.S).group(1)
names = re.findall(r'"([^"]+)"', allow_section)
assert len(names) == len(set(names)) == 13 and 'eurusd_local_forecast_study' in names
receipt = {
    'schema_version': 'eurusd_local_model_validation_and_integration_review_v1_20260907',
    'generated_utc': datetime.now(timezone.utc).isoformat(),
    'source_sha256': {name: sha(ROOT / name) for name in (
        'oanda_eurusd_local_models_v1.py', 'test_oanda_eurusd_local_models_v1.py')},
    'evidence_sha256': {'integration_tests.xml': sha(OUT / 'integration_tests.xml'),
                        'run_tests.py': sha(OUT / 'run_tests.py')},
    'tests': dict(suite.attrib), 'python_executable': sys.executable,
    'dependencies': {'python': platform.python_version(), 'numpy': importlib.metadata.version('numpy'),
                     'pytest': importlib.metadata.version('pytest'),
                     'sklearn_used_by_four_family_test_oracle_only': importlib.metadata.version('scikit-learn')},
    'invocation': "OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1; C:\\Users\\zmoor\\AppData\\Local\\CodexRuntimes\\timeseries312\\Scripts\\python.exe -B ..\\warmup_repair_20260907\\eurusd_local_models\\run_tests.py test_oanda_eurusd_local_models_v1.py",
    'parameters': dict(model.PARAMETERS), 'families': list(model.FAMILIES),
    'fixture': {'scope': 'synthetic_prices_and_clocks_only', 'retained_real_rows': len(rows),
        'friday_rows': 512, 'sunday_closes': 61, 'current_one_minute_intervals': 60,
        'ridge_training_rows': output[model.FAMILIES[0]][2]['training_rows'],
        'state_observations': output[model.FAMILIES[1]][2]['observations']},
    'timing': {'repetitions': 5, 'median_sec': statistics.median(timings),
              'scope': 'pure local model helper, not whole-project latency'},
    'old_registered_bindings_unchanged': old_bindings,
    'new_companion_contract_sha256': canonical_sha(new_config),
    'new_companion_source_bindings_reviewed': new_bindings,
    'independent_integration_review': {
        'scope': 'EURUSD worker/ledger/evaluator diffs against frozen gap_v2 and exact evaluator; PowerShell gate read-only',
        'blocking_findings': [],
        'two_family_gate': 'exactly ridge_return_repaired and probabilistic_state_space, with distinct new ledger cohorts',
        'evaluator_row_count': 'exactly2 forecasts, complete_two_family_decisions counter; prior residual4 repaired by root before final review',
        'clocks': 'input maturity, actual observed capture, later completion/issue, durable publication and independent consumption unchanged',
        'targets_and_prices': 'original H1 targets and fresh executable entry/target quotes retained; Decimal observed-price scoring unchanged',
        'supervisor_sha256': sha(supervisor), 'powershell_ast_parse_errors_observed': 0,
        'unique_research_allowlist_count': len(names), 'research_allowlist': names,
        'sole_added_worker': 'eurusd_local_forecast_study',
        'unknown_or_execution_workers_remain_excluded': True,
    },
    'historical_real_data_accuracy_established': False,
    'prospective_forecasts_created_by_this_subtask': 0,
    'process_start_or_reload_by_this_subtask': 0, 'broker_calls_by_this_subtask': 0,
    'live_csv_or_database_writes_by_this_subtask': 0,
    'limitations': [
        'This direct EURUSD-only API receives market START epochs, not actual observation clocks.',
        'Caller must verify instrument/duplicates/source bytes, observe availability and require both families before issuance.',
        'There is no peer substitution, imputation or bypass of EURUSD current61-close and mature-label requirements.',
        'The state output uses uncalibrated inherited Gaussian/EWMA conventions; no accuracy or trading authority is established.',
        'Runtime activation and current outcome validation belong to root; this receipt does not assert the whole project remains stopped.',
    ],
}
destination = OUT / 'EURUSD_LOCAL_MODELS_VALIDATION_20260907.json'
with destination.open('x', encoding='utf-8') as handle:
    json.dump(receipt, handle, indent=2, allow_nan=False)
    handle.write('\n')
print(json.dumps({'receipt': str(destination), 'sha256': sha(destination), 'tests': suite.attrib['tests'],
                  'median_sec': statistics.median(timings), 'contract_sha256': receipt['new_companion_contract_sha256']}))
