"""Record focused fixture validation without touching runtime data."""
from datetime import datetime, timezone
import hashlib
import importlib.metadata
import json
from pathlib import Path
import platform
import sys
import xml.etree.ElementTree as ET

OUT = Path(__file__).resolve().parent
ROOT = OUT.parent.parent / 'trad'

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

suite = ET.parse(OUT / 'integration_tests.xml').getroot().find('testsuite')
assert suite is not None and suite.attrib['failures'] == '0' and suite.attrib['errors'] == '0'
old_receipt = OUT.parent / 'updater_repair/UPDATER_REPAIR_RECEIPT_20260907.json'
contract = json.loads((ROOT / 'config/causal_forecast_study_v1_io_r2_20260906.json').read_bytes())
bindings = {name: {'expected': expected, 'actual': sha(ROOT / name)}
            for name, expected in contract['source_bindings'].items()}
assert all(row['expected'] == row['actual'] for row in bindings.values())
receipt = {
    'schema_version': 'updater_market_independent_review_fixes_r2_20260907',
    'generated_utc': datetime.now(timezone.utc).isoformat(),
    'source_sha256': {name: sha(ROOT / name) for name in (
        'oanda_all68_m1_forward_updater.py', 'test_oanda_all68_m1_forward_updater.py',
        'oanda_market_overview.py', 'test_oanda_market_overview.py', 'oanda_exact_price_scoring.py')},
    'evidence_sha256': {'integration_tests.xml': sha(OUT / 'integration_tests.xml'),
                        'run_tests.py': sha(OUT / 'run_tests.py')},
    'prior_receipt_preserved': {'path': str(old_receipt), 'sha256': sha(old_receipt)},
    'tests': dict(suite.attrib), 'python_executable': sys.executable,
    'python_version': platform.python_version(), 'pytest_version': importlib.metadata.version('pytest'),
    'invocation': "C:\\Users\\zmoor\\AppData\\Local\\CodexRuntimes\\timeseries312\\Scripts\\python.exe -B ..\\warmup_repair_20260907\\updater_market_review_r2\\run_tests.py test_oanda_all68_m1_forward_updater.py test_oanda_market_overview.py",
    'findings_resolved': [
        'Oversized Decimal quote previously became current with Infinity display fields; it now becomes unavailable.',
        'Unrepresentable derived spread/change metrics and Decimal underflow cannot emit nonfinite or false-flat indicators.',
        'Caller Decimal precision no longer changes market calculations; deeply recursive JSON fails closed.',
        'Gap recovery rejects midpoint OHLC outside its corresponding actual bid/ask values.',
        'Archive gap scanning rejects duplicate/ambiguous columns, absent identity/close/clock fields and malformed CSV.',
        'Archive instrument and M1 identity must match; time/datetime values must identify the same exact minute.',
    ],
    'preserved_checks': [
        'No inferred candles; only requested, complete, observed broker BAM rows can be inserted.',
        'Existing CSV prefix and tail bytes survive LF/CRLF insertions unchanged.',
        'Reservations, observations and publication receipts remain exclusive-create and durable.',
        'Scan byte limits, newest-gap request limits, retry budgets and cooldown remain unchanged.',
        'Stale, retained, nontradeable and future quotes do not become current technical indicators.',
    ],
    'registered_six_source_bindings_unchanged': bindings,
    'external_network_or_broker_requests': 0, 'runtime_started_or_reloaded': False,
    'live_csv_or_database_writes': False, 'synthetic_runtime_rows_created': 0,
    'historical_availability_asserted': False,
    'limitations': [
        'Fixture tests and in-memory reproductions only; deployment and actual API/process checks belong to root.',
        'Gap publication still requires the updater to be the sole CSV writer; original compare-before-replace checks are retained.',
        'Market movement values are bounded descriptive endpoint differences before costs, not forecasts or trading authority.',
        'Scanner now requires explicit instrument, granularity and close fields in addition to a timestamp; unsupported schemas abstain from recovery.',
    ],
}
destination = OUT / 'UPDATER_MARKET_REVIEW_R2_20260907.json'
with destination.open('x', encoding='utf-8') as handle:
    json.dump(receipt, handle, indent=2, allow_nan=False)
    handle.write('\n')
print(json.dumps({'receipt': str(destination), 'sha256': sha(destination),
                  'tests': suite.attrib['tests'], 'source_sha256': receipt['source_sha256']}))
