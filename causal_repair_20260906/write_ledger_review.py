"""Write the independent fixture review receipt without modifying runtime state."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import xml.etree.ElementTree as ET

out = Path(__file__).resolve().parent
source = out.parent / 'trad'
names = ['oanda_causal_forecast_ledger.py', 'oanda_causal_forecast_inputs.py',
         'oanda_fixed_forecast_evaluation.py', 'test_oanda_causal_forecast_ledger.py']
suite = ET.parse(out/'ledger_review_v4.xml').getroot()
totals = {key: sum(int(node.get(key, 0)) for node in suite.iter('testsuite'))
          for key in ('tests', 'failures', 'errors', 'skipped')}
assert totals == {'tests':106, 'failures':0, 'errors':0, 'skipped':0}, totals
result = {
    'schema_version':'independent_causal_ledger_fixture_review_v1',
    'generated_utc':datetime.now(timezone.utc).isoformat(),
    'reviewer':'ledger_adversarial_review',
    'status':'all_reviewed_boundary_and_real_capture_checks_passed',
    'validation':totals,
    'junit':'ledger_review_v4.xml',
    'runtime_started':False, 'production_database_opened':False,
    'network_allowed':False, 'subprocesses_allowed':False, 'python_threads_allowed':False,
    'writes_and_sqlite_scope':'disposable causal_repair_20260906 fixtures only',
    'source_sha256':{name:hashlib.sha256((source/name).read_bytes()).hexdigest() for name in names},
    'harness_sha256':hashlib.sha256((out/'run_ledger_tests.py').read_bytes()).hexdigest(),
    'junit_sha256':hashlib.sha256((out/'ledger_review_v4.xml').read_bytes()).hexdigest(),
    'covered':[
        'Explicit actual-time activation; immutable source/cohort/protocol/tolerance/research constraints.',
        'Forecast commit independently visible before postcommit receipt; separate committed consumer observation.',
        'Crash after forecast commit and late recovery use actual recovery clocks without resetting original target.',
        'Later market tick and observation both strictly after consumer receipt; exact entry/target deadlines.',
        'Ledger owns local quote availability; numeric-equivalent duplicate ticks retain first observation.',
        'Captured-byte validation, model/source/dependency/capture-result binding, compute-before-issue order.',
        'All-four-or-none output; invalid probabilities/sides/numbers cannot partially publish.',
        'Arbitrary cadence buckets rejected; same attempt cannot rerank or republish.',
        'Earliest eligible entry and target remain selected even when later quote prices improve return.',
        'Missing entries/targets and missing consumer receipt remain explicit exclusions.',
        'Clock regression rejected during observation and independent export, including after reopen.',
        'Append-only SQL UPDATE/DELETE triggers for every retained evidence table.',
        'Unrelated preexisting SQLite database rejected without any byte modification.',
        'Independent export snapshot cannot see uncommitted producer rows or consume producer transaction.',
        'Four cohorts exported into existing strict scorer with preserved sides, target and bid/ask stress costs.',
        'Actual synthetic seven-pair CSV capture and four original numerical model functions feed ledger/scorer.',
    ],
    'review_findings_fixed_by_root':[
        'Incomplete contract and nested evaluator consistency checks.',
        'Mutable caller-owned contract dictionaries after hash retention.',
        'Caller-supplied backdated quote availability.',
        'Model/capture/source/dependency and completion-time binding omissions.',
        'Ledger strictly-later market entry inconsistent with evaluator equality acceptance.',
        'Numeric-equivalent tick identities could produce separate first-observation records.',
        'Unconstrained integer cadence buckets and unbound input timeframe.',
    ],
    'limitations':[
        'These are engineering checks, not prediction quality, prospective edge, independent attestation, or authority to trade.',
        'Boundary-only tests intentionally use minimal marked input captures and a validator seam; two real-capture integration cases use the original validator and numerical functions.',
        'Native numerical parallelism is constrained to one thread by OMP/BLAS/LOKY environment settings; runtime throughput was not benchmarked.',
        'No production worker, actual broker quote, historical database, external account, or live study activation was used.',
        'Source hashes describe reviewed bytes at receipt creation; later implementation changes require appropriate rerun.',
    ],
}
json_path = out/'LEDGER_INDEPENDENT_REVIEW_20260906.json'
with json_path.open('x', encoding='utf-8') as handle:
    json.dump(result, handle, indent=2, allow_nan=False)
    handle.write('\n')
markdown = '''# Independent causal forecast ledger review — 2026-09-06

**106 fixture tests passed, with no failures, errors or skips.** The reviewed successor ledger preserves the actual forecast commit, later publication receipt, independent consumer observation, strictly later executable quote and unchanged original target. Research, proof and account authority remain disabled.

The review found and the root implementation corrected incomplete contract checks, mutable configuration references, caller-controlled quote availability, missing capture/model/computation bindings, an evaluator entry-clock mismatch, numeric tick identity inconsistency and insufficient cadence/timeframe constraints. All corresponding adversarial tests pass.

Coverage includes crash recovery without backdating, late and missing quote exclusions, immutable SQL evidence, first-observation retention, first eligible quote selection, independent snapshot export, preserved emitted sides, bid/ask costs and extra cost stress. Two integration cases use actual synthetic seven-pair CSV captures and all four numerical model functions; one verifies valid publication/scoring and one rejects changed captured evidence. Boundary tests use marked minimal captures to isolate ledger rules.

The guarded harness denies network, subprocesses, Python thread starts, non-fixture writes and non-fixture SQLite connections. Native numerical execution is constrained to one thread. No production worker or database was started or accessed. These results validate engineering behavior; they do not establish a predictive edge or authorize trading.

Evidence: `ledger_review_v4.xml`; detailed source hashes and limitations: `LEDGER_INDEPENDENT_REVIEW_20260906.json`. Earlier review runs remain preserved separately.
'''
with (out/'LEDGER_INDEPENDENT_REVIEW_20260906.md').open('x', encoding='utf-8') as handle:
    handle.write(markdown)
print(json.dumps({'receipt':str(json_path),'tests':totals,'sha256':hashlib.sha256(json_path.read_bytes()).hexdigest()}))
