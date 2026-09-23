"""Append the observed continuation state without revising earlier receipts."""
from pathlib import Path
from datetime import datetime, timezone
import hashlib
import json

HERE = Path(__file__).resolve().parent
sha = lambda p: hashlib.sha256(Path(p).read_bytes()).hexdigest()


def record(path):
    return {'path': str(path), 'bytes': path.stat().st_size, 'sha256': sha(path)}


def main():
    richer = HERE / 'direction_richer_archive_003'
    prep = richer / 'prepared_002/RICHER_PREPARATION_RECEIPT.json'
    assert sha(prep) == '998c24d465cb71c26cbf365d2a5de48b80140d9d952690d068204d2976522daa'
    folds = []
    for fold in ('2025_h1', '2025_h2', '2026_ytd'):
        path = richer / ('evaluation_003/H60_' + fold + '/FOLD_COMPLETION.json')
        receipt = json.loads(path.read_bytes())
        assert receipt['new_fits'] == 1 and receipt['reports'] == 2
        for row in receipt['files']:
            assert record(Path(row['path'])) == row
        folds.append(record(path))
    profile = HERE.parent / 'trad/tools/research_profiles/typed_outcomes_v4_20260912'
    test_path = profile / 'TESTS_installed003.json'
    tests = json.loads(test_path.read_bytes())
    assert tests['pytest_exit_code'] == 0 and not tests['tripwires']
    assert sum(c['phase'] == 'call' and c['outcome'] == 'passed' for c in tests['cases']) == 210
    for row in tests['loaded_project_sources'].values():
        assert sha(row['path']) == row['sha256']
    now = datetime.now(timezone.utc).isoformat()
    state = {
        'observed_utc': now, 'preparation_complete': record(prep),
        'richer_H60_completed_cells': folds, 'richer_H240_completed_fits': 0,
        'richer_evaluation_003_complete': False,
        'H240_failure': 'fixed_655_plus68_schema_required; horizon eligible population has67pairs,EUR_DKK has zero eligible H240 rows; global68identity schema correction staged separately',
        'probability_outcome_profile': {'installed_inactive': True, 'canonical_test_receipt': record(test_path), 'passed_cases': 210,
            'identity_revision_source_sha256': sha(profile / 'src/outcome_shadow_profile_v4.py'),
            'old_callers_repointed': False, 'packaging_pending': True},
        'availability': record(richer / 'availability_transport_001/AVAILABILITY_SEAL.json'),
        'support_explanation': record(richer / 'feature_support_explanation_001/FEATURE_SUPPORT.json'),
        'agent_reviews_resumed_after_fresh_available_usage_check': True,
        'services_started': False, 'trading_activated': False, 'old_trial_deadline_changed': False,
    }
    with (HERE / 'CONTINUATION_STATE_003.json').open('x', encoding='utf-8') as handle:
        json.dump(state, handle, indent=2); handle.write('\n')
    with (HERE / 'WORK_LOG.md').open('a', encoding='utf-8') as handle:
        handle.write(f'''\n\n### {now} — complete H60 fits, actual feature support and inactive typed outcomes

All three declared richer H60 model fits and their six reports completed. The original H60 first fold was reused byte-for-byte; later fits used separated matrix-build, fit and report processes under the same scientific settings and memory ceiling after the earlier combined-process memory failure. Numeric results have not yet been interpreted or promoted. The H240 builder then refused because its observed population has67pairs while the fixed matrix schema has68identity columns. EUR_DKK has no eligible H240 rows. A new consumer revision is being prepared to retain the global68identity universe, unchanged actual rows, and all three completedH60fits. Earlier failures and frozen sources remain retained.

The full richer availability audit completed:631possible added fields,378with some finite values,253entirely missing,10constant among their finite values. Exact68pair receipts show maximum available histories at decisions of1377M1bars,45M30bars,22H1bars and5H4bars. Long windows cannot populate under that support. The strict adapter resets at every missing minute/invalid row/source change, including scheduled closures. A session-aware carry/reset experiment is proposed separately; this does not edit the current comparison or quantify gap-cause shares.

The new inactive typed probability/outcome profile is installed with22source/test members. It preserves original forecast and quote clocks, null unsupported midpoint probabilities/Brier, exact bid/ask outcomes and separate hold/no-action denominators. Final review found five Python-dictionary-equality boundaries that accepted different canonical JSON types. Revision002 fixes only those five comparisons in three functions; root reconstructed the exact source, verified seven primitive controls, and the installed210-case suite passes. These tests overlap and are synthetic, not trading validation. The root installer first refused a Windows path-separator mismatch in its manifest lookup before mutation; that checker failure and corrected derivation are retained. Old callers remain separate and no worker or order starts.

Fresh00:44–00:46UTC operational inspection sees offline research jobs only, no dashboard8765 listener or running live collector/study/meter/trial. The saved recovery task still points to the completed Friday trial and is not a current market-open recovery plan. Exact readiness and source-package supplements are being completed. Background reviews resumed only after a fresh usage check reported ordinary usage available; no reset credit was consumed.
''')
    print(json.dumps({'state': str(HERE / 'CONTINUATION_STATE_003.json'), 'H60_complete': 3, 'H240_complete': 0, 'activated': False}))


if __name__ == '__main__':
    main()
