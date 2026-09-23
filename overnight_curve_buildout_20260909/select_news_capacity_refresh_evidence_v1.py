"""Select exact compact news/capacity evidence; no project or vault writes."""
from pathlib import Path
import hashlib
import json
import os
import time

BASE = Path(__file__).resolve().parent
WORK = BASE.parent
PREFIX = 'docs/validation/overnight_curve_buildout_20260909/'


def record(path):
    raw = path.read_bytes()
    return dict(path=str(path.resolve()), sha256=hashlib.sha256(raw).hexdigest(), bytes=len(raw))


def main():
    external = [
        ('news_capacity/audit_retained_capacity_v2.py', 'actual_file_only_capacity_audit_helper'),
        ('news_capacity/retained_capacity_v2_001/RETAINED_NEWS_CAPACITY_AUDIT_20260909.json', 'actual_three_capture_capacity_hash_audit'),
        ('news_capacity/retained_capacity_v2_001/RETAINED_NEWS_CAPACITY_ASSESSMENT_20260909.md', 'actual_capacity_and_inactive_successor_scope'),
        ('entry_news_features/refresh_current_news_only_v1.py', 'actual_current_only_local_news_helper'),
        ('entry_news_features/CURRENT_ENTRY_NEWS_REFRESH_20260909.md', 'actual_current_only_news_summary'),
        ('entry_news_features/current_refresh_001/ENTRY_NEWS_CURRENT_REFRESH_20260909.json', 'preserved_summary_wrapper_failure'),
        ('entry_news_features/current_refresh_001/refresh_current_news_only_v1_original.py', 'preserved_pre_fix_wrapper_source'),
        ('entry_news_features/current_refresh_002/ENTRY_NEWS_CURRENT_REFRESH_20260909.json', 'actual_successful_current_only_news_summary'),
    ]
    prior = WORK/'revamp_baseline_20260908/news_repair_review/adapter_ledger_v2'
    historical = [
        (prior/'history_initial_tests.xml', 'history_initial_tests.xml', 'dated_original_history_test_run'),
        (prior/'adapter_ledger_frozen_final_tests.xml', 'adapter_ledger_frozen_final_tests.xml', 'dated_original_frozen_231_case_run'),
        (prior/'ADAPTER_LEDGER_IMPLEMENTATION_VALIDATION_20260908.json', 'ADAPTER_LEDGER_IMPLEMENTATION_VALIDATION_20260908.json', 'dated_original_implementation_receipt'),
        (WORK/'news_identity_integration_20260908/INPUTS_LEDGER_INDEPENDENT_REVIEW_20260908.json', 'INPUTS_LEDGER_INDEPENDENT_REVIEW_20260908.json', 'dated_original_independent_review'),
        (WORK/'trad/test_oanda_causal_forecast_inputs_joint_news_v2.py', 'test_oanda_causal_forecast_inputs_joint_news_v2.py', 'existing_history_capacity_causality_cache_tests_not_rerun'),
    ]
    items = []
    for name, status in external:
        items.append((BASE/name, name, status))
    for path, name, status in historical:
        items.append((path, 'news_capacity/prior_history_validation/'+name, status))
    entries = []
    for path, relative, status in items:
        assert not any(part.lower().startswith(('private', 'raw_')) for part in path.parts)
        assert path.suffix.lower() in {'.py', '.json', '.md', '.xml'}
        destination = PREFIX+relative+('.txt' if path.suffix == '.py' else '')
        entries.append(dict(original_source=record(path), intended_member=destination, artifact_status=status, copy_performed=False))
    assert len({item['intended_member'].lower() for item in entries}) == len(entries)
    dependencies = [WORK/'trad/config/joint_price_news_study_v3_20260908.json',
        WORK/'trad/oanda_causal_forecast_inputs_joint_news_v2.py', WORK/'trad/oanda_local_news_sentiment_repair_v1.py',
        WORK/'trad/oanda_entry_news_feature_join_v1.py', WORK/'trad/oanda_news_capture_storage_v1.py',
        BASE/'news_capacity/STORAGE_IMPLEMENTATION_VALIDATION_20260909.json',
        BASE/'news_capacity/STORAGE_INDEPENDENT_SOURCE_REVIEW_20260909.json',
        BASE/'news_capacity/NEWS_CAPTURE_STORAGE_REVIEW_20260909.md']
    value = dict(schema_version='curated_news_capacity_refresh_selection_v1_20260909',
        status='selection_only_no_project_or_vault_copy', created_epoch=time.time(),
        source_helper=record(Path(__file__)), entries=entries,
        counts=dict(files=len(entries), bytes=sum(x['original_source']['bytes'] for x in entries)),
        original_source_dependencies=[{**record(path), 'not_copied_by_this_selection': True} for path in dependencies],
        prior_model_selection=record(BASE/'CURATED_MODEL_EVIDENCE_SELECTION_20260909.json'),
        privacy_scope='Exact compact counts/clocks/hash receipts, notes and source/test text only. No raw news, quoted prices, account data, private source rows, full captures, database files, benchmark clones or before-token fixture.',
        validation_scope='Current capacity audit verifies three retained payloads and original diagnostics; historical adapter tests are dated original runs, not new tests or DB reads. Current-entry refresh does not sample historical factor/response memory.',
        open_operational_gap='85.044 percent of active 16 MiB logical capture ceiling; remaining 2,509,284 bytes. Existing CAS candidate inactive and cannot change active contract caps.',
        implementation_status='Successor storage/capture version proposed only. No new cohort, trimming, source/runtime change or numerical model gain claimed.',
        final_export_scan_required=True)
    path = BASE/'CURATED_NEWS_CAPACITY_REFRESH_SELECTION_20260909.json'
    raw = (json.dumps(value, sort_keys=True, indent=2)+'\n').encode()
    with path.open('xb') as stream:
        stream.write(raw); stream.flush(); os.fsync(stream.fileno())
    note = BASE/'CURATED_NEWS_CAPACITY_REFRESH_SELECTION_20260909.md'
    text = f'''# News refresh and capacity evidence selection

Selected {len(entries)} exact compact evidence files ({value['counts']['bytes']:,} bytes). This manifest performs no copy to the project or vault. Previous model selections remain unchanged.

The September 9 current-news refresh and three-retained-capture capacity audit have actual original/read/consumer clocks. The preserved first refresh wrapper failure remains visible. Historical pagination, row/byte/deadline, immutable-cache and causality test sources/XML are dated September 8 evidence; no new history database read or test rerun is attributed.

The current 85.044% shared logical usage leaves 2.39 MiB. This remains an open operational gap. The inactive CAS proposal preserves exact history and clocks but cannot remove the active logical ceiling. Any successor needs separately reviewed source/cohort bindings and full-envelope memory/throughput validation; no late deployment is proposed.

Raw news, prices, accounts, private payloads, database files and benchmark clones are excluded. Hash-bound original source dependencies remain separately identified. Root's final content/credential scan and copy/export are separate.

Manifest SHA-256: `{hashlib.sha256(raw).hexdigest()}`.
'''
    with note.open('x', encoding='utf-8', newline='\n') as stream:
        stream.write(text); stream.flush(); os.fsync(stream.fileno())
    print(json.dumps(dict(manifest=record(path), note=record(note), counts=value['counts'])))


if __name__ == '__main__':
    main()
