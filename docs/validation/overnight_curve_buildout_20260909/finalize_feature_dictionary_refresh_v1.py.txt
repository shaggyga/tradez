"""Retain the completed documentation-only metadata transition and selection."""
import datetime
import hashlib
import json
from pathlib import Path
import xml.etree.ElementTree as ET

BASE = Path(__file__).resolve().parent
ROOT = BASE.parent / 'trad'
FOLDER = BASE / 'feature_dictionary_source_refresh_v1'
PREFIX = 'docs/validation/overnight_curve_buildout_20260909/'


def record(path):
    raw = path.read_bytes()
    return dict(path=str(path.resolve()), sha256=hashlib.sha256(raw).hexdigest(), bytes=len(raw))


def save(path, value):
    raw = (json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + '\n').encode()
    with path.open('xb') as stream:
        stream.write(raw)
    return record(path)


def main():
    application_path = FOLDER / 'DICTIONARY_METADATA_APPLICATION_20260909.json'
    assert record(application_path)['sha256'] == 'affdcb1ddf41be2fba325a7e05e09dc7cb1ae3e5a1b0606df04931aa7701efaa'
    application = json.loads(application_path.read_bytes())
    for item in application['current_files'] + application['old_manifests_unchanged']:
        assert record(Path(item['path'])) == item
    for item in application['preserved']:
        assert record(Path(item['retained']['path'])) == item['retained']
    assert record(Path(application['old_validation_unchanged']['path'])) == application['old_validation_unchanged']
    manager = ROOT / 'oanda_advisor_account_manager_auto.py'
    assert record(manager)['sha256'] == 'f07fb5dc4c1eb460b40bd1108636e1213b082c7c6ca6aa592998e8dcb8389faa'
    xml_path = FOLDER / 'feature_dictionary_refresh_tests.xml'
    xml = ET.fromstring(xml_path.read_bytes())
    suites = [xml] if xml.tag == 'testsuite' else list(xml.iter('testsuite'))
    counts = {key: sum(int(s.get(key, 0)) for s in suites) for key in ('tests', 'failures', 'errors', 'skipped')}
    assert counts == dict(tests=34, failures=0, errors=0, skipped=0)
    checked = json.loads((FOLDER / 'DICTIONARY_CHECK_20260909.json').read_bytes())
    coverage = json.loads(checked['stdout'])
    assert checked['exit_code'] == 0 and coverage['status'] == 'verified'
    documents = [record(ROOT / name) for name in ('docs/feature_dictionary/catalog_251.json',
                 'docs/FOREX_FEATURE_DICTIONARY_CURRENT.json', 'docs/FOREX_FEATURE_DICTIONARY_CURRENT.md')]
    old_md = FOLDER / 'before_source/docs/FOREX_FEATURE_DICTIONARY_CURRENT.md'
    assert record(old_md)['sha256'] == '340349f4c55628234fb354e97ab7d251c1e9182c0ed190cfa65e87a369315374'
    mapper = ROOT / 'forex_model_vault_sync.py'
    assert b'(Path("trad/FOREX_FEATURE_DICTIONARY_VALIDATION_20260908.json"), "FEATURE_DICTIONARY_VALIDATION_CURRENT.json")' in mapper.read_bytes()
    receipt_path = FOLDER / 'FEATURE_DICTIONARY_METADATA_REFRESH_VALIDATION_20260909.json'
    member = PREFIX + receipt_path.relative_to(BASE).as_posix()
    value = dict(schema_version='forex_feature_dictionary_metadata_refresh_validation_v1_20260909',
        status='current_document_metadata_verified_definitions_unchanged', verified_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
        scope='Documentation source-hash and line-location refresh after the separately reviewed account-reconciliation change. No feature-definition or runtime change.',
        application=record(application_path), current_documents=documents, accepted_referenced_manager_source=record(manager),
        unchanged_referenced_function_proofs=application['unchanged_symbol_proofs'],
        exact_authored_delta=dict(feature_id='atr_14_pips', source_hash=dict(
            before='af211315d359e07a5b41b38ca0c4d30f75a7851ea5ce521a84b08d2af9983e27', after=record(manager)['sha256']),
            line_references=[dict(before=1942, after=1950), dict(before=1972, after=1980)],
            all_other_authored_values_and_feature_definitions_unchanged=True),
        generated_delta=dict(json='Only catalog source metadata plus authored catalog SHA changed; all other decoded values equal retained original.',
            markdown='Exactly two source-line references changed; remaining bytes equal retained original.'),
        validation=dict(builder=record(ROOT / 'tools/build_forex_feature_dictionary.py'),
            build_receipt=record(FOLDER / 'DICTIONARY_BUILD_20260909.json'), check_receipt=record(FOLDER / 'DICTIONARY_CHECK_20260909.json'),
            coverage=coverage, existing_test_source=record(ROOT / 'test_forex_feature_dictionary.py'), test_xml=record(xml_path),
            test_counts=counts, reported_test_duration_sec=3.10),
        preserved_originals=application['preserved'],
        old_selection_citations=dict(manifests=application['old_manifests_unchanged'],
            disposition='The two old selections cite the original MD only as a declared historical dependency, not selected/rehashed canonical bytes. Their original citations remain unchanged.',
            original_cited_md_retained=record(old_md), new_retained_archive_member=PREFIX + old_md.relative_to(BASE).as_posix()),
        validation_alias=dict(alias='FEATURE_DICTIONARY_VALIDATION_CURRENT.json',
            mapped_original_source='FOREX_FEATURE_DICTIONARY_VALIDATION_20260908.json',
            original_source_unchanged=application['old_validation_unchanged'], original_as_of_utc='2026-09-08T02:28:00Z',
            meaning='Historical Sep8 definition/build validation for its listed original hashes; it does not verify refreshed Sep9 JSON/MD bytes.',
            current_metadata_validation_archive_member=member, unchanged_mapper_observed=record(mapper),
            navigation_action='README and current overnight report should label the old alias historical and link this current metadata-refresh receipt; publisher mapping remains unchanged.'),
        preservation=dict(original_validation_rewritten=False, prior_manifests_rewritten=False, feature_definitions_changed=False,
            model_sources_changed_by_refresh=False, manager_sources_changed_by_refresh=False, registry_changes=False,
            runtime_changes=False, broker_requests=0, model_fit_or_rescore=False, vault_writes=False),
        unrelated_guard_disposition=dict(fact='An initial receipt attempt also checked the unrelated technical-manager prior hash and stopped before writing a receipt.',
            prior_expected='e6e794a4373a0766dfb6338f75506e3b209b79d57cef419e21375ff40cb3f519',
            observed='94e99d041836c1bf9ff00ad5b4bf6fa39d30f7c5c79547d7756c658c3a1ddf2d',
            resolution='Parent confirmed its separately reviewed 86-case technical-manager correction. That module is not referenced by this dictionary entry; no unchanged-technical-source claim is made here.'),
        limits=['AST equality establishes unchanged referenced implementations, not that every historical catalog feature is implemented or active.',
            'Existing tests and deterministic check validate dictionary structure, selected arithmetic and source bindings, not predictive value or trading eligibility.',
            'Original dated validation retains original hashes; this receipt separately verifies current generated-document metadata.'])
    receipt = save(receipt_path, value)
    note_path = FOLDER / 'FEATURE_DICTIONARY_METADATA_REFRESH_20260909.md'
    note = f'''# Feature dictionary source metadata refresh, 9 September 2026

The dictionary now binds reviewed advisor manager `{record(manager)['sha256']}`. Its only two advisor references belong to `atr_14_pips`: `atr_pips` moved from line 1942 to 1950, and the `candle_snapshot` ATR field from 1972 to 1980. Both complete function ASTs match the retained prior source exactly.

Only that source hash and two line references changed in the authored catalog. The existing builder regenerated JSON and Markdown, and `--check` passed. All other parsed generated-document values are unchanged; Markdown differs only at those two references. Existing dictionary tests passed: 34, no failures. This refresh changed no feature definition, model, manager source, registry, worker or runtime.

Exact before catalog, generated JSON/Markdown, builder/test sources and Sep8 validation remain under `before_source/`. The prior cited MD identity `{record(old_md)['sha256']}` maps to retained before bytes. Original selection manifests and historical citations remain unchanged.

`FEATURE_DICTIONARY_VALIDATION_CURRENT.json` still maps to the dated Sep8 validation (`2026-09-08T02:28:00Z`) and its original hashes. Navigation must label it historical, not imply that it certifies the refreshed documents. Current metadata verification is at `{member}`. No mapper or publisher change was needed.

Current receipt SHA-256: `{receipt['sha256']}`. This verifies documentation/source metadata, not predictive profitability or operational readiness.
'''
    with note_path.open('x', encoding='utf-8', newline='\n') as stream:
        stream.write(note)
    paths = sorted(p for p in (FOLDER / 'before_source').rglob('*') if p.is_file())
    paths += [FOLDER / name for name in ('BEFORE_DICTIONARY_SOURCE_REFRESH_20260909.json', 'BEFORE_EXPECTED_SOURCE_DRIFT_CHECK_20260909.json',
              'DICTIONARY_BUILD_20260909.json', 'DICTIONARY_CHECK_20260909.json', 'DICTIONARY_METADATA_APPLICATION_20260909.json', 'feature_dictionary_refresh_tests.xml')]
    paths += [BASE / 'refresh_feature_dictionary_metadata_v1.py', Path(__file__), receipt_path, note_path]
    entries = []
    for path in paths:
        target = PREFIX + path.relative_to(BASE).as_posix()
        if path.suffix == '.py': target += '.txt'
        entries.append(dict(group='feature_dictionary_metadata_refresh', kind='source_or_test' if path.suffix == '.py' or path.name.endswith('.py.txt') else 'summary_or_receipt',
            original_source=record(path), intended_member=target, artifact_status='exact_before_or_current_metadata_transition', copy_performed=False))
    dependencies = [dict(**record(ROOT / name), source_snapshot_member=name, status='current_canonical_document_or_source_dependency')
        for name in ('docs/feature_dictionary/catalog_251.json', 'docs/FOREX_FEATURE_DICTIONARY_CURRENT.json', 'docs/FOREX_FEATURE_DICTIONARY_CURRENT.md',
                     'tools/build_forex_feature_dictionary.py', 'test_forex_feature_dictionary.py', 'oanda_advisor_account_manager_auto.py', 'FOREX_FEATURE_DICTIONARY_VALIDATION_20260908.json')]
    selection = dict(schema_version='curated_feature_dictionary_metadata_refresh_selection_v1_20260909', status='selection_only_no_export_copy',
        created_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(), scope='Preserve exact original dictionary identities and add current metadata-only verification; prior CURATED manifests remain unchanged.',
        entries=entries, canonical_source_dependencies=dependencies,
        counts=dict(evidence_files=len(entries), evidence_bytes=sum(x['original_source']['bytes'] for x in entries), canonical_source_dependencies=len(dependencies)),
        current_validation_reference=receipt, old_validation_alias_scope='Historical Sep8 proof only; current metadata receipt is separately linked.',
        privacy_and_export_checks=dict(raw_news_or_account_data=False, credentials_read=False, final_scan_and_source_archive='Separate parent curator/publication step.'))
    selected = save(BASE / 'CURATED_FEATURE_DICTIONARY_METADATA_REFRESH_SELECTION_20260909.json', selection)
    print(json.dumps(dict(validation=receipt, selection=selected, note=record(note_path), current_documents=documents, tests=counts, counts=selection['counts'])))


if __name__ == '__main__':
    main()
