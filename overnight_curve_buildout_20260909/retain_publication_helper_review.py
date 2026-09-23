"""Document the bounded publication-helper review; performs no publication."""
import hashlib
import json
import os
from pathlib import Path
import time
import xml.etree.ElementTree as ET

BASE=Path(__file__).resolve().parent
ROOT=BASE.parent/'trad'

def binding(path):
    raw=path.read_bytes()
    return dict(path=str(path),sha256=hashlib.sha256(raw).hexdigest(),bytes=len(raw))

def save(path,value):
    raw=(json.dumps(value,indent=2,sort_keys=True,allow_nan=False)+'\n').encode()
    with path.open('xb') as handle:
        handle.write(raw);handle.flush();os.fsync(handle.fileno())
    return binding(path)

def main():
    helper=BASE/'publish_overnight_vault_v1.py'
    tests=BASE/'test_publish_overnight_vault_v1_review.py'
    xml=BASE/'publication_helper_isolated_review_tests.xml'
    suites=ET.fromstring(xml.read_bytes());suite=list(suites)[0]
    assert suite.attrib['tests']=='16' and suite.attrib['failures']=='0' and suite.attrib['errors']=='0'
    tools={name:binding(ROOT/name) for name in ['forex_model_vault_sync.py',
        'tools/vault_worktree_snapshot.py','tools/audit_forex_vault_readability.py']}
    receipt=BASE/'OVERNIGHT_PUBLICATION_HELPER_INDEPENDENT_REVIEW_20260909.json'
    save(receipt,dict(schema_version='overnight_publication_helper_independent_review_v1_20260909',
        reviewed_epoch=time.time(),status='passed_bounded_source_and_isolated_orchestration_review',
        helper=binding(helper),tests=binding(tests),test_xml=binding(xml),tool_bindings=tools,
        initial_reviewed_source_sha256='6cbc2a79c269eabcc6b2dd5f1a03081683db9f2b2fadb8389f9adb6d2ae520ff',
        initial_source_bytes_retained=False,
        findings_closed=[
            dict(finding='A verified project copy did not independently require exact curated-member inclusion in the source archive.',
                resolution='Copied entries plus the report and validation now require Git inventory/export eligibility before writes and exact persisted manifest membership/SHA/size immediately after source publication and after final verification.'),
            dict(finding='Fixed manifest/pointer/readability destinations were checked only by later tools after publication could have begun.',
                resolution='All fixed roots and outputs, plus all181 canonical record destinations, are checked before source publication. Final hashes retain five persisted publication/navigation files; helper bytes are pinned across the operation.')],
        remaining_blockers=[],
        source_review=[
            'Uses only the existing source-snapshot, canonical-record-only and readability build/check interfaces; does not call the broad model-vault synchronizer main routine.',
            'Expected report/validation/copy-receipt digests, strict complete/scanned/compiled receipt fields, every intended-member SHA/size and all181 canonical payloads are checked before the first publishing interface call.',
            'Source archive publication precedes canonical-record synchronization, then readability build/check and persisted source/canonical/manifest rechecks.',
            'Failures after publication begins retain bounded failure type/status and partial_publication_possible=true; no rollback, deletion or completion is invented.',
            'Final receipt distinguishes source-only export from model weights/runtime inputs. No broker, runtime-control or retention operation was found.'
        ],
        executed_validation=dict(passed=16,failed=0,errors=0,pytest_reported_duration_sec=11.39,
            root_and_vault='temporary isolated fixture directories only',
            publishing_tools='mocked source/canonical/readability interfaces; no real tool publishing call',
            path_guard_scope='Original pure path-guard definitions were AST-loaded read-only. Redirect fixture mocked Path.is_symlink; no claim of an additional actual NTFS junction test.',
            coverage=['all181 preflight before write','last canonical scan failure','missing181st source',
                'required archived member omitted/hash changed/size changed','Git-ineligible required member',
                'four nonregular fixed outputs','redirected fixed output','source/canonical/readability failure',
                'manifest changed during final verification','success order and final receipt assertions']),
        limits=['This is not an actual vault publication receipt or proof the future final copy/report inputs are available.',
            'Existing snapshot archive/credential/readability implementations were source-inspected and hash-bound, not re-executed end-to-end against the real vault.',
            'No real vault/project/runtime/database writes, broker requests, Git operations or credential reads occurred during these tests.',
            'The initial helper hash is retained as review history; its prior bytes were not independently saved and are not reconstructed.'],
        actual_publication_performed=False,orders_enabled=False,history_pruned=False))
    paths=[helper,tests,xml,receipt]
    entries=[dict(group='overnight_source_only_publication',kind='source_or_isolated_validation',
        original_source=binding(path),intended_member='docs/validation/overnight_curve_buildout_20260909/'+path.relative_to(BASE).as_posix(),
        copy_performed=False) for path in paths]
    selection=BASE/'CURATED_PUBLICATION_HELPER_REVIEW_EVIDENCE_SELECTION_20260909.json'
    save(selection,dict(schema_version='curated_publication_helper_review_selection_v1_20260909',
        created_epoch=time.time(),entries=entries,counts=dict(selected_files=len(entries),
        selected_bytes=sum(row['original_source']['bytes'] for row in entries)),
        actual_publication_performed=False,copy_performed=False,vault_writes=False,
        raw_inputs_selected=False,prior_selections_modified=False,requires_parent_credential_aware_scan=True))
    print(json.dumps(dict(review=binding(receipt),selection=binding(selection),helper=binding(helper),tests=binding(tests),xml=binding(xml))))

if __name__=='__main__':main()
