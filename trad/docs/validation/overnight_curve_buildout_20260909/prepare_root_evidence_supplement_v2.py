"""Extend the original explicit root selection without replacing its evidence."""
from pathlib import Path
import hashlib
import json
import time

BASE=Path(__file__).resolve().parent
ORIGINAL=BASE/'CURATED_ROOT_EVIDENCE_SELECTION_20260909.json'
EXPECTED='ae213bd2dc92df1c5971a9fff267804e4983faeb71f10ce11da708c6b14e75b2'
ADDITIONS=[
    'prepare_root_evidence_supplement_v1.py',
    'prepare_root_evidence_supplement_v2.py',
    'PORTABLE_ROOT_PREFLIGHT_001_20260909.json',
    'preflight_source_export.py',
    'run_source_export_preflight_v2.py',
    'source_export_preflight_002/SOURCE_EXPORT_PREFLIGHT_20260909.json',
    'prepare_overnight_canonical_mappings_v1.py',
    'canonical_mapping_preparation_v1/forex_model_vault_sync_before.py.txt',
    'canonical_mapping_preparation_v1/CANONICAL_MAPPING_PREPARATION_20260909.json',
    'retain_reviewed_document_history_v1.py',
    'documentation_reviewed_history_1021_20260909/FOREX_OVERNIGHT_CURVE_BUILDOUT_20260909.md',
    'documentation_reviewed_history_1021_20260909/FOREX_PENDING_IMPROVEMENTS.md',
    'documentation_reviewed_history_1021_20260909/REVIEWED_DOCUMENT_HISTORY_20260909.json',
    'merge_additional_evidence_selections_v1.py',
]


def sha(raw):return hashlib.sha256(raw).hexdigest()


def main():
    raw=ORIGINAL.read_bytes()
    if sha(raw)!=EXPECTED:raise ValueError('original_root_selection_changed')
    old=json.loads(raw);entries=list(old['entries'])
    for name in ADDITIONS:
        path=BASE/name;raw=path.read_bytes()
        entries.append(dict(intended_member='docs/validation/overnight_curve_buildout_20260909/'+name,
            original_source=dict(path=str(path),sha256=sha(raw),bytes=len(raw)),
            kind='source' if path.suffix=='.py' else 'retained_evidence',
            group='root_documentation_history_and_publication_preparation',
            artifact_status='Original dated evidence; actual final source/archive publication is separately verified.'))
    for entry in entries:
        source=entry['original_source'];raw=Path(source['path']).read_bytes()
        if sha(raw)!=source['sha256'] or len(raw)!=source['bytes']:raise ValueError('selected_source_changed')
    need_names=[row['intended_member'].casefold() for row in entries]
    if len(set(need_names))!=len(need_names):raise ValueError('duplicate_root_member')
    value=dict(schema_version='curated_root_evidence_selection_v2_20260909',prepared_epoch=time.time(),
        helper_sha256=sha(Path(__file__).read_bytes()),original_selection=dict(path=str(ORIGINAL),sha256=EXPECTED),
        original_selection_unchanged=True,entries=entries,canonical_source_dependencies=[],
        file_count=len(entries),total_bytes=sum(x['original_source']['bytes'] for x in entries),
        no_project_or_vault_copy_performed=True,
        limits=old['limits']+['Exact reviewed interim document bytes are preserved under their original hashes when current documents change.'])
    out=BASE/'CURATED_ROOT_EVIDENCE_SELECTION_V2_20260909.json'
    raw=(json.dumps(value,indent=2,sort_keys=True)+'\n').encode()
    with out.open('xb') as h:h.write(raw)
    print(json.dumps(dict(path=str(out),sha256=sha(raw),files=len(entries),bytes=value['total_bytes'])))


if __name__=='__main__':main()
