"""Evidence-backed numeric gate disposition; never promotes unknown to verified."""
import hashlib,json
from collections import Counter,defaultdict

def require(value,message):
    if not value:raise ValueError(message)

def disposition(row,repair=None):
    component_only=row['values']['actual_value']['finite_decimal'] is None and bool(row['release_components'])
    repaired=repair is not None and bool(repair['adapter']['cells'])
    rejected=repair is not None and repair['adapter']['status']=='source_or_national_document_scope_rejected'
    unit=row['unit']['status'];clock=row['recorded_clock_summary']
    tasks=[]
    if not component_only and unit!='retained_unit_classified':tasks.append('recover_source_native_unit_frequency_binding')
    if row['reference']['period'] is None and not component_only:tasks.append('recover_explicit_reference_period_evidence')
    if not row['numeric_extraction_contract_id']:tasks.append('recover_missing_extraction_contract_provenance')
    return {'version_id':row['version_id'],'source_id':row['source_id'],'event_series_id':row['event_series_id'],
        'headline_unit_status':'not_applicable_component_only' if component_only else unit,
        'headline_reference_status':'not_applicable_component_only' if component_only else row['reference']['status'],
        'component_status':'rejected_document_scope' if rejected else 'repaired_scoped_evidence' if repaired else 'not_repaired_or_no_components',
        'component_evidence_cells':len(repair['adapter']['cells']) if repair else 0,
        'clock_status':'missing_retained_numeric_clock' if clock['missing_or_invalid_numeric_clocks'] else 'retained_clock_claim_only',
        'original_prior_vintage_verified':False,'independent_issuer_binding_verified':False,
        'archived_consensus_verified':False,'surprise_status':'disabled_no_verified_archived_expectations',
        'adapter_activation_status':'no_repaired_adapter_activation_receipt' if repair else 'not_independently_established',
        'eligible_offline_tasks':tasks,'external_evidence_gates':['original_http_and_extraction_ready_receipts','independent_issuer_binding','archived_expectations_if_surprise_requested','original_prior_vintage_if_revision_feature_requested'],
        'text_event_path_blocked_by_missing_consensus':False,'numeric_fact_admission':False,'forecast_admission':False}

def build(blobs):
    read=lambda n:json.loads(blobs[n]);plan=read('NUMERIC_DISPOSITION_PLAN.json')
    require(set(plan['payloads'])==set(blobs)-{'NUMERIC_DISPOSITION_PLAN.json'},'disposition_input_inventory_mismatch')
    require(all(hashlib.sha256(blobs[n]).hexdigest()==h for n,h in plan['payloads'].items()),'disposition_input_pin_mismatch')
    evidence=read('numeric_evidence_audit.json');repairs={r['version_id']:r for r in read('component_repair_evidence.json')}
    require(len({r['version_id'] for r in evidence})==len(evidence),'duplicate_numeric_version')
    require(set(repairs)<=set(r['version_id'] for r in evidence),'orphan_component_repair')
    rows=[disposition(r,repairs.get(r['version_id'])) for r in evidence];groups=defaultdict(list)
    for r in rows:groups[r['source_id']].append(r)
    sources=[{'source_id':sid,'versions':len(rs),'version_ids':sorted(r['version_id'] for r in rs),'eligible_offline_task_counts':dict(sorted(Counter(t for r in rs for t in r['eligible_offline_tasks']).items())),
        'component_evidence_cells':sum(r['component_evidence_cells'] for r in rs),'forecast_admission':False} for sid,rs in sorted(groups.items())]
    next_versions=sorted(r['version_id'] for r in rows if 'recover_source_native_unit_frequency_binding' in r['eligible_offline_tasks'])
    queue={'next_item':'macro_numeric_source_native_unit_binding_v2','target_version_ids':next_versions,'target_sources':sorted({r['source_id'] for r in rows if r['version_id'] in next_versions}),
        'scope':'Recover retained source-native series metadata, current frozen source configs and exact existing parser formulas for these9 records; distinguish MoM/YoY/quarterly and seasonal definitions. Reproduce against preserved payloads if present; otherwise leave source binding unresolved. Do not infer from source names or generated summaries.',
        'dependencies':['MACRO_NUMERIC_AUDIT_20260922_200101','MACRO_COMPONENT_STATE_20260922_203059'],
        'acceptance':['exact original metadata/formula evidence','explicit unknowns where original payload absent','typed frequency/seasonality distinct from issuer/readiness proof','no surprise, fit or forecast promotion','portable source/input/output identity and meaningful adversarial tests'],
        'then':'resolve remaining explicit reference/extraction provenance gaps using preserved evidence; retain blocked external gates and independent text/event path',
        'deferred':['GPT/advisor comparisons','paid calls','broker/service/account actions','D-drive investigation']}
    report={'versions':len(rows),'sources':len(sources),'unit_binding_target_versions':len(next_versions),'unit_binding_target_sources':queue['target_sources'],
        'component_evidence_cells':sum(r['component_evidence_cells'] for r in rows),'repaired_document_scope_rejections':sum(r['component_status']=='rejected_document_scope' for r in rows),
        'offline_task_counts':dict(sorted(Counter(t for r in rows for t in r['eligible_offline_tasks']).items())),
        'numeric_surprises_computed':0,'base_models_fitted':0,'forecast_features_admitted':0,'forecast_improvement_proven':False,'independent_review':False,'next_item':queue['next_item']}
    boundaries={'design_sha256':plan['design_sha256'],'sections':['14.2','14.3','14.4','14.5'],'independent_issuer_attestations':read('issuer_audit_report.json')['independent_issuer_attestations'],
        'component_reconstruction_rows':read('component_state_report.json')['reconstructed_cell_event_cutoff_rows'],'source_asof_reconstruction_is_activation':False,'missing_consensus_blocks_only_surprise':True,
        'original_vintages_verified':0,'protected_forecast_confirmation':'unchanged','engineering_ready':False}
    return {'numeric_qualification_disposition.json':rows,'numeric_source_disposition.json':sources,'numeric_next_work_package.json':queue,'numeric_disposition_boundaries.json':boundaries,'numeric_disposition_report.json':report}
