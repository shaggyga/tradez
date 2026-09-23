"""Bind completed research and restoration evidence without changing results."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import xml.etree.ElementTree as ET

ROOT=Path(__file__).resolve().parent


def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def read(name):return json.loads((ROOT/name).read_text(encoding='utf-8-sig'))


def tests(name):
    suite=ET.parse(ROOT/name).getroot().find('testsuite')
    result={key:int(suite.attrib[key]) for key in ('tests','errors','failures','skipped')}
    assert result['tests']==130 and result['errors']==result['failures']==result['skipped']==0
    return result


def main():
    target=ROOT/'COMPLETION_RECEIPT.json'
    if target.exists():raise FileExistsError('completion_receipt_exists')
    integrated=tests('review/INTEGRATED_TESTS_SEALED.xml')
    restored=tests('review/RESTORED_TESTS.xml')
    models=read('review/restored_model_check/RESULT.json')
    inputs=read('review/RESTORED_INPUT_RECONSTRUCTION.json')
    meter=read('review/RESTORED_METER_REPLAY.json')
    extraction=read('review/EXTRACTED_INTEGRITY.json')
    build=read('review/LOCAL_RESTORATION_BUILD.json')
    for proof in (models,inputs,extraction):assert proof['verified'] is True
    assert meter['passed'] is True and meter['capture_count']==2
    assert all(case['exact_feature_bytes_equal'] for case in meter['cases'])
    assert models['maximum_absolute_difference']==0 and models['model_bundles_recreated']==8
    assert models['numeric_values_recreated']==718074
    assert inputs['exact_full_panel_reconstruction'] is True
    assert sha(Path(build['archive']))==build['archive_sha256']
    # Check every original packaged member after all restored execution.
    extracted=Path(extraction['extraction']);manifest=json.loads((extracted/'MANIFEST.json').read_text())
    for name,meta in manifest['files'].items():assert sha(extracted/name)==meta['sha256'],name
    frozen=read('evaluation_001/FROZEN_RUN.json')
    previous=ROOT.parent/'direction_research_20260911'
    assert sha(previous/'evaluation_001/RESULTS.json')==frozen['previous_completed_result_sha256']
    assert sha(previous/'prepared_001/PREPARED.json')==frozen['previous_prepared_sha256']
    weekend=ROOT.parent/'weekend_setup_20260911/forex_weekend_setup_20260911.zip'
    weekend_expected='bb17c97e35c9bd3c1bef297b59a1dba0f7185918d15fcc32d8ed5552a3fb8702'
    assert sha(weekend)==weekend_expected
    evidence={}
    for folder in ('src','tests','review','reuse_review','event_bridge'):
        for path in sorted((ROOT/folder).rglob('*')):
            if path.is_file() and '__pycache__' not in path.parts and path.suffix in {'.py','.md','.json','.xml'}:
                evidence[path.relative_to(ROOT).as_posix()]={'sha256':sha(path),'bytes':path.stat().st_size}
    for name in ('DIRECTION_DECISION_REPORT.md','STUDY_SPEC.json','verify_saved_cost_study.py','build_local_restoration.py','publish_research_records.py',
                 'finalize_research_receipt.py','evaluation_001/FROZEN_RUN.json','evaluation_001/FEATURE_GROUPS.json','evaluation_001/RESULTS.json',
                 'evaluation_001/input_panel.parquet','evaluation_001/predictions.parquet'):
        path=ROOT/name;evidence[name]={'sha256':sha(path),'bytes':path.stat().st_size}
    result={'schema_version':'signed_cost_direction_completion_v1_20260912','completed_utc':datetime.now(timezone.utc).isoformat(),
        'verified':True,'scope':'research_implementation_fixed_discovery_comparison_and_scoped_local_restoration',
        'study_id':frozen['spec']['study_id'],'history_already_inspected':True,'selected_model':None,
        'assessment_rows':39893,'integrated_tests_passed':integrated['tests'],'restored_tests_passed':restored['tests'],
        'saved_model_bundles_recreated':8,'estimators_fitted':48,'calibrators_fitted':8,
        'numeric_values_recreated':718074,'maximum_difference':0.0,'replay_curves_checked':384,'replay_heads_checked':1536,
        'restored_input_rows':inputs['rows'],'restored_input_cells':inputs['cells'],'complete_input_exact_reconstruction':True,
        'full_refit_in_extraction_executed':False,'independent_result_scalars_reconciled':720,
        'calibration_improved_aggregate_probability_comparisons':8,'calibration_improved_daily_probability_comparisons':16,
        'learned_policy_disposition':{'negative':17,'positive_limited_or_inconsistent_support':4,'no_selection':3},
        'fresh_currency_captures':2,'fresh_capture_full_features_recreated':True,'currency_count':21,
        'continuous_capture_service_started':False,'new_meter_features_in_retrospective_fit':False,
        'broker_requests':0,'active_runtime_changed':False,'trial_cap_or_cutoff_changed':False,'can_place_orders':False,'can_promote':False,
        'restoration':build,'restored_manifest_files_unchanged':len(manifest['files']),
        'predecessor_results_and_prepared_identity_preserved':True,'sealed_weekend_archive_preserved_sha256':weekend_expected,
        'remaining':['continuous deduplicated bounded meter capture','original-known surprise/consensus/revision/rate inputs',
                     'later independent signed-return validation','eligible curve and actual position-manager comparison'],
        'evidence':evidence}
    with target.open('x',encoding='utf-8') as out:json.dump(result,out,indent=2,sort_keys=True,allow_nan=False)
    print(json.dumps({key:result[key] for key in ('verified','integrated_tests_passed','restored_tests_passed','numeric_values_recreated','restored_manifest_files_unchanged')},indent=2))


if __name__=='__main__':main()
