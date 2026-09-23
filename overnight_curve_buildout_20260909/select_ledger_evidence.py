"""Prepare an exact-byte evidence selection only; never copies into project/vault."""
from collections import Counter
import hashlib
import json
from pathlib import Path
import stat
import time

BASE=Path(__file__).resolve().parent
WORKSPACE=BASE.parent
TRAD=WORKSPACE/'trad'
PREFIX='docs/validation/overnight_curve_buildout_20260909/'

GROUPS={
'curve_contract_receipts':'''CURVE_CONTRACT_INDEPENDENT_BOUNDARIES_20260909T050133.json
CURVE_MANAGEMENT_ADAPTER_INDEPENDENT_REVIEW_20260909.json
CURVE_FILE_STORE_SHORT_PATH_VALIDATION_20260909.json
RECEIPT_HELPERS_IMPLEMENTATION_VALIDATION_20260909.json
curve_contract_tests_52_hardened.xml
curve_contract_adapter_tests_initial.xml
curve_bridge_tests_initial.xml
curve_file_store_initial_tests.xml
curve_file_store_short_path_tests.xml
quote_receipt_initial_tests.xml
receipt_helpers_bound_tests.xml
PIPELINE_PREACTIVATION_ACCEPTANCE_20260909.json
preactivation_combined_tests_20260909.xml
RECOVERED_CURVE_PILOT_INDEPENDENT_REVIEW_20260909.json
pilot_worker_stage_deadline_tests.xml''',
'recovered_original_targets':'''second_ridge/RECOVERED_SECOND_CURVE_BUILDOUT_20260909.md
second_ridge/RECOVERED_CURVE_SAMPLING_AND_CAPTURE_20260909.md
second_ridge/RECOVERED_CURVE_INDEPENDENT_SOURCE_REVIEW_20260909.json
second_ridge/RECOVERED_CURVE_SAMPLING_TARGET_INDEPENDENT_REVIEW_20260909.json
second_ridge/RECOVERED_BRIDGE_INDEPENDENT_SOURCE_REVIEW_20260909.json
second_ridge/INPUT_PRICE_CONVENTION_REVIEW_20260909.json
second_ridge/S5_CAPTURE_INDEPENDENT_SOURCE_REVIEW_20260909.json
second_ridge/S5_MBA_CAPTURE_IMPLEMENTATION_VALIDATION_20260909.json
second_ridge/ba_binding_tests.xml
second_ridge/model_integrity_tests.xml
second_ridge/target_sampling_final_tests.xml
second_ridge/s5_mba_capture_context_final_tests.xml''',
'pure_replay_native_outcomes':'''replay/API_CONTRACT_20260909.md
replay/CURVE_MANAGEMENT_REPLAY_ACCEPTANCE_20260909.json
replay/CURVE_REPLAY_INDEPENDENT_SOURCE_REVIEW_20260909.json
replay/replay_initial_tests.xml
replay/replay_final_106_tests.xml
replay/NATIVE_CURVE_OUTCOMES_API_20260909.md
replay/NATIVE_CURVE_ROLLING_SOURCE_API_20260909.md
replay/NATIVE_CURVE_OUTCOMES_ACCEPTANCE_20260909.json
replay/NATIVE_CURVE_OUTCOMES_ACCEPTANCE_V2_20260909.json
replay/NATIVE_OUTCOMES_INDEPENDENT_SOURCE_REVIEW_20260909.json
replay/NATIVE_OUTCOMES_ROLLING_SOURCE_INDEPENDENT_REVIEW_20260909.json
replay/native_outcomes_initial_tests.xml
replay/native_outcomes_final_47_tests.xml
replay/native_outcomes_wrapper_54_tests.xml''',
'paper_protocol_activation':'''observed_management/OBSERVED_MANAGER_PROTOCOL_20260909.md
observed_management/OBSERVED_MANAGER_ACCEPTANCE_20260909.json
observed_management/OBSERVED_MANAGER_INDEPENDENT_SOURCE_REVIEW_20260909.json
observed_management/OBSERVED_MANAGER_CLOCK_BRIDGE_INDEPENDENT_REVIEW_20260909.json
observed_management/observed_manager_initial_tests.xml
observed_management/observed_manager_final_tests.xml
observed_management/observed_manager_clock_bridge_tests.xml
observed_management/observed_manager_clock_bridge_final_tests.xml
observed_management/OBSERVED_MANAGEMENT_PRE_ACTIVATION_ACCEPTANCE_20260909.json
observed_management/runtime/PROCESS_LAUNCH_20260909T064210Z.json
observed_management/actual_input_preflight/ACTUAL_PREFLIGHT_FAILURE_001.json
replay/MANAGEMENT_IO_INDEPENDENT_SOURCE_REVIEW_20260909.json
replay/MOMENTUM_INDEPENDENT_SOURCE_REVIEW_20260909.json
replay/OBSERVED_MANAGEMENT_WORKER_INDEPENDENT_REVIEW_20260909.json
replay/OBSERVED_MANAGEMENT_WORKER_FINAL_INDEPENDENT_REVIEW_20260909.json
replay/observed_management_worker_initial_tests.xml
replay/observed_management_worker_fractional_integration_tests.xml
replay/observed_management_worker_final_tests.xml
replay/observed_management_io_review_fixes_tests.xml
replay/momentum_observation_review_fixes_tests.xml
replay/observed_management_final_pre_activation_tests.xml''',
'paper_verifier_cost_serializer':'''observed_management/OFFLINE_EPISODE_VERIFIER_20260909.md
observed_management/OFFLINE_EPISODE_VERIFIER_ACCEPTANCE_20260909.json
observed_management/OFFLINE_EPISODE_VERIFIER_INDEPENDENT_REVIEW_20260909.json
observed_management/verify_observed_episodes.py
observed_management/test_verify_observed_episodes.py
observed_management/offline_verifier_review_final_tests.xml
observed_management/QUOTE_COST_ATTRIBUTION_20260909.md
observed_management/COST_ATTRIBUTION_IMPLEMENTATION_VALIDATION_20260909.json
observed_management/COST_ATTRIBUTION_INDEPENDENT_REVIEW_20260909.json
observed_management/attribute_verified_episode_costs_v1.py
observed_management/test_attribute_verified_episode_costs_v1.py
observed_management/cost_attribution_initial_tests.xml
observed_management/cost_attribution_final_tests.xml
observed_management/EPISODE_RESULTS_SERIALIZER_20260909.md
observed_management/EPISODE_SERIALIZER_ACCEPTANCE_20260909.json
observed_management/EPISODE_SERIALIZER_INDEPENDENT_REVIEW_20260909.json
observed_management/EPISODE_SERIALIZER_REFERENCE_INDEPENDENT_REVIEW_20260909.json
observed_management/summarize_verified_episodes_v1.py
observed_management/test_summarize_verified_episodes_v1.py
observed_management/episode_serializer_source_refs_tests.xml''',
'three_completed_paper_results':'''observed_management/OBSERVED_THREE_EPISODE_FINAL_EVALUATION_PREPARATION_20260909.json
observed_management/OBSERVED_THREE_EPISODE_FINAL_RESULT_20260909.json
observed_management/OBSERVED_THREE_EPISODE_FINAL_REVIEW_20260909.md
observed_management/OBSERVED_THREE_EPISODE_FINAL_ACCEPTANCE_20260909.json
observed_management/retain_three_episode_final_result.py
observed_management/diagnose_episode_02_curve_policy.py
observed_management/episode_02_policy_diagnosis_001/EPISODE_02_CURVE_POLICY_DIAGNOSIS_20260909.md
observed_management/episode_02_policy_diagnosis_001/EPISODE_02_DIAGNOSIS_INDEPENDENT_REVIEW_20260909.json
observed_management/episode_02_policy_diagnosis_001/EPISODE_02_POLICY_DIAGNOSIS_ACCEPTANCE_20260909.json''',
'joint_original_outcome_reassessment':'''joint_v3_reassessment_v1/JOINT_V3_REASSESSMENT_REUSE_PLAN_20260909.md
joint_v3_reassessment_v1/JOINT_V3_REASSESSMENT_REUSE_PLAN_20260909.json
joint_v3_reassessment_v1/JOINT_SERIAL_REASSESSMENT_ACCEPTANCE_20260909.json
joint_v3_reassessment_v1/JOINT_SERIAL_REASSESSMENT_INDEPENDENT_REVIEW_20260909.json
joint_v3_reassessment_v1/run_joint_reassessment_v1.py
joint_v3_reassessment_v1/test_run_joint_reassessment_v1.py
joint_v3_reassessment_v1/joint_serial_wrapper_tests.xml
joint_v3_reassessment_v1/joint_serial_wrapper_final_tests.xml
joint_v3_reassessment_v1/summarize_matched_joint_dependence_v1.py
joint_v3_reassessment_v1/test_summarize_matched_joint_dependence_v1.py
joint_v3_reassessment_v1/joint_dependence_retained_scores_tests.xml
joint_v3_reassessment_v1/JOINT_SUPPLEMENT_ATTEMPT_001_FAILED_20260909.json
joint_v3_reassessment_v1/JOINT_MATCHED_DEPENDENCE_SUPPLEMENT_20260909.json
joint_v3_reassessment_v1/actual_assessment_001/V3_COMPLETED_PERFORMANCE_20260909.json
joint_v3_reassessment_v1/actual_assessment_001/WRAPPER_STARTED.json
joint_v3_reassessment_v1/actual_assessment_001/WRAPPER_COMPLETED.json''',
'passive_news_clock':'''news_clock_observer_v1/observe_news_clock_boundaries_v1.py
news_clock_observer_v1/test_observe_news_clock_boundaries_v1.py
news_clock_observer_v1/news_clock_observer_tests.xml
news_clock_observer_v1/NEWS_CLOCK_OBSERVER_INDEPENDENT_REVIEW_20260909.json
news_clock_observer_v1/NEWS_CLOCK_OBSERVER_ACCEPTANCE_20260909.json
news_clock_observer_v1/actual_observation_001/NEWS_CLOCK_PASSIVE_OBSERVATION_20260909.json
news_clock_observer_v1/actual_observation_001/OBSERVATION_STARTED.json
news_clock_observer_v2/observe_news_clock_boundaries_v2.py
news_clock_observer_v2/test_observe_news_clock_boundaries_v2.py
news_clock_observer_v2/news_clock_observer_v2_final_tests.xml
news_clock_observer_v2/NEWS_CLOCK_OBSERVER_V2_IMPLEMENTATION_VALIDATION_20260909.json
news_clock_observer_v2/NEWS_CLOCK_OBSERVER_V2_INDEPENDENT_REVIEW_20260909.json
news_clock_observer_v2/launch_001/LAUNCH_RECEIPT_20260909.json
news_clock_observer_v2/actual_observation_001/OBSERVATION_STARTED.json''',
'operations_preparations_and_dated_observations':'''operations_0551/capture_operations.py
operations_0551/OVERNIGHT_OPERATIONS_REVIEW_20260909.md
operations_v2/capture_operations_v2.py
operations_v2/test_capture_operations_v2.py
operations_v2/OPERATIONS_V2_ACCEPTANCE_20260909.json
operations_v2/OPERATIONS_ACTUAL_ATTEMPT_001_FAILED_20260909.json
operations_v2/OPERATIONS_ACTUAL_ATTEMPT_002_FAILED_20260909.json
operations_v2/operations_v2_inventory_final_tests.xml
operations_v2/actual_observation_003.json
operations_v3/capture_operations_v3.py
operations_v3/test_capture_operations_v3.py
operations_v3/operations_v3_tests.xml
operations_v3/OPERATIONS_V3_INDEPENDENT_SOURCE_REVIEW_20260909.json
operations_v3/OPERATIONS_V3_PREPARATION_ACCEPTANCE_20260909.json
operations_v3/OPERATIONS_V3_ACTUAL_ACCEPTANCE_20260909.json
operations_v3/actual_observation_001.json'''}


def binding(path):
    path=path.absolute()
    for part in reversed([path,*path.parents]):
        info=part.lstat();assert not stat.S_ISLNK(info.st_mode) and not getattr(info,'st_file_attributes',0)&1024
    raw=path.read_bytes();assert len(raw)<=2*1024*1024
    return {'path':str(path),'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)}


def main():
    entries=[];seen=set();selected_paths=set()
    def add(path,member,group):
        assert member.startswith(PREFIX) and '..' not in Path(member).parts and member not in seen
        item=binding(path);seen.add(member);selected_paths.add(path.absolute())
        entries.append({'group':group,'kind':'external_offline_helper_or_test' if path.suffix=='.py' else 'test_xml' if path.suffix=='.xml' else 'summary_protocol_or_receipt',
            'artifact_status':'accepted_or_explicitly_dated_evidence_not_combined_sample_counts','original_source':item,
            'intended_member':member,'copy_performed':False})
    for group,names in GROUPS.items():
        for name in names.splitlines():add(BASE/name,PREFIX+name,group)
    # Exact earlier source versions explain the material accepted clock bridge
    # and failed operational attempts; do not relabel them as current sources.
    for directory,group in [('observed_management/accepted_source','paper_prior_source_before_clock_bridge'),
        ('operations_v2/before_actual_attempt_fix','operations_prior_failed_source'),
        ('operations_v2/before_log_inventory_fix','operations_prior_failed_source')]:
        found=sorted((BASE/directory).iterdir());assert len(found)<=20
        for path in found:
            if path.is_file() and path.suffix=='.py':add(path,PREFIX+str(path.relative_to(BASE)).replace('\\','/'),group)
    for relative in ['program_assessment_20260909T0316Z/performance/assess_v3.py','revamp_baseline_20260908/performance/evaluate_frozen_baseline.py']:
        add(WORKSPACE/relative,PREFIX+'original_helper_dependencies/'+relative,'joint_original_arithmetic_dependencies')
    canonical={};registries=[]
    for name in ['observed_curve_management_v1_20260909.json','recovered_second_curve_pilot_v1_20260909.json','joint_price_news_study_v3_20260908.json']:
        path=TRAD/'config'/name;registries.append(binding(path));registry=json.loads(path.read_bytes())
        for source,expected in registry['source_bindings'].items():
            item=binding(TRAD/source);assert item['sha256']==expected
            canonical[source]=item
    # The source snapshot, not this validation selection, owns canonical files.
    dependencies=[{'original_source':v,'intended_source_snapshot_member':k,'copy_in_this_selection':False,
        'scope':'Existing canonical registered source; parent owns source snapshot and credential-aware scan.'} for k,v in sorted(canonical.items())]
    for name in ['test_oanda_forecast_curve_contract_v1.py','test_oanda_curve_management_adapter_v1.py','test_oanda_recovered_curve_bridge_v1.py',
        'test_oanda_forecast_curve_file_store_v1.py','test_oanda_research_quote_receipt_v1.py','test_oanda_native_curve_outcomes_v1.py',
        'test_oanda_curve_management_replay_v1.py','test_oanda_curve_momentum_observation_v1.py','test_oanda_observed_curve_management_v1.py',
        'test_oanda_observed_curve_management_worker_v1.py','test_oanda_observed_management_io_v1.py']:
        path=TRAD/name
        if path.exists():dependencies.append({'original_source':binding(path),'intended_source_snapshot_member':name,'copy_in_this_selection':False,'scope':'Canonical focused test; source-snapshot dependency only.'})
    local=[]
    for name,why in [
        ('observed_management/OBSERVED_THREE_EPISODE_FINAL_VERIFICATION_20260909.json','Full178-step verifier and2,727 file-reference inventory; compact result selected instead.'),
        ('observed_management/OBSERVED_THREE_EPISODE_FINAL_COST_ATTRIBUTION_20260909.json','Full quote-level virtual-trade decomposition; exact aggregate costs selected, original retained locally.'),
        ('observed_management/OBSERVED_THREE_EPISODE_FINAL_SUMMARY_20260909.json','Full per-step economic source-reference inventory; compact final all-three result selected.'),
        ('observed_management/episode_02_policy_diagnosis_001/EPISODE_02_CURVE_POLICY_DIAGNOSIS_20260909.json','Full59-context/step diagnosis; prose/acceptance/review selected.'),
        ('observed_management/actual_input_preflight_002/ACTUAL_MANAGEMENT_INPUT_PREFLIGHT_20260909.json','Contains actual source/candidate observations; accepted preactivation receipt retains original hash.'),
        ('second_ridge/RECOVERED_SECOND_CURVE_ACCEPTANCE_20260909.json','Detailed actual retained input/fit compatibility evidence; concise source/semantics reviews selected.'),
        ('second_ridge/RECOVERED_SECOND_CURVE_SAMPLING_TARGET_ACCEPTANCE_20260909.json','Detailed actual interpreted input variants; compact sampling/target report selected.')]:
        local.append({'original_source':binding(BASE/name),'disposition':'machine_local_not_selected','reason':why,'intended_member':None})
    counts=Counter(r['group'] for r in entries);total=sum(r['original_source']['bytes'] for r in entries)
    assert len(entries)<=180 and total<=8*1024*1024
    # Recheck exact selected bytes, never substitute a newer original hash.
    for row in entries:assert binding(Path(row['original_source']['path']))==row['original_source']
    value={'schema_version':'curated_ledger_evidence_selection_v1_20260909','created_epoch':time.time(),'status':'selection_prepared_no_copy_performed',
        'plan':binding(BASE/'PORTABLE_EVIDENCE_PLAN.md'),'entries':entries,'canonical_source_dependencies':dependencies,
        'registered_chain_dependencies':registries,'machine_local_dependencies':local,
        'counts':{'selected_files':len(entries),'selected_bytes':total,'groups':dict(counts),'canonical_dependencies':len(dependencies)},
        'remaining_parent_owned_evidence':['Final passive V2 result after12:45; current launch is not a completed no-reproduction result.',
            'Final reload/recovery and any later operations observations are parent-owned or API/table/retirement selection-owned; do not overwrite dated observations.'],
        'excluded_categories':['Raw news/article/event payloads and private_unique_bytes; sanitized sample logs remain local.',
            'Runtime SQLite/WAL files, per-pair inputs/quotes, raw price-capture inventories, full paper step trees and private benchmark mirrors.',
            'Model weights and actual historical/current datasets; hashes and machine-local source paths do not imply data portability.',
            'Process command arguments, account identifiers, credentials and stdout/stderr streams.',
            'The16MiB failed operations XML is retained locally; compact failed-attempt records and final accepted tests are selected.'],
        'source_identity_policy':'All selected files are copied only as exact original bytes if parent publishes. Earlier acceptance-declared hashes remain unchanged, even where current source has a later identity. Earlier paper/operations source variants are selected separately.',
        'dated_result_caveats':['Three paper episodes are one small dependent GBP/USD sequence, not178independent trials or actual account PnL.',
            'Joint same-row internal comparator scores share original decisions/targets; repeated snapshots must not be summed.',
            'Operations V2 reports original17-worker state; V3 reports15 after retirement. They are distinct dated snapshots.',
            'Initial600-second news probe did not reproduce the five earlier errors. V2 still running; no attribution of later input to an earlier failure.'],
        'privacy_and_export_checks':{'selection_only':True,'credential_aware_source_scan':'Required parent prepublication check; not claimed executed here.',
            'source_compile_and_inventory_bracket':'Required parent publication step; not performed by selection.',
            'raw_private_input_copy':False},
        'portability':'External helpers retain machine-specific paths and require original local model/data/runtime inputs. Inclusion is source/evidence portability, not standalone replayability.',
        'project_or_vault_copy_performed':False,'runtime_actions':False}
    path=BASE/'CURATED_LEDGER_EVIDENCE_SELECTION_20260909.json'
    with path.open('x',encoding='utf-8',newline='\n') as f:json.dump(value,f,sort_keys=True,indent=2);f.write('\n')
    lines=['# Ledger, paper-management and operational evidence selection','',
        f'Selected {len(entries)} exact files ({total:,} bytes) for proposed source-snapshot members under `{PREFIX}`. No project or vault copy has occurred.','',
        '| Group | Files |','|---|---:|']
    lines += [f'| {name} | {n} |' for name,n in counts.items()]
    lines += ['',
        'The final three-episode compact result contains all five isolated arms, original targets,178verifiedsteps, exact aggregate gross/spread/slippage/net attribution and explicit three-episode support. Full per-step/quote-level reports stay machine-local with exact hashes in the selection. The result is paper research, not account PnL or robust efficacy evidence.','',
        'The joint reassessment uses2,182originalcompleteddecisions and matched internal comparator rows. Its dependence supplement does not claim independent trials. Initial errors and the corrected retained-score arithmetic are preserved separately.','',
        'News-clock V1 completed600samples without reproducing the five prior errors; private exact source bytes are excluded. The reviewed V2 source, start and launch evidence are selected, with its final result pending12:45UTC. Operations V2/V3 remain separate dated17-worker/15-worker observations.','',
        'Historical clock-bridge and operational failure source variants are preserved under their original directories. Existing acceptance hashes are never rewritten to match current files. Canonical registered sources are listed as source-snapshot dependencies, not duplicated into validation.','',
        'Exclude raw news, raw quote inventories, runtime databases/WALs, model weights, full per-step evidence, process arguments and credentials. Required source scanning, compilation, exact-byte copying and final inventory verification remain parent publication steps. A copied helper still requires the original local inputs and paths for replay.','',
        'Selection SHA256: '+hashlib.sha256(path.read_bytes()).hexdigest()+'.']
    md=BASE/'CURATED_LEDGER_EVIDENCE_SELECTION_20260909.md'
    with md.open('x',encoding='utf-8',newline='\n') as f:f.write('\n'.join(lines)+'\n')
    print(json.dumps({'selection':binding(path),'summary':binding(md),'counts':value['counts']}))


if __name__=='__main__':main()
