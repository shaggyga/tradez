"""Compact static-source curation; exclusive external evidence only."""
import ast
import hashlib
import json
import os
from pathlib import Path
import time

BASE=Path(__file__).resolve().parent
ROOT=BASE.parent/'trad'
AUDIT=BASE/'management_correctness_audit_v1'

def binding(path):
    raw=path.read_bytes();return dict(path=str(path),sha256=hashlib.sha256(raw).hexdigest(),bytes=len(raw))

def save(path,value):
    raw=value if isinstance(value,bytes) else (json.dumps(value,indent=2,sort_keys=True,allow_nan=False)+'\n').encode()
    with path.open('xb') as handle:handle.write(raw);handle.flush();os.fsync(handle.fileno())
    return binding(path)

def main():
    transition=json.loads((AUDIT/'ACCOUNT_RECONCILIATION_SOURCE_TRANSITION_V2_20260909.json').read_bytes())
    peer=AUDIT/'ACCOUNT_MANAGER_RECONCILIATION_INDEPENDENT_REVIEW_20260909.json'
    review=json.loads(peer.read_bytes());assert review['remaining_blockers']==[]
    source=[];lines={}
    for row in transition['transition']:
        path=Path(row['canonical_after']['path']);assert binding(path)==row['canonical_after']
        assert binding(Path(row['accepted_copy']['path']))['sha256']==row['canonical_after']['sha256']
        source.append(binding(path))
        lines[path.name]={n.name:{'line':n.lineno,'end_line':n.end_lineno} for n in ast.walk(ast.parse(path.read_bytes()))
            if isinstance(n,ast.FunctionDef) and n.name in {'observe_open_trade','find_open_trade','trade_lookup_blocks_open',
                'handle_position_action','tighten_trade','partial_close_trade','_close_with_state_confirmation',
                'run_volatile_weekend_flatten_pass','observe_trade_rows','reconcile_reduction','protective_update'}}
    acceptance=dict(schema_version='account_manager_correction_acceptance_v1_20260909',created_epoch=time.time(),
        status='tested_reviewed_source_not_activated',source_bindings=source,final_function_lines=lines,
        transition=binding(AUDIT/'ACCOUNT_RECONCILIATION_SOURCE_TRANSITION_V2_20260909.json'),
        independent_review=binding(peer),report=binding(AUDIT/'ACCOUNT_RECONCILIATION_CORRECTION_20260909.md'),
        tests=transition['tests'],original_defect_audit=transition['original_audit'],
        all_four_active_research_closures_unchanged=True,
        broker_calls=0,account_manager_module_imports=0,manager_starts_or_reloads=0,live_database_writes=0,
        policy_or_parameter_tuning=False,predictive_or_pnl_improvement_claim=False,
        note='Final function lines are recomputed here; the earlierV2 transition carries prior85 line tables beside the exact final source hash. Original before files and both source transitions remain preserved.')
    accepted=save(AUDIT/'ACCOUNT_MANAGER_CORRECTION_ACCEPTANCE_20260909.json',acceptance)
    entries=[]
    names=[
        'EXISTING_MANAGER_CORRECTNESS_AUDIT_20260909.json','test_existing_account_reconciliation_diagnostics.py',
        'existing_account_reconciliation_diagnostics.xml','existing_account_reconciliation_diagnostics_final.xml',
        'ACCOUNT_RECONCILIATION_SOURCE_TRANSITION_20260909.json','ACCOUNT_RECONCILIATION_SOURCE_TRANSITION_V2_20260909.json',
        'ACCOUNT_RECONCILIATION_CORRECTION_20260909.md','ACCOUNT_MANAGER_RECONCILIATION_INDEPENDENT_REVIEW_20260909.json',
        'ACCOUNT_MANAGER_CORRECTION_ACCEPTANCE_20260909.json','test_account_reconciliation_candidate.py',
        'account_reconciliation_candidate_final_tests.xml','account_reconciliation_candidate_counter_final_tests.xml',
        'before_source/oanda_technical_account_manager_auto.py','before_source/oanda_advisor_account_manager_auto.py',
        'accepted_source/oanda_technical_account_manager_auto.py',
        'accepted_source_v2/oanda_technical_account_manager_auto.py','accepted_source_v2/oanda_advisor_account_manager_auto.py',
        'accepted_source_v2/oanda_trade_reconciliation_v1.py',
    ]
    paths=[AUDIT/name for name in names]+[
        BASE/'replay/management_channels_initial_tests.xml',BASE/'replay/management_channels_final_tests.xml',
        BASE/'replay/management_channels_v1/MANAGEMENT_CHANNELS_INDEPENDENT_REVIEW_20260909.json',
        BASE/'replay/management_channels_v1/accepted_source/oanda_curve_management_channels_v1.py',
        BASE/'replay/management_channels_v1/accepted_source/test_oanda_curve_management_channels_v1.py']
    for path in paths:
        assert path.is_file()
        entries.append(dict(original_source=binding(path),intended_member='docs/validation/overnight_curve_buildout_20260909/'+path.relative_to(BASE).as_posix(),
            group='management_correctness_and_separate_channels',kind='static_source_test_or_compact_evidence',copy_performed=False))
    deps=[ROOT/name for name in ('oanda_technical_account_manager_auto.py','oanda_advisor_account_manager_auto.py',
        'oanda_trade_reconciliation_v1.py','oanda_curve_management_channels_v1.py','test_oanda_curve_management_channels_v1.py',
        'oanda_curve_management_replay_v1.py','test_oanda_curve_management_replay_v1.py',
        'src/forex_system/research/sequential_portfolio_replay_v1.py')]
    selection=dict(schema_version='curated_management_correction_evidence_selection_v1_20260909',created_epoch=time.time(),
        status='prepared_not_copied',copy_performed=False,prior_selections_modified=False,entries=entries,
        counts=dict(selected_files=len(entries),selected_bytes=sum(row['original_source']['bytes'] for row in entries)),
        canonical_source_dependencies=[dict(original_source=binding(path),intended_source_snapshot_member=path.relative_to(ROOT).as_posix(),copy_in_this_selection=False,
            scope='Canonical static source; parent owns credential-aware source snapshot and publication.') for path in deps],
        static_source_history_note='Exact before account sources and the material85→86 counter correction are retained. Other historical acceptances keep their original hashes. No active frozen research source changed.',
        exclusions=['No account identifiers, account snapshots, credentials, broker responses, raw news, raw quotes, runtime databases, or process arguments were read for these tests or included as evidence.',
                    'No live manager integration, simulated profit comparison, or source-deployment claim is made. Source files remain subject to the parent credential-aware export scan.'],
        source_acceptance=accepted)
    output=BASE/'CURATED_MANAGEMENT_CORRECTION_EVIDENCE_SELECTION_20260909.json'
    print(json.dumps(save(output,selection)))
    text='# Management source-correction evidence selection\n\n'
    text+=f"Prepared {len(entries)} exact static-source/test/report files ({selection['counts']['selected_bytes']:,} bytes), with no copy or publication performed.\n\n"
    text+='The account-manager correction has86 AST/fake-transport cases and independent source review. Both modified account sources lie outside the four frozen research closures. Original source versions and material diagnostic failures remain preserved. The separate prepared-channel selector has39 pure tests including8 exact unchanged-selector differential cases; its chain-replaying study adapter and prospective integration remain open. No broker execution or PnL improvement is claimed.\n\n'
    text+='The JSON maps exact source hashes to intended validation members and separately lists eight canonical source dependencies. Raw runtime/account/news/quote evidence is excluded. Parent owns scanner, exact copy, source snapshot and vault publication.\n'
    print(json.dumps(save(BASE/'CURATED_MANAGEMENT_CORRECTION_EVIDENCE_SELECTION_20260909.md',text.encode())))
    print(json.dumps(accepted))

if __name__=='__main__':main()
