from copy import deepcopy
from decimal import Context,Decimal,localcontext
import json
import pytest
import summarize_verified_episodes_v1 as s

SOURCES={f'fixture_{n}.py':f'{n:064x}' for n in range(20)}

def episode(number=1,steps=1,terminal=False,unresolved=False):
    eid=f'episode_{number:02}';start=number*4000.;target=start+3600
    cash={'usd_curve_manager':'1.23456789012345','usd_momentum_manager':'-0.23456789012345',
          'legacy_momentum_reference':'-2','curve_hold_no_rotation':'0.5','no_trade':'0'}
    rows=[]
    for i in range(steps):
        last=terminal and i==steps-1
        arithmetic={}
        for arm in s.ARMS:
            positioned=arm!='no_trade' and (not last or unresolved)
            amount=cash[arm] if last and not unresolved else '0'
            arithmetic[arm]=dict(realized_usd=amount,liquidation_usd=(None if last and unresolved and positioned else
                '-99' if positioned else amount),position_open=positioned,
                open_legs=int(i==0 and arm!='no_trade'),close_legs=int(last and not unresolved and arm!='no_trade'))
        rows.append(dict(step_id=f'step_{i:03}',scheduled_epoch=target-1 if last else start+i*60,
            completed_epoch=target+1 if last else start+i*60+2,terminal=last,
            decisions={arm:'wait' if arm=='no_trade' else 'exit' if last else 'enter' if i==0 else 'hold' for arm in s.ARMS},
            action_statuses={arm:'no_virtual_fill' if last and unresolved else 'applied_virtual_action' for arm in s.ARMS},
            independent_arithmetic=arithmetic,curve_candidate_count=0 if last else 1,
            momentum_candidate_count=0 if last else 1,selected_observation_epoch=None if last and unresolved else start+i*60+1))
    perarm={arm:dict(realized_usd=value['realized_usd'],latest_liquidation_usd=value['liquidation_usd'],position_open=value['position_open'])
        for arm,value in rows[-1]['independent_arithmetic'].items()}
    with localcontext(Context(prec=96)):
        delta=str(Decimal(cash['usd_curve_manager'])-Decimal(cash['usd_momentum_manager'])) if terminal and not unresolved else None
    return dict(episode_id=eid,status='terminal_unresolved' if terminal and unresolved else 'terminal_flat' if terminal else 'in_progress_or_partial',
        registered_start_epoch=start,native_target_epoch=target,verified_completed_steps=len(rows),steps=rows,
        original_published_source_evidence={'files':[{'relative_path':f'curves/{number}/curve.json','sha256':'a'*64}]},
        missed_slots=[],uncompleted_or_unobserved_slots=[] if terminal else [{'step_id':'step_099','scheduled_epoch':target-1,'terminal':True}],
        per_arm=perarm,terminal_all_five_flat=terminal and not unresolved,
        completed_matched_endpoint_eligible=terminal and not unresolved,matched_terminal_curve_minus_momentum_usd=delta)

def report(episodes=None,at=17000.):
    active={row['episode_id']:row for row in (episodes or [episode()])}
    return dict(schema_version=s.VERIFIER_SCHEMA,status='passed',registry_sha256=s.REGISTRY_SHA,helper_sha256=s.VERIFIER_SHA,
        verification_started_epoch=at-1,verification_completed_epoch=at,source_bindings=deepcopy(SOURCES),
        episodes=[active.get(eid,dict(episode_id=eid,status='not_initialized')) for eid in s.EPISODES],issues=[],
        runtime_writes=False,broker_actions=False,model_fits=False)

def summary(reports):return s.summarize_reports(reports,clock=lambda:20000.)

def test_three_successive_final_reports_count_each_episode_once():
    first=episode(1,2,True);second=episode(2,2,True);third=episode(3,2,True)
    reports=[report([first],17000),report([first,second],17001),report([first,second,third],17002)]
    out=summary(reports+[deepcopy(reports[-1])])
    assert out['unique_input_report_count']==3 and out['input_report_count']==4
    assert out['matched_completed_support']['eligible_episode_count']==3
    assert out['hypothetical_completed_scenario_totals_by_arm_usd']['usd_curve_manager']=='3.70370367037035'
    assert out['account_state_observed'] is False and 'total_positions' not in out
    assert all(e['candidate_support']['nonterminal_decision_steps']==1 for e in out['episodes'])

def test_open_marks_do_not_become_completed_zero_or_profit():
    out=summary([report()]);first=out['episodes'][0]
    assert first['per_arm']['usd_curve_manager']['latest_hypothetical_liquidation_usd']=='-99'
    assert first['per_arm']['usd_curve_manager']['virtual_scenario_position_open'] is True
    assert out['matched_completed_support']['eligible_episode_count']==0
    assert all(value is None for value in out['hypothetical_completed_scenario_totals_by_arm_usd'].values())
    assert first['per_arm']['no_trade']['realized_scenario_cash_usd']=='0'

def test_unresolved_terminal_preserves_missing_mark_and_withholds_comparison():
    out=summary([report([episode(1,2,True,True)])])
    assert out['episodes'][0]['status']=='terminal_unresolved'
    assert out['episodes'][0]['per_arm']['usd_curve_manager']['latest_hypothetical_liquidation_usd'] is None
    assert out['matched_completed_curve_minus_momentum_sum_usd'] is None

def test_progress_snapshot_replaces_prior_snapshot_without_double_count():
    before=report([episode(1,1)],17000);after=report([episode(1,2,True)],17001)
    out=summary([after,before]);assert out['episodes'][0]['verified_completed_steps']==2
    assert out['episodes'][0]['per_arm']['usd_curve_manager']['decision_counts']=={'enter':1,'exit':1}
    assert out['episodes'][0]['per_arm']['usd_curve_manager']['virtual_open_legs']==1

@pytest.mark.parametrize('mutation',['changed','disappeared','target'])
def test_retained_previous_evidence_cannot_change_or_disappear(mutation):
    before=report([episode(1,1)],17000);after=report([episode(1,2,True)],17001)
    if mutation=='changed':after['episodes'][0]['steps'][0]['extra']='changed'
    if mutation=='disappeared':after['episodes'][0]=dict(episode_id='episode_01',status='not_initialized')
    if mutation=='target':after['episodes'][0]['native_target_epoch']+=60
    with pytest.raises(ValueError,match='summary_prior_step|summary_original_episode_identity'):
        summary([before,after])

def test_latest_issue_never_reuses_older_favorable_completed_cash():
    before=report([episode(1,2,True)],17000);after=report([],17001)
    after['episodes']=[e for e in after['episodes'] if e['episode_id']!='episode_01']
    after['issues']=[{'episode_id':'episode_01','reason_code':'source_changed'}];after['status']='issues'
    out=summary([before,after])
    assert out['episodes'][0]['status']=='latest_verification_issue'
    assert out['matched_completed_support']['eligible_episode_count']==0

@pytest.mark.parametrize('field,value',[('registry_sha256','b'*64),('helper_sha256','b'*64),('verification_completed_epoch',30000)])
def test_wrong_registry_verifier_or_future_report_refused(field,value):
    value_report=report();value_report[field]=value
    with pytest.raises(ValueError):summary([value_report])

def test_false_completed_support_cannot_hide_missing_slots():
    value=report([episode(1,2,True)]);value['episodes'][0]['uncompleted_or_unobserved_slots']=[{'step_id':'step_003','scheduled_epoch':7000}]
    with pytest.raises(ValueError,match='false_completed'):summary([value])

def test_cash_must_equal_latest_independently_verified_state():
    value=report([episode(1,2,True)]);value['episodes'][0]['per_arm']['usd_curve_manager']['realized_usd']='999'
    with pytest.raises(ValueError,match='cash_not_latest'):summary([value])

def test_fixed_decimal_totals_ignore_low_ambient_precision():
    values=[report([episode(1,2,True),episode(2,2,True),episode(3,2,True)])]
    normal=summary(values)
    with localcontext(Context(prec=2)):low=summary(values)
    assert low==normal

def test_empty_source_disposition_is_preserved_not_filled_with_arm_zeroes():
    value=report();value['episodes'][0]={'episode_id':'episode_01','status':'retained_withholding_or_failure',
        'retained_episode_result':{'status':'withheld','reason_code':'no_initial_curve'}}
    out=summary([value]);assert out['episodes'][0]['per_arm']=={}
    assert out['episodes'][0]['retained_disposition']['reason_code']=='no_initial_curve'

def test_changed_source_closure_refused_even_when_hashes_are_well_formed():
    before=report(at=17000);after=report(at=17001);after['source_bindings']['fixture_0.py']='f'*64
    with pytest.raises(ValueError,match='source_closure_changed'):summary([before,after])

def test_duplicate_json_keys_refused():
    with pytest.raises(ValueError,match='duplicate_json_key'):s.strict_json(b'{"status":1,"status":2}')

def test_exclusive_cli_summary_contains_source_file_hashes(tmp_path):
    path=tmp_path/'input.json';path.write_text(json.dumps(report()))
    output=tmp_path/'summary.json'
    s.main(['--report',str(path),'--output',str(output)])
    body=json.loads(output.read_bytes());assert len(body['input_file_evidence'])==1
    assert body['summary_sha256']==s.digest({k:v for k,v in body.items() if k!='summary_sha256'})
    with pytest.raises(FileExistsError):s.main(['--report',str(path),'--output',str(output)])

def test_exact_original_quote_and_settlement_refs_survive_without_inferred_cost():
    value=report()
    value['verified_files']=[dict(relative_path=path,sha256='a'*64,bytes=200,verified_read_epoch=16999.)
        for path in ('episodes/episode_01/episode_config.json',
        'episodes/episode_01/steps/step_000/settlement.json',
        'episodes/episode_01/steps/step_000/quotes/q000_raw.json',
        'episodes/episode_02/episode_config.json')]
    evidence=summary([value])['episodes'][0]['economic_source_evidence']
    assert len(evidence['verified_episode_files'])==3
    assert all(row['sha256']=='a'*64 for row in evidence['verified_episode_files'])
    assert evidence['direct_price_cost_attribution_performed'] is False
    assert 'gross_pnl' not in evidence and 'spread_cost' not in evidence

def test_duplicate_economic_reference_refused():
    value=report();row=dict(relative_path='episodes/episode_01/episode_config.json',sha256='a'*64,bytes=200)
    value['verified_files']=[row,deepcopy(row)]
    with pytest.raises(ValueError,match='duplicate_file_reference'):summary([value])
