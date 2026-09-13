import gzip
import json
from pathlib import Path
from types import SimpleNamespace
import sys
import time
import pytest
import capture_operations_v2 as c

FLAGS=dict(research_only=True,**{k:False for k in c.FLAGS})
PAPER_FLAGS=dict(research_only=True,**{k:False for k in c.PAPER_FLAGS})


def save(path,value):path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(value))


def pilot_cycle(path,index,pairs=3):
    value=dict(schema_version='recovered_curve_pilot_cycle_v1_20260909',cycle_id='cycle_'+str(index),
        pairs=[dict(instrument=p,attempts=[]) for p in c.PAIRS[:pairs]],started_epoch=time.time()-2,completed_epoch=time.time()-1,
        curves_issued=0,curves_published=0,curves_consumed=0,skipped_prior_schedule_slots=0,**FLAGS)
    save(path/'cycles'/value['cycle_id']/'cycle_completed.json',value);return value


def test_new_pilot_inventory_admits_more_than_old_200_without_scoring(tmp_path):
    for index in range(201):pilot_cycle(tmp_path,index)
    value=c.pilot(dict(output_root=str(tmp_path)))
    assert value['completed_cycle_count']==201 and value['cycle_inventory_cap']==600
    assert value['aggregate']['curves_consumed']==0


def test_pilot_inventory_caps_before_file_reads(tmp_path,monkeypatch):
    for i in range(601):(tmp_path/'cycles'/('cycle_'+str(i))).mkdir(parents=True)
    monkeypatch.setattr(c,'read',lambda *a:(_ for _ in ()).throw(AssertionError('should_not_read')))
    with pytest.raises(ValueError,match='directory_inventory_bound'):c.pilot(dict(output_root=str(tmp_path)))


def test_pilot_stop_partial_pairs_and_incomplete_cycle_are_explicit(tmp_path):
    pilot_cycle(tmp_path,1,pairs=1);(tmp_path/'cycles/cycle_2').mkdir()
    result=c.pilot(dict(output_root=str(tmp_path)))
    assert result['partial_cycle_names']==['cycle_2']
    assert result['latest']['pairs_observed']==['EUR_USD']


def test_pilot_counterfeit_stage_count_is_not_accepted(tmp_path):
    value=pilot_cycle(tmp_path,1);value['curves_published']=1
    save(tmp_path/'cycles/cycle_1/cycle_completed.json',value)
    with pytest.raises(ValueError,match='pilot_stage_count'):c.pilot(dict(output_root=str(tmp_path)))


def test_paper_future_incomplete_and_completed_states_stay_operational(tmp_path):
    now=time.time();reg=dict(output_root=str(tmp_path),episode_nominal_start_epochs=[now-120,now-60,now+120])
    save(tmp_path/'episodes/episode_01/episode_result.json',dict(status='completed',completed_epoch=now-1,
        terminal_all_flat=True,original_target_epoch=now-2,actual_broker_actions=0,realized_usd_by_arm={'secret_unwanted':'999'},**PAPER_FLAGS))
    save(tmp_path/'episodes/episode_02/steps/step_000/step_completed.json',dict(completed_epoch=now-1,terminal=False,state_sha256='a'*64,**PAPER_FLAGS))
    result=c.paper(reg)
    assert [r['status'] for r in result['episodes']]==['producer_reported_completed','observed_without_final_result','scheduled_not_started']
    assert 'secret_unwanted' not in json.dumps(result) and 'realized_usd_by_arm' not in json.dumps(result)


def risk_cycle(root,ref,status='withheld_or_partial',published=True):
    pairs=[dict(instrument=p,status=status,phase='published',capture_attempted=True,capture_invoked=True,
        artifacts={'issued':{},'publication':{}} if published else {},reason_code='not_ready',**FLAGS) for p in c.PAIRS]
    value=dict(cycle_epoch=ref,registry_sha256=c.REGISTRIES['risk'][1],started_epoch=ref+.01,completed_epoch=ref+.1,
        pairs=pairs,retained_issue_count=3 if published else 0,publication_count=3 if published else 0,
        consumption_count=0,complete_chain_count=0,**FLAGS)
    save(root/'cycles'/str(ref)/'cycle_completed.json',value);return value


def test_risk_four_stage_counts_and_missing_schedule_not_scored_zeros(tmp_path,monkeypatch):
    ref=1788941760;monkeypatch.setattr(c.time,'time',lambda:ref+321)
    risk_cycle(tmp_path,ref)
    value=c.risk(dict(output_root=str(tmp_path),first_reference_epoch=ref,last_reference_epoch=ref+600))
    assert value['stage_counts']==dict(retained_issue_count=3,publication_count=3,consumption_count=0,complete_chain_count=0)
    assert value['planned_status_counts']=={'producer_reported_withheld_or_partial':3,'missing_entire_cycle':3,'scheduled_not_started':3}


def test_risk_inclusive_deadline_and_incomplete_cycle(tmp_path,monkeypatch):
    ref=1788941760;monkeypatch.setattr(c.time,'time',lambda:ref+20)
    (tmp_path/'cycles'/str(ref)).mkdir(parents=True)
    reg=dict(output_root=str(tmp_path),first_reference_epoch=ref,last_reference_epoch=ref)
    assert c.risk(reg)['planned_status_counts']=={'publication_deadline_not_elapsed':3}
    monkeypatch.setattr(c.time,'time',lambda:ref+20.001)
    assert c.risk(reg)['planned_status_counts']=={'missing_pair_or_cycle_completion':3}


def test_risk_forged_complete_count_rejected(tmp_path,monkeypatch):
    ref=1788941760;monkeypatch.setattr(c.time,'time',lambda:ref+30);value=risk_cycle(tmp_path,ref)
    value['complete_chain_count']=3;save(tmp_path/'cycles'/str(ref)/'cycle_completed.json',value)
    with pytest.raises(ValueError,match='risk_stage_count'):c.risk(dict(output_root=str(tmp_path),first_reference_epoch=ref,last_reference_epoch=ref))


def test_news_reads_only_latest_gzip_and_retains_no_decoded_content(tmp_path):
    old=tmp_path/('a'*64+'.json.gz');new=tmp_path/('b'*64+'.json.gz')
    old.write_bytes(b'not_a_gzip');new.write_bytes(gzip.compress(b'private_news_text_only_size_is_reported'))
    import os
    os.utime(old,ns=(1,1));os.utime(new,ns=(2,2))
    result=c.news_capacity(tmp_path)
    assert result['compressed_files_read']==1 and result['decoded_bytes']==len(b'private_news_text_only_size_is_reported')
    assert 'private_news_text' not in json.dumps(result) and not result['semantic_payload_seal_verified']


@pytest.mark.parametrize('reason',['news_16mib_decode_bound','news_gzip_geometry'])
def test_news_decoder_keeps_original_16mib_cap_and_rejects_extra_members(tmp_path,reason):
    payload=b'x'*(16*1024*1024+1) if reason=='news_16mib_decode_bound' else b'ok'
    raw=gzip.compress(payload)
    if reason=='news_gzip_geometry':raw+=gzip.compress(b'extra')
    (tmp_path/('a'*64+'.json.gz')).write_bytes(raw)
    with pytest.raises(ValueError,match=reason):c.news_capacity(tmp_path)


def test_news_stored_cap_precedes_decode(tmp_path):
    (tmp_path/('a'*64+'.json.gz')).write_bytes(b'x'*(8*1024*1024+1))
    with pytest.raises(ValueError,match='read_bound_or_change'):c.news_capacity(tmp_path)


def test_guarded_component_failure_preserves_explicit_unavailable():
    value=c.guarded(lambda:(_ for _ in ()).throw(ValueError('news_16mib_decode_bound')))
    assert value['status']=='unavailable' and value['reason']=='news_16mib_decode_bound'


def test_collect_rechecks_all_source_identity_after_components(tmp_path,monkeypatch):
    monkeypatch.setattr(c,'DATA',tmp_path);(tmp_path/'logs').mkdir()
    calls=[]
    def binding():
        calls.append(1)
        return {'pilot':{},'paper':{},'risk':{}},{'one':{'registry':{'sha256':'a'},'source_bindings':{}}},{'own':str(len(calls))}
    monkeypatch.setattr(c,'check_bindings',binding)
    ops=SimpleNamespace(read_supervisor_observation=lambda p:{'workers':{},'expected_workers':['worker_'+str(i) for i in range(17)]},os_processes=lambda:{'processes':[]})
    monkeypatch.setattr(c,'load_primitives',lambda:ops)
    monkeypatch.setattr(c,'supervisor_observation',lambda unused:{'status':'unavailable','workers':{},'expected_workers':['worker_'+str(i) for i in range(17)]})
    fake=SimpleNamespace(own_source_bindings=lambda:c.OBSERVER_PINS,observe_joint_v3=lambda *a,**kw:{'original_producer_envelope':{}})
    monkeypatch.setitem(sys.modules,'oanda_joint_v3_ledger_status_observer_v1',fake)
    for name in ('heartbeat','market','pilot','paper','risk','news_capacity'):monkeypatch.setattr(c,name,lambda *a:{})
    with pytest.raises(ValueError,match='source_or_registry_changed'):c.collect()


def test_registered_nested_source_path_is_contained_and_supported(tmp_path,monkeypatch):
    monkeypatch.setattr(c,'ROOT',tmp_path)
    name='src/forex_system/research/sequential_portfolio_replay_v1.py'
    path=tmp_path/name;path.parent.mkdir(parents=True);path.write_text('# fixture')
    assert c.registered_source_path(name)==path


def test_paper_uses_its_declared_account_access_contract_not_absent_account_eligible():
    assert 'account_eligible' not in PAPER_FLAGS
    c.check_flags(PAPER_FLAGS,paper=True)
    with pytest.raises(ValueError,match='authority_flags'):c.check_flags(FLAGS,paper=True)
    with pytest.raises(ValueError,match='authority_flags'):c.check_flags({**PAPER_FLAGS,'account_access':True},paper=True)


@pytest.mark.parametrize('name',['../elsewhere.py','src/../elsewhere.py','C:/elsewhere.py','/elsewhere.py','src\\elsewhere.py'])
def test_registered_source_path_refuses_escape_or_ambiguous_separator(name):
    with pytest.raises(ValueError,match='source_path'):c.registered_source_path(name)


def quote_fixture(tmp_path,monkeypatch):
    monkeypatch.setattr(c,'DATA',tmp_path)
    now=time.time(); stamp=lambda t:c.datetime.fromtimestamp(t,c.timezone.utc).isoformat().replace('+00:00','Z')
    value=dict(schema_version=3,producer='practice_007_dedicated_quote_stream',research_only=True,
        generated_utc=stamp(now-1),connection_generation=6,quote_count=1,
        quotes={'GBP_USD':dict(ask=1.3501,bid=1.35,pip=.0001,source='stream',time=stamp(now-2),tradeable=True)},
        coverage=dict(connection_generation=6,current_quote_count=1,current_tradeable_quote_count=1,
            current_non_tradeable_quote_count=0,current_non_tradeable_instruments=[],
            current_tradeability_unknown_count=0,current_tradeability_unknown_instruments=[],
            last_known_quote_count=1,retained_last_known_count=0,retained_last_known_instruments=[],
            retained_quotes_execution_eligible=False,tradeability_contract='oanda_client_price_status_boolean_v1',
            execution_requires_independent_freshness_check=True))
    path=tmp_path/'state/practice_007_market_quotes_v1.json';save(path,value)
    ops=c.load_primitives()
    return path,value,ops


def test_actual_shaped_generation_coverage_is_current_without_nonexistent_membership_list(tmp_path,monkeypatch):
    path,value,ops=quote_fixture(tmp_path,monkeypatch)
    result=c.market(ops)
    assert result['connection_generation']==6 and result['current_quote_count']==1
    assert result['rows'][0]['current_generation'] and result['rows'][0]['current_within60s']
    assert 'bid' not in json.dumps(result) and 'ask' not in json.dumps(result)


@pytest.mark.parametrize('field,value,reason',[('connection_generation',5,'quote_coverage_generation_mismatch'),('current_quote_count',2,'quote_coverage_count_mismatch')])
def test_market_coverage_generation_and_counts_must_match_actual_inventory(tmp_path,monkeypatch,field,value,reason):
    path,body,ops=quote_fixture(tmp_path,monkeypatch);body['coverage'][field]=value;save(path,body)
    with pytest.raises(ValueError,match=reason):c.market(ops)


def test_retained_prior_generation_quote_is_not_current_even_when_fresh(tmp_path,monkeypatch):
    path,body,ops=quote_fixture(tmp_path,monkeypatch)
    body['coverage'].update(retained_last_known_instruments=['GBP_USD'],retained_last_known_count=1,current_quote_count=0,current_tradeable_quote_count=0)
    save(path,body);result=c.market(ops)
    assert result['current_quote_count']==0 and result['rows'][0]['retained_only'] and not result['rows'][0]['current_generation']


def test_supervisor_expected_list_and_worker_mapping_preserve_missing_workers():
    names=['worker_'+str(i) for i in range(17)]
    supervisor=dict(expected_workers=names,workers={names[0]:dict(running=True,pids=[51],supervisor_check_ok=True,explicitly_inactive=False),
        names[1]:dict(running=True,pids=[52],supervisor_check_ok=True,explicitly_inactive=False)})
    result=c.supervised_workers(supervisor,dict(processes=[dict(pid=51)]))
    assert len(result)==17 and sum(v['all_reported_pids_present_in_os'] for v in result.values())==1
    assert result[names[2]]['status']=='not_reported'


@pytest.mark.parametrize('total_cap,candidate_cap,reason',[(2,10,'supervisor_log_metadata_bound'),(10,1,'supervisor_log_candidate_bound')])
def test_supervisor_inventory_bound_is_unavailable_section_and_does_not_read_content(tmp_path,monkeypatch,total_cap,candidate_cap,reason):
    monkeypatch.setattr(c,'DATA',tmp_path);root=tmp_path/'logs';root.mkdir()
    for i in range(3):(root/('always_on_supervisor_'+str(i)+'.jsonl')).touch()
    monkeypatch.setattr(c,'LOG_METADATA_CAP',total_cap);monkeypatch.setattr(c,'SUPERVISOR_LOG_CAP',candidate_cap)
    namespace={'EXPECTED_RESEARCH_WORKERS':['worker_'+str(i) for i in range(17)]}
    exec("def read(path):\n raise AssertionError('must_not_read_content')",namespace)
    result=c.supervisor_observation(SimpleNamespace(read_supervisor_observation=namespace['read']))
    assert result['status']=='unavailable' and result['reasons']==[reason] and len(result['expected_workers'])==17


def test_unrelated_logs_do_not_use_supervisor_candidate_budget(tmp_path,monkeypatch):
    monkeypatch.setattr(c,'DATA',tmp_path);root=tmp_path/'logs';root.mkdir()
    for i in range(3):(root/('other_'+str(i)+'.log')).touch()
    (root/'always_on_supervisor_1.jsonl').touch();monkeypatch.setattr(c,'SUPERVISOR_LOG_CAP',1)
    namespace={'EXPECTED_RESEARCH_WORKERS':['worker_'+str(i) for i in range(17)]}
    exec("def read(path):\n return {'status':'current','expected_workers':EXPECTED_RESEARCH_WORKERS,'workers':{}}",namespace)
    result=c.supervisor_observation(SimpleNamespace(read_supervisor_observation=namespace['read']))
    assert result['status']=='current' and result['metadata_inventory']['matching_supervisor_candidates']==1
