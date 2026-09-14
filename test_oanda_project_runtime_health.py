from copy import deepcopy
from datetime import datetime,timezone
import json

import pytest

from trad import oanda_project_runtime_health as health

NOW = 1788816000.0


def stamp(epoch):
    return datetime.fromtimestamp(epoch,timezone.utc).isoformat()


def fixture():
    start={'event':'supervisor_started','time':stamp(NOW-600),
           'research_collection_only':True,'research_collection_names':sorted(health.EXPECTED_RESEARCH_WORKERS)}
    rows=[{'name':name,'running':True,'pids':[100+idx*2,101+idx*2],
           'freshness':{'fresh':True,'reason':'fresh','age_sec':1.0}} for idx,name in enumerate(sorted(health.EXPECTED_RESEARCH_WORKERS))]
    rows[next(i for i,row in enumerate(rows) if row['name']=='live_dashboard')]['freshness']={'fresh':True,'reason':'not_checked','age_sec':None}
    inactive=set(n for names in health.CHECK_WORKERS.values() for n in names)-health.EXPECTED_RESEARCH_WORKERS
    rows += [{'name':name,'running':False,'pids':[],'freshness':{'fresh':True,'reason':'research_collection_only'}} for name in sorted(inactive)]
    hb={'event':'heartbeat','time':stamp(NOW-5),'managed':rows}
    return start,hb


def observe(tmp_path,start=None,hb=None,extra=b'',clock=NOW):
    defaults=fixture()
    start=defaults[0] if start is None else start
    hb=defaults[1] if hb is None else hb
    path=tmp_path/'always_on_supervisor_20260907_172000.jsonl'
    path.write_bytes((json.dumps(start)+'\n'+json.dumps(hb)+'\n').encode()+extra)
    before=path.read_bytes(),path.stat().st_mtime_ns
    result=health.read_supervisor_observation(tmp_path,clock=lambda:clock)
    assert (path.read_bytes(),path.stat().st_mtime_ns)==before
    return result


def test_current15_workers_and_unchecked_dashboard_are_explicit(tmp_path):
    result=observe(tmp_path)
    assert result['status']=='current'
    assert result['running_worker_count']==result['expected_worker_count']==15
    assert result['output_unchecked_workers']==['live_dashboard']
    assert result['supervisor_age_sec']==5
    assert len(result['source']['tail_sha256'])==64


@pytest.mark.parametrize('mode',['inactive','running','missing'])
def test_retired_news_watchlist_failure_keeps_its_actual_scope(tmp_path,mode):
    start,hb=fixture()
    row=next(r for r in hb['managed'] if r['name']=='news_technical_watchlist')
    if mode=='running':row.update(running=True,pids=[9998],freshness={'fresh':True,'reason':'fresh','age_sec':0})
    if mode=='missing':hb['managed'].remove(row)
    result=observe(tmp_path,start,hb)
    name='news_watchlist_uses_current_classification_contract'
    scoped=health.scope_integrity_checks({name:False},result)
    assert scoped['check_scopes'][name]['artifact_check_passed'] is False
    assert (name in scoped['inactive_component_failures']) == (mode=='inactive')
    assert (name in scoped['active_shared_or_unknown_failures']) == (mode!='inactive')


@pytest.mark.parametrize('age',[91,-1])
def test_stale_or_future_supervision_leaves_scope_unknown(tmp_path,age):
    start,hb=fixture();hb['time']=stamp(NOW-age)
    result=observe(tmp_path,start,hb)
    assert result['status']=='unavailable'
    scoped=health.scope_integrity_checks({'executor_healthy':True,'independent_verifier_matches':False},result)
    assert scoped['inactive_component_failures']==[]
    assert scoped['active_shared_or_unknown_failures']==['independent_verifier_matches']
    assert scoped['live_runtime_assertions']['executor_healthy']['currently_running'] is None


@pytest.mark.parametrize('change',['missing','not_running','fresh_false','string_running','duplicate_name','shared_pid','invalid_age'])
def test_active_worker_regressions_are_not_treated_as_retirement(tmp_path,change):
    start,hb=fixture();rows=hb['managed'];target=next(r for r in rows if r['name']=='official_release_fast_lane')
    if change=='missing': rows.remove(target)
    elif change=='not_running': target.update(running=False,pids=[],freshness={'fresh':True,'reason':'disabled'})
    elif change=='fresh_false': target['freshness']['fresh']=False
    elif change=='string_running':target['running']='true'
    elif change=='duplicate_name':rows.append(deepcopy(target))
    elif change=='shared_pid':target['pids']=[rows[0]['pids'][0]]
    elif change=='invalid_age':target['freshness']['age_sec']=float('nan')
    result=observe(tmp_path,start,hb)
    assert result['status'] in ('degraded','unavailable')
    scoped=health.scope_integrity_checks({'official_release_fast_lane_current_and_inert':False},result)
    assert scoped['inactive_component_failures']==[]
    assert scoped['active_shared_or_unknown_failures']==['official_release_fast_lane_current_and_inert']


def test_unknown_active_order_worker_degrades_without_hiding_it(tmp_path):
    start,hb=fixture();hb['managed'].append({'name':'unknown_order_executor','running':True,'pids':[9999],'freshness':{'fresh':True,'reason':'fresh','age_sec':0}})
    result=observe(tmp_path,start,hb)
    assert result['status']=='degraded'
    assert 'unexpected_active_worker:unknown_order_executor' in result['reasons']


@pytest.mark.parametrize('value',[False,'true',None])
def test_research_mode_requires_boolean_true(tmp_path,value):
    start,hb=fixture();start['research_collection_only']=value
    assert observe(tmp_path,start,hb)['status']=='unavailable'


def test_partial_append_keeps_last_complete_observation(tmp_path):
    result=observe(tmp_path,extra=b'{"event":"heartbeat","time":')
    assert result['status']=='current'
    assert result['generated_epoch']==NOW-5


def test_malformed_complete_record_fails_closed(tmp_path):
    assert observe(tmp_path,extra=b'{broken}\n')['status']=='unavailable'


def test_newest_bad_supervisor_is_not_replaced_with_older_good_log(tmp_path):
    observe(tmp_path)
    (tmp_path/'always_on_supervisor_20260907_173000.jsonl').write_text('{broken}\n')
    assert health.read_supervisor_observation(tmp_path,clock=lambda:NOW)['status']=='unavailable'


def test_supervisor_error_after_heartbeat_is_retained(tmp_path):
    extra=(json.dumps({'event':'supervisor_error','time':stamp(NOW-1),'error':'test failure'})+'\n').encode()
    result=observe(tmp_path,extra=extra)
    assert result['status']=='degraded'
    assert result['later_supervisor_errors']==[{'time':stamp(NOW-1),'error':'test failure'}]


def test_all16_legacy_failures_remain_false_and_scoped_without_changing_input(tmp_path):
    checks=dict.fromkeys(list(health.CHECK_WORKERS)[:16],False)
    checks.update(executor_healthy=True,four_frozen_proof_families_running=True,unknown_safety_check=False)
    original=deepcopy(checks)
    scoped=health.scope_integrity_checks(checks,observe(tmp_path))
    assert checks==original
    assert len(scoped['inactive_component_failures'])==16
    assert scoped['active_shared_or_unknown_failures']==['unknown_safety_check']
    assert all(not scoped['check_scopes'][name]['artifact_check_passed'] for name in scoped['inactive_component_failures'])
    assert scoped['live_runtime_assertions']['executor_healthy']=={'retained_artifact_claim':True,'currently_running':False,'current_supervisor_check_ok':False,'scope':'inactive_legacy_component'}


def test_stopped_worker_without_explicit_policy_is_not_assumed_retired(tmp_path):
    start,hb=fixture()
    next(r for r in hb['managed'] if r['name']=='continuous_narrative_meter')['freshness']['reason']='crashed'
    result=observe(tmp_path,start,hb)
    scoped=health.scope_integrity_checks({'continuous_narrative_meter_sealed_current_and_inert':False},result)
    assert scoped['inactive_component_failures']==[]
    assert scoped['active_shared_or_unknown_failures']==['continuous_narrative_meter_sealed_current_and_inert']


def test_tail_reader_is_bounded_and_uses_recent_complete_heartbeat(tmp_path):
    start,hb=fixture()
    path=tmp_path/'always_on_supervisor_20260907_172000.jsonl'
    filler=(json.dumps({'event':'diagnostic','note':'a'*4096})+'\n').encode()
    path.write_bytes((json.dumps(start)+'\n').encode()+filler*800+(json.dumps(hb)+'\n').encode())
    result=health.read_supervisor_observation(tmp_path,clock=lambda:NOW)
    assert result['status']=='current'
    assert result['source']['tail_start_offset_before_partial_line_drop']>0


@pytest.mark.parametrize('clock',[float('nan'),float('inf'),True])
def test_invalid_observation_clock_is_unavailable(tmp_path,clock):
    assert observe(tmp_path,clock=clock)['status']=='unavailable'


def test_missing_supervisor_stays_unknown(tmp_path):
    result=health.read_supervisor_observation(tmp_path,clock=lambda:NOW)
    assert result['status']=='unavailable' and result['workers']=={}


def test_audit_database_reader_cannot_create_missing_evidence(tmp_path):
    import sqlite3
    from trad.oanda_project_integrity_audit import _open_readonly_evidence_database
    path=tmp_path/'missing.sqlite'
    with pytest.raises(sqlite3.OperationalError):
        _open_readonly_evidence_database(path)
    assert not path.exists()


def test_audit_database_reader_rejects_mutation(tmp_path):
    import sqlite3
    from trad.oanda_project_integrity_audit import _open_readonly_evidence_database
    path=tmp_path/'existing.sqlite'
    writer=sqlite3.connect(path)
    writer.execute('CREATE TABLE observations(value INTEGER)')
    writer.execute('INSERT INTO observations VALUES(7)')
    writer.commit();writer.close()
    raw=path.read_bytes()
    reader=_open_readonly_evidence_database(path)
    try:
        assert reader.execute('SELECT value FROM observations').fetchone()==(7,)
        with pytest.raises(sqlite3.OperationalError):
            reader.execute('INSERT INTO observations VALUES(8)')
    finally:
        reader.close()
    assert path.read_bytes()==raw


def operational_fixture():
    start,hb=fixture()
    start.update(operational_profile_schema=health.OPERATIONAL_PROFILE_SCHEMA,
                 operational_profile_path='C:/forex/trad/config/operational_runtime_v1_20260913.json',
                 operational_profile_sha256='a'*64,
                 research_collection_names=sorted(health.EXPECTED_OPERATIONAL_WORKERS))
    hb['managed']=[row for row in hb['managed'] if row['name'] not in health.REPLACED_OPERATIONAL_WORKERS]
    hb['managed'] += [dict(name=name,running=True,pids=[10000+i],
                          freshness=dict(fresh=True,reason='fresh',age_sec=1,reported_failure=False,
                                         reported_status='running',reported_phase='waiting',reported_error=None))
                      for i,name in enumerate(sorted(health.OPERATIONAL_PROFILE_WORKERS))]
    hb['managed'] += [dict(name=name,running=False,pids=[],
                          freshness=dict(fresh=True,reason='disabled'))
                      for name in sorted(health.REPLACED_OPERATIONAL_WORKERS)]
    return start,hb


def test_operational_profile_requires_all_twenty_one_workers_and_exact_binding(tmp_path):
    start,hb=operational_fixture()
    result=observe(tmp_path,start,hb)
    assert result['status']=='current'
    assert result['expected_worker_count']==21
    assert set(result['expected_workers'])==health.EXPECTED_OPERATIONAL_WORKERS
    assert result['operational_profile']['sha256']=='a'*64


@pytest.mark.parametrize('name',sorted(health.OPERATIONAL_PROFILE_WORKERS))
def test_every_new_operational_worker_is_required_even_if_reported_disabled(tmp_path,name):
    start,hb=operational_fixture()
    row=next(row for row in hb['managed'] if row['name']==name)
    row.update(running=False,pids=[],freshness=dict(fresh=True,reason='disabled'))
    result=observe(tmp_path,start,hb)
    assert result['status']=='degraded'
    assert 'required_worker_unhealthy_or_missing:'+name in result['reasons']


@pytest.mark.parametrize('name',sorted(health.REPLACED_OPERATIONAL_WORKERS))
def test_retired_producer_cannot_be_running_under_operational_profile(tmp_path,name):
    start,hb=operational_fixture()
    row=next(row for row in hb['managed'] if row['name']==name)
    row.update(running=True,pids=[99999],freshness=dict(fresh=True,reason='fresh',age_sec=1))
    result=observe(tmp_path,start,hb)
    assert result['status']=='degraded'
    assert 'retired_operational_worker_running:'+name in result['reasons']


@pytest.mark.parametrize('field,value',[
    ('operational_profile_schema','unknown'),('operational_profile_schema',None),
    ('operational_profile_sha256',''),('operational_profile_sha256','xyz'),
    ('operational_profile_path',''),
])
def test_invalid_operational_binding_is_not_reported_as_legacy_success(tmp_path,field,value):
    start,hb=operational_fixture(); start[field]=value
    assert observe(tmp_path,start,hb)['status']=='unavailable'


def test_old_retired_producer_actually_running_is_unexpected(tmp_path):
    start,hb=operational_fixture()
    row=next(row for row in hb['managed'] if row['name']=='research_feature_observations_v1')
    row.update(running=True,pids=[99999],freshness=dict(fresh=True,reason='fresh',age_sec=1))
    result=observe(tmp_path,start,hb)
    assert result['status']=='degraded'
    assert 'unexpected_active_worker:research_feature_observations_v1' in result['reasons']


@pytest.mark.parametrize('failure',[True,None,'false'])
def test_fresh_operational_error_heartbeat_is_not_healthy(tmp_path,failure):
    start,hb=operational_fixture()
    name='revision_news_transport_v4'
    row=next(row for row in hb['managed'] if row['name']==name)
    row['freshness'].update(reported_failure=failure,reported_status='failed',
                            reported_phase='cycle_failed',reported_error='sourceguard fixture failure')
    result=observe(tmp_path,start,hb)
    assert result['status']=='degraded'
    assert result['workers'][name]['freshness']['fresh'] is True
    assert result['workers'][name]['reported_error']=='sourceguard fixture failure'
    assert 'required_worker_unhealthy_or_missing:'+name in result['reasons']
