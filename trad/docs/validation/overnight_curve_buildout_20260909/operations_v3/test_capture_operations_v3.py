from copy import deepcopy
from types import SimpleNamespace
import pytest
import capture_operations_v3 as v


def fixture():
    return {'expected_workers':sorted(v.EXPECTED),'workers':{name:{'running':True,'pids':[i+1],
        'supervisor_check_ok':True,'explicitly_inactive':False} for i,name in enumerate(sorted(v.EXPECTED))}}


def test_exact15_os_presence_and_missing_remain_distinct():
    value=fixture();result=v.supervised_workers(value,{'processes':[{'pid':i+1} for i in range(15)]})
    assert len(result)==15 and all(row['all_reported_pids_present_in_os'] for row in result.values())
    del value['workers']['joint_price_news_study_v3']
    assert v.supervised_workers(value,{'processes':[]})['joint_price_news_study_v3']['status']=='not_reported'


@pytest.mark.parametrize('change',['old17','duplicate','wrong15'])
def test_unregistered_expected_population_rejected(change):
    value=fixture()
    if change=='old17':value['expected_workers']+=sorted(v.RETIRED)
    elif change=='duplicate':value['expected_workers'][0]=value['expected_workers'][1]
    else:value['expected_workers'][0]='other_worker'
    with pytest.raises(ValueError,match='main15_expected_inventory'):v.supervised_workers(value,{'processes':[]})


def test_wrapper_retains_actual_inactive_rows_and_restores_accepted_module():
    observed=fixture();observed['workers'].update({name:{'running':False,'pids':[],'explicitly_inactive':True} for name in v.RETIRED})
    original=lambda _:deepcopy(observed);workers=lambda *_:None
    base=SimpleNamespace(HEALTH_SHA='old',supervised_workers=workers,supervisor_observation=original)
    def collect():
        assert base.HEALTH_SHA==v.HEALTH_SHA
        got=base.supervisor_observation(None)
        return {'schema_version':'old','main17_workers':base.supervised_workers(got,{'processes':[]}),
            'main17_os_present_count':0,'old_producer_failures':['retained'],'news_limit':16*1024*1024}
    base.collect=collect;result=v.collect_with_base(base)
    assert result['main15_os_present_count']==0 and 'main17_workers' not in result
    assert all(row['explicitly_inactive'] is True for row in result['retired_joint_workers'].values())
    assert result['old_producer_failures']==['retained'] and result['news_limit']==16*1024*1024
    assert (base.HEALTH_SHA,base.supervised_workers,base.supervisor_observation)==('old',workers,original)


def test_failure_also_restores_private_module():
    function=lambda *_:None
    def fail():raise ValueError('retained_failure')
    base=SimpleNamespace(HEALTH_SHA='old',supervised_workers=function,supervisor_observation=function,collect=fail)
    with pytest.raises(ValueError,match='retained_failure'):v.collect_with_base(base)
    assert (base.HEALTH_SHA,base.supervised_workers,base.supervisor_observation)==('old',function,function)


def test_accepted_v2_bytes_and_limits_stay_exact():
    base=v.load_base()
    assert base.HEALTH_SHA=='d9498a2b42ff9cde525fb9e4cc9f14e5729e8bca5f512003b69f000fbefe3eda'
    assert base.PILOT_CYCLE_CAP==600 and base.NEWS_DECODED_CAP==16*1024*1024
    assert len(base.REGISTRIES)==4 and base.LOG_METADATA_CAP==32768


def test_external_output_rejects_existing_or_outside_before_write(tmp_path):
    with pytest.raises(ValueError,match='fresh_external_output_required'):
        v.write_output(tmp_path/'outside.json',{},SimpleNamespace(safe=lambda _:None))
    assert not (tmp_path/'outside.json').exists()
