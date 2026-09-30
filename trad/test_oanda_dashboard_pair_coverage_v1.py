import hashlib
import json
from pathlib import Path
import shutil
import subprocess

import pytest
import oanda_operational_dashboard_selection_v1 as d
from test_oanda_operational_dashboard_selection_v1 import NOW, fixture


def test_stale_account_does_not_block_current_nontrading_collection(tmp_path):
    root,_,_,_=fixture(tmp_path)
    joint=d.read_dashboard_sources(root,now_epoch=NOW)['joint']
    legacy={'observations':{'quote_stream':{'status':'current','current':True},
                            'account':{'status':'stale','current':False}}}
    result=d.project_collection_status(legacy,joint,now_epoch=NOW)
    assert result['running'] and result['status']=='running'
    assert result['account_status']=='stale' and not result['account_required_for_research']
    assert result['research_blockers']==[] and result['unavailable_observations']==['account:stale']
    assert not result['can_place_orders']
    legacy['observations']['quote_stream']={'status':'stale','current':False}
    result=d.project_collection_status(legacy,joint,now_epoch=NOW)
    assert not result['running'] and result['research_blockers']==['quote_stream:stale']


def technical_fixture(tmp_path):
    root=tmp_path/'trad';root.mkdir()
    def write(name,raw):
        p=root/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(raw)
        return {'path':str(p),'sha256':hashlib.sha256(raw).hexdigest()}
    source=write('oanda_all68_technical_availability_v2.py',b'# fixed source')
    dependency=write('dependency.py',b'# fixed dependency')
    obs=write('config/obs.json',b'{}');ops=write('config/ops.json',b'{}')
    pairs={pair:{'instrument':pair,'quote':{'status':'current','quote_epoch':NOW-5},
                'features':{'status':'partial','bar_end_epoch':NOW-30,'finite_count':12,
                    'expected_count':216,'missing_counts_by_reason':{'missing_elapsed_support':204},
                    'consecutive_suffix_minutes':1,'maximum_required_support_minutes':603,
                    'peer':{'status':'partial'}}} for pair in ['EUR_USD','USD_HKD']}
    report={'schema_version':'all68_technical_availability_v2_20260930','status':'readable',
            'generated_epoch':NOW-1,'source_errors':{},'original_receipt_errors':{},
            'research_only':True,**{k:False for k in d.FLAGS[:3]},
            'source_references':{'identity_binding':{'report_source_sha256':source['sha256'],
                'imported_sources':{'dependency.py':dependency['sha256']},'operations_config':ops},
                'observation_config':obs},'configured_pair_count':2,'reported_pair_count':2,
            'pairs':pairs,'quote_age_reporting_bound_seconds':180,'bar_age_bound_seconds':600}
    path=root/'data/oanda_training_manager/state/all68_technical_availability_v2.json'
    path.parent.mkdir(parents=True)
    def save():path.write_text(json.dumps(report))
    save()
    return root,report,save


def test_coverage_preserves_missing_values_and_ages_each_input(tmp_path):
    root,report,save=technical_fixture(tmp_path)
    report['pairs']['USD_HKD']['quote']['quote_epoch']=NOW-181
    report['pairs']['USD_HKD']['features']['bar_end_epoch']=NOW-601
    save();value=d.read_technical_coverage(root,set(report['pairs']),NOW)
    assert value['status']=='current' and value['pair_count']==2
    assert value['feature_counts']=={'partial':1,'stale':1}
    assert value['quote_counts']=={'current':1,'stale':1}
    assert value['rows'][0]['finite_features']==12
    assert value['rows'][0]['missing_reasons']=={'missing_elapsed_support':204}


@pytest.mark.parametrize('defect',['stale','future','missing_pair','extra_pair','source','dependency','config','receipt','authority'])
def test_unverified_technical_coverage_is_unknown_not_zero_or_complete(tmp_path,defect):
    root,report,save=technical_fixture(tmp_path);expected=set(report['pairs'])
    if defect=='stale':report['generated_epoch']=NOW-181
    if defect=='future':report['generated_epoch']=NOW+1
    if defect=='missing_pair':report['pairs'].pop('USD_HKD')
    if defect=='extra_pair':report['pairs']['GBP_USD']=report['pairs']['EUR_USD']
    if defect=='source':(root/'oanda_all68_technical_availability_v2.py').write_bytes(b'changed')
    if defect=='dependency':(root/'dependency.py').write_bytes(b'changed')
    if defect=='config':(root/'config/obs.json').write_bytes(b'changed')
    if defect=='receipt':report['original_receipt_errors']={'USD_HKD':'unverified'}
    if defect=='authority':report['can_place_orders']=True
    save();value=d.read_technical_coverage(root,expected,NOW)
    assert value['status']=='unavailable' and value['rows']==[] and value['pair_count'] is None


def test_browser_coverage_keeps_all_pairs_separate_families_and_account(tmp_path):
    node=shutil.which('node')
    if not node:pytest.skip('Node unavailable')
    html=(Path(__file__).parent/'oanda_main_signal_dashboard.html').read_text(encoding='utf-8')
    functions=html.split('// BEGIN current pair coverage display.')[1].split('// END current pair coverage display.')[0]
    script=r'''
const assert=require('assert'); const esc=v=>String(v).replaceAll('<','&lt;'); const now=Date.now()/1000;
const obs={status:'current',current:true,generated_epoch:now-1,max_age_sec:90};
const data={collection_status:{observations:{study:obs,quote_stream:obs,account:{status:'stale',current:false}}},operational_dashboard:{pair_coverage:{status:'current',generated_epoch:now-1,observed_epoch:now,rows:[],feature_counts:{partial:68}}}};
const rows=Array.from({length:68},(_,i)=>({instrument:'PAIR_'+i,active_forecasts:[{family:'probabilistic_state_space',publication_epoch:now-10,reference_epoch:now-100,target_epoch:now+3500}],families:{probabilistic_state_space:{status:'forecast'},ridge_return_repaired:{status:'ready',reason:'cached_input_expired_before_issue'},ridge_price_news_v1:{status:'warming',current_readiness:{diagnostics:{mature_exact_h1_training_rows:0,required_joint_training_rows:48,nonzero_news_context_training_rows:0,distinct_news_context_patterns:0}}}}}));
data.pair_local_forecasts={status:'current',generated_epoch:now-1,rows};data.joint_price_news_forecasts={status:'current',generated_epoch:now-1,rows};
let rendered=pairCoverageHtml(data);assert(rendered.includes('68 configured pairs'));assert.equal((rendered.match(/<tr>/g)||[]).length,69);assert(rendered.includes('cached_input_expired_before_issue'));assert(rendered.includes('Mature rows 0/48'));assert(rendered.includes('Availability report unavailable'));
assert(researchHealthHtml(data).includes('Research pipeline: collecting'));assert(researchHealthHtml(data).includes('Account snapshot stale'));
rows[0].active_forecasts[0].target_epoch=now-1;rendered=pairCoverageHtml(data);assert(rendered.includes('No unexpired forecast'));
data.pair_local_forecasts.generated_epoch=now-100;data.joint_price_news_forecasts.generated_epoch=now-100;rendered=pairCoverageHtml(data);assert(!rendered.includes('Forecast available'));
obs.generated_epoch=now-100;assert(!researchHealthHtml(data).includes('Research pipeline: collecting'));
'''
    result=subprocess.run([node,'-e',functions+script],capture_output=True,text=True,timeout=20)
    assert result.returncode==0,result.stderr
