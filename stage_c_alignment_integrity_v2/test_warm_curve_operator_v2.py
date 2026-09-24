import sys,json,copy
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT));sys.path.insert(0,str(ROOT.parent/'trad'))
import warm_curve_runner_v2 as runner
import warm_curve_operator_v2 as op
from warm_curve_predict_v2 import ReadyWarmPredictor
from curve_capacity_predict_v2 import SharedCurvePredictor
def task(key='a',ready=90):return {'fit_id':key,'joint_ready_epoch':ready}
def fake_load(self,t,obs,views):self.cache[t['fit_id']]=('metadata','models');self.validated.add(t['fit_id']);return {}
def test_prewarm_never_reads_not_ready_weights(monkeypatch):
    p=ReadyWarmPredictor(None)
    monkeypatch.setattr(SharedCurvePredictor,'predict',lambda *a:(_ for _ in ()).throw(AssertionError('future_weight_read')))
    assert p.prewarm([task(ready=101)],100,220)['tasks'][0]['status']=='not_ready_at_prewarm_start'
    assert not p.cache
def test_cache_reconstruction_preserves_availability_and_resets_numeric_state(monkeypatch):
    monkeypatch.setattr(SharedCurvePredictor,'predict',fake_load)
    def reconstruct():
        p=ReadyWarmPredictor(None);p.prewarm([task()],100,220);p.begin_origin(220)
        p.values={'old':1};p.frames={'old':2};p.transforms={'old':3}
        p.prewarm([task()],300,420);p.begin_origin(420)
        assert not p.values and not p.frames and not p.transforms
        return p.cache_available
    assert reconstruct()==reconstruct()=={'a':220}
def test_prewarm_clock_and_failure_quarantine(monkeypatch):
    p=ReadyWarmPredictor(None);monkeypatch.setattr(SharedCurvePredictor,'predict',fake_load)
    p.prewarm([task()],100,220)
    with pytest.raises(ValueError,match='monotone'):p.begin_origin(219)
    import warm_curve_predict_v2 as module
    clock=iter([0.,121.]);monkeypatch.setattr(module.time,'monotonic',lambda:next(clock))
    with pytest.raises(ValueError,match='reservation_exceeded'):p.prewarm([task()],300,420)
    with pytest.raises(ValueError,match='failed_prewarm'):p.begin_origin(500)
def timing():return {'origin_epoch':1,'prewarm_elapsed_seconds':1.,'engines':{e:{'elapsed_seconds':1.,'live_ready':False,'diagnostic_target_met':True} for e in runner.ENGINES}}
@pytest.mark.parametrize('value',[True,float('nan'),float('inf'),-1,31])
def test_timing_bounds_refuse_invalid_values(value):
    t=timing();t['engines']['warm']['elapsed_seconds']=value
    with pytest.raises(ValueError,match='warm_origin_resource_limit'):runner.validate_timing(t,1)
def test_scientific_inventory_excludes_only_measured_payloads():
    assert len(runner.required())==66 and len(runner.scientific_names())==44
    assert set(runner.required())-set(runner.scientific_names())=={'startup_resources.json','resource_receipts.json'}|{f'timing_{t}.json' for t in runner.ORIGINS}
def test_operator_never_turns_pin_failure_into_fresh_run(tmp_path):
    p=tmp_path/'recipe.json';p.write_text('{}')
    r=op.operate('resume',p,'0'*64,{},tmp_path,tmp_path/'runs')
    assert r['status']=='review_required' and r['reason']=='warm_recipe_pin_mismatch' and not (tmp_path/'runs').exists()

def test_final_total_guard_rejects_last_origin_overrun(monkeypatch,tmp_path):
    monkeypatch.setattr(runner.time,'monotonic',lambda:902.)
    with pytest.raises(ValueError,match='warm_total_resource_limit'):
        runner.check_resources(tmp_path,0,{'total_hard_seconds':900},'final_precommit')

def test_disk_guard_rejects_receipt_space_exhaustion(tmp_path):
    with pytest.raises(ValueError,match='warm_disk_resource_limit'):
        runner.check_resources(tmp_path,runner.time.monotonic(),{'total_hard_seconds':900,'max_rss_bytes':10**12,'max_run_disk_bytes':1},'final_precommit')

def test_rss_guard_rejects_over_limit(tmp_path):
    with pytest.raises(ValueError,match='warm_rss_resource_limit'):
        runner.check_resources(tmp_path,runner.time.monotonic(),{'total_hard_seconds':900,'max_rss_bytes':1,'max_run_disk_bytes':2*1024**3},'after_prepare')

def test_cached_legacy_eligibility_checked_before_reuse():
    from scoped_warm_predict_v2 import ScopedSharedCurvePredictor
    from contracts import fingerprint
    p=ScopedSharedCurvePredictor(None);p.begin_origin(100)
    obs=[{'origin_epoch':100,'record_id':'r','features':[1.],'available_epoch':99}]
    p.values['a']=(fingerprint([{'observation':obs[0],'view':None}]),{'cached':1.})
    with pytest.raises(ValueError,match='shared_assessment_support_drift'):
        p.predict({'fit_id':'a','group':'legacy26','joint_ready_epoch':90},obs,{'r':{'full228_cost2':{'shared_legacy_population_eligible':False}}})
