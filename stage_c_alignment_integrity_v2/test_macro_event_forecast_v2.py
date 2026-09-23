import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import numpy as np
import pytest
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import macro_event_forecast_v2 as core
import macro_event_forecast_operator_v2 as op
from contracts import TrainingView
from publication import RunPublisher
INPUTS=Path(os.environ.get('FOREX_MACRO_EVENT_FORECAST_INPUTS',str(ROOT/'evidence/timed_20260922_154551/macro_event_forecast/inputs')))
RECIPE=ROOT/'MACRO_EVENT_FORECAST_OPERATOR_RECIPE.json'


@pytest.fixture(scope='module')
def completed(tmp_path_factory):
    supplied=os.environ.get('FOREX_MACRO_EVENT_FORECAST_RUNS');runs=Path(supplied) if supplied else tmp_path_factory.mktemp('eventforecast')
    r=op.operate('verify' if supplied else 'run',RECIPE,op.sha(RECIPE),INPUTS,runs)
    assert r['status']=='completed_verified',r
    return Path(r['run_path'])


def plan():return op.read(INPUTS/'EVENT_FORECAST_PLAN.json')


def test_all_prespecified_arms_and_negative_result_retained(completed):
    r=op.read(completed/'forecast_report.json');attempts=op.read(completed/'attempts.json')
    assert r['base_models_fitted']==r['declared_fit_attempts']==len(attempts)==6
    assert r['feature_counts']=={'technical':26,'technical_clock':30,'technical_clock_event':42}
    assert r['observation_rows_per_group']==6528 and r['coverage_rows']==32640 and r['forecasts']==23930
    assert [d['matched_rows'] for d in r['differences']]==[2280,832]
    assert all(d['event_minus_clock_mae_bps']>0 for d in r['differences'])
    assert not r['forecast_improvement_proven'] and not r['historical_live_admission']
    assert all(a['status']=='fitted' for a in attempts)


def test_exact_matched_training_and_scoring_support(completed):
    attempts=op.read(completed/'attempts.json');scores=op.read(completed/'scores.json')
    for h in (60,1440):
        a=[x for x in attempts if x['horizon_minutes']==h]
        assert len({x['shared_training_support_sha256'] for x in a})==1
        assert len({x['shared_training_rows'] for x in a})==1
        s=[x for x in scores if x['target_id'].endswith(f'_{h}m')]
        assert len(s)==5 and len({x['support_sha256'] for x in s})==1


def test_train_only_normalization_and_mature_labels(completed):
    models=op.read(completed/'models.json');observations=op.read(completed/'observations.json');outcomes=op.read(completed/'outcomes.json');p=plan()
    start,cutoff,end=(int(core.epoch(p[k])) for k in ('training_start','fit_cutoff','evaluation_end_exclusive'))
    view=TrainingView(start,cutoff,cutoff,cutoff,end)
    for name,m in models.items():
        group=name.rsplit('_',1)[0];eligible=core.support(observations[group],outcomes,view,m['target']);ids={x['record_id'] for x in eligible}
        x=np.array([r['features'] for r in observations[group] if r['record_id'] in ids])
        assert np.allclose(x.mean(axis=0),m['mean'],rtol=1e-12,atol=1e-10)
        assert m['maximum_outcome_available_epoch']<=cutoff and m['training_rows']==len(ids)
        assert m['feature_count']==len(core.schemas()[group])


def test_future_features_and_unmatured_outcomes_do_not_change_any_fit(completed):
    observations=op.read(completed/'observations.json');outcomes=op.read(completed/'outcomes.json');p=plan();cutoff=int(core.epoch(p['fit_cutoff']))
    for rows in observations.values():
        for r in rows:
            if r['origin_epoch']>=cutoff:r['features']=['future_non_numeric_not_to_be_read']
    for o in outcomes:
        if o['available_epoch']>cutoff:o['value']='future_non_numeric_not_to_be_read'
    models,attempts=core.fit_groups(observations,outcomes,op.read(INPUTS/'universe.json'),p)
    assert models==op.read(completed/'models.json') and attempts==op.read(completed/'attempts.json')


def test_forged_early_future_label_clock_fails_closed(completed):
    obs=op.read(completed/'observations.json');out=op.read(completed/'outcomes.json');p=plan();cut=int(core.epoch(p['fit_cutoff']))
    row=next(o for o in out if o['label_end_epoch']>cut and int(o['record_id'].rsplit(':',1)[1])<cut)
    row['available_epoch']=cut
    with pytest.raises(ValueError,match='outcome_available_before_target'):core.fit_groups(obs,out,op.read(INPUTS/'universe.json'),p)


def test_forecasts_do_not_require_a_future_endpoint(completed):
    forecasts=op.read(completed/'forecasts.json');out=op.read(completed/'outcomes.json');lookup={(o['record_id'],o['target_id']):o for o in out}
    assert any(lookup[r['record_id'],r['forecast']['target_id']]['value'] is None for r in forecasts)
    assert all(not r['historical_live_admission'] for r in forecasts)
    assert all(not {'actual','outcome','outcome_value'}&set(r['forecast']) for r in forecasts)


def test_original_issuance_reproduces_without_reading_outcomes(completed):
    f,c=core.predict(op.read(completed/'models.json'),op.read(completed/'observations.json'),plan())
    assert f==op.read(completed/'forecasts.json') and c==op.read(completed/'coverage.json')
    assert sum(r['reason']=='model_not_ready' for r in c)==680


def test_raw_future_bars_and_future_source_version_leave_prefix_unchanged():
    p=plan();capture=op.read(INPUTS/'CANDLE_CAPTURE.json');d=next(x for x in capture['pairs'] if x['pair']=='EUR_USD')
    frame=core.decode_candles((INPUTS/'EUR_USD.csv.gz').read_bytes(),d,'EUR_USD',op.read(INPUTS/'MATCHED_POPULATION_PLAN.json'))
    bindings=op.read(INPUTS/'version_bindings.json');cache={c['cache_key']:c for c in op.read(INPUTS/'extraction_cache.json')}
    before=core.prepare({'EUR_USD':frame},['EUR_USD'],bindings,cache,p)
    cutoff=int(core.epoch(p['fit_cutoff']));changed=frame.copy();changed.loc[changed.epoch>=cutoff,['mid','bid','ask']]*=3
    later=copy.deepcopy(bindings[0]);later['version_id']='future-injected';later['event_id']='future-injected'
    for o in later['observations']:o.update(known_epoch=cutoff+10*86400,available_epoch=cutoff+10*86400,published_epoch=cutoff+10*86400)
    after=core.prepare({'EUR_USD':changed},['EUR_USD'],bindings+[later],cache,p)
    for g in core.GROUPS:
        assert [r for r in before[0][g] if r['origin_epoch']<=cutoff]==[r for r in after[0][g] if r['origin_epoch']<=cutoff]
    assert [o for o in before[1] if o['available_epoch']<=cutoff]==[o for o in after[1] if o['available_epoch']<=cutoff]
    assert before[2]==after[2]


def test_context_delay_and_missing_age_mask(completed):
    refs=op.read(completed/'context_references.json')
    assert all(r['source_asof_epoch']==r['origin_epoch']-30 for r in refs)
    empty={'visible_event_count':0,'eligible_event_count':0,'unique_text_count':0,'source_count':0,'minimum_source_age_seconds':None}
    assert core.event_values(empty)==[0,0,0,0,0,1]
    assert core.event_values({**empty,'minimum_source_age_seconds':0})==[0,0,0,0,0,0]
    assert core.event_values({**empty,'minimum_source_age_seconds':999999999})[-2]==840


def test_prefix_projection_shared_finite_technical_population(completed):
    obs=op.read(completed/'observations.json')
    for a,b,c in zip(obs['technical'],obs['technical_clock'],obs['technical_clock_event']):
        assert a['record_id']==b['record_id']==c['record_id']
        if a['features'] is None:assert b['features'] is None and c['features'] is None
        else:
            assert a['features']==b['features'][:26]==c['features'][:26]
            assert b['features']==c['features'][:30] and np.isfinite(c['features']).all()


def test_all68_coverage_even_after_price_support_ends(completed):
    coverage=op.read(completed/'coverage.json');universe=op.read(INPUTS/'universe.json')
    assert sorted({r['pair'] for r in coverage})==universe
    assert len(coverage)==68*48*2*5
    assert any(r['reason']=='missing_features' for r in coverage)
    scores=op.read(completed/'scores.json');assert len(scores)==10 and all(not s['independent_confirmation'] for s in scores)


def test_external_pin_and_live_owner_guard(tmp_path):
    assert op.operate('run',RECIPE,'0'*64,INPUTS,tmp_path/'bad')['status']=='review_required'
    assert not (tmp_path/'bad').exists()
    r=op.read(RECIPE);owner=RunPublisher(tmp_path/'owned',r['run_id'],op.identity_for(r));owner.acquire()
    try:assert op.operate('resume',RECIPE,op.sha(RECIPE),INPUTS,tmp_path/'owned')['status']=='review_required'
    finally:owner.release()


@pytest.mark.parametrize('boundary',[1,7])
def test_process_death_resume_preserves_models_and_predictions(tmp_path,completed,boundary):
    p=subprocess.run([sys.executable,'-I','-B',str(ROOT/'macro_event_forecast_operator_v2.py'),'run','--recipe',str(RECIPE),'--recipe-sha256',op.sha(RECIPE),'--inputs',str(INPUTS),'--runs-dir',str(tmp_path),'--test-crash-after',str(boundary)],capture_output=True,text=True,timeout=180)
    assert p.returncode==91,(p.stdout,p.stderr)
    root=tmp_path/op.read(RECIPE)['run_id'];names=op.read(RECIPE)['required_payloads'];before={n:op.sha(root/n) for n in names if (root/n).exists()}
    assert not (root/'COMPLETION_MANIFEST.json').exists()
    r=op.operate('resume',RECIPE,op.sha(RECIPE),INPUTS,tmp_path);assert r['status']=='completed_verified',r
    assert all(op.sha(root/n)==h for n,h in before.items())
    assert all(op.sha(root/n)==op.sha(completed/n) for n in names)
