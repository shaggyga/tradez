import ast
import copy
from dataclasses import replace
from datetime import datetime
from decimal import Decimal, localcontext
import hashlib
import json
import math
from pathlib import Path
import socket
import sqlite3

import numpy as np
import pyarrow.parquet as pq
import pytest

import oanda_recovered_second_curve_v1 as adapter

MODEL = Path(r'D:\ForexRecovery\revamp_20260908T1353Z\artifacts\legacy_model_components\state\second_ridge_models_v1.json')
REAL_INPUT = Path(r'D:\ForexRecovery\revamp_20260908T1353Z\inputs\second_ridge\EUR_USD_S5.parquet')
NOW = 1788940001.0


@pytest.fixture(scope='module')
def model():
    if not MODEL.exists(): pytest.skip('preserved recovered artifact is not present')
    return adapter.load_model(MODEL)


def rows(pip=.0001):
    end = math.floor((NOW-6)/5)*5
    result = []
    for i in range(13):
        mid=1.2+pip*((i%4)**2/10+i/8)
        spread=1.2+(i%3)/10
        epoch=end-(12-i)*5
        result.append({'bar_start_epoch':float(epoch),'available_epoch':float(epoch+5.25),
            'complete':True,'bid_close':mid-spread*pip/2,'ask_close':mid+spread*pip/2,
            'mid_close':mid,'mid_high':mid+pip*(i%3+1)/10,'mid_low':mid-pip/5,
            'spread_pips':spread,'volume':float(i%5+1)})
    return result


def capture(items=None, **kw):
    items=rows() if items is None else items
    if kw.get('scope')=='current_research':kw.setdefault('price_convention','official_midpoint')
    return adapter.capture_s5_rows(items,instrument=kw.pop('instrument','EUR_USD'),
        pip_size=kw.pop('pip_size',.0001),source_sha256=hashlib.sha256(b'synthetic_fixture').hexdigest(),
        clock=kw.pop('clock',lambda:NOW),**kw)


def test_original_feature_formula_varying_prices():
    raw=rows(); got=capture(raw)['features']; p=.0001
    mids=[x['mid_close'] for x in raw]; moves=np.diff(mids)/p
    expected=[(mids[-1]-mids[-2])/p,(mids[-1]-mids[-3])/p,(mids[-1]-mids[-7])/p,
      (mids[-1]-mids[0])/p,(mids[-1]-mids[-2])/p-(mids[-1]-mids[-7])/p/6,
      float(np.std(moves[-6:])),float(np.std(moves)),
      (max(x['mid_high'] for x in raw[-6:])-min(x['mid_low'] for x in raw[-6:]))/p,
      raw[-1]['spread_pips'],raw[-1]['spread_pips']/np.mean([x['spread_pips'] for x in raw[-12:]]),
      sum(x['volume'] for x in raw[-6:]),sum(x['volume'] for x in raw[-6:])/max(sum(x['volume'] for x in raw[-12:])/2,1),
      math.sin(2*math.pi*(raw[-1]['bar_start_epoch']%86400)/86400),
      math.cos(2*math.pi*(raw[-1]['bar_start_epoch']%86400)/86400)]
    assert list(got)==list(adapter.FEATURE_NAMES)
    assert list(got.values())==pytest.approx(expected,abs=1e-12)


def irregular_rows(extra):
    raw=rows()
    # Keep the latest reference fixed; actual gaps occur inside the window.
    for row in raw[:6]:
        row['bar_start_epoch']-=extra
        row['available_epoch']-=extra
    return raw


@pytest.mark.parametrize('extra',[0,5,10])
def test_retained_fit_window_original_span_policy_no_fill(extra):
    raw=irregular_rows(extra)
    value=capture(raw,sampling_policy='retained_fit_window')
    assert value['sampling_metadata']['actual_endpoint_span_sec']==60+extra
    assert value['sampling_metadata']['missing_s5_intervals']==extra/5
    assert value['rows']==raw and value['fabricated_rows']==0
    assert value['sampling_metadata']['feature_temporal_support']['return_60_pips']['elapsed_sec']==60+extra
    assert value['sampling_metadata']['feature_temporal_support']['return_5_pips']['elapsed_sec']==5
    if extra:
        with pytest.raises(ValueError,match='missing_duplicate'):capture(raw)


def test_retained_fit_window_still_rejects_large_gaps_and_duplicates():
    with pytest.raises(ValueError,match='55_to_70'):capture(irregular_rows(15),sampling_policy='retained_fit_window')
    raw=rows();raw[4]['bar_start_epoch']=raw[3]['bar_start_epoch']
    with pytest.raises(ValueError,match='duplicate'):capture(raw,sampling_policy='retained_fit_window')


def test_sampling_and_price_policy_are_part_of_replay_identity():
    value=capture(irregular_rows(10),sampling_policy='retained_fit_window',price_convention='official_midpoint')
    assert adapter.validate_capture(value)
    changed=copy.deepcopy(value);changed['sampling_metadata']['missing_s5_intervals']=0
    changed['capture_sha256']=adapter._hash({k:v for k,v in changed.items() if k!='capture_sha256'})
    with pytest.raises(ValueError,match='replay'):adapter.validate_capture(changed)


def test_current_research_requires_explicit_midpoint_variant():
    with pytest.raises(ValueError,match='price_convention'):
        capture(scope='current_research',price_convention='retained_midpoint_unknown_ingestion')


def test_timestamp_target_anchor_and_original_allowed_training_delay(model):
    value=capture(irregular_rows(10),sampling_policy='retained_fit_window')
    result=adapter.predict_curve(value,model,clock=lambda:NOW)
    for point in result['points']:
        assert point['target_epoch']==value['reference_price_epoch']+point['horizon_sec']
        assert point['training_target_price_window_end_epoch']==point['target_epoch']+7
        assert point['actual_future_target_epoch'] is None
        assert point['target_anchor_is_nominal_training_endpoints_may_be_delayed'] is True
        policy=point['target_selection_policy']
        assert policy['admissible_target_price_epoch_bounds']==[point['target_epoch'],point['target_epoch']+7]
        assert policy['nominal_target_label_epoch']==point['target_label_epoch']
        assert policy['actual_target_label_epoch'] is None
        assert policy['actual_target_price_epoch'] is None
        assert policy['nominal_anchor_guarantees_exact_training_endpoint'] is False


@pytest.mark.parametrize('delay,accepted',[(0,True),(5,True),(7,True),(8,False),(10,False)])
def test_exact_original_target_selection_prefix_uses_timestamp_not_row_count(delay,accepted):
    source=Path(adapter.__file__).with_name('oanda_second_forecast_fit.py').read_bytes()
    assert hashlib.sha256(source).hexdigest()==adapter.ORIGINAL_SOURCES['oanda_second_forecast_fit.py']
    original=next(node for node in ast.parse(source).body if isinstance(node,ast.FunctionDef) and node.name=='fit_horizon')
    prefix=[]
    for statement in original.body:
        if isinstance(statement,ast.Assign) and any(isinstance(target,ast.Name) and target.id=='x' for target in statement.targets):break
        prefix.append(statement)
    assert prefix and not any(isinstance(node,ast.Call) and isinstance(node.func,ast.Name) and node.func.id=='ridge_fit' for stmt in prefix for node in ast.walk(stmt))
    result=ast.parse('return indices, future, target_ns[valid], delay_ns[valid]').body[0]
    function=ast.FunctionDef(name='original_target_selection_only',args=original.args,
        body=prefix+[result],decorator_list=[],returns=None)
    ns={'np':np,'Any':object}
    exec(compile(ast.fix_missing_locations(ast.Module(body=[function],type_ignores=[])),'<verified-original-target-prefix>','exec'),ns)
    # Missing intermediate S5 rows: a shifted three-row target would select40s,
    # whereas the original timestamp search selects the row at15+delay seconds.
    base=1_780_000_000_000_000_000
    times=np.array([base,base+10_000_000_000,base+(15+delay)*1_000_000_000,base+40_000_000_000],dtype=np.int64)
    indices,future,nominal,delays=ns['original_target_selection_only']({'indices':np.array([0]),'times':times},15)
    assert len(indices)==int(accepted)
    if accepted:
        assert future.tolist()==[2] and nominal.tolist()==[base+15_000_000_000]
        assert delays.tolist()==[delay*1_000_000_000]


@pytest.mark.parametrize('bad', [[], rows()[:-1], rows()+[rows()[-1]]])
def test_exact_real_row_count(bad):
    with pytest.raises(ValueError,match='13_real'): capture(bad)


@pytest.mark.parametrize('mutate', [
    lambda r:r[4].update(bar_start_epoch=r[3]['bar_start_epoch']),
    lambda r:r[4].update(bar_start_epoch=r[4]['bar_start_epoch']+5),
    lambda r:r[4].update(bar_start_epoch=r[4]['bar_start_epoch']+.5),
    lambda r:r[4].update(complete=False),
    lambda r:r[4].update(complete='true'),
    lambda r:r[4].update(available_epoch=r[4]['bar_start_epoch']),
    lambda r:r[4].update(available_epoch=NOW+1),
    lambda r:r[4].update(mid_close=float('nan')),
    lambda r:r[4].update(volume=-1),
    lambda r:r[4].update(volume=True),
    lambda r:r[4].update(spread_pips=100),
    lambda r:r[4].update(bid_close=r[4]['ask_close']+.1),
    lambda r:r[4].update(mid_high=r[4]['mid_close']-.1),
    lambda r:r[4].update(mid_close=r[4]['ask_close']+.1),
    lambda r:r[4].update(extra='silently_ignored'),
])
def test_input_adversaries(mutate):
    raw=rows(); mutate(raw)
    with pytest.raises(ValueError): capture(raw)


@pytest.mark.parametrize('instrument,pip', [('EUR_USD',.01),('HKD_JPY',.01),('USD_HUF',.0001),('USD_THB',.0001),('EUR_EUR',.0001),('eur_usd',.0001)])
def test_explicit_pip_and_pair_binding(instrument,pip):
    with pytest.raises(ValueError): capture(instrument=instrument,pip_size=pip)


@pytest.mark.parametrize('instrument,pip',[('USD_HUF',.01),('USD_THB',.01),('HKD_JPY',.0001),('USD_JPY',.01)])
def test_actual_training_pip_exceptions(instrument,pip):
    value=capture(rows(pip),instrument=instrument,pip_size=pip)
    assert value['pip_size']==pip


def test_resealed_capture_derivations_are_replayed():
    value=capture();value['features']['return_5_pips']+=100
    value['capture_sha256']=adapter._hash({k:v for k,v in value.items() if k!='capture_sha256'})
    with pytest.raises(ValueError,match='replay'):adapter.validate_capture(value)


@pytest.mark.parametrize('field', ['reference_epoch','reference_price','max_bar_close_epoch','first_observed_epoch'])
def test_unsealed_clock_or_price_tamper(field):
    value=capture();value[field]+=1
    with pytest.raises(ValueError,match='seal'):adapter.validate_capture(value)


def test_engineering_old_data_does_not_become_live(model):
    value=capture(clock=lambda:NOW+1000)
    result=adapter.predict_curve(value,model,clock=lambda:NOW+1001)
    assert result['scope']=='engineering_replay'
    assert result['forecast_issued'] is False
    with pytest.raises(ValueError,match='stale_current'):capture(clock=lambda:NOW+1000,scope='current_research')


def test_actual_computation_clock_and_expiry(model):
    value=capture(scope='current_research')
    with pytest.raises(ValueError,match='before_available'):adapter.predict_curve(value,model,clock=lambda:NOW-1)
    with pytest.raises(ValueError,match='start'):adapter.predict_curve(value,model,clock=lambda:NOW+31)
    clock=iter([NOW,NOW+31])
    with pytest.raises(ValueError,match='completion'):adapter.predict_curve(value,model,clock=lambda:next(clock))
    clock=iter([NOW,NOW-1])
    with pytest.raises(ValueError,match='reversed'):adapter.predict_curve(value,model,clock=lambda:next(clock))


def test_model_bytes_fail_closed(tmp_path):
    bad=tmp_path/'model.json';bad.write_text('{}')
    with pytest.raises(ValueError,match='unverified_model'):adapter.load_model(bad)


@pytest.mark.parametrize('mutation',['coefficients','fit_clock','missing_cell'])
def test_self_declared_artifact_hash_does_not_verify_replaced_snapshot(model,mutation):
    if mutation=='coefficients':
        changed=replace(model.cells[0],intercept=model.cells[0].intercept+100)
        forged=replace(model,cells=(changed,*model.cells[1:]))
    elif mutation=='fit_clock':
        forged=replace(model,fitted_epoch=model.fitted_epoch-1)
    else:
        forged=replace(model,cells=model.cells[:-1])
    assert forged.artifact_sha256==adapter.ARTIFACT_SHA256
    with pytest.raises(ValueError,match='verified_model_required'):
        adapter.predict_curve(capture(),forged,clock=lambda:NOW)


def test_factory_verification_required_even_for_exact_looking_model(model,monkeypatch):
    monkeypatch.setattr(adapter,'_VERIFIED_MODEL_BYTES',None)
    with pytest.raises(ValueError,match='verified_model_required'):
        adapter.predict_curve(capture(),model,clock=lambda:NOW)


def test_original_sources_fail_closed(monkeypatch):
    monkeypatch.setattr(adapter,'ORIGINAL_SOURCES',{'oanda_second_forecast.py':'0'*64})
    with pytest.raises(ValueError,match='source_changed'):capture()


def test_inference_deterministic_original_nodes_and_clocks(model):
    value=capture();first=adapter.predict_curve(value,model,clock=lambda:NOW)
    second=adapter.predict_curve(value,model,clock=lambda:NOW)
    assert first==second
    assert [p['horizon_sec'] for p in first['points']]==list(adapter.HORIZONS)
    assert first['reference_price_epoch']==first['reference_label_epoch']+5
    for point in first['points']:
        assert point['target_epoch']==first['reference_price_epoch']+point['horizon_sec']
        assert point['target_label_epoch']==first['reference_label_epoch']+point['horizon_sec']
        assert point['target_price_epoch']==point['target_label_epoch']+5
        assert point['residual_std_pips']>=.05
        assert point['probability_scope'].startswith('uncalibrated')
        assert all(point[k] is v for k,v in adapter.INERT.items())
    assert all(first[k] is v for k,v in adapter.INERT.items())
    assert 'issued_epoch' not in first and '"profiles":' not in json.dumps(first)


def test_all_horizons_independent_decimal_coefficient_formula(model):
    value=capture();actual=adapter.predict_curve(value,model,clock=lambda:NOW)
    original=json.loads(MODEL.read_text())
    with localcontext() as ctx:
        ctx.prec=80
        D=lambda x:Decimal(str(x))
        for point in actual['points']:
            m=original['models']['EUR_USD'][str(point['horizon_sec'])]
            raw=D(m['intercept'])+sum(D(b)*(D(value['features'][name])-D(mean))/max(D(scale),D('1e-9'))
                for name,mean,scale,b in zip(adapter.FEATURE_NAMES,m['feature_means'],m['feature_scales'],m['coefficients']))
            expected=float(raw)*max(0,min(10,m['magnitude_calibration']))
            assert point['raw_score_pips']==pytest.approx(float(raw),abs=1e-10)
            assert point['predicted_signed_pips']==pytest.approx(expected,abs=1e-10)
            p=1/(1+math.exp(-max(-8,min(8,expected/max(.05,m['residual_std_pips'])))))
            assert point['probability_up']==pytest.approx(p,abs=1e-12)


def test_no_sqlite_network_or_runtime_instances(model,monkeypatch):
    def forbidden(*a,**kw):raise AssertionError('side effect prohibited')
    monkeypatch.setattr(sqlite3,'connect',forbidden)
    monkeypatch.setattr(socket,'socket',forbidden)
    assert adapter.predict_curve(capture(),model,clock=lambda:NOW)['status']=='computed_not_issued'


@pytest.mark.parametrize('policy',['exact_grid','retained_fit_window'])
def test_real_recovered_s5_original_transform_compatibility(model,policy):
    if not REAL_INPUT.exists():pytest.skip('preserved engineering S5 input absent')
    source=Path(adapter.__file__).with_name('oanda_second_forecast_fit.py').read_bytes()
    assert hashlib.sha256(source).hexdigest()==adapter.ORIGINAL_SOURCES['oanda_second_forecast_fit.py']
    tree=ast.parse(source)
    names={'column_numpy','window_sum','window_std','feature_matrix','pip_multiplier'}
    functions=[node for node in tree.body if isinstance(node,ast.FunctionDef) and node.name in names]
    # Execute only the five verified pure transform functions; never import a
    # fitter, runner, store or its transitive execution dependencies.
    ns={'np':np,'pq':pq,'Path':Path,'Any':object,'PIP_LOCATION_MINUS2':adapter.TRAINED_PIP_MINUS2}
    exec(compile(ast.Module(body=functions,type_ignores=[]),'<verified-original-transform>','exec'),ns)
    dataset=ns['feature_matrix'](REAL_INPUT,4,14400)
    # The original transform admits a 55–70-second overall span, including
    # interior gaps. Select the latest exact-grid origin without looking at y.
    eligible=[(position,int(i)) for position,i in enumerate(dataset['indices'])
              if policy=='retained_fit_window' or np.all(np.diff(dataset['times'][int(i)-12:int(i)+1])==5_000_000_000)]
    position,index=eligible[-1]
    table=pq.read_table(REAL_INPUT).slice(index-12,13).to_pylist()
    real=[]
    for row in table:
        epoch=datetime.fromisoformat(row['dt']).timestamp()
        real.append({'bar_start_epoch':epoch,'available_epoch':NOW,'complete':True,
            **{k:float(row[k]) for k in ['bid_close','ask_close','mid_close','mid_high','mid_low','spread_pips','volume']}})
    value=adapter.capture_s5_rows(real,instrument='EUR_USD',pip_size=.0001,
        source_sha256=hashlib.sha256(REAL_INPUT.read_bytes()).hexdigest(),clock=lambda:NOW,sampling_policy=policy)
    if policy=='retained_fit_window':
        assert value['sampling_metadata']['missing_s5_intervals']>0
    assert list(value['features'].values())==pytest.approx(dataset['features'][position],abs=2e-7)
    result=adapter.predict_curve(value,model,clock=lambda:NOW)
    assert result['input_after_model_fit'] is False
    assert len(result['points'])==13 and result['forecast_issued'] is False
