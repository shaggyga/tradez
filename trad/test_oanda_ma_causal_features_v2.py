"""Causal-prefix regression tests; no model fitting or market-performance claims."""
import hashlib

import numpy as np
import pytest

import oanda_ma_feature_grid as old
import oanda_ma_causal_features_v2 as new


def test_future_suffix_no_longer_changes_177_flat_prefix_features():
    prefix=np.ones(204);whole=np.r_[prefix,1+np.sin(np.arange(400))*.002]
    old_short,names=old.build_ma_feature_matrix(prefix,[203],.0001,'M1')
    old_whole,_=old.build_ma_feature_matrix(whole,[203],.0001,'M1')
    assert np.count_nonzero(~np.isclose(old_short,old_whole,atol=1e-7,rtol=1e-7))==177
    short,new_names=new.build_feature_matrix(prefix,[203],.0001,'M1')
    longer,_=new.build_feature_matrix(whole,[203],.0001,'M1')
    assert names==new_names
    np.testing.assert_array_equal(short,longer)


@pytest.mark.parametrize('timeframe',['S5','M1','M15','H1','D1'])
@pytest.mark.parametrize('shape',['constant','trend','oscillating','flat_after_motion'])
def test_batch_every_selected_row_equals_its_original_prefix(timeframe,shape):
    i=np.arange(900)
    if shape=='constant':values=np.ones(900)
    elif shape=='trend':values=1+i*.00001
    elif shape=='oscillating':values=1+np.sin(i*.11)*.002
    else:values=np.r_[1+np.sin(i[:350]*.2)*.001,np.full(550,1.01)]
    positions=[203,349,600,899]
    full,names=new.build_feature_matrix(values,positions,.0001,timeframe)
    for row,pos in zip(full,positions):
        prefix,prefix_names=new.build_feature_matrix(values[:pos+1],[pos],.0001,timeframe)
        assert names==prefix_names
        np.testing.assert_array_equal(row,prefix[0])


def test_vector_and_matrix_share_exact_canonical_values():
    values=1+np.sin(np.arange(2000)*.019)*.002
    matrix,names=new.build_feature_matrix(values,[1999],.0001,'M1')
    vector=new.build_feature_vector(values,.0001,'M1')
    assert vector=={name:round(float(v),8) for name,v in zip(names,matrix[0])}


def test_legacy_globals_and_source_bytes_are_unchanged():
    old_scale,old_cross=old._rolling_scale,old._cross_age
    before=hashlib.sha256(new.ORIGINAL_SOURCE_PATH.read_bytes()).hexdigest()
    new.build_feature_vector(np.linspace(1,1.1,500),.0001,'M1')
    assert old._rolling_scale is old_scale and old._cross_age is old_cross
    assert hashlib.sha256(new.ORIGINAL_SOURCE_PATH.read_bytes()).hexdigest()==before==new.ORIGINAL_SOURCE_SHA256


def test_prior_scale_fallback_excludes_current_and_future_valid_values():
    values=np.r_[1+np.arange(30)*.001,np.full(100,1.03),1+np.arange(50)*.1]
    scale=new._past_scale(values,.0001)
    prefix=new._past_scale(values[:100],.0001)
    np.testing.assert_array_equal(scale[:100],prefix)
    assert scale[0]==.1
    assert scale[99]>1  # retains prior observed movement rather than using a future spike.


@pytest.mark.parametrize('values,positions,pip,reason',[
    ([1.0]*203,[202],.0001,'ma_origin_outside_warmed_prefix'),
    ([1.0]*204,[204],.0001,'ma_origin_outside_warmed_prefix'),
    ([1.0]*204,[203.9],.0001,'integer_ma_positions_required'),
    ([1.0]*204,[True],.0001,'integer_ma_positions_required'),
    ([True]*204,[203],.0001,'real_numeric_ma_history_required'),
    ([1.0]*203+[float('nan')],[203],.0001,'finite_positive_ma_prices_required'),
    ([1.0]*203+[-1.0],[203],.0001,'finite_positive_ma_prices_required'),
    ([1.0]*204,[203],True,'explicit_ma_pip_required'),
    ([1.0]*204,[203],0,'explicit_ma_pip_required'),
])
def test_invalid_or_unwarmed_history_is_never_filled(values,positions,pip,reason):
    with pytest.raises(ValueError,match=reason):new.build_feature_matrix(values,positions,pip,'M1')


def test_source_mutation_is_rejected_before_compilation(tmp_path,monkeypatch):
    source=tmp_path/'changed.py';source.write_text('raise RuntimeError("must not execute")')
    monkeypatch.setattr(new,'ORIGINAL_SOURCE_PATH',source)
    with pytest.raises(ValueError,match='original_ma_source_changed'):
        new.build_feature_vector(np.ones(204),.0001,'M1')


def test_shorter_ema_history_is_not_silently_equated_to_full_history():
    history=np.r_[np.full(600,1.1),np.ones(204)]
    full=new.build_feature_vector(history,.0001,'M1')
    truncated=new.build_feature_vector(history[-204:],.0001,'M1')
    assert abs(full['ma__p200__ema_distance_scale']-truncated['ma__p200__ema_distance_scale'])>1
    assert new.metadata()['retained_weight_compatibility_proven'] is False
