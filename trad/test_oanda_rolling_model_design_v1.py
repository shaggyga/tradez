import numpy as np
import pytest
from oanda_rolling_model_design_v1 import (
    COMPACT_LOCAL, feature_groups, fit_normalizer, transform_inputs, learner_inputs,
)


def names():
    return COMPACT_LOCAL + [f'm1__fixture_{i}' for i in range(216-len(COMPACT_LOCAL))] + [f'peer__fixture_{i}' for i in range(12)]


def test_exact_groups_and_future_input_rejected():
    groups=feature_groups(names())
    assert [len(g) for g in groups.values()]==[38,50,216,228]
    bad=names();bad[-1]='label__60m__return_bps'
    with pytest.raises(ValueError,match='future'):feature_groups(bad)
    with pytest.raises(ValueError,match='228'):feature_groups(names()[:-1])


def test_train_only_normalizer_future_changes_cannot_change_past_or_parameters():
    train=np.column_stack((np.arange(60.),np.ones(60),np.full(60,np.nan)))
    p=fit_normalizer(train)
    assert p['supported'].tolist()==[True,False,False]
    assert p['count'].tolist()==[60,60,0]
    earlier=transform_inputs(train,p)
    future=np.array([[1e6,3.,5.],[-1e6,2.,7.]])
    z=transform_inputs(np.vstack((train,future)),p)
    assert np.array_equal(z[:60],earlier,equal_nan=True)
    assert np.isnan(z[:,1:]).all()
    assert abs(float(earlier[:,0].mean()))<1e-7
    assert np.allclose(earlier[:,0].std(),1.)


def test_missingness_not_zero_and_train_support_minimum():
    x=np.column_stack((np.r_[np.arange(19.),np.nan],np.arange(20.)))
    p=fit_normalizer(x);assert p['supported'].tolist()==[False,True]
    y=transform_inputs(np.array([[55.,np.nan]]),p)
    assert np.isnan(y).all()
    ridge=learner_inputs(y,np.array([1]),'ridge',3)
    assert ridge.tolist()==[[0.,0.,1.,1.,0.,1.,0.]]
    hgb=learner_inputs(y,np.array([1]),'hgb',3)
    assert np.isnan(hgb[0,:2]).all() and hgb[0,2]==1


def test_unregistered_context_and_unknown_learner_rejected():
    with pytest.raises(ValueError,match='context'):learner_inputs(np.zeros((2,1)),np.array([0,2]),'ridge',2)
    with pytest.raises(ValueError,match='context'):learner_inputs(np.zeros((2,1)),np.array([0.,1.]),'ridge',2)
    with pytest.raises(ValueError,match='learner'):learner_inputs(np.zeros((2,1)),np.array([0,1]),'bad',2)


def test_float32_overflow_is_not_silently_missing():
    p={'mean':np.array([0.]),'scale':np.array([1.]),'supported':np.array([True])}
    with pytest.raises(ValueError,match='overflow'):transform_inputs(np.array([[1e100]]),p)


def test_decimal_constant_does_not_become_supported_from_rounding():
    p=fit_normalizer(np.full((1000,1),.1))
    assert not p['supported'][0]
    assert np.isnan(transform_inputs(np.array([[.1],[.2]]),p)).all()


def test_float64_overflow_also_fails_before_cast():
    p={'mean':np.array([-1e308]),'scale':np.array([1.]),'supported':np.array([True])}
    with pytest.raises(ValueError,match='float64_transform_overflow'):
        transform_inputs(np.array([[1e308]]),p)
