"""Training-only fitted news and price comparisons on synthetic real timestamps."""
import copy
import math
import pytest
import oanda_joint_price_news_models_v1 as model

START=1788652800

def example():
    rows={START+i*60:1.1+.0001*(.006*i+4*math.sin(i/100)+math.sin(i/17)) for i in range(2100)}
    frames={str(t):[math.sin(i*.4)*.7,math.log1p(1+i%4),(-1 if i%3 else 1)*.5,(i%17)/17,0,0,0,0] for i,t in enumerate(rows) if t%900==0}
    return rows,max(rows),frames,[.5,math.log1p(2),1.,.1,0.,0.,0.,0.]

def predict(data):
    rows,cutoff,frames,news=data
    return model.predict_with_readiness(rows,cutoff,instrument='EUR_USD',pip_size=.0001,news_frames=frames,current_news=news)

def test_fitted_price_news_and_neutral_ablation_are_documented():
    result,ready=predict(example());expected,prob,diag=result[model.FAMILY]
    assert ready[model.FAMILY]['ready'] and .25<=prob<=.75
    assert expected==pytest.approx(diag['neutral_news_ablation_expected_pips']+diag['news_ablation_difference_pips'])
    assert diag['matched_price_only_fitted']['overlap_adjusted_training_count_heuristic']==diag['fitted_joint']['overlap_adjusted_training_count_heuristic']
    assert len(diag['fitted_joint']['coefficients'])==diag['feature_count']+1
    assert diag['training_label_maturity_max_epoch']<=example()[1]+60

def test_neutral_news_query_has_exactly_zero_ablation_difference():
    data=list(example());data[-1]=list(model.NEUTRAL_NEWS)
    result,_=predict(data)
    assert result[model.FAMILY][2]['news_ablation_difference_pips']==pytest.approx(0,abs=1e-12)

def test_current_news_changes_joint_estimate_without_refitting_or_altering_matched_price_baseline():
    data=list(example());before=predict(data)[0][model.FAMILY]
    data[3]=[-.7,math.log1p(4),-1.,.8,0,0,0,0]
    after=predict(data)[0][model.FAMILY]
    assert abs(before[0]-after[0])>1e-8
    assert before[2]['fitted_joint']['coefficients']==after[2]['fitted_joint']['coefficients']
    assert before[2]['matched_price_only_expected_pips']==after[2]['matched_price_only_expected_pips']

def test_future_news_frames_do_not_affect_training_or_prediction():
    data=example();before=predict(data);frames=copy.deepcopy(data[2])
    for t in list(frames):
        if int(t)+3600>data[1]:frames[t]=[-1.5,2.,-1.,.9,0,0,0,0]
    assert predict((data[0],data[1],frames,data[3]))==before

def test_missing_exact_h1_target_reduces_training_without_filling():
    data=list(example());before=predict(data)[0][model.FAMILY][2]['training_rows']
    target=START+100*900;data[0].pop(target)
    after=predict(data)[0][model.FAMILY][2]
    assert after['training_rows']<before and after['synthetic_prices']==0 and after['time_compression'] is False

def test_sparse_news_and_unseen_vetted_signal_abstain():
    data=list(example());data[2]={t:list(model.NEUTRAL_NEWS) for t in data[2]}
    result,ready=predict(data)
    assert result=={} and not ready[model.FAMILY]['ready']
    data=list(example());data[3][4:]=[.5,math.log1p(1),0,.5]
    result,ready=predict(data)
    assert result=={} and any('unseen_in_training' in r for r in ready[model.FAMILY]['reasons'])

@pytest.mark.parametrize('key',[True,1788652800.5,'01788652800','1e9','1788652801'])
def test_news_anchor_identity_not_silently_coerced(key):
    data=list(example());data[2]={key:list(model.NEUTRAL_NEWS)}
    with pytest.raises(ValueError):predict(data)

@pytest.mark.parametrize('value',[float('nan'),float('inf'),True,'0.5',3.])
def test_invalid_news_features_are_rejected(value):
    data=list(example());data[3][0]=value
    with pytest.raises(ValueError):predict(data)

def test_empty_or_wrong_family_selection_rejected():
    data=example()
    for families in ([],['probabilistic_state_space'],[model.FAMILY,model.FAMILY]):
        with pytest.raises(ValueError):model.predict_with_readiness(data[0],data[1],instrument='EUR_USD',pip_size=.0001,news_frames=data[2],current_news=data[3],families=families)
