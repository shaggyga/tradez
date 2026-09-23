"""Independent exact-clock/null/identity probes, owned tiny SQLite files only."""
import copy,json
import pytest
from test_nullable_profile_v4 import T,Clock,inputs,candidate,side_candidate,run,rows,quotes,admitted
import outcome_shadow_profile_v4 as profile
from oanda_forecast_ledger_v4 import LiveForecastLedgerV4
from oanda_outcome_clock_v2 import build_market_quote_snapshot,iso

@pytest.fixture
def tmp_path(tmp_path_factory):return tmp_path_factory.mktemp('t')

def snap(t=T+60,pairs=None):
    return build_market_quote_snapshot({'generated_utc':iso(t),'quotes':pairs or {
        'EUR_USD':{'bid':1.1005,'ask':1.1007,'time':iso(t)}}},now=t,max_quote_age_sec=15)

def row(ledger,table):
    cursor=ledger.connection.execute('SELECT * FROM '+table)
    return [dict(zip([c[0] for c in cursor.description],r)) for r in cursor.fetchall()]

@pytest.mark.parametrize('target',['target_profitable','target_best_side'])
@pytest.mark.parametrize('long,short,direction,expected_net',[(.7,.3,'buy',3),(.3,.7,'sell',-7),(.5,.5,'flat',None)])
def test_side_target_roundtrip_does_not_supply_midpoint_probability(tmp_path,target,long,short,direction,expected_net):
    raw=side_candidate(long,short,target);source=inputs();raw['ask']=1.1002
    source['instruments']['EUR_USD']['quote']['ask']=1.1002
    value,_=profile.normalize_candidate(raw,source,T,15)
    ledger=LiveForecastLedgerV4(tmp_path/'n_v4.sqlite')
    try:
        ledger.register([value],'fixture-source')
        pending=row(ledger,'predictions')[0]
        assert pending['probability_up'] is None
        assert json.loads(pending['source_point_json'])==raw['forecast_curve']['60']
        assert ledger.mature(snap(),10,now=T+60)['matured']==1
        actual=row(ledger,'outcomes')[0]
        assert actual['probability_up'] is None and actual['brier'] is None
        assert actual['signed_mid_move_pips']==pytest.approx(5)
        assert actual['direction']==direction and actual['actual_up']==1
        if expected_net is None:
            assert actual['executable_net_pips'] is None and actual['direction_correct'] is None
        else:assert actual['executable_net_pips']==pytest.approx(expected_net)
        assert actual['generated_epoch']==T and actual['target_epoch']==T+60
        assert actual['entry_quote_epoch']==T and actual['forecast_observed_epoch']==T
        assert json.loads(actual['source_point_json'])==raw['forecast_curve']['60']
    finally:ledger.close()

def test_summary_denominators_exclude_abstention_and_absent_brier(tmp_path):
    ledger=LiveForecastLedgerV4(tmp_path/'n_v4.sqlite')
    try:
        values=[admitted(candidate(identifier=d,direction=d)) for d in ('buy','sell','flat')]
        ledger.register(values,'fixture');ledger.mature(snap(),10,now=T+60)
        cell=ledger.summary(now=T+60)['cells'][0]
        assert cell['n']==3 and cell['direction_n']==cell['executable_n']==2
        assert cell['midpoint_brier_n']==0 and cell['midpoint_brier'] is None
        assert cell['signed_error_n']==cell['magnitude_error_n']==0
    finally:ledger.close()

def test_midpoint_target_label_and_brier_cannot_be_injected_directly(tmp_path):
    ledger=LiveForecastLedgerV4(tmp_path/'n_v4.sqlite')
    try:
        value=admitted();value['forecast_curve']['60']['probability_up']=.5
        with pytest.raises(ValueError):ledger.register([value],'fixture')
        assert not row(ledger,'predictions') and ledger.wall_highwater==0
    finally:ledger.close()

def test_collision_after_first_insert_rolls_back_batch_and_highwater(tmp_path):
    ledger=LiveForecastLedgerV4(tmp_path/'n_v4.sqlite')
    try:
        original=admitted();ledger.register([original],'fixture')
        good=admitted(candidate(identifier='new'),observed=T+1)
        bad=admitted(candidate(direction='sell'))
        with pytest.raises(ValueError,match='identity_collision'):
            ledger.register([good,bad],'fixture')
        assert [r['candidate_id'] for r in row(ledger,'predictions')]==[original['id']]
        assert ledger.wall_highwater==T
        assert ledger.connection.execute('SELECT wall_highwater FROM outcome_clock_contract').fetchone()==(T,)
    finally:ledger.close()

def test_failed_maturity_preserves_pending_and_highwater(tmp_path,monkeypatch):
    ledger=LiveForecastLedgerV4(tmp_path/'n_v4.sqlite')
    try:
        ledger.register([admitted()],'fixture')
        with monkeypatch.context() as patch:
            patch.setattr(ledger,'_delete_prediction_rowids',lambda _:(_ for _ in ()).throw(RuntimeError('fixture')))
            with pytest.raises(RuntimeError):ledger.mature(snap(),10,now=T+60)
        assert not row(ledger,'outcomes') and len(row(ledger,'predictions'))==1
        assert ledger.wall_highwater==T
        assert ledger.connection.execute('SELECT wall_highwater FROM outcome_clock_contract').fetchone()==(T,)
    finally:ledger.close()

@pytest.mark.parametrize('text',[None,'not-a-clock',iso(T+1),iso(T-1)])
def test_direct_register_requires_entry_quote_text_epoch_agreement(tmp_path,text):
    value=admitted();value['research_entry_clock']['entry_quote_time']=text
    ledger=LiveForecastLedgerV4(tmp_path/'n_v4.sqlite')
    try:
        with pytest.raises(ValueError):ledger.register([value],'fixture')
        assert not row(ledger,'predictions')
    finally:ledger.close()

def test_generation_precision_allowed_by_typed_contract_is_not_silently_tightened():
    raw=candidate(t=T+.0005);raw['generated_utc']=iso(T)
    source=inputs(t=T+.0005)
    value,_=profile.normalize_candidate(raw,source,T+.001,15)
    assert value['forecast_generated_epoch']==T+.0005
    assert value['generated_utc']==iso(T)

def test_receipt_before_register_failure_recovers_once_then_never_resurrects(tmp_path,monkeypatch):
    root=tmp_path/'p';clock=Clock()
    with monkeypatch.context() as patch:
        patch.setattr(LiveForecastLedgerV4,'register',lambda *a,**k:(_ for _ in ()).throw(RuntimeError('after receipt fixture')))
        with pytest.raises(RuntimeError):run(root,clock)
    paths=list((root/'typed_signals/forecast_receipts').glob('*.json'))
    assert len(paths)==1;before=paths[0].read_bytes()
    clock.now=T+1;run(root,clock,producer=lambda _:[])
    assert len(rows(root,'predictions'))==1
    clock.now=T+60;run(root,clock,producer=lambda _:[])
    clock.now=T+61;run(root,clock,producer=lambda _:[])
    assert not rows(root,'predictions') and len(rows(root,'outcomes'))==1
    assert paths[0].read_bytes()==before
    assert rows(root,'outcomes')[0]['forecast_observed_epoch']==T

def test_one_pair_quote_does_not_score_another_pair(tmp_path):
    ledger=LiveForecastLedgerV4(tmp_path/'n_v4.sqlite')
    try:
        other,_=profile.normalize_candidate(candidate(pair='GBP_USD',identifier='other'),inputs(pair='GBP_USD'),T,15)
        ledger.register([admitted(),other],'fixture')
        result=ledger.mature(snap(T+60),10,now=T+60)
        assert result['matured']==1 and len(row(ledger,'predictions'))==1
        empty=build_market_quote_snapshot({},now=T+71,max_quote_age_sec=15)
        assert ledger.mature(empty,10,now=T+71)['censored']==1
        second=next(r for r in row(ledger,'outcomes') if r['instrument']=='GBP_USD')
        assert second['status'].startswith('censored')
        assert all(second[k] is None for k in ('actual_up','direction_correct','executable_net_pips','signed_mid_move_pips','brier'))
    finally:ledger.close()

def test_unchanged_midpoint_preserves_declared_zero_rule(tmp_path):
    ledger=LiveForecastLedgerV4(tmp_path/'n_v4.sqlite')
    try:
        ledger.register([admitted(candidate(identifier=d,direction=d)) for d in ('buy','sell')],'fixture')
        terminal=snap(pairs={'EUR_USD':{'bid':1.1,'ask':1.1001,'time':iso(T+60)}})
        ledger.mature(terminal,10,now=T+60)
        result={r['direction']:r for r in row(ledger,'outcomes')}
        assert result['buy']['actual_up']==result['sell']['actual_up']==0
        assert result['buy']['direction_correct']==0 and result['sell']['direction_correct']==1
        assert all(r['executable_net_pips']<0 and r['brier'] is None for r in result.values())
    finally:ledger.close()

def test_market_observations_cannot_mutate_the_stored_typed_point(tmp_path):
    ledger=LiveForecastLedgerV4(tmp_path/'n_v4.sqlite')
    try:
        value=admitted(side_candidate());ledger.register([value],'fixture')
        bad=json.loads(row(ledger,'predictions')[0]['source_point_json']);bad['direction']='sell'
        ledger.connection.execute('UPDATE predictions SET source_point_json=?',(json.dumps(bad),));ledger.connection.commit()
        with pytest.raises(ValueError):ledger.mature(snap(),10,now=T+60)
        assert not row(ledger,'outcomes') and len(row(ledger,'predictions'))==1
    finally:ledger.close()
