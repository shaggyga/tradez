"""Identity equality is canonical JSON equality, not Python numeric equality."""
import copy
import pytest
import outcome_shadow_profile_v4 as profile
from oanda_forecast_ledger_v4 import LiveForecastLedgerV4
from test_nullable_profile_v4 import T,Clock,candidate,run

ALIASES=[(1,1.0),(1,True),(0,False),(0.0,False),(True,1),(1.0,1)]

@pytest.mark.parametrize('left,right',ALIASES)
def test_pinned_producer_metadata_distinguishes_bool_int_float(tmp_path,left,right):
    paths=profile.prepare_paths(tmp_path/'p','typed_signals')
    profile.pin_run_policy(paths,max_quote_age_sec=15,max_delay_sec=10,producer_identity={'release':left})
    original=(paths['role']/'science_policy_v4.json').read_bytes()
    assert {'release':left}=={'release':right}
    with pytest.raises(ValueError,match='new_cohort'):
        profile.pin_run_policy(paths,max_quote_age_sec=15,max_delay_sec=10,producer_identity={'release':right})
    assert (paths['role']/'science_policy_v4.json').read_bytes()==original

@pytest.mark.parametrize('left,right',ALIASES)
def test_shared_source_proof_distinguishes_opaque_bool_int_float_metadata(tmp_path,left,right):
    root=tmp_path/'s';raw=candidate();raw['source_release']=left
    run(root,Clock(),producer=lambda _:[raw])
    envelope=next(profile.read_profile_forecasts(root))
    changed=copy.deepcopy(raw);changed['source_release']=right
    assert changed==raw and profile.canonical(changed)!=profile.canonical(raw)
    paths=profile.prepare_paths(tmp_path/'d','shared_intake')
    profile.pin_run_policy(paths,max_quote_age_sec=15,max_delay_sec=10)
    ledger=LiveForecastLedgerV4(paths['ledger'])
    try:
        with pytest.raises(ValueError,match='observation_proof'):
            profile.record_forecast(paths,ledger,changed,envelope['inputs'],observed_epoch=T,
                max_quote_age_sec=15,observation_receipt=envelope['receipt'],intake_epoch=T+1)
        assert not list(paths['receipts'].glob('*.json'))
        assert not list(paths['snapshots'].glob('*.json'))
        assert ledger.pending_predictions==0
        assert ledger.connection.execute('SELECT wall_highwater FROM outcome_clock_contract').fetchone()==(0,)
    finally:ledger.close()

def test_identical_policy_with_reordered_keys_stays_identical(tmp_path):
    paths=profile.prepare_paths(tmp_path/'p','typed_signals')
    one={'release':1,'scope':{'name':'fixture','disabled':True}}
    two={'scope':{'disabled':True,'name':'fixture'},'release':1}
    profile.pin_run_policy(paths,max_quote_age_sec=15,max_delay_sec=10,producer_identity=one)
    before=(paths['role']/'science_policy_v4.json').read_bytes()
    profile.pin_run_policy(paths,max_quote_age_sec=15,max_delay_sec=10,producer_identity=two)
    assert (paths['role']/'science_policy_v4.json').read_bytes()==before

def test_exact_source_proof_with_reordered_envelope_keys_is_admitted(tmp_path):
    source=tmp_path/'s';raw=candidate(signed=1)
    run(source,Clock(),producer=lambda _:[raw])
    envelope=next(profile.read_profile_forecasts(source))
    changed=dict(reversed(list(raw.items())))
    assert profile.canonical(raw)==profile.canonical(changed)
    paths=profile.prepare_paths(tmp_path/'d','shared_intake')
    profile.pin_run_policy(paths,max_quote_age_sec=15,max_delay_sec=10)
    ledger=LiveForecastLedgerV4(paths['ledger'])
    try:
        assert profile.record_forecast(paths,ledger,changed,envelope['inputs'],observed_epoch=T,
            max_quote_age_sec=15,observation_receipt=envelope['receipt'],intake_epoch=T+1)==1
        assert profile.record_forecast(paths,ledger,changed,envelope['inputs'],observed_epoch=T,
            max_quote_age_sec=15,observation_receipt=envelope['receipt'],intake_epoch=T+2)==0
    finally:ledger.close()
