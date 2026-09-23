"""Exact profile marker and persisted observation-receipt identity."""
import copy,json
from pathlib import Path
import pytest
import outcome_shadow_profile_v4 as profile
from oanda_forecast_ledger_v4 import LiveForecastLedgerV4
from test_nullable_profile_v4 import T,candidate,inputs

def test_profile_marker_false_is_not_integer_zero(tmp_path):
    root=tmp_path/'p';profile.prepare_paths(root,'typed_signals')
    marker=root/'profile.json';value=json.loads(marker.read_text())
    value['account_execution_authorized']=0
    marker.write_text(json.dumps(value))
    with pytest.raises(ValueError,match='profile_identity_mismatch'):
        profile.prepare_paths(root,'typed_signals')

@pytest.mark.parametrize('mutation',['boolean_flag','source_numeric'])
def test_persisted_receipt_alias_is_refused_before_ledger_registration(tmp_path,monkeypatch,mutation):
    paths=profile.prepare_paths(tmp_path/'p','typed_signals')
    profile.pin_run_policy(paths,max_quote_age_sec=15,max_delay_sec=10)
    original=profile.atomic_json
    def altered(path,value):
        if Path(path).parent==paths['receipts']:
            value=copy.deepcopy(value)
            if mutation=='boolean_flag':value['account_execution_authorized']=0
            else:value['identity']['source_forecast']['source_release']=1.0
        return original(path,value)
    ledger=LiveForecastLedgerV4(paths['ledger']);raw=candidate();raw['source_release']=1
    try:
        monkeypatch.setattr(profile,'atomic_json',altered)
        with pytest.raises(ValueError,match='forecast_receipt_readback_failed'):
            profile.record_forecast(paths,ledger,raw,inputs(),observed_epoch=T,max_quote_age_sec=15)
        assert ledger.pending_predictions==0
        assert ledger.connection.execute('SELECT COUNT(*) FROM predictions').fetchone()==(0,)
    finally:ledger.close()
