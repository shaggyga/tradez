"""Availability adapter must preserve missing evidence and original targets."""
import copy

from trad.tools.build_fixed_forecast_input import build, timestamp


def original():
    return {'rows':[{
        'forecast_id':'original','family':'example','cohort_id':'frozen','model_version':'model',
        'feature_version':'features','instrument':'EUR_USD','horizon_sec':3600,'input_timeframe':'M1',
        'probability_up':.4,'direction':'buy','reference_time_utc':'2026-09-01T10:00:00+00:00',
        'entry_quote':{'bid':1.1,'ask':1.1002},'expected_signed_pips':2.0,'pip':.0001,
        'payload_sha256':'a'*64,'recorded_utc':'2026-09-01T10:02:00+00:00',
        'issued_utc':None,'committed_utc':None,'features_available_utc':None,
        'data_cutoff_utc':'2026-09-01T09:59:00+00:00','max_training_label_maturity_utc':None,
        'max_training_label_available_utc':None,
        'outcome':{'stored_target_time_utc':'2026-09-01T11:02:00+00:00'}
    }]}


def test_recorded_clock_and_shifted_target_cannot_fill_missing_evidence():
    source=original();before=copy.deepcopy(source)
    result=build(source,source_sha256='b'*64,cutoff_epoch=timestamp('2026-09-02T00:00:00+00:00'))
    decision=result['decisions'][0];forecast=decision['forecasts'][0]
    assert forecast['issued_epoch'] is None and forecast['committed_available_epoch'] is None
    assert forecast['training_labels_available_max_epoch'] is None
    assert forecast['features_available_epoch'] is None
    assert decision['target_epoch']==timestamp('2026-09-01T11:00:00+00:00')
    assert result['quotes']==[] and result['provenance']['missing_timestamps_inferred'] is False
    assert source==before
    assert forecast['side']==1 and forecast['probability_up']==.4


def test_missing_outcome_and_every_original_family_are_retained():
    source=original();second=copy.deepcopy(source['rows'][0])
    second.update(forecast_id='other',family='other_family',outcome=None)
    source['rows'].append(second)
    result=build(source,source_sha256='b'*64,cutoff_epoch=timestamp('2026-09-02T00:00:00+00:00'))
    assert len(result['decisions'])==1
    forecasts=result['decisions'][0]['forecasts']
    assert len(forecasts)==2 and forecasts[1]['original_stored_outcome_present'] is False
    assert result['provenance']['original_forecast_count']==2


def test_numeric_prediction_is_normalized_to_original_reference_bps():
    result=build(original(),source_sha256='b'*64,cutoff_epoch=timestamp('2026-09-02T00:00:00+00:00'))
    assert abs(result['decisions'][0]['forecasts'][0]['predicted_return_bps']-(2*.0001/1.1001*10000))<1e-12
