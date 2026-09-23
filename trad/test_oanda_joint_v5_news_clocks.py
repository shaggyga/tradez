import copy,hashlib,json
from pathlib import Path
import pytest
import oanda_causal_forecast_inputs_joint_news_v3 as inputs
import oanda_joint_price_news_forecast_study_v5 as worker
import prepare_joint_price_news_registry_v5 as prepare
import oanda_local_news_sentiment_repair_v2 as repaired
from test_oanda_causal_forecast_inputs_joint_news_v1 import NOW,member

def derived_member(story_age=900,derived_age=60):
    row=member(NOW-story_age,909,score=.6)
    row.update(classification_observation_contract='article_classification_observation_v1_20260912',
      classification_clock_status='valid_attested_classification',classification_first_known_utc=inputs._iso(NOW-derived_age),
      classification_available_utc=inputs._iso(NOW-derived_age),source_evidence_contract='retained_story_body_age_v1_20260912',
      source_evidence_content_sha256='a'*64,source_evidence_available_utc=inputs._iso(NOW-story_age),
      causal_known_utc=inputs._iso(NOW-derived_age))
    return row
def frame(row,now=NOW):
    payload={'news_capture_sha256':inputs._digest([row,now]),'first_observed_epoch':now,'current_members':[row]}
    return inputs._frame(payload,now,live=True)

def test_current_source_closure_matches_repaired_v2_exactly():
    assert inputs.CURRENT_SOURCE_FILES==repaired.SOURCE_FILES
    pins=inputs._bindings()
    assert {k:pins[k] for k in inputs.CURRENT_SOURCE_FILES}==repaired.source_bindings()
    assert set(pins)<=worker.REQUIRED_SOURCE_BINDINGS

def test_later_classification_does_not_refresh_story_age():
    row=derived_member();story,derived=inputs._member_clocks(row)
    assert story==NOW-900 and derived==NOW-60
    result=frame(row)
    assert result['members'][0]['known_epoch']==story
    features,_=inputs._pair_features(result,'EUR_USD',NOW)
    assert features[3]==.25

def test_future_classification_is_not_current_context():
    assert frame(derived_member(derived_age=-10))['members']==[]

def test_old_story_cannot_reenter_context_after_new_classifier():
    assert frame(derived_member(story_age=3700))['members']==[]

@pytest.mark.parametrize('field',['source_evidence_contract','source_evidence_available_utc','source_evidence_content_sha256','classification_available_utc','classification_first_known_utc'])
def test_incomplete_new_clock_contract_cannot_enter(field):
    row=derived_member();row.pop(field)
    with pytest.raises((ValueError,TypeError)):inputs._member_clocks(row)
    assert frame(row)['members']==[]

def test_unattested_new_classification_cannot_enter():
    row=derived_member();row['classification_clock_status']='unproven_classification'
    with pytest.raises(ValueError):inputs._member_clocks(row)
    assert frame(row)['members']==[]

def test_historical_mapping_before_derived_clock_is_not_admitted():
    row=derived_member();payload={'news_capture_sha256':inputs._digest(row),'history':[{'mapping_visible_epoch':NOW-1000,'member':row}]}
    row['observed_available_utc']=inputs._iso(NOW-1000)
    assert inputs._frame(payload,NOW-120)['members']==[]
    assert len(inputs._frame(payload,NOW)['members'])==1

def test_legacy_original_stays_explicit_retrospective_material():
    row=member(NOW-900,919,score=.4)
    assert inputs._member_clocks(row)==(NOW-900,NOW-900)
    assert 'legacy original classifications are retrospective material' in inputs.TRAINING_POLICY

def test_registration_is_source_bound_new_cohort_without_database(tmp_path):
    out=tmp_path/'registration.json'
    result=prepare.prepare_registry(out,scope='fixture',selected_pairs=['EUR_USD'],clock=lambda:NOW)
    assert result['status']=='prepared_not_activated' and not result['database_opened']
    loaded=worker.load_registry(out)
    slot=loaded['pairs']['EUR_USD']['families']['ridge_price_news_v1']['contract']
    assert slot['horizon_sec']==3600 and '.source_derived_news_publication_v5_20260913.fixture' in slot['contract_id']
    assert slot['feature_version']=='sha256:'+loaded['source_bindings']['oanda_causal_forecast_inputs_joint_news_v3.py']
    assert not list(tmp_path.rglob('*.sqlite'))
    with pytest.raises(ValueError,match='existing_registration'):prepare.prepare_registry(out,scope='fixture',selected_pairs=['EUR_USD'],clock=lambda:NOW)
