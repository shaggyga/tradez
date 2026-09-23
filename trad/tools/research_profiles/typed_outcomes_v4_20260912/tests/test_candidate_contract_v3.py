"""Root-review counterexamples, including in-memory report/artifact boundaries."""
import copy,hashlib,io,json
from types import SimpleNamespace
import pytest
import oanda_signal_probability_semantics_v2 as semantics
import oanda_typed_signal_feed_adapter_v2 as feed
import oanda_model_gap_live_signal_producer_v2 as producer
from test_candidate_semantics_v2 import make_worker,emitted,snapshot,np,pd

@pytest.mark.parametrize('field,value',[('account_eligible',0),('research_only',1),('horizon_sec',300.0)])
def test_type_alias_in_derived_fields_cannot_forge_a_valid_point(field,value):
    point=emitted()['forecast_curve']['300'];point[field]=value
    with pytest.raises(ValueError):semantics.validate_point(point)

@pytest.mark.parametrize('field,value',[('positive_class',True),('historical_mapping_verified',0)])
def test_type_alias_in_declared_mapping_cannot_forge_positive_class_evidence(field,value):
    point=emitted()['forecast_curve']['300'];point['source_input']['class_mapping'][field]=value
    with pytest.raises(ValueError):semantics.validate_point(point)

@pytest.mark.parametrize('field,value',[('calibrated_brier',.1),('calibration_n',99),('calibration_n',False)])
def test_top_level_midpoint_calibration_cannot_be_smuggled_through_raw_envelope(field,value):
    raw=emitted();raw[field]=value
    with pytest.raises(ValueError):feed.normalize_forecast_v2(raw,source='synthetic')

def add_horizon(raw,horizon):
    original=raw['forecast_curve']['300']
    source=copy.deepcopy(original['source_input']);source['horizon_sec']=horizon
    raw['forecast_curve'][str(horizon)]=semantics.side_classifier_point(**source)

def test_horizon_map_order_does_not_change_candidate_or_dedupe_identity():
    left=emitted();add_horizon(left,60)
    right=copy.deepcopy(left);right['forecast_curve']=dict(reversed(list(right['forecast_curve'].items())))
    first=feed.normalize_forecast_v2(left,source='synthetic');second=feed.normalize_forecast_v2(right,source='synthetic')
    assert first['id']==second['id'] and first['feed_dedupe_key']==second['feed_dedupe_key']
    assert list(first['forecast_curve'])==list(second['forecast_curve'])==['60','300']

def test_named_source_is_part_of_candidate_and_dedupe_identity():
    raw=emitted();left=feed.normalize_forecast_v2(raw,source='producer_A');right=feed.normalize_forecast_v2(raw,source='producer_B')
    assert left['id']!=right['id'] and left['feed_dedupe_key']!=right['feed_dedupe_key']

def test_supported_horizon_set_is_part_of_dedupe_scope():
    raw=emitted();changed=copy.deepcopy(raw);source=copy.deepcopy(changed['forecast_curve'].pop('300')['source_input']);source['horizon_sec']=60
    changed['forecast_curve']['60']=semantics.side_classifier_point(**source)
    assert feed.normalize_forecast_v2(raw,source='synthetic')['feed_dedupe_key']!=feed.normalize_forecast_v2(changed,source='synthetic')['feed_dedupe_key']

@pytest.mark.parametrize('key,value',[('generated_epoch',None),('generated_epoch',True),('generated_epoch',float('nan')),
    ('generated_epoch',float('inf')),('generated_epoch','1800000000'),('generated_epoch',10**400),
    ('generated_utc',None),('generated_utc','not-a-date'),('generated_utc','2027-01-15T08:00:00'),
    ('generated_utc','2027-01-15T08:00:01+00:00')])
def test_original_clock_refused_before_dependency_or_model_call(key,value):
    worker,_=make_worker();state=snapshot();state[key]=value
    def forbidden():raise AssertionError('clock refusal must precede dependency/model calls')
    worker._dependencies=forbidden
    with pytest.raises(ValueError):worker.forecast(state)
    assert worker.last_generated_epoch==0

def test_one_millisecond_precision_is_not_an_offset_estimate():
    state=snapshot();state['generated_epoch']+=.0005
    worker,_=make_worker();assert len(worker.forecast(state))==1
    state['generated_epoch']+=.01
    worker,_=make_worker()
    with pytest.raises(ValueError):worker.forecast(state)

def test_aware_equivalent_clock_is_normalized_without_guessing():
    worker,_=make_worker();state=snapshot();state['generated_utc']='2027-01-15T03:00:00-05:00'
    actual=worker.forecast(state)[0]
    assert actual['generated_epoch']==1800000000.
    assert actual['generated_utc']=='2027-01-15T08:00:00+00:00'

class MemoryPath:
    def __init__(self,name,versions):self.name=name;self.versions=versions;self.opens=0;self.read_sizes=[]
    def is_file(self):return True
    def open(self,mode):
        assert mode=='rb';index=min(self.opens,len(self.versions)-1);self.opens+=1;raw=self.versions[index]
        owner=self
        class Handle(io.BytesIO):
            def read(self,n=-1):owner.read_sizes.append(n);return super().read(n)
        return Handle(raw)
    def __str__(self):return self.name

class MemoryRoot:
    def __init__(self,path):self.path=path
    def __truediv__(self,name):
        assert name=='fictional_model_shared_panel_latest.joblib';return self.path

def reload_fixture(*,wrong_hash=False,large=False):
    worker,run=make_worker();artifact_bytes=b'fictional serialized model bytes A'
    record={'model':'fictional_model','sha256':('c'*64 if wrong_hash else hashlib.sha256(artifact_bytes).hexdigest())}
    metrics={'input_timeframe':'M1','horizon_sec':300,'brier':.04,'events':50,'side_rows':100}
    report={'artifacts':[record],'results':[{'model':'fictional_model','holdout':{'all_prediction_cell_metrics':[metrics]}}]}
    report_bytes=json.dumps(report).encode()
    newer=copy.deepcopy(report);newer['results'][0]['holdout']['all_prediction_cell_metrics'][0]['brier']=.8
    report_path=MemoryPath('never_read_report.json',[report_bytes,json.dumps(newer).encode()])
    artifact_path=MemoryPath('never_read_model.joblib',[artifact_bytes,b'fictional replaced bytes B'])
    loader_calls=[]
    class Loader:
        @staticmethod
        def load(buffer):
            assert isinstance(buffer,io.BytesIO),'must deserialize the exact checked bytes, not reopen a path'
            consumed=buffer.read();loader_calls.append(consumed)
            assert consumed==artifact_bytes
            return run.artifact
    worker.model_root=MemoryRoot(artifact_path);worker.report_path=report_path
    worker.requested_models=('fictional_model',);worker.promotion=SimpleNamespace(reload=lambda:None,summary=lambda *args:{'status':'fixture'})
    worker._dependencies=lambda:(Loader,np,pd)
    worker.runtimes={};worker.load_errors={}
    return worker,report_path,artifact_path,loader_calls,report_bytes,artifact_bytes

def test_report_and_artifact_hashes_bind_exact_bytes_consumed_once():
    worker,report_path,artifact_path,calls,report_bytes,artifact_bytes=reload_fixture()
    worker.reload();assert worker.load_errors=={} and len(worker.runtimes)==1
    assert report_path.opens==1 and artifact_path.opens==1
    assert report_path.read_sizes==[16*1024*1024+1] and artifact_path.read_sizes==[64*1024*1024+1]
    assert calls==[artifact_bytes]
    run=worker.runtimes['fictional_model']
    assert run.sha256==hashlib.sha256(artifact_bytes).hexdigest()
    assert worker.source_report_sha256==hashlib.sha256(report_bytes).hexdigest()
    actual=worker.forecast(snapshot())[0]
    assert actual['forecast_curve']['300']['side_validation']['source_reported_brier']==.04

def test_wrong_artifact_hash_refuses_before_any_deserializer_call():
    worker,_,_,calls,_,_=reload_fixture(wrong_hash=True)
    worker.reload();assert calls==[] and worker.runtimes=={}
    assert 'hash does not match' in worker.load_errors['fictional_model']

@pytest.mark.parametrize('direction',[False,0,1])
def test_malformed_legacy_direction_is_not_treated_as_absent(direction):
    with pytest.raises(ValueError):semantics.legacy_directional_point({'horizon_sec':300,'direction':direction,'predicted_signed_pips':8})
