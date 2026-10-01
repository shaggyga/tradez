"""Exercise actual context loop; no data acquisition, model loading or fitting."""
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'trad'))
import oanda_currency_news_context_v1 as context


def test_new_generation_wakes_before_full_minute_without_busy_retry():
    s=context.RetainedRefresh();a={'generation':'a'*64,'generated_epoch':1000}
    b={'generation':'b'*64,'generated_epoch':1015}
    assert s.reason(0,a)=='startup'
    s.record(0,8,a,'startup',{'status':'current','forecasts':10})
    assert s.reason(10,b) is None
    assert s.reason(23,a) is None
    assert s.reason(23,b)=='technical_publication_changed'
    assert s.reason(68,a)=='periodic_recovery'
    assert s.reason(68,None)=='periodic_recovery'
    assert s.reason(7,b) is None


@pytest.mark.parametrize('fault',['missing','broken','future','stale','bad_generation','error','boolean_clock'])
def test_unusable_hint_is_not_authority_or_capture_failure(tmp_path,fault):
    p=tmp_path/'status.json'
    value={'status':'partial','publication_generation':'a'*64,'generated_epoch':1000}
    if fault=='future':value['generated_epoch']=1100
    elif fault=='stale':value['generated_epoch']=800
    elif fault=='bad_generation':value['publication_generation']='changed'
    elif fault=='error':value['status']='error'
    elif fault=='boolean_clock':value['generated_epoch']=True
    if fault!='missing':p.write_text('{' if fault=='broken' else json.dumps(value),encoding='utf-8')
    assert context.retained_publication_hint(path=p,now=1001) is None


def test_valid_hint_exposes_only_generation_and_clock(tmp_path):
    p=tmp_path/'status.json';p.write_text(json.dumps({'status':'partial','publication_generation':'b'*64,
        'generated_epoch':1000,'untrusted_forecasts':[1,2,3]}),encoding='utf-8')
    assert context.retained_publication_hint(path=p,now=1001)=={'generation':'b'*64,'generated_epoch':1000}


@pytest.mark.parametrize('mode',['changed_generation','hint_unavailable','inference_failure'])
def test_actual_main_loop_refreshes_and_tracks_without_blocking_news(tmp_path,monkeypatch,mode):
    ticks={'now':0.};inferences=[];tracking=[];news=[]
    monkeypatch.setattr(context.time,'monotonic',lambda:ticks['now'])
    monkeypatch.setattr(context.time,'sleep',lambda seconds:ticks.update(now=ticks['now']+seconds))
    def hint():
        if mode=='hint_unavailable':return None
        return {'generation':('a' if ticks['now']<30 else 'b')*64,'generated_epoch':1000+ticks['now']}
    monkeypatch.setattr(context,'retained_publication_hint',hint)
    cached=object()
    def infer(registry,connection):
        assert connection is None if not inferences else connection is cached
        inferences.append(ticks['now']);ticks['now']+=8
        return cached,{'status':'error' if mode=='inference_failure' else 'current','forecasts':10}
    monkeypatch.setattr(context,'run_retained_inference',infer)
    def collect(*args,**kwargs):
        news.append(ticks['now'])
        return {'schema_version':context.SCHEMA,'status':'current','topics':[],**context.FLAGS}
    monkeypatch.setattr(context,'collect',collect)
    def track(*args,**kwargs):tracking.append(ticks['now']);return {'status':'current'}
    monkeypatch.setitem(sys.modules,'oanda_retained_forecast_tracking_v1',SimpleNamespace(collect=track))
    output=tmp_path/'output'
    monkeypatch.setattr(sys,'argv',['context','--database',str(tmp_path/'unused'),
        '--output',str(output),'--collector-heartbeat',str(tmp_path/'unused_heartbeat'),
        '--retained-model-registry',str(tmp_path/'registry'),'--duration-sec','100'])
    assert context.main()==0
    assert inferences==([0.,68.] if mode=='hint_unavailable' else [0.,38.])
    assert len(tracking)==len(inferences)==2 and len(news)>=5
    value=json.loads((output/'current.json').read_bytes())
    assert value['payload_sha256']==context.digest({k:v for k,v in value.items() if k!='payload_sha256'})
    assert value['retained_schedule']['hint_is_not_input_authority'] is True
    assert value['retained_schedule']['inference_seconds']==8
    if mode=='inference_failure':assert value['retained_schedule']['result_status']=='error'


def test_generation_arriving_during_inference_is_not_marked_consumed():
    s=context.RetainedRefresh();a={'generation':'a'*64,'generated_epoch':1000}
    s.record(0,8,a,'startup',{'status':'current','forecasts':10})
    assert s.reason(23,{'generation':'b'*64,'generated_epoch':1005})=='technical_publication_changed'
