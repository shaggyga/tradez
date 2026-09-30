import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import time
import pytest
import oanda_currency_news_context_v1 as context


def values(text,kind):
    return [c.get('value') for c in context.interpret(text)['claims'] if c['kind']==kind]


@pytest.mark.parametrize('text,kind,expected',[
 ("Fed's Williams sees no urgency for next rate hike",'policy_timing','tightening_not_urgent'),
 ("ECB sees no rush to cut rates",'policy_timing','easing_not_urgent'),
 ("Fed's Michael Barr Warns of More Rate Hikes on Inflation",'policy_guidance','further_tightening'),
 ("Fed sees no further rate hikes",'policy_guidance','conditional_or_negated'),
 ("ECB may need more rate cuts",'policy_guidance','conditional_or_negated'),
 ("Fed denies no urgency to hike rates",'policy_timing','unresolved'),
 ("Wall St climbs as cooling inflation tempers Fed rate-hike concerns",'policy_expectations','tightening_expectations_easing'),
 ("Fed rate hike odds fall",'policy_expectations','tightening_expectations_easing'),
 ("Fed rate hike odds might fall",'policy_expectations','unresolved'),
 ("US Oil and Gas Production Rises in Q3 2026 But Price Uncertainty Plagues Producers, Dallas Fed Survey Says",'energy_supply_quantity','increasing'),
 ("Oil production may rise",'energy_supply_quantity','unresolved'),
 ("Oil prices fall as output rises",'energy_price_response','decreasing'),
 ("Iran losing grip on Strait of Hormuz blockade as oil shipments rise",'supply_constraint','reported_easing'),
 ("Iran not losing grip on blockade as oil shipments rise",'supply_constraint','unresolved'),
])
def test_distinct_text_relationships(text,kind,expected):
    assert expected in values(text,kind)
    assert context.interpret(text)['forecast_eligible'] is False


def test_quantities_do_not_create_currency_predictions():
    for text in ['Oil production rises','Oil prices rise','Iran losing grip on blockade as oil shipments rise']:
        claims=context.interpret(text)['claims']
        assert not any(c.get('currency') for c in claims)
    assert values('Oil production rises','energy_price_response')==[]


def test_bank_owner_and_reported_responses():
    x=context.interpret('Fed sees no rush to hike while ECB signals further rate cuts')['claims']
    assert any(c.get('currency')=='USD' and c.get('value')=='tightening_not_urgent' for c in x)
    assert any(c.get('currency')=='EUR' and c.get('value')=='further_easing' for c in x)
    assert not values('Fed and ECB disagree about further rate hikes','policy_guidance')
    c=context.interpret('South African rand firms')['claims'][0]
    assert c['currency']=='ZAR' and c['status']=='retrospective'
    assert not any(c.get('currency') for c in context.interpret('Dollar rises')['claims'])


def test_pce_reaction_keeps_ambiguity_and_explicit_currency_legs():
    x=context.interpret('Dollar Slides on Softer PCE, Aussie Lags as Sterling Extends BoE Rally')['claims']
    assert any(c.get('value')=='reported_dollar_decline_currency_unspecified' for c in x)
    assert any(c.get('currency')=='GBP' and c.get('direction')==1 for c in x)
    assert any(c.get('currency')=='AUD' and c.get('value')=='reported_relative_underperformance' for c in x)
    assert not any(c.get('currency')=='USD' for c in x)


def fixture(tmp_path):
    source=tmp_path/'raw.sqlite';db=sqlite3.connect(source)
    db.execute('CREATE TABLE articles (event_id TEXT,headline TEXT,source_name TEXT,source_id TEXT,source_url TEXT,published_utc TEXT,first_seen_utc TEXT,currency_scores_json TEXT,relevant INTEGER)')
    now=time.time()
    def put(ident,headline,publisher='Publisher',published=None):
        db.execute('INSERT INTO articles VALUES (?,?,?,?,?,?,?,?,1)',(ident,headline,publisher,'feed','https://example.test/'+ident,context.iso(published or now-120),context.iso(now-60),'{}'));db.commit()
    put('one',"Fed's Williams sees no urgency for next rate hike - Publisher")
    health=tmp_path/'heartbeat.json';health.write_text(json.dumps({'schema_version':'local_fx_news_sentiment_v3','generated_utc':context.iso(now),'status':'running_cycle'}))
    return source,db,put,health,now


def test_parse_once_separate_provenance_and_new_text(tmp_path):
    source,db,put,health,now=fixture(tmp_path);out=tmp_path/'out'
    a=context.collect(source,out,health);b=context.collect(source,out,health)
    assert a['new_interpretations']==1 and b['new_interpretations']==0
    assert a['topics'][0]['interpretation_computed_epoch']==b['topics'][0]['interpretation_computed_epoch']
    assert b['store_counts']=={'interpretations':1,'observations':1}
    put('two',"Fed's Williams sees no urgency for next rate hike - Another",'Another')
    c=context.collect(source,out,health)
    assert c['new_interpretations']==0 and c['topic_count']==1 and c['topics'][0]['source_record_count']==2
    put('three','Fed signals further rate hikes')
    d=context.collect(source,out,health)
    assert d['new_interpretations']==1 and d['store_counts']['interpretations']==2
    assert d['topics'][0]['available_epoch']>now
    assert db.execute('SELECT count(*) FROM articles').fetchone()[0]==3
    assert context.read_current(out/'current.json')['status']=='current'
    assert 'original_article_scores' not in context.read_current(out/'current.json')['topics'][0]
    db.close()


def test_source_stale_missing_future_and_payload_tamper(tmp_path):
    source,db,put,health,now=fixture(tmp_path);out=tmp_path/'out'
    put('future','Fed raises rates',published=now+100)
    a=context.collect(source,out,health)
    assert a['source_rows']==2 and a['topic_count']==1
    assert context.read_current(out/'current.json',now=now+1000)['status']=='unavailable'
    a['topics'][0]['headline']='tampered';(out/'current.json').write_text(json.dumps(a))
    assert context.read_current(out/'current.json')['status']=='unavailable'
    health.unlink();a=context.collect(source,out,health)
    assert a['status']=='source_unavailable'
    assert context.read_current(out/'current.json')['status']=='unavailable'
    db.close()


def test_actual_cli_and_capacity_refusal(tmp_path,monkeypatch):
    source,db,put,health,now=fixture(tmp_path);out=tmp_path/'out';db.close()
    r=subprocess.run([sys.executable,'-B',context.__file__,'--database',str(source),'--output',str(out),'--collector-heartbeat',str(health),'--once'],capture_output=True,text=True)
    assert r.returncode==0,r.stderr
    assert context.read_current(out/'current.json')['status']=='current'
    monkeypatch.setattr(context,'MAX_DB',1)
    with pytest.raises(ValueError,match='capacity'):context.collect(source,out,health)


def test_changed_loaded_source_cannot_be_relabeled(monkeypatch):
    monkeypatch.setattr(context,'_LOADED_BINDINGS',{})
    with pytest.raises(ValueError,match='loaded_context_source_changed'):context.bindings()


def test_optional_retained_import_and_error_write_cannot_stop_news(monkeypatch):
    import builtins
    original=builtins.__import__
    def broken(name,*args,**kwargs):
        if name=='oanda_retained_forecast_connection_v1':raise ImportError('optional dependency missing')
        return original(name,*args,**kwargs)
    monkeypatch.setattr(builtins,'__import__',broken)
    monkeypatch.setattr(context,'atomic',lambda *args:(_ for _ in ()).throw(OSError('diagnostic unavailable')))
    connection,result=context.run_retained_inference(Path('unused'),None)
    assert connection is None and result['status']=='error'
    assert 'ImportError' in result['reason']


def test_future_interpretation_refused_even_with_recomputed_seal(tmp_path):
    source,db,put,health,now=fixture(tmp_path);out=tmp_path/'out';db.close()
    a=context.collect(source,out,health)
    a['topics'][0]['available_epoch']=now+1000
    a['payload_sha256']=context.digest({k:v for k,v in a.items() if k!='payload_sha256'})
    (out/'current.json').write_text(json.dumps(a))
    assert context.read_current(out/'current.json')['status']=='unavailable'
