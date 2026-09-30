import json
import time
import urllib.error
from pathlib import Path
from unittest.mock import patch
import pytest
import oanda_news_fast_context_v1 as fast

SOURCE={'source_id':'test','name':'Test','kind':'rss','url':'https://example.test/feed','poll_interval_sec':60}

def lane(tmp_path,clock=lambda:10000,fetcher=fast.fetch):
    with patch.object(fast,'source_config',return_value=[SOURCE]):
        return fast.FastLane(tmp_path/'unused',tmp_path,clock=clock,fetcher=fetcher)

def result(now=10000,headline='Fed signals further hikes'):
    return {'status':'ok','started':now-2,'observed':now,'payload_sha256':'a'*64,
            'rows':[{'headline':headline,'source_url':'https://example.test/article',
                     'published_utc':fast.iso(9900),'source_name':'Test'}]}

def test_repeat_does_not_move_observation_new_text_creates_version(tmp_path):
    l=lane(tmp_path)
    try:
        l.ingest(SOURCE,result());l.ingest(SOURCE,result(10020));l.ingest(SOURCE,result(10040,'Fed sees no urgency'))
        rows=l.db.execute('SELECT first_seen_utc FROM articles ORDER BY first_seen_utc').fetchall()
        assert rows==[(fast.iso(10000),),(fast.iso(10040),)]
        assert l.states['test']['next_due']==10100
    finally:l.close()

def test_future_old_and_missing_publication_withheld(tmp_path):
    l=lane(tmp_path)
    try:
        r=result();r['rows']=[dict(r['rows'][0],published_utc=x) for x in [None,fast.iso(10001),'1969-12-30T00:00:00+00:00']]
        l.ingest(SOURCE,r);assert l.db.execute('SELECT COUNT(*) FROM articles').fetchone()[0]==0
    finally:l.close()

def test_crypto_headlines_not_stored(tmp_path):
    l=lane(tmp_path)
    try:
        l.ingest(SOURCE,result(headline='Bitcoin gains as Fed holds rates'))
        assert l.db.execute('SELECT COUNT(*) FROM articles').fetchone()[0]==0
    finally:l.close()

def test_retry_after_backoff_and_restart(tmp_path):
    l=lane(tmp_path)
    l.ingest(SOURCE,dict(status='error',started=10000,observed=10001,rows=[],retry_after_seconds=7200,error='429'))
    assert l.states['test']['next_due']==17201;l.close()
    l=lane(tmp_path)
    try:assert l.tick()['in_flight']==[] and l.states['test']['errors']==1
    finally:l.close()

def test_slow_feed_cannot_hold_completed_feed(tmp_path):
    import threading
    release=threading.Event()
    def fetcher(source,state):
        if source['source_id']=='slow':release.wait(2)
        return result()
    with patch.object(fast,'source_config',return_value=[dict(SOURCE,source_id='slow'),SOURCE]):
        l=fast.FastLane(tmp_path/'unused',tmp_path,fetcher=fetcher,clock=lambda:10000)
    try:
        l.tick()
        for _ in range(100):
            l.tick()
            if 'test' in l.states:break
            time.sleep(.005)
        assert 'test' in l.states and 'slow' in l.pending
    finally:release.set();l.close()

def test_real_rss_parser_bounded_transport_and_validators():
    class Response:
        headers={'ETag':'test-tag'}
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def read(self,n):
            return b'<rss><channel><item><title>Fed holds rates</title><link>https://example.test/x</link><pubDate>Wed, 30 Sep 2026 12:00:00 GMT</pubDate></item></channel></rss>'
    def opener(request,timeout):
        assert request.get_header('If-none-match')=='old' and timeout==8
        return Response()
    r=fast.fetch(SOURCE,{'etag':'old'},opener=opener)
    assert r['status']=='ok' and r['rows'][0]['title']=='Fed holds rates'
    def quota(*a,**k):raise urllib.error.HTTPError(SOURCE['url'],429,'quota',{'Retry-After':'900'},None)
    assert fast.fetch(SOURCE,{},opener=quota)['retry_after_seconds']==900

def test_config_pin_and_capacity_refuse(tmp_path,monkeypatch):
    p=Path(fast.__file__).parent/'config/fast_headline_sources_v1_20260930.json'
    assert len(fast.source_config(p))==12
    bad=tmp_path/'bad.json';bad.write_bytes(p.read_bytes()+b' ')
    with pytest.raises(ValueError,match='config_changed'):fast.source_config(bad)
    l=lane(tmp_path);monkeypatch.setattr(fast,'MAX_STORE',1)
    try:
        with pytest.raises(ValueError,match='capacity'):l.tick()
    finally:l.close()
