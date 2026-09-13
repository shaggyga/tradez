import copy
import datetime as dt
import hashlib
from pathlib import Path
import pytest
import oanda_local_news_sentiment as news

BODY = b'%PDF-1.7 synthetic fetched body'
TEXT = 'The central bank explains the economic outlook and the current monetary policy decision in this official publication.'
NOW = dt.datetime(2026, 9, 13, 5, 0, tzinfo=dt.timezone.utc)

class Response:
    def __init__(self, url, body, kind):
        self.url, self.body = url, body
        self.headers = {'Content-Type': kind}
    def __enter__(self): return self
    def __exit__(self, *_): return False
    def geturl(self): return self.url
    def read(self, maximum): return self.body[:maximum]

def invoke(tmp_path, monkeypatch, kind, condition):
    root = tmp_path / 'archive'
    monkeypatch.setattr(news, 'DEFAULT_OUTPUT_ROOT', root)
    page_url = 'https://www.boj.or.jp/en/synthetic.htm'
    pdf_url = 'https://www.boj.or.jp/en/synthetic.pdf'
    url = page_url if kind == 'attachment' else pdf_url
    digest = hashlib.sha256(BODY).hexdigest()
    cached = root / 'official_documents/boj_updates' / (digest + '.pdf')
    wrong = b'x' * len(BODY)
    if condition in ('good_hit', 'bad_hit', 'oversize_hit'):
        cached.parent.mkdir(parents=True)
        cached.write_bytes(BODY if condition == 'good_hit' else wrong if condition == 'bad_hit' else BODY + b'x')
    initial_cached = cached.read_bytes() if cached.exists() else None
    original_writer = news.atomic_write_bytes
    writes = []
    def writer(path, body):
        writes.append(str(path))
        original_writer(path, wrong if condition == 'bad_write' else body)
    monkeypatch.setattr(news, 'atomic_write_bytes', writer)
    calls = []
    def fetch(request, **kwargs):
        calls.append(request.full_url)
        if request.full_url == page_url:
            return Response(page_url, b'<html><main>Official monetary policy discussion and economic outlook.</main><a href="synthetic.pdf">Full Text PDF</a></html>', 'text/html')
        assert request.full_url == pdf_url
        return Response(pdf_url, BODY, 'application/pdf')
    monkeypatch.setattr(news.urllib.request, 'urlopen', fetch)
    original_extract = news.extract_official_document_text
    def extract(payload, *, url, content_type, **kwargs):
        if url == pdf_url: return TEXT, 'official_pdf_text'
        assert url == page_url
        return TEXT, 'official_html_text'
    monkeypatch.setattr(news, 'extract_official_document_text', extract)
    article = {'source_id': 'boj_updates', 'source_name': 'Bank of Japan updates', 'source_kind': 'rss',
        'source_verified': True, 'source_direct': True, 'source_role': 'primary_policy_release',
        'source_currencies': ['JPY'], 'title': 'Official monetary policy discussion', 'summary': '',
        'url': url, 'published_utc': (NOW - dt.timedelta(minutes=1)).isoformat()}
    source = {'source_id': 'boj_updates', 'detail_enrichment': 'official_document_text',
        'archive_official_pdfs': True, 'trusted_domains': ['boj.or.jp']}
    if kind == 'attachment':
        source.update(detail_follow_same_authority_pdf_attachments=True,
            detail_attachment_link_text_patterns=['full\\s+text'], detail_attachment_max_items_per_page=1)
    before = copy.deepcopy(article)
    result = news.enrich_recent_official_release_details([article], source, {},
        timeout_sec=1, maximum_bytes=1000000, now=NOW)
    assert calls == ([page_url, pdf_url] if kind == 'attachment' else [pdf_url]), result
    return result, article, before, cached, initial_cached, writes

@pytest.mark.parametrize('kind', ['direct', 'attachment'])
@pytest.mark.parametrize('condition', ['new', 'good_hit', 'bad_hit', 'oversize_hit', 'bad_write'])
def test_actual_enrichment_only_binds_verified_archive(tmp_path, monkeypatch, kind, condition):
    (count, state, error), article, before, cached, prior, writes = invoke(tmp_path, monkeypatch, kind, condition)
    if condition in ('new', 'good_hit'):
        assert count == 1 and error == '' and article['detail_enriched'] is True
        assert cached.read_bytes() == BODY
        if kind == 'direct': assert article['detail_archive_path'] == str(cached)
        else: assert article['detail_attachment_content_sha256'] == hashlib.sha256(BODY).hexdigest()
        assert article['published_utc'] == before['published_utc']
        assert len(writes) == int(condition == 'new')
    else:
        assert count == 0 and 'archive hash mismatch' in error
        assert article == before
        if prior is not None:
            assert cached.read_bytes() == prior and writes == []

@pytest.mark.parametrize('condition', ['wrong_digest', 'wrong_filename', 'empty'])
def test_archive_input_binding_precedes_file_access(tmp_path, condition):
    if not hasattr(news, 'retain_verified_official_pdf'):
        pytest.skip('new helper absent in baseline')
    digest = hashlib.sha256(BODY).hexdigest()
    target = tmp_path / ((('a' * 64) if condition == 'wrong_filename' else digest) + '.pdf')
    with pytest.raises(ValueError, match='archive .* binding invalid'):
        news.retain_verified_official_pdf(target, b'' if condition == 'empty' else BODY,
            'a' * 64 if condition == 'wrong_digest' else digest)
    assert not target.exists()


"""Independent cache binding, I/O error and failed attachment-state probes."""
import copy,hashlib,io
from pathlib import Path
import pytest

@pytest.mark.parametrize('kind',['direct','attachment'])
def test_io_failure_preserves_article_and_rejects_enrichment(tmp_path,monkeypatch,kind):
    def refused(*args,**kwargs):raise PermissionError('synthetic archive write refused')
    monkeypatch.setattr(news,'atomic_write_bytes',refused)
    result,article,before,cached,prior,writes=invoke(tmp_path,monkeypatch,kind,'new')
    assert result[0]==0 and 'synthetic archive write refused' in result[2]
    assert article==before and not cached.exists()

def test_cache_read_is_bounded_to_expected_payload_plus_one(tmp_path,monkeypatch):
    digest=hashlib.sha256(BODY).hexdigest();path=tmp_path/(digest+'.pdf');reads=[];writes=[]
    monkeypatch.setattr(Path,'exists',lambda _:True)
    class Retained(io.BytesIO):
        def read(self,size=-1):reads.append(size);return super().read(size)
    def opened(self,mode='r',*args,**kwargs):
        assert self==path and mode=='rb';return Retained(BODY+b'extra oversized tail')
    monkeypatch.setattr(Path,'open',opened)
    monkeypatch.setattr(news,'atomic_write_bytes',lambda *a:writes.append(a))
    with pytest.raises(ValueError,match='hash mismatch'):news.retain_verified_official_pdf(path,BODY,digest)
    assert reads==[len(BODY)+1] and writes==[]

def test_failed_attachment_does_not_advance_returned_availability(tmp_path,monkeypatch):
    result,article,before,cached,prior,writes=invoke(tmp_path,monkeypatch,'attachment','bad_hit')
    assert result[0]==0 and article==before
    assert result[1]=={}, 'Failed cache verification advanced attachment first-seen state'

def test_failed_second_attachment_does_not_commit_partial_bundle_state(tmp_path,monkeypatch):
    root=tmp_path/'archive';monkeypatch.setattr(news,'DEFAULT_OUTPUT_ROOT',root)
    parent='https://www.boj.or.jp/en/bundle.htm';first='https://www.boj.or.jp/en/first.pdf';second='https://www.boj.or.jp/en/second.pdf'
    payloads={first:BODY+b'first',second:BODY+b'second'}
    bad=root/'official_documents/boj_updates'/(hashlib.sha256(payloads[second]).hexdigest()+'.pdf')
    bad.parent.mkdir(parents=True);bad.write_bytes(b'corrupt retained bytes')
    html=b'<html><main>Official policy release.</main><a href="first.pdf">Full Text PDF</a><a href="second.pdf">Full Text PDF</a></html>'
    def fetch(request,**kwargs):
        url=request.full_url
        return Response(url,html if url==parent else payloads[url],'text/html' if url==parent else 'application/pdf')
    monkeypatch.setattr(news.urllib.request,'urlopen',fetch)
    monkeypatch.setattr(news,'extract_official_document_text',lambda payload,**kwargs:(TEXT,'official_html_text' if kwargs['url']==parent else 'official_pdf_text'))
    source={'source_id':'boj_updates','detail_enrichment':'official_document_text','archive_official_pdfs':True,
        'trusted_domains':['boj.or.jp'],'detail_follow_same_authority_pdf_attachments':True,
        'detail_attachment_link_text_patterns':['full\\s+text'],'detail_attachment_max_items_per_page':2}
    article={'source_id':'boj_updates','source_name':'BOJ','title':'Official monetary policy decision','summary':'',
        'url':parent,'published_utc':NOW.isoformat()};before=copy.deepcopy(article)
    count,state,error=news.enrich_recent_official_release_details([article],source,{},timeout_sec=1,maximum_bytes=1000000,now=NOW)
    assert count==0 and 'hash mismatch' in error and article==before
    good=root/'official_documents/boj_updates'/(hashlib.sha256(payloads[first]).hexdigest()+'.pdf')
    assert good.read_bytes()==payloads[first]
    assert state=={}, 'Failed later attachment leaked earlier partial-bundle availability'
