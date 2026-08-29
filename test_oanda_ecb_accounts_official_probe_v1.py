import oanda_ecb_accounts_official_probe_v1 as probe


def test_rss_probe_selects_only_accounts_release():
    payload = b"""<rss><channel>
    <item><title>Other ECB release</title><link>https://www.ecb.europa.eu/other</link></item>
    <item><title>Account of the monetary policy meeting held on 22-23 July 2026</title>
    <link>https://www.ecb.europa.eu/press/accounts/2026/html/ecb.mg260827~abc.en.html</link>
    <pubDate>Thu, 27 Aug 2026 11:30:00 GMT</pubDate></item>
    </channel></rss>"""
    rows = probe.rss_candidates(payload)
    assert len(rows) == 1
    assert rows[0]["published_raw"] == "Thu, 27 Aug 2026 11:30:00 GMT"
    assert "ecb.mg260827" in rows[0]["url"]


def test_index_probe_deduplicates_accounts_links():
    payload = b"""
    <a href='/press/accounts/2026/html/ecb.mg260827~abc.en.html'>Account</a>
    <a href='/press/accounts/2026/html/ecb.mg260827~abc.en.html'>Account duplicate</a>
    <a href='/press/accounts/2026/html/ecb.mg260625~old.en.html'>Old account</a>
    <a href='/press/pr/date/2026/html/other.en.html'>Other</a>
    """
    rows = probe.index_candidates(payload)
    assert len(rows) == 1
    assert rows[0]["url"].startswith("https://www.ecb.europa.eu/press/accounts/2026/")
