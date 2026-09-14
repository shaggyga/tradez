import datetime as dt
from email.message import Message
import io
import urllib.error
import urllib.request

import pytest

import oanda_rbnz_official_policy_adapter_v1 as subject

NOW = dt.datetime(2026, 9, 14, 0, tzinfo=dt.timezone.utc)
HTML = b'<html><div>Official Cash Rate <b>2.75</b> % Updated: 2:00pm, 02 Sep 2026 Next update: 2:00pm, 28 Oct 2026</div></html>'


def test_existing_ocr_parser_preserves_update_vs_actual_read_and_missing_consensus():
    row, = subject.parse_snapshot(HTML, observed_utc=NOW)
    assert row["actual_value"] == 2.75
    assert row["published_utc"] == "2026-09-02T02:00:00+00:00"
    assert row["source_body_observed_utc"] == NOW.isoformat()
    assert row["consensus_value"] is None
    assert row["numeric_direction_policy"] == "abstain_and_learn_response"
    assert row["historical_original_known_claim"] is False
    assert row["snapshot_is_full_decision_body"] is False


@pytest.mark.parametrize("payload", [b'<html>Access denied</html>', b'', HTML.replace(b'2.75',b'unknown'),
                                     HTML.replace(b'02 Sep 2026', b'02 Sep 2027')])
def test_blockpage_malformed_or_future_rate_cannot_become_release(payload):
    with pytest.raises(ValueError):
        subject.parse_snapshot(payload, observed_utc=NOW)


def test_secure_fetch_records_403_as_blocked_not_zero_error_success():
    class Blocked:
        def open(self, req, timeout):
            assert req.full_url == subject.URL
            assert timeout <= 20
            raise urllib.error.HTTPError(subject.URL, 403, "Forbidden", {}, None)
    result = subject.fetch_snapshot(opener=Blocked(), clock=lambda: NOW)
    assert result["status"] == "publisher_access_blocked"
    assert result["http_status"] == 403
    assert result["rows"] == []
    assert result["error"] == "HTTP403"


@pytest.mark.parametrize("url", ["http://www.rbnz.govt.nz/monetary-policy",
    "https://www.rbnz.govt.nz.evil.test/body", "https://other.govt.nz/release",
    "https://user@www.rbnz.govt.nz/monetary-policy"])
def test_redirect_cannot_downgrade_or_change_authority(url):
    with pytest.raises(ValueError, match="secure_official_authority"):
        subject.OfficialRedirect().redirect_request(urllib.request.Request(subject.URL), None, 302, "", {}, url)


def test_valid_secure_response_does_not_claim_full_statement_or_tradable_signal():
    class Response(io.BytesIO):
        status = 200
        headers = Message()
        headers["Content-Type"] = "text/html"
        def geturl(self):
            return subject.URL
    class Good:
        def open(self, req, timeout):
            return Response(HTML)
    result = subject.fetch_snapshot(opener=Good(), clock=lambda: NOW)
    assert result["status"] == "snapshot_parsed"
    assert result["can_place_orders"] is False
    assert result["full_decision_body_available"] is False


def test_oversized_response_refused():
    with pytest.raises(ValueError, match="size_invalid"):
        subject.parse_snapshot(b'x'*(subject.MAX_BYTES+1), observed_utc=NOW)
