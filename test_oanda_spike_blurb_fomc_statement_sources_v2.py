from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

import trad.oanda_spike_blurb_fomc_statement_sources_v2 as source


NEW_FORMAT = b"""
<html><body><p>For release at 2:00 p.m. EDT</p>
<p>The Federal Open Market Committee approved the following statement for
release by a 12 - 0 vote:</p>
<p>The Committee decided to maintain the target range for the federal funds
rate. Economic activity is expanding at a solid pace. Inflation remains
elevated relative to the Committee's goal. The Committee reaffirmed its policy
of maintaining ample reserves in the banking system. This deterministic text
is deliberately long enough to satisfy the official-body guard and exercise
the new vote formulation without weakening any other source validation.</p>
<p>For media inquiries, please email the Board.</p></body></html>
"""


OLD_FORMAT = NEW_FORMAT.replace(
    b"approved the following statement for\nrelease by a 12 - 0 vote:",
    b"issued the statement. Voting for the monetary policy action were A and B.",
)


@pytest.mark.parametrize("payload", [NEW_FORMAT, OLD_FORMAT])
def test_v2_accepts_both_official_vote_formulations(payload: bytes) -> None:
    release, body = source.extract_statement_body(payload)
    assert release.endswith("EDT")
    assert "federal funds rate" in body


def test_v2_does_not_drop_vote_evidence_requirement() -> None:
    payload = NEW_FORMAT.replace(b"a 12 - 0 vote", b"an unspecified decision")
    with pytest.raises(ValueError, match="official_statement_vote_text_missing"):
        source.extract_statement_body(payload)


def test_v2_tables_are_immutable(tmp_path: Path) -> None:
    connection = sqlite3.connect(tmp_path / "v2.sqlite")
    source.ensure_schema(connection)
    connection.execute(
        "INSERT INTO fomc_statement_source_v2_contracts VALUES(?,?,?,?,?,?,?,?,?,?,?)",
        ("c", "p", "{}", "b", "pb", "ps", "sp", "t", 1, 0, 0),
    )
    connection.commit()
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        connection.execute(
            "UPDATE fomc_statement_source_v2_contracts SET contract_json='x' WHERE contract_id='c'"
        )
    connection.close()
