from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

import trad.oanda_spike_blurb_fomc_statement_sources_v1 as source


FIXTURE = b"""
<html><body><nav>irrelevant navigation</nav>
<div><p>For release at 2:00 p.m. EDT</p>
<p>Recent indicators suggest that economic activity has continued to expand at
a solid pace. Job gains have slowed and inflation remains elevated.</p>
<p>The Committee decided to maintain the target range for the federal funds rate.
The Committee will carefully assess incoming data and the balance of risks.</p>
<p>The Committee will continue reducing its holdings of Treasury securities.
Voting for the monetary policy action were A; B; C. This sentence makes the
deterministic official statement fixture longer than the minimum body length.</p>
<p>For media inquiries, please email the Board.</p></div>
<footer>irrelevant footer</footer></body></html>
"""


def test_source_population_is_exact_baseline_plus_twenty_one() -> None:
    rows = source.source_specs()
    assert len(rows) == 22
    assert rows[0]["event_date"] == "2023-12-13"
    assert rows[0]["role"] == "prior_statement_baseline"
    assert len({row["event_date"] for row in rows}) == 22
    assert all(row["source_url"].startswith("https://www.federalreserve.gov/") for row in rows)


def test_statement_projection_excludes_page_chrome_and_media_footer() -> None:
    release, body = source.extract_statement_body(FIXTURE)
    assert release == "For release at 2:00 p.m. EDT"
    assert "irrelevant navigation" not in body
    assert "For media inquiries" not in body
    assert "irrelevant footer" not in body
    assert "federal funds rate" in body


@pytest.mark.parametrize(
    "payload,error",
    [
        (b"<p>no clock</p>", "official_release_clock_marker_missing"),
        (
            b"<p>For release at 2:00 p.m. EST Committee federal funds rate Voting</p>",
            "official_statement_end_marker_missing",
        ),
    ],
)
def test_statement_projection_fails_closed(payload: bytes, error: str) -> None:
    with pytest.raises(ValueError, match=error):
        source.extract_statement_body(payload)


def test_source_tables_are_immutable(tmp_path: Path) -> None:
    database = tmp_path / "source.sqlite"
    connection = sqlite3.connect(database)
    source.ensure_schema(connection)
    connection.execute(
        "INSERT INTO fomc_statement_source_contracts VALUES(?,?,?,?,?,?,?,?)",
        ("c", "{}", "b", "p", "2026-08-20T00:00:00+00:00", 1, 0, 0),
    )
    connection.commit()
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        connection.execute(
            "UPDATE fomc_statement_source_contracts SET contract_json='x' WHERE contract_id='c'"
        )
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        connection.execute("DELETE FROM fomc_statement_source_contracts WHERE contract_id='c'")
    connection.close()
