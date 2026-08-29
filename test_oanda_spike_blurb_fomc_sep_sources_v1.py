from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

import trad.oanda_spike_blurb_fomc_sep_sources_v1 as sep


FIXTURE = b"""
<html><body><p>For release at 2:00 p.m. EDT</p>
<h2>Summary of Economic Projections</h2>
<table><thead>
<tr><th id="xa1">Variable</th><th id="xa2">Median</th></tr>
<tr><th id="xb1">2026</th><th id="xb2">2027</th><th id="xb3">2028</th><th id="xb4">Longer run</th></tr>
</thead><tbody>
<tr><th id="r1">Change in real GDP</th><td headers="xa2 xb1 r1">2.2</td><td headers="xa2 xb2 r1">2.3</td><td headers="xa2 xb3 r1">2.2</td><td headers="xa2 xb4 r1">2.0</td></tr>
<tr><th id="r2">March projection</th><td headers="xa2 xb1 r1 r2">2.4</td><td headers="xa2 xb2 r1 r2">2.3</td><td headers="xa2 xb3 r1 r2">2.1</td><td headers="xa2 xb4 r1 r2">2.0</td></tr>
<tr><th id="r3">Unemployment rate</th><td headers="xa2 xb1 r3">4.4</td><td headers="xa2 xb2 r3">4.3</td><td headers="xa2 xb3 r3">4.2</td><td headers="xa2 xb4 r3">4.2</td></tr>
<tr><th id="r4">March projection</th><td headers="xa2 xb1 r3 r4">4.3</td><td headers="xa2 xb2 r3 r4">4.2</td><td headers="xa2 xb3 r3 r4">4.2</td><td headers="xa2 xb4 r3 r4">4.2</td></tr>
<tr><th id="r5">PCE inflation</th><td headers="xa2 xb1 r5">3.1</td><td headers="xa2 xb2 r5">2.5</td><td headers="xa2 xb3 r5">2.1</td><td headers="xa2 xb4 r5">2.0</td></tr>
<tr><th id="r6">March projection</th><td headers="xa2 xb1 r5 r6">2.8</td><td headers="xa2 xb2 r5 r6">2.3</td><td headers="xa2 xb3 r5 r6">2.0</td><td headers="xa2 xb4 r5 r6">2.0</td></tr>
<tr><th id="r7">Core PCE inflation4</th><td headers="xa2 xb1 r7">3.3</td><td headers="xa2 xb2 r7">2.5</td><td headers="xa2 xb3 r7">2.1</td></tr>
<tr><th id="r8">March projection</th><td headers="xa2 xb1 r7 r8">2.7</td><td headers="xa2 xb2 r7 r8">2.2</td><td headers="xa2 xb3 r7 r8">2.0</td></tr>
<tr><th id="r9">Memo: Projected appropriate policy path</th></tr>
<tr><th id="r10">Federal funds rate</th><td headers="xa2 xb1 r10">3.8</td><td headers="xa2 xb2 r10">3.6</td><td headers="xa2 xb3 r10">3.4</td><td headers="xa2 xb4 r10">3.1</td></tr>
<tr><th id="r11">March projection</th><td headers="xa2 xb1 r10 r11">3.4</td><td headers="xa2 xb2 r10 r11">3.1</td><td headers="xa2 xb3 r10 r11">3.1</td><td headers="xa2 xb4 r10 r11">3.1</td></tr>
</tbody></table></body></html>
"""


def test_source_population_is_exact_eleven_official_sep_pages() -> None:
    rows = sep.source_specs()
    assert len(rows) == 11
    assert rows[0]["event_date"] == "2023-12-13"
    assert rows[-1]["event_date"] == "2026-06-17"
    assert len({row["event_date"] for row in rows}) == 11
    assert all(row["source_url"].startswith("https://www.federalreserve.gov/") for row in rows)


def test_parser_extracts_current_and_embedded_prior_medians() -> None:
    parsed = sep.parse_projection_release(FIXTURE)
    table = parsed["projection_table"]
    assert parsed["release_clock_text"] == "For release at 2:00 p.m. EDT"
    assert set(table) == {
        "real_gdp_growth", "unemployment_rate", "pce_inflation",
        "core_pce_inflation", "federal_funds_rate",
    }
    assert table["federal_funds_rate"]["current"]["2026"] == 3.8
    assert table["federal_funds_rate"]["prior"]["2026"] == 3.4
    assert table["federal_funds_rate"]["comparison_label"] == "March projection"
    assert table["core_pce_inflation"]["current"]["2028"] == 2.1


@pytest.mark.parametrize(
    "payload,error",
    [
        (b"<h2>Summary of Economic Projections</h2>", "official_release_clock_marker_missing"),
        (
            b"<p>For release at 2:00 p.m. EDT</p><h2>Summary of Economic Projections</h2>",
            "official_sep_table_count_invalid",
        ),
    ],
)
def test_parser_fails_closed_on_missing_source_contract(payload: bytes, error: str) -> None:
    with pytest.raises(ValueError, match=error):
        sep.parse_projection_release(payload)


def test_source_tables_are_immutable(tmp_path: Path) -> None:
    database = tmp_path / "sep.sqlite"
    connection = sqlite3.connect(database)
    sep.ensure_schema(connection)
    connection.execute(
        "INSERT INTO fomc_sep_source_contracts_v1 VALUES(?,?,?,?,?,?,?,?)",
        ("c", "{}", "b", "p", "2026-08-20T00:00:00+00:00", 1, 0, 0),
    )
    connection.commit()
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        connection.execute(
            "UPDATE fomc_sep_source_contracts_v1 SET contract_json='x' WHERE contract_id='c'"
        )
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        connection.execute("DELETE FROM fomc_sep_source_contracts_v1 WHERE contract_id='c'")
    connection.close()
