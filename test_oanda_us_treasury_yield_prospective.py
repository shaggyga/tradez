from __future__ import annotations

import json
from pathlib import Path

from trad.oanda_us_treasury_yield_prospective import (
    cohort_contract,
    connect,
    ingest,
    totals,
)


def _xml(rows: list[tuple[str, float, float]]) -> bytes:
    entries = "".join(
        f"""<entry><updated>2026-08-08T12:00:00Z</updated><content><m:properties>
        <d:NEW_DATE>{date}T00:00:00</d:NEW_DATE><d:BC_2YEAR>{two}</d:BC_2YEAR>
        <d:BC_10YEAR>{ten}</d:BC_10YEAR></m:properties></content></entry>"""
        for date, two, ten in rows
    )
    return (
        '<?xml version="1.0"?><feed xmlns="http://www.w3.org/2005/Atom" '
        'xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices" '
        'xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata">'
        f"{entries}</feed>"
    ).encode()


def test_material_collector_change_creates_new_cohort_id() -> None:
    config = {"cohort_start_utc": "2026-08-08T00:00:00Z"}
    first = cohort_contract(config, "a" * 64)
    second = cohort_contract(config, "b" * 64)
    assert first["cohort_id"] != second["cohort_id"]
    assert first["supersedes_cohort_id"].endswith(first["config_sha256"][:16])


def test_bootstrap_new_date_and_revision_have_distinct_evidence_status(tmp_path: Path) -> None:
    config = {
        "contract_id": "test", "cohort_start_utc": "2026-08-08T00:00:00Z",
        "direction_policy": "abstain", "research_only": True,
    }
    contract = cohort_contract(config, "collector")
    connection = connect(tmp_path / "rates.sqlite")
    first = _xml([("2026-08-06", 3.5, 4.2), ("2026-08-07", 3.6, 4.3)])
    assert ingest(
        connection, raw=first, observed_utc="2026-08-08T12:00:00+00:00",
        contract=contract, archive_sha256="raw-1",
    ) == {
        "parsed_rows": 2, "inserted_rows": 2, "prospective_rows": 0,
        "revised_rows": 0, "initial_bootstrap": True,
    }
    assert ingest(
        connection, raw=first, observed_utc="2026-08-08T18:00:00+00:00",
        contract=contract, archive_sha256="raw-1",
    )["inserted_rows"] == 0
    later = _xml([
        ("2026-08-06", 3.5, 4.2), ("2026-08-07", 3.61, 4.3),
        ("2026-08-10", 3.7, 4.4),
    ])
    result = ingest(
        connection, raw=later, observed_utc="2026-08-10T22:00:00+00:00",
        contract=contract, archive_sha256="raw-2",
    )
    assert result["inserted_rows"] == 2
    assert result["prospective_rows"] == 1
    assert result["revised_rows"] == 1
    aggregate = totals(connection, contract["cohort_id"])
    assert aggregate["prospective_eligible_rows"] == 1
    rows = connection.execute(
        "SELECT yield_date,observation_kind,prospective_eligible FROM rate_observations "
        "WHERE observation_version>1 OR prospective_eligible=1 ORDER BY yield_date"
    ).fetchall()
    assert rows == [
        ("2026-08-07", "revised_or_late_historical_row", 0),
        ("2026-08-10", "new_yield_date_first_observed", 1),
    ]
    try:
        connection.execute("UPDATE rate_observations SET direction_policy='long'")
    except Exception as exc:
        assert "append_only" in str(exc)
    else:
        raise AssertionError("append-only Treasury evidence was mutable")
    connection.close()
