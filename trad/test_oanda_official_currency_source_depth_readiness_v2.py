from __future__ import annotations

import json
import sqlite3
import sys

import oanda_official_currency_source_depth_readiness_v2 as readiness


def _cell(report: dict, currency: str, family: str) -> dict:
    return next(
        row
        for row in report["cells"]
        if row["currency"] == currency and row["event_family"] == family
    )


def test_report_has_exact_governed_universe_and_no_operational_authority() -> None:
    report = readiness.build_report()
    assert report["currency_count"] == 21
    assert report["expected_pair_count"] == 68
    assert report["event_family_count"] == 8
    assert report["currency_event_cell_count"] == 168
    assert report["research_only"] is True
    assert report["execution_eligible"] is False
    assert report["can_place_orders"] is False
    assert report["can_authorize"] is False
    assert report["can_promote"] is False
    assert report["currency_strength_weight"] == 0.0


def test_transport_does_not_self_certify_a_parser() -> None:
    report = readiness.build_report()
    aud_trade = _cell(report, "AUD", "trade")
    assert aud_trade["configured_transport_source_ids"] == ["abs_latest_releases"]
    assert aud_trade["structured_parser_source_ids"] == []
    usd_trade = _cell(report, "USD", "trade")
    assert "census_economic_indicators" in usd_trade["structured_parser_source_ids"]


def test_parser_scope_is_explicit_and_conservative() -> None:
    assert readiness.PARSER_FAMILIES_BY_SOURCE["swiss_fso_releases"] == ("inflation",)
    assert readiness.PARSER_FAMILIES_BY_SOURCE["ons_published_releases"] == (
        "inflation",
        "labour",
        "growth",
    )
    assert readiness.event_family({"category": "risk_off_geopolitical_or_financial"}) is None
    assert readiness.event_family({"event_series_id": "census_retail_sales_mom"}) == "growth"
    assert readiness.event_family({"event_series_id": "census_trade_balance"}) == "trade"


def test_current_mapper_reader_uses_only_snapshot_contract_and_version() -> None:
    snapshot = json.loads(readiness.FAST_MAPPING_SNAPSHOT.read_text(encoding="utf-8"))
    rows = readiness.load_current_mapping_rows()
    assert len(rows) == int(snapshot["counts"]["mappings"])
    assert len(rows) < int(snapshot["counts"]["retained_mappings_all_classifier_versions"])


def test_stage_counts_cannot_exceed_cell_universe_and_prospective_direction_is_separate() -> None:
    report = readiness.build_report()
    assert all(0 <= value <= 168 for value in report["stage_cell_counts"].values())
    assert report["stage_cell_counts"]["configured_transport"] == 168
    assert (
        report["stage_cell_counts"]["prospective_semantic_direction"]
        <= report["stage_cell_counts"]["semantic_direction"]
    )
    assert (
        report["observation_totals"]["prospective_semantic_direction_observations"]
        <= report["observation_totals"]["semantic_direction_observations"]
    )


def test_write_produces_new_v2_artifacts_without_touching_v1(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(readiness, "REPORT_DIR", tmp_path)
    monkeypatch.setattr(readiness, "JSON_REPORT", tmp_path / "current_v2.json")
    monkeypatch.setattr(readiness, "MD_REPORT", tmp_path / "current_v2.md")
    report = readiness.write_report()
    saved = json.loads((tmp_path / "current_v2.json").read_text(encoding="utf-8"))
    markdown = (tmp_path / "current_v2.md").read_text(encoding="utf-8")
    assert saved["schema_version"] == "official_currency_source_depth_readiness_v2"
    assert saved["stage_cell_counts"] == report["stage_cell_counts"]
    assert "Transport" in markdown
    assert "Matured outcome" in markdown
    assert "zero execution" in markdown


def test_main_interval_cycle_stops_without_restart_churn(monkeypatch) -> None:
    cycles: list[int] = []
    sleeps: list[float] = []
    clocks = iter((100.0, 100.0, 102.0))
    sample = {
        "currency_count": 21,
        "expected_pair_count": 68,
        "currency_event_cell_count": 168,
        "stage_cell_counts": {},
        "observation_totals": {},
    }

    def fake_write() -> dict:
        cycles.append(1)
        return sample

    monkeypatch.setattr(readiness, "write_report", fake_write)
    monkeypatch.setattr(readiness.time, "monotonic", lambda: next(clocks))
    monkeypatch.setattr(readiness.time, "sleep", lambda value: sleeps.append(value))
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "oanda_official_currency_source_depth_readiness_v2.py",
            "--interval-sec",
            "1",
            "--duration-sec",
            "2",
        ],
    )

    assert readiness.main() == 0
    assert len(cycles) == 2
    assert sleeps == [1.0]


def _create_causal_response_map(path) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.executescript(
            """
            CREATE TABLE source_event_observation (
                canonical_event_id TEXT PRIMARY KEY,
                currency TEXT NOT NULL,
                category TEXT NOT NULL,
                event_series_id TEXT NOT NULL,
                evidence_class TEXT NOT NULL,
                prospective_proof_eligible INTEGER NOT NULL
            );
            CREATE TABLE source_event_response (
                response_id TEXT PRIMARY KEY,
                canonical_event_id TEXT NOT NULL,
                maturity_state TEXT NOT NULL,
                response_timing_quality TEXT NOT NULL
            );
            """
        )
        events = [
            ("usd_one", "USD", "inflation_release", "cpi", "prospective_v1", 1),
            ("usd_two", "USD", "inflation_release", "ppi", "prospective_v1", 1),
            ("old", "USD", "inflation_release", "cpi", "preactivation_diagnostic", 0),
            ("not_proof", "USD", "inflation_release", "cpi", "prospective_v1", 0),
        ]
        connection.executemany(
            "INSERT INTO source_event_observation VALUES (?,?,?,?,?,?)", events
        )
        valid = "valid_canonical_ls_factor_and_bid_ask_paths"
        exact = "prospective_exact_entry_completed_m1_exit"
        connection.executemany(
            "INSERT INTO source_event_response VALUES (?,?,?,?)",
            [
                ("usd_one_h1", "usd_one", valid, exact),
                ("usd_one_h5", "usd_one", valid, exact),
                ("usd_two_h1", "usd_two", valid, exact),
                ("old_h1", "old", valid, exact),
                ("not_proof_h1", "not_proof", valid, exact),
            ],
        )
        connection.commit()
    finally:
        connection.close()


def test_causal_response_counts_distinct_events_not_horizons_and_excludes_preactivation(
    tmp_path,
) -> None:
    database = tmp_path / "causal.sqlite"
    _create_causal_response_map(database)
    present, counts = readiness.load_causal_response_event_counts(database)
    assert present is True
    assert counts == {("USD", "inflation"): 2}


def test_absent_causal_response_database_is_an_explicit_fallback(tmp_path) -> None:
    present, counts = readiness.load_causal_response_event_counts(
        tmp_path / "absent.sqlite"
    )
    assert present is False
    assert counts == {}


def test_report_prefers_new_causal_event_counts_over_legacy_mapping_ids() -> None:
    report = readiness.build_report(
        causal_outcome_counts={("USD", "inflation"): 2},
        causal_outcome_database_present=True,
        valid_outcome_mapping_ids=("legacy_would_otherwise_count",),
    )
    assert _cell(report, "USD", "inflation")["prospective_causal_outcomes"] == 2
    assert report["prospective_causal_outcome_source"].startswith(
        "causal_source_factor_response_map_v2"
    )
