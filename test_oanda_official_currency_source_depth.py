from __future__ import annotations

import json
import re

import pytest

import oanda_currency_state_engine as currency_engine
import oanda_official_currency_source_depth as depth


def test_all_configured_link_patterns_compile() -> None:
    news = json.loads(depth.NEWS.read_text(encoding="utf-8"))
    for source in news["sources"]:
        for pattern in source.get("link_patterns") or []:
            re.compile(pattern)


def test_depth_contract_has_exact_21_currency_universe_and_known_sources() -> None:
    report = depth.build_report()
    assert report["currency_count"] == 21
    assert len(report["currencies"]) == 21
    assert all(row["policy_source_ids"] for row in report["currencies"])
    assert report["research_only"] is True
    assert report["execution_eligible"] is False
    assert report["currency_strength_weight"] == 0.0


def test_currency_state_engine_binds_complete_depth_as_zero_weight_input() -> None:
    refs = currency_engine.load_source_depth_reference(depth.CONFIG)
    assert refs["official_source_transport_complete"] is True
    assert refs["official_source_transport_currency_count"] == 21
    assert refs["official_source_transport_category_count"] == 7
    assert refs["official_source_currency_strength_weight"] == 0.0


def test_currency_state_engine_rejects_source_depth_that_claims_weight(tmp_path) -> None:
    payload = json.loads(depth.CONFIG.read_text(encoding="utf-8"))
    payload["currency_strength_weight"] = 0.01
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="zero strength weight"):
        currency_engine.load_source_depth_reference(bad)


def test_depth_report_truthfully_exposes_complete_transport_and_nonproof_semantics(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(depth, "REPORT_DIR", tmp_path)
    monkeypatch.setattr(depth, "JSON_REPORT", tmp_path / "current.json")
    monkeypatch.setattr(depth, "MD_REPORT", tmp_path / "current.md")
    report = depth.write_report()
    saved = json.loads((tmp_path / "current.json").read_text(encoding="utf-8"))
    assert saved["complete_transport_depth_count"] == 21
    assert all(not row["missing_categories"] for row in saved["currencies"])
    assert set(saved["category_currency_counts"].values()) == {21}
    assert saved["currency_strength_weight"] == 0.0
    assert saved["execution_eligible"] is False
    assert "transport only" in (tmp_path / "current.md").read_text(encoding="utf-8")
    assert report == saved


def test_supervisor_keeps_source_depth_report_fresh() -> None:
    supervisor = (depth.ROOT / "oanda_always_on_supervisor.ps1").read_text(
        encoding="utf-8"
    )
    assert '-Name "official_currency_source_depth"' in supervisor
    assert '"--interval-sec", "21600"' in supervisor
    assert "OFFICIAL_CURRENCY_SOURCE_DEPTH_CURRENT.json" in supervisor
