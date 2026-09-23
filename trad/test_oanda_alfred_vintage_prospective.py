import json
import sqlite3
import datetime as dt

import oanda_alfred_vintage_prospective as alfred


def config(tmp_path):
    path = tmp_path / "config.json"
    path.write_text(json.dumps({"schema_version":1,"cohort_start_utc":"2026-08-08T00:00:00Z",
        "source_contract_id":"test","api_key_environment":"TEST_FRED_KEY",
        "series":[{"series_id":"CPIAUCSL","economic_family":"cpi","currency":"USD"}]}))
    return path


def response(rows, realtime="2026-08-08"):
    observations=[{"date":date,"value":str(value),"realtime_start":realtime,"realtime_end":realtime} for date,value in rows]
    payload={"observations":observations}; raw=json.dumps(payload).encode(); return payload,raw


def test_material_collector_change_creates_new_cohort_id():
    specification={"cohort_start_utc":"2026-08-08T00:00:00Z"}
    first=alfred.cohort_contract(specification,collector_sha256="a"*64)
    second=alfred.cohort_contract(specification,collector_sha256="b"*64)
    assert first["cohort_id"] != second["cohort_id"]
    assert first["supersedes_cohort_id"].endswith(alfred.stable_hash(specification)[:16])


def test_repository_contract_includes_causal_ppi_vintage_fallback():
    specification = json.loads(alfred.CONFIG.read_text(encoding="utf-8"))
    series = {
        row["series_id"]: row["economic_family"]
        for row in specification["series"]
    }

    assert series["PPIFIS"] == "headline_ppi_final_demand"
    assert series["WPSFD49116"] == "core_ppi_less_food_energy_trade"
    assert specification["contract"]["same_day_intrahour_replay_eligible"] is False
    assert specification["provider_realtime_timezone"] == "America/Chicago"
    assert specification["contract"]["provider_query_day_is_america_chicago"] is True
    assert specification["contract"]["direction_policy"] == "abstain"
    assert specification["contract"]["execution_eligible"] is False


def test_repository_contract_covers_all_21_currency_legs_without_cross_frequency_pooling():
    specification = json.loads(alfred.CONFIG.read_text(encoding="utf-8"))
    currencies = {row["currency"] for row in specification["series"]}
    assert currencies == {
        "AUD", "CAD", "CHF", "CNH", "CZK", "DKK", "EUR", "GBP",
        "HKD", "HUF", "JPY", "MXN", "NOK", "NZD", "PLN", "SEK",
        "SGD", "THB", "TRY", "USD", "ZAR",
    }
    assert specification["contract"]["cross_frequency_comparison_forbidden"] is True
    context_only = {
        row["currency"]
        for row in specification["series"]
        if row.get("comparison_eligible") is False
    }
    assert context_only == {"HKD", "SGD", "THB"}


def test_provider_realtime_date_does_not_roll_ahead_at_utc_midnight():
    observed = dt.datetime(2026, 8, 16, 4, 30, tzinfo=dt.timezone.utc)
    assert alfred.provider_realtime_date(observed) == "2026-08-15"


def test_provider_realtime_date_rolls_on_provider_civil_midnight():
    observed = dt.datetime(2026, 8, 16, 5, 30, tzinfo=dt.timezone.utc)
    assert alfred.provider_realtime_date(observed) == "2026-08-16"


def test_repository_contract_has_explicit_cohort_lineage():
    specification = json.loads(alfred.CONFIG.read_text(encoding="utf-8"))
    contract = alfred.cohort_contract(specification, collector_sha256="c" * 64)
    assert contract["supersedes_cohort_id"] == specification["supersedes_cohort_id"]


def test_missing_key_fails_closed_without_database(tmp_path, monkeypatch):
    monkeypatch.delenv("TEST_FRED_KEY", raising=False)
    result=alfred.collect_once(config(tmp_path),tmp_path/"db.sqlite",tmp_path/"state.json",tmp_path/"report.md")
    assert result["status"] == "blocked_missing_fred_api_key"
    assert result["execution_eligible"] is False
    assert not (tmp_path/"db.sqlite").exists()


def test_bootstrap_is_never_prospective_and_poll_date_does_not_create_revision(tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_FRED_KEY","a"*32); calls=[response([("2026-06-01",100.0)],"2026-08-08"),response([("2026-06-01",100.0)],"2026-08-09")]
    fetch=lambda *_: calls.pop(0)
    paths=(config(tmp_path),tmp_path/"db.sqlite",tmp_path/"state.json",tmp_path/"report.md")
    first=alfred.collect_once(*paths,fetcher=fetch); second=alfred.collect_once(*paths,fetcher=fetch)
    assert first["totals"]["bootstrap_rows"] == 1
    assert first["totals"]["prospective_rows"] == 0
    assert second["totals"]["observations"] == 1


def test_later_new_value_and_revision_are_causal_only_at_first_seen(tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_FRED_KEY","b"*32)
    calls=[response([("2026-06-01",100.0)]),response([("2026-06-01",100.5),("2026-07-01",101.0)])]
    fetch=lambda *_: calls.pop(0); paths=(config(tmp_path),tmp_path/"db.sqlite",tmp_path/"state.json",tmp_path/"report.md")
    alfred.collect_once(*paths,fetcher=fetch); result=alfred.collect_once(*paths,fetcher=fetch)
    assert result["totals"]["observations"] == 3
    assert result["totals"]["prospective_rows"] == 2
    db=sqlite3.connect(tmp_path/"db.sqlite")
    rows=db.execute("SELECT observation_kind,prospective_eligible,same_day_intrahour_eligible FROM vintage_observations WHERE prospective_eligible=1 ORDER BY observation_kind").fetchall()
    assert rows == [("new_release_first_seen",1,0),("revision_first_seen",1,0)]
