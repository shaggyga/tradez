import datetime as dt
import json
import sqlite3

import oanda_direct_source_response as direct
from oanda_direct_source_simple_rules import source_rule

UTC = dt.timezone.utc


def local_news_provenance(**overrides):
    value = {
        "collector_contract_id": direct.COLLECTOR_CONTRACT_ID,
        "collector_cohort_id": direct.COLLECTOR_COHORT_ID,
        "observation_time_contract_id": direct.OBSERVATION_TIME_CONTRACT_ID,
        "observation_clock_trusted": True,
    }
    value.update(overrides)
    return value


def daily_rate_provenance(**overrides):
    value = {
        "collector_cohort_id": direct.DAILY_RATE_CLOCK_BOUND_COHORT_ID,
        "observation_time_contract_id": direct.OBSERVATION_TIME_CONTRACT_ID,
        "observation_clock_trusted": True,
    }
    value.update(overrides)
    return value


def test_daily_rate_revision_uses_new_immutable_cohort():
    assert direct.COHORT == (
        "direct_source_response_v20_clock_v4_provenance_20260817"
    )
    assert direct.PARENT_COHORT == "direct_source_response_v19_clock_v4_20260817"
    assert direct.COHORT != direct.PARENT_COHORT


def _canonical_edge_fixture(path):
    db = sqlite3.connect(path)
    db.executescript(
        """
        CREATE TABLE canonical_economic_outcome_labels (
            event_id TEXT NOT NULL,
            horizon_sec INTEGER NOT NULL,
            first_evidence_run_id TEXT NOT NULL,
            family TEXT NOT NULL,
            instrument TEXT NOT NULL,
            direction TEXT NOT NULL,
            entry_time TEXT NOT NULL,
            executable_return_pips REAL NOT NULL,
            modeled_after_cost_pips REAL NOT NULL,
            label_json TEXT NOT NULL,
            PRIMARY KEY(event_id,horizon_sec)
        );
        CREATE TRIGGER canonical_economic_labels_no_update
        BEFORE UPDATE ON canonical_economic_outcome_labels
        BEGIN SELECT RAISE(ABORT, 'immutable'); END;
        CREATE TRIGGER canonical_economic_labels_no_delete
        BEFORE DELETE ON canonical_economic_outcome_labels
        BEGIN SELECT RAISE(ABORT, 'immutable'); END;
        """
    )
    return db


def _insert_canonical_label(db, event_id, horizon, label):
    db.execute(
        "INSERT INTO canonical_economic_outcome_labels VALUES (?,?,?,?,?,?,?,?,?,?)",
        (
            event_id, horizon, "run", "family", "EUR_USD", "buy",
            "2026-08-28T12:00:00+00:00", 1.0, 0.5,
            json.dumps(label, sort_keys=True),
        ),
    )


def test_canonical_target_census_extends_append_only_highwater_exactly(tmp_path, monkeypatch):
    path = tmp_path / "edge.sqlite"
    db = _canonical_edge_fixture(path)
    _insert_canonical_label(
        db, "event-1", 300,
        {"max_favorable_pips": 2.0, "max_adverse_pips": -1.0},
    )
    _insert_canonical_label(
        db, "event-1", 900,
        {"best_alternative_net_pips": 1.0},
    )
    db.commit()
    monkeypatch.setattr(direct, "_CENSUS_CACHE", None)
    monkeypatch.setattr(direct, "_CENSUS_REFRESH_MONOTONIC", 0.0)
    monkeypatch.setattr(direct, "OUTPUT", tmp_path / "missing.json")
    first = direct.canonical_target_census(path)
    assert first["refresh_mode"] == "full_bootstrap"
    assert first["row_count"] == 2
    assert first["forecast_count"] == 1
    assert first["horizon_values"] == [300, 900]
    assert first["mfe_rows"] == 1
    assert first["mae_rows"] == 1
    assert first["rotation_rows"] == 1

    _insert_canonical_label(
        db, "event-1", 1800,
        {"max_favorable_pips": 3.0, "best_alternative_net_pips": 2.0},
    )
    _insert_canonical_label(
        db, "event-2", 300,
        {"max_adverse_pips": -2.0},
    )
    db.commit()
    monkeypatch.setattr(direct, "_CENSUS_REFRESH_MONOTONIC", 0.0)
    second = direct.canonical_target_census(path)
    assert second["refresh_mode"] == "incremental_append_only"
    assert second["row_count"] == 4
    assert second["forecast_count"] == 2
    assert second["horizon_values"] == [300, 900, 1800]
    assert second["mfe_rows"] == 2
    assert second["mae_rows"] == 2
    assert second["rotation_rows"] == 2
    db.close()


def test_canonical_target_census_uses_warm_cache_without_rescan(tmp_path, monkeypatch):
    path = tmp_path / "edge.sqlite"
    db = _canonical_edge_fixture(path)
    _insert_canonical_label(db, "event-1", 300, {})
    db.commit()
    db.close()
    monkeypatch.setattr(direct, "_CENSUS_CACHE", None)
    monkeypatch.setattr(direct, "_CENSUS_REFRESH_MONOTONIC", 0.0)
    monkeypatch.setattr(direct, "OUTPUT", tmp_path / "missing.json")
    first = direct.canonical_target_census(path)
    assert first["cache_state"] == "refreshed"

    def unexpected_connect(*_args, **_kwargs):
        raise AssertionError("warm census cache reopened the evidence database")

    monkeypatch.setattr(direct.sqlite3, "connect", unexpected_connect)
    cached = direct.canonical_target_census(path)
    assert cached["cache_state"] == "cached"
    assert cached["row_count"] == 1


def test_supplied_replay_time_cannot_write_prospective_evidence(tmp_path):
    observed = dt.datetime(2026, 8, 17, 10, 0, tzinfo=UTC)
    payload = direct.run_once(
        macro_path=tmp_path / "missing_macro.sqlite",
        quotes_path=tmp_path / "missing_quotes.json",
        rates_path=tmp_path / "missing_rates.json",
        db_path=tmp_path / "response.sqlite",
        output=tmp_path / "response.json",
        report=tmp_path / "response.md",
        observed=observed,
        rules_path=tmp_path / "missing_rules.json",
        treasury_db_path=tmp_path / "missing_treasury.sqlite",
        daily_rates_path=tmp_path / "missing_daily_rates.json",
        opportunity_path=tmp_path / "missing_opportunity.sqlite",
        opportunity_state_path=tmp_path / "missing_opportunity.json",
        internal_expectation_path=tmp_path / "missing_expectations.sqlite",
    )
    assert payload["status"] == "blocked_clock_integrity"
    assert payload["new_source_observations"] == 0
    assert payload["new_response_targets"] == 0
    assert payload["sampled_targets"] == 0
    assert payload["matured_targets"] == 0


def test_internal_expectation_is_pre_release_and_never_market_consensus(tmp_path):
    path = tmp_path / "expectations.sqlite"
    db = sqlite3.connect(path)
    db.execute(
        """CREATE TABLE expectation_forecasts(
             forecast_id TEXT,cohort_id TEXT,issued_utc TEXT,expires_utc TEXT,
             expected_value REAL,model_name TEXT,training_episode_count INTEGER,
             training_fingerprint_sha256 TEXT,currency TEXT,event_series_id TEXT)"""
    )
    db.execute(
        "INSERT INTO expectation_forecasts VALUES (?,?,?,?,?,?,?,?,?,?)",
        (
            "forecast-1",
            "cohort-1",
            "2026-08-16T10:00:00+00:00",
            "2026-09-16T10:00:00+00:00",
            2.1,
            "last_observation",
            3,
            "training-hash",
            "EUR",
            "official_cpi",
        ),
    )
    db.commit()
    db.close()
    before = direct.internal_expectation_for_release(
        path, "EUR", "official_cpi", dt.datetime(2026, 8, 16, 9, tzinfo=UTC)
    )
    after = direct.internal_expectation_for_release(
        path, "EUR", "official_cpi", dt.datetime(2026, 8, 16, 11, tzinfo=UTC)
    )
    assert before["available"] is False
    assert after["available"] is True
    assert after["expected_value"] == 2.1
    assert after["market_consensus"] is False
    assert after["causal_market_consensus"] is False


def test_direct_release_horizons_include_delayed_m30_and_h2_response():
    assert direct.HORIZONS == (60, 300, 900, 1800, 3600, 7200, 14400, 86400)


def test_source_native_period_change_rule_is_explicit_and_shadow_only():
    rule = source_rule(
        {
            "event_series_id": "statcan_wholesale_sales_mom",
            "actual_value": 2.8,
            "previous_value": None,
            "consensus_value": None,
        },
        {
            "series": {
                "statcan_wholesale_sales_mom": {
                    "polarity": 1,
                    "actual_is_period_change": True,
                    "meaning": "positive activity supports CAD",
                }
            }
        },
    )
    assert rule["direction"] == "strengthen"
    assert rule["basis"] == "source_native_period_change"
    assert rule["raw_delta"] == 2.8


def test_atomic_publish_retries_transient_windows_replace_denial(tmp_path, monkeypatch):
    target = tmp_path / "state.json"
    real_replace = direct.os.replace
    attempts = []

    def flaky_replace(source, destination):
        attempts.append((source, destination))
        if len(attempts) == 1:
            raise PermissionError("transient target lock")
        return real_replace(source, destination)

    monkeypatch.setattr(direct.os, "replace", flaky_replace)
    monkeypatch.setattr(direct.time, "sleep", lambda _: None)
    direct.atomic_json(target, {"ok": True})
    assert json.loads(target.read_text(encoding="utf-8")) == {"ok": True}
    assert len(attempts) == 2
    assert not list(tmp_path.glob("*.tmp"))


def test_pair_selection_uses_lowest_cost_and_three_legs():
    quotes = {}
    for index, pair in enumerate(["EUR_USD", "EUR_GBP", "EUR_JPY", "EUR_CHF"]):
        quotes[pair] = {"spread_pips": index + 1, "fresh": True}
    rows = direct.select_pairs("EUR", quotes)
    assert [row[0] for row in rows] == ["EUR_USD", "EUR_GBP", "EUR_JPY"]


def test_currency_orientation_is_base_positive_quote_negative():
    quotes = {"EUR_USD": {"spread_pips": 1, "fresh": True}, "USD_EUR": {"spread_pips": 2, "fresh": True}}
    rows = {row[0]: row[2] for row in direct.select_pairs("EUR", quotes)}
    assert rows == {"EUR_USD": 1, "USD_EUR": -1}


def test_pre_event_opportunity_ranker_uses_clearance_not_price_direction():
    quotes = {
        "EUR_USD": {"spread_pips": 1.0, "fresh": True},
        "EUR_GBP": {"spread_pips": 1.2, "fresh": True},
        "EUR_JPY": {"spread_pips": 1.1, "fresh": True},
    }
    opportunities = {
        ("EUR_USD", 300): {"predicted_clear_probability": .4, "predicted_magnitude_pips": 2., "modeled_entry_cost_pips": 1.2, "predicted_direction": 1},
        ("EUR_GBP", 300): {"predicted_clear_probability": .8, "predicted_magnitude_pips": 3., "modeled_entry_cost_pips": 1.4, "predicted_direction": -1},
        ("EUR_JPY", 300): {"predicted_clear_probability": .6, "predicted_magnitude_pips": 4., "modeled_entry_cost_pips": 1.3, "predicted_direction": 1},
    }
    rows = direct.select_opportunity_pairs("EUR", quotes, opportunities, 300)
    assert [row[0] for row in rows] == ["EUR_GBP", "EUR_JPY", "EUR_USD"]
    assert rows[0][3]["predicted_direction"] == -1


def test_pre_event_loader_rejects_old_model_and_post_release_forecast(tmp_path):
    path = tmp_path / "opportunity.sqlite"
    db = sqlite3.connect(path)
    db.execute("""CREATE TABLE forecasts (
        forecast_id TEXT,cohort_id TEXT,issued_at_utc TEXT,instrument TEXT,
        horizon_sec INTEGER,predicted_clear_probability REAL,
        predicted_magnitude_pips REAL,modeled_entry_cost_pips REAL,
        predicted_direction INTEGER)""")
    db.executemany("INSERT INTO forecasts VALUES (?,?,?,?,?,?,?,?,?)", [
        ("valid", "executable_opportunity_ranking_v2_20260814.collector.x", "2026-08-14T11:59:00Z", "EUR_USD", 300, .7, 3., 1.2, 1),
        ("old", "executable_opportunity_ranking_v1_20260808.collector.x", "2026-08-14T11:59:30Z", "EUR_USD", 300, .9, 9., 1.2, -1),
        ("future", "executable_opportunity_ranking_v2_20260814.collector.x", "2026-08-14T12:00:01Z", "EUR_GBP", 300, .9, 9., 1.2, -1),
    ])
    db.commit(); db.close()
    rows = direct.load_pre_event_opportunities(
        path,
        dt.datetime(2026, 8, 14, 12, 0, tzinfo=UTC),
        "executable_opportunity_ranking_v2_20260814.collector.x",
    )
    assert set(rows) == {("EUR_USD", 300)}
    assert rows[("EUR_USD", 300)]["forecast_id"] == "valid"


def test_stale_quotes_are_not_loaded(tmp_path):
    observed = dt.datetime(2026,8,8,12,0,tzinfo=UTC)
    path = tmp_path / "quotes.json"
    path.write_text(json.dumps({"quotes":{"EUR_USD":{"bid":1.0,"ask":1.0001,"pip":.0001,"time":"2026-08-08T11:00:00Z"}}}))
    assert direct.load_quotes(path, observed)["EUR_USD"]["fresh"] is False


def test_exact_horizon_target_matures_and_reports_both_directions(tmp_path):
    db = direct.open_db(tmp_path / "response.sqlite")
    entry = dt.datetime(2026,8,8,12,0,tzinfo=UTC)
    payload = json.dumps({})
    db.execute("INSERT INTO source_observations VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("s",direct.COHORT,1,"r","official","e","cpi","EUR",direct.iso(entry),direct.iso(entry),2.0,1.0,None,1.0,None,None,None,0,"source_not_connected","prospective_entry_ready",payload))
    db.execute("INSERT INTO response_targets VALUES (?,?,?,?,?,?,?,?,?,?,?,?,0,0,0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,?)",
        ("t","s","EUR_USD",1,60,direct.iso(entry),1.0,1.0001,1.00005,.0001,1.0,"pending",payload)); db.commit()
    quote_time = entry + dt.timedelta(seconds=60)
    quotes={"EUR_USD":{"bid":1.0003,"ask":1.0004,"mid":1.00035,"pip":.0001,"time":direct.iso(quote_time),"fresh":True}}
    _, matured = direct.update_targets(db,quotes,quote_time)
    assert matured == 1
    row=db.execute("select currency_return_pips,strengthening_after_cost_pips,weakening_after_cost_pips,movement_cleared_cost from response_targets").fetchone()
    assert round(row[0],6)==3.0 and round(row[1],6)==2.0 and round(row[2],6)==-4.0 and row[3]==1


def test_summary_deduplicates_release_revisions_without_deleting_provenance(tmp_path):
    db = direct.open_db(tmp_path / "response.sqlite")
    values = (direct.COHORT,1,"same-release","official","e","cpi","EUR","2026-08-08T12:00:00+00:00","2026-08-08T12:00:00+00:00",2.0,1.0,None,1.0,None,None,None,0,"missing")
    for suffix,row_id in [("a",1),("b",2)]:
        row=list(values); row[1]=row_id
        db.execute("INSERT INTO source_observations VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",(suffix,*row,"stale_at_first_observation","{}"))
    db.commit(); summary=direct.summarize(db)
    assert summary["independent_release_episodes"] == 1
    assert summary["source_observation_states"]["duplicate_release_revision_no_new_episode"] == 1
    assert db.execute("select count(*) from source_observations").fetchone()[0] == 2


def test_matured_target_records_simple_rule_and_flipped_control(tmp_path):
    db = direct.open_db(tmp_path / "response.sqlite")
    entry = dt.datetime(2026,8,8,12,0,tzinfo=UTC)
    payload = json.dumps({"simple_direction_rule":{"direction":"strengthen"}})
    db.execute("INSERT INTO source_observations VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("s",direct.COHORT,1,"r","official","e","retail_sales","EUR",direct.iso(entry),direct.iso(entry),2.0,1.0,None,1.0,None,None,None,0,"source_not_connected","prospective_entry_ready",payload))
    db.execute("INSERT INTO response_targets VALUES (?,?,?,?,?,?,?,?,?,?,?,?,0,0,0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,?)",
        ("t","s","EUR_USD",1,300,direct.iso(entry),1.0,1.0001,1.00005,.0001,1.0,"pending",payload)); db.commit()
    quote_time = entry + dt.timedelta(seconds=300)
    quotes={"EUR_USD":{"bid":1.0003,"ask":1.0004,"mid":1.00035,"pip":.0001,"time":direct.iso(quote_time),"fresh":True}}
    direct.update_targets(db,quotes,quote_time)
    outcome=json.loads(db.execute("select payload_json from response_targets").fetchone()[0])["outcome"]
    assert round(outcome["simple_rule_after_cost_pips"],6)==2.0
    assert round(outcome["flipped_negative_control_pips"],6)==-4.0
    assert outcome["simple_rule_direction_hit"] is True


def test_slow_treasury_bootstrap_is_context_only(tmp_path):
    path = tmp_path / "treasury.sqlite"
    db = sqlite3.connect(path)
    db.execute("""CREATE TABLE rate_observations (
        observation_id TEXT, cohort_id TEXT, observed_utc TEXT, yield_date TEXT,
        two_year_pct REAL, ten_year_pct REAL, curve_2s10s_bps REAL,
        observation_version INTEGER, observation_kind TEXT,
        bootstrap_current_view INTEGER, prospective_eligible INTEGER,
        direction_policy TEXT)""")
    db.execute("INSERT INTO rate_observations VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
               ("boot","c","2026-08-08T12:00:00Z","2026-08-07",3.7,4.2,50.0,1,
                "bootstrap_current_view",1,0,"abstain"))
    db.commit(); db.close()
    context = direct.load_slow_treasury_context(path)
    assert context["state"] == "bootstrap_or_revision_context_only"
    assert context["causal_daily_feature"] is False
    assert context["intraday_rate_confirmation"] is False


def test_slow_treasury_new_date_can_be_daily_but_not_intraday_context(tmp_path):
    path = tmp_path / "treasury.sqlite"
    db = sqlite3.connect(path)
    db.execute("""CREATE TABLE rate_observations (
        observation_id TEXT, cohort_id TEXT, observed_utc TEXT, yield_date TEXT,
        two_year_pct REAL, ten_year_pct REAL, curve_2s10s_bps REAL,
        observation_version INTEGER, observation_kind TEXT,
        bootstrap_current_view INTEGER, prospective_eligible INTEGER,
        direction_policy TEXT)""")
    db.execute("INSERT INTO rate_observations VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
               ("new","c","2026-08-10T22:00:00Z","2026-08-10",3.6,4.1,50.0,1,
                "new_yield_date_first_observed",0,1,"abstain"))
    db.commit(); db.close()
    context = direct.load_slow_treasury_context(path)
    assert context["state"] == "prospective_daily_context_available"
    assert context["causal_daily_feature"] is True
    assert context["intraday_rate_confirmation"] is False


def test_direct_source_uses_consensus_already_causally_joined_to_release(tmp_path):
    source = sqlite3.connect(":memory:")
    source.execute("""CREATE TABLE macro_release_revisions (
        row_id INTEGER, revision_id TEXT, release_key TEXT, source_event_id TEXT,
        causal_known_utc TEXT, event_series_id TEXT, event_name TEXT,
        currencies_json TEXT, reference_period TEXT, unit TEXT,
        actual_value REAL, previous_value REAL, revised_previous_value REAL,
        source_id TEXT, source_name TEXT, source_url TEXT,
        source_verified INTEGER, source_direct INTEGER, payload_sha256 TEXT,
        consensus_value REAL, known_before_recorded_timestamp INTEGER,
        payload_json TEXT)""")
    known = dt.datetime(2026, 8, 12, 12, tzinfo=UTC)
    source.execute(
        "INSERT INTO macro_release_revisions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (1, "rev", "official-release-key", "event", direct.iso(known), "US_CPI", "CPI",
         '["USD"]', "Jul", "%", 3.0, 2.7, None, "bls", "BLS",
         "https://www.bls.gov", 1, 1, "sha", 2.8, 1,
         json.dumps(local_news_provenance())),
    )
    target = direct.open_db(tmp_path / "response.sqlite")
    quotes = {
        "EUR_USD": {"bid": 1.1, "ask": 1.1001, "mid": 1.10005, "pip": 0.0001,
                    "spread_pips": 1.0, "time": direct.iso(known), "fresh": True}
    }
    observations, targets = direct.ingest_macro(
        source, target, quotes, known, "source_not_connected", {}
    )
    assert observations == 1
    assert targets == len(direct.HORIZONS)
    row = target.execute(
        "SELECT consensus_value,surprise_raw,consensus_causal FROM source_observations"
    ).fetchone()
    assert row[0] == 2.8 and round(row[1], 8) == 0.2 and row[2] == 1
    source.close()
    target.close()


def test_unbound_legacy_macro_source_cannot_open_v19_targets(tmp_path):
    source = sqlite3.connect(":memory:")
    source.execute("""CREATE TABLE macro_release_revisions (
        row_id INTEGER, revision_id TEXT, release_key TEXT, source_event_id TEXT,
        causal_known_utc TEXT, event_series_id TEXT, event_name TEXT,
        currencies_json TEXT, reference_period TEXT, unit TEXT,
        actual_value REAL, previous_value REAL, revised_previous_value REAL,
        source_id TEXT, source_name TEXT, source_url TEXT,
        source_verified INTEGER, source_direct INTEGER, payload_sha256 TEXT,
        consensus_value REAL, known_before_recorded_timestamp INTEGER,
        payload_json TEXT)""")
    known = dt.datetime(2026, 8, 17, 10, tzinfo=UTC)
    source.execute(
        "INSERT INTO macro_release_revisions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            1, "legacy-rev", "legacy-release", "legacy-event",
            direct.iso(known), "JP_POLICY", "BOJ policy", '["JPY"]',
            "current", "%", 0.75, 0.5, None, "boj", "BOJ",
            "https://www.boj.or.jp", 1, 1, "sha", None, 0,
            json.dumps(
                local_news_provenance(
                    collector_contract_id="local_news_incremental_source_commit_v38",
                    collector_cohort_id="local_news_incremental_source_commit_v38",
                    observation_clock_trusted=False,
                )
            ),
        ),
    )
    target = direct.open_db(tmp_path / "response.sqlite")
    quotes = {
        "USD_JPY": {
            "bid": 150.0, "ask": 150.01, "mid": 150.005, "pip": 0.01,
            "spread_pips": 1.0, "time": direct.iso(known), "fresh": True,
        }
    }
    observations, targets = direct.ingest_macro(
        source, target, quotes, known, "source_not_connected", {}
    )
    assert observations == 1
    assert targets == 0
    state, feature_text = target.execute(
        "SELECT eligibility_state,feature_json FROM source_observations"
    ).fetchone()
    assert state == "unbound_source_clock_provenance"
    assert json.loads(feature_text)["source_provenance_bound"] is False
    source.close()
    target.close()


def test_inferred_publication_clock_is_retained_but_never_opens_targets(tmp_path):
    source = sqlite3.connect(":memory:")
    source.execute("""CREATE TABLE macro_release_revisions (
        row_id INTEGER, revision_id TEXT, release_key TEXT, source_event_id TEXT,
        causal_known_utc TEXT, event_series_id TEXT, event_name TEXT,
        currencies_json TEXT, reference_period TEXT, unit TEXT,
        actual_value REAL, previous_value REAL, revised_previous_value REAL,
        source_id TEXT, source_name TEXT, source_url TEXT,
        source_verified INTEGER, source_direct INTEGER, payload_sha256 TEXT,
        consensus_value REAL, known_before_recorded_timestamp INTEGER,
        payload_json TEXT)""")
    known = dt.datetime(2026, 8, 16, 20, tzinfo=UTC)
    source.execute(
        "INSERT INTO macro_release_revisions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (1, "rev-inferred", "release-inferred", "event", direct.iso(known),
         "official_policy_rate", "Policy rate", '["JPY"]', "current", "%",
         0.5, 0.25, None, "boj", "BOJ", "https://www.boj.or.jp", 1, 1,
         "sha", None, 0,
         json.dumps({
             **local_news_provenance(),
             "published_time_inferred": True,
         })),
    )
    target = direct.open_db(tmp_path / "response.sqlite")
    quotes = {
        "USD_JPY": {"bid": 150.0, "ask": 150.01, "mid": 150.005,
                    "pip": 0.01, "spread_pips": 1.0,
                    "time": direct.iso(known), "fresh": True}
    }
    observations, targets = direct.ingest_macro(
        source, target, quotes, known, "source_not_connected", {}
    )
    assert observations == 1
    assert targets == 0
    state, feature_text = target.execute(
        "SELECT eligibility_state,feature_json FROM source_observations"
    ).fetchone()
    assert state == "inferred_publication_clock_not_prospective"
    assert json.loads(feature_text)["published_time_inferred"] is True
    source.close()
    target.close()


def test_bootstrap_revision_invalidates_earlier_targets_without_deleting_them(tmp_path):
    target = direct.open_db(tmp_path / "response.sqlite")
    common = (
        "cohort", 1, "release-key", "source", "event", "series", "USD",
        "2026-08-14T15:00:00+00:00", "2026-08-14T15:00:01+00:00",
        51.0, 55.2, None, -4.2, None, None, None, 0,
        "source_not_connected",
    )
    target.execute(
        "INSERT INTO source_observations VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("old-ready", *common, "prospective_entry_ready", "{}"),
    )
    target.execute(
        "INSERT INTO source_observations VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "bootstrap-proof", *common,
            "bootstrap_context_not_prospective",
            json.dumps({"source_listing_bootstrap": True}),
        ),
    )
    target.execute(
        """INSERT INTO response_targets VALUES
           (?,?,?,?,?,?,?,?,?,?,?,?,0,0,0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,?)""",
        (
            "target", "old-ready", "EUR_USD", -1, 300,
            "2026-08-14T15:00:01+00:00", 1.1, 1.1001, 1.10005,
            0.0001, 1.0, "pending", "{}",
        ),
    )
    target.commit()
    observations, targets = direct.invalidate_bootstrap_release_targets(target)
    assert observations == 1
    assert targets == 1
    assert {
        row[0] for row in target.execute(
            "SELECT DISTINCT eligibility_state FROM source_observations"
        )
    } == {"bootstrap_context_not_prospective"}
    assert target.execute("SELECT status FROM response_targets").fetchone()[0] == (
        "invalid_bootstrap_context"
    )
    direct.summarize(target)
    assert {
        row[0] for row in target.execute(
            "SELECT DISTINCT eligibility_state FROM source_observations"
        )
    } == {"bootstrap_context_not_prospective"}
    target.close()


def test_fresh_prospective_daily_rate_creates_only_h4_h24_shadow_targets(tmp_path):
    target = direct.open_db(tmp_path / "response.sqlite")
    observed = dt.datetime(2026, 8, 12, 12, tzinfo=UTC)
    payload = {
        "currencies": {
            "USD": {
                "observation_id": "usd-2y-2026-08-12", "observed_utc": direct.iso(observed),
                "rate_date": "2026-08-12", "rate_pct": 4.2, "change_bps_1d": 5.0,
                "prospective_eligible": True, "source_id": "us_treasury_2y",
                "provider": "Treasury", **daily_rate_provenance(),
            },
            "EUR": {
                "observation_id": "bootstrap-eur", "observed_utc": direct.iso(observed),
                "rate_date": "2026-08-12", "rate_pct": 2.7, "change_bps_1d": 2.0,
                "prospective_eligible": False, "source_id": "ecb_2y", "provider": "ECB",
                **daily_rate_provenance(),
            },
        }
    }
    quotes = {
        "EUR_USD": {"bid": 1.1, "ask": 1.1001, "mid": 1.10005, "pip": 0.0001,
                    "spread_pips": 1.0, "time": direct.iso(observed), "fresh": True},
        "USD_JPY": {"bid": 150.0, "ask": 150.01, "mid": 150.005, "pip": 0.01,
                    "spread_pips": 1.0, "time": direct.iso(observed), "fresh": True},
    }
    observations, targets = direct.ingest_daily_rate_context(payload, target, quotes, observed)
    assert observations == 2
    assert targets == 4
    assert {row[0] for row in target.execute("SELECT DISTINCT horizon_sec FROM response_targets")} == {14400, 86400}
    usd = json.loads(target.execute(
        "SELECT feature_json FROM source_observations WHERE currency='USD'"
    ).fetchone()[0])
    assert usd["simple_direction_rule"]["direction"] == "strengthen"
    assert usd["intraday_rate_confirmation"] is False
    eur_state = target.execute(
        "SELECT eligibility_state FROM source_observations WHERE currency='EUR'"
    ).fetchone()[0]
    assert eur_state == "not_prospective_source"
    target.close()


def test_stale_daily_rate_never_creates_retroactive_entry(tmp_path):
    target = direct.open_db(tmp_path / "response.sqlite")
    observed = dt.datetime(2026, 8, 12, 12, tzinfo=UTC)
    payload = {"currencies": {"USD": {
        "observation_id": "old", "observed_utc": direct.iso(observed - dt.timedelta(hours=2)),
        "rate_date": "2026-08-11", "rate_pct": 4.2, "change_bps_1d": 5.0,
        "prospective_eligible": True, "source_id": "treasury", "provider": "Treasury",
        **daily_rate_provenance(),
    }}}
    quotes = {"EUR_USD": {
        "bid": 1.1, "ask": 1.1001, "mid": 1.10005, "pip": 0.0001,
        "spread_pips": 1.0, "time": direct.iso(observed), "fresh": True,
    }}
    observations, targets = direct.ingest_daily_rate_context(payload, target, quotes, observed)
    assert observations == 1 and targets == 0
    assert target.execute("SELECT eligibility_state FROM source_observations").fetchone()[0] == "stale_at_first_observation"
    target.close()


def test_legacy_daily_rate_without_clock_provenance_is_context_only(tmp_path):
    target = direct.open_db(tmp_path / "response.sqlite")
    observed = dt.datetime(2026, 8, 17, 10, tzinfo=UTC)
    payload = {"currencies": {"USD": {
        "observation_id": "legacy-daily-rate",
        "observed_utc": direct.iso(observed),
        "rate_date": "2026-08-17", "rate_pct": 4.2,
        "change_bps_1d": 5.0, "prospective_eligible": True,
        "source_id": "treasury", "provider": "Treasury",
    }}}
    quotes = {"EUR_USD": {
        "bid": 1.1, "ask": 1.1001, "mid": 1.10005, "pip": 0.0001,
        "spread_pips": 1.0, "time": direct.iso(observed), "fresh": True,
    }}
    observations, targets = direct.ingest_daily_rate_context(
        payload, target, quotes, observed
    )
    assert observations == 1 and targets == 0
    state, feature_text = target.execute(
        "SELECT eligibility_state,feature_json FROM source_observations"
    ).fetchone()
    assert state == "unbound_daily_rate_clock_provenance"
    assert json.loads(feature_text)["source_provenance_bound"] is False
    target.close()
