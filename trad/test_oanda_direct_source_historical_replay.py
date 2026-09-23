import datetime as dt
import hashlib
import json
import sqlite3

import oanda_direct_source_historical_replay as replay

UTC=dt.timezone.utc


def candle(minute,bid,ask):
    mid=(bid+ask)/2
    return {"time":dt.datetime(2026,8,6,14,minute,tzinfo=UTC),"bid_o":bid,"bid_h":bid+.0001,"bid_l":bid-.0001,
            "ask_o":ask,"ask_h":ask+.0001,"ask_l":ask-.0001,"mid_o":mid,"mid_h":mid+.0001,"mid_l":mid-.0001}


def test_quote_currency_orientation_is_inverted_and_cost_aware():
    event={"release_key":"r","event_series_id":"cpi","event_name":"CPI","source_time_utc":"2026-08-06T14:00:00+00:00",
           "currency":"USD","actual_value":2.0,"previous_value":1.0,"consensus_value":None}
    rows=replay.evaluate_pair(event,"EUR_USD",[candle(0,1.0000,1.0002),candle(1,.9996,.9998)])
    row=next(x for x in rows if x["horizon_sec"]==60)
    assert row["orientation"]==-1
    assert round(row["currency_return_pips"],6)==4.0
    assert round(row["strengthening_after_cost_pips"],6)==2.0
    assert row["proof_eligible"] is False


def test_all_68_pairs_cover_all_21_currency_legs():
    assert len(replay.INSTRUMENTS) == 68
    assert len(replay.PAIR_MAP) == 21
    assert replay.PAIR_MAP["CNH"] == ("USD_CNH",)
    assert "USD_THB" in replay.PAIR_MAP["THB"]


def test_venue_pip_map_is_used_for_huf_pair():
    event={"release_key":"r","event_series_id":"cpi","event_name":"CPI","source_time_utc":"2026-08-06T14:00:00+00:00",
           "currency":"HUF","actual_value":2.0,"previous_value":1.0,"consensus_value":None}
    candles=[
        {"time":dt.datetime(2026,8,6,14,0,tzinfo=UTC),"bid_o":400.00,"bid_h":400.01,"bid_l":399.99,
         "ask_o":400.02,"ask_h":400.03,"ask_l":400.01,"mid_o":400.01,"mid_h":400.02,"mid_l":400.00},
        {"time":dt.datetime(2026,8,6,14,1,tzinfo=UTC),"bid_o":399.90,"bid_h":399.91,"bid_l":399.89,
         "ask_o":399.92,"ask_h":399.93,"ask_l":399.91,"mid_o":399.91,"mid_h":399.92,"mid_l":399.90},
    ]
    row=next(x for x in replay.evaluate_pair(event,"EUR_HUF",candles) if x["horizon_sec"]==60)
    assert round(row["currency_return_pips"],6) == 10.0
    assert round(row["entry_spread_pips"],6) == 2.0


def test_same_currency_clock_bundle_counts_as_one_independent_episode():
    base={"currency":"USD","source_time_utc":"2026-08-06T14:00:00+00:00","horizon_sec":300,
          "instrument":"EUR_USD","currency_return_bps":1.0,"absolute_move_pips":2.0,
          "best_after_cost_pips":1.0,"movement_cleared_cost":True}
    rows=[{**base,"release_key":"headline"},{**base,"release_key":"core"}]
    result=replay.independent_episode_rows(rows)
    assert len(result)==1
    assert result[0]["pair_count"]==1
    assert result[0]["median_best_after_cost_pips"]==1.0


def test_initial_numeric_payload_uses_latest_corrected_identity():
    rows = [
        {
            "release_key": "uk_gdp",
            "actual_value": 0.4,
            "currencies_json": '["CAD", "GBP", "JPY"]',
        },
        {
            "release_key": "uk_gdp",
            "actual_value": 0.5,
            "currencies_json": '["GBP"]',
        },
    ]
    [(numeric, identity)] = replay.initial_numeric_with_latest_identity(rows)
    assert numeric["actual_value"] == 0.4
    assert identity["currencies_json"] == '["GBP"]'


def test_latest_verified_native_clock_repairs_historical_replay_only():
    first = {
        "source_reported_update_utc": "",
        "scheduled_utc": "",
        "payload_json": "{}",
    }
    latest = {
        **first,
        "payload_json": '{"source_native_published_utc":"2026-07-23T23:30:00+00:00"}',
    }
    stamp, basis = replay.source_clock(first, latest)
    assert replay.iso(stamp) == "2026-07-23T23:30:00+00:00"
    assert basis == "latest_verified_source_native_publication"


def test_inferred_poll_clock_is_not_a_release_clock():
    row = {
        "source_reported_update_utc": "2026-08-19T03:24:20+00:00",
        "scheduled_utc": "2026-08-19T03:24:20+00:00",
        "reference_period": "June quarter 2026",
        "payload_json": json.dumps({
            "first_seen_utc": "2026-08-19T03:24:20+00:00",
            "published_time_inferred": True,
        }),
    }
    stamp, basis = replay.source_clock(row, row)
    assert stamp is None
    assert basis == "inferred_collection_clock_not_release_time"


def test_collection_clock_equal_to_first_seen_is_not_source_native():
    row = {
        "source_reported_update_utc": "2026-08-02T21:14:28+00:00",
        "scheduled_utc": "2026-08-02T21:14:28+00:00",
        "reference_period": "June 2026",
        "payload_json": json.dumps({
            "first_seen_utc": "2026-08-02T21:14:28+00:00",
        }),
    }
    stamp, basis = replay.source_clock(row, row)
    assert stamp is None
    assert basis == "collection_clock_not_source_native"


def test_frozen_contract_clock_supersedes_inferred_poll_clock():
    row = {
        "source_reported_update_utc": "2026-08-11T07:04:00+00:00",
        "scheduled_utc": "2026-08-11T07:04:00+00:00",
        "reference_period": "2026M07",
        "payload_json": json.dumps({
            "first_seen_utc": "2026-08-11T07:04:00+00:00",
            "published_time_inferred": True,
        }),
    }
    stamp, basis = replay.source_clock(row, row, {
        "release_utc_by_reference": {"2026M07": "2026-08-11T07:00:00Z"},
        "clock_basis": "reviewed_archive_exact_clock_counterfactual_only",
    })
    assert replay.iso(stamp) == "2026-08-11T07:00:00+00:00"
    assert basis == "reviewed_archive_exact_clock_counterfactual_only"


def test_load_events_collapses_repeated_unclocked_snapshot_exclusions(tmp_path):
    database = tmp_path / "macro.sqlite"
    connection = sqlite3.connect(database)
    connection.execute(
        """CREATE TABLE macro_release_revisions(
            row_id INTEGER PRIMARY KEY, release_key TEXT, event_series_id TEXT,
            actual_value REAL, source_verified INTEGER, source_direct INTEGER,
            source_reported_update_utc TEXT, scheduled_utc TEXT, payload_json TEXT,
            currencies_json TEXT, reference_period TEXT, event_name TEXT,
            causal_known_utc TEXT, previous_value REAL, revised_previous_value REAL,
            consensus_value REAL, standardized_surprise REAL, unit TEXT,
            source_id TEXT, source_url TEXT
        )"""
    )
    for index, minute in enumerate((0, 5), start=1):
        stamp = f"2026-08-19T09:{minute:02d}:00+00:00"
        connection.execute(
            "INSERT INTO macro_release_revisions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                index, f"poll-{index}", "official_policy_rate", 7.0, 1, 1,
                stamp, stamp, json.dumps({
                    "first_seen_utc": stamp,
                    "published_time_inferred": True,
                }), '["ZAR"]', "current", "Policy rate", stamp, None,
                None, None, None, "percent", "official", "https://example.test/rate",
            ),
        )
    connection.commit()
    connection.close()
    source_config = tmp_path / "sources.json"
    source_config.write_text('{"sources":[]}', encoding="utf-8")
    valid, excluded = replay.load_events(
        database, source_config, tmp_path / "missing-overrides.json"
    )
    assert valid == []
    assert len(excluded) == 1
    assert excluded[0]["exclusion_reason"] == "inferred_collection_clock_not_release_time"


def test_load_events_collapses_repeated_keys_for_one_exact_release(tmp_path):
    database = tmp_path / "macro.sqlite"
    connection = sqlite3.connect(database)
    connection.execute(
        """CREATE TABLE macro_release_revisions(
            row_id INTEGER PRIMARY KEY, release_key TEXT, event_series_id TEXT,
            actual_value REAL, source_verified INTEGER, source_direct INTEGER,
            source_reported_update_utc TEXT, scheduled_utc TEXT, payload_json TEXT,
            currencies_json TEXT, reference_period TEXT, event_name TEXT,
            causal_known_utc TEXT, previous_value REAL, revised_previous_value REAL,
            consensus_value REAL, standardized_surprise REAL, unit TEXT,
            source_id TEXT, source_url TEXT
        )"""
    )
    for index in (1, 2):
        stamp = f"2026-08-19T09:0{index}:00+00:00"
        connection.execute(
            "INSERT INTO macro_release_revisions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                index, f"poll-{index}", "official_wpi", 3.2, 1, 1,
                stamp, stamp, json.dumps({
                    "first_seen_utc": stamp,
                    "published_time_inferred": True,
                }), '["AUD"]', "June 2026", "WPI", stamp, None,
                None, None, None, "percent", "official", "https://example.test/wpi",
            ),
        )
    connection.commit()
    connection.close()
    source_config = tmp_path / "sources.json"
    source_config.write_text(json.dumps({"sources": [{
        "source_id": "official",
        "verified": True,
        "direct": True,
        "currencies": ["AUD"],
        "series": [{
            "series_id": "official_wpi",
            "release_utc_by_reference": {"June 2026": "2026-08-19T01:30:00Z"},
        }],
    }]}), encoding="utf-8")
    valid, excluded = replay.load_events(
        database, source_config, tmp_path / "missing-overrides.json"
    )
    assert len(valid) == 1
    assert excluded == []
    assert valid[0]["source_time_utc"] == "2026-08-19T01:30:00+00:00"


def test_verified_series_contract_supplies_currency_and_exact_release_clock(tmp_path):
    path = tmp_path / "sources.json"
    path.write_text(json.dumps({"sources": [{
        "source_id": "bls_batch",
        "verified": True,
        "direct": True,
        "currencies": ["USD"],
        "source_contract_id": "frozen-contract",
        "series": [{
            "series_id": "JTS000000000000000JOL",
            "release_utc_by_reference": {
                "June 2026": "2026-08-04T14:00:00Z"
            },
        }],
    }]}), encoding="utf-8")
    contracts = replay.official_series_contracts(path)
    assert contracts["JTS000000000000000JOL"]["currency"] == "USD"
    assert contracts["JTS000000000000000JOL"]["release_utc_by_reference"] == {
        "June 2026": "2026-08-04T14:00:00Z"
    }


def test_live_source_contract_retains_official_abs_cpi_and_wpi_clocks():
    contracts = replay.official_series_contracts(replay.SOURCE_CONFIG)
    assert contracts["abs_australia_cpi_yoy"]["release_utc_by_reference"] == {
        "June 2026": "2026-07-29T01:30:00Z"
    }
    assert contracts["abs_australia_wage_price_index_yoy"]["release_utc_by_reference"] == {
        "June quarter 2026": "2026-08-19T01:30:00Z"
    }


def test_live_source_contract_retains_official_swiss_cpi_and_ppi_clocks():
    contracts = replay.official_series_contracts(replay.SOURCE_CONFIG)
    assert contracts["swiss_fso_cpi_mom"]["release_utc_by_reference"] == {
        "Juli 2026": "2026-08-03T06:30:00Z"
    }
    assert contracts["swiss_fso_producer_import_price_mom"]["release_utc_by_reference"] == {
        "Juli 2026": "2026-08-13T06:30:00Z"
    }


def test_live_source_contract_retains_remaining_official_release_clocks():
    contracts = replay.official_series_contracts(replay.SOURCE_CONFIG)
    expected = {
        "china_nbs_cpi_yoy": {"July 2026": "2026-08-09T01:30:00Z"},
        "china_nbs_ppi_yoy": {"July 2026": "2026-08-09T01:30:00Z"},
        "poland_gus_cpi_yoy": {"July 2026": "2026-08-13T08:00:00Z"},
        "poland_gus_real_gdp_qoq": {
            "Flash estimate of Gross Domestic Product in the 2nd quarter of 2026":
                "2026-08-13T08:00:00Z"
        },
        "thailand_headline_cpi_index": {"2026M07": "2026-08-05T03:00:00Z"},
        "hungary_headline_cpi_yoy": {"2026M07": "2026-08-07T06:30:00Z"},
    }
    for series_id, release_map in expected.items():
        assert contracts[series_id]["release_utc_by_reference"] == release_map
        assert contracts[series_id]["clock_evidence_url"].startswith("https://")


def test_live_exact_clock_event_retains_frozen_provenance():
    valid, _ = replay.load_events(
        replay.MACRO, replay.SOURCE_CONFIG, replay.CLOCK_OVERRIDES
    )
    china = next(row for row in valid if row["event_series_id"] == "china_nbs_cpi_yoy")
    assert china["source_contract_id"].endswith("exact_cpi_ppi_clock_20260824")
    assert "stats.gov.cn/sj/" in china["release_clock_evidence_url"]
    assert "09:30" in china["release_clock_evidence"]


def test_reviewed_clock_override_is_archive_only_and_exact(tmp_path):
    certificate = tmp_path / "review.json"
    certificate.write_text(json.dumps({
        "verdict": "register_disabled_research_only_exact_bytes",
        "registration": {"enabled": False, "execution_eligible": False},
    }), encoding="utf-8")
    path = tmp_path / "clock_overrides.json"
    path.write_text(json.dumps({
        "research_only": True,
        "execution_eligible": False,
        "evidence_class": "historical_availability_counterfactual_clock_repair",
        "series": [{
            "event_series_id": "czech_headline_cpi_yoy",
            "currency": "CZK",
            "source_contract_id": "reviewed-contract",
            "source_review_certificate": "review.json",
            "source_review_certificate_sha256": hashlib.sha256(certificate.read_bytes()).hexdigest(),
            "release_utc_by_reference": {"2026M07": "2026-08-11T07:00:00Z"},
            "direction": None,
            "consensus": None,
            "surprise": None,
            "proof_eligible": False,
            "execution_eligible": False,
        }],
    }), encoding="utf-8")
    contracts = replay.reviewed_historical_clock_overrides(path, tmp_path)
    assert contracts == {
        "czech_headline_cpi_yoy": {
            "currency": "CZK",
            "release_utc_by_reference": {"2026M07": "2026-08-11T07:00:00Z"},
            "source_contract_id": "reviewed-contract",
            "clock_basis": "reviewed_archive_exact_clock_counterfactual_only",
        }
    }


def test_clock_override_refuses_direction_or_execution(tmp_path):
    certificate = tmp_path / "review.json"
    certificate.write_text(json.dumps({
        "verdict": "register_disabled_research_only_exact_bytes",
        "registration": {"enabled": False, "execution_eligible": False},
    }), encoding="utf-8")
    base = {
        "research_only": True,
        "execution_eligible": False,
        "evidence_class": "historical_availability_counterfactual_clock_repair",
        "series": [{
            "event_series_id": "series",
            "currency": "USD",
            "source_contract_id": "contract",
            "source_review_certificate": "review.json",
            "source_review_certificate_sha256": hashlib.sha256(certificate.read_bytes()).hexdigest(),
            "release_utc_by_reference": {"period": "2026-08-11T07:00:00Z"},
            "direction": None,
            "consensus": None,
            "surprise": None,
            "proof_eligible": False,
            "execution_eligible": False,
        }],
    }
    path = tmp_path / "clock_overrides.json"
    base["series"][0]["direction"] = "long"
    path.write_text(json.dumps(base), encoding="utf-8")
    assert replay.reviewed_historical_clock_overrides(path, tmp_path) == {}
    base["series"][0]["direction"] = None
    base["series"][0]["execution_eligible"] = True
    path.write_text(json.dumps(base), encoding="utf-8")
    assert replay.reviewed_historical_clock_overrides(path, tmp_path) == {}


def test_clock_override_refuses_unreviewed_or_changed_certificate(tmp_path):
    certificate = tmp_path / "review.json"
    certificate.write_text(json.dumps({
        "verdict": "register_disabled_research_only_exact_bytes",
        "registration": {"enabled": False, "execution_eligible": False},
    }), encoding="utf-8")
    payload = {
        "research_only": True,
        "execution_eligible": False,
        "evidence_class": "historical_availability_counterfactual_clock_repair",
        "series": [{
            "event_series_id": "series",
            "currency": "USD",
            "source_contract_id": "contract",
            "source_review_certificate": "review.json",
            "source_review_certificate_sha256": "0" * 64,
            "release_utc_by_reference": {"period": "2026-08-11T07:00:00Z"},
            "direction": None,
            "consensus": None,
            "surprise": None,
            "proof_eligible": False,
            "execution_eligible": False,
        }],
    }
    path = tmp_path / "clock_overrides.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    assert replay.reviewed_historical_clock_overrides(path, tmp_path) == {}


def test_run_reuses_candles_for_same_currency_factor_clock(monkeypatch, tmp_path):
    base_event = {
        "release_key": "release-a",
        "event_series_id": "series-a",
        "event_name": "Release A",
        "source_time_utc": "2026-08-06T14:00:00+00:00",
        "currency": "CNH",
        "actual_value": 1.0,
        "previous_value": None,
        "consensus_value": None,
    }
    monkeypatch.setattr(
        replay,
        "load_events",
        lambda *_: ([base_event, {**base_event, "release_key": "release-b", "event_series_id": "series-b"}], []),
    )
    monkeypatch.setattr(replay, "readonly_price_token", lambda *_: ("token", "https://example.invalid"))
    calls = []

    def fake_fetch(*args):
        calls.append(args)
        control_0 = {**candle(0, 1.0004, 1.0006), "time": dt.datetime(2026, 8, 6, 12, 0, tzinfo=UTC)}
        control_1 = {**candle(1, 1.0002, 1.0004), "time": dt.datetime(2026, 8, 6, 12, 1, tzinfo=UTC)}
        return [control_0, control_1, candle(0, 1.0000, 1.0002), candle(1, 0.9998, 1.0000)]

    monkeypatch.setattr(replay, "fetch_candles", fake_fetch)
    result = replay.run(
        tmp_path / "macro.sqlite",
        tmp_path / "creds",
        tmp_path / "result.json",
        tmp_path / "result.md",
        tmp_path / "sources.json",
        tmp_path / "overrides.json",
    )
    assert len(calls) == 1
    assert result["candle_request_count"] == 1
    assert result["candle_cache_hits"] == 1
    assert result["candle_cache_entry_count"] == 1


def test_paired_magnitude_difference_matches_shifted_control_clock():
    event = [{"currency":"USD","source_time_utc":"2026-08-06T14:00:00+00:00",
              "horizon_sec":300,"median_absolute_move_pips":4.0,
              "median_absolute_currency_bps":3.0}]
    control = [{"currency":"USD","source_time_utc":"2026-08-06T12:00:00+00:00",
                "horizon_sec":300,"median_absolute_move_pips":1.5,
                "median_absolute_currency_bps":1.25}]
    paired = replay.paired_magnitude_differences(event,control)
    assert len(paired)==1
    assert paired[0]["event_minus_control_absolute_pips"]==2.5
    assert paired[0]["event_minus_control_absolute_currency_bps"]==1.75
    summary=replay.difference_interval([2.5,-0.5])
    assert summary["n"]==2
    assert summary["mean"]==1.0
    assert summary["positive_fraction"]==0.5


def test_episode_concentration_reports_leave_largest_result():
    result = replay.episode_concentration([
        {"currency":"NOK","source_time_utc":"a","event_minus_control_absolute_pips":10.0},
        {"currency":"USD","source_time_utc":"b","event_minus_control_absolute_pips":2.0},
        {"currency":"JPY","source_time_utc":"c","event_minus_control_absolute_pips":-1.0},
    ])
    assert result["largest_absolute_episode_currency"] == "NOK"
    assert round(result["largest_absolute_share"], 6) == round(10 / 13, 6)
    assert result["mean_excluding_largest_absolute_episode"] == 0.5
    normalized = replay.episode_concentration([
        {"currency":"NOK","source_time_utc":"a","normalized":1.0},
        {"currency":"USD","source_time_utc":"b","normalized":2.0},
        {"currency":"JPY","source_time_utc":"c","normalized":-1.0},
    ], "normalized")
    assert normalized["largest_absolute_episode_currency"] == "USD"


def test_one_minute_factor_confirmation_enters_after_confirmation_cost():
    event={"release_key":"r","event_series_id":"gdp","event_name":"GDP",
           "source_time_utc":"2026-08-06T14:00:00+00:00","currency":"USD",
           "actual_value":1.0,"previous_value":0.5,"consensus_value":None}
    candles=[
        candle(0,1.0000,1.0002),
        candle(1,0.9996,0.9998),
        candle(5,0.9992,0.9994),
    ]
    row=next(x for x in replay.evaluate_pair(event,"EUR_USD",candles) if x["horizon_sec"]==300)
    assert round(row["h1_confirmation_currency_return_pips"],6)==4.0
    assert round(row["post_confirmation_strengthening_after_cost_pips"],6)==2.0
    episode=replay.technical_confirmation_episode_rows([row])
    assert episode[0]["predicted_currency_direction"]=="strengthen"
    assert round(episode[0]["continuation_after_cost_pips"],6)==2.0


def test_confirmation_sweep_supports_delays_and_factor_abstention():
    event={"release_key":"r","event_series_id":"gdp","event_name":"GDP",
           "source_time_utc":"2026-08-06T14:00:00+00:00","currency":"USD",
           "actual_value":1.0,"previous_value":0.5,"consensus_value":None}
    candles=[
        candle(0,1.0000,1.0002),
        candle(1,0.9998,1.0000),
        candle(2,0.9995,0.9997),
        candle(5,0.9990,0.9992),
    ]
    row=next(x for x in replay.evaluate_pair(event,"EUR_USD",candles)
             if x["horizon_sec"]==300)
    assert round(row["confirmation_paths"]["120"]["currency_return_pips"],6)==5.0
    accepted=replay.technical_confirmation_episode_rows(
        [row], confirmation_delay_sec=120, minimum_abs_factor_bps=1.0
    )
    rejected=replay.technical_confirmation_episode_rows(
        [row], confirmation_delay_sec=120, minimum_abs_factor_bps=10.0
    )
    assert len(accepted)==1
    assert accepted[0]["confirmation_delay_sec"]==120
    assert rejected==[]
