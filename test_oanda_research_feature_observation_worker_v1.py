"""Owned synthetic inputs only; no strategy/model imports or network."""
import csv
from datetime import datetime, timedelta, timezone
import gzip
import json
from pathlib import Path
import sys
import re
from types import SimpleNamespace

import pytest

import oanda_research_feature_calculator_v1 as calculator
import oanda_research_feature_observation_worker_v1 as worker
from oanda_feature_observations_v1 import build_observation_frame
from oanda_feature_move_mapping_v1 import build_feature_move_map

NOW = datetime(2026, 9, 13, 16, 0, tzinfo=timezone.utc)


def candles(timeframe="M1", count=512, now=NOW):
    seconds = worker.NATIVE_SECONDS[timeframe]
    end = int(now.timestamp()) // seconds * seconds
    result = []
    for index in range(count):
        close = 1.1 + index * .000001 + (index % 7) * .000002
        row = {"time": datetime.fromtimestamp(end - (count-index)*seconds, timezone.utc).isoformat(), "complete": True, "volume": 100 + index % 13}
        for side, shift in (("mid", 0), ("bid", -.00005), ("ask", .00005)):
            row[side] = {"o": close + shift - .00001, "h": close + shift + .00002, "l": close + shift - .00003, "c": close + shift}
        result.append(row)
    return result


def quotes(pairs=("EUR_USD",), now=NOW):
    return {"schema_version": 1, "research_only": True, "quote_count": len(pairs), "producer": "practice_007_dedicated_quote_stream", "generated_utc": now.isoformat(), "quotes": {pair: {"bid": 1.2, "ask": 1.2001, "pip": .0001, "time": now.isoformat(), "source": "synthetic", "tradeable": True} for pair in pairs}, "coverage": {"retained_last_known_instruments": []}}


def valid_clock(now=NOW):
    return {"schema_version": 1, "generated_utc": now.isoformat(), "status": "ok", "timestamp_normalization_trusted": True, "host_clock_synchronized": True, "clock_discontinuity_active": False, "clock_sources_consistent": True, "source_fresh": True, "source_age_sec": 0.0, "broker_clock_lead_sec": .2, "broker_clock_sample_count": 32, "external_https_clock": {}}


def write_candles(path, pair, timeframe, rows):
    fields = ["time", "datetime", "instrument", "granularity", "open", "high", "low", "close", "bid_open", "bid_high", "bid_low", "bid_close", "ask_open", "ask_high", "ask_low", "ask_close", "volume"]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for candle in rows:
            row = {"time": candle["time"], "datetime": candle["time"], "instrument": pair, "granularity": timeframe, "volume": candle["volume"]}
            for side, prefix in (("mid", ""), ("bid", "bid_"), ("ask", "ask_")):
                row.update({prefix+long: candle[side][short] for short, long in (("o", "open"), ("h", "high"), ("l", "low"), ("c", "close"))})
            writer.writerow(row)


def arguments(tmp_path):
    return SimpleNamespace(quote_snapshot=tmp_path/"quotes.json", candle_root=tmp_path/"candles", book_snapshot=None, clock_state=tmp_path/"clock.json", archive_root=tmp_path/"observations", heartbeat=tmp_path/"heartbeat.json", max_cycle_sec=30, max_daily_archive_mib=32, minimum_free_mib=64)


def test_reuses_actual_wide_calculators_without_importing_trading_modules():
    sets = {tf: candles(tf) for tf in worker.NATIVE_SECONDS}
    primary, views, coverage = calculator.calculate_pair("EUR_USD", sets, .0001)
    expected = calculator.ma.build_feature_vector([row["mid"]["c"] for row in sets["M1"]], .0001, "M1")
    assert len(expected) == 643
    assert {key: primary[key] for key in expected} == expected
    original, _ = calculator.calculators()["build_features"]("EUR_USD", sets, .0001, include_ma_grid=False)
    assert all(primary[key] == original[key] for key in calculator.STRUCTURAL_NAMES)
    assert all(not any(name.startswith("ma__") for name in row) for row in views.values())
    assert coverage["rich_ma_native_timeframes"] == ["M1"]
    assert "oanda_practice_shadow_strategy_lab" not in sys.modules
    assert "oanda_practice_eurusd_micro_scalper" not in sys.modules


def test_complete_frame_has_wide_fields_and_explicit_missing_news_and_model_families():
    snapshot = worker.build_research_observation(quotes(), {"EUR_USD": {tf: candles(tf) for tf in worker.NATIVE_SECONDS}}, source_read_completed_utc=NOW.isoformat(), generated_utc=NOW.isoformat(), clock=lambda: NOW.isoformat())
    row = build_observation_frame(snapshot)["instruments"]["EUR_USD"]
    assert sum(name.startswith("ma__") for group in row["groups"].values() for name in group["values"]) == 643
    assert all(state == "missing" for state in row["groups"]["news"]["value_states"].values())
    assert row["coverage"]["families"]["supervised_and_pattern_forecasts"] == "model_outputs_not_loaded"
    assert snapshot["coverage"]["all_configured_feature_families_materialized"] is False


def test_resample_reuse_matches_uncached_source_and_never_reuses_another_call():
    for offset in (0, 5):
        sets = {tf: candles(tf, now=NOW + timedelta(minutes=offset)) for tf in worker.NATIVE_SECONDS}
        actual = calculator.calculate_pair("EUR_USD", sets, .0001)
        expected = calculator._calculate_pair("EUR_USD", sets, .0001)
        assert actual == expected


def test_current_quote_schema_and_exact_tradeability_are_required():
    payload = quotes()
    payload["schema_version"] = 3
    result = worker.build_research_observation(payload, {}, source_read_completed_utc=NOW.isoformat(), clock=lambda: NOW.isoformat())
    assert result["coverage"]["accepted_instrument_count"] == 1
    payload["schema_version"] = 2
    with pytest.raises(ValueError, match="snapshot_schema"):
        worker.build_research_observation(payload, {}, source_read_completed_utc=NOW.isoformat())
    payload = quotes()
    payload["quotes"]["EUR_USD"]["tradeable"] = "true"
    result = worker.build_research_observation(payload, {}, source_read_completed_utc=NOW.isoformat(), clock=lambda: NOW.isoformat())
    assert result["coverage"]["accepted_instrument_count"] == 0
    assert result["coverage"]["quote_exclusions"][0]["reason"] == "quote_not_explicitly_tradeable"


def test_stale_retained_quote_keeps_its_original_clock():
    payload = quotes()
    payload["quotes"]["EUR_USD"]["time"] = (NOW-timedelta(hours=1)).isoformat()
    payload["coverage"]["retained_last_known_instruments"] = ["EUR_USD"]
    result = worker.build_research_observation(payload, {}, source_read_completed_utc=NOW.isoformat(), generated_utc=NOW.isoformat(), clock=lambda: NOW.isoformat())
    row = build_observation_frame(result)["instruments"]["EUR_USD"]
    assert row["quote"]["time"] == payload["quotes"]["EUR_USD"]["time"]
    assert row["coverage"]["quote_accepted"] is False
    assert row["groups"]["primary"]["component_clocks"]["quote_feature_source_utc"] == ""
    assert all(row["groups"]["primary"]["value_states"][name] == "missing" for name in calculator.ma_names())


def test_candle_tail_rejects_future_and_only_retains_contiguous_suffix(tmp_path):
    path = tmp_path/"EUR_USD_M1.csv"
    rows = candles(count=80)
    write_candles(path, "EUR_USD", "M1", rows[:20]+rows[21:])
    kept, receipt = worker.read_candle_tail(path, "EUR_USD", "M1", clock=lambda: NOW.isoformat())
    assert len(kept) == 59 and receipt["discarded_rows_before_gap"] == 20
    rows[-1]["time"] = NOW.isoformat()
    write_candles(path, "EUR_USD", "M1", rows)
    with pytest.raises(ValueError, match="incomplete_at_read"):
        worker.read_candle_tail(path, "EUR_USD", "M1", clock=lambda: NOW.isoformat())


def test_clock_failure_prevents_even_quote_read_or_archive_creation(tmp_path):
    args = arguments(tmp_path)
    args.clock_state.write_text(json.dumps({**valid_clock(), "host_clock_synchronized": False}), encoding="utf-8")
    with pytest.raises(ValueError, match="research_clock_unverified"):
        worker.run_cycle(args, clock=lambda: NOW.isoformat())
    assert not args.archive_root.exists()


def test_actual_synthetic_file_cycle_publishes_full_archive_and_receipt(tmp_path):
    args = arguments(tmp_path)
    args.quote_snapshot.write_text(json.dumps(quotes()), encoding="utf-8")
    args.clock_state.write_text(json.dumps(valid_clock()), encoding="utf-8")
    for timeframe in worker.NATIVE_SECONDS:
        write_candles(args.candle_root/f"EUR_USD_{timeframe}.csv", "EUR_USD", timeframe, candles(timeframe))
    result = worker.run_cycle(args, clock=lambda: NOW.isoformat())
    assert result["status"] == "published"
    with gzip.open(result["archive"], "rt", encoding="utf-8") as stream:
        envelope = json.load(stream)
    assert envelope["frame"]["instruments"]["EUR_USD"]["coverage"]["rich_ma_status"] == "available"
    receipt = json.loads(Path(result["publication_receipt"]).read_text())
    assert receipt["frame_sha256"] == worker.payload_sha256(envelope["frame"])
    assert receipt["publication_scope"] == "archive_visible_before_this_receipt"
    with pytest.raises(ValueError, match="identity_collision"):
        worker.run_cycle(args, clock=lambda: NOW.isoformat())


def test_clock_proof_cannot_be_renewed_by_later_healthy_file(tmp_path, monkeypatch):
    args = arguments(tmp_path)
    args.quote_snapshot.write_text(json.dumps(quotes()), encoding="utf-8")
    args.clock_state.write_text(json.dumps(valid_clock()), encoding="utf-8")
    ticks = [NOW.isoformat()]
    original = worker.build_research_observation
    def delayed(*a, **kw):
        result = original(*a, **kw)
        ticks[0] = (NOW+timedelta(seconds=91)).isoformat()
        args.clock_state.write_text(json.dumps(valid_clock(NOW+timedelta(seconds=91))), encoding="utf-8")
        return result
    monkeypatch.setattr(worker, "build_research_observation", delayed)
    with pytest.raises(ValueError, match="initial_research_clock_expired"):
        worker.run_cycle(args, clock=lambda: ticks[0])
    assert not list(args.archive_root.glob("date=*/hour=*/obs_*.json.gz"))


def test_clock_expiry_during_serialization_prevents_archive_and_identity_visibility(tmp_path, monkeypatch):
    args = arguments(tmp_path)
    args.quote_snapshot.write_text(json.dumps(quotes()), encoding="utf-8")
    args.clock_state.write_text(json.dumps(valid_clock()), encoding="utf-8")
    ticks = [NOW.isoformat()]
    original = worker.archive_observation_snapshot
    def delayed(snapshot, root, *, before_publish):
        def expired_before_atomic_visibility():
            ticks[0] = (NOW+timedelta(seconds=91)).isoformat()
            args.clock_state.write_text(json.dumps(valid_clock(NOW+timedelta(seconds=91))), encoding="utf-8")
            before_publish()
        return original(snapshot, root, before_publish=expired_before_atomic_visibility)
    monkeypatch.setattr(worker, "archive_observation_snapshot", delayed)
    with pytest.raises(ValueError, match="expired_before_publication"):
        worker.run_cycle(args, clock=lambda: ticks[0])
    assert not list(args.archive_root.glob("date=*/hour=*/obs_*.json.gz"))
    assert not list((args.archive_root/".identities").glob("*.json"))


def test_archive_daily_capacity_refuses_without_removing_existing_files(tmp_path):
    directory = tmp_path/"date=20260913"/"hour=16"
    directory.mkdir(parents=True)
    path = directory/"obs_owned_synthetic.json.gz"
    path.write_bytes(b"owned-capacity-fixture")
    with pytest.raises(ValueError, match="daily_byte_or_file_bound"):
        worker.check_storage(tmp_path, now=NOW, max_daily_bytes=worker.MAX_ENVELOPE_BYTES, minimum_free_bytes=0)
    assert path.read_bytes() == b"owned-capacity-fixture"


def test_elapsed_cycle_bound_refuses_atomic_publication(tmp_path, monkeypatch):
    args = arguments(tmp_path)
    args.quote_snapshot.write_text(json.dumps(quotes()), encoding="utf-8")
    args.clock_state.write_text(json.dumps(valid_clock()), encoding="utf-8")
    elapsed = [0.0]
    monkeypatch.setattr(worker.time, "monotonic", lambda: elapsed[0])
    original = worker.archive_observation_snapshot
    def delayed(snapshot, root, *, before_publish):
        def expired_before_atomic_visibility():
            elapsed[0] = 31.0
            before_publish()
        return original(snapshot, root, before_publish=expired_before_atomic_visibility)
    monkeypatch.setattr(worker, "archive_observation_snapshot", delayed)
    with pytest.raises(ValueError, match="cycle_time_bound_before_publication"):
        worker.run_cycle(args, clock=lambda: NOW.isoformat())
    assert not list(args.archive_root.glob("date=*/hour=*/obs_*.json.gz"))
    assert not list((args.archive_root/".identities").glob("*.json"))


def test_five_minute_native_updates_do_not_hide_fresh_pure_quote_features():
    frames = []
    for index in range(2):
        now = NOW + timedelta(minutes=5*index)
        source_end = now - timedelta(minutes=4)
        payload = quotes(now=now)
        payload["quotes"]["EUR_USD"]["ask"] += index * .0001
        snapshot = worker.build_research_observation(payload, {"EUR_USD": {tf: candles(tf, now=source_end) for tf in worker.NATIVE_SECONDS}}, source_read_completed_utc=now.isoformat(), generated_utc=now.isoformat(), clock=lambda: now.isoformat())
        readiness = snapshot["coverage"]["family_readiness"]
        assert readiness["rich_M1_materialized_pairs"] == 1
        assert readiness["rich_M1_fresh_pairs"] == 0
        assert readiness["native_M1_age_sec_by_pair"]["EUR_USD"] == 240
        assert readiness["fresh_quote_pairs_at_source_read"] == 1
        frames.append(build_observation_frame(snapshot))
    result = build_feature_move_map(frames, as_of_utc=now.isoformat(), window_sec=300, top_limit=200, instrument="EUR_USD")
    spread = next(row for row in result["feature_changes"] if row["feature_name"] == "live_spread_pips")
    assert spread["status"] == "available" and spread["input_timeframe"] == "QUOTE"
    assert spread["change"] == pytest.approx(1.0)
    assert result["summary"]["unavailable_reasons"]["feature_bar_stale"] >= 643
    assert result["pairs"][0]["status"] == "available"


def test_quote_component_extension_is_opt_in_only():
    from oanda_feature_observations_v1 import capture_feature_group
    raw = {"snapshot_id": "legacy-quote", "generated_utc": NOW.isoformat(), "observation_source": {"producer_contract_id": "unchanged-legacy-contract"}, "observation_inputs": {"instruments": {"EUR_USD": {"groups": {"primary": capture_feature_group({"candle_time": (NOW-timedelta(minutes=5)).isoformat(), "live_spread_pips": 1.0}, input_timeframe="M1", clock={"observed_utc": NOW.isoformat(), "clock_basis": "producer_capture", "component_clocks": {"quote_feature_source_utc": NOW.isoformat()}})}}}}}
    before = build_observation_frame(raw)
    assert "live_spread_pips" not in before["instruments"]["EUR_USD"]["groups"]["primary"]["feature_clocks"]
    raw["observation_inputs"]["quote_component_clocks_v1"] = True
    after = build_observation_frame(raw)
    assert after["instruments"]["EUR_USD"]["groups"]["primary"]["feature_clocks"]["live_spread_pips"]["input_timeframe"] == "QUOTE"


def test_supervisor_research_allowlist_never_enables_legacy_strategy_loop():
    source = (Path(__file__).parent/"oanda_always_on_supervisor.ps1").read_text(encoding="utf-8")
    allowlist = source.split("$ResearchCollectionNames = @(", 1)[1].split("\n)", 1)[0]
    assert '"research_feature_observations_v1"' in allowlist
    assert '"research_feature_forward_v1"' in allowlist
    assert '"strategy_lab"' not in allowlist and '"model_gap_live_signal"' not in allowlist
    block = re.search(r'-Name "research_feature_observations_v1".*?-Name "research_feature_forward_v1"', source, re.S).group(0)
    assert '"--clock-state"' in block and '"--interval-sec", "60"' in block
    assert '"--minimum-free-mib", "4096"' in block
    forward_block = source.split('-Name "research_feature_forward_v1"', 1)[1].split('-Name "clock_integrity_monitor"', 1)[0]
    assert '"feature_forward_v2"' in forward_block
    assert '"--creds"' not in block and '"--account-key"' not in block
