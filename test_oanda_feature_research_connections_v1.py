"""Whole research path on newly created synthetic files, without live inputs."""
from datetime import timedelta
from decimal import Context, Decimal, localcontext
import json

from oanda_feature_observations_v1 import archive_observation_snapshot
from oanda_feature_forward_ledger_v1 import ForwardLedger
from oanda_feature_forward_worker_v1 import ForwardWorker
from oanda_feature_research_clock_v1 import read_verified_clock
from oanda_research_feature_observation_worker_v1 import build_research_observation
from test_oanda_research_feature_observation_worker_v1 import NOW, candles, quotes, valid_clock


PAIRS = ("EUR_USD", "GBP_USD", "AUD_USD")


def test_real_calculators_archive_mapper_publication_and_later_cost_outcomes(tmp_path):
    archive = tmp_path / "synthetic_archive"
    final_quotes = None
    for index in range(21):
        instant = NOW - timedelta(minutes=100 - 5 * index)
        payload = quotes(PAIRS, now=instant)
        # Technical history varies, then holds its last state for quiet controls;
        # the final spread jump supplies an alert without a buy/sell direction.
        for quote in payload["quotes"].values():
            quote["ask"] += (.000001 * ((index * 7) % 11) if index < 20 else .0004)
        sets = {timeframe: candles(timeframe, now=instant)
                for timeframe in ("M1", "M5", "H1")}
        shift = .000002 * ((min(index, 19) * 7) % 11)
        for rows in sets.values():
            for candle in rows[-10:]:
                for side in ("mid", "bid", "ask"):
                    candle[side] = {key: value + shift for key, value in candle[side].items()}
        snapshot = build_research_observation(
            payload, {pair: sets for pair in PAIRS},
            source_read_completed_utc=instant.isoformat(),
            generated_utc=instant.isoformat(), clock=lambda: instant.isoformat())
        archive_observation_snapshot(snapshot, archive)
        final_quotes = payload["quotes"]

    clock_path = tmp_path / "synthetic_clock.json"
    quote_path = tmp_path / "synthetic_quotes.json"
    stamp = [NOW.timestamp() + 1]
    clock_path.write_text(json.dumps(valid_clock(NOW)), encoding="utf-8")
    quote_path.write_text(json.dumps({
        "schema_version": 1, "research_only": True,
        "producer": "practice_007_dedicated_quote_stream",
        "generated_utc": NOW.isoformat(), "quote_count": len(PAIRS),
        "quotes": final_quotes}), encoding="utf-8")
    path = tmp_path / "synthetic_forward.sqlite"
    owner = ForwardLedger(path, clock=lambda: stamp[0], minimum_free_bytes=0)
    try:
        worker = ForwardWorker(
            owner, archive_root=archive, quote_path=quote_path,
            clock_check=lambda: read_verified_clock(clock_path, now_epoch=stamp[0]))
        report = worker.tick()
        assert report["status"] == "observing", report["errors"]
        assert len(report["publication"]) == 3
        assert all(item["status"] == "published" for item in report["publication"])
        assert report["can_place_orders"] is False
        initial = owner.summary()
        assert initial["counts"]["batches"] == 3
        assert initial["counts"]["jobs"] == 27
        population = initial["feature_event_counts"]["300:300"]
        assert population["all_feature_events"] > 50
        assert population["alert"] > 0 and population["control"] > 0
        assert population["excluded"] > 0  # Explicit missing news remains visible.
        publication = stamp[0]

        def observe(elapsed, pair_names, *, large=False):
            stamp[0] = publication + elapsed
            when = NOW + timedelta(seconds=1 + elapsed)
            value = quotes(pair_names, now=when)["quotes"]
            for pair, quote in value.items():
                midpoint = 1.203 if large and pair == "EUR_USD" else 1.20025
                quote.update(bid=str(midpoint - .00005), ask=str(midpoint + .00005))
            return owner.observe_quotes(value, read_started_epoch=stamp[0],
                                        read_completed_epoch=stamp[0])

        assert observe(1, PAIRS)["admitted"] == 3
        # Every actual observed target is after immutable publication+horizon.
        # AUD has no targets and must stay unknown, never become a false alert.
        for horizon in (300, 900, 3600):
            assert observe(horizon, PAIRS[:2], large=True)["admitted"] == 2
            owner.settle()
        stamp[0] = publication + 3661
        owner.settle()
        result = owner.summary()
        assert result["counts"]["outcomes"] == 27
        for counts in result["pair_probe_counts"].values():
            assert (counts["scored"], counts["unknown"], counts["pending"]) == (2, 1, 0)
        events = list(owner.iter_events())
        assert len(events) == sum(group["all_feature_events"]
                                  for group in result["feature_event_counts"].values())
        counts = result["feature_event_counts"]["300:300"]
        assert all(counts[key] > 0 for key in
                   ("hit", "false_alert", "missed_move", "quiet_control", "unknown"))
        for event in events:
            outcome = event["outcome"]
            if event["comparison"]["instrument"] == "AUD_USD":
                assert event["status"] == "unknown"
                assert outcome["score"] is None
                assert outcome["reason"] == "target_quote_unavailable"
            else:
                assert outcome["entry"]["market_epoch"] > outcome["publication_epoch"]
                assert outcome["target"]["market_epoch"] >= outcome["target_epoch"]
                score = outcome["score"]
                assert score["inferred_feature_direction"] is None
                assert score["forecast_probability"] is None
                assert score["can_place_orders"] is False
                with localcontext(Context(prec=80)):
                    for probe in score["probes"].values():
                        assert Decimal(probe["stress_net_bps"]["1"]) == Decimal(probe["net_bps"]) - 1
        frozen = owner.db.execute("SELECT job,body FROM ff_outcomes ORDER BY job").fetchall()
    finally:
        owner.close()
    reopened = ForwardLedger(path, clock=lambda: stamp[0], minimum_free_bytes=0)
    try:
        assert reopened.db.execute("SELECT job,body FROM ff_outcomes ORDER BY job").fetchall() == frozen
        assert reopened.summary()["feature_event_counts"] == result["feature_event_counts"]
    finally:
        reopened.close()
