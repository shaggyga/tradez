import datetime as dt

from oanda_continuous_narrative_backtest import (
    assign_splits,
    evidence_quality_slice,
    evaluate,
    model_stream_equivalence,
    select_decisions,
    summarize,
)


UTC = dt.timezone.utc


def meter_row(minute=0, score=0.4, new=1, stories=None):
    return {
        "clock_utc": dt.datetime(2026, 8, 1, 12, minute, tzinfo=UTC),
        "currency": "JPY",
        "model_scores": {"published_semantic_v1": score},
        "attention_level": 1.0,
        "attention_acceleration": 0.5,
        "new_story_count": new,
        "active_story_count": 1,
        "source_family_count": 1,
        "agreement": 1.0,
        "novelty": 1.0,
        "trusted_story_count": 1,
        "forward_timely_story_count": 1,
        "story_ids": stories or ["story-a"],
        "evidence_class": "trusted_clock_subset",
    }


def test_selector_does_not_count_every_overlapping_five_minute_clock():
    rows = [meter_row(0, new=1), meter_row(5, new=0), meter_row(10, new=0)]
    decisions, counts = select_decisions(rows)
    assert len(decisions) == 1
    assert decisions[0]["sampling_cohort"] == "event_update"
    assert counts["overlapping_clock_not_sampled"] == 2
    assert decisions[0]["quality_slice"] == "trusted_forward_single_source"
    assert counts["quality:trusted_forward_single_source"] == 1


def test_sign_change_is_a_new_causal_decision():
    rows = [meter_row(5, score=0.4, new=1), meter_row(10, score=-0.4, new=0)]
    decisions, _ = select_decisions(rows)
    assert [row["direction"] for row in decisions] == [1, -1]


def test_evidence_quality_slices_are_mutually_exclusive_and_outcome_independent():
    rows = []
    specifications = [
        (1, 1, 2, "trusted_forward_multi_source"),
        (1, 1, 1, "trusted_forward_single_source"),
        (1, 0, 1, "trusted_not_forward"),
        (0, 1, 2, "forward_multi_source_untrusted"),
        (0, 1, 1, "forward_single_source_untrusted"),
        (0, 0, 1, "secondary_or_stale"),
    ]
    for index, (trusted, forward, families, expected) in enumerate(specifications):
        row = meter_row(minute=index, score=0.4, new=1, stories=[f"s-{index}"])
        row["trusted_story_count"] = trusted
        row["forward_timely_story_count"] = forward
        row["source_family_count"] = families
        rows.append(row)
        assert evidence_quality_slice(row) == expected
    decisions, counts = select_decisions(rows)
    assert {row["quality_slice"] for row in decisions} == {
        expected for *_, expected in specifications
    }
    assert sum(value for key, value in counts.items() if key.startswith("quality:")) == len(decisions)


def test_exact_duplicate_model_streams_count_once_as_independent_breadth():
    rows = []
    for minute, score in ((0, 0.4), (5, -0.3)):
        row = meter_row(minute=minute, score=score, new=1)
        row["model_scores"] = {
            "model_a": score,
            "model_b": score,
            "model_c": -score,
        }
        rows.append(row)
    decisions, _ = select_decisions(rows)
    audit = model_stream_equivalence(decisions)
    assert audit["named_model_count"] == 3
    assert audit["independent_exact_stream_count"] == 2
    assert len(audit["duplicate_stream_groups"]) == 1
    assert audit["duplicate_stream_groups"][0]["models"] == ["model_a", "model_b"]


def test_evaluation_keeps_news_technical_conflict_separate():
    decisions, _ = select_decisions([meter_row(0, score=0.4, new=1)])
    clock = decisions[0]["base_clock_id"]
    candidates = {clock: [{
        "spread_bps": 1.0,
        "instrument": "USD_JPY",
        "prior_currency_bps": -2.0,
        "base_multiplier": -1,
        "horizon_pair_net_bps": {
            15: {1: 3.0, -1: -5.0, "pair_mid_bps": 4.0},
        },
    }]}
    rows = evaluate(decisions, candidates)
    arms = {row["arm"] for row in rows}
    assert arms == {
        "sentiment_only", "technical_only_same_clocks",
        "sentiment_technical_conflicted",
    }


def test_chronological_splits_and_summary_remain_research_only():
    outcomes = []
    for day in range(1, 11):
        outcomes.append({
            "decision_id": str(day), "model_id": "m", "sampling_cohort": "event_update",
            "episode_id": f"e{day}", "currency": "JPY",
            "decision_utc": f"2026-08-{day:02d}T12:00:00+00:00", "instrument": "USD_JPY",
            "horizon_minutes": 15, "arm": "sentiment_only", "direction": 1,
            "score": 0.5, "net_bps": 1.0, "realized_currency_mid_bps": 2.0,
            "spread_bps": 1.0, "prior_currency_bps": 1.0,
            "evidence_class": "classifier_adaptive_discovery",
        })
    assign_splits(outcomes)
    assert [sum(row["split"] == name for row in outcomes) for name in ("train", "validation", "test")] == [6, 2, 2]
    summaries = summarize(outcomes)
    assert all(row["confirmation_eligible"] is False for row in summaries)
    assert {row["split"] for row in summaries} == {"train", "validation", "test"}
