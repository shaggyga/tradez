from copy import deepcopy
import datetime as dt
import math

import pytest
import oanda_entry_news_feature_join_v1 as join
import oanda_news_causal_aggregation_guard_v1 as guard
from test_oanda_news_causal_aggregation_guard_v1 import row, topic, START

NOW = (START + dt.timedelta(minutes=30)).timestamp()
BINDINGS = {name: "a" * 64 for name in join.SOURCE_FILES}


def member(name="a", **kwargs):
    value = row(name, **kwargs)
    value["classification_version"] = guard.CLASSIFICATION_VERSION
    return value


def envelope(members=None, topics=None):
    if topics is None:
        topics = [guard.guard_topic(topic(), members or [member(verified=True)],
            as_of=dt.datetime.fromtimestamp(NOW-2, dt.timezone.utc))]
    return dict(complete=True, guard_version=guard.GUARD_VERSION,
        classification_version=guard.CLASSIFICATION_VERSION, source_bindings=BINDINGS,
        raw_source_sha256="b"*64, as_of_epoch=NOW-2, source_visible_epoch=NOW-1,
        consumer_observed_epoch=NOW, topics=topics, topics_sha256=join.digest(topics))


def run(news=None, **kwargs):
    return join.join_entry_news_features(instrument="EUR_USD", cutoff_epoch=NOW,
        current_news=envelope() if news is None else news,
        expected_source_bindings=BINDINGS, clock=lambda: NOW+1, **kwargs)


def history(**changes):
    value = dict(record_id="record1", underlying_event_id="event1", currency="USD", factor_type="policy_rate_change",
        source_sha256="c"*64, source_version="original_contract_v1",
        original_event_epoch=NOW-4000, original_first_seen_epoch=NOW-3999,
        source_visible_epoch=NOW-3998, observed_available_epoch=NOW-3997,
        feature_available_epoch=NOW-3996,
        source_availability_basis="independent_committed_observation",
        selection_basis="source_first_universe", signed_factor_score=0.5,
        numeric_measurement_state="measured", response_start_epoch=NOW-3900,
        response_target_epoch=NOW-300, response_available_epoch=NOW-299,
        response_source_sha256="d"*64, response_value_bps=2.5)
    return {**value, **changes}


def test_actual_value_lineage_and_no_prediction_or_authority():
    result = run()
    assert result["current_news"]["status"] == "available"
    assert len(result["current_news"]["vetted"]) == 1
    assert result["current_news"]["context_only"] == []
    assert result["current_news"]["forward_context_overlap_member_count"] == 1
    assert result["news_features"]["context_balance"]["value"] == -0.5
    assert result["news_features"]["vetted_balance"]["value"] == -0.5
    assert result["news_features"]["vetted_remaining_hours"]["value"] == 1.
    assert list(result["news_features"]) == list(join.NEWS_FEATURES)
    assert result["lineage"]["existing_model_total_features"] == 34
    assert result["numerical_forecast"] is None
    assert all(result[key] is value for key, value in join.AUTHORITY.items())


def test_input_not_mutated_and_output_hash():
    news = envelope(); original = deepcopy(news)
    result = run(news); sealed = result.pop("diagnostic_sha256")
    assert news == original and join.digest(result) == sealed
    result["source_bindings"].clear()
    assert len(BINDINGS) == 7


def test_empty_complete_population_has_only_zero_counts_not_fake_neutral():
    result = run(envelope(topics=[]))
    features = result["news_features"]
    for name, feature in features.items():
        if name.endswith("volume_log"):
            assert feature["value"] == 0 and feature["status"] == "observed_empty_count"
        else:
            assert feature["value"] is None and feature["status"] == "undefined_empty_population"
    assert result["matured_response_memory"]["status"] == "not_provided"


@pytest.mark.parametrize("field,value", [
    ("complete", False), ("complete", 1), ("raw_source_sha256", None),
    ("topics_sha256", "f"*64), ("guard_version", "other"),
    ("classification_version", "other"), ("source_bindings", {}),
    ("as_of_epoch", NOW-301), ("as_of_epoch", NOW+1),
    ("source_visible_epoch", NOW+1), ("consumer_observed_epoch", NOW+1),
    ("consumer_observed_epoch", NOW-3),
])
def test_current_missing_unavailable_never_zero(field, value):
    news = envelope(); news[field] = value
    result = run(news)
    assert result["current_news"]["status"] == "unavailable"
    assert all(f["value"] is None and f["status"] == "unavailable" for f in result["news_features"].values())


def test_missing_news_does_not_prevent_separate_mature_memory():
    result = join.join_entry_news_features(instrument="EUR_USD", cutoff_epoch=NOW,
        expected_source_bindings=BINDINGS, response_records=[history()], clock=lambda: NOW)
    assert result["current_news"]["reason"] == "current_news_missing"
    assert len(result["matured_response_memory"]["eligible"]) == 1


def test_postmove_context_goes_only_to_discovery():
    value = member(verified=True); value["reports_prior_market_move"] = True
    result = run(envelope([value]))
    assert not result["current_news"]["context"] and not result["current_news"]["vetted"]
    assert result["current_news"]["discovery"][0]["reason"] == "post_move_explanation"


@pytest.mark.parametrize("flag", ["context_only", "source_listing_bootstrap", "directional_research_only",
    "detail_enrichment_research_only", "secondary_analysis_context", "non_catalyst_context"])
def test_original_guard_context_rejections_are_preserved(flag):
    value = member(verified=True); value[flag] = True
    result = run(envelope([value]))
    assert not result["current_news"]["vetted"]
    assert len(result["current_news"]["context"]) == 1


def test_original_expiry_not_extended_by_current_consumer_read():
    value = member(verified=True); value["estimated_reaction_horizon_minutes"] = 20
    result = run(envelope([value]))
    assert not result["current_news"]["vetted"]
    assert result["current_news"]["context"]


def test_late_and_future_member_do_not_gain_forward_support():
    value = member(verified=True, published=-80, seen=1)
    assert not run(envelope([value]))["current_news"]["vetted"]
    value["observed_available_utc"] = dt.datetime.fromtimestamp(NOW+1, dt.timezone.utc).isoformat()
    current = run(envelope([value]))["current_news"]
    assert not current["vetted"] and not current["context"]


def test_unrecognized_member_version_cannot_borrow_guard_admission():
    value = member(verified=True); value["classification_version"] = "unknown"
    current = run(envelope([value]))["current_news"]
    assert not current["vetted"] and not current["context"]


def test_internal_duplicate_preserves_guard_withholding():
    value = member(verified=True)
    current = run(envelope([value, value]))["current_news"]
    assert not current["vetted"] and len(current["context"]) == 1


def test_identical_topic_collapses_but_conflicting_event_rejects_all():
    first = envelope()["topics"][0]
    current = run(envelope(topics=[first, first]))["current_news"]
    assert len(current["context"]) == len(current["vetted"]) == 1
    other = member(verified=True); other["currency_scores"] = {"EUR": 0.5}
    second = guard.guard_topic({**topic(), "topic_id": "second"}, [other],
        as_of=dt.datetime.fromtimestamp(NOW-2, dt.timezone.utc))
    current = run(envelope(topics=[first, second]))["current_news"]
    assert not current["context"] and not current["vetted"]
    assert any(r["reason"] == "conflicting_member_identity" for r in current["rejections"])


def test_shared_members_across_distinct_topic_roots_cannot_be_two_votes():
    first = envelope()["topics"][0]
    second = guard.guard_topic({**topic(), "topic_id": "second"}, [member(verified=True)],
        as_of=dt.datetime.fromtimestamp(NOW-2, dt.timezone.utc))
    current = run(envelope(topics=[first, second]))["current_news"]
    assert not current["vetted"]
    assert sum(r["reason"] == "overlapping_forward_topic_support" for r in current["rejections"]) == 2


def test_conflicting_topic_id_cannot_create_two_disjoint_votes():
    first = envelope()["topics"][0]
    second = guard.guard_topic(topic(), [member("b", verified=True)],
        as_of=dt.datetime.fromtimestamp(NOW-2, dt.timezone.utc))
    current = run(envelope(topics=[first, second]))["current_news"]
    assert current["status"] == "unavailable" and current["reason"] == "conflicting_topic_identity"


def test_same_headline_context_not_independent_support():
    a, b = member(), member("b")
    current = run(envelope([a, b]))["current_news"]
    assert len(current["context"]) == 1 and not current["vetted"]


def test_pair_projection_and_true_measured_zero_are_explicit():
    value = member(verified=True); value["currency_scores"] = {"EUR": 0.5, "USD": 0.5}
    result = run(envelope([value]))
    assert result["news_features"]["context_balance"]["value"] == 0
    assert result["news_features"]["context_balance"]["status"] == "measured"


@pytest.mark.parametrize("field,value,reason", [
    ("source_visible_epoch", None, "source_visibility_unavailable"),
    ("feature_available_epoch", None, "feature_availability_unavailable"),
    ("observed_available_epoch", NOW+1, "history_not_available_at_cutoff"),
    ("source_availability_basis", "publication_time", "independent_visibility_unproven"),
    ("response_available_epoch", None, "response_observation_unavailable"),
    ("response_available_epoch", NOW+1, "response_not_causally_mature_at_cutoff"),
    ("response_target_epoch", NOW+1, "response_not_causally_mature_at_cutoff"),
    ("response_start_epoch", NOW-4000, "response_not_causally_mature_at_cutoff"),
    ("response_source_sha256", None, "missing_source_hash"),
    ("response_value_bps", None, "response_measurement_missing"),
])
def test_memory_requires_original_visibility_and_actual_maturity(field, value, reason):
    lane = run(response_records=[history(**{field: value})])["matured_response_memory"]
    assert not lane["eligible"] and lane["rejections"][0]["reason"] == reason


@pytest.mark.parametrize("basis", ["outcome_selected", "unknown", None])
def test_mature_outcome_selected_memory_stays_discovery(basis):
    lane = run(response_records=[history(selection_basis=basis)])["matured_response_memory"]
    assert not lane["eligible"] and len(lane["discovery"]) == 1
    assert lane["learned_orientation"] is None


def test_memory_same_event_multiple_horizons_is_one_distinct_event():
    lane = run(response_records=[history(), history(record_id="record2", response_target_epoch=NOW-100,
        response_available_epoch=NOW-99)])["matured_response_memory"]
    assert len(lane["eligible"]) == 2 and lane["distinct_eligible_events"] == 1


def test_conflicting_history_id_excludes_all_without_selecting_winner():
    lane = run(response_records=[history(), history(response_value_bps=-10)])["matured_response_memory"]
    assert not lane["eligible"] and lane["rejections"][0]["reason"] == "conflicting_history_identity"


def test_zero_factor_and_response_are_measurements_only_with_receipts():
    result = run(factor_records=[history(signed_factor_score=0)], response_records=[history(response_value_bps=0)])
    assert result["entry_factors"]["pair_eligible"][0]["signed_factor_score"] == 0
    assert result["matured_response_memory"]["pair_eligible"][0]["response_value_bps"] == 0
    result = run(factor_records=[history(numeric_measurement_state="unknown", signed_factor_score=0)])
    assert not result["entry_factors"]["eligible"]


@pytest.mark.parametrize("instrument", ["EUR_EUR", "eur_usd", "USD", "EUR_USD_X", None])
def test_instrument_contract(instrument):
    with pytest.raises(ValueError, match="invalid_instrument"):
        join.join_entry_news_features(instrument=instrument, cutoff_epoch=NOW,
            expected_source_bindings=BINDINGS, clock=lambda: NOW)


def test_future_cutoff_and_limits(monkeypatch):
    with pytest.raises(ValueError, match="future_feature_cutoff"):
        join.join_entry_news_features(instrument="EUR_USD", cutoff_epoch=NOW+1,
            expected_source_bindings=BINDINGS, clock=lambda: NOW)
    monkeypatch.setattr(join, "MAX_HISTORY", 1)
    with pytest.raises(ValueError, match="history_bound"):
        run(response_records=[history(), history()])
    monkeypatch.setattr(join, "MAX_BYTES", 10)
    with pytest.raises(ValueError, match="join_input_byte_bound"):
        run()


def test_empty_optional_original_clocks_match_real_collector_schema():
    value = member(verified=True)
    for key in ("detail_available_utc", "numeric_causal_known_utc", "publication_clock_known_utc"):
        value[key] = ""
    current = run(envelope([value]))["current_news"]
    assert len(current["context"]) == len(current["vetted"]) == 1


def test_missing_history_fields_are_named_without_backdating_new_read():
    value = history(source_visible_epoch=None, observed_available_epoch=None,
        feature_available_epoch=None, response_available_epoch=None)
    rejected = run(response_records=[value])["matured_response_memory"]["rejections"][0]
    assert rejected["missing_clock_fields"] == ["source_visible_epoch", "observed_available_epoch",
        "feature_available_epoch", "response_available_epoch"]
