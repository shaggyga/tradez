import oanda_official_source_direction_coverage_audit as audit


def test_event_metrics_separates_detail_direction_and_timeliness():
    metrics = audit.event_metrics(
        {
            "detail_enriched": True,
            "currency_scores": {"JPY": 0.5},
            "forward_signal_timely": False,
            "actual": 1.0,
            "consensus": None,
            "official_policy_release": True,
            "prospective_eligible": True,
        }
    )

    assert metrics["detail"] is True
    assert metrics["directional"] is True
    assert metrics["timely_directional"] is False
    assert metrics["actual"] is True
    assert metrics["prospective_actual"] is True
    assert metrics["consensus"] is False
    assert metrics["causal_consensus"] is False
    assert metrics["official_policy_release"] is True
    assert metrics["prospective_eligible"] is True


def test_empty_listing_is_not_prediction_coverage():
    metrics = audit.event_metrics({"headline": "Policy update"})

    assert metrics["detail"] is False
    assert metrics["directional"] is False
    assert metrics["timely_directional"] is False
    assert metrics["prospective_actual"] is False


def test_empty_consensus_placeholder_is_not_counted():
    metrics = audit.event_metrics(
        {
            "actual": "51.0",
            "consensus": "   ",
            "consensus_value": None,
            "consensus_capture_state": "",
        }
    )

    assert metrics["actual"] is True
    assert metrics["consensus"] is False
    assert metrics["causal_consensus"] is False


def test_causal_consensus_requires_explicit_pre_release_capture():
    metrics = audit.event_metrics(
        {
            "consensus_value": 2.4,
            "consensus_capture_state": "captured_pre_release",
        }
    )

    assert metrics["consensus"] is True
    assert metrics["causal_consensus"] is True
