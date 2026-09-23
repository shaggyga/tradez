import oanda_improvement_control_engine as control


def registry():
    return control.read_json(control.REGISTRY)


def facts(**updates):
    base = {
        "integrity_ok": True, "storage_ok": True, "integrity_failures": [],
        "actionable_integrity_failures": [],
        "externally_blocked_integrity_failures": [],
        "contained_historical_integrity_failures": [],
        "causal_consensus_count": 0, "standardized_surprise_count": 0,
        "rates_connected": False, "news_watchlist_running": True,
        "news_matured_entries": 0, "confirmed_candidates": 0,
        "news_outcome_record_valid": True,
        "news_outcome_record_age_sec": 60,
        "news_diagnosed_effective_theses": 10,
        "news_diagnosed_misses_or_gaps": 4,
        "news_improvement_queue_count": 2,
        "collecting_hypotheses": 100, "futility_retired": 10,
        "allocator_frozen": True, "cftc_collecting": True,
        "direct_source_response_running": True, "direct_source_matured_targets": 0,
        "executable_opportunity_collecting": True,
        "executable_opportunity_forecasts": 0, "executable_opportunity_maturities": 0,
        "major_move_gap_census_ok": True, "major_move_gap_age_sec": 60,
        "major_move_rows": 100, "major_move_factor_episodes": 20,
        "major_move_cross_selection_gaps": 3,
        "major_move_wrong_direction_gaps": 2,
        "major_move_magnitude_gaps": 1,
    }
    base.update(updates)
    return base


def test_completed_internal_build_collects_while_external_sources_are_blocked():
    result = control.evaluate(registry(), facts())
    assert result["active_build_branch"] is None
    assert "causal_macro_rates" in result["blocked_branches"]
    assert "magnitude_cost_targets" in result["collecting_branches"]
    assert "allocator_policy_proof" in result["collecting_branches"]
    assert "major_move_gap_replay" in result["collecting_branches"]


def test_integrity_failure_preempts_research():
    result = control.evaluate(registry(), facts(
        integrity_ok=False,
        integrity_failures=["database_integrity"],
        actionable_integrity_failures=["database_integrity"],
    ))
    assert result["active_build_branch"] == "operational_integrity"


def test_fail_closed_external_clock_does_not_starve_unblocked_research():
    result = control.evaluate(registry(), facts(
        integrity_ok=False,
        integrity_failures=["clock_explicitly_classified"],
        externally_blocked_integrity_failures=["clock_explicitly_classified"],
    ))
    operational = next(row for row in result["branches"] if row["branch_id"] == "operational_integrity")
    assert operational["state"] == "blocked_external"
    assert result["active_build_branch"] is None


def test_contained_historical_gap_does_not_starve_unblocked_research():
    result = control.evaluate(registry(), facts(
        integrity_ok=False,
        integrity_failures=["shadow_archive_historical_detail_reconciled"],
        contained_historical_integrity_failures=[
            "shadow_archive_historical_detail_reconciled"
        ],
    ))
    operational = next(
        row for row in result["branches"]
        if row["branch_id"] == "operational_integrity"
    )
    assert operational["state"] == "contained_degraded"
    assert result["active_build_branch"] is None


def test_connected_macro_source_moves_to_collection_not_same_window_promotion():
    result = control.evaluate(registry(), facts(causal_consensus_count=4, rates_connected=True))
    macro = next(row for row in result["branches"] if row["branch_id"] == "causal_macro_rates")
    assert macro["state"] == "collecting"
    assert result["active_build_branch"] is None


def test_missing_opportunity_collector_reopens_internal_build():
    result = control.evaluate(registry(), facts(executable_opportunity_collecting=False))
    assert result["active_build_branch"] == "magnitude_cost_targets"


def test_canary_stays_governance_blocked_without_confirmation():
    result = control.evaluate(registry(), facts())
    canary = next(row for row in result["branches"] if row["branch_id"] == "practice_canary_execution")
    assert canary["state"] == "blocked_governance"


def test_stale_major_move_gap_census_reopens_diagnostic_build():
    result = control.evaluate(registry(), facts(major_move_gap_census_ok=False))
    assert result["active_build_branch"] == "major_move_gap_replay"


def test_missing_canonical_news_diagnosis_reopens_event_response_build():
    result = control.evaluate(registry(), facts(news_outcome_record_valid=False))
    assert result["active_build_branch"] == "event_response_archetypes"


def test_old_valid_canonical_news_record_remains_collecting():
    result = control.evaluate(
        registry(), facts(news_outcome_record_valid=True, news_outcome_record_age_sec=86400)
    )
    branch = next(
        row for row in result["branches"]
        if row["branch_id"] == "event_response_archetypes"
    )
    assert branch["state"] == "collecting"
