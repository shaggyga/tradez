"""Pure entry telemetry. Counts describe one observed cycle, never orders or trials."""
from collections import Counter

SCHEMA = "entry_selection_diagnostics_v2"


def selection_fields(local_candidates, feed_count, qualified, selected, disabled_reason=""):
    """Keep local generation separate from a consumer's shared feed input."""
    accepted = sum(str(row.get("preconsensus_class") or "accepted") == "accepted"
                   and bool(row.get("account_eligible", True)) for row in local_candidates)
    return {
        "entry_diagnostics_schema": SCHEMA,
        "candidate_count_scope": "sampled_cycle_observations_not_unique_signals",
        "accepted_candidates_scope": "local_candidates_only_excludes_shared_feed",
        "local_accepted_candidates": accepted,
        "shared_feed_candidate_observations": feed_count,
        "pre_final_selection_state": ("execution_disabled" if disabled_reason else
                                      "no_qualified_candidate" if selected is None else
                                      "selected_before_final_entry_rules"),
        "execution_disabled_reason": disabled_reason or None,
        "direction_conflicting_qualified_candidates": sum(
            bool(row.get("direction_conflict")) for row in qualified),
        "final_entry_rules_evaluated": False,
    }


def blocked_fields(blocks, *, detail_limit=12):
    """Count every final rejection before bounding the detailed examples."""
    counts = Counter(str(row.get("reason") or "unknown") for row in blocks)
    return {
        "entry_diagnostics_schema": SCHEMA,
        "entry_stage": "final_candidate_rules",
        "candidate_count_scope": "sampled_cycle_observations_not_unique_signals",
        "candidate_block_reason_counts": dict(sorted(counts.items())),
        "candidate_blocks_total": len(blocks),
        "candidate_blocks_shown": min(len(blocks), detail_limit),
        "candidate_block_details_truncated": len(blocks) > detail_limit,
        "candidates_passing_final_entry_rules": 0,
        "authorization_stage_reached": False,
        "legacy_reason_scope": "all_final_candidate_rules_not_only_position_capacity",
    }


def dashboard_fields(selection, skip):
    """Expose observed scope and timestamps; never invent zeroes for old logs."""
    return {
        "latest_accepted_candidates_scope": selection.get("accepted_candidates_scope")
            or "local_candidates_only_excludes_shared_feed",
        "latest_shared_feed_candidate_observations": selection.get("signal_feed_candidates"),
        "latest_pre_final_selection_state": selection.get("pre_final_selection_state"),
        "latest_entry_skip_time": skip.get("time"),
        "latest_entry_skip_reason": skip.get("reason"),
        "latest_entry_skip_stage": skip.get("entry_stage"),
        "latest_candidate_block_reason_counts": skip.get("candidate_block_reason_counts"),
        "latest_candidate_block_details_truncated": skip.get("candidate_block_details_truncated"),
        "entry_count_scope": "sampled_cycle_observations_not_unique_signals",
    }
