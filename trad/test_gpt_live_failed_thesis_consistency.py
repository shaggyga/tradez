from __future__ import annotations

import oanda_gpt_prod_live_account_manager as live


def test_inactive_live_failed_thesis_flags_false_cooldown_claim() -> None:
    decision = {
        "live_failed_thesis": {
            "active": False,
            "blocked_keys": [],
        },
        "market_summary": (
            "A live_failed_thesis cooldown is active, blocking new USD_LONG "
            "short trades today."
        ),
        "new_trade_candidates": [
            {
                "instrument": "EUR_USD",
                "action": "WATCH",
                "reason": "live_failed_thesis cooldown blocks the setup.",
            }
        ],
    }

    review = live.live_failed_thesis_narrative_review(decision)

    assert review["local_verdict"] == "narrative_mismatch"
    assert review["issue_count"] == 2
    assert review["matches"][0]["path"] == "market_summary"


def test_active_live_failed_thesis_allows_cooldown_claim() -> None:
    decision = {
        "live_failed_thesis": {
            "active": True,
            "blocked_keys": ["USD_LONG"],
        },
        "underdeployment_reason": "live_failed_thesis cooldown blocks same-thesis entries.",
    }

    review = live.live_failed_thesis_narrative_review(decision)

    assert review["local_verdict"] == "ok"
    assert review["issue_count"] == 0


def test_inactive_recent_losses_without_cooldown_claim_are_ok() -> None:
    decision = {
        "live_failed_thesis": {
            "active": False,
            "blocked_keys": [],
        },
        "underdeployment_reason": (
            "Recent losses justify caution because expected_R is weak and fresh "
            "confirmation is missing."
        ),
    }

    review = live.live_failed_thesis_narrative_review(decision)

    assert review["local_verdict"] == "ok"
    assert review["matches"] == []


def test_inactive_generic_recent_loss_cooldown_claim_is_flagged() -> None:
    decision = {
        "live_failed_thesis": {
            "active": False,
            "blocked_keys": [],
        },
        "new_trade_candidates": [
            {
                "instrument": "GBP_USD",
                "action": "WATCH",
                "why_now": (
                    "GBP_USD bears downside momentum but entry timing requires "
                    "correction and risk cooldown due to recent portfolio losses."
                ),
            }
        ],
    }

    review = live.live_failed_thesis_narrative_review(decision)

    assert review["local_verdict"] == "narrative_mismatch"
    assert review["issue_count"] == 1
    assert review["matches"][0]["path"] == "new_trade_candidates[0].why_now"


def test_inactive_cooldown_expiration_language_is_flagged() -> None:
    decision = {
        "live_failed_thesis": {
            "active": False,
            "blocked_keys": [],
        },
        "new_trade_candidates": [
            {
                "instrument": "EUR_USD",
                "action": "WATCH",
                "reason": "Await cooldown expiration and fresh confirmation.",
            },
            {
                "instrument": "GBP_USD",
                "action": "WATCH",
                "reason": "Holding to watch for better entry post cooldown.",
            },
        ],
    }

    review = live.live_failed_thesis_narrative_review(decision)

    assert review["local_verdict"] == "narrative_mismatch"
    assert review["issue_count"] == 2
    assert review["matches"][0]["path"] == "new_trade_candidates[0].reason"


def test_inactive_negated_cooldown_language_is_ok() -> None:
    decision = {
        "live_failed_thesis": {
            "active": False,
            "blocked_keys": [],
        },
        "underdeployment_reason": (
            "No cooldown is active; the account is flat because expected_R is weak."
        ),
    }

    review = live.live_failed_thesis_narrative_review(decision)

    assert review["local_verdict"] == "ok"
    assert review["matches"] == []


def test_high_confidence_watch_plan_requires_expected_r() -> None:
    decision = {
        "underdeployment_reason": "Weak expected_R is not proven.",
        "new_trade_candidates": [
            {
                "instrument": "EUR_USD",
                "action": "WATCH",
                "direction": "SHORT",
                "outlook_confidence": 75,
                "risk_pct": 2.0,
                "entry_min": 1.13,
                "entry_max": 1.134,
                "stop_loss": 1.14,
                "take_profit": 1.10,
                "reason": "Await better entries.",
                "why_now": "Bearish thesis is strong.",
            }
        ],
        "orders_to_execute": [],
    }

    review = live.live_decision_quality_review(decision)

    assert review["local_verdict"] == "retry_decision_quality"
    assert review["watch_accountability"][0]["instrument"] == "EUR_USD"
    assert "missing numeric expected_R" in review["watch_accountability"][0]["reasons"]


def test_high_confidence_watch_plan_with_expected_r_and_blocker_is_ok() -> None:
    decision = {
        "underdeployment_reason": "Weak expected_R and missing fresh evidence keep the account flat.",
        "new_trade_candidates": [
            {
                "instrument": "EUR_USD",
                "action": "WATCH",
                "direction": "SHORT",
                "outlook_confidence": 75,
                "risk_pct": 2.0,
                "entry_min": 1.13,
                "entry_max": 1.134,
                "stop_loss": 1.14,
                "take_profit": 1.10,
                "expected_R": 0.65,
                "reason": "Expected_R below threshold until price confirms below support.",
                "why_now": "Bearish thesis is strong but support has not broken.",
            }
        ],
        "orders_to_execute": [],
    }

    review = live.live_decision_quality_review(decision)

    assert review["local_verdict"] == "ok"
    assert review["watch_accountability"] == []


def test_vague_flat_decision_is_flagged() -> None:
    decision = {
        "underdeployment_reason": "Recent losses warrant a defensive stance.",
        "risk_notes": ["Stay cautious and wait."],
        "new_trade_candidates": [],
        "orders_to_execute": [],
    }

    review = live.live_decision_quality_review(decision)

    assert review["local_verdict"] == "retry_decision_quality"
    assert review["vague_flat_reason"]["reason"] == "flat/no-order decision lacks a concrete blocker"


def test_missed_entry_candidates_are_retryable() -> None:
    decision = {
        "underdeployment_reason": "Weak expected_R is not proven.",
        "new_trade_candidates": [],
        "orders_to_execute": [],
    }

    review = live.live_decision_quality_review(
        decision,
        missed_entry_candidates=[
            {
                "instrument": "EUR_USD",
                "direction": "SHORT",
                "reason": "inside entry range",
            }
        ],
    )

    assert review["local_verdict"] == "retry_decision_quality"
    assert review["missed_entry_candidates"][0]["instrument"] == "EUR_USD"
