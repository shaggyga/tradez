import sqlite3

from oanda_spike_blurb_factor_ledger import (
    FACTOR_CONTRACT_ID,
    classify_factor_type,
    link_factors,
    source_currency,
)


EXPECTED = {
    "AUD", "CAD", "CHF", "CNH", "CZK", "DKK", "EUR", "GBP", "HKD", "HUF", "JPY",
    "MXN", "NOK", "NZD", "PLN", "SEK", "SGD", "THB", "TRY", "USD", "ZAR",
}


def test_official_source_prefixes_cover_all_21_currencies():
    source_ids = {
        "rba_media": "AUD", "boc_press": "CAD", "snb_press": "CHF", "pboc_release": "CNH",
        "cnb_press": "CZK", "nationalbanken_press": "DKK", "ecb_press": "EUR",
        "boe_news": "GBP", "hkma_press": "HKD", "mnb_policy": "HUF", "boj_updates": "JPY",
        "banxico_policy": "MXN", "norges_press": "NOK", "rbnz_release": "NZD",
        "nbp_policy": "PLN", "riksbank_press": "SEK", "mas_policy": "SGD",
        "bot_policy": "THB", "tcmb_press": "TRY", "fed_policy": "USD", "sarb_policy": "ZAR",
    }
    assert {
        source_currency(source_id=source_id, base_currency=None, payload={}, expected=EXPECTED)
        for source_id in source_ids
    } == EXPECTED
    assert all(
        source_currency(source_id=source_id, base_currency=None, payload={}, expected=EXPECTED) == currency
        for source_id, currency in source_ids.items()
    )


def test_factor_ontology_maps_policy_inflation_labor_and_rates():
    assert classify_factor_type("ocr", "Official Cash Rate", "monetary_policy") == "policy_rate_change"
    assert classify_factor_type("cpi", "Consumer price inflation", "macro") == "inflation_level_change"
    assert classify_factor_type("jobs", "Payroll employment", "macro") == "labor_level_change"
    assert classify_factor_type("curve", "Treasury yield curve", "rates") == "yield_or_rate_repricing"


BASE = 1_800_000_000


def _factor(*, factor_id="factor_1", known=100, event=90, causal="point_in_time_observed"):
    return {
        "source_evidence": {"source_evidence_id": "source_1", "event_utc": str(BASE + event)},
        "factor": {
            "factor_id": factor_id,
            "source_evidence_id": "source_1",
            "currency": "JPY",
            "known_utc": str(BASE + known),
            "causal_state": causal,
        },
    }


def test_only_pre_entry_point_in_time_factor_is_decision_eligible():
    candidates = {
        "JPY": [
            {
                "candidate_id": "move_1", "instrument": "USD_JPY", "entry_epoch": BASE + 120,
                "exit_epoch": BASE + 300, "selected_side": "short", "base_currency": "USD",
                "quote_currency": "JPY",
            }
        ]
    }
    link = link_factors([_factor()], candidates)[0]
    assert link["relation"] == "pre_entry_causal"
    assert link["decision_time_eligible"] == 1
    assert link["forecast_proof_eligible"] == 0


def test_backfilled_or_late_factor_can_never_be_pre_entry_evidence():
    candidates = {
        "JPY": [
            {
                "candidate_id": "move_1", "instrument": "USD_JPY", "entry_epoch": BASE + 120,
                "exit_epoch": BASE + 300, "selected_side": "short", "base_currency": "USD",
                "quote_currency": "JPY",
            }
        ]
    }
    backfill = link_factors([_factor(causal="historical_backfill_only")], candidates)[0]
    late = link_factors([_factor(factor_id="factor_2", known=180, event=90)], candidates)[0]
    assert backfill["decision_time_eligible"] == 0
    assert backfill["relation"] == "ex_post_factor_overlap"
    assert late["decision_time_eligible"] == 0
    assert late["relation"] == "published_pre_entry_observed_late"
