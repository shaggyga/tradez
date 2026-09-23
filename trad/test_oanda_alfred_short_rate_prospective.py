import json

import oanda_alfred_short_rate_prospective as worker


def test_short_rate_contract_is_separate_fail_closed_source_cohort():
    config = json.loads(worker.CONFIG.read_text(encoding="utf-8"))
    currencies = {row["currency"] for row in config["series"]}
    assert len(config["series"]) == 17
    assert currencies == {
        "AUD", "CAD", "CHF", "CZK", "DKK", "EUR", "GBP", "HUF",
        "JPY", "MXN", "NOK", "NZD", "PLN", "SEK", "TRY", "USD", "ZAR",
    }
    assert all(
        row["cross_currency_frequency_group"]
        == "monthly_short_term_interest_rate"
        for row in config["series"]
    )
    assert config["contract"]["initial_current_view_is_bootstrap_only"] is True
    assert config["contract"]["direction_policy"] == "abstain"
    assert config["contract"]["execution_eligible"] is False
    assert worker.DATABASE.name != "alfred_vintage_prospective_v1.sqlite"
