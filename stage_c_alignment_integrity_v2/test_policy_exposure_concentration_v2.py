import pytest

from policy_exposure_concentration_v2 import summarize_event_exposures


def event(epoch, sequence, gross):
    exposure = {} if gross is None else {"EUR": {"gross_usd_conservative": str(gross),
        "conversion": {"currency": "EUR"}}}
    return {"epoch": epoch, "sequence": sequence, "arms": {"continuation": {"currency_exposures": exposure}}}


def test_last_same_epoch_state_and_elapsed_seconds_are_used():
    rows = [event(100, 0, None), event(100, 1, 200), event(110, 2, 100), event(120, 3, None)]
    report = summarize_event_exposures(rows, ["continuation"], terminal_epoch=120)
    arm = report["arms"]["continuation"]
    assert report["observed_span_seconds"] == 20
    assert arm["active_portfolio_seconds"] == 20
    assert arm["currencies"]["EUR"]["time_weighted_mean_gross_usd_over_full_span"] == "150"
    assert arm["currencies"]["EUR"]["peak_gross_usd"] == "200"


def test_missing_terminal_and_negative_exposure_are_refused():
    with pytest.raises(ValueError, match="explicit_terminal_event_required"):
        summarize_event_exposures([event(100, 0, 1), event(110, 1, 1)], ["continuation"], terminal_epoch=120)
    with pytest.raises(ValueError, match="negative_currency_gross_exposure"):
        summarize_event_exposures([event(100, 0, -1), event(120, 1, None)], ["continuation"], terminal_epoch=120)
