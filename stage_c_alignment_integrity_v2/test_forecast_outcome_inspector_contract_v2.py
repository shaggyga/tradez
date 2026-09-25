import pytest

from contracts import TrainingView, forecast_record, outcome_record
from forecast_outcome_inspector_contract_v2 import (
    inspect_forecast,
    reconcile_forecast_publication,
    validate_outcome,
)


VIEW = TrainingView(
    origin_start_epoch=1,
    origin_end_epoch=10,
    fit_cutoff_epoch=20,
    protected_eval_start_epoch=30,
    protected_eval_end_epoch=40,
)


def forecast(fid="f1", instrument="EUR_USD", prediction=1.25):
    return forecast_record(
        forecast_id=fid,
        instrument=instrument,
        decision_epoch=100,
        available_epoch=105,
        model_id="ridge-retained",
        model_ready_epoch=90,
        training_view=VIEW,
        target_id="technical_endpoint_midpoint_elapsed_1440m",
        prediction=prediction,
    )


def coverage(fid="f1", status="eligible", reason="eligible"):
    return {
        "forecast_id": fid,
        "instrument": "EUR_USD",
        "decision_epoch": 100,
        "target_id": "technical_endpoint_midpoint_elapsed_1440m",
        "model_id": "ridge-retained",
        "status": status,
        "reason": reason,
    }


def test_reconcile_preserves_pending_outcome_placeholders_without_reveal():
    result = reconcile_forecast_publication(
        forecasts=[forecast()],
        outcomes=[outcome_record(forecast_id="f1", outcome_ready_epoch=1000, state="PENDING", value=None)],
        coverage=[coverage()],
    )
    assert result["accepted"] is True
    assert result["forecast_rows"] == 1 and result["outcome_rows"] == 1
    assert result["status_counts"] == {"eligible": 1}
    assert result["outcome_state_counts"] == {"PENDING": 1}
    assert result["outcomes_revealed"] is False


def test_blocked_wait_rows_are_counted_and_must_not_have_forecasts():
    result = reconcile_forecast_publication(
        forecasts=[],
        outcomes=[],
        coverage=[coverage(fid="blocked-EUR_USD", status="blocked", reason="model_not_ready")],
    )
    assert result["accepted"] is True
    assert result["reason_counts"] == {"model_not_ready": 1}
    assert result["support"][0]["has_forecast"] is False


def test_identity_mismatch_and_unregistered_forecast_fail_contract():
    result = reconcile_forecast_publication(
        forecasts=[forecast(instrument="USD_JPY"), forecast(fid="extra")],
        outcomes=[outcome_record(forecast_id="f1", outcome_ready_epoch=1000, state="PENDING", value=None)],
        coverage=[coverage()],
    )
    errors = {(row["forecast_id"], row["error"]) for row in result["errors"]}
    assert ("f1", "forecast_coverage_identity_mismatch") in errors
    assert ("extra", "forecast_without_coverage") in errors
    assert result["accepted"] is False


def test_inspection_hides_outcome_until_explicit_reveal_and_asof():
    f = forecast()
    o = outcome_record(forecast_id="f1", outcome_ready_epoch=1000, state="MATURED", value=2.5)
    before_publication = inspect_forecast(forecast_id="f1", forecasts=[f], outcomes=[o], asof_epoch=104)
    blind = inspect_forecast(forecast_id="f1", forecasts=[f], outcomes=[o], asof_epoch=105)
    early = inspect_forecast(forecast_id="f1", forecasts=[f], outcomes=[o], asof_epoch=999, reveal_outcome=True)
    revealed = inspect_forecast(forecast_id="f1", forecasts=[f], outcomes=[o], asof_epoch=1000, reveal_outcome=True)
    assert before_publication["status"] == "not_yet_available" and before_publication["forecast"] is None
    assert blind["forecast"] == f and blind["outcome"] is None and blind["outcomes_revealed"] is False
    assert early["outcome"] == {"status": "not_yet_available", "outcome_ready_epoch": 1000}
    assert revealed["outcome"] == o and revealed["outcomes_revealed"] is True


@pytest.mark.parametrize(
    "row,error",
    [
        ({"schema_version": "outcome.v2", "forecast_id": "f", "outcome_ready_epoch": 1, "state": "MATURED", "value": None}, "matured"),
        ({"schema_version": "outcome.v2", "forecast_id": "f", "outcome_ready_epoch": 1, "state": "PENDING", "value": 1.0}, "unmatured"),
        ({"schema_version": "outcome.v2", "forecast_id": "f", "outcome_ready_epoch": 1, "state": "BOGUS", "value": None}, "unknown"),
    ],
)
def test_outcome_state_contract(row, error):
    with pytest.raises(ValueError, match=error):
        validate_outcome(row)


def test_reveal_flag_must_be_real_boolean():
    with pytest.raises(ValueError, match="explicit_boolean"):
        inspect_forecast(forecast_id="f1", forecasts=[forecast()], outcomes=[], asof_epoch=105, reveal_outcome=1)
