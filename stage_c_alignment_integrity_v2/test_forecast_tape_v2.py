import pytest

from contracts import fingerprint
from forecast_tape_v2 import assemble


def row(pair="A", origin=1):
    value = {"record_id": f"{pair}:{origin}", "instrument": pair, "origin_epoch": origin,
             "target_id": "target", "available_epoch": origin + 1, "prediction_bps": 1.0,
             "variant": "direct"}
    value["forecast_id"] = fingerprint(value)
    return value


def test_forecast_tape_preserves_identity_and_availability():
    tape = assemble({"parent": [row()]})
    assert tape["coverage"] == {"records": 1, "sources": {"parent": 1}, "instruments": 1, "origins": 1}
    assert tape["records"][0]["available_epoch"] == 2


def test_forecast_tape_refuses_tampered_row():
    value = row()
    value["prediction_bps"] = 2.0
    with pytest.raises(ValueError, match="forecast_identity"):
        assemble({"parent": [value]})
