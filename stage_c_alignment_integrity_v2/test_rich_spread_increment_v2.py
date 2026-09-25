import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "trad"))

from rich_spread_increment_v2 import NAME, paired_metrics, selected_view


def test_selected_mask_is_ordered_and_uses_only_retained_values():
    source = {"feature_names": ["a", "b", "c"], "values": [1.0, None, 3.0], "record_id": "row"}
    item = selected_view(source, ["c", "a"])
    assert item["group"] == NAME
    assert item["feature_names"] == ["c", "a"]
    assert item["values"] == [3.0, 1.0]
    assert item["record_id"] == "row"
    with pytest.raises(ValueError, match="source_schema_mismatch"):
        selected_view(source, ["future_unknown"])


def _row(prediction, *, record="A:1000", origin=1000, feature_id="pinned"):
    return {"record_id": record, "method": "ridge", "procedure": "frozen",
            "original_rich_sha256": feature_id,
            "forecast": {"target_id": "target", "decision_epoch": origin,
                         "available_epoch": origin + 2, "prediction": prediction, "instrument": "A"}}


def test_matched_scoring_requires_same_record_input_and_mature_outcome():
    outcomes = [{"record_id": "A:1000", "target_id": "target", "value": 5.0,
                 "available_epoch": 2000}]
    scored = paired_metrics([_row(4.0)], [_row(3.0)], outcomes, 2000, "target", "frozen")
    overall = next(x for x in scored if x["stratum"] == "overall")
    assert overall["matched_mature_rows"] == 1
    assert overall["mae_delta_bps"] == -1.0
    assert overall["mse_delta_bps2"] == -3.0
    assert any(x["stratum"] == "origin" for x in scored)
    assert any(x["stratum"] == "utc_day" for x in scored)
    with pytest.raises(ValueError, match="input_identity_mismatch"):
        paired_metrics([_row(4.0)], [_row(3.0, feature_id="changed")], outcomes, 2000, "target", "frozen")
    with pytest.raises(ValueError, match="no_mature_matched_support"):
        paired_metrics([_row(4.0)], [_row(3.0)], outcomes, 1999, "target", "frozen")
