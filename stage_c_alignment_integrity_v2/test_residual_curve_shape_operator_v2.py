import hashlib
import json

import pytest

from contracts import fingerprint
from residual_curve_shape_operator_v2 import _authenticated_residual


def test_residual_result_requires_pinned_lineage_and_forecast_identity(tmp_path):
    row = {"variant": "learned_residual", "record_id": "A:1", "prediction_bps": 1.0}
    row["forecast_id"] = fingerprint(row)
    path = tmp_path / "residual.json"
    path.write_text(json.dumps({"parent": {"run": "pinned"}, "learned_rows": [row]}), encoding="utf-8")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    assert _authenticated_residual(path, digest, {"run": "pinned"}) == [row]
    with pytest.raises(ValueError, match="parent_lineage"):
        _authenticated_residual(path, digest, {"run": "other"})
    with pytest.raises(ValueError, match="pin_mismatch"):
        _authenticated_residual(path, "0" * 64, {"run": "pinned"})
