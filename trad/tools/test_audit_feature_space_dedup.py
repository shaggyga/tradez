"""Regression checks for outcome leakage and exact numeric duplicate handling."""
import csv
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from tools.audit_feature_space_dedup import audit, registered_inputs

def registry(path, names):
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(["feature_name", "model_input", "causal"])
        writer.writerows((name, "True", "True") for name in names)

def test_registered_inputs_exclude_outcomes_and_preserve_source(tmp_path):
    source, reg = tmp_path / "data.parquet", tmp_path / "registry.csv"
    table = pa.table({"window1": [0., 1., None], "window15": [-0., 1., None],
                      "distinct_missingness": [0., None, 1.], "constant": [3., 3., 3.],
                      "all_missing": pa.array([None] * 3, type=pa.float64()),
                      "target_direction_60": [1., 0., 1.], "diag_long_net_pips_60": [1., 2., 3.],
                      "mid_close": [1., 1.1, 1.2]})
    pq.write_table(table, source)
    registry(reg, ["window1", "window15", "distinct_missingness", "constant", "all_missing"])
    before = source.read_bytes()
    result = audit(source, reg)
    assert source.read_bytes() == before
    assert result["representative_numeric_columns"] == ["window1", "distinct_missingness"]
    assert result["duplicate_representatives"] == {"window15": "window1"}
    assert result["constant_columns"] == ["constant", "all_missing"]
    assert set(result["excluded_non_input_columns"]) == {"target_direction_60", "diag_long_net_pips_60", "mid_close"}

@pytest.mark.parametrize("names,error", [
    (["target_direction_60"], "outcome_as_input"),
    (["diag_long_net_pips_60"], "outcome_as_input"),
    (["momentum", "momentum"], "duplicate_feature_name"),
])
def test_bad_registry_fails_closed(tmp_path, names, error):
    reg = tmp_path / "registry.csv"
    registry(reg, names)
    with pytest.raises(ValueError, match=error):
        registered_inputs(reg)

def test_missing_registered_field_fails_closed(tmp_path):
    source, reg = tmp_path / "data.parquet", tmp_path / "registry.csv"
    pq.write_table(pa.table({"other": [1.]}), source)
    registry(reg, ["missing"])
    with pytest.raises(ValueError, match="registered_input_missing"):
        audit(source, reg)
