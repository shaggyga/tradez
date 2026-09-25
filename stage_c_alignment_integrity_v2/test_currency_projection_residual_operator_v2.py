from currency_projection_residual_operator_v2 import _coverage, _scope_coverage


def test_scope_coverage_keeps_absent_parent_origins_visible():
    snapshots = {
        (100, "ridge", 360): {"status": "fitted"},
        (200, "ridge", 360): {"status": "no_mature_training_support"},
    }
    coverage = _coverage([], [], snapshots, {}, 999, [("ridge", 360)])
    result = _scope_coverage(coverage, snapshots, [("ridge", 360)], [100, 200])
    assert [(row["origin_epoch"], row["snapshot_status"], row["direct_control_rows"]) for row in result] == [
        (100, "fitted", 0), (200, "no_mature_training_support", 0)]


def test_coverage_distinguishes_issued_mature_and_unavailable():
    direct = {
        "base_method": "ridge", "horizon_minutes": 360, "record_id": "EUR_USD:100",
        "target_id": "target", "origin_epoch": 100, "instrument": "EUR_USD", "variant": "direct",
    }
    learned = [{**direct, "variant": "learned_residual"}]
    snapshots = {(100, "ridge", 360): {"status": "fitted"}}
    mature = _coverage([direct], learned, snapshots, {("EUR_USD:100", "target"): {
        "value": 1.0, "available_epoch": 999}}, 999, [("ridge", 360)])
    unavailable = _coverage([direct], learned, snapshots, {("EUR_USD:100", "target"): {
        "value": None, "available_epoch": 999}}, 999, [("ridge", 360)])
    assert mature[0]["forecast_issuance_status"] == "issued"
    assert mature[0]["label_status"] == "mature"
    assert unavailable[0]["label_status"] == "unavailable"
