"""Prequential residual retention layer over preserved currency projections."""
from collections import Counter, defaultdict
import math

from contracts import fingerprint

CONTROLS = ("direct", "currency_projection", "half_residual")


def _log_bps(value):
    if not isinstance(value, (int, float)) or not math.isfinite(value) or value <= -10000:
        raise ValueError("residual_layer_invalid_return")
    return math.log1p(value / 10000.0) * 10000.0


def _simple_bps(value):
    result = math.expm1(value / 10000.0) * 10000.0
    if not math.isfinite(result):
        raise ValueError("residual_layer_nonfinite_prediction")
    return result


def _validate(row):
    if row["record_id"] != f"{row['instrument']}:{row['origin_epoch']}":
        raise ValueError("residual_layer_record_identity")
    if row["target_epoch"] != row["origin_epoch"] + row["horizon_minutes"] * 60:
        raise ValueError("residual_layer_target_clock")
    if not row["origin_epoch"] <= row["available_epoch"] < row["target_epoch"]:
        raise ValueError("residual_layer_forecast_not_pre_target")
    if row["variant"] not in CONTROLS:
        raise ValueError("residual_layer_unknown_control")
    if row["forecast_id"] != fingerprint({k: v for k, v in row.items() if k != "forecast_id"}):
        raise ValueError("residual_layer_forecast_identity")
    _log_bps(row["prediction_bps"])


def _outcome(row, outcomes):
    value = outcomes.get((row["record_id"], row["target_id"]))
    if (value is None or value["record_id"] != row["record_id"]
            or value["target_id"] != row["target_id"]
            or value["label_end_epoch"] != row["target_epoch"]):
        raise ValueError("residual_layer_exact_label_required")
    if value["available_epoch"] < value["label_end_epoch"]:
        raise ValueError("residual_layer_label_before_endpoint")
    if value["value"] is not None:
        _log_bps(value["value"])
    return value


def group_controls(rows):
    grouped = {}
    for row in rows:
        _validate(row)
        key = (row["base_method"], row["horizon_minutes"], row["record_id"])
        if key not in grouped:
            grouped[key] = {}
        if row["variant"] in grouped[key]:
            raise ValueError("residual_layer_duplicate_control")
        grouped[key][row["variant"]] = row
    if any(set(part) != set(CONTROLS) for part in grouped.values()):
        raise ValueError("residual_layer_exact_control_support_required")
    return grouped


def fit_snapshot(rows, outcomes, cutoff_epoch, contract, expected_scopes=None):
    if not isinstance(cutoff_epoch, int):
        raise ValueError("residual_layer_integer_cutoff_required")
    grouped = group_controls(rows)
    members = []
    for key, controls in grouped.items():
        direct = controls["direct"]
        outcome = _outcome(direct, outcomes)
        for name in ("currency_projection", "half_residual"):
            if any(controls[name][field] != direct[field] for field in ("record_id", "target_id", "origin_epoch", "target_epoch", "horizon_minutes", "base_method")):
                raise ValueError("residual_layer_unmatched_controls")
        if (direct["origin_epoch"] >= cutoff_epoch or max(item["available_epoch"] for item in controls.values()) > cutoff_epoch
                or outcome["available_epoch"] > cutoff_epoch or outcome["value"] is None):
            continue
        members.append((key, controls, outcome))
    members.sort(key=lambda item: (item[1]["direct"]["origin_epoch"], item[1]["direct"]["instrument"], item[0][0]))
    by_scope = defaultdict(list)
    # The caller supplies the frozen experiment grid.  Emit a diagnostic
    # snapshot for every scope, including ones that have no eligible training
    # labels, rather than making missing scopes disappear from the report.
    for scope in expected_scopes or ():
        if not (isinstance(scope, tuple) and len(scope) == 2):
            raise ValueError("residual_layer_invalid_expected_scope")
        by_scope[scope]
    for key, controls, outcome in members:
        by_scope[key[:2]].append((controls, outcome))
    snapshots = {}
    for (base, horizon), part in sorted(by_scope.items()):
        origins = Counter(controls["direct"]["origin_epoch"] for controls, _ in part)
        days = {origin // 86400 for origin in origins}
        membership = [{"record_id": controls["direct"]["record_id"],
                       "origin_epoch": controls["direct"]["origin_epoch"],
                       "instrument": controls["direct"]["instrument"],
                       "direct_forecast_id": controls["direct"]["forecast_id"],
                       "projection_forecast_id": controls["currency_projection"]["forecast_id"],
                       "half_residual_forecast_id": controls["half_residual"]["forecast_id"],
                       "label_sha256": fingerprint(outcome)}
                      for controls, outcome in part]
        support = {
            "rows": len(part), "distinct_origins": len(origins), "distinct_utc_days": len(days),
            "distinct_pairs": len({controls["direct"]["instrument"] for controls, _ in part}),
            "membership_sha256": fingerprint(membership), "training_membership": membership,
        }
        status, weight = "insufficient_distinct_support", None
        if not part:
            status = "no_mature_training_support"
        if part and (support["distinct_origins"] >= contract["minimum_distinct_origins"]
                and support["distinct_utc_days"] >= contract["minimum_distinct_utc_days"]
                and support["distinct_pairs"] >= contract["minimum_distinct_pairs"]):
            terms = []
            for controls, outcome in part:
                projection = _log_bps(controls["currency_projection"]["prediction_bps"])
                residual = _log_bps(controls["direct"]["prediction_bps"]) - projection
                target = _log_bps(outcome["value"]) - projection
                terms.append((1.0 / origins[controls["direct"]["origin_epoch"]], residual, target))
            denominator = math.fsum(q * x * x for q, x, _ in terms)
            numerator = math.fsum(q * x * y for q, x, y in terms)
            if denominator == 0:
                status = "unidentifiable_zero_residual"
            else:
                weight = min(contract["maximum_residual_weight"], max(contract["minimum_residual_weight"], numerator / denominator))
                status = "fitted"
        snapshot = {"schema_version": "forex_currency_projection_residual_snapshot.v1", "base_method": base, "horizon_minutes": horizon,
                    "cutoff_epoch": cutoff_epoch, "status": status, "residual_weight": weight, "support": support,
                    "contract": contract, "contract_sha256": fingerprint(contract),
                    "base_models_refitted": False, "outcomes_revealed": False}
        snapshot["layer_id"] = fingerprint(snapshot)
        snapshots[base, horizon] = snapshot
    return snapshots


def apply(rows, snapshots):
    grouped = group_controls(rows)
    result = []
    for (base, horizon, record_id), controls in sorted(grouped.items(), key=lambda item: (item[1]["direct"]["origin_epoch"], item[0])):
        snapshot = snapshots.get((base, horizon))
        if snapshot is None:
            continue
        if snapshot["layer_id"] != fingerprint({k: v for k, v in snapshot.items() if k != "layer_id"}):
            raise ValueError("residual_layer_snapshot_identity")
        direct, projected = controls["direct"], controls["currency_projection"]
        half = controls["half_residual"]
        if any(item["base_method"] != base or item["horizon_minutes"] != horizon
               or item["record_id"] != record_id for item in controls.values()):
            raise ValueError("residual_layer_application_scope_mismatch")
        for name in ("currency_projection", "half_residual"):
            if any(controls[name][field] != direct[field] for field in (
                    "record_id", "target_id", "origin_epoch", "target_epoch",
                    "horizon_minutes", "base_method", "instrument")):
                raise ValueError("residual_layer_application_control_mismatch")
        if snapshot["base_method"] != base or snapshot["horizon_minutes"] != horizon:
            raise ValueError("residual_layer_snapshot_scope_mismatch")
        if snapshot["contract_sha256"] != fingerprint(snapshot.get("contract", {})):
            raise ValueError("residual_layer_snapshot_contract_binding")
        if snapshot["cutoff_epoch"] > direct["origin_epoch"]:
            raise ValueError("residual_layer_future_snapshot")
        if snapshot["status"] != "fitted":
            continue
        value = _simple_bps(_log_bps(projected["prediction_bps"]) + snapshot["residual_weight"] * (_log_bps(direct["prediction_bps"]) - _log_bps(projected["prediction_bps"])))
        definition = {"family": "currency_projection_residual_scalar", "contract_sha256": snapshot["contract_sha256"],
                      "base_method": base, "horizon_minutes": horizon, "layer_id": snapshot["layer_id"]}
        row = {**direct, "variant": "learned_residual", "method": base + "__learned_residual", "prediction_bps": value,
               "available_epoch": max(item["available_epoch"] for item in controls.values()), "layer_id": snapshot["layer_id"],
               "residual_weight": snapshot["residual_weight"], "outcomes_revealed": False, "native_policy_admitted": False,
               "parent_direct_forecast_id": direct["forecast_id"], "parent_projection_forecast_id": projected["forecast_id"],
               "parent_half_residual_forecast_id": half["forecast_id"], "layer_definition_sha256": fingerprint(definition)}
        row["forecast_id"] = fingerprint({k: v for k, v in row.items() if k != "forecast_id"})
        result.append(row)
    return result


def score(rows, outcomes, asof):
    groups = defaultdict(list)
    for row in rows:
        outcome = _outcome(row, outcomes)
        if row["available_epoch"] <= asof and outcome["available_epoch"] <= asof and outcome["value"] is not None:
            groups[row["base_method"], row["horizon_minutes"]].append((row, outcome))
    result = []
    for (base, horizon), part in sorted(groups.items()):
        errors = [row["prediction_bps"] - outcome["value"] for row, outcome in part]
        result.append({"base_method": base, "horizon_minutes": horizon, "mature_rows": len(part),
                       "distinct_origins": len({row["origin_epoch"] for row, _ in part}), "distinct_pairs": len({row["instrument"] for row, _ in part}),
                       "support_sha256": fingerprint(sorted(row["record_id"] for row, _ in part)), "mae_bps": math.fsum(map(abs, errors)) / len(errors),
                       "mse_bps2": math.fsum(error * error for error in errors) / len(errors), "bias_bps": math.fsum(errors) / len(errors)})
    return result
