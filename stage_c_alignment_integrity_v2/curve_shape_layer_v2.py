"""Bounded causal cross-horizon shape layer over preserved base forecasts."""
from collections import Counter
import math

from contracts import fingerprint
from magnitude_layer_v2 import weighted_ridge


def _origin(row):
    return row.get("decision_epoch", row.get("origin_epoch"))


def join_curve(rows, horizons, anchor_horizon=None):
    """Join an exact instrument/origin/base panel only after every horizon is ready."""
    expected = tuple(sorted(horizons))
    anchor_horizon = expected[0] if anchor_horizon is None else anchor_horizon
    if anchor_horizon not in expected:
        raise ValueError("curve_shape_anchor_not_in_horizons")
    grouped = {}
    for row in rows:
        origin = _origin(row)
        if not isinstance(origin, int):
            raise ValueError("curve_shape_origin_required")
        key = row["instrument"], origin, row["base_method"]
        part = grouped.setdefault(key, {})
        horizon = row["horizon_minutes"]
        if horizon not in expected or horizon in part:
            raise ValueError("curve_shape_duplicate_or_unexpected_horizon")
        if row["available_epoch"] < origin:
            raise ValueError("curve_shape_pre_origin_forecast")
        part[horizon] = row
    result = []
    for key, part in sorted(grouped.items()):
        if set(part) != set(expected):
            continue
        first = part[anchor_horizon]
        if any(row["record_id"] != first["record_id"] or row["instrument"] != first["instrument"]
               or row.get("decision_epoch", row.get("origin_epoch")) != key[1] for row in part.values()):
            raise ValueError("curve_shape_cross_horizon_identity_mismatch")
        result.append({"instrument": key[0], "decision_epoch": key[1], "base_method": key[2],
                       "record_id": first["record_id"], "target_id": first["target_id"],
                       "anchor_horizon_minutes": anchor_horizon,
                       "target_ids": {str(h): part[h]["target_id"] for h in expected},
                       "available_epoch": max(row["available_epoch"] for row in part.values()),
                       "horizons_minutes": list(expected),
                       "forecast_ids": {str(h): part[h]["forecast_id"] for h in expected},
                       "values": {str(h): part[h]["prediction_bps"] for h in expected}})
    return result


def features(row):
    values = [row["values"][str(h)] for h in row["horizons_minutes"]]
    if not all(isinstance(v, (int, float)) and math.isfinite(v) for v in values):
        raise ValueError("curve_shape_nonfinite_feature")
    anchor_horizon = row.get("anchor_horizon_minutes", row["horizons_minutes"][0])
    anchor = row["values"][str(anchor_horizon)]
    return [anchor, *[row["values"][str(h)] - anchor
                      for h in row["horizons_minutes"] if h != anchor_horizon]]


def fit_snapshot(rows, outcomes, cutoff, contract):
    eligible = []
    for row in rows:
        outcome = outcomes.get((row["record_id"], row["target_id"]))
        if outcome is None or _origin(row) >= cutoff or row["available_epoch"] > cutoff:
            continue
        if outcome["available_epoch"] > cutoff or outcome["value"] is None:
            continue
        eligible.append((row, outcome))
    origins = Counter(_origin(row) for row, _ in eligible)
    support = {"rows": len(eligible), "distinct_origins": len(origins),
               "distinct_utc_days": len({x // 86400 for x in origins}),
               "distinct_pairs": len({row["instrument"] for row, _ in eligible})}
    membership = [{"record_id": row["record_id"], "origin_epoch": _origin(row),
                   "forecast_ids": row["forecast_ids"], "label_sha256": fingerprint(outcome)}
                  for row, outcome in eligible]
    support["membership_sha256"] = fingerprint(membership)
    status = "insufficient_distinct_support"
    parameters = None
    if all(support[key] >= contract["minimum_" + key] for key in ("distinct_origins", "distinct_utc_days", "distinct_pairs")):
        weights = [len(eligible) / (len(origins) * origins[row["decision_epoch"]]) for row, _ in eligible]
        parameters = weighted_ridge([features(row) for row, _ in eligible],
                                    [outcome["value"] for _, outcome in eligible], weights,
                                    contract["ridge_lambda"])
        status = "fitted"
    snapshot = {"schema_version": "forex_curve_shape_snapshot.v1", "cutoff_epoch": cutoff,
                "status": status, "support": support, "training_membership": membership,
                "contract": contract, "contract_sha256": fingerprint(contract), "parameters": parameters,
                "base_models_refitted": False}
    snapshot["layer_id"] = fingerprint(snapshot)
    return snapshot


def apply(row, snapshot):
    if snapshot["layer_id"] != fingerprint({k: v for k, v in snapshot.items() if k != "layer_id"}):
        raise ValueError("curve_shape_snapshot_identity")
    if snapshot["cutoff_epoch"] > _origin(row) or snapshot["status"] != "fitted":
        return None
    model = snapshot["parameters"]
    raw = features(row)
    normalized = [(value - mean) / scale for value, mean, scale in zip(raw, model["mean"], model["scale"])]
    value = model["coefficient"][0] + sum(a * b for a, b in zip(model["coefficient"][1:], normalized))
    if not math.isfinite(value):
        raise ValueError("curve_shape_nonfinite_prediction")
    result = {**row, "variant": "curve_shape", "prediction_bps": value, "layer_id": snapshot["layer_id"],
              "layer_definition_sha256": fingerprint({"family": "curve_shape", "layer_id": snapshot["layer_id"]}),
              "outcomes_revealed": False, "native_policy_admitted": False}
    result["forecast_id"] = fingerprint({k: v for k, v in result.items() if k != "forecast_id"})
    return result
