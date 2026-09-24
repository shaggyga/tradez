"""Fixed, outcome-independent blend of two retained forecast records."""
from collections import defaultdict
import math
import random

from contracts import fingerprint, validate_forecast

METHODS = ("ridge", "recovered_hgb")
BLEND_METHOD = "fixed_equal_half_blend"
ZERO_METHOD = "zero_return_diagnostic"


def target(minutes):
    return f"technical_endpoint_midpoint_elapsed_{minutes}m"


def _key(row):
    forecast = row["forecast"]
    return (row["record_id"], forecast["instrument"], forecast["target_id"],
            forecast["decision_epoch"], row["procedure"], row["selected_fit_id"],
            row["selected_fit_cutoff"])


def _finite(value, reason):
    if type(value) not in (int, float) or not math.isfinite(value):
        raise ValueError(reason)
    return float(value)


def pair_forecasts(rows, group, minutes, procedure):
    """Return exact matched base records; no coverage fallback is permitted."""
    by_key = defaultdict(dict)
    expected_target = target(minutes)
    for row in rows:
        if row["group"] != group or row["procedure"] != procedure:
            raise ValueError("blend_source_scope_mismatch")
        forecast = row["forecast"]
        validate_forecast(forecast)
        if forecast["target_id"] != expected_target or row["method"] not in METHODS:
            raise ValueError("blend_source_target_or_method_mismatch")
        key = _key(row)
        if row["method"] in by_key[key]:
            raise ValueError("duplicate_blend_base_forecast")
        by_key[key][row["method"]] = row
    paired = {}
    for key, values in by_key.items():
        if set(values) == set(METHODS):
            ridge, hgb = values["ridge"]["forecast"], values["recovered_hgb"]["forecast"]
            for field in ("instrument", "target_id", "decision_epoch", "available_epoch", "model_ready_epoch"):
                if ridge[field] != hgb[field]:
                    raise ValueError("blend_base_identity_mismatch:" + field)
            paired[key] = values
        elif set(values) not in ({"ridge"}, {"recovered_hgb"}):
            raise ValueError("invalid_blend_base_method_set")
    return paired


def outcome_map(reader, universe):
    outcomes = {}
    for instrument in universe:
        part = reader.read("technical", "pair_" + instrument + ".json")
        for item in part["outcomes"]:
            key = (item["record_id"], item["target_id"])
            if key in outcomes:
                raise ValueError("duplicate_original_blend_outcome")
            outcomes[key] = item
    return outcomes


def build_chunk(rows, coverage, outcomes, contract):
    group, minutes, procedure = contract["group"], contract["horizon_minutes"], contract["procedure"]
    pairs = pair_forecasts(rows, group, minutes, procedure)
    pairs_by_coverage = {}
    for forecast_key, values in pairs.items():
        coverage_key = forecast_key[:5] + (forecast_key[6],)
        if coverage_key in pairs_by_coverage:
            raise ValueError("duplicate_blend_fit_identity_for_coverage")
        pairs_by_coverage[coverage_key] = values
    coverage_by = defaultdict(dict)
    for item in coverage:
        if item["group"] != group or item["procedure"] != procedure or item["target_id"] != target(minutes):
            raise ValueError("blend_coverage_scope_mismatch")
        # The retained coverage schema has the selected cutoff but intentionally does
        # not repeat the fit identifier. Exact fit-id matching is checked from paired
        # forecast records below; coverage may not invent it.
        key = (item["record_id"], item["instrument"], item["target_id"], item["decision_epoch"],
               item["procedure"], item["selected_fit_cutoff"])
        method = item["method"]
        if method not in METHODS or method in coverage_by[key]:
            raise ValueError("blend_coverage_identity_mismatch")
        coverage_by[key][method] = item
    output, output_coverage = [], []
    for key in sorted(coverage_by):
        base_coverage = coverage_by[key]
        if set(base_coverage) != set(METHODS):
            raise ValueError("blend_base_coverage_pair_required")
        ridge_cov, hgb_cov = base_coverage["ridge"], base_coverage["recovered_hgb"]
        if ridge_cov["reason"] != hgb_cov["reason"]:
            raise ValueError("blend_base_coverage_reason_mismatch")
        status = ridge_cov["reason"]
        paired = pairs_by_coverage.get(key)
        base = {"record_id": key[0], "instrument": key[1], "target_id": key[2], "decision_epoch": key[3],
                "group": group, "procedure": procedure, "selected_fit_id": None, "selected_fit_cutoff": key[5],
                "base_coverage_reason": status, "base_methods": list(METHODS)}
        if status != "eligible":
            output_coverage.append({**base, "reason": "base_" + status, "blend_available": False})
            continue
        if paired is None:
            raise ValueError("eligible_blend_base_forecasts_missing")
        ridge, hgb = paired["ridge"], paired["recovered_hgb"]
        if ridge["selected_fit_cutoff"] != key[5] or hgb["selected_fit_cutoff"] != key[5]:
            raise ValueError("blend_coverage_fit_cutoff_mismatch")
        rp, hp = _finite(ridge["forecast"]["prediction"], "nonfinite_ridge_prediction"), _finite(hgb["forecast"]["prediction"], "nonfinite_hgb_prediction")
        available = max(ridge["forecast"]["available_epoch"], hgb["forecast"]["available_epoch"])
        common = {**base, "selected_fit_id": ridge["selected_fit_id"], "ridge_forecast_id": ridge["forecast"]["forecast_id"], "recovered_hgb_forecast_id": hgb["forecast"]["forecast_id"],
                  "ridge_prediction_bps": rp, "recovered_hgb_prediction_bps": hp, "available_epoch": available,
                  "base_model_ids": {"ridge": ridge["forecast"]["model_id"], "recovered_hgb": hgb["forecast"]["model_id"]},
                  "base_model_ready_epochs": {"ridge": ridge["forecast"]["model_ready_epoch"], "recovered_hgb": hgb["forecast"]["model_ready_epoch"]},
                  "base_source_references": {"ridge": ridge.get("source_reference"), "recovered_hgb": hgb.get("source_reference")},
                  "source_records": {"ridge": fingerprint(ridge), "recovered_hgb": fingerprint(hgb)}}
        blend = .5 * (rp + hp)
        output.append({**common, "blend_forecast_id": fingerprint({"bases": common["source_records"], "weight": "1/2", "available_epoch": available}),
                       "blend_prediction_bps": blend, "zero_prediction_bps": 0.0,
                       "weight_rule": "fixed_equal_half", "outcomes_revealed": False})
        output_coverage.append({**base, "reason": "eligible", "blend_available": True, "available_epoch": available})
    return output, output_coverage


def mature_rows(rows, outcomes, assessment_asof):
    selected = []
    for row in rows:
        outcome = outcomes.get((row["record_id"], row["target_id"]))
        if outcome is None:
            raise ValueError("missing_registered_blend_outcome")
        value = outcome.get("value")
        if value is None:
            continue
        _finite(value, "nonfinite_blend_outcome")
        if outcome["available_epoch"] > assessment_asof:
            continue
        if outcome["available_epoch"] < outcome["label_end_epoch"]:
            raise ValueError("blend_label_clock_invalid")
        selected.append({**row, "outcome_bps": float(value), "outcome_available_epoch": outcome["available_epoch"],
                         "label_end_epoch": outcome["label_end_epoch"], "outcomes_revealed": True})
    return selected


def _metrics(rows):
    if not rows:
        return None
    names = {"ridge": "ridge_prediction_bps", "recovered_hgb": "recovered_hgb_prediction_bps", BLEND_METHOD: "blend_prediction_bps", ZERO_METHOD: "zero_prediction_bps"}
    result = {"rows": len(rows), "distinct_origins": len({r["decision_epoch"] for r in rows}),
              "distinct_instruments": len({r["instrument"] for r in rows})}
    errors = {}
    for method, name in names.items():
        values = [r[name] - r["outcome_bps"] for r in rows]
        errors[method] = values
        result[method] = {"mae_bps": math.fsum(abs(x) for x in values) / len(values),
                          "mse_bps2": math.fsum(x * x for x in values) / len(values),
                          "mean_signed_error_bps": math.fsum(values) / len(values)}
    for base in METHODS:
        base_name = names[base]; blend_name = names[BLEND_METHOD]
        result["paired_delta_" + BLEND_METHOD + "_vs_" + base] = {
            "mae_bps": math.fsum(abs(r[blend_name] - r["outcome_bps"]) - abs(r[base_name] - r["outcome_bps"]) for r in rows) / len(rows),
            "mse_bps2": math.fsum((r[blend_name] - r["outcome_bps"]) ** 2 - (r[base_name] - r["outcome_bps"]) ** 2 for r in rows) / len(rows)}
    a, b = errors["ridge"], errors["recovered_hgb"]
    am, bm = math.fsum(a) / len(a), math.fsum(b) / len(b)
    result["ridge_hgb_error_covariance_bps2"] = math.fsum((x - am) * (y - bm) for x, y in zip(a, b)) / len(a)
    return result


def summarize(rows, contract):
    mature = mature_rows(rows, contract["outcomes"], contract["assessment_asof_epoch"])
    by_origin, by_day = defaultdict(list), defaultdict(list)
    for row in mature:
        by_origin[row["decision_epoch"]].append(row)
        by_day[row["decision_epoch"] // 86400].append(row)
    return {"mature_rows": len(mature), "all_rows": len(rows), "overall": _metrics(mature),
            "by_origin": [{"origin_epoch": k, "metrics": _metrics(v)} for k, v in sorted(by_origin.items())],
            "by_utc_day": [{"utc_day": k, "metrics": _metrics(v)} for k, v in sorted(by_day.items())]}


def block_sensitivity(rows, contract):
    mature = mature_rows(rows, contract["outcomes"], contract["assessment_asof_epoch"])
    origins = sorted({r["decision_epoch"] for r in mature})
    result = []
    for length in (4, 8, 20):
        if len(origins) < length:
            result.append({"block_length": length, "status": "insufficient_distinct_origins", "interval": None})
            continue
        rng = random.Random(20260922 + length)
        deltas = {base: [] for base in METHODS}
        by_origin = {o: [r for r in mature if r["decision_epoch"] == o] for o in origins}
        # Sampling an origin always retains all of its rows. Pre-aggregate its paired
        # absolute-error sums so the fixed 2,000-replicate sensitivity is exact but
        # stays within the declared single-worker five-minute wall limit.
        aggregates = {}
        for origin, origin_rows in by_origin.items():
            aggregates[origin] = {"rows": len(origin_rows), **{
                base: math.fsum(abs(row["blend_prediction_bps"] - row["outcome_bps"]) -
                                 abs(row[base + "_prediction_bps"] - row["outcome_bps"])
                                 for row in origin_rows) for base in METHODS}}
        for _ in range(2000):
            sample = []
            while len(sample) < len(origins):
                start = rng.randrange(len(origins))
                sample.extend(origins[(start + j) % len(origins)] for j in range(length))
            selected = sample[:len(origins)]
            row_count = sum(aggregates[o]["rows"] for o in selected)
            for base in METHODS:
                deltas[base].append(math.fsum(aggregates[o][base] for o in selected) / row_count)
        intervals = {"blend_vs_" + base + "_mae_delta_bps_interval_10_90": [sorted(values)[199], sorted(values)[1799]]
                     for base, values in deltas.items()}
        result.append({"block_length": length, "status": "descriptive_only", "replicates": 2000,
                       "seed": 20260922, **intervals,
                       "no_effective_n_or_p_value": True})
    return result
