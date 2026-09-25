"""Bounded spread/activity increment over preserved compact50 rich forecasts."""
from __future__ import annotations

import hashlib
import json
import os
import time
from collections import defaultdict
from pathlib import Path

from contracts import fingerprint
from publication import RunPublisher, effective_run_identity, verify_completed_run
from rich_family_models_v2 import METHODS, contract, fit_pair, predict, validate_fit
from rich_family_runner_v2 import checked, load_inputs
from rich_family_operator_v2 import dependency

NAME = "compact50_plus_spread_activity"
CONTROL = "compact50_cost2"
MAX_TOTAL_SECONDS = 900
MAX_FIT_SECONDS = 30


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def encoded(obj):
    return (json.dumps(obj, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()


def selected_view(full_view, names):
    ordered = full_view["feature_names"]
    index = {n: i for i, n in enumerate(ordered)}
    if len(index) != len(ordered) or not set(names).issubset(index):
        raise ValueError("spread_increment_source_schema_mismatch")
    values = [full_view["values"][index[n]] for n in names]
    return {**full_view, "group": NAME, "feature_names": names, "values": values,
            "missing_mask": [v is None for v in values]}


def paired_metrics(new_rows, control_rows, outcomes, asof, target, procedure):
    control = {(x["record_id"], x["method"]): x for x in control_rows}
    new = {(x["record_id"], x["method"]): x for x in new_rows}
    if len(control) != len(control_rows) or len(new) != len(new_rows) or set(control) != set(new):
        raise ValueError("spread_increment_unmatched_forecast_support")
    outcome_lookup = {(x["record_id"], x["target_id"]): x for x in outcomes}
    parts = defaultdict(list)
    for key, x in sorted(new.items()):
        y = control[key]
        nf, cf = x["forecast"], y["forecast"]
        if nf["target_id"] != target or cf["target_id"] != target or nf["decision_epoch"] != cf["decision_epoch"] or x["procedure"] != procedure or y["procedure"] != procedure:
            raise ValueError("spread_increment_control_identity_mismatch")
        if x["original_rich_sha256"] != y["original_rich_sha256"]:
            raise ValueError("spread_increment_input_identity_mismatch")
        outcome = outcome_lookup.get((x["record_id"], target))
        if outcome is None or outcome["value"] is None or outcome["available_epoch"] > asof:
            continue
        if max(nf["available_epoch"], cf["available_epoch"]) > asof:
            continue
        origin = nf["decision_epoch"]
        item = (x["record_id"], nf["prediction"] - outcome["value"],
                cf["prediction"] - outcome["value"], nf["instrument"])
        for stratum, value in (("overall", None), ("origin", origin), ("utc_day", origin // 86400)):
            parts[(x["method"], stratum, value)].append(item)
    result = []
    for (method, stratum, value), rows in sorted(parts.items(), key=lambda x: (x[0][0], x[0][1], str(x[0][2]))):
        if not rows:
            continue
        n = len(rows)
        a = [x[1] for x in rows]
        b = [x[2] for x in rows]
        result.append({"target_id": target, "procedure": procedure, "method": method,
                       "stratum": stratum, "stratum_value": value, "matched_mature_rows": n,
                       "distinct_pairs": len({x[3] for x in rows}),
                       "support_sha256": fingerprint(sorted(x[0] for x in rows)),
                       "new_mae_bps": sum(map(abs, a)) / n,
                       "control_mae_bps": sum(map(abs, b)) / n,
                       "mae_delta_bps": (sum(map(abs, a)) - sum(map(abs, b))) / n,
                       "mse_delta_bps2": (sum(x*x for x in a) - sum(x*x for x in b)) / n,
                       "bias_delta_bps": (sum(a) - sum(b)) / n})
    if not any(x["stratum"] == "overall" for x in result):
        raise ValueError("spread_increment_no_mature_matched_support")
    return result


def run(rich_root, baseline_root, old_rich_root, census_path, runs_root, *, resume=False):
    started = time.monotonic()
    census = json.loads(Path(census_path).read_text(encoding="utf-8"))
    candidate = census["selected_candidate"]
    if candidate["name"] != NAME or fingerprint(candidate["ordered_features"]) != candidate["mask_fingerprint"]:
        raise ValueError("spread_increment_candidate_identity_mismatch")
    rich = dependency(Path(rich_root))
    baseline = dependency(Path(baseline_root), exclude_timings=True)
    old = dependency(Path(old_rich_root), exclude_timings=True)
    if rich["identity"]["fingerprint"] != census["rich_run_identity"] or baseline["identity"]["fingerprint"] != census["baseline_run_identity"]:
        raise ValueError("spread_increment_parent_identity_mismatch")
    for path, dep in ((rich_root, rich), (baseline_root, baseline), (old_rich_root, old)):
        verify_completed_run(Path(path), dep["identity"])
    c = contract()
    names = candidate["ordered_features"]
    feature_sets = checked(Path(rich_root), rich, "feature_sets.json")
    universe = rich["identity"]["contract"]["recipe"]["universe"]
    if len(universe) != 68 or feature_sets[CONTROL] != names[:52] + names[-2:]:
        # The candidate inserts nine source-defined fields immediately before costs.
        if feature_sets[CONTROL] != [x for x in names if x not in census["family_features"]["spread_activity"]]:
            raise ValueError("spread_increment_control_schema_mismatch")
    if set(names) - set(feature_sets["full228_cost2"]):
        raise ValueError("spread_increment_noncanonical_feature")
    source = Path(__file__).resolve().parent
    source_hashes = {n: sha(source / n) for n in (
        "rich_spread_increment_v2.py", "rich_family_models_v2.py", "rich_family_runner_v2.py",
        "rich_campaign_consumer_v2.py", "rolling_registry_adapter_v2.py", "matched_campaign_models_v2.py",
        "retained_signed_cost_models_v1.py", "contracts.py", "publication.py")}
    contract_record = {"schema": "forex.rich_spread_increment_contract.v1", "candidate": candidate,
                       "family_features": census["family_features"]["spread_activity"],
                       "source_hashes": source_hashes, "rich_identity": rich["identity"]["fingerprint"],
                       "baseline_identity": baseline["identity"]["fingerprint"],
                       "old_rich_identity": old["identity"]["fingerprint"],
                       "horizons": c["horizon_minutes"], "fit_cutoffs": c["fit_cutoffs"],
                       "procedures": c["procedures"], "decision_epochs": c["decision_epochs"],
                       "evaluation_asof": c["evaluation_asof"], "methods": list(METHODS),
                       "controls": {"direct": CONTROL, "additional_preserved": "baseline zero/history-mean/ridge/HGB scores referenced but not re-fit"},
                       "resource_limits": {"max_seconds": MAX_TOTAL_SECONDS, "max_each_joint_fit_seconds": MAX_FIT_SECONDS,
                                           "max_output_bytes": 1_073_741_824},
                       "causal_timing": "retained mature training population and frozen/adaptive model-ready clocks",
                       "scope": "inspected development only; no confirmation, policy or trading claim"}
    required = ["contract.json", "fit_resources.json", "scores.json", "run_report.json"]
    for h in c["horizon_minutes"]:
        for cutoff in c["fit_cutoffs"]:
            prefix = f"fit_{h}_{cutoff}"
            required += [prefix + ".json", prefix + "_ridge.joblib", prefix + "_recovered_hgb.joblib"]
        for procedure in c["procedures"]:
            required += [f"forecasts_{h}_{procedure}.json", f"coverage_{h}_{procedure}.json"]
    identity = effective_run_identity(contract={"experiment": contract_record, "required_payloads": sorted(required)},
                                      dependency_hashes={**source_hashes, "rich": rich["identity"]["fingerprint"],
                                                         "baseline": baseline["identity"]["fingerprint"],
                                                         "old_rich": old["identity"]["fingerprint"],
                                                         "mask": candidate["mask_fingerprint"]})
    pub = RunPublisher(Path(runs_root), "rich-spread-activity-increment", identity)
    if (pub.root / "COMPLETION_MANIFEST.json").exists():
        verify_completed_run(pub.root, identity)
        return {"status": "completed_verified", "run_identity": identity["fingerprint"], "run_path": str(pub.root)}
    pub.acquire(recover=resume)
    try:
        payloads = [pub.write_or_validate_payload("contract.json", encoded(contract_record))]
        recipe = {"rich": rich, "feature_sets": feature_sets, "universe": universe}
        observations, outcomes, views = load_inputs(Path(rich_root), recipe)
        full = feature_sets["full228_cost2"]
        for each in views.values():
            each[NAME] = selected_view(each["full228_cost2"], names)
        fit_resources = []
        fits = {}
        for h in c["horizon_minutes"]:
            for cutoff in c["fit_cutoffs"]:
                if time.monotonic() - started > MAX_TOTAL_SECONDS:
                    raise TimeoutError("spread_increment_total_time_limit")
                prefix = f"fit_{h}_{cutoff}"
                meta_raw = pub.read_verified_payload(prefix + ".json")
                model_raw = {m: pub.read_verified_payload(prefix + "_" + m + ".joblib") for m in METHODS}
                if meta_raw is None or any(x is None for x in model_raw.values()):
                    base = checked(Path(baseline_root), baseline, f"fit_{h}_{cutoff}.json")
                    t0 = time.monotonic()
                    meta, model_raw = fit_pair(observations, outcomes, views, NAME, names, h, cutoff,
                                               base, c, registered_groups=(NAME,))
                    elapsed = time.monotonic() - t0
                    if elapsed > MAX_FIT_SECONDS:
                        raise TimeoutError("spread_increment_fit_latency_exceeded")
                    fit_resources.append({"fit_id": meta["fit_id"], "horizon": h, "cutoff": cutoff, "elapsed_seconds": elapsed})
                    with (pub.root / "FIT_TIMINGS.jsonl").open("ab") as f:
                        f.write(encoded(fit_resources[-1])); f.flush(); os.fsync(f.fileno())
                    meta_raw = encoded(meta)
                meta = json.loads(meta_raw)
                validate_fit(meta, model_raw)
                payloads.append(pub.write_or_validate_payload(prefix + ".json", meta_raw))
                for m, raw in model_raw.items():
                    payloads.append(pub.write_or_validate_payload(prefix + "_" + m + ".joblib", raw))
                fits[(h, cutoff)] = (meta, model_raw)
        if (pub.root / "FIT_TIMINGS.jsonl").exists():
            fit_resources = [json.loads(x) for x in (pub.root / "FIT_TIMINGS.jsonl").read_text().splitlines()]
        if len({x["fit_id"] for x in fit_resources}) != 14:
            raise ValueError("spread_increment_fit_resource_record_missing")
        scores = []
        forecast_count = 0
        for h in c["horizon_minutes"]:
            target = f"technical_endpoint_midpoint_elapsed_{h}m"
            for procedure in c["procedures"]:
                forecasts, coverage = [], []
                for cutoff in c["fit_cutoffs"]:
                    if procedure == "frozen" and cutoff != c["fit_cutoffs"][0]:
                        continue
                    current = []
                    for obs in observations:
                        t = obs["origin_epoch"]
                        if t not in c["decision_epochs"]:
                            continue
                        ready = [x for x in c["fit_cutoffs"] if x + c["fit_latency_seconds"] <= t]
                        chosen = max(ready) if procedure == "adaptive" else c["fit_cutoffs"][0]
                        if chosen == cutoff:
                            current.append(obs)
                    f, cov = predict(*fits[(h, cutoff)], current, views, procedure=procedure, c=c)
                    forecasts.extend(f); coverage.extend(cov)
                forecasts.sort(key=lambda x: (x["forecast"]["decision_epoch"], x["forecast"]["instrument"], x["method"]))
                coverage.sort(key=lambda x: (x["decision_epoch"], x["instrument"], x["method"]))
                controls = checked(Path(old_rich_root), old, f"forecasts_{CONTROL}_{h}_{procedure}.json")
                scores.extend(paired_metrics(forecasts, controls, outcomes, c["evaluation_asof"], target, procedure))
                forecast_count += len(forecasts)
                payloads.append(pub.write_or_validate_payload(f"forecasts_{h}_{procedure}.json", encoded(forecasts)))
                payloads.append(pub.write_or_validate_payload(f"coverage_{h}_{procedure}.json", encoded(coverage)))
        if forecast_count != 37744 or len(scores) == 0:
            raise ValueError("spread_increment_forecast_population_incomplete")
        report = {"status": "completed_matched_development_comparison", "mask_fingerprint": candidate["mask_fingerprint"],
                  "models_fitted": 28, "base_models_refitted": 0, "new_forecasts": forecast_count,
                  "inspected_development_only": True, "selected_model": None, "confirmation": False,
                  "policy_replay": False, "trading_ready": False, "elapsed_seconds": time.monotonic() - started}
        for name, obj in (("fit_resources.json", fit_resources), ("scores.json", scores), ("run_report.json", report)):
            payloads.append(pub.write_or_validate_payload(name, encoded(obj)))
        if sum((pub.root / x).stat().st_size for x in required) > 1_073_741_824:
            raise ValueError("spread_increment_output_budget_exceeded")
        pub.complete(payloads, set(required))
        verify_completed_run(pub.root, identity)
        return {"status": "completed_verified", "run_identity": identity["fingerprint"], "run_path": str(pub.root), "report": report}
    except BaseException:
        if pub._owner_token is not None:
            pub.release()
        raise
