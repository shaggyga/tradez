"""Posthoc component baselines, declared after the specialist run started.

Reads completed immutable labels, current quotes and retained head forecasts.
Fits no predictive model and changes no gate. Per-pair TRAIN means/medians are
descriptive reference estimates, with the exact final sampled/mature population.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path: sys.path.insert(0, str(ROOT))
import numpy as np
import pyarrow
import pyarrow.parquet as pq

SCHEMA = "posthoc_rolling_specialist_component_baselines_v1_20260915"
HORIZONS = (30, 60)
GROUPS = ("compact38", "compact50")
COMPONENTS = ("expected_absolute_move", "positive_magnitude", "nonpositive_magnitude", "long_exit_cost", "short_exit_cost")
SPLITS = ((1, "validation"), (2, "later_development_test"))
MINIMUM_TRAIN_LABELS = 20


def sha(path):
    with Path(path).open("rb") as f: return hashlib.file_digest(f, "sha256").hexdigest()


def json_read(path):
    return json.loads(Path(path).read_text("utf-8"), parse_constant=lambda x: (_ for _ in ()).throw(ValueError("nonfinite_json:" + x)))


def bind(root, record, receipts):
    root = Path(root).resolve(); path = (root / record["path"]).resolve()
    if not path.is_relative_to(root) or not path.is_file() or sha(path) != record["sha256"]:
        raise ValueError("artifact_path_or_hash_mismatch:" + record["path"])
    receipts[path] = record["sha256"]
    return path


def bound_manifest(path, expected, receipts):
    path = Path(path).resolve()
    if sha(path) != expected: raise ValueError("bound_manifest_hash_mismatch")
    receipts[path] = expected
    value = json_read(path)
    if value.get("status") != "complete": raise ValueError("completed_inputs_required")
    return value


def clocks_hash(times):
    return hashlib.sha256(np.asarray(times, dtype="<i8").tobytes()).hexdigest()


def population_hash(data, rows):
    keys = np.column_stack((data["pair_id"][rows], data["time"][rows])).astype("<i8")
    return hashlib.sha256(keys.tobytes()).hexdigest()


def load_labels_only(manifest, receipts):
    inputs = Path(manifest["inputs_root"]).resolve()
    im = bound_manifest(inputs / "SPECIALIST_INPUTS.json", manifest["inputs_sha256"], receipts)
    prepared, quotes = Path(im["prepared_root"]).resolve(), Path(im["quote_root"]).resolve()
    pm = bound_manifest(prepared / "PREPARED.json", im["prepared_sha256"], receipts)
    qm = bound_manifest(quotes / "QUOTE_PANEL.json", im["quote_sha256"], receipts)
    names = sorted(pm["pairs"])
    if (len(names) != 68 or set(names) != set(im["pairs"]) or set(names) != set(qm["pairs"]) or
            pm["boundaries"] != im["boundaries"] or pm["boundaries"] != qm["boundaries"] or
            pm["base_sha256"] != im["base_sha256"] or pm["base_sha256"] != qm["base_manifest_sha256"] or
            pm["overlay_sha256"] != qm["endpoint_manifest_sha256"]):
        raise ValueError("same_complete_all68_label_population_required")
    n = pm["rows"]
    data = {"pair_names": names, "boundaries": pm["boundaries"], "time": np.empty(n, np.int64),
            "pair_id": np.empty(n, np.int16), "split": np.empty(n, np.int8)}
    for key in ("entry_long", "entry_short", "spread"): data[key] = np.empty(n)
    for h in HORIZONS:
        for stem in ("y", "long", "short", "arima", "momentum"): data[f"{stem}_{h}"] = np.empty(n)
        for stem in ("valid", "strict", "eligible"): data[f"{stem}_{h}"] = np.empty(n, bool)
    offset = 0
    for pid, pair in enumerate(names):
        pr, sr, qr = pm["pairs"][pair], im["pairs"][pair], qm["pairs"][pair]
        pp, sp, qp = bind(prepared, pr, receipts), bind(inputs, sr, receipts), bind(quotes, qr, receipts)
        bind(quotes, {"path": qr["receipt_path"], "sha256": qr["receipt_sha256"]}, receipts)
        columns = ["instrument", "bar_start_epoch", "origin_split", "quote__mid_close", "quote__entry_spread_bps"]
        columns += [f"forecast__arima110_conditional_ols__{h}m_bps" for h in HORIZONS]
        quote = pq.ParquetFile(qp).read(columns=columns)
        qt = quote["bar_start_epoch"].to_numpy(); mid = quote["quote__mid_close"].to_numpy()
        if len(qt) != qr["rows"] or clocks_hash(qt) != qr["key_sha256"] or np.any(np.diff(qt) <= 0) or any(p != pair for p in quote["instrument"].to_pylist()):
            raise ValueError("quote_pair_clock_identity")
        with np.load(pp, allow_pickle=False) as z, np.load(sp, allow_pickle=False) as s:
            t, split = z["time"], z["split"]; count = len(t); sl = slice(offset, offset + count)
            if (count != pr["rows"] or count != sr["rows"] or clocks_hash(t) != pr["key_sha256"] or
                    clocks_hash(t) != sr["key_sha256"] or np.any(np.diff(t) <= 0) or
                    not np.array_equal(t, s["time"]) or not np.array_equal(split, s["split"])):
                raise ValueError("prepared_specialist_original_key_join")
            b = pm["boundaries"]
            expected_split = np.where(t < b["train_end"], 0, np.where(t < b["validation_end"], 1, 2))
            if (not np.array_equal(split, expected_split) or np.any((t < b["start"]) | (t >= b["end"])) or
                    np.any(t % 60) or np.any(t[split == 0] % 900)):
                raise ValueError("original_15minute_TRAIN_and_assessment_clocks_required")
            index = np.searchsorted(qt, t)
            if np.any(index >= len(qt)) or not np.array_equal(qt[index], t): raise ValueError("exact_quote_join_required")
            quote_split = quote["origin_split"].to_numpy()[index]
            if not np.array_equal(quote_split, np.array(["train", "validation", "later_development_test"])[split]):
                raise ValueError("quote_split_identity")
            data["time"][sl], data["split"][sl], data["pair_id"][sl] = t, split, pid
            data["entry_long"][sl], data["entry_short"][sl] = s["known_entry_long_bps"], s["known_entry_short_bps"]
            data["spread"][sl] = quote["quote__entry_spread_bps"].to_numpy()[index]
            for h in HORIZONS:
                for stem in ("y", "long", "short", "valid", "strict", "eligible"):
                    values = z[f"{stem}_{h}"]
                    if values.shape != t.shape or (stem in ("valid", "strict", "eligible") and values.dtype.kind != "b"):
                        raise ValueError("original_aligned_label_dtype")
                    data[f"{stem}_{h}"][sl] = values
                cut = np.choose(split, [b["train_end"], b["validation_end"], b["end"]])
                if not np.array_equal(data[f"eligible_{h}"][sl], t + (h + 1) * 60 < cut):
                    raise ValueError("strict_original_target_END_maturity_required")
                data[f"arima_{h}"][sl] = quote[f"forecast__arima110_conditional_ols__{h}m_bps"].to_numpy()[index]
                past = np.searchsorted(qt, t - h * 60); safe = np.minimum(past, len(qt) - 1)
                valid = (past < len(qt)) & (qt[safe] == t - h * 60) & np.isfinite(mid[index]) & np.isfinite(mid[safe]) & (mid[safe] > 0)
                with np.errstate(divide="ignore", invalid="ignore", over="ignore"):
                    momentum = (mid[index] / mid[safe] - 1.) * 10000.
                momentum[~valid | ~np.isfinite(momentum)] = np.nan
                data[f"momentum_{h}"][sl] = momentum
        offset += count
    if offset != n or not np.isfinite(data["entry_long"] + data["entry_short"]).all() or np.any(data["entry_long"] < 0) or np.any(data["entry_short"] < 0):
        raise ValueError("complete_finite_current_entry_costs_required")
    if not np.allclose(data["entry_long"] + data["entry_short"], data["spread"], rtol=1e-12, atol=1e-10):
        raise ValueError("current_entry_cost_spread_basis_mismatch")
    return data, im


def component_targets(data, h):
    y = np.asarray(data[f"y_{h}"]); valid = np.asarray(data[f"valid_{h}"])
    if valid.dtype.kind != "b": raise ValueError("explicit_endpoint_valid_mask")
    long_exit = y - data["entry_long"] - data[f"long_{h}"]
    short_exit = -y - data["entry_short"] - data[f"short_{h}"]
    if np.any(valid & (~np.isfinite(y) | ~np.isfinite(long_exit) | ~np.isfinite(short_exit) | (long_exit < -1e-7) | (short_exit < -1e-7))):
        raise ValueError("finite_nonnegative_valid_exit_targets_required")
    return {"expected_absolute_move": (np.abs(y), valid), "positive_magnitude": (y, valid & (y > 0)),
            "nonpositive_magnitude": (-y, valid & (y <= 0)),
            "long_exit_cost": (np.maximum(long_exit, 0.), valid), "short_exit_cost": (np.maximum(short_exit, 0.), valid)}


def train_baselines(data, h):
    target = component_targets(data, h)
    times = data["time"]; b = data["boundaries"]
    train = (data["split"] == 0) & data[f"valid_{h}"] & (times + (h + 1) * 60 < b["train_end"])
    if np.any(times[data["split"] == 0] % 900): raise ValueError("sampled_original_TRAIN_clocks_required")
    rows = np.flatnonzero(train)
    target_matrix = np.column_stack((data[f"y_{h}"][rows], target["long_exit_cost"][0][rows], target["short_exit_cost"][0][rows])).astype("<f8")
    receipt = {"rows": len(rows), "pair_clock_sha256": population_hash(data, rows),
               "targets_y_exit_long_exit_short_sha256": hashlib.sha256(target_matrix.tobytes()).hexdigest(),
               "maximum_target_end_epoch": int(np.max(times[rows] + (h + 1) * 60)) if len(rows) else None}
    params = {}
    for component, (values, label_mask) in target.items():
        params[component] = {}
        for pid, pair in enumerate(data["pair_names"]):
            selected = train & label_mask & (data["pair_id"] == pid) & np.isfinite(values)
            value = values[selected]; supported = len(value) >= MINIMUM_TRAIN_LABELS
            params[component][pair] = {"finite_training_labels": len(value), "supported": supported,
                "mean_bps": float(np.mean(value)) if supported else None,
                "median_bps": float(np.median(value)) if supported else None,
                "training_component_pair_clock_sha256": population_hash(data, np.flatnonzero(selected))}
    encoded = json.dumps(params, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    return {"training_selection": receipt, "parameters": params, "parameters_sha256": hashlib.sha256(encoded).hexdigest()}


def errors(prediction, target):
    error = np.asarray(prediction) - np.asarray(target)
    if not np.isfinite(error).all(): raise ValueError("nonfinite_component_error")
    n = len(error)
    if not n: return {"rows": 0, "mae_bps": None, "rmse_bps": None, "mean_error_bps": None}
    scale = float(np.max(np.abs(error)))
    return {"rows": n, "mae_bps": float(np.mean(np.abs(error))),
            "rmse_bps": scale * float(np.sqrt(np.mean((error / scale) ** 2))) if scale else 0.,
            "mean_error_bps": float(np.mean(error))}


def comparison_rows(data, assessment, heads, h, group, baseline):
    assessment = np.asarray(assessment)
    if not np.array_equal(assessment, np.flatnonzero(data["split"] > 0)): raise ValueError("all_original_assessment_rows_required")
    n = len(assessment)
    for key in ("probability", "positive", "nonpositive", "long_exit", "short_exit"):
        value = np.asarray(heads[key])
        if value.shape != (n,) or not np.isfinite(value).all() or np.any(value < 0): raise ValueError("finite_nonnegative_saved_head_vectors_required")
    p = heads["probability"]
    if np.any(p > 1): raise ValueError("probability_in_unit_interval_required")
    prediction = {"expected_absolute_move": p * heads["positive"] + (1 - p) * heads["nonpositive"],
                  "positive_magnitude": heads["positive"], "nonpositive_magnitude": heads["nonpositive"],
                  "long_exit_cost": heads["long_exit"], "short_exit_cost": heads["short_exit"]}
    targets = component_targets(data, h)
    original_pair_train = np.array([baseline["parameters"]["expected_absolute_move"][pair]["supported"] for pair in data["pair_names"]])
    common = np.isfinite(data[f"arima_{h}"][assessment]) & np.isfinite(data[f"momentum_{h}"][assessment]) & original_pair_train[data["pair_id"][assessment]]
    scores = []
    for sid, split in SPLITS:
        origin = (data["split"][assessment] == sid) & common
        endpoint = origin & data[f"valid_{h}"][assessment] & data[f"eligible_{h}"][assessment]
        strict = data[f"strict_{h}"][assessment]
        if strict.dtype.kind != "b" or np.any(strict & ~data[f"valid_{h}"][assessment]): raise ValueError("strict_endpoint_subset_required")
        cohorts = {"full_endpoint": endpoint, "shared_strict": endpoint & strict, "additional_endpoint_only": endpoint & ~strict}
        for cohort, cohort_mask in cohorts.items():
            for component in COMPONENTS:
                values, target_valid = targets[component]
                before = cohort_mask & target_valid[assessment] & np.isfinite(values[assessment])
                param = baseline["parameters"][component]
                supported_pair = np.array([param[pair]["supported"] for pair in data["pair_names"]])
                supported = supported_pair[data["pair_id"][assessment]]
                matched = before & supported; rows = assessment[matched]
                means = np.array([param[pair]["mean_bps"] if param[pair]["supported"] else np.nan for pair in data["pair_names"]])[data["pair_id"][rows]]
                medians = np.array([param[pair]["median_bps"] if param[pair]["supported"] else np.nan for pair in data["pair_names"]])[data["pair_id"][rows]]
                truth = values[rows]
                model = errors(prediction[component][matched], truth)
                mean, median = errors(means, truth), errors(medians, truth)
                row = {"context": f"{group}_{h}m", "horizon_minutes": h, "group": group, "split": split, "cohort": cohort, "component": component,
                    "cohort_endpoint_origins": int(cohort_mask.sum()), "component_label_rows_before_baseline_support": int(before.sum()),
                    "excluded_insufficient_component_TRAIN_support": int((before & ~supported).sum()),
                    "matched_rows": len(rows), "matched_pair_clock_sha256": population_hash(data, rows),
                    "matched_target_float64_sha256": hashlib.sha256(np.asarray(truth, dtype="<f8").tobytes()).hexdigest(),
                    "matched_counts_by_pair": {pair: int(np.sum(data["pair_id"][rows] == pid)) for pid, pair in enumerate(data["pair_names"])},
                    "model_before_baseline_support": errors(prediction[component][before], values[assessment[before]]),
                    "model": model, "TRAIN_pair_mean": mean, "TRAIN_pair_median": median,
                    "mae_improvement_vs_TRAIN_median_bps": median["mae_bps"] - model["mae_bps"] if len(rows) else None,
                    "rmse_improvement_vs_TRAIN_mean_bps": mean["rmse_bps"] - model["rmse_bps"] if len(rows) else None}
                if component in ("long_exit_cost", "short_exit_cost"):
                    # Future bid half compares to current bid half, and ask to ask.
                    entry_key = "entry_short" if component == "long_exit_cost" else "entry_long"
                    persistence = errors(data[entry_key][rows], truth)
                    row["current_quote_wing_persistence"] = {"current_input": entry_key, "score": persistence,
                        "interpretation": "long exit is bid-side so uses current short-entry half; short exit is ask-side so uses current long-entry half; all normalized by original midpoint"}
                    row["mae_improvement_vs_persistence_bps"] = persistence["mae_bps"] - model["mae_bps"] if len(rows) else None
                    row["rmse_improvement_vs_persistence_bps"] = persistence["rmse_bps"] - model["rmse_bps"] if len(rows) else None
                scores.append(row)
    return scores


def analyze(comparison_root):
    root = Path(comparison_root).resolve(); path = root / "RESULTS.json"
    manifest = json_read(path)
    if manifest.get("status") != "complete" or manifest.get("schema") != "rolling_specialist_comparison_v1_20260915":
        raise ValueError("complete_specialist_results_required")
    expected = {f"{g}_{h}m" for g in GROUPS for h in HORIZONS}
    if set(manifest["contexts"]) != expected or manifest["completed_variants"] != 28: raise ValueError("complete_four_contexts_required")
    own_path = Path(__file__).resolve(); receipts = {path: sha(path), own_path: sha(own_path)}
    data, im = load_labels_only(manifest, receipts)
    rows = np.flatnonzero(data["split"] > 0)
    if len(rows) != manifest["assessment_rows"] or data["pair_names"] != manifest["pair_names"]: raise ValueError("original_assessment_population_identity")
    baselines = {str(h): train_baselines(data, h) for h in HORIZONS}
    report_rows = []
    for h in HORIZONS:
        for group in GROUPS:
            context = manifest["contexts"][f"{group}_{h}m"]
            reference = context["final_model"]["training"]
            for key, value in baselines[str(h)]["training_selection"].items():
                if reference[key] != value: raise ValueError("exact_final_model_TRAIN_selection_required:" + key)
            record = context["head_forecasts"]
            head_path = bind(root, record, receipts)
            table = pq.ParquetFile(head_path).read(columns=["pair_id", "bar_start_epoch", "split", "probability", "positive", "nonpositive", "long_exit", "short_exit"])
            if table.num_rows != len(rows) or record["rows"] != len(rows): raise ValueError("complete_saved_head_population_required")
            for field, column in (("pair_id", "pair_id"), ("time", "bar_start_epoch"), ("split", "split")):
                if not np.array_equal(data[field][rows], table[column].to_numpy()): raise ValueError("saved_head_exact_original_population_join")
            heads = {name: table[name].to_numpy() for name in ("probability", "positive", "nonpositive", "long_exit", "short_exit")}
            report_rows.extend(comparison_rows(data, rows, heads, h, group, baselines[str(h)]))
    for p, digest in receipts.items():
        if sha(p) != digest: raise ValueError("posthoc_input_or_tool_changed_during_read")
    return {"schema": SCHEMA, "status": "complete", "role": "POSTHOC_COMPONENT_BASELINES",
        "declaration": "Requested after the specialist comparison began; separate descriptive diagnostic, no study specification rewrite or untouched confirmation.",
        "comparison_root": str(root), "results_sha256": receipts[path], "tool_sha256": receipts[own_path],
        "input_manifest_sha256": manifest["inputs_sha256"], "original_assessment_rows": len(rows),
        "population_digest_format": "pair_id and original candle-start epoch, row-major signed little-endian int64, original pair/time order",
        "minimum_finite_component_TRAIN_labels": MINIMUM_TRAIN_LABELS,
        "training_scope": "exact final model original UTC15m TRAIN sample, endpoint valid, target END strictly before August24 cutoff; per-pair means and medians only",
        "baselines": baselines, "rows": report_rows,
        "verified_files": [{"path": str(p), "sha256": digest} for p, digest in receipts.items()],
        "versions": {"numpy": np.__version__, "pyarrow": pyarrow.__version__},
        "models_fit": 0, "gates_changed": False, "models_promoted": 0,
        "limits": ["Conditional-positive and nonpositive cohorts use future labels for descriptive scoring only, never for origin-time selection.",
                   "All model/reference errors use identical supported rows; before-support model errors and excluded counts remain separate.",
                   "MAE is compared with TRAIN medians; RMSE with TRAIN means. Both errors are also shown for every baseline.",
                   "Terminal absolute move is not intrahorizon volatility, path risk, stops or direction; component skill does not establish trading profit.",
                   "Same-quote-wing persistence preserves asymmetric midpoint accounting; current long entry cost is not the current bid-side half.",
                   "The dates and proposed components have been inspected. No significance, independent-sample, profitable-policy or promotion claim."]}


def markdown_report(report):
    lines = ["# Posthoc component baselines", "", report["declaration"], "", "Positive gains mean the saved head improves on the matched baseline. No model is refitted and no entry rule changes. Training references use per-pair finite counts of at least20 on the exact final TRAIN sample.", "",
             "| Context | Period / cohort | Component | n / unsupported | Model MAE / RMSE | TRAIN median MAE / mean RMSE | MAE / RMSE gain | Persistence MAE / RMSE gain |",
             "| --- | --- | --- | ---: | --- | --- | --- | --- |"]
    def f(v): return "—" if v is None else f"{v:+.5f}"
    for r in report["rows"]:
        lines.append(f"| {r['context']} | {r['split']} / {r['cohort']} | {r['component']} | {r['matched_rows']} / {r['excluded_insufficient_component_TRAIN_support']} | {f(r['model']['mae_bps'])} / {f(r['model']['rmse_bps'])} | {f(r['TRAIN_pair_median']['mae_bps'])} / {f(r['TRAIN_pair_mean']['rmse_bps'])} | {f(r['mae_improvement_vs_TRAIN_median_bps'])} / {f(r['rmse_improvement_vs_TRAIN_mean_bps'])} | {f(r.get('mae_improvement_vs_persistence_bps'))} / {f(r.get('rmse_improvement_vs_persistence_bps'))} |")
    lines += ["", "## Limits", ""] + ["- " + x for x in report["limits"]]
    return "\n".join(lines) + "\n"


def run(args):
    root = Path(args.comparison).resolve(); output = Path(args.output).resolve()
    if output.exists() or output.is_relative_to(root): raise ValueError("new_separate_posthoc_output_required")
    began = time.monotonic(); report = analyze(root)
    # Also forbid writes inside any retained input tree.
    for file in report["verified_files"]:
        p = Path(file["path"])
        if p.name in ("PREPARED.json", "SPECIALIST_INPUTS.json", "QUOTE_PANEL.json") and output.is_relative_to(p.parent):
            raise ValueError("posthoc_output_cannot_modify_accepted_inputs")
    report.update(completed_utc=datetime.now(timezone.utc).isoformat(), elapsed_seconds=time.monotonic() - began)
    payload = json.dumps(report, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
    text = markdown_report(report)
    output.mkdir(parents=True, exist_ok=False)
    (output / "POSTHOC_COMPONENT_BASELINES.json").write_text(payload, encoding="utf-8")
    (output / "POSTHOC_COMPONENT_BASELINES.md").write_text(text, encoding="utf-8")
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--comparison", type=Path, required=True); parser.add_argument("--output", type=Path, required=True)
    r = run(parser.parse_args())
    print(json.dumps({"status": r["status"], "component_rows": len(r["rows"]), "elapsed_seconds": r["elapsed_seconds"]}))
