"""Training-only input diagnostics; no labels, models or trade evaluation.

Read the complete historical dataset through its verified partition loader.
Only the explicit 228 registered model inputs enter any calculation. Sample
original UTC minute starts divisible by 1800; if needed, retain a deterministic
hash-ranked subset of at most 150,000 rows. This cap never uses feature values.

Correlations standardize each input separately within each pair using only its
finite retained training samples (population standard deviation). Constant or
undersupported pair/input samples become unavailable, never invented zeroes.
Pooled Pearson correlations then use pairwise-complete observations, including
joint-support-specific recentering. Absolute correlation >= .995 with >=1,000
joint rows is a diagnostic flag, never an automatic exclusion or evidence of
predictive value. Only exact all-training constants/all-missing inputs from the
builder's TRAIN_FEATURE_SUMMARY are excluded from the retained candidate list.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import numpy as np
import pyarrow as pa

from oanda_rolling_technical_dataset_v1 import SCHEMA, file_sha, iter_partitions

DIAGNOSTIC_SCHEMA = "rolling_technical_train_input_diagnostics_v1_20260915"
FEATURE_COUNT = 228
MAX_SAMPLE_ROWS = 150000
SAMPLE_SECONDS = 1800
MIN_JOINT_ROWS = 1000
CORRELATION_THRESHOLD = .995
METADATA_COLUMNS = ("instrument", "bar_start_epoch", "bar_end_epoch", "origin_split")


def _read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _contract(dataset):
    manifest = _read_json(dataset / "DATASET.json")
    registry = _read_json(dataset / "FEATURE_REGISTRY.json")
    summary = _read_json(dataset / "TRAIN_FEATURE_SUMMARY.json")
    if manifest.get("schema") != SCHEMA or manifest.get("status") != "complete":
        raise ValueError("complete_training_dataset_required")
    names = manifest.get("feature_names", [])
    if (len(names) != FEATURE_COUNT or len(set(names)) != FEATURE_COUNT or
            manifest.get("feature_count") != FEATURE_COUNT or
            [entry.get("name") for entry in registry] != names):
        raise ValueError("exact_228_registered_input_names_required")
    if any(not isinstance(name, str) or not name.startswith(("m1__", "peer__")) for name in names):
        raise ValueError("model_input_category_guard")
    if set(names) & (set(manifest.get("label_names", [])) | set(METADATA_COLUMNS)):
        raise ValueError("label_or_metadata_input_refused")
    for entry in registry:
        if (entry.get("model_input") is not True or entry.get("future_information") or
                entry.get("role") in ("label", "label_metadata", "label_filter") or
                not isinstance(entry.get("family"), str)):
            raise ValueError("registered_input_category_guard")
    if summary.get("selection_scope") != "all TRAIN origins only; no label or later-period selection":
        raise ValueError("full_training_summary_scope_required")
    by_name = summary.get("features", {})
    if set(by_name) != set(names):
        raise ValueError("full_training_summary_input_names_mismatch")
    train_rows = sum(int(record["split_counts"].get("train", 0)) for record in manifest["partitions"])
    if train_rows <= 0 or train_rows != sum(int(record["split_counts"].get("train", 0)) for record in manifest["pairs"].values()):
        raise ValueError("training_origin_count_mismatch_or_empty")
    for name in names:
        stats = by_name[name]
        finite, missing = stats.get("finite_train_rows"), stats.get("missing_train_rows")
        if (type(finite) is not int or type(missing) is not int or finite < 0 or missing < 0 or
                finite + missing != train_rows):
            raise ValueError("exact_full_training_support_required:" + name)
        lo, hi = stats.get("minimum"), stats.get("maximum")
        if finite == 0:
            valid_bounds = lo is None and hi is None
        else:
            valid_bounds = (isinstance(lo, (int, float)) and isinstance(hi, (int, float)) and
                            np.isfinite(lo) and np.isfinite(hi) and lo <= hi)
        if not valid_bounds or stats.get("all_missing_in_train") is not (finite == 0) or stats.get("constant_in_train") is not (finite > 0 and lo == hi):
            raise ValueError("full_training_summary_consistency_required:" + name)
    candidates = [name for name in names if not by_name[name]["all_missing_in_train"] and not by_name[name]["constant_in_train"]]
    if summary.get("candidate_nonconstant_inputs") != candidates:
        raise ValueError("full_training_candidate_list_mismatch")
    return manifest, registry, summary, candidates, train_rows


def _priority(pair, times):
    """Stable SplitMix64 ranking of pair identity and original time; no values."""
    salt = int.from_bytes(hashlib.sha256(("training_clock_sample_v1:" + pair).encode()).digest()[:8], "little")
    x = np.asarray(times, dtype=np.uint64) ^ np.uint64(salt)
    with np.errstate(over="ignore"):
        x = x + np.uint64(0x9E3779B97F4A7C15)
        x = (x ^ (x >> np.uint64(30))) * np.uint64(0xBF58476D1CE4E5B9)
        x = (x ^ (x >> np.uint64(27))) * np.uint64(0x94D049BB133111EB)
    return x ^ (x >> np.uint64(31))


def collect_training_sample(dataset, manifest, names, max_sample_rows=MAX_SAMPLE_ROWS):
    """Use the loader's train split and immediately discard non-input columns."""
    if type(max_sample_rows) is not int or not 2 <= max_sample_rows <= MAX_SAMPLE_ROWS:
        raise ValueError("bounded_sample_cap_required")
    matrices, pairs, clocks, ranks = [], [], [], []
    held = 0
    eligible_rows = 0
    read_rows = 0
    eligible_by_pair = Counter()
    train_start, train_end = manifest["boundaries"]["start"], manifest["boundaries"]["train_end"]
    seen_pair_clocks = {}
    for pair, raw_table in iter_partitions(dataset, split="train", feature_names=names):
        # Loader may return label columns. They are not accessed or converted.
        table = raw_table.select(list(METADATA_COLUMNS) + names)
        if any(not (pa.types.is_floating(table[name].type) or pa.types.is_integer(table[name].type)) for name in names):
            raise ValueError("numeric_registered_inputs_required")
        if not pa.types.is_integer(table["bar_start_epoch"].type) or not pa.types.is_integer(table["bar_end_epoch"].type):
            raise ValueError("original_integer_clock_required")
        times = table["bar_start_epoch"].to_numpy()
        ends = table["bar_end_epoch"].to_numpy()
        if (pair not in manifest["pairs"] or any(value != pair for value in table["instrument"].to_pylist()) or
                any(value != "train" for value in table["origin_split"].to_pylist()) or
                np.any(~np.isfinite(times)) or np.any(times % 60) or np.any(ends != times + 60) or
                np.any(times < train_start) or np.any(times >= train_end) or np.any(np.diff(times) <= 0)):
            raise ValueError("training_only_exact_origin_rows_required")
        if len(times) and pair in seen_pair_clocks and times[0] <= seen_pair_clocks[pair]:
            raise ValueError("ordered_unique_training_pair_clocks_required")
        if len(times):
            seen_pair_clocks[pair] = int(times[-1])
        read_rows += len(times)
        selected = np.flatnonzero(times % SAMPLE_SECONDS == 0)
        eligible_rows += len(selected)
        eligible_by_pair[pair] += len(selected)
        if not len(selected):
            continue
        sampled = table.take(pa.array(selected, type=pa.int64()))
        matrix = np.column_stack([sampled[name].to_numpy() for name in names]).astype(np.float64, copy=False)
        matrix[~np.isfinite(matrix)] = np.nan
        selected_times = times[selected]
        matrices.append(matrix)
        pairs.append(np.full(len(selected), pair, dtype=object))
        clocks.append(selected_times)
        ranks.append(_priority(pair, selected_times))
        held += len(selected)
        if held > max_sample_rows:
            matrix, pair_array, clock_array, rank_array = np.concatenate(matrices), np.concatenate(pairs), np.concatenate(clocks), np.concatenate(ranks)
            # Lexicographic tie-breaking makes cap independent of shard order.
            keep = np.lexsort((clock_array, pair_array, rank_array))[:max_sample_rows]
            matrices, pairs, clocks, ranks = [matrix[keep]], [pair_array[keep]], [clock_array[keep]], [rank_array[keep]]
            held = len(keep)
    if read_rows != sum(int(record["split_counts"].get("train", 0)) for record in manifest["partitions"]):
        raise ValueError("loader_full_training_origin_count_mismatch")
    if not matrices:
        raise ValueError("no_original_clock_training_samples")
    matrix, pair_array, clock_array = np.concatenate(matrices), np.concatenate(pairs), np.concatenate(clocks)
    order = np.lexsort((clock_array, pair_array))
    matrix, pair_array, clock_array = matrix[order], pair_array[order], clock_array[order]
    return matrix, pair_array, clock_array, {
        "all_training_rows_read": read_rows, "clock_eligible_rows_before_cap": eligible_rows,
        "retained_rows": len(matrix), "maximum_rows": max_sample_rows,
        "eligible_rows_by_pair": dict(sorted(eligible_by_pair.items())),
        "retained_rows_by_pair": dict(sorted(Counter(pair_array).items())),
        "clock_rule": "original UTC bar_start_epoch modulo 1800 equals zero",
        "cap_rule": "lowest deterministic SplitMix64(pair SHA256 salt XOR original epoch), ties by pair/time; feature-value independent",
        "cap_applied": eligible_rows > max_sample_rows,
    }


def standardize_within_pair(matrix, pairs):
    """Finite-only per-pair training sample z-scores, with no missing imputation."""
    raw = np.asarray(matrix)
    if raw.ndim != 2 or raw.dtype.kind not in "iuf":
        raise ValueError("numeric_feature_matrix_required")
    pair_array = np.asarray(pairs)
    if pair_array.ndim != 1 or len(pair_array) != len(raw):
        raise ValueError("pair_rows_mismatch")
    result = np.full(raw.shape, np.nan, dtype=float)
    diagnostic = {}
    for pair in sorted(set(pair_array)):
        selected = np.flatnonzero(pair_array == pair)
        values = np.asarray(raw[selected], dtype=float)
        finite = np.isfinite(values)
        count = finite.sum(axis=0)
        safe = np.where(finite, values, 0.)
        with np.errstate(divide="ignore", invalid="ignore", over="ignore"):
            mean = safe.sum(axis=0) / np.maximum(count, 1)
            centered = np.where(finite, values - mean, 0.)
            variance = np.sum(centered * centered, axis=0) / np.maximum(count, 1)
            sd = np.sqrt(variance)
            usable = (count >= 2) & np.isfinite(sd) & (sd > 0) & np.isfinite(mean)
            z = centered / np.where(usable, sd, np.nan)
        z[~finite | ~np.isfinite(z)] = np.nan
        result[selected] = z
        diagnostic[str(pair)] = {
            "rows": len(selected), "inputs_with_finite_nonzero_sample_variance": int(usable.sum()),
            "inputs_with_fewer_than_two_finite_samples": int((count < 2).sum()),
            "inputs_with_zero_sample_variance": int(((count >= 2) & (variance == 0)).sum()),
        }
    return result, diagnostic


def pairwise_complete_correlation(matrix):
    """Pearson correlation and integer joint counts without complete-case drop."""
    raw = np.asarray(matrix)
    if raw.ndim != 2 or raw.dtype.kind not in "iuf":
        raise ValueError("numeric_feature_matrix_required")
    valid = np.isfinite(raw).astype(np.float64)
    x = np.where(valid, raw, 0.).astype(np.float64, copy=False)
    counts = valid.T @ valid
    sums = x.T @ valid
    cross = x.T @ x
    squares = (x * x).T @ valid
    with np.errstate(divide="ignore", invalid="ignore", over="ignore"):
        covariance = cross - sums * sums.T / counts
        variance_x = squares - sums * sums / counts
        variance_y = variance_x.T
        denominator = np.sqrt(variance_x * variance_y)
        correlation = covariance / denominator
    # Joint subsets can be exactly constant despite nonzero whole-pair sample
    # variance. Cancellation must not invent a correlation for those subsets.
    tolerance_x = 64 * np.finfo(np.float64).eps * np.maximum(squares, 0.)
    admissible = (counts >= 2) & (variance_x > tolerance_x) & (variance_y > tolerance_x.T) & np.isfinite(correlation)
    correlation[~admissible] = np.nan
    correlation = np.clip(correlation, -1., 1.)
    return correlation, np.rint(counts).astype(np.int64)


def _families(registry, summary, train_rows):
    grouped = {}
    for entry in registry:
        grouped.setdefault(entry["family"], []).append(entry["name"])
    result = {}
    for family, names in sorted(grouped.items()):
        rates = [summary["features"][name]["finite_train_rows"] / train_rows for name in names]
        result[family] = {
            "feature_count": len(names), "all_train_origin_rows": train_rows,
            "finite_feature_cells": sum(summary["features"][name]["finite_train_rows"] for name in names),
            "possible_feature_cells": train_rows * len(names),
            "mean_full_training_availability": float(np.mean(rates)),
            "minimum_feature_availability": float(min(rates)), "maximum_feature_availability": float(max(rates)),
            "full_training_all_missing_count": sum(summary["features"][name]["all_missing_in_train"] for name in names),
            "full_training_constant_count": sum(summary["features"][name]["constant_in_train"] for name in names),
        }
    return result


def analyze_training(dataset, *, max_sample_rows=MAX_SAMPLE_ROWS):
    """Return a JSON-safe report; performs no writes and never uses labels."""
    dataset = Path(dataset).resolve()
    source_files = ("DATASET.json", "FEATURE_REGISTRY.json", "TRAIN_FEATURE_SUMMARY.json")
    before = {name: file_sha(dataset / name) for name in source_files}
    manifest, registry, summary, candidates, train_rows = _contract(dataset)
    names = manifest["feature_names"]
    matrix, pairs, clocks, sample = collect_training_sample(dataset, manifest, names, max_sample_rows)
    standardized, scaling = standardize_within_pair(matrix, pairs)
    correlation, counts = pairwise_complete_correlation(standardized)
    first, second = np.triu_indices(len(names), 1)
    coefficients, supports = correlation[first, second], counts[first, second]
    eligible = (supports >= MIN_JOINT_ROWS) & np.isfinite(coefficients)
    flagged = eligible & (np.abs(coefficients) >= CORRELATION_THRESHOLD)
    near = [{"left": names[a], "right": names[b], "correlation": float(correlation[a, b]),
             "joint_training_sample_rows": int(counts[a, b])}
            for a, b in zip(first[flagged], second[flagged])]
    near.sort(key=lambda row: (-abs(row["correlation"]), row["left"], row["right"]))
    if before != {name: file_sha(dataset / name) for name in source_files}:
        raise ValueError("training_dataset_metadata_changed_during_analysis")
    excluded = {name: summary["features"][name] for name in names if name not in candidates}
    key_bytes = b"".join(str(pair).encode() + b":" + str(int(at)).encode() + b"\n" for pair, at in zip(pairs, clocks))
    return {
        "schema": DIAGNOSTIC_SCHEMA, "dataset": str(dataset), "dataset_metadata_sha256": before,
        "analysis_source_sha256": file_sha(Path(__file__)),
        "selection_scope": "TRAIN origins only; no labels or later-period data used for selection or statistics",
        "input_feature_count": len(names), "full_training_origin_rows": train_rows,
        "training_boundaries": {key: manifest["boundaries"][key] for key in ("start", "train_end")},
        "historically_opened_development_only": True, "untouched_confirmation": False,
        "sample": dict(sample, ordered_pair_clock_sha256=hashlib.sha256(key_bytes).hexdigest()),
        "standardization": {
            "method": "per pair, each feature centered/scaled from its finite retained TRAIN samples; population standard deviation",
            "missing_policy": "missing values, fewer than two finite samples, zero/nonfinite sample variance remain unavailable",
            "pooled_correlation": "pairwise-complete Pearson with each joint subset's own mean correction",
            "numerical_guard": "joint variance must exceed 64*float64_epsilon*joint_sum_of_squares; numerically constant overlaps are unavailable",
            "by_pair": scaling,
        },
        "full_training_family_availability": _families(registry, summary, train_rows),
        "full_training_exclusions": excluded,
        "candidate_input_names": candidates,
        "candidate_selection_rule": "exclude only exact all-TRAIN all-missing or constant inputs from TRAIN_FEATURE_SUMMARY; near correlations do not remove candidates",
        "sample_correlation_diagnostic": {
            "absolute_threshold": CORRELATION_THRESHOLD, "minimum_joint_rows": MIN_JOINT_ROWS,
            "possible_input_pairs": len(first), "estimable_pairs_with_minimum_support": int(eligible.sum()),
            "pairs_below_minimum_support": int((supports < MIN_JOINT_ROWS).sum()),
            "pairs_with_undefined_correlation": int((~np.isfinite(coefficients)).sum()),
            "joint_rows_minimum": int(supports.min()), "joint_rows_median": float(np.median(supports)),
            "joint_rows_maximum": int(supports.max()), "near_redundant_pair_count": len(near),
            "near_redundant_pairs": near,
            "automatic_correlation_drops": [],
        },
        "limitations": [
            "Correlations measure feature redundancy, not direction, forecasting accuracy or profitability.",
            "Regime dependence, serial dependence and overlapping windows remain; no significance claim is made.",
            "Per-pair scaling reduces price-level artifacts; pooled correlations can still conceal pair/regime differences.",
            "All-training constants can differ from sampled or per-pair constants; only the full-training summary controls candidate exclusions.",
            "Historical feature availability assumes completed bar ends; original receipt/publication timing is not reconstructed.",
        ],
        "label_columns_used": [], "models_fit": 0, "research_only": True, "can_place_orders": False,
    }


def markdown_report(report):
    diagnostic = report["sample_correlation_diagnostic"]
    excluded = report["full_training_exclusions"]
    lines = ["# Training input diagnostic", "",
        f"Analyzed {report['full_training_origin_rows']:,} training origins through the verified loader; retained {report['sample']['retained_rows']:,} original-clock 30-minute samples.", "",
        f"{report['input_feature_count']} registered inputs; {len(report['candidate_input_names'])} retained candidates. Only {len(excluded)} exact full-training constant/all-missing inputs are excluded.", "",
        f"{diagnostic['near_redundant_pair_count']} feature pairs have |r| ≥ {CORRELATION_THRESHOLD} with at least {MIN_JOINT_ROWS:,} joint samples after per-pair training-sample standardization. These flags remove no inputs and establish no predictive value.", "",
        "Full details, exact support counts for flagged pairs, family availability and the complete candidate list are in TRAINING_DIAGNOSTIC.json.", "",
        "## Exact full-training exclusions", ""]
    lines += ([f"- {name}: {'all missing' if stats['all_missing_in_train'] else 'constant = ' + str(stats['minimum'])}; {stats['finite_train_rows']:,} finite / {stats['missing_train_rows']:,} missing." for name, stats in excluded.items()] or ["None."])
    lines += ["", "## Family availability", "", "| Family | Inputs | Mean full-training availability |", "| --- | ---: | ---: |"]
    lines += [f"| {family} | {stats['feature_count']} | {stats['mean_full_training_availability']:.2%} |" for family, stats in report["full_training_family_availability"].items()]
    lines += ["", "## Strongest correlation flags", ""]
    lines += ([f"- {row['left']} ↔ {row['right']}: r={row['correlation']:.6f}; {row['joint_training_sample_rows']:,} joint samples." for row in diagnostic["near_redundant_pairs"][:20]] or ["None meet the support and correlation thresholds."])
    lines += ["", "## Limits", ""] + ["- " + value for value in report["limitations"]]
    return "\n".join(lines) + "\n"


def run(args):
    dataset, output = Path(args.dataset).resolve(), Path(args.output).resolve()
    if output.exists() or output.is_relative_to(dataset):
        raise ValueError("new_separate_diagnostics_output_required")
    report = analyze_training(dataset, max_sample_rows=args.max_sample_rows)
    report["generated_utc"] = datetime.now(timezone.utc).isoformat()
    # Compute and serialize everything before creating the only output directory.
    payload = json.dumps(report, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
    markdown = markdown_report(report)
    output.mkdir(parents=True, exist_ok=False)
    with (output / "TRAINING_DIAGNOSTIC.json").open("x", encoding="utf-8") as stream:
        stream.write(payload)
    with (output / "README.md").open("x", encoding="utf-8") as stream:
        stream.write(markdown)
    return report


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--max-sample-rows", type=int, default=MAX_SAMPLE_ROWS)
    return parser.parse_args(argv)


if __name__ == "__main__":
    completed = run(parse_args())
    print(json.dumps({"retained_samples": completed["sample"]["retained_rows"],
        "candidate_inputs": len(completed["candidate_input_names"]),
        "correlation_flags": completed["sample_correlation_diagnostic"]["near_redundant_pair_count"]}))
