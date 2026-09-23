"""Bounded real-history validation/export of the shared M1 technical kernel.

Default: validate source prefixes and recent CSV tails for all 68 pairs, write
compact receipts, export no feature arrays. This does not expand the complete
53.5-million-row archive, fit a model, fetch data or alter original sources.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import shutil
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import numpy as np
import oanda_rolling_technical_inputs_v1 as inputs
import oanda_rolling_technical_features_v1 as kernel

SCHEMA = "rolling_technical_history_validation_v1_20260915"
DEFAULT_METADATA = ROOT / "config/pair_local_operational_v2_20260913.json"
DEFAULT_METADATA_SHA = "c9464343f012e0bdd83582480a0044bf1c6774eef2622f312bcdd4599e3869c5"
MAX_SOURCE_ROWS = 65536
MAX_EXPORT_ROWS = 1_000_000
SOURCES = (Path(__file__), ROOT/"oanda_rolling_technical_inputs_v1.py", ROOT/"oanda_rolling_technical_features_v1.py")


def _json(value):
    return json.dumps(value, sort_keys=True, indent=2, allow_nan=False).encode()+b"\n"


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _utc(epoch=None):
    return datetime.fromtimestamp(time.time() if epoch is None else epoch, timezone.utc).isoformat()


def _write_new(path, value):
    with Path(path).open("xb") as handle:
        handle.write(_json(value))


def _section(data, start=None, end=None):
    return {name: values[start:end] for name, values in data.items()}


def _equal(left, right):
    """Exact finite float64 bits and identical missing mask, not tolerances."""
    if left.shape != right.shape or np.isinf(left).any() or np.isinf(right).any():
        return False
    a, b = np.isfinite(left), np.isfinite(right)
    return bool(np.array_equal(a, b) and np.array_equal(left[a].view(np.uint64), right[b].view(np.uint64)))


def feature_digest(features):
    digest = hashlib.sha256()
    for name, values in features.items():
        finite = np.isfinite(values)
        digest.update(name.encode()+b"\0")
        digest.update(len(values).to_bytes(8, "little"))
        digest.update(np.ascontiguousarray(finite, dtype=np.uint8).tobytes())
        digest.update(np.ascontiguousarray(values[finite], dtype="<f8").tobytes())
    return digest.hexdigest()


def registry_checks():
    registry = kernel.feature_registry()
    names = [r["name"] for r in registry]
    aliases = [alias for r in registry for alias in r["aliases"]]
    stripped = {name.removeprefix("m1__") for name in names}
    if len(names) != len(set(names)) or len(aliases) != len(set(aliases)) or set(aliases) & stripped:
        raise ValueError("duplicate_canonical_feature_or_alias")
    return {"canonical_columns": len(names), "alias_count": len(aliases),
            "alias_columns_materialized": False, "unique_names_and_aliases": True,
            "dedup_scope": "exact declared aliases only; correlated fields are not silently removed"}


def validate_arrays(data, pair, pip, *, chunk_rows=997):
    """Compare full prefix with finite-overlap chunks on the exact same rows."""
    count = len(data["time"])
    if count == 0:
        raise ValueError("nonempty_completed_source_sample_required")
    if type(chunk_rows) is not int or chunk_rows < 1:
        raise ValueError("positive_chunk_rows_required")
    full = kernel.compute_features(data, pair, pip)
    overlap = kernel.max_lookback_bars()-1
    chunks = {name: [] for name in full}
    boundaries = []
    for start in range(0, count, chunk_rows):
        end = min(count, start+chunk_rows)
        left = max(0, start-overlap)
        calculated = kernel.compute_features(_section(data, left, end), pair, pip)
        boundaries.append({"start": start, "end": end, "overlap": start-left})
        for name in chunks:
            chunks[name].append(calculated[name][start-left:])
    replayed = {name: np.concatenate(parts) if parts else np.array([], dtype=np.float64)
                for name, parts in chunks.items()}
    failures = [name for name in full if not _equal(full[name], replayed[name])]
    if failures:
        raise ValueError("historical_chunk_parity_failed:"+",".join(failures[:8]))
    # Validate actual elapsed 1m and 15m formulas independently. Equal outcomes
    # on a flat prefix are allowed; metadata and arithmetic must still differ.
    formula_checks = {}
    for lag in (1, 15):
        expected = np.full(count, np.nan)
        if count > lag:
            supported = data["time"][lag:]-data["time"][:-lag] == lag*60
            values = (data["close"][lag:]-data["close"][:-lag])/pip
            expected[lag:] = np.where(supported, values, np.nan)
        actual = full[f"m1__return_{lag}_pips"]
        if not _equal(expected, actual):
            raise ValueError("elapsed_return_formula_failed:"+str(lag))
        formula_checks[str(lag)] = {"exact_formula_equal": True, "finite_rows": int(np.isfinite(actual).sum())}
    one, fifteen = full["m1__return_1_pips"], full["m1__return_15_pips"]
    comparable = np.isfinite(one) & np.isfinite(fifteen)
    families = {}
    for family in sorted({r["family"] for r in kernel.feature_registry()}):
        members = [r["name"] for r in kernel.feature_registry() if r["family"] == family]
        masks = np.array([np.isfinite(full[name]) for name in members])
        families[family] = {"columns": len(members), "finite_values": int(masks.sum()),
                            "rows_with_any_value": int(masks.any(axis=0).sum()),
                            "rows_with_all_values": int(masks.all(axis=0).sum())}
    digest = feature_digest(full)
    return full, {"status": "passed", "rows": count, "first_bar_start": int(data["time"][0]) if count else None,
        "last_bar_start": int(data["time"][-1]) if count else None,
        "gap_count": int((np.diff(data["time"]) != 60).sum()),
        "one_shot_sha256": digest, "chunked_sha256": feature_digest(replayed),
        "comparison": "exact finite float64 bits and identical missing masks",
        "overlap_bars": overlap, "chunks": boundaries, "families": families,
        "elapsed_return_checks": formula_checks,
        "one_vs_fifteen_comparable_rows": int(comparable.sum()),
        "one_vs_fifteen_different_value_rows": int((one[comparable] != fifteen[comparable]).sum()),
        "finite_feature_values": sum(int(np.isfinite(v).sum()) for v in full.values()),
        "missing_feature_values": sum(int((~np.isfinite(v)).sum()) for v in full.values())}


def primary_mask(data, lane, cutoff_epoch):
    if lane == "reacquired_prefix":
        return data["time"] <= cutoff_epoch
    if lane in ("canonical_prefix", "canonical_tail"):
        return data["time"] > cutoff_epoch
    raise ValueError("unknown_primary_source_lane")


def export_features_with_primary_support(features, data, lane, cutoff_epoch):
    """Do not borrow pre-cutoff canonical history for primary-lane exports.

    A source-prefix test is not a reconstructed source seam. When the selected
    prefix lacks eligible predecessors, affected fields stay missing until
    their own complete lookback is inside the permitted source lane.
    """
    allowed = primary_mask(data, lane, cutoff_epoch)
    values = {name: array.copy() for name,array in features.items()}
    forbidden = np.cumsum(~allowed, dtype=np.int64)
    for feature in kernel.feature_registry():
        width = feature["lookback_bars"]
        indexes = np.arange(len(allowed))
        starts = np.maximum(0, indexes-width+1)
        bad = forbidden - np.where(starts > 0, forbidden[np.maximum(0,starts-1)], 0)
        values[feature["name"]][(~allowed) | (bad > 0)] = np.nan
    return values


def _compact_receipt(receipt):
    result = {k:v for k,v in receipt.items() if k not in ("row_hashes", "row_value_hashes", "completion_basis")}
    for key in ("row_hashes", "row_value_hashes"):
        result[key+"_sha256"] = _sha(_json(receipt[key]))
    result["row_hash_count"] = len(receipt["row_hashes"])
    return result


def _source_samples(recipe, pair, observed_epoch, rows):
    # One bounded source batch is read; date filtering never triggers an
    # unbounded scan through millions of earlier source rows.
    for lane, method, path in (("reacquired_prefix", inputs.iter_parquet, recipe["reacquired_path"]),
                               ("canonical_prefix", inputs.iter_csv, recipe["canonical_path"])):
        stream = method(path, pair, observed_epoch, batch_rows=rows)
        try:
            sample = next(stream, None)
            if sample is None:
                raise ValueError("empty_source")
            yield lane, sample
        finally:
            stream.close()
    yield "canonical_tail", inputs.read_csv_tail(recipe["canonical_path"], pair, observed_epoch, max_rows=rows)


def _date(value):
    if value is None:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("date_bound_timezone_required")
    return parsed.timestamp()


def _metadata(path, pairs):
    raw = Path(path).read_bytes()
    digest = _sha(raw)
    if Path(path).resolve() == DEFAULT_METADATA.resolve() and digest != DEFAULT_METADATA_SHA:
        raise ValueError("default_pair_metadata_source_changed")
    payload = json.loads(raw)
    result = {}
    for pair in pairs:
        value = payload["pairs"][pair]["pip_size"]
        if isinstance(value, bool) or not isinstance(value, (int,float)) or not math.isfinite(value) or not 0 < value <= .1:
            raise ValueError("explicit_valid_pair_pip_required:"+pair)
        result[pair] = float(value)
    return result, {"path": str(Path(path).resolve()), "sha256": digest,
                    "field": "pairs[PAIR].pip_size", "pip_sizes": result}


def _new_output(path):
    path = Path(path).absolute()
    if path.exists() or any(p.is_symlink() or (hasattr(p,"is_junction") and p.is_junction()) for p in path.parents):
        raise ValueError("new_unlinked_output_directory_required")
    path.mkdir(parents=True, exist_ok=False)
    (path/"pairs").mkdir()
    return path


def run(args):
    if not 1 <= args.max_rows_per_source <= MAX_SOURCE_ROWS:
        raise ValueError("bounded_source_row_count_required")
    if not 1 <= args.max_export_rows <= MAX_EXPORT_ROWS or not 1 <= args.max_output_mib <= 4096:
        raise ValueError("bounded_export_budget_required")
    if not 1 <= args.parity_chunk_rows <= MAX_SOURCE_ROWS or args.minimum_free_mib < 1:
        raise ValueError("positive_bounded_chunk_and_reserve_required")
    start, end = _date(args.start_utc), _date(args.end_utc)
    if start is not None and end is not None and start >= end:
        raise ValueError("ordered_date_bounds_required")
    sources = inputs.discover_archive_sources(args.manifest)
    pairs = sorted(sources) if not args.pairs else sorted(set(args.pairs))
    if not pairs or any(pair not in sources for pair in pairs):
        raise ValueError("pairs_must_belong_to_sealed_manifest")
    pip_sizes, pip_receipt = _metadata(args.pair_metadata, pairs)
    registry = registry_checks()
    source_hashes = {str(p.resolve()): _sha(p.read_bytes()) for p in SOURCES}
    output = _new_output(args.output)
    if shutil.disk_usage(output).free < args.minimum_free_mib*1024**2:
        raise ValueError("history_validation_disk_reserve")
    observed = time.time(); started = time.monotonic()
    report = {"schema_version": SCHEMA, "started_utc": _utc(observed), "observed_epoch": observed,
        "status": "incomplete", "research_only": True, "can_place_orders": False, "model_fits": 0,
        "mode": "bounded_export" if args.export else "validate_only", "source_bindings": source_hashes,
        "runtime": {"python": sys.version, "python_executable": sys.executable, "numpy": np.__version__},
        "kernel": kernel.schema_metadata(), "registry_checks": registry, "pair_metadata": pip_receipt,
        "manifest_path": str(Path(args.manifest).resolve()), "manifest_sha256": next(iter(sources.values()))["manifest_sha256"],
        "sealed_primary_rows": sum(r["sealed_raw_rows"] for r in sources.values()),
        "sealed_pairs": len(sources), "requested_pairs": pairs, "max_rows_per_source": args.max_rows_per_source,
        "sampling": "first bounded batch from each original OHLC source plus recent canonical CSV tail; no future-target filtering",
        "full_archive_features_built": False, "full_archive_ohlc_validated": False,
        "source_seal_scope": "Old archive seals verified close projections; selected full OHLC rows are newly validated and source-range/decoded-row hashes retained.",
        "historical_availability": "bar_start+60 assumed for retrospective feature generation; actual read time retained separately; not original publication evidence",
        "date_bounds": {"start_inclusive": start, "end_exclusive": end},
        "date_bound_scope": "filters only bounded sampled rows; an empty result is not absence of matching rows elsewhere in the archive",
        "export_row_cap": args.max_export_rows, "export_byte_cap": args.max_output_mib*1024**2,
        "minimum_free_bytes": args.minimum_free_mib*1024**2, "pairs": {}, "errors": [],
        "selected_rows_validated": 0, "primary_sample_rows": 0, "exported_rows": 0, "exported_bytes": 0}
    _write_new(output/"FEATURE_REGISTRY.json", kernel.feature_registry())
    for pair in pairs:
        result = {"pair": pair, "pip_size": pip_sizes[pair], "source_recipe": sources[pair],
                  "status": "incomplete", "samples": {}, "exports": []}
        seen = {}
        try:
            for lane, (data, receipt) in _source_samples(sources[pair], pair, observed, args.max_rows_per_source):
                for p in SOURCES:
                    if _sha(p.read_bytes()) != source_hashes[str(p.resolve())]:
                        raise ValueError("validation_source_changed_during_run")
                features, comparison = validate_arrays(data, pair, pip_sizes[pair], chunk_rows=args.parity_chunk_rows)
                admitted = primary_mask(data, lane, sources[pair]["cutoff_epoch"])
                duplicate = 0
                for i in np.flatnonzero(admitted):
                    at = int(data["time"][i]); value_hash = receipt["row_value_hashes"][i]
                    if at in seen:
                        if seen[at] != value_hash:
                            raise ValueError("sample_source_revision_conflict")
                        admitted[i] = False; duplicate += 1
                    else:
                        seen[at] = value_hash
                sealed_end = _date(sources[pair].get("sealed_last_utc"))
                comparison.update({"input_receipt": _compact_receipt(receipt),
                    "primary_precedence_rows": int(admitted.sum()),
                    "duplicate_sample_rows_removed": duplicate,
                    "primary_rows_after_original_seal": int((admitted & (data["time"] > sealed_end)).sum()) if sealed_end else None,
                    "primary_scope": "reacquired<=cutoff; canonical>cutoff; recent canonical extension beyond old seal is explicitly counted"})
                result["samples"][lane] = comparison
                report["selected_rows_validated"] += len(data["time"])
                report["primary_sample_rows"] += int(admitted.sum())
                if args.export:
                    export_features = export_features_with_primary_support(features, data, lane, sources[pair]["cutoff_epoch"])
                    mask = admitted.copy()
                    if start is not None: mask &= data["time"] >= start
                    if end is not None: mask &= data["time"] < end
                    selected = np.flatnonzero(mask)
                    remaining = args.max_export_rows-report["exported_rows"]
                    selected = selected[:max(0, remaining)]
                    comparison["export_rows_omitted_by_cap"] = int(mask.sum())-len(selected)
                    if len(selected):
                        estimated = len(selected)*(8*len(features)+64)
                        if report["exported_bytes"]+estimated > report["export_byte_cap"] or shutil.disk_usage(output).free-estimated < report["minimum_free_bytes"]:
                            raise ValueError("bounded_export_storage_guard")
                        import pyarrow as pa
                        import pyarrow.parquet as pq
                        columns = {"instrument": pa.array([pair]*len(selected)),
                            "bar_start_epoch": pa.array(data["time"][selected]),
                            "bar_end_epoch": pa.array(data["time"][selected]+60),
                            "completion_basis": pa.array([receipt["completion_basis"][i] for i in selected])}
                        columns.update({name:pa.array(values[selected], mask=~np.isfinite(values[selected])) for name,values in export_features.items()})
                        shard = output/(pair+"_"+lane+"_features.parquet")
                        pq.write_table(pa.table(columns), shard, compression="zstd")
                        size = shard.stat().st_size
                        result["exports"].append({"path": shard.name, "rows": len(selected), "bytes": size, "sha256": _sha(shard.read_bytes()),
                            "support": "sourcewise finite lookbacks wholly within permitted precedence lane; unavailable seam predecessors not invented"})
                        report["exported_rows"] += len(selected); report["exported_bytes"] += size
            result["status"] = "passed"
        except Exception as exc:
            result["status"] = "failed"
            result["error"] = type(exc).__name__+":"+str(exc)
            report["errors"].append({"pair": pair, "error": result["error"]})
        _write_new(output/"pairs"/(pair+".json"), result)
        report["pairs"][pair] = {"status": result["status"], "receipt": "pairs/"+pair+".json",
            "samples_checked": len(result["samples"]),
            "rows_checked": sum(s["rows"] for s in result["samples"].values())}
        print(json.dumps({"pair": pair, **report["pairs"][pair]}), flush=True)
    changed = [str(p) for p in SOURCES if _sha(p.read_bytes()) != source_hashes[str(p.resolve())]]
    if changed:
        report["errors"].append({"source_changed_at_completion": changed})
    report.update({"status": "passed" if not report["errors"] else "failed", "completed_utc": _utc(),
                   "elapsed_seconds": round(time.monotonic()-started, 3),
                   "all_requested_pair_samples_passed": not bool(report["errors"])})
    _write_new(output/"VALIDATION.json", report)
    (output/"README.md").write_text(
        "# Bounded rolling technical history validation\n\n"
        f"Status: **{report['status']}**. Completed {report['completed_utc']}.\n\n"
        f"Checked {report['selected_rows_validated']:,} selected original OHLC rows across {len(pairs)} requested pairs; "
        f"{report['primary_sample_rows']:,} distinct sampled rows satisfy source precedence. "
        f"Exported {report['exported_rows']:,} feature rows. [Exact receipt](VALIDATION.json).\n\n"
        "One-shot and overlapping-chunk calculations use identical finite float64 bits and missing masks. "
        "Per-pair receipts report actual rows, source identities, family support and independent 1m/15m checks. "
        "Aliases are not repeated model columns. Missing support remains missing.\n\n"
        "This is bounded historical-prefix and recent-tail validation, not full-archive feature materialization, "
        "complete OHLC validation of 53.5 million rows, predictive evaluation, model training or trading. "
        "Historical availability is assumed bar-end; original live receipt clocks are not recreated. "
        "Recent canonical rows after the old seal are counted separately. "
        "Date filters apply only to the selected bounded rows.\n", encoding="utf-8")
    return report


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--manifest", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--pairs", nargs="+")
    p.add_argument("--pair-metadata", type=Path, default=DEFAULT_METADATA)
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--validate-only", action="store_true", help="default; write receipts only")
    mode.add_argument("--export", action="store_true", help="export only bounded sampled feature rows")
    p.add_argument("--max-rows-per-source", type=int, default=4096)
    p.add_argument("--parity-chunk-rows", type=int, default=997)
    p.add_argument("--start-utc")
    p.add_argument("--end-utc")
    p.add_argument("--max-export-rows", type=int, default=100000)
    p.add_argument("--max-output-mib", type=int, default=1024)
    p.add_argument("--minimum-free-mib", type=int, default=4096)
    return p.parse_args(argv)


if __name__ == "__main__":
    completed = run(parse_args())
    print(json.dumps({"status": completed["status"], "rows": completed["selected_rows_validated"],
                      "exported_rows": completed["exported_rows"]}), flush=True)
    raise SystemExit(0 if completed["status"] == "passed" else 2)
