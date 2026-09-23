"""Audit registered historical inputs; exclude outcomes and preserve source data."""
from __future__ import annotations
import argparse
import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

PROJECT = Path(__file__).resolve().parents[1]
SOURCE = Path.home() / "AppData/Local/ForexResearchData/unified_intrahour_v1/unified_training_matrix.parquet"
REGISTRY = PROJECT.parent / "feature_horizon_audit_20260908/retained/unified/unified_feature_registry.csv"
OUT = PROJECT / "data/oanda_training_manager/price_only_phase1_20260914/feature_space_dedup_audit.json"

def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def registered_inputs(path: Path) -> list[str]:
    with path.open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        if not {"feature_name", "model_input", "causal"}.issubset(reader.fieldnames or []):
            raise ValueError("registry_missing_required_columns")
        rows = list(reader)
    all_names = [r["feature_name"] for r in rows]
    if any(not n for n in all_names) or len(all_names) != len(set(all_names)):
        raise ValueError("registry_empty_or_duplicate_feature_name")
    inputs = []
    for row in rows:
        flag = row["model_input"].lower()
        if flag not in {"true", "false"}:
            raise ValueError("registry_invalid_model_input_flag")
        if flag == "false":
            continue
        name = row["feature_name"]
        if name.startswith(("target_", "diag_")):
            raise ValueError("registry_contains_outcome_as_input")
        if row["causal"].lower() != "true":
            raise ValueError("registry_contains_noncausal_input")
        inputs.append(name)
    if not inputs:
        raise ValueError("registry_has_no_inputs")
    return inputs

def canonical_values(values: np.ndarray) -> np.ndarray:
    # Signed zero and different NaN payloads mean the same numeric feature value.
    result = np.asarray(values, dtype="<f8").copy()
    result[result == 0] = 0.0
    result[np.isnan(result)] = np.nan
    return result

def audit(source: Path, registry: Path) -> dict:
    before = source.stat()
    registry_hash = sha256_file(registry)
    names = registered_inputs(registry)
    pf = pq.ParquetFile(source)
    schema_names = pf.schema_arrow.names
    if len(schema_names) != len(set(schema_names)):
        raise ValueError("duplicate_parquet_column_names")
    for name in names:
        if name not in schema_names:
            raise ValueError(f"registered_input_missing:{name}")
        # Retained registry inputs are floats. Reject an unreviewed numeric cast.
        if not pa.types.is_floating(pf.schema_arrow.field(name).type):
            raise ValueError(f"registered_input_not_floating:{name}")
    groups: dict[str, list[str]] = {}
    constants, nonfinite = [], {}
    for start in range(0, len(names), 32):
        table = pf.read(columns=names[start:start + 32], use_threads=False)
        for name in table.column_names:
            values = canonical_values(table[name].to_numpy())
            h = hashlib.sha256(values.tobytes()).hexdigest()
            groups.setdefault(h, []).append(name)
            if len(values) == 0 or np.all((values == values[0]) | (np.isnan(values) & np.isnan(values[0]))):
                constants.append(name)
            count = int(np.count_nonzero(~np.isfinite(values)))
            if count:
                nonfinite[name] = count
    duplicates = [g for g in groups.values() if len(g) > 1]
    # Verify equality before excluding any candidate from a hash group.
    for group in duplicates:
        table = pf.read(columns=group, use_threads=False)
        first = canonical_values(table[group[0]].to_numpy())
        for name in group[1:]:
            if not np.array_equal(first, canonical_values(table[name].to_numpy()), equal_nan=True):
                raise ValueError("duplicate_hash_without_equal_values")
    aliases = {name: group[0] for group in duplicates for name in group[1:]}
    dropped = set(constants) | set(aliases)
    selected = [name for name in names if name not in dropped]
    source_hash = sha256_file(source)
    after = source.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise ValueError("source_changed_during_audit")
    if sha256_file(registry) != registry_hash:
        raise ValueError("registry_changed_during_audit")
    return {
        "schema_version": "feature_space_dedup_audit_v2_registered_inputs",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "source": {"path": str(source), "sha256": source_hash, "rows": pf.metadata.num_rows,
                   "columns": len(schema_names), "registered_numeric_inputs": len(names)},
        "registry": {"path": str(registry), "sha256": registry_hash},
        "excluded_non_input_columns": [n for n in schema_names if n not in set(names)],
        "exact_duplicate_value_groups": duplicates,
        "exact_duplicate_column_count": len(aliases),
        "duplicate_representatives": aliases,
        "constant_columns": constants,
        "registry_duplicate_names": [],
        "nonfinite_counts": nonfinite,
        "representative_numeric_columns": selected,
        "selected_numeric_input_count": len(selected),
        "dropped_for_research_only": [n for n in names if n in dropped],
        "models_fitted": 0,
        "near_correlation_selection_performed": False,
        "policy": "Retrospective exact-equality audit only. Ordered registry inputs exclude outcomes and diagnostics. No temporal validation, new model, live schema migration, or feature-window repair is implied.",
    }

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=SOURCE)
    parser.add_argument("--registry", type=Path, default=REGISTRY)
    parser.add_argument("--output", type=Path, default=OUT)
    args = parser.parse_args()
    if args.output.resolve() in {args.source.resolve(), args.registry.resolve()}:
        raise ValueError("output_must_not_overwrite_source")
    result = audit(args.source, args.registry)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8", newline="\n")
    print(json.dumps({"source": result["source"], "selected": result["selected_numeric_input_count"],
                      "duplicate_groups": result["exact_duplicate_value_groups"],
                      "constants": result["constant_columns"], "output": str(args.output)}))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
