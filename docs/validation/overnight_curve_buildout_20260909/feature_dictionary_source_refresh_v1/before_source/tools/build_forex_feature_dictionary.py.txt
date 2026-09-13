"""Build explanatory feature records without importing models or touching runtime.

The authored parts are reviewed documentation, not inferred formulas. Design
intent and source-verified computation remain separate throughout rendering.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
PARTS = {
    "catalog": "docs/feature_dictionary/catalog_251.json",
    "active_joint": "docs/feature_dictionary/active_joint_34.json",
    "historical": "docs/feature_dictionary/historical_engines.json",
}
OUT_JSON = "docs/FOREX_FEATURE_DICTIONARY_CURRENT.json"
OUT_MD = "docs/FOREX_FEATURE_DICTIONARY_CURRENT.md"
REQUIRED = (
    "feature_id", "name", "block", "plain_language", "role",
    "intended_inputs", "intended_units", "intended_lookback", "computation",
    "implementation_status", "causal_warning", "source_refs", "gaps",
)


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def encode(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")


def text(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return "; ".join(text(item) for item in value) if value else "None recorded."
    if isinstance(value, dict):
        return "; ".join(f"{key}: {text(child)}" for key, child in value.items())
    return str(value)


def load_parts(root: Path) -> tuple[dict, dict]:
    parts, hashes = {}, {}
    for key, relative in PARTS.items():
        data = (root / relative).read_bytes()
        parts[key] = json.loads(data)
        hashes[relative] = sha(data)
    return parts, hashes


def feature_rows(part: dict) -> list[dict]:
    rows = part.get("features")
    if not isinstance(rows, list):
        raise ValueError("part must contain a features list")
    return rows


def nested_references(value: Any):
    if isinstance(value, dict):
        for key, child in value.items():
            if key == "source_refs" and isinstance(child, list):
                yield from child
            else:
                yield from nested_references(child)
    elif isinstance(value, list):
        for child in value:
            yield from nested_references(child)


def validate_parts(parts: dict, spec: dict, root: Path | None = None) -> dict:
    errors = []
    source_bindings = {}
    for part_name, part in parts.items():
        bindings = part.get("source_hashes")
        if not isinstance(bindings, dict) or not bindings:
            errors.append(f"{part_name}: missing inspected-source hashes")
            continue
        for relative, expected_hash in bindings.items():
            normalized = relative.replace("\\", "/")
            if ":" in normalized or Path(normalized).is_absolute() or ".." in Path(normalized).parts:
                errors.append(f"{part_name}: source binding must be canonical relative path")
                continue
            if normalized.startswith(("data/", "artifacts/", ".git/")):
                errors.append(f"{part_name}: source binding may not read runtime/bulk data")
                continue
            if not isinstance(expected_hash, str) or len(expected_hash) != 64:
                errors.append(f"{part_name}: invalid source hash for {relative}")
            elif relative in source_bindings and source_bindings[relative] != expected_hash:
                errors.append(f"conflicting source versions: {relative}")
            else:
                source_bindings[relative] = expected_hash
                if root is not None:
                    target = root / relative
                    if not target.resolve().is_relative_to(root.resolve()) or not target.is_file():
                        errors.append(f"{part_name}: missing/redirected bound source {relative}")
                    elif sha(target.read_bytes()) != expected_hash:
                        errors.append(f"{part_name}: inspected source changed; review definition before rebuilding: {relative}")
    expected = {(block, name) for block, data in spec["feature_blocks"].items()
                for name in data["features"]}
    catalog = feature_rows(parts["catalog"])
    active = feature_rows(parts["active_joint"])
    actual = {(row.get("block"), row.get("name")) for row in catalog}
    if actual != expected or len(catalog) != len(expected):
        errors.append("catalog membership differs from frozen design specification")
    if len(active) != 34:
        errors.append("current joint dictionary must contain exactly 34 inputs")
    source_cache: dict[str, list[str]] = {}
    for key, rows in (("catalog", catalog), ("active_joint", active)):
        ids = [row.get("feature_id") for row in rows]
        if len(ids) != len(set(ids)):
            errors.append(f"{key}: duplicate feature IDs")
        if len({row.get("name") for row in rows}) != len(rows):
            errors.append(f"{key}: duplicate feature names")
        for row in rows:
            label = f"{key}/{row.get('name')}"
            for field in REQUIRED:
                if field not in row or row[field] is None or row[field] == "":
                    errors.append(f"{label}: missing {field}")
            if not row.get("source_refs"):
                errors.append(f"{label}: no source reference")
            for ref in row.get("source_refs", []):
                if not isinstance(ref, dict) or not ref.get("path") or not ref.get("evidence_kind"):
                    errors.append(f"{label}: incomplete source reference")
                    continue
                line = ref.get("line")
                if not isinstance(line, int) or line < 1:
                    errors.append(f"{label}: invalid source line")
                    continue
                # Only local source references are checked. Historical external
                # locations stay explicitly external; never read D as live truth.
                relative = ref["path"].replace("\\", "/")
                if root is not None and not Path(relative).is_absolute() and ":" not in relative:
                    target = root / relative
                    if ".." in Path(relative).parts or not target.resolve().is_relative_to(root.resolve()):
                        errors.append(f"{label}: reference escapes canonical root")
                    elif not target.is_file():
                        errors.append(f"{label}: missing local source {relative}")
                    elif target.suffix.lower() not in {".py", ".json", ".md", ".ps1", ".txt"}:
                        errors.append(f"{label}: non-source reference")
                    else:
                        if relative not in source_cache:
                            source_cache[relative] = target.read_text(encoding="utf-8-sig").splitlines()
                        if line > len(source_cache[relative]):
                            errors.append(f"{label}: source line beyond EOF {relative}")
    engines = parts["historical"].get("engines", [])
    if len(engines) != 6 or len({row.get("id") for row in engines}) != 6:
        errors.append("historical dictionary must distinguish six engine schemas")
    for engine in engines:
        for field in ("id", "name", "feature_count", "horizons", "generation_recipe",
                      "availability", "validation_limits", "source_refs", "gaps"):
            if field not in engine or engine[field] is None:
                errors.append(f"historical/{engine.get('id')}: missing {field}")
        fields = engine.get("features", [])
        if len(fields) != engine.get("feature_count"):
            errors.append(f"historical/{engine.get('id')}: feature inventory count mismatch")
        if len({field.get("id") for field in fields}) != len(fields):
            errors.append(f"historical/{engine.get('id')}: duplicate feature IDs")
        for field in fields:
            for required in ("id", "name", "meaning", "inputs", "units", "lookback", "generation_recipe",
                             "availability", "validation_limits", "source_refs", "gaps"):
                if required not in field or field[required] is None or field[required] == "":
                    errors.append(f"historical/{engine.get('id')}/{field.get('id')}: missing {required}")
    # Shared derivations and historical per-field recipes are as important as
    # current per-input descriptions. Every canonical reference needs a binding.
    for ref in nested_references(parts):
        if not isinstance(ref, dict) or not ref.get("path") or not ref.get("evidence_kind"):
            errors.append("incomplete nested source reference")
            continue
        relative = ref["path"].replace("\\", "/")
        candidate = Path(relative)
        if candidate.is_absolute() or ":" in relative:
            if root is not None and candidate.is_relative_to(root):
                relative = candidate.relative_to(root).as_posix()
            else:
                continue  # Explicit external historical location; do not read.
        if ".." in Path(relative).parts:
            errors.append("nested reference escapes canonical root")
            continue
        if relative not in source_bindings:
            errors.append(f"local reference lacks inspected-source binding: {relative}")
        line = ref.get("line")
        if not isinstance(line, int) or line < 1:
            errors.append(f"invalid nested source line: {relative}")
        elif root is not None:
            target = root / relative
            if not target.resolve().is_relative_to(root.resolve()) or not target.is_file():
                errors.append(f"missing/redirected nested source: {relative}")
            elif target.suffix.lower() not in {".py", ".json", ".md", ".ps1", ".txt"}:
                errors.append(f"non-source nested reference: {relative}")
            else:
                if relative not in source_cache:
                    source_cache[relative] = target.read_text(encoding="utf-8-sig").splitlines()
                if line > len(source_cache[relative]):
                    errors.append(f"nested source line beyond EOF: {relative}")
    if errors:
        raise ValueError("\n".join(errors[:35]))
    return {
        "design_entries": len(catalog), "design_blocks": len(spec["feature_blocks"]),
        "current_joint_entries": len(active), "historical_engine_schemas": len(engines),
        "historical_field_records": sum(len(engine.get("features", [])) for engine in engines),
        "historical_schema_counts": {engine["id"]: len(engine.get("features", [])) for engine in engines},
        "design_status_counts": dict(Counter(row["implementation_status"] for row in catalog)),
        "joint_status_counts": dict(Counter(row["implementation_status"] for row in active)),
        "local_reference_files_checked": len(source_cache),
        "inspected_source_bindings_checked": len(source_bindings),
        "documentation_validation": "passed",
        "meaning": "Coverage and referenced locations checked; not a certificate of all formulas, runtime, or edge.",
    }


def references(refs: list[dict]) -> str:
    return "; ".join(f"`{ref.get('path')}:{ref.get('line', '?')}` ({ref.get('evidence_kind', 'unspecified')})"
                     for ref in refs)


def table_text(value: Any) -> str:
    return text(value).replace("|", "&#124;").replace("\r", "").replace("\n", "<br>")


def render_definition(value: Any, level: int = 4) -> list[str]:
    """Keep shared equations and provenance readable instead of one huge paragraph."""
    if isinstance(value, dict):
        lines = []
        for key, child in value.items():
            if key == "source_refs" and isinstance(child, list):
                lines.extend(["Sources: " + references(child), ""])
            else:
                lines.extend(["#" * min(level, 6) + " " + key.replace("_", " ").capitalize(), ""])
                lines.extend(render_definition(child, level + 1))
        return lines
    if isinstance(value, list):
        if not value:
            return ["None recorded.", ""]
        if all(not isinstance(item, (dict, list)) for item in value):
            return ["- " + text(item) for item in value] + [""]
        lines = []
        for index, child in enumerate(value, 1):
            lines.extend(["#" * min(level, 6) + f" Record {index}", ""])
            lines.extend(render_definition(child, level + 1))
        return lines
    return [text(value), ""]


def render_historical_fields(engine: dict) -> list[str]:
    lines = ["#### Individual fields", "",
             "Recipe paths below point into the engine-level definitions above (and the identically",
             "named JSON keys). A shared recipe is parameterized by the field's timeframe/period.",
             "Historical-version caveats apply to every row; a current analogous formula does not",
             "prove the original fitted version used it. The JSON also retains each field's inputs",
             "and source locations without abbreviation.", "",
             "| Field | Meaning and calculation | Units / window | Evidence limits and source |",
             "|---|---|---|---|"]
    for row in engine.get("features", []):
        calculation = table_text(row["meaning"]) + "<br>" + table_text(row["generation_recipe"])
        scale = table_text(row["units"]) + "<br>" + table_text(row["lookback"])
        limits = table_text(row["availability"]) + "<br>" + table_text(row["validation_limits"])
        if row["gaps"]:
            limits += "<br>Gap: " + table_text(row["gaps"])
        limits += "<br>" + table_text(references(row["source_refs"]))
        lines.append(f"| `{row['id']}` | {calculation} | {scale} | {limits} |")
    return lines + [""]


def render_feature(row: dict) -> list[str]:
    lines = [f"#### `{row['name']}`", "", text(row["plain_language"]), "",
             f"- Role/status: **{row['role']}** / `{row['implementation_status']}`.",
             f"- Inputs: {text(row['intended_inputs'])}",
             f"- Units: {text(row['intended_units'])}",
             f"- Lookback: {text(row['intended_lookback'])}",
             f"- Calculation/evidence: {text(row['computation'])}"]
    if "vector_index" in row:
        lines.append(f"- Vector position: {row['vector_index']} (zero-based); documentation ID: `{row['feature_id']}`.")
    if row.get("implementation_name"):
        lines.append(f"- Implementation name: `{row['implementation_name']}`.")
    if row.get("intended_definition"):
        lines.append(f"- Design interpretation (not implementation proof): {text(row['intended_definition'])}")
    for key in ("missingness", "normalization", "example"):
        if row.get(key):
            lines.append(f"- {key.capitalize()}: {text(row[key])}")
    lines.extend([f"- Timing/leakage: {text(row['causal_warning'])}",
                  f"- Source: {references(row['source_refs'])}",
                  f"- Remaining documentation/implementation gaps: {text(row['gaps'])}", ""])
    return lines


def render(document: dict) -> str:
    coverage, parts = document["coverage"], document["parts"]
    lines = ["# Forex feature generation dictionary", "",
             "Canonical project: `C:\\Users\\zmoor\\Documents\\forex\\trad`.", "",
             "This is an explanatory record, not merely a file inventory. It separates the",
             "historical design catalogue, actual historical model schemas, and the current",
             "joint price/news calculation, as inspected September 8, 2026 UTC. Counts from those sets must not be added together",
             "as independent predictors. Nothing here changes a model or enables orders.", "",
             "Machine-readable companion: `FEATURE_DICTIONARY_CURRENT.json` in the vault;",
             "`docs/FOREX_FEATURE_DICTIONARY_CURRENT.json` in the source project.", "",
             "Navigation: [current inputs](#current-joint-pricenews-inputs),",
             "[historical engines](#historical-implemented-feature-engines),",
             "[design catalogue](#full-historical-design-catalogue). The separate",
             "`FEATURE_GENERATION_WORKED_EXAMPLE.md` in the vault walks through sparse prices,",
             "news context, interactions and fit arithmetic without creating a forecast.", "",
             "## Read this first", "",
             f"- **{coverage['design_entries']} design entries / {coverage['design_blocks']} blocks:** each has a plain-language definition, units, lookback, role and provenance. Design intent is not proof of implementation.",
             f"- **{coverage['current_joint_entries']} current joint-model inputs:** traced calculations, price/news interactions and causal/missing-data rules.",
             "- **Six historical schemas:** 227; corrected 220/candidates up to 267; 795 + 3 categorical; MA up to 643; second-ridge 14; direct currency panel 223. These are distinct populations and versions.",
             "- **Evidence status is explicit:** implemented calculation, design-only definition, missing archived code, inactive adapter and unavailable data are not interchangeable.",
             "- Exact source references refer to the canonical project unless explicitly labelled as archived/external. A stored formula cannot recover missing original clocks, datasets or fitted weights.", "",
             "## How the information becomes a forecast", "",
             "Completed price observations supply movement, volatility and missingness inputs.",
             "Executable quotes and costs belong to the separate entry/outcome evaluation;",
             "they are not additional columns in the current 34-input fit. Admitted news supplies separate context and vetted",
             "direction inputs at their own knowledge times. The current joint model combines",
             "those inputs and interactions through a fitted regression. Its output is compared",
             "with price-only and neutral-news baselines, then scored on original future quote",
             "outcomes. A model input is not a trade, and a forecast is not proven edge.", "",
             "### Documentation coverage", "",
             "Design status counts: " + text(coverage["design_status_counts"]) + ".", "",
             "Active-model status counts: " + text(coverage["joint_status_counts"]) + ".", "",
             "Validation checks exact catalogue membership, unique names, required explanations",
             "and referenced local line ranges. It does not promote intended definitions to verified code.", "",
             "## Current joint price/news inputs", ""]
    active = parts["active_joint"]
    for key in ("overview", "generation_rules", "shared_definitions", "definitions", "model_transform",
                "upstream_news_mapping", "causal_clocks", "normalization", "missingness_policy", "causal_contract", "limitations"):
        if active.get(key):
            lines.extend([f"### {key.replace('_', ' ').capitalize()}", ""])
            lines.extend(render_definition(active[key]))
    for row in active["features"]:
        lines.extend(render_feature(row))
    lines.extend(["## Historical implemented feature engines", ""])
    for engine in parts["historical"]["engines"]:
        lines.extend([f"### {engine['name']}", ""])
        for key in ("feature_count", "horizons", "meaning", "inputs", "units", "lookback",
                    "generation_recipe", "availability", "validation_limits", "gaps"):
            if key in engine:
                lines.extend([f"#### {key.replace('_', ' ').capitalize()}", ""])
                lines.extend(render_definition(engine[key], 5))
        lines.extend(["Source: " + references(engine["source_refs"]), ""])
        if engine.get("features"):
            lines.extend(render_historical_fields(engine))
        for key, value in engine.items():
            if key not in {"id", "name", "feature_count", "horizons", "meaning", "inputs", "units", "lookback",
                           "generation_recipe", "availability", "validation_limits", "gaps", "source_refs", "features"}:
                lines.extend([f"#### {key.replace('_', ' ').capitalize()}", ""])
                lines.extend(render_definition(value, 5))
    lines.extend(["## Full historical design catalogue", "",
                  "The following entries explain intended meanings individually. Where exact production",
                  "math or wiring was not established, that remains an explicit gap—not invented implementation.", ""])
    rows = parts["catalog"]["features"]
    for block in sorted({row["block"] for row in rows}):
        lines.extend([f"### {block}", ""])
        for row in rows:
            if row["block"] == block:
                lines.extend(render_feature(row))
    lines.extend(["## Rebuild, verification and preservation", "",
                  "Authored parts: `docs/feature_dictionary/`. Rebuild with",
                  "`python -B tools/build_forex_feature_dictionary.py --build`; verify with `--check`.", "",
                  "This offline documentation tool imports no model and reads no broker or runtime database.",
                  "It retains source-part hashes; update explanations through reviewed changes when the",
                  "underlying contracts change. Do not rewrite historical proof or imply source-only recovery",
                  "reconstructs the original fitted model or information arrival times.", ""])
    return "\n".join(lines)


def assemble(root: Path) -> tuple[bytes, bytes, dict]:
    parts, hashes = load_parts(root)
    spec_data = (root / "config/model_feature_space.json").read_bytes()
    coverage = validate_parts(parts, json.loads(spec_data), root)
    document = {
        "schema_version": "forex_explanatory_feature_dictionary_v1",
        "scope": "Documentation only; preserves design, code and historical evidence distinctions.",
        "canonical_project": str(root), "coverage": coverage, "parts": parts,
        "authored_part_sha256": hashes,
        "catalog_spec_sha256": sha(spec_data),
        "generator_sha256": sha(Path(__file__).read_bytes()),
        "runtime_or_model_changes": False,
    }
    return encode(document), render(document).encode("utf-8"), coverage


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--build", action="store_true")
    mode.add_argument("--check", action="store_true")
    args = parser.parse_args()
    root = args.root.resolve()
    json_bytes, md_bytes, coverage = assemble(root)
    outputs = ((root / OUT_JSON, json_bytes), (root / OUT_MD, md_bytes))
    if args.build:
        # These are deterministic generated documentation artifacts only.
        for path, data in outputs:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
    else:
        for path, data in outputs:
            if not path.is_file() or path.read_bytes() != data:
                raise SystemExit(f"documentation output is missing/stale: {path.name}")
    print(json.dumps({"status": "built" if args.build else "verified", **coverage,
                      "json_sha256": sha(json_bytes), "markdown_sha256": sha(md_bytes)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
