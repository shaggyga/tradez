"""Create an isolated successor source kit; never change the running project.

Inputs are exact candidate bytes from an independently reviewed hash manifest.
The original native model bytes and all prior source registrations are retained.
Run only after the candidate authors have frozen their implementation.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
from pathlib import Path
import re

import compact_news_worker_bootstrap_patch_v2 as worker_bootstrap

AREA = Path(__file__).resolve().parent
ROOT = AREA.parent / "trad"
CANDIDATE = AREA / "compact_news_candidate_v1"
REGISTRY = ROOT / "config/joint_price_news_operational_v3_20260913.json"
HELPER = "compact_projection_store_v1.py"
OWNERS = (
    HELPER,
    "projection_revision_admission_v1.py",
    "projection_revision_consumer_v1.py",
    "revision_joint_features_v1.py",
    "revision_news_io_base_v1.py",
    "revision_transport_v4.py",
    "revision_news_io_v10.py",
)
DEPENDENTS = (
    "revision_joint_point_v2.py",
    "shared_revision_history_v3.py",
    "revision_joint_inputs_v3.py",
    "oanda_causal_forecast_ledger_joint_news_v5.py",
    "oanda_joint_price_news_forecast_study_v7.py",
)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def encoded(value):
    return json.dumps(value, sort_keys=True, indent=2, allow_nan=False).encode()


def stage(manifest_path: Path, output: Path, candidate_root: Path = CANDIDATE):
    if output.exists() or output.parent.resolve() != AREA.resolve():
        raise ValueError("new_owned_stage_directory_required")
    registration_raw = REGISTRY.read_bytes()
    registration = json.loads(registration_raw)
    original = {name: (ROOT / name).read_bytes() for name in registration["source_bindings"]}
    if any(sha(raw) != registration["source_bindings"][name] for name, raw in original.items()):
        raise ValueError("original_live_native_source_changed")
    manifest_raw = manifest_path.read_bytes()
    manifest = json.loads(manifest_raw)
    expected = manifest["source_bindings"]
    if set(expected) != set(OWNERS):
        raise ValueError("exact_seven_candidate_owner_bindings_required")
    candidate_root = candidate_root.resolve()
    if candidate_root.parent != AREA.resolve() or manifest_path.resolve().parent != candidate_root:
        raise ValueError("owned_candidate_directory_and_manifest_required")
    candidate = {name: (candidate_root / name).read_bytes() for name in OWNERS}
    if any(sha(raw) != expected[name] for name, raw in candidate.items()):
        raise ValueError("candidate_changed_after_review")
    # V2 composes the reviewed bootstrap patch. Retain both build-time inputs;
    # the generated runtime has neither helper as an import dependency.
    patch_sources = {
        Path(module.__file__): Path(module.__file__).read_bytes()
        for module in (worker_bootstrap, worker_bootstrap.bootstrap)
    }
    staged = dict(original)
    staged.update(candidate)
    # Candidate owners must already bind each other exactly. This utility only
    # changes the downstream original source pins, never candidate semantics.
    replacements = {
        registration["source_bindings"][name]: sha(staged[name])
        for name in OWNERS if name != HELPER
    }
    rebindings = []
    for name in DEPENDENTS:
        raw = original[name]
        text = raw.decode("utf-8")
        if name == "oanda_joint_price_news_forecast_study_v7.py":
            text = worker_bootstrap.apply(text)
        used = []
        for before, after in replacements.items():
            if before in text and before != after:
                used.append({"before": before, "after": after, "occurrences": text.count(before)})
                text = text.replace(before, after)
        if name == "oanda_joint_price_news_forecast_study_v7.py":
            needle = "REQUIRED_SOURCE_BINDINGS=frozenset({\n"
            if text.count(needle) != 1 or HELPER in text:
                raise ValueError("unexpected_worker_source_inventory")
            text = text.replace(needle, needle + " '" + HELPER + "',\n", 1)
        changed = text.encode("utf-8")
        ast.parse(changed, filename=name)
        staged[name] = changed
        replacements[sha(raw)] = sha(changed)
        rebindings.append({"name": name, "original_sha256": sha(raw),
                           "staged_sha256": sha(changed), "bindings_updated": used})
    numeric = "oanda_joint_price_news_models_v1.py"
    if staged[numeric] != original[numeric]:
        raise ValueError("numeric_model_must_remain_exact")
    # No obsolete changed-owner digest may survive in executable staged source.
    old_changed = {old for old, new in replacements.items() if old != new}
    leftovers = {name: sorted(old_changed.intersection(re.findall(r"[0-9a-f]{64}", raw.decode("utf-8"))))
                 for name, raw in staged.items()}
    leftovers = {name: values for name, values in leftovers.items() if values}
    if leftovers:
        raise ValueError("unclosed_successor_source_pins:" + json.dumps(leftovers))
    for name, raw in staged.items():
        ast.parse(raw, filename=name)
    if any(path.read_bytes() != raw for path, raw in patch_sources.items()):
        raise ValueError("worker_patch_changed_during_stage_preparation")
    output.mkdir()
    archive = output / "retained_original_sources"
    kit = output / "kit"
    archive.mkdir()
    kit.mkdir()
    for folder, values in ((archive, original), (kit, staged)):
        for name, raw in values.items():
            with (folder / name).open("xb") as stream:
                stream.write(raw)
    with (archive / REGISTRY.name).open("xb") as stream:
        stream.write(registration_raw)
    with (output / "CANDIDATE_OWNER_MANIFEST.json").open("xb") as stream:
        stream.write(manifest_raw)
    report = {
        "schema_version": "compact_news_isolated_source_stage_v1_20260914",
        "status": "staged_not_deployed",
        "original_registration": str(REGISTRY),
        "original_registration_sha256": sha(registration_raw),
        "candidate_manifest_sha256": sha(manifest_raw),
        "candidate_root": str(candidate_root),
        "worker_bootstrap_patch_sha256": sha(Path(worker_bootstrap.__file__).read_bytes()),
        "worker_patch_source_bindings": {path.name: sha(raw) for path, raw in patch_sources.items()},
        "worker_scheduling_repair": "completion_cadence_and_guarded_pair_fit_handoff",
        "source_bindings": {name: sha(raw) for name, raw in staged.items()},
        "original_source_bindings": registration["source_bindings"],
        "downstream_rebindings": rebindings,
        "numeric_model_unchanged": True,
        "live_sources_changed": False,
        "workers_started": False,
        "orders_enabled": False,
    }
    with (output / "SOURCE_STAGE.json").open("xb") as stream:
        stream.write(encoded(report))
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-manifest", type=Path, required=True)
    parser.add_argument("--candidate-root", type=Path, default=CANDIDATE)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = stage(args.candidate_manifest, args.output, args.candidate_root)
    print(json.dumps({"status": result["status"], "sources": len(result["source_bindings"]),
                      "numeric_model_unchanged": result["numeric_model_unchanged"]}))
