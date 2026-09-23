"""Prepare prospective successor registrations in an isolated reviewed source kit.

This writes only the staging directory. It never creates a live ledger, changes
the current configurations, claims historic availability, or starts a process.
"""
import argparse
import ast
from copy import deepcopy
import hashlib
import json
from pathlib import Path

AREA = Path(__file__).resolve().parent
ROOT = AREA.parent / "trad"
DATA = ROOT / "data/oanda_training_manager"
SCOPE = "operational_repair_20260913_v4"


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def replace_values(value, substitutions):
    if isinstance(value, str):
        return substitutions.get(value, value)
    if isinstance(value, dict):
        return {key: replace_values(item, substitutions) for key, item in value.items()}
    if isinstance(value, list):
        return [replace_values(item, substitutions) for item in value]
    return value


def literal(path, name):
    matches = [node for node in ast.parse(path.read_bytes()).body if isinstance(node, ast.Assign)
               and any(isinstance(target, ast.Name) and target.id == name for target in node.targets)]
    if len(matches) != 1:
        raise ValueError("single_literal_constant_required:" + name)
    return ast.literal_eval(matches[0].value)


def prepare(stage):
    if stage.resolve().parent != AREA.resolve():
        raise ValueError("owned_source_stage_required")
    kit = stage / "kit"
    report_raw = (stage / "SOURCE_STAGE.json").read_bytes()
    report = json.loads(report_raw)
    bindings = report["source_bindings"]
    if any(sha((kit / name).read_bytes()) != expected for name, expected in bindings.items()):
        raise ValueError("staged_sources_changed")
    original_path = ROOT / "config/joint_price_news_operational_v3_20260913.json"
    original_raw = original_path.read_bytes()
    if sha(original_raw) != report["original_registration_sha256"]:
        raise ValueError("original_registration_changed")
    original = json.loads(original_raw)
    news = DATA / SCOPE / "revision_news_v1"
    filenames = {key: name for key, name in (
        ("base", "revision_news_io_base_operational_v4_20260913.json"),
        ("transport", "revision_transport_operational_v4_20260913.json"),
        ("io", "revision_news_io_operational_v4_20260913.json"),
        ("joint", "joint_price_news_operational_v4_20260913.json"),
    )}
    if any((ROOT / "config" / name).exists() for name in filenames.values()):
        raise ValueError("new_prospective_live_registration_required")
    base = json.loads((ROOT / "config/revision_news_io_base_operational_v3_20260913.json").read_bytes())
    source_path = Path(base["input_identity"]["path"])
    info = source_path.stat()
    if (info.st_dev, info.st_ino) != (base["input_identity"]["device"], base["input_identity"]["inode"]):
        raise ValueError("original_collector_database_identity_changed")
    base.update(schema_version=literal(kit / "revision_news_io_base_v1.py", "CONFIG"),
                cohort_id=SCOPE, consumer_id="joint_all68_native_v7_operational_v4",
                publication_path=str(news / "publication.sqlite"),
                observation_path=str(news / "consumer_observations.sqlite"),
                archive_root=str(news / "capture_archive"))
    base_raw = encoded(base)
    transport = json.loads((ROOT / "config/revision_transport_operational_v3_20260913.json").read_bytes())
    transport.update(schema_version=literal(kit / "revision_transport_v4.py", "CONFIG"),
                     news_io_config_path=str(ROOT / "config" / filenames["base"]),
                     news_io_config_sha256=sha(base_raw), state_root=str(news / "transport_state"))
    transport_raw = encoded(transport)
    io = deepcopy(base)
    io.update(schema_version=literal(kit / "revision_news_io_v10.py", "CONFIG"),
              transport_config_path=str(ROOT / "config" / filenames["transport"]),
              transport_config_sha256=sha(transport_raw))
    io_raw = encoded(io)
    registry = deepcopy(original)
    registry.update(source_bindings=bindings, news_io_config_sha256=sha(io_raw))
    for pair, item in registry["pairs"].items():
        for family, slot in item["families"].items():
            contract = slot["contract"]
            old_id = contract["contract_id"]
            new_id = old_id.rsplit(".", 1)[0] + ".operational_repair_v4"
            substitutions = {old: bindings[name] for name, old in original["source_bindings"].items()}
            substitutions.update({old_id: new_id, old_id + ".evaluation": new_id + ".evaluation",
                                  contract["feature_version"]: "sha256:" + bindings["revision_joint_inputs_v3.py"]})
            # Native outcome source bindings include the ledger's input-loader
            # bytes. Update that nested binding too; its numerical sources stay
            # exact, and the new cohort cannot reuse the old contract seal.
            contract = replace_values(contract, substitutions)
            contract["source_bindings"] = bindings
            native_contract_bytes = json.dumps(contract, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
            slot.update(contract=contract, contract_sha256=sha(native_contract_bytes))
    blobs = {"base": base_raw, "transport": transport_raw, "io": io_raw, "joint": encoded(registry)}
    if len(blobs["joint"]) > 1024 * 1024:
        raise ValueError("registered_native_registry_byte_bound")
    target = stage / "prospective_config"
    target.mkdir()
    for key, raw in blobs.items():
        with (target / filenames[key]).open("xb") as stream:
            stream.write(raw)
    receipt = {
        "schema_version": "compact_news_prospective_registration_v1_20260914",
        "status": "prepared_in_stage_not_validated_or_activated",
        "source_stage_sha256": sha(report_raw),
        "prospective_runtime_root": str(DATA / SCOPE),
        "source_files": len(bindings), "pairs": len(registry["pairs"]),
        "news_policy_sha256": sha(encoded(base["policy"])),
        "original_collector_identity_preserved": True, "historical_rows_imported": False,
        "numeric_source_unchanged": bindings["oanda_joint_price_news_models_v1.py"] == original["source_bindings"]["oanda_joint_price_news_models_v1.py"],
        "configurations": {key: {"filename": filenames[key], "sha256": sha(raw), "bytes": len(raw)} for key, raw in blobs.items()},
        "live_configuration_changed": False, "orders_enabled": False,
    }
    with (stage / "PROSPECTIVE_REGISTRATION.json").open("xb") as stream:
        stream.write(encoded(receipt))
    return receipt


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-stage", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(prepare(args.source_stage)))
