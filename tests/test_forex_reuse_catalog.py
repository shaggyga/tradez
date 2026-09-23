"""Keep portable discovery identities aligned with the production retrieval registry."""
import json
from pathlib import Path, PurePosixPath
import re

ROOT = Path(__file__).resolve().parents[1]


def read(name):
    return json.loads((ROOT / "artifacts" / name).read_text(encoding="utf-8-sig"))


def walk(value):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from walk(child)


def test_catalog_references_stay_portable_and_hash_bound():
    catalog = read("reuse_catalog.json")
    for node in walk(catalog):
        for field in ("vault_relative_path", "workspace_relative_path"):
            if field not in node:
                continue
            value = node[field]
            path = PurePosixPath(value)
            assert value and not path.is_absolute()
            assert ".." not in path.parts and "\\" not in value and ":" not in value
            assert re.fullmatch(r"[a-f0-9]{64}", node["sha256"])
            assert type(node["bytes"]) is int and node["bytes"] >= 0


def test_automated_retrieval_targets_exact_original_registry_identity():
    catalog = read("reuse_catalog.json")
    registry = read("registry.json")["artifacts"]
    automated = {key: val for key, val in catalog["artifacts"].items() if val["retrieval"]["mode"] == "workspace_retrieve"}
    assert set(automated) == set(registry)
    for key, entry in automated.items():
        pinned = registry[key]
        assert entry["retrieval"]["cli_artifact"] == key
        assert entry["original_identity"]["fingerprint"] == pinned["original_run_identity_fingerprint"]
        archive = next(x for x in entry["artifact_refs"] if x["kind"] == "archive")
        assert archive["vault_relative_path"] == pinned["archive_vault_relative_path"]
        assert archive["sha256"] == pinned["archive_sha256"]
        assert archive["bytes"] == pinned["archive_bytes"]
        assert entry["model_count"] == len(entry["model_payloads"]) == pinned["model_count"]
        assert sorted(x["path"] for x in entry["model_payloads"]) == sorted(pinned["model_payloads"])


def test_legacy_identity_and_json_fits_are_not_silently_reclassified():
    catalog = read("reuse_catalog.json")
    directional = catalog["artifacts"]["directional_retained_saved"]
    assert directional["original_identity"]["fingerprint"] is None
    assert directional["original_identity"]["kind"] == "original_frozen_study"
    members = {x["path"]: x for x in directional["archive_members"]}
    assert len(members) == directional["archive_member_count"] == 99
    assert all(members[x["path"]] == x for x in directional["model_payloads"])
    macro = catalog["artifacts"]["macro_event_forecast_saved"]
    assert len(macro["model_payloads"]) == macro["model_count"] == 6
    assert all("coefficient" in x["parameter_fields"] for x in macro["model_payloads"])
    assert directional["retrieval"]["mode"] == macro["retrieval"]["mode"] == "manual_verified_byte_copy"


def test_reuse_inventory_never_implies_runtime_approval_or_new_experiments():
    catalog = read("reuse_catalog.json")
    assert catalog["coverage"]["exhaustive_model_census"] is False
    assert catalog["coverage"]["model_counts_are_not_unique_experiment_counts"] is True
    assert len(catalog["inherited_records"]) == catalog["coverage"]["inherited_approval_records"] == 48
    for item in [*catalog["artifacts"].values(), *catalog["inherited_records"]]:
        assert item["runtime_requalified"] is False
        assert item["independent_review"] is False
        assert item["execution_authorization"] is False
    counts = sum(x["model_count"] for x in catalog["artifacts"].values())
    assert counts == catalog["coverage"]["shared_saved_model_payloads_or_parameter_records"] == 120
    assert all(x["classification"] == "preserved_to_saved_package" and x["saved_artifact_catalog_id"] in catalog["artifacts"] for x in catalog["prior_local_only_model_gaps"])
