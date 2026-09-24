"""Refuse confirmation candidates that reuse inspected projection-policy dates."""
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SHA256 = re.compile(r"[0-9a-f]{64}")


def protocol():
    return json.loads((ROOT / "CURRENCY_PROJECTION_CONFIRMATION_PROTOCOL_V2.json").read_text())


def validate(candidate):
    p = protocol()
    origins = candidate.get("origins", [])
    required = p["required"]
    if not isinstance(origins, list) or len(origins) < required["minimum_new_origins"]:
        raise ValueError("confirmation_origin_count")
    if any(not isinstance(origin, int) for origin in origins):
        raise ValueError("confirmation_origin_type")
    if origins != sorted(origins) or len(set(origins)) != len(origins):
        raise ValueError("confirmation_origins_not_distinct_ordered")
    if any(origin <= p["last_inspected_target_epoch"] for origin in origins):
        raise ValueError("confirmation_reuses_inspected_or_unmatured_origin")
    if candidate.get("methods") != required["all_methods"]:
        raise ValueError("confirmation_method_inventory")
    if candidate.get("policy_arms") != required["all_policy_arms"]:
        raise ValueError("confirmation_policy_arm_inventory")
    input_sha256 = candidate.get("input_manifest_sha256")
    if not isinstance(input_sha256, str) or not SHA256.fullmatch(input_sha256):
        raise ValueError("confirmation_input_identity")
    if candidate.get("all68_market_quotes") is not True or candidate.get("mature_labels_before_assessment") is not True:
        raise ValueError("confirmation_input_qualification")
    return True
