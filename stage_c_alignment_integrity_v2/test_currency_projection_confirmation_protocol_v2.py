import pytest

from currency_projection_confirmation_protocol_v2 import protocol, validate


def candidate(origins):
    p = protocol()
    return {
        "origins": origins,
        "methods": p["required"]["all_methods"],
        "policy_arms": p["required"]["all_policy_arms"],
        "input_manifest_sha256": "a" * 64,
        "all68_market_quotes": True,
        "mature_labels_before_assessment": True,
    }


def test_inspected_dates_refuse_and_untouched_candidate_passes():
    with pytest.raises(ValueError, match="reuses"):
        validate(candidate([1722535260 + i for i in range(8)]))
    assert validate(candidate([1722535320 + i * 60 for i in range(8)]))


def test_candidate_requires_distinct_ordered_origins_policy_arms_and_input_identity():
    with pytest.raises(ValueError, match="distinct"):
        validate(candidate([1722535320] * 8))
    with pytest.raises(ValueError, match="distinct"):
        validate(candidate([1722535380, 1722535320, 1722535440, 1722535500, 1722535560, 1722535620, 1722535680, 1722535740]))
    bad_arms = candidate([1722535320 + i * 60 for i in range(8)])
    bad_arms["policy_arms"] = bad_arms["policy_arms"][:-1]
    with pytest.raises(ValueError, match="policy_arm"):
        validate(bad_arms)
    bad_identity = candidate([1722535320 + i * 60 for i in range(8)])
    bad_identity["input_manifest_sha256"] = "not-a-sha"
    with pytest.raises(ValueError, match="input_identity"):
        validate(bad_identity)
