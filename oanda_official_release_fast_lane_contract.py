"""Shared immutable contracts for the official-release fast lane.

This module deliberately has no collector, governance, research, or broker
imports.  Producers, adapters, and read-only integrity checks can therefore
bind the same identities without importing an operational worker.
"""

from __future__ import annotations

import datetime as dt


OFFICIAL_RELEASE_FAST_LANE_SCHEMA_VERSION = "official_release_fast_lane_v4"

# The V4 table schema is unchanged.  This new collector cohort adds only the
# authoritative transports named by ``communication_source_ids`` in the
# governed central-bank source map.  Retained rows continue to carry the
# contract/cohort active when they were observed; they are never rewritten.
OFFICIAL_RELEASE_FAST_LANE_PRIOR_CONTRACT_ID = (
    "official_release_fast_lane_v4_identity_and_publication_clock_20260824"
)
OFFICIAL_RELEASE_FAST_LANE_PRIOR_COHORT_ID = (
    "official_release_fast_lane_v4_20260824"
)
OFFICIAL_RELEASE_FAST_LANE_CONTRACT_ID = (
    "official_release_fast_lane_v4_selection_v2_authoritative_communications_"
    "20260828T150000Z"
)
OFFICIAL_RELEASE_FAST_LANE_COHORT_ID = (
    "official_release_fast_lane_v4_communications_20260828T150000Z"
)
OFFICIAL_RELEASE_FAST_LANE_ACTIVATED_UTC = dt.datetime(
    2026, 8, 28, 15, 0, 0, tzinfo=dt.timezone.utc
)

# The prior adapter identity is retained so integrity checks can validate its
# immutable receipts without treating them as members of the active cohort.
OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_PRIOR_CONTRACT_ID = (
    "official_fast_lane_source_governance_adapter_v1_prospective_only_20260827T060000Z"
)
OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_PRIOR_ACTIVATED_UTC = dt.datetime(
    2026, 8, 27, 6, 0, 0, tzinfo=dt.timezone.utc
)
OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_CONTRACT_ID = (
    "official_fast_lane_source_governance_adapter_v2_communications_"
    "prospective_only_20260828T150000Z"
)
OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_ACTIVATED_UTC = dt.datetime(
    2026, 8, 28, 15, 0, 0, tzinfo=dt.timezone.utc
)


__all__ = [
    "OFFICIAL_RELEASE_FAST_LANE_SCHEMA_VERSION",
    "OFFICIAL_RELEASE_FAST_LANE_PRIOR_CONTRACT_ID",
    "OFFICIAL_RELEASE_FAST_LANE_PRIOR_COHORT_ID",
    "OFFICIAL_RELEASE_FAST_LANE_CONTRACT_ID",
    "OFFICIAL_RELEASE_FAST_LANE_COHORT_ID",
    "OFFICIAL_RELEASE_FAST_LANE_ACTIVATED_UTC",
    "OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_PRIOR_CONTRACT_ID",
    "OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_PRIOR_ACTIVATED_UTC",
    "OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_CONTRACT_ID",
    "OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_ACTIVATED_UTC",
]
