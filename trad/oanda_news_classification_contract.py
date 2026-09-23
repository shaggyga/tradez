"""Shared immutable identifiers for the active FX-news classification contract.

Keep this tiny module free of collector imports so read-only integrity tools can
bind to the producer contract without loading network or parser dependencies.
"""

NEWS_CLASSIFICATION_VERSION_V164 = (
    "local_fx_news_rules_20260904_v164_conflict_duration_recap_guard"
)
NEWS_CLASSIFICATION_VERSION = (
    "local_fx_news_rules_20260907_v165_causal_member_admission"
)

# V165 admits each corroborating article using its own source/availability
# clocks and original reaction expiry. Current direction cannot inherit an
# earlier member's timeliness, or pool broad categories into claim evidence.
# Historical V164 snapshots and registered forecast/outcome records remain
# under their existing identities. Actual deployment time is separately
# recorded by the controlled collector reload, never invented here.

# V162 closes two live secondary-headline gaps without changing the execution
# boundary: a policy action that depends on a future release carries no present
# policy sign, and a headline describing an already accumulated weekly market
# move is reaction context rather than the event that caused it. Prior V161
# forecasts and outcomes remain immutable under their original lineage.
# V163 additionally prevents two opposing policy prescriptions for the same
# currency from being flattened into whichever phrase is processed last.
# V164 keeps secondary conflict-duration recaps (for example, a war entering
# its seventh month) from masquerading as a new escalation clock. A separate
# explicit strike, attack, sanction, mobilization or other newly observed
# action remains eligible under the normal event rules.

CONFLICT_DURATION_RECAP_GUARD_ACTIVATED_UTC_V1 = (
    "2026-09-04T20:25:00+00:00"
)
CONFLICT_DURATION_RECAP_GUARD_CONTRACT_ID_V1 = (
    "secondary_conflict_duration_recap_guard_v1_20260904"
)
CONFLICT_DURATION_RECAP_GUARD_COHORT_ID_V1 = (
    "secondary_conflict_duration_recap_guard_v1_prospective_20260904T202500Z"
)

# Drilling-rig counts and publisher-labelled analysis pieces describe a
# market statistic or interpretation, not a newly observed executable price
# direction.  From this boundary they remain auditable context and require a
# separately observed commodity/rate/FX response; they cannot seed a currency
# factor by lexical inheritance alone.
SECONDARY_MARKET_STATE_GUARD_ACTIVATED_UTC_V1 = (
    "2026-09-04T19:00:00+00:00"
)
SECONDARY_MARKET_STATE_GUARD_CONTRACT_ID_V1 = (
    "secondary_market_state_analysis_guard_v1_20260904"
)
SECONDARY_MARKET_STATE_GUARD_COHORT_ID_V1 = (
    "secondary_market_state_analysis_guard_v1_prospective_20260904T190000Z"
)

# A unilateral request, proposal, or demand for a ceasefire is not evidence
# that the parties agreed to or implemented one.  Beginning at this frozen
# boundary, proposal-only language is retained as context but cannot create a
# broad risk-on currency basket.  Earlier rows can be reclassified for
# diagnostics, while their activation flag keeps them out of prospective
# proof and existing forecast/outcome ledgers remain immutable.
DEESCALATION_PROPOSAL_GUARD_ACTIVATED_UTC_V1 = (
    "2026-09-04T18:30:00+00:00"
)
DEESCALATION_PROPOSAL_GUARD_CONTRACT_ID_V1 = (
    "deescalation_proposal_guard_v1_20260904"
)
DEESCALATION_PROPOSAL_GUARD_COHORT_ID_V1 = (
    "deescalation_proposal_guard_v1_prospective_20260904T183000Z"
)

# A verified Japanese MOF press conference can report foreign official
# pressure on Japan's policy mix before secondary English-language coverage
# reaches the collector. This is a setup/expectations hypothesis, not a BOJ
# action and not an execution signal. Replayed documents observed before the
# boundary remain diagnostic fixtures only.
JAPAN_EXTERNAL_POLICY_PRESSURE_ACTIVATED_UTC_V1 = (
    "2026-09-04T18:00:00+00:00"
)
JAPAN_EXTERNAL_POLICY_PRESSURE_CONTRACT_ID_V1 = (
    "japan_external_policy_pressure_research_v1_20260904"
)
JAPAN_EXTERNAL_POLICY_PRESSURE_COHORT_ID_V1 = (
    "japan_external_policy_pressure_research_v1_prospective_20260904T180000Z"
)

# Official defense feeds publish many operational and personnel notices that
# contain combat vocabulary in agency boilerplate but are not new geopolitical
# events.  This prospective boundary keeps those documents out of the global
# risk basket without changing or deleting their immutable raw observations.
OFFICIAL_DEFENSE_NONMARKET_HEALTH_GUARD_ACTIVATED_UTC_V1 = (
    "2026-09-02T20:45:00+00:00"
)
OFFICIAL_DEFENSE_NONMARKET_HEALTH_GUARD_CONTRACT_ID_V1 = (
    "official_defense_nonmarket_health_guard_v1_20260902"
)
OFFICIAL_DEFENSE_NONMARKET_HEALTH_GUARD_COHORT_ID_V1 = (
    "official_defense_nonmarket_health_guard_v1_prospective_20260902T204500Z"
)

# A trusted official-publisher headline discovered through Google News is not
# the same evidence as the direct central-bank body.  From this prospective
# boundary onward, an explicit policy-rate action in such a headline may be
# retained as a structured numeric observation, but it remains research-only
# and directionless until causal consensus and rate-market repricing exist.
OFFICIAL_SEARCH_POLICY_RATE_ACTIVATED_UTC_V1 = (
    "2026-09-02T02:45:00+00:00"
)
OFFICIAL_SEARCH_POLICY_RATE_CONTRACT_ID_V1 = (
    "official_search_policy_rate_observation_v1_20260902"
)
OFFICIAL_SEARCH_POLICY_RATE_COHORT_ID_V1 = (
    "official_search_policy_rate_observation_v1_prospective_20260902T024500Z"
)
OFFICIAL_SEARCH_POLICY_RATE_SOURCE_CURRENCIES_V1 = {
    "banxico_official_search": "MXN",
    "bot_official_search": "THB",
    "mas_official_search": "SGD",
    "mnb_official_search": "HUF",
    "nationalbanken_official_search": "DKK",
    "nbp_official_search": "PLN",
    "pboc_official_search": "CNH",
    "rbnz_official_search": "NZD",
    "sarb_official_search": "ZAR",
    "snb_official_search": "CHF",
}

# Dedicated, authoritative policy-communication transports whose container
# identity is itself part of the document contract. A speech title does not
# always contain the literal word ``speech`` (for example, Riksbank titles are
# commonly just ``Speaker: subject``), so title-only document classification
# can otherwise let comparison countries, oil examples, or foreign central
# banks become synthetic directional observations. This map is deliberately
# narrower than every ``communication_source_id`` in the authority registry:
# mixed press/publication feeds continue to require an explicit document-type
# marker.
ISSUER_BOUND_POLICY_COMMUNICATION_SOURCE_CURRENCIES_V2 = {
    "boc_speeches": "CAD",
    "boe_speeches": "GBP",
    "cleveland_fed_speeches": "USD",
    "fed_speeches": "USD",
    "japan_mof_international_policy": "JPY",
    "japan_mof_press_conferences_ja": "JPY",
    "norges_speeches": "NOK",
    "rba_speeches": "AUD",
    "richmond_fed_speeches": "USD",
    "riksbank_speeches": "SEK",
}

ISSUER_BOUND_POLICY_COMMUNICATION_SOURCE_CONTRACT_ID_V2 = (
    "issuer_bound_policy_communication_v2_authority_source_identity_20260901"
)
ISSUER_BOUND_POLICY_COMMUNICATION_SOURCE_COHORT_ID_V2 = (
    "issuer_bound_policy_communication_v2_prospective_20260901T160000Z"
)
ISSUER_BOUND_POLICY_COMMUNICATION_SOURCE_ACTIVATED_UTC_V2 = (
    "2026-09-01T16:00:00+00:00"
)
