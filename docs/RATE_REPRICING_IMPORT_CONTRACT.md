# Intraday rate-repricing import contract

The news-verification layer accepts a provider-generated state at:

`data/oanda_training_manager/state/rates_policy_repricing_v1.json`

The provider must supply one current row per currency with:

- `currency`
- `instrument`
- `observed_utc`
- `source_timestamp_utc`
- `retrieved_utc`
- `change_bps_15m`
- `change_bps_60m`
- `source_id`
- `source_contract_id`

The source must represent a timestamp-safe OIS curve, policy-rate future, or
equivalent market-implied policy path. Government bond yields, policy-rate
targets, article text, and inferred FX movements cannot satisfy this contract.

Source time must not follow retrieval time, retrieval must not follow the
decision cutoff, and the latest observation must be no more than 15 minutes
old. Provider, instrument, timestamp semantics, parser, sampling frequency, or
curve-tenor changes require a new source cohort.

Until such a licensed feed is connected, the canonical state remains
`source_not_connected`. Missing rates are unavailable, never zero or neutral.
The official daily-rate collector is a separate H4/H24 research source and is
not allowed to write this file.

The prospective ingestion and knowledge-time replay scaffold is separately
frozen in `config/rates_policy_repricing_shadow_v2.json` and
`oanda_rates_policy_repricing_shadow.py`. It does not rewrite this hash-pinned
V1 placeholder or feed execution. A future provider must first open an explicit
connected source cohort in V2; until then V2 also publishes zero causal rows.
V2 does not accept a row's self-declared trust flag. Causal replay additionally
requires the exact configured cohort/source-contract identity, a configured
HMAC clock attestation, and a content-addressed raw payload whose bytes match
the declared SHA-256. The payload path must be an absolute regular file inside
that source's explicit `raw_intake_root`; traversal, outside paths, and any
symlink component are rejected before archival. Superseded or foreign contract
rows remain immutable but are excluded from current replay, readiness, and
report counts.
