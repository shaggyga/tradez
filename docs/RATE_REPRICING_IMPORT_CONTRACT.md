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
