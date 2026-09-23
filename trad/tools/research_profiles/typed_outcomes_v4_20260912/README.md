# Typed passive outcome profile v4

This is an installed, inactive research profile. It reuses the existing v3
outcome clock, maturity, receipt, lock and recovery infrastructure. No existing
ledger is migrated, no old trial is resumed and no service starts on import.
The old producer/feed/registry/manager paths remain separate legacy callers.

The new producer and intake keep long/short classifier probabilities and
ranking scores separate from midpoint P(up). Unsupported midpoint probability,
confidence, calibration and Brier values stay null. Legacy declared magnitude
estimates remain labelled as unverified source estimates. Source clock syntax
does not prove historical feature completion, causal availability or unit
parity. Promotion and order eligibility remain false.

The explicit run_typed_source_profile API accepts an injected producer and a
fixed source identity. It normalizes each new generation to a typed content ID
and preserves the original producer ID in receipts. Replays retain the first
observation and insert no duplicate. Targets remain original-generation plus
horizon. New generations sharing a candle do not modify older targets.

The v4 SQLite schema and profile marker are distinct. Buy/sell signals can
receive directional and executable endpoint outcomes. Flat, hold and unavailable
signals retain observed midpoint movement with null action accuracy/P&L. Current
typed kinds supply no supported midpoint probability, so the Brier denominator
is zero. Per-pair summaries expose separate counts; native pip sums are not
portfolio or dollar returns. Missing quotes respect the original censor window.

Run the included tests from this directory with the configured project Python:

    python -B run_tests_v1.py uniqueRunName

The harness uses fictional estimators, clocks and owned temporary databases.
It refuses network, child processes, real artifacts and outside databases.
Choose a new alphanumeric run name each time; old receipts remain retained.

Source inspection and synthetic tests establish this profile's local behavior.
They do not establish live model loading, calibrated direction, profitability,
broker operation, complete native dependency recovery or whole-system startup.
The exact default model-gap artifacts remain an external recovery requirement.
There is deliberately no automatic service or trading activation command here.
