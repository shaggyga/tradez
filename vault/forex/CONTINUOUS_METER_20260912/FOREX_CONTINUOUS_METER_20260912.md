# Continuous currency-news capture — September 12

[Implemented service, source audit and recreation](../../currency_meter_continuous_20260912/README.md). **The service has started; fresh collection remains blocked by upstream clock verification.** Automatic approval review rejected the clock-monitor startup with "blocked by policy" and no further reason. No bypass occurred.

133 tests passed in both original and independent restored copies. New storage preserves exact article versions and ordering; the unchanged second benchmark capture adds 8 KiB. The worker enforces source-ID/lineage consistency, real publication clocks, a 300-second feature-age limit, a 75-second capture timeout and a 768 MiB child-memory cap. Process supervision handles crashes and duplicate launches, with bounded restart attempts.

The audit found 299 source identity mismatches beyond 59 old clock exclusions. The upstream dedup/upsert needs a versioned provenance repair; it was not silently rewritten. Numeric consensus and surprise inputs remain absent in the retained snapshot.

[Completion](../../currency_meter_continuous_20260912/COMPLETION_RECEIPT.json) and [dated operational observation](../../currency_meter_continuous_20260912/review/OPERATIONAL_OBSERVATION.json). At 04:11:48 UTC, two capture attempts were refused, zero production captures published, and heartbeat/diagnostic/clock-error counts were zero. No broker worker, numerical model, trading policy or old trial was changed. Successful live collection, clock-monitor restoration, official surprise inputs and separate sign-in recovery remain open.
