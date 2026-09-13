# V2 ledger endpoint reliability, 9 September 2026

The read-only probe ran from 10:45:04 to 11:05:04 UTC. It made 77 ledger polls and two served-HTML checks, retaining 27,094,227 private response bytes. All bound sources and both served HTML hashes stayed unchanged. HTTP/projection errors: 0; unavailable API samples: 0.

The polls contained 67 distinct original reports. First-pass failures occurred in 16 reports, totaling 19 pair observations. There were 19 fresh retry attempts, 19 recovered ledger observations and 19 recovered forecast observations. Final failures occurred in 0 reports (0 pair observations). First failure counts by pair: `{"AUD_CAD": 2, "AUD_CHF": 1, "AUD_HKD": 2, "CAD_CHF": 1, "CHF_JPY": 1, "EUR_NOK": 1, "EUR_NZD": 1, "EUR_PLN": 1, "GBP_CHF": 1, "GBP_PLN": 1, "GBP_USD": 2, "NZD_HKD": 1, "USD_CNH": 1, "USD_MXN": 1, "USD_PLN": 1, "USD_SEK": 1}`.

Final verified-pair counts by distinct report: `{"68": 67}`. Current forecast counts by all polls: `{"66": 77}`. Unvisited counts by distinct report: `{"0": 67}`. No-publication observations by pair: `{"TRY_JPY": 67, "USD_TRY": 67}`; these are separate from transient observation failures.

Original first reason counts: `{"observer_pair_time_budget": 19}`. Of the first failures, 19 have actual expired monotonic budgets and 0 have failed clock integrity. SQLite primary-code counts: `{"None": 19}`. Exact first and retry clocks remain in the JSON. This does not diagnose CPU contention or database locks beyond the observed codes.

HTTP latency was 0.203–2.890 seconds (median 0.547). Original producer statuses by distinct report: `{"coherent_envelope_only": 42, "generation_mismatch": 25}`; reported error-counter observations: `{"13": 67}`. Original producer generation mismatch remains distinct from ledger publication acceptance.

The earlier V1 window had 77 polls / 66 distinct reports, with 40 pair failures in 26 reports and 63–66 current forecasts. These are different dated windows with uncontrolled machine/cache/browser load; two existing refreshed tabs may poll concurrently during V2. Direct retry outcomes are observable here, but the before/after figures are not a controlled gain estimate. Repeated pair observations are not independent trials, and no trading-performance claim follows.

Assessment SHA-256: `698f1c9a0fa57cce8366a67109d6e7663fbfa28f75cfc256d4d0d9960655458f`. Exact responses stay private; compact counts, original clock evidence and source-bound receipts are suitable for curation. No broker, account or news endpoint, source/runtime edit, forecast issue or order was performed by this probe.
