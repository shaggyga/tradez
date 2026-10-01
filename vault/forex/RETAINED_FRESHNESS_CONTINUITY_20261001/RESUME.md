# Retained forecast freshness continuity

Checkpoint `RETAINED_FRESHNESS_CONTINUITY_20261001`; step `retained_input_freshness_continuity_v1`.

The context worker now refreshes the existing saved models when a new technical publication is observed. Previously it waited a full minute after finishing inference, drifting against the producer and sometimes consuming the previous publication. The original 15-second loop, 60-second recovery fallback, freshness refusals, models and numerical inputs remain unchanged. A validated generation hint only schedules work; the existing authenticated input reader remains authoritative. Tracking runs after each refresh. Optional inference failure still allows headline capture, and retained-only context output is now sealed correctly.

## Evidence actually executed

- Baseline: 18 authenticated immutable issues from the current registry, giving 17 completed intervals. Seven intervals exhausted every forecast before replacement; maximum gap 26.323 seconds. Historical deployment downtime was retained separately and excluded from this comparison.
- After the scoped reload: 360 seconds, 63 samples, seven publications and six completed issuance intervals. Zero empty-live samples, zero exact inter-issue expiry gaps, zero read errors. At least 61 pairs had current outputs in every sample; coverage varied with original source availability and refusals.
- Steady publication-to-issue latency was 9.065–27.524 seconds. Startup consumed an older valid publication with 67.685 seconds latency; it remains included in the evidence. This observation does not prove uninterrupted future availability.
- Six actual dashboard HTTP reads returned current forecasts, current news and current tracking with the same 23 registry groups. Requests took 4.329–9.547 seconds. This is API verification, not visual browser QA.
- 82 Python tests passed in the newly executed focused suite: scheduling, actual context loop, unavailable hints, inference failure, generation changes during inference, payload sealing, news, saved consumers and tracking. No new experiment or fit was performed.

Source/config pins were revalidated before reloading only the existing owned context, pipeline controllers and dashboard. Original technical producers and the other task's EUR/USD recorder were left running. Existing recovery expiry (2026-10-07T08:14:50Z) and retry history remain unchanged.

## Review and limitations

Substantive same-task review accepted this scheduling repair. Independent review was not performed. Evidence authenticates each original issue body and checks exact intervals as well as sparse samples. The registry remains `0c56a0ecded544cbd2f282433e7ba31bf0c82ad8bb6a0334b5247c54929f06f1`: 23 connections across 11 elapsed horizons. No forecast quality improvement, global best-model claim, new trading readiness or order authorization follows from improved freshness.

The saved learned residual, full same-base curve inputs, 16 unqualified exact targets, news incremental predictive value and position management remain separate unfinished work. Earlier negative and inconclusive scientific results remain intact.

## Reproduce and recover

Use the source commit in the live Vault packet's REVIEW.json and the qualified Python environment in START_HERE.md. Run:

```powershell
python -B -m pytest -p no:cacheprovider tests/test_retained_refresh_schedule.py trad/test_oanda_currency_news_context_v1.py trad/test_news_fast_context_v1.py tests/test_retained_forecast_connection.py tests/test_retained_forecast_tracking.py -q
```

`measure.py` in the packet documents bounded read-only observation and authenticated issue inspection. `finish_evidence.py` verifies the recorded native interval. These are operational observations, not a historical model replay. Existing model/source artifact restore remains the preceding `RETAINED_PROJECTION_CONNECTION_20261001` packet and capsule SHA `ec85cfbe2813139282fefa998a4bcc01893e84e3ddf0c10991daac660e8db4d5`; no numerical dependency changed here. Reconcile ownership before starting native collectors on another machine.

## Exact next action

`retained_learned_residual_state_connection_v1`: qualify reuse of the preserved learned residual state against the currently connected exact 6h/18h parent models, feature units, maturity and layer contract. Read the accepted residual repair and relocated replay packets first; authenticate layer ID, training membership, contract, parent identities and original evidence. Retrieve missing bytes from the Vault. Call the existing apply path only if compatible; do not call fit_snapshot or refit base models. An unavailable or incompatible saved state is a precise integration blocker, not evidence that every model is already connected. Preserve the current working parent and fixed-layer connections. Complete meaningful refusal/replay tests, native readback if deployment qualifies, and a scoped review/checkpoint before any acceptance claim.

Current coordination is in the live shared Vault. Git vault/forex is a dated knowledge snapshot. Local raw evidence is `evidence/retained_freshness_20261001`; the compact Vault packet carries matching source and evidence identities. Older next-item notices below current handoff entries are historical.
