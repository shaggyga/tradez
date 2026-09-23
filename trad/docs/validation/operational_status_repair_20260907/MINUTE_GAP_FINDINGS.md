# EUR/USD minute gap audit — 7 September 2026

The observed EUR/USD forecast blocker is an actual gap in the practice broker's returned M1 candle series. No recoverable candle, local parser omission, or concurrent-read failure was found. This audit changed no archive, study, configuration, worker, or forecast record.

## What the retained evidence establishes

- At 18:13:38 UTC (14:13:38 EDT), the coherent local source ended at 18:10 UTC. Its latest 61 calendar-minute slots contained **55 actual candles**, with a **15-candle consecutive suffix** starting at 17:56 UTC.
- The six missing starts were **17:31, 17:34, 17:43, 17:47, 17:53, and 17:55 UTC** (13:31, 13:34, 13:43, 13:47, 13:53, and 13:55 EDT).
- One read-only practice broker request returned 180 complete EUR/USD BAM candles through 18:12 UTC. It omitted all six of those same minutes. Every returned complete timestamp survived the updater's normalization, and every comparable local close matched the broker close exactly.
- The fresh response additionally had 18:11 and 18:12 candles, newer than the captured local archive. That difference is consistent with the updater's five-minute cycle. It does not explain the six interior gaps.
- Both the original EUR/USD study and the new pair-local EUR/USD study recorded `current_common_warmup:1<61` at their 18:00 attempts. Their latest successful 17:30 captures had 1,024 consecutive retained real candles ending at 17:24. The failure diagnostics retain their reason, but do not retain the rejected input bytes; the exact rejected reference minute therefore is not independently proven by those diagnostics. The current retained series has a one-candle suffix at 17:54 because 17:53 is missing, consistent with the 18:00 rejection.
- The latest completed all-68 updater cycle ran 18:10:24–18:11:57 UTC. It appended 248 real candles, reported zero errors, recovered zero interior minutes, and retained 1,950 unresolved small gaps in its bounded scope. Of its 68 pairs, 46 received a broker-omission result, 11 had no small gap in scope, and 11 had no retry due.
- All **957 existing observed recovery receipts** inspected across the represented pairs omitted their requested minute. Their retained response hashes verified. This is evidence about those actual requests, not proof that every unresolved minute is permanently unavailable.
- The source snapshot remained unchanged during the fresh request. All five audited source hashes remained unchanged. No read failure was observed.

## Safe changes supported by the findings

**No candle repair is justified by this evidence.** Keep missing prices missing; do not duplicate neighboring prices, interpolate bars, reissue rejected historical forecasts, or reinterpret a later observation as historically available.

For the existing study, show **“55 of 61 recent minute slots present; 15 consecutive; six broker-omitted minutes”** with an observation time. The existing `1/61` last-attempt value is legitimate, but it describes a consecutive suffix at the earlier attempt, not the number of candles collected all day and not all-68 shared coverage. Clearly distinguishing those values is a reporting repair that preserves the registered model.

To maintain research forecasts through genuine sparse minutes, create a separate model/input version with explicit elapsed-time features, missingness and coverage rules, and sufficient real endpoint prices. Compare it prospectively with the current strict-contiguity model. Do not silently lower the current 61-bar requirement or change its frozen training-label policy. A faster archive refresh can reduce the observed two-minute endpoint lag, but cannot recover candle timestamps the broker omits.

## Evidence

`MINUTE_GAP_AUDIT.json` SHA256: `5d3820cbd04f367fd7ed78eb514b4cd768d19f0ec71a318e7524e1b71ca83937`.

The audit binds a retained source capture, the complete updater-cycle snapshot, and the fresh broker GET response. Broker data was obtained from the practice instrument-candle endpoint only; credentials and account data were not recorded. No trading or forecast-success claim follows from these observations.
