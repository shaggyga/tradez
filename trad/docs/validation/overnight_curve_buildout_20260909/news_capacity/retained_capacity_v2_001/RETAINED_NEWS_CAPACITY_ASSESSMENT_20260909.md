# Retained news capacity, 9 September 2026

Three newest retained captures were read at 2026-09-09T10:49:38.213794+00:00–2026-09-09T10:49:43.329566+00:00. All three canonical payload seals, complete-history row counts, retained producer payload hashes, and 20 registered source bindings passed. This was a file-only capacity audit: no database transaction, new news collection, semantic feature replay, runtime change or forecast scoring.

The newest capture was originally observed at 2026-09-09T10:49:21.946222+00:00. It uses **14,267,932 bytes (85.044%) of the 16 MiB shared canonical limit**, leaving **2,509,284 bytes (2.393 MiB)**. The three captures range from 14,247,990 to 14,267,932 bytes. The nearest measured bound is the entire shared logical payload, not its compressed file or history row limit.

| Active component limit | Observed latest | Limit | Remaining |
| --- | ---: | ---: | ---: |
| shared logical bytes | 14,267,932 | 16,777,216 | 2,509,284 |
| shared compressed bytes | 2,587,223 | 8,388,608 | 5,801,385 |
| history rows | 5,905 | 10,000 | 4,095 |
| history raw wrapper bytes | 59,624,777 | 134,217,728 | 74,592,951 |
| history read seconds | 5.578 | 20 | 14.422 |
| current producer evidence rows | 282 | 4,096 | 3,814 |
| current producer raw payload bytes | 2,924,446 | 33,554,432 | 30,629,986 |
| current producer snapshot bytes | 3,592,966 | 16,777,216 | 13,184,250 |

Byte quantities above are exact bytes; duration is seconds. Raw historical-wrapper bytes and read duration are original ingestion diagnostics retained in the sealed capture, not a fresh read of those omitted original wrappers. The 48-hour history was complete with 5,905 unique mapping rows and no truncation. Current-producer evidence is a separate selected 24-hour population of 282 rows. Its nested snapshot reported 35 context topics and zero directional topics; these broad topic counts are not the recent-entry-member counts from the separate current-news join audit.

The total logical capture comprises 10,551,826 history bytes, 3,592,966 current-snapshot bytes, 119,451 current-member bytes, and 3,689 bytes of other canonical framing/metadata. The history-only 16 MiB early check does not reserve room for the current snapshot: the final whole-capture check remains decisive. The single historical-wrapper 1 MiB limit was not independently measured here because full wrappers are omitted from retained history.

These captures are below every measured ceiling. The remaining 14.956% whole-capture headroom warrants capacity planning; this small dated sample cannot establish a growth rate or failure deadline. A rolling 48-hour history can shrink or grow, and current proof size can change independently. Successful retained captures alone do not establish that every attempted build succeeded.

## Versioned successor scope

The inactive `oanda_news_capture_storage_v1.py` candidate already demonstrates lossless content-addressed sharing. Its three-capture benchmark reduced stored bytes by 60.8%, but complete verified reads remained slower (2.66–2.75 seconds versus 1.59–1.66 seconds for whole gzip). Its proposed 64 MiB logical envelope has not been memory-validated at that ceiling. The measured 14.2 MB case reached about 172 MB process working set. Neither disk compression nor deduplication changes the active adapter's 16 MiB replay contract.

The smallest concrete capacity successor is a separately bound input adapter using that existing immutable descriptor and shared validated chunks, with a new ledger dispatch/worker cohort preserving old provenance. It must retain complete ordered 48-hour mappings, original availability clocks, exact source hashes, current-proof replay, and failure on missing/conflicting chunks. Bounded shared reuse across pairs is required; loading and reconstructing the full history independently for 68 pairs would reintroduce the measured cadence problem.

Adopt no larger logical limit merely by changing a constant. First measure the proposed maximum-sized envelope and concurrent readers for peak memory, complete ingestion/replay time and source freshness, then freeze an explicitly budgeted new limit. The candidate's unchanged 10,000-row and existing 128 MiB raw-ingestion limits remain separate constraints. No classification/numerical-policy change or governance-history rewrite is required for a lossless storage representation alone; the changed source/storage contract still requires new cohort bindings. This report proposes no immediate activation.

## Source evidence

- Compact JSON: `RETAINED_NEWS_CAPACITY_AUDIT_20260909.json`, SHA-256 `24ffa48ee3c80b416dadcd2ace3b0cf62dc0f7268c7362c9e8a084a820188862`.
- Active input adapter: `trad/oanda_causal_forecast_inputs_joint_news_v2.py`, constants lines 38–50, full history bounds lines 146–194, whole gzip limits lines 196–225.
- Active producer: `trad/oanda_local_news_sentiment_repair_v1.py`, independent current-producer limits lines 40–44 and source evidence checks lines 154–179.
- Existing inactive storage evidence: `news_capacity/NEWS_CAPTURE_STORAGE_REVIEW_20260909.md` and its linked source-bound tests/benchmarks. Original captures and news payloads remain machine-local and are not export members.
