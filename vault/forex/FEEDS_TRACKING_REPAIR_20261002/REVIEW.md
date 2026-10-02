# Feed and outcome-tracking repair — October 2, 2026

The retained tracker is live again. Its original 2 GiB database remains in place;
new forecasts roll into numbered SQLite segments. The same consumer reads every
segment, preserves first-observation identities, and settles pending records in
their original segment. No old forecast, news link, model or candle was deleted.

The existing news worker and dashboard were restarted with a coherent tracker
source binding. No separate worker, scheduler, trading route or model was added.
Other-task EUR/USD capture was excluded.

## Simplifications and limits

- Incrementally maintained group totals replace repeated whole-history scans.
  SQLite triggers update counts and sums in the same transaction as observations
  and settlements. Original outcome definitions are unchanged.
- One SQLite writer lease serializes rollover and duplicate checks. A stored
  segment count detects missing history; interrupted empty successors recover.
- Settlement shares a bounded batch between old backlog and recently due records.
  Current outcomes no longer wait for the entire outage backlog to clear.
- Each segment accepts new records until 2 GiB; 512 MiB additional space is reserved
  for settlement. Eight segments and a 2 GiB free-disk reserve bound consumption.
  Capacity refusal remains explicit; this is not unlimited retention. Before these
  limits are reached, an evidence-preserving cold-storage successor is required.
- The management inspection consumer resolves first anchors across all segments.
  Always use `tracking.open_store(path, readonly=True)` and `all_forecasts` for
  cross-segment inspection. Reading only `tracking.sqlite` omits newer observations.

## Actual verification

111 distinct tests passed, covering tracking, rollover/restart/refusal, original
projection/residual/curve consumers, current dashboard binding and management
inspection. An earlier 21-test tracking suite also passed in a relocated capsule.
Tests and substantive same-task review are separate from independent review,
which was not performed.

All **683,224** original forecasts retained identical IDs, bodies, first-observed
times and identity columns after migration. All original news links were preserved.
A consistent pre-migration backup and its SHA-256 are recorded locally.

The first direct collection took 5.19 seconds, captured 2,398 new observations and
checked 4,096 outcomes. The subsequent live dashboard and news-worker readback
agreed on the deployed source. At 02:55 UTC the successor segment already had
16 settled new forecasts. The old pending backlog remains catch-up work; it is
not represented as completed or retroactively observed during the outage.

At **02:58:31 UTC**, 65/68 quotes were under 60 seconds old and 64/68 pairs had
current retained forecasts (2,456 outputs). This is a dated observation, not a
permanent count or an improvement attributable to tracker changes.

## Remaining feed limitations

EUR/TRY, TRY/JPY and USD/TRY were reported not tradeable. USD/HKD lacked a fresh
eligible M1 observation despite having a current quote. Other pairs can temporarily
have stale quotes. These are distinct from a dead collector.

All 65 active feature sets were partial: missing elapsed-minute history prevents
some windows, and zero-range quantities can be mathematically undefined. Existing
supported indicators and forecasts continue. No bars were synthesized, no missing
features were filled, and no freshness limit was relaxed. Retrieval of genuine
eligible M1 history is the next feed-recovery item; new broker requests or changes
to another task's recorder were not performed.

## Handoff and rollback

Local evidence: `evidence/feeds_tracking_repair_20261002/` (WORK_LOG.jsonl,
PENDING_CHANGES.md, BACKUP.json, PRESERVATION.json, tests, deployed readbacks and
portable capsule). Shared packet: `FEEDS_TRACKING_REPAIR_20261002` in the live Vault.
Runtime tracker: `trad/oanda_retained_forecast_tracking_v1.py`; no-order scope remains.

Never replace the live database with the baseline backup: that would lose subsequent
observations. A rollback must first preserve **all** current segments and stop the
scoped writer; the old implementation cannot serve multi-segment current history.
Prefer a forward repair and keep the preserved baseline for comparison only.

Next feed item: `rolling_m1_elapsed_support_recovery_v1`. Economic/conditional
position-management qualification remains a separate unfinished item. No new
forecast experiment, profitable strategy claim or order authorization is implied.
