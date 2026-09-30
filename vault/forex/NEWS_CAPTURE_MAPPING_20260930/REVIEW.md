# Current news capture and forecasting observations

The existing typed-context worker now runs a separate bounded headline feed lane and records current news, technical inputs and saved forecasts together. The broad collector, original archives, trained-model cohorts and other chat's EUR/USD capture are unchanged. This is an engineering deployment, not a new forecast-performance result.

## Fast headline path

`trad/oanda_news_fast_context_v1.py` selects twelve existing RSS feeds from the preserved source configuration: Fed policy/speeches, ECB press, BoE news/speeches, BoC press, RBA media/speeches, BoJ updates, SNB press and two existing Google FX searches. It reuses the pinned RSS parser. The source's existing 60- or 180-second cadence is retained; at most four requests are in flight. Requests have an eight-second socket timeout and two-MiB response bound, conditional HTTP validators, persisted retry-after/backoff and a 128-MiB separate database cap. A slow source does not hold completed responses from other sources. This lane reads headlines only: no detail-page enrichment, paid provider or GPT call. Crypto headlines are excluded from this new store.

The separate `fast_headlines.sqlite` stores exact selected text, original publication time, actual response-completion observation, feed payload hash and its own observation contract. Repeats cannot move first_seen; changed text gets a new record. Unknown, future and older-than-24-hour publication times are withheld. Feed hashes are provenance references, not a claim that complete feed bodies/full articles were archived. The original broad archive supplies that separate collection path.

The context publisher combines the bounded original-archive selection with this new store and still deduplicates semantic interpretations. It exposes source provenance and separate broad/fast health. These observations do not acquire the old collector's causal certification or become joint-model inputs automatically. The native worker polls its publication loop every fifteen seconds, so request completion can precede context publication. Provider discovery/publication delays remain outside this change.

## Current mapping

`trad/oanda_current_news_mapping_v1.py` reuses the source-qualified current price/joint dashboard reader and the existing as-of technical selector. Each minute it stores actual saved forecast identities, model family/cohort/registry, original issue/publication/consumption/target clocks and this monitor's first observation. It retains the technical values/hash and publication time available at the new decision. Explicit text currencies determine pair links; unidentified global context is not assigned synthetic currency directions. Missing technical support and absent forecasts stay explicit.

The ledger is `trad/data/oanda_training_manager/currency_news_context_v1/mapping.sqlite`. Mapping records are keyed by interpretation and pair; repeated polling does not create a new event. Forecasts are counted by unique forecast hash; shared targets, stories and pairs are still statistically dependent. At most 256 new mappings/settlements are processed per cycle, database capacity is bounded at 256 MiB, source SQL has an eight-second progress bound, and displayed score summaries use at most the latest 10,000 terminal forecast records. No evidence is automatically deleted.

Two outcome types remain distinct:

- Event windows reuse the gap-free 5/15/60-minute bid/ask close diagnostic, starting at the first completed minute close after this monitor's decision. These are descriptive proxies, not fills or proof that news caused the move.
- Forecast diagnostics compare the original reference midpoint with the first completed minute close at/after its H1 target. Fractional targets are supported; the less-than-60-second endpoint delay is stored explicitly. This is a delayed close proxy, not the original producer's settlement or trading P&L. Original-data revisions are refused. Pending targets/missing bars cannot count as settled observations.

Old headlines first interpreted now are observed now. Nothing is backdated into a historical prediction. The new typed context has not been fitted into the joint forecasting model, so these conjunction records cannot demonstrate its incremental forecasting value.

## Verification and limits

- 124 broad staged tests passed; 33 overlapping final focused tests passed after substantive review. The final tests cover actual-consumer joins, late/future information, fractional forecast targets, revised data, repeat clocks, source failure/backoff, bounded requests, source pins and crypto exclusion.
- The real twelve-feed smoke cycle completed in 2.906 seconds and retained 78 recent records before the final crypto filter. This compares request execution, not validated end-to-end arrival improvement against the approximately ten-minute broad cycle. The final native store uses the filter.
- Real current-root smoke: 130 actual saved forecasts, 100 news/pair mappings, 19 technical snapshots. Native readback subsequently recorded 296 mappings and 43 snapshots with 130 forecasts; counts are dated observations, not constants. Native/HTTP final receipts govern the final deployed state.
- Current model readback: 65/68 pairs publishing price forecasts; joint V11 had 3–4 mature H1 rows per supported family against 48 required, and insufficient nonzero/varied context. Those are captured quarter-hour origins with H1 outcomes, not necessarily one row per hour. No thresholds were weakened or old history imported.
- Final review caught and fixed exact-minute lookup of fractional live forecast targets. Startup readback briefly saw the old context and failed stale/source checks until native adoption; later readback passed. No browser visual QA or independent review is claimed.

## Operation, handoff and remaining work

The existing eighteen-role supervisor profile enables these additions through the context worker's `--fast-config` and `--enable-live-mapping` arguments. The original recovery expiry remains `2026-10-07T08:14:50Z`. Source/display/config pins were updated together. The dashboard shows observed mapping/forecast counts and explicitly calls settled endpoints proxies. Runtime source drift and stale publications remain withheld.

Local evidence is `evidence/news_capture_mapping_20260930/`; shared review is Vault `NEWS_CAPTURE_MAPPING_20260930`. Actual commands, failures and deployment before-images are retained. Git carries runnable source and the dated Vault knowledge snapshot. Restore the source/config/display files as a unit, stop only the exact context/controller/dashboard processes, validate the profile and restart existing tasks; preserve both new databases and original archives. Do not copy this machine's activated runtime into another replica without its own validation.

Next operational action: verify additional native captures and mature outcome settlement when targets arrive, then qualify prospective typed-context feature inputs with exact matched controls. Joint V11 still needs real mature/varied history. The design queue's Extra Trees comparison remains a partial source draft. Readiness diagnosis found one original manifest navigation error causing cascading preflight failures; old pointers also do not describe the current document/source inventory. All 556 original engineering source files still match; the only added engineering-root file is the preserved Extra Trees draft. Repair the current-document/source/coordination handoff before new scientific fitting, preserving original seals and the current queue. No scientific gate has been marked passed by this deployment.
