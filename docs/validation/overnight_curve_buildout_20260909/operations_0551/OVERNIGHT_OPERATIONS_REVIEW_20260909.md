# Current overnight operational review

The research program was advancing in two bounded observations at **05:56:22–23 UTC** and **05:58:11–12 UTC, September 9**. It remains research only, with material input-age and publication-coherence limits. No HTTP/broker requests, database scans, source edits, process changes or order actions were made by this audit.

The source-bound evidence is `OVERNIGHT_OPERATIONS_SUMMARY_20260909.json` (SHA-256 `5bdd7a8d0c8ce1ce67bddc3f728b50a0b3de00c13a4dc64c85f2a7e461e72459`), linked to both larger compact observations and their original collector-source hashes. All **20/20 joint-v3** and **14/14 pilot** registered source bindings matched at both observations. The separate archive-report supplement retains its own later report/read clock.

## What was actually running

The supervisor reported **17/17 required research workers current** at both observations, and every reported PID was independently present in the OS inventory. The standalone recovered-curve pilot **PID26012** was also present, created at 05:33:54 UTC. Its 60-second cycle records were advancing, with no skipped schedule slots. Its frozen issue cutoff is **08:55 UTC**, with collection scheduled through **13:00 UTC**; this snapshot does not guarantee future uninterrupted operation.

The practice fast executor and proof-shadow predictors were explicitly inactive under `research_collection_only`. Both cohort registries retained their inert controls. The locally exposed account observation was about 3 seconds old, with current position/order state, **0 open trades and 0 pending orders**. No account identifiers, credentials, NAV or trade payloads are retained.

The integrity artifact initially was 269 seconds old, beyond its 180-second publication-age allowance. It then refreshed to **47 seconds old**. That later report had **no active/shared/unknown failure rows** and **17 failures attached to explicitly inactive components**. Those old evidence checks remain failed; their retirement does not make them passed. Process presence and this scoped report do not prove model quality or full program correctness.

## Joint forecasts, missing inputs and the publication reader

The initial bracketed summary/heartbeat read was correctly unavailable: the summary was stable across both reads, but its sealed hash `f2c168cd…` differed from the heartbeat's bound summary `c3c2aeb7…`. Both source clocks were recent. The later read was coherent, with exact matching hash `8ff7dc0b…` and **65 still-unelapsed original H1 forecasts**. This records an actual publication-generation mismatch and later recovery; it does not establish the exact duration or sole mechanism of the failure. The worker's independently scheduled status publication is visible at `oanda_joint_price_news_forecast_study_v3.py:507` and `:514`. No binding check was weakened and no dashboard work was performed.

Only **EUR_TRY, TRY_JPY and USD_TRY** lacked forecasts in the coherent observation. Their dedicated quote rows were `tradeable:false`, with market timestamps around September 8 at 14:59:55 UTC, almost 15 hours old. These are genuine no-current-tradeable-quote refusals, consistent with the worker's `market_closed_or_no_stream_quote` reason. A fresh wrapper or an old archived candle must not make them eligible.

The worker's retained counters advanced **2048→2056 publications/consumptions**, **1956→1963 entries**, and **1666→1669 H1 outcomes** between the observations. These are worker-verified summary counters, not a fresh independent scan of every ledger and not a profitability assessment. Cumulative errors remained **7** during this audit, versus 6 in the 03:16 review; the last retained reason was `AUD_CHF:ValueError:news_stale_or_future_at_actual_issue`. The heartbeat does not retain the exact occurrence time. Heartbeat-publication errors remained zero.

Freshly issued forecasts and readiness for the next fit are different. **45 pairs with valid existing H1 forecasts** had cached current-input readiness marked stale/unavailable news. The three TRY rows additionally have absent input clocks and a generic subordinate news-readiness reason, while their primary quote reason remains explicit. The latest 60 completed capture-journal entries spanned 780 seconds: **57 ready, 3 TRY price refusals**, median adjacent completion spacing 5.87 seconds. This demonstrates progressing capture work and a refresh backlog; it does not make all 68 current input bundles simultaneously fresh.

The repaired-news producer heartbeat advanced normally (about 42 then 30 seconds old), with cumulative errors unchanged at **3** and an empty last-error field. Those missing historical error reasons cannot be reconstructed from this heartbeat. The old collector progressed from source collection into evidence postprocessing. Separately, the sibling's full guarded snapshot replay at **05:46:18 UTC** validated 22 topics/59 members and zero forward-admitted members against the original seven sources; that is a dated content observation, not a current-content claim for these later heartbeats.

## Short-horizon pilot: actual refusals

Across the **last 10 completed cycles, 05:48:55–05:57:55 UTC**, the pilot issued and consumed **56 of 60 model variants**. Eight cycles issued all six variants; two issued four. Each source capture feeds two midpoint conventions, so those variants are not independent source observations.

| Pair | Issued / attempted variants | Withheld original endpoint span | Source capture |
| --- | ---: | --- | --- |
| EUR/USD | 18 / 20 | 80 seconds, both conventions in one cycle | All present |
| GBP/USD | 20 / 20 | None | All present |
| USD/JPY | 18 / 20 | 85 seconds, both conventions in one cycle | All present |

Every mapping selected **13 actual complete S5 rows**. The EUR/USD failed row span contained a 15-second internal gap; the USD/JPY failed span contained a 25-second gap. The retained provider response lacks some nominal S5 intervals; there is no evidence here that the local capture omitted returned complete rows. The original retained-fit support rule accepts only **55–70 seconds** (`oanda_recovered_second_curve_v1.py:269`). These were time-support refusals before publication, not missing current quote captures, and the other cycles recovered without intervention. No synthetic bars, changed span limits or frozen-worker changes are justified by this observation.

The full observed pilot inventory contained 26 completed cycles, 156 variant attempts, **132 issued/published/consumed curves** and 1,716 native-node candidate observations. Its 24 lifetime observed refusals all used the same original-span gate. Candidate observations are not orders, fills or successful predictions; outcome evaluation remains the separate pilot evaluation task.

## Archive M1 lag and bounded backlog

Current tradeable quote counts were **63 then 64**, with median quote ages **1.60 and 2.03 seconds**. For those same current-quote pairs, archived M1 close ages were:

| Observation | Minimum | Median | Maximum |
| --- | ---: | ---: | ---: |
| 05:56:22 UTC | 22.8s | 82.8s | 322.8s |
| 05:58:11 UTC | 131.7s | 131.7s | 191.7s |

At the first read, EUR/USD and GBP/USD had reached the 05:56 close while USD/JPY still ended at 05:51. At the follow-up all three ended at 05:56. This is serial archive-update timing, not a disconnected quote stream. The supervisor config uses a **300-second start-to-start cadence** (`oanda_always_on_supervisor.ps1:2073`); the observed 05:55:24–05:56:53 pass took about 88.6 seconds and then scheduled 211.4 seconds of sleep. The updater computes that remainder at `oanda_all68_m1_forward_updater.py:1005`.

The 05:56 archive heartbeat reported 309 appended rows, zero errors/recovered rows and 2,448 unresolved minutes in its explicitly bounded recent-small-interior-gap scope. The separately retained later report, generated **06:02:18 UTC**, reported 347 appended rows, zero errors/recovered rows and **2,121 unresolved minutes**; its 9 gap requests returned no requested minute, with 56 pairs not due for retry and 3 without small gaps in scope. A shrinking rolling count with zero recovered rows must not be called successful repair. This is not a full archive-completeness measure, and silent filling would falsify the source history.

## Safe next steps

Keep the active frozen workers unchanged while their prospective evidence matures. The concrete independent improvements are to retain explicit per-pair span/refusal diagnostics in evaluation, use the separately tested durable sanitized error journal in a future registered producer, and assess a separately versioned archive/companion cadence against measured request and processing budgets. The fast quote stream already provides current prices; a 300-second archive cycle is unsuitable as an interchangeable short-horizon price source. Current-input readiness should stay distinct from valid issued-H1 availability, with true quote absence taking precedence over generic subordinate news labels.

The existing isolated CAS helper remains a capacity proposal with measured slower reads; this audit adds no runtime-adoption or speed claim. The publication-coherence symptom remains logged and dashboard work stays deferred. Nothing in these observations authorizes execution, promotion or a claim that the program is entirely correct.
