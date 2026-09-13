# Current news evidence and causal aggregation repair

The frozen observation was captured at21:27:57 UTC, from a producer cutoff of21:25:26.984 and publication21:27:18.846. It contained18 active topics. Only one supplied forward direction: the same indirect topic appeared in all50 directional pair rows (25Long,25Short);18 pairs had no current direction. None of the18 active topics was source-verified. Thirteen had exceeded their configured reaction horizon and ten described previous market movement; these counts overlap.

The collector was functioning:176 of179 enabled sources were reported healthy, with three current degraded sources. GDELT was respecting its configured6-hour429 backoff; HKMA's API timed out while the separate official HTML fallback remained healthy. Statistics Sweden was serving valid preliminary CPI content without the registered final CPIF paragraph. Historical disabled, unsupported and credential-missing inventory errors were not counted as active faults.

## Five concrete examples

| Captured article/context | What the pipeline actually did | Audit finding |
| --- | --- | --- |
| Energy Intelligence: “Blockade Pushes Iran Output Onto Domestic Market”; CNN: “Trump’s blockade pushes Iran toward military escalation” | Grouped the two publisher headlines under one broad escalation claim and projected a global currency basket into50 pairs. | Different publisher names, not duplicate polling. But CNN arrived56m02s after its own publication; clustering borrowed Energy Intelligence's earlier2m07s arrival lag and converted the combination into forward evidence. The articles also describe different consequences of the blockade. |
| AUDUSD technical analysis describing a four-month price high and copper strength | Assigned monetary-policy research scores of+0.45 to bothAUD andUSD, with the prior-market-move flag false. | A weak underlying category/recap interpretation. The published forward map was empty, so this error was contained by the context/corroboration and age guards. |
| FxWirePro USD/ZAR “edges higher as rand weakens” | Retained research scores USD−0.7 andZAR+0.7. | Those research signs oppose the described pair move. Its published direction remained empty because it was correctly identified as a market recap. |
| Al Jazeera oil-price-surge headline, about8minutes old at the cutoff | Preserved the article as recent commodity context and cleared forward scores. | News was present; the system deliberately withheld a headline reporting price movement already underway. This is different from a feed outage. |
| Deutsche Bank expectations for later ECB rate increases | Preserved EUR research direction but withheld a forward signal as uncorroborated secondary macro context; its reaction horizon had also elapsed. | A bank's expectation is not an observed ECB policy action. No assertion about the truth of the underlying external report is made here. |

All claims above describe retained local evidence and software behavior. The independent public SCB request is separately hashed; headline claims were not treated as verified current macroeconomic facts.

The old label`initial_reaction` meant “within a configured category horizon,” including a6-hour horizon. It did not mean the event was newly occurring: the sole qualifying topic was already roughly4h51 old by its earliest local observation. The50 pair directions were dependent projections of one topic, not50 independent signals or measured prediction successes.

## Implemented versioned repair

The new v165 producer calls a pure member-admission guard. Each contributing article needs its own publication, original observation, any later content/consumer availability, timeliness, original expiry and provenance. Delayed mapping or import cannot renew expiry. Exact syndicated headlines, category wrappers and connected publisher syndication groups count once. Secondary support must describe the same claim; a broad category alone is insufficient. Direction becomes available only when the contributing evidence is available, and expires at the earliest original member deadline. Filtered members cannot contribute scores, direct-currency attribution or confidence support.

Existing unsealed topics remain reviewable context. The new`joint_news_current_v1.json` artifact carries current guarded topics once, a fresh aggregation cutoff, actual publication clock, exact source hashes and replayable member proofs. It is bounded to128 current topics and8MiB; missing or conflicting proof yields unavailable rather than false neutral. The producer recomputes pair eligibility at an attested post-input-read cutoff, instead of merely changing an old result's timestamp.

SCB's explicit CPI-only flash layout now produces a typed`awaiting_regular_cpif_release` observation and zero numeric rows. The parser retains the original final CPI/CPIF path, does not mix preliminary August CPI with July CPIF, and does not invent a final release timestamp. Unknown layouts still fail. The captured page scheduled the regular August publication for September14.

Final validation and source hashes are in`NEWS_CAUSAL_AGGREGATION_VALIDATION_20260907.json`;573 tests passed. The same captured two-member failure is now context-only, and the raw first-seen clock is preserved. Deployment and live artifact verification are pending the root agent's controlled collector reload; this staged record does not claim a runtime restart or improved forecast accuracy.

The original v164 snapshots, raw evidence and registered forecast/outcome ledgers remain preserved. The current snapshot lists serialize500 representative articles/topics although their metadata reports3105 total topics; the audit's94 recent exported rows are not a full-database24-hour article count.
