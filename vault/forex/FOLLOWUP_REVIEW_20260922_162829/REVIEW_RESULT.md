# WP6 retained event identities and matched endpoint/no-move support

The same outcome-blind 2,696 versions / 2,207 canonical event IDs now have a
matched retained-price population. All68 source CSVs were read without writes;
326,654 selected minute rows preserve exact CSV bytes and full-source hashes.
Source coverage ends September17 and remains uneven. No API/broker calls.

The rules were frozen before selected-cohort outcomes: two anchors (unambiguous
publication and earliest attested source availability), one-hour/24-hour elapsed
endpoints, and fixed descriptive abs(return)<=10bps no-move band. All60,220
mapped event/pair/anchor/horizon rows are retained, including missing targets,
missing references, unresolved anchors, negative/positive moves and no-moves.
All68 pair coverage and42 events without currency tags remain explicit.

Publication-anchor supported rows:1,895 at1h and1,326 at24h; no-move counts1,563
and396. Availability-anchor supported rows:11,463 at1h and10,616 at24h; no-move
counts9,740 and2,513. These are dependent pair rows, not independent releases.
Many source publications have conflicting clocks, calendars or bootstrap flags;
only8 canonical IDs satisfy the strict retained-metadata release-candidate screen.
This is not a complete external release calendar or causal forecasting population.

Original clean/endpoint consumers are reused unchanged. Exact endpoints and
bid/ask close proxies do not establish a continuous path, fill or profit. Timing
uses explicit bar-end assumptions. Full-cohort event resolution is retrospective
and must never enter earlier features; the next step uses versionwise as-of data.

24 local and24 relocated tests passed. All71 payload hashes match the standalone
restore, including all68 pair files. Actual process death after payloads3/38
resumes without changing surviving bytes. The actual operator took7.547s on this
host; capture took25.469s. These are host diagnostics, not live readiness.

The bounded matched-support substep is complete. The parent all-release package
remains partial on external calendar completeness, independent announcement
resolution, extraction/quote receipts and broader support. No models fitted,
forecast gain, market causality, policy change or independent review is claimed.
GPT/advisor comparisons, paid calls and execution/service/account/D-drive work
remain deferred.

Same-implementer source/evidence review accepted within this scope. Independent review unperformed. Exact next: macro_causal_version_text_state_v2.
