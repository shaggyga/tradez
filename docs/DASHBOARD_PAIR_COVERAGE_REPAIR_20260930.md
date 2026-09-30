# Dashboard pair coverage repair — September 30

Source `de616d2aee183485ffd2d5f84ca848723aa9af60`. Same-task review; independent review not claimed.

Research health now depends on the current selected producer and quote stream.
Account freshness remains separate and grants no execution authority. The activity
panel derives the actual producer version, including V11, instead of always V7.

Under Bot activity, expand **Pair coverage**. All 68 pairs have separate quote
freshness, technical and peer support, State-space, Ridge price, and joint price/news
statuses. Current family refusals and joint mature-row/context/pattern counts are
visible. Having a price forecast does not imply complete features or a joint forecast.
The display reuses the read-only availability report, checks source/configuration
bindings and exact population, and expires old observations. Unknown is not zero.

Validation: 49 focused tests, full JavaScript syntax, and the exact served renderer
with actual HTTP data passed. At 17:27:58 UTC, 68 rows were present, 65 pairs had
price forecasts, joint forecasts were zero with no worker errors, and technical
coverage was 56 complete / 9 partial / 3 stale. No browser was connected, so visual
browser inspection was unavailable.

Real data limitations remain: non-tradeable TRY quotes, intermittent stale quotes,
insufficient continuous history and undefined features. Retained gap receipts show
both provider-omitted requested minutes and unknown gaps; existing bounded recovery
already retried some gaps. Omission does not prove permanent absence. No synthetic
bars, imputation, numerical changes, model gate changes, collector restarts, new
cohort, account/order actions or other-chat capture changes occurred. Only the
existing dashboard task restarted; runtime profile and native workers are unchanged.

Next operational item: `joint_v11_mature_forecast_verification_v1`. Inspect actual
support and native forecasts/outcomes without repeating this repair or resetting
the cohort. If `cached_input_expired_before_issue` repeatedly affects price families,
quantify original input-to-issue latency before a separately reviewed producer fix.
Scientific queue remains `extra_trees_matched_development_comparison_v1`. Historical
preflight pointer coverage/schema findings are not cleared by a display repair.

Local evidence: `evidence/dashboard_coverage_repair_20260930`. Shared packet:
`DASHBOARD_PAIR_COVERAGE_REPAIR_20260930`. Tracking trends and historical model
inventory remain separate follow-ups; this adds current coverage and support counts.
