# WP6/7 event-existence forecast layer: fixed negative development result

Six unique pooled ridge models were fitted under the predeclared plan: technical
(26 fields), technical+clock (30), and technical+clock+event coverage (42), each
at1h and24h. The original technical, endpoint, TrainingView and ridge consumers
are reused. All arms have exactly the same mature training rows:2941 at1h and
1544 at24h, spanning48 and25 hourly origins respectively. No parameter/seed
search or adaptive refit. Zero/history-mean controls remain alongside all arms.

The event layer worsened matched MAE versus technical+clock:

| Elapsed target | Matched rows / origins | Clock MAE | Clock+event MAE | Difference |
|---|---:|---:|---:|---:|
| 1h | 2280 /36 | 5.81154bps | 6.30829bps | +0.49675bps |
| 24h | 832 /13 | 35.09419bps | 37.62432bps | +2.53014bps |

All23,930 modeled forecast records and32,640 coverage rows remain. Missing
features8030 and model-not-ready680 cases are explicit. All68 pairs,10 score
groups, per-day and origin-balanced diagnostics are preserved.24h evaluation
support comes from only one UTC day;1h support spans two inspected days.
The result is negative for this fixed event-count/age representation, not a
reason to retire macro/text families or tune a winner on these outcomes.

14 local and14 relocated tests passed, including actual future price/source
perturbation, unchanged fits under future feature/label corruption, forged label
clock refusal, train-only normalization, exact shared support and original
issuance without future endpoints. Process death after payloads1/7 preserves
all ten payloads, including exact models and forecasts, on resume/relocation.
The original operator took19.187s on this host; timing is not a live benchmark.

This is previously inspected development data with hypothetical source-asof30s,
fit30s, forecast2s and bar-end clocks. Historical extraction/issuance, independent
confirmation, calendar qualification and live admission remain unproven. No
policy, trading or protected-confirmation claim. GPT/advisor comparisons and
paid/broker/service/account/D-drive actions remain deferred.


Checkpoint: checkpoint/forex_macro_event_forecast.zip, SHA256 f1fcef5dad5d7eb55f65ffa311660cb332930b37c780298cf081be7de44e421b. Exact next: review_macro_event_forecast_checkpoint_v2; then macro_text_long_body_coverage_audit_v2. Prior checkpoints remain sealed in their original packages; approvals reference them without duplicating archives.
