# Causal technical campaign-input contract

This is real input preparation, not a fitted model or policy result. It reuses the
byte-identical direction_features_v1.py and endpoint_targets.py implementations
already retained on C. Original technical, contiguous-path and exact-endpoint
calculations stay separate and unchanged. No live trad source or service changes.

Authenticated26.2MB raw slices cover July1-August2,2024 from all68 source members.
The original1.59GB archive and each member hash were verified before slicing.
Only epoch, midpoint close and bid/ask closes are carried. All76 fixed six-hour
origins July8 00:01 through July26 18:01 remain for all68 pairs:5168rows,4026feature
ready and1142missing exact reference closes. The vector is24 retained technical
fields plus2 current entry costs in bps. No pair one-hots, peer or news inputs.
The saved94/178-column model weights cannot silently consume these26fields.

Features depend only on current/past bars. No future endpoint selects an origin
or its feature availability. Missing historical windows retain explicit indicators.
Duplicate minute stamps are ambiguous and excluded. Invalid midpoint rows break
continuity; invalid bid/ask cannot supply current cost features. Arrival remains
assumed minute-close, not observed. Preserved pip definitions are stationary
research metadata, not a claim of contemporaneous2024 venue specifications.

Seven EXACT-ENDPOINT elapsed gross-midpoint targets are15m,1h,4h,12h,24h,2days,
5days. Each has separate values/readiness or a missing-endpoint record for every
origin. These reuse endpoint_outcomes and do not require uninterrupted paths.
Five separate CONTIGUOUS-PATH diagnostics15m through24h reuse build_labels. The
first preparation found zero full24h paths, so that failure and original inputs
remain preserved. The v2 contract adds explicit endpoint views, extends the past
training window to supply mature5day labels, and does not relabel paths as valid.
There are36176 endpoint rows and15504 explicit calendar-blocker rows. Daily close,
2-session and5-session targets stay blocked by unqualified venue calendar metadata.
Elapsed24h/2day/5day targets never become exchange-session targets by naming.

Outcome readiness is the target bar close. Candidate fitting cutoffs July22/24
have at least795 eligible rows per target (support varies). Seven-day warmup
precedes emitted origins. Session age means contiguous retained-M1-run age, not
venue calendar age. Source slices include later outcomes; the feature function
never consumes them when computing earlier features. Future perturbation and
endpoint-deletion tests check this separation.

The portable checkpoint includes every raw slice, original numerical source,
adapter/runner, frozen recipe and exact expected hashes. Restore reruns all68
instruments and matches all70 payloads. The bulk archive is referenced, not copied.
No environment binaries, credentials, live state or cloud-sync claim. Frozen
status/run/resume/verify checks source/input/environment before import. One writer
publishes pairs durably and completion last. Drift, partial corruption and missing
payloads refuse; actual process death/resume and writer exclusion are tested.

Next: matched_ridge_recovered_hgb_development_campaign_v2. Freeze model parameters,
update clocks and scoring before fitting pooled ridge and corrected HGB on these
same26fields and target-specific matched maturity populations. Add no-change and
past-history controls. Calendar and executable-policy qualification remain open;
input preparation alone is not a campaign. Independent review is unperformed.
GPT/advisor comparisons, paid calls, broker/service/account and D-drive work deferred.
