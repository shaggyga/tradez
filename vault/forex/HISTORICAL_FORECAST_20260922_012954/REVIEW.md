# Historical forecast checkpoint

Completed the forecast-only portion of historical_fitted_slice_and_policy_tape_v2.
Ready for review; independent review unperformed. The historical policy portion is
explicitly blocked on native remaining-horizon and executable/scenario input contracts.

The precommitted all-68 development slice completed 9 pooled ridge fits, 12,540
forecasts and 22,848 explicit coverage rows. Missing features account for3,636 rows
and missing origin observations for6,672. Outcomes remain separate:8,316 matured,
4,224 unavailable endpoints. Results are gross-midpoint errors, never executable PnL.

Frozen and adaptive models both lose to no-change and the training-mean control on
matched MAE and RMSE for all three elapsed24h/2d/5d targets. Matched support is only
778/518/783 dependent rows across3/2/3 origin days respectively. This previously
examined development period is not untouched confirmation; no model family is retired.

22 tests passed locally and again after relocation, covering causality, controls,
actual process crashes, no-refit resume and source/input/config/output refusal.
All13 scientific payloads replayed byte-identically through the actual frozen operator.
Fit wall timings are retained, checked against the60-second simulated allowance and
excluded from cross-run deterministic comparison. Resource receipt is in evidence/.

Checkpoints: checkpoint/forex_historical_slice.zip SHA256 e110e3de2d228eaf8a5ba8da56d763181888bee220dd631b2191f2c9f41e9aec; the preserved
combined synthetic base checkpoint is also included as checkpoint/forex_fitted_consumer.zip.
The historical ZIP includes source, frozen recipe/config and derived inputs; it excludes
the1.59GB bulk archive, environments, credentials and live runtime databases.

Restore: use trusted matching historical_checkpoint_v2.py restore --package <ZIP>
--destination <new-empty-directory> --sha256 e110e3de2d228eaf8a5ba8da56d763181888bee220dd631b2191f2c9f41e9aec --run-tests. Then follow the
packaged historical_operator_v2.py status/run/resume/verify and recipe SHA. Original
absolute paths in receipts are provenance hints; pass equivalent restored paths.

Exact next: review_historical_forecast_slice, then common_target_policy_input_adapter_v2.
That adapter must bind immutable forecast/model/feature identities, available-at clocks,
native original-target horizons and explicit input tiers to the existing policy engine;
gross-return forecasts cannot silently become executable values. Calendar metadata,
recovered corrected-tree campaign, richer features and all37 historical items remain
in FULL_DESIGN_QUEUE.md. GPT/advisor comparisons, broker/service/account actions and
D-drive investigation remain deferred. No independent review or cloud sync is claimed.
