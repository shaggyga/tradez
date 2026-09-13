# Practice007 trial — September 9–11, 2026

The user authorized enabling practice trading and leaving it running until an end-of-week review. This is a separate, finite practice experiment using the current verified joint-v3 price/news forecast stream. Its implementation does not change the original studies' research-only contracts, their forecasts, original outcomes, or promotion eligibility. It has not established a profitable strategy or a statistically best model.

**Activated and broker-verified September 9, 19:09:47 UTC.** The worker opened its first practice trade at 19:08:03 UTC: USD/JPY short 166 units, fill 153.667, broker stop 153.736, initial margin $8.30. A separate broker read reconciled order 2462, fill/trade 2463 and protective stop 2464; all three transactions were retained. At verification, balance was $41.6042 and marked NAV $41.6031, with one open trade and its pending stop. This is a dated observation, not a realized profit or forecast-success claim. The original forecast target is September 9, 20:02:58 UTC.

The repaired release passed **286 focused tests**. Initial startup encountered Windows text-mode newline conversion in the account lock and refused before any broker operation or ledger initialization. The binary-mode correction passed real Windows marker/heartbeat/release tests, was independently reviewed and was pinned before restart. Watchdog 31308 / worker 39908 were running with zero watchdog restarts at the successful verification. Receipt: `C:\Users\zmoor\Documents\forex\practice_current_setup_20260909\runtime\PRACTICE_TRIAL_ACTIVATION_VERIFIED_20260909.json`, SHA256 `7391019844cef6fc33e7a0499e7d2d78fa7dc606feea217d05d6267b071a784c`. Its SQLite backup and source/test release inventories are retained alongside the individual module review receipts.

## Operating contract

- Account: OANDA **practice007**, exact account fingerprint bound in the activation file. The broker transport has a fixed `api-fxpractice.oanda.com` host and credential alias. Real-money accounts are unavailable to this route.
- Actual initial read-only preflight: USD NAV/balance **41.6042**, zero open trades/positions/pending orders, 68 broker FX instruments, one research-collection supervisor, no detected legacy execution worker.
- Signals: the main joint-v3 H1 price/news forecast, read through the existing bounded, source-verified observer. Price-only and neutral-news study controls remain diagnostic research evidence. No invented ensemble, shifted target, sign flip after a price move, or old forecast presented as newly issued.
- Entry: original forecast at most 900 seconds old; at least 60 seconds left to its original H1 target; directional probability at least 0.55; spread at most 4 bps; expected remaining gross move at least twice modeled spread/slippage/tick-rounding costs. These are explicit experimental defaults, not fitted or calibrated success claims.
- Sizing: one account-wide position, at most 0.5% current NAV in modeled stop loss and 20% NAV in entry margin. Broker instrument limits and actual account gain/loss/position-value conversions apply. Twenty percent is an entry sizing limit; small subsequent margin marks do not cause churn exits.
- Protection: broker-held stop on fill at two ATR14 ranges from 15 real consecutive completed M1 candles. No synthetic bars, averaging down, trailing, or target extension. Stops do not guarantee the modeled loss through gaps. Exit at the original forecast target, persistent session loss stop, missing protection, or trial stop.
- Limits: at most eight **claimed submission attempts** per UTC day, including subsequently canceled/refused claims; 30-minute cooldown per attempted pair. This conservative counter survives restarts. Session loss stop is permanent once an observed NAV drawdown reaches the smaller of 10% initial NAV or $5 (about $4.16 at the observed baseline).
- Finish: no newly entered forecast may expire after **Friday September 11, 20:40 UTC / 4:40 p.m. EDT**. Close remaining trial-owned trades by **20:45 UTC / 4:45 p.m. EDT**. If broker confirmation is unavailable, continue close-only recovery; never report confirmed completion from a failed read.

## Execution and recovery

The dedicated hidden watchdog starts only the finite practice worker and restarts it after crashes. The existing research supervisor remains in research-collection mode, preserving collectors, official/news processing, studies, and dashboard. No Codex automation, scheduled task, or chat monitoring was added.

The worker holds the existing account-wide `practice_007_order.lock` with a live-PID check and heartbeat. Every market intent, source observation, decision context and one-time claim is durably committed in SQLite before transport. A claimed order is never blindly resubmitted after restart or a timeout. Fresh account/position/order reads and exact policy revalidation occur before POST. Ambiguous writes are reconciled by original client/order/transaction/trade identity and block further entries until resolved. Foreign trades are never closed.

Mandatory owned-trade exits run before optional account, news, quote, or stored-entry-context reads. Corrupt owned-entry context causes a conservative owned-trade close and an entry halt. Transaction conflicts and evidence-hash changes are rejected. Loss-stop observations from selection and final submission checks are persisted, even if the next account mark recovers.

## Files and controls

- Activation: `config/practice007_joint_v3_20260909_v1.json` (explicit enabled flag, account fingerprint, policy and exact source hashes).
- Worker: `oanda_practice_trial_runner_v1.py`; hidden watchdog: `start_oanda_practice_trial_v1.ps1`.
- Runtime state: `data/oanda_training_manager/practice007_joint_v3_20260909_v1/status.json` and `watchdog.json`.
- Durable ledger: same directory, `trial.sqlite`; `events`, `intents`, `transactions`, and compressed `evidence` tables. Preserve SQLite WAL state when copying an active database; use SQLite backup for review.
- Default `python oanda_practice_trial_runner_v1.py --preflight` performs GET-only checks. `--run` explicitly starts the practice worker. `--stop` persists a local close-and-stop request for the running worker; command completion is not broker flatness confirmation. `completed_flat` is final and will not auto-reactivate.

Verification and release evidence are retained separately under `C:\Users\zmoor\Documents\forex\practice_current_setup_20260909`. The read-only policy integration observed 43 eligible-age source signals, requested nine candle sets, and found two policy candidates at that cutoff. These were hypothetical decisions with actual broker inputs, not trades or results. Subsequent runtime verification is in the activation receipt and live ledger; do not reuse this preflight count as current telemetry.

The dashboard's original study-level “orders disabled” message still describes those registered research studies. Practice trial status is the separate worker status; actual account positions and P/L come from the broker/account observer. Dashboard appearance remains deferred. The legacy account snapshot also calls a trade 'unprotected' when TAKE_PROFIT is absent. This trial uses an original-time exit and a mandatory STOP_LOSS; the actual pending stop was independently verified. That legacy complete-bracket label is not evidence of a missing stop. Renaming/splitting that display diagnostic remains deferred.

## End-of-week review and remaining work

Reconcile all claimed, withheld, canceled, filled, closed and unresolved intents against broker transactions. Report original forecast accuracy, calibration, coverage, gross movement, spread/slippage/financing, realized account P/L, and drawdown separately. A low trade count is a result to explain from retained gate reasons; do not force trades or lower thresholds retrospectively to manufacture success.

Use the September 9 complete records audit and prior-work reconstruction before adding another model: corrected HGB/lag/error features, wide 220/795/MA inputs, older curves, official-source currency response/ranking, and paper management comparisons already exist in different paths. Viable next research remains matched cost-survival admission, causal official-release currency-strength mapping and joint feature/co-movement tests with original observation clocks. Reuse compatible artifacts and register prospective comparisons; this operational activation does not close their statistical or compatibility gaps.
