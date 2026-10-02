# Live currency ranker — October 2, 2026

The ranker is connected to current local data and appears on the Forex dashboard.
Its original weighted 5/15/60-minute currency-strength scoring and spread-weighted
currency solver are reused without fitting. The old September 5 ticker and August
24 Practice-006 heartbeat are historical; neither is the current ranking source.

## Current path

- Publisher: `trad/oanda_live_currency_rank_v1.py`, supervised role `live_currency_rank_v1`.
- Inputs: existing `practice_007_exact_quote_receipts_v1.json` and read-only
  `practice_007_quote_intensity_shadow_v1.sqlite` under the training-manager state directory.
- Publication: `trad/data/oanda_training_manager/state/live_currency_rank_v1.json`.
- Worker liveness: adjacent `live_currency_rank_worker_v1.json`. A data refusal is
  distinct from a dead worker and does not trigger pointless restarts.
- Dashboard: `http://127.0.0.1:8765/`, Currency strength ranker panel;
  read-only endpoint `/api/currency-rank` refreshes every ten seconds.
- Source implementation: `oanda_currency_rank_model.py` and
  `oanda_market_sentiment_ticker.py`; exact dependency hashes are checked.

The registered runtime recovery expiry remains **2026-10-07T08:14:50Z**. Existing
controller, dashboard and logon bindings were migrated; no new scheduled automation
was created. Existing market/news workers and the other task's recorder were not
restarted. The rank publisher and dashboard were restarted for the support repair.
No account challenger or order worker was enabled.

## Meaning and coverage

Scores measure observed relative price strength, not forecast probabilities,
expected profits or trading approval. Original selected pairs are rule-based
confirmation/spread candidates; their `after_cost_proxy_bps` describes historical
movement after a stressed spread proxy, **not expected future net return**.

Two fixed universes are reported separately: all 21 currencies and the eight majors
(AUD, CAD, CHF, EUR, GBP, JPY, NZD, USD). Each needs a connected supported graph with
at least 75% of its listed edges. Every major must have at least three supported
crosses; full21 requires every currency to have an observed connection. Unsupported
crosses are excluded from the solver, explicitly listed and cannot pass pair
confirmation. Scores from different universes or changing support are not directly
interchangeable. Unavailable TRY inputs currently prevent a full21 ranking.

Raw current bid/ask receipts must be verified, tradeable and at most 60 seconds old.
All three historical endpoints must be at or before their cutoff and no more than
90 seconds behind it. No fabricated opening quotes, zero-filled returns or future
minute closes are used. Minute-end mids are observation inputs, not executable
historical bid/ask paths. Publications expire after 45 seconds; the dashboard also
rechecks each used current quote's age. Missing inputs withhold affected output.

## Verification and review

41 tests passed, including unchanged ranker/ticker tests, stale/future/missing input
cases, incomplete/disconnected graphs, dashboard consumer, source identity and
worker-heartbeat behavior. Four native profile cases passed. JavaScript syntax
passed `node --check`. The real dashboard endpoint returned successive current
publications. A relocated replay reverified raw receipts and reproduced the entire
rank result byte-for-byte while reads from original workspace roots were denied.
Original scoring source hashes are unchanged. Review is substantive same-task
review, not an independent reviewer or scientific promotion.

Local evidence: `evidence/ranker_repair_20261002/`. Vault packet:
`LIVE_CURRENCY_RANKER_20261002`. `PORTABLE_REPLAY.json` records the exact capsule and
result hashes; `ranker_replay_capsule.zip` carries source, captured inputs and replay.
Run the capsule's `replay.py` with the original workspace path as its sole argument
under the existing engineering Python environment. It performs no network calls or fits.

## Separate indicator cross-check

The recorded check at **2026-10-02 02:01:24 UTC (October 1, 10:01 p.m. Eastern)**
used independently read M1 feature observations, at most 180 seconds old. Currency
direction is aligned to the base or quote leg; these counts cover each currency's
seven major crosses. This is a separate calculation by the same task. Indicators
share price data with the ranker and are not statistically independent evidence of
predictive value. The existing specialist, mean-reversion and forecast-error studies
remain authoritative for their own historical experiments.

| Rank | Currency | Score | RSI / MACD / EMA20 supporting crosses | Interpretation |
|---|---|---:|---|---|
| 1 | NZD | 3.942 | 7/7 · 7/7 · 7/7 | Strongest broad agreement; NZD/USD stretched above 2 Bollinger standard deviations. |
| 2 | AUD | 1.209 | 6/7 · 6/7 · 6/7 | Broad agreement; AUD/USD and AUD/CAD stretched. |
| 3 | GBP | 0.283 | 5/7 · 4/7 · 3/7 | Mixed; ranker's short-window strength was negative. |
| 4 | JPY | -0.107 | 4/7 · 5/7 · 5/7 | Mixed; MACD acceleration supported only 2/7 crosses. |
| 5 | CAD | -0.230 | 3/7 · 2/6 · 1/7 | Weak confirmation; one MACD input unavailable. |

At that snapshot the original rules selected long NZD/USD and short EUR/AUD.
NZD/USD RSI was 73.5, Bollinger z 2.03; EUR/AUD RSI was 27.9 and its historical
after-spread movement proxy only 0.16 bps. Those are observations, not endorsed
entries. No position was opened. Later rankings can differ; use the timestamped
API rather than treating this table as a continuing live signal.

## Cleanup and exact next work

Navigation now has one current notice instead of a stack of obsolete “current”
notices. Original files are preserved byte-for-byte in
`docs/history/ranker_cleanup_20261002/`; no research evidence was deleted. Old
Practice-006 outputs and source-conditioned research ranks remain historical.

Next: `retained_tracking_capacity_repair_v1`. The outcome store reached its
configured 2 GiB limit. Preserve historical first observations, pending settlement,
news links and replay identity while qualifying a bounded storage lifecycle. Then
resume `retained_management_economic_input_binding_v1`. Costs, conditional
management and broader trading readiness remain unfinished. This repair does not
establish a profitable strategy or complete the full project.
