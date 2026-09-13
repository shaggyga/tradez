# Offline joint-return baseline implementation

The new pure `oanda_joint_return_baselines.py` implements three direct-horizon ridge regressions for EUR/USD: own trailing returns; own plus six peer pairs; and own plus a peer currency factor that excludes EUR/USD. USD-first pairs are inverted so every peer feature means non-USD strength against USD. This is a compact VAR-style baseline, not a recursive VAR system, cointegration/VECM or HMM implementation.

All 68 focused tests passed in 2.26 seconds (JUnit suite timing 2.230 seconds). They cover genuine field requirements, malformed/duplicate/future/partial input rejection, shuffled input determinism, exact targets, missing intervals, actual availability cutoffs, common training subsets, training-only scalers, quote-unit invariance, exclusion of EUR/USD from the peer factor, and two synthetic peer-lead relationships. The synthetic tests establish implementation behavior only. No historical real-data accuracy or trading improvement has been established.

Defaults are fixed in every result: seven major pairs; EUR/USD target; one-hour horizon; trailing compounded 1/5/15/60-minute log returns in basis points; ridge alpha 10; at least 120 training rows; at most the latest 4,096 valid rows; one-minute training stride; training-only mean/std scaling. All arms use identical matured training rows. There is no internal parameter search, probability calibration, P/L calculation or runtime integration. Overlapping labels are explicitly not independent samples.

Each supplied minute must contain exactly `EUR_USD`, `GBP_USD`, `AUD_USD`, `NZD_USD`, `USD_JPY`, `USD_CHF`, and `USD_CAD`. Bar dictionaries require `instrument`, `start_epoch`, `end_epoch`, `close_mid`, and `available_epoch`. Start/end are actual UTC minute boundaries 60 seconds apart. `available_epoch` is the actual time this input became available to the consumer, conservatively sampled after a stable capture; it must never be invented as “bar close plus a delay.” A supplied `complete` flag must be exactly true. Any missing pair, duplicate, invalid price, post-reference bar, or availability at/after decision raises `ValueError`. Missing whole minutes remain gaps; every feature and training-label interval must be continuous. Insufficient history returns an all-arm abstention.

Usage once genuinely observed input has been assembled:

```python
from oanda_joint_return_baselines import fit_joint_return_baselines

# observed_bars contains the five required fields above for all seven pairs.
# reference_epoch is the unchanged market reference minute.
# decision_epoch is the caller's real observation/decision cutoff, after capture.
result = fit_joint_return_baselines(
    observed_bars,
    reference_epoch=reference_epoch,
    decision_epoch=decision_epoch,
    horizon_sec=3600,
    lag_minutes=(1, 5, 15, 60),
    alpha=10.0,
    min_training_rows=120,
    max_training_rows=4096,
)
```

The function requires `reference_epoch <= decision_epoch < reference_epoch + horizon_sec`; it never moves the target to account for latency. Training labels require both their exact maturity and actual availability strictly before decision. A future use would also need independently recorded real fitting/issue/commit times; this offline helper cannot certify them. Archived bars lack original availability, so using synthetic historical receipt times would not establish causal market performance.

The exact runtime was `C:\Users\zmoor\AppData\Local\CodexRuntimes\timeseries312\Scripts\python.exe` (Python 3.12.10, NumPy 2.5.1, pytest 9.1.1). `run_tests.py` disables plugin auto-loading and blocks network, worker starts, non-fixture writes and non-fixture SQLite connections. The adjacent validation JSON records source/test hashes, exact command, timing of three synthetic 600-minute fits, and unchanged hashes of all six registered study sources. No project process, database, registered configuration or existing numerical source was changed by this implementation.
