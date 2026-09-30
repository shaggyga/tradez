# Derived all-68 feature feed — September 16, 2026

This supplement preserves the source, config, test, checkpoint note and final
live state for the M5-derived all-68 feature publisher added on September 16.

The live state records:

- 68 registered pairs.
- 68 native M5 histories read.
- 68 observed-current pairs.
- 68/68 base minimal feature readiness.
- 68/68 immediate one-bar movement readiness.
- `TRY_JPY` explicit derived-current from two short-tail fills.
- 41/68 full 216-field finite vectors.

No forecasts, fitting, broker/account reads, orders or trading-policy changes
are included in this supplement.

Primary files:

- `oanda_derived_technical_publisher_v1.py`
- `derived_technical_features_v1_20260916.json`
- `test_oanda_derived_technical_publisher_v1.py`
- `FOREX_DERIVED_ALL68_FEATURE_FEED_20260916.md`
- `VALIDATION_README.md`
- `all68_derived_technical_features_v1.json`

Recreate from the project root:

```powershell
& 'C:\Users\zmoor\AppData\Local\CodexRuntimes\timeseries312\Scripts\python.exe' -B -m pytest -p no:cacheprovider -q test_oanda_derived_technical_features_v1.py test_oanda_derived_technical_publisher_v1.py
& 'C:\Users\zmoor\AppData\Local\CodexRuntimes\timeseries312\Scripts\python.exe' -B oanda_derived_technical_publisher_v1.py --once
```

The publisher supports bounded `--interval-sec` / `--duration-sec` looping, but
this package does not register a V18 supervisor profile. Hashes are recorded in
`MANIFEST_SHA256.txt`.
