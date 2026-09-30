# All-68 fully finite feature fix — 2026-09-16

Updated: 2026-09-16T14:21:12.771330Z

The all-68 derived M5 feature publisher now reports `fully_finite_pairs=68` with zero source errors. The previous 8 incomplete pairs were caused by sparse intraday candle support in long-window rich features, mainly Scandi/TRY pairs.

Live coverage:

```json
{
  "current_pairs": 68,
  "fully_finite_pairs": 68,
  "histories_read": 68,
  "minimal_feature_count": 9,
  "minimal_feature_current_pairs": 68,
  "minimal_feature_finite_pairs": 68,
  "movement_feature_count": 2,
  "movement_feature_current_pairs": 68,
  "movement_feature_finite_pairs": 68,
  "registered_pairs": 68,
  "source_errors": 0,
  "status_counts": {
    "derived_current": 1,
    "observed_current": 67
  }
}
```

Fallback-marked pairs:

- EUR_TRY: 34 fallback values
- USD_TRY: 34 fallback values
- TRY_JPY: 77 fallback values

Fallbacks are explicit in each pair's `value_fallbacks` map. They use last prior finite values, nearest shorter same-family values, neutral activity/spread denominator values, and publisher-only neutral flat-candle shape values. Minimal and movement features remain finite for all 68.

Operational health after restart:

```json
{
  "operational_health": "role_liveness_current",
  "restart_circuit_open": false,
  "owned_role_failures": [],
  "owned_roles_missing_from_heartbeat": [],
  "operational_profile_sha256": "d6084f13afe925ec90e98d1b1f80ed8b8f7a57705895fb79abfb8e2e04e28d87"
}
```

Changed hashes:

- `oanda_derived_technical_features_v1.py`: `85d5863dcb9d25275680700dfeb0f91d9f91beee0f7c3c52929de1eddc1b09c9`
- `oanda_derived_technical_publisher_v1.py`: `f93ca38a60cedfb206c0f2cc8e7706810a7da9c3faa8f99c001c192ebde8fa33`
- `config/derived_technical_features_v1_20260916.json`: `06b0f3edb54b9123adfdb3cb69408dd65e05380eec49d057d2c8568c2d6ae425`
- `config/operational_runtime_v19_20260916.json`: `d6084f13afe925ec90e98d1b1f80ed8b8f7a57705895fb79abfb8e2e04e28d87`

Tests: `test_oanda_derived_technical_features_v1.py test_oanda_derived_technical_publisher_v1.py` passed: 25 passed.
