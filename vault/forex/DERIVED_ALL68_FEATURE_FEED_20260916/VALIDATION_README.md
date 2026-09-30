# Derived all-68 feature feed validation

Scope: `oanda_derived_technical_publisher_v1.py` and
`config/derived_technical_features_v1_20260916.json`.

Accepted checks:

- `test_oanda_derived_technical_publisher_v1.py`: 4 passed.
- `test_oanda_derived_technical_features_v1.py` plus publisher tests: 23 passed.
- Live one-shot publication wrote
  `data/oanda_training_manager/state/all68_derived_technical_features_v1.json`.

Final live coverage:

```json
{
  "current_pairs": 68,
  "derived_current_pairs": 1,
  "fully_finite_pairs": 41,
  "histories_read": 68,
  "minimal_feature_count": 9,
  "minimal_feature_current_pairs": 68,
  "minimal_feature_finite_pairs": 68,
  "movement_feature_count": 2,
  "movement_feature_current_pairs": 68,
  "movement_feature_finite_pairs": 68,
  "observed_current_pairs": 67,
  "registered_pairs": 68,
  "source_errors": 0,
  "status_counts": {
    "derived_current": 1,
    "observed_current": 67
  }
}
```

Publication generation:
`166cf89e46c2695701ead4819f591d41d897a7694c15569cbae1f2e4849381cb`.

Interpretation: the base M5-derived current feed is present for all 68 pairs.
Immediate one-bar movement support is present for all 68 pairs. `TRY_JPY` is
derived-current with two short-tail fills and no unfilled tail. No forecasts, fitting, broker
queries, account reads, orders or trading-policy changes are part of this
validation.
