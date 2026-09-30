# Feature reuse and account-role registry

Generated UTC: 2026-09-16T17:19:26.0362641Z

This checkpoint prevents duplicate rebuilds while separating the two paper lanes.

## Account roles

- **Practice-006** is scratch paper: discretionary live research, paper-only experiments, candidate logic generation and diagnostics. Its losses and rejected setups are evidence, but they are not proof for 007 by themselves.
- **Practice-007** is the official best bot: only reproducible algorithmic/model/code-driven trades through the existing authorization/governance path. No manual discretionary logic should be imported into 007 unless it has become code, has been checkpointed, and uses canonical feature sources.

## Canonical feature families already present

| Family | Canonical sources | What already exists | Reuse rule |
| --- | --- | --- | --- |
| Currency strength/rank | oanda_currency_rank_model.py; docs/FOREX_FEATURE_DICTIONARY_CURRENT.*; config/model_feature_space.json | 21-currency strength, strong-vs-weak pair expressions, 5m/15m/60m weighted ranks, strength gaps, breadth/dispersion | Reuse before adding any new currency/relative-strength feature. |
| Rolling technical/move quality | oanda_rolling_technical_features_v1.py; oanda_derived_technical_features_v1.py | returns, path efficiency, absolute path, range position, ATR/volatility, spread, tick activity | Extend only if the existing registry lacks the behavior. |
| Conditional patterns | oanda_all68_pattern_research.py; oanda_distinct_strategy_backtest.py | cross-strength continuation, price-strength agreement/disagreement, low-cost momentum, volatility expansion | Research-only until validated/promoted. |
| News currency mapping | oanda_news_blurb_direction_attribution.py; feature dictionary | currency scores and pair impulse as base minus quote | Do not build a second mapper unless this path is proven insufficient. |
| Cost/execution gates | scalper, rank model, pattern/backtest modules | spread hurdles, confirmation-to-cost, spread-to-volatility, bid/ask after-cost outcomes, score gate | Keep 006/007 candidates cost-aware; do not loosen after all-negative fills without evidence. |

## Current live interpretation

006 is allowed to explore. 007 should consume the same canonical feature families only after the logic is reproducible, deduplicated, logged, and promoted through the official path. This avoids repeating work already present in the project/vault.

## Deduplication checkpoint - 2026-09-16 14:22:34 -04:00

Currency strength and relative-strength work is treated as already built and canonical. Do not rebuild it under a new feature name. Reuse oanda_currency_rank_model.py, config/model_feature_space.json, and the current feature dictionary for 21-currency strength, relative strength, strong-vs-weak expressions, strength gaps, cross-strength continuation, and rank/breadth/dispersion concepts.

Current scratch lane is v15, not the older v9 profile: Practice-006 is running the low-candle-pressure score-250 profile with post-candle quote refresh and post-loss score gating. Practice-007 remains unchanged and should only receive reproducible code/model logic after promotion.

Recent existing references found for strength/rank dedupe:

``text
.\advisor_change_log.md:3:- 2026-07-01T02:10:00Z - Research-only all-68 large-spike ruleset search: added `oanda_all68_large_spike_ruleset_search.py` and scanned all 68 OANDA pairs across 15/30/60/120/240m horizons using pair/horizon q99.5 moves with >= `1.5` ATR units and >= `3.0` move/spread filters. Found `48257` deduplicated large spike events. Best spike-concentration predictor was 15m LONG `atr15_to_atr240>=q95(1.904) AND strength_gap_60<=q05(-0.001765)`, with validation precision `2.56%` vs `0.157%` base (`16.33x` lift), `5815` first triggers, and `149` large spikes caught. Execution caveat: the same rule was still negative cost-adjusted (`-574.86` q-threshold units; `41.74%` win rate), and no tested high-volume rule had positive out-of-sample net expectancy. Artifacts are under `data/all68_weekly_move_study/large_spike_ruleset_search/`. Research artifact only; no broker execution changes.
.\build_trading_logic_sheet.py:409:                "strength_gap_rank_15": float(row.get("strength_gap_rank_15", np.nan)),
.\build_trading_logic_sheet.py:410:                "strength_gap_rank_60": float(row.get("strength_gap_rank_60", np.nan)),
.\all68_weekly_missed_move_study.py:79:    "strength_gap_15",
.\all68_weekly_missed_move_study.py:80:    "strength_gap_60",
.\all68_weekly_missed_move_study.py:81:    "strength_gap_rank_15",
.\all68_weekly_missed_move_study.py:82:    "strength_gap_rank_60",
.\all68_weekly_missed_move_study.py:371:def add_currency_strength(
.\all68_weekly_missed_move_study.py:405:            frame[f"strength_gap_{minutes}"] = (
.\all68_weekly_missed_move_study.py:408:            frame[f"strength_gap_rank_{minutes}"] = (
.\all68_weekly_missed_move_study.py:467:    add_currency_strength(paths, base_minutes=base_minutes)
.\all68_weekly_missed_move_study.py:987:        "strength_gap_15",
.\all68_rolling_condition_validation.py:34:    "strength_gap_15",
.\all68_rolling_condition_validation.py:48:    Rule("m5_strength15_upper", "momentum_5_atr", "strength_gap_15"),
.\all68_rolling_condition_validation.py:49:    Rule("m15_strength15_upper", "momentum_15_atr", "strength_gap_15"),
.\all68_rolling_condition_validation.py:50:    Rule("m30_strength15_upper", "momentum_30_atr", "strength_gap_15"),
.\COMPLETE_SOURCE_AUDIT_20260806.md:162:| `currency_strength` | Synthetic base-minus-quote currency strength | 3/4 | 255 | 22 | 34.1 | -3.584 |
.\FOREX_AUDIT_STATE_CURRENT.json:186:    "currency_rank": {
.\FOREX_AUDIT_STATE_CURRENT.json:187:      "contract_id": "source_conditioned_currency_rank_v7_v8_input_explicit_no_trade_20260901",
.\FOREX_AUDIT_STATE_CURRENT.json:299:      "source": "config/source_conditioned_currency_rank_v7.json",
.\FOREX_AUDIT_STATE_CURRENT.json:329:      "source": "data/oanda_training_manager/state/source_conditioned_currency_rank_v7.json",
.\config\alfred_macro_relative_strength_research_v1.json:3:  "contract_id": "alfred_macro_relative_strength_discovery_v3_cpi_unemployment_20260816",
.\FOREX_BOJ_MARKET_STRUCTURE_OVERLAY_VALIDATION_20260901.json:107:    "source_rank_supervisor_command": "python -m pytest -q test_oanda_local_news_sentiment.py test_oanda_causal_source_factor_response_map_v4.py test_oanda_causal_source_factor_response_map_v5.py test_oanda_causal_source_factor_response_map_v6.py test_oanda_causal_source_factor_response_map_v7.py test_oanda_causal_source_factor_response_map_v8.py test_oanda_source_conditioned_currency_rank_v4.py test_oanda_source_conditioned_currency_rank_v5.py test_oanda_source_conditioned_currency_rank_v6.py test_oanda_source_conditioned_currency_rank_v7.py test_oanda_supervisor_watchdog.py test_oanda_supervisor_integrity_freshness.py",
.\FOREX_BOJ_MARKET_STRUCTURE_OVERLAY_VALIDATION_20260901.json:118:    "oanda_source_conditioned_currency_rank_v7.py": "6bb8531713411276cc2b9ab3f2685c740a9d5c25bc9adb246c9e5223235e7265",
.\FOREX_BOJ_MARKET_STRUCTURE_OVERLAY_VALIDATION_20260901.json:119:    "test_oanda_source_conditioned_currency_rank_v7.py": "d5b9c591fb7708e95004da831ecd3e696b2fdcf393bba5a25de0d3c45627d143",
.\FOREX_BOJ_MARKET_STRUCTURE_OVERLAY_VALIDATION_20260901.json:120:    "config/source_conditioned_currency_rank_v7.json": "8339403c4dbe1a8ae267314199f626a83266d943b4d2e59b800673b3e7940e79",
.\FOREX_CAUSAL_IO_REPAIR_VALIDATION_20260906.json:725:      " M test_oanda_source_conditioned_currency_rank_v4.py",
.\FOREX_CAUSAL_IO_REPAIR_VALIDATION_20260906.json:726:      " M test_oanda_source_conditioned_currency_rank_v5.py",
.\FOREX_CAUSAL_IO_REPAIR_VALIDATION_20260906.json:840:      "?? config/source_conditioned_currency_rank_v6.json",
.\FOREX_CAUSAL_IO_REPAIR_VALIDATION_20260906.json:841:      "?? config/source_conditioned_currency_rank_v7.json",
.\FOREX_CAUSAL_IO_REPAIR_VALIDATION_20260906.json:842:      "?? config/source_conditioned_currency_rank_v8.json",
.\FOREX_CAUSAL_IO_REPAIR_VALIDATION_20260906.json:963:      "?? oanda_source_conditioned_currency_rank_v6.py",
.\FOREX_CAUSAL_IO_REPAIR_VALIDATION_20260906.json:964:      "?? oanda_source_conditioned_currency_rank_v7.py",
.\FOREX_CAUSAL_IO_REPAIR_VALIDATION_20260906.json:965:      "?? oanda_source_conditioned_currency_rank_v8.py",
.\FOREX_CAUSAL_IO_REPAIR_VALIDATION_20260906.json:1000:      "?? test_oanda_source_conditioned_currency_rank_v6.py",
.\FOREX_CAUSAL_IO_REPAIR_VALIDATION_20260906.json:1001:      "?? test_oanda_source_conditioned_currency_rank_v7.py",
.\config\alfred_short_rate_relative_strength_research_v1.json:3:  "contract_id": "alfred_short_rate_relative_strength_discovery_v1_20260816",
.\docs\AUDIT_STATE_CURRENT.md:226:| Source-conditioned rank | `config/source_conditioned_currency_rank_v7.json`; `data/oanda_training_manager/state/source_conditioned_currency_rank_v7.json` | V7 requires V8 input; zero decisions and zero independent source episodes. Research only. |
.\FOREX_CORRECTNESS_REPAIR_VALIDATION_20260901.json:14:    "oanda_source_conditioned_currency_rank_v6.py": "dd54e21b9c9c51ce762a31e2903083109486dba44aa0b8a252b9be7f1cf1102f",
.\FOREX_CORRECTNESS_REPAIR_VALIDATION_20260901.json:96:      "contract_id": "source_conditioned_currency_rank_v6_v7_input_explicit_no_trade_20260901",
.\docs\feature_dictionary\historical_panel_schema.json:25:      "cross_strength_m1",
.\docs\feature_dictionary\historical_panel_schema.json:26:      "cross_strength_m3",
.\docs\feature_dictionary\historical_panel_schema.json:27:      "cross_strength_m5",
.\docs\feature_dictionary\historical_panel_schema.json:116:      "absolute__cross_strength_m1",
.\docs\feature_dictionary\historical_panel_schema.json:117:      "absolute__cross_strength_m3",
.\docs\feature_dictionary\historical_panel_schema.json:118:      "absolute__cross_strength_m5",
.\docs\feature_dictionary\historical_panel_schema.json:182:      "signed_square__cross_strength_m1",
.\docs\feature_dictionary\historical_panel_schema.json:183:      "signed_square__cross_strength_m3",
.\docs\feature_dictionary\historical_panel_schema.json:184:      "signed_square__cross_strength_m5",
.\docs\feature_dictionary\catalog_251.json:148:      "block": "currency_strength",
.\docs\feature_dictionary\catalog_251.json:160:        "base_currency_strength",
.\docs\feature_dictionary\catalog_251.json:161:        "quote_currency_strength",
.\docs\feature_dictionary\catalog_251.json:163:        "currency_strength_rank",
.\docs\feature_dictionary\catalog_251.json:164:        "currency_strength_dispersion",
.\docs\feature_dictionary\catalog_251.json:165:        "currency_strength_reversal_z"
.\docs\feature_dictionary\catalog_251.json:2378:      "block": "currency_strength",
.\docs\feature_dictionary\catalog_251.json:2414:      "block": "currency_strength",
.\docs\feature_dictionary\catalog_251.json:2450:      "block": "currency_strength",
.\docs\feature_dictionary\catalog_251.json:2486:      "block": "currency_strength",
.\docs\feature_dictionary\catalog_251.json:2522:      "block": "currency_strength",
.\docs\feature_dictionary\catalog_251.json:2558:      "block": "currency_strength",
.\docs\feature_dictionary\catalog_251.json:2594:      "block": "currency_strength",
.\docs\feature_dictionary\catalog_251.json:2630:      "block": "currency_strength",
.\docs\feature_dictionary\catalog_251.json:2664:      "feature_id": "base_currency_strength",
.\docs\feature_dictionary\catalog_251.json:2665:      "name": "base_currency_strength",
.\docs\feature_dictionary\catalog_251.json:2666:      "block": "currency_strength",
.\docs\feature_dictionary\catalog_251.json:2699:      "feature_id": "quote_currency_strength",
.\docs\feature_dictionary\catalog_251.json:2700:      "name": "quote_currency_strength",
.\docs\feature_dictionary\catalog_251.json:2701:      "block": "currency_strength",
.\docs\feature_dictionary\catalog_251.json:2736:      "block": "currency_strength",
.\docs\feature_dictionary\catalog_251.json:2746:      "intended_definition": "Intended: base_currency_strength - quote_currency_strength.",
.\docs\feature_dictionary\catalog_251.json:2769:      "feature_id": "currency_strength_rank",
.\docs\feature_dictionary\catalog_251.json:2770:      "name": "currency_strength_rank",
.\docs\feature_dictionary\catalog_251.json:2771:      "block": "currency_strength",
.\docs\feature_dictionary\catalog_251.json:2803:      "feature_id": "currency_strength_dispersion",
.\docs\feature_dictionary\catalog_251.json:2804:      "name": "currency_strength_dispersion",
.\docs\feature_dictionary\catalog_251.json:2805:      "block": "currency_strength",
.\docs\feature_dictionary\catalog_251.json:2837:      "feature_id": "currency_strength_reversal_z",
.\docs\feature_dictionary\catalog_251.json:2838:      "name": "currency_strength_reversal_z",
.\docs\feature_dictionary\catalog_251.json:2839:      "block": "currency_strength",
``
