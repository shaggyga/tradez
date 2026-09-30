# Actual checks

Preflight: `python -I -B tools/forex_preflight.py --vault C:/Users/zmoor/OneDrive/thevault/projects/forex --profile stdlib` -> blocked; PREFLIGHT.json retained.

Timeseries312 Python, PYTHONPATH=workspace and trad: `-m pytest trad/test_oanda_dashboard_retirement.py trad/test_oanda_practice_live_dashboard.py trad/test_oanda_collection_dashboard_status.py trad/test_oanda_pair_forecast_dashboard.py trad/test_oanda_pair_forecast_dashboard_v2.py trad/test_oanda_operational_dashboard_selection_v1.py trad/test_oanda_feature_move_dashboard_v1.py trad/test_oanda_news_pair_dashboard.py -q -p no:cacheprovider` -> TESTS_FINAL.txt:388passed1failed. Last stale ordering assertion corrected; focused `test_oanda_dashboard_is_summary_first_with_collapsed_deep_research` passed1 in1.10s.

`-m pytest tests/test_forex_vault_snapshot.py -q -p no:cacheprovider` ->5passed in1.18s.
PowerShell supervisor AST parse -> no errors. git diff --check ->passed after preserving original unchanged line endings.
Dashboard task stopped, read back Ready/no listener, then started/Running. HTTP_AFTER.json:root/main200, crypto410. Browser Forex-only page, no console errors. No collector tasks changed.
