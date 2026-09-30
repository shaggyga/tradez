# Resume after offline dashboard source qualification

Start with project START_HERE.md and live Vault board/current pointers. This is one completed offline compatibility step, not a resumed timed run. The prior user stop is historical and was superseded only for this step by "Resume".

The exact registered news source was recovered from `git show 63cf4f2^:trad/oanda_local_news_sentiment.py`; SHA256 `44e66d85f82b52d6dc82e2e277bad31d2ee8c16304bec9c6a083a8b17eeb6c50`. All68 joint pairs validate. New interpretation remains in `trad/oanda_news_interpretation_v1.py`, accessible with `classify_article_with_interpretation` from `oanda_news_interpretation_adapter_v1`. Legacy direct callers keep the original shape; no archive rows rewritten.

Next operational action: inspect the selected joint/price study paths in `trad/config/operational_dashboard_current.json`, resolve actual intended candle/quote/news input routes, and verify the joint registry news IO SHA256 `09b42dffe55d52d6d375debdcfbfea7fec58a88cf7727a6fae5d46d70a462fc9`. Do not point the historical producer at the newer news archive without qualifying that contract. `oanda_joint_price_news_forecast_study_v7.py` requires explicit `--news-io-config`; its `--once` is a runtime action, not the read-only registry check used here. Prepare health/freshness and rollback checks before a separately authorized service launch. Neither a registry load nor HTTP200 makes saved forecasts current.

Both summaries remain `stale_or_future_summary`. No source mismatch remains in actual HTTP readback. No producer or collector restart, fit, broker/account action, capture mutation, goal or automation occurred.

Current paths587 tests +13 subtests pass.27 old repair-v1 failures reproduce before this change. Current producer uses v2. Preflight blocks on pointer-schema/manifest coverage; engineering environment passes but model runtime remains unqualified. Same-task review complete, independent review unperformed.

Scientific next remains the separately governed Extra Trees source-only partial and exact saved-forecast/news overlap reconciliation. Do not manufacture contemporaneous August24-September7 technical forecasts for September15-17 news. Git publication commit is recorded in live SHARED_GIT_REMOTE_LATEST.json and the successor publication receipt; this packet binds exact changed source bytes through FILE_CHANGES.json.

Rollback: restore this step's changed paths from baseline commit89446cfd490c8202ec838e2e1b999b673c758152 after checking current ownership. That restores the prior news-source mismatch; retain evidence of this repair and do not alter saved model registries. Prefer reverting the publication commit if later unrelated changes exist.
