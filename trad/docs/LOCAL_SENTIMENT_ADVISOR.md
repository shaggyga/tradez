# Local Sentiment Advisor

`oanda_local_sentiment_advisor.py` is the shadow reconstruction of the
portfolio analysis formerly supplied by GPT web-search calls.

It reads only existing local artifacts:

- causal pair news scores from `oanda_local_news_sentiment.py`;
- the current signal matrix for completed-bar price confirmation;
- the completed-bid/ask market sentiment ticker;
- the read-only dashboard snapshot for account suffix `-002`.

It produces a GPT-shaped portfolio review with:

- a currency ranking and portfolio regime;
- globally ranked USD and non-USD pair candidates;
- source freshness, verification, diversity, and directional consistency;
- price/news agreement or contradiction;
- 1/5/15/30/60-minute risk, haven, USD, commodity, and EM market regimes;
- one sentiment review for each existing position;
- explicit reasons and thesis invalidation conditions.

The module is intentionally shadow-only. It has no OANDA client, credential
reader, or order method. `orders_to_execute` and `event_permissions` are always
empty, and all outputs remain `execution_eligible=false`.

Run one read-only snapshot:

```powershell
python .\oanda_local_sentiment_advisor.py
```

Refresh the market ticker first:

```powershell
python .\oanda_market_sentiment_ticker.py
```

Continuous research ticker:

```powershell
python .\oanda_market_sentiment_ticker.py --source quotes --interval-sec 60 --duration-sec 604800
```

Continuous mode consumes the small atomic quote snapshot and maintains its own
bounded 125-minute history. It does not repeatedly query the shared move-alert
SQLite database.

Outputs:

- `data/oanda_training_manager/local_sentiment_advisor/LATEST.json`
- `data/oanda_training_manager/local_sentiment_advisor/decision_history.jsonl`

The active account manager remains fail-closed. Wiring this analysis into
position or order decisions requires frozen scoring rules and untouched
prospective evidence against price-only, no-trade, and the historical GPT
decision surface.
