# Local FX News Sentiment

## Purpose

`oanda_local_news_sentiment.py` provides deterministic, no-GPT current-news
context for the 68-pair Forex signal matrix. It polls configured official
central-bank and statistical-release feeds plus bounded unverified discovery
feeds, deduplicates and clusters headlines, applies timestamped rule-based
currency scoring, and publishes pair-level context.

This subsystem is research-only. It cannot authorize an order, bypass a gate,
or change allocation. Its matrix weight is fixed at zero until prospective
untouched validation demonstrates incremental executable value.

## Inputs And Outputs

- Source configuration: `config/news_sources_v1.json`
- Collector: `oanda_local_news_sentiment.py`
- Live owner: `oanda_always_on_supervisor.ps1`
- Retired standalone watchdog: `oanda_local_news_supervisor.ps1` is preserved
  for history but quarantined and must not run beside the canonical supervisor.
- Joined monitor: `oanda_signal_news_monitor.py`
- SQLite history:
  `data/oanda_training_manager/local_news_sentiment/local_news_sentiment_v1.sqlite`
- Latest articles:
  `data/oanda_training_manager/local_news_sentiment/articles_latest.json`
- Semantic topic stream:
  `data/oanda_training_manager/local_news_sentiment/topics_latest.json`
- Context-only audit articles:
  `data/oanda_training_manager/local_news_sentiment/context_articles_latest.json`
- Latest pair context:
  `data/oanda_training_manager/local_news_sentiment/pair_sentiment_latest.json`
- Currency and pair source coverage:
  `data/oanda_training_manager/local_news_sentiment/source_coverage_latest.json`
- Collector state:
  `data/oanda_training_manager/local_news_sentiment/collector_state_v1.json`
- Historical replay:
  `oanda_news_feed_backtest.py`

The collector synchronizes its timestamped observations into the existing
all-pair event tagger. Generated data stays on D and is not copied into the
Vault checkpoint.

The dedicated watchdog runs the collector with no duration limit, checks the
collector output every 30 seconds, and restarts it when the output remains more
than five minutes old after startup grace. It forces real-money execution
environment flags off in its child process.

## Causality And Quality

- `first_seen_utc` prevents an article from being treated as known before it
  was observed.
- Structured-calendar rows retain `scheduled_utc`, provider update time,
  actual, consensus, previous, revised-previous, unit, reference period,
  importance, and raw surprise/revision. Their directional interpretation is
  explicitly pending series-specific semantics, so a raw positive surprise
  cannot accidentally make unemployment bullish.
- Credentialed sources are skipped with `credential_missing` until one of their
  configured environment variables exists. Disabled or unsupported inventory
  sources are reported but never polled. Credential values are not written to
  state or result artifacts.
- Source roles distinguish primary policy/statistical truth, structured macro
  calendars, aggregators, historical revisions, positioning, options, and
  licensed low-latency discovery.
- Sentiment records separate generic tone, monetary impulse, risk-on/off,
  transmission mechanism, confidence, and uncertainty. Vendor sentiment is
  retained as an independent audit field and does not become a direction vote.
- Semantically equivalent and syndicated headlines are represented by one
  stable topic. Distinct publishers increase corroboration; repeated polls do
  not.
- Topic hashtags describe the event family and action (for example,
  `#usd_hawkish_guidance` or `#oil_oil_down`). Each pair row records which
  topics contributed, their pair-direction score, age-decayed weight, source
  provenance, and direct versus inferred leg evidence.
- All 68 configured instruments are emitted every cycle. Evidence is labelled
  `TWO_SIDED_DIRECT`, `ONE_SIDED_DIRECT`, `GLOBAL_TOPIC_PROXY`,
  `INFERRED_ONLY`, or `NO_CURRENT_EVIDENCE`; confidence is capped according
  to that label.
- `published_utc` controls freshness decay when supplied by the source.
- Source verification and classification confidence are separate fields.
- Negated, expected-hold, preview, and speculative policy headlines are not
  treated as confirmed hikes or cuts. Unverified speculation is retained as
  context with zero directional policy impulse.
- Market-related rows with no deterministic currency impulse are retained in
  SQLite and the context-only audit export, but are excluded from the active
  ledger, pair scores, and all-pair event catalog.
- The all-pair event catalog refreshes in a separate one-shot worker no more
  than once every 15 minutes, so catalog SQLite maintenance cannot stall the
  one-minute collector snapshot.
- Direct RSS/Atom and JSON official feeds are normally checked every three
  minutes. Verified official-publisher search proxies are checked every 15
  minutes. The collector itself publishes once per minute, so a new snapshot
  does not imply that a publisher released a new story.
- Source configuration spans all 21 currencies used by the 68 pairs. Coverage
  distinguishes direct official feeds from domain-verified official-publisher
  proxy discovery.
- Exact syndicated headlines are clustered across publication hours so a
  delayed syndication copy cannot multiply one event's pair weight.
- Atomic output replacement uses unique temporary files and bounded retry for
  Windows readers. Failures and rate limits are recorded; no source failure
  enables execution.
- `openai_calls` is always zero.

## Validation Path

The joined monitor records price-signal direction, news direction, agreement,
disagreement, account state, and bot health at each cycle. It matures
1/5/15/30/60-minute outcomes and reports direction accuracy plus executable
bid/ask returns in basis points for price-only, news-only, agreement-only, and
news-veto diagnostics. Before any nonzero matrix weight is considered, evaluate
chronologically:

1. Price-only baseline.
2. News-only diagnostic.
3. Price plus news context.
4. Untouched final period after freezing sources, rules, thresholds, and
   allocator policy.
5. Executable bid/ask economics after spread and slippage.

No promotion is permitted from headline anecdotes or in-sample agreement.

`oanda_news_feed_backtest.py` also replays the retained topic archive against
local OANDA M1 bid/ask candles. It uses first-known timestamps, per-instrument
pip locations, spread-aware entry/exit prices, and reports direction accuracy,
executable return, MFE, and MAE by pair, topic, source, and horizon. This replay
is diagnostic only and cannot change the matrix weight or place an order.
