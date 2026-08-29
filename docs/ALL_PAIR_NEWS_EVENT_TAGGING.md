# All-Pair News Event Tagging

## Purpose

`oanda_news_event_tagger.py` consolidates the project's previously separate
news/event evidence:

1. Curated historical events from `build_trading_logic_sheet.py`.
2. Verified live GPT news watches from the GPT news-account ledgers.
3. Timestamped monitor news contexts.
4. Historical significant-move and live movement ledgers.

The result is one pair-aware, timestamp-aware contract used by research and the
v5.24 all-pair GPT practice manager.

News tags are evidence. They do not authorize an order or bypass price
confirmation, spread, stop geometry, expectancy, exposure, margin, or account
guardrails.

## Current Build

Generated 2026-07-27 UTC:

- Canonical verified events: 37
- Curated seed events: 15
- Live GPT watch events: 20
- Monitor-context events: 2
- OANDA instruments with explicit context rows: 68
- Pair/event relevance tags: 2,347
- Significant movements classified: 7,048 across all 68 pairs
- Significant movements time/pair matched: 1,110
- Significant movements explicitly unmatched: 5,938
- Recent live movement rows selected: 24,997
- Recent live movements after stable-key deduplication: 10,165
- Recent live movements time/pair matched: 9,021

The 2,347 pair/event rows are intentionally not `37 x 68`. Global curated
events expand to all pairs, while currency-scoped live watches expand only to
relevant pairs. Every instrument still receives a current context row,
including an explicit zero-event row when no event is active.

## Time Discipline

Each event stores:

- `event_utc`: when the event occurred.
- `first_known_utc`: when the information was first available to the system.
- `active_from_utc` and `active_until_utc`: the eligible matching window.
- source, verification, severity, affected currencies, pair hints, and
  directional hypotheses.

A movement cannot match an event whose `first_known_utc` is later than the end
of that movement. This prevents after-the-fact headlines from being treated as
causal forecasting evidence.

## Live Integration

`oanda_forex_gpt_advisor_all_pairs_v5_24_movement_ledger.py`:

- refreshes the canonical catalog at most every five minutes;
- adds pair-specific `news_context` to every market snapshot;
- exposes a compact top-level news-context manifest to GPT; and
- appends each detected movement's event matches to JSONL and SQLite.

`oanda_gpt_prod_live_account_manager.py` refreshes the same canonical catalog
after each successful verified news-watch scan. Experimental and formula83
practice managers inherit this behavior.

## Outputs

Canonical outputs are under:

`data/oanda_training_manager/news_event_tags`

- `events_latest.json` and `.csv`
- `pair_event_tags_latest.json` and `.csv`
- `latest_pair_news_context.json`
- `news_event_tags.sqlite`
- `significant_move_news_tags.csv` and `.parquet`
- `significant_move_news_links.csv` and `.parquet`
- `market_movement_news_tags.csv`
- `live_movement_news_tags.jsonl`
- `manifest.json`
- `run_latest.json`
- `ALL_PAIR_NEWS_EVENT_TAGGING_LATEST.md`

The legacy consumer path is rebuilt from the same contract:

`data/significant_moves/event_links/move_event_links.csv`

## Scale Policy

The 7,048-row significant-move catalog is backfilled exhaustively. The shared
high-frequency `market_movements.csv` is over 1.6 GB and is actively appended,
so a canonical run reads a bounded 25,000-row tail and deduplicates by stable
movement key. New movements are tagged incrementally at write time. This avoids
loading a growing multi-gigabyte CSV into memory and preserves current live
coverage.

## Rebuild

```powershell
python D:\forex\trad\oanda_news_event_tagger.py --movement-tail-rows 25000
```

Focused validation:

```powershell
python -m pytest -q `
  D:\forex\trad\test_oanda_news_event_tagger.py `
  D:\forex\trad\test_gpt_v5_24_account_wiring.py `
  D:\forex\trad\test_gpt_live_news_watch.py
```

## Vault Checkpoint

Because D has a confirmed bad-block history, do not run a full-tree vault hash
for this feature. Use the bounded exact-file checkpoint:

```powershell
python D:\forex\trad\forex_model_vault_sync.py `
  --root D:\forex `
  --interval-sec 0 `
  --news-event-only `
  --destination C:\Users\zmoor\OneDrive\thevault\projects\forex
```

The targeted package includes source, config, tests, docs, SQLite/catalog
metadata, all significant-move results, and the bounded recent live result. It
excludes credentials and the 1.6 GB rolling raw movement ledger.
