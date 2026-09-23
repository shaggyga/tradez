# Significant Moves: durable two-hour research catalog

This subsystem maintains a reproducible catalog of unusually large FOREX price movements centered on a two-hour window. It is intentionally narrower than a general swing or multi-week trend engine: the default horizons are **90, 120, and 150 minutes**, with 120 minutes treated as the primary horizon.

The catalog is designed to answer retrospective questions such as:

- What were the largest roughly two-hour moves in EUR_USD?
- Which pairs produced the most major moves in a month?
- Were recent JPY moves concentrated in a particular session?
- How did an event rank in pips, volatility-adjusted magnitude, cost, or path quality?

It is a permanent research dataset, not a trading signal service.

## Configuration

The checked-in configuration is [`config/significant_moves_2h.json`](config/significant_moves_2h.json). Important defaults include:

- Primary horizon: 120 minutes
- Scanned horizons: 90, 120, and 150 minutes
- Five-minute analysis bars and five-minute scan stride
- Broad per-instrument percentile: 99.0%
- Strict per-instrument percentile: 99.5%
- Strict minimum move: 1.5 ATR units and three times estimated trading cost
- Auditable overlap grouping and strict non-overlap selection
- Pair-level checkpointing under the stable output root

Change research behavior through the JSON file rather than editing constants in the pipeline. A configuration change invalidates affected checkpoints so results can be reproduced from the new settings.

Paths in the configuration are rooted at `trad/`. The default raw source is:

```text
trad/data/oanda_training_manager/candles/*_M1.csv
```

The instrument universe is discovered from those files on every run; it is not hard-coded to 68 pairs. Where available, BAM bid/ask overlays from `trad/data/oanda_training_manager/candles_bam/` are used. Every discovered pair is represented in the inventory, including skipped pairs and their skip reasons.

## Build or update the catalog

From the project root:

```powershell
python trad/significant_moves_pipeline.py --config trad/config/significant_moves_2h.json
```

Use the project research Python when it differs from the system interpreter. The pipeline is safe to rerun as new M1 rows arrive. Pair checkpoints identify unchanged inputs, while a changed or newly appended source file is rescanned without requiring a full-universe restart. Interrupted runs retain completed pair work.

To force a particular pair to rebuild, use `--instrument EUR_USD --force`. The refreshed pair is merged with all other current universe checkpoints before global files are rewritten; the production root refuses a partial assembly if any required checkpoint is missing or stale. Use `--output-root trad/data/significant_moves_smoke` for intentionally partial smoke runs. Keep the raw M1 source files immutable while a run is active.

When detector semantics or per-pair output fields change in code, increment `pipeline_version` in the configuration. Reporting- or assembly-only code changes intentionally reuse compatible pair checkpoints.

## Stable output layout

The configured root is `trad/data/significant_moves/`:

```text
data/significant_moves/
|-- significant_moves.sqlite        # queryable durable catalog
|-- instrument_inventory.csv        # discovered, usable, and skipped instruments
|-- data_quality_report.csv         # ranges, counts, missing bars, and warnings
|-- currency_strength_context.parquet
|-- raw_candidates/                 # overlapping broad candidates by pair
|-- deduplicated/                   # overlap groups and selection audit
|-- final/                          # strict non-overlapping CSV/Parquet results
|-- event_links/                    # enrichment-ready many-to-many event links
|-- checkpoints/                    # resumable per-pair state
|-- manifests/                      # configuration and source/run provenance
|-- cache/                          # reusable intermediate bars
`-- reports/                        # audit crosswalk and compact summary
```

The SQLite table queried by the companion CLI is `significant_moves`. Its canonical columns include:

```text
instrument, base_currency, quote_currency,
start_timestamp, end_timestamp, horizon_minutes,
direction, signed_pip_change, absolute_pip_change,
signed_percentage_return, absolute_percentage_return,
significance_score, start_session
```

Additional columns preserve prices, ATR/cost normalization, path quality, data-quality flags, overlap membership, and selection provenance. Raw overlapping candidates remain separate from the final strict catalog so deduplication decisions can be audited instead of silently discarding windows.

## Query the catalog

[`query_significant_moves.py`](query_significant_moves.py) reads SQLite in query-only mode and writes results to standard output. Its default database is `trad/data/significant_moves/significant_moves.sqlite`; use `--db` to inspect another compatible catalog.

Highest-scoring upward EUR_USD moves (with a 50-pip floor):

```powershell
python trad/query_significant_moves.py --instrument EUR_USD --direction up --min-pips 50 --top 20
```

JPY moves in a date range, requiring a minimum score:

```powershell
python trad/query_significant_moves.py --currency JPY --start 2025-01-01 --end 2025-12-31 --min-score 70 --top 100 --output csv
```

London-session downward moves as JSON:

```powershell
python trad/query_significant_moves.py --direction down --session london --output json
```

Summaries by instrument or currency:

```powershell
python trad/query_significant_moves.py --group-by instrument --top 68
python trad/query_significant_moves.py --group-by currency --top 20
```

Time and session summaries:

```powershell
python trad/query_significant_moves.py --group-by year --output csv
python trad/query_significant_moves.py --group-by month --top 36
python trad/query_significant_moves.py --group-by session --output json
```

`--start` and `--end` constrain the move's start timestamp. A date-only `--end`, such as `2025-12-31`, includes that entire UTC date. Currency grouping counts each move once for its base currency and once for its quote currency. `up`/`long` and `down`/`short` are accepted as direction synonyms.

Without `--group-by`, CSV and JSON output contain every selected catalog column. Grouped output contains the documented summary aggregates. Interactive table output uses a compact set of key columns; use CSV or JSON when downstream tooling needs every field.

## Interpretation and safety warning

**All detected moves, their boundaries, their rankings, and their selection scores are retrospective, hindsight-derived research labels. They are not predictions, live entry signals, or evidence that the move could have been identified at its recorded start.**

In particular:

- A window is labeled only after its endpoint and in-window path are known.
- Percentile and volatility thresholds describe historical rarity; they do not establish future profitability.
- The strict non-overlap selector chooses a clean research representation after candidates are observed.
- Favorable excursion, adverse excursion, path efficiency, and similar whole-window metrics contain future information relative to the move start.
- Rankings can be distorted by changing volatility, liquidity, spread coverage, missing bars, and the length of available history.
- Transaction-cost estimates are approximations and do not reproduce every fill, financing charge, rejection, margin constraint, or market gap.
- Replaying exact starts or ends is an oracle upper-bound scenario unless a separate causal strategy generates those timestamps from information then available.

Do not train or validate a predictive model by placing whole-window fields into pre-move features. Any later modeling must use timestamp-aligned causal snapshots, purged time splits, an embargo at least as long as the outcome horizon, and genuinely out-of-sample evaluation after spread, slippage, financing, and margin constraints.

This dataset answers **what happened**. A separate causal research process is required to claim that a move could have been forecast, entered, or monetized.

## Live move alerts and missed-move errors

[`oanda_move_alert_monitor.py`](oanda_move_alert_monitor.py) is the causal live
companion to this retrospective catalog. It reads the practice quote snapshot
and consolidated signal surface, but it has no broker client, credential
argument, or order path.

The monitor:

- builds bounded one-minute bid/ask bars for 5, 15, 30, and 60-minute moves;
- activates those horizons progressively after restart as live minute history
  becomes available, avoiding expensive startup scans of fragmented depth data;
- measures executable long or short movement after the observed spread;
- deduplicates overlapping horizons into move episodes;
- clusters related pairs into common-currency shocks;
- attributes a catch or miss only from forecasts timestamped at or before the
  move start;
- distinguishes gate suppression, weak/neutral forecasts, wrong direction,
  late reactions, and missing historical attribution;
- publishes continuation and reversal state terms to the contribution matrix
  as prospective shadow research with `account_eligible=false`.

Canonical operator and future-chat files are:

```text
data/significant_moves/live_alerts/MOVE_ALERT_LATEST.md
data/significant_moves/live_alerts/MISSED_MOVES_LATEST.md
data/significant_moves/live_alerts/MOVE_ALERT_LATEST.json
data/significant_moves/live_alerts/MISSED_MOVES_LATEST.json
data/significant_moves/live_alerts/MOVE_STATE_TERMS_LATEST.json
data/significant_moves/live_alerts/move_alerts_v1.sqlite
```

The stable heartbeat is:

```text
data/oanda_training_manager/state/move_alert_monitor_v1.json
```

Run one diagnostic cycle without contribution-feed publication:

```powershell
python trad/oanda_move_alert_monitor.py --once
```

The always-on supervisor runs the monitor at below-normal priority and requests
research-only term publication. Publication does not alter account eligibility
and cannot place a trade.
