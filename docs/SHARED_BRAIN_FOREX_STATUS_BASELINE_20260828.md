# Forex — Shared Status

Updated: 2026-08-28T02:43:10Z

- Project ID: `forex`
- Canonical workspace: `C:\Users\zmoor\Documents\forex\trad`
- Operational environment: OANDA practice only
- Execution account: `101-001-37981792-007` (Practice 007)
- Supported decision: `no_trade`
- Lifecycle state: zero confirmed candidates; promotion, order placement, and
  real-money routing remain disabled.
- Account checkpoint: flat, NAV/balance `$41.6042`, zero open trades, and zero
  pending orders at 2026-08-28T02:41:32Z.

## Current coverage

- OANDA execution truth: fresh executable quotes for 68/68 instruments.
- Direct official mapping: 21/21 currencies and both legs of 68/68 pairs.
- Official source-depth grid: 21 currencies × eight event families = 168/168
  configured and operational transport cells; 163 are currently healthy.
- Depth beyond transport: 43/168 cells have an exact-family structured parser,
  13/168 have emitted exact numeric observations, and 14/168 have prospective
  causal observations. No cell yet has a prospective matured causal response or
  prospective semantic direction.
- Broad source registry: 186 configured definitions, 187 runtime-observed source
  identities, and 179 operational sources at the latest integrity checkpoint.
  These counts describe transport and observation coverage, not independent
  predictors or proven edge.

## Active source and feature families

- Ground truth: OANDA executable bid/ask, spread, and completed candles.
- Causal context: official central-bank, statistical, fiscal/debt, trade,
  labour, inflation, growth, intervention/reserve, and market-rate sources with
  explicit publisher/first-seen/revision clocks.
- Research-only enrichment: continuous narrative/news state, ALFRED vintages,
  CFTC positioning, GDELT, Alpha Vantage, Finnhub discovery, official daily
  rates, and U.S. Treasury yields.
- Active research features include cross-currency strength/residuals,
  factor/episode deduplication, executable-cost outcomes, narrative state, and
  the clean prospective causal level-band geometry.
- The causal source-response map now has a distinct V2 proof cohort with exact
  1/5/10/15/30/60/120-minute responses. A V2-only currency-rank adapter
  compares price-only, source-only, and source-plus-price timing; a separate
  H15 worker tests already-known news direction only after frozen-band
  resolution and completed quote-flow confirmation.
- Unavailable order-book, position-book, and pricing-depth fields remain inert;
  registered zero-output model-gap contributors are dormant and do not count as
  active breadth.

## Runtime and governed findings

- `hgb_live_outcomes` is retired because it consumed a stale legacy-account
  input and had no pending work.
- `manager_decision_outcome_ledger` is retired because its three-row source was
  frozen and fully matured.
- `one_hour_shadow_signal` is in `mature_only` drain mode: it creates no new H1
  forecasts while preserving and resolving every previously issued horizon.
  Its four baseline families were consistently negative after costs and
  produced no holdout candidate; the retained series is a negative control.
- `executable_opportunity_prospective` is also in `mature_only` drain mode after
  169,407 historical forecasts produced zero directional gate passes. It cannot
  add forecasts, promote a lane, or authorize execution.
- The news/technical watchlist no longer queries that retired opportunity
  ledger or exposes stale magnitude rankings as current input.
- The clean level-band cohort
  `level_band_prospective_20260827a.9294e3511aae455f` is collecting under
  geometry contract `level_band_contract_v2_frozen_20260827b`. It is
  research-only and has no authorization, promotion, signal-feed, or broker
  surface.
- The latest project-integrity audit is `ok`; all execution and promotion gates
  remain fail-closed.
- The new source-response, source-rank, and news/band/flow ledgers are
  append-only and integrity-clean. Their current prospective proof, rank
  decision, and timing-candidate counts are all zero; this is an evidence null,
  not an operational failure.
- Storage is safe: C: had 120.454 GiB free (12.94%) at
  2026-08-28T02:39:17Z. Old runtime logs and temporary derivatives were moved
  into a manifest-backed local archive without deleting causal evidence.

## Current objective and next gates

Accumulate independent prospective evidence and test genuinely orthogonal
information after executable costs. Transport breadth alone is not evidence of
direction. The next valid gates remain causal pre-release consensus and
event-time rate/OIS repricing, matured official-source response episodes,
factor/episode-deduplicated level-band outcomes, untouched confirmation, and a
narrow Practice-007 canary only after a real `confirmed_candidate` exists.

Canonical references:

- Pending/evidence queue:
  `C:\Users\zmoor\Documents\forex\trad\FOREX_PENDING_IMPROVEMENTS.md`
- Project history:
  `C:\Users\zmoor\Documents\forex\trad\FOREX_PROJECT_LOG.md`
- Runtime retirement contract:
  `C:\Users\zmoor\Documents\forex\trad\config\shadow_runtime_retirements_v1.json`
- Current project-integrity state:
  `C:\Users\zmoor\Documents\forex\trad\data\oanda_training_manager\state\project_integrity_audit_v1.json`
- Current source-depth report:
  `C:\Users\zmoor\Documents\forex\trad\data\oanda_training_manager\reports\official_currency_source_depth_readiness_v2\OFFICIAL_CURRENCY_SOURCE_DEPTH_READINESS_V2_CURRENT.json`
- Current orientation:
  `C:\Users\zmoor\Documents\forex\trad\docs\SYSTEM_ORIENTATION_CURRENT.md`
