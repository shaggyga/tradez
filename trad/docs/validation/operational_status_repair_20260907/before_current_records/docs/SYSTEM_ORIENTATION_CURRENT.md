# Forex System Orientation — Current

Architecture reference originally written 2026-08-13. For the current stopped
runtime, source lineages and vault records read `../FOREX_AUDIT_START_HERE.md`
and `AUDIT_STATE_CURRENT.md` (September 4/5 reset). Older runtime labels below
are historical reference.

## Source of truth

- Canonical source and runtime: `C:\Users\zmoor\Documents\forex\trad`
- Runtime data: `C:\Users\zmoor\Documents\forex\trad\data\oanda_training_manager`
- Compact shared progress: `C:\Users\zmoor\OneDrive\thevault\projects\SHARED_BRAIN`
- Practice execution scope: `101-001-37981792-007`
- Real-money execution: disabled
- Supported lifecycle decision: `no_trade`

Older references to `D:\forex\trad` are historical. They must not be used as
live truth or as a deployment source.

## Control flow

```text
official/market/vendor sources
            |
            v
 point-in-time ingestion ---> immutable raw/revision records
            |
            v
 causal shared features ---> frozen research hypotheses
            |                         |
            |                         v
            +----------------> immutable forecasts
                                      |
                                      v
                              matured outcomes
                                      |
                                      v
                         independent verification
                                      |
                                      v
                    collecting / retired / confirmed
                                      |
                                      v
                    narrow Practice-007 authorization
                                      |
                                      v
                              practice execution
```

The arrows are one-way authority boundaries. Research cannot promote. Evidence
cannot authorize. Reports cannot mutate state. Execution cannot reinterpret a
lifecycle decision.

## Reorientation

The repository accumulated hundreds of flat root scripts. Physical movement is
being performed compatibility-first:

1. Assign every live and historical entrypoint to an owning domain.
2. Extract pure contracts and utilities behind stable imports.
3. Move read-only ingestion and monitoring first.
4. Move research and evidence only with replay-equivalence tests.
5. Move governance and execution last.
6. Archive an old root path only after no supervisor, importer, test, or runbook
   references it.

The target domains are under `src/forex_system/`. The controlling contract is
`config/project_layout_v1.json`. `forex_structure_audit.py` inventories current
ownership and supervised worker identities without touching broker state.

## Current research position

Breadth is no longer considered evidence. Current and future promotion depends
on immutable prospective cohorts, effective independent sample size, market
episode and signed-currency-factor deduplication, sequential inference,
multiplicity control, economic effect size, cost/concentration stress, and an
untouched confirmation cohort.

New source populations—official releases, ALFRED vintages, CFTC positioning,
GDELT, Alpha Vantage, Finnhub, rates, and future causal consensus—remain
separate. They are tested through placebos and incremental-value ablations and
are not flattened into a generic sentiment score.

## Operational rules

- Preserve forecast-time and first-known timestamps.
- Preserve immutable evidence and source revisions.
- Never count syndicated articles or correlated currency pairs as independent
  confirmation.
- Never use an aggregator as a substitute for an official release.
- Keep provider scores separate from local directional interpretation.
- Do not reactivate a retired hypothesis without materially new information and
  a new cohort ID.
- Keep all new research shadow-only until governed confirmation.
- Any structural migration must pass focused tests and the structure audit.
