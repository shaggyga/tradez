# Sequential Portfolio Mistake Curriculum V1

This read-only research sidecar converts the independently verified Sequential
Portfolio Replay V1 feedback into a compact training curriculum. It does not
change the replay ledger, publish a signal, alter lifecycle state, authorize a
canary, access an account, or place an order.

The frozen categories are direction, entry, management, exit, rotation, cost
awareness, calibration, and opportunity selection. Categories are intentionally
nonexclusive: one bad entry may reveal both a direction and a calibration
mistake. Accordingly, category counts must never be summed as independent
errors.

To reduce repetition, observations within a category and predeclared market
episode are connected transitively when they share a currency resource or a
position thesis. Reports retain both raw observations and those structural
clusters. A structural cluster is a curriculum unit, not an independent market
regime or proof sample.

The sidecar requires a frozen config, the canonical replay state pointer, a
clean upstream verifier receipt, independently reconstructed source roots, and
the immutable database session seal. It opens SQLite in read-only/query-only
mode. The config, producer, core, standalone verifier, source state, source
receipt, source database, source seal, roots, and classification threshold are
all bound into a material-contract hash.

Each material contract receives a new content-addressed cohort directory under
`sequential_portfolio_mistake_curriculum_v1/cohorts/`. Cohort artifacts are
write-once: an existing path may be reused only when its bytes are identical.
The old source-cohort-keyed report remains preserved as a pre-hardening
diagnostic and is not silently overwritten.

The independent verifier does not import the producer or classification core.
It snapshots the sealed replay database read-only, checks the replay receipt,
database hash, session seal and roots, then independently rebuilds every
observation, structural cluster, category statistic, report content hash, and
cohort ID. Only a clean receipt is published through the mutable `current`
pointers.

Run:

```powershell
python oanda_sequential_portfolio_mistake_curriculum.py
python oanda_sequential_portfolio_mistake_curriculum_verifier.py
python -m pytest -q test_oanda_sequential_portfolio_mistake_curriculum.py
```

The material-regret threshold is config-bound. It is no longer a mutable CLI
switch; a threshold or other material config change necessarily creates a new
cohort and cannot reuse an earlier verification receipt.

The output is historical training/discovery evidence only. Depth-one
counterfactuals diagnose local choices; they do not reveal a globally optimal
portfolio path and cannot establish edge.
