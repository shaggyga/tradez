# CurrencyState after-cost counterfactual V6 proposal

Status: engineering proposal for independent review. V6 is not registered,
wired to a supervisor, enabled for operational inputs, or connected to order
execution.

## Scope and immutable ancestry

V6 is a new cohort (`currency_state_after_cost_counterfactual_cohort_20260817f`)
that supersedes V5 only as an engineering proposal. V3, V4, and V5 remain
unchanged and are pinned as external ancestry in the V6 manifest. The
economics, arm set, 68-pair universe, selection thresholds, account policy,
and hold/switch rules remain identical to V5.

The CLI retains a hard-coded canonical genealogy path and has no genealogy or
identity-registry path parameter. Every nonempty operational envelope is
unconditionally rejected. A zero-input publication must have 2,720 `no_trade`
rows and zero submitted evidence, economics-admissible rows, ranks,
selections, allocations, or holds.

## Identity-registry remediation

Before opening the registry, V6 derives the complete candidate-key set for a
submitted batch. Any repeated `(namespace, identity_kind, identity_value)` is
rejected, including byte-identical duplicates. An equivocal repeated key is
also rejected. No binding, replay, observation, or conflict row can be
inserted during this first-batch rejection, so input order cannot select a
winner.

Payload semantic hashes sort records by stable identity/hash and omit the
self-referential envelope hash. Thus reversing distinct records preserves the
semantic submission and replay binding, while raw submission hashes retain
the actually observed ordering. Record content changes still change the
semantic binding and fail closed.

The V6 registry is a new file and schema. Every admission, including an
identical replay, verifies:

- the exact normalized `sqlite_schema` SQL for all five tables, six indexes,
  and ten append-only triggers;
- exact table columns, types, nullability, primary keys, unique-index origin,
  and index columns;
- all SQL `CHECK` definitions and database integrity;
- one exact metadata row binding schema, contract, cohort, manifest ID,
  manifest raw-file SHA-256, manifest semantic SHA-256, schema SQL SHA-256,
  and cohort creation time.

Opening a missing registry uses SQLite `mode=rw` only after a non-creating
regular-file check. Initialization builds and validates a fully committed
database in a unique same-directory temporary file, fsyncs it, and atomically
installs it with a no-clobber hard link. Failure and concurrent losers remove
only their own temporary files. An interrupted process can leave an inert
temporary file, but never a partially initialized canonical target; retry is
safe.

## Canonical genealogy remediation

V6 performs a read-only, non-creating check of the hard-pinned canonical
genealogy. Trust requires the exact three-table, one-index, four-trigger SQL
definition used by `research_genealogy_v1`, including the `hypothesis_id`
primary key/unique index and exact immutable trigger bodies. It then requires
exactly one row for the V6 hypothesis, selects all 23 columns in canonical
order, proves the definition JSON is canonical, recomputes its SHA-256, and
compares every value with the frozen V6 registrar definition.

This proposal does not insert that row. Until a separate independent review
and registration action occur, the expected state is
`v6_genealogy_hypothesis_cardinality_not_exactly_one` and the output remains
engineering-blocked.

## Review artifacts and tests

The frozen V6 manifest pins the V6 core, registrar, contract, CLI, this
proposal, the adversarial test module, and every inherited artifact. The test
suite covers first-batch duplicate/equivocal keys in both orders, benign record
reversal, inert same-name registry and genealogy triggers, same-ID changed
manifests, exact genealogy schema/PK/cardinality/row checks, missing-path
noncreation, interrupted initialization and retry, concurrent initialization,
crash-orphan retry, immutable ancestry hashes, unconditional CLI blocking, and
zero-evidence/no-trade invariants.

No canonical identity database or genealogy row is created by the proposal or
normal zero-input publisher. Tests use only temporary paths.
