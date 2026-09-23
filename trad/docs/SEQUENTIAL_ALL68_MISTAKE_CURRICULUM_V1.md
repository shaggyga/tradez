# Sequential All-68 Mistake Curriculum V1

Status: independently verified historical training curriculum; research-only,
nonexecuting, proof-ineligible, promotion-ineligible, and `no_trade`.

## Frozen source and identity

The final curriculum is bound to the immutable all-68 replay cohort
`sequential_all68_portfolio_batch_replay_v1.efda27295d5107241262` and source
pack `sequential_replay_source_pack_v1.b9d1526f3dfa057bd06d`.

Final curriculum cohort:
`sequential_all68_mistake_curriculum_v1.26b13486af3803244125`

Final report ID:
`a68mistakecurriculum_a3b4d3a281c74afcbb245816f572`

The former `98755c2c2f9bd26b014a` cohort remains preserved; the successor binds
the canonical-gzip replay identity without changing the curriculum result.

The contract binds the exact raw hashes and semantic hashes of source state and
clean verifier receipt, the source material identity, every source dataset
specification and aggregate dataset root, and the exact source-pack manifest
and verifier receipt. The common generated timestamp is derived from the
maximum predeclared feedback epoch (`2026-08-28T16:00:00+00:00`), not wall
clock time.

## Curriculum census

- 144 primary global-clock feedback rows, each with curriculum weight 1;
- 172 depth-one alternatives/reviews, each with weight 0;
- 316 total curriculum rows;
- 58 primary clocks with at least one material mistake;
- 164 nonexclusive mistake labels;
- 175 within-clock connected currency-resource components;
- 21 category × predeclared-session × transitive connected-currency-resource
  structural clusters;
- 144 total deduplicated primary weight;
- three predeclared session episodes;
- independent regime count remains unknown.

At the frozen one-pip material-regret threshold, the nonexclusive categories
are:

| Category | Labels | Materiality (pips) |
|---|---:|---:|
| Cost | 40 | 166.40 |
| Calibration | 29 | 114.50 |
| Opportunity selection / hold versus rotate | 28 | 95.75 |
| Rotation | 22 | 93.30 |
| Entry | 18 | 88.45 |
| Direction | 14 | 76.15 |
| Management / exit | 13 | 41.90 |

These overlapping labels are diagnostic curriculum dimensions. Their counts
and materialities must not be summed as independent evidence or interpreted as
causal attribution.

## Deduplication and quarantine

Primary feedback is first deduplicated at its global clock. Within a clock,
signed base/quote exposures are collapsed through transitive connected
currency resources, so opposing directions that express the same underlying
currency factor cannot inflate repetition. Structural review clusters are
then formed by category, predeclared session episode, and connected currency
resource.

Depth-one alternatives are retained for local review but have zero curriculum,
market-repetition, and regime-repetition weight. They are not a global optimum.
Pair contexts and reruns also do not create independent repetitions. The three
structural sessions are not asserted to be independent regimes.

All artifacts permanently retain:

- `research_only=true`;
- `evidence_role=historical_training_curriculum`;
- proof, execution, promotion, authorization, broker, and account access false;
- `supported_decision=no_trade`.

## Independent verification

The standalone verifier imports neither the curriculum producer nor its core.
It independently validates the source receipt, raw hashes, semantic hashes,
source-pack hashes, dataset specifications, compressed payloads, row hashes,
ordered/set roots, cross-table relations, and safety contract. It then
reconstructs every primary/review row, all seven mistake label families,
within-clock connected-resource components, structural clusters, summaries,
report content hash, report ID, material contract, and cohort ID.

Focused producer, verifier, immutability, source-rebinding, tamper, and research
isolation tests pass. Repeated builds must reproduce the same immutable report,
material contract, and verifier receipt bytes.

## Preserved superseded cohorts

No earlier artifact was overwritten or deleted:

- all-68 replay `sequential_all68_portfolio_batch_replay_v1.7681e61bd31ac8877bd8`
  is the immediate pre-timestamp-hardening source cohort;
- curriculum `sequential_all68_mistake_curriculum_v1.283926823bce8ac804d7`
  is the initial curriculum against that source;
- curriculum `sequential_all68_mistake_curriculum_v1.7712eb2214e1ebfe8013`
  is the intermediate rebound before exact raw source state/receipt hashes were
  added to the final contract.

The final curriculum is a mistake-directed training artifact only. It cannot
promote a policy, authorize an order, write to the live signal feed, or access
a broker/account.
