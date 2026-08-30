# Forex pending improvements

Status: **Stable; paired official-event proof is live and the executable-move census is staged for prospective collection**

Updated: 2026-08-30 16:39 America/New_York

## All-68 executable-move census and dashboard cleanup — 30 August 16:39 ET

- The dashboard's former velocity leaderboard is replaced by a prospective
  executable-move census. Every scheduled minute freezes one exact all-68
  bid/ask frame and evaluates both LONG and SHORT for all 68 pairs at fixed
  1, 5, 10, 15, 30 and 60-minute horizons. Each horizon therefore retains an
  explicit 136-side denominator, including not-cleared, stale, missing,
  invalid and closed-market observations.
- A move clears only when the terminal executable exit is positive after the
  entry/exit bid/ask endpoints and one frozen 0.25-pip round-trip slippage
  deduction. Descriptive first-clear and best/worst path fields remain
  separate from the terminal result. Opportunity is not forecast skill and
  cannot confer lifecycle, promotion, authorization or order eligibility.
- Compact immutable quote frames and per-window terminal/path digests avoid
  materializing millions of duplicate arm rows. Every logical side remains
  reconstructible. Review cases are deduplicated by 15-minute market episode,
  signed currency factor and terminal-versus-path event kind so correlated
  pair expressions do not masquerade as independent misses.
- A standalone verifier imports no producer, has no network or execution
  surface, independently rebuilds all 136 logical sides, checks append-only
  schema and lineage, and uses a hash-pinned verified-prefix checkpoint for
  bounded 30-second operation. A malformed or hash-mismatched checkpoint is
  discarded and forces a full audit.
- The main dashboard now leads with account state, the decision board and the
  fixed executable census. The signal prediction matrix defaults to the live
  forecast and labels chronological after-cost holdout evidence separately.
  Large proof and research surfaces are collapsed into distinct sections;
  legacy velocity/midpoint payload modes remain compatibility-only and are no
  longer selectable in the primary interface.
- Focused producer tests pass 16/16 on Python 3.12 and 3.13; independent
  verifier tests pass 24/24 on both runtimes; dashboard tests pass 56/56 on
  both runtimes. Python compilation and PowerShell parsing are clean. These
  are correctness results, not evidence of edge.

Remaining closeout steps are a controlled supervisor/dashboard reload, first
prospective-frame verification after the Sunday 17:05 ET market-open clock,
and the final source/model vault refresh. Existing external blockers—causal
pre-release consensus, intraday policy-rate repricing and a permitted direct
RBNZ channel—remain unchanged. Practice 007 stays governed and `no_trade`.

## Paired proof Windows publication repair and cohort B — 30 August 15:08 ET

- The final live audit caught one real operational failure: the independent
  paired verifier exited when Windows temporarily denied `os.replace` while
  publishing its JSON heartbeat. The supervisor restarted it and the
  append-only evidence database remained intact, but a recurring transient
  denial could have caused avoidable verifier churn.
- Producer and verifier now share one frozen bounded publication policy:
  retain the same temporary file, retry `PermissionError` up to eight times
  with 0.01-to-0.50-second exponential waits, re-raise a persistent failure,
  preserve the prior destination and always clean the temporary file.
- Cohort A is preserved in `official_event_paired_evaluator_v1.sqlite` with
  one immutable manifest and zero evidence rows. Because the producer source
  changed, its manifest was not edited and its rows were not relabeled.
  Cohort B (`official_event_paired_evaluator_v1_20260830b`) activates at
  `2026-08-30T19:00:00Z` in a separate database and separate producer and
  verifier state/heartbeat paths.
- B freezes config SHA-256 `d553877ba5140e31c5eb42233761bf5923cae4b44e601325fc776dab6b2e50e1`,
  normalized producer SHA-256
  `ea3374e4bab29dfc2f5703853927b14d119b40c2394bc7050fab8045f42073d7`,
  literal producer SHA-256
  `eeccb9679d29bedafa9696c8923dbab3cad45b2616c773a0b9895e816ccd08ed`
  and independent verifier SHA-256
  `96f0b588f39f6f8d0bacbdb3615aa1529c00d4d9ccae95739853e32b1aa8a476`.
- The isolated B startup and independent verifier pass with zero evidence,
  exactly as expected while the market is closed. Paired tests pass 57/57
  under Python 3.12 and 3.13; the broader official-release boundary passes
  125/125 under both runtimes and supervisor freshness/path tests pass 4/4.
  Commit `5de597f0701bd6ad14415856ffc7257d989fa88c` passed a 1,225-file staged
  credential audit with zero findings. Hidden supervisor PID 30724 adopted
  the existing workers and launched only cohort B producer PID 8232 and
  verifier PID 33308. Both are fresh; producer reports `ok`, verifier reports
  `verified`, both error logs are empty, and A/B databases independently pass
  `integrity_check` with one exact manifest and zero evidence rows.
- Final source/model vault publication from the clean post-reload record is
  the remaining housekeeping step. This is operational hardening only. It
  does not change the hypothesis,
  retune an arm, import history, establish edge or alter `no_trade`.

## Paired event-to-executable-quote proof V1 cohort A baseline — 30 August 14:05 ET

The audit's next repository-controlled build is complete and frozen. It adds
measurement discipline, not a trading authorization or a claim of edge.

- Contract `official_event_paired_evaluator_v1_append_only_20260830` and
  cohort `official_event_paired_evaluator_v1_20260830a` require one decision
  or explicit terminal invalid/abstention record for every eligible untouched
  official event. The complete decision and 75-row arm/horizon/cost schedule
  must be sealed before the one-minute outcome exists.
- The raw collector, exact source contract/cohort, V151 mapping,
  source-authority map, news-source configuration, upstream producer bytes,
  numeric timing limits, paired config and producer identity are frozen. Raw
  capture latency, quote clocks, mapping availability, write-lock timing,
  horizon attempt clocks, snapshot age, quote age and target offsets are
  independently rederived rather than accepted from quality labels.
- One outcome-blind issuer pair is selected by the lowest event-T0 executable
  spread with a lexicographic tie break. The frozen arms are official source,
  price-only timing, official plus technical confirmation, flipped official
  control and no-trade. All share the same pair, entry quote, horizon and cost
  assumptions. Technical state may confirm or veto; it cannot reverse or
  invent the official economic direction.
- Long economics use event ask to horizon bid and short economics use event
  bid to horizon ask. Those endpoints embed spread exactly once; only the
  separately declared 0.00/0.25/0.50-pip round-trip slippage stress is
  subtracted. The T0 entry is explicitly a research counterfactual and is not
  described as proof of pre-semantic order submission.
- A standalone read-only verifier imports no producer and reconstructs exact
  rows/bytes, clocks, issuer binding, deterministic pair choice, technical
  material, complete schedule, horizon freshness, arithmetic, dependence keys
  and append-only safety. The final paired/horizon boundary passed 86 tests
  under both Python 3.12 and 3.13; the broader source, governance, isolation,
  supervisor, vault and credential boundary passed 235 tests under each
  runtime. Independent adversarial review found no launch blocker.
- No draft evaluator was launched or allowed to create evidence. Commit
  `ee40350eb2b58ff7eb3eec3a0be0909b044802a2` was credential-audited and
  adopted by a controlled hidden supervisor reload at 13:59 ET. The producer
  reports `ok`, the independent verifier reports `verified` with zero
  failures, and all input databases pass `quick_check`. The append-only ledger
  passes `integrity_check` with one manifest and zero decisions, arms, horizon
  inputs or outcomes, which is the honest market-closed start with no backfill.
  Historical fixtures and Friday's Fed speech remain regression diagnostics
  and cannot enter this cohort.
- Explicit source-vault retention is now fail-closed and opt-in. The clean
  commit published as `forex_source_ee40350eb2b58ff7.zip` (SHA-256
  `e08d6cf998f2c2a9dd96efec1c76c4627abb21001ece09c87e83ff4a4de61e08`;
  ZIP CRC verified). Retention kept that exact immutable pair, retired 12
  superseded managed pairs, and removed only the 13 preidentified obsolete
  legacy source artifacts totaling 20,039,246 bytes. The latter have exact
  name/size/SHA-256 tombstones. The OneDrive-only model checkpoint then
  published 1,475 credential-free files and was independently verify-imported
  at SHA-256
  `9e2d73dc490aa3badfa6a5ddcba5041c8b1651a7e3a522d2f082d402a8c5d5b3`.
  No `D:` destination was used. The full shared vault is 4,982,249,381 bytes,
  below the 5,000,000,000-byte limit.
- A post-run independent review found two cleanup-order defects before
  closeout: the model sync still had an implicit stale-`D:` default, and its
  retention could unlink an older archive before proving that the new archive
  reconstructed. Both are repaired prospectively. The default is OneDrive
  only; every new model archive must pass CRC, safe-path, embedded-manifest,
  payload-hash and temporary full-reconstruction checks and publish a
  current-archive-hash-bound receipt before retention. Unchanged runs never
  prune. Source retention now publishes one verified write-once transaction
  containing exact filename/size/SHA-256 tombstones for every managed
  archive, manifest and legacy target; both families are re-inventoried and
  the final report is serialized before the sole unlink coordinator runs.
  Fault injection proved zero deletion on every planning, publication,
  readback, mutation, validation and reporting failure. The closing vault
  suite passed 48 tests under both Python 3.12 and 3.13.

Remaining gates are evidence or external access: untouched official events,
causally captured pre-release consensus, timestamp-safe intraday OIS/rates or
policy-futures repricing, and a publisher-permitted direct RBNZ channel. The
five-second precommit margin cannot prove commit completion through an extreme
storage stall, and later inference must additionally cluster cross-issuer
global shocks. These limits remain explicit and fail closed.

There are **no unimplemented repository-controlled changes** in this new
section. The active research direction is source-first, paid procurement
remains out of scope, and the supported execution decision remains
`no_trade`.

## Event-to-executable-quote proof V1 — 30 August 03:14 ET

The next source-priority implementation is now present as a separate,
prospective-only research cohort. Nothing in this checkpoint is a claim of
predictive edge.

- Frozen contract
  `official_event_quote_horizon_capture_v1_all68_append_only_20260830`
  and cohort `official_event_quote_horizon_capture_v1_20260830a` bind exact
  all-68 executable bid/ask attempts at 1, 5, 15, 30, and 60 minutes after an
  eligible raw official event. Entry quotes must come from the already frozen
  raw sidecar and are never reacquired.
- Each horizon records both quote-read start and quote-read completion clocks.
  Long economics use entry ask to horizon bid; short economics use entry bid
  to horizon ask. Recorded spreads are already embedded once in those
  executable endpoints, while modeled slippage remains a separate stress.
- Every event/horizon has one terminal attempt and no retry. Partial, stale,
  late, future-skewed, or wrong-generation snapshots retain an immutable
  invalid header with zero quote components; a later snapshot cannot repair
  the attempt.
- The collector is append-only, research-only, execution-ineligible,
  authorization-ineligible, promotion-ineligible, and fixed to `no_trade`.
  A standalone verifier implementation exists to rebuild source linkage,
  due-horizon completeness, hashes, clocks, arithmetic, exact 68/68 coverage,
  invalid zero-component behavior, safety, and trigger presence without
  importing the producer. The integrated source/governance boundary passed
  548 tests, the focused changed boundary passed 99 tests under Python 3.12,
  and the producer/verifier boundary passed 34 tests under both Python 3.12
  and 3.13. The live empty ledger independently verifies with zero failures.
- Zero eligible events and zero horizon captures are expected before the
  `2026-08-30T12:00:00Z` activation while the FX market is closed. Historical
  official events and Friday fixtures are not backfilled into this cohort.
- The three direct RBNZ surfaces that repeatedly return HTTP 403—OCR snapshot,
  wholesale interest rates, and official overseas reserves—are now explicitly
  `runtime_supported=false`. Their parsers and historical rows remain
  preserved. Restoration requires a publisher-permitted subscription,
  authenticated channel, or explicit access; polling must not bypass the
  publisher boundary.

The remaining source blockers are external: causally captured pre-release
consensus and timestamp-safe intraday OIS/rates or policy-futures repricing.
Unavailable values remain unavailable rather than inferred or backfilled.

## Current queue and evidence boundary — 30 August 00:48 ET

The queued replay, curriculum, verifier, policy-challenger, historical
expansion, lossless-genealogy, and cross-runtime reproducibility work is now
implemented. The sections below this one are chronological checkpoints, not a
second active implementation queue.

- Cross-runtime canonical serialization is fixed. Python 3.12 and 3.13 now
  emit the same deterministic gzip bytes, including a normalized `OS=255`
  header. All 702 gzip files in the rebuilt canonical chain pass that check.
- Current base chain: source pack
  `sequential_replay_source_pack_v1.b9d1526f3dfa057bd06d`, replay
  `sequential_all68_portfolio_batch_replay_v1.efda27295d5107241262`, mistake
  curriculum `sequential_all68_mistake_curriculum_v1.26b13486af3803244125`,
  and challenger `sequential_all68_policy_challenger_v1.0b5267c7a5c730a150cf`.
  The replay remains flat at **-120.25 pips** across 110 execution legs.
- Current Wednesday expansion chain: source pack
  `sequential_replay_source_pack_v1.554c8f74212202aa9b86`, replay
  `seq_a68_wed_exp_v1.e7de4ecdd0255eb13306`, and policy expansion
  `sequential_all68_policy_expansion_v1.15a3aa5d039df7df9a7d`. The frozen
  explicit/factor arm is +16.10 pips in-sample, but becomes **-36.85 pips**
  after removing its best Wednesday; the 2x-cost arm similarly falls from
  +1.35 to **-37.80 pips**. This is concentrated historical training evidence,
  not proof or a candidate for execution.
- The final combined boundary passed **208 tests** with three expected
  platform-dependent skips under both Python 3.12 and Python 3.13. All 36
  changed Python files compile under both runtimes. Independent
  current-pointer, material-hash, safety, flat-terminal, clean-temporary-tree,
  and current-ID checks passed.
- The canonical lossless genealogy now contains 53,169 definitions and 87,476
  observations after the supervised reload. The seven current rebuilt
  identities each have exactly one registered observation; SQLite integrity
  is `ok`, foreign-key violations are zero, and confirmed candidates remain
  zero. The exact 53,148-definition/123,820-observation predecessor registry
  is preserved under content-addressed snapshot
  `research_genealogy_predecessor_v1.a5d9b45584888df78f8d`; incompatible
  predecessor rows remain quarantined rather than discarded or rewritten.
- Every predecessor replay/curriculum/policy identity remains immutable. The
  current positive-looking expansion is not merged with the earlier window,
  promoted, authorized, or described as independent evidence.

No pending local code item authorizes trading. The remaining work is evidence
or external-source acquisition and therefore stays fail-closed:

1. Continue untouched prospective official-event and quote-sidecar
   collection after markets reopen.
2. Obtain a permitted causal pre-release consensus source and timestamp-safe
   intraday rate/OIS or policy-futures repricing. Keep unavailable fields
   unavailable rather than inferred or backfilled.
3. Restore the permitted direct RBNZ path through an authenticated official
   subscription or other publisher-authorized channel; do not bypass access
   controls.
4. Evaluate the frozen challenger/allocator only on a later untouched cohort.
   Any material feature, rule, data, cost, or serialization change requires a
   new cohort identity.
5. Advance to Practice 007 only if the immutable lifecycle database eventually
   contains a genuine confirmed candidate and a fresh exact canary
   authorization. Real-money routing remains disabled.

The current supported operational decision remains `no_trade`.

## Verifier hardening and policy checkpoint — 29 August 22:50 ET

An independent adversarial review found that several current receipts could
previously be made self-consistent after tampering. Those artifacts were not
promoted or executed, but the apparent verifier confidence was too weak. The
current source pack, all-68 replay, learner curriculum, and four-pair mistake
curriculum have therefore been rebuilt under stronger immutable contracts;
their predecessors remain preserved and superseded rather than rewritten.

- Exact-window source pack:
  `sequential_replay_source_pack_v1.ee6d6fd4d38744ecc1da`. The material
  contract now binds the complete no-execution safety state and the standalone
  verifier rejects forged state/material safety. Coverage remains 68 pairs,
  144 clocks, 9,792 contexts, and 8,333 fully ready contexts.
- All-68 replay:
  `sequential_all68_portfolio_batch_replay_v1.9d6e7e0f27ad289c4c83`. Its
  verifier now reconstructs the schedule, complete 68-by-clock Cartesian set,
  candidates, primary state chain, every execution leg, feedback,
  counterfactual, terminal row, dataset identity/order, safety, counts, and
  P/L. The unchanged historical result remains **-120.25 pips**, 110 legs,
  and a flat terminal portfolio. State, report, and verifier timestamps are
  now derived from the predeclared schedule, immutable cohort files are
  create-once, current pointers are exact byte copies, and an unchanged rerun
  must reproduce every byte or fail. The immediate predecessor `7681e6...`
  remains preserved after exposing the mutable-wall-clock defect.
- Learner curriculum:
  `sequential_portfolio_curriculum_v1.22124b7c2ebf24ad2430`. Its verifier now
  recomputes every published statistic, exact seal/snapshot payload, full
  safety state, and deterministic content-bound timestamp contract. It retains
  48 attempts, 36 distinct cases, 12 zero-weight reviews, and mean regret
  0.671875 pips.
- Four-pair mistake curriculum:
  `sequential_portfolio_mistake_curriculum_v1.188f47cfdf79c6608cde`. A new
  standalone verifier independently rebuilds all 34 overlapping observations
  and 21 structural clusters and rejects forged roots, counts, report IDs,
  thresholds, configs, or safety. The old fixed-name report remains preserved.
- All-68 mistake curriculum:
  `sequential_all68_mistake_curriculum_v1.98755c2c2f9bd26b014a`, report
  `a68mistakecurriculum_ce03dbb9ca777dacc82304c9b3f0`. It retains 144 weighted
  primary clocks and 172 zero-weight depth-one reviews, identifies 58 mistake
  clocks and 164 nonexclusive labels, and collapses them through 175
  within-clock currency-resource components into 21 structural clusters.
  Cost (166.40 pips), calibration (114.50), opportunity selection / hold versus
  rotate (95.75), rotation (93.30), entry (88.45), direction (76.15), and
  management/exit (41.90) are overlapping curriculum dimensions, not
  independent evidence. Exact raw and semantic parent receipts, source-pack
  hashes, dataset specifications, roots, and all research-only safety fields
  are independently bound. Initial `283926...` and intermediate `7712eb...`
  curricula remain preserved.

The unaffected source-pack/learner/four-pair/isolation boundary currently
passes 62 tests with one platform-permission symlink skip. The final all-68
replay/mistake/isolation boundary passes another 43 tests with zero failures.
A full project integrity refresh initially found only a
chronology-stale move-first news audit. The 924-case audit was rebuilt after its
new upstream census, and the repeat integrity pass is now `ok` with zero
failures, zero confirmed candidates, 177.991 GiB free, and supported decision
`no_trade`. Practice 007 remains flat at NAV 41.6042 with no positions or
orders. The candidate Git tree passed the credential audit with zero bearer-
secret findings.

Policy diagnostics on the inspected all-68 window confirm abstention is the
main immediate control: raising the predicted-move/cost hurdle from 0.75x to
1.25x, 1.50x, and 2.00x reduced the same-window loss from -120.25 pips to
-34.35, -13.35, and -7.50 pips respectively, while no-trade remained 0.00.
These are post-selection training diagnostics, not edge. Rotation and holding-
duration tweaks alone did not help. V1 also exposed a specific switch defect:
an incumbent that falls below the entry threshold disappears from the ranked
set, so its continuation value is not compared explicitly with exit/rotation.
The immutable V1 result is not patched; the V2 contract is specified in
`docs/SEQUENTIAL_ALL68_POLICY_SHORTLIST_V1.md`.

Current continuation queue:

1. Finish the fresh-registry genealogy migration after adversarial validation.
   Register the final and preserved source, replay, learner, and mistake
   identities with exact parent bytes/seals; archive the pre-hardening canonical
   registry before an atomic replacement. A rerun must be idempotent.
2. Finish the frozen research shortlist: no-trade, immutable V1 baseline, 2x cost hurdle,
   out-of-fold remaining-move calibration, explicit hold-versus-switch value,
   and signed-factor conflict suppression. Do not select and confirm on the
   same window.
3. After the policy is frozen, open the calendar-only historical expansion and
   then a strictly later untouched prospective cohort. Missing clocks/quotes
   remain failures, never survivor-filtered rows.
4. Continue prospective official-event collection. The raw first-seen 68/68
   quote-sidecar ledger remains correctly at zero during the weekend; do not
   backfill it. RBNZ permission, causal pre-release consensus, and timestamp-
   safe intraday rate repricing remain external source gates.
5. When current code and records settle, make one reviewed private Git commit,
   publish the exact clean committed source tree to the vault, hash-verify it,
   and refresh the canonical vault project records without retention deletion.

No item above authorizes Practice 007, loosens proof gates, or enables real
money.

## Corrected all-68 deliberate-practice checkpoint — 29 August 22:05 ET

The learner and mistake layers are now implemented above the verified
four-pair sequential session. The learner cohort
`sequential_portfolio_curriculum_v1.af435480002f2140c541` precommitted 48
attempts across four sessions: 36 distinct historical training cases and 12
zero-weight spaced reviews. The mistake report
`sprmistakecurriculum_00d7d59c24772d6805b382a80ef0` reduced 34 nonexclusive
labels to 21 structural clusters. Its leading needs are cost awareness, entry
quality, direction, and calibration. Neither attempts nor reviews manufacture
new market regimes or proof observations.

The corrected exact-window source pack is
`sequential_replay_source_pack_v1.1bddc89d33ec30767c96`. It binds all 68
instruments across homogeneous Monday, Wednesday, and Friday 12:00–16:00 UTC
blocks: 144 global clocks, 9,792 dependent pair contexts, and 204 immutable
archives. It retained 1,441 causal-context failures, 382 missing exact delayed
entry quotes, 290 missing exact feedback quotes, and 2,299 missing source
minutes. The independent verifier rebuilt its exact Cartesian manifest,
archive bounds, links, aggregate coverage, and hashes with zero failures.

The earlier pack `52259f...` remains immutable as a coverage-driven
engineering diagnostic. Its Friday 10:00–14:00 UTC selection improved sparse
TRY coverage but did not match the declared overlap schedule, so it cannot be
used as homogeneous overlap evidence.

The availability-aware all-68 replay is independently verified as cohort
`sequential_all68_portfolio_batch_replay_v1.445ddc96477b7ac284ed`. It retained
all 144 global decisions and all 9,792 contexts, ranked 261 causal candidates,
and exercised 50 waits, 24 entries, 15 holds, 24 exits, and 31 rotations. Its
110 exact bid/ask execution legs and 172 depth-one alternatives ended flat at
**−120.25 pips**. AUD/USD (+12.95) and EUR/USD (+5.40) were descriptively
positive, but repeated USD/JPY, GBP/USD, GBP/JPY, and USD/CAD losses dominated.
This is historical training evidence only: broader activity did not rescue the
unchanged momentum-ranking mechanics and no threshold was loosened.

Current continuation queue:

1. Build an all-68 mistake-directed curriculum from the exact primary and
   depth-one feedback. Deduplicate by global clock, predeclared episode block,
   and connected currency resources; prioritize cost, entry, direction,
   calibration, rotation, and opportunity-selection errors. Reviews remain
   zero-weight repetitions.
2. Use that curriculum to specify a small set of frozen historical-training
   policies: cost-aware abstention, remaining-move-versus-cost calibration,
   factor-conflict suppression, and explicit hold-versus-rotate comparison.
   The inspected sessions may select a definition but cannot confirm it.
3. Lock the selected policy before opening the calendar-only seven-Wednesday
   expansion. Schedule clocks before availability, retain missing quotes, and
   continue to count one global action rather than 68 pair observations.
4. Add levels, official events, news, causal macro consensus, and rate
   repricing only as separate source-conditioned cohorts with exact
   knowledge-time snapshot IDs. Do not retrofit them into price-only history.
5. Open a strictly later untouched prospective cohort only after the policy is
   frozen. Practice 007 remains fail-closed without a genuine confirmed
   candidate and narrow authorization; real-money routing stays disabled.

Storage is not a blocker: C: has **178.09 GiB free**. The all-68 batch artifacts,
curriculum, portfolio replay, and source-pack trees together use about
**24.8 MiB**. Empty or superseded development identities remain preserved until
the normal evidence-retention process can quarantine them with manifests.

## Sequential portfolio-session completion — 29 August 18:40 ET

The real bar-by-bar session layer is now implemented as a new immutable,
research-only sidecar, `sequential_portfolio_replay_v1`. It does not alter the
hourly case bank or either frozen SIM cohort. The pilot reads the exact verified
OANDA bid/ask M1 gzip archives for AUD/NZD, EUR/USD, GBP/CHF and USD/JPY and
uses a metadata-selected Friday overlap interval rather than a move-selected
window.

The canonical session contains 48 global five-minute decisions and 192 causal
pair contexts. It exercised all five actions: 25 waits, six entries, eight
holds, six exits and three two-leg rotations. Exact one-minute-delayed
executable sides plus 0.125-pip adverse slippage per leg produced 18 execution
legs and a flat terminal portfolio. Thirty-seven depth-one alternatives were
cloned from exact predecision state and remain `counts_as_rep=0`.

The frozen baseline lost **13.25 pips**. That result was preserved, not tuned
away: the purpose of this increment is to prove chronology, portfolio state,
costs, rotation, and component feedback. The already-inspected window remains
permanently historical training/discovery and cannot confirm or promote.

The independent verifier imports neither producer nor replay core. It rebuilt
all clocks, slices, candidates, actions, bid/ask fills, costs, state
transitions, branches, feedback and row roots with zero failures. It also
checks SQLite/WAL snapshot visibility, append-only triggers, source/code/config
bindings, foreign keys, integrity, terminal flatness and the immutable session
seal. Repeated runs reuse the same seal when evidence is unchanged.

Current continuation queue:

1. Add human/learner precommitted sessions plus novelty-weighted spaced
   repetition; repeat attempts must never increase distinct market-repetition
   or regime counts.
2. Add mistake curricula across direction, entry, management, exit, rotation,
   costs, calibration and opportunity selection using the verified feedback
   components.
3. Expand through deterministic additional sessions and then all 68 pairs in
   bounded, content-addressed batches. Schedule clocks before observing fill
   availability so missing quotes become recorded failures, not survivor bias.
4. Add official-event, rates, news and level-conditioned policies only as new
   cohorts with explicit source snapshot IDs. Do not retrofit them into this
   price-only history.
5. After selecting and freezing a policy, open a strictly later untouched
   prospective cohort with decisions sealed before feedback. Practice 007
   remains blocked without genuine confirmation and narrow authorization.

The complete contract and result are in
`docs/SEQUENTIAL_PORTFOLIO_REPLAY_V1.md`.

## Sequential deliberate-practice continuation — 29 August 17:02 ET

The first honest repetition/deduplication layer is complete. A new sidecar,
`sequential_deliberate_replay_v1`, leaves the immutable SIM V1 cohorts untouched
and converts their 49,230 variants into an arm-independent practice hierarchy:
5,700 two-sided outcome parts, 2,850 physical-path projections, 478 pair/chart
clocks, and 120 portfolio-choice clocks. The 360 within-clock currency graph
components remain structural diagnostics, not independent regime evidence.

The append-only case bank contains future-free situation fingerprints and blind
aliases plus an empty pre-outcome decision journal. It permits exactly one
primary `wait`, `enter`, `hold`, `exit`, or `rotate` action per session/case;
variants and repeat attempts never manufacture new market repetitions. Every
imported source case is permanently historical training/discovery because all
three SIM partitions were already inspected.

The old mistake sampler is now honestly labeled: 60 raw samples collapse to
four pair clocks, three portfolio clocks, and six paths. The rows remain
immutable diagnostics but can no longer dominate a curriculum as sixty lessons.

The independent verifier reproduced all identities and counts with zero
failures, checked blind-field exclusion, append-only guards, source/code/config
bindings, foreign keys, and SQLite integrity, and imports neither producer nor
core. The cohort is registered as a child of the verified SIM cohort in the
research genealogy. No trading or authorization boundary changed.

The continuation items below are superseded by the completed portfolio-session
increment above:

1. Add the real sequential session driver over completed M1 bars: one frozen
   policy, one global action per clock, maximum one open position, and explicit
   executable entry/hold/exit/rotation state.
2. Add depth-one alternatives cloned from the exact predecision portfolio and
   paired component scoring for direction, entry, management, exit, rotation,
   cost awareness, and opportunity cost. Counterfactuals remain non-repetitions.
3. Add novelty-weighted and spaced mistake replay after the session ledger is
   independently verifiable; prioritize unfamiliar regimes rather than more
   cosmetic parameter variants.
4. Keep all current cases training-only. Any selected frozen policy requires a
   later untouched prospective cohort with sealed decisions before feedback.

Continuity check at 15:38 ET: the prior supervisor had stopped silently at
13:38 ET while its children remained orphaned. A hidden singleton restart first
exposed that the prior launch contract was `SafeCoreOnly`; the three newly
started excluded workers (depth, order/position book, and second-forecast
tracker) were stopped, and that transient supervisor was replaced. Supervisor
PID 25396 is now running hidden with `SafeCoreOnly`, adopted 61 existing workers
with zero new starts on its verified heartbeat, and reports all three excluded
workers stopped for `safe_core_only`. Practice 007 is freshly current, flat at
NAV 41.6042, with zero open trades and zero pending orders. No gate changed.

## High-volume counterfactual SIM continuation — 29 August 15:28 ET

The first bounded high-volume SIM gym is implemented and independently
verified. The corrected cohort contains 49,230 matched virtual-order intents,
44,271 eligible fills, 2,735 causal rule signals, 478 decision clocks and 5,700
unique executable outcome paths across four pairs and one frozen week. Exact
as-signaled, flipped, deterministic-random and no-trade comparisons share the
same entry delay, horizon, path and cost contract. Practice 007, authorization,
lifecycle promotion, signal publication and real-money routing were untouched.

The independent verifier proved its value before the corrected run: it rejected
the first immutable cohort because one `2.22e-12`-pip SMA floating-point residue
was treated as a direction. The new explicit `1e-9`-pip zero tolerance opened a
new material cohort rather than rewriting history. The corrected cohort passed
independent reconstruction of all expected fires and non-fires, exact bid/ask
economics, partitions, re-entry sequence, effective N, append-only triggers,
archives and row roots. Adversarial omission, outcome-tamper, traversal,
oversize and link-boundary tests also fail closed.

The result is a bounded diagnostic null: all 108 as-signaled cells had negative
effective after-cost expectancy and no fixed definition beat both matched
controls in all three purged blocks. Hindsight shows that a cost-clearing side
existed on roughly 47–53% of H30 clocks, so the next bottleneck is causal
direction/magnitude selection rather than fabricating more overlapping orders.

Current SIM continuation queue:

1. Add a compact paired-control report with paired deltas and valid uncertainty
   diagnostics; do not rank ordinary overlapping intent totals as evidence.
2. Replace full-prefix archives with exact-window content-addressed archives,
   then scale through all 68 pairs in deterministic bounded batches.
3. Add level-reaction/support-resistance, existing strategy-lab, official-event
   and source-conditioned adapters as separate immutable cohorts.
4. Add frozen entry, exit, hold and rotation policy comparisons against
   no-trade and matched controls.
5. Only after historical discovery selects a definition, open an untouched
   prospective cohort with multiplicity control; Practice 007 remains blocked
   without genuine confirmation and narrow authorization.

The complete pilot record is
`docs/COUNTERFACTUAL_SIM_GYM_PILOT_20260829.md`; both SIM cohorts are now in the
project-wide research genealogy. The superseded cohort remains engineering
evidence and the corrected cohort remains historical diagnostic evidence only.

## Runtime and vault closeout — 29 August 12:26 ET

The strict V2 macro-surprise import is now adopted by the live research worker;
it is healthy and still contains zero causal consensus rows. The exact reviewed
source commit is in the source-only vault with a verified archive hash. Twelve
superseded model checkpoint ZIPs were moved, never deleted, through the
hash-verifying retention tool into local quarantine. The current checkpoint and
manifest remain in place, and the synchronized vault is now 4.549 GiB. No
runtime/source implementation item remains pending from this closeout.

## Causal consensus correctness checkpoint — 29 August 11:47 ET

The pre-release consensus path now uses a frozen V2 response-complete capture
boundary. Request start time can no longer make bytes received after release
appear causal. A trusted clock, exact provider release time, absent actual,
verified source identity, provider event/version identity, and immutable raw
payload hash are all required. The exact configured deployment activation
rejects pre-cohort captures; provider Calendar ID and LastUpdate cannot be
fabricated or replaced. The downstream surprise ledger independently
recomputes those gates, binds the exact current source/cohort/capture IDs, and
verifies each projection against the raw archived provider bytes. Rejected,
post-release, estimated-clock, archive-missing, contract-mismatched, and
untrusted-clock observations remain immutable diagnostics and cannot become
proof.

The refreshed access audit precisely confirms the external blocker: Trading
Economics has no configured credential, the existing Finnhub credential gets
HTTP 403 because its Economic Calendar requires premium access, and Alpha
Vantage plus FRED/ALFRED are not market-consensus sources. There are zero V2
causal rows and no currently permitted free provider. Do not infer or backfill
consensus; the next gate remains a permitted provider entitlement followed by
untouched prospective capture.

The one-time edge-publication rebuild from the 09:05 checkpoint is also
complete. The worker inspected all 15,746,198 canonical forecast rows, wrote
the exact output hashes, and returned to its normal idle cadence. The
independent verifier now reports `match`, including a passing
`edge_report_fresh_or_semantically_current` check; authorization remains false
because there are still zero confirmed candidates.

## Rates and RBNZ priority pass — 29 August 11:47 ET

The repository-controlled portion of the rates priority is complete. The new
`rates_policy_repricing_shadow_v2_20260829` contract provides an append-only
SQLite observation ledger, explicit source/retrieval/collector clocks,
configured HMAC clock attestation, verified content-addressed raw archives,
per-source intake-root containment with traversal/symlink rejection,
versioned revisions, exact active-cohort/source-contract filtering,
cutoff-causal replay, exact 15/60-minute alignment, duplicate resistance, and
fail-closed access diagnostics. A row cannot self-certify clock trust, and old
or foreign cohort rows remain excluded from replay/readiness counts. Its default state
has zero connected causal intraday sources, emits no observations, never
zero-fills missing rates, cannot trade or promote, and supports `no_trade`.

RBNZ's official B2 wholesale-rate source is now registered accurately as
daily, one-business-day-lag H4/H24 context. Both its page and official XLSX
return HTTP 403 to the bounded collector, so no bypass was attempted and it
cannot satisfy event-window repricing. RBNZ's published free email-update
channel is registered as the preferred direct-release path, but it remains
disconnected until the user subscribes and a permitted authenticated mailbox
connector supplies actual receipt and first-seen clocks. Neither channel is
credited as causal proof. Focused validation passed fifteen dedicated tests
plus two source-gap reconciliation tests, including self-certified-clock
rejection, missing/outside/traversal payload rejection, active-contract
isolation, and future-dated import rejection.

## Pair-options source checkpoint — 29 August 11:44 ET

The repository-controlled pair-options schema/validator gap is now addressed
with a prospective, fail-closed shadow contract. This is **not** an immutable
collector, append-only ledger, connected feed, or collection-ready source. It
distinguishes `direct_pair_surface`,
`inverted_direct_surface`, `two_leg_proxy`, and `unavailable` across all 68
pairs; preserves source, publication, first-seen, retrieval, effective and
decision clocks plus tenor, expiry, delta and quote conventions; flips
directional skew explicitly when CME's futures orientation is inverse to the
OANDA pair; and never zero-fills a missing metric. CME CVOL currently represents
only seven OANDA pair orientations (three direct and four inverted) and is a
30-day CVOL/ATM/whole-surface-skew reference, not a 25-delta risk reversal,
multi-tenor surface, or event-implied move. A two-USD-leg proxy cannot become a
cross-pair surface without timestamp-matched implied correlation/covariance.

No source is connected, public visualizers are not scraped, and the validator
has zero observations and no execution path. The remaining options work is
partly external and partly a future repository build: obtain a permitted
pair/tenor source or CME API access, open a new material source cohort, then
implement an immutable collector and append-only ledger, prove collection
integrity, and keep every derived feature shadow-only until prospective
incremental evidence exists.

## Current correctness and event-proof checkpoint — 29 August 09:05 ET

The append-only proof registry now replays all 16 cohort transitions, rejects a
silent A-to-B-to-A contract reactivation, and activates four explicit
`lineage_repair_20260829_v1` cohort heads. The 328,000 forecasts recorded under
superseded August 6 IDs remain immutable diagnostic evidence and are excluded;
the repair wrote zero forecasts and zero outcomes. A controlled hidden-worker
reload at 09:00 ET adopted all four August 29 IDs before another forecast
bucket, and the weekend cycle emitted zero forecasts.

The event provenance code is live. New official raw observations can receive
one append-only all-68 executable quote sidecar in the same database
transaction; partial, stale, or wrong-generation captures remain diagnostic
and cannot contain proof quotes. The mapper no longer reads a later quote
snapshot. Both live heartbeats now publish the exact raw-quote contract,
cohort, activation, universe hash, and expected count. The table and both
immutability triggers exist with zero retrospective rows. Direct verified
policy communications are prospectively issuer-bound; foreign currency
references are metadata only, and speeches remain research-only. Friday's Fed
speech is a frozen regression fixture, never proof.

Edge unchanged-input semantics now require both the exact semantic fingerprint
and byte/hash equality of the published JSON and Markdown. The independent
verifier separately replays proof and allocator transitions. The restarted
edge worker is intentionally rebuilding once because the legacy checkpoint did
not contain output hashes. The allocator now pauses without inserting empty
decision/candidate/comparator rows when no eligible universe exists while still
maturing pending historical outcomes. Five empty decisions created in the new
allocator cohort before live adoption remain immutable diagnostics. The first
post-reload cycle published `paused_no_eligible_universe`, persisted no sixth
decision, and left Practice canary authorization false.

Rank V5 now carries `no_trade` as an explicit frozen fourth research arm, not
only as a report label. Each future V6-bound decision creates one append-only
no-trade forecast and a declared-horizon outcome fixed at zero gross, zero
after-cost value, zero cost, and zero orders. The baseline requires no quote,
cannot place or authorize an order, and has a new material contract/cohort;
the prior zero-decision V5 contract is not relabeled as evidence.
A controlled hidden Rank-V5 worker-only reload at 09:51 ET adopted the new
contract and all four arms before the 10:00 ET prospective cohort clock. The
first post-reload state has zero decisions, cannot place orders, and remains
execution-ineligible.

The local/private Git source baseline and source-only vault profile are now
complete. The reviewed root commit `b3943607c2f07d3e979d3ab35327aae0f70c0903`
contains 1,135 source/config/test/documentation files, has no remote, passed an
exact-index and exact-commit credential audit with zero findings, and excludes
generated runtime-lock inventories. The first content-addressed vault archive
is 5,686,898 bytes with SHA-256
`10399fa40f9dc6af9060d03041dcdcef1694eb42b50d5a0e001e7b50b2c2c7e3`.
Its ZIP CRC and Git member inventory passed, current records came from the
audited commit rather than the worktree, and no legacy retention deletion ran.

The current unfinished queue is:

1. Observe the first genuinely new post-activation official event. Require a
   raw first-seen fresh 68/68 executable capture, mapper consumption without
   recapture, issuer-only direction, liquid/wide cost separation, and frozen
   event-versus-price-only-versus-no-trade research comparison.
2. Continue the four untouched August 29 proof cohorts plus V6/V151 and rank-V5
   research collection. Never backfill or retune from interim results.
3. Subscribe to RBNZ's free official email updates and connect a permitted
   authenticated mailbox adapter, or obtain an allow-list. Preserve actual
   receipt and first-seen clocks; do not bypass HTTP 403 or treat discovery as
   direct OCR proof.
4. Connect a permitted timestamp-safe intraday OIS/swap/policy-futures source
   to the frozen v2 shadow contract, and separately acquire causally clocked
   pre-release consensus. Daily B2 and official OCR levels remain context;
   continue abstaining rather than postfilling.
5. Obtain a permitted options source only if its access and retention terms
   allow collection. Prefer direct pair/tenor ATM, 25-delta RR/BF and
   event-expiry observations; do not scrape CME's public visualizer or treat
   the seven-pair CVOL reference as an all-68 surface. After access exists,
   create a new material cohort and implement the immutable collector/ledger;
   the current V1 component is validator-only and not collection-ready.
6. At Sunday reopen, revalidate account, quotes, all 68 instruments, source
   freshness, and execution transport without weakening any authorization.

Practice 007 remains flat at balance/NAV 41.6042, cumulative P/L -8.3430,
with zero positions and zero pending orders. No candidate is confirmed and the
supported decision remains `no_trade`.

## Current live-watch checkpoint — 28 August 20:30 ET

Post-recovery verification is clean. Practice 007 remains current, flat, and
unchanged; the verifier is `match` with no failures or warnings; project
integrity supports `no_trade`; and the edge worker is idle on unchanged inputs
with zero passing candidates. The former stale-report warning is complete and
is no longer pending. Resource and WAL diagnostics show no active fault.

The current unfinished queue is now:

1. Continue untouched V6/V151 and rank-V5 collection. They still contain zero
   prospective proof events, non-abstaining forecasts, decisions, or outcomes.
2. Implement and prove the research-only persistent, restart-safe exact
   forecast-membership cache with transactional source/schema/trigger metadata,
   audited highwater, integrity, exact counts, and deterministic anchors.
3. Confirm the detached supervisor and both quote transports remain healthy
   through Sunday reopen; never weaken execution freshness gates.
4. Build the frozen shadow-only `retrospective_recap_continuation` arm against
   an identical price-only control; keep recaps non-causal.
5. Add a lifecycle/genealogy producer-high-water publication handshake, restore
   remaining official-source gaps, obtain permitted pre-release consensus and
   timestamp-safe event-time rates/OIS, and repair or quarantine the historical
   PPI fixture.

No candidate is confirmed and the supported decision remains `no_trade`.

## Current live-watch checkpoint — 28 August 20:06 ET

OANDA practice reads recovered around 19:54 ET. Practice 007 is freshly
verified flat and unchanged at balance/NAV 41.6042, cumulative P/L -8.3430,
zero open trades, and zero pending orders. The account, accounting, allocator,
canary, executor, and re-entry paths all returned to current broker state
without inventing or rewriting outage history. The re-entry worker's stale
post-recovery HTTP-status diagnostic was repaired, tested, and adopted by a
controlled singleton reload. Weekend quotes agree across 68/68 instruments;
there is no routeable market and no confirmed hypothesis.

The current unfinished queue is now:

1. Continue untouched V6/V151 and rank-V5 collection. They still contain zero
   prospective proof events, non-abstaining forecasts, decisions, or outcomes.
2. Implement and prove the research-only persistent, restart-safe exact
   forecast-membership cache. Process-lifetime cache hits are proven, but a
   restart must validate transactional source/schema/trigger metadata, exact
   counts, audited highwater, integrity, and deterministic anchors before reuse;
   any mismatch must rebuild.
3. Let the next edge publication clear the verifier's non-blocking stale-report
   warning while retaining the exact zero-passing/zero-confirmed result.
4. Confirm the detached supervisor and both quote transports remain healthy
   through Sunday reopen; never weaken execution freshness gates.
5. Build the frozen shadow-only `retrospective_recap_continuation` arm against
   an identical price-only control; keep recaps non-causal.
6. Add a lifecycle/genealogy producer-high-water publication handshake, restore
   remaining official-source gaps, obtain permitted pre-release consensus and
   timestamp-safe event-time rates/OIS, and repair or quarantine the historical
   PPI fixture.

No candidate is confirmed and the supported decision remains `no_trade`.

## Current live-watch checkpoint — 28 August 19:25 ET

OANDA maintenance remains active, but unavailable account state is now carried
as unknown/null through every active dashboard, accounting, allocator, canary,
signal-news and integrity path rather than being coerced to zero/flat. Immutable
history was preserved: 3,491 account snapshots are valid and 41 broker-failure
rows are quarantined by an append-only validity sidecar. Current routeability
and all non-baseline allocator arms fail closed with `account_state_unavailable`.

The current unfinished queue is now:

1. At broker recovery, verify a current Practice-007 snapshot, current
   positions/orders, transaction 2461 or its legitimate successor, and automatic
   clearing of the maintenance error. Until then the last valid NAV 41.6042 and
   P/L -8.3430 are stale context only.
2. Verify the re-entry worker clears `broker_read_degraded` without a restart
   and resumes transaction accounting without duplicates.
3. Continue untouched V6/V151 and rank-V5 collection. They still contain zero
   prospective proof events, non-abstaining forecasts, decisions, or outcomes.
4. Implement and prove the research-only persistent, restart-safe exact
   forecast-membership cache. Cycle 2 proved an in-process cache hit, but a
   process restart must validate transactional source/schema/trigger metadata,
   exact counts, audited highwater, integrity and deterministic anchors before
   reuse; any mismatch must rebuild.
5. Confirm the edge cycle-3 publication clears the verifier's temporary stale-
   edge warning and retains zero passing/confirmed candidates.
6. Confirm the detached supervisor remains alive through maintenance and Sunday
   reopen. Recheck both quote transports and the executor compatibility JSON
   mirror then; never weaken execution freshness gates.
7. Build the frozen shadow-only `retrospective_recap_continuation` arm against
   an identical price-only control; keep recaps non-causal.
8. Add a lifecycle/genealogy producer-high-water publication handshake, restore
   remaining official-source gaps, obtain permitted pre-release consensus and
   timestamp-safe event-time rates/OIS, and repair or quarantine the historical
   PPI fixture.

No candidate is confirmed and the supported decision remains `no_trade`.

## Current live-watch checkpoint — 28 August 18:40 ET

OANDA's practice account endpoint is returning HTTP 503 maintenance responses.
The last verified 18:01 ET snapshot was balance/NAV 41.6042, cumulative P/L
-8.3430, flat with no orders; current broker state is unavailable rather than
zero. Account diagnostics now null unavailable current values and preserve only
explicitly stale financial context. The re-entry research worker now holds a
visible fail-closed degraded state instead of restarting on every 503. The edge
cycle completed with 52,640 cells and zero passing candidates.

The current unfinished queue is now:

1. At the next broker recovery, verify that the account writer returns to a
   current snapshot and that Practice 007 remains flat; never infer current
   positions or P/L from the retained pre-maintenance observation.
2. Confirm the re-entry worker clears `broker_read_degraded` on its own and
   resumes from transaction 2461 without duplicate accounting or a restart.
3. Continue untouched V6/V151 and rank-V5 collection. They still contain zero
   prospective proof events, non-abstaining forecasts, decisions, or outcomes.
4. Implement the research-only persistent exact forecast-membership cache with
   transactional metadata, source/schema/immutability validation, exact counts,
   integrity checks, audited highwater and deterministic anchors. Any mismatch
   must rebuild; demonstrate crash/reopen and append-only extension in tests.
5. Confirm the detached supervisor remains alive through the maintenance
   interval and Sunday reopen. The earlier unexplained tree loss remains an
   open operational incident.
6. Recheck the executor compatibility JSON mirror and both quote transports at
   Sunday reopen; SQLite transport is healthy and execution freshness gates
   must not be weakened.
7. Build the frozen shadow-only `retrospective_recap_continuation` arm against
   an identical price-only control; keep recaps non-causal.
8. Add a lifecycle/genealogy producer-high-water publication handshake, restore
   remaining official-source gaps, obtain permitted pre-release consensus and
   timestamp-safe event-time rates/OIS, and repair or quarantine the historical
   PPI fixture.

No candidate is confirmed and the supported decision remains `no_trade`.

## Current live-watch checkpoint — 28 August 18:11 ET

Supervisor recovery remains stable and Practice 007 is flat. The recovered edge
worker completed a second exact full scan at the new 15,746,198-row highwater
with zero malformed or duplicate payloads and is building cells. Audit proved
that the existing exact payload cache is process-lifetime only, so a restart
cannot currently demonstrate the desired cache-hit/append-only path.

The current unfinished queue is now:

1. Continue untouched V6/V151 and rank-V5 collection. They still contain zero
   prospective proof events, non-abstaining forecasts, decisions, or outcomes.
2. Let the recovered edge cycle finish and publish without interruption. Then
   implement a research-only persistent exact payload-membership cache with
   same-database transactional metadata, source identity/schema/trigger checks,
   integrity and exact-count validation, audited highwater, and deterministic
   first/prior-highwater/chunk anchors. Any mismatch must ignore the cache and
   rebuild; activate only after focused crash/reopen/append tests pass.
3. Confirm the detached supervisor remains alive across subsequent heartbeat
   intervals. The prior silent tree loss remains an operational incident with
   no evidenced code exception.
4. Recheck the executor compatibility-mirror atomic-replace permission error at
   Sunday reopen. The SQLite quote transport is healthy; do not weaken quote
   freshness or submission gates to repair a diagnostic mirror.
5. Re-profile CPU/disk after edge cell publication. The repeat scan saturated
   CPU but did not create a quote, execution, or storage failure.
6. Build the frozen shadow-only `retrospective_recap_continuation` arm against
   an identical price-only control; recaps remain non-catalyst evidence.
7. Add an explicit lifecycle/genealogy producer-high-water publication
   handshake to avoid temporary fail-closed mismatches.
8. Retain the unexplained or partially technical factor episodes without
   fitting late, opposed, or context-only media into causal direction.
9. Restore remaining official-source transport gaps and acquire permitted
   pre-release consensus plus timestamp-safe event-time rates/OIS repricing.
10. Repair or formally quarantine the unavailable historical PPI quote fixture.

Practice 007 remains flat at balance/NAV 41.6042 with zero orders. The
supported decision remains `no_trade`.

## Current live-watch checkpoint — 28 August 17:43 ET

The core supervisor tree stopped without an orderly error after 17:09:58 ET
and was recovered as a detached hidden singleton at 17:35:44 ET. All managed
workers are again fresh. The independent verifier self-cleared the transient
19-cell lifecycle/genealogy publication mismatch to 52,632/52,632 and is again
authorization-safe with zero confirmed candidates. The closure occurred after
Friday's market close while Practice 007 was flat.

The current unfinished queue is now:

1. Continue untouched V6/V151 and rank-V5 collection. They still contain zero
   prospective proof events, non-abstaining forecasts, decisions, or outcomes.
2. Verify the recovered edge worker finishes its current cycle using the
   persisted exact forecast-audit cache or append-only extension and reproduces
   the frozen counts. Its first complete publication remains valid.
3. Confirm the detached supervisor remains alive across subsequent heartbeat
   intervals. Treat the abrupt 17:10 loss as an operational incident unless a
   reproducible termination cause appears; do not infer a code exception from
   the absent error record.
4. Re-profile CPU/disk after recovery and the edge cycle. Preserve quote and
   executor priority; storage remains safe.
5. Build the frozen shadow-only `retrospective_recap_continuation` arm against
   an identical price-only control; recaps remain non-catalyst evidence.
6. Add an explicit lifecycle/genealogy producer-high-water publication
   handshake to avoid temporary fail-closed mismatches during equal-cadence
   publication.
7. Retain the two-sided ZAR and CAD episodes, JPY bursts, PLN weakness, and the
   final HKD-/CHF+/USD-/NZD+ close factors as unexplained or partially
   technical. Do not convert late, opposed, or context-only media into causes.
8. Restore RBNZ 403, StatCan 406, Swiss Finance 404, and intermittent
   GDELT/HKMA transport degradation without weakening timestamp rules.
9. Acquire permitted pre-release consensus and timestamp-safe event-time
   rates/OIS repricing. Surprise-confirmed arms remain blocked until causal.
10. Repair or formally quarantine the unavailable historical PPI quote fixture.

Practice 007 remains flat at balance/NAV 41.6042 with zero orders. The
supported decision remains `no_trade`.

## Current live-watch checkpoint — 28 August 17:08 ET

The first replacement edge-evidence cycle is complete and independently
visible: 15,652,740 frozen forecast rows, 52,632 cells, zero malformed or
duplicate payloads, and zero passing candidates. The Friday-close transport
crosscheck DST defect is repaired and active; stale final quotes are now
correctly informational while the market is closed. Practice 007 remains flat
and fail-closed.

The current unfinished queue is now:

1. Continue untouched V6/V151 and rank-V5 collection. They still contain zero
   prospective proof events, non-abstaining forecasts, decisions, or outcomes.
2. Verify the edge worker's next outcome-driven cycle uses the persisted exact
   audit-cache hit or append-only extension and reproduces the frozen-high-water
   counts. The initial full scan and publication are complete.
3. Re-profile the transient disk read burst next interval. CPU eased after
   bootstrap, storage is safe, and no quote/executor priority failure occurred.
4. Build the frozen shadow-only `retrospective_recap_continuation` arm against
   an identical price-only control; recaps remain non-catalyst evidence.
5. Add the lifecycle/genealogy producer-high-water publication handshake while
   keeping the independent verifier fail-closed.
6. Retain the two-sided ZAR and CAD episodes, JPY bursts, PLN weakness, and the
   final HKD-/CHF+/USD- close factors as unexplained or partially technical.
   Do not convert timely-but-opposed/context-only media into causal direction.
7. Restore RBNZ 403, StatCan 406, Swiss Finance 404, and intermittent
   GDELT/HKMA transport degradation without weakening timestamp rules.
8. Acquire permitted pre-release consensus and timestamp-safe event-time
   rates/OIS repricing. Surprise-confirmed arms remain blocked until causal.
9. Repair or formally quarantine the unavailable historical PPI quote fixture.

Practice 007 remains flat at balance/NAV 41.6042 with zero orders. The
supported decision remains `no_trade`.

## Current live-watch checkpoint — 28 August 16:35 ET

The replacement edge worker has now inspected all 15,652,740 frozen forecast
rows with zero malformed rows, duplicate payloads, or errors and has entered
cell construction. Practice 007 remains flat and fail-closed. A broad CAD
reversal and simultaneous PLN weakness cleared costs across multiple pair legs;
technicals aligned, but no strict official or independently directional source
explained either episode.

The current unfinished queue is now:

1. Continue untouched V6/V151 and rank-V5 collection. They still contain zero
   prospective proof events, non-abstaining forecasts, decisions, or outcomes.
2. Allow the edge worker to finish cell construction and publish its first
   complete cycle, then verify the next outcome-driven cycle is an exact cache
   hit or append-only extension with identical frozen-high-water counts.
3. Re-profile CPU only after that full scheduling cycle. Disk queue and memory
   pressure have already improved materially; preserve quote/executor priority.
4. Build the separately frozen, shadow-only
   `retrospective_recap_continuation` arm against an identical price-only
   control; retrospective recaps must never be relabeled as catalysts.
5. Add the lifecycle/genealogy producer-high-water publication handshake while
   keeping the independent verifier fail-closed.
6. Retain the two-sided ZAR episode, both sides of the CAD episode, the JPY
   bursts, and the new PLN weakness as unexplained or partially technical
   factor episodes. The timely Warsh/oil story was directionally mixed and
   cannot be fitted back as a general cause.
7. Restore RBNZ 403, StatCan 406, Swiss Finance 404, and intermittent
   GDELT/HKMA transport degradation without weakening timestamp rules.
8. Acquire permitted pre-release consensus and timestamp-safe event-time
   rates/OIS repricing. Surprise-confirmed arms remain blocked until causal.
9. Repair or formally quarantine the unavailable historical PPI quote fixture.

Practice 007 remains flat at balance/NAV 41.6042 with zero orders. The
supported decision remains `no_trade`.

## Current live-watch checkpoint — 28 August 15:43 ET

The direct-source 8.4 GB diagnostic census, unindexed BLS article loader, and
15.6-million-row repeated forecast audit are now exact incremental/cached paths
in addition to the four earlier fixes. Direct-source and BLS replacements are
healthy. The edge-evidence replacement is in its one required full bootstrap;
later outcome-only cycles should reuse the disk-backed forecast audit exactly.

The current unfinished queue is now:

1. Continue untouched V6/V151 and rank-V5 collection. They still contain zero
   prospective proof events, non-abstaining forecasts, decisions, or outcomes.
2. Verify that the replacement edge-evidence worker completes its initial
   15,614,926-row exact audit and that its next outcome-driven cycle reports an
   audit-cache hit or append-only extension with identical counts. Do not
   interrupt the bootstrap merely to reduce current load.
3. Re-profile CPU/disk only after that cache has completed a full scheduling
   cycle. Remaining observed readers include local-news collection, signal
   trials, project integrity, and proof-shadow work; optimize only exact-output
   research/reporting paths and preserve quote/executor priority.
4. Build a separately frozen, shadow-only `retrospective_recap_continuation`
   diagnostic arm: join first-seen retrospective price recaps to causal
   cross-pair factor agreement and level-band approach state, then compare it
   against an identical price-only continuation control. Never reclassify a
   recap as catalyst news or route it to execution.
5. Add a producer-high-water/publication handshake between lifecycle and
   genealogy so their equal-cadence workers do not create avoidable transient
   verifier mismatches. Until then the verifier must continue failing closed;
   the latest 86-cell race self-cleared exactly to 52,613/52,613.
6. If genuine position-ledger maturities remain materially expensive, maintain
   exact incremental sufficient statistics rather than rebuilding all seven
   mature aggregates. Do not delay or approximate outcome capture.
7. Retain the two-sided ZAR whipsaw, the 15:08 ET JPY-weakness burst, and the
   15:47 ET CAD-weakness cluster as unexplained/partially technical market
   episodes. None had a strict forward causal source. The CAD continuation arm
   was directionally correct across three legs, but the nearby unverified
   Warsh-rate headline cannot be promoted into a CAD cause; retrospective or
   context-only headlines must not be fitted back into these moves.
8. Restore RBNZ 403, StatCan 406, Swiss Finance 404, and intermittent
   GDELT/HKMA transport degradation without weakening timestamp rules.
9. Acquire a permitted pre-release consensus source and timestamp-safe
   event-time rates/OIS repricing. Surprise-confirmed arms remain unavailable
   until those inputs are genuinely causal.
10. Repair or formally quarantine the unavailable historical PPI quote fixture;
    it remains non-blocking for V6 and live collection.

Practice 007 remains flat at balance/NAV 41.6042 with zero orders. The
supported decision is `no_trade`; no gate was loosened to manufacture activity.

## Current live-watch checkpoint — 28 August 15:08 ET

The current pass closed four recurrent exact-output hot paths: V6 input scans,
official-mapper scans, position-ledger mature-summary rebuilds on ordinary
opens, and the mature-only executable-opportunity drain. Replacement workers
are fresh, the opportunity pending queue exactly matches all 3,213 unresolved
forecasts, 88 focused tests pass across the final local suites, and all checked
SQLite databases report `ok`.

The current unfinished queue is now:

1. Continue untouched V6/V151 and rank-V5 collection. They still contain zero
   prospective proof events, non-abstaining forecasts, decisions, or outcomes.
2. Replace the direct-source diagnostic's periodic JSON-wide scan of the 8 GB
   edge ledger with an exact compact/incremental canonical census. Its scheduled
   pass remains the largest observed read burst; do not substitute estimates.
3. Profile the evidence-operations worker and remaining CPU leaders after the
   current I/O fixes have a full scheduling cycle. Optimize only exact-output
   research/reporting paths; quote, executor, and authorization priority must
   remain unchanged.
4. If genuine position-ledger maturities remain materially expensive, maintain
   exact incremental sufficient statistics rather than rebuilding all seven
   mature aggregates. Do not delay or approximate outcome capture.
5. Retain both sides of the late ZAR whipsaw as unexplained
   positioning/liquidity evidence. The earlier weakening leg had zero strict
   forward stories; the later strengthening reversal began before a broad
   Warsh/yield recap was observed and still had no new official fast-lane
   observation. Do not fit either side to retrospective commentary.
6. Restore RBNZ 403, StatCan 406, Swiss Finance 404, and intermittent
   GDELT/HKMA transport degradation without weakening timestamp rules.
7. Acquire a permitted pre-release consensus source and timestamp-safe
   event-time rates/OIS repricing. Surprise-confirmed arms remain unavailable
   until those inputs are genuinely causal.
8. Repair or formally quarantine the unavailable historical PPI quote fixture;
   it remains non-blocking for V6 and live collection.

Practice 007 remains flat at balance/NAV 41.6042 with zero orders. The
supported decision is `no_trade`; no gate was loosened to manufacture activity.

## Current live-watch checkpoint — 28 August 14:17 ET

Two dominant research I/O defects discovered during this interval are closed:

- the V6 source-response mapper now reads the exact required tail window from
  the 630 MB all-68 M1 archive instead of rereading every CSV from byte zero;
  and
- the top-signal position ledger caches immutable mature-history summaries,
  invalidates correctly on both local and external commits, and uses bounded
  indexes for current counts/recent rows.

After worker-only activation, both states are fresh, both SQLite ledgers are
healthy, 40 combined focused tests pass, and a five-sample disk check improved
from roughly 252% disk time/5.0 queue to 20%/0.40. Aggregate CPU remains near
100%, so exact-output CPU reduction is still the leading operational task.

The current unfinished queue is:

1. Continue untouched V6/V151 and rank-V5 collection. They still contain zero
   prospective proof events, non-abstaining forecasts, decisions, or outcomes.
2. Retain the 13:51-14:05 ZAR weakening leg as unexplained positioning or
   liquidity evidence: all ten sampled leaders had zero strict forward stories
   and the official fast lane added no release. Do not retrofit a nearby story.
3. Profile the remaining CPU leaders (proof-shadow predictors, strategy lab,
   signal-trial ledger, news collector, and policy-breakout research) and make
   only output-equivalent, evidence-preserving changes. Quote and executor
   freshness currently remain healthy despite saturation.
4. Remove the direct-source diagnostic's periodic JSON-wide scan of the 8 GB
   edge ledger by consuming or maintaining an exact compact canonical census;
   do not weaken integrity or replace exact counts with estimates.
5. On the next clean move-audit rebuild, parse source payloads once, publish a
   progress sidecar, and bind the audit to a frozen census high-water mark with
   exact-output equivalence.
6. Restore RBNZ 403, StatCan 406, Swiss Finance 404, and intermittent
   GDELT/HKMA transport degradation without weakening timestamp rules.
7. Acquire a permitted pre-release consensus source and timestamp-safe
   event-time rates/OIS repricing. Surprise-confirmed arms remain unavailable
   until those inputs are genuinely causal.
8. Repair or formally quarantine the unavailable historical PPI quote fixture;
   it remains non-blocking for V6 and live collection.

Practice 007 remains flat at balance/NAV 41.6042 with zero orders. The
supported decision is `no_trade`; no gate was loosened to manufacture activity.

## Current live-watch checkpoint — 28 August 13:49 ET

Two operational defects discovered during this interval are closed:

- the independent verifier now evaluates publication freshness from an
  end-of-pass reread while preserving its pass-start payloads for independent
  reconstruction; its result is `match` and project integrity is `ok`; and
- the non-streaming strategy lab now overlays newest-timestamp read-only
  all-68 OANDA pricing during its long lane sweep. Its first live cycle reduced
  stale-quote rejections from 6,146 to 857 (86.1%) without changing the
  30-second freshness limit, venue tradeability, or any execution gate.

The current unfinished queue is deliberately narrow:

1. Continue untouched V6/V151 collection after the 13:30 ET boundary and let
   rank V5 consume only V6 proof rows. Both currently contain zero prospective
   actionable evidence and support `no_trade`.
2. Retain the HUF/CZK/MXN/SEK rotations as unexplained positioning/liquidity
   episodes unless a genuinely pre-move source is found. Do not promote
   retrospective market recaps into causal news.
3. Monitor subsequent strategy-lab cycles for stable quote-overlay coverage,
   provider latency/errors, and residual legitimately quiet quotes. Do not
   relax the freshness or tradeability rules. The first post-fix cycle took
   94.9 seconds under a system-wide near-100% CPU load.
4. Profile and reduce the research-worker CPU/cycle burden without reducing
   quote, executor, evidence, or authorization priority. Preserve exact output
   equivalence and immutable ledgers before changing cadence or computation.
5. On the next clean move-audit rebuild, parse source payloads once, index or
   lazily cache candle histories per instrument, publish a progress sidecar,
   and bind the audit to a frozen census high-water mark with exact-output
   equivalence.
6. Restore RBNZ 403, StatCan 406, Swiss Finance 404, and intermittent
   GDELT/HKMA transport degradation without weakening timestamp rules.
7. Acquire a permitted pre-release consensus source; surprise-confirmed arms
   remain unavailable until consensus is captured before publication.
8. Repair or formally quarantine the unavailable historical PPI quote fixture;
   it remains non-blocking for V6 and live collection.

Practice 007 remains flat at balance/NAV 41.6042 with zero orders. The
supported decision is `no_trade`; no threshold or authorization boundary is to
be loosened to manufacture activity.

## Current live-watch checkpoint — 28 August 13:22 ET

The second retrospective-pair metadata gap and two Windows publication
transients are closed:

- V151 treats pair-led moving-average/breakout recaps as already-observed
  market movement, preserves opposing base/quote direction, and cannot publish
  them as forward evidence;
- V6 and rank V5 are loaded under new immutable contracts, import no V5/V4
  evidence, and begin with zero proof rows or decisions;
- rank snapshots tolerate only bounded transient Windows target locks; and
- supervisor contract reads tolerate only a bounded transient read/parse
  window before retaining fail-closed behavior.

The current unfinished queue is deliberately evidence- and source-bounded:

1. Let V6 cross its **13:30 ET** activation and accumulate untouched V151
   official-source events plus exact matured outcomes. Never merge V5 rows.
2. Let rank V5 consume only V6 proof rows. Its current result is `no_trade`
   with zero decisions and outcomes.
3. Retain both 12:22-12:42 and 12:50-13:03 whipsaws as unexplained/positioning
   episodes unless a source observed before each move is found. Retrospective
   pair recaps and repeated Warsh/Iran stories are not causal explanations.
4. Allow the active independent-verifier pass to complete, then confirm the
   project-integrity report clears its retained three-state lifecycle
   materialization mismatch. The current lifecycle file and database both show
   52,527 hypotheses, 43,141 collecting, 9,386 futile, and zero confirmed; all
   routing surfaces remain closed while verification is in progress.
5. On the next clean move-audit rebuild, parse source payloads once, index or
   lazily cache candle histories per instrument, publish a progress sidecar,
   and bind the audit to a frozen census high-water mark with exact-output
   equivalence.
6. Restore RBNZ 403, StatCan 406, Swiss Finance 404, and intermittent
   GDELT/HKMA transport degradation without weakening timestamp rules.
7. Acquire a permitted pre-release consensus source; surprise-confirmed arms
   remain unavailable until consensus is captured before publication.
8. Repair or formally quarantine the unavailable historical PPI quote fixture;
   it remains non-blocking for V6 and live collection.

Practice 007 remains flat at balance/NAV 41.6042 with zero orders. The
supported decision is `no_trade`; no threshold or authorization boundary is to
be loosened to manufacture activity.

## Current live-watch checkpoint — 28 August 12:55 ET

The live retrospective-recap defect and the independent-verifier transient
integrity warning are resolved:

- classifier V150 explicitly quarantines pair-led "moves to new highs/lows"
  articles as retrospective market reports rather than forward evidence;
- immutable V4/V149 evidence was not rewritten; V5 and rank V4 use fresh
  contracts, import no prior evidence, and have no legacy fallback;
- V5 activates at 13:00 ET with zero pre-activation proof events, and rank V4
  is ready with zero decisions and the supported result `no_trade`;
- the integrity audit may accept a verifier rebuild only under a recent prior
  completed match and fully closed promotion/authorization/execution surfaces;
  and
- the move-first audit no longer performs repeated full event-array copies.

The current unfinished queue is bounded to evidence, source availability, and
the remaining safe audit-performance work:

1. Accumulate untouched V5 official-source proof events and exact matured
   outcomes after its 13:00 ET boundary; do not merge or relabel V4 rows.
2. Let rank V4 collect only V5 proof rows. Current source rows, decisions, and
   maturities are zero by design.
3. Keep the 12:22-12:42 broad USD retracement as an unexplained/positioning
   episode unless a source observed before the move is found. Delayed market
   recaps and later Warsh repetitions must not become new causal evidence.
4. On the next clean move-audit rebuild, parse source payloads once, index or
   lazily cache candle histories per instrument, publish a progress sidecar,
   and bind the audit to a frozen census high-water mark. Preserve exact output
   equivalence before replacing the current long-running pass.
5. Restore RBNZ 403, StatCan 406, Swiss Finance 404, and intermittent
   GDELT/HKMA transport degradation without weakening source or timestamp
   requirements.
6. Acquire a permitted pre-release consensus source. Surprise-confirmed arms
   remain unavailable until consensus is captured before publication.
7. Repair or formally quarantine the unavailable historical PPI quote fixture;
   it remains non-blocking for V5 and live collection.

Practice 007 remains flat at balance/NAV 41.6042 with zero orders. The
supported decision is `no_trade`; no proof, conflict, freshness, or
authorization gate is to be loosened to manufacture activity.

## Current live-watch checkpoint — 28 August 12:12 ET

The clean V4 boundary and the two integrity findings from this interval are
resolved:

- V4 activated at 12:00 ET without importing V3 and correctly excluded its
  first irrelevant ECB diagnostic event from prospective proof;
- rank V3 consumes V4 proof rows only and continues to return `no_trade`;
- the news/technical watchlist now publishes and is supervisor-gated on the
  exact V149 classification contract; and
- integrity accepts the executable-opportunity ledger only in its exact safe
  retired/drain state, with future production and all routing surfaces closed.

The remaining queue is prospective evidence and external data availability:

1. Accumulate untouched V4 official-source proof events and exact matured
   outcomes. Current proof-event and rank-decision counts remain zero.
2. Continue recording direction-conflicted candidates in shadow only. The
   11:59 AUD/USD candidate projected +1.5224 pips after spread but was not an
   independent, conflict-free, confirmed thesis and therefore was not routed.
   A direction-clear GBP/USD preview at 12:14 projected +1.3460 pips after
   spread, but it likewise remained unconfirmed and was correctly stopped by
   the independent canary gate rather than being treated as proof.
3. Keep noon USD/NZD continuation classified as contextual continuation of the
   timely 10:00 Fed Warsh event unless a separate causal source is observed;
   delayed secondary repetitions must not become new evidence.
4. Restore RBNZ 403, StatCan 406, Swiss Finance 404, and intermittent
   GDELT/HKMA transport degradation without weakening source or timestamp
   requirements.
5. Acquire a permitted pre-release consensus source. Surprise-confirmed arms
   remain unavailable until consensus is captured before publication.
6. Repair or formally quarantine the unavailable historical PPI quote fixture;
   it remains non-blocking for V4 and live collection.

Practice 007 remains flat at balance/NAV 41.6042 with no orders. The supported
decision is `no_trade`; no proof, conflict, freshness, or authorization gate is
to be loosened to manufacture activity.

## Current live-watch checkpoint — 28 August 11:06 ET

The implementation defects found in this interval are closed:

- the CBRT university paper-contest notice cannot enter policy proof;
- contaminated V3 remains immutable diagnostic evidence and cannot feed the
  current rank;
- V4 requires explicit relevance, no exclusion, classifier V149, and the exact
  mapper contract;
- rank V3 consumes only the V4 database and has no legacy fallback;
- all 13 governed direct central-bank communication sources are included in
  the official fast lane under a new prospective contract;
- long causal-level and direct-response work publishes progress instead of
  appearing stalled at end-of-cycle only; and
- the remaining hot JSON publishers tolerate transient Windows atomic-replace
  denial without silently losing or weakening output.
- the official-response heartbeat carries classifier V149 in every phase, so
  the supervisor no longer restarts a current worker as contract-stale.

The current queue is evidence and external-source availability:

1. Let the V4 proof cohort accumulate untouched official-source episodes after
   its **12:00 ET** activation and mature exact 1/5/10/15/30/60/120-minute
   executable outcomes. Its initial proof-event count is zero by design.
2. Let rank V3 and the frozen news/band/quote-flow comparisons collect future
   decisions. Rank V3 is ready but has zero source rows and supports
   `no_trade`.
3. Keep the Fed 10:00 ET speech as diagnostic attribution: the broad official
   feed observed it at 10:01:05 ET, before the communications fast-lane
   contract activated, so it must never be backfilled as prospective proof.
4. Restore degraded external transports without weakening causality: RBNZ 403,
   StatCan 406, Swiss Finance 404, and intermittent GDELT/HKMA timeouts. All
   21 currency legs and 68/68 pair legs remain mapped despite those redundant
   transport degradations.
5. Acquire a permitted, timestamped pre-release consensus source. Until then,
   surprise-confirmed arms remain unavailable rather than inferring consensus
   after publication.
6. Continue watching quote reconnect frequency, atomic-publish stderr, and the
   repaired response-watch contract after the reload. Current quote transport
   is 68/68 and the response watcher is supervisor-fresh; recurrence is an
   operational diagnostic, not permission to loosen gates.
7. Repair or formally quarantine the unavailable historical PPI quote fixture;
   it remains non-blocking for V4, live collection, and Practice 007.

Practice 007 remains flat at balance/NAV 41.6042 with no trades or orders.
The supported decision remains `no_trade`; no promotion or entry threshold is
to be loosened to force activity.

## Current live-watch checkpoint — 28 August 02:12 ET

Known implementation defects found at the live-watch baseline are closed:

- BOJ non-market research can no longer masquerade as FX intervention;
- chart ranges cannot become policy-vote splits without local vote language;
- scheduled preflight uses authoritative direct currencies;
- sub-15-second raw source quotes are frozen before slow semantic work;
- V3/rank-V2 have separate immutable evidence identities and import no V2
  evidence;
- long macro/archive cycles publish progress and avoid repeated full-history
  payload scans;
- official fast-lane atomic writes tolerate transient Windows replacement
  denial; and
- reconnect-seeded quotes are research-only and excluded from current
  cross-transport metrics.

The remaining queue is prospective evidence and external-source availability:

1. Let V3 begin at its 03:00 ET boundary and accumulate independent official-
   source episodes plus exact 1/5/10/15/30/60/120-minute outcomes.
2. Let rank-V2 and the frozen news/band/quote-flow arm collect untouched
   decisions and outcomes. Current supported decision remains `no_trade`.
3. Restore the remaining degraded external legs without weakening causality:
   RBNZ policy transport (403), intermittent GDELT/HKMA timeouts, and causal
   pre-release consensus. Trading Economics remains credential-blocked and
   paid/dormant sources remain optional, not required for price-based proof.
4. Preserve the 1,577 accidental V148 timing-probe mappings as diagnostic-only;
   never merge or relabel them into V3 proof.
5. Repair or formally quarantine the unavailable historical PPI quote fixture;
   it is not a blocker for live collection, V3 proof, or Practice 007.

Practice 007 remains flat and fail-closed. Do not loosen promotion or entry
gates because the current evidence is null.

## Current source-first checkpoint — 27 August 22:42 ET

Known implementation blockers from this pass are closed:

- exact 10-minute source response is live in a distinct V2 cohort;
- sealed V1 cannot receive new rows;
- the source-conditioned 21-currency rank comparison is live and V2-only;
- source knowledge clocks precede every frozen entry quote;
- the news x frozen-band x completed quote-flow H15 comparison is live;
- upstream source cohort/contract lists are preserved exactly;
- prior timing cohorts continue to mature after definition changes;
- the news watchlist no longer polls the retired opportunity ledger; and
- all new tables enforce append-only update/delete rejection.

The remaining queue is evidence/data availability, not permission to loosen
Practice-007 gates:

1. Accumulate post-activation official-source repetitions and exact
   1/5/10/15/30/60/120-minute outcomes. Current V2 proof-event count is zero.
2. Let the frozen currency-rank and news-band timing cohorts collect independent
   episodes. Current rank decisions and timing candidates are both zero.
3. Acquire a permitted causal pre-release consensus source and timestamp-safe
   event-time rate/policy-path repricing. Until then, surprise-confirmed arms
   must continue to abstain.
4. Evaluate all new arms after executable costs, episode/currency-factor
   deduplication, concentration stress, and untouched confirmation. Do not
   promote from diagnostic/preactivation rows.
5. Let the two drain-only negative-control workers finish their already-issued
   outcomes without reopening forecast production.

Current supported decision: `no_trade`. Practice 007 and real-money routing
are unchanged.

## Cleanup closeout — 27 August 21:38 ET

- The H1 drain-only worker was reloaded under the hidden supervisor after the
  retirement-idle patch. It resumed with zero new forecasts, zero session
  errors, and 1,924 outstanding causal horizons. After that count reaches zero,
  it backs off to a four-minute liveness interval instead of continuously
  rescanning the historical ledger.
- The executable-opportunity ranker remains drain-only with 3,213 outstanding
  horizons, zero new forecasts, and no execution or promotion authority.
- The focused H1 retirement suite passed 10 tests in the live worker runtime;
  Python compilation also passed.
- Current project and Shared Brain records are included in the final verified
  vault checkpoint. Timestamped older vault checkpoints are moved only through
  the hash-verifying recoverable retention tool; the current archive and its
  manifest are never moved.

No known implementation blocker remains in this queue. The three items below
are deliberately unresolved evidence states and must not be closed by changing
thresholds, deleting negative history, or reusing retired cohort IDs.

## Final cleanup delta — 27 August 21:25 ET

Completed after the 21:15 checkpoint:

- The ineffective executable-opportunity ranker is now supervised in
  `mature_only`; it cannot issue new rows and will resolve its remaining exact
  horizons before becoming a frozen negative control.
- The allocator proof is explicitly paused until a lifecycle-confirmed candidate
  exists, preventing more empty-set candidate/integrity growth.
- Stale August 3 signal-combination rules now fail closed after 24 hours and
  cannot supply active strategy-lab candidates.
- Duplicate read-only Practice-007 polling was collapsed to one broker request
  that atomically maintains both compatibility snapshots.
- Five active/manual tools no longer default to stale `D:\forex\trad` paths.
- A content-hashed 163-file case-history index now prevents repeated or lost
  move/news audits.

The only unfinished items in this queue are evidence outcomes, not known code
or runtime blockers:

1. Let both drain-only workers reach zero pending outcomes.
2. Continue the frozen proof families and clean level-band cohort until governed
   confirmation or futility boundaries are crossed.
3. Keep Practice 007 at `no_trade` while confirmed candidates remain zero.

## Runtime and evidence cleanup checkpoint — 27 August 21:15 ET

The obsolete-shadow and storage-cleanup implementation is complete:

- `hgb_live_outcomes` is retired and disabled. It read a stale D-drive input,
  had zero pending work, and did not establish promotable evidence. Its code and
  6,006 historical outcomes remain preserved.
- `manager_decision_outcome_ledger` is retired and disabled. Its frozen input had
  three matured forecasts, zero waiting rows, and no current work. Its input,
  state, and SQLite ledger remain preserved.
- `one_hour_shadow_signal` is in supervised `mature_only` mode. It cannot create
  or publish new forecasts, but it will resolve every already-issued causal
  horizon. At the dated checkpoint it had 5,816 pending rows and zero errors;
  when the drain reaches zero, the frozen four-family stream remains a negative
  control rather than returning to active production.
- The four immutable governed proof families continue unchanged. Their exact
  cells remain underpowered even though aggregate results are negative, so they
  were not incorrectly retired as a group.
- Unavailable order-book, position-book, and pricing-depth contracts are marked
  inert; the 30 zero-output model-gap contributors are marked dormant. Neither
  group is counted as active predictive breadth.
- A recoverable cleanup moved 18,704 expired derived `.log`, `.bak`, and `.tmp`
  files (394,242,063 bytes) to the manifested archive
  `data/archive/runtime_cleanup/20260828_010345`. It deleted no causal evidence
  and reclaimed no same-drive capacity. The storage guard remains `ok` with about
  124 GiB free; no live SQLite/WAL/SHM file was vacuumed or removed.
- Current source governance reports 187 configured/observed and 179 operational
  sources. Official policy releases and schedules are operational for all 21
  currencies and both legs of all 68 pairs. The one explicit policy-health gap
  is RBNZ; it does not make NZD source coverage absent.

The duplicate backlog under `docs/` is now an archive pointer. This root file is
the only canonical pending queue. Full evidence and historical lineage are in
`SOURCE_MODEL_RUNTIME_CLEANUP_AUDIT_20260827.md` and `FOREX_PROJECT_LOG.md`.

Remaining evidence work is intentionally narrow:

1. Allow the H1 drain-only worker to reach zero pending forecasts without
   reopening production.
2. Accumulate clean level-band outcomes and independent episodes under
   `level_band_contract_v2_frozen_20260827b`.
3. Continue immutable proof-family collection until exact cells cross governed
   confirmation or futility boundaries. Do not loosen gates or authorize a
   Practice-007 entry while confirmed candidates remain zero.

## Level-band maturity and pip-contract checkpoint — 27 August 20:00 ET

The first outcome maturity stopped the continuous worker at 20:58Z with SQLite
`OperationalError: 14 values for 13 columns` in `band_outcomes`. The insert now
names the 13 target columns explicitly, and a regression test covers the first
maturity path. This failure had no execution impact: the worker remains
research-only and cannot place, authorize, promote, or manage orders.

A bounded compatibility recovery wrote 46 exact 15-minute outcomes and 215
missing-path censors. Those rows, and every operational-source-hash cohort
created before the clean pip contract, are engineering diagnostics only and are
permanently non-promotable. They must not be used to estimate edge or satisfy
any proof gate.

The recovery also exposed a nonstandard-pip defect: the static JPY heuristic was
wrong for instruments including HUF, THB, and HKD/JPY. Pip resolution now uses
the audited `oanda_instrument_pips` fallback and the exact quote `pip` when it is
available. Geometry is frozen under the new contract
`level_band_contract_v2_frozen_20260827b`.

The clean prospective cohort is
`level_band_prospective_20260827a.9294e3511aae455f`. Its first completed cycle
had **64/68** ready contexts and retained **25 forecasts with zero
entries/outcomes**. It remains research-only, with no execution or promotion
surface. The focused repair suite passed 30 tests.

The repaired worker is now adopted by the reloaded hidden supervisor. The
supervisor preserved the existing worker PIDs, reports the worker running with a
fresh heartbeat, and produced no reload stderr. The verified active cohort is
`level_band_prospective_20260827a.9294e3511aae455f` under geometry contract
`level_band_contract_v2_frozen_20260827b`.

Evidence remains pending:

1. Accumulate clean-contract matured outcomes and effective independent market
   episodes; exclude all compatibility and pre-contract engineering rows.
2. Compare frozen bounce/break arms with no-trade after executable costs and
   concentration/factor/episode deduplication.
3. Require untouched prospective confirmation before any lifecycle promotion or
   narrowly authorized Practice-007 canary. No candidate is currently confirmed.

## Adaptive causal level-band checkpoint — 27 August 16:34 ET

Persistent causal support/resistance geometry, the prospective pre-outcome
collector, dashboard panel, and hidden supervision are implemented. Geometry is
versioned under `level_band_contract_v2_frozen_20260827a`. The first engineering
cohort, `level_band_prospective_20260827a.eb1a014d25fa52dd`, is frozen with
seven forecasts and zero entries/outcomes. It exposed a sequential all-68 data
handoff defect and is not merged into proof.

The next engineering cohort,
`level_band_prospective_20260827a.acd30b3829e982f4`, is also frozen and
superseded with no promotion. It corrected the sequential all-68 handoff by
consuming the last published complete update-report cutoff and separately
ignoring or validating newer CSV rows written by an in-progress sequential
refresh. This eliminated transient `report_csv_cutoff_mismatch` failures without
treating partially refreshed pairs as one synchronized market snapshot, but its
resident representation still consumed roughly 800 MiB.

The current live collection cohort is
`level_band_prospective_20260827a.d5df939213cb50a8`. It retains only 720 M1 and
600 M5 rows per pair after constructing bands from the full 18,000-row source
tail, reducing resident worker memory to roughly 94 MiB without narrowing the
geometry lookback. Each encounter still freezes its band, approach descriptors,
barriers, and both bounce/break counterfactual arms before the future M5 entry
exists. Later entry, outcome, and censor records are append-only and use
executable bid/ask economics.

The current cohort completed its first cycle at 20:39:46Z: **65/68** instruments
had ready causal context, while EUR/TRY, TRY/JPY, and USD/TRY were explicitly
blocked for stale M1 context. It displayed 24 valid bands and retained **seven
prospective forecasts with zero entries/outcomes**. The dashboard now exposes
band geometry, distance, approach
velocity/acceleration, path efficiency, contact state, cohort counts, and
descriptive after-cost bounce/break cells without presenting them as
probabilities.

The worker is research-only: no credentials, broker client, signal-feed write,
lifecycle write, selected side, authorization, promotion, order, or position
surface exists. Focused geometry/collector/dashboard/isolation validation passed
71 tests.

Implementation is complete; evidence remains pending:

1. Accumulate matured executable outcomes and effective independent episodes;
   raw visits or correlated pair expressions do not establish support.
2. Compare the frozen bounce and break arms against no-trade after costs,
   concentration stress, and signed-currency-factor/market-episode
   deduplication.
3. Any future candidate must pass untouched prospective confirmation and the
   existing lifecycle/canary boundary before promotion. There is currently no
   confirmed level-band candidate and no execution permission.

## Level-collision and U.S. source-clock checkpoint — 27 August 14:43 ET

The completed 68-pair level-reaction census used 3,302,722 retained executable
M1 rows. Every pooled unconditional bounce and unconditional break cell was
negative after costs. The two positive USD/JPY means were best-day/outlier
concentrated and became negative when that day was removed. The immutable
diagnostic is
`data/oanda_training_manager/reports/level_reaction_census/LEVEL_REACTION_CAUSAL_CENSUS_20260827T181303Z.md`.

The bounded `causal_level_approach_ledger_v1_frozen_20260827b` replay now uses
right-confirmed pivots, integrity-gated prior-week pivots, spread/pip/ATR-scaled
approach and first-passage barriers, executable bid/ask paths, explicit
`ambiguous_intrabar` outcomes, and exact contiguous wall-clock horizons at
5/15/30/60/120/240 minutes. Its USD/JPY diagnostic contains 3,868 approaches
and 23,208 outcomes; all rows are `historical_diagnostic`, SQLite
`quick_check=ok`, and the policy explicitly prohibits prospective proof,
selection, promotion, authorization, or execution.

U.S. official-source coverage also advanced:

- BEA RSS is now an explicit statistical fast-lane source with 90-second normal
  polling, a 15-second 08:25–08:45 ET burst, exact publisher/first-seen clocks,
  and bounded source-native blurb extraction. The fast lane currently completes
  29/29 configured transports with zero errors. All 48 BEA rows present at
  onboarding are correctly bootstrap/nonprospective; no consensus or direction
  was inferred.
- Kansas City Fed news releases are a direct versioned USD policy/event source.
  The official Jackson Hole calendar freezes separate clocks for the 27 August
  20:00 ET full-agenda publication and the 28 August 10:00 ET Federal Reserve
  Chair remarks. Scheduled existence supplies timing, not a directional side.
- HTML listing history now retains a separate append-only
  `bootstrap_item_urls` quarantine. Initial back-catalog links cannot become
  prospectively new after the rolling known-URL cap, a restart, a database
  rebuild, or migration from the prior state contract. The repository contract
  is V74. The broad collector was reloaded only after its 186/186-source V73
  cycle completed, then completed a full V74 cycle at 14:50:29 ET. The live
  Kansas City Fed source is HTTP 200 with all 28 inherited listing URLs retained
  in the bootstrap quarantine; the two Jackson Hole clocks are also live under
  V74. No pre-cutover catalog item was reclassified as prospective.

Validation passed 24 causal-level tests, 32 official-fast-lane/coverage tests,
and five focused BEA/Kansas-City/bootstrap-quarantine tests. No execution file,
Practice-007 gate, threshold, authorization, order, or real-money route changed.

Remaining proof work stays open and fail-closed:

1. Build a separately reviewed prospective pre-outcome level collector that
   appends the level/feature decision and exact live bid/ask entry before any
   horizon can mature; historical replay can never populate that cohort.
2. Acquire permitted causal pre-release consensus/dispersion and timestamp-safe
   event-time rate/OIS repricing; official publication or schedule alone does
   not establish currency direction.
3. Accumulate untouched confirmation across independent market episodes for
   source-factor, narrative, level, predictor, and allocator hypotheses without
   interim retuning or gate relaxation.

## Causal source-map improvement checkpoint — 27 August 11:11 ET

The official-source map now measures depth rather than treating transport
coverage as proof. Across 21 currencies × eight event families (168 cells),
all 168 have a configured and operational official transport, 163 are healthy,
43 have an exact-family structured parser, 13 have emitted an exact numeric
observation, 14 have a prospective causal observation, eight have any semantic
direction, and zero yet have a prospective matured causal response or
prospective semantic direction. The diagnostic is
`data/oanda_training_manager/reports/official_currency_source_depth_readiness_v2/OFFICIAL_CURRENCY_SOURCE_DEPTH_READINESS_V2_CURRENT.md`.

A separate research-only causal response ledger now retains official events
even when their semantic currency score is zero. It freezes source-native
factors at their actual knowledge clock, records exact live bid/ask entries for
new events, solves the all-pair currency factor, and matures both executable
directions at 1/5/15/30/60/120 minutes with MFE, MAE, first cost clearance, and
response shape. Effective N is deduplicated by the underlying market episode:
one policy decision, projections release, implementation note, and press
conference count once, while separately scheduled decisions remain distinct.
Late-known factors cannot borrow an earlier entry or response; causal consensus
requires a pre-release observation clock and explicit provenance.

The clean prospective cohort activates at 13:00 ET on 27 August. Its current
pre-activation diagnostic ledger contains 11 canonical events / 11 market
episodes, 91 factor observations, 65 matured horizon responses, and 546
prequential rows. Of those, 402 abstain for low support and 144 diagnostic-only
forecasts are explicitly ineligible for proof. Prospective proof events,
forecasts, and outcomes remain zero. The prior implementation census is
quarantined under `PREVALIDATION_20260827T143754Z` names.

The exact policy-fact framework is also active. Its first bounded adapter is
SARB/ZAR and requires a complete action, current rate, derived prior rate,
signed basis-point delta, structured vote objects, and schedule/receipt/causal
clocks. It currently holds three future scheduled decisions and zero collection
attempts or facts; incomplete releases abstain. The next eligible SARB decision
is 23 September 2026 at 13:00 UTC. Complete exact policy-decision tuples remain
0/21, so the next repository work is source-specific exact adapters for the
remaining monetary authorities—not another generic keyword rule.

The three new diagnostics are hidden/background and have no lifecycle,
authorization, promotion, watchlist-mutation, execution, or broker surface.
The combined focused suite passed 162 tests and the new SQLite ledgers report
`quick_check=ok`. Practice 007 gates are unchanged and supported execution
remains `no_trade`.

## Bounded live-window final checkpoint — 27 August 09:00 ET

The frozen Monday-through-09:00 report is
`data/oanda_training_manager/reports/week_to_date_event_move_audit/WEEK_TO_DATE_PROJECT_EVENT_MOVE_RECAP_20260827T130000Z.md`
with matching JSON and SHA-256 manifest. It retains 34 official items collapsed
to 23 independent five-minute currency/event clocks, 907 factor-deduplicated
movement episodes, 31 material episodes at or above 15 bps, and zero strict
pre-move directional news matches. Eighteen official clocks had some
retrospectively selected after-cost path, but causal pre-release consensus
remained zero and none is treated as a forecast win.

Practice 007 remained flat at balance/NAV $41.6042, cumulative P/L -$8.3430,
zero trades/orders, and zero margin. All 68 quotes remained observable and the
entry path stayed fail-closed with zero confirmed candidates, zero fresh canary
authorization, and real-money routing disabled.

Completed repository-controlled repairs in this window:

- separated central-bank policy sources from national-statistics transports;
  policy coverage is 21/21 currencies and 68/68 pair legs operational, while
  four statistical fast-lane sources are independently classified;
- added the Stats SA scheduled PPI PDF, DOL claims direct PDF, and Census exact
  target RSS to the research-only fast lane; the lane now completes 28/28
  configured sources with zero errors and Census remains bootstrap-only for the
  already-published 27 August release;
- retained the complete ECB accounts body and exact publisher/probe clocks,
  and added a date-scoped 15-second event burst so the special window cannot
  repeat every day;
- fixed U.S. watcher restart relabeling, unstable Census raw-XML signatures,
  DOL optional revision wording, material-identity semantics, and Windows hash
  sidecars without rewriting historical rows;
- fixed the project-integrity concurrent-heartbeat race while continuing to
  reject implausibly future heartbeats;
- made the week-to-date compiler recover bounded pre-cutoff account, narrative,
  and source-readiness snapshots and inventory post-generated diagnostic cases
  only when their explicit evidence cutoff is within the report cutoff;
- replaced the edge-evidence worker's multi-hour read transaction with frozen
  row-id highwaters and short ordered pages. A standard SQLite checkpoint
  reduced the pinned strategy WAL from 1.44 GB to roughly 32 MB without vacuum
  or evidence deletion. The refreshed guard is `ok`: 130.709 GiB free and
  about 160.957 days to the 50-GiB guard at measured positive growth.

Live event results are preserved separately: Stats SA PPI, the ECB accounts,
and the simultaneous Census/DOL 08:30 bundle. The U.S. bundle was mixed and its
deduplicated USD factor was only +1.23736 bps at 30 minutes; six of 20 USD
expressions cleared their observed spread after the fact. ECB weakness peaked
near 30 minutes and mostly retraced by 60 minutes. Neither case had causal
consensus or event-time rate repricing, so no directional claim or order was
made.

Validation after all changes: the combined focused suite passed **558 tests**.
Five lifecycle/accounting/genealogy/trial/position-ledger SQLite quick checks
returned `ok`; the edge worker is supervised with zero errors and bounded RSS.

Current repository-controlled implementation backlog: **none from this
checkpoint**. Remaining gates are external data or untouched evidence:

1. Permitted, causally archived pre-release consensus and dispersion.
2. Intraday event-time OIS/futures/rate repricing (daily rates remain context).
3. A causal intraday oil/commodity transmission source for NOK/high-beta moves.
4. Independent prospective repetitions across regimes for the frozen event,
   narrative, predictor, and allocator cohorts.
5. RBNZ direct-page access remains HTTP 403; its official calendar/search and
   other New Zealand official sources remain operational and must not be
   bypassed.

## Current causal movement/news checkpoint — 27 August 04:12 ET

The prospective narrative clock is now V12. Five-minute states seal only after
bucket close plus a 60-second ingestion grace, the open bucket is exposed only
as an explicitly incomplete `partial_live` view, and outages remain gaps rather
than being reconstructed later. The append-only V12 database currently covers
exactly 21 currencies and 68 pairs with zero incomplete seals, zero late-arrival
incidents, and zero seal-gap incidents. V11 is frozen as historical evidence.

The live movement/news join is now prospective V6R2. V5 incorrectly attached
one current narrative snapshot to mover starts that could precede it. The first
V6 engineering preflight then exposed a second boundary issue: integer timestamp
conversion could make a seal at `:00.265` appear available at a mover start of
`:00.000`. Those ten never-consumed rows remain quarantined. V6R2 uses full
sub-second comparisons and accepts a meter state only when both bucket close and
seal time are no later than the individual move start. It records the exact
meter contract, clock, seal, leg states, and join rule; `partial_live` is never
an input. V2R2 outcomes and V3R2 persistent context are separate new cohorts.

The narrow cutover froze only V5/V1/V2 research diagnostics. The 68-pair quote
stream and Practice-007 executor PIDs were preserved, the account remained flat,
and no entry, exit, promotion, authorization, or real-money policy changed. The
focused V12/V6R2/integrity/dashboard/current-week suite passed 107 tests under
the canonical runtime; the only warning was the existing optional pytest-cache
permission warning.

The corrected current-week audit now binds dynamically to Monday 00:00 UTC
through its generation time instead of silently reusing 17–21 August. Through
07:54 UTC it found 23 substantive official items collapsed to 18 independent
event clocks: eight calendar-only, 15 with some retrospectively selected
cost-clearing pair/horizon response, three strict timing/movement
correspondences, and zero of those three with a defensible source-side
direction. Causal pre-release consensus observations and strict publishable
directional wins both remain zero.

Current repository-controlled implementation backlog: **none from this
checkpoint**. Remaining gates are evidence/data dependencies, not permission
to loosen Practice 007:

1. Causally archived pre-release consensus and revision dispersion from a
   permitted source; Trading Economics remains credential/licence blocked.
2. Event-time rates/OIS or equivalent market-repricing evidence for mapping
   policy/inflation text to a currency side.
3. Untouched prospective outcomes across independent market episodes for V12,
   V6R2, the official response cohort, and the frozen predictor/allocator
   cohorts.

## Canonical news outcome review checkpoint — 26 August 14:38 ET

News-hit, miss, and unexplained-mover reviews now use one on-demand append-only
record. It opens the immutable signal/news and live-mover databases read-only,
collapses correlated pair fan-out to one event/currency-factor thesis,
separates executable-cost buckets, diagnoses direction, magnitude, latency,
stale-context, source-binding, and technical-confirmation failures, and
refreshes an evidence-gated shadow queue only when explicitly invoked. It is
not an always-on worker and has no recurring CLI mode. It cannot access the
broker, place orders, promote hypotheses, or modify executor or authorization
policy.

The latest V2 checkpoint contains **3,010 effective theses**, **930 diagnostic
wins**, and **1,903 misses or gaps**; the immutable ledger retains 3,011
diagnoses in total. These mixed event, mover, and horizon arms are not an
execution win rate. The largest recurring problems are no economic edge
after costs, bad direction/entry, absent strict signal, and post-detection
giveback/reversal. A repeat run inserted zero diagnoses and zero snapshots,
proving idempotence. The V1 preflight database is preserved as superseded
implementation evidence rather than rewritten.

Two logic leaks found by the review are fixed prospectively:

- contextual/research-only events can no longer enter the accepted forecast
  ledger by borrowing a pair-level direction; accepted events must themselves
  be `forward_pair_eligible`;
- current inflation-expectations language is no longer inverted by an older
  comparison clause, and unverified secondary inflation-expectation stories
  remain research-only until event-time rate repricing exists.

Current family-category aliases now collapse inflation and labour rewrites to
the same currency-factor evidence unit. Nothing here loosens Practice 007.
The remaining 19 queue entries are retained research hypotheses requiring new
cohorts and prospective evidence. They refresh only during an explicit review;
they are not unfinished code defects or permission to self-tune execution.

Reload validation also found two downstream version-binding defects. The
official response worker no longer duplicates an old classifier literal, and
the official mapper now remaps each observation append-only for a new
classifier version while reporting current-version counts separately from
retained historical versions. This prevents both a silently stale response
path and false evidence inflation after a classifier migration.

The live mover outcome worker no longer recomputes every immutable matured
path. It skips completed case/arm/horizon keys and uses an indexed candle-time
lookup for the remaining paths. The first supervised optimized cycle matured
109 new outcomes, skipped 13,034 completed paths, finished in seconds rather
than more than ten minutes, and restored the full project-integrity state to
`ok` with zero failures.

The completed implementation and evidence checkpoint was synchronized to the
shared vault at 17:17:38 UTC (1,288 files; 663,016,886 bytes). The 19 items in
the generated improvement queue are prospective research hypotheses awaiting
new evidence, not unfinished implementation work.

The V143 retained-article migration is draining automatically in bounded
5,000-row batches. One batch temporarily caused the completed news view to age
beyond the consumer limit, and the signal layer correctly hard-blocked until a
fresh view published; it recovered without intervention. After the migration
finishes, profile and, only with exact-output equivalence tests, index the
retained-evidence load and topic clustering paths. Do not hot-patch or relax
freshness during the active migration.

## Current prospective response checkpoint — 25 August 00:29 ET

Official response-watch V3 is deployed as a new zero-row cohort with immutable
1/5/15/30/60/120-minute executable bid/ask outcomes. The previous V2 database
and files remain untouched. The extra 30/60/120-minute arms were preregistered
because the movement-conditioned discovery audit's official-only 120-minute
subset was the clearest small signal (9/12 overall and 6/7 on liquid majors),
but those observations are correlated, selected, and far too few to prove an
edge. V3 must wait for genuinely new mapper-certified official releases.

The V3 worker is supervised, its exact contract/cohort/horizon set is enforced
by project integrity, and it remains unable to trade, authorize, promote, or
change the canonical watchlist. This is prospective measurement, not a gate
change or a backfilled promotion claim.

## Current transport-integrity checkpoint — 25 August 00:20 ET

The repository-controlled SARB RSS repair is now V7. V6 correctly retained
certificate and hostname validation while tolerating unavailable Windows
revocation lookup, but its first integrated V72 request exposed a separate
intermittent connection-reset condition. The failed V72 attempt is preserved.
V7 adds exactly two curl retries with `--retry-all-errors`; a live probe
required one retry and then returned HTTP 200, followed by two first-attempt
HTTP 200 responses. All three returned 19,096 bytes and the source parser
returned 25 official RSS items. Collector V73 is tested and awaits the next
safe V72 cycle boundary. `--insecure` remains forbidden and existing source
observations are not rewritten.

The remaining source failures are not silently fixable: RBNZ direct surfaces
return 403 under published access controls; China Customs fails the trusted
certificate chain on this host; Trading Economics lacks a paid credential;
and disabled replacement/predecessor sources remain intentionally inactive.
Do not bypass TLS, publisher controls, or licensing to inflate source health.

## Current mover/source mapping checkpoint — 24 August 23:47 ET

The Treasury/Iran case exposed a visibility and interpretation issue, not a
transport outage. The direct Treasury listing observed the two official
Operation Economic Outcast pages at 17:42 and 18:06 UTC. An early classifier
version mislabeled the first headline; the later governed version correctly
identified a high-severity sanctions/risk-off factor. Its persistent semantic
projection was long USD/ZAR, however, while the 22:58 UTC USD/ZAR move was
down. This is preserved as a mapping miss. It must not be rewritten as a
timely correct forecast or used to retune the active cohort.

`oanda_live_move_persistent_news_context.py` V2 now keeps source factors visible
only through their already-declared research horizon, separately from the
narrow two-hour causal mover join. It reports raw event versions, independent
stories, all-source, official-publisher, and direct-authority-leg direction,
and actual-move alignment. This prevents a U.S. Treasury spillover inference
from being mislabeled as direct AUD, CAD, EUR, HKD, or NZD authority evidence.
The first live output shows 78 raw USD/ZAR event versions / 73 independent
stories; both the aggregate and official persistent sides were long and
opposed the drop. The worker is supervised, exact-contract checked by project
integrity, and permanently unable to execute, authorize, or promote.

The next evidence gate is unchanged: learn whether repeated, independently
observed sanctions/policy factors have a stable currency response, including
delayed/reversal states and commodity/rates confirmation. One contradicted
case is a mapping diagnostic, not a reason to invert or loosen the live rule.

## Current highest-value source blockers — 24 August 23:32 ET

The repository-controlled official-release latency and response path is
complete. A V4 append-only fast lane observes the 23 central-bank release
transports for all 21 currencies independently of the broad collector cycle.
Mapper V3 applies V142 within five seconds and separately records a direct,
verified prospective semantic hypothesis from the stricter publish gate; this
prevents missing consensus/rates from erasing shadow evidence without
weakening any execution boundary. Response-watch V3 freezes exact bid/ask
1/5/15/30/60/120-minute news-only and technical-alignment diagnostics across all pair
expressions, then deduplicates results by source/currency factor episode and
lowest spread. It cannot mutate the canonical watchlist.

The first stable checkpoint has 558 raw observations, 558 mappings, 22
observed transports, 11 historical semantic directions, zero prospective
semantic candidates, zero publish-eligible candidates, zero response watches,
and zero outcomes. V1-V3 raw-lane outputs, mapper V1/V2, and response V1 are
preserved as explicit superseded/quarantined or zero-row engineering evidence;
no consumer can treat them as proof. Exact integrity sentinels are green.

The next gate is an actually new official release observed prospectively by
V4. Only then can mapper V3 and response V3 measure first-seen latency,
semantic direction, technical relation, and after-cost response without
bootstrap contamination. Response V2 is preserved as the superseded 1/5/15-
minute zero-row predecessor. V3 remains a frozen shadow cohort; it cannot alter
Practice 007, the canonical watchlist, or the current proof families.

The latest repository-controlled pass is complete. The all-68 M1 forward
archive now refreshes every five minutes instead of hourly, outcome reports
separate four executable cost buckets, archive lag is explicit, and signed
factor evidence selects one immutable earliest case per episode even if the
live representative pair later changes. Eighty-five quote-bound paths have
matured; the liquid 5-minute broad-context and technical arms are both 0/4
after factor deduplication. The only positive point estimate is 1/2 at the
15-minute broad-context horizon, which is not evidence of an edge. Continue
the frozen prospective collection without retuning or widening execution.

The current AUD policy-release gap is also repository-closed for future
events. A second first-party RBA minutes transport polls the official minutes
listing every 60 seconds and was validated against five exact 2026 documents.
The existing contents are bootstrap/history-only and cannot rewrite today's
publication clock; future unseen documents will be prospectively timestamped.
The secondary recap arrived about eight minutes after today's scheduled
release, so that historical case remains a latency diagnostic. Collector V67
and the new central-bank map contract prevent older code/config combinations
from certifying the source.

The exact RBA minutes body is also retained as late-observed context under a
new source cohort. Policy minutes no longer flatten background market pricing
and the Board's decision into one unordered phrase bag: V142 extracts the
concluding guidance and explicit decision while keeping the complete document
for audit. It also guards date-only titles such as `5 May 2026` from treating
the month as a modal verb. All five official 2026 minutes now map consistently:
three actual increases at +1.0 and two conditional hawkish holds at +0.55.
They remain bootstrap-only, noncausal and publish-ineligible. This is a
semantic diagnostic, not a backtest result or execution permission. Collector
V71 is the first runtime allowed to emit it.

The replay-integrity implementation is complete: repeated page/state polling
no longer counts as repeated economic releases. Exact official release clocks
now cover **21/21 currencies and 136/136 pair legs** in the strict historical
availability replay: 56 deduplicated events, 47 independent currency-factor
episodes, and 44 unique source timestamps. It remains research-only and gives
no direction or execution authority.

The next source work is narrow and factual:

1. Restore a direct RBNZ policy-release transport. The current official RBNZ
   page family returns HTTP 403 from this host, and RBNZ's published terms say
   automated access requires prior permission. The supported paths are an
   allow-list request or a permitted official email/social subscription.
   Keep the healthy official calendar, Stats NZ, Treasury, and
   official-publisher discovery fallback, but do not bypass publisher controls
   or promote a discovery proxy to direct-source status.
2. Continue prospective exact observation for all 21 currencies. Retained
   actuals are 21/21, but prospectively observed numeric actuals and causal
   pre-release consensus remain 0/21.
3. Accumulate untouched comparable-event outcomes under the new neutral
   magnitude preflight. It attaches priors only to a matching release series
   or sufficiently sampled event class, never supplies direction, and cannot
   change Practice-007 gates. Six of 69 current event/currency rows have a
   usable comparable-history prior. The September RBNZ policy event uses a
   frozen 12-event schedule archive whose median H1 absolute NZD-factor move is
   51.15 bps; this is magnitude-risk context only, with no causal side or
   matched-control claim. The rest remain explicitly unavailable.

No other code defect from this pass remains open. These three items are source
availability, untouched evidence, or a new frozen research hypothesis—not
permission to loosen execution.

Live movement/source inspection is no longer blocked on the heavy historical
batch. `oanda_live_move_news_snapshot.py` now refreshes the top ten
cost-clearing velocity legs every two minutes from an exact narrow source
window, and the dashboard exposes strict forward evidence, broad context, and
narrative state separately. At the first checkpoint, 0/10 current movers had a
strict publish-eligible pre-move direction despite 17–60 raw nearby events per
pair. Continue collecting this null/coverage evidence; do not convert broad
context volume into causal direction.

That diagnostic is now durable and prospective. Its V4 append-only case
ledger binds the first fresh bid/ask quote, separates five shadow arms, and
deduplicates correlated pair expressions by signed currency factor and market
clock. A representative checkpoint reduced 10 pair movers to six independent
factor episodes. The 5/15/30/60-minute outcome worker is supervised and begins
with zero matured outcomes; continue unchanged until the forward M1 archive
passes each declared horizon. Do not interpret the current broad-context
alignment as edge because movers were selected after their initial movement.
Raw and signed-factor-representative outcome statistics are now reported
separately. An exact live integrity sentinel rejects stale snapshot contracts,
contract mismatches, stale artifacts, non-inert flags, or an unhealthy outcome
database. The stale V3 live process found during deployment was restarted and
the supervised state now emits the quote-bound V4 contract. There is no
remaining repository repair in this path; the gate is untouched later price.

The Riksbank single-channel latency gap is repository-closed for future
events: a second direct official HTML release listing is live beside RSS and
observed HTTP 200 with the exact August decision link. The first listing
snapshot is bootstrap/history-only and cannot rewrite the 20 August clock.
Future evaluation must measure which official channel is first and preserve
both clocks. RBNZ direct-site access remains the external permission blocker.

There are **no unimplemented repository-controlled changes** in this section;
the remaining gates require external transport, future observations, or a new
pre-registered research cohort.

Completed integrity repairs from the same pass: the watchlist comparison-arm
change now has collector cohort V40 with immutable V39 as parent, eliminating
the genealogy definition conflict; the independent verifier is back to an
exact match. The unused after-cost V6 cohort is terminally retired with an
exact mismatch certificate because two mutable producer paths changed after
its manifest freeze. It had zero evidence or operational rows and cannot be
restored under the old cohort ID.

## Locked target architecture — continuous currency narrative meter

The news branch has one stable target.  It is a continuously sampled
``[-1,+1]`` sentiment/state meter for each of the 21 currencies, paired with a
separate faster technical reaction/timing layer across all 68 instruments.
News is the slow directional prior (the weather forecast); price reaction,
breakout state, spread, and liquidity are the fast entry/exit observations
(the seismograph).  Neither layer may be reported as the other.

Every currency-time observation must retain:

- meter score in ``[-1,+1]`` plus separately calibrated ``P(up)``, ``P(down)``,
  abstain probability, uncertainty, and expected reaction horizon;
- attention level and acceleration, novelty, source-family agreement and
  disagreement, story-cluster count, decay, and the contributors that produced
  the score;
- exact dictionary/classifier/model version, source-weight version, decay
  formula, feature contract, observation cutoff, and cohort ID;
- technical reaction state observed only at or after that cutoff, including
  breakout/change point, currency-factor movement, quote flow, spread, and
  executable cost clearance;
- matured direction, magnitude, MFE/MAE, timing, and after-cost outcomes for
  sentiment-only, technical-only, conjunction, disagreement, and no-trade arms.

Pair state is always derived consistently as base-currency state minus
quote-currency state.  Raw article counts are not votes: syndications collapse
to one story cluster, related publishers collapse to an independent source
family where appropriate, and source weights are learned and reported without
allowing one discovery aggregator to manufacture consensus.

The original recovered blurb rules remain candidate model inputs.  They must
be compared with improved dictionaries, source-native numeric factors, vendor
sentiment, and calibrated statistical classifiers under the same clocks and
outcomes.  A model ledger must preserve every frozen formula and its results so
failed mappings are not silently replaced, retuned, or rediscovered.

Storage is two-tiered.  Trading-facing workers consume compact immutable
features and scores.  Reproducibility retains the smallest legally permitted
point-in-time source record: source/story/event identity, title or structured
fact, timestamps, source URL, version/content hash, parser output, and—for
official releases or permitted material—the captured document/body.  Full
general-news prose need not be duplicated indefinitely, but hashes alone are
not sufficient when a mutable or disappearing page would make the historical
classification impossible to reproduce.

## Implemented checkpoint — narrative acceleration and source incremental value

The requested historical-first checkpoint is complete. The reproducible,
read-only quality audit is at
`data/oanda_training_manager/reports/news_historical_quality/NEWS_HISTORICAL_QUALITY_V1.md`.
The source-first 21-currency response diagnostic is at
`data/oanda_training_manager/reports/news_currency_response/NEWS_CURRENCY_RESPONSE_DIAGNOSTIC_V1.md`.

The baseline found 72,193 current-view articles over 27 UTC days, 63,494
immutable story clusters, 68/68 executable M1 archives, and 21/21 represented
currencies. There are 5,151 retrospectively classifiable direction rows and
1,890 forward-timely rows, but zero fully point-in-time/prospective matured
direction rows and zero causal actual-versus-consensus surprises. Across 6,368
source-first currency clocks, the simple semantic direction rule was negative
after costs at 5/15/60 minutes and barely differed from a within-currency
direction permutation. Fifteen-minute trend confirmation made it worse.

The first implementation of that narrower sequence is now complete:

- `oanda_continuous_narrative_meter.py` writes dense five-minute states for all
  21 currencies and all 68 base-minus-quote pair hypotheses. It version-tracks
  six formulas, collapses exact story identities, balances source families,
  and records attention, acceleration, novelty, agreement, and decay. It is a
  supervised research worker with no broker, authorization, or promotion path.
- `oanda_continuous_narrative_backtest.py` compares sentiment-only,
  technical-only, sentiment-plus-technical, sentiment-plus-breakout, and
  conflict arms on the same executable clocks at 5m, 15m, 1h, 4h, and 1d.
  It uses chronological 60/20/20 day splits and an explicit formula ledger.
- The v2 historical run scored 72,298 decisions, 12,435 price-covered clocks,
  and 779,863 formula/arm/horizon outcomes. The narrative-acceleration event
  cohort was negative after costs at every horizon in the untouched test
  split; its technical-confirmed and breakout-confirmed arms were also
  negative. No cell with at least 30 rows was positive in both validation and
  test. The one positive 1d technical-only test slice failed in train and
  validation, so it is not evidence of edge.
- The recovered blurb database remains useful for causal attribution and
  analog discovery, but its 2,316 factor responses contain zero prequential
  orientation rows. `recovered_blurb_analog_v1` is therefore registered but
  inactive rather than being credited with hindsight direction.
- The dashboard now shows the live research meter beside Practice 007. The
  retired Practice-006 panel and worker were removed. Practice 007 remains the
  only broker account in scope and is unchanged.

Latest full-history comparison (24 August 20:20 ET): V11 was rebuilt over all
retained point-in-time history, producing **168,378 currency clocks across 29
market days**, and V10 evaluated **8,428 non-overlapping decisions / 91,933
executable bid/ask outcomes**. No model/technical arm was positive after costs
across train, validation, and test. Source-quality slices show only **18**
trusted forward-timely decisions; the much larger timely sample is secondary
or untrusted discovery evidence. Exact decision-stream fingerprints also show
that six named formulas are only **five independent streams** because
`published_semantic_v1` and `source_balanced_v1` are identical. The live worker
continues unchanged; this result does not authorize retuning or execution.

The supervised narrative-worker freshness defect is closed. The old
PowerShell process had retained a retired V8 output path and repeatedly
restarted the healthy V11 worker. The same supervisor command was reloaded
hidden; its current heartbeat monitors V11, keeps the worker PIDs, and reports
zero stale-output restarts.

No operational bug or execution relaxation follows from the null result. The
remaining research sequence is deliberately narrower:

1. Replace headline-string-only clustering with a stronger independent-story
   cluster so near-syndicated rewrites cannot overstate evidence.
2. Separate direct-currency policy/macro evidence from broad global-risk
   propagation; measure independent source-family confidence rather than using
   absolute meter score as confidence.
3. Accumulate immutable live outcomes under the frozen v2 observation clock.
   Do not retune the formula using interim results.
4. Repair or rate-limit the stale GDELT discovery channel, then test its
   incremental value rather than its standalone marketing sentiment score.
5. Keep official numeric direction blocked until actual, causal pre-release
   consensus, revision, and event-time rate repricing are available. Absolute
   data levels alone do not set a currency side.

Completed from the 24 August 13:20 ET live audit: the immutable
`secondary_directional_discovery_v1` arm now admits deterministic secondary
rows while remaining separate from published/direct evidence and permanently
ineligible for execution. Frozen predecessor meters remain untouched. V8 also
collapses secondary syndications conservatively and applies real magnitude
decay; the motivating sanctions copies fell from 85–86 apparent stories per
currency to four. The arm remains an unvalidated research hypothesis.

Completed from the same live audit on 24 August: six immutable
`news_remaining_move_*_v1_20260824` comparison cohorts now evaluate every
current liquid pair expression at 5/15/30 minutes. They subtract movement
already completed in the thesis direction, decay predicted magnitude over the
declared arm horizon, require at least `1.5x` modeled executable cost and
`0.55` model cost-clearance probability, and preserve all missing, conflicted,
stale, priced-in, or under-cost expressions as separate negative controls. The
predecessor `news_magnitude_ranked` cohorts and every Practice-007 gate remain
unchanged.

Live audit addition (24 August, 14:44 ET): secondary commentary can contain an
explicit completed FX move in its body even when the headline is political.
The observed example said `USD/CAD was last up 93 pips` but was categorized as
a labour release because its long body also discussed unemployment. At the
next safe collector contract boundary, add a conservative body-level explicit
pair/pip recap veto and distinguish incidental labour discussion from a real
labour release. Preserve the current row and its late first-seen clock as
diagnostic evidence; do not rewrite it or infer a fresh CAD signal.

These are research tasks, not authorization or execution tasks. Practice 007
remains governed `no_trade`; real-money routing remains disabled.

The prior implementation queue has been worked through. Its completed
checkpoints are retained in `FOREX_PROJECT_LOG.md` and in timestamped vault
checkpoints; they are not repeated here as if they were still pending.

The latest requested ten-case historical pass is complete. Ten previously
unlinked factor-move episodes are now frozen in
`data/oanda_training_manager/reports/major_move_case_audits/ten_uncovered_factor_cases_v1/`,
including an idempotent append-only case log and explicit causal-clock,
factor-breadth, event-sequence, and semantic-direction safeguards. These are
completed research controls, not pending execution changes.

The prior Practice-006 currency-rank challenger is retired and no longer
supervised. Its historical files remain evidence, but there is no live 006
worker or dashboard account panel. Practice 007 is the only account in current
scope.

Remaining limitations are source availability or future evidence collection,
not unfinished code changes. Their canonical inventory is
`config/forex_source_gap_register_v1.json`. The active research direction is
source-first:

- preserve and extend exact first-party releases for all 21 currencies;
- capture genuinely causal pre-release expectations when a permitted source
  exists, never post-release backfills masquerading as consensus;
- add timestamp-safe event-time policy-rate repricing where freely available;
- accumulate untouched future source repetitions, including the frozen RBNZ
  2 September 2026 shadow observation;
- expand original-authority reconstruction of unresolved movement episodes
  without forcing attribution.

These items cannot be truthfully marked as implementation defects, loosened
gates, or executable candidates. Paid procurement remains out of scope.
The governed Practice-007 decision remains `no_trade`; real-money routing is
unchanged and disabled. The live narrative meter is observation-only and is
not evidence of validated edge.

## Current-week case-derived queue

Completed in this pass:

- correct explicit `currency stronger/weaker` recap semantics;
- prevent retrospective/research-only context from receiving causal-win credit;
- compile and freeze ten exact cases from 17–21 August 2026;
- recover the stale all-68 M1 archive and supervise hourly atomic extension;
- retain unexplained moves rather than forcing the nearest headline attribution.
- scope official foreign-policy semantics to the current headline and lead so
  archived related-story war language cannot contaminate a routine release;
- scope U.S. Treasury duration/buyback semantics to the current release lead
  so sanctions pages cannot inherit an older buyback item from page furniture;
- separate scheduled calendars from observed releases and exclude newly
  discovered archive/navigation pages from current-event credit;
- freeze a 68-pair current-week official-event replay: 40 source items collapse
  to 24 five-minute currency/event clocks and five strict movement/source
  correspondences, while **zero** currently carry a causally defensible source
  direction because pre-release consensus and rate repricing are absent;
- require the named currency to be an extreme cross-sectional mover, dominate
  the other pair leg, arrive within five minutes, and clear executable cost
  before an event/move correspondence is listed as a strict attribution
  candidate. These remain retrospective research, never execution evidence.
- expand the movement-first record by another 23 current-week factor episodes:
  13 live/selected episodes plus ten mechanically selected independent 15/60m
  peaks. Every unresolved episode stays unresolved rather than inheriting the
  nearest headline;
- distinguish pair-ticker and commodity-price recaps from causal events, keep
  market-policy expectation stories such as “traders bracing for a hawkish
  ECB” research-only even when syndicated, and reconcile stale topic rows as
  soon as recent articles are reclassified;
- treat an expiring ceasefire as failed de-escalation and abstain on a mixed
  threat-plus-tentative-deal headline instead of mapping the word “deal” to a
  risk-on basket;
- preserve multi-word currency recap direction (for example, “New Zealand
  Dollar extends rally”) rather than letting the accompanying Fed clause make
  both currencies bearish;
- freeze classifier `local_fx_news_rules_20260821_v131` and collector
  `local_news_incremental_source_commit_v59_20260821`; the complete relevant
  regression suite passes **438 tests**;
- audit exact five-minute factor bursts. This added one timely-but-late
  official AUD direction hit, one opposed JPY/oil-recap case, and one repaired
  pair-grammar inversion (`EUR/USD ... dollar pressure`) plus named slash-pair
  inversions (`Dollar/Yen falls`, `Thai baht/US dollar stronger`);
- add immutable underway-move/event-decay comparison arms across every current
  liquid pair expression, with explicit cost-clear and negative-control
  cohorts at 5/15/30 minutes. The predecessor ranking arms remain frozen;
- harden macro-consensus readiness so inline/post-release consensus values
  cannot self-certify causal availability; expose raw, causal, and noncausal
  counts separately;
- add immutable official-surprise-plus-rate-repricing confirmed and negative-
  control cohorts. They require a causally captured standardized surprise,
  explicit series direction, and aligned timestamp-safe 15-minute rate
  repricing; missing inputs remain explicit abstentions;
- add the non-destructive `(cohort_id,status,arm)` watchlist evidence index,
  removing repeated wide-table scans from current-cohort reports.

Still open, research-only, and prohibited from changing execution gates without
independent evidence:

1. **Causal consensus acquisition and series semantics** — the fail-closed
   adapter and proof binding are complete, but no permitted live pre-release
   consensus source is connected. Add source-native actual, causal consensus,
   prior, revision, components, and explicit currency-direction semantics;
   continue abstaining until then.
2. **Rate-market source acquisition** — the confirmation binding is complete,
   but the intraday rates contract remains disconnected. Add timestamp-safe
   yield/OIS/policy-path repricing; daily government yields and ALFRED data must
   not masquerade as event-time confirmation.
3. **Source-watch timing arms — V2 implemented, evidence pending** — the
   neutral V2 causal source-factor ledger now observes inert
   1/5/10/15/30/60/120-minute executable responses after fast official-source
   arrivals, including unresolved initial direction. V1 is sealed and
   preserved as an inactive 1/5/15/30/60/120-minute cohort; no V1 evidence was
   rewritten. V2 remains at zero prospective proof events until post-activation
   releases arrive and mature.

The current-week evidence does not authorize looser entry thresholds: in the
ten selected movement-first cases, six of nine nearby source hypotheses were
opposed, three aligned, and one case had no suitable source. The independent
official replay has five strict timing/movement correspondences but zero
resolved causal directions. These items are the next evidence work, not
practice or real-money promotion candidates.
