# Forex project log

Canonical timestamped decision and implementation log. Append new entries;
do not rewrite prior decisions to reflect later knowledge. Active unfinished
work belongs in `FOREX_PENDING_IMPROVEMENTS.md`. Source availability belongs in
`config/forex_source_gap_register_v1.json`.

Historical states before this log was introduced remain preserved in the
timestamped model-vault checkpoints and the existing specialist reports.

## 2026-08-29 18:40 America/New_York — Sequential portfolio replay independently verified

- Added a new content-addressed `sequential_portfolio_replay_v1` sidecar rather
  than mutating the hourly deliberate-practice case bank or either immutable
  SIM cohort. It reads only the verified archived OANDA M1 bid/ask sources and
  cannot access a broker, account, signal feed, lifecycle, authorization, or
  promotion surface.
- Froze the metadata-selected 28 August 12:00–16:00 UTC Friday overlap session:
  48 global five-minute decisions and 192 four-pair causal contexts with stable
  aliases, exact 60-minute input slices, one-minute delayed fills, explicit
  source availability, current portfolio state, and future-free situation IDs.
- Implemented a real one-position state machine with one primary `wait`,
  `enter`, `hold`, `exit`, or `rotate` action per clock. Rotation atomically
  closes then opens and pays two execution legs. Entries/rotations reject wide
  delayed quotes without state mutation; exits remain permitted. A predeclared
  administrative session close is available for residual positions.
- The frozen mechanics baseline produced 25 waits, six entries, eight holds,
  six exits and three rotations; 18 executable legs and 37 depth-one nested
  alternatives ended flat at **−13.25 pips**. The loss was retained rather than
  tuned away. All rows are permanently historical training/discovery and
  proof-ineligible; the four dependence components are not independent regimes.
- Added component feedback for direction, entry, management, exit, rotation,
  cost clearance, calibration and opportunity cost. Counterfactuals clone the
  exact predecision state and always remain `counts_as_rep=0`.
- The independent verifier imports neither producer nor core and rebuilt all
  clocks, slices, candidates, actions, fills, per-leg costs, position states,
  branches, feedback, hashes, roots and the terminal seal with zero failures.
  SQLite backup verification sees committed WAL rows but excludes uncommitted
  rows. Repeated unchanged runs reuse the same immutable seal.
- Focused sequential/SIM validation passed 39 tests with one platform symlink
  skip; genealogy plus sequential validation passed another 37 tests. SQLite
  integrity and foreign keys are clean. Practice 007 and every execution gate
  remained unchanged.

## 2026-08-29 17:02 America/New_York — Honest deliberate-practice hierarchy verified

- Added a separate research-only Sequential Deliberate Replay V1 sidecar; the
  verified counterfactual SIM V1 cohorts, database, and results were not altered.
- Corrected the repetition denominator: 49,230 virtual variants reduce to 5,700
  two-sided outcome parts, 2,850 side-independent physical-path projections,
  478 pair/chart clocks, and only 120 global portfolio-choice clocks. The 360
  within-clock currency components are structural groups, not independent market
  regimes; independent regime evidence remains unknown from one inspected week.
- Added causal situation fingerprints, blind case aliases, canonical global-clock,
  market-episode, physical-path, currency-resource, archetype, lineage, and future
  position-thesis contracts. Shared unsigned currency resources are dependent
  even when signs conflict. Counterfactual variants and repeat attempts can never
  increase genuine market-repetition counts.
- Added an append-only pre-outcome decision journal for exactly one primary
  `wait`, `enter`, `hold`, `exit`, or `rotate` action per session/case. Entries
  and rotations require precommitted confidence, expected move, horizon, entry
  condition, invalidation, and rationale. Rotation is explicitly two execution
  legs; reviewed or revealed cases cannot become proof.
- Corrected the mistake curriculum: the 60 retained severity-ranked source rows
  collapse to four pair clocks, three portfolio clocks, and six two-sided paths.
  They remain outcome diagnostics, not sixty independent learner mistakes.
- The standalone verifier imports neither producer nor core and independently
  reproduced the 120/478/360/2,850 hierarchy, mistake collapse, row identities,
  blind-field exclusion, content bindings, append-only guards, foreign keys, and
  SQLite integrity with zero failures. The new cohort is registered beneath the
  verified SIM parent in the project-wide research genealogy.
- No broker, account, signal-feed, lifecycle, promotion, authorization, or
  real-money path was added or changed; supported execution remains `no_trade`.

## 2026-08-29 15:38 America/New_York — Safe-core supervision continuity restored

- Found that the hidden supervisor had stopped without an orderly terminal log
  at 13:38 ET while its children continued orphaned. This was a supervision
  failure, not a worker or account failure.
- A first hidden singleton restart exposed the prior `SafeCoreOnly` launch
  contract by starting three intentionally excluded workers. Stopped only that
  transient supervisor and its newly created depth, order/position-book, and
  second-forecast-tracker process trees; no causal or outcome evidence was
  deleted.
- Restarted hidden with `SafeCoreOnly`. PID 25396 acquired the singleton,
  adopted 61 existing workers with zero starts on the verified heartbeat, and
  reports all three excluded workers stopped for `safe_core_only`.
- Practice 007 refreshed successfully at NAV 41.6042 with zero open trades and
  zero pending orders. The quote stream and policy-wired executor are streaming;
  no execution or authorization gate changed.

## 2026-08-29 15:28 America/New_York — Counterfactual SIM gym independently verified

- Added a research-only, append-only high-volume replay gym with exact completed
  M1 knowledge clocks, executable bid/ask entry and exit sides, matched original,
  flipped, deterministic-random and no-trade controls, purged time blocks,
  factor/episode effective N, cost/slippage stress and clustered mistake labels.
- The first real cohort failed independent replay: one GBP/CHF SMA clock carried
  a `2.22e-12`-pip floating-point residue. It remains immutable. An explicit
  `1e-9`-pip signal-zero tolerance created a new material cohort rather than
  changing the failed rows.
- Corrected cohort `counterfactual_sim_gym_v1.7482ea2d37e385e90150` contains
  49,230 virtual intents, 44,271 eligible fills, 2,735 signals, 478 clocks and
  5,700 unique outcome paths. The standalone verifier independently rebuilt the
  complete ledger with zero failures and SQLite integrity `ok`.
- Adversarial tests prove that omitted intents, rehashed outcome tampering,
  archive traversal, absolute/outside paths, oversized archives and link
  boundaries fail closed. SIM/isolation and genealogy validation passed 50
  tests with one native Windows symlink-permission skip covered by a deterministic
  link-boundary test; compilation passed.
- All 108 original-direction diagnostic cells were negative after cost and no
  fixed definition beat both controls in all three purged partitions. A
  noncausal best-side oracle still found a cost-clearing side on roughly 47–53%
  of H30 clocks, isolating selection—not absence of movement—as the next target.
- Registered both immutable cohorts in the project-wide research genealogy.
  Historical replay remains unable to confirm, promote, authorize or execute;
  Practice 007 and real-money boundaries were unchanged.

## 2026-08-29 12:26 America/New_York — Priority contracts adopted and vault retained

- Committed the governed consensus, rates/RBNZ, and pair-options source work as
  `04296778d03acf71645b995838d24341d91805f0`; the exact commit passed the
  credential audit with zero findings and was published to the source-only
  vault with archive SHA-256
  `9c066025b2f4dc70889597663e49c073601db12655d1425b7abff82c274aae78`.
- Reloaded only the research macro-surprise worker at a cycle boundary. Its new
  process is healthy, the strict archive-bound V2 import is active, and causal
  consensus remains zero. Practice 007 stayed flat with zero trades or orders.
- Applied the recoverable vault-retention plan: 12 superseded model checkpoint
  ZIPs (1,616,590,976 bytes) were hash-verified into local quarantine. Nothing
  was deleted; the current model checkpoint and manifest remain present. Total
  synchronized vault size fell from about 6.05 GiB to 4.549 GiB.

## 2026-08-29 11:55 America/New_York — Source-priority integration and verifier cleared

- Completed the one-time hash-aware edge rebuild across all 15,746,198
  canonical forecasts. Exact report hashes are published and the independent
  verifier is back to `match`, including the semantic/output-integrity check.
  There are still zero confirmed candidates, so authorization remains false.
- Integrated three separate fail-closed source upgrades: response-complete
  pre-release consensus capture, append-only event-time rates replay, and
  pair-oriented FX-options semantics. None has a newly connected provider or
  an execution path; each preserves missing information as unavailable.
- The priority order remains causal consensus and policy repricing first,
  direct pair/tenor options second for magnitude and skew, followed by the
  first untouched official event under the raw all-68 executable quote clock.
  Practice 007 and real-money boundaries were not changed.

## 2026-08-29 11:44 America/New_York — Pair-options source semantics frozen fail-closed

- Added a prospective point-in-time FX pair-options schema and validator for
  all 68 OANDA pairs. It records source, publication, first-seen, retrieval,
  effective and decision clocks; pair orientation; tenor/expiry; delta and
  volatility conventions; ATM IV, 25-delta risk reversal/butterfly, CME CVOL,
  up/down variance, skew, convexity and genuinely sourced event-implied move.
- The official CME reference maps seven listed CVOL underlyings to OANDA: three
  identity pairs and four inverse pairs. Inversion swaps up/down variance and
  flips directional skew/RR signs. Other eligible G10 crosses are explicitly
  `two_leg_proxy`, never direct surfaces; reconstructing their IV requires
  timestamp-matched implied correlation or covariance.
- CME's 30-day whole-surface skew is not relabeled as a 25-delta RR, and the
  square-root-of-time ATM proxy is not relabeled as an event-implied move.
  Missing values remain null rather than zero. IV level is magnitude/uncertainty,
  not self-certifying trade direction.
- This implementation is validator-only: it does not contain an immutable
  collector, append-only options ledger, collection worker, or collection-
  readiness claim. No permitted source is connected, public view-only tools
  are not scraped, both source adapters start disabled, the state contains zero
  observations, and all proof/promotion/authorization/execution flags remain
  false with `no_trade` the only supported decision. Focused source and queue
  reconciliation validation passed 18 tests.

## 2026-08-29 09:47 America/New_York — Rank-V5 no-trade baseline made explicit

- Opened a new prospective rank-V5 material contract whose fourth comparison
  arm is an explicit `no_trade` counterfactual rather than a report-only label.
  The preceding rank-V5 contract contained zero decisions and was not rewritten.
- Every future V6-bound frozen decision receives one foreign-keyed, append-only
  no-trade forecast. At its declared horizon it matures deterministically to
  zero gross pips, zero executable after-cost pips, and zero realized cost,
  without reading a quote or submitting an order.
- Database constraints and payloads fix `research_only=true`,
  `execution_eligible=false`, `can_place_orders=false`, `can_authorize=false`,
  `can_promote=false`, and `order_submitted=false`. Focused rank-V5 validation
  passed seven tests, including zero-value maturation and append-only enforcement.
- No Practice-007, lifecycle, authorization, signal-feed, broker, or real-money
  setting changed.
- At 09:51 ET a controlled hidden Rank-V5 worker-only reload adopted the new
  contract before its 10:00 ET prospective clock. The first live state lists
  all four comparison arms, contains zero new decisions, and remains unable to
  place or authorize an order.

## 2026-08-29 09:33 America/New_York — Local Git baseline and source-only vault published

- Created the first reviewed local/private Git commit at
  `b3943607c2f07d3e979d3ab35327aae0f70c0903` and annotated it with
  `source-baseline-20260829`. The repository has no remote and nothing was
  pushed.
- The exact proposed index contained 1,135 source/config/test/documentation
  files. Credential audit found zero bearer-credential findings. Forty-one
  files contain Practice account identifiers, explicitly retained as private
  local metadata; credential files, the account registry, and four generated
  host/package runtime-lock inventories are excluded.
- Independent review found and the implementation repaired a vault TOCTOU:
  current-record aliases are now read from the same resolved commit as the
  credential audit and archive, never from the mutable worktree. The publisher
  also rejects tracked symlinks and force-added runtime/private artifacts,
  scopes dummy-secret exemptions to each matched value, uses a single-writer
  lock, verifies immutable-manifest identity, and publishes the latest pointer
  last. Thirteen adversarial Git/vault tests passed.
- Published the first exact source archive to the vault: 1,135 files,
  5,686,898 bytes, CRC verified, SHA-256
  `10399fa40f9dc6af9060d03041dcdcef1694eb42b50d5a0e001e7b50b2c2c7e3`.
  No old checkpoint was deleted and no stale `D:` fallback was used.

## 2026-08-29 09:05 America/New_York — Proof lineage and event provenance repaired prospectively

- An independent transition replay found that the published proof heads had
  returned to the August 6 cohort IDs after August 10 successors already
  existed. The legacy worker subsequently recorded 328,000 forecasts under
  those superseded IDs, exactly 82,000 per proof family. No forecast or outcome
  was edited or deleted. The rows were quarantined as diagnostic-only, four
  explicit `lineage_repair_20260829_v1` heads were opened, and the repair
  command wrote zero forecasts. The registry now verifies its 16-transition
  append chain and refuses silent contract reactivation.
- The hidden supervisor tree was reloaded at a safe post-publication boundary.
  The proof predictor adopted the four August 29 cohort IDs before the next
  forecast bucket and emitted zero weekend forecasts. The edge worker is doing
  one intentional post-reload rebuild: future unchanged-input skips require
  both the semantic fingerprint and SHA-256/size equality of the JSON and
  Markdown outputs. The independent verifier now separately rebuilds proof and
  allocator transition heads rather than trusting their published JSON alone.
- The allocator now rejects an A-to-B-to-A cohort restart and opens a new
  material cohort when required. When there is no policy-eligible universe it
  publishes `paused_no_eligible_universe` without inserting empty decisions,
  candidates, or comparators, while still maturing old pending outcomes. Five
  pre-adoption empty decisions in the new cohort remain immutable diagnostics;
  no allocator cohort is confirmed and the decision remains `no_trade`.
- Official-event proof now begins at raw insertion, not semantic completion.
  Contract `official_release_raw_quote_capture_v1_append_boundary_all68_20260829`
  and cohort `official_release_raw_quote_capture_v1_20260829a`, activated at
  2026-08-29 12:20 UTC, attach an immutable, exact-68 executable quote sidecar
  in the same transaction as each new raw observation. Invalid or partial
  attempts remain visible without proof quotes; duplicates are never recaptured;
  and the mapper has no independent quote-read path. Live lane and mapper
  heartbeats expose the contract, cohort, activation, count, and universe hash.
- Direct verified policy communications now use prospective issuer-only
  currency binding. Foreign currency mentions remain semantic entities, not
  issuer observations, and speeches remain research-only/non-publishable.
  Friday's Warsh speech is frozen solely as a USD-versus-GBP/SGD/TRY leakage
  regression. This implementation did not change Practice-007 routing,
  authorization, thresholds, or historical event classifications.
- Practice 007 remained current and flat at balance/NAV 41.6042, cumulative
  P/L -8.3430, zero positions, and zero pending orders throughout the reload.
  The market was closed and no manual or discretionary order was submitted.

## 2026-08-28 20:30 America/New_York — Post-recovery checks converged cleanly

- The independent verifier republished `match` with zero failed checks and no
  warnings. The temporary stale edge-report warning cleared after the next
  publication cadence. Edge evidence is now idle on unchanged inputs with
  52,640 governed cells, zero passing candidates, and no new economic labels.
- Practice 007 remains freshly current and unchanged at balance/NAV 41.6042,
  cumulative P/L -8.3430, zero open trades, and zero pending orders. Canary
  authorization remains false because there are zero confirmed candidates.
  The re-entry heartbeat continues to show a cleared broker status, transaction
  2461, zero errors, and read-only operation.
- The earlier 100% one-shot CPU sample was not persistent: a later bounded
  sample measured about 46%, with load attributable to expected research
  workers. Memory and disk remain safe. The signal-news WAL retained its stable
  preallocated physical size while the active generation remained only about
  3.4 MiB and below its checkpoint threshold.

## 2026-08-28 20:06 America/New_York — Broker recovery verified; stale recovery diagnostic cleared

- OANDA practice account reads recovered at approximately 19:54 ET. A fresh
  broker snapshot verified Practice 007 unchanged and flat: balance/NAV
  41.6042, cumulative P/L -8.3430, unrealized P/L 0, transaction 2461, zero
  open trades, and zero pending orders. All account, position, and order
  currentness flags returned to true. Governed accounting appended one valid
  current observation; the 41 prior maintenance failures remain quarantined
  and immutable.
- Allocator state returned from `account_state_unavailable` to current account
  context but correctly remained explicit `no_trade`: zero candidates, zero
  confirmed hypotheses, and no canary authorization. The executor recorded no
  fill or order attempt. Both quote transports reconnected and agreed on all
  68 instruments; prices remain non-routeable only because the weekend market
  is closed.
- The read-only re-entry worker recovered from 59 consecutive maintenance
  errors to `observing`, transaction 2461, zero consecutive errors, and no
  duplicate accounting. Its merged heartbeat retained a stale HTTP 503 field
  after a successful read, so the success path now explicitly clears
  `broker_http_status`. Three focused tests and compilation passed. A controlled
  reload of only that worker was adopted by the existing supervisor, and its
  live heartbeat now reports `broker_http_status=null`, `fail_closed=false`,
  and `can_place_orders=false`.
- The independent verifier remained `match` with zero failed checks, project
  integrity remained `ok`, and cycle 3 again produced zero passing cells among
  52,640 governed cells. A temporary stale-report warning remains a publication
  cadence item rather than an authorization failure.
- Resource diagnosis found no storage or runaway-process fault. CPU was
  elevated but variable (about 70-86%) across samples and attributable to the
  expected research workers; memory retained roughly 12.4 GiB free. The
  apparently 362.7 MB signal-news WAL is a stable preallocated file with only
  about 2.1 MB in the current WAL generation, not a growing backlog. C: retained
  more than 183 GiB free.

## 2026-08-28 19:25 America/New_York — Account-unavailable truth propagated end to end

- OANDA practice maintenance continued to return HTTP 503. The repaired source
  snapshot correctly said current NAV, balance, P/L, positions, and orders were
  unavailable. A downstream audit then found that several consumers were still
  converting those unknowns to numeric zero and, in some cases, inferring a
  flat account. This did not authorize an order, but it corrupted dashboard,
  accounting, allocator-counterfactual, and news-monitor state.
- Propagated explicit account currentness through the live dashboard/API,
  signal-news monitor, governed accounting, allocator proof, evidence worker,
  project-integrity audit, runtime audit, bounded improvement report, and local
  sentiment advisor. The dashboard and signal monitor now render unknown/null
  with the 503 reason; stale trades cannot be treated as current positions.
- Preserved the immutable accounting ledger and added an append-only validity
  sidecar. All 3,532 historical account snapshots were classified: 3,491 valid
  and 41 broker-failure snapshots quarantined. No row was deleted or rewritten.
  Current unavailable reads no longer create financial snapshots; the last
  valid context (17:59:56 ET, NAV/balance 41.6042, cumulative P/L -8.3430,
  transaction 2461, flat) is exposed only as stale and never used for governed
  inference.
- The previously stored daily routeability pass is now overlaid fail-closed
  while broker state is unavailable. Every allocator arm except the fixed
  no-trade baseline returns `account_state_unavailable`; account position count
  is null, canary authorization is false, and real-money routing remains false.
  Because this materially changed the allocator contract, a new immutable
  discovery cohort was opened instead of rewriting the old cohort.
- Reloaded only the dashboard, signal-news monitor, and evidence-operations
  workers. Live outputs now show null account values, explicit unavailable
  state, one integrity event, a failed-current routeability sentinel, and an
  explicit no-trade allocator decision. The combined focused validation passed
  156 tests and all modified modules compiled. The inactive post-upgrade audit
  was also corrected so its next run prints unknown rather than zero during a
  broker outage.
- Evidence cycle 2 proved the in-process immutable 15,746,198-row forecast
  audit cache hit and again produced 52,640 cells with zero passing candidates.
  A persistent restart-safe membership cache remains a separate requirement.
  The market stayed closed; official collection completed 42/42 sources with
  zero errors or inserts, V6/rank V5 added no proof decision, and no post-close
  article became directionally publishable.

## 2026-08-28 18:40 America/New_York — Maintenance observability made fail-closed; re-entry churn stopped

- OANDA's practice account endpoint entered scheduled-maintenance behavior and
  returned HTTP 503. The last successful broker read at 18:01 ET showed
  Practice 007 flat at balance/NAV 41.6042, cumulative P/L -8.3430, and no
  orders. No later balance, P/L, position, or order state is currently
  verifiable; a failed read is not evidence that any of those values changed.
- The account snapshot writer had represented an all-failed read as numeric
  zeros. It now publishes `null` current NAV/balance/P/L and position/order
  counts, explicit current/unavailable flags and broker errors, and may retain
  only prior verified financial values under clearly stale metadata. It never
  carries positions or orders forward as current. Five focused tests passed;
  the recycled singleton is live and reports `snapshot_state=unavailable`
  rather than a false zero account during the ongoing 503.
- The read-only re-entry counterfactual worker was also exiting on every 503,
  causing a roughly 49-second supervisor restart loop. Retryable 429/5xx
  broker reads now keep that research worker alive in a visible
  `broker_read_degraded`, fail-closed phase with bounded polling. Client and
  authentication failures still terminate loudly. Two focused tests passed;
  after a controlled recycle the worker remains a singleton and no longer
  churns.
- The recovered edge cycle published cleanly at the 15,746,198-forecast
  highwater: 1,984,258 usable labels, 52,640 cells, zero malformed/duplicate
  payloads, zero passing cells, and allocator `no_trade`. After the normal
  publication catch-up, the independent verifier matched lifecycle and
  genealogy at all 52,640 hypotheses with zero confirmed and authorization
  disabled.
- Friday's frozen 1,560-move history remained unchanged. Two post-close media
  items were context only: a USD-positive Fed recap arrived about 49 minutes
  late and an AUD-positive RBA/inflation article arrived about five minutes
  after publication. Neither can revise Friday's causal attribution.

## 2026-08-28 18:11 America/New_York — Recovery held; restart cache limitation isolated

- The detached safe-core supervisor remained continuously alive after recovery;
  its managed set was fresh with no crash/restart loop, unexpected worker loss,
  new lock, or nonempty supervisor error log. Practice 007 was freshly verified
  flat at balance/NAV 41.6042, cumulative P/L -8.3430, with zero positions,
  pending orders, candidates, or fills. The verifier remained matched at
  52,632 lifecycle/genealogy cells, zero confirmed, and authorization-safe.
- The recovered edge worker necessarily repeated the exact forecast audit. Its
  payload-membership SQLite cache is intentionally anonymous and process-local,
  so the supervisor-tree loss discarded it. The new frozen highwater grew from
  15,652,740 to 15,746,198 rows (+93,458); the worker completed the new full
  audit with zero malformed or duplicate payloads and entered cell construction.
  This was not corruption or cache rejection.
- A safe persistent replacement was scoped but deliberately not rushed into the
  active scan. It requires one SQLite transaction to bind exact payload
  membership and metadata, plus fail-closed validation of source identity,
  canonical schema and immutability triggers, audited highwater, exact distinct
  count, integrity checks, and deterministic row anchors. A restart should then
  reuse or extend the cache; any mismatch must force a full rebuild.
- The repaired quote crosscheck remained healthy and `weekend_closed` with zero
  hard reasons. The executor's compatibility JSON mirror recorded one transient
  Windows atomic-replace permission error after restart; its SQLite transport
  remained enabled and current, the writer thread stayed alive, and closed-
  market execution remained disabled. Recheck mirror recovery at Sunday reopen.
- The live mover view correctly reset to zero after the close while preserving
  all 1,560 retained cases. No official observation, error, revision, V6 proof
  event, or rank-V5 decision appeared. Three post-close media stories about CAD
  trade risk, Hormuz risk, and Warsh/rates are next-session context only and
  cannot revise Friday's causal record.

## 2026-08-28 17:43 America/New_York — Core supervisor tree recovered after post-close loss

- The always-on supervisor and its core child tree stopped abruptly after a
  normal 17:09:58 ET heartbeat. There was no orderly shutdown record,
  supervisor error, Windows Application error, new lock, or traceback. Several
  older ancillary collectors survived, but the account writer, Practice-007
  executor, quote workers, V6/rank writers, edge worker, genealogy, and
  verifier stopped publishing for roughly 26 minutes. The event occurred after
  OANDA's Friday close, while Practice 007 was flat, so no executable market or
  position-management interval was lost.
- Relaunched the singleton supervisor as a detached hidden Windows process in
  the existing safe-core configuration. It recovered all managed workers in
  bounded startup-stagger waves. The account was freshly reverified at
  balance/NAV 41.6042, cumulative P/L -8.3430, with zero trades or orders;
  V6 and rank V5 remained at zero proof events/decisions and `no_trade`.
- The pre-recovery verifier had failed closed on a 19-cell publication-order
  mismatch (52,632 lifecycle versus 52,613 genealogy). After recovery it
  independently rebuilt both at 52,632, missing zero, confirmed zero, returned
  `match`, and restored `authorization_safe=true`. No authorization or order
  existed during the mismatch.
- Recovery exposed a second harmless closed-market crosscheck false alarm:
  the freshly restarted executor had zero new quotes while the independent
  stream retained 68 final quotes, so shared-instrument and unavailable-price
  checks still reported degradation despite `weekend_closed`. Those two cases
  are now informational only during the regular closure; open-market behavior
  remains unchanged. The focused suite now passes eight tests, compilation
  passes, and the recycled worker reports healthy with no failure reasons.
- The edge worker resumed its interrupted second cycle from the persisted
  ledgers and is rebuilding current outcome rows. Its first exact publication
  remains intact; proof of the forecast-audit cache hit/append-only extension
  remains pending. The verifier's temporary stale-edge-report warning is
  expected until that cycle publishes.
- The final post-close snapshot added no new official source or executable
  move. The late Invezz duplicate arrived roughly 24m45s after publication and
  added no causal evidence. Final factor-episode integrity remained clean.

## 2026-08-28 17:08 America/New_York — Friday-close DST diagnostic repaired; first edge cycle published

- The edge-evidence replacement completed and published its first exact cycle:
  all 15,652,740 frozen forecasts were audited with zero malformed or duplicate
  payloads, producing 52,632 cells, 812 family/horizon groups, and 121
  archetype/horizon groups. No cell passed, no discovery or confirmation
  candidate exists, and the independent verifier remains matched and
  authorization-safe. Its prior stale-edge-report warning cleared. The next
  cycle must still prove the persisted cache-hit or append-only-extension path.
- OANDA stopped price updates at the normal Friday 17:00 New York close. Both
  independent transports retained the same 68/68 final-generation snapshot,
  and the executor independently reduced usable prices and candidates to zero.
  The read-only transport crosscheck nevertheless reported a false degradation
  until 18:00 ET because its weekend boundary was hard-coded to 22:00 UTC.
- Replaced that fixed UTC boundary with the timezone-aware America/New_York
  Friday 17:00-to-Sunday 17:00 closure. Seven focused tests and Python
  compilation passed, including EDT and EST boundaries. A controlled recycle
  of only the crosscheck worker produced a healthy `weekend_closed` state with
  stale snapshots listed as informational, while execution freshness gates
  remained unchanged.
- The final pre-close factor surface fragmented into HKD weakness, CHF strength,
  and USD weakness. Largest executable legs were CHF/HKD +66.25 gross/+45.4
  net, HKD/JPY -51.3/+24.8 net short, CHF/JPY +11.85/+5.2, USD/CHF
  -5.55/+3.8 net short, and USD/JPY -3.8/+2.0 net short. Technical continuation
  aligned across the ten frozen top legs; strict source direction remained
  0/10 and the narrative meter neutral. Late PLN/NOK reversals were not
  executable after end-of-week spread expansion.
- Practice 007 remained flat at balance/NAV 41.6042, cumulative P/L -8.3430,
  with zero trades/orders. V6 remained at zero prospective proof events and
  rank V5 at zero decisions. No new official release arrived; the fast lane
  completed 42/42 sources with zero errors.

## 2026-08-28 16:35 America/New_York — CAD reversal and PLN weakness remained source-unconfirmed

- The earlier CAD-weakness cluster fully reversed after roughly 16:10 ET.
  Cost-clearing CAD-strength expressions included CAD/HKD +19.85 gross/+11.0
  executable-net pips, GBP/CAD -9.5/+4.6 net, AUD/CAD -6.0/+3.5 net,
  EUR/CAD -6.6/+3.0 net, and USD/CAD -4.8/+3.0 net. PLN weakened at the same
  time: USD/PLN and EUR/PLN rose roughly 41-45 gross pips. The frozen
  onset-time technical continuation arm agreed with all ten top mover legs.
- No strict directional source or official release explained either factor
  episode. The official fast lane completed all 42 configured sources with
  zero errors and zero new observations. An Invezz Warsh/inflation/oil story
  was observed about 16 seconds after publication and mapped research-only to
  CAD-/NOK-/JPY+. It aligned with the fading NOK-weakness leg and later JPY
  strength but directly opposed the CAD reversal, so it remained an
  unverified context item rather than a causal signal. A later BoE-hike story
  arrived after GBP/CAD had already begun and also opposed that move.
- V6 remained at 77/77 low-support abstentions and zero prospective proof
  events; rank V5 remained at zero decisions. Practice 007 stayed flat at
  balance/NAV 41.6042, cumulative P/L -8.3430, with no trades or orders.
- The replacement edge-evidence worker completed the exact inventory of all
  15,652,740 frozen forecast rows with zero malformed rows, duplicate payloads,
  or errors and entered cell construction. Its first publication and a later
  cache-hit/append-only-extension cycle remain pending. Both quote transports
  stayed healthy at 68/68 current-generation rows. CPU remained saturated
  during evidence construction, while disk queue and memory pressure improved;
  storage remained safe at 184.97 GiB free.

## 2026-08-28 16:05 America/New_York — CAD weakness was technically visible but source-unconfirmed

- A new cross-pair CAD-weakness episode began after the preceding checkpoint:
  CAD/HKD fell from 15:47 ET for roughly +17.3 executable-net pips by the
  latest extension, USD/CAD rose from 15:47 for about +4.4 net, and CAD/SGD
  fell from 15:50 for +1.3 net. The onset-time technical continuation arm had
  the correct side on all three legs.
- No strict directional source or official release existed for the episode.
  The healthy official fast lane covered 42/42 configured sources with zero
  new observations. A secondary headline about higher US rate odds after
  Warsh was published at 15:40:36 and first seen at 15:45:16.797 ET, 1 minute
  44 seconds before the USD/CAD onset, but remained unverified/context-only.
  It cannot explain broad CAD weakness because USD was simultaneously weak
  against CZK, PLN, and GBP. The case is retained as a source/watchlist
  abstention with technically aligned CAD movement, not an official-event
  wrong-way miss.
- Other cost-clearing legs still active in the snapshot included USD/NOK
  +40.4 net, EUR/NOK +61.7, USD/PLN +32.0, GBP/USD +10.2, and CHF/JPY +9.8;
  none had a strict-forward independent story. V6 remained at 77/77
  low-support abstentions and zero prospective proof events; rank V5 remained
  at zero decisions.
- Practice 007 remained flat at balance/NAV 41.6042, cumulative P/L -8.3430,
  with zero trades/orders and zero confirmed candidates. Both quote transports
  remained healthy at 68/68 current-generation rows; stale quiet/Friday-closed
  rows were independently excluded from execution.
- The replacement edge-evidence worker reached 4,725,000 of 15,614,926
  forecast rows (30.26%) in its one-time exact audit-cache bootstrap with zero
  errors. CPU remains saturated during this bootstrap; disk averaged 44.3%
  with queue 1.6. Free C: space remains safe at 185.87 GiB; the roughly
  1.28-GiB decrease is consistent with the growing temporary exact payload
  index and does not alter retained evidence.

## 2026-08-28 15:43 America/New_York — JPY timing miss retained; three more recurrent scans removed

- Practice 007 remained flat at balance/NAV **41.6042**, cumulative P/L
  **-8.3430**, last transaction 2461, with zero positions or pending orders.
  Both independent quote transports remained connected and agreed on 68/68
  current-generation rows. EUR/TRY, TRY/JPY, and USD/TRY stopped receiving
  broker ticks near the normal Friday TRY close; the executor's independent
  30-second usable-quote and final 15-second submission gates exclude them.
- A coherent JPY-weakness episode appeared around 15:08-15:14 ET: HKD/JPY
  moved +36.45 gross/+15.0 executable-net pips, GBP/JPY +5.3/+2.2,
  USD/JPY +2.4/+0.9, and EUR/JPY about +5.3/+2.9. The fixed-cutoff factor
  solver confirmed JPY weakness, but there was no new BOJ/official release,
  strict-forward story, or causal narrative signal. A secondary headline first
  seen at 15:07:25 ET said that the yen had *already* weakened past 160; V151
  correctly classified it as a retrospective price recap and barred it from
  causal proof. V6 remained at zero prospective events and rank V5 at zero
  decisions, so the episode is retained as a technical-timing/source-absence
  miss rather than assigned a hindsight news cause.
- The timing audit found mixed pre-entry evidence. USD/JPY's watchlist side was
  short and wrong; EUR/JPY and AUD/JPY were long but economically marginal.
  Earlier USD/JPY buy shadows suffered -3.4 to -4.2 pips at M1-M5 before only
  +0.3 pip net at M10. Level-band observations did identify approaches to
  resistance, but the frozen bounce-versus-break direction model was absent;
  entering those observations at 15:05 still did not capture the narrow
  15:08-15:14 profitable window.
- The direct-source diagnostic's hourly canonical census no longer reparses
  the 8.4 GB edge database. It performed one exact bootstrap over 1,971,981
  labels/1,097,799 forecasts/15 horizons, persisted an immutable-trigger,
  file-identity, rowid-high-water, and row-anchor contract, then reused the
  exact cache on the next live cycle. A fresh-process production verification
  of the unchanged high-water completed in 0.0166 seconds. New labels extend
  only `(old_highwater,new_highwater]`; changed identity, missing immutable
  guards, or a mismatched anchor forces a complete rebuild.
- The BLS macro-release breakout loader had still filtered the 890 MB article
  table on unindexed `source_kind` every 30 seconds to return four rows. It now
  derives the frozen/configured BLS source lineages and uses the indexed
  `source_id` path while retaining every payload-level semantic and causal
  check. Copied-production output stayed at four identical events while the
  read fell from 1.156 seconds to 0.0086 seconds. The worker-only replacement
  is live and its stderr is empty.
- The evidence worker also performed two forecast-wide passes on every
  outcome-only rebuild, including a repeated JSON/duplicate/microstructure
  audit of 15,614,926 immutable forecasts. The redundant count pass is removed
  and a worker-lifetime disk-backed exact audit cache now reuses identical
  high-waters or scans only appended rowids. Cache reuse requires matching
  database identity/schema, immutable update/delete guards, and monotonic
  high-water; savepoints prevent failed incremental extensions from entering
  evidence. A 100,000+1,000-row fixture improved from 1.246 seconds full to
  0.014 seconds incremental (88.9x) with exact output equality. The old cycle
  was recycled at 21% of its redundant scan; the replacement is now performing
  its one required full bootstrap.
- The independent verifier briefly reported 86 lifecycle hypotheses absent
  from genealogy. Exact row review showed a publication-order race: lifecycle
  had committed the new cells before the slower genealogy publication. The
  verifier correctly set authorization unsafe during the mismatch, then
  self-cleared to 52,613/52,613 with zero confirmed candidates. This was not
  evidence corruption and did not open an execution path.
- Local validation passed 54 direct-source tests, 25 edge-evidence/worker
  tests, four macro-release tests (with the already unavailable historical PPI
  fixture deselected), and 22 verifier/quote-safety tests. No evidence row,
  execution threshold, lifecycle state, authorization, broker order, or
  real-money setting was changed. `no_trade` remains the supported decision.

## 2026-08-28 15:08 America/New_York — Exact-cache rollout removes four recurrent full-scan paths

- Follow-up live byte counters showed that the first candle-tail and ledger
  fixes had removed one class of read amplification but not every invalidation
  path. The official mapper, V6 input adapter, position-ledger reporter, and
  mature-only executable-opportunity drain were therefore profiled at the SQL
  and process-transfer level rather than judged only from cold tests.
- The V6 adapter had still reopened and rescanned its 118.8 MB official mapping
  database every five seconds. It now caches an exact, integrity-checked input
  snapshot by main-database and nonempty-WAL content fingerprint; transient
  empty WAL/SHM timestamps cannot invalidate it, real commits invalidate
  immediately, and a database change during a read is deliberately not cached.
  The production source benchmark improved from 0.556 seconds cold to 0.0017
  seconds unchanged (about 330x) with identical observations.
- The official fast mapper had the same SQLite-sidecar false invalidation plus
  repeated integrity/count/candidate scans. It now uses stable content
  fingerprints, a frozen input high-water, caught-up output reuse, and strict
  partial-limit protection. A copied-production benchmark fell from about
  379 MB per unchanged cycle to 172 KB (99.955% fewer read bytes) while
  preserving exact V151 output and first-seen clocks.
- The top-signal ledger now maintains separate trigger-driven metadata and
  mature-evidence revisions. New research-position opens refresh counts but no
  longer rebuild seven immutable mature aggregates; open MFE/MAE and the latest
  80 rows remain live and uncached. Production-shaped reads fell from about
  829 MB cold to 9.26 MB on a new open and 2.26 MB unchanged. A genuine maturity
  still invalidates exact mature summaries, as required; incremental mature
  aggregation remains a later optimization if this periodic cost is material.
- The retired executable-opportunity producer remained active only to mature
  3,213 unresolved forecasts, yet scanned a 511.7 MB ledger and a 444,214-row
  quote clock every 30 seconds. It now uses a transactionally bootstrapped,
  trigger-maintained pending-outcome queue, an invalidation-safe exact summary
  cache, and indexed quote high-water/tail reads. Live queue and anti-join both
  equal exactly 3,213, SQLite integrity is `ok`, status remains
  `draining_pending_outcomes`, and a steady live interval read about 36.5 MB
  over 32.9 seconds rather than hundreds of MB per second.
- All four workers were recycled individually under the existing hidden
  supervisor; replacement heartbeats/states are fresh and stderr logs are
  empty. Final local verification passed 61 focused mapper/V6/ledger/supervisor
  tests plus 27 opportunity tests. No forecast/outcome evidence was rewritten
  and no execution, promotion, authorization, threshold, or real-money setting
  changed.
- Aggregate CPU improved from repeated 100% samples to an 84.9% mean in the
  latest five-sample window. Disk time/queue remain episodic (77.6%/1.55 in the
  same window) because the direct-source diagnostic and evidence-operations
  jobs still perform scheduled broad reads; those are now the leading bounded
  operational backlog rather than a quote/executor failure.
- Practice 007 remained flat at balance/NAV **41.6042**, cumulative P/L
  **-8.3430**, last transaction 2461, with zero positions, pending orders,
  qualified candidates, or fills. The late ZAR weakening episode still has no
  strict timely source and remains unexplained. `no_trade` is unchanged.
- Before closeout the ZAR leg reversed sharply, reinforcing the whipsaw rather
  than validating the earlier continuation. By 15:08 ET the live move detector
  showed USD/ZAR about -192.5 gross/+126.0 executable net pips over 25 minutes,
  CHF/ZAR -371.0/+257.5 over 43 minutes, EUR/ZAR -305.3/+213.0, and GBP/ZAR
  -299.3/+179.2. A broad article first seen at 14:40 ET describing a 9.5 bp
  two-year Treasury-yield rise after Warsh was already post-move, not an
  official release, and did not provide a causal explanation for broad ZAR
  strengthening. Both directions of the ZAR round trip remain retained as an
  unexplained late-session episode.

## 2026-08-28 14:17 America/New_York — Late ZAR rotation retained; two research I/O hot paths removed

- A new broad ZAR weakening leg developed after the 13:49 checkpoint. The
  move-first snapshot measured GBP/ZAR about +214.9 gross/+103.3 net pips,
  CHF/ZAR +207.1/+95.5, EUR/ZAR +197.4/+94.1, and USD/ZAR
  +236.7/+162.8. Concurrent SEK strength and GBP/AUD weakness made the wider
  tape a fragmented currency rotation rather than one clean common factor.
- The strict forward story count was zero for all ten sampled leaders. The
  official fast lane added no observation; the two new broad-news items were
  irrelevant or late/contextual. The ZAR leg remains an unexplained
  positioning/liquidity episode rather than receiving post-hoc attribution.
  V6 still has zero prospective proof events/non-abstaining forecasts, rank V5
  has zero decisions/outcomes, and the supported result remains `no_trade`.
- A resource audit found sustained 100% aggregate CPU and about 252% physical
  disk time with a queue near 5.0. The largest repeated read was caused by the
  V6 response mapper starting at byte zero across all 68 append-only M1 CSVs
  (about 630 MB total) whenever a maturity was due. The shared loader now walks
  complete CSV records backward from EOF, stops at the exact inclusive window,
  and restores chronological order. Old replay windows remain exact; no
  sampling, carrying, or evidence-contract change was introduced.
- The top-signal position ledger also repeated seven mature-history scans and
  a full-table recent-position sort every five seconds over 58,290 rows. It now
  caches only the immutable mature summaries, invalidates on local maturity or
  policy changes and external SQLite commits via `PRAGMA data_version`, and
  uses bounded covering/order indexes. Open-position path measurements and
  current counts still refresh every cycle. A backup benchmark improved an
  unchanged publish from 3.6101 seconds to 0.1065 seconds (33.9x).
- Both research workers were recycled individually under the existing hidden
  supervisor. Their replacement stderr logs are empty, state files are fresh,
  and both SQLite ledgers passed `quick_check`. Combined focused validation
  passed 40 tests. Post-activation sampling reduced physical-disk time to a
  20.0% mean with queue 0.40 over five samples; CPU remained near saturation,
  so additional output-equivalent profiling remains open.
- The post-change OneDrive vault sync completed with 1,374 model/project files
  at content hash `df98cc7a06eb1ca3cabae953cedf1344382ecc1b13b92be8f29cee46ea7040d0`
  and 90 news/event files at content hash
  `dc23f95b59d544b5b66ac829ab90531bb029da3220247beddae8cbd9a4cbe3cb`.
- Practice 007 remained flat at balance/NAV **41.6042**, cumulative P/L
  **-8.3430**, last transaction 2461, with zero positions, orders, candidates,
  or fills. Both 68-pair quote transports, broker/Windows clocks, verifier, and
  project-integrity gates remained healthy. No execution, threshold,
  authorization, promotion, or real-money setting changed.

## 2026-08-28 13:49 America/New_York — Governance false alarm cleared; live quote-aging repair verified

- The market fragmented from the preceding broad whipsaw into residual
  currency rotations rather than one coherent USD factor. Representative
  cost-clearing legs included USD/HUF about +41.9 gross/+20.0 net pips,
  EUR/HUF +37.1/+14.0, USD/MXN -101.5/+56.0, USD/CZK +133.5/+67.0,
  USD/SEK +51.8/+21.6, NZD/HKD +30.5/+16.9, and CHF/HKD
  -42.7/+24.1. USD was simultaneously strong against HUF/CZK/SEK/CHF and
  weak against MXN/NOK, so this remains positioning/liquidity rotation rather
  than a clean new macro impulse.
- Four new discovery articles were retrospective or contextual, not timely
  causal releases. The official fast lane added no release, semantic
  candidate, or response watch aligned to these rotations. V6 crossed its
  13:30 activation cleanly with only one retained pre-activation diagnostic,
  zero prospective proof events, and zero non-abstaining forecasts. Rank V5
  remains source-ready with zero decisions/outcomes and `no_trade`.
- The newest 781 correlated maturities were again negative after cost. H5
  averaged -3.394 pips with a 13.2% win rate; its liquid subset averaged
  -1.680 pips with 17.3% wins. The liquid H1 subset averaged -2.053 pips with
  27.3% wins across only two shared entry-minute episodes. Ridge was least
  negative at -1.105 pips; tabular, graph, and state-space were also negative.
  This reverses the earlier single-episode positive batch and supplies no
  independent promotion evidence.
- A verifier mismatch was traced to a diagnostic clock error, not evidence
  disagreement: the 31.9-minute pass reused lifecycle/allocator timestamps
  read at pass start for its end-of-pass freshness gate even though both live
  files refreshed during the scan. The verifier now retains pass-start
  payloads for substantive reconstruction but rereads the three publication
  clocks at the actual freshness gate. It completed `match`; the separately
  restarted project-integrity audit returned `ok`. All authorization paths
  stayed fail-closed throughout. Validation passed 51 focused tests.
- The strategy producer's live process had no price stream, making its old
  five-second in-cycle stream refresh a no-op. A representative pre-fix cycle
  took 72.7 seconds and accumulated 6,146 `stale_quote` rejections. The
  research producer now overlays a read-only all-68 OANDA pricing snapshot at
  lane start and every ten seconds, selects only the newest broker timestamp,
  preserves source and tradeability, and retains the unchanged 30-second
  rejection gate. It cannot authorize or submit an order.
- After a strategy-lab-only hidden reload, the first complete 209-lane cycle
  made six successful all-68 snapshot reads and reduced stale-quote
  rejections to 857, an **86.1% reduction**. The 627 venue-nontradeable
  rejections remained intact; three to five quiet instruments still aged out
  naturally. Quote-source provenance is now retained on forecasts/outcomes.
  Validation passed 116 tests plus 220 subtests and Python compilation.
- Practice 007 remained flat at balance/NAV **41.6042**, cumulative P/L
  **-8.3430**, last transaction 2461, with zero positions, orders, or fills.
  Both quote transports remained healthy across all 68 pairs, real-money
  routing remained disabled, and the supported decision stayed `no_trade`.

## 2026-08-28 13:22 America/New_York — Second whipsaw audited; V151/V6 boundary loaded

- The 12:50-13:03 ET market leg reversed much of the immediately preceding
  USD selloff. Executable leaders included USD/ZAR about +173.9 gross/+102.0
  net pips, EUR/ZAR +174.8/+81.6, USD/SEK +66.5/+34.0, GBP/HKD
  -74.0/+57.6, GBP/JPY -15.4/+12.2, GBP/USD -9.4/+7.6, and USD/CHF
  +4.3/+2.7. Factor decomposition showed concurrent ZAR weakness, USD
  recovery, GBP weakness, and JPY/HKD strength rather than one clean factor.
- The move-first audit again found zero during-move events and zero strict
  forward-aligned stories across the ten sampled leaders. Fresh Warsh and
  Iran items were repetitions or context rather than newly observed causal
  releases. This episode remains unexplained/positioning-led evidence rather
  than receiving post-hoc news attribution.
- A Finnhub headline first seen at 13:02:58 ET, `USDJPY jumps above its 100 day
  MA and makes a break for it`, exposed a second retrospective-pair pattern.
  It had already been barred from directional publication, but V150 retained
  current-report metadata and assigned both currency legs bullishly. V151 now
  preserves the pair direction as USD positive/JPY negative, marks the story
  as a prior-market-move retrospective, disables forward timeliness, and
  keeps it ineligible for directional publication.
- V5/V150 evidence remains immutable. A new V6 source-response cohort activates
  at **13:30 ET**, requires exact V151 input, imports no V5 rows, and begins at
  zero proof events. Rank V5 consumes only V6, has no legacy fallback/import,
  reports SQLite integrity `ok`, and currently supports `no_trade` with zero
  decisions or outcomes. The source tree and supervisor were reloaded hidden;
  old V5/rank-V4 workers are absent while quotes and the executor remained
  live.
- Two Windows publication transients were hardened without changing research
  or execution semantics. The rank adapter now uses collision-proof temporary
  files plus bounded exponential replacement retry. Supervisor reads of
  version-gated heartbeat JSON now retry for a bounded 140 ms before failing
  closed. Persistent lock, parse, or contract failures still remain visible.
- The news WAL's temporary 189 MB warning self-checkpointed normally to the
  bounded range; no manual checkpoint was forced. CPU was elevated during
  overlapping strategy, evidence, archive, and verifier passes, but memory and
  storage remained safe and the lower-priority research jobs were already
  scheduled below the execution path.
- V151/V6/rank-V5 validation passed **551 focused tests** plus **24 reliability
  tests**, Python compilation, PowerShell parsing, and JSON validation. A local
  integrated rerun passed another 54 focused selections; only the existing
  pytest-cache permission warning remained.
- Practice 007 remained flat at balance/NAV **41.6042**, cumulative P/L
  **-8.3430**, with zero trades, orders, or fills. Both quote transports stayed
  healthy at 68/68 and real-money routing remained disabled.

## 2026-08-28 12:55 America/New_York — USD retracement audited; retrospective-news boundary repaired

- A broad USD reversal developed between roughly 12:22 and 12:42 ET. The
  largest executable legs included USD/SEK about -110.6 pips, USD/NOK about
  -111.3 pips, USD/MXN about -120 pips, USD/ZAR about -135.6 pips, and
  USD/HUF about -38.3 pips; EUR/SEK also fell about 95.8 pips, confirming that
  the move contained independent SEK strength rather than only a USD print.
  The live move-first audit found no during-move official release and no
  strict directional event across the ten sampled leaders. The supported
  attribution is therefore a broad risk/positioning retracement after the
  earlier 10:00 ET Warsh USD impulse, not a newly observed causal news event.
- Technical continuation candidates were early or wrong-way during the turn:
  AUD/USD H1 sell and USD/CAD H1 buy previews were conflict-blocked, while a
  later USD/JPY H1 buy remained unconfirmed and canary-blocked. The preceding
  GBP/USD sell preview matured at only about +0.2 gross/-1.5 pips after cost,
  so the independent authorization boundary was protective. No order or fill
  occurred.
- The outcome worker matured 1,513 correlated rows during the interval. H5
  results remained negative (N=1,437, mean -0.344 net pips, 47.18% wins).
  Seventy-six H1 proof-family rows averaged +3.669 net pips with 64.47% wins,
  but they are four models over 19 liquid pairs in the same Warsh-followthrough
  episode and do not constitute independent confirmation. Ridge, graph, and
  state-space were positive in that episode; tabular remained negative.
- A delayed Forexlive/Finnhub headline, "EURUSD moves to new lows and tests a
  key cluster of technical levels," exposed a classifier defect: the article
  was a retrospective market recap but retained current-report semantics.
  Classifier V150 now recognizes pair-led high/low recaps, preserves the two
  currency legs, marks prior-market-move/retrospective temporality, and blocks
  forward publication. It never entered the official fast path, V4 proof, or
  execution.
- Because V4 was immutably bound to V149, a separate V5 source-response cohort
  and rank-V4 adapter were created. V5 activates at 13:00 ET, imports no V4
  evidence, and rank V4 has no legacy fallback. A controlled hidden reload
  brought V150, V5, and rank V4 live while leaving quote transport and the
  Practice-007 executor running. Pre-activation V5 and rank-V4 evidence counts
  are zero, SQLite integrity is `ok`, and the supported decision is
  `no_trade`.
- The project-integrity audit now treats an in-progress independent-verifier
  rebuild as operationally safe only when every routing surface is closed, no
  confirmed candidate exists, all retained checks pass, and a recent completed
  match is available. This removes a transient false degradation without
  weakening authorization safety. The focused suite passed 36 tests.
- The long move-first audit was slow rather than deadlocked. Its source-event
  lookup no longer copies the full per-currency event arrays for every case;
  its exact-output regression passed 17 tests. Larger parse-once, indexed
  candle, progress-sidecar, and frozen-high-water optimizations remain queued
  for the next clean rebuild.
- Practice 007 remained flat at balance/NAV **41.6042**, cumulative P/L
  **-8.3430**, with zero open trades or pending orders. Both quote transports
  remained current for 68/68 instruments, the clock remained broker-aligned,
  and storage retained about **189.5 GiB free**.

## 2026-08-28 12:12 America/New_York — Clean V4 boundary verified; stale integrity assumptions repaired

- V4 activated at its frozen **12:00 ET** boundary without importing V3
  evidence. Its first retained post-boundary diagnostic event was ECB
  Schnabel's non-directional "Central banks on-chain" communication. V4
  correctly kept it outside proof: one canonical diagnostic event, zero
  prospective proof events, zero non-abstaining forecasts, and no rank-V3
  decision. Rank V3 remains research-only with the supported result
  `no_trade`.
- The largest continuing legs remained broad USD strength and NZD weakness
  following the 10:00 ET Fed Warsh event. No new timely official directional
  catalyst appeared around the noon legs. Secondary repetitions arrived too
  late to be causal, while the direct Fed observation remains the credible
  earlier context rather than a new noon signal.
- At 11:59:31 ET the executor saw one otherwise qualified AUD/USD H1 sell from
  `volatility_squeeze_breakout.balanced`: confidence 0.613356, 3.4 projected
  gross pips, 1.3-pip spread, and +1.5224 projected net pips. An opposing buy
  thesis made it direction-conflicted, so the conflict gate retained it as
  shadow-only and no order was sent. By 12:11 the sole qualified preview had
  rotated to a similarly conflicted EUR/USD H1 sell; governed authorization
  still reported zero confirmed candidates. At 12:14 a GBP/USD H1 sell became
  direction-clear (confidence 0.596129, 4.1 gross pips, 1.6-pip spread,
  +1.3460 projected net), but it was still correctly blocked by the separate
  governed canary boundary because the lifecycle contains zero confirmed
  candidates. This produced no order or fill.
- The project-integrity audit exposed two stale diagnostic assumptions rather
  than unsafe trading state. The retired executable-opportunity worker is
  intentionally draining 3,213 already-issued outcomes with future production,
  promotion, authorization, and execution disabled. Integrity now recognizes
  only that exact safe retired-drain lifecycle. The long-lived news/technical
  watchlist really was still publishing classifier V147; its state now carries
  an explicit top-level classifier contract, and both integrity and the
  supervisor require exact V149 identity.
- Focused validation passed **78 tests** in the repair branch and **75 tests**
  in the integrated rerun, plus Python compilation and PowerShell parsing. A
  controlled hidden supervisor reload replaced only the stale watchlist and
  integrity-audit trees; quote collection and the Practice-007 executor stayed
  live. The fresh watchlist now reports V149, `retired_drain_only`, and no
  execution path. Supervisor status is 96 managed workers, 69 running, zero
  unexpectedly absent, and zero unhealthy.
- Practice 007 remained flat at balance/NAV **41.6042**, cumulative P/L
  **-8.3430**, with zero trades, zero pending orders, and real-money routing
  disabled. The live price stream retained 68/68 instruments; three TRY pairs
  were last-known rather than currently usable at the sampled executor cycle,
  so entry selection continued to apply its independent freshness check.

## 2026-08-28 11:36 America/New_York — Response-watch V149 liveness contract repaired

- The post-cutover official-response watcher was internally healthy but its
  runtime heartbeat omitted `required_classification_version`. The supervisor
  therefore treated it as an old contract and restarted the wrapper/child tree
  roughly every four minutes. No response evidence, account state, quote data,
  or orders were altered by the churn.
- Added the exact V149 classifier identity to the watcher's loading, completed,
  and error heartbeat states and strengthened the supervisor regression test.
  The focused integration suite passed **21 tests** plus Python compilation.
- Reloaded only the exact response-watch process tree. The next supervisor
  cycle observed the V149 value, matched the expected contract, and reported
  the worker fresh with one logical wrapper/child tree. Practice 007 remained
  flat at balance/NAV 41.6042 with no trades or orders.

## 2026-08-28 11:06 America/New_York — Fed response captured; false policy proof removed; V4 live cutover

- Practice 007 remained flat at balance/NAV **41.6042**, with zero open
  trades/orders and cumulative P/L **-8.3430**. Quote transport is current for
  68/68 instruments and real-money routing remains disabled.
- The official broad feed captured Fed Chair Kevin Warsh's 10:00 ET Jackson
  Hole speech at **10:01:05 ET**. Its hawkish inflation/policy content aligned
  with the subsequent broad USD response: the strongest six-minute legs
  included about +63 bps USD/ZAR, +58 USD/HUF, +53 USD/SEK, +46 USD/NOK, and
  +37 USD/CHF, while NZD/USD, AUD/USD, and EUR/USD fell about 43, 31, and 29
  bps. The event was timely enough for research attribution, but it did not
  enter the official fast lane before that lane was expanded in this pass.
- Between the prior checkpoint and this cutover, 50 executor checkpoints had
  at least one qualified price candidate and 17 had non-conflicting capacity.
  The strongest observed candidates included AUD/USD and EUR/USD H6 shorts
  with projected after-spread moves of about +3.11 and +3.24 pips. All 107
  attempted selections were correctly stopped by the governed canary boundary
  because the lifecycle database still contains zero confirmed candidates;
  no order was submitted.
- The sole V3 source-proof event was invalid: a CBRT university paper-contest
  notice was context-only under classifier V148 but inherited a policy category
  from its institution. It produced no forecast, response, rank row, or trade.
  V3 is now sealed as a contaminated diagnostic. Classifier V149 explicitly
  excludes that administrative document class while preserving genuine CBRT
  policy decisions.
- Opened the separate immutable V4 source-response cohort, activated at
  **2026-08-28 12:00 ET**. V4 requires `relevant=true`, an empty exclusion
  reason, the exact V149 classifier, and the current mapper contract. The new
  rank-V3 adapter consumes V4 only and has no legacy fallback. Its initial
  state is ready with zero source rows and the supported decision `no_trade`.
- Expanded the official fast lane from scheduled numeric releases to the 13
  governed direct central-bank communication sources. Communications remain
  research-only and must satisfy direct-source/domain verification; scheduled
  preflight remains a separate, directionless clock. The new collector
  contract activated at 11:00 ET and all 42 configured fast-lane sources
  completed without an error on the first reloaded cycle.
- Removed avoidable long-worker churn by publishing progress heartbeats for the
  causal-level and direct-source response jobs and using phase-appropriate
  20/15-minute stall bounds. Hardened five remaining atomic JSON publishers
  with collision-proof temporary names and bounded retry on transient Windows
  replacement denial. This directly addresses 36 recent stderr incidents and
  36 churn restarts without weakening fail-closed output semantics.
- Six brief chunked quote-stream disconnects self-recovered in 2.4-2.9 seconds;
  current session/observer error counts are zero. Storage remains healthy with
  about **191.7 GiB free**; the canonical outcome ledger is 46.26 GB and the
  edge-evidence database is 8.39 GB. The unexplained same-day free-space gain is
  retained as an operational observation rather than attributed speculatively.
- Integrated validation passed **597 focused tests**; the vault allowlist and
  its new source/operations tests passed a further 27-test check. A controlled
  hidden restart loaded V149, the communications fast lane, V4, rank V3, and
  the progress-heartbeat repairs. The old V3/rank-V2 live processes are absent.
- Reloaded the four remaining long-lived pre-patch trees while Practice 007 was
  confirmed flat: the executor quote stream, independent quote stream, quote
  crosscheck, and macro-surprise worker. Post-reload the executor is connected
  at 68/68 with zero session/observer errors, the crosscheck is healthy at
  68/68 with p95 quote-time skew 4.88 seconds and p95 price divergence 0.156x
  average spread, and the macro worker prefiltered 87,590 news rows to 449
  structured candidates instead of rereading full payloads downstream.
- Synchronized the complete 1,364-file model checkpoint and the separate
  90-file news/source checkpoint to the current OneDrive vault and verified
  backup, including the V4/rank-V3 contracts, tests, project log, and pending
  queue. Their manifests are content-hashed; the news/source checkpoint hash is
  `3c2ea36ad054741b510d665e8a1254d8a1f10fb36974a94079c31bd843bc3023`.

## 2026-08-28 02:12 America/New_York — Live-watch baseline repaired and V3 source proof staged

- Practice 007 remained flat at balance/NAV **41.6042**, with zero open
  trades/orders and cumulative P/L **-8.3430**. The refreshed practice
  executor is connected, clock-aligned, pricing 68/68 instruments, and has
  zero qualified candidates. Real-money routing remains disabled.
- The first post-activation V2 event exposed a fail-closed classification
  defect: a BOJ Review about hedonic CGPI methodology was labeled
  `fx_intervention`, acquired unrelated CAD/GBP/tariff entities, and parsed a
  chart range (`40-20`) as a policy vote. It produced no valid exact entry, no
  proof forecast, and no trade. Classifier V148 now routes this document class
  to non-market research, requires bounded monetary-currency context for bare
  intervention language, and requires local voting language for vote splits.
- Upcoming-event preflight V5 now uses authoritative `direct_currencies`
  rather than the event tagger's expanded affected-currency set. The 10:00 ET
  Fed Chair event is therefore a direct USD clock; separately declared policy
  dependencies remain explicit instead of appearing as direct event legs.
- The fast mapper now freezes a pre-semantic executable quote bundle before
  PDF parsing and global diagnostics. Downstream proof independently enforces
  the unchanged 15-second capture/offset boundary, rejects late-only bundles,
  and never attaches later technical state to an earlier entry. Unchanged
  integrity/global diagnostics are fingerprint-cached and mapper cadence is
  start-to-start.
- V2 remains immutable. A separate research-only V3 source cohort and rank-V2
  adapter were created with new databases/contracts and a prospective boundary
  of **2026-08-28 03:00 ET**. V3 requires classifier V148 and the pre-semantic
  quote contract; it imports no V1/V2 evidence. The supervisor now retires V1,
  V2, and rank V1 and runs only V3 plus rank V2.
- A pre-coordination live mapper timing probe append-only added 1,577 V148
  mappings. They are retained as diagnostics, predate V3 activation, cannot be
  consumed by V2, and are not migrated or relabeled into V3 proof.
- Repaired operational churn: macro workers now prefilter 86,398 news rows to
  446 structured candidates instead of fetching about 603.5 MB each cycle;
  long jobs publish independent progress heartbeats; current all-68 M1 archives
  request gap-sized batches instead of 5,000 candles per pair; official fast-
  lane atomic writes use unique retrying temporary files; and reconnect-seeded
  quotes are retained research-only rather than counted as current transport.
- A controlled hidden supervisor restart loaded the changes. Quote transport
  recovered to healthy 68/68 with p95 time skew 4.24 seconds and p95 price
  divergence 0.143x spread. Macro and archive progress heartbeats are fresh.
- Integrated validation passed **173 tests**. One unrelated historical PPI
  discovery fixture was deliberately deselected because its retained quote
  artifact is unavailable; it is not used by prospective or execution paths.

## 2026-08-27 22:42 America/New_York — Source-response, rank, and causal timing branch completed

- Closed the active news-watchlist dependency on the retired executable-
  opportunity producer. The watchlist now reports
  `opportunity_state=retired_drain_only`, performs no repeated scan of that
  3.3 GiB historical ledger, and continues its independent news/technical
  comparison arms. Its focused suite passed 36 tests.
- Added the immutable, research-only
  `source_conditioned_currency_rank_v1` adapter. It consumes only V2
  `forecast_state=forecast` rows that are prospective-proof eligible,
  deduplicates by market episode x currency x horizon, and freezes comparable
  price-only, source-only, and source-plus-price/spread timing arms. Missing
  source state remains unavailable rather than zero. Entry quotes must be
  timestamped after every contributing source knowledge clock and before the
  frozen decision cutoff. Evidence is reported separately by immutable
  adapter cohort, and sealed V1 cannot be selected implicitly.
- Added the immutable `news_band_resolution_flow_h15_v1` comparison worker.
  It requires an already-known news direction, prior frozen causal level,
  completed contact minute, distinct completed break/rejection minute, and
  completed signed quote-change-intensity minute before committing a future
  M1 bid/ask entry. It compares fixed H15 with band invalidation and preserves
  matched no-flow/conflicted controls. Source cohort and contract lists are
  bound from the actual watchlist schema; correlated pair expressions count
  once per episode/currency.
- Corrected the quote-intensity retention test to use an injected deterministic
  clock. Runtime retention remains seven days; the test no longer expires as
  wall time advances.
- One coordinated hidden-supervisor reload adopted the V2 response map, V2-only
  currency-rank adapter, and news-band timing worker. Exactly one logical
  wrapper/child tree exists for each; V1 is absent. The final integration suite
  passed 82 tests, the independent quote-intensity suite passed two tests,
  Python compilation and PowerShell parsing passed, and all three new ledgers
  returned SQLite `quick_check=ok` with complete update/delete rejection
  triggers.
- Live evidence remains an honest null: zero post-activation V2 proof events,
  zero rank decisions/outcomes, and zero qualifying news-band candidates.
  Practice 007 remained flat at balance/NAV 41.6042 with zero trades/orders;
  real-money routing remains disabled. Storage guard status was `ok` with
  about 120.5 GiB free.

## 2026-08-27 22:24 America/New_York — Fixed 10-minute source-factor response cohort activated

- Added the bounded, research-only
  `causal_source_factor_response_map_v2_point_in_time_episode_20260828`
  contract with fixed **1/5/10/15/30/60/120-minute** executable response
  horizons. It retains the V1 point-in-time event/factor, market-episode,
  currency-factor, bid/ask path, and prequential cutoff rules; it adds only the
  previously missing exact 10-minute response cell.
- Started the distinct prospective cohort
  `causal_source_factor_response_map_v2_prospective_20260828T030000Z` with a
  new SQLite ledger, snapshot, and report. The activation boundary is later
  than implementation and validation, so all 11 existing events remain
  preactivation diagnostics and cannot become proof.
- Sealed V1 as preserved/inactive in the runtime-retirement registry and
  changed the hidden supervisor to stop V1 and run V2. V1 code and ledger
  hashes remained unchanged across the cutover; no V1 row was migrated,
  rewritten, or deleted.
- The first supervised V2 checkpoint has 11 events/episodes, 91 factor
  observations, 77 responses over seven horizons, and 637 prequential
  forecasts (469 low-support abstentions). It has zero prospective proof
  events, zero exact entry snapshots, and SQLite integrity `ok`. The supported
  decision remains research-only/no-trade.
- Focused validation passed **69 tests**, including exact +10-minute maturity,
  both executable bid/ask paths, maturity-cutoff causality, distinct V2
  identities, V1 preservation, and supervisor adoption. The source-depth
  readiness report now reads V2 as its outcome source; causal outcomes remain
  zero until post-activation official releases arrive and mature.

## 2026-08-27 21:25 America/New_York — Dead-row production stopped and runtime surface deduplicated

- Froze new production in the executable-opportunity ranker after seven cohorts
  accumulated 169,376 forecasts and 166,128 outcomes with zero directional
  frozen-gate passes, 49.31% directional accuracy, and -3.014 average
  predicted-side net pips. The worker now runs `mature_only`, writes zero new
  forecasts, and preserves 3,214 outstanding declared horizons at the latest
  checkpoint. Its 500 MiB ledger remains immutable evidence.
- Added explicit runtime disposition for the paused allocator proof. It had
  3,528 decisions, more than one million candidate/integrity rows, zero fully
  matured eligible decisions, and no confirmed lifecycle input. It remains
  asleep until an exact confirmed candidate exists instead of persisting empty
  eligible sets.
- Added a 24-hour fail-closed freshness boundary to the strategy lab's signal
  combination model. Three August 3 rule artifacts can no longer reactivate
  historical `account_eligible` flags while their fit workers are disabled.
  Their database and rules remain preserved as historical evidence.
- Replaced two redundant Practice-007 account pollers with one one-second
  read-only poll. The single response is atomically written to both
  `account_007_dashboard_v1.json` and the compatibility
  `account_dashboard_v1.json`; the two payloads were verified byte-identical.
- Reanchored five manual/fit tools from stale `D:\forex\trad` defaults to the
  source file's canonical project root. CLI overrides remain available, and a
  regression test prevents those active defaults from returning to D.
- Generated the first content-hashed unified historical case inventory:
  `HISTORICAL_CASE_INDEX_CURRENT.json` covers 163 retained move/news case files
  with entry-list SHA-256
  `47c1c02a15af6df27c4570685ae9ba797ae3749c9a3884e12778eabe38c4c199`.
- Focused cleanup validation passed 30 runtime/retirement/default tests, 24
  combination tests, three account-writer/supervisor tests, and the historical
  index test. The hidden supervisor was reloaded and adopted the cleaned
  processes; Practice 007 remained flat and `no_trade`.

## 2026-08-27 21:15 America/New_York — Shadow retirement, runtime cleanup, and current audit

- Reconciled live shadow collectors against current input freshness and retained
  evidence. Disabled the HGB outcome tracker because it consumed the stale
  D-drive account-019 source and had zero pending work; its preserved ledger has
  6,006 outcomes, only 15 accepted rows, and a 6.67% accepted win rate. Disabled
  the manager-decision outcome ledger because its three forecasts were already
  mature and it only re-polled a frozen 10 August input. Neither collector had
  execution authority, and no code or evidence was deleted.
- Put the four-family H1 baseline into `mature_only` instead of discarding its
  outstanding forecasts. Future forecast production and feed publication are
  false; already-issued causal horizons continue to mature. At 21:13 ET it had
  4,056,490 issued forecasts, 3,985,454 matured prediction points, 5,816 pending,
  and zero errors. Its recent non-overlapping after-cost averages were -2.729,
  -2.481, -2.266, and -2.578 pips with no holdout candidate, so the completed
  stream will remain a preserved negative control.
- Kept the four immutable proof cohorts running because their exact governed
  cells remain underpowered after factor/episode deduplication. Marked unavailable
  order-book, position-book, and pricing-depth fields inert rather than pretending
  they are live features; marked the 30 zero-output model-gap contributors dormant
  rather than active consensus breadth.
- Verified the clean prospective level-band cohort
  `level_band_prospective_20260827a.9294e3511aae455f` running under
  `level_band_contract_v2_frozen_20260827b`. At 21:12 ET it had 65 ready contexts,
  150 forecasts, and 250 outcomes. It remains research-only and cannot authorize,
  promote, place, or manage an order.
- The latest source-governance checkpoint has 187 configured/observed and 179
  operational sources. Official policy releases and schedules are operational
  for 21/21 currencies and both legs of 68/68 pairs; 20/21 policy-release
  transports are currently healthy, with RBNZ the explicit exception. The source
  layer remains research-only.
- Ran a recoverable organization pass over derived runtime artifacts older than
  seven days. It moved 18,704 `.log`, `.bak`, and `.tmp` files totaling
  394,242,063 bytes into `data/archive/runtime_cleanup/20260828_010345` with JSON
  and CSV manifests and zero skips. This was a same-drive move and is not claimed
  as reclaimed capacity. The storage guard remains `ok` with about 124 GiB free;
  no live database, WAL/SHM, causal evidence, snapshot, source event, code, or
  credential was deleted or vacuumed.
- Added `SOURCE_MODEL_RUNTIME_CLEANUP_AUDIT_20260827.md` as the current audit and
  preserved every August 6/8 source and evidence audit as a dated baseline. The
  duplicate `docs/PENDING_IMPROVEMENTS.md` is now only an archive pointer to the
  root canonical queue. Practice 007 remained flat, zero candidates were
  confirmed, and the supported decision remained `no_trade`.

## 2026-08-24 23:32 America/New_York — Semantic-shadow gate separation and exact response arms

- Found and closed a research-coverage error in mapper V2: it required the
  production-style `directional_publish_eligible` gate before a prospectively
  observed official semantic hypothesis could enter a shadow response study.
  That would discard direct, verified policy semantics whenever causal
  consensus or rate confirmation was unavailable. V3 now records two distinct
  states: `prospective_semantic_candidate` and the stricter
  `publish_eligible_forward_candidate`. Only the former feeds research; neither
  can authorize or execute. Mapper V2 remains preserved with zero prospective
  observations.
- Added response-watch V2 as a new immutable cohort downstream of mapper V3.
  For every direct prospective currency score it records all available pair
  expressions at exact executable bid/ask, the pair-leg side, entry spread,
  first-seen-to-decision latency, and the contemporaneous 1/5/15-minute
  technical direction as aligned, conflicted, or unavailable. Later outcomes
  use ask-entry/bid-exit for buys and bid-entry/ask-exit for sells.
- Correlated pairs cannot inflate evidence. Every row retains one conservative
  five-minute source/currency factor episode, and summaries select only the
  lowest-entry-spread pair per factor and horizon. Missing or late target
  quotes become explicit invalid outcomes rather than wins or silent drops.
- The live checkpoint remains an honest zero: 558 mappings, 11 historical
  semantic directions, zero post-activation prospective inputs, zero response
  watches, and zero outcomes. Both hidden workers are supervised. Project
  integrity is `ok` with exact V3/V2 contract, count, freshness, SQLite, and
  inert-policy checks.
- Validation passed 45 focused tests plus Python compilation. No existing
  proof cohort, Practice-007 gate, position, order, close, authorization,
  promotion, or real-money setting changed.

## 2026-08-24 23:14 America/New_York — Exact official-release fast path and semantic mapper

- Closed an operational latency gap between configured source polling and the
  broad collector's roughly ten-minute full cycle. Added a separately
  supervised raw-observation lane for the 23 central-bank release transports
  covering all 21 currencies. It writes exact append-only source observations
  and publisher/first-seen clocks to its own SQLite database and has no
  classifier, broker, lifecycle, authorization, promotion, or execution
  surface.
- Three real bootstrap adversaries were found before deployment. V1 could
  mistake a same-document detail enrichment for a new observation; V2's
  identity was not stable across process reloads; and V3 could mistake an old
  item newly appearing on a feed page for a current release. All three outputs
  are preserved in named quarantine directories. V4 now uses canonical
  publisher identity, ignores listing-ingestion metadata when comparing
  content, requires a genuinely new identity, and requires a source-native
  publication clock within the fast-lane era. Revisions without a native
  revision clock remain nonprospective.
- Added a separate five-second semantic mapper. Mapper V2 applies the frozen
  V142 rules at the raw row's original first-seen clock and recognizes both
  primary and research-only currency scores. It cannot mutate the canonical
  watchlist or contact any broker. The first stable checkpoint contains 558
  raw observations and 558 mappings from 22 observed transports; 11 historical
  mappings have a semantic direction, while zero inputs and zero outputs are
  prospective. The RBNZ direct page remains the only transport error (HTTP
  403), so no publisher restriction was bypassed.
- Added exact fast-lane and mapper sentinels to project integrity: contract,
  schema, heartbeat freshness, source/currency coverage, SQLite integrity,
  raw/mapped count agreement, classifier identity, and inert policy must all
  match. The hidden supervisor was rotated onto the new definitions. Project
  integrity reports `ok` with no failures and both checks true.
- Validation passed 41 focused tests plus Python compilation. Practice 007
  remained unchanged and flat; no order, close, authorization, lifecycle,
  execution, or real-money setting changed.

## 2026-08-24 22:12 America/New_York — Faster proof maturity, cost buckets, and direct RBA minutes

- Reduced the all-68 executable M1 forward-archive cadence from 60 minutes to
  five minutes. The first repaired cycle covered all 68 pairs, appended 2,789
  completed candles with zero errors, and finished in about 83 seconds. The
  outcome report now exposes archive lag and distinguishes a not-yet-observed
  future path from a losing forecast.
- The quote-bound forward ledger matured its first 85 outcomes. Results remain
  diagnostic and negative: among liquid factor-deduplicated rows, the 5-minute
  broad-context arm was 0/4 with -1.12 pips average net and the 5-minute
  technical-continuation arm was 0/4 with -1.38 pips average net. The only
  positive point estimate was 15-minute broad context at 1/2 and +0.30 pips,
  far too small to support an edge claim. Results are now split into liquid,
  moderate, wide and very-wide cost buckets and normalized by entry spread.
- Closed a factor-count inflation path without rewriting history. If the live
  leaderboard changes which correlated pair is marked representative, the
  evaluator now keeps exactly the earliest case per arm, horizon, cost bucket
  and signed-currency-factor episode. Later representative flips cannot
  increase effective N.
- Added the compact factor-deduplicated forward-proof table and M1 archive-lag
  indicator to the live dashboard. Practice 007 remained flat at NAV 41.6042,
  with zero positions and zero pending orders.
- A current AUD source audit found that RBA minutes appeared in secondary
  recaps about eight minutes after their scheduled official publication while
  the configured RBA RSS paths did not expose the minutes document. Added a
  separate 60-second first-party RBA monetary-policy-minutes HTML feed and
  mapped it as a second AUD policy-release transport. The official listing
  returned HTTP 200 and five exact 2026 documents. Its initial contents are
  bootstrap/history-only; only later unseen documents can receive prospective
  first-seen clocks. The collector advances to V67 so old code cannot certify
  the new source configuration.
- Archived the current 27,974-character RBA minutes body under an explicit
  late-observed/bootstrap-only context contract. This exposed and repaired a
  document-structure error: whole-document phrase scoring treated an early
  recap of reduced market tightening expectations as the Board's conclusion,
  producing -0.65 AUD. V142 now scores the publisher's concluding guidance and
  explicit decision while retaining the full text. It also prevents an
  in-section financial-stability reference from truncating the decision and
  prevents the date title `5 May 2026` from being parsed as a hypothetical
  modal verb. Across all five official 2026 RBA minutes, the repaired diagnostic
  maps the three actual increases to +1.0 and the two conditional hawkish holds
  to +0.55. Every row remains noncausal, research-only and publish-ineligible
  because the pages were observed in a bootstrap replay; collector V71 binds
  the repaired classifier.
- Validation passed 79 live-proof/dashboard/integrity tests from the preceding
  deployment, 60 focused RBA/source-governance/integrity tests after the
  source addition, and the full 431-test news/central-bank suite after the
  policy-section repair. No execution, lifecycle, authorization or real-money
  gate changed, and no order was placed.

## 2026-08-24 21:52 America/New_York — Live proof contract sentinel

- Detected that the live movement/news process was still emitting its prior
  V3 contract even though the supervisor itself was healthy. Restarted only
  that hidden research worker; the active state now emits V4, binds 10/10
  mover cases to fresh executable bid/ask quotes, exposes all five frozen
  shadow arms, and deduplicates the current ten pair expressions to seven
  signed-currency factor episodes. Practice 007 and its executor were not
  restarted or altered.
- Added an exact integrity sentinel covering both the V4 live snapshot and V1
  forward-outcome contracts: schema, freshness, case-contract identity,
  SQLite integrity, and every inert execution/promotion flag must match. The
  project integrity audit is back to `ok` with zero failures and will now make
  a stale worker contract visible instead of accepting a fresh old artifact.
- Split forward-result reporting into raw pair statistics and
  factor-representative-only win rate/average. Correlated crosses therefore
  cannot inflate either the evidence count or reported performance. The
  prospective ledger still has zero matured outcomes, so no performance claim
  is made. Validation passed 63 focused dashboard/snapshot/outcome tests and
  36 integrity/snapshot/outcome tests. No order was placed.

## 2026-08-24 21:39 America/New_York — Riksbank transport redundancy and live forward proof

- The official-document SLA exposed a real single-channel weakness: the 20
  August Riksbank decision was scheduled for 07:30 UTC but reached the local
  RSS path at about 09:23 UTC. Collector cycles were already short, so the
  delay was attributable to the publisher channel or historical runtime
  availability rather than a slow current poll loop.
- Added `riksbank_monetary_policy_html`, a second free first-party transport
  over the Riksbank monetary-policy release listing. The current collector
  observed HTTP 200 and 10 official links, including the exact 20 August rate
  decision. The initial listing is explicitly bootstrap/history-only; future
  links use collector first-seen as causal time. RSS and HTML remain separate
  transports under a new central-bank map contract. Live coverage is again
  complete at 21/21 configured currencies and 68/68 pair legs; RBNZ remains
  the sole direct-release health limitation because its site blocks this host.
- Upgraded the live movement/news join to an append-only case ledger with
  contract-versioned identity, explicit explanation states, and signed
  currency-factor/time deduplication. A representative checkpoint reduced 10
  pair moves to 6 independent factor episodes; correlated ZAR and JPY pair
  expressions no longer inflate the apparent number of news hits.
- Froze exact fresh bid/ask entry economics for new V4 cases and started
  `oanda_live_move_news_outcomes.py`. It prospectively scores technical
  continuation, broad-context, strict-forward, continuous-narrative, and
  broad-context-plus-continuation arms at 5/15/30/60 minutes against later
  executable M1 bid/ask paths. The first 10 cases had 10 fresh quotes and zero
  matured outcomes at creation, which is the correct knowledge-time state.
- The dashboard now shows explanation state, factor representative status,
  retained case count, and matured forward outcomes. Both research workers are
  hidden and supervised; neither has broker, authorization, promotion, or
  execution capability.
- Validation passed 484 news/source/integrity tests, 39 live movement tests,
  53 dashboard/outcome tests, and 12 supervisor/isolation tests. One stale ABS
  test pin was reconciled to the already-active exact-clock source contract.
  Source governance completed at 176 configured news sources, SQLite integrity
  remained clean, and the project integrity audit returned `ok` with zero
  failures. No order was placed.

## 2026-08-24 20:58 America/New_York — Live movement-first source attribution

- Added `oanda_live_move_news_snapshot.py`, a supervised research-only join
  between the continuously reranked cost-clearing mover legs and a narrow
  point-in-time source window. It reports strict forward/publish-eligible news,
  broad research context, and the frozen narrative meter as three separate
  evidence classes. It has no broker, lifecycle, authorization, promotion, or
  execution surface.
- The current live join covers the top **10** cost-clearing velocity legs and
  rebuilds in about two seconds. At the checkpoint, **0/10** had a strict
  publish-eligible pre-move direction even though individual pairs had 17–60
  raw nearby context events. This is the intended distinction: article volume
  is not directional evidence.
- Added the same movement/news join to the live dashboard. The panel shows net
  move, strict source alignment, broad noncausal context, narrative state, and
  the nearest retained headline. The refreshed local API reports the snapshot
  live without changing Practice-007 state.
- Added an effective-time index to the append-only source-governance database
  and bounded the historical audit's source read to the exact move-window
  span. This makes narrow current-source queries fast while preserving every
  immutable event and revision. The complete 29-day audit remains a deliberately
  heavy six-hour batch; no evidence was dropped or compacted to force speed.
- Validation passed **33 live/movement-source tests, 57 source-governance and
  movement-index tests, 49 dashboard tests, and 8 supervisor/structure tests**,
  plus Python compilation. The hidden supervisor and dashboard were reloaded;
  existing healthy workers were retained. No order was placed.

## 2026-08-24 20:20 America/New_York — Full narrative replay and supervisor freshness repair

- Replayed the continuous 21-currency narrative meter over its complete
  retained point-in-time history: **168,378 currency clocks across 29 market
  days**, all **21 currencies**, and all **68 instruments**. The V10 executable
  bid/ask comparison selected **8,428 non-overlapping decisions** and produced
  **91,933 outcomes**. No sentiment-only, technical-only, conjunction, or
  conflict model was positive after costs across train, validation, and test.
- Added an outcome-independent source-quality census. Only **18** decisions
  were both forward-timely and from a trusted direct source; the large timely
  sample is predominantly secondary/untrusted discovery evidence. This makes
  the central limitation explicit: current breadth is not causal-source proof.
- Added exact decision-stream equivalence. The six named narrative formulas
  reduce to **five independent streams** because `published_semantic_v1` and
  `source_balanced_v1` emit identical decisions. They can no longer be counted
  as independent confirmation.
- Added a specialized, frozen RBNZ schedule magnitude prior to the neutral
  event preflight. It contributes **12 independent policy events** and a median
  H1 absolute NZD-factor movement of **51.15 bps**, but supplies no side,
  matched-control claim, promotion, or execution authority. Comparable priors
  now exist for **6 of 69** upcoming event/currency rows.
- Confirmed that direct RBNZ policy pages return HTTP 403 on this host. RBNZ's
  published terms prohibit automated access without permission; no bypass or
  discovery-proxy relabeling was attempted. The honest live minimum remains
  20/21 direct, with the official calendar, Stats NZ, Treasury, and
  official-publisher discovery fallback retained separately.
- Found an operational freshness defect: the long-running PowerShell
  supervisor still held the retired V8 narrative-state path in memory and was
  recycling a healthy V11 worker about every six minutes. Reloaded the same
  supervisor command hidden; it now monitors V11, retains the worker PIDs, and
  has issued **zero** stale-output restarts.
- Repaired an integrity-audit false alarm during long source-governance builds.
  The audit now verifies exact current collector identity for both terminal
  `ok` and honest fail-closed `building_governance` states, while still
  rejecting any operational capability or unknown status. The fresh project
  audit is **ok with zero failures**.
- Validation passed **9 event-preflight tests, 16 narrative meter/backtest
  tests, and 47 source-governance/project-integrity tests**, plus Python
  compilation. Practice 007 remains practice-only, flat, and unchanged; no
  order was placed.

## 2026-08-24 19:52 America/New_York — Comparable-event magnitude prior and genealogy repair

- Added a research-only magnitude-risk prior to the neutral event/technical
  preflight. It now selects historical evidence in the order exact release
  series, same currency/event class, then same event class, with minimum
  independent-event counts. There is no generic event fallback and every
  prior explicitly abstains on direction and execution.
- The corrected live board maps **69 upcoming currency/event rows across all
  21 currencies and 68 pairs**. Only **5** currently have sufficiently
  comparable magnitude history. Routine poultry, population, trade, and other
  low-severity releases no longer inherit a global policy/event prior. A Thai
  policy decision correctly reports insufficient comparable episodes rather
  than borrowing unrelated evidence.
- Found and repaired an append-only genealogy conflict caused by adding new
  comparison arms while retaining the old watchlist collector cohort ID.
  Collector V40 now explicitly supersedes V39; V39 remains immutable. The
  supervised hidden worker restarted under V40, the missing lifecycle cell was
  registered, and the independent verifier returned **match / authorization
  safe** with **0 failed checks and 0 confirmed candidates**.
- Formally retired the unused after-cost counterfactual V6 lineage after its
  manifest exposed two shared mutable dependency paths that had changed after
  freeze. V6 has no state artifact, proof, lifecycle, genealogy, or execution
  rows. Its manifest was not rewritten; the exact expected/observed hashes and
  terminal no-trade disposition are preserved in a separate retirement
  certificate. Any replacement requires a new cohort and versioned immutable
  dependency paths.
- Repaired the vault handoff itself. Each checkpoint now also publishes exact
  standalone mirrors of pending work, project log, source-gap register, feature
  inventory, and 21-currency source-depth contract with a shared manifest.
  The canonical pending file and vault mirror now have the same SHA-256.
- Validation passed **7 focused preflight tests, 52 integrated source/replay
  tests, 76 watchlist/genealogy/verifier tests, 21 project-integrity tests,
  and 9 vault-sync tests**, plus Python compile and JSON validation. Practice
  007 remains flat, practice-only, and unchanged; no order was placed.

## 2026-08-24 19:29 America/New_York — Strict historical official clocks reach 21/21

- Recovered exact, official public-release clocks for the remaining historical
  availability gaps: Australian CPI/WPI, Swiss CPI/PPI, Chinese CPI/PPI,
  Hungarian CPI, Polish CPI/GDP, and Thai CPI. Every clock is tied to an
  official release page, calendar, or frozen publication-time rule. These are
  archive-only availability facts; they provide no consensus, surprise,
  direction, proof, promotion, authorization, or execution eligibility.
- The strict replay now covers **21/21 currencies and 136/136 pair legs** with
  **56 deduplicated events, 47 independent currency-factor/source-time
  episodes, 44 unique source timestamps, 2,803 pair/horizon outcomes, and zero
  OANDA candle fetch errors**. Repeated current-state polls remain excluded.
- Official event windows showed greater median absolute currency movement than
  matched controls by **+2.35/+2.75/+2.33/+3.62 bps** at 1m/5m/15m/60m.
  However, all 108 predeclared direction/timing sweep cells with N >= 8 had
  non-positive after-cost point expectancy. The result supports event-risk
  timing research, not a trade side.
- The replay now retains the frozen source-contract ID and clock-evidence URL
  with every repaired event. The final combined source/replay/completeness/
  reconciliation suite passes **129 tests**.
- Live minimum readiness remains **20/21** only because direct RBNZ policy pages
  return HTTP 403 from this host. Practice 007 remains supervised, practice-
  only, and unchanged; no order was placed.

## 2026-08-24 19:13 America/New_York — Exact-clock replay and snapshot identity repair

- Audited the official numeric-release replay before using its results for the
  next source-first hypothesis pass. One Australian WPI observation had been
  counted as **52 releases** because repeated enrichment polls inherited their
  collection times as release timestamps. Similar current-state repetition was
  present in the SARB policy-rate source. The immutable macro ledger was not
  deleted or rewritten.
- Historical replay now accepts only an explicit source-native publication
  clock, a frozen official schedule, or a separately reviewed archive-only
  exact-clock certificate. Inferred collection clocks and first-seen clocks
  that merely equal collection time are excluded and deduplicated by economic
  identity. Reviewed, disabled CZK and MXN exact clocks were added; HUF and THB
  remain truthfully unresolved.
- Future unclocked current-state polls now share one stable state identity and
  enter the append-only ledger as revisions rather than manufacturing a new
  release key on every poll. Exact scheduled/native releases retain distinct
  identities.
- The corrected GET-only OANDA replay contains **46 clock-valid source events,
  39 independent currency clocks, 15 currencies, and 2,615 pair/horizon
  rows**, with zero fetch errors. Official windows retain greater absolute
  movement than matched controls at 1m/5m/15m/60m by approximately
  **1.31/1.43/1.53/2.34 basis points**. The one-minute factor-following entry
  remains negative after cost at every evaluated horizon, so this is a
  magnitude/timing finding, not a directional edge or execution candidate.
- The combined source/replay/completeness regression set passes **122 tests**.
  Practice 007 and all authorization, promotion, and real-money boundaries are
  unchanged.
- Live minimum official-feed readiness is currently **20/21** because RBNZ's
  direct policy/OCR pages return HTTP 403 to the bounded collector. Its frozen
  official decision calendar, Stats NZ releases/calendar, and Treasury feed
  remain healthy. The official-publisher search channel is not being relabeled
  as direct central-bank truth.

## 2026-08-24 18:37 America/New_York — Causal surprise/rate binding and evidence index

- Repaired a misleading macro-ledger readiness state. Two retained releases
  contain consensus numbers, but both were observed without a verified pre-
  release clock. Contract `macro_surprise_causal_readiness_v3_20260824` now
  reports raw/causal/noncausal actual-plus-consensus counts separately as
  `2 / 0 / 2` and correctly returns
  `causal_consensus_source_unavailable`. Inline values cannot self-certify.
- Added immutable `official_surprise_rate_confirmed_h15_v1_20260824` and
  negative-control cohorts. Confirmation requires causal consensus, a
  standardized surprise of at least `|0.5|`, explicit series currency
  direction, timestamp-safe 15-minute rate repricing of at least `|1.0|` basis
  point, and directional agreement. These cohorts are research-only and
  currently emitted zero rows because both external inputs are unavailable.
- Added a non-destructive `(cohort_id,status,arm)` SQLite index to the 2.4 GB
  watchlist evidence database. Its first indexed cycle completed in about 17
  seconds after the predecessor unindexed scan exceeded three minutes. SQLite
  integrity remains `ok`.
- The combined focused/downstream suite passes **121 tests**. The hidden macro
  and watchlist workers restarted under the supervisor with two-process chains.
  Practice 007 and every execution/authorization gate were unchanged.
- The first integrity cycle briefly observed the independent verifier while it
  was rebuilding; the verifier completed `match` with zero failed checks and an
  immediate read-only integrity refresh returned `ok` / `no_trade`.
- Synchronized the credential-free 1,268-file, 627.7 MB model/source checkpoint
  and the exact pending mirror to the shared OneDrive vault. Source and vault
  pending hashes match.

## 2026-08-24 18:25 America/New_York — Remaining-move/cost-clearance proof arm

- Added six immutable research-only cohorts at 5/15/30 minutes. For every
  current liquid pair expression of a corroborated currency factor, the new
  contract subtracts completed thesis-direction movement, applies declared-
  horizon age decay, requires remaining magnitude of at least `1.5x` modeled
  executable cost and model cost-clearance probability of at least `0.55`, and
  retains every failed/missing/conflicted expression as a negative control.
- The predecessor top-three `news_magnitude_ranked` cohorts were not changed.
  The focused and downstream compatibility suites pass **113 tests**; compile
  checks also pass.
- Restarted only the hidden research watchlist worker. Its first fresh cycle
  produced 18 comparison rows over six liquid CAD expressions and three
  horizons; all 18 were correctly negative controls because the current factor
  was horizon-expired and/or model-conflicted or below cost-clearance. The
  state remains `research_only=true`, `execution_eligible=false`, and
  `can_place_orders=false`.
- Practice 007 remained flat and unchanged: NAV/balance `41.6042`, cumulative
  P/L `-8.3430`, zero open trades, zero pending orders, and zero margin used.
  Project integrity remained `ok`. Storage remained non-blocking at
  **137.717 GiB free**; no historical evidence was deleted.

## 2026-08-24 17:34 America/New_York — Five-hour live watch close

- Practice 007 remained flat throughout the watch: NAV/balance `41.6042`,
  cumulative P/L `-8.3430`, zero open trades, zero pending orders, and zero
  qualified signals. No discretionary order or close was submitted and real
  money remained disabled.
- The 68-pair mover surface was continuously audited. The material intraday
  legs were dominated by earlier USD weakness against PLN, SEK, and ZAR; no
  fresh post-rollover mover became a validated forward entry. The independent
  lifecycle verifier remained `match` with zero confirmed candidates.
- Completed collector v66 across all 175 configured sources and classifier
  v138 across 5,668 retained rows. Source governance, watchlist, narrative
  meter v11, and integrity were refreshed against the completed contracts.
  Three focused research-only classification repairs were validated by 431
  tests: clause-local oil direction, retrospective currency-performance
  headlines, and hypothetical escalation questions.
- At close, integrity's only failure was downstream freshness of the 670-case
  move-first-news audit after its upstream gap census advanced. The exact
  research worker was restarted under the hidden supervisor and remained
  actively rebuilding; execution, authorization, and account safety were not
  affected.

## 2026-08-24 16:46 America/New_York — Hypothetical escalation question guard

- A secondary headline asking what options Iran had `to escalate further` was
  classified as an observed risk-off event in the research channel. It was
  already non-publishable and non-executable, but it still biased the shadow
  narrative basket.
- Extended the geopolitical-hypothetical grammar to explicit `options/ways to
  attack/strike/invade/escalate/war` questions. The classifier plus narrative
  suites pass 431 tests. Froze the final watch cohorts as classifier v138,
  collector v66, and narrative meter v11; all remain research-only.

## 2026-08-24 16:15 America/New_York — Currency-performance recap boundary

- The first complete v136 cycle exposed a second retrospective leak in the
  secondary research channel: `Risk-off vibe sees NZD and AUD underperform`
  was observed ten minutes after publication and described the price outcome,
  but `underperform` was not part of the explicit market-move grammar.
- Added a conservative ISO-currency `outperform/underperform` headline pattern
  to the prior-move boundary. The complete classifier plus narrative regression
  set passes 430 tests. Froze classifier v137, collector v65, and narrative
  meter v10; supervised hidden replacements started successfully. The change
  is research-only and does not alter Practice-007 entry authorization.

## 2026-08-24 16:05 America/New_York — Clause-local commodity mapping and recap exclusion

- A live secondary multi-asset ticker headline said Dow Jones rose, oil prices
  fell, and gold surged. The v135 fallback looked ahead from `oil` across the
  ampersand to `gold surged`, set both oil directions, and preferred the wrong
  positive sign. That contaminated the research-only CAD/NOK/MXN/JPY mapping;
  it never had publish or execution authority.
- Added clause-local oil direction extraction, fail-neutral handling for
  unresolved broad conflicts, and an explicit retrospective boundary for
  `LIVE ... latest move ... why stocks are up/down` ticker recaps. Oil-down now
  maps exporters CAD/NOK/MXN negative and importer JPY positive while the
  already-observed market recap is excluded from forward narrative credit.
- The full 415-test local-news suite and all 14 narrative-meter/backtest tests
  pass. A separate full-suite catch also fixed Treasury administrative
  sanctions removals being neutralized and then incorrectly re-promoted as a
  sanctions escalation later in the same classifier.
- Froze new inert research identities: classifier v136, collector v64, and
  narrative meter v9. Hidden supervised replacements started normally; v9 is
  now consuming v136 rows, remains research-only, and has placed zero orders.

## 2026-08-24 15:50 America/New_York — Patched cycles complete and USD-factor correction

- The patched news collector completed a full live cycle: 175/175 configured
  sources, 5,000/5,000 retained-row reclassifications, derived clustering, and
  completed-snapshot publication. It survived the exact workload that had
  repeatedly triggered the 15-minute progress guard. The lifecycle/allocator/
  accounting/opportunity worker also completed end to end; the independent
  verifier subsequently rebuilt the ledgers with zero failed checks.
- The 15:39--15:50 JPY-cross move was initially compatible with the late Iran
  risk narrative, but the 15-minute 68-pair decomposition showed USD at about
  -2.35 median bps while JPY was only +0.13. USD/CAD was -4.65 pips,
  USD/JPY -4.05, NZD/USD +2.05, and USD/CHF -2.75. The episode is therefore
  recorded as broad USD weakness, not credited as a clean JPY safe-haven news
  hit. The secondary sanctions story remains useful context only.

## 2026-08-24 15:43 America/New_York — Collector/lifecycle liveness repair and JPY continuation audit

- The news collector was completing all 175 source polls and durably committing
  new articles, then being killed after 15 minutes inside an opaque retained-row
  reclassification/derived-view stage. Added bounded progress checkpoints every
  100 retained rows and between reclassification, retention, loading, and
  clustering. The change does not alter classification, timestamps, or derived
  evidence. Two focused collector tests and compilation passed.
- The lifecycle worker's 52,420-cell ingest was similarly alive but opaque.
  Added progress every 500 cells plus lifecycle substage checkpoints. Seven
  lifecycle/worker tests and compilation passed. The restarted worker reported
  52,420/52,420 cells and advanced to the allocator instead of being mistaken
  for a deadlock. Both liveness payloads remain research-only and fail closed.
- At about 15:39--15:42, USD/JPY fell 4.15 pips versus a 1.6-pip spread, with
  EUR/JPY and GBP/JPY also falling. A technical-only USD/JPY long observed near
  15:39 had 54.4% confidence but only +0.033 projected net pips; it remained
  ineligible and is preserved as a research miss rather than an avoided trade.
- Secondary Iran-rial/sanctions coverage published at 15:20 was not first seen
  until 15:42, after the JPY move began. It mapped the expected risk-off basket
  in the isolated secondary arm but remained non-publishable. V8 collapsed
  4,747 input rows into 34 independent stories and kept published direction
  neutral, so the late continuation narrative cannot receive causal entry
  credit. Practice 007 remained flat at NAV $41.6042.

## 2026-08-24 15:13 America/New_York — Evidence-operations restart-loop repair

- The supervisor had been restarting the passive evidence-operations worker
  about every 20 minutes because the four-stage lifecycle/allocator/accounting/
  opportunity cycle did not publish until completion. This prevented lifecycle
  and allocator outputs from refreshing and correctly forced the independent
  verifier and integrity audit into a stale, fail-closed state.
- Added a separate research-only liveness heartbeat with per-stage phase,
  progress age, and sequence fields. The last completed evidence report is no
  longer overwritten by an in-progress payload; a stage that makes no progress
  for 30 minutes still fails supervision.
- The liveness payload explicitly disables order placement, submission,
  promotion, and real-money routing. Four focused tests, Python compilation,
  and PowerShell parsing passed. The repaired worker entered
  `running_lifecycle`; Practice-007 execution and the news/quote streams were
  left running and adopted by the restarted hidden supervisor.

## 2026-08-24 14:52 America/New_York — Sanctions continuation pulse and late Canada recap audit

- A second short risk-off pulse from roughly 14:31–14:40 matched the isolated
  sanctions hypothesis: NZD/USD fell 3.90 gross pips (about +1.60 after cost),
  while USD/SEK and USD/NOK rose 62.85/+31.00 net and 54.95/+18.90 net pips.
  No new independent authoritative event preceded this pulse, so it is
  continuation evidence inside the same episode, not a second news hit.
- A Finnhub/InvestingLive Canada commentary item was first seen 33 minutes
  after publication and explicitly said USD/CAD was already up 93 pips. Its
  body also discussed unemployment, causing a false `labor_release` category;
  it remained neutral, publish-ineligible, and non-executable. The next safe
  collector cohort will add a conservative body-level pair/pip recap veto and
  separate incidental labour prose from a real release.
- A newer 50% Canada-tariff headline observed at 14:44 was a rewrite of the
  already-known tariff episode. USD/CAD slipped about 0.5 pip from 14:40–14:48,
  so the row is not credited as a fresh CAD-negative hit.

## 2026-08-24 14:35 America/New_York — Secondary narrative persistence repair after AUD reversal

- AUD/USD reversed upward by 8.05 gross pips from 13:55–14:22 ET (about
  +6.7 pips after the contemporaneous spread), while the isolated secondary
  sanctions arm still held AUD near -0.41. No fresh authoritative AUD-specific
  event explained the rebound. This is preserved as a miss for the secondary
  narrative hypothesis, not grounds to loosen execution.
- The audit found two research-meter defects: source-local event lineage IDs
  made syndicated secondary copies look independent, and the prior normalized
  weighted mean applied decay to both numerator and denominator, so a common
  narrative retained constant magnitude until hard expiry.
- V8 conservatively deduplicates secondary rows by their currency/topic
  signature when available and divides decayed evidence by original quality
  mass. Narrative magnitude now fades continuously. The live sanctions state
  fell from 85–86 secondary stories per affected currency to four; published
  story counts and scores remain exactly zero. V8 remains research-only,
  covers all 21 currencies/68 pairs, and cannot place orders.
- Fourteen focused meter/backtest tests, 47 dashboard tests, and compilation
  passed. Only the meter/dashboard/supervisor were rolled; Practice 007's
  policy-wired executor remained streaming and untouched.

## 2026-08-24 14:23 America/New_York — Published/secondary isolation and meter read optimization

- The continuous meter's `published_semantic_v1` arm treated database
  `relevant=true` as equivalent to `directional_publish_eligible=true`.
  Secondary sanctions copies therefore produced a nonzero "published" score
  despite their explicit publication veto. This could not trade, but it
  contaminated the main-versus-secondary research comparison.
- V7 now requires the exact directional publication flag for the published
  arm. Relevant or non-relevant rows with deterministic but unpublishable
  direction are isolated in `secondary_directional_discovery_v1`; relevant
  rows without direction are counted separately and omitted. The current
  sanctions state now shows zero published stories/score and 85–86 secondary
  stories per mapped currency, with the research score unchanged.
- The first v7 build also exposed a full-table scan on the 700 MB news ledger.
  Rewrote the equivalent two-day read predicate to use the existing
  `(relevant, first_seen_utc)` range index; `EXPLAIN QUERY PLAN` now confirms an
  index search. The restarted build completed in under 23 seconds instead of
  remaining in a multi-minute scan. No database schema or historical rows were
  changed.
- Twelve focused meter/backtest tests, including index-plan and relevant-but-
  unpublishable isolation regressions, plus 47 dashboard tests and compilation
  passed. V7 remains research-only and cannot place orders.

## 2026-08-24 14:12 America/New_York — Official Treasury sanctions mapping repair

- The direct U.S. Treasury release and prepared remarks for Operation Economic
  Outcast were initially classified as inflation/labor context with no
  direction, while secondary copies carried the sanctions hypothesis. The
  broad Treasury `primary_policy_release` feed needed an action-specific split
  between domestic finance documents, sanctions relief, and OFAC escalation.
- Added a source-specific first-party sanctions-campaign detector for exact
  Treasury/OFAC actions, including Operation Economic Outcast, while retaining
  the separate sanctions-relief neutralizer. Both official rows now map to the
  governed risk-off basket with `official_sanctions_escalation=true`; they are
  still publish-ineligible and have no execution authority.
- The official detail became available around 13:42 ET, after the initial
  13:08 burst and therefore cannot explain advance timing. It is confirmation,
  while the 12:42 expected-announcement headline remains the earliest observed
  narrative warning. AUD/USD subsequently rebounded 2.65 pips gross from
  13:55–14:04, clearing only 1.30 pips after cost, so blindly trading the late
  official confirmation would already face priced-in/reversal risk.
- Rolled classification/collection/meter evidence to v135/v63/v6. Seven
  action/relief regressions, 9 contract/classifier tests, 10 meter/backtest
  tests, 47 dashboard tests, 3 source-governance tests, and compilation passed.
  V6 is live at 21/21 currencies and 68/68 pairs, research-only and inert.

## 2026-08-24 14:05 America/New_York — Mixed secondary macro abstention and v5 rollover

- A fresh secondary headline, `Mexico GDP Misses, Inflation Rises, Banxico
  Holds`, was reduced to bullish MXN because its inflation/rate implication
  outweighed an unrecognized growth miss. The row could not publish or trade,
  but it would have entered the secondary-discovery pressure state.
- Added GDP/growth beat-and-miss semantics plus an explicit secondary mixed
  macro conflict. When activity and inferred policy impulses have opposite
  signs in an unverified headline, the classifier preserves both diagnostic
  components but emits no forward or research currency direction. The live
  row now has empty scores and `directional_evidence=false`.
- Rolled the classifier, collector, and meter to v134, v62, and v5 in new
  evidence paths. Six semantic regressions, 7 contract/classifier tests, 10
  meter/backtest tests, 47 dashboard tests, 3 source-governance tests, and
  compilation passed. V5 is live for 21/21 currencies and 68/68 pairs,
  research-only with no order capability; Practice 007 remained uninterrupted.

## 2026-08-24 13:59 America/New_York — High-confidence/zero-edge technical miss

- The live research watchlist sampled a two-minute EUR/USD short with 0.7341
  raw technical confidence but effectively zero projected after-cost edge and
  a 1.6-pip entry spread. It was correctly research-only and ineligible.
- At maturity EUR/USD had risen 1.35 pips rather than fallen; the short result
  was -2.90 executable pips and did not clear cost. This prospective example
  reinforces the existing magnitude-versus-cost gate: directional confidence
  must not be treated as expected tradable value.

## 2026-08-24 13:57 America/New_York — Source-governance progress contract repair

- The integrity audit briefly reported that source governance used an obsolete
  news-collector contract while its registry rebuild was in progress. The
  worker itself had imported v61; the progress writer copied the preceding
  completed state and did not replace its contract fields until final commit.
- Progress states now explicitly bind the current collector contract and
  cohort while remaining `building_governance`, research-only, and unable to
  place orders. A regression proves a stale prior contract is replaced. Three
  focused tests and compilation passed.
- Restarted only source governance and the integrity auditor under the hidden
  supervisor. The live in-progress state now reports v61 correctly. This was a
  diagnostic race repair; collection, evidence history, and execution policy
  were not changed.

## 2026-08-24 13:51 America/New_York — Sanctions-relief polarity guard and v4 rollover

- The next official batch exposed a live opposite-action error: U.S. Treasury
  `Deliver Additional Sanctions Relief on Syria` inherited the high-weight
  sanctions noun and was represented as a broad risk-off escalation. The row
  was publish-ineligible and never had order authority, but it polluted the
  research pressure state.
- Added a sanctions-relief/waiver/lifting/suspension action guard. Such items
  retain official context but receive neither the sanctions-escalation basket
  nor an automatic risk-on basket; a country-specific easing hypothesis must
  be governed separately. The live item now reclassifies as `market_news`,
  with zero risk-off/risk-on scores, no currency scores, and no directional
  evidence.
- Rolled classification to `local_fx_news_rules_20260824_v133`, collection to
  `local_news_incremental_source_commit_v61_20260824`, and the continuous
  meter to `continuous_currency_narrative_meter_v4_20260824` in new v4 paths.
  The short v3 run is preserved as a transitional diagnostic, not merged into
  v4 evidence.
- Six sanctions-focused regressions passed, followed by 7 classifier/contract,
  10 meter/backtest, and 47 dashboard tests; compilation passed. Reloaded only
  the hidden supervisor/research workers. V4 is live at 21/21 currencies and
  68/68 pairs, remains research-only, and cannot place orders. Practice 007's
  executor heartbeat continued throughout and the account stayed flat.

## 2026-08-24 13:44 America/New_York — Contract rollover and isolated secondary-discovery meter

- The two live classifier fixes above materially changed news semantics, so the
  unchanged classifier/collector identifiers were no longer an honest evidence
  boundary. Rolled the rules contract to
  `local_fx_news_rules_20260824_v132` and the collector contract to
  `local_news_incremental_source_commit_v60_20260824`. New rows cannot be
  silently mixed into the preceding classifier cohort.
- Froze the prior continuous meter as v2 and started
  `continuous_currency_narrative_meter_v3_20260824` in separate v3 database,
  state, backtest, and report paths. Any v2 rows produced after the first live
  semantic edit at 13:27 ET are transitional diagnostics, not current proof
  evidence; earlier frozen v2 reports remain preserved as point-in-time null
  results.
- V3 adds the queued `secondary_directional_discovery_v1` research arm. It can
  inspect rows with deterministic directional evidence that are deliberately
  excluded from publication pending corroboration, but keeps their published
  score at zero and cannot place or authorize orders. The main published,
  source-balanced, and narrative-acceleration scores continue to use only
  publish-relevant evidence.
- Added regression coverage proving secondary rows stay isolated and inert.
  Focused classifier/contract tests passed 7/7, meter/backtest tests 10/10, and
  dashboard tests 47/47 in the configured runtime; Python compilation passed.
- Restarted only the hidden supervisor and affected research/diagnostic
  workers. The v3 meter then produced 21/21 currency states, 68/68 pair states,
  seven model arms, and 45 secondary-discovery rows, with
  `research_only=true`, `execution_eligible=false`, and
  `can_place_orders=false`. Practice 007 remained continuously healthy and
  flat with no pending orders.

## 2026-08-24 13:20 America/New_York — Live sanctions/risk-off burst audit

- A cross-currency risk-off burst began near 13:08 ET: JPY and USD strengthened
  while AUD and CAD weakened. The live mover ledger measured AUD/USD down about
  4.5 bps, USD/JPY up about 3.8 bps, and the corresponding AUD/JPY decline; the
  liquid legs cleared their contemporaneous spreads.
- The source collector was not late to the developing story. A PBS headline
  announcing an expected new Iran-sanctions round was observed at 12:42:39 ET,
  about 25 minutes before the burst, and was correctly decomposed into the
  project risk-off basket. A second mapped copy arrived at 13:01 ET. The actual
  sanctions-announcement headlines were published around 13:06–13:07 and first
  observed around 13:14.
- The routed narrative/watch layers nevertheless omitted the early mapped
  warning because secondary stories with `directional_evidence=true` remain
  `relevant=false`/`directional_publish_eligible=false` pending corroboration.
  That gate is correct for execution, but reusing it in a research-only meter
  suppresses the very secondary-discovery arm that should be measured.
- Price timing did react: the technical-only ledger selected USD/JPY short at
  13:06:05 ET and realized +4.7 executable pips over its two-minute diagnostic
  horizon. The combined news-plus-technical arm was absent, not opposed. This
  case supports a separately labeled secondary-discovery narrative channel;
  it does not validate automatic trading from an expected-news headline.
- Attribution remains deliberately non-exclusive. Two Canada-tariff stories
  observed at 12:47 ET had already established a broad risk-off narrative
  state before the Iran-sanctions warning and announcement. The price burst is
  consistent with both narratives, but this audit cannot causally assign it to
  one headline. The live meter's clipped score and one-family agreement are
  uncalibrated pressure, not probability or independent confirmation.
- The execution feed expressed the later move only weakly: AUD/USD was short
  in the correct direction but projected just +0.23 pip against a 1.4-pip
  spread, while USD/JPY was long with +2.18 projected net pips but remained
  unvalidated. No non-conflicting qualified signal existed and Practice 007
  correctly stayed flat.
- A separate research defect was found in the news-magnitude ranking arm. It
  nominated GBP/CAD from an approximately three-hour-old CAD narrative even
  though predicted magnitude (3.56 pips) was below modeled cost (5.215 pips),
  cost-clearance probability was 0.357, and the factor was marked
  `possibly_priced_in`. A cost-clearance/age-aware comparison arm was queued;
  no execution setting was changed.
- Fixed a deterministic sanctions-grammar inconsistency discovered from the
  next live source batch. Headlines using `announcement of ... sanctions`,
  `launches ... sanctions`, or `to detail ... sanctions push` had fallen back
  to generic market news while `unveils sanctions` mapped correctly. The
  fresh-action recognizer now covers those equivalent forms, while all such
  secondary rows remain research-only and publish-ineligible without required
  corroboration. Six focused sanctions/negative-control tests and Python
  compilation passed. The hidden collector restarted under the supervisor at
  13:27 ET; Practice 007 and every execution gate were untouched.
- Quarantined a second live false clock. A VT Markets headline led with the
  already-observed pair state, `EUR/JPY steadies near 185.70`, then attributed
  it to Japan inflation/BoJ-hike bets and prospective ECB tightening. The
  watchlist had treated the recap as fresh uncorroborated EUR/JPY policy
  direction; the JPY expression lost 5.1 executable pips over five minutes
  and was simultaneously marked `reaction_conflict`. Pair-led price-state
  recaps (`steadies`, `hovers`, `holds`, or `trades` near a quoted level) now
  receive retrospective-market-report timing and cannot seed a fresh causal
  clock. Ten focused classifier/watchlist tests and compilation passed. The
  supervised collector restarted at 13:31 ET; execution was unchanged.

## 2026-08-24 12:30 America/New_York — Continuous 21-currency narrative meter, governed null, and 007-only operation

- Added `continuous_currency_narrative_meter_v2_20260824`: a dense five-minute
  `[-1,+1]` state for every one of 21 currencies and base-minus-quote research
  hypotheses for all 68 executable pairs. Six separately versioned formulas
  cover published semantics, research semantics, linguistic-tone placebo,
  source-family balancing, narrative acceleration, and the recovered blurb
  analog. The analog is explicitly inactive because the recovered response
  table has zero prequential direction-orientation rows.
- Added a same-clock historical comparison for sentiment-only,
  technical-only, sentiment-plus-technical, sentiment-plus-breakout, and
  disagreement arms at 5m/15m/1h/4h/1d using executable bid/ask outcomes and
  chronological 60/20/20 splits. The v2 run produced 72,298 decisions, 12,435
  price-scored clocks, and 779,863 outcome rows.
- Result: narrative acceleration and both conjunction arms were negative after
  costs at every tested horizon. No N>=30 cell was positive in both validation
  and test. A positive 1d technical-only test slice contradicted negative train
  and validation results and was rejected as unstable. No execution or
  lifecycle threshold was loosened.
- Corrected a custom-horizon shadowing defect in the reusable price-response
  diagnostic and added a regression test. An initial v1 meter also extended
  decayed states beyond the actual observation cutoff; v2 caps history at the
  true current clock and uses a new cohort instead of relabeling v1 evidence.
  The two reproducible superseded v1 SQLite products occupy about 497 MiB and
  remain clearly versioned; they are not read by the live worker.
- Added the narrative meter to hidden supervision and the live dashboard. The
  dashboard labels its scores as uncalibrated research pressure, not confidence
  or order authority. Focused meter/backtest/diagnostic/dashboard validation
  passed 63 tests.
- Retired and stopped the Practice-006 currency-rank worker and removed its
  dashboard/backend surface. Practice 007 remained running throughout. At the
  post-change check it was flat with NAV/balance $41.6042, cumulative realized
  P/L -$8.3430, and zero positions or pending orders. Real-money routing remains
  disabled.

## 2026-08-23 12:39 America/New_York — Historical news quality and 21-currency source-first diagnostic

- Added a reproducible read-only quality audit which separates the mutable
  current article view, immutable source versions, replay-safe observations,
  and movement-conditioned recovered blurb research. It cannot import broker,
  lifecycle, authorization, or execution paths.
- Audited 72,193 articles over 27 UTC days. The immutable registry contains
  857,967 versions, 796,647 supersessions, and 63,494 canonical story clusters.
  The retained OANDA archive covers all 68 pairs and all 21 currencies (about
  522 MiB).
- The replay funnel contains 5,151 retrospectively classifiable, price-covered
  direction rows and 1,890 also marked forward-timely, but only six diagnostic
  rows have the new trusted clock contract. Zero rows satisfy every prospective
  replay gate, zero direct official rows do, and zero structured releases have
  a causally captured pre-release consensus.
- Preserved the recovered blurb corpus as hypothesis discovery: 2,935 factor
  observations, 2,316 factor responses, 21,413 response-entry outcomes, and
  1,692 representative cases. It remains movement-conditioned and therefore
  cannot estimate live prediction accuracy.
- Built a source-first response diagnostic across all 21 currencies and 68
  pairs. It collapsed the current history into 7,909 deduplicated five-minute
  currency clocks and scored 6,368 clocks/18,725 pair-horizon outcomes using
  the lowest-spread causal pair expression and executable bid/ask paths.
- The basic semantic direction rule failed: after-cost wins were 11.0% at 5m,
  18.6% at 15m, and 29.9% at 60m; mean net results were -2.323, -2.409, and
  -2.380 bps. Gross direction was 46.4%, 47.0%, and 48.1%. A direction-permuted
  control was nearly identical, while 15-minute technical confirmation was
  worse. The failure is both weak direction and movement too small for the
  roughly 2.25-bps average spread.
- The next branch is narrative acceleration/novelty/agreement as a watch and
  magnitude feature, followed by technical timing. Historical rows remain
  discovery-only; the current trusted-clock contract starts the untouched
  prospective cohort. No order, account, lifecycle, or execution setting was
  changed. Focused validation passed 13 tests plus Python compilation.

## 2026-08-19 21:32 America/New_York — Restore the full spike/blurb entry objective

Decision:

- The narrow 41-window direct-source response replay is a diagnostic and does
  not replace the original movement-first news-blurb project.
- Rebuild the complete significant-movement surface across all 68 instruments
  and the retained 1-minute through 30-day horizons.
- Preserve both views: movement-first ex-post attribution and event-first
  causal entry replay.
- A fresh blurb or official event opens a currency watch. Direction may be
  supplied by a synchronized observed currency-factor change rather than being
  forced from prose. The entry timestamp is the first causal detection time,
  not the event time or the hindsight move start.
- Select the lowest-cost liquid pair expressing the detected strong-versus-weak
  currency factor, then measure remaining executable movement, MFE, MAE,
  continuation lifetime, reversal risk, and cost clearance from that entry.
- Learn separate immediate-continuation, pullback/resumption, delayed-break,
  spike/fade, conflict, and no-response mappings by event class and liquidity.
- Restore or explicitly classify the 7,048 legacy significant-move labels whose
  executable timestamps are presently unavailable; never silently discard them.

Safety/evidence disposition:

- Research and shadow only. No execution, authorization, promotion, or
  real-money routing changes.
- Ex-post or late blurbs remain explanatory labels only. Entry features must be
  point-in-time causal and outcomes must use executable bid/ask economics.
- Any selected entry rule requires factor/episode deduplication, cost stress,
  multiplicity control, and untouched prospective confirmation.

## 2026-08-19 21:38 America/New_York — Preserve pure-source factor discovery

Decision:

- Historical movement review must work backward from each material currency
  move to the purest available cause: the original official release,
  central-bank statement, policy action, intervention record, government fact,
  or directly measured market variable. A media article is a discovery pointer
  and timestamped narrative observation, not automatically the causal source.
- Extract the smallest measurable factor that changed: rate action, expected
  policy path, actual/prior/revision, inflation or labour delta, intervention
  amount, reserve or balance-sheet change, yield repricing, commodity shock,
  funding change, or a versioned statement/speech delta.
- Preserve numeric value, units, prior state, expected sign mechanism,
  knowledge time, provenance, confidence, and affected currencies. When no
  numeric factor exists, retain a versioned semantic factor with lower evidence
  grade and explicit uncertainty rather than a generic sentiment score.
- Map that factor to the realized currency-strength response across all pair
  legs at multiple horizons, separating common currency movement from
  pair-specific residuals. Repeated cases form factor-response analogs used to
  estimate onset, magnitude, continuation, reversal, and entryable remainder.
- Historical backward attribution is hypothesis discovery. Live use may read
  only the factor and market observations known at the decision timestamp.

## 2026-08-19 22:43 America/New_York — First reconstruction checkpoint and governed null

Implemented:

- Frozen the all-68, 20-horizon reconstruction contract and audited 2,492,576
  executable M1 rows plus the 7,048-row recovered legacy label inventory.
- Materialized 41,454 q95/q99 movement candidates and 6,465 market
  factor-time episodes without treating hindsight opportunity as predictability.
- Materialized 2,935 pure-source factors across all 21 currencies, retaining
  source/event/story identity, publication and first-seen clocks, causal state,
  relevance state, and explicit numeric-versus-semantic factor grade.
- Linked factors to movement candidates with separate pre-entry, during-move,
  observed-late, and ex-post relation states.

Falsification result:

- Event-first replay V1 appeared positive at long horizons, but concentration
  audit showed repeated source-batch trades, stale first observations, and
  wide-spread HKD/ZAR/exotic expressions created the result. V1 is preserved
  unchanged as a falsification artifact.
- Repaired V2 bars publications more than 15 minutes stale when first seen,
  collapses one currency watch per five-minute source batch, selects a
  synchronized confirming pair, and caps entry spread at five pips.
- V2 retained 226 fresh official watches and 380 evidence units. Every liquid
  arm/horizon was negative after executable costs. A +0.20-pip 120-minute
  moderate-cost point estimate with N=27 is discovery noise, not a candidate.
- Supported execution decision remains `no_trade`; no authorization, promotion,
  executor, account, or real-money setting changed.

Legacy evidence recovery:

- Verified that the OANDA practice candle endpoint still serves exact BAM M1
  history for a 2025 labeled event.
- Added a resumable immutable recovery archive containing normalized raw candle
  payloads, payload hashes, bid/ask entry and exit, spread, MFE/MAE, long and
  short net outcomes, latency, and explicit coverage state.
- The first 20 windows restored exactly. A hidden read-only collector is
  continuing the other 7,028 labels. These remain outcome-selected attribution
  diagnostics and can never self-promote as prospective forecasts.

Recovered-package reconciliation:

- Frozen the user-supplied `FOREX_NEWS_BLURB_RESEARCH_RECOVERED.zip` inside the
  canonical source archive with SHA-256
  `f0b79f7f1f58084573541c428d68b929041d890b7e7f6bdf4db4527b64210903`.
- The package contains eight historical Python programs and one EUR/USD example
  workbook, but not the large generated blurb CSV/JSONL corpus. The old data is
  therefore not silently assumed recovered.
- The all-68 movement generator is superseded by the new executable inventory;
  the detailed blurb collectors remain useful ex-post schema references; the
  old ruleset is barred from execution because it conditions on known moves.
- The useful missing pieces are now explicit pending items: a diverse canonical
  case sampler, an API-free research-chunk export, and governed source research
  for unmatched representative cases.

## 2026-08-19 22:59 America/New_York — Factor analog and representative-case checkpoint

- Built 1,042 immutable all-leg currency-response observations at eight
  horizons for 131 fresh measurable factors. The mean absolute median currency
  response rises from 0.62 bps at one minute to 6.42 bps at 120 minutes.
- The evidence spans only nine currencies and 60 source/factor/relevance
  identities within the exact price overlap. No identity had three earlier
  already-matured nonzero analogs, so the source-specific prequential mapper
  abstained on every row. This is an evidence-coverage result, not a software
  failure and not permission to pool unrelated factors after seeing outcomes.
- Built 1,692 low-overlap representative movement cases across all 68 pairs.
  Forty-nine pairs contain all five horizon classes and every pair has a
  research queue; 293 selected cases currently have a causal pre-entry factor
  link. CSV and Markdown views are API-free and preserve canonical IDs.
- No external model/web batch was launched and no entry rule was created.

## 2026-08-19 23:27 America/New_York — Legacy closure and expanded analog null

Executable recovery and verification:

- Completed the immutable practice-candle recovery for all 7,048 legacy move
  labels: 7,002 exact executable bid/ask windows and 46 partial windows. The
  recovery contract snapshot is
  `7ef8d24b277038930697deddbb37d9a19f66d023c7591edd2b67bccf21142146`.
- The separate verifier independently decompressed every raw payload, checked
  hashes and executable-price arithmetic, and returned 7,048/7,048 verified,
  zero violations, SQLite integrity `ok`; verifier snapshot
  `5f3ca8f5f7291003ea94e399e38d4940be3b893c6b8f78620e1d77e6107f5432`.
- Built the immutable legacy attribution ledger. It contains 1,095 retained
  news matches: 161 potential pre-entry, 177 first-wave confirmation, 734 stale
  context, and 23 post-hoc; 5,953 cases remain unmatched. Only 31 pre-entry
  cases had an explicit legacy direction, with 22.6% descriptive alignment.
  First-wave alignment was 91.8% on 73 mapped cases, but those observations
  occur after movement onset and cannot support an entry rule. Attribution
  snapshot:
  `ca91328d0cf88a902a7c527f48f78dbfd82e602c547abfa6a91c5d22df7ee041`.

Current price and factor-response evidence:

- Appended 296,723 complete OANDA BAM M1 rows across 68/68 instruments with
  zero retrieval errors. The archive now contains 2,789,299 rows; all 68 files
  have complete executable schemas and no duplicate/non-monotonic rows.
- Rebuilt the immutable response analog ledger under a price-coverage-only V2
  cohort: 160 measurable factors, 1,274 horizon responses, 76 exact analog
  keys, and 11 currencies. Mean absolute median response is 0.62 bps at one
  minute and 6.57 bps at 120 minutes.
- The strict prequential mapper again emitted zero predictions: no exact
  source/factor/relevance identity had three earlier, already-matured nonzero
  analogs. This is a repeated-evidence/source-coverage blocker, not permission
  to pool unrelated event types or lower the threshold after seeing outcomes.
  Snapshot:
  `6024893c729e6231d0f7b9e024d64717c64fa3c72c282717a6872f8f3f3ae254`.

Validation and disposition:

- Reconstruction-focused suite: 34 passed. All new modules compile and the
  source-gap/branch/config JSON parses.
- No executor, authorization, promotion, account, or real-money setting was
  changed. The supported decision remains `no_trade`.
- The active next gap is now bounded official-source research for the 5,953
  unmatched cases plus prospective accumulation of repeated measured-driver
  identities across the ten currently uncovered currencies.

Operational closeout:

- Refreshed the move-first news case audit after the upstream census changed;
  it now contains 619 independent cases, is current under the integrity
  contract, and still supports `no_trade`.
- Reran the project integrity audit. Status is `ok` with no current failures;
  executor health, 68-quote retention, and real-money disablement pass. The
  historical shadow-archive discrepancy remains explicitly frozen and its gap
  dates have matching snapshots; no historical evidence was rewritten.
- Storage remains safe at approximately 120.7 GiB free. Practice 007 remains
  flat at NAV/balance $41.6042, with zero positions and zero pending orders.

## 2026-08-19 23:46 America/New_York — Unmatched official-source queue opened

- Materialized all 5,953 unmatched legacy cases as immutable research jobs in
  120 bounded chunks. The queue covers 67/68 instruments and all 21 currency
  legs; 5,912 jobs have exact executable windows and 41 are explicitly partial.
- Every job carries both currencies' configured central-bank authority,
  release/communication/calendar source IDs, and official numeric source
  families. The queue makes zero causal assignments and labels the restored
  best side as hindsight-only.
- Liquidity split: 1,258 jobs <=2 pips, 1,888 at 2-5 pips, 693 at 5-15 pips,
  and 2,114 above 15 pips. Research should begin with liquid/moderate cases and
  retain pair/episode diversity rather than chase the largest exotic pip count.
- Snapshot:
  `d0a69c160aa742d685f79a3ccfa8e874ca46f21b263138cd5498a8850d82e2d2`.
- This begins the active source-repetition phase; it does not change the
  `no_trade` decision or any execution surface.

## 2026-08-20 12:40 America/New_York — Verified source clocks and response reversals

Queue compression and archive reconciliation:

- Collapsed 3,135 exact liquid/moderate unmatched pair jobs into 1,646
  immutable factor/time episodes across 1,337 market clocks. The episode factor
  remains a hindsight correlation-dedup key and is not a causal attribution.
  Queue snapshot:
  `5884c296d38ddaa6247fc9546833d7566427492ba384644d0ab68238c7818699`.
- Reconciled all episodes against a frozen 64,730-article local source view.
  Only 15 late/ex-post candidates survived; 1,631 episodes had no local
  candidate and no causal driver was assigned. Reconciliation snapshot:
  `0b4a2110d1d8a177db19b680c3168a3e882a763f1b61a0d9f82ce09f710ab2c0`.

Original-authority cases:

- Frozen three verified cases with five source documents, 12 measurable
  versioned factors, and 14 movement-episode links. The BOJ page itself reports
  the policy decision at 12:56 JST (03:56 UTC), and its official press page
  identifies the governor's 15:30 JST (06:30 UTC) conference. The next-day
  transcript cannot be treated as fully known at the event clock.
- The BLS employment release is exactly clocked at 12:30 UTC and retains the
  +142,000 payroll actual, 4.2% unemployment rate, +0.4% monthly earnings, and
  -86,000 combined prior-month revision. Causal consensus and contemporaneous
  rates repricing remain absent, so direction abstains.
- The Chinese tariff commission page confirms a 34% additional tariff on all
  U.S.-origin imports but exposes only a publication date. The first retained
  exact secondary witness is AP at 10:35:30 UTC, after the 09:55-10:05 move
  onset. It is an ex-post source candidate, not entry evidence.
- Verified-source snapshot:
  `d752e18bea169fd2fd7cbc974bf596f9cce596d9ae355d5bcf278798d2fbd2b6`.

Executable event-watch replay:

- Reacquired exact all-68 OANDA BAM M1 paths for both exact event watches:
  136/136 windows, SQLite integrity `ok`, zero proof/execution flags. The V1
  attempt committed only an immutable zero-window contract before an optional
  dependency failure; it was not overwritten. Corrected V2 owns a minimal
  practice GET-only candle client that rejects account and order surfaces.
  Price snapshot:
  `1fae0e6fbdf2498f881994182ceeb5baaa4ccf2155bd585975d23148e0ca2288`.
- Four frozen response arms produced eight detections and 32 executable
  bid/ask outcomes. Direction came only from post-event cross-pair response;
  source-assigned direction count is zero. Only the one-minute breadth arm was
  positive on average at five and 15 minutes, with just two retrospective
  events. All other cells were negative; no cell is promotion eligible.
  Replay snapshot:
  `2b962960b9ff0518636576c774d93eb177f1fc06818578835d32cebbbd9bb570`.

Factor-response analog result:

- Stored each event response once at 1/3/5/10/15/30/60 minutes and linked its
  nine exact-source factors using a shared effective-event identity. This
  produced 14 trajectory rows and 63 factor-response links without inflating N.
- Both cases reversed the initial response: the BOJ JPY trajectory first flips
  from weaker to stronger at the 10-minute checkpoint; the BLS USD trajectory
  first flips from weaker to stronger at 60 minutes. There are zero repeated
  exact analog keys and zero prequential predictions. Analog snapshot:
  `bcbdc008075bf8f7d52871e369d3ae76ca2b6135e87b01ea9cb1596dc4df2a19`.

Validation and disposition:

- New focused causality, immutability, executable arithmetic, client-surface,
  event-weight, and replay tests passed. SQLite integrity is `ok` throughout.
- No account, broker execution, authorization, promotion, or real-money state
  changed. Practice 007 remained flat and the supported decision is
  `no_trade`.

## 2026-08-20 13:00 America/New_York — Second official-event cohort and cross-cohort falsification

New original-authority cases:

- Added a separate frozen cohort for the 18 September 2024 FOMC release, the
  14 August 2024 RBNZ release, and the 1 August 2024 Bank of England decision.
  The Federal Reserve statement and projections both self-report an exact
  14:00 EDT release clock; the RBNZ event page states that the OCR and Monetary
  Policy Statement are announced to markets at 14:00 NZT. Both are exact event
  watches. The Bank of England page exposes only a publication date in the
  retained primary document, so that case remains ex-post and cannot open a
  watch.
- The closed V3 source cohort contains three cases, four documents, 14
  measurable factors, and five movement links. It assigns no source direction,
  causal driver, proof, or execution eligibility. Snapshot:
  `b4cea42f61ef4921fa84ebc93b82e2543d5c79d82e04a505d6dc2d648a2d3180`.

Executable response evidence:

- Reacquired 136/136 all-68 price-window records for the two exact watches.
  There are 135 exact windows and one explicit EUR/DKK partial window in the
  RBNZ case; it begins three minutes after the requested pre-event boundary.
  Because EUR/DKK is unrelated to NZD and every direct NZD leg is exact, the
  response replay excludes that one path and records the exclusion rather than
  inferring candles. Price snapshot:
  `58a52b00f59fbbd34059e169816f9525277a310685f6ebe51fc5905b31265501`.
- Both FOMC USD and RBNZ NZD weakened broadly immediately after their release.
  Four frozen timing arms produced eight detections and 32 executable bid/ask
  outcomes. The three-minute persistent arm was positive at 5/15/30 minutes in
  this two-event cohort, but it was materially negative in the first cohort.
  Replay snapshot:
  `9a89cdaa08601f0010a2a2aec111e4362e8c2177e5402ae24e78da684ff1cfcb`.
- Stored 14 new event trajectories and 70 factor links for ten exact source
  factors. Neither new case reversed by 60 minutes, unlike both first-cohort
  cases. There are still zero repeated exact analog keys and zero prequential
  predictions. Analog snapshot:
  `8651bf9e04731440ecd5d1292a95348fd352368e6c41ae3d159bf65af0c80dd0`.

Cross-cohort result:

- The only two point-positive V1 cells, one-minute breadth at 5 and 15 minutes,
  both turned negative in V2 (-10.05 and -1.30 pips respectively). No positive
  timing cell remained positive from V1 to V2. Across the four selected events,
  all initially showed currency weakening and two later reversed, but these
  are movement-selected retrospective cases and therefore cannot confirm a
  generic weakening or timing rule. Comparison snapshot:
  `d8ad66c1834e0087be2b8f9ac7f275dfdbe1b367b4fb1368731350d1918c4ea5`.

Immutable incident handling:

- Source-cases V2 committed valid rows and then hit a copied V1 post-commit
  count assertion. A count-only attempted repair changed the builder bytes, so
  V2 is permanently excluded and documented without deleting or rewriting its
  rows. The fresh V3 contract uses new case/document identities and is the only
  downstream source for this cohort.

Validation and disposition:

- Focused source, price, response, analog, comparison, immutability, and
  executable-arithmetic tests pass. SQLite integrity remains `ok`.
- No broker, account, order, authorization, promotion, or real-money state was
  changed. Supported execution remains `no_trade`.

## 2026-08-20 13:42 America/New_York — Schedule-selected RBNZ response replication

Discovery and negative controls:

- Built an immutable schedule-selected cohort containing all 12 RBNZ policy
  decisions from 28 February 2024 through 20 August 2025. Selection used the
  official calendar and exact 14:00 Pacific/Auckland clock, never subsequent
  movement. It contains 816 all-68 executable price windows, 84 response
  trajectories, and zero execution/proof flags.
- Exact prior-only analogs based on OCR-change magnitude predicted direction
  correctly only 2/21 times. A cut, hold, or hike sign alone is therefore not a
  reliable currency-direction label without causal expectations and statement
  or rate-market repricing.
- Of four frozen response arms and four horizons, the first-complete-M1 breadth
  arm at a fixed 15-minute hold was the strongest discovery cell: +12.483 pips
  after cost over 12 events versus -1.496 pips over 24 same-weekday/same-local-
  clock +/-7-day controls. Exact p=0.000350; Holm-adjusted p=0.005600. Technical
  confirmation delayed entry and was not helpful.

Locked later-period replay:

- Before requesting later paths, froze one candidate only: first complete M1
  response, median absolute NZD strength >=1 bp, breadth >=60%, cheapest
  agreeing direct NZD leg with spread <=5 pips, response-following direction,
  15-minute fixed hold, executable bid/ask, and 0.25-pip modeled slippage.
  The contract includes every later scheduled decision from 8 October 2025
  through 8 July 2026 and two same-clock +/-7-day controls per event.
- Reacquired 162/162 exact direct-NZD M1 windows from the OANDA practice candle
  endpoint. V1 failed closed because one control's selected NZD/SGD path lacked
  one interior minute; it committed zero decision rows and remains preserved.
  V2 froze a gap-tolerant outcome contract before calculation, requiring the
  executable entry and declared exit while labeling MFE/MAE partial for that
  single incomplete path.
- The six later decisions averaged +2.367 pips after cost; controls averaged
  -1.700, for +4.067 incremental pips. Four of six cleared costs, incremental
  effect was positive in four of six, and the minimum leave-one-event-out
  treatment average was +1.290 pips. The exact within-trio p-value was 0.068587,
  so the predeclared 0.05 replication gate did not pass.
- Event results: Oct-2025 cut +6.75 pips, Nov-2025 cut +7.05, Feb-2026 hold
  -10.45, Apr-2026 hold -1.25, May-2026 hold +4.35, Jul-2026 hike +7.75. The
  November cut produced an NZD-stronger response, again showing why OCR action
  sign cannot assign direction.

Validation and disposition:

- New candidate source hashes are frozen (`644aed61564ea91375b537dc3421df7df96058ba5c9e896c0b929506c7877201`
  for V1 and `c2f513a7e9e99f6fd791639cfdd5d0fd2ad2671205d3cf97350fd7082fdc3710`
  for V2). The canonical database passes SQLite integrity and all execution
  eligibility sums are zero.
- Reconstruction/source/pending/vault/storage regression suite: 132 passed;
  project-integrity suite: 21 passed. A pandas 3 timestamp-resolution test
  fixture was made portable; production movement logic was unchanged.
- Disk headroom remains safe at 120.06 GiB free (12.9%). No executor, account,
  authorization, promotion, broker-order, or real-money state changed. The
  supported decision remains `no_trade`; the next valid step is immutable
  prospective accumulation and cross-authority generalization.

## 2026-08-20 14:05 America/New_York — FOMC cross-authority falsification

Frozen design:

- Verified the complete official population of 21 regular FOMC statements
  from 31 January 2024 through 29 July 2026 and their exact 14:00
  America/New_York release clocks. Each event received same-weekday, same-clock
  -7/+7-day controls.
- Before requesting any FOMC price path, froze the one candidate selected from
  RBNZ: first complete M1 USD response, median absolute strength >=1 bp,
  breadth >=60%, cheapest agreeing direct USD leg with spread <=5 pips,
  response-following direction, 15-minute hold, executable bid/ask, and 0.25
  pip modeled slippage. The official rate action remained unsigned because
  contemporaneous consensus and event-time rate repricing are missing.
- V1 module SHA-256 is
  `13792b138409162fa00932772ee442ab0e2302a8e8eb8645d704cb5a71d19430`.
  The canonical contract, 21 event facts, and 63 clocks were committed while
  its price table still contained zero rows.

Availability incident and repair:

- V1 then acquired 1,260/1,260 requested practice-only OANDA BAM M1 windows:
  1,179 exact, 37 partial, and 44 empty. The +7-day control for 18 December
  2024 was 25 December, so all 20 legs were empty; HKD/THB/TRY also caused
  expected thin-cross sparsity. The predeclared all-20-exact gate failed and
  V1 committed zero decisions.
- V2 was frozen after that coverage diagnosis but before any fallback request
  or outcome calculation. It preserved the rule and price observations,
  allowed available direct legs when at least three event candles existed,
  and predeclared same-weekday +14/+21/+28-day fallbacks for the one closed
  control. It fetched 60 fallback windows and selected 8 January 2025, the
  first usable date; selection used only candle presence, never return.
- V2 module SHA-256 is
  `897b086c126b2e3514b3fa07d84aca989baad8bfe5f17a752d1f165580305ffe`.
  It produced 63 immutable decisions and retained all 21 complete matched
  event trios with zero execution/proof eligibility.

Result and interpretation:

- Treatment average was **-4.012 pips after cost**, versus **+0.118** for
  matched controls, for **-4.130 incremental pips**. Only 5/21 event entries
  cleared cost; incremental value was positive in 5/21; its median was -4.250
  pips. The minimum leave-one-event-out treatment average was -6.025 pips and
  the frozen 200,000-draw within-trio randomization p-value was 0.963130.
- Every FOMC event triggered the first-minute detector, demonstrating a broad
  immediate USD response, but blindly following it for 15 minutes lost. The
  largest positive event was 18 December 2024 (+36.25 pips); large failures
  included 7 November 2024 (-21.05), 7 May 2025 (-25.45), and 28 January 2026
  (-20.75). Rate-action sign did not resolve continuation versus reversal.
- The RBNZ point result therefore does not transport as a universal central-
  bank timing rule. The next research unit is authority/event-specific response
  shape plus causal expectations, statement delta, dissent/projections, and
  rate repricing—not a lower gate or a retuned universal hold.

Validation and safety:

- FOMC/RBNZ focused tests: 19 passed before freeze. The explicit 31-file spike/
  blurb regression suite: 126 passed under the bundled scientific runtime.
  The canonical spike database passes SQLite integrity with 1,320 FOMC price
  windows, 63 resolutions, 63 decisions, and zero execution-eligible rows.
- Project integrity status remains `ok`, `can_place_orders=false`, and
  `can_promote=false`. Storage remains safe at 119.834 GiB free (12.872%); no
  delete or vacuum ran. Supported execution remains `no_trade`.

## 2026-08-20 14:15 America/New_York — FOMC response-shape discovery closes with zero seeds

- After the governed FOMC transport failure, froze one adaptive diagnostic
  grid before calculating its outcomes: response at 1/3/5 minutes, follow or
  fade, and 3/5/10/15-minute fixed holds. The 24 cells form one Holm family and
  are explicitly discovery-only because FOMC performance motivated the grid.
- Materialized 1,512 immutable executable outcomes across the 63 selected
  event/control clocks. The first-M1/follow/H15 cell reproduced the frozen V2
  result byte-for-statistic, validating the generalized horizon arithmetic.
- No cell passed all predeclared discovery gates. Best was
  `response_at_5m_persist_3:follow:h3`: treatment +1.164 pips, control -0.594,
  incremental +1.758, minimum leave-one-out treatment +0.410, but only 52.4%
  cost-clearance, raw p=0.094665, and Holm p=1.0. The next positive cells were
  similarly weak and non-significant.
- First-M1 fade/H15 did not rescue the failed rule: +0.050 treatment pips,
  -0.565 controls, +0.615 incremental, 47.6% cost-clearance, raw p=0.408863,
  and Holm p=1.0. Continuation/fade cannot be selected from price response
  alone in this cohort.
- Builder SHA-256 is
  `0f1ee9ba0c927ee0a83db95aa85e6ef4061b82140c7e4fe1a486a98c3b767a31`;
  input snapshot is
  `ef556a67249a6251ea0a3352bd8a3bec5c4b52a92689c69fe8d229faef7c8f21`.
  The 32-file spike/blurb regression suite passes 131 tests, database integrity
  is `ok`, and all shape rows remain research-only and execution-ineligible.
- Disposition: no selected seed, no threshold relaxation, and no route change.
  The next improvement must add source-proximate state—expectations/repricing,
  statement deltas, projection/dissent state, or press-conference timing—or
  gather genuinely untouched prospective events. Supported execution remains
  `no_trade`.

## 2026-08-20 14:48 America/New_York — official FOMC statement factors add no validated seed

- Archived the December 2023 baseline and all 21 regular FOMC statements from
  January 2024 through July 2026 directly from the Federal Reserve, preserving
  raw HTML, normalized text, hashes, exact official release clocks, retrieval
  metadata, and immutable source identities. V1 safely stopped after 20
  documents when the Federal Reserve changed its vote wording; V2 repaired
  only that format transition and resolved 22/22 documents. Source-builder
  hashes are `2394f007e5a8b2e5cbbb792a61a1cfbac2161da919631ad10a86a45125355543`
  and `c79efcd956f4bf27d780c1beb0f0faa343758b6c45ce0ec8cb424a2d79df9be3`.
- Froze a statement-delta vocabulary before response selection: rate action,
  activity, labor, inflation pressure/progress, risk balance, guidance,
  balance-sheet tightness, dissent tilt/count, SEP flag, and statement novelty.
  Directional source agreement and direction-conflicted aggressive fade remain
  separate research arms; contextual magnitude fields cannot vote on direction.
- Preserved factor-response V1 as a diagnostic after its event audit exposed
  bounded phrase-matching errors. V2 changed only the activity/risk/dissent
  parsing rules and rebuilt a separate immutable cohort. Its builder SHA-256 is
  `2009a258a56e29aa5d18f0df4967a44a1b321aad33155f7a998c513f2ef391dc`.
- V2 contains 21 events, 294 factor observations, and 21,168 factor/response
  links. Nine factors supplied enough directional observations for 16 tested
  cells; **zero passed all gates**. The best apparent cell was dissent tilt,
  first-minute response, follow for ten minutes: +4.217 treatment pips versus
  -0.279 controls (+4.496 incremental), but only 50% cleared costs, minimum
  leave-one-out treatment was -2.690 pips, raw p=0.255727, and Holm p=1.0.
- The complete 36-file spike/blurb regression suite passes **154 tests**.
  SQLite integrity is `ok`; all proof/execution flags remain zero. Causal
  consensus and event-time rate repricing remain unavailable, so this is
  retrospective discovery—not forecast proof. Supported execution remains
  `no_trade`.

## 2026-08-20 15:16 America/New_York — exact official FOMC projections are now source-normalized

- Added a source-only archive for the December 2023 Summary of Economic
  Projections baseline and every quarterly SEP through June 2026: **11/11
  official Federal Reserve pages**. Raw HTML, response metadata, exact official
  event clocks, hashes, normalized table-1 medians, and the prior-projection
  comparison embedded in each release are immutable in the canonical ledger.
- Extracted **458 normalized projection values** spanning real GDP growth,
  unemployment, PCE inflation, core PCE inflation, and the projected federal
  funds path. The source-builder SHA-256 is
  `ca3e1a9bf1ac3c00d555ee8e5c691a8d9dda4d1eb69e81e6add03d8be39643a2`;
  the source snapshot is
  `c44e5c2578bd351506bac27440818d741c423bcffc95f6da825f57e0f3bf6932`.
- Preserved two official-source clock-label discrepancies instead of silently
  correcting them: the December 2024 and December 2025 accessible pages label
  their winter 2:00 p.m. releases as `EDT`; the canonical event clocks remain
  the independently established 19:00 UTC clocks and the mismatch is explicit.
- The archive is retrospective, research-only, direction-abstaining, and has
  no price/account/execution imports. It adds exact policy-path and macro
  projection changes, but still does not provide pre-release market consensus
  or event-time traded-rate repricing. The complete spike/blurb suite passes
  **159 tests** and SQLite integrity remains `ok`; supported execution is
  `no_trade`.

## 2026-08-20 15:23 America/New_York — official FOMC projection factors are linked to executable responses

- Froze a separate factor-response cohort over the ten comparable SEP events
  from March 2024 through June 2026. The December 2023 release remains the
  baseline only. The cohort contains **10 source events, 110 factor
  observations, and 7,920 immutable links** to the already-frozen FOMC
  event/control bid/ask response paths.
- The source vocabulary was fixed before outcome calculation: current-, next-
  year, longer-run and curve-slope policy-path revisions; GDP, unemployment,
  PCE and core-PCE projection revisions; plus magnitude-only revision context.
  Magnitude/context fields cannot cast a directional vote. Source-confirmed
  follow and source-conflicted fade remain separate discovery arms.
- Sixty cells had at least five usable events. **Zero cells passed every
  gate.** The strongest point estimate was an unemployment-projection revision
  aligned with the 3-minute persistent response and held 15 minutes: +14.310
  treatment pips versus -0.650 controls (+14.960 incremental), 5/5 cost clear,
  and positive leave-one-out means. Its raw randomization p-value was 0.004170,
  but Holm-adjusted p was 0.232198, so it is not a validated seed.
- The next-year policy-rate path aligned with the 5-minute persistent response
  and held five minutes was also positive (+11.550 treatment, +11.390
  incremental, 5/5 cost clear) but likewise failed multiplicity control
  (Holm p=0.232198). These are hypotheses for later untouched events, not
  permission to select, retune, or execute them.
- Builder SHA-256 is
  `ded34d6daefec15682f8c12334820e664ac5930c2a0d002fd01b5930f3187d76`;
  input snapshot is
  `316cdf4f45ab42afef816be9209b7cf2ec68a51c9779c99bda4d962cec58c53e`.
  The complete spike/blurb suite passes **164 tests** and SQLite integrity is
  `ok`. All source, factor, proof, promotion, authorization, and execution
  surfaces remain research-only or false; supported execution is `no_trade`.

## 2026-08-21 07:10 America/New_York — Practice 006 becomes a 21-currency rank challenger

- Removed the active Practice-006 identity as a riskier/looser copy of the
  news-plus-technical Practice-007 concept. The old news challenger code and
  historical evidence remain preserved, but the always-on supervisor no longer
  launches it and the dashboard no longer labels or reads it as the live 006
  strategy.
- Added a pure rank model over the exact **21-currency / 68-pair** executable
  universe. Each currency receives a robust standardized score from 5-minute
  (50%), 15-minute (35%), and 60-minute (15%) factor strength. A pair is
  eligible only when one leg is positive on both 5m and 15m, the other is
  negative on both, the real pair reaction agrees on both horizons, and its
  movement proxy clears 1.2x stressed current spread. These values describe
  the fixed Practice-006 experiment; they are not a validated production edge.
- Candidate allocation is factor-deduplicated: a greedy strongest-versus-
  weakest selection cannot reuse either currency, including currencies already
  reserved by any open account trade. Re-entry uses one attempt per continuous
  thesis and resets only after the qualifying rank thesis actually disappears;
  no arbitrary post-exit timer was added.
- Kept hard execution boundaries: OANDA practice endpoint, exact account suffix
  `-006`, fixed 100 units, exact model/lane/universe binding at the last order
  hook, maximum eight positions, maximum 48 daily fills, $1 session-loss stop,
  live quote/slippage gates, broker stop/trailing/time protection, and no
  real-money route. Practice 007 and its governed lifecycle are unchanged.
- Rewired the hidden supervisor, state/heartbeat, SQLite ledger, JSONL log, and
  dashboard. The live panel now shows strongest/weakest currency tables,
  disjoint selected pairs, 5-minute reaction, cost-cover ratio, positions,
  fills, exits, errors, and rejection reasons.
- The first live minutes exposed and repaired one operational logic defect:
  current entry-cost clearance had also been used as rank validity, causing a
  trade to close when the rolling five-minute move stopped clearing spread even
  though its currency ordering had not necessarily reversed. Entry and exit
  contracts are now separate. Cost/reaction checks govern entry; only a true
  strong-versus-weak score reversal (or reversal on both 5m and 15m), broker
  protection, or the one-hour horizon governs exit. Source loss cannot itself
  close a trade or reset its thesis generation.
- Validation: Python compilation and PowerShell parsing passed; **71 focused
  tests passed**. The live heartbeat reports `running`, 21 currencies, 68
  instruments, zero errors, and SQLite integrity `ok`. The first frozen-model
  decision opened a protected 100-unit Practice-006 **USD/JPY long** (USD ranked
  above JPY); no discretionary/manual order or close was used. After the exit
  repair, the worker remained stable beyond the former one-minute churn point
  with two factor-disjoint protected positions (USD/HUF short and GBP/HKD
  long). These early practice outcomes are execution observations, not proof of
  positive edge.

## 2026-08-21 08:58 America/New_York — Ten previously uncovered factor-move cases

- Audited and permanently logged ten unique unresolved movement episodes from
  the immutable unmatched-episode queue. None duplicates the existing verified
  external-source case ledger. The representative paths span August 2024 to
  June 2025 and include two payroll releases, Jackson Hole, a BOJ decision and
  press phase, the U.S. election sequence, the euro-area PMI cascade, two
  Canada tariff sequences, a tariff-delay report, and the Israel-Iran shock.
- The ten representative after-cost paths ranged from **+72.9 to +225.9 pips**
  and each had breadth across four to ten pairs. Breadth is now explicitly
  retained as factor confirmation but counted as one signed-currency market
  episode, never four to ten independent wins.
- The central defect exposed by the batch was causal-clock ownership. Only two
  episodes had an exact retained source clock before the selected interval;
  seven first appeared during it and one only after it. Four profitable
  intervals began 5–75 minutes before the proposed source. The source may
  explain a release/continuation leg, but cannot be credited with the entire
  hindsight-selected path.
- Added the research-only `uncovered_factor_case_batch_v1_20260821` compiler,
  frozen ten-case configuration, detailed JSON/Markdown report, and append-only
  JSONL case log. It fails closed on duplicate episode IDs, already-covered
  source episodes, invalid/unverified clocks represented as exact, altered
  upstream contracts, execution flags, and log-content collisions. Re-running
  the compiler is idempotent: the case log remains exactly ten rows.
- General logic upgrades are now explicit contracts: split hindsight intervals
  from event entries; gate source clocks by quality; model multi-phase and
  progressive events; deduplicate release cascades and factor breadth; require
  response confirmation before assigning semantic direction; and represent
  geopolitical shocks first as cross-currency risk/haven factors.
- The existing reconstruction archive preserves executable endpoints, selected
  side, spread, after-cost move, and factor breadth, but not a complete
  pre-event 21-currency minute snapshot for every old episode. The audit records
  that limitation instead of manufacturing a rank replay. Missing causal
  consensus and contemporaneous rate repricing also remain explicit evidence
  gaps.
- Validation: Python compilation and JSON parsing passed; the focused compiler
  suite passed **5 tests** and the compatibility set for the unmatched queue,
  verified source cases, and new batch passed **15 tests**. SQLite integrity is
  `ok`. The only warning was inability to write pytest's optional cache in the
  repository. The artifacts are research-only, execution-ineligible, and
  support `no_trade`; Practice 006 and Practice 007 were not changed and no
  order or close was submitted.

## 2026-08-21 10:16 America/New_York — Ten current-week move cases and archive repair

- Replaced the prior historical interpretation with the exact requested scope:
  ten distinct movement-first cases from **17–21 August 2026**. The frozen
  report is in
  `data/oanda_training_manager/reports/major_move_case_audits/ten_current_week_move_cases_v1/`.
  It uses executable bid/ask outcomes, first-seen source clocks, pre-event-only
  5/60-minute momentum, and contemporaneous cross-pair currency-factor breadth.
- Nine of ten cases had a nearby official or secondary source. Only **3/9**
  source hypotheses aligned with the observed side, **6/9** opposed it, and one
  GBP/AUD move remained honestly unexplained. This is a source-direction and
  response-timing problem, not mainly a feed-count problem. The operational
  conclusion remains `no_trade`.
- The week's cleanest source-response miss was the 19 August Treasury buyback:
  the official page was caught in 108 seconds and carried a research-only
  USD-negative score, while USD/JPY fell 64.15 midpoint pips in 15 minutes.
  The missing component was a consumer that bound the official source to
  price/yield confirmation and ranked a watch; capture itself was present.
- Repaired a concrete semantic/hindsight defect exposed by the 17 August JPY
  case. Explicit `currency stronger/weaker` recaps now preserve the stated
  currency sign and are marked as observed market context, not new forecasts.
  The move-first audit's success label now requires a timely,
  directionally-publishable source, so research-only and retrospective recaps
  cannot be credited as causal forecast wins.
- Found that the durable 68-pair M1 archive had stopped extending while the
  short rolling quote buffer continued. Recovered **132,737** missing rows over
  68/68 pairs with zero request errors, then added the updater to hidden
  supervision on an hourly loop with atomic reports. The first supervised pass
  appended another **1,463** rows across 68/68 with zero errors. The hidden
  supervisor was restarted without opening a foreground window or touching
  positions.
- Added focused tests for explicit adjective semantics, strict causal labeling,
  the ten-case compiler, atomic archive reports, and supervised worker
  structure. Validation passed: **405** news/case compatibility tests, **3**
  updater tests, and **16** supervisor/structure/isolation tests. The optional
  pytest cache remains unwritable but does not affect test execution.
- The general all-history move audit exceeded its five-minute diagnostic budget
  and was terminated without output replacement. The exact weekly compiler is
  complete; optimizing the larger audit is now a documented performance task.
  No discretionary order or close was submitted. Practice 007 remains flat;
  Practice 006's rank challenger was also flat at the final snapshot.

## 2026-08-21 12:49 America/New_York — Current-week official-event response replay and parser repairs

- Expanded the current-week investigation from ten selected cases to every
  substantive high-quality official item found for 17–21 August, replayed
  against the exact 68-pair executable M1 panel at fixed 5/15/60/120-minute
  horizons. Forty deduplicated official source items collapse to **24**
  five-minute currency/event clocks. Selecting the best pair and horizon after
  every clock found a cost-clearing move in all 24 cases, demonstrating why
  that unrestricted hindsight statistic is not evidence of predictability.
- Added a stricter research attribution rule: the source must be observed from
  five seconds before through 300 seconds after the event clock, the named
  currency must rank in the strongest/weakest five, its factor move must be at
  least 3 bps and dominate the other pair leg, and the representative path must
  clear at least five pips after executable spread. **Nine** event/move
  correspondences remain. They are hypothesis-generation cases only; none has
  a causally defensible source direction because contemporaneous consensus and
  rate-market repricing are absent.
- Examples retained for follow-up include CAD weakness around Employment
  Insurance, USD weakness after FOMC minutes, NZD weakness after business-price
  indexes, JPY weakness after national CPI, and CAD/GBP responses around retail
  and labor publications. The Swedish decision calendar and China LPR calendar
  were removed from strict credit: a pre-known schedule is not the observed
  release, and the Riksbank content arrived too late for the first move.
- Fixed two concrete official-page contamination defects. Oman foreign-policy
  pages are now parsed only from the current headline/lead rather than old
  related-story conflict text. Treasury duration/buyback classification is now
  bounded to the current release lead, so a sanctions page cannot inherit an
  older buyback article from site furniture. Exact adversarial fixtures were
  added for both bugs.
- Fixed canonical pip sizing in the current-week compiler, rejected out-of-week
  calendar rows that were first discovered this week, separated calendar-only
  clocks from releases, collapsed simultaneous currency items into one market
  episode, and split low-cost from wider-spread representative paths.
- Validation passed: **406 focused/compatibility tests**, plus Python compile.
  The news collector was restarted hidden onto the repaired classifier and
  reports a running research-only cycle. C: retains about 150 GiB free and D:
  about 2.35 TiB free. No discretionary order or close was submitted; no
  execution, authorization, promotion, or real-money gate was changed.

## 2026-08-21 14:18 America/New_York — Two-hour wrong-direction and additional-move audit

- Rebuilt the ten selected current-week cases and the complete current-week
  official-event replay under current source clocks. The selected set remains
  **3 aligned, 6 opposed, and 1 without a source**. The official replay retains
  40 source items, 24 event clocks, **5** strict timing/movement
  correspondences, and **0** resolved source directions. The earlier count of
  nine strict official correspondences was corrected.
- Added 23 additional movement-first factor cases to the durable report: 13
  selected/live episodes and ten mechanically selected independent 15/60m
  peaks from the synchronized 68-pair M1 panel. Representative after-cost
  paths include CHF/ZAR +722.1 pips, USD/ZAR +652.6, EUR/SEK +336.0,
  EUR/NOK +238.9, and USD/HUF +76.7. These are factor episodes, not independent
  pair wins or executable forecasts.
- The added cases show why nearby-source directions were wrong: the source
  often arrived after the move; the article reported the already-completed FX,
  oil, bond, or equity reaction; a shared USD/CHF/ZAR/NOK factor was mislabeled
  as pair-specific; surprise and rate repricing were unavailable; or competing
  risk and commodity channels made the headline sign indeterminate.
- Repaired five diagnostic/classification paths: pair-ticker price extremes are
  retrospective; activity-release identity outranks incidental central-bank
  wording; oil-blockade effects abstain without observed commodity response;
  contradictory blockade paraphrases cannot corroborate one another; and
  crude-supply headlines that explicitly report prices surging remain
  retrospective context.
- Repaired three additional causal-boundary defects exposed by the larger
  sample: syndicated “traders bracing for hawkish/dovish policy” stories remain
  research-only expectations state; expiring ceasefires are not risk-on; and a
  mixed threat-plus-tentative-deal headline abstains instead of flattening the
  word “deal” into de-escalation. Recent article reclassification now
  reconciles stale topic identities immediately.
- A fresh live recap then exposed one more research-label defect: “New Zealand
  Dollar extends rally as Fed rate-hike bets fade” had marked both NZD and USD
  negative. Multi-word `extends rally/gains/advance` and
  `extends slide/losses/decline` forms now preserve the explicitly observed
  currency sign while remaining retrospective and non-publishable.
- Froze classifier `local_fx_news_rules_20260821_v129` and collector contract
  `local_news_incremental_source_commit_v57_20260821`. The hidden collector
  restarted on the new contract. The complete focused/compatibility suite
  passed **434 tests**; the only warning is the pre-existing unwritable optional
  pytest cache. No execution, authorization, promotion, position, or real-money
  setting changed. Supported Practice-007 action remains `no_trade`.

### 2026-08-21 14:40 ET — five-minute burst extension

- Audited the week's mechanically largest exact five-minute currency-factor
  bursts. A 12:32–12:37 UTC JPY rally opposed the nearest oil-recap mapping;
  the recap was already describing market prices and could not own the onset.
- Found a valid official AUD direction hit at the Australian labor release.
  The feed first observed “Unemployment rate rises to 4.5%” at 01:31:31 UTC,
  but AUD/NZD had already fallen about 21 pips. After the 01:32:31 causal hold,
  only about 2.8 executable pips remained before a two-minute reversal. This
  separates semantic accuracy from entry value.
- Corrected a retrospective parser inversion: “EUR/USD breakout puts dollar
  pressure ...” now assigns EUR positive/USD negative instead of attaching
  `pressure` to the pair rate. Added the mirrored USD/JPY/yen adversarial case.
- Froze classifier `local_fx_news_rules_20260821_v131` and collector
  `local_news_incremental_source_commit_v59_20260821`. All **438** focused and
  compatibility tests pass. No execution or account policy changed.
- Extended pair-grammar coverage to named slash pairs. “Dollar/Yen falls” and
  “Thai baht/US dollar stronger” now resolve base/quote identity before the
  movement verb; their old USD-positive research labels were inverted.
- A topic-deduplicated 229-observation sign diagnostic found that retrospective
  recaps aligned with the next five minutes only 48.2% of the time and with the
  next 15 minutes 43.9%. Secondary context was 61.2%/52.4%, but median signed
  movement was only +0.20/+0.09 bps. The 12 official observations were
  58.3%/75.0% with sub-cost median magnitude. Zero observations was
  publishable; these are diagnostic signs, not after-cost evidence.
- The final live sample found CAD weakest over one hour and GBP/CAD about
  +14.1 gross pips. No timely official Canadian event explained it; the nearby
  trade-talk headline contained no completed deal and oil recaps pointed the
  opposite way. It was logged unresolved, with the 15-minute leg already
  slowing below normal executable cost.
- The later CAD trace showed a distinct GBP/CAD step around 18:34–18:36.
  Secondary beef-tariff/Canada-friction stories were timestamped near 18:33 but
  were not first seen locally until 18:46, after the peak. They were retained
  as late hypotheses, never assigned causal or execution credit.

## 2026-08-24 23:47 America/New_York — Persistent official-factor mover audit

- Exhaustively traced the current USD/ZAR drop to the direct-source ledger.
  The Treasury listing did capture the two official Iran-sanctions pages at
  17:42 and 18:06 UTC; the apparent source miss came from examining only the
  narrow active-context subset. Historical event versions preserve an initial
  wrong `labor_release` label and the later governed geopolitical correction.
- The corrected factor still projected USD/ZAR long while the later 22:58 UTC
  move was down. Recorded this as a semantic-to-market mapping miss rather
  than changing the headline sign or claiming causal attribution.
- Added and deployed the inert supervised
  `live_move_persistent_news_context_v2_direct_authority_20260825` diagnostic.
  It retains still-active source factors for their frozen declared horizon,
  deduplicates story identities, and reports all-source, official-publisher,
  and direct-authority-leg alignment separately from the narrow causal vote.
- The first live snapshot retained 78 raw USD/ZAR versions / 73 independent
  stories and exposed the official long/opposed result. Focused mover and
  integrity suites passed 40 tests. No broker, authorization, promotion,
  account, or real-money switch changed; Practice 007 remained flat.

## 2026-08-24 23:55 America/New_York — SARB direct RSS transport repair

- Audited every non-operational news source. RBNZ, China Customs, and Trading
  Economics remain governed external/access blockers; retired replacements
  remain deliberately disabled. The active repository defect was SARB's
  official RSS connection resetting under Windows/SChannel.
- Added a source-specific `--ssl-revoke-best-effort` curl option. Certificate
  and hostname validation remain active; no insecure TLS option exists.
  Rolled the exact SARB source identity to V6 and the collector to V72.
- Repeated isolated probes returned HTTP 200, 19,096 bytes, and 25 official
  items. The source/news/integrity compatibility suite passed 486 tests. The
  current V71 collection cycle was left uninterrupted; V72 activation waits
  for that cycle's safe completion. No trading or account switch changed.

## 2026-08-25 00:20 America/New_York — SARB integrated retry follow-up

- Activated collector V72 only after the exact V71 worker published
  `cycle_complete`. Its first integrated SARB attempt retained the V6 source
  identity but encountered an independent intermittent connection reset.
- Preserved that failed attempt and created source cohort V7. V7 retains
  certificate/hostname validation and revocation-best-effort, while adding two
  bounded `--retry-all-errors` attempts for this source only.
- In three live transport probes, the first recovered from a reset on retry
  and the next two succeeded without retry; every successful response was
  19,096 bytes and parsed to 25 official RSS items. The source, authority-map,
  governance, and integrity suite passed 485 tests; compile and JSON checks
  passed. V73 activation waits for the active V72 cycle's clean boundary.
- The independent evidence and move-first audit freshness alerts observed
  during this pass both self-cleared after their running recomputations. No
  evidence mismatch, order, account, promotion, or real-money switch changed.

## 2026-08-25 00:29 America/New_York — Prospective official response horizon extension

- The refreshed move-first discovery audit still showed no general directional
  edge, but its small official-only 120-minute subset was the strongest
  falsifiable lead: 9/12 overall and 6/7 on liquid majors. These are correlated,
  movement-conditioned discovery observations, not proof.
- Froze and deployed response-watch V3 as a new append-only cohort with exact
  executable 1/5/15/30/60/120-minute outcomes. The V2 database and artifacts
  were preserved; no historical observation was reclassified.
- Project integrity now requires the exact V3 schema, contract, cohort, mapper,
  classifier, horizon set, inert policy, SQLite integrity, and snapshot/database
  count parity. The V3 worker is live under hidden supervision with zero
  prospective watches and zero outcomes.
- The focused response/integrity suite passed 34 tests; compilation and JSON
  validation passed. No execution, authorization, promotion, account, or
  real-money setting changed.

## 2026-08-26 12:48 America/New_York — Automatic news hit/miss improvement loop

- Added the inert `news_outcome_improvement_audit_v2_20260826` worker. Every
  five minutes it independently reads matured prospective news decisions and
  mover outcomes, reduces correlated pair expressions to event/factor theses,
  splits results by executable-cost bucket, records immutable diagnoses, and
  refreshes the research-only improvement queue.
- Preserved the V1 implementation preflight and opened V2 after freezing the
  stable source fingerprint and horizon-level effective-evidence rules. Two
  identical V2 runs inserted 2,886 then zero diagnoses and one then zero
  snapshots; SQLite validation passed.
- The first current audit classifies 891 wins and 1,819 misses/gaps. It exposes
  systematic no-edge-after-cost, wrong-direction/entry, strict-signal absence,
  mapping conflict, stale-context, and giveback/reversal problems without
  treating ten correlated pair legs as ten independent proofs.
- Repaired an accepted-ledger leak: a contextual event must now have its own
  `forward_pair_eligible` direction and cannot hitchhike on a pair-level score.
  Expanded factor aliases so current inflation/labour category names cannot
  evade currency-factor deduplication.
- Repaired the UK inflation-expectations case prospectively. The current-state
  rise is no longer overwritten by the phrase “after recent falls”; because
  the source is secondary and event-time rate repricing is absent, the result
  is retained as a research score and is not publish-eligible.
- Integrated the audit with the hidden supervisor, continuous validation,
  project integrity, and the existing `event_response_archetypes` control
  branch. The final focused compatibility suite passed **525 tests**; compilation
  passed. C: retained **129.36 GiB** free and D: **2,290.73 GiB** free.
- Reload validation found an official-response worker pinned to the prior V142
  classifier literal. It now imports the canonical classifier contract. The
  official mapper also now performs its intended append-only remap on a new
  classifier version, filters current candidates by exact version/contract,
  and reports current mappings separately from retained historical mappings;
  historical versions can no longer double the apparent evidence count.
- Added the automatic audit ledger, compact state, queue, report, source, and
  focused tests to both full and news-only credential-free vault checkpoints.
- Completed the post-change vault sync at 17:17:38 UTC: 1,288 files and
  663,016,886 bytes were checkpointed, including refreshed canonical project
  records and the automatic-audit artifacts. The checkpoint and pointer hashes
  were regenerated successfully.
- Removed an outcome-worker scaling failure that caused false staleness. The
  worker now skips immutable completed case/arm/horizon paths and binary-searches
  indexed candle clocks for pending paths. Its first supervised optimized
  cycle inserted 109 outcomes, skipped 13,034 completed paths, used roughly
  66 MiB instead of roughly 800 MiB, and completed in seconds. Project
  integrity then returned `ok` with zero failures.
- No broker, order, lifecycle-promotion, authorization, account, or real-money
  boundary changed. Practice 007 remains governed and the supported action is
  `no_trade` unless a separately confirmed candidate receives an exact canary.
- The V143 classifier migration temporarily made the last completed news view
  exceed the consumer freshness limit, so signal/news correctly hard-blocked.
  The worker remained live and made heartbeat progress throughout; its bounded
  5,000-row batch completed at 17:21:50 UTC, the fresh pair view published at
  17:21:47 UTC, and the hard block cleared automatically. No freshness or
  execution gate was loosened.

## 2026-08-26 14:38 America/New_York — News review changed to canonical on-demand record

- Removed `news_outcome_improvement_audit` from always-on supervision and from
  continuous runtime-health expectations. The CLI no longer accepts interval
  or duration arguments, so it cannot be turned back into a polling worker by
  accident.
- Preserved the V2 diagnosis ledger, queue, report, and project history. An
  explicit review appends only new deterministic diagnoses; unchanged evidence
  keeps the same immutable snapshot and canonical `recorded_utc`.
- Added exact JSON-to-SQLite snapshot binding and append-only-trigger checks.
  Record age is informational rather than a health failure; corrupt, unsafe,
  future-dated, or database-mismatched records still fail closed.
- Reloaded the hidden supervisor, stopped the old 300-second audit process tree,
  and generated one final on-demand checkpoint: 3,010 current effective theses,
  930 diagnostic wins, 1,903 misses/gaps, and 19 retained research proposals.
- Focused tests passed and project integrity returned `ok`; Practice 007 and
  all execution gates were unchanged.

## 2026-08-27 00:06 America/New_York — Reboot recovery, native MNB repair, and term-factor clock

- Determined that the full safe core stopped at 20:52 on 26 August because
  Windows rebooted at 20:53, not because an individual worker crashed. Restarted
  the exact canonical safe core hidden at 23:45 and verified one supervisor,
  fresh Practice-007 quotes, fresh worker heartbeats, and a flat practice account.
- Installed and validated the hidden current-user `ForexSafeCoreAtLogon` task.
  It invokes the canonical `start_oanda_safe_core.ps1`, ignores duplicate starts,
  and its test run returned result 0 without disturbing supervisor PID 5212.
- Repaired explicit official-policy semantics. English and Hungarian MNB rate-cut
  language now maps only HUF weaker; foreign-policy and oil references inside an
  official statement no longer become direct currency legs. Added the native
  Hungarian MNB decision listing as a direct official source while retaining the
  English source. The classification contract is now
  `local_fx_news_rules_20260827_v144_native_policy_action`.
- Added append-only prospective `narrative_term_factor_clock` and
  `narrative_term_factor_contribution` tables to the continuous narrative meter.
  The first sealed clock contains exactly 21 currencies, 20 explicit zero states,
  and five bounded research-only CZK/CNB terms. Updates/deletes are trigger-blocked.
- Preserved source publication/first-seen/retrieval/revision/supersession lineage
  through the major-move diagnostic and compact live context surfaces.
- The combined focused news, meter, official-source, fast-lane, and move-diagnostic
  suite passed **476 tests**. SQLite quick checks passed. No order, promotion,
  authorization, account, or real-money setting changed.
- Frozen the initial week-to-date movement audit at
  `data/oanda_training_manager/reports/WEEK_TO_DATE_EVENT_MOVE_AUDIT_20260827.md`:
  496 factor-deduplicated cost-clearing episodes, 207 broad-context alignments,
  209 oppositions, and zero strict publishable pre-move news wins. The HUF rate
  cut is the cleanest research hit and the concrete source-timing repair case.

## 2026-08-27 01:18 America/New_York — Live week audit extension and fail-closed transition

- Reconstructed the complete frozen material set, not only its top ten: 23
  independent signed-factor episodes reached 15 bps. After correcting the MNB
  rate-action polarity, broad nearby context was aligned for eight, opposed for
  eleven, neutral/conflicted for two, and absent for two. Strict point-in-time
  publishable direction and execution eligibility were both zero of 23.
- Added the complete 23-event tail and the post-freeze BOJ/JPY chronology to
  `data/oanda_training_manager/reports/WEEK_TO_DATE_EVENT_MOVE_AUDIT_20260827.md`.
  The BOJ speech was hawkish in text, but JPY was already weakening before its
  publication and all four completed-M1 JPY expressions opposed an automatic
  long-JPY interpretation. The late PDF extraction is retained as observation
  evidence and cannot be backdated.
- The prospective term/factor meter has continued sealing five-minute clocks
  for exactly 21 currencies. Late or nonpublishable CNB, BOJ, USD-media, and
  geopolitical terms retain their raw research observations but contribute
  zero effective directional weight. The current all-currency meter remains
  neutral rather than converting retrospective text into a live vote.
- A new post-freeze HUF- move crossed the material threshold around 00:21-01:10
  ET: EUR/HUF rose about 15.4 bps (+25.2 executable pips) and USD/HUF rose about
  14.3 bps (+15.0 pips). A timely secondary tanker/Hormuz headline and technical
  continuation were present, but there was no strict verified HUF catalyst.
  The simultaneous legs exposed a possible signed-factor-primary deduplication
  defect and were sent to a read-only factor audit before being counted as two
  independent events.
- The integrity sentinel correctly entered `degraded/no_trade` while disk code
  and running news workers held different classifier/source contracts. No
  execution gate was loosened. The collector, fast lane, mapper, response watch,
  watchlist, and integrity worker will be restarted together only after the
  release-clock/policy-state patch is finalized and focused tests pass.

## 2026-08-27 02:46 America/New_York — Live HUF-strength reversal case

- The corrected causal all-68 surface grouped simultaneous USD/HUF and EUR/HUF
  declines as one **HUF+** factor move using 67/68 completed, knowledge-time-safe
  observations. From 02:35-02:43 ET, USD/HUF fell 12.633 bps (39.25 gross pips,
  19.60 after recorded cost) and EUR/HUF fell 12.387 bps (44.85 gross pips,
  24.40 after recorded cost).
- There was no strict forward/publishable source story. The broad USD/HUF news
  context leaned the wrong way and EUR/HUF context was neutral; the earlier MNB
  rate cut is a week-level HUF-negative backdrop and cannot be retrofitted as an
  explanation for this HUF-strength reversal.
- The pre-move technical-continuation shadow arm was short both pair expressions
  and therefore aligned. This is one prospective diagnostic hit, not independent
  proof or an authorization. Practice 007 remained flat and no order was placed.
- The move then reversed violently: by the 02:50 completed minute, USD/HUF had
  risen about 78.1 pips and EUR/HUF about 85.05 pips over five minutes, erasing
  and overshooting the prior decline. The case is therefore also an exit/holding-
  horizon and giveback warning. A correct segment direction is not a tradable win
  unless the frozen entry and exit policy captures it after cost.

## 2026-08-27 04:12 America/New_York — Causal narrative clock and mover-join cutover

- Replaced the mutable latest-state narrative input with prospective V12 sealed
  five-minute clocks. A state now becomes proof-readable only after bucket close
  plus 60 seconds; the unsealed live view is separate, incomplete, unpersisted,
  and execution-ineligible. V12 has exact 21-currency/68-pair coverage and its
  current direct integrity check is `ok` with no late-arrival or seal-gap events.
- Found that V5 could attach a post-start narrative state to an earlier mover.
  The initial V6 preflight also caught an integer-truncation boundary at a
  sub-second seal. Preserved the ten engineering rows and opened V6R2 at 08:05Z.
  It requires both full-precision meter close and seal timestamps to precede the
  mover start; exact clock/seal/contract provenance is retained per case.
- Activated separate V2R2 forward outcomes and V3R2 persistent context. The old
  V5/V1/V2 diagnostic workers were stopped and frozen. The quote stream and
  Practice-007 executor PIDs were untouched; account 007 stayed flat at
  balance/NAV $41.6042 with zero open trades and zero pending orders.
- Corrected the nominal current-week official-response report, whose defaults
  were still bound to 17–21 August. The report now derives Monday 00:00 UTC to
  generation time and supports explicit reproducible historical windows. The
  refreshed current-week sample contains 23 official items / 18 independent
  clocks, 15 post-hoc cost-clearing responses, three strict timing/movement
  correspondences, and no direction-resolved strict candidate.
- Audited the European-open rotation. USD strengthened across 18/19 exact legs
  from 03:03–03:35 ET while HUF, PLN, NZD, NOK, and SEK led weakness; no fresh
  official numeric or policy release occurred at onset. USD/HUF and EUR/HUF
  later exceeded +50 bps before retracing about 18–20 bps in ten minutes;
  USD/PLN and EUR/PLN similarly gave back about 37–43 pips. This is one
  factor/liquidity episode and an entry/exit/giveback diagnostic, not many wins.
- The focused V12/V6R2/integrity/dashboard/current-week suite passed 107 tests
  with only the known optional pytest-cache permission warning. Supported
  execution remains `no_trade`; no order, lifecycle, authorization, or
  real-money switch changed.

## 2026-08-27 09:33 America/New_York — Live-window close, exact events, and bounded storage

- Closed the requested watcher exactly at 09:00 ET and froze the full
  Monday-through-cutoff recap: 34 official items / 23 independent event clocks,
  907 signed-factor episodes, 31 episodes >=15 bps, and zero strict pre-move
  directional news matches. Practice 007 was flat at $41.6042 with no orders.
- Audited Stats SA PPI, ECB accounts, and the simultaneous Census/DOL 08:30
  bundle at declared fixed horizons. Census first appeared on the independent
  official probe at +0.551s and DOL at +1.156s; production DOL was +10.505s,
  while the missing Census production path was +351.816s through the broad
  collector. Census now has a prospective 15-second statistical fast lane;
  today's rows remain audit/bootstrap evidence and were not backdated.
- Separated central-bank policy and statistical-release source roles. Policy
  transports remain 21/21 currencies and 68/68 pair legs operational; four
  statistical transports are mapped and operational. Added date-scoped ECB
  polling, repaired live source signature/parser defects, and fixed a bounded
  concurrent-heartbeat integrity race.
- Fixed the WTD compiler's long-run cutoff race by selecting bounded append-only
  account/source records and V12 buckets sealed before cutoff. The U.S. case is
  inventoried only as diagnostic evidence with explicit no-direction/no-win
  semantics. Final report hashes are verified.
- Removed the edge evidence worker's long read transaction. Immutable highwater
  pagination released the strategy WAL; a normal checkpoint reduced it from
  1.44 GB to ~32 MB without deleting or vacuuming evidence. Storage returned to
  `ok` with 130.709 GiB free and a 160.957-day projection to the 50-GiB guard.
- Final combined focused validation: 558 passed. Lifecycle, practice accounting,
  genealogy, position ledger, and canonical trial databases returned `ok`.
  Supported execution remains `no_trade`; no discretionary entry/exit,
  authorization, promotion, threshold loosening, or real-money route occurred.

## 2026-08-27 11:11 America/New_York — Source-depth and causal response maps

- Added a 21-currency × eight-event-family readiness map that separates
  transport, live health, exact-family parsing, numeric extraction, prospective
  causal observation, matured response, and semantic direction. Current depth
  is 168/168 configured and operational cells, 163 healthy, 43 parsed, 13 with
  numeric output, 14 with prospective inputs, and zero with prospective matured
  causal outcomes.
- Added a new append-only, research-only official source-factor response ledger.
  It keeps neutral semantic events, freezes actual factor knowledge clocks,
  takes <=15-second exact bid/ask entry snapshots for future events, derives a
  constrained all-pair currency response, and stores both executable paths at
  1/5/15/30/60/120 minutes with intrabar MFE/MAE and cost-clear timing.
- Closed six independent-review defects before deployment: later-known factor
  leakage, currency/release effective-N inflation, post-horizon entry capture,
  cross-host mirror duplication, consensus self-certification, and read-write
  input access. A second review then found multi-document policy-episode
  inflation; the final contract collapses a decision, projections,
  implementation note, and press conference into one underlying episode while
  preserving separately scheduled decisions, minutes, speeches, and unrelated
  releases.
- Preserved the superseded engineering census under explicit
  `PREVALIDATION_20260827T143754Z` filenames. The clean V1 cohort activates at
  2026-08-27 17:00 UTC. Its current pre-activation diagnostics contain 11
  events / 11 episodes, 91 factors, 65 response rows, 402 low-support
  abstentions, 144 ineligible diagnostic forecasts, and zero prospective proof
  events or forecasts.
- Added an exact source-specific policy fact framework and the first SARB/ZAR
  adapter. A valid fact must contain action, current/prior rate, signed decision
  delta, structured vote objects, and causal clocks; incomplete rows abstain.
  Three future SARB dates are retained, with zero attempts/facts before the next
  eligible 23 September decision. Complete exact policy tuples remain 0/21.
- Wired all three diagnostics into the hidden supervisor and started them in
  the background without restarting the broader live stack. The final combined
  focused suite passed 162 tests; PowerShell parsing and new-ledger SQLite
  integrity checks passed. No account, lifecycle, authorization, threshold,
  promotion, watchlist, order, or real-money setting changed.
- The storage guard remained `ok` with 128.244 GiB free after the artifacts and
  checkpoint were written. Canonical pending/log records were synchronized to
  the shared Forex vault with matching SHA-256 hashes.
- A fresh full-project integrity pass completed `ok`: all 68 current quotes,
  the executor, sealed narrative clock, and storage check passed; supported
  execution remained `no_trade` with no failures.

## 2026-08-27 14:43 America/New_York — Level-collision diagnostic and U.S. source clocks

- Completed a 68-pair causal level-reaction census over 3,302,722 executable
  M1 rows. Every pooled unconditional bounce and break cell was negative after
  costs. Two superficially positive USD/JPY means failed best-day concentration
  checks and remain discovery artifacts, not edge.
- Froze the diagnostic-only level-approach ledger as
  `causal_level_approach_ledger_v1_frozen_20260827b`. It uses only causally
  confirmed pivots and coverage-valid prior weeks, applies spread/pip/ATR-scaled
  level zones and symmetric first-passage barriers, preserves both executable
  bid/ask paths, censors same-bar ordering as `ambiguous_intrabar`, and rejects
  noncontiguous horizon clocks. The first USD/JPY replay contains 3,868
  approaches and 23,208 outcomes across 5/15/30/60/120/240 minutes, all marked
  `historical_diagnostic`; SQLite integrity is `ok`.
- Added BEA RSS to the official statistical fast lane with 90-second normal and
  15-second 08:25–08:45 ET burst polling, exact source clocks, and bounded BEA
  blurb extraction. The active lane completes 29/29 configured transports with
  zero errors. Its 48 initially observed BEA items are explicitly bootstrap and
  zero are prospective.
- Added the Kansas City Fed release listing as a direct USD policy/event source
  and froze official Jackson Hole clocks for the 27 August 20:00 ET full-agenda
  publication and 28 August 10:00 ET Chair remarks. Calendar rows remain
  research-only and directionless.
- Closed the HTML-listing causal-history defect in repository contract V74.
  A separate persistent `bootstrap_item_urls` quarantine now survives rolling
  known-URL truncation, restarts, database rebuilds, and conservative legacy
  migration. After the prior 186/186-source V73 cycle closed, only the broad
  collector was reloaded under the existing hidden supervisor. A full V74 cycle
  completed at 14:50:29 ET: Kansas City Fed remained HTTP 200 with 28/28
  inherited URLs quarantined as bootstrap, both Jackson Hole clocks remained
  healthy, and the new stderr log was empty. V74 cannot backdate any existing
  catalog item; Practice 007, the quote stream, executor, and official fast lane
  were not restarted.
- Validation passed 24 causal-level tests, 32 official-fast-lane/coverage tests,
  and five focused BEA/Kansas-City/bootstrap-quarantine tests. No broker,
  execution, Practice-007, lifecycle, authorization, promotion, threshold, or
  real-money setting changed. The still-open research gates are a true
  pre-outcome prospective level collector, causal consensus/rate repricing, and
  untouched confirmation across independent episodes.

## 2026-08-27 16:34 America/New_York — Adaptive causal level-band collector

- Implemented persistent, versioned support/resistance band geometry under
  `level_band_contract_v2_frozen_20260827a` and opened the first engineering
  cohort `level_band_prospective_20260827a.eb1a014d25fa52dd`. The observer
  freezes causal band versions, approach speed/acceleration, path quality,
  spread-scaled barriers, and both bounce/break counterfactual arms before a
  future M5 entry can exist; entry, outcome, and censor rows append later.
- Added the worker to hidden supervision and added an adaptive-band dashboard
  panel. The panel reports physical geometry and descriptive cohort evidence,
  explicitly states that response probabilities and calibrated edge are not
  available, and cannot select a pair or side.
- The first cycle had ready context for 65/68 instruments. EUR/TRY, TRY/JPY,
  and USD/TRY remained explicitly blocked by stale M1 context instead of being
  reconstructed or carried forward. It recorded seven forecasts and zero
  matured outcomes.
- Focused geometry, prospective-collector, dashboard, and research-isolation
  validation passed 69 tests. The worker has no credentials, broker,
  signal-feed, lifecycle, authorization, promotion, order, or position surface;
  Practice 007 and real-money routing were unchanged.
- Implementation is complete, but proof is not. Effective independent matured
  outcomes, after-cost bounce/break comparisons against no-trade, concentration
  stress, untouched prospective confirmation, and any future lifecycle/canary
  promotion remain pending. No level-band candidate is currently confirmed.

## 2026-08-27 16:38 America/New_York — Sequential all-68 handoff correction

- Froze `level_band_prospective_20260827a.eb1a014d25fa52dd` as the superseded
  first engineering cohort with seven forecasts and zero entries/outcomes. Its
  rows are not merged into prospective proof.
- Corrected the sequential all-68 handoff: the collector now consumes the last
  published complete update-report cutoff and ignores or validates newer CSV
  rows from an in-progress pair-by-pair refresh. This removes transient
  `report_csv_cutoff_mismatch` failures without constructing a mixed-cutoff
  market panel.
- Opened the corrected current collection cohort
  `level_band_prospective_20260827a.acd30b3829e982f4`. Its first completed cycle
  had 65/68 ready contexts, with EUR/TRY, TRY/JPY, and USD/TRY blocked for stale
  M1 context, 11 forecasts, and zero entries/outcomes.
- The expanded focused geometry/collector/dashboard/isolation suite passed 70
  tests. Research-only and no-execution policy is unchanged. Effective
  independent outcomes, untouched confirmation, and any future promotion
  remain pending.

## 2026-08-27 16:42 America/New_York — Memory-bounded level-band cohort

- Froze `level_band_prospective_20260827a.acd30b3829e982f4` as a second
  superseded engineering cohort with no promotion. Its sequential all-68 cutoff
  fix remains the active data-handoff rule, but the implementation retained
  roughly 800 MiB of resident history.
- Opened the current live cohort
  `level_band_prospective_20260827a.d5df939213cb50a8`. Band construction still
  reads the complete 18,000-row source tail per pair, then retains only 720 M1
  and 600 M5 rows per pair for ongoing processing. Resident worker memory fell
  to roughly 94 MiB without shortening the causal geometry lookback.
- Its first completed cycle at 20:39:46Z had 65/68 ready contexts, with
  EUR/TRY, TRY/JPY, and USD/TRY explicitly blocked for stale M1 context. It
  displayed 24 valid bands, retained seven prospective forecasts, and had zero
  entries or outcomes.
- The focused geometry/collector/dashboard/isolation suite passed 71 tests.
  Research-only/no-execution policy is unchanged. Effective independent
  outcomes, after-cost comparison with no-trade, untouched confirmation, and
  any future promotion remain pending; neither superseded cohort contributes
  promotion evidence.

## 2026-08-27 20:00 America/New_York — Level-band maturity repair and clean pip cohort

- At 20:58Z the continuous research worker reached its first outcome maturity
  and stopped on SQLite `OperationalError: 14 values for 13 columns` in
  `band_outcomes`. The insert now names all 13 destination columns explicitly,
  and a regression test exercises the first-maturity write.
- A bounded compatibility recovery wrote 46 exact 15-minute outcomes and 215
  missing-path censors. These records and all operational-source-hash cohorts
  predating the clean pip contract are engineering-only and permanently
  non-promotable; they are excluded from edge and proof calculations.
- Replaced the static JPY pip heuristic, which mis-scaled nonstandard HUF, THB,
  and HKD/JPY instruments, with the audited `oanda_instrument_pips` fallback and
  the exact live-quote `pip` when available. The corrected geometry contract is
  frozen as `level_band_contract_v2_frozen_20260827b`.
- Opened clean prospective cohort
  `level_band_prospective_20260827a.9294e3511aae455f`. Its first completed cycle
  had 64/68 ready contexts, 25 forecasts, and zero entries/outcomes. The focused
  repair suite passed 30 tests.
- The continuous hidden worker is running. The older supervisor definition has
  not yet adopted the repaired worker/contract and requires a controlled reload
  followed by exact cohort/contract heartbeat verification; this remains an
  operational pending item.
- No broker, Practice-007, lifecycle, authorization, promotion, order, or
  real-money setting changed. Collection remains research-only. Clean effective
  independent outcomes, after-cost comparison with no-trade, untouched
  confirmation, and any future promotion remain pending.

## 2026-08-27 20:03 America/New_York — Hidden-supervisor adoption verified

- Reloaded only the hidden supervisor after a completed loop; all existing
  child workers were preserved.
- The replacement supervisor is the sole active supervisor and adopted the
  repaired level-band worker without starting a duplicate. It reports the same
  worker PIDs `25756,24704`, `running=true`, `started=false`, and a fresh worker
  heartbeat. Reload stderr is empty.
- The verified worker heartbeat remains on clean cohort
  `level_band_prospective_20260827a.9294e3511aae455f`, geometry contract
  `level_band_contract_v2_frozen_20260827b`, with research-only policy and no
  authorization, promotion, signal-feed, broker, or order surface.

## 2026-08-27 21:38 America/New_York — Runtime, history, and vault cleanup closeout

- Retired ineffective producers are now explicit runtime policy rather than
  ambiguous dormant files. HGB and the frozen manager-decision ledger are
  disabled; the H1 family stream and executable-opportunity ranker are
  supervised in `mature_only` and cannot publish new predictions.
- Reloaded the H1 drain worker after adding a terminal four-minute idle interval.
  It restarted under the sole hidden supervisor with `--mature-only`, zero new
  forecasts, zero session errors, and 1,924 unresolved historical horizons.
  The executable-opportunity drain had 3,213 unresolved horizons and zero new
  forecasts.
- Unavailable order/position-book and pricing-depth inputs are inert, zero-output
  model-gap contributors are dormant, August 3 combination rules fail closed,
  and the empty allocator proof is paused until a lifecycle-confirmed candidate
  exists. The clean level-band collector and four governed proof families remain
  immutable research cohorts; none is authorized to trade.
- Consolidated duplicate account polling into one broker request with two atomic
  compatibility outputs. Practice 007 remained flat at balance/NAV 41.6042,
  cumulative operational P/L -8.3430, zero open trades, and zero pending orders.
- Refreshed the model/feature registry, backtest registry, structure audit,
  163-file SHA-256 historical-case index, source-gap register, project log,
  canonical pending queue, and Shared Brain Forex status/index.
- Organized 18,704 expired derived runtime artifacts (394,242,063 bytes) into a
  recoverable manifested archive. No causal forecast, outcome, source event,
  proof cohort, database, WAL/SHM file, credential, or code was deleted.
- Final H1 retirement regression: 10 passed; compilation passed. The prior
  combined cleanup verification had 57 focused tests passing, both relevant
  ledgers passed SQLite `quick_check`, and the structure migration guard was
  `ok`.
- C-drive capacity remained safe at roughly 122.4 GiB free. The final vault sync
  includes the current canonical records and Shared Brain metadata; its external
  `CHECKPOINT_LATEST.json` is the authoritative content/archive hash pointer.

## 2026-08-29 11:47 America/New_York — Causal consensus V2 boundary

- Corrected the prospective consensus clock from request start to full-response
  completion. A response completed at or after release is now rejected even if
  its request began before release.
- Opened the material V2 source/cohort contract. Causal use now requires a
  trusted clock, exact release timestamp, actual absent at capture, verified
  source, provider event/version provenance, and an immutable payload hash.
  The downstream surprise ledger independently recomputes every gate.
- Refreshed the access audit without storing credentials: Trading Economics is
  missing a credential; the configured Finnhub key returns HTTP 403 because the
  Economic Calendar is premium; Alpha Vantage and FRED/ALFRED are not consensus
  sources. No permitted free provider or V2 causal row currently exists.
- Focused consensus/access/surprise verification passed 19 tests plus compile.
  Research remains shadow-only, execution-ineligible, and `no_trade`.
- Acceptance hardening then bound the producer and importer to the exact
  `2026-08-29T16:05:00Z` deployment activation and current source, cohort,
  response-capture, and observation-clock identities. Provider Calendar ID and
  LastUpdate are mandatory; request start cannot follow response/capture;
  source update cannot follow capture; SHA-256 must be lowercase hexadecimal.
  The surprise importer now verifies the archived response bytes and matching
  raw event fields before accepting a projection. Final focused verification:
  26 tests passed plus compile; current causal V2 rows remain zero.

## 2026-08-29 11:47 America/New_York — Rates/RBNZ causal readiness contract

- Preserved the hash-pinned V1 disconnected placeholder byte-for-byte and added
  material contract `rates_policy_repricing_shadow_v2_20260829` alongside it.
  The new shadow collector writes
  append-only SQLite observations with exact source, retrieval, and collector
  clocks; configured HMAC clock attestation; verified content-addressed raw
  archives sourced only from an explicit per-source intake root with
  traversal/outside/symlink rejection; immutable revisions; source-cohort
  identity; duplicate protection;
  and cutoff-causal replay with exact 15/60-minute alignment. Replay,
  readiness, and report counts are restricted to exact configured cohort,
  source-contract, source, and instrument IDs. Missing windows remain
  unavailable rather than being zero-filled.
- The default contract intentionally has zero connected causal intraday
  sources. It cannot place orders, promote evidence, or infer direction, and
  its supported execution decision is `no_trade`. Fifteen focused contract,
  knowledge-time, independently attested-clock, raw-archive, active-contract,
  future-import, duplicate, append-only, and fail-closed tests passed plus
  compilation.
- Audited the official RBNZ path against its current publication guidance. OCR
  decisions are released at 2pm NZT through the website, social media,
  Bloomberg/Refinitiv, and a free official email subscription. The bounded
  collector still receives HTTP 403 from the direct policy/B2 page family, so
  it did not bypass publisher controls. The free email path is registered but
  remains disconnected pending user subscription and a permitted authenticated
  mailbox connector carrying actual receipt and first-seen clocks.
- Registered RBNZ B2 accurately as one-business-day-lag daily H4/H24 context.
  It contains useful OCR, bill, bond, and swap information but is not intraday
  event-window repricing and is not credited as causal confirmation. A
  permitted timestamp-safe OIS/swap/policy-futures source remains external.

## 2026-08-29 22:05 America/New_York — Corrected all-68 deliberate-practice line

- Added a deterministic learner curriculum above the sealed four-pair
  sequential portfolio session. Cohort
  `sequential_portfolio_curriculum_v1.af435480002f2140c541` contains 48
  precommitted attempts: 36 distinct training cases and 12 spaced reviews that
  have zero evidence/repetition weight. The independent verifier passed.
- Added a read-only mistake curriculum over exact primary and depth-one
  feedback. Report `sprmistakecurriculum_00d7d59c24772d6805b382a80ef0`
  reduced 34 nonexclusive observations to 21 structural clusters; cost,
  entry, direction, and calibration are the leading practice priorities.
- Built exact-window pack
  `sequential_replay_source_pack_v1.1bddc89d33ec30767c96` across all 68 pairs
  and homogeneous Monday/Wednesday/Friday 12:00–16:00 UTC blocks. It contains
  204 content-addressed archives, 144 scheduled clocks, 9,792 pair contexts,
  and 8,333 fully ready contexts. Missing context, delayed-entry, feedback, and
  source minutes remain explicit rather than filled or filtered.
- Hardened the pack after independent adversarial review: link/reparse
  rejection now occurs before resolution; compressed and expanded archive
  bytes are bounded; exact session-by-instrument Cartesian identity and unique
  archive paths are required; aggregate coverage is independently rebuilt;
  out-of-window append-only source growth reuses the sealed slice; and the
  Friday schedule mismatch opened a new pack instead of rewriting history.
  Seventeen tests passed with one native Windows symlink privilege skip, and
  the independent verifier returned zero failures.
- Preserved pack `52259f...` and its all-68 replay `ea3dd...` as explicitly
  nonhomogeneous engineering diagnostics. They are not current overlap proof.
- Ran the corrected availability-aware all-68 sequential replay as cohort
  `sequential_all68_portfolio_batch_replay_v1.445ddc96477b7ac284ed`: 144
  decisions, 9,792 retained contexts, 261 ranked candidates, 110 execution
  legs, 172 depth-one alternatives, and a flat terminal portfolio. The frozen
  policy lost **120.25 pips** after executable spreads and fixed slippage.
  AUD/USD and EUR/USD were positive, but USD/JPY, GBP/USD, GBP/JPY, and
  USD/CAD dominated losses. The result was retained and no gate was loosened.
- The source pack and all-68 batch were independently reconstructed with zero
  failures. The combined curriculum, source-pack, genealogy, and isolation
  regression passed 66 tests with one platform-permission skip before the
  final genealogy refresh.
- Operational state remained unchanged: the hidden SafeCoreOnly supervisor is
  running, Practice 007 is flat at NAV 41.6042 with zero open trades and zero
  pending orders, and no discretionary/manual or real-money action occurred.
- Storage remains healthy: C: has 178.09 GiB free and the new sequential
  artifact trees use about 24.8 MiB.

## 2026-08-29 22:50 America/New_York — Sequential verifier boundaries hardened

- An adversarial review showed that older current receipts could trust
  mutually editable summaries or omit datasets while remaining internally
  self-consistent. No affected artifact had been promoted or executed. The
  current research cohorts were rebuilt instead of relabeling old evidence.
- Opened exact-window source pack
  `sequential_replay_source_pack_v1.ee6d6fd4d38744ecc1da`; its material and
  independent verifier now bind the full no-execution safety state. Coverage
  remains 68 instruments, 144 global clocks, 9,792 pair contexts, and 8,333
  fully ready contexts.
- Opened all-68 replay cohort
  `sequential_all68_portfolio_batch_replay_v1.7681e61bd31ac8877bd8`. The
  standalone verifier now reconstructs the complete schedule/Cartesian set,
  candidates, state chain, exact bid/ask legs, feedback, counterfactuals,
  terminals, dataset identities/order, safety, and aggregate economics. The
  honest result remains flat and **-120.25 pips** across 110 legs.
- Opened learner cohort
  `sequential_portfolio_curriculum_v1.22124b7c2ebf24ad2430`; its verifier now
  recomputes all statistics, roots, seal/snapshot payloads, safety, and the
  deterministic content-bound timestamp. It retained 48 attempts, 36 distinct
  cases, 12 zero-weight reviews, and 0.671875-pip mean regret.
- Opened four-pair mistake cohort
  `sequential_portfolio_mistake_curriculum_v1.188f47cfdf79c6608cde`. Its new
  standalone verifier rebuilt 34 nonexclusive labels and 21 structural
  clusters and rejected forged roots, counts, IDs, safety, thresholds, and
  configs. Earlier fixed-name and intermediate artifacts remain preserved.
- Corrected the operational isolation sentinel to name the actual
  `sequential_all68_portfolio_batch_replay` module. The hardened source,
  all-68, curriculum, and isolation suite passed 63 tests with one Windows
  symlink-permission skip; the mistake suite added 17 passing tests.
- A fresh project-integrity pass found one stale diagnostic dependency: the
  move-first news audit predated its latest major-move census. Its focused 18
  tests passed, all 924 retained cases were rebuilt under `no_trade`, and the
  repeated project-integrity audit returned `ok` with zero failures and zero
  confirmed candidates.
- Same-window policy diagnostics showed that cost-aware abstention reduced but
  did not reverse the loss: 1.25x, 1.50x, and 2.00x expected-move/cost hurdles
  produced -34.35, -13.35, and -7.50 pips, while no-trade produced 0.00. The
  V1 switch rule also exposed that an incumbent below the entry threshold has
  no explicit continuation estimate. V1 remains immutable; a separate V2
  hold-versus-switch contract is specified for later untouched evaluation.
- Candidate-source credential audit passed across 1,190 files with zero bearer
  secret findings. C: remained safe at roughly 178 GiB free. Practice 007 was
  current and flat at NAV 41.6042, with no positions or pending orders; no
  manual order, authorization change, or real-money action occurred.

## 2026-08-30 00:58 America/New_York — Deterministic chain, lossless genealogy, and policy expansion closed

- Closed a cross-runtime reproducibility defect discovered before checkpoint:
  Python 3.12 and 3.13 emitted different gzip platform-header bytes even when
  the decompressed evidence payload was identical. Added one canonical gzip
  writer with `mtime=0`, no filename, and normalized `OS=255`; both runtimes
  now emit the same 51-byte frozen fixture with SHA-256
  `225761bb2a033f28effe940697c9b4252d34e396c4313a80d06fda2fa11ec4f0`.
  All 702 gzip files in the seven current canonical chain roots conform.
- Rebuilt rather than relabeled the current immutable identities. The base
  chain is source pack `b9d1526f3dfa057bd06d`, replay
  `efda27295d5107241262`, all-68 mistake curriculum
  `26b13486af3803244125`, and policy challenger
  `0b5267c7a5c730a150cf`. The Wednesday chain is source pack
  `554c8f74212202aa9b86`, replay `e7de4ecdd0255eb13306`, and policy expansion
  `15a3aa5d039df7df9a7d`. Every predecessor remains byte-preserved.
- The base replay still lost **120.25 pips** across 110 exact bid/ask execution
  legs. The challenger preserved no-trade at 0.00, V1 at -120.25, 2x cost at
  -7.50, explicit hold/switch at -7.60, signed-factor suppression at -7.60,
  and unavailable out-of-fold calibration at 0.00. No diagnostic arm proved
  edge.
- The larger Wednesday replay lost **186.20 pips** across 268 legs. Its frozen
  expansion showed +1.35 pips for the 2x arm and +16.10 for explicit/factor
  arms only in pooled inspected history. Excluding the best Wednesday changed
  them to -37.80 and -36.85 pips respectively. The result is concentrated
  historical training evidence, not a discovery pass, confirmation,
  promotion, authorization, or execution route.
- Migrated the live genealogy without discarding incompatible history. The
  exact predecessor registry—53,148 definitions and 123,820 observations—is
  preserved under snapshot `research_genealogy_predecessor_v1.a5d9b45584888df78f8d`.
  Compatible predecessor records were merged; conflicting definitions and
  dependent observations were quarantined with roots and counts. The
  supervised canonical reload finished at 53,169 definitions and 87,476
  observations, registered each rebuilt identity once, passed SQLite
  integrity and foreign-key checks, and retained zero confirmed candidates.
- The final focused boundary passed **208 tests with three Windows
  symlink-privilege skips under each of Python 3.12 and 3.13**. The protected
  junction/reparse fallback passed. All 36 changed Python files compiled under
  both runtimes, config/current-ID checks had zero mismatches, diff/whitespace
  checks were clean, and the credential audit found zero bearer-secret
  findings across 1,213 candidate files.
- Live state remained fail-closed: project integrity `ok`, 68 retained weekend
  quotes, supported decision `no_trade`, and Practice 007 flat at balance/NAV
  41.6042 with no positions or pending orders. Storage remained healthy at
  about 174.6 GiB free. No discretionary/manual order, close, authorization
  change, gate relaxation, or real-money action occurred.
- The reviewed Git commit and source/model vault checkpoints follow this
  source record. Their immutable manifests carry the exact commit, tree,
  archive, content, CRC, and credential-audit identities; no retention deletion
  is authorized.

## 2026-08-30 03:14 America/New_York — Event-to-executable-quote horizon proof implemented

- Added the separate prospective-only, append-only contract
  `official_event_quote_horizon_capture_v1_all68_append_only_20260830` and
  cohort `official_event_quote_horizon_capture_v1_20260830a`. It accepts only
  exact 68/68 entry sidecars from the frozen raw official-event cohort and
  makes one terminal executable bid/ask attempt at 1, 5, 15, 30, and 60
  minutes. Entry quotes are never reacquired.
- Each attempt retains quote-read start and read-completion clocks. Long paths
  use entry ask to horizon bid; short paths use entry bid to horizon ask.
  Spreads are embedded exactly once in those executable endpoints and modeled
  slippage remains separate. Partial, stale, late, future-skewed, or
  wrong-generation attempts retain an immutable invalid header with zero quote
  components and can never be repaired by a retry.
- The collector is research-only, promotion- and authorization-ineligible,
  cannot execute, and remains fixed to `no_trade`. A standalone independent
  verifier implementation is present to rebuild source linkage, expected due
  horizons, hashes, clocks, exact universe coverage, arithmetic, invalid
  zero-component behavior, safety, and trigger presence without importing the
  producer. This entry records implementation, not a passing validation claim.
- Activation is `2026-08-30T12:00:00Z`. Zero eligible events and zero captures
  before activation during the closed market are expected; no historical event
  or Friday regression fixture is backfilled into the proof cohort.
- Marked the three recurring HTTP-403 RBNZ surfaces—OCR snapshot, wholesale
  interest rates, and official overseas reserves—as
  `runtime_supported=false`. Existing parsers, historical rows, and provenance
  remain preserved. Future direct collection requires a publisher-permitted
  subscription, authenticated channel, or explicit access rather than a
  polling bypass.
- Causal pre-release consensus and timestamp-safe intraday OIS/rates or
  policy-futures repricing remain external source blockers. Their absence does
  not weaken fail-closed behavior and is not filled by inference or backfill.
- Final validation passed: 99 focused changed-boundary tests under Python 3.12,
  34 producer/verifier tests under both Python 3.12 and 3.13, and 548 broader
  source/governance/integrity tests. The live empty ledger verifies with zero
  failures; the project integrity audit remains `ok`. These results validate
  implementation behavior, not predictive edge.
- The collector and independent verifier are now running as hidden research
  workers. Their fresh heartbeats report `ok` and `verified`; the verifier
  reruns every 30 seconds and detects due-but-omitted horizons independently.

## 2026-08-30 13:52 America/New_York — Paired official-event proof frozen before deployment

- Implemented prospective-only, append-only contract
  `official_event_paired_evaluator_v1_append_only_20260830` and cohort
  `official_event_paired_evaluator_v1_20260830a`. Every eligible post-
  activation raw event must receive an outcome-blind decision or explicit
  terminal invalid/abstention by T0+55 seconds, before its one-minute outcome.
  The full five-arm, five-horizon, three-slippage schedule is precommitted.
- The five arms are issuer-bound official direction, price-only timing,
  official plus technical confirmation, flipped official control and
  no-trade. One pair is selected without outcomes by minimum event-T0
  executable spread, then lexicographic tie break. Every arm shares the same
  event, pair, entry quote, horizon and cost assumptions.
- Froze config SHA-256
  `e22f3b7af29c3290b8ab35c8a170a1012d6bbb4af32f5e15c713e557dedbad5b`,
  normalized producer SHA-256
  `4f606d644daafb8a3a737555fbf04cbfac56597e15c0bedaf63bcbbc9493dbc1`,
  the exact raw collector/source lineages, V151 classification, authority and
  news-source configurations, upstream producer hashes and all timing limits.
  The initial freeze was rejected before launch when adversarial review found
  classification-version mixing, stale pre-lock clocks, trusted latency
  labels, mutable dependency limits and insufficient horizon backfill checks.
  V2 rederives and binds each of those inputs.
- Added standalone verifier SHA-256
  `aa33bb898846a7e2916ff7f43e7f8e77fc539a2b4a99e2bec2a213fcd34beda9`.
  It imports no paired producer and reconstructs config/manifest identities,
  source and mapping lineage, entry and horizon clocks, issuer-only scope,
  deterministic pair choice, causal completed-M1 technical state, complete
  arm/outcome grids, executable endpoint arithmetic, dependence keys and
  append-only triggers. Honest terminal-invalid evidence remains verifiable;
  forged or omitted material fails.
- Final paired plus horizon boundary passed 86 tests under Python 3.12 and
  3.13. The broader source/governance/integrity/isolation/supervisor/vault/
  credential boundary passed 235 tests under each runtime. All changed Python
  modules compile under both runtimes and PowerShell supervisor parsing is
  clean. These are implementation results, not predictive evidence.
- Added explicit, audited source-vault retention. Default behavior still
  preserves everything. Retention requires a positive explicit count and
  validates exact direct-child immutable archive/manifest pairs, Git/tree/
  hash/size/CRC metadata and the current pointer/mirror before any unlink.
  Legacy cleanup is a second explicit flag, recognizes only four exact naming
  families, rejects links/unknown names and records every deleted file's hash
  and size. Its preflight identifies 13 obsolete files totaling 20,039,246
  bytes; no deletion occurred before the clean committed checkpoint.
- Updated the SARB source register from the stale pending-V73 description to
  the active V7 bounded-retry transport under collector V74. TLS and hostname
  validation remain required; there is no insecure fallback.
- Bounded residuals remain explicit: T0 is a counterfactual attribution clock,
  the final precommit sample cannot prove commit completion through an extreme
  storage stall, and later inference must cluster cross-issuer global shocks.
  None relaxes the execution boundary. Practice 007 remained flat, and no
  manual/discretionary or real-money action occurred during implementation.

## 2026-08-30 14:05 America/New_York — Paired proof activated; vault retention verified

- Committed the governed paired proof as
  `ee40350eb2b58ff7eb3eec3a0be0909b044802a2` after a staged credential audit
  scanned 1,225 files with zero findings. A controlled hidden reload replaced
  only supervisor PID 25332 with PID 19512 and adopted all existing workers.
  It started only the paired producer and its independent verifier.
- The producer heartbeat is fresh and `ok`; the verifier is fresh,
  `verified=true`, and has zero failures. All three upstream databases pass
  `quick_check`. The output database passes `integrity_check` and contains one
  frozen cohort manifest, all ten append-only triggers, and zero evidence rows.
  This is the correct no-backfill state while the market is closed.
- The final two-runtime release subset passed 116 tests under Python 3.12 and
  116 under Python 3.13. Independent release review found no blocking
  correctness, security, or worker-ownership defect. The paired launcher/base
  interpreter process pairs are one logical worker each, not duplicates.
- Published the clean source commit to the OneDrive vault as
  `forex_source_ee40350eb2b58ff7.zip`, SHA-256
  `e08d6cf998f2c2a9dd96efec1c76c4627abb21001ece09c87e83ff4a4de61e08`,
  with 1,225 tracked files and verified ZIP CRC. The audited retention pass
  kept the newest immutable pair, retired 12 superseded pairs, and removed 13
  exact obsolete legacy artifacts totaling 20,039,246 bytes with tombstones.
- Refreshed the model vault through the explicit OneDrive destination only.
  The 1,475-file archive has content SHA-256
  `a3c4c8bbeb342c504cd73b4003a85f4eff16454ff22e3e39a18eaa7c8649271b`
  and archive SHA-256
  `9e2d73dc490aa3badfa6a5ddcba5041c8b1651a7e3a522d2f082d402a8c5d5b3`.
  An independent `forex_vault_import.py --verify-only` rebuilt its manifest
  accounting and returned `verified`; it started zero account processes.
  No `D:` destination was used.
- Shared-vault usage fell from 5,202,258,971 to 4,982,249,381 bytes, below the
  5,000,000,000-byte ceiling. Practice 007 remained flat and no execution,
  authorization, promotion, manual close, or real-money action occurred.
- A late independent retention audit caught that the model sync's exact command
  was OneDrive-only but its omitted-destination default still named the stale
  `D:` vault, and that model retention preceded a current reconstruction
  receipt. The default is now OneDrive only. Current archives must pass full
  CRC/member/hash/path/manifest validation and temporary reconstruction, and
  publish a hash-bound validation receipt before any explicit retention.
  Unchanged syncs do not prune; clean-import claims against an older archive
  are reported as `stale_not_current_archive`.
- Source managed-pair and legacy cleanup now share one durable transaction.
  Exact filename/size/SHA-256 tombstones, the planned pointer and verified
  readback precede cross-family re-inventory, re-hashing and final-report
  serialization; only then can one coordinator unlink while journaling
  progress. Fault-injection tests cover manifest and legacy drift, tombstone
  publication/readback failure, final-report failure and target mutation, and
  prove zero deletion before the complete preflight passes. The final source,
  model, importer and pending-reconciliation vault suite passed 48 tests under
  Python 3.12 and 48 under 3.13.
- A fresh full project-integrity audit returned `ok`: 50 checks, zero failures,
  `weekend_closed`, `no_trade`, `can_place_orders=false`,
  `can_promote=false`, and real-money routing disabled. Destructive retention
  remains explicit and journaled rather than crash-atomic; a hostile external
  path replacement in the final preflight-to-unlink interval remains a narrow
  documented filesystem limit, not an execution or evidence-authority path.

## 2026-08-30 15:08 America/New_York — Verifier heartbeat failure isolated; cohort B frozen

- The live paired verifier recorded a genuine Windows transient publication
  failure: `os.replace` returned access denied while replacing its heartbeat
  JSON. The supervisor restarted the verifier and the append-only ledger was
  unaffected, but the incident proved the publisher needed the same bounded
  Windows retry contract as the surrounding supervised state readers.
- Stopped only supervisor PID 19512 and the paired producer/verifier process
  trees before changing frozen source. All unrelated hidden children were
  left running for later adoption. Producer and verifier now retry the same
  atomic replace up to eight times with bounded exponential delay, preserve
  the last good destination on terminal failure and always remove the
  temporary file.
- Preserved cohort A and its original database without editing its manifest:
  one manifest and zero decisions, arms, horizon inputs or outcomes,
  `integrity_check=ok`. Cohort B
  `official_event_paired_evaluator_v1_20260830b` starts at
  `2026-08-30T19:00:00Z` in its own database and own producer/verifier state
  paths. No A record is copied, backfilled or reclassified.
- B freezes config SHA `d553877ba5140e31c5eb42233761bf5923cae4b44e601325fc776dab6b2e50e1`,
  normalized producer SHA
  `ea3374e4bab29dfc2f5703853927b14d119b40c2394bc7050fab8045f42073d7`,
  literal producer SHA
  `eeccb9679d29bedafa9696c8923dbab3cad45b2616c773a0b9895e816ccd08ed`
  and independent verifier SHA
  `96f0b588f39f6f8d0bacbdb3615aa1529c00d4d9ccae95739853e32b1aa8a476`.
  Its first isolated cycle is healthy; the independent verifier reports zero
  failures and the honest market-closed ledger has zero evidence rows.
- Paired producer/verifier tests pass 57/57 under Python 3.12 and 3.13.
  Supervisor parsing is clean and its cohort/path binding tests pass 4/4.
  These results validate operational continuity, not predictive edge. The
  supported execution decision remains `no_trade`.

## 2026-08-30 15:14 America/New_York — Cohort B live under hidden supervision

- Committed the cohort-B repair as
  `5de597f0701bd6ad14415856ffc7257d989fa88c` after the staged credential
  audit scanned 1,225 files with zero findings. The official-release release
  boundary passed 125/125 tests under Python 3.12 and 3.13; credential tests
  passed 8/8 under both runtimes.
- Started hidden supervisor PID 30724 with the canonical safe-core command.
  It adopted the existing workers and started only paired cohort-B producer
  PID 8232 and verifier PID 33308. The first supervisor loop retained the
  prelaunch heartbeat age; the next loop correctly reported both new workers
  fresh and did not restart them.
- Producer is `ok`; the independent verifier is `verified` with no failures;
  both supervised stderr logs are empty. Cohort A SHA-256
  `cae48ab421eac9b2e5ca3b0e2b80dea4a61465cc44c65277890f7ee0e6cb7880`
  and cohort B SHA-256
  `628855de19ff67808fb72ce9e12f614a5d6c160fc75766f7201a8276a054b679`
  independently pass SQLite integrity, preserve their exact A/B manifests
  and contain zero evidence rows. Practice execution and authorization were
  not touched; `no_trade` remains supported.
