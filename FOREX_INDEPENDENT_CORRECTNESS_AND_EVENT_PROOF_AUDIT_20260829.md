# Independent correctness and event-to-executable-quote audit

Audit date: 2026-08-29 America/New_York

Canonical workspace: `C:\Users\zmoor\Documents\forex\trad`
Execution boundary: OANDA Practice 007 only; no manual order; no real money

## Decision

The operationally supported decision remains **`no_trade`**. This pass repairs
lineage, freshness, event-time provenance, and empty-allocator behavior; it does
not establish positive after-cost expectancy or authorize a candidate.

The highest-value prediction experiment is now an official-event hypothesis
measured from the executable market state captured at raw first-seen time. It
must be compared prospectively with identical price-only clocks and no-trade.
Technicals may time entry, confirm a break, estimate remaining movement, and
manage exit; they may not manufacture the economic direction.

## 1. Proof-cohort lineage incident

The append-only registry contained valid August 10 successor transitions, but
the published active map and long-running predictor had returned to the older
August 6 cohort IDs. That made restart behavior inconsistent with immutable
cohort semantics.

Resolution:

- Registry replay verifies one incoming transition per cohort, family and
  previous/next continuity, absence of cycles, and exact active heads.
- A material contract cannot silently reactivate after it has been superseded.
  Reuse requires a new explicit `cohort_generation_id`.
- The one-time repair command is dry-run by default and requires the exact
  `lineage_repair_20260829_v1` confirmation token.
- Four clean August 29 heads were opened. The repair produced zero forecasts
  and zero outcomes.
- 328,000 forecasts recorded after supersession under August 6 IDs remain
  immutable and are quarantined from proof, 82,000 per family.
- Registry highwater after repair is 16 cohorts and 16 transitions. The live
  predictor adopted the four August 29 heads after a controlled reload and
  emitted zero weekend forecasts.

Evidence:

- `data/oanda_training_manager/state/proof_lineage_repair_20260829_v1.json`
- `data/oanda_training_manager/reports/proof_lineage_repair/PROOF_LINEAGE_REPAIR_20260829_V1.md`
- `data/oanda_training_manager/state/proof_cohort_registry_v1.json`

## 2. Edge-publication freshness

An old edge report is no longer called current merely because inputs have not
changed. The checkpoint now binds:

- the semantic input fingerprint;
- the completed run/highwaters;
- the exact JSON output SHA-256 and size; and
- the exact Markdown output SHA-256 and size.

A legacy checkpoint without output hashes forces one rebuild. A changed or
missing output also forces a rebuild. The independent verifier reconstructs the
same handshake and separately checks proof-registry replay. Thus a stale or
mutated report cannot pass by presenting an unchanged input fingerprint.

The post-reload rebuild is operational maintenance, not new evidence. It must
retain zero confirmed candidates unless the immutable underlying evidence
genuinely changes.

## 3. Allocator correctness

The allocator now independently replays its transition chain and refuses the
same A-to-B-to-A reactivation pattern. More importantly, zero eligible
opportunities no longer create a stream of empty decision, candidate, and
comparator records.

At an empty universe the allocator:

- publishes `paused_no_eligible_universe`;
- persists no new decision row;
- still matures eligible historical pending outcomes;
- retains no-trade and all comparators in the frozen contract; and
- remains research-only and unable to authorize execution.

Five empty decisions created in the current August 29 allocator cohort before
runtime adoption remain immutable diagnostics. They are not evidence of five
market decisions and are not deleted.

## 4. Raw event-to-quote provenance

New official observations use:

- quote contract
  `official_release_raw_quote_capture_v1_append_boundary_all68_20260829`;
- quote cohort `official_release_raw_quote_capture_v1_20260829a`;
- prospective activation `2026-08-29T12:20:00Z`; and
- a frozen/hash-checked 68-instrument universe.

For each genuinely new raw identity, the raw observation and quote sidecar are
inserted in the same SQLite transaction. Exact proof requires all 68 current-
generation executable bid/ask quotes, a fresh snapshot, fresh component quotes,
valid pip metadata, and bounded capture latency. A partial, stale, or wrong-
generation attempt is retained with its failure details but its proof quote
payload is cleared. Update/delete triggers make the sidecar append-only.

The semantic mapper has no quote loader and cannot recapture a later market
state. It only adapts a valid attached sidecar. Preactivation observations and
unmapped old rows without a sidecar remain diagnostic; they are not rewritten
or promoted.

At audit time the live table and both immutability triggers exist. The table has
zero rows, which is correct because historical events were not backfilled. Both
lane and mapper heartbeats publish the raw contract, cohort, activation,
expected count, and universe hash.

## 5. Issuer-bound policy communication

The Friday Fed speech exposed mentioned-currency leakage: references to foreign
institutions or currencies could be interpreted as observations about those
currencies. The prospective rule now binds an initial official policy
communication only to the configured issuing authority's currency. Other
currencies remain `mentioned_currency_entities`.

The issuer-bound cohort is research-only. Policy speeches cannot become
stance-bearing or directionally publishable merely because they use policy
language. The frozen Warsh fixture verifies USD-only issuer scope while GBP,
SGD, and TRY remain mentions. Its historical quotes and outcome are a regression
case, never proof or a current setup.

Rank V5 now persists a separately contracted `no_trade` comparison for every
future V6-bound decision. Its forecast and declared-horizon outcome are fixed
at zero value and zero cost without requesting a quote. Append-only constraints
also require zero orders and keep the arm research-only, execution-ineligible,
unable to authorize, and unable to promote. This is a new prospective contract;
the prior V5 state contained zero decisions and was not rewritten.

## 6. Data-source state and blockers

- OANDA executable bid/ask remains market and cost ground truth.
- Official authority coverage maps all 21 currencies and both legs of all 68
  pairs, but minimum direct live readiness remains 20/21 because direct RBNZ
  policy pages return HTTP 403 from this host. No bypass is permitted. A
  publisher allow-list or permitted official email/social subscription is
  required for the direct OCR path.
- Causal pre-release consensus remains zero. Trading Economics credentials are
  absent and the tested Finnhub calendar path returns HTTP 403. Post-release
  retrieval cannot substitute for a pre-release snapshot.
- Official daily rate context exists, but timestamp-safe intraday OIS or policy-
  futures repricing is not connected. Disabled parser paths are not evidence.
- Liquid/normal/elevated/wide cost buckets remain separate. Wide/exotic moves
  cannot inflate liquid-pair economics.

## 7. Practice and safety state

During the repair and controlled reload, Practice 007 remained current and
flat: balance/NAV 41.6042, cumulative P/L -8.3430, zero open trades, and zero
pending orders. No manual or discretionary order was sent. No execution or
authorization threshold was relaxed. Real-money routing remains disabled.

## 8. Git and vault governance

The workspace already had Git metadata but no commit or remote. The correct
baseline is a local/private commit after staged diff and credential audit. It
must not include credentials, runtime databases, logs, models, account registry,
or private keys, and this pass does not configure or push a remote.

The new vault publisher archives only the exact clean committed tree, verifies
the credential audit, member inventory, ZIP CRC, size, and SHA-256, and then
publishes a content-addressed source archive plus a latest pointer. It performs
no retention deletion and never falls back to `D:\forex\trad`. The older mixed
vault checkpoints remain preserved as legacy recovery evidence.

The final safety review additionally pins the credential scan, tree inventory,
archive, and every current-record byte to one resolved commit. It rejects
tracked symlinks and force-added runtime/private artifacts, scopes synthetic
credential exceptions to the exact matched dummy value, serializes publishers
with an exclusive lock, and publishes the latest manifest pointer last. The
four generated host/package runtime-lock inventories remain local and ignored.

## 9. Validation

Focused validation covered proof registry/repair, allocator lineage and empty-
universe behavior, edge output integrity, independent verifier replay, raw
all-68 capture, incomplete capture, duplicate no-recapture, mapper-only
adaptation, issuer-bound communication, credential audit, and source-vault
publication. The combined correctness/event/Git-vault/source-register suite
passed 91 tests; the final event heartbeat additions passed the 41-test
lane/mapper/communications suite, and the full local-news plus communications
suite passed 463 tests. A separate adversarial Git/vault security suite passed
13 tests. All material Python modules compiled.

## 10. Next gates

1. Complete the first post-reload edge publication and exact verifier handshake.
2. Prove the allocator inserts no further empty decision at its next cycle.
3. Observe a genuinely new post-activation official event and verify its raw
   68/68 sidecar before semantic use.
4. Compare the frozen event hypothesis against identical price-only clocks and
   the explicit zero-value no-trade arm after executable costs, latency, and
   missed-entry stress.
5. Continue untouched prospective collection; do not retune from interim data.
6. Resolve RBNZ permission, causal consensus, and event-time rate repricing as
   external data gates.
7. Revalidate quotes, account, sources, and transports at Sunday reopen.

Until those gates produce independently replicated positive after-cost evidence,
`no_trade` is the correct governed result.
