# Official event paired evaluator V1

## Purpose

This research-only evaluator compares a frozen official-event hypothesis with
price-only and placebo controls on the same event, instrument, entry quote,
horizon and cost assumptions. It is the modeling companion to the append-only
raw event quote sidecar and horizon collector; it is not an execution route.

The evaluator answers a narrow prospective question: after an official source
is first observed, does its issuer-bound direction add after-cost information
beyond the technical state already known at that clock? It cannot place an
order, authorize a candidate, promote a lane or alter Practice 007. The
supported execution decision remains `no_trade`.

## Frozen lineage

- Contract: `official_event_paired_evaluator_v1_append_only_20260830`
- Active cohort: `official_event_paired_evaluator_v1_20260830b`
- Activation: `2026-08-30T19:00:00Z`
- Active database:
  `official_event_paired_evaluator_v1_20260830b.sqlite`
- Raw entry contract:
  `official_release_raw_quote_capture_v1_append_boundary_all68_20260829`
- Raw entry cohort: `official_release_raw_quote_capture_v1_20260829a`
- Raw collector contract:
  `official_release_fast_lane_v4_selection_v2_authoritative_communications_20260828T150000Z`
- Raw collector cohort:
  `official_release_fast_lane_v4_communications_20260828T150000Z`
- Mapping contract:
  `official_release_fast_mapping_v3_semantic_vs_publish_gate_20260824`
- Mapping cohort: `official_release_fast_mapping_v3_20260824`
- Required classification:
  `local_fx_news_rules_20260828_v151_pair_breakout_recap_boundary`
- Horizon contract:
  `official_event_quote_horizon_capture_v1_all68_append_only_20260830`
- Horizon cohort: `official_event_quote_horizon_capture_v1_20260830a`
- Frozen 68-instrument universe SHA-256:
  `b6abe559dcaa9faa0a1e48217f97a12dc9043eb9595d2b946bf80f93035be142`
- Frozen source-authority map:
  `official_central_bank_source_map_v9_us_policy_communication_clocks_20260827`
  with SHA-256
  `09d4f80708cbed509564e2f7bc5245a24de44df3728ed0583b1d9cbb6f299a19`
- Frozen news-source configuration SHA-256:
  `6058d3a81d250add66e1d22b0f2541990f3bc90c52575c4a7a97c3361ad0a45b`
- Frozen raw-capture producer SHA-256:
  `55f3a1260c2c3644fa2c4de0e70c4fe081e21fff366caeeaae8fe80bfb1d752a`
- Frozen horizon-capture producer SHA-256:
  `a0920b5edb530876c42e1e2892f624624db4221262c6a67fdcb42f9a12c146c9`
- Frozen paired-evaluator config SHA-256:
  `d553877ba5140e31c5eb42233761bf5923cae4b44e601325fc776dab6b2e50e1`
- Frozen normalized paired-producer SHA-256:
  `ea3374e4bab29dfc2f5703853927b14d119b40c2394bc7050fab8045f42073d7`
- Frozen literal paired-producer SHA-256:
  `eeccb9679d29bedafa9696c8923dbab3cad45b2616c773a0b9895e816ccd08ed`
- Independent verifier SHA-256:
  `96f0b588f39f6f8d0bacbdb3615aa1529c00d4d9ccae95739853e32b1aa8a476`

Cohort `official_event_paired_evaluator_v1_20260830a` remains preserved in
the original `official_event_paired_evaluator_v1.sqlite` as an immutable
zero-evidence baseline: one manifest and no decisions, arms, horizon inputs or
outcomes. It was superseded before the first evidence row after a transient
Windows `os.replace` access-denied failure terminated the verifier heartbeat
publisher. Cohort B changes only the operational JSON publication boundary:
producer and verifier retry that transient failure with a frozen, bounded
eight-attempt policy. A and B have separate databases, state files and
heartbeats; no A observation is imported or relabeled.

No preactivation observation is imported. Any material change to a source,
mapping, technical rule, pair selector, cost assumption or outcome contract
requires a new cohort.

## Decision-time boundary

An eligible raw event already contains an exact, immutable all-68 executable
bid/ask sidecar. Before the one-minute outcome exists, V1 must write one
append-only decision and its complete arm/horizon schedule. The decision owns
copies and hashes of the exact raw observation, entry capture, selected
mapping and causal technical input used to decide. A mutable upstream mapper
row is never described as immutable merely because its payload was copied;
the independent verifier also detects later upstream drift.

The mapper contract/cohort contains multiple historical classification
versions, so V1 selects only the exact classification named above. A one-row
append-only cohort manifest additionally freezes the config bytes, normalized
producer source identity, literal producer-file hash, authority-map bytes,
news-source configuration, upstream producer hashes, all numeric timing
limits and classification version. The raw observation must also carry the
exact source-specific contract/cohort from that frozen news-source
configuration and the exact frozen raw-collector contract/cohort. A later
code, config, source, authority, dependency, timing-limit or classification
change therefore fails rather than entering the same cohort.

The schedule deadline is 55 seconds after event T0. By that deadline every
eligible event must be represented, even when mapping, issuer identity,
direction or technical state is absent. Such cases receive sealed abstention
or invalid rows rather than being dropped. An outcome database is not read
before the decision transaction commits.

The decision clock is sampled once before causal reads and again only after
the append transaction owns its write lock. This prevents a database-lock
wait from retaining an earlier, apparently timely clock. Entry-capture
latency is independently derived from `captured_utc - event_T0`, must equal
the value recorded in the payload and must remain within the frozen 15-second
limit. Every entry component's quote age and event offset are similarly
rederived instead of trusting an upstream quality label.

The technical rule uses only completed M1 closes present in a feature snapshot
whose generation clock does not follow event T0. A later rolling snapshot may
never repair a missing decision-time feature. The frozen diagnostic is a
5-versus-20 exponential-average direction; it is a comparison arm, not a
claim that moving averages independently provide edge.

The exact event-T0 quote is an event-clock counterfactual baseline. It measures
the response available from the frozen quote sidecar; it is not evidence that
an order was submitted before semantic parsing completed. Decision latency is
retained explicitly, and any later practice canary would require separate
forecast-to-order and realized-latency evidence.

## Issuer and pair binding

Only the configured source authority may establish the affected currency.
The raw source currency and mapped source currency must identify the same
single issuer currency. Foreign currencies merely mentioned in article text
cannot become affected legs.

V1 chooses one instrument without looking at outcomes: among frozen-universe
pairs containing the issuer currency, choose the lowest event-T0 executable
spread, breaking ties lexicographically. Every arm then shares that event,
pair, quote and horizon. Pair orientation is applied mechanically so a
positive issuer-currency view becomes long when the issuer is the base and
short when it is the quote.

## Frozen comparison arms

Every event/pair/horizon contains all five arms, including explicit
abstentions:

1. `official_source_only` — issuer-bound mapped direction.
2. `price_only` — causal completed-bar technical direction.
3. `official_plus_technical_confirmation` — official direction only when the
   technical state agrees; conflict abstains and never reverses the official
   view.
4. `official_flipped_control` — the exact opposite of the official direction.
5. `no_trade` — zero exposure and zero P/L.

The flipped and no-trade rows are controls, not eligible alternatives. Missing
official direction does not cause the price-only arm to disappear, and missing
technical data does not cause the official arm to disappear.

## Outcomes and dependence

Declared horizons are 1, 5, 15, 30 and 60 minutes. Long returns use event-T0
ask to horizon bid; short returns use event-T0 bid to horizon ask. Both
executable spreads are therefore already embedded exactly once. Separate
round-trip slippage stresses of 0.00, 0.25 and 0.50 pips are subtracted once
from a non-abstaining arm and are never folded into the recorded spread.

An invalid or missing terminal horizon produces explicit invalid outcomes for
the complete arm grid; it cannot be survivor-filtered. Pair manifestations,
horizons and arms from one event are dependent observations. Effective
evidence is deduplicated by issuer-currency factor and predeclared 15-minute
market episode, never by raw row count.

For each terminal horizon the evaluator rederives the target, read-start,
attempt-completion, attempt-delay, snapshot-age, quote-age and target-offset
clocks from copied row and payload bytes. The frozen attempt/age/skew limits
are checked again. A later capture carrying historical-looking target quotes
therefore remains invalid rather than becoming a backfilled success.

## Verification and safety

A standalone verifier must not import the producer. It independently rebuilds
the eligible-event schedule, deadline and pre-outcome seals, source and
mapping lineage, issuer-only pair selection, causal technical state, all
event/pair/horizon/arm rows, side algebra, spread/slippage math, episode IDs,
payload hashes, foreign keys and append-only triggers.

Adversarial tests cover late decision insertion, later feature substitution,
mentioned-currency leakage, outcome-aware pair deletion, missing controls,
omitted invalid horizons, spread or slippage double counting, dependent-row
inflation, upstream payload drift, forged self-consistent payloads,
update/delete attempts, wrong cohorts and preactivation imports.

Passing implementation tests establishes only that the prospective experiment
is measured as declared. It does not establish predictive edge. Promotion and
authorization remain impossible unless later independent governed evidence
passes the separate lifecycle contract.

## Known limits

- The T0 entry remains a counterfactual attribution clock, not proof that an
  executable order could have been formulated before semantic parsing.
- The five-second precommit margin protects against ordinary lock delay, but
  an extreme storage stall after the final precommit sample is not a
  cryptographic commit-completion timestamp. A later practice canary still
  requires forecast-to-order latency evidence.
- The current 15-minute episode key is issuer-bound. Later statistical
  aggregation must also cluster cross-issuer global shocks before treating
  simultaneous authority events as independent.
- Internal insertion helpers are not an authorization boundary. The separate
  read-only verifier is required to accept ledger integrity.
