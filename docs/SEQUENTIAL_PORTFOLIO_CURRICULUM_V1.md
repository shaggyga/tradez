# Sequential Portfolio Curriculum V1

Status: independently verified historical training/discovery; nonexecuting and proof-ineligible

## Purpose

This sidecar turns the immutable Sequential Portfolio Replay V1 cases into a
deliberate-practice curriculum. It is intentionally separate from the replay
ledger and every broker, account, lifecycle, authorization, signal, and live
runtime path.

The initial fixture contains four precommitted practice sessions of 12 actions:

- current hardened cohort
  `sequential_portfolio_curriculum_v1.22124b7c2ebf24ad2430`;

- 48 learner attempts;
- 36 distinct historical decision cases;
- 12 spaced-repetition reviews;
- four structural UTC-hour episode components;
- 36 selections equal to the best stored depth-one action;
- mean local regret of 0.671875 pips.

Those are learner-mechanics diagnostics. They are not trading results, market
proof, or evidence that the deterministic fixture learned a profitable policy.

## Withheld-feedback contract

At the start of each practice session, the scheduler uses only memory from
earlier sessions. It then stores and commits:

1. the complete session schedule;
2. every assignment identity;
3. every exact learner-attempt identity;
4. every learner action.

Only after that database commit may the sidecar reveal any source outcome for
the session. The minimum feedback stage is greater than the maximum action
commit stage. Every attempt also records the number of prior feedback rows
available at precommit time. Within-session adaptation is forbidden.

The deterministic baseline chooses a future-free hash-selected legal action on
first exposure. On a later review it may recall the best action revealed in an
earlier session. This exercises the learning plumbing without inventing a human
performance record.

## Spaced repetition and novelty

The frozen scheduler gives unseen cases a novelty weight and reserves review
slots after the first session. Incorrect cases become due in the next session.
Correct recall expands the interval geometrically, capped by the frozen
contract. Overdue reviews receive a small deterministic priority bonus.

The exact schedule is deterministic from:

- source case identities;
- the frozen seed;
- earlier-session memory only;
- the immutable scheduler contract.

Material changes require a new content-addressed cohort.

## Evidence accounting

The curriculum keeps three counts separate:

- learner attempts: every practice action;
- distinct market repetitions: unique historical cases encountered;
- structural episode components: unique source episode labels encountered.

A first case exposure may add one distinct market repetition. A repeat attempt
always has `counts_as_market_repetition=0` and
`counts_as_regime_repetition=0`. Every primary/alternative action option also
has zero repetition weight. Four structural episode components are not claimed
as four independent regimes; the independent-regime count remains unknown.

## Immutable identities and storage

Case identity binds the source cohort, source session, source clock, causal
snapshot hash, and exact legal-action set. Attempt identity additionally binds
the learner, practice session, assignment, per-case attempt ordinal, and exact
committed action.

All tables reject updates and deletes. A session seal binds the row roots,
counts, frozen source seal, and curriculum contract. The independent verifier
imports neither producer nor replay/curriculum core and rebuilds cases,
schedules, precommit ordering, actions, feedback, memory transitions,
repetition counts, hashes, roots, and the final seal from read-only SQLite
snapshots. It also independently recomputes every published summary statistic,
requires the full research-safety state, checks the exact seal and snapshot
payloads, and enforces the content-bound deterministic timestamp contract.
Forged safety, forged statistics, and runtime timestamp drift fail closed.

## Mistake labels

Local regret is grouped into compact curriculum labels:

- direction;
- entry;
- management;
- exit;
- rotation;
- opportunity selection;
- none when the chosen option matches the best stored depth-one result.

These labels describe one-step training decisions. They do not establish the
globally optimal portfolio action or a profitable strategy.

## Commands

```powershell
python oanda_sequential_portfolio_curriculum.py
python oanda_sequential_portfolio_curriculum_verifier.py
python -m pytest -q test_oanda_sequential_portfolio_curriculum.py
```

## Safe continuation

The next curriculum increment can add a human-entered action-plan interface and
mistake-balanced practice batches. Expansion to additional deterministic source
sessions must retain the same one-case evidence accounting. Any learned policy
selected here still requires a strictly later untouched prospective cohort;
this sidecar can never authorize Practice 007.
